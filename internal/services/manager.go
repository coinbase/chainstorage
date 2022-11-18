package services

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/utils/log"
)

type (
	SystemManager interface {
		Context() context.Context
		Logger() *zap.Logger
		Tracer() opentracing.Tracer
		ServiceContext() context.Context
		ServiceWaitGroup() *sync.WaitGroup
		State() ServiceState
		AddPreShutdownHook(PreShutdownHook)
		AddShutdownHook(ShutdownHook)
		WaitForInterrupt()
		Shutdown()
	}

	// ManagerOption allows the manager to be customized via options.
	ManagerOption func(*managerOpts)

	Params struct {
		fx.In
		SystemManager SystemManager `optional:"true"`
	}

	systemManager struct {
		mu    sync.RWMutex
		state ServiceState

		log    *zap.Logger
		tracer opentracing.Tracer

		ctx              context.Context
		serviceCtx       context.Context
		serviceCtxCancel context.CancelFunc
		serviceWg        sync.WaitGroup
		shutdownHooks    []ShutdownHook
		preShutdownHooks []PreShutdownHook
		shutdownChannel  chan struct{}
	}

	managerOpts struct {
		logger      *zap.Logger
		logSampling *zap.SamplingConfig
		rootContext context.Context
	}

	ServiceState int

	ShutdownHook     func()
	PreShutdownHook  func()
	ShutdownFunction func(ctx context.Context) error
	DaemonFunction   func(ctx context.Context) (ShutdownFunction, chan error)
)

const (
	shutdownTimeout = 10 * time.Second

	// termDelay specifies the timeout after which the service will be forced to terminate.
	termDelay = time.Second * 20

	Starting ServiceState = iota + 1
	Running
	Stopping
	Terminated
	Failed
)

// NewManager creates a new system manager.
func NewManager(opts ...ManagerOption) SystemManager {
	mOpts := managerOpts{
		rootContext: context.Background(),
	}
	for _, o := range opts {
		o(&mOpts)
	}
	if mOpts.logger == nil {
		mOpts.logger = log.New()
	}

	manager := &systemManager{
		shutdownChannel: make(chan struct{}),
		log:             mOpts.logger,
	}

	// Setup contexts
	// (Note however that neither of these contexts are passed into grpc server handler functions)
	ctx := ctxzap.ToContext(mOpts.rootContext, manager.log)
	manager.ctx = ctx
	manager.serviceCtx, manager.serviceCtxCancel = context.WithCancel(ctx)

	return manager
}

// WithLogger allows the logger to be injected into the manager.
func WithLogger(logger *zap.Logger) ManagerOption {
	return func(mOpts *managerOpts) {
		mOpts.logger = logger
	}
}

// WithLogSampling will enable sampling on the default logger if you didn't
// provide one yourself. This option will have no effect if used with the
// WithLogger option.
func WithLogSampling(sampling *zap.SamplingConfig) ManagerOption {
	return func(mOpts *managerOpts) {
		mOpts.logSampling = sampling
	}
}

// WithContext allows to set root context instead of
// using context.Background() by default
func WithContext(ctx context.Context) ManagerOption {
	return func(mOpts *managerOpts) {
		mOpts.rootContext = ctx
	}
}

func Daemonize(manager SystemManager, f DaemonFunction, name string) {
	ctx := manager.ServiceContext()
	logger := manager.Logger()

	for {
		shutdownFunction, errChannel := f(ctx)
		select {
		case err := <-errChannel:
			logger.Error(name+" died or failed to start! Restarting", zap.Error(err))
			time.Sleep(time.Millisecond * 100)
		case <-ctx.Done():
			logger.Info("Shutting down " + name)
			shutdownCtx, shutdownCtxCancel := context.WithTimeout(context.TODO(), shutdownTimeout)
			defer shutdownCtxCancel()
			if shutdownFunction == nil {
				logger.Warn("No shutdown function available for " + name)
			} else {
				if err := shutdownFunction(shutdownCtx); err != nil {
					logger.Warn("Failed to shut down "+name, zap.Error(err))
				}
			}
			logger.Debug(name + " finished")
			return
		}
	}
}

// GracefulShutdown sets up a shutdown handler that calls killFunc when the process needs to clean up and terminate.
// killFunc will be executed at most once.
func GracefulShutdown(ctx context.Context, killFunc func()) {
	log := ctxzap.Extract(ctx)
	intCh := make(chan os.Signal, 1)
	signal.Notify(intCh, os.Interrupt, syscall.SIGTERM)
	var sigCount int
	go func() {
		for sig := range intCh {
			log.Debug("Received termination signal", zap.String("signal", sig.String()))
			switch sigCount {
			case 0:
				go func() {
					log.Info("Shutdown requested", zap.String("signal", sig.String()))
					killFunc()
				}()
			case 1:
				log.Info("Delayed forced termination requested", zap.Duration("delay", termDelay), zap.String("signal", sig.String()))
				time.AfterFunc(termDelay, func() {
					os.Exit(2)
				})
			default:
				log.Warn("Forced termination requested", zap.String("signal", sig.String()))
				_ = log.Sync() // #nosec
				os.Exit(2)
			}
			sigCount++
		}
	}()
}

func (m *systemManager) Logger() *zap.Logger {
	return m.log
}

func (m *systemManager) Tracer() opentracing.Tracer {
	return m.tracer
}

// ServiceContext returns a cancellable context derived from the background context
func (m *systemManager) ServiceContext() context.Context {
	return m.serviceCtx
}

// State returns the current state of the manager.
func (m *systemManager) State() ServiceState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state
}

func (m *systemManager) setState(state ServiceState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = state
}

// Context returns the background context
func (m *systemManager) Context() context.Context {
	return m.ctx
}

func (m *systemManager) ServiceWaitGroup() *sync.WaitGroup {
	return &m.serviceWg
}

// Pre-shutdown hooks are done in parallel before canceling the system context.
func (m *systemManager) AddPreShutdownHook(hook PreShutdownHook) {
	m.preShutdownHooks = append(m.preShutdownHooks, hook)
}

// Shutdown hooks are done sequentially after canceling the system context.
func (m *systemManager) AddShutdownHook(hook ShutdownHook) {
	m.shutdownHooks = append(m.shutdownHooks, hook)
}

// terminationReporter reports when a service is not shutting down.
func (m *systemManager) terminationReporter(serviceName string, doneChan chan struct{}) {
	<-m.serviceCtx.Done()
	var minuteCounter int
	for {
		select {
		case <-doneChan:
			return
		case <-time.After(time.Minute):
			minuteCounter++
			m.log.Sugar().Errorf("Service: %s has not exited after %d minute(s)", serviceName, minuteCounter)
		}
	}
}

// Shutdown signals the manager to gracefully shutdown. You must have
// previously called WaitForInterrupt for Shutdown to have any effect.
func (m *systemManager) Shutdown() {
	select {
	case <-m.shutdownChannel:
		m.log.Sugar().Info("Manager is already shutdown")
	default:
		close(m.shutdownChannel)
	}
}

// WaitForInterrupt will block until either Shutdown is called or a signal is
// sent to shutdown the process. It will fire the appropriate shutdown hooks
// and cleanly shutdown the manager before it unblocks.
func (m *systemManager) WaitForInterrupt() {
	m.setState(Running)

	m.blockOnSignal()
	m.serviceWg.Wait()
	// Run shutdown hooks
	m.log.Debug("Running shutdown hooks")
	for _, hook := range m.shutdownHooks {
		hook()
	}
	_ = m.log.Sync()
	m.setState(Terminated)
}

// Blocks until SIGINT, SIGTERM or a message is received on
// manager.shutdownChannel After receiving a shutdown signal, cancels service
// context then runs shutdown hooks
func (m *systemManager) blockOnSignal() {
	// 1 signal is graceful, 2 starts a 20 second to force kill, 3 is immediate force kill
	GracefulShutdown(m.serviceCtx, m.Shutdown)

	// Wait for shutdown signal
	<-m.shutdownChannel
	m.setState(Stopping)
	m.performPreShutdownHooks()
	m.log.Info("Shutting down")
	m.serviceCtxCancel()
}

func (m *systemManager) performPreShutdownHooks() {
	m.log.Debug("Running pre-shutdown hooks")
	var wg sync.WaitGroup
	wg.Add(len(m.preShutdownHooks))
	for _, hook := range m.preShutdownHooks {
		go func(hook PreShutdownHook) {
			defer wg.Done()
			hook()
		}(hook)
	}
	wg.Wait()
}
