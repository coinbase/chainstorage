package cadence

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/uber-go/tally"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	zapadapter "logur.dev/adapter/zap"
	"logur.dev/logur"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/timesource"
)

type (
	Runtime interface {
		RegisterWorkflow(w interface{}, options workflow.RegisterOptions)
		RegisterActivity(a interface{}, options activity.RegisterOptions)
		ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow interface{}, request interface{}) (client.WorkflowRun, error)
		ExecuteActivity(ctx workflow.Context, activity interface{}, request interface{}, response interface{}) error
		GetLogger(ctx workflow.Context) *zap.Logger
		GetScope(ctx workflow.Context) tally.Scope
		GetActivityLogger(ctx context.Context) *zap.Logger
		GetActivityScope(ctx context.Context) tally.Scope
		GetTimeSource(ctx workflow.Context) timesource.TimeSource
		TerminateWorkflow(ctx context.Context, workflowID string, runID string, reason string) error
		OnStart(ctx context.Context) error
		OnStop(ctx context.Context) error
		ListOpenWorkflows(ctx context.Context, namespace string, maxPageSize int32) (*workflowservice.ListOpenWorkflowExecutionsResponse, error)
	}

	RuntimeParams struct {
		fx.In
		fxparams.Params
		Tracer  opentracing.Tracer
		TestEnv *TestEnv `optional:"true"`
	}

	runtimeImpl struct {
		config          *config.Config
		logger          *zap.Logger
		workflowClient  client.Client
		namespaceClient client.NamespaceClient
		workers         []worker.Worker
	}
)

func NewRuntime(params RuntimeParams) (Runtime, error) {
	if params.TestEnv != nil {
		return newTestRuntime(params.TestEnv, params.Logger)
	}

	// Temporal uses a different logger. Adapt zap.Logger into its interface.
	logger := params.Logger
	runtimeLogger := logur.LoggerToKV(zapadapter.New(logger))

	address := params.Config.Cadence.Address
	tlsConfig := params.Config.Cadence.TLSConfig
	connectionOptions := client.ConnectionOptions{}
	if tlsConfig.Enabled && params.Config.Env() != config.EnvLocal {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse address (%v): %w", address, err)
		}

		connectionOptions.TLS = &tls.Config{
			MinVersion:         tls.VersionTLS12,
			ServerName:         host,
			InsecureSkipVerify: !tlsConfig.ValidateHostname,
		}

		if tlsConfig.CertificateAuthority != "" {
			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM([]byte(tlsConfig.CertificateAuthority)) {
				return nil, xerrors.Errorf("failed to parse CA certificate: %v", tlsConfig.CertificateAuthority)
			}
			connectionOptions.TLS.RootCAs = caCertPool
		}

		if tlsConfig.ClientCertificate != "" && tlsConfig.ClientPrivateKey != "" {
			clientCert, err := tls.X509KeyPair([]byte(tlsConfig.ClientCertificate), []byte(tlsConfig.ClientPrivateKey))
			if err != nil {
				return nil, xerrors.Errorf("failed to parse client certificate or key (%v): %w", tlsConfig.ClientCertificate, err)
			}
			connectionOptions.TLS.Certificates = []tls.Certificate{clientCert}
		}
	}

	options := client.Options{
		Namespace:         params.Config.Cadence.Domain,
		HostPort:          address,
		MetricsScope:      params.Metrics,
		Logger:            runtimeLogger,
		Tracer:            params.Tracer,
		ConnectionOptions: connectionOptions,
	}

	namespaceClient, err := client.NewNamespaceClient(options)
	if err != nil {
		return nil, xerrors.Errorf("failed to create namespace client: %w", err)
	}

	workflowClient, err := client.NewClient(options)
	if err != nil {
		return nil, xerrors.Errorf("failed to create workflow client: %w", err)
	}

	workers := make([]worker.Worker, len(params.Config.Workflows.Workers))
	for i, workerConfig := range params.Config.Workflows.Workers {
		workers[i] = worker.New(
			workflowClient,
			workerConfig.TaskList,
			worker.Options{
				EnableSessionWorker: true,
			},
		)
	}

	runtime := &runtimeImpl{
		config:          params.Config,
		logger:          logger,
		workflowClient:  workflowClient,
		namespaceClient: namespaceClient,
		workers:         workers,
	}

	return runtime, nil

}

func (r *runtimeImpl) ListOpenWorkflows(ctx context.Context, namespace string, maxPageSize int32) (*workflowservice.ListOpenWorkflowExecutionsResponse, error) {
	openWorkflows, err := r.workflowClient.ListOpenWorkflow(ctx, &workflowservice.ListOpenWorkflowExecutionsRequest{
		Namespace:       namespace,
		MaximumPageSize: maxPageSize,
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to get open workflows: %w", err)
	}
	return openWorkflows, nil
}

func (r *runtimeImpl) RegisterWorkflow(w interface{}, options workflow.RegisterOptions) {
	for _, worker := range r.workers {
		worker.RegisterWorkflowWithOptions(w, options)
	}
}

func (r *runtimeImpl) RegisterActivity(a interface{}, options activity.RegisterOptions) {
	for _, worker := range r.workers {
		worker.RegisterActivityWithOptions(a, options)
	}
}

func (r *runtimeImpl) ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow interface{}, request interface{}) (client.WorkflowRun, error) {
	return r.workflowClient.ExecuteWorkflow(ctx, options, workflow, request)
}

func (r *runtimeImpl) ExecuteActivity(ctx workflow.Context, activity interface{}, request interface{}, response interface{}) error {
	future := workflow.ExecuteActivity(ctx, activity, request)
	return future.Get(ctx, response)
}

func (r *runtimeImpl) GetLogger(ctx workflow.Context) *zap.Logger {
	logger := workflow.GetLogger(ctx)
	return log.FromTemporal(logger)
}

func (r *runtimeImpl) GetScope(ctx workflow.Context) tally.Scope {
	return workflow.GetMetricsScope(ctx)
}

func (r *runtimeImpl) GetActivityLogger(ctx context.Context) *zap.Logger {
	logger := activity.GetLogger(ctx)
	return log.FromTemporal(logger)
}

func (r *runtimeImpl) GetActivityScope(ctx context.Context) tally.Scope {
	return activity.GetMetricsScope(ctx)
}

func (r *runtimeImpl) GetTimeSource(ctx workflow.Context) timesource.TimeSource {
	return timesource.NewWorkflowTimeSource(ctx)
}

func (r *runtimeImpl) TerminateWorkflow(ctx context.Context, workflowID string, runID string, reason string) error {
	return r.workflowClient.TerminateWorkflow(ctx, workflowID, runID, reason)
}

func (r *runtimeImpl) OnStart(ctx context.Context) error {
	r.logger.Info("starting workflow runtime")

	if err := r.startDomain(ctx); err != nil {
		return xerrors.Errorf("failed to start domain: %w", err)
	}

	if err := r.startWorkers(); err != nil {
		return xerrors.Errorf("failed to start workers: %w", err)
	}

	return nil
}

func (r *runtimeImpl) OnStop(ctx context.Context) error {
	r.logger.Info("stopping workflow runtime")
	r.stopWorkers()
	r.workflowClient.Close()
	r.namespaceClient.Close()
	return nil
}

func (r *runtimeImpl) startDomain(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	cadenceConfig := r.config.Cadence
	retentionPeriod := 24 * time.Hour * time.Duration(cadenceConfig.RetentionPeriod)
	err := r.namespaceClient.Register(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        cadenceConfig.Domain,
		WorkflowExecutionRetentionPeriod: &retentionPeriod,
	})
	if err != nil {
		if _, ok := err.(*serviceerror.NamespaceAlreadyExists); !ok {
			return err
		}

		r.logger.Info("domain name is already registered", zap.String("domain", cadenceConfig.Domain))
	}

	describeResponse, err := r.namespaceClient.Describe(ctx, cadenceConfig.Domain)
	if err != nil {
		return xerrors.Errorf("failed to register cadence domain: %w", err)
	}

	r.logger.Info("started cadence domain", zap.Reflect("response", describeResponse))
	return nil
}

func (r *runtimeImpl) startWorkers() error {
	for _, w := range r.workers {
		if err := w.Start(); err != nil {
			return xerrors.Errorf("failed to start worker: %w", err)
		}

		r.logger.Info("started worker")
	}

	return nil
}

func (r *runtimeImpl) stopWorkers() {
	r.logger.Info("stopping workers")
	for _, w := range r.workers {
		w.Stop()
	}
}
