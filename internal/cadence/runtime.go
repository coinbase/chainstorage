package cadence

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"time"

	filterpb "go.temporal.io/api/filter/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	sdktally "go.temporal.io/sdk/contrib/tally"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/types/known/durationpb"
	zapadapter "logur.dev/adapter/zap"
	"logur.dev/logur"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/retry"
	"github.com/coinbase/chainstorage/internal/utils/timesource"
)

type (
	Runtime interface {
		RegisterWorkflow(w any, options workflow.RegisterOptions)
		RegisterActivity(a any, options activity.RegisterOptions)
		ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow any, request any) (client.WorkflowRun, error)
		ExecuteActivity(ctx workflow.Context, activity any, request any, response any) error
		GetLogger(ctx workflow.Context) *zap.Logger
		GetMetricsHandler(ctx workflow.Context) client.MetricsHandler
		GetActivityLogger(ctx context.Context) *zap.Logger
		GetTimeSource(ctx workflow.Context) timesource.TimeSource
		TerminateWorkflow(ctx context.Context, workflowID string, runID string, reason string) error
		OnStart(ctx context.Context) error
		OnStop(ctx context.Context) error
		ListOpenWorkflows(ctx context.Context, namespace string, maxPageSize int32, workflowType string) (*workflowservice.ListOpenWorkflowExecutionsResponse, error)
	}

	RuntimeParams struct {
		fx.In
		fxparams.Params
		TestEnv *TestEnv `optional:"true"`
	}

	runtimeImpl struct {
		config          *config.Config
		logger          *zap.Logger
		workflowClient  client.Client
		namespaceClient client.NamespaceClient
		workers         []worker.Worker
	}

	listOpenWorkflowsFn func(ctx context.Context, request *workflowservice.ListOpenWorkflowExecutionsRequest) (*workflowservice.ListOpenWorkflowExecutionsResponse, error)
)

func NewRuntime(params RuntimeParams) (Runtime, error) {
	if params.TestEnv != nil {
		return newTestRuntime(params.TestEnv, params.Logger)
	}

	// Temporal uses a different logger. Adapt zap.Logger into its interface.
	logger := params.Logger
	runtimeLogger := logur.LoggerToKV(zapadapter.New(logger))

	address := params.Config.Cadence.Address
	connectionOptions, err := newConnectionOptions(params.Config.Cadence, params.Config.Env())
	if err != nil {
		return nil, err
	}

	options := client.Options{
		Namespace:         params.Config.Cadence.Domain,
		HostPort:          address,
		MetricsHandler:    sdktally.NewMetricsHandler(params.Metrics),
		Logger:            runtimeLogger,
		ConnectionOptions: connectionOptions,
	}

	namespaceClient, err := client.NewNamespaceClient(options)
	if err != nil {
		return nil, xerrors.Errorf("failed to create namespace client: %w", err)
	}

	workflowClient, err := client.Dial(options)
	if err != nil {
		return nil, xerrors.Errorf("failed to create workflow client: %w", err)
	}

	workers := make([]worker.Worker, len(params.Config.Workflows.Workers))
	for i, workerConfig := range params.Config.Workflows.Workers {
		workers[i] = worker.New(
			workflowClient,
			workerConfig.TaskList,
			newWorkerOptions(workerConfig),
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

func newConnectionOptions(cadenceConfig config.CadenceConfig, env config.Env) (client.ConnectionOptions, error) {
	connectionOptions := client.ConnectionOptions{}
	if cadenceConfig.KeepAliveTime > 0 {
		connectionOptions.KeepAliveTime = cadenceConfig.KeepAliveTime
	}
	if cadenceConfig.KeepAliveTimeout > 0 {
		connectionOptions.KeepAliveTimeout = cadenceConfig.KeepAliveTimeout
	}

	tlsConfig := cadenceConfig.TLSConfig
	if tlsConfig.Enabled && env != config.EnvLocal {
		host, _, err := net.SplitHostPort(cadenceConfig.Address)
		if err != nil {
			return client.ConnectionOptions{}, xerrors.Errorf("failed to parse address (%v): %w", cadenceConfig.Address, err)
		}

		connectionOptions.TLS = &tls.Config{
			MinVersion:         tls.VersionTLS12,
			ServerName:         host,
			InsecureSkipVerify: !tlsConfig.ValidateHostname,
		}

		if tlsConfig.CertificateAuthority != "" {
			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM([]byte(tlsConfig.CertificateAuthority)) {
				return client.ConnectionOptions{}, xerrors.Errorf("failed to parse CA certificate: %v", tlsConfig.CertificateAuthority)
			}
			connectionOptions.TLS.RootCAs = caCertPool
		}

		if tlsConfig.ClientCertificate != "" && tlsConfig.ClientPrivateKey != "" {
			clientCert, err := tls.X509KeyPair([]byte(tlsConfig.ClientCertificate), []byte(tlsConfig.ClientPrivateKey))
			if err != nil {
				return client.ConnectionOptions{}, xerrors.Errorf("failed to parse client certificate or key (%v): %w", tlsConfig.ClientCertificate, err)
			}
			connectionOptions.TLS.Certificates = []tls.Certificate{clientCert}
		}
	}

	return connectionOptions, nil
}

// workerStopTimeout is how long worker.Stop waits for running activities to
// observe the worker-stop channel and unwind. The retention activity uses it
// to release its row claims (INF-1603); the claim cleanup — an outcome write
// then the release, each on its own retirement.RetirementClaimCleanupTimeout —
// is bounded in total by retirement.RetirementClaimCleanupBudget, which must
// stay below this value (pinned by
// TestWorkerStopTimeoutCoversRetirementClaimCleanup). It must in
// turn stay inside fx's stop timeout (15s default) and the pod's termination
// grace period (30s default) or the process is killed mid-release, which is
// the exact failure the wait exists to prevent. The SDK cancels every
// activity's context only after this wait, so before it was set (default
// 0s) in-flight activities were canceled the instant the worker stopped;
// this is more grace for all of them, not less.
const workerStopTimeout = 10 * time.Second

func newWorkerOptions(workerConfig config.WorkerConfig) worker.Options {
	options := worker.Options{
		// Enable this option to allow worker to process sessions. Defaults to false.
		EnableSessionWorker: true,
		// If set defines maximum amount of time that workflow task will be allowed to run. Defaults to 1 sec.
		DeadlockDetectionTimeout: 2 * time.Second,
		WorkerStopTimeout:        workerStopTimeout,
	}
	if workerConfig.MaxConcurrentActivityExecutionSize > 0 {
		options.MaxConcurrentActivityExecutionSize = workerConfig.MaxConcurrentActivityExecutionSize
	}

	return options
}

func (r *runtimeImpl) ListOpenWorkflows(ctx context.Context, namespace string, maxPageSize int32, workflowType string) (*workflowservice.ListOpenWorkflowExecutionsResponse, error) {
	return listOpenWorkflowExecutions(ctx, namespace, maxPageSize, workflowType, r.workflowClient.ListOpenWorkflow)
}

func listOpenWorkflowExecutions(
	ctx context.Context,
	namespace string,
	maxPageSize int32,
	workflowType string,
	listFn listOpenWorkflowsFn,
) (*workflowservice.ListOpenWorkflowExecutionsResponse, error) {
	var executions []*workflowpb.WorkflowExecutionInfo
	var nextPageToken []byte
	for {
		request := &workflowservice.ListOpenWorkflowExecutionsRequest{
			Namespace:       namespace,
			MaximumPageSize: maxPageSize,
			NextPageToken:   nextPageToken,
		}
		if workflowType != "" {
			request.Filters = &workflowservice.ListOpenWorkflowExecutionsRequest_TypeFilter{
				TypeFilter: &filterpb.WorkflowTypeFilter{Name: workflowType},
			}
		}

		openWorkflows, err := listFn(ctx, request)
		if err != nil {
			return nil, xerrors.Errorf("failed to get open workflows: %w", err)
		}
		if openWorkflows != nil {
			executions = append(executions, openWorkflows.Executions...)
			nextPageToken = openWorkflows.GetNextPageToken()
		}
		if len(nextPageToken) == 0 {
			return &workflowservice.ListOpenWorkflowExecutionsResponse{Executions: executions}, nil
		}
	}
}

func (r *runtimeImpl) RegisterWorkflow(w any, options workflow.RegisterOptions) {
	for _, worker := range r.workers {
		worker.RegisterWorkflowWithOptions(w, options)
	}
}

func (r *runtimeImpl) RegisterActivity(a any, options activity.RegisterOptions) {
	for _, worker := range r.workers {
		worker.RegisterActivityWithOptions(a, options)
	}
}

func (r *runtimeImpl) ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow any, request any) (client.WorkflowRun, error) {
	return r.workflowClient.ExecuteWorkflow(ctx, options, workflow, request)
}

func (r *runtimeImpl) ExecuteActivity(ctx workflow.Context, activity any, request any, response any) error {
	future := workflow.ExecuteActivity(ctx, activity, request)
	return future.Get(ctx, response)
}

func (r *runtimeImpl) GetLogger(ctx workflow.Context) *zap.Logger {
	logger := workflow.GetLogger(ctx)
	return log.FromTemporal(logger)
}

func (r *runtimeImpl) GetMetricsHandler(ctx workflow.Context) client.MetricsHandler {
	return workflow.GetMetricsHandler(ctx)
}

func (r *runtimeImpl) GetActivityLogger(ctx context.Context) *zap.Logger {
	logger := activity.GetLogger(ctx)
	return log.FromTemporal(logger)
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
		WorkflowExecutionRetentionPeriod: durationpb.New(retentionPeriod),
	})
	if err != nil {
		if _, ok := err.(*serviceerror.NamespaceAlreadyExists); !ok {
			return err
		}

		r.logger.Info("domain name is already registered", zap.String("domain", cadenceConfig.Domain))
	}

	describeResponse, err := retry.WrapWithResult(ctx, func(ctx context.Context) (*workflowservice.DescribeNamespaceResponse, error) {
		res, err := r.namespaceClient.Describe(ctx, cadenceConfig.Domain)
		if err != nil {
			return nil, retry.Retryable(xerrors.Errorf("failed to register cadence domain: %w", err))
		}

		return res, nil
	})
	if err != nil {
		return err
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
