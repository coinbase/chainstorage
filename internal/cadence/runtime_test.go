package cadence

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/retirement"
)

func TestNewConnectionOptionsLeavesTemporalKeepAliveUnsetByDefault(t *testing.T) {
	require := require.New(t)

	options, err := newConnectionOptions(
		config.CadenceConfig{
			Address: "temporal.example.com:7233",
		},
		config.EnvProduction,
	)

	require.NoError(err)
	require.Zero(options.KeepAliveTime)
	require.Zero(options.KeepAliveTimeout)
	require.False(options.DisableKeepAliveCheck)
	require.False(options.DisableKeepAlivePermitWithoutStream)
	require.Nil(options.TLS)
}

func TestNewConnectionOptionsAppliesConfiguredTemporalKeepAlive(t *testing.T) {
	require := require.New(t)

	options, err := newConnectionOptions(
		config.CadenceConfig{
			Address:          "temporal.example.com:7233",
			KeepAliveTime:    10 * time.Second,
			KeepAliveTimeout: 30 * time.Second,
		},
		config.EnvProduction,
	)

	require.NoError(err)
	require.Equal(10*time.Second, options.KeepAliveTime)
	require.Equal(30*time.Second, options.KeepAliveTimeout)
	require.False(options.DisableKeepAliveCheck)
	require.False(options.DisableKeepAlivePermitWithoutStream)
	require.Nil(options.TLS)
}

func TestNewConnectionOptionsPreservesTLSConfig(t *testing.T) {
	require := require.New(t)

	options, err := newConnectionOptions(
		config.CadenceConfig{
			Address:          "temporal.example.com:7233",
			KeepAliveTime:    10 * time.Second,
			KeepAliveTimeout: 30 * time.Second,
			TLSConfig: config.CadenceTLSConfig{
				Enabled:          true,
				ValidateHostname: true,
			},
		},
		config.EnvProduction,
	)

	require.NoError(err)
	require.NotNil(options.TLS)
	require.Equal(uint16(tls.VersionTLS12), options.TLS.MinVersion)
	require.Equal("temporal.example.com", options.TLS.ServerName)
	require.False(options.TLS.InsecureSkipVerify)
	require.Equal(10*time.Second, options.KeepAliveTime)
	require.Equal(30*time.Second, options.KeepAliveTimeout)
}

func TestNewConnectionOptionsSuppressesTLSConfigForLocalEnv(t *testing.T) {
	require := require.New(t)

	options, err := newConnectionOptions(
		config.CadenceConfig{
			Address: "temporal.example.com:7233",
			TLSConfig: config.CadenceTLSConfig{
				Enabled: true,
			},
		},
		config.EnvLocal,
	)

	require.NoError(err)
	require.Nil(options.TLS)
}

func TestNewConnectionOptionsRejectsInvalidCA(t *testing.T) {
	require := require.New(t)

	_, err := newConnectionOptions(
		config.CadenceConfig{
			Address: "temporal.example.com:7233",
			TLSConfig: config.CadenceTLSConfig{
				Enabled:              true,
				CertificateAuthority: "not-pem",
			},
		},
		config.EnvProduction,
	)

	require.ErrorContains(err, "failed to parse CA certificate")
}

func TestNewWorkerOptionsPreservesDefaultActivityConcurrency(t *testing.T) {
	require := require.New(t)

	options := newWorkerOptions(config.WorkerConfig{TaskList: "default"})

	require.True(options.EnableSessionWorker)
	require.Equal(2*time.Second, options.DeadlockDetectionTimeout)
	require.Equal(workerStopTimeout, options.WorkerStopTimeout)
	require.Zero(options.MaxConcurrentActivityExecutionSize)
}

func TestNewWorkerOptionsAppliesActivityConcurrencyLimit(t *testing.T) {
	require := require.New(t)

	options := newWorkerOptions(config.WorkerConfig{
		TaskList:                           "batch_consolidator",
		MaxConcurrentActivityExecutionSize: 1,
	})

	require.True(options.EnableSessionWorker)
	require.Equal(2*time.Second, options.DeadlockDetectionTimeout)
	require.Equal(workerStopTimeout, options.WorkerStopTimeout)
	require.Equal(1, options.MaxConcurrentActivityExecutionSize)
}

// TestWorkerStopTimeoutCoversRetirementClaimCleanup pins the ordering the
// claim release depends on: both bounded cleanup writes, run in sequence on
// their own deadlines, must be able to complete — or fail — before
// worker.Stop gives up waiting and the process exits (INF-1603).
func TestWorkerStopTimeoutCoversRetirementClaimCleanup(t *testing.T) {
	require.Less(t, retirement.RetirementClaimCleanupBudget, workerStopTimeout)
	require.Equal(t, 2*retirement.RetirementClaimCleanupTimeout, retirement.RetirementClaimCleanupBudget)
}

func TestListOpenWorkflowExecutionsPaginatesAndAppliesTypeFilter(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	calls := make([]*workflowservice.ListOpenWorkflowExecutionsRequest, 0, 2)
	responses := []*workflowservice.ListOpenWorkflowExecutionsResponse{
		{
			Executions: []*workflowpb.WorkflowExecutionInfo{
				{
					Execution: &commonpb.WorkflowExecution{WorkflowId: "custom-manual-workflow-id-1"},
					Type:      &commonpb.WorkflowType{Name: "workflow.batch_consolidator"},
				},
			},
			NextPageToken: []byte("page-2"),
		},
		{
			Executions: []*workflowpb.WorkflowExecutionInfo{
				{
					Execution: &commonpb.WorkflowExecution{WorkflowId: "custom-manual-workflow-id-2"},
					Type:      &commonpb.WorkflowType{Name: "workflow.batch_consolidator"},
				},
			},
		},
	}

	response, err := listOpenWorkflowExecutions(
		ctx,
		"chainstorage-prod",
		100,
		"workflow.batch_consolidator",
		func(ctx context.Context, request *workflowservice.ListOpenWorkflowExecutionsRequest) (*workflowservice.ListOpenWorkflowExecutionsResponse, error) {
			calls = append(calls, request)
			return responses[len(calls)-1], nil
		},
	)

	require.NoError(err)
	require.Len(calls, 2)
	require.Equal("chainstorage-prod", calls[0].GetNamespace())
	require.Equal(int32(100), calls[0].GetMaximumPageSize())
	require.Empty(calls[0].GetNextPageToken())
	require.Equal("workflow.batch_consolidator", calls[0].GetTypeFilter().GetName())
	require.Equal([]byte("page-2"), calls[1].GetNextPageToken())
	require.Equal("workflow.batch_consolidator", calls[1].GetTypeFilter().GetName())
	require.Len(response.GetExecutions(), 2)
	require.Equal("custom-manual-workflow-id-1", response.GetExecutions()[0].GetExecution().GetWorkflowId())
	require.Equal("custom-manual-workflow-id-2", response.GetExecutions()[1].GetExecution().GetWorkflowId())
	require.Empty(response.GetNextPageToken())
}
