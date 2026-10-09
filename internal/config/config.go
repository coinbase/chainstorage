package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/config"
	"github.com/coinbase/chainstorage/internal/utils/utils"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	Config struct {
		ConfigName     string               `mapstructure:"config_name" validate:"required"`
		StorageType    StorageType          `mapstructure:"storage_type"`
		Chain          ChainConfig          `mapstructure:"chain"`
		AWS            AwsConfig            `mapstructure:"aws"`
		GCP            *GcpConfig           `mapstructure:"gcp"`
		Cadence        CadenceConfig        `mapstructure:"cadence"`
		Workflows      WorkflowsConfig      `mapstructure:"workflows"`
		Api            ApiConfig            `mapstructure:"api"`
		SDK            SDKConfig            `mapstructure:"sdk"`
		Server         ServerConfig         `mapstructure:"server"`
		Cron           CronConfig           `mapstructure:"cron"`
		SLA            SLAConfig            `mapstructure:"sla"`
		FunctionalTest FunctionalTestConfig `mapstructure:"functional_test"`
		StatsD         *StatsDConfig        `mapstructure:"statsd"`
		Prometheus     *PrometheusConfig    `mapstructure:"prometheus"`

		namespace string
		env       Env
	}

	StorageType struct {
		BlobStorageType BlobStorageType `mapstructure:"blob"`
		MetaStorageType MetaStorageType `mapstructure:"meta"`
		DLQType         DLQType         `mapstructure:"dlq"`
	}

	BlobStorageType   int32
	MetaStorageType   int32
	DLQType           int32
	ConsolidationMode string
	StorageGeneration string

	BlockStorageConfig struct {
		// WriteGeneration selects the physical generation for all new single-block
		// and consolidated-block writes. "legacy" is the backward-compatible
		// default and is persisted as an empty generation/SQL NULL.
		WriteGeneration StorageGeneration `mapstructure:"write_generation"`
		// Generations resolves non-legacy metadata values (for example, "v2")
		// to buckets. Once referenced by a metadata row, a mapping is immutable;
		// future bucket replacements must add a new generation instead of
		// repointing an existing one.
		Generations map[string]BlockStorageGenerationConfig `mapstructure:"generations"`
	}

	BlockStorageGenerationConfig struct {
		Bucket string `mapstructure:"bucket"`
	}

	ChainConfig struct {
		Blockchain       common.Blockchain `mapstructure:"blockchain" validate:"required"`
		Network          common.Network    `mapstructure:"network" validate:"required"`
		Sidechain        api.SideChain     `mapstructure:"sidechain"`
		BlockTag         BlockTagConfig    `mapstructure:"block_tag"`
		EventTag         EventTagConfig    `mapstructure:"event_tag"`
		Client           ClientConfig      `mapstructure:"client"`
		Feature          FeatureConfig     `mapstructure:"feature"`
		BlockStartHeight uint64            `mapstructure:"block_start_height"`
		// IrreversibleDistance is the maximum distance between the current block height and the last irreversible block height, also
		// known as the max reorg distance, finalization depth. This does not have a default value and must be set by the onboarding user.
		IrreversibleDistance uint64        `mapstructure:"irreversible_distance" validate:"required"`
		Rosetta              RosettaConfig `mapstructure:"rosetta"`
		BlockTime            time.Duration `mapstructure:"block_time" validate:"required"`
		// CustomParams stores chain-specific custom parameters as key-value pairs.
		// For example, Seismic uses "src20_aes_key" for encrypted token transfer parsing.
		CustomParams map[string]string `mapstructure:"custom_params"`
	}

	ClientConfig struct {
		Master                 JSONRPCConfig     `mapstructure:"master"`
		Slave                  JSONRPCConfig     `mapstructure:"slave"`
		Validator              JSONRPCConfig     `mapstructure:"validator"`
		Consensus              JSONRPCConfig     `mapstructure:"consensus"`
		Additional             JSONRPCConfig     `mapstructure:"additional"`
		Retry                  ClientRetryConfig `mapstructure:"retry"`
		HttpTimeout            time.Duration     `mapstructure:"http_timeout"`
		TxBatchSize            int               `mapstructure:"tx_batch_size"`
		RpcTimeoutGetBlock     time.Duration     `mapstructure:"rpc_timeout_get_block"`
		RpcTimeoutGetRawTx     time.Duration     `mapstructure:"rpc_timeout_get_raw_tx"`
		RpcTimeoutGetBlockHash time.Duration     `mapstructure:"rpc_timeout_get_block_hash"`
	}

	JSONRPCConfig struct {
		EndpointGroup EndpointGroup `mapstructure:"endpoint_group"`
	}

	ClientRetryConfig struct {
		MaxAttempts int `mapstructure:"max_attempts"`
	}

	FeatureConfig struct {
		RosettaParser               bool `mapstructure:"rosetta_parser"`
		DefaultStableEvent          bool `mapstructure:"default_stable_event"`
		TransactionIndexing         bool `mapstructure:"transaction_indexing"`
		BlockValidationEnabled      bool `mapstructure:"block_validation_enabled"`
		BlockValidationMuted        bool `mapstructure:"block_validation_muted"`
		VerifiedAccountStateEnabled bool `mapstructure:"verified_account_state_enabled"`
	}

	BlockTagConfig struct {
		Stable uint32 `mapstructure:"stable"`
		Latest uint32 `mapstructure:"latest"`
	}

	EventTagConfig struct {
		Stable uint32 `mapstructure:"stable"`
		Latest uint32 `mapstructure:"latest"`
	}

	AccountConfig struct {
		User     string `json:"user"`
		Password string `json:"password"`
		Role     string `json:"role"`
	}

	AwsConfig struct {
		Region string `mapstructure:"region" validate:"required"`
		// Bucket is the immutable legacy block bucket used by metadata rows whose
		// storage generation is empty/SQL NULL.
		Bucket                 string             `mapstructure:"bucket" validate:"required"`
		BlockStorage           BlockStorageConfig `mapstructure:"block_storage"`
		Postgres               *PostgresConfig    `mapstructure:"postgres" validate:"required_without=DynamoDB"`
		DynamoDB               *DynamoDBConfig    `mapstructure:"dynamodb" validate:"required_without=Postgres"`
		IsLocalStack           bool               `mapstructure:"local_stack"`
		IsResetLocal           bool               `mapstructure:"reset_local"`
		PresignedUrlExpiration time.Duration      `mapstructure:"presigned_url_expiration" validate:"required"`
		DLQ                    SQSConfig          `mapstructure:"dlq"`
		Storage                StorageConfig      `mapstructure:"storage"`
		AWSAccount             AWSAccount         `mapstructure:"aws_account" validate:"required"`
	}

	PostgresConfig struct {
		Host             string        `mapstructure:"host" validate:"required"`
		Port             int           `mapstructure:"port" validate:"required"`
		Database         string        `mapstructure:"database" validate:"required"`
		User             string        `mapstructure:"user"`
		Password         string        `mapstructure:"password"`
		SSLMode          string        `mapstructure:"ssl_mode" validate:"required"`
		MaxConnections   int           `mapstructure:"max_connections"`
		MinConnections   int           `mapstructure:"min_connections"`
		MaxIdleTime      time.Duration `mapstructure:"max_idle_time"`
		MaxLifetime      time.Duration `mapstructure:"max_lifetime"`
		ConnectTimeout   time.Duration `mapstructure:"connect_timeout"`
		StatementTimeout time.Duration `mapstructure:"statement_timeout"`
		Schema           string        `mapstructure:"schema"`
		TablePrefix      string        `mapstructure:"table_prefix"`
	}

	GcpConfig struct {
		Project                string        `mapstructure:"project" validate:"required"`
		Bucket                 string        `mapstructure:"bucket"`
		PresignedUrlExpiration time.Duration `mapstructure:"presigned_url_expiration" validate:"required"`
	}

	DynamoDBConfig struct {
		BlockTable                    string `mapstructure:"block_table" validate:"required"`
		EventTable                    string `mapstructure:"event_table"`
		EventTableHeightIndex         string `mapstructure:"event_table_height_index"`
		VersionedEventTable           string `mapstructure:"versioned_event_table" validate:"required"`
		VersionedEventTableBlockIndex string `mapstructure:"versioned_event_table_block_index" validate:"required"`
		TransactionTable              string `mapstructure:"transaction_table"`
		Arn                           string `mapstructure:"arn"`
	}

	SQSConfig struct {
		Name                  string `mapstructure:"name" validate:"required"`
		VisibilityTimeoutSecs int64  `mapstructure:"visibility_timeout_secs"`
		DelaySecs             int64  `mapstructure:"delay_secs"`
		OwnerAccountId        string `mapstructure:"owner_account_id"`
	}

	CadenceConfig struct {
		Address          string           `mapstructure:"address" validate:"required"`
		Domain           string           `mapstructure:"domain" validate:"required"`
		RetentionPeriod  int32            `mapstructure:"retention_period" validate:"required"`
		KeepAliveTime    time.Duration    `mapstructure:"keep_alive_time"`
		KeepAliveTimeout time.Duration    `mapstructure:"keep_alive_timeout"`
		TLSConfig        CadenceTLSConfig `mapstructure:"tls" validate:"required"`
	}

	CadenceTLSConfig struct {
		Enabled              bool   `mapstructure:"enabled"`
		ValidateHostname     bool   `mapstructure:"validate_hostname"`
		CertificateAuthority string `mapstructure:"certificate_authority"`
		ClientCertificate    string `mapstructure:"client_certificate"`
		ClientPrivateKey     string `mapstructure:"client_private_key"`
	}

	WorkflowsConfig struct {
		Workers              []WorkerConfig                     `mapstructure:"workers"`
		Backfiller           BackfillerWorkflowConfig           `mapstructure:"backfiller"`
		Poller               PollerWorkflowConfig               `mapstructure:"poller"`
		Benchmarker          BenchmarkerWorkflowConfig          `mapstructure:"benchmarker"`
		Monitor              MonitorWorkflowConfig              `mapstructure:"monitor"`
		Streamer             StreamerWorkflowConfig             `mapstructure:"streamer"`
		CrossValidator       CrossValidatorWorkflowConfig       `mapstructure:"cross_validator"`
		EventBackfiller      EventBackfillerWorkflowConfig      `mapstructure:"event_backfiller"`
		Replicator           ReplicatorWorkflowConfig           `mapstructure:"replicator"`
		Migrator             MigratorWorkflowConfig             `mapstructure:"migrator"`
		BatchConsolidator    BatchConsolidatorWorkflowConfig    `mapstructure:"batch_consolidator"`
		SingleBlockRetention SingleBlockRetentionWorkflowConfig `mapstructure:"single_block_retention"`
	}

	WorkerConfig struct {
		TaskList                           string `mapstructure:"task_list"`
		MaxConcurrentActivityExecutionSize int    `mapstructure:"max_concurrent_activity_execution_size"`
	}

	WorkflowConfig struct {
		WorkflowIdentity               string         `mapstructure:"workflow_identity" validate:"required"`
		Enabled                        bool           `mapstructure:"enabled"`
		TaskList                       string         `mapstructure:"task_list" validate:"required"`
		WorkflowRunTimeout             time.Duration  `mapstructure:"workflow_run_timeout" validate:"required"`
		WorkflowRetry                  *RetryPolicy   `mapstructure:"workflow_retry"`
		ActivityScheduleToCloseTimeout time.Duration  `mapstructure:"activity_schedule_to_close_timeout" validate:"required"`
		ActivityStartToCloseTimeout    time.Duration  `mapstructure:"activity_start_to_close_timeout" validate:"required"`
		ActivityHeartbeatTimeout       time.Duration  `mapstructure:"activity_heartbeat_timeout"`
		ActivityRetry                  *RetryPolicy   `mapstructure:"activity_retry" validate:"required"`
		BlockTag                       BlockTagConfig `mapstructure:"block_tag"`
		EventTag                       EventTagConfig `mapstructure:"event_tag"`
		Storage                        StorageConfig  `mapstructure:"storage"`
		IrreversibleDistance           uint64         `mapstructure:"irreversible_distance" validate:"required"`
		FailoverEnabled                bool           `mapstructure:"failover_enabled"`
		ConsensusFailoverEnabled       bool           `mapstructure:"consensus_failover_enabled"`
		SLA                            SLAConfig      `mapstructure:"sla"`
	}

	RetryPolicy struct {
		MaximumAttempts    int32         `mapstructure:"maximum_attempts" validate:"required,gt=1"` // 1 means no retries.
		BackoffCoefficient float64       `mapstructure:"backoff_coefficient" validate:"required"`
		InitialInterval    time.Duration `mapstructure:"initial_interval" validate:"required"`
		MaximumInterval    time.Duration `mapstructure:"maximum_interval" validate:"required"`
	}

	BackfillerWorkflowConfig struct {
		WorkflowConfig          `mapstructure:",squash"`
		BatchSize               uint64 `mapstructure:"batch_size" validate:"required"`
		MiniBatchSize           uint64 `mapstructure:"mini_batch_size" validate:"required"`
		CheckpointSize          uint64 `mapstructure:"checkpoint_size" validate:"required,gtfield=BatchSize"`
		MaxReprocessedPerBatch  uint64 `mapstructure:"max_reprocessed_per_batch"`
		NumConcurrentExtractors int    `mapstructure:"num_concurrent_extractors" validate:"required"`
	}

	ReplicatorWorkflowConfig struct {
		WorkflowConfig `mapstructure:",squash"`
		BatchSize      uint64 `mapstructure:"batch_size" validate:"required"`
		MiniBatchSize  uint64 `mapstructure:"mini_batch_size" validate:"required"`
		CheckpointSize uint64 `mapstructure:"checkpoint_size" validate:"required,gtfield=BatchSize"`
		Parallelism    int    `mapstructure:"parallelism" validate:"required"`
	}

	EventBackfillerWorkflowConfig struct {
		WorkflowConfig `mapstructure:",squash"`
		BatchSize      uint64 `mapstructure:"batch_size" validate:"required"`
		CheckpointSize uint64 `mapstructure:"checkpoint_size" validate:"required,gtfield=BatchSize"`
	}

	PollerWorkflowConfig struct {
		WorkflowConfig               `mapstructure:",squash"`
		MaxBlocksToSyncPerCycle      uint64        `mapstructure:"max_blocks_to_sync_per_cycle" validate:"required"`
		CheckpointSize               uint64        `mapstructure:"checkpoint_size" validate:"required"`
		BackoffInterval              time.Duration `mapstructure:"backoff_interval"`
		Parallelism                  int           `mapstructure:"parallelism" validate:"required"`
		SessionCreationTimeout       time.Duration `mapstructure:"session_creation_timeout" validate:"required"`
		SessionEnabled               bool          `mapstructure:"session_enabled"`
		FastSync                     bool          `mapstructure:"fast_sync"`
		NumBlocksToSkip              uint64        `mapstructure:"num_blocks_to_skip"`
		TransactionsWriteParallelism int           `mapstructure:"transactions_write_parallelism"`
		ConsensusValidation          bool          `mapstructure:"consensus_validation"`
		ConsensusValidationMuted     bool          `mapstructure:"consensus_validation_muted"`
		LivenessCheckEnabled         bool          `mapstructure:"liveness_check_enabled"`
		// LivenessCheckViolationLimit is threshold for liveness check violations before poller failover is triggered.
		// time to trigger failover = LivenessCheckInterval * LivenessCheckViolationLimit
		LivenessCheckViolationLimit uint64 `mapstructure:"liveness_check_violation_limit"`
		// LivenessCheckInterval is the interval between liveness checks.
		LivenessCheckInterval time.Duration `mapstructure:"liveness_check_interval"`
	}

	BenchmarkerWorkflowConfig struct {
		WorkflowConfig                            `mapstructure:",squash"`
		ChildWorkflowExecutionStartToCloseTimeout time.Duration `mapstructure:"child_workflow_execution_start_to_close_timeout" validate:"required"`
	}

	MonitorWorkflowConfig struct {
		WorkflowConfig  `mapstructure:",squash"`
		BatchSize       uint64        `mapstructure:"batch_size" validate:"required"`
		CheckpointSize  uint64        `mapstructure:"checkpoint_size" validate:"required"`
		BackoffInterval time.Duration `mapstructure:"backoff_interval"`
		Parallelism     int           `mapstructure:"parallelism" validate:"required,gt=0"`
		BlockGapLimit   uint64        `mapstructure:"block_gap_limit" validate:"required"`
		EventGapLimit   int64         `mapstructure:"event_gap_limit" validate:"required"`
	}

	CrossValidatorWorkflowConfig struct {
		WorkflowConfig        `mapstructure:",squash"`
		BatchSize             uint64        `mapstructure:"batch_size" validate:"required"`
		CheckpointSize        uint64        `mapstructure:"checkpoint_size" validate:"required"`
		BackoffInterval       time.Duration `mapstructure:"backoff_interval"`
		Parallelism           int           `mapstructure:"parallelism" validate:"required,gt=0"`
		ValidationStartHeight uint64        `mapstructure:"validation_start_height"`
		ValidationPercentage  int           `mapstructure:"validation_percentage" validate:"min=0,max=100"`
	}

	StreamerWorkflowConfig struct {
		WorkflowConfig  `mapstructure:",squash"`
		BatchSize       uint64        `mapstructure:"batch_size" validate:"required"`
		CheckpointSize  uint64        `mapstructure:"checkpoint_size" validate:"required"`
		BackoffInterval time.Duration `mapstructure:"backoff_interval"`
	}

	MigratorWorkflowConfig struct {
		WorkflowConfig  `mapstructure:",squash"`
		BatchSize       uint64        `mapstructure:"batch_size" validate:"required"`
		MiniBatchSize   uint64        `mapstructure:"mini_batch_size"`
		CheckpointSize  uint64        `mapstructure:"checkpoint_size" validate:"required"`
		Parallelism     int           `mapstructure:"parallelism"`
		BackoffInterval time.Duration `mapstructure:"backoff_interval"`
		ContinuousSync  bool          `mapstructure:"continuous_sync"`
		SyncInterval    time.Duration `mapstructure:"sync_interval"`
		AutoResume      bool          `mapstructure:"auto_resume"`
	}

	BatchConsolidatorWorkflowConfig struct {
		WorkflowConfig     `mapstructure:",squash"`
		BatchSize          uint64 `mapstructure:"batch_size" validate:"required"`
		CheckpointSize     uint64 `mapstructure:"checkpoint_size" validate:"required,gtfield=BatchSize"`
		MaxBlocks          uint64 `mapstructure:"max_blocks"`
		HistoricalTaskList string `mapstructure:"historical_task_list"`
	}

	SingleBlockRetentionWorkflowConfig struct {
		WorkflowConfig  `mapstructure:",squash"`
		MaxObjectRanges int `mapstructure:"max_object_ranges" validate:"required,gt=0,lte=250"`
		// RowParallelism bounds how many rows the retention planner processes
		// concurrently inside one cohort activity. Zero or one is serial. The
		// cap also bounds worker memory: each Apply worker pins up to two
		// decompressed CSCB chunks.
		RowParallelism int `mapstructure:"row_parallelism" validate:"omitempty,gte=0,lte=16"`
		// RowMemoryBudgetBytes caps the decompressed CSCB chunk working set a
		// cohort may pin. The planner lowers RowParallelism until the cohort's
		// measured chunk size fits, so a chain with large blocks runs fewer
		// workers instead of exhausting the pod (INF-1330).
		RowMemoryBudgetBytes uint64 `mapstructure:"row_memory_budget_bytes"`
	}

	RosettaConfig struct {
		Blockchain              string  `mapstructure:"blockchain"`
		Network                 string  `mapstructure:"network" validate:"required_with=Blockchain"`
		BlockNotFoundErrorCodes []int32 `mapstructure:"block_not_found_error_codes"`
		EnableRawBlockApi       bool    `mapstructure:"enable_raw_block_api"`
		FromRosetta             bool    `mapstructure:"from_rosetta"`
	}

	// EndpointGroup is decoded from two different sources, so every field needs
	// both tags: encoding/json when the group arrives as a JSON string (e.g. the
	// CHAINSTORAGE_CHAIN_CLIENT_MASTER_ENDPOINT_GROUP env var, see UnmarshalText),
	// and mapstructure when it arrives as a YAML mapping. mapstructure falls back
	// to a case-insensitive field-name match, which silently drops any key with
	// an underscore in it, so the tags are not optional.
	EndpointGroup struct {
		Endpoints              []Endpoint     `json:"endpoints" mapstructure:"endpoints"`
		EndpointsFailover      []Endpoint     `json:"endpoints_failover" mapstructure:"endpoints_failover"`
		UseFailover            bool           `json:"use_failover" mapstructure:"use_failover"`
		EndpointConfig         EndpointConfig `json:"endpoint_config" mapstructure:"endpoint_config"`
		EndpointConfigFailover EndpointConfig `json:"endpoint_config_failover" mapstructure:"endpoint_config_failover"`
	}

	// endpointGroup must be in sync with EndpointGroup
	endpointGroup struct {
		Endpoints              []Endpoint     `json:"endpoints"`
		EndpointsFailover      []Endpoint     `json:"endpoints_failover"`
		UseFailover            bool           `json:"use_failover"`
		EndpointConfig         EndpointConfig `json:"endpoint_config"`
		EndpointConfigFailover EndpointConfig `json:"endpoint_config_failover"`
	}

	Endpoint struct {
		Name       string            `json:"name" mapstructure:"name"`
		ProviderID string            `json:"provider_id" mapstructure:"provider_id"`
		Url        string            `json:"url" mapstructure:"url"`
		User       string            `json:"user" mapstructure:"user"`
		Password   string            `json:"password" mapstructure:"password"`
		Weight     uint8             `json:"weight" mapstructure:"weight"`
		ExtraUrls  map[string]string `json:"extra_urls" mapstructure:"extra_urls"`
		RPS        int               `json:"rps" mapstructure:"rps"`
		// RPSCountBatch makes the rate limiter charge one token per call inside
		// a JSON-RPC batch request, for providers whose rate limit counts each
		// batched call individually. Has no effect unless RPS is set.
		RPSCountBatch bool `json:"rps_count_batch" mapstructure:"rps_count_batch"`
	}

	EndpointConfig struct {
		StickySession StickySessionConfig `json:"sticky_session" mapstructure:"sticky_session"`
		Headers       map[string]string   `json:"headers" mapstructure:"headers"`
	}

	StickySessionConfig struct {
		// The CookieHash method consistently maps a cookie value to a specific node.
		CookieHash string `json:"cookie_hash" mapstructure:"cookie_hash"`

		// The CookiePassive method persists the cookie value provided by the server.
		CookiePassive bool `json:"cookie_passive" mapstructure:"cookie_passive"`

		// The HeaderHash method consistently maps a header value to a specific node.
		HeaderHash string `json:"header_hash" mapstructure:"header_hash"`
	}

	ApiConfig struct {
		MaxNumBlocks            uint64          `mapstructure:"max_num_blocks" validate:"required"`
		MaxNumBlockFiles        uint64          `mapstructure:"max_num_block_files" validate:"required"`
		NumWorkers              uint64          `mapstructure:"num_workers" validate:"required"`
		StreamingInterval       time.Duration   `mapstructure:"streaming_interval" validate:"required"`
		StreamingBatchSize      uint64          `mapstructure:"streaming_batch_size" validate:"required"`
		StreamingMaxNoEventTime time.Duration   `mapstructure:"streaming_max_no_event_time" validate:"required"`
		Auth                    AuthConfig      `mapstructure:"auth"`
		RateLimit               RateLimitConfig `mapstructure:"rate_limit"`
	}

	SDKConfig struct {
		ChainstorageAddress string `mapstructure:"chainstorage_address" validate:"required"`
		NumWorkers          uint64 `mapstructure:"num_workers" validate:"required"`
		Restful             bool   `mapstructure:"restful"`
		AuthHeader          string `mapstructure:"auth_header"`
		AuthToken           string `mapstructure:"auth_token"`
	}

	ServerConfig struct {
		BindAddress string `mapstructure:"bind_address" validate:"required"`
	}

	CronConfig struct {
		BlockRangeSize         uint64                         `mapstructure:"block_range_size" validate:"required"`
		BatchConsolidator      BatchConsolidatorCronConfig    `mapstructure:"batch_consolidator"`
		SingleBlockRetention   SingleBlockRetentionCronConfig `mapstructure:"single_block_retention"`
		DisableDLQProcessor    bool                           `mapstructure:"disable_dlq_processor"`
		DisablePollingCanary   bool                           `mapstructure:"disable_polling_canary"`
		DisableStreamingCanary bool                           `mapstructure:"disable_streaming_canary"`
		DisableNodeCanary      bool                           `mapstructure:"disable_node_canary"`
		DisableWorkflowStatus  bool                           `mapstructure:"disable_workflow_status"`
	}

	BatchConsolidatorCronConfig struct {
		Enabled             bool          `mapstructure:"enabled"`
		Spec                string        `mapstructure:"spec"`
		Parallelism         int64         `mapstructure:"parallelism"`
		WorkflowParallelism int           `mapstructure:"workflow_parallelism"`
		DelayStartDuration  time.Duration `mapstructure:"delay_start_duration"`
		StartHeight         uint64        `mapstructure:"start_height"`
	}

	// SingleBlockRetentionCronConfig drives unattended single-block retention.
	// Enabling it moves the operator's per-run deletion approval into reviewed
	// configuration: the Approved* fields and the guard assertions are the
	// standing operator input the cron passes through verbatim, and every
	// in-workflow fail-closed gate still applies.
	SingleBlockRetentionCronConfig struct {
		Enabled             bool          `mapstructure:"enabled"`
		Spec                string        `mapstructure:"spec"`
		Parallelism         int64         `mapstructure:"parallelism"`
		DelayStartDuration  time.Duration `mapstructure:"delay_start_duration"`
		MaxObjectRanges     int           `mapstructure:"max_object_ranges" validate:"omitempty,gt=0,lte=250"`
		WorkflowParallelism int           `mapstructure:"workflow_parallelism" validate:"omitempty,gt=0,lte=20"`
		// WindowBlocks bounds each launched sweep so selection queries stay on
		// the height index; the window is anchored at the oldest due work.
		WindowBlocks uint64 `mapstructure:"window_blocks" validate:"omitempty,lte=2000000"`
		// ApprovedEndHeight zero follows the consolidation frontier and
		// requires AllowOpenEndedApproval as an explicit opt-in.
		ApprovedChain               string `mapstructure:"approved_chain"`
		ApprovedStartHeight         uint64 `mapstructure:"approved_start_height"`
		ApprovedEndHeight           uint64 `mapstructure:"approved_end_height"`
		AllowOpenEndedApproval      bool   `mapstructure:"allow_open_ended_approval"`
		DirectStorageClientsGuarded bool   `mapstructure:"direct_storage_clients_guarded"`
		SingleBlockWritersGuarded   bool   `mapstructure:"single_block_writers_guarded"`
		FallbackReadsValidated      bool   `mapstructure:"fallback_reads_validated"`
		ProductionDeleteEnabled     bool   `mapstructure:"production_delete_enabled"`
		// PersistFloorWatermark starts each tick's floor walk from the floor
		// the previous walk found (persisted as a consolidation cursor) rather
		// than from ApprovedStartHeight, and reconciles the skipped range one
		// bounded chunk per tick (INF-1571). Off by default: with it off the
		// walk re-scans every retired row above ApprovedStartHeight on every
		// tick, which grows at the retention drain rate until it crosses the
		// statement timeout and stalls retention (INF-1569).
		PersistFloorWatermark bool `mapstructure:"persist_floor_watermark"`
		// FloorWalkChunkBlocks bounds every chunk of the persisted-floor walk:
		// the walk above the floor that resolves the tick's probe start, and
		// the reconciliation walk below it. 0 selects the default. Sized so one
		// chunk of fully retired rows stays well inside the statement timeout.
		FloorWalkChunkBlocks uint64 `mapstructure:"floor_walk_chunk_blocks" validate:"omitempty,lte=5000000"`
	}

	StorageConfig struct {
		DataCompression api.Compression     `mapstructure:"data_compression"`
		Consolidation   ConsolidationConfig `mapstructure:"consolidation"`
	}

	ConsolidationConfig struct {
		Enabled                                  bool              `mapstructure:"enabled"`
		Mode                                     ConsolidationMode `mapstructure:"mode"`
		Codec                                    api.Compression   `mapstructure:"codec"`
		CodecLevel                               int               `mapstructure:"codec_level"`
		ZstdLongDistanceWindowLog                *int              `mapstructure:"zstd_long_distance_window_log"`
		MaxCompressedBytes                       uint64            `mapstructure:"max_compressed_bytes"`
		MaxUncompressedBytes                     uint64            `mapstructure:"max_uncompressed_bytes"`
		MaxBlocks                                uint64            `mapstructure:"max_blocks"`
		CompressionChunkBlocks                   uint64            `mapstructure:"compression_chunk_blocks"`
		MaxChunkCompressedBytes                  *uint64           `mapstructure:"max_chunk_compressed_bytes"`
		MaxChunkUncompressedBytes                *uint64           `mapstructure:"max_chunk_uncompressed_bytes"`
		FlushInterval                            time.Duration     `mapstructure:"flush_interval"`
		ShadowTimeout                            time.Duration     `mapstructure:"shadow_timeout"`
		MaxInflightRawBlocks                     uint64            `mapstructure:"max_inflight_raw_blocks"`
		MemoryBudgetBytes                        *uint64           `mapstructure:"memory_budget_bytes"`
		LocalSpillDir                            string            `mapstructure:"local_spill_dir"`
		LocalSpillMaxBytes                       *uint64           `mapstructure:"local_spill_max_bytes"`
		ShardSize                                uint64            `mapstructure:"shard_size"`
		MultipartThreshold                       uint64            `mapstructure:"multipart_threshold"`
		ReadShadowFirst                          bool              `mapstructure:"read_shadow_first"`
		PromotionGateHeight                      *uint64           `mapstructure:"promotion_gate_height"`
		SafePromotionLag                         *uint64           `mapstructure:"safe_promotion_lag"`
		SingleBlockObjectRetention               time.Duration     `mapstructure:"single_block_object_retention"`
		SingleBlockShadowWriteAfterSyncerCutover bool              `mapstructure:"single_block_shadow_write_after_syncer_cutover"`
	}

	SLAConfig struct {
		Tier                           int           `mapstructure:"tier" validate:"required"` // 1 for high urgency; 2 for low urgency; 3 for work in progress.
		BlockHeightDelta               uint64        `mapstructure:"block_height_delta" validate:"required"`
		BlockTimeDelta                 time.Duration `mapstructure:"block_time_delta" validate:"required"`
		TimeSinceLastBlock             time.Duration `mapstructure:"time_since_last_block" validate:"required"`
		EventHeightDelta               uint64        `mapstructure:"event_height_delta" validate:"required"`
		EventTimeDelta                 time.Duration `mapstructure:"event_time_delta" validate:"required"`
		TimeSinceLastEvent             time.Duration `mapstructure:"time_since_last_event" validate:"required"`
		OutOfSyncNodeDistance          uint64        `mapstructure:"out_of_sync_node_distance" validate:"required"`
		OutOfSyncValidatorNodeDistance uint64        `mapstructure:"out_of_sync_validator_node_distance"` // If not set, use OutOfSyncNodeDistance.
		ExpectedWorkflows              []string      `mapstructure:"expected_workflows"`
	}

	FunctionalTestConfig struct {
		SkipFunctionalTest []FunctionalTest `json:"skip_functional_test"`
	}

	FunctionalTest struct {
		ConfigName string `json:"config_name"`
	}

	AuthConfig struct {
		Clients    []AuthClient `json:"clients"`
		DefaultRPS int          `json:"default_rps"`
	}

	// authConfig must be in sync with AuthConfig.
	authConfig struct {
		Clients    []AuthClient `json:"clients"`
		DefaultRPS int          `json:"default_rps"`
	}

	AuthClient struct {
		ClientID string `json:"client_id"`
		Token    string `json:"token"`
		RPS      int    `json:"rps"`
	}

	RateLimitConfig struct {
		GlobalRPS    int `mapstructure:"global_rps"`
		PerClientRPS int `mapstructure:"per_client_rps"`
	}

	StatsDConfig struct {
		Address string `mapstructure:"address" validate:"required"`
		Prefix  string `mapstructure:"prefix"`
	}

	PrometheusConfig struct {
		// Port is the port to listen on for the metrics server.
		Port int `mapstructure:"port" validate:"required"`
		// MetricsPath is the path to listen on for the metrics server.
		MetricsPath string `mapstructure:"metrics_path"`
		// Namespace is the namespace for the metrics.
		Namespace string `mapstructure:"namespace"`
		// GlobalLabels are labels that are applied to all metrics.
		GlobalLabels map[string]string `mapstructure:"global_labels"`
		// DefaultHistogramBuckets are the default buckets for histogram metrics
		// if not specified in HistogramBuckets.
		DefaultHistogramBuckets []float64 `mapstructure:"default_histogram_buckets"`
		// HistogramBuckets are custom buckets for specific histogram metrics.
		// This allows for more granular control over the histogram buckets on a
		// per-metric basis.
		HistogramBuckets map[string][]float64 `mapstructure:"histogram_buckets"`
	}

	ConfigOption func(options *configOptions)

	Env string

	AWSAccount string

	BaseWorkflowConfig interface {
		Base() *WorkflowConfig
	}

	configOptions struct {
		Namespace  string            `validate:"required"`
		Blockchain common.Blockchain `validate:"required"`
		Network    common.Network    `validate:"required"`
		Env        Env               `validate:"required,oneof=production development local"`
		Sidechain  api.SideChain
	}

	// derivedConfig defines a callback where a config struct can override its fields based on the global config.
	// For example, WorkflowConfig implements this interface to copy the global tag into its own struct.
	derivedConfig interface {
		DeriveConfig(cfg *Config)
	}
)

var (
	_ derivedConfig = (*WorkflowConfig)(nil)
	_ derivedConfig = (*AwsConfig)(nil)
	_ derivedConfig = (*CadenceConfig)(nil)
	_ derivedConfig = (*SDKConfig)(nil)

	AWSAccountEnvMap = map[AWSAccount]Env{
		"":                    EnvLocal,
		AWSAccountDevelopment: EnvDevelopment,
		AWSAccountProduction:  EnvProduction,
	}

	AWSAccountShortMap = map[AWSAccount]string{
		AWSAccountDevelopment: "dev",
		AWSAccountProduction:  "prod",
	}

	BlobStorageType_value = map[string]int32{
		"UNSPECIFIED": 0,
		"S3":          1,
		"GCS":         2,
	}

	MetaStorageType_value = map[string]int32{
		"UNSPECIFIED": 0,
		"DYNAMODB":    1,
		"FIRESTORE":   2,
		"POSTGRES":    3,
	}

	DLQType_value = map[string]int32{
		"UNSPECIFIED": 0,
		"SQS":         1,
		"FIRESTORE":   2,
	}

	storageGenerationPattern = regexp.MustCompile(`^v[1-9][0-9]*$`)
)

const (
	EnvVarNamespace   = "CHAINSTORAGE_NAMESPACE"
	EnvVarConfigName  = "CHAINSTORAGE_CONFIG"
	EnvVarEnvironment = "CHAINSTORAGE_ENVIRONMENT"
	EnvVarConfigRoot  = "CHAINSTORAGE_CONFIG_ROOT"
	EnvVarConfigPath  = "CHAINSTORAGE_CONFIG_PATH"
	EnvVarTestType    = "TEST_TYPE"
	EnvVarCI          = "CI"

	CurrentFileName = "/internal/config/config.go"

	DefaultNamespace  = "chainstorage"
	DefaultConfigName = "ethereum-mainnet"

	EnvBase        Env = "base"
	EnvLocal       Env = "local"
	EnvDevelopment Env = "development"
	EnvProduction  Env = "production"
	envSecrets     Env = "secrets" // secrets.yml is merged into the env-specific config

	BlobStorageType_UNSPECIFIED BlobStorageType = 0
	BlobStorageType_S3          BlobStorageType = 1
	BlobStorageType_GCS         BlobStorageType = 2

	MetaStorageType_UNSPECIFIED MetaStorageType = 0
	MetaStorageType_DYNAMODB    MetaStorageType = 1
	MetaStorageType_FIRESTORE   MetaStorageType = 2
	MetaStorageType_POSTGRES    MetaStorageType = 3

	DLQType_UNSPECIFIED DLQType = 0
	DLQType_SQS         DLQType = 1
	DLQType_FIRESTORE   DLQType = 2

	ConsolidationModeSingleBlockOnly ConsolidationMode = "single_block_only"
	ConsolidationModeShadowDualWrite ConsolidationMode = "shadow_dual_write"
	ConsolidationModeAutoConsolidate ConsolidationMode = "auto_consolidate"
	// Historical backfill is the manual catch-up mode. It remains separate from
	// auto_consolidate, which is the normal serial cron mode.
	ConsolidationModeHistoricalBackfill        ConsolidationMode = "historical_backfill"
	ConsolidationModePromoteFinalized          ConsolidationMode = "promote_finalized"
	ConsolidationModeRepairExistingCSCB        ConsolidationMode = "repair_existing_cscb"
	ConsolidationModeSyncerConsolidatedPrimary ConsolidationMode = "syncer_consolidated_primary"
	// DefaultSingleBlockObjectRetention is how long a single-block object
	// outlives consolidation into a CSCB chunk. It is a repair window: the
	// single-block originals are the only source a corrupt chunk can be
	// rebuilt from, so 7 days covers a weekend plus a holiday, where 3 days
	// let a Friday-evening problem age out before anyone looked on Monday.
	DefaultSingleBlockObjectRetention = 168 * time.Hour

	StorageGenerationLegacy            StorageGeneration = ""
	storageGenerationLegacyConfigValue                   = "legacy"

	AWSAccountDevelopment AWSAccount = "development"
	AWSAccountProduction  AWSAccount = "production"

	placeholderPassword      = "<placeholder>"
	tagBlockchain            = "blockchain"
	tagNetwork               = "network"
	tagTier                  = "tier"
	s3BucketFormat           = "example-chainstorage-%v-%v"
	cadenceAddressLocal      = "localhost:7233"
	chainstorageAddressLocal = "http://localhost:9090"
	tagSidechain             = "sidechain"
)

func (m ConsolidationMode) IsAutoConsolidate() bool {
	return m == ConsolidationModeAutoConsolidate
}

func (m ConsolidationMode) IsShadowWrite() bool {
	return m == ConsolidationModeShadowDualWrite ||
		m == ConsolidationModeAutoConsolidate ||
		m == ConsolidationModeHistoricalBackfill
}

func (m ConsolidationMode) IsRepairExistingCSCB() bool {
	return m == ConsolidationModeRepairExistingCSCB
}

func New(opts ...ConfigOption) (*Config, error) {
	validate := validator.New()

	// Get configname, such as "ethereum-mainnet"
	configName := getConfigName()

	// Get "blockchain", "network" "env"
	configOpts, err := getConfigOptions(configName, opts...)
	if err != nil {
		return nil, xerrors.Errorf("failed to get config options %w", err)
	}

	if err := validate.Struct(configOpts); err != nil {
		return nil, xerrors.Errorf("failed to validate config options: %w", err)
	}

	// Get data in base.yml for the target blockchain-network-env
	configReader, err := getConfigData(configOpts.Namespace, EnvBase, configOpts.Blockchain, configOpts.Network, configOpts.Sidechain)
	if err != nil {
		return nil, xerrors.Errorf("failed to locate config file: %w", err)
	}

	cfg := Config{
		namespace: configOpts.Namespace,
		env:       configOpts.Env,
	}

	v := viper.New()
	// First, read the data in base.yml
	v.SetConfigName(string(EnvBase))
	v.SetConfigType("yaml")
	v.AutomaticEnv()
	v.AllowEmptyEnv(true)
	// All env set by codeflow has the prefix of CHAINSTORAGE
	v.SetEnvPrefix("CHAINSTORAGE")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Set default values.
	// Note that the default values may be overridden by environment variable or config file.
	if cfg.Env() == EnvLocal {
		v.SetDefault("aws.local_stack", true)
	}
	if cfg.IsTest() {
		v.SetDefault("aws.local_stack", true)
		v.SetDefault("aws.reset_local", true)
	}
	v.SetDefault("chain.client.tx_batch_size", 100)
	v.SetDefault("cron.batch_consolidator.enabled", false)
	v.SetDefault("cron.batch_consolidator.spec", "@every 30m")
	v.SetDefault("cron.batch_consolidator.parallelism", 1)
	v.SetDefault("cron.batch_consolidator.delay_start_duration", "1m")
	v.SetDefault("aws.storage.consolidation.enabled", false)
	v.SetDefault("aws.storage.consolidation.mode", string(ConsolidationModeSingleBlockOnly))
	v.SetDefault("aws.block_storage.write_generation", storageGenerationLegacyConfigValue)
	v.SetDefault("aws.storage.consolidation.codec", "ZSTD")
	v.SetDefault("aws.storage.consolidation.codec_level", 6)
	v.SetDefault("aws.storage.consolidation.max_compressed_bytes", 2147483648)
	v.SetDefault("aws.storage.consolidation.max_uncompressed_bytes", 137438953472)
	v.SetDefault("aws.storage.consolidation.max_blocks", 1000)
	v.SetDefault("aws.storage.consolidation.compression_chunk_blocks", 10)
	v.SetDefault("aws.storage.consolidation.flush_interval", "1m")
	v.SetDefault("aws.storage.consolidation.shadow_timeout", "30s")
	v.SetDefault("aws.storage.consolidation.max_inflight_raw_blocks", 4)
	v.SetDefault("aws.storage.consolidation.memory_budget_bytes", 268435456)
	v.SetDefault("aws.storage.consolidation.local_spill_dir", "/tmp/chainstorage-cscb")
	v.SetDefault("aws.storage.consolidation.shard_size", 10000)
	v.SetDefault("aws.storage.consolidation.multipart_threshold", 134217728)
	v.SetDefault("aws.storage.consolidation.read_shadow_first", false)
	v.SetDefault("aws.storage.consolidation.single_block_object_retention", DefaultSingleBlockObjectRetention.String())
	if err := v.BindEnv("aws.storage.consolidation.max_chunk_uncompressed_bytes"); err != nil {
		return nil, xerrors.Errorf("failed to bind max_chunk_uncompressed_bytes env: %w", err)
	}
	if err := v.BindEnv("aws.storage.consolidation.promotion_gate_height"); err != nil {
		return nil, xerrors.Errorf("failed to bind promotion_gate_height env: %w", err)
	}
	if err := v.BindEnv("aws.storage.consolidation.safe_promotion_lag"); err != nil {
		return nil, xerrors.Errorf("failed to bind safe_promotion_lag env: %w", err)
	}

	// Read the data in base.yml
	if err := v.ReadConfig(configReader); err != nil {
		return nil, xerrors.Errorf("failed to read config: %w", err)
	}

	// Merge in the env-specific config, such as development.yml
	if err := mergeInConfig(v, configOpts, configOpts.Env); err != nil {
		return nil, xerrors.Errorf("failed to merge in %v config: %w", configOpts.Env, err)
	}

	// Merge in .secrets.yml. Note that this is a no-op for development and production env.
	if err := mergeInConfig(v, configOpts, envSecrets); err != nil {
		return nil, xerrors.Errorf("failed to merge in %v config: %w", envSecrets, err)
	}
	applyDeprecatedConsolidationAliases(v)

	if err := v.Unmarshal(&cfg, viper.DecodeHook(mapstructure.ComposeDecodeHookFunc(
		mapstructure.TextUnmarshallerHookFunc(),
		mapstructure.StringToTimeDurationHookFunc(),
		mapstructure.StringToSliceHookFunc(","),
		stringToBlobStorageTypeHookFunc(),
		stringToMetaStorageTypeHookFunc(),
		stringToDLQTypeHookFunc(),
		stringToBlockchainHookFunc(),
		stringToNetworkHookFunc(),
		stringToCompressionHookFunc(),
		stringToSidechainHookFunc(),
	))); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal config: %w", err)
	}
	if cfg.AWS.Storage.Consolidation.Mode == deprecatedConsolidationModeBeforeSingleBlockRename {
		cfg.AWS.Storage.Consolidation.Mode = ConsolidationModeSingleBlockOnly
	}

	cfg.setDerivedConfigs(reflect.ValueOf(&cfg))

	if err := validate.Struct(&cfg); err != nil {
		return nil, xerrors.Errorf("failed to validate config: %w", err)
	}
	if err := cfg.validateStorageGenerationConfig(); err != nil {
		return nil, xerrors.Errorf("failed to validate storage generation config: %w", err)
	}
	if err := cfg.validateConsolidationConfig(); err != nil {
		return nil, xerrors.Errorf("failed to validate consolidation config: %w", err)
	}

	if cfg.Chain.Blockchain != common.Blockchain_BLOCKCHAIN_ETHEREUM || cfg.Chain.Network != common.Network_NETWORK_ETHEREUM_MAINNET {
		// Zero-value blockTag is reserved as an alias of the stable blockTag.
		// Other than ethereum/mainnet (whose tags actually started from zero), do not allow zero-value blockTag.
		if cfg.Chain.BlockTag.Stable == 0 {
			return nil, xerrors.New("stable block tag cannot be zero")
		}

		if cfg.Chain.BlockTag.Latest == 0 {
			return nil, xerrors.New("latest block tag cannot be zero")
		}
	}

	return &cfg, nil
}

const (
	consolidationSingleBlockObjectRetentionKey               = "aws.storage.consolidation.single_block_object_retention"
	consolidationSingleBlockShadowWriteAfterSyncerCutoverKey = "aws.storage.consolidation.single_block_shadow_write_after_syncer_cutover"

	// Deprecated aliases are accepted only at the configuration boundary so a
	// rolling deployment can consume the previous production configuration.
	deprecatedConsolidationModeBeforeSingleBlockRename               ConsolidationMode = "legacy_only"
	deprecatedConsolidationObjectRetentionKeyBeforeSingleBlockRename                   = "aws.storage.consolidation.legacy_object_retention"
	deprecatedConsolidationShadowWriteKeyBeforeSingleBlockRename                       = "aws.storage.consolidation.legacy_shadow_write_after_syncer_cutover"
)

func applyDeprecatedConsolidationAliases(v *viper.Viper) {
	applyDeprecatedConfigAlias(
		v,
		consolidationSingleBlockObjectRetentionKey,
		deprecatedConsolidationObjectRetentionKeyBeforeSingleBlockRename,
	)
	applyDeprecatedConfigAlias(
		v,
		consolidationSingleBlockShadowWriteAfterSyncerCutoverKey,
		deprecatedConsolidationShadowWriteKeyBeforeSingleBlockRename,
	)
}

func applyDeprecatedConfigAlias(v *viper.Viper, canonicalKey string, deprecatedKey string) {
	canonicalEnv := "CHAINSTORAGE_" + strings.ToUpper(strings.ReplaceAll(canonicalKey, ".", "_"))
	if _, ok := os.LookupEnv(canonicalEnv); ok {
		return
	}

	deprecatedEnv := "CHAINSTORAGE_" + strings.ToUpper(strings.ReplaceAll(deprecatedKey, ".", "_"))
	if value, ok := os.LookupEnv(deprecatedEnv); ok {
		v.Set(canonicalKey, value)
		return
	}

	if !v.InConfig(canonicalKey) && v.InConfig(deprecatedKey) {
		v.Set(canonicalKey, v.Get(deprecatedKey))
	}
}

func (c *Config) validateStorageGenerationConfig() error {
	writeGeneration, err := normalizeConfiguredStorageGeneration(c.AWS.BlockStorage.WriteGeneration)
	if err != nil {
		return xerrors.Errorf("invalid aws.block_storage.write_generation: %w", err)
	}

	seenBuckets := map[string]string{c.AWS.Bucket: storageGenerationLegacyConfigValue}
	for generation, location := range c.AWS.BlockStorage.Generations {
		if !storageGenerationPattern.MatchString(generation) {
			return xerrors.Errorf("invalid aws.block_storage.generations key %q", generation)
		}
		if strings.TrimSpace(location.Bucket) != location.Bucket || location.Bucket == "" {
			return xerrors.Errorf("aws.block_storage.generations.%s.bucket must be non-empty without surrounding whitespace", generation)
		}
		if previousGeneration, ok := seenBuckets[location.Bucket]; ok {
			return xerrors.Errorf(
				"aws.block_storage generations %q and %q must use different buckets",
				previousGeneration,
				generation,
			)
		}
		seenBuckets[location.Bucket] = generation
	}

	if len(c.AWS.BlockStorage.Generations) > 0 {
		switch c.StorageType.BlobStorageType {
		case BlobStorageType_UNSPECIFIED, BlobStorageType_S3:
		default:
			return xerrors.Errorf("non-legacy block storage generations require S3 blob storage, got %v", c.StorageType.BlobStorageType)
		}
		if c.StorageType.MetaStorageType != MetaStorageType_POSTGRES {
			return xerrors.Errorf("non-legacy block storage generations require Postgres meta storage, got %v", c.StorageType.MetaStorageType)
		}
	}

	if writeGeneration == StorageGenerationLegacy {
		return nil
	}
	if _, ok := c.AWS.BlockStorage.Generations[string(writeGeneration)]; !ok {
		return xerrors.Errorf("aws.block_storage.write_generation=%s is not configured in aws.block_storage.generations", writeGeneration)
	}
	return nil
}

func normalizeConfiguredStorageGeneration(generation StorageGeneration) (StorageGeneration, error) {
	if generation == StorageGenerationLegacy || generation == storageGenerationLegacyConfigValue {
		return StorageGenerationLegacy, nil
	}
	if !storageGenerationPattern.MatchString(string(generation)) {
		return StorageGenerationLegacy, xerrors.Errorf("unsupported storage generation %q", generation)
	}
	return generation, nil
}

func (c *Config) WriteBlockStorageGeneration() (string, error) {
	generation, err := normalizeConfiguredStorageGeneration(c.AWS.BlockStorage.WriteGeneration)
	if err != nil {
		return "", xerrors.Errorf("invalid aws.block_storage.write_generation: %w", err)
	}
	if generation != StorageGenerationLegacy {
		if _, ok := c.AWS.BlockStorage.Generations[string(generation)]; !ok {
			return "", xerrors.Errorf("aws.block_storage.write_generation=%s is not configured", generation)
		}
	}
	return string(generation), nil
}

func (c *Config) ResolveBlockStorageBucket(generation string) (string, error) {
	if generation == string(StorageGenerationLegacy) {
		return c.AWS.Bucket, nil
	}
	if !storageGenerationPattern.MatchString(generation) {
		return "", xerrors.Errorf("unsupported block storage generation %q", generation)
	}
	location, ok := c.AWS.BlockStorage.Generations[generation]
	if !ok {
		return "", xerrors.Errorf("block metadata requires unconfigured storage generation %q", generation)
	}
	if strings.TrimSpace(location.Bucket) != location.Bucket || location.Bucket == "" {
		return "", xerrors.Errorf("block metadata storage generation %q has an invalid bucket", generation)
	}
	return location.Bucket, nil
}

func (c *Config) WriteBlockStorageBucket() (string, error) {
	generation, err := c.WriteBlockStorageGeneration()
	if err != nil {
		return "", err
	}
	return c.ResolveBlockStorageBucket(generation)
}

func (c *Config) validateConsolidationConfig() error {
	consolidation := c.AWS.Storage.Consolidation
	switch consolidation.Mode {
	case ConsolidationModeSingleBlockOnly,
		ConsolidationModeShadowDualWrite,
		ConsolidationModeAutoConsolidate,
		ConsolidationModeHistoricalBackfill,
		ConsolidationModePromoteFinalized,
		ConsolidationModeSyncerConsolidatedPrimary:
	default:
		return xerrors.Errorf("invalid consolidation mode %q", consolidation.Mode)
	}

	switch consolidation.Codec {
	case api.Compression_GZIP, api.Compression_ZSTD:
	default:
		return xerrors.Errorf("invalid consolidation codec %v", consolidation.Codec)
	}

	if !consolidation.Enabled && !consolidation.ReadShadowFirst {
		return nil
	}

	switch c.StorageType.BlobStorageType {
	case BlobStorageType_UNSPECIFIED, BlobStorageType_S3:
	default:
		return xerrors.Errorf("consolidation requires S3 blob storage, got %v", c.StorageType.BlobStorageType)
	}
	if c.StorageType.MetaStorageType != MetaStorageType_POSTGRES {
		return xerrors.Errorf("consolidation requires Postgres meta storage until DynamoDB shadow placement and guarded promotion are implemented, got %v", c.StorageType.MetaStorageType)
	}

	if !consolidation.Enabled {
		return nil
	}

	if consolidation.CodecLevel <= 0 {
		return xerrors.New("consolidation codec_level must be positive")
	}
	if consolidation.MaxCompressedBytes == 0 {
		return xerrors.New("consolidation max_compressed_bytes must be positive")
	}
	if consolidation.MaxUncompressedBytes == 0 {
		return xerrors.New("consolidation max_uncompressed_bytes must be positive")
	}
	if consolidation.MaxBlocks == 0 {
		return xerrors.New("consolidation max_blocks must be positive")
	}
	if consolidation.CompressionChunkBlocks == 0 {
		return xerrors.New("consolidation compression_chunk_blocks must be positive")
	}
	if consolidation.MaxChunkUncompressedBytes != nil && *consolidation.MaxChunkUncompressedBytes == 0 {
		return xerrors.New("consolidation max_chunk_uncompressed_bytes must be positive when set")
	}
	if consolidation.ShardSize == 0 {
		return xerrors.New("consolidation shard_size must be positive")
	}
	if consolidation.MultipartThreshold == 0 {
		return xerrors.New("consolidation multipart_threshold must be positive")
	}
	if consolidation.MaxInflightRawBlocks == 0 {
		return xerrors.New("consolidation max_inflight_raw_blocks must be positive")
	}
	if consolidation.MemoryBudgetBytes != nil && *consolidation.MemoryBudgetBytes == 0 {
		return xerrors.New("consolidation memory_budget_bytes must be positive when set")
	}
	if consolidation.LocalSpillMaxBytes != nil && *consolidation.LocalSpillMaxBytes == 0 {
		return xerrors.New("consolidation local_spill_max_bytes must be positive when set")
	}
	if consolidation.FlushInterval <= 0 {
		return xerrors.New("consolidation flush_interval must be positive")
	}
	if consolidation.ShadowTimeout <= 0 {
		return xerrors.New("consolidation shadow_timeout must be positive")
	}
	if consolidation.SingleBlockObjectRetention <= 0 {
		return xerrors.New("consolidation single_block_object_retention must be positive")
	}
	if consolidation.Mode == ConsolidationModePromoteFinalized && consolidation.SafePromotionLag == nil {
		return xerrors.New("consolidation promote_finalized requires safe_promotion_lag")
	}
	if consolidation.SafePromotionLag != nil && *consolidation.SafePromotionLag < c.Chain.IrreversibleDistance {
		return xerrors.Errorf(
			"consolidation safe_promotion_lag(%d) must be at least irreversible_distance(%d)",
			*consolidation.SafePromotionLag,
			c.Chain.IrreversibleDistance,
		)
	}
	return nil
}

func GetEnv() Env {
	awsAccount := AWSAccount(os.Getenv(EnvVarEnvironment))
	env, ok := AWSAccountEnvMap[awsAccount]
	if !ok {
		return EnvLocal
	}

	return env
}

func getConfigName() string {
	configName, ok := os.LookupEnv(EnvVarConfigName)
	if !ok {
		configName = DefaultConfigName
	}
	return configName
}

func GetConfigRoot() string {
	return os.Getenv(EnvVarConfigRoot)
}

func GetConfigPath() string {
	return os.Getenv(EnvVarConfigPath)
}

func mergeInConfig(v *viper.Viper, configOpts *configOptions, env Env) error {
	// Merge in the env-specific config if available.
	if configReader, err := getConfigData(configOpts.Namespace, env, configOpts.Blockchain, configOpts.Network, configOpts.Sidechain); err == nil {
		v.SetConfigName(string(env))
		if err := v.MergeConfig(configReader); err != nil {
			return xerrors.Errorf("failed to merge config %v: %w", env, err)
		}
	}
	return nil
}

func (c *Config) Namespace() string {
	return c.namespace
}

func (c *Config) Env() Env {
	return c.env
}

func (c *Config) Blockchain() common.Blockchain {
	return c.Chain.Blockchain
}

func (c *Config) Network() common.Network {
	return c.Chain.Network
}

func (c *Config) Sidechain() api.SideChain {
	return c.Chain.Sidechain
}

func (c *Config) Tier() int {
	return c.SLA.Tier
}

func (c *Config) GetCommonTags() map[string]string {
	return map[string]string{
		tagBlockchain: c.Blockchain().GetName(),
		tagNetwork:    c.Network().GetName(),
		tagTier:       strconv.Itoa(c.Tier()),
		tagSidechain:  c.Sidechain().GetName(),
	}
}

func (c *Config) AwsEnv() string {
	shortEnv := "dev"
	if val, exists := AWSAccountShortMap[c.AWS.AWSAccount]; exists {
		shortEnv = val
	}
	return shortEnv
}

func (c *Config) normalizeResourceName(name string) string {
	return strings.ReplaceAll(name, "_", "-")
}

func (c *Config) IsTest() bool {
	return os.Getenv(EnvVarTestType) != ""
}

func (c *Config) IsIntegrationTest() bool {
	return os.Getenv(EnvVarTestType) == "integration"
}

func (c *Config) IsFunctionalTest() bool {
	return os.Getenv(EnvVarTestType) == "functional"
}

func (c *Config) IsCI() bool {
	return os.Getenv(EnvVarCI) != ""
}

func (c *Config) GetEffectiveBlockTag(tag uint32) uint32 {
	return c.Chain.BlockTag.GetEffectiveBlockTag(tag)
}

func (c *Config) GetStableBlockTag() uint32 {
	return c.Chain.BlockTag.Stable
}

func (c *Config) GetLatestBlockTag() uint32 {
	return c.Chain.BlockTag.Latest
}

func (c *Config) IsRosetta() bool {
	return c.Chain.Rosetta.Blockchain != "" && c.Chain.Rosetta.Network != ""
}

// setDerivedConfigs recursively calls DeriveConfig on all the derivedConfig.
func (c *Config) setDerivedConfigs(v reflect.Value) {
	if v.CanInterface() {
		if oc, ok := v.Interface().(derivedConfig); ok {
			oc.DeriveConfig(c)
			return
		}
	}

	elem := v.Elem()
	for i := 0; i < elem.NumField(); i++ {
		field := elem.Field(i)
		if field.Kind() == reflect.Struct {
			c.setDerivedConfigs(field.Addr())
		}
	}
}

func (c *Config) GetLatestEventTag() uint32 {
	return c.Chain.EventTag.Latest
}

func (c *Config) GetStableEventTag() uint32 {
	return c.Chain.EventTag.Stable
}

func (c *Config) GetEffectiveEventTag(eventTag uint32) uint32 {
	return c.Chain.EventTag.GetEffectiveEventTag(eventTag)
}

func (c *Config) GetChainMetadataHelper(req *api.GetChainMetadataRequest) (*api.GetChainMetadataResponse, error) {
	return &api.GetChainMetadataResponse{
		LatestBlockTag:       c.GetLatestBlockTag(),
		StableBlockTag:       c.GetStableBlockTag(),
		LatestEventTag:       c.GetLatestEventTag(),
		StableEventTag:       c.GetStableEventTag(),
		BlockStartHeight:     c.Chain.BlockStartHeight,
		IrreversibleDistance: c.Chain.IrreversibleDistance,
		BlockTime:            c.Chain.BlockTime.String(),
	}, nil
}

func WithNamespace(namespace string) ConfigOption {
	return func(opts *configOptions) {
		opts.Namespace = namespace
	}
}

func WithBlockchain(blockchain common.Blockchain) ConfigOption {
	return func(opts *configOptions) {
		opts.Blockchain = blockchain
	}
}

func WithNetwork(network common.Network) ConfigOption {
	return func(opts *configOptions) {
		opts.Network = network
	}
}

func WithSidechain(sidechain api.SideChain) ConfigOption {
	return func(opts *configOptions) {
		opts.Sidechain = sidechain
	}
}

func WithEnvironment(env Env) ConfigOption {
	return func(opts *configOptions) {
		opts.Env = env
	}
}

func getConfigOptions(configName string, opts ...ConfigOption) (*configOptions, error) {
	configOpts := &configOptions{}
	for _, opt := range opts {
		opt(configOpts)
	}

	if configOpts.Namespace == "" {
		namespace := os.Getenv(EnvVarNamespace)
		if namespace == "" {
			namespace = DefaultNamespace
		}

		configOpts.Namespace = namespace
	}

	if configOpts.Env == "" {
		configOpts.Env = GetEnv()
	}

	if configOpts.Blockchain == common.Blockchain_BLOCKCHAIN_UNKNOWN && configOpts.Network == common.Network_NETWORK_UNKNOWN {
		blockchain, network, sidechain, err := ParseConfigName(configName)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse config name: %w", err)
		}

		configOpts.Blockchain = blockchain
		configOpts.Network = network
		configOpts.Sidechain = sidechain
	}

	return configOpts, nil
}

func ParseConfigName(configName string) (common.Blockchain, common.Network, api.SideChain, error) {
	// Normalize the config name by replacing "-" with "_".
	configName = strings.ReplaceAll(configName, "-", "_")

	splitString := strings.Split(configName, "_")
	if len(splitString) < 2 || len(splitString) > 3 {
		return common.Blockchain_BLOCKCHAIN_UNKNOWN, common.Network_NETWORK_UNKNOWN, api.SideChain_SIDECHAIN_NONE, xerrors.Errorf("config name is invalid: %v", configName)
	}

	blockchainName := splitString[0]
	blockchain, err := utils.ParseBlockchain(blockchainName)
	if err != nil {
		return common.Blockchain_BLOCKCHAIN_UNKNOWN, common.Network_NETWORK_UNKNOWN, api.SideChain_SIDECHAIN_NONE, xerrors.Errorf("failed to parse blockchain from config name %v: %w", configName, err)
	}

	networkName := fmt.Sprintf("%v_%v", splitString[0], splitString[1])
	network, err := utils.ParseNetwork(networkName)
	if err != nil {
		return common.Blockchain_BLOCKCHAIN_UNKNOWN, common.Network_NETWORK_UNKNOWN, api.SideChain_SIDECHAIN_NONE, xerrors.Errorf("failed to parse network from config name %v: %w", configName, err)
	}

	if len(splitString) == 3 {
		sidechainName := fmt.Sprintf("%v_%v_%v", splitString[0], splitString[1], splitString[2])
		sidechain, err := utils.ParseSidechain(sidechainName)
		if err != nil {
			return common.Blockchain_BLOCKCHAIN_UNKNOWN, common.Network_NETWORK_UNKNOWN, api.SideChain_SIDECHAIN_NONE, xerrors.Errorf("failed to parse sidechain from config name %v: %w", configName, err)
		}

		return blockchain, network, sidechain, nil
	}

	return blockchain, network, api.SideChain_SIDECHAIN_NONE, nil
}

func getConfigData(namespace string, env Env, blockchain common.Blockchain, network common.Network, sidechain api.SideChain) (io.Reader, error) {
	blockchainName := blockchain.GetName()
	networkName := strings.TrimPrefix(network.GetName(), blockchainName+"-")
	sidechainName := strings.TrimPrefix(sidechain.GetName(), blockchainName+"-"+networkName+"-")

	configRoot := GetConfigRoot()
	if env == envSecrets {
		// .secrets.yml is intentionally not embedded in config.Store.
		// Read it from the file system instead.
		// If configRoot is not set, use the default path.
		if len(configRoot) == 0 {
			_, filename, _, ok := runtime.Caller(0)
			if !ok {
				return nil, xerrors.Errorf("failed to recover the filename information")
			}
			rootDir := strings.TrimSuffix(filename, CurrentFileName)
			configRoot = fmt.Sprintf("%v/config", rootDir)
		}
		configPath := fmt.Sprintf("%v/%v/%v/%v/.secrets.yml", configRoot, namespace, blockchainName, networkName)
		if sidechain != api.SideChain_SIDECHAIN_NONE {
			configPath = fmt.Sprintf("%v/%v/%v/%v/%v/.secrets.yml", configRoot, namespace, blockchainName, networkName, sidechainName)
		}
		reader, err := os.Open(configPath)
		if err != nil {
			return nil, xerrors.Errorf("failed to read config file %v: %w", configPath, err)
		}
		return reader, nil
	}

	configPath := GetConfigPath()
	// If configPath is not set, try to construct the file system path from configRoot.
	if len(configPath) == 0 && len(configRoot) > 0 {
		configPath = fmt.Sprintf("%v/%v/%v/%v/%v.yml", configRoot, namespace, blockchainName, networkName, env)
		if sidechain != api.SideChain_SIDECHAIN_NONE {
			configPath = fmt.Sprintf("%v/%v/%v/%v/%v/%v.yml", configRoot, namespace, blockchainName, networkName, sidechainName, env)
		}
	}

	// If either configRoot or configPath is set, read the config from the file system.
	if len(configPath) > 0 {
		reader, err := os.Open(configPath)
		if err != nil {
			return nil, xerrors.Errorf("failed to read config file %v: %w", configPath, err)
		}
		return reader, nil
	}

	// Read the config from the embedded config.Store.
	configPath = fmt.Sprintf("%v/%v/%v/%v.yml", namespace, blockchainName, networkName, env)
	if sidechain != api.SideChain_SIDECHAIN_NONE {
		configPath = fmt.Sprintf("%v/%v/%v/%v/%v.yml", namespace, blockchainName, networkName, sidechainName, env)
	}

	data, err := config.Store.ReadFile(configPath)
	if err != nil {
		return nil, xerrors.Errorf("failed to read config file %v: %w", configPath, err)
	}
	return bytes.NewBuffer(data), nil
}

func keysWithoutUnspecified[V interface{}](m map[string]V) []string {
	var keys []string
	for k := range m {
		if k != "UNSPECIFIED" {
			keys = append(keys, k)
		}
	}
	return keys
}

func stringToBlobStorageTypeHookFunc() mapstructure.DecodeHookFunc {
	return func(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}

		if t != reflect.TypeOf(BlobStorageType_UNSPECIFIED) {
			return data, nil
		}

		v, ok := BlobStorageType_value[data.(string)]
		if !ok {
			return nil, xerrors.Errorf(
				"invalid blob storage type: %v, possible values are: %v",
				data, strings.Join(keysWithoutUnspecified(BlobStorageType_value), ", "))
		}

		return v, nil
	}
}

func stringToMetaStorageTypeHookFunc() mapstructure.DecodeHookFunc {
	return func(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}

		if t != reflect.TypeOf(MetaStorageType_UNSPECIFIED) {
			return data, nil
		}

		v, ok := MetaStorageType_value[data.(string)]
		if !ok {
			return nil, xerrors.Errorf(
				"invalid blob storage type: %v, possible values are: %v",
				data, strings.Join(keysWithoutUnspecified(MetaStorageType_value), ", "))
		}

		return v, nil
	}
}

func stringToDLQTypeHookFunc() mapstructure.DecodeHookFunc {
	return func(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}

		if t != reflect.TypeOf(DLQType_UNSPECIFIED) {
			return data, nil
		}

		v, ok := DLQType_value[data.(string)]
		if !ok {
			return nil, xerrors.Errorf(
				"invalid dlq type: %v, possible values are: %v",
				data, strings.Join(keysWithoutUnspecified(DLQType_value), ", "))
		}

		return v, nil
	}
}

func stringToBlockchainHookFunc() mapstructure.DecodeHookFunc {
	return func(f reflect.Type, t reflect.Type, data any) (any, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}

		if t != reflect.TypeOf(common.Blockchain_BLOCKCHAIN_UNKNOWN) {
			return data, nil
		}

		return common.Blockchain_value[data.(string)], nil
	}
}

func stringToNetworkHookFunc() mapstructure.DecodeHookFunc {
	return func(f reflect.Type, t reflect.Type, data any) (any, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}

		if t != reflect.TypeOf(common.Network_NETWORK_UNKNOWN) {
			return data, nil
		}

		return common.Network_value[data.(string)], nil
	}
}

func stringToCompressionHookFunc() mapstructure.DecodeHookFunc {
	return func(f reflect.Type, t reflect.Type, data any) (any, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}

		if t != reflect.TypeOf(api.Compression_NONE) {
			return data, nil
		}

		v, ok := api.Compression_value[strings.ToUpper(data.(string))]
		if !ok {
			return nil, xerrors.Errorf("invalid compression type: %v", data)
		}
		return v, nil
	}
}

func stringToSidechainHookFunc() mapstructure.DecodeHookFunc {
	return func(f reflect.Type, t reflect.Type, data any) (any, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}

		if t != reflect.TypeOf(api.SideChain_SIDECHAIN_NONE) {
			return data, nil
		}

		return api.SideChain_value[data.(string)], nil
	}
}

func (f *FunctionalTestConfig) UnmarshalText(text []byte) error {
	if len(text) == 0 {
		return nil
	}

	var ft struct {
		SkipFunctionalTest []FunctionalTest `json:"skip_functional_test"`
	}

	err := json.Unmarshal(text, &ft)
	if err != nil {
		return xerrors.Errorf("failed to parse FunctionalTestConfig JSON: %w", err)
	}
	f.SkipFunctionalTest = ft.SkipFunctionalTest

	return nil
}

func (f *FunctionalTestConfig) Empty() bool {
	return len(f.SkipFunctionalTest) == 0
}

func (e *EndpointGroup) Empty() bool {
	if len(e.Endpoints) == 0 {
		return true
	}

	for _, endpoint := range e.Endpoints {
		if endpoint.Password == placeholderPassword {
			return true
		}
	}

	return false
}

func (e *EndpointGroup) ActiveStickySession() *StickySessionConfig {
	if e.UseFailover {
		return &e.EndpointConfigFailover.StickySession
	}

	return &e.EndpointConfig.StickySession
}

func (e *EndpointGroup) UnmarshalText(text []byte) error {
	if len(text) == 0 {
		return nil
	}

	var eg endpointGroup
	err := json.Unmarshal(text, &eg)
	if err != nil {
		return xerrors.Errorf("failed to parse EndpointGroup JSON: %w", err)
	}

	if len(eg.Endpoints) == 0 {
		return xerrors.New("endpoints is empty")
	}
	if eg.UseFailover && len(eg.EndpointsFailover) == 0 {
		return xerrors.New("failover endpoints is empty")
	}

	e.Endpoints = eg.Endpoints
	e.EndpointsFailover = eg.EndpointsFailover
	e.UseFailover = eg.UseFailover
	e.EndpointConfig = eg.EndpointConfig
	e.EndpointConfigFailover = eg.EndpointConfigFailover

	if err := e.EndpointConfig.StickySession.validateStickySession(); err != nil {
		return xerrors.Errorf("invalid sticky session config for the primary endpoint group: %w", err)
	}

	if err := e.EndpointConfigFailover.StickySession.validateStickySession(); err != nil {
		return xerrors.Errorf("invalid sticky session config for the failover endpoint group: %w", err)
	}

	for _, endpoints := range [][]Endpoint{e.Endpoints, e.EndpointsFailover} {
		for _, endpoint := range endpoints {
			if endpoint.Name == "" {
				return xerrors.New("empty endpoint.Name")
			}
			if endpoint.Url == "" {
				return xerrors.New("empty endpoint.URL")
			}
		}
	}
	return nil
}

func (e *StickySessionConfig) validateStickySession() error {
	if e.enabled() > 1 {
		return xerrors.New("only one sticky session type can be supported")
	}
	return nil
}

func (e *StickySessionConfig) Enabled() bool {
	return e.enabled() == 1
}

func (e *StickySessionConfig) enabled() int {
	count := 0
	if e.CookieHash != "" {
		count += 1
	}
	if e.CookiePassive {
		count += 1
	}
	if e.HeaderHash != "" {
		count += 1
	}

	return count
}

func (c *WorkflowConfig) Base() *WorkflowConfig {
	return c
}

func (c *WorkflowConfig) GetEffectiveBlockTag(tag uint32) uint32 {
	return c.BlockTag.GetEffectiveBlockTag(tag)
}

func (c *WorkflowConfig) GetEffectiveEventTag(eventTag uint32) uint32 {
	return c.EventTag.GetEffectiveEventTag(eventTag)
}

func (c *WorkflowConfig) DeriveConfig(cfg *Config) {
	if c.BlockTag == (BlockTagConfig{}) {
		// Derive from the global block tag.
		c.BlockTag = cfg.Chain.BlockTag
	}

	if c.EventTag == (EventTagConfig{}) {
		c.EventTag = cfg.Chain.EventTag
	}

	// Derive from the global Storage config.
	c.Storage = cfg.AWS.Storage

	if c.IrreversibleDistance == 0 {
		c.IrreversibleDistance = cfg.Chain.IrreversibleDistance
	}

	c.SLA = cfg.SLA
}

// GetEffectiveBlockTag returns the effective tag value.
// Because tag zero was chosen as the first valid tag, it is impossible to tell whether the tag field is omitted or tag zero is explicitly requested.
// Given that most users will need the stable tag by default, this function returns the stable tag if tag has the default zero value.
// To request for tag zero, MaxUint32 (4294967295) should be specified.
func (c *BlockTagConfig) GetEffectiveBlockTag(tag uint32) uint32 {
	if tag == 0 {
		return c.Stable
	}

	if tag == math.MaxUint32 {
		return 0
	}

	return tag
}

func (c *EventTagConfig) GetEffectiveEventTag(eventTag uint32) uint32 {
	if eventTag == 0 {
		return c.Stable
	}

	if eventTag == math.MaxUint32 {
		return 0
	}

	return eventTag
}

func (c *AwsConfig) DeriveConfig(cfg *Config) {
	configName := cfg.ConfigName
	normalizedConfigName := cfg.normalizeResourceName(configName)
	if c.Bucket == "" && cfg.Env() == EnvLocal {
		c.Bucket = fmt.Sprintf(s3BucketFormat, normalizedConfigName, cfg.AwsEnv())
	}
}

func (c *CadenceConfig) DeriveConfig(cfg *Config) {
	if c.Address == "" && cfg.Env() == EnvLocal {
		c.Address = cadenceAddressLocal
	}
}

func (c *SDKConfig) DeriveConfig(cfg *Config) {
	if c.ChainstorageAddress == "" && cfg.Env() == EnvLocal {
		c.ChainstorageAddress = chainstorageAddressLocal
	}
}

func (c *AuthConfig) UnmarshalText(text []byte) error {
	if len(text) == 0 {
		return nil
	}

	var tmp authConfig
	err := json.Unmarshal(text, &tmp)
	if err != nil {
		return xerrors.Errorf("failed to parse AuthConfig: %w", err)
	}

	for _, client := range tmp.Clients {
		if client.ClientID == "" {
			return xerrors.New("empty client_id")
		}
		if client.Token == "" {
			return xerrors.New("empty token")
		}
	}

	c.Clients = tmp.Clients
	c.DefaultRPS = tmp.DefaultRPS
	return nil
}

func (c *AuthConfig) AsMap() map[string]*AuthClient {
	res := make(map[string]*AuthClient, len(c.Clients))
	for i := range c.Clients {
		client := &c.Clients[i]
		res[client.Token] = client
	}
	return res
}
