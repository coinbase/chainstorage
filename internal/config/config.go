package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
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

		namespace string
		env       Env
	}

	StorageType struct {
		BlobStorageType BlobStorageType `mapstructure:"blob"`
		MetaStorageType MetaStorageType `mapstructure:"meta"`
		DLQType         DLQType         `mapstructure:"dlq"`
	}

	BlobStorageType int32
	MetaStorageType int32
	DLQType         int32

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
	}

	ClientConfig struct {
		Master      JSONRPCConfig     `mapstructure:"master"`
		Slave       JSONRPCConfig     `mapstructure:"slave"`
		Validator   JSONRPCConfig     `mapstructure:"validator"`
		Consensus   JSONRPCConfig     `mapstructure:"consensus"`
		Retry       ClientRetryConfig `mapstructure:"retry"`
		HttpTimeout time.Duration     `mapstructure:"http_timeout"`
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
		Region                 string         `mapstructure:"region" validate:"required"`
		Bucket                 string         `mapstructure:"bucket" validate:"required"`
		DynamoDB               DynamoDBConfig `mapstructure:"dynamodb" validate:"required"`
		IsLocalStack           bool           `mapstructure:"local_stack"`
		IsResetLocal           bool           `mapstructure:"reset_local"`
		PresignedUrlExpiration time.Duration  `mapstructure:"presigned_url_expiration" validate:"required"`
		DLQ                    SQSConfig      `mapstructure:"dlq"`
		Storage                StorageConfig  `mapstructure:"storage"`
		AWSAccount             AWSAccount     `mapstructure:"aws_account" validate:"required"`
	}

	GcpConfig struct {
		Project string `mapstructure:"project" validate:"required"`
		Bucket  string `mapstructure:"bucket"`
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
		Address         string           `mapstructure:"address" validate:"required"`
		Domain          string           `mapstructure:"domain" validate:"required"`
		RetentionPeriod int32            `mapstructure:"retention_period" validate:"required"`
		TLSConfig       CadenceTLSConfig `mapstructure:"tls" validate:"required"`
	}

	CadenceTLSConfig struct {
		Enabled              bool   `mapstructure:"enabled"`
		ValidateHostname     bool   `mapstructure:"validate_hostname"`
		CertificateAuthority string `mapstructure:"certificate_authority"`
		ClientCertificate    string `mapstructure:"client_certificate"`
		ClientPrivateKey     string `mapstructure:"client_private_key"`
	}

	WorkflowsConfig struct {
		Workers         []WorkerConfig                `mapstructure:"workers"`
		Backfiller      BackfillerWorkflowConfig      `mapstructure:"backfiller"`
		Poller          PollerWorkflowConfig          `mapstructure:"poller"`
		Benchmarker     BenchmarkerWorkflowConfig     `mapstructure:"benchmarker"`
		Monitor         MonitorWorkflowConfig         `mapstructure:"monitor"`
		Streamer        StreamerWorkflowConfig        `mapstructure:"streamer"`
		CrossValidator  CrossValidatorWorkflowConfig  `mapstructure:"cross_validator"`
		EventBackfiller EventBackfillerWorkflowConfig `mapstructure:"event_backfiller"`
	}

	WorkerConfig struct {
		TaskList string `mapstructure:"task_list"`
	}

	WorkflowConfig struct {
		WorkflowIdentity               string         `mapstructure:"workflow_identity" validate:"required"`
		Enabled                        bool           `mapstructure:"enabled"`
		TaskList                       string         `mapstructure:"task_list" validate:"required"`
		WorkflowDecisionTimeout        time.Duration  `mapstructure:"workflow_decision_timeout" validate:"required"`
		WorkflowExecutionTimeout       time.Duration  `mapstructure:"workflow_execution_timeout" validate:"required"`
		ActivityScheduleToStartTimeout time.Duration  `mapstructure:"activity_schedule_to_start_timeout" validate:"required"`
		ActivityStartToCloseTimeout    time.Duration  `mapstructure:"activity_start_to_close_timeout" validate:"required"`
		ActivityHeartbeatTimeout       time.Duration  `mapstructure:"activity_heartbeat_timeout"`
		ActivityRetryMaximumAttempts   int32          `mapstructure:"activity_retry_maximum_attempts" validate:"required"`
		BlockTag                       BlockTagConfig `mapstructure:"block_tag"`
		EventTag                       EventTagConfig `mapstructure:"event_tag"`
		Storage                        StorageConfig  `mapstructure:"storage"`
		IrreversibleDistance           uint64         `mapstructure:"irreversible_distance" validate:"required"`
		FailoverEnabled                bool           `mapstructure:"failover_enabled"`
		ConsensusFailoverEnabled       bool           `mapstructure:"consensus_failover_enabled"`
		SLA                            SLAConfig      `mapstructure:"sla"`
	}

	BackfillerWorkflowConfig struct {
		WorkflowConfig          `mapstructure:",squash"`
		BatchSize               uint64 `mapstructure:"batch_size" validate:"required"`
		MiniBatchSize           uint64 `mapstructure:"mini_batch_size" validate:"required"`
		CheckpointSize          uint64 `mapstructure:"checkpoint_size" validate:"required,gtfield=BatchSize"`
		MaxReprocessedPerBatch  uint64 `mapstructure:"max_reprocessed_per_batch"`
		NumConcurrentExtractors int    `mapstructure:"num_concurrent_extractors" validate:"required"`
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

	RosettaConfig struct {
		Blockchain              string  `mapstructure:"blockchain"`
		Network                 string  `mapstructure:"network" validate:"required_with=Blockchain"`
		BlockNotFoundErrorCodes []int32 `mapstructure:"block_not_found_error_codes"`
		EnableRawBlockApi       bool    `mapstructure:"enable_raw_block_api"`
		FromRosetta             bool    `mapstructure:"from_rosetta"`
	}

	EndpointGroup struct {
		Endpoints              []Endpoint     `json:"endpoints"`
		EndpointsFailover      []Endpoint     `json:"endpoints_failover"`
		UseFailover            bool           `json:"use_failover"`
		EndpointConfig         EndpointConfig `json:"endpoint_config"`
		EndpointConfigFailover EndpointConfig `json:"endpoint_config_failover"`
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
		Name       string            `json:"name"`
		ProviderID string            `json:"provider_id"`
		Url        string            `json:"url"`
		User       string            `json:"user"`
		Password   string            `json:"password"`
		Weight     uint8             `json:"weight"`
		ExtraUrls  map[string]string `json:"extra_urls"`
		RPS        int               `json:"rps"`
	}

	EndpointConfig struct {
		StickySession StickySessionConfig `json:"sticky_session"`
		Headers       map[string]string   `json:"headers"`
	}

	StickySessionConfig struct {
		// The CookieHash method consistently maps a cookie value to a specific node.
		CookieHash string `json:"cookie_hash"`

		// The CookiePassive method persists the cookie value provided by the server.
		CookiePassive bool `json:"cookie_passive"`

		// The HeaderHash method consistently maps a header value to a specific node.
		HeaderHash string `json:"header_hash"`
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
		BlockRangeSize         uint64 `mapstructure:"block_range_size" validate:"required"`
		DisableDLQProcessor    bool   `mapstructure:"disable_dlq_processor"`
		DisablePollingCanary   bool   `mapstructure:"disable_polling_canary"`
		DisableStreamingCanary bool   `mapstructure:"disable_streaming_canary"`
		DisableNodeCanary      bool   `mapstructure:"disable_node_canary"`
		DisableWorkflowStatus  bool   `mapstructure:"disable_workflow_status"`
	}

	StorageConfig struct {
		DataCompression api.Compression `mapstructure:"data_compression"`
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
	}

	DLQType_value = map[string]int32{
		"UNSPECIFIED": 0,
		"SQS":         1,
	}
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

	DLQType_UNSPECIFIED DLQType = 0
	DLQType_SQS         DLQType = 1

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

	cfg.setDerivedConfigs(reflect.ValueOf(&cfg))

	if err := validate.Struct(&cfg); err != nil {
		return nil, xerrors.Errorf("failed to validate config: %w", err)
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

		return api.Compression_value[data.(string)], nil
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
