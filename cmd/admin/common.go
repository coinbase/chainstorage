package main

import (
	"bufio"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/gateway"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/utils"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	"github.com/coinbase/chainstorage/sdk"
	"github.com/coinbase/chainstorage/sdk/services"
)

const (
	envFlagName        = "env"
	blockchainFlagName = "blockchain"
	networkFlagName    = "network"
	sidechainFlagName  = "sidechain"
)

const (
	protobufExtension = ".pb"
	defaultParser     = parserNative
	parserNative      = "native"
	parserRosetta     = "rosetta"
	parserRaw         = "raw"
)

type (
	CmdApp interface {
		Close()
		Manager() services.SystemManager
		Config() *config.Config
	}

	cmdAppImpl struct {
		app     *fx.App
		manager services.SystemManager
		config  *config.Config
	}
)

var (
	commonFlags struct {
		env        string
		blockchain string
		network    string
		sidechain  string
		out        string
		parser     string
		meta       bool
	}

	logger     *zap.Logger
	env        config.Env
	blockchain *common.Blockchain
	network    *common.Network
	sidechain  api.SideChain
)

func init() {
	logger = log.NewDevelopment()
	rootCmd.PersistentFlags().StringVar(&commonFlags.env, envFlagName, "", "one of [local, development, production]")
	rootCmd.PersistentFlags().StringVar(&commonFlags.blockchain, blockchainFlagName, "", "blockchain full name (e.g. ethereum)")
	rootCmd.PersistentFlags().StringVar(&commonFlags.network, networkFlagName, "", "network name (e.g. mainnet)")
	rootCmd.PersistentFlags().StringVar(&commonFlags.sidechain, sidechainFlagName, "", "sidechain name (e.g. beacon)")
	rootCmd.PersistentFlags().StringVar(&commonFlags.out, "out", "", "output filepath: default format is json; use a .pb extension for protobuf format")
	rootCmd.PersistentFlags().StringVar(&commonFlags.parser, "parser", defaultParser, "parser type: one of native, rosetta, or raw")
	rootCmd.PersistentFlags().BoolVar(&commonFlags.meta, "meta", false, "output metadata only")

	err := rootCmd.MarkPersistentFlagRequired(blockchainFlagName)
	if err != nil {
		logger.Fatal(fmt.Sprintf("error marking flag %s required", blockchainFlagName))
	}
	err = rootCmd.MarkPersistentFlagRequired(networkFlagName)
	if err != nil {
		logger.Fatal(fmt.Sprintf("error marking flag %s required", networkFlagName))
	}
	err = rootCmd.MarkPersistentFlagRequired(envFlagName)
	if err != nil {
		logger.Fatal(fmt.Sprintf("error marking flag %s required", envFlagName))
	}
}

// This function parses the global variables set by rootCmd for the chain info(blockchain, network, env, sidechain) flags
func initializeChainInfoFromFlags() error {
	var err error
	blockchain, network, sidechain, err = parseChainInfo(commonFlags.blockchain, commonFlags.network, commonFlags.sidechain)
	if err != nil {
		return xerrors.Errorf("failed to parse blockchain and network: %w", err)
	}
	env = config.Env(commonFlags.env)
	return nil
}

func startApp(opts ...fx.Option) CmdApp {
	manager := services.NewManager(services.WithLogger(logger))

	err := initializeChainInfoFromFlags()
	if err != nil {
		panic(xerrors.Errorf("failed to initialize blockchain, network, env from flags: %w", err))
	}

	cfg, err := config.New(
		config.WithBlockchain(*blockchain),
		config.WithNetwork(*network),
		config.WithSidechain(sidechain),
		config.WithEnvironment(env),
	)
	if err != nil {
		panic(xerrors.Errorf("failed to create service config: %w", err))
	}

	finalOpts := []fx.Option{
		config.Module,
		config.WithCustomConfig(cfg),
		fxparams.Module,
		gateway.Module,
		sdk.Module,
		fx.NopLogger,
		fx.Provide(func() *zap.Logger { return logger }),
		fx.Provide(func() tally.Scope { return tally.NoopScope }),
		fx.Provide(func() services.SystemManager { return manager }),
		fx.Provide(dlq.NewNop),
	}

	for _, opt := range opts {
		finalOpts = append(finalOpts, opt)
	}

	app := fx.New(finalOpts...)
	if err := app.Start(manager.Context()); err != nil {
		logger.Fatal("failed to start app", zap.Error(err))
	}

	return &cmdAppImpl{
		app:     app,
		manager: manager,
		config:  cfg,
	}
}

func (a *cmdAppImpl) Close() {
	if err := a.app.Stop(a.manager.Context()); err != nil {
		logger.Error("failed to stop app", zap.Error(err))
	}

	a.manager.Shutdown()
}

func (a *cmdAppImpl) Manager() services.SystemManager {
	return a.manager
}

func (a *cmdAppImpl) Config() *config.Config {
	return a.config
}

func printBlock(parser sdk.Parser, rawBlock *api.Block, outputWithHeight bool) error {
	block, err := parseBlock(parser, rawBlock)
	if err != nil {
		return xerrors.Errorf("failed to parse block: %w", err)
	}

	out := commonFlags.out
	if out != "" && outputWithHeight {
		// Include height in output name,
		// e.g. block.json => block_1234.json
		ext := filepath.Ext(out)
		out = fmt.Sprintf(
			"%v_%v%v",
			strings.TrimSuffix(out, ext),
			strconv.Itoa(int(rawBlock.Metadata.Height)),
			ext,
		)
	}

	if err := logBlock(rawBlock.Metadata, block, out); err != nil {
		return xerrors.Errorf("failed to log block: %w", err)
	}

	return nil
}

func parseBlock(parser sdk.Parser, rawBlock *api.Block) (proto.Message, error) {
	switch commonFlags.parser {
	case parserRaw:
		return rawBlock, nil
	case parserNative:
		return parser.ParseNativeBlock(context.Background(), rawBlock)
	case parserRosetta:
		return parser.ParseRosettaBlock(context.Background(), rawBlock)
	default:
		return nil, xerrors.Errorf("unknown parser: %v", commonFlags.parser)
	}
}

func logBlock(metadata *api.BlockMetadata, block proto.Message, out string) error {
	var data []byte
	var err error
	if out == "" {
		if commonFlags.meta {
			logger.Info("fetched block", zap.Reflect("metadata", metadata))
		} else {
			logger.Info("fetched block", zap.Reflect("block", block))
		}
		return nil
	}
	extension := strings.ToLower(filepath.Ext(out))
	if extension == protobufExtension {
		data, err = proto.Marshal(block)
		if err != nil {
			return xerrors.Errorf("failed to marshal block in protobuf format: %w", err)
		}
	} else {
		// Defaults to json format.
		marshaler := protojson.MarshalOptions{Indent: "  "}
		data, err = marshaler.Marshal(block)
		if err != nil {
			return xerrors.Errorf("failed to marshal block in json format: %w", err)
		}
	}

	if err := ioutil.WriteFile(out, data, 0644); /* #nosec G306 */ err != nil {
		return xerrors.Errorf("failed to write output file: %w", err)
	}

	logger.Info("fetched block", zap.Reflect("metadata", metadata), zap.String("out", out))
	return nil
}

func parseChainInfo(
	blockchainFlag string,
	networkFlag string,
	sidechainFlag string,
) (*common.Blockchain, *common.Network, api.SideChain, error) {
	blockchain, err := utils.ParseBlockchain(blockchainFlag)
	if err != nil {
		return nil, nil, api.SideChain_SIDECHAIN_NONE, xerrors.Errorf("failed to parse blockchain name: %s", blockchain)
	}

	networkName := fmt.Sprintf("%v_%v", blockchainFlag, networkFlag)
	network, err := utils.ParseNetwork(networkName)
	if err != nil {
		return nil, nil, api.SideChain_SIDECHAIN_NONE, xerrors.Errorf("failed to parse network name: %s", networkName)
	}

	if sidechainFlag != "" {
		sidechainName := fmt.Sprintf("%v_%v_%v", blockchainFlag, networkFlag, sidechainFlag)
		sidechain, err := utils.ParseSidechain(sidechainName)
		if err != nil {
			return nil, nil, api.SideChain_SIDECHAIN_NONE, xerrors.Errorf("failed to parse sidechain name: %s", sidechainName)
		}

		return &blockchain, &network, sidechain, nil
	}

	return &blockchain, &network, api.SideChain_SIDECHAIN_NONE, nil
}

func confirm(prompt string) bool {
	if env == config.EnvLocal {
		return true
	}

	fmt.Printf(prompt)
	response, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		logger.Error("failed to read from console", zap.Error(err))
		return false
	}

	if strings.ToLower(strings.TrimSpace(response)) != "y" {
		return false
	}

	return true
}
