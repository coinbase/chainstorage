package sdk

import (
	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/gateway"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/downloader"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/pointer"
	"github.com/coinbase/chainstorage/sdk/services"
)

type (
	Session interface {
		Client() Client
		Parser() Parser
	}

	SessionParams struct {
		fx.In
		Client Client
		Parser parser.Parser
	}

	sessionImpl struct {
		client Client
		parser Parser
	}
)

func New(manager services.SystemManager, cfg *Config) (Session, error) {
	if err := cfg.validate(); err != nil {
		return nil, xerrors.Errorf("invalid config %+v: %w", cfg, err)
	}

	internalCfg, err := config.New(
		config.WithBlockchain(cfg.Blockchain),
		config.WithNetwork(cfg.Network),
		config.WithEnvironment(cfg.Env),
		config.WithSidechain(cfg.Sidechain),
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to create config %w", err)
	}

	var session Session
	app := fx.New(
		Module,
		fxparams.Module,
		parser.Module,
		downloader.Module,
		gateway.Module,
		gateway.WithClientID(cfg.ClientID),
		gateway.WithServerAddress(cfg.ServerAddress),
		fx.NopLogger,
		fx.Provide(func() services.SystemManager { return manager }),
		fx.Provide(func() *zap.Logger { return manager.Logger() }),
		fx.Provide(func() tally.Scope { return tally.NoopScope }),
		fx.Provide(func() *config.Config { return internalCfg }),
		fx.Populate(&session),
	)
	if err := app.Start(manager.Context()); err != nil {
		return nil, xerrors.Errorf("failed to start fx app: %w", err)
	}

	manager.AddPreShutdownHook(func() {
		if err := app.Stop(manager.Context()); err != nil {
			manager.Logger().Error("failed to stop app", zap.Error(err))
		}
	})

	if cfg.Tag != 0 {
		session.Client().SetTag(cfg.Tag)
	}

	if cfg.ClientID != "" {
		session.Client().SetClientID(cfg.ClientID)
	}

	if cfg.BlockValidation != nil {
		session.Client().SetBlockValidation(pointer.Deref(cfg.BlockValidation))
	}

	return session, nil
}

func newSession(params SessionParams) (Session, error) {
	session := &sessionImpl{
		client: params.Client,
		parser: params.Parser,
	}

	return session, nil
}

func (s *sessionImpl) Client() Client {
	return s.client
}

func (s *sessionImpl) Parser() Parser {
	return s.parser
}
