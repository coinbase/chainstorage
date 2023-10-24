package metastorage

import (
	"github.com/coinbase/chainstorage/internal/config"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
)

func WithMetaStorageFactory(params MetaStorageFactoryParams) (Result, error) {
	var factory MetaStorageFactory
	switch params.Config.StorageType.MetaStorageType {
	case config.MetaStorageType_UNSPECIFIED, config.MetaStorageType_DYNAMODB:
		factory = params.DynamoDB
	}
	if factory == nil {
		return Result{}, xerrors.Errorf(
			"meta storage type is not implemented: %v",
			params.Config.StorageType.MetaStorageType)
	}
	result, err := factory.Create()
	if err != nil {
		return Result{}, xerrors.Errorf("failed to create meta storage, error: %w", err)
	}
	return result, nil
}

var Module = fx.Options(
	// TODO: move this into sub package
	fx.Provide(fx.Annotated{
		Name:   "metastorage/dynamodb",
		Target: NewFactory,
	}),
	fx.Provide(WithMetaStorageFactory),
)
