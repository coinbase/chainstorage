package metastorage

import (
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/storage/metastorage/dynamodb"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
)

type (
	MetaStorage          = internal.MetaStorage
	BlockStorage         = internal.BlockStorage
	EventStorage         = internal.EventStorage
	EventsToChainAdaptor = internal.EventsToChainAdaptor
)

const (
	EventIdStartValue = model.EventIdStartValue
	EventIdDeleted    = model.EventIdDeleted
)

func NewEventsToChainAdaptor() *EventsToChainAdaptor {
	return internal.NewEventsToChainAdaptor()
}

var Module = fx.Options(
	dynamodb.Module,
	fx.Provide(internal.WithMetaStorageFactory),
)
