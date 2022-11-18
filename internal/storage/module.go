package storage

import (
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	"github.com/coinbase/chainstorage/internal/storage/internal/errors"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
)

type (
	BlobStorage  = blobstorage.BlobStorage
	MetaStorage  = metastorage.MetaStorage
	BlockStorage = metastorage.BlockStorage
	EventStorage = metastorage.EventStorage
)

var Module = fx.Options(
	blobstorage.Module,
	metastorage.Module,
)

var (
	ErrRequestCanceled   = errors.ErrRequestCanceled
	ErrDownloadFailure   = errors.ErrDownloadFailure
	ErrOutOfRange        = errors.ErrOutOfRange
	ErrInvalidHeight     = errors.ErrInvalidHeight
	ErrInvalidEventId    = errors.ErrInvalidEventId
	ErrItemNotFound      = errors.ErrItemNotFound
	ErrNoEventHistory    = errors.ErrNoEventHistory
	ErrNoEventAvailable  = errors.ErrNoEventAvailable
	ErrNoMaxEventIdFound = errors.ErrNoMaxEventIdFound
)

var (
	EventIdStartValue = metastorage.EventIdStartValue
	EventIdDeleted    = metastorage.EventIdDeleted
)
