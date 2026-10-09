package sdk

import (
	"context"
	"io"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/storage/blobstorage/downloader"
)

type (
	// NativeStreamedBlockIterator yields one NativeStreamedBlock per
	// block in [startHeight, endHeight) order and returns exactly io.EOF
	// when exhausted. It is the chunk-once counterpart of calling
	// StreamNativeBlock per height: consecutive blocks that share a
	// consolidated (CSCB) chunk are decompressed in one pass.
	//
	// Test for the end of the range with `err == io.EOF`. Any other error
	// is terminal and is returned again by every later Next, so a walker
	// that checkpoints on io.EOF can never mistake a failed download for
	// the end of the range.
	//
	// Only the block most recently returned is resident. Close each
	// NativeStreamedBlock before calling Next again to keep memory
	// bounded to one block; callers MUST close every returned block and
	// the iterator.
	NativeStreamedBlockIterator interface {
		Next(ctx context.Context) (NativeStreamedBlock, error)
		Close() error
	}

	nativeStreamedBlockIterator struct {
		inner  downloader.SpooledBlockIterator
		parser streamingParser
		opts   []ParseOption
		err    error
	}
)

func (i *nativeStreamedBlockIterator) Next(ctx context.Context) (NativeStreamedBlock, error) {
	if i.err != nil {
		return nil, i.err
	}
	spooled, err := i.inner.Next(ctx)
	if err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		i.err = err
		return nil, err
	}
	if spooled == nil {
		i.err = xerrors.New("spooled block iterator returned a nil block")
		return nil, i.err
	}
	stream, err := i.parser.ParseStreamNative(ctx, spooled, i.opts...)
	if err != nil {
		_ = spooled.Close()
		i.err = xerrors.Errorf("failed to create native stream (height=%d): %w", spooled.BlockFile.GetHeight(), err)
		return nil, i.err
	}
	return stream, nil
}

func (i *nativeStreamedBlockIterator) Close() error {
	if i == nil || i.inner == nil {
		return nil
	}
	return i.inner.Close()
}
