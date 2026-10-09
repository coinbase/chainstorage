package sdk

import (
	"context"
	"io"
	"sync"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/storage/blobstorage/downloader"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	// RawBlockPayload is an open reader for one validated, decompressed
	// protobuf-serialized api.Block payload. It deliberately does not unmarshal
	// the payload. Callers must read it to EOF or close it to complete
	// downloader-side validation and release resources.
	RawBlockPayload struct {
		BlockFile *api.BlockFile
		Length    uint64

		reader    io.ReadCloser
		closeOnce sync.Once
	}

	// RawBlockPayloadIterator yields raw block payloads in height/range order
	// and returns io.EOF when exhausted. Callers must close every returned
	// payload and then close the iterator.
	RawBlockPayloadIterator interface {
		Next(ctx context.Context) (*RawBlockPayload, error)
		Close() error
	}

	rawBlockPayloadIterator struct {
		inner downloader.RawBlockPayloadIterator
	}
)

func (p *RawBlockPayload) Read(buf []byte) (int, error) {
	if p == nil || p.reader == nil {
		return 0, xerrors.New("raw block payload is closed")
	}
	return p.reader.Read(buf)
}

func (p *RawBlockPayload) Close() error {
	if p == nil {
		return nil
	}
	var err error
	p.closeOnce.Do(func() {
		if p.reader != nil {
			err = p.reader.Close()
		}
		p.reader = nil
	})
	return err
}

func (i *rawBlockPayloadIterator) Next(ctx context.Context) (*RawBlockPayload, error) {
	payload, err := i.inner.Next(ctx)
	if err != nil {
		return nil, err
	}
	return wrapRawBlockPayload(payload), nil
}

func (i *rawBlockPayloadIterator) Close() error {
	if i == nil || i.inner == nil {
		return nil
	}
	return i.inner.Close()
}

func wrapRawBlockPayload(payload *downloader.RawBlockPayload) *RawBlockPayload {
	if payload == nil {
		return nil
	}
	return &RawBlockPayload{
		BlockFile: payload.BlockFile,
		Length:    payload.Length,
		reader:    payload,
	}
}
