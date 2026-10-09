package internal

import (
	"context"
	"io"
)

const consolidatedUploadProgressReadInterval = 64 * 1024 * 1024

type (
	ConsolidatedUploadProgress func(stage string, details ...any)

	consolidatedUploadProgressKey struct{}

	progressReadCloser struct {
		ctx       context.Context
		stage     string
		reader    io.ReadCloser
		totalRead uint64
		nextRead  uint64
	}

	seekableProgressReadCloser struct {
		*progressReadCloser
		seeker io.Seeker
	}
)

func WithConsolidatedUploadProgress(ctx context.Context, fn ConsolidatedUploadProgress) context.Context {
	if fn == nil {
		return ctx
	}
	return context.WithValue(ctx, consolidatedUploadProgressKey{}, fn)
}

func RecordConsolidatedUploadProgress(ctx context.Context, stage string, details ...any) {
	fn, ok := ctx.Value(consolidatedUploadProgressKey{}).(ConsolidatedUploadProgress)
	if !ok {
		return
	}
	fn(stage, details...)
}

func NewConsolidatedUploadProgressReadCloser(ctx context.Context, stage string, reader io.ReadCloser) io.ReadCloser {
	if _, ok := ctx.Value(consolidatedUploadProgressKey{}).(ConsolidatedUploadProgress); !ok {
		return reader
	}
	progressReader := &progressReadCloser{
		ctx:      ctx,
		stage:    stage,
		reader:   reader,
		nextRead: consolidatedUploadProgressReadInterval,
	}
	if seeker, ok := reader.(io.Seeker); ok {
		return &seekableProgressReadCloser{
			progressReadCloser: progressReader,
			seeker:             seeker,
		}
	}
	return progressReader
}

func (r *progressReadCloser) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.totalRead += uint64(n)
		if r.totalRead >= r.nextRead {
			RecordConsolidatedUploadProgress(r.ctx, r.stage, r.totalRead)
			r.nextRead = r.totalRead + consolidatedUploadProgressReadInterval
		}
	}
	return n, err
}

func (r *progressReadCloser) Close() error {
	return r.reader.Close()
}

func (r *seekableProgressReadCloser) Seek(offset int64, whence int) (int64, error) {
	return r.seeker.Seek(offset, whence)
}
