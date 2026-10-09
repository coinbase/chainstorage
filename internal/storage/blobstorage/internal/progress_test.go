package internal

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

type infiniteReader struct{}

func (infiniteReader) Read(p []byte) (int, error) {
	return len(p), nil
}

type readSeekCloser struct {
	*bytes.Reader
}

func (readSeekCloser) Close() error {
	return nil
}

func TestConsolidatedUploadProgressReadCloserReportsThreshold(t *testing.T) {
	var progressBytes []uint64
	ctx := WithConsolidatedUploadProgress(context.Background(), func(stage string, details ...any) {
		require.Equal(t, "s3_put_read", stage)
		require.Len(t, details, 1)
		progressBytes = append(progressBytes, details[0].(uint64))
	})

	reader := NewConsolidatedUploadProgressReadCloser(
		ctx,
		"s3_put_read",
		io.NopCloser(io.LimitReader(infiniteReader{}, int64(consolidatedUploadProgressReadInterval+1))),
	)
	copied, err := io.Copy(io.Discard, reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, int64(consolidatedUploadProgressReadInterval+1), copied)
	require.Len(t, progressBytes, 1)
	require.GreaterOrEqual(t, progressBytes[0], uint64(consolidatedUploadProgressReadInterval))
}

func TestConsolidatedUploadProgressReadCloserPreservesSeekableReader(t *testing.T) {
	ctx := WithConsolidatedUploadProgress(context.Background(), func(stage string, details ...any) {})
	reader := NewConsolidatedUploadProgressReadCloser(
		ctx,
		"s3_put_read",
		readSeekCloser{Reader: bytes.NewReader([]byte("abcdef"))},
	)

	seeker, ok := reader.(io.Seeker)
	require.True(t, ok)
	offset, err := seeker.Seek(2, io.SeekStart)
	require.NoError(t, err)
	require.Equal(t, int64(2), offset)

	buf := make([]byte, 2)
	n, err := reader.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, []byte("cd"), buf)
	require.NoError(t, reader.Close())
}

func TestConsolidatedUploadProgressReadCloserDoesNotAdvertiseSeekForNonSeekableReader(t *testing.T) {
	ctx := WithConsolidatedUploadProgress(context.Background(), func(stage string, details ...any) {})
	reader := NewConsolidatedUploadProgressReadCloser(
		ctx,
		"s3_put_read",
		io.NopCloser(io.LimitReader(infiniteReader{}, 1)),
	)
	_, ok := reader.(io.Seeker)
	require.False(t, ok)
	require.NoError(t, reader.Close())
}
