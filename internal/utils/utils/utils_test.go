package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestParseCompression(t *testing.T) {
	tests := []struct {
		name     string
		expected api.Compression
	}{
		{
			name:     "gzip",
			expected: api.Compression_GZIP,
		},
		{
			name:     "GZIP",
			expected: api.Compression_GZIP,
		},
		{
			name:     "none",
			expected: api.Compression_NONE,
		},
		{
			name:     "None",
			expected: api.Compression_NONE,
		},
		{
			name:     "",
			expected: api.Compression_NONE,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)

			actual, err := ParseCompression(test.name)
			require.NoError(err)
			require.Equal(test.expected, actual)
		})
	}
}

func TestParseCompressionUnknown(t *testing.T) {
	require := require.New(t)

	compression, err := ParseCompression("invalid")
	require.Error(err)
	require.Equal(api.Compression_NONE, compression)
}

func TestToTimestamp(t *testing.T) {
	require := require.New(t)
	require.Nil(ToTimestamp(0))
	require.Equal(&timestamppb.Timestamp{Seconds: 123456789}, ToTimestamp(123456789))
}

func TestSinceTimestamp(t *testing.T) {
	require := require.New(t)
	duration := SinceTimestamp(nil)
	require.Zero(duration)
	duration = SinceTimestamp(&timestamppb.Timestamp{Seconds: 0})
	require.Zero(duration)

	ts := &timestamppb.Timestamp{Seconds: time.Now().Unix() - 1}
	duration = SinceTimestamp(ts)
	require.NotZero(duration)
	require.Less(duration, time.Second*2)
}
