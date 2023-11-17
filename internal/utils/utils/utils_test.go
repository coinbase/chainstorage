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

func TestGenerateSha256HashString(t *testing.T) {
	require := require.New(t)

	hash := GenerateSha256HashString("test")
	require.Equal("9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08", hash)
}

func TestParseSidechain(t *testing.T) {
	require := require.New(t)

	sidechainName := "ethereum-mainnet-beacon"
	sidechain, err := ParseSidechain(sidechainName)
	require.NoError(err)
	require.Equal(api.SideChain_SIDECHAIN_ETHEREUM_MAINNET_BEACON, sidechain)
}
