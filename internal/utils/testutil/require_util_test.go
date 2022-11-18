package testutil

import (
	"testing"

	"github.com/golang/protobuf/proto"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestRequire_CompareProtos(t *testing.T) {
	require := Require(t)

	expected := &api.BlockMetadata{Height: 1}
	actual := &api.BlockMetadata{Height: 1}
	// proto.Size causes state changes in the proto object and fails the equality check in testify/require.
	_ = proto.Size(actual)
	require.Equal(expected, actual)
}

func TestRequire_CompareSlices(t *testing.T) {
	require := Require(t)

	expected := []*api.BlockMetadata{
		{Height: 1},
		{Height: 2},
	}
	actual := []*api.BlockMetadata{
		{Height: 1},
		{Height: 2},
	}
	_ = proto.Size(actual[0])
	require.Equal(expected, actual)
}

func TestRequire_CompareStrings(t *testing.T) {
	require := Require(t)

	expected := "hello"
	actual := "hello"
	require.Equal(expected, actual)
}
