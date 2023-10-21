package dynamodb

import (
	"testing"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"

	"github.com/coinbase/chainstorage/internal/storage/metastorage/dynamodb/model"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
)

func TestMakeWatermarkVersionedDDBEntry(t *testing.T) {
	require := testutil.Require(t)

	expected := &model.VersionedEventDDBEntry{
		EventId:      "2-latest",
		Sequence:     100,
		BlockId:      "2-latest",
		BlockHeight:  blockHeightForWatermark,
		BlockHash:    "",
		EventType:    api.BlockchainEvent_UNKNOWN,
		Tag:          0,
		ParentHash:   "",
		BlockSkipped: false,
		EventTag:     2,
	}

	actualWatermark := makeWatermarkVersionedDDBEntry(uint32(2), int64(100))
	require.Equal(expected, actualWatermark)
}

func TestCastDDBEntryToVersionedDDBEntry(t *testing.T) {
	require := testutil.Require(t)

	ddbEntry := &model.EventDDBEntry{
		EventId:      101,
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		BlockHeight:  100,
		BlockHash:    "aaa",
		Tag:          0,
		ParentHash:   "bbb",
		MaxEventId:   0,
		BlockSkipped: false,
		EventTag:     2,
	}

	expectedVersionedDDBEntry := &model.VersionedEventDDBEntry{
		EventId:      "2-101",
		Sequence:     101,
		BlockId:      "2-100",
		BlockHeight:  100,
		BlockHash:    "aaa",
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		Tag:          0,
		ParentHash:   "bbb",
		BlockSkipped: false,
		EventTag:     2,
	}

	actualVersionedDDBEntry := castDDBEntryToVersionedDDBEntry(ddbEntry)
	require.Equal(expectedVersionedDDBEntry, actualVersionedDDBEntry)
}

func TestCastDDBEntryToVersionedDDBEntry_Watermark(t *testing.T) {
	require := testutil.Require(t)

	ddbEntry := &model.EventDDBEntry{
		EventId:     pkeyValueForWatermark,
		EventType:   api.BlockchainEvent_UNKNOWN,
		BlockHeight: blockHeightForWatermark,
		BlockHash:   "",
		MaxEventId:  101,
		EventTag:    2,
	}

	expectedVersionedDDBEntry := &model.VersionedEventDDBEntry{
		EventId:      "2-latest",
		Sequence:     101,
		BlockId:      "2-latest",
		BlockHeight:  blockHeightForWatermark,
		EventType:    api.BlockchainEvent_UNKNOWN,
		BlockHash:    "",
		Tag:          0,
		ParentHash:   "",
		BlockSkipped: false,
		EventTag:     2,
	}

	actualVersionedDDBEntry := castDDBEntryToVersionedDDBEntry(ddbEntry)
	require.Equal(expectedVersionedDDBEntry, actualVersionedDDBEntry)
}

func TestCastVersionedItemToDDBEntry(t *testing.T) {
	require := testutil.Require(t)

	versionedDDBEntry := &model.VersionedEventDDBEntry{
		EventId:      "2-101",
		Sequence:     101,
		BlockId:      "2-100",
		BlockHeight:  100,
		BlockHash:    "aaa",
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		Tag:          0,
		ParentHash:   "bbb",
		BlockSkipped: false,
		EventTag:     2,
	}

	expectedDDBEntry := &model.EventDDBEntry{
		EventId:      101,
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		BlockHeight:  100,
		BlockHash:    "aaa",
		Tag:          internal.DefaultBlockTag,
		ParentHash:   "bbb",
		MaxEventId:   0,
		BlockSkipped: false,
		EventTag:     2,
	}

	actualDDBEntry, err := castVersionedItemToDDBEntry(versionedDDBEntry)
	require.True(err)
	require.Equal(expectedDDBEntry, actualDDBEntry)
}

func TestCastVersionedItemToDDBEntry_Watermark(t *testing.T) {
	require := testutil.Require(t)

	versionedEntry := &model.VersionedEventDDBEntry{
		EventId:     "2-latest",
		Sequence:    101,
		BlockId:     "2-latest",
		BlockHeight: blockHeightForWatermark,
		EventType:   api.BlockchainEvent_UNKNOWN,
		Tag:         2,
		EventTag:    2,
	}

	expectedEntry := &model.EventDDBEntry{
		EventId:     -1,
		EventType:   api.BlockchainEvent_UNKNOWN,
		BlockHeight: blockHeightForWatermark,
		Tag:         2,
		MaxEventId:  101,
		EventTag:    2,
	}

	actualEntry, err := castVersionedItemToDDBEntry(versionedEntry)
	require.True(err)
	require.Equal(expectedEntry, actualEntry)
}
