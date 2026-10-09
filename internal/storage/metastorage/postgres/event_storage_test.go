package postgres

import (
	"testing"

	pgmodel "github.com/coinbase/chainstorage/internal/storage/metastorage/postgres/model"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestEventTypeToString(t *testing.T) {
	require := testutil.Require(t)

	tests := []struct {
		name      string
		eventType api.BlockchainEvent_Type
		expected  string
	}{
		{
			name:      "BLOCK_ADDED",
			eventType: api.BlockchainEvent_BLOCK_ADDED,
			expected:  "BLOCK_ADDED",
		},
		{
			name:      "BLOCK_REMOVED",
			eventType: api.BlockchainEvent_BLOCK_REMOVED,
			expected:  "BLOCK_REMOVED",
		},
		{
			name:      "UNKNOWN",
			eventType: api.BlockchainEvent_UNKNOWN,
			expected:  "UNKNOWN",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := pgmodel.EventTypeToString(test.eventType)
			require.Equal(test.expected, result)
		})
	}
}

func TestParseEventType(t *testing.T) {
	require := testutil.Require(t)

	tests := []struct {
		name     string
		input    string
		expected api.BlockchainEvent_Type
	}{
		{
			name:     "BLOCK_ADDED",
			input:    "BLOCK_ADDED",
			expected: api.BlockchainEvent_BLOCK_ADDED,
		},
		{
			name:     "BLOCK_REMOVED",
			input:    "BLOCK_REMOVED",
			expected: api.BlockchainEvent_BLOCK_REMOVED,
		},
		{
			name:     "UNKNOWN",
			input:    "UNKNOWN",
			expected: api.BlockchainEvent_UNKNOWN,
		},
		{
			name:     "Invalid string",
			input:    "INVALID_TYPE",
			expected: api.BlockchainEvent_UNKNOWN,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := pgmodel.ParseEventType(test.input)
			require.Equal(test.expected, result)
		})
	}
}

func TestEventTypeConversion(t *testing.T) {
	require := testutil.Require(t)

	eventTypes := []api.BlockchainEvent_Type{
		api.BlockchainEvent_BLOCK_ADDED,
		api.BlockchainEvent_BLOCK_REMOVED,
		api.BlockchainEvent_UNKNOWN,
	}

	for _, eventType := range eventTypes {
		t.Run(eventType.String(), func(t *testing.T) {
			// Convert to string
			eventTypeStr := pgmodel.EventTypeToString(eventType)

			// Convert back to enum
			convertedEventType := pgmodel.ParseEventType(eventTypeStr)

			// Should be the same
			require.Equal(eventType, convertedEventType)
		})
	}
}
