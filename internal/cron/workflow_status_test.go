package cron

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetWorkflowSeverity(t *testing.T) {
	tests := []struct {
		name     string
		severity string
		workflow string
	}{
		{
			name:     "poller",
			severity: sev1,
			workflow: "poller",
		},
		{
			name:     "poller with tag",
			severity: sev2,
			workflow: "poller/block_tag=2",
		},
		{
			name:     "poller with unknown tag",
			severity: sev3,
			workflow: "poller/unknown_tag=3",
		},
		{
			name:     "streamer",
			severity: sev1,
			workflow: "streamer",
		},
		{
			name:     "streamer with tag",
			severity: sev2,
			workflow: "streamer/event_tag=2",
		},
		{
			name:     "streamer with missing tag",
			severity: sev3,
			workflow: "streamer/event_tag=",
		},
		{
			name:     "cross_validator",
			severity: sev2,
			workflow: "cross_validator",
		},
		{
			name:     "monitor",
			severity: sev2,
			workflow: "monitor",
		},
		{
			name:     "backfiller",
			severity: sev3,
			workflow: "backfiller",
		},
		{
			name:     "exact match without suffix",
			severity: sev3,
			workflow: "pollers",
		},
		{
			name:     "exact match without prefix",
			severity: sev3,
			workflow: "sstreamer",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			require.Equal(test.severity, getWorkflowSeverity(test.workflow))
		})
	}
}
