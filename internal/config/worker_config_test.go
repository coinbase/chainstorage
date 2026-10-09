package config

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestWorkerConfigDecodesActivityConcurrencyLimit(t *testing.T) {
	require := require.New(t)

	v := viper.New()
	v.SetConfigType("yaml")
	err := v.ReadConfig(strings.NewReader(`
workflows:
  workers:
    - task_list: batch_consolidator
      max_concurrent_activity_execution_size: 1
`))
	require.NoError(err)

	var cfg Config
	err = v.Unmarshal(&cfg)
	require.NoError(err)
	require.Len(cfg.Workflows.Workers, 1)
	require.Equal("batch_consolidator", cfg.Workflows.Workers[0].TaskList)
	require.Equal(1, cfg.Workflows.Workers[0].MaxConcurrentActivityExecutionSize)
}
