package config_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/coinbase/chainstorage/internal/config"
)

// TestDumpEndpoints prints the endpoint settings that config.New produces after
// viper has merged base.yml, <env>.yml, .secrets.yml and the CHAINSTORAGE_*
// environment variables. It is a diagnostic tool rather than an assertion: run
// it with the same environment as a deployment to see which rps and
// rps_count_batch values that deployment actually ends up with.
//
//	DUMP_ENDPOINTS=1 CHAINSTORAGE_CONFIG=story-mainnet CHAINSTORAGE_ENVIRONMENT=production \
//	  go test ./internal/config -run TestDumpEndpoints -v
func TestDumpEndpoints(t *testing.T) {
	if os.Getenv("DUMP_ENDPOINTS") == "" {
		t.Skip("set DUMP_ENDPOINTS=1 to dump the effective endpoint config")
	}

	cfg, err := config.New()
	if err != nil {
		t.Fatal(err)
	}

	client := cfg.Chain.Client
	groups := []struct {
		name  string
		group *config.EndpointGroup
	}{
		{"master", &client.Master.EndpointGroup},
		{"slave", &client.Slave.EndpointGroup},
		{"validator", &client.Validator.EndpointGroup},
		{"consensus", &client.Consensus.EndpointGroup},
		{"additional", &client.Additional.EndpointGroup},
	}

	fmt.Printf("config=%v env=%v tx_batch_size=%v\n", cfg.ConfigName, cfg.Env(), client.TxBatchSize)
	for _, g := range groups {
		if len(g.group.Endpoints) == 0 {
			fmt.Printf("group=%-10v <empty>\n", g.name)
			continue
		}

		for _, endpoint := range g.group.Endpoints {
			fmt.Printf(
				"group=%-10v name=%-16v rps=%-6v rps_count_batch=%-6v weight=%v use_failover=%v\n",
				g.name, endpoint.Name, endpoint.RPS, endpoint.RPSCountBatch, endpoint.Weight, g.group.UseFailover,
			)
		}
	}
}
