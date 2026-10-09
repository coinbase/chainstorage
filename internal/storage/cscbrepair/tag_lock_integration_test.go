package cscbrepair_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/cscbrepairlock"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

func TestIntegrationCSCBRepairTagLockModes(t *testing.T) {
	if os.Getenv("TEST_TYPE") != "integration" {
		t.Skip("integration test")
	}
	require := require.New(t)
	ctx := context.Background()
	tag := uint32(1_700_000_000 + time.Now().UTC().UnixNano()%100_000_000)

	cfg, err := config.New(
		config.WithEnvironment(config.EnvLocal),
		config.WithBlockchain(common.Blockchain_BLOCKCHAIN_SOLANA),
		config.WithNetwork(common.Network_NETWORK_SOLANA_MAINNET),
	)
	require.NoError(err)
	if cfg.AWS.Postgres == nil {
		t.Skip("Postgres is not configured")
	}
	configureRepairTestEnvironment(t, cfg.AWS.Postgres)
	db, err := openRepairDB(ctx, cfg.AWS.Postgres)
	require.NoError(err)
	defer func() { _ = db.Close() }()

	sharedHolder, err := db.BeginTx(ctx, nil)
	require.NoError(err)
	defer func() { _ = sharedHolder.Rollback() }()
	require.NoError(cscbrepairlock.AcquireTagShared(ctx, sharedHolder, tag))

	sharedPeer, err := db.BeginTx(ctx, nil)
	require.NoError(err)
	sharedCtx, cancelShared := context.WithTimeout(ctx, time.Second)
	require.NoError(cscbrepairlock.AcquireTagShared(sharedCtx, sharedPeer, tag))
	cancelShared()
	require.NoError(sharedPeer.Rollback(), "same-tag shared writer locks must coexist")

	exclusiveWaiter, err := db.BeginTx(ctx, nil)
	require.NoError(err)
	exclusiveCtx, cancelExclusive := context.WithTimeout(ctx, 250*time.Millisecond)
	err = cscbrepairlock.AcquireTag(exclusiveCtx, exclusiveWaiter, tag)
	cancelExclusive()
	require.Error(err, "an active writer lock must block an exclusive repair transition")
	_ = exclusiveWaiter.Rollback()
	require.NoError(sharedHolder.Rollback())

	exclusiveHolder, err := db.BeginTx(ctx, nil)
	require.NoError(err)
	defer func() { _ = exclusiveHolder.Rollback() }()
	require.NoError(cscbrepairlock.AcquireTag(ctx, exclusiveHolder, tag))

	sharedWaiter, err := db.BeginTx(ctx, nil)
	require.NoError(err)
	blockedWriterCtx, cancelBlockedWriter := context.WithTimeout(ctx, 250*time.Millisecond)
	err = cscbrepairlock.AcquireTagShared(blockedWriterCtx, sharedWaiter, tag)
	cancelBlockedWriter()
	require.Error(err, "an active exclusive repair transition must block a writer")
	_ = sharedWaiter.Rollback()
}
