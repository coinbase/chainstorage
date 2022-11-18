package dlq

import (
	"context"
	"testing"

	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/s3"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
)

func TestIntegrationDLQ(t *testing.T) {
	require := testutil.Require(t)

	cfg, err := config.New()
	require.NoError(err)
	cfg.AWS.DLQ.DelaySecs = 0

	var q DLQ
	app := testapp.New(
		t,
		testapp.WithIntegration(),
		testapp.WithConfig(cfg),
		Module,
		s3.Module,
		fx.Populate(&q),
	)
	defer app.Close()

	sent := Message{
		Topic: FailedBlockTopic,
		Data:  FailedBlockData{Tag: 3, Height: 123},
	}
	err = q.SendMessage(context.Background(), &sent)
	require.NoError(err)

	received, err := q.ReceiveMessage(context.Background())
	require.NoError(err)
	require.Equal(FailedBlockTopic, received.Topic)
	require.Equal(0, received.Retries)
	require.NotEmpty(received.ReceiptHandle)
	data, ok := received.Data.(*FailedBlockData)
	require.True(ok)
	require.Equal(uint32(3), data.Tag)
	require.Equal(uint64(123), data.Height)

	err = q.DeleteMessage(context.Background(), received)
	require.NoError(err)

	_, err = q.ReceiveMessage(context.Background())
	require.Error(err)
	require.True(xerrors.Is(err, ErrNotFound))
}

func TestIntegrationDLQ_Resend(t *testing.T) {
	require := testutil.Require(t)

	cfg, err := config.New()
	require.NoError(err)
	cfg.AWS.DLQ.DelaySecs = 0

	var q DLQ
	app := testapp.New(
		t,
		testapp.WithIntegration(),
		testapp.WithConfig(cfg),
		Module,
		s3.Module,
		fx.Populate(&q),
	)
	defer app.Close()

	sent := Message{
		Topic: FailedTransactionTraceTopic,
		Data:  FailedTransactionTraceData{Tag: 1, Height: 321, IgnoredTransactions: []int{2, 3, 5}},
	}
	err = q.SendMessage(context.Background(), &sent)
	require.NoError(err)

	const retries = 4
	for i := 0; i < retries; i++ {
		received, err := q.ReceiveMessage(context.Background())
		require.NoError(err)
		require.Equal(i, received.Retries)

		err = q.ResendMessage(context.Background(), received)
		require.NoError(err)
	}

	received, err := q.ReceiveMessage(context.Background())
	require.NoError(err)

	require.Equal(FailedTransactionTraceTopic, received.Topic)
	require.Equal(retries, received.Retries)
	require.NotEmpty(received.ReceiptHandle)
	data, ok := received.Data.(*FailedTransactionTraceData)
	require.True(ok)
	require.Equal(uint32(1), data.Tag)
	require.Equal(uint64(321), data.Height)
	require.Equal([]int{2, 3, 5}, data.IgnoredTransactions)

	err = q.DeleteMessage(context.Background(), received)
	require.NoError(err)

	_, err = q.ReceiveMessage(context.Background())
	require.Error(err)
	require.True(xerrors.Is(err, ErrNotFound))
}

func TestIntegrationDLQ_UnknownTopic(t *testing.T) {
	require := testutil.Require(t)

	cfg, err := config.New()
	require.NoError(err)
	cfg.AWS.DLQ.DelaySecs = 0

	var q DLQ
	app := testapp.New(
		t,
		testapp.WithIntegration(),
		testapp.WithConfig(cfg),
		Module,
		s3.Module,
		fx.Populate(&q),
	)
	defer app.Close()

	sent := Message{
		Topic: "foo",
		Data:  FailedBlockData{Tag: 3, Height: 123},
	}
	err = q.SendMessage(context.Background(), &sent)
	require.NoError(err)

	received, err := q.ReceiveMessage(context.Background())
	require.NoError(err)
	require.Equal("foo", received.Topic)
	require.Equal(0, received.Retries)
	require.NotEmpty(received.ReceiptHandle)
	require.Nil(received.Data)

	err = q.DeleteMessage(context.Background(), received)
	require.NoError(err)

	_, err = q.ReceiveMessage(context.Background())
	require.Error(err)
	require.True(xerrors.Is(err, ErrNotFound))
}
