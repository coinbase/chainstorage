package picker

import (
	"fmt"
	"math"
	"testing"

	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
)

const (
	concurrency = 10
	numPicks    = 100000
	numChoices  = 5
)

func TestPicker_Empty(t *testing.T) {
	require := testutil.Require(t)
	picker := New(nil)
	require.Nil(picker.Next())
}

func TestPicker_Simple(t *testing.T) {
	require := testutil.Require(t)
	picker := New([]*Choice{
		{
			Item:   &config.Endpoint{Name: "foo"},
			Weight: 1,
		},
	})
	choice := picker.Next().(*config.Endpoint)
	require.Equal("foo", choice.Name)
}

func TestPicker_Weighted(t *testing.T) {
	require := testutil.Require(t)
	choices := make([]*Choice, numChoices)
	totalWeights := uint32(0)
	for i := 0; i < numChoices; i++ {
		weight := int(^uint8(0) - uint8(i*2))
		endpoint := &config.Endpoint{
			Name: fmt.Sprintf("name%d", i),
		}
		totalWeights += uint32(weight)
		choices[i] = &Choice{
			Item:   endpoint,
			Weight: weight,
		}
	}

	picker := New(choices)
	stats := make(map[interface{}]*atomic.Int32, numChoices)
	for _, choice := range choices {
		stats[choice.Item] = new(atomic.Int32)
	}

	group := errgroup.Group{}
	for i := 0; i < concurrency; i++ {
		i := i
		group.Go(func() error {
			for j := 0; j < numPicks; j++ {
				if j%concurrency == i {
					choice := picker.Next()
					require.NotNil(choice)
					stats[choice].Add(1)
				}
			}
			return nil
		})
	}

	err := group.Wait()
	require.NoError(err)

	for _, choice := range choices {
		expectedPickProbability := float64(choice.Weight) / float64(totalWeights)
		actualPickedProbability := float64(stats[choice.Item].Load()) * 1.0 / float64(numPicks)
		endpoint := choice.Item.(*config.Endpoint)
		require.True(
			math.Abs(expectedPickProbability-actualPickedProbability) < 0.01,
			"endpoint %+v: expectedPickProbability:%3f, actualPickedProbability:%3f",
			endpoint,
			expectedPickProbability,
			actualPickedProbability,
		)
	}
}
