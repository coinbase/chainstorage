package testutil

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	rt "github.com/coinbase/rosetta-sdk-go/types"
	"github.com/golang/protobuf/jsonpb"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// AssertEqualErrors asserts that the errors are equal, by checking if the
// actual error is caused by the expected error or that they are both nil.
func AssertEqualErrors(t *testing.T, testName string, expectedError error, actualError error) {
	if expectedError == nil {
		assert.Nil(
			t,
			actualError,
			fmt.Sprintf(
				"test name: %s; expected error is nil, actual error: %v",
				testName,
				actualError))
	} else {
		assert.EqualError(
			t,
			errors.Cause(actualError),
			expectedError.Error(),
			fmt.Sprintf(
				"test name: %s, expected error: %v, actual error %v",
				testName,
				expectedError.Error(),
				errors.Cause(actualError)))
	}
}

func LoadRosettaRaw(
	block *rt.BlockResponse,
	transactions *rt.BlockTransactionResponse,
	blockChain common.Blockchain,
	network common.Network) (*api.Block, error) {
	header, err := json.Marshal(block)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal test rosetta block: %w", err)
	}

	rosettaRaw := &api.Block{
		Blockchain: blockChain,
		Network:    network,
		Metadata: &api.BlockMetadata{
			Tag:        0,
			Height:     1,
			Hash:       "hash",
			ParentHash: "parentHash",
		},
		Blobdata: &api.Block_Rosetta{
			Rosetta: &api.RosettaBlobdata{
				Header: header,
			},
		},
	}

	if transactions != nil {
		txData, err := json.Marshal(transactions)
		if err != nil {
			return nil, xerrors.Errorf("failed to marshal test rosetta transaction: %w", err)
		}
		rosettaRaw.GetRosetta().OtherTransactions = [][]byte{txData}
	}
	return rosettaRaw, nil
}

func LoadRosettaBlock(file string) (*rt.BlockResponse, error) {
	data, err := fixtures.ReadFile(file)
	if err != nil {
		return nil, err
	}
	var block rt.BlockResponse
	if err = json.Unmarshal(data, &block); err != nil {
		return nil, err
	}
	return &block, nil
}

func LoadRosettaTransaction(file string) (*rt.BlockTransactionResponse, error) {
	data, err := fixtures.ReadFile(file)
	if err != nil {
		return nil, err
	}
	var tx rt.BlockTransactionResponse
	if err = json.Unmarshal(data, &tx); err != nil {
		return nil, err
	}
	return &tx, nil
}

func LoadRawBlock(file string) (*api.Block, error) {
	data, err := fixtures.ReadFile(file)
	if err != nil {
		return nil, err
	}

	var rawBlock api.Block

	u := jsonpb.Unmarshaler{}
	if err := u.Unmarshal(bytes.NewBuffer(data), &rawBlock); err != nil {
		return nil, err
	}

	return &rawBlock, nil
}
