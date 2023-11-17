package testutil

import (
	rosetta "github.com/coinbase/chainstorage/protos/coinbase/crypto/rosetta/types"
)

func NumberOfSolanaVoteTransactions(require *Assertions, rosettaTxs []*rosetta.Transaction) int {
	numOfVoteTransactions := 0
	for _, rosettaTx := range rosettaTxs {
		metadata, err := rosetta.ToSDKMetadata(rosettaTx.Metadata)
		require.NoError(err)
		isVoteTransaction, ok := metadata["is_solana_vote_transaction"]
		if ok && isVoteTransaction.(bool) {
			numOfVoteTransactions++
		}
	}
	return numOfVoteTransactions
}
