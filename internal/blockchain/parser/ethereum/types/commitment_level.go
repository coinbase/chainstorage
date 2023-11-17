package types

// CommitmentLevel is the level of commitment to use when querying the blockchain.
// See https://www.alchemy.com/overviews/ethereum-commitment-levels
type CommitmentLevel string

const (
	CommitmentLevelLatest    CommitmentLevel = "latest"
	CommitmentLevelSafe      CommitmentLevel = "safe"
	CommitmentLevelFinalized CommitmentLevel = "finalized"
)
