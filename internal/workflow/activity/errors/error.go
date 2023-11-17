package errors

const (
	ErrTypeNodeProvider               = "ErrTypeNodeProvider"               // Used in monitor workflow, indicate master/slave fetching block failures
	ErrTypeConsensusClusterFailure    = "ErrTypeConsensusClusterFailure"    // Used in poller workflow, indicate consensus cluster fetching block failures
	ErrTypeConsensusValidationFailure = "ErrTypeConsensusValidationFailure" // Used in poller workflow, indicate consensus validation failures
)
