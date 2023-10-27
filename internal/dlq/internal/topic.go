package internal

const (
	FailedBlockTopic            = "failed_block"
	FailedTransactionTraceTopic = "failed_transaction_trace"
)

type (
	FailedBlockData struct {
		Tag    uint32
		Height uint64
	}

	FailedTransactionTraceData struct {
		Tag                 uint32
		Height              uint64
		Hash                string
		IgnoredTransactions []int
	}
)
