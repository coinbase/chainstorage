package firestore

const (
	// https://firebase.google.com/docs/firestore/manage-data/transactions#batched-writes
	maxBulkWriteSize = 500
	maxWriteWorkers  = 10
)
