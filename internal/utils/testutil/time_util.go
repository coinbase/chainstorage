package testutil

import (
	"time"

	"github.com/golang/protobuf/ptypes/timestamp"
)

func MustTime(value string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		panic(err)
	}
	return t
}

func MustTimestamp(value string) *timestamp.Timestamp {
	t := MustTime(value)
	return &timestamp.Timestamp{
		Seconds: t.Unix(),
		Nanos:   int32(t.Nanosecond()),
	}
}
