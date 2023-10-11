package testutil

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

func MustTime(value string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		panic(err)
	}
	return t
}

func MustTimestamp(value string) *timestamppb.Timestamp {
	t := MustTime(value)
	return &timestamppb.Timestamp{
		Seconds: t.Unix(),
		Nanos:   int32(t.Nanosecond()),
	}
}
