package testutil

import (
	"fmt"

	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

type ProtoMatcher struct {
	expected proto.Message
}

// Matches implements gomock.Matcher.
func (m *ProtoMatcher) Matches(x any) bool {
	return proto.Equal(x.(proto.Message), m.expected)
}

// String implements gomock.Matcher.
func (m *ProtoMatcher) String() string {
	return fmt.Sprintf("is equal to %+v", m.expected)
}

func MatchProto(message proto.Message) gomock.Matcher {
	return &ProtoMatcher{expected: message}
}
