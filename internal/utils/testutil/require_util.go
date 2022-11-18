package testutil

import (
	"reflect"

	"github.com/golang/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
)

// Assertions extends require.Assertions to support comparison of proto messages.
type Assertions struct {
	*require.Assertions
}

var protoMessageType = reflect.TypeOf((*proto.Message)(nil)).Elem()

func Require(t require.TestingT) *Assertions {
	return &Assertions{
		Assertions: require.New(t),
	}
}

func (a *Assertions) Equal(expected interface{}, actual interface{}, msgAndArgs ...interface{}) {
	// Use protocmp if proto messages are being compared.
	if _, ok := expected.(proto.Message); ok {
		if _, ok := actual.(proto.Message); ok {
			a.equalProto(expected, actual, msgAndArgs...)
			return
		}
	}

	// Use protocmp if slices of proto messages are being compared.
	if reflect.TypeOf(expected).Kind() == reflect.Slice && reflect.TypeOf(expected).Elem().Implements(protoMessageType) {
		if reflect.TypeOf(actual).Kind() == reflect.Slice && reflect.TypeOf(actual).Elem().Implements(protoMessageType) {
			a.equalProto(expected, actual, msgAndArgs...)
			return
		}
	}

	// Otherwise, use the original implementation.
	a.Assertions.Equal(expected, actual, msgAndArgs...)
}

func (a *Assertions) equalProto(expected interface{}, actual interface{}, msgAndArgs ...interface{}) {
	if diff := cmp.Diff(expected, actual, protocmp.Transform()); diff != "" {
		a.FailNow(diff, msgAndArgs...)
	}
}
