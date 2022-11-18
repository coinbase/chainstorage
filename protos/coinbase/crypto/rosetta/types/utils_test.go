package types

import (
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
	"testing"
)

func TestMarshalToAny(t *testing.T) {
	tests := []struct {
		name  string
		input interface{}
	}{
		{
			name:  "string",
			input: "nan",
		},
		{
			name:  "int",
			input: 123,
		},
		{
			name:  "list",
			input: []interface{}{"a", "b"},
		},
		{
			name:  "map",
			input: map[string]interface{}{"k1": "v1", "k2": []interface{}{"v1", "v2"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)

			anyValue, err := MarshalToAny(test.input)
			require.NoError(err)

			value := &structpb.Value{}
			err = anyValue.UnmarshalTo(value)
			require.NoError(err)

			require.EqualValues(test.input, value.AsInterface())
		})
	}
}
