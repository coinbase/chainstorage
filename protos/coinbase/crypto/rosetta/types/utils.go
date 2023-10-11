package types

import (
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Int64Deref dereferences a int64 pointer
func Int64Deref(p *int64) int64 {
	if p == nil {
		return 0
	}

	return *p
}

// Int64Ref returns reference of int64
func Int64Ref(v int64) *int64 {
	return &v
}

// StringDeref dereferences a string pointer
func StringDeref(p *string) string {
	if p == nil {
		return ""
	}

	return *p
}

// StringRef returns reference of string
func StringRef(v string) *string {
	return &v
}

// ConvertMillisecondsToTimestamp converts milliseconds to timestamp
func ConvertMillisecondsToTimestamp(milliseconds int64) *timestamppb.Timestamp {
	return &timestamppb.Timestamp{
		Seconds: milliseconds / 1000,
		Nanos:   int32(milliseconds%1000) * 1000000,
	}
}

// MarshalToAny converts interface to anypb.Any
func MarshalToAny(v interface{}) (*anypb.Any, error) {
	pv, err := structpb.NewValue(v)
	if err != nil {
		return nil, xerrors.Errorf("%v cannot convert to structpb.Value: %w", v, err)
	}

	value, err := anypb.New(pv)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal into Any for %v: %w", pv, err)
	}

	return value, nil
}

// UnmarshalToInterface converts anypb.Any to interface
func UnmarshalToInterface(v *anypb.Any) (interface{}, error) {
	value := &structpb.Value{}
	err := v.UnmarshalTo(value)
	if err != nil {
		return nil, err
	}
	return value.AsInterface(), nil
}
