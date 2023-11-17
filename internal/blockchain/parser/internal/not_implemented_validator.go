package internal

import (
	"context"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	notImplementedValidatorImpl struct{}
)

var (
	_ TrustlessValidator = (*notImplementedValidatorImpl)(nil)
)

func NewNotImplementedValidator(params ParserParams) TrustlessValidator {
	return &notImplementedValidatorImpl{}
}

func (v *notImplementedValidatorImpl) ValidateBlock(ctx context.Context, block *api.NativeBlock) error {
	return ErrNotImplemented
}

func (v *notImplementedValidatorImpl) ValidateAccountState(ctx context.Context, req *api.ValidateAccountStateRequest) (*api.ValidateAccountStateResponse, error) {
	return nil, ErrNotImplemented
}
