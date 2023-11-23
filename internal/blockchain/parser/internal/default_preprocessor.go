package internal

import (
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type PreProcessor interface {
	PreProcessNativeBlock(expected, actual *api.NativeBlock) error
}

type defaultPreProcessor struct {
}

func NewDefaultChecker(params ParserParams) (Checker, error) {
	return NewChecker(params, &defaultPreProcessor{}, &defaultRosettaProcessor{})
}

func (p *defaultPreProcessor) PreProcessNativeBlock(expected, actual *api.NativeBlock) error {
	return nil
}
