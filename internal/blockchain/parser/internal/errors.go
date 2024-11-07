package internal

import "errors"

var (
	ErrInvalidChain      = errors.New("invalid chain")
	ErrNotImplemented    = errors.New("not implemented")
	ErrNotFound          = errors.New("not found")
	ErrInvalidParameters = errors.New("invalid input parameters")
)
