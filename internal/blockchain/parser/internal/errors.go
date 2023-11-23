package internal

import (
	"golang.org/x/xerrors"
)

var (
	ErrInvalidChain      = xerrors.New("invalid chain")
	ErrNotImplemented    = xerrors.New("not implemented")
	ErrNotFound          = xerrors.New("not found")
	ErrInvalidParameters = xerrors.New("invalid input parameters")
)
