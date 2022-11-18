package parser

import (
	"golang.org/x/xerrors"
)

var (
	ErrInvalidChain   = xerrors.New("invalid chain")
	ErrNotImplemented = xerrors.New("not implemented")
)
