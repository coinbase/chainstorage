package errors

import "golang.org/x/xerrors"

var (
	ErrRequestCanceled   = xerrors.New("request canceled")
	ErrDownloadFailure   = xerrors.New("download failure")
	ErrOutOfRange        = xerrors.New("out of range")
	ErrInvalidHeight     = xerrors.New("height is lower than block start height")
	ErrInvalidEventId    = xerrors.New("event id is lower than event start id")
	ErrItemNotFound      = xerrors.New("item not found")
	ErrNoEventHistory    = xerrors.New("no event history")
	ErrNoEventAvailable  = xerrors.New("no event available")
	ErrNoMaxEventIdFound = xerrors.New("no max event id found")
)
