package errors

import "errors"

var (
	ErrRequestCanceled   = errors.New("request canceled")
	ErrDownloadFailure   = errors.New("download failure")
	ErrOutOfRange        = errors.New("out of range")
	ErrInvalidHeight     = errors.New("height is lower than block start height")
	ErrInvalidEventId    = errors.New("event id is lower than event start id")
	ErrItemNotFound      = errors.New("item not found")
	ErrNoEventHistory    = errors.New("no event history")
	ErrNoEventAvailable  = errors.New("no event available")
	ErrNoMaxEventIdFound = errors.New("no max event id found")
)
