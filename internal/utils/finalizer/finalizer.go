package finalizer

import (
	"io"
)

type (
	// Finalizer implements the defer-close pattern to guarantee:
	// - close is always called exactly once
	// - any error is properly surfaced
	// See more details below:
	// - https://www.digitalocean.com/community/tutorials/understanding-defer-in-go
	// - https://www.joeshaw.org/dont-defer-close-on-writable-files/
	Finalizer struct {
		close  CloseFn
		closed bool
	}

	CloseFn func() error
)

func WithClose(close CloseFn) *Finalizer {
	return &Finalizer{close: close, closed: false}
}

func WithCloser(closer io.Closer) *Finalizer {
	return WithClose(closer.Close)
}

// Finalize should be called in the defer statement. Any error is ignored.
func (f *Finalizer) Finalize() {
	_ = f.Close()
}

// Close should be called at the end of the function.
// As a result, the error from close is surfaced only when there is no error before Close is called.
func (f *Finalizer) Close() error {
	if f.closed {
		return nil
	}

	f.closed = true
	return f.close()
}
