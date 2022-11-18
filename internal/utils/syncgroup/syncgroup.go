package syncgroup

import (
	"context"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

type (
	Group interface {
		Go(fn func() error)
		Wait() error
	}

	Option   func(group *groupImpl)
	FilterFn func(err error) error

	groupImpl struct {
		group  *errgroup.Group
		ctx    context.Context
		err    error
		sem    *semaphore.Weighted
		filter FilterFn
	}
)

func New(ctx context.Context, opts ...Option) (Group, context.Context) {
	g, ctx := errgroup.WithContext(ctx)
	group := &groupImpl{
		group:  g,
		ctx:    ctx,
		filter: nopFilter,
	}

	for _, opt := range opts {
		opt(group)
	}

	return group, ctx
}

func WithThrottling(limit int) Option {
	return func(group *groupImpl) {
		group.sem = semaphore.NewWeighted(int64(limit))
	}
}

func WithFilter(filter FilterFn) Option {
	return func(group *groupImpl) {
		group.filter = filter
	}
}

func (g *groupImpl) Go(fn func() error) {
	if g.sem == nil {
		g.group.Go(func() error {
			return g.filter(fn())
		})
		return
	}

	// Block until a slot is available.
	if err := g.sem.Acquire(g.ctx, 1); err != nil {
		// When any worker (i.e. fn) returns an error, the context would be cancelled by errgroup, causing Acquire to fail.
		// Save the error and return it in Wait.
		g.err = err
		return
	}

	g.group.Go(func() error {
		defer g.sem.Release(1)
		return g.filter(fn())
	})
}

func (g *groupImpl) Wait() error {
	// The error returned by the worker is more important than the one from the semaphore.
	if err := g.group.Wait(); err != nil {
		return err
	}

	if g.err != nil {
		return g.err
	}

	return nil
}

func nopFilter(err error) error {
	return err
}
