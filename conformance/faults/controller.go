package faults

import (
	"context"
	"errors"
	"sort"
	"sync"
)

// Controller owns fault handles and closes them when its context is canceled.
//
// A controller has no payload trace. It stores only cleanup handles.
type Controller struct {
	mu sync.Mutex

	handles map[uint64]Handle
	next    uint64
	closed  bool
	done    chan struct{}

	stop      func() bool
	closeOnce sync.Once
	closeErr  error
}

// NewController creates a cleanup owner bound to ctx.
func NewController(ctx context.Context) (*Controller, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	controller := &Controller{
		handles: make(map[uint64]Handle),
		done:    make(chan struct{}),
	}
	controller.mu.Lock()
	controller.stop = context.AfterFunc(ctx, func() {
		_ = controller.Close()
	})
	controller.mu.Unlock()
	return controller, nil
}

// Close closes every registered handle exactly once.
func (c *Controller) Close() error {
	if c == nil {
		return ErrControllerClosed
	}
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		stop := c.stop
		c.stop = nil
		ids := make([]uint64, 0, len(c.handles))
		for id := range c.handles {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(left, right int) bool {
			return ids[left] < ids[right]
		})
		handles := make([]Handle, 0, len(ids))
		for _, id := range ids {
			handles = append(handles, c.handles[id])
		}
		c.handles = make(map[uint64]Handle)
		c.mu.Unlock()
		if stop != nil {
			stop()
		}

		var failures []error
		for _, handle := range handles {
			if err := handle.Close(); err != nil {
				failures = append(failures, err)
			}
		}
		c.closeErr = errors.Join(failures...)
		close(c.done)
	})
	return c.closeErr
}

// Done closes after the controller has closed all owned handles.
func (c *Controller) Done() <-chan struct{} {
	if c == nil {
		return nil
	}
	return c.done
}

func (c *Controller) register(handle Handle) (func(), error) {
	if c == nil {
		return func() {}, nil
	}
	if handle == nil {
		return nil, ErrFaultClosed
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrControllerClosed
	}
	c.next++
	id := c.next
	c.handles[id] = handle
	c.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			delete(c.handles, id)
			c.mu.Unlock()
		})
	}, nil
}
