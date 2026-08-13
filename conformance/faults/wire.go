package faults

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
)

// WireFault is an HTTP RoundTripper that injects one deterministic fault.
//
// It never stores request or response payload bytes. Replay reopens request
// bodies through Request.GetBody and rejects bodies that cannot be reopened.
type WireFault struct {
	ctx      context.Context
	upstream http.RoundTripper
	options  WireOptions

	mu     sync.Mutex
	active map[uint64]io.Closer
	next   uint64
	closed bool
	done   chan struct{}

	stop       func() bool
	unregister func()
	closeOnce  sync.Once
	closeErr   error
}

// NewWireFault creates a cleanup-safe typed transport fault.
func NewWireFault(ctx context.Context, owner *Controller, upstream http.RoundTripper, options WireOptions) (*WireFault, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if upstream == nil {
		return nil, ErrNilRoundTripper
	}
	if err := validateWireOptions(options); err != nil {
		return nil, err
	}

	fault := &WireFault{
		ctx:      ctx,
		upstream: upstream,
		options:  options,
		active:   make(map[uint64]io.Closer),
		done:     make(chan struct{}),
	}
	fault.mu.Lock()
	fault.stop = context.AfterFunc(ctx, func() {
		_ = fault.Close()
	})
	fault.mu.Unlock()
	if owner != nil {
		unregister, err := owner.register(fault)
		if err != nil {
			_ = fault.Close()
			return nil, err
		}
		fault.setUnregister(unregister)
	}
	if err := ctx.Err(); err != nil {
		_ = fault.Close()
		return nil, err
	}
	return fault, nil
}

// RoundTrip performs the configured fault without recording payload content.
func (f *WireFault) RoundTrip(request *http.Request) (*http.Response, error) {
	if f == nil {
		return nil, ErrFaultClosed
	}
	if err := f.ready(request); err != nil {
		return nil, err
	}

	switch f.options.Mode {
	case WireResponseLoss:
		response, err := f.upstream.RoundTrip(request)
		if err != nil {
			closeResponse(response)
			return nil, err
		}
		if err := f.discardCompletedResponse(request.Context(), response); err != nil {
			return nil, err
		}
		return nil, ErrResponseLost
	case WireTimeout:
		response, err := f.upstream.RoundTrip(request)
		if err != nil {
			closeResponse(response)
			return nil, err
		}
		if err := f.discardCompletedResponse(request.Context(), response); err != nil {
			return nil, err
		}
		return nil, timeoutError{}
	case WireTruncate:
		response, err := f.upstream.RoundTrip(request)
		if err != nil {
			closeResponse(response)
			return nil, err
		}
		return f.truncateResponse(request.Context(), response)
	case WireDuplicate:
		return f.repeatRequest(request, 2)
	case WireReplay:
		return f.repeatRequest(request, f.options.ReplayCount)
	default:
		return nil, ErrInvalidWireOptions
	}
}

// Close closes in-flight response bodies and prevents further transport use.
func (f *WireFault) Close() error {
	if f == nil {
		return ErrFaultClosed
	}
	f.closeOnce.Do(func() {
		f.mu.Lock()
		f.closed = true
		stop := f.stop
		f.stop = nil
		closers := make([]io.Closer, 0, len(f.active))
		for _, closer := range f.active {
			closers = append(closers, closer)
		}
		f.active = make(map[uint64]io.Closer)
		unregister := f.unregister
		f.unregister = nil
		f.mu.Unlock()
		if stop != nil {
			stop()
		}

		var failures []error
		for _, closer := range closers {
			if err := closer.Close(); err != nil {
				failures = append(failures, err)
			}
		}
		if unregister != nil {
			unregister()
		}
		f.closeErr = errors.Join(failures...)
		close(f.done)
	})
	return f.closeErr
}

// Done closes after in-flight transport resources have been closed.
func (f *WireFault) Done() <-chan struct{} {
	if f == nil {
		return nil
	}
	return f.done
}

func (f *WireFault) ready(request *http.Request) error {
	if request == nil {
		return ErrInvalidWireOptions
	}
	if err := checkContext(f.ctx); err != nil {
		return err
	}
	if err := request.Context().Err(); err != nil {
		return err
	}
	f.mu.Lock()
	closed := f.closed
	f.mu.Unlock()
	if closed {
		return ErrFaultClosed
	}
	return nil
}

func (f *WireFault) discardCompletedResponse(ctx context.Context, response *http.Response) error {
	if response == nil {
		return ErrResponseLost
	}
	body := response.Body
	if body == nil {
		return checkWireContexts(f.ctx, ctx)
	}
	release, err := f.activate(body)
	if err != nil {
		_ = body.Close()
		return err
	}
	defer release()

	stop := context.AfterFunc(ctx, func() {
		_ = body.Close()
	})
	_, copyErr := io.Copy(io.Discard, body)
	closeErr := body.Close()
	stop()
	if err := checkWireContexts(f.ctx, ctx); err != nil {
		return err
	}
	return errors.Join(copyErr, closeErr)
}

func (f *WireFault) truncateResponse(ctx context.Context, response *http.Response) (*http.Response, error) {
	if response == nil {
		return nil, ErrInvalidWireOptions
	}
	body := response.Body
	if body == nil {
		body = http.NoBody
	}
	release, err := f.activate(body)
	if err != nil {
		_ = body.Close()
		return nil, err
	}
	response.Body = newTruncatedBody(ctx, body, f.options.TruncateAfter, release)
	response.ContentLength = -1
	if response.Header != nil {
		response.Header.Del("Content-Length")
	}
	return response, nil
}

func (f *WireFault) repeatRequest(request *http.Request, count int) (*http.Response, error) {
	var first *http.Response
	for index := 0; index < count; index++ {
		if err := f.ready(request); err != nil {
			closeResponse(first)
			return nil, err
		}
		response, err := f.replayOnce(request)
		if err != nil {
			closeResponse(first)
			return nil, err
		}
		if index == 0 {
			first, err = f.manageResponse(request.Context(), response)
			if err != nil {
				closeResponse(response)
				return nil, err
			}
			continue
		}
		if err := f.discardCompletedResponse(request.Context(), response); err != nil {
			closeResponse(first)
			return nil, err
		}
	}
	if first == nil {
		return nil, ErrInvalidWireOptions
	}
	return first, nil
}

func (f *WireFault) replayOnce(request *http.Request) (*http.Response, error) {
	clone, err := replayRequest(request)
	if err != nil {
		return nil, err
	}
	response, roundTripErr := f.upstream.RoundTrip(clone)
	if clone.Body != nil {
		_ = clone.Body.Close()
	}
	if roundTripErr != nil {
		closeResponse(response)
		return nil, roundTripErr
	}
	return response, nil
}

func (f *WireFault) manageResponse(ctx context.Context, response *http.Response) (*http.Response, error) {
	if response == nil {
		return nil, ErrInvalidWireOptions
	}
	body := response.Body
	if body == nil {
		body = http.NoBody
	}
	release, err := f.activate(body)
	if err != nil {
		_ = body.Close()
		return nil, err
	}
	response.Body = newManagedBody(ctx, body, release)
	return response, nil
}

func (f *WireFault) activate(closer io.Closer) (func(), error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, ErrFaultClosed
	}
	f.next++
	id := f.next
	f.active[id] = closer
	var once sync.Once
	return func() {
		once.Do(func() {
			f.mu.Lock()
			delete(f.active, id)
			f.mu.Unlock()
		})
	}, nil
}

func (f *WireFault) setUnregister(unregister func()) {
	f.mu.Lock()
	closed := f.closed
	if !closed {
		f.unregister = unregister
	}
	f.mu.Unlock()
	if closed && unregister != nil {
		unregister()
	}
}

func validateWireOptions(options WireOptions) error {
	switch options.Mode {
	case WireResponseLoss, WireTimeout, WireDuplicate:
		if options.TruncateAfter != 0 || options.ReplayCount != 0 {
			return ErrInvalidWireOptions
		}
	case WireTruncate:
		if options.TruncateAfter < 0 || options.ReplayCount != 0 {
			return ErrInvalidWireOptions
		}
	case WireReplay:
		if options.TruncateAfter != 0 || options.ReplayCount < 2 {
			return ErrInvalidWireOptions
		}
	default:
		return ErrInvalidWireOptions
	}
	return nil
}

func replayRequest(request *http.Request) (*http.Request, error) {
	clone := request.Clone(request.Context())
	if request.Body == nil || request.Body == http.NoBody {
		clone.Body = http.NoBody
		return clone, nil
	}
	if request.GetBody == nil {
		return nil, ErrRequestNotReplayable
	}
	body, err := request.GetBody()
	if err != nil {
		return nil, err
	}
	clone.Body = body
	return clone, nil
}

func closeResponse(response *http.Response) {
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
}

func checkWireContexts(faultContext, requestContext context.Context) error {
	if err := checkContext(faultContext); err != nil {
		return err
	}
	if requestContext == nil {
		return ErrNilContext
	}
	return requestContext.Err()
}

type timeoutError struct{}

func (timeoutError) Error() string {
	return ErrInjectedTimeout.Error()
}

func (timeoutError) Timeout() bool {
	return true
}

func (timeoutError) Temporary() bool {
	return true
}

func (timeoutError) Unwrap() error {
	return context.DeadlineExceeded
}

func (timeoutError) Is(target error) bool {
	return target == ErrInjectedTimeout
}

type managedBody struct {
	source  io.ReadCloser
	release func()
	stop    func() bool
	once    sync.Once
	err     error
}

func newManagedBody(ctx context.Context, source io.ReadCloser, release func()) *managedBody {
	body := &managedBody{source: source, release: release}
	body.stop = context.AfterFunc(ctx, func() {
		_ = source.Close()
	})
	return body
}

func (b *managedBody) Read(data []byte) (int, error) {
	return b.source.Read(data)
}

func (b *managedBody) Close() error {
	b.once.Do(func() {
		if b.stop != nil {
			b.stop()
		}
		b.err = b.source.Close()
		if b.release != nil {
			b.release()
		}
	})
	return b.err
}

type truncatedBody struct {
	mu sync.Mutex

	source    io.ReadCloser
	remaining int64
	exhausted bool
	closed    bool
	release   func()
	stop      func() bool
	once      sync.Once
	err       error
}

func newTruncatedBody(ctx context.Context, source io.ReadCloser, remaining int64, release func()) *truncatedBody {
	body := &truncatedBody{source: source, remaining: remaining, release: release}
	body.stop = context.AfterFunc(ctx, func() {
		_ = source.Close()
	})
	return body
}

func (b *truncatedBody) Read(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return 0, io.ErrClosedPipe
	}
	if b.exhausted || b.remaining == 0 {
		b.exhausted = true
		_ = b.source.Close()
		return 0, io.ErrUnexpectedEOF
	}
	if int64(len(data)) > b.remaining {
		data = data[:b.remaining]
	}
	count, err := b.source.Read(data)
	b.remaining -= int64(count)
	if b.remaining == 0 {
		b.exhausted = true
		_ = b.source.Close()
		return count, io.ErrUnexpectedEOF
	}
	return count, err
}

func (b *truncatedBody) Close() error {
	b.once.Do(func() {
		b.mu.Lock()
		b.closed = true
		if b.stop != nil {
			b.stop()
		}
		b.err = b.source.Close()
		release := b.release
		b.mu.Unlock()
		if release != nil {
			release()
		}
	})
	return b.err
}
