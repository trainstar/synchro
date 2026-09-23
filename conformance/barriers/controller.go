package barriers

import (
	"context"
	"regexp"
	"sort"
	"sync"
)

var (
	barrierIDPattern   = regexp.MustCompile(`^BAR-[A-Z0-9]+(?:-[A-Z0-9]+)*-[0-9]{3}$`)
	participantPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.-]{0,63}$`)
)

// DeterministicController coordinates configured barriers without time-based
// ordering. Release uses channels. Arrival observation uses a condition
// variable so a runner can wait for a known state without sleeping.
type DeterministicController struct {
	mu sync.Mutex

	arrivals *sync.Cond
	barriers map[BarrierID]*barrier
	release  []BarrierID
	next     int
	trace    traceBuffer
}

type barrier struct {
	participants map[string]struct{}
	released     bool
	release      chan struct{}
	arrivals     uint64
	pending      uint64
}

// NewController creates a controller with the default bounded trace capacity.
func NewController(definitions []Definition) (*DeterministicController, error) {
	return NewControllerWithTraceLimit(definitions, DefaultTraceLimit)
}

// NewControllerWithTraceLimit creates a controller with a bounded trace.
func NewControllerWithTraceLimit(definitions []Definition, traceLimit int) (*DeterministicController, error) {
	if traceLimit < 1 || traceLimit > MaximumTraceLimit {
		return nil, ErrTraceLimit
	}
	if len(definitions) == 0 {
		return nil, ErrInvalidDefinition
	}

	controller := &DeterministicController{
		barriers: make(map[BarrierID]*barrier, len(definitions)),
		trace:    newTraceBuffer(traceLimit),
	}
	controller.arrivals = sync.NewCond(&controller.mu)

	ordered := append([]Definition(nil), definitions...)
	sort.Slice(ordered, func(left, right int) bool {
		return ordered[left].ReleaseOrder < ordered[right].ReleaseOrder
	})

	for index, definition := range ordered {
		if definition.ReleaseOrder != index+1 {
			return nil, ErrInvalidDefinition
		}
		if !barrierIDPattern.MatchString(string(definition.ID)) {
			return nil, ErrInvalidDefinition
		}
		if _, exists := controller.barriers[definition.ID]; exists {
			return nil, ErrInvalidDefinition
		}
		if len(definition.Participants) == 0 {
			return nil, ErrInvalidDefinition
		}

		participants := make(map[string]struct{}, len(definition.Participants))
		for _, participant := range definition.Participants {
			if !participantPattern.MatchString(participant) {
				return nil, ErrInvalidDefinition
			}
			if _, exists := participants[participant]; exists {
				return nil, ErrInvalidDefinition
			}
			participants[participant] = struct{}{}
		}

		controller.barriers[definition.ID] = &barrier{
			participants: participants,
			release:      make(chan struct{}),
		}
		controller.release = append(controller.release, definition.ID)
	}

	return controller, nil
}

// Await records a configured participant arrival and waits for release.
func (c *DeterministicController) Await(ctx context.Context, id BarrierID, participant string) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	c.mu.Lock()
	barrier, err := c.lookupParticipantLocked(id, participant)
	if err != nil {
		c.mu.Unlock()
		return err
	}

	barrier.arrivals++
	barrier.pending++
	arrivalOrder := barrier.arrivals
	c.trace.add(Event{
		BarrierID:    id,
		Participant:  participant,
		ArrivalOrder: arrivalOrder,
		Decision:     DecisionArrived,
	})
	c.arrivals.Broadcast()
	release := barrier.release
	alreadyReleased := barrier.released
	c.mu.Unlock()

	if alreadyReleased {
		c.finishAwait(id, participant, arrivalOrder, false)
		return nil
	}

	select {
	case <-release:
		c.finishAwait(id, participant, arrivalOrder, false)
		return nil
	case <-ctx.Done():
		c.finishAwait(id, participant, arrivalOrder, true)
		return ctx.Err()
	}
}

// Release permits all current and future waiters at one configured barrier.
func (c *DeterministicController) Release(ctx context.Context, id BarrierID) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	barrier, exists := c.barriers[id]
	if !exists {
		return ErrUnknownBarrier
	}
	if barrier.released {
		c.trace.add(Event{BarrierID: id, Decision: DecisionReleaseRejected})
		return ErrAlreadyReleased
	}
	if c.next >= len(c.release) || c.release[c.next] != id {
		c.trace.add(Event{BarrierID: id, Decision: DecisionReleaseRejected})
		return ErrReleaseOrder
	}

	barrier.released = true
	c.next++
	c.trace.add(Event{BarrierID: id, Decision: DecisionReleased})
	close(barrier.release)
	c.arrivals.Broadcast()
	return nil
}

// WaitForArrivals waits until a barrier has recorded count arrivals.
//
// It uses a condition variable and context cancellation. It never uses a time
// delay to infer ordering.
func (c *DeterministicController) WaitForArrivals(ctx context.Context, id BarrierID, count int) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if count < 1 {
		return ErrInvalidArrivalCount
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	barrier, exists := c.barriers[id]
	if !exists {
		return ErrUnknownBarrier
	}

	stop := context.AfterFunc(ctx, func() {
		c.mu.Lock()
		c.arrivals.Broadcast()
		c.mu.Unlock()
	})
	defer stop()

	for barrier.arrivals < uint64(count) && ctx.Err() == nil {
		c.arrivals.Wait()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

// WaitForIdle waits until no participant remains blocked in Await.
func (c *DeterministicController) WaitForIdle(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	stop := context.AfterFunc(ctx, func() {
		c.mu.Lock()
		c.arrivals.Broadcast()
		c.mu.Unlock()
	})
	defer stop()

	for c.pendingLocked() != 0 && ctx.Err() == nil {
		c.arrivals.Wait()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

// Trace returns a chronological copy of the bounded payload-free trace.
func (c *DeterministicController) Trace() Trace {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.trace.snapshot()
}

func (c *DeterministicController) lookupParticipantLocked(id BarrierID, participant string) (*barrier, error) {
	barrier, exists := c.barriers[id]
	if !exists {
		return nil, ErrUnknownBarrier
	}
	if _, exists := barrier.participants[participant]; !exists {
		return nil, ErrUnknownParticipant
	}
	return barrier, nil
}

func (c *DeterministicController) finishAwait(id BarrierID, participant string, arrivalOrder uint64, canceled bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	barrier, exists := c.barriers[id]
	if !exists {
		return
	}
	if barrier.pending > 0 {
		barrier.pending--
	}
	if canceled {
		c.trace.add(Event{
			BarrierID:    id,
			Participant:  participant,
			ArrivalOrder: arrivalOrder,
			Decision:     DecisionCanceled,
		})
	}
	c.arrivals.Broadcast()
}

func (c *DeterministicController) pendingLocked() uint64 {
	var pending uint64
	for _, barrier := range c.barriers {
		pending += barrier.pending
	}
	return pending
}

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	return ctx.Err()
}
