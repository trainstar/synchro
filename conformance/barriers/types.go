// Package barriers provides deterministic, named synchronization barriers.
package barriers

import (
	"context"
	"errors"
)

const (
	// DefaultTraceLimit bounds retained trace events for one controller.
	DefaultTraceLimit = 256
	// MaximumTraceLimit prevents a caller from creating an unbounded trace.
	MaximumTraceLimit = 4096
)

// BarrierID identifies one authored synchronization point.
type BarrierID string

// Controller coordinates participants at named barriers.
type Controller interface {
	Await(ctx context.Context, id BarrierID, participant string) error
	Release(ctx context.Context, id BarrierID) error
	Trace() Trace
}

// Definition configures one named barrier.
//
// Participants are stable role names. They are the only participant strings
// that the controller records in its trace.
type Definition struct {
	ID           BarrierID
	Participants []string
	ReleaseOrder int
}

var (
	// ErrNilContext reports a missing cancellation boundary.
	ErrNilContext = errors.New("barrier context is nil")
	// ErrInvalidDefinition reports an unsafe or incomplete barrier definition.
	ErrInvalidDefinition = errors.New("barrier definition is invalid")
	// ErrUnknownBarrier reports an ID that was not configured.
	ErrUnknownBarrier = errors.New("barrier is unknown")
	// ErrUnknownParticipant reports a role that was not configured.
	ErrUnknownParticipant = errors.New("barrier participant is unknown")
	// ErrReleaseOrder reports an out-of-order release attempt.
	ErrReleaseOrder = errors.New("barrier release order is invalid")
	// ErrAlreadyReleased reports a duplicate release attempt.
	ErrAlreadyReleased = errors.New("barrier is already released")
	// ErrInvalidArrivalCount reports a nonpositive arrival count.
	ErrInvalidArrivalCount = errors.New("barrier arrival count is invalid")
	// ErrTraceLimit reports a trace capacity outside the safe range.
	ErrTraceLimit = errors.New("barrier trace limit is invalid")
)
