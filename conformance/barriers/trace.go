package barriers

// Decision describes one payload-free barrier event.
type Decision string

const (
	// DecisionArrived records a configured participant arrival.
	DecisionArrived Decision = "arrived"
	// DecisionReleased records an accepted release decision.
	DecisionReleased Decision = "released"
	// DecisionReleaseRejected records a rejected release decision.
	DecisionReleaseRejected Decision = "release_rejected"
	// DecisionCanceled records a canceled participant wait.
	DecisionCanceled Decision = "canceled"
)

// Event is one bounded, payload-free barrier trace record.
//
// It contains only configured IDs, configured participant roles, an arrival
// ordinal, and a release decision. It never contains a payload or credentials.
type Event struct {
	Sequence     uint64    `json:"sequence"`
	BarrierID    BarrierID `json:"barrier_id"`
	Participant  string    `json:"participant,omitempty"`
	ArrivalOrder uint64    `json:"arrival_order,omitempty"`
	Decision     Decision  `json:"decision"`
}

// Trace is a chronological snapshot of retained trace records.
//
// Dropped is the number of older records discarded by the bounded ring buffer.
type Trace struct {
	Events  []Event `json:"events"`
	Dropped uint64  `json:"dropped"`
}

type traceBuffer struct {
	limit    int
	events   []Event
	next     int
	dropped  uint64
	sequence uint64
}

func newTraceBuffer(limit int) traceBuffer {
	return traceBuffer{
		limit:  limit,
		events: make([]Event, 0, limit),
	}
}

func (b *traceBuffer) add(event Event) {
	b.sequence++
	event.Sequence = b.sequence

	if len(b.events) < b.limit {
		b.events = append(b.events, event)
		return
	}

	b.events[b.next] = event
	b.next = (b.next + 1) % b.limit
	b.dropped++
}

func (b *traceBuffer) snapshot() Trace {
	events := make([]Event, 0, len(b.events))
	if len(b.events) < b.limit || b.next == 0 {
		events = append(events, b.events...)
	} else {
		events = append(events, b.events[b.next:]...)
		events = append(events, b.events[:b.next]...)
	}
	return Trace{Events: events, Dropped: b.dropped}
}
