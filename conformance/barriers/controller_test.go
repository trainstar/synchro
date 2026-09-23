package barriers

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestControllerCoordinatesArrivalAndRelease(t *testing.T) {
	controller := testController(t, DefaultTraceLimit)
	ctx := context.Background()
	result := make(chan error, 1)
	go func() {
		result <- controller.Await(ctx, "BAR-TEST-ONE-001", "system-under-test")
	}()
	if err := controller.WaitForArrivals(ctx, "BAR-TEST-ONE-001", 1); err != nil {
		t.Fatalf("wait for arrival: %v", err)
	}
	if err := controller.Release(ctx, "BAR-TEST-ONE-001"); err != nil {
		t.Fatalf("release barrier: %v", err)
	}
	if err := <-result; err != nil {
		t.Fatalf("await result: %v", err)
	}
	if err := controller.WaitForIdle(ctx); err != nil {
		t.Fatalf("wait for idle: %v", err)
	}

	trace := controller.Trace()
	want := []Event{
		{
			Sequence:     1,
			BarrierID:    "BAR-TEST-ONE-001",
			Participant:  "system-under-test",
			ArrivalOrder: 1,
			Decision:     DecisionArrived,
		},
		{
			Sequence:  2,
			BarrierID: "BAR-TEST-ONE-001",
			Decision:  DecisionReleased,
		},
	}
	if !reflect.DeepEqual(trace.Events, want) {
		t.Fatalf("trace events = %#v, want %#v", trace.Events, want)
	}
	if trace.Dropped != 0 {
		t.Fatalf("dropped trace events = %d, want 0", trace.Dropped)
	}
}

func TestControllerRejectsOutOfOrderAndRepeatedRelease(t *testing.T) {
	controller, err := NewController([]Definition{
		{ID: "BAR-TEST-ONE-001", Participants: []string{"worker"}, ReleaseOrder: 1},
		{ID: "BAR-TEST-TWO-001", Participants: []string{"worker"}, ReleaseOrder: 2},
	})
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	ctx := context.Background()
	if err := controller.Release(ctx, "BAR-TEST-TWO-001"); !errors.Is(err, ErrReleaseOrder) {
		t.Fatalf("out-of-order release error = %v, want %v", err, ErrReleaseOrder)
	}
	if err := controller.Release(ctx, "BAR-TEST-ONE-001"); err != nil {
		t.Fatalf("release first barrier: %v", err)
	}
	if err := controller.Release(ctx, "BAR-TEST-ONE-001"); !errors.Is(err, ErrAlreadyReleased) {
		t.Fatalf("duplicate release error = %v, want %v", err, ErrAlreadyReleased)
	}
	if err := controller.Release(ctx, "BAR-TEST-TWO-001"); err != nil {
		t.Fatalf("release second barrier: %v", err)
	}

	trace := controller.Trace()
	if len(trace.Events) != 4 {
		t.Fatalf("trace event count = %d, want 4", len(trace.Events))
	}
	if trace.Events[0].Decision != DecisionReleaseRejected || trace.Events[2].Decision != DecisionReleaseRejected {
		t.Fatalf("release rejection decisions = %#v", trace.Events)
	}
}

func TestControllerCancellationCleansPendingWait(t *testing.T) {
	controller := testController(t, DefaultTraceLimit)
	parent := context.Background()
	ctx, cancel := context.WithCancel(parent)
	result := make(chan error, 1)
	go func() {
		result <- controller.Await(ctx, "BAR-TEST-ONE-001", "system-under-test")
	}()
	if err := controller.WaitForArrivals(parent, "BAR-TEST-ONE-001", 1); err != nil {
		t.Fatalf("wait for arrival: %v", err)
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("await cancellation error = %v, want context cancellation", err)
	}
	if err := controller.WaitForIdle(parent); err != nil {
		t.Fatalf("wait for idle after cancellation: %v", err)
	}

	trace := controller.Trace()
	if len(trace.Events) != 2 || trace.Events[1].Decision != DecisionCanceled {
		t.Fatalf("cancellation trace = %#v", trace.Events)
	}
}

func TestControllerTraceIsBoundedAndPayloadFree(t *testing.T) {
	controller := testController(t, 2)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- controller.Await(ctx, "BAR-TEST-ONE-001", "system-under-test")
	}()
	if err := controller.WaitForArrivals(context.Background(), "BAR-TEST-ONE-001", 1); err != nil {
		t.Fatalf("wait for arrival: %v", err)
	}
	if err := controller.Await(context.Background(), "BAR-TEST-ONE-001", "credential-canary-value"); !errors.Is(err, ErrUnknownParticipant) {
		t.Fatalf("unknown participant error = %v, want %v", err, ErrUnknownParticipant)
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("await cancellation error = %v", err)
	}
	if err := controller.Release(context.Background(), "BAR-TEST-ONE-001"); err != nil {
		t.Fatalf("release barrier: %v", err)
	}

	trace := controller.Trace()
	if len(trace.Events) != 2 || trace.Dropped != 1 {
		t.Fatalf("bounded trace = %#v, want two retained events and one drop", trace)
	}
	encoded, err := json.Marshal(trace)
	if err != nil {
		t.Fatalf("marshal trace: %v", err)
	}
	for _, forbidden := range []string{"credential-canary-value", "payload", "authorization", "token"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("trace contains forbidden value %q: %s", forbidden, encoded)
		}
	}
}

func TestControllerProducesTheSameTraceAcrossRuns(t *testing.T) {
	var expected Trace
	for run := 0; run < 32; run++ {
		trace := runDeterministicTrace(t)
		if run == 0 {
			expected = trace
			continue
		}
		if !reflect.DeepEqual(trace, expected) {
			t.Fatalf("run %d trace = %#v, want %#v", run, trace, expected)
		}
	}
}

func testController(t *testing.T, traceLimit int) *DeterministicController {
	t.Helper()
	controller, err := NewControllerWithTraceLimit([]Definition{
		{
			ID:           "BAR-TEST-ONE-001",
			Participants: []string{"runner", "system-under-test"},
			ReleaseOrder: 1,
		},
	}, traceLimit)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	return controller
}

func runDeterministicTrace(t *testing.T) Trace {
	t.Helper()
	controller := testController(t, DefaultTraceLimit)
	ctx := context.Background()
	result := make(chan error, 1)
	go func() {
		result <- controller.Await(ctx, "BAR-TEST-ONE-001", "system-under-test")
	}()
	if err := controller.WaitForArrivals(ctx, "BAR-TEST-ONE-001", 1); err != nil {
		t.Fatalf("wait for arrival: %v", err)
	}
	if err := controller.Release(ctx, "BAR-TEST-ONE-001"); err != nil {
		t.Fatalf("release barrier: %v", err)
	}
	if err := <-result; err != nil {
		t.Fatalf("await result: %v", err)
	}
	return controller.Trace()
}
