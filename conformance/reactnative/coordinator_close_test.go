package reactnative

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// An exchange holds the coordinator mutex while it waits for a proxy barrier.
// Close must end that wait itself, because nothing else releases the barrier
// and the caller's cleanup context does not bound mutex acquisition.
func TestCloseEndsBlockedExchangeBarrierWithExpiredContext(t *testing.T) {
	upstream := httptest.NewServer(http.NotFoundHandler())
	defer upstream.Close()
	for _, test := range []struct {
		name  string
		setup func(t *testing.T) blockedExchange
	}{
		{name: "queue replay response-loss push", setup: func(t *testing.T) blockedExchange {
			c, err := NewQueueReplayCoordinator(QueueReplayCoordinatorConfig{Scenario: loadQueueReplayAuthoredScenario(t), Platform: "ios", ServerURL: upstream.URL, AuthToken: "unit-token"})
			if err != nil {
				t.Fatalf("create queue-replay coordinator: %v", err)
			}
			c.armResponseLossPush()
			c.stage = queueReplayStageResponseLossPushReady
			return blockedExchange{mu: &c.mu, listener: c.listener, close: c.Close, advance: func(ctx context.Context) error {
				_, err := c.advanceLocked(ctx, 1)
				return err
			}, stage: func() any { return c.stage }}
		}},
		{name: "queue replay terminal pull", setup: func(t *testing.T) blockedExchange {
			c, err := NewQueueReplayCoordinator(QueueReplayCoordinatorConfig{Scenario: loadQueueReplayAuthoredScenario(t), Platform: "ios", ServerURL: upstream.URL, AuthToken: "unit-token"})
			if err != nil {
				t.Fatalf("create queue-replay coordinator: %v", err)
			}
			c.armReplayPull()
			c.stage = queueReplayStageReplayBegun
			return blockedExchange{mu: &c.mu, listener: c.listener, close: c.Close, advance: func(ctx context.Context) error {
				_, err := c.advanceLocked(ctx, 1)
				return err
			}, stage: func() any { return c.stage }}
		}},
		{name: "push response loss commit", setup: func(t *testing.T) blockedExchange {
			c, err := NewPushResponseLossCoordinator(PushResponseLossCoordinatorConfig{Scenario: loadPushResponseLossAuthoredScenario(t), Platform: "ios", ServerURL: upstream.URL, AuthToken: "unit-token"})
			if err != nil {
				t.Fatalf("create push-response-loss coordinator: %v", err)
			}
			c.stage = pushResponseLossStageAwaitBackoff
			return blockedExchange{mu: &c.mu, listener: c.listener, close: c.Close, advance: func(ctx context.Context) error {
				_, err := c.advanceLocked(ctx, 1)
				return err
			}, stage: func() any { return c.stage }}
		}},
		{name: "push response loss replay", setup: func(t *testing.T) blockedExchange {
			c, err := NewPushResponseLossCoordinator(PushResponseLossCoordinatorConfig{Scenario: loadPushResponseLossAuthoredScenario(t), Platform: "ios", ServerURL: upstream.URL, AuthToken: "unit-token"})
			if err != nil {
				t.Fatalf("create push-response-loss coordinator: %v", err)
			}
			c.stage = pushResponseLossStageFinalCapture
			return blockedExchange{mu: &c.mu, listener: c.listener, close: c.Close, advance: func(ctx context.Context) error {
				_, err := c.advanceLocked(ctx, 1)
				return err
			}, stage: func() any { return c.stage }}
		}},
		{name: "forged cursor first page", setup: func(t *testing.T) blockedExchange {
			c, err := NewForgedCursorCoordinator(ForgedCursorCoordinatorConfig{Scenario: loadForgedCursorAuthoredScenario(t), Platform: "ios", ServerURL: upstream.URL, AuthToken: "unit-token"})
			if err != nil {
				t.Fatalf("create forged-cursor coordinator: %v", err)
			}
			c.stage = forgedCursorStageFirstPage
			return blockedExchange{mu: &c.mu, listener: c.listener, close: c.Close, advance: func(ctx context.Context) error {
				_, err := c.advanceLocked(ctx, 1)
				return err
			}, stage: func() any { return c.stage }}
		}},
		{name: "pending cycle accepted push", setup: func(t *testing.T) blockedExchange {
			c, err := NewPendingCycleCoordinator(PendingCycleCoordinatorConfig{Scenario: loadPendingCycleAuthoredScenario(t), Platform: "ios", ServerURL: upstream.URL, AuthToken: "unit-token"})
			if err != nil {
				t.Fatalf("create pending-cycle coordinator: %v", err)
			}
			c.stage = pendingCycleStageInitialPushBegun
			return blockedExchange{mu: &c.mu, listener: c.listener, close: c.Close, advance: func(ctx context.Context) error {
				_, err := c.advanceLocked(ctx, 1)
				return err
			}, stage: func() any { return c.stage }}
		}},
		{name: "retention reconnect sealed push", setup: func(t *testing.T) blockedExchange {
			c, err := NewRetentionReconnectCoordinator(RetentionReconnectCoordinatorConfig{Scenario: loadRetentionReconnectAuthoredScenario(t), Platform: "ios", ServerURL: upstream.URL, AuthToken: "unit-token"})
			if err != nil {
				t.Fatalf("create retention-reconnect coordinator: %v", err)
			}
			c.stage = retentionReconnectStageInitialBegun
			return blockedExchange{mu: &c.mu, listener: c.listener, close: c.Close, advance: func(ctx context.Context) error {
				_, err := c.advanceLocked(ctx, 1)
				return err
			}, stage: func() any { return c.stage }}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			exchange := test.setup(t)
			stage := exchange.stage()
			// The exchange context is cancelled only by cleanup, so a failed
			// Close cannot leave the exchange goroutine or the mutex held.
			exchangeContext, cancelExchange := context.WithCancel(context.Background())
			locked := make(chan struct{})
			advanceDone := make(chan struct{})
			var advanceErr error
			go func() {
				defer close(advanceDone)
				exchange.mu.Lock()
				close(locked)
				advanceErr = exchange.advance(exchangeContext)
				exchange.mu.Unlock()
			}()
			<-locked
			expired, cancel := context.WithCancel(context.Background())
			cancel()
			closeDone := make(chan struct{})
			go func() {
				defer close(closeDone)
				_ = exchange.close(expired)
			}()
			t.Cleanup(func() {
				cancelExchange()
				_ = exchange.listener.Close()
				for _, done := range []chan struct{}{advanceDone, closeDone} {
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Error("coordinator goroutine did not stop within the cleanup bound")
					}
				}
			})
			select {
			case <-advanceDone:
				if advanceErr == nil {
					t.Fatal("blocked exchange advanced after Close")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not end the blocked exchange barrier wait")
			}
			select {
			case <-closeDone:
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not return after the blocked exchange ended")
			}
			if got := exchange.stage(); got != stage {
				t.Fatalf("stage after closed exchange = %v, want %v", got, stage)
			}
			// A deadline bounds Accept if Close left the listener open.
			if tcp, ok := exchange.listener.(*net.TCPListener); ok {
				_ = tcp.SetDeadline(time.Now().Add(time.Second))
			}
			if connection, err := exchange.listener.Accept(); !errors.Is(err, net.ErrClosed) {
				if connection != nil {
					_ = connection.Close()
				}
				t.Fatalf("coordinator listener after Close: %v, want net.ErrClosed", err)
			}
		})
	}
}

type blockedExchange struct {
	mu       *sync.Mutex
	listener net.Listener
	advance  func(context.Context) error
	close    func(context.Context) error
	stage    func() any
}
