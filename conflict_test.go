package synchro

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestLWWResolver_ClientNewer(t *testing.T) {
	resolver := &LWWResolver{}
	now := time.Now()

	res, err := resolver.Resolve(context.Background(), Conflict{
		Table:      "orders",
		RecordID:   "abc",
		ClientData: json.RawMessage(`{"name":"new"}`),
		ServerData: json.RawMessage(`{"name":"old"}`),
		ClientTime: now.Add(1 * time.Second),
		ServerTime: now,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Winner != "client" {
		t.Errorf("Winner = %q, want %q", res.Winner, "client")
	}
}

func TestLWWResolver_ServerNewer(t *testing.T) {
	resolver := &LWWResolver{}
	now := time.Now()

	res, err := resolver.Resolve(context.Background(), Conflict{
		Table:      "orders",
		RecordID:   "abc",
		ClientData: json.RawMessage(`{"name":"old"}`),
		ServerData: json.RawMessage(`{"name":"new"}`),
		ClientTime: now,
		ServerTime: now.Add(1 * time.Second),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Winner != "server" {
		t.Errorf("Winner = %q, want %q", res.Winner, "server")
	}
}

func TestLWWResolver_EqualTimestamps_ServerWins(t *testing.T) {
	resolver := &LWWResolver{}
	now := time.Now()

	res, err := resolver.Resolve(context.Background(), Conflict{
		Table:      "orders",
		RecordID:   "abc",
		ClientTime: now,
		ServerTime: now,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Equal timestamps: clientTime.After(serverTime) is false, so server wins.
	if res.Winner != "server" {
		t.Errorf("Winner = %q, want %q (server wins on tie)", res.Winner, "server")
	}
}

func TestLWWResolver_WithBaseVersion_ServerUnchanged(t *testing.T) {
	resolver := &LWWResolver{}
	now := time.Now()
	base := now

	res, err := resolver.Resolve(context.Background(), Conflict{
		Table:       "orders",
		RecordID:    "abc",
		ClientTime:  now.Add(1 * time.Second),
		ServerTime:  now,
		BaseVersion: &base,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// BaseVersion == ServerTime means server unchanged, client wins.
	if res.Winner != "client" {
		t.Errorf("Winner = %q, want %q", res.Winner, "client")
	}
}

func TestLWWResolver_WithBaseVersion_ServerChanged_ClientNewer(t *testing.T) {
	resolver := &LWWResolver{}
	now := time.Now()
	base := now.Add(-5 * time.Second)

	res, err := resolver.Resolve(context.Background(), Conflict{
		Table:       "orders",
		RecordID:    "abc",
		ClientTime:  now.Add(1 * time.Second),
		ServerTime:  now,
		BaseVersion: &base,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Server changed since base, but client is newer.
	if res.Winner != "client" {
		t.Errorf("Winner = %q, want %q", res.Winner, "client")
	}
}

func TestLWWResolver_WithBaseVersion_ServerChanged_ServerNewer(t *testing.T) {
	resolver := &LWWResolver{}
	now := time.Now()
	base := now.Add(-5 * time.Second)

	res, err := resolver.Resolve(context.Background(), Conflict{
		Table:       "orders",
		RecordID:    "abc",
		ClientTime:  now.Add(-1 * time.Second),
		ServerTime:  now,
		BaseVersion: &base,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Winner != "server" {
		t.Errorf("Winner = %q, want %q", res.Winner, "server")
	}
}

func TestLWWResolver_ClockSkewTolerance(t *testing.T) {
	// Client timestamp is 500ms before server, but tolerance is 1s.
	// Adjusted client time = clientTime + 1s = server + 500ms -> client wins.
	resolver := &LWWResolver{ClockSkewTolerance: 1 * time.Second}
	now := time.Now()

	res, err := resolver.Resolve(context.Background(), Conflict{
		Table:      "orders",
		RecordID:   "abc",
		ClientTime: now.Add(-500 * time.Millisecond),
		ServerTime: now,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Winner != "client" {
		t.Errorf("Winner = %q, want %q (clock skew tolerance should tip to client)", res.Winner, "client")
	}
}

func TestLWWResolver_ClockSkewTolerance_NotEnough(t *testing.T) {
	// Client timestamp is 2s before server, tolerance is only 1s.
	// Adjusted client time = clientTime + 1s = server - 1s -> server wins.
	resolver := &LWWResolver{ClockSkewTolerance: 1 * time.Second}
	now := time.Now()

	res, err := resolver.Resolve(context.Background(), Conflict{
		Table:      "orders",
		RecordID:   "abc",
		ClientTime: now.Add(-2 * time.Second),
		ServerTime: now,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Winner != "server" {
		t.Errorf("Winner = %q, want %q", res.Winner, "server")
	}
}

func TestServerWinsResolver(t *testing.T) {
	resolver := &ServerWinsResolver{}
	now := time.Now()

	res, err := resolver.Resolve(context.Background(), Conflict{
		Table:      "orders",
		RecordID:   "abc",
		ClientTime: now.Add(10 * time.Second),
		ServerTime: now,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Winner != "server" {
		t.Errorf("Winner = %q, want %q", res.Winner, "server")
	}
}
