package blackbox

import (
	"context"
	"testing"
	"time"
)

// appliedExtensionUpdateFixture gives the state that a successful
// ApplyExtensionUpdate leaves. Its PostgreSQL process is absent.
func appliedExtensionUpdateFixture(t *testing.T) extensionUpdateFixture {
	t.Helper()
	fixture := newExtensionUpdateFixture(t)
	harness := fixture.harness
	harness.socketDir = t.TempDir()
	harness.port = 1
	harness.config.StartupTimeout = 100 * time.Millisecond
	harness.extensionUpdated = true
	t.Cleanup(func() {
		if err := harness.closeDatabaseHandles(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return fixture
}

func TestFinishExtensionUpdateRejectsFalsePreconditions(t *testing.T) {
	t.Run("control", func(t *testing.T) {
		harness := appliedExtensionUpdateFixture(t).harness
		if err := harness.FinishExtensionUpdate(context.Background()); err == nil {
			t.Fatal("extension update completion without PostgreSQL succeeded")
		}
		if len(harness.databaseHandles) == 0 {
			t.Fatal("control harness did not reach the database connection")
		}
	})
	var missingContext context.Context
	expiredContext, cancel := context.WithCancel(context.Background())
	cancel()
	for _, test := range []struct {
		name   string
		ctx    context.Context
		mutate func(*testing.T, *Harness)
	}{
		{name: "missing context", ctx: missingContext},
		{name: "expired context", ctx: expiredContext},
		{name: "source not ready", ctx: context.Background(), mutate: func(_ *testing.T, h *Harness) { h.sourceReady = false }},
		{name: "no applied update", ctx: context.Background(), mutate: func(_ *testing.T, h *Harness) { h.extensionUpdated = false }},
		{name: "completed update", ctx: context.Background(), mutate: func(_ *testing.T, h *Harness) { h.extensionUpdateCompleted = true }},
		{name: "adapter runs", ctx: context.Background(), mutate: func(_ *testing.T, h *Harness) { h.adapter = &ownedProcess{} }},
		{name: "closing", ctx: context.Background(), mutate: func(_ *testing.T, h *Harness) { h.closeStarted = true }},
		{name: "closed", ctx: context.Background(), mutate: func(t *testing.T, h *Harness) {
			if err := h.Close(context.Background()); err != nil {
				t.Fatalf("close applied update harness: %v", err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := appliedExtensionUpdateFixture(t)
			harness := fixture.harness
			if test.mutate != nil {
				test.mutate(t, harness)
			}
			adapter := harness.adapter
			completed := harness.extensionUpdateCompleted
			if err := harness.FinishExtensionUpdate(test.ctx); err == nil {
				t.Fatal("extension update completion accepted a false precondition")
			}
			if len(harness.databaseHandles) != 0 {
				t.Fatal("extension update completion used PostgreSQL before it checked its preconditions")
			}
			if harness.adapter != adapter || harness.extensionUpdateCompleted != completed {
				t.Fatal("extension update completion changed the harness before it checked its preconditions")
			}
			requireNoExtensionInstallation(t, fixture, nil)
		})
	}
	var missingHarness *Harness
	if err := missingHarness.FinishExtensionUpdate(context.Background()); err == nil {
		t.Fatal("extension update completion accepted a missing harness")
	}
}

func TestApplyExtensionUpdateRejectsClosedHarness(t *testing.T) {
	fixture := newExtensionUpdateFixture(t)
	harness := fixture.harness
	if err := harness.Close(context.Background()); err != nil {
		t.Fatalf("close update baseline harness: %v", err)
	}
	if _, err := harness.ApplyExtensionUpdate(context.Background()); err == nil {
		t.Fatal("extension update accepted a closed harness")
	}
	if harness.extensionUpdated {
		t.Fatal("extension update marked a closed harness as updated")
	}
	requireNoExtensionInstallation(t, fixture, nil)
}
