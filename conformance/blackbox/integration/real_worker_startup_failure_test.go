package integration

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
)

const (
	realWorkerStartupFailureRestartLimit = 60 * time.Second
	realWorkerStartupRecoveryMargin      = 40 * time.Second
)

// TestRealWorkerStartupIdentityFailureRestarts proves that a worker identity failure in the startup gate is not a stale extension state.
// The worker must exit after the startup retry budget, and the postmaster must start it again.
// The worker must recover readiness after the operator restores the group membership.
func TestRealWorkerStartupIdentityFailureRestarts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	waitForRealWorkerReadiness(t, ctx, harness, admin, harness.StartupTimeout())

	if err := harness.SetWorkerGroupMembership(ctx, false); err != nil {
		t.Fatalf("revoke worker group membership: %v", err)
	}
	priorPID := waitForRealWALWorkerPID(t, ctx, harness, admin, 0, harness.StartupTimeout(), "WAL worker is not unique after the revoke")
	// The worker can exit on its own after the revoke. The next wait then observes its successor.
	if _, err := admin.ExecContext(ctx, "SELECT pg_catalog.pg_terminate_backend($1)", priorPID); err != nil {
		t.Fatalf("terminate WAL worker backend %d: %v", priorPID, err)
	}
	firstPID := waitForRealWALWorkerPID(t, ctx, harness, admin, priorPID, harness.StartupTimeout(), "WAL worker did not start after the termination")
	restartedPID := waitForRealWALWorkerPID(t, ctx, harness, admin, firstPID, realWorkerStartupFailureRestartLimit, "WAL worker did not exit after the startup retry budget")
	t.Logf("worker startup identity failure: prior=%d first=%d restarted=%d", priorPID, firstPID, restartedPID)

	if err := harness.SetWorkerGroupMembership(ctx, true); err != nil {
		t.Fatalf("grant worker group membership: %v", err)
	}
	waitForRealWorkerReadiness(t, ctx, harness, admin, harness.StartupTimeout()+realWorkerStartupRecoveryMargin)
}

// waitForRealWALWorkerPID waits for exactly one WAL worker backend with a pid other than excludedPID.
func waitForRealWALWorkerPID(
	t *testing.T,
	ctx context.Context,
	harness *blackbox.Harness,
	admin *sql.DB,
	excludedPID int,
	timeout time.Duration,
	failure string,
) int {
	t.Helper()
	var count, pid int
	var err error
	deadline := time.Now().Add(timeout)
	for {
		err = admin.QueryRowContext(ctx, realWALWorkerActivityQuery).Scan(&count, &pid)
		if err == nil && count == 1 && pid > 0 && pid != excludedPID {
			return pid
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("%s within %s: excluded=%d count=%d pid=%d err=%v; %s", failure, timeout, excludedPID, count, pid, err, harness.FailureDiagnostics())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func waitForRealWorkerReadiness(t *testing.T, ctx context.Context, harness *blackbox.Harness, admin *sql.DB, timeout time.Duration) {
	t.Helper()
	var detail map[string]any
	deadline := time.Now().Add(timeout)
	for {
		detail = loadIssue49Health(t, ctx, admin)
		if ready, ok := detail["ready"].(bool); ok && ready {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("worker readiness did not recover within %s: %#v; %s", timeout, detail, harness.FailureDiagnostics())
		}
		time.Sleep(250 * time.Millisecond)
	}
}
