package integration

import (
	"context"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
)

func TestRealExtensionReinstallRebindsWorkerSlot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)

	before := connectRealProtocolClient(t, ctx, harness, token, "extension-reinstall-before")
	rebuildRealScope(t, ctx, harness, token, before, "user:diagnostic-user", "00000000-0000-4000-8c03-000000000011")
	rebuildRealScope(t, ctx, harness, token, before, "cf:global", "00000000-0000-4000-8c03-000000000012")
	beforeTable := requireRealTable(t, before, "cf_items")
	beforeID := "00000000-0000-4000-8c03-000000000001"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		beforeID,
		"diagnostic-user",
		"before-extension-reinstall",
	); err != nil {
		t.Fatalf("insert pre-reinstall source row: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", beforeID)
	pullUntilRealRecords(t, ctx, harness, token, before, []realRecordExpectation{{
		scopeID:  "user:diagnostic-user",
		table:    beforeTable,
		recordID: beforeID,
		value:    "before-extension-reinstall",
	}})
	acknowledgeRealClientCursors(t, ctx, harness, token, before)
	if err := harness.Source().ExecContext(ctx, "DELETE FROM cf_items WHERE id = $1", beforeID); err != nil {
		t.Fatalf("delete pre-reinstall source row: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", beforeID)

	reinstall, err := harness.ReinstallExtension(ctx)
	if err != nil {
		t.Fatalf("reinstall extension: %v", err)
	}
	rebound := waitForReinstalledWorker(t, ctx, harness, reinstall, 0)
	if rebound.ActiveRegistryGeneration <= 0 {
		t.Fatalf("reinstalled worker has no active registry generation: %#v", rebound)
	}
	if err := harness.RestoreDiagnosticRegistrations(ctx); err != nil {
		t.Fatalf("restore diagnostic registrations: %v", err)
	}
	activated := waitForReinstalledWorker(t, ctx, harness, reinstall, rebound.ActiveRegistryGeneration)
	if activated.WorkerRegistryGeneration != activated.ActiveRegistryGeneration ||
		activated.PendingRegistryGenerationCount != 0 {
		t.Fatalf("reinstalled registry generations did not activate: %#v", activated)
	}

	after := connectRealProtocolClient(t, ctx, harness, token, "extension-reinstall-after")
	rebuildRealScope(t, ctx, harness, token, after, "user:diagnostic-user", "00000000-0000-4000-8c03-000000000021")
	rebuildRealScope(t, ctx, harness, token, after, "cf:global", "00000000-0000-4000-8c03-000000000022")
	afterTable := requireRealTable(t, after, "cf_items")
	afterID := "00000000-0000-4000-8c03-000000000002"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		afterID,
		"diagnostic-user",
		"after-extension-reinstall",
	); err != nil {
		t.Fatalf("insert post-reinstall source row: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", afterID)
	pullUntilRealRecords(t, ctx, harness, token, after, []realRecordExpectation{{
		scopeID:  "user:diagnostic-user",
		table:    afterTable,
		recordID: afterID,
		value:    "after-extension-reinstall",
	}})
	acknowledgeRealClientCursors(t, ctx, harness, token, after)
	final := waitForReinstalledWorker(t, ctx, harness, reinstall, rebound.ActiveRegistryGeneration)
	if !final.NoValidationFailurePoison {
		t.Fatalf("reinstalled worker has validation_failed poison: %#v", final)
	}
}

func waitForReinstalledWorker(
	t *testing.T,
	ctx context.Context,
	harness *blackbox.Harness,
	reinstall blackbox.ExtensionReinstallResult,
	minimumGeneration int64,
) blackbox.ExtensionReinstallObservation {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	var observation blackbox.ExtensionReinstallObservation
	var err error
	for time.Now().Before(deadline) {
		observation, err = harness.Operator().ObserveExtensionReinstall(ctx, reinstall.ReinstallLSN)
		if err == nil && observation.WorkerPID > 0 && observation.WorkerPID != reinstall.PriorWorkerPID &&
			observation.ActiveSlotName == harness.Names().ReplicationSlot && observation.RestartLSN != "" &&
			observation.SlotActive && observation.RestartLSNAtOrAfterReinstall &&
			observation.ActiveRegistryGeneration > minimumGeneration &&
			observation.PendingRegistryGenerationCount == 0 && observation.NoValidationFailurePoison {
			return observation
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("reinstalled worker did not bind a fresh active slot: %#v, %v; %s", observation, err, harness.FailureDiagnostics())
	return blackbox.ExtensionReinstallObservation{}
}
