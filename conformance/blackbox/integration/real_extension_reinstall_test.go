package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/trainstar/synchro/conformance/blackbox"
)

func TestRealExtensionReinstallRebindsWorkerSlot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	controller, err := blackbox.NewNativeController(blackbox.NativeControllerConfig{Harness: harness})
	if err != nil {
		t.Fatalf("create native controller: %v", err)
	}

	resumeWAL, err := controller.PauseWALMaterialization(ctx)
	if err != nil {
		t.Fatalf("pause WAL materialization: %v", err)
	}
	walPaused := true
	defer func() {
		if walPaused {
			cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			if err := resumeWAL(cleanupContext); err != nil {
				t.Errorf("resume WAL materialization: %v", err)
			}
		}
	}()
	pausedID := "00000000-0000-4000-8c03-000000000000"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		pausedID,
		"diagnostic-user",
		"paused-WAL-materialization",
	); err != nil {
		t.Fatalf("insert source row while WAL materialization is paused: %v", err)
	}
	paused, err := harness.Operator().ObserveWALRecordsForTable(ctx, "cf_items", []string{pausedID})
	if err != nil {
		t.Fatalf("observe paused WAL materialization: %v", err)
	}
	if len(paused.Records) != 0 {
		t.Fatalf("WAL worker materialized a source row while paused: %#v", paused)
	}
	if err := resumeWAL(ctx); err != nil {
		t.Fatalf("resume WAL materialization: %v", err)
	}
	walPaused = false
	waitForRealWALRecords(t, ctx, harness, "cf_items", pausedID)
	if err := harness.Source().ExecContext(ctx, "DELETE FROM cf_items WHERE id = $1", pausedID); err != nil {
		t.Fatalf("delete WAL materialization gate row: %v", err)
	}
	waitForRealWALEffects(t, ctx, harness, "cf_items", 2, pausedID)

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
	// Client performance suites restore the authored source shapes at this point.
	if err := harness.Operator().RestoreDiagnosticSourceTableShapes(ctx); err != nil {
		t.Fatalf("restore diagnostic source table shapes: %v", err)
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
	if err := harness.Source().ExecContext(ctx, "DELETE FROM cf_items WHERE id = $1", afterID); err != nil {
		t.Fatalf("delete post-reinstall source row: %v", err)
	}
	waitForRealWALEffects(t, ctx, harness, "cf_items", 2, afterID)

	for index, cycle := range []struct {
		name          string
		disableReplay bool
	}{
		{"lost activation control", true},
		{"recovery", false},
		{"repeated reinstall", false},
	} {
		if !t.Run(cycle.name, func(t *testing.T) {
			admin := openIssue49Admin(t, ctx, harness)
			priorPID, err := harness.Operator().CurrentWALWorkerPID(ctx)
			if err != nil {
				t.Fatalf("observe worker before cold reinstall: %v", err)
			}
			var priorStream string
			if err := admin.QueryRowContext(ctx, "SELECT stream_generation FROM synchro.sync_runtime_state WHERE singleton").Scan(&priorStream); err != nil {
				t.Fatalf("observe stream before cold reinstall: %v", err)
			}
			if _, err := admin.ExecContext(ctx, "ALTER SYSTEM SET synchro.auto_start = 'off'"); err != nil {
				t.Fatalf("disable worker for cold reinstall: %v", err)
			}
			if err := admin.Close(); err != nil {
				t.Fatalf("close administrator connection before worker detachment: %v", err)
			}
			if err := harness.RestartPostgres(ctx); err != nil {
				t.Fatalf("restart isolated PostgreSQL without worker: %v", err)
			}
			admin = openIssue49Admin(t, ctx, harness)
			if _, err := admin.ExecContext(ctx, "SELECT pg_catalog.pg_drop_replication_slot($1)", harness.Names().ReplicationSlot); err != nil {
				t.Fatalf("drop inactive prior slot: %v", err)
			}
			tx, err := admin.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("begin cold reinstall: %v", err)
			}
			defer tx.Rollback()
			publication := pgx.Identifier{harness.Names().Publication}.Sanitize()
			if _, err := tx.ExecContext(ctx,
				"DROP PUBLICATION "+publication+"; DROP EXTENSION synchro_pg CASCADE; CREATE EXTENSION synchro_pg; CREATE PUBLICATION "+publication,
			); err != nil {
				t.Fatalf("replace extension and publication atomically: %v", err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("commit cold reinstall: %v", err)
			}
			if cycle.disableReplay {
				// Recreate the lost pre-slot activation without changing registration or worker behavior.
				if _, err := admin.ExecContext(ctx, "ALTER TABLE synchro.sync_runtime_state DISABLE TRIGGER synchro_replay_registry_activation_requests"); err != nil {
					t.Fatalf("disable initial activation replay control: %v", err)
				}
			}
			if err := harness.RestoreDiagnosticRegistrations(ctx); err != nil {
				t.Fatalf("register relations before slot creation: %v", err)
			}
			var freshStream, registrationLSN string
			var unbound bool
			var pending int
			var targetGeneration int64
			if err := admin.QueryRowContext(ctx, `
				SELECT runtime.stream_generation,
				       runtime.active_slot_name IS NULL AND progress.generation_start_lsn IS NULL,
				       (SELECT count(*) FROM synchro.sync_registry_generations WHERE state = 'pending'),
				       (SELECT max(generation) FROM synchro.sync_registry_generations WHERE state = 'pending'),
				       pg_catalog.pg_current_wal_lsn()::text
				FROM synchro.sync_runtime_state runtime
				JOIN synchro.sync_wal_progress progress ON progress.singleton
				WHERE runtime.singleton`).Scan(&freshStream, &unbound, &pending, &targetGeneration, &registrationLSN); err != nil {
				t.Fatalf("observe committed pre-slot registrations: %v", err)
			}
			if freshStream == priorStream || !unbound || pending == 0 {
				t.Fatalf("cold reinstall did not commit unbound registrations: new_stream=%t unbound=%t pending=%d",
					freshStream != priorStream, unbound, pending)
			}
			if _, err := admin.ExecContext(ctx, "ALTER SYSTEM SET synchro.auto_start = 'on'"); err != nil {
				t.Fatalf("enable reinstalled worker: %v", err)
			}
			if err := admin.Close(); err != nil {
				t.Fatalf("close administrator connection before worker startup: %v", err)
			}
			if err := harness.RestartPostgres(ctx); err != nil {
				t.Fatalf("restart isolated PostgreSQL with worker: %v", err)
			}
			admin = openIssue49Admin(t, ctx, harness)
			cold := blackbox.ExtensionReinstallResult{PriorWorkerPID: priorPID, ReinstallLSN: registrationLSN}
			if cycle.disableReplay {
				bound := false
				deadline := time.Now().Add(30 * time.Second)
				for time.Now().Before(deadline) {
					if err := admin.QueryRowContext(ctx, `
						SELECT EXISTS (
							SELECT 1 FROM synchro.sync_wal_progress
							WHERE singleton AND generation_start_lsn > $1::pg_lsn
						)`, registrationLSN).Scan(&bound); err != nil {
						t.Fatalf("observe replacement slot boundary: %v", err)
					}
					if bound {
						break
					}
					time.Sleep(50 * time.Millisecond)
				}
				if !bound {
					t.Fatal("replacement slot did not start after the committed registrations")
				}
				var probeLSN string
				if err := admin.QueryRowContext(ctx,
					"SELECT pg_catalog.pg_logical_emit_message(true, 'conformance_reinstall_probe', '')::text",
				).Scan(&probeLSN); err != nil {
					t.Fatalf("emit post-binding progress probe: %v", err)
				}
				acknowledged := false
				deadline = time.Now().Add(30 * time.Second)
				for time.Now().Before(deadline) {
					if err := admin.QueryRowContext(ctx, `
						SELECT EXISTS (
							SELECT 1 FROM synchro.sync_wal_progress progress
							JOIN synchro.sync_wal_worker_state worker ON worker.worker_id = 'synchro_wal_consumer'
							WHERE progress.singleton AND progress.acknowledged_end_lsn >= $1::pg_lsn
							  AND worker.state = 'running'
						)`, probeLSN).Scan(&acknowledged); err != nil {
						t.Fatalf("observe post-binding progress probe: %v", err)
					}
					if acknowledged {
						break
					}
					time.Sleep(50 * time.Millisecond)
				}
				if !acknowledged {
					t.Fatal("worker did not acknowledge the post-binding progress probe")
				}
				stalled, err := harness.Operator().ObserveExtensionReinstall(ctx, registrationLSN)
				if err != nil || stalled.ActiveRegistryGeneration != 1 || stalled.WorkerRegistryGeneration != 1 ||
					stalled.PendingRegistryGenerationCount != int64(pending) || !stalled.NoValidationFailurePoison {
					t.Fatalf("lost activation control did not reproduce the pending registry: observation=%#v err=%v", stalled, err)
				}
				var publicationState, publicationReason string
				if err := admin.QueryRowContext(ctx, `
					SELECT publication->>'state', publication->>'reason'
					FROM (SELECT synchro.synchro_health_detail()->'checks'->'publication' AS publication) health`,
				).Scan(&publicationState, &publicationReason); err != nil {
					t.Fatalf("observe lost activation readiness: %v", err)
				}
				if publicationState != "failed" || publicationReason != "publication_mismatch" {
					t.Fatalf("lost activation control reported publication state=%s reason=%s", publicationState, publicationReason)
				}
				waitForIssue49PublicReady(t, ctx, harness.AdapterURL(), false)
				t.Logf("lost activation reproduced: active=%d worker=%d pending=%d publication=%s later_WAL_acknowledged=%t",
					stalled.ActiveRegistryGeneration, stalled.WorkerRegistryGeneration,
					stalled.PendingRegistryGenerationCount, publicationReason, acknowledged)
				return
			}

			recovered := waitForReinstalledWorker(t, ctx, harness, cold, 1)
			if recovered.ActiveRegistryGeneration != targetGeneration {
				t.Fatalf("recovery did not activate the last committed generation: active=%d target=%d",
					recovered.ActiveRegistryGeneration, targetGeneration)
			}
			waitForIssue49CanonicalHealth(t, ctx, admin, true)
			waitForIssue49PublicReady(t, ctx, harness.AdapterURL(), true)
			client := connectRealProtocolClient(t, ctx, harness, token, fmt.Sprintf("cold-reinstall-%d", index))
			rebuildRealScope(t, ctx, harness, token, client, "user:diagnostic-user", fmt.Sprintf("00000000-0000-4000-8c03-%012d", 100+index))
			rebuildRealScope(t, ctx, harness, token, client, "cf:global", fmt.Sprintf("00000000-0000-4000-8c03-%012d", 200+index))
			witnessID := fmt.Sprintf("00000000-0000-4000-8c03-%012d", 300+index)
			if err := harness.Source().ExecContext(ctx,
				"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'recovered')", witnessID,
			); err != nil {
				t.Fatalf("insert recovery witness: %v", err)
			}
			pullUntilRealRecords(t, ctx, harness, token, client, []realRecordExpectation{{
				scopeID: "user:diagnostic-user", table: requireRealTable(t, client, "cf_items"),
				recordID: witnessID, value: "recovered",
			}})
			acknowledgeRealClientCursors(t, ctx, harness, token, client)
			if err := harness.Source().ExecContext(ctx, "DELETE FROM cf_items WHERE id = $1", witnessID); err != nil {
				t.Fatalf("delete recovery witness: %v", err)
			}
			waitForRealWALEffects(t, ctx, harness, "cf_items", 2, witnessID)
			t.Logf("recovered registry=%d pending=%d with a source-to-client write",
				recovered.ActiveRegistryGeneration, recovered.PendingRegistryGenerationCount)
		}) {
			return
		}
	}

	// Issue 256: a registration that commits after the replacement slot
	// boundary and before the slot binding must activate exactly once.
	t.Run("registration before slot binding", func(t *testing.T) {
		admin := openIssue49Admin(t, ctx, harness)
		priorPID, err := harness.Operator().CurrentWALWorkerPID(ctx)
		if err != nil {
			t.Fatalf("observe worker before reinstall: %v", err)
		}
		// The prior worker waits at its poll gate, so the binding lock below precedes every replacement worker.
		releaseWorker, err := controller.PauseWALMaterialization(ctx)
		if err != nil {
			t.Fatalf("pause WAL worker before reinstall: %v", err)
		}
		workerReleased := false
		defer func() {
			if !workerReleased {
				cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cleanupCancel()
				if err := releaseWorker(cleanupContext); err != nil {
					t.Errorf("release WAL worker: %v", err)
				}
			}
		}()
		tx, err := admin.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin extension reinstall: %v", err)
		}
		defer tx.Rollback()
		if _, err := tx.ExecContext(ctx, "DROP EXTENSION synchro_pg CASCADE; CREATE EXTENSION synchro_pg"); err != nil {
			t.Fatalf("replace extension: %v", err)
		}
		var publicationExists bool
		if err := tx.QueryRowContext(ctx,
			"SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = $1)", harness.Names().Publication,
		).Scan(&publicationExists); err != nil || !publicationExists {
			t.Fatalf("observe publication after extension replacement: exists=%t err=%v", publicationExists, err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit extension reinstall: %v", err)
		}
		reinstall := blackbox.ExtensionReinstallResult{PriorWorkerPID: priorPID}
		if err := admin.QueryRowContext(ctx, "SELECT pg_catalog.pg_current_wal_lsn()::text").Scan(&reinstall.ReinstallLSN); err != nil {
			t.Fatalf("read reinstall WAL position: %v", err)
		}

		// A SHARE table lock has no transaction ID, so slot creation does not wait for it.
		// It blocks only the binding UPDATE of the runtime state.
		bindingLock, err := admin.Conn(ctx)
		if err != nil {
			t.Fatalf("open slot binding lock session: %v", err)
		}
		defer bindingLock.Close()
		bindingTx, err := bindingLock.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin slot binding lock: %v", err)
		}
		defer bindingTx.Rollback()
		var lockPID int
		var unbound bool
		if err := bindingTx.QueryRowContext(ctx, `
			SELECT pg_catalog.pg_backend_pid(), active_slot_name IS NULL
			FROM synchro.sync_runtime_state WHERE singleton`).Scan(&lockPID, &unbound); err != nil || !unbound {
			t.Fatalf("observe unbound runtime after reinstall: unbound=%t err=%v", unbound, err)
		}
		if _, err := bindingTx.ExecContext(ctx, "LOCK TABLE synchro.sync_runtime_state IN SHARE MODE"); err != nil {
			t.Fatalf("lock slot binding: %v", err)
		}
		if err := releaseWorker(ctx); err != nil {
			t.Fatalf("release prior WAL worker: %v", err)
		}
		workerReleased = true
		// An unbound worker never drops an existing configured slot, so the
		// documented reinstall drops the slot that the prior worker releases.
		if err := harness.DropReleasedWorkerSlot(ctx, harness.Names().ReplicationSlot); err != nil {
			t.Fatalf("drop released prior slot: %v", err)
		}

		var slotBoundary string
		deadline := time.Now().Add(90 * time.Second)
		for slotBoundary == "" && time.Now().Before(deadline) {
			err := admin.QueryRowContext(ctx, `
				SELECT slot.confirmed_flush_lsn::text
				FROM pg_catalog.pg_replication_slots slot
				WHERE slot.slot_name = $1
				  AND slot.confirmed_flush_lsn >= $2::pg_lsn
				  AND EXISTS (
				      SELECT 1 FROM pg_catalog.pg_stat_activity worker
				      WHERE worker.datname = pg_catalog.current_database()
				        AND worker.backend_type = 'synchro WAL consumer'
				        AND worker.pid <> $3
				        AND $4 = ANY(pg_catalog.pg_blocking_pids(worker.pid)))`,
				harness.Names().ReplicationSlot, reinstall.ReinstallLSN, priorPID, lockPID,
			).Scan(&slotBoundary)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("observe replacement slot before binding: %v", err)
			}
			if slotBoundary == "" {
				time.Sleep(50 * time.Millisecond)
			}
		}
		if slotBoundary == "" {
			t.Fatalf("replacement worker did not create its slot and wait at the binding: %s", harness.FailureDiagnostics())
		}

		if err := harness.RestoreDiagnosticRegistrations(ctx); err != nil {
			t.Fatalf("register relations between slot creation and binding: %v", err)
		}
		var registeredUnbound, afterBoundary bool
		var pending int
		var targetGeneration int64
		if err := admin.QueryRowContext(ctx, `
			SELECT runtime.active_slot_name IS NULL,
			       pg_catalog.pg_current_wal_lsn() > $1::pg_lsn,
			       (SELECT count(*) FROM synchro.sync_registry_generations WHERE state = 'pending' AND validated),
			       (SELECT COALESCE(max(generation), 0) FROM synchro.sync_registry_generations WHERE state = 'pending')
			FROM synchro.sync_runtime_state runtime
			WHERE runtime.singleton`, slotBoundary).Scan(&registeredUnbound, &afterBoundary, &pending, &targetGeneration); err != nil {
			t.Fatalf("observe registrations before slot binding: %v", err)
		}
		if !registeredUnbound || !afterBoundary || pending < 2 {
			t.Fatalf("registrations did not commit after the slot boundary and before binding: unbound=%t after_boundary=%t pending=%d",
				registeredUnbound, afterBoundary, pending)
		}
		if err := bindingTx.Rollback(); err != nil {
			t.Fatalf("release slot binding lock: %v", err)
		}

		activated := waitForReinstalledWorker(t, ctx, harness, reinstall, targetGeneration-1)
		if activated.ActiveRegistryGeneration != targetGeneration {
			t.Fatalf("slot binding did not activate the last registered generation: active=%d target=%d",
				activated.ActiveRegistryGeneration, targetGeneration)
		}
		witnessID := "00000000-0000-4000-8c03-000000000400"
		if err := harness.Source().ExecContext(ctx,
			"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'activated-once')", witnessID,
		); err != nil {
			t.Fatalf("insert post-activation witness: %v", err)
		}
		waitForRealWALRecords(t, ctx, harness, "cf_items", witnessID)
		t.Logf("registrations after slot boundary %s activated once: active=%d pending_before_binding=%d",
			slotBoundary, activated.ActiveRegistryGeneration, pending)

		// This control proves that the no-poison observation can fail and that validation stays strict.
		var forged int64
		if err := admin.QueryRowContext(ctx, `
			INSERT INTO synchro.sync_registry_generations (stream_generation, state, validated, parent_generation)
			SELECT runtime.stream_generation, 'pending', false, progress.registry_generation
			FROM synchro.sync_runtime_state runtime
			JOIN synchro.sync_wal_progress progress ON progress.singleton
			WHERE runtime.singleton
			RETURNING generation`).Scan(&forged); err != nil {
			t.Fatalf("insert never validated generation: %v", err)
		}
		var messageLSN string
		if err := admin.QueryRowContext(ctx,
			"SELECT pg_catalog.pg_logical_emit_message(true, 'synchro_registry', pg_catalog.convert_to($1, 'UTF8'))::text",
			fmt.Sprintf(`{"generation":%d,"action":"activate"}`, forged),
		).Scan(&messageLSN); err != nil {
			t.Fatalf("emit never validated activation: %v", err)
		}
		var class, detail string
		deadline = time.Now().Add(30 * time.Second)
		for class == "" && time.Now().Before(deadline) {
			err := admin.QueryRowContext(ctx, `
				SELECT failure_class, COALESCE(failure_detail, '')
				FROM synchro.sync_wal_poison
				WHERE lifecycle = 'active' AND commit_lsn >= $1::pg_lsn`, messageLSN).Scan(&class, &detail)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("observe never validated activation poison: %v", err)
			}
			if class == "" {
				time.Sleep(50 * time.Millisecond)
			}
		}
		if class != "validation_failed" || detail != "registry activation is not a validated pending generation" {
			t.Fatalf("never validated activation did not poison: class=%q detail=%q; %s", class, detail, harness.FailureDiagnostics())
		}
	})
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
		if err == nil && !observation.NoValidationFailurePoison {
			// A validation_failed poison blocks the stream until an operator acts, so waiting cannot succeed.
			var commitLSN, detail string
			poisonErr := openIssue49Admin(t, ctx, harness).QueryRowContext(ctx, `
				SELECT commit_lsn::text, COALESCE(failure_detail, '')
				FROM synchro.sync_wal_poison WHERE lifecycle = 'active'
				ORDER BY id DESC LIMIT 1`).Scan(&commitLSN, &detail)
			t.Fatalf("reinstalled worker poisoned the stream at %s: %q: %#v, %v; %s",
				commitLSN, detail, observation, poisonErr, harness.FailureDiagnostics())
		}
		if err == nil && observation.WorkerPID > 0 && observation.WorkerPID != reinstall.PriorWorkerPID &&
			observation.ActiveSlotName == harness.Names().ReplicationSlot && observation.RestartLSN != "" &&
			observation.SlotActive && observation.RestartLSNAtOrAfterReinstall &&
			observation.ActiveRegistryGeneration > minimumGeneration &&
			// The worker reports its own generation after activation, so a
			// sample can land between the two. Wait for the settled state
			// the caller asserts.
			observation.WorkerRegistryGeneration == observation.ActiveRegistryGeneration &&
			observation.PendingRegistryGenerationCount == 0 && observation.NoValidationFailurePoison {
			return observation
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("reinstalled worker did not bind a fresh active slot: %#v, %v; %s", observation, err, harness.FailureDiagnostics())
	return blackbox.ExtensionReinstallObservation{}
}

// TestRealWorkerStartupRetainsUnownedConfiguredSlot proves that an unbound worker
// never drops an existing slot with the configured name. Only the runtime binding
// is ownership evidence, and a fresh installation has no binding.
func TestRealWorkerStartupRetainsUnownedConfiguredSlot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	slot := harness.Names().ReplicationSlot
	admin := openIssue49Admin(t, ctx, harness)
	priorPID, err := harness.Operator().CurrentWALWorkerPID(ctx)
	if err != nil {
		t.Fatalf("observe worker before cold reinstall: %v", err)
	}
	if _, err := admin.ExecContext(ctx, "ALTER SYSTEM SET synchro.auto_start = 'off'"); err != nil {
		t.Fatalf("disable worker for cold reinstall: %v", err)
	}
	if err := admin.Close(); err != nil {
		t.Fatalf("close administrator connection before worker detachment: %v", err)
	}
	if err := harness.RestartPostgres(ctx); err != nil {
		t.Fatalf("restart isolated PostgreSQL without worker: %v", err)
	}
	admin = openIssue49Admin(t, ctx, harness)
	// The runtime binding proves that the harness worker owns this slot.
	if _, err := admin.ExecContext(ctx, `
		SELECT pg_catalog.pg_drop_replication_slot(runtime.active_slot_name)
		FROM synchro.sync_runtime_state runtime
		WHERE runtime.singleton AND runtime.active_slot_name = $1`, slot); err != nil {
		t.Fatalf("drop owned prior slot: %v", err)
	}
	tx, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin cold reinstall: %v", err)
	}
	defer tx.Rollback()
	publication := pgx.Identifier{harness.Names().Publication}.Sanitize()
	if _, err := tx.ExecContext(ctx,
		"DROP PUBLICATION "+publication+"; DROP EXTENSION synchro_pg CASCADE; CREATE EXTENSION synchro_pg; CREATE PUBLICATION "+publication,
	); err != nil {
		t.Fatalf("replace extension and publication atomically: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit cold reinstall: %v", err)
	}
	if err := harness.RestoreDiagnosticRegistrations(ctx); err != nil {
		t.Fatalf("register relations before slot creation: %v", err)
	}

	// Another consumer owns this slot. Its retained position must survive worker startup.
	type slotState struct {
		plugin, database, restartLSN, confirmedLSN string
		active, temporary                          bool
	}
	readSlot := func() (slotState, bool) {
		var state slotState
		err := admin.QueryRowContext(ctx, `
			SELECT plugin, database, restart_lsn::text, confirmed_flush_lsn::text, active, temporary
			FROM pg_catalog.pg_replication_slots WHERE slot_name = $1`, slot,
		).Scan(&state.plugin, &state.database, &state.restartLSN, &state.confirmedLSN, &state.active, &state.temporary)
		if errors.Is(err, sql.ErrNoRows) {
			return slotState{}, false
		}
		if err != nil {
			t.Fatalf("observe configured slot: %v", err)
		}
		return state, true
	}
	if _, err := admin.ExecContext(ctx, "SELECT pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput')", slot); err != nil {
		t.Fatalf("create unrelated consumer slot: %v", err)
	}
	created, _ := readSlot()
	if _, err := admin.ExecContext(ctx, "SELECT pg_catalog.pg_logical_emit_message(true, 'unrelated_consumer', 'progress')"); err != nil {
		t.Fatalf("write unrelated consumer progress: %v", err)
	}
	var advancedLSN string
	if err := admin.QueryRowContext(ctx,
		"SELECT end_lsn::text FROM pg_catalog.pg_replication_slot_advance($1, pg_catalog.pg_current_wal_lsn())", slot,
	).Scan(&advancedLSN); err != nil {
		t.Fatalf("advance unrelated consumer slot: %v", err)
	}
	foreign, present := readSlot()
	if !present || foreign.confirmedLSN != advancedLSN || foreign.confirmedLSN == created.confirmedLSN ||
		foreign.plugin != "pgoutput" || foreign.active || foreign.temporary {
		t.Fatalf("unrelated consumer slot setup is invalid: present=%t created=%#v state=%#v advanced=%s", present, created, foreign, advancedLSN)
	}

	if _, err := admin.ExecContext(ctx, "ALTER SYSTEM SET synchro.auto_start = 'on'"); err != nil {
		t.Fatalf("enable worker: %v", err)
	}
	if err := admin.Close(); err != nil {
		t.Fatalf("close administrator connection before worker startup: %v", err)
	}
	if err := harness.RestartPostgres(ctx); err != nil {
		t.Fatalf("restart isolated PostgreSQL with worker: %v", err)
	}
	admin = openIssue49Admin(t, ctx, harness)

	// A worker exhausts its 30-second preparation budget before PostgreSQL restarts it.
	// A second worker process proves that one complete startup attempt refused the slot.
	workerPIDs := map[int]bool{}
	deadline := time.Now().Add(120 * time.Second)
	for len(workerPIDs) < 2 && time.Now().Before(deadline) {
		rows, err := admin.QueryContext(ctx, `
			SELECT pid FROM pg_catalog.pg_stat_activity
			WHERE datname = current_database() AND backend_type = 'synchro WAL consumer'`)
		if err != nil {
			t.Fatalf("observe worker processes: %v", err)
		}
		for rows.Next() {
			var pid int
			if err := rows.Scan(&pid); err != nil {
				t.Fatalf("scan worker process: %v", err)
			}
			if pid != priorPID {
				workerPIDs[pid] = true
			}
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("read worker processes: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	var unbound bool
	if err := admin.QueryRowContext(ctx, `
		SELECT runtime.active_slot_name IS NULL AND progress.generation_start_lsn IS NULL
		FROM synchro.sync_runtime_state runtime
		JOIN synchro.sync_wal_progress progress ON progress.singleton
		WHERE runtime.singleton`).Scan(&unbound); err != nil {
		t.Fatalf("observe worker binding: %v", err)
	}
	retained, present := readSlot()
	t.Run("assertion", func(t *testing.T) {
		if !present || retained != foreign {
			t.Fatalf("worker startup changed the unrelated consumer slot: present=%t before=%#v after=%#v", present, foreign, retained)
		}
		if !unbound {
			t.Fatal("worker bound a slot that it does not own")
		}
		if len(workerPIDs) < 2 {
			t.Fatalf("worker did not complete one refused startup attempt: processes=%d", len(workerPIDs))
		}
	})

	// Explicit operator removal is the recovery path. The worker then creates and binds a fresh slot.
	if _, err := admin.ExecContext(ctx, "SELECT pg_catalog.pg_drop_replication_slot($1)", slot); err != nil {
		t.Fatalf("drop unrelated consumer slot as the operator: %v", err)
	}
	recovered := waitForReinstalledWorker(t, ctx, harness, blackbox.ExtensionReinstallResult{PriorWorkerPID: priorPID, ReinstallLSN: advancedLSN}, 1)
	if recovered.PendingRegistryGenerationCount != 0 {
		t.Fatalf("worker did not activate registrations after operator recovery: %#v", recovered)
	}
}

// A fixture in the same database, such as the client integration schema, can
// register tables that the diagnostic registry does not restore. Readiness
// requires publication members to equal the active registry, so the scenario
// reset must remove them. Every native scenario runner starts from this reset
// on an attached database, so the reset must also leave no connected client,
// no source row, and no captured row of an earlier scenario, and the worker
// must capture a new write.
func TestRealScenarioResetLeavesPublicationEqualToRestoredRegistry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	database, err := sql.Open("pgx", harness.DatabaseURL())
	if err != nil {
		t.Fatalf("open administrator connection: %v", err)
	}
	defer database.Close()
	publication := pgx.Identifier{harness.Names().Publication}.Sanitize()
	if _, err := database.ExecContext(ctx, "CREATE TABLE public.reset_foreign_fixture (id text PRIMARY KEY)"); err != nil {
		t.Fatalf("create foreign fixture table: %v", err)
	}
	defer func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := database.ExecContext(cleanupContext, "DROP TABLE IF EXISTS public.reset_foreign_fixture"); err != nil {
			t.Errorf("drop foreign fixture table: %v", err)
		}
	}()
	if _, err := database.ExecContext(ctx, "ALTER PUBLICATION "+publication+" ADD TABLE public.reset_foreign_fixture"); err != nil {
		t.Fatalf("publish foreign fixture table: %v", err)
	}
	const retainedClientID = "reset-retained-client"
	retainedID := "00000000-0000-4000-8c08-000000000001"
	connectRealProtocolClient(t, ctx, harness, token, retainedClientID)
	if err := harness.Source().ExecContext(ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'before-scenario-reset')", retainedID); err != nil {
		t.Fatalf("insert source row before the reset: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", retainedID)

	if err := harness.ResetScenarioServer(ctx); err != nil {
		t.Fatalf("reset scenario server: %v", err)
	}
	var clients, sourceRows, capturedRows int
	if err := database.QueryRowContext(ctx, `
		SELECT (SELECT count(*) FROM synchro.sync_clients WHERE client_id = $1),
		       (SELECT count(*) FROM public.cf_items WHERE id::text = $2::text),
		       (SELECT count(*) FROM synchro.sync_captured_rows WHERE record_id = $2::text)`,
		retainedClientID, retainedID).Scan(&clients, &sourceRows, &capturedRows); err != nil {
		t.Fatalf("read state retained across the reset: %v", err)
	}
	if clients != 0 || sourceRows != 0 || capturedRows != 0 {
		t.Fatalf("scenario reset retained earlier state: clients=%d source rows=%d captured rows=%d", clients, sourceRows, capturedRows)
	}
	freshID := "00000000-0000-4000-8c08-000000000002"
	if err := harness.Source().ExecContext(ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'after-scenario-reset')", freshID); err != nil {
		t.Fatalf("insert source row after the reset: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", freshID)
	var unregistered, unpublished []string
	for _, check := range []struct {
		target *[]string
		query  string
	}{
		{&unregistered, `
			SELECT member.prrelid::regclass::text
			FROM pg_catalog.pg_publication_rel member
			JOIN pg_catalog.pg_publication publication ON publication.oid = member.prpubid
			WHERE publication.pubname = $1
			EXCEPT
			SELECT registry.physical_relation_oid::regclass::text
			FROM synchro.sync_registry registry
			JOIN synchro.sync_registry_generations generation ON generation.generation = registry.registry_generation
			JOIN synchro.sync_runtime_state runtime ON runtime.singleton AND runtime.stream_generation = generation.stream_generation
			WHERE generation.state = 'active' AND generation.validated`},
		{&unpublished, `
			SELECT registry.physical_relation_oid::regclass::text
			FROM synchro.sync_registry registry
			JOIN synchro.sync_registry_generations generation ON generation.generation = registry.registry_generation
			JOIN synchro.sync_runtime_state runtime ON runtime.singleton AND runtime.stream_generation = generation.stream_generation
			WHERE generation.state = 'active' AND generation.validated
			EXCEPT
			SELECT member.prrelid::regclass::text
			FROM pg_catalog.pg_publication_rel member
			JOIN pg_catalog.pg_publication publication ON publication.oid = member.prpubid
			WHERE publication.pubname = $1`},
	} {
		rows, err := database.QueryContext(ctx, check.query, harness.Names().Publication)
		if err != nil {
			t.Fatalf("compare publication with registry: %v", err)
		}
		for rows.Next() {
			var relation string
			if err := rows.Scan(&relation); err != nil {
				t.Fatalf("read publication comparison: %v", err)
			}
			*check.target = append(*check.target, relation)
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			t.Fatalf("read publication comparison: %v", err)
		}
	}
	var published int
	if err := database.QueryRowContext(ctx, `
		SELECT count(*)
		FROM pg_catalog.pg_publication_rel member
		JOIN pg_catalog.pg_publication publication ON publication.oid = member.prpubid
		WHERE publication.pubname = $1`, harness.Names().Publication).Scan(&published); err != nil || published == 0 {
		t.Fatalf("restored publication has %d members: %v", published, err)
	}
	if len(unregistered) != 0 || len(unpublished) != 0 {
		t.Fatalf("publication differs from the restored registry: published without registration %v, registered without publication %v", unregistered, unpublished)
	}
}
