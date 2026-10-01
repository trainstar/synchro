package integration

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
)

func TestRealPushAcceptsSourceFilledInsertAndTriggerWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	if err := harness.Operator().RegisterSourceFilledItems(ctx); err != nil {
		t.Fatalf("register source-filled fixture table: %v", err)
	}
	reference := waitForRealSourceFilledTable(t, ctx, harness)
	if _, present := reference.Fields["slug"]; present {
		t.Fatal("source-filled schema exposed the excluded slug field")
	}
	sequenceField := requireRealSchemaField(t, reference, "seq")
	triggerField := requireRealSchemaField(t, reference, "trigger_value")
	admin := openIssue49Admin(t, ctx, harness)
	waitForIssue49CanonicalHealth(t, ctx, admin, true)

	client := connectRealProtocolClient(t, ctx, harness, token, "source-filled-client")
	rebuildRealScope(t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-8f01-00000000b001")
	rebuildRealScope(t, ctx, harness, token, client, "cf:global", "00000000-0000-4000-8f01-00000000b002")
	table := requireRealTable(t, client, "cf_source_filled_items")
	ownerField := loadRealProtocolFieldID(t, ctx, harness, "cf_source_filled_items", "owner_id")
	recordID := "00000000-0000-4000-8f01-000000000001"
	insertMutationID := "00000000-0000-4000-8f01-000000000002"
	insertStatus, insertResponse := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
		client,
		"00000000-0000-4000-8f01-000000000003",
		[]map[string]any{{
			"mutation_id":     insertMutationID,
			"table":           table.ID,
			"pk":              map[string]any{table.PrimaryKeyField: recordID},
			"authored_schema": client.Schema,
			"op":              "insert",
			"client_version":  phase4ClientVersion,
			"columns":         map[string]any{table.ValueField: "source-filled"},
		}},
	))
	if insertStatus != http.StatusOK {
		t.Fatalf("source-filled insert status = %d, want 200: %#v", insertStatus, insertResponse)
	}
	insertAccepted := requireOutcomeList(t, insertResponse, "accepted")
	if len(insertAccepted) != 1 || len(requireOutcomeList(t, insertResponse, "rejected")) != 0 ||
		insertAccepted[0]["status"] != "applied" {
		t.Fatalf("source-filled insert outcome is invalid: %#v", insertResponse)
	}
	insertOutcome := insertAccepted[0]
	insertRow, ok := insertOutcome["server_row"].(map[string]any)
	if !ok || insertRow[table.ValueField] != "source-filled" ||
		insertRow[triggerField] != "source-filled-triggered" {
		t.Fatalf("source-filled insert row does not contain generated source values: %#v", insertOutcome)
	}
	var storedSequence string
	if err := admin.QueryRowContext(
		ctx,
		"SELECT seq::text FROM public.cf_source_filled_items WHERE id = $1::uuid",
		recordID,
	).Scan(&storedSequence); err != nil || storedSequence == "" ||
		fmt.Sprint(insertRow[sequenceField]) != storedSequence {
		t.Fatalf(
			"source-filled insert identity differs from the source: row=%#v sequence=%q error=%v",
			insertRow,
			storedSequence,
			err,
		)
	}
	insertVersion, ok := insertOutcome["server_version"].(string)
	if !ok || !uuidPattern.MatchString(insertVersion) {
		t.Fatalf("source-filled insert version is invalid: %#v", insertOutcome)
	}

	updateMutationID := "00000000-0000-4000-8f01-000000000004"
	updateStatus, updateResponse := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
		client,
		"00000000-0000-4000-8f01-000000000005",
		[]map[string]any{{
			"mutation_id":     updateMutationID,
			"table":           table.ID,
			"pk":              map[string]any{table.PrimaryKeyField: recordID},
			"authored_schema": client.Schema,
			"op":              "update",
			"base_version":    insertVersion,
			"client_version":  phase4ClientVersion,
			"columns":         map[string]any{table.ValueField: "source-updated"},
		}},
	))
	if updateStatus != http.StatusOK {
		t.Fatalf("source-filled update status = %d, want 200: %#v", updateStatus, updateResponse)
	}
	updateAccepted := requireOutcomeList(t, updateResponse, "accepted")
	if len(updateAccepted) != 1 || len(requireOutcomeList(t, updateResponse, "rejected")) != 0 ||
		updateAccepted[0]["status"] != "applied" {
		t.Fatalf("source-filled update outcome is invalid: %#v", updateResponse)
	}
	updateOutcome := updateAccepted[0]
	updateRow, ok := updateOutcome["server_row"].(map[string]any)
	if !ok || updateRow[table.ValueField] != "source-updated" ||
		updateRow[triggerField] != "source-updated-triggered" {
		t.Fatalf("source-filled update row does not contain trigger values: %#v", updateOutcome)
	}
	updateVersion, ok := updateOutcome["server_version"].(string)
	updateChecksum, checksumOK := updateOutcome["row_checksum"].(map[string]any)
	if !ok || !uuidPattern.MatchString(updateVersion) || !checksumOK {
		t.Fatalf("source-filled update response lacks the final row identity: %#v", updateOutcome)
	}
	waitForRealSourceFilledPull(t, ctx, harness, token, client, table, recordID, updateVersion, updateChecksum)

	missingID := "00000000-0000-4000-8f01-000000000006"
	missingStatus, missingResponse := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
		client,
		"00000000-0000-4000-8f01-000000000007",
		[]map[string]any{{
			"mutation_id":     "00000000-0000-4000-8f01-000000000008",
			"table":           table.ID,
			"pk":              map[string]any{table.PrimaryKeyField: missingID},
			"authored_schema": client.Schema,
			"op":              "insert",
			"client_version":  phase4ClientVersion,
			"columns":         map[string]any{ownerField: "diagnostic-user"},
		}},
	))
	if missingStatus != http.StatusOK {
		t.Fatalf("source-filled missing-value status = %d, want 200: %#v", missingStatus, missingResponse)
	}
	missingRejected := requireOutcomeList(t, missingResponse, "rejected")
	if len(missingRejected) != 1 || len(requireOutcomeList(t, missingResponse, "accepted")) != 0 ||
		missingRejected[0]["status"] != "rejected_terminal" ||
		missingRejected[0]["code"] != "validation_failed" {
		t.Fatalf("source-filled missing-value outcome is invalid: %#v", missingResponse)
	}
	var missingRows int
	if err := admin.QueryRowContext(
		ctx,
		"SELECT count(*) FROM public.cf_source_filled_items WHERE id = $1::uuid",
		missingID,
	).Scan(&missingRows); err != nil || missingRows != 0 {
		t.Fatalf("source-filled missing-value write persisted a source row: rows=%d error=%v", missingRows, err)
	}

	t.Run("trigger writes peer row", func(t *testing.T) {
		sourceID := "00000000-0000-4000-8f01-000000000011"
		// The zz_cf_source_filled_items_peer trigger in schema.sql writes this row.
		peerID := "00000000-0000-4000-8f01-000000000012"
		status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
			client,
			"00000000-0000-4000-8f01-000000000013",
			[]map[string]any{{
				"mutation_id":     "00000000-0000-4000-8f01-000000000014",
				"table":           table.ID,
				"pk":              map[string]any{table.PrimaryKeyField: sourceID},
				"authored_schema": client.Schema,
				"op":              "insert",
				"client_version":  phase4ClientVersion,
				"columns":         map[string]any{table.ValueField: "peer-trigger-source"},
			}},
		))
		if status != http.StatusOK {
			t.Fatalf("peer trigger insert status = %d, want 200: %#v", status, response)
		}
		accepted := requireOutcomeList(t, response, "accepted")
		if len(accepted) != 1 || len(requireOutcomeList(t, response, "rejected")) != 0 ||
			accepted[0]["status"] != "applied" {
			t.Fatalf("peer trigger insert outcome is invalid: %#v", response)
		}
		version, versionOK := accepted[0]["server_version"].(string)
		checksum, checksumOK := accepted[0]["row_checksum"].(map[string]any)
		if !versionOK || !uuidPattern.MatchString(version) || !checksumOK {
			t.Fatalf("peer trigger insert response lacks the pushed row identity: %#v", accepted[0])
		}
		fences := loadRealPeerTriggerFences(t, ctx, admin, sourceID, peerID)
		if !fences.samePush || fences.sourceVersion != version {
			t.Fatalf("peer trigger insert version is not the pushed-row fence version: version=%s fences=%+v", version, fences)
		}
		if !uuidPattern.MatchString(fences.peerVersion) || fences.peerVersion == version {
			t.Fatalf("peer row does not have its own fence version: version=%s fences=%+v", version, fences)
		}
		waitForRealSourceFilledPull(t, ctx, harness, token, client, table, sourceID, version, checksum)
		waitForRealSourceFilledPull(t, ctx, harness, token, client, table, peerID, fences.peerVersion, nil)
	})
}

type realPeerTriggerFences struct {
	sourceVersion string
	peerVersion   string
	// samePush reports that one push transaction and mutation wrote both rows.
	samePush bool
}

func loadRealPeerTriggerFences(
	t *testing.T,
	ctx context.Context,
	admin *sql.DB,
	sourceID, peerID string,
) realPeerTriggerFences {
	t.Helper()
	var fences realPeerTriggerFences
	if err := admin.QueryRowContext(ctx, `
		WITH fence AS (
			SELECT fence.new_record_id, fence.row_version, fence.dml_ordinal,
			       fence.transaction_xid, fence.mutation_id
			FROM synchro.sync_write_fences fence
			WHERE fence.physical_schema = 'public'
			  AND fence.physical_relation = 'cf_source_filled_items'
			  AND fence.new_record_id IN ($1, $2)
		)
		SELECT COALESCE((
		           SELECT row_version::text FROM fence
		           WHERE new_record_id = $1 ORDER BY dml_ordinal DESC LIMIT 1
		       ), ''),
		       COALESCE((
		           SELECT row_version::text FROM fence
		           WHERE new_record_id = $2 ORDER BY dml_ordinal DESC LIMIT 1
		       ), ''),
		       (
		           SELECT count(DISTINCT transaction_xid::text) = 1
		                  AND count(DISTINCT mutation_id) = 1
		                  AND count(mutation_id) = count(*)
		                  AND count(DISTINCT new_record_id) = 2
		           FROM fence
		       )`,
		sourceID,
		peerID,
	).Scan(&fences.sourceVersion, &fences.peerVersion, &fences.samePush); err != nil {
		t.Fatalf("load peer trigger fences: %v", err)
	}
	return fences
}

func waitForRealSourceFilledTable(
	t *testing.T,
	ctx context.Context,
	harness *blackbox.Harness,
) realSchemaTableReference {
	t.Helper()
	var lastErr error
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		table, err := fetchRealSchemaTableReference(ctx, harness.AdapterURL(), "cf_source_filled_items")
		if err == nil {
			return table
		}
		lastErr = err
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("source-filled table did not activate: %v", lastErr)
	return realSchemaTableReference{}
}

// waitForRealSourceFilledPull waits until pull returns the record at version.
// A nil checksum matches the version only.
func waitForRealSourceFilledPull(
	t *testing.T,
	ctx context.Context,
	harness *blackbox.Harness,
	token string,
	client *realProtocolClient,
	table realProtocolTable,
	recordID, version string,
	checksum map[string]any,
) {
	t.Helper()
	var lastStatus int
	var lastMatch map[string]any
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/pull", map[string]any{
			"client_id":         client.ID,
			"client_generation": client.Generation,
			"schema":            client.Schema,
			"scope_set_version": client.ScopeSetVersion,
			"scopes":            client.Scopes,
			"limit":             100,
		})
		lastStatus = status
		if status == http.StatusServiceUnavailable {
			errorBody, _ := response["error"].(map[string]any)
			if errorBody["code"] == "capture_pending" && errorBody["retryable"] == true {
				time.Sleep(50 * time.Millisecond)
				continue
			}
		}
		if status != http.StatusOK {
			t.Fatalf("source-filled pull status = %d, want 200: %#v", status, response)
		}
		changes := requireRealChanges(t, response)
		for _, change := range changes {
			if change["table"] != table.ID {
				continue
			}
			pk, ok := change["pk"].(map[string]any)
			if !ok || pk[table.PrimaryKeyField] != recordID {
				continue
			}
			lastMatch = change
			if change["server_version"] == version &&
				(checksum == nil || reflect.DeepEqual(change["row_checksum"], checksum)) {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	var pendingFences int
	var poison string
	admin := openIssue49Admin(t, ctx, harness)
	if err := admin.QueryRowContext(ctx, `
		SELECT (SELECT count(*) FROM synchro.sync_write_fences
		        WHERE new_record_id = $1 AND coverage = 'pending'),
		       COALESCE((SELECT failure_class || ': ' || failure_detail FROM synchro.sync_wal_poison LIMIT 1), '')`,
		recordID,
	).Scan(&pendingFences, &poison); err != nil {
		t.Fatalf("read source-filled capture state: %v", err)
	}
	t.Fatalf(
		"source-filled pull did not return the final push response identity: status=%d want_version=%s want_checksum=%#v change=%#v pending_fences=%d poison=%q",
		lastStatus, version, checksum, lastMatch, pendingFences, poison,
	)
}
