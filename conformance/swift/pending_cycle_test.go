package swift

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os/exec"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestSwiftLogicalApplicationRowsOmitsTombstones(t *testing.T) {
	rows := []map[string]json.RawMessage{
		{"id": json.RawMessage(`"live"`), "removed_on": json.RawMessage(`null`)},
		{"id": json.RawMessage(`"deleted"`), "removed_on": json.RawMessage(`"2026-09-11T00:00:00Z"`)},
		{"id": json.RawMessage(`"untracked"`), "deleted_at": json.RawMessage(`"application-value"`)},
	}
	lifecycles := []swiftApplicationRowLifecycle{{PrimaryKeyField: "id", RecordID: "live", DeletedAtField: "removed_on"}, {PrimaryKeyField: "id", RecordID: "deleted", DeletedAtField: "removed_on"}}
	count, logicalRows, err := swiftLogicalApplicationRows(3, rows, lifecycles)
	if err != nil {
		t.Fatalf("normalize application rows: %v", err)
	}
	if count != 2 || len(logicalRows) != 2 {
		t.Fatalf("logical rows = count %d rows %d, want count 2 rows 2", count, len(logicalRows))
	}
	for _, row := range logicalRows {
		if string(row["id"]) == `"deleted"` {
			t.Fatal("tombstone remained in logical application rows")
		}
	}
}

func TestSwiftLogicalApplicationRowsRejectsMalformedDeletedAt(t *testing.T) {
	_, _, err := swiftLogicalApplicationRows(1, []map[string]json.RawMessage{{"id": json.RawMessage(`"row-a"`), "removed_on": json.RawMessage(`{`)}}, []swiftApplicationRowLifecycle{{PrimaryKeyField: "id", RecordID: "row-a", DeletedAtField: "removed_on"}})
	if err == nil {
		t.Fatal("accepted malformed deleted_at JSON")
	}
}

func TestSwiftPendingCycleWaitsForRecoveredPullApply(t *testing.T) {
	push := json.RawMessage(`{"sequence":1,"operation_class":"push","status_code":200,"retryable":false,"duration_nanoseconds":1,"request_facts":{"client_generation":1,"schema_version":1,"schema_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","mutation_count":1}}`)
	retry := json.RawMessage(`{"sequence":2,"operation_class":"pull","status_code":503,"error_code":"capture_pending","retryable":true,"duration_nanoseconds":1,"cursor_fingerprints":["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],"cursor_fingerprints_complete":true,"request_facts":{"client_generation":1,"schema_version":1,"schema_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","scope_set_version":1,"scope_count":1,"limit":1}}`)
	pull := json.RawMessage(`{"sequence":3,"operation_class":"pull","status_code":200,"retryable":false,"duration_nanoseconds":1,"cursor_fingerprints":["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],"cursor_fingerprints_complete":true,"request_facts":{"client_generation":1,"schema_version":1,"schema_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","scope_set_version":1,"scope_count":1,"limit":1},"pull_response_facts":{"change_count":0,"has_more":false,"rebuild_scope_count":0,"checksum_count":1,"scope_cursor_fingerprints":["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"],"scope_cursor_fingerprints_complete":true}}`)
	for _, test := range []struct {
		name      string
		status    string
		pending   int
		wantError bool
	}{
		{name: "recovered pull applied", status: "ready"},
		{name: "pending mutation remains", status: "ready", pending: 1, wantError: true},
		{name: "failed recovery", status: "error", wantError: true},
		{name: "stopped recovery", status: "stopped", wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				reply := func(status string, pending int, observations ...json.RawMessage) string {
					result := map[string]any{
						"status": status, "pending_change_count": pending,
						"process_id": "1234", "database_identity_fingerprint": strings.Repeat("a", 64),
						"transport_observations": map[string]any{"observations": observations, "overflowed": false, "sequence_checkpoint": len(observations)},
					}
					for _, field := range []string{"application_row_count", "mutation_ledger_count", "mutation_outcome_count", "sealed_batch_count", "rejected_mutation_count", "scope_state_count", "scope_row_count", "provenance_count", "row_metadata_count", "rebuild_attempt_count", "rebuild_receipt_count", "provenance_maintenance_work_cursor"} {
						result[field] = 0
					}
					for _, field := range []string{"application_rows", "retained_mutations", "rejected_mutations", "scope_states", "scope_rows", "row_metadata_records", "rebuild_attempts", "rebuild_receipts", "events"} {
						result[field] = []any{}
					}
					for _, field := range []string{"scope_states_truncated", "scope_rows_truncated", "rebuild_attempts_truncated", "rebuild_receipts_truncated", "row_metadata_truncated", "capture_overflowed"} {
						result[field] = false
					}
					encoded, err := json.Marshal(map[string]any{"schema_version": 1, "outcome": "passed", "result": result, "error_code": nil})
					if err != nil {
						t.Fatal(err)
					}
					return string(encoded)
				}
				// The push response precedes pull backoff and final local apply.
				// Stopping at the first ready state leaves that pull incomplete.
				responses := []string{
					reply("ready", 0, push),
					reply("backoff", 0, push, retry),
					reply("pulling", 0, push, retry, pull),
					reply(test.status, test.pending, push, retry, pull),
				}
				var commands bytes.Buffer
				process := &runnerProcess{
					command: &exec.Cmd{},
					stdin: struct {
						io.Writer
						io.Closer
					}{&commands, io.NopCloser(strings.NewReader(""))},
					scanner: bufio.NewScanner(strings.NewReader(strings.Join(responses, "\n") + "\n")),
					stderr:  &boundedWriter{maximum: maximumRunnerStderr},
				}
				var acceptedPush transportObservation
				if err := json.Unmarshal(push, &acceptedPush); err != nil {
					t.Fatal(err)
				}
				process.transportCheckpoint = 1
				process.transportObservations = []transportObservation{acceptedPush}
				client := Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "pending-cycle"}
				platform := &Platform{clients: map[string]*platformClient{client.Key: {client: client, session: &Session{process: process}}}}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				snapshot, err := awaitSwiftPendingCycleReady(ctx, platform, client, 1)
				if (err != nil) != test.wantError {
					t.Fatalf("cycle completion error = %v, want error %t", err, test.wantError)
				}
				if process.transportCheckpointValue() != 3 || !test.wantError && (snapshot.Status == nil || *snapshot.Status != "ready") {
					t.Fatal("cycle completed before the recovered pull applied")
				}
				decoder := json.NewDecoder(&commands)
				for decoder.More() {
					var command Request
					if err := decoder.Decode(&command); err != nil {
						t.Fatal(err)
					}
					if command.Operation != "capture" {
						t.Fatalf("recovery issued %q instead of waiting for the native scheduler", command.Operation)
					}
				}
			})
		})
	}
}
