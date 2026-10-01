package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/vectors"
)

const (
	pushRequestLimitOctets   = 1 << 20
	responseSizeMutations    = 64
	responseSizeRecordPrefix = "00000000-0000-4000-8194-"
)

// The push response has no octet limit. Each applied outcome echoes its full row, so a
// request at the request limit returns a response above it.
func TestRealPushResponseAboveRequestLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	client := connectRealProtocolClient(t, ctx, harness, token, "response-size-client")
	table := requireRealTable(t, client, "cf_items")
	ownerField := loadRealProtocolFieldID(t, ctx, harness, "cf_items", "owner_id")
	updatedField := loadRealProtocolFieldID(t, ctx, harness, "cf_items", "updated_at")
	deletedField := loadRealProtocolFieldID(t, ctx, harness, "cf_items", "deleted_at")
	manifest := loadMutationControlManifest(t, ctx, harness)

	batchID := "00000000-0000-4000-a194-000000000001"
	recordIDs := make([]string, responseSizeMutations)
	mutationIDs := make([]string, responseSizeMutations)
	values := make([]string, responseSizeMutations)
	for index := range responseSizeMutations {
		recordIDs[index] = fmt.Sprintf("%s%012x", responseSizeRecordPrefix, index+1)
		mutationIDs[index] = fmt.Sprintf("00000000-0000-4000-9194-%012x", index+1)
		values[index] = fmt.Sprintf("%02d:", index)
	}
	encode := func() []byte {
		mutations := make([]map[string]any, responseSizeMutations)
		for index := range responseSizeMutations {
			mutations[index] = phase4InsertMutation(client, table, ownerField, mutationIDs[index], recordIDs[index], values[index])
		}
		body, err := json.Marshal(phase4PushPayload(client, batchID, mutations))
		if err != nil {
			t.Fatalf("encode push request: %v", err)
		}
		return body
	}
	// ASCII values and sorted map keys make these bytes equal to their RFC 8785 form, so
	// one length is both the raw and the canonical request size.
	padding := pushRequestLimitOctets - len(encode())
	for index := range responseSizeMutations {
		fill := padding / responseSizeMutations
		if index < padding%responseSizeMutations {
			fill++
		}
		values[index] += strings.Repeat(string(rune('a'+index%26)), fill)
	}
	request := encode()
	if len(request) != pushRequestLimitOctets {
		t.Fatalf("push request octets = %d, want %d", len(request), pushRequestLimitOctets)
	}

	status, response := postRawPush(t, ctx, harness, token, request)
	if status != http.StatusOK || len(response) <= pushRequestLimitOctets {
		t.Fatalf("push = status %d with %d response octets, want 200 above %d", status, len(response), pushRequestLimitOctets)
	}
	t.Logf("request octets = %d, response octets = %d", len(request), len(response))

	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(response, &envelope); err != nil {
		t.Fatalf("decode push response: %v", err)
	}
	if keys := sortedKeys(envelope); !slices.Equal(keys, []string{"accepted", "batch_id", "rejected", "server_time"}) {
		t.Fatalf("push response members = %v", keys)
	}
	var responseBatchID string
	var accepted, rejected []map[string]any
	if json.Unmarshal(envelope["batch_id"], &responseBatchID) != nil || responseBatchID != batchID ||
		json.Unmarshal(envelope["accepted"], &accepted) != nil || len(accepted) != responseSizeMutations ||
		json.Unmarshal(envelope["rejected"], &rejected) != nil || rejected == nil || len(rejected) != 0 {
		t.Fatalf("push response batch %q accepted %d rejected %v", responseBatchID, len(accepted), rejected)
	}

	state := readResponseSizeState(t, ctx, admin)
	if len(state) != responseSizeMutations {
		t.Fatalf("server rows = %d, want %d", len(state), responseSizeMutations)
	}
	schemaJSON := mustJSON(t, client.Schema)
	outcomeKeys := []string{"mutation_id", "outcome_schema", "pk", "row_checksum", "server_row", "server_version", "status", "table"}
	for index, outcome := range accepted {
		recordID := recordIDs[index]
		row := state[index]
		if row.id != recordID || row.owner != "diagnostic-user" || row.value != values[index] || row.deleted {
			t.Fatalf("server row %d = %+v, want id %s owner diagnostic-user and pushed value", index, row, recordID)
		}
		expectedRow := map[string]any{
			table.PrimaryKeyField: recordID,
			ownerField:            "diagnostic-user",
			table.ValueField:      values[index],
			updatedField:          row.updatedAt,
			deletedField:          nil,
		}
		fields := make([]vectors.RowField, 0, len(expectedRow))
		for fieldID, value := range expectedRow {
			fields = append(fields, vectors.RowField{FieldID: fieldID, Value: mustJSON(t, value)})
		}
		digest, err := vectors.RowDigest(manifest, table.ID, vectors.Row{PK: mustJSON(t, recordID), Fields: fields}, row.version)
		if err != nil {
			t.Fatalf("compute row digest %d: %v", index, err)
		}
		checksum, validChecksum := mutationControlChecksumDigest(outcome["row_checksum"])
		if keys := sortedKeys(outcome); !slices.Equal(keys, outcomeKeys) ||
			outcome["mutation_id"] != mutationIDs[index] || outcome["table"] != table.ID ||
			outcome["status"] != "applied" || outcome["server_version"] != row.version ||
			!bytes.Equal(mustJSON(t, outcome["pk"]), mustJSON(t, map[string]any{table.PrimaryKeyField: recordID})) ||
			!bytes.Equal(mustJSON(t, outcome["outcome_schema"]), schemaJSON) ||
			!bytes.Equal(mustJSON(t, outcome["server_row"]), mustJSON(t, expectedRow)) ||
			!validChecksum || checksum != hex.EncodeToString(digest[:]) {
			t.Fatalf("accepted outcome %d does not match the committed row at version %s", index, row.version)
		}
	}

	ledger := readResponseSizeLedger(t, ctx, admin, client.ID)
	if ledger.batches != 1 || ledger.mutations != responseSizeMutations || ledger.state != "completed" ||
		ledger.status != http.StatusOK || !bytes.Equal(ledger.request, request) || !bytes.Equal(ledger.response, response) {
		t.Fatalf("push ledger = %d batches, %d mutations, state %q, status %d, request %d octets, response %d octets",
			ledger.batches, ledger.mutations, ledger.state, ledger.status, len(ledger.request), len(ledger.response))
	}

	replayStatus, replay := postRawPush(t, ctx, harness, token, request)
	if replayStatus != http.StatusOK || !bytes.Equal(replay, response) {
		t.Fatalf("replay = status %d with %d octets, want the original %d octets", replayStatus, len(replay), len(response))
	}
	if after := readResponseSizeState(t, ctx, admin); !slices.Equal(after, state) {
		t.Fatal("replay changed a server row, timestamp, or row version")
	}
	if after := readResponseSizeLedger(t, ctx, admin, client.ID); after.batches != 1 || after.mutations != responseSizeMutations {
		t.Fatalf("replay ledger = %d batches, %d mutations, want 1 and %d", after.batches, after.mutations, responseSizeMutations)
	}
}

type responseSizeRow struct {
	id, owner, value, updatedAt, version string
	deleted                              bool
}

type responseSizeLedger struct {
	batches, mutations int
	state              string
	status             int
	request, response  []byte
}

// postRawPush reads the whole response, because the shared helper stops at 1 MiB.
func postRawPush(t *testing.T, ctx context.Context, harness *blackbox.Harness, token string, body []byte) (int, []byte) {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, harness.AdapterURL()+"/sync/push", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create push request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: time.Minute}).Do(request)
	if err != nil {
		t.Fatalf("send push request: %v", err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read push response: %v", err)
	}
	return response.StatusCode, responseBody
}

func readResponseSizeState(t *testing.T, ctx context.Context, admin *sql.DB) []responseSizeRow {
	t.Helper()
	rows, err := admin.QueryContext(ctx, `
		SELECT item.id::text, item.owner_id, item.value,
		       to_char(item.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
		       item.deleted_at IS NOT NULL, version.row_version::text
		FROM public.cf_items item
		JOIN synchro.sync_registry registry ON registry.table_name = 'cf_items'
		JOIN synchro.sync_registry_generations generation
		  ON generation.generation = registry.registry_generation AND generation.state = 'active'
		JOIN synchro.sync_row_versions version
		  ON version.relation_id = registry.relation_id AND version.record_id = item.id::text
		WHERE item.id::text LIKE $1
		ORDER BY item.id`, responseSizeRecordPrefix+"%")
	if err != nil {
		t.Fatalf("read server rows: %v", err)
	}
	defer rows.Close()
	var state []responseSizeRow
	for rows.Next() {
		var row responseSizeRow
		if err := rows.Scan(&row.id, &row.owner, &row.value, &row.updatedAt, &row.deleted, &row.version); err != nil {
			t.Fatalf("scan server row: %v", err)
		}
		state = append(state, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read server rows: %v", err)
	}
	return state
}

func readResponseSizeLedger(t *testing.T, ctx context.Context, admin *sql.DB, clientID string) responseSizeLedger {
	t.Helper()
	var ledger responseSizeLedger
	if err := admin.QueryRowContext(ctx, `
		SELECT (SELECT count(*) FROM synchro.sync_push_batches WHERE client_id = $1),
		       (SELECT count(*) FROM synchro.sync_push_mutations WHERE client_id = $1),
		       batch.execution_state, batch.http_status,
		       batch.sealed_canonical_request, batch.sealed_canonical_response
		FROM synchro.sync_push_batches batch
		WHERE batch.client_id = $1`, clientID).Scan(
		&ledger.batches, &ledger.mutations, &ledger.state, &ledger.status, &ledger.request, &ledger.response,
	); err != nil {
		t.Fatalf("read push ledger: %v", err)
	}
	return ledger
}

func sortedKeys[V any](object map[string]V) []string {
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encode JSON: %v", err)
	}
	return encoded
}
