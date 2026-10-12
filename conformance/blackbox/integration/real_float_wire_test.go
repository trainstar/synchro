package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
	"github.com/trainstar/synchro/conformance/vectors"
)

// realFloatWireCase is one shared source binary64 value and its RFC 8785 text.
type realFloatWireCase struct {
	Source    string `json:"source"`
	Canonical string `json:"canonical"`
}

type realFloatWireExchange struct {
	response blackbox.Response
	body     map[string]any
}

func TestRealFloatWire(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	cases := readRealFloatWireCases(t)
	harness, token := provisionRealProofHarness(t, ctx)

	// The diagnostic schema has no float field. The fixed operator transitions add one to cf_items.
	if err := harness.Operator().TransitionSyncedTableField(ctx, "cf_items", "", &scenarios.QueueReplaySchemaField{Name: "measure", Type: "string", Nullable: true, Writable: true}, "", ""); err != nil {
		t.Fatalf("add float wire field: %v", err)
	}
	waitForRealFloatWireField(t, ctx, harness, "", "string")
	if err := harness.Operator().TransitionSyncedTableField(ctx, "cf_items", "", nil, "measure", "float"); err != nil {
		t.Fatalf("change float wire field type: %v", err)
	}
	manifest, measureField := waitForRealFloatWireField(t, ctx, harness, "string", "float")

	// Source DML crosses the real WAL decoder and worker, so -0.0 reaches the server as PostgreSQL text.
	sourceIDs := make([]string, len(cases))
	for index, testCase := range cases {
		source, err := strconv.ParseFloat(testCase.Source, 64)
		if err != nil {
			t.Fatalf("parse float wire source %q: %v", testCase.Source, err)
		}
		sourceIDs[index] = fmt.Sprintf("00000000-0000-4000-8f48-%012d", index+1)
		if err := harness.Source().ExecContext(
			ctx,
			"INSERT INTO cf_items (id, owner_id, value, measure) VALUES ($1, $2, $3, $4)",
			sourceIDs[index],
			"diagnostic-user",
			"float-wire-source",
			source,
		); err != nil {
			t.Fatalf("insert float wire source row %d: %v", index, err)
		}
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", sourceIDs...)

	client := connectRealProtocolClient(t, ctx, harness, token, "float-wire-client")
	table := requireRealTable(t, client, "cf_items")
	rebuildRealScope(t, ctx, harness, token, client, "cf:global", "00000000-0000-4000-8f48-000000001001")
	const rebuildLimit = 4
	records, pages := rebuildRealFloatWireScope(t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-8f48-000000001002", rebuildLimit)
	if pages < (len(cases)+rebuildLimit-1)/rebuildLimit {
		t.Fatalf("float wire rebuild used %d pages for %d rows at limit %d", pages, len(cases), rebuildLimit)
	}
	for index, testCase := range cases {
		record := requireRealFloatWireEntry(t, records, "rebuild", table, sourceIDs[index])
		requireRealFloatWireRow(t, manifest, table, measureField, record, "row", testCase.Canonical)
	}

	// Later source work must still reach pull after the worker materialized the float rows.
	for index, recordID := range sourceIDs {
		if err := harness.Source().ExecContext(ctx, "UPDATE cf_items SET value = $2 WHERE id = $1", recordID, "float-wire-later"); err != nil {
			t.Fatalf("update float wire source row %d: %v", index, err)
		}
	}
	later := pullUntilRealFloatWireRows(t, ctx, harness, token, client, table, sourceIDs, "float-wire-later")
	for index, testCase := range cases {
		requireRealFloatWireRow(t, manifest, table, measureField, later[sourceIDs[index]], "row", testCase.Canonical)
	}

	// Push keeps its stored canonical TEXT, so a replay must return identical bytes.
	pushIDs := make([]string, len(cases))
	mutations := make([]map[string]any, len(cases))
	for index, testCase := range cases {
		pushIDs[index] = fmt.Sprintf("00000000-0000-4000-8f48-%012d", 2001+index)
		mutations[index] = map[string]any{
			"mutation_id":     fmt.Sprintf("00000000-0000-4000-8f48-%012d", 3001+index),
			"table":           table.ID,
			"pk":              map[string]any{table.PrimaryKeyField: pushIDs[index]},
			"authored_schema": client.Schema,
			"op":              "insert",
			"client_version":  phase4ClientVersion,
			"columns": map[string]any{
				table.ValueField: "float-wire-push",
				measureField:     json.RawMessage(testCase.Canonical),
			},
		}
	}
	pushBody, err := json.Marshal(phase4PushPayload(client, "00000000-0000-4000-8f48-000000004001", mutations))
	if err != nil {
		t.Fatalf("encode float wire push: %v", err)
	}
	push := blackbox.Request{
		Method:  http.MethodPost,
		Path:    "/sync/push",
		Headers: http.Header{"Content-Type": []string{"application/json"}},
		Body:    pushBody,
		Class:   "float-wire/push",
	}
	first := doRealFloatWire(t, ctx, harness, token, push)
	accepted := requireOutcomeList(t, first.body, "accepted")
	if len(accepted) != len(cases) {
		t.Fatalf("float wire push accepted %d of %d mutations: %s", len(accepted), len(cases), first.response.Body)
	}
	for index, testCase := range cases {
		outcome := requireRealFloatWireEntry(t, accepted, "push", table, pushIDs[index])
		if outcome["status"] != "applied" {
			t.Fatalf("float wire push outcome %d = %#v", index, outcome)
		}
		requireRealFloatWireRow(t, manifest, table, measureField, outcome, "server_row", testCase.Canonical)
	}
	replay := doRealFloatWire(t, ctx, harness, token, push)
	if !bytes.Equal(first.response.Body, replay.response.Body) {
		t.Fatalf("float wire push replay changed bytes:\nfirst=%s\nreplay=%s", first.response.Body, replay.response.Body)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", pushIDs...)
	pushed := pullUntilRealFloatWireRows(t, ctx, harness, token, client, table, pushIDs, "float-wire-push")
	for index, testCase := range cases {
		requireRealFloatWireRow(t, manifest, table, measureField, pushed[pushIDs[index]], "row", testCase.Canonical)
	}
}

func readRealFloatWireCases(t *testing.T) []realFloatWireCase {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "protocol", "float-wire-boundaries-v1.json"))
	if err != nil {
		t.Fatalf("read shared float wire cases: %v", err)
	}
	var document struct {
		Version int                 `json:"version"`
		Cases   []realFloatWireCase `json:"cases"`
	}
	if err := json.Unmarshal(data, &document); err != nil || document.Version != 1 || len(document.Cases) == 0 {
		t.Fatalf("decode shared float wire cases: version=%d cases=%d err=%v", document.Version, len(document.Cases), err)
	}
	return document.Cases
}

// waitForRealFloatWireField waits while the active manifest gives cf_items.measure the prior type.
// An empty prior type means that the field is absent. Any other response or state fails the test.
func waitForRealFloatWireField(t *testing.T, ctx context.Context, harness *blackbox.Harness, priorType, fieldType string) (vectors.Manifest, string) {
	t.Helper()
	client := &blackbox.Client{BaseURL: harness.AdapterURL(), HTTP: &http.Client{Timeout: 30 * time.Second}}
	deadline := time.Now().Add(45 * time.Second)
	for {
		response, err := client.Do(ctx, blackbox.Request{
			Method: http.MethodGet,
			Path:   "/sync/schema",
			Class:  "float-wire/schema",
		})
		if err != nil {
			t.Fatalf("request float wire schema: %v", err)
		}
		if response.Status != http.StatusOK {
			t.Fatalf("float wire schema status = %d: %s", response.Status, response.Body)
		}
		var envelope struct {
			Manifest json.RawMessage `json:"manifest"`
		}
		var manifest struct {
			Tables []struct {
				Name   string `json:"name"`
				Fields []struct {
					FieldID string `json:"field_id"`
					Name    string `json:"name"`
					Type    string `json:"type"`
				} `json:"fields"`
			} `json:"tables"`
		}
		if err := json.Unmarshal(response.Body, &envelope); err != nil || len(envelope.Manifest) == 0 {
			t.Fatalf("decode float wire schema envelope: %v", err)
		}
		if err := json.Unmarshal(envelope.Manifest, &manifest); err != nil {
			t.Fatalf("decode float wire schema manifest: %v", err)
		}
		tableFound := false
		currentType, fieldID := "", ""
		for _, table := range manifest.Tables {
			if table.Name != "cf_items" {
				continue
			}
			tableFound = true
			for _, field := range table.Fields {
				if field.Name == "measure" {
					currentType, fieldID = field.Type, field.FieldID
				}
			}
		}
		switch {
		case !tableFound:
			t.Fatal("float wire schema manifest has no cf_items table")
		case currentType == fieldType:
			parsed, err := vectors.ParseManifest(envelope.Manifest)
			if err != nil {
				t.Fatalf("parse float wire manifest independently: %v", err)
			}
			return parsed, fieldID
		case currentType != priorType:
			t.Fatalf("cf_items.measure type = %q, want %q or %q", currentType, priorType, fieldType)
		case time.Now().After(deadline):
			t.Fatalf("cf_items.measure did not become %s: %s", fieldType, harness.FailureDiagnostics())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// doRealFloatWire sends one request and checks the HTTP framing of its raw successful body.
func doRealFloatWire(t *testing.T, ctx context.Context, harness *blackbox.Harness, token string, request blackbox.Request) realFloatWireExchange {
	t.Helper()
	response, err := newRealBlackboxClient(harness.AdapterURL(), token).Do(ctx, request)
	if err != nil {
		t.Fatalf("send float wire %s: %v", request.Class, err)
	}
	if response.Status != http.StatusOK {
		t.Fatalf("float wire %s status = %d: %s", request.Class, response.Status, response.Body)
	}
	if got := response.Headers.Get("Content-Length"); got != strconv.Itoa(len(response.Body)) {
		t.Fatalf("float wire %s Content-Length = %q, want %d", request.Class, got, len(response.Body))
	}
	decoder := json.NewDecoder(bytes.NewReader(response.Body))
	// json.Number keeps the exact number token text from the HTTP body.
	decoder.UseNumber()
	var body map[string]any
	if err := decoder.Decode(&body); err != nil {
		t.Fatalf("decode float wire %s: %v", request.Class, err)
	}
	return realFloatWireExchange{response: response, body: body}
}

func postRealFloatWire(t *testing.T, ctx context.Context, harness *blackbox.Harness, token, path, class string, payload map[string]any) map[string]any {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("encode float wire %s: %v", class, err)
	}
	return doRealFloatWire(t, ctx, harness, token, blackbox.Request{
		Method:  http.MethodPost,
		Path:    path,
		Headers: http.Header{"Content-Type": []string{"application/json"}},
		Body:    body,
		Class:   class,
	}).body
}

func rebuildRealFloatWireScope(t *testing.T, ctx context.Context, harness *blackbox.Harness, token string, client *realProtocolClient, scopeID, rebuildID string, limit int) ([]map[string]any, int) {
	t.Helper()
	var cursor any
	var records []map[string]any
	for page := 1; page <= 16; page++ {
		response := postRealFloatWire(t, ctx, harness, token, "/sync/rebuild", "float-wire/rebuild", map[string]any{
			"client_id":         client.ID,
			"client_generation": client.Generation,
			"schema":            client.Schema,
			"scope":             scopeID,
			"rebuild_id":        rebuildID,
			"cursor":            cursor,
			"limit":             limit,
		})
		pageRecords, ok := response["records"].([]any)
		if !ok || response["scope"] != scopeID {
			t.Fatalf("float wire rebuild page %d is invalid: %#v", page, response)
		}
		for _, rawRecord := range pageRecords {
			record, ok := rawRecord.(map[string]any)
			if !ok {
				t.Fatalf("float wire rebuild record is invalid: %#v", rawRecord)
			}
			records = append(records, record)
		}
		if response["has_more"] == true {
			next, ok := response["cursor"].(string)
			if !ok || next == "" {
				t.Fatalf("float wire rebuild continuation is invalid: %#v", response["cursor"])
			}
			cursor = next
			continue
		}
		final, ok := response["final_scope_cursor"].(string)
		if response["has_more"] != false || !ok || final == "" {
			t.Fatalf("float wire rebuild final page is invalid: %#v", response)
		}
		client.Scopes[scopeID] = map[string]any{"cursor": final}
		return records, page
	}
	t.Fatal("float wire rebuild exceeded its page bound")
	return nil, 0
}

// pullUntilRealFloatWireRows pulls until every record arrives with the wanted value.
func pullUntilRealFloatWireRows(t *testing.T, ctx context.Context, harness *blackbox.Harness, token string, client *realProtocolClient, table realProtocolTable, recordIDs []string, value string) map[string]map[string]any {
	t.Helper()
	delivered := make(map[string]map[string]any, len(recordIDs))
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		response := postRealFloatWire(t, ctx, harness, token, "/sync/pull", "float-wire/pull", map[string]any{
			"client_id":         client.ID,
			"client_generation": client.Generation,
			"schema":            client.Schema,
			"scope_set_version": client.ScopeSetVersion,
			"scopes":            client.Scopes,
			"limit":             100,
		})
		changes, changesOK := response["changes"].([]any)
		cursors, cursorsOK := response["scope_cursors"].(map[string]any)
		if !changesOK || !cursorsOK {
			t.Fatalf("float wire pull is invalid: %#v", response)
		}
		for scopeID, rawCursor := range cursors {
			cursor, ok := rawCursor.(string)
			if _, assigned := client.Scopes[scopeID]; !ok || cursor == "" || !assigned {
				t.Fatalf("float wire pull cursor for %s is invalid", scopeID)
			}
			client.Scopes[scopeID] = map[string]any{"cursor": cursor}
		}
		for _, rawChange := range changes {
			change, ok := rawChange.(map[string]any)
			if !ok {
				t.Fatalf("float wire pull change is invalid: %#v", rawChange)
			}
			row, _ := change["row"].(map[string]any)
			for _, recordID := range recordIDs {
				if realFloatWireEntryMatches(change, table, recordID) && row[table.ValueField] == value {
					delivered[recordID] = change
				}
			}
		}
		if len(delivered) == len(recordIDs) {
			return delivered
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("float wire pull delivered %d of %d rows with value %q: %s", len(delivered), len(recordIDs), value, harness.FailureDiagnostics())
	return nil
}

func realFloatWireEntryMatches(entry map[string]any, table realProtocolTable, recordID string) bool {
	pk, _ := entry["pk"].(map[string]any)
	return entry["table"] == table.ID && pk[table.PrimaryKeyField] == recordID
}

func requireRealFloatWireEntry(t *testing.T, entries []map[string]any, source string, table realProtocolTable, recordID string) map[string]any {
	t.Helper()
	for _, entry := range entries {
		if realFloatWireEntryMatches(entry, table, recordID) {
			return entry
		}
	}
	t.Fatalf("float wire %s omitted record %s", source, recordID)
	return nil
}

// requireRealFloatWireRow checks the exact number token and the independently computed row checksum.
func requireRealFloatWireRow(t *testing.T, manifest vectors.Manifest, table realProtocolTable, measureField string, entry map[string]any, rowMember, canonical string) {
	t.Helper()
	row, ok := entry[rowMember].(map[string]any)
	if !ok {
		t.Fatalf("float wire entry has no %s: %#v", rowMember, entry)
	}
	token, ok := row[measureField].(json.Number)
	if !ok || token.String() != canonical {
		t.Fatalf("float wire token = %#v, want %s", row[measureField], canonical)
	}
	pk, ok := entry["pk"].(map[string]any)
	if !ok || len(pk) != 1 {
		t.Fatalf("float wire entry primary key is invalid: %#v", entry["pk"])
	}
	primaryKey, err := json.Marshal(pk[table.PrimaryKeyField])
	if err != nil {
		t.Fatalf("encode float wire primary key: %v", err)
	}
	fields := make([]vectors.RowField, 0, len(row))
	for fieldID, value := range row {
		encoded := json.RawMessage(canonical)
		if fieldID != measureField {
			encoded, err = json.Marshal(value)
			if err != nil {
				t.Fatalf("encode float wire field %s: %v", fieldID, err)
			}
		}
		fields = append(fields, vectors.RowField{FieldID: fieldID, Value: encoded})
	}
	serverVersion, _ := entry["server_version"].(string)
	digest, err := vectors.RowDigest(manifest, table.ID, vectors.Row{PK: primaryKey, Fields: fields}, serverVersion)
	if err != nil {
		t.Fatalf("compute independent float wire row digest: %v", err)
	}
	checksum, ok := entry["row_checksum"].(map[string]any)
	if !ok || checksum["digest"] != fmt.Sprintf("%x", digest) {
		t.Fatalf("float wire row checksum = %#v, want digest %x", entry["row_checksum"], digest)
	}
}
