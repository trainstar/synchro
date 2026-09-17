package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParseInitialConnectBindsWritableCustomerFields(t *testing.T) {
	body := `{
		"client_generation": 1,
		"scope_set_version": 1,
		"schema": {"version": 2, "hash": "` + strings.Repeat("a", 64) + `", "action": "replace"},
		"scopes": {"add": [{"id": "user:test", "cursor": null}], "remove": []},
		"scope_cursor_updates": {"user:test": null},
		"schema_definition": {
			"tables": [{
				"name": "customers",
				"table_id": "table-customers",
				"primary_key_field_id": "field-id",
				"fields": [
					{"name": "user_id", "field_id": "field-user", "writable": true},
					{"name": "name", "field_id": "field-name", "writable": true},
					{"name": "balance", "field_id": "field-balance", "writable": true},
					{"name": "is_active", "field_id": "field-active", "writable": true},
					{"name": "created_at", "field_id": "field-created", "writable": false},
					{"name": "updated_at", "field_id": "field-updated", "writable": false}
				]
			}]
		}
	}`
	state, err := parseInitialConnect([]byte(body), smokeClientID)
	if err != nil {
		t.Fatalf("parse initial connect: %v", err)
	}
	if state.TableID != "table-customers" || state.PrimaryKeyID != "field-id" {
		t.Fatalf("customer binding = %#v", state)
	}
	if len(state.Scopes) != 1 || state.Fields["name"] != "field-name" {
		t.Fatalf("customer fields or scopes = %#v", state)
	}
	request := pushPayload(state)
	mutation := request["mutations"].([]any)[0].(map[string]any)
	columns := mutation["columns"].(map[string]any)
	if len(columns) != 4 || columns["field-user"] != smokeUserID ||
		columns["field-name"] != "Packaged server consumer" ||
		columns["field-balance"] != "0" || columns["field-active"] != true {
		t.Fatalf("push contains fields outside the writable customer input: %#v", columns)
	}
	if mutation["client_version"] != smokeTime {
		t.Fatal("push lost its mutation timestamp")
	}
	changed := strings.Replace(body, `"field_id": "field-name", "writable": true`, `"field_id": "field-name", "writable": false`, 1)
	if _, err := parseInitialConnect([]byte(changed), smokeClientID); err == nil {
		t.Fatal("missing writable customer input was accepted")
	}
}

func TestParseInitialConnectRejectsIncompleteSchema(t *testing.T) {
	body := `{
		"client_generation": 1,
		"scope_set_version": 1,
		"schema": {"version": 2, "hash": "` + strings.Repeat("a", 64) + `", "action": "replace"},
		"scopes": {"add": [], "remove": []},
		"scope_cursor_updates": {},
		"schema_definition": {"tables": []}
	}`
	if _, err := parseInitialConnect([]byte(body), smokeClientID); err == nil {
		t.Fatal("incomplete connect response passed")
	}
}

func TestRequestAddsPublicHeaders(t *testing.T) {
	request, err := newRequest(
		context.Background(),
		"test-token",
		"http://127.0.0.1/sync/connect",
		[]byte(`{"client_id":"client"}`),
	)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if request.Header.Get("Author"+"ization") != "Bear"+"er test-token" {
		t.Fatal("authorization header is invalid")
	}
	if request.Header.Get("Content-Type") != "application/json" {
		t.Fatal("content type header is invalid")
	}
}

func TestPullRetriesOnlyCapturePending(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requests++
		if requests == 1 {
			response.WriteHeader(http.StatusServiceUnavailable)
			_, _ = response.Write([]byte(`{"error":{"code":"capture_pending"}}`))
			return
		}
		_ = json.NewEncoder(response).Encode(map[string]any{
			"changes":           []any{},
			"scope_set_version": 1,
			"scope_cursors":     map[string]string{},
			"scope_updates":     map[string]any{"add": []any{}, "remove": []any{}},
			"rebuild":           []any{},
			"has_more":          false,
			"checksums":         map[string]any{},
		})
	}))
	defer server.Close()

	state := clientState{
		ClientID:        smokeClientID,
		Generation:      1,
		ScopeSetVersion: 1,
		Schema:          schemaRef{Version: 1, Hash: strings.Repeat("a", 64)},
		Scopes:          map[string]scopeCursor{},
	}
	if err := pullUntilReady(context.Background(), server.Client(), "token", server.URL, state); err != nil {
		t.Fatalf("pull until ready: %v", err)
	}
	if requests != 2 {
		t.Fatalf("request count = %d, want 2", requests)
	}
}

func TestPushDigestBindsRequestAndResponse(t *testing.T) {
	baseline := pushDigest([]byte("request"), []byte("response"))
	if baseline == pushDigest([]byte("changed"), []byte("response")) {
		t.Fatal("request change did not change the digest")
	}
	if baseline == pushDigest([]byte("request"), []byte("changed")) {
		t.Fatal("response change did not change the digest")
	}
}

func TestRequireAcceptedPushRejectsTerminalOutcome(t *testing.T) {
	accepted := `{
			"batch_id":"` + smokeBatchID + `",
			"accepted":[{"mutation_id":"` + smokeMutationID + `"}],
			"rejected":[]
		}`
	if err := requireAcceptedPush([]byte(accepted)); err != nil {
		t.Fatalf("accepted push: %v", err)
	}
	rejected := `{
			"batch_id":"` + smokeBatchID + `",
			"accepted":[],
			"rejected":[{"mutation_id":"` + smokeMutationID + `"}]
		}`
	if err := requireAcceptedPush([]byte(rejected)); err == nil {
		t.Fatal("terminally rejected push passed")
	}
}
