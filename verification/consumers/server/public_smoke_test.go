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
	state := smokePullState()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requests++
		if requests == 1 {
			response.WriteHeader(http.StatusServiceUnavailable)
			_, _ = response.Write([]byte(`{"error":{"code":"capture_pending"}}`))
			return
		}
		_ = json.NewEncoder(response).Encode(pullPage(state, []any{smokePulledCustomer(state)}, false, nil))
	}))
	defer server.Close()

	if err := pullUntilCustomerDelivered(
		context.Background(),
		server.Client(),
		"token",
		server.URL,
		state,
	); err != nil {
		t.Fatalf("pull until customer delivered: %v", err)
	}
	if requests != 2 {
		t.Fatalf("request count = %d, want 2", requests)
	}
}

func TestPullRequiresExactAuthoredCustomer(t *testing.T) {
	state := smokePullState()
	wrongValue := smokePulledCustomer(state)
	wrongValue["row"].(map[string]any)[state.Fields["balance"]] = "1"
	missingValue := smokePulledCustomer(state)
	delete(missingValue["row"].(map[string]any), state.Fields["name"])
	wrongRow := smokePulledCustomer(state)
	wrongRow["pk"] = map[string]any{state.PrimaryKeyID: "another-customer"}
	wrongRowPrimaryKey := smokePulledCustomer(state)
	wrongRowPrimaryKey["row"].(map[string]any)[state.PrimaryKeyID] = "another-customer"
	rebuildOnly := pullPage(state, []any{}, false, nil)
	rebuildOnly["rebuild"] = []any{"scope:customer"}

	tests := []struct {
		name string
		page map[string]any
	}{
		{name: "empty", page: pullPage(state, []any{}, false, nil)},
		{name: "wrong row", page: pullPage(state, []any{wrongRow}, false, nil)},
		{name: "wrong row primary key", page: pullPage(state, []any{wrongRowPrimaryKey}, false, nil)},
		{name: "wrong value", page: pullPage(state, []any{wrongValue}, false, nil)},
		{name: "missing value", page: pullPage(state, []any{missingValue}, false, nil)},
		{name: "rebuild only", page: rebuildOnly},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				_ = json.NewEncoder(response).Encode(test.page)
			}))
			defer server.Close()

			if err := pullUntilCustomerDelivered(
				context.Background(),
				server.Client(),
				"token",
				server.URL,
				state,
			); err == nil {
				t.Fatal("pull without the exact authored customer passed")
			}
		})
	}
}

func TestPullFollowsCursorDeltasUntilAuthoredCustomer(t *testing.T) {
	state := smokePullState()
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requests++
		var body struct {
			Scopes map[string]scopeCursor `json:"scopes"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Errorf("decode pull request: %v", err)
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		if requests == 1 {
			if body.Scopes["scope:customer"].Cursor == nil ||
				*body.Scopes["scope:customer"].Cursor != "baseline-cursor" {
				t.Errorf("first pull cursor = %#v", body.Scopes)
			}
			_ = json.NewEncoder(response).Encode(
				pullPage(state, []any{}, true, map[string]string{"scope:customer": "page-one"}),
			)
			return
		}
		if body.Scopes["scope:customer"].Cursor == nil ||
			*body.Scopes["scope:customer"].Cursor != "page-one" {
			t.Errorf("second pull cursor = %#v", body.Scopes)
		}
		_ = json.NewEncoder(response).Encode(pullPage(state, []any{smokePulledCustomer(state)}, false, nil))
	}))
	defer server.Close()

	if err := pullUntilCustomerDelivered(
		context.Background(),
		server.Client(),
		"token",
		server.URL,
		state,
	); err != nil {
		t.Fatalf("pull until customer delivered: %v", err)
	}
	if requests != 2 {
		t.Fatalf("request count = %d, want 2", requests)
	}
	if state.Scopes["scope:customer"].Cursor == nil ||
		*state.Scopes["scope:customer"].Cursor != "baseline-cursor" {
		t.Fatal("paginated pull changed the persisted baseline cursor")
	}
}

func TestBootstrapIncrementalCursorsFollowsRebuildContinuation(t *testing.T) {
	state := smokePullState()
	state.Scopes["scope:customer"] = scopeCursor{Cursor: nil}
	requests := 0
	var rebuildID string
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requests++
		var body struct {
			Scope     string  `json:"scope"`
			RebuildID string  `json:"rebuild_id"`
			Cursor    *string `json:"cursor"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Errorf("decode rebuild request: %v", err)
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		if body.Scope != "scope:customer" {
			t.Errorf("rebuild scope = %q", body.Scope)
		}
		if requests == 1 {
			rebuildID = body.RebuildID
			if body.Cursor != nil {
				t.Errorf("first rebuild cursor = %q", *body.Cursor)
			}
			_ = json.NewEncoder(response).Encode(map[string]any{
				"scope":    body.Scope,
				"records":  []any{},
				"cursor":   "rebuild-page-one",
				"has_more": true,
			})
			return
		}
		if body.RebuildID != rebuildID || body.Cursor == nil || *body.Cursor != "rebuild-page-one" {
			t.Errorf("continuation request = %#v", body)
		}
		_ = json.NewEncoder(response).Encode(map[string]any{
			"scope":              body.Scope,
			"records":            []any{},
			"cursor":             nil,
			"has_more":           false,
			"final_scope_cursor": "incremental-baseline",
			"checksum": map[string]any{
				"algorithm": "sha256",
				"version":   1,
				"encoding":  "hex",
				"digest":    strings.Repeat("b", 64),
			},
		})
	}))
	defer server.Close()

	bootstrapped, err := bootstrapIncrementalCursors(
		context.Background(),
		server.Client(),
		"token",
		server.URL,
		state,
	)
	if err != nil {
		t.Fatalf("bootstrap incremental cursors: %v", err)
	}
	if requests != 2 || bootstrapped.Scopes["scope:customer"].Cursor == nil ||
		*bootstrapped.Scopes["scope:customer"].Cursor != "incremental-baseline" {
		t.Fatalf("bootstrapped state = %#v after %d requests", bootstrapped.Scopes, requests)
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

func smokePullState() clientState {
	baseline := "baseline-cursor"
	return clientState{
		ClientID:        smokeClientID,
		Generation:      1,
		ScopeSetVersion: 1,
		Schema:          schemaRef{Version: 1, Hash: strings.Repeat("a", 64)},
		Scopes: map[string]scopeCursor{
			"scope:customer": {Cursor: &baseline},
		},
		TableID:      "table-customers",
		PrimaryKeyID: "field-id",
		Fields: map[string]string{
			"user_id":   "field-user",
			"name":      "field-name",
			"balance":   "field-balance",
			"is_active": "field-active",
		},
	}
}

func smokePulledCustomer(state clientState) map[string]any {
	return map[string]any{
		"scope": "scope:customer",
		"table": state.TableID,
		"op":    "upsert",
		"pk": map[string]any{
			state.PrimaryKeyID: smokeRowID,
		},
		"row": map[string]any{
			state.PrimaryKeyID:        smokeRowID,
			state.Fields["user_id"]:   smokeUserID,
			state.Fields["name"]:      "Packaged server consumer",
			state.Fields["balance"]:   "0",
			state.Fields["is_active"]: true,
		},
	}
}

func pullPage(
	state clientState,
	changes []any,
	hasMore bool,
	scopeCursors map[string]string,
) map[string]any {
	if scopeCursors == nil {
		scopeCursors = map[string]string{}
	}
	return map[string]any{
		"changes":           changes,
		"scope_set_version": state.ScopeSetVersion,
		"scope_cursors":     scopeCursors,
		"scope_updates":     map[string]any{"add": []any{}, "remove": []any{}},
		"rebuild":           []any{},
		"has_more":          hasMore,
		"checksums":         map[string]any{},
	}
}
