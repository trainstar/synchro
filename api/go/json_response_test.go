package synchroapi

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

// serveJSONRoute sends one valid request to a route whose database call returns result.
func serveJSONRoute(t *testing.T, route string, result []byte) (*httptest.ResponseRecorder, *readinessDriverState) {
	t.Helper()
	db, state := newReadinessTestDB(t, result, nil)
	handler := &Handler{db: db}
	serve, method, body := map[string]func(http.ResponseWriter, *http.Request){
		"connect": handler.serveConnect,
		"pull":    handler.servePull,
		"push":    handler.servePush,
		"rebuild": handler.serveRebuild,
		"schema":  handler.serveSchema,
		"tables":  handler.serveTables,
	}[route], http.MethodPost, `{"client_id":"client"}`
	switch route {
	case "rebuild":
		body = `{"client_id":"client","scope":"scope"}`
	case "schema", "tables":
		method, body = http.MethodGet, ""
	}
	request := httptest.NewRequest(method, "/sync/"+route, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request = request.WithContext(WithUserID(request.Context(), "user"))
	response := httptest.NewRecorder()
	serve(response, request)
	return response, state
}

func requireJSONBody(t *testing.T, response *httptest.ResponseRecorder, want string) {
	t.Helper()
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusOK, response.Body.String())
	}
	if got := response.Body.String(); got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
	if got := response.Header().Get("Content-Length"); got != strconv.Itoa(len(want)) {
		t.Fatalf("Content-Length = %q, want %d", got, len(want))
	}
	if got := response.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
}

func TestJSONBRoutesWriteRFC8785NumberTokens(t *testing.T) {
	// PostgreSQL JSONB output spacing with numeric text that native clients reject.
	result := `{"changes": [{"row": {"fld_a": 0.0000001, "fld_b": 1000000000000000000000, "fld_c": 5.0, ` +
		`"fld_d": 0.0, "fld_e": -0.0, "fld_f": 1.50, "fld_g": 18446744073709552000, "fld_h": 0.0000015, ` +
		`"fld_s": "0.0000001", "fld_i": "9223372036854775807", "fld_m": "12.3400", "fld_x": "\"1.0\\\u0031E5"}, ` +
		`"server_version": "v1"}], "has_more": false}`
	want := `{"changes": [{"row": {"fld_a": 1e-7, "fld_b": 1e+21, "fld_c": 5, ` +
		`"fld_d": 0, "fld_e": 0, "fld_f": 1.5, "fld_g": 18446744073709552000, "fld_h": 0.0000015, ` +
		`"fld_s": "0.0000001", "fld_i": "9223372036854775807", "fld_m": "12.3400", "fld_x": "\"1.0\\\u0031E5"}, ` +
		`"server_version": "v1"}], "has_more": false}`

	for _, route := range []string{"connect", "pull", "rebuild", "schema", "tables"} {
		t.Run(route, func(t *testing.T) {
			response, state := serveJSONRoute(t, route, []byte(result))
			requireJSONBody(t, response, want)
			state.mu.Lock()
			defer state.mu.Unlock()
			if state.queries != 1 {
				t.Fatalf("query count = %d, want 1", state.queries)
			}
		})
	}
}

func TestJSONBResponseReplacesOnlyNumberTokens(t *testing.T) {
	tests := []struct {
		name   string
		result string
		want   string
	}{
		{name: "top-level number", result: `0.0000001`, want: `1e-7`},
		{name: "array delimiters", result: `[0.0000001,1.0]`, want: `[1e-7,1]`},
		{name: "nested containers", result: `[[1.0],{"a":[2.50]},{"b":{"c":1E21}}]`, want: `[[1],{"a":[2.5]},{"b":{"c":1e+21}}]`},
		{name: "whitespace", result: " \t\r\n[ 1.0 ,\n\t2 ]\r\n ", want: " \t\r\n[ 1 ,\n\t2 ]\r\n "},
		{name: "trailing newline", result: "{\"a\":-0}\n", want: "{\"a\":0}\n"},
		{name: "member names and strings", result: `{"1.0":1.0,"s":["1.0","\u0031.0","-0"]}`, want: `{"1.0":1,"s":["1.0","\u0031.0","-0"]}`},
		{name: "string escapes", result: `{"a":"x\"1.0","b":["\\",1.0,"\u00221.0\\\"2.50",-1.50E-7]}`, want: `{"a":"x\"1.0","b":["\\",1,"\u00221.0\\\"2.50",-1.5e-7]}`},
		{
			name:   "canonical bytes",
			result: "{\"a\": [1e-7, 1e+21, 5, 0, -1.5, 9007199254740991, 9007199254740992, 5e-324, 1.7976931348623157e+308],\n\"b\": {\"c\": \"0.0000001\", \"d\": true, \"e\": false, \"f\": null, \"g\": \"\\u00e9\\ud83d\\ude00\\\\\\\"\\/\"}, \"h\": \"\"}",
			want:   "{\"a\": [1e-7, 1e+21, 5, 0, -1.5, 9007199254740991, 9007199254740992, 5e-324, 1.7976931348623157e+308],\n\"b\": {\"c\": \"0.0000001\", \"d\": true, \"e\": false, \"f\": null, \"g\": \"\\u00e9\\ud83d\\ude00\\\\\\\"\\/\"}, \"h\": \"\"}",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response, _ := serveJSONRoute(t, "schema", []byte(tt.result))
			requireJSONBody(t, response, tt.want)
		})
	}
}

func TestJSONBResponseRejectsInvalidResultBeforeSuccessHeaders(t *testing.T) {
	originalWriter := log.Writer()
	originalFlags := log.Flags()
	originalPrefix := log.Prefix()
	var logs bytes.Buffer
	log.SetOutput(&logs)
	log.SetFlags(0)
	log.SetPrefix("")
	t.Cleanup(func() {
		log.SetOutput(originalWriter)
		log.SetFlags(originalFlags)
		log.SetPrefix(originalPrefix)
	})

	const marker = "private7731"
	results := []string{
		``,
		` `,
		`{"private7731":1`,
		`{"a":1}}`,
		`{"a":1} {"private7731":2}`,
		`{"a":1} private7731`,
		`[1,]`,
		`{"a" 1}`,
		`{"a":01}`,
		`{"a":1,}`,
		`{"a":tru}`,
		`NaN`,
		`{"private7731":1e400}`,
		"{\"private7731\":\"\xff\"}",
		"{\"private7731\":\"\x01\"}",
	}
	for _, route := range []string{"pull", "schema"} {
		for _, result := range results {
			t.Run(route+"/"+strconv.Quote(result), func(t *testing.T) {
				logs.Reset()
				response, _ := serveJSONRoute(t, route, []byte(result))
				if response.Code != http.StatusInternalServerError {
					t.Fatalf("status = %d, want %d", response.Code, http.StatusInternalServerError)
				}
				var envelope protocolErrorEnvelope
				if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
					t.Fatalf("decode error envelope: %v", err)
				}
				if envelope.Error != (protocolError{Code: "sync_integrity_failure", Message: "sync operation failed", Retryable: false}) {
					t.Fatalf("error envelope = %#v", envelope.Error)
				}
				if got := response.Header().Get("Retry-After"); got != "" {
					t.Fatalf("Retry-After = %q, want none", got)
				}
				if strings.Contains(response.Body.String(), marker) || strings.Contains(logs.String(), marker) {
					t.Fatalf("response or log exposed result content: body=%q log=%q", response.Body.String(), logs.String())
				}
				if !strings.Contains(logs.String(), "JSONB response rejected") {
					t.Fatalf("log = %q, want one bounded rejection", logs.String())
				}
			})
		}
	}
}

func TestPushResponseKeepsStoredTextBytes(t *testing.T) {
	stored := `{"accepted": [{"server_row": {"fld_a": 0.0000001, "fld_c": 5.0}}], "note": "stored"}`
	response, state := serveJSONRoute(t, "push", []byte(stored))
	requireJSONBody(t, response, stored)
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.queries != 1 || !strings.HasPrefix(state.query, "SELECT synchro.synchro_push(") {
		t.Fatalf("push queries = %d %q, want one canonical push call", state.queries, state.query)
	}
}
