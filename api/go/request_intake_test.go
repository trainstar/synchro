package synchroapi

import (
	"bytes"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func runRequestIntake(t *testing.T, raw []byte, allowedMembers ...string) (int, []byte, bool) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/sync/test", bytes.NewReader(raw))
	response := httptest.NewRecorder()
	got, _, ok := decodeJSONBodyObject(response, req, allowedMembers...)
	return response.Code, got, ok
}

func TestRequestIntakeRejectsDuplicateMembersAtEveryNestingLevel(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "top level",
			body: `{"client_id":"first","client_id":"second"}`,
		},
		{
			name: "nested mutation",
			body: `{"client_id":"client","mutations":[{"mutation_id":"first","mutation_id":"second"}]}`,
		},
		{
			name: "nested field",
			body: `{"client_id":"client","mutations":[{"columns":{"field":"first","field":"second"}}]}`,
		},
		{
			name: "escaped member name",
			body: `{"client_id":"first","\u0063lient_id":"second"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, _, ok := runRequestIntake(t, []byte(tt.body), pushRequestMembers...)
			if ok {
				t.Fatal("duplicate member was accepted")
			}
			if status != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", status, http.StatusBadRequest)
			}
		})
	}
}

func TestRequestIntakeRejectsMalformedJSONAndNonObjectEnvelopes(t *testing.T) {
	tests := []struct {
		name string
		body []byte
	}{
		{name: "malformed JSON", body: []byte(`{"client_id":`)},
		{name: "multiple top level values", body: []byte(`{} {}`)},
		{name: "array envelope", body: []byte(`[]`)},
		{name: "scalar envelope", body: []byte(`null`)},
		{name: "invalid UTF-8", body: []byte{'{', '"', 'x', '"', ':', '"', 0xff, '"', '}'}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, _, ok := runRequestIntake(t, tt.body, pushRequestMembers...)
			if ok {
				t.Fatal("invalid JSON was accepted")
			}
			if status != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", status, http.StatusBadRequest)
			}
		})
	}
}

func TestRequestIntakeRejectsUnknownTopLevelMembersPerEndpoint(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		allowed []string
	}{
		{
			name:    "connect",
			body:    `{"client_id":"client","unknown":true}`,
			allowed: connectRequestMembers,
		},
		{
			name:    "pull",
			body:    `{"client_id":"client","unknown":true}`,
			allowed: pullRequestMembers,
		},
		{
			name:    "push",
			body:    `{"client_id":"client","unknown":true}`,
			allowed: pushRequestMembers,
		},
		{
			name:    "rebuild",
			body:    `{"client_id":"client","unknown":true}`,
			allowed: rebuildRequestMembers,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, _, ok := runRequestIntake(t, []byte(tt.body), tt.allowed...)
			if ok {
				t.Fatal("unknown top-level member was accepted")
			}
			if status != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", status, http.StatusBadRequest)
			}
		})
	}
}

func TestRequestIntakePreservesRawBytes(t *testing.T) {
	want := []byte(" {\"client_id\" : \"client\"} \n")
	status, got, ok := runRequestIntake(t, want, "client_id")
	if !ok {
		t.Fatalf("request was rejected with status %d", status)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("raw body changed from %q to %q", want, got)
	}
}

func TestRequestIntakeEnforcesExactBodyLimit(t *testing.T) {
	prefix := `{"client_id":"`
	suffix := `"}`
	body := []byte(prefix + strings.Repeat("x", maxJSONBodyBytes-len(prefix)-len(suffix)) + suffix)
	if len(body) != maxJSONBodyBytes {
		t.Fatalf("test body length = %d, want %d", len(body), maxJSONBodyBytes)
	}

	status, _, ok := runRequestIntake(t, body, "client_id")
	if !ok {
		t.Fatalf("body at limit was rejected with status %d", status)
	}

	overLimit := append(append([]byte(nil), body...), ' ')
	status, _, ok = runRequestIntake(t, overLimit, "client_id")
	if ok {
		t.Fatal("body over limit was accepted")
	}
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", status, http.StatusBadRequest)
	}
}

func TestRequestIntakeEnforcesNestingLimit(t *testing.T) {
	atLimit := `{"client_id":"client","mutations":` + strings.Repeat("[", maxJSONNestingDepth-1) +
		`null` + strings.Repeat("]", maxJSONNestingDepth-1) + `}`
	status, _, ok := runRequestIntake(t, []byte(atLimit), pushRequestMembers...)
	if !ok {
		t.Fatalf("JSON at nesting limit was rejected with status %d", status)
	}

	overLimit := `{"client_id":"client","mutations":` + strings.Repeat("[", maxJSONNestingDepth) +
		`null` + strings.Repeat("]", maxJSONNestingDepth) + `}`
	status, _, ok = runRequestIntake(t, []byte(overLimit), pushRequestMembers...)
	if ok {
		t.Fatal("JSON over nesting limit was accepted")
	}
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", status, http.StatusBadRequest)
	}
}

func TestRequestIntakeRequiresApplicationJSONMediaType(t *testing.T) {
	tests := []struct {
		name        string
		contentType []string
		wantOK      bool
	}{
		{name: "canonical", contentType: []string{"application/json"}, wantOK: true},
		{name: "compatible parameter", contentType: []string{"application/json; charset=utf-8"}, wantOK: true},
		{name: "case insensitive type", contentType: []string{"Application/JSON; Charset=UTF-8"}, wantOK: true},
		{name: "missing"},
		{name: "unsupported", contentType: []string{"text/plain"}},
		{name: "malformed", contentType: []string{"application/json; charset"}},
		{name: "duplicate", contentType: []string{"application/json", "application/json"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/sync/connect", nil)
			for _, value := range tt.contentType {
				request.Header.Add("Content-Type", value)
			}
			response := httptest.NewRecorder()
			if got := requireJSONMediaType(response, request); got != tt.wantOK {
				t.Fatalf("requireJSONMediaType() = %t, want %t", got, tt.wantOK)
			}
			if !tt.wantOK && response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
			}
		})
	}
}

func TestJSONRoutesRejectUnsupportedMediaTypeBeforeDatabaseAccess(t *testing.T) {
	handler := &Handler{db: &sql.DB{}}
	tests := []struct {
		name  string
		serve func(http.ResponseWriter, *http.Request)
	}{
		{name: "connect", serve: handler.serveConnect},
		{name: "pull", serve: handler.servePull},
		{name: "push", serve: handler.servePush},
		{name: "rebuild", serve: handler.serveRebuild},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/sync/"+tt.name, nil)
			request.Header.Set("Content-Type", "text/plain")
			request = request.WithContext(WithUserID(request.Context(), "user"))
			response := httptest.NewRecorder()
			tt.serve(response, request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
			}
		})
	}
}
