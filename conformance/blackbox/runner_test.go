package blackbox

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSignHS256KnownVector(t *testing.T) {
	claims := Claims{
		"sub":  "1234567890",
		"name": "John Doe",
		"iat":  1516239022,
	}
	token, err := SignHS256([]byte("your-256-bit-secret"), claims)
	if err != nil {
		t.Fatalf("sign HS256 token: %v", err)
	}
	const expected = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1MTYyMzkwMjIsIm5hbWUiOiJKb2huIERvZSIsInN1YiI6IjEyMzQ1Njc4OTAifQ.fdOPQ05ZfRhkST2-rIWgUpbqUsVhkkNVNcuG7Ki0s-8"
	if token != expected {
		t.Fatalf("HS256 token does not match the independent vector: %q", token)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("JWT part count = %d, want 3", len(parts))
	}
	mac := hmac.New(sha256.New, []byte("your-256-bit-secret"))
	_, _ = mac.Write([]byte(parts[0] + "." + parts[1]))
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || !hmac.Equal(signature, mac.Sum(nil)) {
		t.Fatal("JWT signature is not valid HS256")
	}
}

func TestStrictResponsesRejectUnknownAndDuplicateMembers(t *testing.T) {
	type nested struct {
		Name string `json:"name"`
	}
	type closed struct {
		Nested nested `json:"nested"`
	}
	for name, body := range map[string][]byte{
		"unknown top level": []byte(`{"nested":{"name":"ok"},"unknown":true}`),
		"unknown nested":    []byte(`{"nested":{"name":"ok","unknown":true}}`),
		"duplicate":         []byte(`{"nested":{"name":"first","name":"second"}}`),
	} {
		t.Run(name, func(t *testing.T) {
			var value closed
			if err := DecodeStrictResponse(body, &value); err == nil {
				t.Fatal("strict response accepted a non-closed member set")
			}
		})
	}
}

func TestNormalizationChangesOnlyDeclaredDynamicFields(t *testing.T) {
	expected := []byte(`{"request_id":"expected","opaque_value":"opaque-a","result":{"value":1}}`)
	observed := []byte(`{"result":{"value":1},"opaque_value":"opaque-a","request_id":"observed"}`)
	if err := CompareSemanticJSON(expected, observed, NormalizationSpec{DynamicFields: []string{"/request_id"}}); err != nil {
		t.Fatalf("compare declared dynamic field: %v", err)
	}
	changedOpaque := []byte(`{"request_id":"observed","opaque_value":"opaque-b","result":{"value":1}}`)
	if err := CompareSemanticJSON(expected, changedOpaque, NormalizationSpec{DynamicFields: []string{"/request_id"}}); !errors.Is(err, ErrSemanticMismatch) {
		t.Fatalf("opaque value comparison error = %v", err)
	}
	if _, err := NormalizeResponse(expected, NormalizationSpec{DynamicFields: []string{"/missing"}}); err == nil {
		t.Fatal("normalization accepted an absent declared field")
	}
}

func TestExactReplayUsesRawStatusRelevantHeadersAndCanonicalBody(t *testing.T) {
	first := Response{
		Status: http.StatusOK,
		Headers: http.Header{
			"Content-Type": []string{"application/json"},
			"Date":         []string{"first"},
		},
		Body: []byte(`{"b":2,"a":1}`),
	}
	replay := Response{
		Status: http.StatusOK,
		Headers: http.Header{
			"Content-Type": []string{"application/json"},
			"Date":         []string{"second"},
		},
		Body: []byte(`{"a":1,"b":2}`),
	}
	if err := CompareExactReplay(first, replay); err != nil {
		t.Fatalf("canonical replay comparison: %v", err)
	}
	replay.Status = http.StatusCreated
	if err := CompareExactReplay(first, replay); !errors.Is(err, ErrReplayMismatch) {
		t.Fatalf("changed status replay error = %v", err)
	}
	replay.Status = http.StatusOK
	replay.Headers.Set("Content-Type", "application/problem+json")
	if err := CompareExactReplay(first, replay); !errors.Is(err, ErrReplayMismatch) {
		t.Fatalf("changed relevant header replay error = %v", err)
	}
	replay.Headers.Set("Content-Type", "application/json")
	replay.Body = []byte(`{"a":1,"b":3}`)
	if err := CompareExactReplay(first, replay); !errors.Is(err, ErrReplayMismatch) {
		t.Fatalf("changed canonical body replay error = %v", err)
	}
}

func TestRecorderFailsClosedAtBoundsAndBeforeSensitiveStorage(t *testing.T) {
	root := filepath.Join(t.TempDir(), "bounded")
	recorder, err := NewRecorder(RecorderConfig{AttachmentRoot: root, MaxRecords: 1, MaxRawBodyBytes: 1024, MaxHeaderValues: 1, MaxHeaderValueBytes: 32})
	if err != nil {
		t.Fatalf("create bounded recorder: %v", err)
	}
	if _, err := recorder.recordExchange("push/submit", http.StatusOK, http.Header{"Content-Type": []string{"application/json"}}, 0, []byte(`{"request":1}`), []byte(`{"response":1}`), nil); err != nil {
		t.Fatalf("record bounded exchange: %v", err)
	}
	if _, err := recorder.recordExchange("push/submit", http.StatusOK, nil, 0, []byte(`{}`), []byte(`{}`), nil); !errors.Is(err, ErrRecorderBound) {
		t.Fatalf("metadata overflow error = %v", err)
	}

	sensitiveRoot := filepath.Join(t.TempDir(), "sensitive")
	sensitiveRecorder, err := NewRecorder(RecorderConfig{AttachmentRoot: sensitiveRoot, MaxRecords: 2, MaxRawBodyBytes: 1024})
	if err != nil {
		t.Fatalf("create sensitive recorder: %v", err)
	}
	secret := []byte("must-never-enter-an-attachment")
	if _, err := sensitiveRecorder.recordExchange("push/submit", http.StatusOK, nil, 0, append([]byte(`{"value":"`), append(secret, []byte(`"}`)...)...), nil, [][]byte{secret}); !errors.Is(err, ErrSensitiveRecording) {
		t.Fatalf("sensitive recording error = %v", err)
	}
	entries, err := os.ReadDir(sensitiveRoot)
	if err != nil {
		t.Fatalf("read sensitive attachment root: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("sensitive rejection created %d attachments", len(entries))
	}
}
