package execution

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
)

func TestRunnerArtifactPayloadDigestRejectsChangedBytes(t *testing.T) {
	data := []byte("locked runner payload\n")
	digest := sha256.Sum256(data)
	_, err := RunnerArtifactPayloadDigest([]ArtifactPayload{{
		Binding: ArtifactBinding{
			InventoryID: "ARTDEF-CONFORMANCE-RUNNER-001",
			ArtifactID:  "ART-CONFORMANCE-RUNNER-001",
			Role:        "conformance-runner",
			Path:        "artifacts/runner.bin",
			MediaType:   "application/octet-stream",
			Size:        int64(len(data)),
			SHA256:      hex.EncodeToString(digest[:]),
		},
		Bytes: []byte("changed runner payload\n"),
	}})
	if !errors.Is(err, ErrInvalidIssuer) {
		t.Fatalf("RunnerArtifactPayloadDigest() error = %v, want %v", err, ErrInvalidIssuer)
	}
}
