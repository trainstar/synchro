package blackbox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestNativeControllerApplyRejectsWorkloadMacro(t *testing.T) {
	controller := &NativeController{harness: &Harness{}}
	operation := scenarios.Operation{
		ContractOperation: "workload",
		Name:              "prepare",
		Payload:           []byte(`{"profile":"scope_topology","scope_fanout":1,"impact_rows":1}`),
	}
	_, err := controller.ApplyStep(context.Background(), operation)
	if err == nil || !strings.Contains(err.Error(), "does not execute workload macros") {
		t.Fatalf("ApplyStep error = %v, want workload macro boundary error", err)
	}
}

func TestNativeControllerAssignmentRebindsPlaceholderScope(t *testing.T) {
	controller := &NativeController{installation: &nativeInstallationBinding{
		scopes:        map[string]string{"scope-a": "cf:global"},
		runtimeScopes: map[string]string{"cf:global": "scope-a"},
	}}
	operation := scenarios.Operation{
		ContractOperation: "model",
		Name:              "set-client-assignments",
		Payload:           json.RawMessage(`{"user_id":"user-a","client_id":"client-a","assignments":[{"scope_id":"scope-a"}]}`),
	}
	for range 2 {
		observation, err := controller.setClientAssignments(operation)
		if err != nil {
			t.Fatalf("set client assignments: %v", err)
		}
		if observation.Disposition != "success" {
			t.Fatalf("assignment disposition = %q", observation.Disposition)
		}
	}
	if got := controller.installation.scopes["scope-a"]; got != "user:user-a" {
		t.Fatalf("runtime scope = %q, want user:user-a", got)
	}
	if _, found := controller.installation.runtimeScopes["cf:global"]; found {
		t.Fatal("placeholder global scope binding remains")
	}
	if len(controller.installation.clients) != 1 || controller.installation.clients[0] != (nativeInstalledClient{UserID: "user-a", ClientID: "client-a"}) {
		t.Fatalf("installed clients = %#v", controller.installation.clients)
	}
}

func TestNativeArtifactStageRejectsUnsupportedOperationKey(t *testing.T) {
	artifact := &NativeArtifact{harness: &Harness{}}
	_, err := artifact.StageStep(context.Background(), scenarios.Operation{
		ContractOperation: "workload",
		Name:              "prepare",
		Payload:           []byte(`{"profile":"scope_topology","scope_fanout":1,"impact_rows":1}`),
	})
	if err == nil || !strings.Contains(err.Error(), `stage operation "workload/prepare" is unsupported`) {
		t.Fatalf("StageStep error = %v, want unsupported operation key", err)
	}
}

func TestNativeArtifactCloseRemovesOnlyUnchangedOwnedFiles(t *testing.T) {
	directory := t.TempDir()
	ownedPath := filepath.Join(directory, "owned.sqlite")
	unrelatedPath := filepath.Join(directory, "unrelated.sqlite")
	ownedData := []byte("owned portable seed")
	if err := os.WriteFile(ownedPath, ownedData, 0o600); err != nil {
		t.Fatalf("write owned artifact: %v", err)
	}
	if err := os.WriteFile(unrelatedPath, []byte("unrelated"), 0o600); err != nil {
		t.Fatalf("write unrelated artifact: %v", err)
	}
	digest := sha256.Sum256(ownedData)
	artifact := &NativeArtifact{
		harness:          &Harness{},
		stagingDirectory: directory,
		staged: map[string]*nativeStagedArtifact{
			"target": {path: ownedPath, sha256: hex.EncodeToString(digest[:])},
		},
	}
	if err := artifact.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Lstat(ownedPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("owned artifact remains after Close: %v", err)
	}
	if _, err := os.Lstat(unrelatedPath); err != nil {
		t.Fatalf("Close changed unrelated file: %v", err)
	}
}

func TestNativeArtifactCloseRefusesChangedOwnedFile(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "changed.sqlite")
	if err := os.WriteFile(path, []byte("changed"), 0o600); err != nil {
		t.Fatalf("write changed artifact: %v", err)
	}
	artifact := &NativeArtifact{
		harness:          &Harness{},
		stagingDirectory: directory,
		staged: map[string]*nativeStagedArtifact{
			"target": {path: path, sha256: strings.Repeat("0", 64)},
		},
	}
	if err := artifact.Close(context.Background()); err == nil || !strings.Contains(err.Error(), "refused a changed file") {
		t.Fatalf("Close error = %v, want changed-file refusal", err)
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("Close removed changed artifact: %v", err)
	}
}
