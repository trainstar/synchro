package syntheticproof

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/execution"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const pendingCycleScenarioPath = "conformance/scenarios/performance/pending-cycle-001.json"
const divergentPullScenarioPath = "conformance/scenarios/server/pull-divergent-checkpoints-001.json"

func TestRunnerAcceptsCompliantSystemAndRejectsSixSemanticDefects(t *testing.T) {
	tests := []struct {
		name      string
		path      string
		fault     SyntheticFault
		assertion AssertionName
		passed    bool
	}{
		{name: "compliant", path: pendingCycleScenarioPath, fault: SyntheticCompliant, passed: true},
		{name: "omitted mutation outcomes", path: pendingCycleScenarioPath, fault: SyntheticOmitMutation, assertion: AssertionMutationOutcomes},
		{name: "constant checksums", path: pendingCycleScenarioPath, fault: SyntheticConstantChecksum, assertion: AssertionChecksum},
		{name: "duplicate delivery", path: divergentPullScenarioPath, fault: SyntheticDuplicateDelivery, assertion: AssertionDeliveryUniqueness},
		{name: "wrong-scope rows", path: divergentPullScenarioPath, fault: SyntheticWrongScope, assertion: AssertionScopeBinding},
		{name: "replay corruption", path: pendingCycleScenarioPath, fault: SyntheticReplayCorruption, assertion: AssertionExactReplay},
		{name: "wrong status", path: pendingCycleScenarioPath, fault: SyntheticWrongStatus, assertion: AssertionRawStatus},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scenario := loadScenario(t, test.path)
			obligation := serverBlackBoxObligation(t, scenario)
			secret := []byte("task9-distinct-private-hs256-secret")
			provider, err := blackbox.NewHS256TokenProvider(secret, blackbox.Claims{"sub": "synthetic-user", "aud": "blackbox"})
			if err != nil {
				t.Fatalf("create token provider: %v", err)
			}
			token, err := provider.Token(context.Background())
			if err != nil {
				t.Fatalf("get test token: %v", err)
			}
			system, err := NewSyntheticSystem(context.Background(), scenario, SyntheticOptions{Fault: test.fault, ExpectedToken: token})
			if err != nil {
				t.Fatalf("start synthetic system: %v", err)
			}
			t.Cleanup(func() {
				if err := system.Close(); err != nil {
					t.Errorf("close synthetic system: %v", err)
				}
			})

			attachmentRoot := filepath.Join(t.TempDir(), "private-attachments")
			bindings := artifactBindingsFor(obligation)
			runner, err := NewRunner(RunnerConfig{
				Client: &blackbox.Client{BaseURL: system.BaseURL(), HTTP: &http.Client{}, Tokens: provider},
				Recorder: blackbox.RecorderConfig{
					AttachmentRoot:  attachmentRoot,
					MaxRecords:      64,
					MaxRawBodyBytes: 1 << 20,
				},
				ArtifactBindings: bindings,
			})
			if err != nil {
				t.Fatalf("create runner: %v", err)
			}
			result, runErr := runner.Run(context.Background(), scenario, obligation)
			if system.RequestCount() == 0 || len(result.Exchanges) == 0 || len(result.PrivateAttachmentIDs) == 0 {
				t.Fatalf("run did not use recorded loopback HTTP: requests=%d exchanges=%d attachments=%d", system.RequestCount(), len(result.Exchanges), len(result.PrivateAttachmentIDs))
			}

			if test.passed {
				if runErr != nil {
					t.Fatalf("compliant run failed: %v", runErr)
				}
				if !result.Passed || result.Failure.Kind != FailureNone || result.ExitCode != 0 || result.Result != execution.ResultPassed {
					t.Fatalf("compliant run result = %#v", result)
				}
				if system.FaultApplied() {
					t.Fatal("compliant system applied a fault")
				}
				requirePrivateContentAddressedAttachments(t, attachmentRoot, runner.recorder, result.PrivateAttachmentIDs)
				requireNoSensitiveRecording(t, runner.recorder, result, secret, token)
				return
			}

			if runErr == nil {
				t.Fatal("faulty synthetic system passed")
			}
			if result.Passed || result.Failure.Kind != FailureSemantic || result.Failure.Assertion != test.assertion || result.Result != execution.ResultFailed {
				t.Fatalf("fault result = %#v, want semantic assertion %q", result, test.assertion)
			}
			if !system.FaultApplied() {
				t.Fatal("faulty system did not apply its one semantic change")
			}
			failedChecks := 0
			for _, check := range result.Checks {
				if !check.Passed {
					failedChecks++
					if check.Name != test.assertion {
						t.Fatalf("failed check = %q, want %q", check.Name, test.assertion)
					}
				}
			}
			if failedChecks != 1 {
				t.Fatalf("failed semantic check count = %d, want 1", failedChecks)
			}
		})
	}
}

func loadScenario(t *testing.T, path string) scenarios.Scenario {
	t.Helper()
	scenario, err := scenarios.LoadFile(context.Background(), "../../..", path)
	if err != nil {
		t.Fatalf("load scenario: %v", err)
	}
	return scenario
}

func serverBlackBoxObligation(t *testing.T, scenario scenarios.Scenario) scenarios.ProofObligation {
	t.Helper()
	for _, obligation := range scenario.ProofObligations {
		if obligation.ProofType == "server-black-box" && obligation.MakeTarget == "test-blackbox" {
			return obligation
		}
	}
	t.Fatal("scenario has no server black-box obligation")
	return scenarios.ProofObligation{}
}

func artifactBindingsFor(obligation scenarios.ProofObligation) []execution.ArtifactBinding {
	result := make([]execution.ArtifactBinding, len(obligation.ArtifactInventoryIDs))
	for index, inventoryID := range obligation.ArtifactInventoryIDs {
		digest := sha256.Sum256([]byte(fmt.Sprintf("synthetic-artifact-%d", index)))
		result[index] = execution.ArtifactBinding{
			InventoryID: string(inventoryID),
			ArtifactID:  fmt.Sprintf("ART-SYNTHETIC-%03d", index+1),
			Path:        fmt.Sprintf("synthetic/artifact-%03d", index+1),
			Size:        int64(index + 1),
			SHA256:      hex.EncodeToString(digest[:]),
		}
	}
	return result
}

func requirePrivateContentAddressedAttachments(t *testing.T, root string, recorder *blackbox.Recorder, ids []string) {
	t.Helper()
	rootInfo, err := os.Stat(root)
	if err != nil {
		t.Fatalf("stat private attachment root: %v", err)
	}
	if rootInfo.Mode().Perm()&0o077 != 0 {
		t.Fatalf("attachment root permissions = %o", rootInfo.Mode().Perm())
	}
	for _, id := range ids {
		path, err := recorder.AttachmentPath(id)
		if err != nil {
			t.Fatalf("resolve attachment %q: %v", id, err)
		}
		info, err := os.Lstat(path)
		if err != nil {
			t.Fatalf("stat attachment %q: %v", id, err)
		}
		if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
			t.Fatalf("attachment %q mode = %v", id, info.Mode())
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read attachment %q: %v", id, err)
		}
		digest := sha256.Sum256(data)
		if id != "raw-body-sha256:"+hex.EncodeToString(digest[:]) {
			t.Fatalf("attachment %q is not content addressed", id)
		}
	}
}

func requireNoSensitiveRecording(t *testing.T, recorder *blackbox.Recorder, result RunResult, secret []byte, token string) {
	t.Helper()
	metadata, err := json.Marshal(result.Exchanges)
	if err != nil {
		t.Fatalf("marshal exchange metadata: %v", err)
	}
	secretDigest := sha256.Sum256(secret)
	tokenDigest := sha256.Sum256([]byte(token))
	for name, forbidden := range map[string][]byte{
		"secret": secret, "token": []byte(token),
		"secret digest": []byte(hex.EncodeToString(secretDigest[:])),
		"token digest":  []byte(hex.EncodeToString(tokenDigest[:])),
	} {
		if strings.Contains(string(metadata), string(forbidden)) {
			t.Fatalf("bounded metadata recorded %s", name)
		}
		for _, id := range result.PrivateAttachmentIDs {
			path, err := recorder.AttachmentPath(id)
			if err != nil {
				t.Fatalf("resolve attachment for secret scan: %v", err)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read attachment for secret scan: %v", err)
			}
			if strings.Contains(string(data), string(forbidden)) {
				t.Fatalf("raw-body attachment recorded %s", name)
			}
		}
	}
}
