package blackbox

import (
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/execution"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const pendingCycleScenarioPath = "conformance/scenarios/performance/pending-cycle-001.json"
const divergentPullScenarioPath = "conformance/scenarios/server/pull-divergent-checkpoints-001.json"

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
			provider, err := NewHS256TokenProvider(secret, Claims{"sub": "synthetic-user", "aud": "blackbox"})
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
			_, privateKey, err := ed25519.GenerateKey(nil)
			if err != nil {
				t.Fatalf("generate trusted runner key: %v", err)
			}
			trustedRunner, err := execution.NewTrustedRunner(privateKey)
			if err != nil {
				t.Fatalf("create trusted runner: %v", err)
			}
			runner, err := NewRunner(RunnerConfig{
				Client: &Client{BaseURL: system.BaseURL(), HTTP: &http.Client{}, Tokens: provider},
				Recorder: RecorderConfig{
					AttachmentRoot:  attachmentRoot,
					MaxRecords:      64,
					MaxRawBodyBytes: 1 << 20,
				},
				ArtifactBindings: bindings,
				EnvironmentDimensions: []execution.EnvironmentDimension{
					{Name: "postgresql", Value: "18-synthetic"},
				},
				TrustedRunner: trustedRunner,
			})
			if err != nil {
				t.Fatalf("create runner: %v", err)
			}
			issuer, err := runner.NewReceiptIssuer()
			if err != nil {
				t.Fatalf("create receipt issuer: %v", err)
			}
			if test.passed {
				proveIssuerCannotFabricateCompletion(t, issuer, scenario, obligation, bindings)
			}
			receipt, result, runErr := runner.Run(context.Background(), scenario, obligation, issuer)
			if err := receipt.Verify(); err != nil {
				t.Fatalf("verify completed receipt: %v", err)
			}
			fields, err := receipt.Fields()
			if err != nil {
				t.Fatalf("read completed receipt: %v", err)
			}
			if fields.ScenarioID != string(scenario.ID) || fields.ProofObligationID != string(obligation.ObligationID) || fields.MakeTarget != obligation.MakeTarget || !reflect.DeepEqual(fields.Argv, obligation.Argv) {
				t.Fatalf("receipt command binding = %#v", fields)
			}
			if fields.EvidenceClass != execution.EvidenceClassHarnessOnly {
				t.Fatalf("receipt evidence class = %q, want %q", fields.EvidenceClass, execution.EvidenceClassHarnessOnly)
			}
			if system.RequestCount() == 0 || len(result.Exchanges) == 0 || len(result.PrivateAttachmentIDs) == 0 {
				t.Fatalf("run did not use recorded loopback HTTP: requests=%d exchanges=%d attachments=%d", system.RequestCount(), len(result.Exchanges), len(result.PrivateAttachmentIDs))
			}

			if test.passed {
				if runErr != nil {
					t.Fatalf("compliant run failed: %v", runErr)
				}
				if !result.Passed || result.Failure.Kind != FailureNone || result.ExitCode != 0 || result.Result != execution.ResultPassed || fields.Result != execution.ResultPassed {
					t.Fatalf("compliant run result = %#v", result)
				}
				if system.FaultApplied() {
					t.Fatal("compliant system applied a fault")
				}
				requireReceiptFieldsExact(t, receipt)
				requirePrivateContentAddressedAttachments(t, runner.recorder, result.PrivateAttachmentIDs)
				requireNoSensitiveRecording(t, runner.recorder, result, receipt, secret, token)
				if err := receipt.VerifyAndConsume(); err != nil {
					t.Fatalf("consume receipt: %v", err)
				}
				if err := receipt.VerifyAndConsume(); !errors.Is(err, execution.ErrReceiptConsumed) {
					t.Fatalf("second receipt consumption error = %v", err)
				}
				if _, _, err := runner.Run(context.Background(), scenario, obligation, issuer); !errors.Is(err, ErrRunInput) {
					t.Fatalf("reused issuer error = %v", err)
				}
				return
			}

			if runErr == nil {
				t.Fatal("faulty synthetic system passed")
			}
			if result.Passed || result.Failure.Kind != FailureSemantic || result.Failure.Assertion != test.assertion || result.Result != execution.ResultFailed || fields.Result != execution.ResultFailed {
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

func loadPendingCycleScenario(t *testing.T) scenarios.Scenario {
	return loadScenario(t, pendingCycleScenarioPath)
}

func loadScenario(t *testing.T, path string) scenarios.Scenario {
	t.Helper()
	scenario, err := scenarios.LoadFile(context.Background(), "../..", path)
	if err != nil {
		t.Fatalf("load pending-cycle scenario: %v", err)
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
	t.Fatal("pending-cycle scenario has no server black-box obligation")
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

func proveIssuerCannotFabricateCompletion(t *testing.T, issuer execution.ReceiptIssuer, scenario scenarios.Scenario, obligation scenarios.ProofObligation, bindings []execution.ArtifactBinding) {
	t.Helper()
	now := time.Unix(1, 0).UTC()
	assertions := make([]execution.AssertionResult, len(obligation.AssertionIDs))
	for index, id := range obligation.AssertionIDs {
		assertions[index] = execution.AssertionResult{AssertionID: string(id), Outcome: "passed"}
	}
	completion, err := execution.PrepareCompletion(issuer, execution.ReceiptFields{
		EvidenceClass:     execution.EvidenceClassHarnessOnly,
		ScenarioID:        string(scenario.ID),
		ProofObligationID: string(obligation.ObligationID),
		MakeTarget:        obligation.MakeTarget,
		Argv:              append([]string(nil), obligation.Argv...),
		StartedAt:         now,
		CompletedAt:       now,
		ExitCode:          0,
		Result:            execution.ResultPassed,
		Assertions:        assertions,
		ArtifactBindings:  append([]execution.ArtifactBinding(nil), bindings...),
	})
	if err != nil {
		t.Fatalf("prepare attempted fabricated completion: %v", err)
	}
	if _, err := execution.CompleteReceipt(issuer, completion, make([]byte, 64)); !errors.Is(err, execution.ErrInvalidCompletion) {
		t.Fatalf("fabricated completion error = %v", err)
	}
	if issuer.Used() {
		t.Fatal("invalid signature consumed the receipt issuer")
	}
}

func requireReceiptFieldsExact(t *testing.T, receipt execution.Receipt) {
	t.Helper()
	encoded, err := json.Marshal(receipt)
	if err != nil {
		t.Fatalf("marshal receipt: %v", err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &object); err != nil {
		t.Fatalf("decode receipt fields: %v", err)
	}
	got := make([]string, 0, len(object))
	for name := range object {
		got = append(got, name)
	}
	sort.Strings(got)
	want := []string{
		"argv",
		"artifact_bindings",
		"assertions",
		"attachment_ids",
		"attachments",
		"attempt",
		"candidate_lock_sha256",
		"command_observation",
		"completed_at",
		"corrective_action",
		"counters",
		"evidence_class",
		"environment_dimensions",
		"execution_artifacts",
		"execution_lineage_id",
		"exit_code",
		"fault_execution",
		"generator_binary_sha256",
		"generator_name",
		"generator_version",
		"http_observations",
		"make_target",
		"negative_control",
		"observations",
		"performance_results",
		"previous_evidence_id",
		"proof_obligation_id",
		"receipt_id",
		"replay",
		"required_measurement_results",
		"rerun_approval",
		"rerun_cause",
		"rerun_diagnosis",
		"result",
		"run_id",
		"run_url",
		"runner_artifact_sha256",
		"runner_executable_sha256",
		"runner_digest",
		"scenario_id",
		"seed",
		"started_at",
		"vector_results",
	}
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("receipt fields = %v, want %v", got, want)
	}
	fields, err := receipt.Fields()
	if err != nil {
		t.Fatalf("get receipt fields: %v", err)
	}
	fields.Argv[0] = "changed"
	again, err := receipt.Fields()
	if err != nil {
		t.Fatalf("get receipt fields again: %v", err)
	}
	if again.Argv[0] != "make" {
		t.Fatal("receipt fields were mutable through a defensive copy")
	}
}

func requirePrivateContentAddressedAttachments(t *testing.T, recorder *Recorder, ids []string) {
	t.Helper()
	rootInfo, err := os.Stat(recorder.root)
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

func requireNoSensitiveRecording(t *testing.T, recorder *Recorder, result RunResult, receipt execution.Receipt, secret []byte, token string) {
	t.Helper()
	metadata, err := json.Marshal(result.Exchanges)
	if err != nil {
		t.Fatalf("marshal exchange metadata: %v", err)
	}
	receiptBytes, err := json.Marshal(receipt)
	if err != nil {
		t.Fatalf("marshal receipt for secret scan: %v", err)
	}
	secretDigest := sha256.Sum256(secret)
	tokenDigest := sha256.Sum256([]byte(token))
	for name, forbidden := range map[string][]byte{
		"secret":        secret,
		"token":         []byte(token),
		"secret digest": []byte(hex.EncodeToString(secretDigest[:])),
		"token digest":  []byte(hex.EncodeToString(tokenDigest[:])),
	} {
		if strings.Contains(string(metadata), string(forbidden)) || strings.Contains(string(receiptBytes), string(forbidden)) {
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
