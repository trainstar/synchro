package inventory

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/evidence"
	"github.com/trainstar/synchro/conformance/execution"
	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestGenerateAndRenderDeterministically(t *testing.T) {
	in := inventoryFixture(t, fixtureOptions{})
	report, err := Generate(context.Background(), in)
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if len(report.Rows) != 1 {
		t.Fatalf("row count = %d, want 1", len(report.Rows))
	}
	var firstJSON, secondJSON bytes.Buffer
	if err := WriteJSON(&firstJSON, report); err != nil {
		t.Fatalf("first WriteJSON() error = %v", err)
	}
	if err := WriteJSON(&secondJSON, report); err != nil {
		t.Fatalf("second WriteJSON() error = %v", err)
	}
	if firstJSON.String() != secondJSON.String() {
		t.Fatal("WriteJSON() produced non-deterministic output")
	}
	var loaded Report
	if err := json.Unmarshal(firstJSON.Bytes(), &loaded); err != nil {
		t.Fatalf("report JSON did not load: %v", err)
	}
	if err := json.Unmarshal([]byte(`{"schema_version":1,"candidate_id":"RC-TEST","protocol_version":3,"rows":[],"covered":true}`), &loaded); err == nil {
		t.Fatal("Report accepted unknown covered field")
	}
	var firstMarkdown, secondMarkdown bytes.Buffer
	if err := WriteMarkdown(&firstMarkdown, report); err != nil {
		t.Fatalf("first WriteMarkdown() error = %v", err)
	}
	if err := WriteMarkdown(&secondMarkdown, report); err != nil {
		t.Fatalf("second WriteMarkdown() error = %v", err)
	}
	if firstMarkdown.String() != secondMarkdown.String() {
		t.Fatal("WriteMarkdown() produced non-deterministic output")
	}
	if strings.Contains(strings.ToLower(firstMarkdown.String()), "ready") || strings.Contains(strings.ToLower(firstMarkdown.String()), "covered") {
		t.Fatal("Markdown contains a status claim")
	}
}

func TestGenerateRejectsFalseGreenMutants(t *testing.T) {
	tests := []struct {
		name    string
		options fixtureOptions
	}{
		{name: "omitted proof type", options: fixtureOptions{omitProof: true}},
		{name: "omitted support cell", options: fixtureOptions{omitSupport: true}},
		{name: "failed terminal evidence", options: fixtureOptions{failed: true}},
		{name: "hand-authored covered field", options: fixtureOptions{covered: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Generate(context.Background(), inventoryFixture(t, test.options))
			if err == nil {
				t.Fatal("Generate() accepted a false-green mutant")
			}
		})
	}
}

type fixtureOptions struct {
	omitProof   bool
	omitSupport bool
	failed      bool
	covered     bool
}

func inventoryFixture(t *testing.T, options fixtureOptions) Inputs {
	t.Helper()
	repoRoot := repositoryRoot(t)
	root := t.TempDir()
	_, err := contract.Load(context.Background(), repoRoot)
	if err != nil {
		t.Fatalf("contract.Load() error = %v", err)
	}
	loaded, err := scenarios.LoadFile(context.Background(), repoRoot, "conformance/scenarios/server/wal-order-001.json")
	if err != nil {
		t.Fatalf("scenarios.LoadFile() error = %v", err)
	}
	scenario := loaded
	obligation := loaded.ProofObligations[0]
	scenario.ProofObligations = []scenarios.ProofObligation{obligation}
	scenario.Ownership = nil
	for _, owner := range loaded.Ownership {
		if owner.ProofObligationID == obligation.ObligationID {
			scenario.Ownership = append(scenario.Ownership, owner)
		}
	}
	requirement := contract.Requirement{ID: loaded.RequirementIDs[0], RequiredProofTypes: []string{obligation.ProofType}, ApplicableComponents: []string{"conformance-runner"}}
	bundle := &contract.Bundle{Requirements: contract.Requirements{Requirements: []contract.Requirement{requirement}}}
	if options.omitSupport {
		scenario.Ownership[0].SupportCellID = contractSupportValue("SUP-PG-018")
	}
	if options.omitProof {
		scenario.ProofObligations = nil
		scenario.Ownership = nil
	}
	artifacts := make(map[string]evidence.LockedArtifact)
	artifactsByInventory := make(map[string]evidence.LockedArtifact)
	artifactPath := "artifacts/runner.bin"
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable() error = %v", err)
	}
	artifactBytes, err := os.ReadFile(executable)
	if err != nil {
		t.Fatalf("read runner artifact: %v", err)
	}
	writeFixtureFile(t, filepath.Join(root, artifactPath), artifactBytes)
	artifactSHA := digest(artifactBytes)
	runner := evidence.LockedArtifact{ID: "ART-CONFORMANCE-RUNNER-001", InventoryID: "ARTDEF-CONFORMANCE-RUNNER-001", Role: "conformance-runner", Payloads: []evidence.LockedPayload{{Path: artifactPath, MediaType: "application/octet-stream", SizeBytes: int64(len(artifactBytes)), SHA256: artifactSHA}}}
	artifacts[runner.ID] = runner
	artifactsByInventory[runner.InventoryID] = runner
	_, private, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	trustedRunner, err := execution.NewTrustedRunner(private)
	if err != nil {
		t.Fatalf("NewTrustedRunner: %v", err)
	}
	scenarioPath := "conformance/scenarios/server/wal-order-001.json"
	scenarioBytes, err := os.ReadFile(filepath.Join(repoRoot, filepath.FromSlash(scenarioPath)))
	if err != nil {
		t.Fatalf("ReadFile scenario: %v", err)
	}
	writeFixtureFile(t, filepath.Join(root, filepath.FromSlash(scenarioPath)), scenarioBytes)
	candidate := evidence.Candidate{RepoRoot: repoRoot, Root: root, LockSHA256: digest([]byte("candidate lock")), RunnerDigest: trustedRunner.RunnerDigest(), ID: "RC-TEST", ReleaseVersion: "0.3.0", ProtocolVersion: 3, SourceCommit: repositoryCommit(t, repoRoot), ContractSnapshotSHA256: digest([]byte("contract")), Scenarios: map[string]evidence.LockedScenario{string(scenario.ID): {ID: string(scenario.ID), Path: scenarioPath, SHA256: digest(scenarioBytes)}}, Artifacts: artifacts, ArtifactsByInventoryID: artifactsByInventory, TrustedRerunApprovers: map[string]struct{}{"github:release-operator": {}}}
	candidate, err = evidence.BindCandidateRoot(candidate)
	if err != nil {
		t.Fatalf("BindCandidateRoot() error = %v", err)
	}
	item := signedEvidence(t, candidate, trustedRunner, scenario, obligation, artifactPath, artifactSHA, int64(len(artifactBytes)), options.failed)
	evidenceDir := filepath.Join(root, "evidence")
	data, err := json.Marshal(item)
	if err != nil {
		t.Fatalf("Marshal evidence: %v", err)
	}
	if options.covered {
		var document map[string]any
		if err := json.Unmarshal(data, &document); err != nil {
			t.Fatalf("decode evidence: %v", err)
		}
		document["covered"] = true
		data, err = json.Marshal(document)
		if err != nil {
			t.Fatalf("Marshal covered evidence: %v", err)
		}
	}
	writeFixtureFile(t, filepath.Join(evidenceDir, "attempt-001.json"), data)
	return Inputs{Contract: bundle, Scenarios: []scenarios.Scenario{scenario}, EvidenceRoot: "evidence", Candidate: candidate}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("filepath.Abs() error = %v", err)
	}
	return root
}

func signedEvidence(t *testing.T, candidate evidence.Candidate, runner execution.TrustedRunner, scenario scenarios.Scenario, obligation scenarios.ProofObligation, artifactPath, artifactSHA string, artifactSize int64, failed bool) evidence.Evidence {
	t.Helper()
	digestValue, err := execution.RunnerArtifactDigest([]execution.ArtifactBinding{{InventoryID: "ARTDEF-CONFORMANCE-RUNNER-001", ArtifactID: "ART-CONFORMANCE-RUNNER-001", Role: "conformance-runner", Path: artifactPath, MediaType: "application/octet-stream", Size: artifactSize, SHA256: artifactSHA}})
	if err != nil {
		t.Fatalf("RunnerArtifactDigest: %v", err)
	}
	authorization, err := runner.AuthorizeExecutable(digestValue, artifactSHA)
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	generator := execution.GeneratorIdentity{Name: "inventory-fixture", Version: "1.0.0", BinarySHA256: digest([]byte("inventory-fixture"))}
	issuer, err := execution.NewReceiptIssuerFromAuthorizationAndGeneratorAndCandidateLock(authorization, generator, candidate.LockSHA256)
	if err != nil {
		t.Fatalf("NewReceiptIssuerFromAuthorizationAndGeneratorAndCandidateLock: %v", err)
	}
	start := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	result := execution.ResultPassed
	exitCode := 0
	assertionOutcome := "passed"
	if failed {
		result = execution.ResultFailed
		exitCode = 1
		assertionOutcome = "failed"
	}
	attachments := inventoryRunAttachments(t, candidate.Root, scenario)
	snapshotSHA256, err := execution.SourceSnapshotSHA256(context.Background(), candidate.RepoRoot, candidate.SourceCommit)
	if err != nil {
		t.Fatalf("SourceSnapshotSHA256: %v", err)
	}
	fields := execution.ReceiptFields{ScenarioID: string(scenario.ID), ProofObligationID: string(obligation.ObligationID), MakeTarget: obligation.MakeTarget, Argv: append([]string(nil), obligation.Argv...), StartedAt: start, CompletedAt: start.Add(time.Second), ExitCode: exitCode, Result: result, Assertions: []execution.AssertionResult{{AssertionID: string(obligation.AssertionIDs[0]), Outcome: assertionOutcome}}, ArtifactBindings: []execution.ArtifactBinding{{InventoryID: "ARTDEF-CONFORMANCE-RUNNER-001", ArtifactID: "ART-CONFORMANCE-RUNNER-001", Role: "conformance-runner", Path: artifactPath, MediaType: "application/octet-stream", Size: artifactSize, SHA256: artifactSHA}}, AttachmentIDs: []string{}, RunID: "RUN-TEST-001", ExecutionLineageID: "EXEC-TEST-001", RunURL: "https://example.test/runs/1", Attempt: 1, Attachments: attachments, ExecutionArtifacts: &execution.ExecutionArtifacts{}, Replay: &execution.ReplayEvidence{}}
	fields.Command = execution.CommandObservation{Argv: append([]string(nil), fields.Argv...), ExitCode: exitCode, StartedAt: start, CompletedAt: start.Add(time.Second), MakeExecutableSHA256: digest([]byte("make")), SourceSnapshotSHA256: snapshotSHA256}
	for _, attachment := range attachments {
		fields.AttachmentIDs = append(fields.AttachmentIDs, attachment.ID)
		switch attachment.Kind {
		case "log":
			fields.ExecutionArtifacts.LogAttachmentIDs = append(fields.ExecutionArtifacts.LogAttachmentIDs, attachment.ID)
		case "trace":
			fields.ExecutionArtifacts.TraceAttachmentIDs = append(fields.ExecutionArtifacts.TraceAttachmentIDs, attachment.ID)
		case "replay-data":
			fields.ExecutionArtifacts.ReplayDataAttachmentIDs = append(fields.ExecutionArtifacts.ReplayDataAttachmentIDs, attachment.ID)
		case "barrier-trace":
			fields.ExecutionArtifacts.BarrierTraceAttachmentIDs = append(fields.ExecutionArtifacts.BarrierTraceAttachmentIDs, attachment.ID)
		}
	}
	for index, barrier := range scenario.BarrierPlan.Barriers {
		fields.Replay.BarrierTraces = append(fields.Replay.BarrierTraces, execution.BarrierTrace{BarrierID: string(barrier.ID), AttachmentID: fields.ExecutionArtifacts.BarrierTraceAttachmentIDs[index]})
	}
	completion, err := execution.PrepareCompletion(issuer, fields)
	if err != nil {
		t.Fatalf("PrepareCompletion: %v", err)
	}
	receipt, err := runner.CompleteReceipt(issuer, completion)
	if err != nil {
		t.Fatalf("CompleteReceipt: %v", err)
	}
	stored, err := receipt.Fields()
	if err != nil {
		t.Fatalf("receipt Fields: %v", err)
	}
	authentication, err := receipt.Authentication()
	if err != nil {
		t.Fatalf("receipt Authentication: %v", err)
	}
	evidenceAttachments := make([]evidence.Attachment, len(stored.Attachments))
	for index, attachment := range stored.Attachments {
		evidenceAttachments[index] = evidence.Attachment{ID: attachment.ID, Kind: attachment.Kind, Path: attachment.Path, MediaType: attachment.MediaType, SizeBytes: attachment.SizeBytes, SHA256: attachment.SHA256}
	}
	requirementIDs := make([]string, len(obligation.RequirementIDs))
	for index, id := range obligation.RequirementIDs {
		requirementIDs[index] = string(id)
	}
	return evidence.Evidence{SchemaURI: "https://synchro.dev/conformance/schemas/evidence-v2.schema.json", SchemaVersion: 2, EvidenceID: evidenceID(stored.ReceiptID), ReceiptID: stored.ReceiptID, CandidateID: candidate.ID, ReleaseVersion: candidate.ReleaseVersion, ProtocolVersion: candidate.ProtocolVersion, ContractSnapshotSHA256: candidate.ContractSnapshotSHA256, ScenarioID: string(scenario.ID), ProofObligationID: string(obligation.ObligationID), RequirementIDs: requirementIDs, ProofType: obligation.ProofType, SourceCommit: candidate.SourceCommit, Generator: evidence.Generator{Name: stored.GeneratorName, Version: stored.GeneratorVersion, BinarySHA256: stored.GeneratorBinarySHA256}, Run: evidence.Run{ID: stored.RunID, ExecutionLineageID: stored.ExecutionLineageID, URL: stored.RunURL, MakeTarget: stored.MakeTarget, Argv: stored.Argv, Attempt: stored.Attempt, StartedAt: stored.StartedAt, CompletedAt: stored.CompletedAt, DurationMS: stored.CompletedAt.Sub(stored.StartedAt).Milliseconds(), Result: stored.Result, ExitCode: stored.ExitCode, Command: stored.Command}, Environment: stored.EnvironmentDimensions, Assertions: stored.Assertions, Attachments: evidenceAttachments, AttachmentIDs: stored.AttachmentIDs, ExecutionArtifacts: *stored.ExecutionArtifacts, Replay: *stored.Replay, FaultExecution: stored.FaultExecution, PerformanceResults: stored.PerformanceResults, RequiredMeasurementResults: stored.RequiredMeasurements, VectorResults: stored.VectorResults, ArtifactBindings: stored.ArtifactBindings, HTTPObservations: stored.HTTPObservations, Observations: stored.Observations, NegativeControl: stored.NegativeControl, Seed: stored.Seed, RunnerDigest: stored.RunnerDigest, Receipt: evidence.ReceiptProjection{Fields: stored, Authentication: authentication}}
}

func inventoryRunAttachments(t *testing.T, root string, scenario scenarios.Scenario) []execution.Attachment {
	t.Helper()
	inputs := []struct {
		kind string
		data []byte
	}{
		{kind: "log", data: []byte("execution log\n")},
		{kind: "trace", data: []byte("execution trace\n")},
		{kind: "replay-data", data: []byte("replay data\n")},
	}
	for _, barrier := range scenario.BarrierPlan.Barriers {
		inputs = append(inputs, struct {
			kind string
			data []byte
		}{kind: "barrier-trace", data: []byte("barrier " + barrier.ID + "\n")})
	}
	result := make([]execution.Attachment, 0, len(inputs))
	store := evidence.Store{Root: root}
	for _, input := range inputs {
		stored, err := store.Put(input.kind, "text/plain", input.data)
		if err != nil {
			t.Fatalf("attachment Put: %v", err)
		}
		result = append(result, execution.Attachment{ID: stored.ID, Kind: stored.Kind, Path: stored.Path, MediaType: stored.MediaType, SizeBytes: stored.SizeBytes, SHA256: stored.SHA256})
	}
	return result
}

func evidenceID(receiptID string) string {
	value := sha256.Sum256([]byte(receiptID))
	return "EVD-" + strings.ToUpper(hex.EncodeToString(value[:8])) + "-001"
}

func repositoryCommit(t *testing.T, root string) string {
	t.Helper()
	command := exec.Command("git", "rev-parse", "HEAD")
	command.Dir = root
	output, err := command.Output()
	if err != nil {
		t.Fatalf("git rev-parse HEAD error = %v", err)
	}
	return strings.TrimSpace(string(output))
}

func contractSupportValue(value contract.SupportCellID) *contract.SupportCellID {
	return &value
}

func writeFixtureFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

func digest(data []byte) string {
	value := sha256.Sum256(data)
	return hex.EncodeToString(value[:])
}

func stringValue(value string) *string {
	return &value
}
