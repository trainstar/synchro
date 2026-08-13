package evidence

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/trainstar/synchro/conformance/execution"
	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
	"github.com/trainstar/synchro/conformance/internal/schemavalidator"
	"github.com/trainstar/synchro/conformance/scenarios"
)

// ValidateEvidence validates one evidence file. It does not claim full closure.
func ValidateEvidence(ctx context.Context, repoRoot, candidateRoot, path string) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	candidate, err := LoadCandidate(ctx, repoRoot, candidateRoot)
	if err != nil {
		return err
	}
	if !validCandidatePath(path) {
		return fmt.Errorf("%w: evidence path", ErrInvalidEvidence)
	}
	data, _, err := readLockedCandidateFile(candidate, path)
	if err != nil {
		return fmt.Errorf("%w: read evidence: %v", ErrInvalidEvidence, err)
	}
	validator := schemavalidator.New(repoRoot)
	defer validator.Close()
	if err := validator.ValidateBytes(ctx, evidenceSchema, data); err != nil {
		return fmt.Errorf("%w: evidence schema: %v", ErrInvalidEvidence, err)
	}
	if err := rejectDeclaredVerified(data); err != nil {
		return err
	}
	var evidence Evidence
	if err := jsonstrict.Decode(data, &evidence); err != nil {
		return fmt.Errorf("%w: decode evidence: %v", ErrInvalidEvidence, err)
	}
	return validateEvidence(ctx, repoRoot, candidate, evidence, path, nil)
}

func validateEvidence(ctx context.Context, repoRoot string, candidate Candidate, evidence Evidence, path string, manifest *FinalManifest) error {
	if err := verifyCandidateRoot(candidate); err != nil {
		return fmt.Errorf("%w: candidate root changed: %v", ErrInvalidEvidence, err)
	}
	if evidence.SchemaURI != "https://synchro.dev/conformance/schemas/evidence-v2.schema.json" || evidence.SchemaVersion != 2 || evidence.CandidateID != candidate.ID || evidence.ReleaseVersion != candidate.ReleaseVersion || evidence.ProtocolVersion != candidate.ProtocolVersion || evidence.ContractSnapshotSHA256 != candidate.ContractSnapshotSHA256 || evidence.SourceCommit != candidate.SourceCommit || evidence.ReceiptID == "" || evidence.RunnerDigest != candidate.RunnerDigest {
		return fmt.Errorf("%w: evidence candidate binding", ErrInvalidEvidence)
	}
	if evidence.Run.DurationMS != evidence.Run.CompletedAt.Sub(evidence.Run.StartedAt).Milliseconds() || evidence.Run.MakeTarget == "" || len(evidence.Run.Argv) != 2 || evidence.Run.Argv[0] != "make" || evidence.Run.Argv[1] != evidence.Run.MakeTarget {
		return fmt.Errorf("%w: evidence run projection", ErrInvalidEvidence)
	}
	if evidence.Receipt.Fields.ReceiptID != evidence.ReceiptID || evidence.Receipt.Fields.RunnerDigest != evidence.RunnerDigest || evidence.Receipt.Fields.CandidateLockSHA256 != candidate.LockSHA256 || evidence.Receipt.Fields.RunnerExecutableSHA256 == "" {
		return fmt.Errorf("%w: receipt projection", ErrInvalidEvidence)
	}
	if evidence.EvidenceID != evidenceID(evidence.Receipt.Fields) {
		return fmt.Errorf("%w: evidence identity", ErrInvalidEvidence)
	}
	receiptBytes, err := json.Marshal(struct {
		execution.ReceiptFields
		Authentication execution.ReceiptAuthentication `json:"authentication"`
	}{ReceiptFields: evidence.Receipt.Fields, Authentication: evidence.Receipt.Authentication})
	if err != nil {
		return fmt.Errorf("%w: encode receipt projection: %v", ErrInvalidEvidence, err)
	}
	receipt, err := execution.ParseReceipt(receiptBytes)
	if err != nil {
		return fmt.Errorf("%w: authenticated receipt: %v", ErrInvalidEvidence, err)
	}
	fields, err := receipt.Fields()
	if err != nil {
		return fmt.Errorf("%w: receipt fields: %v", ErrInvalidEvidence, err)
	}
	if err := compareEvidenceToReceipt(evidence, fields); err != nil {
		return err
	}
	if fields.RunnerDigest != evidence.RunnerDigest {
		return fmt.Errorf("%w: runner digest", ErrInvalidEvidence)
	}
	if err := validateEvidenceOuterProjection(ctx, repoRoot, candidate, evidence); err != nil {
		return err
	}
	if err := validateReceiptSemantics(ctx, repoRoot, candidate, fields); err != nil {
		return err
	}
	if err := validateEvidenceAttachments(candidate, evidence); err != nil {
		return err
	}
	if err := validateEvidenceLineage(candidate, evidence, manifest); err != nil {
		return err
	}
	return nil
}

func compareEvidenceToReceipt(evidence Evidence, fields execution.ReceiptFields) error {
	if evidence.ReceiptID != fields.ReceiptID || evidence.RunnerDigest != fields.RunnerDigest ||
		evidence.Generator.Name != fields.GeneratorName || evidence.Generator.Version != fields.GeneratorVersion || evidence.Generator.BinarySHA256 != fields.GeneratorBinarySHA256 ||
		evidence.ScenarioID != fields.ScenarioID || evidence.ProofObligationID != fields.ProofObligationID ||
		evidence.Run.ID != fields.RunID || evidence.Run.ExecutionLineageID != fields.ExecutionLineageID ||
		evidence.Run.URL != fields.RunURL || evidence.Run.MakeTarget != fields.MakeTarget ||
		!equalStrings(evidence.Run.Argv, fields.Argv) || evidence.Run.Attempt != fields.Attempt ||
		evidence.Run.StartedAt != fields.StartedAt || evidence.Run.CompletedAt != fields.CompletedAt ||
		evidence.Run.DurationMS != fields.CompletedAt.Sub(fields.StartedAt).Milliseconds() ||
		evidence.Run.ExitCode != fields.ExitCode || evidence.Run.Result != fields.Result ||
		!reflect.DeepEqual(evidence.Run.Command, fields.Command) ||
		!equalOptionalString(evidence.Run.PreviousEvidenceID, fields.PreviousEvidenceID) ||
		!equalOptionalString(evidence.Run.RerunCause, fields.RerunCause) ||
		!equalOptionalString(evidence.Run.RerunDiagnosis, fields.RerunDiagnosis) ||
		!equalOptionalString(evidence.Run.CorrectiveAction, fields.CorrectiveAction) ||
		!reflect.DeepEqual(evidence.Run.RerunApproval, fields.RerunApproval) ||
		!equalAssertionResults(evidence.Assertions, fields.Assertions) ||
		!equalAttachments(evidence.Attachments, fields.Attachments) ||
		!equalStrings(evidence.AttachmentIDs, fields.AttachmentIDs) ||
		!reflect.DeepEqual(evidence.ExecutionArtifacts, receiptExecutionArtifacts(fields)) ||
		!reflect.DeepEqual(evidence.Replay, receiptReplay(fields)) ||
		!reflect.DeepEqual(evidence.FaultExecution, fields.FaultExecution) ||
		!reflect.DeepEqual(evidence.PerformanceResults, fields.PerformanceResults) ||
		!reflect.DeepEqual(evidence.RequiredMeasurementResults, fields.RequiredMeasurements) ||
		!equalVectorResults(evidence.VectorResults, fields.VectorResults) ||
		!equalArtifactBindings(evidence.ArtifactBindings, fields.ArtifactBindings) ||
		!equalEnvironment(evidence.Environment, fields.EnvironmentDimensions) ||
		!equalHTTPObservations(evidence.HTTPObservations, fields.HTTPObservations) ||
		!equalCounters(evidence.Counters, fields.Counters) ||
		!equalObservations(evidence.Observations, fields.Observations) ||
		!reflect.DeepEqual(evidence.NegativeControl, fields.NegativeControl) ||
		!equalOptionalString(evidence.Seed, fields.Seed) {
		return fmt.Errorf("%w: evidence differs from receipt", ErrInvalidEvidence)
	}
	return nil
}

func receiptExecutionArtifacts(fields execution.ReceiptFields) execution.ExecutionArtifacts {
	if fields.ExecutionArtifacts == nil {
		return execution.ExecutionArtifacts{}
	}
	return *fields.ExecutionArtifacts
}

func receiptReplay(fields execution.ReceiptFields) execution.ReplayEvidence {
	if fields.Replay == nil {
		return execution.ReplayEvidence{}
	}
	return *fields.Replay
}

func validateEvidenceAttachments(candidate Candidate, evidence Evidence) error {
	if len(evidence.Attachments) != len(evidence.AttachmentIDs) {
		return fmt.Errorf("%w: attachment projection count", ErrInvalidEvidence)
	}
	store := candidateStore(candidate)
	seenPaths := make(map[string]struct{}, len(evidence.Attachments))
	seenIDs := make(map[string]struct{}, len(evidence.Attachments))
	for _, attachment := range evidence.Attachments {
		if _, duplicate := seenPaths[attachment.Path]; duplicate {
			return fmt.Errorf("%w: reused attachment path", ErrInvalidEvidence)
		}
		if _, duplicate := seenIDs[attachment.ID]; duplicate {
			return fmt.Errorf("%w: duplicate attachment ID", ErrInvalidEvidence)
		}
		seenPaths[attachment.Path] = struct{}{}
		seenIDs[attachment.ID] = struct{}{}
		if err := store.Verify(attachment); err != nil {
			return fmt.Errorf("%w: verify attachment %s: %v", ErrInvalidEvidence, attachment.ID, err)
		}
	}
	for _, id := range evidence.AttachmentIDs {
		if _, found := seenIDs[id]; !found {
			return fmt.Errorf("%w: attachment ID is unbound", ErrInvalidEvidence)
		}
	}
	return nil
}

func validateEvidenceOuterProjection(ctx context.Context, repoRoot string, candidate Candidate, evidence Evidence) error {
	scenario, err := loadCandidateScenario(ctx, repoRoot, candidate, evidence.ScenarioID)
	if err != nil {
		return err
	}
	obligation, found := obligationByID(scenario, evidence.ProofObligationID)
	if !found || obligation.ProofType != evidence.ProofType || !equalStrings(evidence.RequirementIDs, stringsForRequirements(obligation.RequirementIDs)) {
		return fmt.Errorf("%w: obligation binding", ErrInvalidEvidence)
	}
	if !sameOptionalSupport(evidence.SupportCellID, obligation.SupportCellID) {
		return fmt.Errorf("%w: support cell binding", ErrInvalidEvidence)
	}
	return nil
}

func loadCandidateScenario(ctx context.Context, repoRoot string, candidate Candidate, scenarioID string) (scenarios.Scenario, error) {
	locked, found := candidate.Scenarios[scenarioID]
	if !found {
		return scenarios.Scenario{}, fmt.Errorf("%w: scenario is not locked", ErrInvalidEvidence)
	}
	scenarioData, _, err := readLockedCandidateFile(candidate, locked.Path)
	if err != nil || sha256Hex(scenarioData) != locked.SHA256 {
		return scenarios.Scenario{}, fmt.Errorf("%w: locked scenario changed", ErrInvalidEvidence)
	}
	scenario, err := scenarios.LoadBytes(ctx, repoRoot, locked.Path, scenarioData)
	if err != nil {
		return scenarios.Scenario{}, fmt.Errorf("%w: load scenario: %v", ErrInvalidEvidence, err)
	}
	if string(scenario.ID) != scenarioID {
		return scenarios.Scenario{}, fmt.Errorf("%w: locked scenario identity", ErrInvalidEvidence)
	}
	return scenario, nil
}

func vectorResultKey(vectorSetID, language string) string {
	return vectorSetID + "\x00" + language
}

func validateEvidenceLineage(candidate Candidate, evidence Evidence, manifest *FinalManifest) error {
	if evidence.Run.Attempt < 1 || evidence.Run.ID == "" || evidence.Run.ExecutionLineageID == "" || evidence.Run.URL == "" {
		return fmt.Errorf("%w: run lineage", ErrInvalidEvidence)
	}
	if evidence.Run.Attempt == 1 {
		if evidence.Run.PreviousEvidenceID != nil || evidence.Run.RerunCause != nil || evidence.Run.RerunDiagnosis != nil || evidence.Run.CorrectiveAction != nil || evidence.Run.RerunApproval != nil {
			return fmt.Errorf("%w: initial run has rerun state", ErrInvalidEvidence)
		}
		return nil
	}
	if evidence.Run.PreviousEvidenceID == nil || evidence.Run.RerunCause == nil || evidence.Run.RerunDiagnosis == nil || evidence.Run.CorrectiveAction == nil || evidence.Run.RerunApproval == nil {
		return fmt.Errorf("%w: missing predecessor metadata", ErrInvalidEvidence)
	}
	if manifest == nil {
		return fmt.Errorf("%w: predecessor manifest is required", ErrInvalidEvidence)
	}
	if evidence.Run.RerunCause == nil || !allowedRerunCause(*evidence.Run.RerunCause) {
		return fmt.Errorf("%w: rerun cause", ErrInvalidEvidence)
	}
	if evidence.Run.RerunApproval == nil {
		return fmt.Errorf("%w: rerun approval", ErrInvalidEvidence)
	}
	if _, trusted := candidate.TrustedRerunApprovers[evidence.Run.RerunApproval.ApproverIdentity]; !trusted {
		return fmt.Errorf("%w: untrusted rerun approver", ErrInvalidEvidence)
	}
	return nil
}

func allowedRerunCause(value string) bool {
	switch value {
	case "ci-orchestrator-failure", "compute-host-failure", "network-infrastructure-outage", "device-or-simulator-failure", "artifact-registry-outage":
		return true
	default:
		return false
	}
}

func allPassedAssertions(values []execution.AssertionResult) bool {
	for _, value := range values {
		if value.Outcome != "passed" {
			return false
		}
	}
	return true
}

func allPassedVectors(values []execution.VectorResult) bool {
	for _, value := range values {
		if value.Outcome != "passed" || value.FailedCount != 0 || value.PassedCount != value.ExecutedCount {
			return false
		}
	}
	return true
}

func allPassedPerformance(budgets []execution.PerformanceResult, measurements []execution.RequiredMeasurementResult) bool {
	for _, value := range budgets {
		if value.Outcome != "passed" {
			return false
		}
	}
	for _, value := range measurements {
		if value.Outcome != "passed" {
			return false
		}
	}
	return true
}

func sameOptionalSupport(left *string, right *contract.SupportCellID) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == string(*right)
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func equalAssertionResults(left, right []execution.AssertionResult) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].AssertionID != right[index].AssertionID || left[index].Outcome != right[index].Outcome {
			return false
		}
	}
	return true
}

func equalAttachments(left []Attachment, right []execution.Attachment) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].ID != right[index].ID || left[index].Kind != right[index].Kind ||
			left[index].Path != right[index].Path || left[index].MediaType != right[index].MediaType ||
			left[index].SizeBytes != right[index].SizeBytes || left[index].SHA256 != right[index].SHA256 {
			return false
		}
	}
	return true
}

func equalArtifactBindings(left, right []execution.ArtifactBinding) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func equalEnvironment(left, right []execution.EnvironmentDimension) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func equalVectorResults(left, right []execution.VectorResult) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func equalHTTPObservations(left, right []execution.HTTPObservation) bool {
	return reflect.DeepEqual(left, right)
}

func equalCounters(value execution.Counters, expected *execution.Counters) bool {
	if expected == nil {
		return value == (execution.Counters{})
	}
	return value == *expected
}

func equalObservations(left, right []execution.Observation) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func equalOptionalString(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}
