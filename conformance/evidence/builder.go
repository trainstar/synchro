package evidence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/trainstar/synchro/conformance/execution"
	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
	"github.com/trainstar/synchro/conformance/scenarios"
	"github.com/trainstar/synchro/conformance/vectors"
)

// NewBuilder loads the immutable candidate lock before any execution begins.
func NewBuilder(cfg BuilderConfig) (*Builder, error) {
	if cfg.RepoRoot == "" || cfg.CandidateRoot == "" || cfg.RunnerAuthorization.RunnerDigest() == "" {
		return nil, fmt.Errorf("%w: builder configuration", ErrInvalidCandidate)
	}
	candidate, err := LoadCandidate(context.Background(), cfg.RepoRoot, cfg.CandidateRoot)
	if err != nil {
		return nil, err
	}
	if cfg.RunnerAuthorization.RunnerDigest() != candidate.RunnerDigest {
		return nil, fmt.Errorf("%w: runner authorization digest", ErrInvalidCandidate)
	}
	if cfg.Generator.Name == "" || cfg.Generator.Version == "" || !validSHA256(cfg.Generator.BinarySHA256) {
		return nil, fmt.Errorf("%w: evidence generator", ErrInvalidEvidence)
	}
	makefile, err := readBoundMakefile(context.Background(), candidate)
	if err != nil {
		return nil, err
	}
	commandCapability, err := execution.NewCommandCapability(candidate.RepoRoot, candidate.SourceCommit, makefile)
	if err != nil {
		return nil, fmt.Errorf("%w: create command capability: %v", ErrInvalidCandidate, err)
	}
	runnerPayloads, err := lockedRunnerArtifactPayloads(candidate)
	if err != nil {
		return nil, err
	}
	runnerArtifactSHA256, err := execution.RunnerArtifactPayloadDigest(runnerPayloads)
	if err != nil {
		return nil, fmt.Errorf("%w: runner artifact digest: %v", ErrInvalidCandidate, err)
	}
	if cfg.RunnerAuthorization.RunnerArtifactSHA256() != runnerArtifactSHA256 {
		return nil, fmt.Errorf("%w: runner authorization artifact digest", ErrInvalidCandidate)
	}
	runnerExecutableSHA256, err := lockedRunnerExecutableSHA256(candidate)
	if err != nil || cfg.RunnerAuthorization.RunnerExecutableSHA256() != runnerExecutableSHA256 {
		return nil, fmt.Errorf("%w: runner authorization executable digest", ErrInvalidCandidate)
	}
	issuer, err := execution.NewReceiptIssuerFromAuthorizationAndGeneratorAndCandidateLockAndCommandCapability(cfg.RunnerAuthorization, execution.GeneratorIdentity{
		Name: cfg.Generator.Name, Version: cfg.Generator.Version, BinarySHA256: cfg.Generator.BinarySHA256,
	}, candidate.LockSHA256, commandCapability)
	if err != nil {
		return nil, fmt.Errorf("%w: create receipt issuer: %v", ErrInvalidEvidence, err)
	}
	return &Builder{cfg: cloneBuilderConfig(cfg), candidate: candidate, issuer: issuer, commandCapability: commandCapability, consumed: make(map[string]struct{})}, nil
}

// ReceiptIssuer returns the builder-owned, single-use runner completion issuer.
func (b *Builder) ReceiptIssuer() execution.ReceiptIssuer {
	if b == nil {
		return execution.ReceiptIssuer{}
	}
	return b.issuer
}

// CommandCapability returns the builder-owned process launcher for its issuer.
func (b *Builder) CommandCapability() execution.CommandCapability {
	if b == nil {
		return execution.CommandCapability{}
	}
	return b.commandCapability
}

// RunnerArtifactBindings returns copies of the locked conformance-runner payload
// bindings required to complete this builder's receipt issuer.
func (b *Builder) RunnerArtifactBindings() ([]execution.ArtifactBinding, error) {
	if b == nil {
		return nil, fmt.Errorf("%w: builder is nil", ErrInvalidEvidence)
	}
	bindings, err := lockedRunnerArtifactBindings(b.candidate)
	if err != nil {
		return nil, err
	}
	return evidenceSlice(bindings), nil
}

// ScenarioBinding returns one exact scenario identity for the trusted runner.
func (b *Builder) ScenarioBinding(scenarioID string) (string, string, error) {
	if b == nil {
		return "", "", fmt.Errorf("%w: builder is nil", ErrInvalidEvidence)
	}
	locked, found := b.candidate.Scenarios[scenarioID]
	if !found {
		return "", "", fmt.Errorf("%w: scenario is not locked", ErrInvalidEvidence)
	}
	return locked.ID, locked.SHA256, nil
}

// AttachmentPublisher returns the runner's narrow candidate-confined attachment capability.
func (b *Builder) AttachmentPublisher() execution.AttachmentPublisher {
	if b == nil {
		return nil
	}
	return runnerAttachmentPublisher{store: candidateStore(b.candidate)}
}

type runnerAttachmentPublisher struct {
	store Store
}

func (p runnerAttachmentPublisher) Publish(kind, mediaType string, data []byte) (execution.Attachment, error) {
	return p.store.Publish(kind, mediaType, data)
}

// Build accepts only one completed authenticated receipt from the issuer.
func (b *Builder) Build(ctx context.Context, receipt execution.Receipt) (Evidence, error) {
	if err := contextError(ctx); err != nil {
		return Evidence{}, err
	}
	if b == nil {
		return Evidence{}, fmt.Errorf("%w: builder is nil", ErrInvalidEvidence)
	}
	if !b.issuer.Used() || receipt.Verify() != nil || !receipt.IssuedBy(b.issuer) {
		return Evidence{}, fmt.Errorf("%w: completed runner receipt is required", ErrInvalidEvidence)
	}
	fields, err := receipt.Fields()
	if err != nil {
		return Evidence{}, fmt.Errorf("%w: receipt fields: %v", ErrInvalidEvidence, err)
	}
	if fields.RunnerDigest != b.candidate.RunnerDigest || fields.CandidateLockSHA256 != b.candidate.LockSHA256 || fields.RunnerArtifactSHA256 != b.issuer.RunnerArtifactSHA256() || fields.RunnerExecutableSHA256 != b.issuer.RunnerExecutableSHA256() {
		return Evidence{}, fmt.Errorf("%w: receipt runner is foreign", ErrInvalidEvidence)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.consumed[fields.ReceiptID]; exists {
		return Evidence{}, fmt.Errorf("%w: receipt replay", ErrInvalidEvidence)
	}
	if err := receipt.VerifyAndConsume(); err != nil {
		return Evidence{}, fmt.Errorf("%w: receipt consume: %v", ErrInvalidEvidence, err)
	}
	if err := b.validateReceipt(ctx, fields); err != nil {
		return Evidence{}, err
	}
	evidence, err := b.projectEvidence(receipt, fields)
	if err != nil {
		return Evidence{}, err
	}
	if err := validateEvidenceShape(evidence); err != nil {
		return Evidence{}, err
	}
	b.consumed[fields.ReceiptID] = struct{}{}
	return evidence, nil
}

func (b *Builder) validateReceipt(ctx context.Context, fields execution.ReceiptFields) error {
	if err := verifyRepositorySource(ctx, b.candidate.RepoRoot, b.candidate.SourceCommit); err != nil {
		return fmt.Errorf("%w: source changed after execution: %v", ErrInvalidEvidence, err)
	}
	if err := verifyCandidateFiles(ctx, b.candidate); err != nil {
		return fmt.Errorf("%w: candidate changed after execution: %v", ErrInvalidEvidence, err)
	}
	return validateReceiptSemantics(ctx, b.cfg.RepoRoot, b.candidate, fields)
}

// validateReceiptSemantics validates authenticated execution against the immutable candidate and contract.
// It does not require a Builder or receipt issuer.
func validateReceiptSemantics(ctx context.Context, repoRoot string, candidate Candidate, fields execution.ReceiptFields) error {
	if err := verifyCandidateRoot(candidate); err != nil {
		return fmt.Errorf("%w: candidate root changed: %v", ErrInvalidEvidence, err)
	}
	if err := validateReceiptAuthority(candidate, fields); err != nil {
		return err
	}
	bundle, err := contract.Load(ctx, repoRoot)
	if err != nil {
		return fmt.Errorf("%w: load contract: %v", ErrInvalidEvidence, err)
	}
	if _, locked := candidate.Scenarios[fields.ScenarioID]; !locked {
		return fmt.Errorf("%w: unknown locked scenario", ErrInvalidEvidence)
	}
	scenario, err := loadCandidateScenario(ctx, repoRoot, candidate, fields.ScenarioID)
	if err != nil {
		return fmt.Errorf("%w: load locked scenario: %v", ErrInvalidEvidence, err)
	}
	if scenario.ID != contract.ScenarioID(fields.ScenarioID) {
		return fmt.Errorf("%w: scenario identity", ErrInvalidEvidence)
	}
	obligation, found := obligationByID(scenario, fields.ProofObligationID)
	if !found {
		return fmt.Errorf("%w: unknown proof obligation", ErrInvalidEvidence)
	}
	if fields.MakeTarget != obligation.MakeTarget || !equalStrings(fields.Argv, obligation.Argv) {
		return fmt.Errorf("%w: command does not match obligation", ErrInvalidEvidence)
	}
	if err := validateReceiptAssertions(fields, obligation); err != nil {
		return err
	}
	if err := validateReceiptOwnership(scenario, obligation, fields); err != nil {
		return err
	}
	if err := validateArtifactBindings(candidate, fields.ArtifactBindings, obligation); err != nil {
		return err
	}
	if err := validateEnvironment(candidate, fields.EnvironmentDimensions, obligation); err != nil {
		return err
	}
	if err := validateVectors(ctx, repoRoot, candidate, fields.VectorResults, obligation, fields.Attachments); err != nil {
		return err
	}
	if err := validateReceiptAttachments(fields); err != nil {
		return err
	}
	store := candidateStore(candidate)
	for _, attachment := range fields.Attachments {
		if err := store.Verify(Attachment{
			ID: attachment.ID, Kind: attachment.Kind, Path: attachment.Path,
			MediaType: attachment.MediaType, SizeBytes: attachment.SizeBytes, SHA256: attachment.SHA256,
		}); err != nil {
			return fmt.Errorf("%w: verify receipt attachment: %v", ErrInvalidEvidence, err)
		}
	}
	if err := validateReceiptReplay(fields, scenario); err != nil {
		return err
	}
	if err := validateReceiptOutcome(fields, obligation); err != nil {
		return err
	}
	if err := validateReceiptMeasurements(fields, obligation, bundle); err != nil {
		return err
	}
	if err := validateReceiptFaults(fields, scenario, obligation, bundle); err != nil {
		return err
	}
	return nil
}

func validateReceiptAuthority(candidate Candidate, fields execution.ReceiptFields) error {
	payloads, err := lockedRunnerArtifactPayloads(candidate)
	if err != nil {
		return err
	}
	digest, err := execution.RunnerArtifactPayloadDigest(payloads)
	if err != nil {
		return fmt.Errorf("%w: runner artifact digest: %v", ErrInvalidEvidence, err)
	}
	snapshotSHA256, snapshotErr := execution.SourceSnapshotSHA256(context.Background(), candidate.RepoRoot, candidate.SourceCommit)
	runnerExecutableSHA256, executableErr := lockedRunnerExecutableSHA256(candidate)
	if fields.RunnerDigest != candidate.RunnerDigest || fields.CandidateLockSHA256 != candidate.LockSHA256 || fields.RunnerArtifactSHA256 != digest || fields.RunnerExecutableSHA256 != runnerExecutableSHA256 || executableErr != nil || fields.GeneratorName == "" || fields.GeneratorVersion == "" || !validSHA256(fields.GeneratorBinarySHA256) || snapshotErr != nil || fields.Command.SourceSnapshotSHA256 != snapshotSHA256 {
		return fmt.Errorf("%w: receipt authority binding", ErrInvalidEvidence)
	}
	return nil
}

func lockedRunnerExecutableSHA256(candidate Candidate) (string, error) {
	artifact, found := candidate.ArtifactsByInventoryID["ARTDEF-CONFORMANCE-RUNNER-001"]
	if !found || artifact.Role != "conformance-runner" || len(artifact.Payloads) != 1 {
		return "", fmt.Errorf("%w: locked runner executable", ErrInvalidCandidate)
	}
	payload := artifact.Payloads[0]
	data, _, err := readLockedCandidateFile(candidate, payload.Path)
	if err != nil || int64(len(data)) != payload.SizeBytes || sha256Hex(data) != payload.SHA256 {
		return "", fmt.Errorf("%w: locked runner executable", ErrInvalidCandidate)
	}
	return payload.SHA256, nil
}

func lockedRunnerArtifactBindings(candidate Candidate) ([]execution.ArtifactBinding, error) {
	payloads, err := lockedRunnerArtifactPayloads(candidate)
	if err != nil {
		return nil, err
	}
	result := make([]execution.ArtifactBinding, len(payloads))
	for index, payload := range payloads {
		result[index] = payload.Binding
	}
	return result, nil
}

func lockedRunnerArtifactPayloads(candidate Candidate) ([]execution.ArtifactPayload, error) {
	var result []execution.ArtifactPayload
	for _, artifact := range candidate.Artifacts {
		if artifact.Role != "conformance-runner" {
			continue
		}
		for _, payload := range artifact.Payloads {
			data, _, err := readLockedCandidateFile(candidate, payload.Path)
			if err != nil || int64(len(data)) != payload.SizeBytes || sha256Hex(data) != payload.SHA256 {
				return nil, fmt.Errorf("%w: locked conformance runner payload", ErrInvalidCandidate)
			}
			result = append(result, execution.ArtifactPayload{
				Binding: execution.ArtifactBinding{
					InventoryID: artifact.InventoryID,
					ArtifactID:  artifact.ID,
					Role:        artifact.Role,
					Path:        payload.Path,
					MediaType:   payload.MediaType,
					Size:        int64(len(data)),
					SHA256:      sha256Hex(data),
				},
				Bytes: data,
			})
		}
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("%w: locked conformance runner artifact", ErrInvalidCandidate)
	}
	return result, nil
}

func candidateStore(candidate Candidate) Store {
	return Store{Root: candidate.Root, rootIdentity: candidate.rootIdentity}
}

func validateReceiptOwnership(scenario scenarios.Scenario, obligation scenarios.ProofObligation, fields execution.ReceiptFields) error {
	expected := make(map[string]struct{}, len(obligation.AssertionIDs))
	for _, row := range scenario.Ownership {
		if row.ProofObligationID != obligation.ObligationID {
			continue
		}
		if row.ScenarioID != scenario.ID || row.ProofType != obligation.ProofType || !sameOptionalSupportID(row.SupportCellID, obligation.SupportCellID) {
			return fmt.Errorf("%w: ownership binding", ErrInvalidEvidence)
		}
		expected[string(row.AssertionID)] = struct{}{}
	}
	if len(expected) != len(fields.Assertions) {
		return fmt.Errorf("%w: omitted ownership tuple", ErrInvalidEvidence)
	}
	for _, assertion := range fields.Assertions {
		if _, found := expected[assertion.AssertionID]; !found {
			return fmt.Errorf("%w: assertion ownership", ErrInvalidEvidence)
		}
		delete(expected, assertion.AssertionID)
	}
	if len(expected) != 0 {
		return fmt.Errorf("%w: omitted ownership tuple", ErrInvalidEvidence)
	}
	return nil
}

func sameOptionalSupportID(left, right *contract.SupportCellID) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func validateArtifactBindings(candidate Candidate, bindings []execution.ArtifactBinding, obligation scenarios.ProofObligation) error {
	expected := make(map[string]LockedArtifact, len(obligation.ArtifactInventoryIDs))
	expectedPayloadCount := 0
	for _, inventoryID := range obligation.ArtifactInventoryIDs {
		id := string(inventoryID)
		artifact, found := candidate.ArtifactsByInventoryID[id]
		if !found {
			return fmt.Errorf("%w: missing locked artifact", ErrInvalidEvidence)
		}
		expected[id] = artifact
		expectedPayloadCount += len(artifact.Payloads)
	}
	if len(bindings) != expectedPayloadCount {
		return fmt.Errorf("%w: artifact payload binding count", ErrInvalidEvidence)
	}
	seenPayloads := make(map[string]map[string]struct{}, len(expected))
	seenPaths := make(map[string]string, len(bindings))
	for _, binding := range bindings {
		locked, found := expected[binding.InventoryID]
		if !found {
			return fmt.Errorf("%w: unknown or unbound artifact", ErrInvalidEvidence)
		}
		if binding.ArtifactID != locked.ID || binding.Role != locked.Role {
			return fmt.Errorf("%w: artifact ID or role mismatch", ErrInvalidEvidence)
		}
		payload, found := exactPayload(locked.Payloads, binding.Path, binding.MediaType, binding.Size, binding.SizeBytes, binding.SHA256)
		if !found || payload.Path == "" {
			return fmt.Errorf("%w: artifact payload mismatch", ErrInvalidEvidence)
		}
		if owner, reused := seenPaths[binding.Path]; reused && owner != binding.InventoryID {
			return fmt.Errorf("%w: artifact payload path is reused", ErrInvalidEvidence)
		}
		inventoryPayloads := seenPayloads[binding.InventoryID]
		if inventoryPayloads == nil {
			inventoryPayloads = make(map[string]struct{}, len(locked.Payloads))
			seenPayloads[binding.InventoryID] = inventoryPayloads
		}
		if _, duplicate := inventoryPayloads[payload.Path]; duplicate {
			return fmt.Errorf("%w: duplicate artifact payload", ErrInvalidEvidence)
		}
		inventoryPayloads[payload.Path] = struct{}{}
		seenPaths[binding.Path] = binding.InventoryID
	}
	for inventoryID, artifact := range expected {
		payloads := seenPayloads[inventoryID]
		if len(payloads) != len(artifact.Payloads) {
			return fmt.Errorf("%w: missing artifact payload", ErrInvalidEvidence)
		}
		for _, payload := range artifact.Payloads {
			if _, found := payloads[payload.Path]; !found {
				return fmt.Errorf("%w: missing artifact payload", ErrInvalidEvidence)
			}
		}
	}
	return nil
}

func validateEnvironment(candidate Candidate, dimensions []execution.EnvironmentDimension, obligation scenarios.ProofObligation) error {
	if obligation.SupportCellID == nil {
		if len(dimensions) != 0 {
			return fmt.Errorf("%w: support-neutral obligation has environment", ErrInvalidEvidence)
		}
		return nil
	}
	locked, found := candidate.SupportCells[string(*obligation.SupportCellID)]
	if !found || len(dimensions) != len(locked.Dimensions) {
		return fmt.Errorf("%w: support environment", ErrInvalidEvidence)
	}
	seen := make(map[string]struct{}, len(dimensions))
	for _, dimension := range dimensions {
		if dimension.Name == "" || dimension.Value == "" || containsSecretText(dimension.Name) || containsSecretText(dimension.Value) {
			return fmt.Errorf("%w: unsafe environment dimension", ErrInvalidEvidence)
		}
		if expected, exists := locked.Dimensions[dimension.Name]; !exists || expected != dimension.Value {
			return fmt.Errorf("%w: unallowlisted environment dimension", ErrInvalidEvidence)
		}
		if _, exists := seen[dimension.Name]; exists {
			return fmt.Errorf("%w: duplicate environment dimension", ErrInvalidEvidence)
		}
		seen[dimension.Name] = struct{}{}
	}
	return nil
}

func validateVectors(ctx context.Context, repoRoot string, candidate Candidate, results []execution.VectorResult, obligation scenarios.ProofObligation, attachments []execution.Attachment) error {
	if len(results) == 0 && len(obligation.RequiredVectorSetIDs) == 0 {
		return nil
	}
	catalog, err := vectors.Load(ctx, repoRoot)
	if err != nil {
		return fmt.Errorf("%w: load vector catalog: %v", ErrInvalidEvidence, err)
	}
	required := make(map[string]struct{}, len(obligation.RequiredVectorSetIDs))
	for _, id := range obligation.RequiredVectorSetIDs {
		required[string(id)] = struct{}{}
	}
	seen := make(map[string]struct{}, len(results))
	for _, result := range results {
		set, found := catalog.Set(contract.VectorSetID(result.VectorSetID))
		if !found || result.SourceSHA256 != set.SourceSHA256 || result.AggregateSHA256 != set.AggregateSHA256 || result.Outcome != "passed" || result.ExecutedCount < 1 || result.PassedCount != result.ExecutedCount || result.FailedCount != 0 || result.ResultAttachmentID == "" || !attachmentHasKind(attachments, result.ResultAttachmentID, "vector-results") {
			return fmt.Errorf("%w: vector result", ErrInvalidEvidence)
		}
		if _, required := required[result.VectorSetID]; !required {
			return fmt.Errorf("%w: unexpected vector set", ErrInvalidEvidence)
		}
		key := result.VectorSetID + "\x00" + result.Language
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("%w: duplicate vector language result", ErrInvalidEvidence)
		}
		artifact, found := candidate.Artifacts[result.ArtifactID]
		if !found || !validVectorArtifactRole(result.Language, artifact.Role) {
			return fmt.Errorf("%w: vector artifact role", ErrInvalidEvidence)
		}
		seen[key] = struct{}{}
	}
	for id := range required {
		found := false
		for _, result := range results {
			if result.VectorSetID == id {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("%w: missing vector result", ErrInvalidEvidence)
		}
	}
	return nil
}

func (b *Builder) projectEvidence(receipt execution.Receipt, fields execution.ReceiptFields) (Evidence, error) {
	if _, locked := b.candidate.Scenarios[fields.ScenarioID]; !locked {
		return Evidence{}, fmt.Errorf("%w: unknown locked scenario", ErrInvalidEvidence)
	}
	scenario, err := loadCandidateScenario(context.Background(), b.cfg.RepoRoot, b.candidate, fields.ScenarioID)
	if err != nil {
		return Evidence{}, err
	}
	obligation, _ := obligationByID(scenario, fields.ProofObligationID)
	attachmentIDs := evidenceSlice(fields.AttachmentIDs)
	attachments := make([]Attachment, len(fields.Attachments))
	for index, attachment := range fields.Attachments {
		attachments[index] = Attachment{ID: attachment.ID, Kind: attachment.Kind, Path: attachment.Path, MediaType: attachment.MediaType, SizeBytes: attachment.SizeBytes, SHA256: attachment.SHA256}
	}
	supportCell := cloneSupportCell(obligation.SupportCellID)
	artifacts := execution.ExecutionArtifacts{}
	if fields.ExecutionArtifacts != nil {
		artifacts = *fields.ExecutionArtifacts
	}
	replay := execution.ReplayEvidence{}
	if fields.Replay != nil {
		replay = *fields.Replay
	}
	counters := execution.Counters{}
	if fields.Counters != nil {
		counters = *fields.Counters
	}
	authentication, err := receipt.Authentication()
	if err != nil {
		return Evidence{}, err
	}
	evidence := Evidence{
		SchemaURI:                  "https://synchro.dev/conformance/schemas/evidence-v2.schema.json",
		SchemaVersion:              2,
		EvidenceID:                 evidenceID(fields),
		ReceiptID:                  fields.ReceiptID,
		CandidateID:                b.candidate.ID,
		ReleaseVersion:             b.candidate.ReleaseVersion,
		ProtocolVersion:            b.candidate.ProtocolVersion,
		ContractSnapshotSHA256:     b.candidate.ContractSnapshotSHA256,
		SupportCellID:              supportCell,
		ScenarioID:                 fields.ScenarioID,
		ProofObligationID:          fields.ProofObligationID,
		RequirementIDs:             stringsForRequirements(obligation.RequirementIDs),
		ProofType:                  obligation.ProofType,
		SourceCommit:               b.candidate.SourceCommit,
		Generator:                  Generator{Name: fields.GeneratorName, Version: fields.GeneratorVersion, BinarySHA256: fields.GeneratorBinarySHA256},
		Run:                        projectRun(fields),
		Environment:                evidenceSlice(fields.EnvironmentDimensions),
		Assertions:                 evidenceSlice(fields.Assertions),
		Attachments:                attachments,
		AttachmentIDs:              attachmentIDs,
		ExecutionArtifacts:         artifacts,
		Replay:                     replay,
		FaultExecution:             fields.FaultExecution,
		PerformanceResults:         evidenceSlice(fields.PerformanceResults),
		RequiredMeasurementResults: evidenceSlice(fields.RequiredMeasurements),
		VectorResults:              evidenceSlice(fields.VectorResults),
		ArtifactBindings:           evidenceSlice(fields.ArtifactBindings),
		HTTPObservations:           evidenceSlice(fields.HTTPObservations),
		Counters:                   counters,
		Observations:               evidenceSlice(fields.Observations),
		NegativeControl:            fields.NegativeControl,
		Seed:                       fields.Seed,
		RunnerDigest:               fields.RunnerDigest,
		Receipt:                    ReceiptProjection{Fields: fields, Authentication: authentication},
	}
	return evidence, nil
}

func evidenceSlice[T any](source []T) []T {
	result := make([]T, len(source))
	copy(result, source)
	return result
}

func validateReceiptAssertions(fields execution.ReceiptFields, obligation scenarios.ProofObligation) error {
	if len(fields.Assertions) != len(obligation.AssertionIDs) {
		return fmt.Errorf("%w: assertion count", ErrInvalidEvidence)
	}
	expected := make(map[string]struct{}, len(obligation.AssertionIDs))
	for _, id := range obligation.AssertionIDs {
		expected[string(id)] = struct{}{}
	}
	for _, assertion := range fields.Assertions {
		if _, found := expected[assertion.AssertionID]; !found {
			return fmt.Errorf("%w: unrelated assertion", ErrInvalidEvidence)
		}
		delete(expected, assertion.AssertionID)
	}
	if len(expected) != 0 {
		return fmt.Errorf("%w: missing assertion", ErrInvalidEvidence)
	}
	return nil
}

func validateReceiptAttachments(fields execution.ReceiptFields) error {
	if len(fields.Attachments) != len(fields.AttachmentIDs) {
		return fmt.Errorf("%w: receipt attachments", ErrInvalidEvidence)
	}
	ids := make(map[string]struct{}, len(fields.AttachmentIDs))
	paths := make(map[string]struct{}, len(fields.Attachments))
	for _, attachment := range fields.Attachments {
		if attachment.ID == "" || attachment.Kind == "" || !validCandidatePath(attachment.Path) || attachment.MediaType == "" || attachment.SizeBytes < 0 || !validSHA256(attachment.SHA256) {
			return fmt.Errorf("%w: attachment fields", ErrInvalidEvidence)
		}
		if containsSecretText(attachment.Path) || containsSecretText(attachment.ID) || containsSecretText(attachment.SHA256) {
			return fmt.Errorf("%w: unsafe attachment projection", ErrInvalidEvidence)
		}
		if _, duplicate := paths[attachment.Path]; duplicate {
			return fmt.Errorf("%w: duplicate attachment path", ErrInvalidEvidence)
		}
		paths[attachment.Path] = struct{}{}
		ids[attachment.ID] = struct{}{}
	}
	for _, id := range fields.AttachmentIDs {
		if _, found := ids[id]; !found {
			return fmt.Errorf("%w: unbound attachment ID", ErrInvalidEvidence)
		}
	}
	return nil
}

func validateReceiptReplay(fields execution.ReceiptFields, scenario scenarios.Scenario) error {
	if fields.ExecutionArtifacts == nil || fields.Replay == nil {
		return fmt.Errorf("%w: execution artifacts and replay are required", ErrInvalidEvidence)
	}
	attachments := attachmentKindsByID(fields.Attachments)
	if !validateExecutionAttachmentGroup(fields.ExecutionArtifacts.LogAttachmentIDs, attachments, "log") ||
		!validateExecutionAttachmentGroup(fields.ExecutionArtifacts.TraceAttachmentIDs, attachments, "trace") ||
		!validateExecutionAttachmentGroup(fields.ExecutionArtifacts.ReplayDataAttachmentIDs, attachments, "replay-data") ||
		!validateExecutionAttachmentGroup(fields.ExecutionArtifacts.BarrierTraceAttachmentIDs, attachments, "barrier-trace") {
		return fmt.Errorf("%w: execution artifact attachment kind", ErrInvalidEvidence)
	}
	if err := validateBarrierReplay(fields.ExecutionArtifacts, *fields.Replay, scenario, attachments); err != nil {
		return err
	}
	if scenario.Replay.SeedRequired {
		if fields.Seed == nil || *fields.Seed == "" || fields.Replay.Seed == nil || *fields.Replay.Seed == "" {
			return fmt.Errorf("%w: required replay seed", ErrInvalidEvidence)
		}
	} else if fields.Seed != nil || fields.Replay.Seed != nil {
		return fmt.Errorf("%w: unexpected replay seed", ErrInvalidEvidence)
	}
	if !equalOptionalString(fields.Seed, fields.Replay.Seed) {
		return fmt.Errorf("%w: replay seed mismatch", ErrInvalidEvidence)
	}
	return nil
}

func validateExecutionAttachmentGroup(ids []string, attachments map[string]string, kind string) bool {
	if len(ids) == 0 || !uniqueStringSet(ids) {
		return false
	}
	for _, id := range ids {
		if attachments[id] != kind {
			return false
		}
	}
	return true
}

func validateBarrierReplay(artifacts *execution.ExecutionArtifacts, replay execution.ReplayEvidence, scenario scenarios.Scenario, attachments map[string]string) error {
	if !uniqueStringSet(artifacts.BarrierTraceAttachmentIDs) {
		return fmt.Errorf("%w: duplicate barrier trace attachment", ErrInvalidEvidence)
	}
	if len(replay.BarrierTraces) != len(scenario.BarrierPlan.Barriers) || len(artifacts.BarrierTraceAttachmentIDs) != len(scenario.BarrierPlan.Barriers) {
		return fmt.Errorf("%w: barrier trace count", ErrInvalidEvidence)
	}
	expected := make(map[string]struct{}, len(scenario.BarrierPlan.Barriers))
	for _, barrier := range scenario.BarrierPlan.Barriers {
		expected[string(barrier.ID)] = struct{}{}
	}
	seenBarriers := make(map[string]struct{}, len(replay.BarrierTraces))
	seenAttachments := make(map[string]struct{}, len(replay.BarrierTraces))
	listedAttachments := stringSet(artifacts.BarrierTraceAttachmentIDs)
	for _, trace := range replay.BarrierTraces {
		if _, found := expected[trace.BarrierID]; !found {
			return fmt.Errorf("%w: unknown barrier trace", ErrInvalidEvidence)
		}
		if _, duplicate := seenBarriers[trace.BarrierID]; duplicate {
			return fmt.Errorf("%w: duplicate barrier trace", ErrInvalidEvidence)
		}
		if _, duplicate := seenAttachments[trace.AttachmentID]; duplicate || attachments[trace.AttachmentID] != "barrier-trace" {
			return fmt.Errorf("%w: barrier trace attachment", ErrInvalidEvidence)
		}
		if _, listed := listedAttachments[trace.AttachmentID]; !listed {
			return fmt.Errorf("%w: missing barrier trace execution binding", ErrInvalidEvidence)
		}
		seenBarriers[trace.BarrierID] = struct{}{}
		seenAttachments[trace.AttachmentID] = struct{}{}
	}
	if len(seenBarriers) != len(expected) || len(seenAttachments) != len(listedAttachments) {
		return fmt.Errorf("%w: incomplete barrier trace binding", ErrInvalidEvidence)
	}
	if scenario.Replay.BarrierTraceRequired && len(scenario.BarrierPlan.Barriers) > 0 && len(replay.BarrierTraces) == 0 {
		return fmt.Errorf("%w: required barrier trace", ErrInvalidEvidence)
	}
	return nil
}

func attachmentKindsByID(attachments []execution.Attachment) map[string]string {
	result := make(map[string]string, len(attachments))
	for _, attachment := range attachments {
		result[attachment.ID] = attachment.Kind
	}
	return result
}

func attachmentHasKind(attachments []execution.Attachment, id, kind string) bool {
	for _, attachment := range attachments {
		if attachment.ID == id {
			return attachment.Kind == kind
		}
	}
	return false
}

func validateReceiptOutcome(fields execution.ReceiptFields, obligation scenarios.ProofObligation) error {
	if fields.ExitCode == 0 {
		if fields.Result != execution.ResultPassed {
			return fmt.Errorf("%w: zero exit result", ErrInvalidEvidence)
		}
		for _, assertion := range fields.Assertions {
			if assertion.Outcome != "passed" {
				return fmt.Errorf("%w: fabricated passed assertion", ErrInvalidEvidence)
			}
		}
		for _, vector := range fields.VectorResults {
			if vector.Outcome != "passed" {
				return fmt.Errorf("%w: fabricated passed vector", ErrInvalidEvidence)
			}
		}
		for _, result := range fields.PerformanceResults {
			if result.Outcome != "passed" {
				return fmt.Errorf("%w: failed performance result", ErrInvalidEvidence)
			}
		}
		for _, result := range fields.RequiredMeasurements {
			if result.Outcome != "passed" {
				return fmt.Errorf("%w: failed required measurement", ErrInvalidEvidence)
			}
		}
	}
	if obligation.ProofType == "negative-control" && fields.NegativeControl == nil {
		return fmt.Errorf("%w: missing negative control result", ErrInvalidEvidence)
	}
	return nil
}

func validateReceiptMeasurements(fields execution.ReceiptFields, obligation scenarios.ProofObligation, bundle *contract.Bundle) error {
	if bundle == nil {
		return fmt.Errorf("%w: contract bundle", ErrInvalidEvidence)
	}
	attachments := attachmentKindsByID(fields.Attachments)
	budgets := make(map[string]contract.PerformanceBudget, len(obligation.PerformanceBudgetIDs))
	for _, id := range obligation.PerformanceBudgetIDs {
		budget, found := performanceBudgetByID(bundle, string(id))
		if !found {
			return fmt.Errorf("%w: unknown performance budget", ErrInvalidEvidence)
		}
		budgets[string(id)] = budget
	}
	if len(fields.PerformanceResults) != len(budgets) {
		return fmt.Errorf("%w: performance result count", ErrInvalidEvidence)
	}
	for _, result := range fields.PerformanceResults {
		budget, found := budgets[result.BudgetID]
		if !found || !containsAttachment(fields.AttachmentIDs, result.MeasurementAttachmentID) || attachments[result.MeasurementAttachmentID] != "performance-measurements" {
			return fmt.Errorf("%w: performance binding", ErrInvalidEvidence)
		}
		if err := validatePerformanceResult(result, budget); err != nil {
			return err
		}
		delete(budgets, result.BudgetID)
	}
	if len(budgets) != 0 {
		return fmt.Errorf("%w: missing performance result", ErrInvalidEvidence)
	}
	measurements := make(map[string]contract.RequiredMeasurement, len(obligation.RequiredMeasurementIDs))
	for _, id := range obligation.RequiredMeasurementIDs {
		measurement, found := requiredMeasurementByID(bundle, string(id))
		if !found {
			return fmt.Errorf("%w: unknown required measurement", ErrInvalidEvidence)
		}
		measurements[string(id)] = measurement
	}
	if len(fields.RequiredMeasurements) != len(measurements) {
		return fmt.Errorf("%w: required measurement count", ErrInvalidEvidence)
	}
	for _, result := range fields.RequiredMeasurements {
		measurement, found := measurements[result.MeasurementID]
		if !found || !containsAttachment(fields.AttachmentIDs, result.MeasurementAttachmentID) || attachments[result.MeasurementAttachmentID] != "performance-measurements" {
			return fmt.Errorf("%w: required measurement binding", ErrInvalidEvidence)
		}
		if err := validateRequiredMeasurementResult(result, measurement); err != nil {
			return err
		}
		delete(measurements, result.MeasurementID)
	}
	if len(measurements) != 0 {
		return fmt.Errorf("%w: missing required measurement", ErrInvalidEvidence)
	}
	return nil
}

func performanceBudgetByID(bundle *contract.Bundle, id string) (contract.PerformanceBudget, bool) {
	if bundle == nil {
		return contract.PerformanceBudget{}, false
	}
	for _, budget := range bundle.Performance.Budgets {
		if string(budget.ID) == id {
			return budget, true
		}
	}
	return contract.PerformanceBudget{}, false
}

func requiredMeasurementByID(bundle *contract.Bundle, id string) (contract.RequiredMeasurement, bool) {
	if bundle == nil {
		return contract.RequiredMeasurement{}, false
	}
	for _, measurement := range bundle.Performance.RequiredMeasurements {
		if string(measurement.ID) == id {
			return measurement, true
		}
	}
	return contract.RequiredMeasurement{}, false
}

func validatePerformanceResult(result execution.PerformanceResult, budget contract.PerformanceBudget) error {
	if result.Metric != budget.Metric || result.Unit != budget.Unit || result.Comparator != budget.Comparator ||
		!semanticJSONEqual(result.DataProfile.Parameters, budget.DataProfile.Parameters) ||
		result.DataProfile.ProfileType != budget.DataProfile.ProfileType ||
		result.MeasurementMethod != (execution.MeasurementMethod{
			MethodType: budget.MeasurementMethod.MethodType, Instrumentation: budget.MeasurementMethod.Instrumentation, Aggregation: budget.MeasurementMethod.Aggregation,
		}) {
		return fmt.Errorf("%w: performance metadata", ErrInvalidEvidence)
	}
	limit, err := exactNumber(budget.Limit.String())
	if err != nil {
		return fmt.Errorf("%w: performance limit: %v", ErrInvalidEvidence, err)
	}
	observed, err := exactFloat(result.ObservedValue)
	if err != nil {
		return fmt.Errorf("%w: performance observed value: %v", ErrInvalidEvidence, err)
	}
	resultLimit, limitErr := exactFloat(result.Limit)
	if limitErr != nil || resultLimit.Cmp(limit) != 0 {
		return fmt.Errorf("%w: performance limit", ErrInvalidEvidence)
	}
	derived := observedPerformanceValue(result.Metric, result.Measurement)
	derivedNumber, err := exactFloat(derived)
	if err != nil || derivedNumber.Cmp(observed) != 0 {
		return fmt.Errorf("%w: performance observed value is not derived", ErrInvalidEvidence)
	}
	passed := compareNumbers(observed, limit, result.Comparator)
	if (result.Outcome == "passed" && !passed) || (result.Outcome == "failed" && passed) {
		return fmt.Errorf("%w: performance outcome", ErrInvalidEvidence)
	}
	if result.Outcome != "passed" && result.Outcome != "failed" {
		return fmt.Errorf("%w: performance outcome", ErrInvalidEvidence)
	}
	return nil
}

func validateRequiredMeasurementResult(result execution.RequiredMeasurementResult, measurement contract.RequiredMeasurement) error {
	if result.DataProfile.ProfileType != measurement.DataProfile.ProfileType ||
		!semanticJSONEqual(result.DataProfile.Parameters, measurement.DataProfile.Parameters) ||
		result.MeasurementMethod != (execution.MeasurementMethod{
			MethodType: measurement.MeasurementMethod.MethodType, Instrumentation: measurement.MeasurementMethod.Instrumentation, Aggregation: measurement.MeasurementMethod.Aggregation,
		}) || result.Outcome != "passed" {
		return fmt.Errorf("%w: required measurement metadata", ErrInvalidEvidence)
	}
	expectedMetrics := make(map[string]contract.PerformanceMetric, len(measurement.Metrics))
	for _, metric := range measurement.Metrics {
		expectedMetrics[string(metric.ID)] = metric
	}
	if len(result.Metrics) != len(expectedMetrics) {
		return fmt.Errorf("%w: required measurement metric set", ErrInvalidEvidence)
	}
	for _, metric := range result.Metrics {
		expected, found := expectedMetrics[metric.ID]
		if !found || metric.Name != expected.Name || metric.Unit != expected.Unit {
			return fmt.Errorf("%w: required measurement metric", ErrInvalidEvidence)
		}
		delete(expectedMetrics, metric.ID)
	}
	if len(expectedMetrics) != 0 {
		return fmt.Errorf("%w: missing required measurement metric", ErrInvalidEvidence)
	}
	expectedStrata := make(map[string]contract.PerformanceStratum, len(measurement.Strata))
	for _, stratum := range measurement.Strata {
		expectedStrata[string(stratum.StratumID)] = stratum
	}
	if len(result.Strata) != len(expectedStrata) {
		return fmt.Errorf("%w: required measurement stratum set", ErrInvalidEvidence)
	}
	minimum, err := exactNumber(measurement.MinimumSampleCountPerStratum.String())
	if err != nil || minimum.Denom().Cmp(big.NewInt(1)) != 0 || minimum.Sign() <= 0 {
		return fmt.Errorf("%w: required measurement minimum sample count", ErrInvalidEvidence)
	}
	minimumInt := minimum.Num().Int64()
	for _, stratum := range result.Strata {
		authored, found := expectedStrata[stratum.StratumID]
		if !found || !semanticJSONEqual(stratum.Parameters, authored.Parameters) || int64(stratum.SampleCount) < minimumInt || stratum.SampleCount != len(stratum.Observations) {
			return fmt.Errorf("%w: required measurement stratum", ErrInvalidEvidence)
		}
		delete(expectedStrata, stratum.StratumID)
		seenSamples := make(map[string]struct{}, len(stratum.Observations))
		for _, observation := range stratum.Observations {
			if observation.SampleID == "" {
				return fmt.Errorf("%w: required measurement sample", ErrInvalidEvidence)
			}
			if _, duplicate := seenSamples[observation.SampleID]; duplicate {
				return fmt.Errorf("%w: duplicate required measurement sample", ErrInvalidEvidence)
			}
			seenSamples[observation.SampleID] = struct{}{}
			if len(observation.MetricValues) != len(expectedMetricsForMeasurement(measurement.Metrics)) {
				return fmt.Errorf("%w: required measurement metric values", ErrInvalidEvidence)
			}
			seenMetrics := make(map[string]struct{}, len(observation.MetricValues))
			for _, value := range observation.MetricValues {
				if _, expected := expectedMetricsForMeasurement(measurement.Metrics)[value.MetricID]; !expected || !finiteNumber(value.Value) {
					return fmt.Errorf("%w: required measurement metric value", ErrInvalidEvidence)
				}
				if _, duplicate := seenMetrics[value.MetricID]; duplicate {
					return fmt.Errorf("%w: duplicate required measurement metric value", ErrInvalidEvidence)
				}
				seenMetrics[value.MetricID] = struct{}{}
			}
			if len(seenMetrics) != len(expectedMetricsForMeasurement(measurement.Metrics)) {
				return fmt.Errorf("%w: incomplete required measurement metric values", ErrInvalidEvidence)
			}
		}
	}
	if len(expectedStrata) != 0 {
		return fmt.Errorf("%w: missing required measurement stratum", ErrInvalidEvidence)
	}
	return nil
}

func expectedMetricsForMeasurement(metrics []contract.PerformanceMetric) map[string]struct{} {
	result := make(map[string]struct{}, len(metrics))
	for _, metric := range metrics {
		result[string(metric.ID)] = struct{}{}
	}
	return result
}

func observedPerformanceValue(metric string, measurement execution.PerformanceMeasurement) float64 {
	counts := measurement.RequestCounts
	sum := func(values ...int) int {
		total := 0
		for _, value := range values {
			total += value
		}
		return total
	}
	switch metric {
	case "warm_connect_http_requests", "rebuild_connect_http_requests":
		return float64(counts.Connect)
	case "warm_connect_non_connect_http_requests":
		return float64(sum(counts.Push, counts.Pull, counts.RebuildPage, counts.SchemaFetch, counts.Other))
	case "steady_state_pull_http_requests_per_cycle", "pending_cycle_pull_http_requests", "rebuild_pull_http_requests":
		return float64(counts.Pull)
	case "steady_state_pull_non_pull_http_requests_per_cycle":
		return float64(sum(counts.Connect, counts.Push, counts.RebuildPage, counts.SchemaFetch, counts.Other))
	case "pending_cycle_push_http_requests":
		return float64(counts.Push)
	case "pending_cycle_non_push_or_pull_http_requests":
		return float64(sum(counts.Connect, counts.RebuildPage, counts.SchemaFetch, counts.Other))
	case "rebuild_page_request_count_minus_returned_page_count":
		return float64(counts.RebuildPage - measurement.ReturnedRebuildPageCount)
	case "rebuild_schema_fetch_http_requests":
		return float64(counts.SchemaFetch)
	case "rebuild_unexpected_http_requests":
		return float64(sum(counts.Push, counts.SchemaFetch, counts.Other))
	case "core_sync_outbound_network_or_rpc_hops":
		return float64(measurement.OutboundNetworkOrRPCHops)
	default:
		return math.NaN()
	}
}

func exactNumber(value string) (*big.Rat, error) {
	parsed := new(big.Rat)
	if _, ok := parsed.SetString(value); !ok {
		return nil, fmt.Errorf("invalid number %q", value)
	}
	return parsed, nil
}

func exactFloat(value float64) (*big.Rat, error) {
	if !finiteNumber(value) {
		return nil, fmt.Errorf("non-finite number")
	}
	return exactNumber(strconv.FormatFloat(value, 'g', -1, 64))
}

func finiteNumber(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func compareNumbers(left, right *big.Rat, comparator string) bool {
	switch comparator {
	case "eq":
		return left.Cmp(right) == 0
	case "lte":
		return left.Cmp(right) <= 0
	case "gte":
		return left.Cmp(right) >= 0
	default:
		return false
	}
}

func semanticJSONEqual(left, right json.RawMessage) bool {
	if len(left) == 0 || len(right) == 0 || jsonstrict.ValidateValue(left) != nil || jsonstrict.ValidateValue(right) != nil {
		return false
	}
	var leftValue, rightValue any
	if jsonstrict.Decode(left, &leftValue) != nil || jsonstrict.Decode(right, &rightValue) != nil {
		return false
	}
	return semanticJSONValueEqual(leftValue, rightValue)
}

func semanticJSONValueEqual(left, right any) bool {
	switch leftValue := left.(type) {
	case json.Number:
		rightValue, ok := right.(json.Number)
		if !ok {
			return false
		}
		leftNumber, leftErr := exactNumber(leftValue.String())
		rightNumber, rightErr := exactNumber(rightValue.String())
		return leftErr == nil && rightErr == nil && leftNumber.Cmp(rightNumber) == 0
	case map[string]any:
		rightValue, ok := right.(map[string]any)
		if !ok || len(leftValue) != len(rightValue) {
			return false
		}
		for key, value := range leftValue {
			other, found := rightValue[key]
			if !found || !semanticJSONValueEqual(value, other) {
				return false
			}
		}
		return true
	case []any:
		rightValue, ok := right.([]any)
		if !ok || len(leftValue) != len(rightValue) {
			return false
		}
		for index := range leftValue {
			if !semanticJSONValueEqual(leftValue[index], rightValue[index]) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(left, right)
	}
}

func uniqueStringSet(values []string) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" {
			return false
		}
		if _, exists := seen[value]; exists {
			return false
		}
		seen[value] = struct{}{}
	}
	return true
}

func stringSet(values []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}

func equalStringSets(left, right []string) bool {
	if !uniqueStringSet(left) || !uniqueStringSet(right) || len(left) != len(right) {
		return false
	}
	for _, value := range left {
		if _, found := stringSet(right)[value]; !found {
			return false
		}
	}
	return true
}

func stringSetKeys(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func stringsForAssertionIDs(values []contract.AssertionID) []string {
	result := make([]string, len(values))
	for index, value := range values {
		result[index] = string(value)
	}
	return result
}

func sameFaultInjection(left execution.InjectionRecipe, right scenarios.InjectionRecipe) bool {
	return left.Mechanism == right.Mechanism && left.Target == right.Target && left.Operator == right.Operator &&
		left.Parameters.Scenario == right.Parameters.Scenario && left.Parameters.Defect == right.Parameters.Defect && left.Parameters.Precondition == right.Parameters.Precondition
}

func faultPlanByID(scenario scenarios.Scenario, id string) (scenarios.FaultPlan, bool) {
	for _, plan := range scenario.FaultPlans {
		if string(plan.ID) == id {
			return plan, true
		}
	}
	return scenarios.FaultPlan{}, false
}

func negativeControlByID(scenario scenarios.Scenario, id string) (scenarios.NegativeControl, bool) {
	for _, control := range scenario.NegativeControls {
		if string(control.ControlID) == id {
			return control, true
		}
	}
	return scenarios.NegativeControl{}, false
}

func catalogControlByID(bundle *contract.Bundle, id string) (contract.Control, bool) {
	if bundle == nil {
		return contract.Control{}, false
	}
	for _, control := range bundle.Faults.Controls {
		if string(control.ID) == id {
			return control, true
		}
	}
	return contract.Control{}, false
}

func artifactBindingForInventory(bindings []execution.ArtifactBinding, inventoryID string) (execution.ArtifactBinding, bool) {
	for _, binding := range bindings {
		if binding.InventoryID == inventoryID {
			return binding, true
		}
	}
	return execution.ArtifactBinding{}, false
}

func validateReceiptFaults(fields execution.ReceiptFields, scenario scenarios.Scenario, obligation scenarios.ProofObligation, bundle *contract.Bundle) error {
	faultBearing := obligation.ProofType == "fault-injection" || obligation.ProofType == "negative-control"
	if faultBearing && fields.FaultExecution == nil {
		return fmt.Errorf("%w: missing fault execution", ErrInvalidEvidence)
	}
	if !faultBearing && fields.FaultExecution != nil {
		return fmt.Errorf("%w: unexpected fault execution", ErrInvalidEvidence)
	}
	if fields.FaultExecution != nil {
		if err := validateFaultExecution(fields, scenario, obligation, bundle); err != nil {
			return err
		}
	}
	if obligation.ProofType == "negative-control" {
		if fields.NegativeControl == nil {
			return fmt.Errorf("%w: missing negative control result", ErrInvalidEvidence)
		}
		if err := validateNegativeControl(fields, scenario, obligation, bundle); err != nil {
			return err
		}
	} else if fields.NegativeControl != nil {
		return fmt.Errorf("%w: unexpected negative control result", ErrInvalidEvidence)
	}
	return nil
}

func validateFaultExecution(fields execution.ReceiptFields, scenario scenarios.Scenario, obligation scenarios.ProofObligation, bundle *contract.Bundle) error {
	value := fields.FaultExecution
	if value == nil || obligation.FaultPlanID == nil || obligation.ControlID == nil {
		return fmt.Errorf("%w: fault ownership", ErrInvalidEvidence)
	}
	plan, found := faultPlanByID(scenario, string(*obligation.FaultPlanID))
	if !found || value.FaultPlanID != string(plan.ID) || value.FaultID != string(plan.FaultID) || value.ControlID != string(plan.ControlID) || value.ControlID != string(*obligation.ControlID) {
		return fmt.Errorf("%w: fault ownership", ErrInvalidEvidence)
	}
	control, found := negativeControlByID(scenario, value.ControlID)
	if !found || !equalStringSets(value.DetectedBy, stringsForAssertionIDs(control.DetectedBy)) || !sameFaultInjection(value.Injection, plan.Injection) || !attachmentHasKind(fields.Attachments, value.FaultPlanAttachmentID, "fault-plan") {
		return fmt.Errorf("%w: fault execution", ErrInvalidEvidence)
	}
	if catalogControl, found := catalogControlByID(bundle, value.ControlID); !found || value.SubjectType != catalogControl.SubjectType || value.FaultID != string(control.FaultID) {
		return fmt.Errorf("%w: fault execution metadata", ErrInvalidEvidence)
	}
	return nil
}

func validateNegativeControl(fields execution.ReceiptFields, scenario scenarios.Scenario, obligation scenarios.ProofObligation, bundle *contract.Bundle) error {
	value := fields.NegativeControl
	if value == nil || obligation.FaultPlanID == nil || obligation.ControlID == nil {
		return fmt.Errorf("%w: negative control ownership", ErrInvalidEvidence)
	}
	plan, planFound := faultPlanByID(scenario, string(*obligation.FaultPlanID))
	control, controlFound := negativeControlByID(scenario, string(*obligation.ControlID))
	if !planFound || !controlFound || value.FaultID != string(control.FaultID) || value.ControlID != string(control.ControlID) || value.FaultPlanID != string(plan.ID) || value.ControlSubjectID != string(control.ControlID) || value.Outcome != "detected" || !equalStringSets(value.DetectedBy, stringsForAssertionIDs(control.DetectedBy)) || !attachmentHasKind(fields.Attachments, value.FaultPlanAttachmentID, "fault-plan") {
		return fmt.Errorf("%w: negative control metadata", ErrInvalidEvidence)
	}
	catalogControl, found := catalogControlByID(bundle, string(control.ControlID))
	if !found || value.ControlSubjectType != catalogControl.SubjectType {
		return fmt.Errorf("%w: negative control subject type", ErrInvalidEvidence)
	}
	expectedSubjects := make(map[string]struct{}, len(control.SubjectArtifactInventoryIDs))
	for _, inventoryID := range control.SubjectArtifactInventoryIDs {
		artifact, found := artifactBindingForInventory(fields.ArtifactBindings, string(inventoryID))
		if !found {
			return fmt.Errorf("%w: negative control subject artifact", ErrInvalidEvidence)
		}
		expectedSubjects[artifact.ArtifactID] = struct{}{}
	}
	if !equalStringSets(value.ControlSubjectArtifactIDs, stringSetKeys(expectedSubjects)) {
		return fmt.Errorf("%w: negative control subject artifact", ErrInvalidEvidence)
	}
	for _, attachmentID := range value.AttachmentIDs {
		if !attachmentHasKind(fields.Attachments, attachmentID, "negative-control") {
			return fmt.Errorf("%w: negative control attachment", ErrInvalidEvidence)
		}
	}
	if len(value.AttachmentIDs) == 0 {
		return fmt.Errorf("%w: missing negative control attachment", ErrInvalidEvidence)
	}
	if _, found := artifactBindingForInventory(fields.ArtifactBindings, "ARTDEF-CONFORMANCE-RUNNER-001"); !found {
		return fmt.Errorf("%w: negative control runner binding", ErrInvalidEvidence)
	}
	return nil
}

func obligationByID(scenario scenarios.Scenario, id string) (scenarios.ProofObligation, bool) {
	for _, obligation := range scenario.ProofObligations {
		if string(obligation.ObligationID) == id {
			return obligation, true
		}
	}
	return scenarios.ProofObligation{}, false
}

func exactPayload(payloads []LockedPayload, path, mediaType string, size, sizeBytes int64, digest string) (LockedPayload, bool) {
	if sizeBytes != 0 && size != 0 && size != sizeBytes {
		return LockedPayload{}, false
	}
	if sizeBytes == 0 {
		sizeBytes = size
	}
	for _, payload := range payloads {
		if payload.Path == path && payload.MediaType == mediaType && payload.SizeBytes == sizeBytes && payload.SHA256 == digest {
			return payload, true
		}
	}
	return LockedPayload{}, false
}

func validVectorArtifactRole(language, role string) bool {
	switch language {
	case "go":
		return role == "conformance-runner"
	case "rust":
		return role == "pg-extension"
	case "swift":
		return role == "swift-spm"
	case "kotlin":
		return role == "kotlin-maven"
	default:
		return false
	}
}

func containsInventoryID(values []contract.ArtifactInventoryID, wanted string) bool {
	for _, value := range values {
		if string(value) == wanted {
			return true
		}
	}
	return false
}

func containsAttachment(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func containsSecretText(value string) bool {
	lower := strings.ToLower(value)
	for _, forbidden := range []string{"password", "passwd", "secret", "token", "jwt", "dsn", "authorization", "credential", "private_key", "private-key"} {
		if strings.Contains(lower, forbidden) {
			return true
		}
	}
	return false
}

func projectRun(fields execution.ReceiptFields) Run {
	duration := fields.CompletedAt.Sub(fields.StartedAt).Milliseconds()
	return Run{ID: fields.RunID, ExecutionLineageID: fields.ExecutionLineageID, URL: fields.RunURL, MakeTarget: fields.MakeTarget, Argv: append([]string(nil), fields.Argv...), Attempt: fields.Attempt, StartedAt: fields.StartedAt, CompletedAt: fields.CompletedAt, DurationMS: duration, Result: fields.Result, ExitCode: fields.ExitCode, Command: fields.Command, PreviousEvidenceID: fields.PreviousEvidenceID, RerunCause: fields.RerunCause, RerunDiagnosis: fields.RerunDiagnosis, CorrectiveAction: fields.CorrectiveAction, RerunApproval: fields.RerunApproval}
}

func evidenceID(fields execution.ReceiptFields) string {
	digest := sha256.Sum256([]byte(fields.ReceiptID))
	return "EVD-" + strings.ToUpper(hex.EncodeToString(digest[:8])) + "-001"
}

func stringsForRequirements(values []contract.RequirementID) []string {
	result := make([]string, len(values))
	for index, value := range values {
		result[index] = string(value)
	}
	sort.Strings(result)
	return result
}

func cloneSupportCell(value *contract.SupportCellID) *string {
	if value == nil {
		return nil
	}
	result := string(*value)
	return &result
}

func cloneBuilderConfig(cfg BuilderConfig) BuilderConfig {
	return cfg
}

func validateEvidenceShape(evidence Evidence) error {
	if evidence.ReceiptID == "" || evidence.RunnerDigest == "" || evidence.Receipt.Fields.RunnerArtifactSHA256 == "" || evidence.Receipt.Fields.RunnerExecutableSHA256 == "" || evidence.Receipt.Fields.GeneratorName == "" || evidence.Receipt.Fields.GeneratorVersion == "" || evidence.Receipt.Fields.GeneratorBinarySHA256 == "" || len(evidence.ArtifactBindings) == 0 || len(evidence.AttachmentIDs) == 0 {
		return fmt.Errorf("%w: evidence projection", ErrInvalidEvidence)
	}
	return nil
}
