package evidence

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/trainstar/synchro/conformance/execution"
	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
	"github.com/trainstar/synchro/conformance/internal/schemavalidator"
	"github.com/trainstar/synchro/conformance/scenarios"
	"github.com/trainstar/synchro/conformance/vectors"
)

// ValidateCandidate validates a final manifest and complete post-execution
// Phase 3 closure. Later release validation must add the frozen all-language
// vector catalog gate before this result can authorize promotion.
func ValidateCandidate(ctx context.Context, repoRoot, candidateRoot string) error {
	return ValidateCandidateWithPhase6(ctx, repoRoot, candidateRoot, nil)
}

// ValidateCandidateWithPhase6 validates candidate closure then invokes the
// Phase 6 verifier. A nil verifier fails closed.
func ValidateCandidateWithPhase6(ctx context.Context, repoRoot, candidateRoot string, verifier Phase6Verifier) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	candidate, err := LoadCandidate(ctx, repoRoot, candidateRoot)
	if err != nil {
		return err
	}
	manifest, err := loadFinalManifest(ctx, repoRoot, candidate)
	if err != nil {
		return err
	}
	if err := validateFinalManifestBinding(candidate, manifest); err != nil {
		return err
	}
	evidence, err := loadManifestEvidence(ctx, repoRoot, candidate, manifest)
	if err != nil {
		return err
	}
	if err := validateTerminalLineageForCandidate(candidate, evidence); err != nil {
		return err
	}
	if err := validateFullOwnershipClosure(ctx, repoRoot, candidate, evidence); err != nil {
		return err
	}
	if err := validateVectorLanguageClosure(evidence); err != nil {
		return err
	}
	if err := validateAttachmentClosure(candidate, evidence, manifest); err != nil {
		return err
	}
	if verifier == nil {
		return fmt.Errorf("%w: Phase 6 verifier is required", ErrIncompleteCandidate)
	}
	if err := verifier.VerifyPhase6(ctx, repoRoot, candidate, manifest); err != nil {
		return fmt.Errorf("%w: Phase 6 verifier: %v", ErrInvalidCandidate, err)
	}
	return nil
}

func loadFinalManifest(ctx context.Context, repoRoot string, candidate Candidate) (FinalManifest, error) {
	data, _, err := readLockedCandidateFile(candidate, finalManifestFile)
	if err != nil {
		return FinalManifest{}, fmt.Errorf("%w: final manifest is missing: %v", ErrIncompleteCandidate, err)
	}
	validator := schemavalidator.New(repoRoot)
	defer validator.Close()
	if err := validator.ValidateBytes(ctx, finalManifestSchema, data); err != nil {
		return FinalManifest{}, fmt.Errorf("%w: final manifest schema: %v", ErrInvalidCandidate, err)
	}
	if err := rejectDeclaredVerified(data); err != nil {
		return FinalManifest{}, err
	}
	var document finalManifestDocument
	if err := jsonstrict.Decode(data, &document); err != nil {
		return FinalManifest{}, fmt.Errorf("%w: decode final manifest: %v", ErrInvalidCandidate, err)
	}
	manifest := FinalManifest{
		CandidateID:            document.CandidateID,
		RunnerDigest:           document.RunnerDigest,
		SourceCommit:           document.SourceCommit,
		CandidateLock:          document.CandidateLock,
		ContractSnapshotSHA256: document.Contract.SnapshotSHA256,
		Generator:              document.Generator,
		Contract:               cloneLockedContract(document.Contract),
		Evidence:               append([]EvidenceReference(nil), document.Evidence...),
		TrustedRerunApprovers:  append([]string(nil), document.TrustedRerunApprovers...),
		Attestations:           cloneAttestations(document.Attestations),
	}
	for _, scenario := range document.Scenarios {
		manifest.Scenarios = append(manifest.Scenarios, LockedScenario{ID: scenario.ScenarioID, Path: scenario.Path, SHA256: scenario.SHA256})
	}
	for _, cell := range document.ResolvedSupportCells {
		dimensions := make(map[string]string, len(cell.Dimensions))
		for _, dimension := range cell.Dimensions {
			dimensions[dimension.Name] = dimension.Version
		}
		manifest.SupportCells = append(manifest.SupportCells, LockedSupportCell{ID: cell.SupportCellID, Dimensions: dimensions})
	}
	roles, err := lockedArtifactRoles(repoRoot, ctx)
	if err != nil {
		return FinalManifest{}, err
	}
	for _, artifact := range document.Artifacts {
		item := LockedArtifact{ID: artifact.ID, InventoryID: artifact.InventoryID, Role: roles[artifact.InventoryID]}
		for _, payload := range artifact.Payloads {
			item.Payloads = append(item.Payloads, LockedPayload{Path: payload.Path, MediaType: payload.MediaType, SizeBytes: payload.SizeBytes, SHA256: payload.SHA256})
		}
		manifest.Artifacts = append(manifest.Artifacts, item)
	}
	return manifest, nil
}

func lockedArtifactRoles(repoRoot string, ctx context.Context) (map[string]string, error) {
	bundle, err := contract.Load(ctx, repoRoot)
	if err != nil {
		return nil, fmt.Errorf("%w: load artifact inventory: %v", ErrInvalidCandidate, err)
	}
	roles := make(map[string]string, len(bundle.Artifacts.Artifacts))
	for _, artifact := range bundle.Artifacts.Artifacts {
		roles[string(artifact.ID)] = artifact.Role
	}
	return roles, nil
}

func validateFinalManifestBinding(candidate Candidate, manifest FinalManifest) error {
	if manifest.CandidateID != candidate.ID || manifest.RunnerDigest != candidate.RunnerDigest || manifest.SourceCommit != candidate.SourceCommit || manifest.Generator != candidate.Generator || !equalLockedContracts(manifest.Contract, candidate.Contract) || manifest.CandidateLock.Path != candidate.LockPath || manifest.CandidateLock.SHA256 != candidate.LockSHA256 {
		return fmt.Errorf("%w: final manifest candidate lock binding", ErrInvalidCandidate)
	}
	if len(manifest.Evidence) == 0 {
		return fmt.Errorf("%w: no evidence", ErrIncompleteCandidate)
	}
	if !equalLockedScenarios(candidate.Scenarios, manifest.Scenarios) || !equalLockedSupportCells(candidate.SupportCells, manifest.SupportCells) || !equalLockedArtifacts(candidate.Artifacts, manifest.Artifacts) || !equalAttestations(candidate.Attestations, manifest.Attestations) {
		return fmt.Errorf("%w: final manifest changed locked state", ErrInvalidCandidate)
	}
	approvers := make(map[string]struct{}, len(manifest.TrustedRerunApprovers))
	for _, approver := range manifest.TrustedRerunApprovers {
		approvers[approver] = struct{}{}
	}
	if len(approvers) != len(candidate.TrustedRerunApprovers) {
		return fmt.Errorf("%w: final manifest approver set", ErrInvalidCandidate)
	}
	for approver := range candidate.TrustedRerunApprovers {
		if _, found := approvers[approver]; !found {
			return fmt.Errorf("%w: final manifest approver set", ErrInvalidCandidate)
		}
	}
	return nil
}

func loadManifestEvidence(ctx context.Context, repoRoot string, candidate Candidate, manifest FinalManifest) ([]Evidence, error) {
	paths, err := candidateJSONPaths(candidate, "evidence")
	if err != nil {
		return nil, err
	}
	if len(paths) != len(manifest.Evidence) {
		return nil, fmt.Errorf("%w: final evidence file set", ErrInvalidCandidate)
	}
	seenPaths := make(map[string]struct{}, len(manifest.Evidence))
	seenIDs := make(map[string]struct{}, len(manifest.Evidence))
	seenReceipts := make(map[string]struct{}, len(manifest.Evidence))
	values := make([]Evidence, 0, len(manifest.Evidence))
	for _, reference := range manifest.Evidence {
		if !isPathUnder(reference.Path, "evidence") || !validSHA256(reference.SHA256) {
			return nil, fmt.Errorf("%w: final evidence reference", ErrInvalidCandidate)
		}
		if _, duplicate := seenPaths[reference.Path]; duplicate {
			return nil, fmt.Errorf("%w: reused evidence path", ErrInvalidCandidate)
		}
		if _, duplicate := seenIDs[reference.EvidenceID]; duplicate {
			return nil, fmt.Errorf("%w: duplicate evidence ID", ErrInvalidCandidate)
		}
		seenPaths[reference.Path] = struct{}{}
		seenIDs[reference.EvidenceID] = struct{}{}
		data, _, err := readLockedCandidateFile(candidate, reference.Path)
		if err != nil || sha256Hex(data) != reference.SHA256 {
			return nil, fmt.Errorf("%w: final evidence bytes", ErrInvalidCandidate)
		}
		validator := schemavalidator.New(repoRoot)
		if err := validator.ValidateBytes(ctx, evidenceSchema, data); err != nil {
			validator.Close()
			return nil, fmt.Errorf("%w: final evidence schema: %v", ErrInvalidCandidate, err)
		}
		validator.Close()
		if err := rejectDeclaredVerified(data); err != nil {
			return nil, err
		}
		var evidence Evidence
		if err := jsonstrict.Decode(data, &evidence); err != nil {
			return nil, fmt.Errorf("%w: decode final evidence: %v", ErrInvalidCandidate, err)
		}
		if evidence.EvidenceID != reference.EvidenceID || evidence.ScenarioID != reference.ScenarioID || evidence.ProofObligationID != reference.ProofObligationID || evidence.ProofType != reference.ProofType || !sameStringPointer(evidence.SupportCellID, reference.SupportCellID) {
			return nil, fmt.Errorf("%w: final evidence reference projection", ErrInvalidCandidate)
		}
		if _, duplicate := seenReceipts[evidence.ReceiptID]; duplicate {
			return nil, fmt.Errorf("%w: replayed receipt", ErrInvalidCandidate)
		}
		seenReceipts[evidence.ReceiptID] = struct{}{}
		if err := validateEvidence(ctx, repoRoot, candidate, evidence, reference.Path, &manifest); err != nil {
			return nil, err
		}
		values = append(values, evidence)
	}
	for _, path := range paths {
		if _, referenced := seenPaths[path]; !referenced {
			return nil, fmt.Errorf("%w: unreferenced evidence file", ErrInvalidCandidate)
		}
	}
	return values, nil
}

func validateTerminalLineage(values []Evidence) error {
	return validateTerminalLineageForCandidate(Candidate{}, values)
}

func validateTerminalLineageForCandidate(candidate Candidate, values []Evidence) error {
	byKey := make(map[string][]Evidence)
	seenRunIDs := make(map[string]struct{}, len(values))
	for _, evidence := range values {
		if _, duplicate := seenRunIDs[evidence.Run.ID]; duplicate {
			return fmt.Errorf("%w: duplicate run ID", ErrInvalidCandidate)
		}
		seenRunIDs[evidence.Run.ID] = struct{}{}
		key := evidence.CandidateID + "\x00" + evidence.ScenarioID + "\x00" + evidence.ProofObligationID + "\x00" + nullableString(evidence.SupportCellID)
		byKey[key] = append(byKey[key], evidence)
	}
	for _, lineage := range byKey {
		sort.Slice(lineage, func(left, right int) bool { return lineage[left].Run.Attempt < lineage[right].Run.Attempt })
		lineageID := lineage[0].Run.ExecutionLineageID
		for index, evidence := range lineage {
			if evidence.Run.Attempt != index+1 {
				return fmt.Errorf("%w: non-linear attempt sequence", ErrInvalidCandidate)
			}
			if evidence.Run.ExecutionLineageID != lineageID {
				return fmt.Errorf("%w: changed execution lineage", ErrInvalidCandidate)
			}
			if index == 0 {
				continue
			}
			predecessor := lineage[index-1]
			if evidence.Run.PreviousEvidenceID == nil || *evidence.Run.PreviousEvidenceID != predecessor.EvidenceID {
				return fmt.Errorf("%w: missing immediate predecessor", ErrInvalidCandidate)
			}
			if !rerunnableInfrastructureError(predecessor) {
				return fmt.Errorf("%w: semantic failure relabeled as infrastructure", ErrInvalidCandidate)
			}
			if evidence.Run.RerunCause == nil || !allowedRerunCause(*evidence.Run.RerunCause) {
				return fmt.Errorf("%w: rerun cause", ErrInvalidCandidate)
			}
			if evidence.Run.RerunApproval == nil || !evidence.Run.RerunApproval.ApprovedAt.After(predecessor.Run.CompletedAt) || !evidence.Run.RerunApproval.ApprovedAt.Before(evidence.Run.StartedAt) {
				return fmt.Errorf("%w: rerun approval ordering", ErrInvalidCandidate)
			}
			if len(candidate.TrustedRerunApprovers) != 0 {
				if _, trusted := candidate.TrustedRerunApprovers[evidence.Run.RerunApproval.ApproverIdentity]; !trusted {
					return fmt.Errorf("%w: untrusted rerun approver", ErrInvalidCandidate)
				}
			}
			if !unchangedLineageBindings(predecessor, evidence) {
				return fmt.Errorf("%w: changed rerun bindings", ErrInvalidCandidate)
			}
		}
		terminal := lineage[len(lineage)-1]
		if terminal.Run.ExitCode != 0 || terminal.Run.Result != execution.ResultPassed || !allRequiredOutcomesPassed(terminal) {
			return fmt.Errorf("%w: terminal evidence is not passed", ErrInvalidCandidate)
		}
	}
	return nil
}

func validateFullOwnershipClosure(ctx context.Context, repoRoot string, candidate Candidate, evidence []Evidence) error {
	bundle, err := contract.Load(ctx, repoRoot)
	if err != nil {
		return err
	}
	allScenarios, err := scenarios.LoadAll(ctx, repoRoot)
	if err != nil {
		return err
	}
	if err := requireExactCandidateScenarioCatalog(ctx, repoRoot, candidate, allScenarios); err != nil {
		return err
	}
	vectorCatalog, err := vectors.Load(ctx, repoRoot)
	if err != nil {
		return fmt.Errorf("%w: load vector catalog: %v", ErrInvalidCandidate, err)
	}
	lockedScenarios, err := loadLockedScenarios(ctx, repoRoot, candidate, allScenarios)
	if err != nil {
		return err
	}
	if err := scenarios.ValidateAllWithVectors(lockedScenarios, bundle, vectorCatalog); err != nil {
		return fmt.Errorf("%w: scenario cross-validation: %v", ErrInvalidCandidate, err)
	}
	selectedRequirements := make(map[contract.RequirementID]struct{})
	for _, scenario := range lockedScenarios {
		for _, requirementID := range scenario.RequirementIDs {
			selectedRequirements[requirementID] = struct{}{}
		}
	}
	if len(selectedRequirements) != len(bundle.Requirements.Requirements) {
		return fmt.Errorf("%w: selected requirement set", ErrIncompleteCandidate)
	}
	for _, requirement := range bundle.Requirements.Requirements {
		if _, selected := selectedRequirements[requirement.ID]; !selected {
			return fmt.Errorf("%w: selected requirement set", ErrIncompleteCandidate)
		}
	}
	expected := make(map[string]struct{})
	for _, scenario := range lockedScenarios {
		for _, ownership := range scenario.Ownership {
			expected[ownershipTupleKey(string(ownership.ScenarioID), string(ownership.ProofObligationID), ownership.ProofType, nullableContractSupport(ownership.SupportCellID), string(ownership.RequirementID), string(ownership.AssertionID))] = struct{}{}
		}
	}
	actual := make(map[string]struct{})
	for _, item := range evidence {
		locked, found := candidate.Scenarios[item.ScenarioID]
		if !found || locked.Path == "" {
			return fmt.Errorf("%w: evidence scenario not locked", ErrInvalidCandidate)
		}
		scenarioData, _, err := readLockedCandidateFile(candidate, locked.Path)
		if err != nil || sha256Hex(scenarioData) != locked.SHA256 {
			return fmt.Errorf("%w: locked scenario changed", ErrInvalidCandidate)
		}
		scenario, err := scenarios.LoadBytes(ctx, repoRoot, locked.Path, scenarioData)
		if err != nil {
			return err
		}
		for _, assertion := range item.Assertions {
			for _, ownership := range scenario.Ownership {
				if string(ownership.ProofObligationID) == item.ProofObligationID && ownership.ProofType == item.ProofType && nullableContractSupport(ownership.SupportCellID) == nullableString(item.SupportCellID) && string(ownership.AssertionID) == assertion.AssertionID {
					key := ownershipTupleKey(item.ScenarioID, item.ProofObligationID, item.ProofType, nullableString(item.SupportCellID), string(ownership.RequirementID), assertion.AssertionID)
					if _, duplicate := actual[key]; duplicate {
						return fmt.Errorf("%w: duplicate ownership evidence", ErrInvalidCandidate)
					}
					actual[key] = struct{}{}
				}
			}
		}
	}
	if len(actual) != len(expected) {
		return fmt.Errorf("%w: ownership tuple count", ErrIncompleteCandidate)
	}
	for key := range expected {
		if _, found := actual[key]; !found {
			return fmt.Errorf("%w: missing ownership tuple", ErrIncompleteCandidate)
		}
	}
	return nil
}

// validateVectorLanguageClosure enforces the Phase 3 Go-only vector policy.
// Later release validation must fail closed until it checks all frozen languages.
func validateVectorLanguageClosure(evidence []Evidence) error {
	for _, item := range evidence {
		seen := make(map[string]struct{}, len(item.VectorResults))
		for _, result := range item.VectorResults {
			key := vectorResultKey(result.VectorSetID, result.Language)
			if _, duplicate := seen[key]; duplicate || result.Language != "go" {
				return fmt.Errorf("%w: vector set-language closure", ErrInvalidCandidate)
			}
			if result.Outcome != "passed" || result.FailedCount != 0 || result.PassedCount != result.ExecutedCount {
				return fmt.Errorf("%w: failed vector result", ErrInvalidCandidate)
			}
			seen[key] = struct{}{}
		}
	}
	return nil
}

func requireExactCandidateScenarioCatalog(ctx context.Context, repoRoot string, candidate Candidate, repository []scenarios.Scenario) error {
	catalogData, err := os.ReadFile(filepath.Join(repoRoot, "conformance", "catalog.json"))
	if err != nil {
		return fmt.Errorf("%w: read repository scenario catalog: %v", ErrInvalidCandidate, err)
	}
	var catalog scenarios.Catalog
	if err := jsonstrict.Decode(catalogData, &catalog); err != nil {
		return fmt.Errorf("%w: decode repository scenario catalog: %v", ErrInvalidCandidate, err)
	}
	if len(catalog.Scenarios) != len(repository) || len(candidate.Scenarios) != len(catalog.Scenarios) {
		return fmt.Errorf("%w: candidate scenario catalog", ErrIncompleteCandidate)
	}
	entries := make(map[contract.ScenarioID]scenarios.ScenarioEntry, len(catalog.Scenarios))
	for _, entry := range catalog.Scenarios {
		if _, duplicate := entries[entry.ScenarioID]; duplicate {
			return fmt.Errorf("%w: duplicate repository scenario catalog ID", ErrInvalidCandidate)
		}
		entries[entry.ScenarioID] = entry
	}
	for _, scenario := range repository {
		if err := contextError(ctx); err != nil {
			return err
		}
		locked, found := candidate.Scenarios[string(scenario.ID)]
		if !found {
			return fmt.Errorf("%w: candidate scenario catalog", ErrIncompleteCandidate)
		}
		entry, found := entries[scenario.ID]
		if !found {
			return fmt.Errorf("%w: repository scenario catalog", ErrInvalidCandidate)
		}
		if locked.ID != string(entry.ScenarioID) || locked.Path != entry.Path || locked.SHA256 != entry.SHA256 {
			return fmt.Errorf("%w: candidate scenario catalog", ErrInvalidCandidate)
		}
		data, _, readErr := readLockedCandidateFile(candidate, locked.Path)
		if readErr != nil || sha256Hex(data) != locked.SHA256 {
			return fmt.Errorf("%w: locked scenario changed", ErrInvalidCandidate)
		}
	}
	return nil
}

func loadLockedScenarios(ctx context.Context, repoRoot string, candidate Candidate, repository []scenarios.Scenario) ([]scenarios.Scenario, error) {
	lockedValues := make([]scenarios.Scenario, 0, len(repository))
	for _, repositoryScenario := range repository {
		locked, found := candidate.Scenarios[string(repositoryScenario.ID)]
		if !found {
			return nil, fmt.Errorf("%w: candidate scenario catalog", ErrIncompleteCandidate)
		}
		data, _, err := readLockedCandidateFile(candidate, locked.Path)
		if err != nil || sha256Hex(data) != locked.SHA256 {
			return nil, fmt.Errorf("%w: locked scenario changed", ErrInvalidCandidate)
		}
		loaded, err := scenarios.LoadBytes(ctx, repoRoot, locked.Path, data)
		if err != nil {
			return nil, fmt.Errorf("%w: load locked scenario: %v", ErrInvalidCandidate, err)
		}
		if loaded.ID != repositoryScenario.ID {
			return nil, fmt.Errorf("%w: locked scenario identity", ErrInvalidCandidate)
		}
		lockedValues = append(lockedValues, loaded)
	}
	return lockedValues, nil
}

func candidateJSONPaths(candidate Candidate, directory string) ([]string, error) {
	return candidateRegularPaths(candidate, directory, true)
}

func candidateRegularPaths(candidate Candidate, directory string, jsonOnly bool) ([]string, error) {
	if err := verifyCandidateRoot(candidate); err != nil {
		return nil, err
	}
	root := filepath.Join(candidate.Root, filepath.FromSlash(directory))
	info, err := os.Lstat(root)
	if err != nil {
		return nil, fmt.Errorf("%w: candidate directory %q: %v", ErrInvalidCandidate, directory, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return nil, fmt.Errorf("%w: candidate directory %q", ErrInvalidCandidate, directory)
	}
	var paths []string
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("candidate directory contains a symlink")
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("candidate directory contains a nonregular file")
		}
		if jsonOnly && filepath.Ext(entry.Name()) != ".json" {
			return fmt.Errorf("evidence directory contains a non-JSON file")
		}
		relative, err := filepath.Rel(candidate.Root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if !isPathUnder(relative, directory) {
			return fmt.Errorf("candidate directory path is invalid")
		}
		paths = append(paths, relative)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("%w: walk candidate directory %q: %v", ErrInvalidCandidate, directory, err)
	}
	if err := verifyCandidateRoot(candidate); err != nil {
		return nil, err
	}
	sort.Strings(paths)
	return paths, nil
}

func isPathUnder(path, directory string) bool {
	return validCandidatePath(path) && strings.HasPrefix(path, directory+"/")
}

func validateAttachmentClosure(candidate Candidate, evidence []Evidence, manifest FinalManifest) error {
	diskPaths, err := candidateRegularPaths(candidate, attachmentDirectory, false)
	if err != nil {
		return err
	}
	referenced := make(map[string]struct{})
	reserved := reservedCandidatePaths(candidate, manifest)
	for _, item := range evidence {
		for _, attachment := range item.Attachments {
			if !isPathUnder(attachment.Path, attachmentDirectory) {
				return fmt.Errorf("%w: attachment outside attachment directory", ErrInvalidCandidate)
			}
			if _, duplicate := referenced[attachment.Path]; duplicate {
				return fmt.Errorf("%w: cross-evidence attachment path reuse", ErrInvalidCandidate)
			}
			if _, reused := reserved[attachment.Path]; reused {
				return fmt.Errorf("%w: attachment path reuses candidate path", ErrInvalidCandidate)
			}
			referenced[attachment.Path] = struct{}{}
		}
	}
	if len(referenced) != len(diskPaths) {
		return fmt.Errorf("%w: attachment file set", ErrInvalidCandidate)
	}
	for _, path := range diskPaths {
		if _, found := referenced[path]; !found {
			return fmt.Errorf("%w: unreferenced attachment", ErrInvalidCandidate)
		}
	}
	return nil
}

func reservedCandidatePaths(candidate Candidate, manifest FinalManifest) map[string]struct{} {
	paths := map[string]struct{}{candidate.LockPath: {}, finalManifestFile: {}}
	for _, item := range candidate.Scenarios {
		paths[item.Path] = struct{}{}
	}
	for _, item := range candidate.Artifacts {
		for _, payload := range item.Payloads {
			paths[payload.Path] = struct{}{}
		}
	}
	for _, item := range candidate.Attestations {
		paths[item.Path] = struct{}{}
		paths[item.SigstoreVerification.BundlePath] = struct{}{}
	}
	for _, item := range manifest.Evidence {
		paths[item.Path] = struct{}{}
	}
	return paths
}

func unchangedLineageBindings(predecessor, successor Evidence) bool {
	return predecessor.CandidateID == successor.CandidateID &&
		predecessor.EvidenceClass == successor.EvidenceClass &&
		predecessor.ReleaseVersion == successor.ReleaseVersion &&
		predecessor.ProtocolVersion == successor.ProtocolVersion &&
		predecessor.ContractSnapshotSHA256 == successor.ContractSnapshotSHA256 &&
		predecessor.SourceCommit == successor.SourceCommit &&
		predecessor.ScenarioID == successor.ScenarioID &&
		predecessor.ProofObligationID == successor.ProofObligationID &&
		predecessor.ProofType == successor.ProofType &&
		sameStringPointer(predecessor.SupportCellID, successor.SupportCellID) &&
		equalStrings(predecessor.RequirementIDs, successor.RequirementIDs) &&
		predecessor.Run.MakeTarget == successor.Run.MakeTarget &&
		equalStrings(predecessor.Run.Argv, successor.Run.Argv) &&
		predecessor.RunnerDigest == successor.RunnerDigest &&
		predecessor.Receipt.Fields.RunnerArtifactSHA256 == successor.Receipt.Fields.RunnerArtifactSHA256 &&
		predecessor.Receipt.Fields.RunnerExecutableSHA256 == successor.Receipt.Fields.RunnerExecutableSHA256 &&
		predecessor.Generator == successor.Generator &&
		reflect.DeepEqual(predecessor.ArtifactBindings, successor.ArtifactBindings) &&
		reflect.DeepEqual(predecessor.Environment, successor.Environment) &&
		sameAssertionIdentities(predecessor.Assertions, successor.Assertions) &&
		reflect.DeepEqual(predecessor.ExecutionArtifacts, successor.ExecutionArtifacts) &&
		reflect.DeepEqual(predecessor.Replay, successor.Replay) &&
		reflect.DeepEqual(predecessor.FaultExecution, successor.FaultExecution) &&
		reflect.DeepEqual(predecessor.PerformanceResults, successor.PerformanceResults) &&
		reflect.DeepEqual(predecessor.RequiredMeasurementResults, successor.RequiredMeasurementResults) &&
		reflect.DeepEqual(predecessor.VectorResults, successor.VectorResults) &&
		reflect.DeepEqual(predecessor.NegativeControl, successor.NegativeControl) &&
		sameStringPointer(predecessor.Seed, successor.Seed)
}

func sameAssertionIdentities(left, right []execution.AssertionResult) bool {
	if len(left) != len(right) {
		return false
	}
	seen := make(map[string]struct{}, len(left))
	for _, value := range left {
		seen[value.AssertionID] = struct{}{}
	}
	for _, value := range right {
		if _, found := seen[value.AssertionID]; !found {
			return false
		}
		delete(seen, value.AssertionID)
	}
	return len(seen) == 0
}

func rerunnableInfrastructureError(item Evidence) bool {
	if item.Run.ExitCode == 0 || item.Run.Result != execution.ResultError {
		return false
	}
	return allRequiredOutcomesPassed(item)
}

func allRequiredOutcomesPassed(item Evidence) bool {
	if !allPassedAssertions(item.Assertions) || !allPassedVectors(item.VectorResults) || !allPassedPerformance(item.PerformanceResults, item.RequiredMeasurementResults) {
		return false
	}
	if item.NegativeControl != nil && item.NegativeControl.Outcome != "detected" {
		return false
	}
	return true
}

func equalLockedScenarios(expected map[string]LockedScenario, actual []LockedScenario) bool {
	if len(expected) != len(actual) {
		return false
	}
	seenIDs := make(map[string]struct{}, len(actual))
	for _, item := range actual {
		if _, found := seenIDs[item.ID]; found || expected[item.ID] != item {
			return false
		}
		seenIDs[item.ID] = struct{}{}
	}
	return true
}

func equalLockedSupportCells(expected map[string]LockedSupportCell, actual []LockedSupportCell) bool {
	if len(expected) != len(actual) {
		return false
	}
	seenIDs := make(map[string]struct{}, len(actual))
	for _, item := range actual {
		want, found := expected[item.ID]
		if !found || len(want.Dimensions) != len(item.Dimensions) {
			return false
		}
		if _, found := seenIDs[item.ID]; found {
			return false
		}
		seenIDs[item.ID] = struct{}{}
		for name, value := range want.Dimensions {
			if item.Dimensions[name] != value {
				return false
			}
		}
	}
	return true
}

func equalLockedArtifacts(expected map[string]LockedArtifact, actual []LockedArtifact) bool {
	if len(expected) != len(actual) {
		return false
	}
	seenIDs := make(map[string]struct{}, len(actual))
	for _, item := range actual {
		want, found := expected[item.ID]
		if !found || want.InventoryID != item.InventoryID || want.Role != item.Role || len(want.Payloads) != len(item.Payloads) {
			return false
		}
		if _, found := seenIDs[item.ID]; found {
			return false
		}
		seenIDs[item.ID] = struct{}{}
		for index := range want.Payloads {
			if want.Payloads[index] != item.Payloads[index] {
				return false
			}
		}
	}
	return true
}

func cloneAttestations(values []Attestation) []Attestation {
	result := make([]Attestation, 0, len(values))
	for _, value := range values {
		result = append(result, cloneAttestation(value))
	}
	return result
}

func equalAttestations(expected, actual []Attestation) bool {
	if len(expected) != len(actual) {
		return false
	}
	expectedByID := make(map[string]Attestation, len(expected))
	for _, value := range expected {
		if _, found := expectedByID[value.ID]; found {
			return false
		}
		expectedByID[value.ID] = value
	}
	seenIDs := make(map[string]struct{}, len(actual))
	for _, value := range actual {
		if _, found := seenIDs[value.ID]; found {
			return false
		}
		seenIDs[value.ID] = struct{}{}
		expectedValue, found := expectedByID[value.ID]
		if !found || !equalAttestation(expectedValue, value) {
			return false
		}
	}
	return true
}

func equalAttestation(left, right Attestation) bool {
	if left.ID != right.ID || left.Kind != right.Kind || left.Format != right.Format || left.MediaType != right.MediaType || left.SubjectArtifactID != right.SubjectArtifactID || left.Path != right.Path || left.SHA256 != right.SHA256 || !equalAttestationSubjectSets(left.SubjectPayloads, right.SubjectPayloads) {
		return false
	}
	leftVerification := left.SigstoreVerification
	rightVerification := right.SigstoreVerification
	return leftVerification.BundlePath == rightVerification.BundlePath &&
		leftVerification.BundleMediaType == rightVerification.BundleMediaType &&
		leftVerification.BundleSHA256 == rightVerification.BundleSHA256 &&
		leftVerification.SignedAttestationSHA256 == rightVerification.SignedAttestationSHA256 &&
		equalAttestationSubjectSets(leftVerification.SignedSubjects, rightVerification.SignedSubjects) &&
		leftVerification.CertificateIssuer == rightVerification.CertificateIssuer &&
		leftVerification.CertificateIdentity == rightVerification.CertificateIdentity &&
		leftVerification.Verifier == rightVerification.Verifier &&
		leftVerification.VerifiedAt == rightVerification.VerifiedAt &&
		leftVerification.VerificationURI == rightVerification.VerificationURI
}

func sameStringPointer(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func nullableString(value *string) string {
	if value == nil {
		return "null"
	}
	return *value
}

func nullableContractSupport(value *contract.SupportCellID) string {
	if value == nil {
		return "null"
	}
	return string(*value)
}

func ownershipTupleKey(scenario, obligation, proof, support, requirement, assertion string) string {
	return scenario + "\x00" + obligation + "\x00" + proof + "\x00" + support + "\x00" + requirement + "\x00" + assertion
}

func hasSemanticFailure(assertions []execution.AssertionResult) bool {
	for _, assertion := range assertions {
		if assertion.Outcome == "failed" {
			return true
		}
	}
	return false
}
