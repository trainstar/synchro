package evidence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
	"github.com/trainstar/synchro/conformance/internal/schemavalidator"
)

const (
	candidateLockSchema = "conformance/schemas/rc-candidate-lock-v1.schema.json"
	finalManifestSchema = "conformance/schemas/rc-manifest-v2.schema.json"
	evidenceSchema      = "conformance/schemas/evidence-v2.schema.json"
)

type candidateLockDocument struct {
	SchemaURI             string                      `json:"$schema"`
	SchemaVersion         int                         `json:"schema_version"`
	CandidateID           string                      `json:"candidate_id"`
	ReleaseVersion        string                      `json:"release_version"`
	ProtocolVersion       int                         `json:"protocol_version"`
	SourceCommit          string                      `json:"source_commit"`
	RunnerDigest          string                      `json:"runner_digest"`
	Generator             Generator                   `json:"generator"`
	TrustedRerunApprovers []string                    `json:"trusted_rerun_approvers"`
	Contract              lockedContract              `json:"contract"`
	Scenarios             []lockedScenarioDocument    `json:"scenarios"`
	ResolvedSupportCells  []lockedSupportCellDocument `json:"resolved_support_cells"`
	Artifacts             []lockedArtifactDocument    `json:"artifacts"`
	Attestations          []Attestation               `json:"attestations"`
}

type lockedContract struct {
	SnapshotSHA256    string                       `json:"snapshot_sha256"`
	Requirements      contract.FileBinding         `json:"requirements"`
	SupportMatrix     contract.FileBinding         `json:"support_matrix"`
	BehavioralFiles   []contract.BehavioralBinding `json:"behavioral_files"`
	VerificationInput contract.VerificationInputs  `json:"verification_inputs"`
	SchemaFiles       contract.SchemaFiles         `json:"schema_files"`
}

type lockedScenarioDocument struct {
	ScenarioID string `json:"scenario_id"`
	Path       string `json:"path"`
	SHA256     string `json:"sha256"`
}

type lockedSupportCellDocument struct {
	SupportCellID string                    `json:"support_cell_id"`
	Dimensions    []lockedDimensionDocument `json:"dimensions"`
}

type lockedDimensionDocument struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type lockedArtifactDocument struct {
	ID             string                  `json:"id"`
	InventoryID    string                  `json:"inventory_id"`
	ReleaseVersion string                  `json:"release_version"`
	PackageVersion string                  `json:"package_version"`
	Payloads       []lockedPayloadDocument `json:"payloads"`
}

type lockedPayloadDocument struct {
	Path      string `json:"path"`
	MediaType string `json:"media_type"`
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
}

type finalManifestDocument struct {
	SchemaURI             string                      `json:"$schema"`
	SchemaVersion         int                         `json:"schema_version"`
	CandidateID           string                      `json:"candidate_id"`
	ReleaseVersion        string                      `json:"release_version"`
	ProtocolVersion       int                         `json:"protocol_version"`
	SourceCommit          string                      `json:"source_commit"`
	RunnerDigest          string                      `json:"runner_digest"`
	Generator             Generator                   `json:"generator"`
	CandidateLock         FileBinding                 `json:"candidate_lock"`
	TrustedRerunApprovers []string                    `json:"trusted_rerun_approvers"`
	Contract              lockedContract              `json:"contract"`
	Scenarios             []lockedScenarioDocument    `json:"scenarios"`
	Evidence              []EvidenceReference         `json:"evidence"`
	ResolvedSupportCells  []lockedSupportCellDocument `json:"resolved_support_cells"`
	Artifacts             []lockedArtifactDocument    `json:"artifacts"`
	Attestations          []Attestation               `json:"attestations"`
}

type scenarioCatalogDocument struct {
	SchemaVersion int                            `json:"schema_version"`
	Scenarios     []scenarioCatalogEntryDocument `json:"scenarios"`
}

type scenarioCatalogEntryDocument struct {
	ScenarioID string `json:"scenario_id"`
	Path       string `json:"path"`
	SHA256     string `json:"sha256"`
}

// LoadCandidate loads and verifies the pre-execution candidate lock.
func LoadCandidate(ctx context.Context, repoRoot, candidateRoot string) (Candidate, error) {
	if err := contextError(ctx); err != nil {
		return Candidate{}, err
	}
	repositoryRoot, err := resolvedRepositoryRoot(repoRoot)
	if err != nil {
		return Candidate{}, fmt.Errorf("%w: repository root: %v", ErrInvalidCandidate, err)
	}
	if err := verifyRepositorySource(ctx, repositoryRoot, ""); err != nil {
		return Candidate{}, fmt.Errorf("%w: repository source before candidate reads: %v", ErrInvalidCandidate, err)
	}
	root, rootIdentity, err := candidateRootIdentity(candidateRoot)
	if err != nil {
		return Candidate{}, err
	}
	lockBytes, lockPath, err := readCandidateFileWithIdentity(root, rootIdentity, candidateLockFile)
	if err != nil {
		return Candidate{}, fmt.Errorf("%w: read candidate lock: %v", ErrInvalidCandidate, err)
	}
	validator := schemavalidator.New(repositoryRoot)
	defer validator.Close()
	if err := validator.ValidateBytes(ctx, candidateLockSchema, lockBytes); err != nil {
		return Candidate{}, fmt.Errorf("%w: validate candidate lock schema: %v", ErrInvalidCandidate, err)
	}
	var lock candidateLockDocument
	if err := jsonstrict.Decode(lockBytes, &lock); err != nil {
		return Candidate{}, fmt.Errorf("%w: decode candidate lock: %v", ErrInvalidCandidate, err)
	}
	if err := rejectDeclaredVerified(lockBytes); err != nil {
		return Candidate{}, err
	}
	bundle, err := contract.Load(ctx, repositoryRoot)
	if err != nil {
		return Candidate{}, fmt.Errorf("%w: load contract: %v", ErrInvalidCandidate, err)
	}
	snapshot, err := contract.BuildSnapshot(ctx, repositoryRoot)
	if err != nil {
		return Candidate{}, fmt.Errorf("%w: build contract snapshot: %v", ErrInvalidCandidate, err)
	}
	digest, err := snapshot.SHA256()
	if err != nil {
		return Candidate{}, fmt.Errorf("%w: hash contract snapshot: %v", ErrInvalidCandidate, err)
	}
	expectedContract := contractLockForSnapshot(snapshot, hex.EncodeToString(digest[:]))
	if lock.ReleaseVersion != "0.3.0" || lock.ProtocolVersion != 3 || !equalLockedContracts(lock.Contract, expectedContract) {
		return Candidate{}, fmt.Errorf("%w: candidate lock contract binding", ErrInvalidCandidate)
	}
	if lock.CandidateID == "" || !validCommit(lock.SourceCommit) || !validSHA256(lock.RunnerDigest) || !validGenerator(lock.Generator) || len(lock.Scenarios) == 0 || len(lock.ResolvedSupportCells) == 0 || len(lock.Artifacts) == 0 {
		return Candidate{}, fmt.Errorf("%w: candidate lock identity", ErrInvalidCandidate)
	}
	candidate := Candidate{
		RepoRoot:               repositoryRoot,
		Root:                   root,
		LockPath:               lockPath,
		LockSHA256:             sha256Hex(lockBytes),
		ID:                     lock.CandidateID,
		ReleaseVersion:         lock.ReleaseVersion,
		ProtocolVersion:        lock.ProtocolVersion,
		SourceCommit:           lock.SourceCommit,
		RunnerDigest:           lock.RunnerDigest,
		ContractSnapshotSHA256: lock.Contract.SnapshotSHA256,
		Generator:              lock.Generator,
		Contract:               cloneLockedContract(lock.Contract),
		TrustedRerunApprovers:  make(map[string]struct{}, len(lock.TrustedRerunApprovers)),
		Scenarios:              make(map[string]LockedScenario, len(lock.Scenarios)),
		SupportCells:           make(map[string]LockedSupportCell, len(lock.ResolvedSupportCells)),
		Artifacts:              make(map[string]LockedArtifact, len(lock.Artifacts)),
		ArtifactsByInventoryID: make(map[string]LockedArtifact, len(lock.Artifacts)),
		Attestations:           make([]Attestation, 0, len(lock.Attestations)),
		rootIdentity:           rootIdentity,
	}
	repositoryCommit, err := repositorySourceCommit(ctx, repositoryRoot)
	if err != nil || repositoryCommit != lock.SourceCommit {
		return Candidate{}, fmt.Errorf("%w: source commit binding", ErrInvalidCandidate)
	}
	candidate.RepositorySourceCommit = repositoryCommit
	if err := populateCandidate(&candidate, lock, bundle); err != nil {
		return Candidate{}, err
	}
	if err := verifyAuthoritativeSources(ctx, candidate); err != nil {
		return Candidate{}, err
	}
	if err := verifyCandidateFiles(ctx, candidate); err != nil {
		return Candidate{}, err
	}
	if err := verifyRepositorySource(ctx, repositoryRoot, lock.SourceCommit); err != nil {
		return Candidate{}, fmt.Errorf("%w: repository source after candidate reads: %v", ErrInvalidCandidate, err)
	}
	return candidate, nil
}

// BindCandidateRoot binds a manually constructed candidate to its current root.
// Later validation rejects a replacement at that path.
func BindCandidateRoot(candidate Candidate) (Candidate, error) {
	root, identity, err := candidateRootIdentity(candidate.Root)
	if err != nil {
		return Candidate{}, fmt.Errorf("%w: candidate root: %v", ErrInvalidCandidate, err)
	}
	candidate.Root = root
	candidate.rootIdentity = identity
	return candidate, nil
}

func contractLockForSnapshot(snapshot contract.Snapshot, digest string) lockedContract {
	return lockedContract{
		SnapshotSHA256:    digest,
		Requirements:      snapshot.Requirements,
		SupportMatrix:     snapshot.SupportMatrix,
		BehavioralFiles:   append([]contract.BehavioralBinding(nil), snapshot.BehavioralFiles...),
		VerificationInput: snapshot.VerificationInputs,
		SchemaFiles:       snapshot.SchemaFiles,
	}
}

func cloneLockedContract(value lockedContract) lockedContract {
	value.BehavioralFiles = append([]contract.BehavioralBinding(nil), value.BehavioralFiles...)
	return value
}

func equalLockedContracts(left, right lockedContract) bool {
	return left.SnapshotSHA256 == right.SnapshotSHA256 &&
		left.Requirements == right.Requirements &&
		left.SupportMatrix == right.SupportMatrix &&
		reflect.DeepEqual(left.BehavioralFiles, right.BehavioralFiles) &&
		left.VerificationInput == right.VerificationInput &&
		left.SchemaFiles == right.SchemaFiles
}

func populateCandidate(candidate *Candidate, lock candidateLockDocument, bundle *contract.Bundle) error {
	if candidate == nil || bundle == nil {
		return ErrInvalidCandidate
	}
	for _, approver := range lock.TrustedRerunApprovers {
		if approver == "" {
			return fmt.Errorf("%w: empty trusted rerun approver", ErrInvalidCandidate)
		}
		if _, exists := candidate.TrustedRerunApprovers[approver]; exists {
			return fmt.Errorf("%w: duplicate trusted rerun approver", ErrInvalidCandidate)
		}
		candidate.TrustedRerunApprovers[approver] = struct{}{}
	}
	if len(candidate.TrustedRerunApprovers) == 0 {
		return fmt.Errorf("%w: no trusted rerun approver", ErrInvalidCandidate)
	}
	catalog, err := loadCommittedScenarioCatalog(context.Background(), candidate.RepoRoot, candidate.SourceCommit, candidate.Contract.VerificationInput.ScenarioCatalog.Path)
	if err != nil {
		return fmt.Errorf("%w: committed scenario catalog: %v", ErrInvalidCandidate, err)
	}
	for _, scenario := range lock.Scenarios {
		if scenario.ScenarioID == "" || !validCandidatePath(scenario.Path) || !validSHA256(scenario.SHA256) {
			return fmt.Errorf("%w: invalid locked scenario", ErrInvalidCandidate)
		}
		catalogEntry, found := catalog[scenario.ScenarioID]
		if !found || catalogEntry.Path != scenario.Path || catalogEntry.SHA256 != scenario.SHA256 {
			return fmt.Errorf("%w: locked scenario differs from committed catalog", ErrInvalidCandidate)
		}
		if _, exists := candidate.Scenarios[scenario.ScenarioID]; exists {
			return fmt.Errorf("%w: duplicate locked scenario", ErrInvalidCandidate)
		}
		candidate.Scenarios[scenario.ScenarioID] = LockedScenario{ID: scenario.ScenarioID, Path: scenario.Path, SHA256: scenario.SHA256}
	}
	for _, cell := range lock.ResolvedSupportCells {
		if cell.SupportCellID == "" {
			return fmt.Errorf("%w: empty support cell", ErrInvalidCandidate)
		}
		if _, exists := candidate.SupportCells[cell.SupportCellID]; exists {
			return fmt.Errorf("%w: duplicate support cell", ErrInvalidCandidate)
		}
		dimensions := make(map[string]string, len(cell.Dimensions))
		for _, dimension := range cell.Dimensions {
			if dimension.Name == "" || dimension.Version == "" {
				return fmt.Errorf("%w: empty support dimension", ErrInvalidCandidate)
			}
			if _, exists := dimensions[dimension.Name]; exists {
				return fmt.Errorf("%w: duplicate support dimension", ErrInvalidCandidate)
			}
			dimensions[dimension.Name] = dimension.Version
		}
		candidate.SupportCells[cell.SupportCellID] = LockedSupportCell{ID: cell.SupportCellID, Dimensions: dimensions}
	}
	inventoryRoles := make(map[string]string, len(bundle.Artifacts.Artifacts))
	for _, item := range bundle.Artifacts.Artifacts {
		inventoryRoles[string(item.ID)] = item.Role
	}
	for _, artifact := range lock.Artifacts {
		role, known := inventoryRoles[artifact.InventoryID]
		if !known || artifact.ID == "" || artifact.InventoryID == "" || artifact.ReleaseVersion != "0.3.0" || artifact.PackageVersion != "0.3.0" || len(artifact.Payloads) == 0 {
			return fmt.Errorf("%w: unknown or incomplete artifact", ErrInvalidCandidate)
		}
		if _, exists := candidate.Artifacts[artifact.ID]; exists {
			return fmt.Errorf("%w: duplicate artifact ID", ErrInvalidCandidate)
		}
		if _, exists := candidate.ArtifactsByInventoryID[artifact.InventoryID]; exists {
			return fmt.Errorf("%w: duplicate artifact inventory ID", ErrInvalidCandidate)
		}
		result := LockedArtifact{ID: artifact.ID, InventoryID: artifact.InventoryID, Role: role, Payloads: make([]LockedPayload, 0, len(artifact.Payloads))}
		paths := make(map[string]struct{}, len(artifact.Payloads))
		for _, payload := range artifact.Payloads {
			if !validCandidatePath(payload.Path) || payload.MediaType == "" || payload.SizeBytes < 1 || !validSHA256(payload.SHA256) {
				return fmt.Errorf("%w: artifact payload", ErrInvalidCandidate)
			}
			if _, exists := paths[payload.Path]; exists {
				return fmt.Errorf("%w: duplicate artifact payload path", ErrInvalidCandidate)
			}
			paths[payload.Path] = struct{}{}
			result.Payloads = append(result.Payloads, LockedPayload{Path: payload.Path, MediaType: payload.MediaType, SizeBytes: payload.SizeBytes, SHA256: payload.SHA256})
		}
		candidate.Artifacts[result.ID] = result
		candidate.ArtifactsByInventoryID[result.InventoryID] = result
	}
	if err := populateAttestations(candidate, lock.Attestations); err != nil {
		return err
	}
	return nil
}

func loadCommittedScenarioCatalog(ctx context.Context, repoRoot, commit, path string) (map[string]scenarioCatalogEntryDocument, error) {
	data, err := repositoryBlob(ctx, repoRoot, commit, path)
	if err != nil {
		return nil, err
	}
	var document scenarioCatalogDocument
	if err := jsonstrict.Decode(data, &document); err != nil || document.SchemaVersion != 1 || len(document.Scenarios) == 0 {
		return nil, errors.New("scenario catalog is invalid")
	}
	result := make(map[string]scenarioCatalogEntryDocument, len(document.Scenarios))
	paths := make(map[string]struct{}, len(document.Scenarios))
	for _, entry := range document.Scenarios {
		if entry.ScenarioID == "" || !validCandidatePath(entry.Path) || !validSHA256(entry.SHA256) {
			return nil, errors.New("scenario catalog entry is invalid")
		}
		if _, duplicate := result[entry.ScenarioID]; duplicate {
			return nil, errors.New("scenario catalog ID is duplicated")
		}
		if _, duplicate := paths[entry.Path]; duplicate {
			return nil, errors.New("scenario catalog path is duplicated")
		}
		result[entry.ScenarioID] = entry
		paths[entry.Path] = struct{}{}
	}
	return result, nil
}

func populateAttestations(candidate *Candidate, values []Attestation) error {
	if candidate == nil {
		return ErrInvalidCandidate
	}
	seenIDs := make(map[string]struct{}, len(values))
	kindsByArtifact := make(map[string]map[string]struct{}, len(candidate.Artifacts))
	for _, attestation := range values {
		if err := validateAttestation(candidate.Artifacts, attestation); err != nil {
			return err
		}
		if _, exists := seenIDs[attestation.ID]; exists {
			return fmt.Errorf("%w: duplicate attestation ID", ErrInvalidCandidate)
		}
		seenIDs[attestation.ID] = struct{}{}
		kinds := kindsByArtifact[attestation.SubjectArtifactID]
		if kinds == nil {
			kinds = make(map[string]struct{}, 2)
			kindsByArtifact[attestation.SubjectArtifactID] = kinds
		}
		if _, exists := kinds[attestation.Kind]; exists {
			return fmt.Errorf("%w: duplicate %s attestation", ErrInvalidCandidate, attestation.Kind)
		}
		kinds[attestation.Kind] = struct{}{}
		candidate.Attestations = append(candidate.Attestations, cloneAttestation(attestation))
	}
	for _, artifactID := range sortedArtifactIDs(candidate.Artifacts) {
		kinds := kindsByArtifact[artifactID]
		if len(kinds) != 2 {
			return fmt.Errorf("%w: incomplete artifact attestations", ErrInvalidCandidate)
		}
		if _, found := kinds["sbom"]; !found {
			return fmt.Errorf("%w: missing artifact SBOM", ErrInvalidCandidate)
		}
		if _, found := kinds["provenance"]; !found {
			return fmt.Errorf("%w: missing artifact provenance", ErrInvalidCandidate)
		}
	}
	return nil
}

func validateAttestation(artifacts map[string]LockedArtifact, attestation Attestation) error {
	artifact, found := artifacts[attestation.SubjectArtifactID]
	if !found || attestation.ID == "" || !validCandidatePath(attestation.Path) || !validSHA256(attestation.SHA256) {
		return fmt.Errorf("%w: invalid attestation binding", ErrInvalidCandidate)
	}
	if !validAttestationFormat(attestation.Kind, attestation.Format, attestation.MediaType) {
		return fmt.Errorf("%w: attestation format and media type", ErrInvalidCandidate)
	}
	if !validAttestationSubjects(attestation.SubjectPayloads) || !equalAttestationSubjectSets(attestation.SubjectPayloads, subjectsForPayloads(artifact.Payloads)) {
		return fmt.Errorf("%w: attestation payload subjects", ErrInvalidCandidate)
	}
	verification := attestation.SigstoreVerification
	if !validCandidatePath(verification.BundlePath) || verification.BundleMediaType != "application/vnd.dev.sigstore.bundle+json;version=0.3" || !validSHA256(verification.BundleSHA256) || verification.SignedAttestationSHA256 != attestation.SHA256 {
		return fmt.Errorf("%w: Sigstore attestation binding", ErrInvalidCandidate)
	}
	if !validAttestationSubjects(verification.SignedSubjects) || !equalAttestationSubjectSets(verification.SignedSubjects, attestation.SubjectPayloads) {
		return fmt.Errorf("%w: Sigstore signed subjects", ErrInvalidCandidate)
	}
	return nil
}

func validAttestationFormat(kind, format, mediaType string) bool {
	switch kind {
	case "sbom":
		return (format == "spdx-json" && mediaType == "application/spdx+json") ||
			(format == "cyclonedx-json" && mediaType == "application/vnd.cyclonedx+json")
	case "provenance":
		return format == "slsa-provenance-v1" && mediaType == "application/vnd.in-toto+json"
	default:
		return false
	}
}

func validAttestationSubjects(values []AttestationSubject) bool {
	if len(values) == 0 {
		return false
	}
	seen := make(map[string]struct{}, len(values))
	for _, subject := range values {
		if !validCandidatePath(subject.Path) || !validSHA256(subject.SHA256) {
			return false
		}
		key := subject.Path + "\x00" + subject.SHA256
		if _, found := seen[key]; found {
			return false
		}
		seen[key] = struct{}{}
	}
	return true
}

func subjectsForPayloads(payloads []LockedPayload) []AttestationSubject {
	values := make([]AttestationSubject, 0, len(payloads))
	for _, payload := range payloads {
		values = append(values, AttestationSubject{Path: payload.Path, SHA256: payload.SHA256})
	}
	return values
}

func equalAttestationSubjectSets(left, right []AttestationSubject) bool {
	if len(left) != len(right) {
		return false
	}
	leftValues := append([]AttestationSubject(nil), left...)
	rightValues := append([]AttestationSubject(nil), right...)
	sortAttestationSubjects(leftValues)
	sortAttestationSubjects(rightValues)
	for index := range leftValues {
		if leftValues[index] != rightValues[index] {
			return false
		}
	}
	return true
}

func sortAttestationSubjects(values []AttestationSubject) {
	sort.Slice(values, func(left, right int) bool {
		if values[left].Path == values[right].Path {
			return values[left].SHA256 < values[right].SHA256
		}
		return values[left].Path < values[right].Path
	})
}

func cloneAttestation(value Attestation) Attestation {
	result := value
	result.SubjectPayloads = append([]AttestationSubject(nil), value.SubjectPayloads...)
	result.SigstoreVerification.SignedSubjects = append([]AttestationSubject(nil), value.SigstoreVerification.SignedSubjects...)
	return result
}

func verifyCandidateFiles(ctx context.Context, candidate Candidate) error {
	if err := verifyCandidateRoot(candidate); err != nil {
		return err
	}
	seenPaths := map[string]string{candidate.LockPath: "candidate lock"}
	for _, scenarioID := range sortedScenarioIDs(candidate.Scenarios) {
		scenario := candidate.Scenarios[scenarioID]
		if err := contextError(ctx); err != nil {
			return err
		}
		if prior, exists := seenPaths[scenario.Path]; exists {
			return fmt.Errorf("%w: path %q is reused by %s and scenario", ErrInvalidCandidate, scenario.Path, prior)
		}
		seenPaths[scenario.Path] = "scenario"
	}
	for _, artifactID := range sortedArtifactIDs(candidate.Artifacts) {
		artifact := candidate.Artifacts[artifactID]
		for _, payload := range artifact.Payloads {
			if prior, exists := seenPaths[payload.Path]; exists {
				return fmt.Errorf("%w: path %q is reused by %s and artifact", ErrInvalidCandidate, payload.Path, prior)
			}
			seenPaths[payload.Path] = "artifact"
		}
	}
	for _, attestation := range candidate.Attestations {
		if prior, exists := seenPaths[attestation.Path]; exists {
			return fmt.Errorf("%w: path %q is reused by %s and attestation", ErrInvalidCandidate, attestation.Path, prior)
		}
		seenPaths[attestation.Path] = "attestation"
		bundlePath := attestation.SigstoreVerification.BundlePath
		if prior, exists := seenPaths[bundlePath]; exists {
			return fmt.Errorf("%w: path %q is reused by %s and Sigstore bundle", ErrInvalidCandidate, bundlePath, prior)
		}
		seenPaths[bundlePath] = "Sigstore bundle"
	}
	for _, scenarioID := range sortedScenarioIDs(candidate.Scenarios) {
		scenario := candidate.Scenarios[scenarioID]
		data, _, err := readLockedCandidateFile(candidate, scenario.Path)
		if err != nil || sha256Hex(data) != scenario.SHA256 {
			return fmt.Errorf("%w: stale scenario %s", ErrInvalidCandidate, scenario.ID)
		}
	}
	for _, artifactID := range sortedArtifactIDs(candidate.Artifacts) {
		artifact := candidate.Artifacts[artifactID]
		for _, payload := range artifact.Payloads {
			data, _, err := readLockedCandidateFile(candidate, payload.Path)
			if err != nil || int64(len(data)) != payload.SizeBytes || sha256Hex(data) != payload.SHA256 {
				return fmt.Errorf("%w: stale artifact %s payload", ErrInvalidCandidate, artifact.ID)
			}
		}
	}
	for _, attestation := range candidate.Attestations {
		data, _, err := readLockedCandidateFile(candidate, attestation.Path)
		if err != nil || sha256Hex(data) != attestation.SHA256 {
			return fmt.Errorf("%w: stale attestation %s", ErrInvalidCandidate, attestation.ID)
		}
		bundle := attestation.SigstoreVerification
		data, _, err = readLockedCandidateFile(candidate, bundle.BundlePath)
		if err != nil || sha256Hex(data) != bundle.BundleSHA256 {
			return fmt.Errorf("%w: stale Sigstore bundle for attestation %s", ErrInvalidCandidate, attestation.ID)
		}
	}
	if err := verifyCandidateRootFileSet(candidate, seenPaths); err != nil {
		return err
	}
	return nil
}

func verifyCandidateRootFileSet(candidate Candidate, locked map[string]string) error {
	if err := verifyCandidateRoot(candidate); err != nil {
		return err
	}
	allowedDirectories := candidateAllowedDirectories(locked)
	root, _, err := openCandidateRoot(candidate.Root, candidate.rootIdentity)
	if err != nil {
		return fmt.Errorf("%w: open candidate root: %v", ErrInvalidCandidate, err)
	}
	defer root.Close()
	var files []string
	err = filepath.WalkDir(candidate.Root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == candidate.Root {
			return nil
		}
		relative, err := filepath.Rel(candidate.Root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if !validCandidatePath(relative) {
			return errors.New("candidate root path is invalid")
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return errors.New("candidate root contains a symlink")
		}
		if entry.IsDir() {
			if _, allowed := allowedDirectories[relative]; !allowed {
				return fmt.Errorf("candidate root contains an unbound directory %q", relative)
			}
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("candidate root contains a nonregular file %q", relative)
		}
		files = append(files, relative)
		return nil
	})
	if err != nil {
		return fmt.Errorf("%w: candidate root file set: %v", ErrInvalidCandidate, err)
	}
	if err := verifyCandidateRoot(candidate); err != nil {
		return err
	}
	foundLocked := make(map[string]struct{}, len(locked))
	for _, path := range files {
		if _, found := locked[path]; found {
			foundLocked[path] = struct{}{}
			continue
		}
		if !candidateRuntimeFileAllowed(path) {
			return fmt.Errorf("%w: candidate root contains an unbound file %q", ErrInvalidCandidate, path)
		}
	}
	if len(foundLocked) != len(locked) {
		return fmt.Errorf("%w: candidate root file set", ErrInvalidCandidate)
	}
	return nil
}

func candidateAllowedDirectories(locked map[string]string) map[string]struct{} {
	directories := make(map[string]struct{})
	for path := range locked {
		directory := filepath.ToSlash(filepath.Dir(filepath.FromSlash(path)))
		for directory != "." && directory != "" {
			directories[directory] = struct{}{}
			directory = filepath.ToSlash(filepath.Dir(filepath.FromSlash(directory)))
		}
	}
	directories["evidence"] = struct{}{}
	directories[attachmentDirectory] = struct{}{}
	return directories
}

func candidateRuntimeFileAllowed(path string) bool {
	switch path {
	case finalManifestFile, "inventory.json", "inventory.md":
		return true
	}
	if strings.HasPrefix(path, "evidence/") {
		return strings.HasSuffix(path, ".json")
	}
	if strings.HasPrefix(path, attachmentDirectory+"/") {
		return !strings.Contains(strings.TrimPrefix(path, attachmentDirectory+"/"), "/")
	}
	return false
}

func sortedScenarioIDs(values map[string]LockedScenario) []string {
	ids := make([]string, 0, len(values))
	for id := range values {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func sortedArtifactIDs(values map[string]LockedArtifact) []string {
	ids := make([]string, 0, len(values))
	for id := range values {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func readCandidateFile(root, relative string) ([]byte, string, error) {
	return readCandidateFileWithIdentity(root, nil, relative)
}

func readLockedCandidateFile(candidate Candidate, relative string) ([]byte, string, error) {
	return readCandidateFileWithIdentity(candidate.Root, candidate.rootIdentity, relative)
}

func readCandidateFileWithIdentity(root string, expectedRoot fs.FileInfo, relative string) ([]byte, string, error) {
	if !validCandidatePath(relative) {
		return nil, "", errors.New("candidate path is not canonical")
	}
	rootFD, rootPath, err := openCandidateRoot(root, expectedRoot)
	if err != nil {
		return nil, "", err
	}
	defer rootFD.Close()
	if err := rejectSymlinkComponents(rootFD, relative); err != nil {
		return nil, "", err
	}
	file, err := rootFD.Open(filepath.FromSlash(relative))
	if err != nil {
		return nil, "", err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() {
		return nil, "", errors.New("candidate path is not a regular file")
	}
	current, err := rootFD.Lstat(filepath.FromSlash(relative))
	if err != nil || current.Mode()&os.ModeSymlink != 0 || !current.Mode().IsRegular() || !os.SameFile(opened, current) {
		return nil, "", errors.New("candidate path identity changed")
	}
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, "", err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, "", err
	}
	second, err := io.ReadAll(file)
	if err != nil || !bytesEqual(data, second) {
		return nil, "", errors.New("candidate file bytes changed during read")
	}
	if err := verifyOpenedCandidateRoot(rootFD, rootPath, expectedRoot); err != nil {
		return nil, "", err
	}
	return data, relative, nil
}

func candidateRootIdentity(root string) (string, fs.FileInfo, error) {
	rootFD, rootPath, err := openCandidateRoot(root, nil)
	if err != nil {
		return "", nil, err
	}
	defer rootFD.Close()
	identity, err := rootFD.Stat(".")
	if err != nil {
		return "", nil, err
	}
	return rootPath, identity, nil
}

func verifyCandidateRoot(candidate Candidate) error {
	if candidate.Root == "" || candidate.rootIdentity == nil {
		return fmt.Errorf("%w: candidate root identity", ErrInvalidCandidate)
	}
	rootFD, _, err := openCandidateRoot(candidate.Root, candidate.rootIdentity)
	if err != nil {
		return fmt.Errorf("%w: candidate root identity: %v", ErrInvalidCandidate, err)
	}
	return rootFD.Close()
}

func openCandidateRoot(root string, expectedRoot fs.FileInfo) (*os.Root, string, error) {
	rootPath, err := candidateRootPath(root)
	if err != nil {
		return nil, "", err
	}
	rootFD, err := os.OpenRoot(rootPath)
	if err != nil {
		return nil, "", err
	}
	if err := verifyOpenedCandidateRoot(rootFD, rootPath, expectedRoot); err != nil {
		_ = rootFD.Close()
		return nil, "", err
	}
	return rootFD, rootPath, nil
}

func verifyOpenedCandidateRoot(rootFD *os.Root, rootPath string, expectedRoot fs.FileInfo) error {
	if rootFD == nil {
		return errors.New("candidate root is not open")
	}
	opened, err := rootFD.Stat(".")
	if err != nil || !opened.IsDir() {
		return errors.New("candidate root is not a directory")
	}
	current, err := os.Lstat(rootPath)
	if err != nil || current.Mode()&os.ModeSymlink != 0 || !current.IsDir() || !os.SameFile(opened, current) {
		return errors.New("candidate root identity changed")
	}
	if expectedRoot != nil && !os.SameFile(opened, expectedRoot) {
		return errors.New("candidate root identity changed")
	}
	return nil
}

func rejectSymlinkComponents(root *os.Root, relative string) error {
	var current string
	for _, component := range strings.Split(relative, "/") {
		if current == "" {
			current = component
		} else {
			current += "/" + component
		}
		info, err := root.Lstat(filepath.FromSlash(current))
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return errors.New("candidate path contains a symlink")
		}
	}
	return nil
}

func validCandidatePath(path string) bool {
	if path == "" || strings.IndexByte(path, 0) >= 0 || strings.Contains(path, "\\") || filepath.IsAbs(path) || strings.HasPrefix(path, "/") {
		return false
	}
	clean := filepath.ToSlash(filepath.Clean(filepath.FromSlash(path)))
	if clean != path || clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return false
	}
	for _, part := range strings.Split(path, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}

func validCommit(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !(character >= '0' && character <= '9') && !(character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

func validGenerator(value Generator) bool {
	return value.Name != "" && value.Version != "" && validSHA256(value.BinarySHA256)
}

func repositorySourceCommit(ctx context.Context, repoRoot string) (string, error) {
	command := repositoryGitCommand(ctx, repoRoot, "rev-parse", "HEAD")
	command.Dir = repoRoot
	output, err := command.Output()
	if err != nil {
		return "", err
	}
	value := strings.TrimSpace(string(output))
	if !validCommit(value) {
		return "", errors.New("repository commit is invalid")
	}
	return value, nil
}

func verifyRepositorySource(ctx context.Context, repoRoot, expectedCommit string) error {
	if err := repositoryWorktreeClean(ctx, repoRoot); err != nil {
		return err
	}
	commit, err := repositorySourceCommit(ctx, repoRoot)
	if err != nil {
		return err
	}
	if expectedCommit != "" && commit != expectedCommit {
		return errors.New("repository source commit changed")
	}
	makefile, err := readRepositoryMakefile(repoRoot)
	if err != nil {
		return err
	}
	if expectedCommit != "" {
		committed, err := repositoryBlob(ctx, repoRoot, expectedCommit, "Makefile")
		if err != nil || !bytesEqual(makefile, committed) {
			return errors.New("Makefile differs from source commit")
		}
	}
	return nil
}

func repositoryWorktreeClean(ctx context.Context, repoRoot string) error {
	command := repositoryGitCommand(ctx, repoRoot, "status", "--porcelain=v1", "--untracked-files=all", "--ignored=no", "--ignore-submodules=none")
	command.Dir = repoRoot
	command.Stdin = nil
	output, err := command.Output()
	if err != nil {
		return err
	}
	if len(output) != 0 {
		return errors.New("repository worktree is dirty")
	}
	return nil
}

func readRepositoryMakefile(repoRoot string) ([]byte, error) {
	path := filepath.Join(repoRoot, "Makefile")
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil, errors.New("repository Makefile is not a regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	current, err := os.Lstat(path)
	if err != nil || !current.Mode().IsRegular() || !os.SameFile(info, current) {
		return nil, errors.New("repository Makefile identity changed")
	}
	return data, nil
}

func readBoundMakefile(ctx context.Context, candidate Candidate) ([]byte, error) {
	if err := verifyRepositorySource(ctx, candidate.RepoRoot, candidate.SourceCommit); err != nil {
		return nil, fmt.Errorf("%w: repository source: %v", ErrInvalidCandidate, err)
	}
	makefile, err := readRepositoryMakefile(candidate.RepoRoot)
	if err != nil {
		return nil, fmt.Errorf("%w: read Makefile: %v", ErrInvalidCandidate, err)
	}
	return makefile, nil
}

func resolvedRepositoryRoot(repoRoot string) (string, error) {
	if repoRoot == "" || strings.IndexByte(repoRoot, 0) >= 0 {
		return "", errors.New("repository root is invalid")
	}
	absolute, err := filepath.Abs(repoRoot)
	if err != nil {
		return "", err
	}
	real, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(real)
	if err != nil || !info.IsDir() {
		return "", errors.New("repository root is not a directory")
	}
	return filepath.Clean(real), nil
}

func verifyAuthoritativeSources(ctx context.Context, candidate Candidate) error {
	paths := make([]string, 0, 28+len(candidate.Scenarios))
	paths = append(paths, candidateContractPaths(candidate.Contract)...)
	for _, scenarioID := range sortedScenarioIDs(candidate.Scenarios) {
		paths = append(paths, candidate.Scenarios[scenarioID].Path)
	}
	seen := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		if err := contextError(ctx); err != nil {
			return err
		}
		if !validCandidatePath(path) {
			return fmt.Errorf("%w: authoritative source path", ErrInvalidCandidate)
		}
		if _, duplicate := seen[path]; duplicate {
			continue
		}
		seen[path] = struct{}{}
		current, err := os.ReadFile(filepath.Join(candidate.RepoRoot, filepath.FromSlash(path)))
		if err != nil {
			return fmt.Errorf("%w: read authoritative source %q: %v", ErrInvalidCandidate, path, err)
		}
		committed, err := repositoryBlob(ctx, candidate.RepoRoot, candidate.SourceCommit, path)
		if err != nil || !bytesEqual(current, committed) {
			return fmt.Errorf("%w: authoritative source drift %q", ErrInvalidCandidate, path)
		}
	}
	return nil
}

func candidateContractPaths(value lockedContract) []string {
	paths := []string{value.Requirements.Path, value.SupportMatrix.Path}
	for _, binding := range value.BehavioralFiles {
		paths = append(paths, binding.Path)
	}
	paths = append(paths,
		value.VerificationInput.ScenarioCatalog.Path,
		value.VerificationInput.VectorCatalog.Path,
		value.VerificationInput.FaultCatalog.Path,
		value.VerificationInput.PerformanceBudgets.Path,
		value.VerificationInput.ArtifactInventory.Path,
		value.SchemaFiles.Requirements.Path,
		value.SchemaFiles.SupportMatrix.Path,
		value.SchemaFiles.Scenario.Path,
		value.SchemaFiles.Evidence.Path,
		value.SchemaFiles.RCCandidateLock.Path,
		value.SchemaFiles.RCManifest.Path,
		value.SchemaFiles.FaultCatalog.Path,
		value.SchemaFiles.ArtifactInventory.Path,
		value.SchemaFiles.PerformanceBudgets.Path,
		value.SchemaFiles.VectorCatalog.Path,
	)
	return paths
}

func repositoryBlob(ctx context.Context, repoRoot, commit, path string) ([]byte, error) {
	if !validCommit(commit) || !validCandidatePath(path) {
		return nil, errors.New("repository blob binding is invalid")
	}
	command := repositoryGitCommand(ctx, repoRoot, "--no-pager", "show", commit+":"+path)
	command.Dir = repoRoot
	command.Stdin = nil
	output, err := command.Output()
	if err != nil {
		return nil, err
	}
	return output, nil
}

func repositoryGitCommand(ctx context.Context, repoRoot string, arguments ...string) *exec.Cmd {
	command := exec.CommandContext(ctx, "/usr/bin/git", arguments...)
	command.Dir = repoRoot
	command.Stdin = nil
	environment := make([]string, 0, len(os.Environ())+4)
	for _, value := range os.Environ() {
		name, _, found := strings.Cut(value, "=")
		if !found || name == "PATH" || strings.HasPrefix(name, "DYLD_") || strings.HasPrefix(name, "GIT_") || strings.HasPrefix(name, "LD_") {
			continue
		}
		environment = append(environment, value)
	}
	command.Env = append(environment,
		"PATH=/usr/bin:/bin:/usr/sbin:/sbin",
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_NO_REPLACE_OBJECTS=1",
		"GIT_OPTIONAL_LOCKS=0",
		"GIT_TERMINAL_PROMPT=0",
	)
	return command
}

func sha256Hex(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func rejectDeclaredVerified(data []byte) error {
	var value any
	if err := jsonstrict.Decode(data, &value); err != nil {
		return fmt.Errorf("%w: decode candidate claimed status: %v", ErrInvalidCandidate, err)
	}
	if containsVerifiedKey(value) {
		return fmt.Errorf("%w: candidate declares verified status", ErrInvalidCandidate)
	}
	return nil
}

func containsVerifiedKey(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			normalized := strings.ReplaceAll(strings.ToLower(key), "_", "-")
			if normalized == "verified" || normalized == "verification-status" || normalized == "trust-disposition" {
				return true
			}
			if containsVerifiedKey(child) {
				return true
			}
		}
	case []any:
		for _, child := range typed {
			if containsVerifiedKey(child) {
				return true
			}
		}
	}
	return false
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	return ctx.Err()
}

func bytesEqual(left, right []byte) bool {
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
