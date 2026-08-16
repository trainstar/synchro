// Package evidence constructs and verifies immutable candidate evidence.
package evidence

import (
	"errors"
	"io/fs"
	"sync"
	"time"

	"github.com/trainstar/synchro/conformance/execution"
)

const (
	candidateLockFile = "rc-candidate-lock.json"
	finalManifestFile = "rc-manifest.json"
)

var (
	// ErrInvalidStore reports an unsafe attachment store configuration.
	ErrInvalidStore = errors.New("evidence attachment store is invalid")
	// ErrDuplicateAttachment reports an attempted attachment path reuse.
	ErrDuplicateAttachment = errors.New("evidence attachment already exists")
	// ErrInvalidAttachment reports changed, unsafe, or unbound attachment bytes.
	ErrInvalidAttachment = errors.New("evidence attachment is invalid")
	// ErrInvalidCandidate reports an unsafe or incomplete candidate bundle.
	ErrInvalidCandidate = errors.New("evidence candidate is invalid")
	// ErrInvalidEvidence reports an invalid evidence-v2 projection.
	ErrInvalidEvidence = errors.New("evidence record is invalid")
	// ErrIncompleteCandidate reports missing full release closure.
	ErrIncompleteCandidate = errors.New("candidate full closure is incomplete")
	// ErrNotImplemented reports a typed unavailable optional command.
	ErrNotImplemented = errors.New("operation is not implemented")
)

// Attachment is one immutable content-addressed stored file.
type Attachment struct {
	ID        string `json:"id"`
	Kind      string `json:"kind"`
	Path      string `json:"path"`
	MediaType string `json:"media_type"`
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
}

// Store publishes attachments beneath one candidate-root confined directory.
type Store struct {
	Root         string
	rootIdentity fs.FileInfo
}

// Candidate is the verified immutable pre-execution lock state.
type Candidate struct {
	RepoRoot               string
	Root                   string
	LockPath               string
	LockSHA256             string
	RunnerDigest           string
	ID                     string
	ReleaseVersion         string
	ProtocolVersion        int
	SourceCommit           string
	RepositorySourceCommit string
	ContractSnapshotSHA256 string
	Generator              Generator
	Contract               lockedContract
	TrustedRerunApprovers  map[string]struct{}
	Scenarios              map[string]LockedScenario
	SupportCells           map[string]LockedSupportCell
	Artifacts              map[string]LockedArtifact
	ArtifactsByInventoryID map[string]LockedArtifact
	Attestations           []Attestation
	rootIdentity           fs.FileInfo
}

// LockedScenario binds one candidate scenario source file.
type LockedScenario struct {
	ID     string
	Path   string
	SHA256 string
}

// LockedSupportCell binds one finite exact environment.
type LockedSupportCell struct {
	ID         string
	Dimensions map[string]string
}

// LockedArtifact resolves one exact realized artifact inventory item.
type LockedArtifact struct {
	ID          string
	InventoryID string
	Role        string
	Payloads    []LockedPayload
}

// LockedPayload binds one realized artifact payload.
type LockedPayload struct {
	Path      string
	MediaType string
	SizeBytes int64
	SHA256    string
}

// Attestation binds one typed attestation file to one complete artifact.
type Attestation struct {
	ID                   string               `json:"id"`
	Kind                 string               `json:"kind"`
	Format               string               `json:"format"`
	MediaType            string               `json:"media_type"`
	SubjectArtifactID    string               `json:"subject_artifact_id"`
	SubjectPayloads      []AttestationSubject `json:"subject_payloads"`
	Path                 string               `json:"path"`
	SHA256               string               `json:"sha256"`
	SigstoreVerification SigstoreVerification `json:"sigstore_verification"`
}

// AttestationSubject identifies one payload covered by an attestation.
type AttestationSubject struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

// SigstoreVerification binds an attestation to its serialized bundle.
// Phase 6 owns signature trust. This package checks only locked bytes and metadata.
type SigstoreVerification struct {
	BundlePath              string               `json:"bundle_path"`
	BundleMediaType         string               `json:"bundle_media_type"`
	BundleSHA256            string               `json:"bundle_sha256"`
	SignedAttestationSHA256 string               `json:"signed_attestation_sha256"`
	SignedSubjects          []AttestationSubject `json:"signed_subjects"`
	CertificateIssuer       string               `json:"certificate_issuer"`
	CertificateIdentity     string               `json:"certificate_identity"`
	Verifier                SigstoreVerifier     `json:"verifier"`
	VerifiedAt              string               `json:"verified_at"`
	VerificationURI         string               `json:"verification_uri"`
}

// SigstoreVerifier identifies the pre-authored verifier metadata.
type SigstoreVerifier struct {
	Name         string `json:"name"`
	Version      string `json:"version"`
	BinarySHA256 string `json:"binary_sha256"`
}

// BuilderConfig supplies immutable candidate state and the trusted runner capability.
type BuilderConfig struct {
	RepoRoot            string
	CandidateRoot       string
	RunnerAuthorization execution.RunnerAuthorization
	Generator           Generator
}

// Generator identifies the evidence producer without secret data.
type Generator struct {
	Name         string `json:"name"`
	Version      string `json:"version"`
	BinarySHA256 string `json:"binary_sha256"`
}

// Builder creates one evidence-v2 document only from a completed receipt.
type Builder struct {
	cfg               BuilderConfig
	candidate         Candidate
	issuer            execution.ReceiptIssuer
	commandCapability execution.CommandCapability
	mu                sync.Mutex
	consumed          map[string]struct{}
}

// Evidence is a typed evidence-v2 projection.
type Evidence struct {
	SchemaURI                  string                                `json:"$schema"`
	SchemaVersion              int                                   `json:"schema_version"`
	EvidenceID                 string                                `json:"evidence_id"`
	ReceiptID                  string                                `json:"receipt_id"`
	EvidenceClass              execution.EvidenceClass               `json:"evidence_class"`
	CandidateID                string                                `json:"candidate_id"`
	ReleaseVersion             string                                `json:"release_version"`
	ProtocolVersion            int                                   `json:"protocol_version"`
	ContractSnapshotSHA256     string                                `json:"contract_snapshot_sha256"`
	SupportCellID              *string                               `json:"support_cell_id"`
	ScenarioID                 string                                `json:"scenario_id"`
	ProofObligationID          string                                `json:"proof_obligation_id"`
	RequirementIDs             []string                              `json:"requirement_ids"`
	ProofType                  string                                `json:"proof_type"`
	SourceCommit               string                                `json:"source_commit"`
	Generator                  Generator                             `json:"generator"`
	Run                        Run                                   `json:"run"`
	Environment                []execution.EnvironmentDimension      `json:"environment"`
	Assertions                 []execution.AssertionResult           `json:"assertions"`
	Attachments                []Attachment                          `json:"attachments"`
	AttachmentIDs              []string                              `json:"attachment_ids"`
	ExecutionArtifacts         execution.ExecutionArtifacts          `json:"execution_artifacts"`
	Replay                     execution.ReplayEvidence              `json:"replay"`
	FaultExecution             *execution.FaultExecution             `json:"fault_execution"`
	PerformanceResults         []execution.PerformanceResult         `json:"performance_results"`
	RequiredMeasurementResults []execution.RequiredMeasurementResult `json:"required_measurement_results"`
	VectorResults              []execution.VectorResult              `json:"vector_results"`
	ArtifactBindings           []execution.ArtifactBinding           `json:"artifact_bindings"`
	HTTPObservations           []execution.HTTPObservation           `json:"http_observations"`
	Counters                   execution.Counters                    `json:"counters"`
	Observations               []execution.Observation               `json:"observations"`
	NegativeControl            *execution.NegativeControl            `json:"negative_control"`
	Seed                       *string                               `json:"seed"`
	RunnerDigest               string                                `json:"runner_digest"`
	Receipt                    ReceiptProjection                     `json:"receipt"`
}

// Run is the public run projection from one authenticated receipt.
type Run struct {
	ID                 string                       `json:"id"`
	ExecutionLineageID string                       `json:"execution_lineage_id"`
	URL                string                       `json:"url"`
	MakeTarget         string                       `json:"make_target"`
	Argv               []string                     `json:"argv"`
	Attempt            int                          `json:"attempt"`
	StartedAt          time.Time                    `json:"started_at"`
	CompletedAt        time.Time                    `json:"completed_at"`
	DurationMS         int64                        `json:"duration_ms"`
	Result             execution.Result             `json:"result"`
	ExitCode           int                          `json:"exit_code"`
	Command            execution.CommandObservation `json:"command_observation"`
	PreviousEvidenceID *string                      `json:"previous_evidence_id"`
	RerunCause         *string                      `json:"rerun_cause"`
	RerunDiagnosis     *string                      `json:"rerun_diagnosis"`
	CorrectiveAction   *string                      `json:"corrective_action"`
	RerunApproval      *execution.RerunApproval     `json:"rerun_approval"`
}

// ReceiptProjection is the serializable completed receipt proof.
type ReceiptProjection struct {
	Fields         execution.ReceiptFields         `json:"fields"`
	Authentication execution.ReceiptAuthentication `json:"authentication"`
}

// Phase6Verifier supplies the later cryptographic trust decision.
// It must not be replaced by a candidate-declared status field.
type Phase6Verifier interface {
	VerifyPhase6(ctx Context, repoRoot string, candidate Candidate, manifest FinalManifest) error
}

// Context permits this package to expose a narrow verifier interface.
// context.Context satisfies it directly.
type Context interface {
	Done() <-chan struct{}
	Err() error
}

// FinalManifest is the post-execution candidate manifest projection.
type FinalManifest struct {
	CandidateID            string
	RunnerDigest           string
	SourceCommit           string
	CandidateLock          FileBinding
	ContractSnapshotSHA256 string
	Generator              Generator
	Contract               lockedContract
	Scenarios              []LockedScenario
	Evidence               []EvidenceReference
	SupportCells           []LockedSupportCell
	Artifacts              []LockedArtifact
	Attestations           []Attestation
	TrustedRerunApprovers  []string
}

// FileBinding binds one candidate-relative regular file.
type FileBinding struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

// EvidenceReference is one final manifest evidence binding.
type EvidenceReference struct {
	EvidenceID        string  `json:"evidence_id"`
	ScenarioID        string  `json:"scenario_id"`
	ProofObligationID string  `json:"proof_obligation_id"`
	SupportCellID     *string `json:"support_cell_id"`
	ProofType         string  `json:"proof_type"`
	Path              string  `json:"path"`
	SHA256            string  `json:"sha256"`
}
