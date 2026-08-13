// Package execution defines opaque receipts for completed conformance runs.
package execution

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

const receiptSignatureDomain = "synchro:conformance:receipt:v1"

var (
	// ErrInvalidIssuer reports an incomplete or mismatched receipt issuer.
	ErrInvalidIssuer = errors.New("receipt issuer is invalid")
	// ErrIssuerUsed reports a second completion attempt with one issuer.
	ErrIssuerUsed = errors.New("receipt issuer was already used")
	// ErrInvalidCompletion reports fields or a signature that cannot complete a receipt.
	ErrInvalidCompletion = errors.New("receipt completion is invalid")
	// ErrInvalidReceipt reports a zero, changed, or unauthenticated receipt.
	ErrInvalidReceipt = errors.New("receipt is invalid")
	// ErrReceiptConsumed reports a second evidence claim for one receipt.
	ErrReceiptConsumed = errors.New("receipt was already consumed")
)

// Result is the closed terminal result of one run.
type Result string

const (
	ResultPassed Result = "passed"
	ResultFailed Result = "failed"
	ResultError  Result = "error"
)

// AssertionResult binds one authored assertion to its terminal outcome.
type AssertionResult struct {
	AssertionID string `json:"assertion_id"`
	Outcome     string `json:"outcome"`
	// Detail remains source-compatible with older runners. Receipts and evidence
	// never serialize it because free-form details can contain user data.
	Detail string `json:"-"`
}

// VectorResult is the closed vector result carried by a receipt.
type VectorResult struct {
	VectorSetID        string `json:"vector_set_id"`
	SourceSHA256       string `json:"source_sha256"`
	AggregateSHA256    string `json:"aggregate_sha256"`
	Language           string `json:"language"`
	ArtifactID         string `json:"artifact_id"`
	Outcome            string `json:"outcome"`
	ResultAttachmentID string `json:"result_attachment_id"`
	ExecutedCount      int    `json:"executed_count"`
	PassedCount        int    `json:"passed_count"`
	FailedCount        int    `json:"failed_count"`
}

// ArtifactBinding resolves one inventory definition to an executed artifact.
type ArtifactBinding struct {
	InventoryID string `json:"inventory_id"`
	ArtifactID  string `json:"artifact_id"`
	Role        string `json:"role,omitempty"`
	Path        string `json:"path"`
	MediaType   string `json:"media_type,omitempty"`
	Size        int64  `json:"size_bytes"`
	SHA256      string `json:"sha256"`

	// SizeBytes is accepted by newer callers. Size remains the serialized field
	// so existing runners remain source-compatible.
	SizeBytes int64 `json:"-"`
}

// ArtifactPayload binds a runner artifact declaration to the bytes used to
// derive its identity. Callers must safely open the payload before use.
type ArtifactPayload struct {
	Binding ArtifactBinding
	Bytes   []byte
}

// EnvironmentDimension is one allowlisted support-cell dimension.
type EnvironmentDimension struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// Attachment describes one content-addressed candidate attachment.
type Attachment struct {
	ID        string `json:"id"`
	Kind      string `json:"kind"`
	Path      string `json:"path"`
	MediaType string `json:"media_type"`
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
}

// AttachmentPublisher stores one sanitized execution attachment for a receipt.
// The runner receives this narrow capability from the candidate evidence owner.
type AttachmentPublisher interface {
	Publish(kind, mediaType string, data []byte) (Attachment, error)
}

// HTTPHeader records one bounded allowlisted response header.
type HTTPHeader struct {
	Name   string   `json:"name"`
	Values []string `json:"values"`
}

// HTTPObservation records bounded transport metadata without request or
// response bodies, body digests, credentials, or payloads.
type HTTPObservation struct {
	RequestClass        string       `json:"request_class"`
	Status              int          `json:"status"`
	Headers             []HTTPHeader `json:"headers"`
	DurationNanoseconds int64        `json:"duration_nanoseconds"`
}

// RequestCounts records the finite request classes used by performance proof.
type RequestCounts struct {
	Connect     int `json:"connect"`
	Push        int `json:"push"`
	Pull        int `json:"pull"`
	RebuildPage int `json:"rebuild_page"`
	SchemaFetch int `json:"schema_fetch"`
	Other       int `json:"other"`
}

// Counters contains bounded run counters.
type Counters struct {
	RequestCounts            RequestCounts `json:"request_counts"`
	ReturnedRebuildPageCount int           `json:"returned_rebuild_page_count"`
	OutboundNetworkOrRPCHops int           `json:"outbound_network_or_rpc_hops"`
}

// Observation is one bounded non-payload observation.
type Observation struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// ExecutionArtifacts binds typed attachment roles to a receipt.
type ExecutionArtifacts struct {
	LogAttachmentIDs          []string `json:"log_attachment_ids"`
	TraceAttachmentIDs        []string `json:"trace_attachment_ids"`
	ReplayDataAttachmentIDs   []string `json:"replay_data_attachment_ids"`
	BarrierTraceAttachmentIDs []string `json:"barrier_trace_attachment_ids"`
}

// BarrierTrace binds an authored barrier to its trace attachment.
type BarrierTrace struct {
	BarrierID    string `json:"barrier_id"`
	AttachmentID string `json:"attachment_id"`
}

// ReplayEvidence contains replay-safe execution bindings.
type ReplayEvidence struct {
	Seed          *string        `json:"seed"`
	BarrierTraces []BarrierTrace `json:"barrier_traces"`
}

// InjectionParameters identifies an authored fault recipe without payloads.
type InjectionParameters struct {
	Scenario     string `json:"scenario"`
	Defect       string `json:"defect"`
	Precondition string `json:"precondition,omitempty"`
}

// InjectionRecipe is one typed fault recipe.
type InjectionRecipe struct {
	Mechanism  string              `json:"mechanism"`
	Target     string              `json:"target"`
	Operator   string              `json:"operator"`
	Parameters InjectionParameters `json:"parameters"`
}

// FaultExecution binds a completed fault-bearing receipt to its authored plan.
type FaultExecution struct {
	FaultPlanID           string          `json:"fault_plan_id"`
	FaultID               string          `json:"fault_id"`
	ControlID             string          `json:"control_id"`
	FaultPlanAttachmentID string          `json:"fault_plan_attachment_id"`
	SubjectType           string          `json:"subject_type"`
	DetectedBy            []string        `json:"detected_by"`
	Injection             InjectionRecipe `json:"injection"`
}

// DataProfile is the frozen workload shape for one performance result.
type DataProfile struct {
	ProfileType string          `json:"profile_type"`
	Parameters  json.RawMessage `json:"parameters"`
}

// MeasurementMethod records the fixed measurement mechanism.
type MeasurementMethod struct {
	MethodType      string `json:"method_type"`
	Instrumentation string `json:"instrumentation"`
	Aggregation     string `json:"aggregation"`
}

// PerformanceMeasurement contains request and hop counters for one budget.
type PerformanceMeasurement struct {
	RequestCounts            RequestCounts `json:"request_counts"`
	ReturnedRebuildPageCount int           `json:"returned_rebuild_page_count"`
	OutboundNetworkOrRPCHops int           `json:"outbound_network_or_rpc_hops"`
}

// PerformanceResult is one typed pre-authored budget result.
type PerformanceResult struct {
	BudgetID                string                 `json:"budget_id"`
	Outcome                 string                 `json:"outcome"`
	MeasurementAttachmentID string                 `json:"measurement_attachment_id"`
	Metric                  string                 `json:"metric"`
	Unit                    string                 `json:"unit"`
	Comparator              string                 `json:"comparator"`
	Limit                   float64                `json:"limit"`
	ObservedValue           float64                `json:"observed_value"`
	Measurement             PerformanceMeasurement `json:"measurement"`
	DataProfile             DataProfile            `json:"data_profile"`
	MeasurementMethod       MeasurementMethod      `json:"measurement_method"`
}

// Metric identifies one required characterization value.
type Metric struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Unit string `json:"unit"`
}

// MetricValue records one numeric observation.
type MetricValue struct {
	MetricID string  `json:"metric_id"`
	Value    float64 `json:"value"`
}

// MeasurementObservation records one named sample.
type MeasurementObservation struct {
	SampleID     string        `json:"sample_id"`
	MetricValues []MetricValue `json:"metric_values"`
}

// StratumResult records all samples for one authored stratum.
type StratumResult struct {
	StratumID    string                   `json:"stratum_id"`
	Parameters   json.RawMessage          `json:"parameters"`
	SampleCount  int                      `json:"sample_count"`
	Observations []MeasurementObservation `json:"observations"`
}

// RequiredMeasurementResult records one required characterization result.
type RequiredMeasurementResult struct {
	MeasurementID           string            `json:"measurement_id"`
	Outcome                 string            `json:"outcome"`
	MeasurementAttachmentID string            `json:"measurement_attachment_id"`
	DataProfile             DataProfile       `json:"data_profile"`
	MeasurementMethod       MeasurementMethod `json:"measurement_method"`
	Metrics                 []Metric          `json:"metrics"`
	Strata                  []StratumResult   `json:"strata"`
}

// NegativeControl records a completed intentional-defect proof.
type NegativeControl struct {
	FaultID                   string   `json:"fault_id"`
	ControlID                 string   `json:"control_id"`
	FaultPlanID               string   `json:"fault_plan_id"`
	FaultPlanAttachmentID     string   `json:"fault_plan_attachment_id"`
	ControlSubjectID          string   `json:"control_subject_id"`
	ControlSubjectType        string   `json:"control_subject_type"`
	ControlSubjectArtifactIDs []string `json:"control_subject_artifact_ids"`
	DetectedBy                []string `json:"detected_by"`
	Outcome                   string   `json:"outcome"`
	AttachmentIDs             []string `json:"attachment_ids"`
}

// RerunApproval is the fixed authorization record for an infrastructure rerun.
type RerunApproval struct {
	ApproverIdentity string    `json:"approver_identity"`
	ApprovedAt       time.Time `json:"approved_at"`
	URI              string    `json:"uri"`
}

// ReceiptAuthentication is the public verification material for a completed
// receipt projection. It contains no private signing material.
type ReceiptAuthentication struct {
	RunnerPublicKey string `json:"runner_public_key"`
	Nonce           string `json:"nonce"`
	Signature       string `json:"signature"`
}

// ReceiptFields is a defensive copy of the exact receipt data fields.
// A ReceiptFields value is not evidence of execution.
type ReceiptFields struct {
	ReceiptID              string                 `json:"receipt_id"`
	ScenarioID             string                 `json:"scenario_id"`
	ProofObligationID      string                 `json:"proof_obligation_id"`
	MakeTarget             string                 `json:"make_target"`
	Argv                   []string               `json:"argv"`
	StartedAt              time.Time              `json:"started_at"`
	CompletedAt            time.Time              `json:"completed_at"`
	ExitCode               int                    `json:"exit_code"`
	Result                 Result                 `json:"result"`
	Command                CommandObservation     `json:"command_observation"`
	Assertions             []AssertionResult      `json:"assertions"`
	VectorResults          []VectorResult         `json:"vector_results"`
	ArtifactBindings       []ArtifactBinding      `json:"artifact_bindings"`
	EnvironmentDimensions  []EnvironmentDimension `json:"environment_dimensions"`
	AttachmentIDs          []string               `json:"attachment_ids"`
	RunnerDigest           string                 `json:"runner_digest"`
	CandidateLockSHA256    string                 `json:"candidate_lock_sha256"`
	RunnerArtifactSHA256   string                 `json:"runner_artifact_sha256"`
	RunnerExecutableSHA256 string                 `json:"runner_executable_sha256"`
	GeneratorName          string                 `json:"generator_name"`
	GeneratorVersion       string                 `json:"generator_version"`
	GeneratorBinarySHA256  string                 `json:"generator_binary_sha256"`

	RunID                string                      `json:"run_id"`
	ExecutionLineageID   string                      `json:"execution_lineage_id"`
	RunURL               string                      `json:"run_url"`
	Attempt              int                         `json:"attempt"`
	PreviousEvidenceID   *string                     `json:"previous_evidence_id"`
	RerunCause           *string                     `json:"rerun_cause"`
	RerunDiagnosis       *string                     `json:"rerun_diagnosis"`
	CorrectiveAction     *string                     `json:"corrective_action"`
	RerunApproval        *RerunApproval              `json:"rerun_approval"`
	Attachments          []Attachment                `json:"attachments"`
	HTTPObservations     []HTTPObservation           `json:"http_observations"`
	Counters             *Counters                   `json:"counters"`
	Observations         []Observation               `json:"observations"`
	ExecutionArtifacts   *ExecutionArtifacts         `json:"execution_artifacts"`
	Replay               *ReplayEvidence             `json:"replay"`
	FaultExecution       *FaultExecution             `json:"fault_execution"`
	PerformanceResults   []PerformanceResult         `json:"performance_results"`
	RequiredMeasurements []RequiredMeasurementResult `json:"required_measurement_results"`
	NegativeControl      *NegativeControl            `json:"negative_control"`
	Seed                 *string                     `json:"seed"`
}

func normalizeReceiptFields(source ReceiptFields) ReceiptFields {
	result := cloneReceiptFields(source)
	result.Argv = nonNilSlice(result.Argv)
	result.Assertions = nonNilSlice(result.Assertions)
	result.VectorResults = nonNilSlice(result.VectorResults)
	result.ArtifactBindings = nonNilSlice(result.ArtifactBindings)
	result.EnvironmentDimensions = nonNilSlice(result.EnvironmentDimensions)
	result.AttachmentIDs = nonNilSlice(result.AttachmentIDs)
	result.Attachments = nonNilSlice(result.Attachments)
	result.HTTPObservations = nonNilSlice(result.HTTPObservations)
	result.Observations = nonNilSlice(result.Observations)
	result.PerformanceResults = nonNilSlice(result.PerformanceResults)
	result.RequiredMeasurements = nonNilSlice(result.RequiredMeasurements)
	if result.Attachments != nil {
		result.Attachments = append([]Attachment(nil), result.Attachments...)
	}
	if result.HTTPObservations != nil {
		for index := range result.HTTPObservations {
			result.HTTPObservations[index].Headers = nonNilSlice(result.HTTPObservations[index].Headers)
			for headerIndex := range result.HTTPObservations[index].Headers {
				result.HTTPObservations[index].Headers[headerIndex].Values = nonNilSlice(result.HTTPObservations[index].Headers[headerIndex].Values)
			}
		}
	}
	if result.ExecutionArtifacts != nil {
		result.ExecutionArtifacts.LogAttachmentIDs = nonNilSlice(result.ExecutionArtifacts.LogAttachmentIDs)
		result.ExecutionArtifacts.TraceAttachmentIDs = nonNilSlice(result.ExecutionArtifacts.TraceAttachmentIDs)
		result.ExecutionArtifacts.ReplayDataAttachmentIDs = nonNilSlice(result.ExecutionArtifacts.ReplayDataAttachmentIDs)
		result.ExecutionArtifacts.BarrierTraceAttachmentIDs = nonNilSlice(result.ExecutionArtifacts.BarrierTraceAttachmentIDs)
	}
	if result.Replay != nil {
		result.Replay.BarrierTraces = nonNilSlice(result.Replay.BarrierTraces)
	}
	if result.FaultExecution != nil {
		result.FaultExecution.DetectedBy = nonNilSlice(result.FaultExecution.DetectedBy)
	}
	if result.NegativeControl != nil {
		result.NegativeControl.ControlSubjectArtifactIDs = nonNilSlice(result.NegativeControl.ControlSubjectArtifactIDs)
		result.NegativeControl.DetectedBy = nonNilSlice(result.NegativeControl.DetectedBy)
		result.NegativeControl.AttachmentIDs = nonNilSlice(result.NegativeControl.AttachmentIDs)
	}
	for index := range result.RequiredMeasurements {
		result.RequiredMeasurements[index].Metrics = nonNilSlice(result.RequiredMeasurements[index].Metrics)
		result.RequiredMeasurements[index].Strata = nonNilSlice(result.RequiredMeasurements[index].Strata)
		for stratumIndex := range result.RequiredMeasurements[index].Strata {
			stratum := &result.RequiredMeasurements[index].Strata[stratumIndex]
			stratum.Observations = nonNilSlice(stratum.Observations)
			for observationIndex := range stratum.Observations {
				stratum.Observations[observationIndex].MetricValues = nonNilSlice(stratum.Observations[observationIndex].MetricValues)
			}
		}
	}
	return result
}

func nonNilSlice[T any](source []T) []T {
	if source == nil {
		return make([]T, 0)
	}
	return source
}

// Receipt is immutable execution evidence. Its zero value is invalid.
type Receipt struct {
	fields ReceiptFields
	seal   *receiptSeal
	origin *issuerState
}

// ReceiptIssuer is an opaque, single-use completion challenge.
// Possession of an issuer does not provide the runner signing key.
type ReceiptIssuer struct {
	state *issuerState
}

// TrustedRunner owns the private key used to authenticate completed receipts.
// It is the only object that can create a RunnerAuthorization capability.
type TrustedRunner struct {
	privateKey ed25519.PrivateKey
}

// RunnerAuthorization is a process-local capability for one trusted runner.
// Its unexported state prevents public-key-only callers from creating it.
type RunnerAuthorization struct {
	state *runnerAuthorizationState
}

// GeneratorIdentity identifies the evidence generator bound to a receipt.
type GeneratorIdentity struct {
	Name         string
	Version      string
	BinarySHA256 string
}

type runnerAuthorizationState struct {
	verifier               ed25519.PublicKey
	runnerArtifactSHA256   string
	runnerExecutableSHA256 string
}

// Completion contains validated fields that await the runner signature.
type Completion struct {
	issuer  *issuerState
	fields  ReceiptFields
	message []byte
}

type issuerState struct {
	mu                     sync.Mutex
	verifier               ed25519.PublicKey
	nonce                  [32]byte
	runnerDigest           string
	candidateLockSHA256    string
	runnerArtifactSHA256   string
	runnerExecutableSHA256 string
	generator              GeneratorIdentity
	command                *commandCapabilityState
	used                   bool
}

type receiptSeal struct {
	mu        sync.Mutex
	verifier  ed25519.PublicKey
	nonce     [32]byte
	signature []byte
	consumed  bool
}

// NewTrustedRunner creates a trusted runner object from its private key.
func NewTrustedRunner(privateKey []byte) (TrustedRunner, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return TrustedRunner{}, fmt.Errorf("%w: private key length", ErrInvalidIssuer)
	}
	return TrustedRunner{privateKey: append(ed25519.PrivateKey(nil), privateKey...)}, nil
}

// RunnerDigest returns the public runner identity without exposing signing material.
func (r TrustedRunner) RunnerDigest() string {
	if len(r.privateKey) != ed25519.PrivateKeySize {
		return ""
	}
	publicKey, ok := r.privateKey.Public().(ed25519.PublicKey)
	if !ok || len(publicKey) != ed25519.PublicKeySize {
		return ""
	}
	digest := sha256.Sum256(publicKey)
	return hex.EncodeToString(digest[:])
}

// NewReceiptIssuer creates a legacy diagnostic issuer for this trusted runner.
func (r TrustedRunner) NewReceiptIssuer() (ReceiptIssuer, error) {
	if len(r.privateKey) != ed25519.PrivateKeySize {
		return ReceiptIssuer{}, ErrInvalidIssuer
	}
	publicKey, ok := r.privateKey.Public().(ed25519.PublicKey)
	if !ok || len(publicKey) != ed25519.PublicKeySize {
		return ReceiptIssuer{}, ErrInvalidIssuer
	}
	return newReceiptIssuer(append(ed25519.PublicKey(nil), publicKey...), "", "", GeneratorIdentity{})
}

// CompleteReceipt signs and completes a prepared receipt for the matching issuer.
func (r TrustedRunner) CompleteReceipt(issuer ReceiptIssuer, completion Completion) (Receipt, error) {
	if r.RunnerDigest() == "" || issuer.RunnerDigest() != r.RunnerDigest() {
		return Receipt{}, fmt.Errorf("%w: trusted runner binding", ErrInvalidCompletion)
	}
	return CompleteReceipt(issuer, completion, ed25519.Sign(r.privateKey, completion.SigningBytes()))
}

// RunnerArtifactDigest returns the deterministic digest of all locked
// conformance-runner payload bindings. Callers must provide every binding.
func RunnerArtifactDigest(bindings []ArtifactBinding) (string, error) {
	if len(bindings) == 0 {
		return "", fmt.Errorf("%w: runner artifact bindings", ErrInvalidIssuer)
	}
	type binding struct {
		InventoryID string `json:"inventory_id"`
		ArtifactID  string `json:"artifact_id"`
		Role        string `json:"role"`
		Path        string `json:"path"`
		MediaType   string `json:"media_type"`
		SizeBytes   int64  `json:"size_bytes"`
		SHA256      string `json:"sha256"`
	}
	canonical := make([]binding, 0, len(bindings))
	seen := make(map[string]struct{}, len(bindings))
	for _, value := range bindings {
		size := value.Size
		if value.SizeBytes != 0 {
			if value.Size != 0 && value.Size != value.SizeBytes {
				return "", fmt.Errorf("%w: runner artifact size", ErrInvalidIssuer)
			}
			size = value.SizeBytes
		}
		if value.InventoryID == "" || value.ArtifactID == "" || value.Role != "conformance-runner" || value.Path == "" || value.MediaType == "" || size < 1 || !validLowerHex(value.SHA256, sha256.Size) {
			return "", fmt.Errorf("%w: runner artifact binding", ErrInvalidIssuer)
		}
		key := value.InventoryID + "\x00" + value.ArtifactID + "\x00" + value.Path
		if _, exists := seen[key]; exists {
			return "", fmt.Errorf("%w: duplicate runner artifact binding", ErrInvalidIssuer)
		}
		seen[key] = struct{}{}
		canonical = append(canonical, binding{InventoryID: value.InventoryID, ArtifactID: value.ArtifactID, Role: value.Role, Path: value.Path, MediaType: value.MediaType, SizeBytes: size, SHA256: value.SHA256})
	}
	sort.Slice(canonical, func(left, right int) bool {
		if canonical[left].InventoryID != canonical[right].InventoryID {
			return canonical[left].InventoryID < canonical[right].InventoryID
		}
		if canonical[left].ArtifactID != canonical[right].ArtifactID {
			return canonical[left].ArtifactID < canonical[right].ArtifactID
		}
		return canonical[left].Path < canonical[right].Path
	})
	encoded, err := json.Marshal(struct {
		Domain  string    `json:"domain"`
		Version int       `json:"version"`
		Payload []binding `json:"payloads"`
	}{Domain: "synchro:conformance:runner-artifact", Version: 1, Payload: canonical})
	if err != nil {
		return "", fmt.Errorf("%w: encode runner artifact bindings: %v", ErrInvalidIssuer, err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

// RunnerArtifactPayloadDigest returns a runner identity from verified payload
// bytes and their immutable bindings.
func RunnerArtifactPayloadDigest(payloads []ArtifactPayload) (string, error) {
	if len(payloads) == 0 {
		return "", fmt.Errorf("%w: runner artifact payloads", ErrInvalidIssuer)
	}
	bindings := make([]ArtifactBinding, 0, len(payloads))
	for _, payload := range payloads {
		binding := payload.Binding
		size := binding.Size
		if binding.SizeBytes != 0 {
			if binding.Size != 0 && binding.Size != binding.SizeBytes {
				return "", fmt.Errorf("%w: runner artifact size", ErrInvalidIssuer)
			}
			size = binding.SizeBytes
		}
		digest := sha256.Sum256(payload.Bytes)
		if int64(len(payload.Bytes)) != size || hex.EncodeToString(digest[:]) != binding.SHA256 {
			return "", fmt.Errorf("%w: runner artifact payload bytes", ErrInvalidIssuer)
		}
		bindings = append(bindings, binding)
	}
	return RunnerArtifactDigest(bindings)
}

// Authorize binds this trusted runner to one exact locked runner artifact.
func (r TrustedRunner) Authorize(runnerArtifactSHA256 string) (RunnerAuthorization, error) {
	return r.AuthorizeExecutable(runnerArtifactSHA256, "")
}

// AuthorizeExecutable binds this trusted runner to one artifact and executable.
func (r TrustedRunner) AuthorizeExecutable(runnerArtifactSHA256, runnerExecutableSHA256 string) (RunnerAuthorization, error) {
	if len(r.privateKey) != ed25519.PrivateKeySize || !validLowerHex(runnerArtifactSHA256, sha256.Size) {
		return RunnerAuthorization{}, fmt.Errorf("%w: runner authorization", ErrInvalidIssuer)
	}
	if runnerExecutableSHA256 != "" && !validLowerHex(runnerExecutableSHA256, sha256.Size) {
		return RunnerAuthorization{}, fmt.Errorf("%w: runner executable authorization", ErrInvalidIssuer)
	}
	publicKey, ok := r.privateKey.Public().(ed25519.PublicKey)
	if !ok || len(publicKey) != ed25519.PublicKeySize {
		return RunnerAuthorization{}, fmt.Errorf("%w: runner public key", ErrInvalidIssuer)
	}
	return RunnerAuthorization{state: &runnerAuthorizationState{
		verifier:               append(ed25519.PublicKey(nil), publicKey...),
		runnerArtifactSHA256:   runnerArtifactSHA256,
		runnerExecutableSHA256: runnerExecutableSHA256,
	}}, nil
}

// RunnerExecutableSHA256 returns the exact locked runner executable digest.
func (a RunnerAuthorization) RunnerExecutableSHA256() string {
	if a.state == nil {
		return ""
	}
	return a.state.runnerExecutableSHA256
}

// RunnerDigest returns the public runner identity bound to this capability.
func (a RunnerAuthorization) RunnerDigest() string {
	if a.state == nil {
		return ""
	}
	digest := sha256.Sum256(a.state.verifier)
	return hex.EncodeToString(digest[:])
}

// RunnerArtifactSHA256 returns the exact locked runner artifact digest.
func (a RunnerAuthorization) RunnerArtifactSHA256() string {
	if a.state == nil {
		return ""
	}
	return a.state.runnerArtifactSHA256
}

// NewReceiptIssuer creates one challenge for the specified runner key.
// This legacy constructor authenticates receipts but cannot authorize evidence.
func NewReceiptIssuer(verifierKey []byte) (ReceiptIssuer, error) {
	if len(verifierKey) != ed25519.PublicKeySize {
		return ReceiptIssuer{}, fmt.Errorf("%w: verifier key length", ErrInvalidIssuer)
	}
	return newReceiptIssuer(append(ed25519.PublicKey(nil), verifierKey...), "", "", GeneratorIdentity{})
}

// NewReceiptIssuerFromAuthorization creates one evidence-authorizing challenge.
func NewReceiptIssuerFromAuthorization(authorization RunnerAuthorization) (ReceiptIssuer, error) {
	return newAuthorizedReceiptIssuer(authorization, "", GeneratorIdentity{})
}

// NewReceiptIssuerFromAuthorizationAndGenerator creates an evidence-authorizing
// challenge bound to the runner artifact and immutable generator identity.
func NewReceiptIssuerFromAuthorizationAndGenerator(authorization RunnerAuthorization, generator GeneratorIdentity) (ReceiptIssuer, error) {
	if generator.Name == "" || generator.Version == "" || !validLowerHex(generator.BinarySHA256, sha256.Size) {
		return ReceiptIssuer{}, fmt.Errorf("%w: generator identity", ErrInvalidIssuer)
	}
	return newAuthorizedReceiptIssuer(authorization, "", generator)
}

// NewReceiptIssuerFromAuthorizationAndGeneratorAndCandidateLock creates an evidence-authorizing challenge.
func NewReceiptIssuerFromAuthorizationAndGeneratorAndCandidateLock(authorization RunnerAuthorization, generator GeneratorIdentity, candidateLockSHA256 string) (ReceiptIssuer, error) {
	if generator.Name == "" || generator.Version == "" || !validLowerHex(generator.BinarySHA256, sha256.Size) || !validLowerHex(candidateLockSHA256, sha256.Size) || authorization.state == nil || !validLowerHex(authorization.state.runnerExecutableSHA256, sha256.Size) {
		return ReceiptIssuer{}, fmt.Errorf("%w: evidence authority", ErrInvalidIssuer)
	}
	return newReceiptIssuerWithCommand(authorization.state.verifier, authorization.state.runnerArtifactSHA256, authorization.state.runnerExecutableSHA256, candidateLockSHA256, generator, CommandCapability{})
}

// NewReceiptIssuerFromAuthorizationAndGeneratorAndCandidateLockAndCommandCapability
// creates an evidence-authorizing challenge that requires one observed Make
// process before receipt completion.
func NewReceiptIssuerFromAuthorizationAndGeneratorAndCandidateLockAndCommandCapability(authorization RunnerAuthorization, generator GeneratorIdentity, candidateLockSHA256 string, command CommandCapability) (ReceiptIssuer, error) {
	if generator.Name == "" || generator.Version == "" || !validLowerHex(generator.BinarySHA256, sha256.Size) || !validLowerHex(candidateLockSHA256, sha256.Size) || command.state == nil {
		return ReceiptIssuer{}, fmt.Errorf("%w: evidence command authority", ErrInvalidIssuer)
	}
	if authorization.state == nil || len(authorization.state.verifier) != ed25519.PublicKeySize || !validLowerHex(authorization.state.runnerArtifactSHA256, sha256.Size) || !validLowerHex(authorization.state.runnerExecutableSHA256, sha256.Size) {
		return ReceiptIssuer{}, ErrInvalidIssuer
	}
	return newReceiptIssuerWithCommand(authorization.state.verifier, authorization.state.runnerArtifactSHA256, authorization.state.runnerExecutableSHA256, candidateLockSHA256, generator, command)
}

func newAuthorizedReceiptIssuer(authorization RunnerAuthorization, candidateLockSHA256 string, generator GeneratorIdentity) (ReceiptIssuer, error) {
	if authorization.state == nil || len(authorization.state.verifier) != ed25519.PublicKeySize || !validLowerHex(authorization.state.runnerArtifactSHA256, sha256.Size) || (candidateLockSHA256 != "" && !validLowerHex(candidateLockSHA256, sha256.Size)) {
		return ReceiptIssuer{}, ErrInvalidIssuer
	}
	return newReceiptIssuer(authorization.state.verifier, authorization.state.runnerArtifactSHA256, candidateLockSHA256, generator)
}

func newReceiptIssuer(verifierKey ed25519.PublicKey, runnerArtifactSHA256, candidateLockSHA256 string, generator GeneratorIdentity) (ReceiptIssuer, error) {
	return newReceiptIssuerWithCommand(verifierKey, runnerArtifactSHA256, "", candidateLockSHA256, generator, CommandCapability{})
}

func newReceiptIssuerWithCommand(verifierKey ed25519.PublicKey, runnerArtifactSHA256, runnerExecutableSHA256, candidateLockSHA256 string, generator GeneratorIdentity, command CommandCapability) (ReceiptIssuer, error) {
	state := &issuerState{verifier: append(ed25519.PublicKey(nil), verifierKey...), runnerArtifactSHA256: runnerArtifactSHA256, runnerExecutableSHA256: runnerExecutableSHA256, candidateLockSHA256: candidateLockSHA256, generator: generator}
	if _, err := rand.Read(state.nonce[:]); err != nil {
		return ReceiptIssuer{}, fmt.Errorf("%w: create nonce: %v", ErrInvalidIssuer, err)
	}
	digest := sha256.Sum256(state.verifier)
	state.runnerDigest = hex.EncodeToString(digest[:])
	if command.state != nil {
		if err := command.bindIssuer(state); err != nil {
			return ReceiptIssuer{}, fmt.Errorf("%w: command capability: %v", ErrInvalidIssuer, err)
		}
		state.command = command.state
	}
	return ReceiptIssuer{state: state}, nil
}

// RunnerDigest returns the public runner identity bound to the issuer.
func (i ReceiptIssuer) RunnerDigest() string {
	if i.state == nil {
		return ""
	}
	return i.state.runnerDigest
}

// RunnerArtifactSHA256 returns the artifact digest bound to the issuer.
func (i ReceiptIssuer) RunnerArtifactSHA256() string {
	if i.state == nil {
		return ""
	}
	return i.state.runnerArtifactSHA256
}

// RunnerExecutableSHA256 returns the exact executable digest bound to the issuer.
func (i ReceiptIssuer) RunnerExecutableSHA256() string {
	if i.state == nil {
		return ""
	}
	return i.state.runnerExecutableSHA256
}

// AuthorizesEvidence reports whether this issuer has all evidence authority bindings.
func (i ReceiptIssuer) AuthorizesEvidence() bool {
	return i.state != nil &&
		i.state.runnerArtifactSHA256 != "" &&
		i.state.runnerExecutableSHA256 != "" &&
		i.state.candidateLockSHA256 != "" &&
		i.state.generator.Name != "" &&
		i.state.generator.Version != "" &&
		i.state.generator.BinarySHA256 != ""
}

// MatchesCommandCapability reports whether the capability is the exact
// process launcher bound to this issuer.
func (i ReceiptIssuer) MatchesCommandCapability(command CommandCapability) bool {
	return i.state != nil && command.matchesIssuer(i.state)
}

// Used reports whether the issuer completed one receipt.
func (i ReceiptIssuer) Used() bool {
	if i.state == nil {
		return false
	}
	i.state.mu.Lock()
	defer i.state.mu.Unlock()
	return i.state.used
}

// PrepareCompletion validates and seals terminal fields for runner signing.
// This operation does not consume the issuer.
func PrepareCompletion(issuer ReceiptIssuer, fields ReceiptFields) (Completion, error) {
	if issuer.state == nil || len(issuer.state.verifier) != ed25519.PublicKeySize {
		return Completion{}, ErrInvalidIssuer
	}
	issuer.state.mu.Lock()
	defer issuer.state.mu.Unlock()
	if issuer.state.used {
		return Completion{}, ErrIssuerUsed
	}

	prepared := cloneReceiptFields(fields)
	prepared.StartedAt = prepared.StartedAt.Round(0).UTC()
	prepared.CompletedAt = prepared.CompletedAt.Round(0).UTC()
	prepared = normalizeReceiptFields(prepared)
	if prepared.RunnerDigest != "" && prepared.RunnerDigest != issuer.state.runnerDigest {
		return Completion{}, fmt.Errorf("%w: runner digest mismatch", ErrInvalidCompletion)
	}
	prepared.RunnerDigest = issuer.state.runnerDigest
	if issuer.state.runnerArtifactSHA256 == "" {
		if prepared.RunnerArtifactSHA256 != "" {
			return Completion{}, fmt.Errorf("%w: runner artifact digest is unauthorized", ErrInvalidCompletion)
		}
	} else if prepared.RunnerArtifactSHA256 != "" && prepared.RunnerArtifactSHA256 != issuer.state.runnerArtifactSHA256 {
		return Completion{}, fmt.Errorf("%w: runner artifact digest mismatch", ErrInvalidCompletion)
	}
	prepared.RunnerArtifactSHA256 = issuer.state.runnerArtifactSHA256
	if issuer.state.runnerExecutableSHA256 == "" {
		if prepared.RunnerExecutableSHA256 != "" {
			return Completion{}, fmt.Errorf("%w: runner executable digest is unauthorized", ErrInvalidCompletion)
		}
	} else if prepared.RunnerExecutableSHA256 != "" && prepared.RunnerExecutableSHA256 != issuer.state.runnerExecutableSHA256 {
		return Completion{}, fmt.Errorf("%w: runner executable digest mismatch", ErrInvalidCompletion)
	}
	prepared.RunnerExecutableSHA256 = issuer.state.runnerExecutableSHA256
	if issuer.state.candidateLockSHA256 == "" {
		if prepared.CandidateLockSHA256 != "" {
			return Completion{}, fmt.Errorf("%w: candidate lock digest is unauthorized", ErrInvalidCompletion)
		}
	} else {
		if prepared.CandidateLockSHA256 != "" && prepared.CandidateLockSHA256 != issuer.state.candidateLockSHA256 {
			return Completion{}, fmt.Errorf("%w: candidate lock digest mismatch", ErrInvalidCompletion)
		}
		prepared.CandidateLockSHA256 = issuer.state.candidateLockSHA256
	}
	if issuer.state.generator.Name == "" {
		if prepared.GeneratorName != "" || prepared.GeneratorVersion != "" || prepared.GeneratorBinarySHA256 != "" {
			return Completion{}, fmt.Errorf("%w: generator identity is unauthorized", ErrInvalidCompletion)
		}
	} else {
		if (prepared.GeneratorName != "" && prepared.GeneratorName != issuer.state.generator.Name) ||
			(prepared.GeneratorVersion != "" && prepared.GeneratorVersion != issuer.state.generator.Version) ||
			(prepared.GeneratorBinarySHA256 != "" && prepared.GeneratorBinarySHA256 != issuer.state.generator.BinarySHA256) {
			return Completion{}, fmt.Errorf("%w: generator identity mismatch", ErrInvalidCompletion)
		}
		prepared.GeneratorName = issuer.state.generator.Name
		prepared.GeneratorVersion = issuer.state.generator.Version
		prepared.GeneratorBinarySHA256 = issuer.state.generator.BinarySHA256
	}
	prepared.ReceiptID = ""
	if issuer.state.command != nil && !(CommandCapability{state: issuer.state.command}).validates(prepared, issuer.state) {
		return Completion{}, fmt.Errorf("%w: observed evidence command", ErrInvalidCompletion)
	}
	if err := validateReceiptFields(prepared, false); err != nil {
		return Completion{}, err
	}
	identifier, err := receiptIdentifier(issuer.state.nonce, prepared)
	if err != nil {
		return Completion{}, err
	}
	prepared.ReceiptID = identifier
	if err := validateReceiptFields(prepared, true); err != nil {
		return Completion{}, err
	}
	message, err := completionMessage(issuer.state.nonce, prepared)
	if err != nil {
		return Completion{}, err
	}
	return Completion{
		issuer:  issuer.state,
		fields:  cloneReceiptFields(prepared),
		message: append([]byte(nil), message...),
	}, nil
}

// SigningBytes returns an isolated copy of the authenticated completion data.
func (c Completion) SigningBytes() []byte {
	return append([]byte(nil), c.message...)
}

// CompleteReceipt verifies the runner signature and consumes the issuer.
func CompleteReceipt(issuer ReceiptIssuer, completion Completion, signature []byte) (Receipt, error) {
	if issuer.state == nil || completion.issuer == nil || issuer.state != completion.issuer {
		return Receipt{}, fmt.Errorf("%w: issuer binding", ErrInvalidCompletion)
	}
	state := issuer.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.used {
		return Receipt{}, ErrIssuerUsed
	}
	message, err := completionMessage(state.nonce, completion.fields)
	if err != nil || !bytes.Equal(message, completion.message) {
		return Receipt{}, fmt.Errorf("%w: changed completion", ErrInvalidCompletion)
	}
	if len(signature) != ed25519.SignatureSize || !ed25519.Verify(state.verifier, message, signature) {
		return Receipt{}, fmt.Errorf("%w: runner signature", ErrInvalidCompletion)
	}
	state.used = true
	return Receipt{
		fields: cloneReceiptFields(completion.fields),
		origin: state,
		seal: &receiptSeal{
			verifier:  append(ed25519.PublicKey(nil), state.verifier...),
			nonce:     state.nonce,
			signature: append([]byte(nil), signature...),
		},
	}, nil
}

// Fields returns an isolated copy after receipt authentication succeeds.
func (r Receipt) Fields() (ReceiptFields, error) {
	if err := r.Verify(); err != nil {
		return ReceiptFields{}, err
	}
	return cloneReceiptFields(r.fields), nil
}

// Verify authenticates the immutable receipt fields.
func (r Receipt) Verify() error {
	if r.seal == nil || len(r.seal.verifier) != ed25519.PublicKeySize || len(r.seal.signature) != ed25519.SignatureSize {
		return ErrInvalidReceipt
	}
	if err := validateReceiptFields(r.fields, true); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidReceipt, err)
	}
	digest := sha256.Sum256(r.seal.verifier)
	if r.fields.RunnerDigest != hex.EncodeToString(digest[:]) {
		return fmt.Errorf("%w: runner digest", ErrInvalidReceipt)
	}
	if r.fields.RunnerArtifactSHA256 != "" && !validLowerHex(r.fields.RunnerArtifactSHA256, sha256.Size) {
		return fmt.Errorf("%w: runner artifact digest", ErrInvalidReceipt)
	}
	identifier, err := receiptIdentifier(r.seal.nonce, withoutReceiptID(r.fields))
	if err != nil || identifier != r.fields.ReceiptID {
		return fmt.Errorf("%w: receipt identifier", ErrInvalidReceipt)
	}
	message, err := completionMessage(r.seal.nonce, r.fields)
	if err != nil || !ed25519.Verify(r.seal.verifier, message, r.seal.signature) {
		return fmt.Errorf("%w: signature", ErrInvalidReceipt)
	}
	return nil
}

// VerifyAndConsume makes one receipt unavailable for a second evidence build.
func (r Receipt) VerifyAndConsume() error {
	if err := r.Verify(); err != nil {
		return err
	}
	r.seal.mu.Lock()
	defer r.seal.mu.Unlock()
	if r.seal.consumed {
		return ErrReceiptConsumed
	}
	r.seal.consumed = true
	return nil
}

// IssuedBy reports whether the receipt came from the exact in-memory issuer.
// Serialized receipts authenticate execution but cannot claim this build slot.
func (r Receipt) IssuedBy(issuer ReceiptIssuer) bool {
	return r.origin != nil && issuer.state != nil && r.origin == issuer.state
}

// MarshalJSON emits only the exact receipt data fields.
func (r Receipt) MarshalJSON() ([]byte, error) {
	if err := r.Verify(); err != nil {
		return nil, err
	}
	return json.Marshal(normalizeReceiptFields(r.fields))
}

// Authentication returns a defensive copy of the public verification material.
func (r Receipt) Authentication() (ReceiptAuthentication, error) {
	if err := r.Verify(); err != nil {
		return ReceiptAuthentication{}, err
	}
	return ReceiptAuthentication{
		RunnerPublicKey: base64.RawURLEncoding.EncodeToString(r.seal.verifier),
		Nonce:           base64.RawURLEncoding.EncodeToString(r.seal.nonce[:]),
		Signature:       base64.RawURLEncoding.EncodeToString(r.seal.signature),
	}, nil
}

// AuthenticatedBytes returns one serializable completed-receipt document.
func (r Receipt) AuthenticatedBytes() ([]byte, error) {
	if err := r.Verify(); err != nil {
		return nil, err
	}
	authentication, err := r.Authentication()
	if err != nil {
		return nil, err
	}
	return json.Marshal(receiptDocument{ReceiptFields: r.fields, Authentication: authentication})
}

// ParseReceipt authenticates a serialized completed receipt.
func ParseReceipt(data []byte) (Receipt, error) {
	var document receiptDocument
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		return Receipt{}, fmt.Errorf("%w: decode receipt: %v", ErrInvalidReceipt, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return Receipt{}, fmt.Errorf("%w: trailing receipt data", ErrInvalidReceipt)
		}
		return Receipt{}, fmt.Errorf("%w: trailing receipt data: %v", ErrInvalidReceipt, err)
	}
	verifier, err := base64.RawURLEncoding.DecodeString(document.Authentication.RunnerPublicKey)
	if err != nil || len(verifier) != ed25519.PublicKeySize {
		return Receipt{}, fmt.Errorf("%w: runner public key", ErrInvalidReceipt)
	}
	nonce, err := base64.RawURLEncoding.DecodeString(document.Authentication.Nonce)
	if err != nil || len(nonce) != sha256.Size {
		return Receipt{}, fmt.Errorf("%w: nonce", ErrInvalidReceipt)
	}
	signature, err := base64.RawURLEncoding.DecodeString(document.Authentication.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return Receipt{}, fmt.Errorf("%w: signature", ErrInvalidReceipt)
	}
	var nonceArray [sha256.Size]byte
	copy(nonceArray[:], nonce)
	receipt := Receipt{
		fields: cloneReceiptFields(document.ReceiptFields),
		seal: &receiptSeal{
			verifier:  append(ed25519.PublicKey(nil), verifier...),
			nonce:     nonceArray,
			signature: append([]byte(nil), signature...),
		},
	}
	if err := receipt.Verify(); err != nil {
		return Receipt{}, err
	}
	return receipt, nil
}

type receiptDocument struct {
	ReceiptFields
	Authentication ReceiptAuthentication `json:"authentication"`
}

func (document receiptDocument) MarshalJSON() ([]byte, error) {
	fields, err := json.Marshal(normalizeReceiptFields(document.ReceiptFields))
	if err != nil {
		return nil, err
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(fields, &members); err != nil {
		return nil, err
	}
	authentication, err := json.Marshal(document.Authentication)
	if err != nil {
		return nil, err
	}
	members["authentication"] = authentication
	return json.Marshal(members)
}

func receiptIdentifier(nonce [32]byte, fields ReceiptFields) (string, error) {
	encoded, err := json.Marshal(normalizeReceiptFields(fields))
	if err != nil {
		return "", fmt.Errorf("%w: encode receipt identifier", ErrInvalidCompletion)
	}
	hash := sha256.New()
	hash.Write([]byte(receiptSignatureDomain))
	hash.Write([]byte{0})
	hash.Write(nonce[:])
	hash.Write([]byte{0})
	hash.Write(encoded)
	return "receipt-sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}

func completionMessage(nonce [32]byte, fields ReceiptFields) ([]byte, error) {
	encoded, err := json.Marshal(normalizeReceiptFields(fields))
	if err != nil {
		return nil, fmt.Errorf("%w: encode completion", ErrInvalidCompletion)
	}
	message := make([]byte, 0, len(receiptSignatureDomain)+len(nonce)+len(encoded)+2)
	message = append(message, receiptSignatureDomain...)
	message = append(message, 0)
	message = append(message, nonce[:]...)
	message = append(message, 0)
	message = append(message, encoded...)
	return message, nil
}

func withoutReceiptID(fields ReceiptFields) ReceiptFields {
	result := cloneReceiptFields(fields)
	result.ReceiptID = ""
	return result
}

func validateReceiptFields(fields ReceiptFields, requireID bool) error {
	if requireID {
		if len(fields.ReceiptID) != len("receipt-sha256:")+sha256.Size*2 || fields.ReceiptID[:len("receipt-sha256:")] != "receipt-sha256:" {
			return fmt.Errorf("%w: receipt ID", ErrInvalidCompletion)
		}
		if !validLowerHex(fields.ReceiptID[len("receipt-sha256:"):], sha256.Size) {
			return fmt.Errorf("%w: receipt ID digest", ErrInvalidCompletion)
		}
	} else if fields.ReceiptID != "" {
		return fmt.Errorf("%w: receipt ID must be assigned by the issuer", ErrInvalidCompletion)
	}
	if fields.ScenarioID == "" || fields.ProofObligationID == "" || fields.MakeTarget == "" {
		return fmt.Errorf("%w: run identity", ErrInvalidCompletion)
	}
	if len(fields.Argv) != 2 || fields.Argv[0] != "make" || fields.Argv[1] != fields.MakeTarget {
		return fmt.Errorf("%w: command binding", ErrInvalidCompletion)
	}
	if fields.StartedAt.IsZero() || fields.CompletedAt.IsZero() || fields.CompletedAt.Before(fields.StartedAt) {
		return fmt.Errorf("%w: timestamps", ErrInvalidCompletion)
	}
	if err := validateCommandObservation(fields); err != nil {
		return err
	}
	if fields.ExitCode < 0 {
		return fmt.Errorf("%w: exit code", ErrInvalidCompletion)
	}
	switch fields.Result {
	case ResultPassed:
		if fields.ExitCode != 0 {
			return fmt.Errorf("%w: passed exit code", ErrInvalidCompletion)
		}
	case ResultFailed, ResultError:
		if fields.ExitCode == 0 {
			return fmt.Errorf("%w: unsuccessful exit code", ErrInvalidCompletion)
		}
	default:
		return fmt.Errorf("%w: result", ErrInvalidCompletion)
	}
	if len(fields.Assertions) == 0 || !uniqueAssertionResults(fields.Assertions) {
		return fmt.Errorf("%w: assertions", ErrInvalidCompletion)
	}
	if fields.Result == ResultPassed {
		for _, assertion := range fields.Assertions {
			if assertion.Outcome != "passed" {
				return fmt.Errorf("%w: passed assertion outcome", ErrInvalidCompletion)
			}
		}
		for _, vector := range fields.VectorResults {
			if vector.Outcome != "passed" {
				return fmt.Errorf("%w: passed vector outcome", ErrInvalidCompletion)
			}
		}
	}
	if !uniqueStrings(fields.AttachmentIDs) || !uniqueEnvironment(fields.EnvironmentDimensions) || !uniqueArtifacts(fields.ArtifactBindings) || !uniqueVectors(fields.VectorResults) {
		return fmt.Errorf("%w: duplicate receipt binding", ErrInvalidCompletion)
	}
	if !validLowerHex(fields.RunnerDigest, sha256.Size) {
		return fmt.Errorf("%w: runner digest", ErrInvalidCompletion)
	}
	if fields.CandidateLockSHA256 != "" && !validLowerHex(fields.CandidateLockSHA256, sha256.Size) {
		return fmt.Errorf("%w: candidate lock digest", ErrInvalidCompletion)
	}
	if fields.RunnerArtifactSHA256 != "" && !validLowerHex(fields.RunnerArtifactSHA256, sha256.Size) {
		return fmt.Errorf("%w: runner artifact digest", ErrInvalidCompletion)
	}
	if fields.RunnerExecutableSHA256 != "" && !validLowerHex(fields.RunnerExecutableSHA256, sha256.Size) {
		return fmt.Errorf("%w: runner executable digest", ErrInvalidCompletion)
	}
	hasGenerator := fields.GeneratorName != "" || fields.GeneratorVersion != "" || fields.GeneratorBinarySHA256 != ""
	if hasGenerator && (fields.GeneratorName == "" || fields.GeneratorVersion == "" || !validLowerHex(fields.GeneratorBinarySHA256, sha256.Size)) {
		return fmt.Errorf("%w: generator identity", ErrInvalidCompletion)
	}
	if err := validateExtendedReceiptFields(fields); err != nil {
		return err
	}
	return nil
}

func validateCommandObservation(fields ReceiptFields) error {
	value := fields.Command
	hasCommand := len(value.Argv) != 0 || value.ExitCode != 0 || !value.StartedAt.IsZero() || !value.CompletedAt.IsZero() || value.MakeExecutableSHA256 != "" || value.SourceSnapshotSHA256 != ""
	if !hasCommand {
		if fields.CandidateLockSHA256 != "" && fields.RunnerExecutableSHA256 == "" {
			return fmt.Errorf("%w: missing command observation", ErrInvalidCompletion)
		}
		return nil
	}
	if !sameCommandArgv(value.Argv, fields.Argv) || value.ExitCode < 0 || value.StartedAt.IsZero() || value.CompletedAt.IsZero() || value.CompletedAt.Before(value.StartedAt) || value.StartedAt.Before(fields.StartedAt) || value.CompletedAt.After(fields.CompletedAt) || !validLowerHex(value.MakeExecutableSHA256, sha256.Size) || !validLowerHex(value.SourceSnapshotSHA256, sha256.Size) {
		return fmt.Errorf("%w: command observation", ErrInvalidCompletion)
	}
	if fields.Result == ResultPassed && value.ExitCode != 0 {
		return fmt.Errorf("%w: passed command exit code", ErrInvalidCompletion)
	}
	return nil
}

func validateExtendedReceiptFields(fields ReceiptFields) error {
	if fields.Attempt < 0 {
		return fmt.Errorf("%w: attempt", ErrInvalidCompletion)
	}
	if fields.Attempt == 0 {
		if fields.RunID != "" || fields.ExecutionLineageID != "" || fields.RunURL != "" || fields.PreviousEvidenceID != nil || fields.RerunCause != nil || fields.RerunDiagnosis != nil || fields.CorrectiveAction != nil || fields.RerunApproval != nil {
			return fmt.Errorf("%w: incomplete run lineage", ErrInvalidCompletion)
		}
	} else if fields.RunID == "" || fields.ExecutionLineageID == "" || fields.RunURL == "" {
		return fmt.Errorf("%w: run identity", ErrInvalidCompletion)
	}
	if fields.Attempt <= 1 {
		if fields.PreviousEvidenceID != nil || fields.RerunCause != nil || fields.RerunDiagnosis != nil || fields.CorrectiveAction != nil || fields.RerunApproval != nil {
			return fmt.Errorf("%w: initial run lineage", ErrInvalidCompletion)
		}
	} else if fields.PreviousEvidenceID == nil || *fields.PreviousEvidenceID == "" || fields.RerunCause == nil || fields.RerunDiagnosis == nil || fields.CorrectiveAction == nil || fields.RerunApproval == nil || fields.RerunApproval.ApproverIdentity == "" || fields.RerunApproval.URI == "" || fields.RerunApproval.ApprovedAt.IsZero() {
		return fmt.Errorf("%w: rerun lineage", ErrInvalidCompletion)
	}
	if !uniqueAttachments(fields.Attachments) || !uniqueHTTPObservations(fields.HTTPObservations) || !uniqueObservations(fields.Observations) {
		return fmt.Errorf("%w: duplicate extended receipt binding", ErrInvalidCompletion)
	}
	if len(fields.Attachments) > 0 && !attachmentsMatchIDs(fields.Attachments, fields.AttachmentIDs) {
		return fmt.Errorf("%w: attachment IDs", ErrInvalidCompletion)
	}
	if fields.ExecutionArtifacts != nil && !validateExecutionArtifacts(*fields.ExecutionArtifacts, fields.AttachmentIDs) {
		return fmt.Errorf("%w: execution artifacts", ErrInvalidCompletion)
	}
	if fields.Replay != nil && !validateReplay(*fields.Replay, fields.AttachmentIDs) {
		return fmt.Errorf("%w: replay", ErrInvalidCompletion)
	}
	if fields.FaultExecution != nil && !validateFaultExecution(*fields.FaultExecution, fields.AttachmentIDs) {
		return fmt.Errorf("%w: fault execution", ErrInvalidCompletion)
	}
	if !uniquePerformanceResults(fields.PerformanceResults) || !uniqueRequiredMeasurements(fields.RequiredMeasurements) {
		return fmt.Errorf("%w: duplicate measurement", ErrInvalidCompletion)
	}
	if fields.NegativeControl != nil && !validateNegativeControl(*fields.NegativeControl, fields.AttachmentIDs) {
		return fmt.Errorf("%w: negative control", ErrInvalidCompletion)
	}
	if fields.Counters != nil && !validCounters(*fields.Counters) {
		return fmt.Errorf("%w: counters", ErrInvalidCompletion)
	}
	return nil
}

func validLowerHex(value string, byteCount int) bool {
	if len(value) != byteCount*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != byteCount {
		return false
	}
	return value == hex.EncodeToString(decoded)
}

func uniqueAssertionResults(values []AssertionResult) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value.AssertionID == "" || !validOutcome(value.Outcome) {
			return false
		}
		if _, exists := seen[value.AssertionID]; exists {
			return false
		}
		seen[value.AssertionID] = struct{}{}
	}
	return true
}

func validOutcome(value string) bool {
	switch value {
	case "passed", "failed", "error", "skipped":
		return true
	default:
		return false
	}
}

func uniqueStrings(values []string) bool {
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

func uniqueEnvironment(values []EnvironmentDimension) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value.Name == "" || value.Value == "" {
			return false
		}
		if _, exists := seen[value.Name]; exists {
			return false
		}
		seen[value.Name] = struct{}{}
	}
	return true
}

func uniqueArtifacts(values []ArtifactBinding) bool {
	seen := make(map[string]map[string]struct{}, len(values))
	paths := make(map[string]struct{}, len(values))
	for _, value := range values {
		size := value.Size
		if value.SizeBytes != 0 {
			if value.Size != 0 && value.Size != value.SizeBytes {
				return false
			}
			size = value.SizeBytes
		}
		if value.InventoryID == "" || value.ArtifactID == "" || value.Path == "" || size < 0 || !validLowerHex(value.SHA256, sha256.Size) {
			return false
		}
		if _, exists := paths[value.Path]; exists {
			return false
		}
		inventoryPaths := seen[value.InventoryID]
		if inventoryPaths == nil {
			inventoryPaths = make(map[string]struct{})
			seen[value.InventoryID] = inventoryPaths
		}
		if _, exists := inventoryPaths[value.Path]; exists {
			return false
		}
		inventoryPaths[value.Path] = struct{}{}
		paths[value.Path] = struct{}{}
	}
	return true
}

func uniqueVectors(values []VectorResult) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value.VectorSetID == "" || value.ArtifactID == "" || value.ResultAttachmentID == "" || value.ExecutedCount < 0 || value.PassedCount < 0 || value.FailedCount < 0 || value.ExecutedCount != value.PassedCount+value.FailedCount || !validLowerHex(value.SourceSHA256, sha256.Size) || !validLowerHex(value.AggregateSHA256, sha256.Size) {
			return false
		}
		if value.Outcome != "passed" && value.Outcome != "failed" && value.Outcome != "error" {
			return false
		}
		key := value.VectorSetID + "\x00" + value.Language
		if _, exists := seen[key]; exists {
			return false
		}
		seen[key] = struct{}{}
	}
	return true
}

func cloneReceiptFields(source ReceiptFields) ReceiptFields {
	result := source
	result.Argv = cloneSlice(source.Argv)
	result.Command.Argv = cloneSlice(source.Command.Argv)
	result.Assertions = cloneSlice(source.Assertions)
	result.VectorResults = cloneSlice(source.VectorResults)
	result.ArtifactBindings = cloneSlice(source.ArtifactBindings)
	result.EnvironmentDimensions = cloneSlice(source.EnvironmentDimensions)
	result.AttachmentIDs = cloneSlice(source.AttachmentIDs)
	result.Attachments = cloneAttachments(source.Attachments)
	result.HTTPObservations = cloneHTTPObservations(source.HTTPObservations)
	result.Observations = cloneSlice(source.Observations)
	result.PerformanceResults = clonePerformanceResults(source.PerformanceResults)
	result.RequiredMeasurements = cloneRequiredMeasurements(source.RequiredMeasurements)
	if source.PreviousEvidenceID != nil {
		value := *source.PreviousEvidenceID
		result.PreviousEvidenceID = &value
	}
	if source.RerunCause != nil {
		value := *source.RerunCause
		result.RerunCause = &value
	}
	if source.RerunDiagnosis != nil {
		value := *source.RerunDiagnosis
		result.RerunDiagnosis = &value
	}
	if source.CorrectiveAction != nil {
		value := *source.CorrectiveAction
		result.CorrectiveAction = &value
	}
	if source.RerunApproval != nil {
		value := *source.RerunApproval
		result.RerunApproval = &value
	}
	if source.Counters != nil {
		value := *source.Counters
		result.Counters = &value
	}
	if source.ExecutionArtifacts != nil {
		value := *source.ExecutionArtifacts
		value.LogAttachmentIDs = cloneSlice(source.ExecutionArtifacts.LogAttachmentIDs)
		value.TraceAttachmentIDs = cloneSlice(source.ExecutionArtifacts.TraceAttachmentIDs)
		value.ReplayDataAttachmentIDs = cloneSlice(source.ExecutionArtifacts.ReplayDataAttachmentIDs)
		value.BarrierTraceAttachmentIDs = cloneSlice(source.ExecutionArtifacts.BarrierTraceAttachmentIDs)
		result.ExecutionArtifacts = &value
	}
	if source.Replay != nil {
		value := *source.Replay
		if source.Replay.Seed != nil {
			seed := *source.Replay.Seed
			value.Seed = &seed
		}
		value.BarrierTraces = cloneSlice(source.Replay.BarrierTraces)
		result.Replay = &value
	}
	if source.FaultExecution != nil {
		value := *source.FaultExecution
		value.DetectedBy = append([]string(nil), source.FaultExecution.DetectedBy...)
		result.FaultExecution = &value
	}
	if source.NegativeControl != nil {
		value := *source.NegativeControl
		value.ControlSubjectArtifactIDs = append([]string(nil), source.NegativeControl.ControlSubjectArtifactIDs...)
		value.DetectedBy = append([]string(nil), source.NegativeControl.DetectedBy...)
		value.AttachmentIDs = append([]string(nil), source.NegativeControl.AttachmentIDs...)
		result.NegativeControl = &value
	}
	if source.Seed != nil {
		value := *source.Seed
		result.Seed = &value
	}
	return result
}

func cloneSlice[T any](source []T) []T {
	if source == nil {
		return nil
	}
	result := make([]T, len(source))
	copy(result, source)
	return result
}

func uniqueAttachments(values []Attachment) bool {
	seenIDs := make(map[string]struct{}, len(values))
	seenPaths := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value.ID == "" || value.Kind == "" || value.Path == "" || value.MediaType == "" || value.SizeBytes < 0 || !validLowerHex(value.SHA256, sha256.Size) {
			return false
		}
		if _, exists := seenIDs[value.ID]; exists {
			return false
		}
		if _, exists := seenPaths[value.Path]; exists {
			return false
		}
		seenIDs[value.ID] = struct{}{}
		seenPaths[value.Path] = struct{}{}
	}
	return true
}

func attachmentsMatchIDs(attachments []Attachment, ids []string) bool {
	if len(attachments) != len(ids) || !uniqueStrings(ids) {
		return false
	}
	wanted := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		wanted[id] = struct{}{}
	}
	for _, attachment := range attachments {
		if _, exists := wanted[attachment.ID]; !exists {
			return false
		}
	}
	return true
}

func uniqueHTTPObservations(values []HTTPObservation) bool {
	if len(values) > 4096 {
		return false
	}
	for _, value := range values {
		if value.RequestClass == "" || value.Status < 0 || value.Status > 599 || value.DurationNanoseconds < 0 || len(value.Headers) > 32 {
			return false
		}
		seenHeaders := make(map[string]struct{}, len(value.Headers))
		for _, header := range value.Headers {
			if header.Name == "" || len(header.Values) == 0 || len(header.Values) > 32 {
				return false
			}
			canonical := strings.ToLower(header.Name)
			if canonical != "content-type" && canonical != "etag" && canonical != "retry-after" && canonical != "x-synchro-protocol-version" {
				return false
			}
			if _, exists := seenHeaders[canonical]; exists {
				return false
			}
			seenHeaders[canonical] = struct{}{}
			for _, item := range header.Values {
				if item == "" || len(item) > 4096 || strings.ContainsAny(item, "\r\n") {
					return false
				}
			}
		}
	}
	return true
}

func uniqueObservations(values []Observation) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value.Name == "" || value.Value == "" || len(value.Name) > 128 || len(value.Value) > 4096 {
			return false
		}
		if _, exists := seen[value.Name]; exists {
			return false
		}
		seen[value.Name] = struct{}{}
	}
	return true
}

func validateExecutionArtifacts(value ExecutionArtifacts, attachmentIDs []string) bool {
	available := make(map[string]struct{}, len(attachmentIDs))
	for _, id := range attachmentIDs {
		available[id] = struct{}{}
	}
	for _, group := range [][]string{value.LogAttachmentIDs, value.TraceAttachmentIDs, value.ReplayDataAttachmentIDs, value.BarrierTraceAttachmentIDs} {
		if !uniqueStrings(group) {
			return false
		}
		for _, id := range group {
			if _, found := available[id]; !found {
				return false
			}
		}
	}
	return true
}

func validateReplay(value ReplayEvidence, attachmentIDs []string) bool {
	available := make(map[string]struct{}, len(attachmentIDs))
	for _, id := range attachmentIDs {
		available[id] = struct{}{}
	}
	seen := make(map[string]struct{}, len(value.BarrierTraces))
	for _, trace := range value.BarrierTraces {
		if trace.BarrierID == "" || trace.AttachmentID == "" {
			return false
		}
		if _, exists := seen[trace.BarrierID]; exists {
			return false
		}
		if _, found := available[trace.AttachmentID]; !found {
			return false
		}
		seen[trace.BarrierID] = struct{}{}
	}
	return true
}

func validateFaultExecution(value FaultExecution, attachmentIDs []string) bool {
	if value.FaultPlanID == "" || value.FaultID == "" || value.ControlID == "" || value.FaultPlanAttachmentID == "" || value.SubjectType == "" || !uniqueStrings(value.DetectedBy) || value.Injection.Mechanism == "" || value.Injection.Target == "" || value.Injection.Operator == "" || value.Injection.Parameters.Scenario == "" || value.Injection.Parameters.Defect == "" {
		return false
	}
	return containsString(attachmentIDs, value.FaultPlanAttachmentID)
}

func uniquePerformanceResults(values []PerformanceResult) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value.BudgetID == "" || value.Outcome == "" || value.MeasurementAttachmentID == "" || value.Metric == "" || value.Unit == "" || value.Comparator == "" || !finite(value.Limit) || !finite(value.ObservedValue) || !validCounters(Counters{RequestCounts: value.Measurement.RequestCounts, ReturnedRebuildPageCount: value.Measurement.ReturnedRebuildPageCount, OutboundNetworkOrRPCHops: value.Measurement.OutboundNetworkOrRPCHops}) {
			return false
		}
		if _, exists := seen[value.BudgetID]; exists {
			return false
		}
		seen[value.BudgetID] = struct{}{}
	}
	return true
}

func uniqueRequiredMeasurements(values []RequiredMeasurementResult) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value.MeasurementID == "" || value.Outcome == "" || value.MeasurementAttachmentID == "" || len(value.Metrics) == 0 || len(value.Strata) == 0 {
			return false
		}
		if _, exists := seen[value.MeasurementID]; exists {
			return false
		}
		seen[value.MeasurementID] = struct{}{}
		metricIDs := make(map[string]struct{}, len(value.Metrics))
		for _, metric := range value.Metrics {
			if metric.ID == "" || metric.Name == "" || metric.Unit == "" {
				return false
			}
			if _, exists := metricIDs[metric.ID]; exists {
				return false
			}
			metricIDs[metric.ID] = struct{}{}
		}
		strata := make(map[string]struct{}, len(value.Strata))
		for _, stratum := range value.Strata {
			if stratum.StratumID == "" || stratum.SampleCount < 1 || len(stratum.Observations) == 0 || len(stratum.Parameters) == 0 {
				return false
			}
			if _, exists := strata[stratum.StratumID]; exists {
				return false
			}
			strata[stratum.StratumID] = struct{}{}
			if len(stratum.Observations) != stratum.SampleCount {
				return false
			}
			for _, observation := range stratum.Observations {
				if observation.SampleID == "" || len(observation.MetricValues) != len(metricIDs) {
					return false
				}
				observedMetrics := make(map[string]struct{}, len(observation.MetricValues))
				for _, metric := range observation.MetricValues {
					if metric.MetricID == "" || !finite(metric.Value) {
						return false
					}
					if _, known := metricIDs[metric.MetricID]; !known {
						return false
					}
					if _, exists := observedMetrics[metric.MetricID]; exists {
						return false
					}
					observedMetrics[metric.MetricID] = struct{}{}
				}
			}
		}
	}
	return true
}

func validateNegativeControl(value NegativeControl, attachmentIDs []string) bool {
	if value.FaultID == "" || value.ControlID == "" || value.FaultPlanID == "" || value.FaultPlanAttachmentID == "" || value.ControlSubjectID == "" || value.ControlSubjectType == "" || value.Outcome != "detected" || !uniqueStrings(value.ControlSubjectArtifactIDs) || !uniqueStrings(value.DetectedBy) || !uniqueStrings(value.AttachmentIDs) {
		return false
	}
	if !containsString(attachmentIDs, value.FaultPlanAttachmentID) {
		return false
	}
	for _, id := range value.AttachmentIDs {
		if !containsString(attachmentIDs, id) {
			return false
		}
	}
	return true
}

func validCounters(value Counters) bool {
	return value.RequestCounts.Connect >= 0 && value.RequestCounts.Push >= 0 && value.RequestCounts.Pull >= 0 && value.RequestCounts.RebuildPage >= 0 && value.RequestCounts.SchemaFetch >= 0 && value.RequestCounts.Other >= 0 && value.ReturnedRebuildPageCount >= 0 && value.OutboundNetworkOrRPCHops >= 0
}

func finite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func cloneAttachments(source []Attachment) []Attachment {
	result := make([]Attachment, len(source))
	copy(result, source)
	return result
}

func cloneHTTPObservations(source []HTTPObservation) []HTTPObservation {
	result := make([]HTTPObservation, len(source))
	for index, value := range source {
		result[index] = value
		result[index].Headers = make([]HTTPHeader, len(value.Headers))
		for headerIndex, header := range value.Headers {
			result[index].Headers[headerIndex] = HTTPHeader{Name: header.Name, Values: cloneSlice(value.Headers[headerIndex].Values)}
		}
	}
	return result
}

func clonePerformanceResults(source []PerformanceResult) []PerformanceResult {
	result := make([]PerformanceResult, len(source))
	copy(result, source)
	for index := range result {
		result[index].DataProfile.Parameters = append(json.RawMessage(nil), source[index].DataProfile.Parameters...)
	}
	return result
}

func cloneRequiredMeasurements(source []RequiredMeasurementResult) []RequiredMeasurementResult {
	result := make([]RequiredMeasurementResult, len(source))
	copy(result, source)
	for index := range result {
		result[index].DataProfile.Parameters = append(json.RawMessage(nil), source[index].DataProfile.Parameters...)
		result[index].Metrics = append([]Metric(nil), source[index].Metrics...)
		result[index].Strata = append([]StratumResult(nil), source[index].Strata...)
		for stratumIndex := range result[index].Strata {
			result[index].Strata[stratumIndex].Parameters = append(json.RawMessage(nil), source[index].Strata[stratumIndex].Parameters...)
			result[index].Strata[stratumIndex].Observations = append([]MeasurementObservation(nil), source[index].Strata[stratumIndex].Observations...)
			for observationIndex := range result[index].Strata[stratumIndex].Observations {
				result[index].Strata[stratumIndex].Observations[observationIndex].MetricValues = append([]MetricValue(nil), source[index].Strata[stratumIndex].Observations[observationIndex].MetricValues...)
			}
		}
	}
	return result
}
