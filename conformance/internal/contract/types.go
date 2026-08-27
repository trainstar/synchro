package contract

import (
	"encoding/json"
)

// Bundle is the complete typed set of authored contract inputs.
type Bundle struct {
	Requirements Requirements       `json:"requirements"`
	Support      SupportMatrix      `json:"support"`
	Faults       FaultCatalog       `json:"faults"`
	Artifacts    ArtifactInventory  `json:"artifacts"`
	Performance  PerformanceCatalog `json:"performance"`

	sources                bundleSources
	performanceFingerprint [32]byte
}

type bundleSources struct {
	catalogs   map[string][]byte
	schemas    map[string][]byte
	behavioral map[string][]byte
}

// Requirements is conformance/requirements.json.
type Requirements struct {
	SchemaURI     string        `json:"$schema"`
	SchemaVersion int           `json:"schema_version"`
	Release       string        `json:"release"`
	Requirements  []Requirement `json:"requirements"`
}

// Requirement is one normative invariant requirement.
type Requirement struct {
	ID                   RequirementID        `json:"id"`
	Title                string               `json:"title"`
	Category             string               `json:"category"`
	Statement            string               `json:"statement"`
	RequiredProofTypes   []string             `json:"required_proof_types"`
	ApplicableComponents []string             `json:"applicable_components"`
	NormativeReferences  []NormativeReference `json:"normative_references"`
}

// NormativeReference binds a requirement to a Markdown or MDX heading.
type NormativeReference struct {
	Path   string `json:"path"`
	Anchor string `json:"anchor"`
}

// SupportMatrix is conformance/support-matrix.json.
type SupportMatrix struct {
	SchemaURI             string             `json:"$schema"`
	SchemaVersion         int                `json:"schema_version"`
	Release               string             `json:"release"`
	CurrentTrackPolicy    CurrentTrackPolicy `json:"current_track_policy"`
	SemanticCorpusCellIDs []SupportCellID    `json:"semantic_corpus_cell_ids"`
	Cells                 []SupportCell      `json:"cells"`
}

type CurrentTrackPolicy struct {
	Selector              string `json:"selector"`
	ResolveAt             string `json:"resolve_at"`
	RecordExactVersionsIn string `json:"record_exact_versions_in"`
}

// SupportCell describes one locked platform support policy cell.
type SupportCell struct {
	ID                    SupportCellID    `json:"id"`
	Component             string           `json:"component"`
	Platform              string           `json:"platform"`
	PlatformVersion       *VersionSelector `json:"platform_version,omitempty"`
	RuntimeVersion        *VersionSelector `json:"runtime_version,omitempty"`
	ExtensionArchitecture string           `json:"extension_architecture,omitempty"`
	Policy                string           `json:"policy"`
	Note                  string           `json:"note,omitempty"`
}

// VersionSelector is one exact, minimum, series, or current-stable selector.
type VersionSelector struct {
	Kind  string `json:"kind"`
	Value string `json:"value,omitempty"`
}

// FaultCatalog is conformance/faults/catalog.json.
type FaultCatalog struct {
	SchemaURI     string    `json:"$schema"`
	SchemaVersion int       `json:"schema_version"`
	Release       string    `json:"release"`
	Faults        []Fault   `json:"faults"`
	Controls      []Control `json:"controls"`
}

type Fault struct {
	ID          FaultID `json:"id"`
	Description string  `json:"description"`
}

type Control struct {
	ID                  ControlID       `json:"id"`
	FaultID             FaultID         `json:"fault_id"`
	SubjectType         string          `json:"subject_type"`
	RequirementIDs      []RequirementID `json:"requirement_ids"`
	NormativeReferences []string        `json:"normative_references"`
	Injection           FaultInjection  `json:"injection"`
	ExpectedDetection   string          `json:"expected_detection"`
}

type FaultInjection struct {
	Mechanism  string                   `json:"mechanism"`
	Target     string                   `json:"target"`
	Operator   string                   `json:"operator"`
	Parameters FaultInjectionParameters `json:"parameters"`
}

type FaultInjectionParameters struct {
	Scenario     string `json:"scenario"`
	Defect       string `json:"defect"`
	Precondition string `json:"precondition,omitempty"`
}

// ArtifactInventory is conformance/artifacts/inventory.json.
type ArtifactInventory struct {
	SchemaURI     string                  `json:"$schema"`
	SchemaVersion int                     `json:"schema_version"`
	Release       string                  `json:"release"`
	Artifacts     []ArtifactInventoryItem `json:"artifacts"`
}

type ArtifactInventoryItem struct {
	ID   ArtifactInventoryID `json:"id"`
	Role string              `json:"role"`
	Name string              `json:"name"`
}

// PerformanceCatalog is conformance/performance/budgets.json.
type PerformanceCatalog struct {
	SchemaURI            string                `json:"$schema"`
	SchemaVersion        int                   `json:"schema_version"`
	Release              string                `json:"release"`
	Budgets              []PerformanceBudget   `json:"budgets"`
	RequiredMeasurements []RequiredMeasurement `json:"required_measurements"`
}

type PerformanceBudget struct {
	ID                   BudgetID              `json:"id"`
	ScenarioID           ScenarioID            `json:"scenario_id"`
	SupportCellIDs       []SupportCellID       `json:"support_cell_ids"`
	ArtifactInventoryIDs []ArtifactInventoryID `json:"artifact_inventory_ids"`
	Metric               string                `json:"metric"`
	Unit                 string                `json:"unit"`
	Comparator           string                `json:"comparator"`
	Limit                json.Number           `json:"limit"`
	DataProfile          DataProfile           `json:"data_profile"`
	MeasurementMethod    MeasurementMethod     `json:"measurement_method"`
}

type RequiredMeasurement struct {
	ID                           MeasurementID         `json:"id"`
	ScenarioID                   ScenarioID            `json:"scenario_id"`
	SupportCellIDs               []SupportCellID       `json:"support_cell_ids"`
	ArtifactInventoryIDs         []ArtifactInventoryID `json:"artifact_inventory_ids"`
	DataProfile                  DataProfile           `json:"data_profile"`
	MeasurementMethod            MeasurementMethod     `json:"measurement_method"`
	Metrics                      []PerformanceMetric   `json:"metrics"`
	Strata                       []PerformanceStratum  `json:"strata"`
	MinimumSampleCountPerStratum json.Number           `json:"minimum_sample_count_per_stratum"`
}

type DataProfile struct {
	ProfileType string          `json:"profile_type"`
	Parameters  json.RawMessage `json:"parameters"`
}

type MeasurementMethod struct {
	MethodType      string `json:"method_type"`
	Instrumentation string `json:"instrumentation"`
	Aggregation     string `json:"aggregation"`
}

type PerformanceMetric struct {
	ID   MetricID `json:"id"`
	Name string   `json:"name"`
	Unit string   `json:"unit"`
}

type PerformanceStratum struct {
	StratumID  StratumID       `json:"stratum_id"`
	Parameters json.RawMessage `json:"parameters"`
}

// FileBinding binds a repository-relative regular file to its raw SHA-256.
type FileBinding struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

// BehavioralBinding additionally records the frontmatter status of an ADR.
// A nil Status is the required JSON null used for specification files.
type BehavioralBinding struct {
	Path   string  `json:"path"`
	SHA256 string  `json:"sha256"`
	Status *string `json:"status"`
}

type VerificationInputs struct {
	ScenarioCatalog    FileBinding `json:"scenario_catalog"`
	VectorCatalog      FileBinding `json:"vector_catalog"`
	FaultCatalog       FileBinding `json:"fault_catalog"`
	PerformanceBudgets FileBinding `json:"performance_budgets"`
	ArtifactInventory  FileBinding `json:"artifact_inventory"`
}

type SchemaFiles struct {
	Requirements       FileBinding `json:"requirements"`
	SupportMatrix      FileBinding `json:"support_matrix"`
	Scenario           FileBinding `json:"scenario"`
	CISummary          FileBinding `json:"ci_summary"`
	RCCandidateLock    FileBinding `json:"rc_candidate_lock"`
	RCManifest         FileBinding `json:"rc_manifest"`
	FaultCatalog       FileBinding `json:"fault_catalog"`
	ArtifactInventory  FileBinding `json:"artifact_inventory"`
	PerformanceBudgets FileBinding `json:"performance_budgets"`
	VectorCatalog      FileBinding `json:"vector_catalog"`
}

// Snapshot is the exact contract snapshot_binding shape. It deliberately does
// not include a self-referential digest or generated-file fields.
type Snapshot struct {
	ReleaseVersion     string              `json:"release_version"`
	ProtocolVersion    int                 `json:"protocol_version"`
	Requirements       FileBinding         `json:"requirements"`
	SupportMatrix      FileBinding         `json:"support_matrix"`
	BehavioralFiles    []BehavioralBinding `json:"behavioral_files"`
	VerificationInputs VerificationInputs  `json:"verification_inputs"`
	SchemaFiles        SchemaFiles         `json:"schema_files"`
}
