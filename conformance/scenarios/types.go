// Package scenarios loads authored conformance scenarios without production dependencies.
package scenarios

import (
	"encoding/json"

	"github.com/trainstar/synchro/conformance/internal/contract"
)

// Stable identifiers that are local to the scenario schema.
type (
	StepID         string
	ExpectationID  string
	BarrierID      string
	NativeActionID string
)

// Scenario is one schema-valid authored conformance scenario.
type Scenario struct {
	SchemaURI           string                        `json:"$schema"`
	SchemaVersion       int                           `json:"schema_version"`
	ID                  contract.ScenarioID           `json:"id"`
	Title               string                        `json:"title"`
	Description         string                        `json:"description,omitempty"`
	RequirementIDs      []contract.RequirementID      `json:"requirement_ids"`
	NormativeReferences []contract.NormativeReference `json:"normative_references"`
	ProofTypes          []string                      `json:"proof_types"`
	ProofObligations    []ProofObligation             `json:"proof_obligations"`
	Ownership           []Ownership                   `json:"ownership"`
	Model               ModelSpec                     `json:"model"`
	BarrierPlan         BarrierPlan                   `json:"barrier_plan"`
	FaultPlans          []FaultPlan                   `json:"fault_plans"`
	Replay              ReplaySpec                    `json:"replay"`
	NegativeControls    []NegativeControl             `json:"negative_controls"`
	Steps               []Step                        `json:"steps"`
	WireExpectations    []WireExpectation             `json:"wire_expectations"`
	Assertions          []Assertion                   `json:"assertions"`
	NativeExecution     *NativeExecutionPlan          `json:"native_execution,omitempty"`

	sourcePath  string
	sourceBytes []byte
	makeTargets map[string]struct{}
}

// NativeExecutionPlan is the shared public-driver plan for every native support cell.
type NativeExecutionPlan struct {
	Version int            `json:"version"`
	Clients []NativeClient `json:"clients"`
	Actions []NativeAction `json:"actions"`
}

// NativeClient binds a stable scenario identity to one durable SQLite database.
type NativeClient struct {
	Key         string `json:"key"`
	UserID      string `json:"user_id"`
	ClientID    string `json:"client_id"`
	DatabaseKey string `json:"database_key"`
}

// NativeAction is one ordered call through a closed platform-driver boundary.
type NativeAction struct {
	ID            NativeActionID  `json:"id"`
	Phase         string          `json:"phase"`
	Actor         string          `json:"actor"`
	Command       string          `json:"command"`
	CoversStepIDs []StepID        `json:"covers_step_ids"`
	Parameters    json.RawMessage `json:"parameters"`
}

type NativeClientOpenParameters struct {
	ClientKey      string  `json:"client_key"`
	DatabaseMode   string  `json:"database_mode"`
	Initialization string  `json:"initialization"`
	SeedStepID     *StepID `json:"seed_step_id"`
}

type NativeClientParameters struct {
	ClientKey string `json:"client_key"`
}

type NativeSynchronizeParameters struct {
	ClientKey  string `json:"client_key"`
	Method     string `json:"method"`
	Completion string `json:"completion"`
}

type NativeCallID string

type NativeBeginCallParameters struct {
	ClientKey string       `json:"client_key"`
	CallID    NativeCallID `json:"call_id"`
	Method    string       `json:"method"`
}

type NativeAwaitCallParameters struct {
	ClientKey  string       `json:"client_key"`
	CallID     NativeCallID `json:"call_id"`
	Completion string       `json:"completion"`
}

type NativeAwaitStepParameters struct {
	ClientKey string        `json:"client_key"`
	CallID    *NativeCallID `json:"call_id,omitempty"`
}

type NativeLifecycleParameters struct {
	ClientKey string `json:"client_key"`
	Operation string `json:"operation"`
}

type NativeProcessStepParameters struct {
	ClientKey *string `json:"client_key"`
}

type NativeProcessBoundaryParameters struct {
	ClientKey     string         `json:"client_key"`
	Boundary      string         `json:"boundary"`
	AfterActionID NativeActionID `json:"after_action_id"`
}

type NativeCaptureParameters struct {
	ClientKeys     []string        `json:"client_keys"`
	Sources        []string        `json:"sources"`
	ExpectationIDs []ExpectationID `json:"expectation_ids"`
}

type NativeMeasureParameters struct {
	PerformanceBudgetIDs []contract.BudgetID      `json:"performance_budget_ids"`
	MeasurementIDs       []contract.MeasurementID `json:"measurement_ids"`
}

type Operation struct {
	ContractOperation string          `json:"contract_operation"`
	Name              string          `json:"name"`
	Payload           json.RawMessage `json:"payload"`
}

type ProofObligation struct {
	ObligationID           contract.ObligationID          `json:"obligation_id"`
	RequirementIDs         []contract.RequirementID       `json:"requirement_ids"`
	AssertionIDs           []contract.AssertionID         `json:"assertion_ids"`
	ProofType              string                         `json:"proof_type"`
	SupportCellID          *contract.SupportCellID        `json:"support_cell_id"`
	ArtifactInventoryIDs   []contract.ArtifactInventoryID `json:"artifact_inventory_ids"`
	PerformanceBudgetIDs   []contract.BudgetID            `json:"performance_budget_ids"`
	RequiredMeasurementIDs []contract.MeasurementID       `json:"required_measurement_ids"`
	RequiredVectorSetIDs   []contract.VectorSetID         `json:"required_vector_set_ids"`
	MakeTarget             string                         `json:"make_target"`
	Argv                   []string                       `json:"argv"`
	FaultPlanID            *contract.FaultPlanID          `json:"fault_plan_id"`
	ControlID              *contract.ControlID            `json:"control_id"`
}

// Ownership is an authored tuple. It is not an inferred Cartesian product.
type Ownership struct {
	ScenarioID        contract.ScenarioID     `json:"scenario_id"`
	RequirementID     contract.RequirementID  `json:"requirement_id"`
	ProofObligationID contract.ObligationID   `json:"proof_obligation_id"`
	AssertionID       contract.AssertionID    `json:"assertion_id"`
	ProofType         string                  `json:"proof_type"`
	SupportCellID     *contract.SupportCellID `json:"support_cell_id"`
}

type ModelSpec struct {
	Setup         []Operation        `json:"setup"`
	ExpectedState []ModelExpectation `json:"expected_state"`
}

type ModelExpectation struct {
	ID         ExpectationID `json:"id"`
	Predicate  Predicate     `json:"predicate"`
	StateFacts *StateFacts   `json:"state_facts,omitempty"`
}

// StateFacts is a closed partial projection of contract-relevant durable state.
// Omitted families are not part of the expectation.
type StateFacts struct {
	TransactionCount *uint64                `json:"transaction_count,omitempty"`
	RowCount         *uint64                `json:"row_count,omitempty"`
	ScopeCount       *uint64                `json:"scope_count,omitempty"`
	RebuildCount     *uint64                `json:"rebuild_count,omitempty"`
	BatchCount       *uint64                `json:"batch_count,omitempty"`
	MutationCount    *uint64                `json:"mutation_count,omitempty"`
	ConfiguredLimits *ConfiguredLimitsFact  `json:"configured_limits,omitempty"`
	Transactions     []TransactionFact      `json:"transactions,omitempty"`
	Registry         *RegistryFact          `json:"registry,omitempty"`
	Stream           *StreamFact            `json:"stream,omitempty"`
	Rows             []RowFact              `json:"rows,omitempty"`
	Scopes           []ScopeFact            `json:"scopes,omitempty"`
	Poison           []PoisonFact           `json:"poison,omitempty"`
	Rebuilds         []RebuildFact          `json:"rebuilds,omitempty"`
	Clients          []ClientDurabilityFact `json:"clients,omitempty"`
}

type ConfiguredLimitsFact struct {
	MaxScopeFanout         uint64 `json:"max_scope_fanout"`
	MaxImpactRows          uint64 `json:"max_impact_rows"`
	PullMaximum            uint64 `json:"pull_maximum"`
	RebuildMaximum         uint64 `json:"rebuild_maximum"`
	CompactionBatchMaximum uint64 `json:"compaction_batch_maximum"`
	BackfillBatchMaximum   uint64 `json:"backfill_batch_maximum"`
}

type TransactionFact struct {
	StreamGeneration   string   `json:"stream_generation"`
	CommitLSN          string   `json:"commit_lsn"`
	EndLSN             string   `json:"end_lsn"`
	RegistryGeneration uint64   `json:"registry_generation"`
	Lifecycle          string   `json:"lifecycle"`
	EventOrdinals      []uint64 `json:"event_ordinals"`
}

type RegistryFact struct {
	CurrentGeneration uint64 `json:"current_generation"`
}

type StreamFact struct {
	MaterializedStreamGeneration string `json:"materialized_stream_generation"`
	MaterializedKind             string `json:"materialized_kind"`
	MaterializedCommitLSN        string `json:"materialized_commit_lsn"`
	AcknowledgedEndLSN           string `json:"acknowledged_end_lsn"`
}

type RowFact struct {
	TableID           string `json:"table_id"`
	CanonicalWireJSON string `json:"canonical_wire_json"`
	Version           string `json:"version"`
	Checksum          string `json:"checksum"`
}

type ScopeFact struct {
	ScopeID              string   `json:"scope_id"`
	MembershipGeneration uint64   `json:"membership_generation"`
	Cardinality          uint64   `json:"cardinality"`
	EffectVersions       []string `json:"effect_versions"`
}

type PoisonFact struct {
	StreamGeneration string  `json:"stream_generation"`
	CommitLSN        string  `json:"commit_lsn"`
	Relation         *string `json:"relation"`
	Reason           string  `json:"reason"`
	Lifecycle        string  `json:"lifecycle"`
}

type RebuildFact struct {
	UserID          string `json:"user_id"`
	ClientID        string `json:"client_id"`
	ScopeID         string `json:"scope_id"`
	RebuildID       string `json:"rebuild_id"`
	PageLimit       uint64 `json:"page_limit"`
	StagedRowCount  uint64 `json:"staged_row_count"`
	PageCount       uint64 `json:"page_count"`
	NextRowOrdinal  uint64 `json:"next_row_ordinal"`
	HasContinuation bool   `json:"has_continuation"`
	HasFinalCursor  bool   `json:"has_final_cursor"`
	Status          string `json:"status"`
}

type ClientDurabilityFact struct {
	UserID              string                `json:"user_id"`
	ClientID            string                `json:"client_id"`
	CurrentSchema       *SchemaFact           `json:"current_schema,omitempty"`
	RowCount            *uint64               `json:"row_count,omitempty"`
	ProvenanceCount     *uint64               `json:"provenance_count,omitempty"`
	CheckpointCount     *uint64               `json:"checkpoint_count,omitempty"`
	QueueCount          *uint64               `json:"queue_count,omitempty"`
	OutcomeCount        *uint64               `json:"outcome_count,omitempty"`
	SealedBatchCount    *uint64               `json:"sealed_batch_count,omitempty"`
	RebuildAttemptCount *uint64               `json:"rebuild_attempt_count,omitempty"`
	Provenance          []ProvenanceFact      `json:"provenance,omitempty"`
	Checkpoints         []CheckpointFact      `json:"checkpoints,omitempty"`
	Queue               []QueuedMutationFact  `json:"queue,omitempty"`
	Outcomes            []MutationOutcomeFact `json:"outcomes,omitempty"`
}

type SchemaFact struct {
	Version uint64 `json:"version"`
	Hash    string `json:"hash"`
}

type ProvenanceFact struct {
	TableID           string   `json:"table_id"`
	CanonicalWireJSON string   `json:"canonical_wire_json"`
	Scopes            []string `json:"scopes"`
	Version           string   `json:"version"`
}

type CheckpointFact struct {
	ScopeID     string  `json:"scope_id"`
	HasCursor   bool    `json:"has_cursor"`
	HasChecksum bool    `json:"has_checksum"`
	Checksum    *string `json:"checksum,omitempty"`
	Verified    bool    `json:"verified"`
}

type QueuedMutationFact struct {
	MutationID        string      `json:"mutation_id"`
	TableID           string      `json:"table_id"`
	CanonicalWireJSON string      `json:"canonical_wire_json"`
	AuthoredSchema    SchemaFact  `json:"authored_schema"`
	Operation         string      `json:"operation"`
	BaseVersion       *string     `json:"base_version"`
	ClientVersion     string      `json:"client_version"`
	AuthoredColumns   []FieldFact `json:"authored_columns"`
	LocalOrder        uint64      `json:"local_order"`
	Status            string      `json:"status"`
}

type FieldFact struct {
	FieldID  string `json:"field_id"`
	Type     string `json:"type"`
	WireJSON string `json:"wire_json"`
}

type MutationOutcomeFact struct {
	MutationID string `json:"mutation_id"`
	State      string `json:"state"`
	Reason     string `json:"reason"`
}

type Predicate struct {
	ContractPredicate string          `json:"contract_predicate"`
	Name              string          `json:"name"`
	Payload           json.RawMessage `json:"payload"`
}

// SchemaDispatchMeasurementPlan binds a schema-dispatch predicate to the
// authored performance measurement and its required semantic cases.
type SchemaDispatchMeasurementPlan struct {
	MeasurementID                contract.MeasurementID             `json:"measurement_id"`
	MinimumSampleCountPerStratum uint64                             `json:"minimum_sample_count_per_stratum"`
	Strata                       []SchemaDispatchMeasurementStratum `json:"strata"`
}

// SchemaDispatchMeasurementStratum binds one authored stratum to its semantic
// schema-dispatch case.
type SchemaDispatchMeasurementStratum struct {
	StratumID  contract.StratumID `json:"stratum_id"`
	SchemaCase string             `json:"schema_case"`
}

// MeasurementSample binds one executable step to one authored measurement
// stratum and its exact parameter set.
type MeasurementSample struct {
	MeasurementID contract.MeasurementID `json:"measurement_id"`
	StratumID     contract.StratumID     `json:"stratum_id"`
	SampleID      string                 `json:"sample_id"`
	Parameters    json.RawMessage        `json:"parameters"`
}

type BarrierPlan struct {
	Barriers []Barrier `json:"barriers"`
}

type Barrier struct {
	ID           BarrierID `json:"id"`
	Name         string    `json:"name"`
	ReleaseOrder int       `json:"release_order"`
	Participants []string  `json:"participants"`
}

type FaultPlan struct {
	ID                   contract.FaultPlanID   `json:"id"`
	RequirementID        contract.RequirementID `json:"requirement_id"`
	FaultID              contract.FaultID       `json:"fault_id"`
	ControlID            contract.ControlID     `json:"control_id"`
	BarrierID            BarrierID              `json:"barrier_id"`
	ExpectedAssertionIDs []contract.AssertionID `json:"expected_assertion_ids"`
	Injection            InjectionRecipe        `json:"injection"`
}

type InjectionRecipe struct {
	Mechanism  string              `json:"mechanism"`
	Target     string              `json:"target"`
	Operator   string              `json:"operator"`
	Parameters InjectionParameters `json:"parameters"`
}

type InjectionParameters struct {
	Scenario     string `json:"scenario"`
	Defect       string `json:"defect"`
	Precondition string `json:"precondition,omitempty"`
}

type ReplaySpec struct {
	Mode                 string `json:"mode"`
	SeedRequired         bool   `json:"seed_required"`
	BarrierTraceRequired bool   `json:"barrier_trace_required"`
}

type NegativeControl struct {
	ControlID                   contract.ControlID             `json:"control_id"`
	RequirementID               contract.RequirementID         `json:"requirement_id"`
	FaultID                     contract.FaultID               `json:"fault_id"`
	SubjectArtifactInventoryIDs []contract.ArtifactInventoryID `json:"subject_artifact_inventory_ids"`
	DetectedBy                  []contract.AssertionID         `json:"detected_by"`
}

type Step struct {
	ID                StepID             `json:"id"`
	Phase             string             `json:"phase"`
	Transport         string             `json:"transport"`
	Description       string             `json:"description,omitempty"`
	MeasurementSample *MeasurementSample `json:"measurement_sample,omitempty"`
	Operation         Operation          `json:"operation"`
	ExpectedOutcome   ExpectedOutcome    `json:"expected_outcome"`
}

type ExpectedOutcome struct {
	Disposition string  `json:"disposition"`
	ErrorCode   *string `json:"error_code,omitempty"`
}

type WireExpectation struct {
	StepID       StepID               `json:"step_id"`
	AssertionID  contract.AssertionID `json:"assertion_id"`
	ContractCase string               `json:"contract_case"`
	HTTPStatus   int                  `json:"http_status"`
	ErrorCode    *string              `json:"error_code"`
	Retryable    bool                 `json:"retryable"`
}

type Assertion struct {
	ID                contract.AssertionID     `json:"id"`
	RequirementIDs    []contract.RequirementID `json:"requirement_ids"`
	Description       string                   `json:"description"`
	ExpectationIDs    []ExpectationID          `json:"expectation_ids"`
	Predicate         Predicate                `json:"predicate"`
	Oracle            Oracle                   `json:"oracle"`
	DetectsControlIDs []contract.ControlID     `json:"detects_control_ids"`
}

type Oracle struct {
	Kind           string `json:"kind"`
	ExpectedSource string `json:"expected_source"`
	ObservedSource string `json:"observed_source"`
}
