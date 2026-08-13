package reference

import "time"

// Identifiers are typed so unrelated state keys cannot be mixed.
type (
	UserID              string
	ClientID            string
	RelationID          string
	ScopeID             string
	TableID             string
	FieldID             string
	IndexID             string
	BatchID             string
	MutationID          string
	RebuildID           string
	SessionID           string
	ExportID            string
	StreamResetID       string
	TransactionNonce    string
	FenceID             string
	CapabilityID        string
	RoleID              string
	Capability          string
	DatabaseName        string
	Endpoint            string
	WorkerID            string
	SlotID              string
	LimitID             string
	CheckID             string
	ScopeRuleID         string
	CaptureDependencyID string
	DependencyImpactID  string
	LocalOnlyRowID      string
	FunctionID          string
	PortableType        string
	ReasonCode          string
	StreamGeneration    string
	RowVersion          string
	ClientVersion       string
	Generation          uint64
	ScopeSetVersion     uint64
	AcceptedWriteEpoch  uint64
	Cardinality         uint64
	CommitLSN           uint64
	EndLSN              uint64
	EventOrdinal        uint64
	EffectOrdinal       uint64
	Checksum            [32]byte
	Fingerprint         [32]byte
)

// SchemaRef identifies one immutable schema manifest.
type SchemaRef struct {
	Version uint64
	Hash    [32]byte
}

// RowIdentity retains the complete canonical identity framing and inspectable terms.
type RowIdentity struct {
	CanonicalIdentityBytes string
	TableID                TableID
	PrimaryKeyFieldID      FieldID
	PortableType           PortableType
	CanonicalWireJSON      string
}

// PositionKind distinguishes the committed stream boundaries and effects.
type PositionKind string

const (
	PositionKindUnknown         PositionKind = "unknown"
	PositionKindGenerationStart PositionKind = "generation_start"
	PositionKindEffect          PositionKind = "effect"
	PositionKindTransactionEnd  PositionKind = "transaction_end"
)

// StreamPosition identifies an ordered position in an opaque stream generation.
type StreamPosition struct {
	StreamGeneration StreamGeneration
	Kind             PositionKind
	CommitLSN        CommitLSN
	EventOrdinal     EventOrdinal
	EffectOrdinal    EffectOrdinal
}

// TransactionReplayKey is the durable identity of one source transaction.
type TransactionReplayKey struct {
	StreamGeneration StreamGeneration
	CommitLSN        CommitLSN
}

// EventReplayKey is the durable identity of one source event.
type EventReplayKey struct {
	Transaction  TransactionReplayKey
	EventOrdinal EventOrdinal
}

// ClientKey identifies a client under its authenticated user identity.
type ClientKey struct {
	UserID   UserID
	ClientID ClientID
}

// ProjectionKey identifies one captured relation projection from one source event.
type ProjectionKey struct {
	Relation RelationID
	Event    EventReplayKey
	Image    ProjectionImage
}

// ProjectionImage distinguishes the two relation images from one source event.
type ProjectionImage string

const (
	ProjectionImageBefore ProjectionImage = "before"
	ProjectionImageAfter  ProjectionImage = "after"
)

// BatchKey identifies one sealed batch for one client.
type BatchKey struct {
	Client ClientKey
	Batch  BatchID
}

// MutationKey identifies a mutation across all batches for one client.
type MutationKey struct {
	Client   ClientKey
	Mutation MutationID
}

// RebuildKey identifies one client scope rebuild.
type RebuildKey struct {
	Client  ClientKey
	Scope   ScopeID
	Rebuild RebuildID
}

// SeedRecordKey identifies one record in one portable seed export.
type SeedRecordKey struct {
	Export  ExportID
	Scope   ScopeID
	Ordinal uint64
}

// LocalOnlyRowKey identifies a row that cannot enter authoritative sync state.
type LocalOnlyRowKey struct {
	Table TableID
	Row   LocalOnlyRowID
}

// OpaqueToken is an internal comparable handle with no public token representation.
type OpaqueToken struct {
	namespace uint64
	sequence  uint64
}

type TokenKind string

const (
	TokenKindIncrementalCursor   TokenKind = "incremental_cursor"
	TokenKindRebuildContinuation TokenKind = "rebuild_continuation"
	TokenKindSeedPage            TokenKind = "seed_page"
	TokenKindSeedReceipt         TokenKind = "seed_receipt"
)

// TokenStatus reports validation without exposing token internals.
type TokenStatus uint8

const (
	TokenStatusValid TokenStatus = iota + 1
	TokenStatusStale
	TokenStatusWrongKind
	TokenStatusForged
	TokenStatusMisbound
)

// BindingSet contains every optional token binding as a comparable value.
type BindingSet struct {
	HasUser                 bool
	User                    UserID
	HasClient               bool
	Client                  ClientKey
	HasClientGeneration     bool
	ClientGeneration        Generation
	HasRegistryGeneration   bool
	RegistryGeneration      Generation
	HasMembershipGeneration bool
	MembershipGeneration    Generation
	HasRetentionGeneration  bool
	RetentionGeneration     Generation
	HasStreamGeneration     bool
	StreamGeneration        StreamGeneration
	HasSchema               bool
	Schema                  SchemaRef
	HasScope                bool
	Scope                   ScopeID
	HasStreamPosition       bool
	StreamPosition          StreamPosition
	HasSnapshotBoundary     bool
	SnapshotBoundary        StreamPosition
	HasSessionID            bool
	SessionID               SessionID
	HasRebuildID            bool
	RebuildID               RebuildID
	HasExportID             bool
	ExportID                ExportID
	HasTransactionNonce     bool
	TransactionNonce        TransactionNonce
	HasExportManifestHash   bool
	ExportManifestHash      [32]byte
	HasIssuedAt             bool
	IssuedAt                time.Time
	HasExpiresAt            bool
	ExpiresAt               time.Time
	HasOrdinal              bool
	Ordinal                 uint64
	HasPageLimit            bool
	PageLimit               uint32
	HasAcceptedWriteEpoch   bool
	AcceptedWriteEpoch      AcceptedWriteEpoch
	HasCardinality          bool
	Cardinality             Cardinality
	HasChecksum             bool
	Checksum                Checksum
}

// SchemaClass uses the protocol class names.
type SchemaClass string

const (
	SchemaClassInitial SchemaClass = "initial"
	SchemaClass2       SchemaClass = "class_2"
	SchemaClass3       SchemaClass = "class_3"
	SchemaClass4       SchemaClass = "class_4"
)

type Composition string

const (
	CompositionSingleScope Composition = "single_scope"
	CompositionMultiScope  Composition = "multi_scope"
)

type RegistrationKind string

const (
	RegistrationKindSynced            RegistrationKind = "synced"
	RegistrationKindCaptureDependency RegistrationKind = "capture_dependency"
)

type ReplicaIdentity string

const (
	ReplicaIdentityDefault ReplicaIdentity = "default"
	ReplicaIdentityNothing ReplicaIdentity = "nothing"
	ReplicaIdentityFull    ReplicaIdentity = "full"
	ReplicaIdentityIndex   ReplicaIdentity = "index"
)

type DMLOperation string

const (
	DMLOperationInsert DMLOperation = "insert"
	DMLOperationUpdate DMLOperation = "update"
	DMLOperationDelete DMLOperation = "delete"
)

type FenceCoverageState string

const (
	FenceCoveragePending       FenceCoverageState = "pending"
	FenceCoverageMaterialized  FenceCoverageState = "materialized"
	FenceCoverageResetBaseline FenceCoverageState = "reset_baseline"
)

// BatchExecutionState identifies whether a sealed batch has a replayable response.
type BatchExecutionState string

const (
	BatchExecutionExecuting BatchExecutionState = "executing"
	BatchExecutionCompleted BatchExecutionState = "completed"
)

// MutationOutcomeState is the complete protocol outcome partition.
type MutationOutcomeState string

const (
	MutationOutcomeApplied          MutationOutcomeState = "applied"
	MutationOutcomeConflict         MutationOutcomeState = "conflict"
	MutationOutcomeRejectedTerminal MutationOutcomeState = "rejected_terminal"
)

type RebuildStatus string

const (
	RebuildStatusStaged      RebuildStatus = "staged"
	RebuildStatusComplete    RebuildStatus = "complete"
	RebuildStatusExpired     RebuildStatus = "expired"
	RebuildStatusInvalidated RebuildStatus = "invalidated"
)

// ClientLifecycle is the complete ADR 005 local state set.
type ClientLifecycle string

const (
	ClientLifecycleUninitialized  ClientLifecycle = "uninitialized"
	ClientLifecycleLocalReady     ClientLifecycle = "local_ready"
	ClientLifecycleConnecting     ClientLifecycle = "connecting"
	ClientLifecycleSchemaApplying ClientLifecycle = "schema_applying"
	ClientLifecycleReady          ClientLifecycle = "ready"
	ClientLifecyclePushing        ClientLifecycle = "pushing"
	ClientLifecyclePulling        ClientLifecycle = "pulling"
	ClientLifecycleRebuilding     ClientLifecycle = "rebuilding"
	ClientLifecycleBackoff        ClientLifecycle = "backoff"
	ClientLifecycleError          ClientLifecycle = "error"
	ClientLifecycleStopped        ClientLifecycle = "stopped"
)

type ModelEventKind string

const (
	ModelEventConnected         ModelEventKind = "connected"
	ModelEventMutationApplied   ModelEventKind = "mutation_applied"
	ModelEventMutationRejected  ModelEventKind = "mutation_rejected"
	ModelEventPulled            ModelEventKind = "pulled"
	ModelEventRebuildCompleted  ModelEventKind = "rebuild_completed"
	ModelEventResponseLoss      ModelEventKind = "response_loss"
	ModelEventTransportFailure  ModelEventKind = "transport_failure"
	ModelEventLocalApplyFailure ModelEventKind = "local_apply_failure"
	ModelEventProcessDeath      ModelEventKind = "process_death"
	ModelEventRestart           ModelEventKind = "restart"
	ModelEventBackgrounding     ModelEventKind = "backgrounding"
	ModelEventConnectivityLoss  ModelEventKind = "connectivity_loss"
	ModelEventRecovery          ModelEventKind = "recovery"
	ModelEventWorkerRestart     ModelEventKind = "worker_restart"
)

type SchemaManifest struct {
	Body               []byte
	Parent             *SchemaRef
	Tables             []TableManifest
	AffectedScopes     []ScopeID
	Class              SchemaClass
	CompatibilityFloor uint64
}

type TableManifest struct {
	ID                TableID
	Relation          RelationID
	Name              string
	Composition       Composition
	PrimaryKeyFieldID FieldID
	CreatedFieldID    *FieldID
	UpdatedFieldID    *FieldID
	DeletedFieldID    *FieldID
	Fields            []FieldManifest
	Indexes           []IndexManifest
}

type FieldManifest struct {
	ID                  FieldID
	Name                string
	PortableType        PortableType
	PrimaryKey          bool
	Nullable            bool
	Writable            bool
	HasDecimalPrecision bool
	DecimalPrecision    uint32
	HasDecimalScale     bool
	DecimalScale        uint32
	DefaultWireJSON     *string
}

type IndexManifest struct {
	ID     IndexID
	Name   string
	Fields []FieldID
	Unique bool
}

type RegistryState struct {
	CurrentGeneration Generation
	Generations       []RegistryGenerationState
}

type RegistryGenerationState struct {
	Generation          Generation
	ActivationBoundary  StreamPosition
	Validated           bool
	HasBootstrapStage   bool
	BootstrapStage      CandidateProjectionStage
	Relations           []RegistryRelation
	CaptureDependencies []CaptureDependency
	ScopeRules          []ScopeRule
	DependencyImpacts   []DependencyImpact
}

type PhysicalRelation struct {
	Schema          string
	Name            string
	OID             uint32
	ReplicaIdentity ReplicaIdentity
}

// RelationDefinition is the validated relation contract for one registry generation.
type RelationDefinition struct {
	Relation                   RelationID
	RegistrationKind           RegistrationKind
	HasTableID                 bool
	TableID                    TableID
	Physical                   PhysicalRelation
	PrimaryKeyFieldID          FieldID
	PrimaryKeyPhysicalColumn   string
	PrimaryKeyPortableType     PortableType
	CaptureKeyFieldIDs         []FieldID
	CapturedFieldIDs           []FieldID
	MembershipFunction         FunctionID
	PositiveFanoutBound        uint64
	DependencyImpactFunction   FunctionID
	DependencyCapturedFieldIDs []FieldID
	PositiveDependencyRowBound uint64
	Drifted                    bool
	CaptureBlocked             bool
	BlockReason                ReasonCode
}

type RegistryRelation struct {
	Definition RelationDefinition
}

type CaptureDependency struct {
	ID        CaptureDependencyID
	Relation  RelationID
	DependsOn RelationID
}

type ScopeRule struct {
	ID                  ScopeRuleID
	Relation            RelationID
	MembershipFunction  FunctionID
	PositiveFanoutBound uint64
	Evaluations         []MembershipEvaluation
}

// MembershipEvaluation is one deterministic row-specific membership result.
type MembershipEvaluation struct {
	Row    RowIdentity
	Scopes []ScopeID
}

type DependencyImpact struct {
	ID               DependencyImpactID
	Relation         RelationID
	Function         FunctionID
	CapturedFieldIDs []FieldID
	PositiveRowBound uint64
	AffectedRows     []RowIdentity
	RequiresRebuild  bool
}

type RelationState struct {
	Definition          RelationDefinition
	CaptureDependencies []CaptureDependencyID
	ScopeRules          []ScopeRuleID
	DependencyImpacts   []DependencyImpactID
}

type ClientState struct {
	CurrentGeneration  Generation
	Generations        []ClientGenerationState
	Retirement         *PermanentRetirement
	ScopeSetVersion    ScopeSetVersion
	ScopeAssignments   []ScopeAssignment
	Checkpoints        []ClientCheckpoint
	AcceptedWriteEpoch AcceptedWriteEpoch
}

type ClientGenerationState struct {
	Generation               Generation
	CreatedAt                *time.Time
	LastCursorAcknowledgedAt *time.Time
	ExpiresAt                *time.Time
}

type PermanentRetirement struct {
	RetiredAt *time.Time
	Reason    ReasonCode
}

type ScopeAssignment struct {
	Scope                ScopeID
	MembershipGeneration Generation
	RetentionGeneration  Generation
	Assigned             bool
	RebuildRequired      bool
}

type ClientCheckpoint struct {
	Scope       ScopeID
	Position    StreamPosition
	HasCursor   bool
	Cursor      OpaqueToken
	HasChecksum bool
	Checksum    Checksum
	Verified    bool
}

type AuthoritativeRow struct {
	Identity     RowIdentity
	FieldValues  []FieldValue
	Version      RowVersion
	Checksum     Checksum
	Deleted      bool
	DeletedAt    *time.Time
	DeleteReason *string
	UpdatedAt    *time.Time
}

type FieldValue struct {
	Field    FieldID
	Type     PortableType
	WireJSON string
}

type ScopeState struct {
	Schema               SchemaRef
	MembershipGeneration Generation
	RetentionGeneration  Generation
	StreamGeneration     StreamGeneration
	Membership           []ScopeMembership
	Effects              []ScopeEffect
	Cardinality          Cardinality
	Checksum             Checksum
	HighWatermark        StreamPosition
}

type ScopeMembership struct {
	Row        RowIdentity
	Generation Generation
	Included   bool
}

type ScopeEffect struct {
	Position              StreamPosition
	Row                   RowIdentity
	SourceEvent           EventReplayKey
	Operation             EffectOperation
	Version               RowVersion
	HasCapturedProjection bool
	CapturedProjection    ProjectionKey
	HasChecksum           bool
	Checksum              Checksum
}

type EffectOperation string

const (
	EffectOperationDelete EffectOperation = "delete"
	EffectOperationUpsert EffectOperation = "upsert"

	EffectOperationDeleteRank uint8 = 0
	EffectOperationUpsertRank uint8 = 1
)

type StreamState struct {
	Authority          StreamAuthority
	Reset              *StreamReset
	SourceRows         []SourceRowEntry
	Transactions       []StreamTransaction
	TransactionReplays []TransactionReplayRecord
	EventReplays       []EventReplayRecord
	Materializations   []MaterializationRecord
	Acknowledgements   []SlotAcknowledgement
	Poison             []PoisonRecord
}

type StreamAuthority struct {
	ActiveGeneration              StreamGeneration
	GlobalMaterializationBoundary StreamPosition
	AcknowledgedEndLSN            EndLSN
	HasActiveSlot                 bool
	ActiveSlot                    SlotID
}

type StreamResetPhase string

const (
	StreamResetPhasePreparing          StreamResetPhase = "preparing"
	StreamResetPhaseAwaitingActivation StreamResetPhase = "awaiting_activation"
	StreamResetPhaseActive             StreamResetPhase = "active"
	StreamResetPhaseFailed             StreamResetPhase = "failed"
)

type StreamReset struct {
	ID                     StreamResetID
	CandidateSlot          SlotID
	CandidateSlotPermanent bool
	Database               DatabaseName
	Plugin                 string
	ConsistentPoint        CommitLSN
	SnapshotBoundary       StreamPosition
	ActivationBarrier      StreamPosition
	TargetStreamGeneration StreamGeneration
	Phase                  StreamResetPhase
	HasCandidateStage      bool
	CandidateStage         CandidateProjectionStage
}

// CandidateProjectionStage is an isolated candidate graph for a reset or pending registry activation.
type CandidateProjectionStage struct {
	RegistryGeneration Generation
	Schema             SchemaRef
	StreamGeneration   StreamGeneration
	SnapshotBoundary   StreamPosition
	ActivationBarrier  StreamPosition
	Verified           bool
	Rows               []CandidateRowEntry
	Projections        []CandidateProjectionEntry
	Fences             []CandidateFenceEntry
	Scopes             []CandidateScopeEntry
}

type CandidateRowEntry struct {
	Identity RowIdentity
	Row      AuthoritativeRow
}

type CandidateProjectionEntry struct {
	Key        ProjectionKey
	Projection CapturedProjection
}

type CandidateFenceEntry struct {
	ID    FenceID
	Fence VersionFence
}

type CandidateScopeEntry struct {
	Scope ScopeID
	State ScopeState
}

type StreamTransaction struct {
	ReplayKey          TransactionReplayKey
	End                StreamPosition
	EndLSN             EndLSN
	RegistryGeneration Generation
	Lifecycle          TransactionLifecycle
	CommittedAt        *time.Time
	Events             []SourceEvent
}

type TransactionLifecycle string

const (
	TransactionLifecycleCommitted    TransactionLifecycle = "committed"
	TransactionLifecycleMaterialized TransactionLifecycle = "materialized"
	TransactionLifecyclePoisoned     TransactionLifecycle = "poisoned"
)

// SourceRowEntry retains the source relation row that can lead materialization.
type SourceRowEntry struct {
	Identity RowIdentity
	Row      AuthoritativeRow
}

type SourceEvent struct {
	ReplayKey  EventReplayKey
	Position   StreamPosition
	Relation   RelationID
	Operation  DMLOperation
	HasBefore  bool
	Before     SourceImage
	HasAfter   bool
	After      SourceImage
	CapturedAt *time.Time
}

// SourceImage is one registered relation image from an exact source operation.
type SourceImage struct {
	Identity    RegisteredIdentity
	Fields      []FieldValue
	Version     RowVersion
	HasChecksum bool
	Checksum    Checksum
	Deleted     bool
}

type TransactionReplayRecord struct {
	Key                TransactionReplayKey
	RegistryGeneration Generation
	EndLSN             EndLSN
	Completed          bool
	Replayed           bool
}

type EventReplayRecord struct {
	Key      EventReplayKey
	Replayed bool
}

type MaterializationRecord struct {
	Event        EventReplayKey
	Materialized bool
}

type SlotAcknowledgement struct {
	StreamGeneration StreamGeneration
	EndLSN           EndLSN
	AcknowledgedAt   *time.Time
}

type PoisonRecord struct {
	Transaction TransactionReplayKey
	HasRelation bool
	Relation    RelationID
	Reason      ReasonCode
	Lifecycle   PoisonLifecycle
	PoisonedAt  *time.Time
}

type PoisonLifecycle string

const (
	PoisonLifecycleActive   PoisonLifecycle = "active"
	PoisonLifecycleRepaired PoisonLifecycle = "repaired"
)

// CanonicalCaptureKey identifies one capture-dependency key without a table row identity.
type CanonicalCaptureKey struct {
	CanonicalKeyBytes string
}

// RegisteredIdentity carries either a synced row identity or a capture key.
type RegisteredIdentity struct {
	Kind       RegistrationKind
	SyncedRow  RowIdentity
	CaptureKey CanonicalCaptureKey
}

type VersionFence struct {
	ID                       FenceID
	RegistrationKind         RegistrationKind
	Relation                 RelationID
	Physical                 PhysicalRelation
	Operation                DMLOperation
	DMLOrdinal               uint64
	HasOldRegisteredIdentity bool
	OldRegisteredIdentity    RegisteredIdentity
	HasNewRegisteredIdentity bool
	NewRegisteredIdentity    RegisteredIdentity
	RowVersion               RowVersion
	HasEventReplayKey        bool
	EventReplayKey           EventReplayKey
	HasMutationKey           bool
	MutationKey              MutationKey
	Coverage                 FenceCoverageState
	HasResetBaselineCoverage bool
	ResetBaselineCoverage    ResetBaselineCoverage
}

type ResetBaselineCoverage struct {
	ResetID                StreamResetID
	CandidateSlot          SlotID
	SnapshotBoundary       StreamPosition
	TargetStreamGeneration StreamGeneration
}

type CapturedProjection struct {
	Event      EventReplayKey
	Image      ProjectionImage
	Row        RowIdentity
	Fields     []FieldValue
	Version    RowVersion
	Checksum   Checksum
	CapturedAt *time.Time
}

// FingerprintRecord identifies a domain-separated canonical digest.
type FingerprintRecord struct {
	Algorithm string
	Version   uint64
	Domain    string
	Digest    Fingerprint
}

type BatchLedger struct {
	Fingerprint             FingerprintRecord
	ProtocolVersion         int
	ClientGeneration        Generation
	Schema                  SchemaRef
	SealedCanonicalRequest  []byte
	SealedCanonicalResponse []byte
	Execution               BatchExecutionState
	Mutations               []MutationID
	Outcomes                []MutationOutcome
	HTTPStatus              int
	ServerTime              *time.Time
	CreatedAt               *time.Time
	CompletedAt             *time.Time
	SealedAt                *time.Time
}

type MutationLedger struct {
	Fingerprint             FingerprintRecord
	FirstBatch              BatchID
	RequestOrdinal          uint64
	Table                   TableID
	Row                     RowIdentity
	Operation               DMLOperation
	AuthoredSchema          SchemaRef
	SubmittedSchema         SchemaRef
	OutcomeSchema           SchemaRef
	SealedCanonicalRequest  []byte
	SealedCanonicalResponse []byte
	Outcome                 MutationOutcome
	ResolvedAt              *time.Time
}

type MutationOutcome struct {
	Mutation MutationID
	State    MutationOutcomeState
	Reason   ReasonCode
	Response []byte
}

type RebuildSession struct {
	SessionID            SessionID
	ClientGeneration     Generation
	Scope                ScopeID
	Schema               SchemaRef
	MembershipGeneration Generation
	RetentionGeneration  Generation
	StreamGeneration     StreamGeneration
	SnapshotBoundary     StreamPosition
	PageLimit            uint32
	StagedRows           []RebuildStagedRow
	HasContinuation      bool
	Continuation         OpaqueToken
	NextRowOrdinal       uint64
	Checksum             Checksum
	CreatedAt            *time.Time
	ExpiresAt            *time.Time
	AcceptedWriteEpoch   AcceptedWriteEpoch
	Pages                []RebuildPage
	HasFinalCursor       bool
	FinalCursor          OpaqueToken
	Status               RebuildStatus
}

type RebuildStagedRow struct {
	Row      AuthoritativeRow
	Ordinal  uint64
	StagedAt *time.Time
}

type RebuildPage struct {
	Ordinal           uint64
	Rows              []AuthoritativeRow
	HasToken          bool
	Token             OpaqueToken
	CanonicalResponse []byte
	HasContinuation   bool
	Continuation      OpaqueToken
	HasFinalCursor    bool
	FinalCursor       OpaqueToken
	HasChecksum       bool
	Checksum          Checksum
}

type ClientLocalState struct {
	ClientGeneration             Generation
	CurrentSchema                SchemaRef
	AuthoritativeScopeSetVersion ScopeSetVersion
	ScopeAssignments             []LocalScopeAssignment
	ScopeCheckpoints             []LocalScopeCheckpoint
	Backoff                      *DurableBackoff
	Rows                         []LocalRow
	LocalOnlyRows                []LocalOnlyRow
	Provenance                   []LocalProvenance
	SeedReceipts                 []LocalSeedReceipt
	RebuildAttempts              []LocalRebuildAttempt
	SealedBatches                []LocalSealedBatch
	DurableQueue                 []QueuedMutation
	Outcomes                     []MutationOutcome
	SchemaJournal                []SchemaJournalEntry
	RebuildStaging               []LocalRebuildStage
	ErrorState                   *ClientErrorState
	Lifecycle                    ClientLifecycleState
}

type LocalScopeAssignment struct {
	Scope                ScopeID
	MembershipGeneration Generation
	RetentionGeneration  Generation
	Assigned             bool
	RebuildRequired      bool
}

type LocalScopeCheckpoint struct {
	Scope       ScopeID
	Position    StreamPosition
	HasCursor   bool
	Cursor      OpaqueToken
	HasChecksum bool
	Checksum    Checksum
	Verified    bool
}

type ResumableWorkKind string

const (
	ResumableWorkConnect ResumableWorkKind = "connect"
	ResumableWorkPush    ResumableWorkKind = "push"
	ResumableWorkPull    ResumableWorkKind = "pull"
	ResumableWorkRebuild ResumableWorkKind = "rebuild"
)

type ResumableWorkIdentity struct {
	Kind       ResumableWorkKind
	HasBatch   bool
	Batch      BatchKey
	HasScope   bool
	Scope      ScopeID
	HasRebuild bool
	Rebuild    RebuildID
}

type RetryClassification string

const (
	RetryClassificationTransport   RetryClassification = "transport"
	RetryClassificationRateLimited RetryClassification = "rate_limited"
	RetryClassificationUnavailable RetryClassification = "unavailable"
)

type DurableBackoff struct {
	InterruptedLifecycle ClientLifecycle
	Work                 ResumableWorkIdentity
	Retry                RetryClassification
	Attempt              uint32
	NextEligibleAt       *time.Time
}

type LocalRow struct {
	Identity         RowIdentity
	Fields           []FieldValue
	Deleted          bool
	HasServerVersion bool
	ServerVersion    RowVersion
	HasChecksum      bool
	Checksum         Checksum
	UpdatedAt        *time.Time
}

type LocalOnlyRow struct {
	Key       LocalOnlyRowKey
	Fields    []FieldValue
	UpdatedAt *time.Time
}

type LocalProvenance struct {
	Row     RowIdentity
	Scopes  []ScopeID
	Version RowVersion
}

// LocalSeedReceipt is one durable receipt for a completed portable seed scope.
type LocalSeedReceipt struct {
	Scope                ScopeID
	HasReceipt           bool
	Receipt              OpaqueToken
	ExportID             ExportID
	ExportManifestHash   [32]byte
	Schema               SchemaRef
	RegistryGeneration   Generation
	MembershipGeneration Generation
	RetentionGeneration  Generation
	StreamGeneration     StreamGeneration
	SnapshotBoundary     StreamPosition
	Cardinality          Cardinality
	Checksum             Checksum
}

type LocalRebuildAttemptPhase string

const (
	LocalRebuildAttemptPhaseCreated         LocalRebuildAttemptPhase = "created"
	LocalRebuildAttemptPhaseApplying        LocalRebuildAttemptPhase = "applying"
	LocalRebuildAttemptPhasePendingFinality LocalRebuildAttemptPhase = "pending_finality"
	LocalRebuildAttemptPhaseCompleted       LocalRebuildAttemptPhase = "completed"
	LocalRebuildAttemptPhaseAbandoned       LocalRebuildAttemptPhase = "abandoned"
)

// LocalRebuildAttempt is the durable local state for one immutable rebuild attempt.
type LocalRebuildAttempt struct {
	Rebuild               RebuildID
	Scope                 ScopeID
	ClientGeneration      Generation
	Schema                SchemaRef
	PageLimit             uint32
	HasContinuation       bool
	Continuation          OpaqueToken
	AppliedPages          []AppliedRebuildPage
	HasPendingFinalResult bool
	PendingFinalResult    PendingRebuildFinalResult
	Phase                 LocalRebuildAttemptPhase
}

// AppliedRebuildPage identifies one page that the local rebuild attempt applied.
type AppliedRebuildPage struct {
	RequestPageOrdinal uint64
	HasRequestToken    bool
	RequestToken       OpaqueToken
	HasPageDigest      bool
	PageDigest         Checksum
	AppliedAt          *time.Time
}

// PendingRebuildFinalResult retains the final result until local finality completes.
type PendingRebuildFinalResult struct {
	HasFinalCursor bool
	FinalCursor    OpaqueToken
	ScopeChecksum  Checksum
	Cardinality    Cardinality
}

type LocalMutationStatus string

const (
	LocalMutationStatusPending              LocalMutationStatus = "pending"
	LocalMutationStatusSealed               LocalMutationStatus = "sealed"
	LocalMutationStatusAccepted             LocalMutationStatus = "accepted"
	LocalMutationStatusServerRejected       LocalMutationStatus = "server_rejected"
	LocalMutationStatusSupersededBeforeSend LocalMutationStatus = "superseded_before_send"
	LocalMutationStatusCancelledBeforeSend  LocalMutationStatus = "cancelled_before_send"
	LocalMutationStatusBlockedByPredecessor LocalMutationStatus = "blocked_by_predecessor"
)

type QueuedMutation struct {
	Mutation        MutationID
	Table           TableID
	Row             RowIdentity
	AuthoredSchema  SchemaRef
	Operation       DMLOperation
	HasBaseVersion  bool
	BaseVersion     RowVersion
	ClientVersion   ClientVersion
	AuthoredColumns []FieldValue
	LocalOrder      uint64
	HasPredecessor  bool
	Predecessor     MutationID
	Status          LocalMutationStatus
	Request         []byte
	QueuedAt        *time.Time
}

type LocalSealedBatchState string

const (
	LocalSealedBatchStateSealed              LocalSealedBatchState = "sealed"
	LocalSealedBatchStateSent                LocalSealedBatchState = "sent"
	LocalSealedBatchStateResponseLost        LocalSealedBatchState = "response_lost"
	LocalSealedBatchStateReconciled          LocalSealedBatchState = "reconciled"
	LocalSealedBatchStateAbandonedGeneration LocalSealedBatchState = "abandoned_generation"
)

type LocalSealedBatch struct {
	Batch                BatchID
	ClientGeneration     Generation
	Schema               SchemaRef
	Mutations            []MutationID
	CanonicalRequest     []byte
	Fingerprint          FingerprintRecord
	State                LocalSealedBatchState
	HasCanonicalResponse bool
	CanonicalResponse    []byte
	HTTPStatus           int
	SealedAt             *time.Time
	ReconciledAt         *time.Time
}

type ClientErrorState struct {
	Reason       ReasonCode
	Retryable    bool
	Acknowledged bool
	At           *time.Time
}

type SchemaJournalEntry struct {
	JournalVersion         uint64
	MigrationPlanVersion   uint64
	SourceSchema           SchemaRef
	TargetSchema           SchemaRef
	VerifiedTargetManifest SchemaManifest
	Action                 SchemaAction
	AffectedScopes         []ScopeID
	MigrationPlan          []MigrationPlanOperation
	Phase                  MigrationPhase
	Ordinal                uint64
}

type SchemaAction string

const (
	SchemaActionNone         SchemaAction = "none"
	SchemaActionReplace      SchemaAction = "replace"
	SchemaActionRebuildLocal SchemaAction = "rebuild_local"
	SchemaActionUnsupported  SchemaAction = "unsupported"
)

type MigrationOperationKind string

const (
	MigrationOperationCreateTable          MigrationOperationKind = "create_table"
	MigrationOperationDropTable            MigrationOperationKind = "drop_table"
	MigrationOperationAddField             MigrationOperationKind = "add_field"
	MigrationOperationDropField            MigrationOperationKind = "drop_field"
	MigrationOperationCreateIndex          MigrationOperationKind = "create_index"
	MigrationOperationDropIndex            MigrationOperationKind = "drop_index"
	MigrationOperationUpdateCursor         MigrationOperationKind = "update_cursor"
	MigrationOperationUpdateAssignment     MigrationOperationKind = "update_assignment"
	MigrationOperationUpdateChecksum       MigrationOperationKind = "update_checksum"
	MigrationOperationUpdateProvenance     MigrationOperationKind = "update_provenance"
	MigrationOperationUpdateSchemaMetadata MigrationOperationKind = "update_schema_metadata"
)

type MigrationPlanOperation struct {
	Kind  MigrationOperationKind
	Table TableID
	Field FieldID
	Index IndexID
}

type MigrationPhase string

const (
	MigrationPhasePlanned  MigrationPhase = "planned"
	MigrationPhaseApplying MigrationPhase = "applying"
	MigrationPhaseApplied  MigrationPhase = "applied"
	MigrationPhaseFailed   MigrationPhase = "failed"
)

type LocalRebuildStage struct {
	Rebuild RebuildID
	Ordinal uint64
	Row     LocalRow
}

type ClientLifecycleState struct {
	State     ClientLifecycle
	ChangedAt *time.Time
}

type RetentionFloor struct {
	MembershipGeneration Generation
	RetentionGeneration  Generation
	StreamGeneration     StreamGeneration
	Position             StreamPosition
	ExpiresAt            *time.Time
}

type SeedState struct {
	Exports []SeedExport
	Records []SeedRecord
}

type SeedExport struct {
	ID                 ExportID
	TransactionNonce   TransactionNonce
	Schema             SchemaRef
	RegistryGeneration Generation
	StreamGeneration   StreamGeneration
	SnapshotBoundary   StreamPosition
	ManifestHash       [32]byte
	Status             SeedExportStatus
	CreatedAt          *time.Time
	Scopes             []SeedScopeState
	Pages              []SeedPageState
}

type SeedScopeState struct {
	Scope                ScopeID
	MembershipGeneration Generation
	RetentionGeneration  Generation
	Cardinality          Cardinality
	Checksum             Checksum
	HasReceipt           bool
	Receipt              OpaqueToken
}

type SeedPageState struct {
	Scope          ScopeID
	NextRowOrdinal uint64
	PageLimit      uint32
	HasToken       bool
	Token          OpaqueToken
}

type SeedExportStatus string

const (
	SeedExportStatusBuilding SeedExportStatus = "building"
	SeedExportStatusComplete SeedExportStatus = "complete"
	SeedExportStatusFailed   SeedExportStatus = "failed"
)

type SeedRecord struct {
	Key      SeedRecordKey
	Row      AuthoritativeRow
	Checksum Checksum
}

type AuthorizationState struct {
	Roles         []RoleCapabilities
	WritePolicies []WritePolicyDecision
}

// WritePolicyDecision is one deterministic server write-policy result.
type WritePolicyDecision struct {
	User    UserID
	Table   TableID
	Allowed bool
}

type RoleCapabilities struct {
	Role         RoleID
	Capabilities []Capability
}

type InstallationCapabilities struct {
	Installed                       bool
	SchemaName                      string
	ExtensionVersion                string
	ProtocolVersion                 int
	MinimumClientRuntime            int
	StaleClientIntervalMilliseconds uint64
	Endpoints                       []Endpoint
	Capabilities                    []InstallationCapability
}

type InstallationCapability struct {
	ID        CapabilityID
	Enabled   bool
	CheckedAt *time.Time
}

// ConfiguredLimits contains the closed Protocol 3 release bounds.
type ConfiguredLimits struct {
	MaxScopeFanout         uint64
	MaxImpactRows          uint64
	PullMaximum            uint32
	RebuildMaximum         uint32
	CompactionBatchMaximum uint64
	BackfillBatchMaximum   uint64
}

type ReadinessState struct {
	ConfiguredDatabase DatabaseName
	Workers            []WorkerReadiness
	Slots              []SlotReadiness
	Limits             []ReadinessLimit
	Checks             []ReadinessCheck
	Reasons            []ReasonCode
}

type WorkerReadiness struct {
	ID                   WorkerID
	Database             DatabaseName
	Running              bool
	HeartbeatAt          *time.Time
	RegistryGeneration   Generation
	MaterializedPosition StreamPosition
}

type SlotReadiness struct {
	ID                 SlotID
	Database           DatabaseName
	Plugin             string
	Active             bool
	AcknowledgedEndLSN EndLSN
}

type ReadinessLimit struct {
	ID     LimitID
	Value  uint64
	Finite bool
}

type ReadinessCheckState string

const (
	ReadinessCheckOK      ReadinessCheckState = "ok"
	ReadinessCheckFailed  ReadinessCheckState = "failed"
	ReadinessCheckUnknown ReadinessCheckState = "unknown"
)

type ReadinessCheck struct {
	ID                 CheckID
	State              ReadinessCheckState
	Reason             ReasonCode
	NumericObservation uint64
	CheckedAt          *time.Time
}

type ModelEvent struct {
	Ordinal        uint64
	Kind           ModelEventKind
	At             *time.Time
	HasClient      bool
	Client         ClientKey
	HasTransaction bool
	Transaction    TransactionReplayKey
	Reason         ReasonCode
}
