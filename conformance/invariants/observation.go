package invariants

import (
	"github.com/trainstar/synchro/conformance/scenarios"
	"github.com/trainstar/synchro/conformance/vectors"
)

// Observation joins one ordered capture from the server, operator, clients, and wire.
// No contract-layer type joins these four observation surfaces.
type Observation struct {
	Sequence      uint64
	Manifest      *vectors.Manifest
	ServerState   *scenarios.StateFacts
	Operator      *OperatorObservation
	Clients       []ClientObservation
	WireExchanges []WireExchangeObservation
}

// OperatorObservation contains operational state that is outside scenarios.StateFacts.
// The contract layer has no aggregate for checkpoints, WAL progress, rebuild boundaries, and retention floors.
type OperatorObservation struct {
	Checkpoints []OperatorCheckpointObservation
	WALProgress *WALProgressObservation
	Rebuilds    []OperatorRebuildObservation
	Retention   []RetentionObservation
}

// OperatorCheckpointObservation binds a client scope to its durable server position.
// scenarios.CheckpointFact has no stream generation or server position fields.
type OperatorCheckpointObservation struct {
	UserID           string
	ClientID         string
	ScopeID          string
	StreamGeneration string
	Position         PositionObservation
}

// PositionObservation is one nullable server checkpoint or retention position.
// The contract layer has no type for PostgreSQL checkpoint position components.
type PositionObservation struct {
	Kind          string
	CommitLSN     *string
	EventOrdinal  *uint64
	EffectOrdinal *uint64
}

// WALProgressObservation contains durable WAL progress and the slot flush position.
// The contract layer has no operational replication-slot observation type.
type WALProgressObservation struct {
	AcknowledgedEndLSN    string
	SlotConfirmedFlushLSN string
}

// OperatorRebuildObservation adds operator-only boundaries to a contract rebuild fact.
// scenarios.RebuildFact has no session, schema, generation, boundary, or expiry fields.
type OperatorRebuildObservation struct {
	State                scenarios.RebuildFact
	SessionID            string
	ClientGeneration     uint64
	Schema               scenarios.SchemaFact
	StreamGeneration     string
	MembershipGeneration uint64
	RetentionGeneration  uint64
	Boundary             PositionObservation
	Expired              bool
}

// RetentionObservation contains one scope floor and the positions retained by active rebuilds.
// The contract layer has no retention-floor or rebuild-pin observation type.
type RetentionObservation struct {
	ScopeID         string
	Generation      uint64
	Floor           PositionObservation
	PinnedPositions []PositionObservation
}

// ClientObservation combines contract durability facts with missing native capture signals.
// scenarios.ClientDurabilityFact has no complete rows, raw cursors, local digests, or process identity.
type ClientObservation struct {
	State     scenarios.ClientDurabilityFact
	Rows      []ClientRowObservation
	Scopes    []ClientScopeObservation
	ScopeRows []ClientScopeRowObservation
	Process   *ProcessIdentityObservation
	Complete  bool
}

// ClientRowObservation provides one complete row for independent digest computation.
// scenarios.RowFact stores canonical JSON, but vectors.Row supplies the typed fields required by vectors.RowDigest.
type ClientRowObservation struct {
	TableID       string
	Row           vectors.Row
	ServerVersion string
	StoredDigest  *[32]byte
}

// ClientScopeObservation contains the raw cursor and both client-held scope digests.
// scenarios.CheckpointFact has no raw cursor, local digest, or client scope generation.
type ClientScopeObservation struct {
	ScopeID             string
	RawCursor           *string
	AuthoritativeDigest *[32]byte
	LocalDigest         *[32]byte
	Generation          uint64
}

// ClientScopeRowObservation binds a canonical row digest entry to one client scope.
// The contract layer has no scope-row type with canonical row identity bytes.
type ClientScopeRowObservation struct {
	ScopeID    string
	Entry      vectors.DigestEntry
	Generation uint64
}

// ProcessIdentityObservation identifies one process and its durable database.
// The contract layer has no portable process or database identity type.
type ProcessIdentityObservation struct {
	ProcessID                   string
	DatabaseIdentityFingerprint string
}

// WireExchangeObservation retains one raw request and response without importing net/http.
// The contract layer has no raw wire-exchange type.
type WireExchangeObservation struct {
	Sequence       uint64
	OperationClass string
	RequestBody    []byte
	ResponseStatus int
	ResponseBody   []byte
}
