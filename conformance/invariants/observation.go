package invariants

import (
	"encoding/json"

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
	// A driver must put one position relation in CursorPositions for each captured non-null raw client cursor.
	CursorPositions []CursorPositionObservation
	// A driver must put each pull exchange that CheckCursorMonotonicity must validate in PullResults.
	PullResults []PullResultObservation
	// A driver must put each request that acknowledges a PullResults cursor in CursorAcknowledgements.
	CursorAcknowledgements []CursorAcknowledgementObservation
	// A driver must map each distinct RowScopeEdges row to its normalized identity in ServerRowIdentities.
	ServerRowIdentities []ServerRowIdentityObservation
}

// OperatorObservation contains operational state that is outside scenarios.StateFacts.
// The contract layer has no aggregate for client checkpoint positions.
type OperatorObservation struct {
	Checkpoints []OperatorCheckpointObservation
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

// PositionObservation is one nullable server checkpoint position.
// The contract layer has no type for PostgreSQL checkpoint position components.
type PositionObservation struct {
	Kind          string
	CommitLSN     *string
	EventOrdinal  *uint64
	EffectOrdinal *uint64
}

// ClientObservation combines contract durability facts with missing native capture signals.
// scenarios.ClientDurabilityFact has no complete rows, raw cursors, local digests, or process identity.
type ClientObservation struct {
	State     scenarios.ClientDurabilityFact
	Rows      []ClientRowObservation
	Scopes    []ClientScopeObservation
	ScopeRows []ClientScopeRowObservation
	Process   *ProcessIdentityObservation
	// A driver must set RestartBoundary after a controlled kill and relaunch since this client's previous capture.
	RestartBoundary bool
	// Complete asserts that Rows, Scopes, and ScopeRows contain the complete client state after this observation's operations.
	Complete bool
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

// CursorPositionObservation binds one raw cursor to its client, scope generation, and server checkpoint position.
// A driver must emit the exact decoded server position represented by RawCursor.
type CursorPositionObservation struct {
	UserID           string
	ClientID         string
	ScopeID          string
	Generation       uint64
	RawCursor        string
	StreamGeneration string
	Position         PositionObservation
}

// PullChangeIdentityObservation identifies one expected change in a checker-owned pull result.
// A driver must preserve the wire order and the exact primary-key JSON value.
type PullChangeIdentityObservation struct {
	ScopeID           string
	TableID           string
	PrimaryKeyFieldID string
	PrimaryKey        json.RawMessage
}

// PullResultObservation binds one checker-owned pull exchange to its expected changes and issued cursor positions.
// A driver must emit this fact only for the successful terminal pull control defined by the source validator.
type PullResultObservation struct {
	ExchangeSequence uint64
	UserID           string
	ClientID         string
	Changes          []PullChangeIdentityObservation
	Cursors          []CursorPositionObservation
}

// CursorAcknowledgementObservation binds one pull request to the issued cursor position that it acknowledges.
// A driver must emit the fact after the server durably records the matching checkpoint.
type CursorAcknowledgementObservation struct {
	ExchangeSequence uint64
	Cursor           CursorPositionObservation
}

// ServerRowIdentityObservation maps one server row projection to its normalized vectors.RowIdentity bytes.
// A driver must emit one relation for each distinct row in StateFacts.RowScopeEdges.
type ServerRowIdentityObservation struct {
	TableID           string
	CanonicalWireJSON string
	RowIdentity       []byte
}

// WireExchangeObservation retains one raw request and response without importing net/http.
// The contract layer has no raw wire-exchange type.
type WireExchangeObservation struct {
	Sequence       uint64
	OperationClass string
	RequestBody    []byte
	ResponseStatus int
	ResponseBody   []byte
	// A driver must set ExpectMutationConservation for a successful push control that the mutation checker must validate.
	ExpectMutationConservation bool
	// A driver must set ExpectChecksumConvergence for a successful pull control that the checksum checker must validate.
	// The observation must include one matching complete post-pull client capture.
	ExpectChecksumConvergence bool
	// A driver must set ExpectScopeIsolation for a successful zero-change pull control that the scope checker must validate.
	ExpectScopeIsolation bool
}
