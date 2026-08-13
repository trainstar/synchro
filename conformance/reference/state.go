package reference

// State contains the reference-model state families.
type State struct {
	ProtocolVersion int
	Schemas         map[SchemaRef]SchemaManifest
	CurrentSchema   SchemaRef
	Registry        RegistryState
	Relations       map[RelationID]RelationState
	Clients         map[ClientKey]ClientState
	// Rows contains current WAL-materialized synchronized rows.
	Rows             map[RowIdentity]AuthoritativeRow
	Scopes           map[ScopeID]ScopeState
	Stream           StreamState
	Fences           map[FenceID]VersionFence
	Projections      map[ProjectionKey]CapturedProjection
	Batches          map[BatchKey]BatchLedger
	Mutations        map[MutationKey]MutationLedger
	Rebuilds         map[RebuildKey]RebuildSession
	ClientLocal      map[ClientKey]ClientLocalState
	RetentionFloors  map[ScopeID]RetentionFloor
	Seed             SeedState
	Authorization    AuthorizationState
	Installation     InstallationCapabilities
	ConfiguredLimits ConfiguredLimits
	Readiness        ReadinessState
	Events           []ModelEvent
}

// SnapshotEntry retains a root-map key beside its normalized value.
type SnapshotEntry[K comparable, V any] struct {
	Key   K
	Value V
}

// StateSnapshot contains a deterministic state observation without maps.
type StateSnapshot struct {
	ProtocolVersion  int
	Schemas          []SnapshotEntry[SchemaRef, SchemaManifest]
	CurrentSchema    SchemaRef
	Registry         RegistryState
	Relations        []SnapshotEntry[RelationID, RelationState]
	Clients          []SnapshotEntry[ClientKey, ClientState]
	Rows             []SnapshotEntry[RowIdentity, AuthoritativeRow]
	Scopes           []SnapshotEntry[ScopeID, ScopeState]
	Stream           StreamState
	Fences           []SnapshotEntry[FenceID, VersionFence]
	Projections      []SnapshotEntry[ProjectionKey, CapturedProjection]
	Batches          []SnapshotEntry[BatchKey, BatchLedger]
	Mutations        []SnapshotEntry[MutationKey, MutationLedger]
	Rebuilds         []SnapshotEntry[RebuildKey, RebuildSession]
	ClientLocal      []SnapshotEntry[ClientKey, ClientLocalState]
	RetentionFloors  []SnapshotEntry[ScopeID, RetentionFloor]
	Seed             SeedState
	Authorization    AuthorizationState
	Installation     InstallationCapabilities
	ConfiguredLimits ConfiguredLimits
	Readiness        ReadinessState
	Events           []ModelEvent
}
