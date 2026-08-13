package reference

import (
	"errors"
	"math"
	"sync"
	"time"
)

// Clock supplies the current time to deterministic reference-model operations.
type Clock interface {
	Now() time.Time
}

// TokenAuthority mints opaque labels and validates their bound context.
type TokenAuthority interface {
	Mint(kind string, bindings BindingSet) OpaqueToken
	Validate(token OpaqueToken, kind string, bindings BindingSet) TokenStatus
}

type tokenAuthority struct {
	mu        sync.RWMutex
	namespace uint64
	next      uint64
	minted    map[OpaqueToken]tokenRecord
}

type tokenRecord struct {
	kind     string
	bindings BindingSet
}

type tokenReservation struct {
	token    OpaqueToken
	kind     string
	bindings BindingSet
}

// newTokenAuthority creates a deterministic authority for one model instance.
func newTokenAuthority(seed int64) TokenAuthority {
	authority, err := newRestoredTokenAuthority(seed, nil)
	if err != nil {
		panic(err)
	}
	return authority
}

func newRestoredTokenAuthority(seed int64, reservations []tokenReservation) (TokenAuthority, error) {
	authority := &tokenAuthority{
		namespace: uint64(seed),
		minted:    make(map[OpaqueToken]tokenRecord, len(reservations)),
	}
	for _, reservation := range reservations {
		if reservation.token == (OpaqueToken{}) {
			return nil, errors.New("token restoration contains a zero handle")
		}
		if !supportedTokenKind(reservation.kind) {
			return nil, errors.New("token restoration contains an unsupported kind")
		}
		record := tokenRecord{
			kind:     reservation.kind,
			bindings: canonicalizeBindings(reservation.bindings),
		}
		if existing, ok := authority.minted[reservation.token]; ok {
			if existing != record {
				return nil, errors.New("token restoration contains conflicting reservations")
			}
			continue
		}
		authority.minted[reservation.token] = record
		if reservation.token.namespace == authority.namespace && reservation.token.sequence > authority.next {
			authority.next = reservation.token.sequence
		}
	}
	return authority, nil
}

func cloneTokenAuthority(source TokenAuthority) (TokenAuthority, error) {
	authority, ok := source.(*tokenAuthority)
	if !ok || authority == nil {
		return nil, errors.New("clone token authority: unsupported authority")
	}

	authority.mu.RLock()
	defer authority.mu.RUnlock()
	cloned := &tokenAuthority{
		namespace: authority.namespace,
		next:      authority.next,
		minted:    make(map[OpaqueToken]tokenRecord, len(authority.minted)),
	}
	for token, record := range authority.minted {
		cloned.minted[token] = record
	}
	return cloned, nil
}

func (a *tokenAuthority) Mint(kind string, bindings BindingSet) OpaqueToken {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !supportedTokenKind(kind) {
		return OpaqueToken{}
	}
	for {
		if a.next == math.MaxUint64 {
			return OpaqueToken{}
		}
		a.next++
		token := OpaqueToken{namespace: a.namespace, sequence: a.next}
		if _, exists := a.minted[token]; exists {
			continue
		}
		a.minted[token] = tokenRecord{kind: kind, bindings: canonicalizeBindings(bindings)}
		return token
	}
}

func (a *tokenAuthority) Validate(token OpaqueToken, kind string, bindings BindingSet) TokenStatus {
	a.mu.RLock()
	record, ok := a.minted[token]
	a.mu.RUnlock()
	if !ok {
		return TokenStatusForged
	}
	if !supportedTokenKind(kind) || record.kind != kind {
		return TokenStatusWrongKind
	}
	bindings = canonicalizeBindings(bindings)
	if record.bindings == bindings {
		return TokenStatusValid
	}
	if requestBindingsDiffer(record.bindings, bindings) {
		return TokenStatusMisbound
	}
	if staleBindingsDiffer(record.bindings, bindings) {
		return TokenStatusStale
	}
	// Keep validation closed if BindingSet gains a field without classification.
	return TokenStatusStale
}

func validateTokenAgainstCurrent(authority TokenAuthority, token OpaqueToken, kind string, current BindingSet, now time.Time) TokenStatus {
	concrete, ok := authority.(*tokenAuthority)
	if !ok || concrete == nil {
		return TokenStatusForged
	}

	concrete.mu.RLock()
	record, found := concrete.minted[token]
	concrete.mu.RUnlock()
	if !found {
		return TokenStatusForged
	}
	if !supportedTokenKind(kind) || record.kind != kind {
		return TokenStatusWrongKind
	}

	current = canonicalizeBindings(current)
	if record.bindings.HasIssuedAt {
		current.HasIssuedAt = true
		current.IssuedAt = record.bindings.IssuedAt
	}
	if record.bindings.HasExpiresAt {
		if !now.Round(0).UTC().Before(record.bindings.ExpiresAt) {
			return TokenStatusStale
		}
		current.HasExpiresAt = true
		current.ExpiresAt = record.bindings.ExpiresAt
	}
	return concrete.Validate(token, kind, current)
}

func requestBindingsDiffer(stored, requested BindingSet) bool {
	return stored.HasUser != requested.HasUser ||
		stored.User != requested.User ||
		stored.HasClient != requested.HasClient ||
		stored.Client != requested.Client ||
		stored.HasScope != requested.HasScope ||
		stored.Scope != requested.Scope ||
		stored.HasSessionID != requested.HasSessionID ||
		stored.SessionID != requested.SessionID ||
		stored.HasRebuildID != requested.HasRebuildID ||
		stored.RebuildID != requested.RebuildID ||
		stored.HasExportID != requested.HasExportID ||
		stored.ExportID != requested.ExportID ||
		stored.HasTransactionNonce != requested.HasTransactionNonce ||
		stored.TransactionNonce != requested.TransactionNonce ||
		stored.HasIssuedAt != requested.HasIssuedAt ||
		stored.IssuedAt != requested.IssuedAt ||
		stored.HasOrdinal != requested.HasOrdinal ||
		stored.Ordinal != requested.Ordinal ||
		stored.HasPageLimit != requested.HasPageLimit ||
		stored.PageLimit != requested.PageLimit
}

func staleBindingsDiffer(stored, requested BindingSet) bool {
	return stored.HasClientGeneration != requested.HasClientGeneration ||
		stored.ClientGeneration != requested.ClientGeneration ||
		stored.HasRegistryGeneration != requested.HasRegistryGeneration ||
		stored.RegistryGeneration != requested.RegistryGeneration ||
		stored.HasMembershipGeneration != requested.HasMembershipGeneration ||
		stored.MembershipGeneration != requested.MembershipGeneration ||
		stored.HasRetentionGeneration != requested.HasRetentionGeneration ||
		stored.RetentionGeneration != requested.RetentionGeneration ||
		stored.HasStreamGeneration != requested.HasStreamGeneration ||
		stored.StreamGeneration != requested.StreamGeneration ||
		stored.HasSchema != requested.HasSchema ||
		stored.Schema != requested.Schema ||
		stored.HasStreamPosition != requested.HasStreamPosition ||
		stored.StreamPosition != requested.StreamPosition ||
		stored.HasSnapshotBoundary != requested.HasSnapshotBoundary ||
		stored.SnapshotBoundary != requested.SnapshotBoundary ||
		stored.HasExportManifestHash != requested.HasExportManifestHash ||
		stored.ExportManifestHash != requested.ExportManifestHash ||
		stored.HasExpiresAt != requested.HasExpiresAt ||
		stored.ExpiresAt != requested.ExpiresAt ||
		stored.HasAcceptedWriteEpoch != requested.HasAcceptedWriteEpoch ||
		stored.AcceptedWriteEpoch != requested.AcceptedWriteEpoch ||
		stored.HasCardinality != requested.HasCardinality ||
		stored.Cardinality != requested.Cardinality ||
		stored.HasChecksum != requested.HasChecksum ||
		stored.Checksum != requested.Checksum
}

func supportedTokenKind(kind string) bool {
	switch kind {
	case string(TokenKindIncrementalCursor), string(TokenKindRebuildContinuation), string(TokenKindSeedPage), string(TokenKindSeedReceipt):
		return true
	default:
		return false
	}
}

func canonicalizeBindings(bindings BindingSet) BindingSet {
	if !bindings.HasUser {
		bindings.User = UserID("")
	}
	if !bindings.HasClient {
		bindings.Client = ClientKey{}
	}
	if !bindings.HasClientGeneration {
		bindings.ClientGeneration = Generation(0)
	}
	if !bindings.HasRegistryGeneration {
		bindings.RegistryGeneration = Generation(0)
	}
	if !bindings.HasMembershipGeneration {
		bindings.MembershipGeneration = Generation(0)
	}
	if !bindings.HasRetentionGeneration {
		bindings.RetentionGeneration = Generation(0)
	}
	if !bindings.HasStreamGeneration {
		bindings.StreamGeneration = StreamGeneration("")
	}
	if !bindings.HasSchema {
		bindings.Schema = SchemaRef{}
	}
	if !bindings.HasScope {
		bindings.Scope = ScopeID("")
	}
	if !bindings.HasStreamPosition {
		bindings.StreamPosition = StreamPosition{}
	}
	if !bindings.HasSnapshotBoundary {
		bindings.SnapshotBoundary = StreamPosition{}
	}
	if !bindings.HasSessionID {
		bindings.SessionID = SessionID("")
	}
	if !bindings.HasRebuildID {
		bindings.RebuildID = RebuildID("")
	}
	if !bindings.HasExportID {
		bindings.ExportID = ExportID("")
	}
	if !bindings.HasTransactionNonce {
		bindings.TransactionNonce = TransactionNonce("")
	}
	if !bindings.HasExportManifestHash {
		bindings.ExportManifestHash = [32]byte{}
	}
	if bindings.HasIssuedAt {
		bindings.IssuedAt = bindings.IssuedAt.Round(0).UTC()
	} else {
		bindings.IssuedAt = time.Time{}
	}
	if bindings.HasExpiresAt {
		bindings.ExpiresAt = bindings.ExpiresAt.Round(0).UTC()
	} else {
		bindings.ExpiresAt = time.Time{}
	}
	if !bindings.HasOrdinal {
		bindings.Ordinal = 0
	}
	if !bindings.HasPageLimit {
		bindings.PageLimit = 0
	}
	if !bindings.HasAcceptedWriteEpoch {
		bindings.AcceptedWriteEpoch = AcceptedWriteEpoch(0)
	}
	if !bindings.HasCardinality {
		bindings.Cardinality = Cardinality(0)
	}
	if !bindings.HasChecksum {
		bindings.Checksum = Checksum{}
	}
	return bindings
}
