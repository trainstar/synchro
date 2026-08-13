package reference

import (
	"math"
	"testing"
	"time"
)

type clockTestDouble struct {
	now time.Time
}

func (c clockTestDouble) Now() time.Time {
	return c.now
}

func TestInjectedClockReturnsControlledTime(t *testing.T) {
	want := time.Date(2031, time.March, 14, 15, 9, 26, 535897932, time.FixedZone("test", -7*60*60))
	var clock Clock = clockTestDouble{now: want}

	if got := clock.Now(); !got.Equal(want) {
		t.Fatalf("clock returned %v, want %v", got, want)
	}
}

func TestTokenAuthorityDeterminismAndDistinctLabels(t *testing.T) {
	bindings := BindingSet{HasUser: true, User: "user-1", HasOrdinal: true, Ordinal: 4}
	first := newTokenAuthority(41)
	second := newTokenAuthority(41)

	firstOne := first.Mint(string(TokenKindIncrementalCursor), bindings)
	firstTwo := first.Mint(string(TokenKindSeedPage), bindings)
	secondOne := second.Mint(string(TokenKindIncrementalCursor), bindings)
	secondTwo := second.Mint(string(TokenKindSeedPage), bindings)

	if firstOne != secondOne || firstTwo != secondTwo {
		t.Fatal("same seed and mint sequence did not produce the same opaque labels")
	}
	if firstOne == firstTwo {
		t.Fatal("successive mints returned the same opaque label")
	}
}

func TestTokenAuthorityClonePreservesStateWithoutMapAliasing(t *testing.T) {
	sourceBindings := BindingSet{HasUser: true, User: "source-user"}
	cloneBindings := BindingSet{HasUser: true, User: "clone-user"}
	source := newTokenAuthority(56)
	first := source.Mint(string(TokenKindIncrementalCursor), sourceBindings)

	cloned, err := cloneTokenAuthority(source)
	if err != nil {
		t.Fatalf("clone failed: %v", err)
	}
	if got := cloned.Validate(first, string(TokenKindIncrementalCursor), sourceBindings); got != TokenStatusValid {
		t.Fatalf("cloned record status = %v, want %v", got, TokenStatusValid)
	}

	cloneOnly := cloned.Mint(string(TokenKindSeedPage), cloneBindings)
	if got := source.Validate(cloneOnly, string(TokenKindSeedPage), cloneBindings); got != TokenStatusForged {
		t.Fatalf("clone mint changed source map: status = %v, want %v", got, TokenStatusForged)
	}
	sourceOnly := source.Mint(string(TokenKindSeedPage), sourceBindings)
	if sourceOnly != cloneOnly {
		t.Fatal("clone did not preserve the next token sequence")
	}
	if got := cloned.Validate(sourceOnly, string(TokenKindSeedPage), sourceBindings); got != TokenStatusMisbound {
		t.Fatalf("source mint changed clone map: status = %v, want %v", got, TokenStatusMisbound)
	}
}

func TestTokenAuthorityValidationStatuses(t *testing.T) {
	bindings := BindingSet{
		HasUser:                 true,
		User:                    "user-1",
		HasClient:               true,
		Client:                  ClientKey{UserID: "user-1", ClientID: "client-1"},
		HasClientGeneration:     true,
		ClientGeneration:        2,
		HasRegistryGeneration:   true,
		RegistryGeneration:      3,
		HasMembershipGeneration: true,
		MembershipGeneration:    4,
		HasRetentionGeneration:  true,
		RetentionGeneration:     5,
		HasStreamGeneration:     true,
		StreamGeneration:        "stream-1",
		HasSchema:               true,
		Schema:                  SchemaRef{Version: 6},
		HasScope:                true,
		Scope:                   "scope-1",
		HasStreamPosition:       true,
		StreamPosition:          StreamPosition{CommitLSN: 7},
		HasSnapshotBoundary:     true,
		SnapshotBoundary:        StreamPosition{CommitLSN: 8},
		HasSessionID:            true,
		SessionID:               "session-1",
		HasRebuildID:            true,
		RebuildID:               "rebuild-1",
		HasExportID:             true,
		ExportID:                "export-1",
		HasIssuedAt:             true,
		IssuedAt:                time.Date(2031, time.March, 14, 15, 9, 26, 0, time.UTC),
		HasExpiresAt:            true,
		ExpiresAt:               time.Date(2031, time.March, 15, 15, 9, 26, 0, time.UTC),
		HasOrdinal:              true,
		Ordinal:                 9,
		HasPageLimit:            true,
		PageLimit:               10,
		HasAcceptedWriteEpoch:   true,
		AcceptedWriteEpoch:      11,
		HasCardinality:          true,
		Cardinality:             12,
		HasChecksum:             true,
		Checksum:                [32]byte{0x01},
	}
	authority := newTokenAuthority(42)
	token := authority.Mint(string(TokenKindIncrementalCursor), bindings)

	if got := authority.Validate(token, string(TokenKindIncrementalCursor), bindings); got != TokenStatusValid {
		t.Fatalf("valid token status = %v, want %v", got, TokenStatusValid)
	}
	if got := authority.Validate(token, string(TokenKindRebuildContinuation), bindings); got != TokenStatusWrongKind {
		t.Fatalf("wrong-kind token status = %v, want %v", got, TokenStatusWrongKind)
	}
	fabricated := newTokenAuthority(99).Mint(string(TokenKindIncrementalCursor), bindings)
	if got := authority.Validate(fabricated, string(TokenKindIncrementalCursor), bindings); got != TokenStatusForged {
		t.Fatalf("fabricated token status = %v, want %v", got, TokenStatusForged)
	}

	misbound := bindings
	misbound.User = "other-user"
	if got := authority.Validate(token, string(TokenKindIncrementalCursor), misbound); got != TokenStatusMisbound {
		t.Fatalf("misbound token status = %v, want %v", got, TokenStatusMisbound)
	}

	stale := bindings
	stale.Schema.Version++
	if got := authority.Validate(token, string(TokenKindIncrementalCursor), stale); got != TokenStatusStale {
		t.Fatalf("stale token status = %v, want %v", got, TokenStatusStale)
	}
}

func TestTokenAuthorityFailsClosedOnBindingPresenceMismatch(t *testing.T) {
	requestBindings := BindingSet{HasUser: true, User: "user-1"}
	requestAuthority := newTokenAuthority(43)
	requestToken := requestAuthority.Mint(string(TokenKindIncrementalCursor), requestBindings)
	requestMismatch := requestBindings
	requestMismatch.HasUser = false
	if got := requestAuthority.Validate(requestToken, string(TokenKindIncrementalCursor), requestMismatch); got != TokenStatusMisbound {
		t.Fatalf("request-context presence mismatch = %v, want %v", got, TokenStatusMisbound)
	}

	stateBindings := BindingSet{HasStreamGeneration: true, StreamGeneration: "stream-1"}
	stateAuthority := newTokenAuthority(44)
	stateToken := stateAuthority.Mint(string(TokenKindIncrementalCursor), stateBindings)
	stateMismatch := stateBindings
	stateMismatch.HasStreamGeneration = false
	if got := stateAuthority.Validate(stateToken, string(TokenKindIncrementalCursor), stateMismatch); got != TokenStatusStale {
		t.Fatalf("stale-state presence mismatch = %v, want %v", got, TokenStatusStale)
	}
}

func TestTokenAuthorityCanonicalizesAbsentBindingValues(t *testing.T) {
	bindings := BindingSet{
		User:                 "hidden-user",
		Client:               ClientKey{UserID: "hidden-user", ClientID: "hidden-client"},
		ClientGeneration:     1,
		RegistryGeneration:   2,
		MembershipGeneration: 3,
		RetentionGeneration:  4,
		StreamGeneration:     "hidden-stream",
		Schema:               SchemaRef{Version: 5, Hash: [32]byte{0x01}},
		Scope:                "hidden-scope",
		StreamPosition:       StreamPosition{CommitLSN: 6},
		SnapshotBoundary:     StreamPosition{CommitLSN: 7},
		SessionID:            "hidden-session",
		RebuildID:            "hidden-rebuild",
		ExportID:             "hidden-export",
		TransactionNonce:     "hidden-nonce",
		ExportManifestHash:   [32]byte{0x02},
		IssuedAt:             time.Date(2031, time.March, 14, 15, 9, 26, 0, time.UTC),
		ExpiresAt:            time.Date(2031, time.March, 15, 15, 9, 26, 0, time.UTC),
		Ordinal:              8,
		PageLimit:            9,
		AcceptedWriteEpoch:   10,
		Cardinality:          11,
		Checksum:             [32]byte{0x03},
	}
	authority := newTokenAuthority(45)
	token := authority.Mint(string(TokenKindIncrementalCursor), bindings)

	if got := authority.Validate(token, string(TokenKindIncrementalCursor), BindingSet{}); got != TokenStatusValid {
		t.Fatalf("absent hidden bindings status = %v, want %v", got, TokenStatusValid)
	}
}

func TestTokenAuthorityCanonicalizesAndClassifiesBindingTimes(t *testing.T) {
	issued := time.Date(2031, time.March, 14, 15, 9, 26, 123, time.FixedZone("issued", 3600))
	expires := issued.Add(24 * time.Hour)
	bindings := BindingSet{
		HasIssuedAt:  true,
		IssuedAt:     issued,
		HasExpiresAt: true,
		ExpiresAt:    expires,
	}
	authority := newTokenAuthority(49)
	token := authority.Mint(string(TokenKindRebuildContinuation), bindings)

	equivalent := bindings
	equivalent.IssuedAt = issued.UTC()
	equivalent.ExpiresAt = expires.UTC()
	if got := authority.Validate(token, string(TokenKindRebuildContinuation), equivalent); got != TokenStatusValid {
		t.Fatalf("equivalent binding times status = %v, want %v", got, TokenStatusValid)
	}

	changedIssue := equivalent
	changedIssue.IssuedAt = changedIssue.IssuedAt.Add(time.Second)
	if got := authority.Validate(token, string(TokenKindRebuildContinuation), changedIssue); got != TokenStatusMisbound {
		t.Fatalf("issue-time mismatch = %v, want %v", got, TokenStatusMisbound)
	}

	changedExpiry := equivalent
	changedExpiry.ExpiresAt = changedExpiry.ExpiresAt.Add(time.Second)
	if got := authority.Validate(token, string(TokenKindRebuildContinuation), changedExpiry); got != TokenStatusStale {
		t.Fatalf("expiry mismatch = %v, want %v", got, TokenStatusStale)
	}
}

func TestValidateTokenAgainstCurrentUsesProtectedTimes(t *testing.T) {
	issued := time.Date(2031, time.March, 14, 15, 9, 26, 0, time.UTC)
	expires := issued.Add(24 * time.Hour)
	bindings := BindingSet{
		HasUser:      true,
		User:         "user-1",
		HasIssuedAt:  true,
		IssuedAt:     issued,
		HasExpiresAt: true,
		ExpiresAt:    expires,
	}
	authority := newTokenAuthority(50)
	token := authority.Mint(string(TokenKindRebuildContinuation), bindings)
	current := BindingSet{HasUser: true, User: "user-1"}

	if got := validateTokenAgainstCurrent(authority, token, string(TokenKindRebuildContinuation), current, issued.Add(time.Hour)); got != TokenStatusValid {
		t.Fatalf("current token status = %v, want %v", got, TokenStatusValid)
	}
	if got := validateTokenAgainstCurrent(authority, token, string(TokenKindRebuildContinuation), current, expires); got != TokenStatusStale {
		t.Fatalf("expired token status = %v, want %v", got, TokenStatusStale)
	}
	misbound := current
	misbound.User = "user-2"
	if got := validateTokenAgainstCurrent(authority, token, string(TokenKindRebuildContinuation), misbound, issued.Add(time.Hour)); got != TokenStatusMisbound {
		t.Fatalf("misbound current token status = %v, want %v", got, TokenStatusMisbound)
	}
}

func TestTokenAuthorityRejectsUnsupportedMintKinds(t *testing.T) {
	authority := newTokenAuthority(46)
	unsupported := authority.Mint("unsupported", BindingSet{})
	if unsupported != (OpaqueToken{}) {
		t.Fatal("unsupported mint returned a non-zero token")
	}
	if got := authority.Validate(unsupported, string(TokenKindIncrementalCursor), BindingSet{}); got != TokenStatusForged {
		t.Fatalf("unsupported mint status = %v, want %v", got, TokenStatusForged)
	}
}

func TestTokenAuthorityRejectsUnsupportedRequestedKinds(t *testing.T) {
	authority := newTokenAuthority(47)
	token := authority.Mint(string(TokenKindIncrementalCursor), BindingSet{})
	if got := authority.Validate(token, "unsupported", BindingSet{}); got != TokenStatusWrongKind {
		t.Fatalf("unsupported requested kind status = %v, want %v", got, TokenStatusWrongKind)
	}
}

func TestTokenAuthorityClassifiesSeedBindings(t *testing.T) {
	authority := newTokenAuthority(48)
	requestBindings := BindingSet{HasTransactionNonce: true, TransactionNonce: "nonce-1"}
	requestToken := authority.Mint(string(TokenKindSeedPage), requestBindings)
	changedNonce := requestBindings
	changedNonce.TransactionNonce = "nonce-2"
	if got := authority.Validate(requestToken, string(TokenKindSeedPage), changedNonce); got != TokenStatusMisbound {
		t.Fatalf("transaction nonce mismatch = %v, want %v", got, TokenStatusMisbound)
	}

	stateBindings := BindingSet{HasExportManifestHash: true, ExportManifestHash: [32]byte{0x01}}
	stateToken := authority.Mint(string(TokenKindSeedReceipt), stateBindings)
	changedManifest := stateBindings
	changedManifest.ExportManifestHash = [32]byte{0x02}
	if got := authority.Validate(stateToken, string(TokenKindSeedReceipt), changedManifest); got != TokenStatusStale {
		t.Fatalf("export manifest hash mismatch = %v, want %v", got, TokenStatusStale)
	}
}

func TestRestoredTokenAuthorityValidatesAndMintsDeterministically(t *testing.T) {
	seed := int64(52)
	bindings := BindingSet{HasExportID: true, ExportID: "export-1"}
	current := OpaqueToken{namespace: uint64(seed), sequence: 2}
	olderNamespace := OpaqueToken{namespace: uint64(seed + 1), sequence: 1}
	reservations := []tokenReservation{
		{token: current, kind: string(TokenKindSeedPage), bindings: bindings},
		{token: olderNamespace, kind: string(TokenKindSeedReceipt), bindings: bindings},
	}

	authority, err := newRestoredTokenAuthority(seed, reservations)
	if err != nil {
		t.Fatalf("restore failed: %v", err)
	}
	if got := authority.Validate(current, string(TokenKindSeedPage), bindings); got != TokenStatusValid {
		t.Fatalf("restored current label status = %v, want %v", got, TokenStatusValid)
	}
	if got := authority.Validate(olderNamespace, string(TokenKindSeedReceipt), bindings); got != TokenStatusValid {
		t.Fatalf("restored older label status = %v, want %v", got, TokenStatusValid)
	}

	minted := authority.Mint(string(TokenKindIncrementalCursor), bindings)
	fresh := newTokenAuthority(seed)
	fresh.Mint(string(TokenKindIncrementalCursor), bindings)
	fresh.Mint(string(TokenKindIncrementalCursor), bindings)
	want := fresh.Mint(string(TokenKindIncrementalCursor), bindings)
	if minted == current || minted == olderNamespace || minted != want {
		t.Fatal("mint after restoration collided or was not deterministic")
	}

	restoredAgain, err := newRestoredTokenAuthority(seed, reservations)
	if err != nil {
		t.Fatalf("second restore failed: %v", err)
	}
	if got := restoredAgain.Mint(string(TokenKindIncrementalCursor), bindings); got != minted {
		t.Fatal("equivalent restorations produced different labels")
	}
}

func TestRestoredTokenAuthorityAcceptsIdenticalDuplicates(t *testing.T) {
	reservation := tokenReservation{
		token:    OpaqueToken{namespace: 53, sequence: 1},
		kind:     string(TokenKindSeedReceipt),
		bindings: BindingSet{HasExportID: true, ExportID: "export-1"},
	}
	if _, err := newRestoredTokenAuthority(53, []tokenReservation{reservation, reservation}); err != nil {
		t.Fatalf("identical duplicate reservations failed: %v", err)
	}
}

func TestRestoredTokenAuthorityRejectsInvalidReservations(t *testing.T) {
	valid := tokenReservation{
		token:    OpaqueToken{namespace: 54, sequence: 1},
		kind:     string(TokenKindSeedPage),
		bindings: BindingSet{},
	}
	tests := []struct {
		name         string
		reservations []tokenReservation
	}{
		{
			name:         "conflicting duplicate",
			reservations: []tokenReservation{valid, {token: valid.token, kind: valid.kind, bindings: BindingSet{HasOrdinal: true, Ordinal: 1}}},
		},
		{
			name:         "zero handle",
			reservations: []tokenReservation{{token: OpaqueToken{}, kind: valid.kind}},
		},
		{
			name:         "unsupported kind",
			reservations: []tokenReservation{{token: valid.token, kind: "unsupported"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := newRestoredTokenAuthority(54, test.reservations); err == nil {
				t.Fatal("invalid reservations were accepted")
			}
		})
	}
}

func TestTokenAuthorityFailsClosedWhenCurrentNamespaceIsExhausted(t *testing.T) {
	seed := int64(55)
	last := OpaqueToken{namespace: uint64(seed), sequence: math.MaxUint64}
	authority, err := newRestoredTokenAuthority(seed, []tokenReservation{{
		token:    last,
		kind:     string(TokenKindIncrementalCursor),
		bindings: BindingSet{},
	}})
	if err != nil {
		t.Fatalf("restore failed: %v", err)
	}
	if got := authority.Mint(string(TokenKindIncrementalCursor), BindingSet{}); got != (OpaqueToken{}) {
		t.Fatal("exhausted namespace returned a minted token")
	}
	if got := authority.Validate(last, string(TokenKindIncrementalCursor), BindingSet{}); got != TokenStatusValid {
		t.Fatal("exhausted namespace overwrote its last restored record")
	}
}
