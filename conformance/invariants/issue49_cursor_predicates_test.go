package invariants

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"strconv"
	"strings"
	"time"
)

type issue49OpaqueCursorCase struct {
	Name                   string
	Null                   bool
	ServerIssued           string
	Persisted              string
	Presented              string
	Parsed                 bool
	Ordered                bool
	MeaningAssigned        bool
	AuthenticatedBeforeUse bool
	Usable                 bool
	RebuildRequired        bool
}

func issue49OpaqueCursorsValid(cases []issue49OpaqueCursorCase) bool {
	if len(cases) != 2 {
		return false
	}
	seen := make(map[string]struct{}, len(cases))
	for _, cursor := range cases {
		if _, duplicate := seen[cursor.Name]; duplicate {
			return false
		}
		seen[cursor.Name] = struct{}{}
		if cursor.Parsed || cursor.Ordered || cursor.MeaningAssigned {
			return false
		}
		if cursor.Null {
			if cursor.ServerIssued != "" || cursor.Persisted != "" || cursor.Presented != "" || cursor.AuthenticatedBeforeUse || cursor.Usable || !cursor.RebuildRequired {
				return false
			}
			continue
		}
		if cursor.ServerIssued == "" || cursor.Persisted != cursor.ServerIssued || cursor.Presented != cursor.ServerIssued || !cursor.AuthenticatedBeforeUse || !cursor.Usable || cursor.RebuildRequired {
			return false
		}
	}
	return hasExactKeys(seen, "non-null", "null")
}

type issue49ScopeSetState struct {
	Assigned []string
	Version  uint64
}

func issue49ScopeSetVersionsValid(states []issue49ScopeSetState) bool {
	if len(states) < 2 {
		return false
	}
	for index, state := range states {
		if state.Version == 0 || !uniqueStrings(state.Assigned) {
			return false
		}
		if index == 0 {
			continue
		}
		previous := states[index-1]
		changed := !sameStringSet(previous.Assigned, state.Assigned)
		if changed && state.Version <= previous.Version {
			return false
		}
		if !changed && state.Version != previous.Version {
			return false
		}
	}
	return true
}

type issue49ScopeLocalRebuild struct {
	RebuiltScope    string
	BeforeScopes    map[string][]string
	AfterScopes     map[string][]string
	BeforeLocalOnly []string
	AfterLocalOnly  []string
}

func issue49ScopeLocalRebuildValid(observation issue49ScopeLocalRebuild) bool {
	if observation.RebuiltScope == "" || len(observation.BeforeScopes) < 2 || len(observation.AfterScopes) != len(observation.BeforeScopes) || !sameStringSet(observation.BeforeLocalOnly, observation.AfterLocalOnly) {
		return false
	}
	if _, exists := observation.BeforeScopes[observation.RebuiltScope]; !exists {
		return false
	}
	if _, exists := observation.AfterScopes[observation.RebuiltScope]; !exists {
		return false
	}
	for scope, before := range observation.BeforeScopes {
		after, exists := observation.AfterScopes[scope]
		if !exists || !uniqueStrings(before) || !uniqueStrings(after) {
			return false
		}
		if scope != observation.RebuiltScope && !sameStringSet(before, after) {
			return false
		}
	}
	return uniqueStrings(observation.BeforeLocalOnly) && uniqueStrings(observation.AfterLocalOnly)
}

type issue49AppliedOutcome struct {
	ID        string
	Validated bool
	Durable   bool
}

type issue49ScopeCursorApply struct {
	Scope          string
	BeforeCursor   string
	ReturnedCursor string
	AfterCursor    string
	Outcomes       []issue49AppliedOutcome
}

func issue49PerScopeCursorApplyValid(scopes []issue49ScopeCursorApply) bool {
	if len(scopes) < 2 {
		return false
	}
	seenScopes := make(map[string]struct{}, len(scopes))
	for _, scope := range scopes {
		if scope.Scope == "" || scope.BeforeCursor == "" || scope.ReturnedCursor == "" || scope.ReturnedCursor == scope.BeforeCursor || len(scope.Outcomes) == 0 {
			return false
		}
		if _, duplicate := seenScopes[scope.Scope]; duplicate {
			return false
		}
		seenScopes[scope.Scope] = struct{}{}
		complete := true
		seenOutcomes := make(map[string]struct{}, len(scope.Outcomes))
		for _, outcome := range scope.Outcomes {
			if outcome.ID == "" {
				return false
			}
			if _, duplicate := seenOutcomes[outcome.ID]; duplicate {
				return false
			}
			seenOutcomes[outcome.ID] = struct{}{}
			complete = complete && outcome.Validated && outcome.Durable
		}
		if complete && scope.AfterCursor != scope.ReturnedCursor {
			return false
		}
		if !complete && scope.AfterCursor != scope.BeforeCursor {
			return false
		}
	}
	return true
}

type issue49ScopeHealth struct {
	Name                        string
	CursorValid                 bool
	AuthoritativeDigestPresent  bool
	AuthoritativeDigestVerified bool
	LocalDigestMatches          bool
	RebuildRequired             bool
	Healthy                     bool
}

func issue49ProgressIntegritySeparationValid(scopes []issue49ScopeHealth) bool {
	if len(scopes) != 3 {
		return false
	}
	seen := make(map[string]struct{}, len(scopes))
	for _, scope := range scopes {
		if scope.Name == "" {
			return false
		}
		if _, duplicate := seen[scope.Name]; duplicate {
			return false
		}
		seen[scope.Name] = struct{}{}
		wantHealthy := scope.CursorValid && scope.AuthoritativeDigestPresent && scope.AuthoritativeDigestVerified && scope.LocalDigestMatches && !scope.RebuildRequired
		if scope.Healthy != wantHealthy {
			return false
		}
	}
	return hasExactKeys(seen, "cursor-only", "verified", "rebuild")
}

type issue49RebuildProvenance struct {
	RebuiltScope string
	Before       map[string][]string
	StagedRows   []string
	After        map[string][]string
	Materialized map[string]bool
}

func issue49RebuildProvenanceValid(observation issue49RebuildProvenance) bool {
	if observation.RebuiltScope == "" || len(observation.Before) == 0 || !uniqueNonemptyStrings(observation.StagedRows) {
		return false
	}
	rows := make(map[string]struct{}, len(observation.Before)+len(observation.StagedRows))
	for row, edges := range observation.Before {
		if row == "" || !uniqueNonemptyStrings(edges) {
			return false
		}
		rows[row] = struct{}{}
	}
	for _, row := range observation.StagedRows {
		rows[row] = struct{}{}
	}
	if len(observation.After) != len(rows) || len(observation.Materialized) != len(rows) {
		return false
	}
	staged := make(map[string]struct{}, len(observation.StagedRows))
	for _, row := range observation.StagedRows {
		staged[row] = struct{}{}
	}
	for row := range rows {
		expected := make([]string, 0, len(observation.Before[row])+1)
		for _, scope := range observation.Before[row] {
			if scope != observation.RebuiltScope {
				expected = append(expected, scope)
			}
		}
		if _, exists := staged[row]; exists {
			expected = append(expected, observation.RebuiltScope)
		}
		after, exists := observation.After[row]
		if !exists || !uniqueStrings(after) || !sameStringSet(after, expected) {
			return false
		}
		materialized, exists := observation.Materialized[row]
		if !exists || materialized != (len(expected) > 0) {
			return false
		}
	}
	return true
}

type issue49TypedPullKey struct {
	Type  string
	Value string
}

type issue49PullCandidate struct {
	ID             string
	ScopeID        string
	LogicalTableID string
	PrimaryKey     issue49TypedPullKey
	Position       issue49EffectPosition
}

type issue49TypedDeduplication struct {
	DeclaredPKTypes map[string]string
	Eligible        []issue49PullCandidate
	Retained        []issue49PullCandidate
}

func issue49TypedDeduplicationValid(observation issue49TypedDeduplication) bool {
	expected, ok := issue49DeduplicateTypedCandidates(observation.DeclaredPKTypes, observation.Eligible)
	if !ok || len(expected) != len(observation.Retained) {
		return false
	}
	for index := range expected {
		if expected[index] != observation.Retained[index] {
			return false
		}
	}
	return true
}

func issue49DeduplicateTypedCandidates(declared map[string]string, candidates []issue49PullCandidate) ([]issue49PullCandidate, bool) {
	type key struct {
		scopeID        string
		logicalTableID string
		primaryType    string
		primaryValue   string
	}
	retained := make(map[key]issue49PullCandidate, len(candidates))
	seenIDs := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		if candidate.ID == "" || candidate.ScopeID == "" || candidate.LogicalTableID == "" || candidate.PrimaryKey.Type != declared[candidate.LogicalTableID] || !issue49PullKeyCanonical(candidate.PrimaryKey) {
			return nil, false
		}
		if _, duplicate := seenIDs[candidate.ID]; duplicate {
			return nil, false
		}
		seenIDs[candidate.ID] = struct{}{}
		identity := key{candidate.ScopeID, candidate.LogicalTableID, candidate.PrimaryKey.Type, candidate.PrimaryKey.Value}
		current, exists := retained[identity]
		if !exists || issue49CompareEffectPosition(candidate.Position, current.Position) > 0 {
			retained[identity] = candidate
		}
	}
	result := make([]issue49PullCandidate, 0, len(retained))
	for _, candidate := range retained {
		result = append(result, candidate)
	}
	sort.Slice(result, func(left, right int) bool {
		compared := issue49CompareEffectPosition(result[left].Position, result[right].Position)
		if compared != 0 {
			return compared < 0
		}
		if result[left].ScopeID != result[right].ScopeID {
			return result[left].ScopeID < result[right].ScopeID
		}
		if result[left].LogicalTableID != result[right].LogicalTableID {
			return result[left].LogicalTableID < result[right].LogicalTableID
		}
		if result[left].PrimaryKey.Type != result[right].PrimaryKey.Type {
			return result[left].PrimaryKey.Type < result[right].PrimaryKey.Type
		}
		return result[left].PrimaryKey.Value < result[right].PrimaryKey.Value
	})
	return result, true
}

func issue49TextOnlyDeduplication(candidates []issue49PullCandidate) []issue49PullCandidate {
	retained := make(map[string]issue49PullCandidate, len(candidates))
	for _, candidate := range candidates {
		current, exists := retained[candidate.PrimaryKey.Value]
		if !exists || issue49CompareEffectPosition(candidate.Position, current.Position) > 0 {
			retained[candidate.PrimaryKey.Value] = candidate
		}
	}
	result := make([]issue49PullCandidate, 0, len(retained))
	for _, candidate := range retained {
		result = append(result, candidate)
	}
	sort.Slice(result, func(left, right int) bool {
		return issue49CompareEffectPosition(result[left].Position, result[right].Position) < 0
	})
	return result
}

func issue49PullKeyCanonical(key issue49TypedPullKey) bool {
	switch key.Type {
	case "string":
		return true
	case "int":
		value, err := strconv.ParseInt(key.Value, 10, 32)
		return err == nil && strconv.FormatInt(value, 10) == key.Value
	case "int64":
		value, err := strconv.ParseInt(key.Value, 10, 64)
		return err == nil && strconv.FormatInt(value, 10) == key.Value
	default:
		return false
	}
}

func issue49CompareEffectPosition(left, right issue49EffectPosition) int {
	if left.CommitLSN != right.CommitLSN {
		if left.CommitLSN < right.CommitLSN {
			return -1
		}
		return 1
	}
	if left.EventOrdinal != right.EventOrdinal {
		if left.EventOrdinal < right.EventOrdinal {
			return -1
		}
		return 1
	}
	if left.EffectOrdinal < right.EffectOrdinal {
		return -1
	}
	if left.EffectOrdinal > right.EffectOrdinal {
		return 1
	}
	return 0
}

type issue49IncrementalCursor struct {
	Kind                 string
	TokenVersion         uint64
	KeyID                string
	StreamGeneration     uint64
	PositionKind         string
	UserBinding          string
	ClientBinding        string
	ClientGeneration     uint64
	ScopeID              string
	SchemaHash           string
	MembershipGeneration uint64
	RetentionGeneration  uint64
	CommitLSN            uint64
	EventOrdinal         uint64
	EffectOrdinal        uint64
	IssuedAt             string
	MAC                  []byte
}

type issue49IncrementalCursorContext struct {
	UserBinding             string
	ClientBinding           string
	ClientGeneration        uint64
	ScopeID                 string
	SchemaHash              string
	MembershipGeneration    uint64
	RetentionGeneration     uint64
	StreamGeneration        uint64
	RetentionFloorCommitLSN uint64
	Keys                    map[string][]byte
}

type issue49CursorBindingObservation struct {
	Context  issue49IncrementalCursorContext
	Valid    issue49IncrementalCursor
	Stale    issue49IncrementalCursor
	Misbound issue49IncrementalCursor
	Tampered map[string]issue49IncrementalCursor
}

func issue49CompleteCursorBindingsValid(observation issue49CursorBindingObservation) bool {
	if issue49ClassifyIncrementalCursor(observation.Valid, observation.Context) != "accepted" || issue49ClassifyIncrementalCursor(observation.Stale, observation.Context) != "stale" || issue49ClassifyIncrementalCursor(observation.Misbound, observation.Context) != "forged" {
		return false
	}
	wantTampered := []string{"kind", "token_version", "key_id", "stream_generation", "position_kind", "user_binding", "client_binding", "client_generation", "scope_id", "schema_hash", "membership_generation", "retention_generation", "commit_lsn", "event_ordinal", "effect_ordinal", "issued_at", "mac"}
	if len(observation.Tampered) != len(wantTampered) {
		return false
	}
	for _, binding := range wantTampered {
		token, exists := observation.Tampered[binding]
		if !exists || issue49ClassifyIncrementalCursor(token, observation.Context) != "forged" {
			return false
		}
	}
	return true
}

func issue49ClassifyIncrementalCursor(token issue49IncrementalCursor, context issue49IncrementalCursorContext) string {
	if token.Kind != "incremental" || token.TokenVersion != 1 || token.KeyID == "" || token.StreamGeneration == 0 || token.UserBinding == "" || token.ClientBinding == "" || token.ClientGeneration == 0 || token.ScopeID == "" || token.SchemaHash == "" || token.MembershipGeneration == 0 || token.RetentionGeneration == 0 || token.IssuedAt == "" {
		return "forged"
	}
	issuedAt, err := time.Parse(time.RFC3339Nano, token.IssuedAt)
	if err != nil || !strings.HasSuffix(token.IssuedAt, "Z") || issuedAt.Location() != time.UTC {
		return "forged"
	}
	switch token.PositionKind {
	case "generation_start":
		if token.CommitLSN != 0 || token.EventOrdinal != 0 || token.EffectOrdinal != 0 {
			return "forged"
		}
	case "transaction_end":
		if token.CommitLSN == 0 || token.EventOrdinal != 0 || token.EffectOrdinal != 0 {
			return "forged"
		}
	case "effect":
		if token.CommitLSN == 0 {
			return "forged"
		}
	default:
		return "forged"
	}
	key, exists := context.Keys[token.KeyID]
	if !exists || len(key) == 0 || !hmac.Equal(token.MAC, issue49IncrementalCursorMAC(token, key)) {
		return "forged"
	}
	if token.UserBinding != context.UserBinding || token.ClientBinding != context.ClientBinding || token.ScopeID != context.ScopeID {
		return "forged"
	}
	if token.ClientGeneration != context.ClientGeneration || token.SchemaHash != context.SchemaHash || token.MembershipGeneration != context.MembershipGeneration || token.RetentionGeneration != context.RetentionGeneration || token.StreamGeneration != context.StreamGeneration || token.CommitLSN < context.RetentionFloorCommitLSN {
		return "stale"
	}
	return "accepted"
}

func issue49IncrementalCursorMAC(token issue49IncrementalCursor, key []byte) []byte {
	input := bytes.NewBuffer(nil)
	issue49WriteCursorText(input, token.Kind)
	issue49WriteCursorUint64(input, token.TokenVersion)
	issue49WriteCursorText(input, token.KeyID)
	issue49WriteCursorUint64(input, token.StreamGeneration)
	issue49WriteCursorText(input, token.PositionKind)
	issue49WriteCursorText(input, token.UserBinding)
	issue49WriteCursorText(input, token.ClientBinding)
	issue49WriteCursorUint64(input, token.ClientGeneration)
	issue49WriteCursorText(input, token.ScopeID)
	issue49WriteCursorText(input, token.SchemaHash)
	issue49WriteCursorUint64(input, token.MembershipGeneration)
	issue49WriteCursorUint64(input, token.RetentionGeneration)
	issue49WriteCursorUint64(input, token.CommitLSN)
	issue49WriteCursorUint64(input, token.EventOrdinal)
	issue49WriteCursorUint64(input, token.EffectOrdinal)
	issue49WriteCursorText(input, token.IssuedAt)
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(input.Bytes())
	return mac.Sum(nil)
}

func issue49WriteCursorText(buffer *bytes.Buffer, value string) {
	issue49WriteCursorUint64(buffer, uint64(len([]byte(value))))
	_, _ = buffer.WriteString(value)
}

func issue49WriteCursorUint64(buffer *bytes.Buffer, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = buffer.Write(encoded[:])
}

func uniqueNonemptyStrings(values []string) bool {
	return len(values) > 0 && uniqueStrings(values)
}

func uniqueStrings(values []string) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" {
			return false
		}
		if _, duplicate := seen[value]; duplicate {
			return false
		}
		seen[value] = struct{}{}
	}
	return true
}
