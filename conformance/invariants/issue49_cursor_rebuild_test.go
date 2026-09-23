package invariants

import "testing"

func TestIssue49OpaqueScopeCursors(t *testing.T) {
	observation := []issue49OpaqueCursorCase{
		{
			Name:                   "non-null",
			ServerIssued:           "z10.A/+=:009-opaque",
			Persisted:              "z10.A/+=:009-opaque",
			Presented:              "z10.A/+=:009-opaque",
			AuthenticatedBeforeUse: true,
			Usable:                 true,
		},
		{Name: "null", Null: true, RebuildRequired: true},
	}
	mutant := append([]issue49OpaqueCursorCase(nil), observation...)
	mutant[0].Presented = "z10.a/+=:9-opaque"
	issue49Proof(t, "SYNC-CURSOR-001", issue49OpaqueCursorsValid(observation), issue49OpaqueCursorsValid(mutant))
}

func TestIssue49ScopeSetVersionMonotonicity(t *testing.T) {
	states := []issue49ScopeSetState{
		{Assigned: []string{"documents:user-17", "templates:public"}, Version: 41},
		{Assigned: []string{"templates:public", "documents:user-17"}, Version: 41},
		{Assigned: []string{"documents:user-17", "projects:team-9", "templates:public"}, Version: 44},
		{Assigned: []string{"templates:public", "projects:team-9", "documents:user-17"}, Version: 44},
		{Assigned: []string{"projects:team-9", "templates:public"}, Version: 45},
	}
	mutant := append([]issue49ScopeSetState(nil), states...)
	mutant[1].Version = 42
	issue49Proof(t, "SYNC-SCOPE-002", issue49ScopeSetVersionsValid(states), issue49ScopeSetVersionsValid(mutant))
}

func TestIssue49ScopeLocalRebuild(t *testing.T) {
	observation := issue49ScopeLocalRebuild{
		RebuiltScope: "scope-a",
		BeforeScopes: map[string][]string{
			"scope-a": {"a-before", "shared-row"},
			"scope-b": {"b-stable", "shared-row"},
		},
		AfterScopes: map[string][]string{
			"scope-a": {"a-after", "shared-row"},
			"scope-b": {"shared-row", "b-stable"},
		},
		BeforeLocalOnly: []string{"draft-17", "preference-4"},
		AfterLocalOnly:  []string{"preference-4", "draft-17"},
	}
	mutant := cloneIssue49ScopeLocalRebuild(observation)
	mutant.AfterScopes["scope-b"] = []string{"shared-row"}
	issue49Proof(t, "SYNC-REBUILD-001", issue49ScopeLocalRebuildValid(observation), issue49ScopeLocalRebuildValid(mutant))
}

func TestIssue49PerScopeCursorApplyOrdering(t *testing.T) {
	observation := []issue49ScopeCursorApply{
		{
			Scope:          "scope-a",
			BeforeCursor:   "cursor-a-4",
			ReturnedCursor: "cursor-a-7",
			AfterCursor:    "cursor-a-7",
			Outcomes: []issue49AppliedOutcome{
				{ID: "a-change-5", Validated: true, Durable: true},
				{ID: "a-change-7", Validated: true, Durable: true},
			},
		},
		{
			Scope:          "scope-b",
			BeforeCursor:   "cursor-b-2",
			ReturnedCursor: "cursor-b-8",
			AfterCursor:    "cursor-b-2",
			Outcomes: []issue49AppliedOutcome{
				{ID: "b-change-3", Validated: true, Durable: true},
				{ID: "b-change-8", Validated: false, Durable: false},
			},
		},
	}
	mutant := append([]issue49ScopeCursorApply(nil), observation...)
	mutant[1].AfterCursor = mutant[1].ReturnedCursor
	issue49Proof(t, "SYNC-CURSOR-002", issue49PerScopeCursorApplyValid(observation), issue49PerScopeCursorApplyValid(mutant))
}

func TestIssue49ProgressAndIntegritySeparation(t *testing.T) {
	observation := []issue49ScopeHealth{
		{Name: "cursor-only", CursorValid: true, AuthoritativeDigestPresent: true, AuthoritativeDigestVerified: true},
		{Name: "verified", CursorValid: true, AuthoritativeDigestPresent: true, AuthoritativeDigestVerified: true, LocalDigestMatches: true, Healthy: true},
		{Name: "rebuild", CursorValid: true, AuthoritativeDigestPresent: true, AuthoritativeDigestVerified: true, LocalDigestMatches: true, RebuildRequired: true},
	}
	mutant := append([]issue49ScopeHealth(nil), observation...)
	mutant[0].Healthy = true
	issue49Proof(t, "SYNC-INTEGRITY-001", issue49ProgressIntegritySeparationValid(observation), issue49ProgressIntegritySeparationValid(mutant))
}

func TestIssue49RebuildPreservesOverlappingProvenance(t *testing.T) {
	observation := issue49RebuildProvenance{
		RebuiltScope: "scope-a",
		Before: map[string][]string{
			"a-current": {"scope-a"},
			"a-old":     {"scope-a"},
			"b-only":    {"scope-b"},
			"overlap":   {"scope-a", "scope-b"},
		},
		StagedRows: []string{"a-current", "a-new"},
		After: map[string][]string{
			"a-current": {"scope-a"},
			"a-new":     {"scope-a"},
			"a-old":     {},
			"b-only":    {"scope-b"},
			"overlap":   {"scope-b"},
		},
		Materialized: map[string]bool{
			"a-current": true,
			"a-new":     true,
			"a-old":     false,
			"b-only":    true,
			"overlap":   true,
		},
	}
	mutant := cloneIssue49RebuildProvenance(observation)
	mutant.Materialized["overlap"] = false
	issue49Proof(t, "SYNC-PROVENANCE-001", issue49RebuildProvenanceValid(observation), issue49RebuildProvenanceValid(mutant))
}

func TestIssue49ScopeInclusiveTypedDeduplication(t *testing.T) {
	observation := issue49TypedDeduplication{
		DeclaredPKTypes: map[string]string{"tbl-counters": "int", "tbl-notes": "string"},
		Eligible: []issue49PullCandidate{
			{ID: "a-note-old", ScopeID: "scope-a", LogicalTableID: "tbl-notes", PrimaryKey: issue49TypedPullKey{Type: "string", Value: "1"}, Position: issue49EffectPosition{CommitLSN: 90, EventOrdinal: 1}},
			{ID: "b-note", ScopeID: "scope-b", LogicalTableID: "tbl-notes", PrimaryKey: issue49TypedPullKey{Type: "string", Value: "1"}, Position: issue49EffectPosition{CommitLSN: 91, EventOrdinal: 1}},
			{ID: "a-note-new", ScopeID: "scope-a", LogicalTableID: "tbl-notes", PrimaryKey: issue49TypedPullKey{Type: "string", Value: "1"}, Position: issue49EffectPosition{CommitLSN: 92, EventOrdinal: 1}},
			{ID: "a-counter", ScopeID: "scope-a", LogicalTableID: "tbl-counters", PrimaryKey: issue49TypedPullKey{Type: "int", Value: "1"}, Position: issue49EffectPosition{CommitLSN: 93, EventOrdinal: 1}},
		},
		Retained: []issue49PullCandidate{
			{ID: "b-note", ScopeID: "scope-b", LogicalTableID: "tbl-notes", PrimaryKey: issue49TypedPullKey{Type: "string", Value: "1"}, Position: issue49EffectPosition{CommitLSN: 91, EventOrdinal: 1}},
			{ID: "a-note-new", ScopeID: "scope-a", LogicalTableID: "tbl-notes", PrimaryKey: issue49TypedPullKey{Type: "string", Value: "1"}, Position: issue49EffectPosition{CommitLSN: 92, EventOrdinal: 1}},
			{ID: "a-counter", ScopeID: "scope-a", LogicalTableID: "tbl-counters", PrimaryKey: issue49TypedPullKey{Type: "int", Value: "1"}, Position: issue49EffectPosition{CommitLSN: 93, EventOrdinal: 1}},
		},
	}
	mutant := observation
	mutant.Retained = issue49TextOnlyDeduplication(observation.Eligible)
	issue49Proof(t, "SYNC-PULL-004", issue49TypedDeduplicationValid(observation), issue49TypedDeduplicationValid(mutant))
}

func TestIssue49CompleteCursorBindings(t *testing.T) {
	key := []byte("issue-49-incremental-cursor-key")
	context := issue49IncrementalCursorContext{
		UserBinding:             "user-17",
		ClientBinding:           "client-9",
		ClientGeneration:        4,
		ScopeID:                 "documents:user-17",
		SchemaHash:              "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		MembershipGeneration:    12,
		RetentionGeneration:     5,
		StreamGeneration:        7,
		RetentionFloorCommitLSN: 80,
		Keys:                    map[string][]byte{"cursor-key-3": key},
	}
	valid := issue49IncrementalCursor{
		Kind:                 "incremental",
		TokenVersion:         1,
		KeyID:                "cursor-key-3",
		StreamGeneration:     7,
		PositionKind:         "effect",
		UserBinding:          "user-17",
		ClientBinding:        "client-9",
		ClientGeneration:     4,
		ScopeID:              "documents:user-17",
		SchemaHash:           "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		MembershipGeneration: 12,
		RetentionGeneration:  5,
		CommitLSN:            91,
		EventOrdinal:         3,
		EffectOrdinal:        2,
		IssuedAt:             "2026-09-09T12:34:56.123456Z",
	}
	valid.MAC = issue49IncrementalCursorMAC(valid, key)
	stale := cloneIssue49IncrementalCursor(valid)
	stale.StreamGeneration = 6
	stale.MAC = issue49IncrementalCursorMAC(stale, key)
	misbound := cloneIssue49IncrementalCursor(valid)
	misbound.ScopeID = "documents:user-18"
	misbound.MAC = issue49IncrementalCursorMAC(misbound, key)
	observation := issue49CursorBindingObservation{
		Context:  context,
		Valid:    valid,
		Stale:    stale,
		Misbound: misbound,
		Tampered: issue49TamperedIncrementalCursors(valid),
	}
	mutant := observation
	mutant.Valid = cloneIssue49IncrementalCursor(valid)
	mutant.Valid.IssuedAt = "2026-09-09T12:34:57.123456Z"
	issue49Proof(t, "SYNC-CURSOR-005", issue49CompleteCursorBindingsValid(observation), issue49CompleteCursorBindingsValid(mutant))
}

func cloneIssue49ScopeLocalRebuild(source issue49ScopeLocalRebuild) issue49ScopeLocalRebuild {
	clone := source
	clone.BeforeScopes = make(map[string][]string, len(source.BeforeScopes))
	clone.AfterScopes = make(map[string][]string, len(source.AfterScopes))
	for scope, rows := range source.BeforeScopes {
		clone.BeforeScopes[scope] = append([]string(nil), rows...)
	}
	for scope, rows := range source.AfterScopes {
		clone.AfterScopes[scope] = append([]string(nil), rows...)
	}
	clone.BeforeLocalOnly = append([]string(nil), source.BeforeLocalOnly...)
	clone.AfterLocalOnly = append([]string(nil), source.AfterLocalOnly...)
	return clone
}

func cloneIssue49RebuildProvenance(source issue49RebuildProvenance) issue49RebuildProvenance {
	clone := source
	clone.Before = make(map[string][]string, len(source.Before))
	clone.After = make(map[string][]string, len(source.After))
	clone.Materialized = make(map[string]bool, len(source.Materialized))
	for row, edges := range source.Before {
		clone.Before[row] = append([]string(nil), edges...)
	}
	for row, edges := range source.After {
		clone.After[row] = append([]string(nil), edges...)
	}
	for row, materialized := range source.Materialized {
		clone.Materialized[row] = materialized
	}
	clone.StagedRows = append([]string(nil), source.StagedRows...)
	return clone
}

func cloneIssue49IncrementalCursor(source issue49IncrementalCursor) issue49IncrementalCursor {
	clone := source
	clone.MAC = append([]byte(nil), source.MAC...)
	return clone
}

func issue49TamperedIncrementalCursors(valid issue49IncrementalCursor) map[string]issue49IncrementalCursor {
	tampered := make(map[string]issue49IncrementalCursor, 17)
	add := func(name string, mutate func(*issue49IncrementalCursor)) {
		token := cloneIssue49IncrementalCursor(valid)
		mutate(&token)
		tampered[name] = token
	}
	add("kind", func(token *issue49IncrementalCursor) { token.Kind = "rebuild" })
	add("token_version", func(token *issue49IncrementalCursor) { token.TokenVersion = 2 })
	add("key_id", func(token *issue49IncrementalCursor) { token.KeyID = "cursor-key-4" })
	add("stream_generation", func(token *issue49IncrementalCursor) { token.StreamGeneration++ })
	add("position_kind", func(token *issue49IncrementalCursor) { token.PositionKind = "transaction_end" })
	add("user_binding", func(token *issue49IncrementalCursor) { token.UserBinding = "user-18" })
	add("client_binding", func(token *issue49IncrementalCursor) { token.ClientBinding = "client-10" })
	add("client_generation", func(token *issue49IncrementalCursor) { token.ClientGeneration++ })
	add("scope_id", func(token *issue49IncrementalCursor) { token.ScopeID = "documents:user-18" })
	add("schema_hash", func(token *issue49IncrementalCursor) {
		token.SchemaHash = "1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	})
	add("membership_generation", func(token *issue49IncrementalCursor) { token.MembershipGeneration++ })
	add("retention_generation", func(token *issue49IncrementalCursor) { token.RetentionGeneration++ })
	add("commit_lsn", func(token *issue49IncrementalCursor) { token.CommitLSN++ })
	add("event_ordinal", func(token *issue49IncrementalCursor) { token.EventOrdinal++ })
	add("effect_ordinal", func(token *issue49IncrementalCursor) { token.EffectOrdinal++ })
	add("issued_at", func(token *issue49IncrementalCursor) { token.IssuedAt = "2026-09-09T12:34:57.123456Z" })
	add("mac", func(token *issue49IncrementalCursor) { token.MAC[0] ^= 0xff })
	return tampered
}
