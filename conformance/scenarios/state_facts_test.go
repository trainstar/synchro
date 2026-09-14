package scenarios

import "testing"

func TestNormalizeStateFactsPreservesProjectionAndCanonicalizesOrder(t *testing.T) {
	emptyRows := []RowFact{}
	source := StateFacts{
		Rows: emptyRows,
		Scopes: []ScopeFact{
			{ScopeID: "scope-b", EffectVersions: []string{"v2", "v1"}},
			{ScopeID: "scope-a", EffectVersions: []string{}},
		},
		MutationOutcomes: []MutationOutcomeIdentityFact{
			{UserID: "user-b", ClientID: "client-b", MutationID: "mutation-b"},
			{UserID: "user-a", ClientID: "client-a", MutationID: "mutation-a"},
		},
		RowScopeEdges: []RowScopeEdgeFact{
			{TableID: "items", CanonicalWireJSON: `"row-b"`, ScopeID: "scope-b"},
			{TableID: "items", CanonicalWireJSON: `"row-a"`, ScopeID: "scope-a"},
		},
	}
	normalized, err := NormalizeStateFacts(source)
	if err != nil {
		t.Fatalf("normalize state facts: %v", err)
	}
	if normalized.Rows == nil || len(normalized.Rows) != 0 {
		t.Fatal("normalization lost an explicit empty row projection")
	}
	if len(normalized.Scopes) != 2 || normalized.Scopes[0].ScopeID != "scope-a" || normalized.Scopes[1].ScopeID != "scope-b" {
		t.Fatalf("scope order = %+v", normalized.Scopes)
	}
	if got := normalized.Scopes[1].EffectVersions; len(got) != 2 || got[0] != "v1" || got[1] != "v2" {
		t.Fatalf("effect version order = %v", got)
	}
	if source.Scopes[0].ScopeID != "scope-b" || source.Scopes[0].EffectVersions[0] != "v2" {
		t.Fatal("normalization mutated its input")
	}
	if got := normalized.MutationOutcomes; len(got) != 2 || got[0].MutationID != "mutation-a" || got[1].MutationID != "mutation-b" {
		t.Fatalf("mutation outcome identity order = %v", got)
	}
	if got := normalized.RowScopeEdges; len(got) != 2 || got[0].CanonicalWireJSON != `"row-a"` || got[1].CanonicalWireJSON != `"row-b"` {
		t.Fatalf("row scope edge order = %v", got)
	}
	if source.MutationOutcomes[0].MutationID != "mutation-b" || source.RowScopeEdges[0].CanonicalWireJSON != `"row-b"` {
		t.Fatal("normalization changed a new observation family")
	}
}

func TestStateFactsProjectionEqualDistinguishesOmittedAndEmptyLists(t *testing.T) {
	got := StateFacts{Rows: []RowFact{{TableID: "items", CanonicalWireJSON: `"one"`}}}
	if !StateFactsProjectionEqual(StateFacts{}, got) {
		t.Fatal("omitted rows did not act as an omitted projection")
	}
	if StateFactsProjectionEqual(StateFacts{Rows: []RowFact{}}, got) {
		t.Fatal("explicit empty rows accepted a nonempty observation")
	}
}

func TestNormalizeStateFactsRejectsDuplicateNestedIdentity(t *testing.T) {
	_, err := NormalizeStateFacts(StateFacts{Clients: []ClientDurabilityFact{{
		UserID:   "user-a",
		ClientID: "client-a",
		Checkpoints: []CheckpointFact{
			{ScopeID: "scope-a"},
			{ScopeID: "scope-a"},
		},
	}}})
	if err == nil {
		t.Fatal("duplicate checkpoint identity passed normalization")
	}
}

func TestNormalizeStateFactsRejectsDuplicateServerObservationIdentity(t *testing.T) {
	for _, facts := range []StateFacts{
		{MutationOutcomes: []MutationOutcomeIdentityFact{
			{UserID: "user-a", ClientID: "client-a", MutationID: "mutation-a"},
			{UserID: "user-a", ClientID: "client-a", MutationID: "mutation-a"},
		}},
		{RowScopeEdges: []RowScopeEdgeFact{
			{TableID: "items", CanonicalWireJSON: `"row-a"`, ScopeID: "scope-a"},
			{TableID: "items", CanonicalWireJSON: `"row-a"`, ScopeID: "scope-a"},
		}},
	} {
		if _, err := NormalizeStateFacts(facts); err == nil {
			t.Fatal("duplicate server observation identity passed normalization")
		}
	}
}

func TestStateFactsProjectionEqualIgnoresOmittedServerObservationFamilies(t *testing.T) {
	want := StateFacts{
		Rows:   []RowFact{{TableID: "items", CanonicalWireJSON: `"row-a"`}},
		Scopes: []ScopeFact{{ScopeID: "scope-a"}},
	}
	got := want
	got.MutationOutcomes = []MutationOutcomeIdentityFact{{UserID: "user-a", ClientID: "client-a", MutationID: "mutation-a"}}
	got.RowScopeEdges = []RowScopeEdgeFact{{TableID: "items", CanonicalWireJSON: `"row-a"`, ScopeID: "scope-a"}}
	if !StateFactsProjectionEqual(want, got) {
		t.Fatal("omitted server observation families changed an authored projection")
	}
	if StateFactsProjectionEqual(StateFacts{MutationOutcomes: []MutationOutcomeIdentityFact{}}, got) {
		t.Fatal("explicit empty mutation outcome identities accepted an observation")
	}
	if StateFactsProjectionEqual(StateFacts{RowScopeEdges: []RowScopeEdgeFact{}}, got) {
		t.Fatal("explicit empty row scope edges accepted an observation")
	}
}
