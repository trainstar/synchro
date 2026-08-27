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
