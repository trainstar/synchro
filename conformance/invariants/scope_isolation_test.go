package invariants

import (
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
	"github.com/trainstar/synchro/conformance/vectors"
)

func TestCheckScopeIsolationAcceptsZeroChangeAndCompleteMembership(t *testing.T) {
	observation := scopeIsolationFixture(t)
	violations, err := CheckScopeIsolation([]Observation{observation})
	assertNoViolations(t, violations, err)
}

func TestCheckScopeIsolationCatchesEachRule(t *testing.T) {
	tests := []struct {
		name   string
		ruleID RuleID
		mutate func(*testing.T, *Observation)
	}{
		{name: "unexpected status", ruleID: RuleScopeUnexpectedStatus, mutate: func(_ *testing.T, observation *Observation) {
			observation.WireExchanges[0].ResponseStatus = 503
		}},
		{name: "wire shape", ruleID: RuleScopeWireShapeInvalid, mutate: func(_ *testing.T, observation *Observation) {
			observation.WireExchanges[0].ResponseBody = []byte(`{}`)
		}},
		{name: "selection invalid", ruleID: RuleScopeSelectionInvalid, mutate: func(t *testing.T, observation *Observation) {
			request := decodeFixtureObject(t, observation.WireExchanges[0].RequestBody)
			request["scopes"].(map[string]any)["scope-b"] = map[string]any{"cursor": "cursor-b"}
			observation.WireExchanges[0].RequestBody = marshalFixture(t, request)
		}},
		{name: "unselected change", ruleID: RuleScopeUnselectedChange, mutate: func(t *testing.T, observation *Observation) {
			setFixturePullMember(t, &observation.WireExchanges[0], "changes", []any{map[string]any{"scope": "scope-global"}})
		}},
		{name: "malformed selected change", ruleID: RuleScopeUnexpectedChange, mutate: func(t *testing.T, observation *Observation) {
			setFixturePullMember(t, &observation.WireExchanges[0], "changes", []any{map[string]any{"scope": "scope-a"}})
		}},
		{name: "scope duplicate", ruleID: RuleScopeDuplicate, mutate: func(_ *testing.T, observation *Observation) {
			observation.Clients[0].Scopes = append(observation.Clients[0].Scopes, observation.Clients[0].Scopes[0])
		}},
		{name: "membership unknown", ruleID: RuleScopeMembershipUnknown, mutate: func(_ *testing.T, observation *Observation) {
			observation.Clients[0].ScopeRows[0].ScopeID = "scope-unknown"
			observation.Clients[0].Complete = false
		}},
		{name: "membership duplicate", ruleID: RuleScopeMembershipDuplicate, mutate: func(_ *testing.T, observation *Observation) {
			observation.Clients[0].ScopeRows = append(observation.Clients[0].ScopeRows, observation.Clients[0].ScopeRows[0])
		}},
		{name: "membership generation mismatch", ruleID: RuleScopeMembershipGeneration, mutate: func(_ *testing.T, observation *Observation) {
			observation.Clients[0].ScopeRows[0].Generation = 3
			observation.Clients[0].Complete = false
		}},
		{name: "server generation mismatch", ruleID: RuleScopeServerGenerationMismatch, mutate: func(_ *testing.T, observation *Observation) {
			observation.ServerState.Scopes[0].MembershipGeneration = 5
		}},
		{name: "server cardinality mismatch", ruleID: RuleScopeServerCardinalityMismatch, mutate: func(_ *testing.T, observation *Observation) {
			observation.ServerState.Scopes[0].Cardinality = 2
		}},
		{name: "row identity relation invalid", ruleID: RuleScopeRowIdentityRelationInvalid, mutate: func(_ *testing.T, observation *Observation) {
			observation.ServerRowIdentities = observation.ServerRowIdentities[1:]
			observation.Clients[0].Complete = false
		}},
		{name: "equal-cardinality swapped membership", ruleID: RuleScopeServerMembershipMismatch, mutate: func(_ *testing.T, observation *Observation) {
			observation.Clients[0].ScopeRows[0].Entry.RowIdentity = []byte("normalized-row-b")
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			observation := scopeIsolationFixture(t)
			test.mutate(t, &observation)
			violations, err := CheckScopeIsolation([]Observation{observation})
			assertCaughtRule(t, violations, err, test.ruleID)
		})
	}
}

func TestCheckScopeIsolationRejectsEqualCardinalitySwappedMembership(t *testing.T) {
	observation := scopeIsolationFixture(t)
	observation.Clients[0].ScopeRows[0].Entry.RowIdentity = []byte("normalized-row-b")
	observation.Clients[0].ScopeRows[1].Entry.RowIdentity = []byte("normalized-row-a")
	violations, err := CheckScopeIsolation([]Observation{observation})
	if err != nil {
		t.Fatalf("checker error = %v", err)
	}
	assertViolationBounds(t, violations)
	if len(violations) != 2 || violations[0].RuleID != RuleScopeServerMembershipMismatch || violations[1].RuleID != RuleScopeServerMembershipMismatch {
		t.Fatalf("violations = %+v, want two swapped-membership failures", violations)
	}
}

func TestCheckScopeIsolationRejectsChangeWithoutScope(t *testing.T) {
	observation := scopeIsolationFixture(t)
	setFixturePullMember(t, &observation.WireExchanges[0], "changes", []any{map[string]any{"table": mutationTableID}})
	violations, err := CheckScopeIsolation([]Observation{observation})
	assertCaughtRule(t, violations, err, RuleScopeWireShapeInvalid)
}

func scopeIsolationFixture(t *testing.T) Observation {
	t.Helper()
	exchange := pullExchangeFixture(
		t,
		1,
		map[string]string{"scope-a": "cursor-a"},
		map[string]string{"scope-a": "cursor-b"},
		false,
	)
	exchange.ExpectScopeIsolation = true
	return Observation{
		Sequence: 31,
		ServerState: &scenarios.StateFacts{
			Scopes: []scenarios.ScopeFact{
				{ScopeID: "scope-a", MembershipGeneration: 4, Cardinality: 1},
				{ScopeID: "scope-b", MembershipGeneration: 4, Cardinality: 1},
			},
			RowScopeEdges: []scenarios.RowScopeEdgeFact{
				{TableID: "table-authored", CanonicalWireJSON: `"row-a"`, ScopeID: "scope-a"},
				{TableID: "table-authored", CanonicalWireJSON: `"row-b"`, ScopeID: "scope-b"},
			},
		},
		ServerRowIdentities: []ServerRowIdentityObservation{
			{TableID: "table-authored", CanonicalWireJSON: `"row-a"`, RowIdentity: []byte("normalized-row-a")},
			{TableID: "table-authored", CanonicalWireJSON: `"row-b"`, RowIdentity: []byte("normalized-row-b")},
		},
		Clients: []ClientObservation{{
			State: scenarios.ClientDurabilityFact{UserID: "user-a", ClientID: "client-a"},
			Scopes: []ClientScopeObservation{
				{ScopeID: "scope-a", Generation: 4},
				{ScopeID: "scope-b", Generation: 4},
			},
			ScopeRows: []ClientScopeRowObservation{
				{ScopeID: "scope-a", Generation: 4, Entry: vectors.DigestEntry{RowIdentity: []byte("normalized-row-a")}},
				{ScopeID: "scope-b", Generation: 4, Entry: vectors.DigestEntry{RowIdentity: []byte("normalized-row-b")}},
			},
			Complete: true,
		}},
		WireExchanges: []WireExchangeObservation{exchange},
	}
}
