package invariants

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

const (
	cursorTableID   = "00000000-0000-4000-8000-000000000061"
	cursorPKFieldID = "00000000-0000-4000-8000-000000000062"
)

func TestCheckCursorMonotonicityAcceptsBoundResultsAndAcknowledgements(t *testing.T) {
	observations := cursorMonotonicityFixture(t)
	violations, err := CheckCursorMonotonicity(observations)
	assertNoViolations(t, violations, err)
}

func TestCheckCursorMonotonicityCatchesEachRule(t *testing.T) {
	tests := []struct {
		name   string
		ruleID RuleID
		mutate func(*testing.T, *[]Observation)
	}{
		{name: "unexpected status", ruleID: RuleCursorUnexpectedStatus, mutate: func(_ *testing.T, observations *[]Observation) {
			(*observations)[0].WireExchanges[0].ResponseStatus = 503
			(*observations)[1].CursorAcknowledgements = nil
		}},
		{name: "wire shape", ruleID: RuleCursorWireShapeInvalid, mutate: func(_ *testing.T, observations *[]Observation) {
			(*observations)[0].WireExchanges[0].RequestBody = []byte(`{}`)
			(*observations)[1].CursorAcknowledgements = nil
		}},
		{name: "terminal page", ruleID: RuleCursorTerminalPageInvalid, mutate: func(t *testing.T, observations *[]Observation) {
			setFixturePullMember(t, &(*observations)[0].WireExchanges[0], "has_more", true)
		}},
		{name: "empty changes", ruleID: RuleCursorChangeSetMismatch, mutate: func(t *testing.T, observations *[]Observation) {
			setFixturePullMember(t, &(*observations)[0].WireExchanges[0], "changes", []any{})
		}},
		{name: "extra changes", ruleID: RuleCursorChangeSetMismatch, mutate: func(t *testing.T, observations *[]Observation) {
			response := decodeFixtureObject(t, (*observations)[0].WireExchanges[0].ResponseBody)
			changes := response["changes"].([]any)
			response["changes"] = append(changes, changes[0])
			(*observations)[0].WireExchanges[0].ResponseBody = marshalFixture(t, response)
		}},
		{name: "terminal set mismatch", ruleID: RuleCursorTerminalSetMismatch, mutate: func(t *testing.T, observations *[]Observation) {
			setFixtureScopeCursors(t, &(*observations)[0].WireExchanges[0], map[string]any{"scope-a": "cursor-terminal-a"})
			(*observations)[1].CursorAcknowledgements = (*observations)[1].CursorAcknowledgements[:1]
		}},
		{name: "terminal value invalid", ruleID: RuleCursorTerminalValueInvalid, mutate: func(t *testing.T, observations *[]Observation) {
			setFixtureScopeCursors(t, &(*observations)[0].WireExchanges[0], map[string]any{
				"scope-a": "", "scope-b": "cursor-terminal-b",
			})
			(*observations)[1].CursorAcknowledgements = (*observations)[1].CursorAcknowledgements[1:]
		}},
		{name: "checkpoint duplicate", ruleID: RuleCursorCheckpointDuplicate, mutate: func(_ *testing.T, observations *[]Observation) {
			checkpoint := (*observations)[1].Operator.Checkpoints[0]
			(*observations)[1].Operator.Checkpoints = append((*observations)[1].Operator.Checkpoints, checkpoint)
		}},
		{name: "checkpoint position invalid", ruleID: RuleCursorCheckpointPositionInvalid, mutate: func(_ *testing.T, observations *[]Observation) {
			ordinal := uint64(1)
			(*observations)[1].Operator.Checkpoints[0].Position.EventOrdinal = &ordinal
			(*observations)[0].PullResults = nil
			(*observations)[1].CursorAcknowledgements = nil
		}},
		{name: "checkpoint regressed", ruleID: RuleCursorCheckpointRegressed, mutate: func(_ *testing.T, observations *[]Observation) {
			later := "0/30"
			(*observations)[0].Operator.Checkpoints[0].Position.CommitLSN = &later
		}},
		{name: "terminal checkpoint invalid", ruleID: RuleCursorTerminalCheckpointInvalid, mutate: func(_ *testing.T, observations *[]Observation) {
			(*observations)[1].Operator.Checkpoints = (*observations)[1].Operator.Checkpoints[1:]
		}},
		{name: "client generation regressed", ruleID: RuleCursorClientGenerationRegressed, mutate: func(_ *testing.T, observations *[]Observation) {
			regressed := "cursor-generation-regressed"
			(*observations)[1].Clients[0].Scopes[0].Generation = 1
			(*observations)[1].Clients[0].Scopes[0].RawCursor = &regressed
			(*observations)[1].CursorPositions[0].Generation = 1
			(*observations)[1].CursorPositions[0].RawCursor = regressed
		}},
		{name: "client cursor value invalid", ruleID: RuleCursorClientValueInvalid, mutate: func(_ *testing.T, observations *[]Observation) {
			empty := ""
			(*observations)[1].Clients[0].Scopes[0].RawCursor = &empty
		}},
		{name: "client cursor regressed", ruleID: RuleCursorClientRegressed, mutate: func(_ *testing.T, observations *[]Observation) {
			regressed := "cursor-regressed"
			lsn := "0/15"
			(*observations)[1].Clients[0].Scopes[0].RawCursor = &regressed
			(*observations)[1].CursorPositions[0].RawCursor = regressed
			(*observations)[1].CursorPositions[0].Position.CommitLSN = &lsn
		}},
		{name: "cursor position unbound", ruleID: RuleCursorPositionUnbound, mutate: func(_ *testing.T, observations *[]Observation) {
			(*observations)[0].CursorPositions = (*observations)[0].CursorPositions[1:]
		}},
		{name: "terminal acknowledgement missing", ruleID: RuleCursorTerminalAcknowledgement, mutate: func(_ *testing.T, observations *[]Observation) {
			(*observations)[1].CursorAcknowledgements = (*observations)[1].CursorAcknowledgements[:1]
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			observations := cursorMonotonicityFixture(t)
			test.mutate(t, &observations)
			violations, err := CheckCursorMonotonicity(observations)
			assertCaughtRule(t, violations, err, test.ruleID)
		})
	}
}

func TestCheckCursorMonotonicityOrdersMapDerivedFailures(t *testing.T) {
	observations := cursorMonotonicityFixture(t)
	observation := observations[0]
	observation.Clients = nil
	observation.CursorPositions = nil
	setFixtureScopeCursors(t, &observation.WireExchanges[0], map[string]any{"scope-b": "", "scope-a": ""})
	violations, err := CheckCursorMonotonicity([]Observation{observation})
	if err != nil {
		t.Fatalf("checker error = %v", err)
	}
	assertViolationBounds(t, violations)
	want := []Violation{
		{
			Family: InvariantCursorMonotonicity, RuleID: RuleCursorTerminalValueInvalid, ObservationSequence: 1,
			Evidence: []EvidenceField{{Name: "exchange_sequence", Value: "1"}, {Name: "client_id", Value: "client-a"}, {Name: "scope_id", Value: "scope-a"}},
		},
		{
			Family: InvariantCursorMonotonicity, RuleID: RuleCursorTerminalValueInvalid, ObservationSequence: 1,
			Evidence: []EvidenceField{{Name: "exchange_sequence", Value: "1"}, {Name: "client_id", Value: "client-a"}, {Name: "scope_id", Value: "scope-b"}},
		},
	}
	if !reflect.DeepEqual(violations, want) {
		t.Fatalf("violations = %+v, want %+v", violations, want)
	}
}

func cursorMonotonicityFixture(t *testing.T) []Observation {
	t.Helper()
	initialLSN := "0/10"
	terminalLSN := "0/20"
	positions := []CursorPositionObservation{
		cursorPositionFixture("scope-a", "cursor-terminal-a", terminalLSN),
		cursorPositionFixture("scope-b", "cursor-terminal-b", terminalLSN),
	}
	first := Observation{
		Sequence: 1,
		Operator: &OperatorObservation{Checkpoints: []OperatorCheckpointObservation{
			checkpointFixture("scope-a", initialLSN), checkpointFixture("scope-b", initialLSN),
		}},
		Clients:         []ClientObservation{clientCursorFixture("cursor-terminal-a", "cursor-terminal-b", 2)},
		CursorPositions: append([]CursorPositionObservation(nil), positions...),
		WireExchanges: []WireExchangeObservation{
			pullExchangeFixture(t, 1, map[string]string{"scope-a": "cursor-old-a", "scope-b": "cursor-old-b"}, map[string]string{
				"scope-a": "cursor-terminal-a", "scope-b": "cursor-terminal-b",
			}, true),
		},
		PullResults: []PullResultObservation{{
			ExchangeSequence: 1, UserID: "user-a", ClientID: "client-a",
			Changes: []PullChangeIdentityObservation{{
				ScopeID: "scope-a", TableID: cursorTableID, PrimaryKeyFieldID: cursorPKFieldID, PrimaryKey: json.RawMessage(`"row-a"`),
			}},
			Cursors: append([]CursorPositionObservation(nil), positions...),
		}},
	}
	secondPositions := append([]CursorPositionObservation(nil), positions...)
	second := Observation{
		Sequence: 2,
		Operator: &OperatorObservation{Checkpoints: []OperatorCheckpointObservation{
			checkpointFixture("scope-a", terminalLSN), checkpointFixture("scope-b", terminalLSN),
		}},
		Clients:         []ClientObservation{clientCursorFixture("cursor-terminal-a", "cursor-terminal-b", 2)},
		CursorPositions: secondPositions,
		WireExchanges: []WireExchangeObservation{
			pullExchangeFixture(t, 2, map[string]string{
				"scope-a": "cursor-terminal-a", "scope-b": "cursor-terminal-b",
			}, map[string]string{"scope-a": "cursor-next-a", "scope-b": "cursor-next-b"}, false),
		},
		CursorAcknowledgements: []CursorAcknowledgementObservation{
			{ExchangeSequence: 2, Cursor: positions[0]},
			{ExchangeSequence: 2, Cursor: positions[1]},
		},
	}
	return []Observation{first, second}
}

func pullExchangeFixture(t *testing.T, sequence uint64, requestCursors, responseCursors map[string]string, includeChange bool) WireExchangeObservation {
	t.Helper()
	requestScopes := make(map[string]any, len(requestCursors))
	for scopeID, cursor := range requestCursors {
		requestScopes[scopeID] = map[string]any{"cursor": cursor}
	}
	changes := []any{}
	if includeChange {
		changes = append(changes, map[string]any{
			"scope": "scope-a", "table": cursorTableID, "pk": map[string]any{cursorPKFieldID: "row-a"},
		})
	}
	responseScopeCursors := make(map[string]any, len(responseCursors))
	for scopeID, cursor := range responseCursors {
		responseScopeCursors[scopeID] = cursor
	}
	request := map[string]any{"client_id": "client-a", "scopes": requestScopes}
	response := map[string]any{
		"changes": changes, "scope_cursors": responseScopeCursors,
		"scope_updates": map[string]any{"add": []any{}, "remove": []any{}},
		"rebuild":       []any{}, "has_more": false,
	}
	return WireExchangeObservation{
		Sequence: sequence, OperationClass: "pull", RequestBody: marshalFixture(t, request),
		ResponseStatus: 200, ResponseBody: marshalFixture(t, response),
	}
}

func checkpointFixture(scopeID, lsn string) OperatorCheckpointObservation {
	return OperatorCheckpointObservation{
		UserID: "user-a", ClientID: "client-a", ScopeID: scopeID, StreamGeneration: "stream-a",
		Position: PositionObservation{Kind: "transaction_end", CommitLSN: &lsn},
	}
}

func cursorPositionFixture(scopeID, cursor, lsn string) CursorPositionObservation {
	return CursorPositionObservation{
		UserID: "user-a", ClientID: "client-a", ScopeID: scopeID, Generation: 2,
		RawCursor: cursor, StreamGeneration: "stream-a",
		Position: PositionObservation{Kind: "transaction_end", CommitLSN: &lsn},
	}
}

func clientCursorFixture(cursorA, cursorB string, generation uint64) ClientObservation {
	return ClientObservation{
		State: scenarios.ClientDurabilityFact{UserID: "user-a", ClientID: "client-a"},
		Scopes: []ClientScopeObservation{
			{ScopeID: "scope-a", RawCursor: &cursorA, Generation: generation},
			{ScopeID: "scope-b", RawCursor: &cursorB, Generation: generation},
		},
	}
}

func setFixtureScopeCursors(t *testing.T, exchange *WireExchangeObservation, cursors map[string]any) {
	t.Helper()
	setFixturePullMember(t, exchange, "scope_cursors", cursors)
}

func setFixturePullMember(t *testing.T, exchange *WireExchangeObservation, name string, value any) {
	t.Helper()
	response := decodeFixtureObject(t, exchange.ResponseBody)
	response[name] = value
	exchange.ResponseBody = marshalFixture(t, response)
}
