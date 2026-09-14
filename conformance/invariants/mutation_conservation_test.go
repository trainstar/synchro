package invariants

import (
	"encoding/json"
	"strings"
	"testing"
)

const (
	mutationBatchID      = "00000000-0000-4000-8000-000000000020"
	mutationTableID      = "00000000-0000-4000-8000-000000000030"
	mutationPKFieldID    = "00000000-0000-4000-8000-000000000031"
	mutationOwnerFieldID = "00000000-0000-4000-8000-000000000032"
	mutationValueFieldID = "00000000-0000-4000-8000-000000000033"
)

var mutationIDs = []string{
	"00000000-0000-4000-8000-000000000011",
	"00000000-0000-4000-8000-000000000012",
	"00000000-0000-4000-8000-000000000013",
	"00000000-0000-4000-8000-000000000014",
}

func TestCheckMutationConservationAcceptsCompleteOrderedPartition(t *testing.T) {
	observation := mutationConservationFixture(t)
	violations, err := CheckMutationConservation([]Observation{observation})
	assertNoViolations(t, violations, err)
}

func TestCheckMutationConservationCatchesEachRule(t *testing.T) {
	tests := []struct {
		name   string
		ruleID RuleID
		mutate func(*testing.T, *Observation)
	}{
		{name: "unexpected status", ruleID: RuleMutationUnexpectedStatus, mutate: func(_ *testing.T, observation *Observation) {
			observation.WireExchanges[0].ResponseStatus = 503
		}},
		{name: "wire shape", ruleID: RuleMutationWireShapeInvalid, mutate: func(t *testing.T, observation *Observation) {
			request := decodeFixtureObject(t, observation.WireExchanges[0].RequestBody)
			delete(request, "client_id")
			observation.WireExchanges[0].RequestBody = marshalFixture(t, request)
		}},
		{name: "malformed client version", ruleID: RuleMutationWireShapeInvalid, mutate: func(t *testing.T, observation *Observation) {
			request := decodeFixtureObject(t, observation.WireExchanges[0].RequestBody)
			request["mutations"].([]any)[0].(map[string]any)["client_version"] = "2032-01-02T03:04:05Z"
			observation.WireExchanges[0].RequestBody = marshalFixture(t, request)
		}},
		{name: "batch mismatch", ruleID: RuleMutationBatchMismatch, mutate: func(t *testing.T, observation *Observation) {
			response := decodeFixtureObject(t, observation.WireExchanges[0].ResponseBody)
			response["batch_id"] = "00000000-0000-4000-8000-000000000099"
			observation.WireExchanges[0].ResponseBody = marshalFixture(t, response)
		}},
		{name: "outcome ID invalid", ruleID: RuleMutationOutcomeIDInvalid, mutate: func(t *testing.T, observation *Observation) {
			response := decodeFixtureObject(t, observation.WireExchanges[0].ResponseBody)
			accepted := response["accepted"].([]any)
			response["accepted"] = append(accepted, map[string]any{"mutation_id": "not-a-uuid"})
			observation.WireExchanges[0].ResponseBody = marshalFixture(t, response)
		}},
		{name: "outcome status invalid", ruleID: RuleMutationOutcomeStatusInvalid, mutate: mutateMutationOutcome(func(outcome map[string]any) {
			outcome["status"] = "conflict"
		})},
		{name: "outcome duplicate", ruleID: RuleMutationOutcomeDuplicate, mutate: func(t *testing.T, observation *Observation) {
			response := decodeFixtureObject(t, observation.WireExchanges[0].ResponseBody)
			accepted := response["accepted"].([]any)
			response["accepted"] = append(accepted, accepted[len(accepted)-1])
			observation.WireExchanges[0].ResponseBody = marshalFixture(t, response)
		}},
		{name: "outcome unrequested", ruleID: RuleMutationOutcomeUnrequested, mutate: func(t *testing.T, observation *Observation) {
			response := decodeFixtureObject(t, observation.WireExchanges[0].ResponseBody)
			accepted := response["accepted"].([]any)
			outcome := decodeFixtureObject(t, marshalFixture(t, accepted[len(accepted)-1]))
			outcome["mutation_id"] = "00000000-0000-4000-8000-000000000099"
			response["accepted"] = append(accepted, outcome)
			observation.WireExchanges[0].ResponseBody = marshalFixture(t, response)
		}},
		{name: "outcome omitted", ruleID: RuleMutationOutcomeOmitted, mutate: func(t *testing.T, observation *Observation) {
			response := decodeFixtureObject(t, observation.WireExchanges[0].ResponseBody)
			accepted := response["accepted"].([]any)
			response["accepted"] = accepted[:1]
			observation.WireExchanges[0].ResponseBody = marshalFixture(t, response)
		}},
		{name: "outcome order", ruleID: RuleMutationOutcomeOrder, mutate: func(t *testing.T, observation *Observation) {
			response := decodeFixtureObject(t, observation.WireExchanges[0].ResponseBody)
			accepted := response["accepted"].([]any)
			accepted[0], accepted[1] = accepted[1], accepted[0]
			observation.WireExchanges[0].ResponseBody = marshalFixture(t, response)
		}},
		{name: "outcome table", ruleID: RuleMutationOutcomeTable, mutate: mutateMutationOutcome(func(outcome map[string]any) {
			outcome["table"] = "00000000-0000-4000-8000-000000000099"
		})},
		{name: "outcome primary key", ruleID: RuleMutationOutcomePrimaryKey, mutate: mutateMutationOutcome(func(outcome map[string]any) {
			outcome["pk"] = map[string]any{mutationPKFieldID: "row-mutant"}
		})},
		{name: "outcome schema", ruleID: RuleMutationOutcomeSchema, mutate: mutateMutationOutcome(func(outcome map[string]any) {
			outcome["outcome_schema"] = map[string]any{"version": 2, "hash": strings.Repeat("a", 64)}
		})},
		{name: "outcome code", ruleID: RuleMutationOutcomeCode, mutate: mutateMutationOutcome(func(outcome map[string]any) {
			outcome["code"] = "row_already_exists"
		})},
		{name: "outcome server row", ruleID: RuleMutationOutcomeServerRow, mutate: mutateMutationOutcome(func(outcome map[string]any) {
			outcome["server_row"] = map[string]any{mutationOwnerFieldID: "wrong", mutationValueFieldID: "value-a"}
		})},
		{name: "outcome server version", ruleID: RuleMutationOutcomeServerVersion, mutate: mutateMutationOutcome(func(outcome map[string]any) {
			outcome["server_version"] = "not-a-uuid"
		})},
		{name: "outcome checksum", ruleID: RuleMutationOutcomeChecksum, mutate: mutateMutationOutcome(func(outcome map[string]any) {
			outcome["row_checksum"].(map[string]any)["algorithm"] = "sha1"
		})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			observation := mutationConservationFixture(t)
			test.mutate(t, &observation)
			violations, err := CheckMutationConservation([]Observation{observation})
			assertCaughtRule(t, violations, err, test.ruleID)
		})
	}
}

func TestCheckMutationConservationRejectsMalformedRequestMutationIdentifier(t *testing.T) {
	observation := mutationConservationFixture(t)
	request := decodeFixtureObject(t, observation.WireExchanges[0].RequestBody)
	request["mutations"].([]any)[0].(map[string]any)["mutation_id"] = "mutation-a"
	observation.WireExchanges[0].RequestBody = marshalFixture(t, request)
	violations, err := CheckMutationConservation([]Observation{observation})
	assertCaughtRule(t, violations, err, RuleMutationWireShapeInvalid)
}

func TestCheckMutationConservationAcceptsRejectedTerminalControlOutcome(t *testing.T) {
	observation := mutationConservationFixture(t)
	response := decodeFixtureObject(t, observation.WireExchanges[0].ResponseBody)
	outcome := response["rejected"].([]any)[0].(map[string]any)
	outcome["status"] = "rejected_terminal"
	outcome["code"] = "policy_rejected"
	delete(outcome, "server_row")
	delete(outcome, "server_version")
	delete(outcome, "row_checksum")
	observation.WireExchanges[0].ResponseBody = marshalFixture(t, response)
	violations, err := CheckMutationConservation([]Observation{observation})
	assertNoViolations(t, violations, err)
}

func mutationConservationFixture(t *testing.T) Observation {
	t.Helper()
	schema := map[string]any{"version": 1, "hash": strings.Repeat("a", 64)}
	mutations := make([]map[string]any, 0, len(mutationIDs))
	accepted := make([]map[string]any, 0, 2)
	rejected := make([]map[string]any, 0, 2)
	for index, mutationID := range mutationIDs {
		rowID := "row-" + string(rune('a'+index))
		value := "value-" + string(rune('a'+index))
		columns := map[string]any{mutationOwnerFieldID: "diagnostic-user", mutationValueFieldID: value}
		mutations = append(mutations, map[string]any{
			"mutation_id": mutationID, "table": mutationTableID,
			"pk": map[string]any{mutationPKFieldID: rowID}, "authored_schema": schema,
			"op": "insert", "client_version": "2032-01-02T03:04:05.000000Z", "columns": columns,
		})
		outcome := map[string]any{
			"mutation_id": mutationID, "table": mutationTableID,
			"pk": map[string]any{mutationPKFieldID: rowID}, "outcome_schema": schema,
			"server_row": columns, "server_version": "00000000-0000-4000-8000-00000000005" + string(rune('0'+index)),
			"row_checksum": checksumFixture(strings.Repeat(string(rune('a'+index)), 64)),
		}
		if index%2 == 0 {
			outcome["status"] = "applied"
			accepted = append(accepted, outcome)
		} else {
			outcome["status"] = "conflict"
			outcome["code"] = "row_already_exists"
			rejected = append(rejected, outcome)
		}
	}
	request := map[string]any{
		"client_id": "client-authored", "client_generation": 1,
		"batch_id": mutationBatchID, "schema": schema, "mutations": mutations,
	}
	response := map[string]any{"batch_id": mutationBatchID, "accepted": accepted, "rejected": rejected}
	return Observation{
		Sequence: 11,
		WireExchanges: []WireExchangeObservation{{
			Sequence: 4, OperationClass: "push", RequestBody: marshalFixture(t, request),
			ResponseStatus: 200, ResponseBody: marshalFixture(t, response), ExpectMutationConservation: true,
		}},
	}
}

func mutateMutationOutcome(mutate func(map[string]any)) func(*testing.T, *Observation) {
	return func(t *testing.T, observation *Observation) {
		response := decodeFixtureObject(t, observation.WireExchanges[0].ResponseBody)
		outcome := response["accepted"].([]any)[0].(map[string]any)
		mutate(outcome)
		observation.WireExchanges[0].ResponseBody = marshalFixture(t, response)
	}
}

func decodeFixtureObject(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("decode authored object: %v", err)
	}
	return value
}

func checksumFixture(digest string) map[string]any {
	return map[string]any{"algorithm": "sha256", "version": 1, "encoding": "hex", "digest": digest}
}

func marshalFixture(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal authored fixture: %v", err)
	}
	return encoded
}
