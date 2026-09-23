package scenarios

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
)

func TestRebuildWorkloadInputsPreserveAuthoredSourceAndPaging(t *testing.T) {
	for _, name := range []string{"rebuild-apply", "rebuild-cardinality"} {
		t.Run(name, func(t *testing.T) {
			scenario := rebuildWorkloadScenario(t, name)
			inputs, err := BuildRebuildWorkloadInputs(scenario)
			if err != nil {
				t.Fatal(err)
			}
			if len(inputs) != len(scenario.Steps) {
				t.Fatalf("input count = %d, want %d", len(inputs), len(scenario.Steps))
			}
			counts := []int{1, 1, 1, 100, 1, 1, 899, 1, 1}
			pages := []int{1, 1, 1, 2, 2, 2, 10, 10, 10}
			for index, input := range inputs {
				step := scenario.Steps[index]
				if input.StepID != step.ID || len(input.Operations) != 4+2*pages[index] {
					t.Fatalf("step %d lost its authored identity or page controls", index+1)
				}
				var commit struct {
					CommitLSN string `json:"commit_lsn"`
					Events    []struct {
						Operation string          `json:"operation"`
						Before    json.RawMessage `json:"before"`
						After     json.RawMessage `json:"after"`
					} `json:"events"`
				}
				if err := json.Unmarshal(input.Operations[0].Payload, &commit); err != nil {
					t.Fatal(err)
				}
				if OperationKey(input.Operations[0]) != "model/commit-source-transaction" ||
					commit.CommitLSN != fmt.Sprint((index+1)*10) || len(commit.Events) != counts[index] {
					t.Fatalf("step %d source transaction is invalid", index+1)
				}
				insert := index == 0 || index == 3 || index == 6
				for _, event := range commit.Events {
					if insert && (event.Operation != "insert" || string(event.Before) != "null") ||
						!insert && (event.Operation != "update" || string(event.Before) == "null") {
						t.Fatalf("step %d changed insert/update semantics", index+1)
					}
				}
				var begin struct {
					UserID   string `json:"user_id"`
					ClientID string `json:"client_id"`
				}
				if err := json.Unmarshal(input.Operations[2].Payload, &begin); err != nil {
					t.Fatal(err)
				}
				if begin.UserID != step.NativeBinding.UserID || begin.ClientID != step.NativeBinding.ClientID {
					t.Fatalf("step %d targeted another client", index+1)
				}
				for page := 0; page < pages[index]; page++ {
					request, apply := input.Operations[3+2*page], input.Operations[4+2*page]
					var requestBody struct {
						Cursor string `json:"cursor_source"`
						Limit  uint64 `json:"limit"`
					}
					var applyBody struct {
						Ordinal uint64 `json:"page_ordinal"`
						Token   string `json:"request_token_source"`
					}
					if json.Unmarshal(request.Payload, &requestBody) != nil || json.Unmarshal(apply.Payload, &applyBody) != nil {
						t.Fatal("page control is not JSON")
					}
					cursor := "none"
					if page != 0 {
						cursor = "local_rebuild_continuation"
					}
					if OperationKey(request) != "rebuild/request-page" || OperationKey(apply) != "local/apply-rebuild-page" ||
						requestBody.Limit != 100 || requestBody.Cursor != cursor ||
						applyBody.Ordinal != uint64(page*100+1) || applyBody.Token != cursor {
						t.Fatalf("step %d page %d changed its continuation contract", index+1, page+1)
					}
				}
				if index == 0 {
					assertRebuildWorkloadField(t, commit.Events[0].After, "id", "cardinality-000001")
					assertRebuildWorkloadField(t, commit.Events[0].After, "value", "cardinality-value-000001-0000000010")
				}
				if index == 1 {
					assertRebuildWorkloadField(t, commit.Events[0].Before, "value", "cardinality-value-000001-0000000010")
					assertRebuildWorkloadField(t, commit.Events[0].After, "value", "cardinality-update-0000000020")
				}
				if index == 2 {
					assertRebuildWorkloadField(t, commit.Events[0].Before, "value", "cardinality-update-0000000020")
				}
				if index == 3 {
					assertRebuildWorkloadField(t, commit.Events[0].After, "id", "cardinality-000002")
					assertRebuildWorkloadField(t, commit.Events[99].After, "id", "cardinality-000101")
				}
			}
		})
	}
}

func TestRebuildWorkloadInputsRejectInvalidBindings(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*Scenario)
	}{
		{"missing setup", func(s *Scenario) { s.Model.Setup = nil }},
		{"missing steps", func(s *Scenario) { s.Steps = nil }},
		{"missing binding", func(s *Scenario) { s.Steps[0].NativeBinding = nil }},
		{"different outcome", func(s *Scenario) { s.Steps[0].ExpectedOutcome.Disposition = "error" }},
		{"unknown client", func(s *Scenario) { s.Steps[0].NativeBinding.ClientID = "missing-client" }},
		{"changed schema", func(s *Scenario) { s.Steps[0].NativeBinding.Workload.AuthoredSchema.Version++ }},
		{"changed count", func(s *Scenario) { s.Steps[0].NativeBinding.Workload.RecordCount++ }},
		{"changed target", func(s *Scenario) { s.Steps[0].NativeBinding.Workload.Targets[0].TableID = "other" }},
		{"decreasing cardinality", func(s *Scenario) { s.Steps[0], s.Steps[3] = s.Steps[3], s.Steps[0] }},
	} {
		t.Run(test.name, func(t *testing.T) {
			scenario := rebuildWorkloadScenario(t, "rebuild-apply")
			test.change(&scenario)
			if _, err := BuildRebuildWorkloadInputs(scenario); err == nil {
				t.Fatal("invalid workload was accepted")
			}
		})
	}
}

func rebuildWorkloadScenario(t *testing.T, name string) Scenario {
	t.Helper()
	scenario, err := LoadFile(context.Background(), "../..", "conformance/scenarios/performance/"+name+"-001.json")
	if err != nil {
		t.Fatal(err)
	}
	return scenario
}

func assertRebuildWorkloadField(t *testing.T, raw json.RawMessage, field, expected string) {
	t.Helper()
	var image struct {
		Fields []struct {
			Field string `json:"field"`
			Wire  string `json:"wire_json"`
		} `json:"fields"`
	}
	if err := json.Unmarshal(raw, &image); err != nil {
		t.Fatal(err)
	}
	for _, actual := range image.Fields {
		if actual.Field != field {
			continue
		}
		var value string
		if json.Unmarshal([]byte(actual.Wire), &value) != nil || value != expected {
			t.Fatalf("field %s = %s, want %q", field, actual.Wire, expected)
		}
		return
	}
	t.Fatalf("field %s is absent", field)
}
