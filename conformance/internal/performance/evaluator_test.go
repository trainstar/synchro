package performance

import (
	"encoding/json"
	"math"
	"strconv"
	"testing"

	"github.com/trainstar/synchro/conformance/execution"
	"github.com/trainstar/synchro/conformance/internal/contract"
)

func TestEvaluateBudgetDerivesEveryAuthoredMetricFamily(t *testing.T) {
	measurement := execution.PerformanceMeasurement{
		RequestCounts: execution.RequestCounts{
			Connect: 2, Push: 3, Pull: 5, RebuildPage: 7, SchemaFetch: 11, Other: 13,
		},
		ReturnedRebuildPageCount: 6,
		OutboundNetworkOrRPCHops: 17,
	}
	for _, test := range []struct {
		metric string
		want   float64
	}{
		{"warm_connect_http_requests", 2},
		{"rebuild_connect_http_requests", 2},
		{"warm_connect_non_connect_http_requests", 39},
		{"warm_connect_pull_http_requests", 5},
		{"warm_connect_push_http_requests", 3},
		{"warm_connect_rebuild_page_http_requests", 7},
		{"warm_connect_schema_fetch_http_requests", 11},
		{"warm_connect_other_http_requests", 13},
		{"steady_state_pull_http_requests_per_cycle", 5},
		{"pending_cycle_pull_http_requests", 5},
		{"rebuild_pull_http_requests", 5},
		{"steady_state_pull_non_pull_http_requests_per_cycle", 36},
		{"pending_cycle_push_http_requests", 3},
		{"pending_cycle_non_push_or_pull_http_requests", 33},
		{"rebuild_page_request_count_minus_returned_page_count", 1},
		{"rebuild_schema_fetch_http_requests", 11},
		{"rebuild_unexpected_http_requests", 27},
		{"core_sync_outbound_network_or_rpc_hops", 17},
	} {
		t.Run(test.metric, func(t *testing.T) {
			budget := testBudget(test.metric, "eq", json.Number(formatInteger(test.want)))
			result, err := EvaluateBudget(budget, measurement)
			if err != nil {
				t.Fatalf("evaluate budget: %v", err)
			}
			if result.ObservedValue != test.want || !result.Passed {
				t.Fatalf("evaluation = %+v, want observed %v and passed", result, test.want)
			}
		})
	}
}

func TestEvaluateBudgetAppliesAuthoredComparators(t *testing.T) {
	measurement := execution.PerformanceMeasurement{RequestCounts: execution.RequestCounts{Connect: 2}}
	for _, test := range []struct {
		name       string
		comparator string
		limit      string
		passed     bool
	}{
		{"equal passes", "eq", "2", true},
		{"equal fails", "eq", "1", false},
		{"less than or equal passes", "lte", "3", true},
		{"less than or equal fails", "lte", "1", false},
		{"greater than or equal passes", "gte", "1", true},
		{"greater than or equal fails", "gte", "3", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := EvaluateBudget(testBudget("warm_connect_http_requests", test.comparator, json.Number(test.limit)), measurement)
			if err != nil {
				t.Fatalf("evaluate budget: %v", err)
			}
			if result.Passed != test.passed {
				t.Fatalf("passed = %v, want %v", result.Passed, test.passed)
			}
		})
	}
}

func TestEvaluateBudgetRejectsInvalidDefinitionsAndObservations(t *testing.T) {
	valid := execution.PerformanceMeasurement{RequestCounts: execution.RequestCounts{Connect: 1}}
	for _, test := range []struct {
		name        string
		budget      contract.PerformanceBudget
		measurement execution.PerformanceMeasurement
	}{
		{"unknown metric", testBudget("unknown", "eq", "1"), valid},
		{"unknown comparator", testBudget("warm_connect_http_requests", "unknown", "1"), valid},
		{"invalid limit", testBudget("warm_connect_http_requests", "eq", "NaN"), valid},
		{"negative counter", testBudget("warm_connect_http_requests", "eq", "1"), execution.PerformanceMeasurement{RequestCounts: execution.RequestCounts{Connect: -1}}},
		{"oversized counter", testBudget("warm_connect_http_requests", "eq", "1"), execution.PerformanceMeasurement{RequestCounts: execution.RequestCounts{Connect: int(MaximumObservationMagnitude) + 1}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := EvaluateBudget(test.budget, test.measurement); err == nil {
				t.Fatal("invalid budget evaluation was accepted")
			}
		})
	}
	for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), MaximumObservationMagnitude + 1} {
		if IsBoundedObservation(value) {
			t.Fatalf("invalid observation %v was accepted", value)
		}
	}
}

func testBudget(metric, comparator string, limit json.Number) contract.PerformanceBudget {
	return contract.PerformanceBudget{
		ID:         "BUD-TEST-001",
		Metric:     metric,
		Comparator: comparator,
		Limit:      limit,
	}
}

func formatInteger(value float64) string {
	return strconv.FormatFloat(value, 'f', 0, 64)
}
