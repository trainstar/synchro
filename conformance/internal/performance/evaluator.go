// Package performance evaluates authored performance budgets from bounded raw
// observations. It does not accept an implementation-declared outcome.
package performance

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"

	"github.com/trainstar/synchro/conformance/execution"
	"github.com/trainstar/synchro/conformance/internal/contract"
)

// MaximumObservationMagnitude keeps numeric observations within the exact
// integer range shared by JSON runtimes.
const MaximumObservationMagnitude = float64(1<<53 - 1)

// BudgetEvaluation is the centrally derived result for one authored budget.
type BudgetEvaluation struct {
	Limit         float64
	ObservedValue float64
	Passed        bool
}

// EvaluateBudget derives the selected metric and applies the authored
// comparator and limit.
func EvaluateBudget(budget contract.PerformanceBudget, measurement execution.PerformanceMeasurement) (BudgetEvaluation, error) {
	if budget.ID == "" || budget.Metric == "" || budget.Comparator == "" || budget.Limit == "" {
		return BudgetEvaluation{}, errors.New("performance budget is incomplete")
	}
	if err := validateMeasurement(measurement); err != nil {
		return BudgetEvaluation{}, err
	}
	observed, err := observedValue(budget.Metric, measurement)
	if err != nil {
		return BudgetEvaluation{}, err
	}
	if !IsBoundedObservation(observed) {
		return BudgetEvaluation{}, errors.New("derived performance value is out of bounds")
	}
	limit, err := exactNumber(budget.Limit.String())
	if err != nil {
		return BudgetEvaluation{}, fmt.Errorf("performance limit is invalid: %w", err)
	}
	limitValue, err := strconv.ParseFloat(budget.Limit.String(), 64)
	if err != nil || !IsBoundedObservation(limitValue) {
		return BudgetEvaluation{}, errors.New("performance limit is out of bounds")
	}
	observedNumber, err := exactFloat(observed)
	if err != nil {
		return BudgetEvaluation{}, fmt.Errorf("derived performance value is invalid: %w", err)
	}
	passed, err := compare(observedNumber, limit, budget.Comparator)
	if err != nil {
		return BudgetEvaluation{}, err
	}
	return BudgetEvaluation{Limit: limitValue, ObservedValue: observed, Passed: passed}, nil
}

// IsFinite reports whether a floating-point value is suitable for JSON.
func IsFinite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

// IsBoundedObservation reports whether a value is finite and bounded for a
// portable native trace.
func IsBoundedObservation(value float64) bool {
	return IsFinite(value) && math.Abs(value) <= MaximumObservationMagnitude
}

func validateMeasurement(value execution.PerformanceMeasurement) error {
	counts := value.RequestCounts
	for _, counter := range []int{
		counts.Connect,
		counts.Push,
		counts.Pull,
		counts.RebuildPage,
		counts.SchemaFetch,
		counts.Other,
		value.ReturnedRebuildPageCount,
		value.OutboundNetworkOrRPCHops,
	} {
		if counter < 0 || float64(counter) > MaximumObservationMagnitude {
			return errors.New("performance measurement counter is out of bounds")
		}
	}
	return nil
}

func observedValue(metric string, measurement execution.PerformanceMeasurement) (float64, error) {
	counts := measurement.RequestCounts
	sum := func(values ...int) (float64, error) {
		var total int64
		for _, value := range values {
			total += int64(value)
			if float64(total) > MaximumObservationMagnitude {
				return 0, errors.New("derived performance counter is out of bounds")
			}
		}
		return float64(total), nil
	}
	switch metric {
	case "warm_connect_http_requests", "rebuild_connect_http_requests":
		return float64(counts.Connect), nil
	case "warm_connect_non_connect_http_requests":
		return sum(counts.Push, counts.Pull, counts.RebuildPage, counts.SchemaFetch, counts.Other)
	case "warm_connect_pull_http_requests":
		return float64(counts.Pull), nil
	case "warm_connect_push_http_requests":
		return float64(counts.Push), nil
	case "warm_connect_rebuild_page_http_requests":
		return float64(counts.RebuildPage), nil
	case "warm_connect_schema_fetch_http_requests":
		return float64(counts.SchemaFetch), nil
	case "warm_connect_other_http_requests":
		return float64(counts.Other), nil
	case "steady_state_pull_http_requests_per_cycle", "pending_cycle_pull_http_requests", "rebuild_pull_http_requests":
		return float64(counts.Pull), nil
	case "steady_state_pull_non_pull_http_requests_per_cycle":
		return sum(counts.Connect, counts.Push, counts.RebuildPage, counts.SchemaFetch, counts.Other)
	case "pending_cycle_push_http_requests":
		return float64(counts.Push), nil
	case "pending_cycle_non_push_or_pull_http_requests":
		return sum(counts.Connect, counts.RebuildPage, counts.SchemaFetch, counts.Other)
	case "rebuild_page_request_count_minus_returned_page_count":
		return float64(int64(counts.RebuildPage) - int64(measurement.ReturnedRebuildPageCount)), nil
	case "rebuild_schema_fetch_http_requests":
		return float64(counts.SchemaFetch), nil
	case "rebuild_unexpected_http_requests":
		return sum(counts.Push, counts.SchemaFetch, counts.Other)
	case "core_sync_outbound_network_or_rpc_hops":
		return float64(measurement.OutboundNetworkOrRPCHops), nil
	default:
		return 0, errors.New("performance metric is not supported")
	}
}

func exactNumber(value string) (*big.Rat, error) {
	parsed := new(big.Rat)
	if _, ok := parsed.SetString(value); !ok {
		return nil, errors.New("number is invalid")
	}
	return parsed, nil
}

func exactFloat(value float64) (*big.Rat, error) {
	if !IsFinite(value) {
		return nil, errors.New("number is not finite")
	}
	return exactNumber(strconv.FormatFloat(value, 'g', -1, 64))
}

func compare(left, right *big.Rat, comparator string) (bool, error) {
	switch comparator {
	case "eq":
		return left.Cmp(right) == 0, nil
	case "lte":
		return left.Cmp(right) <= 0, nil
	case "gte":
		return left.Cmp(right) >= 0, nil
	default:
		return false, errors.New("performance comparator is not supported")
	}
}
