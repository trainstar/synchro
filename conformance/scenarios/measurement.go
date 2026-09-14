package scenarios

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
)

// DecodeSchemaDispatchMeasurementPlan decodes the closed payload for the
// schema-dispatch semantic and measurement predicates.
func DecodeSchemaDispatchMeasurementPlan(payload json.RawMessage) (SchemaDispatchMeasurementPlan, error) {
	var object map[string]json.RawMessage
	if err := jsonstrict.Decode(payload, &object); err != nil {
		return SchemaDispatchMeasurementPlan{}, fmt.Errorf("decode schema-dispatch predicate payload: %w", err)
	}
	if len(object) != 3 {
		return SchemaDispatchMeasurementPlan{}, errors.New("schema-dispatch predicate payload has an unknown or missing member")
	}
	for _, name := range []string{"measurement_id", "minimum_sample_count_per_stratum", "strata"} {
		if _, found := object[name]; !found {
			return SchemaDispatchMeasurementPlan{}, fmt.Errorf("schema-dispatch predicate payload member %q is required", name)
		}
	}

	var plan SchemaDispatchMeasurementPlan
	if err := json.Unmarshal(payload, &plan); err != nil {
		return SchemaDispatchMeasurementPlan{}, fmt.Errorf("decode schema-dispatch predicate payload: %w", err)
	}
	if plan.MeasurementID == "" || plan.MinimumSampleCountPerStratum == 0 || len(plan.Strata) == 0 {
		return SchemaDispatchMeasurementPlan{}, errors.New("schema-dispatch predicate payload is incomplete")
	}
	seenStrata := make(map[string]struct{}, len(plan.Strata))
	seenCases := make(map[string]struct{}, len(plan.Strata))
	for _, stratum := range plan.Strata {
		if stratum.StratumID == "" || stratum.SchemaCase == "" {
			return SchemaDispatchMeasurementPlan{}, errors.New("schema-dispatch predicate payload has an incomplete stratum")
		}
		if _, duplicate := seenStrata[string(stratum.StratumID)]; duplicate {
			return SchemaDispatchMeasurementPlan{}, errors.New("schema-dispatch predicate payload has a duplicate stratum")
		}
		if _, duplicate := seenCases[stratum.SchemaCase]; duplicate {
			return SchemaDispatchMeasurementPlan{}, errors.New("schema-dispatch predicate payload has a duplicate schema case")
		}
		seenStrata[string(stratum.StratumID)] = struct{}{}
		seenCases[stratum.SchemaCase] = struct{}{}
	}
	return plan, nil
}
