package contract

import (
	"errors"
	"fmt"
)

// PerformanceCatalogBinding is an immutable view of the validated performance
// catalog and its canonical semantic digest.
type PerformanceCatalogBinding struct {
	sha256               string
	budgets              map[BudgetID]PerformanceBudget
	requiredMeasurements map[MeasurementID]RequiredMeasurement
}

// PerformanceCatalogBinding validates and snapshots the bundle performance
// catalog. Returned definitions never share mutable storage with the bundle.
func (b *Bundle) PerformanceCatalogBinding() (PerformanceCatalogBinding, error) {
	if b == nil {
		return PerformanceCatalogBinding{}, errors.New("contract bundle is nil")
	}
	if err := b.Validate(); err != nil {
		return PerformanceCatalogBinding{}, fmt.Errorf("validate performance catalog binding: %w", err)
	}
	digest, err := b.performanceCatalogDigest()
	if err != nil {
		return PerformanceCatalogBinding{}, fmt.Errorf("digest performance catalog binding: %w", err)
	}
	binding := PerformanceCatalogBinding{
		sha256:               digest,
		budgets:              make(map[BudgetID]PerformanceBudget, len(b.Performance.Budgets)),
		requiredMeasurements: make(map[MeasurementID]RequiredMeasurement, len(b.Performance.RequiredMeasurements)),
	}
	for _, budget := range b.Performance.Budgets {
		binding.budgets[budget.ID] = clonePerformanceBudget(budget)
	}
	for _, measurement := range b.Performance.RequiredMeasurements {
		binding.requiredMeasurements[measurement.ID] = cloneRequiredMeasurement(measurement)
	}
	return binding, nil
}

// SHA256 returns the canonical semantic SHA-256 for the validated catalog.
func (b PerformanceCatalogBinding) SHA256() string { return b.sha256 }

// Budget returns an immutable copy of one authored budget definition.
func (b PerformanceCatalogBinding) Budget(id BudgetID) (PerformanceBudget, bool) {
	budget, found := b.budgets[id]
	if !found {
		return PerformanceBudget{}, false
	}
	return clonePerformanceBudget(budget), true
}

// RequiredMeasurement returns an immutable copy of one authored measurement.
func (b PerformanceCatalogBinding) RequiredMeasurement(id MeasurementID) (RequiredMeasurement, bool) {
	measurement, found := b.requiredMeasurements[id]
	if !found {
		return RequiredMeasurement{}, false
	}
	return cloneRequiredMeasurement(measurement), true
}

func clonePerformanceBudget(source PerformanceBudget) PerformanceBudget {
	result := source
	result.SupportCellIDs = append([]SupportCellID(nil), source.SupportCellIDs...)
	result.ArtifactInventoryIDs = append([]ArtifactInventoryID(nil), source.ArtifactInventoryIDs...)
	result.DataProfile.Parameters = append([]byte(nil), source.DataProfile.Parameters...)
	return result
}

func cloneRequiredMeasurement(source RequiredMeasurement) RequiredMeasurement {
	result := source
	result.SupportCellIDs = append([]SupportCellID(nil), source.SupportCellIDs...)
	result.ArtifactInventoryIDs = append([]ArtifactInventoryID(nil), source.ArtifactInventoryIDs...)
	result.DataProfile.Parameters = append([]byte(nil), source.DataProfile.Parameters...)
	result.Metrics = append([]PerformanceMetric(nil), source.Metrics...)
	result.Strata = make([]PerformanceStratum, len(source.Strata))
	for index, stratum := range source.Strata {
		result.Strata[index] = stratum
		result.Strata[index].Parameters = append([]byte(nil), stratum.Parameters...)
	}
	return result
}
