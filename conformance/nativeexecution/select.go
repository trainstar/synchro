// Package nativeexecution selects and executes authored native conformance plans.
package nativeexecution

import (
	"context"
	"errors"
	"fmt"

	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/modelrunner"
	"github.com/trainstar/synchro/conformance/scenarios"
	"github.com/trainstar/synchro/conformance/vectors"
)

// Selection binds one authored scenario to one native support-cell obligation.
type Selection struct {
	scenario                 scenarios.Scenario
	obligation               scenarios.ProofObligation
	supportCellID            string
	component                string
	platform                 string
	digest                   string
	performanceCatalogSHA256 string
	performanceBudgets       []contract.PerformanceBudget
	requiredMeasurements     []contract.RequiredMeasurement
	workloadExpansions       map[scenarios.StepID][]scenarios.Operation
}

// Select validates the complete authored corpus and selects one native obligation.
func Select(ctx context.Context, repoRoot, scenarioID, supportCellID string) (Selection, error) {
	if ctx == nil {
		return Selection{}, errors.New("native selection context is nil")
	}
	if err := ctx.Err(); err != nil {
		return Selection{}, fmt.Errorf("native selection canceled: %w", err)
	}
	if repoRoot == "" || scenarioID == "" || supportCellID == "" {
		return Selection{}, errors.New("native selection requires repository root, scenario ID, and support cell ID")
	}

	authored, err := scenarios.LoadAll(ctx, repoRoot)
	if err != nil {
		return Selection{}, fmt.Errorf("load native scenario corpus: %w", err)
	}
	catalog, err := scenarios.GenerateCatalogContext(ctx, repoRoot, authored)
	if err != nil {
		return Selection{}, fmt.Errorf("generate native scenario catalog: %w", err)
	}
	if err := scenarios.CheckCatalog(ctx, repoRoot, catalog); err != nil {
		return Selection{}, fmt.Errorf("check native scenario catalog: %w", err)
	}
	bundle, err := contract.Load(ctx, repoRoot)
	if err != nil {
		return Selection{}, fmt.Errorf("load native contract: %w", err)
	}
	performanceBinding, err := bundle.PerformanceCatalogBinding()
	if err != nil {
		return Selection{}, fmt.Errorf("bind native performance catalog: %w", err)
	}
	vectorCatalog, err := vectors.Load(ctx, repoRoot)
	if err != nil {
		return Selection{}, fmt.Errorf("load native vector catalog: %w", err)
	}
	if err := scenarios.ValidateAllWithVectors(authored, bundle, vectorCatalog); err != nil {
		return Selection{}, fmt.Errorf("validate native scenario corpus: %w", err)
	}

	var selectedScenario *scenarios.Scenario
	for index := range authored {
		if string(authored[index].ID) == scenarioID {
			if selectedScenario != nil {
				return Selection{}, fmt.Errorf("native scenario %s is duplicated", scenarioID)
			}
			selectedScenario = &authored[index]
		}
	}
	if selectedScenario == nil {
		return Selection{}, fmt.Errorf("native scenario %s is not authored", scenarioID)
	}

	var selectedCell *contract.SupportCell
	for index := range bundle.Support.Cells {
		if string(bundle.Support.Cells[index].ID) == supportCellID {
			if selectedCell != nil {
				return Selection{}, fmt.Errorf("native support cell %s is duplicated", supportCellID)
			}
			selectedCell = &bundle.Support.Cells[index]
		}
	}
	if selectedCell == nil {
		return Selection{}, fmt.Errorf("native support cell %s is not authored", supportCellID)
	}

	var selectedObligation *scenarios.ProofObligation
	for index := range selectedScenario.ProofObligations {
		obligation := &selectedScenario.ProofObligations[index]
		if obligation.ProofType != "native-e2e" || obligation.SupportCellID == nil || string(*obligation.SupportCellID) != supportCellID {
			continue
		}
		if selectedObligation != nil {
			return Selection{}, fmt.Errorf("native scenario %s has multiple obligations for support cell %s", scenarioID, supportCellID)
		}
		selectedObligation = obligation
	}
	if selectedObligation == nil {
		return Selection{}, fmt.Errorf("native scenario %s has no obligation for support cell %s", scenarioID, supportCellID)
	}
	if selectedScenario.NativeExecution == nil {
		return Selection{}, fmt.Errorf("native scenario %s has no execution plan", scenarioID)
	}
	digest := scenarios.SHA256(*selectedScenario)
	if digest == "" {
		return Selection{}, fmt.Errorf("native scenario %s lost its catalog binding", scenarioID)
	}
	budgets, measurements, err := resolvePerformanceDefinitions(performanceBinding, *selectedScenario, *selectedObligation, selectedCell.ID)
	if err != nil {
		return Selection{}, err
	}
	modelResult, err := modelrunner.RunScenario(ctx, *selectedScenario)
	if err != nil {
		return Selection{}, fmt.Errorf("resolve native workload operations: %w", err)
	}
	workloadExpansions := make(map[scenarios.StepID][]scenarios.Operation)
	for _, execution := range modelResult.Steps {
		if len(execution.Expanded) == 0 {
			continue
		}
		operations := make([]scenarios.Operation, len(execution.Expanded))
		for index, operation := range execution.Expanded {
			operations[index] = operation
			operations[index].Payload = append([]byte(nil), operation.Payload...)
		}
		workloadExpansions[execution.StepID] = operations
	}

	return Selection{
		scenario:                 *selectedScenario,
		obligation:               *selectedObligation,
		supportCellID:            supportCellID,
		component:                selectedCell.Component,
		platform:                 selectedCell.Platform,
		digest:                   digest,
		performanceCatalogSHA256: performanceBinding.SHA256(),
		performanceBudgets:       budgets,
		requiredMeasurements:     measurements,
		workloadExpansions:       workloadExpansions,
	}, nil
}

func resolvePerformanceDefinitions(binding contract.PerformanceCatalogBinding, scenario scenarios.Scenario, obligation scenarios.ProofObligation, supportCellID contract.SupportCellID) ([]contract.PerformanceBudget, []contract.RequiredMeasurement, error) {
	budgets := make([]contract.PerformanceBudget, 0, len(obligation.PerformanceBudgetIDs))
	seenBudgets := make(map[contract.BudgetID]struct{}, len(obligation.PerformanceBudgetIDs))
	for _, id := range obligation.PerformanceBudgetIDs {
		if _, duplicate := seenBudgets[id]; duplicate {
			return nil, nil, fmt.Errorf("native obligation %s duplicates performance budget %s", obligation.ObligationID, id)
		}
		budget, found := binding.Budget(id)
		if !found || budget.ScenarioID != scenario.ID || !containsSupportCell(budget.SupportCellIDs, supportCellID) {
			return nil, nil, fmt.Errorf("native obligation %s cannot bind performance budget %s", obligation.ObligationID, id)
		}
		seenBudgets[id] = struct{}{}
		budgets = append(budgets, budget)
	}

	measurements := make([]contract.RequiredMeasurement, 0, len(obligation.RequiredMeasurementIDs))
	seenMeasurements := make(map[contract.MeasurementID]struct{}, len(obligation.RequiredMeasurementIDs))
	for _, id := range obligation.RequiredMeasurementIDs {
		if _, duplicate := seenMeasurements[id]; duplicate {
			return nil, nil, fmt.Errorf("native obligation %s duplicates required measurement %s", obligation.ObligationID, id)
		}
		measurement, found := binding.RequiredMeasurement(id)
		if !found || measurement.ScenarioID != scenario.ID || !containsSupportCell(measurement.SupportCellIDs, supportCellID) {
			return nil, nil, fmt.Errorf("native obligation %s cannot bind required measurement %s", obligation.ObligationID, id)
		}
		seenMeasurements[id] = struct{}{}
		measurements = append(measurements, measurement)
	}
	return budgets, measurements, nil
}

func containsSupportCell(values []contract.SupportCellID, wanted contract.SupportCellID) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

// ScenarioID returns the selected authored scenario ID.
func (s Selection) ScenarioID() string { return string(s.scenario.ID) }

// ObligationID returns the selected native proof-obligation ID.
func (s Selection) ObligationID() string { return string(s.obligation.ObligationID) }

// SupportCellID returns the selected support-cell ID.
func (s Selection) SupportCellID() string { return s.supportCellID }

// MakeTarget returns the selected obligation's validated Make target.
func (s Selection) MakeTarget() string { return s.obligation.MakeTarget }

// PerformanceCatalogSHA256 returns the selected validated catalog binding.
func (s Selection) PerformanceCatalogSHA256() string { return s.performanceCatalogSHA256 }
