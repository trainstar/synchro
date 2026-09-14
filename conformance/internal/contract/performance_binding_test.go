package contract

import (
	"context"
	"testing"
)

func TestPerformanceCatalogBindingReturnsImmutableDefinitions(t *testing.T) {
	bundle, err := Load(context.Background(), repositoryRoot(t))
	if err != nil {
		t.Fatalf("load contract: %v", err)
	}
	binding, err := bundle.PerformanceCatalogBinding()
	if err != nil {
		t.Fatalf("bind performance catalog: %v", err)
	}
	if binding.SHA256() != lockedPerformanceDigest {
		t.Fatalf("binding digest = %q, want %q", binding.SHA256(), lockedPerformanceDigest)
	}

	budget, found := binding.Budget("BUD-WARM-CONNECT-001")
	if !found {
		t.Fatal("binding omitted authored budget")
	}
	budget.SupportCellIDs[0] = "SUP-MUTATED-001"
	budget.DataProfile.Parameters[0] = '['
	again, found := binding.Budget("BUD-WARM-CONNECT-001")
	if !found || again.SupportCellIDs[0] == "SUP-MUTATED-001" || again.DataProfile.Parameters[0] == '[' {
		t.Fatal("budget accessor returned shared mutable storage")
	}

	measurement, found := binding.RequiredMeasurement("MEAS-SCHEMA-CHECK-001")
	if !found {
		t.Fatal("binding omitted authored required measurement")
	}
	measurement.Metrics[0].ID = "MET-MUTATED-001"
	measurement.Strata[0].Parameters[0] = '['
	measurementAgain, found := binding.RequiredMeasurement("MEAS-SCHEMA-CHECK-001")
	if !found || measurementAgain.Metrics[0].ID == "MET-MUTATED-001" || measurementAgain.Strata[0].Parameters[0] == '[' {
		t.Fatal("measurement accessor returned shared mutable storage")
	}

	bundle.Performance.Budgets[0].Metric = "changed"
	boundBudget, found := binding.Budget("BUD-WARM-CONNECT-001")
	if !found || boundBudget.Metric != "warm_connect_http_requests" {
		t.Fatal("bundle mutation changed an existing performance binding")
	}
	if _, err := bundle.PerformanceCatalogBinding(); err == nil {
		t.Fatal("binding accessor accepted a mutated catalog")
	}
}
