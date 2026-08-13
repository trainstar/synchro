package reference

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestPullHydrationFaultIsTransientAndAtomic(t *testing.T) {
	workingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	repoRoot := filepath.Clean(filepath.Join(workingDirectory, "../.."))
	scenario, err := scenarios.LoadFile(context.Background(), repoRoot, "conformance/scenarios/server/pull-hydration-failure-001.json")
	if err != nil {
		t.Fatalf("load hydration scenario: %v", err)
	}
	model, err := New(Config{State: State{ProtocolVersion: 3}, Clock: &modelClock{}, Seed: 813})
	if err != nil {
		t.Fatalf("create hydration model: %v", err)
	}
	if _, err := model.Apply(context.Background(), scenario.Model.Setup[0]); err != nil {
		t.Fatalf("apply hydration setup: %v", err)
	}
	for _, step := range scenario.Steps[:len(scenario.Steps)-1] {
		if _, err := model.Apply(context.Background(), step.Operation); err != nil {
			t.Fatalf("apply hydration step %s: %v", step.ID, err)
		}
	}

	before := model.Snapshot()
	projection, found := onlyHydrationProjection(before, "scope-a")
	if !found {
		t.Fatal("hydration scenario did not produce one captured projection")
	}
	pull := scenario.Steps[len(scenario.Steps)-1].Operation
	result, err := model.ApplyResolvedWithPullHydrationFault(context.Background(), pull, ResolvedOperationInput{}, PullHydrationFault{Projection: projection})
	if err != nil {
		t.Fatalf("apply hydration fault: %v", err)
	}
	if result.HTTP == nil || result.HTTP.Status != 500 || !result.HTTP.HasCode || result.HTTP.Code != "sync_integrity_failure" || result.HTTP.Retryable {
		t.Fatalf("hydration fault result = %#v", result)
	}
	if after := model.Snapshot(); !reflect.DeepEqual(after, before) {
		t.Fatal("hydration fault changed durable model state")
	}

	normal, err := model.Apply(context.Background(), pull)
	if err != nil {
		t.Fatalf("retry pull without hydration fault: %v", err)
	}
	if normal.HTTP == nil || normal.HTTP.Status != 200 || normal.Pull == nil || len(normal.Pull.Changes) != 1 {
		t.Fatalf("normal retry result = %#v", normal)
	}
}

func TestPullHydrationFaultRejectsAnUnselectedProjection(t *testing.T) {
	model, err := New(Config{State: State{ProtocolVersion: 3}, Clock: &modelClock{}, Seed: 814})
	if err != nil {
		t.Fatal(err)
	}
	operation := scenarios.Operation{ContractOperation: "pull", Name: "request-page", Payload: []byte(`{"user_id":"user-a","client_id":"client-a","client_generation":1,"schema":{"version":1,"hash":"0000000000000000000000000000000000000000000000000000000000000000"},"scope_set_version":1,"scopes":[{"scope_id":"scope-a","cursor_source":"none"}],"limit":1}`)}
	before := model.Snapshot()
	_, err = model.ApplyResolvedWithPullHydrationFault(context.Background(), operation, ResolvedOperationInput{}, PullHydrationFault{Projection: ProjectionKey{Relation: "public.items"}})
	if err == nil || !strings.Contains(err.Error(), "target was not selected") {
		t.Fatalf("unselected hydration fault error = %v", err)
	}
	if !reflect.DeepEqual(model.Snapshot(), before) {
		t.Fatal("rejected hydration fault changed durable model state")
	}
}

func onlyHydrationProjection(snapshot StateSnapshot, scope ScopeID) (ProjectionKey, bool) {
	var selected ProjectionKey
	found := false
	for _, entry := range snapshot.Scopes {
		if entry.Key != scope {
			continue
		}
		for _, effect := range entry.Value.Effects {
			if !effect.HasCapturedProjection || found {
				return ProjectionKey{}, false
			}
			selected = effect.CapturedProjection
			found = true
		}
	}
	return selected, found
}
