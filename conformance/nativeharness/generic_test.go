package nativeharness

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/trainstar/synchro/conformance/nativeexecution"
	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestExecutorHandlesReorderedAndRenamedActions(t *testing.T) {
	controller := &recordingController{}
	executor, err := NewExecutor(Config{
		Controller: controller,
		Artifact:   &recordingArtifact{},
		Platform:   &recordingPlatform{},
	})
	if err != nil {
		t.Fatalf("create executor: %v", err)
	}

	requests := []nativeexecution.ExecuteRequest{
		{
			Action: nativeexecution.ExecutionAction{Actor: "controller", Command: "apply-step"},
			Steps: []nativeexecution.ExecutionStep{{
				ID:        "step-renamed-a",
				Operation: scenarios.Operation{ContractOperation: "workload", Name: "prepare", Payload: json.RawMessage(`{"profile":"scope_topology","scope_fanout":1,"impact_rows":1}`)},
			}},
		},
		{
			Action: nativeexecution.ExecutionAction{Actor: "controller", Command: "apply-step"},
			Steps: []nativeexecution.ExecutionStep{{
				ID:        "step-renamed-b",
				Operation: scenarios.Operation{ContractOperation: "model", Name: "activate-registry-membership-generation", Payload: json.RawMessage(`{"registry_generation":2}`)},
			}},
		},
	}
	for _, request := range requests {
		result, executeErr := executor.Execute(context.Background(), request)
		if executeErr != nil {
			t.Fatalf("execute renamed action: %v", executeErr)
		}
		if len(result.StepObservations) != 1 || result.StepObservations[0].StepID != request.Steps[0].ID {
			t.Fatalf("step observation did not close renamed step: %#v", result.StepObservations)
		}
	}
	if len(controller.operationKeys) != 2 || controller.operationKeys[0] != "workload/prepare" || controller.operationKeys[1] != "model/activate-registry-membership-generation" {
		t.Fatalf("operation dispatch = %#v", controller.operationKeys)
	}
}

func TestExecutorRoutesWorkloadByOperationKey(t *testing.T) {
	controller := &recordingController{}
	executor, err := NewExecutor(Config{Controller: controller, Artifact: &recordingArtifact{}, Platform: &recordingPlatform{}})
	if err != nil {
		t.Fatalf("create executor: %v", err)
	}
	_, err = executor.Execute(context.Background(), nativeexecution.ExecuteRequest{
		Action: nativeexecution.ExecutionAction{Actor: "controller", Command: "apply-step"},
		Steps:  []nativeexecution.ExecutionStep{{Operation: scenarios.Operation{ContractOperation: "workload", Name: "prepare"}}},
	})
	if err != nil {
		t.Fatalf("execute workload operation: %v", err)
	}
	if len(controller.operationKeys) != 1 || controller.operationKeys[0] != "workload/prepare" {
		t.Fatalf("workload operation key = %#v", controller.operationKeys)
	}
}

func TestSourceClosureIsStrict(t *testing.T) {
	if err := exactSourceClosure([]string{"application-rows"}, []CaptureSourceObservation{}); err == nil {
		t.Fatal("missing capture source passed closure")
	}
	if err := exactSourceClosure([]string{"application-rows"}, []CaptureSourceObservation{{Source: "application-rows"}, {Source: "application-rows"}}); err == nil {
		t.Fatal("duplicate capture source passed closure")
	}
	if err := exactSourceClosure([]string{"application-rows"}, []CaptureSourceObservation{{Source: "scope-state"}}); err == nil {
		t.Fatal("wrong capture source passed closure")
	}
	if err := exactSourceClosure([]string{"application-rows"}, []CaptureSourceObservation{{Source: "application-rows"}}); err != nil {
		t.Fatalf("exact capture source failed closure: %v", err)
	}
}

func TestStateFactMergeRejectsConflicts(t *testing.T) {
	left := scenarios.StateFacts{Rows: []scenarios.RowFact{{TableID: "table", CanonicalWireJSON: `"row"`, Version: "v1"}}}
	right := scenarios.StateFacts{Rows: []scenarios.RowFact{{TableID: "table", CanonicalWireJSON: `"row"`, Version: "v2"}}}
	if _, err := MergeStateFacts([]scenarios.StateFacts{left, right}); err == nil {
		t.Fatal("conflicting raw row facts passed merge")
	}
}

type recordingController struct {
	operationKeys []string
}

func (c *recordingController) Install(context.Context, InstallRequest) error { return nil }

func (c *recordingController) ApplyStep(_ context.Context, request StepRequest) (nativeexecution.StepObservation, error) {
	c.operationKeys = append(c.operationKeys, scenarios.OperationKey(request.Operation))
	return nativeexecution.StepObservation{Disposition: "success"}, nil
}

func (c *recordingController) RequestStep(context.Context, StepRequest) (nativeexecution.StepObservation, error) {
	return nativeexecution.StepObservation{Disposition: "success"}, nil
}

func (c *recordingController) ProcessStep(context.Context, StepRequest) (nativeexecution.StepObservation, error) {
	return nativeexecution.StepObservation{Disposition: "success"}, nil
}

func (c *recordingController) Capture(context.Context, CaptureRequest) ([]CaptureSourceObservation, error) {
	return nil, nil
}

func (c *recordingController) Close(context.Context) error { return nil }

type recordingArtifact struct{}

func (recordingArtifact) StageStep(context.Context, StepRequest) (nativeexecution.StepObservation, error) {
	return nativeexecution.StepObservation{Disposition: "success"}, nil
}

func (recordingArtifact) Capture(context.Context, CaptureRequest) ([]CaptureSourceObservation, error) {
	return nil, nil
}

func (recordingArtifact) Close(context.Context) error { return nil }

type recordingPlatform struct{}

func (recordingPlatform) Open(context.Context, OpenRequest) error { return nil }

func (recordingPlatform) LocalAction(context.Context, LocalActionRequest) (nativeexecution.StepObservation, error) {
	return nativeexecution.StepObservation{Disposition: "success"}, nil
}

func (recordingPlatform) Synchronize(_ context.Context, request SynchronizeRequest) (nativeexecution.SynchronizationResult, []nativeexecution.StepObservation, error) {
	observations := make([]nativeexecution.StepObservation, len(request.Steps))
	for index := range observations {
		observations[index].Disposition = "success"
	}
	return nativeexecution.SynchronizationResult{Completion: "idle"}, observations, nil
}

func (recordingPlatform) BeginCall(_ context.Context, request CallRequest) (nativeexecution.ClientCallResult, []nativeexecution.StepObservation, error) {
	observations := make([]nativeexecution.StepObservation, len(request.Steps))
	for index := range observations {
		observations[index].Disposition = "success"
	}
	return nativeexecution.ClientCallResult{}, observations, nil
}

func (recordingPlatform) AwaitCall(context.Context, CallRequest) (nativeexecution.ClientCallResult, error) {
	return nativeexecution.ClientCallResult{}, nil
}

func (recordingPlatform) Lifecycle(context.Context, LifecycleRequest) error { return nil }

func (recordingPlatform) AwaitStep(context.Context, AwaitRequest) (nativeexecution.StepObservation, error) {
	return nativeexecution.StepObservation{Disposition: "success"}, nil
}

func (recordingPlatform) ProcessStep(context.Context, StepRequest) (nativeexecution.StepObservation, error) {
	return nativeexecution.StepObservation{Disposition: "success"}, nil
}

func (recordingPlatform) ProcessBoundary(context.Context, ProcessBoundaryRequest) (nativeexecution.ProcessBoundaryResult, error) {
	return nativeexecution.ProcessBoundaryResult{}, nil
}

func (recordingPlatform) Capture(context.Context, CaptureRequest) ([]CaptureSourceObservation, error) {
	return nil, nil
}

func (recordingPlatform) MeasureBudgets(context.Context, BudgetRequest) ([]nativeexecution.BudgetObservation, error) {
	return nil, nil
}

func (recordingPlatform) MeasureSample(context.Context, SampleRequest) (nativeexecution.MeasurementSampleObservation, error) {
	return nativeexecution.MeasurementSampleObservation{}, nil
}

func (recordingPlatform) Close(context.Context) error { return nil }
