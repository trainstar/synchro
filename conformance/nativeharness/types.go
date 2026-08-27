// Package nativeharness provides the transport-neutral native execution boundary.
package nativeharness

import (
	"context"
	"encoding/json"

	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/nativeexecution"
	"github.com/trainstar/synchro/conformance/scenarios"
)

// Config supplies the three capability implementations used by Executor.
//
// The harness owns dispatch and observation normalization. The capabilities own
// platform, controller, and artifact behavior.
type Config struct {
	Controller Controller
	Artifact   Artifact
	Platform   Platform
}

// InstallRequest contains only the authored installation operation.
type InstallRequest struct {
	Operation scenarios.Operation
}

// StepRequest contains one operation after expected outcomes have been removed.
type StepRequest struct {
	ClientKey *string
	Phase     string
	Transport string
	Operation scenarios.Operation
}

// OpenRequest contains one client identity and generic open parameters.
type OpenRequest struct {
	Client         scenarios.NativeClient
	ClientKey      string
	DatabaseMode   string
	Initialization string
	SeedStepID     *scenarios.StepID
}

// LocalActionRequest contains one client-local operation.
type LocalActionRequest struct {
	ClientKey string
	Operation scenarios.Operation
}

// SynchronizeRequest contains one client synchronization operation.
type SynchronizeRequest struct {
	ClientKey string
	Method    string
	Steps     []StepRequest
}

// CallRequest contains one generic client call boundary.
type CallRequest struct {
	ClientKey string
	CallID    scenarios.NativeCallID
	Method    string
	Steps     []StepRequest
}

// LifecycleRequest contains one client lifecycle operation.
type LifecycleRequest struct {
	ClientKey string
	Operation string
}

// AwaitRequest contains one generic client await operation.
type AwaitRequest struct {
	ClientKey string
	CallID    *scenarios.NativeCallID
	Step      StepRequest
}

// ProcessBoundaryRequest contains one process boundary and its dispatch operation.
type ProcessBoundaryRequest struct {
	ClientKey     string
	Operation     string
	Boundary      string
	AfterActionID scenarios.NativeActionID
}

// CaptureRequest identifies source classes that a capability must capture.
type CaptureRequest struct {
	ClientKeys []string
	Sources    []string
}

// CaptureSourceObservation binds one raw fact set to one requested source.
type CaptureSourceObservation struct {
	Source     string
	StateFacts scenarios.StateFacts
}

// BudgetRequest supplies complete authored budgets for raw measurement.
type BudgetRequest struct {
	Budgets []nativeexecution.BudgetInstruction
}

// SampleRequest supplies one authored measurement and operation without expected values.
type SampleRequest struct {
	Measurement nativeexecution.MeasurementInstruction
	Stratum     contract.PerformanceStratum
	SampleID    string
	Parameters  json.RawMessage
	ClientKey   *string
	Operation   scenarios.Operation
}

// Controller owns server-side install, model, request, capture, and shutdown.
type Controller interface {
	Install(context.Context, InstallRequest) error
	ApplyStep(context.Context, StepRequest) (nativeexecution.StepObservation, error)
	RequestStep(context.Context, StepRequest) (nativeexecution.StepObservation, error)
	ProcessStep(context.Context, StepRequest) (nativeexecution.StepObservation, error)
	Capture(context.Context, CaptureRequest) ([]CaptureSourceObservation, error)
	Close(context.Context) error
}

// Artifact owns portable artifact staging and artifact capture.
type Artifact interface {
	StageStep(context.Context, StepRequest) (nativeexecution.StepObservation, error)
	Capture(context.Context, CaptureRequest) ([]CaptureSourceObservation, error)
	Close(context.Context) error
}

// Platform owns the native client and process boundaries.
type Platform interface {
	Open(context.Context, OpenRequest) error
	LocalAction(context.Context, LocalActionRequest) (nativeexecution.StepObservation, error)
	Synchronize(context.Context, SynchronizeRequest) (nativeexecution.SynchronizationResult, []nativeexecution.StepObservation, error)
	BeginCall(context.Context, CallRequest) (nativeexecution.ClientCallResult, []nativeexecution.StepObservation, error)
	AwaitCall(context.Context, CallRequest) (nativeexecution.ClientCallResult, error)
	Lifecycle(context.Context, LifecycleRequest) error
	AwaitStep(context.Context, AwaitRequest) (nativeexecution.StepObservation, error)
	ProcessStep(context.Context, StepRequest) (nativeexecution.StepObservation, error)
	ProcessBoundary(context.Context, ProcessBoundaryRequest) (nativeexecution.ProcessBoundaryResult, error)
	Capture(context.Context, CaptureRequest) ([]CaptureSourceObservation, error)
	MeasureBudgets(context.Context, BudgetRequest) ([]nativeexecution.BudgetObservation, error)
	MeasureSample(context.Context, SampleRequest) (nativeexecution.MeasurementSampleObservation, error)
	Close(context.Context) error
}

// CaptureSourceClass identifies the capability that owns a capture source.
type CaptureSourceClass string

const (
	CaptureSourceClassController CaptureSourceClass = "controller"
	CaptureSourceClassArtifact   CaptureSourceClass = "artifact"
	CaptureSourceClassPlatform   CaptureSourceClass = "platform"
)

// CaptureSourceClassFor returns the generic owner of a closed capture source.
func CaptureSourceClassFor(source string) (CaptureSourceClass, bool) {
	switch source {
	case "server-state":
		return CaptureSourceClassController, true
	case "artifact-state":
		return CaptureSourceClassArtifact, true
	case "application-rows", "pending-mutations", "rejected-mutations", "sync-status", "sync-events", "scope-state", "checkpoints", "provenance", "rebuild-state", "request-trace", "process-trace":
		return CaptureSourceClassPlatform, true
	default:
		return "", false
	}
}
