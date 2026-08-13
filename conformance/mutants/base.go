package mutants

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/trainstar/synchro/conformance/reference"
	"github.com/trainstar/synchro/conformance/scenarios"
)

type deterministicClock struct {
	now time.Time
}

func (c deterministicClock) Now() time.Time {
	return c.now
}

// Base is the unmodified deterministic reference-model subject.
type Base struct {
	model       *reference.Model
	currentStep string
	rawResults  map[string]reference.StepResult
}

// NewBase creates an isolated subject with a deterministic clock and token seed.
// The optional seed exists only to make independent test subjects reproducible.
func NewBase(seed ...int64) (*Base, error) {
	if len(seed) > 1 {
		return nil, errors.New("base accepts at most one seed")
	}
	value := int64(1)
	if len(seed) == 1 {
		value = seed[0]
	}
	if value == 0 {
		value = 1
	}
	model, err := reference.New(reference.Config{
		State: reference.State{ProtocolVersion: 3},
		Clock: deterministicClock{now: time.Unix(0, 0).UTC()},
		Seed:  value,
	})
	if err != nil {
		return nil, fmt.Errorf("create base subject: %w", err)
	}
	return &Base{model: model, rawResults: make(map[string]reference.StepResult)}, nil
}

// Subject returns the unmodified operation subject.
func (b *Base) Subject() Subject {
	return b
}

// Descriptor identifies the unmodified base.
func (b *Base) Descriptor() Descriptor {
	return Descriptor{Name: "base", Kind: MutationKindBase}
}

// MutationApplied is false for the unmodified base.
func (b *Base) MutationApplied() bool {
	return false
}

// SetStepID supplies the closed scenario step identity before execution.
func (b *Base) SetStepID(stepID string) {
	b.currentStep = stepID
}

// RawResult returns the unmodified result recorded for one scenario step.
func (b *Base) RawResult(stepID string) (reference.StepResult, bool) {
	result, found := b.rawResults[stepID]
	if !found {
		return reference.StepResult{}, false
	}
	return cloneStepResult(result), true
}

// Snapshot returns the current isolated reference-model state.
func (b *Base) Snapshot() reference.StateSnapshot {
	if b == nil || b.model == nil {
		return reference.StateSnapshot{}
	}
	return b.model.Snapshot()
}

// Execute applies one closed operation to the reference model.
func (b *Base) Execute(ctx context.Context, op scenarios.Operation) (reference.StepResult, error) {
	if b == nil || b.model == nil {
		return reference.StepResult{}, errors.New("base subject is not initialized")
	}
	if ctx == nil {
		return reference.StepResult{}, errors.New("base operation context is required")
	}
	if err := ctx.Err(); err != nil {
		return reference.StepResult{}, err
	}
	if err := scenarios.ValidateOperation(op); err != nil {
		return reference.StepResult{}, fmt.Errorf("validate closed operation %s: %w", operationKey(op), err)
	}

	input, err := b.resolvedInput(op)
	if err != nil {
		return reference.StepResult{}, err
	}
	result, err := b.model.ApplyResolved(ctx, cloneOperation(op), input)
	if err != nil {
		return reference.StepResult{}, err
	}
	if b.currentStep != "" {
		b.rawResults[b.currentStep] = cloneStepResult(result)
	}
	return cloneStepResult(result), nil
}

func (b *Base) resolvedInput(op scenarios.Operation) (reference.ResolvedOperationInput, error) {
	if operationKey(op) != "local/apply-pull-page" {
		return reference.ResolvedOperationInput{}, nil
	}
	var payload struct {
		SourceStepID string `json:"source_step_id"`
	}
	if err := json.Unmarshal(op.Payload, &payload); err != nil {
		return reference.ResolvedOperationInput{}, fmt.Errorf("decode local/apply-pull-page source: %w", err)
	}
	if payload.SourceStepID == "" {
		return reference.ResolvedOperationInput{}, errors.New("local/apply-pull-page source step is required")
	}
	source, found := b.rawResults[payload.SourceStepID]
	if !found {
		return reference.ResolvedOperationInput{}, fmt.Errorf("local/apply-pull-page source step %q was not executed", payload.SourceStepID)
	}
	return reference.ResolvedOperationInput{SourceStep: &reference.ResolvedStep{
		StepID:       payload.SourceStepID,
		OperationKey: "pull/request-page",
		Result:       cloneStepResult(source),
	}}, nil
}
