package reference

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/trainstar/synchro/conformance/scenarios"
)

const supportedProtocolVersion = 3

// ErrUnregisteredOperation reports an operation without Task 6 semantics.
var ErrUnregisteredOperation = errors.New("unregistered operation")

// ErrInvalidInitialState reports state that cannot enter the model.
var ErrInvalidInitialState = errors.New("invalid initial state")

// Config contains the deterministic inputs for one model.
// State must be token-free because setup operations mint all model tokens.
type Config struct {
	State State
	Clock Clock
	Seed  int64
}

// Model owns isolated deterministic reference state.
type Model struct {
	gate                        chan struct{}
	state                       State
	clock                       Clock
	seed                        int64
	authority                   TokenAuthority
	pullHydrationFault          *PullHydrationFault
	pullHydrationFaultTriggered bool
}

// PullHydrationFault omits one selected captured projection without changing
// durable model state.
type PullHydrationFault struct {
	Projection ProjectionKey
}

// New creates a protocol 3 reference model from isolated input state.
func New(cfg Config) (*Model, error) {
	if cfg.State.ProtocolVersion != supportedProtocolVersion {
		return nil, fmt.Errorf("create model: %w: protocol version %d is not supported", ErrInvalidInitialState, cfg.State.ProtocolVersion)
	}
	if isNilValue(reflect.ValueOf(cfg.Clock)) {
		return nil, errors.New("create model: clock is required")
	}
	if err := validateInitialState(cfg.State); err != nil {
		return nil, fmt.Errorf("create model: %w: %v", ErrInvalidInitialState, err)
	}

	return &Model{
		gate:      newModelGate(),
		state:     cloneState(cfg.State),
		clock:     cfg.Clock,
		seed:      cfg.Seed,
		authority: newTokenAuthority(cfg.Seed),
	}, nil
}

// Snapshot returns an isolated deterministic observation of model state.
func (m *Model) Snapshot() StateSnapshot {
	<-m.gate
	defer m.releaseGate()

	return snapshotState(m.state)
}

// Apply dispatches one registered operation without runner-resolved input.
func (m *Model) Apply(ctx context.Context, op scenarios.Operation) (StepResult, error) {
	return m.ApplyResolved(ctx, op, ResolvedOperationInput{})
}

// ApplyResolved dispatches one registered operation with cloned runner-resolved input.
func (m *Model) ApplyResolved(ctx context.Context, op scenarios.Operation, input ResolvedOperationInput) (StepResult, error) {
	if err := m.acquireApplyGate(ctx, op); err != nil {
		return StepResult{}, err
	}
	defer m.releaseGate()

	handler, registered := operationHandlers[op.ContractOperation+"/"+op.Name]
	if !registered {
		return StepResult{}, fmt.Errorf("apply operation %q/%q: %w", op.ContractOperation, op.Name, ErrUnregisteredOperation)
	}
	if err := validateResolvedOperationInput(scenarios.OperationKey(op), input); err != nil {
		return StepResult{}, fmt.Errorf("apply operation %q/%q: %w", op.ContractOperation, op.Name, err)
	}
	return m.executeHandler(ctx, op, handler, input)
}

// ApplyResolvedWithPullHydrationFault dispatches one pull with a transient,
// exact projection omission. ResolvedOperationInput retains its normal rules.
func (m *Model) ApplyResolvedWithPullHydrationFault(ctx context.Context, op scenarios.Operation, input ResolvedOperationInput, fault PullHydrationFault) (StepResult, error) {
	if err := m.acquireApplyGate(ctx, op); err != nil {
		return StepResult{}, err
	}
	defer m.releaseGate()
	key := scenarios.OperationKey(op)
	if key != "pull/request-page" || fault.Projection.Relation == "" {
		return StepResult{}, errors.New("pull hydration fault requires one exact pull projection")
	}
	handler, registered := operationHandlers[key]
	if !registered {
		return StepResult{}, fmt.Errorf("apply operation %q/%q: %w", op.ContractOperation, op.Name, ErrUnregisteredOperation)
	}
	if err := validateResolvedOperationInput(key, input); err != nil {
		return StepResult{}, fmt.Errorf("apply operation %q/%q: %w", op.ContractOperation, op.Name, err)
	}
	return m.executeHandlerWithPullHydrationFault(ctx, op, handler, input, &fault)
}

func (m *Model) applyHandler(ctx context.Context, op scenarios.Operation, handler operationImplementation) (StepResult, error) {
	if err := m.acquireApplyGate(ctx, op); err != nil {
		return StepResult{}, err
	}
	defer m.releaseGate()
	if handler == nil {
		return StepResult{}, fmt.Errorf("apply operation %q/%q: handler is required", op.ContractOperation, op.Name)
	}
	return m.executeHandler(ctx, op, withoutResolvedInput(handler), ResolvedOperationInput{})
}

func (m *Model) applyResolvedHandler(ctx context.Context, op scenarios.Operation, input ResolvedOperationInput, handler OperationHandler) (StepResult, error) {
	if err := m.acquireApplyGate(ctx, op); err != nil {
		return StepResult{}, err
	}
	defer m.releaseGate()
	if handler == nil {
		return StepResult{}, fmt.Errorf("apply operation %q/%q: handler is required", op.ContractOperation, op.Name)
	}
	return m.executeHandler(ctx, op, handler, input)
}

func (m *Model) acquireApplyGate(ctx context.Context, op scenarios.Operation) error {
	if isNilValue(reflect.ValueOf(ctx)) {
		return errors.New("apply operation: context is required")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("apply operation %q/%q: %w", op.ContractOperation, op.Name, err)
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("apply operation %q/%q: %w", op.ContractOperation, op.Name, ctx.Err())
	case <-m.gate:
	}
	if err := ctx.Err(); err != nil {
		m.releaseGate()
		return fmt.Errorf("apply operation %q/%q: %w", op.ContractOperation, op.Name, err)
	}
	return nil
}

func (m *Model) executeHandler(ctx context.Context, op scenarios.Operation, handler OperationHandler, input ResolvedOperationInput) (StepResult, error) {
	return m.executeHandlerWithPullHydrationFault(ctx, op, handler, input, nil)
}

func (m *Model) executeHandlerWithPullHydrationFault(ctx context.Context, op scenarios.Operation, handler OperationHandler, input ResolvedOperationInput, fault *PullHydrationFault) (StepResult, error) {
	workingAuthority, err := cloneTokenAuthority(m.authority)
	if err != nil {
		return StepResult{}, fmt.Errorf("apply operation %q/%q: %w", op.ContractOperation, op.Name, err)
	}
	working := &Model{
		gate:      newModelGate(),
		state:     cloneState(m.state),
		clock:     m.clock,
		seed:      m.seed,
		authority: workingAuthority,
	}
	if fault != nil {
		copy := *fault
		working.pullHydrationFault = &copy
	}
	if err := ctx.Err(); err != nil {
		return StepResult{}, fmt.Errorf("apply operation %q/%q: %w", op.ContractOperation, op.Name, err)
	}

	result, err := handler(ctx, working, op.Payload, cloneResolvedOperationInput(input))
	if err != nil {
		return StepResult{}, fmt.Errorf("apply operation %q/%q: %w", op.ContractOperation, op.Name, err)
	}
	if err := validateStepResult(result); err != nil {
		return StepResult{}, fmt.Errorf("apply operation %q/%q: invalid result: %w", op.ContractOperation, op.Name, err)
	}
	if fault != nil && !working.pullHydrationFaultTriggered {
		return StepResult{}, fmt.Errorf("apply operation %q/%q: pull hydration fault target was not selected", op.ContractOperation, op.Name)
	}

	committedAuthority, err := cloneTokenAuthority(working.authority)
	if err != nil {
		return StepResult{}, fmt.Errorf("apply operation %q/%q: %w", op.ContractOperation, op.Name, err)
	}
	committedState := cloneState(working.state)
	if err := ctx.Err(); err != nil {
		return StepResult{}, fmt.Errorf("apply operation %q/%q: %w", op.ContractOperation, op.Name, err)
	}

	m.state = committedState
	m.authority = committedAuthority
	return result, nil
}

func validateResolvedOperationInput(key string, input ResolvedOperationInput) error {
	if input.SourceStep == nil && input.PortableSeed == nil {
		return nil
	}
	switch key {
	case "local/apply-pull-page", "artifact/install-portable-seed":
		return nil
	default:
		return errors.New("resolved operation input is not permitted")
	}
}

func newModelGate() chan struct{} {
	gate := make(chan struct{}, 1)
	gate <- struct{}{}
	return gate
}

func (m *Model) releaseGate() {
	m.gate <- struct{}{}
}

func validateInitialState(state State) error {
	if err := validateAuthoritativeRows(state.Rows); err != nil {
		return err
	}
	if err := validateRelationRegistrations(state); err != nil {
		return err
	}
	return validateTokenFreeState(state)
}

func validateAuthoritativeRows(rows map[RowIdentity]AuthoritativeRow) error {
	canonicalIdentities := make(map[string]RowIdentity, len(rows))
	for key, row := range rows {
		if key.CanonicalIdentityBytes == "" || row.Identity.CanonicalIdentityBytes == "" {
			return errors.New("authoritative row has empty canonical identity bytes")
		}
		if key != row.Identity {
			return errors.New("authoritative row map key differs from row identity")
		}
		if existing, found := canonicalIdentities[key.CanonicalIdentityBytes]; found && existing != key {
			return errors.New("authoritative rows share canonical identity bytes")
		}
		canonicalIdentities[key.CanonicalIdentityBytes] = key
	}
	return nil
}

func validateRelationRegistrations(state State) error {
	for key, relation := range state.Relations {
		if key != relation.Definition.Relation {
			return errors.New("relation map key differs from definition relation")
		}
		if err := validateRelationDefinition(relation.Definition); err != nil {
			return err
		}
	}
	for _, generation := range state.Registry.Generations {
		for _, relation := range generation.Relations {
			if err := validateRelationDefinition(relation.Definition); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateRelationDefinition(definition RelationDefinition) error {
	switch definition.RegistrationKind {
	case RegistrationKindSynced:
		if !definition.HasTableID || definition.TableID == "" {
			return errors.New("synced relation requires a table ID")
		}
	case RegistrationKindCaptureDependency:
		if definition.HasTableID || definition.TableID != "" {
			return errors.New("capture dependency relation cannot have a table ID")
		}
	default:
		return errors.New("relation has unknown registration kind")
	}
	return nil
}

func validateTokenFreeState(state State) error {
	for _, client := range state.Clients {
		for _, checkpoint := range client.Checkpoints {
			if configuredToken(checkpoint.HasCursor, checkpoint.Cursor) {
				return errors.New("server client checkpoint contains a configured token")
			}
		}
	}
	for _, rebuild := range state.Rebuilds {
		if configuredToken(rebuild.HasContinuation, rebuild.Continuation) {
			return errors.New("rebuild continuation contains a configured token")
		}
		for _, page := range rebuild.Pages {
			if configuredToken(page.HasToken, page.Token) {
				return errors.New("rebuild page contains a configured token")
			}
		}
	}
	for _, local := range state.ClientLocal {
		for _, checkpoint := range local.ScopeCheckpoints {
			if configuredToken(checkpoint.HasCursor, checkpoint.Cursor) {
				return errors.New("local scope checkpoint contains a configured token")
			}
		}
		for _, receipt := range local.SeedReceipts {
			if configuredToken(receipt.HasReceipt, receipt.Receipt) {
				return errors.New("local seed receipt contains a configured token")
			}
		}
		for _, attempt := range local.RebuildAttempts {
			if configuredToken(attempt.HasContinuation, attempt.Continuation) {
				return errors.New("local rebuild continuation contains a configured token")
			}
			for _, page := range attempt.AppliedPages {
				if configuredToken(page.HasRequestToken, page.RequestToken) {
					return errors.New("local applied rebuild page contains a configured token")
				}
			}
			pending := attempt.PendingFinalResult
			if configuredToken(pending.HasFinalCursor, pending.FinalCursor) {
				return errors.New("local pending rebuild result contains a configured token")
			}
		}
	}
	for _, export := range state.Seed.Exports {
		for _, scope := range export.Scopes {
			if configuredToken(scope.HasReceipt, scope.Receipt) {
				return errors.New("export scope receipt contains a configured token")
			}
		}
		for _, page := range export.Pages {
			if configuredToken(page.HasToken, page.Token) {
				return errors.New("export page contains a configured token")
			}
		}
	}
	return nil
}

func configuredToken(present bool, token OpaqueToken) bool {
	return present || token != (OpaqueToken{})
}

func isNilValue(value reflect.Value) bool {
	if !value.IsValid() {
		return true
	}

	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
