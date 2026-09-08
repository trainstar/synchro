package soak

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/trainstar/synchro/conformance/faults"
	"github.com/trainstar/synchro/conformance/invariants"
	"github.com/trainstar/synchro/conformance/scenarios"
	"github.com/trainstar/synchro/conformance/vectors"
)

var (
	// ErrHarnessRequired reports that a run has no execution boundary.
	ErrHarnessRequired = errors.New("soak harness is required")
	// ErrZeroOperations reports a plan that executed no workload operations.
	ErrZeroOperations = errors.New("soak executed zero operations")
	// ErrInvariantViolation reports a checker-detected violation.
	ErrInvariantViolation = errors.New("soak invariant violation")
	// ErrCheckerCoverage reports a completed run that did not exercise every applicable checker family.
	ErrCheckerCoverage = errors.New("soak checker coverage is incomplete")
)

// ObservationCapture contains the neutral surfaces returned by one harness step.
// The harness assembles these values from its existing black-box captures.
type ObservationCapture struct {
	Manifest               *vectors.Manifest
	ServerState            *scenarios.StateFacts
	Operator               *invariants.OperatorObservation
	Clients                []invariants.ClientObservation
	WireExchanges          []invariants.WireExchangeObservation
	CursorPositions        []invariants.CursorPositionObservation
	PullResults            []invariants.PullResultObservation
	CursorAcknowledgements []invariants.CursorAcknowledgementObservation
	ServerRowIdentities    []invariants.ServerRowIdentityObservation
	FaultActivation        *FaultActivationObservation
}

// AssembleObservation assigns the runner sequence to one harness capture.
func AssembleObservation(sequence uint64, capture ObservationCapture) invariants.Observation {
	return invariants.Observation{
		Sequence:               sequence,
		Manifest:               capture.Manifest,
		ServerState:            capture.ServerState,
		Operator:               capture.Operator,
		Clients:                append([]invariants.ClientObservation(nil), capture.Clients...),
		WireExchanges:          append([]invariants.WireExchangeObservation(nil), capture.WireExchanges...),
		CursorPositions:        append([]invariants.CursorPositionObservation(nil), capture.CursorPositions...),
		PullResults:            append([]invariants.PullResultObservation(nil), capture.PullResults...),
		CursorAcknowledgements: append([]invariants.CursorAcknowledgementObservation(nil), capture.CursorAcknowledgements...),
		ServerRowIdentities:    append([]invariants.ServerRowIdentityObservation(nil), capture.ServerRowIdentities...),
	}
}

// Harness is the narrow execution boundary required by the soak runner.
//
// An implementation maps one neutral operation to black-box controller calls,
// then returns the captured server, operator, client, and wire surfaces.
type Harness interface {
	Execute(context.Context, Operation) (ObservationCapture, error)
}

// RunResult contains bounded execution results and every assembled observation.
type RunResult struct {
	Seed               uint64
	OperationsExecuted int
	Observations       []invariants.Observation
	Violations         []invariants.Violation
}

// Run executes a plan, journals each operation and observation sequence, and
// runs all five exported invariant checkers after every assembled observation.
func Run(ctx context.Context, plan Plan, harness Harness, journalPath string) (result RunResult, runErr error) {
	if journalPath == "" {
		return RunResult{}, ErrJournalPathRequired
	}
	writer, err := newJournalWriter(journalPath, plan)
	if err != nil {
		return RunResult{}, err
	}
	return runPlan(ctx, plan, harness, writer)
}

func runPlan(ctx context.Context, plan Plan, harness Harness, writer *journalWriter) (result RunResult, runErr error) {
	if ctx == nil {
		return RunResult{}, errors.New("soak context is required")
	}
	if harness == nil {
		return RunResult{}, ErrHarnessRequired
	}
	if len(plan.Operations) == 0 {
		return RunResult{}, ErrZeroOperations
	}
	if err := plan.Config.validate(); err != nil {
		return RunResult{}, err
	}
	if err := validateRunPlan(plan); err != nil {
		return RunResult{}, err
	}
	if writer != nil {
		defer func() {
			if closeErr := writer.Close(); runErr == nil && closeErr != nil {
				runErr = closeErr
			}
		}()
		for _, operation := range plan.Operations {
			if err := writer.RecordOperation(operation); err != nil {
				return result, err
			}
		}
	}

	result.Seed = plan.Seed
	result.Observations = make([]invariants.Observation, 0, len(plan.Operations))
	for index, operation := range plan.Operations {
		if err := ctx.Err(); err != nil {
			return result, recordRunFailure(writer, operation.Sequence, "context-cancelled", err)
		}
		capture, err := harness.Execute(ctx, operation)
		if err != nil {
			err = fmt.Errorf("execute soak operation %d: %w", index+1, err)
			return result, recordRunFailure(writer, operation.Sequence, "harness-error", err)
		}
		if err := validateObservationCapture(operation, capture, result.Observations); err != nil {
			return result, recordRunFailure(writer, operation.Sequence, "capture-incomplete", err)
		}
		sequence := operation.Sequence
		observation := AssembleObservation(sequence, capture)
		if writer != nil {
			if err := writer.RecordObservation(observation); err != nil {
				return result, err
			}
		}
		result.Observations = append(result.Observations, observation)
		result.OperationsExecuted++

		violations, checkerErr := checkLatest(result.Observations)
		result.Violations = append(result.Violations, violations...)
		result.Violations = orderViolations(result.Violations)
		if checkerErr != nil {
			checkerErr = fmt.Errorf("run invariant checkers after operation %d: %w", index+1, checkerErr)
			return result, recordRunFailure(writer, operation.Sequence, "checker-error", checkerErr)
		}
		if len(violations) != 0 {
			if writer != nil {
				if err := writer.RecordFailure(operation.Sequence, "invariant-violation"); err != nil {
					return result, err
				}
			}
			return result, fmt.Errorf("%w after operation %d", ErrInvariantViolation, index+1)
		}
		if writer != nil {
			if err := writer.RecordCompletion(operation.Sequence, observation.Sequence); err != nil {
				return result, err
			}
		}
	}
	if result.OperationsExecuted == 0 {
		return result, ErrZeroOperations
	}
	if err := validateCheckerCoverage(plan, result.Observations); err != nil {
		return result, err
	}
	if writer != nil {
		if err := writer.Seal(); err != nil {
			return result, err
		}
	}
	return result, nil
}

// recordRunFailure writes the failure fact and preserves a recording error
// beside the run error, so a broken failure artifact is never silent.
func recordRunFailure(writer *journalWriter, sequence uint64, code string, runErr error) error {
	if writer == nil {
		return runErr
	}
	if recordErr := writer.RecordFailure(sequence, code); recordErr != nil {
		return errors.Join(runErr, fmt.Errorf("record soak failure fact: %w", recordErr))
	}
	return runErr
}

// ReplayPlan regenerates a journal plan and rejects any operation divergence.
func ReplayPlan(journal Journal, catalog *faults.Catalog) (Plan, error) {
	if err := journal.CatalogIdentity.validate(); err != nil {
		return Plan{}, ErrReplayMismatch
	}
	identity, err := catalogIdentityOf(catalog)
	if err != nil || identity != journal.CatalogIdentity {
		return Plan{}, ErrReplayMismatch
	}
	digest, err := planDigest(journal.Seed, journal.Config, journal.CatalogIdentity, journal.Operations)
	if err != nil || digest != journal.PlanDigest {
		return Plan{}, ErrReplayMismatch
	}
	plan, err := Generate(journal.Seed, journal.Config, catalog)
	if err != nil {
		return Plan{}, err
	}
	if !reflect.DeepEqual(plan.Operations, journal.Operations) || !reflect.DeepEqual(plan.Config, journal.Config) {
		return Plan{}, ErrReplayMismatch
	}
	return plan, nil
}

// ReplayRun regenerates a journal plan and executes that exact replay.
func ReplayRun(ctx context.Context, journalPath string, catalog *faults.Catalog, harness Harness) (RunResult, error) {
	journal, err := ReadJournal(journalPath)
	if err != nil && !errors.Is(err, ErrJournalUnsealed) {
		return RunResult{}, err
	}
	plan, err := ReplayPlan(journal, catalog)
	if err != nil {
		return RunResult{}, err
	}
	return runPlan(ctx, plan, harness, nil)
}

func validateRunPlan(plan Plan) error {
	if len(plan.Operations) != plan.Config.OperationCount {
		return fmt.Errorf("%w: operation count does not match configuration", ErrInvalidPlan)
	}
	if err := plan.CatalogIdentity.validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidPlan, err)
	}
	for index, operation := range plan.Operations {
		if operation.Sequence != uint64(index+1) || !validOperationKind(operation.Kind) || operation.UserID == "" || operation.ClientID == "" || operation.ScopeID == "" || operation.SchemaVersion == 0 {
			return fmt.Errorf("%w: operation %d identity", ErrInvalidPlan, index+1)
		}
		if index < len(operationKinds) && operation.Kind != operationKinds[index] {
			return fmt.Errorf("%w: operation %d does not satisfy coverage prefix", ErrInvalidPlan, index+1)
		}
		if !equalObservationSurfaces(operation.RequiredObservationSurfaces, requiredObservationSurfaces(operation.Kind)) {
			return fmt.Errorf("%w: operation %d observation surfaces", ErrInvalidPlan, index+1)
		}
		if err := validateOperationInput(operation); err != nil {
			return fmt.Errorf("%w: operation %d input: %w", ErrInvalidPlan, index+1, err)
		}
		if operation.FaultPlan != nil && !faultSupportsOperation(operation.Kind, *operation.FaultPlan) {
			return fmt.Errorf("%w: operation %d fault has no supported trigger", ErrInvalidPlan, index+1)
		}
	}
	return nil
}

func checkLatest(observations []invariants.Observation) ([]invariants.Violation, error) {
	latest := observations[len(observations)-1:]
	current := latest[0]
	priorClients := make(map[string]invariants.ClientObservation, len(current.Clients))
	needed := make(map[string]struct{}, len(current.Clients))
	for _, client := range current.Clients {
		needed[client.State.UserID+"\x00"+client.State.ClientID] = struct{}{}
	}
	for index := len(observations) - 2; index >= 0 && len(priorClients) < len(needed); index-- {
		for _, client := range observations[index].Clients {
			key := client.State.UserID + "\x00" + client.State.ClientID
			if _, wanted := needed[key]; !wanted {
				continue
			}
			if _, found := priorClients[key]; !found {
				priorClients[key] = client
			}
		}
	}
	stateForkWindow := latest
	if len(priorClients) != 0 {
		prior := invariants.Observation{Sequence: current.Sequence - 1, Clients: make([]invariants.ClientObservation, 0, len(priorClients))}
		for _, client := range priorClients {
			client.RestartBoundary = false
			prior.Clients = append(prior.Clients, client)
		}
		stateForkWindow = []invariants.Observation{prior, current}
	}
	checks := []struct {
		checker      func([]invariants.Observation) ([]invariants.Violation, error)
		observations []invariants.Observation
	}{
		{checker: invariants.CheckMutationConservation, observations: latest},
		{checker: invariants.CheckCursorMonotonicity, observations: observations},
		{checker: invariants.CheckChecksumConvergence, observations: latest},
		{checker: invariants.CheckScopeIsolation, observations: latest},
		{checker: invariants.CheckNoStateForks, observations: stateForkWindow},
	}
	var violations []invariants.Violation
	var failures []error
	for _, check := range checks {
		found, err := check.checker(check.observations)
		violations = append(violations, found...)
		if err != nil {
			failures = append(failures, err)
		}
	}
	return orderViolations(violations), errors.Join(failures...)
}

func validateCheckerCoverage(plan Plan, observations []invariants.Observation) error {
	applicable := make(map[invariants.InvariantFamily]struct{})
	for _, operation := range plan.Operations {
		switch operation.Kind {
		case OperationPush:
			applicable[invariants.InvariantMutationConservation] = struct{}{}
		case OperationPull:
			applicable[invariants.InvariantCursorMonotonicity] = struct{}{}
			applicable[invariants.InvariantChecksumConvergence] = struct{}{}
			applicable[invariants.InvariantScopeIsolation] = struct{}{}
		}
	}
	judged := make(map[invariants.InvariantFamily]struct{})
	for _, observation := range observations {
		for _, exchange := range observation.WireExchanges {
			if exchange.ExpectMutationConservation {
				judged[invariants.InvariantMutationConservation] = struct{}{}
			}
			if exchange.ExpectChecksumConvergence {
				judged[invariants.InvariantChecksumConvergence] = struct{}{}
			}
			if exchange.ExpectScopeIsolation {
				judged[invariants.InvariantScopeIsolation] = struct{}{}
			}
		}
		if len(observation.PullResults) != 0 && len(observation.CursorAcknowledgements) != 0 {
			judged[invariants.InvariantCursorMonotonicity] = struct{}{}
		}
	}
	missing := make([]string, 0, len(applicable))
	for family := range applicable {
		if _, ok := judged[family]; !ok {
			missing = append(missing, string(family))
		}
	}
	if len(missing) == 0 {
		return nil
	}
	sort.Strings(missing)
	return fmt.Errorf("%w: missing %s", ErrCheckerCoverage, strings.Join(missing, ", "))
}

func orderViolations(violations []invariants.Violation) []invariants.Violation {
	ordered := append([]invariants.Violation(nil), violations...)
	sort.SliceStable(ordered, func(left, right int) bool {
		return compareViolations(ordered[left], ordered[right]) < 0
	})
	return ordered
}

func compareViolations(left, right invariants.Violation) int {
	if left.ObservationSequence != right.ObservationSequence {
		if left.ObservationSequence < right.ObservationSequence {
			return -1
		}
		return 1
	}
	if left.Family != right.Family {
		return strings.Compare(string(left.Family), string(right.Family))
	}
	if left.RuleID != right.RuleID {
		return strings.Compare(string(left.RuleID), string(right.RuleID))
	}
	for index := 0; index < len(left.Evidence) && index < len(right.Evidence); index++ {
		if left.Evidence[index].Name != right.Evidence[index].Name {
			return strings.Compare(left.Evidence[index].Name, right.Evidence[index].Name)
		}
		if left.Evidence[index].Value != right.Evidence[index].Value {
			return strings.Compare(left.Evidence[index].Value, right.Evidence[index].Value)
		}
	}
	if len(left.Evidence) < len(right.Evidence) {
		return -1
	}
	if len(left.Evidence) > len(right.Evidence) {
		return 1
	}
	return 0
}
