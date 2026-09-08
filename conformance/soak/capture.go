package soak

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/trainstar/synchro/conformance/invariants"
	"github.com/trainstar/synchro/conformance/scenarios"
)

// ObservationSurface identifies one required black-box capture surface.
type ObservationSurface string

const (
	SurfaceManifest               ObservationSurface = "manifest"
	SurfaceServerState            ObservationSurface = "server-state"
	SurfaceOperator               ObservationSurface = "operator"
	SurfaceClients                ObservationSurface = "clients"
	SurfaceWireExchanges          ObservationSurface = "wire-exchanges"
	SurfaceCursorPositions        ObservationSurface = "cursor-positions"
	SurfacePullResults            ObservationSurface = "pull-results"
	SurfaceCursorAcknowledgements ObservationSurface = "cursor-acknowledgements"
	SurfaceServerRowIdentities    ObservationSurface = "server-row-identities"
	SurfaceFaultActivation        ObservationSurface = "fault-activation"
)

var baseObservationSurfaces = []ObservationSurface{
	SurfaceManifest,
	SurfaceServerState,
	SurfaceOperator,
	SurfaceClients,
	SurfaceWireExchanges,
}

func requiredObservationSurfaces(kind OperationKind) []ObservationSurface {
	surfaces := append([]ObservationSurface(nil), baseObservationSurfaces...)
	switch kind {
	case OperationPull:
		surfaces = append(surfaces,
			SurfaceCursorPositions,
			SurfacePullResults,
			SurfaceCursorAcknowledgements,
			SurfaceServerRowIdentities,
		)
	case OperationRebuild:
		surfaces = append(surfaces, SurfaceServerRowIdentities)
	case OperationProcessDeath:
		surfaces = append(surfaces, SurfaceFaultActivation)
	}
	return surfaces
}

func equalObservationSurfaces(left, right []ObservationSurface) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range right {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

// FaultActivationObservation records the bounded activation and cleanup of a selected fault.
type FaultActivationObservation struct {
	ControlID string `json:"control_id"`
	Target    string `json:"target"`
	Activated bool   `json:"activated"`
	CleanedUp bool   `json:"cleaned_up"`
}

// ErrCaptureIncomplete reports a capture that cannot support its operation contract.
var ErrCaptureIncomplete = errors.New("soak observation capture is incomplete")

func validateObservationCapture(operation Operation, capture ObservationCapture, prior []invariants.Observation) error {
	for _, surface := range operation.RequiredObservationSurfaces {
		if !captureHasSurface(capture, surface) {
			return fmt.Errorf("%w: operation %d missing %s", ErrCaptureIncomplete, operation.Sequence, surface)
		}
	}
	if capture.Manifest == nil || len(capture.Manifest.CanonicalBody()) == 0 {
		return fmt.Errorf("%w: operation %d manifest is empty", ErrCaptureIncomplete, operation.Sequence)
	}
	if !stateFactsPresent(capture.ServerState) {
		return fmt.Errorf("%w: operation %d server state is empty", ErrCaptureIncomplete, operation.Sequence)
	}
	if capture.Operator == nil || len(capture.Operator.Checkpoints) == 0 {
		return fmt.Errorf("%w: operation %d operator facts are empty", ErrCaptureIncomplete, operation.Sequence)
	}
	if len(capture.Clients) == 0 {
		return fmt.Errorf("%w: operation %d has no clients", ErrCaptureIncomplete, operation.Sequence)
	}
	if err := validateCaptureClients(operation, capture.Clients, prior); err != nil {
		return err
	}
	if err := validateFaultActivation(operation, capture.FaultActivation); err != nil {
		return err
	}
	if err := validateWireExchanges(operation, capture.WireExchanges); err != nil {
		return err
	}
	if err := validateCursorBindings(operation, capture, prior); err != nil {
		return err
	}
	if err := validateServerRowIdentities(operation, capture); err != nil {
		return err
	}
	return nil
}

func stateFactsPresent(facts *scenarios.StateFacts) bool {
	if facts == nil {
		return false
	}
	return facts.TransactionCount != nil || facts.RowCount != nil || facts.ScopeCount != nil || facts.RebuildCount != nil ||
		facts.BatchCount != nil || facts.MutationCount != nil || facts.ConfiguredLimits != nil || facts.Registry != nil ||
		facts.Stream != nil || len(facts.Transactions) != 0 || len(facts.Rows) != 0 || len(facts.Scopes) != 0 ||
		len(facts.MutationOutcomes) != 0 || len(facts.RowScopeEdges) != 0 || len(facts.Poison) != 0 ||
		len(facts.Rebuilds) != 0 || len(facts.Clients) != 0
}

func captureHasSurface(capture ObservationCapture, surface ObservationSurface) bool {
	switch surface {
	case SurfaceManifest:
		return capture.Manifest != nil
	case SurfaceServerState:
		return capture.ServerState != nil
	case SurfaceOperator:
		return capture.Operator != nil
	case SurfaceClients:
		return capture.Clients != nil
	case SurfaceWireExchanges:
		return capture.WireExchanges != nil
	case SurfaceCursorPositions:
		return capture.CursorPositions != nil
	case SurfacePullResults:
		return len(capture.PullResults) != 0
	case SurfaceCursorAcknowledgements:
		return len(capture.CursorAcknowledgements) != 0
	case SurfaceServerRowIdentities:
		return capture.ServerRowIdentities != nil
	case SurfaceFaultActivation:
		return capture.FaultActivation != nil
	default:
		return false
	}
}

func validateCaptureClients(operation Operation, clients []invariants.ClientObservation, prior []invariants.Observation) error {
	priorClients := make(map[string]struct{})
	for _, observation := range prior {
		for _, client := range observation.Clients {
			priorClients[clientKey(client)] = struct{}{}
		}
	}
	seen := make(map[string]struct{}, len(clients))
	for _, client := range clients {
		if client.State.UserID == "" || client.State.ClientID == "" {
			return fmt.Errorf("%w: operation %d client identity", ErrCaptureIncomplete, operation.Sequence)
		}
		key := clientKey(client)
		if _, exists := seen[key]; exists {
			return fmt.Errorf("%w: operation %d duplicate client", ErrCaptureIncomplete, operation.Sequence)
		}
		seen[key] = struct{}{}
		if !client.Complete || client.Process == nil {
			return fmt.Errorf("%w: operation %d client %s is not complete", ErrCaptureIncomplete, operation.Sequence, client.State.ClientID)
		}
		expectedBoundary := operation.Kind == OperationProcessDeath && client.State.UserID == operation.UserID && client.State.ClientID == operation.ClientID
		if client.RestartBoundary != expectedBoundary {
			return fmt.Errorf("%w: operation %d restart boundary for client %s is incorrect", ErrCaptureIncomplete, operation.Sequence, client.State.ClientID)
		}
		if expectedBoundary {
			if _, exists := priorClients[key]; !exists {
				return fmt.Errorf("%w: operation %d restart boundary has no prior client capture", ErrCaptureIncomplete, operation.Sequence)
			}
		}
	}
	if operation.Kind == OperationProcessDeath {
		if _, exists := seen[operation.UserID+"\x00"+operation.ClientID]; !exists {
			return fmt.Errorf("%w: operation %d process target is not captured", ErrCaptureIncomplete, operation.Sequence)
		}
	}
	return nil
}

func validateFaultActivation(operation Operation, activation *FaultActivationObservation) error {
	if operation.FaultPlan == nil {
		if activation != nil {
			return fmt.Errorf("%w: operation %d has an unexpected fault activation", ErrCaptureIncomplete, operation.Sequence)
		}
		return nil
	}
	if activation == nil || !activation.Activated || !activation.CleanedUp || activation.ControlID != string(operation.FaultPlan.ControlID) || activation.Target == "" {
		return fmt.Errorf("%w: operation %d fault activation is incomplete", ErrCaptureIncomplete, operation.Sequence)
	}
	return nil
}

func validateWireExchanges(operation Operation, exchanges []invariants.WireExchangeObservation) error {
	expectedClass := operationExchangeClass(operation.Kind)
	if operation.Kind != OperationProcessDeath && len(exchanges) == 0 {
		return fmt.Errorf("%w: operation %d has no wire exchange", ErrCaptureIncomplete, operation.Sequence)
	}
	seen := make(map[uint64]struct{}, len(exchanges))
	mutationJudged := false
	checksumJudged := false
	scopeJudged := false
	for _, exchange := range exchanges {
		if exchange.Sequence == 0 {
			return fmt.Errorf("%w: operation %d wire exchange has no sequence", ErrCaptureIncomplete, operation.Sequence)
		}
		if _, exists := seen[exchange.Sequence]; exists {
			return fmt.Errorf("%w: operation %d wire exchange sequence is duplicated", ErrCaptureIncomplete, operation.Sequence)
		}
		seen[exchange.Sequence] = struct{}{}
		if expectedClass != "" && exchange.OperationClass != expectedClass {
			return fmt.Errorf("%w: operation %d wire exchange class %q does not match %q", ErrCaptureIncomplete, operation.Sequence, exchange.OperationClass, expectedClass)
		}
		if exchange.ExpectMutationConservation {
			if operation.Kind != OperationPush || exchange.OperationClass != "push" {
				return fmt.Errorf("%w: operation %d has mutation facts on a non-push exchange", ErrCaptureIncomplete, operation.Sequence)
			}
			mutationJudged = true
		}
		if exchange.ExpectChecksumConvergence {
			if operation.Kind != OperationPull || exchange.OperationClass != "pull" {
				return fmt.Errorf("%w: operation %d has checksum facts on a non-pull exchange", ErrCaptureIncomplete, operation.Sequence)
			}
			terminal, changeCount, err := terminalPullExchange(exchange)
			if err != nil || !terminal || changeCount != 1 {
				return fmt.Errorf("%w: operation %d checksum facts are not bound to one terminal pull change", ErrCaptureIncomplete, operation.Sequence)
			}
			checksumJudged = true
		}
		if exchange.ExpectScopeIsolation {
			if operation.Kind != OperationPull || exchange.OperationClass != "pull" {
				return fmt.Errorf("%w: operation %d has scope facts on a non-pull exchange", ErrCaptureIncomplete, operation.Sequence)
			}
			terminal, changeCount, err := terminalPullExchange(exchange)
			if err != nil || !terminal || changeCount != 0 {
				return fmt.Errorf("%w: operation %d scope facts are not bound to a terminal zero-change pull", ErrCaptureIncomplete, operation.Sequence)
			}
			scopeJudged = true
		}
	}
	switch operation.Kind {
	case OperationPush:
		if !mutationJudged {
			return fmt.Errorf("%w: operation %d push exchange has no mutation checker facts", ErrCaptureIncomplete, operation.Sequence)
		}
	case OperationPull:
		if !checksumJudged || !scopeJudged {
			return fmt.Errorf("%w: operation %d pull exchange lacks terminal checker facts", ErrCaptureIncomplete, operation.Sequence)
		}
	}
	return nil
}

func operationExchangeClass(kind OperationKind) string {
	switch kind {
	case OperationConnect:
		return "connect"
	case OperationPush:
		return "push"
	case OperationPull:
		return "pull"
	case OperationRebuild:
		return "rebuild"
	case OperationSchemaTransition:
		return "schema-transition"
	case OperationProcessDeath:
		return "process-death"
	case OperationWireFault:
		return "wire-fault"
	default:
		return ""
	}
}

func terminalPullExchange(exchange invariants.WireExchangeObservation) (bool, int, error) {
	if exchange.ResponseStatus != 200 || exchange.OperationClass != "pull" {
		return false, 0, nil
	}
	var response struct {
		HasMore *bool             `json:"has_more"`
		Changes []json.RawMessage `json:"changes"`
	}
	if err := json.Unmarshal(exchange.ResponseBody, &response); err != nil || response.HasMore == nil || response.Changes == nil {
		return false, 0, errors.New("pull response is invalid")
	}
	return !*response.HasMore, len(response.Changes), nil
}

func validateCursorBindings(operation Operation, capture ObservationCapture, prior []invariants.Observation) error {
	if operation.Kind != OperationPull {
		if len(capture.PullResults) != 0 || len(capture.CursorAcknowledgements) != 0 {
			return fmt.Errorf("%w: operation %d has unexpected pull facts", ErrCaptureIncomplete, operation.Sequence)
		}
		return validateRawCursorRelations(operation, capture.Clients, capture.CursorPositions)
	}
	if len(capture.PullResults) == 0 || len(capture.CursorAcknowledgements) == 0 {
		return fmt.Errorf("%w: operation %d pull facts are empty", ErrCaptureIncomplete, operation.Sequence)
	}
	if err := validateRawCursorRelations(operation, capture.Clients, capture.CursorPositions); err != nil {
		return err
	}
	wires := make(map[uint64]invariants.WireExchangeObservation, len(capture.WireExchanges))
	for _, exchange := range capture.WireExchanges {
		wires[exchange.Sequence] = exchange
	}
	pulls := make(map[uint64]invariants.PullResultObservation, len(capture.PullResults))
	for _, result := range capture.PullResults {
		if result.ExchangeSequence == 0 || result.UserID == "" || result.ClientID == "" || len(result.Cursors) == 0 {
			return fmt.Errorf("%w: operation %d pull result is incomplete", ErrCaptureIncomplete, operation.Sequence)
		}
		exchange, exists := wires[result.ExchangeSequence]
		if !exists || exchange.OperationClass != "pull" || exchange.ResponseStatus != 200 {
			return fmt.Errorf("%w: operation %d pull result is not bound to a successful pull exchange", ErrCaptureIncomplete, operation.Sequence)
		}
		if _, duplicate := pulls[result.ExchangeSequence]; duplicate {
			return fmt.Errorf("%w: operation %d pull result is duplicated", ErrCaptureIncomplete, operation.Sequence)
		}
		pulls[result.ExchangeSequence] = result
		for _, cursor := range result.Cursors {
			if err := requireCursorPosition(capture.CursorPositions, cursor, operation.Sequence); err != nil {
				return err
			}
		}
	}
	acknowledged := make(map[string]struct{}, len(capture.CursorAcknowledgements))
	for _, acknowledgement := range capture.CursorAcknowledgements {
		result, exists := pulls[acknowledgement.ExchangeSequence]
		if !exists || !sameCursorInList(result.Cursors, acknowledgement.Cursor) {
			return fmt.Errorf("%w: operation %d acknowledgement is not bound to a pull result", ErrCaptureIncomplete, operation.Sequence)
		}
		key := strconv.FormatUint(acknowledgement.ExchangeSequence, 10) + "\x00" + cursorKey(acknowledgement.Cursor)
		if _, duplicate := acknowledged[key]; duplicate {
			return fmt.Errorf("%w: operation %d acknowledgement is duplicated", ErrCaptureIncomplete, operation.Sequence)
		}
		acknowledged[key] = struct{}{}
	}
	for _, result := range capture.PullResults {
		for _, cursor := range result.Cursors {
			key := strconv.FormatUint(result.ExchangeSequence, 10) + "\x00" + cursorKey(cursor)
			if _, exists := acknowledged[key]; !exists {
				return fmt.Errorf("%w: operation %d pull cursor is not acknowledged", ErrCaptureIncomplete, operation.Sequence)
			}
		}
	}
	return nil
}

func validateRawCursorRelations(operation Operation, clients []invariants.ClientObservation, positions []invariants.CursorPositionObservation) error {
	byKey := make(map[string]invariants.CursorPositionObservation, len(positions))
	for _, position := range positions {
		key := cursorKey(position)
		if position.RawCursor == "" {
			return fmt.Errorf("%w: operation %d cursor position is empty", ErrCaptureIncomplete, operation.Sequence)
		}
		if _, duplicate := byKey[key]; duplicate {
			return fmt.Errorf("%w: operation %d cursor position is duplicated", ErrCaptureIncomplete, operation.Sequence)
		}
		byKey[key] = position
	}
	for _, client := range clients {
		for _, scope := range client.Scopes {
			if scope.RawCursor == nil {
				continue
			}
			key := client.State.UserID + "\x00" + client.State.ClientID + "\x00" + scope.ScopeID + "\x00" + strconv.FormatUint(scope.Generation, 10) + "\x00" + *scope.RawCursor
			if position, exists := byKey[key]; !exists || position.UserID != client.State.UserID || position.ClientID != client.State.ClientID {
				return fmt.Errorf("%w: operation %d raw cursor has no position binding", ErrCaptureIncomplete, operation.Sequence)
			}
		}
	}
	return nil
}

func requireCursorPosition(positions []invariants.CursorPositionObservation, want invariants.CursorPositionObservation, sequence uint64) error {
	for _, position := range positions {
		if sameCursorInList([]invariants.CursorPositionObservation{position}, want) {
			return nil
		}
	}
	return fmt.Errorf("%w: operation %d pull cursor has no position binding", ErrCaptureIncomplete, sequence)
}

func sameCursorInList(values []invariants.CursorPositionObservation, want invariants.CursorPositionObservation) bool {
	for _, value := range values {
		if cursorKey(value) == cursorKey(want) && value.UserID == want.UserID && value.ClientID == want.ClientID && value.StreamGeneration == want.StreamGeneration && samePosition(value.Position, want.Position) {
			return true
		}
	}
	return false
}

func samePosition(left, right invariants.PositionObservation) bool {
	if left.Kind != right.Kind {
		return false
	}
	if !equalOptionalString(left.CommitLSN, right.CommitLSN) || !equalOptionalUint64(left.EventOrdinal, right.EventOrdinal) || !equalOptionalUint64(left.EffectOrdinal, right.EffectOrdinal) {
		return false
	}
	return true
}

func equalOptionalString(left, right *string) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func equalOptionalUint64(left, right *uint64) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func cursorKey(cursor invariants.CursorPositionObservation) string {
	return cursor.UserID + "\x00" + cursor.ClientID + "\x00" + cursor.ScopeID + "\x00" + strconv.FormatUint(cursor.Generation, 10) + "\x00" + cursor.RawCursor
}

func clientKey(client invariants.ClientObservation) string {
	return client.State.UserID + "\x00" + client.State.ClientID
}

func validateServerRowIdentities(operation Operation, capture ObservationCapture) error {
	if capture.ServerState == nil || capture.ServerState.RowScopeEdges == nil {
		return nil
	}
	required := make(map[string]struct{})
	for _, edge := range capture.ServerState.RowScopeEdges {
		required[edge.TableID+"\x00"+edge.CanonicalWireJSON] = struct{}{}
	}
	seen := make(map[string]struct{}, len(capture.ServerRowIdentities))
	for _, relation := range capture.ServerRowIdentities {
		key := relation.TableID + "\x00" + relation.CanonicalWireJSON
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("%w: operation %d server row identity is duplicated", ErrCaptureIncomplete, operation.Sequence)
		}
		if len(relation.RowIdentity) == 0 {
			return fmt.Errorf("%w: operation %d server row identity is empty", ErrCaptureIncomplete, operation.Sequence)
		}
		seen[key] = struct{}{}
	}
	if len(seen) != len(required) {
		return fmt.Errorf("%w: operation %d server row identity relations are incomplete", ErrCaptureIncomplete, operation.Sequence)
	}
	for key := range required {
		if _, exists := seen[key]; !exists {
			return fmt.Errorf("%w: operation %d server row identity relation is missing", ErrCaptureIncomplete, operation.Sequence)
		}
	}
	return nil
}
