package reference

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"time"
)

const maxReferenceJSONInteger uint64 = 9007199254740991

type tokenSource string

const (
	tokenSourceNone                     tokenSource = "none"
	tokenSourceLocalCheckpoint          tokenSource = "local_checkpoint"
	tokenSourceLocalRebuildContinuation tokenSource = "local_rebuild_continuation"
	tokenSourceForged                   tokenSource = "forged"
)

type pullScopePayload struct {
	ScopeID      string      `json:"scope_id"`
	CursorSource tokenSource `json:"cursor_source"`
}

type pullRequestPayload struct {
	UserID           string                  `json:"user_id"`
	ClientID         string                  `json:"client_id"`
	ClientGeneration uint64                  `json:"client_generation"`
	Schema           *schemaReferencePayload `json:"schema"`
	ScopeSetVersion  uint64                  `json:"scope_set_version"`
	Scopes           []pullScopePayload      `json:"scopes"`
	Limit            uint32                  `json:"limit"`
}

type localApplyPullPagePayload struct {
	UserID       string `json:"user_id"`
	ClientID     string `json:"client_id"`
	SourceStepID string `json:"source_step_id"`
}

type localPullChangePlan struct {
	change     PullChangeObservation
	projection CapturedProjection
}

type localPullCursorPlan struct {
	scope    ScopeID
	token    OpaqueToken
	position StreamPosition
}

type pullScopeRuntime struct {
	scope            ScopeID
	assignment       ScopeAssignment
	state            ScopeState
	hasState         bool
	cursor           StreamPosition
	hasCursor        bool
	acknowledged     bool
	completedRebuild bool
	rebuild          bool
	candidates       []pullCandidate
	selected         []pullCandidate
	issue            StreamPosition
	hasIssue         bool
}

type pullCandidate struct {
	scope  ScopeID
	effect ScopeEffect
}

type scopedRowIdentity struct {
	Scope ScopeID
	Row   RowIdentity
}

type issuedCursorPlan struct {
	runtime *pullScopeRuntime
	token   OpaqueToken
}

func init() {
	registerOperation("pull/request-page", pullRequestPage)
	registerResolvedOperation("local/apply-pull-page", localApplyPullPage)
}

func pullRequestPage(_ context.Context, model *Model, payload json.RawMessage) (StepResult, error) {
	request, err := decodePullRequest(payload)
	if err != nil {
		return pullEndpointError(400, "invalid_request", false), nil
	}
	if request.Limit > model.state.ConfiguredLimits.PullMaximum {
		return pullEndpointError(400, "invalid_request", false), nil
	}

	client := ClientKey{UserID: UserID(request.UserID), ClientID: ClientID(request.ClientID)}
	clientState, gate := endpointClientGate(model.state, client, Generation(request.ClientGeneration), request.Schema)
	if gate != nil {
		return pullEndpointError(gate.Status, gate.Code, gate.Retryable), nil
	}
	if request.ScopeSetVersion > uint64(clientState.ScopeSetVersion) {
		return pullEndpointError(400, "invalid_request", false), nil
	}

	requested := make(map[ScopeID]pullScopePayload, len(request.Scopes))
	for _, scopeRequest := range request.Scopes {
		scope := ScopeID(scopeRequest.ScopeID)
		if _, duplicate := requested[scope]; duplicate {
			return pullEndpointError(400, "invalid_request", false), nil
		}
		assignment, found := scopeAssignmentFor(clientState, scope)
		if !found {
			return pullEndpointError(400, "invalid_request", false), nil
		}
		requested[scope] = scopeRequest
		_ = assignment
	}

	now := referenceNow(model.clock)
	runtimes := make(map[ScopeID]*pullScopeRuntime, len(requested))
	for _, scope := range sortedPullScopes(requested) {
		scopeRequest := requested[scope]
		assignment, _ := scopeAssignmentFor(clientState, scope)
		runtime := &pullScopeRuntime{scope: scope, assignment: assignment}
		if !assignment.Assigned {
			runtimes[scope] = runtime
			continue
		}

		scopeState, exists := model.state.Scopes[scope]
		if !exists {
			return pullEndpointError(500, "sync_integrity_failure", false), nil
		}
		runtime.state = scopeState
		runtime.hasState = true

		if scopeRequest.CursorSource == tokenSourceNone {
			runtime.rebuild = true
			runtimes[scope] = runtime
			continue
		}

		token, found := resolveModelToken(model, client, scope, "", scopeRequest.CursorSource)
		if !found {
			return pullEndpointError(400, "invalid_request", false), nil
		}
		status, position := validateIncrementalCursor(model, client, assignment, scopeState, scope, token, now)
		switch status {
		case TokenStatusValid:
			runtime.hasCursor = true
			runtime.cursor = position
		case TokenStatusStale:
			runtime.rebuild = true
		default:
			return pullEndpointError(400, "invalid_request", false), nil
		}
		if assignment.RebuildRequired {
			if completedRebuildFinalCursor(model, client, assignment, scopeState, scope, token, now) {
				runtime.completedRebuild = true
			} else {
				runtime.rebuild = true
			}
		}
		runtimes[scope] = runtime
	}

	addedScopes := make([]ScopeID, 0)
	for _, assignment := range sortedActiveAssignments(clientState) {
		if _, requestedByClient := requested[assignment.Scope]; requestedByClient {
			continue
		}
		scopeState, exists := model.state.Scopes[assignment.Scope]
		if !exists {
			return pullEndpointError(500, "sync_integrity_failure", false), nil
		}
		runtimes[assignment.Scope] = &pullScopeRuntime{
			scope:      assignment.Scope,
			assignment: assignment,
			state:      scopeState,
			hasState:   true,
			rebuild:    true,
		}
		addedScopes = append(addedScopes, assignment.Scope)
	}
	removedScopes := make([]ScopeID, 0)
	for scope := range requested {
		assignment, _ := scopeAssignmentFor(clientState, scope)
		if !assignment.Assigned {
			removedScopes = append(removedScopes, scope)
		}
	}
	sortScopeIDs(addedScopes)
	sortScopeIDs(removedScopes)

	if hasPendingAcceptedFence(model.state, client) {
		return pullEndpointError(503, "capture_pending", true), nil
	}

	boundary := currentPullBoundary(model.state)
	for _, scope := range sortedRuntimeScopes(runtimes) {
		runtime := runtimes[scope]
		if runtime.rebuild || !runtime.assignment.Assigned {
			continue
		}
		candidates, integrityOK := eligiblePullCandidates(runtime, boundary)
		if !integrityOK {
			return pullEndpointError(500, "sync_integrity_failure", false), nil
		}
		runtime.candidates = candidates
	}

	streams := make(map[ScopeID][]pullCandidate, len(runtimes))
	for _, scope := range sortedRuntimeScopes(runtimes) {
		runtime := runtimes[scope]
		if runtime.rebuild || !runtime.assignment.Assigned {
			continue
		}
		streams[scope] = runtime.candidates
	}
	selected, hasMore := mergePullCandidates(streams, int(request.Limit))
	for _, candidate := range selected {
		runtimes[candidate.scope].selected = append(runtimes[candidate.scope].selected, candidate)
	}

	changes := make([]PullChangeObservation, 0, len(selected))
	for _, candidate := range selected {
		if model.pullHydrationFault != nil && candidate.effect.HasCapturedProjection && candidate.effect.CapturedProjection == model.pullHydrationFault.Projection {
			model.pullHydrationFaultTriggered = true
			return pullEndpointError(500, "sync_integrity_failure", false), nil
		}
		change, valid := hydratePullCandidate(model.state, candidate)
		if !valid {
			return pullEndpointError(500, "sync_integrity_failure", false), nil
		}
		changes = append(changes, change)
	}

	checksums := []ScopeChecksumObservation(nil)
	if !hasMore {
		active := sortedActiveAssignments(clientState)
		checksums = make([]ScopeChecksumObservation, 0, len(active))
		for _, assignment := range active {
			scopeState, exists := model.state.Scopes[assignment.Scope]
			if !exists {
				return pullEndpointError(500, "sync_integrity_failure", false), nil
			}
			checksums = append(checksums, ScopeChecksumObservation{
				Scope:       assignment.Scope,
				HasChecksum: true,
				Checksum:    scopeState.Checksum,
			})
		}
	}

	issueCount := 0
	for _, scope := range sortedRuntimeScopes(runtimes) {
		runtime := runtimes[scope]
		if runtime.rebuild || !runtime.assignment.Assigned || !runtime.hasCursor {
			continue
		}
		position, shouldIssue := pullCursorPosition(runtime, boundary, hasMore)
		if !shouldIssue || position == runtime.cursor {
			continue
		}
		runtime.issue = position
		runtime.hasIssue = true
		issueCount++
	}
	if !canMintTokens(model.authority, issueCount) {
		return pullEndpointError(500, "sync_integrity_failure", false), nil
	}

	plans := make([]issuedCursorPlan, 0, issueCount)
	for _, scope := range sortedRuntimeScopes(runtimes) {
		runtime := runtimes[scope]
		if !runtime.hasIssue {
			continue
		}
		bindings := incrementalCursorBindings(model.state, client, runtime.assignment, runtime.state, scope, runtime.issue, now)
		token := model.authority.Mint(string(TokenKindIncrementalCursor), bindings)
		if token == (OpaqueToken{}) {
			return StepResult{}, fmt.Errorf("mint pull cursor: token authority exhausted")
		}
		plans = append(plans, issuedCursorPlan{runtime: runtime, token: token})
	}

	for _, scope := range sortedRuntimeScopes(runtimes) {
		runtime := runtimes[scope]
		if runtime.rebuild || !runtime.assignment.Assigned || !runtime.hasCursor {
			continue
		}
		runtime.acknowledged = acknowledgeServerCursor(&clientState, scope, runtime.cursor, now)
		if runtime.completedRebuild {
			if !clearAcknowledgedRebuildRequirement(&clientState, scope) {
				return pullEndpointError(500, "sync_integrity_failure", false), nil
			}
			runtime.acknowledged = true
		}
	}
	for _, plan := range plans {
		storeIssuedServerCursor(&clientState, plan.runtime.scope, plan.token)
	}
	model.state.Clients[client] = clientState

	rebuildScopes := make([]ScopeID, 0)
	scopeCursors := make([]ScopeCursorObservation, 0, len(runtimes))
	for _, scope := range sortedRuntimeScopes(runtimes) {
		runtime := runtimes[scope]
		disposition := CursorDispositionUnchanged
		if runtime.rebuild {
			disposition = CursorDispositionRebuildRequired
			rebuildScopes = append(rebuildScopes, scope)
		} else if runtime.hasIssue {
			disposition = CursorDispositionIssued
		} else if runtime.acknowledged {
			disposition = CursorDispositionAcknowledged
		}
		scopeCursors = append(scopeCursors, ScopeCursorObservation{Scope: scope, Disposition: disposition})
	}
	sortScopeIDs(rebuildScopes)

	return StepResult{
		Kind: StepResultKindPull,
		HTTP: &HTTPObservation{
			Status: 200,
		},
		Pull: &PullObservation{
			Changes:        changes,
			ScopeCursors:   scopeCursors,
			AddedScopes:    addedScopes,
			RemovedScopes:  removedScopes,
			RebuildScopes:  rebuildScopes,
			HasMore:        hasMore,
			ScopeChecksums: checksums,
		},
	}, nil
}

func localApplyPullPage(_ context.Context, model *Model, payload json.RawMessage, input ResolvedOperationInput) (StepResult, error) {
	var request localApplyPullPagePayload
	if err := decodeStrictPayload(payload, &request); err != nil {
		return StepResult{}, fmt.Errorf("decode local apply-pull-page payload: %w", err)
	}
	if request.UserID == "" || request.ClientID == "" || request.SourceStepID == "" {
		return StepResult{}, fmt.Errorf("local apply-pull-page identity is invalid")
	}
	if input.SourceStep == nil || input.PortableSeed != nil {
		return StepResult{}, fmt.Errorf("local apply-pull-page requires only a resolved source step")
	}
	source := input.SourceStep
	if source.StepID != request.SourceStepID || source.OperationKey != "pull/request-page" {
		return StepResult{}, fmt.Errorf("local apply-pull-page source step binding is invalid")
	}
	if source.Result.Kind != StepResultKindPull || source.Result.HTTP == nil || source.Result.HTTP.Status != 200 || source.Result.Pull == nil {
		return StepResult{}, fmt.Errorf("local apply-pull-page source result is not a successful pull")
	}

	client := ClientKey{UserID: UserID(request.UserID), ClientID: ClientID(request.ClientID)}
	server, found := model.state.Clients[client]
	if !found {
		return StepResult{}, fmt.Errorf("local apply-pull-page server client is absent")
	}
	local, found := model.state.ClientLocal[client]
	if !found {
		return StepResult{}, fmt.Errorf("local apply-pull-page local client is absent")
	}

	changes, err := planLocalPullChanges(model.state, server, local, source.Result.Pull.Changes)
	if err != nil {
		return StepResult{}, err
	}
	cursors, err := planLocalPullCursors(model, client, server, local, source.Result.Pull.ScopeCursors)
	if err != nil {
		return StepResult{}, err
	}
	checksums, err := planLocalPullChecksums(model.state, server, local, source.Result.Pull)
	if err != nil {
		return StepResult{}, err
	}

	for _, plan := range changes {
		if plan.change.Operation == EffectOperationDelete {
			removed := removeOneLocalScopeProvenance(&local, plan.change.Row, plan.change.Scope)
			pruneUnjustifiedLocalRows(&local, removed)
			continue
		}
		putLocalRow(&local, LocalRow{
			Identity:         plan.change.Row,
			Fields:           cloneFieldValues(plan.projection.Fields),
			HasServerVersion: true,
			ServerVersion:    plan.change.Version,
			HasChecksum:      true,
			Checksum:         plan.change.Checksum,
			UpdatedAt:        cloneTime(plan.projection.CapturedAt),
		})
		addLocalScopeProvenance(&local, plan.change.Row, plan.change.Scope, plan.change.Version)
	}
	for _, plan := range cursors {
		index := localScopeCheckpointIndex(local.ScopeCheckpoints, plan.scope)
		if index < 0 {
			local.ScopeCheckpoints = append(local.ScopeCheckpoints, LocalScopeCheckpoint{Scope: plan.scope})
			index = len(local.ScopeCheckpoints) - 1
		}
		checkpoint := &local.ScopeCheckpoints[index]
		checkpoint.Position = plan.position
		checkpoint.HasCursor = true
		checkpoint.Cursor = plan.token
	}
	for _, checksum := range checksums {
		index := localScopeCheckpointIndex(local.ScopeCheckpoints, checksum.Scope)
		if index < 0 {
			local.ScopeCheckpoints = append(local.ScopeCheckpoints, LocalScopeCheckpoint{Scope: checksum.Scope})
			index = len(local.ScopeCheckpoints) - 1
		}
		checkpoint := &local.ScopeCheckpoints[index]
		checkpoint.HasChecksum = true
		checkpoint.Checksum = checksum.Checksum
		checkpoint.Verified = true
	}
	model.state.ClientLocal[client] = local
	return localOperationResult(client, LocalMutationStatusAccepted), nil
}

func planLocalPullChanges(state State, server ClientState, local ClientLocalState, changes []PullChangeObservation) ([]localPullChangePlan, error) {
	plans := make([]localPullChangePlan, 0, len(changes))
	seen := make(map[scopedRowIdentity]struct{}, len(changes))
	for _, change := range changes {
		key := scopedRowIdentity{Scope: change.Scope, Row: change.Row}
		if _, duplicate := seen[key]; duplicate {
			return nil, fmt.Errorf("local apply-pull-page source change is duplicated")
		}
		seen[key] = struct{}{}
		if change.Scope == "" || change.Row.CanonicalIdentityBytes == "" || change.Version == "" || !change.HasChecksum || !validEffectOperation(change.Operation) {
			return nil, fmt.Errorf("local apply-pull-page source change is incomplete")
		}
		assignment, scopeState, err := currentPullApplyScope(state, server, local, change.Scope)
		if err != nil {
			return nil, err
		}
		_ = assignment
		projection, found := currentProjectionForPullChange(state, scopeState, change)
		if !found {
			return nil, fmt.Errorf("local apply-pull-page source change has no matching current projection")
		}
		plans = append(plans, localPullChangePlan{change: change, projection: projection})
	}
	return plans, nil
}

func currentProjectionForPullChange(state State, scope ScopeState, change PullChangeObservation) (CapturedProjection, bool) {
	var matched CapturedProjection
	found := false
	for _, effect := range scope.Effects {
		if effect.Row != change.Row || effect.Operation != change.Operation || effect.Version != change.Version || !effect.HasChecksum || effect.Checksum != change.Checksum {
			continue
		}
		candidate := pullCandidate{scope: change.Scope, effect: effect}
		observed, valid := hydratePullCandidate(state, candidate)
		if !valid || observed != change {
			continue
		}
		projection := state.Projections[effect.CapturedProjection]
		if found && !equivalentCapturedProjection(matched, projection) {
			return CapturedProjection{}, false
		}
		matched = projection
		found = true
	}
	return matched, found
}

func equivalentCapturedProjection(left, right CapturedProjection) bool {
	if left.Event != right.Event || left.Image != right.Image || left.Row != right.Row || left.Version != right.Version || left.Checksum != right.Checksum || len(left.Fields) != len(right.Fields) {
		return false
	}
	for index := range left.Fields {
		if left.Fields[index] != right.Fields[index] {
			return false
		}
	}
	if left.CapturedAt == nil || right.CapturedAt == nil {
		return left.CapturedAt == nil && right.CapturedAt == nil
	}
	return left.CapturedAt.Equal(*right.CapturedAt)
}

func planLocalPullCursors(model *Model, client ClientKey, server ClientState, local ClientLocalState, observations []ScopeCursorObservation) ([]localPullCursorPlan, error) {
	plans := make([]localPullCursorPlan, 0)
	seen := make(map[ScopeID]struct{}, len(observations))
	for _, observation := range observations {
		if observation.Scope == "" {
			return nil, fmt.Errorf("local apply-pull-page source cursor has an empty scope")
		}
		if _, duplicate := seen[observation.Scope]; duplicate {
			return nil, fmt.Errorf("local apply-pull-page source cursor is duplicated")
		}
		seen[observation.Scope] = struct{}{}
		switch observation.Disposition {
		case CursorDispositionIssued:
		case CursorDispositionAcknowledged, CursorDispositionUnchanged, CursorDispositionRebuildRequired:
			continue
		default:
			return nil, fmt.Errorf("local apply-pull-page source cursor disposition is invalid")
		}
		assignment, scopeState, err := currentPullApplyScope(model.state, server, local, observation.Scope)
		if err != nil {
			return nil, err
		}
		checkpoint, found := serverCheckpointForScope(server.Checkpoints, observation.Scope)
		if !found || !checkpoint.HasCursor || checkpoint.Cursor == (OpaqueToken{}) {
			return nil, fmt.Errorf("local apply-pull-page issued server cursor is absent")
		}
		status, position := validateIncrementalCursor(model, client, assignment, scopeState, observation.Scope, checkpoint.Cursor, referenceNow(model.clock))
		if status != TokenStatusValid {
			return nil, fmt.Errorf("local apply-pull-page issued server cursor is not current")
		}
		if existing, found := localCheckpointForScope(local.ScopeCheckpoints, observation.Scope); found && existing.Position.StreamGeneration != "" && lessStreamPosition(position, existing.Position) {
			return nil, fmt.Errorf("local apply-pull-page issued cursor moves local progress backward")
		}
		plans = append(plans, localPullCursorPlan{scope: observation.Scope, token: checkpoint.Cursor, position: position})
	}
	return plans, nil
}

func planLocalPullChecksums(state State, server ClientState, local ClientLocalState, pull *PullObservation) ([]ScopeChecksumObservation, error) {
	if pull.HasMore {
		if len(pull.ScopeChecksums) != 0 {
			return nil, fmt.Errorf("local apply-pull-page nonterminal source has checksums")
		}
		return nil, nil
	}
	active := sortedActiveAssignments(server)
	if len(pull.ScopeChecksums) != len(active) {
		return nil, fmt.Errorf("local apply-pull-page terminal checksum set is incomplete")
	}
	checksums := make([]ScopeChecksumObservation, 0, len(pull.ScopeChecksums))
	seen := make(map[ScopeID]struct{}, len(pull.ScopeChecksums))
	for _, checksum := range pull.ScopeChecksums {
		if _, duplicate := seen[checksum.Scope]; duplicate || !checksum.HasChecksum {
			return nil, fmt.Errorf("local apply-pull-page terminal checksum is invalid")
		}
		seen[checksum.Scope] = struct{}{}
		_, scopeState, err := currentPullApplyScope(state, server, local, checksum.Scope)
		if err != nil {
			return nil, err
		}
		if scopeState.Checksum != checksum.Checksum {
			return nil, fmt.Errorf("local apply-pull-page terminal checksum changed")
		}
		checksums = append(checksums, checksum)
	}
	for _, assignment := range active {
		if _, found := seen[assignment.Scope]; !found {
			return nil, fmt.Errorf("local apply-pull-page terminal checksum omits an assigned scope")
		}
	}
	return checksums, nil
}

func currentPullApplyScope(state State, server ClientState, local ClientLocalState, scope ScopeID) (ScopeAssignment, ScopeState, error) {
	assignment, found := scopeAssignmentFor(server, scope)
	if !found || !assignment.Assigned {
		return ScopeAssignment{}, ScopeState{}, fmt.Errorf("local apply-pull-page server assignment is absent")
	}
	scopeState, found := state.Scopes[scope]
	if !found || scopeState.Schema != state.CurrentSchema || scopeState.MembershipGeneration != assignment.MembershipGeneration || scopeState.RetentionGeneration != assignment.RetentionGeneration {
		return ScopeAssignment{}, ScopeState{}, fmt.Errorf("local apply-pull-page scope binding changed")
	}
	index := localScopeAssignmentIndex(local.ScopeAssignments, scope)
	if index < 0 || !local.ScopeAssignments[index].Assigned || local.ScopeAssignments[index].MembershipGeneration != assignment.MembershipGeneration || local.ScopeAssignments[index].RetentionGeneration != assignment.RetentionGeneration {
		return ScopeAssignment{}, ScopeState{}, fmt.Errorf("local apply-pull-page local assignment is absent or changed")
	}
	return assignment, scopeState, nil
}

func serverCheckpointForScope(checkpoints []ClientCheckpoint, scope ScopeID) (ClientCheckpoint, bool) {
	for _, checkpoint := range checkpoints {
		if checkpoint.Scope == scope {
			return checkpoint, true
		}
	}
	return ClientCheckpoint{}, false
}

func localCheckpointForScope(checkpoints []LocalScopeCheckpoint, scope ScopeID) (LocalScopeCheckpoint, bool) {
	for _, checkpoint := range checkpoints {
		if checkpoint.Scope == scope {
			return checkpoint, true
		}
	}
	return LocalScopeCheckpoint{}, false
}

func removeOneLocalScopeProvenance(local *ClientLocalState, row RowIdentity, scope ScopeID) map[RowIdentity]struct{} {
	removed := make(map[RowIdentity]struct{})
	for index := 0; index < len(local.Provenance); index++ {
		entry := &local.Provenance[index]
		if entry.Row != row {
			continue
		}
		remaining := make([]ScopeID, 0, len(entry.Scopes))
		for _, candidate := range entry.Scopes {
			if candidate == scope {
				removed[row] = struct{}{}
				continue
			}
			remaining = append(remaining, candidate)
		}
		if len(remaining) == 0 {
			local.Provenance = append(local.Provenance[:index], local.Provenance[index+1:]...)
		} else {
			entry.Scopes = remaining
		}
		return removed
	}
	return removed
}

func decodePullRequest(payload json.RawMessage) (pullRequestPayload, error) {
	var request pullRequestPayload
	if err := decodeStrictPayload(payload, &request); err != nil {
		return pullRequestPayload{}, err
	}
	if !jsonObjectHasMember(payload, "scope_set_version") {
		return pullRequestPayload{}, fmt.Errorf("pull request scope_set_version is required")
	}
	if request.UserID == "" || request.ClientID == "" || request.ClientGeneration == 0 || request.ClientGeneration > maxReferenceJSONInteger {
		return pullRequestPayload{}, fmt.Errorf("pull request identity is invalid")
	}
	if request.ScopeSetVersion > maxReferenceJSONInteger || request.Limit == 0 {
		return pullRequestPayload{}, fmt.Errorf("pull request limit or scope-set version is invalid")
	}
	if request.Scopes == nil {
		return pullRequestPayload{}, fmt.Errorf("pull request scopes are required")
	}
	if _, _, err := decodeSchemaReference(request.Schema, false); err != nil {
		return pullRequestPayload{}, fmt.Errorf("pull request schema: %w", err)
	}
	for _, scope := range request.Scopes {
		if scope.ScopeID == "" || !validTokenSource(scope.CursorSource) {
			return pullRequestPayload{}, fmt.Errorf("pull request scope is invalid")
		}
	}
	return request, nil
}

func jsonObjectHasMember(payload json.RawMessage, member string) bool {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(payload, &object); err != nil {
		return false
	}
	_, found := object[member]
	return found
}

func validTokenSource(source tokenSource) bool {
	switch source {
	case tokenSourceNone, tokenSourceLocalCheckpoint, tokenSourceLocalRebuildContinuation, tokenSourceForged:
		return true
	default:
		return false
	}
}

func endpointClientGate(state State, client ClientKey, generation Generation, schema *schemaReferencePayload) (ClientState, *HTTPObservation) {
	clientState, exists := state.Clients[client]
	if !exists {
		return ClientState{}, endpointHTTPObservation(401, "auth_required", false)
	}
	if clientState.Retirement != nil {
		return ClientState{}, endpointHTTPObservation(409, "client_retired", false)
	}
	if generation == 0 || clientState.CurrentGeneration != generation {
		return ClientState{}, endpointHTTPObservation(409, "client_generation_expired", false)
	}
	requestSchema, fresh, err := decodeSchemaReference(schema, false)
	if err != nil || fresh || requestSchema != state.CurrentSchema {
		return ClientState{}, endpointHTTPObservation(422, "schema_mismatch", false)
	}
	return clientState, nil
}

func pullEndpointError(status int, code HTTPCode, retryable bool) StepResult {
	return StepResult{
		Kind: StepResultKindPull,
		HTTP: endpointHTTPObservation(status, code, retryable),
		Pull: &PullObservation{},
	}
}

func endpointHTTPObservation(status int, code HTTPCode, retryable bool) *HTTPObservation {
	return &HTTPObservation{
		Status:    status,
		HasCode:   code != "",
		Code:      code,
		Retryable: retryable,
	}
}

func referenceNow(clock Clock) time.Time {
	return clock.Now().Round(0).UTC()
}

func scopeAssignmentFor(client ClientState, scope ScopeID) (ScopeAssignment, bool) {
	for _, assignment := range client.ScopeAssignments {
		if assignment.Scope == scope {
			return assignment, true
		}
	}
	return ScopeAssignment{}, false
}

func sortedActiveAssignments(client ClientState) []ScopeAssignment {
	assignments := make([]ScopeAssignment, 0, len(client.ScopeAssignments))
	seen := make(map[ScopeID]struct{}, len(client.ScopeAssignments))
	for _, assignment := range client.ScopeAssignments {
		if !assignment.Assigned {
			continue
		}
		if _, duplicate := seen[assignment.Scope]; duplicate {
			return nil
		}
		seen[assignment.Scope] = struct{}{}
		assignments = append(assignments, assignment)
	}
	sort.Slice(assignments, func(left, right int) bool {
		return assignments[left].Scope < assignments[right].Scope
	})
	return assignments
}

func sortedPullScopes(scopes map[ScopeID]pullScopePayload) []ScopeID {
	result := make([]ScopeID, 0, len(scopes))
	for scope := range scopes {
		result = append(result, scope)
	}
	sortScopeIDs(result)
	return result
}

func sortedRuntimeScopes(runtimes map[ScopeID]*pullScopeRuntime) []ScopeID {
	result := make([]ScopeID, 0, len(runtimes))
	for scope := range runtimes {
		result = append(result, scope)
	}
	sortScopeIDs(result)
	return result
}

func hasPendingAcceptedFence(state State, client ClientKey) bool {
	for _, fence := range state.Fences {
		if !fence.HasMutationKey || fence.MutationKey.Client != client {
			continue
		}
		if !fenceCoverageSatisfied(state, fence) {
			return true
		}
	}
	return false
}

func fenceCoverageSatisfied(state State, fence VersionFence) bool {
	if fence.Coverage == FenceCoverageMaterialized {
		return true
	}
	if fence.Coverage != FenceCoverageResetBaseline || !fence.HasResetBaselineCoverage || state.Stream.Reset == nil {
		return false
	}
	reset := state.Stream.Reset
	coverage := fence.ResetBaselineCoverage
	return reset.Phase == StreamResetPhaseActive &&
		reset.HasCandidateStage &&
		reset.CandidateStage.Verified &&
		coverage.ResetID == reset.ID &&
		coverage.CandidateSlot == reset.CandidateSlot &&
		coverage.SnapshotBoundary == reset.SnapshotBoundary &&
		coverage.TargetStreamGeneration == reset.TargetStreamGeneration
}

func currentPullBoundary(state State) StreamPosition {
	boundary := state.Stream.Authority.GlobalMaterializationBoundary
	if boundary.StreamGeneration == "" {
		return StreamPosition{
			StreamGeneration: state.Stream.Authority.ActiveGeneration,
			Kind:             PositionKindGenerationStart,
		}
	}
	if boundary.Kind == PositionKindEffect {
		return StreamPosition{
			StreamGeneration: boundary.StreamGeneration,
			Kind:             PositionKindTransactionEnd,
			CommitLSN:        boundary.CommitLSN,
		}
	}
	return boundary
}

func eligiblePullCandidates(runtime *pullScopeRuntime, boundary StreamPosition) ([]pullCandidate, bool) {
	if runtime.state.StreamGeneration == "" || runtime.state.StreamGeneration != boundary.StreamGeneration {
		return nil, false
	}
	if !positionAtOrBefore(runtime.cursor, boundary) {
		return nil, false
	}

	deduplicated := make(map[scopedRowIdentity]pullCandidate)
	for _, effect := range runtime.state.Effects {
		if effect.Position.StreamGeneration != runtime.state.StreamGeneration || !positionAfter(effect.Position, runtime.cursor) || !positionAtOrBefore(effect.Position, boundary) {
			continue
		}
		if effect.Row.CanonicalIdentityBytes == "" || effect.Row.TableID == "" || effect.Row.PrimaryKeyFieldID == "" || effect.Row.PortableType == "" || !validEffectOperation(effect.Operation) {
			return nil, false
		}
		candidate := pullCandidate{scope: runtime.scope, effect: effect}
		key := scopedRowIdentity{Scope: runtime.scope, Row: effect.Row}
		previous, exists := deduplicated[key]
		if !exists || lessStreamPosition(previous.effect.Position, effect.Position) || previous.effect.Position == effect.Position && lessPullCandidate(previous, candidate) {
			deduplicated[key] = candidate
		}
	}

	result := make([]pullCandidate, 0, len(deduplicated))
	for _, candidate := range deduplicated {
		result = append(result, candidate)
	}
	sort.Slice(result, func(left, right int) bool {
		return lessPullCandidate(result[left], result[right])
	})
	return result, true
}

func validEffectOperation(operation EffectOperation) bool {
	return operation == EffectOperationDelete || operation == EffectOperationUpsert
}

func mergePullCandidates(streams map[ScopeID][]pullCandidate, limit int) ([]pullCandidate, bool) {
	indexes := make(map[ScopeID]int, len(streams))
	result := make([]pullCandidate, 0, limit)
	for len(result) < limit {
		var next pullCandidate
		hasNext := false
		for _, scope := range sortedCandidateScopes(streams) {
			index := indexes[scope]
			stream := streams[scope]
			if index >= len(stream) {
				continue
			}
			candidate := stream[index]
			if !hasNext || lessPullCandidate(candidate, next) {
				next = candidate
				hasNext = true
			}
		}
		if !hasNext {
			break
		}
		result = append(result, next)
		indexes[next.scope]++
	}
	for _, scope := range sortedCandidateScopes(streams) {
		if indexes[scope] < len(streams[scope]) {
			return result, true
		}
	}
	return result, false
}

func sortedCandidateScopes(streams map[ScopeID][]pullCandidate) []ScopeID {
	result := make([]ScopeID, 0, len(streams))
	for scope := range streams {
		result = append(result, scope)
	}
	sortScopeIDs(result)
	return result
}

func lessPullCandidate(left, right pullCandidate) bool {
	if left.effect.Position != right.effect.Position {
		return lessStreamPosition(left.effect.Position, right.effect.Position)
	}
	if left.scope != right.scope {
		return left.scope < right.scope
	}
	if left.effect.Row.TableID != right.effect.Row.TableID {
		return left.effect.Row.TableID < right.effect.Row.TableID
	}
	if left.effect.Row != right.effect.Row {
		return lessRowIdentity(left.effect.Row, right.effect.Row)
	}
	leftRank := effectOperationRank(left.effect.Operation)
	rightRank := effectOperationRank(right.effect.Operation)
	if leftRank != rightRank {
		return leftRank < rightRank
	}
	return lessEventReplayKey(left.effect.SourceEvent, right.effect.SourceEvent)
}

func hydratePullCandidate(state State, candidate pullCandidate) (PullChangeObservation, bool) {
	effect := candidate.effect
	if !effect.HasCapturedProjection || !effect.HasChecksum || effect.Version == "" {
		return PullChangeObservation{}, false
	}
	projection, found := state.Projections[effect.CapturedProjection]
	if !found || projection.Event != effect.SourceEvent || projection.Row != effect.Row || projection.Version != effect.Version || projection.Checksum != effect.Checksum {
		return PullChangeObservation{}, false
	}
	if effect.CapturedProjection.Event != effect.SourceEvent || !validEffectOperation(effect.Operation) {
		return PullChangeObservation{}, false
	}
	return PullChangeObservation{
		Scope:       candidate.scope,
		Row:         effect.Row,
		Operation:   effect.Operation,
		Version:     effect.Version,
		HasChecksum: true,
		Checksum:    effect.Checksum,
	}, true
}

func pullCursorPosition(runtime *pullScopeRuntime, boundary StreamPosition, hasMore bool) (StreamPosition, bool) {
	if !hasMore {
		return boundary, true
	}
	if len(runtime.selected) == 0 {
		if len(runtime.candidates) == 0 {
			return boundary, true
		}
		return StreamPosition{}, false
	}
	position := runtime.selected[0].effect.Position
	for _, candidate := range runtime.selected[1:] {
		if lessStreamPosition(position, candidate.effect.Position) {
			position = candidate.effect.Position
		}
	}
	return position, true
}

func positionAfter(position, cursor StreamPosition) bool {
	return position.StreamGeneration == cursor.StreamGeneration && lessStreamPosition(cursor, position)
}

func positionAtOrBefore(position, boundary StreamPosition) bool {
	return position.StreamGeneration == boundary.StreamGeneration && (position == boundary || lessStreamPosition(position, boundary))
}

func incrementalCursorBindings(state State, client ClientKey, assignment ScopeAssignment, scopeState ScopeState, scope ScopeID, position StreamPosition, issuedAt time.Time) BindingSet {
	return BindingSet{
		HasUser:                 true,
		User:                    client.UserID,
		HasClient:               true,
		Client:                  client,
		HasClientGeneration:     true,
		ClientGeneration:        currentClientGeneration(state, client),
		HasRegistryGeneration:   true,
		RegistryGeneration:      state.Registry.CurrentGeneration,
		HasMembershipGeneration: true,
		MembershipGeneration:    assignment.MembershipGeneration,
		HasRetentionGeneration:  true,
		RetentionGeneration:     assignment.RetentionGeneration,
		HasStreamGeneration:     true,
		StreamGeneration:        scopeState.StreamGeneration,
		HasSchema:               true,
		Schema:                  state.CurrentSchema,
		HasScope:                true,
		Scope:                   scope,
		HasStreamPosition:       true,
		StreamPosition:          position,
		HasIssuedAt:             true,
		IssuedAt:                issuedAt,
	}
}

func currentClientGeneration(state State, client ClientKey) Generation {
	return state.Clients[client].CurrentGeneration
}

func validateIncrementalCursor(model *Model, client ClientKey, assignment ScopeAssignment, scopeState ScopeState, scope ScopeID, token OpaqueToken, now time.Time) (TokenStatus, StreamPosition) {
	stored, found := tokenBindings(model.authority, token)
	position := StreamPosition{}
	if found && stored.HasStreamPosition {
		position = stored.StreamPosition
	}
	current := incrementalCursorBindings(model.state, client, assignment, scopeState, scope, position, time.Time{})
	current.HasIssuedAt = false
	status := validateTokenAgainstCurrent(model.authority, token, string(TokenKindIncrementalCursor), current, now)
	if status != TokenStatusValid {
		return status, position
	}
	if !stored.HasStreamPosition || !positionAtOrBefore(position, currentPullBoundary(model.state)) {
		return TokenStatusStale, position
	}
	if floor, hasFloor := model.state.RetentionFloors[scope]; hasFloor {
		if floor.StreamGeneration != scopeState.StreamGeneration || floor.MembershipGeneration != assignment.MembershipGeneration || floor.RetentionGeneration != assignment.RetentionGeneration || !positionAtOrBefore(floor.Position, position) {
			return TokenStatusStale, position
		}
	}
	return TokenStatusValid, position
}

func tokenBindings(authority TokenAuthority, token OpaqueToken) (BindingSet, bool) {
	concrete, ok := authority.(*tokenAuthority)
	if !ok || concrete == nil {
		return BindingSet{}, false
	}
	concrete.mu.RLock()
	record, found := concrete.minted[token]
	concrete.mu.RUnlock()
	if !found {
		return BindingSet{}, false
	}
	return record.bindings, true
}

func resolveModelToken(model *Model, client ClientKey, scope ScopeID, rebuild RebuildID, source tokenSource) (OpaqueToken, bool) {
	switch source {
	case tokenSourceForged:
		return OpaqueToken{namespace: math.MaxUint64, sequence: 1}, true
	case tokenSourceLocalCheckpoint:
		if local, found := model.state.ClientLocal[client]; found {
			for _, checkpoint := range local.ScopeCheckpoints {
				if checkpoint.Scope == scope && checkpoint.HasCursor {
					return checkpoint.Cursor, true
				}
			}
		}
		if server, found := model.state.Clients[client]; found {
			for _, checkpoint := range server.Checkpoints {
				if checkpoint.Scope == scope && checkpoint.HasCursor {
					return checkpoint.Cursor, true
				}
			}
		}
		return OpaqueToken{}, false
	case tokenSourceLocalRebuildContinuation:
		if local, found := model.state.ClientLocal[client]; found {
			var token OpaqueToken
			foundToken := false
			for _, attempt := range local.RebuildAttempts {
				if attempt.Scope != scope || !attempt.HasContinuation || rebuild != "" && attempt.Rebuild != rebuild {
					continue
				}
				if foundToken {
					return OpaqueToken{}, false
				}
				token = attempt.Continuation
				foundToken = true
			}
			if foundToken {
				return token, true
			}
		}
		if rebuild != "" {
			key := RebuildKey{Client: client, Scope: scope, Rebuild: rebuild}
			if session, found := model.state.Rebuilds[key]; found && session.HasContinuation {
				return session.Continuation, true
			}
		}
		return OpaqueToken{}, false
	case tokenSourceNone:
		return OpaqueToken{}, false
	default:
		return OpaqueToken{}, false
	}
}

func canMintTokens(authority TokenAuthority, count int) bool {
	if count < 0 {
		return false
	}
	concrete, ok := authority.(*tokenAuthority)
	if !ok || concrete == nil {
		return false
	}
	concrete.mu.RLock()
	defer concrete.mu.RUnlock()
	return uint64(count) <= math.MaxUint64-concrete.next
}

func acknowledgeServerCursor(client *ClientState, scope ScopeID, position StreamPosition, now time.Time) bool {
	index := clientCheckpointIndex(client.Checkpoints, scope)
	if index >= 0 {
		checkpoint := &client.Checkpoints[index]
		if checkpoint.Position.StreamGeneration != "" && !lessStreamPosition(checkpoint.Position, position) {
			return false
		}
		checkpoint.Position = position
	} else {
		client.Checkpoints = append(client.Checkpoints, ClientCheckpoint{Scope: scope, Position: position})
	}
	for index := range client.Generations {
		if client.Generations[index].Generation == client.CurrentGeneration {
			acknowledgedAt := now
			client.Generations[index].LastCursorAcknowledgedAt = &acknowledgedAt
			break
		}
	}
	return true
}

func completedRebuildFinalCursor(model *Model, client ClientKey, assignment ScopeAssignment, scopeState ScopeState, scope ScopeID, token OpaqueToken, now time.Time) bool {
	for key, session := range model.state.Rebuilds {
		if key.Client != client || key.Scope != scope || session.Status != RebuildStatusComplete || !session.HasFinalCursor || session.FinalCursor != token {
			continue
		}
		clientState, found := model.state.Clients[client]
		return found && rebuildSessionCurrent(model.state, clientState, assignment, scopeState, session, now)
	}
	return false
}

func clearAcknowledgedRebuildRequirement(client *ClientState, scope ScopeID) bool {
	index, found := findScopeAssignment(client.ScopeAssignments, scope)
	if !found || !client.ScopeAssignments[index].Assigned || !client.ScopeAssignments[index].RebuildRequired {
		return false
	}
	client.ScopeAssignments[index].RebuildRequired = false
	return true
}

func storeIssuedServerCursor(client *ClientState, scope ScopeID, token OpaqueToken) {
	index := clientCheckpointIndex(client.Checkpoints, scope)
	if index < 0 {
		client.Checkpoints = append(client.Checkpoints, ClientCheckpoint{Scope: scope, HasCursor: true, Cursor: token})
		return
	}
	client.Checkpoints[index].HasCursor = true
	client.Checkpoints[index].Cursor = token
}

func clientCheckpointIndex(checkpoints []ClientCheckpoint, scope ScopeID) int {
	for index := range checkpoints {
		if checkpoints[index].Scope == scope {
			return index
		}
	}
	return -1
}
