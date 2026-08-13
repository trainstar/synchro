package reference

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const maxProtocolCounter uint64 = 9007199254740991

type startSyncPayload struct {
	UserID   *string `json:"user_id"`
	ClientID *string `json:"client_id"`
}

type stopSyncPayload struct {
	UserID   *string `json:"user_id"`
	ClientID *string `json:"client_id"`
}

type recoveryAction string

const (
	recoveryActionRetry      recoveryAction = "retry"
	recoveryActionRemediated recoveryAction = "remediated"
)

type recoverErrorPayload struct {
	UserID   *string         `json:"user_id"`
	ClientID *string         `json:"client_id"`
	Action   *recoveryAction `json:"action"`
}

type restartClientPayload struct {
	UserID   *string `json:"user_id"`
	ClientID *string `json:"client_id"`
}

type retireClientPayload struct {
	UserID   *string `json:"user_id"`
	ClientID *string `json:"client_id"`
}

func init() {
	registerOperation("process/restart-client", restartClient)
}

func startSync(_ context.Context, model *Model, payload json.RawMessage) (StepResult, error) {
	var request startSyncPayload
	if err := decodeStrictPayload(payload, &request); err != nil {
		return StepResult{}, fmt.Errorf("decode start-sync payload: %w", err)
	}
	client, err := decodeClientKey(request.UserID, request.ClientID)
	if err != nil {
		return StepResult{}, fmt.Errorf("validate start-sync payload: %w", err)
	}

	local, found := model.state.ClientLocal[client]
	if !found {
		local = ClientLocalState{Lifecycle: ClientLifecycleState{State: ClientLifecycleUninitialized}}
	}
	prior, err := transitionClientLifecycle(model, &local, ClientLifecycleLocalReady)
	if err != nil {
		return StepResult{}, fmt.Errorf("start sync for client: %w", err)
	}
	model.state.ClientLocal[client] = local
	appendClientEvent(model, ModelEventRestart, client, "")

	return lifecycleResult(client, prior, ClientLifecycleLocalReady), nil
}

func stopSync(_ context.Context, model *Model, payload json.RawMessage) (StepResult, error) {
	var request stopSyncPayload
	if err := decodeStrictPayload(payload, &request); err != nil {
		return StepResult{}, fmt.Errorf("decode stop-sync payload: %w", err)
	}
	client, err := decodeClientKey(request.UserID, request.ClientID)
	if err != nil {
		return StepResult{}, fmt.Errorf("validate stop-sync payload: %w", err)
	}

	local, found := model.state.ClientLocal[client]
	if !found {
		local = ClientLocalState{Lifecycle: ClientLifecycleState{State: ClientLifecycleUninitialized}}
	}
	prior, err := transitionClientLifecycle(model, &local, ClientLifecycleStopped)
	if err != nil {
		return StepResult{}, fmt.Errorf("stop sync for client: %w", err)
	}
	model.state.ClientLocal[client] = local
	appendClientEvent(model, ModelEventBackgrounding, client, "")

	return lifecycleResult(client, prior, ClientLifecycleStopped), nil
}

func recoverError(_ context.Context, model *Model, payload json.RawMessage) (StepResult, error) {
	var request recoverErrorPayload
	if err := decodeStrictPayload(payload, &request); err != nil {
		return StepResult{}, fmt.Errorf("decode recover-error payload: %w", err)
	}
	client, err := decodeClientKey(request.UserID, request.ClientID)
	if err != nil {
		return StepResult{}, fmt.Errorf("validate recover-error payload: %w", err)
	}
	if request.Action == nil {
		return StepResult{}, errors.New("validate recover-error payload: action is required")
	}
	if *request.Action != recoveryActionRetry && *request.Action != recoveryActionRemediated {
		return StepResult{}, errors.New("validate recover-error payload: action is unknown")
	}

	local, found := model.state.ClientLocal[client]
	if !found || local.Lifecycle.State != ClientLifecycleError || local.ErrorState == nil {
		return StepResult{}, errors.New("recover error requires a durable error state")
	}
	if *request.Action == recoveryActionRetry && !local.ErrorState.Retryable {
		return StepResult{}, errors.New("recover error requires remediation for a non-retryable error")
	}
	local.ErrorState.Acknowledged = true
	prior, err := transitionClientLifecycle(model, &local, ClientLifecycleLocalReady)
	if err != nil {
		return StepResult{}, fmt.Errorf("recover client error: %w", err)
	}
	model.state.ClientLocal[client] = local
	appendClientEvent(model, ModelEventRecovery, client, local.ErrorState.Reason)

	return lifecycleResult(client, prior, ClientLifecycleLocalReady), nil
}

func restartClient(_ context.Context, model *Model, payload json.RawMessage) (StepResult, error) {
	var request restartClientPayload
	if err := decodeStrictPayload(payload, &request); err != nil {
		return StepResult{}, fmt.Errorf("decode restart-client payload: %w", err)
	}
	client, err := decodeClientKey(request.UserID, request.ClientID)
	if err != nil {
		return StepResult{}, fmt.Errorf("validate restart-client payload: %w", err)
	}

	local, found := model.state.ClientLocal[client]
	if !found {
		local = ClientLocalState{Lifecycle: ClientLifecycleState{State: ClientLifecycleUninitialized}}
	}
	if !knownClientLifecycle(local.Lifecycle.State) {
		return StepResult{}, errors.New("restart client has an unknown lifecycle")
	}

	appendClientEvent(model, ModelEventProcessDeath, client, "")
	local.Lifecycle = ClientLifecycleState{
		State:     ClientLifecycleUninitialized,
		ChangedAt: modelNow(model),
	}

	next := ClientLifecycleLocalReady
	if local.ErrorState != nil && !local.ErrorState.Acknowledged {
		next = ClientLifecycleError
	}
	if _, err := transitionClientLifecycle(model, &local, next); err != nil {
		return StepResult{}, fmt.Errorf("restart client lifecycle: %w", err)
	}
	model.state.ClientLocal[client] = local
	appendClientEvent(model, ModelEventRestart, client, "")

	return lifecycleResult(client, ClientLifecycleUninitialized, next), nil
}

func retireClient(_ context.Context, model *Model, payload json.RawMessage) (StepResult, error) {
	var request retireClientPayload
	if err := decodeStrictPayload(payload, &request); err != nil {
		return StepResult{}, fmt.Errorf("decode retire-client payload: %w", err)
	}
	clientKey, err := decodeClientKey(request.UserID, request.ClientID)
	if err != nil {
		return StepResult{}, fmt.Errorf("validate retire-client payload: %w", err)
	}

	client, found := model.state.Clients[clientKey]
	if !found {
		return StepResult{}, errors.New("retire client requires an existing client")
	}
	if client.Retirement != nil {
		return StepResult{}, errors.New("client is already retired")
	}
	priorGeneration := client.CurrentGeneration
	client.Retirement = &PermanentRetirement{
		RetiredAt: modelNow(model),
		Reason:    ReasonCode("retired"),
	}
	model.state.Clients[clientKey] = client

	return StepResult{
		Kind: StepResultKindClient,
		Client: &ClientObservation{
			Client:               clientKey,
			PriorGeneration:      priorGeneration,
			NewGeneration:        client.CurrentGeneration,
			PriorScopeSetVersion: client.ScopeSetVersion,
			NewScopeSetVersion:   client.ScopeSetVersion,
		},
	}, nil
}

func decodeClientKey(userID, clientID *string) (ClientKey, error) {
	if userID == nil || *userID == "" {
		return ClientKey{}, errors.New("user_id is required")
	}
	if clientID == nil || *clientID == "" {
		return ClientKey{}, errors.New("client_id is required")
	}
	return ClientKey{UserID: UserID(*userID), ClientID: ClientID(*clientID)}, nil
}

func lifecycleResult(client ClientKey, prior, next ClientLifecycle) StepResult {
	return StepResult{
		Kind: StepResultKindLifecycle,
		Lifecycle: &LifecycleObservation{
			Client: client,
			Prior:  prior,
			Next:   next,
		},
	}
}

func modelNow(model *Model) *time.Time {
	now := model.clock.Now().Round(0).UTC()
	return &now
}

func appendClientEvent(model *Model, kind ModelEventKind, client ClientKey, reason ReasonCode) {
	ordinal := uint64(0)
	for _, event := range model.state.Events {
		if event.Ordinal > ordinal {
			ordinal = event.Ordinal
		}
	}
	if ordinal == maxProtocolCounter {
		return
	}
	model.state.Events = append(model.state.Events, ModelEvent{
		Ordinal:   ordinal + 1,
		Kind:      kind,
		At:        modelNow(model),
		HasClient: true,
		Client:    client,
		Reason:    reason,
	})
}

func knownClientLifecycle(state ClientLifecycle) bool {
	switch state {
	case ClientLifecycleUninitialized,
		ClientLifecycleLocalReady,
		ClientLifecycleConnecting,
		ClientLifecycleSchemaApplying,
		ClientLifecycleReady,
		ClientLifecyclePushing,
		ClientLifecyclePulling,
		ClientLifecycleRebuilding,
		ClientLifecycleBackoff,
		ClientLifecycleError,
		ClientLifecycleStopped:
		return true
	default:
		return false
	}
}

func lifecycleTransitionAllowed(from, to ClientLifecycle) bool {
	switch from {
	case ClientLifecycleUninitialized:
		return to == ClientLifecycleLocalReady || to == ClientLifecycleError || to == ClientLifecycleStopped
	case ClientLifecycleLocalReady:
		return to == ClientLifecycleConnecting || to == ClientLifecycleError || to == ClientLifecycleStopped
	case ClientLifecycleConnecting:
		return to == ClientLifecycleSchemaApplying || to == ClientLifecycleReady || to == ClientLifecycleBackoff || to == ClientLifecycleError || to == ClientLifecycleStopped
	case ClientLifecycleSchemaApplying:
		return to == ClientLifecycleReady || to == ClientLifecycleRebuilding || to == ClientLifecycleError || to == ClientLifecycleStopped
	case ClientLifecycleReady:
		return to == ClientLifecycleConnecting || to == ClientLifecyclePushing || to == ClientLifecyclePulling || to == ClientLifecycleRebuilding || to == ClientLifecycleError || to == ClientLifecycleStopped
	case ClientLifecyclePushing:
		return to == ClientLifecyclePushing || to == ClientLifecycleReady || to == ClientLifecyclePulling || to == ClientLifecycleConnecting || to == ClientLifecycleBackoff || to == ClientLifecycleError || to == ClientLifecycleStopped
	case ClientLifecyclePulling:
		return to == ClientLifecyclePulling || to == ClientLifecycleReady || to == ClientLifecycleRebuilding || to == ClientLifecycleConnecting || to == ClientLifecycleBackoff || to == ClientLifecycleError || to == ClientLifecycleStopped
	case ClientLifecycleRebuilding:
		return to == ClientLifecycleRebuilding || to == ClientLifecycleReady || to == ClientLifecycleConnecting || to == ClientLifecycleBackoff || to == ClientLifecycleError || to == ClientLifecycleStopped
	case ClientLifecycleBackoff:
		return to == ClientLifecycleConnecting || to == ClientLifecyclePushing || to == ClientLifecyclePulling || to == ClientLifecycleRebuilding || to == ClientLifecycleError || to == ClientLifecycleStopped
	case ClientLifecycleError:
		return to == ClientLifecycleLocalReady || to == ClientLifecycleStopped
	case ClientLifecycleStopped:
		return to == ClientLifecycleLocalReady
	default:
		return false
	}
}

func transitionClientLifecycle(model *Model, local *ClientLocalState, next ClientLifecycle) (ClientLifecycle, error) {
	if local == nil {
		return "", errors.New("local state is required")
	}
	prior := local.Lifecycle.State
	if !knownClientLifecycle(prior) {
		return "", fmt.Errorf("unknown lifecycle state %q", prior)
	}
	if !knownClientLifecycle(next) {
		return "", fmt.Errorf("unknown lifecycle state %q", next)
	}
	if !lifecycleTransitionAllowed(prior, next) {
		return "", fmt.Errorf("illegal lifecycle transition %q to %q", prior, next)
	}
	local.Lifecycle = ClientLifecycleState{State: next, ChangedAt: modelNow(model)}
	return prior, nil
}

func transitionLocalToError(model *Model, local *ClientLocalState, reason ReasonCode, retryable bool) error {
	if local == nil {
		return errors.New("local state is required")
	}
	if local.Lifecycle.State != ClientLifecycleError {
		if _, err := transitionClientLifecycle(model, local, ClientLifecycleError); err != nil {
			return err
		}
	}
	local.ErrorState = &ClientErrorState{
		Reason:    reason,
		Retryable: retryable,
		At:        modelNow(model),
	}
	return nil
}

func currentClientGenerationIndex(client ClientState) int {
	for index := range client.Generations {
		if client.Generations[index].Generation == client.CurrentGeneration {
			return index
		}
	}
	return -1
}

func nextClientGeneration(client ClientState) (Generation, error) {
	maximum := client.CurrentGeneration
	for _, generation := range client.Generations {
		if generation.Generation > maximum {
			maximum = generation.Generation
		}
	}
	if uint64(maximum) >= maxProtocolCounter {
		return 0, errors.New("client generation allocation exceeds the protocol limit")
	}
	return maximum + 1, nil
}

func clientGenerationExpired(model *Model, client ClientState, now time.Time) (bool, error) {
	if client.CurrentGeneration == 0 {
		return false, nil
	}
	index := currentClientGenerationIndex(client)
	if index < 0 {
		return false, errors.New("current client generation is missing from history")
	}
	generation := client.Generations[index]
	if generation.ExpiresAt != nil {
		return true, nil
	}
	if model.state.Installation.StaleClientIntervalMilliseconds == 0 {
		return false, errors.New("stale client interval must be positive")
	}
	activity := generation.CreatedAt
	if generation.LastCursorAcknowledgedAt != nil {
		activity = generation.LastCursorAcknowledgedAt
	}
	if activity == nil {
		return false, errors.New("current client generation has no activity time")
	}
	maximumMilliseconds := uint64((time.Duration(1<<63 - 1)) / time.Millisecond)
	if model.state.Installation.StaleClientIntervalMilliseconds > maximumMilliseconds {
		return false, errors.New("stale client interval exceeds duration range")
	}
	deadline := activity.Add(time.Duration(model.state.Installation.StaleClientIntervalMilliseconds) * time.Millisecond)
	return !now.Before(deadline), nil
}

func expireCurrentClientGeneration(model *Model, clientKey ClientKey, now time.Time) (ClientState, bool, error) {
	client, found := model.state.Clients[clientKey]
	if !found || client.CurrentGeneration == 0 || client.Retirement != nil {
		return client, false, nil
	}
	expired, err := clientGenerationExpired(model, client, now)
	if err != nil {
		return ClientState{}, false, err
	}
	if !expired {
		return client, false, nil
	}
	index := currentClientGenerationIndex(client)
	if index < 0 {
		return ClientState{}, false, errors.New("current client generation is missing from history")
	}
	if client.Generations[index].ExpiresAt == nil {
		at := now.Round(0).UTC()
		client.Generations[index].ExpiresAt = &at
		model.state.Clients[clientKey] = client
	}
	return client, true, nil
}

func renewClientGeneration(model *Model, clientKey ClientKey, client *ClientState, local *ClientLocalState) (Generation, error) {
	if client == nil {
		return 0, errors.New("client state is required")
	}
	next, err := nextClientGeneration(*client)
	if err != nil {
		return 0, err
	}
	client.CurrentGeneration = next
	client.Generations = append(client.Generations, ClientGenerationState{
		Generation: next,
		CreatedAt:  modelNow(model),
	})
	for index := range client.ScopeAssignments {
		if client.ScopeAssignments[index].Assigned {
			client.ScopeAssignments[index].RebuildRequired = true
		}
	}
	client.Checkpoints = nil

	if local != nil {
		local.ClientGeneration = next
		for index := range local.ScopeAssignments {
			if !local.ScopeAssignments[index].Assigned {
				continue
			}
			local.ScopeAssignments[index].RebuildRequired = true
			invalidateLocalScopeCheckpoint(local, local.ScopeAssignments[index].Scope)
		}
		for index := range local.SealedBatches {
			batch := &local.SealedBatches[index]
			if batch.ClientGeneration != next && batch.State != LocalSealedBatchStateReconciled {
				batch.State = LocalSealedBatchStateAbandonedGeneration
			}
		}
	}
	return next, nil
}

func findScopeAssignment(assignments []ScopeAssignment, scope ScopeID) (int, bool) {
	for index := range assignments {
		if assignments[index].Scope == scope {
			return index, true
		}
	}
	return 0, false
}

func findLocalScopeAssignment(assignments []LocalScopeAssignment, scope ScopeID) (int, bool) {
	for index := range assignments {
		if assignments[index].Scope == scope {
			return index, true
		}
	}
	return 0, false
}

func findClientCheckpoint(checkpoints []ClientCheckpoint, scope ScopeID) (int, bool) {
	for index := range checkpoints {
		if checkpoints[index].Scope == scope {
			return index, true
		}
	}
	return 0, false
}

func findLocalScopeCheckpoint(checkpoints []LocalScopeCheckpoint, scope ScopeID) (int, bool) {
	for index := range checkpoints {
		if checkpoints[index].Scope == scope {
			return index, true
		}
	}
	return 0, false
}

func invalidateLocalScopeCheckpoint(local *ClientLocalState, scope ScopeID) {
	if local == nil {
		return
	}
	index, found := findLocalScopeCheckpoint(local.ScopeCheckpoints, scope)
	if !found {
		return
	}
	local.ScopeCheckpoints[index].Position = StreamPosition{}
	local.ScopeCheckpoints[index].HasCursor = false
	local.ScopeCheckpoints[index].Cursor = OpaqueToken{}
	local.ScopeCheckpoints[index].HasChecksum = false
	local.ScopeCheckpoints[index].Checksum = Checksum{}
	local.ScopeCheckpoints[index].Verified = false
}

func installLocalScopeCursor(local *ClientLocalState, scope ScopeID, position StreamPosition, cursor OpaqueToken) error {
	if local == nil {
		return errors.New("local state is required")
	}
	index, found := findLocalScopeCheckpoint(local.ScopeCheckpoints, scope)
	if !found {
		return errors.New("scope cursor replacement requires an existing local checkpoint")
	}
	checkpoint := &local.ScopeCheckpoints[index]
	checkpoint.Position = position
	checkpoint.HasCursor = true
	checkpoint.Cursor = cursor
	checkpoint.HasChecksum = false
	checkpoint.Checksum = Checksum{}
	checkpoint.Verified = false
	return nil
}

func localScopeHasUnresolvedIntent(local ClientLocalState, row RowIdentity) bool {
	for _, mutation := range local.DurableQueue {
		if mutation.Row != row {
			continue
		}
		switch mutation.Status {
		case LocalMutationStatusAccepted,
			LocalMutationStatusServerRejected,
			LocalMutationStatusSupersededBeforeSend,
			LocalMutationStatusCancelledBeforeSend:
			continue
		default:
			return true
		}
	}
	return false
}

func removeLocalScopeState(local *ClientLocalState, scope ScopeID) {
	if local == nil {
		return
	}
	if index, found := findLocalScopeAssignment(local.ScopeAssignments, scope); found {
		local.ScopeAssignments = append(local.ScopeAssignments[:index], local.ScopeAssignments[index+1:]...)
	}
	if index, found := findLocalScopeCheckpoint(local.ScopeCheckpoints, scope); found {
		local.ScopeCheckpoints = append(local.ScopeCheckpoints[:index], local.ScopeCheckpoints[index+1:]...)
	}
	for index := 0; index < len(local.SeedReceipts); {
		if local.SeedReceipts[index].Scope == scope {
			local.SeedReceipts = append(local.SeedReceipts[:index], local.SeedReceipts[index+1:]...)
			continue
		}
		index++
	}

	for index := 0; index < len(local.Provenance); {
		provenance := &local.Provenance[index]
		for scopeIndex := 0; scopeIndex < len(provenance.Scopes); {
			if provenance.Scopes[scopeIndex] == scope {
				provenance.Scopes = append(provenance.Scopes[:scopeIndex], provenance.Scopes[scopeIndex+1:]...)
				continue
			}
			scopeIndex++
		}
		if len(provenance.Scopes) != 0 || localScopeHasUnresolvedIntent(*local, provenance.Row) {
			index++
			continue
		}
		for rowIndex := 0; rowIndex < len(local.Rows); rowIndex++ {
			if local.Rows[rowIndex].Identity == provenance.Row {
				local.Rows = append(local.Rows[:rowIndex], local.Rows[rowIndex+1:]...)
				break
			}
		}
		local.Provenance = append(local.Provenance[:index], local.Provenance[index+1:]...)
	}
}

func clientHasSendableIntent(local ClientLocalState) bool {
	for _, mutation := range local.DurableQueue {
		switch mutation.Status {
		case LocalMutationStatusPending, LocalMutationStatusSealed:
			return true
		}
	}
	return false
}
