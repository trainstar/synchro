package reference

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

type expireClientGenerationPayload struct {
	UserID   *string `json:"user_id"`
	ClientID *string `json:"client_id"`
}

type compactScopePayload struct {
	ScopeID   *string `json:"scope_id"`
	BatchSize *uint64 `json:"batch_size"`
}

type invalidCompactionLimitError struct {
	value   uint64
	maximum uint64
}

func (e invalidCompactionLimitError) Error() string {
	return fmt.Sprintf("compact-scope batch_size %d must be between 1 and %d", e.value, e.maximum)
}

func (invalidCompactionLimitError) ErrorCode() string {
	return "invalid_limit"
}

func init() {
	registerOperation("model/expire-client-generation", expireClientGeneration)
	registerOperation("model/compact-scope", compactScope)
}

func expireClientGeneration(_ context.Context, model *Model, payload json.RawMessage) (StepResult, error) {
	var request expireClientGenerationPayload
	if err := decodeStrictPayload(payload, &request); err != nil {
		return StepResult{}, fmt.Errorf("decode expire-client-generation payload: %w", err)
	}
	clientKey, err := decodeClientKey(request.UserID, request.ClientID)
	if err != nil {
		return StepResult{}, fmt.Errorf("validate expire-client-generation payload: %w", err)
	}
	before, found := model.state.Clients[clientKey]
	if !found {
		return StepResult{}, errors.New("expire-client-generation requires an existing client")
	}
	if before.Retirement != nil {
		return StepResult{}, errors.New("retired clients have no renewable generation")
	}
	after, _, err := expireCurrentClientGeneration(model, clientKey, model.clock.Now().Round(0).UTC())
	if err != nil {
		return StepResult{}, fmt.Errorf("expire client generation: %w", err)
	}

	return StepResult{
		Kind: StepResultKindClient,
		Client: &ClientObservation{
			Client:               clientKey,
			PriorGeneration:      before.CurrentGeneration,
			NewGeneration:        after.CurrentGeneration,
			PriorScopeSetVersion: before.ScopeSetVersion,
			NewScopeSetVersion:   after.ScopeSetVersion,
		},
	}, nil
}

func compactScope(_ context.Context, model *Model, payload json.RawMessage) (StepResult, error) {
	var request compactScopePayload
	if err := decodeStrictPayload(payload, &request); err != nil {
		return StepResult{}, fmt.Errorf("decode compact-scope payload: %w", err)
	}
	batchSize := uint64(0)
	if request.BatchSize != nil {
		batchSize = *request.BatchSize
	}
	maximumBatchSize := model.state.ConfiguredLimits.CompactionBatchMaximum
	if batchSize == 0 || batchSize > maximumBatchSize {
		return StepResult{}, invalidCompactionLimitError{value: batchSize, maximum: maximumBatchSize}
	}
	if request.ScopeID == nil || *request.ScopeID == "" {
		return StepResult{}, errors.New("validate compact-scope payload: scope_id is required")
	}
	scopeID := ScopeID(*request.ScopeID)
	scope, found := model.state.Scopes[scopeID]
	if !found {
		return StepResult{}, errors.New("compact-scope requires an authoritative scope")
	}
	if scope.MembershipGeneration == 0 || scope.RetentionGeneration == 0 || scope.StreamGeneration == "" {
		return StepResult{}, errors.New("compact-scope requires complete scope lineage")
	}

	now := model.clock.Now().Round(0).UTC()
	prior, hasPriorFloor := model.state.RetentionFloors[scopeID]
	if hasPriorFloor && !retentionFloorMatchesScope(prior, scope) {
		return StepResult{}, errors.New("retention floor has an obsolete scope lineage")
	}

	safe, foundCheckpoint, err := compactableCheckpointMinimum(model, scopeID, scope, now)
	if err != nil {
		return StepResult{}, err
	}
	pinnedPosition, pinned, err := activeRebuildPinMinimum(model, scopeID, scope, now)
	if err != nil {
		return StepResult{}, err
	}
	if pinned && (!foundCheckpoint || lessStreamPosition(pinnedPosition, safe)) {
		safe = pinnedPosition
		foundCheckpoint = true
	}
	if !foundCheckpoint {
		safe = scope.HighWatermark
	}
	if safe.StreamGeneration != "" && safe.StreamGeneration != scope.StreamGeneration {
		return StepResult{}, errors.New("compaction safe position has another stream generation")
	}
	if hasPriorFloor && lessStreamPosition(safe, prior.Position) {
		safe = prior.Position
	}

	newFloor := RetentionFloor{
		MembershipGeneration: scope.MembershipGeneration,
		RetentionGeneration:  scope.RetentionGeneration,
		StreamGeneration:     scope.StreamGeneration,
		Position:             safe,
	}
	if hasPriorFloor {
		newFloor.ExpiresAt = cloneTime(prior.ExpiresAt)
	}
	eligible := make([]int, 0, len(scope.Effects))
	for index, effect := range scope.Effects {
		if effect.Position.StreamGeneration != scope.StreamGeneration {
			return StepResult{}, errors.New("scope effect has another stream generation")
		}
		if !lessStreamPosition(newFloor.Position, effect.Position) {
			eligible = append(eligible, index)
		}
	}
	sort.SliceStable(eligible, func(left, right int) bool {
		return lessStreamPosition(scope.Effects[eligible[left]].Position, scope.Effects[eligible[right]].Position)
	})
	deleteCount := len(eligible)
	if uint64(deleteCount) > batchSize {
		deleteCount = int(batchSize)
		newFloor.Position = scope.Effects[eligible[deleteCount-1]].Position
	}
	deletedEffects := make([]bool, len(scope.Effects))
	for _, index := range eligible[:deleteCount] {
		deletedEffects[index] = true
	}
	remaining := make([]ScopeEffect, 0, len(scope.Effects)-deleteCount)
	deleted := uint64(0)
	for index, effect := range scope.Effects {
		if deletedEffects[index] {
			deleted++
			continue
		}
		remaining = append(remaining, effect)
	}

	// The model transaction commits this effect deletion and floor update together.
	scope.Effects = remaining
	model.state.Scopes[scopeID] = scope
	model.state.RetentionFloors[scopeID] = newFloor

	return StepResult{
		Kind: StepResultKindRetention,
		Retention: &RetentionObservation{
			Scope:        scopeID,
			PriorFloor:   prior,
			NewFloor:     newFloor,
			BatchSize:    batchSize,
			DeletedCount: deleted,
			Pinned:       pinned,
		},
	}, nil
}

func compactableCheckpointMinimum(model *Model, scopeID ScopeID, scope ScopeState, now time.Time) (StreamPosition, bool, error) {
	var minimum StreamPosition
	found := false
	for clientKey, client := range model.state.Clients {
		if client.Retirement != nil || client.CurrentGeneration == 0 {
			continue
		}
		updated, expired, err := expireCurrentClientGeneration(model, clientKey, now)
		if err != nil {
			return StreamPosition{}, false, fmt.Errorf("expire client %q during compaction: %w", clientKey.ClientID, err)
		}
		if expired {
			continue
		}
		assignmentIndex, assigned := findScopeAssignment(updated.ScopeAssignments, scopeID)
		if !assigned {
			continue
		}
		assignment := updated.ScopeAssignments[assignmentIndex]
		if !assignment.Assigned || assignment.RebuildRequired || assignment.MembershipGeneration != scope.MembershipGeneration || assignment.RetentionGeneration != scope.RetentionGeneration {
			continue
		}
		checkpointIndex, hasCheckpoint := findClientCheckpoint(updated.Checkpoints, scopeID)
		if !hasCheckpoint {
			continue
		}
		checkpoint := updated.Checkpoints[checkpointIndex]
		if checkpoint.Position.StreamGeneration == "" || checkpoint.Position.StreamGeneration != scope.StreamGeneration {
			continue
		}
		if !found || lessStreamPosition(checkpoint.Position, minimum) {
			minimum = checkpoint.Position
			found = true
		}
	}
	return minimum, found, nil
}

func activeRebuildPinMinimum(model *Model, scopeID ScopeID, scope ScopeState, now time.Time) (StreamPosition, bool, error) {
	var minimum StreamPosition
	found := false
	for key, session := range model.state.Rebuilds {
		if key.Scope != scopeID {
			continue
		}
		if session.Status != RebuildStatusStaged {
			continue
		}
		if session.ExpiresAt != nil && !now.Before(session.ExpiresAt.Round(0).UTC()) {
			session.Status = RebuildStatusExpired
			model.state.Rebuilds[key] = session
			continue
		}
		if session.StreamGeneration != scope.StreamGeneration || session.MembershipGeneration != scope.MembershipGeneration || session.RetentionGeneration != scope.RetentionGeneration {
			session.Status = RebuildStatusInvalidated
			model.state.Rebuilds[key] = session
			continue
		}
		if session.SnapshotBoundary.StreamGeneration != scope.StreamGeneration {
			return StreamPosition{}, false, errors.New("active rebuild snapshot has another stream generation")
		}
		if !found || lessStreamPosition(session.SnapshotBoundary, minimum) {
			minimum = session.SnapshotBoundary
			found = true
		}
	}
	return minimum, found, nil
}

func retentionFloorMatchesScope(floor RetentionFloor, scope ScopeState) bool {
	return floor.MembershipGeneration == scope.MembershipGeneration &&
		floor.RetentionGeneration == scope.RetentionGeneration &&
		floor.StreamGeneration == scope.StreamGeneration
}

func cursorPositionAtOrAboveFloor(position StreamPosition, floor RetentionFloor) bool {
	if position.StreamGeneration != floor.StreamGeneration {
		return false
	}
	return !lessStreamPosition(position, floor.Position)
}

func sameOrAfterTime(left, right time.Time) bool {
	return !left.Before(right)
}
