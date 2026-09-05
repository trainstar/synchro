package blackbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

const diagnosticRetentionClientID = "s12-retention-client"

// DiagnosticPushObservation contains bounded durable state for one diagnostic push client.
type DiagnosticPushObservation struct {
	BatchCount         int64
	MutationCount      int64
	SourceRowCount     int64
	AcceptedWriteEpoch int64
}

// DiagnosticClientGenerationObservation contains bounded lifecycle state for one diagnostic client.
type DiagnosticClientGenerationObservation struct {
	Generation              int64
	Active                  bool
	AcceptedWriteEpoch      int64
	CheckpointCount         int64
	ScopeHistoryGenerations int64
}

// DiagnosticCompactionResult contains the fixed retention control result.
type DiagnosticCompactionResult struct {
	DeactivatedClients int64 `json:"deactivated_clients"`
	SafeSeq            int64 `json:"safe_seq"`
	DeletedEntries     int64 `json:"deleted_entries"`
}

// DiagnosticRetentionCompactionObservation contains bounded scope-local
// compaction state around one active rebuild pin.
type DiagnosticRetentionCompactionObservation struct {
	RebuildSessionCount   int64
	RebuildPinActive      bool
	PinnedAfterBoundary   bool
	PrefixEffectCount     int64
	PinnedEffectCount     int64
	UnrelatedEffectCount  int64
	PrefixMaximumPosition string
	PinnedPosition        string
	UserFloorPosition     string
	GlobalFloorPosition   string
}

// ObserveDiagnosticPush returns counts without returning request or source payloads.
func (executor *OperatorExecutor) ObserveDiagnosticPush(ctx context.Context, clientID string, recordIDs []string) (DiagnosticPushObservation, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return DiagnosticPushObservation{}, errors.New("operator executor is unavailable")
	}
	if ctx == nil || clientID == "" || len(clientID) > 128 || len(recordIDs) == 0 || len(recordIDs) > 1001 {
		return DiagnosticPushObservation{}, errors.New("push observation input is invalid")
	}
	for _, recordID := range recordIDs {
		if !diagnosticUUIDPattern.MatchString(recordID) {
			return DiagnosticPushObservation{}, errors.New("push observation input is invalid")
		}
	}

	harness := executor.harness
	database, err := harness.openDatabase(ctx, harness.names.Database, harness.env.Admin, false)
	if err != nil {
		return DiagnosticPushObservation{}, errors.New("open push observation connection failed")
	}
	defer database.Close()

	var observation DiagnosticPushObservation
	err = database.QueryRowContext(ctx, `
		SELECT
			(SELECT count(*) FROM synchro.sync_push_batches
			 WHERE user_id = 'diagnostic-user' AND client_id = $1),
			(SELECT count(*) FROM synchro.sync_push_mutations
			 WHERE user_id = 'diagnostic-user' AND client_id = $1),
			(SELECT count(*) FROM public.cf_items
			 WHERE id::text = ANY($2::text[])),
			client.accepted_write_epoch
		FROM synchro.sync_clients client
		WHERE client.user_id = 'diagnostic-user' AND client.client_id = $1`, clientID, recordIDs).Scan(
		&observation.BatchCount,
		&observation.MutationCount,
		&observation.SourceRowCount,
		&observation.AcceptedWriteEpoch,
	)
	if err != nil {
		return DiagnosticPushObservation{}, errors.New("read push observation failed")
	}
	return observation, nil
}

// ExpireRetentionClient marks one client for retention expiry. The identity is
// a parameter because the extension entry point serves any registered client,
// and an authored scenario names the client it expires.
func (executor *OperatorExecutor) ExpireRetentionClient(ctx context.Context, userID, clientID string) error {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady || ctx == nil {
		return errors.New("operator executor is unavailable")
	}
	if userID == "" || clientID == "" {
		return errors.New("retention client identity is incomplete")
	}
	harness := executor.harness
	database, err := harness.openDatabase(ctx, harness.names.Database, harness.env.Admin, false)
	if err != nil {
		return errors.New("open retention age control connection failed")
	}
	defer database.Close()

	var expired bool
	err = database.QueryRowContext(
		ctx,
		"SELECT synchro.synchro_inject_client_retention_expiry($1, $2)",
		userID,
		clientID,
	).Scan(&expired)
	if err != nil {
		return fmt.Errorf("expire retention client: %w", err)
	}
	if !expired {
		return errors.New("retention client was not active")
	}
	return nil
}

// RunDiagnosticRetentionCompaction runs the fixed S-12 production compaction entry point.
func (executor *OperatorExecutor) RunDiagnosticRetentionCompaction(ctx context.Context) (DiagnosticCompactionResult, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady || ctx == nil {
		return DiagnosticCompactionResult{}, errors.New("operator executor is unavailable")
	}
	harness := executor.harness
	database, err := harness.openDatabase(ctx, harness.names.Database, harness.env.Admin, false)
	if err != nil {
		return DiagnosticCompactionResult{}, errors.New("open retention compaction connection failed")
	}
	defer database.Close()

	var raw []byte
	if err := database.QueryRowContext(
		ctx,
		"SELECT synchro.synchro_compact($1, $2)",
		"30 days",
		10000,
	).Scan(&raw); err != nil {
		return DiagnosticCompactionResult{}, fmt.Errorf("run diagnostic retention compaction failed: %w", err)
	}
	var result DiagnosticCompactionResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return DiagnosticCompactionResult{}, errors.New("decode diagnostic retention compaction failed")
	}
	if result.DeactivatedClients < 0 || result.SafeSeq < 0 || result.DeletedEntries < 0 {
		return DiagnosticCompactionResult{}, errors.New("diagnostic retention compaction result is invalid")
	}
	return result, nil
}

// ObserveDiagnosticRetentionCompaction returns bounded effect, pin, and floor
// state for the fixed S-12 diagnostic scopes.
func (executor *OperatorExecutor) ObserveDiagnosticRetentionCompaction(
	ctx context.Context,
	clientID string,
	rebuildID string,
	prefixRecordIDs []string,
	pinnedRecordID string,
	unrelatedRecordID string,
) (DiagnosticRetentionCompactionObservation, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return DiagnosticRetentionCompactionObservation{}, errors.New("operator executor is unavailable")
	}
	if ctx == nil || clientID == "" || len(clientID) > 128 ||
		!diagnosticUUIDPattern.MatchString(rebuildID) || len(prefixRecordIDs) != 2 ||
		!diagnosticUUIDPattern.MatchString(pinnedRecordID) ||
		!diagnosticUUIDPattern.MatchString(unrelatedRecordID) {
		return DiagnosticRetentionCompactionObservation{}, errors.New("retention compaction observation input is invalid")
	}
	for _, recordID := range prefixRecordIDs {
		if !diagnosticUUIDPattern.MatchString(recordID) {
			return DiagnosticRetentionCompactionObservation{}, errors.New("retention compaction observation input is invalid")
		}
	}

	harness := executor.harness
	database, err := harness.openDatabase(ctx, harness.names.Database, harness.env.Admin, false)
	if err != nil {
		return DiagnosticRetentionCompactionObservation{}, errors.New("open retention compaction observation connection failed")
	}
	defer database.Close()

	var observation DiagnosticRetentionCompactionObservation
	err = database.QueryRowContext(ctx, `
		WITH rebuild_session AS (
			SELECT boundary_position_kind, boundary_commit_lsn, expires_at
			FROM synchro.sync_rebuild_sessions
			WHERE user_id = 'diagnostic-user'
			  AND client_id = $1
			  AND rebuild_id = $2::uuid
		), prefix_effects AS (
			SELECT commit_lsn, event_ordinal, effect_ordinal
			FROM synchro.sync_changelog
			WHERE bucket_id = 'user:diagnostic-user'
			  AND table_name = 'cf_items'
			  AND record_id = ANY($3::text[])
		), pinned_effect AS (
			SELECT commit_lsn, event_ordinal, effect_ordinal
			FROM synchro.sync_changelog
			WHERE bucket_id = 'user:diagnostic-user'
			  AND table_name = 'cf_items'
			  AND record_id = $4
		), unrelated_effect AS (
			SELECT commit_lsn, event_ordinal, effect_ordinal
			FROM synchro.sync_changelog
			WHERE bucket_id = 'cf:global'
			  AND table_name = 'cf_global_items'
			  AND record_id = $5
		), user_floor AS (
			SELECT floor_position_kind, floor_commit_lsn,
			       floor_event_ordinal, floor_effect_ordinal
			FROM synchro.sync_scope_state
			WHERE scope_id = 'user:diagnostic-user'
		), global_floor AS (
			SELECT floor_position_kind, floor_commit_lsn,
			       floor_event_ordinal, floor_effect_ordinal
			FROM synchro.sync_scope_state
			WHERE scope_id = 'cf:global'
		)
		SELECT
			(SELECT count(*) FROM rebuild_session),
			COALESCE((SELECT expires_at > now() FROM rebuild_session), false),
			COALESCE((
				SELECT rebuild_session.boundary_position_kind = 'transaction_end'
				   AND pinned_effect.commit_lsn > rebuild_session.boundary_commit_lsn
				FROM rebuild_session, pinned_effect
			), false),
			(SELECT count(*) FROM prefix_effects),
			(SELECT count(*) FROM pinned_effect),
			(SELECT count(*) FROM unrelated_effect),
			COALESCE((
				SELECT format('effect|%s|%s|%s', commit_lsn, event_ordinal, effect_ordinal)
				FROM prefix_effects
				ORDER BY commit_lsn DESC, event_ordinal DESC, effect_ordinal DESC
				LIMIT 1
			), ''),
			COALESCE((
				SELECT format('effect|%s|%s|%s', commit_lsn, event_ordinal, effect_ordinal)
				FROM pinned_effect
			), ''),
			(SELECT format('%s|%s|%s|%s', floor_position_kind,
			       COALESCE(floor_commit_lsn::text, ''),
			       COALESCE(floor_event_ordinal::text, ''),
			       COALESCE(floor_effect_ordinal::text, '')) FROM user_floor),
			(SELECT format('%s|%s|%s|%s', floor_position_kind,
			       COALESCE(floor_commit_lsn::text, ''),
			       COALESCE(floor_event_ordinal::text, ''),
			       COALESCE(floor_effect_ordinal::text, '')) FROM global_floor)`,
		clientID,
		rebuildID,
		prefixRecordIDs,
		pinnedRecordID,
		unrelatedRecordID,
	).Scan(
		&observation.RebuildSessionCount,
		&observation.RebuildPinActive,
		&observation.PinnedAfterBoundary,
		&observation.PrefixEffectCount,
		&observation.PinnedEffectCount,
		&observation.UnrelatedEffectCount,
		&observation.PrefixMaximumPosition,
		&observation.PinnedPosition,
		&observation.UserFloorPosition,
		&observation.GlobalFloorPosition,
	)
	if err != nil {
		return DiagnosticRetentionCompactionObservation{}, errors.New("read retention compaction observation failed")
	}
	if observation.RebuildSessionCount != 1 || observation.UserFloorPosition == "" || observation.GlobalFloorPosition == "" {
		return DiagnosticRetentionCompactionObservation{}, errors.New("retention compaction observation is invalid")
	}
	return observation, nil
}

// ObserveDiagnosticClientGeneration returns bounded state for one diagnostic client.
func (executor *OperatorExecutor) ObserveDiagnosticClientGeneration(ctx context.Context, clientID string) (DiagnosticClientGenerationObservation, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return DiagnosticClientGenerationObservation{}, errors.New("operator executor is unavailable")
	}
	if ctx == nil || clientID == "" || len(clientID) > 128 {
		return DiagnosticClientGenerationObservation{}, errors.New("client generation observation input is invalid")
	}

	harness := executor.harness
	database, err := harness.openDatabase(ctx, harness.names.Database, harness.env.Admin, false)
	if err != nil {
		return DiagnosticClientGenerationObservation{}, errors.New("open client generation observation connection failed")
	}
	defer database.Close()

	var observation DiagnosticClientGenerationObservation
	err = database.QueryRowContext(ctx, `
		SELECT client.client_generation,
		       client.is_active,
		       client.accepted_write_epoch,
		       (SELECT count(*)
		        FROM synchro.sync_client_checkpoints checkpoint
		        WHERE checkpoint.user_id = client.user_id
		          AND checkpoint.client_id = client.client_id),
		       (SELECT count(DISTINCT history.client_generation)
		        FROM synchro.sync_client_scope_history history
		        WHERE history.user_id = client.user_id
		          AND history.client_id = client.client_id)
		FROM synchro.sync_clients client
		WHERE client.user_id = 'diagnostic-user' AND client.client_id = $1`, clientID).Scan(
		&observation.Generation,
		&observation.Active,
		&observation.AcceptedWriteEpoch,
		&observation.CheckpointCount,
		&observation.ScopeHistoryGenerations,
	)
	if err != nil {
		return DiagnosticClientGenerationObservation{}, errors.New("read client generation observation failed")
	}
	return observation, nil
}
