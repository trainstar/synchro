package blackbox

import (
	"context"
	"database/sql"
	"errors"
)

// RebuildSessionObservation is the bounded identity and boundary state for one
// fixed diagnostic rebuild session.
type RebuildSessionObservation struct {
	SessionID              string
	ClientID               string
	RebuildID              string
	ScopeID                string
	ClientGeneration       int64
	SchemaVersion          int64
	SchemaHash             string
	StreamGeneration       string
	MembershipGeneration   int64
	RetentionGeneration    int64
	BoundaryPositionKind   string
	BoundaryCommitLSN      string
	BoundaryCommitLSNValid bool
	PageLimit              int64
	StagedRowCount         int64
	Expired                bool
}

// ClientScopeAssignmentObservation is one bounded latest assignment row.
type ClientScopeAssignmentObservation struct {
	ScopeID              string
	ClientGeneration     int64
	ScopeSetVersion      int64
	Assigned             bool
	AssignmentSource     string
	MembershipGeneration int64
	RetentionGeneration  int64
}

// WALRecordStageObservation contains bounded stage counts for fixed diagnostic
// records when pull-visible materialization does not complete.
type WALRecordStageObservation struct {
	FenceCount      int64
	PendingFences   int64
	EventCount      int64
	ProjectionCount int64
	CapturedCount   int64
	EdgeCount       int64
	ChangeCount     int64
}

// ObserveWALRecordStages returns bounded pipeline counts for fixed diagnostic
// records without returning source or synchronization payloads.
func (executor *OperatorExecutor) ObserveWALRecordStages(
	ctx context.Context,
	tableName string,
	recordIDs []string,
) (WALRecordStageObservation, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return WALRecordStageObservation{}, errors.New("operator executor is unavailable")
	}
	if ctx == nil || !validPullRebuildObservationTable(tableName) || len(recordIDs) == 0 || len(recordIDs) > 16 {
		return WALRecordStageObservation{}, errors.New("WAL stage observation input is invalid")
	}
	for _, recordID := range recordIDs {
		if !diagnosticUUIDPattern.MatchString(recordID) {
			return WALRecordStageObservation{}, errors.New("WAL stage observation record ID is invalid")
		}
	}

	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return WALRecordStageObservation{}, errors.New("open WAL stage observation connection failed")
	}
	defer database.Close()

	var observation WALRecordStageObservation
	err = database.QueryRowContext(ctx, `
		WITH relation AS (
			SELECT registry.relation_id
			FROM synchro.sync_registry registry
			JOIN synchro.sync_registry_generations generation
			  ON generation.generation = registry.registry_generation
			WHERE generation.state = 'active'
			  AND registry.table_name = $1
		), fences AS (
			SELECT count(*) AS total,
			       count(*) FILTER (WHERE coverage = 'pending') AS pending
			FROM synchro.sync_write_fences fence
			JOIN relation ON relation.relation_id = fence.relation_id
			WHERE fence.new_record_id = ANY($2)
		)
		SELECT fences.total,
		       fences.pending,
		       (SELECT count(*) FROM synchro.sync_wal_events event
		        JOIN synchro.sync_write_fences fence ON fence.fence_id = event.fence_id
		        JOIN relation ON relation.relation_id = event.relation_id
		        WHERE fence.new_record_id = ANY($2)),
		       (SELECT count(*) FROM synchro.sync_captured_projections projection
		        JOIN relation ON relation.relation_id = projection.relation_id
		        WHERE projection.record_id = ANY($2)),
		       (SELECT count(*) FROM synchro.sync_captured_rows captured
		        JOIN relation ON relation.relation_id = captured.relation_id
		        WHERE captured.record_id = ANY($2)),
		       (SELECT count(*) FROM synchro.sync_bucket_edges edge
		        JOIN relation ON relation.relation_id = edge.relation_id
		        WHERE edge.record_id = ANY($2)),
		       (SELECT count(*) FROM synchro.sync_changelog change
		        JOIN relation ON relation.relation_id = change.relation_id
		        WHERE change.record_id = ANY($2))
		FROM fences`, tableName, recordIDs).Scan(
		&observation.FenceCount,
		&observation.PendingFences,
		&observation.EventCount,
		&observation.ProjectionCount,
		&observation.CapturedCount,
		&observation.EdgeCount,
		&observation.ChangeCount,
	)
	if err != nil {
		return WALRecordStageObservation{}, errors.New("read WAL stage observation failed")
	}
	return observation, nil
}

// ObserveWALRecordsForTable returns bounded materialization evidence for fixed
// diagnostic source tables without exposing synchronization payloads.
func (executor *OperatorExecutor) ObserveWALRecordsForTable(
	ctx context.Context,
	tableName string,
	recordIDs []string,
) (WALPipelineObservation, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return WALPipelineObservation{}, errors.New("operator executor is unavailable")
	}
	if ctx == nil || !validPullRebuildObservationTable(tableName) || len(recordIDs) == 0 || len(recordIDs) > 16 {
		return WALPipelineObservation{}, errors.New("WAL observation input is invalid")
	}
	for _, recordID := range recordIDs {
		if !diagnosticUUIDPattern.MatchString(recordID) {
			return WALPipelineObservation{}, errors.New("WAL observation record ID is invalid")
		}
	}

	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return WALPipelineObservation{}, errors.New("open WAL observation connection failed")
	}
	defer database.Close()

	rows, err := database.QueryContext(ctx, `
		SELECT c.record_id,
		       c.commit_lsn::text,
		       transaction.end_lsn::text,
		       c.event_ordinal,
		       c.effect_ordinal,
		       fence.coverage,
		       c.row_version::text,
		       transaction.replay_count
		FROM synchro.sync_changelog c
		JOIN synchro.sync_wal_transactions transaction
		  ON transaction.stream_generation = c.stream_generation
		 AND transaction.commit_lsn = c.commit_lsn
		JOIN synchro.sync_wal_events event
		  ON event.stream_generation = c.stream_generation
		 AND event.commit_lsn = c.commit_lsn
		 AND event.event_ordinal = c.event_ordinal
		 AND event.relation_id = c.relation_id
		JOIN synchro.sync_write_fences fence ON fence.fence_id = event.fence_id
		WHERE c.table_name = $1
		  AND c.record_id = ANY($2)
		ORDER BY c.commit_lsn, c.event_ordinal, c.effect_ordinal, c.record_id`, tableName, recordIDs)
	if err != nil {
		return WALPipelineObservation{}, errors.New("read WAL record observations failed")
	}
	defer rows.Close()

	observation := WALPipelineObservation{}
	for rows.Next() {
		var record WALRecordObservation
		if err := rows.Scan(
			&record.RecordID,
			&record.CommitLSN,
			&record.EndLSN,
			&record.EventOrdinal,
			&record.EffectOrdinal,
			&record.FenceCoverage,
			&record.RowVersion,
			&record.ReplayCount,
		); err != nil {
			return WALPipelineObservation{}, errors.New("scan WAL record observation failed")
		}
		observation.Records = append(observation.Records, record)
	}
	if err := rows.Err(); err != nil {
		return WALPipelineObservation{}, errors.New("read WAL record observations failed")
	}

	if err := database.QueryRowContext(ctx, `
		WITH observed AS (
			SELECT max(transaction.end_lsn) AS maximum_end_lsn
			FROM synchro.sync_changelog c
			JOIN synchro.sync_wal_transactions transaction
			  ON transaction.stream_generation = c.stream_generation
			 AND transaction.commit_lsn = c.commit_lsn
			WHERE c.table_name = $1 AND c.record_id = ANY($2)
		)
		SELECT EXISTS (
				SELECT 1 FROM pg_catalog.pg_stat_activity
				WHERE datname = current_database() AND backend_type = 'synchro WAL consumer'
			),
		       EXISTS (SELECT 1 FROM synchro.sync_wal_poison WHERE lifecycle = 'active'),
		       COALESCE(
				(SELECT progress.acknowledged_end_lsn >= observed.maximum_end_lsn
				 FROM synchro.sync_wal_progress progress, observed
				 WHERE progress.singleton AND observed.maximum_end_lsn IS NOT NULL),
				false
			       )`, tableName, recordIDs).Scan(
		&observation.WorkerRunning,
		&observation.BlockingPoison,
		&observation.ContiguousAcknowledged,
	); err != nil {
		return WALPipelineObservation{}, errors.New("read WAL pipeline observation failed")
	}
	return observation, nil
}

// ObserveRebuildSession returns one immutable session boundary and bounded
// staging cardinality for a fixed diagnostic client and rebuild identity.
func (executor *OperatorExecutor) ObserveRebuildSession(
	ctx context.Context,
	clientID string,
	rebuildID string,
) (RebuildSessionObservation, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return RebuildSessionObservation{}, errors.New("operator executor is unavailable")
	}
	if ctx == nil || clientID == "" || len(clientID) > 128 || !diagnosticUUIDPattern.MatchString(rebuildID) {
		return RebuildSessionObservation{}, errors.New("rebuild session observation input is invalid")
	}

	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return RebuildSessionObservation{}, errors.New("open rebuild session observation connection failed")
	}
	defer database.Close()

	var observation RebuildSessionObservation
	var boundaryCommitLSN sql.NullString
	err = database.QueryRowContext(ctx, `
		SELECT session_id::text,
		       client_id,
		       rebuild_id::text,
		       scope_id,
		       client_generation,
		       schema_version,
		       schema_hash,
		       stream_generation,
		       membership_generation,
		       retention_generation,
		       boundary_position_kind,
		       boundary_commit_lsn::text,
		       page_limit,
		       staged_row_count,
		       expires_at <= now()
		FROM synchro.sync_rebuild_sessions
		WHERE user_id = 'diagnostic-user'
		  AND client_id = $1
		  AND rebuild_id = $2::uuid`, clientID, rebuildID).Scan(
		&observation.SessionID,
		&observation.ClientID,
		&observation.RebuildID,
		&observation.ScopeID,
		&observation.ClientGeneration,
		&observation.SchemaVersion,
		&observation.SchemaHash,
		&observation.StreamGeneration,
		&observation.MembershipGeneration,
		&observation.RetentionGeneration,
		&observation.BoundaryPositionKind,
		&boundaryCommitLSN,
		&observation.PageLimit,
		&observation.StagedRowCount,
		&observation.Expired,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return RebuildSessionObservation{}, errors.New("rebuild session observation is missing")
	}
	if err != nil {
		return RebuildSessionObservation{}, errors.New("read rebuild session observation failed")
	}
	observation.BoundaryCommitLSN = boundaryCommitLSN.String
	observation.BoundaryCommitLSNValid = boundaryCommitLSN.Valid
	if !diagnosticUUIDPattern.MatchString(observation.SessionID) ||
		observation.ClientID != clientID || observation.RebuildID != rebuildID ||
		observation.ScopeID == "" || observation.ClientGeneration <= 0 ||
		observation.SchemaVersion <= 0 || len(observation.SchemaHash) != 64 ||
		observation.StreamGeneration == "" || observation.MembershipGeneration <= 0 ||
		observation.RetentionGeneration <= 0 || observation.PageLimit <= 0 ||
		observation.StagedRowCount < 0 {
		return RebuildSessionObservation{}, errors.New("rebuild session observation is invalid")
	}
	switch observation.BoundaryPositionKind {
	case "generation_start":
		if observation.BoundaryCommitLSNValid {
			return RebuildSessionObservation{}, errors.New("rebuild session boundary observation is invalid")
		}
	case "transaction_end":
		if !observation.BoundaryCommitLSNValid || observation.BoundaryCommitLSN == "" {
			return RebuildSessionObservation{}, errors.New("rebuild session boundary observation is invalid")
		}
	default:
		return RebuildSessionObservation{}, errors.New("rebuild session boundary observation is invalid")
	}
	return observation, nil
}

// ObserveClientScopeAssignment returns the latest assignment for one diagnostic scope.
func (executor *OperatorExecutor) ObserveClientScopeAssignment(ctx context.Context, clientID, scopeID string) (ClientScopeAssignmentObservation, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return ClientScopeAssignmentObservation{}, errors.New("operator executor is unavailable")
	}
	if ctx == nil || clientID == "" || len(clientID) > 128 || scopeID == "" || len(scopeID) > 256 {
		return ClientScopeAssignmentObservation{}, errors.New("client scope assignment observation input is invalid")
	}
	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return ClientScopeAssignmentObservation{}, errors.New("open client scope assignment observation connection failed")
	}
	defer database.Close()
	var observation ClientScopeAssignmentObservation
	err = database.QueryRowContext(ctx, `
		SELECT scope_id, client_generation, scope_set_version, assigned,
		       assignment_source, membership_generation, retention_generation
		FROM synchro.sync_client_scope_history
		WHERE user_id = 'diagnostic-user' AND client_id = $1 AND scope_id = $2
		ORDER BY scope_set_version DESC
		LIMIT 1`, clientID, scopeID).Scan(
		&observation.ScopeID,
		&observation.ClientGeneration,
		&observation.ScopeSetVersion,
		&observation.Assigned,
		&observation.AssignmentSource,
		&observation.MembershipGeneration,
		&observation.RetentionGeneration,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return ClientScopeAssignmentObservation{}, errors.New("client scope assignment observation is missing")
	}
	if err != nil {
		return ClientScopeAssignmentObservation{}, errors.New("read client scope assignment observation failed")
	}
	if observation.ScopeID != scopeID || observation.ClientGeneration <= 0 || observation.ScopeSetVersion <= 0 || !observation.Assigned || observation.MembershipGeneration <= 0 || observation.RetentionGeneration <= 0 {
		return ClientScopeAssignmentObservation{}, errors.New("client scope assignment observation is invalid")
	}
	switch observation.AssignmentSource {
	case "identity", "shared", "assignment_rule":
	default:
		return ClientScopeAssignmentObservation{}, errors.New("client scope assignment source is invalid")
	}
	return observation, nil
}

func validPullRebuildObservationTable(tableName string) bool {
	switch tableName {
	case "cf_global_items", "cf_items", "cf_schema_queue":
		return true
	default:
		return false
	}
}
