package blackbox

import (
	"context"
	"errors"
	"fmt"
)

// RegistryActivationObservation contains bounded generation evidence around one
// fixed diagnostic registry activation.
type RegistryActivationObservation struct {
	SourceGeneration                int64
	ActiveGeneration                int64
	PriorTransactionGeneration      int64
	ActivationTransactionGeneration int64
	PostTransactionGeneration       int64
	ActivationBoundaryComplete      bool
	PostTransactionSingleCommit     bool
	PostProjectionGenerationMatches bool
	RuntimeGenerationMatches        bool
	WorkerGenerationMatches         bool
	NoPendingRegistryGeneration     bool
}

// SchemaIncompatibleMutationObservation contains bounded durable state for one
// fixed diagnostic schema rejection.
type SchemaIncompatibleMutationObservation struct {
	LedgerCount             int64
	RequestOrdinal          int64
	AuthoredSchemaVersion   int64
	AuthoredSchemaHash      string
	SubmittedSchemaVersion  int64
	SubmittedSchemaHash     string
	OutcomeSchemaVersion    int64
	OutcomeSchemaHash       string
	OutcomeStatus           string
	RejectionCode           string
	CanonicalRequestMatches bool
	SourceRowCount          int64
}

// TransitionSchemaQueue removes the fixed field and stages its replacement
// registry generation in one source transaction.
func (executor *OperatorExecutor) TransitionSchemaQueue(ctx context.Context) error {
	return executor.transitionSchemaQueue(ctx, "legacy_value", "")
}

// TransitionSchemaQueueField replaces one diagnostic queue field and stages
// its replacement registry generation in one source transaction.
func (executor *OperatorExecutor) TransitionSchemaQueueField(ctx context.Context, removed, added string) error {
	// A transition may drop a field without adding one. transitionSchemaQueue
	// already guards its ADD COLUMN on a non-empty name.
	if !validSchemaTransitionColumn(removed) || removed == added {
		return errors.New("schema transition fields are invalid")
	}
	if added != "" && !validSchemaTransitionColumn(added) {
		return errors.New("schema transition fields are invalid")
	}
	return executor.transitionSchemaQueue(ctx, removed, added)
}

func (executor *OperatorExecutor) transitionSchemaQueue(ctx context.Context, removed, added string) error {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady || ctx == nil {
		return errors.New("operator executor is unavailable")
	}
	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return errors.New("open schema transition connection failed")
	}
	defer database.Close()
	transaction, err := database.BeginTx(ctx, nil)
	if err != nil {
		return errors.New("begin schema transition failed")
	}
	if _, err := transaction.ExecContext(ctx, "ALTER TABLE public.cf_schema_queue DROP COLUMN "+removed); err != nil {
		_ = transaction.Rollback()
		return errors.New("drop schema transition field failed")
	}
	if added != "" {
		if _, err := transaction.ExecContext(ctx, "ALTER TABLE public.cf_schema_queue ADD COLUMN "+added+" TEXT NOT NULL DEFAULT ''"); err != nil {
			_ = transaction.Rollback()
			return errors.New("add schema transition field failed")
		}
	}
	if _, err := transaction.ExecContext(ctx, `SELECT synchro.synchro_register_table(
		'public.cf_schema_queue', 'public.cf_schema_queue_membership', 'single_scope',
		'id', 'updated_at', 'deleted_at', 'enabled'
	)`); err != nil {
		_ = transaction.Rollback()
		return errors.New("stage schema transition registry failed")
	}
	if err := transaction.Commit(); err != nil {
		return errors.New("commit schema transition failed")
	}
	return nil
}

func validSchemaTransitionColumn(value string) bool {
	if value == "" || len(value) > 63 || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, character := range value[1:] {
		if character != '_' && (character < 'a' || character > 'z') && (character < '0' || character > '9') {
			return false
		}
	}
	return true
}

// ObserveRegistryActivation verifies transaction-scoped registry selection for
// three fixed diagnostic records.
func (executor *OperatorExecutor) ObserveRegistryActivation(
	ctx context.Context,
	priorRecordID, postItemRecordID, postSchemaRecordID string,
) (RegistryActivationObservation, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return RegistryActivationObservation{}, errors.New("operator executor is unavailable")
	}
	if ctx == nil || !diagnosticUUIDPattern.MatchString(priorRecordID) ||
		!diagnosticUUIDPattern.MatchString(postItemRecordID) ||
		!diagnosticUUIDPattern.MatchString(postSchemaRecordID) {
		return RegistryActivationObservation{}, errors.New("registry activation observation input is invalid")
	}

	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return RegistryActivationObservation{}, errors.New("open registry activation observation connection failed")
	}
	defer database.Close()

	var observation RegistryActivationObservation
	err = database.QueryRowContext(ctx, `
		WITH active AS (
			SELECT generation, parent_generation, activation_commit_lsn, activation_end_lsn
			FROM synchro.sync_registry_generations
			WHERE state = 'active' AND validated
		), prior_event AS (
			SELECT event.stream_generation, event.commit_lsn
			FROM synchro.sync_wal_events event
			JOIN synchro.sync_write_fences fence ON fence.fence_id = event.fence_id
			JOIN synchro.sync_registry registry
			  ON registry.relation_id = event.relation_id
			 AND registry.registry_generation = (SELECT parent_generation FROM active)
			WHERE registry.table_name = 'cf_items'
			  AND fence.new_record_id = $1
		), post_events AS (
			SELECT event.stream_generation, event.commit_lsn, event.relation_id, event.event_ordinal
			FROM synchro.sync_wal_events event
			JOIN synchro.sync_write_fences fence ON fence.fence_id = event.fence_id
			WHERE fence.new_record_id IN ($2, $3)
		), post_summary AS (
			SELECT count(*) AS event_count,
			       count(DISTINCT (event.stream_generation, event.commit_lsn)) AS commit_count,
			       min(transaction.registry_generation) AS minimum_generation,
			       max(transaction.registry_generation) AS maximum_generation
			FROM post_events event
			JOIN synchro.sync_wal_transactions transaction
			  ON transaction.stream_generation = event.stream_generation
			 AND transaction.commit_lsn = event.commit_lsn
		), projection_summary AS (
			SELECT count(*) AS projection_count,
			       bool_and(projection.registry_generation = active.generation) AS generation_matches
			FROM synchro.sync_captured_projections projection
			CROSS JOIN active
			WHERE projection.image_kind = 'after'
			  AND projection.record_id IN ($2, $3)
		)
		SELECT active.parent_generation,
		       active.generation,
		       prior_transaction.registry_generation,
		       activation_transaction.registry_generation,
		       post_summary.minimum_generation,
		       active.activation_commit_lsn IS NOT NULL
		         AND active.activation_end_lsn IS NOT NULL
		         AND activation_transaction.commit_lsn = active.activation_commit_lsn
		         AND activation_transaction.end_lsn = active.activation_end_lsn,
		       post_summary.event_count = 2
		         AND post_summary.commit_count = 1
		         AND post_summary.minimum_generation = post_summary.maximum_generation,
		       projection_summary.projection_count = 2
		         AND projection_summary.generation_matches,
		       progress.registry_generation = active.generation,
		       worker.registry_generation = active.generation,
		       NOT EXISTS (
		         SELECT 1 FROM synchro.sync_registry_generations WHERE state = 'pending'
		       )
		FROM active
		JOIN prior_event ON true
		JOIN synchro.sync_wal_transactions prior_transaction
		  ON prior_transaction.stream_generation = prior_event.stream_generation
		 AND prior_transaction.commit_lsn = prior_event.commit_lsn
		JOIN synchro.sync_wal_transactions activation_transaction
		  ON activation_transaction.commit_lsn = active.activation_commit_lsn
		CROSS JOIN post_summary
		CROSS JOIN projection_summary
		CROSS JOIN synchro.sync_wal_progress progress
		JOIN synchro.sync_wal_worker_state worker
		  ON worker.worker_id = 'synchro_wal_consumer'
		WHERE progress.singleton`, priorRecordID, postItemRecordID, postSchemaRecordID).Scan(
		&observation.SourceGeneration,
		&observation.ActiveGeneration,
		&observation.PriorTransactionGeneration,
		&observation.ActivationTransactionGeneration,
		&observation.PostTransactionGeneration,
		&observation.ActivationBoundaryComplete,
		&observation.PostTransactionSingleCommit,
		&observation.PostProjectionGenerationMatches,
		&observation.RuntimeGenerationMatches,
		&observation.WorkerGenerationMatches,
		&observation.NoPendingRegistryGeneration,
	)
	if err != nil {
		return RegistryActivationObservation{}, fmt.Errorf("read registry activation observation failed: %w", err)
	}
	return observation, nil
}

// ObserveSchemaIncompatibleMutation verifies one terminal ledger entry without
// returning the preserved mutation payload.
func (executor *OperatorExecutor) ObserveSchemaIncompatibleMutation(
	ctx context.Context,
	clientID, mutationID, recordID string,
	expectedCanonicalRequest []byte,
) (SchemaIncompatibleMutationObservation, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return SchemaIncompatibleMutationObservation{}, errors.New("operator executor is unavailable")
	}
	if ctx == nil || clientID == "" || len(clientID) > 128 ||
		!diagnosticUUIDPattern.MatchString(mutationID) ||
		!diagnosticUUIDPattern.MatchString(recordID) ||
		len(expectedCanonicalRequest) == 0 || len(expectedCanonicalRequest) > 1<<20 {
		return SchemaIncompatibleMutationObservation{}, errors.New("schema rejection observation input is invalid")
	}

	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return SchemaIncompatibleMutationObservation{}, errors.New("open schema rejection observation connection failed")
	}
	defer database.Close()

	var observation SchemaIncompatibleMutationObservation
	err = database.QueryRowContext(ctx, `
		SELECT count(*) OVER (),
		       mutation.request_ordinal,
		       mutation.authored_schema_version,
		       mutation.authored_schema_hash,
		       mutation.submitted_schema_version,
		       mutation.submitted_schema_hash,
		       mutation.outcome_schema_version,
		       mutation.outcome_schema_hash,
		       mutation.outcome_status,
		       mutation.rejection_code,
		       mutation.sealed_canonical_request = $4,
		       (SELECT count(*) FROM public.cf_schema_queue WHERE id = $3::uuid)
		FROM synchro.sync_push_mutations mutation
		WHERE mutation.user_id = 'diagnostic-user'
		  AND mutation.client_id = $1
		  AND mutation.mutation_id = $2::uuid`, clientID, mutationID, recordID, expectedCanonicalRequest).Scan(
		&observation.LedgerCount,
		&observation.RequestOrdinal,
		&observation.AuthoredSchemaVersion,
		&observation.AuthoredSchemaHash,
		&observation.SubmittedSchemaVersion,
		&observation.SubmittedSchemaHash,
		&observation.OutcomeSchemaVersion,
		&observation.OutcomeSchemaHash,
		&observation.OutcomeStatus,
		&observation.RejectionCode,
		&observation.CanonicalRequestMatches,
		&observation.SourceRowCount,
	)
	if err != nil {
		return SchemaIncompatibleMutationObservation{}, errors.New("read schema rejection observation failed")
	}
	return observation, nil
}
