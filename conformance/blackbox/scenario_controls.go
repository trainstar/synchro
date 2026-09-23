package blackbox

import (
	"context"
	"database/sql"
	"encoding/json"
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
	var nonempty bool
	if err := transaction.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM public.cf_schema_queue)").Scan(&nonempty); err != nil {
		_ = transaction.Rollback()
		return errors.New("read schema transition relation state failed")
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
	return executor.bootstrapStagedTransition(ctx, database, added != "", nonempty)
}

// bootstrapStagedTransition completes a committed transition that the WAL
// activation path cannot finish alone. The extension activates a removal-only
// transition from commit order. A transition that adds a field or changes a
// type over a nonempty relation requires the operator projection bootstrap,
// so the staged generation stays pending until this runs it.
func (executor *OperatorExecutor) bootstrapStagedTransition(ctx context.Context, database *sql.DB, shapeChanged, nonempty bool) error {
	if !shapeChanged || !nonempty {
		return nil
	}
	var generation int64
	if err := database.QueryRowContext(ctx, `
		SELECT generation
		FROM synchro.sync_registry_generations
		WHERE state = 'pending' AND validated
		ORDER BY generation DESC
		LIMIT 1`).Scan(&generation); err != nil || generation <= 0 {
		return errors.New("read staged schema transition generation failed")
	}
	if _, err := executor.RunProjectionBootstrap(ctx, generation); err != nil {
		return fmt.Errorf("bootstrap staged schema transition generation %d failed: %w", generation, err)
	}
	return nil
}

type syncedTableRegistration struct {
	physicalSchema           string
	physicalRelation         string
	composition              string
	membershipFunctionSchema string
	membershipFunctionName   string
	pkColumn                 string
	updatedAtColumn          string
	deletedAtColumn          string
	pushPolicy               string
	syncColumns              []string
	excludeColumns           []string
	maxScopeFanout           int
}

func postgresTypeForAuthoredField(fieldType string) (string, error) {
	switch fieldType {
	case "string":
		return "TEXT", nil
	case "int":
		return "INTEGER", nil
	case "int64":
		return "BIGINT", nil
	case "decimal":
		return "NUMERIC", nil
	case "float":
		return "DOUBLE PRECISION", nil
	case "boolean":
		return "BOOLEAN", nil
	case "datetime":
		return "TIMESTAMPTZ", nil
	case "date":
		return "DATE", nil
	case "time":
		return "TIME", nil
	case "json":
		return "JSONB", nil
	case "bytes":
		return "BYTEA", nil
	default:
		return "", fmt.Errorf("authored field type %q is unsupported for PostgreSQL", fieldType)
	}
}

// TransitionSyncedTableField changes one synced table field and preserves its
// active registration settings in one source transaction.
func (executor *OperatorExecutor) TransitionSyncedTableField(
	ctx context.Context, relation, removed, added, typeChanged, authoredType string,
) error {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady || ctx == nil || relation == "" {
		return errors.New("operator executor is unavailable")
	}
	if (removed == "" && added == "" && typeChanged == "") ||
		(removed != "" && removed == added) ||
		(removed != "" && !validSchemaTransitionColumn(removed)) ||
		(added != "" && !validSchemaTransitionColumn(added)) ||
		(typeChanged != "" && (!validSchemaTransitionColumn(typeChanged) || typeChanged == removed || typeChanged == added)) ||
		(typeChanged == "" && authoredType != "") {
		return errors.New("schema transition fields are invalid")
	}
	postgresType := ""
	if typeChanged != "" {
		var err error
		postgresType, err = postgresTypeForAuthoredField(authoredType)
		if err != nil {
			return err
		}
	}
	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return errors.New("open synced schema transition connection failed")
	}
	defer database.Close()
	transaction, err := database.BeginTx(ctx, nil)
	if err != nil {
		return errors.New("begin synced schema transition failed")
	}
	committed := false
	defer func() {
		if !committed {
			_ = transaction.Rollback()
		}
	}()

	var registration syncedTableRegistration
	var syncColumnsJSON, excludeColumnsJSON string
	err = transaction.QueryRowContext(ctx, `
		SELECT registry.physical_schema,
		       registry.physical_relation,
		       registry.composition,
		       registry.membership_function_schema,
		       registry.membership_function_name,
		       registry.pk_column,
		       registry.updated_at_col,
		       registry.deleted_at_col,
		       registry.push_policy,
		       array_to_json(registry.sync_columns)::text,
		       array_to_json(registry.exclude_columns)::text,
		       registry.max_scope_fanout
		FROM synchro.sync_registry registry
		JOIN synchro.sync_registry_generations generation
		  ON generation.generation = registry.registry_generation
		WHERE generation.state = 'active'
		  AND generation.validated
		  AND registry.registration_kind = 'synced'
		  AND registry.table_name = $1`, relation).Scan(
		&registration.physicalSchema,
		&registration.physicalRelation,
		&registration.composition,
		&registration.membershipFunctionSchema,
		&registration.membershipFunctionName,
		&registration.pkColumn,
		&registration.updatedAtColumn,
		&registration.deletedAtColumn,
		&registration.pushPolicy,
		&syncColumnsJSON,
		&excludeColumnsJSON,
		&registration.maxScopeFanout,
	)
	if err != nil || registration.physicalSchema == "" || registration.physicalRelation == "" ||
		registration.composition == "" || registration.membershipFunctionSchema == "" ||
		registration.membershipFunctionName == "" || registration.pkColumn == "" ||
		registration.updatedAtColumn == "" || registration.deletedAtColumn == "" ||
		registration.pushPolicy == "" || registration.maxScopeFanout <= 0 ||
		json.Unmarshal([]byte(syncColumnsJSON), &registration.syncColumns) != nil ||
		json.Unmarshal([]byte(excludeColumnsJSON), &registration.excludeColumns) != nil {
		return errors.New("read active synced table registration failed")
	}

	physicalRelation := quoteIdentifier(registration.physicalSchema) + "." + quoteIdentifier(registration.physicalRelation)
	var nonempty bool
	if err := transaction.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM "+physicalRelation+")").Scan(&nonempty); err != nil {
		return errors.New("read synced schema transition relation state failed")
	}
	if removed != "" {
		if _, err := transaction.ExecContext(ctx, "ALTER TABLE "+physicalRelation+" DROP COLUMN "+quoteIdentifier(removed)); err != nil {
			return errors.New("drop synced schema transition field failed")
		}
	}
	if added != "" {
		// A class 2 addition must accept rows from the earlier schema.
		if _, err := transaction.ExecContext(ctx, "ALTER TABLE "+physicalRelation+" ADD COLUMN "+quoteIdentifier(added)+" TEXT NULL"); err != nil {
			return errors.New("add synced schema transition field failed")
		}
	}
	if typeChanged != "" {
		// USING keeps the conversion explicit when PostgreSQL cannot infer it.
		statement := "ALTER TABLE " + physicalRelation + " ALTER COLUMN " + quoteIdentifier(typeChanged) + " TYPE " + postgresType + " USING " + quoteIdentifier(typeChanged) + "::" + postgresType
		if _, err := transaction.ExecContext(ctx, statement); err != nil {
			// PostgreSQL reports why it refused the cast, and a row it cannot
			// convert is the likely reason. Keep that cause.
			return fmt.Errorf("change synced schema transition field type failed: %w", err)
		}
	}

	registration.syncColumns = transitionSchemaColumns(registration.syncColumns, removed, added)
	registration.excludeColumns = transitionSchemaColumns(registration.excludeColumns, removed, "")
	syncColumns := registration.syncColumns
	excludeColumns := []string{}
	if len(registration.excludeColumns) != 0 {
		syncColumns = []string{}
		excludeColumns = registration.excludeColumns
	}
	syncColumnsValue, err := json.Marshal(syncColumns)
	if err != nil {
		return errors.New("encode synced schema transition columns failed")
	}
	excludeColumnsValue, err := json.Marshal(excludeColumns)
	if err != nil {
		return errors.New("encode synced schema transition columns failed")
	}
	membershipFunction := quoteIdentifier(registration.membershipFunctionSchema) + "." + quoteIdentifier(registration.membershipFunctionName)
	if _, err := transaction.ExecContext(ctx, `SELECT synchro.synchro_register_table(
		$1, $2, $3, $4, $5, $6, $7,
		ARRAY(SELECT jsonb_array_elements_text($8::jsonb)),
		ARRAY(SELECT jsonb_array_elements_text($9::jsonb)), $10
	)`,
		physicalRelation,
		membershipFunction,
		registration.composition,
		registration.pkColumn,
		registration.updatedAtColumn,
		registration.deletedAtColumn,
		registration.pushPolicy,
		// synchro_register_table takes the excluded columns before the synced
		// columns. Reversing them excludes the primary key and registers no
		// synced column.
		string(excludeColumnsValue),
		string(syncColumnsValue),
		registration.maxScopeFanout,
	); err != nil {
		// The extension reports why it rejected the registration. Dropping that
		// cause costs one full gate run to learn it again.
		return fmt.Errorf("stage synced schema transition registry failed: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return errors.New("commit synced schema transition failed")
	}
	committed = true
	return executor.bootstrapStagedTransition(ctx, database, added != "" || typeChanged != "", nonempty)
}

func transitionSchemaColumns(columns []string, removed, added string) []string {
	result := make([]string, 0, len(columns)+1)
	for _, column := range columns {
		if column != removed {
			result = append(result, column)
		}
	}
	if added != "" {
		result = append(result, added)
	}
	return result
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
