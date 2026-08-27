package blackbox

import (
	"context"
	"database/sql"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

// ConfiguredBoundServerObservation records the terminal PostgreSQL result of
// one configured-bound operation.
type ConfiguredBoundServerObservation struct {
	Accepted bool
	SQLState string
}

// ExerciseConfiguredFanoutLimit calls the production registration boundary
// and rolls back its registry change after the server result is observed.
func (executor *OperatorExecutor) ExerciseConfiguredFanoutLimit(ctx context.Context, value int) (ConfiguredBoundServerObservation, error) {
	return executor.exerciseConfiguredLimit(ctx, `SELECT synchro.synchro_register_table(
		'public.cf_items', 'public.cf_items_membership', 'single_scope',
		'id', 'updated_at', 'deleted_at', 'enabled',
		ARRAY[]::text[], ARRAY[]::text[], $1::integer
	)`, value)
}

// ExerciseConfiguredImpactLimit calls the production dependency-registration
// boundary and rolls back its registry change after the result is observed.
func (executor *OperatorExecutor) ExerciseConfiguredImpactLimit(ctx context.Context, value int) (ConfiguredBoundServerObservation, error) {
	return executor.exerciseConfiguredLimit(ctx, `SELECT synchro.synchro_register_membership_dependency(
		'cf_document_access', 'cf_document_members',
		'public.cf_document_access_impact',
		ARRAY['id', 'document_id', 'owner_id']::text[], $1::integer
	)`, value)
}

// ExerciseConfiguredCompactionLimit calls the production compaction boundary
// in a transaction that cannot retain diagnostic state.
func (executor *OperatorExecutor) ExerciseConfiguredCompactionLimit(ctx context.Context, value int) (ConfiguredBoundServerObservation, error) {
	return executor.exerciseConfiguredLimit(
		ctx,
		"SELECT synchro.synchro_compact('30 days', $1::integer)",
		value,
	)
}

// ExerciseConfiguredBackfillLimit calls the production backfill boundary in a
// transaction that cannot retain diagnostic state.
func (executor *OperatorExecutor) ExerciseConfiguredBackfillLimit(ctx context.Context, value int) (ConfiguredBoundServerObservation, error) {
	return executor.exerciseConfiguredLimit(
		ctx,
		"SELECT synchro.synchro_backfill_bucket_edges('cf_items', $1::bigint)",
		value,
	)
}

func (executor *OperatorExecutor) exerciseConfiguredLimit(ctx context.Context, statement string, value int) (ConfiguredBoundServerObservation, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady || ctx == nil {
		return ConfiguredBoundServerObservation{}, errors.New("operator executor is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return ConfiguredBoundServerObservation{}, errors.New("configured limit context expired")
	}
	database, err := executor.harness.openDatabase(
		ctx,
		executor.harness.names.Database,
		executor.harness.env.Admin,
		false,
	)
	if err != nil {
		return ConfiguredBoundServerObservation{}, errors.New("open configured limit connection failed")
	}
	defer database.Close()

	transaction, err := database.BeginTx(ctx, nil)
	if err != nil {
		return ConfiguredBoundServerObservation{}, errors.New("begin configured limit transaction failed")
	}
	_, operationErr := transaction.ExecContext(ctx, statement, value)
	rollbackErr := transaction.Rollback()
	if rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
		return ConfiguredBoundServerObservation{}, errors.New("roll back configured limit transaction failed")
	}
	if operationErr != nil && ctx.Err() != nil {
		return ConfiguredBoundServerObservation{}, errors.New("configured limit operation context expired")
	}
	if operationErr == nil {
		return ConfiguredBoundServerObservation{Accepted: true}, nil
	}
	var postgresError *pgconn.PgError
	if !errors.As(operationErr, &postgresError) || postgresError.Code != "XX000" {
		return ConfiguredBoundServerObservation{}, errors.New("configured limit failed outside limit validation")
	}
	return ConfiguredBoundServerObservation{SQLState: postgresError.Code}, nil
}
