package integration

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

// mixedActivationPhase is one source transaction with row writes before,
// between, and after two registry activations. The moved row starts in
// user:fp-alice, moves to user:fp-bob before the first activation, and moves
// back after it. The inserted row starts before the first activation and
// receives the second added field after the second activation.
type mixedActivationPhase struct {
	moved    string
	inserted string
	first    string
	second   string
}

type mixedActivationCaptured struct {
	generation int64
	owner      sql.NullString
	value      sql.NullString
	first      sql.NullString
	second     sql.NullString
	deleted    bool
}

type mixedActivationGeneration struct {
	generation  int64
	parent      int64
	state       string
	requirement sql.NullInt64
	activatedBy bool
}

type mixedActivationResult struct {
	generations []mixedActivationGeneration
	transaction string
	progress    int64
	captured    map[string]mixedActivationCaptured
	edges       map[string]string
	effects     []string
}

// TestRealTransactionMembershipUsesFinalProjectionAcrossActivations proves
// SYNC-WAL-007 with the ADR 001 transaction rule. Each row event uses the
// generation of its source position. Membership and pull effects follow only
// the final projection of the whole source transaction. The proof covers a
// source rollback, a worker crash inside materialization, and a replay after
// durable materialization.
func TestRealTransactionMembershipUsesFinalProjectionAcrossActivations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	crashPhase := mixedActivationPhase{
		moved:    "00000000-0000-4000-a507-000000000001",
		inserted: "00000000-0000-4000-a507-000000000002",
		first:    "crash_first",
		second:   "crash_second",
	}
	replayPhase := mixedActivationPhase{
		moved:    "00000000-0000-4000-a507-000000000011",
		inserted: "00000000-0000-4000-a507-000000000012",
		first:    "replay_first",
		second:   "replay_second",
	}
	rolledBack := mixedActivationPhase{
		moved:    crashPhase.moved,
		inserted: "00000000-0000-4000-a507-000000000021",
		first:    "rolled_back_first",
		second:   "rolled_back_second",
	}
	for _, recordID := range []string{crashPhase.moved, replayPhase.moved} {
		if _, err := admin.ExecContext(ctx, `
			INSERT INTO public.cf_items (id, owner_id, value)
			VALUES ($1, 'fp-alice', 'baseline')`, recordID); err != nil {
			t.Fatalf("insert baseline row %s: %v", recordID, err)
		}
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", crashPhase.moved, replayPhase.moved)
	var initialGeneration, generationsBefore int64
	if err := admin.QueryRowContext(ctx, `
		SELECT (SELECT generation FROM synchro.sync_registry_generations WHERE state = 'active'),
		       (SELECT count(*) FROM synchro.sync_registry_generations)`).Scan(&initialGeneration, &generationsBefore); err != nil {
		t.Fatalf("observe the initial registry generation: %v", err)
	}

	rollback, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin rolled-back source transaction: %v", err)
	}
	if _, err := rolledBack.write(ctx, rollback); err != nil {
		_ = rollback.Rollback()
		t.Fatalf("write rolled-back source transaction: %v", err)
	}
	if err := rollback.Rollback(); err != nil {
		t.Fatalf("roll back source transaction: %v", err)
	}

	// The worker blocks on its first effect write, after it has applied every
	// segment and both activations. The crash must discard that whole work.
	lockSession, err := admin.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire effect lock session: %v", err)
	}
	defer lockSession.Close()
	effectLock, err := lockSession.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin effect lock: %v", err)
	}
	if _, err := effectLock.ExecContext(ctx, "LOCK TABLE synchro.sync_changelog IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatalf("lock pull effects: %v", err)
	}
	crashXID := commitMixedActivationPhase(t, ctx, admin, crashPhase)
	waitForMixedActivationWorkerBlock(t, ctx, admin)
	crashRealWALWorkerBackend(t, ctx, harness, admin)
	_ = effectLock.Rollback()
	waitForMixedActivationTransaction(t, ctx, admin, crashXID)
	crash := observeMixedActivationPhase(t, ctx, admin, crashPhase, crashXID, initialGeneration)

	var replayXID string
	restart, err := harness.Operator().RunWALTransactionReplayRestart(
		ctx,
		[]string{replayPhase.moved, replayPhase.inserted},
		func(ctx context.Context) error {
			transaction, err := admin.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer transaction.Rollback()
			if replayXID, err = replayPhase.write(ctx, transaction); err != nil {
				return err
			}
			return transaction.Commit()
		},
	)
	if err != nil {
		t.Fatalf("replay the mixed activation transaction: %v; %s", err, harness.FailureDiagnostics())
	}
	replay := observeMixedActivationPhase(t, ctx, admin, replayPhase, replayXID, crash.progress)
	var generationsAfter int64
	if err := admin.QueryRowContext(ctx, "SELECT count(*) FROM synchro.sync_registry_generations").Scan(&generationsAfter); err != nil {
		t.Fatalf("count registry generations: %v", err)
	}

	t.Run("assertion", func(t *testing.T) {
		if generationsAfter != generationsBefore+4 {
			t.Fatalf("registry generations = %d, want %d: the rolled-back registrations must leave none",
				generationsAfter, generationsBefore+4)
		}
		requireMixedActivationPhase(t, "crash", crash, crashPhase, initialGeneration, 0)
		requireMixedActivationPhase(t, "replay", replay, replayPhase, crash.progress, 1)
		if !restart.WorkerExitedBeforeAcknowledgement || !restart.WorkerRestarted ||
			restart.BeforeRestart.ContiguousAcknowledged || !restart.AfterRestart.ContiguousAcknowledged ||
			restart.BeforeStages != restart.AfterStages {
			t.Fatalf("replay changed materialized state or skipped the restart: %#v", restart)
		}
	})
}

// write runs the phase statements and returns the source transaction ID.
func (phase mixedActivationPhase) write(ctx context.Context, transaction *sql.Tx) (string, error) {
	register := func(column string) string {
		return fmt.Sprintf(`
			WITH parent AS MATERIALIZED (
			     SELECT r.*
			     FROM synchro.sync_registry r
			     JOIN synchro.sync_registry_generations g ON g.generation = r.registry_generation
			     WHERE g.state IN ('active', 'pending') AND g.validated
			       AND r.physical_relation_oid = 'public.cf_items'::regclass
			     ORDER BY r.registry_generation DESC
			     LIMIT 1
			 )
			 SELECT synchro.synchro_register_table(
			     format('%%I.%%I', physical_schema, physical_relation),
			     format('%%I.%%I', membership_function_schema, membership_function_name),
			     composition, pk_column, updated_at_col, deleted_at_col, push_policy,
			     exclude_columns, array_append(sync_columns, '%s'),
			     max_scope_fanout
			 )
			 FROM parent`, column)
	}
	statements := []struct {
		sql       string
		arguments []any
	}{
		{"UPDATE public.cf_items SET owner_id = 'fp-bob', value = 'moved-away' WHERE id = $1", []any{phase.moved}},
		{"INSERT INTO public.cf_items (id, owner_id, value) VALUES ($1, 'fp-alice', 'inserted-before')", []any{phase.inserted}},
		{fmt.Sprintf("ALTER TABLE public.cf_items ADD COLUMN %s text", phase.first), nil},
		{register(phase.first), nil},
		{fmt.Sprintf("UPDATE public.cf_items SET owner_id = 'fp-alice', value = 'moved-back', %s = 'first-value' WHERE id = $1", phase.first), []any{phase.moved}},
		{fmt.Sprintf("ALTER TABLE public.cf_items ADD COLUMN %s text", phase.second), nil},
		{register(phase.second), nil},
		{fmt.Sprintf("UPDATE public.cf_items SET %s = 'second-value' WHERE id = $1", phase.second), []any{phase.inserted}},
	}
	for index, statement := range statements {
		if _, err := transaction.ExecContext(ctx, statement.sql, statement.arguments...); err != nil {
			return "", fmt.Errorf("statement %d: %w", index+1, err)
		}
	}
	var xid string
	if err := transaction.QueryRowContext(ctx, "SELECT pg_catalog.xid(pg_catalog.pg_current_xact_id())::text").Scan(&xid); err != nil {
		return "", fmt.Errorf("read source transaction ID: %w", err)
	}
	return xid, nil
}

func commitMixedActivationPhase(t *testing.T, ctx context.Context, admin *sql.DB, phase mixedActivationPhase) string {
	t.Helper()
	transaction, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin mixed activation transaction: %v", err)
	}
	defer transaction.Rollback()
	xid, err := phase.write(ctx, transaction)
	if err != nil {
		t.Fatalf("write mixed activation transaction: %v", err)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatalf("commit mixed activation transaction: %v", err)
	}
	return xid
}

func waitForMixedActivationWorkerBlock(t *testing.T, ctx context.Context, admin *sql.DB) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var blocked bool
		if err := admin.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_catalog.pg_stat_activity activity
				JOIN pg_catalog.pg_locks waiting
				  ON waiting.pid = activity.pid AND NOT waiting.granted
				WHERE activity.datname = current_database()
				  AND activity.backend_type = 'synchro WAL consumer'
				  AND waiting.relation = 'synchro.sync_changelog'::regclass
				  AND waiting.mode = 'RowExclusiveLock'
			)`).Scan(&blocked); err != nil {
			t.Fatalf("observe the blocked effect write: %v", err)
		}
		if blocked {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("the WAL worker did not reach the effect write of the mixed activation transaction")
}

func waitForMixedActivationTransaction(t *testing.T, ctx context.Context, admin *sql.DB, xid string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		var acknowledged bool
		err := admin.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM synchro.sync_wal_transactions transaction
				JOIN synchro.sync_wal_progress progress ON progress.singleton
				JOIN synchro.sync_runtime_state runtime ON runtime.singleton
				JOIN pg_catalog.pg_replication_slots slot ON slot.slot_name = runtime.active_slot_name
				WHERE transaction.source_xid::text = $1
				  AND progress.acknowledged_end_lsn >= transaction.end_lsn
				  AND slot.confirmed_flush_lsn = progress.acknowledged_end_lsn
			)`, xid).Scan(&acknowledged)
		if err == nil && acknowledged {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("the mixed activation transaction %s was not materialized and acknowledged after the crash", xid)
}

func observeMixedActivationPhase(
	t *testing.T,
	ctx context.Context,
	admin *sql.DB,
	phase mixedActivationPhase,
	xid string,
	activeBefore int64,
) mixedActivationResult {
	t.Helper()
	result := mixedActivationResult{
		captured: make(map[string]mixedActivationCaptured),
		edges:    make(map[string]string),
	}
	if err := admin.QueryRowContext(ctx, `
		SELECT pg_catalog.format('registry_generation=%s events=%s effects=%s replays=%s hash_format=%s',
		           registry_generation, event_count, effect_count, replay_count, content_hash_format),
		       (SELECT registry_generation FROM synchro.sync_wal_progress WHERE singleton)
		FROM synchro.sync_wal_transactions
		WHERE source_xid::text = $1`, xid).Scan(&result.transaction, &result.progress); err != nil {
		t.Fatalf("observe the mixed activation transaction record: %v", err)
	}
	rows, err := admin.QueryContext(ctx, `
		WITH RECURSIVE chain AS (
			SELECT generation, parent_generation, state, source_requirement, activation_commit_lsn
			FROM synchro.sync_registry_generations
			WHERE parent_generation = $1
			UNION ALL
			SELECT child.generation, child.parent_generation, child.state,
			       child.source_requirement, child.activation_commit_lsn
			FROM synchro.sync_registry_generations child
			JOIN chain ON child.parent_generation = chain.generation
		)
		SELECT chain.generation, chain.parent_generation, chain.state, chain.source_requirement,
		       chain.activation_commit_lsn IS NOT DISTINCT FROM transaction.commit_lsn
		FROM chain
		CROSS JOIN synchro.sync_wal_transactions transaction
		WHERE transaction.source_xid::text = $2
		ORDER BY chain.generation`, activeBefore, xid)
	if err != nil {
		t.Fatalf("observe the activated generations: %v", err)
	}
	for rows.Next() {
		var generation mixedActivationGeneration
		if err := rows.Scan(&generation.generation, &generation.parent, &generation.state,
			&generation.requirement, &generation.activatedBy); err != nil {
			t.Fatalf("scan an activated generation: %v", err)
		}
		result.generations = append(result.generations, generation)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("read the activated generations: %v", err)
	}
	for _, recordID := range []string{phase.moved, phase.inserted} {
		var captured mixedActivationCaptured
		if err := admin.QueryRowContext(ctx, `
			SELECT captured.registry_generation,
			       captured.row_data ->> owner.field_id::text,
			       captured.row_data ->> value.field_id::text,
			       captured.row_data ->> first.field_id::text,
			       captured.row_data ->> second.field_id::text,
			       captured.deleted
			FROM synchro.sync_captured_rows captured
			JOIN synchro.sync_registry registry
			  ON registry.registry_generation = captured.registry_generation
			 AND registry.relation_id = captured.relation_id
			JOIN synchro.sync_registry_fields owner
			  ON owner.registry_generation = captured.registry_generation
			 AND owner.relation_id = captured.relation_id AND owner.physical_column = 'owner_id'
			JOIN synchro.sync_registry_fields value
			  ON value.registry_generation = captured.registry_generation
			 AND value.relation_id = captured.relation_id AND value.physical_column = 'value'
			JOIN synchro.sync_registry_fields first
			  ON first.registry_generation = captured.registry_generation
			 AND first.relation_id = captured.relation_id AND first.physical_column = $2
			JOIN synchro.sync_registry_fields second
			  ON second.registry_generation = captured.registry_generation
			 AND second.relation_id = captured.relation_id AND second.physical_column = $3
			WHERE registry.table_name = 'cf_items' AND captured.record_id = $1`,
			recordID, phase.first, phase.second).Scan(
			&captured.generation, &captured.owner, &captured.value,
			&captured.first, &captured.second, &captured.deleted); err != nil {
			t.Fatalf("observe captured row %s: %v", recordID, err)
		}
		result.captured[recordID] = captured
		var edges string
		if err := admin.QueryRowContext(ctx, `
			SELECT COALESCE(string_agg(bucket_id, ',' ORDER BY bucket_id), '')
			FROM synchro.sync_bucket_edges
			WHERE table_name = 'cf_items' AND record_id = $1`, recordID).Scan(&edges); err != nil {
			t.Fatalf("observe membership edges of %s: %v", recordID, err)
		}
		result.edges[recordID] = edges
	}
	effects, err := admin.QueryContext(ctx, `
		SELECT pg_catalog.format('%s|%s|%s|%s|%s|%s',
		           effect.bucket_id, effect.record_id, effect.operation,
		           effect.event_ordinal, effect.effect_ordinal,
		           CASE WHEN effect.row_version = captured.row_version
		                THEN 'captured-version' ELSE 'other-version' END)
		FROM synchro.sync_changelog effect
		JOIN synchro.sync_wal_transactions transaction
		  ON transaction.stream_generation = effect.stream_generation
		 AND transaction.commit_lsn = effect.commit_lsn
		LEFT JOIN synchro.sync_captured_rows captured
		  ON captured.relation_id = effect.relation_id
		 AND captured.record_id = effect.record_id
		WHERE transaction.source_xid::text = $1
		ORDER BY effect.event_ordinal, effect.effect_ordinal, effect.bucket_id, effect.record_id`, xid)
	if err != nil {
		t.Fatalf("observe the pull effects of the mixed activation transaction: %v", err)
	}
	for effects.Next() {
		var effect string
		if err := effects.Scan(&effect); err != nil {
			t.Fatalf("scan a pull effect: %v", err)
		}
		result.effects = append(result.effects, effect)
	}
	if err := effects.Close(); err != nil {
		t.Fatalf("read the pull effects: %v", err)
	}
	return result
}

// requireMixedActivationPhase compares one phase with values authored from the
// contract. Row ordinals number the four row events of the transaction from 0.
// Operation 1 is insert and 2 is update.
func requireMixedActivationPhase(
	t *testing.T,
	name string,
	result mixedActivationResult,
	phase mixedActivationPhase,
	activeBefore int64,
	replays int,
) {
	t.Helper()
	if len(result.generations) != 2 {
		t.Fatalf("%s phase activated generations %#v, want two chained generations", name, result.generations)
	}
	first, second := result.generations[0], result.generations[1]
	wantGenerations := []mixedActivationGeneration{
		{generation: first.generation, parent: activeBefore, state: "superseded", requirement: sql.NullInt64{Int64: 0, Valid: true}, activatedBy: true},
		{generation: second.generation, parent: first.generation, state: "active", requirement: sql.NullInt64{Int64: 0, Valid: true}, activatedBy: true},
	}
	if !reflect.DeepEqual(result.generations, wantGenerations) {
		t.Fatalf("%s phase generations = %#v, want %#v", name, result.generations, wantGenerations)
	}
	wantTransaction := fmt.Sprintf("registry_generation=%d events=4 effects=2 replays=%d hash_format=2", activeBefore, replays)
	if result.transaction != wantTransaction || result.progress != second.generation {
		t.Fatalf("%s phase transaction = %q progress generation %d, want %q and %d",
			name, result.transaction, result.progress, wantTransaction, second.generation)
	}
	text := func(value string) sql.NullString { return sql.NullString{String: value, Valid: true} }
	wantCaptured := map[string]mixedActivationCaptured{
		phase.moved:    {generation: second.generation, owner: text("fp-alice"), value: text("moved-back"), first: text("first-value")},
		phase.inserted: {generation: second.generation, owner: text("fp-alice"), value: text("inserted-before"), second: text("second-value")},
	}
	if !reflect.DeepEqual(result.captured, wantCaptured) {
		t.Fatalf("%s phase captured rows = %#v, want %#v", name, result.captured, wantCaptured)
	}
	wantEdges := map[string]string{phase.moved: "user:fp-alice", phase.inserted: "user:fp-alice"}
	if !reflect.DeepEqual(result.edges, wantEdges) {
		t.Fatalf("%s phase membership edges = %#v, want %#v", name, result.edges, wantEdges)
	}
	wantEffects := []string{
		"user:fp-alice|" + phase.moved + "|2|2|0|captured-version",
		"user:fp-alice|" + phase.inserted + "|1|3|0|captured-version",
	}
	if !reflect.DeepEqual(result.effects, wantEffects) {
		t.Fatalf("%s phase pull effects =\n%s\nwant\n%s", name, strings.Join(result.effects, "\n"), strings.Join(wantEffects, "\n"))
	}
}
