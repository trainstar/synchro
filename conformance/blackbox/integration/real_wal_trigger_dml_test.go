package integration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
)

var realTriggerDMLFunctions = []string{
	`CREATE FUNCTION public.cf_touch_document() RETURNS trigger LANGUAGE plpgsql AS $$
	BEGIN
		UPDATE public.cf_documents
		SET updated_at = clock_timestamp()
		WHERE id = COALESCE(NEW.document_id, OLD.document_id);
		RETURN NULL;
	END
	$$`,
	`CREATE FUNCTION public.cf_stamp_document() RETURNS trigger LANGUAGE plpgsql AS $$
	BEGIN
		UPDATE public.cf_documents
		SET title = NEW.title || '-stamped'
		WHERE id = NEW.id;
		RETURN NULL;
	END
	$$`,
}

type realTriggerDMLStatement struct {
	statement string
	arguments []any
}

type realTriggerDMLTrigger struct {
	name     string
	event    string
	relation string
	function string
}

type realTriggerDMLRow struct {
	relation string
	recordID string
	column   string
	instant  bool
	deleted  bool
}

type realTriggerDMLCase struct {
	name           string
	setup          []realTriggerDMLStatement
	setupKey       string
	trigger        realTriggerDMLTrigger
	change         realTriggerDMLStatement
	changeKey      string
	changeFences   int
	rows           []realTriggerDMLRow
	captureRecords []string
}

// TestRealWALCorrelatesTriggerDMLPerRowIdentity proves SYNC-WAL-009 for
// application trigger DML in statements that change more than one row.
func TestRealWALCorrelatesTriggerDMLPerRowIdentity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	for _, statement := range realTriggerDMLFunctions {
		if _, err := admin.ExecContext(ctx, statement); err != nil {
			t.Fatalf("create trigger DML function: %v", err)
		}
	}
	waitForIssue49CanonicalHealth(t, ctx, admin, true)

	touchNotes := func(name, event string) realTriggerDMLTrigger {
		return realTriggerDMLTrigger{name: name, event: event, relation: "cf_document_notes", function: "cf_touch_document"}
	}
	insertDocument := func(documentID string) realTriggerDMLStatement {
		return realTriggerDMLStatement{
			"INSERT INTO cf_documents (id, owner_id, title) VALUES ($1, 'diagnostic-user', 'trigger-dml-parent')",
			[]any{documentID},
		}
	}
	insertNotes := func(documentID, firstID, secondID string) realTriggerDMLStatement {
		return realTriggerDMLStatement{
			"INSERT INTO cf_document_notes (id, document_id, author_id, body) VALUES ($1, $3, 'diagnostic-user', 'trigger-dml-first'), ($2, $3, 'diagnostic-user', 'trigger-dml-second')",
			[]any{firstID, secondID, documentID},
		}
	}
	document := func(documentID string) realTriggerDMLRow {
		return realTriggerDMLRow{relation: "cf_documents", recordID: documentID, column: "updated_at", instant: true}
	}
	note := func(noteID string, deleted bool) realTriggerDMLRow {
		return realTriggerDMLRow{relation: "cf_document_notes", recordID: noteID, column: "body", deleted: deleted}
	}
	insertedNotesCase := func(name, triggerName, prefix string) realTriggerDMLCase {
		documentID := prefix + "001"
		firstID, secondID := prefix+"011", prefix+"012"
		return realTriggerDMLCase{
			name:         name,
			setup:        []realTriggerDMLStatement{insertDocument(documentID)},
			setupKey:     documentID,
			trigger:      touchNotes(triggerName, "INSERT"),
			change:       insertNotes(documentID, firstID, secondID),
			changeKey:    firstID,
			changeFences: 4,
			rows:         []realTriggerDMLRow{document(documentID), note(firstID, false), note(secondID, false)},
		}
	}
	existingNotesSetup := func(prefix string) []realTriggerDMLStatement {
		return []realTriggerDMLStatement{insertDocument(prefix + "001"), insertNotes(prefix+"001", prefix+"011", prefix+"012")}
	}

	cases := []realTriggerDMLCase{
		insertedNotesCase("multi-row-insert-trigger-after-fence", "trigger_touch_document", "00000000-0000-4000-8178-00000000a"),
		{
			name:         "multi-row-update-trigger-after-fence",
			setup:        existingNotesSetup("00000000-0000-4000-8178-00000000b"),
			setupKey:     "00000000-0000-4000-8178-00000000b011",
			trigger:      touchNotes("trigger_touch_document", "UPDATE"),
			change:       realTriggerDMLStatement{"UPDATE cf_document_notes SET body = body || '-updated' WHERE id IN ($1, $2)", []any{"00000000-0000-4000-8178-00000000b011", "00000000-0000-4000-8178-00000000b012"}},
			changeKey:    "00000000-0000-4000-8178-00000000b011",
			changeFences: 4,
			rows: []realTriggerDMLRow{
				document("00000000-0000-4000-8178-00000000b001"),
				note("00000000-0000-4000-8178-00000000b011", false),
				note("00000000-0000-4000-8178-00000000b012", false),
			},
		},
		{
			name:         "multi-row-delete-trigger-after-fence",
			setup:        existingNotesSetup("00000000-0000-4000-8178-00000000c"),
			setupKey:     "00000000-0000-4000-8178-00000000c011",
			trigger:      touchNotes("trigger_touch_document", "DELETE"),
			change:       realTriggerDMLStatement{"DELETE FROM cf_document_notes WHERE id IN ($1, $2)", []any{"00000000-0000-4000-8178-00000000c011", "00000000-0000-4000-8178-00000000c012"}},
			changeKey:    "00000000-0000-4000-8178-00000000c011",
			changeFences: 4,
			rows: []realTriggerDMLRow{
				document("00000000-0000-4000-8178-00000000c001"),
				note("00000000-0000-4000-8178-00000000c011", true),
				note("00000000-0000-4000-8178-00000000c012", true),
			},
		},
		insertedNotesCase("multi-row-insert-trigger-before-fence", "audit_touch_document", "00000000-0000-4000-8178-00000000d"),
		{
			name:    "multi-row-insert-same-row-trigger",
			trigger: realTriggerDMLTrigger{name: "trigger_stamp_document", event: "INSERT", relation: "cf_documents", function: "cf_stamp_document"},
			change: realTriggerDMLStatement{
				"INSERT INTO cf_documents (id, owner_id, title) VALUES ($1, 'diagnostic-user', 'trigger-dml-first'), ($2, 'diagnostic-user', 'trigger-dml-second')",
				[]any{"00000000-0000-4000-8178-00000000e001", "00000000-0000-4000-8178-00000000e002"},
			},
			changeKey:    "00000000-0000-4000-8178-00000000e001",
			changeFences: 4,
			rows: []realTriggerDMLRow{
				{relation: "cf_documents", recordID: "00000000-0000-4000-8178-00000000e001", column: "title"},
				{relation: "cf_documents", recordID: "00000000-0000-4000-8178-00000000e002", column: "title"},
			},
		},
		{
			name:     "insert-select-capture-dependency-trigger",
			setup:    []realTriggerDMLStatement{insertDocument("00000000-0000-4000-8178-00000000f001")},
			setupKey: "00000000-0000-4000-8178-00000000f001",
			trigger:  realTriggerDMLTrigger{name: "trigger_touch_document", event: "INSERT", relation: "cf_document_access", function: "cf_touch_document"},
			change: realTriggerDMLStatement{
				"INSERT INTO cf_document_access (id, document_id, owner_id) SELECT access_id, $3::uuid, 'diagnostic-user' FROM unnest(ARRAY[$1::uuid, $2::uuid]) AS access_id",
				[]any{"00000000-0000-4000-8178-00000000f021", "00000000-0000-4000-8178-00000000f022", "00000000-0000-4000-8178-00000000f001"},
			},
			changeKey:      "00000000-0000-4000-8178-00000000f021",
			changeFences:   4,
			rows:           []realTriggerDMLRow{document("00000000-0000-4000-8178-00000000f001")},
			captureRecords: []string{"00000000-0000-4000-8178-00000000f021", "00000000-0000-4000-8178-00000000f022"},
		},
	}

	t.Run("assertion", func(t *testing.T) {
		for _, test := range cases {
			t.Run(test.name, func(t *testing.T) {
				runRealTriggerDMLCase(t, ctx, harness, admin, test)
			})
		}
	})
}

func runRealTriggerDMLCase(t *testing.T, ctx context.Context, harness *blackbox.Harness, admin *sql.DB, test realTriggerDMLCase) {
	t.Helper()
	if len(test.setup) > 0 {
		commitRealTriggerDMLStatements(t, ctx, harness, test.setup)
		waitForRealTriggerDMLCommit(t, ctx, harness, admin, test.setupKey)
	}
	activateRealTriggerDMLTrigger(t, ctx, admin, test.trigger)
	commitRealTriggerDMLStatements(t, ctx, harness, []realTriggerDMLStatement{test.change})
	if fences := waitForRealTriggerDMLCommit(t, ctx, harness, admin, test.changeKey); fences != test.changeFences {
		t.Fatalf("trigger DML commit emitted %d fences, want %d", fences, test.changeFences)
	}
	requireRealTriggerDMLCaptureHealthy(t, ctx, admin)
	for _, row := range test.rows {
		requireRealTriggerDMLRow(t, ctx, admin, row)
	}
	for _, recordID := range test.captureRecords {
		requireRealTriggerDMLCaptureDependencyRow(t, ctx, admin, recordID)
	}
}

func activateRealTriggerDMLTrigger(t *testing.T, ctx context.Context, admin *sql.DB, trigger realTriggerDMLTrigger) {
	t.Helper()
	if _, err := admin.ExecContext(ctx, fmt.Sprintf(
		"CREATE TRIGGER %s AFTER %s ON public.%s FOR EACH ROW EXECUTE FUNCTION public.%s()",
		trigger.name, trigger.event, trigger.relation, trigger.function,
	)); err != nil {
		t.Fatalf("create trigger %s on %s: %v", trigger.name, trigger.relation, err)
	}
	t.Cleanup(func() {
		dropContext, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dropCancel()
		if _, err := admin.ExecContext(dropContext, fmt.Sprintf("DROP TRIGGER %s ON public.%s", trigger.name, trigger.relation)); err != nil {
			t.Errorf("drop trigger %s on %s: %v", trigger.name, trigger.relation, err)
		}
	})
}

func commitRealTriggerDMLStatements(t *testing.T, ctx context.Context, harness *blackbox.Harness, statements []realTriggerDMLStatement) {
	t.Helper()
	transaction, err := harness.Source().BeginTx(ctx)
	if err != nil {
		t.Fatalf("begin trigger DML source transaction: %v", err)
	}
	for step, statement := range statements {
		if _, err := transaction.ExecContext(ctx, statement.statement, statement.arguments...); err != nil {
			_ = transaction.Rollback()
			t.Fatalf("execute trigger DML source statement %d: %v", step+1, err)
		}
	}
	if err := transaction.Commit(); err != nil {
		t.Fatalf("commit trigger DML source transaction: %v", err)
	}
}

// waitForRealTriggerDMLCommit waits until every fence of the latest source
// transaction that wrote the row key is materialized. It returns the fence count.
func waitForRealTriggerDMLCommit(t *testing.T, ctx context.Context, harness *blackbox.Harness, admin *sql.DB, rowKey string) int {
	t.Helper()
	var fences, materialized int
	var poisonClass, poisonDetail sql.NullString
	var err error
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		err = admin.QueryRowContext(ctx, `
			WITH latest AS (
				SELECT fence.transaction_xid
				FROM synchro.sync_write_fences fence
				WHERE $1 IN (
					fence.old_record_id, fence.new_record_id,
					fence.old_capture_key ->> 'id', fence.new_capture_key ->> 'id'
				)
				ORDER BY fence.transaction_xid::text::bigint DESC
				LIMIT 1
			), poison AS (
				SELECT failure_class, left(failure_detail, 256) AS failure_detail
				FROM synchro.sync_wal_poison
				WHERE lifecycle = 'active'
				ORDER BY id DESC
				LIMIT 1
			)
			SELECT count(fence.fence_id),
			       count(fence.fence_id) FILTER (WHERE fence.coverage = 'materialized'),
			       (SELECT failure_class FROM poison),
			       (SELECT failure_detail FROM poison)
			FROM latest
			JOIN synchro.sync_write_fences fence USING (transaction_xid)`, rowKey).Scan(&fences, &materialized, &poisonClass, &poisonDetail)
		if err == nil && poisonClass.Valid {
			t.Fatalf("trigger DML commit for row %s left active WAL poison: failure_class=%s failure_detail=%q", rowKey, poisonClass.String, poisonDetail.String)
		}
		if err == nil && fences > 0 && materialized == fences {
			return fences
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("trigger DML commit for row %s did not materialize: fences=%d materialized=%d err=%v; %s", rowKey, fences, materialized, err, harness.FailureDiagnostics())
	return 0
}

func requireRealTriggerDMLCaptureHealthy(t *testing.T, ctx context.Context, admin *sql.DB) {
	t.Helper()
	var activePoison int
	if err := admin.QueryRowContext(ctx, "SELECT count(*) FROM synchro.sync_wal_poison WHERE lifecycle = 'active'").Scan(&activePoison); err != nil {
		t.Fatalf("count active trigger DML poison: %v", err)
	}
	checks := issue49HealthChecks(t, loadIssue49Health(t, ctx, admin))
	if activePoison != 0 || checks["poison"] != "ok" {
		t.Fatalf("trigger DML capture poison state is invalid: active_poison=%d poison_check=%q", activePoison, checks["poison"])
	}
}

func requireRealTriggerDMLRow(t *testing.T, ctx context.Context, admin *sql.DB, row realTriggerDMLRow) {
	t.Helper()
	var sourcePresent, capturedPresent, capturedDeleted, versionPresent, versionDeleted, versionsEqual bool
	var sourceValue, capturedValue sql.NullString
	err := admin.QueryRowContext(ctx, fmt.Sprintf(`
		WITH registered AS (
			SELECT registry.relation_id, field.field_id::text AS field_id
			FROM synchro.sync_registry registry
			JOIN synchro.sync_registry_generations generation
			  ON generation.generation = registry.registry_generation
			JOIN synchro.sync_registry_fields field
			  ON field.registry_generation = registry.registry_generation
			 AND field.relation_id = registry.relation_id
			WHERE generation.state = 'active'
			  AND registry.physical_schema = 'public'
			  AND registry.physical_relation = $1
			  AND field.physical_column = $3
		)
		SELECT source.id IS NOT NULL,
		       to_jsonb(source.%[2]s) #>> '{}',
		       captured.record_id IS NOT NULL,
		       COALESCE(captured.deleted, false),
		       captured.row_data ->> registered.field_id,
		       version.record_id IS NOT NULL,
		       COALESCE(version.deleted, false),
		       COALESCE(captured.row_version = version.row_version, false)
		FROM registered
		LEFT JOIN public.%[1]s source ON source.id::text = $2
		LEFT JOIN synchro.sync_captured_rows captured
		  ON captured.relation_id = registered.relation_id AND captured.record_id = $2
		LEFT JOIN synchro.sync_row_versions version
		  ON version.relation_id = registered.relation_id AND version.record_id = $2`, row.relation, row.column),
		row.relation, row.recordID, row.column,
	).Scan(&sourcePresent, &sourceValue, &capturedPresent, &capturedDeleted, &capturedValue, &versionPresent, &versionDeleted, &versionsEqual)
	if err != nil {
		t.Fatalf("observe trigger DML row %s %s: %v", row.relation, row.recordID, err)
	}
	if row.deleted {
		if sourcePresent || (capturedPresent && !capturedDeleted) || !versionPresent || !versionDeleted {
			t.Fatalf("trigger DML row %s %s is not deleted: source=%t captured=%t captured_deleted=%t version=%t version_deleted=%t",
				row.relation, row.recordID, sourcePresent, capturedPresent, capturedDeleted, versionPresent, versionDeleted)
		}
		return
	}
	if !sourcePresent || !capturedPresent || capturedDeleted || !versionPresent || versionDeleted || !versionsEqual ||
		!realTriggerDMLValuesEqual(row.instant, sourceValue, capturedValue) {
		t.Fatalf("trigger DML row %s %s capture differs from source: source=%t source_%s=%q captured=%t captured_deleted=%t captured_%s=%q version=%t version_deleted=%t versions_equal=%t",
			row.relation, row.recordID, sourcePresent, row.column, sourceValue.String, capturedPresent, capturedDeleted, row.column, capturedValue.String, versionPresent, versionDeleted, versionsEqual)
	}
}

func realTriggerDMLValuesEqual(instant bool, source, captured sql.NullString) bool {
	if !source.Valid || !captured.Valid {
		return false
	}
	if !instant {
		return source.String == captured.String
	}
	sourceTime, sourceErr := time.Parse(time.RFC3339Nano, source.String)
	capturedTime, capturedErr := time.Parse(time.RFC3339Nano, captured.String)
	return sourceErr == nil && capturedErr == nil && sourceTime.Equal(capturedTime)
}

func requireRealTriggerDMLCaptureDependencyRow(t *testing.T, ctx context.Context, admin *sql.DB, recordID string) {
	t.Helper()
	var rows, deletedRows int
	if err := admin.QueryRowContext(ctx, `
		SELECT count(*), count(*) FILTER (WHERE captured.deleted)
		FROM synchro.sync_capture_dependency_rows captured
		JOIN synchro.sync_registry registry ON registry.relation_id = captured.relation_id
		JOIN synchro.sync_registry_generations generation
		  ON generation.generation = registry.registry_generation
		WHERE generation.state = 'active'
		  AND registry.physical_schema = 'public'
		  AND registry.physical_relation = 'cf_document_access'
		  AND captured.capture_key ->> 'id' = $1`, recordID).Scan(&rows, &deletedRows); err != nil {
		t.Fatalf("observe trigger DML capture dependency row %s: %v", recordID, err)
	}
	if rows != 1 || deletedRows != 0 {
		t.Fatalf("trigger DML capture dependency row %s is invalid: rows=%d deleted=%d", recordID, rows, deletedRows)
	}
}
