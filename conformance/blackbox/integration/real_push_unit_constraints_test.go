package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestRealPushUnitConstraintBoundary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	if err := harness.Operator().RegisterSourceFilledItems(ctx); err != nil {
		t.Fatalf("register source-filled fixture table: %v", err)
	}
	waitForRealSourceFilledTable(t, ctx, harness)
	admin := openIssue49Admin(t, ctx, harness)
	installRealPushUnitConstraintFixtures(t, ctx, admin)
	waitForIssue49CanonicalHealth(t, ctx, admin, true)

	client := connectRealProtocolClient(t, ctx, harness, token, "push-unit-constraint-client")
	rebuildRealScope(t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-8f03-00000000b001")
	rebuildRealScope(t, ctx, harness, token, client, "cf:global", "00000000-0000-4000-8f03-00000000b002")
	table := requireRealTable(t, client, "cf_source_filled_items")

	t.Run("assertion", func(t *testing.T) {
		validID := "00000000-0000-4000-8f03-000000000001"
		rejectedID := "00000000-0000-4000-8f03-000000000002"
		validMutationID := "00000000-0000-4000-8f03-000000000003"
		rejectedMutationID := "00000000-0000-4000-8f03-000000000004"
		status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
			client,
			"00000000-0000-4000-8f03-000000000005",
			[]map[string]any{
				{
					"mutation_id":     validMutationID,
					"table":           table.ID,
					"pk":              map[string]any{table.PrimaryKeyField: validID},
					"authored_schema": client.Schema,
					"op":              "insert",
					"client_version":  phase4ClientVersion,
					"columns":         map[string]any{table.ValueField: "accept-deferred"},
				},
				{
					"mutation_id":     rejectedMutationID,
					"table":           table.ID,
					"pk":              map[string]any{table.PrimaryKeyField: rejectedID},
					"authored_schema": client.Schema,
					"op":              "insert",
					"client_version":  phase4ClientVersion,
					"columns":         map[string]any{table.ValueField: "reject-deferred"},
				},
			},
		))
		if status != http.StatusOK {
			t.Fatalf("deferred check push status = %d, want 200: %#v", status, response)
		}
		accepted := requireOutcomeList(t, response, "accepted")
		rejected := requireOutcomeList(t, response, "rejected")
		if len(accepted) != 1 || accepted[0]["mutation_id"] != validMutationID ||
			accepted[0]["status"] != "applied" ||
			len(rejected) != 1 || rejected[0]["mutation_id"] != rejectedMutationID ||
			rejected[0]["status"] != "rejected_terminal" ||
			rejected[0]["code"] != "validation_failed" {
			t.Fatalf("deferred check push outcomes are invalid: %#v", response)
		}
		var validRows int
		if err := admin.QueryRowContext(
			ctx,
			"SELECT count(*) FROM public.cf_source_filled_items WHERE id = $1::uuid",
			validID,
		).Scan(&validRows); err != nil || validRows != 1 {
			t.Fatalf("accepted deferred check source rows = %d, want 1: %v", validRows, err)
		}
		requireRealPushUnitNoDurableRecord(t, ctx, admin, rejectedID, rejectedMutationID)
	})

	t.Run("assertion", func(t *testing.T) {
		recordID := "00000000-0000-4000-8f03-000000000011"
		mutationID := "00000000-0000-4000-8f03-000000000012"
		if _, err := admin.ExecContext(
			ctx,
			`INSERT INTO public.cf_source_filled_items (id, owner_id, value)
			 VALUES ($1::uuid, 'diagnostic-user', 'update-original')`,
			recordID,
		); err != nil {
			t.Fatalf("insert deferred update source row: %v", err)
		}
		beforeVersion := loadReleaseRowVersion(t, ctx, admin, "cf_source_filled_items", recordID)
		var beforeRow string
		if err := admin.QueryRowContext(
			ctx,
			"SELECT to_jsonb(item)::text FROM public.cf_source_filled_items item WHERE id = $1::uuid",
			recordID,
		).Scan(&beforeRow); err != nil {
			t.Fatalf("load deferred update source row: %v", err)
		}

		status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
			client,
			"00000000-0000-4000-8f03-000000000013",
			[]map[string]any{{
				"mutation_id":     mutationID,
				"table":           table.ID,
				"pk":              map[string]any{table.PrimaryKeyField: recordID},
				"authored_schema": client.Schema,
				"op":              "update",
				"base_version":    beforeVersion,
				"client_version":  phase4ClientVersion,
				"columns":         map[string]any{table.ValueField: "reject-deferred"},
			}},
		))
		if status != http.StatusOK {
			t.Fatalf("deferred update status = %d, want 200: %#v", status, response)
		}
		rejected := requireOutcomeList(t, response, "rejected")
		if len(requireOutcomeList(t, response, "accepted")) != 0 ||
			len(rejected) != 1 || rejected[0]["mutation_id"] != mutationID ||
			rejected[0]["status"] != "rejected_terminal" ||
			rejected[0]["code"] != "validation_failed" {
			t.Fatalf("deferred update outcome is invalid: %#v", response)
		}
		var afterRow string
		if err := admin.QueryRowContext(
			ctx,
			"SELECT to_jsonb(item)::text FROM public.cf_source_filled_items item WHERE id = $1::uuid",
			recordID,
		).Scan(&afterRow); err != nil {
			t.Fatalf("reload deferred update source row: %v", err)
		}
		afterVersion := loadReleaseRowVersion(t, ctx, admin, "cf_source_filled_items", recordID)
		if afterRow != beforeRow || afterVersion != beforeVersion {
			t.Fatalf(
				"deferred update changed source state: before_row=%s after_row=%s before_version=%s after_version=%s",
				beforeRow,
				afterRow,
				beforeVersion,
				afterVersion,
			)
		}
		var fences int
		if err := admin.QueryRowContext(
			ctx,
			"SELECT count(*) FROM synchro.sync_write_fences WHERE mutation_id = $1",
			mutationID,
		).Scan(&fences); err != nil || fences != 0 {
			t.Fatalf("deferred update write fences = %d, want 0: %v", fences, err)
		}
	})

	t.Run("assertion", func(t *testing.T) {
		firstID := "00000000-0000-4000-8f03-000000000021"
		secondID := "00000000-0000-4000-8f03-000000000022"
		mutationIDs := []string{
			"00000000-0000-4000-8f03-000000000023",
			"00000000-0000-4000-8f03-000000000024",
		}
		status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
			client,
			"00000000-0000-4000-8f03-000000000025",
			[]map[string]any{
				{
					"mutation_id":     mutationIDs[0],
					"table":           table.ID,
					"pk":              map[string]any{table.PrimaryKeyField: firstID},
					"authored_schema": client.Schema,
					"op":              "insert",
					"client_version":  phase4ClientVersion,
					"columns":         map[string]any{table.ValueField: "transient-deferred"},
				},
				{
					"mutation_id":     mutationIDs[1],
					"table":           table.ID,
					"pk":              map[string]any{table.PrimaryKeyField: secondID},
					"authored_schema": client.Schema,
					"op":              "insert",
					"client_version":  phase4ClientVersion,
					"columns":         map[string]any{table.ValueField: "transient-deferred"},
				},
			},
		))
		if status != http.StatusOK {
			t.Fatalf("transient deferred push status = %d, want 200: %#v", status, response)
		}
		accepted := requireOutcomeList(t, response, "accepted")
		if len(accepted) != 2 || len(requireOutcomeList(t, response, "rejected")) != 0 {
			t.Fatalf("transient deferred push outcomes are invalid: %#v", response)
		}
		for index, outcome := range accepted {
			if outcome["mutation_id"] != mutationIDs[index] || outcome["status"] != "applied" {
				t.Fatalf("transient deferred outcome %d is invalid: %#v", index, outcome)
			}
		}
		var sourceRows, parentRows, childRows int
		if err := admin.QueryRowContext(ctx, `
			SELECT
			    (SELECT count(*) FROM public.cf_source_filled_items WHERE id IN ($1::uuid, $2::uuid)),
			    (SELECT count(*) FROM public.cf_deferred_parent WHERE id IN ($1::uuid, $2::uuid)),
			    (SELECT count(*) FROM public.cf_deferred_child WHERE id IN ($1::uuid, $2::uuid))`,
			firstID,
			secondID,
		).Scan(&sourceRows, &parentRows, &childRows); err != nil ||
			sourceRows != 2 || parentRows != 2 || childRows != 2 {
			t.Fatalf(
				"transient deferred rows: source=%d parent=%d child=%d error=%v",
				sourceRows,
				parentRows,
				childRows,
				err,
			)
		}
	})

	t.Run("assertion", func(t *testing.T) {
		firstID := "00000000-0000-4000-8f03-000000000031"
		secondID := "00000000-0000-4000-8f03-000000000032"
		mutationIDs := []string{
			"00000000-0000-4000-8f03-000000000033",
			"00000000-0000-4000-8f03-000000000034",
		}
		payload := phase4PushPayload(
			client,
			"00000000-0000-4000-8f03-000000000035",
			[]map[string]any{
				{
					"mutation_id":     mutationIDs[0],
					"table":           table.ID,
					"pk":              map[string]any{table.PrimaryKeyField: firstID},
					"authored_schema": client.Schema,
					"op":              "insert",
					"client_version":  phase4ClientVersion,
					"columns":         map[string]any{table.ValueField: "partner:" + secondID},
				},
				{
					"mutation_id":     mutationIDs[1],
					"table":           table.ID,
					"pk":              map[string]any{table.PrimaryKeyField: secondID},
					"authored_schema": client.Schema,
					"op":              "insert",
					"client_version":  phase4ClientVersion,
					"columns":         map[string]any{table.ValueField: "partner:" + firstID},
				},
			},
		)
		payload["atomic"] = true
		status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", payload)
		if status != http.StatusOK {
			t.Fatalf("partner atomic push status = %d, want 200: %#v", status, response)
		}
		accepted := requireOutcomeList(t, response, "accepted")
		if len(accepted) != 2 || len(requireOutcomeList(t, response, "rejected")) != 0 {
			t.Fatalf("partner atomic push outcomes are invalid: %#v", response)
		}
		for index, outcome := range accepted {
			if outcome["mutation_id"] != mutationIDs[index] || outcome["status"] != "applied" {
				t.Fatalf("partner atomic outcome %d is invalid: %#v", index, outcome)
			}
		}
		var sourceRows int
		if err := admin.QueryRowContext(
			ctx,
			"SELECT count(*) FROM public.cf_source_filled_items WHERE id IN ($1::uuid, $2::uuid)",
			firstID,
			secondID,
		).Scan(&sourceRows); err != nil || sourceRows != 2 {
			t.Fatalf("partner atomic source rows = %d, want 2: %v", sourceRows, err)
		}
	})

	t.Run("assertion", func(t *testing.T) {
		firstID := "00000000-0000-4000-8f03-000000000041"
		secondID := "00000000-0000-4000-8f03-000000000042"
		missingPartnerID := "00000000-0000-4000-8f03-000000000043"
		mutationIDs := []string{
			"00000000-0000-4000-8f03-000000000044",
			"00000000-0000-4000-8f03-000000000045",
		}
		var beforeEpoch int64
		if err := admin.QueryRowContext(ctx, `
			SELECT accepted_write_epoch
			FROM synchro.sync_clients
			WHERE user_id = 'diagnostic-user' AND client_id = $1`,
			client.ID,
		).Scan(&beforeEpoch); err != nil {
			t.Fatalf("load accepted-write epoch before failed group: %v", err)
		}
		payload := phase4PushPayload(
			client,
			"00000000-0000-4000-8f03-000000000046",
			[]map[string]any{
				{
					"mutation_id":     mutationIDs[0],
					"table":           table.ID,
					"pk":              map[string]any{table.PrimaryKeyField: firstID},
					"authored_schema": client.Schema,
					"op":              "insert",
					"client_version":  phase4ClientVersion,
					"columns":         map[string]any{table.ValueField: "partner:" + missingPartnerID},
				},
				{
					"mutation_id":     mutationIDs[1],
					"table":           table.ID,
					"pk":              map[string]any{table.PrimaryKeyField: secondID},
					"authored_schema": client.Schema,
					"op":              "insert",
					"client_version":  phase4ClientVersion,
					"columns":         map[string]any{table.ValueField: "accept-deferred"},
				},
			},
		)
		payload["atomic"] = true
		status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", payload)
		if status != http.StatusOK {
			t.Fatalf("failed partner atomic push status = %d, want 200: %#v", status, response)
		}
		if accepted := requireOutcomeList(t, response, "accepted"); len(accepted) != 0 {
			t.Fatalf("failed partner atomic push accepted mutations: %#v", accepted)
		}
		rejected := requireOutcomeList(t, response, "rejected")
		if len(rejected) != 2 ||
			rejected[0]["mutation_id"] != mutationIDs[0] ||
			rejected[0]["status"] != "rejected_terminal" ||
			rejected[0]["code"] != "atomic_batch_rejected" ||
			rejected[1]["mutation_id"] != mutationIDs[1] ||
			rejected[1]["status"] != "rejected_terminal" ||
			rejected[1]["code"] != "validation_failed" {
			t.Fatalf("failed partner atomic outcomes are invalid: %#v", response)
		}
		requireRealPushUnitNoDurableRecord(t, ctx, admin, firstID, mutationIDs[0])
		requireRealPushUnitNoDurableRecord(t, ctx, admin, secondID, mutationIDs[1])
		var afterEpoch int64
		if err := admin.QueryRowContext(ctx, `
			SELECT accepted_write_epoch
			FROM synchro.sync_clients
			WHERE user_id = 'diagnostic-user' AND client_id = $1`,
			client.ID,
		).Scan(&afterEpoch); err != nil {
			t.Fatalf("load accepted-write epoch after failed group: %v", err)
		}
		if afterEpoch != beforeEpoch {
			t.Fatalf("failed partner atomic group changed accepted-write epoch from %d to %d", beforeEpoch, afterEpoch)
		}
	})

	t.Run("assertion", func(t *testing.T) {
		callerID := "00000000-0000-4000-8f03-000000000051"
		recordID := "00000000-0000-4000-8f03-000000000052"
		mutationID := "00000000-0000-4000-8f03-000000000053"
		request := phase4PushPayload(
			client,
			"00000000-0000-4000-8f03-000000000054",
			[]map[string]any{{
				"mutation_id":     mutationID,
				"table":           table.ID,
				"pk":              map[string]any{table.PrimaryKeyField: recordID},
				"authored_schema": client.Schema,
				"op":              "insert",
				"client_version":  phase4ClientVersion,
				"columns":         map[string]any{table.ValueField: "accept-deferred"},
			}},
		)
		requestJSON, err := json.Marshal(request)
		if err != nil {
			t.Fatalf("encode direct push request: %v", err)
		}
		transaction, err := admin.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin pending-event transaction: %v", err)
		}
		if _, err := transaction.ExecContext(
			ctx,
			`INSERT INTO public.cf_source_filled_items (id, owner_id, value)
			 VALUES ($1::uuid, 'diagnostic-user', 'reject-deferred')`,
			callerID,
		); err != nil {
			_ = transaction.Rollback()
			t.Fatalf("queue caller deferred event: %v", err)
		}
		var directResponse string
		directErr := transaction.QueryRowContext(
			ctx,
			"SELECT synchro.synchro_push($1, $2::jsonb)",
			"diagnostic-user",
			string(requestJSON),
		).Scan(&directResponse)
		if err := transaction.Rollback(); err != nil {
			t.Fatalf("roll back pending-event transaction: %v", err)
		}
		var postgresError *pgconn.PgError
		if !errors.As(directErr, &postgresError) || postgresError.Code != "25000" {
			t.Fatalf(
				"direct push error = %v, response = %q, want PostgreSQL invalid_transaction_state",
				directErr,
				directResponse,
			)
		}

		status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", request)
		if status != http.StatusOK {
			t.Fatalf("valid push after pending-event rollback status = %d, want 200: %#v", status, response)
		}
		accepted := requireOutcomeList(t, response, "accepted")
		if len(accepted) != 1 || accepted[0]["mutation_id"] != mutationID ||
			accepted[0]["status"] != "applied" ||
			len(requireOutcomeList(t, response, "rejected")) != 0 {
			t.Fatalf("valid push after pending-event rollback is invalid: %#v", response)
		}
	})
}

func installRealPushUnitConstraintFixtures(
	t *testing.T,
	ctx context.Context,
	admin *sql.DB,
) {
	t.Helper()
	if _, err := admin.ExecContext(ctx, `
		CREATE TABLE public.cf_deferred_parent (
			id uuid PRIMARY KEY
		);
		CREATE TABLE public.cf_deferred_child (
			id uuid PRIMARY KEY,
			parent_id uuid NOT NULL REFERENCES public.cf_deferred_parent (id)
				DEFERRABLE INITIALLY DEFERRED
		);
		GRANT SELECT, INSERT ON TABLE public.cf_deferred_parent, public.cf_deferred_child
			TO synchro_owner;

		CREATE FUNCTION public.cf_source_filled_items_deferred_value()
		RETURNS trigger
		LANGUAGE plpgsql
		SECURITY INVOKER
		SET search_path = pg_catalog, public
		AS $$
		BEGIN
		    IF NEW.value = 'reject-deferred' THEN
		        RAISE EXCEPTION 'deferred value check failed' USING ERRCODE = '23514';
		    END IF;
		    RETURN NULL;
		END
		$$;
		CREATE CONSTRAINT TRIGGER cf_source_filled_items_deferred_value
		AFTER INSERT OR UPDATE ON public.cf_source_filled_items
		DEFERRABLE INITIALLY DEFERRED
		FOR EACH ROW EXECUTE FUNCTION public.cf_source_filled_items_deferred_value();

		CREATE FUNCTION public.cf_source_filled_items_deferred_partner()
		RETURNS trigger
		LANGUAGE plpgsql
		SECURITY INVOKER
		SET search_path = pg_catalog, public
		AS $$
		DECLARE
		    partner_id uuid;
		BEGIN
		    IF NEW.value LIKE 'partner:%' THEN
		        partner_id := pg_catalog.substr(NEW.value, 9)::uuid;
		        IF NOT EXISTS (
		            SELECT 1 FROM public.cf_source_filled_items WHERE id = partner_id
		        ) THEN
		            RAISE EXCEPTION 'deferred partner check failed' USING ERRCODE = '23503';
		        END IF;
		    END IF;
		    RETURN NULL;
		END
		$$;
		CREATE CONSTRAINT TRIGGER cf_source_filled_items_deferred_partner
		AFTER INSERT OR UPDATE ON public.cf_source_filled_items
		DEFERRABLE INITIALLY IMMEDIATE
		FOR EACH ROW EXECUTE FUNCTION public.cf_source_filled_items_deferred_partner();

		CREATE FUNCTION public.cf_source_filled_items_transient_deferred()
		RETURNS trigger
		LANGUAGE plpgsql
		SECURITY INVOKER
		SET search_path = pg_catalog, public
		AS $$
		BEGIN
		    IF NEW.value = 'transient-deferred' THEN
		        INSERT INTO public.cf_deferred_child (id, parent_id)
		        VALUES (NEW.id, NEW.id);
		        INSERT INTO public.cf_deferred_parent (id) VALUES (NEW.id);
		    END IF;
		    RETURN NULL;
		END
		$$;
		CREATE TRIGGER cf_source_filled_items_transient_deferred
		AFTER INSERT ON public.cf_source_filled_items
		FOR EACH ROW EXECUTE FUNCTION public.cf_source_filled_items_transient_deferred();`,
	); err != nil {
		t.Fatalf("install push unit constraint fixtures: %v", err)
	}
}

func requireRealPushUnitNoDurableRecord(
	t *testing.T,
	ctx context.Context,
	admin *sql.DB,
	recordID string,
	mutationID string,
) {
	t.Helper()
	var sourceRows, fences, versions int
	if err := admin.QueryRowContext(ctx, `
		SELECT
		    (SELECT count(*) FROM public.cf_source_filled_items WHERE id = $1::uuid),
		    (SELECT count(*) FROM synchro.sync_write_fences WHERE mutation_id = $2),
		    (
		        SELECT count(*)
		        FROM synchro.sync_row_versions version
		        JOIN synchro.sync_registry registry ON registry.relation_id = version.relation_id
		        JOIN synchro.sync_registry_generations generation
		          ON generation.generation = registry.registry_generation
		        WHERE generation.state = 'active'
		          AND registry.table_name = 'cf_source_filled_items'
		          AND version.record_id = ($1::uuid)::text
		    )`,
		recordID,
		mutationID,
	).Scan(&sourceRows, &fences, &versions); err != nil ||
		sourceRows != 0 || fences != 0 || versions != 0 {
		t.Fatalf(
			"rejected push unit state: source=%d fences=%d versions=%d error=%v",
			sourceRows,
			fences,
			versions,
			err,
		)
	}
}
