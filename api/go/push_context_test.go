package synchroapi

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/trainstar/synchro/api/go/internal/testsupport"
)

type pushContextKey struct{}

func TestPreparePushTransactionLocalContext(t *testing.T) {
	db := testsupport.OpenPostgres(t)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	fixture := registerAuthRowsTable(t, db, nil)
	quotedTable := "public." + quoteTestIdentifier(fixture.tableName)
	var membership string
	if err := db.QueryRow(`
		SELECT pg_catalog.format('%I.%I', membership_function_schema, membership_function_name)
		FROM synchro.sync_registry registry
		JOIN synchro.sync_registry_generations generation ON generation.generation = registry.registry_generation
		WHERE generation.state = 'active' AND table_name = $1
	`, fixture.tableName).Scan(&membership); err != nil {
		t.Fatalf("load registered membership function: %v", err)
	}
	if _, err := db.Exec("GRANT INSERT, UPDATE ON " + quotedTable + " TO synchro_owner"); err != nil {
		t.Fatalf("grant push privileges: %v", err)
	}
	if _, err := db.Exec(
		"SELECT synchro.synchro_register_table($1, $2, 'single_scope', 'id', 'updated_at', 'deleted_at', 'enabled')",
		"public."+fixture.tableName, membership,
	); err != nil {
		t.Fatalf("enable registered push table: %v", err)
	}
	if err := waitForRegistryActivation(db); err != nil {
		t.Fatalf("activate push table: %v", err)
	}

	function := "public." + quoteTestIdentifier(testsupport.UniqueName(t, "push_context_trigger"))
	if _, err := db.Exec(fmt.Sprintf(`
		CREATE FUNCTION %[1]s() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.label = 'retry' THEN
				RAISE EXCEPTION 'private_push_context_error' USING ERRCODE = '40001';
			END IF;
			IF NEW.label = 'sleep' THEN
				PERFORM pg_catalog.pg_sleep(10);
			END IF;
			IF COALESCE(pg_catalog.current_setting('host.push_context', true), '') = '' THEN
				RAISE EXCEPTION 'private_push_context_error' USING ERRCODE = '23514';
			END IF;
			NEW.label := NEW.label || '/' || pg_catalog.current_setting('host.push_context');
			RETURN NEW;
		END;
		$$;
		CREATE TRIGGER host_push_context BEFORE INSERT ON %[2]s
		FOR EACH ROW EXECUTE FUNCTION %[1]s()
	`, function, quotedTable)); err != nil {
		t.Fatalf("create context-observing trigger: %v", err)
	}
	t.Cleanup(func() {
		if _, err := db.Exec("DROP TRIGGER host_push_context ON " + quotedTable); err != nil {
			t.Errorf("drop context trigger: %v", err)
		}
		if _, err := db.Exec("DROP FUNCTION " + function + "()"); err != nil {
			t.Errorf("drop context trigger function: %v", err)
		}
	})

	var prepared atomic.Int64
	prepare := func(ctx context.Context, tx *sql.Tx) error {
		prepared.Add(1)
		value, _ := ctx.Value(pushContextKey{}).(string)
		if _, err := tx.ExecContext(ctx, "SELECT pg_catalog.set_config('host.push_context', $1, true)", value); err != nil {
			return err
		}
		if value == "fail" {
			return errors.New("private_push_context_error")
		}
		return nil
	}
	config := Config{DB: db, JWTSecret: []byte("test-secret-for-integration-tests"), PreparePush: prepare}
	handler := Routes(config)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	identity := testsupport.UniqueName(t, "push_context_user")
	token := testToken(identity)
	clientID := testClientID(t, "push-context-client")
	client := connectClient(t, server, token, clientID)
	for _, route := range []struct {
		method string
		path   string
		body   any
	}{
		{http.MethodGet, "/sync/tables", nil},
		{http.MethodGet, "/ready", nil},
		{http.MethodPost, "/sync/pull", authPullRequest(clientID, client)},
		{http.MethodPost, "/sync/rebuild", authRebuildRequest(t, clientID, client, "user:"+identity)},
	} {
		status, _ := doJSON(t, route.method, server.URL+route.path, token, route.body)
		if status != http.StatusOK && !(route.path == "/ready" && status == http.StatusServiceUnavailable) {
			t.Fatalf("%s status = %d", route.path, status)
		}
	}
	if prepared.Load() != 0 {
		t.Fatal("another route invoked push preparation")
	}

	push := func(t *testing.T, label string) map[string]any {
		body := authPushRequest(t, fixture, clientID, client, identity)
		mutation := body["mutations"].([]map[string]any)[0]
		mutation["pk"] = map[string]any{fixture.primaryField: identity + "/" + randomUUID(t)}
		mutation["columns"] = map[string]any{fixture.labelField: label}
		return body
	}
	assertStored := func(t *testing.T, wantRows int, wantLabel string) {
		t.Helper()
		var rows int
		var label string
		if err := db.QueryRow("SELECT count(*), COALESCE(max(label), '') FROM "+quotedTable).Scan(&rows, &label); err != nil {
			t.Fatalf("read pushed source rows: %v", err)
		}
		if rows != wantRows || label != wantLabel {
			t.Fatalf("stored rows = %d label = %q, want %d %q", rows, label, wantRows, wantLabel)
		}
	}
	connectionID := func(t *testing.T) int {
		t.Helper()
		var pid int
		if err := db.QueryRow("SELECT pg_catalog.pg_backend_pid()").Scan(&pid); err != nil {
			t.Fatalf("read pooled connection identity: %v", err)
		}
		return pid
	}
	assertRemoved := func(t *testing.T, pid int) {
		t.Helper()
		var value string
		var reusedPID int
		if err := db.QueryRow("SELECT COALESCE(pg_catalog.current_setting('host.push_context', true), ''), pg_catalog.pg_backend_pid()").Scan(&value, &reusedPID); err != nil {
			t.Fatalf("read context after transaction: %v", err)
		}
		if value != "" || reusedPID != pid {
			t.Fatalf("context = %q connection = %d, want empty context on connection %d", value, reusedPID, pid)
		}
	}
	assertNoPushLedger := func(t *testing.T, body map[string]any) {
		t.Helper()
		mutationID := body["mutations"].([]map[string]any)[0]["mutation_id"]
		var batches, mutations int
		if err := db.QueryRow(`
			SELECT
				(SELECT count(*) FROM synchro.sync_push_batches
				 WHERE user_id = $1 AND client_id = $2 AND batch_id = $3::uuid),
				(SELECT count(*) FROM synchro.sync_push_mutations
				 WHERE user_id = $1 AND client_id = $2 AND mutation_id = $4::uuid)
		`, identity, clientID, body["batch_id"], mutationID).Scan(&batches, &mutations); err != nil {
			t.Fatalf("read failed push ledgers: %v", err)
		}
		if batches != 0 || mutations != 0 {
			t.Fatalf("failed push batches = %d mutations = %d, want zero", batches, mutations)
		}
	}

	body := push(t, "created")
	pid := connectionID(t)
	response := servePushContextRequest(t, handler, context.Background(), token, "first", body)
	if response.Code != http.StatusOK {
		t.Fatalf("push status = %d: %s", response.Code, response.Body.String())
	}
	var outcome struct {
		Accepted []json.RawMessage `json:"accepted"`
		Rejected []json.RawMessage `json:"rejected"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &outcome); err != nil || len(outcome.Accepted) != 1 || len(outcome.Rejected) != 0 {
		t.Fatalf("prepared push did not apply: %s", response.Body.String())
	}
	var storedResponse []byte
	if err := db.QueryRow("SELECT sealed_canonical_response FROM synchro.sync_push_batches WHERE user_id = $1 AND client_id = $2 AND batch_id = $3::uuid", identity, clientID, body["batch_id"]).Scan(&storedResponse); err != nil {
		t.Fatalf("read stored canonical push response: %v", err)
	}
	if !bytes.Equal(response.Body.Bytes(), storedResponse) {
		t.Fatal("prepared push changed the stored response bytes")
	}
	assertStored(t, 1, "created/first")
	assertRemoved(t, pid)
	replay := servePushContextRequest(t, handler, context.Background(), token, "replay", body)
	if replay.Code != http.StatusOK || !bytes.Equal(replay.Body.Bytes(), storedResponse) {
		t.Fatal("prepared push replay changed the stored response bytes")
	}
	assertStored(t, 1, "created/first")
	assertRemoved(t, pid)

	failed := push(t, "not-written")
	response = servePushContextRequest(t, handler, context.Background(), token, "fail", failed)
	requirePushContextFailure(t, response, http.StatusInternalServerError, "sync_integrity_failure", false)
	assertNoPushLedger(t, failed)
	assertStored(t, 1, "created/first")
	assertRemoved(t, pid)

	protocolFailure := push(t, "not-written")
	protocolFailure["client_generation"] = client.Generation + 1
	response = servePushContextRequest(t, handler, context.Background(), token, "protocol", protocolFailure)
	requirePushContextFailure(t, response, http.StatusConflict, "client_generation_expired", false)
	assertNoPushLedger(t, protocolFailure)
	assertStored(t, 1, "created/first")
	assertRemoved(t, pid)

	queryFailure := push(t, "retry")
	response = servePushContextRequest(t, handler, context.Background(), token, "query-error", queryFailure)
	requirePushContextFailure(t, response, http.StatusServiceUnavailable, "temporary_unavailable", true)
	assertNoPushLedger(t, queryFailure)
	assertStored(t, 1, "created/first")
	assertRemoved(t, pid)

	observer := testsupport.OpenPostgres(t)
	for _, phase := range []string{"preparation", "query"} {
		for _, interruption := range []string{"cancellation", "deadline"} {
			t.Run(phase+"/"+interruption, func(t *testing.T) {
				cfg := config
				cfg.DatabaseQueryTimeout = 2 * time.Second
				entered := make(chan struct{})
				cfg.PreparePush = func(ctx context.Context, tx *sql.Tx) error {
					if err := prepare(ctx, tx); err != nil {
						return err
					}
					close(entered)
					if phase == "preparation" {
						<-ctx.Done()
						return fmt.Errorf("private_push_context_error: %w", ctx.Err())
					}
					return nil
				}
				requestContext, cancel := context.WithCancel(context.Background())
				defer cancel()
				requestBody := push(t, "sleep")
				pid := connectionID(t)
				done := make(chan *httptest.ResponseRecorder, 1)
				go func() {
					done <- servePushContextRequest(t, Routes(cfg), requestContext, token, "bounded", requestBody)
				}()
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("push preparation did not start")
				}
				if phase == "query" {
					deadline := time.Now().Add(time.Second)
					for {
						var sleeping bool
						if err := observer.QueryRow("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid = $1 AND wait_event = 'PgSleep')", pid).Scan(&sleeping); err != nil {
							t.Fatalf("observe blocked push query: %v", err)
						}
						if sleeping {
							break
						}
						if time.Now().After(deadline) {
							t.Fatal("canonical push did not enter the trigger wait")
						}
						time.Sleep(10 * time.Millisecond)
					}
				}
				wait := 5 * time.Second
				if interruption == "cancellation" {
					cancel()
					wait = time.Second
				}
				select {
				case response := <-done:
					if interruption == "deadline" {
						requirePushContextFailure(t, response, http.StatusServiceUnavailable, "temporary_unavailable", true)
					} else {
						requirePushContextFailure(t, response, http.StatusInternalServerError, "sync_integrity_failure", false)
					}
				case <-time.After(wait):
					t.Fatal("bounded push did not return")
				}
				assertNoPushLedger(t, requestBody)
				assertStored(t, 1, "created/first")
				assertRemoved(t, connectionID(t))
			})
		}
	}
}

func servePushContextRequest(t *testing.T, handler http.Handler, ctx context.Context, token, value string, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("encode push request: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, "/sync/push", bytes.NewReader(raw))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("X-Client-Version", "1.0.0")
	request = request.WithContext(context.WithValue(ctx, pushContextKey{}, value))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func requirePushContextFailure(t *testing.T, response *httptest.ResponseRecorder, status int, code string, retryable bool) {
	t.Helper()
	var envelope protocolErrorEnvelope
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode push error: %v", err)
	}
	if response.Code != status || envelope.Error.Code != code || envelope.Error.Retryable != retryable {
		t.Fatalf("push error = %d %#v, want %d %s retryable=%t", response.Code, envelope.Error, status, code, retryable)
	}
	if strings.Contains(response.Body.String(), "private_push_context_error") {
		t.Fatal("push response exposed private error text")
	}
	if retryable && response.Header().Get("Retry-After") != "5" {
		t.Fatal("retryable push omitted Retry-After")
	}
}

type pushTransactionFailureDriver struct {
	beginErr  error
	commitErr error
	queries   *atomic.Int64
}

var pushTransactionDriverSequence atomic.Uint64

func (d pushTransactionFailureDriver) Open(string) (driver.Conn, error) { return d, nil }
func (d pushTransactionFailureDriver) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not supported")
}
func (d pushTransactionFailureDriver) Close() error { return nil }
func (d pushTransactionFailureDriver) Begin() (driver.Tx, error) {
	if d.beginErr != nil {
		return nil, d.beginErr
	}
	return d, nil
}
func (d pushTransactionFailureDriver) Commit() error   { return d.commitErr }
func (d pushTransactionFailureDriver) Rollback() error { return nil }
func (d pushTransactionFailureDriver) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	d.queries.Add(1)
	return &readinessRows{result: []byte(`{"accepted":[],"rejected":[],"private_push_context_success":true}`)}, nil
}

func TestPreparePushBeginAndCommitFailures(t *testing.T) {
	for _, phase := range []string{"begin", "commit"} {
		t.Run(phase, func(t *testing.T) {
			var prepared, queries atomic.Int64
			d := pushTransactionFailureDriver{queries: &queries}
			if phase == "begin" {
				d.beginErr = errors.New("private_push_context_error")
			} else {
				d.commitErr = errors.New("private_push_context_error")
			}
			name := fmt.Sprintf("synchro-push-transaction-%d", pushTransactionDriverSequence.Add(1))
			sql.Register(name, d)
			db, err := sql.Open(name, "")
			if err != nil {
				t.Fatalf("open failure-injection database: %v", err)
			}
			t.Cleanup(func() { _ = db.Close() })
			handler := Routes(Config{
				DB: db, JWTSecret: []byte("test-secret-for-integration-tests"),
				PreparePush: func(context.Context, *sql.Tx) error {
					prepared.Add(1)
					return nil
				},
			})
			response := servePushContextRequest(t, handler, context.Background(), testToken("user"), "", map[string]any{"client_id": "client"})
			requirePushContextFailure(t, response, http.StatusInternalServerError, "sync_integrity_failure", false)
			if strings.Contains(response.Body.String(), "private_push_context_success") {
				t.Fatal("failed commit returned the canonical successful response")
			}
			wantCalls := int64(1)
			if phase == "begin" {
				wantCalls = 0
			}
			if prepared.Load() != wantCalls || queries.Load() != wantCalls {
				t.Fatalf("preparations = %d queries = %d, want %d", prepared.Load(), queries.Load(), wantCalls)
			}
			if phase == "begin" {
				unset := Routes(Config{DB: db, JWTSecret: []byte("test-secret-for-integration-tests")})
				response := servePushContextRequest(t, unset, context.Background(), testToken("user"), "", map[string]any{"client_id": "client"})
				if response.Code != http.StatusOK || queries.Load() != 1 || prepared.Load() != 0 {
					t.Fatal("unset callback changed the direct query path")
				}
			}
		})
	}
}
