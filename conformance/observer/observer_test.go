package observer

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
)

const observerTestDriverName = "synchro-observer-test-driver"

var (
	observerDriverOnce         sync.Once
	currentObserverDriverState *observerDriverState
)

type observerDriverState struct {
	mu             sync.Mutex
	beginCount     int
	commitCount    int
	readOnly       bool
	isolation      driver.IsolationLevel
	setReadOnly    int
	queries        []string
	writesRejected int
}

type observerDriver struct{}

func (observerDriver) Open(string) (driver.Conn, error) {
	return &observerConn{state: currentObserverDriverState}, nil
}

type observerConn struct {
	state *observerDriverState
}

func (connection *observerConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepared statements are not used")
}

func (connection *observerConn) Close() error {
	return nil
}

func (connection *observerConn) Begin() (driver.Tx, error) {
	return connection.BeginTx(context.Background(), driver.TxOptions{})
}

func (connection *observerConn) BeginTx(_ context.Context, options driver.TxOptions) (driver.Tx, error) {
	connection.state.mu.Lock()
	defer connection.state.mu.Unlock()
	connection.state.beginCount++
	connection.state.readOnly = options.ReadOnly
	connection.state.isolation = options.Isolation
	return observerTx{state: connection.state}, nil
}

func (connection *observerConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	connection.state.mu.Lock()
	defer connection.state.mu.Unlock()
	if strings.EqualFold(strings.TrimSpace(query), "SET TRANSACTION READ ONLY") {
		connection.state.setReadOnly++
		return driver.RowsAffected(0), nil
	}
	connection.state.writesRejected++
	return nil, errors.New("permission denied for table cf_items")
}

func (connection *observerConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	connection.state.mu.Lock()
	connection.state.queries = append(connection.state.queries, query)
	connection.state.mu.Unlock()
	return &observerRows{values: [][]driver.Value{{[]byte(`[{"id":"source-row"}]`)}}}, nil
}

type observerTx struct {
	state *observerDriverState
}

func (transaction observerTx) Commit() error {
	transaction.state.mu.Lock()
	defer transaction.state.mu.Unlock()
	transaction.state.commitCount++
	return nil
}

func (transaction observerTx) Rollback() error {
	return nil
}

type observerRows struct {
	values [][]driver.Value
	index  int
}

func (rows *observerRows) Columns() []string {
	return []string{"value"}
}

func (rows *observerRows) Close() error {
	return nil
}

func (rows *observerRows) Next(destination []driver.Value) error {
	if rows.index >= len(rows.values) {
		return io.EOF
	}
	copy(destination, rows.values[rows.index])
	rows.index++
	return nil
}

func observerTestDB(t *testing.T) (*sql.DB, *observerDriverState) {
	t.Helper()
	observerDriverOnce.Do(func() {
		sql.Register(observerTestDriverName, observerDriver{})
	})
	state := &observerDriverState{}
	currentObserverDriverState = state
	database, err := sql.Open(observerTestDriverName, "")
	if err != nil {
		t.Fatalf("open observer test database: %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close observer test database: %v", err)
		}
	})
	return database, state
}

func TestSnapshotUsesOneReadOnlyRepeatableReadTransaction(t *testing.T) {
	database, state := observerTestDB(t)
	observer, err := NewPostgres(PostgresConfig{
		DB: database,
		SourceTables: []SourceTable{{
			Name:     "items",
			Relation: "public.cf_items",
			Columns:  []string{"id"},
			OrderBy:  []string{"id"},
		}},
	})
	if err != nil {
		t.Fatalf("create observer: %v", err)
	}
	snapshot, err := observer.Snapshot(context.Background(), SnapshotRequest{SourceTables: []string{"items"}})
	if err != nil {
		t.Fatalf("capture snapshot: %v", err)
	}
	if len(snapshot.SourceTables) != 1 || len(snapshot.SourceTables[0].Rows) != 1 {
		t.Fatalf("snapshot rows = %#v", snapshot.SourceTables)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.beginCount != 1 || state.commitCount != 1 || !state.readOnly || state.isolation != driver.IsolationLevel(sql.LevelRepeatableRead) || state.setReadOnly != 1 || len(state.queries) != 1 {
		t.Fatalf("observer transaction state = %#v", state)
	}
}

func TestObserverRejectsMutationAndInternalAccess(t *testing.T) {
	for _, statement := range []string{
		"INSERT INTO cf_items (id) VALUES ('x')",
		"SELECT * FROM sync_changelog",
		"WITH changed AS (DELETE FROM cf_items RETURNING id) SELECT * FROM changed",
		"SELECT id INTO copied_items FROM cf_items",
	} {
		if err := ValidateReadOnlySQL(statement); err == nil {
			t.Fatalf("ValidateReadOnlySQL(%q) succeeded", statement)
		}
	}
	database, _ := observerTestDB(t)
	if _, err := NewPostgres(PostgresConfig{
		DB: database,
		SourceTables: []SourceTable{{
			Name:     "internal",
			Relation: "public.sync_changelog",
			Columns:  []string{"seq"},
			OrderBy:  []string{"seq"},
		}},
	}); err == nil {
		t.Fatal("observer accepted an internal sync table")
	}
}

func TestObserverRoleRejectsWrites(t *testing.T) {
	database, state := observerTestDB(t)
	if _, err := database.ExecContext(context.Background(), "INSERT INTO cf_items (id) VALUES ('x')"); err == nil {
		t.Fatal("observer role write succeeded")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.writesRejected != 1 {
		t.Fatalf("rejected write count = %d", state.writesRejected)
	}
}
