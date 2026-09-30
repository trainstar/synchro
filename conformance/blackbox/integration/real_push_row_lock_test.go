package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
)

const (
	rowLockGateClass = 265100
	rowLockWait      = 30 * time.Second
)

// TestRealPushRowLockMatchesSourceStatement proves that the first lock of the authoritative
// source row has the row lock mode of the source statement that follows it. A stronger lock
// blocks the foreign key checks of child rows. A weaker lock makes the statement upgrade the
// lock while it holds the weaker lock. The push also holds the table lock of the statement
// before it reads the key columns. Through a registered partitioned table, the key columns come
// from the partition of the row, and the push takes the table lock only on the relations from the
// registered table down to that partition.
func TestRealPushRowLockMatchesSourceStatement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	var detectableWait bool
	if err := admin.QueryRowContext(ctx,
		"SELECT current_setting('deadlock_timeout')::interval >= interval '200 milliseconds'",
	).Scan(&detectableWait); err != nil || !detectableWait {
		t.Fatalf("deadlock_timeout is too short to observe a row lock wait: %v", err)
	}
	fixture := installRowLockFixture(t, ctx, harness, token, admin)

	t.Run("update", func(t *testing.T) {
		observer := newRowLockObserver(t, ctx, fixture)
		gate := observer.session()
		observer.holdGate(gate, 1)
		update := fixture.mutation(fixture.parents, "lock-p1", "update",
			map[string]any{fixture.parents.ValueField: "p1-new"})
		push := observer.push(update)
		pushPID := observer.pushPID(push)
		observer.gateWait(push, pushPID, 1)
		writer := observer.session()
		writes := observer.begin(writer)
		observer.notBlocked(writer, pushPID, "insert child of P1", 1, func() (sql.Result, error) {
			return writes.ExecContext(writer.ctx,
				"INSERT INTO public.cf_lock_children (id, parent_id) VALUES ('lock-child-p1', 'lock-p1')")
		})
		observer.commit("writer", writes)
		observer.releaseGate(gate, 1)
		observer.wait(push)
		observer.requireOutcomes(push, []map[string]any{update}, nil)
		var value string
		observer.scan("SELECT value FROM public.cf_lock_parents WHERE id = 'lock-p1'", &value)
		if value != "p1-new" {
			observer.fatalf("P1 value = %q, want p1-new", value)
		}
	})

	t.Run("soft_delete", func(t *testing.T) {
		observer := newRowLockObserver(t, ctx, fixture)
		gate := observer.session()
		observer.holdGate(gate, 2)
		remove := fixture.mutation(fixture.parents, "lock-p2", "delete", nil)
		push := observer.push(remove)
		pushPID := observer.pushPID(push)
		observer.gateWait(push, pushPID, 2)
		writer := observer.session()
		writes := observer.begin(writer)
		observer.notBlocked(writer, pushPID, "key share lock of P2", 1, func() (sql.Result, error) {
			return writes.ExecContext(writer.ctx,
				"SELECT 1 FROM public.cf_lock_parents WHERE id = 'lock-p2' FOR KEY SHARE")
		})
		observer.notBlocked(writer, pushPID, "shared target key of P2", 1, func() (sql.Result, error) {
			return writes.ExecContext(writer.ctx,
				"SELECT pg_catalog.pg_advisory_xact_lock_shared(pg_catalog.hashtextextended('cf-lock:' || 'lock-p2', 0))")
		})
		observer.notBlocked(writer, pushPID, "insert child of P2", 1, func() (sql.Result, error) {
			return writes.ExecContext(writer.ctx,
				"INSERT INTO public.cf_lock_children (id, parent_id) VALUES ('lock-child-p2', 'lock-p2')")
		})
		observer.releaseGate(gate, 2)
		// The target key trigger of the push waits for the shared target key of the writer.
		observer.blocked(push, pushPID, writer.pid)
		observer.commit("writer", writes)
		observer.wait(push)
		observer.requireOutcomes(push, []map[string]any{remove}, nil)
		var deleted, child bool
		observer.scan(`
			SELECT deleted_at IS NOT NULL,
			       EXISTS (SELECT 1 FROM public.cf_lock_children WHERE id = 'lock-child-p2')
			FROM public.cf_lock_parents WHERE id = 'lock-p2'`, &deleted, &child)
		if !deleted || !child {
			observer.fatalf("P2 deleted = %t and child = %t, want both", deleted, child)
		}
	})

	t.Run("key_update", func(t *testing.T) {
		observer := newRowLockObserver(t, ctx, fixture)
		writer := observer.session()
		writes := observer.begin(writer)
		observer.exec("insert child of P3", 1, func() (sql.Result, error) {
			return writes.ExecContext(writer.ctx,
				"INSERT INTO public.cf_lock_children (id, parent_id) VALUES ('lock-child-p3', 'lock-p3')")
		})
		update := fixture.mutation(fixture.parents, "lock-p3", "update",
			map[string]any{fixture.parentCode: "code-p3-new"})
		push := observer.push(update)
		pushPID := observer.pushPID(push)
		observer.blocked(push, pushPID, writer.pid)
		// The push waits for its first lock and holds no row lock on P3.
		observer.notBlocked(writer, pushPID, "update value of P3", 1, func() (sql.Result, error) {
			return writes.ExecContext(writer.ctx,
				"UPDATE public.cf_lock_parents SET value = 'writer' WHERE id = 'lock-p3'")
		})
		observer.commit("writer", writes)
		observer.wait(push)
		row := observer.requireOutcomes(push, nil, []map[string]any{update})[update["mutation_id"].(string)]
		if row[fixture.parents.ValueField] != "writer" || row[fixture.parentCode] != "code-p3" {
			observer.fatalf("P3 server row = %#v, want value writer and code code-p3", row)
		}
		var code, value string
		observer.scan("SELECT code, value FROM public.cf_lock_parents WHERE id = 'lock-p3'", &code, &value)
		if code != "code-p3" || value != "writer" {
			observer.fatalf("P3 code = %q and value = %q, want code-p3 and writer", code, value)
		}
	})

	t.Run("hard_delete", func(t *testing.T) {
		observer := newRowLockObserver(t, ctx, fixture)
		writer := observer.session()
		writes := observer.begin(writer)
		observer.exec("insert child of H1", 1, func() (sql.Result, error) {
			return writes.ExecContext(writer.ctx,
				"INSERT INTO public.cf_lock_hard_children (id, parent_id) VALUES ('lock-child-h1', 'lock-h1')")
		})
		remove := fixture.mutation(fixture.hardParents, "lock-h1", "delete", nil)
		push := observer.push(remove)
		pushPID := observer.pushPID(push)
		observer.blocked(push, pushPID, writer.pid)
		observer.notBlocked(writer, pushPID, "update value of H1", 1, func() (sql.Result, error) {
			return writes.ExecContext(writer.ctx,
				"UPDATE public.cf_lock_hard_parents SET value = 'writer' WHERE id = 'lock-h1'")
		})
		observer.commit("writer", writes)
		observer.wait(push)
		row := observer.requireOutcomes(push, nil, []map[string]any{remove})[remove["mutation_id"].(string)]
		if row[fixture.hardParents.ValueField] != "writer" {
			observer.fatalf("H1 server row = %#v, want value writer", row)
		}
		var value string
		observer.scan("SELECT value FROM public.cf_lock_hard_parents WHERE id = 'lock-h1'", &value)
		if value != "writer" {
			observer.fatalf("H1 value = %q, want writer", value)
		}
	})

	t.Run("trigger_key_update", func(t *testing.T) {
		observer := newRowLockObserver(t, ctx, fixture)
		gate := observer.session()
		writer := observer.session()

		observer.holdGate(gate, 4)
		update := fixture.mutation(fixture.parents, "lock-p4", "update",
			map[string]any{fixture.parents.ValueField: "rekey"})
		push := observer.push(update)
		pushPID := observer.pushPID(push)
		observer.gateWait(push, pushPID, 4)
		writes := observer.begin(writer)
		observer.notBlocked(writer, pushPID, "insert child of P4", 1, func() (sql.Result, error) {
			return writes.ExecContext(writer.ctx,
				"INSERT INTO public.cf_lock_children (id, parent_id) VALUES ('lock-child-p4', 'lock-p4')")
		})
		observer.releaseGate(gate, 4)
		// The BEFORE trigger changes a key column. heap_update then upgrades the row lock.
		observer.blocked(push, pushPID, writer.pid)
		observer.commit("writer", writes)
		observer.wait(push)
		observer.requireOutcomes(push, []map[string]any{update}, nil)
		var code, value string
		observer.scan("SELECT code, value FROM public.cf_lock_parents WHERE id = 'lock-p4'", &code, &value)
		if code != "code-p4-rekeyed" || value != "rekey" {
			observer.fatalf("P4 code = %q and value = %q, want code-p4-rekeyed and rekey", code, value)
		}

		observer.holdGate(gate, 5)
		native := observer.session()
		nativeWrites := observer.begin(native)
		nativeUpdate := observer.statement("native update of P5", func() (sql.Result, error) {
			return nativeWrites.ExecContext(native.ctx,
				"UPDATE public.cf_lock_parents SET value = 'rekey' WHERE id = 'lock-p5'")
		})
		observer.gateWait(nativeUpdate, native.pid, 5)
		writes = observer.begin(writer)
		observer.notBlocked(writer, native.pid, "insert child of P5", 1, func() (sql.Result, error) {
			return writes.ExecContext(writer.ctx,
				"INSERT INTO public.cf_lock_children (id, parent_id) VALUES ('lock-child-p5', 'lock-p5')")
		})
		observer.releaseGate(gate, 5)
		observer.blocked(nativeUpdate, native.pid, writer.pid)
		observer.commit("writer", writes)
		observer.wait(nativeUpdate)
		if nativeUpdate.err != nil || nativeUpdate.rows != 1 {
			observer.fatalf("native update of P5 = %d rows, error %v, want 1 row", nativeUpdate.rows, nativeUpdate.err)
		}
		observer.commit("native transaction", nativeWrites)
		observer.scan("SELECT code, value FROM public.cf_lock_parents WHERE id = 'lock-p5'", &code, &value)
		if code != "code-p5-rekeyed" || value != "rekey" {
			observer.fatalf("P5 code = %q and value = %q, want code-p5-rekeyed and rekey", code, value)
		}
	})

	t.Run("catalog_change", func(t *testing.T) {
		observer := newRowLockObserver(t, ctx, fixture)
		writer := observer.session()
		writes := observer.begin(writer)
		observer.exec("insert child of Q2", 1, func() (sql.Result, error) {
			return writes.ExecContext(writer.ctx,
				"INSERT INTO public.cf_lock_catalog_children (id, parent_id) VALUES ('lock-child-q2', 'lock-q2')")
		})
		gate := observer.session()
		observer.holdGate(gate, 6)
		// The first mutation makes the push take each lock of a cf_lock_catalog mutation before
		// the index exists.
		first := fixture.mutation(fixture.catalog, "lock-q0", "update",
			map[string]any{fixture.catalog.ValueField: "q0-new"})
		gated := fixture.mutation(fixture.catalog, "lock-q1", "update",
			map[string]any{fixture.catalog.ValueField: "q1-new"})
		keyed := fixture.mutation(fixture.catalog, "lock-q2", "update",
			map[string]any{fixture.catalogCode: "code-q2-new"})
		push := observer.push(first, gated, keyed)
		pushPID := observer.pushPID(push)
		observer.gateWait(push, pushPID, 6)
		builder := observer.session()
		build := observer.statement("concurrent index build", func() (sql.Result, error) {
			return builder.conn.ExecContext(builder.ctx,
				"CREATE UNIQUE INDEX CONCURRENTLY cf_lock_catalog_code ON public.cf_lock_catalog (code)")
		})
		observer.poll("live index cf_lock_catalog_code", func(ctx context.Context) (bool, error) {
			var live bool
			if err := fixture.admin.QueryRowContext(ctx, `
				SELECT EXISTS (
					SELECT 1 FROM pg_catalog.pg_index
					WHERE indexrelid = pg_catalog.to_regclass('public.cf_lock_catalog_code')
					  AND indislive
				)`).Scan(&live); err != nil {
				return false, err
			}
			if !live && build.finished() {
				return false, errors.New("the index build returned")
			}
			return live, nil
		})
		observer.blocked(build, builder.pid, pushPID)
		observer.releaseGate(gate, 6)
		observer.blocked(push, pushPID, writer.pid)
		observer.notBlocked(writer, pushPID, "update value of Q2", 1, func() (sql.Result, error) {
			return writes.ExecContext(writer.ctx,
				"UPDATE public.cf_lock_catalog SET value = 'writer' WHERE id = 'lock-q2'")
		})
		observer.commit("writer", writes)
		observer.wait(push)
		row := observer.requireOutcomes(push, []map[string]any{first, gated}, []map[string]any{keyed})[keyed["mutation_id"].(string)]
		if row[fixture.catalog.ValueField] != "writer" || row[fixture.catalogCode] != "code-q2" {
			observer.fatalf("Q2 server row = %#v, want value writer and code code-q2", row)
		}
		var firstValue, gatedValue string
		observer.scan(`
			SELECT
				(SELECT value FROM public.cf_lock_catalog WHERE id = 'lock-q0'),
				(SELECT value FROM public.cf_lock_catalog WHERE id = 'lock-q1')`, &firstValue, &gatedValue)
		if firstValue != "q0-new" || gatedValue != "q1-new" {
			observer.fatalf("Q0 value = %q and Q1 value = %q, want q0-new and q1-new", firstValue, gatedValue)
		}
		observer.wait(build)
		if build.err != nil {
			observer.fatalf("concurrent index build failed: %v", build.err)
		}
		observer.exec("drop index cf_lock_catalog_code", 0, func() (sql.Result, error) {
			return fixture.admin.ExecContext(observer.ctx, "DROP INDEX public.cf_lock_catalog_code")
		})
	})

	t.Run("index_build_waits", func(t *testing.T) {
		observer := newRowLockObserver(t, ctx, fixture)
		version := observer.session()
		versions := observer.begin(version)
		observer.exec("lock version of R1", 1, func() (sql.Result, error) {
			return versions.ExecContext(version.ctx, `
				SELECT 1
				FROM synchro.sync_row_versions version
				JOIN synchro.sync_registry registry ON registry.relation_id = version.relation_id
				JOIN synchro.sync_registry_generations generation
				  ON generation.generation = registry.registry_generation
				WHERE generation.state = 'active'
				  AND registry.table_name = 'cf_lock_catalog'
				  AND version.record_id = 'lock-r1'
				FOR UPDATE OF version`)
		})
		update := fixture.mutation(fixture.catalog, "lock-r1", "update",
			map[string]any{fixture.catalog.ValueField: "r1-new"})
		push := observer.push(update)
		pushPID := observer.pushPID(push)
		// The push has read the key columns and locked the source row.
		observer.blocked(push, pushPID, version.pid)
		builder := observer.session()
		build := observer.statement("index build", func() (sql.Result, error) {
			return builder.conn.ExecContext(builder.ctx,
				"CREATE UNIQUE INDEX cf_lock_catalog_value ON public.cf_lock_catalog (value)")
		})
		observer.blocked(build, builder.pid, pushPID)
		observer.commit("version lock", versions)
		observer.wait(push)
		observer.wait(build)
		if build.err != nil {
			observer.fatalf("index build failed: %v", build.err)
		}
		observer.exec("drop index cf_lock_catalog_value", 0, func() (sql.Result, error) {
			return fixture.admin.ExecContext(observer.ctx, "DROP INDEX public.cf_lock_catalog_value")
		})
		observer.requireOutcomes(push, []map[string]any{update}, nil)
	})

	// The unique index on code exists only on cf_lock_parts_leaf, so code is a key column of that
	// leaf and not of the registered root. Each native statement is the PostgreSQL control.
	t.Run("partition_leaf", func(t *testing.T) {
		observer := newRowLockObserver(t, ctx, fixture)
		// A rebuild waits for WAL capture of the accepted pushes of the client.
		observer.poll("capture of the accepted pushes", func(ctx context.Context) (bool, error) {
			var pending bool
			var poison string
			if err := fixture.admin.QueryRowContext(ctx, `
				SELECT EXISTS (
					SELECT 1 FROM synchro.sync_write_fences
					WHERE client_id = $1 AND mutation_id IS NOT NULL AND coverage = 'pending'
				),
				COALESCE((
					SELECT string_agg(concat_ws(': ', failure_class, failure_detail), '; ')
					FROM synchro.sync_wal_poison WHERE lifecycle = 'active'
				), '')`, fixture.client.ID).Scan(&pending, &poison); err != nil {
				return false, err
			}
			if poison != "" {
				return false, fmt.Errorf("WAL capture is poisoned: %s", poison)
			}
			return !pending, nil
		})
		parts := waitForRealShapeTable(t, ctx, harness, "cf_lock_parts")
		tables := []realShapeTable{parts}
		rebuildRealShapeOtherScope(t, ctx, harness, token, fixture.client, realShapeUserScope, fixture.nextID())
		states := rebuildRealShapeRows(t, ctx, harness, token, admin, fixture.client, realShapeUserScope, fixture.nextID(), tables)
		var path string
		observer.scan(`
			SELECT string_agg(tree.relid::regclass::text, ',' ORDER BY tree.level)
			FROM pg_catalog.pg_partition_tree('public.cf_lock_parts') tree
			JOIN pg_catalog.pg_partition_ancestors('public.cf_lock_parts_leaf') path
			  ON path.relid = tree.relid`, &path)
		requirePath := func(name string, pid int) {
			t.Helper()
			if locks := observer.partitionLocks(pid); locks != path {
				observer.fatalf("%s holds RowExclusiveLock on %q, want %q", name, locks, path)
			}
		}
		gate := observer.session()
		native := observer.session()
		writer := observer.session()

		observer.holdGate(gate, 7)
		nativeWrites := observer.begin(native)
		nativeUpdate := observer.statement("native update of S1", func() (sql.Result, error) {
			return nativeWrites.ExecContext(native.ctx,
				"UPDATE public.cf_lock_parts SET value = 'native' WHERE id = 'lock-s1'")
		})
		observer.gateWait(nativeUpdate, native.pid, 7)
		requirePath("native update of S1", native.pid)
		writes := observer.begin(writer)
		observer.notBlocked(writer, native.pid, "insert child of S1", 1, func() (sql.Result, error) {
			return writes.ExecContext(writer.ctx,
				"INSERT INTO public.cf_lock_parts_children (id, parent_id) VALUES ('lock-child-s1', 'lock-s1')")
		})
		observer.commit("writer", writes)
		observer.releaseGate(gate, 7)
		observer.wait(nativeUpdate)
		if nativeUpdate.err != nil || nativeUpdate.rows != 1 {
			observer.fatalf("native update of S1 = %d rows, error %v, want 1 row", nativeUpdate.rows, nativeUpdate.err)
		}
		observer.commit("native transaction", nativeWrites)

		observer.holdGate(gate, 8)
		update := fixture.mutation(fixture.parts, "lock-s2", "update",
			map[string]any{fixture.parts.ValueField: "s2-new"})
		push := observer.push(update)
		pushPID := observer.pushPID(push)
		observer.gateWait(push, pushPID, 8)
		requirePath("push update of S2", pushPID)
		writes = observer.begin(writer)
		observer.notBlocked(writer, pushPID, "insert child of S2", 1, func() (sql.Result, error) {
			return writes.ExecContext(writer.ctx,
				"INSERT INTO public.cf_lock_parts_children (id, parent_id) VALUES ('lock-child-s2', 'lock-s2')")
		})
		observer.commit("writer", writes)
		observer.releaseGate(gate, 8)
		observer.wait(push)
		observer.requireOutcomes(push, []map[string]any{update}, nil)

		writes = observer.begin(writer)
		observer.exec("insert child of S3", 1, func() (sql.Result, error) {
			return writes.ExecContext(writer.ctx,
				"INSERT INTO public.cf_lock_parts_children (id, parent_id) VALUES ('lock-child-s3', 'lock-s3')")
		})
		nativeWrites = observer.begin(native)
		nativeKey := observer.statement("native key update of S3", func() (sql.Result, error) {
			return nativeWrites.ExecContext(native.ctx,
				"UPDATE public.cf_lock_parts SET code = 'code-s3-native' WHERE id = 'lock-s3'")
		})
		observer.blocked(nativeKey, native.pid, writer.pid)
		requirePath("native key update of S3", native.pid)
		observer.commit("writer", writes)
		observer.wait(nativeKey)
		if nativeKey.err != nil || nativeKey.rows != 1 {
			observer.fatalf("native key update of S3 = %d rows, error %v, want 1 row", nativeKey.rows, nativeKey.err)
		}
		observer.commit("native transaction", nativeWrites)

		writes = observer.begin(writer)
		observer.exec("insert child of S4", 1, func() (sql.Result, error) {
			return writes.ExecContext(writer.ctx,
				"INSERT INTO public.cf_lock_parts_children (id, parent_id) VALUES ('lock-child-s4', 'lock-s4')")
		})
		keyed := fixture.mutation(fixture.parts, "lock-s4", "update",
			map[string]any{fixture.partsCode: "code-s4-new"})
		push = observer.push(keyed)
		pushPID = observer.pushPID(push)
		observer.blocked(push, pushPID, writer.pid)
		requirePath("push key update of S4", pushPID)
		// The push waits for its first lock and holds no row lock on S4.
		observer.notBlocked(writer, pushPID, "update value of S4", 1, func() (sql.Result, error) {
			return writes.ExecContext(writer.ctx,
				"UPDATE public.cf_lock_parts SET value = 'writer' WHERE id = 'lock-s4'")
		})
		observer.commit("writer", writes)
		observer.wait(push)
		row := observer.requireOutcomes(push, nil, []map[string]any{keyed})[keyed["mutation_id"].(string)]
		if row[fixture.parts.ValueField] != "writer" || row[fixture.partsCode] != "code-s4" {
			observer.fatalf("S4 server row = %#v, want value writer and code code-s4", row)
		}

		// WAL capture of the leaf rows continues, so pull reaches each committed row and version.
		pullRealShapeUntilServer(t, ctx, harness, token, admin, fixture.client, realShapeUserScope, tables, states)
		want := map[string][2]string{
			"lock-s1": {"code-s1", "native"},
			"lock-s2": {"code-s2", "s2-new"},
			"lock-s3": {"code-s3-native", "value-s3"},
			"lock-s4": {"code-s4", "writer"},
		}
		for id, values := range want {
			if row := states["cf_lock_parts"][id].values; row["code"] != values[0] || row["value"] != values[1] {
				observer.fatalf("pulled %s row = %#v, want code %s and value %s", id, row, values[0], values[1])
			}
		}
	})
}

// partitionLocks returns each relation of the cf_lock_parts partition tree on which the backend
// holds RowExclusiveLock, from the root down.
func (observer *rowLockObserver) partitionLocks(pid int) string {
	observer.t.Helper()
	var relations string
	if err := observer.fixture.admin.QueryRowContext(observer.ctx, `
		SELECT COALESCE(string_agg(tree.relid::regclass::text, ',' ORDER BY tree.level, tree.relid::regclass::text), '')
		FROM pg_catalog.pg_partition_tree('public.cf_lock_parts') tree
		WHERE EXISTS (
			SELECT 1 FROM pg_catalog.pg_locks held
			WHERE held.locktype = 'relation'
			  AND held.database = (SELECT oid FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database())
			  AND held.relation = tree.relid
			  AND held.pid = $1::integer
			  AND held.mode = 'RowExclusiveLock'
			  AND held.granted
		)`, pid).Scan(&relations); err != nil {
		observer.fatalf("read partition locks of backend %d: %v", pid, err)
	}
	return relations
}

// rowLockFixture holds the registered relations and the row versions of the row lock test.
type rowLockFixture struct {
	harness     *blackbox.Harness
	token       string
	admin       *sql.DB
	sessions    *sql.DB
	client      *realProtocolClient
	parents     realProtocolTable
	hardParents realProtocolTable
	catalog     realProtocolTable
	parts       realProtocolTable
	parentCode  string
	catalogCode string
	partsCode   string
	versions    map[string]string
	sequence    int
}

// rowLockSession is one dedicated database session of a test role.
type rowLockSession struct {
	ctx  context.Context
	conn *sql.Conn
	pid  int
}

// rowLockCall is one push request or one SQL statement that runs in a goroutine.
type rowLockCall struct {
	name     string
	request  bool
	done     chan struct{}
	status   int
	response map[string]any
	rows     int64
	err      error
}

// rowLockObserver runs the waits and the checks of one subtest.
type rowLockObserver struct {
	t       *testing.T
	ctx     context.Context
	fixture *rowLockFixture
	calls   []*rowLockCall
}

func (fixture *rowLockFixture) nextID() string {
	fixture.sequence++
	return fmt.Sprintf("00000000-0000-4000-8277-%012x", fixture.sequence)
}

// mutation returns one push mutation of the row with the key. A mutation that is not an insert
// uses the version from the fixture push as its base version.
func (fixture *rowLockFixture) mutation(table realProtocolTable, key, op string, columns map[string]any) map[string]any {
	mutation := map[string]any{
		"mutation_id":     fixture.nextID(),
		"table":           table.ID,
		"pk":              map[string]any{table.PrimaryKeyField: key},
		"authored_schema": fixture.client.Schema,
		"op":              op,
		"client_version":  phase4ClientVersion,
	}
	if op != "insert" {
		mutation["base_version"] = fixture.versions[key]
	}
	if columns != nil {
		mutation["columns"] = columns
	}
	return mutation
}

func (call *rowLockCall) finished() bool {
	select {
	case <-call.done:
		return true
	default:
		return false
	}
}

func (call *rowLockCall) String() string {
	if !call.finished() {
		return call.name + ": pending"
	}
	if call.request {
		return fmt.Sprintf("%s: status %d response %#v error %v", call.name, call.status, call.response, call.err)
	}
	return fmt.Sprintf("%s: rows %d error %v", call.name, call.rows, call.err)
}

func newRowLockObserver(t *testing.T, ctx context.Context, fixture *rowLockFixture) *rowLockObserver {
	ctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	return &rowLockObserver{t: t, ctx: ctx, fixture: fixture}
}

// session opens a dedicated session. The cleanup cancels its operations and closes it. The pool
// keeps no idle connection, so the close ends the session, its transaction, and its advisory
// locks.
func (observer *rowLockObserver) session() *rowLockSession {
	observer.t.Helper()
	ctx, cancel := context.WithCancel(observer.ctx)
	conn, err := observer.fixture.sessions.Conn(ctx)
	if err != nil {
		cancel()
		observer.fatalf("open row lock session: %v", err)
	}
	observer.t.Cleanup(func() {
		cancel()
		_ = conn.Close()
	})
	session := &rowLockSession{ctx: ctx, conn: conn}
	if err := conn.QueryRowContext(ctx, "SELECT pg_catalog.pg_backend_pid()").Scan(&session.pid); err != nil {
		observer.fatalf("read row lock session PID: %v", err)
	}
	return session
}

func (observer *rowLockObserver) begin(session *rowLockSession) *sql.Tx {
	observer.t.Helper()
	transaction, err := session.conn.BeginTx(session.ctx, nil)
	if err != nil {
		observer.fatalf("begin session %d transaction: %v", session.pid, err)
	}
	return transaction
}

func (observer *rowLockObserver) commit(name string, transaction *sql.Tx) {
	observer.t.Helper()
	if err := transaction.Commit(); err != nil {
		observer.fatalf("commit %s: %v", name, err)
	}
}

func (observer *rowLockObserver) exec(name string, wantRows int64, run func() (sql.Result, error)) {
	observer.t.Helper()
	result, err := run()
	var rows int64
	if err == nil {
		rows, err = result.RowsAffected()
	}
	if err != nil || rows != wantRows {
		observer.fatalf("%s = %d rows, error %v, want %d rows", name, rows, err, wantRows)
	}
}

func (observer *rowLockObserver) holdGate(gate *rowLockSession, number int) {
	observer.t.Helper()
	if _, err := gate.conn.ExecContext(gate.ctx,
		"SELECT pg_catalog.pg_advisory_lock($1, $2)", rowLockGateClass, number); err != nil {
		observer.fatalf("hold gate %d: %v", number, err)
	}
}

func (observer *rowLockObserver) releaseGate(gate *rowLockSession, number int) {
	observer.t.Helper()
	var released bool
	if err := gate.conn.QueryRowContext(gate.ctx,
		"SELECT pg_catalog.pg_advisory_unlock($1, $2)", rowLockGateClass, number).Scan(&released); err != nil || !released {
		observer.fatalf("release gate %d = %t, error %v", number, released, err)
	}
}

func (observer *rowLockObserver) push(mutations ...map[string]any) *rowLockCall {
	fixture := observer.fixture
	payload := phase4PushPayload(fixture.client, fixture.nextID(), mutations)
	call := &rowLockCall{name: "push", request: true, done: make(chan struct{})}
	observer.calls = append(observer.calls, call)
	ctx := observer.ctx
	go func() {
		defer close(call.done)
		call.status, call.response, call.err = executeSyncRequest(
			ctx, fixture.harness.AdapterURL(), fixture.token, "/sync/push", payload)
	}()
	return call
}

func (observer *rowLockObserver) statement(name string, run func() (sql.Result, error)) *rowLockCall {
	call := &rowLockCall{name: name, done: make(chan struct{})}
	observer.calls = append(observer.calls, call)
	go func() {
		defer close(call.done)
		result, err := run()
		if err == nil {
			call.rows, err = result.RowsAffected()
		}
		call.err = err
	}()
	return call
}

func (observer *rowLockObserver) fatalf(format string, arguments ...any) {
	observer.t.Helper()
	observer.t.Fatalf("%s\ncalls: %v\nlock state: %s",
		fmt.Sprintf(format, arguments...), observer.calls, rowLockState(observer.fixture.admin))
}

// rowLockState returns the client backends of the test database and the lock requests that
// wait.
func rowLockState(admin *sql.DB) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var state string
	if err := admin.QueryRowContext(ctx, `
		SELECT jsonb_build_object(
			'backends', COALESCE((
				SELECT jsonb_agg(jsonb_build_object(
					'pid', activity.pid,
					'state', activity.state,
					'wait_event', concat_ws(':', activity.wait_event_type, activity.wait_event),
					'blocking_pids', pg_catalog.pg_blocking_pids(activity.pid),
					'query', left(activity.query, 160)
				) ORDER BY activity.pid)
				FROM pg_catalog.pg_stat_activity activity
				WHERE activity.datname = pg_catalog.current_database()
				  AND activity.backend_type = 'client backend'
				  AND activity.pid <> pg_catalog.pg_backend_pid()
			), '[]'::jsonb),
			'waiting_locks', COALESCE((
				SELECT jsonb_agg(jsonb_build_object(
					'pid', waiting.pid,
					'locktype', waiting.locktype,
					'mode', waiting.mode,
					'relation', waiting.relation::regclass::text,
					'classid', waiting.classid,
					'objid', waiting.objid,
					'transactionid', waiting.transactionid::text,
					'virtualxid', waiting.virtualxid
				) ORDER BY waiting.pid)
				FROM pg_catalog.pg_locks waiting
				WHERE NOT waiting.granted
			), '[]'::jsonb)
		)::text`).Scan(&state); err != nil {
		return fmt.Sprintf("unavailable: %v", err)
	}
	return state
}

// poll runs check until it returns true. The wait bound comes from the subtest context.
func (observer *rowLockObserver) poll(what string, check func(context.Context) (bool, error)) {
	observer.t.Helper()
	ctx, cancel := context.WithTimeout(observer.ctx, rowLockWait)
	defer cancel()
	for {
		ready, err := check(ctx)
		if err != nil {
			observer.fatalf("wait for %s: %v", what, err)
		}
		if ready {
			return
		}
		select {
		case <-ctx.Done():
			observer.fatalf("wait for %s: %v", what, ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (observer *rowLockObserver) wait(call *rowLockCall) {
	observer.t.Helper()
	ctx, cancel := context.WithTimeout(observer.ctx, rowLockWait)
	defer cancel()
	select {
	case <-call.done:
	case <-ctx.Done():
		observer.fatalf("%s did not return: %v", call.name, ctx.Err())
	}
}

// pushPID returns the backend that runs the push. The query text is split, so that the poll
// query does not match itself.
func (observer *rowLockObserver) pushPID(push *rowLockCall) int {
	observer.t.Helper()
	var pid int
	observer.poll("push backend", func(ctx context.Context) (bool, error) {
		rows, err := observer.fixture.admin.QueryContext(ctx, `
			SELECT pid FROM pg_catalog.pg_stat_activity
			WHERE datname = pg_catalog.current_database()
			  AND backend_type = 'client backend'
			  AND state = 'active'
			  AND pid <> pg_catalog.pg_backend_pid()
			  AND query LIKE '%synchro.' || 'synchro_push%'`)
		if err != nil {
			return false, err
		}
		defer rows.Close()
		var pids []int
		for rows.Next() {
			var found int
			if err := rows.Scan(&found); err != nil {
				return false, err
			}
			pids = append(pids, found)
		}
		if err := rows.Err(); err != nil {
			return false, err
		}
		switch {
		case len(pids) > 1:
			return false, fmt.Errorf("more than one push backend: %v", pids)
		case len(pids) == 1:
			pid = pids[0]
			return true, nil
		case push.finished():
			return false, errors.New("the push returned before its backend was observed")
		}
		return false, nil
	})
	return pid
}

// gateWait waits until the backend waits for the gate in the gate trigger.
func (observer *rowLockObserver) gateWait(waiter *rowLockCall, pid, gate int) {
	observer.t.Helper()
	observer.poll(fmt.Sprintf("%s wait for gate %d", waiter.name, gate), func(ctx context.Context) (bool, error) {
		var waiting bool
		if err := observer.fixture.admin.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_catalog.pg_locks
				WHERE locktype = 'advisory'
				  AND classid = $1::integer::oid
				  AND objid = $2::integer::oid
				  AND objsubid = 2
				  AND pid = $3::integer
				  AND NOT granted
			)`, rowLockGateClass, gate, pid).Scan(&waiting); err != nil {
			return false, err
		}
		if !waiting && waiter.finished() {
			return false, errors.New("the waiter returned")
		}
		return waiting, nil
	})
}

// blocked waits until the blocker is in the blocking backends of the waiter.
func (observer *rowLockObserver) blocked(waiter *rowLockCall, waiterPID, blockerPID int) {
	observer.t.Helper()
	observer.poll(fmt.Sprintf("%s blocked by backend %d", waiter.name, blockerPID), func(ctx context.Context) (bool, error) {
		var blocked bool
		if err := observer.fixture.admin.QueryRowContext(ctx,
			"SELECT $2::integer = ANY (pg_catalog.pg_blocking_pids($1::integer))",
			waiterPID, blockerPID).Scan(&blocked); err != nil {
			return false, err
		}
		if !blocked && waiter.finished() {
			return false, errors.New("the waiter returned")
		}
		return blocked, nil
	})
}

// notBlocked runs the statement of the session in a goroutine until it returns. It fails at once
// when the blocker is in the blocking backends of the session.
func (observer *rowLockObserver) notBlocked(session *rowLockSession, blockerPID int, name string, wantRows int64, run func() (sql.Result, error)) {
	observer.t.Helper()
	call := observer.statement(name, run)
	ctx, cancel := context.WithTimeout(observer.ctx, rowLockWait)
	defer cancel()
	for !call.finished() {
		var blocked bool
		if err := observer.fixture.admin.QueryRowContext(ctx,
			"SELECT $2::integer = ANY (pg_catalog.pg_blocking_pids($1::integer))",
			session.pid, blockerPID).Scan(&blocked); err != nil {
			observer.fatalf("read blocking backends of %s: %v", name, err)
		}
		if blocked {
			observer.fatalf("%s is blocked by backend %d", name, blockerPID)
		}
		select {
		case <-call.done:
		case <-ctx.Done():
			observer.fatalf("%s did not return: %v", name, ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
	if call.err != nil || call.rows != wantRows {
		observer.fatalf("%s = %d rows, error %v, want %d rows", name, call.rows, call.err, wantRows)
	}
}

// requireOutcomes requires status 200, one applied outcome for each applied mutation, and one
// version conflict for each conflict mutation. It returns the server row of each conflict by
// mutation ID.
func (observer *rowLockObserver) requireOutcomes(push *rowLockCall, applied, conflicts []map[string]any) map[string]map[string]any {
	observer.t.Helper()
	if push.err != nil || push.status != http.StatusOK {
		observer.fatalf("push did not return status 200")
	}
	accepted := requireOutcomeList(observer.t, push.response, "accepted")
	rejected := requireOutcomeList(observer.t, push.response, "rejected")
	if len(accepted) != len(applied) || len(rejected) != len(conflicts) {
		observer.fatalf("push has %d accepted and %d rejected outcomes, want %d and %d",
			len(accepted), len(rejected), len(applied), len(conflicts))
	}
	byID := make(map[string]map[string]any, len(accepted)+len(rejected))
	for _, outcomes := range [][]map[string]any{accepted, rejected} {
		for _, outcome := range outcomes {
			id, _ := outcome["mutation_id"].(string)
			byID[id] = outcome
		}
	}
	for _, mutation := range applied {
		if outcome := byID[mutation["mutation_id"].(string)]; outcome["status"] != "applied" {
			observer.fatalf("mutation %v outcome = %#v, want applied", mutation["mutation_id"], outcome)
		}
	}
	rows := make(map[string]map[string]any, len(conflicts))
	for _, mutation := range conflicts {
		id := mutation["mutation_id"].(string)
		outcome := byID[id]
		row, ok := outcome["server_row"].(map[string]any)
		if outcome["status"] != "conflict" || outcome["code"] != "version_conflict" || !ok {
			observer.fatalf("mutation %s outcome = %#v, want version_conflict", id, outcome)
		}
		rows[id] = row
	}
	return rows
}

func (observer *rowLockObserver) scan(query string, destinations ...any) {
	observer.t.Helper()
	if err := observer.fixture.admin.QueryRowContext(observer.ctx, query).Scan(destinations...); err != nil {
		observer.fatalf("read row state: %v", err)
	}
}

// installRowLockFixture creates and registers the fixture relations, creates each registered
// row with one push, and records the version of each row.
func installRowLockFixture(t *testing.T, ctx context.Context, harness *blackbox.Harness, token string, admin *sql.DB) *rowLockFixture {
	t.Helper()
	// Registration of a partitioned table requires the D-02 root publication identity.
	if _, err := admin.ExecContext(ctx, fmt.Sprintf(
		"ALTER PUBLICATION %q SET (publish_via_partition_root = true)", harness.Names().Publication)); err != nil {
		t.Fatalf("publish partitioned tables under their root identity: %v", err)
	}
	if _, err := admin.ExecContext(ctx, `
		CREATE TABLE public.cf_lock_gates (
			id text PRIMARY KEY,
			gate integer NOT NULL UNIQUE
		);
		CREATE TABLE public.cf_lock_parents (
			id text PRIMARY KEY,
			code text NOT NULL UNIQUE,
			value text NOT NULL,
			updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
			deleted_at timestamptz
		);
		CREATE TABLE public.cf_lock_children (
			id text PRIMARY KEY,
			parent_id text NOT NULL REFERENCES public.cf_lock_parents (id)
		);
		CREATE TABLE public.cf_lock_hard_parents (
			id text PRIMARY KEY,
			value text NOT NULL
		);
		CREATE TABLE public.cf_lock_hard_children (
			id text PRIMARY KEY,
			parent_id text NOT NULL REFERENCES public.cf_lock_hard_parents (id)
		);
		CREATE TABLE public.cf_lock_catalog (
			id text PRIMARY KEY,
			code text NOT NULL,
			value text NOT NULL
		);
		CREATE TABLE public.cf_lock_catalog_children (
			id text PRIMARY KEY,
			parent_id text NOT NULL REFERENCES public.cf_lock_catalog (id)
		);
		CREATE TABLE public.cf_lock_parts (
			id text PRIMARY KEY,
			code text NOT NULL,
			value text NOT NULL,
			updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
			deleted_at timestamptz
		) PARTITION BY RANGE (id);
		CREATE TABLE public.cf_lock_parts_other PARTITION OF public.cf_lock_parts
			FOR VALUES FROM (MINVALUE) TO ('lock-s');
		CREATE TABLE public.cf_lock_parts_sub PARTITION OF public.cf_lock_parts
			FOR VALUES FROM ('lock-s') TO (MAXVALUE) PARTITION BY RANGE (id);
		CREATE TABLE public.cf_lock_parts_leaf PARTITION OF public.cf_lock_parts_sub
			FOR VALUES FROM ('lock-s') TO ('lock-t');
		CREATE TABLE public.cf_lock_parts_sibling PARTITION OF public.cf_lock_parts_sub
			FOR VALUES FROM ('lock-t') TO (MAXVALUE);
		CREATE UNIQUE INDEX cf_lock_parts_leaf_code ON public.cf_lock_parts_leaf (code);
		CREATE TABLE public.cf_lock_parts_children (
			id text PRIMARY KEY,
			parent_id text NOT NULL REFERENCES public.cf_lock_parts (id)
		);
		GRANT SELECT, INSERT, UPDATE ON TABLE public.cf_lock_parents, public.cf_lock_parts TO synchro_owner;
		GRANT SELECT, INSERT, UPDATE, DELETE
			ON TABLE public.cf_lock_hard_parents, public.cf_lock_catalog
			TO synchro_owner;
		GRANT SELECT
			ON TABLE public.cf_lock_parents, public.cf_lock_hard_parents, public.cf_lock_catalog,
				public.cf_lock_parts
			TO synchro_worker;
		GRANT SELECT ON public.cf_lock_gates TO synchro_owner;
		ALTER TABLE public.cf_lock_parents ENABLE ROW LEVEL SECURITY;
		ALTER TABLE public.cf_lock_hard_parents ENABLE ROW LEVEL SECURITY;
		ALTER TABLE public.cf_lock_catalog ENABLE ROW LEVEL SECURITY;
		ALTER TABLE public.cf_lock_parts ENABLE ROW LEVEL SECURITY;
		CREATE POLICY synchro_owner_all ON public.cf_lock_parents
			AS PERMISSIVE FOR ALL TO synchro_owner USING (true) WITH CHECK (true);
		CREATE POLICY synchro_owner_all ON public.cf_lock_hard_parents
			AS PERMISSIVE FOR ALL TO synchro_owner USING (true) WITH CHECK (true);
		CREATE POLICY synchro_owner_all ON public.cf_lock_catalog
			AS PERMISSIVE FOR ALL TO synchro_owner USING (true) WITH CHECK (true);
		CREATE POLICY synchro_owner_all ON public.cf_lock_parts
			AS PERMISSIVE FOR ALL TO synchro_owner USING (true) WITH CHECK (true);
		CREATE FUNCTION public.cf_lock_parents_membership(p_id text)
		RETURNS SETOF text
		LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
		BEGIN ATOMIC SELECT 'user:diagnostic-user'::text; END;
		CREATE FUNCTION public.cf_lock_hard_parents_membership(p_id text)
		RETURNS SETOF text
		LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
		BEGIN ATOMIC SELECT 'user:diagnostic-user'::text; END;
		CREATE FUNCTION public.cf_lock_catalog_membership(p_id text)
		RETURNS SETOF text
		LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
		BEGIN ATOMIC SELECT 'user:diagnostic-user'::text; END;
		CREATE FUNCTION public.cf_lock_parts_membership(p_id text)
		RETURNS SETOF text
		LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
		BEGIN ATOMIC SELECT 'user:diagnostic-user'::text; END;
		REVOKE ALL ON FUNCTION
			public.cf_lock_parents_membership(text),
			public.cf_lock_hard_parents_membership(text),
			public.cf_lock_catalog_membership(text),
			public.cf_lock_parts_membership(text)
			FROM PUBLIC;
		GRANT EXECUTE ON FUNCTION
			public.cf_lock_parents_membership(text),
			public.cf_lock_hard_parents_membership(text),
			public.cf_lock_catalog_membership(text),
			public.cf_lock_parts_membership(text)
			TO synchro_owner, synchro_worker;
		CREATE FUNCTION public.cf_lock_gate() RETURNS trigger
		LANGUAGE plpgsql AS $$
		DECLARE
			gate_number integer;
		BEGIN
			SELECT gate INTO gate_number FROM public.cf_lock_gates WHERE id = NEW.id;
			IF FOUND THEN
				PERFORM pg_catalog.pg_advisory_xact_lock_shared(265100, gate_number);
			END IF;
			RETURN NEW;
		END
		$$;
		CREATE FUNCTION public.cf_lock_rekey() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.value = 'rekey' THEN
				NEW.code := NEW.code || '-rekeyed';
			END IF;
			RETURN NEW;
		END
		$$;
		CREATE FUNCTION public.cf_lock_target_key() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			IF OLD.deleted_at IS NULL AND NEW.deleted_at IS NOT NULL THEN
				PERFORM pg_catalog.pg_advisory_xact_lock(
					pg_catalog.hashtextextended('cf-lock:' || OLD.id, 0));
			END IF;
			RETURN NEW;
		END
		$$;
		CREATE TRIGGER cf_lock_a_gate BEFORE UPDATE ON public.cf_lock_parents
			FOR EACH ROW EXECUTE FUNCTION public.cf_lock_gate();
		CREATE TRIGGER cf_lock_a_gate BEFORE UPDATE ON public.cf_lock_catalog
			FOR EACH ROW EXECUTE FUNCTION public.cf_lock_gate();
		CREATE TRIGGER cf_lock_a_gate BEFORE UPDATE ON public.cf_lock_parts
			FOR EACH ROW EXECUTE FUNCTION public.cf_lock_gate();
		CREATE TRIGGER cf_lock_b_rekey BEFORE UPDATE ON public.cf_lock_parents
			FOR EACH ROW EXECUTE FUNCTION public.cf_lock_rekey();
		CREATE TRIGGER cf_lock_c_target_key BEFORE UPDATE ON public.cf_lock_parents
			FOR EACH ROW EXECUTE FUNCTION public.cf_lock_target_key();
		INSERT INTO public.cf_lock_gates (id, gate) VALUES
			('lock-p1', 1), ('lock-p2', 2), ('lock-p4', 4), ('lock-p5', 5), ('lock-q1', 6),
			('lock-s1', 7), ('lock-s2', 8);
		SELECT synchro.synchro_register_table(
			'public.cf_lock_parents', 'public.cf_lock_parents_membership', 'single_scope',
			'id', 'updated_at', 'deleted_at', 'enabled');
		SELECT synchro.synchro_register_table(
			'public.cf_lock_hard_parents', 'public.cf_lock_hard_parents_membership', 'single_scope',
			'id', 'updated_at', 'deleted_at', 'enabled');
		SELECT synchro.synchro_register_table(
			'public.cf_lock_catalog', 'public.cf_lock_catalog_membership', 'single_scope',
			'id', 'updated_at', 'deleted_at', 'enabled');
		SELECT synchro.synchro_register_table(
			'public.cf_lock_parts', 'public.cf_lock_parts_membership', 'single_scope',
			'id', 'updated_at', 'deleted_at', 'enabled')`); err != nil {
		t.Fatalf("install row lock fixture: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for _, name := range []string{"cf_lock_parents", "cf_lock_hard_parents", "cf_lock_catalog", "cf_lock_parts"} {
		for {
			_, err := fetchRealSchemaTableReference(ctx, harness.AdapterURL(), name)
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("row lock table %s did not activate: %v; %s", name, err, harness.FailureDiagnostics())
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	sessions, err := sql.Open("pgx", harness.DatabaseURL())
	if err != nil {
		t.Fatalf("open row lock sessions: %v", err)
	}
	sessions.SetMaxIdleConns(0)
	t.Cleanup(func() {
		if err := sessions.Close(); err != nil {
			t.Errorf("close row lock sessions: %v", err)
		}
	})
	client := connectRealProtocolClient(t, ctx, harness, token, "row-lock-client")
	fixture := &rowLockFixture{
		harness:     harness,
		token:       token,
		admin:       admin,
		sessions:    sessions,
		client:      client,
		parents:     requireRealTable(t, client, "cf_lock_parents"),
		hardParents: requireRealTable(t, client, "cf_lock_hard_parents"),
		catalog:     requireRealTable(t, client, "cf_lock_catalog"),
		parts:       requireRealTable(t, client, "cf_lock_parts"),
		parentCode:  loadRealProtocolFieldID(t, ctx, harness, "cf_lock_parents", "code"),
		catalogCode: loadRealProtocolFieldID(t, ctx, harness, "cf_lock_catalog", "code"),
		partsCode:   loadRealProtocolFieldID(t, ctx, harness, "cf_lock_parts", "code"),
		versions:    make(map[string]string),
	}
	var inserts []map[string]any
	keys := make(map[string]string)
	insert := func(table realProtocolTable, key string, columns map[string]any) {
		mutation := fixture.mutation(table, key, "insert", columns)
		keys[mutation["mutation_id"].(string)] = key
		inserts = append(inserts, mutation)
	}
	for _, key := range []string{"p1", "p2", "p3", "p4", "p5"} {
		insert(fixture.parents, "lock-"+key, map[string]any{
			fixture.parentCode:         "code-" + key,
			fixture.parents.ValueField: "value-" + key,
		})
	}
	insert(fixture.hardParents, "lock-h1", map[string]any{fixture.hardParents.ValueField: "value-h1"})
	for _, key := range []string{"q0", "q1", "q2", "r1"} {
		insert(fixture.catalog, "lock-"+key, map[string]any{
			fixture.catalogCode:        "code-" + key,
			fixture.catalog.ValueField: "value-" + key,
		})
	}
	for _, key := range []string{"s1", "s2", "s3", "s4"} {
		insert(fixture.parts, "lock-"+key, map[string]any{
			fixture.partsCode:        "code-" + key,
			fixture.parts.ValueField: "value-" + key,
		})
	}
	status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push",
		phase4PushPayload(client, fixture.nextID(), inserts))
	if status != http.StatusOK {
		t.Fatalf("row lock fixture push status = %d: %#v", status, response)
	}
	if rejected := requireOutcomeList(t, response, "rejected"); len(rejected) != 0 {
		t.Fatalf("row lock fixture push rejected rows: %#v", rejected)
	}
	accepted := requireOutcomeList(t, response, "accepted")
	for _, outcome := range accepted {
		id, _ := outcome["mutation_id"].(string)
		version, _ := outcome["server_version"].(string)
		key, known := keys[id]
		if !known || outcome["status"] != "applied" || !uuidPattern.MatchString(version) {
			t.Fatalf("row lock fixture push outcome is invalid: %#v", outcome)
		}
		fixture.versions[key] = version
	}
	if len(fixture.versions) != len(inserts) {
		t.Fatalf("row lock fixture push applied %d rows, want %d: %#v", len(fixture.versions), len(inserts), accepted)
	}
	return fixture
}
