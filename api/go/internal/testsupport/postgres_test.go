package testsupport

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"testing"
)

func TestUniqueName(t *testing.T) {
	const prefix = "Long Prefix/With Spaces"
	name := UniqueName(t, prefix)

	if len(name) > 63 {
		t.Fatalf("UniqueName length = %d, want at most 63 bytes", len(name))
	}
	if !regexp.MustCompile(`^[a-z0-9_]+$`).MatchString(name) {
		t.Fatalf("UniqueName = %q, want a lowercase PostgreSQL identifier", name)
	}
	if !strings.Contains(name, "long_prefix_with_spaces") {
		t.Fatalf("UniqueName = %q, want sanitized prefix", name)
	}
	if strings.Contains(name, "Long Prefix") {
		t.Fatalf("UniqueName = %q, negative control matched unsanitized prefix", name)
	}

	other := UniqueName(t, "other")
	if name == other {
		t.Fatal("UniqueName returned the same name for distinct prefixes")
	}
}

func TestQuoteIdentifier(t *testing.T) {
	got := quoteIdentifier(`role"name`)
	if got != `"role""name"` {
		t.Fatalf("quoteIdentifier() = %q, want escaped identifier", got)
	}
	if got == `"role"name"` {
		t.Fatal("quoteIdentifier negative control accepted an unescaped quote")
	}
}

func TestQuoteLiteral(t *testing.T) {
	got := quoteLiteral("owner's role")
	if got != `'owner''s role'` {
		t.Fatalf("quoteLiteral() = %q, want escaped literal", got)
	}
	if got == `'owner's role'` {
		t.Fatal("quoteLiteral negative control accepted an unescaped quote")
	}
}

func TestContainsLibrary(t *testing.T) {
	value := "  pg_stat_statements, synchro_pg , other_library  "
	if !containsLibrary(value, "synchro_pg") {
		t.Fatalf("containsLibrary() = false, want exact library match with whitespace")
	}
	if containsLibrary(value, "synchro") {
		t.Fatal("containsLibrary negative control accepted a partial library name")
	}
	if containsLibrary("other_library", "synchro_pg") {
		t.Fatal("containsLibrary negative control accepted an absent library")
	}
}

var contractDriverSequence uint64

type contractDriverState struct {
	mu       sync.Mutex
	result   []byte
	queries  int
	queryErr error
}

type contractDriver struct {
	state *contractDriverState
}

func (d contractDriver) Open(string) (driver.Conn, error) {
	return contractConnection{state: d.state}, nil
}

type contractConnection struct {
	state *contractDriverState
}

func (c contractConnection) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepare is not supported")
}

func (c contractConnection) Close() error {
	return nil
}

func (c contractConnection) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("transactions are not supported")
}

func (c contractConnection) QueryContext(_ context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	c.state.queries++
	if c.state.queryErr != nil {
		return nil, c.state.queryErr
	}
	return &contractRows{result: c.state.result}, nil
}

type contractRows struct {
	result []byte
	done   bool
}

func (r *contractRows) Columns() []string {
	return []string{"synchro_contract_info"}
}

func (r *contractRows) Close() error {
	return nil
}

func (r *contractRows) Next(values []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	values[0] = r.result
	return nil
}

func TestVerifyExtensionObjectsRejectsStale(t *testing.T) {
	stale, err := json.Marshal(map[string]any{
		"library_build_fingerprint":   "library-fingerprint",
		"installed_build_fingerprint": "installed-fingerprint",
		"extension_objects_current":   false,
	})
	if err != nil {
		t.Fatalf("encoding stale contract: %v", err)
	}
	state := &contractDriverState{result: stale}
	driverName := fmt.Sprintf("synchro-contract-%d", contractDriverSequence)
	contractDriverSequence++
	sql.Register(driverName, contractDriver{state: state})
	db, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatalf("opening contract database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	err = extensionObjectsError(context.Background(), db)
	if err == nil {
		t.Fatal("stale extension objects passed test support validation")
	}
	for _, expected := range []string{
		`library fingerprint "library-fingerprint"`,
		`installed objects fingerprint "installed-fingerprint"`,
		"recreate or update the extension",
	} {
		if !strings.Contains(err.Error(), expected) {
			t.Fatalf("stale extension error = %q, missing %q", err, expected)
		}
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.queries != 1 {
		t.Fatalf("contract queries = %d, want 1", state.queries)
	}
}
