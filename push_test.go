package synchro

import (
	"testing"
)

// helper to build a registered TableConfig with protectedSet populated.
// Simulates a table with all timestamp columns present (the common case).
func testConfig(t *testing.T, opts ...func(*TableConfig)) *TableConfig {
	t.Helper()
	cfg := &TableConfig{
		TableName:   "orders",
		PushPolicy:  PushPolicyOwnerOnly,
		OwnerColumn: "user_id",
	}
	for _, fn := range opts {
		fn(cfg)
	}
	// Apply defaults, same as Register does.
	if cfg.IDColumn == "" {
		cfg.IDColumn = "id"
	}
	// Simulate introspection: all timestamp columns present by default.
	cfg.hasUpdatedAt = true
	cfg.hasDeletedAt = true
	cfg.hasCreatedAt = true
	cfg.updatedAtColumn = "updated_at"
	cfg.deletedAtColumn = "deleted_at"
	cfg.finalizeProtectedSet()
	return cfg
}

func TestInsertRecord_ColumnFiltering(t *testing.T) {
	cfg := testConfig(t)

	dataCols := []string{"id", "user_id", "name", "description", "created_at", "updated_at", "deleted_at"}
	allowed := cfg.AllowedInsertColumns(dataCols)

	allowedSet := make(map[string]bool, len(allowed))
	for _, col := range allowed {
		allowedSet[col] = true
	}

	// id and user_id are allowed on insert even though they are protected.
	if !allowedSet["id"] {
		t.Error("expected 'id' to be allowed on insert")
	}
	if !allowedSet["user_id"] {
		t.Error("expected 'user_id' (owner column) to be allowed on insert")
	}
	if !allowedSet["name"] {
		t.Error("expected 'name' to be allowed on insert")
	}
	if !allowedSet["description"] {
		t.Error("expected 'description' to be allowed on insert")
	}

	// Timestamps are denied.
	for _, col := range []string{"created_at", "updated_at", "deleted_at"} {
		if allowedSet[col] {
			t.Errorf("expected %q to be denied on insert", col)
		}
	}
}

func TestInsertRecord_WithParentFK(t *testing.T) {
	cfg := testConfig(t, func(c *TableConfig) {
		c.ParentFKCol = "order_id"
	})

	dataCols := []string{"id", "user_id", "order_id", "name", "updated_at"}
	allowed := cfg.AllowedInsertColumns(dataCols)

	allowedSet := make(map[string]bool, len(allowed))
	for _, col := range allowed {
		allowedSet[col] = true
	}

	// ParentFKCol should be allowed on insert.
	if !allowedSet["order_id"] {
		t.Error("expected 'order_id' (ParentFKCol) to be allowed on insert")
	}
	if allowedSet["updated_at"] {
		t.Error("expected 'updated_at' to be denied on insert")
	}
}

func TestInsertRecord_WithExtraProtected(t *testing.T) {
	cfg := testConfig(t, func(c *TableConfig) {
		c.ProtectedColumns = []string{"internal_score"}
	})

	dataCols := []string{"id", "user_id", "name", "internal_score"}
	allowed := cfg.AllowedInsertColumns(dataCols)

	allowedSet := make(map[string]bool, len(allowed))
	for _, col := range allowed {
		allowedSet[col] = true
	}

	if allowedSet["internal_score"] {
		t.Error("expected 'internal_score' (extra protected) to be denied on insert")
	}
	if !allowedSet["name"] {
		t.Error("expected 'name' to be allowed on insert")
	}
}

func TestUpdateRecord_ColumnFiltering(t *testing.T) {
	cfg := testConfig(t)

	dataCols := []string{"id", "user_id", "name", "description", "created_at", "updated_at", "deleted_at"}
	allowed := cfg.AllowedUpdateColumns(dataCols)

	allowedSet := make(map[string]bool, len(allowed))
	for _, col := range allowed {
		allowedSet[col] = true
	}

	// Only non-protected columns should pass through.
	if !allowedSet["name"] {
		t.Error("expected 'name' to be allowed on update")
	}
	if !allowedSet["description"] {
		t.Error("expected 'description' to be allowed on update")
	}

	// Everything else should be denied — including id and user_id.
	for _, col := range []string{"id", "user_id", "created_at", "updated_at", "deleted_at"} {
		if allowedSet[col] {
			t.Errorf("expected %q to be denied on update", col)
		}
	}
}

func TestUpdateRecord_WithParentFK(t *testing.T) {
	cfg := testConfig(t, func(c *TableConfig) {
		c.ParentFKCol = "order_id"
	})

	dataCols := []string{"order_id", "name"}
	allowed := cfg.AllowedUpdateColumns(dataCols)

	allowedSet := make(map[string]bool, len(allowed))
	for _, col := range allowed {
		allowedSet[col] = true
	}

	// ParentFKCol is protected on update.
	if allowedSet["order_id"] {
		t.Error("expected 'order_id' (ParentFKCol) to be denied on update")
	}
	if !allowedSet["name"] {
		t.Error("expected 'name' to be allowed on update")
	}
}

func TestUpdateRecord_AllProtected_ReturnsEmpty(t *testing.T) {
	cfg := testConfig(t)

	dataCols := []string{"id", "user_id", "created_at", "updated_at", "deleted_at"}
	allowed := cfg.AllowedUpdateColumns(dataCols)

	if len(allowed) != 0 {
		t.Errorf("expected empty allowed list, got %v", allowed)
	}
}

func TestInsertRecord_EmptyData(t *testing.T) {
	cfg := testConfig(t)

	allowed := cfg.AllowedInsertColumns(nil)
	if len(allowed) != 0 {
		t.Errorf("expected empty allowed list for nil input, got %v", allowed)
	}

	allowed = cfg.AllowedInsertColumns([]string{})
	if len(allowed) != 0 {
		t.Errorf("expected empty allowed list for empty input, got %v", allowed)
	}
}

// --- quoteIdentifier tests ---

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"simple name", "table_name", `"table_name"`},
		{"already has quotes", `tab"le`, `"tab""le"`},
		{"multiple quotes", `a"b"c`, `"a""b""c"`},
		{"empty string", "", `""`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := quoteIdentifier(tt.input)
			if got != tt.want {
				t.Errorf("quoteIdentifier(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
