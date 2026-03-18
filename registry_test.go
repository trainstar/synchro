package synchro

import (
	"testing"
)

func TestRegisterForTest_Defaults(t *testing.T) {
	r := NewRegistry()
	r.RegisterForTest(&TableConfig{
		TableName: "orders",
	})

	cfg := r.Get("orders")
	if cfg == nil {
		t.Fatal("expected registered config, got nil")
	}
	if cfg.IDColumn != "id" {
		t.Errorf("IDColumn = %q, want %q", cfg.IDColumn, "id")
	}
	// UpdatedAtColumn/DeletedAtColumn are computed by introspection,
	// so they start empty until Introspect runs.
	if cfg.UpdatedAtCol() != "" {
		t.Errorf("UpdatedAtCol() = %q, want empty before introspection", cfg.UpdatedAtCol())
	}
	if cfg.DeletedAtCol() != "" {
		t.Errorf("DeletedAtCol() = %q, want empty before introspection", cfg.DeletedAtCol())
	}
}

func TestGet_Unregistered(t *testing.T) {
	r := NewRegistry()
	if got := r.Get("nonexistent"); got != nil {
		t.Errorf("Get(unregistered) = %v, want nil", got)
	}
}

func TestAll_PreservesOrder(t *testing.T) {
	r := NewRegistry()
	names := []string{"alpha", "beta", "gamma"}
	for _, n := range names {
		r.RegisterForTest(&TableConfig{TableName: n})
	}

	all := r.All()
	if len(all) != len(names) {
		t.Fatalf("All() returned %d configs, want %d", len(all), len(names))
	}
	for i, cfg := range all {
		if cfg.TableName != names[i] {
			t.Errorf("All()[%d].TableName = %q, want %q", i, cfg.TableName, names[i])
		}
	}
}

func TestTableNames(t *testing.T) {
	r := NewRegistry()
	r.RegisterForTest(&TableConfig{TableName: "a"})
	r.RegisterForTest(&TableConfig{TableName: "b"})

	got := r.TableNames()
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("TableNames() = %v, want [a b]", got)
	}

	got[0] = "modified"
	orig := r.TableNames()
	if orig[0] != "a" {
		t.Error("TableNames() returned internal slice, not a copy")
	}
}

func TestIsRegistered(t *testing.T) {
	r := NewRegistry()
	r.RegisterForTest(&TableConfig{TableName: "orders"})

	if !r.IsRegistered("orders") {
		t.Error("IsRegistered(orders) = false, want true")
	}
	if r.IsRegistered("missing") {
		t.Error("IsRegistered(missing) = true, want false")
	}
}

func TestValidate_EmptyName(t *testing.T) {
	r := NewRegistry()
	r.RegisterForTest(&TableConfig{TableName: ""})

	err := r.Validate()
	if err == nil {
		t.Fatal("expected error for empty table name, got nil")
	}
}

func TestValidate_DuplicateName(t *testing.T) {
	r := NewRegistry()
	r.RegisterForTest(&TableConfig{TableName: "orders"})
	r.RegisterForTest(&TableConfig{TableName: "orders"})

	err := r.Validate()
	if err == nil {
		t.Fatal("expected error for duplicate table name, got nil")
	}
}

func TestValidate_ValidConfig(t *testing.T) {
	r := NewRegistry()
	r.RegisterForTest(&TableConfig{TableName: "orders"})
	r.RegisterForTest(&TableConfig{TableName: "order_details"})
	r.RegisterForTest(&TableConfig{TableName: "products"})

	if err := r.Validate(); err != nil {
		t.Errorf("Validate() returned unexpected error: %v", err)
	}
}

func TestAllowedInsertColumns(t *testing.T) {
	r := NewRegistry()
	cfg := &TableConfig{
		TableName: "orders",
		foreignKeys: []FKRelation{
			{Column: "parent_id", RefTable: "parents", RefColumn: "id"},
		},
	}
	r.RegisterForTest(cfg)
	// Simulate introspection detecting all timestamp columns.
	cfg.hasUpdatedAt = true
	cfg.hasDeletedAt = true
	cfg.hasCreatedAt = true
	cfg.updatedAtColumn = "updated_at"
	cfg.deletedAtColumn = "deleted_at"
	cfg.finalizeProtectedSet()

	dataCols := []string{"id", "parent_id", "name", "created_at", "updated_at", "deleted_at"}
	allowed := cfg.AllowedInsertColumns(dataCols)

	allowedSet := make(map[string]bool, len(allowed))
	for _, col := range allowed {
		allowedSet[col] = true
	}

	for _, col := range []string{"id", "parent_id", "name"} {
		if !allowedSet[col] {
			t.Errorf("AllowedInsertColumns: expected %q to be allowed", col)
		}
	}
	for _, col := range []string{"created_at", "updated_at", "deleted_at"} {
		if allowedSet[col] {
			t.Errorf("AllowedInsertColumns: expected %q to be denied", col)
		}
	}
}

func TestAllowedUpdateColumns(t *testing.T) {
	r := NewRegistry()
	cfg := &TableConfig{
		TableName: "orders",
		foreignKeys: []FKRelation{
			{Column: "parent_id", RefTable: "parents", RefColumn: "id"},
		},
	}
	r.RegisterForTest(cfg)
	// Simulate introspection detecting all timestamp columns.
	cfg.hasUpdatedAt = true
	cfg.hasDeletedAt = true
	cfg.hasCreatedAt = true
	cfg.updatedAtColumn = "updated_at"
	cfg.deletedAtColumn = "deleted_at"
	cfg.finalizeProtectedSet()

	dataCols := []string{"id", "parent_id", "name", "created_at", "updated_at", "deleted_at"}
	allowed := cfg.AllowedUpdateColumns(dataCols)

	allowedSet := make(map[string]bool, len(allowed))
	for _, col := range allowed {
		allowedSet[col] = true
	}

	if !allowedSet["name"] {
		t.Error("AllowedUpdateColumns: expected 'name' to be allowed")
	}
	for _, col := range []string{"id", "parent_id", "created_at", "updated_at", "deleted_at"} {
		if allowedSet[col] {
			t.Errorf("AllowedUpdateColumns: expected %q to be denied", col)
		}
	}
}

func TestIsProtected(t *testing.T) {
	r := NewRegistry()
	cfg := &TableConfig{TableName: "orders"}
	r.RegisterForTest(cfg)

	// Before introspection: only id is in the set
	if !cfg.IsProtected("id") {
		t.Error("id should be protected before introspection")
	}
	if cfg.IsProtected("name") {
		t.Error("name should not be protected")
	}

	// After introspection (simulate all timestamp columns present)
	cfg.hasUpdatedAt = true
	cfg.hasDeletedAt = true
	cfg.hasCreatedAt = true
	cfg.updatedAtColumn = "updated_at"
	cfg.deletedAtColumn = "deleted_at"
	cfg.foreignKeys = []FKRelation{
		{Column: "parent_id", RefTable: "parents", RefColumn: "id"},
	}
	cfg.finalizeProtectedSet()

	postTests := []struct {
		col  string
		want bool
	}{
		{"id", true},
		{"created_at", true},
		{"updated_at", true},
		{"deleted_at", true},
		{"parent_id", true},
		{"name", false},
		{"description", false},
	}
	for _, tt := range postTests {
		t.Run("post/"+tt.col, func(t *testing.T) {
			if got := cfg.IsProtected(tt.col); got != tt.want {
				t.Errorf("IsProtected(%q) after introspection = %v, want %v", tt.col, got, tt.want)
			}
		})
	}
}

func TestTopologicalSort(t *testing.T) {
	r := NewRegistry()
	r.RegisterForTest(&TableConfig{TableName: "orders"})
	r.RegisterForTest(&TableConfig{
		TableName:   "order_details",
		parentTable: "orders",
		parentFKCol: "order_id",
	})
	r.RegisterForTest(&TableConfig{TableName: "products"})

	sorted := r.topologicalSort()
	if len(sorted) != 3 {
		t.Fatalf("expected 3 tables, got %d", len(sorted))
	}

	// orders must come before order_details.
	ordersIdx := -1
	detailsIdx := -1
	for i, name := range sorted {
		if name == "orders" {
			ordersIdx = i
		}
		if name == "order_details" {
			detailsIdx = i
		}
	}
	if ordersIdx >= detailsIdx {
		t.Errorf("orders (idx=%d) should come before order_details (idx=%d)", ordersIdx, detailsIdx)
	}
}

func TestRegisterTable(t *testing.T) {
	r := NewRegistry()
	r.registerTable(Table{Name: "orders", Exclude: []string{"col_a"}}, []string{"global_col"})

	cfg := r.Get("orders")
	if cfg == nil {
		t.Fatal("expected registered config")
	}
	if cfg.IDColumn != "id" {
		t.Errorf("IDColumn = %q, want %q", cfg.IDColumn, "id")
	}
	// Check merged excludes.
	if len(cfg.ExcludeColumns) != 2 {
		t.Errorf("ExcludeColumns = %v, want 2 entries", cfg.ExcludeColumns)
	}
}
