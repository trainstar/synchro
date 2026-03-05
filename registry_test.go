package synchro

import (
	"strings"
	"testing"
)

func TestRegister_Defaults(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{
		TableName:   "items",
		Direction:   Bidirectional,
		OwnerColumn: "user_id",
	})

	cfg := r.Get("items")
	if cfg == nil {
		t.Fatal("expected registered config, got nil")
	}
	if cfg.IDColumn != "id" {
		t.Errorf("IDColumn = %q, want %q", cfg.IDColumn, "id")
	}
	if cfg.UpdatedAtColumn != "updated_at" {
		t.Errorf("UpdatedAtColumn = %q, want %q", cfg.UpdatedAtColumn, "updated_at")
	}
	if cfg.DeletedAtColumn != "deleted_at" {
		t.Errorf("DeletedAtColumn = %q, want %q", cfg.DeletedAtColumn, "deleted_at")
	}
}

func TestRegister_CustomColumns(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{
		TableName:       "events",
		Direction:       ServerOnly,
		IDColumn:        "event_id",
		UpdatedAtColumn: "modified_at",
		DeletedAtColumn: "removed_at",
	})

	cfg := r.Get("events")
	if cfg.IDColumn != "event_id" {
		t.Errorf("IDColumn = %q, want %q", cfg.IDColumn, "event_id")
	}
	if cfg.UpdatedAtColumn != "modified_at" {
		t.Errorf("UpdatedAtColumn = %q, want %q", cfg.UpdatedAtColumn, "modified_at")
	}
	if cfg.DeletedAtColumn != "removed_at" {
		t.Errorf("DeletedAtColumn = %q, want %q", cfg.DeletedAtColumn, "removed_at")
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
		r.Register(&TableConfig{TableName: n, Direction: ServerOnly})
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
	r.Register(&TableConfig{TableName: "a", Direction: ServerOnly})
	r.Register(&TableConfig{TableName: "b", Direction: ServerOnly})

	got := r.TableNames()
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("TableNames() = %v, want [a b]", got)
	}

	// Verify returned slice is a copy (modifying it does not affect registry).
	got[0] = "modified"
	orig := r.TableNames()
	if orig[0] != "a" {
		t.Error("TableNames() returned internal slice, not a copy")
	}
}

func TestIsRegistered(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{TableName: "items", Direction: ServerOnly})

	if !r.IsRegistered("items") {
		t.Error("IsRegistered(items) = false, want true")
	}
	if r.IsRegistered("missing") {
		t.Error("IsRegistered(missing) = true, want false")
	}
}

func TestIsPushable(t *testing.T) {
	tests := []struct {
		name      string
		direction SyncDirection
		want      bool
	}{
		{"bidirectional", Bidirectional, true},
		{"server_only", ServerOnly, false},
		{"system_and_user", SystemAndUser, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRegistry()
			r.Register(&TableConfig{
				TableName:   tt.name,
				Direction:   tt.direction,
				OwnerColumn: "user_id",
			})
			if got := r.IsPushable(tt.name); got != tt.want {
				t.Errorf("IsPushable(%q) = %v, want %v", tt.name, got, tt.want)
			}
		})
	}

	// Unregistered table is not pushable.
	r := NewRegistry()
	if r.IsPushable("nope") {
		t.Error("IsPushable(unregistered) = true, want false")
	}
}

func TestBidirectionalTables(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{TableName: "a", Direction: Bidirectional, OwnerColumn: "user_id"})
	r.Register(&TableConfig{TableName: "b", Direction: ServerOnly})
	r.Register(&TableConfig{TableName: "c", Direction: Bidirectional, OwnerColumn: "user_id"})

	got := r.BidirectionalTables()
	if len(got) != 2 {
		t.Fatalf("BidirectionalTables() returned %d, want 2", len(got))
	}
	if got[0].TableName != "a" || got[1].TableName != "c" {
		t.Errorf("BidirectionalTables() = [%s, %s], want [a, c]", got[0].TableName, got[1].TableName)
	}
}

func TestServerOnlyTables(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{TableName: "a", Direction: Bidirectional, OwnerColumn: "user_id"})
	r.Register(&TableConfig{TableName: "b", Direction: ServerOnly})

	got := r.ServerOnlyTables()
	if len(got) != 1 || got[0].TableName != "b" {
		t.Errorf("ServerOnlyTables() = %v, want [b]", got)
	}
}

// --- Validate tests ---

func TestValidate_UnregisteredParent(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{
		TableName:   "child",
		Direction:   Bidirectional,
		OwnerColumn: "user_id",
		ParentTable: "missing_parent",
		ParentFKCol: "parent_id",
	})

	err := r.Validate()
	if err == nil {
		t.Fatal("expected error for unregistered parent, got nil")
	}
	if !strings.Contains(err.Error(), "unregistered parent") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_CycleDetection(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{
		TableName:   "a",
		Direction:   Bidirectional,
		OwnerColumn: "user_id",
		ParentTable: "b",
		ParentFKCol: "b_id",
	})
	r.Register(&TableConfig{
		TableName:   "b",
		Direction:   Bidirectional,
		OwnerColumn: "user_id",
		ParentTable: "a",
		ParentFKCol: "a_id",
	})

	err := r.Validate()
	if err == nil {
		t.Fatal("expected error for cycle, got nil")
	}
	if !strings.Contains(err.Error(), "cycle detected") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_OrphanedChain(t *testing.T) {
	r := NewRegistry()
	// Root table has no OwnerColumn.
	r.Register(&TableConfig{
		TableName: "root",
		Direction: ServerOnly,
	})
	r.Register(&TableConfig{
		TableName:   "child",
		Direction:   Bidirectional,
		OwnerColumn: "user_id",
		ParentTable: "root",
		ParentFKCol: "root_id",
	})

	err := r.Validate()
	if err == nil {
		t.Fatal("expected error for orphaned chain, got nil")
	}
	if !strings.Contains(err.Error(), "orphaned chain") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_PushableWithoutOwnerColumn(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{
		TableName: "items",
		Direction: Bidirectional,
		// No OwnerColumn, no ParentTable.
	})

	err := r.Validate()
	if err == nil {
		t.Fatal("expected error for pushable without OwnerColumn, got nil")
	}
	if !strings.Contains(err.Error(), "no OwnerColumn or ParentTable") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_ParentTableWithoutParentFKCol(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{
		TableName:   "parent",
		Direction:   Bidirectional,
		OwnerColumn: "user_id",
	})
	r.Register(&TableConfig{
		TableName:   "child",
		Direction:   Bidirectional,
		OwnerColumn: "user_id",
		ParentTable: "parent",
		// Missing ParentFKCol.
	})

	err := r.Validate()
	if err == nil {
		t.Fatal("expected error for ParentTable without ParentFKCol, got nil")
	}
	if !strings.Contains(err.Error(), "no ParentFKCol") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_RedundantProtectedColumn(t *testing.T) {
	tests := []struct {
		name      string
		protected []string
		errSubstr string
	}{
		{
			name:      "default protected column",
			protected: []string{"created_at"},
			errSubstr: "default protected column",
		},
		{
			name:      "PK column",
			protected: []string{"id"},
			errSubstr: "PK column",
		},
		{
			name:      "ownership column",
			protected: []string{"user_id"},
			errSubstr: "ownership column",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRegistry()
			r.Register(&TableConfig{
				TableName:        "items",
				Direction:        Bidirectional,
				OwnerColumn:      "user_id",
				ProtectedColumns: tt.protected,
			})

			err := r.Validate()
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.errSubstr) {
				t.Errorf("error %q does not contain %q", err.Error(), tt.errSubstr)
			}
		})
	}
}

func TestValidate_ValidConfig(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{
		TableName:   "workouts",
		Direction:   Bidirectional,
		OwnerColumn: "user_id",
	})
	r.Register(&TableConfig{
		TableName:   "workout_sets",
		Direction:   Bidirectional,
		OwnerColumn: "user_id",
		ParentTable: "workouts",
		ParentFKCol: "workout_id",
	})
	r.Register(&TableConfig{
		TableName: "equipment_types",
		Direction: ServerOnly,
	})

	if err := r.Validate(); err != nil {
		t.Errorf("Validate() returned unexpected error: %v", err)
	}
}

// --- AllowedInsertColumns / AllowedUpdateColumns ---

func TestAllowedInsertColumns(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{
		TableName:   "items",
		Direction:   Bidirectional,
		OwnerColumn: "user_id",
		ParentFKCol: "parent_id",
	})
	cfg := r.Get("items")

	dataCols := []string{"id", "user_id", "parent_id", "name", "created_at", "updated_at", "deleted_at"}
	allowed := cfg.AllowedInsertColumns(dataCols)

	allowedSet := make(map[string]bool, len(allowed))
	for _, col := range allowed {
		allowedSet[col] = true
	}

	// id, user_id, parent_id should be allowed on insert.
	for _, col := range []string{"id", "user_id", "parent_id", "name"} {
		if !allowedSet[col] {
			t.Errorf("AllowedInsertColumns: expected %q to be allowed", col)
		}
	}
	// Timestamps should be denied.
	for _, col := range []string{"created_at", "updated_at", "deleted_at"} {
		if allowedSet[col] {
			t.Errorf("AllowedInsertColumns: expected %q to be denied", col)
		}
	}
}

func TestAllowedUpdateColumns(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{
		TableName:        "items",
		Direction:        Bidirectional,
		OwnerColumn:      "user_id",
		ParentFKCol:      "parent_id",
		ProtectedColumns: []string{"secret"},
	})
	cfg := r.Get("items")

	dataCols := []string{"id", "user_id", "parent_id", "name", "secret", "created_at", "updated_at", "deleted_at"}
	allowed := cfg.AllowedUpdateColumns(dataCols)

	allowedSet := make(map[string]bool, len(allowed))
	for _, col := range allowed {
		allowedSet[col] = true
	}

	// Only "name" should pass through on update.
	if !allowedSet["name"] {
		t.Error("AllowedUpdateColumns: expected 'name' to be allowed")
	}
	// Everything else should be denied.
	for _, col := range []string{"id", "user_id", "parent_id", "secret", "created_at", "updated_at", "deleted_at"} {
		if allowedSet[col] {
			t.Errorf("AllowedUpdateColumns: expected %q to be denied", col)
		}
	}
}

func TestIsProtected(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{
		TableName:        "items",
		Direction:        Bidirectional,
		OwnerColumn:      "user_id",
		ProtectedColumns: []string{"internal_flag"},
	})
	cfg := r.Get("items")

	tests := []struct {
		col  string
		want bool
	}{
		{"id", true},
		{"user_id", true},
		{"created_at", true},
		{"updated_at", true},
		{"deleted_at", true},
		{"internal_flag", true},
		{"name", false},
		{"description", false},
	}
	for _, tt := range tests {
		t.Run(tt.col, func(t *testing.T) {
			if got := cfg.IsProtected(tt.col); got != tt.want {
				t.Errorf("IsProtected(%q) = %v, want %v", tt.col, got, tt.want)
			}
		})
	}
}
