package synchro

import (
	"errors"
	"testing"
)

func TestRegister_Defaults(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{
		TableName:   "orders",
		OwnerColumn: "user_id",
	})

	cfg := r.Get("orders")
	if cfg == nil {
		t.Fatal("expected registered config, got nil")
	}
	if cfg.IDColumn != "id" {
		t.Errorf("IDColumn = %q, want %q", cfg.IDColumn, "id")
	}
	// UpdatedAtColumn/DeletedAtColumn are now computed by introspection,
	// so they start empty until NewEngine runs.
	if cfg.UpdatedAtCol() != "" {
		t.Errorf("UpdatedAtCol() = %q, want empty before introspection", cfg.UpdatedAtCol())
	}
	if cfg.DeletedAtCol() != "" {
		t.Errorf("DeletedAtCol() = %q, want empty before introspection", cfg.DeletedAtCol())
	}
	if cfg.PushPolicy != PushPolicyOwnerOnly {
		t.Errorf("PushPolicy = %q, want %q", cfg.PushPolicy, PushPolicyOwnerOnly)
	}
	if cfg.BucketByColumn != "user_id" {
		t.Errorf("BucketByColumn = %q, want %q", cfg.BucketByColumn, "user_id")
	}
	if cfg.BucketPrefix != "user:" {
		t.Errorf("BucketPrefix = %q, want %q", cfg.BucketPrefix, "user:")
	}
}

func TestRegister_CustomIDColumn(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{
		TableName:  "events",
		PushPolicy: PushPolicyDisabled,
		IDColumn:   "event_id",
	})

	cfg := r.Get("events")
	if cfg.IDColumn != "event_id" {
		t.Errorf("IDColumn = %q, want %q", cfg.IDColumn, "event_id")
	}
	// UpdatedAtColumn/DeletedAtColumn are set by introspection via Config-level names,
	// not per-table registration.
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
		r.Register(&TableConfig{TableName: n})
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
	r.Register(&TableConfig{TableName: "a"})
	r.Register(&TableConfig{TableName: "b"})

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
	r.Register(&TableConfig{TableName: "orders"})

	if !r.IsRegistered("orders") {
		t.Error("IsRegistered(orders) = false, want true")
	}
	if r.IsRegistered("missing") {
		t.Error("IsRegistered(missing) = true, want false")
	}
}

func TestIsPushable(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{
		TableName:   "owned",
		PushPolicy:  PushPolicyOwnerOnly,
		OwnerColumn: "user_id",
	})
	r.Register(&TableConfig{
		TableName:  "ref",
		PushPolicy: PushPolicyDisabled,
	})
	if !r.IsPushable("owned") {
		t.Error("IsPushable(owned) = false, want true")
	}
	if r.IsPushable("ref") {
		t.Error("IsPushable(ref) = true, want false")
	}
	if r.IsPushable("missing") {
		t.Error("IsPushable(missing) = true, want false")
	}
}

func TestValidate_UnregisteredParent(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{
		TableName:   "child",
		PushPolicy:  PushPolicyOwnerOnly,
		OwnerColumn: "user_id",
		ParentTable: "missing_parent",
		ParentFKCol: "parent_id",
	})

	err := r.Validate()
	if err == nil {
		t.Fatal("expected error for unregistered parent, got nil")
	}
	if !errors.Is(err, ErrUnregisteredParent) {
		t.Errorf("expected ErrUnregisteredParent, got: %v", err)
	}
}

func TestValidate_CycleDetection(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{
		TableName:   "a",
		PushPolicy:  PushPolicyOwnerOnly,
		OwnerColumn: "user_id",
		ParentTable: "b",
		ParentFKCol: "b_id",
	})
	r.Register(&TableConfig{
		TableName:   "b",
		PushPolicy:  PushPolicyOwnerOnly,
		OwnerColumn: "user_id",
		ParentTable: "a",
		ParentFKCol: "a_id",
	})

	err := r.Validate()
	if err == nil {
		t.Fatal("expected error for cycle, got nil")
	}
	if !errors.Is(err, ErrCycleDetected) {
		t.Errorf("expected ErrCycleDetected, got: %v", err)
	}
}

func TestValidate_OrphanedChain(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{
		TableName:  "root",
		PushPolicy: PushPolicyDisabled,
	})
	r.Register(&TableConfig{
		TableName:   "child",
		PushPolicy:  PushPolicyOwnerOnly,
		OwnerColumn: "user_id",
		ParentTable: "root",
		ParentFKCol: "root_id",
	})

	err := r.Validate()
	if err == nil {
		t.Fatal("expected error for orphaned chain, got nil")
	}
	if !errors.Is(err, ErrOrphanedChain) {
		t.Errorf("expected ErrOrphanedChain, got: %v", err)
	}
}

func TestValidate_PushableWithoutOwnerColumn(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{
		TableName:  "orders",
		PushPolicy: PushPolicyOwnerOnly,
	})

	err := r.Validate()
	if err == nil {
		t.Fatal("expected error for pushable without OwnerColumn, got nil")
	}
	if !errors.Is(err, ErrMissingOwnership) {
		t.Errorf("expected ErrMissingOwnership, got: %v", err)
	}
}

func TestValidate_ParentTableWithoutParentFKCol(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{
		TableName:   "parent",
		PushPolicy:  PushPolicyOwnerOnly,
		OwnerColumn: "user_id",
	})
	r.Register(&TableConfig{
		TableName:   "child",
		PushPolicy:  PushPolicyOwnerOnly,
		OwnerColumn: "user_id",
		ParentTable: "parent",
	})

	err := r.Validate()
	if err == nil {
		t.Fatal("expected error for ParentTable without ParentFKCol, got nil")
	}
	if !errors.Is(err, ErrMissingParentFKCol) {
		t.Errorf("expected ErrMissingParentFKCol, got: %v", err)
	}
}

func TestValidate_RedundantProtectedColumn(t *testing.T) {
	tests := []struct {
		name      string
		protected []string
	}{
		{name: "PK column", protected: []string{"id"}},
		{name: "ownership column", protected: []string{"user_id"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRegistry()
			r.Register(&TableConfig{
				TableName:        "orders",
				PushPolicy:       PushPolicyOwnerOnly,
				OwnerColumn:      "user_id",
				ProtectedColumns: tt.protected,
			})

			err := r.Validate()
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !errors.Is(err, ErrRedundantProtected) {
				t.Errorf("expected ErrRedundantProtected, got: %v", err)
			}
		})
	}
}

func TestValidate_InvalidPushPolicy(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{
		TableName:   "orders",
		PushPolicy:  PushPolicy("invalid"),
		OwnerColumn: "user_id",
	})
	err := r.Validate()
	if err == nil || !errors.Is(err, ErrInvalidPushPolicy) {
		t.Fatalf("expected ErrInvalidPushPolicy, got: %v", err)
	}
}

func TestValidate_InvalidBucketConfig(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{
		TableName:            "orders",
		PushPolicy:           PushPolicyOwnerOnly,
		OwnerColumn:          "user_id",
		GlobalWhenBucketNull: true,
		AllowGlobalRead:      false,
	})
	err := r.Validate()
	if err == nil || !errors.Is(err, ErrInvalidBucketConfig) {
		t.Fatalf("expected ErrInvalidBucketConfig, got: %v", err)
	}
}

func TestValidate_ValidConfig(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{
		TableName:   "orders",
		PushPolicy:  PushPolicyOwnerOnly,
		OwnerColumn: "user_id",
	})
	r.Register(&TableConfig{
		TableName:   "order_details",
		PushPolicy:  PushPolicyOwnerOnly,
		OwnerColumn: "user_id",
		ParentTable: "orders",
		ParentFKCol: "order_id",
	})
	r.Register(&TableConfig{
		TableName:  "products",
		PushPolicy: PushPolicyDisabled,
	})

	if err := r.Validate(); err != nil {
		t.Errorf("Validate() returned unexpected error: %v", err)
	}
}

func TestAllowedInsertColumns(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{
		TableName:   "orders",
		PushPolicy:  PushPolicyOwnerOnly,
		OwnerColumn: "user_id",
		ParentFKCol: "parent_id",
	})
	cfg := r.Get("orders")
	// Simulate introspection detecting all timestamp columns.
	cfg.hasUpdatedAt = true
	cfg.hasDeletedAt = true
	cfg.hasCreatedAt = true
	cfg.updatedAtColumn = "updated_at"
	cfg.deletedAtColumn = "deleted_at"
	cfg.finalizeProtectedSet()

	dataCols := []string{"id", "user_id", "parent_id", "name", "created_at", "updated_at", "deleted_at"}
	allowed := cfg.AllowedInsertColumns(dataCols)

	allowedSet := make(map[string]bool, len(allowed))
	for _, col := range allowed {
		allowedSet[col] = true
	}

	for _, col := range []string{"id", "user_id", "parent_id", "name"} {
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
	r.Register(&TableConfig{
		TableName:        "orders",
		PushPolicy:       PushPolicyOwnerOnly,
		OwnerColumn:      "user_id",
		ParentFKCol:      "parent_id",
		ProtectedColumns: []string{"secret"},
	})
	cfg := r.Get("orders")
	// Simulate introspection detecting all timestamp columns.
	cfg.hasUpdatedAt = true
	cfg.hasDeletedAt = true
	cfg.hasCreatedAt = true
	cfg.updatedAtColumn = "updated_at"
	cfg.deletedAtColumn = "deleted_at"
	cfg.finalizeProtectedSet()

	dataCols := []string{"id", "user_id", "parent_id", "name", "secret", "created_at", "updated_at", "deleted_at"}
	allowed := cfg.AllowedUpdateColumns(dataCols)

	allowedSet := make(map[string]bool, len(allowed))
	for _, col := range allowed {
		allowedSet[col] = true
	}

	if !allowedSet["name"] {
		t.Error("AllowedUpdateColumns: expected 'name' to be allowed")
	}
	for _, col := range []string{"id", "user_id", "parent_id", "secret", "created_at", "updated_at", "deleted_at"} {
		if allowedSet[col] {
			t.Errorf("AllowedUpdateColumns: expected %q to be denied", col)
		}
	}
}

func TestIsProtected(t *testing.T) {
	// Test before introspection (no timestamp columns in protected set)
	r := NewRegistry()
	r.Register(&TableConfig{
		TableName:        "orders",
		PushPolicy:       PushPolicyOwnerOnly,
		OwnerColumn:      "user_id",
		ProtectedColumns: []string{"internal_flag"},
	})
	cfg := r.Get("orders")

	// Before introspection: only id, owner, extra protected are in the set
	preTests := []struct {
		col  string
		want bool
	}{
		{"id", true},
		{"user_id", true},
		{"created_at", false}, // not in set until introspection
		{"updated_at", false},
		{"deleted_at", false},
		{"internal_flag", true},
		{"name", false},
	}
	for _, tt := range preTests {
		t.Run("pre/"+tt.col, func(t *testing.T) {
			if got := cfg.IsProtected(tt.col); got != tt.want {
				t.Errorf("IsProtected(%q) before introspection = %v, want %v", tt.col, got, tt.want)
			}
		})
	}

	// After introspection (simulate all timestamp columns present)
	cfg.hasUpdatedAt = true
	cfg.hasDeletedAt = true
	cfg.hasCreatedAt = true
	cfg.updatedAtColumn = "updated_at"
	cfg.deletedAtColumn = "deleted_at"
	cfg.finalizeProtectedSet()

	postTests := []struct {
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
	for _, tt := range postTests {
		t.Run("post/"+tt.col, func(t *testing.T) {
			if got := cfg.IsProtected(tt.col); got != tt.want {
				t.Errorf("IsProtected(%q) after introspection = %v, want %v", tt.col, got, tt.want)
			}
		})
	}
}
