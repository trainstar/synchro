package synchro

import (
	"context"
	"testing"
)

func TestJoinResolver_GlobalTable(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{
		TableName:  "products",
		PushPolicy: PushPolicyDisabled,
	})

	resolver := NewJoinResolver(r)
	buckets, err := resolver.ResolveOwner(context.Background(), nil, "products", "prod-1", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(buckets) != 1 || buckets[0] != "global" {
		t.Errorf("buckets = %v, want [global]", buckets)
	}
}

func TestJoinResolver_BucketByColumn(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{
		TableName:      "orders",
		PushPolicy:     PushPolicyOwnerOnly,
		BucketByColumn: "owner_id",
		BucketPrefix:   "user:",
	})

	resolver := NewJoinResolver(r)
	data := map[string]any{"owner_id": "user-abc"}
	buckets, err := resolver.ResolveOwner(context.Background(), nil, "orders", "order-1", data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(buckets) != 1 || buckets[0] != "user:user-abc" {
		t.Errorf("buckets = %v, want [user:user-abc]", buckets)
	}
}

func TestJoinResolver_BucketByColumn_NullNoGlobal(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{
		TableName:      "orders",
		PushPolicy:     PushPolicyOwnerOnly,
		BucketByColumn: "owner_id",
		BucketPrefix:   "user:",
	})

	resolver := NewJoinResolver(r)
	buckets, err := resolver.ResolveOwner(context.Background(), nil, "orders", "order-1", map[string]any{"owner_id": nil})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(buckets) != 0 {
		t.Errorf("buckets = %v, want []", buckets)
	}
}

func TestJoinResolver_BucketByColumn_NullGlobalOptIn(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{
		TableName:            "orders",
		PushPolicy:           PushPolicyOwnerOnly,
		BucketByColumn:       "owner_id",
		BucketPrefix:         "user:",
		GlobalWhenBucketNull: true,
		AllowGlobalRead:      true,
	})

	resolver := NewJoinResolver(r)
	buckets, err := resolver.ResolveOwner(context.Background(), nil, "orders", "order-1", map[string]any{"owner_id": nil})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(buckets) != 1 || buckets[0] != "global" {
		t.Errorf("buckets = %v, want [global]", buckets)
	}
}

// bucketAssigner mirrors wal.BucketAssigner for compile-time conformance
// checking without introducing a circular import.
type bucketAssigner interface {
	AssignBuckets(ctx context.Context, table string, recordID string, operation Operation, data map[string]any) ([]string, error)
}

var _ bucketAssigner = (*JoinResolver)(nil)

func TestJoinResolver_AssignBuckets_SatisfiesBucketAssigner(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{
		TableName:      "orders",
		PushPolicy:     PushPolicyOwnerOnly,
		BucketByColumn: "owner_id",
		BucketPrefix:   "user:",
	})

	// Without DB, AssignBuckets should return an error.
	resolverNoDB := NewJoinResolver(r)
	_, err := resolverNoDB.AssignBuckets(context.Background(), "orders", "order-1", OpInsert, map[string]any{"owner_id": "u1"})
	if err == nil {
		t.Fatal("expected error when db is nil, got nil")
	}
}

func TestJoinResolver_UnregisteredTable(t *testing.T) {
	r := NewRegistry()
	resolver := NewJoinResolver(r)

	_, err := resolver.ResolveOwner(context.Background(), nil, "nonexistent", "id-1", nil)
	if err == nil {
		t.Fatal("expected error for unregistered table, got nil")
	}
}
