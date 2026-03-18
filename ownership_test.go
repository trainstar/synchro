package synchro

import (
	"context"
	"testing"
)

func TestJoinResolver_GlobalTable(t *testing.T) {
	r := NewRegistry()
	r.RegisterForTest(&TableConfig{
		TableName: "products",
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

// bucketAssigner mirrors wal.BucketAssigner for compile-time conformance
// checking without introducing a circular import.
type bucketAssigner interface {
	AssignBuckets(ctx context.Context, table string, recordID string, operation Operation, data map[string]any) ([]string, error)
}

var _ bucketAssigner = (*JoinResolver)(nil)

func TestJoinResolver_AssignBuckets_RequiresDB(t *testing.T) {
	r := NewRegistry()
	r.RegisterForTest(&TableConfig{TableName: "orders"})

	// Without DB, AssignBuckets should return an error.
	resolverNoDB := NewJoinResolver(r)
	_, err := resolverNoDB.AssignBuckets(context.Background(), "orders", "order-1", OpInsert, map[string]any{})
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
