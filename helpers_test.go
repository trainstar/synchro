package synchro

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

// ---------------------------------------------------------------------------
// UserBucket
// ---------------------------------------------------------------------------

func TestUserBucket_Assign_WithValue(t *testing.T) {
	bf := UserBucket("user_id")

	buckets := bf("workouts", "create", map[string]any{
		"user_id": "abc-123",
		"name":    "Push Day",
	})

	if len(buckets) != 1 {
		t.Fatalf("expected 1 bucket, got %d: %v", len(buckets), buckets)
	}
	if buckets[0] != "user:abc-123" {
		t.Errorf("bucket = %q, want %q", buckets[0], "user:abc-123")
	}
}

func TestUserBucket_Assign_NilValue(t *testing.T) {
	bf := UserBucket("user_id")

	buckets := bf("exercises", "create", map[string]any{
		"user_id": nil,
		"name":    "Bench Press",
	})

	if len(buckets) != 1 {
		t.Fatalf("expected 1 bucket, got %d: %v", len(buckets), buckets)
	}
	if buckets[0] != "global" {
		t.Errorf("bucket = %q, want %q", buckets[0], "global")
	}
}

func TestUserBucket_Assign_MissingColumn(t *testing.T) {
	bf := UserBucket("user_id")

	// When the ownership column is not present in the data at all (e.g. a
	// child table that doesn't have user_id), BucketFunc returns nil to
	// signal "I can't determine ownership". allowing the caller to fall
	// back to FK chain resolution.
	buckets := bf("exercises", "create", map[string]any{
		"name": "Squat",
	})

	if len(buckets) != 0 {
		t.Fatalf("expected 0 buckets (nil), got %d: %v", len(buckets), buckets)
	}
}

func TestUserBucket_Assign_EmptyString(t *testing.T) {
	bf := UserBucket("user_id")

	buckets := bf("exercises", "create", map[string]any{
		"user_id": "",
		"name":    "Deadlift",
	})

	if len(buckets) != 1 {
		t.Fatalf("expected 1 bucket, got %d: %v", len(buckets), buckets)
	}
	if buckets[0] != "global" {
		t.Errorf("bucket = %q, want %q", buckets[0], "global")
	}
}

// ---------------------------------------------------------------------------
// StampColumn
// ---------------------------------------------------------------------------

func TestStampColumn_Create(t *testing.T) {
	ctx := context.Background()
	userID := "user-abc-123"
	stamp := StampColumn("user_id")

	data := mustMarshal(t, map[string]any{
		"id":   "rec-1",
		"name": "Push Day",
	})

	record := &PushRecord{
		ID:        "rec-1",
		TableName: "workouts",
		Operation: "create",
		Data:      data,
	}

	got, err := stamp(ctx, userID, record)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal(got.Data, &parsed); err != nil {
		t.Fatalf("failed to unmarshal result data: %v", err)
	}

	if parsed["user_id"] != userID {
		t.Errorf("user_id = %v, want %q", parsed["user_id"], userID)
	}
}

func TestStampColumn_Update(t *testing.T) {
	ctx := context.Background()
	userID := "user-abc-123"
	stamp := StampColumn("user_id")

	data := mustMarshal(t, map[string]any{
		"id":      "rec-1",
		"name":    "Pull Day",
		"user_id": "someone-else",
	})

	record := &PushRecord{
		ID:        "rec-1",
		TableName: "workouts",
		Operation: "update",
		Data:      data,
	}

	got, err := stamp(ctx, userID, record)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal(got.Data, &parsed); err != nil {
		t.Fatalf("failed to unmarshal result data: %v", err)
	}

	if _, exists := parsed["user_id"]; exists {
		t.Errorf("user_id should be removed on update, but found %v", parsed["user_id"])
	}
}

func TestStampColumn_Delete(t *testing.T) {
	ctx := context.Background()
	userID := "user-abc-123"
	stamp := StampColumn("user_id")

	data := mustMarshal(t, map[string]any{
		"id": "rec-1",
	})

	record := &PushRecord{
		ID:        "rec-1",
		TableName: "workouts",
		Operation: "delete",
		Data:      data,
	}

	got, err := stamp(ctx, userID, record)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Delete passes through unchanged; record pointer should be returned as-is.
	if got != record {
		t.Error("expected same record pointer on delete passthrough")
	}
}

// ---------------------------------------------------------------------------
// VerifyOwner
// ---------------------------------------------------------------------------

func TestVerifyOwner_Create_Passthrough(t *testing.T) {
	ctx := context.Background()
	userID := "user-abc-123"
	verify := VerifyOwner("user_id")

	record := &PushRecord{
		ID:        "rec-1",
		TableName: "workouts",
		Operation: "create",
		Data:      mustMarshal(t, map[string]any{"name": "Leg Day"}),
	}

	got, err := verify(ctx, userID, record)
	if err != nil {
		t.Fatalf("unexpected error on create: %v", err)
	}
	if got != record {
		t.Error("expected same record pointer on create passthrough")
	}
}

func TestVerifyOwner_Update_MatchingOwner(t *testing.T) {
	ctx := context.Background()
	userID := "user-abc-123"
	verify := VerifyOwner("user_id")

	record := &PushRecord{
		ID:        "rec-1",
		TableName: "workouts",
		Operation: "update",
		Data:      mustMarshal(t, map[string]any{"name": "Updated"}),
		Existing:  map[string]any{"user_id": "user-abc-123"},
	}

	got, err := verify(ctx, userID, record)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != record {
		t.Error("expected same record pointer on matching owner")
	}
}

func TestVerifyOwner_Update_MismatchedOwner(t *testing.T) {
	ctx := context.Background()
	userID := "user-abc-123"
	verify := VerifyOwner("user_id")

	record := &PushRecord{
		ID:        "rec-1",
		TableName: "workouts",
		Operation: "update",
		Data:      mustMarshal(t, map[string]any{"name": "Hijacked"}),
		Existing:  map[string]any{"user_id": "other-user-999"},
	}

	_, err := verify(ctx, userID, record)
	if err == nil {
		t.Fatal("expected ErrOwnershipViolation, got nil")
	}
	if !errors.Is(err, ErrOwnershipViolation) {
		t.Errorf("error = %v, want ErrOwnershipViolation", err)
	}
}

func TestVerifyOwner_Update_NilExisting(t *testing.T) {
	ctx := context.Background()
	userID := "user-abc-123"
	verify := VerifyOwner("user_id")

	record := &PushRecord{
		ID:        "rec-1",
		TableName: "workouts",
		Operation: "update",
		Data:      mustMarshal(t, map[string]any{"name": "Ghost"}),
		Existing:  nil,
	}

	_, err := verify(ctx, userID, record)
	if err == nil {
		t.Fatal("expected error when Existing is nil, got nil")
	}
}

func TestVerifyOwner_Update_NullOwner(t *testing.T) {
	ctx := context.Background()
	userID := "user-abc-123"
	verify := VerifyOwner("user_id")

	record := &PushRecord{
		ID:        "rec-1",
		TableName: "exercises",
		Operation: "update",
		Data:      mustMarshal(t, map[string]any{"name": "Global Exercise"}),
		Existing:  map[string]any{"user_id": nil},
	}

	got, err := verify(ctx, userID, record)
	if err != nil {
		t.Fatalf("expected nil error for global record (null owner), got: %v", err)
	}
	if got != record {
		t.Error("expected same record pointer for global record passthrough")
	}
}

// ---------------------------------------------------------------------------
// ReadOnly
// ---------------------------------------------------------------------------

func TestReadOnly_BlockedTable(t *testing.T) {
	ctx := context.Background()
	userID := "user-abc-123"
	guard := ReadOnly("units", "muscles", "equipment_types")

	record := &PushRecord{
		ID:        "unit-1",
		TableName: "units",
		Operation: "create",
		Data:      mustMarshal(t, map[string]any{"name": "kg"}),
	}

	_, err := guard(ctx, userID, record)
	if err == nil {
		t.Fatal("expected ErrTableReadOnly, got nil")
	}
	if !errors.Is(err, ErrTableReadOnly) {
		t.Errorf("error = %v, want ErrTableReadOnly", err)
	}
}

func TestReadOnly_AllowedTable(t *testing.T) {
	ctx := context.Background()
	userID := "user-abc-123"
	guard := ReadOnly("units", "muscles", "equipment_types")

	record := &PushRecord{
		ID:        "w-1",
		TableName: "workouts",
		Operation: "create",
		Data:      mustMarshal(t, map[string]any{"name": "Push Day"}),
	}

	got, err := guard(ctx, userID, record)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != record {
		t.Error("expected same record pointer for allowed table")
	}
}

// ---------------------------------------------------------------------------
// Chain
// ---------------------------------------------------------------------------

func TestChain_AllPass(t *testing.T) {
	ctx := context.Background()
	userID := "user-abc-123"

	passA := func(_ context.Context, _ string, r *PushRecord) (*PushRecord, error) {
		return r, nil
	}
	passB := func(_ context.Context, _ string, r *PushRecord) (*PushRecord, error) {
		return r, nil
	}

	chained := Chain(passA, passB)

	record := &PushRecord{
		ID:        "rec-1",
		TableName: "workouts",
		Operation: "create",
		Data:      mustMarshal(t, map[string]any{"name": "Test"}),
	}

	got, err := chained(ctx, userID, record)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != record {
		t.Error("expected same record pointer when all pass")
	}
}

func TestChain_FirstFails(t *testing.T) {
	ctx := context.Background()
	userID := "user-abc-123"

	sentinel := errors.New("first failed")

	failFirst := func(_ context.Context, _ string, _ *PushRecord) (*PushRecord, error) {
		return nil, sentinel
	}
	shouldNotRun := func(_ context.Context, _ string, _ *PushRecord) (*PushRecord, error) {
		t.Fatal("second function should not be called")
		return nil, nil
	}

	chained := Chain(failFirst, shouldNotRun)

	record := &PushRecord{
		ID:        "rec-1",
		TableName: "workouts",
		Operation: "create",
		Data:      mustMarshal(t, map[string]any{"name": "Test"}),
	}

	_, err := chained(ctx, userID, record)
	if !errors.Is(err, sentinel) {
		t.Errorf("error = %v, want %v", err, sentinel)
	}
}

func TestChain_TransformsChain(t *testing.T) {
	ctx := context.Background()
	userID := "user-abc-123"

	// First function replaces the record with a new one.
	replacement := &PushRecord{
		ID:        "rec-replaced",
		TableName: "workouts",
		Operation: "create",
		Data:      mustMarshal(t, map[string]any{"name": "Replaced"}),
	}

	transformFirst := func(_ context.Context, _ string, _ *PushRecord) (*PushRecord, error) {
		return replacement, nil
	}

	// Second function verifies it receives the replacement.
	var received *PushRecord
	captureSecond := func(_ context.Context, _ string, r *PushRecord) (*PushRecord, error) {
		received = r
		return r, nil
	}

	chained := Chain(transformFirst, captureSecond)

	original := &PushRecord{
		ID:        "rec-original",
		TableName: "workouts",
		Operation: "create",
		Data:      mustMarshal(t, map[string]any{"name": "Original"}),
	}

	got, err := chained(ctx, userID, original)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if received != replacement {
		t.Error("second function did not receive the transformed record from the first")
	}
	if got != replacement {
		t.Error("final result should be the replacement record")
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func mustMarshal(t *testing.T, v map[string]any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}
	return b
}
