package synchro

import (
	"context"
	"encoding/json"
	"fmt"
)

// AuthorizeWriteFunc authorizes and optionally transforms a push record.
// Return nil error to allow, ErrTableReadOnly to reject read-only tables,
// ErrOwnershipViolation to reject ownership failures.
// The record's Existing field is populated for UPDATE/DELETE operations.
type AuthorizeWriteFunc func(ctx context.Context, userID string, record *PushRecord) (*PushRecord, error)

// BucketFunc determines which buckets a record belongs to.
// It receives the table name, operation string, and record data,
// and returns the bucket IDs the record should be placed in.
type BucketFunc func(table string, op string, data map[string]any) []string

// UserBucket creates a BucketFunc that assigns buckets based on a user-ownership
// column. Records where the column is non-nil and non-empty are placed in
// "user:<value>"; records where the column is nil or empty are placed in
// "global".
func UserBucket(column string) BucketFunc {
	return func(table string, op string, data map[string]any) []string {
		v, ok := data[column]
		if !ok {
			// Column not present in data. This table doesn't have the
			// ownership column. Return nil to let the caller fall back
			// to FK chain resolution.
			return nil
		}
		if v == nil {
			return []string{"global"}
		}
		s := fmt.Sprintf("%v", v)
		if s == "" {
			return []string{"global"}
		}
		return []string{"user:" + s}
	}
}

// StampColumn creates an AuthorizeWriteFunc that stamps the given column with
// the authenticated user's ID on INSERT and strips it on UPDATE (since
// ownership is immutable after creation). DELETE operations pass through
// unchanged.
func StampColumn(column string) AuthorizeWriteFunc {
	return func(ctx context.Context, userID string, record *PushRecord) (*PushRecord, error) {
		switch record.Operation {
		case "create":
			var data map[string]any
			if err := json.Unmarshal(record.Data, &data); err != nil {
				return nil, fmt.Errorf("StampColumn: unmarshal data: %w", err)
			}
			data[column] = userID
			raw, err := json.Marshal(data)
			if err != nil {
				return nil, fmt.Errorf("StampColumn: marshal data: %w", err)
			}
			record.Data = raw
			return record, nil

		case "update":
			var data map[string]any
			if err := json.Unmarshal(record.Data, &data); err != nil {
				return nil, fmt.Errorf("StampColumn: unmarshal data: %w", err)
			}
			delete(data, column)
			raw, err := json.Marshal(data)
			if err != nil {
				return nil, fmt.Errorf("StampColumn: marshal data: %w", err)
			}
			record.Data = raw
			return record, nil

		default:
			return record, nil
		}
	}
}

// VerifyOwner creates an AuthorizeWriteFunc that checks record ownership on
// UPDATE and DELETE operations. It compares the named column in
// record.Existing against the authenticated userID. INSERT operations pass
// through (no existing record to verify). Records where the ownership column
// is nil (global/system records) are allowed through without restriction.
func VerifyOwner(column string) AuthorizeWriteFunc {
	return func(ctx context.Context, userID string, record *PushRecord) (*PushRecord, error) {
		if record.Operation == "create" {
			return record, nil
		}

		if record.Existing == nil {
			return nil, fmt.Errorf("VerifyOwner: existing data required for %s on %s/%s",
				record.Operation, record.TableName, record.ID)
		}

		ownerVal, ok := record.Existing[column]
		if !ok || ownerVal == nil {
			// No owner set (global/system record), allow the operation.
			return record, nil
		}

		owner := fmt.Sprintf("%v", ownerVal)
		if owner == "" {
			// Empty string treated as no owner.
			return record, nil
		}

		if owner != userID {
			return nil, ErrOwnershipViolation
		}

		return record, nil
	}
}

// ReadOnly creates an AuthorizeWriteFunc that rejects pushes to the specified
// tables with ErrTableReadOnly. Tables not in the set pass through unchanged.
func ReadOnly(tables ...string) AuthorizeWriteFunc {
	set := make(map[string]struct{}, len(tables))
	for _, t := range tables {
		set[t] = struct{}{}
	}
	return func(ctx context.Context, userID string, record *PushRecord) (*PushRecord, error) {
		if _, blocked := set[record.TableName]; blocked {
			return nil, ErrTableReadOnly
		}
		return record, nil
	}
}

// Chain composes multiple AuthorizeWriteFunc into a single function that calls
// each in order. If any function returns an error, the chain stops and returns
// that error. Each function receives the record returned by the previous one.
func Chain(fns ...AuthorizeWriteFunc) AuthorizeWriteFunc {
	return func(ctx context.Context, userID string, record *PushRecord) (*PushRecord, error) {
		var err error
		for _, fn := range fns {
			record, err = fn(ctx, userID, record)
			if err != nil {
				return nil, err
			}
		}
		return record, nil
	}
}
