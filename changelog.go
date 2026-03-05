package synchro

import (
	"context"
	"fmt"
	"time"
)

// ChangelogEntry represents a single entry in the sync changelog.
type ChangelogEntry struct {
	Seq       int64     `json:"seq"`
	BucketID  string    `json:"bucket_id"`
	TableName string    `json:"table_name"`
	RecordID  string    `json:"record_id"`
	Operation Operation `json:"operation"`
	CreatedAt time.Time `json:"created_at"`
}

// changelogStore handles changelog read/write operations.
type changelogStore struct{}

// WriteEntry writes a single changelog entry and returns the assigned sequence number.
func (s *changelogStore) WriteEntry(ctx context.Context, db DB, bucketID, table, recordID string, op Operation) (int64, error) {
	query := `
		INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation)
		VALUES ($1, $2, $3, $4)
		RETURNING seq
	`

	var seq int64
	err := db.QueryRowContext(ctx, query, bucketID, table, recordID, int(op)).Scan(&seq)
	if err != nil {
		return 0, fmt.Errorf("writing changelog entry: %w", err)
	}
	return seq, nil
}

// WriteBatch writes multiple changelog entries in a single query.
func (s *changelogStore) WriteBatch(ctx context.Context, db DB, entries []ChangelogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	// Build a multi-value INSERT
	query := "INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES "
	values := make([]any, 0, len(entries)*4)

	for i, entry := range entries {
		if i > 0 {
			query += ", "
		}
		base := i * 4
		query += fmt.Sprintf("($%d, $%d, $%d, $%d)", base+1, base+2, base+3, base+4)
		values = append(values, entry.BucketID, entry.TableName, entry.RecordID, int(entry.Operation))
	}

	_, err := db.ExecContext(ctx, query, values...)
	if err != nil {
		return fmt.Errorf("writing changelog batch: %w", err)
	}
	return nil
}

// QueryAfter returns changelog entries for the given buckets after the given sequence.
// If tables is non-empty, only entries for those tables are returned.
func (s *changelogStore) QueryAfter(ctx context.Context, db DB, bucketIDs []string, afterSeq int64, limit int, tables []string) ([]ChangelogEntry, error) {
	if len(bucketIDs) == 0 {
		return nil, nil
	}

	// Expand slice into individual placeholders for database/sql compatibility.
	var args []any
	bucketList, args, nextIdx := expandSlicePlaceholder(bucketIDs, 1, args)

	tableFilter := ""
	if len(tables) > 0 {
		tableList, newArgs, newIdx := expandSlicePlaceholder(tables, nextIdx, args)
		args = newArgs
		nextIdx = newIdx
		tableFilter = fmt.Sprintf(" AND table_name IN %s", tableList)
	}

	query := fmt.Sprintf(`
		SELECT seq, bucket_id, table_name, record_id, operation, created_at
		FROM sync_changelog
		WHERE bucket_id IN %s AND seq > $%d%s
		ORDER BY seq
		LIMIT $%d
	`, bucketList, nextIdx, tableFilter, nextIdx+1)

	args = append(args, afterSeq, limit)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying changelog: %w", err)
	}
	defer rows.Close()

	var entries []ChangelogEntry
	for rows.Next() {
		var e ChangelogEntry
		var opInt int
		if err := rows.Scan(&e.Seq, &e.BucketID, &e.TableName, &e.RecordID, &opInt, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("scanning changelog entry: %w", err)
		}
		e.Operation = Operation(opInt)
		entries = append(entries, e)
	}

	return entries, rows.Err()
}
