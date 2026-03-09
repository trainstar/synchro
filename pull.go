package synchro

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
)

// pullProcessor handles pull operations.
type pullProcessor struct {
	registry   *Registry
	changelog  *changelogStore
	checkpoint *checkpointStore
	logger     *slog.Logger
}

// processPull retrieves changes for a client based on their checkpoint.
func (p *pullProcessor) processPull(ctx context.Context, db DB, req *PullRequest, bucketIDs []string) (*PullResponse, error) {
	checkpoint := req.Checkpoint
	limit := req.Limit
	if limit <= 0 {
		limit = DefaultPullLimit
	}
	if limit > MaxPullLimit {
		limit = MaxPullLimit
	}

	// Query changelog for entries after the client's checkpoint
	entries, err := p.changelog.QueryAfter(ctx, db, bucketIDs, checkpoint, limit+1, req.Tables)
	if err != nil {
		return nil, fmt.Errorf("querying changelog: %w", err)
	}

	hasMore := len(entries) > limit
	if hasMore {
		entries = entries[:limit]
	}

	if len(entries) == 0 {
		return &PullResponse{
			Changes:    []Record{},
			Deletes:    []DeleteEntry{},
			Checkpoint: checkpoint,
			HasMore:    false,
		}, nil
	}

	// Deduplicate: for the same record, keep only the latest entry
	refs := deduplicateEntries(entries)

	// Separate deletes from changes, group changes by table
	deletes := make([]DeleteEntry, 0)
	changesByTable := make(map[string][]string) // table → record IDs

	for _, ref := range refs {
		if ref.Operation == OpDelete {
			deletes = append(deletes, DeleteEntry{
				ID:        ref.RecordID,
				TableName: ref.TableName,
			})
		} else {
			changesByTable[ref.TableName] = append(changesByTable[ref.TableName], ref.RecordID)
		}
	}

	// Hydrate changed records by batch-fetching per table
	changes := make([]Record, 0)
	for tableName, ids := range changesByTable {
		records, err := p.hydrateRecords(ctx, db, tableName, ids)
		if err != nil {
			p.logger.ErrorContext(ctx, "failed to hydrate records",
				"err", err, "table", tableName, "count", len(ids))
			continue
		}
		changes = append(changes, records...)
	}

	// New checkpoint is the max seq from the entries we processed
	newCheckpoint := entries[len(entries)-1].Seq

	return &PullResponse{
		Changes:    changes,
		Deletes:    deletes,
		Checkpoint: newCheckpoint,
		HasMore:    hasMore,
	}, nil
}

// hydrateRecords fetches full record data for a batch of IDs from one table.
func (p *pullProcessor) hydrateRecords(ctx context.Context, db DB, tableName string, ids []string) ([]Record, error) {
	cfg := p.registry.Get(tableName)
	if cfg == nil {
		return nil, fmt.Errorf("table %q not registered", tableName)
	}

	// Build SELECT clause
	var selectExpr string
	if len(cfg.SyncColumns) > 0 {
		quotedCols := make([]string, len(cfg.SyncColumns))
		for i, col := range cfg.SyncColumns {
			quotedCols[i] = quoteIdentifier(col)
		}
		selectExpr = fmt.Sprintf("json_build_object(%s)::text",
			buildJsonPairs(cfg.SyncColumns))
	} else {
		selectExpr = "row_to_json(t)::text"
	}

	// Expand slice into individual placeholders for database/sql compatibility.
	var args []any
	idList, args, _ := expandSlicePlaceholder(ids, 1, args)

	query := fmt.Sprintf(
		"SELECT %s::text AS id, %s AS data, %s AS updated_at, %s AS deleted_at FROM %s t WHERE %s IN %s",
		quoteIdentifier(cfg.IDColumn),
		selectExpr,
		quoteIdentifier(cfg.UpdatedAtColumn),
		quoteIdentifier(cfg.DeletedAtColumn),
		quoteIdentifier(cfg.TableName),
		quoteIdentifier(cfg.IDColumn),
		idList)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("hydrating records from %q: %w", tableName, err)
	}
	defer rows.Close()

	var records []Record
	for rows.Next() {
		var r Record
		var dataStr string
		var deletedAt sql.NullTime

		if err := rows.Scan(&r.ID, &dataStr, &r.UpdatedAt, &deletedAt); err != nil {
			return nil, fmt.Errorf("scanning record from %q: %w", tableName, err)
		}
		r.TableName = tableName
		r.Data = json.RawMessage(dataStr)
		if deletedAt.Valid {
			r.DeletedAt = &deletedAt.Time
		}
		records = append(records, r)
	}

	return records, rows.Err()
}

// recordRef is a deduplicated reference to a changelog entry.
type recordRef struct {
	TableName string
	RecordID  string
	Operation Operation
	Seq       int64
}

// deduplicateEntries keeps only the latest changelog entry per record.
// Entries are assumed to be ordered by seq. For duplicate records, the later
// entry replaces the earlier one at the same position in the output slice.
func deduplicateEntries(entries []ChangelogEntry) []recordRef {
	seen := make(map[string]int, len(entries)) // "table:id" → index in refs
	refs := make([]recordRef, 0, len(entries))
	for _, e := range entries {
		key := e.TableName + ":" + e.RecordID
		if idx, ok := seen[key]; ok {
			refs[idx] = recordRef{
				TableName: e.TableName,
				RecordID:  e.RecordID,
				Operation: e.Operation,
				Seq:       e.Seq,
			}
		} else {
			seen[key] = len(refs)
			refs = append(refs, recordRef{
				TableName: e.TableName,
				RecordID:  e.RecordID,
				Operation: e.Operation,
				Seq:       e.Seq,
			})
		}
	}
	return refs
}

// DefaultSnapshotLimit is the default number of records per snapshot page.
const DefaultSnapshotLimit = 100

// MaxSnapshotLimit is the maximum records per snapshot page.
const MaxSnapshotLimit = 1000

// processSnapshot returns one page of snapshot data, iterating tables in registration order.
// The cursor tracks which table and which record ID the client has consumed up to.
func (p *pullProcessor) processSnapshot(ctx context.Context, db DB, cursor *SnapshotCursor, limit int) (*SnapshotResponse, error) {
	tables := p.registry.TableNames()
	if len(tables) == 0 {
		return &SnapshotResponse{Records: []Record{}, HasMore: false}, nil
	}

	if limit <= 0 {
		limit = DefaultSnapshotLimit
	}
	if limit > MaxSnapshotLimit {
		limit = MaxSnapshotLimit
	}

	tableIndex := 0
	afterID := ""
	var checkpoint int64

	if cursor != nil {
		tableIndex = cursor.TableIndex
		afterID = cursor.AfterID
		checkpoint = cursor.Checkpoint
	}

	// On the first page, capture MAX(seq) as the checkpoint.
	if checkpoint == 0 {
		err := db.QueryRowContext(ctx, "SELECT COALESCE(MAX(seq), 0) FROM sync_changelog").Scan(&checkpoint)
		if err != nil {
			return nil, fmt.Errorf("capturing snapshot checkpoint: %w", err)
		}
	}

	var records []Record

	for tableIndex < len(tables) && len(records) < limit {
		tableName := tables[tableIndex]
		cfg := p.registry.Get(tableName)
		if cfg == nil {
			tableIndex++
			afterID = ""
			continue
		}

		remaining := limit - len(records)
		page, err := snapshotPage(ctx, db, cfg, afterID, remaining)
		if err != nil {
			return nil, fmt.Errorf("snapshot page for %q: %w", tableName, err)
		}

		records = append(records, page...)

		if len(page) < remaining {
			// Table exhausted, move to next
			tableIndex++
			afterID = ""
		} else {
			// More records may exist in this table
			afterID = page[len(page)-1].ID
			break
		}
	}

	hasMore := tableIndex < len(tables)

	resp := &SnapshotResponse{
		Records:    records,
		Checkpoint: checkpoint,
		HasMore:    hasMore,
	}
	if len(resp.Records) == 0 {
		resp.Records = []Record{}
	}

	if hasMore {
		resp.Cursor = &SnapshotCursor{
			Checkpoint: checkpoint,
			TableIndex: tableIndex,
			AfterID:    afterID,
		}
	}

	return resp, nil
}

// snapshotPage fetches one page of records from a table for full snapshot.
//
// The WHERE clause uses id::text > $1 with ORDER BY id::text for cursor pagination.
// Text-cast is used because Synchro is a generic library with no constraint on the
// underlying PK type (UUID, BIGSERIAL, TEXT, ULID, etc.). Casting to text is the only
// comparison that works across all types without requiring type configuration.
//
// This prevents PostgreSQL from using the native PK index (a seq scan + sort per page).
// This is acceptable because: (1) full resync is rare, only after compaction boundary;
// (2) pages are bounded by LIMIT; (3) dominant cost is JSON serialization + network;
// (4) consuming apps can add an expression index (CREATE INDEX ON t ((id::text))) if needed.
func snapshotPage(ctx context.Context, db DB, cfg *TableConfig, afterID string, limit int) ([]Record, error) {
	var selectExpr string
	if len(cfg.SyncColumns) > 0 {
		selectExpr = fmt.Sprintf("json_build_object(%s)::text", buildJsonPairs(cfg.SyncColumns))
	} else {
		selectExpr = "row_to_json(t)::text"
	}

	query := fmt.Sprintf(
		`SELECT %s::text AS id, %s AS data, %s AS updated_at
		FROM %s t
		WHERE %s IS NULL AND %s::text > $1
		ORDER BY %s::text
		LIMIT $2`,
		quoteIdentifier(cfg.IDColumn),
		selectExpr,
		quoteIdentifier(cfg.UpdatedAtColumn),
		quoteIdentifier(cfg.TableName),
		quoteIdentifier(cfg.DeletedAtColumn),
		quoteIdentifier(cfg.IDColumn),
		quoteIdentifier(cfg.IDColumn),
	)

	rows, err := db.QueryContext(ctx, query, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("querying snapshot page from %q: %w", cfg.TableName, err)
	}
	defer rows.Close()

	var records []Record
	for rows.Next() {
		var r Record
		var dataStr string
		if err := rows.Scan(&r.ID, &dataStr, &r.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scanning snapshot record from %q: %w", cfg.TableName, err)
		}
		r.TableName = cfg.TableName
		r.Data = json.RawMessage(dataStr)
		records = append(records, r)
	}

	return records, rows.Err()
}

// buildJsonPairs builds json_build_object arguments like "'col1', "col1", 'col2', "col2"".
// Column names are escaped as both SQL string literal keys (single-quote doubled)
// and SQL identifiers (double-quote escaped via quoteIdentifier).
func buildJsonPairs(cols []string) string {
	pairs := make([]string, 0, len(cols)*2)
	for _, col := range cols {
		safeKey := strings.ReplaceAll(col, "'", "''")
		pairs = append(pairs, fmt.Sprintf("'%s', %s", safeKey, quoteIdentifier(col)))
	}
	return strings.Join(pairs, ", ")
}
