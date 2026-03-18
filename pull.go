package synchro

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
)

// pullProcessor handles pull and rebuild operations.
type pullProcessor struct {
	registry   *Registry
	changelog  *changelogStore
	checkpoint *checkpointStore
	logger     *slog.Logger
}

// processPull retrieves changes for a client based on their checkpoint.
// Used for both legacy (single checkpoint) and per-bucket checkpoint modes.
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
	// Track bucket_id per record for the response
	recordBuckets := make(map[string]string) // "table:id" → bucket_id

	for _, ref := range refs {
		if ref.Operation == OpDelete {
			deletes = append(deletes, DeleteEntry{
				ID:        ref.RecordID,
				TableName: ref.TableName,
			})
		} else {
			changesByTable[ref.TableName] = append(changesByTable[ref.TableName], ref.RecordID)
		}
		recordBuckets[ref.TableName+":"+ref.RecordID] = ref.BucketID
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
		// Set BucketID on each hydrated record
		for i := range records {
			key := records[i].TableName + ":" + records[i].ID
			records[i].BucketID = recordBuckets[key]
		}
		changes = append(changes, records...)
	}

	// New checkpoint is the max seq from the entries we processed
	newCheckpoint := entries[len(entries)-1].Seq

	resp := &PullResponse{
		Changes:    changes,
		Deletes:    deletes,
		Checkpoint: newCheckpoint,
		HasMore:    hasMore,
	}

	p.setBucketChecksumsOnPull(ctx, db, resp, bucketIDs)

	return resp, nil
}

// processPerBucketPull handles pull for clients using per-bucket checkpoints.
// It uses the minimum of all bucket checkpoints as the query start, then
// filters entries per-bucket in Go.
func (p *pullProcessor) processPerBucketPull(ctx context.Context, db DB, req *PullRequest, bucketIDs []string) (*PullResponse, error) {
	limit := req.Limit
	if limit <= 0 {
		limit = DefaultPullLimit
	}
	if limit > MaxPullLimit {
		limit = MaxPullLimit
	}

	bucketCheckpoints := req.BucketCheckpoints

	// Compute minimum checkpoint across all subscribed buckets
	var minCheckpoint int64
	first := true
	for _, bid := range bucketIDs {
		cp, ok := bucketCheckpoints[bid]
		if !ok {
			// Unknown bucket → checkpoint 0 (needs rebuild)
			cp = 0
		}
		if first || cp < minCheckpoint {
			minCheckpoint = cp
			first = false
		}
	}

	// Query changelog from the minimum checkpoint
	entries, err := p.changelog.QueryAfter(ctx, db, bucketIDs, minCheckpoint, limit+1, req.Tables)
	if err != nil {
		return nil, fmt.Errorf("querying changelog: %w", err)
	}

	// Per-bucket filtering: skip entries already seen by the specific bucket
	var filtered []ChangelogEntry
	for _, e := range entries {
		bucketCP, ok := bucketCheckpoints[e.BucketID]
		if !ok {
			bucketCP = 0
		}
		if e.Seq > bucketCP {
			filtered = append(filtered, e)
		}
	}

	hasMore := len(filtered) > limit
	if hasMore {
		filtered = filtered[:limit]
	}

	if len(filtered) == 0 {
		return &PullResponse{
			Changes:           []Record{},
			Deletes:           []DeleteEntry{},
			Checkpoint:        minCheckpoint,
			BucketCheckpoints: bucketCheckpoints,
			HasMore:           false,
		}, nil
	}

	// Deduplicate
	refs := deduplicateEntries(filtered)

	// Separate deletes from changes, group changes by table
	deletes := make([]DeleteEntry, 0)
	changesByTable := make(map[string][]string)
	recordBuckets := make(map[string]string)

	for _, ref := range refs {
		if ref.Operation == OpDelete {
			deletes = append(deletes, DeleteEntry{
				ID:        ref.RecordID,
				TableName: ref.TableName,
			})
		} else {
			changesByTable[ref.TableName] = append(changesByTable[ref.TableName], ref.RecordID)
		}
		recordBuckets[ref.TableName+":"+ref.RecordID] = ref.BucketID
	}

	// Hydrate
	changes := make([]Record, 0)
	for tableName, ids := range changesByTable {
		records, err := p.hydrateRecords(ctx, db, tableName, ids)
		if err != nil {
			p.logger.ErrorContext(ctx, "failed to hydrate records",
				"err", err, "table", tableName, "count", len(ids))
			continue
		}
		for i := range records {
			key := records[i].TableName + ":" + records[i].ID
			records[i].BucketID = recordBuckets[key]
		}
		changes = append(changes, records...)
	}

	// Track max seq per bucket for the response checkpoints
	newBucketCheckpoints := make(map[string]int64, len(bucketCheckpoints))
	for k, v := range bucketCheckpoints {
		newBucketCheckpoints[k] = v
	}
	for _, e := range filtered {
		if e.Seq > newBucketCheckpoints[e.BucketID] {
			newBucketCheckpoints[e.BucketID] = e.Seq
		}
	}

	// Global checkpoint = max seq processed
	newCheckpoint := filtered[len(filtered)-1].Seq

	resp := &PullResponse{
		Changes:           changes,
		Deletes:           deletes,
		Checkpoint:        newCheckpoint,
		BucketCheckpoints: newBucketCheckpoints,
		HasMore:           hasMore,
	}

	p.setBucketChecksumsOnPull(ctx, db, resp, bucketIDs)

	return resp, nil
}

// processRebuild returns one page of records from a bucket via sync_bucket_edges.
// This replaces the snapshot mechanism for rebuilding stale buckets.
func (p *pullProcessor) processRebuild(ctx context.Context, db DB, bucketID string, cursor string, limit int) (*RebuildResponse, error) {
	if limit <= 0 {
		limit = DefaultRebuildLimit
	}
	if limit > MaxRebuildLimit {
		limit = MaxRebuildLimit
	}

	// Capture MAX(seq) as the checkpoint for this bucket
	var checkpoint int64
	err := db.QueryRowContext(ctx, "SELECT COALESCE(MAX(seq), 0) FROM sync_changelog").Scan(&checkpoint)
	if err != nil {
		return nil, fmt.Errorf("capturing rebuild checkpoint: %w", err)
	}

	// Parse cursor: "table_name|record_id" format
	var cursorTable, cursorID string
	if cursor != "" {
		parts := strings.SplitN(cursor, "|", 2)
		if len(parts) == 2 {
			cursorTable = parts[0]
			cursorID = parts[1]
		}
	}

	// Query sync_bucket_edges for this bucket with cursor pagination
	var rows *sql.Rows
	if cursorTable != "" {
		rows, err = db.QueryContext(ctx,
			`SELECT e.table_name, e.record_id
			FROM sync_bucket_edges e
			WHERE e.bucket_id = $1
			  AND (e.table_name, e.record_id) > ($2, $3)
			ORDER BY e.table_name, e.record_id
			LIMIT $4`,
			bucketID, cursorTable, cursorID, limit+1)
	} else {
		rows, err = db.QueryContext(ctx,
			`SELECT e.table_name, e.record_id
			FROM sync_bucket_edges e
			WHERE e.bucket_id = $1
			ORDER BY e.table_name, e.record_id
			LIMIT $2`,
			bucketID, limit+1)
	}
	if err != nil {
		return nil, fmt.Errorf("querying bucket edges for rebuild: %w", err)
	}

	type edgeRef struct {
		tableName string
		recordID  string
	}
	var edges []edgeRef
	for rows.Next() {
		var ref edgeRef
		if err := rows.Scan(&ref.tableName, &ref.recordID); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scanning bucket edge: %w", err)
		}
		edges = append(edges, ref)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating bucket edges: %w", err)
	}

	hasMore := len(edges) > limit
	if hasMore {
		edges = edges[:limit]
	}

	// Group record IDs by table for batch hydration
	byTable := make(map[string][]string)
	for _, edge := range edges {
		byTable[edge.tableName] = append(byTable[edge.tableName], edge.recordID)
	}

	// Hydrate records, filtering out soft-deleted ones
	var records []Record
	for tableName, ids := range byTable {
		hydrated, err := p.hydrateRecords(ctx, db, tableName, ids)
		if err != nil {
			p.logger.ErrorContext(ctx, "failed to hydrate rebuild records",
				"err", err, "table", tableName, "count", len(ids))
			continue
		}
		// Filter out soft-deleted records
		for _, r := range hydrated {
			if r.DeletedAt == nil {
				r.BucketID = bucketID
				records = append(records, r)
			}
		}
	}

	resp := &RebuildResponse{
		Records:    records,
		Checkpoint: checkpoint,
		HasMore:    hasMore,
	}
	if len(resp.Records) == 0 {
		resp.Records = []Record{}
	}

	if hasMore && len(edges) > 0 {
		lastEdge := edges[len(edges)-1]
		resp.Cursor = lastEdge.tableName + "|" + lastEdge.recordID
	}

	p.setBucketChecksumOnRebuild(ctx, db, resp, bucketID)

	return resp, nil
}

// detectStaleBuckets checks which buckets need a rebuild because their
// checkpoint is behind the changelog's minimum seq (data was compacted away).
func (p *pullProcessor) detectStaleBuckets(ctx context.Context, db DB, bucketCheckpoints map[string]int64, bucketIDs []string) ([]string, error) {
	minSeq, err := p.changelog.MinSeq(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("checking min seq for stale buckets: %w", err)
	}
	if minSeq == 0 {
		// Empty changelog, nothing is stale
		return nil, nil
	}

	var stale []string
	for _, bid := range bucketIDs {
		cp, ok := bucketCheckpoints[bid]
		if !ok {
			// Bucket not in client's checkpoint map (new subscription).
			stale = append(stale, bid)
			continue
		}
		if cp > 0 && cp < minSeq {
			// Checkpoint is behind the compaction boundary.
			stale = append(stale, bid)
		}
		// cp == 0 is valid: the client has never pulled, but all changelog
		// entries are available (nothing compacted). Normal incremental pull
		// from seq 0 will catch up.
	}
	return stale, nil
}

// hydrateRecords fetches full record data for a batch of IDs from one table.
func (p *pullProcessor) hydrateRecords(ctx context.Context, db DB, tableName string, ids []string) ([]Record, error) {
	cfg := p.registry.Get(tableName)
	if cfg == nil {
		return nil, fmt.Errorf("table %q not registered", tableName)
	}

	selectExpr := "row_to_json(t)::text"

	uaExpr := "NULL::timestamptz"
	if cfg.HasUpdatedAt() {
		uaExpr = quoteIdentifier(cfg.UpdatedAtCol())
	}
	daExpr := "NULL::timestamptz"
	if cfg.HasDeletedAt() {
		daExpr = quoteIdentifier(cfg.DeletedAtCol())
	}

	// Expand slice into individual placeholders for database/sql compatibility.
	var args []any
	idList, args, _ := expandSlicePlaceholder(ids, 1, args)

	query := fmt.Sprintf(
		"SELECT %s::text AS id, %s AS data, %s AS updated_at, %s AS deleted_at FROM %s t WHERE %s IN %s",
		quoteIdentifier(cfg.IDColumn),
		selectExpr,
		uaExpr,
		daExpr,
		quoteIdentifier(cfg.TableName),
		quoteIdentifier(cfg.IDColumn),
		idList)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("hydrating records from %q: %w", tableName, err)
	}
	defer func() { _ = rows.Close() }()

	var records []Record
	for rows.Next() {
		var r Record
		var dataStr string
		var updatedAt sql.NullTime
		var deletedAt sql.NullTime

		if err := rows.Scan(&r.ID, &dataStr, &updatedAt, &deletedAt); err != nil {
			return nil, fmt.Errorf("scanning record from %q: %w", tableName, err)
		}
		r.TableName = tableName
		r.Data = json.RawMessage(dataStr)
		if updatedAt.Valid {
			r.UpdatedAt = &updatedAt.Time
		}
		if deletedAt.Valid {
			r.DeletedAt = &deletedAt.Time
		}
		// Compute per-record checksum from the canonical JSON representation.
		// Stored as int32 (signed) to match PostgreSQL INTEGER and Kotlin Int.
		cs := int32(ComputeRecordChecksum(dataStr))
		r.Checksum = &cs
		records = append(records, r)
	}

	return records, rows.Err()
}

// setBucketChecksumsOnPull computes and attaches bucket checksums to the pull
// response on the final page. No-op if hasMore is true.
func (p *pullProcessor) setBucketChecksumsOnPull(ctx context.Context, db DB, resp *PullResponse, bucketIDs []string) {
	if resp.HasMore {
		return
	}
	checksums, err := p.computeBucketChecksums(ctx, db, bucketIDs)
	if err != nil {
		p.logger.ErrorContext(ctx, "failed to compute bucket checksums", "err", err)
		return
	}
	if len(checksums) > 0 {
		resp.BucketChecksums = checksums
	}
}

// setBucketChecksumOnRebuild computes and attaches the bucket checksum to the
// rebuild response on the final page. No-op if hasMore is true.
func (p *pullProcessor) setBucketChecksumOnRebuild(ctx context.Context, db DB, resp *RebuildResponse, bucketID string) {
	if resp.HasMore {
		return
	}
	checksums, err := p.computeBucketChecksums(ctx, db, []string{bucketID})
	if err != nil {
		p.logger.ErrorContext(ctx, "failed to compute rebuild bucket checksum", "err", err)
		return
	}
	if cs, ok := checksums[bucketID]; ok {
		resp.BucketChecksum = &cs
	}
}

// computeBucketChecksums returns the XOR of all per-record checksums for each
// bucket via BIT_XOR in PostgreSQL (one row per bucket). The checksum column
// is INTEGER (int4) so BIT_XOR works natively with no casts.
func (p *pullProcessor) computeBucketChecksums(ctx context.Context, db DB, bucketIDs []string) (map[string]int32, error) {
	if len(bucketIDs) == 0 {
		return nil, nil
	}

	var args []any
	inClause, args, _ := expandSlicePlaceholder(bucketIDs, 1, args)

	query := fmt.Sprintf(
		`SELECT bucket_id, BIT_XOR(checksum)
		FROM sync_bucket_edges
		WHERE bucket_id IN %s AND checksum IS NOT NULL
		GROUP BY bucket_id`,
		inClause)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("computing bucket checksums: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make(map[string]int32, len(bucketIDs))
	for rows.Next() {
		var bucketID string
		var xorVal int32
		if err := rows.Scan(&bucketID, &xorVal); err != nil {
			return nil, fmt.Errorf("scanning bucket checksum: %w", err)
		}
		result[bucketID] = xorVal
	}
	return result, rows.Err()
}

// recordRef is a deduplicated reference to a changelog entry.
type recordRef struct {
	TableName string
	RecordID  string
	BucketID  string
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
				BucketID:  e.BucketID,
				Operation: e.Operation,
				Seq:       e.Seq,
			}
		} else {
			seen[key] = len(refs)
			refs = append(refs, recordRef{
				TableName: e.TableName,
				RecordID:  e.RecordID,
				BucketID:  e.BucketID,
				Operation: e.Operation,
				Seq:       e.Seq,
			})
		}
	}
	return refs
}

// ---------------------------------------------------------------------------
// Legacy snapshot support (deprecated, use bucket rebuild)
// ---------------------------------------------------------------------------

// DefaultSnapshotLimit is the default number of records per snapshot page.
const DefaultSnapshotLimit = 100

// MaxSnapshotLimit is the maximum records per snapshot page.
const MaxSnapshotLimit = 1000

// processSnapshot returns one page of snapshot data, iterating tables in registration order.
// Deprecated: Use processRebuild for per-bucket reconstruction.
// Kept for legacy client compatibility. Internally delegates to bucket_edges where possible.
func (p *pullProcessor) processSnapshot(ctx context.Context, db DB, userID string, cursor *SnapshotCursor, limit int) (*SnapshotResponse, error) {
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
		page, err := snapshotPageLegacy(ctx, db, cfg, afterID, remaining)
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

// snapshotPageLegacy fetches one page of records from a table for legacy full snapshot.
// Uses RLS (set_config) for ownership filtering rather than BucketFunc.SQLFilter.
func snapshotPageLegacy(ctx context.Context, db DB, cfg *TableConfig, afterID string, limit int) ([]Record, error) {
	selectExpr := "row_to_json(t)::text"

	uaExpr := "NULL::timestamptz"
	if cfg.HasUpdatedAt() {
		uaExpr = quoteIdentifier(cfg.UpdatedAtCol())
	}

	var conditions []string
	args := []any{afterID}
	paramIdx := 2

	conditions = append(conditions, fmt.Sprintf("%s::text > $1", quoteIdentifier(cfg.IDColumn)))

	if cfg.HasDeletedAt() {
		conditions = append(conditions, fmt.Sprintf("%s IS NULL", quoteIdentifier(cfg.DeletedAtCol())))
	}

	whereClause := "WHERE " + strings.Join(conditions, " AND ")

	query := fmt.Sprintf(
		`SELECT %s::text AS id, %s AS data, %s AS updated_at
		FROM %s t
		%s
		ORDER BY %s::text
		LIMIT $%d`,
		quoteIdentifier(cfg.IDColumn),
		selectExpr,
		uaExpr,
		quoteIdentifier(cfg.TableName),
		whereClause,
		quoteIdentifier(cfg.IDColumn),
		paramIdx,
	)
	args = append(args, limit)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying snapshot page from %q: %w", cfg.TableName, err)
	}
	defer func() { _ = rows.Close() }()

	var records []Record
	for rows.Next() {
		var r Record
		var dataStr string
		var updatedAt sql.NullTime
		if err := rows.Scan(&r.ID, &dataStr, &updatedAt); err != nil {
			return nil, fmt.Errorf("scanning snapshot record from %q: %w", cfg.TableName, err)
		}
		r.TableName = cfg.TableName
		r.Data = json.RawMessage(dataStr)
		if updatedAt.Valid {
			r.UpdatedAt = &updatedAt.Time
		}
		records = append(records, r)
	}

	return records, rows.Err()
}
