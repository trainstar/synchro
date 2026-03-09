package wal

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/trainstar/synchro"
)

// BucketAssigner resolves which buckets a changed record belongs to.
type BucketAssigner interface {
	AssignBuckets(ctx context.Context, table string, recordID string, operation synchro.Operation, data map[string]any) ([]string, error)
}

// ConsumerConfig configures the WAL consumer.
type ConsumerConfig struct {
	// ConnString is the PostgreSQL connection string with replication=database.
	ConnString string

	// SlotName is the replication slot name.
	SlotName string

	// PublicationName is the publication to subscribe to.
	PublicationName string

	// Registry is used to look up table configurations.
	Registry *synchro.Registry

	// Assigner determines bucket IDs for changed records.
	Assigner BucketAssigner

	// ChangelogDB writes entries to sync sidecar tables and persists WAL position.
	ChangelogDB synchro.DB

	// Logger for consumer events.
	Logger *slog.Logger

	// StandbyTimeout is how often to send standby status updates.
	// Defaults to 10 seconds.
	StandbyTimeout time.Duration
}

// Consumer reads from a PostgreSQL logical replication slot and writes
// changelog entries for each captured change.
type Consumer struct {
	cfg      ConsumerConfig
	conn     *pgconn.PgConn
	position *Position
	decoder  *Decoder
	logger   *slog.Logger
}

// NewConsumer creates a new WAL consumer.
func NewConsumer(cfg ConsumerConfig) *Consumer {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Consumer{
		cfg:      cfg,
		position: NewPosition(cfg.ChangelogDB, cfg.SlotName),
		decoder:  NewDecoder(cfg.Registry),
		logger:   logger,
	}
}

// Start begins consuming WAL events. Blocks until ctx is cancelled.
func (c *Consumer) Start(ctx context.Context) error {
	persistedLSN, err := c.position.Load(ctx)
	if err != nil {
		return fmt.Errorf("loading persisted LSN: %w", err)
	}

	conn, err := pgconn.Connect(ctx, c.cfg.ConnString)
	if err != nil {
		return fmt.Errorf("connecting to replication slot: %w", err)
	}
	c.conn = conn
	defer conn.Close(ctx)

	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, c.cfg.SlotName, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Temporary: false})
	if err != nil {
		c.logger.InfoContext(ctx, "replication slot creation",
			"slot", c.cfg.SlotName, "result", err.Error())
	}

	startLSN := persistedLSN
	if startLSN == 0 {
		sysident, err := pglogrepl.IdentifySystem(ctx, conn)
		if err != nil {
			return fmt.Errorf("identify system: %w", err)
		}
		startLSN = sysident.XLogPos
		c.logger.InfoContext(ctx, "no persisted LSN, starting from current server position",
			"start_lsn", startLSN)
	} else {
		c.logger.InfoContext(ctx, "resuming from persisted LSN",
			"start_lsn", startLSN)
	}

	err = pglogrepl.StartReplication(ctx, conn, c.cfg.SlotName, startLSN,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '1'",
				fmt.Sprintf("publication_names '%s'", c.cfg.PublicationName),
			},
		})
	if err != nil {
		return fmt.Errorf("start replication: %w", err)
	}

	c.logger.InfoContext(ctx, "WAL consumer started",
		"slot", c.cfg.SlotName,
		"publication", c.cfg.PublicationName,
		"start_lsn", startLSN)

	standbyTimeout := c.cfg.StandbyTimeout
	if standbyTimeout == 0 {
		standbyTimeout = 10 * time.Second
	}
	nextStandbyDeadline := time.Now().Add(standbyTimeout)

	for {
		if ctx.Err() != nil {
			_ = pglogrepl.SendStandbyStatusUpdate(ctx, conn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: c.position.Confirmed()})
			return ctx.Err()
		}

		if time.Now().After(nextStandbyDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(ctx, conn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: c.position.Confirmed()})
			if err != nil {
				return fmt.Errorf("send standby status: %w", err)
			}
			nextStandbyDeadline = time.Now().Add(standbyTimeout)
		}

		receiveCtx, cancel := context.WithDeadline(ctx, nextStandbyDeadline)
		rawMsg, err := conn.ReceiveMessage(receiveCtx)
		cancel()

		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("receive message: %w", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("postgres replication error: %s", errMsg.Message)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("parse keepalive: %w", err)
			}
			if pkm.ReplyRequested {
				nextStandbyDeadline = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("parse xlog data: %w", err)
			}

			events, err := c.decoder.Decode(xld.WALData)
			if err != nil {
				c.logger.ErrorContext(ctx, "failed to decode WAL message",
					"err", err, "lsn", xld.WALStart)
				continue
			}

			for _, event := range events {
				if err := c.applyEvent(ctx, event); err != nil {
					return fmt.Errorf("processing WAL event for %s/%s %s: %w", event.TableName, event.RecordID, event.Operation.String(), err)
				}
			}

			newLSN := xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			if err := c.position.SetConfirmed(ctx, newLSN); err != nil {
				return fmt.Errorf("persisting WAL position %s: %w", newLSN, err)
			}
		}
	}
}

func (c *Consumer) applyEvent(ctx context.Context, event WALEvent) error {
	db := c.cfg.ChangelogDB
	var (
		tx  *sql.Tx
		err error
	)
	if beginner, ok := c.cfg.ChangelogDB.(synchro.TxBeginner); ok {
		tx, err = beginner.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin WAL event tx: %w", err)
		}
		db = tx
		defer tx.Rollback()
	}

	existing, err := c.loadExistingBuckets(ctx, db, event.TableName, event.RecordID)
	if err != nil {
		return err
	}

	desired := []string{}
	if event.Operation != synchro.OpDelete || len(existing) == 0 {
		desired, err = c.cfg.Assigner.AssignBuckets(ctx, event.TableName, event.RecordID, event.Operation, event.Data)
		if err != nil {
			if writeErr := c.writeRuleFailure(ctx, db, event, err); writeErr != nil {
				c.logger.ErrorContext(ctx, "failed to write rule failure record",
					"err", writeErr, "table", event.TableName, "id", event.RecordID)
			}
			if tx != nil {
				if commitErr := tx.Commit(); commitErr != nil {
					return fmt.Errorf("commit rule failure tx: %w", commitErr)
				}
			}
			return nil
		}
		desired = dedupeBuckets(desired)
	}

	entries := buildEdgeDiffEntries(event, existing, desired)
	if err := c.writeChangelogEntries(ctx, db, entries); err != nil {
		return err
	}

	if err := c.applyEdgeDiff(ctx, db, event, existing, desired); err != nil {
		return err
	}

	if tx != nil {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit WAL event tx: %w", err)
		}
	}
	return nil
}

func (c *Consumer) loadExistingBuckets(ctx context.Context, db synchro.DB, tableName, recordID string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT bucket_id
		FROM sync_bucket_edges
		WHERE table_name = $1 AND record_id = $2
	`, tableName, recordID)
	if err != nil {
		return nil, fmt.Errorf("loading existing bucket edges: %w", err)
	}
	defer rows.Close()

	var buckets []string
	for rows.Next() {
		var b string
		if err := rows.Scan(&b); err != nil {
			return nil, fmt.Errorf("scanning bucket edge: %w", err)
		}
		buckets = append(buckets, b)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading bucket edges: %w", err)
	}
	return buckets, nil
}

func (c *Consumer) applyEdgeDiff(
	ctx context.Context,
	db synchro.DB,
	event WALEvent,
	existing []string,
	desired []string,
) error {
	if event.Operation == synchro.OpDelete {
		_, err := db.ExecContext(ctx, `
			DELETE FROM sync_bucket_edges
			WHERE table_name = $1 AND record_id = $2
		`, event.TableName, event.RecordID)
		if err != nil {
			return fmt.Errorf("deleting bucket edges for delete event: %w", err)
		}
		return nil
	}

	added, _, removed := diffBucketSets(existing, desired)
	if len(added) > 0 {
		if err := c.upsertBucketEdges(ctx, db, event.TableName, event.RecordID, added); err != nil {
			return err
		}
	}
	if len(removed) > 0 {
		if err := c.deleteBucketEdges(ctx, db, event.TableName, event.RecordID, removed); err != nil {
			return err
		}
	}
	return nil
}

func (c *Consumer) upsertBucketEdges(ctx context.Context, db synchro.DB, tableName, recordID string, buckets []string) error {
	if len(buckets) == 0 {
		return nil
	}
	query := "INSERT INTO sync_bucket_edges (table_name, record_id, bucket_id, updated_at) VALUES "
	args := make([]any, 0, len(buckets)*3)
	for i, bucket := range buckets {
		if i > 0 {
			query += ", "
		}
		base := i*3 + 1
		query += fmt.Sprintf("($%d, $%d, $%d, now())", base, base+1, base+2)
		args = append(args, tableName, recordID, bucket)
	}
	query += " ON CONFLICT (table_name, record_id, bucket_id) DO UPDATE SET updated_at = now()"
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("upserting bucket edges: %w", err)
	}
	return nil
}

func (c *Consumer) deleteBucketEdges(ctx context.Context, db synchro.DB, tableName, recordID string, buckets []string) error {
	if len(buckets) == 0 {
		return nil
	}
	inClause, args, next := synchroExpandSlicePlaceholder(buckets, 3, []any{tableName, recordID})
	query := fmt.Sprintf(`
		DELETE FROM sync_bucket_edges
		WHERE table_name = $1
		  AND record_id = $2
		  AND bucket_id IN %s
	`, inClause)
	_ = next
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("deleting bucket edges: %w", err)
	}
	return nil
}

func (c *Consumer) writeChangelogEntries(ctx context.Context, db synchro.DB, entries []changelogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	query := "INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES "
	args := make([]any, 0, len(entries)*4)
	for i, e := range entries {
		if i > 0 {
			query += ", "
		}
		base := i*4 + 1
		query += fmt.Sprintf("($%d, $%d, $%d, $%d)", base, base+1, base+2, base+3)
		args = append(args, e.BucketID, e.TableName, e.RecordID, int(e.Operation))
	}
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("writing changelog entries: %w", err)
	}
	return nil
}

func (c *Consumer) writeRuleFailure(ctx context.Context, db synchro.DB, event WALEvent, cause error) error {
	payload, _ := json.Marshal(event.Data)
	_, err := db.ExecContext(ctx, `
		INSERT INTO sync_rule_failures (table_name, record_id, operation, error_text, payload)
		VALUES ($1, $2, $3, $4, $5::jsonb)
	`, event.TableName, event.RecordID, int(event.Operation), cause.Error(), string(payload))
	if err != nil {
		return fmt.Errorf("writing sync_rule_failures row: %w", err)
	}
	return nil
}

type changelogEntry struct {
	BucketID  string
	TableName string
	RecordID  string
	Operation synchro.Operation
}

func buildEdgeDiffEntries(event WALEvent, existing []string, desired []string) []changelogEntry {
	existing = dedupeBuckets(existing)
	desired = dedupeBuckets(desired)

	existingSet := make(map[string]struct{}, len(existing))
	for _, b := range existing {
		existingSet[b] = struct{}{}
	}
	desiredSet := make(map[string]struct{}, len(desired))
	for _, b := range desired {
		desiredSet[b] = struct{}{}
	}

	out := make([]changelogEntry, 0, len(existing)+len(desired))

	if event.Operation == synchro.OpDelete {
		targets := existing
		if len(targets) == 0 {
			targets = desired
		}
		for _, bucket := range targets {
			out = append(out, changelogEntry{
				BucketID:  bucket,
				TableName: event.TableName,
				RecordID:  event.RecordID,
				Operation: synchro.OpDelete,
			})
		}
		return out
	}

	for _, bucket := range desired {
		if _, ok := existingSet[bucket]; ok {
			op := event.Operation
			if op == synchro.OpInsert {
				op = synchro.OpUpdate
			}
			out = append(out, changelogEntry{
				BucketID:  bucket,
				TableName: event.TableName,
				RecordID:  event.RecordID,
				Operation: op,
			})
			continue
		}
		out = append(out, changelogEntry{
			BucketID:  bucket,
			TableName: event.TableName,
			RecordID:  event.RecordID,
			Operation: synchro.OpInsert,
		})
	}

	for _, bucket := range existing {
		if _, ok := desiredSet[bucket]; ok {
			continue
		}
		out = append(out, changelogEntry{
			BucketID:  bucket,
			TableName: event.TableName,
			RecordID:  event.RecordID,
			Operation: synchro.OpDelete,
		})
	}
	return out
}

func diffBucketSets(existing []string, desired []string) (added []string, kept []string, removed []string) {
	existingSet := make(map[string]struct{}, len(existing))
	for _, b := range existing {
		existingSet[b] = struct{}{}
	}
	desiredSet := make(map[string]struct{}, len(desired))
	for _, b := range desired {
		desiredSet[b] = struct{}{}
		if _, ok := existingSet[b]; ok {
			kept = append(kept, b)
		} else {
			added = append(added, b)
		}
	}
	for _, b := range existing {
		if _, ok := desiredSet[b]; !ok {
			removed = append(removed, b)
		}
	}
	return added, kept, removed
}

func dedupeBuckets(in []string) []string {
	if len(in) < 2 {
		return in
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, b := range in {
		if b == "" {
			continue
		}
		if _, ok := seen[b]; ok {
			continue
		}
		seen[b] = struct{}{}
		out = append(out, b)
	}
	return out
}

// Local copy of synchro.expandSlicePlaceholder to avoid exposing internal helpers.
func synchroExpandSlicePlaceholder(slice []string, startIdx int, args []any) (string, []any, int) {
	if len(slice) == 0 {
		return "(NULL)", args, startIdx
	}
	query := "("
	for i, v := range slice {
		if i > 0 {
			query += ", "
		}
		query += fmt.Sprintf("$%d", startIdx)
		args = append(args, v)
		startIdx++
	}
	query += ")"
	return query, args, startIdx
}
