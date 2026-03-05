package wal

import (
	"context"
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
	AssignBuckets(ctx context.Context, table string, recordID string, data map[string]any) ([]string, error)
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

	// ChangelogDB writes entries to the sync_changelog table and persists WAL position.
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
	// Load persisted LSN for crash recovery
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

	// Create replication slot if it doesn't exist
	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, c.cfg.SlotName, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Temporary: false})
	if err != nil {
		// Slot may already exist — log and continue
		c.logger.InfoContext(ctx, "replication slot creation",
			"slot", c.cfg.SlotName, "result", err.Error())
	}

	// Determine start position: use persisted LSN if available,
	// otherwise fall back to current server position (first run only).
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

	// Start replication from the determined position
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
			// Send final status before exiting
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
				buckets, err := c.cfg.Assigner.AssignBuckets(ctx, event.TableName, event.RecordID, event.Data)
				if err != nil {
					c.logger.ErrorContext(ctx, "failed to assign buckets",
						"err", err, "table", event.TableName, "id", event.RecordID)
					continue
				}

				for _, bucket := range buckets {
					_, err := c.cfg.ChangelogDB.ExecContext(ctx,
						"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
						bucket, event.TableName, event.RecordID, int(event.Operation))
					if err != nil {
						c.logger.ErrorContext(ctx, "failed to write changelog",
							"err", err, "bucket", bucket, "table", event.TableName, "id", event.RecordID)
					}
				}
			}

			newLSN := xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			if err := c.position.SetConfirmed(ctx, newLSN); err != nil {
				c.logger.ErrorContext(ctx, "failed to persist WAL position",
					"err", err, "lsn", newLSN)
			}
		}
	}
}
