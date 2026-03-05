package wal

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/jackc/pglogrepl"

	"github.com/trainstar/synchro"
)

// Position tracks the confirmed LSN with persistent storage for crash recovery.
type Position struct {
	mu        sync.Mutex
	confirmed pglogrepl.LSN
	db        synchro.DB
	slotName  string
}

// NewPosition creates a new Position tracker backed by persistent storage.
// On first call, loads the last confirmed LSN from sync_wal_position.
func NewPosition(db synchro.DB, slotName string) *Position {
	return &Position{db: db, slotName: slotName}
}

// Load reads the persisted LSN from the database. Returns 0 if no row exists.
func (p *Position) Load(ctx context.Context) (pglogrepl.LSN, error) {
	var lsn uint64
	err := p.db.QueryRowContext(ctx,
		"SELECT confirmed_lsn FROM sync_wal_position WHERE slot_name = $1",
		p.slotName).Scan(&lsn)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("loading WAL position: %w", err)
	}

	p.mu.Lock()
	p.confirmed = pglogrepl.LSN(lsn)
	p.mu.Unlock()

	return pglogrepl.LSN(lsn), nil
}

// Confirmed returns the last confirmed LSN.
func (p *Position) Confirmed() pglogrepl.LSN {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.confirmed
}

// SetConfirmed updates the confirmed LSN in memory and persists it.
func (p *Position) SetConfirmed(ctx context.Context, lsn pglogrepl.LSN) error {
	p.mu.Lock()
	if lsn <= p.confirmed {
		p.mu.Unlock()
		return nil
	}
	p.confirmed = lsn
	p.mu.Unlock()

	_, err := p.db.ExecContext(ctx,
		`INSERT INTO sync_wal_position (slot_name, confirmed_lsn, updated_at)
		 VALUES ($1, $2, now())
		 ON CONFLICT (slot_name) DO UPDATE SET confirmed_lsn = $2, updated_at = now()`,
		p.slotName, uint64(lsn))
	if err != nil {
		return fmt.Errorf("persisting WAL position: %w", err)
	}
	return nil
}
