package synchro

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
)

// infraColumn defines a column that must exist on a synchro infrastructure table.
type infraColumn struct {
	Table      string
	Column     string
	DataType   string // PostgreSQL type for ALTER TABLE ADD COLUMN
	DefaultSQL string // e.g. "DEFAULT now()" or "" for nullable
}

// requiredInfraColumns lists columns that synchro requires on its infrastructure
// tables. NewEngine checks for these and auto-adds any that are missing. This
// is how the library self-heals when upgrading -- no consumer action needed.
var requiredInfraColumns = []infraColumn{
	{Table: "sync_bucket_edges", Column: "checksum", DataType: "INTEGER", DefaultSQL: ""},
}

// ensureInfraSchema checks that all required columns exist on synchro's
// infrastructure tables and adds any that are missing. This runs once during
// NewEngine and is the mechanism that replaces manual migration re-runs when
// upgrading the library.
func ensureInfraSchema(ctx context.Context, db *sql.DB, logger *slog.Logger) error {
	for _, col := range requiredInfraColumns {
		exists, err := columnExists(ctx, db, col.Table, col.Column)
		if err != nil {
			return fmt.Errorf("checking column %s.%s: %w", col.Table, col.Column, err)
		}
		if exists {
			continue
		}

		ddl := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s",
			quoteIdentifier(col.Table),
			quoteIdentifier(col.Column),
			col.DataType)
		if col.DefaultSQL != "" {
			ddl += " " + col.DefaultSQL
		}

		logger.Info("auto-adding missing infrastructure column",
			"table", col.Table, "column", col.Column)
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("adding column %s.%s: %w", col.Table, col.Column, err)
		}
	}
	return nil
}

// columnExists checks if a column exists on a table via information_schema.
func columnExists(ctx context.Context, db *sql.DB, table, column string) (bool, error) {
	var exists bool
	err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_name = $1 AND column_name = $2
		)`, table, column).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}
