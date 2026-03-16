package synchro

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// pushProcessor handles push operations.
type pushProcessor struct {
	registry  *Registry
	resolver  ConflictResolver
	changelog *changelogStore
	hooks     Hooks
	logger    *slog.Logger
}

// processPush processes a single push record within a transaction.
func (p *pushProcessor) processPush(ctx context.Context, tx DB, userID string, record *PushRecord) (*PushResult, error) {
	cfg := p.registry.Get(record.TableName)
	if cfg == nil {
		return &PushResult{
			ID: record.ID, TableName: record.TableName, Operation: record.Operation,
			Status: PushStatusRejectedTerminal, ReasonCode: "table_not_registered", Message: "table not registered for sync",
		}, nil
	}

	if !p.registry.IsPushable(record.TableName) {
		return &PushResult{
			ID: record.ID, TableName: record.TableName, Operation: record.Operation,
			Status: PushStatusRejectedTerminal, ReasonCode: "table_read_only", Message: "table is read-only",
		}, nil
	}

	op, ok := ParseOperation(record.Operation)
	if !ok {
		return &PushResult{
			ID: record.ID, TableName: record.TableName, Operation: record.Operation,
			Status: PushStatusRejectedTerminal, ReasonCode: "invalid_operation", Message: fmt.Sprintf("unknown operation: %s", record.Operation),
		}, nil
	}

	switch op {
	case OpInsert:
		return p.pushCreate(ctx, tx, userID, cfg, record)
	case OpUpdate:
		return p.pushUpdate(ctx, tx, userID, cfg, record)
	case OpDelete:
		return p.pushDelete(ctx, tx, userID, cfg, record)
	default:
		return &PushResult{
			ID: record.ID, TableName: record.TableName, Operation: record.Operation,
			Status: PushStatusRejectedTerminal, ReasonCode: "unsupported_operation", Message: "unsupported operation",
		}, nil
	}
}

// pushCreate handles a create operation.
func (p *pushProcessor) pushCreate(ctx context.Context, tx DB, userID string, cfg *TableConfig, record *PushRecord) (*PushResult, error) {
	existing, err := getRecordByID(ctx, tx, cfg, record.ID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("checking existing record: %w", err)
	}

	if existing != nil {
		// Resurrection: if the table supports soft deletes and the record is deleted,
		// treat as update to resurrect. When HasDeletedAt is false, DeletedAt is always nil.
		if existing.DeletedAt != nil {
			return p.pushUpdate(ctx, tx, userID, cfg, record)
		}

		serverVersion := &Record{
			ID: existing.ID, TableName: record.TableName,
			Data: existing.Data, UpdatedAt: existing.UpdatedAt,
		}

		return &PushResult{
			ID: record.ID, TableName: record.TableName, Operation: record.Operation,
			Status: PushStatusConflict, ReasonCode: "record_exists", Message: "record already exists",
			ServerVersion: serverVersion,
		}, nil
	}

	// Parse data and enforce ownership
	var data map[string]any
	if err := json.Unmarshal(record.Data, &data); err != nil {
		return &PushResult{
			ID: record.ID, TableName: record.TableName, Operation: record.Operation,
			Status: PushStatusRejectedTerminal, ReasonCode: "invalid_data", Message: "invalid data format",
		}, nil
	}

	if cfg.OwnerColumn != "" {
		data[cfg.OwnerColumn] = userID
	}

	// Set the ID
	data[cfg.IDColumn] = record.ID

	ts, err := insertRecord(ctx, tx, cfg, data)
	if err != nil {
		p.logger.ErrorContext(ctx, "failed to insert record",
			"err", err, "table", record.TableName, "id", record.ID)
		return &PushResult{
			ID: record.ID, TableName: record.TableName, Operation: record.Operation,
			Status: PushStatusRejectedRetryable, ReasonCode: "create_failed", Message: "failed to create record",
		}, nil
	}

	return &PushResult{
		ID: record.ID, TableName: record.TableName, Operation: record.Operation,
		Status:          PushStatusApplied,
		ServerUpdatedAt: ts,
	}, nil
}

// pushUpdate handles an update operation with conflict resolution.
func (p *pushProcessor) pushUpdate(ctx context.Context, tx DB, userID string, cfg *TableConfig, record *PushRecord) (*PushResult, error) {
	existing, err := getRecordByID(ctx, tx, cfg, record.ID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return &PushResult{
				ID: record.ID, TableName: record.TableName, Operation: record.Operation,
				Status: PushStatusRejectedTerminal, ReasonCode: "record_not_found", Message: "record not found",
			}, nil
		}
		return nil, fmt.Errorf("getting existing record: %w", err)
	}

	// Check if record is deleted (can't update deleted records unless resurrecting)
	if existing.DeletedAt != nil && record.Operation != "create" {
		serverVersion := &Record{
			ID: existing.ID, TableName: record.TableName,
			Data: existing.Data, UpdatedAt: existing.UpdatedAt,
			DeletedAt: existing.DeletedAt,
		}
		return &PushResult{
			ID: record.ID, TableName: record.TableName, Operation: record.Operation,
			Status: PushStatusConflict, ReasonCode: "record_deleted", Message: "record is deleted",
			ServerVersion: serverVersion,
		}, nil
	}

	// Conflict resolution: only possible when the table has updated_at for LWW.
	// Without updated_at, last push wins unconditionally.
	if cfg.HasUpdatedAt() {
		var serverTime time.Time
		if existing.UpdatedAt != nil {
			serverTime = *existing.UpdatedAt
		}

		conflict := Conflict{
			Table:      record.TableName,
			RecordID:   record.ID,
			UserID:     userID,
			ClientData: record.Data,
			ServerData: existing.Data,
			ClientTime: record.ClientUpdatedAt,
			ServerTime: serverTime,
		}
		if record.BaseUpdatedAt != nil {
			conflict.BaseVersion = record.BaseUpdatedAt
		}

		resolution, err := p.resolver.Resolve(ctx, conflict)
		if err != nil {
			return nil, fmt.Errorf("resolving conflict: %w", err)
		}

		if p.hooks.OnConflict != nil {
			p.hooks.OnConflict(ctx, conflict, resolution)
		}

		if resolution.Winner == "server" {
			serverVersion := &Record{
				ID: existing.ID, TableName: record.TableName,
				Data: existing.Data, UpdatedAt: existing.UpdatedAt,
			}
			return &PushResult{
				ID: record.ID, TableName: record.TableName, Operation: record.Operation,
				Status: PushStatusConflict, ReasonCode: "server_won_conflict", Message: resolution.Reason,
				ServerVersion: serverVersion,
			}, nil
		}
	}

	// Parse data
	var data map[string]any
	if err := json.Unmarshal(record.Data, &data); err != nil {
		return &PushResult{
			ID: record.ID, TableName: record.TableName, Operation: record.Operation,
			Status: PushStatusRejectedTerminal, ReasonCode: "invalid_data", Message: "invalid data format",
		}, nil
	}

	// Resurrection: if this is a create on a deleted record, clear deleted_at
	isResurrection := record.Operation == "create" && existing.DeletedAt != nil

	ts, err := updateRecord(ctx, tx, cfg, record.ID, data)
	if err != nil {
		p.logger.ErrorContext(ctx, "failed to update record",
			"err", err, "table", record.TableName, "id", record.ID)
		return &PushResult{
			ID: record.ID, TableName: record.TableName, Operation: record.Operation,
			Status: PushStatusRejectedRetryable, ReasonCode: "update_failed", Message: "failed to update record",
		}, nil
	}

	if isResurrection && cfg.HasDeletedAt() {
		if cfg.HasUpdatedAt() {
			clearQuery := fmt.Sprintf("UPDATE %s SET %s = NULL WHERE %s = $1 RETURNING %s",
				quoteIdentifier(cfg.TableName), quoteIdentifier(cfg.DeletedAtCol()),
				quoteIdentifier(cfg.IDColumn), quoteIdentifier(cfg.UpdatedAtCol()))
			var resTS time.Time
			if err := tx.QueryRowContext(ctx, clearQuery, record.ID).Scan(&resTS); err != nil {
				p.logger.ErrorContext(ctx, "failed to clear deleted_at for resurrection",
					"err", err, "table", record.TableName, "id", record.ID)
				return &PushResult{
					ID: record.ID, TableName: record.TableName, Operation: record.Operation,
					Status: PushStatusRejectedRetryable, ReasonCode: "resurrection_failed", Message: "failed to resurrect record",
				}, nil
			}
			ts = &resTS
		} else {
			clearQuery := fmt.Sprintf("UPDATE %s SET %s = NULL WHERE %s = $1",
				quoteIdentifier(cfg.TableName), quoteIdentifier(cfg.DeletedAtCol()),
				quoteIdentifier(cfg.IDColumn))
			if _, err := tx.ExecContext(ctx, clearQuery, record.ID); err != nil {
				p.logger.ErrorContext(ctx, "failed to clear deleted_at for resurrection",
					"err", err, "table", record.TableName, "id", record.ID)
				return &PushResult{
					ID: record.ID, TableName: record.TableName, Operation: record.Operation,
					Status: PushStatusRejectedRetryable, ReasonCode: "resurrection_failed", Message: "failed to resurrect record",
				}, nil
			}
		}
	}

	return &PushResult{
		ID: record.ID, TableName: record.TableName, Operation: record.Operation,
		Status:          PushStatusApplied,
		ServerUpdatedAt: ts,
	}, nil
}

// pushDelete handles a delete operation (soft delete when deleted_at exists, hard delete otherwise).
func (p *pushProcessor) pushDelete(ctx context.Context, tx DB, _ string, cfg *TableConfig, record *PushRecord) (*PushResult, error) {
	if cfg.HasDeletedAt() {
		return p.pushSoftDelete(ctx, tx, cfg, record)
	}
	return p.pushHardDelete(ctx, tx, cfg, record)
}

// pushSoftDelete performs a soft delete by setting deleted_at = now().
func (p *pushProcessor) pushSoftDelete(ctx context.Context, tx DB, cfg *TableConfig, record *PushRecord) (*PushResult, error) {
	existing, err := getRecordByID(ctx, tx, cfg, record.ID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return &PushResult{
				ID: record.ID, TableName: record.TableName, Operation: record.Operation,
				Status: PushStatusRejectedTerminal, ReasonCode: "record_not_found", Message: "record not found or not accessible",
			}, nil
		}
		return nil, fmt.Errorf("getting existing record: %w", err)
	}

	// Already deleted — idempotent success
	if existing.DeletedAt != nil {
		return &PushResult{
			ID: record.ID, TableName: record.TableName, Operation: record.Operation,
			Status: PushStatusApplied, ReasonCode: "already_deleted", Message: "record already deleted",
		}, nil
	}

	query := fmt.Sprintf("UPDATE %s SET %s = now() WHERE %s = $1 RETURNING %s",
		quoteIdentifier(cfg.TableName), quoteIdentifier(cfg.DeletedAtCol()),
		quoteIdentifier(cfg.IDColumn), quoteIdentifier(cfg.DeletedAtCol()))

	var deletedAt time.Time
	if err := tx.QueryRowContext(ctx, query, record.ID).Scan(&deletedAt); err != nil {
		p.logger.ErrorContext(ctx, "failed to delete record",
			"err", err, "table", record.TableName, "id", record.ID)
		return &PushResult{
			ID: record.ID, TableName: record.TableName, Operation: record.Operation,
			Status: PushStatusRejectedRetryable, ReasonCode: "delete_failed", Message: "failed to delete record",
		}, nil
	}

	return &PushResult{
		ID: record.ID, TableName: record.TableName, Operation: record.Operation,
		Status:          PushStatusApplied,
		ServerDeletedAt: &deletedAt,
	}, nil
}

// pushHardDelete performs a hard DELETE when the table has no deleted_at column.
func (p *pushProcessor) pushHardDelete(ctx context.Context, tx DB, cfg *TableConfig, record *PushRecord) (*PushResult, error) {
	query := fmt.Sprintf("DELETE FROM %s WHERE %s = $1",
		quoteIdentifier(cfg.TableName), quoteIdentifier(cfg.IDColumn))

	result, err := tx.ExecContext(ctx, query, record.ID)
	if err != nil {
		p.logger.ErrorContext(ctx, "failed to hard delete record",
			"err", err, "table", record.TableName, "id", record.ID)
		return &PushResult{
			ID: record.ID, TableName: record.TableName, Operation: record.Operation,
			Status: PushStatusRejectedRetryable, ReasonCode: "delete_failed", Message: "failed to delete record",
		}, nil
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		// Record not found — idempotent: treat as already deleted
		return &PushResult{
			ID: record.ID, TableName: record.TableName, Operation: record.Operation,
			Status: PushStatusApplied, ReasonCode: "already_deleted", Message: "record already deleted",
		}, nil
	}

	return &PushResult{
		ID: record.ID, TableName: record.TableName, Operation: record.Operation,
		Status: PushStatusApplied,
	}, nil
}

// existingRecord holds data about a record for conflict detection.
type existingRecord struct {
	ID        string
	Data      json.RawMessage
	UpdatedAt *time.Time
	DeletedAt *time.Time
}

// getRecordByID retrieves a record by ID for conflict detection.
// Uses NULL casts for timestamp columns that don't exist on the table.
func getRecordByID(ctx context.Context, db DB, cfg *TableConfig, id string) (*existingRecord, error) {
	uaExpr := "NULL::timestamptz"
	if cfg.HasUpdatedAt() {
		uaExpr = quoteIdentifier(cfg.UpdatedAtCol())
	}
	daExpr := "NULL::timestamptz"
	if cfg.HasDeletedAt() {
		daExpr = quoteIdentifier(cfg.DeletedAtCol())
	}

	query := fmt.Sprintf(
		"SELECT %s::text as id, row_to_json(t)::text as data, %s as updated_at, %s as deleted_at FROM %s t WHERE %s = $1",
		quoteIdentifier(cfg.IDColumn), uaExpr, daExpr,
		quoteIdentifier(cfg.TableName), quoteIdentifier(cfg.IDColumn),
	)

	var (
		recID     string
		dataStr   string
		updatedAt sql.NullTime
		deletedAt sql.NullTime
	)

	err := db.QueryRowContext(ctx, query, id).Scan(&recID, &dataStr, &updatedAt, &deletedAt)
	if err != nil {
		return nil, err
	}

	rec := &existingRecord{
		ID:   recID,
		Data: json.RawMessage(dataStr),
	}
	if updatedAt.Valid {
		rec.UpdatedAt = &updatedAt.Time
	}
	if deletedAt.Valid {
		rec.DeletedAt = &deletedAt.Time
	}
	return rec, nil
}

// insertRecord inserts a new record from sync data and returns the server-assigned updated_at.
// Uses the deny-list model: all columns are allowed except protected ones.
// Returns nil when the table has no updated_at column.
func insertRecord(ctx context.Context, db DB, cfg *TableConfig, data map[string]any) (*time.Time, error) {
	// Collect column names from data
	dataCols := make([]string, 0, len(data))
	for col := range data {
		dataCols = append(dataCols, col)
	}

	// Filter to allowed insert columns
	allowed := cfg.AllowedInsertColumns(dataCols)
	if len(allowed) == 0 {
		return nil, fmt.Errorf("no allowed columns for insert on table %q", cfg.TableName)
	}

	columns := make([]string, 0, len(allowed))
	placeholders := make([]string, 0, len(allowed))
	values := make([]any, 0, len(allowed))

	for i, col := range allowed {
		columns = append(columns, quoteIdentifier(col))
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
		values = append(values, data[col])
	}

	if cfg.HasUpdatedAt() {
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) RETURNING %s",
			quoteIdentifier(cfg.TableName),
			strings.Join(columns, ", "),
			strings.Join(placeholders, ", "),
			quoteIdentifier(cfg.UpdatedAtCol()))

		var ts time.Time
		if err := db.QueryRowContext(ctx, query, values...).Scan(&ts); err != nil {
			return nil, err
		}
		return &ts, nil
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		quoteIdentifier(cfg.TableName),
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	_, err := db.ExecContext(ctx, query, values...)
	return nil, err
}

// updateRecord updates a record from sync data and returns the server-assigned updated_at.
// Uses the deny-list model: protected columns are silently dropped.
// Returns nil when all columns are protected (nothing to update) or the table has no updated_at.
func updateRecord(ctx context.Context, db DB, cfg *TableConfig, id string, data map[string]any) (*time.Time, error) {
	dataCols := make([]string, 0, len(data))
	for col := range data {
		dataCols = append(dataCols, col)
	}

	allowed := cfg.AllowedUpdateColumns(dataCols)
	if len(allowed) == 0 {
		return nil, nil // Nothing to update
	}

	setClauses := make([]string, 0, len(allowed))
	values := make([]any, 0, len(allowed)+1)

	for i, col := range allowed {
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", quoteIdentifier(col), i+1))
		values = append(values, data[col])
	}

	values = append(values, id)

	if cfg.HasUpdatedAt() {
		query := fmt.Sprintf("UPDATE %s SET %s WHERE %s = $%d RETURNING %s",
			quoteIdentifier(cfg.TableName),
			strings.Join(setClauses, ", "),
			quoteIdentifier(cfg.IDColumn),
			len(values),
			quoteIdentifier(cfg.UpdatedAtCol()))

		var ts time.Time
		if err := db.QueryRowContext(ctx, query, values...).Scan(&ts); err != nil {
			return nil, err
		}
		return &ts, nil
	}

	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s = $%d",
		quoteIdentifier(cfg.TableName),
		strings.Join(setClauses, ", "),
		quoteIdentifier(cfg.IDColumn),
		len(values))

	_, err := db.ExecContext(ctx, query, values...)
	return nil, err
}

// SetAuthContext sets the RLS auth context for push operations.
// Uses set_config() because SET LOCAL does not accept parameterized values.
func SetAuthContext(ctx context.Context, tx DB, userID string) error {
	_, err := tx.ExecContext(ctx, "SELECT set_config('app.user_id', $1, true)", userID)
	return err
}
