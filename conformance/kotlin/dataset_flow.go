package kotlin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/trainstar/synchro/conformance/dataset"
)

// DatasetPlatform runs the authored dataset flow through Kotlin instrumentation.
type DatasetPlatform struct {
	Platform *Platform
	clients  map[string]Client
}

var _ dataset.NativePlatform = (*DatasetPlatform)(nil)

// Open installs a fresh Kotlin client and synchronizes it to idle.
func (d *DatasetPlatform) Open(ctx context.Context, key, user string) error {
	client := Client{Key: key, UserID: user, ClientID: key, DatabaseKey: key}
	if err := d.Platform.Install(ctx, InstallRequest{Client: client, Initialization: "current"}); err != nil {
		return err
	}
	if d.clients == nil {
		d.clients = map[string]Client{}
	}
	d.clients[key] = client
	return nil
}

// Write inserts one row through the instrumentation's authored write transaction.
func (d *DatasetPlatform) Write(ctx context.Context, key string, write dataset.LocalWrite) error {
	state, err := d.Platform.clientFor(d.clients[key])
	if err != nil {
		return err
	}
	action := LocalAction{Operation: "insert", TableName: write.Table, PrimaryKeyField: "id", PrimaryKey: TypedValue{Type: "string", Value: write.ID}, Fields: map[string]TypedValue{}}
	for name, raw := range write.Columns {
		value, err := typedValue(raw, true)
		if err != nil {
			return fmt.Errorf("Kotlin Android dataset column %s: %w", name, err)
		}
		action.Fields[name] = value
		action.AuthoredColumns = append(action.AuthoredColumns, name)
	}
	for name, raw := range write.Support {
		value, err := typedValue(raw, true)
		if err != nil {
			return fmt.Errorf("Kotlin Android dataset support column %s: %w", name, err)
		}
		action.Fields[name] = value
	}
	sort.Strings(action.AuthoredColumns)
	state.mu.Lock()
	defer state.mu.Unlock()
	if err := state.available("dataset write"); err != nil {
		return err
	}
	result, err := state.session.Execute(ctx, Request{Operation: "local-action", LocalAction: &action})
	if err != nil {
		return fmt.Errorf("execute Kotlin Android dataset write: %w", err)
	}
	if result.RowsAffected == nil || *result.RowsAffected != 1 {
		return errors.New("Kotlin Android dataset write did not affect one row")
	}
	return state.advanceMaintenanceCursor(result)
}

// Synchronize runs one public start call to idle and stops the client.
func (d *DatasetPlatform) Synchronize(ctx context.Context, key string) error {
	state, err := d.Platform.clientFor(d.clients[key])
	if err != nil {
		return err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if err := state.available("dataset synchronization"); err != nil {
		return err
	}
	return d.Platform.initializeCurrent(ctx, state)
}

// Capture reads the selected local rows and the local application row count.
func (d *DatasetPlatform) Capture(ctx context.Context, key string, selectors []dataset.RowRef) ([]map[string]json.RawMessage, int, error) {
	state, err := d.Platform.clientFor(d.clients[key])
	if err != nil {
		return nil, 0, err
	}
	rowSelectors := make([]RowSelector, 0, len(selectors))
	for _, selector := range selectors {
		rowSelectors = append(rowSelectors, RowSelector{TableName: selector.Table, PrimaryKeyField: "id", PrimaryKey: TypedValue{Type: "string", Value: selector.ID}})
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if err := state.available("dataset capture"); err != nil {
		return nil, 0, err
	}
	result, err := captureClientStateBatch(ctx, state, rowSelectors)
	if err != nil {
		return nil, 0, err
	}
	if *result.ApplicationRowCount > maximumRows {
		return nil, *result.ApplicationRowCount, nil
	}
	rows, err := androidApplicationRows(result.ApplicationRows)
	if err != nil {
		return nil, 0, err
	}
	return rows, *result.ApplicationRowCount, nil
}
