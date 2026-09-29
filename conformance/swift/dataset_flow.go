package swift

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/trainstar/synchro/conformance/dataset"
)

// DatasetPlatform runs the authored dataset flow through direct Swift runners.
type DatasetPlatform struct {
	Platform *Platform
	clients  map[string]Client
}

var _ dataset.NativePlatform = (*DatasetPlatform)(nil)

// Open installs a fresh Swift client and synchronizes it to idle.
func (d *DatasetPlatform) Open(ctx context.Context, key, user string) error {
	client := Client{Key: key, UserID: user, ClientID: key, DatabaseKey: key}
	if err := d.Platform.Install(ctx, client, "current", ""); err != nil {
		return err
	}
	if d.clients == nil {
		d.clients = map[string]Client{}
	}
	d.clients[key] = client
	return nil
}

// Write inserts one row through the runner's authored write transaction.
func (d *DatasetPlatform) Write(ctx context.Context, key string, write dataset.LocalWrite) error {
	state, err := d.Platform.client(d.clients[key])
	if err != nil {
		return err
	}
	primaryKey, err := json.Marshal(write.ID)
	if err != nil {
		return err
	}
	action := runnerLocalAction{Operation: "insert", TableName: write.Table, PrimaryKeyField: "id", PrimaryKey: primaryKey, Fields: map[string]json.RawMessage{}}
	for name, raw := range write.Columns {
		action.Fields[name] = raw
		// The runner command holds only portable JSON integers. A wider int64
		// travels as text, and the INTEGER column affinity stores the integer.
		if integer, err := strconv.ParseInt(string(raw), 10, 64); err == nil && (integer > warmConnectMaximumSafeInteger || integer < -warmConnectMaximumSafeInteger) {
			action.Fields[name] = json.RawMessage(`{"type":"string","value":"` + string(raw) + `"}`)
		}
		action.AuthoredColumns = append(action.AuthoredColumns, name)
	}
	for name, raw := range write.Support {
		action.Fields[name] = raw
	}
	sort.Strings(action.AuthoredColumns)
	if err := validateRunnerLocalAction(action); err != nil {
		return err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	result, err := state.session.Execute(ctx, Request{Operation: "local-action", LocalAction: &action})
	if err != nil {
		return fmt.Errorf("execute Swift dataset write: %w (runner reported: %s)", err, state.session.stderrReport())
	}
	if result.RowsAffected == nil || *result.RowsAffected != 1 {
		return errors.New("Swift dataset write did not affect one row")
	}
	return nil
}

// Synchronize runs one public start call to idle and stops the client.
func (d *DatasetPlatform) Synchronize(ctx context.Context, key string) error {
	state, err := d.Platform.client(d.clients[key])
	if err != nil {
		return err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	return d.Platform.initializeCurrent(ctx, state)
}

// Capture reads the selected local rows and the local application row count.
func (d *DatasetPlatform) Capture(ctx context.Context, key string, selectors []dataset.RowRef) ([]dataset.LocalRow, int, error) {
	state, err := d.Platform.client(d.clients[key])
	if err != nil {
		return nil, 0, err
	}
	runnerSelectors := make([]runnerRowSelector, 0, len(selectors))
	for _, selector := range selectors {
		primaryKey, err := json.Marshal(selector.ID)
		if err != nil {
			return nil, 0, err
		}
		runnerSelectors = append(runnerSelectors, runnerRowSelector{TableName: selector.Table, PrimaryKeyField: "id", PrimaryKey: primaryKey})
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	result, err := captureRunnerBatch(ctx, state, runnerSelectors)
	if err != nil {
		return nil, 0, err
	}
	if len(result.ApplicationRowStorageClasses) != len(result.ApplicationRows) {
		return nil, 0, errors.New("Swift dataset capture has no storage class for each row")
	}
	rows := make([]dataset.LocalRow, 0, len(result.ApplicationRows))
	for index, values := range result.ApplicationRows {
		rows = append(rows, dataset.LocalRow{Values: values, StorageClasses: result.ApplicationRowStorageClasses[index]})
	}
	return rows, *result.ApplicationRowCount, nil
}
