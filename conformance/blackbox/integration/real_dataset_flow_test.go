package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/dataset"
)

// TestRealDatasetAuthoredFlow runs the authored training dataset through the
// real extension and adapter. Each checkpoint compares three independent
// sources: the hand-written expectation, the authored business rule over
// live source rows, and the exact rows that each user received.
func TestRealDatasetAuthoredFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	harness := provisionRealDatasetHarness(t, ctx)
	runtime := newDatasetRuntime(t, ctx, harness)

	runtime.applyTransaction([]dataset.Statement{{SQL: dataset.AuthoredSeed.SQL}})
	runtime.applyAssignments(dataset.AuthoredSeed.Grants, nil)
	runtime.waitMaterialized(2 * time.Minute)

	clients := make(map[string]*datasetClient, len(dataset.AuthoredUsers))
	for _, user := range dataset.AuthoredUsers {
		clients[user] = runtime.connect(user, "dataset-incremental-"+user)
	}
	requireDatasetManifest(t, clients[dataset.Alice])

	push := dataset.AuthoredSetPush
	accepted, rejected, _ := runtime.push(clients[push.User], []map[string]any{
		clients[push.User].mutation(t, push.Table, push.ID, push.Op, "", push.Columns),
	})
	if len(accepted) != 1 || len(rejected) != 0 || accepted[0].Status != "applied" || accepted[0].ServerVersion == "" {
		t.Fatalf("authored push outcome: accepted=%+v rejected=%+v", accepted, rejected)
	}
	runtime.waitMaterialized(time.Minute)
	requireDatasetOutcomeRow(t, runtime, clients[push.User], push.Table, push.ID, accepted[0].ServerRow)

	requireDatasetCheckpoint(t, runtime, clients, dataset.AuthoredInitial)

	for _, step := range dataset.AuthoredHistory {
		runtime.applyTransaction([]dataset.Statement{{SQL: step.SQL}})
		runtime.applyAssignments(step.Grants, step.Revokes)
		runtime.waitMaterialized(time.Minute)
		// Every client reads each history step incrementally.
		for _, user := range dataset.AuthoredUsers {
			t.Logf("history step %s: %s reconnects and pulls", step.Name, user)
			runtime.reconnect(clients[user])
			runtime.pull(clients[user])
		}
	}
	requireDatasetCheckpoint(t, runtime, clients, dataset.AuthoredFinal)

	fresh := make(map[string]*datasetClient, len(dataset.AuthoredUsers))
	for _, user := range dataset.AuthoredUsers {
		fresh[user] = runtime.connect(user, "dataset-rebuild-"+user)
	}
	requireDatasetCheckpoint(t, runtime, fresh, dataset.AuthoredFinal)
}

// requireDatasetManifest checks the synced fields and portable types that the
// registration contract defines, including excluded generated columns.
func requireDatasetManifest(t *testing.T, client *datasetClient) {
	t.Helper()
	for _, table := range dataset.Tables {
		manifest := client.Tables[table.Name]
		if manifest == nil || manifest.PKField != manifest.FieldIDs["id"] {
			t.Fatalf("dataset manifest table %s is missing or has the wrong key", table.Name)
		}
		if len(manifest.Types) != len(table.Columns) {
			t.Fatalf("dataset manifest table %s has fields %v, want %v", table.Name, manifest.Types, table.Columns)
		}
		for _, column := range table.Columns {
			if manifest.Types[column.Name] != column.Type {
				t.Fatalf("dataset manifest field %s.%s type = %q, want %q", table.Name, column.Name, manifest.Types[column.Name], column.Type)
			}
		}
	}
}

func requireDatasetOutcomeRow(t *testing.T, runtime *datasetRuntime, client *datasetClient, tableName, id string, row map[string]json.RawMessage) {
	t.Helper()
	table, _ := dataset.LookupTable(tableName)
	manifest := client.Tables[tableName]
	source := runtime.expected().rows[tableName+"/"+id]
	if source == nil || len(row) != len(table.Columns) {
		t.Fatalf("push outcome row %s/%s has %d fields or no source row", tableName, id, len(row))
	}
	for _, column := range table.Columns {
		if err := dataset.CompareWire(column.Type, row[manifest.FieldIDs[column.Name]], source[column.Name]); err != nil {
			t.Fatalf("push outcome field %s: %v", column.Name, err)
		}
	}
}

func requireDatasetCheckpoint(t *testing.T, runtime *datasetRuntime, clients map[string]*datasetClient, checkpoint dataset.Checkpoint) {
	t.Helper()
	expected := runtime.expected()
	// The authored rule over live source rows must equal the hand-written rows.
	for scope, tables := range checkpoint.Rows {
		want := map[string]bool{}
		for table, ids := range tables {
			for _, id := range ids {
				want[table+"/"+id] = true
			}
		}
		got := expected.scopes[scope]
		if len(got) != len(want) {
			t.Fatalf("source rule scope %s has %d rows, hand-written %d", scope, len(got), len(want))
		}
		for key := range want {
			if !got[key] {
				t.Fatalf("source rule scope %s lacks hand-written row %s", scope, key)
			}
		}
	}
	for _, user := range dataset.AuthoredUsers {
		assigned := runtime.assignedScopes(user)
		if !slices.Equal(assigned, checkpoint.Assigned[user]) {
			t.Fatalf("source assignment for %s = %v, hand-written %v", user, assigned, checkpoint.Assigned[user])
		}
		runtime.converge(clients[user], expected, time.Minute)
	}
	for _, value := range checkpoint.Values {
		key := value.Table + "/" + value.ID
		found := false
		for _, client := range clients {
			for _, rows := range client.Rows {
				row, ok := rows[key]
				if !ok {
					continue
				}
				found = true
				var compact bytes.Buffer
				if err := json.Compact(&compact, row[value.Column]); err != nil || compact.String() != value.Wire {
					t.Fatalf("%s.%s wire = %s, hand-written %s", key, value.Column, row[value.Column], value.Wire)
				}
			}
		}
		if !found {
			t.Fatalf("hand-written value row %s was not delivered", key)
		}
	}
	for user, client := range clients {
		for scope := range client.Rows {
			if !strings.HasPrefix(scope, "user:") || scope == "user:"+user {
				continue
			}
			t.Fatalf("%s received another private scope %s", user, scope)
		}
	}
}
