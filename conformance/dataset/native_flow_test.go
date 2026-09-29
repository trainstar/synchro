package dataset

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

// sampleSource is one canonical source text for each portable type.
var sampleSource = map[string]string{
	"string":   "text",
	"int":      "7",
	"int64":    "9007199254740993",
	"decimal":  "1.5",
	"float":    "0.5",
	"datetime": "2026-03-02T06:30:00.123456Z",
	"date":     "2026-03-02",
	"json":     `{"a":1}`,
	"bytes":    "00ff",
}

// nativeImage builds a source row and its exact local copy for one identity.
func nativeImage(t *testing.T, tableName, id string, deleted bool) (map[string]*string, LocalRow) {
	t.Helper()
	table, found := LookupTable(tableName)
	if !found {
		t.Fatalf("unknown table %s", tableName)
	}
	source := map[string]*string{}
	local := LocalRow{Values: map[string]json.RawMessage{}, StorageClasses: map[string]string{}}
	for _, column := range table.Columns {
		text := sampleSource[column.Type]
		switch column.Name {
		case "id":
			text = id
		case "deleted_at":
			if !deleted {
				local.Values[column.Name], local.StorageClasses[column.Name] = json.RawMessage("null"), "null"
				continue
			}
		}
		source[column.Name] = &text
		local.StorageClasses[column.Name] = storageClass(column.Type)
		switch column.Type {
		case "int", "int64", "float":
			local.Values[column.Name] = json.RawMessage(text)
		case "bytes":
			decoded, err := hex.DecodeString(text)
			if err != nil {
				t.Fatal(err)
			}
			local.Values[column.Name], _ = json.Marshal(base64.RawURLEncoding.EncodeToString(decoded))
		default:
			local.Values[column.Name], _ = json.Marshal(text)
		}
	}
	return source, local
}

// TestNativeRowsRejectUnpermittedTombstones proves that an exact source image
// of a deleted row outside the user's authored deliveries fails membership.
func TestNativeRowsRejectUnpermittedTombstones(t *testing.T) {
	tables := map[string]string{}
	for _, selector := range authoredSelectors() {
		tables[selector.ID] = selector.Table
	}
	capture := func(user string, tombstones ...string) ([]LocalRow, map[string]map[string]*string) {
		sources := map[string]map[string]*string{}
		var rows []LocalRow
		for id := range checkpointIDs(AuthoredFinal, user) {
			source, row := nativeImage(t, tables[id], id, false)
			sources[id], rows = source, append(rows, row)
		}
		for _, id := range tombstones {
			source, row := nativeImage(t, tables[id], id, true)
			sources[id], rows = source, append(rows, row)
		}
		return rows, sources
	}
	for _, test := range []struct {
		name       string
		user       string
		previous   *Checkpoint
		tombstones []string
		rejected   []string
	}{
		{"dana without tombstones", Dana, &AuthoredInitial, nil, nil},
		{"dana with bob's deleted program", Dana, &AuthoredInitial, []string{ProgramBob}, []string{ProgramBob}},
		{"bob keeps his delivered tombstones", Bob, &AuthoredInitial, []string{MemberBobA, ProgramBob}, nil},
		{"a rebuilt bob holds no tombstone", Bob, nil, []string{MemberBobA, ProgramBob}, []string{MemberBobA, ProgramBob}},
	} {
		t.Run(test.name, func(t *testing.T) {
			rows, sources := capture(test.user, test.tombstones...)
			_, problems, _, err := compareNativeRows(test.user, rows, len(rows), tables, sources, checkpointIDs(AuthoredFinal, test.user), permittedTombstones(test.previous, test.user))
			if err != nil {
				t.Fatal(err)
			}
			if len(problems) != len(test.rejected) {
				t.Fatalf("problems = %q, want one for each of %v", problems, test.rejected)
			}
			for index, id := range test.rejected {
				if !strings.Contains(problems[index], "tombstone") || !strings.Contains(strings.Join(problems, "\n"), id) {
					t.Fatalf("problems = %q, want a tombstone rejection of %s", problems, id)
				}
			}
		})
	}
}
