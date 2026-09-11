package kotlin

import (
	"encoding/json"
	"testing"
)

func TestKotlinLogicalApplicationRowsOmitsTombstones(t *testing.T) {
	rows := []map[string]json.RawMessage{
		{"id": json.RawMessage(`"live"`), "removed_on": json.RawMessage(`null`)},
		{"id": json.RawMessage(`"deleted"`), "removed_on": json.RawMessage(`"2026-09-11T00:00:00Z"`)},
		{"id": json.RawMessage(`"untracked"`), "deleted_at": json.RawMessage(`"application-value"`)},
	}
	lifecycles := []kotlinApplicationRowLifecycle{{PrimaryKeyField: "id", RecordID: "live", DeletedAtField: "removed_on"}, {PrimaryKeyField: "id", RecordID: "deleted", DeletedAtField: "removed_on"}}
	count, logicalRows, err := kotlinLogicalApplicationRows(3, rows, lifecycles)
	if err != nil {
		t.Fatalf("normalize application rows: %v", err)
	}
	if count != 2 || len(logicalRows) != 2 {
		t.Fatalf("logical rows = count %d rows %d, want count 2 rows 2", count, len(logicalRows))
	}
	for _, row := range logicalRows {
		if string(row["id"]) == `"deleted"` {
			t.Fatal("tombstone remained in logical application rows")
		}
	}
}

func TestKotlinLogicalApplicationRowsRejectsMalformedDeletedAt(t *testing.T) {
	_, _, err := kotlinLogicalApplicationRows(1, []map[string]json.RawMessage{{"id": json.RawMessage(`"row-a"`), "removed_on": json.RawMessage(`{`)}}, []kotlinApplicationRowLifecycle{{PrimaryKeyField: "id", RecordID: "row-a", DeletedAtField: "removed_on"}})
	if err == nil {
		t.Fatal("accepted malformed deleted_at JSON")
	}
}
