package seeddb

import (
	"context"
	"encoding/json"
	"testing"
)

func TestPortableInt64KeyOnlyRowsPreserveValuesAndOverlap(t *testing.T) {
	ctx := context.Background()
	db := newCanonicalInternalSQLiteDatabase(t)
	table := localSchemaTable{
		TableName:  "keys",
		PrimaryKey: []string{"id"},
		Columns:    []localSchemaColumn{{FieldID: "key", Name: "id", LogicalType: "int64", IsPrimaryKey: true}},
	}
	if _, err := db.ExecContext(ctx, createTableSQL(table)); err != nil {
		t.Fatal(err)
	}
	values := []struct {
		wire string
		want int64
	}{
		{"-9223372036854775808", -9223372036854775808},
		{"0", 0},
		{"9007199254740993", 9007199254740993},
		{"9223372036854775807", 9223372036854775807},
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, value := range values {
		if _, err := encodeTypedValue("int64", value.wire, false); err != nil {
			t.Fatal(err)
		}
		record := portableSeedRecord{Row: map[string]any{"key": value.wire}}
		for range 2 {
			if err := upsertPortableRecord(ctx, tx, table, record); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	rows, err := db.QueryContext(ctx, "SELECT id, typeof(id) FROM keys ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for _, value := range values {
		if !rows.Next() {
			t.Fatal("missing stored integer")
		}
		var got int64
		var storage string
		if err := rows.Scan(&got, &storage); err != nil {
			t.Fatal(err)
		}
		if got != value.want || storage != "integer" {
			t.Fatalf("stored %d as %s, want exact integer %d", got, storage, value.want)
		}
	}
	if rows.Next() {
		t.Fatal("overlapping scope rows created duplicates")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []any{json.Number("1"), "9223372036854775808", "-9223372036854775809", "not-an-integer"} {
		if _, err := sqliteValue(table.Columns[0], invalid); err == nil {
			t.Fatalf("accepted invalid int64 input of type %T", invalid)
		}
	}
}

func TestSeedTriggerVerificationPreservesQuotedNames(t *testing.T) {
	for _, name := range []string{"order items", `order "items"`} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			db := newCanonicalInternalSQLiteDatabase(t)
			table := localSchemaTable{
				TableName:  name,
				PrimaryKey: []string{"id"},
				Columns:    []localSchemaColumn{{FieldID: "key", Name: "id", LogicalType: "string", IsPrimaryKey: true}},
			}
			if _, err := db.ExecContext(ctx, createTableSQL(table)); err != nil {
				t.Fatal(err)
			}
			for _, statement := range cdcTriggerSQL(table) {
				if _, err := db.ExecContext(ctx, statement); err != nil {
					t.Fatal(err)
				}
			}
			if err := verifySQLiteSchema(ctx, db, []localSchemaTable{table}); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "DROP TRIGGER "+quoteIdentifier("_synchro_cdc_update_"+name)); err != nil {
				t.Fatal(err)
			}
			if err := verifySQLiteSchema(ctx, db, []localSchemaTable{table}); err == nil {
				t.Fatal("accepted a missing trigger")
			}
		})
	}
}
