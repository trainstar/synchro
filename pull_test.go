package synchro

import "testing"

func TestDeduplicateEntries_NoDuplicates(t *testing.T) {
	entries := []ChangelogEntry{
		{Seq: 1, TableName: "orders", RecordID: "a", Operation: OpInsert},
		{Seq: 2, TableName: "orders", RecordID: "b", Operation: OpUpdate},
		{Seq: 3, TableName: "categories", RecordID: "c", Operation: OpDelete},
	}

	refs := deduplicateEntries(entries)
	if len(refs) != 3 {
		t.Fatalf("got %d refs, want 3", len(refs))
	}
	for i, ref := range refs {
		if ref.RecordID != entries[i].RecordID {
			t.Errorf("refs[%d].RecordID = %q, want %q", i, ref.RecordID, entries[i].RecordID)
		}
		if ref.Operation != entries[i].Operation {
			t.Errorf("refs[%d].Operation = %v, want %v", i, ref.Operation, entries[i].Operation)
		}
	}
}

func TestDeduplicateEntries_SameRecordDifferentOps(t *testing.T) {
	entries := []ChangelogEntry{
		{Seq: 1, TableName: "orders", RecordID: "a", Operation: OpInsert},
		{Seq: 2, TableName: "orders", RecordID: "a", Operation: OpUpdate},
		{Seq: 3, TableName: "orders", RecordID: "a", Operation: OpDelete},
	}

	refs := deduplicateEntries(entries)
	if len(refs) != 1 {
		t.Fatalf("got %d refs, want 1", len(refs))
	}
	if refs[0].Operation != OpDelete {
		t.Errorf("expected OpDelete, got %v", refs[0].Operation)
	}
	if refs[0].Seq != 3 {
		t.Errorf("expected seq 3, got %d", refs[0].Seq)
	}
}

func TestDeduplicateEntries_Interleaved(t *testing.T) {
	entries := []ChangelogEntry{
		{Seq: 1, TableName: "orders", RecordID: "a", Operation: OpInsert},
		{Seq: 2, TableName: "categories", RecordID: "b", Operation: OpInsert},
		{Seq: 3, TableName: "orders", RecordID: "a", Operation: OpUpdate},
		{Seq: 4, TableName: "orders", RecordID: "c", Operation: OpInsert},
		{Seq: 5, TableName: "categories", RecordID: "b", Operation: OpDelete},
	}

	refs := deduplicateEntries(entries)
	if len(refs) != 3 {
		t.Fatalf("got %d refs, want 3", len(refs))
	}

	// Position 0: orders:a → updated to OpUpdate at seq 3
	if refs[0].RecordID != "a" || refs[0].Operation != OpUpdate || refs[0].Seq != 3 {
		t.Errorf("refs[0] = {%s, %v, %d}, want {a, OpUpdate, 3}", refs[0].RecordID, refs[0].Operation, refs[0].Seq)
	}
	// Position 1: categories:b → updated to OpDelete at seq 5
	if refs[1].RecordID != "b" || refs[1].Operation != OpDelete || refs[1].Seq != 5 {
		t.Errorf("refs[1] = {%s, %v, %d}, want {b, OpDelete, 5}", refs[1].RecordID, refs[1].Operation, refs[1].Seq)
	}
	// Position 2: orders:c → kept at seq 4
	if refs[2].RecordID != "c" || refs[2].Operation != OpInsert || refs[2].Seq != 4 {
		t.Errorf("refs[2] = {%s, %v, %d}, want {c, OpInsert, 4}", refs[2].RecordID, refs[2].Operation, refs[2].Seq)
	}
}

func TestDeduplicateEntries_SingleEntry(t *testing.T) {
	entries := []ChangelogEntry{
		{Seq: 42, TableName: "orders", RecordID: "x", Operation: OpInsert},
	}

	refs := deduplicateEntries(entries)
	if len(refs) != 1 {
		t.Fatalf("got %d refs, want 1", len(refs))
	}
	if refs[0].RecordID != "x" || refs[0].Seq != 42 {
		t.Errorf("refs[0] = {%s, %d}, want {x, 42}", refs[0].RecordID, refs[0].Seq)
	}
}

func TestDeduplicateEntries_Empty(t *testing.T) {
	refs := deduplicateEntries(nil)
	if len(refs) != 0 {
		t.Errorf("got %d refs, want 0", len(refs))
	}
}

func TestBuildJsonPairs_EscapesSingleQuotes(t *testing.T) {
	// Column name with embedded single quote must not break the SQL string literal.
	result := buildJsonPairs([]string{"it's_col", "normal"})

	// Key must have doubled single quotes, value must be double-quote-escaped identifier.
	if expected := `'it''s_col', "it's_col", 'normal', "normal"`; result != expected {
		t.Errorf("buildJsonPairs =\n  %s\nwant:\n  %s", result, expected)
	}
}

func TestBuildJsonPairs_SafeColumns(t *testing.T) {
	result := buildJsonPairs([]string{"name", "age"})
	if expected := `'name', "name", 'age', "age"`; result != expected {
		t.Errorf("buildJsonPairs = %s, want %s", result, expected)
	}
}
