package seeddb

import (
	"context"
	"database/sql"
	"encoding/hex"
	"strings"
	"testing"
)

func TestVerifyPortableSeedRecordUsesProtocolThreeDigest(t *testing.T) {
	env := manifestEnvelope{SchemaHash: "0000000000000000000000000000000000000000000000000000000000000000"}
	table := localSchemaTable{
		TableID:   "tbl_items",
		TableName: "items",
		Columns: []localSchemaColumn{
			{FieldID: "fld_id", Name: "id", LogicalType: "string", IsPrimaryKey: true},
			{FieldID: "fld_title", Name: "title", LogicalType: "string"},
		},
	}
	record := portableSeedRecord{
		Table: "tbl_items",
		PK:    map[string]any{"fld_id": "row-1"},
		Row: map[string]any{
			"fld_id":    "row-1",
			"fld_title": "Plan",
		},
		ServerVersion: "sv-1",
		RowChecksum: checksumObject{
			Algorithm: "sha256",
			Version:   1,
			Encoding:  "hex",
			Digest:    "ed32f93c5db56e975bf3d70360fff9d8cad423b2e6df7d4a6cae5ec46b83140e",
		},
	}

	recordID, row, err := verifyPortableSeedRecord(env, table, record)
	if err != nil {
		t.Fatalf("verify portable row: %v", err)
	}
	if recordID != "row-1" {
		t.Fatalf("record ID = %q, want row-1", recordID)
	}
	wantIdentity, err := hex.DecodeString("73796e6368726f3a76333a726f772d6964656e746974793a763100000000000000000974626c5f6974656d730000000000000006666c645f696401010000000000000005726f772d31")
	if err != nil {
		t.Fatalf("decode identity fixture: %v", err)
	}
	if string(row.identity) != string(wantIdentity) {
		t.Fatalf("row identity = %x, want %x", row.identity, wantIdentity)
	}

	expectedScope := checksumObject{
		Algorithm: "sha256",
		Version:   1,
		Encoding:  "hex",
		Digest:    "76d6992a9ea67c0fcfe9d7299e3f5cc0112cb9ce1439c5bb49399f5497054fb0",
	}
	if err := verifyScopeDigest(env.SchemaHash, "global", []seedDigestRow{row}, expectedScope); err != nil {
		t.Fatalf("verify scope digest: %v", err)
	}

	record.ServerVersion = "changed"
	if _, _, err := verifyPortableSeedRecord(env, table, record); err == nil {
		t.Fatal("changed server version retained the row digest")
	}
}

func TestChecksumObjectRejectsMissingStructuredChecksum(t *testing.T) {
	if err := (checksumObject{}).validate(); err == nil {
		t.Fatal("empty structured checksum was accepted")
	}
}

func TestVerifySQLiteInternalSchemaRejectsCorruption(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(t *testing.T, db *sql.DB)
	}{
		{
			name: "empty pending queue without primary key",
			mutate: func(t *testing.T, db *sql.DB) {
				_, err := db.Exec(`
					CREATE TABLE _synchro_pending_changes_corrupt (
						record_id TEXT NOT NULL,
						table_name TEXT NOT NULL,
						operation TEXT NOT NULL,
						base_updated_at TEXT,
						client_updated_at TEXT NOT NULL
					);
					DROP TABLE _synchro_pending_changes;
					ALTER TABLE _synchro_pending_changes_corrupt RENAME TO _synchro_pending_changes;
				`)
				if err != nil {
					t.Fatalf("remove pending queue primary key: %v", err)
				}
			},
		},
		{
			name: "scope row index missing",
			mutate: func(t *testing.T, db *sql.DB) {
				if _, err := db.Exec("DROP INDEX idx_synchro_scope_rows_record"); err != nil {
					t.Fatalf("remove scope row index: %v", err)
				}
			},
		},
		{
			name: "scope row index expression changed",
			mutate: func(t *testing.T, db *sql.DB) {
				recreateScopeRowsIndex(t, db, "(lower(table_name), record_id)")
			},
		},
		{
			name: "scope row index collation changed",
			mutate: func(t *testing.T, db *sql.DB) {
				recreateScopeRowsIndex(t, db, "(table_name COLLATE NOCASE, record_id)")
			},
		},
		{
			name: "scope row index sort order changed",
			mutate: func(t *testing.T, db *sql.DB) {
				recreateScopeRowsIndex(t, db, "(table_name DESC, record_id)")
			},
		},
		{
			name: "scope row index partial predicate changed",
			mutate: func(t *testing.T, db *sql.DB) {
				recreateScopeRowsIndex(t, db, "(table_name, record_id) WHERE table_name IS NOT NULL")
			},
		},
		{
			name: "scope row index SQL shape changed",
			mutate: func(t *testing.T, db *sql.DB) {
				recreateScopeRowsIndex(t, db, `("table_name", "record_id")`)
			},
		},
		{
			name: "scope checksum type changed",
			mutate: func(t *testing.T, db *sql.DB) {
				_, err := db.Exec(`
					CREATE TABLE _synchro_scopes_corrupt (
						scope_id TEXT PRIMARY KEY,
						cursor TEXT,
						checksum INTEGER,
						generation INTEGER NOT NULL DEFAULT 0,
						local_checksum TEXT NOT NULL DEFAULT ''
					);
					DROP TABLE _synchro_scopes;
					ALTER TABLE _synchro_scopes_corrupt RENAME TO _synchro_scopes;
				`)
				if err != nil {
					t.Fatalf("change scope checksum type: %v", err)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := newCanonicalInternalSQLiteDatabase(t)
			test.mutate(t, db)
			if err := verifySQLiteSchema(context.Background(), db, nil); err == nil {
				t.Fatal("accepted corrupt internal schema")
			}
		})
	}
}

func recreateScopeRowsIndex(t *testing.T, db *sql.DB, columns string) {
	t.Helper()
	if _, err := db.Exec("DROP INDEX idx_synchro_scope_rows_record; CREATE INDEX idx_synchro_scope_rows_record ON _synchro_scope_rows " + columns); err != nil {
		t.Fatalf("recreate scope row index: %v", err)
	}
}

func newCanonicalInternalSQLiteDatabase(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin sqlite schema transaction: %v", err)
	}
	if err := createInternalTables(context.Background(), tx); err != nil {
		_ = tx.Rollback()
		t.Fatalf("create canonical internal schema: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit canonical internal schema: %v", err)
	}
	return db
}

func TestSchemaManifestRejectsChangedBodyWithStaleHash(t *testing.T) {
	env := validManifestEnvelope(t)
	env.Manifest.Tables[0].Fields[0].Name = "changed"

	if err := env.validate(); err == nil {
		t.Fatal("changed schema manifest body retained its hash")
	}
}

func TestSchemaManifestRejectsRehashedSemanticMutants(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*manifestEnvelope)
	}{
		{
			name: "decimal metadata is incomplete",
			mutate: func(env *manifestEnvelope) {
				env.Manifest.Tables[0].Fields[1].Type = "decimal"
			},
		},
		{
			name: "decimal scale exceeds precision",
			mutate: func(env *manifestEnvelope) {
				precision, scale := 4, 5
				env.Manifest.Tables[0].Fields[1].Type = "decimal"
				env.Manifest.Tables[0].Fields[1].Precision = &precision
				env.Manifest.Tables[0].Fields[1].Scale = &scale
			},
		},
		{
			name: "non-decimal field has decimal metadata",
			mutate: func(env *manifestEnvelope) {
				precision, scale := 4, 2
				env.Manifest.Tables[0].Fields[1].Precision = &precision
				env.Manifest.Tables[0].Fields[1].Scale = &scale
			},
		},
		{
			name: "primary key is writable",
			mutate: func(env *manifestEnvelope) {
				env.Manifest.Tables[0].Fields[0].Writable = true
			},
		},
		{
			name: "lifecycle field is missing",
			mutate: func(env *manifestEnvelope) {
				fieldID := "missing-field"
				env.Manifest.Tables[0].Lifecycle.UpdatedAtFieldID = &fieldID
			},
		},
		{
			name: "lifecycle field has the wrong type",
			mutate: func(env *manifestEnvelope) {
				fieldID := env.Manifest.Tables[0].Fields[1].ID
				env.Manifest.Tables[0].Lifecycle.UpdatedAtFieldID = &fieldID
			},
		},
		{
			name: "lifecycle field is writable",
			mutate: func(env *manifestEnvelope) {
				fieldID := env.Manifest.Tables[0].Fields[1].ID
				env.Manifest.Tables[0].Fields[1].Type = "datetime"
				env.Manifest.Tables[0].Fields[1].Writable = true
				env.Manifest.Tables[0].Lifecycle.UpdatedAtFieldID = &fieldID
			},
		},
		{
			name: "relation identity is duplicated",
			mutate: func(env *manifestEnvelope) {
				env.Manifest.Tables[1].RelationID = env.Manifest.Tables[0].RelationID
			},
		},
		{
			name: "parent schema is not older",
			mutate: func(env *manifestEnvelope) {
				env.Manifest.TransitionClass = "class_3"
				env.Manifest.ParentSchema = &schemaRef{
					Version: env.SchemaVersion,
					Hash:    strings.Repeat("1", 64),
				}
			},
		},
		{
			name: "non-class-2 compatibility floor is older",
			mutate: func(env *manifestEnvelope) {
				env.SchemaVersion = 2
				env.Manifest.SchemaVersion = 2
				env.Manifest.CompatibilityFloor = 1
			},
		},
		{
			name: "class-2 compatibility floor does not extend parent lineage",
			mutate: func(env *manifestEnvelope) {
				env.SchemaVersion = 2
				env.Manifest.SchemaVersion = 2
				env.Manifest.TransitionClass = "class_2"
				env.Manifest.CompatibilityFloor = 2
				env.Manifest.ParentSchema = &schemaRef{Version: 1, Hash: strings.Repeat("1", 64)}
			},
		},
		{
			name: "index name is duplicated",
			mutate: func(env *manifestEnvelope) {
				env.Manifest.Tables[0].Indexes[1].Name = env.Manifest.Tables[0].Indexes[0].Name
			},
		},
		{
			name: "index field is duplicated",
			mutate: func(env *manifestEnvelope) {
				env.Manifest.Tables[0].Indexes[0].FieldIDs = []string{"field-a", "field-a"}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			env := validManifestEnvelope(t)
			test.mutate(&env)
			rehashSchemaManifest(t, &env)
			if err := env.validate(); err == nil {
				t.Fatal("accepted a semantically invalid rehashed manifest")
			}
		})
	}
}

func TestExportManifestRejectsChangedBodyWithStaleHash(t *testing.T) {
	portable := validPortableSeedManifest(t, validManifestEnvelope(t))
	portable.PortableScopes[0].RetentionGeneration++

	if err := portable.validate(); err == nil {
		t.Fatal("changed export manifest body retained its hash")
	}
}

func TestManifestHashCanonicalizesSemanticallyUnorderedCollections(t *testing.T) {
	t.Run("fields", func(t *testing.T) {
		env := validManifestEnvelope(t)
		originalHash := env.SchemaHash
		env.Manifest.Tables[0].Fields[0], env.Manifest.Tables[0].Fields[1] =
			env.Manifest.Tables[0].Fields[1], env.Manifest.Tables[0].Fields[0]
		changedHash, err := schemaManifestDigest(env.Manifest)
		if err != nil {
			t.Fatalf("hash reordered schema manifest: %v", err)
		}
		if changedHash != originalHash {
			t.Fatal("schema manifest hash changed with field order")
		}
		if err := env.validate(); err != nil {
			t.Fatalf("schema manifest rejected equivalent field order: %v", err)
		}
	})

	t.Run("tables", func(t *testing.T) {
		env := validManifestEnvelope(t)
		originalHash := env.SchemaHash
		env.Manifest.Tables[0], env.Manifest.Tables[1] = env.Manifest.Tables[1], env.Manifest.Tables[0]
		changedHash, err := schemaManifestDigest(env.Manifest)
		if err != nil {
			t.Fatalf("hash reordered schema manifest: %v", err)
		}
		if changedHash != originalHash {
			t.Fatal("schema manifest hash changed with table order")
		}
		if err := env.validate(); err != nil {
			t.Fatalf("schema manifest rejected equivalent table order: %v", err)
		}
	})

	t.Run("indexes", func(t *testing.T) {
		env := validManifestEnvelope(t)
		originalHash := env.SchemaHash
		env.Manifest.Tables[0].Indexes[0], env.Manifest.Tables[0].Indexes[1] =
			env.Manifest.Tables[0].Indexes[1], env.Manifest.Tables[0].Indexes[0]
		changedHash, err := schemaManifestDigest(env.Manifest)
		if err != nil {
			t.Fatalf("hash reordered schema manifest: %v", err)
		}
		if changedHash != originalHash {
			t.Fatal("schema manifest hash changed with index order")
		}
		if err := env.validate(); err != nil {
			t.Fatalf("schema manifest rejected equivalent index order: %v", err)
		}
	})

	t.Run("portable scopes", func(t *testing.T) {
		portable := validPortableSeedManifest(t, validManifestEnvelope(t))
		originalHash := portable.ExportManifestHash
		portable.PortableScopes[0], portable.PortableScopes[1] = portable.PortableScopes[1], portable.PortableScopes[0]
		changedHash, err := exportManifestDigest(portable)
		if err != nil {
			t.Fatalf("hash reordered export manifest: %v", err)
		}
		if changedHash == originalHash {
			t.Fatal("export manifest hash ignored received scope order")
		}
		if err := portable.validate(); err == nil {
			t.Fatal("export manifest accepted noncanonical scope order")
		}
	})
}

func validManifestEnvelope(t *testing.T) manifestEnvelope {
	t.Helper()
	env := manifestEnvelope{
		SchemaVersion: 1,
		Manifest: schemaManifest{
			SchemaVersion:      1,
			TransitionClass:    "initial",
			CompatibilityFloor: 1,
			Tables: []tableSchema{
				{
					ID:                "table-a",
					RelationID:        "relation-a",
					Name:              "items_a",
					PrimaryKeyFieldID: "field-a",
					Composition:       "single_scope",
					Fields: []fieldSchema{
						{ID: "field-a", Name: "id", Type: "string", Nullable: false},
						{ID: "field-b", Name: "title", Type: "string", Nullable: false},
					},
					Indexes: []indexSchema{
						{ID: "index-a", Name: "items_a_id", FieldIDs: []string{"field-a"}, Unique: true},
						{ID: "index-b", Name: "items_a_title", FieldIDs: []string{"field-b"}, Unique: false},
					},
				},
				{
					ID:                "table-b",
					RelationID:        "relation-b",
					Name:              "items_b",
					PrimaryKeyFieldID: "field-c",
					Composition:       "single_scope",
					Fields: []fieldSchema{
						{ID: "field-c", Name: "id", Type: "string", Nullable: false},
					},
				},
			},
		},
	}
	rehashSchemaManifest(t, &env)
	return env
}

func validPortableSeedManifest(t *testing.T, env manifestEnvelope) portableSeedManifest {
	t.Helper()
	checksum := checksumObject{
		Algorithm: "sha256",
		Version:   1,
		Encoding:  "hex",
		Digest:    strings.Repeat("0", 64),
	}
	portable := portableSeedManifest{
		ExportID:         "00000000-0000-0000-0000-000000000001",
		SchemaVersion:    env.SchemaVersion,
		SchemaHash:       env.SchemaHash,
		StreamGeneration: "generation-a",
		SnapshotBoundary: seedSnapshotBoundary{PositionKind: "generation_start"},
		PageLimit:        1000,
		PortableScopes: []portableSeedScope{
			{
				ID:                   "scope-a",
				RegistryGeneration:   1,
				MembershipGeneration: 1,
				RetentionGeneration:  1,
				Checksum:             checksum,
				Continuation:         "receipt-a",
				PageToken:            "page-a",
			},
			{
				ID:                   "scope-b",
				RegistryGeneration:   1,
				MembershipGeneration: 1,
				RetentionGeneration:  1,
				Checksum:             checksum,
				Continuation:         "receipt-b",
				PageToken:            "page-b",
			},
		},
	}
	hash, err := exportManifestDigest(portable)
	if err != nil {
		t.Fatalf("hash portable seed manifest: %v", err)
	}
	portable.ExportManifestHash = hash
	return portable
}

func rehashSchemaManifest(t *testing.T, env *manifestEnvelope) {
	t.Helper()
	hash, err := schemaManifestDigest(env.Manifest)
	if err != nil {
		t.Fatalf("hash schema manifest: %v", err)
	}
	env.SchemaHash = hash
	env.Manifest.SchemaHash = hash
}
