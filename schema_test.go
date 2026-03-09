package synchro

import "testing"

func TestValidateClientSchema(t *testing.T) {
	manifest := schemaManifest{
		Version: 7,
		Hash:    "abc123",
	}

	tests := []struct {
		name    string
		version int64
		hash    string
		allow   bool
		wantErr bool
	}{
		{
			name:    "bootstrap allowed",
			version: 0,
			hash:    "",
			allow:   true,
			wantErr: false,
		},
		{
			name:    "bootstrap rejected when disabled",
			version: 0,
			hash:    "",
			allow:   false,
			wantErr: true,
		},
		{
			name:    "matching schema",
			version: 7,
			hash:    "abc123",
			allow:   false,
			wantErr: false,
		},
		{
			name:    "missing hash rejected",
			version: 7,
			hash:    "",
			allow:   false,
			wantErr: true,
		},
		{
			name:    "missing version rejected",
			version: 0,
			hash:    "abc123",
			allow:   false,
			wantErr: true,
		},
		{
			name:    "mismatch rejected",
			version: 8,
			hash:    "abc123",
			allow:   false,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateClientSchema(manifest, tt.version, tt.hash, tt.allow)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateClientSchema() err = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestMapLogicalType(t *testing.T) {
	tests := []struct {
		dbType  string
		want    string
		wantErr bool
	}{
		// string types
		{dbType: "uuid", want: "string"},
		{dbType: "text", want: "string"},
		{dbType: "character varying", want: "string"},
		{dbType: "character varying(255)", want: "string"},
		{dbType: "character(10)", want: "string"},
		// int types
		{dbType: "integer", want: "int"},
		{dbType: "smallint", want: "int"},
		{dbType: "serial", want: "int"},
		// int64 types
		{dbType: "bigint", want: "int64"},
		{dbType: "bigserial", want: "int64"},
		// float types
		{dbType: "numeric(10,2)", want: "float"},
		{dbType: "double precision", want: "float"},
		{dbType: "real", want: "float"},
		// boolean
		{dbType: "boolean", want: "boolean"},
		// json
		{dbType: "jsonb", want: "json"},
		{dbType: "json", want: "json"},
		// bytes
		{dbType: "bytea", want: "bytes"},
		// date/time types
		{dbType: "date", want: "date"},
		{dbType: "time without time zone", want: "time"},
		{dbType: "time with time zone", want: "time"},
		{dbType: "timestamp with time zone", want: "datetime"},
		{dbType: "timestamp without time zone", want: "datetime"},
		// array types → json
		{dbType: "text[]", want: "json"},
		{dbType: "integer[]", want: "json"},
		// interval → string
		{dbType: "interval", want: "string"},
		// unsupported
		{dbType: "hstore", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.dbType, func(t *testing.T) {
			got, err := mapLogicalType(tt.dbType)
			if (err != nil) != tt.wantErr {
				t.Fatalf("mapLogicalType(%q) err = %v, wantErr = %v", tt.dbType, err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if got != tt.want {
				t.Fatalf("mapLogicalType(%q) = %q, want %q", tt.dbType, got, tt.want)
			}
		})
	}
}

func TestSQLiteDefaultSQL(t *testing.T) {
	tests := []struct {
		name       string
		defaultSQL string
		wantKind   string
		wantSQLite string
	}{
		{
			name:       "empty string with text cast",
			defaultSQL: "''::text",
			wantKind:   DefaultKindPortable,
			wantSQLite: "''",
		},
		{
			name:       "enum string literal",
			defaultSQL: "'pending'::order_status",
			wantKind:   DefaultKindPortable,
			wantSQLite: "'pending'",
		},
		{
			name:       "wrapped boolean cast",
			defaultSQL: "(true)::boolean",
			wantKind:   DefaultKindPortable,
			wantSQLite: "1",
		},
		{
			name:       "server only function",
			defaultSQL: "gen_random_uuid()",
			wantKind:   DefaultKindServerOnly,
			wantSQLite: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := defaultKindFor(tt.defaultSQL); got != tt.wantKind {
				t.Fatalf("defaultKindFor(%q) = %q, want %q", tt.defaultSQL, got, tt.wantKind)
			}
			if got := sqliteDefaultSQL(tt.defaultSQL); got != tt.wantSQLite {
				t.Fatalf("sqliteDefaultSQL(%q) = %q, want %q", tt.defaultSQL, got, tt.wantSQLite)
			}
		})
	}
}
