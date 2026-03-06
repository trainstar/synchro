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
		{dbType: "uuid", want: "string"},
		{dbType: "text", want: "string"},
		{dbType: "timestamp with time zone", want: "datetime"},
		{dbType: "integer", want: "int"},
		{dbType: "numeric(10,2)", want: "float"},
		{dbType: "jsonb", want: "json"},
		{dbType: "bytea", want: "bytes"},
		{dbType: "text[]", wantErr: true},
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
