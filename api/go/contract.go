package synchroapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	ExpectedProtocolVersion    = 2
	ExpectedSQLContractVersion = 1
)

type ExtensionContractInfo struct {
	ExtensionVersion   string `json:"extension_version"`
	SQLContractVersion int    `json:"sql_contract_version"`
	ProtocolVersion    int    `json:"protocol_version"`
}

func RequireCompatibleExtension(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("synchroapi: database is required")
	}

	var extExists bool
	if err := db.QueryRowContext(
		ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'synchro_pg')",
	).Scan(&extExists); err != nil {
		return fmt.Errorf("checking synchro_pg extension: %w", err)
	}
	if !extExists {
		return fmt.Errorf("synchro_pg extension is not installed")
	}

	var raw []byte
	if err := db.QueryRowContext(ctx, "SELECT synchro_contract_info()").Scan(&raw); err != nil {
		return fmt.Errorf("loading synchro_pg contract info: %w", err)
	}

	var info ExtensionContractInfo
	if err := json.Unmarshal(raw, &info); err != nil {
		return fmt.Errorf("decoding synchro_pg contract info: %w", err)
	}

	if info.SQLContractVersion != ExpectedSQLContractVersion {
		return fmt.Errorf(
			"synchro_pg sql contract mismatch: expected %d, got %d (extension %s)",
			ExpectedSQLContractVersion,
			info.SQLContractVersion,
			info.ExtensionVersion,
		)
	}
	if info.ProtocolVersion != ExpectedProtocolVersion {
		return fmt.Errorf(
			"synchro_pg protocol version mismatch: expected %d, got %d (extension %s)",
			ExpectedProtocolVersion,
			info.ProtocolVersion,
			info.ExtensionVersion,
		)
	}

	required := []string{
		"synchro_connect(text, jsonb)",
		"synchro_pull(text, jsonb)",
		"synchro_push(text, jsonb)",
		"synchro_rebuild(text, jsonb)",
		"synchro_schema_manifest()",
		"synchro_tables()",
		"synchro_debug(text, text)",
	}
	missing := make([]string, 0)
	for _, signature := range required {
		var exists bool
		if err := db.QueryRowContext(
			ctx,
			"SELECT to_regprocedure($1) IS NOT NULL",
			signature,
		).Scan(&exists); err != nil {
			return fmt.Errorf("checking synchro_pg function %s: %w", signature, err)
		}
		if !exists {
			missing = append(missing, signature)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf(
			"synchro_pg is missing required canonical functions: %s",
			strings.Join(missing, ", "),
		)
	}

	return nil
}
