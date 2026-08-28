package synchroapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const (
	ExpectedProtocolVersion    = 3
	ExpectedSQLContractVersion = 1
	ExpectedExtensionSchema    = "synchro"
)

type ExtensionContractInfo struct {
	ExtensionVersion          string `json:"extension_version"`
	SQLContractVersion        int    `json:"sql_contract_version"`
	ProtocolVersion           int    `json:"protocol_version"`
	LibraryBuildFingerprint   string `json:"library_build_fingerprint"`
	InstalledBuildFingerprint string `json:"installed_build_fingerprint"`
	ExtensionObjectsCurrent   bool   `json:"extension_objects_current"`
}

func RequireCompatibleExtension(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("synchroapi: database is required")
	}

	var extensionSchema string
	err := db.QueryRowContext(
		ctx,
		`SELECT n.nspname
		 FROM pg_catalog.pg_extension e
		 JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
		 WHERE e.extname = 'synchro_pg'`,
	).Scan(&extensionSchema)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("synchro_pg extension is not installed")
	}
	if err != nil {
		return fmt.Errorf("checking synchro_pg extension: %w", err)
	}
	if extensionSchema != ExpectedExtensionSchema {
		return fmt.Errorf(
			"synchro_pg extension schema mismatch: expected %s, got %s",
			ExpectedExtensionSchema,
			extensionSchema,
		)
	}

	var raw []byte
	if err := db.QueryRowContext(ctx, "SELECT synchro.synchro_contract_info()").Scan(&raw); err != nil {
		return fmt.Errorf("loading synchro_pg contract info: %w", err)
	}

	var info ExtensionContractInfo
	if err := json.Unmarshal(raw, &info); err != nil {
		return fmt.Errorf("decoding synchro_pg contract info: %w", err)
	}
	if err := requireCurrentExtensionObjects(info); err != nil {
		return err
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
		"synchro.synchro_connect(pg_catalog.text, pg_catalog.jsonb)",
		"synchro.synchro_pull(pg_catalog.text, pg_catalog.jsonb)",
		"synchro.synchro_push(pg_catalog.text, pg_catalog.jsonb)",
		"synchro.synchro_rebuild(pg_catalog.text, pg_catalog.jsonb)",
		"synchro.synchro_schema_manifest()",
		"synchro.synchro_tables()",
		"synchro.synchro_readiness()",
	}
	missing := make([]string, 0)
	for _, signature := range required {
		var exists bool
		if err := db.QueryRowContext(
			ctx,
			"SELECT pg_catalog.to_regprocedure($1) IS NOT NULL",
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

func requireCurrentExtensionObjects(info ExtensionContractInfo) error {
	library := strings.TrimSpace(info.LibraryBuildFingerprint)
	installed := strings.TrimSpace(info.InstalledBuildFingerprint)
	objectsCurrent := info.ExtensionObjectsCurrent && library != "" && installed != ""
	if library == "" {
		library = "missing"
	}
	if installed == "" {
		installed = "missing"
	}
	if !objectsCurrent || library != installed {
		return fmt.Errorf(
			"synchro_pg extension objects are stale: library fingerprint %q, installed objects fingerprint %q; recreate or update the extension",
			library,
			installed,
		)
	}
	return nil
}
