package migrate

// Migrations returns the SQL statements needed to create the synchro
// infrastructure tables. All statements are idempotent (IF NOT EXISTS).
//
// The consuming application should run these once during initial setup.
// After that, synchro.NewEngine() automatically detects and adds any
// missing columns to existing tables -- no need to re-run Migrations()
// when upgrading the library.
func Migrations() []string {
	return []string{
		// Changelog table: ordered log of all changes by bucket
		`CREATE TABLE IF NOT EXISTS sync_changelog (
			seq BIGSERIAL PRIMARY KEY,
			bucket_id TEXT NOT NULL,
			table_name TEXT NOT NULL,
			record_id TEXT NOT NULL,
			operation SMALLINT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,

		// Index for pull queries: bucket + checkpoint cursor
		`CREATE INDEX IF NOT EXISTS idx_sync_changelog_bucket_seq
			ON sync_changelog (bucket_id, seq)`,

		// Index for compaction: find entries by record
		`CREATE INDEX IF NOT EXISTS idx_sync_changelog_record
			ON sync_changelog (table_name, record_id)`,

		// Client registration table
		`CREATE TABLE IF NOT EXISTS sync_clients (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			user_id TEXT NOT NULL,
			client_id TEXT NOT NULL,
			client_name TEXT,
			platform TEXT NOT NULL DEFAULT '',
			app_version TEXT NOT NULL DEFAULT '',
			bucket_subs TEXT[] NOT NULL DEFAULT '{}',
			last_sync_at TIMESTAMPTZ,
			last_pull_at TIMESTAMPTZ,
			last_push_at TIMESTAMPTZ,
			last_pull_seq BIGINT,
			is_active BOOLEAN NOT NULL DEFAULT true,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			UNIQUE (user_id, client_id)
		)`,

		// Index for client lookups
		`CREATE INDEX IF NOT EXISTS idx_sync_clients_user_id
			ON sync_clients (user_id)`,

		// WAL consumer position tracking for crash recovery
		`CREATE TABLE IF NOT EXISTS sync_wal_position (
			slot_name TEXT PRIMARY KEY,
			confirmed_lsn BIGINT NOT NULL DEFAULT 0,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,

		// Membership index for bucket delta assignment.
		`CREATE TABLE IF NOT EXISTS sync_bucket_edges (
			table_name TEXT NOT NULL,
			record_id TEXT NOT NULL,
			bucket_id TEXT NOT NULL,
			checksum INTEGER,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (table_name, record_id, bucket_id)
		)`,

		`CREATE INDEX IF NOT EXISTS idx_sync_bucket_edges_bucket
			ON sync_bucket_edges (bucket_id, table_name, record_id)`,

		// Resolver failures for operational debugging and replay tooling.
		`CREATE TABLE IF NOT EXISTS sync_rule_failures (
			id BIGSERIAL PRIMARY KEY,
			table_name TEXT NOT NULL,
			record_id TEXT NOT NULL,
			operation SMALLINT NOT NULL,
			error_text TEXT NOT NULL,
			payload JSONB,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,

		`CREATE INDEX IF NOT EXISTS idx_sync_rule_failures_created
			ON sync_rule_failures (created_at)`,

		// Schema contract version/hash history.
		`CREATE TABLE IF NOT EXISTS sync_schema_manifest (
			schema_version BIGINT PRIMARY KEY,
			schema_hash TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,

		`CREATE UNIQUE INDEX IF NOT EXISTS idx_sync_schema_manifest_hash
			ON sync_schema_manifest (schema_hash)`,

		// Per-bucket checkpoints for each client.
		`CREATE TABLE IF NOT EXISTS sync_client_checkpoints (
			user_id TEXT NOT NULL,
			client_id TEXT NOT NULL,
			bucket_id TEXT NOT NULL,
			checkpoint BIGINT NOT NULL DEFAULT 0,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (user_id, client_id, bucket_id)
		)`,
	}
}

// RLSMigrations returns SQL statements to enable RLS policies.
// These are generated from a Registry and should be run after the
// application tables exist. Use synchro.GenerateRLSPolicies() to generate.
func RLSMigrations(policies []string) []string {
	return policies
}
