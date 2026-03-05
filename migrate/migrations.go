package migrate

// Migrations returns the SQL statements needed to create the synchro
// infrastructure tables. The consuming application should run these
// through their own migration system.
func Migrations() []string {
	return []string{
		// Changelog table — ordered log of all changes by bucket
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
	}
}

// RLSMigrations returns SQL statements to enable RLS policies.
// These are generated from a Registry and should be run after the
// application tables exist. Use synchro.GenerateRLSPolicies() to generate.
func RLSMigrations(policies []string) []string {
	return policies
}
