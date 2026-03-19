use pgrx::prelude::*;

/// Register a client for synchronization.
///
/// Upserts into sync_clients with default bucket subscriptions (user:{user_id}
/// and global). Returns JSONB matching the Go RegisterResponse.
#[pg_extern]
fn synchro_register_client(
    p_user_id: &str,
    p_client_id: &str,
    p_platform: default!(&str, "''"),
    p_app_version: default!(&str, "''"),
    p_schema_version: default!(i64, "0"),
    p_schema_hash: default!(&str, "''"),
) -> pgrx::JsonB {
    // Validate schema version/hash if provided.
    if p_schema_version > 0 || !p_schema_hash.is_empty() {
        validate_schema(p_schema_version, p_schema_hash);
    }

    let user_bucket = format!("user:{p_user_id}");

    let row: Option<pgrx::JsonB> = Spi::get_one_with_args(
        "WITH upserted AS (
            INSERT INTO sync_clients (
                user_id, client_id, platform, app_version,
                bucket_subs, is_active
            ) VALUES ($1, $2, $3, $4, ARRAY[$5, 'global'], true)
            ON CONFLICT (user_id, client_id) DO UPDATE SET
                platform = EXCLUDED.platform,
                app_version = EXCLUDED.app_version,
                is_active = true,
                updated_at = now()
            RETURNING id, last_sync_at, last_pull_seq
        ),
        schema AS (
            SELECT schema_version, schema_hash
            FROM sync_schema_manifest
            ORDER BY schema_version DESC
            LIMIT 1
        ),
        bucket_cps AS (
            SELECT jsonb_object_agg(bucket_id, checkpoint) AS cps
            FROM sync_client_checkpoints
            WHERE user_id = $1 AND client_id = $2
        )
        SELECT jsonb_build_object(
            'id', u.id::text,
            'server_time', now(),
            'last_sync_at', u.last_sync_at,
            'checkpoint', COALESCE(u.last_pull_seq, 0),
            'bucket_checkpoints', COALESCE(bc.cps, '{}'::jsonb),
            'schema_version', COALESCE(s.schema_version, 0),
            'schema_hash', COALESCE(s.schema_hash, '')
        )
        FROM upserted u
        LEFT JOIN schema s ON true
        LEFT JOIN bucket_cps bc ON true",
        vec![
            (PgBuiltInOids::TEXTOID.oid(), p_user_id.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), p_client_id.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), p_platform.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), p_app_version.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), user_bucket.as_str().into_datum()),
        ],
    )
    .unwrap_or(None);

    match row {
        Some(json) => json,
        None => pgrx::error!("client registration returned no result"),
    }
}

/// Validate client schema version/hash against the server manifest.
fn validate_schema(schema_version: i64, schema_hash: &str) {
    if schema_version == 0 && schema_hash.is_empty() {
        return;
    }

    let server_hash: Option<String> = Spi::get_one_with_args(
        "SELECT schema_hash FROM sync_schema_manifest WHERE schema_version = $1",
        vec![(PgBuiltInOids::INT8OID.oid(), schema_version.into_datum())],
    )
    .unwrap_or(None);

    match server_hash {
        Some(ref h) if h == schema_hash => {}
        Some(_) => {
            pgrx::error!(
                "schema mismatch: client hash {:?} does not match server for version {}",
                schema_hash,
                schema_version
            );
        }
        None => {
            let has_any: bool = Spi::get_one("SELECT EXISTS (SELECT 1 FROM sync_schema_manifest)")
                .unwrap_or(Some(false))
                .unwrap_or(false);

            if has_any {
                pgrx::error!(
                    "schema version {} not found on server",
                    schema_version
                );
            }
        }
    }
}
