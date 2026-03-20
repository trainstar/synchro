use pgrx::prelude::*;

use crate::push::ts_to_iso;

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
        if let Err(err_json) = validate_schema(p_schema_version, p_schema_hash) {
            return err_json;
        }
    }

    let user_bucket = format!("user:{p_user_id}");

    let sql = format!(
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
            'server_time', {server_time},
            'last_sync_at', {last_sync_at},
            'checkpoint', COALESCE(u.last_pull_seq, 0),
            'bucket_checkpoints', COALESCE(bc.cps, '{{}}'::jsonb),
            'schema_version', COALESCE(s.schema_version, 0),
            'schema_hash', COALESCE(s.schema_hash, '')
        )
        FROM upserted u
        LEFT JOIN schema s ON true
        LEFT JOIN bucket_cps bc ON true",
        server_time = ts_to_iso("now()"),
        last_sync_at = ts_to_iso("u.last_sync_at"),
    );

    Spi::connect_mut(|client| {
        // Ensure timestamps serialize as ISO 8601 UTC via to_json().
        let _ = client.update("SET LOCAL timezone = 'UTC'", None, &[]);

        let tup = client.update(
            &sql,
            None,
            &[
                p_user_id.into(),
                p_client_id.into(),
                p_platform.into(),
                p_app_version.into(),
                user_bucket.as_str().into(),
            ],
        )
        .unwrap_or_else(|e| pgrx::error!("registering client: {}", e));

        let row: Option<pgrx::JsonB> = tup.first().get_one().ok().flatten();

        match row {
            Some(json) => json,
            None => pgrx::error!("client registration returned no result"),
        }
    })
}

/// Validate client schema version/hash against the server manifest.
///
/// Returns Ok(()) if valid, or Err(JsonB) with a structured error response
/// for schema mismatches. Business conditions are returned as JSONB, not
/// PG exceptions.
pub fn validate_schema(schema_version: i64, schema_hash: &str) -> Result<(), pgrx::JsonB> {
    if schema_version == 0 && schema_hash.is_empty() {
        return Ok(());
    }

    let server_hash: Option<String> = Spi::get_one_with_args(
        "SELECT schema_hash FROM sync_schema_manifest WHERE schema_version = $1",
        &[schema_version.into()],
    )
    .unwrap_or(None);

    match server_hash {
        Some(ref h) if h == schema_hash => Ok(()),
        Some(_) => {
            // Hash mismatch for this version.
            let (sv, sh) = latest_server_schema();
            Err(pgrx::JsonB(serde_json::json!({
                "error": "schema_mismatch",
                "server_schema_version": sv,
                "server_schema_hash": sh,
            })))
        }
        None => {
            // Version not found. Only error if the manifest has entries.
            let has_any: bool =
                Spi::get_one("SELECT EXISTS (SELECT 1 FROM sync_schema_manifest)")
                    .unwrap_or(Some(false))
                    .unwrap_or(false);

            if has_any {
                let (sv, sh) = latest_server_schema();
                Err(pgrx::JsonB(serde_json::json!({
                    "error": "schema_mismatch",
                    "server_schema_version": sv,
                    "server_schema_hash": sh,
                })))
            } else {
                // No schema manifest entries yet, skip validation.
                Ok(())
            }
        }
    }
}

/// Get the latest server schema version and hash.
fn latest_server_schema() -> (i64, String) {
    Spi::connect(|client| {
        let tup = match client.select(
            "SELECT schema_version, schema_hash FROM sync_schema_manifest \
             ORDER BY schema_version DESC LIMIT 1",
            None,
            &[],
        ) {
            Ok(t) => t,
            Err(_) => return (0, String::new()),
        };
        for row in tup {
            let v: i64 = row
                .get_by_name::<i64, &str>("schema_version")
                .unwrap_or(None)
                .unwrap_or(0);
            let h: String = row
                .get_by_name::<String, &str>("schema_hash")
                .unwrap_or(None)
                .unwrap_or_default();
            return (v, h);
        }
        (0, String::new())
    })
}
