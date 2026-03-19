use pgrx::prelude::*;

/// Compact the sync changelog by removing entries that all active clients
/// have already processed.
///
/// Three-phase process:
/// 1. Deactivate stale clients (no sync within threshold).
/// 2. Calculate safe sequence (minimum checkpoint across active clients).
/// 3. Batch-delete changelog entries at or below the safe sequence.
///
/// Returns JSONB with: deactivated_clients, safe_seq, deleted_entries.
#[pg_extern]
fn synchro_compact(
    p_stale_threshold: default!(&str, "'7 days'"),
    p_batch_size: default!(i32, "10000"),
) -> pgrx::JsonB {
    Spi::connect_mut(|client| {
        // Phase 1: Deactivate stale clients.
        let deactivated = deactivate_stale_clients(client, p_stale_threshold);

        // Phase 2: Calculate safe sequence.
        let safe_seq = calculate_safe_seq(client);

        if safe_seq == 0 {
            return pgrx::JsonB(serde_json::json!({
                "deactivated_clients": deactivated,
                "safe_seq": 0,
                "deleted_entries": 0,
            }));
        }

        // Phase 3: Batch delete.
        let deleted = batch_delete_changelog(client, safe_seq, p_batch_size);

        pgrx::JsonB(serde_json::json!({
            "deactivated_clients": deactivated,
            "safe_seq": safe_seq,
            "deleted_entries": deleted,
        }))
    })
}

fn deactivate_stale_clients(client: &mut SpiClient<'_>, threshold: &str) -> i64 {
    match client.update(
        "UPDATE sync_clients SET is_active = false, updated_at = now() \
         WHERE is_active = true \
         AND last_sync_at IS NOT NULL \
         AND last_sync_at < now() - $1::interval",
        None,
        &[threshold.into()],
    ) {
        Ok(tup) => tup.len() as i64,
        Err(e) => pgrx::error!("deactivating stale clients: {}", e),
    }
}

fn calculate_safe_seq(client: &SpiClient<'_>) -> i64 {
    // Check if there are any active clients at all.
    let has_active_clients: bool = match client.select(
        "SELECT EXISTS (SELECT 1 FROM sync_clients WHERE is_active = true) AS has_active",
        None,
        &[],
    ) {
        Ok(tup) => tup
            .first()
            .get_one::<bool>()
            .ok()
            .flatten()
            .unwrap_or(false),
        Err(e) => pgrx::error!("checking active clients: {}", e),
    };

    // No active clients: safe to compact everything up to MAX(seq).
    if !has_active_clients {
        return match client.select(
            "SELECT COALESCE(MAX(seq), 0) AS max_seq FROM sync_changelog",
            None,
            &[],
        ) {
            Ok(tup) => tup
                .first()
                .get_one::<i64>()
                .ok()
                .flatten()
                .unwrap_or(0),
            Err(e) => pgrx::error!("querying max changelog seq: {}", e),
        };
    }

    // Per-bucket checkpoints (primary source).
    let bucket_min: i64 = match client.select(
        "SELECT COALESCE(MIN(cp.checkpoint), 0) AS min_cp \
         FROM sync_client_checkpoints cp \
         JOIN sync_clients c ON c.user_id = cp.user_id AND c.client_id = cp.client_id \
         WHERE c.is_active = true",
        None,
        &[],
    ) {
        Ok(tup) => tup
            .first()
            .get_one::<i64>()
            .ok()
            .flatten()
            .unwrap_or(0),
        Err(e) => pgrx::error!("querying bucket checkpoints for safe seq: {}", e),
    };

    // Legacy single checkpoint (fallback for clients without per-bucket CPs).
    let legacy_min: i64 = match client.select(
        "SELECT COALESCE(MIN(c.last_pull_seq), 0) AS min_cp \
         FROM sync_clients c \
         WHERE c.is_active = true \
         AND c.last_pull_seq IS NOT NULL \
         AND NOT EXISTS ( \
             SELECT 1 FROM sync_client_checkpoints cp \
             WHERE cp.user_id = c.user_id AND cp.client_id = c.client_id \
         )",
        None,
        &[],
    ) {
        Ok(tup) => tup
            .first()
            .get_one::<i64>()
            .ok()
            .flatten()
            .unwrap_or(0),
        Err(e) => pgrx::error!("querying legacy checkpoints for safe seq: {}", e),
    };

    // Take the minimum of both (0 means that source has no data).
    match (bucket_min, legacy_min) {
        (0, 0) => 0,
        (0, l) => l,
        (b, 0) => b,
        (b, l) => b.min(l),
    }
}

fn batch_delete_changelog(client: &mut SpiClient<'_>, safe_seq: i64, batch_size: i32) -> i64 {
    let mut total: i64 = 0;

    loop {
        let deleted = match client.update(
            "DELETE FROM sync_changelog WHERE seq IN ( \
                 SELECT seq FROM sync_changelog WHERE seq <= $1 ORDER BY seq LIMIT $2 \
             )",
            None,
            &[safe_seq.into(), batch_size.into()],
        ) {
            Ok(tup) => tup.len() as i64,
            Err(_) => break,
        };

        total += deleted;
        if deleted < batch_size as i64 {
            break;
        }
    }

    total
}

use pgrx::spi::SpiClient;
