use pgrx::prelude::*;
use pgrx::spi::SpiClient;

const MAX_COMPACTION_BATCH_SIZE: i32 = 10_000;
const ELIGIBLE_EFFECT: &str = "
    effect.stream_generation IS NOT NULL
    AND effect.commit_lsn IS NOT NULL
    AND effect.event_ordinal IS NOT NULL
    AND effect.effect_ordinal IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM sync_clients active_client
        WHERE active_client.is_active
          AND effect.bucket_id = ANY(active_client.bucket_subs)
          AND NOT EXISTS (
              SELECT 1 FROM sync_client_checkpoints checkpoint
              WHERE checkpoint.user_id = active_client.user_id
                AND checkpoint.client_id = active_client.client_id
                AND checkpoint.bucket_id = effect.bucket_id
                AND checkpoint.stream_generation = effect.stream_generation
                AND (
                    checkpoint.position_kind = 'transaction_end'
                    AND checkpoint.commit_lsn >= effect.commit_lsn
                    OR checkpoint.position_kind = 'effect'
                    AND (checkpoint.commit_lsn, checkpoint.event_ordinal,
                         checkpoint.effect_ordinal) >=
                        (effect.commit_lsn, effect.event_ordinal, effect.effect_ordinal)
                )
          )
    )
    AND NOT EXISTS (
        SELECT 1 FROM sync_rebuild_sessions rebuild_session
        JOIN sync_clients rebuild_client
          ON rebuild_client.user_id = rebuild_session.user_id
         AND rebuild_client.client_id = rebuild_session.client_id
         AND rebuild_client.client_generation = rebuild_session.client_generation
         AND rebuild_client.is_active
         AND rebuild_session.scope_id = ANY(rebuild_client.bucket_subs)
        JOIN sync_scope_state scope_state
          ON scope_state.scope_id = rebuild_session.scope_id
         AND scope_state.stream_generation = rebuild_session.stream_generation
         AND scope_state.membership_generation = rebuild_session.membership_generation
         AND scope_state.retention_generation = rebuild_session.retention_generation
        WHERE rebuild_session.scope_id = effect.bucket_id
          AND rebuild_session.expires_at > now()
          AND rebuild_session.stream_generation = effect.stream_generation
          AND (
              rebuild_session.boundary_position_kind = 'generation_start'
              OR rebuild_session.boundary_position_kind = 'transaction_end'
              AND effect.commit_lsn > rebuild_session.boundary_commit_lsn
          )
    )";

/// Compact effects that every active, currently assigned client acknowledged.
#[pg_extern]
fn synchro_compact(
    p_stale_threshold: default!(Option<&str>, "'30 days'"),
    p_batch_size: default!(i32, "10000"),
) -> pgrx::JsonB {
    if !(1..=MAX_COMPACTION_BATCH_SIZE).contains(&p_batch_size) {
        pgrx::error!(
            "compaction batch size must be between 1 and {}",
            MAX_COMPACTION_BATCH_SIZE
        );
    }
    Spi::connect_mut(|client| {
        let p_stale_threshold = p_stale_threshold
            .unwrap_or_else(|| pgrx::error!("compaction stale inputs are invalid"));
        validate_stale_inputs(client, p_stale_threshold);
        let deactivated = deactivate_stale_clients(client, p_stale_threshold, p_batch_size);
        let (deleted_entries, last_deleted_seq) = delete_acknowledged_effects(client, p_batch_size);

        pgrx::JsonB(serde_json::json!({
            "deactivated_clients": deactivated,
            "safe_seq": last_deleted_seq,
            "deleted_entries": deleted_entries,
        }))
    })
}

/// Test-support injection that marks one active client generation for expiry during compaction.
#[pg_extern]
fn synchro_inject_client_retention_expiry(
    p_user_id: Option<&str>,
    p_client_id: Option<&str>,
) -> bool {
    let p_user_id =
        p_user_id.unwrap_or_else(|| pgrx::error!("retention client identity is invalid"));
    let p_client_id =
        p_client_id.unwrap_or_else(|| pgrx::error!("retention client identity is invalid"));
    if p_user_id.is_empty() || p_client_id.is_empty() {
        pgrx::error!("retention client identity is invalid");
    }
    Spi::connect_mut(|client| {
        let expired = client
            .update(
                "UPDATE sync_clients
                 SET generation_expires_at = pg_catalog.statement_timestamp(),
                     updated_at = now()
                 WHERE user_id = $1
                   AND client_id = $2
                   AND is_active
                   AND generation_expires_at IS NULL
                  RETURNING client_id",
                None,
                &[p_user_id.into(), p_client_id.into()],
            )
            .unwrap_or_else(|error| pgrx::error!("expiring retention client: {error}"));
        if expired.len() > 1 {
            pgrx::error!("retention client expiry affected multiple clients");
        }
        !expired.is_empty()
    })
}

fn validate_stale_inputs(client: &SpiClient<'_>, threshold: &str) {
    let valid = client
        .select(
            "SELECT pg_catalog.isfinite(parsed.value)
                    AND parsed.value > interval '0 seconds'
                    AND pg_catalog.isfinite(
                        pg_catalog.statement_timestamp() - parsed.value
                    ) AS valid
             FROM (SELECT $1::interval AS value) parsed",
            None,
            &[threshold.into()],
        )
        .unwrap_or_else(|_| pgrx::error!("compaction stale inputs are invalid"))
        .first()
        .get_by_name::<bool, &str>("valid")
        .unwrap_or_else(|_| pgrx::error!("reading compaction stale input validation failed"))
        .unwrap_or(false);
    if !valid {
        pgrx::error!("compaction stale inputs must be finite and the threshold must be positive");
    }
}

fn deactivate_stale_clients(client: &mut SpiClient<'_>, threshold: &str, batch_size: i32) -> i64 {
    match client.update(
        "WITH candidates AS (
             SELECT user_id, client_id FROM sync_clients
             WHERE is_active = true
             AND (
                 (
                     generation_expires_at IS NOT NULL
                     AND generation_expires_at <= pg_catalog.statement_timestamp()
                 )
                 OR GREATEST(
                     created_at,
                     COALESCE(last_sync_at, '-infinity'::timestamptz),
                     COALESCE(last_acknowledged_at, '-infinity'::timestamptz)
                 ) < pg_catalog.statement_timestamp() - $1::interval
             )
             ORDER BY user_id, client_id
             LIMIT $2
             FOR UPDATE
         )
         UPDATE sync_clients target
         SET is_active = false, updated_at = now()
         FROM candidates
         WHERE target.user_id = candidates.user_id AND target.client_id = candidates.client_id
         RETURNING target.client_id",
        None,
        &[threshold.into(), batch_size.into()],
    ) {
        Ok(tup) => tup.len() as i64,
        Err(error) => pgrx::error!("deactivating stale clients: {}", error),
    }
}

fn delete_acknowledged_effects(client: &mut SpiClient<'_>, batch_size: i32) -> (i64, i64) {
    let candidates = client
        .select(
            &format!(
                "SELECT effect.seq, effect.bucket_id,
                        effect.event_ordinal >= 0 AND effect.effect_ordinal >= 0 AS valid
                 FROM sync_changelog effect
                 WHERE {ELIGIBLE_EFFECT}
                 ORDER BY effect.seq LIMIT $1"
            ),
            None,
            &[batch_size.into()],
        )
        .unwrap_or_else(|error| pgrx::error!("selecting acknowledged effects: {error}"));
    let mut sequences = Vec::with_capacity(candidates.len());
    let mut scopes = std::collections::BTreeSet::new();
    for row in candidates {
        if row
            .get_by_name::<bool, &str>("valid")
            .unwrap_or_else(|error| pgrx::error!("reading compactable position: {error}"))
            != Some(true)
        {
            pgrx::error!("compactable position is invalid");
        }
        let seq = row
            .get_by_name::<i64, &str>("seq")
            .unwrap_or_else(|error| pgrx::error!("reading compactable sequence: {error}"))
            .unwrap_or_else(|| pgrx::error!("compactable sequence is missing"));
        let scope_id = row
            .get_by_name::<String, &str>("bucket_id")
            .unwrap_or_else(|error| pgrx::error!("reading compactable scope: {error}"))
            .unwrap_or_else(|| pgrx::error!("compactable scope is missing"));
        sequences.push(seq);
        scopes.insert(scope_id);
    }
    if sequences.is_empty() {
        return (0, 0);
    }
    let expected_scopes = scopes.len();
    let locked = client
        .select(
            "SELECT scope_id FROM sync_scope_state
             WHERE scope_id = ANY($1)
             ORDER BY scope_id FOR UPDATE",
            None,
            &[scopes.into_iter().collect::<Vec<_>>().into()],
        )
        .unwrap_or_else(|error| pgrx::error!("locking retention state: {error}"))
        .len();
    if locked != expected_scopes {
        pgrx::error!("compactable scope state is missing");
    }
    // A rebuild can create a pin while compaction waits for its scope lock.
    let result = client
        .update(
            &format!(
                "WITH deleted AS (
                     DELETE FROM sync_changelog effect
                     WHERE effect.seq = ANY($1) AND {ELIGIBLE_EFFECT}
                     RETURNING effect.seq, effect.bucket_id, effect.stream_generation,
                               effect.commit_lsn, effect.event_ordinal, effect.effect_ordinal
                 ),
                 floors AS (
                     SELECT DISTINCT ON (bucket_id, stream_generation)
                            bucket_id, stream_generation, commit_lsn,
                            event_ordinal, effect_ordinal
                     FROM deleted
                     ORDER BY bucket_id, stream_generation, commit_lsn DESC,
                              event_ordinal DESC, effect_ordinal DESC
                 ),
                 advanced AS (
                     UPDATE sync_scope_state state
                     SET floor_position_kind = 'effect',
                         floor_commit_lsn = floors.commit_lsn,
                         floor_event_ordinal = floors.event_ordinal,
                         floor_effect_ordinal = floors.effect_ordinal,
                         updated_at = now()
                     FROM floors
                     WHERE state.scope_id = floors.bucket_id
                       AND state.stream_generation = floors.stream_generation
                       AND (
                           state.floor_position_kind = 'generation_start'
                           OR state.floor_position_kind = 'effect'
                           AND (state.floor_commit_lsn, state.floor_event_ordinal,
                                state.floor_effect_ordinal) <
                               (floors.commit_lsn, floors.event_ordinal, floors.effect_ordinal)
                           OR state.floor_position_kind = 'transaction_end'
                           AND state.floor_commit_lsn < floors.commit_lsn
                       )
                     RETURNING state.scope_id
                 )
                 SELECT count(*)::bigint AS count, COALESCE(max(seq), 0)::bigint AS safe_seq
                 FROM deleted"
            ),
            None,
            &[sequences.into()],
        )
        .unwrap_or_else(|error| pgrx::error!("compacting acknowledged effects: {error}"))
        .first();
    let count = result
        .get_by_name::<i64, &str>("count")
        .unwrap_or_else(|error| pgrx::error!("reading compaction count: {error}"))
        .unwrap_or_else(|| pgrx::error!("compaction count is missing"));
    let safe_seq = result
        .get_by_name::<i64, &str>("safe_seq")
        .unwrap_or_else(|error| pgrx::error!("reading compacted sequence: {error}"))
        .unwrap_or_else(|| pgrx::error!("compacted sequence is missing"));
    (count, safe_seq)
}
