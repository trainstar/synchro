use pgrx::prelude::*;
use pgrx::spi::SpiClient;

use crate::stream_position::StreamPosition;

const MAX_COMPACTION_BATCH_SIZE: i32 = 10_000;

/// Compact effects that every active, currently assigned client acknowledged.
#[pg_extern]
fn synchro_compact(
    p_stale_threshold: default!(&str, "'30 days'"),
    p_batch_size: default!(i32, "10000"),
    p_stale_at: default!(Option<&str>, "NULL"),
) -> pgrx::JsonB {
    if !(1..=MAX_COMPACTION_BATCH_SIZE).contains(&p_batch_size) {
        pgrx::error!(
            "compaction batch size must be between 1 and {}",
            MAX_COMPACTION_BATCH_SIZE
        );
    }
    Spi::connect_mut(|client| {
        validate_stale_inputs(client, p_stale_threshold, p_stale_at);
        let deactivated = deactivate_stale_clients(client, p_stale_threshold, p_stale_at);
        lock_retention_state(client);
        let (deleted_entries, last_deleted_seq) = delete_acknowledged_effects(client, p_batch_size);

        pgrx::JsonB(serde_json::json!({
            "deactivated_clients": deactivated,
            "safe_seq": last_deleted_seq,
            "deleted_entries": deleted_entries,
        }))
    })
}

fn validate_stale_inputs(client: &SpiClient<'_>, threshold: &str, stale_at: Option<&str>) {
    let valid = client
        .select(
            "SELECT pg_catalog.isfinite(parsed.value)
                    AND parsed.value > interval '0 seconds'
                    AND pg_catalog.isfinite(
                        COALESCE($2::timestamptz, pg_catalog.statement_timestamp())
                    )
                    AND pg_catalog.isfinite(
                        COALESCE($2::timestamptz, pg_catalog.statement_timestamp()) - parsed.value
                    ) AS valid
             FROM (SELECT $1::interval AS value) parsed",
            None,
            &[threshold.into(), stale_at.into()],
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

fn deactivate_stale_clients(
    client: &mut SpiClient<'_>,
    threshold: &str,
    stale_at: Option<&str>,
) -> i64 {
    match client.update(
        "UPDATE sync_clients SET is_active = false, updated_at = now()
         WHERE is_active = true
            AND GREATEST(
                created_at,
                COALESCE(last_sync_at, '-infinity'::timestamptz),
                COALESCE(last_acknowledged_at, '-infinity'::timestamptz)
            ) < COALESCE($2::timestamptz, pg_catalog.statement_timestamp()) - $1::interval",
        None,
        &[threshold.into(), stale_at.into()],
    ) {
        Ok(tup) => tup.len() as i64,
        Err(error) => pgrx::error!("deactivating stale clients: {}", error),
    }
}

fn lock_retention_state(client: &SpiClient<'_>) {
    client
        .select(
            "SELECT scope_id FROM sync_scope_state ORDER BY scope_id FOR UPDATE",
            None,
            &[],
        )
        .unwrap_or_else(|error| pgrx::error!("locking retention state: {}", error));
}

fn delete_acknowledged_effects(client: &mut SpiClient<'_>, batch_size: i32) -> (i64, i64) {
    let mut total = 0i64;
    let mut last_deleted_seq = 0i64;
    loop {
        let deleted = client
            .update(
                "WITH candidates AS (
                     SELECT effect.seq
                     FROM sync_changelog effect
                     WHERE effect.stream_generation IS NOT NULL
                       AND effect.commit_lsn IS NOT NULL
                       AND effect.event_ordinal IS NOT NULL
                       AND effect.effect_ordinal IS NOT NULL
                       AND NOT EXISTS (
                         SELECT 1
                         FROM sync_clients active_client
                         WHERE active_client.is_active = true
                           AND effect.bucket_id = ANY(active_client.bucket_subs)
                           AND NOT EXISTS (
                               SELECT 1
                               FROM sync_client_checkpoints checkpoint
                               WHERE checkpoint.user_id = active_client.user_id
                                 AND checkpoint.client_id = active_client.client_id
                                 AND checkpoint.bucket_id = effect.bucket_id
                                 AND checkpoint.stream_generation = effect.stream_generation
                                 AND (
                                     checkpoint.position_kind = 'transaction_end'
                                         AND checkpoint.commit_lsn >= effect.commit_lsn
                                     OR checkpoint.position_kind = 'effect'
                                         AND (checkpoint.commit_lsn,
                                              checkpoint.event_ordinal,
                                              checkpoint.effect_ordinal) >=
                                             (effect.commit_lsn,
                                              effect.event_ordinal,
                                              effect.effect_ordinal)
                                 )
                           )
                      )
                       AND NOT EXISTS (
                         SELECT 1
                         FROM sync_rebuild_sessions rebuild_session
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
                       )
                      ORDER BY effect.seq
                     LIMIT $1
                 )
                 DELETE FROM sync_changelog effect
                 USING candidates
                 WHERE effect.seq = candidates.seq
                 RETURNING effect.seq, effect.bucket_id, effect.stream_generation,
                           effect.commit_lsn::text AS commit_lsn,
                           effect.event_ordinal, effect.effect_ordinal",
                None,
                &[batch_size.into()],
            )
            .unwrap_or_else(|error| pgrx::error!("deleting acknowledged effects: {}", error));
        let count = deleted.len() as i64;
        let mut floors = std::collections::BTreeMap::<(String, String), StreamPosition>::new();
        for row in deleted {
            let seq = row
                .get_by_name::<i64, &str>("seq")
                .unwrap_or_else(|error| pgrx::error!("reading deleted effect sequence: {}", error))
                .unwrap_or(0);
            last_deleted_seq = last_deleted_seq.max(seq);
            let scope_id = row
                .get_by_name::<String, &str>("bucket_id")
                .unwrap_or_else(|error| pgrx::error!("reading compacted scope: {}", error))
                .unwrap_or_else(|| pgrx::error!("compacted scope is missing"));
            let stream_generation = row
                .get_by_name::<String, &str>("stream_generation")
                .unwrap_or_else(|error| {
                    pgrx::error!("reading compacted stream generation: {}", error)
                })
                .unwrap_or_else(|| pgrx::error!("compacted stream generation is missing"));
            let commit_lsn = row
                .get_by_name::<String, &str>("commit_lsn")
                .unwrap_or_else(|error| pgrx::error!("reading compacted commit LSN: {}", error))
                .unwrap_or_else(|| pgrx::error!("compacted commit LSN is missing"));
            let event_ordinal = row
                .get_by_name::<i64, &str>("event_ordinal")
                .unwrap_or_else(|error| pgrx::error!("reading compacted event ordinal: {}", error))
                .unwrap_or_else(|| pgrx::error!("compacted event ordinal is missing"));
            let effect_ordinal = row
                .get_by_name::<i32, &str>("effect_ordinal")
                .unwrap_or_else(|error| pgrx::error!("reading compacted effect ordinal: {}", error))
                .unwrap_or_else(|| pgrx::error!("compacted effect ordinal is missing"));
            let position = StreamPosition::effect(&commit_lsn, event_ordinal, effect_ordinal)
                .unwrap_or_else(|error| pgrx::error!("reading compacted position: {}", error));
            floors
                .entry((scope_id, stream_generation))
                .and_modify(|prior| *prior = prior.clone().max(position.clone()))
                .or_insert(position);
        }
        for ((scope_id, stream_generation), floor) in floors {
            advance_retention_floor(client, &scope_id, &stream_generation, &floor);
        }
        total += count;
        if count < i64::from(batch_size) {
            return (total, last_deleted_seq);
        }
    }
}

fn advance_retention_floor(
    client: &mut SpiClient<'_>,
    scope_id: &str,
    stream_generation: &str,
    floor: &StreamPosition,
) {
    let commit_lsn = floor
        .commit_lsn()
        .unwrap_or_else(|| pgrx::error!("compaction floor has no commit LSN"));
    let updated = client
        .update(
            "UPDATE sync_scope_state
             SET floor_position_kind = 'effect',
                 floor_commit_lsn = $3::pg_lsn,
                 floor_event_ordinal = $4,
                 floor_effect_ordinal = $5,
                 updated_at = now()
             WHERE scope_id = $1
               AND stream_generation = $2
               AND (
                   floor_position_kind = 'generation_start'
                   OR floor_position_kind = 'effect'
                      AND (floor_commit_lsn, floor_event_ordinal, floor_effect_ordinal) <
                          ($3::pg_lsn, $4::bigint, $5::integer)
                   OR floor_position_kind = 'transaction_end'
                      AND floor_commit_lsn < $3::pg_lsn
               )",
            None,
            &[
                scope_id.into(),
                stream_generation.into(),
                commit_lsn.as_str().into(),
                floor.event_ordinal().into(),
                floor.effect_ordinal().into(),
            ],
        )
        .unwrap_or_else(|error| pgrx::error!("advancing retention floor: {}", error));
    if updated.len() > 1 {
        pgrx::error!("retention floor update affected multiple scopes");
    }
}
