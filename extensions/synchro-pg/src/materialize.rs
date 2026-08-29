use pgrx::prelude::*;
use pgrx::spi::SpiClient;

use crate::bucketing::resolve_membership;
use crate::pull::{canonicalize_synced_row_data, synced_row_digest, typed_primary_key_bytes};
use crate::registry::{
    load_registry_from_client, load_registry_generation_for_activation,
    load_registry_generation_from_client, TableRegistration,
};
use crate::spi_helpers::required_text;

const DEFAULT_BACKFILL_BATCH_SIZE: i64 = 1_000;
const MAX_BACKFILL_BATCH_SIZE: i64 = 1_000;

#[derive(Debug)]
struct CapturedRecord {
    record_id: String,
    row_data: pgrx::JsonB,
    row_version: String,
    checksum: Vec<u8>,
}

#[derive(Debug)]
struct SchemaDigestRecord {
    relation_id: String,
    record_id: String,
    row_data: pgrx::JsonB,
    row_version: String,
    checksum: Vec<u8>,
    registry_generation: i64,
}

#[derive(Debug)]
struct CapturedProjectionRecord {
    stream_generation: String,
    commit_lsn: String,
    event_ordinal: i64,
    relation_id: String,
    image_kind: String,
    record_id: String,
    row_data: pgrx::JsonB,
    row_version: String,
    checksum: Vec<u8>,
    registry_generation: i64,
}

#[pg_extern]
fn synchro_backfill_bucket_edges(
    p_table_name: default!(Option<&str>, "NULL"),
    p_batch_size: default!(i64, "1000"),
) -> pgrx::JsonB {
    if !(1..=MAX_BACKFILL_BATCH_SIZE).contains(&p_batch_size) {
        pgrx::error!(
            "backfill batch size must be between 1 and {}",
            MAX_BACKFILL_BATCH_SIZE
        );
    }
    Spi::connect_mut(|client| {
        lock_backfill_state(client)
            .unwrap_or_else(|error| pgrx::error!("locking membership backfill state: {error}"));

        // The state locks prevent the worker from changing the active
        // projection while this generation is being built.
        let registry = load_registry_from_client(client)
            .unwrap_or_else(|error| pgrx::error!("loading registry for backfill: {error}"));
        let tables: Vec<&TableRegistration> = registry
            .iter()
            .filter(|table| {
                p_table_name
                    .map(|name| name == table.table_name)
                    .unwrap_or(true)
            })
            .collect();

        let table_names: Vec<String> = tables
            .iter()
            .map(|table| table.table_name.clone())
            .collect();
        if table_names.is_empty() {
            return pgrx::JsonB(serde_json::json!({
                "tables": table_names,
                "records": 0,
                "edges": 0,
                "affected_scopes": [],
                "batch_size": p_batch_size,
                "batch_count": 0,
            }));
        }

        validate_existing_edges(client, &tables)
            .unwrap_or_else(|error| pgrx::error!("validating existing membership edges: {error}"));
        create_staging_table(client)
            .unwrap_or_else(|error| pgrx::error!("creating membership backfill stage: {error}"));

        let mut record_count = 0i64;
        let mut edge_count = 0i64;
        let mut batch_count = 0i64;
        for table in &tables {
            let (records, edges, batches) = stage_table_edges(client, table, p_batch_size)
                .unwrap_or_else(|error| {
                    pgrx::error!("staging membership edges for {}: {error}", table.table_name)
                });
            record_count = record_count
                .checked_add(records)
                .unwrap_or_else(|| pgrx::error!("membership backfill record count overflowed"));
            edge_count = edge_count
                .checked_add(edges)
                .unwrap_or_else(|| pgrx::error!("membership backfill edge count overflowed"));
            batch_count = batch_count
                .checked_add(batches)
                .unwrap_or_else(|| pgrx::error!("membership backfill batch count overflowed"));
        }

        verify_staging(client, &tables)
            .unwrap_or_else(|error| pgrx::error!("verifying staged membership edges: {error}"));
        let affected_scopes = changed_scopes(client, &table_names)
            .unwrap_or_else(|error| pgrx::error!("computing affected membership scopes: {error}"));

        // The only live-edge mutation happens after the complete stage has
        // passed verification.  The table lock and the surrounding SQL
        // transaction make replacement atomic to every public reader.
        install_staged_edges(client, &table_names)
            .unwrap_or_else(|error| pgrx::error!("installing staged membership edges: {error}"));
        advance_affected_generations(client, &affected_scopes, None).unwrap_or_else(|error| {
            pgrx::error!("invalidating affected membership generations: {error}")
        });

        let boundary = load_materialization_boundary(client)
            .unwrap_or_else(|error| pgrx::error!("loading membership backfill boundary: {error}"));

        pgrx::JsonB(serde_json::json!({
            "tables": table_names,
            "records": record_count,
            "edges": edge_count,
            "affected_scopes": affected_scopes,
            "batch_size": p_batch_size,
            "batch_count": batch_count,
            "boundary": boundary,
        }))
    })
}

pub(crate) fn activate_staged_membership_generation(
    client: &mut SpiClient<'_>,
    source_generation: i64,
    target_generation: i64,
    stream_generation: &str,
    activation_commit_lsn: &str,
    activation_end_lsn: &str,
) -> Result<(), String> {
    let rows = client
        .select(
            "SELECT source_registry_generation, state
             FROM synchro.sync_registry_membership_stages
             WHERE registry_generation = $1",
            None,
            &[target_generation.into()],
        )
        .map_err(|error| format!("loading membership activation stage: {error}"))?;
    let Some(stage) = rows.into_iter().next() else {
        return Ok(());
    };
    let staged_source = stage
        .get_by_name::<i64, &str>("source_registry_generation")
        .map_err(|error| format!("reading membership activation source: {error}"))?
        .ok_or_else(|| "membership activation source is missing".to_string())?;
    let state = required_text(&stage, "state", "")?;
    if staged_source != source_generation || state != "pending" {
        return Err("membership activation stage binding is invalid".to_string());
    }

    acquire_backfill_lock(client)?;
    let registry = load_registry_generation_from_client(client, target_generation)
        .map_err(|error| format!("loading pending membership registry: {error}"))?;
    // A captured row stores the digest computed under the registration that
    // captured it, and a rebuild recomputes that digest under the active
    // registration. Activating a membership generation must therefore migrate
    // retained rows onto the target generation before their edges are staged,
    // exactly as schema publication does.
    migrate_schema_digests(client, target_generation)?;
    let target_rows = client
        .select(
            "SELECT target_relation_id::text AS relation_id
             FROM synchro.sync_registry_membership_stages stage
             CROSS JOIN LATERAL unnest(stage.target_relation_ids) target(target_relation_id)
             WHERE stage.registry_generation = $1
             ORDER BY target_relation_id",
            None,
            &[target_generation.into()],
        )
        .map_err(|error| format!("loading membership activation targets: {error}"))?;
    let mut target_relation_ids = Vec::with_capacity(target_rows.len());
    for row in target_rows {
        target_relation_ids.push(required_text(&row, "relation_id", "")?);
    }
    let tables: Vec<&TableRegistration> = registry
        .iter()
        .filter(|registration| {
            registration.is_synced() && target_relation_ids.contains(&registration.relation_id)
        })
        .collect();
    if tables.len() != target_relation_ids.len() || tables.is_empty() {
        return Err("membership activation targets are incomplete".to_string());
    }
    let table_names: Vec<String> = tables
        .iter()
        .map(|table| table.table_name.clone())
        .collect();

    validate_existing_edges(client, &tables)?;
    create_staging_table(client)?;
    let mut record_count = 0i64;
    let mut edge_count = 0i64;
    for table in &tables {
        let (records, edges, _) = stage_table_edges(client, table, DEFAULT_BACKFILL_BATCH_SIZE)?;
        record_count = record_count
            .checked_add(records)
            .ok_or_else(|| "membership activation record count overflowed".to_string())?;
        edge_count = edge_count
            .checked_add(edges)
            .ok_or_else(|| "membership activation edge count overflowed".to_string())?;
    }
    verify_staging(client, &tables)?;
    let affected_scopes = changed_scopes(client, &table_names)?;
    install_staged_edges(client, &table_names)?;
    advance_affected_generations(client, &affected_scopes, Some(target_generation))?;

    let updated = client
        .update(
            "UPDATE synchro.sync_registry_membership_stages
             SET state = 'activated', stream_generation = $2,
                 activation_commit_lsn = $3::pg_lsn,
                 activation_end_lsn = $4::pg_lsn,
                 staged_record_count = $5, staged_edge_count = $6,
                 affected_scopes = $7::text[], verified = true,
                 activated_at = now()
             WHERE registry_generation = $1 AND state = 'pending'",
            None,
            &[
                target_generation.into(),
                stream_generation.into(),
                activation_commit_lsn.into(),
                activation_end_lsn.into(),
                record_count.into(),
                edge_count.into(),
                affected_scopes.clone().into(),
            ],
        )
        .map_err(|error| format!("recording membership activation: {error}"))?
        .len();
    if updated != 1 {
        return Err("membership activation stage changed".to_string());
    }
    Ok(())
}

pub(crate) fn migrate_schema_digests(
    client: &mut SpiClient<'_>,
    target_generation: i64,
) -> Result<(), String> {
    if target_generation <= 0 {
        return Err("target registry generation is invalid".to_string());
    }

    // Public readers must observe the child manifest and its migrated digest
    // state from one transaction. This lock order matches projection writers.
    for table in [
        "synchro.sync_captured_projections",
        "synchro.sync_captured_rows",
        "synchro.sync_bucket_edges",
    ] {
        client
            .update(
                &format!("LOCK TABLE {table} IN SHARE ROW EXCLUSIVE MODE"),
                None,
                &[],
            )
            .map_err(|error| format!("locking {table} for schema migration: {error}"))?;
    }

    let target_registry = load_registry_generation_from_client(client, target_generation)
        .map_err(|error| format!("loading target registry for schema migration: {error}"))?;
    retire_removed_schema_rows(client, target_generation)?;
    let rows = client
        .select(
            "SELECT captured.relation_id::text AS relation_id,
                    captured.record_id, captured.row_data,
                    captured.row_version::text AS row_version,
                    captured.checksum, captured.registry_generation
             FROM synchro.sync_captured_rows captured
             JOIN synchro.sync_registry target
               ON target.registry_generation = $1
              AND target.relation_id = captured.relation_id
              AND target.registration_kind = 'synced'
             ORDER BY captured.relation_id, captured.record_id
             FOR UPDATE OF captured",
            None,
            &[target_generation.into()],
        )
        .map_err(|error| format!("loading retained schema rows: {error}"))?;
    let mut records = Vec::with_capacity(rows.len());
    for row in rows {
        records.push(SchemaDigestRecord {
            relation_id: required_text(&row, "relation_id", "")?,
            record_id: required_text(&row, "record_id", "")?,
            row_data: row
                .get_by_name::<pgrx::JsonB, &str>("row_data")
                .map_err(|error| format!("reading retained row data: {error}"))?
                .ok_or_else(|| "retained row data is missing".to_string())?,
            row_version: required_text(&row, "row_version", "")?,
            checksum: required_bytes(&row, "checksum")?,
            registry_generation: row
                .get_by_name::<i64, &str>("registry_generation")
                .map_err(|error| format!("reading retained row generation: {error}"))?
                .filter(|generation| *generation > 0)
                .ok_or_else(|| "retained row generation is invalid".to_string())?,
        });
    }

    let mut source_registries = std::collections::HashMap::new();
    for record in records {
        if let std::collections::hash_map::Entry::Vacant(entry) =
            source_registries.entry(record.registry_generation)
        {
            let registry = load_registry_generation_for_activation(
                client,
                record.registry_generation,
                target_generation,
            )
            .map_err(|error| format!("loading source registry for schema migration: {error}"))?;
            entry.insert(registry);
        }
        let source = source_registries
            .get(&record.registry_generation)
            .and_then(|registry| {
                registry
                    .iter()
                    .find(|table| table.relation_id == record.relation_id && table.is_synced())
            })
            .ok_or_else(|| "retained row source registration is missing".to_string())?;
        let target = target_registry
            .iter()
            .find(|table| table.relation_id == record.relation_id && table.is_synced())
            .ok_or_else(|| "retained row target registration is missing".to_string())?;
        if source.table_id != target.table_id
            || source.primary_key_field_id != target.primary_key_field_id
            || typed_primary_key_bytes(source, &record.record_id)?
                != typed_primary_key_bytes(target, &record.record_id)?
        {
            return Err("schema migration changed row identity".to_string());
        }

        let source_digest = synced_row_digest(
            client,
            source,
            &record.row_data.0,
            &record.record_id,
            &record.row_version,
        )?;
        if source_digest.as_bytes() != record.checksum.as_slice() {
            return Err("retained row source checksum does not match".to_string());
        }

        let (child_row, child_digest) = migrate_schema_row(
            client,
            source,
            target,
            record.row_data.0,
            &record.record_id,
            &record.row_version,
            &record.checksum,
        )?;
        let updated = client
            .update(
                "UPDATE synchro.sync_captured_rows
                 SET row_data = $3, checksum = $4,
                     registry_generation = $5, updated_at = now()
                 WHERE relation_id = $1::uuid AND record_id = $2
                   AND checksum = $6 AND row_version = $7::uuid
                   AND registry_generation = $8
                 RETURNING record_id",
                None,
                &[
                    record.relation_id.as_str().into(),
                    record.record_id.as_str().into(),
                    pgrx::JsonB(child_row).into(),
                    child_digest.clone().into(),
                    target_generation.into(),
                    record.checksum.clone().into(),
                    record.row_version.as_str().into(),
                    record.registry_generation.into(),
                ],
            )
            .map_err(|error| format!("migrating retained schema row: {error}"))?
            .len();
        if updated != 1 {
            return Err("retained row changed during schema migration".to_string());
        }
        client
            .update(
                "UPDATE synchro.sync_bucket_edges
                 SET checksum = $4, updated_at = now()
                 WHERE relation_id = $1::uuid AND table_name = $2 AND record_id = $3
                   AND checksum = $5 AND row_version = $6::uuid",
                None,
                &[
                    record.relation_id.as_str().into(),
                    target.table_name.as_str().into(),
                    record.record_id.as_str().into(),
                    child_digest.clone().into(),
                    record.checksum.into(),
                    record.row_version.as_str().into(),
                ],
            )
            .map_err(|error| format!("migrating retained schema edges: {error}"))?;
    }

    let projection_rows = client
        .select(
            "SELECT projection.stream_generation,
                    projection.commit_lsn::text AS commit_lsn,
                    projection.event_ordinal,
                    projection.relation_id::text AS relation_id,
                    projection.image_kind,
                    projection.record_id,
                    projection.row_data,
                    projection.row_version::text AS row_version,
                    projection.checksum,
                    projection.registry_generation
             FROM synchro.sync_captured_projections projection
             JOIN synchro.sync_registry target
               ON target.registry_generation = $1
              AND target.relation_id = projection.relation_id
              AND target.registration_kind = 'synced'
             ORDER BY projection.relation_id, projection.record_id,
                      projection.commit_lsn, projection.event_ordinal,
                      projection.image_kind
             FOR UPDATE OF projection",
            None,
            &[target_generation.into()],
        )
        .map_err(|error| format!("loading retained schema projections: {error}"))?;
    let mut projections = Vec::with_capacity(projection_rows.len());
    for row in projection_rows {
        projections.push(CapturedProjectionRecord {
            stream_generation: required_text(&row, "stream_generation", "")?,
            commit_lsn: required_text(&row, "commit_lsn", "")?,
            event_ordinal: row
                .get_by_name::<i64, &str>("event_ordinal")
                .map_err(|error| format!("reading retained projection ordinal: {error}"))?
                .filter(|ordinal| *ordinal >= 0)
                .ok_or_else(|| "retained projection ordinal is invalid".to_string())?,
            relation_id: required_text(&row, "relation_id", "")?,
            image_kind: required_text(&row, "image_kind", "")?,
            record_id: required_text(&row, "record_id", "")?,
            row_data: row
                .get_by_name::<pgrx::JsonB, &str>("row_data")
                .map_err(|error| format!("reading retained projection row data: {error}"))?
                .ok_or_else(|| "retained projection row data is missing".to_string())?,
            row_version: required_text(&row, "row_version", "")?,
            checksum: required_bytes(&row, "checksum")?,
            registry_generation: row
                .get_by_name::<i64, &str>("registry_generation")
                .map_err(|error| format!("reading retained projection generation: {error}"))?
                .filter(|generation| *generation > 0)
                .ok_or_else(|| "retained projection generation is invalid".to_string())?,
        });
    }
    for projection in projections {
        if let std::collections::hash_map::Entry::Vacant(entry) =
            source_registries.entry(projection.registry_generation)
        {
            let registry = load_registry_generation_for_activation(
                client,
                projection.registry_generation,
                target_generation,
            )
            .map_err(|error| format!("loading projection source registry: {error}"))?;
            entry.insert(registry);
        }
        let source = source_registries
            .get(&projection.registry_generation)
            .and_then(|registry| {
                registry
                    .iter()
                    .find(|table| table.relation_id == projection.relation_id && table.is_synced())
            })
            .ok_or_else(|| "retained projection source registration is missing".to_string())?;
        let target = target_registry
            .iter()
            .find(|table| table.relation_id == projection.relation_id && table.is_synced())
            .ok_or_else(|| "retained projection target registration is missing".to_string())?;
        let (child_row, child_digest) = migrate_schema_row(
            client,
            source,
            target,
            projection.row_data.0,
            &projection.record_id,
            &projection.row_version,
            &projection.checksum,
        )?;
        let updated = client
            .update(
                "UPDATE synchro.sync_captured_projections
                 SET row_data = $8, checksum = $9, registry_generation = $10
                 WHERE stream_generation = $1
                   AND commit_lsn = $2::pg_lsn
                   AND event_ordinal = $3
                   AND relation_id = $4::uuid
                   AND image_kind = $5
                   AND record_id = $6
                   AND row_version = $7::uuid
                   AND checksum = $11
                   AND registry_generation = $12
                 RETURNING record_id",
                None,
                &[
                    projection.stream_generation.as_str().into(),
                    projection.commit_lsn.as_str().into(),
                    projection.event_ordinal.into(),
                    projection.relation_id.as_str().into(),
                    projection.image_kind.as_str().into(),
                    projection.record_id.as_str().into(),
                    projection.row_version.as_str().into(),
                    pgrx::JsonB(child_row).into(),
                    child_digest.clone().into(),
                    target_generation.into(),
                    projection.checksum.into(),
                    projection.registry_generation.into(),
                ],
            )
            .map_err(|error| format!("migrating retained schema projection: {error}"))?
            .len();
        if updated != 1 {
            return Err("retained projection changed during schema migration".to_string());
        }
    }

    let invalid = client
        .select(
            "SELECT EXISTS (
                 SELECT 1
                 FROM synchro.sync_bucket_edges edge
                 JOIN synchro.sync_registry target
                   ON target.registry_generation = $1
                  AND target.relation_id = edge.relation_id
                  AND target.registration_kind = 'synced'
                 LEFT JOIN synchro.sync_captured_rows captured
                   ON captured.relation_id = edge.relation_id
                  AND captured.record_id = edge.record_id
                 WHERE edge.table_name <> target.table_name
                    OR captured.record_id IS NULL
                    OR captured.deleted IS DISTINCT FROM false
                    OR captured.registry_generation IS DISTINCT FROM $1
                    OR captured.row_version IS DISTINCT FROM edge.row_version
                    OR captured.checksum IS DISTINCT FROM edge.checksum
             ) AS invalid",
            None,
            &[target_generation.into()],
        )
        .map_err(|error| format!("verifying schema digest migration: {error}"))?
        .first()
        .get_by_name::<bool, &str>("invalid")
        .map_err(|error| format!("reading schema digest verification: {error}"))?
        .unwrap_or(true);
    if invalid {
        return Err("schema digest migration left invalid retained edges".to_string());
    }
    Ok(())
}

fn retire_removed_schema_rows(
    client: &mut SpiClient<'_>,
    target_generation: i64,
) -> Result<(), String> {
    client
        .update(
            "DELETE FROM synchro.sync_bucket_edges edge
             WHERE NOT EXISTS (
                 SELECT 1
                 FROM synchro.sync_registry target
                 WHERE target.registry_generation = $1
                   AND target.relation_id = edge.relation_id
                   AND target.registration_kind = 'synced'
             )",
            None,
            &[target_generation.into()],
        )
        .map_err(|error| format!("retiring removed schema edges: {error}"))?;
    client
        .update(
            "DELETE FROM synchro.sync_captured_rows captured
             WHERE NOT EXISTS (
                 SELECT 1
                 FROM synchro.sync_registry target
                 WHERE target.registry_generation = $1
                   AND target.relation_id = captured.relation_id
                   AND target.registration_kind = 'synced'
             )",
            None,
            &[target_generation.into()],
        )
        .map_err(|error| format!("retiring removed schema rows: {error}"))?;
    Ok(())
}

fn migrate_schema_row(
    client: &SpiClient<'_>,
    source: &TableRegistration,
    target: &TableRegistration,
    mut row_data: serde_json::Value,
    record_id: &str,
    row_version: &str,
    checksum: &[u8],
) -> Result<(serde_json::Value, Vec<u8>), String> {
    if source.table_id != target.table_id
        || source.primary_key_field_id != target.primary_key_field_id
        || typed_primary_key_bytes(source, record_id)?
            != typed_primary_key_bytes(target, record_id)?
    {
        return Err("schema migration changed row identity".to_string());
    }

    let source_digest = synced_row_digest(client, source, &row_data, record_id, row_version)?;
    if source_digest.as_bytes() != checksum {
        return Err("retained row source checksum does not match".to_string());
    }

    canonicalize_synced_row_data(source, &mut row_data)?;
    let child_fields = row_data
        .as_object_mut()
        .ok_or_else(|| "retained row is not an object".to_string())?;
    child_fields.retain(|field_id, _| {
        target
            .fields
            .iter()
            .any(|field| field.field_id == field_id.as_str())
    });
    for field in &target.fields {
        if child_fields.contains_key(&field.field_id) {
            continue;
        }
        if source
            .fields
            .iter()
            .any(|source_field| source_field.field_id == field.field_id)
            || !field.nullable
        {
            return Err("child schema row omits a retained field".to_string());
        }
        child_fields.insert(field.field_id.clone(), serde_json::Value::Null);
    }
    let child_digest = synced_row_digest(client, target, &row_data, record_id, row_version)?;
    Ok((row_data, child_digest.as_bytes().to_vec()))
}

fn lock_backfill_state(client: &mut SpiClient<'_>) -> Result<(), String> {
    client
        .update(
            "SELECT 1 FROM synchro.sync_wal_progress WHERE singleton FOR UPDATE",
            None,
            &[],
        )
        .map_err(|error| format!("locking materialization progress row: {error}"))?;
    client
        .update(
            "LOCK TABLE synchro.sync_wal_progress IN SHARE ROW EXCLUSIVE MODE",
            None,
            &[],
        )
        .map_err(|error| format!("locking materialization progress table: {error}"))?;
    acquire_backfill_lock(client)?;
    // Progress serializes the worker and blocks new pulls before these locks.
    for table in [
        "synchro.sync_captured_projections",
        "synchro.sync_captured_rows",
        "synchro.sync_bucket_edges",
        "synchro.sync_scope_state",
        "synchro.sync_client_checkpoints",
    ] {
        client
            .update(
                &format!("LOCK TABLE {table} IN SHARE ROW EXCLUSIVE MODE"),
                None,
                &[],
            )
            .map_err(|error| format!("locking {table}: {error}"))?;
    }
    Ok(())
}

fn acquire_backfill_lock(client: &SpiClient<'_>) -> Result<(), String> {
    client
        .select(
            "SELECT pg_catalog.pg_advisory_xact_lock($1::bigint)",
            None,
            &[crate::MEMBERSHIP_BACKFILL_LOCK_KEY.into()],
        )
        .map_err(|error| format!("locking membership backfill operation: {error}"))?;
    Ok(())
}

fn create_staging_table(client: &mut SpiClient<'_>) -> Result<(), String> {
    client
        .update(
            "CREATE TEMP TABLE IF NOT EXISTS synchro_backfill_edges (
                 relation_id UUID NOT NULL,
                 table_name TEXT NOT NULL,
                 record_id TEXT NOT NULL,
                 bucket_id TEXT NOT NULL,
                 checksum BYTEA NOT NULL CHECK (octet_length(checksum) = 32),
                 row_version UUID NOT NULL,
                 PRIMARY KEY (table_name, record_id, bucket_id)
             ) ON COMMIT DROP",
            None,
            &[],
        )
        .map_err(|error| format!("creating temporary edge table: {error}"))?;
    client
        .update("TRUNCATE pg_temp.synchro_backfill_edges", None, &[])
        .map_err(|error| format!("clearing temporary edge table: {error}"))?;
    Ok(())
}

fn validate_existing_edges(
    client: &SpiClient<'_>,
    tables: &[&TableRegistration],
) -> Result<(), String> {
    let names: Vec<String> = tables
        .iter()
        .map(|table| table.table_name.clone())
        .collect();
    let rows = client
        .select(
            "SELECT edge.table_name,
                    edge.relation_id::text AS relation_id,
                    edge.checksum,
                    captured.record_id AS captured_record_id,
                    captured.deleted,
                    captured.checksum AS captured_checksum
             FROM synchro.sync_bucket_edges edge
             LEFT JOIN synchro.sync_captured_rows captured
               ON captured.relation_id = edge.relation_id
              AND captured.record_id = edge.record_id
             WHERE edge.table_name = ANY($1)",
            None,
            &[names.into()],
        )
        .map_err(|error| format!("querying existing edges: {error}"))?;

    for row in rows {
        let table_name = required_text(&row, "table_name", "")?;
        let table = tables
            .iter()
            .find(|table| table.table_name == table_name)
            .ok_or_else(|| format!("edge references unknown table {table_name:?}"))?;
        let relation_id = required_text(&row, "relation_id", "")?;
        if relation_id != table.relation_id {
            return Err(format!(
                "edge for {table_name:?} has relation identity {relation_id:?}"
            ));
        }
        let edge_checksum = required_bytes(&row, "checksum")?;
        if edge_checksum.len() != 32 {
            return Err(format!("edge for {table_name:?} has an invalid checksum"));
        }
    }
    Ok(())
}

fn stage_table_edges(
    client: &mut SpiClient<'_>,
    table: &TableRegistration,
    batch_size: i64,
) -> Result<(i64, i64, i64), String> {
    if batch_size <= 0 {
        return Err("backfill batch size must be positive".to_string());
    }

    let mut last_record_id = String::new();
    let mut record_count = 0i64;
    let mut edge_count = 0i64;
    let mut batch_count = 0i64;

    loop {
        let rows = client
            .select(
                "SELECT record_id, row_data, row_version::text AS row_version,
                        checksum
                 FROM synchro.sync_captured_rows
                 WHERE relation_id = $1::uuid
                   AND NOT deleted
                   AND record_id > $2
                 ORDER BY record_id
                 LIMIT $3",
                None,
                &[
                    table.relation_id.as_str().into(),
                    last_record_id.as_str().into(),
                    batch_size.into(),
                ],
            )
            .map_err(|error| format!("querying captured rows: {error}"))?;
        if rows.is_empty() {
            break;
        }
        batch_count = batch_count
            .checked_add(1)
            .ok_or_else(|| "membership backfill batch count overflowed".to_string())?;

        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            records.push(CapturedRecord {
                record_id: required_text(&row, "record_id", "")?,
                row_data: row
                    .get_by_name::<pgrx::JsonB, &str>("row_data")
                    .map_err(|error| format!("reading captured row data: {error}"))?
                    .ok_or_else(|| "captured row data is missing".to_string())?,
                row_version: row
                    .get_by_name::<String, &str>("row_version")
                    .map_err(|error| format!("reading captured row version: {error}"))?
                    .filter(|version| !version.is_empty())
                    .ok_or_else(|| "captured row version is missing".to_string())?,
                checksum: required_bytes(&row, "checksum")?,
            });
        }

        for record in records {
            if record.checksum.len() != 32 {
                return Err(format!(
                    "captured row {}.{} has an invalid checksum",
                    table.table_name, record.record_id
                ));
            }
            let computed = synced_row_digest(
                client,
                table,
                &record.row_data.0,
                &record.record_id,
                &record.row_version,
            )
            .map_err(|error| {
                format!(
                    "computing checksum for {}.{}: {error}",
                    table.table_name, record.record_id
                )
            })?;
            if computed.as_bytes() != record.checksum.as_slice() {
                return Err(format!(
                    "captured row {}.{} checksum does not match row data",
                    table.table_name, record.record_id
                ));
            }

            let desired =
                resolve_membership(client, table, &record.record_id).map_err(|error| {
                    format!(
                        "resolving membership for {}.{}: {error}",
                        table.table_name, record.record_id
                    )
                })?;
            for bucket_id in desired {
                client
                    .update(
                        "INSERT INTO pg_temp.synchro_backfill_edges (
                             relation_id, table_name, record_id, bucket_id,
                             checksum, row_version
                         ) VALUES ($1::uuid, $2, $3, $4, $5, $6::uuid)",
                        None,
                        &[
                            table.relation_id.as_str().into(),
                            table.table_name.as_str().into(),
                            record.record_id.as_str().into(),
                            bucket_id.as_str().into(),
                            computed.as_bytes().to_vec().into(),
                            record.row_version.as_str().into(),
                        ],
                    )
                    .map_err(|error| {
                        format!(
                            "staging edge for {}.{}: {error}",
                            table.table_name, record.record_id
                        )
                    })?;
                edge_count = edge_count
                    .checked_add(1)
                    .ok_or_else(|| "membership backfill edge count overflowed".to_string())?;
            }
            record_count = record_count
                .checked_add(1)
                .ok_or_else(|| "membership backfill record count overflowed".to_string())?;
            last_record_id = record.record_id;
        }
    }

    Ok((record_count, edge_count, batch_count))
}

fn verify_staging(client: &SpiClient<'_>, tables: &[&TableRegistration]) -> Result<(), String> {
    let names: Vec<String> = tables
        .iter()
        .map(|table| table.table_name.clone())
        .collect();
    let invalid = client
        .select(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_temp.synchro_backfill_edges edge
                 LEFT JOIN synchro.sync_captured_rows captured
                   ON captured.relation_id = edge.relation_id
                  AND captured.record_id = edge.record_id
                 WHERE edge.table_name = ANY($1)
                   AND (captured.record_id IS NULL
                        OR captured.deleted
                        OR captured.checksum <> edge.checksum
                        OR captured.row_version <> edge.row_version)
             ) AS invalid",
            None,
            &[names.into()],
        )
        .map_err(|error| format!("querying staged edge verification: {error}"))?
        .first()
        .get_by_name::<bool, &str>("invalid")
        .map_err(|error| format!("reading staged edge verification: {error}"))?
        .unwrap_or(true);
    if invalid {
        return Err(
            "staged membership edge set is incomplete or has a digest mismatch".to_string(),
        );
    }
    Ok(())
}

fn changed_scopes(client: &SpiClient<'_>, table_names: &[String]) -> Result<Vec<String>, String> {
    let rows = client
        .select(
            "SELECT bucket_id AS scope_id
             FROM (
                 (SELECT relation_id, table_name, record_id, bucket_id
                  FROM synchro.sync_bucket_edges
                  WHERE table_name = ANY($1)
                  EXCEPT
                  SELECT relation_id, table_name, record_id, bucket_id
                  FROM pg_temp.synchro_backfill_edges)
                 UNION
                 (SELECT relation_id, table_name, record_id, bucket_id
                  FROM pg_temp.synchro_backfill_edges
                  EXCEPT
                  SELECT relation_id, table_name, record_id, bucket_id
                  FROM synchro.sync_bucket_edges
                  WHERE table_name = ANY($1))
             ) AS changed",
            None,
            &[table_names.to_vec().into()],
        )
        .map_err(|error| format!("querying changed scopes: {error}"))?;
    let mut scopes = Vec::with_capacity(rows.len());
    for row in rows {
        scopes.push(required_text(&row, "scope_id", "")?);
    }
    scopes.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    scopes.dedup();
    Ok(scopes)
}

fn install_staged_edges(client: &mut SpiClient<'_>, table_names: &[String]) -> Result<(), String> {
    client
        .update(
            "DELETE FROM synchro.sync_bucket_edges
             WHERE table_name = ANY($1)",
            None,
            &[table_names.to_vec().into()],
        )
        .map_err(|error| format!("removing replaced membership edges: {error}"))?;
    client
        .update(
            "INSERT INTO synchro.sync_bucket_edges (
                 relation_id, table_name, record_id, bucket_id,
                 checksum, row_version, updated_at
             )
             SELECT relation_id, table_name, record_id, bucket_id,
                    checksum, row_version, now()
             FROM pg_temp.synchro_backfill_edges",
            None,
            &[],
        )
        .map_err(|error| format!("installing replacement membership edges: {error}"))?;
    Ok(())
}

fn advance_affected_generations(
    client: &mut SpiClient<'_>,
    affected_scopes: &[String],
    activation_generation: Option<i64>,
) -> Result<(), String> {
    if affected_scopes.is_empty() {
        return Ok(());
    }
    client
        .update(
            "INSERT INTO synchro.sync_scope_state (scope_id, stream_generation)
             SELECT scope_id, runtime.stream_generation
             FROM unnest($1::text[]) AS scope(scope_id)
             CROSS JOIN synchro.sync_runtime_state runtime
             WHERE runtime.singleton = true
             ON CONFLICT (scope_id) DO NOTHING",
            None,
            &[affected_scopes.to_vec().into()],
        )
        .map_err(|error| format!("creating affected scope state: {error}"))?;
    let updated = client
        .update(
            "UPDATE synchro.sync_scope_state
             SET membership_generation = membership_generation + 1,
                 updated_at = now()
             WHERE scope_id = ANY($1)
             RETURNING scope_id",
            None,
            &[affected_scopes.to_vec().into()],
        )
        .map_err(|error| format!("installing replacement membership edges: {error}"))?;
    if updated.len() != affected_scopes.len() {
        return Err("not every affected scope has generation state".to_string());
    }

    // A checkpoint stores a stream position without a membership binding.
    // Retaining it after a membership replacement could skip the rebuilt set.
    client
        .update(
            "DELETE FROM synchro.sync_client_checkpoints
             WHERE bucket_id = ANY($1)",
            None,
            &[affected_scopes.to_vec().into()],
        )
        .map_err(|error| format!("invalidating affected checkpoints: {error}"))?;
    if let Some(generation) = activation_generation {
        client
            .update(
                "SELECT set_config(
                     'synchro.membership_activation_generation', $1, true
                 )",
                None,
                &[generation.to_string().as_str().into()],
            )
            .map_err(|error| format!("authorizing rebuild invalidation: {error}"))?;
        client
            .update(
                "DELETE FROM synchro.sync_rebuild_pages page
                 USING synchro.sync_rebuild_sessions session
                 WHERE page.session_id = session.session_id
                   AND session.scope_id = ANY($1)",
                None,
                &[affected_scopes.to_vec().into()],
            )
            .map_err(|error| format!("invalidating affected rebuild pages: {error}"))?;
        client
            .update(
                "DELETE FROM synchro.sync_rebuild_staged_rows staged
                 USING synchro.sync_rebuild_sessions session
                 WHERE staged.session_id = session.session_id
                   AND session.scope_id = ANY($1)",
                None,
                &[affected_scopes.to_vec().into()],
            )
            .map_err(|error| format!("invalidating affected rebuild rows: {error}"))?;
        client
            .update(
                "DELETE FROM synchro.sync_rebuild_sessions
                 WHERE scope_id = ANY($1)",
                None,
                &[affected_scopes.to_vec().into()],
            )
            .map_err(|error| format!("invalidating affected rebuild sessions: {error}"))?;
    }
    Ok(())
}

pub(crate) fn invalidate_affected_membership_generation(
    client: &mut SpiClient<'_>,
    affected_scopes: &[String],
    activation_generation: i64,
) -> Result<(), String> {
    if activation_generation <= 0 {
        return Err("projection bootstrap registry generation is invalid".to_string());
    }
    advance_affected_generations(client, affected_scopes, Some(activation_generation))
}

fn load_materialization_boundary(client: &SpiClient<'_>) -> Result<serde_json::Value, String> {
    let row = client
        .select(
            "SELECT stream_generation,
                    materialized_end_lsn::text AS materialized_end_lsn
             FROM synchro.sync_wal_progress
             WHERE singleton = true",
            None,
            &[],
        )
        .map_err(|error| format!("loading materialization boundary: {error}"))?
        .first();
    let stream_generation = required_table_text(&row, "stream_generation")?;
    let commit_lsn = row
        .get_by_name::<String, &str>("materialized_end_lsn")
        .map_err(|error| format!("reading materialization boundary: {error}"))?;
    Ok(serde_json::json!({
        "stream_generation": stream_generation,
        "kind": if commit_lsn.is_some() { "transaction_end" } else { "generation_start" },
        "commit_lsn": commit_lsn,
    }))
}

fn required_table_text(row: &pgrx::spi::SpiTupleTable<'_>, column: &str) -> Result<String, String> {
    row.get_by_name::<String, &str>(column)
        .map_err(|error| format!("reading {column}: {error}"))?
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{column} is missing"))
}

fn required_bytes(row: &pgrx::spi::SpiHeapTupleData<'_>, column: &str) -> Result<Vec<u8>, String> {
    row.get_by_name::<Vec<u8>, &str>(column)
        .map_err(|error| format!("reading {column}: {error}"))?
        .ok_or_else(|| format!("{column} is missing"))
}
