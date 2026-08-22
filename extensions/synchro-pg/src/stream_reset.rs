use std::collections::{BTreeMap, BTreeSet};

use pgrx::prelude::*;
use pgrx::spi::{SpiClient, SpiHeapTupleData, SpiTupleTable};
use synchro_core::checksum::{row_identity, scope_digest, ScopeDigestEntry, Sha256Digest};

use crate::bucketing::resolve_membership;
use crate::pull::{
    canonical_table, canonicalize_synced_row_data, hydrate_records, row_primary_key_json,
    schema_hash_for_generation, synced_row_digest, synced_row_projection_sql,
};
use crate::registry::{
    acquire_registry_write_lock, load_registry_generation_from_client, qualified_relation_name,
    TableRegistration,
};

const MAX_SLOT_NAME_BYTES: usize = 63;
const MAX_SNAPSHOT_NAME_BYTES: usize = 128;

#[derive(Clone, Copy, PartialEq, Eq)]
enum SlotValidation {
    Required,
    #[cfg(feature = "pg_test")]
    TestBypass,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SourceLockLifetime {
    Session,
    Transaction,
}

struct ResetRecord {
    reset_id: String,
    operation_kind: String,
    lifecycle: String,
    source_stream_generation: String,
    target_stream_generation: String,
    source_registry_generation: i64,
    target_registry_generation: Option<i64>,
    old_slot_name: String,
    candidate_slot_name: String,
    database_oid: i64,
    database_name: String,
    plugin: String,
    consistent_point: Option<String>,
    exported_snapshot_name: Option<String>,
    snapshot_before_xid: Option<String>,
    snapshot_after_xid: Option<String>,
    snapshot_before_nonce: Option<String>,
    snapshot_after_nonce: Option<String>,
    activation_barrier: Option<String>,
    target_schema_version: Option<i64>,
    target_schema_hash: Option<String>,
    target_canonical_manifest_body: Option<String>,
    candidate_materialized_commit_lsn: Option<String>,
    candidate_materialized_end_lsn: Option<String>,
    candidate_acknowledged_end_lsn: Option<String>,
    candidate_verified: bool,
    affected_scopes: Option<Vec<String>>,
}

impl ResetRecord {
    fn is_projection_bootstrap(&self) -> bool {
        self.operation_kind == "projection_bootstrap"
    }

    fn staging_registry_generation(&self) -> Result<i64, String> {
        if self.is_projection_bootstrap() {
            return self
                .target_registry_generation
                .filter(|value| *value > 0)
                .ok_or_else(|| "projection bootstrap target registry is missing".to_string());
        }
        Ok(self.source_registry_generation)
    }
}

struct SnapshotBinding<'a> {
    before_xid: &'a str,
    before_nonce: &'a str,
    after_xid: &'a str,
    after_nonce: &'a str,
}

struct SourceRow {
    record_id: String,
    row_data: serde_json::Value,
    deleted: bool,
}

struct CaptureDependencySourceRow {
    capture_key: serde_json::Value,
    row_data: serde_json::Value,
}

struct StagedVersion {
    row_version: String,
    deleted: bool,
}

trait ResetRow {
    fn reset_text(&self, name: &str) -> Result<Option<String>, spi::Error>;
    fn reset_bool(&self, name: &str) -> Result<Option<bool>, spi::Error>;
    fn reset_i64(&self, name: &str) -> Result<Option<i64>, spi::Error>;
    fn reset_bytes(&self, name: &str) -> Result<Option<Vec<u8>>, spi::Error>;
}

impl ResetRow for SpiTupleTable<'_> {
    fn reset_text(&self, name: &str) -> Result<Option<String>, spi::Error> {
        self.get_by_name::<String, &str>(name)
    }

    fn reset_bool(&self, name: &str) -> Result<Option<bool>, spi::Error> {
        self.get_by_name::<bool, &str>(name)
    }

    fn reset_i64(&self, name: &str) -> Result<Option<i64>, spi::Error> {
        self.get_by_name::<i64, &str>(name)
    }

    fn reset_bytes(&self, name: &str) -> Result<Option<Vec<u8>>, spi::Error> {
        self.get_by_name::<Vec<u8>, &str>(name)
    }
}

impl ResetRow for SpiHeapTupleData<'_> {
    fn reset_text(&self, name: &str) -> Result<Option<String>, spi::Error> {
        self.get_by_name::<String, &str>(name)
    }

    fn reset_bool(&self, name: &str) -> Result<Option<bool>, spi::Error> {
        self.get_by_name::<bool, &str>(name)
    }

    fn reset_i64(&self, name: &str) -> Result<Option<i64>, spi::Error> {
        self.get_by_name::<i64, &str>(name)
    }

    fn reset_bytes(&self, name: &str) -> Result<Option<Vec<u8>>, spi::Error> {
        self.get_by_name::<Vec<u8>, &str>(name)
    }
}

#[pg_extern(volatile)]
fn synchro_prepare_stream_reset(candidate_slot_name: &str) -> pgrx::JsonB {
    let result = Spi::connect_mut(|client| prepare_stream_reset(client, candidate_slot_name));
    match result {
        Ok(value) => pgrx::JsonB(value),
        Err(_) => pgrx::error!("stream reset preparation failed"),
    }
}

#[pg_extern(volatile)]
fn synchro_lock_stream_reset_sources(reset_id: pgrx::Uuid) -> bool {
    let reset_id = reset_id.to_string();
    let result = Spi::connect_mut(|client| {
        lock_stream_reset_sources(client, &reset_id, SourceLockLifetime::Session)
    });
    match result {
        Ok(()) => true,
        Err(_) => pgrx::error!("stream reset source locking failed"),
    }
}

#[pg_extern(volatile)]
fn synchro_mark_stream_reset_snapshot(reset_id: pgrx::Uuid, phase: &str) -> pgrx::JsonB {
    let reset_id = reset_id.to_string();
    match Spi::connect_mut(|client| mark_stream_reset_snapshot(client, &reset_id, phase)) {
        Ok(value) => pgrx::JsonB(value),
        Err(_) => pgrx::error!("stream reset snapshot marking failed"),
    }
}

#[pg_extern(volatile)]
#[allow(clippy::too_many_arguments)]
fn synchro_stage_stream_reset(
    reset_id: pgrx::Uuid,
    candidate_slot_name: &str,
    consistent_point: &str,
    exported_snapshot_name: &str,
    snapshot_before_xid: &str,
    snapshot_before_nonce: pgrx::Uuid,
    snapshot_after_xid: &str,
    snapshot_after_nonce: pgrx::Uuid,
) -> pgrx::JsonB {
    let reset_id = reset_id.to_string();
    let snapshot_before_nonce = snapshot_before_nonce.to_string();
    let snapshot_after_nonce = snapshot_after_nonce.to_string();
    let result = Spi::connect_mut(|client| {
        stage_stream_reset(
            client,
            &reset_id,
            candidate_slot_name,
            consistent_point,
            exported_snapshot_name,
            Some(SnapshotBinding {
                before_xid: snapshot_before_xid,
                before_nonce: &snapshot_before_nonce,
                after_xid: snapshot_after_xid,
                after_nonce: &snapshot_after_nonce,
            }),
            SlotValidation::Required,
        )
    });
    match result {
        Ok(value) => pgrx::JsonB(value),
        Err(error) => {
            log!("synchro stream reset staging failed: {error}");
            pgrx::error!("stream reset staging failed")
        }
    }
}

#[pg_extern(volatile)]
fn synchro_activate_stream_reset(reset_id: pgrx::Uuid) -> pgrx::JsonB {
    let reset_id = reset_id.to_string();
    let result = Spi::connect_mut(|client| {
        activate_stream_reset(client, &reset_id, SlotValidation::Required)
    });
    match result {
        Ok(value) => pgrx::JsonB(value),
        Err(error) => {
            log!("synchro stream reset activation failed: {error}");
            pgrx::error!("stream reset activation failed")
        }
    }
}

#[pg_extern(volatile)]
fn synchro_abort_stream_reset(reset_id: pgrx::Uuid) -> pgrx::JsonB {
    let reset_id = reset_id.to_string();
    let result = Spi::connect_mut(|client| abort_stream_reset(client, &reset_id));
    match result {
        Ok(value) => pgrx::JsonB(value),
        Err(_) => pgrx::error!("stream reset abort failed"),
    }
}

#[pg_extern(volatile)]
fn synchro_complete_stream_reset_cleanup(reset_id: pgrx::Uuid) -> bool {
    let reset_id = reset_id.to_string();
    let result = Spi::connect_mut(|client| complete_stream_reset_cleanup(client, &reset_id));
    match result {
        Ok(()) => true,
        Err(_) => pgrx::error!("stream reset cleanup completion failed"),
    }
}

#[pg_extern(volatile)]
fn synchro_prepare_projection_bootstrap(
    registry_generation: i64,
    candidate_slot_name: &str,
) -> pgrx::JsonB {
    match Spi::connect_mut(|client| {
        prepare_projection_bootstrap(client, registry_generation, candidate_slot_name)
    }) {
        Ok(value) => pgrx::JsonB(value),
        Err(error) => {
            log!("synchro projection bootstrap preparation failed: {error}");
            pgrx::error!("projection bootstrap preparation failed")
        }
    }
}

#[pg_extern(volatile)]
#[allow(clippy::too_many_arguments)]
fn synchro_stage_projection_bootstrap(
    bootstrap_id: pgrx::Uuid,
    candidate_slot_name: &str,
    consistent_point: &str,
    exported_snapshot_name: &str,
    snapshot_before_xid: &str,
    snapshot_before_nonce: pgrx::Uuid,
    snapshot_after_xid: &str,
    snapshot_after_nonce: pgrx::Uuid,
) -> pgrx::JsonB {
    let bootstrap_id = bootstrap_id.to_string();
    let snapshot_before_nonce = snapshot_before_nonce.to_string();
    let snapshot_after_nonce = snapshot_after_nonce.to_string();
    match Spi::connect_mut(|client| {
        stage_projection_bootstrap(
            client,
            &bootstrap_id,
            candidate_slot_name,
            consistent_point,
            exported_snapshot_name,
            Some(SnapshotBinding {
                before_xid: snapshot_before_xid,
                before_nonce: &snapshot_before_nonce,
                after_xid: snapshot_after_xid,
                after_nonce: &snapshot_after_nonce,
            }),
            SlotValidation::Required,
        )
    }) {
        Ok(value) => pgrx::JsonB(value),
        Err(error) => {
            log!("synchro projection bootstrap staging failed: {error}");
            pgrx::error!("projection bootstrap staging failed")
        }
    }
}

#[pg_extern(volatile)]
fn synchro_emit_projection_bootstrap_barrier(bootstrap_id: pgrx::Uuid) -> pgrx::JsonB {
    let bootstrap_id = bootstrap_id.to_string();
    match Spi::connect_mut(|client| emit_projection_bootstrap_barrier(client, &bootstrap_id)) {
        Ok(value) => pgrx::JsonB(value),
        Err(error) => {
            log!("synchro projection bootstrap barrier emission failed: {error}");
            pgrx::error!("projection bootstrap barrier emission failed")
        }
    }
}

#[pg_extern(volatile)]
fn synchro_request_projection_bootstrap_barrier(bootstrap_id: pgrx::Uuid) -> pgrx::JsonB {
    let bootstrap_id = bootstrap_id.to_string();
    match Spi::connect_mut(|client| request_projection_bootstrap_barrier(client, &bootstrap_id)) {
        Ok(value) => pgrx::JsonB(value),
        Err(error) => {
            log!("synchro projection bootstrap barrier request failed: {error}");
            pgrx::error!("projection bootstrap barrier request failed")
        }
    }
}

#[pg_extern(volatile)]
fn synchro_activate_projection_bootstrap(bootstrap_id: pgrx::Uuid) -> pgrx::JsonB {
    let bootstrap_id = bootstrap_id.to_string();
    match Spi::connect_mut(|client| {
        activate_projection_bootstrap(client, &bootstrap_id, SlotValidation::Required)
    }) {
        Ok(value) => pgrx::JsonB(value),
        Err(error) => {
            log!("synchro projection bootstrap activation failed: {error}");
            pgrx::error!("projection bootstrap activation failed")
        }
    }
}

#[pg_extern(stable)]
fn synchro_projection_bootstrap_status(bootstrap_id: pgrx::Uuid) -> pgrx::JsonB {
    let bootstrap_id = bootstrap_id.to_string();
    match Spi::connect_mut(|client| {
        let reset = load_reset(client, &bootstrap_id, false)?;
        if !reset.is_projection_bootstrap() {
            return Err("projection bootstrap does not exist".to_string());
        }
        Ok(serde_json::json!({
            "bootstrap_id": reset.reset_id,
            "lifecycle": reset.lifecycle,
            "source_registry_generation": reset.source_registry_generation,
            "target_registry_generation": reset.target_registry_generation,
            "candidate_slot_name": reset.candidate_slot_name,
            "consistent_point": reset.consistent_point,
            "activation_barrier": reset.activation_barrier,
            "candidate_materialized_commit_lsn": reset.candidate_materialized_commit_lsn,
            "candidate_materialized_end_lsn": reset.candidate_materialized_end_lsn,
            "candidate_acknowledged_end_lsn": reset.candidate_acknowledged_end_lsn,
            "candidate_verified": reset.candidate_verified,
            "affected_scopes": reset.affected_scopes,
        }))
    }) {
        Ok(value) => pgrx::JsonB(value),
        Err(_) => pgrx::error!("projection bootstrap status failed"),
    }
}

#[pg_extern(volatile)]
fn synchro_abort_projection_bootstrap(bootstrap_id: pgrx::Uuid) -> pgrx::JsonB {
    let bootstrap_id = bootstrap_id.to_string();
    match Spi::connect_mut(|client| abort_candidate_operation(client, &bootstrap_id, true)) {
        Ok(value) => pgrx::JsonB(value),
        Err(_) => pgrx::error!("projection bootstrap abort failed"),
    }
}

#[pg_extern(volatile)]
fn synchro_complete_projection_bootstrap_cleanup(bootstrap_id: pgrx::Uuid) -> bool {
    let bootstrap_id = bootstrap_id.to_string();
    match Spi::connect_mut(|client| complete_candidate_cleanup(client, &bootstrap_id, true)) {
        Ok(()) => true,
        Err(_) => pgrx::error!("projection bootstrap cleanup completion failed"),
    }
}

fn prepare_stream_reset(
    client: &mut SpiClient<'_>,
    candidate_slot_name: &str,
) -> Result<serde_json::Value, String> {
    acquire_stream_reset_operation_lock(client)?;
    validate_slot_name(candidate_slot_name)?;
    acquire_registry_write_lock(client).map_err(|_| "locking registry failed".to_string())?;
    client
        .update(
            "LOCK TABLE synchro.sync_stream_resets IN SHARE ROW EXCLUSIVE MODE",
            None,
            &[],
        )
        .map_err(|_| "locking reset state failed".to_string())?;
    let existing = required_bool(
        &client
            .select(
                "SELECT EXISTS (
                     SELECT 1 FROM synchro.sync_stream_resets
                     WHERE lifecycle IN ('preparing', 'baseline_staged', 'activated')
                 ) AS present",
                None,
                &[],
            )
            .map_err(|_| "loading reset state failed".to_string())?
            .first(),
        "present",
    )?;
    if existing {
        return Err("a stream reset is already active".to_string());
    }

    let row = client
        .select(
            "SELECT runtime.stream_generation,
                    runtime.active_slot_name::text AS old_slot_name,
                    progress.registry_generation,
                    database.oid::bigint AS database_oid,
                    database.datname::text AS database_name
             FROM synchro.sync_runtime_state runtime
             JOIN synchro.sync_wal_progress progress ON progress.singleton
             JOIN synchro.sync_registry_generations registry
               ON registry.generation = progress.registry_generation
              AND registry.state = 'active'
              AND registry.validated
              AND registry.stream_generation = runtime.stream_generation
             JOIN pg_catalog.pg_database database
               ON database.datname = pg_catalog.current_database()
             WHERE runtime.singleton",
            None,
            &[],
        )
        .map_err(|_| "loading active stream failed".to_string())?
        .first();
    let source_stream_generation = required_text(&row, "stream_generation")?;
    let old_slot_name = required_text(&row, "old_slot_name")?;
    validate_slot_name(&old_slot_name)?;
    if candidate_slot_name == old_slot_name {
        return Err("candidate slot is already active".to_string());
    }
    let source_registry_generation = required_positive_i64(&row, "registry_generation")?;
    let database_oid = required_positive_i64(&row, "database_oid")?;
    let database_name = required_text(&row, "database_name")?;

    let inserted = client
        .select(
            "INSERT INTO synchro.sync_stream_resets (
                 reset_id, operation_kind, source_stream_generation, target_stream_generation,
                 source_registry_generation, old_slot_name, candidate_slot_name,
                 database_oid, database_name, plugin, lifecycle
             ) VALUES (
                 gen_random_uuid(), 'stream_reset', $1, gen_random_uuid()::text,
                 $2, $3, $4, $5::oid, $6, 'pgoutput', 'preparing'
             )
             RETURNING reset_id::text AS reset_id, target_stream_generation",
            None,
            &[
                source_stream_generation.as_str().into(),
                source_registry_generation.into(),
                old_slot_name.as_str().into(),
                candidate_slot_name.into(),
                database_oid.into(),
                database_name.as_str().into(),
            ],
        )
        .map_err(|_| "creating reset state failed".to_string())?
        .first();
    Ok(serde_json::json!({
        "reset_id": required_text(&inserted, "reset_id")?,
        "target_stream_generation": required_text(&inserted, "target_stream_generation")?,
        "old_slot_name": old_slot_name,
    }))
}

fn prepare_projection_bootstrap(
    client: &mut SpiClient<'_>,
    target_registry_generation: i64,
    candidate_slot_name: &str,
) -> Result<serde_json::Value, String> {
    acquire_stream_reset_operation_lock(client)?;
    validate_slot_name(candidate_slot_name)?;
    acquire_registry_write_lock(client).map_err(|_| "locking registry failed".to_string())?;
    lock_reset_state(client)?;
    if target_registry_generation <= 0 {
        return Err("projection bootstrap registry generation is invalid".to_string());
    }
    let active = client
        .select(
            "SELECT runtime.stream_generation,
                    runtime.active_slot_name::text AS old_slot_name,
                    progress.registry_generation AS source_registry_generation,
                    database.oid::bigint AS database_oid,
                    database.datname::text AS database_name
             FROM synchro.sync_runtime_state runtime
             JOIN synchro.sync_wal_progress progress ON progress.singleton
             JOIN synchro.sync_registry_generations source
               ON source.generation = progress.registry_generation
              AND source.state = 'active'
              AND source.validated
              AND source.stream_generation = runtime.stream_generation
             JOIN pg_catalog.pg_database database
               ON database.datname = pg_catalog.current_database()
             WHERE runtime.singleton",
            None,
            &[],
        )
        .map_err(|_| "loading active stream failed".to_string())?
        .first();
    let source_stream_generation = required_text(&active, "stream_generation")?;
    let old_slot_name = required_text(&active, "old_slot_name")?;
    let source_registry_generation = required_positive_i64(&active, "source_registry_generation")?;
    let database_oid = required_positive_i64(&active, "database_oid")?;
    let database_name = required_text(&active, "database_name")?;
    if old_slot_name == candidate_slot_name {
        return Err("candidate slot is already active".to_string());
    }
    pending_generation_chain(
        client,
        source_registry_generation,
        target_registry_generation,
        &source_stream_generation,
    )?;
    if !crate::schema::generation_requires_projection_bootstrap(client, target_registry_generation)
        .map_err(|_| "projection bootstrap registry classification failed".to_string())?
    {
        return Err("projection bootstrap is not required for this generation".to_string());
    }
    let pending = crate::schema::prepare_pending_manifest(client, target_registry_generation)?;
    let inserted = client
        .select(
            "INSERT INTO synchro.sync_stream_resets (
                 reset_id, operation_kind, source_stream_generation,
                 target_stream_generation, source_registry_generation,
                 target_registry_generation, old_slot_name, candidate_slot_name,
                 database_oid, database_name, plugin, lifecycle,
                 target_schema_version, target_schema_hash,
                 target_canonical_manifest_body
             ) VALUES (
                 gen_random_uuid(), 'projection_bootstrap', $1, $1, $2, $3,
                 $4, $5, $6::oid, $7, 'pgoutput', 'preparing', $8, $9, $10
             ) RETURNING reset_id::text AS reset_id",
            None,
            &[
                source_stream_generation.as_str().into(),
                source_registry_generation.into(),
                target_registry_generation.into(),
                old_slot_name.as_str().into(),
                candidate_slot_name.into(),
                database_oid.into(),
                database_name.as_str().into(),
                pending.as_ref().map(|manifest| manifest.version).into(),
                pending
                    .as_ref()
                    .map(|manifest| manifest.hash.as_str())
                    .into(),
                pending
                    .as_ref()
                    .map(|manifest| manifest.canonical_body.as_str())
                    .into(),
            ],
        )
        .map_err(|_| "creating projection bootstrap state failed".to_string())?
        .first();
    Ok(serde_json::json!({
        "bootstrap_id": required_text(&inserted, "reset_id")?,
        "registry_generation": target_registry_generation,
        "schema_version": pending.as_ref().map(|manifest| manifest.version),
        "schema_hash": pending.as_ref().map(|manifest| manifest.hash.as_str()),
        "candidate_slot_name": candidate_slot_name,
    }))
}

fn mark_stream_reset_snapshot(
    client: &mut SpiClient<'_>,
    reset_id: &str,
    phase: &str,
) -> Result<serde_json::Value, String> {
    acquire_stream_reset_operation_lock(client)?;
    if !matches!(phase, "before" | "after") {
        return Err("reset snapshot marker phase is invalid".to_string());
    }
    lock_reset_state(client)?;
    let reset = load_reset(client, reset_id, false)?;
    if reset.lifecycle != "preparing" {
        return Err("reset is not preparing".to_string());
    }
    if phase == "before" {
        verify_source_locks(
            client,
            reset.staging_registry_generation()?,
            !reset.is_projection_bootstrap(),
        )?;
    }
    if phase == "after" {
        let before_exists = required_bool(
            &client
                .select(
                    "SELECT EXISTS (
                         SELECT 1 FROM synchro.sync_stream_reset_snapshot_markers
                         WHERE reset_id = $1::uuid AND phase = 'before'
                     ) AS present",
                    None,
                    &[reset_id.into()],
                )
                .map_err(|_| "checking reset snapshot marker failed".to_string())?
                .first(),
            "present",
        )?;
        if !before_exists {
            return Err("reset snapshot before marker is missing".to_string());
        }
    }
    let row = client
        .select(
            "INSERT INTO synchro.sync_stream_reset_snapshot_markers (
                 reset_id, phase, marker_xid, marker_nonce
             ) VALUES ($1::uuid, $2, pg_current_xact_id(), gen_random_uuid())
             RETURNING marker_xid::text AS marker_xid, marker_nonce::text AS marker_nonce",
            None,
            &[reset_id.into(), phase.into()],
        )
        .map_err(|_| "creating reset snapshot marker failed".to_string())?
        .first();
    Ok(serde_json::json!({
        "marker_xid": required_text(&row, "marker_xid")?,
        "marker_nonce": required_text(&row, "marker_nonce")?,
    }))
}

fn lock_stream_reset_sources(
    client: &mut SpiClient<'_>,
    reset_id: &str,
    lifetime: SourceLockLifetime,
) -> Result<(), String> {
    let operation_lock_function = if lifetime == SourceLockLifetime::Session {
        "pg_advisory_lock_shared"
    } else {
        "pg_advisory_xact_lock_shared"
    };
    client
        .select(
            &format!("SELECT pg_catalog.{operation_lock_function}($1::bigint)"),
            None,
            &[crate::STREAM_RESET_OPERATION_LOCK_KEY.into()],
        )
        .map_err(|_| "locking stream reset operation failed".to_string())?;
    let result = (|| {
        let reset = load_reset(client, reset_id, false)?;
        if reset.lifecycle != "preparing" {
            return Err("reset is not preparing".to_string());
        }
        validate_active_reset_binding(client, &reset)?;

        if lifetime == SourceLockLifetime::Session {
            client
                .select(
                    "SELECT pg_catalog.pg_advisory_lock($1::bigint)",
                    None,
                    &[0x7379_6e63i64.into()],
                )
                .map_err(|_| "locking registry failed".to_string())?;
        } else {
            acquire_registry_write_lock(client)
                .map_err(|_| "locking registry failed".to_string())?;
        }
        lock_stream_reset_sources_after_registry_lock(client, &reset, lifetime)
    })();
    if result.is_err() && lifetime == SourceLockLifetime::Session {
        let _ = client.select("SELECT pg_catalog.pg_advisory_unlock_all()", None, &[]);
    }
    result
}

fn lock_stream_reset_sources_after_registry_lock(
    client: &mut SpiClient<'_>,
    reset: &ResetRecord,
    lifetime: SourceLockLifetime,
) -> Result<(), String> {
    let lock_function = if lifetime == SourceLockLifetime::Session {
        "pg_advisory_lock"
    } else {
        "pg_advisory_xact_lock"
    };
    let mut lock_keys = vec![crate::SOURCE_WRITE_GATE_LOCK_KEY];
    if !reset.is_projection_bootstrap() {
        lock_keys.push(crate::WAL_WORKER_GATE_LOCK_KEY);
    }
    for lock_key in lock_keys {
        client
            .update(
                &format!("SELECT pg_catalog.{lock_function}($1::bigint)"),
                None,
                &[lock_key.into()],
            )
            .map_err(|_| "locking reset gate failed".to_string())?;
    }
    let reset = load_reset(client, &reset.reset_id, false)?;
    if reset.lifecycle != "preparing" {
        return Err("reset is not preparing".to_string());
    }
    validate_active_reset_binding(client, &reset)?;
    if lifetime == SourceLockLifetime::Transaction {
        lock_reset_configuration(client)?;
    }
    let registry =
        load_registry_generation_from_client(client, reset.staging_registry_generation()?)
            .map_err(|_| "loading reset registry failed".to_string())?;
    let mut relation_ids = registry
        .iter()
        .map(|registration| registration.relation_id.as_str())
        .collect::<Vec<_>>();
    relation_ids.sort_unstable();
    for relation_id in relation_ids {
        client
            .update(
                &format!(
                    "SELECT pg_catalog.{lock_function}(\
                         pg_catalog.hashtextextended('synchro:relation:' || $1, 0)\
                     )"
                ),
                None,
                &[relation_id.into()],
            )
            .map_err(|_| "locking registered source failed".to_string())?;
    }
    Ok(())
}

fn lock_reset_configuration(client: &mut SpiClient<'_>) -> Result<(), String> {
    for table in [
        "synchro.sync_runtime_state",
        "synchro.sync_registry_generations",
        "synchro.sync_registry",
        "synchro.sync_registry_fields",
        "synchro.sync_capture_dependency_fields",
        "synchro.sync_membership_dependencies",
        "synchro.sync_schema_manifest",
        "synchro.sync_wal_progress",
    ] {
        client
            .update(
                &format!("LOCK TABLE {table} IN SHARE ROW EXCLUSIVE MODE"),
                None,
                &[],
            )
            .map_err(|_| "locking reset configuration failed".to_string())?;
    }
    Ok(())
}

fn acquire_stream_reset_operation_lock(client: &SpiClient<'_>) -> Result<(), String> {
    client
        .select(
            "SELECT pg_catalog.pg_advisory_xact_lock_shared($1::bigint)",
            None,
            &[crate::STREAM_RESET_OPERATION_LOCK_KEY.into()],
        )
        .map_err(|_| "locking stream reset operation failed".to_string())?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn stage_stream_reset(
    client: &mut SpiClient<'_>,
    reset_id: &str,
    candidate_slot_name: &str,
    consistent_point: &str,
    exported_snapshot_name: &str,
    snapshot_binding: Option<SnapshotBinding<'_>>,
    slot_validation: SlotValidation,
) -> Result<serde_json::Value, String> {
    acquire_stream_reset_operation_lock(client)?;
    validate_slot_name(candidate_slot_name)?;
    validate_snapshot_name(exported_snapshot_name)?;
    let normalized_point = normalize_lsn(client, consistent_point)?;
    lock_reset_state(client)?;
    let reset = load_reset(client, reset_id, false)?;
    if reset.is_projection_bootstrap()
        || reset.lifecycle != "preparing"
        || reset.candidate_slot_name != candidate_slot_name
    {
        return Err("reset staging metadata is invalid".to_string());
    }
    validate_active_reset_binding(client, &reset)?;
    verify_source_locks(
        client,
        reset.staging_registry_generation()?,
        !reset.is_projection_bootstrap(),
    )?;
    if slot_validation == SlotValidation::Required {
        validate_imported_snapshot_transaction(client)?;
        validate_snapshot_binding(
            client,
            &reset,
            snapshot_binding
                .as_ref()
                .ok_or_else(|| "reset snapshot binding is missing".to_string())?,
        )?;
        validate_candidate_slot(client, &reset, &normalized_point)?;
    }

    clear_staging(client, reset_id)?;
    stage_existing_versions(client, &reset)?;
    let registry =
        load_registry_generation_from_client(client, reset.staging_registry_generation()?)
            .map_err(|_| "loading reset registry failed".to_string())?;
    for registration in &registry {
        stage_registration(client, &reset, registration)?;
    }
    prune_stale_versions(client, &reset)?;
    select_staged_projection(client, &reset)?;
    for registration in &registry {
        stage_registration_membership(client, &reset, registration)?;
    }
    stage_fence_coverage(client, &reset, &normalized_point)?;
    verify_staging(client, &reset, &registry, &normalized_point)?;
    stage_scope_digests(client, &reset, &registry)?;
    verify_scope_digests(client, &reset, &registry)?;

    let counts = staging_counts(client, reset_id)?;
    let updated = client
        .update(
            "UPDATE synchro.sync_stream_resets
             SET lifecycle = 'baseline_staged',
                  consistent_point = $2::pg_lsn,
                  exported_snapshot_name = $3,
                  snapshot_before_xid = $4::xid8,
                  snapshot_before_nonce = $5::uuid,
                  snapshot_after_xid = $6::xid8,
                  snapshot_after_nonce = $7::uuid,
                  staged_row_count = $8,
                  staged_version_count = $9,
                  staged_edge_count = $10,
                  staged_fence_count = $11,
                  staged_scope_count = $12,
                 baseline_staged_at = now(), updated_at = now()
             WHERE reset_id = $1::uuid AND lifecycle = 'preparing'",
            None,
            &[
                reset_id.into(),
                normalized_point.as_str().into(),
                exported_snapshot_name.into(),
                snapshot_binding
                    .as_ref()
                    .map(|binding| binding.before_xid)
                    .into(),
                snapshot_binding
                    .as_ref()
                    .map(|binding| binding.before_nonce)
                    .into(),
                snapshot_binding
                    .as_ref()
                    .map(|binding| binding.after_xid)
                    .into(),
                snapshot_binding
                    .as_ref()
                    .map(|binding| binding.after_nonce)
                    .into(),
                counts[0].into(),
                counts[1].into(),
                counts[2].into(),
                counts[3].into(),
                counts[4].into(),
            ],
        )
        .map_err(|_| "persisting staged reset failed".to_string())?
        .len();
    if updated != 1 {
        return Err("reset staging state changed".to_string());
    }
    Ok(serde_json::json!({
        "reset_id": reset.reset_id,
        "target_stream_generation": reset.target_stream_generation,
        "candidate_slot_name": reset.candidate_slot_name,
        "consistent_point": normalized_point,
        "rows": counts[0],
        "versions": counts[1],
        "edges": counts[2],
        "fences": counts[3],
        "scopes": counts[4],
    }))
}

#[allow(clippy::too_many_arguments)]
fn stage_projection_bootstrap(
    client: &mut SpiClient<'_>,
    bootstrap_id: &str,
    candidate_slot_name: &str,
    consistent_point: &str,
    exported_snapshot_name: &str,
    snapshot_binding: Option<SnapshotBinding<'_>>,
    slot_validation: SlotValidation,
) -> Result<serde_json::Value, String> {
    acquire_stream_reset_operation_lock(client)?;
    validate_slot_name(candidate_slot_name)?;
    validate_snapshot_name(exported_snapshot_name)?;
    let normalized_point = normalize_lsn(client, consistent_point)?;
    lock_reset_state(client)?;
    let reset = load_reset(client, bootstrap_id, false)?;
    if !reset.is_projection_bootstrap()
        || reset.lifecycle != "preparing"
        || reset.candidate_slot_name != candidate_slot_name
    {
        return Err("projection bootstrap staging metadata is invalid".to_string());
    }
    validate_active_reset_binding(client, &reset)?;
    let staging_generation = reset.staging_registry_generation()?;
    verify_source_locks(client, staging_generation, false)?;
    if slot_validation == SlotValidation::Required {
        validate_imported_snapshot_transaction(client)?;
        validate_snapshot_binding(
            client,
            &reset,
            snapshot_binding
                .as_ref()
                .ok_or_else(|| "projection bootstrap snapshot binding is missing".to_string())?,
        )?;
        validate_candidate_slot(client, &reset, &normalized_point)?;
    }

    clear_staging(client, bootstrap_id)?;
    stage_existing_versions(client, &reset)?;
    let registry = load_registry_generation_from_client(client, staging_generation)
        .map_err(|_| "loading projection bootstrap registry failed".to_string())?;
    for registration in &registry {
        stage_registration(client, &reset, registration)?;
    }
    prune_stale_versions(client, &reset)?;
    select_staged_projection(client, &reset)?;
    for registration in &registry {
        stage_registration_membership(client, &reset, registration)?;
    }
    stage_fence_coverage(client, &reset, &normalized_point)?;
    verify_staging(client, &reset, &registry, &normalized_point)?;
    stage_scope_digests(client, &reset, &registry)?;
    verify_scope_digests(client, &reset, &registry)?;
    let counts = staging_counts(client, bootstrap_id)?;
    let updated = client
        .update(
            "UPDATE synchro.sync_stream_resets
             SET lifecycle = 'baseline_staged',
                  consistent_point = $2::pg_lsn,
                  exported_snapshot_name = $3,
                  snapshot_before_xid = $4::xid8,
                  snapshot_before_nonce = $5::uuid,
                  snapshot_after_xid = $6::xid8,
                  snapshot_after_nonce = $7::uuid,
                  staged_row_count = $8,
                  staged_version_count = $9,
                   staged_edge_count = $10,
                   staged_fence_count = $11,
                   staged_scope_count = $12,
                   candidate_materialized_commit_lsn = NULL,
                   candidate_materialized_end_lsn = NULL,
                   candidate_acknowledged_end_lsn = $2::pg_lsn,
                   candidate_verified = false,
                  baseline_staged_at = now(), updated_at = now()
             WHERE reset_id = $1::uuid
               AND operation_kind = 'projection_bootstrap'
               AND lifecycle = 'preparing'",
            None,
            &[
                bootstrap_id.into(),
                normalized_point.as_str().into(),
                exported_snapshot_name.into(),
                snapshot_binding
                    .as_ref()
                    .map(|value| value.before_xid)
                    .into(),
                snapshot_binding
                    .as_ref()
                    .map(|value| value.before_nonce)
                    .into(),
                snapshot_binding
                    .as_ref()
                    .map(|value| value.after_xid)
                    .into(),
                snapshot_binding
                    .as_ref()
                    .map(|value| value.after_nonce)
                    .into(),
                counts[0].into(),
                counts[1].into(),
                counts[2].into(),
                counts[3].into(),
                counts[4].into(),
            ],
        )
        .map_err(|_| "persisting projection bootstrap failed".to_string())?
        .len();
    if updated != 1 {
        return Err("projection bootstrap state changed".to_string());
    }
    Ok(serde_json::json!({
        "bootstrap_id": reset.reset_id,
        "registry_generation": staging_generation,
        "candidate_slot_name": reset.candidate_slot_name,
        "consistent_point": normalized_point,
        "rows": counts[0],
        "versions": counts[1],
        "edges": counts[2],
        "fences": counts[3],
        "scopes": counts[4],
    }))
}

fn request_projection_bootstrap_barrier(
    client: &mut SpiClient<'_>,
    bootstrap_id: &str,
) -> Result<serde_json::Value, String> {
    acquire_stream_reset_operation_lock(client)?;
    lock_reset_state(client)?;
    let reset = load_reset(client, bootstrap_id, false)?;
    if !reset.is_projection_bootstrap() || reset.lifecycle != "baseline_staged" {
        return Err("projection bootstrap baseline is not staged".to_string());
    }
    validate_active_reset_binding(client, &reset)?;
    let boundary = client
        .update(
            "SELECT materialized_commit_lsn::text AS commit_lsn,
                    materialized_end_lsn::text AS end_lsn
             FROM synchro.sync_wal_progress
             WHERE singleton
               AND stream_generation = $1
               AND materialized_commit_lsn IS NOT NULL
               AND materialized_end_lsn IS NOT NULL
             FOR UPDATE",
            None,
            &[reset.source_stream_generation.as_str().into()],
        )
        .map_err(|_| "loading main worker boundary failed".to_string())?
        .first();
    let commit_lsn = required_text(&boundary, "commit_lsn")?;
    let end_lsn = required_text(&boundary, "end_lsn")?;
    let consistent_point = reset
        .consistent_point
        .as_deref()
        .ok_or_else(|| "projection bootstrap consistent point is missing".to_string())?;
    let advances_snapshot = required_bool(
        &client
            .select(
                "SELECT $1::pg_lsn > $2::pg_lsn AS advances",
                None,
                &[end_lsn.as_str().into(), consistent_point.into()],
            )
            .map_err(|_| "validating projection bootstrap barrier failed".to_string())?
            .first(),
        "advances",
    )?;
    if !advances_snapshot {
        return Err("projection bootstrap barrier does not follow its snapshot".to_string());
    }
    let updated = client
        .update(
            "UPDATE synchro.sync_stream_resets
             SET lifecycle = 'catching_up', activation_barrier = $2::pg_lsn,
                 candidate_verified = false, updated_at = now()
             WHERE reset_id = $1::uuid
               AND operation_kind = 'projection_bootstrap'
               AND lifecycle = 'baseline_staged'",
            None,
            &[bootstrap_id.into(), end_lsn.as_str().into()],
        )
        .map_err(|_| "recording projection bootstrap barrier failed".to_string())?
        .len();
    if updated != 1 {
        return Err("projection bootstrap state changed".to_string());
    }
    Ok(serde_json::json!({
        "bootstrap_id": reset.reset_id,
        "stream_generation": reset.source_stream_generation,
        "commit_lsn": commit_lsn,
        "end_lsn": end_lsn,
    }))
}

fn emit_projection_bootstrap_barrier(
    client: &mut SpiClient<'_>,
    bootstrap_id: &str,
) -> Result<serde_json::Value, String> {
    acquire_stream_reset_operation_lock(client)?;
    lock_reset_state(client)?;
    let reset = load_reset(client, bootstrap_id, false)?;
    if !reset.is_projection_bootstrap() || reset.lifecycle != "baseline_staged" {
        return Err("projection bootstrap baseline is not staged".to_string());
    }
    validate_active_reset_binding(client, &reset)?;
    let marker = client
        .select(
            "SELECT pg_catalog.pg_logical_emit_message(
                 true, 'synchro_projection_bootstrap',
                 pg_catalog.convert_to($1, 'UTF8')
             )::text AS marker_lsn",
            None,
            &[bootstrap_id.into()],
        )
        .map_err(|_| "emitting projection bootstrap barrier failed".to_string())?
        .first();
    Ok(serde_json::json!({
        "bootstrap_id": bootstrap_id,
        "marker_lsn": required_text(&marker, "marker_lsn")?,
    }))
}

fn activate_projection_bootstrap(
    client: &mut SpiClient<'_>,
    bootstrap_id: &str,
    slot_validation: SlotValidation,
) -> Result<serde_json::Value, String> {
    acquire_stream_reset_operation_lock(client)?;
    client
        .select(
            "SELECT pg_catalog.pg_advisory_xact_lock($1::bigint)",
            None,
            &[crate::WAL_WORKER_GATE_LOCK_KEY.into()],
        )
        .map_err(|_| "locking WAL worker for projection bootstrap activation failed".to_string())?;
    acquire_registry_write_lock(client).map_err(|_| "locking registry failed".to_string())?;
    client
        .select(
            "SELECT pg_catalog.pg_advisory_xact_lock($1::bigint)",
            None,
            &[crate::SOURCE_WRITE_GATE_LOCK_KEY.into()],
        )
        .map_err(|_| "locking projection bootstrap source writes failed".to_string())?;
    lock_reset_state(client)?;
    client
        .update(
            "SELECT 1 FROM synchro.sync_wal_progress WHERE singleton FOR UPDATE",
            None,
            &[],
        )
        .map_err(|_| "locking WAL worker progress failed".to_string())?;
    lock_reset_configuration(client)?;
    let reset = load_reset(client, bootstrap_id, false)?;
    if !reset.is_projection_bootstrap() || reset.lifecycle != "catching_up" {
        return Err("projection bootstrap is not catching up".to_string());
    }
    let target_generation = reset.staging_registry_generation()?;
    let barrier = reset
        .activation_barrier
        .as_deref()
        .ok_or_else(|| "projection bootstrap barrier is missing".to_string())?;
    if !reset.candidate_verified || reset.candidate_materialized_end_lsn.as_deref() != Some(barrier)
    {
        return Err("projection bootstrap candidate has not reached the barrier".to_string());
    }
    validate_active_reset_binding(client, &reset)?;
    if slot_validation == SlotValidation::Required {
        verify_persisted_snapshot_binding(client, &reset)?;
        validate_candidate_slot(client, &reset, barrier)?;
    }
    let main_reached = required_bool(
        &client
            .select(
                "SELECT progress.materialized_end_lsn >= $1::pg_lsn
                        AND EXISTS (
                            SELECT 1
                            FROM synchro.sync_wal_transactions transaction
                            WHERE transaction.stream_generation = progress.stream_generation
                              AND transaction.end_lsn = $1::pg_lsn
                        ) AS reached
                 FROM synchro.sync_wal_progress progress
                 WHERE progress.singleton
                   AND progress.stream_generation = $2",
                None,
                &[
                    barrier.into(),
                    reset.source_stream_generation.as_str().into(),
                ],
            )
            .map_err(|_| "checking main worker barrier failed".to_string())?
            .first(),
        "reached",
    )?;
    if !main_reached {
        return Err("main worker has not reached the projection bootstrap barrier".to_string());
    }
    let registry = load_registry_generation_from_client(client, target_generation)
        .map_err(|_| "loading projection bootstrap registry failed".to_string())?;
    select_staged_projection(client, &reset)?;
    verify_projection_stage_integrity(client, &reset, &registry)?;
    verify_scope_digests(client, &reset, &registry)?;
    verify_persisted_counts(client, &reset)?;
    let affected_scopes = projection_bootstrap_affected_scopes(client, &reset, &registry)?;

    cover_projection_bootstrap_fences(client, &reset, barrier)?;
    replace_live_projection(client, &reset)?;
    activate_projection_registry(client, &reset, target_generation, barrier, &affected_scopes)?;
    crate::materialize::invalidate_affected_membership_generation(
        client,
        &affected_scopes,
        target_generation,
    )?;
    let schema_reference = match (
        reset.target_schema_version,
        reset.target_schema_hash.as_deref(),
        reset.target_canonical_manifest_body.as_deref(),
    ) {
        (Some(version), Some(hash), Some(body)) => {
            crate::schema::publish_pending_manifest(
                client,
                target_generation,
                version,
                hash,
                body,
                affected_scopes.clone(),
            )?;
            Some((version, hash))
        }
        (None, None, None) => None,
        _ => return Err("projection bootstrap schema binding is incomplete".to_string()),
    };
    let updated = client
        .update(
            "UPDATE synchro.sync_stream_resets
             SET lifecycle = 'activated', affected_scopes = $2::text[],
                 activated_at = now(), updated_at = now()
             WHERE reset_id = $1::uuid
               AND operation_kind = 'projection_bootstrap'
               AND lifecycle = 'catching_up'",
            None,
            &[bootstrap_id.into(), affected_scopes.clone().into()],
        )
        .map_err(|_| "activating projection bootstrap failed".to_string())?
        .len();
    if updated != 1 {
        return Err("projection bootstrap state changed".to_string());
    }
    Ok(serde_json::json!({
        "bootstrap_id": reset.reset_id,
        "registry_generation": target_generation,
        "schema_version": schema_reference.map(|reference| reference.0),
        "schema_hash": schema_reference.map(|reference| reference.1),
        "activation_barrier": barrier,
        "affected_scopes": affected_scopes,
    }))
}

fn activate_stream_reset(
    client: &mut SpiClient<'_>,
    reset_id: &str,
    slot_validation: SlotValidation,
) -> Result<serde_json::Value, String> {
    acquire_stream_reset_operation_lock(client)?;
    lock_reset_state(client)?;
    client
        .update(
            "SELECT 1 FROM synchro.sync_wal_progress WHERE singleton FOR UPDATE",
            None,
            &[],
        )
        .map_err(|_| "locking WAL worker progress failed".to_string())?;
    lock_reset_configuration(client)?;
    let reset = load_reset(client, reset_id, false)?;
    if reset.lifecycle != "baseline_staged" {
        return Err("reset baseline is not staged".to_string());
    }
    let consistent_point = reset
        .consistent_point
        .as_deref()
        .ok_or_else(|| "reset consistent point is missing".to_string())?;
    if reset.exported_snapshot_name.is_none() {
        return Err("reset snapshot is missing".to_string());
    }
    validate_active_reset_binding(client, &reset)?;
    verify_source_locks(client, reset.source_registry_generation, true)?;
    if slot_validation == SlotValidation::Required {
        verify_persisted_snapshot_binding(client, &reset)?;
        validate_candidate_slot(client, &reset, consistent_point)?;
    }
    let registry = load_registry_generation_from_client(client, reset.source_registry_generation)
        .map_err(|_| "loading reset registry failed".to_string())?;
    select_staged_projection(client, &reset)?;
    verify_staging(client, &reset, &registry, consistent_point)?;
    verify_scope_digests(client, &reset, &registry)?;
    verify_persisted_counts(client, &reset)?;

    replace_live_projection(client, &reset)?;
    cover_pending_fences(client, &reset, consistent_point)?;
    invalidate_client_state(client, &reset)?;
    activate_registry_and_runtime(client, &reset)?;

    let updated = client
        .update(
            "UPDATE synchro.sync_stream_resets
             SET lifecycle = 'activated', activation_barrier = $2::pg_lsn,
                 activated_at = now(), updated_at = now()
             WHERE reset_id = $1::uuid AND lifecycle = 'baseline_staged'",
            None,
            &[reset_id.into(), consistent_point.into()],
        )
        .map_err(|_| "marking reset activated failed".to_string())?
        .len();
    if updated != 1 {
        return Err("reset activation state changed".to_string());
    }

    Ok(serde_json::json!({
        "reset_id": reset.reset_id,
        "stream_generation": reset.target_stream_generation,
        "active_slot_name": reset.candidate_slot_name,
        "activation_barrier": consistent_point,
        "old_slot_name": reset.old_slot_name,
    }))
}

fn abort_stream_reset(
    client: &mut SpiClient<'_>,
    reset_id: &str,
) -> Result<serde_json::Value, String> {
    abort_candidate_operation(client, reset_id, false)
}

fn abort_candidate_operation(
    client: &mut SpiClient<'_>,
    reset_id: &str,
    projection_bootstrap: bool,
) -> Result<serde_json::Value, String> {
    acquire_stream_reset_operation_lock(client)?;
    lock_reset_state(client)?;
    let reset = load_reset(client, reset_id, false)?;
    if reset.is_projection_bootstrap() != projection_bootstrap
        || !matches!(
            reset.lifecycle.as_str(),
            "preparing" | "baseline_staged" | "catching_up"
        )
        || (!projection_bootstrap && reset.lifecycle == "catching_up")
    {
        return Err("reset cannot be aborted".to_string());
    }
    clear_staging(client, reset_id)?;
    let updated = client
        .update(
            "UPDATE synchro.sync_stream_resets
             SET lifecycle = 'aborted', activation_barrier = NULL,
                 candidate_materialized_commit_lsn = NULL,
                 candidate_materialized_end_lsn = NULL,
                 candidate_acknowledged_end_lsn = NULL,
                 candidate_verified = false, affected_scopes = NULL,
                 aborted_at = now(), updated_at = now()
             WHERE reset_id = $1::uuid
               AND lifecycle IN ('preparing', 'baseline_staged', 'catching_up')",
            None,
            &[reset_id.into()],
        )
        .map_err(|_| "marking reset aborted failed".to_string())?
        .len();
    if updated != 1 {
        return Err("reset abort state changed".to_string());
    }
    Ok(serde_json::json!({
        "reset_id": reset.reset_id,
        "candidate_slot_name": reset.candidate_slot_name,
    }))
}

fn complete_stream_reset_cleanup(client: &mut SpiClient<'_>, reset_id: &str) -> Result<(), String> {
    complete_candidate_cleanup(client, reset_id, false)
}

fn complete_candidate_cleanup(
    client: &mut SpiClient<'_>,
    reset_id: &str,
    projection_bootstrap: bool,
) -> Result<(), String> {
    acquire_stream_reset_operation_lock(client)?;
    lock_reset_state(client)?;
    let reset = load_reset(client, reset_id, false)?;
    if reset.is_projection_bootstrap() != projection_bootstrap || reset.lifecycle != "activated" {
        return Err("reset is not activated".to_string());
    }
    let retired_slot = if projection_bootstrap {
        reset.candidate_slot_name.as_str()
    } else {
        reset.old_slot_name.as_str()
    };
    let retired_slot_present = required_bool(
        &client
            .select(
                "SELECT EXISTS (
                     SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = $1
                 ) AS present",
                None,
                &[retired_slot.into()],
            )
            .map_err(|_| "checking old slot failed".to_string())?
            .first(),
        "present",
    )?;
    if retired_slot_present {
        return Err("retired slot still exists".to_string());
    }
    clear_staging(client, reset_id)?;
    let updated = client
        .update(
            "UPDATE synchro.sync_stream_resets
             SET lifecycle = 'cleanup_complete', cleanup_completed_at = now(), updated_at = now()
             WHERE reset_id = $1::uuid AND lifecycle = 'activated'",
            None,
            &[reset_id.into()],
        )
        .map_err(|_| "marking reset cleanup complete failed".to_string())?
        .len();
    if updated != 1 {
        return Err("reset cleanup state changed".to_string());
    }
    Ok(())
}

fn load_reset(
    client: &mut SpiClient<'_>,
    reset_id: &str,
    _for_update: bool,
) -> Result<ResetRecord, String> {
    let rows = client
        .select(
            "SELECT reset_id::text AS reset_id, operation_kind, lifecycle,
                     source_stream_generation, target_stream_generation,
                     source_registry_generation, target_registry_generation,
                    old_slot_name::text AS old_slot_name,
                    candidate_slot_name::text AS candidate_slot_name,
                    database_oid::bigint AS database_oid,
                    database_name::text AS database_name, plugin,
                    consistent_point::text AS consistent_point,
                    exported_snapshot_name,
                     snapshot_before_xid::text AS snapshot_before_xid,
                     snapshot_after_xid::text AS snapshot_after_xid,
                     snapshot_before_nonce::text AS snapshot_before_nonce,
                     snapshot_after_nonce::text AS snapshot_after_nonce,
                     activation_barrier::text AS activation_barrier,
                     target_schema_version, target_schema_hash,
                     target_canonical_manifest_body,
                     candidate_materialized_commit_lsn::text AS candidate_materialized_commit_lsn,
                     candidate_materialized_end_lsn::text AS candidate_materialized_end_lsn,
                     candidate_acknowledged_end_lsn::text AS candidate_acknowledged_end_lsn,
                     candidate_verified, affected_scopes
              FROM synchro.sync_stream_resets
              WHERE reset_id = $1::uuid",
            None,
            &[reset_id.into()],
        )
        .map_err(|_| "loading reset failed".to_string())?;
    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| "reset does not exist".to_string())?;
    Ok(ResetRecord {
        reset_id: required_text(&row, "reset_id")?,
        operation_kind: required_text(&row, "operation_kind")?,
        lifecycle: required_text(&row, "lifecycle")?,
        source_stream_generation: required_text(&row, "source_stream_generation")?,
        target_stream_generation: required_text(&row, "target_stream_generation")?,
        source_registry_generation: required_positive_i64(&row, "source_registry_generation")?,
        target_registry_generation: row
            .get_by_name::<i64, &str>("target_registry_generation")
            .map_err(|_| "reading reset state failed".to_string())?,
        old_slot_name: required_text(&row, "old_slot_name")?,
        candidate_slot_name: required_text(&row, "candidate_slot_name")?,
        database_oid: required_positive_i64(&row, "database_oid")?,
        database_name: required_text(&row, "database_name")?,
        plugin: required_text(&row, "plugin")?,
        consistent_point: optional_text(&row, "consistent_point")?,
        exported_snapshot_name: optional_text(&row, "exported_snapshot_name")?,
        snapshot_before_xid: optional_text(&row, "snapshot_before_xid")?,
        snapshot_after_xid: optional_text(&row, "snapshot_after_xid")?,
        snapshot_before_nonce: optional_text(&row, "snapshot_before_nonce")?,
        snapshot_after_nonce: optional_text(&row, "snapshot_after_nonce")?,
        activation_barrier: optional_text(&row, "activation_barrier")?,
        target_schema_version: row
            .get_by_name::<i64, &str>("target_schema_version")
            .map_err(|_| "reading reset state failed".to_string())?,
        target_schema_hash: optional_text(&row, "target_schema_hash")?,
        target_canonical_manifest_body: optional_text(&row, "target_canonical_manifest_body")?,
        candidate_materialized_commit_lsn: optional_text(
            &row,
            "candidate_materialized_commit_lsn",
        )?,
        candidate_materialized_end_lsn: optional_text(&row, "candidate_materialized_end_lsn")?,
        candidate_acknowledged_end_lsn: optional_text(&row, "candidate_acknowledged_end_lsn")?,
        candidate_verified: required_bool(&row, "candidate_verified")?,
        affected_scopes: row
            .get_by_name::<Vec<String>, &str>("affected_scopes")
            .map_err(|_| "reading reset state failed".to_string())?,
    })
}

fn lock_reset_state(client: &mut SpiClient<'_>) -> Result<(), String> {
    client
        .update(
            "LOCK TABLE synchro.sync_stream_resets IN SHARE ROW EXCLUSIVE MODE",
            None,
            &[],
        )
        .map_err(|_| "locking reset state failed".to_string())?;
    Ok(())
}

fn validate_active_reset_binding(
    client: &SpiClient<'_>,
    reset: &ResetRecord,
) -> Result<(), String> {
    let valid = required_bool(
        &client
            .select(
                "SELECT EXISTS (
                     SELECT 1
                     FROM synchro.sync_runtime_state runtime
                     JOIN synchro.sync_wal_progress progress ON progress.singleton
                     JOIN synchro.sync_registry_generations registry
                       ON registry.generation = progress.registry_generation
                      AND registry.state = 'active'
                      AND registry.validated
                     JOIN pg_catalog.pg_database database
                       ON database.datname = pg_catalog.current_database()
                     WHERE runtime.singleton
                       AND runtime.stream_generation = $1
                       AND runtime.active_slot_name::text = $2
                       AND progress.stream_generation = $1
                       AND progress.registry_generation = $3
                       AND registry.stream_generation = $1
                       AND database.oid = $4::oid
                       AND database.datname::text = $5
                 ) AS valid",
                None,
                &[
                    reset.source_stream_generation.as_str().into(),
                    reset.old_slot_name.as_str().into(),
                    reset.source_registry_generation.into(),
                    reset.database_oid.into(),
                    reset.database_name.as_str().into(),
                ],
            )
            .map_err(|_| "validating active reset binding failed".to_string())?
            .first(),
        "valid",
    )?;
    if !valid {
        return Err("active stream changed during reset".to_string());
    }
    Ok(())
}

fn validate_candidate_slot(
    client: &SpiClient<'_>,
    reset: &ResetRecord,
    consistent_point: &str,
) -> Result<(), String> {
    if reset.plugin != "pgoutput" || reset.candidate_slot_name == reset.old_slot_name {
        return Err("candidate slot metadata is invalid".to_string());
    }
    let valid = required_bool(
        &client
            .select(
                "SELECT EXISTS (
                     SELECT 1
                     FROM pg_catalog.pg_replication_slots slot
                     JOIN pg_catalog.pg_database database ON database.oid = slot.datoid
                     WHERE slot.slot_name = $1
                       AND slot.slot_type = 'logical'
                       AND slot.plugin = $2
                       AND slot.datoid = $3::oid
                       AND database.datname::text = $4
                       AND NOT slot.temporary
                       AND NOT slot.active
                       AND slot.invalidation_reason IS NULL
                       AND slot.wal_status IS DISTINCT FROM 'lost'
                       AND slot.restart_lsn IS NOT NULL
                       AND slot.restart_lsn <= $5::pg_lsn
                       AND slot.confirmed_flush_lsn IS NOT NULL
                       AND slot.confirmed_flush_lsn = $5::pg_lsn
                 ) AS valid",
                None,
                &[
                    reset.candidate_slot_name.as_str().into(),
                    reset.plugin.as_str().into(),
                    reset.database_oid.into(),
                    reset.database_name.as_str().into(),
                    consistent_point.into(),
                ],
            )
            .map_err(|_| "validating candidate slot failed".to_string())?
            .first(),
        "valid",
    )?;
    if !valid {
        return Err("candidate slot is invalid".to_string());
    }
    Ok(())
}

fn validate_imported_snapshot_transaction(client: &SpiClient<'_>) -> Result<(), String> {
    let isolation = required_text(
        &client
            .select(
                "SELECT current_setting('transaction_isolation') AS isolation",
                None,
                &[],
            )
            .map_err(|_| "checking reset transaction failed".to_string())?
            .first(),
        "isolation",
    )?;
    if !matches!(isolation.as_str(), "repeatable read" | "serializable") {
        return Err("reset transaction has no imported snapshot isolation".to_string());
    }
    Ok(())
}

fn validate_snapshot_binding(
    client: &SpiClient<'_>,
    reset: &ResetRecord,
    binding: &SnapshotBinding<'_>,
) -> Result<(), String> {
    // The imported snapshot must not see the after marker. Activation validates
    // that marker and its nonce against current committed state.
    let valid = required_bool(
        &client
            .select(
                "SELECT EXISTS (
                 SELECT 1
                  FROM synchro.sync_stream_reset_snapshot_markers marker
                  WHERE marker.reset_id = $1::uuid
                    AND marker.phase = 'before'
                    AND marker.marker_xid = $2::xid8
                    AND marker.marker_nonce = $3::uuid
              )
              AND NOT EXISTS (
                   SELECT 1
                   FROM synchro.sync_stream_reset_snapshot_markers marker
                   WHERE marker.reset_id = $1::uuid
                     AND marker.phase = 'after'
                     AND marker.marker_xid = $4::xid8
                     AND marker.marker_nonce = $5::uuid
               )
              AND pg_catalog.pg_xact_status($4::xid8) = 'committed'
              AND pg_catalog.pg_visible_in_snapshot(
                   $2::xid8, pg_catalog.pg_current_snapshot()
                  )
                 AND NOT pg_catalog.pg_visible_in_snapshot(
                     $4::xid8, pg_catalog.pg_current_snapshot()
                 )
                 AND $2::xid8 < $4::xid8 AS valid",
                None,
                &[
                    reset.reset_id.as_str().into(),
                    binding.before_xid.into(),
                    binding.before_nonce.into(),
                    binding.after_xid.into(),
                    binding.after_nonce.into(),
                ],
            )
            .map_err(|_| "validating imported reset snapshot failed".to_string())?
            .first(),
        "valid",
    )?;
    if !valid {
        return Err("reset snapshot is not bound to the candidate slot".to_string());
    }
    Ok(())
}

fn verify_persisted_snapshot_binding(
    client: &SpiClient<'_>,
    reset: &ResetRecord,
) -> Result<(), String> {
    let before_xid = reset
        .snapshot_before_xid
        .as_deref()
        .ok_or_else(|| "reset snapshot before marker is missing".to_string())?;
    let after_xid = reset
        .snapshot_after_xid
        .as_deref()
        .ok_or_else(|| "reset snapshot after marker is missing".to_string())?;
    let before_nonce = reset
        .snapshot_before_nonce
        .as_deref()
        .ok_or_else(|| "reset snapshot before nonce is missing".to_string())?;
    let after_nonce = reset
        .snapshot_after_nonce
        .as_deref()
        .ok_or_else(|| "reset snapshot after nonce is missing".to_string())?;
    let valid = required_bool(
        &client
            .select(
                "SELECT count(*) = 2
                        AND bool_and(
                            (marker.phase = 'before'
                             AND marker.marker_xid = $2::xid8
                             AND marker.marker_nonce = $3::uuid)
                            OR
                            (marker.phase = 'after'
                             AND marker.marker_xid = $4::xid8
                             AND marker.marker_nonce = $5::uuid)
                        )
                        AND $2::xid8 < $4::xid8 AS valid
                 FROM synchro.sync_stream_reset_snapshot_markers marker
                 WHERE marker.reset_id = $1::uuid",
                None,
                &[
                    reset.reset_id.as_str().into(),
                    before_xid.into(),
                    before_nonce.into(),
                    after_xid.into(),
                    after_nonce.into(),
                ],
            )
            .map_err(|_| "verifying reset snapshot binding failed".to_string())?
            .first(),
        "valid",
    )?;
    if !valid {
        return Err("reset snapshot binding changed".to_string());
    }
    Ok(())
}

fn verify_source_locks(
    client: &SpiClient<'_>,
    registry_generation: i64,
    require_worker_gate: bool,
) -> Result<(), String> {
    let locked = required_bool(
        &client
            .select(
                "WITH required AS (
                      SELECT DISTINCT pg_catalog.hashtextextended(
                          'synchro:relation:' || relation_id::text, 0
                      ) AS lock_key
                      FROM synchro.sync_registry
                      WHERE registry_generation = $1
                      UNION SELECT $2::bigint
                      UNION SELECT $3::bigint
                       UNION SELECT $4::bigint WHERE $5
                  ), required_count AS (
                     SELECT count(*)::bigint AS value FROM required
                 ), holders AS (
                     SELECT lock.pid, count(*)::bigint AS value
                     FROM required
                     JOIN pg_catalog.pg_locks lock
                       ON lock.locktype = 'advisory'
                      AND lock.database = (
                          SELECT oid FROM pg_catalog.pg_database
                          WHERE datname = pg_catalog.current_database()
                      )
                      AND lock.classid = (
                          ((required.lock_key >> 32) & 4294967295)::bigint
                      )::oid
                      AND lock.objid = (
                          (required.lock_key & 4294967295)::bigint
                      )::oid
                      AND lock.objsubid = 1
                      AND lock.mode = 'ExclusiveLock'
                      AND lock.granted
                     GROUP BY lock.pid
                 )
                 SELECT required_count.value = 0 OR EXISTS (
                     SELECT 1 FROM holders
                     WHERE holders.value = required_count.value
                 ) AS locked
                 FROM required_count",
                None,
                &[
                    registry_generation.into(),
                    0x7379_6e63i64.into(),
                    crate::SOURCE_WRITE_GATE_LOCK_KEY.into(),
                    crate::WAL_WORKER_GATE_LOCK_KEY.into(),
                    require_worker_gate.into(),
                ],
            )
            .map_err(|_| "checking source locks failed".to_string())?
            .first(),
        "locked",
    )?;
    if !locked {
        return Err("registered sources are not locked".to_string());
    }
    Ok(())
}

fn stage_existing_versions(client: &mut SpiClient<'_>, reset: &ResetRecord) -> Result<(), String> {
    client
        .update(
            "INSERT INTO synchro.sync_stream_reset_row_versions (
                 reset_id, relation_id, record_id, row_version, fence_id,
                 source_reset_id, deleted, baseline_generated
             )
             SELECT $1::uuid, version.relation_id, version.record_id,
                    version.row_version, version.fence_id, version.reset_id,
                    version.deleted, false
             FROM synchro.sync_row_versions version
             JOIN synchro.sync_registry registry
               ON registry.registry_generation = $2
              AND registry.relation_id = version.relation_id",
            None,
            &[
                reset.reset_id.as_str().into(),
                reset.staging_registry_generation()?.into(),
            ],
        )
        .map_err(|_| "staging row versions failed".to_string())?;
    Ok(())
}

fn stage_registration(
    client: &mut SpiClient<'_>,
    reset: &ResetRecord,
    registration: &TableRegistration,
) -> Result<(), String> {
    if registration.is_capture_dependency() {
        return stage_capture_dependency_registration(client, reset, registration);
    }
    for source in load_source_rows(client, registration)? {
        let staged_version = load_or_create_staged_version(client, reset, registration, &source)?;
        if staged_version.deleted != source.deleted {
            return Err("source row and durable version differ".to_string());
        }
        let digest = synced_row_digest(
            client,
            registration,
            &source.row_data,
            &source.record_id,
            &staged_version.row_version,
        )?;
        client
            .update(
                "INSERT INTO synchro.sync_stream_reset_captured_rows (
                     reset_id, relation_id, record_id, row_data, row_version,
                     checksum, deleted, registry_generation
                 ) VALUES ($1::uuid, $2::uuid, $3, $4, $5::uuid, $6, $7, $8)",
                None,
                &[
                    reset.reset_id.as_str().into(),
                    registration.relation_id.as_str().into(),
                    source.record_id.as_str().into(),
                    pgrx::JsonB(source.row_data).into(),
                    staged_version.row_version.as_str().into(),
                    digest.as_bytes().to_vec().into(),
                    source.deleted.into(),
                    reset.staging_registry_generation()?.into(),
                ],
            )
            .map_err(|_| "staging captured row failed".to_string())?;
    }
    Ok(())
}

fn stage_capture_dependency_registration(
    client: &mut SpiClient<'_>,
    reset: &ResetRecord,
    registration: &TableRegistration,
) -> Result<(), String> {
    for source in load_capture_dependency_source_rows(client, registration)? {
        client
            .update(
                "INSERT INTO synchro.sync_stream_reset_capture_dependency_rows (
                     reset_id, relation_id, capture_key, row_data, deleted,
                     registry_generation
                 ) VALUES ($1::uuid, $2::uuid, $3, $4, false, $5)",
                None,
                &[
                    reset.reset_id.as_str().into(),
                    registration.relation_id.as_str().into(),
                    pgrx::JsonB(source.capture_key).into(),
                    pgrx::JsonB(source.row_data).into(),
                    reset.staging_registry_generation()?.into(),
                ],
            )
            .map_err(|_| "staging capture dependency row failed".to_string())?;
    }
    Ok(())
}

fn select_staged_projection(client: &mut SpiClient<'_>, reset: &ResetRecord) -> Result<(), String> {
    client
        .update(
            "SELECT set_config('synchro.stream_reset_staging_id', $1, true)",
            None,
            &[reset.reset_id.as_str().into()],
        )
        .map_err(|_| "selecting staged reset projection failed".to_string())?;
    let generation = reset.staging_registry_generation()?;
    client
        .update(
            "SELECT set_config('synchro.stream_reset_staging_registry_generation', $1, true)",
            None,
            &[generation.to_string().as_str().into()],
        )
        .map_err(|_| "selecting staged reset registry failed".to_string())?;
    Ok(())
}

fn stage_registration_membership(
    client: &mut SpiClient<'_>,
    reset: &ResetRecord,
    registration: &TableRegistration,
) -> Result<(), String> {
    if !registration.is_synced() {
        return Ok(());
    }
    let rows = client
        .select(
            "SELECT record_id, checksum, row_version::text AS row_version
             FROM synchro.sync_stream_reset_captured_rows
             WHERE reset_id = $1::uuid AND relation_id = $2::uuid AND NOT deleted
             ORDER BY record_id",
            None,
            &[
                reset.reset_id.as_str().into(),
                registration.relation_id.as_str().into(),
            ],
        )
        .map_err(|_| "loading staged membership rows failed".to_string())?;
    for row in rows {
        let record_id = required_text(&row, "record_id")?;
        let checksum = required_digest(&row, "checksum")?;
        let row_version = required_text(&row, "row_version")?;
        let scopes = resolve_membership(client, registration, &record_id)
            .map_err(|_| "resolving staged membership failed".to_string())?;
        for scope_id in scopes {
            client
                .update(
                    "INSERT INTO synchro.sync_stream_reset_membership_edges (
                         reset_id, relation_id, table_name, record_id, scope_id,
                         checksum, row_version
                     ) VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7::uuid)",
                    None,
                    &[
                        reset.reset_id.as_str().into(),
                        registration.relation_id.as_str().into(),
                        registration.table_name.as_str().into(),
                        record_id.as_str().into(),
                        scope_id.as_str().into(),
                        checksum.as_bytes().to_vec().into(),
                        row_version.as_str().into(),
                    ],
                )
                .map_err(|_| "staging membership edge failed".to_string())?;
        }
    }
    Ok(())
}

fn load_source_rows(
    client: &SpiClient<'_>,
    registration: &TableRegistration,
) -> Result<Vec<SourceRow>, String> {
    let relation = qualified_relation_name(
        &registration.physical_schema,
        &registration.physical_relation,
    );
    let primary_key = crate::pull::pg_quote_ident(&registration.pk_column);
    let deleted = if registration.has_deleted_at {
        format!(
            "source.{} IS NOT NULL",
            crate::pull::pg_quote_ident(&registration.deleted_at_col)
        )
    } else {
        "false".to_string()
    };
    let query = format!(
        "SELECT source.{primary_key}::text AS record_id,
                ({})::text AS row_data,
                {deleted} AS deleted
         FROM {relation} source
         ORDER BY source.{primary_key}",
        synced_row_projection_sql(registration, "source"),
    );
    let rows = client
        .select(&query, None, &[])
        .map_err(|_| "loading registered source rows failed".to_string())?;
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        let record_id = required_text(&row, "record_id")?;
        let encoded = required_text(&row, "row_data")?;
        let mut row_data: serde_json::Value = serde_json::from_str(&encoded)
            .map_err(|_| "registered source row is invalid".to_string())?;
        canonicalize_synced_row_data(registration, &mut row_data)?;
        if !row_data.is_object() {
            return Err("registered source projection is invalid".to_string());
        }
        result.push(SourceRow {
            record_id,
            row_data,
            deleted: required_bool(&row, "deleted")?,
        });
    }
    Ok(result)
}

fn load_capture_dependency_source_rows(
    client: &SpiClient<'_>,
    registration: &TableRegistration,
) -> Result<Vec<CaptureDependencySourceRow>, String> {
    let relation = qualified_relation_name(
        &registration.physical_schema,
        &registration.physical_relation,
    );
    let selections = registration
        .capture_fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            format!(
                "COALESCE(to_jsonb(source.{}), 'null'::jsonb) AS capture_value_{index}",
                crate::pull::pg_quote_ident(&field.physical_column)
            )
        })
        .collect::<Vec<_>>();
    if selections.is_empty() {
        return Err("capture dependency projection has no fields".to_string());
    }
    let query = format!(
        "SELECT {} FROM {relation} source ORDER BY source.{}",
        selections.join(", "),
        crate::pull::pg_quote_ident(&registration.pk_column),
    );
    let rows = client
        .select(&query, None, &[])
        .map_err(|_| "loading capture dependency source rows failed".to_string())?;
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        let mut capture_key = serde_json::Map::new();
        let mut row_data = serde_json::Map::new();
        for (index, field) in registration.capture_fields.iter().enumerate() {
            let value = row
                .get_by_name::<pgrx::JsonB, String>(format!("capture_value_{index}"))
                .map_err(|_| "reading capture dependency source row failed".to_string())?
                .ok_or_else(|| "capture dependency source value is missing".to_string())?
                .0;
            if field.capture_key {
                if value.is_null() {
                    return Err("capture dependency source key is null".to_string());
                }
                capture_key.insert(field.physical_column.clone(), value.clone());
            }
            row_data.insert(field.physical_column.clone(), value);
        }
        if capture_key.len() != registration.capture_key_columns.len() {
            return Err("capture dependency source key is incomplete".to_string());
        }
        result.push(CaptureDependencySourceRow {
            capture_key: capture_key.into(),
            row_data: row_data.into(),
        });
    }
    Ok(result)
}

fn load_or_create_staged_version(
    client: &mut SpiClient<'_>,
    reset: &ResetRecord,
    registration: &TableRegistration,
    source: &SourceRow,
) -> Result<StagedVersion, String> {
    let rows = client
        .select(
            "SELECT row_version::text AS row_version, deleted
             FROM synchro.sync_stream_reset_row_versions
             WHERE reset_id = $1::uuid AND relation_id = $2::uuid AND record_id = $3",
            None,
            &[
                reset.reset_id.as_str().into(),
                registration.relation_id.as_str().into(),
                source.record_id.as_str().into(),
            ],
        )
        .map_err(|_| "loading staged row version failed".to_string())?;
    if let Some(row) = rows.into_iter().next() {
        return Ok(StagedVersion {
            row_version: required_text(&row, "row_version")?,
            deleted: required_bool(&row, "deleted")?,
        });
    }
    let row = client
        .select(
            "INSERT INTO synchro.sync_stream_reset_row_versions (
                 reset_id, relation_id, record_id, row_version, fence_id,
                 source_reset_id, deleted, baseline_generated
             ) VALUES (
                 $1::uuid, $2::uuid, $3, gen_random_uuid(), NULL,
                 $1::uuid, $4, true
             )
             RETURNING row_version::text AS row_version, deleted",
            None,
            &[
                reset.reset_id.as_str().into(),
                registration.relation_id.as_str().into(),
                source.record_id.as_str().into(),
                source.deleted.into(),
            ],
        )
        .map_err(|_| "creating baseline row version failed".to_string())?
        .first();
    Ok(StagedVersion {
        row_version: required_text(&row, "row_version")?,
        deleted: required_bool(&row, "deleted")?,
    })
}

fn stage_fence_coverage(
    client: &mut SpiClient<'_>,
    reset: &ResetRecord,
    consistent_point: &str,
) -> Result<(), String> {
    client
        .update(
            "INSERT INTO synchro.sync_stream_reset_fence_coverage (
                  reset_id, fence_id, relation_id, registration_kind, table_id,
                  operation, old_record_id, new_record_id,
                  old_capture_key, new_capture_key, row_version,
                  candidate_slot_name, consistent_point, target_stream_generation
              )
              SELECT $1::uuid, fence.fence_id, fence.relation_id,
                     fence.registration_kind, fence.table_id, fence.operation,
                     fence.old_record_id, fence.new_record_id,
                     fence.old_capture_key, fence.new_capture_key, fence.row_version,
                     $2, $3::pg_lsn, $4
             FROM synchro.sync_write_fences fence
             JOIN synchro.sync_registry registry
               ON registry.registry_generation = $5
              AND registry.relation_id = fence.relation_id
             WHERE fence.coverage = 'pending'",
            None,
            &[
                reset.reset_id.as_str().into(),
                reset.candidate_slot_name.as_str().into(),
                consistent_point.into(),
                reset.target_stream_generation.as_str().into(),
                reset.staging_registry_generation()?.into(),
            ],
        )
        .map_err(|_| "staging fence coverage failed".to_string())?;
    Ok(())
}

fn prune_stale_versions(client: &mut SpiClient<'_>, reset: &ResetRecord) -> Result<(), String> {
    client
        .update(
            "DELETE FROM synchro.sync_stream_reset_row_versions version
             WHERE version.reset_id = $1::uuid
               AND NOT version.deleted
               AND NOT EXISTS (
                   SELECT 1
                   FROM synchro.sync_stream_reset_captured_rows captured
                   WHERE captured.reset_id = version.reset_id
                     AND captured.relation_id = version.relation_id
                     AND captured.record_id = version.record_id
               )",
            None,
            &[reset.reset_id.as_str().into()],
        )
        .map_err(|_| "pruning stale reset row versions failed".to_string())?;
    Ok(())
}

fn verify_staging(
    client: &SpiClient<'_>,
    reset: &ResetRecord,
    registry: &[TableRegistration],
    consistent_point: &str,
) -> Result<(), String> {
    verify_source_projection(client, reset, registry)?;
    let valid = required_bool(
        &client
            .select(
                "SELECT
                     NOT EXISTS (
                         (SELECT fence_id
                          FROM synchro.sync_write_fences fence
                          JOIN synchro.sync_registry registry
                            ON registry.registry_generation = $2
                           AND registry.relation_id = fence.relation_id
                          WHERE fence.coverage = 'pending'
                          EXCEPT
                          SELECT fence_id
                          FROM synchro.sync_stream_reset_fence_coverage
                          WHERE reset_id = $1::uuid)
                         UNION ALL
                         (SELECT fence_id
                          FROM synchro.sync_stream_reset_fence_coverage
                          WHERE reset_id = $1::uuid
                          EXCEPT
                          SELECT fence_id
                          FROM synchro.sync_write_fences fence
                          JOIN synchro.sync_registry registry
                            ON registry.registry_generation = $2
                           AND registry.relation_id = fence.relation_id
                          WHERE fence.coverage = 'pending')
                     )
                     AND NOT EXISTS (
                         SELECT 1
                         FROM synchro.sync_stream_reset_fence_coverage coverage
                         LEFT JOIN synchro.sync_write_fences fence
                           ON fence.fence_id = coverage.fence_id
                         WHERE coverage.reset_id = $1::uuid
                           AND (
                                fence.fence_id IS NULL
                                OR fence.coverage <> 'pending'
                                OR coverage.relation_id <> fence.relation_id
                                OR coverage.registration_kind <> fence.registration_kind
                                OR coverage.table_id IS DISTINCT FROM fence.table_id
                                OR coverage.operation <> fence.operation
                                OR coverage.old_record_id IS DISTINCT FROM fence.old_record_id
                                OR coverage.new_record_id IS DISTINCT FROM fence.new_record_id
                                OR coverage.old_capture_key IS DISTINCT FROM fence.old_capture_key
                                OR coverage.new_capture_key IS DISTINCT FROM fence.new_capture_key
                                OR coverage.row_version <> fence.row_version
                               OR coverage.candidate_slot_name::text <> $3
                               OR coverage.consistent_point <> $4::pg_lsn
                                OR coverage.target_stream_generation <> $5
                                OR CASE
                                    WHEN fence.registration_kind = 'synced'
                                         AND fence.operation IN ('insert', 'update')
                                    THEN NOT EXISTS (
                                        SELECT 1
                                       FROM synchro.sync_stream_reset_row_versions version
                                       JOIN synchro.sync_stream_reset_captured_rows captured
                                         ON captured.reset_id = version.reset_id
                                        AND captured.relation_id = version.relation_id
                                        AND captured.record_id = version.record_id
                                        AND captured.row_version = version.row_version
                                       WHERE version.reset_id = $1::uuid
                                         AND version.relation_id = fence.relation_id
                                         AND version.record_id = COALESCE(fence.new_record_id, fence.old_record_id)
                                         AND version.row_version = fence.row_version
                                   )
                                    WHEN fence.registration_kind = 'synced'
                                         AND fence.operation = 'delete'
                                    THEN
                                        NOT EXISTS (
                                           SELECT 1
                                           FROM synchro.sync_stream_reset_row_versions version
                                           WHERE version.reset_id = $1::uuid
                                             AND version.relation_id = fence.relation_id
                                             AND version.record_id = fence.old_record_id
                                             AND version.row_version = fence.row_version
                                             AND version.deleted
                                       )
                                       OR EXISTS (
                                           SELECT 1
                                           FROM synchro.sync_stream_reset_captured_rows captured
                                           WHERE captured.reset_id = $1::uuid
                                             AND captured.relation_id = fence.relation_id
                                              AND captured.record_id = fence.old_record_id
                                        )
                                    WHEN fence.registration_kind = 'capture_dependency'
                                         AND fence.operation IN ('insert', 'update')
                                    THEN NOT EXISTS (
                                        SELECT 1
                                        FROM synchro.sync_stream_reset_capture_dependency_rows captured
                                        WHERE captured.reset_id = $1::uuid
                                          AND captured.relation_id = fence.relation_id
                                          AND captured.capture_key = COALESCE(
                                              fence.new_capture_key,
                                              fence.old_capture_key
                                          )
                                          AND NOT captured.deleted
                                    )
                                    WHEN fence.registration_kind = 'capture_dependency'
                                         AND fence.operation = 'delete'
                                    THEN EXISTS (
                                        SELECT 1
                                        FROM synchro.sync_stream_reset_capture_dependency_rows captured
                                        WHERE captured.reset_id = $1::uuid
                                          AND captured.relation_id = fence.relation_id
                                          AND captured.capture_key = fence.old_capture_key
                                    )
                                    ELSE true
                                END
                           )
                     ) AS valid",
                None,
                &[
                    reset.reset_id.as_str().into(),
                    reset.staging_registry_generation()?.into(),
                    reset.candidate_slot_name.as_str().into(),
                    consistent_point.into(),
                    reset.target_stream_generation.as_str().into(),
                ],
            )
            .map_err(|_| "verifying fence coverage failed".to_string())?
            .first(),
        "valid",
    )?;
    if !valid {
        return Err("staged fence coverage is incomplete".to_string());
    }
    let projection_valid = required_bool(
        &client
            .select(
                "SELECT
                     NOT EXISTS (
                         SELECT 1
                         FROM synchro.sync_stream_reset_row_versions version
                         LEFT JOIN synchro.sync_stream_reset_captured_rows captured
                           ON captured.reset_id = version.reset_id
                          AND captured.relation_id = version.relation_id
                          AND captured.record_id = version.record_id
                         WHERE version.reset_id = $1::uuid
                           AND captured.record_id IS NULL
                           AND NOT version.deleted
                     )
                     AND NOT EXISTS (
                         SELECT 1
                         FROM synchro.sync_stream_reset_membership_edges edge
                         LEFT JOIN synchro.sync_stream_reset_captured_rows captured
                           ON captured.reset_id = edge.reset_id
                          AND captured.relation_id = edge.relation_id
                          AND captured.record_id = edge.record_id
                         WHERE edge.reset_id = $1::uuid
                           AND (captured.record_id IS NULL
                                OR captured.deleted
                                OR captured.row_version <> edge.row_version
                                OR captured.checksum <> edge.checksum)
                     ) AS valid",
                None,
                &[reset.reset_id.as_str().into()],
            )
            .map_err(|_| "verifying staged projection integrity failed".to_string())?
            .first(),
        "valid",
    )?;
    if !projection_valid {
        return Err("staged projection integrity is invalid".to_string());
    }
    Ok(())
}

fn verify_source_projection(
    client: &SpiClient<'_>,
    reset: &ResetRecord,
    registry: &[TableRegistration],
) -> Result<(), String> {
    let mut source_keys = BTreeSet::new();
    let mut capture_source_keys = BTreeSet::new();
    for registration in registry {
        if registration.is_capture_dependency() {
            for source in load_capture_dependency_source_rows(client, registration)? {
                let row = client
                    .select(
                        "SELECT capture_key, row_data, deleted, registry_generation
                         FROM synchro.sync_stream_reset_capture_dependency_rows
                         WHERE reset_id = $1::uuid
                           AND relation_id = $2::uuid
                           AND capture_key = $3",
                        None,
                        &[
                            reset.reset_id.as_str().into(),
                            registration.relation_id.as_str().into(),
                            pgrx::JsonB(source.capture_key.clone()).into(),
                        ],
                    )
                    .map_err(|_| "loading staged capture dependency failed".to_string())?
                    .first();
                let staged_key = row
                    .get_by_name::<pgrx::JsonB, &str>("capture_key")
                    .map_err(|_| "reading staged capture dependency failed".to_string())?
                    .map(|value| value.0)
                    .ok_or_else(|| "staged capture dependency is missing".to_string())?;
                let row_data = row
                    .get_by_name::<pgrx::JsonB, &str>("row_data")
                    .map_err(|_| "reading staged capture dependency failed".to_string())?
                    .map(|value| value.0)
                    .ok_or_else(|| "staged capture dependency is missing".to_string())?;
                let deleted = required_bool(&row, "deleted")?;
                let generation = required_positive_i64(&row, "registry_generation")?;
                if staged_key != source.capture_key
                    || row_data != source.row_data
                    || deleted
                    || generation != reset.staging_registry_generation()?
                {
                    return Err(
                        "staged capture dependency projection differs from source".to_string()
                    );
                }
                capture_source_keys.insert((
                    registration.relation_id.clone(),
                    serde_json::to_string(&source.capture_key)
                        .map_err(|_| "encoding capture dependency key failed".to_string())?,
                ));
            }
            continue;
        }
        for source in load_source_rows(client, registration)? {
            source_keys.insert((registration.relation_id.clone(), source.record_id.clone()));
            let row = client
                .select(
                    "SELECT captured.row_data, captured.row_version::text AS row_version,
                            captured.checksum, captured.deleted,
                            captured.registry_generation, version.deleted AS version_deleted,
                            version.baseline_generated
                     FROM synchro.sync_stream_reset_captured_rows captured
                     JOIN synchro.sync_stream_reset_row_versions version
                       ON version.reset_id = captured.reset_id
                      AND version.relation_id = captured.relation_id
                      AND version.record_id = captured.record_id
                      AND version.row_version = captured.row_version
                     WHERE captured.reset_id = $1::uuid
                       AND captured.relation_id = $2::uuid
                       AND captured.record_id = $3",
                    None,
                    &[
                        reset.reset_id.as_str().into(),
                        registration.relation_id.as_str().into(),
                        source.record_id.as_str().into(),
                    ],
                )
                .map_err(|_| "loading staged projection failed".to_string())?
                .first();
            let row_data = row
                .get_by_name::<pgrx::JsonB, &str>("row_data")
                .map_err(|_| "reading staged projection failed".to_string())?
                .map(|value| value.0)
                .ok_or_else(|| "staged projection is missing".to_string())?;
            let row_version = required_text(&row, "row_version")?;
            let deleted = required_bool(&row, "deleted")?;
            let version_deleted = required_bool(&row, "version_deleted")?;
            let baseline_generated = required_bool(&row, "baseline_generated")?;
            let generation = required_positive_i64(&row, "registry_generation")?;
            let checksum = required_digest(&row, "checksum")?;
            let computed = synced_row_digest(
                client,
                registration,
                &source.row_data,
                &source.record_id,
                &row_version,
            )?;
            if row_data != source.row_data
                || deleted != source.deleted
                || version_deleted != source.deleted
                || generation != reset.staging_registry_generation()?
                || checksum != computed
            {
                return Err("staged source projection differs from source".to_string());
            }
            if !baseline_generated {
                let hydrated = hydrate_records(
                    client,
                    &registration.table_name,
                    &[source.record_id.as_str()],
                    registry,
                )?
                .into_iter()
                .next()
                .ok_or_else(|| "canonical source hydration is missing".to_string())?;
                if hydrated.get("data") != Some(&source.row_data)
                    || hydrated
                        .get("server_version")
                        .and_then(serde_json::Value::as_str)
                        != Some(row_version.as_str())
                {
                    return Err("canonical source hydration differs from staging".to_string());
                }
            }
            let expected_scopes = if source.deleted {
                Vec::new()
            } else {
                resolve_membership(client, registration, &source.record_id)
                    .map_err(|_| "resolving verified membership failed".to_string())?
            };
            let rows = client
                .select(
                    "SELECT scope_id
                     FROM synchro.sync_stream_reset_membership_edges
                     WHERE reset_id = $1::uuid AND relation_id = $2::uuid AND record_id = $3
                     ORDER BY scope_id",
                    None,
                    &[
                        reset.reset_id.as_str().into(),
                        registration.relation_id.as_str().into(),
                        source.record_id.as_str().into(),
                    ],
                )
                .map_err(|_| "loading staged membership failed".to_string())?;
            let actual_scopes = rows
                .into_iter()
                .map(|row| required_text(&row, "scope_id"))
                .collect::<Result<Vec<_>, _>>()?;
            if actual_scopes != expected_scopes {
                return Err("staged membership differs from source".to_string());
            }
        }
    }
    let staged_rows = client
        .select(
            "SELECT relation_id::text AS relation_id, record_id
             FROM synchro.sync_stream_reset_captured_rows
             WHERE reset_id = $1::uuid",
            None,
            &[reset.reset_id.as_str().into()],
        )
        .map_err(|_| "loading staged source identities failed".to_string())?;
    let staged_keys = staged_rows
        .into_iter()
        .map(|row| {
            Ok((
                required_text(&row, "relation_id")?,
                required_text(&row, "record_id")?,
            ))
        })
        .collect::<Result<BTreeSet<_>, String>>()?;
    if staged_keys != source_keys {
        return Err("staged source row set is incomplete".to_string());
    }
    let staged_capture_rows = client
        .select(
            "SELECT relation_id::text AS relation_id, capture_key
             FROM synchro.sync_stream_reset_capture_dependency_rows
             WHERE reset_id = $1::uuid",
            None,
            &[reset.reset_id.as_str().into()],
        )
        .map_err(|_| "loading staged capture dependency identities failed".to_string())?;
    let staged_capture_keys = staged_capture_rows
        .into_iter()
        .map(|row| {
            let capture_key = row
                .get_by_name::<pgrx::JsonB, &str>("capture_key")
                .map_err(|_| "reading staged capture dependency key failed".to_string())?
                .map(|value| value.0)
                .ok_or_else(|| "staged capture dependency key is missing".to_string())?;
            Ok((
                required_text(&row, "relation_id")?,
                serde_json::to_string(&capture_key)
                    .map_err(|_| "encoding staged capture dependency key failed".to_string())?,
            ))
        })
        .collect::<Result<BTreeSet<_>, String>>()?;
    if staged_capture_keys != capture_source_keys {
        return Err("staged capture dependency row set is incomplete".to_string());
    }
    Ok(())
}

fn stage_scope_digests(
    client: &mut SpiClient<'_>,
    reset: &ResetRecord,
    registry: &[TableRegistration],
) -> Result<(), String> {
    for (scope_id, digest, row_count, schema_hash) in
        compute_staged_scope_digests(client, reset, registry)?
    {
        client
            .update(
                "INSERT INTO synchro.sync_stream_reset_scope_digests (
                     reset_id, scope_id, schema_hash, digest, row_count
                 ) VALUES ($1::uuid, $2, $3, $4, $5)",
                None,
                &[
                    reset.reset_id.as_str().into(),
                    scope_id.as_str().into(),
                    schema_hash.as_str().into(),
                    digest.as_bytes().to_vec().into(),
                    row_count.into(),
                ],
            )
            .map_err(|_| "staging scope digest failed".to_string())?;
    }
    Ok(())
}

fn verify_scope_digests(
    client: &SpiClient<'_>,
    reset: &ResetRecord,
    registry: &[TableRegistration],
) -> Result<(), String> {
    let expected = compute_staged_scope_digests(client, reset, registry)?;
    let rows = client
        .select(
            "SELECT scope_id, schema_hash, digest, row_count
             FROM synchro.sync_stream_reset_scope_digests
             WHERE reset_id = $1::uuid
             ORDER BY scope_id",
            None,
            &[reset.reset_id.as_str().into()],
        )
        .map_err(|_| "loading staged scope digests failed".to_string())?;
    if rows.len() != expected.len() {
        return Err("staged scope digest set is incomplete".to_string());
    }
    for (row, (scope_id, digest, row_count, schema_hash)) in rows.into_iter().zip(expected) {
        if required_text(&row, "scope_id")? != scope_id
            || required_text(&row, "schema_hash")? != schema_hash
            || required_digest(&row, "digest")? != digest
            || required_nonnegative_i64(&row, "row_count")? != row_count
        {
            return Err("staged scope digest differs from staged edges".to_string());
        }
    }
    Ok(())
}

fn compute_staged_scope_digests(
    client: &SpiClient<'_>,
    reset: &ResetRecord,
    registry: &[TableRegistration],
) -> Result<Vec<(String, Sha256Digest, i64, String)>, String> {
    let schema_hash = schema_hash_for_generation(client, reset.staging_registry_generation()?)?;
    let schema_hash_text = schema_hash.to_lower_hex();
    let scope_rows = client
        .select(
            "SELECT scope_id FROM synchro.sync_scope_state
             UNION
             SELECT scope_id FROM synchro.sync_stream_reset_membership_edges
             WHERE reset_id = $1::uuid
             ORDER BY scope_id",
            None,
            &[reset.reset_id.as_str().into()],
        )
        .map_err(|_| "loading staged scope identities failed".to_string())?;
    let mut entries = BTreeMap::<String, Vec<ScopeDigestEntry>>::new();
    for row in scope_rows {
        entries.insert(required_text(&row, "scope_id")?, Vec::new());
    }
    let edge_rows = client
        .select(
            "SELECT relation_id::text AS relation_id, record_id, scope_id, checksum
             FROM synchro.sync_stream_reset_membership_edges
             WHERE reset_id = $1::uuid
             ORDER BY scope_id, relation_id, record_id",
            None,
            &[reset.reset_id.as_str().into()],
        )
        .map_err(|_| "loading staged digest edges failed".to_string())?;
    for row in edge_rows {
        let relation_id = required_text(&row, "relation_id")?;
        let record_id = required_text(&row, "record_id")?;
        let scope_id = required_text(&row, "scope_id")?;
        let registration = registry
            .iter()
            .find(|candidate| candidate.relation_id == relation_id)
            .ok_or_else(|| "staged edge relation is not registered".to_string())?;
        let primary_key = row_primary_key_json(registration, &record_id)?;
        let identity = row_identity(
            &canonical_table(registration)?,
            &serde_json::to_string(&primary_key)
                .map_err(|_| "encoding staged row identity failed".to_string())?,
        )
        .map_err(|_| "staged row identity is invalid".to_string())?;
        entries
            .get_mut(&scope_id)
            .ok_or_else(|| "staged edge scope is missing".to_string())?
            .push(ScopeDigestEntry::new(
                identity,
                required_digest(&row, "checksum")?,
            ));
    }
    entries
        .into_iter()
        .map(|(scope_id, scope_entries)| {
            let row_count = i64::try_from(scope_entries.len())
                .map_err(|_| "staged scope row count overflowed".to_string())?;
            let digest = scope_digest(schema_hash, &scope_id, &scope_entries)
                .map_err(|_| "computing staged scope digest failed".to_string())?;
            Ok((scope_id, digest, row_count, schema_hash_text.clone()))
        })
        .collect()
}

fn staging_counts(client: &SpiClient<'_>, reset_id: &str) -> Result<[i64; 5], String> {
    let row = client
        .select(
            "SELECT
                 ((SELECT count(*) FROM synchro.sync_stream_reset_captured_rows
                   WHERE reset_id = $1::uuid)
                  +
                  (SELECT count(*) FROM synchro.sync_stream_reset_capture_dependency_rows
                   WHERE reset_id = $1::uuid)) AS rows,
                 (SELECT count(*) FROM synchro.sync_stream_reset_row_versions WHERE reset_id = $1::uuid) AS versions,
                 (SELECT count(*) FROM synchro.sync_stream_reset_membership_edges WHERE reset_id = $1::uuid) AS edges,
                 (SELECT count(*) FROM synchro.sync_stream_reset_fence_coverage WHERE reset_id = $1::uuid) AS fences,
                 (SELECT count(*) FROM synchro.sync_stream_reset_scope_digests WHERE reset_id = $1::uuid) AS scopes",
            None,
            &[reset_id.into()],
        )
        .map_err(|_| "loading reset staging counts failed".to_string())?
        .first();
    Ok([
        required_nonnegative_i64(&row, "rows")?,
        required_nonnegative_i64(&row, "versions")?,
        required_nonnegative_i64(&row, "edges")?,
        required_nonnegative_i64(&row, "fences")?,
        required_nonnegative_i64(&row, "scopes")?,
    ])
}

fn verify_persisted_counts(client: &SpiClient<'_>, reset: &ResetRecord) -> Result<(), String> {
    let counts = staging_counts(client, &reset.reset_id)?;
    let valid = required_bool(
        &client
            .select(
                "SELECT staged_row_count = $2
                        AND staged_version_count = $3
                        AND staged_edge_count = $4
                        AND staged_fence_count = $5
                        AND staged_scope_count = $6 AS valid
                 FROM synchro.sync_stream_resets WHERE reset_id = $1::uuid",
                None,
                &[
                    reset.reset_id.as_str().into(),
                    counts[0].into(),
                    counts[1].into(),
                    counts[2].into(),
                    counts[3].into(),
                    counts[4].into(),
                ],
            )
            .map_err(|_| "checking reset staging counts failed".to_string())?
            .first(),
        "valid",
    )?;
    if !valid {
        return Err("reset staging counts changed".to_string());
    }
    Ok(())
}

fn verify_projection_stage_integrity(
    client: &SpiClient<'_>,
    reset: &ResetRecord,
    registry: &[TableRegistration],
) -> Result<(), String> {
    if registry.is_empty() {
        return Err("projection bootstrap registry is empty".to_string());
    }
    let valid = required_bool(
        &client
            .select(
                "SELECT
                     NOT EXISTS (
                         SELECT 1
                         FROM synchro.sync_stream_reset_row_versions version
                         LEFT JOIN synchro.sync_stream_reset_captured_rows captured
                           ON captured.reset_id = version.reset_id
                          AND captured.relation_id = version.relation_id
                          AND captured.record_id = version.record_id
                         WHERE version.reset_id = $1::uuid
                           AND captured.record_id IS NULL
                           AND NOT version.deleted
                     )
                     AND NOT EXISTS (
                         SELECT 1
                         FROM synchro.sync_stream_reset_membership_edges edge
                         LEFT JOIN synchro.sync_stream_reset_captured_rows captured
                           ON captured.reset_id = edge.reset_id
                          AND captured.relation_id = edge.relation_id
                          AND captured.record_id = edge.record_id
                         WHERE edge.reset_id = $1::uuid
                           AND (captured.record_id IS NULL
                                OR captured.deleted
                                OR captured.row_version <> edge.row_version
                                OR captured.checksum <> edge.checksum)
                     ) AS valid",
                None,
                &[reset.reset_id.as_str().into()],
            )
            .map_err(|_| "verifying projection bootstrap stage failed".to_string())?
            .first(),
        "valid",
    )?;
    if !valid {
        return Err("projection bootstrap stage is incomplete".to_string());
    }
    Ok(())
}

fn projection_bootstrap_affected_scopes(
    client: &SpiClient<'_>,
    reset: &ResetRecord,
    _registry: &[TableRegistration],
) -> Result<Vec<String>, String> {
    let target_generation = reset.staging_registry_generation()?;
    let rows = client
        .select(
            "WITH changed_relations AS (
                 SELECT target.relation_id
                 FROM synchro.sync_registry target
                 LEFT JOIN synchro.sync_registry source
                   ON source.registry_generation = $2
                  AND source.relation_id = target.relation_id
                 WHERE target.registry_generation = $3
                   AND (
                       source.relation_id IS NULL
                       OR source.composition IS DISTINCT FROM target.composition
                       OR source.membership_function_oid IS DISTINCT FROM target.membership_function_oid
                       OR source.sync_columns IS DISTINCT FROM target.sync_columns
                       OR source.capture_key_columns IS DISTINCT FROM target.capture_key_columns
                   )
                 UNION
                 SELECT target.relation_id
                 FROM synchro.sync_registry target
                 WHERE target.registry_generation = $3
                   AND EXISTS (
                       SELECT 1
                       FROM synchro.sync_registry_fields target_field
                       LEFT JOIN synchro.sync_registry_fields source_field
                         ON source_field.registry_generation = $2
                        AND source_field.relation_id = target_field.relation_id
                        AND source_field.field_id = target_field.field_id
                       WHERE target_field.registry_generation = $3
                         AND target_field.relation_id = target.relation_id
                         AND (
                             source_field.field_id IS NULL
                              OR source_field.physical_column IS DISTINCT FROM target_field.physical_column
                              OR source_field.portable_type IS DISTINCT FROM target_field.portable_type
                              OR source_field.native_json IS DISTINCT FROM target_field.native_json
                              OR source_field.nullable IS DISTINCT FROM target_field.nullable
                             OR source_field.writable IS DISTINCT FROM target_field.writable
                         )
                   )
             ), changed_scopes AS (
                 SELECT scope_id
                 FROM synchro.sync_stream_reset_membership_edges
                 WHERE reset_id = $1::uuid
                   AND relation_id IN (SELECT relation_id FROM changed_relations)
                 UNION
                 SELECT edge.bucket_id AS scope_id
                 FROM synchro.sync_bucket_edges edge
                 WHERE NOT EXISTS (
                     SELECT 1
                     FROM synchro.sync_stream_reset_membership_edges staged
                     WHERE staged.reset_id = $1::uuid
                       AND staged.relation_id = edge.relation_id
                       AND staged.table_name = edge.table_name
                       AND staged.record_id = edge.record_id
                       AND staged.scope_id = edge.bucket_id
                 )
                 UNION
                 SELECT staged.scope_id
                 FROM synchro.sync_stream_reset_membership_edges staged
                 WHERE staged.reset_id = $1::uuid
                   AND NOT EXISTS (
                       SELECT 1
                       FROM synchro.sync_bucket_edges edge
                       WHERE edge.relation_id = staged.relation_id
                         AND edge.table_name = staged.table_name
                         AND edge.record_id = staged.record_id
                         AND edge.bucket_id = staged.scope_id
                   )
             )
             SELECT scope_id FROM changed_scopes WHERE scope_id <> '' ORDER BY scope_id",
            None,
            &[
                reset.reset_id.as_str().into(),
                reset.source_registry_generation.into(),
                target_generation.into(),
            ],
        )
        .map_err(|_| "loading projection bootstrap affected scopes failed".to_string())?;
    let mut scopes = Vec::with_capacity(rows.len());
    for row in rows {
        scopes.push(required_text(&row, "scope_id")?);
    }
    scopes.dedup();
    if scopes.is_empty() {
        return Err("projection bootstrap has no affected scopes".to_string());
    }
    Ok(scopes)
}

fn activate_projection_registry(
    client: &mut SpiClient<'_>,
    reset: &ResetRecord,
    target_generation: i64,
    barrier: &str,
    affected_scopes: &[String],
) -> Result<(), String> {
    let generations = pending_generation_chain(
        client,
        reset.source_registry_generation,
        target_generation,
        &reset.source_stream_generation,
    )?;
    let superseded = client
        .update(
            "UPDATE synchro.sync_registry_generations
             SET state = 'superseded', activated_at = now(),
                 activation_commit_lsn = $2::pg_lsn, activation_end_lsn = $2::pg_lsn
             WHERE generation = $1 AND state = 'active' AND validated",
            None,
            &[reset.source_registry_generation.into(), barrier.into()],
        )
        .map_err(|_| "superseding active projection registry failed".to_string())?
        .len();
    if superseded != 1 {
        return Err("projection bootstrap active registry changed".to_string());
    }
    let commit_lsn = reset
        .candidate_materialized_commit_lsn
        .as_deref()
        .ok_or_else(|| "projection bootstrap commit boundary is missing".to_string())?;
    for generation in generations {
        let state = if generation == target_generation {
            "active"
        } else {
            "superseded"
        };
        let activated = client
            .update(
                "UPDATE synchro.sync_registry_generations
                 SET state = $2, activated_at = now(),
                     activation_commit_lsn = $3::pg_lsn,
                     activation_end_lsn = $4::pg_lsn
                 WHERE generation = $1 AND state = 'pending' AND validated",
                None,
                &[
                    generation.into(),
                    state.into(),
                    commit_lsn.into(),
                    barrier.into(),
                ],
            )
            .map_err(|_| "activating projection registry failed".to_string())?
            .len();
        if activated != 1 {
            return Err("projection bootstrap target registry changed".to_string());
        }
        client
            .update(
                "UPDATE synchro.sync_registry_membership_stages stage
                 SET state = 'activated', stream_generation = $2,
                     activation_commit_lsn = $3::pg_lsn,
                     activation_end_lsn = $4::pg_lsn,
                     staged_record_count = (
                         SELECT count(*)
                         FROM synchro.sync_stream_reset_captured_rows captured
                         WHERE captured.reset_id = $5::uuid
                           AND captured.relation_id = ANY(stage.target_relation_ids)
                     ),
                     staged_edge_count = (
                         SELECT count(*)
                         FROM synchro.sync_stream_reset_membership_edges edge
                         WHERE edge.reset_id = $5::uuid
                           AND edge.relation_id = ANY(stage.target_relation_ids)
                     ),
                     affected_scopes = $6::text[], verified = true,
                     activated_at = now()
                 WHERE stage.registry_generation = $1 AND stage.state = 'pending'",
                None,
                &[
                    generation.into(),
                    reset.source_stream_generation.as_str().into(),
                    commit_lsn.into(),
                    barrier.into(),
                    reset.reset_id.as_str().into(),
                    affected_scopes.to_vec().into(),
                ],
            )
            .map_err(|_| "recording projection membership activation failed".to_string())?;
    }
    let progress = client
        .update(
            "UPDATE synchro.sync_wal_progress
             SET registry_generation = $1, updated_at = now()
             WHERE singleton AND stream_generation = $2",
            None,
            &[
                target_generation.into(),
                reset.source_stream_generation.as_str().into(),
            ],
        )
        .map_err(|_| "activating projection registry progress failed".to_string())?
        .len();
    if progress != 1 {
        return Err("projection bootstrap progress changed".to_string());
    }
    client
        .update(
            "UPDATE synchro.sync_wal_worker_state
             SET registry_generation = $1, updated_at = now()
             WHERE worker_id = 'synchro_wal_consumer'",
            None,
            &[target_generation.into()],
        )
        .map_err(|_| "updating projection bootstrap worker state failed".to_string())?;
    Ok(())
}

fn pending_generation_chain(
    client: &SpiClient<'_>,
    source_generation: i64,
    target_generation: i64,
    stream_generation: &str,
) -> Result<Vec<i64>, String> {
    if source_generation <= 0 || target_generation <= source_generation {
        return Err("projection bootstrap registry is not a validated successor".to_string());
    }
    let mut current = target_generation;
    let mut descending = Vec::new();
    while current != source_generation {
        if descending.len() >= 10_000 || descending.contains(&current) {
            return Err("projection bootstrap registry lineage is invalid".to_string());
        }
        let row = client
            .select(
                "SELECT parent_generation, state, validated,
                        stream_generation::text AS stream_generation
                 FROM synchro.sync_registry_generations
                 WHERE generation = $1",
                None,
                &[current.into()],
            )
            .map_err(|_| "loading projection bootstrap registry lineage failed".to_string())?
            .first();
        let parent = row
            .get_by_name::<i64, &str>("parent_generation")
            .map_err(|_| "reading projection bootstrap registry lineage failed".to_string())?
            .filter(|parent| *parent > 0 && *parent < current)
            .ok_or_else(|| "projection bootstrap registry lineage is invalid".to_string())?;
        let state = required_text(&row, "state")?;
        let validated = required_bool(&row, "validated")?;
        let generation_stream = required_text(&row, "stream_generation")?;
        if state != "pending" || !validated || generation_stream != stream_generation {
            return Err("projection bootstrap registry lineage is invalid".to_string());
        }
        descending.push(current);
        current = parent;
    }
    descending.reverse();
    Ok(descending)
}

fn replace_live_projection(client: &mut SpiClient<'_>, reset: &ResetRecord) -> Result<(), String> {
    for table in [
        "synchro.sync_bucket_edges",
        "synchro.sync_capture_dependency_rows",
        "synchro.sync_captured_rows",
        "synchro.sync_row_versions",
    ] {
        client
            .update(&format!("DELETE FROM {table}"), None, &[])
            .map_err(|_| "clearing live reset projection failed".to_string())?;
    }
    client
        .update(
            "INSERT INTO synchro.sync_row_versions (
                 relation_id, record_id, row_version, fence_id, reset_id, deleted, updated_at
             )
             SELECT relation_id, record_id, row_version, fence_id, source_reset_id, deleted, now()
             FROM synchro.sync_stream_reset_row_versions
             WHERE reset_id = $1::uuid",
            None,
            &[reset.reset_id.as_str().into()],
        )
        .map_err(|_| "installing reset row versions failed".to_string())?;
    client
        .update(
            "INSERT INTO synchro.sync_captured_rows (
                 relation_id, record_id, row_data, row_version, checksum, deleted,
                 source_stream_generation, source_commit_lsn, source_event_ordinal,
                 source_reset_id, registry_generation, updated_at
             )
             SELECT relation_id, record_id, row_data, row_version, checksum, deleted,
                    $2, NULL, NULL, $1::uuid, registry_generation, now()
             FROM synchro.sync_stream_reset_captured_rows
             WHERE reset_id = $1::uuid",
            None,
            &[
                reset.reset_id.as_str().into(),
                reset.target_stream_generation.as_str().into(),
            ],
        )
        .map_err(|_| "installing reset captured rows failed".to_string())?;
    client
        .update(
            "INSERT INTO synchro.sync_capture_dependency_rows (
                 relation_id, capture_key, row_data, deleted,
                 source_stream_generation, source_commit_lsn, source_event_ordinal,
                 source_reset_id, registry_generation, updated_at
             )
             SELECT relation_id, capture_key, row_data, deleted,
                    $2, NULL, NULL, $1::uuid, registry_generation, now()
             FROM synchro.sync_stream_reset_capture_dependency_rows
             WHERE reset_id = $1::uuid",
            None,
            &[
                reset.reset_id.as_str().into(),
                reset.target_stream_generation.as_str().into(),
            ],
        )
        .map_err(|_| "installing reset capture dependency rows failed".to_string())?;
    client
        .update(
            "INSERT INTO synchro.sync_bucket_edges (
                 relation_id, table_name, record_id, bucket_id,
                 checksum, row_version, updated_at
             )
             SELECT relation_id, table_name, record_id, scope_id,
                    checksum, row_version, now()
             FROM synchro.sync_stream_reset_membership_edges
             WHERE reset_id = $1::uuid",
            None,
            &[reset.reset_id.as_str().into()],
        )
        .map_err(|_| "installing reset membership failed".to_string())?;
    Ok(())
}

fn cover_pending_fences(
    client: &mut SpiClient<'_>,
    reset: &ResetRecord,
    consistent_point: &str,
) -> Result<(), String> {
    let covered = client
        .update(
            "UPDATE synchro.sync_write_fences fence
             SET coverage = 'reset_baseline',
                 stream_generation = $2,
                 commit_lsn = NULL, event_ordinal = NULL,
                 reset_id = $1::uuid,
                 reset_slot_name = $3,
                 reset_consistent_point = $4::pg_lsn,
                 materialized_at = now()
             FROM synchro.sync_stream_reset_fence_coverage coverage
             WHERE coverage.reset_id = $1::uuid
               AND coverage.fence_id = fence.fence_id
               AND fence.coverage = 'pending'",
            None,
            &[
                reset.reset_id.as_str().into(),
                reset.target_stream_generation.as_str().into(),
                reset.candidate_slot_name.as_str().into(),
                consistent_point.into(),
            ],
        )
        .map_err(|_| "covering reset fences failed".to_string())?
        .len();
    let staged = usize::try_from(staging_counts(client, &reset.reset_id)?[3])
        .map_err(|_| "staged fence count is invalid".to_string())?;
    if covered != staged {
        return Err("not every staged fence was covered".to_string());
    }
    Ok(())
}

fn cover_projection_bootstrap_fences(
    client: &mut SpiClient<'_>,
    reset: &ResetRecord,
    barrier: &str,
) -> Result<(), String> {
    let consistent_point = reset
        .consistent_point
        .as_deref()
        .ok_or_else(|| "projection bootstrap consistent point is missing".to_string())?;
    client
        .update(
            "UPDATE synchro.sync_write_fences fence
             SET coverage = 'projection_bootstrap',
                 stream_generation = $2,
                 commit_lsn = event.commit_lsn,
                 event_ordinal = event.event_ordinal,
                 reset_id = $1::uuid,
                 reset_slot_name = $3,
                 reset_consistent_point = $4::pg_lsn,
                 materialized_at = now()
             FROM synchro.sync_projection_bootstrap_events event
             JOIN synchro.sync_projection_bootstrap_transactions transaction
               ON transaction.bootstrap_id = event.bootstrap_id
              AND transaction.commit_lsn = event.commit_lsn
             WHERE event.bootstrap_id = $1::uuid
               AND transaction.end_lsn <= $5::pg_lsn
               AND event.fence_id = fence.fence_id
               AND fence.coverage = 'pending'",
            None,
            &[
                reset.reset_id.as_str().into(),
                reset.target_stream_generation.as_str().into(),
                reset.candidate_slot_name.as_str().into(),
                consistent_point.into(),
                barrier.into(),
            ],
        )
        .map_err(|_| "covering projection bootstrap candidate fences failed".to_string())?;
    client
        .update(
            "UPDATE synchro.sync_write_fences fence
             SET coverage = 'projection_bootstrap_baseline',
                 stream_generation = $2,
                 commit_lsn = NULL,
                 event_ordinal = NULL,
                 reset_id = $1::uuid,
                 reset_slot_name = $3,
                 reset_consistent_point = $4::pg_lsn,
                 materialized_at = now()
             FROM synchro.sync_stream_reset_fence_coverage coverage
             WHERE coverage.reset_id = $1::uuid
               AND coverage.fence_id = fence.fence_id
               AND fence.coverage = 'pending'
               AND NOT EXISTS (
                   SELECT 1
                   FROM synchro.sync_projection_bootstrap_events event
                   WHERE event.bootstrap_id = $1::uuid
                     AND event.fence_id = fence.fence_id
               )",
            None,
            &[
                reset.reset_id.as_str().into(),
                reset.target_stream_generation.as_str().into(),
                reset.candidate_slot_name.as_str().into(),
                consistent_point.into(),
            ],
        )
        .map_err(|_| "covering projection bootstrap baseline fences failed".to_string())?;
    let valid = required_bool(
        &client
            .select(
                "SELECT
                     NOT EXISTS (
                         SELECT 1
                         FROM synchro.sync_projection_bootstrap_events event
                         JOIN synchro.sync_projection_bootstrap_transactions transaction
                           ON transaction.bootstrap_id = event.bootstrap_id
                          AND transaction.commit_lsn = event.commit_lsn
                         JOIN synchro.sync_write_fences fence
                           ON fence.fence_id = event.fence_id
                         WHERE event.bootstrap_id = $1::uuid
                           AND transaction.end_lsn <= $5::pg_lsn
                           AND NOT (
                               (fence.coverage = 'materialized'
                                AND fence.stream_generation = $2
                                AND fence.commit_lsn = event.commit_lsn
                                AND fence.event_ordinal = event.event_ordinal)
                               OR
                               (fence.coverage = 'projection_bootstrap'
                                AND fence.stream_generation = $2
                                AND fence.commit_lsn = event.commit_lsn
                                AND fence.event_ordinal = event.event_ordinal
                                AND fence.reset_id = $1::uuid
                                AND fence.reset_slot_name::text = $3
                                AND fence.reset_consistent_point = $4::pg_lsn)
                           )
                     )
                     AND NOT EXISTS (
                         SELECT 1
                         FROM synchro.sync_stream_reset_fence_coverage coverage
                         JOIN synchro.sync_write_fences fence
                           ON fence.fence_id = coverage.fence_id
                         WHERE coverage.reset_id = $1::uuid
                           AND NOT (
                               fence.coverage = 'materialized'
                               AND fence.stream_generation = $2
                               OR fence.coverage = 'projection_bootstrap'
                               AND fence.stream_generation = $2
                               AND fence.reset_id = $1::uuid
                               AND fence.reset_slot_name::text = $3
                               AND fence.reset_consistent_point = $4::pg_lsn
                               OR fence.coverage = 'projection_bootstrap_baseline'
                               AND fence.stream_generation = $2
                               AND fence.commit_lsn IS NULL
                               AND fence.event_ordinal IS NULL
                               AND fence.reset_id = $1::uuid
                               AND fence.reset_slot_name::text = $3
                               AND fence.reset_consistent_point = $4::pg_lsn
                           )
                     ) AS valid",
                None,
                &[
                    reset.reset_id.as_str().into(),
                    reset.target_stream_generation.as_str().into(),
                    reset.candidate_slot_name.as_str().into(),
                    consistent_point.into(),
                    barrier.into(),
                ],
            )
            .map_err(|_| "verifying projection bootstrap fence coverage failed".to_string())?
            .first(),
        "valid",
    )?;
    if !valid {
        return Err("projection bootstrap fence coverage is incomplete".to_string());
    }
    Ok(())
}

fn invalidate_client_state(client: &mut SpiClient<'_>, reset: &ResetRecord) -> Result<(), String> {
    client
        .update(
            "SELECT set_config('synchro.stream_reset_id', $1, true)",
            None,
            &[reset.reset_id.as_str().into()],
        )
        .map_err(|_| "authorizing reset invalidation failed".to_string())?;
    client
        .update("DELETE FROM synchro.sync_client_checkpoints", None, &[])
        .map_err(|_| "invalidating client checkpoints failed".to_string())?;
    client
        .update("DELETE FROM synchro.sync_rebuild_pages", None, &[])
        .map_err(|_| "invalidating rebuild pages failed".to_string())?;
    client
        .update("DELETE FROM synchro.sync_rebuild_staged_rows", None, &[])
        .map_err(|_| "invalidating rebuild rows failed".to_string())?;
    client
        .update("DELETE FROM synchro.sync_rebuild_sessions", None, &[])
        .map_err(|_| "invalidating rebuild sessions failed".to_string())?;
    Ok(())
}

fn activate_registry_and_runtime(
    client: &mut SpiClient<'_>,
    reset: &ResetRecord,
) -> Result<(), String> {
    let registry_updated = client
        .update(
            "UPDATE synchro.sync_registry_generations
             SET stream_generation = $1
             WHERE stream_generation = $2",
            None,
            &[
                reset.target_stream_generation.as_str().into(),
                reset.source_stream_generation.as_str().into(),
            ],
        )
        .map_err(|_| "moving reset registry generation failed".to_string())?
        .len();
    if registry_updated == 0 {
        return Err("reset registry lineage changed".to_string());
    }
    let runtime_updated = client
        .update(
            "UPDATE synchro.sync_runtime_state
             SET stream_generation = $1, active_slot_name = $2, updated_at = now()
             WHERE singleton AND stream_generation = $3 AND active_slot_name::text = $4",
            None,
            &[
                reset.target_stream_generation.as_str().into(),
                reset.candidate_slot_name.as_str().into(),
                reset.source_stream_generation.as_str().into(),
                reset.old_slot_name.as_str().into(),
            ],
        )
        .map_err(|_| "switching active reset stream failed".to_string())?
        .len();
    if runtime_updated != 1 {
        return Err("active reset stream changed".to_string());
    }
    client
        .update(
            "UPDATE synchro.sync_wal_progress
             SET stream_generation = $1,
                  generation_start_lsn = $3::pg_lsn,
                  materialized_commit_lsn = NULL,
                 materialized_end_lsn = NULL,
                 acknowledged_end_lsn = NULL,
                 registry_generation = $2,
                 updated_at = now()
             WHERE singleton",
            None,
            &[
                reset.target_stream_generation.as_str().into(),
                reset.source_registry_generation.into(),
                reset.consistent_point.as_deref().into(),
            ],
        )
        .map_err(|_| "resetting WAL progress failed".to_string())?;
    client
        .update(
            "INSERT INTO synchro.sync_scope_state (scope_id, stream_generation)
             SELECT scope_id, $2
             FROM synchro.sync_stream_reset_scope_digests
             WHERE reset_id = $1::uuid
             ON CONFLICT (scope_id) DO NOTHING",
            None,
            &[
                reset.reset_id.as_str().into(),
                reset.target_stream_generation.as_str().into(),
            ],
        )
        .map_err(|_| "creating reset scope state failed".to_string())?;
    client
        .update(
            "UPDATE synchro.sync_scope_state
             SET stream_generation = $1,
                 membership_generation = membership_generation + 1,
                 floor_position_kind = 'generation_start',
                 floor_commit_lsn = NULL,
                 floor_event_ordinal = NULL,
                 floor_effect_ordinal = NULL,
                 updated_at = now()",
            None,
            &[reset.target_stream_generation.as_str().into()],
        )
        .map_err(|_| "advancing reset scope generations failed".to_string())?;
    client
        .update(
            "UPDATE synchro.sync_wal_poison
             SET lifecycle = 'reset', resolved_at = now()
             WHERE lifecycle = 'active'",
            None,
            &[],
        )
        .map_err(|_| "resolving reset poison failed".to_string())?;
    client
        .update(
            "UPDATE synchro.sync_wal_worker_state
             SET registry_generation = $1,
                 materialized_commit_lsn = NULL,
                 materialized_end_lsn = NULL,
                 oldest_unmaterialized_commit_timestamp = NULL,
                 updated_at = now()
             WHERE worker_id = 'synchro_wal_consumer'",
            None,
            &[reset.source_registry_generation.into()],
        )
        .map_err(|_| "resetting worker state failed".to_string())?;
    Ok(())
}

fn clear_staging(client: &mut SpiClient<'_>, reset_id: &str) -> Result<(), String> {
    for table in [
        "synchro.sync_projection_bootstrap_events",
        "synchro.sync_projection_bootstrap_transactions",
    ] {
        client
            .update(
                &format!("DELETE FROM {table} WHERE bootstrap_id = $1::uuid"),
                None,
                &[reset_id.into()],
            )
            .map_err(|_| "clearing projection bootstrap replay state failed".to_string())?;
    }
    for table in [
        "synchro.sync_stream_reset_scope_digests",
        "synchro.sync_stream_reset_fence_coverage",
        "synchro.sync_stream_reset_membership_edges",
        "synchro.sync_stream_reset_capture_dependency_rows",
        "synchro.sync_stream_reset_captured_rows",
        "synchro.sync_stream_reset_row_versions",
    ] {
        client
            .update(
                &format!("DELETE FROM {table} WHERE reset_id = $1::uuid"),
                None,
                &[reset_id.into()],
            )
            .map_err(|_| "clearing reset staging failed".to_string())?;
    }
    Ok(())
}

fn normalize_lsn(client: &SpiClient<'_>, value: &str) -> Result<String, String> {
    if value.is_empty() || value.len() > 32 || !value.is_ascii() {
        return Err("consistent point is invalid".to_string());
    }
    client
        .select("SELECT $1::pg_lsn::text AS point", None, &[value.into()])
        .map_err(|_| "consistent point is invalid".to_string())?
        .first()
        .get_by_name::<String, &str>("point")
        .map_err(|_| "consistent point is invalid".to_string())?
        .ok_or_else(|| "consistent point is invalid".to_string())
}

fn validate_slot_name(value: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > MAX_SLOT_NAME_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err("slot name is invalid".to_string());
    }
    Ok(())
}

fn validate_snapshot_name(value: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > MAX_SNAPSHOT_NAME_BYTES
        || !value.is_ascii()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err("snapshot name is invalid".to_string());
    }
    Ok(())
}

fn required_text<T: ResetRow>(row: &T, name: &str) -> Result<String, String> {
    row.reset_text(name)
        .map_err(|_| "reading reset state failed".to_string())?
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "reset state is incomplete".to_string())
}

fn optional_text<T: ResetRow>(row: &T, name: &str) -> Result<Option<String>, String> {
    row.reset_text(name)
        .map_err(|_| "reading reset state failed".to_string())
}

fn required_bool<T: ResetRow>(row: &T, name: &str) -> Result<bool, String> {
    row.reset_bool(name)
        .map_err(|_| "reading reset state failed".to_string())?
        .ok_or_else(|| "reset state is incomplete".to_string())
}

fn required_positive_i64<T: ResetRow>(row: &T, name: &str) -> Result<i64, String> {
    row.reset_i64(name)
        .map_err(|_| "reading reset state failed".to_string())?
        .filter(|value| *value > 0)
        .ok_or_else(|| "reset state is incomplete".to_string())
}

fn required_nonnegative_i64<T: ResetRow>(row: &T, name: &str) -> Result<i64, String> {
    row.reset_i64(name)
        .map_err(|_| "reading reset state failed".to_string())?
        .filter(|value| *value >= 0)
        .ok_or_else(|| "reset state is incomplete".to_string())
}

fn required_digest<T: ResetRow>(row: &T, name: &str) -> Result<Sha256Digest, String> {
    let bytes = row
        .reset_bytes(name)
        .map_err(|_| "reading reset digest failed".to_string())?
        .ok_or_else(|| "reset digest is missing".to_string())?;
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| "reset digest is invalid".to_string())?;
    Ok(Sha256Digest::from_bytes(bytes))
}

#[cfg(feature = "pg_test")]
pub(crate) fn prepare_stream_reset_for_test(
    client: &mut SpiClient<'_>,
    candidate_slot_name: &str,
) -> Result<serde_json::Value, String> {
    prepare_stream_reset(client, candidate_slot_name)
}

#[cfg(feature = "pg_test")]
pub(crate) fn lock_stream_reset_sources_for_test(
    client: &mut SpiClient<'_>,
    reset_id: &str,
) -> Result<(), String> {
    lock_stream_reset_sources(client, reset_id, SourceLockLifetime::Transaction)
}

#[cfg(feature = "pg_test")]
pub(crate) fn stage_stream_reset_for_test(
    client: &mut SpiClient<'_>,
    reset_id: &str,
    candidate_slot_name: &str,
    consistent_point: &str,
    exported_snapshot_name: &str,
) -> Result<serde_json::Value, String> {
    stage_stream_reset(
        client,
        reset_id,
        candidate_slot_name,
        consistent_point,
        exported_snapshot_name,
        None,
        SlotValidation::TestBypass,
    )
}

#[cfg(feature = "pg_test")]
pub(crate) fn activate_stream_reset_for_test(
    client: &mut SpiClient<'_>,
    reset_id: &str,
) -> Result<serde_json::Value, String> {
    activate_stream_reset(client, reset_id, SlotValidation::TestBypass)
}

#[cfg(feature = "pg_test")]
pub(crate) fn abort_stream_reset_for_test(
    client: &mut SpiClient<'_>,
    reset_id: &str,
) -> Result<serde_json::Value, String> {
    abort_stream_reset(client, reset_id)
}

#[cfg(feature = "pg_test")]
pub(crate) fn prepare_projection_bootstrap_for_test(
    client: &mut SpiClient<'_>,
    registry_generation: i64,
    candidate_slot_name: &str,
) -> Result<serde_json::Value, String> {
    prepare_projection_bootstrap(client, registry_generation, candidate_slot_name)
}

#[cfg(feature = "pg_test")]
pub(crate) fn stage_projection_bootstrap_for_test(
    client: &mut SpiClient<'_>,
    bootstrap_id: &str,
    candidate_slot_name: &str,
    consistent_point: &str,
    exported_snapshot_name: &str,
) -> Result<serde_json::Value, String> {
    stage_projection_bootstrap(
        client,
        bootstrap_id,
        candidate_slot_name,
        consistent_point,
        exported_snapshot_name,
        None,
        SlotValidation::TestBypass,
    )
}

#[cfg(feature = "pg_test")]
pub(crate) fn request_projection_bootstrap_barrier_for_test(
    client: &mut SpiClient<'_>,
    bootstrap_id: &str,
) -> Result<serde_json::Value, String> {
    request_projection_bootstrap_barrier(client, bootstrap_id)
}

#[cfg(feature = "pg_test")]
pub(crate) fn activate_projection_bootstrap_for_test(
    client: &mut SpiClient<'_>,
    bootstrap_id: &str,
) -> Result<serde_json::Value, String> {
    activate_projection_bootstrap(client, bootstrap_id, SlotValidation::TestBypass)
}
