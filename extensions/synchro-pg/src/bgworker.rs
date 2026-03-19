use pgrx::prelude::*;
use pgrx::bgworkers::*;
use pgrx::spi::SpiClient;
use std::collections::HashMap;

use synchro_core::checksum::compute_record_checksum;
use synchro_core::edge_diff::{build_edge_diff_entries, diff_bucket_sets, dedup_buckets};
use synchro_core::protocol::Operation;

use crate::registry::{load_registry, TableRegistration};
use crate::wal_decoder::{TableMeta, WalDecoder, WalEvent};

/// GUC: replication slot name.
static REPLICATION_SLOT: &str = "synchro_slot";

/// GUC: publication name.
static PUBLICATION_NAME: &str = "synchro_pub";

/// Register the WAL background worker.
///
/// Called from `_PG_init()`.
pub fn register_bgworker() {
    BackgroundWorkerBuilder::new("synchro WAL consumer")
        .set_function("synchro_wal_worker_main")
        .set_library("synchro_pg")
        .set_argument(0i32.into_datum())
        .enable_spi_access()
        .load();
}

/// Background worker entry point.
///
/// This function runs as a separate PostgreSQL process. It:
/// 1. Loads the table registry from sync_registry.
/// 2. Connects to the replication slot.
/// 3. Decodes pgoutput messages.
/// 4. Executes bucket SQL via SPI.
/// 5. Writes changelog entries and manages bucket edges.
///
/// Currently a skeleton that will be filled when the replication protocol
/// client library is integrated.
#[pg_guard]
#[no_mangle]
pub extern "C" fn synchro_wal_worker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    BackgroundWorker::connect_worker_to_spi(Some("synchro_pg"), None);

    log!(
        "synchro WAL worker started (slot={}, publication={})",
        REPLICATION_SLOT,
        PUBLICATION_NAME,
    );

    // Load registry and build decoder table metadata.
    let registry = match load_registry_for_worker() {
        Ok(r) => r,
        Err(e) => {
            log!("synchro WAL worker: failed to load registry: {}", e);
            return;
        }
    };

    let mut _decoder = WalDecoder::new();
    let table_meta: HashMap<String, TableMeta> = registry
        .iter()
        .map(|t| {
            (
                t.table_name.clone(),
                TableMeta {
                    pk_column: t.pk_column.clone(),
                    deleted_at_col: t.deleted_at_col.clone(),
                    has_deleted_at: t.has_deleted_at,
                },
            )
        })
        .collect();
    _decoder.set_registered_tables(table_meta);

    // Main loop placeholder.
    // The actual replication protocol connection will be implemented
    // using a Rust pglogrepl library or raw protocol handling.
    // For now, the worker waits for shutdown signal.
    while BackgroundWorker::wait_latch(Some(std::time::Duration::from_secs(10))) {
        if BackgroundWorker::sighup_received() {
            // Reload registry on SIGHUP.
            log!("synchro WAL worker: reloading registry");
        }
    }

    log!("synchro WAL worker shutting down");
}

/// Apply a single WAL event: resolve buckets, diff edges, write changelog.
///
/// This function is called within the bgworker's SPI connection.
/// It matches the Go `Consumer.applyEvent` logic.
pub fn apply_wal_event(
    event: &WalEvent,
    registry: &[TableRegistration],
) -> Result<(), String> {
    let table_reg = registry
        .iter()
        .find(|t| t.table_name == event.table_name)
        .ok_or_else(|| format!("table {:?} not in registry", event.table_name))?;

    Spi::connect(|mut client| {
        // 1. Load existing bucket edges.
        let existing = load_existing_buckets(&client, &event.table_name, &event.record_id)?;

        // 2. Resolve desired buckets via bucket_sql (unless delete with existing edges).
        let desired = if event.operation != Operation::Delete || existing.is_empty() {
            execute_bucket_sql(&client, &table_reg.bucket_sql, &event.record_id)?
        } else {
            vec![]
        };

        // 3. Build changelog entries from edge diff.
        let entries = build_edge_diff_entries(
            &event.table_name,
            &event.record_id,
            event.operation,
            &existing,
            &desired,
        );

        // 4. Write changelog entries.
        write_changelog_entries(&mut client, &entries)?;

        // 5. Apply edge diff (upsert/delete bucket edges).
        apply_edge_diff(&mut client, event, &existing, &desired)?;

        Ok(())
    })
}

fn load_existing_buckets(
    client: &SpiClient<'_>,
    table_name: &str,
    record_id: &str,
) -> Result<Vec<String>, String> {
    let tup_table = client
        .select(
            "SELECT bucket_id FROM sync_bucket_edges WHERE table_name = $1 AND record_id = $2",
            None,
            Some(vec![
                (PgBuiltInOids::TEXTOID.oid(), table_name.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), record_id.into_datum()),
            ]),
        )
        .map_err(|e| format!("loading existing buckets: {e}"))?;

    let mut buckets = Vec::new();
    for row in tup_table {
        if let Ok(Some(bid)) = row.get_by_name::<String, _>("bucket_id") {
            buckets.push(bid);
        }
    }
    Ok(buckets)
}

fn execute_bucket_sql(
    client: &SpiClient<'_>,
    bucket_sql: &str,
    record_id: &str,
) -> Result<Vec<String>, String> {
    // Execute the developer-defined bucket SQL with the record ID as $1.
    let tup_table = client
        .select(
            bucket_sql,
            None,
            Some(vec![(
                PgBuiltInOids::TEXTOID.oid(),
                record_id.into_datum(),
            )]),
        )
        .map_err(|e| format!("executing bucket SQL: {e}"))?;

    let mut buckets = Vec::new();
    for row in tup_table {
        let val: Option<Vec<String>> = row.get(1).unwrap_or(None);
        if let Some(arr) = val {
            buckets.extend(arr);
        }
    }
    Ok(dedup_buckets(&buckets))
}

fn write_changelog_entries(
    client: &mut SpiClient<'_>,
    entries: &[synchro_core::edge_diff::ChangelogEntry],
) -> Result<(), String> {
    for entry in entries {
        client
            .update(
                "INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
                None,
                Some(vec![
                    (PgBuiltInOids::TEXTOID.oid(), entry.bucket_id.as_str().into_datum()),
                    (PgBuiltInOids::TEXTOID.oid(), entry.table_name.as_str().into_datum()),
                    (PgBuiltInOids::TEXTOID.oid(), entry.record_id.as_str().into_datum()),
                    (PgBuiltInOids::INT2OID.oid(), entry.operation.to_i16().into_datum()),
                ]),
            )
            .map_err(|e| format!("writing changelog entry: {e}"))?;
    }
    Ok(())
}

fn apply_edge_diff(
    client: &mut SpiClient<'_>,
    event: &WalEvent,
    existing: &[String],
    desired: &[String],
) -> Result<(), String> {
    if event.operation == Operation::Delete {
        // Remove all edges for this record.
        client
            .update(
                "DELETE FROM sync_bucket_edges WHERE table_name = $1 AND record_id = $2",
                None,
                Some(vec![
                    (PgBuiltInOids::TEXTOID.oid(), event.table_name.as_str().into_datum()),
                    (PgBuiltInOids::TEXTOID.oid(), event.record_id.as_str().into_datum()),
                ]),
            )
            .map_err(|e| format!("deleting bucket edges: {e}"))?;
        return Ok(());
    }

    // Compute checksum from event data.
    let checksum: Option<i32> = {
        // Build a JSON string from the event data for checksumming.
        let json_map: serde_json::Map<String, serde_json::Value> = event
            .data
            .iter()
            .map(|(k, v)| {
                let val = match v {
                    Some(s) => serde_json::Value::String(s.clone()),
                    None => serde_json::Value::Null,
                };
                (k.clone(), val)
            })
            .collect();
        let json_str = serde_json::to_string(&json_map).ok();
        json_str.map(|s| compute_record_checksum(&s) as i32)
    };

    let diff = diff_bucket_sets(existing, desired);

    // Upsert added + kept buckets (checksum may have changed).
    let upsert_buckets: Vec<&str> = diff
        .added
        .iter()
        .chain(diff.kept.iter())
        .map(|s| s.as_str())
        .collect();

    for bucket in &upsert_buckets {
        client
            .update(
                "INSERT INTO sync_bucket_edges (table_name, record_id, bucket_id, checksum, updated_at)
                 VALUES ($1, $2, $3, $4, now())
                 ON CONFLICT (table_name, record_id, bucket_id)
                 DO UPDATE SET checksum = EXCLUDED.checksum, updated_at = now()",
                None,
                Some(vec![
                    (PgBuiltInOids::TEXTOID.oid(), event.table_name.as_str().into_datum()),
                    (PgBuiltInOids::TEXTOID.oid(), event.record_id.as_str().into_datum()),
                    (PgBuiltInOids::TEXTOID.oid(), (*bucket).into_datum()),
                    (PgBuiltInOids::INT4OID.oid(), checksum.into_datum()),
                ]),
            )
            .map_err(|e| format!("upserting bucket edge: {e}"))?;
    }

    // Delete removed buckets.
    for bucket in &diff.removed {
        client
            .update(
                "DELETE FROM sync_bucket_edges WHERE table_name = $1 AND record_id = $2 AND bucket_id = $3",
                None,
                Some(vec![
                    (PgBuiltInOids::TEXTOID.oid(), event.table_name.as_str().into_datum()),
                    (PgBuiltInOids::TEXTOID.oid(), event.record_id.as_str().into_datum()),
                    (PgBuiltInOids::TEXTOID.oid(), bucket.as_str().into_datum()),
                ]),
            )
            .map_err(|e| format!("deleting bucket edge: {e}"))?;
    }

    Ok(())
}

fn load_registry_for_worker() -> Result<Vec<TableRegistration>, String> {
    Spi::connect(|_client| load_registry().map_err(|e| format!("loading registry: {e}")))
}

/// Write a rule failure row when bucket resolution fails.
pub fn write_rule_failure(
    event: &WalEvent,
    error_text: &str,
) -> Result<(), String> {
    let payload = serde_json::to_string(&event.data).unwrap_or_default();

    Spi::connect(|mut client| {
        client
            .update(
                "INSERT INTO sync_rule_failures (table_name, record_id, operation, error_text, payload)
                 VALUES ($1, $2, $3, $4, $5::jsonb)",
                None,
                Some(vec![
                    (PgBuiltInOids::TEXTOID.oid(), event.table_name.as_str().into_datum()),
                    (PgBuiltInOids::TEXTOID.oid(), event.record_id.as_str().into_datum()),
                    (PgBuiltInOids::INT2OID.oid(), event.operation.to_i16().into_datum()),
                    (PgBuiltInOids::TEXTOID.oid(), error_text.into_datum()),
                    (PgBuiltInOids::TEXTOID.oid(), payload.as_str().into_datum()),
                ]),
            )
            .map_err(|e| format!("writing rule failure: {e}"))?;
        Ok(())
    })
}
