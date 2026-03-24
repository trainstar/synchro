use pgrx::bgworkers::*;
use pgrx::prelude::*;
use pgrx::spi::SpiClient;

use synchro_core::change::ChangeOperation;
use synchro_core::checksum::compute_record_checksum;
use synchro_core::edge_diff::{build_edge_diff_entries, diff_bucket_sets};

use crate::bucketing::resolve_buckets;
use crate::registry::{load_registry, TableRegistration};
use crate::wal_decoder::{TableMeta, WalDecoder, WalEvent};

// Replication slot and publication name are read from GUCs:
//   crate::REPLICATION_SLOT_GUC
//   crate::PUBLICATION_NAME_GUC

/// How many WAL messages to consume per poll cycle.
static BATCH_SIZE: i32 = 500;

/// Poll interval when no changes are found (milliseconds).
static IDLE_POLL_MS: u64 = 100;

/// Read the replication slot name from the GUC, falling back to default.
fn replication_slot() -> String {
    crate::REPLICATION_SLOT_GUC
        .get()
        .and_then(|cs| cs.to_str().ok().map(String::from))
        .unwrap_or_else(|| "synchro_slot".to_string())
}

/// Read the publication name from the GUC, falling back to default.
fn publication_name() -> String {
    crate::PUBLICATION_NAME_GUC
        .get()
        .and_then(|cs| cs.to_str().ok().map(String::from))
        .unwrap_or_else(|| "synchro_pub".to_string())
}

/// Read the worker database from the GUC, falling back to postgres.
fn database_name() -> String {
    crate::DATABASE_GUC
        .get()
        .and_then(|cs| cs.to_str().ok().map(String::from))
        .unwrap_or_else(|| "postgres".to_string())
}

/// Register the WAL background worker.
///
/// Called from `_PG_init()` once GUCs are registered.
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
/// Lifecycle:
/// 1. Ensure replication slot exists.
/// 2. Load table registry from sync_registry.
/// 3. Build WAL decoder with table metadata.
/// 4. Poll pg_logical_slot_peek_binary_changes in a loop.
/// 5. Decode pgoutput messages and apply events (buckets, changelog, edges).
/// 6. Advance slot only after successful processing.
/// 7. Shutdown on SIGTERM, reload registry on SIGHUP.
#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn synchro_wal_worker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    let db_name = database_name();
    BackgroundWorker::connect_worker_to_spi(Some(&db_name), None);

    let slot_name = replication_slot();
    let pub_name_init = publication_name();
    log!(
        "synchro WAL worker started (slot={}, publication={})",
        slot_name,
        pub_name_init,
    );

    // Ensure the replication slot exists.
    if let Err(e) = ensure_replication_slot() {
        log!(
            "synchro WAL worker: failed to create replication slot: {}",
            e
        );
        return;
    }

    // Load registry and build decoder.
    let mut registry = match load_registry_for_worker() {
        Ok(r) => r,
        Err(e) => {
            log!("synchro WAL worker: failed to load registry: {}", e);
            return;
        }
    };

    let mut decoder = build_decoder(&registry);
    decoder.preload_relations(preload_relations_from_catalog(&registry));

    log!(
        "synchro WAL worker: registry loaded ({} tables), entering main loop",
        registry.len()
    );

    // Main loop: poll WAL changes, process events, handle signals.
    while BackgroundWorker::wait_latch(Some(std::time::Duration::from_millis(IDLE_POLL_MS))) {
        if BackgroundWorker::sighup_received() {
            log!("synchro WAL worker: reloading registry");
            match load_registry_for_worker() {
                Ok(r) => {
                    decoder = build_decoder(&r);
                    registry = r;
                }
                Err(e) => {
                    log!("synchro WAL worker: registry reload failed: {}", e);
                }
            }
        }

        match poll_and_process(&mut decoder, &registry) {
            Ok(0) => {}
            Ok(n) => {
                log!("synchro WAL worker: processed {} WAL messages", n);
            }
            Err(e) => {
                log!("synchro WAL worker: error processing WAL: {}", e);
            }
        }
    }

    log!("synchro WAL worker shutting down");
}

/// Build a WalDecoder from the current registry.
fn build_decoder(registry: &[TableRegistration]) -> WalDecoder {
    let mut decoder = WalDecoder::new();
    let table_meta: std::collections::HashMap<String, TableMeta> = registry
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
    decoder.set_registered_tables(table_meta);
    decoder
}

fn preload_relations_from_catalog(
    registry: &[TableRegistration],
) -> Vec<(u32, String, Vec<crate::wal_decoder::ColumnInfo>)> {
    use crate::wal_decoder::ColumnInfo;

    let mut relations = Vec::new();
    for table_reg in registry {
        let oid_sql = format!(
            "SELECT c.oid::integer FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relname = '{}' AND n.nspname = ANY(current_schemas(false))",
            table_reg.table_name.replace('\'', "''")
        );

        let relid: i32 = match Spi::get_one::<i32>(&oid_sql) {
            Ok(Some(id)) => id,
            _ => continue,
        };

        let col_sql = format!(
            "SELECT a.attname::text AS name \
             FROM pg_catalog.pg_attribute a \
             WHERE a.attrelid = {} AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
            relid
        );

        let mut columns = Vec::new();
        let _ = Spi::connect(|client| {
            if let Ok(tup) = client.select(&col_sql, None, &[]) {
                for row in tup {
                    let name: String = row
                        .get_by_name::<String, &str>("name")
                        .unwrap_or(None)
                        .unwrap_or_default();
                    if !name.is_empty() {
                        columns.push(ColumnInfo { name });
                    }
                }
            }
            Ok::<_, pgrx::spi::SpiError>(())
        });

        if !columns.is_empty() {
            relations.push((relid as u32, table_reg.table_name.clone(), columns));
        }
    }

    relations
}

/// Create the replication slot if it does not already exist.
fn ensure_replication_slot() -> Result<(), String> {
    let slot = replication_slot();
    let pub_name = publication_name();

    let slot_exists: bool = Spi::get_one_with_args(
        "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
        &[slot.as_str().into()],
    )
    .unwrap_or(Some(false))
    .unwrap_or(false);

    if !slot_exists {
        Spi::run_with_args(
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
            &[slot.as_str().into()],
        )
        .map_err(|e| format!("creating replication slot: {e}"))?;
        log!("synchro WAL worker: created replication slot '{}'", slot);
    }

    // Ensure publication exists (may not yet if no tables registered).
    let pub_exists: bool = Spi::get_one_with_args(
        "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)",
        &[pub_name.as_str().into()],
    )
    .unwrap_or(Some(false))
    .unwrap_or(false);

    if !pub_exists {
        let create_sql = format!(
            "CREATE PUBLICATION {} FOR ALL TABLES",
            crate::pull::pg_quote_ident(&pub_name)
        );
        Spi::run(&create_sql).ok();
        // Publication may not exist yet; that is fine. The first
        // synchro_register_table call creates it. We just avoid
        // a hard error on the peek call below.
    }

    Ok(())
}

/// Poll WAL changes, decode, apply events, advance slot.
///
/// Uses peek (non-consuming) + advance pattern for crash safety:
/// 1. Peek changes without advancing the slot.
/// 2. Decode and apply all events in a single SPI transaction.
/// 3. On commit, advance the slot to the last processed LSN.
///
/// If any step fails, the slot stays put and changes are re-peeked on retry.
fn poll_and_process(
    decoder: &mut WalDecoder,
    registry: &[TableRegistration],
) -> Result<usize, String> {
    let slot = replication_slot();
    let pub_name = publication_name();

    // Phase 1: Peek WAL messages and apply events in one SPI transaction.
    let result: (usize, Option<String>) = Spi::connect_mut(|client| {
        // Peek at changes without consuming them.
        let tup_table = client
            .select(
                "SELECT lsn::text, data \
                 FROM pg_logical_slot_peek_binary_changes($1, NULL, $2, \
                 'proto_version', '1', 'publication_names', $3)",
                None,
                &[
                    slot.as_str().into(),
                    BATCH_SIZE.into(),
                    pub_name.as_str().into(),
                ],
            )
            .map_err(|e| format!("peeking WAL slot: {e}"))?;

        let mut messages: Vec<Vec<u8>> = Vec::new();
        let mut max_lsn: Option<String> = None;
        for row in tup_table {
            if let Ok(Some(lsn)) = row.get::<String>(1) {
                max_lsn = Some(lsn);
            }
            if let Ok(Some(data)) = row.get::<Vec<u8>>(2) {
                messages.push(data);
            }
        }
        // SpiTupleTable dropped here, releasing borrow on client.

        if messages.is_empty() {
            return Ok::<_, String>((0, None));
        }

        let msg_count = messages.len();

        // Decode pgoutput messages into events.
        let mut events: Vec<WalEvent> = Vec::new();
        for wal_data in &messages {
            match decoder.decode(wal_data) {
                Ok(decoded) => events.extend(decoded),
                Err(e) => {
                    log!("synchro WAL worker: decode error (skipping): {}", e);
                }
            }
        }

        // Apply each event. Failures are per-event (non-fatal).
        for event in &events {
            if let Err(e) = apply_event(client, event, registry) {
                log!(
                    "synchro WAL worker: event error ({}.{}): {}",
                    event.table_name,
                    event.record_id,
                    e
                );
                let _ = write_rule_failure(client, event, &e);
            }
        }

        Ok::<_, String>((msg_count, max_lsn))
    })?;

    let (msg_count, max_lsn) = result;

    // Phase 2: Advance slot after the SPI transaction has committed.
    // Slot advancement is non-transactional in PG, so we do it only
    // after the changelog and edge writes are durable.
    if let Some(lsn) = max_lsn {
        Spi::run_with_args(
            "SELECT pg_replication_slot_advance($1, $2::pg_lsn)",
            &[slot.as_str().into(), lsn.as_str().into()],
        )
        .map_err(|e| format!("advancing slot: {e}"))?;
    }

    Ok(msg_count)
}

/// Apply a single WAL event: resolve buckets, diff edges, write changelog.
///
/// Operates within the caller's SPI transaction.
fn apply_event(
    client: &mut SpiClient<'_>,
    event: &WalEvent,
    registry: &[TableRegistration],
) -> Result<(), String> {
    let table_reg = registry
        .iter()
        .find(|t| t.table_name == event.table_name)
        .ok_or_else(|| format!("table {:?} not in registry", event.table_name))?;

    // 1. Load existing bucket edges.
    let existing = load_existing_buckets(client, &event.table_name, &event.record_id)?;

    // 2. Resolve desired buckets.
    // For deletes: the record no longer exists in the table, so bucket_sql
    // would return nothing. Use existing buckets instead.
    let desired = if event.operation == ChangeOperation::Delete {
        vec![]
    } else {
        resolve_buckets(client, &table_reg.bucket_sql, &event.record_id)
            .map_err(|e| format!("resolving buckets: {e}"))?
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
    write_changelog_entries(client, &entries)?;

    // 5. Apply edge diff (upsert/delete bucket edges).
    apply_edge_diff(client, event, &existing, &desired)?;

    Ok(())
}

fn load_existing_buckets(
    client: &SpiClient<'_>,
    table_name: &str,
    record_id: &str,
) -> Result<Vec<String>, String> {
    let tup_table = client
        .select(
            "SELECT bucket_id FROM sync_bucket_edges \
             WHERE table_name = $1 AND record_id = $2",
            None,
            &[table_name.into(), record_id.into()],
        )
        .map_err(|e| format!("loading existing buckets: {e}"))?;

    let mut buckets = Vec::new();
    for row in tup_table {
        if let Ok(Some(bid)) = row.get_by_name::<String, &str>("bucket_id") {
            buckets.push(bid);
        }
    }
    Ok(buckets)
}

fn write_changelog_entries(
    client: &mut SpiClient<'_>,
    entries: &[synchro_core::edge_diff::ChangelogEntry],
) -> Result<(), String> {
    for entry in entries {
        client
            .update(
                "INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) \
                 VALUES ($1, $2, $3, $4)",
                None,
                &[
                    entry.bucket_id.as_str().into(),
                    entry.table_name.as_str().into(),
                    entry.record_id.as_str().into(),
                    entry.operation.to_i16().into(),
                ],
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
    if event.operation == ChangeOperation::Delete {
        client
            .update(
                "DELETE FROM sync_bucket_edges \
                 WHERE table_name = $1 AND record_id = $2",
                None,
                &[
                    event.table_name.as_str().into(),
                    event.record_id.as_str().into(),
                ],
            )
            .map_err(|e| format!("deleting bucket edges: {e}"))?;
        return Ok(());
    }

    // Compute checksum from event data.
    let checksum: Option<i32> = {
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
        serde_json::to_string(&json_map)
            .ok()
            .map(|s| compute_record_checksum(&s) as i32)
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
                "INSERT INTO sync_bucket_edges \
                 (table_name, record_id, bucket_id, checksum, updated_at) \
                 VALUES ($1, $2, $3, $4, now()) \
                 ON CONFLICT (table_name, record_id, bucket_id) \
                 DO UPDATE SET checksum = EXCLUDED.checksum, updated_at = now()",
                None,
                &[
                    event.table_name.as_str().into(),
                    event.record_id.as_str().into(),
                    (*bucket).into(),
                    checksum.into(),
                ],
            )
            .map_err(|e| format!("upserting bucket edge: {e}"))?;
    }

    // Delete removed buckets.
    for bucket in &diff.removed {
        client
            .update(
                "DELETE FROM sync_bucket_edges \
                 WHERE table_name = $1 AND record_id = $2 AND bucket_id = $3",
                None,
                &[
                    event.table_name.as_str().into(),
                    event.record_id.as_str().into(),
                    bucket.as_str().into(),
                ],
            )
            .map_err(|e| format!("deleting bucket edge: {e}"))?;
    }

    Ok(())
}

fn load_registry_for_worker() -> Result<Vec<TableRegistration>, String> {
    load_registry().map_err(|e| format!("loading registry: {e}"))
}

/// Write a rule failure row when event processing fails.
/// Participates in the caller's SPI transaction.
fn write_rule_failure(
    client: &mut SpiClient<'_>,
    event: &WalEvent,
    error_text: &str,
) -> Result<(), String> {
    let payload = serde_json::to_string(&event.data).unwrap_or_default();

    client
        .update(
            "INSERT INTO sync_rule_failures \
             (table_name, record_id, operation, error_text, payload) \
             VALUES ($1, $2, $3, $4, $5::jsonb)",
            None,
            &[
                event.table_name.as_str().into(),
                event.record_id.as_str().into(),
                event.operation.to_i16().into(),
                error_text.into(),
                payload.as_str().into(),
            ],
        )
        .map_err(|e| format!("writing rule failure: {e}"))?;
    Ok(())
}
