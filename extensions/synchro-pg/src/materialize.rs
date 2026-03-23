use pgrx::prelude::*;
use pgrx::spi::SpiClient;
use synchro_core::change::ChangeOperation;
use synchro_core::checksum::compute_record_checksum;
use synchro_core::edge_diff::{build_edge_diff_entries, diff_bucket_sets};

use crate::bucketing::resolve_buckets;
use crate::pull::pg_quote_ident;
use crate::registry::{load_registry, TableRegistration};

#[pg_extern]
fn synchro_backfill_bucket_edges(p_table_name: default!(Option<&str>, "NULL")) -> pgrx::JsonB {
    Spi::connect_mut(|client| {
        let registry = load_registry()
            .unwrap_or_else(|err| pgrx::error!("loading registry for backfill: {}", err));
        let tables: Vec<&TableRegistration> = registry
            .iter()
            .filter(|table| {
                p_table_name
                    .map(|name| name == table.table_name)
                    .unwrap_or(true)
            })
            .collect();

        let table_names: Vec<&str> = tables
            .iter()
            .map(|table| table.table_name.as_str())
            .collect();
        if !table_names.is_empty() {
            client
                .update(
                    "DELETE FROM sync_bucket_edges WHERE table_name = ANY($1)",
                    None,
                    &[table_names.clone().into()],
                )
                .unwrap_or_else(|err| pgrx::error!("clearing bucket edges for backfill: {}", err));
        }

        let mut edge_count = 0i64;
        let mut record_count = 0i64;
        for table in tables {
            let record_ids = load_live_record_ids(client, table).unwrap_or_else(|err| {
                pgrx::error!("loading record ids for {}: {}", table.table_name, err)
            });
            for record_id in record_ids {
                let desired = resolve_buckets(client, &table.bucket_sql, &record_id)
                    .unwrap_or_else(|err| {
                        pgrx::error!(
                            "resolving buckets for {}.{}: {}",
                            table.table_name,
                            record_id,
                            err
                        )
                    });
                let checksum =
                    current_row_checksum(client, table, &record_id).unwrap_or_else(|err| {
                        pgrx::error!(
                            "loading checksum for {}.{}: {}",
                            table.table_name,
                            record_id,
                            err
                        )
                    });
                for bucket_id in desired {
                    client
                        .update(
                            "INSERT INTO sync_bucket_edges \
                             (table_name, record_id, bucket_id, checksum, updated_at) \
                             VALUES ($1, $2, $3, $4, now())",
                            None,
                            &[
                                table.table_name.as_str().into(),
                                record_id.as_str().into(),
                                bucket_id.as_str().into(),
                                checksum.into(),
                            ],
                        )
                        .unwrap_or_else(|err| {
                            pgrx::error!(
                                "backfilling bucket edge for {}.{}: {}",
                                table.table_name,
                                record_id,
                                err
                            )
                        });
                    edge_count += 1;
                }
                record_count += 1;
            }
        }

        pgrx::JsonB(serde_json::json!({
            "tables": table_names,
            "records": record_count,
            "edges": edge_count,
        }))
    })
}

pub(crate) fn sync_record_change(
    client: &mut SpiClient<'_>,
    registry: &[TableRegistration],
    table_name: &str,
    record_id: &str,
    operation: ChangeOperation,
) -> Result<(), String> {
    let table = registry
        .iter()
        .find(|table| table.table_name == table_name)
        .ok_or_else(|| format!("table {:?} not in registry", table_name))?;

    let existing = load_existing_buckets(client, table_name, record_id)?;
    let desired = if operation == ChangeOperation::Delete {
        Vec::new()
    } else {
        resolve_buckets(client, &table.bucket_sql, record_id)
            .map_err(|err| format!("resolving buckets: {err}"))?
    };

    let entries = build_edge_diff_entries(table_name, record_id, operation, &existing, &desired);
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
            .map_err(|err| format!("writing changelog entry: {err}"))?;
    }

    apply_bucket_diff(client, table, record_id, operation, &existing, &desired)
}

fn apply_bucket_diff(
    client: &mut SpiClient<'_>,
    table: &TableRegistration,
    record_id: &str,
    operation: ChangeOperation,
    existing: &[String],
    desired: &[String],
) -> Result<(), String> {
    if operation == ChangeOperation::Delete {
        client
            .update(
                "DELETE FROM sync_bucket_edges WHERE table_name = $1 AND record_id = $2",
                None,
                &[table.table_name.as_str().into(), record_id.into()],
            )
            .map_err(|err| format!("deleting bucket edges: {err}"))?;
        return Ok(());
    }

    let checksum = current_row_checksum(client, table, record_id)?;
    let diff = diff_bucket_sets(existing, desired);
    let upsert_buckets: Vec<&str> = diff
        .added
        .iter()
        .chain(diff.kept.iter())
        .map(|bucket| bucket.as_str())
        .collect();

    for bucket_id in upsert_buckets {
        client
            .update(
                "INSERT INTO sync_bucket_edges \
                 (table_name, record_id, bucket_id, checksum, updated_at) \
                 VALUES ($1, $2, $3, $4, now()) \
                 ON CONFLICT (table_name, record_id, bucket_id) DO UPDATE \
                 SET checksum = EXCLUDED.checksum, updated_at = now()",
                None,
                &[
                    table.table_name.as_str().into(),
                    record_id.into(),
                    bucket_id.into(),
                    checksum.into(),
                ],
            )
            .map_err(|err| format!("upserting bucket edge: {err}"))?;
    }

    for bucket_id in diff.removed {
        client
            .update(
                "DELETE FROM sync_bucket_edges \
                 WHERE table_name = $1 AND record_id = $2 AND bucket_id = $3",
                None,
                &[
                    table.table_name.as_str().into(),
                    record_id.into(),
                    bucket_id.as_str().into(),
                ],
            )
            .map_err(|err| format!("removing bucket edge: {err}"))?;
    }

    Ok(())
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
            &[table_name.into(), record_id.into()],
        )
        .map_err(|err| format!("loading existing buckets: {err}"))?;

    let mut buckets = Vec::new();
    for row in tup_table {
        if let Ok(Some(bucket_id)) = row.get_by_name::<String, &str>("bucket_id") {
            buckets.push(bucket_id);
        }
    }
    Ok(buckets)
}

fn load_live_record_ids(
    client: &SpiClient<'_>,
    table: &TableRegistration,
) -> Result<Vec<String>, String> {
    let sql = if table.has_deleted_at {
        format!(
            "SELECT {}::text AS record_id FROM {} WHERE {} IS NULL ORDER BY {}",
            pg_quote_ident(&table.pk_column),
            pg_quote_ident(&table.table_name),
            pg_quote_ident(&table.deleted_at_col),
            pg_quote_ident(&table.pk_column),
        )
    } else {
        format!(
            "SELECT {}::text AS record_id FROM {} ORDER BY {}",
            pg_quote_ident(&table.pk_column),
            pg_quote_ident(&table.table_name),
            pg_quote_ident(&table.pk_column),
        )
    };

    let tup_table = client
        .select(&sql, None, &[])
        .map_err(|err| format!("querying live record ids: {err}"))?;

    let mut record_ids = Vec::new();
    for row in tup_table {
        if let Ok(Some(record_id)) = row.get_by_name::<String, &str>("record_id") {
            record_ids.push(record_id);
        }
    }
    Ok(record_ids)
}

fn current_row_checksum(
    client: &SpiClient<'_>,
    table: &TableRegistration,
    record_id: &str,
) -> Result<Option<i32>, String> {
    let sql = format!(
        "SELECT row_to_json(t)::text AS row_json FROM {} t WHERE {} = $1::{}",
        pg_quote_ident(&table.table_name),
        pg_quote_ident(&table.pk_column),
        table.pk_type,
    );

    let row_json: Option<String> = client
        .select(&sql, None, &[record_id.into()])
        .map_err(|err| format!("loading current row: {err}"))?
        .first()
        .get_by_name::<String, &str>("row_json")
        .unwrap_or(None);

    Ok(row_json.map(|row_json| compute_record_checksum(&row_json) as i32))
}
