use pgrx::prelude::*;
use pgrx::spi::SpiClient;

use synchro_core::edge_diff::dedup_buckets;

/// Execute a table's bucket SQL to resolve which buckets a record belongs to.
///
/// The bucket SQL is a developer-defined query that takes a record ID as $1
/// and returns TEXT[] of bucket IDs. It is prepared once per table and cached
/// by PostgreSQL's SPI plan cache.
///
/// This is used by:
/// - The background worker when processing WAL events.
/// - synchro_rebuild() when re-evaluating buckets for existing records.
pub fn resolve_buckets(
    client: &SpiClient<'_>,
    bucket_sql: &str,
    record_id: &str,
) -> Result<Vec<String>, spi::Error> {
    let tup_table = client.select(bucket_sql, None, &[record_id.into()])?;

    let mut buckets = Vec::new();
    for row in tup_table {
        let val: Option<Vec<String>> = row.get::<Vec<String>>(1).unwrap_or(None);
        if let Some(arr) = val {
            buckets.extend(arr);
        }
    }

    Ok(dedup_buckets(&buckets))
}
