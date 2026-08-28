use std::collections::BTreeMap;

use pgrx::spi::{SpiClient, SpiHeapTupleData};
use synchro_core::checksum::{row_identity, scope_digest, ScopeDigestEntry, Sha256Digest};

use crate::pull::{canonical_table, row_primary_key_json, schema_hash_for_generation};
use crate::registry::TableRegistration;
use crate::spi_helpers::required_text;

pub(crate) fn compute_reset_scope_digests(
    client: &SpiClient<'_>,
    reset_id: &str,
    registry_generation: i64,
    registry: &[TableRegistration],
) -> Result<Vec<(String, Sha256Digest, i64, String)>, String> {
    let schema_hash = schema_hash_for_generation(client, registry_generation)?;
    let schema_hash_text = schema_hash.to_lower_hex();
    let scope_rows = client
        .select(
            "SELECT scope_id FROM synchro.sync_scope_state
             UNION
             SELECT scope_id FROM synchro.sync_stream_reset_membership_edges
             WHERE reset_id = $1::uuid
             ORDER BY scope_id",
            None,
            &[reset_id.into()],
        )
        .map_err(|_| "loading scope digest identities failed".to_string())?;
    let mut entries = BTreeMap::<String, Vec<ScopeDigestEntry>>::new();
    for row in scope_rows {
        entries.insert(
            required_text(&row, "scope_id", "scope digest ")?,
            Vec::new(),
        );
    }
    let edge_rows = client
        .select(
            "SELECT relation_id::text AS relation_id, record_id, scope_id, checksum
             FROM synchro.sync_stream_reset_membership_edges
             WHERE reset_id = $1::uuid
             ORDER BY scope_id, relation_id, record_id",
            None,
            &[reset_id.into()],
        )
        .map_err(|_| "loading scope digest edges failed".to_string())?;
    for row in edge_rows {
        let relation_id = required_text(&row, "relation_id", "scope digest edge ")?;
        let record_id = required_text(&row, "record_id", "scope digest edge ")?;
        let scope_id = required_text(&row, "scope_id", "scope digest edge ")?;
        let registration = registry
            .iter()
            .find(|candidate| candidate.relation_id == relation_id)
            .ok_or_else(|| "scope digest edge relation is not registered".to_string())?;
        let primary_key = row_primary_key_json(registration, &record_id)?;
        let identity = row_identity(
            &canonical_table(registration)?,
            &serde_json::to_string(&primary_key)
                .map_err(|_| "encoding scope digest row identity failed".to_string())?,
        )
        .map_err(|_| "scope digest row identity is invalid".to_string())?;
        entries
            .get_mut(&scope_id)
            .ok_or_else(|| "scope digest edge scope is unavailable".to_string())?
            .push(ScopeDigestEntry::new(
                identity,
                required_digest(&row, "checksum")?,
            ));
    }
    entries
        .into_iter()
        .map(|(scope_id, scope_entries)| {
            let row_count = i64::try_from(scope_entries.len())
                .map_err(|_| "scope digest row count overflowed".to_string())?;
            let digest = scope_digest(schema_hash, &scope_id, &scope_entries)
                .map_err(|_| "computing scope digest failed".to_string())?;
            Ok((scope_id, digest, row_count, schema_hash_text.clone()))
        })
        .collect()
}

fn required_digest(row: &SpiHeapTupleData<'_>, name: &str) -> Result<Sha256Digest, String> {
    let bytes = row
        .get_by_name::<Vec<u8>, &str>(name)
        .map_err(|_| "reading scope digest failed".to_string())?
        .ok_or_else(|| "scope digest is missing".to_string())?;
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| "scope digest is invalid".to_string())?;
    Ok(Sha256Digest::from_bytes(bytes))
}
