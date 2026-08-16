use std::collections::HashSet;

use pgrx::prelude::*;
use pgrx::spi::SpiClient;

use crate::registry::{MembershipDependency, RegisteredFunction, TableRegistration};

/// Evaluate a registered membership function against the worker projection.
///
/// The function identity, argument type, and positive result bound come from
/// the validated registry generation. This path never evaluates caller SQL.
pub(crate) fn resolve_membership(
    client: &SpiClient<'_>,
    registration: &TableRegistration,
    record_id: &str,
) -> Result<Vec<String>, spi::Error> {
    resolve_registered_membership(
        client,
        &registration.membership_function,
        &registration.pk_type,
        record_id,
        registration.max_scope_fanout,
    )
}

pub(crate) fn resolve_registered_membership(
    client: &SpiClient<'_>,
    function: &RegisteredFunction,
    primary_key_type: &str,
    record_id: &str,
    max_scope_fanout: i32,
) -> Result<Vec<String>, spi::Error> {
    if max_scope_fanout <= 0 {
        pgrx::error!("registered membership evaluation metadata is invalid");
    }
    let result_limit = max_scope_fanout
        .checked_add(1)
        .unwrap_or_else(|| pgrx::error!("registered scope fanout limit overflowed"));
    let maximum = usize::try_from(max_scope_fanout)
        .unwrap_or_else(|_| pgrx::error!("registered scope fanout limit is invalid"));
    let sql = membership_query(function, primary_key_type, result_limit);
    let rows = client.select(&sql, None, &[record_id.into()])?;
    let mut scopes = Vec::new();
    let mut seen = HashSet::new();
    let mut row_count = 0usize;
    for row in rows {
        row_count = row_count
            .checked_add(1)
            .unwrap_or_else(|| pgrx::error!("membership result count overflowed"));
        if row_count > maximum {
            pgrx::error!("membership function exceeded its registered scope fanout bound");
        }
        let scope_id = row
            .get_by_name::<String, &str>("scope_id")?
            .unwrap_or_else(|| pgrx::error!("membership function returned a null scope ID"));
        validate_scope_id(&scope_id);
        if seen.insert(scope_id.clone()) {
            scopes.push(scope_id);
        }
    }
    scopes.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    Ok(scopes)
}

pub(crate) fn membership_query(
    function: &RegisteredFunction,
    primary_key_type: &str,
    result_limit: i32,
) -> String {
    format!(
        "SELECT membership.scope_id
         FROM {}($1::{}) AS membership(scope_id)
         LIMIT {}",
        qualified_function_name(function),
        primary_key_type,
        result_limit,
    )
}

pub(crate) fn qualified_function_name(function: &RegisteredFunction) -> String {
    format!(
        "{}.{}",
        crate::pull::pg_quote_ident(&function.schema),
        crate::pull::pg_quote_ident(&function.name),
    )
}

fn validate_scope_id(scope_id: &str) {
    if scope_id.is_empty()
        || scope_id.as_bytes().contains(&0)
        || scope_id.chars().any(char::is_control)
    {
        pgrx::error!("membership function returned an invalid scope ID");
    }
}

/// Evaluate one registered dependency impact function.
///
/// The result must name exactly the declaration target and use its portable
/// primary-key type. The worker unions these keys before it reevaluates target
/// membership from final transaction projections.
pub(crate) fn resolve_dependency_impacts(
    client: &SpiClient<'_>,
    dependency: &MembershipDependency,
    target: &TableRegistration,
    old_row: Option<&serde_json::Value>,
    new_row: Option<&serde_json::Value>,
) -> Result<Vec<String>, spi::Error> {
    if dependency.max_impact_rows <= 0 || dependency.target_table_id != target.table_id {
        pgrx::error!("registered dependency metadata is invalid");
    }
    let result_limit = dependency
        .max_impact_rows
        .checked_add(1)
        .unwrap_or_else(|| pgrx::error!("registered impact row limit overflowed"));
    let maximum = usize::try_from(dependency.max_impact_rows)
        .unwrap_or_else(|_| pgrx::error!("registered impact row limit is invalid"));
    let sql = dependency_impact_query(&dependency.impact_function, result_limit);
    let old_value = old_row.cloned().map(pgrx::JsonB);
    let new_value = new_row.cloned().map(pgrx::JsonB);
    let rows = client.select(&sql, None, &[old_value.into(), new_value.into()])?;
    let mut record_ids = Vec::new();
    let mut seen = HashSet::new();
    let mut row_count = 0usize;
    for row in rows {
        row_count = row_count
            .checked_add(1)
            .unwrap_or_else(|| pgrx::error!("impact result count overflowed"));
        if row_count > maximum {
            pgrx::error!("impact function exceeded its registered row bound");
        }
        let table_id = row
            .get_by_name::<String, &str>("table_id")?
            .unwrap_or_else(|| pgrx::error!("impact function returned a null table ID"));
        let portable_type = row
            .get_by_name::<String, &str>("pk_type")?
            .unwrap_or_else(|| pgrx::error!("impact function returned a null primary-key type"));
        let primary_key = row
            .get_by_name::<pgrx::JsonB, &str>("pk_value")?
            .unwrap_or_else(|| pgrx::error!("impact function returned a null primary-key value"));
        if table_id != dependency.target_table_id || portable_type != target.pk_portable_type {
            pgrx::error!("impact function returned a row outside its declared target");
        }
        let record_id = canonical_record_id(&primary_key.0, &portable_type);
        if !seen.insert(record_id.clone()) {
            pgrx::error!("impact function returned a duplicate row");
        }
        record_ids.push(record_id);
    }
    record_ids.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    Ok(record_ids)
}

pub(crate) fn dependency_impact_query(function: &RegisteredFunction, result_limit: i32) -> String {
    format!(
        "SELECT (row_ref).table_id::text AS table_id,
                (row_ref).pk_type::text AS pk_type,
                (row_ref).pk_value AS pk_value
         FROM {}($1::jsonb, $2::jsonb) AS row_ref
         LIMIT {}",
        qualified_function_name(function),
        result_limit,
    )
}

fn canonical_record_id(value: &serde_json::Value, portable_type: &str) -> String {
    match portable_type {
        "string" => value.as_str().map(String::from).unwrap_or_else(|| {
            pgrx::error!("impact function returned an invalid string primary key")
        }),
        "int" => value
            .as_i64()
            .and_then(|value| i32::try_from(value).ok())
            .map(|value| value.to_string())
            .unwrap_or_else(|| pgrx::error!("impact function returned an invalid int primary key")),
        "int64" => value
            .as_i64()
            .map(|value| value.to_string())
            .unwrap_or_else(|| {
                pgrx::error!("impact function returned an invalid int64 primary key")
            }),
        _ => pgrx::error!("impact function returned an invalid primary-key type"),
    }
}
