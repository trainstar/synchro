use std::collections::HashSet;

use pgrx::prelude::*;
use pgrx::spi::SpiClient;

use crate::registry::{MembershipDependency, RegisteredFunction, TableRegistration};

pub(crate) fn qualified_function_name(function: &RegisteredFunction) -> String {
    format!(
        "{}.{}",
        crate::pull::pg_quote_ident(&function.schema),
        crate::pull::pg_quote_ident(&function.name),
    )
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
) -> Result<Vec<String>, String> {
    evaluate_scope(|| {
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
        let rows = evaluate_as_function_owner(&dependency.impact_function, || {
            client.select(&sql, None, &[old_value.into(), new_value.into()])
        })?;
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
                .unwrap_or_else(|| {
                    pgrx::error!("impact function returned a null primary-key type")
                });
            let primary_key = row
                .get_by_name::<pgrx::JsonB, &str>("pk_value")?
                .unwrap_or_else(|| {
                    pgrx::error!("impact function returned a null primary-key value")
                });
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
    })
}

/// Runs one SPI evaluation of a registered function as the function owner in
/// a security-restricted operation. PostgreSQL uses the same model for index
/// expressions, ANALYZE, and REFRESH MATERIALIZED VIEW. Thus the function body
/// never runs with the privileges of `synchro_owner` or `synchro_worker`. The
/// evaluation must reference only the function and its inputs.
pub(crate) fn evaluate_as_function_owner<T>(
    function: &RegisteredFunction,
    evaluation: impl FnOnce() -> Result<T, spi::Error>,
) -> Result<T, spi::Error> {
    let owner = function_owner(function.oid);
    let mut saved_user = pg_sys::InvalidOid;
    let mut saved_context = 0;
    // SAFETY: These calls only save and replace the backend user identity and
    // GUC nest level. The finally block below restores both on every exit path.
    let nest_level = unsafe {
        pg_sys::GetUserIdAndSecContext(&mut saved_user, &mut saved_context);
        pg_sys::SetUserIdAndSecContext(
            owner,
            saved_context | pg_sys::SECURITY_RESTRICTED_OPERATION as i32,
        );
        pg_sys::NewGUCNestLevel()
    };
    PgTryBuilder::new(std::panic::AssertUnwindSafe(|| {
        // SAFETY: The GUC nest level opened above scopes this search path.
        unsafe { pg_sys::RestrictSearchPath() };
        evaluation()
    }))
    .finally(move || unsafe {
        pg_sys::AtEOXact_GUC(false, nest_level);
        pg_sys::SetUserIdAndSecContext(saved_user, saved_context);
    })
    .execute()
}

fn function_owner(function_oid: u32) -> pg_sys::Oid {
    // SAFETY: The PROCOID cache takes one function OID key.
    let entry = unsafe {
        pg_sys::SearchSysCache1(
            pg_sys::SysCacheIdentifier::PROCOID as i32,
            pg_sys::Oid::from(function_oid).into(),
        )
    };
    if entry.is_null() {
        pgrx::error!("registered function is missing");
    }
    // SAFETY: A PROCOID cache entry holds one pg_proc row, and this code
    // releases the entry once.
    unsafe {
        let owner = (*pg_sys::GETSTRUCT(entry).cast::<pg_sys::FormData_pg_proc>()).proowner;
        pg_sys::ReleaseSysCache(entry);
        owner
    }
}

// Return scope errors so the caller can abort materialization before it persists poison.
pub(crate) fn evaluate_scope<T>(
    evaluation: impl FnOnce() -> Result<T, spi::Error>,
) -> Result<T, String> {
    PgTryBuilder::new(std::panic::AssertUnwindSafe(|| {
        evaluation().map_err(|_| "scope evaluation failed".to_string())
    }))
    .catch_others(|_| Err("scope evaluation failed".to_string()))
    .execute()
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
