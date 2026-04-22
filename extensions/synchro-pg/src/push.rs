use pgrx::prelude::*;
use pgrx::spi::SpiClient;
use synchro_core::change::ChangeOperation;
use synchro_core::conflict::{
    TimestampConflictContext, TimestampLwwResolver, TimestampResolutionWinner,
};
use synchro_core::contract::{
    AcceptedMutation, Mutation, MutationRejectionCode, MutationStatus, ProtocolErrorCode,
    PushRequest, PushResponse, RejectedMutation, Operation,
};

use crate::client::{protocol_error_response, validate_schema_ref};
use crate::materialize::sync_record_change;
use crate::pull::{load_client_buckets, pg_quote_ident};
use crate::registry::{PushPolicy, TableRegistration};

#[derive(Clone, Copy)]
struct WriteTimestamps {
    client_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    base_updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Clone, Copy)]
enum UpdateMode {
    Standard,
    Resurrection,
}

#[derive(Clone, Copy)]
struct WritePayload<'a> {
    data: &'a serde_json::Value,
    valid_columns: &'a std::collections::HashSet<String>,
}

enum PushMutationResult {
    Applied {
        server_updated_at: Option<String>,
        server_deleted_at: Option<String>,
    },
    Conflict {
        message: String,
        server_row: Option<serde_json::Value>,
        server_version: Option<String>,
    },
    RejectedTerminal {
        code: MutationRejectionCode,
        message: String,
    },
    RejectedRetryable {
        code: MutationRejectionCode,
        message: String,
    },
}

impl PushMutationResult {
    fn is_applied(&self) -> bool {
        matches!(self, Self::Applied { .. })
    }
}

// Clock skew tolerance is read from the synchro.clock_skew_tolerance_ms GUC
// (registered in lib.rs). Accessed via crate::CLOCK_SKEW_TOLERANCE_MS_GUC.

/// Push canonical mutations through the extension write path.
#[pg_extern(name = "synchro_push")]
fn synchro_push_contract(p_user_id: &str, p_request: pgrx::JsonB) -> pgrx::JsonB {
    let request: PushRequest = match serde_json::from_value(p_request.0) {
        Ok(request) => request,
        Err(err) => {
            return protocol_error_response(
                ProtocolErrorCode::InvalidRequest,
                format!("invalid push request: {err}"),
                false,
            );
        }
    };

    if let Err(err) = request.validate() {
        return protocol_error_response(
            ProtocolErrorCode::InvalidRequest,
            format!("invalid push request: {err}"),
            false,
        );
    }

    if let Err(err_json) = validate_schema_ref(&request.schema) {
        return err_json;
    }

    Spi::connect_mut(|client| {
        client
            .update(
                "SELECT set_config('app.user_id', $1, true)",
                None,
                &[p_user_id.into()],
            )
            .unwrap_or_else(|err| pgrx::error!("setting RLS context: {}", err));

        if load_client_buckets(client, p_user_id, &request.client_id).is_err() {
            return protocol_error_response(
                ProtocolErrorCode::InvalidRequest,
                "client is not registered",
                false,
            );
        }

        let registry = load_registry_inner(client);
        let has_write_protect = check_write_protect_exists(client);
        let response_server_time = chrono::Utc::now();

        let mut accepted = Vec::new();
        let mut rejected = Vec::new();

        for mutation in &request.mutations {
            let table_reg = match registry
                .iter()
                .find(|table| table.table_name == mutation.table)
            {
                Some(table) => table,
                None => {
                    rejected.push(RejectedMutation {
                        mutation_id: mutation.mutation_id.clone(),
                        table: mutation.table.clone(),
                        pk: mutation.pk.clone(),
                        status: MutationStatus::RejectedTerminal,
                        code: MutationRejectionCode::TableNotSynced,
                        message: Some(format!("table {:?} not registered", mutation.table)),
                        server_row: None,
                        server_version: None,
                    });
                    continue;
                }
            };

            let record_id =
                match extract_mutation_record_id(&mutation.pk, &table_reg.pk_column) {
                    Some(record_id) => record_id,
                    None => {
                        rejected.push(RejectedMutation {
                            mutation_id: mutation.mutation_id.clone(),
                            table: mutation.table.clone(),
                            pk: mutation.pk.clone(),
                            status: MutationStatus::RejectedTerminal,
                            code: MutationRejectionCode::ValidationFailed,
                            message: Some(format!(
                                "primary key payload must contain string field {}",
                                table_reg.pk_column
                            )),
                            server_row: None,
                            server_version: None,
                        });
                        continue;
                    }
                };
            let operation = resolve_mutation_operation(client, mutation, &record_id, table_reg);

            let result = process_mutation(
                client,
                p_user_id,
                mutation,
                table_reg,
                has_write_protect,
                &record_id,
                operation,
            );

            if result.is_applied() {
                if let Err(err) =
                    sync_record_change(client, &registry, &mutation.table, &record_id, operation)
                {
                    pgrx::error!(
                        "materializing sync metadata for {}.{}: {}",
                        mutation.table,
                        record_id,
                        err
                    );
                }
            }

            classify_push_result(
                client,
                mutation,
                table_reg,
                result,
                &response_server_time.to_rfc3339(),
                &mut accepted,
                &mut rejected,
            );
        }

        pgrx::JsonB(
            serde_json::to_value(PushResponse {
                server_time: response_server_time,
                accepted,
                rejected,
            })
            .unwrap(),
        )
    })
}

fn process_mutation(
    client: &mut SpiClient<'_>,
    user_id: &str,
    mutation: &Mutation,
    table_reg: &TableRegistration,
    has_write_protect: bool,
    record_id: &str,
    operation: ChangeOperation,
) -> PushMutationResult {
    if table_reg.push_policy == PushPolicy::ReadOnly {
        return reject_terminal(
            MutationRejectionCode::PolicyRejected,
            format!("table {:?} is read-only", mutation.table),
        );
    }

    let mut data = mutation
        .columns
        .clone()
        .unwrap_or_else(|| serde_json::json!({}));
    strip_protected_columns(&mut data, table_reg);

    if has_write_protect {
        match call_write_protect(
            client,
            user_id,
            &mutation.table,
            write_protect_operation_name(operation),
            &data,
        ) {
            Ok(transformed) => {
                data = transformed;
                strip_protected_columns(&mut data, table_reg);
            }
            Err(msg) => {
                return reject_terminal(MutationRejectionCode::PolicyRejected, msg);
            }
        }
    }

    let valid_columns = table_reg
        .sync_columns
        .iter()
        .cloned()
        .collect::<std::collections::HashSet<_>>();
    let timestamps = WriteTimestamps {
        client_updated_at: mutation
            .client_version
            .as_deref()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc)),
        base_updated_at: mutation
            .base_version
            .as_deref()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc)),
    };
    let payload = WritePayload {
        data: &data,
        valid_columns: &valid_columns,
    };

    match operation {
        ChangeOperation::Insert => push_create(client, record_id, table_reg, payload, timestamps),
        ChangeOperation::Update => push_update(client, record_id, table_reg, payload, timestamps),
        ChangeOperation::Delete => push_delete(client, record_id, table_reg),
    }
}

fn extract_mutation_record_id(pk: &serde_json::Value, pk_column: &str) -> Option<String> {
    pk.get(pk_column)
        .and_then(|value| value.as_str())
        .map(|value| value.to_string())
}

fn resolve_mutation_operation(
    client: &SpiClient<'_>,
    mutation: &Mutation,
    record_id: &str,
    table_reg: &TableRegistration,
) -> ChangeOperation {
    match mutation.op {
        Operation::Insert => ChangeOperation::Insert,
        Operation::Update => ChangeOperation::Update,
        Operation::Delete => ChangeOperation::Delete,
        Operation::Upsert => {
            if load_existing_record(client, record_id, table_reg).is_some() {
                ChangeOperation::Update
            } else {
                ChangeOperation::Insert
            }
        }
    }
}

fn write_protect_operation_name(operation: ChangeOperation) -> &'static str {
    match operation {
        ChangeOperation::Insert => "insert",
        ChangeOperation::Update => "update",
        ChangeOperation::Delete => "delete",
    }
}

fn classify_push_result(
    client: &SpiClient<'_>,
    mutation: &Mutation,
    table_reg: &TableRegistration,
    result: PushMutationResult,
    fallback_server_version: &str,
    accepted: &mut Vec<AcceptedMutation>,
    rejected: &mut Vec<RejectedMutation>,
) {
    match result {
        PushMutationResult::Applied {
            server_updated_at,
            server_deleted_at,
        } => {
            let server_row = load_current_server_row_json(
                client,
                extract_mutation_record_id(&mutation.pk, &table_reg.pk_column).as_deref(),
                table_reg,
            );
            let server_version =
                applied_server_version(
                    server_updated_at.as_deref(),
                    server_deleted_at.as_deref(),
                    server_row.as_ref(),
                    fallback_server_version,
                );
            accepted.push(AcceptedMutation {
                mutation_id: mutation.mutation_id.clone(),
                table: mutation.table.clone(),
                pk: mutation.pk.clone(),
                status: MutationStatus::Applied,
                server_row,
                server_version,
            });
        }
        PushMutationResult::Conflict {
            message,
            server_row,
            server_version,
        } => rejected.push(RejectedMutation {
            mutation_id: mutation.mutation_id.clone(),
            table: mutation.table.clone(),
            pk: mutation.pk.clone(),
            status: MutationStatus::Conflict,
            code: MutationRejectionCode::VersionConflict,
            message: Some(message),
            server_row,
            server_version,
        }),
        PushMutationResult::RejectedTerminal { code, message } => rejected.push(RejectedMutation {
            mutation_id: mutation.mutation_id.clone(),
            table: mutation.table.clone(),
            pk: mutation.pk.clone(),
            status: MutationStatus::RejectedTerminal,
            code,
            message: Some(message),
            server_row: None,
            server_version: None,
        }),
        PushMutationResult::RejectedRetryable { code, message } => rejected.push(RejectedMutation {
            mutation_id: mutation.mutation_id.clone(),
            table: mutation.table.clone(),
            pk: mutation.pk.clone(),
            status: MutationStatus::RejectedRetryable,
            code,
            message: Some(message),
            server_row: None,
            server_version: None,
        }),
    }
}

fn load_current_server_row_json(
    client: &SpiClient<'_>,
    record_id: Option<&str>,
    table_reg: &TableRegistration,
) -> Option<serde_json::Value> {
    let record_id = record_id?;
    let existing = load_existing_record(client, record_id, table_reg)?;
    let data = existing.data?;
    let mut row: serde_json::Value = serde_json::from_str(&data).ok()?;
    canonicalize_server_row(&mut row, table_reg);
    Some(row)
}

fn applied_server_version(
    server_updated_at: Option<&str>,
    server_deleted_at: Option<&str>,
    server_row: Option<&serde_json::Value>,
    fallback_server_version: &str,
) -> String {
    if let Some(updated_at) = server_updated_at {
        return updated_at.to_string();
    }
    if let Some(deleted_at) = server_deleted_at {
        return deleted_at.to_string();
    }
    if let Some(server_row) = server_row {
        if let Some(updated_at) = server_row
            .get("updated_at")
            .and_then(|value| value.as_str())
        {
            return updated_at.to_string();
        }
        if let Some(deleted_at) = server_row
            .get("deleted_at")
            .and_then(|value| value.as_str())
        {
            return deleted_at.to_string();
        }
    }
    fallback_server_version.to_string()
}

fn canonicalize_server_row(row: &mut serde_json::Value, table_reg: &TableRegistration) {
    let Some(obj) = row.as_object_mut() else {
        return;
    };

    if table_reg.has_updated_at {
        if let Some(updated_at) = obj
            .get(&table_reg.updated_at_col)
            .and_then(|value| value.as_str())
        {
            obj.insert(
                table_reg.updated_at_col.clone(),
                serde_json::json!(canonical_timestamp(updated_at)),
            );
        }
    }

    if table_reg.has_deleted_at {
        if let Some(deleted_at) = obj
            .get(&table_reg.deleted_at_col)
            .and_then(|value| value.as_str())
        {
            obj.insert(
                table_reg.deleted_at_col.clone(),
                serde_json::json!(canonical_timestamp(deleted_at)),
            );
        }
    }
}

// ---------------------------------------------------------------------------
// CREATE
// ---------------------------------------------------------------------------

fn push_create(
    client: &mut SpiClient<'_>,
    id: &str,
    table_reg: &TableRegistration,
    payload: WritePayload<'_>,
    timestamps: WriteTimestamps,
) -> PushMutationResult {
    // Check if record already exists.
    let existing = load_existing_record(client, id, table_reg);

    if let Some(ref ex) = existing {
        if ex.deleted_at.is_none() {
            return conflict_result("record already exists".to_string(), ex);
        }
        // Exists but soft-deleted: resurrection via update path.
        return push_update_inner(
            client,
            id,
            table_reg,
            payload,
            timestamps,
            existing,
            UpdateMode::Resurrection,
        );
    }

    // Insert new record.
    let columns = allowed_columns(payload.data, table_reg, payload.valid_columns);
    if columns.is_empty() {
        return reject_terminal(
            MutationRejectionCode::ValidationFailed,
            "no allowed columns for insert".to_string(),
        );
    }

    let canonical_client_updated_at = canonical_client_updated_at(timestamps);

    // Build INSERT SQL. Column names are validated against pg_catalog.
    let col_list: String = std::iter::once(pg_quote_ident(&table_reg.pk_column))
        .chain(columns.iter().map(|c| pg_quote_ident(c)))
        .chain(
            (table_reg.has_updated_at && canonical_client_updated_at.is_some())
                .then(|| pg_quote_ident(&table_reg.updated_at_col)),
        )
        .collect::<Vec<_>>()
        .join(", ");

    let required_columns = get_required_insert_columns(client, table_reg);
    for column in &required_columns {
        let missing = payload
            .data
            .get(column)
            .map(|value| value.is_null())
            .unwrap_or(true);
        if missing {
            return reject_terminal(
                MutationRejectionCode::ValidationFailed,
                format!("missing required column {}", column),
            );
        }
    }

    // Use jsonb_populate_record for automatic type coercion (handles numeric, uuid, etc.).
    let select_list: String = std::iter::once(format!("$1::{}", table_reg.pk_type))
        .chain(columns.iter().map(|c| format!("r.{}", pg_quote_ident(c))))
        .chain(
            canonical_client_updated_at
                .as_ref()
                .map(|_| "$3::timestamptz".to_string()),
        )
        .collect::<Vec<_>>()
        .join(", ");

    let returning = if table_reg.has_updated_at {
        format!(
            " RETURNING {}::text AS updated_at",
            pg_quote_ident(&table_reg.updated_at_col)
        )
    } else {
        String::new()
    };

    let sql = format!(
        "INSERT INTO {} ({}) SELECT {} FROM jsonb_populate_record(NULL::{}, $2) r{}",
        pg_quote_ident(&table_reg.table_name),
        col_list,
        select_list,
        pg_quote_ident(&table_reg.table_name),
        returning,
    );

    let data_jsonb = pgrx::JsonB(payload.data.clone());
    let insert_result = if let Some(updated_at) = canonical_client_updated_at.as_deref() {
        client.update(&sql, None, &[id.into(), data_jsonb.into(), updated_at.into()])
    } else {
        client.update(&sql, None, &[id.into(), data_jsonb.into()])
    };

    match insert_result {
        Ok(tup) => {
            let server_updated_at = if table_reg.has_updated_at {
                tup.first().get_one::<String>().ok().flatten()
            } else {
                None
            };
            applied_result(server_updated_at, None)
        }
        Err(e) => reject_retryable(MutationRejectionCode::ServerRetryable, e.to_string()),
    }
}

// ---------------------------------------------------------------------------
// UPDATE
// ---------------------------------------------------------------------------

fn push_update(
    client: &mut SpiClient<'_>,
    id: &str,
    table_reg: &TableRegistration,
    payload: WritePayload<'_>,
    timestamps: WriteTimestamps,
) -> PushMutationResult {
    let existing = load_existing_record(client, id, table_reg);
    push_update_inner(
        client,
        id,
        table_reg,
        payload,
        timestamps,
        existing,
        UpdateMode::Standard,
    )
}

fn push_update_inner(
    client: &mut SpiClient<'_>,
    id: &str,
    table_reg: &TableRegistration,
    payload: WritePayload<'_>,
    timestamps: WriteTimestamps,
    existing: Option<ExistingRecord>,
    mode: UpdateMode,
) -> PushMutationResult {
    let existing = match existing {
        Some(existing) => existing,
        None => {
            return reject_terminal(
                MutationRejectionCode::ValidationFailed,
                "record does not exist".to_string(),
            );
        }
    };

    // If soft-deleted and not a resurrection: conflict.
    if existing.deleted_at.is_some() && !matches!(mode, UpdateMode::Resurrection) {
        return conflict_result("record has been deleted".to_string(), &existing);
    }

    // Conflict resolution via LWW (if table has updated_at).
    if table_reg.has_updated_at {
        if let Some(server_time) = &existing.updated_at {
            if let Some(server_utc) = parse_pg_timestamptz(server_time) {
                let client_time = timestamps.client_updated_at.unwrap_or(chrono::Utc::now());

                let conflict = TimestampConflictContext {
                    table: table_reg.table_name.clone(),
                    record_id: id.to_string(),
                    client_id: String::new(),
                    user_id: String::new(),
                    client_time,
                    server_time: server_utc,
                    base_version: timestamps.base_updated_at,
                };

                let tolerance =
                    chrono::Duration::milliseconds(crate::CLOCK_SKEW_TOLERANCE_MS_GUC.get() as i64);
                let resolver = TimestampLwwResolver::new(tolerance);
                let resolution = resolver.resolve(&conflict);

                if resolution.winner == TimestampResolutionWinner::Server {
                    return conflict_result(resolution.reason.to_string(), &existing);
                }
            }
        }
    }

    let canonical_client_updated_at = canonical_client_updated_at(timestamps);

    // Build UPDATE SQL.
    let columns = allowed_columns(payload.data, table_reg, payload.valid_columns);
    if columns.is_empty() && !matches!(mode, UpdateMode::Resurrection) {
        return applied_result(existing.updated_at.clone(), None);
    }

    let server_updated_at = if !columns.is_empty() {
        let mut set_clause = columns
            .iter()
            .map(|c| format!("{} = r.{}", pg_quote_ident(c), pg_quote_ident(c)))
            .collect::<Vec<_>>();
        if table_reg.has_updated_at && canonical_client_updated_at.is_some() {
            set_clause.push(format!(
                "{} = $3::timestamptz",
                pg_quote_ident(&table_reg.updated_at_col)
            ));
        }
        let set_clause = set_clause.join(", ");

        let returning = if table_reg.has_updated_at {
            format!(
                " RETURNING target.{}::text AS updated_at",
                pg_quote_ident(&table_reg.updated_at_col)
            )
        } else {
            String::new()
        };

        let sql = format!(
            "UPDATE {} AS target SET {} FROM jsonb_populate_record(NULL::{}, $1) AS r WHERE target.{} = $2::{}{}",
            pg_quote_ident(&table_reg.table_name),
            set_clause,
            pg_quote_ident(&table_reg.table_name),
            pg_quote_ident(&table_reg.pk_column),
            table_reg.pk_type,
            returning,
        );

        let data_jsonb = pgrx::JsonB(payload.data.clone());
        let update_result = if let Some(updated_at) = canonical_client_updated_at.as_deref() {
            client.update(&sql, None, &[data_jsonb.into(), id.into(), updated_at.into()])
        } else {
            client.update(&sql, None, &[data_jsonb.into(), id.into()])
        };

        match update_result {
            Ok(tup) => {
                if table_reg.has_updated_at {
                    tup.first().get_one::<String>().ok().flatten()
                } else {
                    None
                }
            }
            Err(e) => {
                return reject_retryable(MutationRejectionCode::ServerRetryable, e.to_string());
            }
        }
    } else {
        existing.updated_at.clone()
    };

    // Resurrection: clear deleted_at.
    if matches!(mode, UpdateMode::Resurrection) && table_reg.has_deleted_at {
        let resurrection_sql = if table_reg.has_updated_at {
            if canonical_client_updated_at.is_some() {
                format!(
                    "UPDATE {} SET {} = NULL, {} = $2::timestamptz WHERE {} = $1::{} RETURNING {}::text AS updated_at",
                    pg_quote_ident(&table_reg.table_name),
                    pg_quote_ident(&table_reg.deleted_at_col),
                    pg_quote_ident(&table_reg.updated_at_col),
                    pg_quote_ident(&table_reg.pk_column),
                    table_reg.pk_type,
                    pg_quote_ident(&table_reg.updated_at_col),
                )
            } else {
                format!(
                    "UPDATE {} SET {} = NULL WHERE {} = $1::{} RETURNING {}::text AS updated_at",
                    pg_quote_ident(&table_reg.table_name),
                    pg_quote_ident(&table_reg.deleted_at_col),
                    pg_quote_ident(&table_reg.pk_column),
                    table_reg.pk_type,
                    pg_quote_ident(&table_reg.updated_at_col),
                )
            }
        } else {
            format!(
                "UPDATE {} SET {} = NULL WHERE {} = $1::{}",
                pg_quote_ident(&table_reg.table_name),
                pg_quote_ident(&table_reg.deleted_at_col),
                pg_quote_ident(&table_reg.pk_column),
                table_reg.pk_type,
            )
        };

        let resurrection_result = if let Some(updated_at) = canonical_client_updated_at.as_deref() {
            client.update(&resurrection_sql, None, &[id.into(), updated_at.into()])
        } else {
            client.update(&resurrection_sql, None, &[id.into()])
        };

        match resurrection_result {
            Ok(tup) => {
                let ua = if table_reg.has_updated_at {
                    tup.first().get_one::<String>().ok().flatten()
                } else {
                    None
                };
                return applied_result(ua, None);
            }
            Err(e) => {
                return reject_retryable(MutationRejectionCode::ServerRetryable, e.to_string());
            }
        }
    }

    applied_result(server_updated_at, None)
}

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------

fn push_delete(
    client: &mut SpiClient<'_>,
    id: &str,
    table_reg: &TableRegistration,
) -> PushMutationResult {
    let existing = load_existing_record(client, id, table_reg);

    if table_reg.has_deleted_at {
        push_soft_delete(client, id, table_reg, existing)
    } else {
        push_hard_delete(client, id, table_reg)
    }
}

fn push_soft_delete(
    client: &mut SpiClient<'_>,
    id: &str,
    table_reg: &TableRegistration,
    existing: Option<ExistingRecord>,
) -> PushMutationResult {
    match &existing {
        None => {
            return reject_terminal(
                MutationRejectionCode::ValidationFailed,
                "record does not exist".to_string(),
            );
        }
        Some(ex) if ex.deleted_at.is_some() => {
            // Already soft-deleted: idempotent success.
            return applied_result(None, ex.deleted_at.clone());
        }
        _ => {}
    }

    let sql = format!(
        "UPDATE {} SET {} = now() WHERE {} = $1::{} RETURNING {}::text AS deleted_at",
        pg_quote_ident(&table_reg.table_name),
        pg_quote_ident(&table_reg.deleted_at_col),
        pg_quote_ident(&table_reg.pk_column),
        table_reg.pk_type,
        pg_quote_ident(&table_reg.deleted_at_col),
    );

    match client.update(&sql, None, &[id.into()]) {
        Ok(tup) => {
            let deleted_at = tup.first().get_one::<String>().ok().flatten();
            applied_result(None, deleted_at)
        }
        Err(e) => reject_retryable(MutationRejectionCode::ServerRetryable, e.to_string()),
    }
}

fn push_hard_delete(
    client: &mut SpiClient<'_>,
    id: &str,
    table_reg: &TableRegistration,
) -> PushMutationResult {
    let sql = format!(
        "DELETE FROM {} WHERE {} = $1::{}",
        pg_quote_ident(&table_reg.table_name),
        pg_quote_ident(&table_reg.pk_column),
        table_reg.pk_type,
    );

    match client.update(&sql, None, &[id.into()]) {
        Ok(_) => applied_result(None, None),
        Err(e) => reject_retryable(MutationRejectionCode::ServerRetryable, e.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

struct ExistingRecord {
    updated_at: Option<String>,
    deleted_at: Option<String>,
    data: Option<String>,
}

fn load_existing_record(
    client: &SpiClient<'_>,
    id: &str,
    table_reg: &TableRegistration,
) -> Option<ExistingRecord> {
    let updated_at_expr = if table_reg.has_updated_at {
        format!("{}::text", pg_quote_ident(&table_reg.updated_at_col))
    } else {
        "NULL::text".into()
    };
    let deleted_at_expr = if table_reg.has_deleted_at {
        format!("{}::text", pg_quote_ident(&table_reg.deleted_at_col))
    } else {
        "NULL::text".into()
    };

    let sql = format!(
        "SELECT {} AS updated_at, {} AS deleted_at, ({})::text AS data \
         FROM {} t WHERE {} = $1::{}",
        updated_at_expr,
        deleted_at_expr,
        crate::pull::synced_row_projection_sql(table_reg, "t"),
        pg_quote_ident(&table_reg.table_name),
        pg_quote_ident(&table_reg.pk_column),
        table_reg.pk_type,
    );

    let tup_table = match client.select(&sql, None, &[id.into()]) {
        Ok(t) => t,
        Err(_) => return None,
    };

    if let Some(row) = tup_table.into_iter().next() {
        let updated_at: Option<String> = row
            .get_by_name::<String, &str>("updated_at")
            .unwrap_or(None);
        let deleted_at: Option<String> = row
            .get_by_name::<String, &str>("deleted_at")
            .unwrap_or(None);
        let data: Option<String> = row.get_by_name::<String, &str>("data").unwrap_or(None);
        Some(ExistingRecord {
            updated_at,
            deleted_at,
            data,
        })
    } else {
        None
    }
}

fn get_required_insert_columns(
    client: &SpiClient<'_>,
    table_reg: &TableRegistration,
) -> std::collections::HashSet<String> {
    let tup_table = match client.select(
        "SELECT a.attname::text AS attname
         FROM pg_catalog.pg_attribute a
         JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
         LEFT JOIN pg_catalog.pg_attrdef d
           ON d.adrelid = a.attrelid AND d.adnum = a.attnum
         WHERE c.relname = $1
           AND n.nspname = ANY(current_schemas(false))
           AND a.attnum > 0
           AND NOT a.attisdropped
           AND a.attnotnull
           AND d.adbin IS NULL",
        None,
        &[table_reg.table_name.as_str().into()],
    ) {
        Ok(t) => t,
        Err(e) => pgrx::error!("loading required insert columns: {}", e),
    };

    let sync_columns: std::collections::HashSet<&str> = table_reg
        .sync_columns
        .iter()
        .map(|column| column.as_str())
        .collect();
    let mut required = std::collections::HashSet::new();
    for row in tup_table {
        if let Ok(Some(column_name)) = row.get_by_name::<String, &str>("attname") {
            if sync_columns.contains(column_name.as_str()) {
                required.insert(column_name);
            }
        }
    }

    required.remove(&table_reg.pk_column);
    if table_reg.has_updated_at {
        required.remove(&table_reg.updated_at_col);
    }
    if table_reg.has_deleted_at {
        required.remove(&table_reg.deleted_at_col);
    }
    required.remove("created_at");

    required
}

/// Get allowed columns: client data keys that are part of the synced shape,
/// minus protected columns (pk, timestamps, created_at).
fn allowed_columns(
    data: &serde_json::Value,
    table_reg: &TableRegistration,
    valid_columns: &std::collections::HashSet<String>,
) -> Vec<String> {
    let obj = match data.as_object() {
        Some(o) => o,
        None => return vec![],
    };

    let mut protected = std::collections::HashSet::new();
    protected.insert(table_reg.pk_column.as_str());
    if table_reg.has_updated_at {
        protected.insert(table_reg.updated_at_col.as_str());
    }
    if table_reg.has_deleted_at {
        protected.insert(table_reg.deleted_at_col.as_str());
    }
    // Protect created_at by convention.
    protected.insert("created_at");

    obj.keys()
        .filter(|k| {
            let k_str = k.as_str();
            valid_columns.contains(k_str) && !protected.contains(k_str)
        })
        .cloned()
        .collect()
}

/// Strip protected and non-synced columns from client data in place.
fn strip_protected_columns(data: &mut serde_json::Value, table_reg: &TableRegistration) {
    if let Some(obj) = data.as_object_mut() {
        let synced_columns: std::collections::HashSet<&str> = table_reg
            .sync_columns
            .iter()
            .map(|column| column.as_str())
            .collect();
        obj.retain(|key, _| synced_columns.contains(key.as_str()));
        obj.remove(&table_reg.pk_column);
        if table_reg.has_updated_at {
            obj.remove(&table_reg.updated_at_col);
        }
        if table_reg.has_deleted_at {
            obj.remove(&table_reg.deleted_at_col);
        }
        obj.remove("created_at");
    }
}

/// Check if synchro_write_protect() function exists.
fn check_write_protect_exists(client: &SpiClient<'_>) -> bool {
    let tup = match client.select(
        "SELECT EXISTS (
            SELECT 1 FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE p.proname = 'synchro_write_protect'
            AND n.nspname = ANY(current_schemas(false))
        ) AS exists",
        None,
        &[],
    ) {
        Ok(t) => t,
        Err(_) => return false,
    };
    tup.first()
        .get_one::<bool>()
        .ok()
        .flatten()
        .unwrap_or(false)
}

/// Call the developer-defined synchro_write_protect() function.
fn call_write_protect(
    client: &mut SpiClient<'_>,
    user_id: &str,
    table_name: &str,
    operation: &str,
    data: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let data_jsonb = pgrx::JsonB(data.clone());

    let result: Option<pgrx::JsonB> = match client.update(
        "SELECT synchro_write_protect($1, $2, $3, $4) AS result",
        None,
        &[
            user_id.into(),
            table_name.into(),
            operation.into(),
            data_jsonb.into(),
        ],
    ) {
        Ok(tup) => tup.first().get_one().ok().flatten(),
        Err(e) => return Err(format!("{}", e)),
    };

    match result {
        Some(jsonb) => Ok(jsonb.0),
        None => Err("write_protect returned NULL".into()),
    }
}

/// Load registry within an existing SPI context.
fn load_registry_inner(client: &SpiClient<'_>) -> Vec<TableRegistration> {
    crate::registry::load_registry_from_client(client)
        .unwrap_or_else(|e| pgrx::error!("loading registry: {}", e))
}

// ---------------------------------------------------------------------------
// Response builders
// ---------------------------------------------------------------------------

fn applied_result(
    server_updated_at: Option<String>,
    server_deleted_at: Option<String>,
) -> PushMutationResult {
    PushMutationResult::Applied {
        server_updated_at: server_updated_at.map(|value| canonical_timestamp(&value)),
        server_deleted_at: server_deleted_at.map(|value| canonical_timestamp(&value)),
    }
}

fn canonical_client_updated_at(timestamps: WriteTimestamps) -> Option<String> {
    timestamps
        .client_updated_at
        .map(|value| canonical_timestamp(&value.to_rfc3339()))
}

fn conflict_result(message: String, existing: &ExistingRecord) -> PushMutationResult {
    PushMutationResult::Conflict {
        message,
        server_row: existing_server_row(existing),
        server_version: existing_server_version(existing),
    }
}

fn reject_terminal(code: MutationRejectionCode, message: String) -> PushMutationResult {
    PushMutationResult::RejectedTerminal { code, message }
}

fn reject_retryable(code: MutationRejectionCode, message: String) -> PushMutationResult {
    PushMutationResult::RejectedRetryable { code, message }
}

fn existing_server_row(existing: &ExistingRecord) -> Option<serde_json::Value> {
    let mut row = serde_json::from_str::<serde_json::Value>(existing.data.as_ref()?).ok()?;
    canonicalize_existing_row(&mut row);
    Some(row)
}

fn existing_server_version(existing: &ExistingRecord) -> Option<String> {
    existing
        .deleted_at
        .as_deref()
        .map(canonical_timestamp)
        .or_else(|| existing.updated_at.as_deref().map(canonical_timestamp))
}

fn canonicalize_existing_row(row: &mut serde_json::Value) {
    let Some(obj) = row.as_object_mut() else {
        return;
    };

    if let Some(updated_at) = obj.get("updated_at").and_then(|value| value.as_str()) {
        obj.insert(
            "updated_at".into(),
            serde_json::json!(canonical_timestamp(updated_at)),
        );
    }
    if let Some(deleted_at) = obj.get("deleted_at").and_then(|value| value.as_str()) {
        obj.insert(
            "deleted_at".into(),
            serde_json::json!(canonical_timestamp(deleted_at)),
        );
    }
}

/// Parse a PostgreSQL timestamptz text representation into a chrono DateTime<Utc>.
///
/// PostgreSQL outputs timestamptz::text in formats like:
///   "2025-06-15 12:00:00+00"
///   "2025-06-15 12:00:00.123+00"
///   "2099-01-01 00:00:00-05"
///
/// This differs from RFC3339 which uses 'T' separator and full timezone offset (+00:00).
/// This function handles both PostgreSQL format and RFC3339 for robustness.
fn parse_pg_timestamptz(s: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    // Try RFC3339 first (handles client-supplied timestamps).
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&chrono::Utc));
    }

    // Try PostgreSQL's default timestamptz text output format.
    // Format: "YYYY-MM-DD HH:MM:SS.fff+ZZ" or "YYYY-MM-DD HH:MM:SS+ZZ"
    let formats = ["%Y-%m-%d %H:%M:%S%.f%#z", "%Y-%m-%d %H:%M:%S%#z"];
    for fmt in &formats {
        if let Ok(dt) = chrono::DateTime::parse_from_str(s, fmt) {
            return Some(dt.with_timezone(&chrono::Utc));
        }
    }

    None
}

pub(crate) fn canonical_timestamp(s: &str) -> String {
    parse_pg_timestamptz(s)
        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
        .unwrap_or_else(|| s.to_string())
}
