use std::collections::{HashMap, HashSet};

use base64::Engine;
use chrono::{SecondsFormat, Timelike, Utc};
use pgrx::prelude::*;
use pgrx::spi::SpiClient;
use synchro_core::checksum::{
    encode_typed_value, row_digest, row_identity, CanonicalRow, ChecksumObject, FieldSpec,
    PortableType, SchemaHash,
};
use synchro_core::contract::{
    AcceptedMutation, ErrorBody, ErrorResponse, Mutation, Operation, ProtocolErrorCode,
    PushRequest, PushResponse, RejectedMutation, SchemaManifest, SchemaRef, TableSchema,
};
use synchro_core::fingerprint::{batch_fingerprint, mutation_fingerprint, normalized_mutation};

use crate::client::{acquire_client_identity_lock, PROTOCOL_VERSION};
use crate::pull::{pg_quote_ident, synced_row_projection_sql};
use crate::registry::{qualified_relation_name, FieldRegistration, PushPolicy, TableRegistration};
use crate::spi_helpers::is_lower_uuid;

const FINGERPRINT_ALGORITHM: &str = "sha256";
const FINGERPRINT_VERSION: i64 = 1;
const BATCH_FINGERPRINT_DOMAIN: &str = "synchro:v3:push-batch-fingerprint:v1";
const MUTATION_FINGERPRINT_DOMAIN: &str = "synchro:v3:push-mutation-fingerprint:v1";
const MAX_PUSH_REQUEST_BYTES: usize = 1 << 20;

#[derive(Debug, Clone)]
struct Fingerprints {
    batch: Vec<u8>,
    mutations: Vec<Vec<u8>>,
    sealed_request: Vec<u8>,
}

#[derive(Debug, Clone)]
struct StoredBatch {
    algorithm: String,
    version: i64,
    domain: String,
    digest: Vec<u8>,
    state: String,
    http_status: Option<i32>,
    response: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
struct StoredMutation {
    algorithm: String,
    version: i64,
    domain: String,
    digest: Vec<u8>,
    outcome_schema: SchemaRef,
    outcome: serde_json::Value,
}

struct EvaluationContext<'a> {
    submitted_schema: &'a SchemaRef,
    authored_tables: &'a AuthoredTableIndexes<'a>,
    current_tables: &'a TableIndex<'a>,
    registry: &'a HashMap<String, TableRegistration>,
    ever_synced_tables: &'a HashSet<String>,
    has_write_protect: bool,
    mutation_is_push_unit: bool,
}

type TableIndex<'a> = HashMap<String, &'a TableSchema>;
type AuthoredTableIndexes<'a> = HashMap<SchemaRef, TableIndex<'a>>;

struct EvaluationTarget {
    table_id: String,
    primary_key_field_id: String,
    primary_key_type: String,
    primary_key_value: serde_json::Value,
    row_identity: Option<Vec<u8>>,
}

struct SchemaIncompatibility {
    authored_schema: SchemaRef,
    current_schema: SchemaRef,
    field_ids: Vec<String>,
}

struct ConflictTarget<'a> {
    existing: Option<&'a RowState>,
    table: &'a TableRegistration,
    record_id: &'a str,
    row_identity: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
struct RowState {
    data: Option<serde_json::Value>,
    row_version: Option<String>,
    deleted: bool,
}

impl RowState {
    /// The capture fence trigger updates `sync_row_versions` in the transaction of each
    /// source write. Thus, a live version without a visible source row identifies a
    /// present row that the row security of the source relation hides from the caller.
    fn hidden_by_row_security(&self) -> bool {
        self.data.is_none() && !self.deleted
    }
}

#[derive(Debug, Clone)]
struct EvaluatedMutation {
    mutation: Mutation,
    outcome: serde_json::Value,
    accepted: bool,
    new_write: bool,
    outcome_schema: SchemaRef,
    table_id: String,
    primary_key_field_id: String,
    primary_key_type: String,
    primary_key_value: serde_json::Value,
    row_identity: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
struct MutationLedgerInsert {
    mutation: Mutation,
    outcome: serde_json::Value,
    outcome_bytes: Vec<u8>,
    outcome_schema: SchemaRef,
    table_id: String,
    primary_key_field_id: String,
    primary_key_type: String,
    primary_key_value: serde_json::Value,
    row_identity: Option<Vec<u8>>,
    ordinal: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DmlOutcome {
    Applied,
    NotApplied,
    ValidationFailed,
    PolicyRejected,
}

fn require_no_pending_deferred_trigger_events(client: &SpiClient<'_>) {
    let relations = client
        .select(
            "SELECT DISTINCT tgrelid::bigint AS relation_oid
             FROM pg_catalog.pg_trigger
             WHERE tgdeferrable",
            None,
            &[],
        )
        .unwrap_or_else(|_| pgrx::error!("checking pending deferred trigger events failed"));
    for relation in relations {
        let relation_oid = relation
            .get_by_name::<i64, &str>("relation_oid")
            .unwrap_or_else(|_| pgrx::error!("reading deferred trigger relation failed"))
            .and_then(|oid| u32::try_from(oid).ok())
            .map(pg_sys::Oid::from)
            .unwrap_or_else(|| pgrx::error!("deferred trigger relation is invalid"));
        // A deferred check uses current user authority, which is synchro_owner here.
        if unsafe { pg_sys::AfterTriggerPendingOnRel(relation_oid) } {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_INVALID_TRANSACTION_STATE,
                "synchro_push requires a transaction without pending deferred trigger events"
            );
        }
    }
}

/// Push canonical Protocol 3 mutations through one transactional extension path.
#[pg_extern(name = "synchro_push")]
fn synchro_push_contract(p_user_id: &str, p_request: pgrx::JsonB) -> String {
    if p_user_id.is_empty() {
        return push_error(
            ProtocolErrorCode::AuthRequired,
            "authentication is required",
            false,
            None,
            None,
        );
    }

    // Parser errors can quote submitted values, so the response keeps only the class.
    let request: PushRequest = match serde_json::from_value(p_request.0.clone()) {
        Ok(request) => request,
        Err(_) => {
            return push_protocol_error(
                ProtocolErrorCode::InvalidRequest,
                "invalid push request",
                false,
            )
        }
    };

    if let Err(error) = request.validate() {
        return push_protocol_error(
            ProtocolErrorCode::InvalidRequest,
            format!("invalid push request: {error}"),
            false,
        );
    }

    let sealed_request = match canonical_json_bytes(&request) {
        Ok(bytes) if bytes.len() <= MAX_PUSH_REQUEST_BYTES => bytes,
        _ => {
            return push_protocol_error(
                ProtocolErrorCode::InvalidRequest,
                "invalid push request",
                false,
            )
        }
    };

    // Fingerprints are computed before loading mutable registry, client, policy, or row state.
    let fingerprints = match compute_fingerprints(p_user_id, &request, sealed_request) {
        Ok(fingerprints) => fingerprints,
        Err(_) => {
            return push_protocol_error(
                ProtocolErrorCode::InvalidRequest,
                "invalid push request",
                false,
            )
        }
    };

    Spi::connect_mut(|client| {
        set_push_context(client, p_user_id, &request.client_id);
        acquire_client_identity_lock(client, p_user_id, &request.client_id);

        if client_is_retired(client, p_user_id, &request.client_id) {
            return push_error(
                ProtocolErrorCode::ClientRetired,
                "client identity is retired",
                false,
                None,
                None,
            );
        }
        if !scoped_client_exists(client, p_user_id, &request.client_id) {
            return push_error(
                ProtocolErrorCode::AuthRequired,
                "authentication is required",
                false,
                None,
                None,
            );
        }

        let mut lock_ids = vec![format!(
            "batch\0{p_user_id}\0{}\0{}",
            request.client_id, request.batch_id
        )];
        lock_ids.extend(request.mutations.iter().map(|mutation| {
            format!(
                "mutation\0{p_user_id}\0{}\0{}",
                request.client_id, mutation.mutation_id
            )
        }));
        lock_ids.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        for identity in lock_ids {
            lock_push_identity(client, &identity);
        }

        if let Some(batch) =
            load_batch_ledger(client, p_user_id, &request.client_id, &request.batch_id)
                .unwrap_or_else(|_| pgrx::error!("loading push batch ledger failed"))
        {
            if batch.algorithm != FINGERPRINT_ALGORITHM
                || batch.version != FINGERPRINT_VERSION
                || batch.domain != BATCH_FINGERPRINT_DOMAIN
                || batch.digest != fingerprints.batch
            {
                return push_error(
                    ProtocolErrorCode::IdempotencyConflict,
                    "push identity has different content",
                    false,
                    None,
                    None,
                );
            }
            if batch.state != "completed" {
                return push_error(
                    ProtocolErrorCode::TemporaryUnavailable,
                    "request could not be committed",
                    true,
                    None,
                    None,
                );
            }
            let response = batch
                .response
                .as_deref()
                .filter(|response| !response.is_empty())
                .unwrap_or_else(|| pgrx::error!("completed push batch has no response"));
            if batch.http_status != Some(200) {
                pgrx::error!("completed push batch has an invalid status")
            }
            return json_bytes_response(response);
        }

        let mutation_ids = request
            .mutations
            .iter()
            .map(|mutation| mutation.mutation_id.clone())
            .collect::<Vec<_>>();
        let stored_mutations =
            load_mutation_ledgers(client, p_user_id, &request.client_id, &mutation_ids)
                .unwrap_or_else(|_| pgrx::error!("loading push mutation ledger failed"));
        let mutation_fingerprints = request
            .mutations
            .iter()
            .zip(fingerprints.mutations.iter())
            .map(|(mutation, digest)| (mutation.mutation_id.as_str(), digest.as_slice()))
            .collect::<HashMap<_, _>>();
        for mutation in &request.mutations {
            let Some(stored) = stored_mutations.get(&mutation.mutation_id) else {
                continue;
            };
            let digest = mutation_fingerprints
                .get(mutation.mutation_id.as_str())
                .copied()
                .unwrap_or_else(|| pgrx::error!("push mutation fingerprint is missing"));
            if stored.algorithm != FINGERPRINT_ALGORITHM
                || stored.version != FINGERPRINT_VERSION
                || stored.domain != MUTATION_FINGERPRINT_DOMAIN
                || stored.digest != digest
            {
                return push_error(
                    ProtocolErrorCode::IdempotencyConflict,
                    "push identity has different content",
                    false,
                    None,
                    None,
                );
            }
        }
        // Exact batch replay returned above, so every stored mutation belongs to another batch.
        if request.atomic == Some(true) && !stored_mutations.is_empty() {
            return push_protocol_error(
                ProtocolErrorCode::InvalidRequest,
                "atomic push request reuses a mutation from another batch",
                false,
            );
        }
        if request.atomic != Some(true)
            && stored_mutations.values().any(|stored| {
                stored
                    .outcome
                    .get("code")
                    .and_then(serde_json::Value::as_str)
                    == Some("atomic_batch_rejected")
            })
        {
            return push_protocol_error(
                ProtocolErrorCode::InvalidRequest,
                "non-atomic push request reuses an atomic batch rejection",
                false,
            );
        }

        let generation = match check_client_generation(
            client,
            p_user_id,
            &request.client_id,
            request.client_generation,
        ) {
            Ok(generation) => generation,
            Err(GenerationGate::NotRegistered) => {
                return push_protocol_error(
                    ProtocolErrorCode::AuthRequired,
                    "authentication is required",
                    false,
                )
            }
            Err(GenerationGate::Expired(current)) => {
                return push_error(
                    ProtocolErrorCode::ClientGenerationExpired,
                    "client generation has expired",
                    false,
                    Some(current),
                    None,
                )
            }
            Err(GenerationGate::Operational) => {
                pgrx::error!("checking client generation failed")
            }
        };
        if generation != request.client_generation {
            return push_error(
                ProtocolErrorCode::ClientGenerationExpired,
                "client generation has expired",
                false,
                Some(generation),
                None,
            );
        }

        crate::registry::acquire_registry_write_lock(client)
            .unwrap_or_else(|_| pgrx::error!("locking active registry failed"));
        crate::schema::ensure_schema_manifest(client);
        let current_manifest = crate::schema::load_latest_schema_manifest(client);
        let current_schema = SchemaRef {
            version: current_manifest.schema_version,
            hash: current_manifest.schema_hash.clone(),
        };
        if request.schema != current_schema {
            return push_error(
                ProtocolErrorCode::SchemaMismatch,
                "schema does not match the current server schema",
                false,
                None,
                Some((current_schema, request.schema.clone())),
            );
        }

        let mut manifests = load_authored_manifests(client, &request.mutations);
        manifests.insert(current_schema.clone(), current_manifest);
        let authored_tables = manifests
            .iter()
            .map(|(reference, manifest)| {
                let tables = manifest
                    .tables
                    .iter()
                    .map(|table| (table.table_id.clone(), table))
                    .collect();
                (reference.clone(), tables)
            })
            .collect::<AuthoredTableIndexes<'_>>();
        let current_tables = authored_tables
            .get(&current_schema)
            .expect("current manifest was indexed");
        let registry = load_registry_inner(client)
            .into_iter()
            .map(|registration| (registration.table_id.clone(), registration))
            .collect::<HashMap<_, _>>();
        let historically_synced_table_ids = request
            .mutations
            .iter()
            .filter(|mutation| {
                authored_tables
                    .get(&mutation.authored_schema)
                    .is_some_and(|tables| !tables.contains_key(&mutation.table))
            })
            .map(|mutation| mutation.table.clone())
            .collect::<HashSet<_>>();
        let ever_synced_tables = load_ever_synced_tables(client, &historically_synced_table_ids);
        let has_write_protect = check_write_protect_exists(client);
        let evaluation_context = EvaluationContext {
            submitted_schema: &request.schema,
            authored_tables: &authored_tables,
            current_tables,
            registry: &registry,
            ever_synced_tables: &ever_synced_tables,
            has_write_protect,
            mutation_is_push_unit: request.atomic != Some(true),
        };

        require_no_pending_deferred_trigger_events(client);
        // This claim and every later source write remain in the same SPI transaction.
        claim_batch_ledger(client, p_user_id, &request, &fingerprints)
            .unwrap_or_else(|_| pgrx::error!("claiming push batch ledger failed"));

        let server_time = canonical_server_time();
        let evaluated = if request.atomic == Some(true) {
            evaluate_atomic_group(client, p_user_id, &request, &evaluation_context)
        } else {
            let mut evaluated = Vec::with_capacity(request.mutations.len());
            for mutation in &request.mutations {
                if let Some(stored) = stored_mutations.get(&mutation.mutation_id) {
                    evaluated.push(replayed_mutation(mutation, stored));
                    continue;
                }

                evaluated.push(evaluate_mutation(
                    client,
                    p_user_id,
                    mutation,
                    &evaluation_context,
                ));
            }
            evaluated
        };
        let accepted_write = evaluated.iter().any(|evaluation| evaluation.new_write);

        if accepted_write {
            increment_accepted_write_epoch(client, p_user_id, &request.client_id);
        }

        let response = build_push_response(&request, server_time, &evaluated);
        let response_bytes = canonical_push_response_bytes(&response)
            .unwrap_or_else(|_| pgrx::error!("canonicalizing push response failed"));

        let mut mutation_ledgers = Vec::new();
        for (ordinal, evaluation) in evaluated.iter().enumerate() {
            if stored_mutations.contains_key(&evaluation.mutation.mutation_id) {
                continue;
            }
            let outcome_bytes = canonical_json_bytes(&evaluation.outcome)
                .unwrap_or_else(|_| pgrx::error!("canonicalizing push outcome failed"));
            mutation_ledgers.push(MutationLedgerInsert {
                mutation: evaluation.mutation.clone(),
                outcome: evaluation.outcome.clone(),
                outcome_bytes,
                outcome_schema: evaluation.outcome_schema.clone(),
                table_id: evaluation.table_id.clone(),
                primary_key_field_id: evaluation.primary_key_field_id.clone(),
                primary_key_type: evaluation.primary_key_type.clone(),
                primary_key_value: evaluation.primary_key_value.clone(),
                row_identity: evaluation.row_identity.clone(),
                ordinal: i32::try_from(ordinal + 1)
                    .unwrap_or_else(|_| pgrx::error!("push mutation ordinal overflow")),
            });
        }
        insert_mutation_ledgers(
            client,
            p_user_id,
            &request,
            &mutation_ledgers,
            &fingerprints.mutations,
        )
        .unwrap_or_else(|_| pgrx::error!("writing push mutation ledger failed"));

        complete_batch_ledger(client, p_user_id, &request, server_time, &response_bytes)
            .unwrap_or_else(|_| pgrx::error!("writing completed push batch ledger failed"));
        String::from_utf8(response_bytes)
            .unwrap_or_else(|_| pgrx::error!("encoding push response failed"))
    })
}

fn compute_fingerprints(
    user_id: &str,
    request: &PushRequest,
    sealed_request: Vec<u8>,
) -> Result<Fingerprints, String> {
    let batch = batch_fingerprint(user_id, request)
        .map_err(|error| error.to_string())?
        .as_bytes()
        .to_vec();
    let mut mutations = Vec::with_capacity(request.mutations.len());
    for mutation in &request.mutations {
        // Calling the core normalizer explicitly also enforces its per-mutation byte bound.
        normalized_mutation(mutation).map_err(|error| error.to_string())?;
        mutations.push(
            mutation_fingerprint(user_id, &request.client_id, mutation)
                .map_err(|error| error.to_string())?
                .as_bytes()
                .to_vec(),
        );
    }
    Ok(Fingerprints {
        batch,
        mutations,
        sealed_request,
    })
}

fn canonical_json_bytes<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, String> {
    serde_json_canonicalizer::to_vec(value).map_err(|error| error.to_string())
}

fn canonical_server_time() -> chrono::DateTime<Utc> {
    let now = Utc::now();
    now.with_nanosecond((now.nanosecond() / 1_000) * 1_000)
        .expect("truncated nanoseconds remain valid")
}

fn canonical_push_response_bytes(response: &PushResponse) -> Result<Vec<u8>, String> {
    let mut value = serde_json::to_value(response).map_err(|error| error.to_string())?;
    value["server_time"] = serde_json::Value::String(
        response
            .server_time
            .to_rfc3339_opts(SecondsFormat::Micros, true),
    );
    canonical_json_bytes(&value)
}

fn set_push_context(client: &mut SpiClient<'_>, user_id: &str, client_id: &str) {
    client
        .update(
            "SELECT set_config('app.user_id', $1, true),
                    set_config('synchro.user_id', $1, true),
                    set_config('synchro.client_id', $2, true),
                    set_config('synchro.mutation_id', '', true)",
            None,
            &[user_id.into(), client_id.into()],
        )
        .unwrap_or_else(|_| pgrx::error!("setting push context failed"));
}

fn set_push_mutation_id(client: &mut SpiClient<'_>, mutation_id: &str) {
    client
        .update(
            "SELECT set_config('synchro.mutation_id', $1, true)",
            None,
            &[mutation_id.into()],
        )
        .unwrap_or_else(|_| pgrx::error!("setting push mutation context failed"));
}

fn clear_push_mutation_id(client: &mut SpiClient<'_>) {
    client
        .update(
            "SELECT set_config('synchro.mutation_id', '', true)",
            None,
            &[],
        )
        .unwrap_or_else(|_| pgrx::error!("clearing push mutation context failed"));
}

fn lock_push_identity(client: &mut SpiClient<'_>, identity: &str) {
    client
        .update(
            "SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))",
            None,
            &[identity.into()],
        )
        .unwrap_or_else(|_| pgrx::error!("locking push identity failed"));
}

fn client_is_retired(client: &SpiClient<'_>, user_id: &str, client_id: &str) -> bool {
    client
        .select(
            "SELECT EXISTS (
                 SELECT 1 FROM sync_client_retirements
                 WHERE user_id = $1 AND client_id = $2
             ) AS retired",
            None,
            &[user_id.into(), client_id.into()],
        )
        .unwrap_or_else(|_| pgrx::error!("checking client retirement failed"))
        .first()
        .get_one::<bool>()
        .unwrap_or_else(|_| pgrx::error!("reading client retirement failed"))
        .unwrap_or(false)
}

fn scoped_client_exists(client: &SpiClient<'_>, user_id: &str, client_id: &str) -> bool {
    client
        .select(
            "SELECT EXISTS (
                 SELECT 1 FROM sync_clients
                 WHERE user_id = $1 AND client_id = $2
             ) AS scoped",
            None,
            &[user_id.into(), client_id.into()],
        )
        .unwrap_or_else(|_| pgrx::error!("checking scoped client identity failed"))
        .first()
        .get_one::<bool>()
        .unwrap_or_else(|_| pgrx::error!("reading scoped client identity failed"))
        .unwrap_or(false)
}

enum GenerationGate {
    NotRegistered,
    Expired(i64),
    Operational,
}

fn check_client_generation(
    client: &SpiClient<'_>,
    user_id: &str,
    client_id: &str,
    presented: i64,
) -> Result<i64, GenerationGate> {
    let rows = client
        .select(
            "SELECT client_generation,
                    is_active,
                    generation_expires_at IS NULL OR generation_expires_at > now() AS unexpired
             FROM sync_clients
             WHERE user_id = $1 AND client_id = $2
             FOR UPDATE",
            None,
            &[user_id.into(), client_id.into()],
        )
        .map_err(|_| GenerationGate::Operational)?;
    let Some(row) = rows.into_iter().next() else {
        return Err(GenerationGate::NotRegistered);
    };
    let current = row
        .get_by_name::<i64, &str>("client_generation")
        .map_err(|_| GenerationGate::Operational)?
        .ok_or(GenerationGate::Operational)?;
    let active = row
        .get_by_name::<bool, &str>("is_active")
        .map_err(|_| GenerationGate::Operational)?
        .unwrap_or(false);
    let unexpired = row
        .get_by_name::<bool, &str>("unexpired")
        .map_err(|_| GenerationGate::Operational)?
        .unwrap_or(false);
    if current != presented || !active || !unexpired {
        return Err(GenerationGate::Expired(current));
    }
    Ok(current)
}

fn push_protocol_error(
    code: ProtocolErrorCode,
    message: impl Into<String>,
    retryable: bool,
) -> String {
    let error = ErrorBody {
        code,
        message: message.into(),
        retryable,
        current_schema: None,
        received_schema: None,
        current_client_generation: None,
        scope_id: None,
        required_protocol_version: None,
        received_protocol_version: None,
        minimum_client_version: None,
        received_client_version: None,
        reason: None,
        field: None,
        minimum: None,
        maximum: None,
    };
    encode_error_response(error)
}

fn push_error(
    code: ProtocolErrorCode,
    message: &str,
    retryable: bool,
    current_generation: Option<i64>,
    schemas: Option<(SchemaRef, SchemaRef)>,
) -> String {
    let (current_schema, received_schema) = schemas
        .map(|(current, received)| (Some(current), Some(received)))
        .unwrap_or((None, None));
    let error = ErrorBody {
        code,
        message: message.to_string(),
        retryable,
        current_schema,
        received_schema,
        current_client_generation: current_generation,
        scope_id: None,
        required_protocol_version: None,
        received_protocol_version: None,
        minimum_client_version: None,
        received_client_version: None,
        reason: None,
        field: None,
        minimum: None,
        maximum: None,
    };
    if error.validate().is_err() {
        pgrx::error!("invalid push error response")
    }
    encode_error_response(error)
}

fn encode_error_response(error: ErrorBody) -> String {
    let bytes = canonical_json_bytes(&ErrorResponse { error })
        .unwrap_or_else(|_| pgrx::error!("encoding push error response failed"));
    String::from_utf8(bytes).unwrap_or_else(|_| pgrx::error!("encoding push error response failed"))
}

fn json_bytes_response(bytes: &[u8]) -> String {
    serde_json::from_slice::<serde_json::Value>(bytes)
        .unwrap_or_else(|_| pgrx::error!("stored push response is not valid JSON"));
    String::from_utf8(bytes.to_vec())
        .unwrap_or_else(|_| pgrx::error!("stored push response is not valid UTF-8"))
}

fn load_batch_ledger(
    client: &SpiClient<'_>,
    user_id: &str,
    client_id: &str,
    batch_id: &str,
) -> Result<Option<StoredBatch>, spi::Error> {
    let rows = client.select(
        "SELECT fingerprint_algorithm, fingerprint_version, fingerprint_domain,
                fingerprint_digest, execution_state, http_status, sealed_canonical_response
         FROM sync_push_batches
         WHERE user_id = $1 AND client_id = $2 AND batch_id = $3::uuid",
        None,
        &[user_id.into(), client_id.into(), batch_id.into()],
    )?;
    let Some(row) = rows.into_iter().next() else {
        return Ok(None);
    };
    Ok(Some(StoredBatch {
        algorithm: row
            .get_by_name("fingerprint_algorithm")?
            .unwrap_or_default(),
        version: row.get_by_name("fingerprint_version")?.unwrap_or_default(),
        domain: row.get_by_name("fingerprint_domain")?.unwrap_or_default(),
        digest: row.get_by_name("fingerprint_digest")?.unwrap_or_default(),
        state: row.get_by_name("execution_state")?.unwrap_or_default(),
        http_status: row.get_by_name("http_status")?,
        response: row.get_by_name("sealed_canonical_response")?,
    }))
}

fn load_mutation_ledgers(
    client: &SpiClient<'_>,
    user_id: &str,
    client_id: &str,
    mutation_ids: &[String],
) -> Result<HashMap<String, StoredMutation>, spi::Error> {
    let rows = client.select(
        "SELECT mutation_id::text AS mutation_id,
                fingerprint_algorithm, fingerprint_version, fingerprint_domain,
                fingerprint_digest, first_batch_id::text AS first_batch_id,
                request_ordinal, outcome_schema_version, outcome_schema_hash,
                sealed_canonical_response
         FROM sync_push_mutations
         WHERE user_id = $1 AND client_id = $2 AND mutation_id = ANY($3::uuid[])",
        None,
        &[
            user_id.into(),
            client_id.into(),
            mutation_ids.to_vec().into(),
        ],
    )?;
    let mut result = HashMap::with_capacity(rows.len());
    for row in rows {
        let mutation_id: String = row.get_by_name("mutation_id")?.unwrap_or_default();
        let outcome_bytes: Vec<u8> = row
            .get_by_name("sealed_canonical_response")?
            .unwrap_or_else(|| pgrx::error!("push mutation ledger has no outcome"));
        let outcome = serde_json::from_slice(&outcome_bytes)
            .unwrap_or_else(|_| pgrx::error!("push mutation ledger outcome is not JSON"));
        result.insert(
            mutation_id,
            StoredMutation {
                algorithm: row
                    .get_by_name("fingerprint_algorithm")?
                    .unwrap_or_default(),
                version: row.get_by_name("fingerprint_version")?.unwrap_or_default(),
                domain: row.get_by_name("fingerprint_domain")?.unwrap_or_default(),
                digest: row.get_by_name("fingerprint_digest")?.unwrap_or_default(),
                outcome_schema: SchemaRef {
                    version: row
                        .get_by_name("outcome_schema_version")?
                        .unwrap_or_default(),
                    hash: row.get_by_name("outcome_schema_hash")?.unwrap_or_default(),
                },
                outcome,
            },
        );
    }
    Ok(result)
}

fn claim_batch_ledger(
    client: &mut SpiClient<'_>,
    user_id: &str,
    request: &PushRequest,
    fingerprints: &Fingerprints,
) -> Result<(), spi::Error> {
    client.update(
        "INSERT INTO sync_push_batches (
             user_id, client_id, batch_id, protocol_version, client_generation,
             request_schema_version, request_schema_hash,
             fingerprint_algorithm, fingerprint_version, fingerprint_domain,
             fingerprint_digest, sealed_canonical_request, execution_state,
             created_at
         ) VALUES (
             $1, $2, $3::uuid, $4, $5, $6, $7,
             $8, $9, $10, $11, $12, 'executing', now()
         )",
        None,
        &[
            user_id.into(),
            request.client_id.as_str().into(),
            request.batch_id.as_str().into(),
            i32::try_from(PROTOCOL_VERSION).unwrap_or(3).into(),
            request.client_generation.into(),
            request.schema.version.into(),
            request.schema.hash.as_str().into(),
            FINGERPRINT_ALGORITHM.into(),
            FINGERPRINT_VERSION.into(),
            BATCH_FINGERPRINT_DOMAIN.into(),
            fingerprints.batch.clone().into(),
            fingerprints.sealed_request.clone().into(),
        ],
    )?;
    Ok(())
}

fn complete_batch_ledger(
    client: &mut SpiClient<'_>,
    user_id: &str,
    request: &PushRequest,
    server_time: chrono::DateTime<Utc>,
    response: &[u8],
) -> Result<(), spi::Error> {
    let server_time = server_time.to_rfc3339_opts(SecondsFormat::Micros, true);
    let rows = client.update(
        "UPDATE sync_push_batches
         SET execution_state = 'completed',
             http_status = 200,
             sealed_canonical_response = $4,
             server_time = $5::timestamptz,
             completed_at = $5::timestamptz
         WHERE user_id = $1 AND client_id = $2 AND batch_id = $3::uuid
           AND execution_state = 'executing'
         RETURNING batch_id",
        None,
        &[
            user_id.into(),
            request.client_id.as_str().into(),
            request.batch_id.as_str().into(),
            response.to_vec().into(),
            server_time.as_str().into(),
        ],
    )?;
    if rows.is_empty() {
        pgrx::error!("completing push batch ledger affected no row")
    }
    Ok(())
}

fn insert_mutation_ledgers(
    client: &mut SpiClient<'_>,
    user_id: &str,
    request: &PushRequest,
    ledgers: &[MutationLedgerInsert],
    fingerprints: &[Vec<u8>],
) -> Result<(), spi::Error> {
    if ledgers.is_empty() {
        return Ok(());
    }
    let rows = ledgers
        .iter()
        .map(|ledger| {
            let fingerprint = usize::try_from(ledger.ordinal - 1)
                .ok()
                .and_then(|index| fingerprints.get(index))
                .unwrap_or_else(|| pgrx::error!("push mutation fingerprint is missing"));
            let status = ledger
                .outcome
                .get("status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_else(|| pgrx::error!("push outcome has no status"));
            let code = ledger
                .outcome
                .get("code")
                .and_then(serde_json::Value::as_str);
            let sealed_request = String::from_utf8(
                canonical_json_bytes(&ledger.mutation)
                    .unwrap_or_else(|_| pgrx::error!("canonicalizing mutation request failed")),
            )
            .unwrap_or_else(|_| pgrx::error!("canonical mutation request is not UTF-8"));
            let sealed_response = String::from_utf8(ledger.outcome_bytes.clone())
                .unwrap_or_else(|_| pgrx::error!("canonical mutation outcome is not UTF-8"));
            serde_json::json!({
                "mutation_id": ledger.mutation.mutation_id,
                "fingerprint_digest": base64::engine::general_purpose::STANDARD.encode(fingerprint),
                "request_ordinal": ledger.ordinal,
                "authored_schema_version": ledger.mutation.authored_schema.version,
                "authored_schema_hash": ledger.mutation.authored_schema.hash,
                "outcome_schema_version": ledger.outcome_schema.version,
                "outcome_schema_hash": ledger.outcome_schema.hash,
                "table_id": ledger.table_id,
                "primary_key_field_id": ledger.primary_key_field_id,
                "primary_key_type": ledger.primary_key_type,
                "primary_key_value": ledger.primary_key_value,
                "row_identity": ledger.row_identity.as_ref().map(|identity| {
                    base64::engine::general_purpose::STANDARD.encode(identity)
                }),
                "operation": operation_name(ledger.mutation.op),
                "outcome_status": status,
                "rejection_code": code,
                "sealed_canonical_request": sealed_request,
                "sealed_canonical_response": sealed_response,
            })
        })
        .collect::<Vec<_>>();
    let inserted = client.update(
        "INSERT INTO sync_push_mutations (
             user_id, client_id, mutation_id, fingerprint_algorithm,
             fingerprint_version, fingerprint_domain, fingerprint_digest,
             first_batch_id, request_ordinal, authored_schema_version,
             authored_schema_hash, submitted_schema_version, submitted_schema_hash,
             outcome_schema_version, outcome_schema_hash, table_id,
             primary_key_field_id, primary_key_type, primary_key_value,
             row_identity, operation, outcome_status, rejection_code,
              sealed_canonical_request, sealed_canonical_response,
              created_at, completed_at
          )
          SELECT $1, $2, ledger.mutation_id::uuid, $4, $5, $6,
                 decode(ledger.fingerprint_digest, 'base64'), $3::uuid,
                 ledger.request_ordinal, ledger.authored_schema_version,
                 ledger.authored_schema_hash, $7, $8,
                 ledger.outcome_schema_version, ledger.outcome_schema_hash,
                 ledger.table_id, ledger.primary_key_field_id,
                 ledger.primary_key_type, ledger.primary_key_value,
                 CASE WHEN ledger.row_identity IS NULL THEN NULL
                      ELSE decode(ledger.row_identity, 'base64') END,
                 ledger.operation, ledger.outcome_status, ledger.rejection_code,
                 convert_to(ledger.sealed_canonical_request, 'UTF8'),
                 convert_to(ledger.sealed_canonical_response, 'UTF8'),
                 now(), now()
          FROM jsonb_to_recordset($9::jsonb) AS ledger(
              mutation_id text, fingerprint_digest text, request_ordinal integer,
              authored_schema_version bigint, authored_schema_hash text,
              outcome_schema_version bigint, outcome_schema_hash text,
              table_id text, primary_key_field_id text, primary_key_type text,
              primary_key_value jsonb, row_identity text, operation text,
              outcome_status text, rejection_code text,
              sealed_canonical_request text, sealed_canonical_response text
          )
          ORDER BY ledger.request_ordinal
          RETURNING mutation_id",
        None,
        &[
            user_id.into(),
            request.client_id.as_str().into(),
            request.batch_id.as_str().into(),
            FINGERPRINT_ALGORITHM.into(),
            FINGERPRINT_VERSION.into(),
            MUTATION_FINGERPRINT_DOMAIN.into(),
            request.schema.version.into(),
            request.schema.hash.as_str().into(),
            pgrx::JsonB(serde_json::Value::Array(rows)).into(),
        ],
    )?;
    if inserted.len() != ledgers.len() {
        pgrx::error!("writing push mutation ledger affected an unexpected row count")
    }
    Ok(())
}

fn operation_name(operation: Operation) -> &'static str {
    match operation {
        Operation::Insert => "insert",
        Operation::Update => "update",
        Operation::Delete => "delete",
        Operation::Upsert => pgrx::error!("push upsert passed contract validation"),
    }
}

pub(crate) fn load_authored_manifests(
    client: &SpiClient<'_>,
    mutations: &[Mutation],
) -> HashMap<SchemaRef, SchemaManifest> {
    let references = mutations
        .iter()
        .map(|mutation| {
            serde_json::json!({
                "schema_version": mutation.authored_schema.version,
                "schema_hash": mutation.authored_schema.hash,
            })
        })
        .collect::<Vec<_>>();
    let rows = client
        .select(
            "WITH authored AS (
                 SELECT DISTINCT schema_version, schema_hash
                 FROM jsonb_to_recordset($1::jsonb) AS reference(
                     schema_version bigint,
                     schema_hash text
                 )
             )
             SELECT manifest.schema_version, manifest.schema_hash,
                    manifest.canonical_manifest_body
             FROM sync_schema_manifest manifest
             JOIN authored
               ON authored.schema_version = manifest.schema_version
              AND authored.schema_hash = manifest.schema_hash
             ORDER BY manifest.schema_version",
            None,
            &[pgrx::JsonB(serde_json::Value::Array(references)).into()],
        )
        .unwrap_or_else(|_| pgrx::error!("loading schema manifest history failed"));
    let mut manifests = HashMap::with_capacity(rows.len());
    for row in rows {
        let version = row
            .get_by_name::<i64, &str>("schema_version")
            .unwrap_or_else(|_| pgrx::error!("reading schema manifest version failed"))
            .unwrap_or_else(|| pgrx::error!("schema manifest version is missing"));
        let hash = row
            .get_by_name::<String, &str>("schema_hash")
            .unwrap_or_else(|_| pgrx::error!("reading schema manifest hash failed"))
            .unwrap_or_else(|| pgrx::error!("schema manifest hash is missing"));
        let body = row
            .get_by_name::<String, &str>("canonical_manifest_body")
            .unwrap_or_else(|_| pgrx::error!("reading schema manifest body failed"))
            .unwrap_or_else(|| pgrx::error!("schema manifest body is missing"));
        let mut value: serde_json::Value = serde_json::from_str(&body)
            .unwrap_or_else(|_| pgrx::error!("stored schema manifest is not valid JSON"));
        value["schema_hash"] = serde_json::Value::String(hash.clone());
        let manifest: SchemaManifest = serde_json::from_value(value)
            .unwrap_or_else(|_| pgrx::error!("stored schema manifest is invalid"));
        if manifest.schema_version != version || manifest.schema_hash != hash {
            pgrx::error!("stored schema manifest identity is inconsistent");
        }
        if manifest.validate().is_err() {
            pgrx::error!("stored schema manifest violates the contract");
        }
        manifests.insert(SchemaRef { version, hash }, manifest);
    }
    manifests
}

fn load_ever_synced_tables(client: &SpiClient<'_>, table_ids: &HashSet<String>) -> HashSet<String> {
    // A value that is not a canonical UUID cannot match a logical ID.
    let table_ids = table_ids
        .iter()
        .filter(|table_id| is_lower_uuid(table_id))
        .cloned()
        .collect::<Vec<_>>();
    if table_ids.is_empty() {
        return HashSet::new();
    }
    let rows = client
        .select(
            "SELECT logical_id::text AS table_id
             FROM synchro.sync_logical_ids
            WHERE kind = 'table'
               AND logical_id = ANY($1::uuid[])",
            None,
            &[table_ids.into()],
        )
        .unwrap_or_else(|_| pgrx::error!("loading push table identities failed"));
    rows.into_iter()
        .map(|row| {
            row.get_by_name::<String, &str>("table_id")
                .unwrap_or_else(|_| pgrx::error!("reading push table identity failed"))
                .unwrap_or_else(|| pgrx::error!("push table identity is missing"))
        })
        .collect()
}

fn all_mutation_field_ids(mutation: &Mutation) -> Vec<String> {
    let mut fields = mutation
        .pk
        .as_object()
        .into_iter()
        .flat_map(|object| object.keys().cloned())
        .collect::<Vec<_>>();
    if let Some(columns) = mutation
        .columns
        .as_ref()
        .and_then(serde_json::Value::as_object)
    {
        fields.extend(columns.keys().cloned());
    }
    fields.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    fields.dedup();
    fields
}

fn fields_for_schema_incompatibility(
    mutation: &Mutation,
    authored_table: Option<&TableSchema>,
    current_table: Option<&TableSchema>,
) -> Vec<String> {
    let references = all_mutation_field_ids(mutation);
    let Some(authored_table) = authored_table else {
        return references;
    };
    let Some(current_table) = current_table else {
        return references;
    };
    let mut incompatible = HashSet::new();
    if authored_table.relation_id != current_table.relation_id
        || authored_table.composition != current_table.composition
    {
        incompatible.insert(authored_table.primary_key_field_id.clone());
    }
    if let Some(primary_key_field_id) = mutation
        .pk
        .as_object()
        .and_then(|primary_key| primary_key.keys().next())
    {
        if primary_key_field_id != &authored_table.primary_key_field_id
            || primary_key_field_id != &current_table.primary_key_field_id
        {
            incompatible.insert(primary_key_field_id.clone());
        }
    }
    let authored_fields: HashMap<&str, _> = authored_table
        .fields
        .iter()
        .map(|field| (field.field_id.as_str(), field))
        .collect();
    let current_fields: HashMap<&str, _> = current_table
        .fields
        .iter()
        .map(|field| (field.field_id.as_str(), field))
        .collect();
    for field_id in references {
        let authored = authored_fields.get(field_id.as_str());
        let current = current_fields.get(field_id.as_str());
        if authored.is_none()
            || current.is_none()
            || !fields_compatible(authored.unwrap(), current.unwrap())
        {
            incompatible.insert(field_id);
        }
    }
    let mut result = incompatible.into_iter().collect::<Vec<_>>();
    result.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    result
}

fn fields_compatible(
    authored: &synchro_core::contract::ColumnSchema,
    current: &synchro_core::contract::ColumnSchema,
) -> bool {
    authored.type_name == current.type_name
        && (!authored.nullable || current.nullable)
        && (!authored.writable || current.writable)
        && (authored.type_name != "decimal" || authored.decimal_domain_within(current))
}

fn mutation_primary_key(mutation: &Mutation) -> (String, serde_json::Value) {
    let Some((field_id, value)) = mutation
        .pk
        .as_object()
        .and_then(|object| object.iter().next())
    else {
        return (String::new(), serde_json::Value::Null);
    };
    (field_id.clone(), value.clone())
}

fn evaluate_mutation(
    client: &mut SpiClient<'_>,
    user_id: &str,
    mutation: &Mutation,
    context: &EvaluationContext<'_>,
) -> EvaluatedMutation {
    let submitted_schema = context.submitted_schema;
    let authored_tables = context.authored_tables;
    let current_tables = context.current_tables;
    let registry = context.registry;
    let ever_synced_tables = context.ever_synced_tables;
    let has_write_protect = context.has_write_protect;
    let mutation_is_push_unit = context.mutation_is_push_unit;
    let (pk_field_id, pk_value) = mutation_primary_key(mutation);
    let outcome_schema = submitted_schema.clone();
    let authored_tables = authored_tables.get(&mutation.authored_schema);
    let authored_table = authored_tables.and_then(|tables| tables.get(&mutation.table).copied());
    let current_table = current_tables.get(&mutation.table).copied();
    let table_reg = registry.get(&mutation.table);

    if authored_tables.is_none() {
        return terminal_evaluation(
            mutation,
            outcome_schema,
            "schema_incompatible",
            "authored mutation cannot be represented by the current schema",
            unresolved_target(mutation, &pk_field_id, &pk_value),
            Some(SchemaIncompatibility {
                authored_schema: mutation.authored_schema.clone(),
                current_schema: submitted_schema.clone(),
                field_ids: all_mutation_field_ids(mutation),
            }),
        );
    }

    if authored_table.is_none() {
        if ever_synced_tables.contains(&mutation.table) {
            return terminal_evaluation(
                mutation,
                outcome_schema,
                "schema_incompatible",
                "authored mutation cannot be represented by the current schema",
                unresolved_target(mutation, &pk_field_id, &pk_value),
                Some(SchemaIncompatibility {
                    authored_schema: mutation.authored_schema.clone(),
                    current_schema: submitted_schema.clone(),
                    field_ids: all_mutation_field_ids(mutation),
                }),
            );
        }
        return terminal_evaluation(
            mutation,
            outcome_schema,
            "table_not_synced",
            "target table is not registered for synchronization",
            unresolved_target(mutation, &pk_field_id, &pk_value),
            None,
        );
    }

    let incompatible_fields =
        fields_for_schema_incompatibility(mutation, authored_table, current_table);
    if current_table.is_none() || !incompatible_fields.is_empty() {
        return terminal_evaluation(
            mutation,
            outcome_schema,
            "schema_incompatible",
            "authored mutation cannot be represented by the current schema",
            unresolved_target(mutation, &pk_field_id, &pk_value),
            Some(SchemaIncompatibility {
                authored_schema: mutation.authored_schema.clone(),
                current_schema: submitted_schema.clone(),
                field_ids: incompatible_fields,
            }),
        );
    }

    let authored_table =
        authored_table.unwrap_or_else(|| pgrx::error!("validated authored table is missing"));
    let current_table =
        current_table.unwrap_or_else(|| pgrx::error!("validated current table is missing"));
    let table_reg =
        table_reg.unwrap_or_else(|| pgrx::error!("active logical table registry is missing"));
    if table_reg.table_id != current_table.table_id
        || table_reg.relation_id != current_table.relation_id
        || table_reg.primary_key_field_id != current_table.primary_key_field_id
    {
        pgrx::error!("active registry and schema manifest are inconsistent")
    }
    if table_reg.push_policy == PushPolicy::ReadOnly {
        return policy_evaluation(
            mutation,
            outcome_schema,
            registered_target(table_reg, &pk_field_id, &pk_value, None),
        );
    }

    let authored_columns = mutation
        .columns
        .as_ref()
        .and_then(serde_json::Value::as_object)
        .cloned()
        .unwrap_or_default();
    let policy_value = if has_write_protect {
        let Some(value) = call_write_protect_or_error(
            client,
            user_id,
            &mutation.table,
            operation_name(mutation.op),
            &serde_json::Value::Object(authored_columns.clone()),
        ) else {
            return policy_evaluation(
                mutation,
                outcome_schema,
                registered_target(table_reg, &pk_field_id, &pk_value, None),
            );
        };
        value
    } else {
        serde_json::Value::Object(authored_columns.clone())
    };

    if !authored_columns_are_valid(table_reg, &authored_columns) {
        return validation_evaluation(
            mutation,
            outcome_schema,
            table_reg,
            &pk_field_id,
            &pk_value,
            None,
            "mutation field failed validation",
        );
    }
    let policy_columns = match validate_policy_columns(
        mutation.op,
        policy_value,
        authored_table,
        current_table,
        table_reg,
    ) {
        Ok(columns) => columns,
        Err(()) => {
            return validation_evaluation(
                mutation,
                outcome_schema,
                table_reg,
                &pk_field_id,
                &pk_value,
                None,
                "write policy result failed validation",
            )
        }
    };

    let pk_field = table_reg
        .fields
        .iter()
        .find(|field| field.field_id == table_reg.primary_key_field_id)
        .unwrap_or_else(|| pgrx::error!("registered primary-key field is missing"));
    if validate_wire_value(pk_field, &pk_value).is_err() {
        return validation_evaluation(
            mutation,
            outcome_schema,
            table_reg,
            &pk_field_id,
            &pk_value,
            None,
            "mutation primary key failed validation",
        );
    }

    let wire_record_id = match wire_record_id(table_reg, &pk_value) {
        Ok(value) => value,
        Err(_) => {
            return validation_evaluation(
                mutation,
                outcome_schema,
                table_reg,
                &pk_field_id,
                &pk_value,
                None,
                "mutation primary key failed validation",
            )
        }
    };
    let Some(record_id) = canonicalize_record_id(client, &wire_record_id, table_reg) else {
        return validation_evaluation(
            mutation,
            outcome_schema,
            table_reg,
            &pk_field_id,
            &pk_value,
            None,
            "mutation primary key failed validation",
        );
    };

    let dml_data = if matches!(mutation.op, Operation::Insert | Operation::Update) {
        build_dml_data(table_reg, &policy_columns)
    } else {
        serde_json::Value::Object(serde_json::Map::new())
    };
    let row_identity = logical_row_identity(table_reg, &pk_value);
    let existing = load_existing_record(
        client,
        &record_id,
        table_reg,
        match mutation.op {
            Operation::Insert => SourceStatement::NoRowWrite,
            Operation::Delete if !table_reg.has_deleted_at => SourceStatement::Delete,
            Operation::Update | Operation::Delete => {
                let mut assigned = if mutation.op == Operation::Update {
                    dml_data
                        .as_object()
                        .unwrap_or_else(|| pgrx::error!("push update payload is not an object"))
                        .keys()
                        .cloned()
                        .collect()
                } else {
                    vec![table_reg.deleted_at_col.clone()]
                };
                if table_reg.has_updated_at {
                    assigned.push(table_reg.updated_at_col.clone());
                }
                SourceStatement::Update(assigned)
            }
            Operation::Upsert => pgrx::error!("push upsert passed contract validation"),
        },
    );

    if existing.as_ref().is_some_and(|row| row.deleted) {
        return conflict_evaluation(
            mutation,
            outcome_schema,
            "row_deleted",
            "the row has been deleted",
            client,
            ConflictTarget {
                existing: existing.as_ref(),
                table: table_reg,
                record_id: &record_id,
                row_identity,
            },
        );
    }
    if existing
        .as_ref()
        .is_some_and(RowState::hidden_by_row_security)
    {
        return policy_evaluation(
            mutation,
            outcome_schema,
            registered_target(table_reg, &pk_field_id, &pk_value, row_identity),
        );
    }

    match mutation.op {
        Operation::Insert => {
            if let Some(existing) = existing.as_ref() {
                return conflict_evaluation(
                    mutation,
                    outcome_schema,
                    "row_already_exists",
                    "the row already exists",
                    client,
                    ConflictTarget {
                        existing: Some(existing),
                        table: table_reg,
                        record_id: &record_id,
                        row_identity,
                    },
                );
            }
            match push_insert(
                client,
                &mutation.mutation_id,
                &record_id,
                table_reg,
                &dml_data,
                mutation_is_push_unit,
            ) {
                DmlOutcome::ValidationFailed => validation_evaluation(
                    mutation,
                    outcome_schema,
                    table_reg,
                    &pk_field_id,
                    &pk_value,
                    row_identity,
                    "mutation failed physical validation",
                ),
                DmlOutcome::PolicyRejected => policy_evaluation(
                    mutation,
                    outcome_schema,
                    registered_target(table_reg, &pk_field_id, &pk_value, row_identity),
                ),
                DmlOutcome::NotApplied => {
                    // The insert found no conflicting row that the caller can see, so a BEFORE
                    // trigger or row security denied the write.
                    let current = load_existing_record(
                        client,
                        &record_id,
                        table_reg,
                        SourceStatement::NoRowWrite,
                    )
                    .filter(|current| !current.hidden_by_row_security());
                    let Some(current) = current else {
                        return policy_evaluation(
                            mutation,
                            outcome_schema,
                            registered_target(table_reg, &pk_field_id, &pk_value, row_identity),
                        );
                    };
                    let code = if current.deleted {
                        "row_deleted"
                    } else {
                        "row_already_exists"
                    };
                    conflict_evaluation(
                        mutation,
                        outcome_schema,
                        code,
                        "the row already exists",
                        client,
                        ConflictTarget {
                            existing: Some(&current),
                            table: table_reg,
                            record_id: &record_id,
                            row_identity,
                        },
                    )
                }
                DmlOutcome::Applied => accepted_evaluation(
                    client,
                    mutation,
                    outcome_schema,
                    table_reg,
                    &record_id,
                    row_identity,
                ),
            }
        }
        Operation::Update | Operation::Delete => {
            let Some(existing) = existing.as_ref() else {
                return conflict_evaluation(
                    mutation,
                    outcome_schema,
                    "row_not_found",
                    "the row does not exist",
                    client,
                    ConflictTarget {
                        existing: None,
                        table: table_reg,
                        record_id: &record_id,
                        row_identity,
                    },
                );
            };
            let current_version = existing
                .row_version
                .as_deref()
                .unwrap_or_else(|| pgrx::error!("authoritative row has no server version"));
            if mutation.base_version.as_deref() != Some(current_version) {
                return conflict_evaluation(
                    mutation,
                    outcome_schema,
                    "version_conflict",
                    "the base version does not match the current row",
                    client,
                    ConflictTarget {
                        existing: Some(existing),
                        table: table_reg,
                        record_id: &record_id,
                        row_identity,
                    },
                );
            }
            let dml_outcome = if mutation.op == Operation::Update {
                push_update(
                    client,
                    &mutation.mutation_id,
                    &record_id,
                    table_reg,
                    &dml_data,
                    mutation_is_push_unit,
                )
            } else if table_reg.has_deleted_at {
                push_soft_delete(
                    client,
                    &mutation.mutation_id,
                    &record_id,
                    table_reg,
                    mutation_is_push_unit,
                )
            } else {
                push_hard_delete(
                    client,
                    &mutation.mutation_id,
                    &record_id,
                    table_reg,
                    mutation_is_push_unit,
                )
            };
            match dml_outcome {
                DmlOutcome::ValidationFailed => validation_evaluation(
                    mutation,
                    outcome_schema,
                    table_reg,
                    &pk_field_id,
                    &pk_value,
                    row_identity,
                    "mutation failed physical validation",
                ),
                // Push holds the lock on the source row, so a zero-row write to that row is a
                // write policy denial.
                DmlOutcome::NotApplied | DmlOutcome::PolicyRejected => policy_evaluation(
                    mutation,
                    outcome_schema,
                    registered_target(table_reg, &pk_field_id, &pk_value, row_identity),
                ),
                DmlOutcome::Applied => accepted_evaluation(
                    client,
                    mutation,
                    outcome_schema,
                    table_reg,
                    &record_id,
                    row_identity,
                ),
            }
        }
        Operation::Upsert => pgrx::error!("push upsert passed contract validation"),
    }
}

fn authored_columns_are_valid(
    table_reg: &TableRegistration,
    columns: &serde_json::Map<String, serde_json::Value>,
) -> bool {
    columns.iter().all(|(field_id, value)| {
        table_reg
            .fields
            .iter()
            .find(|field| field.field_id == *field_id)
            .is_some_and(|field| {
                field.writable
                    && !field.primary_key
                    && !table_reg.exclude_columns.contains(&field.physical_column)
                    && validate_wire_value(field, value).is_ok()
            })
    })
}

fn validate_policy_columns(
    operation: Operation,
    value: serde_json::Value,
    authored_table: &TableSchema,
    current_table: &TableSchema,
    table_reg: &TableRegistration,
) -> Result<serde_json::Map<String, serde_json::Value>, ()> {
    let columns = value.as_object().ok_or(())?;
    match operation {
        Operation::Update if columns.is_empty() => return Err(()),
        Operation::Delete if !columns.is_empty() => return Err(()),
        Operation::Upsert => return Err(()),
        _ => {}
    }
    for (field_id, field_value) in columns {
        let authored_field = authored_table
            .fields
            .iter()
            .find(|field| field.field_id == *field_id)
            .ok_or(())?;
        let current_field = current_table
            .fields
            .iter()
            .find(|field| field.field_id == *field_id)
            .ok_or(())?;
        let registered_field = table_reg
            .fields
            .iter()
            .find(|field| field.field_id == *field_id)
            .ok_or(())?;
        if !authored_field.writable
            || !current_field.writable
            || !fields_compatible(authored_field, current_field)
            || !registered_field.writable
            || registered_field.primary_key
            || table_reg
                .exclude_columns
                .contains(&registered_field.physical_column)
            || validate_wire_value(registered_field, field_value).is_err()
        {
            return Err(());
        }
    }
    Ok(columns.clone())
}

fn validation_evaluation(
    mutation: &Mutation,
    outcome_schema: SchemaRef,
    table_reg: &TableRegistration,
    primary_key_field_id: &str,
    primary_key_value: &serde_json::Value,
    row_identity: Option<Vec<u8>>,
    message: &str,
) -> EvaluatedMutation {
    terminal_evaluation(
        mutation,
        outcome_schema,
        "validation_failed",
        message,
        registered_target(
            table_reg,
            primary_key_field_id,
            primary_key_value,
            row_identity,
        ),
        None,
    )
}

fn unresolved_target(
    mutation: &Mutation,
    primary_key_field_id: &str,
    primary_key_value: &serde_json::Value,
) -> EvaluationTarget {
    EvaluationTarget {
        table_id: mutation.table.clone(),
        primary_key_field_id: primary_key_field_id.to_string(),
        primary_key_type: "json".into(),
        primary_key_value: primary_key_value.clone(),
        row_identity: None,
    }
}

fn registered_target(
    table_reg: &TableRegistration,
    primary_key_field_id: &str,
    primary_key_value: &serde_json::Value,
    row_identity: Option<Vec<u8>>,
) -> EvaluationTarget {
    EvaluationTarget {
        table_id: table_reg.table_id.clone(),
        primary_key_field_id: primary_key_field_id.to_string(),
        primary_key_type: table_reg.pk_portable_type.clone(),
        primary_key_value: primary_key_value.clone(),
        row_identity,
    }
}

fn policy_evaluation(
    mutation: &Mutation,
    outcome_schema: SchemaRef,
    target: EvaluationTarget,
) -> EvaluatedMutation {
    terminal_evaluation(
        mutation,
        outcome_schema,
        "policy_rejected",
        "authenticated write policy rejected the mutation",
        target,
        None,
    )
}

fn terminal_evaluation(
    mutation: &Mutation,
    outcome_schema: SchemaRef,
    code: &str,
    message: &str,
    target: EvaluationTarget,
    schema_incompatibility: Option<SchemaIncompatibility>,
) -> EvaluatedMutation {
    let mut object = base_outcome(
        mutation,
        &outcome_schema,
        &target.table_id,
        &target.primary_key_field_id,
        &target.primary_key_value,
    );
    object["status"] = serde_json::Value::String("rejected_terminal".into());
    object["code"] = serde_json::Value::String(code.into());
    object["message"] = serde_json::Value::String(message.into());
    if code == "schema_incompatible" {
        let incompatibility = schema_incompatibility
            .unwrap_or_else(|| pgrx::error!("schema rejection has no incompatibility details"));
        object["retryable"] = serde_json::Value::Bool(false);
        object["authored_schema"] = serde_json::to_value(incompatibility.authored_schema).unwrap();
        object["current_schema"] = serde_json::to_value(incompatibility.current_schema).unwrap();
        object["incompatible_field_ids"] = serde_json::to_value(incompatibility.field_ids).unwrap();
    }
    EvaluatedMutation {
        mutation: mutation.clone(),
        outcome: object,
        accepted: false,
        new_write: false,
        outcome_schema: outcome_schema.clone(),
        table_id: target.table_id,
        primary_key_field_id: target.primary_key_field_id,
        primary_key_type: target.primary_key_type,
        primary_key_value: target.primary_key_value,
        row_identity: target.row_identity,
    }
}

fn base_outcome(
    mutation: &Mutation,
    outcome_schema: &SchemaRef,
    table_id: &str,
    primary_key_field_id: &str,
    primary_key_value: &serde_json::Value,
) -> serde_json::Value {
    let mut pk = serde_json::Map::new();
    pk.insert(primary_key_field_id.to_string(), primary_key_value.clone());
    serde_json::json!({
        "mutation_id": mutation.mutation_id,
        "table": table_id,
        "pk": pk,
        "outcome_schema": outcome_schema,
    })
}

fn conflict_evaluation(
    mutation: &Mutation,
    outcome_schema: SchemaRef,
    code: &str,
    message: &str,
    client: &SpiClient<'_>,
    target: ConflictTarget<'_>,
) -> EvaluatedMutation {
    let mut object = base_outcome(
        mutation,
        &outcome_schema,
        &target.table.table_id,
        &target.table.primary_key_field_id,
        mutation
            .pk
            .as_object()
            .and_then(|object| object.values().next())
            .unwrap_or(&serde_json::Value::Null),
    );
    object["status"] = serde_json::Value::String("conflict".into());
    object["code"] = serde_json::Value::String(code.into());
    object["message"] = serde_json::Value::String(message.into());
    if let Some(existing) = target.existing {
        if let Some(row) = existing.data.as_ref() {
            object["server_row"] = row.clone();
            let version = existing
                .row_version
                .as_deref()
                .unwrap_or_else(|| pgrx::error!("conflict row has no server version"));
            let checksum = row_checksum(
                client,
                target.table,
                row,
                target.record_id,
                version,
                &outcome_schema,
            );
            object["row_checksum"] = serde_json::to_value(checksum).unwrap();
        }
        if let Some(version) = existing.row_version.as_ref() {
            object["server_version"] = serde_json::Value::String(version.clone());
        }
    }
    EvaluatedMutation {
        mutation: mutation.clone(),
        outcome: object,
        accepted: false,
        new_write: false,
        outcome_schema: outcome_schema.clone(),
        table_id: target.table.table_id.clone(),
        primary_key_field_id: target.table.primary_key_field_id.clone(),
        primary_key_type: target.table.pk_portable_type.clone(),
        primary_key_value: mutation
            .pk
            .as_object()
            .and_then(|object| object.values().next())
            .cloned()
            .unwrap_or(serde_json::Value::Null),
        row_identity: target.row_identity,
    }
}

fn accepted_evaluation(
    client: &SpiClient<'_>,
    mutation: &Mutation,
    outcome_schema: SchemaRef,
    table_reg: &TableRegistration,
    record_id: &str,
    row_identity: Option<Vec<u8>>,
) -> EvaluatedMutation {
    let fence = load_current_fence_version(client, mutation, table_reg, record_id)
        .unwrap_or_else(|| pgrx::error!("accepted push source write has no version fence"));
    accepted_outcome(
        client,
        mutation,
        outcome_schema,
        table_reg,
        record_id,
        row_identity,
        fence,
    )
}

/// The last write fence of a row in a push unit. It gives the row version and whether the row
/// exists after the unit.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct UnitEndFence {
    pub(crate) version: String,
    pub(crate) row_present: bool,
}

fn accepted_outcome(
    client: &SpiClient<'_>,
    mutation: &Mutation,
    outcome_schema: SchemaRef,
    table_reg: &TableRegistration,
    record_id: &str,
    row_identity: Option<Vec<u8>>,
    fence: UnitEndFence,
) -> EvaluatedMutation {
    let mut object = base_outcome(
        mutation,
        &outcome_schema,
        &table_reg.table_id,
        &table_reg.primary_key_field_id,
        mutation
            .pk
            .as_object()
            .and_then(|object| object.values().next())
            .unwrap_or(&serde_json::Value::Null),
    );
    object["status"] = serde_json::Value::String("applied".into());
    object["server_version"] = serde_json::Value::String(fence.version.clone());
    if fence.row_present {
        let row = load_current_server_row_json(client, record_id, table_reg)
            .unwrap_or_else(|| pgrx::error!("accepted row is missing from the source relation"));
        object["server_row"] = row.clone();
        let checksum = row_checksum(
            client,
            table_reg,
            &row,
            record_id,
            &fence.version,
            &outcome_schema,
        );
        object["row_checksum"] = serde_json::to_value(checksum).unwrap();
    }
    EvaluatedMutation {
        mutation: mutation.clone(),
        outcome: object,
        accepted: true,
        new_write: true,
        outcome_schema: outcome_schema.clone(),
        table_id: table_reg.table_id.clone(),
        primary_key_field_id: table_reg.primary_key_field_id.clone(),
        primary_key_type: table_reg.pk_portable_type.clone(),
        primary_key_value: mutation
            .pk
            .as_object()
            .and_then(|object| object.values().next())
            .cloned()
            .unwrap_or(serde_json::Value::Null),
        row_identity,
    }
}

fn replayed_mutation(mutation: &Mutation, stored: &StoredMutation) -> EvaluatedMutation {
    let accepted = stored
        .outcome
        .get("status")
        .and_then(serde_json::Value::as_str)
        == Some("applied");
    EvaluatedMutation {
        mutation: mutation.clone(),
        outcome: stored.outcome.clone(),
        accepted,
        new_write: false,
        outcome_schema: stored.outcome_schema.clone(),
        table_id: mutation.table.clone(),
        primary_key_field_id: mutation
            .pk
            .as_object()
            .and_then(|object| object.keys().next())
            .cloned()
            .unwrap_or_default(),
        primary_key_type: "json".into(),
        primary_key_value: mutation
            .pk
            .as_object()
            .and_then(|object| object.values().next())
            .cloned()
            .unwrap_or(serde_json::Value::Null),
        row_identity: None,
    }
}

/// Evaluates an atomic group in request order and keeps its writes only when every mutation applies.
fn evaluate_atomic_group(
    client: &mut SpiClient<'_>,
    user_id: &str,
    request: &PushRequest,
    context: &EvaluationContext<'_>,
) -> Vec<EvaluatedMutation> {
    let evaluated = run_in_subtransaction(|| {
        client
            .update("SET CONSTRAINTS ALL DEFERRED", None, &[])
            .unwrap_or_else(|_| pgrx::error!("deferring atomic group constraints failed"));
        let mut evaluated = Vec::with_capacity(request.mutations.len());
        for mutation in &request.mutations {
            let evaluation = evaluate_mutation(client, user_id, mutation, context);
            let applied = evaluation.accepted;
            evaluated.push(evaluation);
            if !applied {
                return (evaluated, false);
            }
        }
        // The last mutation owns the group-end check, so it also owns each write that a
        // deferred trigger makes during that check.
        let last_mutation = request
            .mutations
            .last()
            .unwrap_or_else(|| pgrx::error!("atomic group has no last mutation"));
        set_push_mutation_id(client, &last_mutation.mutation_id);
        let constraints_valid = client
            .select(
                "SELECT synchro_check_push_constraints() AS valid",
                None,
                &[],
            )
            .unwrap_or_else(|_| pgrx::error!("checking atomic group constraints failed"))
            .first()
            .get_by_name::<bool, &str>("valid")
            .unwrap_or_else(|_| pgrx::error!("reading atomic group constraint result failed"))
            .unwrap_or_else(|| pgrx::error!("atomic group constraint result is missing"));
        clear_push_mutation_id(client);
        if !constraints_valid {
            let failure = evaluated
                .last()
                .map(push_constraint_validation_evaluation)
                .unwrap_or_else(|| pgrx::error!("atomic group has no last mutation"));
            *evaluated
                .last_mut()
                .unwrap_or_else(|| pgrx::error!("atomic group has no last mutation")) = failure;
            return (evaluated, false);
        }
        let evaluated = evaluated
            .iter()
            .map(|evaluation| group_end_evaluation(client, evaluation, context))
            .collect();
        (evaluated, true)
    });
    let Some(failure) = evaluated.last().filter(|evaluation| !evaluation.accepted) else {
        return evaluated;
    };
    let failing_index = evaluated.len() - 1;
    let failure = if failure.outcome["status"] == "conflict" {
        reread_group_conflict(client, failure, context)
    } else {
        failure.clone()
    };
    request
        .mutations
        .iter()
        .enumerate()
        .map(|(index, mutation)| {
            if index == failing_index {
                failure.clone()
            } else {
                atomic_group_rejection(mutation, context)
            }
        })
        .collect()
}

/// Rebuilds an accepted group outcome from the row state after the group-end check. The group is
/// one push unit, so a later mutation or a deferred trigger can write the row after its mutation.
fn group_end_evaluation(
    client: &SpiClient<'_>,
    evaluation: &EvaluatedMutation,
    context: &EvaluationContext<'_>,
) -> EvaluatedMutation {
    let (table_reg, record_id) = group_evaluation_record(client, evaluation, context);
    let fence = load_group_end_fence(client, table_reg, &record_id);
    accepted_outcome(
        client,
        &evaluation.mutation,
        evaluation.outcome_schema.clone(),
        table_reg,
        &record_id,
        evaluation.row_identity.clone(),
        fence,
    )
}

fn group_evaluation_record<'a>(
    client: &SpiClient<'_>,
    evaluation: &EvaluatedMutation,
    context: &EvaluationContext<'a>,
) -> (&'a TableRegistration, String) {
    let table_reg = context
        .registry
        .get(&evaluation.table_id)
        .unwrap_or_else(|| pgrx::error!("atomic group table is not registered"));
    let wire_record_id = wire_record_id(table_reg, &evaluation.primary_key_value)
        .unwrap_or_else(|_| pgrx::error!("atomic group primary key is invalid"));
    let record_id = canonicalize_record_id(client, &wire_record_id, table_reg)
        .unwrap_or_else(|| pgrx::error!("atomic group primary key is not canonical"));
    (table_reg, record_id)
}

/// Reads the last write fence of a row in the current transaction. An atomic request writes each
/// row in one group, so every fence of the row in this transaction belongs to the group.
fn load_group_end_fence(
    client: &SpiClient<'_>,
    table_reg: &TableRegistration,
    record_id: &str,
) -> UnitEndFence {
    let rows = client
        .select(
            "SELECT new_record_id, row_version::text AS row_version, coverage
             FROM sync_write_fences
             WHERE transaction_xid = pg_current_xact_id()
               AND relation_id = $1::uuid
               AND (old_record_id = $2 OR new_record_id = $2)
             ORDER BY dml_ordinal DESC
             LIMIT 1",
            None,
            &[table_reg.relation_id.as_str().into(), record_id.into()],
        )
        .unwrap_or_else(|_| pgrx::error!("loading atomic group write fence failed"));
    rows.into_iter()
        .next()
        .and_then(|fence| {
            let new_record_id = fence.get_by_name::<String, &str>("new_record_id").ok()?;
            let coverage = fence.get_by_name::<String, &str>("coverage").ok()??;
            let version = fence.get_by_name::<String, &str>("row_version").ok()??;
            (coverage == "pending").then(|| UnitEndFence {
                version,
                row_present: new_record_id.as_deref() == Some(record_id),
            })
        })
        .unwrap_or_else(|| pgrx::error!("accepted atomic group row has no final version fence"))
}

fn push_constraint_validation_evaluation(evaluation: &EvaluatedMutation) -> EvaluatedMutation {
    terminal_evaluation(
        &evaluation.mutation,
        evaluation.outcome_schema.clone(),
        "validation_failed",
        "mutation failed physical validation",
        EvaluationTarget {
            table_id: evaluation.table_id.clone(),
            primary_key_field_id: evaluation.primary_key_field_id.clone(),
            primary_key_type: evaluation.primary_key_type.clone(),
            primary_key_value: evaluation.primary_key_value.clone(),
            row_identity: evaluation.row_identity.clone(),
        },
        None,
    )
}

/// Runs `body` in one internal subtransaction. `body` returns its value and whether to keep its writes.
fn run_in_subtransaction<T>(body: impl FnOnce() -> (T, bool)) -> T {
    // SAFETY: This is the PL/pgSQL exec_stmt_block sequence. It saves the memory context and
    // resource owner, begins an internal subtransaction, runs the body in the outer memory context,
    // then releases or rolls back the subtransaction and restores both. The catch is required
    // because an error that escapes an open internal subtransaction leaves an autocommit session in
    // a failed subtransaction, and a catching caller would then roll back the wrong subtransaction.
    // The catch keeps the error data stack, because pgrx rethrows a PostgreSQL error from it.
    unsafe {
        let outer_context = pg_sys::CurrentMemoryContext;
        let outer_owner = pg_sys::CurrentResourceOwner;
        pg_sys::BeginInternalSubTransaction(std::ptr::null());
        // pgrx runs SPI read-only while the current subtransaction has no transaction ID. A
        // read-only statement cannot lock rows and does not see earlier writes of the calling function.
        pg_sys::GetCurrentTransactionId();
        pg_sys::MemoryContextSwitchTo(outer_context);
        let (value, commit) = PgTryBuilder::new(std::panic::AssertUnwindSafe(body))
            .catch_others(|error| {
                pg_sys::RollbackAndReleaseCurrentSubTransaction();
                pg_sys::MemoryContextSwitchTo(outer_context);
                pg_sys::CurrentResourceOwner = outer_owner;
                error.rethrow()
            })
            .execute();
        if commit {
            pg_sys::ReleaseCurrentSubTransaction();
        } else {
            pg_sys::RollbackAndReleaseCurrentSubTransaction();
        }
        pg_sys::MemoryContextSwitchTo(outer_context);
        pg_sys::CurrentResourceOwner = outer_owner;
        value
    }
}

/// Rebuilds a failing group conflict from the committed row state after the rollback.
fn reread_group_conflict(
    client: &SpiClient<'_>,
    failure: &EvaluatedMutation,
    context: &EvaluationContext<'_>,
) -> EvaluatedMutation {
    let (table_reg, record_id) = group_evaluation_record(client, failure, context);
    let existing = load_existing_record(client, &record_id, table_reg, SourceStatement::NoRowWrite);
    if existing
        .as_ref()
        .is_some_and(RowState::hidden_by_row_security)
    {
        return policy_evaluation(
            &failure.mutation,
            failure.outcome_schema.clone(),
            registered_target(
                table_reg,
                &failure.primary_key_field_id,
                &failure.primary_key_value,
                failure.row_identity.clone(),
            ),
        );
    }
    let outcome_text = |member: &str| {
        failure.outcome[member]
            .as_str()
            .unwrap_or_else(|| pgrx::error!("atomic group conflict outcome is incomplete"))
    };
    conflict_evaluation(
        &failure.mutation,
        failure.outcome_schema.clone(),
        outcome_text("code"),
        outcome_text("message"),
        client,
        ConflictTarget {
            existing: existing.as_ref(),
            table: table_reg,
            record_id: &record_id,
            row_identity: failure.row_identity.clone(),
        },
    )
}

fn atomic_group_rejection(
    mutation: &Mutation,
    context: &EvaluationContext<'_>,
) -> EvaluatedMutation {
    let (pk_field_id, pk_value) = mutation_primary_key(mutation);
    let target = match context.registry.get(&mutation.table) {
        Some(table_reg) => registered_target(table_reg, &pk_field_id, &pk_value, None),
        None => unresolved_target(mutation, &pk_field_id, &pk_value),
    };
    terminal_evaluation(
        mutation,
        context.submitted_schema.clone(),
        "atomic_batch_rejected",
        "atomic batch rejected",
        target,
        None,
    )
}

fn build_push_response(
    request: &PushRequest,
    server_time: chrono::DateTime<chrono::Utc>,
    evaluated: &[EvaluatedMutation],
) -> PushResponse {
    let mut accepted = Vec::new();
    let mut rejected = Vec::new();
    for evaluation in evaluated {
        if evaluation.accepted {
            accepted.push(
                serde_json::from_value::<AcceptedMutation>(evaluation.outcome.clone())
                    .unwrap_or_else(|_| {
                        pgrx::error!("accepted push outcome violates the contract")
                    }),
            );
        } else {
            rejected.push(
                serde_json::from_value::<RejectedMutation>(evaluation.outcome.clone())
                    .unwrap_or_else(|_| {
                        pgrx::error!("rejected push outcome violates the contract")
                    }),
            );
        }
    }
    let response = PushResponse {
        batch_id: request.batch_id.clone(),
        server_time,
        accepted,
        rejected,
    };
    if response.validate_for_request(request).is_err() {
        pgrx::error!("push outcome partition violates the contract")
    }
    response
}

fn increment_accepted_write_epoch(client: &mut SpiClient<'_>, user_id: &str, client_id: &str) {
    let rows = client
        .update(
            "UPDATE sync_clients
             SET accepted_write_epoch = accepted_write_epoch + 1,
                 last_push_at = now(), updated_at = now()
             WHERE user_id = $1 AND client_id = $2 AND is_active
             RETURNING accepted_write_epoch",
            None,
            &[user_id.into(), client_id.into()],
        )
        .unwrap_or_else(|_| pgrx::error!("incrementing accepted write epoch failed"));
    if rows.is_empty() {
        pgrx::error!("accepted write epoch row is missing")
    }
}

fn validate_wire_value(field: &FieldRegistration, value: &serde_json::Value) -> Result<(), String> {
    let portable = PortableType::parse(&field.portable_type).map_err(|error| error.to_string())?;
    let spec = FieldSpec::new(
        portable,
        field.nullable,
        field.decimal_precision.map(|value| value as u32),
        field.decimal_scale.map(|value| value as u32),
    )
    .map_err(|error| error.to_string())?;
    // The float wire text is the RFC 8785 form of the binary64 value. Other types keep the parsed text.
    let raw = if portable == PortableType::Float {
        serde_json_canonicalizer::to_string(value)
    } else {
        serde_json::to_string(value)
    }
    .map_err(|error| error.to_string())?;
    encode_typed_value(&spec, &raw)
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn wire_record_id(
    table_reg: &TableRegistration,
    value: &serde_json::Value,
) -> Result<String, String> {
    match table_reg.pk_portable_type.as_str() {
        "string" | "int64" => value
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| "primary key wire type is invalid".into()),
        "int" => value
            .as_i64()
            .map(|value| value.to_string())
            .ok_or_else(|| "primary key wire type is invalid".into()),
        _ => Err("primary key type is invalid".into()),
    }
}

fn logical_row_identity(
    table_reg: &TableRegistration,
    primary_key_value: &serde_json::Value,
) -> Option<Vec<u8>> {
    let table = crate::pull::canonical_table(table_reg)
        .unwrap_or_else(|_| pgrx::error!("building canonical table identity failed"));
    let primary_key_json = serde_json::to_string(primary_key_value)
        .unwrap_or_else(|_| pgrx::error!("encoding primary key identity failed"));
    Some(
        row_identity(&table, &primary_key_json)
            .unwrap_or_else(|_| pgrx::error!("building logical row identity failed"))
            .into_bytes(),
    )
}

fn row_checksum(
    _client: &SpiClient<'_>,
    table_reg: &TableRegistration,
    row: &serde_json::Value,
    record_id: &str,
    version: &str,
    outcome_schema: &SchemaRef,
) -> ChecksumObject {
    let table = crate::pull::canonical_table(table_reg)
        .unwrap_or_else(|_| pgrx::error!("building canonical table for row checksum failed"));
    let primary_key_json = match table_reg.pk_portable_type.as_str() {
        "string" | "int64" => serde_json::Value::String(record_id.to_string()),
        "int" => serde_json::from_str(record_id)
            .unwrap_or_else(|_| pgrx::error!("encoding integer row identity failed")),
        _ => pgrx::error!("unsupported row primary-key type"),
    };
    let row_json = serde_json_canonicalizer::to_string(row)
        .unwrap_or_else(|_| pgrx::error!("encoding authoritative row failed"));
    let pk_json = serde_json::to_string(&primary_key_json)
        .unwrap_or_else(|_| pgrx::error!("encoding authoritative primary key failed"));
    let canonical_row = CanonicalRow::from_json(pk_json, &row_json)
        .unwrap_or_else(|_| pgrx::error!("authoritative row is not canonical"));
    let schema_hash = SchemaHash::from_lower_hex(&outcome_schema.hash)
        .unwrap_or_else(|_| pgrx::error!("outcome schema hash is invalid"));
    ChecksumObject::new(
        row_digest(schema_hash, &table, &canonical_row, version)
            .unwrap_or_else(|_| pgrx::error!("computing authoritative row checksum failed")),
    )
}

fn canonicalize_record_id(
    client: &SpiClient<'_>,
    record_id: &str,
    table_reg: &TableRegistration,
) -> Option<String> {
    let valid = client
        .select(
            "SELECT pg_input_is_valid($1, $2) AS valid",
            None,
            &[record_id.into(), table_reg.pk_type.as_str().into()],
        )
        .unwrap_or_else(|_| pgrx::error!("validating push row identity failed"))
        .first()
        .get_by_name::<bool, &str>("valid")
        .unwrap_or_else(|_| pgrx::error!("reading push row identity validity failed"))
        .unwrap_or(false);
    if !valid {
        return None;
    }

    let sql = format!("SELECT ($1::{})::text AS record_id", table_reg.pk_type);
    let canonical = client
        .select(&sql, None, &[record_id.into()])
        .unwrap_or_else(|_| pgrx::error!("canonicalizing push row identity failed"))
        .first()
        .get_by_name::<String, &str>("record_id")
        .unwrap_or_else(|_| pgrx::error!("reading canonical push row identity failed"))
        .unwrap_or_else(|| pgrx::error!("canonical push row identity is missing"));
    (canonical == record_id).then_some(canonical)
}

fn load_existing_record(
    client: &SpiClient<'_>,
    record_id: &str,
    table_reg: &TableRegistration,
    statement: SourceStatement,
) -> Option<RowState> {
    let deleted_at_expr = if table_reg.has_deleted_at {
        format!("{}::text", pg_quote_ident(&table_reg.deleted_at_col))
    } else {
        "NULL::text".into()
    };
    let source_rows = format!(
        "FROM {table} t WHERE {pk} = $1::{pk_type}",
        table = qualified_relation_name(&table_reg.physical_schema, &table_reg.physical_relation),
        pk = pg_quote_ident(&table_reg.pk_column),
        pk_type = table_reg.pk_type,
    );
    let sql = format!(
        "SELECT {deleted_at} AS deleted_at, ({projection})::text AS data
         {source_rows}",
        deleted_at = deleted_at_expr,
        projection = synced_row_projection_sql(table_reg, "t"),
    );
    let registered = pg_sys::Oid::from(table_reg.physical_relation_oid);
    if matches!(statement, SourceStatement::Update(_)) {
        // SAFETY: LockRelationOid only takes a lock. The source DML takes the same lock later.
        unsafe {
            pg_sys::LockRelationOid(registered, pg_sys::RowExclusiveLock as pg_sys::LOCKMODE)
        };
    }
    let row_lock = || match &statement {
        SourceStatement::NoRowWrite => Some(SourceRowLock::NoKeyUpdate),
        SourceStatement::Delete => Some(SourceRowLock::Update),
        SourceStatement::Update(assigned) => {
            // SAFETY: get_rel_relkind only reads the system cache.
            let relkind = unsafe { pg_sys::get_rel_relkind(registered) } as u8;
            let partition = if relkind == pg_sys::RELKIND_PARTITIONED_TABLE {
                let partition = client
                    .select(
                        &format!("SELECT t.tableoid {source_rows}"),
                        None,
                        &[record_id.into()],
                    )
                    .unwrap_or_else(|_| {
                        pgrx::error!("finding authoritative source partition failed")
                    })
                    .next()?
                    .get_by_name::<pg_sys::Oid, &str>("tableoid")
                    .unwrap_or_else(|_| {
                        pgrx::error!("reading authoritative source partition failed")
                    })
                    .unwrap_or_else(|| pgrx::error!("authoritative source partition is missing"));
                if !lock_partition_path(registered, partition) {
                    return None;
                }
                partition
            } else {
                registered
            };
            // The first lock takes the mode that PostgreSQL takes for the same statement before
            // its BEFORE row triggers. A weaker lock forces an upgrade in the statement. A
            // stronger lock blocks the foreign key checks of child rows.
            Some(update_row_lock(partition, assigned))
        }
    };
    let load_source = |nowait: bool| {
        let lock = row_lock()?;
        let nowait = if nowait { " NOWAIT" } else { "" };
        client
            .select(
                &format!("{sql} {}{nowait}", lock.clause()),
                None,
                &[record_id.into()],
            )
            .unwrap_or_else(|_| pgrx::error!("locking authoritative source row failed"))
            .next()
            .map(|row| {
                let deleted = row
                    .get_by_name::<String, &str>("deleted_at")
                    .unwrap_or(None)
                    .is_some();
                let data = row
                    .get_by_name::<String, &str>("data")
                    .unwrap_or(None)
                    .map(|data| {
                        let mut data: serde_json::Value = serde_json::from_str(&data)
                            .unwrap_or_else(|_| {
                                pgrx::error!("authoritative source row is not JSON")
                            });
                        crate::pull::canonicalize_synced_row_data(table_reg, &mut data)
                            .unwrap_or_else(|_| {
                                pgrx::error!("authoritative source row is not canonical")
                            });
                        data
                    });
                (deleted, data)
            })
    };
    let mut source = load_source(false);
    let versions = client
        .select(
            "SELECT row_version::text AS row_version, deleted
             FROM sync_row_versions
             WHERE relation_id = $1::uuid AND record_id = $2
             FOR UPDATE",
            None,
            &[table_reg.relation_id.as_str().into(), record_id.into()],
        )
        .unwrap_or_else(|_| pgrx::error!("locking authoritative row version failed"));
    let version = versions.into_iter().next().map(|row| {
        (
            row.get_by_name::<String, &str>("row_version")
                .unwrap_or(None),
            row.get_by_name::<bool, &str>("deleted")
                .unwrap_or(Some(false))
                .unwrap_or(false),
        )
    });
    if source.is_none() && version.as_ref().is_some_and(|(_, deleted)| !deleted) {
        // A writer can commit a new row between the two reads. Each writer changes the version
        // row in its own transaction, so the locked version fixes the committed source state.
        // A writer locks the source row before the version row. NOWAIT fails this push with a
        // retryable lock error instead of a deadlock that can abort that writer.
        source = load_source(true);
    }
    match (source, version) {
        (None, None) => None,
        (source, version) => {
            let (source_deleted, data) = source.unwrap_or((false, None));
            let (row_version, version_deleted) = version.unwrap_or((None, false));
            Some(RowState {
                data,
                row_version,
                deleted: source_deleted || version_deleted,
            })
        }
    }
}

/// Row lock mode for the authoritative source row of one push operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SourceRowLock {
    /// Serializes writers of the row and permits foreign key checks of child rows.
    NoKeyUpdate,
    /// Serializes writers and foreign key checks. A delete or a key update needs it.
    Update,
}

impl SourceRowLock {
    fn clause(self) -> &'static str {
        match self {
            Self::NoKeyUpdate => "FOR NO KEY UPDATE OF t",
            Self::Update => "FOR UPDATE OF t",
        }
    }
}

/// Source statement that follows the lock of the authoritative source row.
enum SourceStatement {
    /// An insert or a reread. It does not write the locked row.
    NoRowWrite,
    /// A hard delete.
    Delete,
    /// An update or a soft delete that assigns these physical columns.
    Update(Vec<String>),
}

/// Returns the row lock mode that `ExecUpdateLockMode` computes for an `UPDATE` of `relation`
/// that assigns the `assigned` columns. The caller must hold a lock on `relation`.
pub(crate) fn update_row_lock(relation: pg_sys::Oid, assigned: &[String]) -> SourceRowLock {
    if assigned.is_empty() {
        pgrx::error!("push update assigns no source column");
    }
    // A new statement accepts invalidation messages when it first locks the relation. The push
    // can hold that lock from an earlier mutation. So the push accepts them here, and the
    // statement of this mutation uses the same relation cache entry.
    // SAFETY: AcceptInvalidationMessages only makes the caches of this backend current.
    unsafe { pg_sys::AcceptInvalidationMessages() };
    // SAFETY: The caller holds a lock on the relation.
    let rel = unsafe { pg_sys::relation_open(relation, pg_sys::NoLock as pg_sys::LOCKMODE) };
    let mut memory = pgrx::PgMemoryContexts::new("synchro source row lock");
    // SAFETY: The relation stays open until the closure returns. The closure returns no value
    // that the memory context owns.
    let lock = unsafe {
        memory.switch_to(|_| {
            let mut assigned_columns: *mut pg_sys::Bitmapset = std::ptr::null_mut();
            for name in assigned {
                let name = std::ffi::CString::new(name.as_str())
                    .unwrap_or_else(|_| pgrx::error!("push update column name is invalid"));
                let attnum = pg_sys::get_attnum(relation, name.as_ptr());
                if attnum <= 0 {
                    pgrx::error!("push update column is not a source column");
                }
                assigned_columns = pg_sys::bms_add_member(
                    assigned_columns,
                    i32::from(attnum) - pg_sys::FirstLowInvalidHeapAttributeNumber,
                );
            }
            // This is the rule of ExecInitGenerated for an UPDATE.
            let mut generated_columns: *mut pg_sys::Bitmapset = std::ptr::null_mut();
            let constraints = (*(*rel).rd_att).constr;
            if !constraints.is_null()
                && ((*constraints).has_generated_stored || (*constraints).has_generated_virtual)
            {
                let before_row_trigger =
                    !(*rel).trigdesc.is_null() && (*(*rel).trigdesc).trig_update_before_row;
                let descriptor = pgrx::PgTupleDesc::from_pg_unchecked((*rel).rd_att);
                for (index, attribute) in descriptor.iter().enumerate() {
                    if attribute.attgenerated == 0 {
                        continue;
                    }
                    let attnum = i32::try_from(index + 1)
                        .unwrap_or_else(|_| pgrx::error!("source column number is invalid"));
                    if !before_row_trigger {
                        let expression = pg_sys::build_column_default(rel, attnum);
                        if expression.is_null() {
                            pgrx::error!("generated source column has no expression");
                        }
                        let mut referenced: *mut pg_sys::Bitmapset = std::ptr::null_mut();
                        pg_sys::pull_varattnos(expression, 1, &mut referenced);
                        if !pg_sys::bms_overlap(assigned_columns, referenced) {
                            continue;
                        }
                    }
                    generated_columns = pg_sys::bms_add_member(
                        generated_columns,
                        attnum - pg_sys::FirstLowInvalidHeapAttributeNumber,
                    );
                }
            }
            let key_columns = pg_sys::RelationGetIndexAttrBitmap(
                rel,
                pg_sys::IndexAttrBitmapKind::INDEX_ATTR_BITMAP_KEY,
            );
            if pg_sys::bms_overlap(key_columns, assigned_columns)
                || pg_sys::bms_overlap(key_columns, generated_columns)
            {
                SourceRowLock::Update
            } else {
                SourceRowLock::NoKeyUpdate
            }
        })
    };
    drop(memory);
    // SAFETY: relation_open opened this relation.
    unsafe { pg_sys::relation_close(rel, pg_sys::NoLock as pg_sys::LOCKMODE) };
    lock
}

/// Locks each relation below `registered` down to `partition` in `ROW EXCLUSIVE` mode, in that
/// order, as an `UPDATE` of `registered` does. Returns `false` and takes no lock when
/// `registered` is not an ancestor of `partition`.
pub(crate) fn lock_partition_path(registered: pg_sys::Oid, partition: pg_sys::Oid) -> bool {
    let ancestors = Spi::connect(|client| {
        client
            .select(
                "SELECT ancestor.relid::pg_catalog.oid AS relid
                 FROM pg_catalog.pg_partition_ancestors($1::pg_catalog.regclass)
                      WITH ORDINALITY AS ancestor(relid, ordinal)
                 ORDER BY ancestor.ordinal",
                None,
                &[partition.into()],
            )
            .unwrap_or_else(|_| pgrx::error!("reading source partition ancestors failed"))
            .map(|row| {
                row.get_by_name::<pg_sys::Oid, &str>("relid")
                    .unwrap_or_else(|_| pgrx::error!("reading source partition ancestor failed"))
                    .unwrap_or_else(|| pgrx::error!("source partition ancestor is missing"))
            })
            .collect::<Vec<_>>()
    });
    let Some(depth) = ancestors
        .iter()
        .position(|ancestor| *ancestor == registered)
    else {
        return false;
    };
    for relation in ancestors[..depth].iter().rev() {
        // SAFETY: LockRelationOid only takes a lock.
        unsafe { pg_sys::LockRelationOid(*relation, pg_sys::RowExclusiveLock as pg_sys::LOCKMODE) };
    }
    true
}

fn load_current_server_row_json(
    client: &SpiClient<'_>,
    record_id: &str,
    table_reg: &TableRegistration,
) -> Option<serde_json::Value> {
    let sql = format!(
        "SELECT ({projection})::text AS data
         FROM {table} t WHERE {pk} = $1::{pk_type}",
        projection = synced_row_projection_sql(table_reg, "t"),
        table = qualified_relation_name(&table_reg.physical_schema, &table_reg.physical_relation),
        pk = pg_quote_ident(&table_reg.pk_column),
        pk_type = table_reg.pk_type,
    );
    client
        .select(&sql, None, &[record_id.into()])
        .unwrap_or_else(|_| pgrx::error!("loading accepted authoritative row failed"))
        .next()
        .and_then(|row| {
            row.get_by_name::<String, &str>("data")
                .unwrap_or_else(|_| pgrx::error!("reading accepted authoritative row failed"))
        })
        .map(|data| {
            let mut data: serde_json::Value = serde_json::from_str(&data)
                .unwrap_or_else(|_| pgrx::error!("accepted authoritative row is not JSON"));
            crate::pull::canonicalize_synced_row_data(table_reg, &mut data)
                .unwrap_or_else(|_| pgrx::error!("accepted authoritative row is not canonical"));
            data
        })
}

/// Reads the fences of one mutation for its row. The first fence must be the write of the mutation.
/// Later fences come from triggers in the same push unit, and the last fence gives the final state.
pub(crate) fn load_current_fence_version(
    client: &SpiClient<'_>,
    mutation: &Mutation,
    table_reg: &TableRegistration,
    record_id: &str,
) -> Option<UnitEndFence> {
    let rows = client
        .select(
            "SELECT operation, old_record_id, new_record_id,
                    row_version::text AS row_version, coverage
             FROM sync_write_fences
             WHERE transaction_xid = pg_current_xact_id()
               AND mutation_id = $1
               AND relation_id = $2::uuid
               AND (old_record_id = $3 OR new_record_id = $3)
             ORDER BY dml_ordinal",
            None,
            &[
                mutation.mutation_id.as_str().into(),
                table_reg.relation_id.as_str().into(),
                record_id.into(),
            ],
        )
        .unwrap_or_else(|_| pgrx::error!("loading push write fence failed"));
    if rows.is_empty() {
        return None;
    }
    let expected = match mutation.op {
        Operation::Insert => ("insert", None, Some(record_id)),
        Operation::Update => ("update", Some(record_id), Some(record_id)),
        Operation::Delete if table_reg.has_deleted_at => {
            ("update", Some(record_id), Some(record_id))
        }
        Operation::Delete => ("delete", Some(record_id), None),
        Operation::Upsert => pgrx::error!("push upsert passed contract validation"),
    };
    let mut fence = None;
    for (index, row) in rows.into_iter().enumerate() {
        let operation = row
            .get_by_name::<String, &str>("operation")
            .unwrap_or_else(|_| pgrx::error!("reading push write fence operation failed"))?;
        let old_record_id = row
            .get_by_name::<String, &str>("old_record_id")
            .unwrap_or_else(|_| pgrx::error!("reading push write fence old identity failed"));
        let new_record_id = row
            .get_by_name::<String, &str>("new_record_id")
            .unwrap_or_else(|_| pgrx::error!("reading push write fence new identity failed"));
        let coverage = row
            .get_by_name::<String, &str>("coverage")
            .unwrap_or_else(|_| pgrx::error!("reading push write fence coverage failed"))?;
        let identities_match = index > 0
            || (operation == expected.0
                && old_record_id.as_deref() == expected.1
                && new_record_id.as_deref() == expected.2);
        if !identities_match || coverage != "pending" {
            return None;
        }
        let version = row
            .get_by_name::<String, &str>("row_version")
            .unwrap_or_else(|_| pgrx::error!("reading push write fence failed"))?;
        fence = Some(UnitEndFence {
            version,
            row_present: new_record_id.as_deref() == Some(record_id),
        });
    }
    fence
}

fn build_dml_data(
    table_reg: &TableRegistration,
    columns: &serde_json::Map<String, serde_json::Value>,
) -> serde_json::Value {
    let mut data = serde_json::Map::new();
    for (field_id, value) in columns {
        let field = table_reg
            .fields
            .iter()
            .find(|field| field.field_id == *field_id)
            .unwrap_or_else(|| pgrx::error!("validated push field is missing"));
        data.insert(field.physical_column.clone(), sql_wire_value(field, value));
    }
    serde_json::Value::Object(data)
}

fn sql_wire_value(field: &FieldRegistration, value: &serde_json::Value) -> serde_json::Value {
    if value.is_null() {
        return serde_json::Value::Null;
    }
    match field.portable_type.as_str() {
        "json" => value
            .as_str()
            .and_then(|text| serde_json::from_str(text).ok())
            .unwrap_or_else(|| pgrx::error!("validated JSON field is not canonical")),
        "bytes" => {
            let encoded = value
                .as_str()
                .unwrap_or_else(|| pgrx::error!("validated bytes field is not canonical"));
            let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(encoded)
                .unwrap_or_else(|_| pgrx::error!("validated bytes field is not canonical"));
            let mut hex_value = String::from("\\x");
            for byte in bytes {
                hex_value.push_str(&format!("{byte:02x}"));
            }
            serde_json::Value::String(hex_value)
        }
        _ => value.clone(),
    }
}

fn push_insert(
    client: &mut SpiClient<'_>,
    mutation_id: &str,
    record_id: &str,
    table_reg: &TableRegistration,
    data: &serde_json::Value,
    mutation_is_push_unit: bool,
) -> DmlOutcome {
    let object = data
        .as_object()
        .unwrap_or_else(|| pgrx::error!("push insert payload is not an object"));
    let mut columns = object.keys().cloned().collect::<Vec<_>>();
    columns.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    let col_list = std::iter::once(pg_quote_ident(&table_reg.pk_column))
        .chain(columns.iter().map(|column| pg_quote_ident(column)))
        .collect::<Vec<_>>()
        .join(", ");
    let select_list = std::iter::once(format!("$2::{}", table_reg.pk_type))
        .chain(
            columns
                .iter()
                .map(|column| format!("r.{}", pg_quote_ident(column))),
        )
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "INSERT INTO {} ({col_list})
         SELECT {select_list} FROM jsonb_populate_record(NULL::{table}, $1::jsonb) r
         ON CONFLICT ({pk}) DO NOTHING RETURNING true AS applied",
        qualified_relation_name(&table_reg.physical_schema, &table_reg.physical_relation),
        col_list = col_list,
        select_list = select_list,
        table = qualified_relation_name(&table_reg.physical_schema, &table_reg.physical_relation),
        pk = pg_quote_ident(&table_reg.pk_column),
    );
    execute_push_dml(
        client,
        mutation_id,
        &sql,
        data,
        record_id,
        mutation_is_push_unit,
    )
}

fn push_update(
    client: &mut SpiClient<'_>,
    mutation_id: &str,
    record_id: &str,
    table_reg: &TableRegistration,
    data: &serde_json::Value,
    mutation_is_push_unit: bool,
) -> DmlOutcome {
    let object = data
        .as_object()
        .unwrap_or_else(|| pgrx::error!("push update payload is not an object"));
    let mut columns = object.keys().cloned().collect::<Vec<_>>();
    columns.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    if columns.is_empty() {
        pgrx::error!("push update has no writable fields")
    }
    let mut assignments = columns
        .iter()
        .map(|column| format!("{} = r.{}", pg_quote_ident(column), pg_quote_ident(column)))
        .collect::<Vec<_>>();
    if table_reg.has_updated_at {
        assignments.push(format!(
            "{} = now()",
            pg_quote_ident(&table_reg.updated_at_col)
        ));
    }
    let sql = format!(
        "UPDATE {} AS target SET {assignments}
         FROM jsonb_populate_record(NULL::{table}, $1::jsonb) r
         WHERE target.{pk} = $2::{pk_type} RETURNING true AS applied",
        qualified_relation_name(&table_reg.physical_schema, &table_reg.physical_relation),
        assignments = assignments.join(", "),
        table = qualified_relation_name(&table_reg.physical_schema, &table_reg.physical_relation),
        pk = pg_quote_ident(&table_reg.pk_column),
        pk_type = table_reg.pk_type,
    );
    execute_push_dml(
        client,
        mutation_id,
        &sql,
        data,
        record_id,
        mutation_is_push_unit,
    )
}

fn push_soft_delete(
    client: &mut SpiClient<'_>,
    mutation_id: &str,
    record_id: &str,
    table_reg: &TableRegistration,
    mutation_is_push_unit: bool,
) -> DmlOutcome {
    let updated_at = if table_reg.has_updated_at {
        format!(", {} = now()", pg_quote_ident(&table_reg.updated_at_col))
    } else {
        String::new()
    };
    let sql = format!(
        "UPDATE {table} SET {deleted_at} = now(){updated_at}
         WHERE {pk} = $2::{pk_type} RETURNING true AS applied",
        table = qualified_relation_name(&table_reg.physical_schema, &table_reg.physical_relation),
        deleted_at = pg_quote_ident(&table_reg.deleted_at_col),
        updated_at = updated_at,
        pk = pg_quote_ident(&table_reg.pk_column),
        pk_type = table_reg.pk_type,
    );
    execute_push_dml(
        client,
        mutation_id,
        &sql,
        &serde_json::Value::Object(serde_json::Map::new()),
        record_id,
        mutation_is_push_unit,
    )
}

fn push_hard_delete(
    client: &mut SpiClient<'_>,
    mutation_id: &str,
    record_id: &str,
    table_reg: &TableRegistration,
    mutation_is_push_unit: bool,
) -> DmlOutcome {
    let sql = format!(
        "DELETE FROM {} WHERE {} = $2::{} RETURNING true AS applied",
        qualified_relation_name(&table_reg.physical_schema, &table_reg.physical_relation),
        pg_quote_ident(&table_reg.pk_column),
        table_reg.pk_type,
    );
    execute_push_dml(
        client,
        mutation_id,
        &sql,
        &serde_json::Value::Object(serde_json::Map::new()),
        record_id,
        mutation_is_push_unit,
    )
}

fn execute_push_dml(
    client: &mut SpiClient<'_>,
    mutation_id: &str,
    sql: &str,
    data: &serde_json::Value,
    record_id: &str,
    mutation_is_push_unit: bool,
) -> DmlOutcome {
    set_push_mutation_id(client, mutation_id);
    let outcome = client
        .update(
            "SELECT applied, validation_failed, policy_rejected
             FROM synchro_execute_push_dml($1, $2::jsonb, $3, $4)",
            None,
            &[
                sql.into(),
                pgrx::JsonB(data.clone()).into(),
                record_id.into(),
                mutation_is_push_unit.into(),
            ],
        )
        .unwrap_or_else(|_| pgrx::error!("executing push source DML failed"))
        .first();
    let applied = outcome
        .get_by_name::<bool, &str>("applied")
        .unwrap_or_else(|_| pgrx::error!("reading push source DML result failed"))
        .unwrap_or_else(|| pgrx::error!("push source DML result is missing"));
    let validation_failed = outcome
        .get_by_name::<bool, &str>("validation_failed")
        .unwrap_or_else(|_| pgrx::error!("reading push source DML validation result failed"))
        .unwrap_or_else(|| pgrx::error!("push source DML validation result is missing"));
    let policy_rejected = outcome
        .get_by_name::<bool, &str>("policy_rejected")
        .unwrap_or_else(|_| pgrx::error!("reading push source DML policy result failed"))
        .unwrap_or_else(|| pgrx::error!("push source DML policy result is missing"));
    clear_push_mutation_id(client);
    match (applied, validation_failed, policy_rejected) {
        (true, false, false) => DmlOutcome::Applied,
        (false, false, false) => DmlOutcome::NotApplied,
        (false, true, false) => DmlOutcome::ValidationFailed,
        (false, false, true) => DmlOutcome::PolicyRejected,
        _ => pgrx::error!("push source DML returned an invalid disposition"),
    }
}

fn call_write_protect_or_error(
    client: &mut SpiClient<'_>,
    user_id: &str,
    table_id: &str,
    operation: &str,
    data: &serde_json::Value,
) -> Option<serde_json::Value> {
    client
        .update(
            "SELECT synchro_write_protect($1, $2, $3, $4) AS result",
            None,
            &[
                user_id.into(),
                table_id.into(),
                operation.into(),
                pgrx::JsonB(data.clone()).into(),
            ],
        )
        .unwrap_or_else(|_| pgrx::error!("write policy evaluation failed"))
        .first()
        .get_one::<pgrx::JsonB>()
        .unwrap_or_else(|_| pgrx::error!("reading write policy result failed"))
        .map(|result| result.0)
}

fn check_write_protect_exists(client: &SpiClient<'_>) -> bool {
    client
        .select(
            "SELECT EXISTS (
                 SELECT 1 FROM pg_proc p
                 JOIN pg_namespace n ON n.oid = p.pronamespace
                 WHERE p.proname = 'synchro_write_protect'
                   AND n.nspname = ANY(current_schemas(false))
             ) AS exists",
            None,
            &[],
        )
        .unwrap_or_else(|_| pgrx::error!("checking write policy availability failed"))
        .first()
        .get_one::<bool>()
        .unwrap_or_else(|_| pgrx::error!("reading write policy availability failed"))
        .unwrap_or(false)
}

fn load_registry_inner(client: &SpiClient<'_>) -> Vec<TableRegistration> {
    crate::registry::load_registry_from_client(client)
        .unwrap_or_else(|_| pgrx::error!("loading active registry failed"))
}
