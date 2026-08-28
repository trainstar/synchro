use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::CString;
use std::panic::{RefUnwindSafe, UnwindSafe};

use pgrx::bgworkers::*;
use pgrx::prelude::*;
use pgrx::spi::{SpiClient, SpiHeapTupleData, SpiTupleTable};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use synchro_core::change::ChangeOperation;
use synchro_core::checksum::{row_identity, scope_digest, ScopeDigestEntry, Sha256Digest};
use synchro_core::edge_diff::{build_edge_diff_entries, diff_bucket_sets};

use crate::bucketing::{resolve_dependency_impacts, resolve_membership};
use crate::pull::synced_row_digest;
use crate::registry::{
    load_membership_dependencies_from_client, load_registry_generation_for_activation,
    load_registry_generation_for_worker, load_registry_generation_from_client,
    MembershipDependency, RegistrationKind, TableRegistration,
};
use crate::wal_decoder::{
    ColumnInfo, RelationKey, TupleImage, TupleValue, WalDecoder, WalEvent, WalTransaction,
};

const BATCH_SIZE: i32 = 500;
const JSONB_BATCH_SIZE: usize = 500;
const IDLE_POLL_MS: u64 = 100;
const WORKER_ID: &str = "synchro_wal_consumer";
const REGISTRY_PREFIX: &str = "synchro_registry";
const FENCE_PREFIX: &str = "synchro_fence";
const MAX_CONTROL_MESSAGE_BYTES: usize = 4096;
const MAX_POISON_DETAIL_BYTES: usize = 512;
const STARTUP_RETRY_BUDGET: std::time::Duration = std::time::Duration::from_secs(30);
const STARTUP_RETRY_WAIT: std::time::Duration = std::time::Duration::from_secs(1);

#[derive(Clone)]
struct PoisonFailure {
    class: &'static str,
    detail: String,
    commit_lsn: u64,
    relation_id: Option<String>,
    commit_timestamp: Option<i64>,
}

enum PollFailure {
    Poison(PoisonFailure),
    Transient(&'static str),
    ActivationBarrier,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegistryActivation {
    generation: i64,
    action: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FenceMessage {
    fence_id: String,
    dml_ordinal: u64,
    registration_kind: String,
    relation_id: String,
    table_id: Option<String>,
    physical_schema: String,
    physical_relation: String,
    physical_relation_oid: u32,
    operation: String,
    old_record_id: Option<String>,
    new_record_id: Option<String>,
    old_capture_key: Option<serde_json::Value>,
    new_capture_key: Option<serde_json::Value>,
    row_version: String,
}

struct ApplicableEvent<'a> {
    event: &'a WalEvent,
    registration: &'a TableRegistration,
    operation: ChangeOperation,
    operation_name: &'static str,
    record_id: String,
    old_capture_key: Option<serde_json::Value>,
    new_capture_key: Option<serde_json::Value>,
    row_version: String,
    fence_id: String,
}

#[derive(Clone)]
struct ImpactedRow {
    registration_index: usize,
    record_id: String,
    operation: ChangeOperation,
    direct_change: bool,
    event_ordinal: u64,
    row_version: String,
    delete_projection_image: Option<&'static str>,
    digest: Option<synchro_core::checksum::Sha256Digest>,
}

#[derive(Clone)]
struct CapturedRow {
    row_data: serde_json::Value,
    row_version: String,
    digest: synchro_core::checksum::Sha256Digest,
    deleted: bool,
    registry_generation: i64,
}

struct DependencyEvent {
    dependency_relation_id: String,
    dependency_registration_kind: RegistrationKind,
    event_ordinal: u64,
    old_row: Option<serde_json::Value>,
    new_row: Option<serde_json::Value>,
}

struct PersistedEvents {
    direct_impacts: Vec<ImpactedRow>,
    dependency_events: Vec<DependencyEvent>,
}

struct MaterializedTransaction {
    end_lsn: u64,
}

#[derive(Clone)]
struct WorkerIdentity {
    session_login_oid: i64,
    worker_role_oid: pg_sys::Oid,
    startup_runtime: WorkerStartupIdentity,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct WorkerRuntimeIdentity {
    stream_generation: String,
    slot_name: String,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct WorkerStartupIdentity {
    runtime: WorkerRuntimeIdentity,
    active_slot_is_unbound: bool,
}

#[derive(Clone, PartialEq, Eq)]
struct RuntimeCaptureIdentity {
    stream_generation: String,
    slot_name: String,
    registry_generation: i64,
}

struct DecoderState {
    identity: RuntimeCaptureIdentity,
    decoder: WalDecoder,
}

#[derive(Clone, PartialEq, Eq)]
struct CandidateCaptureIdentity {
    bootstrap_id: String,
    slot_name: String,
    registry_generation: i64,
}

struct CandidateBootstrap {
    identity: CandidateCaptureIdentity,
    source_stream_generation: String,
    source_registry_generation: i64,
    consistent_point: u64,
    activation_barrier: u64,
    acknowledged_end_lsn: u64,
}

struct CandidateDecoderState {
    identity: CandidateCaptureIdentity,
    decoder: WalDecoder,
}

#[derive(Clone, Copy)]
enum ProjectionTarget<'a> {
    Active {
        stream_generation: &'a str,
    },
    Candidate {
        bootstrap_id: &'a str,
        registry_generation: i64,
    },
}

impl<'a> ProjectionTarget<'a> {
    const fn bootstrap_id(self) -> Option<&'a str> {
        match self {
            Self::Active { .. } => None,
            Self::Candidate { bootstrap_id, .. } => Some(bootstrap_id),
        }
    }

    const fn registry_generation(self) -> Option<i64> {
        match self {
            Self::Active { .. } => None,
            Self::Candidate {
                registry_generation,
                ..
            } => Some(registry_generation),
        }
    }
}

trait CandidateRow {
    fn candidate_text(&self, name: &str) -> Result<Option<String>, spi::Error>;
}

impl CandidateRow for SpiTupleTable<'_> {
    fn candidate_text(&self, name: &str) -> Result<Option<String>, spi::Error> {
        self.get_by_name::<String, &str>(name)
    }
}

impl CandidateRow for SpiHeapTupleData<'_> {
    fn candidate_text(&self, name: &str) -> Result<Option<String>, spi::Error> {
        self.get_by_name::<String, &str>(name)
    }
}

#[derive(Clone, Copy)]
enum FenceTarget<'a> {
    Active,
    Candidate {
        source_stream_generation: &'a str,
        source_registry_generation: i64,
        target_registry_generation: i64,
    },
}

fn transaction_content_hash(transaction: &WalTransaction) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hash_u64(&mut hasher, u64::from(transaction.xid));
    hash_u64(&mut hasher, transaction.final_lsn);
    hash_u64(&mut hasher, transaction.commit_lsn);
    hash_u64(&mut hasher, transaction.end_lsn);
    hasher.update(transaction.commit_timestamp.to_be_bytes());
    hash_u64(&mut hasher, transaction.events.len() as u64);
    for event in &transaction.events {
        hash_u64(&mut hasher, event.event_ordinal);
        hash_relation(&mut hasher, &event.relation);
        hash_bytes(&mut hasher, operation_name(event.operation).as_bytes());
        hash_image(&mut hasher, event.before.as_ref());
        hash_image(&mut hasher, event.after.as_ref());
    }
    hash_u64(&mut hasher, transaction.truncates.len() as u64);
    for truncate in &transaction.truncates {
        hash_u64(&mut hasher, truncate.event_ordinal);
        hash_relation(&mut hasher, &truncate.relation);
    }
    hash_u64(&mut hasher, transaction.messages.len() as u64);
    for message in &transaction.messages {
        hash_u64(&mut hasher, message.message_lsn);
        hash_bytes(&mut hasher, message.prefix.as_bytes());
        hash_bytes(&mut hasher, &message.content);
    }
    hasher.finalize().into()
}

fn hash_relation(hasher: &mut Sha256, relation: &RelationKey) {
    hash_bytes(hasher, relation.namespace.as_bytes());
    hash_bytes(hasher, relation.name.as_bytes());
    hash_u64(hasher, u64::from(relation.oid));
}

fn hash_image(hasher: &mut Sha256, image: Option<&TupleImage>) {
    let Some(image) = image else {
        hasher.update([0]);
        return;
    };
    hasher.update([1]);
    let mut columns: Vec<_> = image.iter().collect();
    columns.sort_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
    hash_u64(hasher, columns.len() as u64);
    for (column, value) in columns {
        hash_bytes(hasher, column.as_bytes());
        match value {
            TupleValue::Null => hasher.update([0]),
            TupleValue::Text(bytes) => {
                hasher.update([1]);
                hash_bytes(hasher, bytes);
            }
            TupleValue::Binary(bytes) => {
                hasher.update([2]);
                hash_bytes(hasher, bytes);
            }
            TupleValue::Unchanged => hasher.update([3]),
        }
    }
}

fn hash_bytes(hasher: &mut Sha256, value: &[u8]) {
    hash_u64(hasher, value.len() as u64);
    hasher.update(value);
}

fn hash_u64(hasher: &mut Sha256, value: u64) {
    hasher.update(value.to_be_bytes());
}

fn configured_replication_slot() -> String {
    configured_string(&crate::REPLICATION_SLOT_GUC, "synchro_slot")
}

fn publication_name() -> String {
    configured_string(&crate::PUBLICATION_NAME_GUC, "synchro_pub")
}

fn database_name() -> String {
    configured_string(&crate::DATABASE_GUC, "postgres")
}

fn configured_string(setting: &pgrx::GucSetting<Option<CString>>, fallback: &str) -> String {
    setting
        .get()
        .and_then(|value| value.to_str().ok().map(String::from))
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

pub fn register_bgworker() {
    BackgroundWorkerBuilder::new("synchro WAL consumer")
        .set_function("synchro_wal_worker_main")
        .set_library("synchro_pg")
        .set_argument(0i32.into_datum())
        .set_restart_time(Some(std::time::Duration::from_secs(5)))
        .enable_spi_access()
        .load();
}

fn startup_retry_exhausted(started_at: std::time::Instant) -> bool {
    started_at.elapsed() >= STARTUP_RETRY_BUDGET
}

#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn synchro_wal_worker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    // PostgreSQL restarts this worker only after a nonzero exit status.
    // Self-heal paths must raise errors. SIGTERM and shutdown return cleanly.
    let Some(worker_login) = crate::configured_worker_login() else {
        pgrx::error!("synchro WAL worker login is unavailable");
    };
    let database = database_name();
    BackgroundWorker::connect_worker_to_spi(Some(&database), Some(&worker_login));

    let preparation_started_at = std::time::Instant::now();
    let expected_runtime = loop {
        match run_worker_transaction(|| {
            capture_worker_preparation_identity(&database, &worker_login)
        }) {
            Ok(identity) => break identity,
            Err(error) => {
                if startup_retry_exhausted(preparation_started_at) {
                    log!(
                        "synchro WAL worker preparation failed after {} seconds: {error}",
                        STARTUP_RETRY_BUDGET.as_secs()
                    );
                    pgrx::error!(
                        "synchro WAL worker preparation failed after {} seconds: {error}",
                        STARTUP_RETRY_BUDGET.as_secs()
                    );
                }
                if !BackgroundWorker::wait_latch(Some(STARTUP_RETRY_WAIT)) {
                    return;
                }
            }
        }
    };
    let identity = loop {
        if let Err(error) = validate_worker_preparation_identity(&worker_login, &expected_runtime) {
            log!("synchro WAL worker identity changed: {error}");
            pgrx::error!("synchro WAL worker identity changed: {error}");
        }
        match run_worker_transaction(|| prepare_worker(&database, &worker_login)) {
            Ok(identity) => {
                if identity.startup_runtime != expected_runtime {
                    log!("synchro WAL worker identity changed: worker runtime identity changed");
                    pgrx::error!(
                        "synchro WAL worker identity changed: worker runtime identity changed"
                    );
                }
                if let Err(error) =
                    validate_worker_preparation_identity(&worker_login, &expected_runtime)
                {
                    log!("synchro WAL worker identity changed: {error}");
                    pgrx::error!("synchro WAL worker identity changed: {error}");
                }
                break identity;
            }
            Err(error) => {
                if let Err(identity_error) =
                    validate_worker_preparation_identity(&worker_login, &expected_runtime)
                {
                    log!("synchro WAL worker identity changed: {identity_error}");
                    pgrx::error!("synchro WAL worker identity changed: {identity_error}");
                }
                if startup_retry_exhausted(preparation_started_at) {
                    log!(
                        "synchro WAL worker preparation failed after {} seconds: {error}",
                        STARTUP_RETRY_BUDGET.as_secs()
                    );
                    pgrx::error!(
                        "synchro WAL worker preparation failed after {} seconds: {error}",
                        STARTUP_RETRY_BUDGET.as_secs()
                    );
                }
                if !BackgroundWorker::wait_latch(Some(STARTUP_RETRY_WAIT)) {
                    return;
                }
            }
        }
    };
    activate_worker_role(identity.worker_role_oid);

    let initialization_started_at = std::time::Instant::now();
    loop {
        if let Err(error) = validate_worker_startup_authorization(&worker_login, &identity) {
            log!("synchro WAL worker identity changed: {error}");
            pgrx::error!("synchro WAL worker identity changed: {error}");
        }
        match run_worker_transaction(|| initialize_worker(&database, identity.session_login_oid)) {
            Ok(()) => break,
            Err(error) => {
                if let Err(identity_error) =
                    validate_worker_startup_authorization(&worker_login, &identity)
                {
                    log!("synchro WAL worker identity changed: {identity_error}");
                    pgrx::error!("synchro WAL worker identity changed: {identity_error}");
                }
                if startup_retry_exhausted(initialization_started_at) {
                    log!(
                        "synchro WAL worker initialization failed after {} seconds: {error}",
                        STARTUP_RETRY_BUDGET.as_secs()
                    );
                    pgrx::error!(
                        "synchro WAL worker initialization failed after {} seconds: {error}",
                        STARTUP_RETRY_BUDGET.as_secs()
                    );
                }
            }
        }
        if !BackgroundWorker::wait_latch(Some(STARTUP_RETRY_WAIT)) {
            return;
        }
    }

    let mut decoder_failure_logged = false;
    let mut decoder_state = loop {
        if let Err(error) = validate_worker_authorization(&worker_login, &identity) {
            log!("synchro WAL worker identity changed: {error}");
            pgrx::error!("synchro WAL worker identity changed: {error}");
        }
        match fresh_decoder() {
            Ok(state) => break state,
            Err(error) => {
                if !decoder_failure_logged {
                    log!("synchro WAL decoder initialization blocked: {error}");
                    decoder_failure_logged = true;
                }
                let _ = heartbeat("blocked");
                if !BackgroundWorker::wait_latch(Some(std::time::Duration::from_secs(1))) {
                    return;
                }
            }
        }
    };
    let mut candidate_decoder_state = None;
    let mut candidate_failure_logged = false;
    let mut transient_failure_logged = false;
    let mut poll_gate_failure_logged = false;

    loop {
        if let Err(error) = validate_worker_authorization(&worker_login, &identity) {
            log!("synchro WAL worker identity changed: {error}");
            pgrx::error!("synchro WAL worker identity changed: {error}");
        }
        if !BackgroundWorker::wait_latch(Some(std::time::Duration::from_millis(IDLE_POLL_MS))) {
            break;
        }
        if let Err(error) = acquire_worker_poll_gate() {
            if !poll_gate_failure_logged {
                log!("synchro WAL poll gate blocked: {error}");
                poll_gate_failure_logged = true;
            }
            let _ = heartbeat("blocked");
            continue;
        }
        poll_gate_failure_logged = false;
        poll_worker_once(
            &mut decoder_state,
            &mut candidate_decoder_state,
            &mut candidate_failure_logged,
            &mut transient_failure_logged,
            identity.worker_role_oid,
        );
        if let Err(error) = release_worker_poll_gate() {
            log!("synchro WAL poll gate release failed: {error}");
            pgrx::error!("synchro WAL poll gate release failed: {error}");
        }
    }

    let _ = heartbeat("stopped");
}

fn poll_worker_once(
    decoder_state: &mut DecoderState,
    candidate_decoder_state: &mut Option<CandidateDecoderState>,
    candidate_failure_logged: &mut bool,
    transient_failure_logged: &mut bool,
    worker_role_oid: pg_sys::Oid,
) {
    let mut blocked = false;
    match validated_runtime_capture_identity() {
        Ok(identity) if identity != decoder_state.identity => {
            let refreshed = retire_prior_generation_poison_for_worker(&identity.stream_generation)
                .and_then(|_| fresh_decoder_for(identity));
            match refreshed {
                Ok(fresh) => *decoder_state = fresh,
                Err(error) => {
                    if !*transient_failure_logged {
                        log!("synchro WAL runtime decoder refresh blocked: {error}");
                        *transient_failure_logged = true;
                    }
                    blocked = true;
                }
            }
        }
        Ok(_) => {}
        Err(error) => {
            if !*transient_failure_logged {
                log!("synchro WAL runtime identity blocked: {error}");
                *transient_failure_logged = true;
            }
            blocked = true;
        }
    }
    if !blocked
        && active_poison_state(&decoder_state.identity.stream_generation)
            .unwrap_or((true, false))
            .0
    {
        blocked = true;
        if retry_requested(&decoder_state.identity.stream_generation).unwrap_or(false) {
            if let Ok(fresh) = fresh_decoder() {
                *decoder_state = fresh;
                blocked = false;
            }
        }
    }

    if !blocked {
        match poll_and_process(
            &mut decoder_state.decoder,
            &decoder_state.identity.slot_name,
            worker_role_oid,
        ) {
            Ok(_) => *transient_failure_logged = false,
            Err(PollFailure::Poison(failure)) => {
                let failure_class = failure.class;
                if let Err(error) = persist_poison(failure) {
                    log!("synchro WAL poison persistence failed for {failure_class}: {error}");
                }
                blocked = true;
            }
            Err(PollFailure::Transient(stage)) => {
                if !*transient_failure_logged {
                    log!("synchro WAL poll blocked by transient {stage} failure");
                    *transient_failure_logged = true;
                }
                blocked = true;
            }
            Err(PollFailure::ActivationBarrier) => {
                if !*transient_failure_logged {
                    log!("synchro WAL poll blocked at a registry activation barrier");
                    *transient_failure_logged = true;
                }
                blocked = true;
                if let Ok(fresh) = fresh_decoder() {
                    *decoder_state = fresh;
                }
            }
        }
    }

    match poll_candidate_once(candidate_decoder_state, worker_role_oid) {
        Ok(()) => *candidate_failure_logged = false,
        Err(error) => {
            if !*candidate_failure_logged {
                log!("synchro projection bootstrap candidate failed: {error}");
                *candidate_failure_logged = true;
            }
            *candidate_decoder_state = None;
            blocked = true;
        }
    }
    let _ = heartbeat(if blocked { "blocked" } else { "running" });
}

fn acquire_worker_poll_gate() -> Result<(), String> {
    run_worker_transaction(|| {
        Spi::connect(|client| {
            client
                .select(
                    "SELECT pg_catalog.pg_advisory_lock_shared($1::bigint)",
                    None,
                    &[crate::WAL_WORKER_GATE_LOCK_KEY.into()],
                )
                .map_err(|_| "locking WAL worker poll failed".to_string())?;
            Ok(())
        })
    })
}

fn release_worker_poll_gate() -> Result<(), String> {
    run_worker_transaction(|| {
        Spi::connect(|client| {
            let released = client
                .select(
                    "SELECT pg_catalog.pg_advisory_unlock_shared($1::bigint) AS released",
                    None,
                    &[crate::WAL_WORKER_GATE_LOCK_KEY.into()],
                )
                .map_err(|_| "unlocking WAL worker poll failed".to_string())?
                .first()
                .get_by_name::<bool, &str>("released")
                .map_err(|_| "reading WAL worker poll unlock failed".to_string())?
                .unwrap_or(false);
            if !released {
                return Err("WAL worker poll lock was not held".to_string());
            }
            Ok(())
        })
    })
}

#[pg_extern]
fn synchro_retry_wal_poison() -> bool {
    Spi::connect_mut(|client| {
        let updated = client
            .update(
                "UPDATE synchro.sync_wal_poison
                 SET retry_requested_at = now()
                 WHERE lifecycle = 'active'
                   AND stream_generation = (
                       SELECT stream_generation
                       FROM synchro.sync_runtime_state
                       WHERE singleton
                   )
                   AND failure_class <> 'truncate_unsupported'",
                None,
                &[],
            )
            .unwrap_or_else(|_| pgrx::error!("requesting WAL poison retry failed"));
        updated.len() == 1
    })
}

fn validate_worker_authorization(
    worker_login: &str,
    expected_identity: &WorkerIdentity,
) -> Result<(), String> {
    run_worker_transaction(|| {
        Spi::connect(|client| {
            validate_worker_runtime_identity(client, &expected_identity.startup_runtime.runtime)?;
            let (session_login_oid, worker_role_oid) =
                validated_worker_identity(client, worker_login)?;
            if session_login_oid != expected_identity.session_login_oid
                || worker_role_oid != expected_identity.worker_role_oid
            {
                return Err("worker group role identity changed".to_string());
            }
            Ok(())
        })
    })
}

fn validate_worker_startup_authorization(
    worker_login: &str,
    expected_identity: &WorkerIdentity,
) -> Result<(), String> {
    run_worker_transaction(|| {
        Spi::connect(|client| {
            validate_worker_startup_identity(client, &expected_identity.startup_runtime)?;
            let (session_login_oid, worker_role_oid) =
                validated_worker_identity(client, worker_login)?;
            if session_login_oid != expected_identity.session_login_oid
                || worker_role_oid != expected_identity.worker_role_oid
            {
                return Err("worker group role identity changed".to_string());
            }
            Ok(())
        })
    })
}

fn validate_worker_preparation_identity(
    worker_login: &str,
    expected_identity: &WorkerStartupIdentity,
) -> Result<(), String> {
    run_worker_transaction(|| {
        Spi::connect_mut(|client| {
            let (_, worker_role_oid) = validated_worker_identity(client, worker_login)?;
            activate_worker_role(worker_role_oid);
            let result = validate_worker_startup_identity(client, expected_identity);
            activate_session_login();
            result
        })
    })
}

fn validated_worker_identity(
    client: &SpiClient<'_>,
    worker_login: &str,
) -> Result<(i64, pg_sys::Oid), String> {
    let validation = crate::health::validate_worker_login(client, worker_login)?;
    if !validation.is_valid() {
        return Err("worker login authorization is invalid".to_string());
    }
    let row = client
        .select(
            "SELECT session_role.oid::bigint AS session_user_oid,
                    worker_role.oid::bigint AS worker_role_oid
             FROM pg_catalog.pg_roles session_role
             CROSS JOIN pg_catalog.pg_roles worker_role
             WHERE session_role.rolname = session_user
               AND worker_role.rolname = 'synchro_worker'",
            None,
            &[],
        )
        .map_err(|_| "validating worker session failed".to_string())?
        .first();
    let session_user_oid = row
        .get_by_name::<i64, &str>("session_user_oid")
        .map_err(|_| "reading worker session validation failed".to_string())?
        .ok_or_else(|| "worker session authorization is invalid".to_string())?;
    if validation.worker_login_oid != Some(session_user_oid) {
        return Err("worker session authorization is invalid".to_string());
    }
    let worker_role_oid = row
        .get_by_name::<i64, &str>("worker_role_oid")
        .map_err(|_| "reading worker role validation failed".to_string())?
        .and_then(|oid| u32::try_from(oid).ok())
        .map(pg_sys::Oid::from)
        .filter(|oid| *oid != pg_sys::InvalidOid)
        .ok_or_else(|| "worker group role is invalid".to_string())?;
    Ok((session_user_oid, worker_role_oid))
}

fn activate_worker_role(worker_role_oid: pg_sys::Oid) {
    // SAFETY: Catalog validation proved membership and all negative role attributes.
    unsafe { pg_sys::SetCurrentRoleId(worker_role_oid, false) };
}

fn activate_session_login() {
    // SAFETY: PostgreSQL defines InvalidOid as SET ROLE NONE for the session user.
    unsafe { pg_sys::SetCurrentRoleId(pg_sys::InvalidOid, false) };
}

fn prepare_worker(database: &str, worker_login: &str) -> Result<WorkerIdentity, String> {
    let configured_slot = configured_replication_slot();
    Spi::connect_mut(|client| {
        let (session_login_oid, worker_role_oid) = validated_worker_identity(client, worker_login)?;
        let (_, connected_database) = connected_database(client, database)?;
        activate_worker_role(worker_role_oid);
        let runtime = capture_worker_startup_identity(client, &configured_slot);
        activate_session_login();
        let runtime = runtime?;
        ensure_slot(
            client,
            &runtime.runtime.slot_name,
            &connected_database,
            runtime.active_slot_is_unbound,
        )?;
        Ok(WorkerIdentity {
            session_login_oid,
            worker_role_oid,
            startup_runtime: runtime,
        })
    })
}

fn capture_worker_preparation_identity(
    database: &str,
    worker_login: &str,
) -> Result<WorkerStartupIdentity, String> {
    let configured_slot = configured_replication_slot();
    Spi::connect_mut(|client| {
        let (_, worker_role_oid) = validated_worker_identity(client, worker_login)?;
        let _ = connected_database(client, database)?;
        activate_worker_role(worker_role_oid);
        let runtime = capture_worker_startup_identity(client, &configured_slot);
        activate_session_login();
        runtime
    })
}

fn initialize_worker(database: &str, worker_login_oid: i64) -> Result<(), String> {
    let configured_slot = configured_replication_slot();
    let publication = publication_name();
    Spi::connect_mut(|client| {
        let (database_oid, connected_database) = connected_database(client, database)?;
        ensure_publication(client, &publication)?;
        let slot = effective_slot_name(client, &configured_slot)?;
        client
            .update(
                "UPDATE synchro.sync_runtime_state
                  SET active_slot_name = $1, updated_at = now()
                  WHERE singleton = true AND active_slot_name IS NULL",
                None,
                &[slot.as_str().into()],
            )
            .map_err(|_| "storing active slot failed".to_string())?;
        let stream_generation = active_stream_generation(client)?;
        retire_prior_generation_poison(client, &stream_generation)?;
        client
            .update(
                "UPDATE synchro.sync_wal_progress progress
                 SET generation_start_lsn = slot.confirmed_flush_lsn, updated_at = now()
                 FROM synchro.sync_runtime_state runtime
                 JOIN pg_catalog.pg_replication_slots slot
                   ON slot.slot_name = runtime.active_slot_name
                 WHERE progress.singleton AND runtime.singleton
                   AND progress.generation_start_lsn IS NULL
                   AND progress.materialized_end_lsn IS NULL
                   AND progress.acknowledged_end_lsn IS NULL
                   AND slot.slot_type = 'logical'
                   AND slot.confirmed_flush_lsn IS NOT NULL",
                None,
                &[],
            )
            .map_err(|_| "storing stream generation boundary failed".to_string())?;
        client
            .update(
                "INSERT INTO synchro.sync_wal_worker_state (
                     worker_id, database_oid, database_name, worker_login_oid,
                     backend_pid, state,
                     registry_generation, materialized_commit_lsn, materialized_end_lsn,
                     heartbeat_at, updated_at
                 )
                 SELECT $1, $2::oid, $3, $4::oid, pg_backend_pid(), 'starting', registry_generation,
                        materialized_commit_lsn, materialized_end_lsn, now(), now()
                 FROM synchro.sync_wal_progress
                 WHERE singleton = true
                 ON CONFLICT (worker_id) DO UPDATE SET
                     database_oid = EXCLUDED.database_oid,
                     database_name = EXCLUDED.database_name,
                     worker_login_oid = EXCLUDED.worker_login_oid,
                     backend_pid = EXCLUDED.backend_pid,
                     state = EXCLUDED.state,
                     registry_generation = EXCLUDED.registry_generation,
                     materialized_commit_lsn = EXCLUDED.materialized_commit_lsn,
                     materialized_end_lsn = EXCLUDED.materialized_end_lsn,
                     heartbeat_at = EXCLUDED.heartbeat_at,
                     updated_at = now()",
                None,
                &[
                    WORKER_ID.into(),
                    database_oid.into(),
                    connected_database.as_str().into(),
                    worker_login_oid.into(),
                ],
            )
            .map_err(|_| "storing worker state failed".to_string())?;
        Ok(())
    })
}

fn connected_database(
    client: &SpiClient<'_>,
    expected_database: &str,
) -> Result<(i64, String), String> {
    let row = client
        .select(
            "SELECT oid::bigint AS database_oid, current_database()::text AS database_name
             FROM pg_catalog.pg_database
             WHERE datname = current_database()",
            None,
            &[],
        )
        .map_err(|_| "loading configured database failed".to_string())?
        .first();
    let database_oid = row
        .get_by_name::<i64, &str>("database_oid")
        .map_err(|_| "loading configured database failed".to_string())?
        .ok_or_else(|| "configured database is unavailable".to_string())?;
    let connected_database = row
        .get_by_name::<String, &str>("database_name")
        .map_err(|_| "loading configured database failed".to_string())?
        .ok_or_else(|| "configured database is unavailable".to_string())?;
    if connected_database != expected_database {
        return Err("worker connected to the wrong database".to_string());
    }
    Ok((database_oid, connected_database))
}

pub(crate) fn effective_slot_name(
    client: &SpiClient<'_>,
    configured_slot: &str,
) -> Result<String, String> {
    capture_worker_runtime_identity(client, configured_slot).map(|(identity, _)| identity.slot_name)
}

pub(crate) fn capture_worker_startup_identity(
    client: &SpiClient<'_>,
    configured_slot: &str,
) -> Result<WorkerStartupIdentity, String> {
    capture_worker_runtime_identity(client, configured_slot).map(
        |(runtime, active_slot_is_unbound)| WorkerStartupIdentity {
            runtime,
            active_slot_is_unbound,
        },
    )
}

pub(crate) fn capture_worker_runtime_identity(
    client: &SpiClient<'_>,
    configured_slot: &str,
) -> Result<(WorkerRuntimeIdentity, bool), String> {
    let row = client
        .select(
            "SELECT stream_generation,
                    active_slot_name::text AS active_slot_name
             FROM synchro.sync_runtime_state WHERE singleton = true",
            None,
            &[],
        )
        .map_err(|_| "loading active replication slot failed".to_string())?
        .first();
    let stream_generation = row
        .get_by_name::<String, &str>("stream_generation")
        .map_err(|_| "loading active replication slot failed".to_string())?
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "active stream generation is unavailable".to_string())?;
    let active_slot_name = row
        .get_by_name::<String, &str>("active_slot_name")
        .map_err(|_| "loading active replication slot failed".to_string())?;
    let (slot_name, bootstrap) = match active_slot_name {
        Some(slot) if !slot.is_empty() => (slot, false),
        Some(_) => return Err("active replication slot is invalid".to_string()),
        None if !configured_slot.is_empty() => (configured_slot.to_string(), true),
        None => return Err("configured replication slot is invalid".to_string()),
    };
    Ok((
        WorkerRuntimeIdentity {
            stream_generation,
            slot_name,
        },
        bootstrap,
    ))
}

pub(crate) fn validate_worker_runtime_identity(
    client: &SpiClient<'_>,
    identity: &WorkerRuntimeIdentity,
) -> Result<(), String> {
    validate_worker_identity(client, identity, false)
}

pub(crate) fn validate_worker_startup_identity(
    client: &SpiClient<'_>,
    identity: &WorkerStartupIdentity,
) -> Result<(), String> {
    validate_worker_identity(client, &identity.runtime, identity.active_slot_is_unbound)
}

fn validate_worker_identity(
    client: &SpiClient<'_>,
    identity: &WorkerRuntimeIdentity,
    active_slot_is_unbound: bool,
) -> Result<(), String> {
    let valid = client
        .select(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_namespace namespace
                 WHERE namespace.nspname = 'synchro'
                   AND (
                       SELECT count(*)
                       FROM pg_catalog.pg_class class
                       WHERE class.relnamespace = namespace.oid
                         AND class.relkind IN ('r', 'p')
                         AND class.relname IN (
                             'sync_registry',
                             'sync_registry_generations',
                             'sync_runtime_state',
                             'sync_stream_resets',
                             'sync_wal_poison',
                             'sync_wal_progress',
                             'sync_wal_worker_state'
                         )
                   ) = 7
                   AND EXISTS (
                       SELECT 1
                        FROM synchro.sync_runtime_state runtime
                        WHERE runtime.singleton
                          AND runtime.stream_generation = $1
                          AND (
                              ($3 AND runtime.active_slot_name IS NULL)
                              OR (NOT $3 AND runtime.active_slot_name::text = $2)
                          )
                    )
              ) AS valid",
            None,
            &[
                identity.stream_generation.as_str().into(),
                identity.slot_name.as_str().into(),
                active_slot_is_unbound.into(),
            ],
        )
        .map_err(|_| "validating worker runtime identity failed".to_string())?
        .first()
        .get_by_name::<bool, &str>("valid")
        .map_err(|_| "validating worker runtime identity failed".to_string())?
        .unwrap_or(false);
    if !valid {
        return Err("worker runtime identity changed".to_string());
    }
    Ok(())
}

fn validated_runtime_capture_identity() -> Result<RuntimeCaptureIdentity, String> {
    run_worker_transaction(|| {
        Spi::connect(|client| {
            let row = client
                .select(
                    "SELECT runtime.stream_generation,
                            runtime.active_slot_name::text AS active_slot_name,
                            progress.registry_generation
                     FROM synchro.sync_runtime_state runtime
                     JOIN synchro.sync_wal_progress progress ON progress.singleton
                     WHERE runtime.singleton",
                    None,
                    &[],
                )
                .map_err(|_| "loading active capture identity failed".to_string())?
                .first();
            let identity = RuntimeCaptureIdentity {
                stream_generation: row
                    .get_by_name::<String, &str>("stream_generation")
                    .map_err(|_| "loading active capture identity failed".to_string())?
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| "active stream generation is unavailable".to_string())?,
                slot_name: row
                    .get_by_name::<String, &str>("active_slot_name")
                    .map_err(|_| "loading active capture identity failed".to_string())?
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| "active replication slot is unavailable".to_string())?,
                registry_generation: row
                    .get_by_name::<i64, &str>("registry_generation")
                    .map_err(|_| "loading active capture identity failed".to_string())?
                    .filter(|value| *value > 0)
                    .ok_or_else(|| "active registry generation is unavailable".to_string())?,
            };
            validate_runtime_capture_identity(client, &identity)?;
            Ok(identity)
        })
    })
}

fn validate_runtime_capture_identity(
    client: &SpiClient<'_>,
    identity: &RuntimeCaptureIdentity,
) -> Result<(), String> {
    let valid = client
        .select(
            "SELECT EXISTS (
                 SELECT 1
                  FROM synchro.sync_runtime_state runtime
                   JOIN synchro.sync_wal_progress progress ON progress.singleton
                   JOIN synchro.sync_registry_generations registry
                     ON registry.generation = progress.registry_generation
                    AND registry.state = 'active'
                    AND registry.validated
                    AND registry.stream_generation = runtime.stream_generation
                   JOIN pg_catalog.pg_replication_slots slot
                     ON slot.slot_name = runtime.active_slot_name
                  JOIN pg_catalog.pg_database database
                    ON database.oid = slot.datoid
                  AND database.datname = pg_catalog.current_database()
                 WHERE runtime.singleton
                   AND runtime.stream_generation = $1
                   AND runtime.active_slot_name::text = $2
                   AND progress.stream_generation = $1
                   AND progress.registry_generation = $3
                   AND slot.slot_type = 'logical'
                   AND slot.plugin = 'pgoutput'
                   AND NOT slot.temporary
                   AND slot.invalidation_reason IS NULL
                   AND slot.wal_status IS DISTINCT FROM 'lost'
                   AND slot.restart_lsn IS NOT NULL
                   AND slot.confirmed_flush_lsn IS NOT NULL
             ) AS valid",
            None,
            &[
                identity.stream_generation.as_str().into(),
                identity.slot_name.as_str().into(),
                identity.registry_generation.into(),
            ],
        )
        .map_err(|_| "validating active replication slot failed".to_string())?
        .first()
        .get_by_name::<bool, &str>("valid")
        .map_err(|_| "validating active replication slot failed".to_string())?
        .unwrap_or(false);
    if !valid {
        return Err("active replication slot is invalid".to_string());
    }
    Ok(())
}

fn ensure_slot(
    client: &mut SpiClient<'_>,
    slot: &str,
    connected_database: &str,
    allow_create: bool,
) -> Result<(), String> {
    let rows = client
        .select(
            "SELECT plugin::text AS plugin, database::text AS database_name,
                    slot_type::text AS slot_type, temporary,
                    invalidation_reason, wal_status::text AS wal_status,
                    restart_lsn IS NOT NULL AS has_restart_lsn,
                    confirmed_flush_lsn IS NOT NULL AS has_confirmed_flush_lsn
             FROM pg_catalog.pg_replication_slots
             WHERE slot_name = $1",
            None,
            &[slot.into()],
        )
        .map_err(|_| "checking replication slot failed".to_string())?;
    if let Some(row) = rows.into_iter().next() {
        let plugin = row
            .get_by_name::<String, &str>("plugin")
            .map_err(|_| "checking replication slot failed".to_string())?
            .unwrap_or_default();
        let database = row
            .get_by_name::<String, &str>("database_name")
            .map_err(|_| "checking replication slot failed".to_string())?
            .unwrap_or_default();
        let slot_type = row
            .get_by_name::<String, &str>("slot_type")
            .map_err(|_| "checking replication slot failed".to_string())?
            .unwrap_or_default();
        let temporary = row
            .get_by_name::<bool, &str>("temporary")
            .map_err(|_| "checking replication slot failed".to_string())?
            .unwrap_or(true);
        let invalidation_reason = row
            .get_by_name::<String, &str>("invalidation_reason")
            .map_err(|_| "checking replication slot failed".to_string())?;
        let wal_status = row
            .get_by_name::<String, &str>("wal_status")
            .map_err(|_| "checking replication slot failed".to_string())?;
        let has_restart_lsn = row
            .get_by_name::<bool, &str>("has_restart_lsn")
            .map_err(|_| "checking replication slot failed".to_string())?
            .unwrap_or(false);
        let has_confirmed_flush_lsn = row
            .get_by_name::<bool, &str>("has_confirmed_flush_lsn")
            .map_err(|_| "checking replication slot failed".to_string())?
            .unwrap_or(false);
        if plugin != "pgoutput"
            || database != connected_database
            || slot_type != "logical"
            || temporary
            || invalidation_reason.is_some()
            || wal_status.as_deref() == Some("lost")
            || !has_restart_lsn
            || !has_confirmed_flush_lsn
        {
            return Err("configured replication slot is incompatible".to_string());
        }
        return Ok(());
    }

    if !allow_create {
        return Err("active replication slot is unavailable".to_string());
    }

    client
        .select(
            "SELECT pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput')",
            None,
            &[slot.into()],
        )
        .map_err(|_| "creating replication slot failed".to_string())?;
    Ok(())
}

fn ensure_publication(client: &mut SpiClient<'_>, publication: &str) -> Result<(), String> {
    let rows = client
        .select(
            "SELECT puballtables FROM pg_catalog.pg_publication WHERE pubname = $1",
            None,
            &[publication.into()],
        )
        .map_err(|_| "checking publication failed".to_string())?;
    if let Some(row) = rows.into_iter().next() {
        if row
            .get_by_name::<bool, &str>("puballtables")
            .map_err(|_| "checking publication failed".to_string())?
            .unwrap_or(true)
        {
            return Err("configured publication must be explicit".to_string());
        }
        return Ok(());
    }

    Err("configured publication is unavailable".to_string())
}

fn fresh_decoder() -> Result<DecoderState, String> {
    let identity = validated_runtime_capture_identity()?;
    fresh_decoder_for(identity)
}

fn fresh_decoder_for(identity: RuntimeCaptureIdentity) -> Result<DecoderState, String> {
    run_worker_transaction(|| {
        Spi::connect_mut(|client| {
            validate_runtime_capture_identity(client, &identity)?;
            let registry =
                load_registry_generation_for_worker(client, identity.registry_generation)
                    .map_err(|error| format!("loading active registry failed: {error}"))?;
            let mut decoder = WalDecoder::new();
            decoder.preload_relations(preload_relations(client, &registry)?);
            Ok(DecoderState { identity, decoder })
        })
    })
}

fn poll_candidate_once(
    decoder_state: &mut Option<CandidateDecoderState>,
    worker_role_oid: pg_sys::Oid,
) -> Result<(), String> {
    let Some(bootstrap) = load_candidate_bootstrap()? else {
        *decoder_state = None;
        return Ok(());
    };
    validate_candidate_slot_boundary(&bootstrap, worker_role_oid)?;
    if decoder_state
        .as_ref()
        .is_none_or(|state| state.identity != bootstrap.identity)
    {
        *decoder_state = Some(fresh_candidate_decoder(&bootstrap)?);
    }
    let state = decoder_state
        .as_mut()
        .ok_or_else(|| "candidate decoder is unavailable".to_string())?;
    poll_candidate_and_process(&mut state.decoder, &bootstrap, worker_role_oid)
}

fn load_candidate_bootstrap() -> Result<Option<CandidateBootstrap>, String> {
    run_worker_transaction(|| {
        Spi::connect(|client| {
            let rows = client
                .select(
                    "SELECT reset.reset_id::text AS bootstrap_id,
                            reset.candidate_slot_name::text AS candidate_slot_name,
                            reset.source_stream_generation,
                            reset.source_registry_generation,
                            reset.target_registry_generation,
                            reset.consistent_point::text AS consistent_point,
                            reset.activation_barrier::text AS activation_barrier,
                            COALESCE(
                                reset.candidate_acknowledged_end_lsn,
                                reset.consistent_point
                            )::text AS acknowledged_end_lsn
                     FROM synchro.sync_stream_resets reset
                     JOIN synchro.sync_runtime_state runtime
                       ON runtime.singleton
                      AND runtime.stream_generation = reset.source_stream_generation
                      AND runtime.active_slot_name::text = reset.old_slot_name::text
                     JOIN synchro.sync_wal_progress progress
                       ON progress.singleton
                      AND progress.stream_generation = reset.source_stream_generation
                      AND progress.registry_generation = reset.source_registry_generation
                      JOIN synchro.sync_registry_generations target
                        ON target.generation = reset.target_registry_generation
                       AND target.stream_generation = reset.source_stream_generation
                      AND target.state = 'pending'
                      AND target.validated
                     JOIN pg_catalog.pg_database database
                       ON database.oid = reset.database_oid
                      AND database.datname::text = reset.database_name::text
                      AND database.datname = pg_catalog.current_database()
                     WHERE reset.operation_kind = 'projection_bootstrap'
                       AND reset.lifecycle = 'catching_up'
                       AND reset.plugin = 'pgoutput'
                       AND reset.consistent_point IS NOT NULL
                       AND reset.activation_barrier IS NOT NULL
                     ORDER BY reset.reset_id
                     LIMIT 2",
                    None,
                    &[],
                )
                .map_err(|_| "loading candidate bootstrap failed".to_string())?;
            if rows.len() > 1 {
                return Err("multiple candidate bootstraps are catching up".to_string());
            }
            let Some(row) = rows.into_iter().next() else {
                return Ok(None);
            };
            let bootstrap_id = row
                .get_by_name::<String, &str>("bootstrap_id")
                .map_err(|_| "reading candidate bootstrap failed".to_string())?
                .filter(|value| !value.is_empty())
                .ok_or_else(|| "candidate bootstrap identity is unavailable".to_string())?;
            let slot_name = row
                .get_by_name::<String, &str>("candidate_slot_name")
                .map_err(|_| "reading candidate bootstrap failed".to_string())?
                .filter(|value| !value.is_empty())
                .ok_or_else(|| "candidate slot identity is unavailable".to_string())?;
            let source_stream_generation = row
                .get_by_name::<String, &str>("source_stream_generation")
                .map_err(|_| "reading candidate bootstrap failed".to_string())?
                .filter(|value| !value.is_empty())
                .ok_or_else(|| "candidate stream identity is unavailable".to_string())?;
            let source_registry_generation = row
                .get_by_name::<i64, &str>("source_registry_generation")
                .map_err(|_| "reading candidate bootstrap failed".to_string())?
                .filter(|value| *value > 0)
                .ok_or_else(|| "candidate source registry is unavailable".to_string())?;
            let registry_generation = row
                .get_by_name::<i64, &str>("target_registry_generation")
                .map_err(|_| "reading candidate bootstrap failed".to_string())?
                .filter(|value| *value > source_registry_generation)
                .ok_or_else(|| "candidate target registry is unavailable".to_string())?;
            let consistent_point = required_lsn(&row, "consistent_point")?;
            let activation_barrier = required_lsn(&row, "activation_barrier")?;
            let acknowledged_end_lsn = required_lsn(&row, "acknowledged_end_lsn")?;
            if acknowledged_end_lsn < consistent_point
                || acknowledged_end_lsn > activation_barrier
                || activation_barrier < consistent_point
            {
                return Err("candidate bootstrap boundary is invalid".to_string());
            }
            Ok(Some(CandidateBootstrap {
                identity: CandidateCaptureIdentity {
                    bootstrap_id,
                    slot_name,
                    registry_generation,
                },
                source_stream_generation,
                source_registry_generation,
                consistent_point,
                activation_barrier,
                acknowledged_end_lsn,
            }))
        })
    })
}

fn required_lsn(row: &impl CandidateRow, name: &str) -> Result<u64, String> {
    row.candidate_text(name)
        .map_err(|_| "reading candidate bootstrap boundary failed".to_string())?
        .and_then(|value| parse_lsn(&value))
        .ok_or_else(|| "candidate bootstrap boundary is unavailable".to_string())
}

fn fresh_candidate_decoder(
    bootstrap: &CandidateBootstrap,
) -> Result<CandidateDecoderState, String> {
    let identity = bootstrap.identity.clone();
    run_worker_transaction(|| {
        Spi::connect_mut(|client| {
            validate_candidate_binding(client, bootstrap, false)?;
            let registry =
                load_registry_generation_from_client(client, identity.registry_generation)
                    .map_err(|_| "loading candidate registry failed".to_string())?;
            let mut decoder = WalDecoder::new();
            decoder.preload_relations(preload_relations(client, &registry)?);
            Ok(CandidateDecoderState { identity, decoder })
        })
    })
}

fn validate_candidate_slot_boundary(
    bootstrap: &CandidateBootstrap,
    worker_role_oid: pg_sys::Oid,
) -> Result<(), String> {
    run_replication_transaction(worker_role_oid, || {
        Spi::connect_mut(|client| {
            let valid = client
                .select(
                    "SELECT EXISTS (
                         SELECT 1
                         FROM pg_catalog.pg_replication_slots slot
                         JOIN pg_catalog.pg_database database ON database.oid = slot.datoid
                         WHERE slot.slot_name = $1
                           AND slot.slot_type = 'logical'
                           AND slot.plugin = 'pgoutput'
                           AND database.datname = pg_catalog.current_database()
                           AND NOT slot.temporary
                           AND slot.invalidation_reason IS NULL
                           AND slot.wal_status IS DISTINCT FROM 'lost'
                           AND slot.restart_lsn IS NOT NULL
                           AND slot.restart_lsn <= $2::pg_lsn
                           AND slot.confirmed_flush_lsn = $3::pg_lsn
                     ) AS valid",
                    None,
                    &[
                        bootstrap.identity.slot_name.as_str().into(),
                        format_lsn(bootstrap.consistent_point).as_str().into(),
                        format_lsn(bootstrap.acknowledged_end_lsn).as_str().into(),
                    ],
                )
                .map_err(|_| "validating candidate slot failed".to_string())?
                .first()
                .get_by_name::<bool, &str>("valid")
                .map_err(|_| "validating candidate slot failed".to_string())?
                .unwrap_or(false);
            if !valid {
                return Err("candidate slot boundary is invalid".to_string());
            }
            activate_worker_role(worker_role_oid);
            validate_candidate_binding(client, bootstrap, false)
        })
    })
}

fn validate_candidate_binding(
    client: &mut SpiClient<'_>,
    bootstrap: &CandidateBootstrap,
    for_update: bool,
) -> Result<(), String> {
    if for_update {
        let locked = client
            .update(
                "SELECT 1 AS locked
                 FROM synchro.sync_stream_resets
                 WHERE reset_id = $1::uuid
                 FOR UPDATE",
                None,
                &[bootstrap.identity.bootstrap_id.as_str().into()],
            )
            .map_err(|_| "locking candidate bootstrap failed".to_string())?
            .len();
        if locked != 1 {
            return Err("candidate bootstrap is unavailable".to_string());
        }
    }
    let query = "SELECT EXISTS (
             SELECT 1
             FROM synchro.sync_stream_resets reset
             JOIN synchro.sync_runtime_state runtime
               ON runtime.singleton
              AND runtime.stream_generation = reset.source_stream_generation
              AND runtime.active_slot_name::text = reset.old_slot_name::text
             JOIN synchro.sync_wal_progress progress
               ON progress.singleton
              AND progress.stream_generation = reset.source_stream_generation
              AND progress.registry_generation = reset.source_registry_generation
              JOIN synchro.sync_registry_generations target
                ON target.generation = reset.target_registry_generation
               AND target.state = 'pending'
              AND target.validated
              AND target.stream_generation = reset.source_stream_generation
             WHERE reset.reset_id = $1::uuid
               AND reset.operation_kind = 'projection_bootstrap'
               AND reset.lifecycle = 'catching_up'
               AND reset.candidate_slot_name::text = $2
               AND reset.source_stream_generation = $3
               AND reset.source_registry_generation = $4
               AND reset.target_registry_generation = $5
               AND reset.consistent_point = $6::pg_lsn
               AND reset.activation_barrier = $7::pg_lsn
               AND COALESCE(
                       reset.candidate_acknowledged_end_lsn,
                       reset.consistent_point
                   ) = $8::pg_lsn
         ) AS valid";
    let valid = client
        .select(
            query,
            None,
            &[
                bootstrap.identity.bootstrap_id.as_str().into(),
                bootstrap.identity.slot_name.as_str().into(),
                bootstrap.source_stream_generation.as_str().into(),
                bootstrap.source_registry_generation.into(),
                bootstrap.identity.registry_generation.into(),
                format_lsn(bootstrap.consistent_point).as_str().into(),
                format_lsn(bootstrap.activation_barrier).as_str().into(),
                format_lsn(bootstrap.acknowledged_end_lsn).as_str().into(),
            ],
        )
        .map_err(|_| "validating candidate bootstrap binding failed".to_string())?
        .first()
        .get_by_name::<bool, &str>("valid")
        .map_err(|_| "validating candidate bootstrap binding failed".to_string())?
        .unwrap_or(false);
    if !valid {
        return Err("candidate bootstrap binding changed".to_string());
    }
    Ok(())
}

fn poll_candidate_and_process(
    decoder: &mut WalDecoder,
    bootstrap: &CandidateBootstrap,
    worker_role_oid: pg_sys::Oid,
) -> Result<(), String> {
    if bootstrap.acknowledged_end_lsn == bootstrap.activation_barrier {
        return finalize_candidate(bootstrap);
    }
    let publication = publication_name();
    let messages = run_replication_transaction(worker_role_oid, || {
        peek_messages(&bootstrap.identity.slot_name, &publication)
    })?;
    if messages.is_empty() {
        return Ok(());
    }
    let mut transactions = Vec::new();
    for message in messages {
        let completed = decoder
            .feed(&message.data)
            .map_err(|_| "decoding candidate WAL failed".to_string())?;
        for transaction in &completed {
            if message
                .sql_xid
                .is_some_and(|sql_xid| sql_xid != transaction.xid)
            {
                return Err("candidate WAL transaction identity is invalid".to_string());
            }
        }
        transactions.extend(completed);
    }
    let mut previous = None;
    let mut current = clone_candidate_bootstrap(bootstrap);
    for transaction in transactions {
        if previous.is_some_and(|commit_lsn| commit_lsn >= transaction.commit_lsn) {
            return Err("candidate WAL transaction order is invalid".to_string());
        }
        previous = Some(transaction.commit_lsn);
        if transaction.end_lsn > bootstrap.activation_barrier {
            return Err("candidate WAL crossed the activation barrier".to_string());
        }
        materialize_candidate(&current, &transaction)?;
        advance_candidate_slot(&current, &transaction, worker_role_oid)?;
        current.acknowledged_end_lsn = transaction.end_lsn;
        if transaction.end_lsn == bootstrap.activation_barrier {
            return finalize_candidate(&current);
        }
    }
    Ok(())
}

fn clone_candidate_bootstrap(bootstrap: &CandidateBootstrap) -> CandidateBootstrap {
    CandidateBootstrap {
        identity: bootstrap.identity.clone(),
        source_stream_generation: bootstrap.source_stream_generation.clone(),
        source_registry_generation: bootstrap.source_registry_generation,
        consistent_point: bootstrap.consistent_point,
        activation_barrier: bootstrap.activation_barrier,
        acknowledged_end_lsn: bootstrap.acknowledged_end_lsn,
    }
}

fn materialize_candidate(
    bootstrap: &CandidateBootstrap,
    transaction: &WalTransaction,
) -> Result<(), String> {
    let commit_lsn = transaction.commit_lsn;
    run_worker_transaction(|| {
        Spi::connect_mut(|client| {
            validate_candidate_binding(client, bootstrap, true)?;
            select_candidate_projection(client, bootstrap)?;
            let registry = load_registry_generation_from_client(
                client,
                bootstrap.identity.registry_generation,
            )
            .map_err(|_| "loading candidate registry failed".to_string())?;
            let dependencies = load_membership_dependencies_from_client(
                client,
                bootstrap.identity.registry_generation,
                &registry,
            )
            .map_err(|_| "loading candidate membership dependencies failed".to_string())?;
            if !parse_registry_activations(transaction)
                .map_err(candidate_failure)?
                .is_empty()
            {
                return Err("candidate WAL contains an unexpected registry activation".to_string());
            }
            if transaction
                .truncates
                .iter()
                .any(|truncate| find_registration(&registry, &truncate.relation).is_some())
            {
                return Err("candidate WAL contains a registered truncate".to_string());
            }
            let fences = parse_fence_messages(transaction).map_err(candidate_failure)?;
            let applicable = correlate_events(
                client,
                transaction,
                &registry,
                &fences,
                FenceTarget::Candidate {
                    source_stream_generation: &bootstrap.source_stream_generation,
                    source_registry_generation: bootstrap.source_registry_generation,
                    target_registry_generation: bootstrap.identity.registry_generation,
                },
            )
            .map_err(candidate_failure)?;
            let content_hash = transaction_content_hash(transaction);
            if existing_candidate_transaction(
                client,
                bootstrap,
                transaction,
                &content_hash,
                &applicable,
            )? {
                return Ok(());
            }
            validate_candidate_progress(client, bootstrap, transaction)?;
            client
                .update(
                    "INSERT INTO synchro.sync_projection_bootstrap_transactions (
                         bootstrap_id, commit_lsn, end_lsn, source_xid,
                         registry_generation, event_count, content_hash,
                         materialized_at, replay_count
                     ) VALUES (
                         $1::uuid, $2::pg_lsn, $3::pg_lsn, $4::xid,
                         $5, $6, $7, now(), 0
                     )",
                    None,
                    &[
                        bootstrap.identity.bootstrap_id.as_str().into(),
                        format_lsn(transaction.commit_lsn).as_str().into(),
                        format_lsn(transaction.end_lsn).as_str().into(),
                        transaction.xid.to_string().as_str().into(),
                        bootstrap.identity.registry_generation.into(),
                        i64::try_from(applicable.len())
                            .map_err(|_| "candidate event count overflowed".to_string())?
                            .into(),
                        content_hash.to_vec().into(),
                    ],
                )
                .map_err(|_| "recording candidate transaction failed".to_string())?;
            let target = ProjectionTarget::Candidate {
                bootstrap_id: &bootstrap.identity.bootstrap_id,
                registry_generation: bootstrap.identity.registry_generation,
            };
            let persisted =
                persist_events_and_projections(client, target, transaction, &registry, &applicable)
                    .map_err(candidate_failure)?;
            let impacts = collect_membership_impacts(
                client,
                target,
                transaction,
                &registry,
                &dependencies,
                persisted,
            )
            .map_err(candidate_failure)?;
            materialize_impacts(client, target, transaction, &registry, impacts)
                .map_err(candidate_failure)?;
            let updated = client
                .update(
                    "UPDATE synchro.sync_stream_resets
                     SET candidate_materialized_commit_lsn = $2::pg_lsn,
                         candidate_materialized_end_lsn = $3::pg_lsn,
                         candidate_verified = false,
                         updated_at = now()
                     WHERE reset_id = $1::uuid
                       AND operation_kind = 'projection_bootstrap'
                       AND lifecycle = 'catching_up'
                       AND target_registry_generation = $4
                       AND activation_barrier >= $3::pg_lsn",
                    None,
                    &[
                        bootstrap.identity.bootstrap_id.as_str().into(),
                        format_lsn(transaction.commit_lsn).as_str().into(),
                        format_lsn(transaction.end_lsn).as_str().into(),
                        bootstrap.identity.registry_generation.into(),
                    ],
                )
                .map_err(|_| "updating candidate materialization failed".to_string())?
                .len();
            if updated != 1 {
                return Err("candidate bootstrap changed during materialization".to_string());
            }
            Ok(())
        })
    })
    .inspect_err(|_| {
        let _ = commit_lsn;
    })
}

fn candidate_failure(failure: PoisonFailure) -> String {
    match failure.class {
        "decode_failed" => "candidate WAL decoding failed",
        "fence_correlation_failed" => "candidate fence correlation failed",
        "projection_write_failed" => "candidate projection write failed",
        "scope_evaluation_failed" => "candidate scope evaluation failed",
        "registered_relation_drift" => "candidate registered relation drifted",
        _ => "candidate transaction validation failed",
    }
    .to_string()
}

fn validate_candidate_progress(
    client: &SpiClient<'_>,
    bootstrap: &CandidateBootstrap,
    transaction: &WalTransaction,
) -> Result<(), String> {
    let row = client
        .select(
            "SELECT COALESCE(
                        candidate_materialized_end_lsn,
                        consistent_point
                    )::text AS materialized_end_lsn,
                    activation_barrier::text AS activation_barrier
             FROM synchro.sync_stream_resets
             WHERE reset_id = $1::uuid
               AND operation_kind = 'projection_bootstrap'
               AND lifecycle = 'catching_up'
               AND target_registry_generation = $2",
            None,
            &[
                bootstrap.identity.bootstrap_id.as_str().into(),
                bootstrap.identity.registry_generation.into(),
            ],
        )
        .map_err(|_| "loading candidate progress failed".to_string())?
        .first();
    let prior_end = required_lsn(&row, "materialized_end_lsn")?;
    let barrier = required_lsn(&row, "activation_barrier")?;
    if prior_end != bootstrap.acknowledged_end_lsn
        || transaction.end_lsn <= prior_end
        || transaction.commit_lsn < prior_end
        || transaction.end_lsn > barrier
    {
        return Err("candidate transaction order is invalid".to_string());
    }
    Ok(())
}

fn existing_candidate_transaction(
    client: &mut SpiClient<'_>,
    bootstrap: &CandidateBootstrap,
    transaction: &WalTransaction,
    content_hash: &[u8; 32],
    events: &[ApplicableEvent<'_>],
) -> Result<bool, String> {
    let rows = client
        .select(
            "SELECT end_lsn::text AS end_lsn, source_xid::text AS source_xid,
                    registry_generation, event_count, content_hash
             FROM synchro.sync_projection_bootstrap_transactions
             WHERE bootstrap_id = $1::uuid AND commit_lsn = $2::pg_lsn",
            None,
            &[
                bootstrap.identity.bootstrap_id.as_str().into(),
                format_lsn(transaction.commit_lsn).as_str().into(),
            ],
        )
        .map_err(|_| "loading candidate transaction failed".to_string())?;
    let Some(row) = rows.into_iter().next() else {
        return Ok(false);
    };
    let end_lsn = required_lsn(&row, "end_lsn")?;
    let source_xid = row
        .get_by_name::<String, &str>("source_xid")
        .map_err(|_| "reading candidate transaction failed".to_string())?
        .and_then(|value| value.parse::<u32>().ok());
    let registry_generation = row
        .get_by_name::<i64, &str>("registry_generation")
        .map_err(|_| "reading candidate transaction failed".to_string())?;
    let event_count = row
        .get_by_name::<i64, &str>("event_count")
        .map_err(|_| "reading candidate transaction failed".to_string())?;
    let recorded_hash = row
        .get_by_name::<Vec<u8>, &str>("content_hash")
        .map_err(|_| "reading candidate transaction failed".to_string())?;
    if end_lsn != transaction.end_lsn
        || source_xid != Some(transaction.xid)
        || registry_generation != Some(bootstrap.identity.registry_generation)
        || event_count != i64::try_from(events.len()).ok()
        || recorded_hash.as_deref() != Some(content_hash.as_slice())
    {
        return Err("candidate transaction replay identity differs".to_string());
    }
    verify_candidate_events(client, bootstrap, transaction, events)?;
    let progress_end = client
        .select(
            "SELECT candidate_materialized_end_lsn::text AS end_lsn
             FROM synchro.sync_stream_resets
             WHERE reset_id = $1::uuid
               AND operation_kind = 'projection_bootstrap'
               AND lifecycle = 'catching_up'",
            None,
            &[bootstrap.identity.bootstrap_id.as_str().into()],
        )
        .map_err(|_| "loading candidate replay progress failed".to_string())?
        .first();
    if required_lsn(&progress_end, "end_lsn")? != transaction.end_lsn {
        return Err("candidate replay progress differs".to_string());
    }
    let updated = client
        .update(
            "UPDATE synchro.sync_projection_bootstrap_transactions
             SET replay_count = replay_count + 1
             WHERE bootstrap_id = $1::uuid AND commit_lsn = $2::pg_lsn",
            None,
            &[
                bootstrap.identity.bootstrap_id.as_str().into(),
                format_lsn(transaction.commit_lsn).as_str().into(),
            ],
        )
        .map_err(|_| "updating candidate replay count failed".to_string())?
        .len();
    if updated != 1 {
        return Err("candidate replay transaction changed".to_string());
    }
    Ok(true)
}

fn verify_candidate_events(
    client: &SpiClient<'_>,
    bootstrap: &CandidateBootstrap,
    transaction: &WalTransaction,
    events: &[ApplicableEvent<'_>],
) -> Result<(), String> {
    let rows = client
        .select(
            "SELECT event_ordinal, relation_id::text AS relation_id,
                    registration_kind, physical_schema::text AS physical_schema,
                    physical_relation::text AS physical_relation,
                    physical_relation_oid::bigint AS physical_relation_oid,
                    operation, fence_id::text AS fence_id
             FROM synchro.sync_projection_bootstrap_events
             WHERE bootstrap_id = $1::uuid AND commit_lsn = $2::pg_lsn
             ORDER BY event_ordinal",
            None,
            &[
                bootstrap.identity.bootstrap_id.as_str().into(),
                format_lsn(transaction.commit_lsn).as_str().into(),
            ],
        )
        .map_err(|_| "loading candidate replay events failed".to_string())?;
    if rows.len() != events.len() {
        return Err("candidate replay event count differs".to_string());
    }
    for (row, event) in rows.into_iter().zip(events) {
        let ordinal = row
            .get_by_name::<i64, &str>("event_ordinal")
            .map_err(|_| "reading candidate replay event failed".to_string())?
            .and_then(|value| u64::try_from(value).ok());
        let relation_oid = row
            .get_by_name::<i64, &str>("physical_relation_oid")
            .map_err(|_| "reading candidate replay event failed".to_string())?
            .and_then(|value| u32::try_from(value).ok());
        if ordinal != Some(event.event.event_ordinal)
            || optional_text(&row, "relation_id")?.as_deref()
                != Some(event.registration.relation_id.as_str())
            || optional_text(&row, "registration_kind")?.as_deref()
                != Some(event.registration.registration_kind.as_str())
            || optional_text(&row, "physical_schema")?.as_deref()
                != Some(event.event.relation.namespace.as_str())
            || optional_text(&row, "physical_relation")?.as_deref()
                != Some(event.event.relation.name.as_str())
            || relation_oid != Some(event.event.relation.oid)
            || optional_text(&row, "operation")?.as_deref() != Some(event.operation_name)
            || optional_text(&row, "fence_id")?.as_deref() != Some(event.fence_id.as_str())
        {
            return Err("candidate replay event identity differs".to_string());
        }
    }
    Ok(())
}

fn optional_text(row: &impl CandidateRow, name: &str) -> Result<Option<String>, String> {
    row.candidate_text(name)
        .map_err(|_| "reading candidate replay event failed".to_string())
}

fn advance_candidate_slot(
    bootstrap: &CandidateBootstrap,
    transaction: &WalTransaction,
    worker_role_oid: pg_sys::Oid,
) -> Result<(), String> {
    let requested = format_lsn(transaction.end_lsn);
    run_replication_transaction(worker_role_oid, || {
        Spi::connect_mut(|client| {
            let actual = client
                .select(
                    "SELECT end_lsn::text AS end_lsn
                     FROM pg_catalog.pg_replication_slot_advance($1, $2::pg_lsn)",
                    None,
                    &[
                        bootstrap.identity.slot_name.as_str().into(),
                        requested.as_str().into(),
                    ],
                )
                .map_err(|_| "advancing candidate slot failed".to_string())?
                .first()
                .get_by_name::<String, &str>("end_lsn")
                .map_err(|_| "reading candidate slot advancement failed".to_string())?
                .and_then(|value| parse_lsn(&value))
                .ok_or_else(|| "candidate slot advancement is invalid".to_string())?;
            if actual != transaction.end_lsn {
                return Err("candidate slot advanced to an unexpected boundary".to_string());
            }
            activate_worker_role(worker_role_oid);
            let updated = client
                .update(
                    "UPDATE synchro.sync_stream_resets
                     SET candidate_acknowledged_end_lsn = $2::pg_lsn,
                         updated_at = now()
                     WHERE reset_id = $1::uuid
                       AND operation_kind = 'projection_bootstrap'
                       AND lifecycle = 'catching_up'
                       AND candidate_slot_name::text = $3
                       AND target_registry_generation = $4
                       AND candidate_materialized_commit_lsn = $5::pg_lsn
                       AND candidate_materialized_end_lsn = $2::pg_lsn
                       AND COALESCE(
                               candidate_acknowledged_end_lsn,
                               consistent_point
                           ) = $6::pg_lsn",
                    None,
                    &[
                        bootstrap.identity.bootstrap_id.as_str().into(),
                        requested.as_str().into(),
                        bootstrap.identity.slot_name.as_str().into(),
                        bootstrap.identity.registry_generation.into(),
                        format_lsn(transaction.commit_lsn).as_str().into(),
                        format_lsn(bootstrap.acknowledged_end_lsn).as_str().into(),
                    ],
                )
                .map_err(|_| "acknowledging candidate slot failed".to_string())?
                .len();
            if updated != 1 {
                return Err("candidate slot acknowledgement changed".to_string());
            }
            Ok(())
        })
    })
}

fn select_candidate_projection(
    client: &mut SpiClient<'_>,
    bootstrap: &CandidateBootstrap,
) -> Result<(), String> {
    client
        .update(
            "SELECT set_config('synchro.stream_reset_staging_id', $1, true)",
            None,
            &[bootstrap.identity.bootstrap_id.as_str().into()],
        )
        .map_err(|_| "selecting candidate projection failed".to_string())?;
    client
        .update(
            "SELECT set_config(
                 'synchro.stream_reset_staging_registry_generation', $1, true
             )",
            None,
            &[bootstrap
                .identity
                .registry_generation
                .to_string()
                .as_str()
                .into()],
        )
        .map_err(|_| "selecting candidate registry failed".to_string())?;
    Ok(())
}

fn recompute_candidate_membership(
    client: &mut SpiClient<'_>,
    target: ProjectionTarget<'_>,
    registry: &[TableRegistration],
) -> Result<(), String> {
    let bootstrap_id = target
        .bootstrap_id()
        .ok_or_else(|| "candidate projection target is required".to_string())?;
    let registry_generation = target
        .registry_generation()
        .ok_or_else(|| "candidate registry target is required".to_string())?;
    client
        .update(
            "DELETE FROM synchro.sync_stream_reset_membership_edges
             WHERE reset_id = $1::uuid",
            None,
            &[bootstrap_id.into()],
        )
        .map_err(|_| "clearing candidate membership failed".to_string())?;
    for registration in registry
        .iter()
        .filter(|registration| registration.is_synced())
    {
        if registration.registry_generation != registry_generation {
            return Err("candidate registry generation changed".to_string());
        }
        let rows = client
            .select(
                "SELECT record_id, checksum, row_version::text AS row_version
                 FROM synchro.sync_stream_reset_captured_rows
                 WHERE reset_id = $1::uuid
                   AND relation_id = $2::uuid
                   AND registry_generation = $3
                   AND NOT deleted
                 ORDER BY record_id",
                None,
                &[
                    bootstrap_id.into(),
                    registration.relation_id.as_str().into(),
                    registry_generation.into(),
                ],
            )
            .map_err(|_| "loading candidate membership rows failed".to_string())?;
        for row in rows {
            let record_id = optional_text(&row, "record_id")?
                .filter(|value| !value.is_empty())
                .ok_or_else(|| "candidate membership row identity is missing".to_string())?;
            let row_version = optional_text(&row, "row_version")?
                .filter(|value| !value.is_empty())
                .ok_or_else(|| "candidate membership row version is missing".to_string())?;
            let checksum = row
                .get_by_name::<Vec<u8>, &str>("checksum")
                .map_err(|_| "reading candidate membership row failed".to_string())?
                .and_then(|value| <[u8; 32]>::try_from(value).ok())
                .ok_or_else(|| "candidate membership row digest is invalid".to_string())?;
            let scopes = resolve_membership(client, registration, &record_id)
                .map_err(|_| "resolving candidate membership failed".to_string())?;
            for scope_id in scopes {
                client
                    .update(
                        "INSERT INTO synchro.sync_stream_reset_membership_edges (
                             reset_id, relation_id, table_name, record_id, scope_id,
                             checksum, row_version, staged_at
                         ) VALUES (
                             $1::uuid, $2::uuid, $3, $4, $5, $6, $7::uuid, now()
                         )",
                        None,
                        &[
                            bootstrap_id.into(),
                            registration.relation_id.as_str().into(),
                            registration.table_name.as_str().into(),
                            record_id.as_str().into(),
                            scope_id.as_str().into(),
                            checksum.to_vec().into(),
                            row_version.as_str().into(),
                        ],
                    )
                    .map_err(|_| "recording candidate membership failed".to_string())?;
            }
        }
    }
    Ok(())
}

fn finalize_candidate(bootstrap: &CandidateBootstrap) -> Result<(), String> {
    run_worker_transaction(|| {
        Spi::connect_mut(|client| {
            validate_candidate_binding(client, bootstrap, true)?;
            select_candidate_projection(client, bootstrap)?;
            let boundary_valid = client
                .select(
                    "SELECT candidate_materialized_end_lsn = activation_barrier
                            AND COALESCE(
                                    candidate_acknowledged_end_lsn,
                                    consistent_point
                                ) = activation_barrier
                            AND activation_barrier = $2::pg_lsn AS valid
                     FROM synchro.sync_stream_resets
                     WHERE reset_id = $1::uuid
                       AND operation_kind = 'projection_bootstrap'
                       AND lifecycle = 'catching_up'",
                    None,
                    &[
                        bootstrap.identity.bootstrap_id.as_str().into(),
                        format_lsn(bootstrap.activation_barrier).as_str().into(),
                    ],
                )
                .map_err(|_| "checking candidate barrier failed".to_string())?
                .first()
                .get_by_name::<bool, &str>("valid")
                .map_err(|_| "checking candidate barrier failed".to_string())?
                .unwrap_or(false);
            if !boundary_valid {
                return Err("candidate has not reached the activation barrier".to_string());
            }
            let registry = load_registry_generation_from_client(
                client,
                bootstrap.identity.registry_generation,
            )
            .map_err(|_| "loading candidate verification registry failed".to_string())?;
            let target = ProjectionTarget::Candidate {
                bootstrap_id: &bootstrap.identity.bootstrap_id,
                registry_generation: bootstrap.identity.registry_generation,
            };
            recompute_candidate_membership(client, target, &registry)?;
            verify_candidate_stage_integrity(client, bootstrap)?;
            replace_candidate_scope_digests(client, bootstrap, &registry)?;
            verify_candidate_scope_digests(client, bootstrap, &registry)?;
            update_candidate_staged_counts(client, bootstrap)?;
            let updated = client
                .update(
                    "UPDATE synchro.sync_stream_resets
                     SET candidate_acknowledged_end_lsn = activation_barrier,
                         candidate_verified = true, updated_at = now()
                     WHERE reset_id = $1::uuid
                       AND operation_kind = 'projection_bootstrap'
                       AND lifecycle = 'catching_up'
                       AND target_registry_generation = $2
                       AND candidate_materialized_end_lsn = activation_barrier
                       AND COALESCE(
                               candidate_acknowledged_end_lsn,
                               consistent_point
                           ) = activation_barrier
                       AND activation_barrier = $3::pg_lsn",
                    None,
                    &[
                        bootstrap.identity.bootstrap_id.as_str().into(),
                        bootstrap.identity.registry_generation.into(),
                        format_lsn(bootstrap.activation_barrier).as_str().into(),
                    ],
                )
                .map_err(|_| "verifying candidate bootstrap failed".to_string())?
                .len();
            if updated != 1 {
                return Err("candidate verification state changed".to_string());
            }
            Ok(())
        })
    })
}

fn verify_candidate_stage_integrity(
    client: &SpiClient<'_>,
    bootstrap: &CandidateBootstrap,
) -> Result<(), String> {
    let valid = client
        .select(
            "SELECT
                 NOT EXISTS (
                     SELECT 1
                     FROM synchro.sync_stream_reset_row_versions version
                     LEFT JOIN synchro.sync_stream_reset_captured_rows captured
                       ON captured.reset_id = version.reset_id
                      AND captured.relation_id = version.relation_id
                      AND captured.record_id = version.record_id
                      AND captured.row_version = version.row_version
                     WHERE version.reset_id = $1::uuid
                       AND NOT version.deleted
                       AND captured.record_id IS NULL
                 )
                 AND NOT EXISTS (
                     SELECT 1
                     FROM synchro.sync_stream_reset_captured_rows captured
                     LEFT JOIN synchro.sync_stream_reset_row_versions version
                       ON version.reset_id = captured.reset_id
                      AND version.relation_id = captured.relation_id
                      AND version.record_id = captured.record_id
                      AND version.row_version = captured.row_version
                     WHERE captured.reset_id = $1::uuid
                       AND (version.record_id IS NULL
                            OR captured.registry_generation <> $2)
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
            &[
                bootstrap.identity.bootstrap_id.as_str().into(),
                bootstrap.identity.registry_generation.into(),
            ],
        )
        .map_err(|_| "verifying candidate projection integrity failed".to_string())?
        .first()
        .get_by_name::<bool, &str>("valid")
        .map_err(|_| "verifying candidate projection integrity failed".to_string())?
        .unwrap_or(false);
    if !valid {
        return Err("candidate projection integrity is invalid".to_string());
    }
    Ok(())
}

fn replace_candidate_scope_digests(
    client: &mut SpiClient<'_>,
    bootstrap: &CandidateBootstrap,
    registry: &[TableRegistration],
) -> Result<(), String> {
    let expected = compute_candidate_scope_digests(client, bootstrap, registry)?;
    client
        .update(
            "DELETE FROM synchro.sync_stream_reset_scope_digests
             WHERE reset_id = $1::uuid",
            None,
            &[bootstrap.identity.bootstrap_id.as_str().into()],
        )
        .map_err(|_| "clearing candidate scope digests failed".to_string())?;
    for (scope_id, digest, row_count, schema_hash) in expected {
        client
            .update(
                "INSERT INTO synchro.sync_stream_reset_scope_digests (
                     reset_id, scope_id, schema_hash, digest, row_count, staged_at
                 ) VALUES ($1::uuid, $2, $3, $4, $5, now())",
                None,
                &[
                    bootstrap.identity.bootstrap_id.as_str().into(),
                    scope_id.as_str().into(),
                    schema_hash.as_str().into(),
                    digest.as_bytes().to_vec().into(),
                    row_count.into(),
                ],
            )
            .map_err(|_| "recording candidate scope digest failed".to_string())?;
    }
    Ok(())
}

fn verify_candidate_scope_digests(
    client: &SpiClient<'_>,
    bootstrap: &CandidateBootstrap,
    registry: &[TableRegistration],
) -> Result<(), String> {
    let expected = compute_candidate_scope_digests(client, bootstrap, registry)?;
    let rows = client
        .select(
            "SELECT scope_id, schema_hash, digest, row_count
             FROM synchro.sync_stream_reset_scope_digests
             WHERE reset_id = $1::uuid
             ORDER BY scope_id",
            None,
            &[bootstrap.identity.bootstrap_id.as_str().into()],
        )
        .map_err(|_| "loading candidate scope digests failed".to_string())?;
    if rows.len() != expected.len() {
        return Err("candidate scope digest set is incomplete".to_string());
    }
    for (row, (scope_id, digest, row_count, schema_hash)) in rows.into_iter().zip(expected) {
        let actual_digest = row
            .get_by_name::<Vec<u8>, &str>("digest")
            .map_err(|_| "reading candidate scope digest failed".to_string())?
            .and_then(|value| <[u8; 32]>::try_from(value).ok())
            .map(Sha256Digest::from_bytes);
        let actual_count = row
            .get_by_name::<i64, &str>("row_count")
            .map_err(|_| "reading candidate scope digest failed".to_string())?;
        if optional_text(&row, "scope_id")?.as_deref() != Some(scope_id.as_str())
            || optional_text(&row, "schema_hash")?.as_deref() != Some(schema_hash.as_str())
            || actual_digest != Some(digest)
            || actual_count != Some(row_count)
        {
            return Err("candidate scope digest differs from candidate edges".to_string());
        }
    }
    Ok(())
}

fn compute_candidate_scope_digests(
    client: &SpiClient<'_>,
    bootstrap: &CandidateBootstrap,
    registry: &[TableRegistration],
) -> Result<Vec<(String, Sha256Digest, i64, String)>, String> {
    let schema_hash =
        crate::pull::schema_hash_for_generation(client, bootstrap.identity.registry_generation)?;
    let schema_hash_text = schema_hash.to_lower_hex();
    let scope_rows = client
        .select(
            "SELECT scope_id FROM synchro.sync_scope_state
             UNION
             SELECT scope_id FROM synchro.sync_stream_reset_membership_edges
             WHERE reset_id = $1::uuid
             ORDER BY scope_id",
            None,
            &[bootstrap.identity.bootstrap_id.as_str().into()],
        )
        .map_err(|_| "loading candidate scope identities failed".to_string())?;
    let mut entries = BTreeMap::<String, Vec<ScopeDigestEntry>>::new();
    for row in scope_rows {
        let scope_id = optional_text(&row, "scope_id")?
            .filter(|value| !value.is_empty())
            .ok_or_else(|| "candidate scope identity is invalid".to_string())?;
        entries.insert(scope_id, Vec::new());
    }
    let edge_rows = client
        .select(
            "SELECT relation_id::text AS relation_id, record_id, scope_id, checksum
             FROM synchro.sync_stream_reset_membership_edges
             WHERE reset_id = $1::uuid
             ORDER BY scope_id, relation_id, record_id",
            None,
            &[bootstrap.identity.bootstrap_id.as_str().into()],
        )
        .map_err(|_| "loading candidate digest edges failed".to_string())?;
    for row in edge_rows {
        let relation_id = optional_text(&row, "relation_id")?
            .ok_or_else(|| "candidate edge relation is missing".to_string())?;
        let record_id = optional_text(&row, "record_id")?
            .ok_or_else(|| "candidate edge row identity is missing".to_string())?;
        let scope_id = optional_text(&row, "scope_id")?
            .ok_or_else(|| "candidate edge scope is missing".to_string())?;
        let registration = registry
            .iter()
            .find(|candidate| candidate.relation_id == relation_id)
            .ok_or_else(|| "candidate edge relation is not registered".to_string())?;
        let primary_key = crate::pull::row_primary_key_json(registration, &record_id)?;
        let identity = row_identity(
            &crate::pull::canonical_table(registration)?,
            &serde_json::to_string(&primary_key)
                .map_err(|_| "encoding candidate row identity failed".to_string())?,
        )
        .map_err(|_| "candidate row identity is invalid".to_string())?;
        let checksum = row
            .get_by_name::<Vec<u8>, &str>("checksum")
            .map_err(|_| "reading candidate edge digest failed".to_string())?
            .and_then(|value| <[u8; 32]>::try_from(value).ok())
            .map(Sha256Digest::from_bytes)
            .ok_or_else(|| "candidate edge digest is invalid".to_string())?;
        entries
            .get_mut(&scope_id)
            .ok_or_else(|| "candidate edge scope is unavailable".to_string())?
            .push(ScopeDigestEntry::new(identity, checksum));
    }
    entries
        .into_iter()
        .map(|(scope_id, scope_entries)| {
            let row_count = i64::try_from(scope_entries.len())
                .map_err(|_| "candidate scope row count overflowed".to_string())?;
            let digest = scope_digest(schema_hash, &scope_id, &scope_entries)
                .map_err(|_| "computing candidate scope digest failed".to_string())?;
            Ok((scope_id, digest, row_count, schema_hash_text.clone()))
        })
        .collect()
}

fn update_candidate_staged_counts(
    client: &mut SpiClient<'_>,
    bootstrap: &CandidateBootstrap,
) -> Result<(), String> {
    let updated = client
        .update(
            "UPDATE synchro.sync_stream_resets reset
             SET staged_row_count = counts.rows,
                 staged_version_count = counts.versions,
                 staged_edge_count = counts.edges,
                 staged_scope_count = counts.scopes,
                 updated_at = now()
             FROM (
                 SELECT
                     ((SELECT count(*)
                       FROM synchro.sync_stream_reset_captured_rows
                       WHERE reset_id = $1::uuid)
                      +
                      (SELECT count(*)
                       FROM synchro.sync_stream_reset_capture_dependency_rows
                       WHERE reset_id = $1::uuid)) AS rows,
                     (SELECT count(*)
                      FROM synchro.sync_stream_reset_row_versions
                      WHERE reset_id = $1::uuid) AS versions,
                     (SELECT count(*)
                      FROM synchro.sync_stream_reset_membership_edges
                      WHERE reset_id = $1::uuid) AS edges,
                     (SELECT count(*)
                      FROM synchro.sync_stream_reset_scope_digests
                      WHERE reset_id = $1::uuid) AS scopes
             ) counts
             WHERE reset.reset_id = $1::uuid
               AND reset.operation_kind = 'projection_bootstrap'
               AND reset.lifecycle = 'catching_up'",
            None,
            &[bootstrap.identity.bootstrap_id.as_str().into()],
        )
        .map_err(|_| "updating candidate staging counts failed".to_string())?
        .len();
    if updated != 1 {
        return Err("candidate staging counts changed".to_string());
    }
    Ok(())
}

fn preload_relations(
    client: &SpiClient<'_>,
    registry: &[TableRegistration],
) -> Result<Vec<(RelationKey, u8, Vec<ColumnInfo>)>, String> {
    let mut relations = Vec::with_capacity(registry.len());
    for registration in registry {
        let rows = client
            .select(
                "SELECT a.attname::text AS name,
                        (a.attnum = ANY(i.indkey)) AS is_key,
                        a.atttypid::bigint AS type_oid,
                        a.atttypmod AS type_modifier
                 FROM pg_catalog.pg_attribute a
                 JOIN pg_catalog.pg_index i
                   ON i.indrelid = a.attrelid AND i.indisprimary
                 WHERE a.attrelid = $1::oid
                   AND a.attnum > 0
                   AND NOT a.attisdropped
                 ORDER BY a.attnum",
                None,
                &[i64::from(registration.physical_relation_oid).into()],
            )
            .map_err(|_| "loading relation metadata failed".to_string())?;
        let mut columns = Vec::new();
        for row in rows {
            let name = row
                .get_by_name::<String, &str>("name")
                .map_err(|_| "loading relation metadata failed".to_string())?
                .ok_or_else(|| "relation metadata is incomplete".to_string())?;
            let is_key = row
                .get_by_name::<bool, &str>("is_key")
                .map_err(|_| "loading relation metadata failed".to_string())?
                .unwrap_or(false);
            let type_oid = row
                .get_by_name::<i64, &str>("type_oid")
                .map_err(|_| "loading relation metadata failed".to_string())?
                .and_then(|value| u32::try_from(value).ok())
                .ok_or_else(|| "relation metadata is incomplete".to_string())?;
            let type_modifier = row
                .get_by_name::<i32, &str>("type_modifier")
                .map_err(|_| "loading relation metadata failed".to_string())?
                .ok_or_else(|| "relation metadata is incomplete".to_string())?;
            columns.push(ColumnInfo {
                name,
                is_key,
                type_oid,
                type_modifier,
            });
        }
        if columns.is_empty() {
            return Err("relation metadata is incomplete".to_string());
        }
        relations.push((
            RelationKey::new(
                &registration.physical_schema,
                &registration.physical_relation,
                registration.physical_relation_oid,
            ),
            b'd',
            columns,
        ));
    }
    Ok(relations)
}

fn poll_and_process(
    decoder: &mut WalDecoder,
    slot: &str,
    worker_role_oid: pg_sys::Oid,
) -> Result<usize, PollFailure> {
    validate_slot_boundary(slot, worker_role_oid)?;
    let publication = publication_name();
    let messages =
        run_replication_transaction(worker_role_oid, || peek_messages(slot, &publication))
            .map_err(|_| PollFailure::Transient("peek"))?;
    if messages.is_empty() {
        record_oldest_unmaterialized_commit(decoder.pending_commit_timestamp())
            .map_err(|_| PollFailure::Transient("lag_record"))?;
        return Ok(0);
    }

    let message_count = messages.len();
    let mut transactions = Vec::new();
    let mut pending_final_lsn = None;
    let mut pending_commit_timestamp = decoder.pending_commit_timestamp();
    for message in messages {
        if message.data.first() == Some(&crate::wal_decoder::BEGIN_MSG) && message.data.len() >= 17
        {
            pending_final_lsn = Some(u64::from_be_bytes(
                message.data[1..9].try_into().unwrap_or([0; 8]),
            ));
            pending_commit_timestamp = Some(i64::from_be_bytes(
                message.data[9..17].try_into().unwrap_or([0; 8]),
            ));
        }
        match decoder.feed(&message.data) {
            Ok(completed) => {
                for transaction in &completed {
                    if let Some(sql_xid) = message.sql_xid {
                        if sql_xid != transaction.xid {
                            return Err(PollFailure::Poison(PoisonFailure {
                                class: "validation_failed",
                                detail: "WAL transaction identifier did not match the decoded transaction"
                                    .to_string(),
                                commit_lsn: transaction.commit_lsn,
                                relation_id: infer_transaction_relation_id(transaction),
                                commit_timestamp: Some(transaction.commit_timestamp),
                            }));
                        }
                    }
                }
                if !completed.is_empty() {
                    pending_final_lsn = None;
                    pending_commit_timestamp = decoder.pending_commit_timestamp();
                }
                transactions.extend(completed);
            }
            Err(_) => {
                return Err(PollFailure::Poison(PoisonFailure {
                    class: "decode_failed",
                    detail: "WAL decoder rejected a replication message".to_string(),
                    commit_lsn: pending_final_lsn.unwrap_or(message.lsn),
                    relation_id: None,
                    commit_timestamp: pending_commit_timestamp,
                }));
            }
        }
    }

    record_oldest_unmaterialized_commit(
        transactions
            .first()
            .map(|transaction| transaction.commit_timestamp)
            .or_else(|| decoder.pending_commit_timestamp()),
    )
    .map_err(|_| PollFailure::Transient("lag_record"))?;

    let mut previous = None;
    let mut acknowledged_boundary = None;
    for (index, transaction) in transactions.iter().enumerate() {
        if previous.is_some_and(|commit_lsn| commit_lsn >= transaction.commit_lsn) {
            return Err(PollFailure::Poison(PoisonFailure {
                class: "validation_failed",
                detail: "decoded transactions were not in commit order".to_string(),
                commit_lsn: transaction.commit_lsn,
                relation_id: infer_transaction_relation_id(transaction),
                commit_timestamp: Some(transaction.commit_timestamp),
            }));
        }
        previous = Some(transaction.commit_lsn);
        let materialized = materialize_one(transaction).map_err(|failure| {
            if failure.class == "activation_barrier" {
                PollFailure::ActivationBarrier
            } else {
                PollFailure::Poison(failure)
            }
        })?;
        acknowledged_boundary = Some((transaction.commit_lsn, materialized.end_lsn));
        record_oldest_unmaterialized_commit(
            transactions
                .get(index + 1)
                .map(|next| next.commit_timestamp)
                .or_else(|| decoder.pending_commit_timestamp()),
        )
        .map_err(|_| PollFailure::Transient("lag_record"))?;
    }
    if let Some((commit_lsn, end_lsn)) = acknowledged_boundary {
        advance_slot(slot, commit_lsn, end_lsn, worker_role_oid)
            .map_err(|_| PollFailure::Transient("slot_advance"))?;
    }

    Ok(message_count)
}

fn validate_slot_boundary(slot: &str, worker_role_oid: pg_sys::Oid) -> Result<(), PollFailure> {
    run_replication_transaction(worker_role_oid, || {
        Spi::connect(|client| {
            let actual = client
                .select(
                    "SELECT confirmed_flush_lsn::text AS actual_lsn
                     FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                    None,
                    &[slot.into()],
                )
                .map_err(|_| PollFailure::Transient("slot_boundary"))?
                .first()
                .get_by_name::<String, &str>("actual_lsn")
                .map_err(|_| PollFailure::Transient("slot_boundary"))?
                .and_then(|value| parse_lsn(&value))
                .ok_or(PollFailure::Transient("slot_boundary"))?;
            activate_worker_role(worker_role_oid);
            let expected = client
                .select(
                    "SELECT COALESCE(acknowledged_end_lsn, generation_start_lsn)::text
                            AS expected_lsn
                     FROM synchro.sync_wal_progress WHERE singleton",
                    None,
                    &[],
                )
                .map_err(|_| PollFailure::Transient("slot_boundary"))?
                .first()
                .get_by_name::<String, &str>("expected_lsn")
                .map_err(|_| PollFailure::Transient("slot_boundary"))?
                .and_then(|value| parse_lsn(&value))
                .ok_or(PollFailure::Transient("slot_boundary"))?;
            if actual != expected {
                return Err(PollFailure::Poison(PoisonFailure {
                    class: "transaction_commit_failed",
                    detail: "logical slot acknowledgement did not match durable progress"
                        .to_string(),
                    commit_lsn: actual,
                    relation_id: None,
                    commit_timestamp: None,
                }));
            }
            Ok(())
        })
    })
    .map_err(|failure| match failure {
        PollFailure::Poison(failure) => PollFailure::Poison(failure),
        PollFailure::Transient(stage) => PollFailure::Transient(stage),
        PollFailure::ActivationBarrier => PollFailure::ActivationBarrier,
    })
}

struct PeekedMessage {
    lsn: u64,
    sql_xid: Option<u32>,
    data: Vec<u8>,
}

fn peek_messages(slot: &str, publication: &str) -> Result<Vec<PeekedMessage>, String> {
    Spi::connect(|client| {
        let rows = client
            .select(
                "SELECT lsn::text AS lsn, xid::text AS xid, data
                 FROM pg_catalog.pg_logical_slot_peek_binary_changes(
                     $1, NULL, $2,
                     'proto_version', '1',
                     'publication_names', $3,
                     'messages', 'true'
                 )",
                None,
                &[slot.into(), BATCH_SIZE.into(), publication.into()],
            )
            .map_err(|_| "peeking WAL failed".to_string())?;
        let mut messages = Vec::new();
        for row in rows {
            let lsn = row
                .get_by_name::<String, &str>("lsn")
                .map_err(|_| "reading WAL position failed".to_string())?
                .and_then(|value| parse_lsn(&value))
                .ok_or_else(|| "WAL position is invalid".to_string())?;
            let sql_xid = row
                .get_by_name::<String, &str>("xid")
                .map_err(|_| "reading WAL transaction failed".to_string())?
                .and_then(|value| value.parse::<u32>().ok());
            let data = row
                .get_by_name::<Vec<u8>, &str>("data")
                .map_err(|_| "reading WAL data failed".to_string())?
                .ok_or_else(|| "WAL data is missing".to_string())?;
            messages.push(PeekedMessage { lsn, sql_xid, data });
        }
        Ok(messages)
    })
}

fn materialize_one(transaction: &WalTransaction) -> Result<MaterializedTransaction, PoisonFailure> {
    let commit_lsn = transaction.commit_lsn;
    run_worker_transaction(|| {
        Spi::connect_mut(|client| materialize_transaction(client, transaction))
    })
    .map_err(|mut failure| {
        if failure.commit_lsn == 0 {
            failure.commit_lsn = commit_lsn;
        }
        failure.commit_timestamp = Some(transaction.commit_timestamp);
        if failure.relation_id.is_none() {
            failure.relation_id = infer_transaction_relation_id(transaction);
        }
        failure
    })
}

fn materialize_transaction(
    client: &mut SpiClient<'_>,
    transaction: &WalTransaction,
) -> Result<MaterializedTransaction, PoisonFailure> {
    let stream_generation = active_stream_generation(client)
        .map_err(|_| failure("validation_failed", transaction.commit_lsn))?;
    let generation = active_registry_generation(client)
        .map_err(|_| failure("validation_failed", transaction.commit_lsn))?;

    let content_hash = transaction_content_hash(transaction);
    if existing_transaction(client, &stream_generation, transaction, &content_hash)? {
        reconcile_replay_counts(client, &stream_generation, transaction)?;
        repair_same_position_poison(client, &stream_generation, transaction.commit_lsn)?;
        return Ok(MaterializedTransaction {
            end_lsn: transaction.end_lsn,
        });
    }

    validate_progress_order(client, &stream_generation, transaction)?;
    let activations = parse_registry_activations(transaction)?;
    validate_activation_chain(
        client,
        &stream_generation,
        generation,
        &activations,
        transaction,
    )?;
    let registry = match activations.last().copied() {
        Some(final_generation) => {
            load_registry_generation_for_activation(client, generation, final_generation)
        }
        None => load_registry_generation_from_client(client, generation),
    }
    .map_err(|_| failure("registered_relation_drift", transaction.commit_lsn))?;
    let membership_dependencies =
        load_membership_dependencies_from_client(client, generation, &registry)
            .map_err(|_| failure("registered_relation_drift", transaction.commit_lsn))?;
    let projection_target = ProjectionTarget::Active {
        stream_generation: &stream_generation,
    };

    if transaction
        .truncates
        .iter()
        .any(|truncate| find_registration(&registry, &truncate.relation).is_some())
    {
        return Err(failure("truncate_unsupported", transaction.commit_lsn));
    }

    let fences = parse_fence_messages(transaction)?;
    let applicable =
        correlate_events(client, transaction, &registry, &fences, FenceTarget::Active)?;

    client
        .update(
            "INSERT INTO synchro.sync_wal_transactions (
                 stream_generation, commit_lsn, end_lsn, source_xid,
                 registry_generation, event_count, effect_count, content_hash,
                 commit_timestamp
             ) VALUES (
                 $1, $2::pg_lsn, $3::pg_lsn, $4::xid,
                 $5, $6, 0, $7,
                 '2000-01-01 00:00:00+00'::timestamptz + ($8::bigint * interval '1 microsecond')
             )",
            None,
            &[
                stream_generation.as_str().into(),
                format_lsn(transaction.commit_lsn).as_str().into(),
                format_lsn(transaction.end_lsn).as_str().into(),
                transaction.xid.to_string().as_str().into(),
                generation.into(),
                i64::try_from(applicable.len())
                    .map_err(|_| failure("validation_failed", transaction.commit_lsn))?
                    .into(),
                content_hash.to_vec().into(),
                transaction.commit_timestamp.into(),
            ],
        )
        .map_err(|_| failure("materialization_failed", transaction.commit_lsn))?;

    let persisted = persist_events_and_projections(
        client,
        projection_target,
        transaction,
        &registry,
        &applicable,
    )?;
    let impacts = collect_membership_impacts(
        client,
        projection_target,
        transaction,
        &registry,
        &membership_dependencies,
        persisted,
    )?;
    let effect_count =
        materialize_impacts(client, projection_target, transaction, &registry, impacts)?;

    client
        .update(
            "UPDATE synchro.sync_wal_transactions
             SET effect_count = $3
             WHERE stream_generation = $1 AND commit_lsn = $2::pg_lsn",
            None,
            &[
                stream_generation.as_str().into(),
                format_lsn(transaction.commit_lsn).as_str().into(),
                effect_count.into(),
            ],
        )
        .map_err(|_| failure("materialization_failed", transaction.commit_lsn))?;

    let final_generation = activate_generations(
        client,
        generation,
        &activations,
        transaction.commit_lsn,
        transaction.end_lsn,
    )?;
    client
        .update(
            "UPDATE synchro.sync_wal_progress
             SET stream_generation = $1,
                 materialized_commit_lsn = $2::pg_lsn,
                 materialized_end_lsn = $3::pg_lsn,
                 registry_generation = $4,
                 updated_at = now()
             WHERE singleton = true",
            None,
            &[
                stream_generation.as_str().into(),
                format_lsn(transaction.commit_lsn).as_str().into(),
                format_lsn(transaction.end_lsn).as_str().into(),
                final_generation.into(),
            ],
        )
        .map_err(|_| failure("transaction_commit_failed", transaction.commit_lsn))?;
    repair_same_position_poison(client, &stream_generation, transaction.commit_lsn)?;

    Ok(MaterializedTransaction {
        end_lsn: transaction.end_lsn,
    })
}

fn existing_transaction(
    client: &SpiClient<'_>,
    stream_generation: &str,
    transaction: &WalTransaction,
    content_hash: &[u8; 32],
) -> Result<bool, PoisonFailure> {
    let rows = client
        .select(
            "SELECT end_lsn::text AS end_lsn, source_xid::text AS source_xid,
                    event_count, content_hash
             FROM synchro.sync_wal_transactions
             WHERE stream_generation = $1 AND commit_lsn = $2::pg_lsn",
            None,
            &[
                stream_generation.into(),
                format_lsn(transaction.commit_lsn).as_str().into(),
            ],
        )
        .map_err(|_| failure("validation_failed", transaction.commit_lsn))?;
    let Some(row) = rows.into_iter().next() else {
        return Ok(false);
    };
    let end_lsn = row
        .get_by_name::<String, &str>("end_lsn")
        .map_err(|_| failure("validation_failed", transaction.commit_lsn))?
        .and_then(|value| parse_lsn(&value));
    let source_xid = row
        .get_by_name::<String, &str>("source_xid")
        .map_err(|_| failure("validation_failed", transaction.commit_lsn))?
        .and_then(|value| value.parse::<u32>().ok());
    let event_count = row
        .get_by_name::<i64, &str>("event_count")
        .map_err(|_| failure("validation_failed", transaction.commit_lsn))?
        .unwrap_or(-1);
    let recorded_hash = row
        .get_by_name::<Vec<u8>, &str>("content_hash")
        .map_err(|_| failure("validation_failed", transaction.commit_lsn))?
        .unwrap_or_default();
    if end_lsn != Some(transaction.end_lsn)
        || source_xid != Some(transaction.xid)
        || event_count < 0
        || recorded_hash != content_hash
    {
        return Err(failure("validation_failed", transaction.commit_lsn));
    }
    Ok(true)
}

fn reconcile_replay_counts(
    client: &mut SpiClient<'_>,
    stream_generation: &str,
    transaction: &WalTransaction,
) -> Result<(), PoisonFailure> {
    let event_count = client
        .select(
            "SELECT count(*)::bigint AS count
             FROM synchro.sync_wal_events
             WHERE stream_generation = $1 AND commit_lsn = $2::pg_lsn",
            None,
            &[
                stream_generation.into(),
                format_lsn(transaction.commit_lsn).as_str().into(),
            ],
        )
        .map_err(|_| failure("validation_failed", transaction.commit_lsn))?
        .first()
        .get_by_name::<i64, &str>("count")
        .map_err(|_| failure("validation_failed", transaction.commit_lsn))?
        .unwrap_or(-1);
    let recorded = client
        .select(
            "SELECT event_count
             FROM synchro.sync_wal_transactions
             WHERE stream_generation = $1 AND commit_lsn = $2::pg_lsn",
            None,
            &[
                stream_generation.into(),
                format_lsn(transaction.commit_lsn).as_str().into(),
            ],
        )
        .map_err(|_| failure("validation_failed", transaction.commit_lsn))?
        .first()
        .get_by_name::<i64, &str>("event_count")
        .map_err(|_| failure("validation_failed", transaction.commit_lsn))?
        .unwrap_or(-2);
    if event_count != recorded {
        return Err(failure("validation_failed", transaction.commit_lsn));
    }
    client
        .update(
            "UPDATE synchro.sync_wal_transactions
             SET replay_count = replay_count + 1
             WHERE stream_generation = $1 AND commit_lsn = $2::pg_lsn",
            None,
            &[
                stream_generation.into(),
                format_lsn(transaction.commit_lsn).as_str().into(),
            ],
        )
        .map_err(|_| failure("materialization_failed", transaction.commit_lsn))?;
    Ok(())
}

fn validate_progress_order(
    client: &mut SpiClient<'_>,
    stream_generation: &str,
    transaction: &WalTransaction,
) -> Result<(), PoisonFailure> {
    let rows = client
        .update(
            "SELECT stream_generation::text AS stream_generation,
                    materialized_commit_lsn::text AS commit_lsn,
                    materialized_end_lsn::text AS end_lsn
             FROM synchro.sync_wal_progress WHERE singleton = true FOR UPDATE",
            None,
            &[],
        )
        .map_err(|_| failure("validation_failed", transaction.commit_lsn))?;
    let row = rows.first();
    let progress_stream = row
        .get_by_name::<String, &str>("stream_generation")
        .map_err(|_| failure("validation_failed", transaction.commit_lsn))?
        .unwrap_or_default();
    let prior_commit = row
        .get_by_name::<String, &str>("commit_lsn")
        .map_err(|_| failure("validation_failed", transaction.commit_lsn))?
        .and_then(|value| parse_lsn(&value));
    let prior_end = row
        .get_by_name::<String, &str>("end_lsn")
        .map_err(|_| failure("validation_failed", transaction.commit_lsn))?
        .and_then(|value| parse_lsn(&value));
    let blocked_by_barrier = client
        .select(
            "SELECT EXISTS (
                 SELECT 1
                 FROM synchro.sync_stream_resets reset
                 WHERE reset.operation_kind = 'projection_bootstrap'
                   AND reset.lifecycle = 'catching_up'
                   AND reset.source_stream_generation = $1
                   AND reset.activation_barrier IS NOT NULL
                   AND $2::pg_lsn > reset.activation_barrier
             ) AS blocked",
            None,
            &[
                stream_generation.into(),
                format_lsn(transaction.end_lsn).as_str().into(),
            ],
        )
        .map_err(|_| failure("validation_failed", transaction.commit_lsn))?
        .first()
        .get_by_name::<bool, &str>("blocked")
        .map_err(|_| failure("validation_failed", transaction.commit_lsn))?
        .unwrap_or(true);
    if blocked_by_barrier {
        return Err(failure("activation_barrier", transaction.commit_lsn));
    }
    if progress_stream != stream_generation
        || prior_commit.is_some_and(|value| value >= transaction.commit_lsn)
        || prior_end.is_some_and(|value| value >= transaction.end_lsn)
    {
        return Err(failure("validation_failed", transaction.commit_lsn));
    }
    Ok(())
}

fn parse_registry_activations(transaction: &WalTransaction) -> Result<Vec<i64>, PoisonFailure> {
    let mut activations = Vec::new();
    for message in transaction
        .messages
        .iter()
        .filter(|message| message.prefix == REGISTRY_PREFIX)
    {
        if message.content.len() > MAX_CONTROL_MESSAGE_BYTES {
            return Err(failure("validation_failed", transaction.commit_lsn));
        }
        let activation: RegistryActivation = serde_json::from_slice(&message.content)
            .map_err(|_| failure("validation_failed", transaction.commit_lsn))?;
        if activation.action != "activate" || activation.generation <= 0 {
            return Err(failure("validation_failed", transaction.commit_lsn));
        }
        activations.push(activation.generation);
    }
    Ok(activations)
}

fn validate_activation_chain(
    client: &SpiClient<'_>,
    stream_generation: &str,
    active_generation: i64,
    activations: &[i64],
    transaction: &WalTransaction,
) -> Result<(), PoisonFailure> {
    let mut parent = active_generation;
    let mut seen = HashSet::new();
    for generation in activations {
        if !seen.insert(*generation) {
            return Err(failure_with_detail(
                "validation_failed",
                transaction.commit_lsn,
                "registry activation contained a duplicate generation",
            ));
        }
        let rows = client
            .select(
                "SELECT parent_generation, validated, state,
                        stream_generation::text AS stream_generation
                 FROM synchro.sync_registry_generations
                 WHERE generation = $1",
                None,
                &[(*generation).into()],
            )
            .map_err(|_| {
                failure_with_detail(
                    "validation_failed",
                    transaction.commit_lsn,
                    "loading registry activation state failed",
                )
            })?;
        let Some(row) = rows.into_iter().next() else {
            return Err(failure_with_detail(
                "validation_failed",
                transaction.commit_lsn,
                "registry activation generation is missing",
            ));
        };
        let actual_parent = row
            .get_by_name::<i64, &str>("parent_generation")
            .map_err(|_| {
                failure_with_detail(
                    "validation_failed",
                    transaction.commit_lsn,
                    "reading registry activation parent failed",
                )
            })?;
        let validated = row
            .get_by_name::<bool, &str>("validated")
            .map_err(|_| {
                failure_with_detail(
                    "validation_failed",
                    transaction.commit_lsn,
                    "reading registry activation validation state failed",
                )
            })?
            .unwrap_or(false);
        let state = row
            .get_by_name::<String, &str>("state")
            .map_err(|_| {
                failure_with_detail(
                    "validation_failed",
                    transaction.commit_lsn,
                    "reading registry activation lifecycle state failed",
                )
            })?
            .unwrap_or_default();
        let stream = row
            .get_by_name::<String, &str>("stream_generation")
            .map_err(|_| {
                failure_with_detail(
                    "validation_failed",
                    transaction.commit_lsn,
                    "reading registry activation stream generation failed",
                )
            })?
            .unwrap_or_default();
        if actual_parent != Some(parent)
            || !validated
            || state != "pending"
            || stream != stream_generation
        {
            return Err(failure_with_detail(
                "validation_failed",
                transaction.commit_lsn,
                "registry activation is not a validated pending generation",
            ));
        }
        parent = *generation;
    }
    Ok(())
}

fn parse_fence_messages(transaction: &WalTransaction) -> Result<Vec<FenceMessage>, PoisonFailure> {
    let mut fences = Vec::new();
    let mut ids = HashSet::new();
    let mut ordinals = HashSet::new();
    for message in transaction
        .messages
        .iter()
        .filter(|message| message.prefix == FENCE_PREFIX)
    {
        if message.content.len() > MAX_CONTROL_MESSAGE_BYTES {
            log!("synchro WAL fence message exceeded the bounded size");
            return Err(failure("fence_correlation_failed", transaction.commit_lsn));
        }
        let fence: FenceMessage = serde_json::from_slice(&message.content).map_err(|error| {
            log!(
                "synchro WAL fence message did not match the required shape: {}",
                error
            );
            failure("fence_correlation_failed", transaction.commit_lsn)
        })?;
        let expected_ordinal = u64::try_from(fences.len())
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| failure("fence_correlation_failed", transaction.commit_lsn))?;
        let shape_valid = match fence.registration_kind.as_str() {
            "synced" => {
                fence.table_id.is_some()
                    && fence.old_capture_key.is_none()
                    && fence.new_capture_key.is_none()
            }
            "capture_dependency" => {
                fence.table_id.is_none()
                    && fence.old_record_id.is_none()
                    && fence.new_record_id.is_none()
            }
            _ => false,
        };
        if fence.dml_ordinal != expected_ordinal
            || !shape_valid
            || !ids.insert(fence.fence_id.clone())
            || !ordinals.insert(fence.dml_ordinal)
        {
            log!("synchro WAL fence source ordering was invalid");
            return Err(failure("fence_correlation_failed", transaction.commit_lsn));
        }
        fences.push(fence);
    }
    Ok(fences)
}

fn correlate_events<'a>(
    client: &SpiClient<'_>,
    transaction: &'a WalTransaction,
    registry: &'a [TableRegistration],
    fences: &'a [FenceMessage],
    fence_target: FenceTarget<'_>,
) -> Result<Vec<ApplicableEvent<'a>>, PoisonFailure> {
    let mut applicable = Vec::new();
    let mut applicable_events = Vec::new();
    for event in &transaction.events {
        if let Some(registration) = find_registration(registry, &event.relation) {
            applicable_events.push((event, registration));
        }
    }
    let applicable_fences: Vec<&FenceMessage> = fences
        .iter()
        .filter(|fence| {
            registry
                .iter()
                .any(|registration| registration.relation_id == fence.relation_id)
        })
        .collect();
    if applicable_events.len() != applicable_fences.len() {
        log!(
            "synchro WAL fence count did not match registered source events: events={}, fences={}",
            applicable_events.len(),
            applicable_fences.len()
        );
        return Err(failure("fence_correlation_failed", transaction.commit_lsn));
    }

    for ((event, registration), fence) in applicable_events.into_iter().zip(applicable_fences) {
        let (old_record_id, new_record_id, old_capture_key, new_capture_key) =
            if registration.is_synced() {
                let old_record_id = event
                    .before
                    .as_ref()
                    .map(|image| registered_id(image, &registration.pk_column))
                    .transpose()
                    .map_err(|_| failure("validation_failed", transaction.commit_lsn))?;
                let new_record_id = event
                    .after
                    .as_ref()
                    .map(|image| registered_id(image, &registration.pk_column))
                    .transpose()
                    .map_err(|_| failure("validation_failed", transaction.commit_lsn))?;
                let old_record_id =
                    if event.operation == ChangeOperation::Update && old_record_id.is_none() {
                        new_record_id.clone()
                    } else {
                        old_record_id
                    };
                (old_record_id, new_record_id, None, None)
            } else {
                let old_capture_key = event
                    .before
                    .as_ref()
                    .map(|image| capture_dependency_key(client, registration, image))
                    .transpose()
                    .map_err(|_| failure("validation_failed", transaction.commit_lsn))?;
                let new_capture_key = event
                    .after
                    .as_ref()
                    .map(|image| capture_dependency_key(client, registration, image))
                    .transpose()
                    .map_err(|_| failure("validation_failed", transaction.commit_lsn))?;
                let old_capture_key =
                    if event.operation == ChangeOperation::Update && old_capture_key.is_none() {
                        new_capture_key.clone()
                    } else {
                        old_capture_key
                    };
                (None, None, old_capture_key, new_capture_key)
            };
        let operation_name = operation_name(event.operation);
        let expected_ordinal = event
            .event_ordinal
            .checked_add(1)
            .ok_or_else(|| failure("fence_correlation_failed", transaction.commit_lsn))?;
        if fence.dml_ordinal != expected_ordinal
            || fence.relation_id != registration.relation_id
            || fence.registration_kind != registration.registration_kind.as_str()
            || fence.table_id.as_deref()
                != registration
                    .is_synced()
                    .then_some(registration.table_id.as_str())
            || fence.physical_schema != event.relation.namespace
            || fence.physical_relation != event.relation.name
            || fence.physical_relation_oid != event.relation.oid
            || fence.operation != operation_name
            || fence.old_record_id != old_record_id
            || fence.new_record_id != new_record_id
            || fence.old_capture_key != old_capture_key
            || fence.new_capture_key != new_capture_key
        {
            log!(
                "synchro WAL fence metadata correlation failed at source ordinal {}",
                event.event_ordinal
            );
            return Err(failure("fence_correlation_failed", transaction.commit_lsn));
        }
        validate_fence_row(
            client,
            fence,
            transaction.xid,
            transaction.commit_lsn,
            event.event_ordinal,
            fence_target,
        )
        .map_err(|_| {
            log!(
                "synchro WAL durable fence correlation failed at source ordinal {}",
                event.event_ordinal
            );
            failure("fence_correlation_failed", transaction.commit_lsn)
        })?;

        let mut operation = event.operation;
        let mut record_id = if registration.is_synced() {
            new_record_id
                .clone()
                .or_else(|| old_record_id.clone())
                .ok_or_else(|| failure("validation_failed", transaction.commit_lsn))?
        } else {
            serde_json::to_string(
                new_capture_key
                    .as_ref()
                    .or(old_capture_key.as_ref())
                    .ok_or_else(|| failure("validation_failed", transaction.commit_lsn))?,
            )
            .map_err(|_| failure("validation_failed", transaction.commit_lsn))?
        };
        if registration.is_synced()
            && event.operation == ChangeOperation::Update
            && registration.has_deleted_at
        {
            let after_deleted = event
                .after
                .as_ref()
                .and_then(|image| image.get(&registration.deleted_at_col))
                .is_some_and(|value| !matches!(value, TupleValue::Null));
            if after_deleted {
                operation = ChangeOperation::Delete;
                record_id = old_record_id
                    .clone()
                    .or_else(|| new_record_id.clone())
                    .ok_or_else(|| failure("validation_failed", transaction.commit_lsn))?;
            }
        }
        applicable.push(ApplicableEvent {
            event,
            registration,
            operation,
            operation_name,
            record_id,
            old_capture_key,
            new_capture_key,
            row_version: fence.row_version.clone(),
            fence_id: fence.fence_id.clone(),
        });
    }

    Ok(applicable)
}

fn validate_fence_row(
    client: &SpiClient<'_>,
    fence: &FenceMessage,
    source_xid: u32,
    commit_lsn: u64,
    event_ordinal: u64,
    target: FenceTarget<'_>,
) -> Result<(), String> {
    let coverage_predicate = match target {
        FenceTarget::Active => "fence.coverage = 'pending'",
        FenceTarget::Candidate { .. } => {
            "(
                    (fence.coverage = 'materialized'
                     AND fence.stream_generation = $16
                     AND fence.commit_lsn = $17::pg_lsn
                     AND fence.event_ordinal = $18)
                    OR
                    (fence.coverage = 'pending'
                     AND EXISTS (
                         SELECT 1 FROM synchro.sync_registry target_registry
                         WHERE target_registry.registry_generation = $20
                           AND target_registry.relation_id = fence.relation_id
                     )
                     AND NOT EXISTS (
                         SELECT 1 FROM synchro.sync_registry source_registry
                         WHERE source_registry.registry_generation = $19
                           AND source_registry.relation_id = fence.relation_id
                     ))
                 )"
        }
    };
    let query = format!(
        "SELECT count(*)::bigint AS count
         FROM synchro.sync_write_fences fence
         WHERE fence.fence_id = $1::uuid
            AND fence.dml_ordinal = $2
            AND fence.relation_id = $3::uuid
            AND fence.registration_kind = $4
            AND fence.table_id::text IS NOT DISTINCT FROM $5
            AND fence.physical_schema = $6
            AND fence.physical_relation = $7
            AND fence.physical_relation_oid = $8::oid
            AND fence.operation = $9
            AND fence.old_record_id IS NOT DISTINCT FROM $10
            AND fence.new_record_id IS NOT DISTINCT FROM $11
            AND fence.old_capture_key IS NOT DISTINCT FROM $12
            AND fence.new_capture_key IS NOT DISTINCT FROM $13
            AND fence.row_version = $14::uuid
            AND (fence.transaction_xid::text::numeric % 4294967296) = $15
            AND {coverage_predicate}"
    );
    let mut values = vec![
        fence.fence_id.as_str().into(),
        i64::try_from(fence.dml_ordinal).unwrap_or(i64::MAX).into(),
        fence.relation_id.as_str().into(),
        fence.registration_kind.as_str().into(),
        fence.table_id.as_deref().into(),
        fence.physical_schema.as_str().into(),
        fence.physical_relation.as_str().into(),
        i64::from(fence.physical_relation_oid).into(),
        fence.operation.as_str().into(),
        fence.old_record_id.as_deref().into(),
        fence.new_record_id.as_deref().into(),
        fence.old_capture_key.clone().map(pgrx::JsonB).into(),
        fence.new_capture_key.clone().map(pgrx::JsonB).into(),
        fence.row_version.as_str().into(),
        i64::from(source_xid).into(),
    ];
    let commit_lsn = format_lsn(commit_lsn);
    if let FenceTarget::Candidate {
        source_stream_generation,
        source_registry_generation,
        target_registry_generation,
    } = target
    {
        values.extend([
            source_stream_generation.into(),
            commit_lsn.as_str().into(),
            i64::try_from(event_ordinal)
                .map_err(|_| "fence event ordinal is invalid".to_string())?
                .into(),
            source_registry_generation.into(),
            target_registry_generation.into(),
        ]);
    }
    let count = client
        .select(&query, None, &values)
        .map_err(|_| "loading fence failed".to_string())?
        .first()
        .get_by_name::<i64, &str>("count")
        .map_err(|_| "loading fence failed".to_string())?
        .unwrap_or(0);
    if count != 1 {
        return Err("fence correlation failed".to_string());
    }
    Ok(())
}

fn persist_events_and_projections(
    client: &mut SpiClient<'_>,
    target: ProjectionTarget<'_>,
    transaction: &WalTransaction,
    registry: &[TableRegistration],
    events: &[ApplicableEvent<'_>],
) -> Result<PersistedEvents, PoisonFailure> {
    let mut impacts = Vec::with_capacity(events.len());
    let mut dependency_events = Vec::with_capacity(events.len());
    for event_chunk_input in events.chunks(JSONB_BATCH_SIZE) {
        let mut event_rows = Vec::with_capacity(event_chunk_input.len());
        let mut fence_rows = Vec::with_capacity(event_chunk_input.len());
        for event in event_chunk_input {
            let event_ordinal = i64::try_from(event.event.event_ordinal)
                .map_err(|_| failure("validation_failed", transaction.commit_lsn))?;
            event_rows.push(serde_json::json!({
                "write_ordinal": event_ordinal,
                "event_ordinal": event_ordinal,
                "bootstrap_id": target.bootstrap_id(),
                "relation_id": event.registration.relation_id,
                "registration_kind": event.registration.registration_kind.as_str(),
                "physical_schema": event.event.relation.namespace,
                "physical_relation": event.event.relation.name,
                "physical_relation_oid": i64::from(event.event.relation.oid),
                "operation": event.operation_name,
                "fence_id": event.fence_id,
            }));
            fence_rows.push(serde_json::json!({
                "fence_id": event.fence_id,
                "event_ordinal": event_ordinal,
            }));
        }

        match target {
            ProjectionTarget::Active { stream_generation } => {
                let event_count = event_rows.len();
                let inserted = client
                    .update(
                        "INSERT INTO synchro.sync_wal_events (
                             stream_generation, commit_lsn, event_ordinal, relation_id,
                             registration_kind, physical_schema, physical_relation,
                             physical_relation_oid, operation, fence_id
                         )
                         SELECT $3, $2::pg_lsn, input.event_ordinal,
                                input.relation_id::uuid, input.registration_kind,
                                input.physical_schema, input.physical_relation,
                                input.physical_relation_oid::oid, input.operation,
                                input.fence_id::uuid
                         FROM jsonb_to_recordset($1::jsonb) AS input(
                             write_ordinal bigint,
                             event_ordinal bigint,
                             bootstrap_id text,
                             relation_id text,
                             registration_kind text,
                             physical_schema text,
                             physical_relation text,
                             physical_relation_oid bigint,
                             operation text,
                             fence_id text
                         )
                         ORDER BY input.write_ordinal
                         RETURNING event_ordinal",
                        None,
                        &[
                            pgrx::JsonB(serde_json::Value::Array(event_rows)).into(),
                            format_lsn(transaction.commit_lsn).as_str().into(),
                            stream_generation.into(),
                        ],
                    )
                    .map_err(|_| failure("materialization_failed", transaction.commit_lsn))?;
                if inserted.len() != event_count {
                    return Err(failure("materialization_failed", transaction.commit_lsn));
                }
                let fence_count = fence_rows.len();
                let covered = client
                    .update(
                        "UPDATE synchro.sync_write_fences fence
                         SET coverage = 'materialized', stream_generation = $2,
                             commit_lsn = $3::pg_lsn, event_ordinal = input.event_ordinal,
                             materialized_at = now()
                         FROM jsonb_to_recordset($1::jsonb) AS input(
                             fence_id text, event_ordinal bigint
                         )
                         WHERE fence.fence_id = input.fence_id::uuid
                           AND fence.coverage = 'pending'
                         RETURNING fence.fence_id",
                        None,
                        &[
                            pgrx::JsonB(serde_json::Value::Array(fence_rows)).into(),
                            stream_generation.into(),
                            format_lsn(transaction.commit_lsn).as_str().into(),
                        ],
                    )
                    .map_err(|_| failure("fence_correlation_failed", transaction.commit_lsn))?
                    .len();
                if covered != fence_count {
                    return Err(failure("fence_correlation_failed", transaction.commit_lsn));
                }
            }
            ProjectionTarget::Candidate { .. } => {
                let event_count = event_rows.len();
                let inserted = client
                    .update(
                        "INSERT INTO synchro.sync_projection_bootstrap_events (
                             bootstrap_id, commit_lsn, event_ordinal, relation_id,
                             registration_kind, physical_schema, physical_relation,
                             physical_relation_oid, operation, fence_id
                         )
                         SELECT input.bootstrap_id::uuid, $2::pg_lsn, input.event_ordinal,
                                input.relation_id::uuid, input.registration_kind,
                                input.physical_schema, input.physical_relation,
                                input.physical_relation_oid::oid, input.operation,
                                input.fence_id::uuid
                         FROM jsonb_to_recordset($1::jsonb) AS input(
                             write_ordinal bigint,
                             event_ordinal bigint,
                             bootstrap_id text,
                             relation_id text,
                             registration_kind text,
                             physical_schema text,
                             physical_relation text,
                             physical_relation_oid bigint,
                             operation text,
                             fence_id text
                         )
                         ORDER BY input.write_ordinal
                         RETURNING event_ordinal",
                        None,
                        &[
                            pgrx::JsonB(serde_json::Value::Array(event_rows)).into(),
                            format_lsn(transaction.commit_lsn).as_str().into(),
                        ],
                    )
                    .map_err(|_| failure("materialization_failed", transaction.commit_lsn))?;
                if inserted.len() != event_count {
                    return Err(failure("materialization_failed", transaction.commit_lsn));
                }
            }
        }
    }

    for event in events {
        if let ProjectionTarget::Candidate { .. } = target {
            if event.registration.is_synced() {
                persist_candidate_row_version(client, target, event, transaction.commit_lsn)?;
            }
        }
        if event.registration.is_capture_dependency() {
            let dependency_event =
                persist_capture_dependency_event(client, target, transaction, event)?;
            dependency_events.push(dependency_event);
            continue;
        }

        let prior = load_captured_row(client, target, event.registration, &event.record_id)
            .map_err(|_| failure("projection_write_failed", transaction.commit_lsn))?;
        let dependency_old_row = prior.as_ref().map(|captured| captured.row_data.clone());
        let (digest, delete_projection_image, dependency_new_row) = match event.event.operation {
            ChangeOperation::Insert => {
                if prior.is_some() {
                    return Err(failure("projection_write_failed", transaction.commit_lsn));
                }
                let after = capture_after_projection(client, event, None)
                    .map_err(|_| failure("projection_write_failed", transaction.commit_lsn))?;
                persist_projection(client, target, transaction, event, "after", &after)?;
                persist_current_row(client, target, transaction, event, &after)?;
                let new_row = (!after.deleted).then(|| after.row_data.clone());
                (Some(after.digest), None, new_row)
            }
            ChangeOperation::Update => {
                let prior = prior
                    .ok_or_else(|| failure("projection_write_failed", transaction.commit_lsn))?;
                persist_projection(client, target, transaction, event, "before", &prior)?;
                let after = capture_after_projection(client, event, Some(&prior))
                    .map_err(|_| failure("projection_write_failed", transaction.commit_lsn))?;
                persist_projection(client, target, transaction, event, "after", &after)?;
                persist_current_row(client, target, transaction, event, &after)?;
                let delete_image = (event.operation == ChangeOperation::Delete).then_some("after");
                let new_row = (!after.deleted).then(|| after.row_data.clone());
                (Some(after.digest), delete_image, new_row)
            }
            ChangeOperation::Delete => {
                let prior = prior
                    .ok_or_else(|| failure("projection_write_failed", transaction.commit_lsn))?;
                persist_projection(client, target, transaction, event, "before", &prior)?;
                delete_current_row(client, target, event)?;
                (None, None, None)
            }
        };

        let registration_index = registry
            .iter()
            .position(|candidate| candidate.relation_id == event.registration.relation_id)
            .ok_or_else(|| failure("validation_failed", transaction.commit_lsn))?;
        impacts.push(ImpactedRow {
            registration_index,
            record_id: event.record_id.clone(),
            operation: event.operation,
            direct_change: true,
            event_ordinal: event.event.event_ordinal,
            row_version: event.row_version.clone(),
            delete_projection_image,
            digest,
        });
        dependency_events.push(DependencyEvent {
            dependency_relation_id: event.registration.relation_id.clone(),
            dependency_registration_kind: event.registration.registration_kind,
            event_ordinal: event.event.event_ordinal,
            old_row: dependency_old_row,
            new_row: dependency_new_row,
        });
    }
    Ok(PersistedEvents {
        direct_impacts: impacts,
        dependency_events,
    })
}

fn persist_candidate_row_version(
    client: &mut SpiClient<'_>,
    target: ProjectionTarget<'_>,
    event: &ApplicableEvent<'_>,
    commit_lsn: u64,
) -> Result<(), PoisonFailure> {
    let ProjectionTarget::Candidate { bootstrap_id, .. } = target else {
        return Ok(());
    };
    if !event.registration.is_synced() {
        return Ok(());
    }
    client
        .update(
            "INSERT INTO synchro.sync_stream_reset_row_versions (
                 reset_id, relation_id, record_id, row_version, fence_id,
                 source_reset_id, deleted, baseline_generated, staged_at
             ) VALUES (
                 $1::uuid, $2::uuid, $3, $4::uuid, $5::uuid,
                 NULL, $6, false, now()
             )
             ON CONFLICT (reset_id, relation_id, record_id) DO UPDATE SET
                 row_version = EXCLUDED.row_version,
                 fence_id = EXCLUDED.fence_id,
                 source_reset_id = NULL,
                 deleted = EXCLUDED.deleted,
                 baseline_generated = false,
                 staged_at = now()",
            None,
            &[
                bootstrap_id.into(),
                event.registration.relation_id.as_str().into(),
                event.record_id.as_str().into(),
                event.row_version.as_str().into(),
                event.fence_id.as_str().into(),
                (event.operation == ChangeOperation::Delete).into(),
            ],
        )
        .map_err(|_| failure("projection_write_failed", commit_lsn))?;
    Ok(())
}

fn persist_capture_dependency_event(
    client: &mut SpiClient<'_>,
    target: ProjectionTarget<'_>,
    transaction: &WalTransaction,
    event: &ApplicableEvent<'_>,
) -> Result<DependencyEvent, PoisonFailure> {
    let current_key = event
        .new_capture_key
        .as_ref()
        .or(event.old_capture_key.as_ref())
        .ok_or_else(|| failure("validation_failed", transaction.commit_lsn))?;
    let prior = load_capture_dependency_row(client, target, event.registration, current_key)
        .map_err(|_| failure("projection_write_failed", transaction.commit_lsn))?;
    let old_row = prior.as_ref().map(|(_, row)| row.clone());
    let new_row = match event.event.operation {
        ChangeOperation::Insert => {
            if prior.is_some() {
                return Err(failure("projection_write_failed", transaction.commit_lsn));
            }
            let row = capture_dependency_projection(
                client,
                event.registration,
                event
                    .event
                    .after
                    .as_ref()
                    .ok_or_else(|| failure("validation_failed", transaction.commit_lsn))?,
                None,
            )
            .map_err(|_| failure("projection_write_failed", transaction.commit_lsn))?;
            persist_capture_dependency_projection(
                client,
                target,
                transaction,
                event,
                "after",
                current_key,
                &row,
                false,
            )?;
            persist_current_capture_dependency(
                client,
                target,
                transaction,
                event,
                current_key,
                &row,
            )?;
            Some(row)
        }
        ChangeOperation::Update => {
            let (prior_key, prior_row) = prior
                .as_ref()
                .ok_or_else(|| failure("projection_write_failed", transaction.commit_lsn))?;
            persist_capture_dependency_projection(
                client,
                target,
                transaction,
                event,
                "before",
                prior_key,
                prior_row,
                false,
            )?;
            let row = capture_dependency_projection(
                client,
                event.registration,
                event
                    .event
                    .after
                    .as_ref()
                    .ok_or_else(|| failure("validation_failed", transaction.commit_lsn))?,
                Some(prior_row),
            )
            .map_err(|_| failure("projection_write_failed", transaction.commit_lsn))?;
            persist_capture_dependency_projection(
                client,
                target,
                transaction,
                event,
                "after",
                current_key,
                &row,
                false,
            )?;
            persist_current_capture_dependency(
                client,
                target,
                transaction,
                event,
                current_key,
                &row,
            )?;
            Some(row)
        }
        ChangeOperation::Delete => {
            let (prior_key, prior_row) = prior
                .as_ref()
                .ok_or_else(|| failure("projection_write_failed", transaction.commit_lsn))?;
            persist_capture_dependency_projection(
                client,
                target,
                transaction,
                event,
                "before",
                prior_key,
                prior_row,
                true,
            )?;
            let (table, id_column, id_value) = match target {
                ProjectionTarget::Active { .. } => {
                    ("synchro.sync_capture_dependency_rows", "", None)
                }
                ProjectionTarget::Candidate { bootstrap_id, .. } => (
                    "synchro.sync_stream_reset_capture_dependency_rows",
                    "reset_id = $3::uuid AND ",
                    Some(bootstrap_id),
                ),
            };
            let query = format!(
                "DELETE FROM {table}
                 WHERE {id_column}relation_id = $1::uuid AND capture_key = $2"
            );
            let mut values = vec![
                event.registration.relation_id.as_str().into(),
                pgrx::JsonB(prior_key.clone()).into(),
            ];
            if let Some(bootstrap_id) = id_value {
                values.push(bootstrap_id.into());
            }
            let deleted = client
                .update(&query, None, &values)
                .map_err(|_| failure("projection_write_failed", transaction.commit_lsn))?
                .len();
            if deleted != 1 {
                return Err(failure("projection_write_failed", transaction.commit_lsn));
            }
            None
        }
    };
    Ok(DependencyEvent {
        dependency_relation_id: event.registration.relation_id.clone(),
        dependency_registration_kind: event.registration.registration_kind,
        event_ordinal: event.event.event_ordinal,
        old_row,
        new_row,
    })
}

fn capture_dependency_key(
    client: &SpiClient<'_>,
    registration: &TableRegistration,
    image: &TupleImage,
) -> Result<serde_json::Value, String> {
    let row = canonical_capture_dependency_values(
        client,
        registration,
        image,
        &registration.capture_key_columns,
        None,
    )?;
    if !row.is_object() || row.as_object().is_none_or(serde_json::Map::is_empty) {
        return Err("capture dependency key is invalid".to_string());
    }
    Ok(row)
}

fn capture_dependency_projection(
    client: &SpiClient<'_>,
    registration: &TableRegistration,
    image: &TupleImage,
    prior: Option<&serde_json::Value>,
) -> Result<serde_json::Value, String> {
    let columns = registration
        .capture_fields
        .iter()
        .map(|field| field.physical_column.clone())
        .collect::<Vec<_>>();
    canonical_capture_dependency_values(client, registration, image, &columns, prior)
}

fn canonical_capture_dependency_values(
    client: &SpiClient<'_>,
    registration: &TableRegistration,
    image: &TupleImage,
    columns: &[String],
    prior: Option<&serde_json::Value>,
) -> Result<serde_json::Value, String> {
    let prior = prior.and_then(serde_json::Value::as_object);
    let mut raw = serde_json::Map::new();
    for column in columns {
        let value = image
            .get(column)
            .ok_or_else(|| format!("capture dependency image omits column {column}"))?;
        let value = match value {
            TupleValue::Null => serde_json::Value::Null,
            TupleValue::Text(bytes) => serde_json::Value::String(
                std::str::from_utf8(bytes)
                    .map_err(|_| format!("capture dependency column {column} has invalid text"))?
                    .to_string(),
            ),
            TupleValue::Binary(_) => {
                return Err(format!(
                    "capture dependency column {column} uses unsupported binary output"
                ));
            }
            TupleValue::Unchanged => {
                prior
                    .and_then(|row| row.get(column))
                    .cloned()
                    .ok_or_else(|| {
                        format!("unchanged capture dependency column {column} has no prior value")
                    })?
            }
        };
        raw.insert(column.clone(), value);
    }
    let relation = crate::registry::qualified_relation_name(
        &registration.physical_schema,
        &registration.physical_relation,
    );
    let mut row_data = client
        .select(
            &format!(
                "SELECT to_jsonb(projected) AS row_data
                 FROM jsonb_populate_record(NULL::{relation}, $1) AS projected"
            ),
            None,
            &[pgrx::JsonB(raw.into()).into()],
        )
        .map_err(|_| "canonicalizing capture dependency row failed".to_string())?
        .first()
        .get_by_name::<pgrx::JsonB, &str>("row_data")
        .map_err(|_| "reading canonical capture dependency row failed".to_string())?
        .ok_or_else(|| "canonical capture dependency row is missing".to_string())?
        .0;
    let object = row_data
        .as_object_mut()
        .ok_or_else(|| "canonical capture dependency row is invalid".to_string())?;
    object.retain(|column, _| columns.iter().any(|candidate| candidate == column));
    if object.len() != columns.len() {
        return Err("canonical capture dependency projection is incomplete".to_string());
    }
    Ok(row_data)
}

fn load_capture_dependency_row(
    client: &SpiClient<'_>,
    target: ProjectionTarget<'_>,
    registration: &TableRegistration,
    capture_key: &serde_json::Value,
) -> Result<Option<(serde_json::Value, serde_json::Value)>, String> {
    let (table, reset_predicate, bootstrap_id) = match target {
        ProjectionTarget::Active { .. } => ("synchro.sync_capture_dependency_rows", "", None),
        ProjectionTarget::Candidate { bootstrap_id, .. } => (
            "synchro.sync_stream_reset_capture_dependency_rows",
            "reset_id = $3::uuid AND ",
            Some(bootstrap_id),
        ),
    };
    let query = format!(
        "SELECT capture_key, row_data
         FROM {table}
         WHERE {reset_predicate}relation_id = $1::uuid AND capture_key = $2 AND NOT deleted"
    );
    let mut values = vec![
        registration.relation_id.as_str().into(),
        pgrx::JsonB(capture_key.clone()).into(),
    ];
    if let Some(bootstrap_id) = bootstrap_id {
        values.push(bootstrap_id.into());
    }
    let rows = client
        .select(&query, None, &values)
        .map_err(|_| "loading capture dependency row failed".to_string())?;
    let Some(row) = rows.into_iter().next() else {
        return Ok(None);
    };
    let key = row
        .get_by_name::<pgrx::JsonB, &str>("capture_key")
        .map_err(|_| "reading capture dependency key failed".to_string())?
        .ok_or_else(|| "capture dependency key is missing".to_string())?
        .0;
    let data = row
        .get_by_name::<pgrx::JsonB, &str>("row_data")
        .map_err(|_| "reading capture dependency row failed".to_string())?
        .ok_or_else(|| "capture dependency row is missing".to_string())?
        .0;
    Ok(Some((key, data)))
}

#[allow(clippy::too_many_arguments)]
fn persist_capture_dependency_projection(
    client: &mut SpiClient<'_>,
    target: ProjectionTarget<'_>,
    transaction: &WalTransaction,
    event: &ApplicableEvent<'_>,
    image_kind: &str,
    capture_key: &serde_json::Value,
    row_data: &serde_json::Value,
    deleted: bool,
) -> Result<(), PoisonFailure> {
    let ProjectionTarget::Active { stream_generation } = target else {
        return Ok(());
    };
    client
        .update(
            "INSERT INTO synchro.sync_capture_dependency_projections (
                 stream_generation, commit_lsn, event_ordinal, relation_id,
                 image_kind, registry_generation, capture_key, row_data, deleted
             ) VALUES ($1, $2::pg_lsn, $3, $4::uuid, $5, $6, $7, $8, $9)",
            None,
            &[
                stream_generation.into(),
                format_lsn(transaction.commit_lsn).as_str().into(),
                i64::try_from(event.event.event_ordinal)
                    .map_err(|_| failure("validation_failed", transaction.commit_lsn))?
                    .into(),
                event.registration.relation_id.as_str().into(),
                image_kind.into(),
                event.registration.registry_generation.into(),
                pgrx::JsonB(capture_key.clone()).into(),
                pgrx::JsonB(row_data.clone()).into(),
                deleted.into(),
            ],
        )
        .map_err(|_| failure("projection_write_failed", transaction.commit_lsn))?;
    Ok(())
}

fn persist_current_capture_dependency(
    client: &mut SpiClient<'_>,
    target: ProjectionTarget<'_>,
    transaction: &WalTransaction,
    event: &ApplicableEvent<'_>,
    capture_key: &serde_json::Value,
    row_data: &serde_json::Value,
) -> Result<(), PoisonFailure> {
    if let ProjectionTarget::Candidate {
        bootstrap_id,
        registry_generation,
    } = target
    {
        client
            .update(
                "INSERT INTO synchro.sync_stream_reset_capture_dependency_rows (
                     reset_id, relation_id, capture_key, row_data, deleted,
                     registry_generation, staged_at
                 ) VALUES ($1::uuid, $2::uuid, $3, $4, false, $5, now())
                 ON CONFLICT (reset_id, relation_id, capture_key) DO UPDATE SET
                     row_data = EXCLUDED.row_data,
                     deleted = false,
                     registry_generation = EXCLUDED.registry_generation,
                     staged_at = now()",
                None,
                &[
                    bootstrap_id.into(),
                    event.registration.relation_id.as_str().into(),
                    pgrx::JsonB(capture_key.clone()).into(),
                    pgrx::JsonB(row_data.clone()).into(),
                    registry_generation.into(),
                ],
            )
            .map_err(|_| failure("projection_write_failed", transaction.commit_lsn))?;
        return Ok(());
    }
    let ProjectionTarget::Active { stream_generation } = target else {
        unreachable!();
    };
    client
        .update(
            "INSERT INTO synchro.sync_capture_dependency_rows (
                 relation_id, capture_key, row_data, deleted,
                 source_stream_generation, source_commit_lsn, source_event_ordinal,
                 source_reset_id, registry_generation, updated_at
             ) VALUES ($1::uuid, $2, $3, false, $4, $5::pg_lsn, $6, NULL, $7, now())
             ON CONFLICT (relation_id, capture_key) DO UPDATE SET
                 row_data = EXCLUDED.row_data,
                 deleted = false,
                 source_stream_generation = EXCLUDED.source_stream_generation,
                 source_commit_lsn = EXCLUDED.source_commit_lsn,
                 source_event_ordinal = EXCLUDED.source_event_ordinal,
                 source_reset_id = NULL,
                 registry_generation = EXCLUDED.registry_generation,
                 updated_at = now()",
            None,
            &[
                event.registration.relation_id.as_str().into(),
                pgrx::JsonB(capture_key.clone()).into(),
                pgrx::JsonB(row_data.clone()).into(),
                stream_generation.into(),
                format_lsn(transaction.commit_lsn).as_str().into(),
                i64::try_from(event.event.event_ordinal)
                    .map_err(|_| failure("validation_failed", transaction.commit_lsn))?
                    .into(),
                event.registration.registry_generation.into(),
            ],
        )
        .map_err(|_| failure("projection_write_failed", transaction.commit_lsn))?;
    Ok(())
}

fn load_captured_row(
    client: &SpiClient<'_>,
    target: ProjectionTarget<'_>,
    registration: &TableRegistration,
    record_id: &str,
) -> Result<Option<CapturedRow>, String> {
    let (table, reset_predicate, bootstrap_id) = match target {
        ProjectionTarget::Active { .. } => ("synchro.sync_captured_rows", "", None),
        ProjectionTarget::Candidate { bootstrap_id, .. } => (
            "synchro.sync_stream_reset_captured_rows",
            "reset_id = $3::uuid AND ",
            Some(bootstrap_id),
        ),
    };
    let query = format!(
        "SELECT row_data, row_version::text AS row_version, checksum, deleted,
                registry_generation
         FROM {table}
         WHERE {reset_predicate}relation_id = $1::uuid AND record_id = $2"
    );
    let mut values = vec![registration.relation_id.as_str().into(), record_id.into()];
    if let Some(bootstrap_id) = bootstrap_id {
        values.push(bootstrap_id.into());
    }
    let rows = client
        .select(&query, None, &values)
        .map_err(|_| "loading captured row failed".to_string())?;
    let Some(row) = rows.into_iter().next() else {
        return Ok(None);
    };
    let row_data = row
        .get_by_name::<pgrx::JsonB, &str>("row_data")
        .map_err(|_| "reading captured row failed".to_string())?
        .ok_or_else(|| "captured row data is missing".to_string())?
        .0;
    let row_version = row
        .get_by_name::<String, &str>("row_version")
        .map_err(|_| "reading captured row failed".to_string())?
        .ok_or_else(|| "captured row version is missing".to_string())?;
    let digest = row
        .get_by_name::<Vec<u8>, &str>("checksum")
        .map_err(|_| "reading captured row failed".to_string())?
        .ok_or_else(|| "captured row digest is missing".to_string())?
        .try_into()
        .map(synchro_core::checksum::Sha256Digest::from_bytes)
        .map_err(|_| "captured row digest must contain exactly 32 octets".to_string())?;
    let deleted = row
        .get_by_name::<bool, &str>("deleted")
        .map_err(|_| "reading captured row failed".to_string())?
        .ok_or_else(|| "captured row deletion state is missing".to_string())?;
    let registry_generation = row
        .get_by_name::<i64, &str>("registry_generation")
        .map_err(|_| "reading captured row registry generation failed".to_string())?
        .ok_or_else(|| "captured row registry generation is missing".to_string())?;
    if registry_generation <= 0 {
        return Err("captured row registry generation is invalid".to_string());
    }
    Ok(Some(CapturedRow {
        row_data,
        row_version,
        digest,
        deleted,
        registry_generation,
    }))
}

fn capture_after_projection(
    client: &SpiClient<'_>,
    event: &ApplicableEvent<'_>,
    prior: Option<&CapturedRow>,
) -> Result<CapturedRow, String> {
    let image = event
        .event
        .after
        .as_ref()
        .ok_or_else(|| "source event has no after image".to_string())?;
    let prior_data = prior.and_then(|row| row.row_data.as_object());
    let mut raw = serde_json::Map::new();
    let mut native_json_values = serde_json::Map::new();
    for field in &event.registration.fields {
        let column = &field.physical_column;
        let value = image
            .get(column)
            .ok_or_else(|| format!("source after image omits synced column {column}"))?;
        let value = match value {
            TupleValue::Null => {
                if field.native_json {
                    native_json_values.insert(field.field_id.clone(), serde_json::Value::Null);
                }
                serde_json::Value::Null
            }
            TupleValue::Text(bytes) => {
                let text = std::str::from_utf8(bytes)
                    .map_err(|_| format!("synced column {column} has invalid text"))?
                    .to_string();
                if field.native_json {
                    native_json_values.insert(
                        field.field_id.clone(),
                        serde_json::Value::String(text.clone()),
                    );
                }
                serde_json::Value::String(text)
            }
            TupleValue::Binary(_) => {
                return Err(format!(
                    "synced column {column} uses unsupported binary output"
                ))
            }
            TupleValue::Unchanged => {
                let prior_value = prior_data
                    .and_then(|data| data.get(&field.field_id))
                    .cloned()
                    .ok_or_else(|| {
                        format!("unchanged synced column {column} has no prior value")
                    })?;
                if field.native_json {
                    native_json_values.insert(field.field_id.clone(), prior_value.clone());
                }
                prior_value
            }
        };
        raw.insert(column.clone(), value);
    }

    let query = format!(
        "SELECT {} AS row_data
         FROM jsonb_populate_record(NULL::{}, $1) AS projected",
        crate::pull::synced_row_projection_sql(event.registration, "projected"),
        crate::registry::qualified_relation_name(
            &event.registration.physical_schema,
            &event.registration.physical_relation,
        ),
    );
    let mut row_data = client
        .select(&query, None, &[pgrx::JsonB(raw.into()).into()])
        .map_err(|_| "canonicalizing captured row failed".to_string())?
        .first()
        .get_by_name::<pgrx::JsonB, &str>("row_data")
        .map_err(|_| "reading canonical captured row failed".to_string())?
        .ok_or_else(|| "canonical captured row is missing".to_string())?
        .0;
    let row_object = row_data
        .as_object_mut()
        .ok_or_else(|| "canonical captured row is not an object".to_string())?;
    row_object.extend(native_json_values);
    crate::pull::canonicalize_synced_row_data(event.registration, &mut row_data)?;
    let deleted = if event.registration.has_deleted_at {
        captured_row_deleted(
            &event.registration.fields,
            &event.registration.deleted_at_col,
            &row_data,
        )?
    } else {
        false
    };
    let digest = synced_row_digest(
        client,
        event.registration,
        &row_data,
        &event.record_id,
        &event.row_version,
    )?;
    Ok(CapturedRow {
        row_data,
        row_version: event.row_version.clone(),
        digest,
        deleted,
        registry_generation: event.registration.registry_generation,
    })
}

fn captured_row_deleted(
    fields: &[crate::registry::FieldRegistration],
    deleted_at_col: &str,
    row_data: &serde_json::Value,
) -> Result<bool, String> {
    let field = fields
        .iter()
        .find(|field| field.physical_column == deleted_at_col)
        .ok_or_else(|| "registered deletion field is missing".to_string())?;
    let value = row_data
        .get(&field.field_id)
        .ok_or_else(|| "captured deletion field is missing".to_string())?;
    Ok(!value.is_null())
}

fn persist_projection(
    client: &mut SpiClient<'_>,
    target: ProjectionTarget<'_>,
    transaction: &WalTransaction,
    event: &ApplicableEvent<'_>,
    image_kind: &str,
    captured: &CapturedRow,
) -> Result<(), PoisonFailure> {
    let ProjectionTarget::Active { stream_generation } = target else {
        return Ok(());
    };
    client
        .update(
            "INSERT INTO synchro.sync_captured_projections (
                 stream_generation, commit_lsn, event_ordinal, relation_id,
                 image_kind, registry_generation, record_id, row_data,
                  row_version, checksum, deleted
             ) VALUES (
                 $1, $2::pg_lsn, $3, $4::uuid, $5, $6, $7, $8,
                 $9::uuid, $10, $11
             )",
            None,
            &[
                stream_generation.into(),
                format_lsn(transaction.commit_lsn).as_str().into(),
                i64::try_from(event.event.event_ordinal)
                    .map_err(|_| failure("validation_failed", transaction.commit_lsn))?
                    .into(),
                event.registration.relation_id.as_str().into(),
                image_kind.into(),
                event.registration.registry_generation.into(),
                event.record_id.as_str().into(),
                pgrx::JsonB(captured.row_data.clone()).into(),
                captured.row_version.as_str().into(),
                captured.digest.as_bytes().to_vec().into(),
                captured.deleted.into(),
            ],
        )
        .map_err(|_| failure("projection_write_failed", transaction.commit_lsn))?;
    Ok(())
}

fn persist_current_row(
    client: &mut SpiClient<'_>,
    target: ProjectionTarget<'_>,
    transaction: &WalTransaction,
    event: &ApplicableEvent<'_>,
    captured: &CapturedRow,
) -> Result<(), PoisonFailure> {
    if let ProjectionTarget::Candidate {
        bootstrap_id,
        registry_generation,
    } = target
    {
        client
            .update(
                "INSERT INTO synchro.sync_stream_reset_captured_rows (
                     reset_id, relation_id, record_id, row_data, row_version,
                     checksum, deleted, registry_generation, staged_at
                 ) VALUES (
                     $1::uuid, $2::uuid, $3, $4, $5::uuid, $6, $7, $8, now()
                 )
                 ON CONFLICT (reset_id, relation_id, record_id) DO UPDATE SET
                     row_data = EXCLUDED.row_data,
                     row_version = EXCLUDED.row_version,
                     checksum = EXCLUDED.checksum,
                     deleted = EXCLUDED.deleted,
                     registry_generation = EXCLUDED.registry_generation,
                     staged_at = now()",
                None,
                &[
                    bootstrap_id.into(),
                    event.registration.relation_id.as_str().into(),
                    event.record_id.as_str().into(),
                    pgrx::JsonB(captured.row_data.clone()).into(),
                    captured.row_version.as_str().into(),
                    captured.digest.as_bytes().to_vec().into(),
                    captured.deleted.into(),
                    registry_generation.into(),
                ],
            )
            .map_err(|_| failure("projection_write_failed", transaction.commit_lsn))?;
        return Ok(());
    }
    let ProjectionTarget::Active { stream_generation } = target else {
        unreachable!();
    };
    client
        .update(
            "INSERT INTO synchro.sync_captured_rows (
                 relation_id, record_id, row_data, row_version, checksum, deleted,
                 source_stream_generation, source_commit_lsn, source_event_ordinal,
                 registry_generation, updated_at
             ) VALUES (
                 $1::uuid, $2, $3, $4::uuid, $5, $6,
                 $7, $8::pg_lsn, $9, $10, now()
             )
             ON CONFLICT (relation_id, record_id) DO UPDATE SET
                 row_data = EXCLUDED.row_data,
                 row_version = EXCLUDED.row_version,
                 checksum = EXCLUDED.checksum,
                 deleted = EXCLUDED.deleted,
                 source_stream_generation = EXCLUDED.source_stream_generation,
                 source_commit_lsn = EXCLUDED.source_commit_lsn,
                 source_event_ordinal = EXCLUDED.source_event_ordinal,
                 source_reset_id = NULL,
                 registry_generation = EXCLUDED.registry_generation,
                 updated_at = now()",
            None,
            &[
                event.registration.relation_id.as_str().into(),
                event.record_id.as_str().into(),
                pgrx::JsonB(captured.row_data.clone()).into(),
                captured.row_version.as_str().into(),
                captured.digest.as_bytes().to_vec().into(),
                captured.deleted.into(),
                stream_generation.into(),
                format_lsn(transaction.commit_lsn).as_str().into(),
                i64::try_from(event.event.event_ordinal)
                    .map_err(|_| failure("validation_failed", transaction.commit_lsn))?
                    .into(),
                event.registration.registry_generation.into(),
            ],
        )
        .map_err(|_| failure("projection_write_failed", transaction.commit_lsn))?;
    Ok(())
}

fn delete_current_row(
    client: &mut SpiClient<'_>,
    target: ProjectionTarget<'_>,
    event: &ApplicableEvent<'_>,
) -> Result<(), PoisonFailure> {
    let (table, reset_predicate, bootstrap_id) = match target {
        ProjectionTarget::Active { .. } => ("synchro.sync_captured_rows", "", None),
        ProjectionTarget::Candidate { bootstrap_id, .. } => (
            "synchro.sync_stream_reset_captured_rows",
            "reset_id = $3::uuid AND ",
            Some(bootstrap_id),
        ),
    };
    let query = format!(
        "DELETE FROM {table}
         WHERE {reset_predicate}relation_id = $1::uuid AND record_id = $2"
    );
    let mut values = vec![
        event.registration.relation_id.as_str().into(),
        event.record_id.as_str().into(),
    ];
    if let Some(bootstrap_id) = bootstrap_id {
        values.push(bootstrap_id.into());
    }
    let deleted = client
        .update(&query, None, &values)
        .map_err(|_| failure("projection_write_failed", 0))?
        .len();
    if deleted != 1 {
        return Err(failure("projection_write_failed", 0));
    }
    Ok(())
}

fn collect_membership_impacts(
    client: &mut SpiClient<'_>,
    target: ProjectionTarget<'_>,
    transaction: &WalTransaction,
    registry: &[TableRegistration],
    dependencies: &[MembershipDependency],
    persisted: PersistedEvents,
) -> Result<Vec<ImpactedRow>, PoisonFailure> {
    let mut impacts: HashMap<(usize, String), ImpactedRow> = persisted
        .direct_impacts
        .into_iter()
        .map(|impact| {
            (
                (impact.registration_index, impact.record_id.clone()),
                impact,
            )
        })
        .collect();
    let mut reevaluation_projections = Vec::new();

    for event in persisted.dependency_events {
        for dependency in dependencies
            .iter()
            .filter(|dependency| dependency.dependency_relation_id == event.dependency_relation_id)
            .filter(|dependency| {
                dependency.dependency_registration_kind == event.dependency_registration_kind
            })
        {
            if dependency.dependency_columns.is_empty() {
                return Err(failure("scope_evaluation_failed", transaction.commit_lsn));
            }
            let target_index = registry
                .iter()
                .position(|registration| registration.relation_id == dependency.target_relation_id)
                .ok_or_else(|| failure("scope_evaluation_failed", transaction.commit_lsn))?;
            let target_registration = &registry[target_index];
            let record_ids = resolve_dependency_impacts(
                client,
                dependency,
                target_registration,
                event.old_row.as_ref(),
                event.new_row.as_ref(),
            )
            .map_err(|_| failure("scope_evaluation_failed", transaction.commit_lsn))?;
            for record_id in record_ids {
                let key = (target_index, record_id.clone());
                let Some(captured) =
                    load_captured_row(client, target, target_registration, &record_id)
                        .map_err(|_| failure("scope_evaluation_failed", transaction.commit_lsn))?
                else {
                    continue;
                };
                if impacts
                    .get(&key)
                    .is_some_and(|impact| event.event_ordinal <= impact.event_ordinal)
                {
                    continue;
                }
                if let ProjectionTarget::Active { stream_generation } = target {
                    let event_ordinal = i64::try_from(event.event_ordinal)
                        .map_err(|_| failure("validation_failed", transaction.commit_lsn))?;
                    reevaluation_projections.push(serde_json::json!({
                        "event_ordinal": event_ordinal,
                        "relation_id": target_registration.relation_id,
                        "registry_generation": captured.registry_generation,
                        "record_id": record_id,
                        "row_version": captured.row_version,
                        "checksum_hex": captured.digest.to_lower_hex(),
                        "deleted": captured.deleted,
                    }));
                    if reevaluation_projections.len() == JSONB_BATCH_SIZE {
                        let projections = std::mem::replace(
                            &mut reevaluation_projections,
                            Vec::with_capacity(JSONB_BATCH_SIZE),
                        );
                        persist_reevaluation_projection_batch(
                            client,
                            stream_generation,
                            transaction.commit_lsn,
                            projections,
                        )
                        .map_err(|_| failure("projection_write_failed", transaction.commit_lsn))?;
                    }
                }
                if let Some(impact) = impacts.get_mut(&key) {
                    impact.event_ordinal = event.event_ordinal;
                    if !impact.direct_change {
                        impact.operation = if captured.deleted {
                            ChangeOperation::Delete
                        } else {
                            ChangeOperation::Update
                        };
                        impact.row_version = captured.row_version;
                        impact.delete_projection_image = captured.deleted.then_some("after");
                        impact.digest = Some(captured.digest);
                    }
                    continue;
                }
                impacts.insert(
                    key,
                    ImpactedRow {
                        registration_index: target_index,
                        record_id,
                        operation: if captured.deleted {
                            ChangeOperation::Delete
                        } else {
                            ChangeOperation::Update
                        },
                        direct_change: false,
                        event_ordinal: event.event_ordinal,
                        row_version: captured.row_version,
                        delete_projection_image: captured.deleted.then_some("after"),
                        digest: Some(captured.digest),
                    },
                );
            }
        }
    }
    if let ProjectionTarget::Active { stream_generation } = target {
        persist_reevaluation_projection_batch(
            client,
            stream_generation,
            transaction.commit_lsn,
            reevaluation_projections,
        )
        .map_err(|_| failure("projection_write_failed", transaction.commit_lsn))?;
    }

    let mut keyed_impacts = impacts
        .into_values()
        .map(|impact| {
            let registration = &registry[impact.registration_index];
            crate::pull::typed_primary_key_bytes(registration, &impact.record_id)
                .map(|typed_primary_key| (impact, typed_primary_key))
                .map_err(|_| failure("validation_failed", transaction.commit_lsn))
        })
        .collect::<Result<Vec<_>, _>>()?;
    keyed_impacts.sort_by(|(left, left_primary_key), (right, right_primary_key)| {
        left.event_ordinal
            .cmp(&right.event_ordinal)
            .then_with(|| {
                registry[left.registration_index]
                    .table_id
                    .as_bytes()
                    .cmp(registry[right.registration_index].table_id.as_bytes())
            })
            .then_with(|| left_primary_key.cmp(right_primary_key))
            .then_with(|| operation_rank(left.operation).cmp(&operation_rank(right.operation)))
    });
    Ok(keyed_impacts
        .into_iter()
        .map(|(impact, _)| impact)
        .collect())
}

pub(super) fn persist_reevaluation_projection_batch(
    client: &mut SpiClient<'_>,
    stream_generation: &str,
    commit_lsn: u64,
    projections: Vec<serde_json::Value>,
) -> Result<(), ()> {
    if projections.is_empty() {
        return Ok(());
    }
    if projections.len() > JSONB_BATCH_SIZE {
        return Err(());
    }
    let expected = i64::try_from(projections.len()).map_err(|_| ())?;
    let counts = client
        .update(
            "WITH projection_input AS (
                 SELECT event_ordinal, relation_id, registry_generation,
                        record_id, row_version, checksum_hex, deleted
                 FROM jsonb_to_recordset($1::jsonb) AS input(
                     event_ordinal bigint,
                     relation_id text,
                     registry_generation bigint,
                     record_id text,
                     row_version text,
                     checksum_hex text,
                     deleted boolean
                 )
             ), matched AS (
                 SELECT input.event_ordinal, input.relation_id, input.record_id,
                        captured.registry_generation, captured.row_data,
                        captured.row_version, captured.checksum, captured.deleted
                 FROM projection_input input
                 JOIN synchro.sync_captured_rows captured
                   ON captured.relation_id = input.relation_id::uuid
                  AND captured.record_id = input.record_id
                  AND captured.registry_generation = input.registry_generation
                  AND captured.row_version = input.row_version::uuid
                  AND captured.checksum = decode(input.checksum_hex, 'hex')
                  AND captured.deleted = input.deleted
             ), inserted AS (
                 INSERT INTO synchro.sync_captured_projections (
                     stream_generation, commit_lsn, event_ordinal, relation_id,
                     image_kind, registry_generation, record_id, row_data,
                     row_version, checksum, deleted
                 )
                 SELECT $2, $3::pg_lsn, matched.event_ordinal,
                        matched.relation_id::uuid, 'after',
                        matched.registry_generation, matched.record_id,
                        matched.row_data, matched.row_version, matched.checksum,
                        matched.deleted
                 FROM matched
                 WHERE (SELECT count(*) FROM matched) = $4
                 RETURNING record_id
             )
             SELECT (SELECT count(*) FROM matched) = $4
                    AND (SELECT count(*) FROM inserted) = $4 AS complete",
            None,
            &[
                pgrx::JsonB(serde_json::Value::Array(projections)).into(),
                stream_generation.into(),
                format_lsn(commit_lsn).as_str().into(),
                expected.into(),
            ],
        )
        .map_err(|_| ())?;
    if counts.len() != 1
        || counts
            .first()
            .get_by_name::<bool, &str>("complete")
            .map_err(|_| ())?
            != Some(true)
    {
        return Err(());
    }
    Ok(())
}

fn persist_impact_batch(
    client: &mut SpiClient<'_>,
    stream_generation: &str,
    commit_lsn: u64,
    changelog_effects: &[serde_json::Value],
    edge_deletes: &[serde_json::Value],
    edge_upserts: &[serde_json::Value],
) -> Result<(), PoisonFailure> {
    if changelog_effects.len() > JSONB_BATCH_SIZE
        || edge_deletes.len() > JSONB_BATCH_SIZE
        || edge_upserts.len() > JSONB_BATCH_SIZE
    {
        return Err(failure("validation_failed", commit_lsn));
    }
    let expected_effect_count = i64::try_from(changelog_effects.len())
        .map_err(|_| failure("validation_failed", commit_lsn))?;
    let expected_edge_delete_count =
        i64::try_from(edge_deletes.len()).map_err(|_| failure("validation_failed", commit_lsn))?;
    let expected_edge_upsert_count =
        i64::try_from(edge_upserts.len()).map_err(|_| failure("validation_failed", commit_lsn))?;
    let counts = client
        .update(
            "WITH effect_input AS (
                 SELECT write_ordinal, bucket_id, table_name, record_id, operation,
                        event_ordinal, effect_ordinal, relation_id, row_version,
                        projection_image
                 FROM jsonb_to_recordset($1::jsonb) AS input(
                     write_ordinal bigint,
                     bucket_id text,
                     table_name text,
                     record_id text,
                     operation smallint,
                     event_ordinal bigint,
                     effect_ordinal integer,
                     relation_id text,
                     row_version text,
                     projection_image text
                 )
             ), scope_input AS (
                 SELECT DISTINCT effect.bucket_id AS scope_id
                 FROM effect_input effect
             ), scope_inserted AS (
                 INSERT INTO synchro.sync_scope_state (scope_id, stream_generation)
                 SELECT scope_id, $4
                 FROM scope_input
                 ON CONFLICT (scope_id) DO NOTHING
                 RETURNING scope_id
             ), effect_inserted AS (
                 INSERT INTO synchro.sync_changelog (
                     bucket_id, table_name, record_id, operation,
                     stream_generation, commit_lsn, event_ordinal,
                     effect_ordinal, relation_id, row_version, projection_image
                 )
                 SELECT effect.bucket_id, effect.table_name, effect.record_id,
                        effect.operation, $4, $5::pg_lsn, effect.event_ordinal,
                        effect.effect_ordinal, effect.relation_id::uuid,
                        effect.row_version::uuid, effect.projection_image
                 FROM effect_input effect
                 ORDER BY effect.write_ordinal
                 RETURNING seq
             ), edge_delete_input AS (
                 SELECT table_name, record_id, bucket_id
                 FROM jsonb_to_recordset($2::jsonb) AS input(
                     table_name text, record_id text, bucket_id text
                 )
             ), edge_deleted AS (
                 DELETE FROM synchro.sync_bucket_edges edge
                 USING edge_delete_input input
                 WHERE edge.table_name = input.table_name
                   AND edge.record_id = input.record_id
                   AND edge.bucket_id = input.bucket_id
                 RETURNING edge.table_name, edge.record_id, edge.bucket_id
             ), edge_upsert_input AS (
                 SELECT relation_id, table_name, record_id, bucket_id,
                        checksum_hex, row_version
                 FROM jsonb_to_recordset($3::jsonb) AS input(
                     relation_id text,
                     table_name text,
                     record_id text,
                     bucket_id text,
                     checksum_hex text,
                     row_version text
                 )
             ), edge_upserted AS (
                 INSERT INTO synchro.sync_bucket_edges (
                     relation_id, table_name, record_id, bucket_id,
                     checksum, row_version, updated_at
                 )
                 SELECT input.relation_id::uuid, input.table_name, input.record_id,
                        input.bucket_id, decode(input.checksum_hex, 'hex'),
                        input.row_version::uuid, now()
                 FROM edge_upsert_input input
                 ON CONFLICT (table_name, record_id, bucket_id) DO UPDATE SET
                     relation_id = EXCLUDED.relation_id,
                     checksum = EXCLUDED.checksum,
                     row_version = EXCLUDED.row_version,
                     updated_at = now()
                 RETURNING table_name, record_id, bucket_id
             )
             SELECT (SELECT count(*) FROM effect_input)::bigint AS effect_expected,
                    (SELECT count(*) FROM effect_inserted)::bigint AS effect_inserted,
                    (SELECT count(*) FROM edge_delete_input)::bigint AS edge_delete_expected,
                    (SELECT count(*) FROM edge_deleted)::bigint AS edge_deleted,
                    (SELECT count(*) FROM edge_upsert_input)::bigint AS edge_upsert_expected,
                    (SELECT count(*) FROM edge_upserted)::bigint AS edge_upserted",
            None,
            &[
                pgrx::JsonB(serde_json::Value::Array(changelog_effects.to_vec())).into(),
                pgrx::JsonB(serde_json::Value::Array(edge_deletes.to_vec())).into(),
                pgrx::JsonB(serde_json::Value::Array(edge_upserts.to_vec())).into(),
                stream_generation.into(),
                format_lsn(commit_lsn).as_str().into(),
            ],
        )
        .map_err(|_| failure("materialization_failed", commit_lsn))?;
    if counts.len() != 1 {
        return Err(failure("materialization_failed", commit_lsn));
    }
    let counts = counts.first();
    let effect_expected = counts
        .get_by_name::<i64, &str>("effect_expected")
        .map_err(|_| failure("materialization_failed", commit_lsn))?
        .ok_or_else(|| failure("materialization_failed", commit_lsn))?;
    let effect_inserted = counts
        .get_by_name::<i64, &str>("effect_inserted")
        .map_err(|_| failure("materialization_failed", commit_lsn))?
        .ok_or_else(|| failure("materialization_failed", commit_lsn))?;
    let edge_delete_expected = counts
        .get_by_name::<i64, &str>("edge_delete_expected")
        .map_err(|_| failure("materialization_failed", commit_lsn))?
        .ok_or_else(|| failure("materialization_failed", commit_lsn))?;
    let edge_deleted = counts
        .get_by_name::<i64, &str>("edge_deleted")
        .map_err(|_| failure("materialization_failed", commit_lsn))?
        .ok_or_else(|| failure("materialization_failed", commit_lsn))?;
    let edge_upsert_expected = counts
        .get_by_name::<i64, &str>("edge_upsert_expected")
        .map_err(|_| failure("materialization_failed", commit_lsn))?
        .ok_or_else(|| failure("materialization_failed", commit_lsn))?;
    let edge_upserted = counts
        .get_by_name::<i64, &str>("edge_upserted")
        .map_err(|_| failure("materialization_failed", commit_lsn))?
        .ok_or_else(|| failure("materialization_failed", commit_lsn))?;
    if effect_inserted != effect_expected
        || edge_deleted != edge_delete_expected
        || edge_upserted != edge_upsert_expected
        || effect_inserted != expected_effect_count
        || edge_deleted != expected_edge_delete_count
        || edge_upserted != expected_edge_upsert_count
    {
        return Err(failure("materialization_failed", commit_lsn));
    }
    Ok(())
}

fn materialize_impacts(
    client: &mut SpiClient<'_>,
    target: ProjectionTarget<'_>,
    transaction: &WalTransaction,
    registry: &[TableRegistration],
    impacts: Vec<ImpactedRow>,
) -> Result<i64, PoisonFailure> {
    if matches!(target, ProjectionTarget::Candidate { .. }) {
        recompute_candidate_membership(client, target, registry)
            .map_err(|_| failure("scope_evaluation_failed", transaction.commit_lsn))?;
        return Ok(0);
    }
    let ProjectionTarget::Active { stream_generation } = target else {
        unreachable!();
    };

    let mut effect_count = 0i64;
    let mut next_effect_ordinals: HashMap<(u64, String), i32> = HashMap::new();
    for impact_chunk in impacts.chunks(JSONB_BATCH_SIZE) {
        let impact_keys = impact_chunk
            .iter()
            .map(|impact| {
                serde_json::json!({
                    "table_name": registry[impact.registration_index].table_name,
                    "record_id": impact.record_id,
                })
            })
            .collect::<Vec<_>>();
        let mut existing_buckets = HashMap::<(String, String), Vec<String>>::new();
        let existing_rows = client
            .select(
                "SELECT edge.table_name, edge.record_id, edge.bucket_id
                     FROM synchro.sync_bucket_edges edge
                     JOIN jsonb_to_recordset($1::jsonb) AS impact(table_name text, record_id text)
                       ON impact.table_name = edge.table_name
                      AND impact.record_id = edge.record_id
                     ORDER BY edge.table_name, edge.record_id, edge.bucket_id",
                None,
                &[pgrx::JsonB(serde_json::Value::Array(impact_keys)).into()],
            )
            .map_err(|_| failure("scope_evaluation_failed", transaction.commit_lsn))?;
        for row in existing_rows {
            let table_name = row
                .get_by_name::<String, &str>("table_name")
                .map_err(|_| failure("scope_evaluation_failed", transaction.commit_lsn))?
                .ok_or_else(|| failure("scope_evaluation_failed", transaction.commit_lsn))?;
            let record_id = row
                .get_by_name::<String, &str>("record_id")
                .map_err(|_| failure("scope_evaluation_failed", transaction.commit_lsn))?
                .ok_or_else(|| failure("scope_evaluation_failed", transaction.commit_lsn))?;
            if let Some(bucket_id) = row
                .get_by_name::<String, &str>("bucket_id")
                .map_err(|_| failure("scope_evaluation_failed", transaction.commit_lsn))?
            {
                existing_buckets
                    .entry((table_name, record_id))
                    .or_default()
                    .push(bucket_id);
            }
        }

        let mut changelog_effects = Vec::new();
        let mut edge_deletes = Vec::new();
        let mut edge_upserts = Vec::new();
        for impact in impact_chunk {
            let registration = &registry[impact.registration_index];
            let mut existing = existing_buckets
                .remove(&(registration.table_name.clone(), impact.record_id.clone()))
                .unwrap_or_default();
            let mut desired = if impact.operation == ChangeOperation::Delete {
                Vec::new()
            } else {
                resolve_membership(client, registration, &impact.record_id)
                    .map_err(|_| failure("scope_evaluation_failed", transaction.commit_lsn))?
            };
            desired.sort();
            desired.dedup();
            existing.sort();
            existing.dedup();

            let mut entries = build_edge_diff_entries(
                &registration.table_name,
                &impact.record_id,
                impact.operation,
                &existing,
                &desired,
            );
            if !impact.direct_change {
                entries.retain(|entry| entry.operation != ChangeOperation::Update);
            }
            entries.sort_by(|left, right| {
                left.bucket_id
                    .cmp(&right.bucket_id)
                    .then_with(|| left.operation.to_i16().cmp(&right.operation.to_i16()))
            });
            let mut local_effects = Vec::new();
            let mut local_edge_deletes = Vec::new();
            let mut local_edge_upserts = Vec::new();
            for entry in &entries {
                let next_effect_ordinal = next_effect_ordinals
                    .entry((impact.event_ordinal, entry.bucket_id.clone()))
                    .or_insert(0);
                let effect_ordinal = *next_effect_ordinal;
                *next_effect_ordinal = next_effect_ordinal
                    .checked_add(1)
                    .ok_or_else(|| failure("validation_failed", transaction.commit_lsn))?;
                let projection_image = if entry.operation == ChangeOperation::Delete {
                    impact.delete_projection_image
                } else {
                    Some("after")
                };
                let write_ordinal = effect_count;
                let event_ordinal = i64::try_from(impact.event_ordinal)
                    .map_err(|_| failure("validation_failed", transaction.commit_lsn))?;
                local_effects.push(serde_json::json!({
                    "write_ordinal": write_ordinal,
                    "bucket_id": entry.bucket_id,
                    "table_name": entry.table_name,
                    "record_id": entry.record_id,
                    "operation": entry.operation.to_i16(),
                    "event_ordinal": event_ordinal,
                    "effect_ordinal": effect_ordinal,
                    "relation_id": registration.relation_id,
                    "row_version": impact.row_version,
                    "projection_image": projection_image,
                }));
                effect_count = effect_count
                    .checked_add(1)
                    .ok_or_else(|| failure("validation_failed", transaction.commit_lsn))?;
            }
            let diff = diff_bucket_sets(&existing, &desired);
            if impact.operation == ChangeOperation::Delete {
                for bucket_id in diff.removed {
                    local_edge_deletes.push(serde_json::json!({
                        "table_name": registration.table_name,
                        "record_id": impact.record_id,
                        "bucket_id": bucket_id,
                    }));
                }
            } else {
                let digest = impact
                    .digest
                    .ok_or_else(|| failure("materialization_failed", transaction.commit_lsn))?;
                let checksum_hex = digest.to_lower_hex();
                for bucket_id in diff.added.iter().chain(diff.kept.iter()) {
                    local_edge_upserts.push(serde_json::json!({
                        "relation_id": registration.relation_id,
                        "table_name": registration.table_name,
                        "record_id": impact.record_id,
                        "bucket_id": bucket_id,
                        "checksum_hex": checksum_hex,
                        "row_version": impact.row_version,
                    }));
                }
                for bucket_id in diff.removed {
                    local_edge_deletes.push(serde_json::json!({
                        "table_name": registration.table_name,
                        "record_id": impact.record_id,
                        "bucket_id": bucket_id,
                    }));
                }
            }
            if local_effects.len() > JSONB_BATCH_SIZE
                || local_edge_deletes.len() > JSONB_BATCH_SIZE
                || local_edge_upserts.len() > JSONB_BATCH_SIZE
            {
                return Err(failure("scope_evaluation_failed", transaction.commit_lsn));
            }
            if local_effects.len() > JSONB_BATCH_SIZE - changelog_effects.len()
                || local_edge_deletes.len() > JSONB_BATCH_SIZE - edge_deletes.len()
                || local_edge_upserts.len() > JSONB_BATCH_SIZE - edge_upserts.len()
            {
                persist_impact_batch(
                    client,
                    stream_generation,
                    transaction.commit_lsn,
                    &changelog_effects,
                    &edge_deletes,
                    &edge_upserts,
                )?;
                changelog_effects.clear();
                edge_deletes.clear();
                edge_upserts.clear();
            }
            changelog_effects.extend(local_effects);
            edge_deletes.extend(local_edge_deletes);
            edge_upserts.extend(local_edge_upserts);
        }

        persist_impact_batch(
            client,
            stream_generation,
            transaction.commit_lsn,
            &changelog_effects,
            &edge_deletes,
            &edge_upserts,
        )?;
    }
    Ok(effect_count)
}

fn activate_generations(
    client: &mut SpiClient<'_>,
    active_generation: i64,
    activations: &[i64],
    commit_lsn: u64,
    end_lsn: u64,
) -> Result<i64, PoisonFailure> {
    let Some(final_generation) = activations.last().copied() else {
        return Ok(active_generation);
    };
    let stream_generation =
        active_stream_generation(client).map_err(|_| failure("validation_failed", commit_lsn))?;
    let mut source_generation = active_generation;
    for generation in activations {
        crate::materialize::activate_staged_membership_generation(
            client,
            source_generation,
            *generation,
            &stream_generation,
            &format_lsn(commit_lsn),
            &format_lsn(end_lsn),
        )
        .map_err(|_| failure("scope_evaluation_failed", commit_lsn))?;
        source_generation = *generation;
    }
    crate::registry::remove_retired_capture_configuration(
        client,
        active_generation,
        final_generation,
    )
    .map_err(|_| failure("validation_failed", commit_lsn))?;
    let superseded = client
        .update(
            "UPDATE synchro.sync_registry_generations
             SET state = 'superseded'
             WHERE generation = $1 AND state = 'active'",
            None,
            &[active_generation.into()],
        )
        .map_err(|_| failure("validation_failed", commit_lsn))?
        .len();
    if superseded != 1 {
        return Err(failure("validation_failed", commit_lsn));
    }
    for generation in activations {
        let state = if *generation == final_generation {
            "active"
        } else {
            "superseded"
        };
        let updated = client
            .update(
                "UPDATE synchro.sync_registry_generations
                 SET state = $2, activated_at = now(),
                     activation_commit_lsn = $3::pg_lsn,
                     activation_end_lsn = $4::pg_lsn
                 WHERE generation = $1 AND state = 'pending' AND validated",
                None,
                &[
                    (*generation).into(),
                    state.into(),
                    format_lsn(commit_lsn).as_str().into(),
                    format_lsn(end_lsn).as_str().into(),
                ],
            )
            .map_err(|_| failure("validation_failed", commit_lsn))?
            .len();
        if updated != 1 {
            return Err(failure("validation_failed", commit_lsn));
        }
    }
    crate::schema::publish_schema_manifest(client)
        .map_err(|_| failure("materialization_failed", commit_lsn))?;
    Ok(final_generation)
}

fn advance_slot(
    slot: &str,
    commit_lsn: u64,
    end_lsn: u64,
    worker_role_oid: pg_sys::Oid,
) -> Result<(), PoisonFailure> {
    let requested = format_lsn(end_lsn);
    run_replication_transaction(worker_role_oid, || {
        Spi::connect_mut(|client| {
            let actual = client
                .select(
                    "SELECT end_lsn::text AS end_lsn
                     FROM pg_catalog.pg_replication_slot_advance($1, $2::pg_lsn)",
                    None,
                    &[slot.into(), requested.as_str().into()],
                )
                .map_err(|_| failure("transaction_commit_failed", commit_lsn))?
                .first()
                .get_by_name::<String, &str>("end_lsn")
                .map_err(|_| failure("transaction_commit_failed", commit_lsn))?
                .and_then(|value| parse_lsn(&value))
                .ok_or_else(|| failure("transaction_commit_failed", commit_lsn))?;
            if actual != end_lsn {
                return Err(failure("transaction_commit_failed", commit_lsn));
            }
            activate_worker_role(worker_role_oid);
            let updated = client
                .update(
                    "UPDATE synchro.sync_wal_progress
                     SET acknowledged_end_lsn = $1::pg_lsn, updated_at = now()
                     WHERE singleton = true
                       AND materialized_end_lsn >= $1::pg_lsn",
                    None,
                    &[requested.as_str().into()],
                )
                .map_err(|_| failure("transaction_commit_failed", commit_lsn))?
                .len();
            if updated != 1 {
                return Err(failure("transaction_commit_failed", commit_lsn));
            }
            Ok(())
        })
    })
}

fn persist_poison(failure: PoisonFailure) -> Result<(), String> {
    run_worker_transaction(|| {
        Spi::connect_mut(|client| {
            let stream = active_stream_generation(client)?;
            retire_prior_generation_poison(client, &stream)?;
            client
                .update(
                    "INSERT INTO synchro.sync_wal_poison (
                          stream_generation, commit_lsn, failure_class,
                          failure_detail, relation_id, lifecycle, poisoned_at, attempt_count
                      )
                       SELECT $1, $2::pg_lsn, $3, $4, $5::uuid, 'active', now(), 1
                      WHERE NOT EXISTS (
                          SELECT 1
                          FROM synchro.sync_wal_poison
                          WHERE lifecycle = 'active' AND stream_generation = $1
                      )",
                    None,
                    &[
                        stream.as_str().into(),
                        format_lsn(failure.commit_lsn).as_str().into(),
                        failure.class.into(),
                        failure.detail.as_str().into(),
                        failure.relation_id.as_deref().into(),
                    ],
                )
                .map_err(|_| "persisting WAL poison failed".to_string())?;
            if let Some(commit_timestamp) = failure.commit_timestamp {
                client
                    .update(
                        "UPDATE synchro.sync_wal_worker_state
                         SET oldest_unmaterialized_commit_timestamp =
                                 '2000-01-01 00:00:00+00'::timestamptz
                                 + ($1::bigint * interval '1 microsecond'),
                             wal_observed_at = now(),
                             updated_at = now()
                         WHERE worker_id = $2",
                        None,
                        &[commit_timestamp.into(), WORKER_ID.into()],
                    )
                    .map_err(|_| "persisting WAL lag observation failed".to_string())?;
            }
            Ok(())
        })
    })
}

fn record_oldest_unmaterialized_commit(commit_timestamp: Option<i64>) -> Result<(), String> {
    run_worker_transaction(|| {
        Spi::connect_mut(|client| {
            client
                .update(
                    "UPDATE synchro.sync_wal_worker_state
                     SET oldest_unmaterialized_commit_timestamp = CASE
                             WHEN $1::bigint IS NULL THEN NULL
                             ELSE '2000-01-01 00:00:00+00'::timestamptz
                                  + ($1::bigint * interval '1 microsecond')
                         END,
                         wal_observed_at = now(),
                         updated_at = now()
                     WHERE worker_id = $2",
                    None,
                    &[commit_timestamp.into(), WORKER_ID.into()],
                )
                .map_err(|_| "recording WAL lag observation failed".to_string())?;
            Ok(())
        })
    })
}

fn infer_transaction_relation_id(transaction: &WalTransaction) -> Option<String> {
    run_worker_transaction(|| {
        Spi::connect(|client| {
            let generation = active_registry_generation(client)?;
            let registry = load_registry_generation_for_worker(client, generation)
                .map_err(|error| format!("loading active registry failed: {error}"))?;
            let mut relation_ids = transaction
                .events
                .iter()
                .map(|event| &event.relation)
                .chain(
                    transaction
                        .truncates
                        .iter()
                        .map(|truncate| &truncate.relation),
                )
                .filter_map(|relation| {
                    find_registration(&registry, relation)
                        .map(|registration| registration.relation_id.clone())
                });
            let first = relation_ids.next();
            if first
                .as_ref()
                .is_some_and(|relation_id| relation_ids.any(|next| next != *relation_id))
            {
                return Ok::<Option<String>, String>(None);
            }
            Ok::<Option<String>, String>(first)
        })
    })
    .ok()
    .flatten()
}

fn repair_same_position_poison(
    client: &mut SpiClient<'_>,
    stream_generation: &str,
    commit_lsn: u64,
) -> Result<(), PoisonFailure> {
    client
        .update(
            "UPDATE synchro.sync_wal_poison
             SET lifecycle = 'repaired', resolved_at = now(),
                 attempt_count = attempt_count + 1
             WHERE lifecycle = 'active'
               AND retry_requested_at IS NOT NULL
               AND stream_generation = $1
               AND commit_lsn = $2::pg_lsn
               AND failure_class <> 'truncate_unsupported'",
            None,
            &[
                stream_generation.into(),
                format_lsn(commit_lsn).as_str().into(),
            ],
        )
        .map_err(|_| failure("materialization_failed", commit_lsn))?;
    Ok(())
}

pub(crate) fn retire_prior_generation_poison(
    client: &mut SpiClient<'_>,
    stream_generation: &str,
) -> Result<usize, String> {
    client
        .update(
            "UPDATE synchro.sync_wal_poison
             SET lifecycle = 'reset', resolved_at = now()
             WHERE lifecycle = 'active' AND stream_generation <> $1",
            None,
            &[stream_generation.into()],
        )
        .map(|updated| updated.len())
        .map_err(|_| "retiring prior stream WAL poison failed".to_string())
}

fn retire_prior_generation_poison_for_worker(stream_generation: &str) -> Result<usize, String> {
    run_worker_transaction(|| {
        Spi::connect_mut(|client| retire_prior_generation_poison(client, stream_generation))
    })
}

pub(crate) fn active_poison_state(stream_generation: &str) -> Result<(bool, bool), String> {
    run_worker_transaction(|| {
        Spi::connect_mut(|client| {
            let row = client
                .select(
                    "SELECT EXISTS (
                          SELECT 1
                          FROM synchro.sync_wal_poison
                          WHERE lifecycle = 'active' AND stream_generation = $1
                      ) AS active,
                      EXISTS (
                          SELECT 1 FROM synchro.sync_wal_poison
                          WHERE lifecycle = 'active'
                            AND stream_generation = $1
                            AND retry_requested_at IS NOT NULL
                            AND failure_class <> 'truncate_unsupported'
                      ) AS repairable",
                    None,
                    &[stream_generation.into()],
                )
                .map_err(|_| "loading WAL poison failed".to_string())?
                .first();
            let active = row
                .get_by_name::<bool, &str>("active")
                .map_err(|_| "loading WAL poison failed".to_string())?
                .unwrap_or(true);
            let repairable = row
                .get_by_name::<bool, &str>("repairable")
                .map_err(|_| "loading WAL poison failed".to_string())?
                .unwrap_or(false);
            Ok((active, repairable))
        })
    })
}

fn retry_requested(stream_generation: &str) -> Result<bool, String> {
    active_poison_state(stream_generation).map(|state| state.1)
}

fn heartbeat(state: &str) -> Result<(), String> {
    run_worker_transaction(|| {
        Spi::connect_mut(|client| {
            client
                .update(
                    "UPDATE synchro.sync_wal_worker_state w
                     SET state = $1,
                         registry_generation = p.registry_generation,
                         materialized_commit_lsn = p.materialized_commit_lsn,
                         materialized_end_lsn = p.materialized_end_lsn,
                         heartbeat_at = now(), updated_at = now()
                     FROM synchro.sync_wal_progress p
                     WHERE w.worker_id = $2 AND p.singleton = true",
                    None,
                    &[state.into(), WORKER_ID.into()],
                )
                .map_err(|_| "updating worker heartbeat failed".to_string())?;
            Ok(())
        })
    })
}

fn active_stream_generation(client: &SpiClient<'_>) -> Result<String, String> {
    client
        .select(
            "SELECT stream_generation::text AS stream_generation
             FROM synchro.sync_runtime_state WHERE singleton = true",
            None,
            &[],
        )
        .map_err(|_| "loading stream generation failed".to_string())?
        .first()
        .get_by_name::<String, &str>("stream_generation")
        .map_err(|_| "loading stream generation failed".to_string())?
        .ok_or_else(|| "stream generation is unavailable".to_string())
}

fn active_registry_generation(client: &SpiClient<'_>) -> Result<i64, String> {
    client
        .select(
            "SELECT generation
             FROM synchro.sync_registry_generations
             WHERE state = 'active' AND validated",
            None,
            &[],
        )
        .map_err(|_| "loading registry generation failed".to_string())?
        .first()
        .get_by_name::<i64, &str>("generation")
        .map_err(|_| "loading registry generation failed".to_string())?
        .filter(|generation| *generation > 0)
        .ok_or_else(|| "active registry generation is unavailable".to_string())
}

fn find_registration<'a>(
    registry: &'a [TableRegistration],
    relation: &RelationKey,
) -> Option<&'a TableRegistration> {
    registry.iter().find(|registration| {
        registration.physical_schema == relation.namespace
            && registration.physical_relation == relation.name
            && registration.physical_relation_oid == relation.oid
    })
}

fn registered_id(image: &TupleImage, column: &str) -> Result<String, String> {
    match image.get(column) {
        Some(TupleValue::Text(bytes)) => std::str::from_utf8(bytes)
            .ok()
            .filter(|value| !value.is_empty())
            .map(String::from)
            .ok_or_else(|| "registered identity is invalid".to_string()),
        Some(TupleValue::Binary(_)) => Err("binary registered identity is unsupported".to_string()),
        _ => Err("registered identity is missing".to_string()),
    }
}

fn operation_name(operation: ChangeOperation) -> &'static str {
    match operation {
        ChangeOperation::Insert => "insert",
        ChangeOperation::Update => "update",
        ChangeOperation::Delete => "delete",
    }
}

const fn operation_rank(operation: ChangeOperation) -> u8 {
    match operation {
        ChangeOperation::Delete => 0,
        ChangeOperation::Insert | ChangeOperation::Update => 1,
    }
}

fn parse_lsn(value: &str) -> Option<u64> {
    let (high, low) = value.split_once('/')?;
    let high = u64::from_str_radix(high, 16).ok()?;
    let low = u64::from_str_radix(low, 16).ok()?;
    high.checked_shl(32)?.checked_add(low)
}

fn format_lsn(value: u64) -> String {
    format!("{:X}/{:08X}", value >> 32, value & 0xffff_ffff)
}

fn failure(class: &'static str, commit_lsn: u64) -> PoisonFailure {
    let detail = match class {
        "decode_failed" => "WAL decoding failed",
        "validation_failed" => "WAL validation failed",
        "fence_correlation_failed" => "WAL fence correlation failed",
        "materialization_failed" => "WAL materialization failed",
        "projection_write_failed" => "WAL projection write failed",
        "scope_evaluation_failed" => "WAL scope evaluation failed",
        "transaction_commit_failed" => "WAL transaction commit failed",
        "truncate_unsupported" => "WAL transaction truncated a registered relation",
        "registered_relation_drift" => "registered relation metadata drifted",
        "activation_barrier" => "WAL processing reached an activation barrier",
        _ => "WAL processing failed",
    };
    failure_with_detail(class, commit_lsn, detail)
}

fn failure_with_detail(class: &'static str, commit_lsn: u64, detail: &str) -> PoisonFailure {
    PoisonFailure {
        class,
        detail: bounded_poison_detail(detail),
        commit_lsn,
        relation_id: None,
        commit_timestamp: None,
    }
}

fn bounded_poison_detail(detail: &str) -> String {
    let mut end = detail.len().min(MAX_POISON_DETAIL_BYTES);
    while !detail.is_char_boundary(end) {
        end -= 1;
    }
    detail[..end].to_string()
}

fn run_worker_transaction<R, E, F: FnOnce() -> Result<R, E> + UnwindSafe + RefUnwindSafe>(
    body: F,
) -> Result<R, E> {
    unsafe {
        pg_sys::SetCurrentStatementStartTimestamp();
        pg_sys::StartTransactionCommand();
        pg_sys::PushActiveSnapshot(pg_sys::GetTransactionSnapshot());
    }
    let result = PgTryBuilder::new(body).execute();
    unsafe {
        pg_sys::PopActiveSnapshot();
        if result.is_ok() {
            pg_sys::CommitTransactionCommand();
        } else {
            pg_sys::AbortCurrentTransaction();
        }
    }
    result
}

fn run_replication_transaction<R, E, F: FnOnce() -> Result<R, E> + UnwindSafe + RefUnwindSafe>(
    worker_role_oid: pg_sys::Oid,
    body: F,
) -> Result<R, E> {
    activate_session_login();
    let result = run_worker_transaction(body);
    activate_worker_role(worker_role_oid);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn captured_row_deletion_uses_logical_field_identity() {
        let field = crate::registry::FieldRegistration {
            field_id: "field-deleted-at".to_string(),
            physical_column: "deleted_at".to_string(),
            portable_type: "datetime".to_string(),
            native_json: false,
            decimal_precision: None,
            decimal_scale: None,
            nullable: true,
            writable: false,
            primary_key: false,
        };

        assert_eq!(
            captured_row_deleted(
                &[field],
                "deleted_at",
                &serde_json::json!({ "field-deleted-at": "2026-08-17T02:31:43.476060Z" }),
            ),
            Ok(true)
        );
    }

    #[test]
    fn poison_detail_stays_within_the_storage_bound() {
        let detail = format!("{}x", "a".repeat(MAX_POISON_DETAIL_BYTES));
        assert_eq!(
            bounded_poison_detail(&detail).len(),
            MAX_POISON_DETAIL_BYTES
        );
    }
}
