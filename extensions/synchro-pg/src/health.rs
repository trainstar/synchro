use std::collections::BTreeMap;

use pgrx::prelude::*;
use pgrx::spi::{SpiClient, SpiTupleTable};
use serde::Serialize;
use sha2::{Digest, Sha256};
use synchro_core::contract::SchemaManifest;

const SCHEMA_MANIFEST_DOMAIN: &[u8] = b"synchro:v3:schema-manifest:v1\0";

const WORKER_LOGIN_VALIDATION_SQL: &str = "WITH configured_login AS (
         SELECT oid,
                rolcanlogin
                AND rolreplication
                AND NOT rolinherit
                AND NOT rolsuper
                AND NOT rolcreatedb
                AND NOT rolcreaterole
                AND NOT rolbypassrls AS attributes_valid
         FROM pg_catalog.pg_roles
         WHERE rolname = $1
     ),
     worker_group AS (
         SELECT oid,
                NOT rolcanlogin
                AND NOT rolreplication
                AND NOT rolsuper
                AND NOT rolcreatedb
                AND NOT rolcreaterole
                AND NOT rolbypassrls AS attributes_valid
         FROM pg_catalog.pg_roles
         WHERE rolname = 'synchro_worker'
     )
     SELECT
         (SELECT oid::bigint FROM configured_login) AS worker_login_oid,
         COALESCE((SELECT attributes_valid FROM configured_login), false)
             AS login_attributes_valid,
         COALESCE((SELECT attributes_valid FROM worker_group), false)
             AS worker_group_attributes_valid,
         COALESCE((
             SELECT count(membership.roleid) = 1
                    AND bool_and(membership.roleid = worker_group.oid)
                    AND pg_catalog.pg_has_role(
                        configured_login.oid,
                        worker_group.oid,
                        'SET'
                    )
                    AND NOT EXISTS (
                        SELECT 1
                        FROM pg_catalog.pg_roles other_group
                        WHERE other_group.oid <> configured_login.oid
                          AND other_group.oid <> worker_group.oid
                          AND pg_catalog.pg_has_role(
                              configured_login.oid,
                              other_group.oid,
                              'MEMBER'
                          )
                    )
             FROM configured_login
             CROSS JOIN worker_group
             LEFT JOIN pg_catalog.pg_auth_members membership
               ON membership.member = configured_login.oid
             GROUP BY configured_login.oid, worker_group.oid
         ), false) AS membership_valid,
         COALESCE((
             SELECT NOT EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_roles other_login
                 WHERE other_login.rolcanlogin
                   AND other_login.rolreplication
                   AND NOT other_login.rolsuper
                   AND other_login.oid <> configured_login.oid
             )
             FROM configured_login
         ), false) AS sole_replication_principal";

const READINESS_SQL: &str = r#"
WITH current_database_state AS (
    SELECT database.oid AS database_oid,
           database.datname::text AS database_name
    FROM pg_catalog.pg_database database
    WHERE database.datname = pg_catalog.current_database()
),
runtime AS (
    SELECT state.*
    FROM synchro.sync_runtime_state state
    WHERE state.singleton
),
active_registry AS (
    SELECT generation.*
    FROM synchro.sync_registry_generations generation
    JOIN runtime ON runtime.stream_generation = generation.stream_generation
    WHERE generation.state = 'active'
      AND generation.validated
),
progress AS (
    SELECT state.*
    FROM synchro.sync_wal_progress state
    WHERE state.singleton
),
worker AS (
    SELECT state.*
    FROM synchro.sync_wal_worker_state state
    WHERE state.worker_id = 'synchro_wal_consumer'
),
active_poison AS (
    SELECT poison.failure_class,
           poison.failure_detail,
           poison.commit_lsn::text AS commit_lsn,
           poison.relation_id::text AS relation_id
    FROM synchro.sync_wal_poison poison
    JOIN runtime ON poison.stream_generation = runtime.stream_generation
    WHERE poison.lifecycle = 'active'
),
runtime_slot AS (
    SELECT slot.*
    FROM pg_catalog.pg_replication_slots slot
    JOIN runtime ON runtime.active_slot_name::text = slot.slot_name::text
),
configured_publication AS (
    SELECT publication.*
    FROM pg_catalog.pg_publication publication
    WHERE publication.pubname = $2::text
),
latest_schema AS (
    SELECT manifest.*
    FROM synchro.sync_schema_manifest manifest
    ORDER BY manifest.schema_version DESC
    LIMIT 1
)
SELECT
    EXISTS (
        SELECT 1
        FROM current_database_state database
        WHERE database.database_name = $1::text
    ) AS database_matches,
    EXISTS (
        SELECT 1
        FROM pg_catalog.pg_extension extension
        JOIN pg_catalog.pg_namespace namespace ON namespace.oid = extension.extnamespace
        WHERE extension.extname = 'synchro_pg'
          AND extension.extversion = $4::text
          AND namespace.nspname = 'synchro'
    ) AS extension_matches,
    (
        (SELECT count(*) = 1 FROM runtime)
        AND (SELECT count(*) = 1 FROM active_registry)
        AND EXISTS (
            SELECT 1
            FROM runtime
            JOIN active_registry
              ON active_registry.stream_generation = runtime.stream_generation
            JOIN progress
              ON progress.stream_generation = runtime.stream_generation
             AND progress.registry_generation = active_registry.generation
        )
    ) AS registry_generation_valid,
    EXISTS (
        SELECT 1
        FROM latest_schema schema
        JOIN synchro.sync_registry_generations schema_registry
          ON schema_registry.generation = schema.registry_generation
         AND schema_registry.validated
         AND schema_registry.state IN ('active', 'superseded')
        JOIN active_registry
          ON active_registry.stream_generation = schema_registry.stream_generation
         AND schema.registry_generation <= active_registry.generation
    ) AS schema_generation_consistent,
    NOT EXISTS (
        SELECT 1
        FROM active_registry
        JOIN synchro.sync_registry registry
          ON registry.registry_generation = active_registry.generation
        LEFT JOIN pg_catalog.pg_class relation
          ON relation.oid = registry.physical_relation_oid
        LEFT JOIN pg_catalog.pg_namespace namespace
          ON namespace.oid = relation.relnamespace
        WHERE relation.oid IS NULL
           OR namespace.nspname::text <> registry.physical_schema::text
           OR relation.relname::text <> registry.physical_relation::text
           OR relation.relkind NOT IN ('r', 'p')
           OR relation.relreplident <> registry.replica_identity
           OR registry.replica_identity <> 'd'
           OR NOT EXISTS (
               SELECT 1
               FROM pg_catalog.pg_index primary_index
               JOIN pg_catalog.pg_attribute primary_attribute
                 ON primary_attribute.attrelid = primary_index.indrelid
                AND primary_attribute.attnum = ANY(primary_index.indkey)
               WHERE primary_index.indrelid = registry.physical_relation_oid
                 AND primary_index.indisprimary
                 AND primary_index.indnkeyatts = 1
                 AND primary_index.indexprs IS NULL
                 AND primary_index.indpred IS NULL
                 AND primary_attribute.attname::text = registry.pk_column
                 AND primary_attribute.attnotnull
           )
           OR (registry.registration_kind = 'synced' AND NOT EXISTS (
               SELECT 1
               FROM pg_catalog.pg_proc membership_function
               JOIN pg_catalog.pg_namespace function_namespace
                 ON function_namespace.oid = membership_function.pronamespace
               WHERE membership_function.oid = registry.membership_function_oid
                 AND function_namespace.nspname::text = registry.membership_function_schema::text
                 AND membership_function.proname::text = registry.membership_function_name::text
                 AND membership_function.prokind = 'f'
           ))
    ) AS relation_identity_valid,
    (
        EXISTS (
            SELECT 1
            FROM configured_publication publication
            WHERE NOT publication.puballtables
              AND publication.pubinsert
              AND publication.pubupdate
              AND publication.pubdelete
              AND publication.pubtruncate
        )
        AND NOT EXISTS (
            SELECT registry.physical_relation_oid
            FROM active_registry
            JOIN synchro.sync_registry registry
              ON registry.registry_generation = active_registry.generation
            EXCEPT
            SELECT member.prrelid
            FROM configured_publication publication
            JOIN pg_catalog.pg_publication_rel member
              ON member.prpubid = publication.oid
        )
        AND NOT EXISTS (
            SELECT member.prrelid
            FROM configured_publication publication
            JOIN pg_catalog.pg_publication_rel member
              ON member.prpubid = publication.oid
            EXCEPT
            SELECT registry.physical_relation_oid
            FROM active_registry
            JOIN synchro.sync_registry registry
              ON registry.registry_generation = active_registry.generation
        )
    ) AS publication_valid,
    NOT EXISTS (
        SELECT 1
        FROM active_registry
        JOIN synchro.sync_registry registry
          ON registry.registry_generation = active_registry.generation
        WHERE (
            SELECT count(*)
            FROM pg_catalog.pg_trigger trigger
            JOIN pg_catalog.pg_proc function ON function.oid = trigger.tgfoid
            JOIN pg_catalog.pg_namespace namespace ON namespace.oid = function.pronamespace
            WHERE trigger.tgrelid = registry.physical_relation_oid
              AND NOT trigger.tgisinternal
              AND trigger.tgname IN (
                  'synchro_primary_key_guard', 'synchro_capture_fence',
                  'synchro_capture_truncate_guard'
              )
              AND namespace.nspname = 'synchro'
        ) <> 3
        OR (
            SELECT count(*)
            FROM pg_catalog.pg_trigger trigger
            JOIN pg_catalog.pg_proc function ON function.oid = trigger.tgfoid
            JOIN pg_catalog.pg_namespace namespace ON namespace.oid = function.pronamespace
            WHERE trigger.tgrelid = registry.physical_relation_oid
              AND NOT trigger.tgisinternal
              AND trigger.tgname = 'synchro_primary_key_guard'
              AND trigger.tgenabled = 'O'
              AND trigger.tgtype::integer = 19
              AND trigger.tgnargs = 1
              AND trigger.tgargs = pg_catalog.convert_to(registry.pk_column, 'UTF8')
                                   || pg_catalog.decode('00', 'hex')
              AND namespace.nspname = 'synchro'
              AND function.proname = 'synchro_primary_key_guard'
        ) <> 1
        OR (
            SELECT count(*)
            FROM pg_catalog.pg_trigger trigger
            JOIN pg_catalog.pg_proc function ON function.oid = trigger.tgfoid
            JOIN pg_catalog.pg_namespace namespace ON namespace.oid = function.pronamespace
            WHERE trigger.tgrelid = registry.physical_relation_oid
              AND NOT trigger.tgisinternal
              AND trigger.tgname = 'synchro_capture_fence'
              AND trigger.tgenabled = 'O'
              AND trigger.tgtype::integer = 29
              AND trigger.tgnargs = 5
              AND trigger.tgargs = pg_catalog.convert_to(registry.relation_id::text, 'UTF8')
                                   || pg_catalog.decode('00', 'hex')
                                   || pg_catalog.convert_to(registry.registration_kind, 'UTF8')
                                   || pg_catalog.decode('00', 'hex')
                                   || pg_catalog.convert_to(
                                          CASE
                                              WHEN registry.registration_kind = 'synced'
                                              THEN registry.table_id::text
                                              ELSE ''
                                          END,
                                          'UTF8'
                                      )
                                   || pg_catalog.decode('00', 'hex')
                                   || pg_catalog.convert_to(
                                          pg_catalog.to_json(registry.capture_key_columns)::text,
                                          'UTF8'
                                      )
                                   || pg_catalog.decode('00', 'hex')
                                   || pg_catalog.convert_to(
                                          CASE
                                              WHEN registry.registration_kind = 'synced'
                                                   AND registry.has_deleted_at
                                              THEN registry.deleted_at_col
                                              ELSE ''
                                          END,
                                          'UTF8'
                                      )
                                   || pg_catalog.decode('00', 'hex')
              AND namespace.nspname = 'synchro'
              AND function.proname = 'synchro_capture_fence'
        ) <> 1
        OR (
            SELECT count(*)
            FROM pg_catalog.pg_trigger trigger
            JOIN pg_catalog.pg_proc function ON function.oid = trigger.tgfoid
            JOIN pg_catalog.pg_namespace namespace ON namespace.oid = function.pronamespace
            WHERE trigger.tgrelid = registry.physical_relation_oid
              AND NOT trigger.tgisinternal
              AND trigger.tgname = 'synchro_capture_truncate_guard'
              AND trigger.tgenabled = 'O'
              AND trigger.tgtype::integer = 34
              AND trigger.tgnargs = 1
              AND trigger.tgargs = pg_catalog.convert_to(registry.relation_id::text, 'UTF8')
                                   || pg_catalog.decode('00', 'hex')
              AND namespace.nspname = 'synchro'
              AND function.proname = 'synchro_capture_truncate_guard'
        ) <> 1
    ) AS capture_triggers_valid,
    EXISTS (
        SELECT 1
        FROM runtime_slot slot
        JOIN current_database_state database ON database.database_name = slot.database::text
        JOIN runtime ON runtime.active_slot_name::text = slot.slot_name::text
        LEFT JOIN worker ON true
        WHERE slot.slot_type = 'logical'
          AND slot.plugin = 'pgoutput'
          AND NOT slot.temporary
          AND slot.invalidation_reason IS NULL
          AND slot.wal_status IS DISTINCT FROM 'lost'
          AND slot.restart_lsn IS NOT NULL
          AND slot.confirmed_flush_lsn IS NOT NULL
          AND slot.confirmed_flush_lsn <= pg_catalog.pg_current_wal_lsn()
          AND (NOT slot.active OR slot.active_pid = worker.backend_pid)
    ) AS replication_slot_valid,
    EXISTS (
        SELECT 1
        FROM worker
        JOIN current_database_state database
          ON database.database_oid = worker.database_oid
         AND database.database_name = worker.database_name::text
        JOIN active_registry ON active_registry.generation = worker.registry_generation
        JOIN progress
          ON progress.registry_generation = worker.registry_generation
         AND progress.materialized_commit_lsn IS NOT DISTINCT FROM worker.materialized_commit_lsn
         AND progress.materialized_end_lsn IS NOT DISTINCT FROM worker.materialized_end_lsn
        JOIN pg_catalog.pg_stat_activity activity
          ON activity.pid = worker.backend_pid
         AND activity.datid = database.database_oid
         WHERE worker.worker_login_oid = $3::oid
          AND worker.state = 'running'
    ) AS worker_state_valid,
    (
        NOT EXISTS (SELECT 1 FROM active_poison)
    ) AS poison_clear,
    (SELECT failure_class FROM active_poison) AS poison_failure_class,
    (SELECT failure_detail FROM active_poison) AS poison_failure_detail,
    (SELECT commit_lsn FROM active_poison) AS poison_commit_lsn,
    (SELECT relation_id FROM active_poison) AS poison_relation_id,
    (SELECT active_slot_name::text FROM runtime) AS active_slot_name,
    NOT EXISTS (
        SELECT 1
        FROM synchro.sync_stream_resets reset
        WHERE reset.lifecycle IN ('preparing', 'baseline_staged')
    ) AS stream_reset_clear,
    EXISTS (
        SELECT 1
        FROM runtime
        JOIN active_registry
          ON active_registry.stream_generation = runtime.stream_generation
        JOIN progress
          ON progress.stream_generation = runtime.stream_generation
         AND progress.registry_generation = active_registry.generation
        WHERE (
            progress.generation_start_lsn IS NOT NULL
            AND progress.materialized_commit_lsn IS NULL
            AND progress.materialized_end_lsn IS NULL
            AND progress.acknowledged_end_lsn IS NULL
            AND NOT EXISTS (
                SELECT 1
                FROM synchro.sync_wal_transactions transaction
                WHERE transaction.stream_generation = runtime.stream_generation
            )
        ) OR (
             progress.generation_start_lsn IS NOT NULL
             AND progress.materialized_commit_lsn IS NOT NULL
            AND progress.materialized_end_lsn IS NOT NULL
            AND progress.acknowledged_end_lsn = progress.materialized_end_lsn
            AND EXISTS (
                SELECT 1
                FROM synchro.sync_wal_transactions transaction
                WHERE transaction.stream_generation = runtime.stream_generation
                  AND transaction.commit_lsn = progress.materialized_commit_lsn
                  AND transaction.end_lsn = progress.materialized_end_lsn
                  AND transaction.registry_generation <= progress.registry_generation
            )
            AND NOT EXISTS (
                SELECT 1
                FROM synchro.sync_wal_transactions transaction
                WHERE transaction.stream_generation = runtime.stream_generation
                  AND (
                      transaction.commit_lsn > progress.materialized_commit_lsn
                      OR transaction.end_lsn > progress.materialized_end_lsn
                  )
            )
        )
    ) AS progress_valid,
    CASE
        WHEN NOT EXISTS (SELECT 1 FROM progress) THEN NULL
        WHEN NOT EXISTS (SELECT 1 FROM runtime_slot) THEN NULL
        ELSE (
            SELECT slot.confirmed_flush_lsn = COALESCE(
                       progress.acknowledged_end_lsn,
                       progress.generation_start_lsn
                   )
                   AND slot.confirmed_flush_lsn <= pg_catalog.pg_current_wal_lsn()
            FROM runtime_slot slot
            CROSS JOIN progress
        )
    END AS slot_acknowledgement_valid,
    (
        SELECT extract(
                   epoch FROM pg_catalog.clock_timestamp() - worker.heartbeat_at
               )::double precision
        FROM worker
        JOIN current_database_state database
          ON database.database_oid = worker.database_oid
         AND database.database_name = worker.database_name::text
         WHERE worker.worker_login_oid = $3::oid
    ) AS heartbeat_age_seconds,
    (
            SELECT least(
                   greatest(
                       pg_catalog.pg_wal_lsn_diff(
                           pg_catalog.pg_current_wal_lsn(),
                           slot.confirmed_flush_lsn
                       ),
                        0::numeric
                    ),
                    9223372036854775807::numeric
                )::bigint
        FROM runtime_slot slot
        WHERE slot.confirmed_flush_lsn IS NOT NULL
          AND slot.confirmed_flush_lsn <= pg_catalog.pg_current_wal_lsn()
    ) AS wal_lag_bytes,
    (
        SELECT CASE
            WHEN worker.oldest_unmaterialized_commit_timestamp IS NOT NULL THEN
                extract(
                    epoch FROM pg_catalog.clock_timestamp()
                               - worker.oldest_unmaterialized_commit_timestamp
                )::double precision
             WHEN worker.wal_observed_at IS NOT NULL
                  AND NOT EXISTS (SELECT 1 FROM active_poison) THEN 0::double precision
            ELSE NULL
        END
        FROM worker
    ) AS wal_lag_seconds,
    schema.schema_version AS schema_version,
    schema.schema_hash AS schema_hash,
    schema.canonical_manifest_body AS schema_body,
    schema.parent_schema_version AS schema_parent_version,
    schema.parent_schema_hash AS schema_parent_hash,
    schema.transition_class AS schema_transition_class,
    schema.compatibility_floor AS schema_compatibility_floor
FROM (SELECT 1) singleton
LEFT JOIN latest_schema schema ON true
"#;

#[derive(Clone, Copy, Default)]
pub(crate) struct WorkerLoginValidation {
    pub(crate) worker_login_oid: Option<i64>,
    pub(crate) login_attributes_valid: bool,
    pub(crate) worker_group_attributes_valid: bool,
    pub(crate) membership_valid: bool,
    pub(crate) sole_replication_principal: bool,
}

impl WorkerLoginValidation {
    pub(crate) fn is_valid(self) -> bool {
        self.worker_login_oid.is_some()
            && self.login_attributes_valid
            && self.worker_group_attributes_valid
            && self.membership_valid
            && self.sole_replication_principal
    }
}

pub(crate) fn validate_worker_login(
    client: &SpiClient<'_>,
    worker_login: &str,
) -> Result<WorkerLoginValidation, String> {
    let row = client
        .select(WORKER_LOGIN_VALIDATION_SQL, None, &[worker_login.into()])
        .map_err(|_| "validating worker login failed".to_string())?
        .first();
    Ok(WorkerLoginValidation {
        worker_login_oid: optional_value(&row, "worker_login_oid")?,
        login_attributes_valid: required_bool(&row, "login_attributes_valid")?,
        worker_group_attributes_valid: required_bool(&row, "worker_group_attributes_valid")?,
        membership_valid: required_bool(&row, "membership_valid")?,
        sole_replication_principal: required_bool(&row, "sole_replication_principal")?,
    })
}

#[derive(Clone)]
pub(crate) struct ReadinessConfiguration {
    pub(crate) database: Option<String>,
    pub(crate) publication: Option<String>,
    pub(crate) worker_login: Option<String>,
    pub(crate) max_heartbeat_age_seconds: i32,
    pub(crate) max_wal_lag_bytes: i32,
    pub(crate) max_wal_lag_seconds: i32,
}

impl ReadinessConfiguration {
    pub(crate) fn configured() -> Self {
        Self {
            database: configured_string(&crate::DATABASE_GUC),
            publication: configured_string(&crate::PUBLICATION_NAME_GUC),
            worker_login: crate::configured_worker_login(),
            max_heartbeat_age_seconds: crate::MAX_WORKER_HEARTBEAT_AGE_SECONDS_GUC.get(),
            max_wal_lag_bytes: crate::MAX_WAL_LAG_BYTES_GUC.get(),
            max_wal_lag_seconds: crate::MAX_WAL_LAG_SECONDS_GUC.get(),
        }
    }
}

fn configured_string(setting: &pgrx::GucSetting<Option<std::ffi::CString>>) -> Option<String> {
    setting
        .get()
        .and_then(|value| value.to_str().ok().map(String::from))
        .filter(|value| !value.is_empty())
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum CheckState {
    Ok,
    Failed,
    Unknown,
}

#[derive(Clone, Serialize)]
struct HealthCheck {
    state: CheckState,
    reason: &'static str,
}

impl HealthCheck {
    const fn ok() -> Self {
        Self {
            state: CheckState::Ok,
            reason: "ok",
        }
    }

    const fn failed(reason: &'static str) -> Self {
        Self {
            state: CheckState::Failed,
            reason,
        }
    }

    const fn unknown(reason: &'static str) -> Self {
        Self {
            state: CheckState::Unknown,
            reason,
        }
    }
}

#[derive(Default, Serialize)]
struct HealthObservations {
    active_slot_name: Option<String>,
    heartbeat_age_seconds: Option<f64>,
    wal_lag_bytes: Option<i64>,
    wal_lag_seconds: Option<f64>,
    poison: Option<PoisonObservation>,
}

#[derive(Serialize)]
struct PoisonObservation {
    failure_class: String,
    failure_detail: String,
    commit_lsn: String,
    relation_id: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct ReadinessStatus {
    checks: BTreeMap<&'static str, HealthCheck>,
    observations: HealthObservations,
}

impl Default for ReadinessStatus {
    fn default() -> Self {
        let mut checks = BTreeMap::new();
        for name in [
            "database_contract",
            "registry_generation",
            "schema_generation",
            "relation_identity",
            "publication",
            "capture_triggers",
            "replication_slot",
            "stream_reset",
            "poison",
            "materialization_progress",
            "worker",
            "heartbeat",
            "wal_byte_lag",
            "wal_time_lag",
        ] {
            checks.insert(name, HealthCheck::unknown("health_query_unavailable"));
        }
        Self {
            checks,
            observations: HealthObservations::default(),
        }
    }
}

impl ReadinessStatus {
    pub(crate) fn ready(&self) -> bool {
        self.checks
            .values()
            .all(|check| check.state == CheckState::Ok)
    }

    pub(crate) fn detail(&self) -> serde_json::Value {
        serde_json::json!({
            "ready": self.ready(),
            "checks": self.checks,
            "observations": self.observations,
        })
    }

    fn set(&mut self, name: &'static str, check: HealthCheck) {
        self.checks.insert(name, check);
    }
}

struct StoredSchemaState {
    version: i64,
    hash: String,
    body: String,
    parent_version: Option<i64>,
    parent_hash: Option<String>,
    transition_class: String,
    compatibility_floor: i64,
}

struct RawReadiness {
    database_matches: bool,
    extension_matches: bool,
    registry_generation_valid: bool,
    schema_generation_consistent: bool,
    relation_identity_valid: bool,
    publication_valid: bool,
    capture_triggers_valid: bool,
    replication_slot_valid: bool,
    worker_state_valid: bool,
    poison_clear: bool,
    poison: Option<PoisonObservation>,
    active_slot_name: Option<String>,
    stream_reset_clear: bool,
    progress_valid: bool,
    slot_acknowledgement_valid: Option<bool>,
    heartbeat_age_seconds: Option<f64>,
    wal_lag_bytes: Option<i64>,
    wal_lag_seconds: Option<f64>,
    schema: Option<StoredSchemaState>,
}

fn load_raw_readiness(
    client: &SpiClient<'_>,
    configuration: &ReadinessConfiguration,
    worker_login_oid: i64,
) -> Result<RawReadiness, String> {
    let database = configuration.database.as_deref().unwrap_or("");
    let publication = configuration.publication.as_deref().unwrap_or("");
    let row = client
        .select(
            READINESS_SQL,
            None,
            &[
                database.into(),
                publication.into(),
                worker_login_oid.into(),
                env!("CARGO_PKG_VERSION").into(),
            ],
        )
        .map_err(|_| "loading readiness state failed".to_string())?
        .first();

    let schema_version = optional_value::<i64>(&row, "schema_version")?;
    let schema_hash = optional_value::<String>(&row, "schema_hash")?;
    let schema_body = optional_value::<String>(&row, "schema_body")?;
    let schema_transition = optional_value::<String>(&row, "schema_transition_class")?;
    let schema_floor = optional_value::<i64>(&row, "schema_compatibility_floor")?;
    let schema = match (
        schema_version,
        schema_hash,
        schema_body,
        schema_transition,
        schema_floor,
    ) {
        (
            Some(version),
            Some(hash),
            Some(body),
            Some(transition_class),
            Some(compatibility_floor),
        ) => Some(StoredSchemaState {
            version,
            hash,
            body,
            parent_version: optional_value(&row, "schema_parent_version")?,
            parent_hash: optional_value(&row, "schema_parent_hash")?,
            transition_class,
            compatibility_floor,
        }),
        (None, None, None, None, None) => None,
        _ => return Err("schema readiness state is incomplete".to_string()),
    };

    let poison_failure_class = optional_value::<String>(&row, "poison_failure_class")?;
    let poison_failure_detail = optional_value::<String>(&row, "poison_failure_detail")?;
    let poison_commit_lsn = optional_value::<String>(&row, "poison_commit_lsn")?;
    let poison = match (
        poison_failure_class,
        poison_failure_detail,
        poison_commit_lsn,
    ) {
        (Some(failure_class), Some(failure_detail), Some(commit_lsn)) => Some(PoisonObservation {
            failure_class,
            failure_detail,
            commit_lsn,
            relation_id: optional_value(&row, "poison_relation_id")?,
        }),
        (None, None, None) => None,
        _ => return Err("poison readiness state is incomplete".to_string()),
    };

    Ok(RawReadiness {
        database_matches: required_bool(&row, "database_matches")?,
        extension_matches: required_bool(&row, "extension_matches")?,
        registry_generation_valid: required_bool(&row, "registry_generation_valid")?,
        schema_generation_consistent: required_bool(&row, "schema_generation_consistent")?,
        relation_identity_valid: required_bool(&row, "relation_identity_valid")?,
        publication_valid: required_bool(&row, "publication_valid")?,
        capture_triggers_valid: required_bool(&row, "capture_triggers_valid")?,
        replication_slot_valid: required_bool(&row, "replication_slot_valid")?,
        worker_state_valid: required_bool(&row, "worker_state_valid")?,
        poison_clear: required_bool(&row, "poison_clear")?,
        poison,
        active_slot_name: optional_value(&row, "active_slot_name")?,
        stream_reset_clear: required_bool(&row, "stream_reset_clear")?,
        progress_valid: required_bool(&row, "progress_valid")?,
        slot_acknowledgement_valid: optional_value(&row, "slot_acknowledgement_valid")?,
        heartbeat_age_seconds: optional_value(&row, "heartbeat_age_seconds")?,
        wal_lag_bytes: optional_value(&row, "wal_lag_bytes")?,
        wal_lag_seconds: optional_value(&row, "wal_lag_seconds")?,
        schema,
    })
}

fn required_bool(row: &SpiTupleTable<'_>, name: &str) -> Result<bool, String> {
    optional_value(row, name)?.ok_or_else(|| "readiness state is incomplete".to_string())
}

fn optional_value<T: FromDatum + IntoDatum>(
    row: &SpiTupleTable<'_>,
    name: &str,
) -> Result<Option<T>, String> {
    row.get_by_name::<T, &str>(name)
        .map_err(|_| "reading readiness state failed".to_string())
}

fn stored_schema_valid(stored: &StoredSchemaState) -> bool {
    let Ok(mut body) = serde_json::from_str::<serde_json::Value>(&stored.body) else {
        return false;
    };
    let Ok(canonical) = serde_json_canonicalizer::to_vec(&body) else {
        return false;
    };
    if canonical != stored.body.as_bytes() {
        return false;
    }
    let mut hasher = Sha256::new();
    hasher.update(SCHEMA_MANIFEST_DOMAIN);
    hasher.update(&canonical);
    if format!("{:x}", hasher.finalize()) != stored.hash {
        return false;
    }
    let Some(object) = body.as_object_mut() else {
        return false;
    };
    object.insert(
        "schema_hash".to_string(),
        serde_json::Value::String(stored.hash.clone()),
    );
    let Ok(manifest) = serde_json::from_value::<SchemaManifest>(body) else {
        return false;
    };
    manifest.schema_version == stored.version
        && manifest.parent_schema.as_ref().map(|parent| parent.version) == stored.parent_version
        && manifest
            .parent_schema
            .as_ref()
            .map(|parent| parent.hash.as_str())
            == stored.parent_hash.as_deref()
        && serde_json::to_value(manifest.transition_class)
            .ok()
            .and_then(|value| value.as_str().map(String::from))
            .as_deref()
            == Some(stored.transition_class.as_str())
        && manifest.compatibility_floor == stored.compatibility_floor
        && manifest.validate().is_ok()
}

fn known_check(valid: bool, reason: &'static str) -> HealthCheck {
    if valid {
        HealthCheck::ok()
    } else {
        HealthCheck::failed(reason)
    }
}

pub(crate) fn load_readiness_status_with_configuration(
    configuration: ReadinessConfiguration,
) -> ReadinessStatus {
    let mut status = ReadinessStatus::default();

    if configuration.database.is_none() {
        status.set(
            "database_contract",
            HealthCheck::failed("database_not_configured"),
        );
    }
    if configuration.publication.is_none() {
        status.set(
            "publication",
            HealthCheck::failed("publication_not_configured"),
        );
    }
    if configuration.worker_login.is_none() {
        status.set("worker", HealthCheck::failed("worker_not_configured"));
    }
    if configuration.max_heartbeat_age_seconds <= 0 {
        status.set("heartbeat", HealthCheck::failed("invalid_limit"));
    }
    if configuration.max_wal_lag_bytes <= 0 {
        status.set("wal_byte_lag", HealthCheck::failed("invalid_limit"));
    }
    if configuration.max_wal_lag_seconds <= 0 {
        status.set("wal_time_lag", HealthCheck::failed("invalid_limit"));
    }

    let Some(worker_login) = configuration.worker_login.as_deref() else {
        return status;
    };
    let loaded = Spi::connect(|client| {
        let login = validate_worker_login(client, worker_login)?;
        let raw = load_raw_readiness(
            client,
            &configuration,
            login.worker_login_oid.unwrap_or_default(),
        )?;
        Ok::<_, String>((login, raw))
    });
    let Ok((login, raw)) = loaded else {
        return status;
    };

    status.observations.poison = raw.poison;
    status.observations.active_slot_name = raw.active_slot_name;

    if configuration.database.is_some() {
        status.set(
            "database_contract",
            known_check(
                raw.database_matches
                    && raw.extension_matches
                    && crate::client::SQL_CONTRACT_VERSION == 1
                    && crate::client::PROTOCOL_VERSION == 3,
                "database_contract_invalid",
            ),
        );
    }
    status.set(
        "registry_generation",
        known_check(raw.registry_generation_valid, "registry_generation_invalid"),
    );
    status.set(
        "schema_generation",
        match raw.schema {
            None => HealthCheck::unknown("schema_generation_missing"),
            Some(ref schema) if raw.schema_generation_consistent && stored_schema_valid(schema) => {
                HealthCheck::ok()
            }
            Some(_) => HealthCheck::failed("schema_generation_invalid"),
        },
    );
    status.set(
        "relation_identity",
        known_check(raw.relation_identity_valid, "relation_identity_invalid"),
    );
    if configuration.publication.is_some() {
        status.set(
            "publication",
            known_check(raw.publication_valid, "publication_mismatch"),
        );
    }
    status.set(
        "capture_triggers",
        known_check(raw.capture_triggers_valid, "capture_triggers_invalid"),
    );
    status.set(
        "replication_slot",
        known_check(raw.replication_slot_valid, "replication_slot_invalid"),
    );
    status.set("poison", known_check(raw.poison_clear, "blocking_poison"));
    status.set(
        "stream_reset",
        known_check(raw.stream_reset_clear, "stream_reset_incomplete"),
    );
    status.set(
        "materialization_progress",
        if !raw.progress_valid {
            HealthCheck::failed("materialization_progress_invalid")
        } else {
            match raw.slot_acknowledgement_valid {
                Some(true) => HealthCheck::ok(),
                Some(false) => HealthCheck::failed("slot_acknowledgement_invalid"),
                None => HealthCheck::unknown("slot_acknowledgement_unknown"),
            }
        },
    );
    if configuration.worker_login.is_some() {
        status.set(
            "worker",
            if !login.is_valid() {
                HealthCheck::failed("worker_identity_invalid")
            } else {
                known_check(raw.worker_state_valid, "worker_state_invalid")
            },
        );
    }

    status.observations.heartbeat_age_seconds = raw
        .heartbeat_age_seconds
        .filter(|value| value.is_finite() && *value >= 0.0);
    if configuration.max_heartbeat_age_seconds > 0 {
        status.set(
            "heartbeat",
            match raw.heartbeat_age_seconds {
                None => HealthCheck::unknown("heartbeat_unknown"),
                Some(value) if !value.is_finite() || value < 0.0 => {
                    HealthCheck::failed("heartbeat_invalid")
                }
                Some(value) if value <= f64::from(configuration.max_heartbeat_age_seconds) => {
                    HealthCheck::ok()
                }
                Some(_) => HealthCheck::failed("heartbeat_stale"),
            },
        );
    }

    status.observations.wal_lag_bytes = raw.wal_lag_bytes.filter(|value| *value >= 0);
    if configuration.max_wal_lag_bytes > 0 {
        status.set(
            "wal_byte_lag",
            if !raw.replication_slot_valid {
                HealthCheck::unknown("wal_lag_unknown")
            } else {
                match raw.wal_lag_bytes {
                    None => HealthCheck::unknown("wal_lag_unknown"),
                    Some(value) if value < 0 => HealthCheck::failed("wal_lag_invalid"),
                    Some(value) if value <= i64::from(configuration.max_wal_lag_bytes) => {
                        HealthCheck::ok()
                    }
                    Some(_) => HealthCheck::failed("wal_lag_exceeded"),
                }
            },
        );
    }

    status.observations.wal_lag_seconds = raw
        .wal_lag_seconds
        .filter(|value| value.is_finite() && *value >= 0.0);
    if configuration.max_wal_lag_seconds > 0 {
        status.set(
            "wal_time_lag",
            if !raw.replication_slot_valid {
                HealthCheck::unknown("wal_lag_unknown")
            } else {
                match raw.wal_lag_seconds {
                    None => HealthCheck::unknown("wal_lag_unknown"),
                    Some(value) if !value.is_finite() || value < 0.0 => {
                        HealthCheck::failed("wal_lag_invalid")
                    }
                    Some(value) if value <= f64::from(configuration.max_wal_lag_seconds) => {
                        HealthCheck::ok()
                    }
                    Some(_) => HealthCheck::failed("wal_lag_exceeded"),
                }
            },
        );
    }

    status
}

fn load_readiness_status() -> ReadinessStatus {
    load_readiness_status_with_configuration(ReadinessConfiguration::configured())
}

#[pg_extern]
fn synchro_readiness() -> pgrx::JsonB {
    pgrx::JsonB(serde_json::json!({
        "ready": load_readiness_status().ready()
    }))
}

#[pg_extern]
fn synchro_health_detail() -> pgrx::JsonB {
    pgrx::JsonB(load_readiness_status().detail())
}
