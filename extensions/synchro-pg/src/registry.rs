use pgrx::prelude::*;
use pgrx::spi::{SpiClient, SpiHeapTupleData};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use synchro_core::contract::{normalize_portable_type_name, CompositionClass};

const DEFAULT_PUBLICATION_NAME: &str = "synchro_pub";
const PRIMARY_KEY_GUARD_TRIGGER: &str = "synchro_primary_key_guard";
const CAPTURE_FENCE_TRIGGER: &str = "synchro_capture_fence";
const CAPTURE_TRUNCATE_TRIGGER: &str = "synchro_capture_truncate_guard";

/// Capture role for one registered physical relation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RegistrationKind {
    Synced,
    CaptureDependency,
}

impl RegistrationKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Synced => "synced",
            Self::CaptureDependency => "capture_dependency",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "synced" => Some(Self::Synced),
            "capture_dependency" => Some(Self::CaptureDependency),
            _ => None,
        }
    }
}

/// Push policy for a registered table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PushPolicy {
    Enabled,
    ReadOnly,
}

impl PushPolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Enabled => "enabled",
            Self::ReadOnly => "read_only",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "enabled" => Some(Self::Enabled),
            "read_only" => Some(Self::ReadOnly),
            _ => None,
        }
    }
}

/// In-memory representation of one immutable registry-generation entry.
///
/// `table_name` remains the client-visible logical table name. Physical SQL and
/// catalog work must use the schema-qualified physical identity fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRegistration {
    pub registry_generation: i64,
    pub relation_id: String,
    pub registration_kind: RegistrationKind,
    pub table_id: String,
    pub primary_key_field_id: String,
    pub table_name: String,
    pub physical_schema: String,
    pub physical_relation: String,
    pub physical_relation_oid: u32,
    pub replica_identity: String,
    pub composition: CompositionClass,
    pub membership_function: RegisteredFunction,
    pub membership_function_fingerprint: Vec<u8>,
    pub max_scope_fanout: i32,
    pub pk_column: String,
    pub pk_type: String,
    pub pk_portable_type: String,
    pub capture_key_columns: Vec<String>,
    pub updated_at_col: String,
    pub deleted_at_col: String,
    pub push_policy: PushPolicy,
    pub sync_columns: Vec<String>,
    pub exclude_columns: Vec<String>,
    pub has_updated_at: bool,
    pub has_deleted_at: bool,
    pub fields: Vec<FieldRegistration>,
    pub capture_fields: Vec<CaptureFieldRegistration>,
}

impl TableRegistration {
    pub(crate) fn is_synced(&self) -> bool {
        self.registration_kind == RegistrationKind::Synced
    }

    pub(crate) fn is_capture_dependency(&self) -> bool {
        self.registration_kind == RegistrationKind::CaptureDependency
    }
}

/// An immutable catalog identity for a registered SQL function.
///
/// Runtime evaluation constructs a call from this catalog identity. It never
/// executes a caller-provided query string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredFunction {
    pub oid: u32,
    pub schema: String,
    pub name: String,
}

#[derive(Debug, Clone)]
pub(crate) struct MembershipDependency {
    pub dependency_relation_id: String,
    pub dependency_registration_kind: RegistrationKind,
    pub target_relation_id: String,
    pub target_table_id: String,
    pub impact_function: RegisteredFunction,
    pub max_impact_rows: i32,
    pub dependency_columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldRegistration {
    pub field_id: String,
    pub physical_column: String,
    pub portable_type: String,
    pub native_json: bool,
    pub decimal_precision: Option<i32>,
    pub decimal_scale: Option<i32>,
    pub nullable: bool,
    pub writable: bool,
    pub primary_key: bool,
}

/// Internal physical field metadata for a capture-only dependency projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CaptureFieldRegistration {
    pub physical_column: String,
    pub portable_type: String,
    pub nullable: bool,
    pub capture_key: bool,
}

#[derive(Debug, Clone)]
struct PhysicalRelation {
    schema: String,
    relation: String,
    oid: u32,
    replica_identity: String,
}

#[derive(Debug, Clone)]
struct PrimaryKey {
    column: String,
    sql_type: String,
    portable_type: String,
    type_oid: u32,
}

#[derive(Debug, Clone)]
struct BaseGeneration {
    generation: i64,
    stream_generation: String,
}

#[derive(Debug, Clone)]
struct ExistingRegistration {
    relation_id: String,
    table_id: String,
    physical_schema: String,
    physical_relation: String,
    physical_relation_oid: u32,
    pk_column: String,
}

#[pg_extern]
fn synchro_prepare_projection_view(
    p_relation_name: &str,
    p_view_name: &str,
    p_projected_columns: Vec<String>,
) -> pgrx::JsonB {
    let actor = unsafe { pg_sys::GetOuterUserId() };
    if p_relation_name.trim().is_empty()
        || p_view_name.trim().is_empty()
        || p_projected_columns.is_empty()
    {
        pgrx::error!("projection view metadata is incomplete");
    }
    Spi::connect_mut(|client| {
        let view_parts: Vec<String> = client
            .select(
                "SELECT pg_catalog.parse_ident($1, false)",
                None,
                &[p_view_name.into()],
            )?
            .first()
            .get_one()?
            .unwrap_or_default();
        let [view_name] = view_parts.as_slice() else {
            pgrx::error!("projection view name must be one SQL identifier");
        };
        if view_name.starts_with("pg_") || view_name == "information_schema" {
            pgrx::error!("projection view name is invalid");
        }
        let physical = resolve_physical_relation(client, p_relation_name)?;
        validate_actor_owns_relation(client, actor, physical.oid)?;
        let actual_columns = ordered_table_columns_for_oid_in_client(client, physical.oid)?;
        let actual: std::collections::HashSet<&str> =
            actual_columns.iter().map(String::as_str).collect();
        let mut declared = std::collections::HashSet::new();
        for column in &p_projected_columns {
            if column.is_empty()
                || matches!(column.as_str(), "record_id" | "capture_key" | "deleted")
                || !actual.contains(column.as_str())
                || !declared.insert(column.as_str())
            {
                pgrx::error!("projection view columns are invalid");
            }
        }
        let mut projected_columns = p_projected_columns.clone();
        projected_columns.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));

        let existing = client
            .select(
                "SELECT view_name::text AS view_name, projected_columns::text[] AS projected_columns
                 FROM synchro.sync_projection_views
                 WHERE physical_relation_oid = $1::oid OR view_name = $2::name",
                None,
                &[i64::from(physical.oid).into(), view_name.as_str().into()],
            )?
            .next();
        if let Some(existing) = existing {
            let existing_name = existing
                .get_by_name::<String, &str>("view_name")?
                .unwrap_or_default();
            let existing_columns = existing
                .get_by_name::<Vec<String>, &str>("projected_columns")?
                .unwrap_or_default();
            if existing_name != *view_name || existing_columns != projected_columns {
                pgrx::error!("projection view identity is immutable");
            }
            return Ok::<_, spi::Error>(pgrx::JsonB(serde_json::json!({
                "view": format!("synchro_projection.{view_name}"),
                "physical_schema": physical.schema,
                "physical_relation": physical.relation,
                "projected_columns": projected_columns,
            })));
        }

        let mut expressions = Vec::with_capacity(projected_columns.len());
        for column in &projected_columns {
            let column_literal = quote_literal(client, column)?;
            expressions.push(format!(
                "CASE WHEN projection.registration_kind = 'synced' THEN \
                     projection.row_data -> (\
                         SELECT field.field_id::text \
                         FROM synchro.sync_registry_fields field \
                         WHERE field.registry_generation = projection.registry_generation \
                           AND field.relation_id = projection.relation_id \
                           AND field.physical_column = {column_literal}\
                     ) ELSE projection.row_data -> {column_literal} END AS {}",
                crate::pull::pg_quote_ident(column),
            ));
        }
        let schema_literal = quote_literal(client, &physical.schema)?;
        let relation_literal = quote_literal(client, &physical.relation)?;
        let qualified_view = format!(
            "synchro_projection.{}",
            crate::pull::pg_quote_ident(view_name),
        );
        client.update(
            &format!(
                "CREATE VIEW {qualified_view} WITH (security_barrier = true) AS \
                 SELECT projection.record_id, projection.capture_key, projection.deleted, {} \
                 FROM synchro.sync_current_projections projection \
                 WHERE projection.physical_schema = {schema_literal} \
                   AND projection.physical_relation = {relation_literal}",
                expressions.join(", "),
            ),
            None,
            &[],
        )?;
        client.update(
            &format!("ALTER VIEW {qualified_view} OWNER TO synchro_owner"),
            None,
            &[],
        )?;
        client.update(
            &format!("REVOKE ALL ON {qualified_view} FROM PUBLIC"),
            None,
            &[],
        )?;
        client.update(
            &format!("GRANT SELECT ON {qualified_view} TO synchro_owner, synchro_worker"),
            None,
            &[],
        )?;
        client.update(
            "INSERT INTO synchro.sync_projection_views (
                 physical_relation_oid, physical_schema, physical_relation,
                 view_oid, view_name, projected_columns
             ) VALUES (
                 $1::oid, $2::name, $3::name,
                 $4::regclass::oid, $5::name, $6::name[]
             )",
            None,
            &[
                i64::from(physical.oid).into(),
                physical.schema.as_str().into(),
                physical.relation.as_str().into(),
                qualified_view.as_str().into(),
                view_name.as_str().into(),
                projected_columns.clone().into(),
            ],
        )?;
        Ok::<_, spi::Error>(pgrx::JsonB(serde_json::json!({
            "view": format!("synchro_projection.{view_name}"),
            "physical_schema": physical.schema,
            "physical_relation": physical.relation,
            "projected_columns": projected_columns,
        })))
    })
    .unwrap_or_else(|error| pgrx::error!("preparing projection view: {error}"))
}

/// Register a table for synchronization.
///
/// The input identifies one schema-qualified physical relation. The stored
/// table name remains the unqualified physical relation name for clients.
#[pg_extern]
#[allow(clippy::too_many_arguments)]
fn synchro_register_table(
    p_table_name: &str,
    p_membership_function: &str,
    p_composition: &str,
    p_pk_column: default!(&str, "'id'"),
    p_updated_at_col: default!(&str, "'updated_at'"),
    p_deleted_at_col: default!(&str, "'deleted_at'"),
    p_push_policy: default!(&str, "'enabled'"),
    p_exclude_columns: default!(Vec<String>, "'{}'"),
    p_sync_columns: default!(Vec<String>, "'{}'"),
    p_max_scope_fanout: default!(i32, "8"),
) {
    let actor = unsafe { pg_sys::GetOuterUserId() };
    let policy = PushPolicy::parse(p_push_policy).unwrap_or_else(|| {
        pgrx::error!(
            "invalid push_policy: {:?}, expected 'enabled' or 'read_only'",
            p_push_policy
        )
    });
    let composition = match p_composition {
        "single_scope" => CompositionClass::SingleScope,
        "multi_scope" => CompositionClass::MultiScope,
        _ => pgrx::error!(
            "invalid composition: {:?}, expected 'single_scope' or 'multi_scope'",
            p_composition
        ),
    };

    if p_table_name.trim().is_empty() {
        pgrx::error!("table name must not be empty");
    }
    if p_membership_function.trim().is_empty() {
        pgrx::error!("membership_function must not be empty");
    }
    if p_pk_column.trim().is_empty() {
        pgrx::error!("primary key column must not be empty");
    }

    Spi::connect_mut(|client| {
        acquire_registry_write_lock(client)?;
        acquire_source_write_gate(client)?;
        let base = latest_complete_generation(client)?;
        let physical = resolve_physical_relation(client, p_table_name)?;
        let logical_table_name = physical.relation.clone();
        let primary_key = load_and_validate_primary_key(client, physical.oid, p_pk_column)?;
        let max_scope_fanout = validate_scope_fanout_limit(client, p_max_scope_fanout)?;
        let membership_function =
            resolve_membership_function(client, p_membership_function, primary_key.type_oid)?;
        let membership_function_fingerprint =
            registered_function_fingerprint(client, membership_function.oid)?;
        validate_actor_owns_relation(client, actor, physical.oid)?;
        validate_actor_owns_function(client, actor, membership_function.oid)?;
        validate_application_ownership(client, physical.oid, membership_function.oid)?;
        validate_actor_can_manage_publication(client, actor, physical.oid)?;
        validate_publication_owner(client, physical.oid)?;
        let actual_columns = ordered_table_columns_for_oid_in_client(client, physical.oid)?;
        let has_updated_at = actual_columns
            .iter()
            .any(|column| column == p_updated_at_col);
        let has_deleted_at = actual_columns
            .iter()
            .any(|column| column == p_deleted_at_col);
        let (sync_columns, exclude_columns) = normalize_synced_columns(
            &actual_columns,
            &logical_table_name,
            p_pk_column,
            p_updated_at_col,
            p_deleted_at_col,
            &p_sync_columns,
            &p_exclude_columns,
        )
        .unwrap_or_else(|message| pgrx::error!("{}", message));
        validate_relation_privileges(client, physical.oid, &policy, has_deleted_at)?;
        validate_relation_rls(client, physical.oid)?;
        validate_owner_row_security_active(client, physical.oid)?;

        let existing =
            active_registration_for_logical_name(client, base.generation, &logical_table_name)?;
        if let Some(registration) = existing.as_ref() {
            validate_persisted_registration_metadata(client, registration)?;
        }
        reject_physical_registration_collision(
            client,
            base.generation,
            &logical_table_name,
            physical.oid,
        )?;

        let retained = existing.as_ref().filter(|registration| {
            registration.physical_schema == physical.schema
                && registration.physical_relation == physical.relation
                && registration.physical_relation_oid == physical.oid
                && registration.replica_identity == physical.replica_identity
                && registration.pk_column == primary_key.column
                && registration.pk_type == primary_key.sql_type
                && registration.pk_portable_type == primary_key.portable_type
        });
        let prepared_generation = if retained.is_none() {
            Some(create_next_generation(client, &base)?)
        } else {
            None
        };
        let relation_id = retained
            .map(|registration| registration.relation_id.clone())
            .unwrap_or_else(|| new_logical_id(client, "relation"));
        let table_id = retained
            .map(|registration| registration.table_id.clone())
            .unwrap_or_else(|| new_logical_id(client, "table"));
        let fields = build_field_registrations(
            client,
            physical.oid,
            &sync_columns,
            &primary_key.column,
            p_updated_at_col,
            p_deleted_at_col,
            retained.map(|registration| registration.fields.as_slice()),
        )?;
        let primary_key_field_id = fields
            .iter()
            .find(|field| field.primary_key)
            .map(|field| field.field_id.clone())
            .unwrap_or_else(|| pgrx::error!("registered primary key has no field identity"));
        let mut registration = TableRegistration {
            registry_generation: base.generation,
            relation_id,
            registration_kind: RegistrationKind::Synced,
            table_id,
            primary_key_field_id,
            table_name: logical_table_name,
            physical_schema: physical.schema.clone(),
            physical_relation: physical.relation.clone(),
            physical_relation_oid: physical.oid,
            replica_identity: physical.replica_identity.clone(),
            composition,
            membership_function,
            membership_function_fingerprint,
            max_scope_fanout,
            pk_column: primary_key.column,
            pk_type: primary_key.sql_type,
            pk_portable_type: primary_key.portable_type,
            capture_key_columns: vec![p_pk_column.to_string()],
            updated_at_col: p_updated_at_col.to_string(),
            deleted_at_col: p_deleted_at_col.to_string(),
            push_policy: policy,
            sync_columns,
            exclude_columns,
            has_updated_at,
            has_deleted_at,
            fields,
            capture_fields: Vec::new(),
        };
        if retained.is_some_and(|active| same_registration_content(active, &registration)) {
            return Ok(());
        }

        let next_generation = match prepared_generation {
            Some(generation) => generation,
            None => create_next_generation(client, &base)?,
        };
        registration.registry_generation = next_generation;

        if retained.is_some() {
            client.update(
                "DELETE FROM synchro.sync_registry_fields
                 WHERE registry_generation = $1 AND relation_id = $2::uuid",
                None,
                &[
                    registration.registry_generation.into(),
                    registration.relation_id.as_str().into(),
                ],
            )?;
        } else {
            client.update(
                "DELETE FROM synchro.sync_registry
                 WHERE registry_generation = $1 AND table_name = $2",
                None,
                &[
                    registration.registry_generation.into(),
                    registration.table_name.as_str().into(),
                ],
            )?;
        }
        client.update(
            "INSERT INTO synchro.sync_registry (
                 registry_generation,
                 relation_id,
                 registration_kind,
                 table_id,
                primary_key_field_id,
                table_name,
                physical_schema,
                physical_relation,
                 physical_relation_oid,
                 replica_identity,
                 composition,
                 membership_function_oid,
                 membership_function_schema,
                 membership_function_name,
                 max_scope_fanout,
                 pk_column,
                 pk_type,
                 pk_portable_type,
                 capture_key_columns,
                updated_at_col,
                deleted_at_col,
                push_policy,
                sync_columns,
                 exclude_columns,
                 has_updated_at,
                 has_deleted_at,
                 membership_function_fingerprint
             ) VALUES (
                $1, $2::uuid, 'synced', $3::uuid, $4::uuid, $5, $6, $7, $8::oid,
                 $9::\"char\", $10, $11::oid, $12::name, $13::name, $14, $15,
                  $16, $17, $18::text[], $19, $20, $21, $22::text[], $23::text[], $24, $25,
                   $26
              )
              ON CONFLICT (registry_generation, relation_id) DO UPDATE SET
                  registration_kind = EXCLUDED.registration_kind,
                  table_id = EXCLUDED.table_id,
                  primary_key_field_id = EXCLUDED.primary_key_field_id,
                  table_name = EXCLUDED.table_name,
                  physical_schema = EXCLUDED.physical_schema,
                  physical_relation = EXCLUDED.physical_relation,
                  physical_relation_oid = EXCLUDED.physical_relation_oid,
                  replica_identity = EXCLUDED.replica_identity,
                  composition = EXCLUDED.composition,
                  membership_function_oid = EXCLUDED.membership_function_oid,
                  membership_function_schema = EXCLUDED.membership_function_schema,
                  membership_function_name = EXCLUDED.membership_function_name,
                  max_scope_fanout = EXCLUDED.max_scope_fanout,
                  pk_column = EXCLUDED.pk_column,
                  pk_type = EXCLUDED.pk_type,
                  pk_portable_type = EXCLUDED.pk_portable_type,
                  capture_key_columns = EXCLUDED.capture_key_columns,
                  updated_at_col = EXCLUDED.updated_at_col,
                  deleted_at_col = EXCLUDED.deleted_at_col,
                  push_policy = EXCLUDED.push_policy,
                  sync_columns = EXCLUDED.sync_columns,
                  exclude_columns = EXCLUDED.exclude_columns,
                  has_updated_at = EXCLUDED.has_updated_at,
                  has_deleted_at = EXCLUDED.has_deleted_at,
                  membership_function_fingerprint = EXCLUDED.membership_function_fingerprint,
                  created_at = EXCLUDED.created_at,
                  updated_at = EXCLUDED.updated_at",
            None,
            &[
                registration.registry_generation.into(),
                registration.relation_id.as_str().into(),
                registration.table_id.as_str().into(),
                registration.primary_key_field_id.as_str().into(),
                registration.table_name.as_str().into(),
                registration.physical_schema.as_str().into(),
                registration.physical_relation.as_str().into(),
                i64::from(registration.physical_relation_oid).into(),
                registration.replica_identity.as_str().into(),
                p_composition.into(),
                i64::from(registration.membership_function.oid).into(),
                registration.membership_function.schema.as_str().into(),
                registration.membership_function.name.as_str().into(),
                registration.max_scope_fanout.into(),
                registration.pk_column.as_str().into(),
                registration.pk_type.as_str().into(),
                registration.pk_portable_type.as_str().into(),
                registration.capture_key_columns.clone().into(),
                registration.updated_at_col.as_str().into(),
                registration.deleted_at_col.as_str().into(),
                registration.push_policy.as_str().into(),
                registration.sync_columns.clone().into(),
                registration.exclude_columns.clone().into(),
                registration.has_updated_at.into(),
                registration.has_deleted_at.into(),
                registration.membership_function_fingerprint.clone().into(),
            ],
        )?;
        insert_field_registrations(
            client,
            registration.registry_generation,
            &registration.relation_id,
            &registration.fields,
        )?;
        stage_membership_replacement_if_changed(
            client,
            base.generation,
            registration.registry_generation,
            &registration.relation_id,
        )?;

        with_registration_actor_ddl(
            actor,
            std::panic::AssertUnwindSafe(|| {
                ensure_publication_membership(client, &physical)?;
                install_capture_triggers(client, &registration)
            }),
        )?;
        validate_generation_entries(client, registration.registry_generation)?;
        mark_generation_validated(client, registration.registry_generation)?;
        emit_registry_activation_when_ready(client, registration.registry_generation)?;
        Ok::<_, spi::Error>(())
    })
    .unwrap_or_else(|error| pgrx::error!("registering table {:?}: {}", p_table_name, error));
}

/// Register a capture-only relation for declared membership impacts.
///
/// A capture dependency has no client table, fields, push surface, or direct
/// pull effects. This bounded registration path accepts only an empty source.
/// A populated source needs the verified projection bootstrap before activation.
#[pg_extern]
fn synchro_register_capture_dependency(
    p_relation_name: &str,
    p_capture_key_columns: Vec<String>,
    p_captured_columns: Vec<String>,
) {
    let actor = unsafe { pg_sys::GetOuterUserId() };
    if p_relation_name.trim().is_empty()
        || p_capture_key_columns.is_empty()
        || p_captured_columns.is_empty()
    {
        pgrx::error!("capture dependency registration metadata is incomplete");
    }
    if p_capture_key_columns.len() != 1 {
        pgrx::error!("capture dependency capture key must contain exactly one primary-key column");
    }
    let capture_key_column = p_capture_key_columns
        .first()
        .expect("capture dependency key was checked")
        .clone();
    if capture_key_column.trim().is_empty() {
        pgrx::error!("capture dependency capture key is invalid");
    }

    Spi::connect_mut(|client| {
        acquire_registry_write_lock(client)?;
        acquire_source_write_gate(client)?;
        let base = latest_complete_generation(client)?;
        let physical = resolve_physical_relation(client, p_relation_name)?;
        let primary_key = load_and_validate_primary_key(client, physical.oid, &capture_key_column)?;
        let capture_fields = build_capture_field_registrations(
            client,
            physical.oid,
            &p_capture_key_columns,
            &p_captured_columns,
        )?;
        validate_actor_owns_relation(client, actor, physical.oid)?;
        validate_capture_application_ownership(client, physical.oid)?;
        validate_actor_can_manage_publication(client, actor, physical.oid)?;
        validate_publication_owner(client, physical.oid)?;
        validate_relation_privileges(client, physical.oid, &PushPolicy::ReadOnly, false)?;
        validate_relation_rls(client, physical.oid)?;
        validate_owner_row_security_active(client, physical.oid)?;

        let existing = client
            .select(
                "SELECT registry_generation,
                        relation_id::text AS relation_id,
                        registration_kind,
                        table_id::text AS table_id,
                        primary_key_field_id::text AS primary_key_field_id,
                        table_name,
                        physical_schema::text AS physical_schema,
                        physical_relation::text AS physical_relation,
                        physical_relation_oid::bigint AS physical_relation_oid,
                        replica_identity::text AS replica_identity,
                        composition,
                        membership_function_oid::bigint AS membership_function_oid,
                        membership_function_schema::text AS membership_function_schema,
                        membership_function_name::text AS membership_function_name,
                        membership_function_fingerprint,
                        max_scope_fanout,
                        pk_column,
                        pk_type,
                        pk_portable_type,
                        capture_key_columns,
                        updated_at_col,
                        deleted_at_col,
                        push_policy,
                        sync_columns,
                        exclude_columns,
                        has_updated_at,
                        has_deleted_at
                 FROM synchro.sync_registry
                 WHERE registry_generation = $1
                   AND physical_relation_oid = $2::oid",
                None,
                &[base.generation.into(), i64::from(physical.oid).into()],
            )?
            .next()
            .map(|row| {
                let mut registration = registration_from_row(&row)?;
                registration.fields =
                    load_field_registrations(client, base.generation, &registration.relation_id)?;
                registration.capture_fields = load_capture_field_registrations(
                    client,
                    base.generation,
                    &registration.relation_id,
                )?;
                Ok::<_, spi::Error>(registration)
            })
            .transpose()?;
        if let Some(existing) = &existing {
            validate_persisted_registration_metadata(client, existing)?;
            if !existing.is_capture_dependency() {
                pgrx::error!("physical relation is already registered under a synced table");
            }
        }
        let relation_id = existing
            .as_ref()
            .map(|registration| registration.relation_id.clone())
            .unwrap_or_else(|| {
                client
                    .update(
                        "INSERT INTO synchro.sync_logical_ids (logical_id, kind)
                         VALUES (gen_random_uuid(), 'relation')
                         RETURNING logical_id::text AS logical_id",
                        None,
                        &[],
                    )
                    .unwrap_or_else(|error| pgrx::error!("creating relation ID: {error}"))
                    .first()
                    .get_by_name::<String, &str>("logical_id")
                    .unwrap_or_else(|error| pgrx::error!("reading relation ID: {error}"))
                    .unwrap_or_else(|| pgrx::error!("creating relation ID returned no value"))
            });
        let registration_name = format!(
            "capture_dependency:{}.{}",
            physical.schema, physical.relation
        );
        let mut registration = TableRegistration {
            registry_generation: base.generation,
            relation_id,
            registration_kind: RegistrationKind::CaptureDependency,
            table_id: String::new(),
            primary_key_field_id: String::new(),
            table_name: registration_name,
            physical_schema: physical.schema.clone(),
            physical_relation: physical.relation.clone(),
            physical_relation_oid: physical.oid,
            replica_identity: physical.replica_identity.clone(),
            composition: CompositionClass::SingleScope,
            membership_function: RegisteredFunction {
                oid: 0,
                schema: String::new(),
                name: String::new(),
            },
            membership_function_fingerprint: Vec::new(),
            max_scope_fanout: 0,
            pk_column: primary_key.column,
            pk_type: primary_key.sql_type,
            pk_portable_type: primary_key.portable_type,
            capture_key_columns: p_capture_key_columns,
            updated_at_col: String::new(),
            deleted_at_col: String::new(),
            push_policy: PushPolicy::ReadOnly,
            sync_columns: Vec::new(),
            exclude_columns: Vec::new(),
            has_updated_at: false,
            has_deleted_at: false,
            fields: Vec::new(),
            capture_fields,
        };
        if existing
            .as_ref()
            .is_some_and(|active| same_registration_content(active, &registration))
        {
            return Ok(());
        }
        let next_generation = create_next_generation(client, &base)?;
        registration.registry_generation = next_generation;
        client.update(
            "DELETE FROM synchro.sync_registry
             WHERE registry_generation = $1 AND physical_relation_oid = $2::oid",
            None,
            &[
                registration.registry_generation.into(),
                i64::from(registration.physical_relation_oid).into(),
            ],
        )?;
        client.update(
            "INSERT INTO synchro.sync_registry (
                 registry_generation, relation_id, registration_kind, table_id,
                 primary_key_field_id, table_name, physical_schema,
                 physical_relation, physical_relation_oid, replica_identity,
                 composition, membership_function_oid, membership_function_schema,
                 membership_function_name, max_scope_fanout, pk_column, pk_type,
                 pk_portable_type, capture_key_columns, updated_at_col,
                 deleted_at_col, push_policy, sync_columns, exclude_columns,
                 has_updated_at, has_deleted_at, membership_function_fingerprint
             ) VALUES (
                 $1, $2::uuid, 'capture_dependency', NULL, NULL, $3, $4, $5,
                 $6::oid, $7::\"char\", NULL, NULL, NULL, NULL, NULL, $8, $9,
                 $10, $11::text[], '', '', 'read_only', '{}'::text[],
                  '{}'::text[], false, false, NULL
             )",
            None,
            &[
                registration.registry_generation.into(),
                registration.relation_id.as_str().into(),
                registration.table_name.as_str().into(),
                registration.physical_schema.as_str().into(),
                registration.physical_relation.as_str().into(),
                i64::from(registration.physical_relation_oid).into(),
                registration.replica_identity.as_str().into(),
                registration.pk_column.as_str().into(),
                registration.pk_type.as_str().into(),
                registration.pk_portable_type.as_str().into(),
                registration.capture_key_columns.clone().into(),
            ],
        )?;
        insert_capture_field_registrations(
            client,
            registration.registry_generation,
            &registration.relation_id,
            &registration.capture_fields,
        )?;

        with_registration_actor_ddl(
            actor,
            std::panic::AssertUnwindSafe(|| {
                ensure_publication_membership(client, &physical)?;
                install_capture_triggers(client, &registration)
            }),
        )?;
        validate_generation_entries(client, registration.registry_generation)?;
        mark_generation_validated(client, registration.registry_generation)?;
        emit_registry_activation_when_ready(client, registration.registry_generation)?;
        Ok::<_, spi::Error>(())
    })
    .unwrap_or_else(|error| {
        pgrx::error!(
            "registering capture dependency {:?}: {}",
            p_relation_name,
            error
        )
    });
}

/// Unregister a logical client table from synchronization.
///
/// The operation writes a new complete generation. It never mutates the active
/// generation in place.
#[pg_extern]
fn synchro_unregister_table(p_table_name: &str) {
    let actor = unsafe { pg_sys::GetOuterUserId() };
    if p_table_name.trim().is_empty() {
        pgrx::error!("table name must not be empty");
    }

    Spi::connect_mut(|client| {
        acquire_registry_write_lock(client)?;
        acquire_source_write_gate(client)?;
        let base = latest_complete_generation(client)?;
        let existing = active_registration_for_unregister(client, base.generation, p_table_name)?
            .unwrap_or_else(|| pgrx::error!("table {:?} is not registered", p_table_name));
        validate_actor_owns_relation(client, actor, existing.physical_relation_oid)?;
        validate_actor_owns_registered_membership_function(
            client,
            actor,
            base.generation,
            &existing.relation_id,
        )?;
        validate_actor_can_manage_publication(client, actor, existing.physical_relation_oid)?;
        validate_publication_owner(client, existing.physical_relation_oid)?;
        let next_generation = create_next_generation(client, &base)?;

        client.update(
            "DELETE FROM synchro.sync_registry WHERE registry_generation = $1 AND table_name = $2",
            None,
            &[next_generation.into(), existing.table_name.as_str().into()],
        )?;
        validate_generation_entries(client, next_generation)?;
        mark_generation_validated(client, next_generation)?;
        emit_registry_activation_when_ready(client, next_generation)?;
        Ok::<_, spi::Error>(())
    })
    .unwrap_or_else(|error| pgrx::error!("unregistering table {:?}: {}", p_table_name, error));
}

/// Declare the bounded impact rule from a captured dependency relation to a
/// synced target relation.
///
/// The declaration is copied with the complete registry generation. The worker
/// evaluates this function after it applies all source projections for a WAL
/// transaction.
#[pg_extern]
fn synchro_register_membership_dependency(
    p_dependency_table_name: &str,
    p_target_table_name: &str,
    p_impact_function: &str,
    p_dependency_field_ids: Vec<String>,
    p_max_impact_rows: default!(i32, "1000"),
) {
    let actor = unsafe { pg_sys::GetOuterUserId() };
    if p_dependency_table_name.trim().is_empty()
        || p_target_table_name.trim().is_empty()
        || p_impact_function.trim().is_empty()
    {
        pgrx::error!("membership dependency metadata is incomplete");
    }
    if p_dependency_field_ids.is_empty() {
        pgrx::error!("membership dependency must declare captured fields");
    }

    Spi::connect_mut(|client| {
        acquire_registry_write_lock(client)?;
        acquire_source_write_gate(client)?;
        let base = latest_complete_generation(client)?;
        let registrations = load_registry_generation_entries(client, base.generation, false)?;
        let dependency = registered_relation_for_dependency_reference(
            client,
            &registrations,
            p_dependency_table_name,
        )?;
        let target = registered_relation_for_dependency_reference(
            client,
            &registrations,
            p_target_table_name,
        )?;
        if dependency.relation_id == target.relation_id {
            pgrx::error!("membership dependency cannot target itself");
        }
        if !target.is_synced() {
            pgrx::error!("membership dependency target must be a synced relation");
        }
        let (dependency_field_ids, dependency_columns) =
            validate_declared_dependency_fields(dependency, &p_dependency_field_ids)?;
        let impact_function = resolve_impact_function(client, p_impact_function)?;
        let impact_function_fingerprint =
            registered_function_fingerprint(client, impact_function.oid)?;
        validate_actor_owns_relation(client, actor, dependency.physical_relation_oid)?;
        validate_actor_owns_relation(client, actor, target.physical_relation_oid)?;
        if dependency.is_synced() {
            validate_actor_owns_registered_membership_function(
                client,
                actor,
                base.generation,
                &dependency.relation_id,
            )?;
        }
        validate_actor_owns_registered_membership_function(
            client,
            actor,
            base.generation,
            &target.relation_id,
        )?;
        validate_actor_owns_function(client, actor, impact_function.oid)?;
        validate_dependency_application_ownership(
            client,
            dependency.physical_relation_oid,
            target.physical_relation_oid,
            &impact_function,
        )?;
        let max_impact_rows = validate_impact_row_limit(client, p_max_impact_rows)?;

        let matches = client
            .select(
                "SELECT EXISTS (
                     SELECT 1
                     FROM synchro.sync_membership_dependencies
                     WHERE registry_generation = $1
                       AND dependency_relation_id = $2::uuid
                       AND dependency_registration_kind = $3
                       AND target_relation_id = $4::uuid
                       AND impact_function_oid = $5::oid
                       AND impact_function_schema = $6::name
                       AND impact_function_name = $7::name
                       AND impact_function_fingerprint = $8::bytea
                       AND max_impact_rows = $9
                       AND dependency_field_ids = $10::text[]
                       AND dependency_columns = $11::text[]
                 ) AS matches",
                None,
                &[
                    base.generation.into(),
                    dependency.relation_id.as_str().into(),
                    dependency.registration_kind.as_str().into(),
                    target.relation_id.as_str().into(),
                    i64::from(impact_function.oid).into(),
                    impact_function.schema.as_str().into(),
                    impact_function.name.as_str().into(),
                    impact_function_fingerprint.clone().into(),
                    max_impact_rows.into(),
                    dependency_field_ids.clone().into(),
                    dependency_columns.clone().into(),
                ],
            )?
            .first()
            .get_by_name::<bool, &str>("matches")?
            .unwrap_or(false);
        if matches {
            return Ok(());
        }

        let next_generation = create_next_generation(client, &base)?;
        client.update(
            "DELETE FROM synchro.sync_membership_dependencies
             WHERE registry_generation = $1
               AND dependency_relation_id = $2::uuid
               AND target_relation_id = $3::uuid",
            None,
            &[
                next_generation.into(),
                dependency.relation_id.as_str().into(),
                target.relation_id.as_str().into(),
            ],
        )?;
        client.update(
            "INSERT INTO synchro.sync_membership_dependencies (
                 registry_generation, dependency_id, dependency_relation_id,
                  dependency_registration_kind, target_relation_id,
                  impact_function_oid, impact_function_schema,
                   impact_function_name, max_impact_rows, dependency_field_ids,
                   dependency_columns, impact_function_fingerprint
              ) VALUES (
                  $1, gen_random_uuid(), $2::uuid, $3, $4::uuid, $5::oid, $6::name,
                   $7::name, $8, $9::text[], $10::text[], $11
              )",
            None,
            &[
                next_generation.into(),
                dependency.relation_id.as_str().into(),
                dependency.registration_kind.as_str().into(),
                target.relation_id.as_str().into(),
                i64::from(impact_function.oid).into(),
                impact_function.schema.as_str().into(),
                impact_function.name.as_str().into(),
                max_impact_rows.into(),
                dependency_field_ids
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    .into(),
                dependency_columns
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    .into(),
                impact_function_fingerprint.into(),
            ],
        )?;
        client.update(
            "INSERT INTO synchro.sync_registry_membership_stages (
                 registry_generation, source_registry_generation,
                 target_relation_ids, state
             ) VALUES ($1, $2, ARRAY[$3::uuid], 'pending')",
            None,
            &[
                next_generation.into(),
                base.generation.into(),
                target.relation_id.as_str().into(),
            ],
        )?;
        validate_generation_entries(client, next_generation)?;
        mark_generation_validated(client, next_generation)?;
        emit_registry_activation_when_ready(client, next_generation)?;
        Ok::<_, spi::Error>(())
    })
    .unwrap_or_else(|error| {
        pgrx::error!(
            "registering membership dependency {:?} -> {:?}: {}",
            p_dependency_table_name,
            p_target_table_name,
            error
        )
    });
}

fn validate_declared_dependency_fields(
    dependency: &TableRegistration,
    field_ids: &[String],
) -> Result<(Vec<String>, Vec<String>), spi::Error> {
    if dependency.is_synced() {
        let available: std::collections::HashMap<&str, &str> = dependency
            .fields
            .iter()
            .map(|field| (field.field_id.as_str(), field.physical_column.as_str()))
            .collect();
        let mut declared = std::collections::HashSet::new();
        let mut columns = Vec::with_capacity(field_ids.len());
        for field_id in field_ids {
            let Some(column) = available.get(field_id.as_str()) else {
                pgrx::error!("membership dependency fields are invalid");
            };
            if field_id.is_empty() || !declared.insert(field_id) {
                pgrx::error!("membership dependency fields are invalid");
            }
            columns.push((*column).to_string());
        }
        columns.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        return Ok((field_ids.to_vec(), columns));
    }

    let available: std::collections::HashSet<&str> = dependency
        .capture_fields
        .iter()
        .map(|field| field.physical_column.as_str())
        .collect();
    let mut declared = std::collections::HashSet::new();
    for column in field_ids {
        if column.is_empty() || !available.contains(column.as_str()) || !declared.insert(column) {
            pgrx::error!("capture dependency fields are invalid");
        }
    }
    if declared.len() != available.len() {
        pgrx::error!("capture dependency fields must equal the registered capture projection");
    }
    let mut columns = field_ids.to_vec();
    columns.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    Ok((Vec::new(), columns))
}

fn registered_relation_for_dependency_reference<'a>(
    _client: &SpiClient<'_>,
    registrations: &'a [TableRegistration],
    reference: &str,
) -> Result<&'a TableRegistration, spi::Error> {
    let mut matches = registrations.iter().filter(|registration| {
        if registration.is_synced() {
            registration.table_name == reference
        } else {
            registration.physical_relation == reference
                || format!(
                    "{}.{}",
                    registration.physical_schema, registration.physical_relation
                ) == reference
        }
    });
    let Some(registration) = matches.next() else {
        pgrx::error!("membership dependency relation is not registered");
    };
    if matches.next().is_some() {
        pgrx::error!("membership dependency relation reference is ambiguous");
    }
    Ok(registration)
}

fn stage_membership_replacement_if_changed(
    client: &mut SpiClient<'_>,
    source_generation: i64,
    target_generation: i64,
    relation_id: &str,
) -> Result<(), spi::Error> {
    client.update(
        "INSERT INTO synchro.sync_registry_membership_stages (
             registry_generation, source_registry_generation,
             target_relation_ids, state
         )
         SELECT $2, $1, ARRAY[target.relation_id], 'pending'
         FROM synchro.sync_registry target
         JOIN synchro.sync_registry source
           ON source.registry_generation = $1
          AND source.relation_id = target.relation_id
         WHERE target.registry_generation = $2
           AND target.relation_id = $3::uuid
           AND target.registration_kind = 'synced'
           AND (
               source.composition IS DISTINCT FROM target.composition
               OR source.membership_function_oid IS DISTINCT FROM target.membership_function_oid
               OR source.membership_function_schema IS DISTINCT FROM target.membership_function_schema
               OR source.membership_function_name IS DISTINCT FROM target.membership_function_name
               OR source.membership_function_fingerprint IS DISTINCT FROM target.membership_function_fingerprint
               OR source.max_scope_fanout IS DISTINCT FROM target.max_scope_fanout
           )",
            None,
            &[
                source_generation.into(),
                target_generation.into(),
                relation_id.into(),
            ],
        )?;
    Ok(())
}

fn validate_capture_application_ownership(
    client: &SpiClient<'_>,
    relation_oid: u32,
) -> Result<(), spi::Error> {
    let valid = client
        .select(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_class relation
                 JOIN pg_catalog.pg_roles owner_role ON owner_role.oid = relation.relowner
                 WHERE relation.oid = $1::oid
                   AND owner_role.rolname <> 'synchro_owner'
             ) AS valid",
            None,
            &[i64::from(relation_oid).into()],
        )?
        .first()
        .get_by_name::<bool, &str>("valid")?
        .unwrap_or(false);
    if !valid {
        pgrx::error!("capture dependency relation owner is invalid");
    }
    Ok(())
}

fn configured_membership_limits(client: &SpiClient<'_>) -> Result<(i32, i32), spi::Error> {
    let row = client
        .select(
            "SELECT max_scope_fanout, max_impact_rows
             FROM synchro.sync_membership_limits
             WHERE singleton = true",
            None,
            &[],
        )?
        .first();
    let max_scope_fanout = row
        .get_by_name::<i32, &str>("max_scope_fanout")?
        .unwrap_or_else(|| pgrx::error!("configured scope fanout limit is missing"));
    let max_impact_rows = row
        .get_by_name::<i32, &str>("max_impact_rows")?
        .unwrap_or_else(|| pgrx::error!("configured impact row limit is missing"));
    if max_scope_fanout <= 0 || max_impact_rows <= 0 {
        pgrx::error!("configured membership limits must be positive");
    }
    Ok((max_scope_fanout, max_impact_rows))
}

fn validate_scope_fanout_limit(client: &SpiClient<'_>, value: i32) -> Result<i32, spi::Error> {
    let (maximum, _) = configured_membership_limits(client)?;
    if value <= 0 || value > maximum {
        pgrx::error!("membership scope fanout limit is invalid");
    }
    Ok(value)
}

fn validate_impact_row_limit(client: &SpiClient<'_>, value: i32) -> Result<i32, spi::Error> {
    let (_, maximum) = configured_membership_limits(client)?;
    if value <= 0 || value > maximum {
        pgrx::error!("membership impact row limit is invalid");
    }
    Ok(value)
}

fn parse_qualified_function(
    client: &SpiClient<'_>,
    value: &str,
) -> Result<(String, String), spi::Error> {
    let parts: Option<Vec<String>> = client
        .select(
            "SELECT pg_catalog.parse_ident($1, false)",
            None,
            &[value.into()],
        )?
        .first()
        .get_one()?;
    let Some(parts) = parts else {
        pgrx::error!("function identity is invalid");
    };
    let [schema, name] = parts.as_slice() else {
        pgrx::error!("function identity must be schema-qualified");
    };
    if schema.starts_with("pg_") || schema == "information_schema" || name.is_empty() {
        pgrx::error!("function identity is invalid");
    }
    Ok((schema.clone(), name.clone()))
}

fn quote_literal(client: &SpiClient<'_>, value: &str) -> Result<String, spi::Error> {
    let literal = client
        .select(
            "SELECT pg_catalog.quote_literal($1) AS literal",
            None,
            &[value.into()],
        )?
        .first()
        .get_by_name::<String, &str>("literal")?
        .unwrap_or_else(|| pgrx::error!("quoting projection metadata failed"));
    Ok(literal)
}

fn registered_function_fingerprint(
    client: &SpiClient<'_>,
    function_oid: u32,
) -> Result<Vec<u8>, spi::Error> {
    let prior_search_path = client
        .select(
            "SELECT pg_catalog.current_setting('search_path') AS search_path",
            None,
            &[],
        )?
        .first()
        .get_by_name::<String, &str>("search_path")?
        .unwrap_or_else(|| pgrx::error!("search path is unavailable"));
    client.select(
        "SELECT pg_catalog.set_config('search_path', 'pg_catalog, synchro', true)",
        None,
        &[],
    )?;
    let definition = client
        .select(
            "SELECT pg_catalog.pg_get_functiondef($1::oid) AS definition",
            None,
            &[i64::from(function_oid).into()],
        )?
        .first()
        .get_by_name::<String, &str>("definition")?
        .unwrap_or_else(|| pgrx::error!("registered function definition is missing"));
    client.select(
        "SELECT pg_catalog.set_config('search_path', $1, true)",
        None,
        &[prior_search_path.as_str().into()],
    )?;
    Ok(Sha256::digest(definition.as_bytes()).to_vec())
}

fn resolve_membership_function(
    client: &SpiClient<'_>,
    identity: &str,
    primary_key_type_oid: u32,
) -> Result<RegisteredFunction, spi::Error> {
    let (schema, name) = parse_qualified_function(client, identity)?;
    let rows = client.select(
        "SELECT p.oid::bigint AS function_oid,
                n.nspname::text AS function_schema,
                p.proname::text AS function_name,
                p.proretset AS returns_set,
                p.provolatile::text AS volatility,
                p.prosecdef AS security_definer,
                l.lanname::text AS language,
                p.prokind::text AS function_kind,
                p.prosqlbody IS NOT NULL AS parsed_body,
                COALESCE(p.proconfig, '{}'::text[]) =
                    ARRAY['search_path=pg_catalog, synchro']::text[] AS fixed_path
         FROM pg_catalog.pg_proc p
         JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
         JOIN pg_catalog.pg_language l ON l.oid = p.prolang
         WHERE n.nspname = $1
            AND p.proname = $2
            AND p.pronargs = 1
            AND p.pronargdefaults = 0
            AND p.provariadic = 0::oid
            AND p.proargtypes[0] = $3::oid
            AND p.prorettype = 'text'::pg_catalog.regtype",
        None,
        &[
            schema.as_str().into(),
            name.as_str().into(),
            i64::from(primary_key_type_oid).into(),
        ],
    )?;
    let rows: Vec<_> = rows.into_iter().collect();
    if rows.len() != 1 {
        pgrx::error!("membership function signature is invalid");
    }
    let row = &rows[0];
    let returns_set = row
        .get_by_name::<bool, &str>("returns_set")?
        .unwrap_or(false);
    let volatility = row
        .get_by_name::<String, &str>("volatility")?
        .unwrap_or_default();
    let security_definer = row
        .get_by_name::<bool, &str>("security_definer")?
        .unwrap_or(true);
    let language = row
        .get_by_name::<String, &str>("language")?
        .unwrap_or_default();
    let function_kind = row
        .get_by_name::<String, &str>("function_kind")?
        .unwrap_or_default();
    let parsed_body = row
        .get_by_name::<bool, &str>("parsed_body")?
        .unwrap_or(false);
    let fixed_path = row
        .get_by_name::<bool, &str>("fixed_path")?
        .unwrap_or(false);
    #[cfg(feature = "pg_test")]
    let legacy_test_function = schema == "tests";
    #[cfg(not(feature = "pg_test"))]
    let legacy_test_function = false;
    if !returns_set
        || volatility != "s"
        || security_definer
        || language != "sql"
        || function_kind != "f"
        || (!legacy_test_function && (!parsed_body || !fixed_path))
    {
        pgrx::error!("membership function does not meet the deterministic contract");
    }
    let function = registered_function_from_row(row)?;
    validate_registered_function_acl(client, &function)?;
    validate_registered_function_dependencies(client, &function)?;
    Ok(function)
}

fn resolve_impact_function(
    client: &SpiClient<'_>,
    identity: &str,
) -> Result<RegisteredFunction, spi::Error> {
    let (schema, name) = parse_qualified_function(client, identity)?;
    let rows = client.select(
        "SELECT p.oid::bigint AS function_oid,
                n.nspname::text AS function_schema,
                p.proname::text AS function_name,
                p.proretset AS returns_set,
                p.provolatile::text AS volatility,
                p.prosecdef AS security_definer,
                l.lanname::text AS language,
                p.prokind::text AS function_kind,
                p.prosqlbody IS NOT NULL AS parsed_body,
                COALESCE(p.proconfig, '{}'::text[]) =
                    ARRAY['search_path=pg_catalog, synchro']::text[] AS fixed_path
         FROM pg_catalog.pg_proc p
         JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
         JOIN pg_catalog.pg_language l ON l.oid = p.prolang
         WHERE n.nspname = $1
            AND p.proname = $2
            AND p.pronargs = 2
            AND p.pronargdefaults = 0
            AND p.provariadic = 0::oid
            AND p.proargtypes = ARRAY[
                 'jsonb'::pg_catalog.regtype::oid,
                 'jsonb'::pg_catalog.regtype::oid
           ]::pg_catalog.oidvector
            AND p.prorettype = 'synchro.synchro_row_ref'::pg_catalog.regtype",
        None,
        &[schema.as_str().into(), name.as_str().into()],
    )?;
    let rows: Vec<_> = rows.into_iter().collect();
    if rows.len() != 1 {
        pgrx::error!("impact function signature is invalid");
    }
    let row = &rows[0];
    let returns_set = row
        .get_by_name::<bool, &str>("returns_set")?
        .unwrap_or(false);
    let volatility = row
        .get_by_name::<String, &str>("volatility")?
        .unwrap_or_default();
    let security_definer = row
        .get_by_name::<bool, &str>("security_definer")?
        .unwrap_or(true);
    let language = row
        .get_by_name::<String, &str>("language")?
        .unwrap_or_default();
    let function_kind = row
        .get_by_name::<String, &str>("function_kind")?
        .unwrap_or_default();
    let parsed_body = row
        .get_by_name::<bool, &str>("parsed_body")?
        .unwrap_or(false);
    let fixed_path = row
        .get_by_name::<bool, &str>("fixed_path")?
        .unwrap_or(false);
    if !returns_set
        || volatility != "s"
        || security_definer
        || language != "sql"
        || function_kind != "f"
        || !parsed_body
        || !fixed_path
    {
        pgrx::error!("impact function does not meet the deterministic contract");
    }
    let function = registered_function_from_row(row)?;
    validate_registered_function_acl(client, &function)?;
    validate_registered_function_dependencies(client, &function)?;
    Ok(function)
}

fn validate_registered_function_dependencies(
    client: &SpiClient<'_>,
    function: &RegisteredFunction,
) -> Result<(), spi::Error> {
    #[cfg(feature = "pg_test")]
    if function.schema == "tests" {
        return Ok(());
    }
    let valid = client
        .select(
            "SELECT
                 NOT EXISTS (
                     SELECT 1
                     FROM pg_catalog.pg_depend dependency
                     JOIN pg_catalog.pg_class relation
                       ON dependency.refclassid = 'pg_catalog.pg_class'::regclass
                      AND relation.oid = dependency.refobjid
                     JOIN pg_catalog.pg_namespace namespace
                       ON namespace.oid = relation.relnamespace
                     LEFT JOIN synchro.sync_projection_views projection
                       ON projection.view_oid = relation.oid
                     WHERE dependency.classid = 'pg_catalog.pg_proc'::regclass
                       AND dependency.objid = $1::oid
                       AND dependency.deptype = 'n'
                       AND namespace.nspname NOT IN ('pg_catalog', 'information_schema')
                       AND projection.view_oid IS NULL
                 )
                 AND NOT EXISTS (
                     SELECT 1
                     FROM pg_catalog.pg_depend dependency
                     JOIN pg_catalog.pg_proc called
                       ON dependency.refclassid = 'pg_catalog.pg_proc'::regclass
                      AND called.oid = dependency.refobjid
                     JOIN pg_catalog.pg_namespace namespace
                       ON namespace.oid = called.pronamespace
                     WHERE dependency.classid = 'pg_catalog.pg_proc'::regclass
                       AND dependency.objid = $1::oid
                       AND dependency.deptype = 'n'
                       AND namespace.nspname <> 'pg_catalog'
                 ) AS valid",
            None,
            &[i64::from(function.oid).into()],
        )?
        .first()
        .get_by_name::<bool, &str>("valid")?
        .unwrap_or(false);
    if !valid {
        pgrx::error!("registered function reads an undeclared projection dependency");
    }
    Ok(())
}

fn validate_registered_function_acl(
    client: &SpiClient<'_>,
    function: &RegisteredFunction,
) -> Result<(), spi::Error> {
    let valid: bool = client
        .select(
            "WITH roles AS (
                 SELECT (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = 'synchro_owner') AS owner_oid,
                        (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = 'synchro_worker') AS worker_oid
             ), function_acl AS (
                 SELECT procedure.proacl,
                        procedure.proowner,
                        namespace.oid AS schema_oid
                 FROM pg_catalog.pg_proc procedure
                 JOIN pg_catalog.pg_namespace namespace ON namespace.oid = procedure.pronamespace
                 WHERE procedure.oid = $1::oid
             )
             SELECT EXISTS (
                 SELECT 1
                 FROM function_acl
                 CROSS JOIN roles
                 WHERE roles.owner_oid IS NOT NULL
                   AND roles.worker_oid IS NOT NULL
                   AND pg_catalog.has_schema_privilege(roles.owner_oid, function_acl.schema_oid, 'USAGE')
                   AND pg_catalog.has_schema_privilege(roles.worker_oid, function_acl.schema_oid, 'USAGE')
                   AND EXISTS (
                       SELECT 1
                       FROM pg_catalog.aclexplode(
                            COALESCE(function_acl.proacl, pg_catalog.acldefault('f', function_acl.proowner))
                       ) AS acl(grantor, grantee, privilege_type, is_grantable)
                       WHERE acl.grantee = roles.owner_oid
                         AND acl.privilege_type = 'EXECUTE'
                   )
                   AND EXISTS (
                       SELECT 1
                       FROM pg_catalog.aclexplode(
                            COALESCE(function_acl.proacl, pg_catalog.acldefault('f', function_acl.proowner))
                       ) AS acl(grantor, grantee, privilege_type, is_grantable)
                       WHERE acl.grantee = roles.worker_oid
                         AND acl.privilege_type = 'EXECUTE'
                   )
                   AND NOT EXISTS (
                       SELECT 1
                       FROM pg_catalog.aclexplode(
                            COALESCE(function_acl.proacl, pg_catalog.acldefault('f', function_acl.proowner))
                       ) AS acl(grantor, grantee, privilege_type, is_grantable)
                       WHERE acl.grantee = 0
                         AND acl.privilege_type = 'EXECUTE'
                   )
             ) AS valid",
            None,
            &[i64::from(function.oid).into()],
        )?
        .first()
        .get_by_name("valid")?
        .unwrap_or(false);
    if !valid {
        pgrx::error!("registered function access control is invalid");
    }
    Ok(())
}

fn registered_function_from_row(
    row: &SpiHeapTupleData<'_>,
) -> Result<RegisteredFunction, spi::Error> {
    let oid = row
        .get_by_name::<i64, &str>("function_oid")?
        .map(checked_oid)
        .unwrap_or_else(|| pgrx::error!("registered function has no OID"));
    let schema = row
        .get_by_name::<String, &str>("function_schema")?
        .unwrap_or_else(|| pgrx::error!("registered function has no schema"));
    let name = row
        .get_by_name::<String, &str>("function_name")?
        .unwrap_or_else(|| pgrx::error!("registered function has no name"));
    Ok(RegisteredFunction { oid, schema, name })
}

fn validate_registered_membership_function(
    client: &SpiClient<'_>,
    registration: &TableRegistration,
    primary_key_type_oid: u32,
) -> Result<(), spi::Error> {
    let resolved = resolve_membership_function(
        client,
        &format!(
            "{}.{}",
            crate::pull::pg_quote_ident(&registration.membership_function.schema),
            crate::pull::pg_quote_ident(&registration.membership_function.name),
        ),
        primary_key_type_oid,
    )?;
    if resolved != registration.membership_function {
        pgrx::error!("registered membership function has drifted");
    }
    let expected = client
        .select(
            "SELECT membership_function_fingerprint
             FROM synchro.sync_registry
             WHERE registry_generation = $1 AND relation_id = $2::uuid",
            None,
            &[
                registration.registry_generation.into(),
                registration.relation_id.as_str().into(),
            ],
        )?
        .first()
        .get_one::<Vec<u8>>()?
        .unwrap_or_default();
    if expected.len() != 32 || registered_function_fingerprint(client, resolved.oid)? != expected {
        pgrx::error!("registered membership function definition has drifted");
    }
    Ok(())
}

fn validate_registered_impact_function(
    client: &SpiClient<'_>,
    function: &RegisteredFunction,
    expected_fingerprint: &[u8],
) -> Result<(), spi::Error> {
    let resolved = resolve_impact_function(
        client,
        &format!(
            "{}.{}",
            crate::pull::pg_quote_ident(&function.schema),
            crate::pull::pg_quote_ident(&function.name),
        ),
    )?;
    if resolved != *function {
        pgrx::error!("registered impact function has drifted");
    }
    if expected_fingerprint.len() != 32
        || registered_function_fingerprint(client, resolved.oid)? != expected_fingerprint
    {
        pgrx::error!("registered impact function definition has drifted");
    }
    Ok(())
}

fn resolve_physical_relation(
    client: &SpiClient<'_>,
    input: &str,
) -> Result<PhysicalRelation, spi::Error> {
    let parts: Vec<String> = client
        .select(
            "SELECT pg_catalog.parse_ident($1, false)",
            None,
            &[input.into()],
        )?
        .first()
        .get_one()?
        .unwrap_or_else(|| pgrx::error!("invalid table name {:?}", input));

    match parts.as_slice() {
        [schema, relation] => {
            let candidates = load_physical_relation_candidates(client, Some(schema), relation)?;
            match candidates.as_slice() {
                [physical] => Ok(physical.clone()),
                [] => pgrx::error!("table {:?} does not name a non-system relation", input),
                _ => pgrx::error!("table {:?} is ambiguous", input),
            }
        }
        _ => pgrx::error!("table {:?} must be schema-qualified", input),
    }
}

fn load_physical_relation_candidates(
    client: &SpiClient<'_>,
    schema: Option<&String>,
    relation: &str,
) -> Result<Vec<PhysicalRelation>, spi::Error> {
    let query = if schema.is_some() {
        "SELECT n.nspname::text AS physical_schema,
                    c.relname::text AS physical_relation,
                    c.oid::bigint AS physical_relation_oid,
                    c.relreplident::text AS replica_identity
             FROM pg_catalog.pg_class c
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1
               AND c.relname = $2
               AND c.relkind IN ('r', 'p')
               AND n.nspname !~ '^pg_'
               AND n.nspname <> 'information_schema'"
    } else {
        "SELECT n.nspname::text AS physical_schema,
                    c.relname::text AS physical_relation,
                    c.oid::bigint AS physical_relation_oid,
                    c.relreplident::text AS replica_identity
             FROM pg_catalog.pg_class c
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
             WHERE c.relname = $1
               AND c.relkind IN ('r', 'p')
               AND n.nspname !~ '^pg_'
               AND n.nspname <> 'information_schema'
             ORDER BY n.nspname, c.oid"
    };
    let arguments = match schema {
        Some(schema) => vec![schema.as_str().into(), relation.into()],
        None => vec![relation.into()],
    };
    let rows = client.select(query, None, &arguments)?;
    let mut candidates = Vec::new();
    for row in rows {
        candidates.push(physical_relation_from_row(&row)?);
    }
    Ok(candidates)
}

fn physical_relation_from_row(row: &SpiHeapTupleData<'_>) -> Result<PhysicalRelation, spi::Error> {
    let schema = row
        .get_by_name::<String, &str>("physical_schema")?
        .unwrap_or_else(|| pgrx::error!("physical relation has no schema"));
    let relation = row
        .get_by_name::<String, &str>("physical_relation")?
        .unwrap_or_else(|| pgrx::error!("physical relation has no name"));
    let relation_oid = row
        .get_by_name::<i64, &str>("physical_relation_oid")?
        .unwrap_or_else(|| pgrx::error!("physical relation has no OID"));
    let replica_identity = row
        .get_by_name::<String, &str>("replica_identity")?
        .unwrap_or_else(|| pgrx::error!("physical relation has no replica identity"));

    Ok(PhysicalRelation {
        schema,
        relation,
        oid: checked_oid(relation_oid),
        replica_identity,
    })
}

fn relation_by_oid(
    client: &SpiClient<'_>,
    relation_oid: u32,
) -> Result<Option<PhysicalRelation>, spi::Error> {
    let rows = client.select(
        "SELECT n.nspname::text AS physical_schema,
                c.relname::text AS physical_relation,
                c.oid::bigint AS physical_relation_oid,
                c.relreplident::text AS replica_identity
         FROM pg_catalog.pg_class c
         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
         WHERE c.oid = $1::oid
           AND c.relkind IN ('r', 'p')
           AND n.nspname !~ '^pg_'
           AND n.nspname <> 'information_schema'",
        None,
        &[i64::from(relation_oid).into()],
    )?;
    let Some(row) = rows.into_iter().next() else {
        return Ok(None);
    };
    Ok(Some(physical_relation_from_row(&row)?))
}

fn validate_actor_owns_relation(
    client: &SpiClient<'_>,
    actor: pg_sys::Oid,
    relation_oid: u32,
) -> Result<(), spi::Error> {
    let owns: bool = client
        .select(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_class relation
                 WHERE relation.oid = $1::oid
                   AND relation.relowner = $2::oid
             ) AS owns",
            None,
            &[
                i64::from(relation_oid).into(),
                i64::from(actor.to_u32()).into(),
            ],
        )?
        .first()
        .get_by_name("owns")?
        .unwrap_or(false);
    if !owns {
        pgrx::error!("registration actor does not own the application relation");
    }
    Ok(())
}

fn validate_actor_owns_function(
    client: &SpiClient<'_>,
    actor: pg_sys::Oid,
    function_oid: u32,
) -> Result<(), spi::Error> {
    let owns: bool = client
        .select(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_proc procedure
                 WHERE procedure.oid = $1::oid
                   AND procedure.proowner = $2::oid
             ) AS owns",
            None,
            &[
                i64::from(function_oid).into(),
                i64::from(actor.to_u32()).into(),
            ],
        )?
        .first()
        .get_by_name("owns")?
        .unwrap_or(false);
    if !owns {
        pgrx::error!("registration actor does not own the membership function");
    }
    Ok(())
}

fn validate_actor_owns_registered_membership_function(
    client: &SpiClient<'_>,
    actor: pg_sys::Oid,
    generation: i64,
    relation_id: &str,
) -> Result<(), spi::Error> {
    let owns: bool = client
        .select(
            "SELECT EXISTS (
                 SELECT 1
                 FROM synchro.sync_registry registry
                 JOIN pg_catalog.pg_proc procedure
                   ON procedure.oid = registry.membership_function_oid
                 WHERE registry.registry_generation = $1
                   AND registry.relation_id = $2::uuid
                   AND procedure.proowner = $3::oid
             ) AS owns",
            None,
            &[
                generation.into(),
                relation_id.into(),
                i64::from(actor.to_u32()).into(),
            ],
        )?
        .first()
        .get_by_name("owns")?
        .unwrap_or(false);
    if !owns {
        pgrx::error!("registration actor does not own the membership function");
    }
    Ok(())
}

fn validate_application_ownership(
    client: &SpiClient<'_>,
    relation_oid: u32,
    membership_function_oid: u32,
) -> Result<(), spi::Error> {
    let valid: bool = client
        .select(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_class relation
                 JOIN pg_catalog.pg_proc membership_function
                   ON membership_function.oid = $2::oid
                 CROSS JOIN (
                     SELECT oid
                     FROM pg_catalog.pg_roles
                     WHERE rolname = 'synchro_owner'
                 ) AS synchro_owner
                 WHERE relation.oid = $1::oid
                   AND relation.relowner = membership_function.proowner
                   AND relation.relowner <> synchro_owner.oid
             ) AS valid",
            None,
            &[
                i64::from(relation_oid).into(),
                i64::from(membership_function_oid).into(),
            ],
        )?
        .first()
        .get_by_name("valid")?
        .unwrap_or(false);
    if !valid {
        pgrx::error!("registered relation and membership function owners are invalid");
    }
    Ok(())
}

fn validate_dependency_application_ownership(
    client: &SpiClient<'_>,
    dependency_relation_oid: u32,
    target_relation_oid: u32,
    impact_function: &RegisteredFunction,
) -> Result<(), spi::Error> {
    let valid: bool = client
        .select(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_class dependency_relation
                 JOIN pg_catalog.pg_class target_relation ON target_relation.oid = $2::oid
                 JOIN pg_catalog.pg_proc impact ON impact.oid = $3::oid
                 WHERE dependency_relation.oid = $1::oid
                   AND dependency_relation.relowner = target_relation.relowner
                   AND dependency_relation.relowner = impact.proowner
             ) AS valid",
            None,
            &[
                i64::from(dependency_relation_oid).into(),
                i64::from(target_relation_oid).into(),
                i64::from(impact_function.oid).into(),
            ],
        )?
        .first()
        .get_by_name("valid")?
        .unwrap_or(false);
    if !valid {
        pgrx::error!("membership dependency application owners are invalid");
    }
    Ok(())
}

fn validate_publication_owner(client: &SpiClient<'_>, relation_oid: u32) -> Result<(), spi::Error> {
    let publication = configured_publication_name();
    let valid: bool = client
        .select(
            "SELECT NOT EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_publication publication
                 WHERE publication.pubname = $1
             ) OR EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_publication publication
                 JOIN pg_catalog.pg_class relation ON relation.oid = $2::oid
                 WHERE publication.pubname = $1
                   AND publication.pubowner = relation.relowner
             ) AS valid",
            None,
            &[publication.as_str().into(), i64::from(relation_oid).into()],
        )?
        .first()
        .get_by_name("valid")?
        .unwrap_or(false);
    if !valid {
        pgrx::error!("configured publication owner does not match the application relation owner");
    }
    Ok(())
}

fn validate_actor_can_manage_publication(
    client: &SpiClient<'_>,
    actor: pg_sys::Oid,
    relation_oid: u32,
) -> Result<(), spi::Error> {
    let publication = configured_publication_name();
    let rows = client.select(
        "SELECT publication.pubowner = $2::oid AS owns_publication
         FROM pg_catalog.pg_publication publication
         WHERE publication.pubname = $1",
        None,
        &[
            publication.as_str().into(),
            i64::from(actor.to_u32()).into(),
        ],
    )?;
    if let Some(row) = rows.into_iter().next() {
        let owns_publication = row
            .get_by_name::<bool, &str>("owns_publication")?
            .unwrap_or(false);
        if !owns_publication {
            pgrx::error!("registration actor does not own the configured publication");
        }
        return Ok(());
    }

    validate_actor_owns_relation(client, actor, relation_oid)?;
    let can_create: bool = client
        .select(
            "SELECT pg_catalog.has_database_privilege(
                 $1::oid,
                 pg_catalog.current_database(),
                 'CREATE'
             ) AS can_create",
            None,
            &[i64::from(actor.to_u32()).into()],
        )?
        .first()
        .get_by_name("can_create")?
        .unwrap_or(false);
    if !can_create {
        pgrx::error!("registration actor cannot create the configured publication");
    }
    Ok(())
}

fn validate_relation_privileges(
    client: &SpiClient<'_>,
    relation_oid: u32,
    policy: &PushPolicy,
    has_deleted_at: bool,
) -> Result<(), spi::Error> {
    let requires_write = *policy == PushPolicy::Enabled;
    let requires_delete = requires_write && !has_deleted_at;
    let valid: bool = client
        .select(
            "WITH owner_role AS (
                 SELECT oid
                 FROM pg_catalog.pg_roles
                 WHERE rolname = 'synchro_owner'
             ), relation_acl AS (
                 SELECT relation.relacl, relation.relowner
                 FROM pg_catalog.pg_class relation
                 WHERE relation.oid = $1::oid
             )
             SELECT EXISTS (
                 SELECT 1
                 FROM relation_acl
                 CROSS JOIN owner_role
                 WHERE EXISTS (
                     SELECT 1
                     FROM pg_catalog.aclexplode(
                          COALESCE(relation_acl.relacl, pg_catalog.acldefault('r', relation_acl.relowner))
                     ) AS acl(grantor, grantee, privilege_type, is_grantable)
                     WHERE acl.grantee = owner_role.oid
                       AND acl.privilege_type = 'SELECT'
                 )
                   AND (
                       NOT $2
                       OR (
                           EXISTS (
                               SELECT 1
                               FROM pg_catalog.aclexplode(
                                    COALESCE(relation_acl.relacl, pg_catalog.acldefault('r', relation_acl.relowner))
                               ) AS acl(grantor, grantee, privilege_type, is_grantable)
                               WHERE acl.grantee = owner_role.oid
                                 AND acl.privilege_type = 'INSERT'
                           )
                           AND EXISTS (
                               SELECT 1
                               FROM pg_catalog.aclexplode(
                                    COALESCE(relation_acl.relacl, pg_catalog.acldefault('r', relation_acl.relowner))
                               ) AS acl(grantor, grantee, privilege_type, is_grantable)
                               WHERE acl.grantee = owner_role.oid
                                 AND acl.privilege_type = 'UPDATE'
                           )
                       )
                   )
                    AND (
                       NOT $3
                       OR EXISTS (
                           SELECT 1
                           FROM pg_catalog.aclexplode(
                                COALESCE(relation_acl.relacl, pg_catalog.acldefault('r', relation_acl.relowner))
                           ) AS acl(grantor, grantee, privilege_type, is_grantable)
                           WHERE acl.grantee = owner_role.oid
                             AND acl.privilege_type = 'DELETE'
                       )
                    )
                   AND NOT EXISTS (
                       SELECT 1
                       FROM pg_catalog.aclexplode(
                            COALESCE(relation_acl.relacl, pg_catalog.acldefault('r', relation_acl.relowner))
                       ) AS acl(grantor, grantee, privilege_type, is_grantable)
                       WHERE acl.grantee = owner_role.oid
                         AND acl.privilege_type IN ('INSERT', 'UPDATE', 'DELETE')
                         AND (
                             (acl.privilege_type IN ('INSERT', 'UPDATE') AND NOT $2)
                             OR (acl.privilege_type = 'DELETE' AND NOT $3)
                         )
                   )
              ) AS valid",
            None,
            &[
                i64::from(relation_oid).into(),
                requires_write.into(),
                requires_delete.into(),
            ],
        )?
        .first()
        .get_by_name("valid")?
        .unwrap_or(false);
    if !valid {
        pgrx::error!("synchro_owner direct relation privileges do not match the push policy");
    }
    Ok(())
}

fn validate_relation_rls(client: &SpiClient<'_>, relation_oid: u32) -> Result<(), spi::Error> {
    let valid: bool = client
        .select(
            "WITH owner_role AS (
                 SELECT oid
                 FROM pg_catalog.pg_roles
                 WHERE rolname = 'synchro_owner'
             ), relation_policy AS (
                 SELECT relation.relrowsecurity, relation.relowner
                 FROM pg_catalog.pg_class relation
                 WHERE relation.oid = $1::oid
             )
             SELECT EXISTS (
                 SELECT 1
                 FROM relation_policy
                 CROSS JOIN owner_role
                 WHERE relation_policy.relrowsecurity
                   AND relation_policy.relowner <> owner_role.oid
                   AND (
                       SELECT count(*)
                       FROM pg_catalog.pg_policy policy
                       WHERE policy.polrelid = $1::oid
                         AND policy.polpermissive
                         AND policy.polcmd = '*'
                         AND cardinality(policy.polroles) = 1
                         AND policy.polroles[1] = owner_role.oid
                         AND policy.polqual IS NOT NULL
                         AND policy.polwithcheck IS NOT NULL
                   ) = 1
                   AND NOT EXISTS (
                       SELECT 1
                       FROM pg_catalog.pg_policy policy
                       WHERE policy.polrelid = $1::oid
                         AND policy.polpermissive
                         AND 0 = ANY(policy.polroles)
                   )
             ) AS valid",
            None,
            &[i64::from(relation_oid).into()],
        )?
        .first()
        .get_by_name("valid")?
        .unwrap_or(false);
    if !valid {
        pgrx::error!("registered relation row-level security policy is invalid");
    }
    Ok(())
}

fn validate_owner_row_security_active(
    client: &SpiClient<'_>,
    relation_oid: u32,
) -> Result<(), spi::Error> {
    let active: bool = client
        .select(
            "SELECT pg_catalog.row_security_active($1::oid) AS active",
            None,
            &[i64::from(relation_oid).into()],
        )?
        .first()
        .get_by_name("active")?
        .unwrap_or(false);
    if !active {
        pgrx::error!("row-level security is not active for synchro_owner");
    }
    Ok(())
}

fn with_registration_actor_ddl<R, F>(actor: pg_sys::Oid, body: F) -> R
where
    F: FnOnce() -> R + std::panic::UnwindSafe,
{
    let mut saved_user_id = pg_sys::InvalidOid;
    let mut saved_sec_context = 0;
    unsafe {
        pg_sys::GetUserIdAndSecContext(&mut saved_user_id, &mut saved_sec_context);
        pg_sys::SetUserIdAndSecContext(
            actor,
            saved_sec_context | pg_sys::SECURITY_LOCAL_USERID_CHANGE as i32,
        );
    }
    // SAFETY: PgTryBuilder restores the saved definer identity on every exit path.
    PgTryBuilder::new(body)
        .finally(move || unsafe {
            pg_sys::SetUserIdAndSecContext(saved_user_id, saved_sec_context);
        })
        .execute()
}

fn load_and_validate_primary_key(
    client: &SpiClient<'_>,
    relation_oid: u32,
    requested_column: &str,
) -> Result<PrimaryKey, spi::Error> {
    let rows = client.select(
        "SELECT a.attname::text AS attname,
                a.attnotnull AS attnotnull,
                a.atttypid::bigint AS type_oid,
                pg_catalog.format_type(a.atttypid, a.atttypmod) AS sql_type,
                i.indnkeyatts::integer AS key_count,
                (i.indpred IS NULL) AS is_not_partial,
                (i.indexprs IS NULL) AS has_no_expressions,
                key.attnum::integer AS key_attnum,
                c.relreplident::text AS replica_identity
         FROM pg_catalog.pg_class c
         JOIN pg_catalog.pg_index i ON i.indrelid = c.oid AND i.indisprimary
         JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS key(attnum, ordinality) ON true
         LEFT JOIN pg_catalog.pg_attribute a
           ON a.attrelid = c.oid AND a.attnum = key.attnum AND NOT a.attisdropped
         WHERE c.oid = $1::oid
         ORDER BY key.ordinality",
        None,
        &[i64::from(relation_oid).into()],
    )?;
    let rows: Vec<_> = rows.into_iter().collect();
    let Some(first) = rows.first() else {
        pgrx::error!("registered relation must have one declared primary key");
    };

    let key_count = first
        .get_by_name::<i32, &str>("key_count")?
        .unwrap_or_else(|| pgrx::error!("primary key metadata is incomplete"));
    let is_not_partial = first
        .get_by_name::<bool, &str>("is_not_partial")?
        .unwrap_or(false);
    let has_no_expressions = first
        .get_by_name::<bool, &str>("has_no_expressions")?
        .unwrap_or(false);
    let replica_identity = first
        .get_by_name::<String, &str>("replica_identity")?
        .unwrap_or_default();

    if key_count != 1 || rows.len() != 1 {
        pgrx::error!("registered relation primary key must have exactly one column");
    }
    if !is_not_partial || !has_no_expressions {
        pgrx::error!("registered relation primary key must be a plain non-partial key");
    }
    if replica_identity != "d" {
        pgrx::error!("registered relation requires REPLICA IDENTITY DEFAULT");
    }

    let key_attnum = first.get_by_name::<i32, &str>("key_attnum")?.unwrap_or(0);
    let column = first
        .get_by_name::<String, &str>("attname")?
        .unwrap_or_else(|| pgrx::error!("registered relation primary key must be a column"));
    let not_null = first
        .get_by_name::<bool, &str>("attnotnull")?
        .unwrap_or(false);
    let sql_type = first
        .get_by_name::<String, &str>("sql_type")?
        .unwrap_or_else(|| pgrx::error!("registered relation primary key has no SQL type"));
    let type_oid = first
        .get_by_name::<i64, &str>("type_oid")?
        .map(checked_oid)
        .unwrap_or_else(|| pgrx::error!("registered relation primary key has no type OID"));

    if key_attnum <= 0 {
        pgrx::error!("registered relation primary key must be a column");
    }
    if !not_null {
        pgrx::error!("registered relation primary key must be non-null");
    }
    if column != requested_column {
        pgrx::error!(
            "requested primary key column {:?} does not match declared primary key {:?}",
            requested_column,
            column
        );
    }

    let portable_type = primary_key_portable_type(&sql_type).unwrap_or_else(|| {
        pgrx::error!(
            "registered relation primary key type {:?} is not portable",
            sql_type
        )
    });

    Ok(PrimaryKey {
        column,
        sql_type,
        portable_type,
        type_oid,
    })
}

fn primary_key_portable_type(sql_type: &str) -> Option<String> {
    normalize_portable_type_name(sql_type)
        .filter(|portable| matches!(*portable, "string" | "int" | "int64"))
        .map(str::to_string)
}

pub(crate) fn acquire_registry_write_lock(client: &SpiClient<'_>) -> Result<(), spi::Error> {
    client.select(
        "SELECT pg_catalog.pg_advisory_xact_lock($1::bigint)",
        None,
        &[0x7379_6e63i64.into()],
    )?;
    Ok(())
}

pub(crate) fn acquire_source_write_gate(client: &SpiClient<'_>) -> Result<(), spi::Error> {
    client.select(
        "SELECT pg_catalog.pg_advisory_xact_lock($1::bigint)",
        None,
        &[crate::SOURCE_WRITE_GATE_LOCK_KEY.into()],
    )?;
    Ok(())
}

fn latest_complete_generation(client: &SpiClient<'_>) -> Result<BaseGeneration, spi::Error> {
    let rows = client.select(
        "SELECT rg.generation, rg.stream_generation::text AS stream_generation
          FROM synchro.sync_registry_generations rg
          JOIN synchro.sync_runtime_state rs
           ON rs.singleton = true
          AND rs.stream_generation = rg.stream_generation
         WHERE rg.state IN ('active', 'pending')
           AND rg.validated
         ORDER BY rg.generation DESC
         LIMIT 1",
        None,
        &[],
    )?;
    let row = rows
        .into_iter()
        .next()
        .unwrap_or_else(|| pgrx::error!("there is no complete registry generation"));
    let generation = row
        .get_by_name::<i64, &str>("generation")?
        .unwrap_or_else(|| pgrx::error!("complete registry generation has no number"));
    let stream_generation = row
        .get_by_name::<String, &str>("stream_generation")?
        .unwrap_or_else(|| pgrx::error!("complete registry generation has no stream generation"));
    if generation <= 0 {
        pgrx::error!("complete registry generation is invalid");
    }
    Ok(BaseGeneration {
        generation,
        stream_generation,
    })
}

fn create_next_generation(
    client: &mut SpiClient<'_>,
    base: &BaseGeneration,
) -> Result<i64, spi::Error> {
    let bootstrap_active = client
        .select(
            "SELECT EXISTS (
                 SELECT 1
                 FROM synchro.sync_stream_resets
                 WHERE operation_kind = 'projection_bootstrap'
                   AND lifecycle IN ('preparing', 'baseline_staged', 'catching_up')
             ) AS active",
            None,
            &[],
        )?
        .first()
        .get_by_name::<bool, &str>("active")?
        .unwrap_or(true);
    if bootstrap_active {
        pgrx::error!("registry configuration is locked by a projection bootstrap");
    }
    let generation_rows = client.update(
        "INSERT INTO synchro.sync_registry_generations (
             stream_generation, state, validated, parent_generation
         )
         VALUES ($1, 'pending', false, $2)
         RETURNING generation",
        None,
        &[
            base.stream_generation.as_str().into(),
            base.generation.into(),
        ],
    )?;
    let new_generation = generation_rows
        .first()
        .get_by_name::<i64, &str>("generation")?
        .unwrap_or_else(|| pgrx::error!("creating registry generation returned no generation"));
    if new_generation <= base.generation {
        pgrx::error!("registry generation did not advance");
    }

    client.update(
        "INSERT INTO synchro.sync_registry (
            registry_generation,
            relation_id,
            registration_kind,
            table_id,
            primary_key_field_id,
            table_name,
            physical_schema,
            physical_relation,
             physical_relation_oid,
             replica_identity,
             composition,
             membership_function_oid,
             membership_function_schema,
             membership_function_name,
             membership_function_fingerprint,
             max_scope_fanout,
            pk_column,
            pk_type,
            pk_portable_type,
            capture_key_columns,
            updated_at_col,
            deleted_at_col,
            push_policy,
            sync_columns,
            exclude_columns,
            has_updated_at,
            has_deleted_at,
            created_at,
            updated_at
         )
         SELECT
             $1,
             relation_id,
             registration_kind,
             table_id,
            primary_key_field_id,
            table_name,
            physical_schema,
            physical_relation,
             physical_relation_oid,
             replica_identity,
             composition,
             membership_function_oid,
             membership_function_schema,
             membership_function_name,
             membership_function_fingerprint,
             max_scope_fanout,
             pk_column,
             pk_type,
             pk_portable_type,
             capture_key_columns,
            updated_at_col,
            deleted_at_col,
            push_policy,
            sync_columns,
            exclude_columns,
            has_updated_at,
            has_deleted_at,
            created_at,
            now()
          FROM synchro.sync_registry
         WHERE registry_generation = $2",
        None,
        &[new_generation.into(), base.generation.into()],
    )?;
    client.update(
        "INSERT INTO synchro.sync_registry_fields (
             registry_generation, relation_id, field_id, physical_column,
             portable_type, native_json, decimal_precision, decimal_scale,
             nullable, writable, primary_key
         )
         SELECT $1, relation_id, field_id, physical_column,
                 portable_type, native_json, decimal_precision, decimal_scale,
                nullable, writable, primary_key
          FROM synchro.sync_registry_fields
         WHERE registry_generation = $2",
        None,
        &[new_generation.into(), base.generation.into()],
    )?;
    client.update(
        "INSERT INTO synchro.sync_capture_dependency_fields (
             registry_generation, relation_id, physical_column,
             portable_type, nullable, capture_key
         )
         SELECT $1, relation_id, physical_column,
                portable_type, nullable, capture_key
         FROM synchro.sync_capture_dependency_fields
         WHERE registry_generation = $2",
        None,
        &[new_generation.into(), base.generation.into()],
    )?;
    client.update(
        "INSERT INTO synchro.sync_membership_dependencies (
             registry_generation, dependency_id, dependency_relation_id,
             dependency_registration_kind, target_relation_id,
             impact_function_oid, impact_function_schema,
             impact_function_name, max_impact_rows, dependency_field_ids,
             dependency_columns, impact_function_fingerprint
         )
         SELECT $1, dependency_id, dependency_relation_id,
                dependency_registration_kind, target_relation_id,
                impact_function_oid, impact_function_schema,
                impact_function_name, max_impact_rows, dependency_field_ids,
                dependency_columns, impact_function_fingerprint
          FROM synchro.sync_membership_dependencies
         WHERE registry_generation = $2",
        None,
        &[new_generation.into(), base.generation.into()],
    )?;

    Ok(new_generation)
}

fn mark_generation_validated(
    client: &mut SpiClient<'_>,
    generation: i64,
) -> Result<(), spi::Error> {
    let count = client
        .update(
            "UPDATE synchro.sync_registry_generations
             SET validated = true
             WHERE generation = $1 AND state = 'pending' AND NOT validated",
            None,
            &[generation.into()],
        )?
        .len();
    if count != 1 {
        pgrx::error!("pending registry generation is invalid");
    }
    Ok(())
}

fn emit_registry_activation(client: &SpiClient<'_>, generation: i64) -> Result<(), spi::Error> {
    if generation <= 0 {
        pgrx::error!("registry generation is invalid");
    }
    let payload = format!(r#"{{"generation":{generation},"action":"activate"}}"#);
    client.select(
        "SELECT pg_catalog.pg_logical_emit_message(
             true,
             'synchro_registry',
             pg_catalog.convert_to($1, 'UTF8')
         )",
        None,
        &[payload.as_str().into()],
    )?;
    Ok(())
}

/// Class 3 generations remain pending until the operator has staged and
/// verified an exported-snapshot projection bootstrap. Other generations keep
/// the normal commit-ordered WAL activation path.
fn emit_registry_activation_when_ready(
    client: &SpiClient<'_>,
    generation: i64,
) -> Result<(), spi::Error> {
    if crate::schema::generation_requires_projection_bootstrap(client, generation)? {
        return Ok(());
    }
    emit_registry_activation(client, generation)
}

fn active_registration_for_logical_name(
    client: &SpiClient<'_>,
    generation: i64,
    table_name: &str,
) -> Result<Option<TableRegistration>, spi::Error> {
    let rows = client.select(
        "SELECT registry_generation,
                relation_id::text AS relation_id,
                registration_kind,
                table_id::text AS table_id,
                primary_key_field_id::text AS primary_key_field_id,
                table_name,
                physical_schema::text AS physical_schema,
                physical_relation::text AS physical_relation,
                physical_relation_oid::bigint AS physical_relation_oid,
                replica_identity::text AS replica_identity,
                composition,
                membership_function_oid::bigint AS membership_function_oid,
                membership_function_schema::text AS membership_function_schema,
                membership_function_name::text AS membership_function_name,
                membership_function_fingerprint,
                max_scope_fanout,
                pk_column,
                pk_type,
                pk_portable_type,
                capture_key_columns,
                updated_at_col,
                deleted_at_col,
                push_policy,
                sync_columns,
                exclude_columns,
                has_updated_at,
                has_deleted_at
          FROM synchro.sync_registry
         WHERE registry_generation = $1
           AND table_name = $2
           AND registration_kind = 'synced'",
        None,
        &[generation.into(), table_name.into()],
    )?;
    let Some(row) = rows.into_iter().next() else {
        return Ok(None);
    };
    let mut registration = registration_from_row(&row)?;
    registration.fields = load_field_registrations(client, generation, &registration.relation_id)?;
    registration.capture_fields =
        load_capture_field_registrations(client, generation, &registration.relation_id)?;
    Ok(Some(registration))
}

fn active_registration_for_unregister(
    client: &SpiClient<'_>,
    generation: i64,
    table_name: &str,
) -> Result<Option<TableRegistration>, spi::Error> {
    let direct = active_registration_for_logical_name(client, generation, table_name)?;
    if direct.is_some() {
        return Ok(direct);
    }

    let input_parts: Option<Vec<String>> = client
        .select(
            "SELECT pg_catalog.parse_ident($1, false)",
            None,
            &[table_name.into()],
        )?
        .first()
        .get_one()?;
    let Some(parts) = input_parts else {
        return Ok(None);
    };
    if parts.len() != 2 {
        return Ok(None);
    }

    let rows = client.select(
        "SELECT registry_generation,
                relation_id::text AS relation_id,
                registration_kind,
                table_id::text AS table_id,
                primary_key_field_id::text AS primary_key_field_id,
                table_name,
                physical_schema::text AS physical_schema,
                physical_relation::text AS physical_relation,
                physical_relation_oid::bigint AS physical_relation_oid,
                replica_identity::text AS replica_identity,
                composition,
                membership_function_oid::bigint AS membership_function_oid,
                membership_function_schema::text AS membership_function_schema,
                membership_function_name::text AS membership_function_name,
                membership_function_fingerprint,
                max_scope_fanout,
                pk_column,
                pk_type,
                pk_portable_type,
                capture_key_columns,
                updated_at_col,
                deleted_at_col,
                push_policy,
                sync_columns,
                exclude_columns,
                has_updated_at,
                has_deleted_at
          FROM synchro.sync_registry
          WHERE registry_generation = $1
            AND physical_schema = $2::name
            AND physical_relation = $3::name
            AND registration_kind = 'synced'",
        None,
        &[
            generation.into(),
            parts[0].as_str().into(),
            parts[1].as_str().into(),
        ],
    )?;
    let Some(row) = rows.into_iter().next() else {
        return Ok(None);
    };
    let mut registration = registration_from_row(&row)?;
    registration.fields = load_field_registrations(client, generation, &registration.relation_id)?;
    registration.capture_fields =
        load_capture_field_registrations(client, generation, &registration.relation_id)?;
    Ok(Some(registration))
}

fn reject_physical_registration_collision(
    client: &SpiClient<'_>,
    generation: i64,
    table_name: &str,
    relation_oid: u32,
) -> Result<(), spi::Error> {
    let collision: bool = client
        .select(
            "SELECT EXISTS (
                 SELECT 1
                  FROM synchro.sync_registry
                 WHERE registry_generation = $1
                   AND physical_relation_oid = $2::oid
                   AND table_name <> $3
             ) AS collision",
            None,
            &[
                generation.into(),
                i64::from(relation_oid).into(),
                table_name.into(),
            ],
        )?
        .first()
        .get_by_name("collision")?
        .unwrap_or(true);
    if collision {
        pgrx::error!("physical relation is already registered under another logical table");
    }
    Ok(())
}

fn new_logical_id(client: &SpiClient<'_>, kind: &str) -> String {
    client
        .select(
            "INSERT INTO synchro.sync_logical_ids (logical_id, kind)
             VALUES (gen_random_uuid(), $1)
             RETURNING logical_id::text AS logical_id",
            None,
            &[kind.into()],
        )
        .unwrap_or_else(|error| pgrx::error!("creating {} ID: {}", kind, error))
        .first()
        .get_by_name::<String, &str>("logical_id")
        .unwrap_or_else(|error| pgrx::error!("reading {} ID: {}", kind, error))
        .unwrap_or_else(|| pgrx::error!("creating {} ID returned no value", kind))
}

fn build_field_registrations(
    client: &SpiClient<'_>,
    relation_oid: u32,
    sync_columns: &[String],
    pk_column: &str,
    updated_at_column: &str,
    deleted_at_column: &str,
    retained: Option<&[FieldRegistration]>,
) -> Result<Vec<FieldRegistration>, spi::Error> {
    let sync_column_refs: Vec<&str> = sync_columns.iter().map(String::as_str).collect();
    let rows = client.select(
        "SELECT a.attname::text AS physical_column,
                pg_catalog.format_type(a.atttypid, a.atttypmod) AS sql_type,
                NOT a.attnotnull AS nullable,
                a.attgenerated::text AS generated
         FROM pg_catalog.pg_attribute a
         WHERE a.attrelid = $1::oid
           AND a.attnum > 0
           AND NOT a.attisdropped
           AND a.attname::text = ANY($2)
         ORDER BY a.attnum",
        None,
        &[i64::from(relation_oid).into(), sync_column_refs.into()],
    )?;
    let retained_by_column: std::collections::HashMap<&str, &FieldRegistration> = retained
        .unwrap_or_default()
        .iter()
        .map(|field| (field.physical_column.as_str(), field))
        .collect();
    let mut fields = Vec::with_capacity(sync_columns.len());
    for row in rows {
        let physical_column = row
            .get_by_name::<String, &str>("physical_column")?
            .unwrap_or_else(|| pgrx::error!("registered field has no physical column"));
        let sql_type = row
            .get_by_name::<String, &str>("sql_type")?
            .unwrap_or_else(|| pgrx::error!("registered field has no SQL type"));
        let portable_type = normalize_portable_type_name(&sql_type)
            .unwrap_or_else(|| {
                pgrx::error!(
                    "registered field {:?} has unsupported type {:?}",
                    physical_column,
                    sql_type
                )
            })
            .to_string();
        let (decimal_precision, decimal_scale) = if portable_type == "decimal" {
            parse_decimal_metadata(&sql_type)
                .unwrap_or_else(|| pgrx::error!("decimal field has no precision and scale"))
        } else {
            (None, None)
        };
        let nullable = row
            .get_by_name::<bool, &str>("nullable")?
            .unwrap_or_else(|| pgrx::error!("registered field has no nullability"));
        let generated = row
            .get_by_name::<String, &str>("generated")?
            .unwrap_or_default();
        let primary_key = physical_column == pk_column;
        let writable = !primary_key
            && physical_column != updated_at_column
            && physical_column != deleted_at_column
            && physical_column != "created_at"
            && generated.is_empty();
        let retained_field = retained_by_column.get(physical_column.as_str());
        fields.push(FieldRegistration {
            field_id: retained_field
                .map(|field| field.field_id.clone())
                .unwrap_or_else(|| new_logical_id(client, "field")),
            physical_column,
            portable_type,
            native_json: matches!(sql_type.as_str(), "json" | "jsonb"),
            decimal_precision,
            decimal_scale,
            nullable,
            writable,
            primary_key,
        });
    }
    if fields.len() != sync_columns.len() {
        pgrx::error!("registered field identity does not cover the synced projection");
    }
    Ok(fields)
}

fn build_capture_field_registrations(
    client: &SpiClient<'_>,
    relation_oid: u32,
    capture_key_columns: &[String],
    captured_columns: &[String],
) -> Result<Vec<CaptureFieldRegistration>, spi::Error> {
    let mut requested = capture_key_columns.to_vec();
    requested.extend(captured_columns.iter().cloned());
    requested.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    requested.dedup();
    if requested.len() != capture_key_columns.len() + captured_columns.len() {
        pgrx::error!("capture dependency fields are duplicated");
    }
    let requested_refs = requested.iter().map(String::as_str).collect::<Vec<_>>();
    let rows = client.select(
        "SELECT attribute.attname::text AS physical_column,
                pg_catalog.format_type(attribute.atttypid, attribute.atttypmod) AS sql_type,
                NOT attribute.attnotnull AS nullable
         FROM pg_catalog.pg_attribute attribute
         WHERE attribute.attrelid = $1::oid
           AND attribute.attnum > 0
           AND NOT attribute.attisdropped
           AND attribute.attname::text = ANY($2)
         ORDER BY attribute.attnum",
        None,
        &[i64::from(relation_oid).into(), requested_refs.into()],
    )?;
    let capture_keys = capture_key_columns
        .iter()
        .map(String::as_str)
        .collect::<std::collections::HashSet<_>>();
    let mut fields = Vec::with_capacity(requested.len());
    for row in rows {
        let physical_column = row
            .get_by_name::<String, &str>("physical_column")?
            .unwrap_or_else(|| pgrx::error!("capture dependency field has no column"));
        let sql_type = row
            .get_by_name::<String, &str>("sql_type")?
            .unwrap_or_else(|| pgrx::error!("capture dependency field has no SQL type"));
        let portable_type = normalize_portable_type_name(&sql_type)
            .unwrap_or_else(|| {
                pgrx::error!(
                    "capture dependency field {:?} has unsupported type {:?}",
                    physical_column,
                    sql_type
                )
            })
            .to_string();
        fields.push(CaptureFieldRegistration {
            capture_key: capture_keys.contains(physical_column.as_str()),
            physical_column,
            portable_type,
            nullable: row
                .get_by_name::<bool, &str>("nullable")?
                .unwrap_or_else(|| pgrx::error!("capture dependency field has no nullability")),
        });
    }
    if fields.len() != requested.len()
        || fields.iter().filter(|field| field.capture_key).count() != capture_key_columns.len()
        || fields
            .iter()
            .any(|field| field.capture_key && field.nullable)
    {
        pgrx::error!("capture dependency projection is incomplete");
    }
    Ok(fields)
}

fn insert_field_registrations(
    client: &mut SpiClient<'_>,
    generation: i64,
    relation_id: &str,
    fields: &[FieldRegistration],
) -> Result<(), spi::Error> {
    for field in fields {
        client.update(
            "INSERT INTO synchro.sync_registry_fields (
                 registry_generation, relation_id, field_id, physical_column,
                 portable_type, native_json, decimal_precision, decimal_scale,
                 nullable, writable, primary_key
             ) VALUES ($1, $2::uuid, $3::uuid, $4, $5, $6, $7, $8, $9, $10, $11)",
            None,
            &[
                generation.into(),
                relation_id.into(),
                field.field_id.as_str().into(),
                field.physical_column.as_str().into(),
                field.portable_type.as_str().into(),
                field.native_json.into(),
                field.decimal_precision.into(),
                field.decimal_scale.into(),
                field.nullable.into(),
                field.writable.into(),
                field.primary_key.into(),
            ],
        )?;
    }
    Ok(())
}

fn insert_capture_field_registrations(
    client: &mut SpiClient<'_>,
    generation: i64,
    relation_id: &str,
    fields: &[CaptureFieldRegistration],
) -> Result<(), spi::Error> {
    for field in fields {
        client.update(
            "INSERT INTO synchro.sync_capture_dependency_fields (
                 registry_generation, relation_id, physical_column,
                 portable_type, nullable, capture_key
             ) VALUES ($1, $2::uuid, $3, $4, $5, $6)",
            None,
            &[
                generation.into(),
                relation_id.into(),
                field.physical_column.as_str().into(),
                field.portable_type.as_str().into(),
                field.nullable.into(),
                field.capture_key.into(),
            ],
        )?;
    }
    Ok(())
}

fn load_field_registrations(
    client: &SpiClient<'_>,
    generation: i64,
    relation_id: &str,
) -> Result<Vec<FieldRegistration>, spi::Error> {
    let rows = client.select(
        "SELECT field_id::text AS field_id,
                physical_column::text AS physical_column,
                portable_type, native_json, decimal_precision, decimal_scale,
                nullable, writable, primary_key
         FROM synchro.sync_registry_fields
         WHERE registry_generation = $1 AND relation_id = $2::uuid
         ORDER BY field_id",
        None,
        &[generation.into(), relation_id.into()],
    )?;
    let mut fields = Vec::new();
    for row in rows {
        fields.push(FieldRegistration {
            field_id: row
                .get_by_name::<String, &str>("field_id")?
                .unwrap_or_else(|| pgrx::error!("registry field has no field ID")),
            physical_column: row
                .get_by_name::<String, &str>("physical_column")?
                .unwrap_or_else(|| pgrx::error!("registry field has no physical column")),
            portable_type: row
                .get_by_name::<String, &str>("portable_type")?
                .unwrap_or_else(|| pgrx::error!("registry field has no portable type")),
            native_json: row
                .get_by_name::<bool, &str>("native_json")?
                .unwrap_or_else(|| pgrx::error!("registry field has no native JSON state")),
            decimal_precision: row.get_by_name::<i32, &str>("decimal_precision")?,
            decimal_scale: row.get_by_name::<i32, &str>("decimal_scale")?,
            nullable: row
                .get_by_name::<bool, &str>("nullable")?
                .unwrap_or_else(|| pgrx::error!("registry field has no nullability")),
            writable: row
                .get_by_name::<bool, &str>("writable")?
                .unwrap_or_else(|| pgrx::error!("registry field has no writable state")),
            primary_key: row
                .get_by_name::<bool, &str>("primary_key")?
                .unwrap_or_else(|| pgrx::error!("registry field has no primary-key state")),
        });
    }
    Ok(fields)
}

fn load_capture_field_registrations(
    client: &SpiClient<'_>,
    generation: i64,
    relation_id: &str,
) -> Result<Vec<CaptureFieldRegistration>, spi::Error> {
    let rows = client.select(
        "SELECT physical_column::text AS physical_column,
                portable_type, nullable, capture_key
         FROM synchro.sync_capture_dependency_fields
         WHERE registry_generation = $1 AND relation_id = $2::uuid
         ORDER BY physical_column",
        None,
        &[generation.into(), relation_id.into()],
    )?;
    let mut fields = Vec::new();
    for row in rows {
        fields.push(CaptureFieldRegistration {
            physical_column: row
                .get_by_name::<String, &str>("physical_column")?
                .unwrap_or_else(|| pgrx::error!("capture dependency field has no column")),
            portable_type: row
                .get_by_name::<String, &str>("portable_type")?
                .unwrap_or_else(|| pgrx::error!("capture dependency field has no type")),
            nullable: row
                .get_by_name::<bool, &str>("nullable")?
                .unwrap_or_else(|| pgrx::error!("capture dependency field has no nullability")),
            capture_key: row
                .get_by_name::<bool, &str>("capture_key")?
                .unwrap_or_else(|| pgrx::error!("capture dependency field has no key state")),
        });
    }
    Ok(fields)
}

fn parse_decimal_metadata(sql_type: &str) -> Option<(Option<i32>, Option<i32>)> {
    let arguments = sql_type
        .trim()
        .strip_prefix("numeric(")
        .or_else(|| sql_type.trim().strip_prefix("decimal("))?
        .strip_suffix(')')?;
    let (precision, scale) = arguments.split_once(',')?;
    let precision = precision.trim().parse::<i32>().ok()?;
    let scale = scale.trim().parse::<i32>().ok()?;
    if precision <= 0 || scale < 0 || scale > precision {
        return None;
    }
    Some((Some(precision), Some(scale)))
}

pub(crate) fn configured_publication_name() -> String {
    crate::PUBLICATION_NAME_GUC
        .get()
        .and_then(|value| value.to_str().ok().map(String::from))
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_PUBLICATION_NAME.to_string())
}

fn ensure_publication_membership(
    client: &mut SpiClient<'_>,
    relation: &PhysicalRelation,
) -> Result<(), spi::Error> {
    let publication = configured_publication_name();
    let publication_rows = client.select(
        "SELECT puballtables FROM pg_catalog.pg_publication WHERE pubname = $1",
        None,
        &[publication.as_str().into()],
    )?;
    if let Some(row) = publication_rows.into_iter().next() {
        let all_tables = row
            .get_by_name::<bool, &str>("puballtables")?
            .unwrap_or(false);
        if all_tables {
            pgrx::error!("configured publication must not use FOR ALL TABLES");
        }
    } else {
        let create_sql = format!(
            "CREATE PUBLICATION {} FOR TABLE {}",
            crate::pull::pg_quote_ident(&publication),
            qualified_relation_name(&relation.schema, &relation.relation),
        );
        client.update(&create_sql, None, &[])?;
        return Ok(());
    }

    if !publication_contains_relation(client, &publication, relation.oid)? {
        let add_sql = format!(
            "ALTER PUBLICATION {} ADD TABLE {}",
            crate::pull::pg_quote_ident(&publication),
            qualified_relation_name(&relation.schema, &relation.relation),
        );
        client.update(&add_sql, None, &[])?;
    }
    Ok(())
}

fn remove_capture_configuration(
    client: &mut SpiClient<'_>,
    registration: &ExistingRegistration,
) -> Result<(), spi::Error> {
    let publication = configured_publication_name();
    if !publication_exists(client, &publication)?
        || publication_is_for_all_tables(client, &publication)?
        || !publication_contains_relation(client, &publication, registration.physical_relation_oid)?
    {
        pgrx::error!("registered relation is not an exact publication member");
    }
    if !capture_trigger_names_match(
        client,
        registration.physical_relation_oid,
        &registration.relation_id,
        &registration.table_id,
        &registration.pk_column,
    )? {
        pgrx::error!("registered relation has incompatible capture triggers");
    }

    let relation_name = qualified_relation_name(
        &registration.physical_schema,
        &registration.physical_relation,
    );
    let drop_publication_sql = format!(
        "ALTER PUBLICATION {} DROP TABLE {}",
        crate::pull::pg_quote_ident(&publication),
        relation_name,
    );
    client.update(&drop_publication_sql, None, &[])?;
    for trigger in [
        PRIMARY_KEY_GUARD_TRIGGER,
        CAPTURE_FENCE_TRIGGER,
        CAPTURE_TRUNCATE_TRIGGER,
    ] {
        let drop_trigger_sql = format!(
            "DROP TRIGGER {} ON {}",
            crate::pull::pg_quote_ident(trigger),
            relation_name,
        );
        client.update(&drop_trigger_sql, None, &[])?;
    }
    Ok(())
}

pub(crate) fn remove_retired_capture_configuration(
    client: &mut SpiClient<'_>,
    source_generation: i64,
    target_generation: i64,
) -> Result<(), spi::Error> {
    let transition_valid = client
        .select(
            "SELECT EXISTS (
                 SELECT 1
                 FROM synchro.sync_registry_generations source
                 JOIN synchro.sync_registry_generations target
                   ON target.stream_generation = source.stream_generation
                 WHERE source.generation = $1
                   AND source.state = 'active'
                   AND source.validated
                   AND target.generation = $2
                   AND target.state = 'pending'
                   AND target.validated
             ) AS valid",
            None,
            &[source_generation.into(), target_generation.into()],
        )?
        .first()
        .get_by_name::<bool, &str>("valid")?
        .unwrap_or(false);
    if !transition_valid {
        pgrx::error!("capture configuration transition is invalid");
    }
    let rows = client.select(
        "SELECT source.relation_id::text AS relation_id,
                source.table_id::text AS table_id,
                source.physical_schema::text AS physical_schema,
                source.physical_relation::text AS physical_relation,
                source.physical_relation_oid::bigint AS physical_relation_oid,
                source.pk_column,
                relation.relowner::bigint AS relation_owner
         FROM synchro.sync_registry source
         LEFT JOIN pg_catalog.pg_class relation
           ON relation.oid = source.physical_relation_oid
         WHERE source.registry_generation = $1
           AND source.registration_kind = 'synced'
           AND NOT EXISTS (
               SELECT 1
               FROM synchro.sync_registry target
               WHERE target.registry_generation = $2
                 AND target.relation_id = source.relation_id
           )
         ORDER BY source.relation_id",
        None,
        &[source_generation.into(), target_generation.into()],
    )?;
    for row in rows {
        let relation_id = row
            .get_by_name::<String, &str>("relation_id")?
            .unwrap_or_else(|| pgrx::error!("retired registration has no relation identity"));
        let registration = ExistingRegistration {
            relation_id: relation_id.clone(),
            table_id: row
                .get_by_name::<String, &str>("table_id")?
                .unwrap_or_else(|| pgrx::error!("retired registration has no table identity")),
            physical_schema: row
                .get_by_name::<String, &str>("physical_schema")?
                .unwrap_or_else(|| pgrx::error!("retired registration has no physical schema")),
            physical_relation: row
                .get_by_name::<String, &str>("physical_relation")?
                .unwrap_or_else(|| pgrx::error!("retired registration has no physical relation")),
            physical_relation_oid: row
                .get_by_name::<i64, &str>("physical_relation_oid")?
                .map(checked_oid)
                .unwrap_or_else(|| pgrx::error!("retired registration has no relation OID")),
            pk_column: row
                .get_by_name::<String, &str>("pk_column")?
                .unwrap_or_else(|| pgrx::error!("retired registration has no primary key")),
        };
        let owner = row
            .get_by_name::<i64, &str>("relation_owner")?
            .map(checked_oid)
            .map(pg_sys::Oid::from)
            .unwrap_or_else(|| pgrx::error!("retired registration relation no longer exists"));
        with_registration_actor_ddl(
            owner,
            std::panic::AssertUnwindSafe(|| remove_capture_configuration(client, &registration)),
        )?;
    }
    Ok(())
}

fn capture_trigger_names_match(
    client: &SpiClient<'_>,
    relation_oid: u32,
    relation_id: &str,
    table_id: &str,
    primary_key_column: &str,
) -> Result<bool, spi::Error> {
    let primary_key_argument = format!("'{}'", primary_key_column);
    let relation_argument = format!("'{}'", relation_id);
    let capture_key_argument = format!(
        "'{}'",
        serde_json::to_string(&vec![primary_key_column])
            .unwrap_or_else(|_| pgrx::error!("encoding capture key metadata failed"))
    );
    let valid: bool = client
        .select(
            "SELECT count(*) = 3
                    AND count(*) FILTER (
                        WHERE tg.tgname = $2
                          AND pn.nspname = 'synchro'
                          AND p.proname = $2
                          AND tg.tgenabled = 'O'
                          AND tg.tgtype = 19
                          AND tg.tgnargs = 1
                          AND pg_catalog.pg_get_triggerdef(tg.oid, true) LIKE '%' || $4 || '%'
                    ) = 1
                    AND count(*) FILTER (
                        WHERE tg.tgname = $3
                          AND pn.nspname = 'synchro'
                          AND p.proname = $3
                          AND tg.tgenabled = 'O'
                          AND tg.tgtype = 29
                          AND tg.tgnargs = 5
                          AND pg_catalog.pg_get_triggerdef(tg.oid, true) LIKE '%' || $5 || '%'
                          AND pg_catalog.pg_get_triggerdef(tg.oid, true) LIKE '%''synced''%'
                          AND pg_catalog.pg_get_triggerdef(tg.oid, true) LIKE '%' || $8 || '%'
                          AND pg_catalog.pg_get_triggerdef(tg.oid, true) LIKE '%' || $9 || '%'
                    ) = 1
                    AND count(*) FILTER (
                        WHERE tg.tgname = $7
                          AND pn.nspname = 'synchro'
                          AND p.proname = $7
                          AND tg.tgenabled = 'O'
                          AND tg.tgtype = 34
                          AND tg.tgnargs = 1
                          AND pg_catalog.pg_get_triggerdef(tg.oid, true) LIKE '%' || $5 || '%'
                    ) = 1 AS valid
             FROM pg_catalog.pg_trigger tg
             JOIN pg_catalog.pg_proc p ON p.oid = tg.tgfoid
             JOIN pg_catalog.pg_namespace pn ON pn.oid = p.pronamespace
             WHERE tg.tgrelid = $1::oid
               AND NOT tg.tgisinternal
               AND tg.tgname = ANY($6)",
            None,
            &[
                i64::from(relation_oid).into(),
                PRIMARY_KEY_GUARD_TRIGGER.into(),
                CAPTURE_FENCE_TRIGGER.into(),
                primary_key_argument.as_str().into(),
                relation_argument.as_str().into(),
                vec![
                    PRIMARY_KEY_GUARD_TRIGGER.to_string(),
                    CAPTURE_FENCE_TRIGGER.to_string(),
                    CAPTURE_TRUNCATE_TRIGGER.to_string(),
                ]
                .into(),
                CAPTURE_TRUNCATE_TRIGGER.into(),
                format!("'{}'", table_id).into(),
                capture_key_argument.as_str().into(),
            ],
        )?
        .first()
        .get_by_name("valid")?
        .unwrap_or(false);
    Ok(valid)
}

pub(crate) fn publication_exists(
    client: &SpiClient<'_>,
    publication: &str,
) -> Result<bool, spi::Error> {
    Ok(client
        .select(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = $1) AS exists",
            None,
            &[publication.into()],
        )?
        .first()
        .get_by_name("exists")?
        .unwrap_or(false))
}

pub(crate) fn publication_is_for_all_tables(
    client: &SpiClient<'_>,
    publication: &str,
) -> Result<bool, spi::Error> {
    Ok(client
        .select(
            "SELECT puballtables FROM pg_catalog.pg_publication WHERE pubname = $1",
            None,
            &[publication.into()],
        )?
        .first()
        .get_by_name("puballtables")?
        .unwrap_or(false))
}

pub(crate) fn publication_contains_relation(
    client: &SpiClient<'_>,
    publication: &str,
    relation_oid: u32,
) -> Result<bool, spi::Error> {
    Ok(client
        .select(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_publication p
                 JOIN pg_catalog.pg_publication_rel pr ON pr.prpubid = p.oid
                 WHERE p.pubname = $1 AND pr.prrelid = $2::oid
             ) AS contains_relation",
            None,
            &[publication.into(), i64::from(relation_oid).into()],
        )?
        .first()
        .get_by_name("contains_relation")?
        .unwrap_or(false))
}

pub(crate) fn qualified_relation_name(schema: &str, relation: &str) -> String {
    format!(
        "{}.{}",
        crate::pull::pg_quote_ident(schema),
        crate::pull::pg_quote_ident(relation),
    )
}

fn install_capture_triggers(
    client: &mut SpiClient<'_>,
    registration: &TableRegistration,
) -> Result<(), spi::Error> {
    if capture_triggers_match(client, registration)? {
        return Ok(());
    }

    if capture_trigger_names_exist(client, registration.physical_relation_oid)? {
        pgrx::error!("registered relation has incompatible capture triggers");
    }

    let guard_sql = client
        .select(
            "SELECT pg_catalog.format(
                 'CREATE TRIGGER %I BEFORE UPDATE ON %I.%I FOR EACH ROW EXECUTE FUNCTION synchro.%I(%L)',
                 $1, $2, $3, $4, $5
             ) AS sql",
            None,
            &[
                PRIMARY_KEY_GUARD_TRIGGER.into(),
                registration.physical_schema.as_str().into(),
                registration.physical_relation.as_str().into(),
                PRIMARY_KEY_GUARD_TRIGGER.into(),
                registration.pk_column.as_str().into(),
            ],
        )?
        .first()
        .get_by_name::<String, &str>("sql")?
        .unwrap_or_else(|| pgrx::error!("building primary-key guard trigger DDL failed"));
    client.update(&guard_sql, None, &[])?;

    let fence_sql = client
        .select(
            "SELECT pg_catalog.format(
                 'CREATE TRIGGER %I AFTER INSERT OR UPDATE OR DELETE ON %I.%I FOR EACH ROW EXECUTE FUNCTION synchro.%I(%L, %L, %L, %L, %L)',
                  $1, $2, $3, $4, $5, $6, $7, $8, $9
             ) AS sql",
            None,
            &[
                CAPTURE_FENCE_TRIGGER.into(),
                registration.physical_schema.as_str().into(),
                registration.physical_relation.as_str().into(),
                CAPTURE_FENCE_TRIGGER.into(),
                registration.relation_id.as_str().into(),
                registration.registration_kind.as_str().into(),
                if registration.is_synced() {
                    registration.table_id.as_str().into()
                } else {
                    "".into()
                },
                serde_json::to_string(&registration.capture_key_columns)
                    .unwrap_or_else(|_| pgrx::error!("encoding capture key metadata failed"))
                    .into(),
                if registration.is_synced() && registration.has_deleted_at {
                    registration.deleted_at_col.as_str().into()
                } else {
                    "".into()
                },
            ],
        )?
        .first()
        .get_by_name::<String, &str>("sql")?
        .unwrap_or_else(|| pgrx::error!("building capture fence trigger DDL failed"));
    client.update(&fence_sql, None, &[])?;

    let truncate_sql = client
        .select(
            "SELECT pg_catalog.format(
                 'CREATE TRIGGER %I BEFORE TRUNCATE ON %I.%I FOR EACH STATEMENT EXECUTE FUNCTION synchro.%I(%L)',
                  $1, $2, $3, $4, $5
             ) AS sql",
            None,
            &[
                CAPTURE_TRUNCATE_TRIGGER.into(),
                registration.physical_schema.as_str().into(),
                registration.physical_relation.as_str().into(),
                CAPTURE_TRUNCATE_TRIGGER.into(),
                registration.relation_id.as_str().into(),
            ],
        )?
        .first()
        .get_by_name::<String, &str>("sql")?
        .unwrap_or_else(|| pgrx::error!("building truncate guard trigger DDL failed"));
    client.update(&truncate_sql, None, &[])?;

    let relation_name = qualified_relation_name(
        &registration.physical_schema,
        &registration.physical_relation,
    );
    for trigger in [
        PRIMARY_KEY_GUARD_TRIGGER,
        CAPTURE_FENCE_TRIGGER,
        CAPTURE_TRUNCATE_TRIGGER,
    ] {
        let enable_sql = format!(
            "ALTER TABLE {} ENABLE TRIGGER {}",
            relation_name,
            crate::pull::pg_quote_ident(trigger),
        );
        client.update(&enable_sql, None, &[])?;
    }
    Ok(())
}

fn capture_trigger_names_exist(
    client: &SpiClient<'_>,
    relation_oid: u32,
) -> Result<bool, spi::Error> {
    Ok(client
        .select(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_trigger tg
                 WHERE tg.tgrelid = $1::oid
                   AND NOT tg.tgisinternal
                   AND tg.tgname = ANY($2)
             ) AS exists",
            None,
            &[
                i64::from(relation_oid).into(),
                vec![
                    PRIMARY_KEY_GUARD_TRIGGER.to_string(),
                    CAPTURE_FENCE_TRIGGER.to_string(),
                    CAPTURE_TRUNCATE_TRIGGER.to_string(),
                ]
                .into(),
            ],
        )?
        .first()
        .get_by_name("exists")?
        .unwrap_or(false))
}

fn validate_generation_entries(
    client: &mut SpiClient<'_>,
    generation: i64,
) -> Result<(), spi::Error> {
    carry_pending_membership_stage(client, generation)?;
    validate_generation_identity(client, generation, false)?;
    let rows = client.select(
        "SELECT registry_generation,
                 relation_id::text AS relation_id,
                 registration_kind,
                 table_id::text AS table_id,
                primary_key_field_id::text AS primary_key_field_id,
                table_name,
                physical_schema::text AS physical_schema,
                physical_relation::text AS physical_relation,
                physical_relation_oid::bigint AS physical_relation_oid,
                replica_identity::text AS replica_identity,
                composition,
                 membership_function_oid::bigint AS membership_function_oid,
                 membership_function_schema::text AS membership_function_schema,
                 membership_function_name::text AS membership_function_name,
                 membership_function_fingerprint,
                 max_scope_fanout,
                 pk_column,
                 pk_type,
                 pk_portable_type,
                 capture_key_columns,
                updated_at_col,
                deleted_at_col,
                push_policy,
                sync_columns,
                exclude_columns,
                has_updated_at,
                has_deleted_at
         FROM synchro.sync_registry
         WHERE registry_generation = $1
         ORDER BY table_name",
        None,
        &[generation.into()],
    )?;
    let mut registrations = Vec::new();
    for row in rows {
        let mut registration = registration_from_row(&row)?;
        registration.fields = load_field_registrations(
            client,
            registration.registry_generation,
            &registration.relation_id,
        )?;
        registration.capture_fields = load_capture_field_registrations(
            client,
            registration.registry_generation,
            &registration.relation_id,
        )?;
        if registration.registry_generation != generation {
            pgrx::error!("registry generation contains an invalid entry");
        }
        validate_registration_metadata(client, &registration)?;
        registrations.push(registration);
    }
    load_membership_dependencies_from_client(client, generation, &registrations)?;
    Ok(())
}

fn carry_pending_membership_stage(
    client: &mut SpiClient<'_>,
    generation: i64,
) -> Result<(), spi::Error> {
    client.update(
        "WITH lineage AS (
             SELECT parent_generation
             FROM synchro.sync_registry_generations
             WHERE generation = $1 AND state = 'pending'
         ), candidate_targets AS (
             SELECT DISTINCT target_relation_id
             FROM lineage
             JOIN synchro.sync_registry_membership_stages stage
               ON stage.registry_generation IN ($1, lineage.parent_generation)
              AND stage.state = 'pending'
             CROSS JOIN LATERAL unnest(stage.target_relation_ids) target(target_relation_id)
             JOIN synchro.sync_registry registry
               ON registry.registry_generation = $1
              AND registry.relation_id = target.target_relation_id
              AND registry.registration_kind = 'synced'
         )
         INSERT INTO synchro.sync_registry_membership_stages (
             registry_generation, source_registry_generation,
             target_relation_ids, state
         )
         SELECT $1, lineage.parent_generation,
                array_agg(candidate_targets.target_relation_id ORDER BY candidate_targets.target_relation_id),
                'pending'
         FROM lineage
         JOIN candidate_targets ON true
         GROUP BY lineage.parent_generation
         ON CONFLICT (registry_generation) DO UPDATE
         SET source_registry_generation = EXCLUDED.source_registry_generation,
             target_relation_ids = EXCLUDED.target_relation_ids
         WHERE synchro.sync_registry_membership_stages.state = 'pending'",
        None,
        &[generation.into()],
    )?;
    client.update(
        "DELETE FROM synchro.sync_registry_membership_stages stage
         USING synchro.sync_registry_generations generation
         WHERE generation.generation = $1
           AND stage.registry_generation = generation.parent_generation
           AND stage.state = 'pending'",
        None,
        &[generation.into()],
    )?;
    Ok(())
}

/// Load the one active, complete registry generation.
#[cfg(any(test, feature = "pg_test"))]
pub fn load_registry() -> Result<Vec<TableRegistration>, spi::Error> {
    Spi::connect(load_registry_from_client)
}

/// Load and revalidate the active registry generation in an existing SPI context.
///
/// This function fails closed if a catalog object changed after registration or
/// if persisted generation metadata is incomplete.
pub(crate) fn load_registry_from_client(
    client: &SpiClient<'_>,
) -> Result<Vec<TableRegistration>, spi::Error> {
    let active_generation = active_generation_for_load(client)?;
    Ok(
        load_registry_generation_from_client(client, active_generation)?
            .into_iter()
            .filter(TableRegistration::is_synced)
            .collect(),
    )
}

/// Load and revalidate one complete registry generation selected by the worker.
pub(crate) fn load_registry_generation_from_client(
    client: &SpiClient<'_>,
    generation: i64,
) -> Result<Vec<TableRegistration>, spi::Error> {
    load_registry_generation_entries(client, generation, true)
}

/// Load prior metadata for the transaction that activates a validated generation.
/// The final generation validates current capture controls before prior controls are ignored.
pub(crate) fn load_registry_generation_for_activation(
    client: &SpiClient<'_>,
    active_generation: i64,
    final_generation: i64,
) -> Result<Vec<TableRegistration>, spi::Error> {
    load_registry_generation_entries(client, final_generation, true)?;
    load_registry_generation_entries(client, active_generation, false)
}

/// Load the active decoder registry after a committed registration transaction.
/// A validated pending generation proves the current physical shape before the
/// worker reads the commit-ordered activation message.
pub(crate) fn load_registry_generation_for_worker(
    client: &SpiClient<'_>,
    active_generation: i64,
) -> Result<Vec<TableRegistration>, spi::Error> {
    let pending_rows = client.select(
        "SELECT pending.generation
             FROM synchro.sync_registry_generations active
             JOIN synchro.sync_registry_generations pending
               ON pending.stream_generation = active.stream_generation
              AND pending.generation > active.generation
              AND pending.state = 'pending'
              AND pending.validated
             WHERE active.generation = $1
               AND active.state = 'active'
               AND active.validated
             ORDER BY pending.generation DESC
             LIMIT 1",
        None,
        &[active_generation.into()],
    )?;
    let pending = match pending_rows.into_iter().next() {
        Some(row) => row.get_by_name::<i64, &str>("generation")?,
        None => None,
    };
    match pending {
        Some(final_generation) => {
            load_registry_generation_for_activation(client, active_generation, final_generation)
        }
        None => load_registry_generation_from_client(client, active_generation),
    }
}

fn load_registry_generation_entries(
    client: &SpiClient<'_>,
    generation: i64,
    validate_capture_controls: bool,
) -> Result<Vec<TableRegistration>, spi::Error> {
    validate_complete_generation(client, generation)?;
    let rows = client.select(
        "SELECT registry_generation,
                 relation_id::text AS relation_id,
                 registration_kind,
                 table_id::text AS table_id,
                primary_key_field_id::text AS primary_key_field_id,
                table_name,
                physical_schema::text AS physical_schema,
                physical_relation::text AS physical_relation,
                physical_relation_oid::bigint AS physical_relation_oid,
                replica_identity::text AS replica_identity,
                composition,
                 membership_function_oid::bigint AS membership_function_oid,
                 membership_function_schema::text AS membership_function_schema,
                 membership_function_name::text AS membership_function_name,
                 membership_function_fingerprint,
                 max_scope_fanout,
                 pk_column,
                 pk_type,
                 pk_portable_type,
                 capture_key_columns,
                updated_at_col,
                deleted_at_col,
                push_policy,
                sync_columns,
                exclude_columns,
                has_updated_at,
                has_deleted_at
         FROM synchro.sync_registry
         WHERE registry_generation = $1
         ORDER BY table_name",
        None,
        &[generation.into()],
    )?;
    let mut registrations = Vec::new();
    for row in rows {
        let mut registration = registration_from_row(&row)?;
        registration.fields = load_field_registrations(
            client,
            registration.registry_generation,
            &registration.relation_id,
        )?;
        registration.capture_fields = load_capture_field_registrations(
            client,
            registration.registry_generation,
            &registration.relation_id,
        )?;
        if registration.registry_generation != generation {
            pgrx::error!("registry entry belongs to another generation");
        }
        if validate_capture_controls {
            validate_loaded_registration(client, &registration)?;
        } else {
            validate_persisted_registration_metadata(client, &registration)?;
        }
        registrations.push(registration);
    }
    if validate_capture_controls {
        let dependencies =
            load_membership_dependencies_from_client(client, generation, &registrations)?;
        validate_generation_function_projections(client, &registrations, &dependencies)?;
    }
    Ok(registrations)
}

fn validate_persisted_registration_metadata(
    client: &SpiClient<'_>,
    registration: &TableRegistration,
) -> Result<(), spi::Error> {
    if registration.is_capture_dependency() {
        if registration.relation_id.is_empty()
            || registration.table_name.trim().is_empty()
            || registration.pk_column.trim().is_empty()
            || registration.pk_type.trim().is_empty()
            || registration.registry_generation <= 0
            || !registration.table_id.is_empty()
            || !registration.primary_key_field_id.is_empty()
            || registration.membership_function.oid != 0
            || registration.max_scope_fanout != 0
            || !registration.fields.is_empty()
            || registration.capture_fields.is_empty()
            || registration.capture_key_columns.len() != 1
            || registration.capture_key_columns[0] != registration.pk_column
        {
            pgrx::error!("capture dependency registry metadata is incomplete");
        }
        let relation_identity_valid = client
            .select(
                "SELECT EXISTS (
                     SELECT 1 FROM synchro.sync_logical_ids
                     WHERE logical_id = $1::uuid AND kind = 'relation'
                 ) AS valid",
                None,
                &[registration.relation_id.as_str().into()],
            )?
            .first()
            .get_by_name::<bool, &str>("valid")?
            .unwrap_or(false);
        if !relation_identity_valid {
            pgrx::error!("capture dependency relation identity is invalid");
        }
        return Ok(());
    }

    if registration.relation_id.is_empty()
        || registration.table_id.is_empty()
        || registration.primary_key_field_id.is_empty()
        || registration.table_name.trim().is_empty()
        || registration.membership_function.schema.is_empty()
        || registration.membership_function.name.is_empty()
        || registration.membership_function.oid == 0
        || registration.max_scope_fanout <= 0
        || registration.pk_column.trim().is_empty()
        || registration.pk_type.trim().is_empty()
        || registration.registry_generation <= 0
    {
        pgrx::error!("registry metadata is incomplete");
    }
    let primary_key_fields: Vec<&FieldRegistration> = registration
        .fields
        .iter()
        .filter(|field| field.primary_key)
        .collect();
    if primary_key_fields.len() != 1
        || primary_key_fields[0].field_id != registration.primary_key_field_id
        || primary_key_fields[0].physical_column != registration.pk_column
        || primary_key_fields[0].portable_type != registration.pk_portable_type
        || primary_key_fields[0].writable
    {
        pgrx::error!("registry primary key field identity is invalid");
    }
    if registration.replica_identity != "d" {
        pgrx::error!("registry replica identity is invalid");
    }
    if !matches!(
        registration.pk_portable_type.as_str(),
        "string" | "int" | "int64"
    ) {
        pgrx::error!("registry portable primary key type is invalid");
    }
    let stored_field_columns: std::collections::HashSet<&str> = registration
        .fields
        .iter()
        .map(|field| field.physical_column.as_str())
        .collect();
    let synced_columns: std::collections::HashSet<&str> = registration
        .sync_columns
        .iter()
        .map(String::as_str)
        .collect();
    if stored_field_columns != synced_columns {
        pgrx::error!("registered field identity does not cover the synced projection");
    }
    validate_lifecycle_fields(registration)?;
    validate_logical_id_kinds(client, registration)
}

fn validate_lifecycle_fields(registration: &TableRegistration) -> Result<(), spi::Error> {
    for lifecycle_column in [
        Some("created_at"),
        registration
            .has_updated_at
            .then_some(registration.updated_at_col.as_str()),
        registration
            .has_deleted_at
            .then_some(registration.deleted_at_col.as_str()),
    ]
    .into_iter()
    .flatten()
    {
        if let Some(field) = registration
            .fields
            .iter()
            .find(|field| field.physical_column == lifecycle_column)
        {
            if field.portable_type != "datetime" || field.writable {
                pgrx::error!("registered lifecycle field is invalid");
            }
        }
    }
    Ok(())
}

fn validate_generation_function_projections(
    client: &SpiClient<'_>,
    registrations: &[TableRegistration],
    dependencies: &[MembershipDependency],
) -> Result<(), spi::Error> {
    for target in registrations
        .iter()
        .filter(|registration| registration.is_synced())
    {
        #[cfg(feature = "pg_test")]
        if target.membership_function.schema == "tests" {
            continue;
        }
        for (physical_oid, columns) in
            function_projection_dependencies(client, target.membership_function.oid)?
        {
            let source = registrations
                .iter()
                .find(|registration| registration.physical_relation_oid == physical_oid)
                .unwrap_or_else(|| {
                    pgrx::error!("membership function projection is not registered")
                });
            let available = projected_registration_columns(source);
            if columns
                .iter()
                .any(|column| !available.contains(column.as_str()))
            {
                pgrx::error!("membership function reads an undeclared projection field");
            }
            if source.relation_id != target.relation_id {
                let dependency = dependencies
                    .iter()
                    .find(|dependency| {
                        dependency.dependency_relation_id == source.relation_id
                            && dependency.target_relation_id == target.relation_id
                    })
                    .unwrap_or_else(|| {
                        pgrx::error!("membership function has no declared impact dependency")
                    });
                let declared: std::collections::HashSet<&str> = dependency
                    .dependency_columns
                    .iter()
                    .map(String::as_str)
                    .collect();
                if columns
                    .iter()
                    .any(|column| !declared.contains(column.as_str()))
                {
                    pgrx::error!("membership function dependency fields are incomplete");
                }
            }
        }
    }

    for dependency in dependencies {
        let source = registrations
            .iter()
            .find(|registration| registration.relation_id == dependency.dependency_relation_id)
            .expect("validated dependency source");
        let target = registrations
            .iter()
            .find(|registration| registration.relation_id == dependency.target_relation_id)
            .expect("validated dependency target");
        for (physical_oid, columns) in
            function_projection_dependencies(client, dependency.impact_function.oid)?
        {
            let registration = if physical_oid == source.physical_relation_oid {
                source
            } else if physical_oid == target.physical_relation_oid {
                target
            } else {
                pgrx::error!("impact function reads outside its declared relations");
            };
            let available = projected_registration_columns(registration);
            if columns
                .iter()
                .any(|column| !available.contains(column.as_str()))
            {
                pgrx::error!("impact function reads an undeclared projection field");
            }
            if physical_oid == source.physical_relation_oid {
                let declared: std::collections::HashSet<&str> = dependency
                    .dependency_columns
                    .iter()
                    .map(String::as_str)
                    .collect();
                if columns
                    .iter()
                    .any(|column| !declared.contains(column.as_str()))
                {
                    pgrx::error!("impact function dependency fields are incomplete");
                }
            }
        }
    }
    Ok(())
}

fn projected_registration_columns(
    registration: &TableRegistration,
) -> std::collections::HashSet<&str> {
    if registration.is_synced() {
        registration
            .fields
            .iter()
            .map(|field| field.physical_column.as_str())
            .collect()
    } else {
        registration
            .capture_fields
            .iter()
            .map(|field| field.physical_column.as_str())
            .collect()
    }
}

fn function_projection_dependencies(
    client: &SpiClient<'_>,
    function_oid: u32,
) -> Result<Vec<(u32, Vec<String>)>, spi::Error> {
    let rows = client.select(
        "SELECT projection.physical_relation_oid::bigint AS physical_relation_oid,
                COALESCE(
                    array_agg(DISTINCT attribute.attname::text)
                        FILTER (
                            WHERE attribute.attname IS NOT NULL
                              AND attribute.attname NOT IN ('record_id', 'capture_key', 'deleted')
                        ),
                    '{}'::text[]
                ) AS columns
         FROM pg_catalog.pg_depend dependency
         JOIN synchro.sync_projection_views projection
           ON dependency.refclassid = 'pg_catalog.pg_class'::regclass
          AND projection.view_oid = dependency.refobjid
         LEFT JOIN pg_catalog.pg_attribute attribute
           ON attribute.attrelid = projection.view_oid
          AND attribute.attnum = dependency.refobjsubid
         WHERE dependency.classid = 'pg_catalog.pg_proc'::regclass
           AND dependency.objid = $1::oid
           AND dependency.deptype = 'n'
         GROUP BY projection.physical_relation_oid
         ORDER BY projection.physical_relation_oid",
        None,
        &[i64::from(function_oid).into()],
    )?;
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        let oid = row
            .get_by_name::<i64, &str>("physical_relation_oid")?
            .map(checked_oid)
            .unwrap_or_else(|| pgrx::error!("projection dependency has no relation identity"));
        let columns = row
            .get_by_name::<Vec<String>, &str>("columns")?
            .unwrap_or_default();
        result.push((oid, columns));
    }
    Ok(result)
}

/// Load the registered impact declarations for one complete generation.
///
/// Each declaration links one captured dependency relation to one synced target
/// relation. The worker uses this metadata after source projections are final.
pub(crate) fn load_membership_dependencies_from_client(
    client: &SpiClient<'_>,
    generation: i64,
    registrations: &[TableRegistration],
) -> Result<Vec<MembershipDependency>, spi::Error> {
    let rows = client.select(
        "SELECT dependency_id::text AS dependency_id,
                dependency_relation_id::text AS dependency_relation_id,
                dependency_registration_kind,
                target_relation_id::text AS target_relation_id,
                target.table_id::text AS target_table_id,
                impact_function_oid::bigint AS impact_function_oid,
                impact_function_schema::text AS impact_function_schema,
                impact_function_name::text AS impact_function_name,
                impact_function_fingerprint,
                max_impact_rows,
                dependency_field_ids::text[] AS dependency_field_ids,
                dependency_columns::text[] AS dependency_columns
         FROM synchro.sync_membership_dependencies dependency
         JOIN synchro.sync_registry target
           ON target.registry_generation = dependency.registry_generation
          AND target.relation_id = dependency.target_relation_id
         WHERE dependency.registry_generation = $1
         ORDER BY dependency_id",
        None,
        &[generation.into()],
    )?;
    let mut dependencies = Vec::new();
    let mut identities = std::collections::HashSet::new();
    let mut edges = std::collections::HashSet::new();
    for row in rows {
        let dependency_id = row
            .get_by_name::<String, &str>("dependency_id")?
            .unwrap_or_else(|| pgrx::error!("membership dependency has no identity"));
        let dependency_relation_id = row
            .get_by_name::<String, &str>("dependency_relation_id")?
            .unwrap_or_else(|| pgrx::error!("membership dependency has no source relation"));
        let target_relation_id = row
            .get_by_name::<String, &str>("target_relation_id")?
            .unwrap_or_else(|| pgrx::error!("membership dependency has no target relation"));
        let dependency_registration_kind = row
            .get_by_name::<String, &str>("dependency_registration_kind")?
            .as_deref()
            .and_then(RegistrationKind::parse)
            .unwrap_or_else(|| pgrx::error!("membership dependency has invalid source kind"));
        let target_table_id = row
            .get_by_name::<String, &str>("target_table_id")?
            .unwrap_or_else(|| pgrx::error!("membership dependency target has no table ID"));
        let impact_function = RegisteredFunction {
            oid: row
                .get_by_name::<i64, &str>("impact_function_oid")?
                .map(checked_oid)
                .unwrap_or_else(|| {
                    pgrx::error!("membership dependency has no impact function OID")
                }),
            schema: row
                .get_by_name::<String, &str>("impact_function_schema")?
                .unwrap_or_else(|| {
                    pgrx::error!("membership dependency has no impact function schema")
                }),
            name: row
                .get_by_name::<String, &str>("impact_function_name")?
                .unwrap_or_else(|| {
                    pgrx::error!("membership dependency has no impact function name")
                }),
        };
        let max_impact_rows = row
            .get_by_name::<i32, &str>("max_impact_rows")?
            .unwrap_or_else(|| pgrx::error!("membership dependency has no impact row limit"));
        let impact_function_fingerprint = row
            .get_by_name::<Vec<u8>, &str>("impact_function_fingerprint")?
            .unwrap_or_default();
        let dependency_field_ids = row
            .get_by_name::<Vec<String>, &str>("dependency_field_ids")?
            .unwrap_or_else(|| pgrx::error!("membership dependency has no captured fields"));
        let dependency_columns = row
            .get_by_name::<Vec<String>, &str>("dependency_columns")?
            .unwrap_or_else(|| pgrx::error!("membership dependency has no captured columns"));
        let Some(dependency_registration) = registrations
            .iter()
            .find(|registration| registration.relation_id == dependency_relation_id)
        else {
            pgrx::error!("membership dependency source relation is not registered");
        };
        let Some(target_registration) = registrations
            .iter()
            .find(|registration| registration.relation_id == target_relation_id)
        else {
            pgrx::error!("membership dependency target relation is not registered");
        };
        if dependency_relation_id == target_relation_id
            || dependency_registration_kind != dependency_registration.registration_kind
            || !target_registration.is_synced()
            || target_table_id != target_registration.table_id
            || dependency_columns.is_empty()
            || !identities.insert(dependency_id.clone())
            || !edges.insert((dependency_relation_id.clone(), target_relation_id.clone()))
        {
            pgrx::error!("membership dependency metadata is invalid");
        }
        if dependency_registration.is_synced() {
            validate_application_ownership(
                client,
                dependency_registration.physical_relation_oid,
                dependency_registration.membership_function.oid,
            )?;
        } else {
            validate_capture_application_ownership(
                client,
                dependency_registration.physical_relation_oid,
            )?;
        }
        validate_application_ownership(
            client,
            target_registration.physical_relation_oid,
            target_registration.membership_function.oid,
        )?;
        validate_dependency_application_ownership(
            client,
            dependency_registration.physical_relation_oid,
            target_registration.physical_relation_oid,
            &impact_function,
        )?;
        let (validated_field_ids, validated_columns) = validate_declared_dependency_fields(
            dependency_registration,
            if dependency_registration.is_synced() {
                &dependency_field_ids
            } else {
                &dependency_columns
            },
        )?;
        if validated_field_ids != dependency_field_ids || validated_columns != dependency_columns {
            pgrx::error!("membership dependency captured fields changed");
        }
        validate_impact_row_limit(client, max_impact_rows)?;
        validate_registered_impact_function(
            client,
            &impact_function,
            &impact_function_fingerprint,
        )?;
        dependencies.push(MembershipDependency {
            dependency_relation_id,
            dependency_registration_kind,
            target_relation_id,
            target_table_id,
            impact_function,
            max_impact_rows,
            dependency_columns,
        });
    }
    Ok(dependencies)
}

fn validate_complete_generation(client: &SpiClient<'_>, generation: i64) -> Result<(), spi::Error> {
    validate_generation_identity(client, generation, true)
}

fn validate_generation_identity(
    client: &SpiClient<'_>,
    generation: i64,
    require_validated: bool,
) -> Result<(), spi::Error> {
    if generation <= 0 {
        pgrx::error!("registry generation is invalid");
    }
    let rows = client.select(
        "SELECT rg.stream_generation = rs.stream_generation AS stream_matches,
                rg.validated,
                rg.state
         FROM synchro.sync_registry_generations rg
         JOIN synchro.sync_runtime_state rs ON rs.singleton = true
         WHERE rg.generation = $1",
        None,
        &[generation.into()],
    )?;
    let row = rows
        .into_iter()
        .next()
        .unwrap_or_else(|| pgrx::error!("registry generation is not complete"));
    let stream_matches = row
        .get_by_name::<bool, &str>("stream_matches")?
        .unwrap_or(false);
    let validated = row.get_by_name::<bool, &str>("validated")?.unwrap_or(false);
    let state = row
        .get_by_name::<String, &str>("state")?
        .unwrap_or_default();
    if !matches!(state.as_str(), "active" | "pending" | "superseded")
        || !stream_matches
        || (require_validated && !validated)
    {
        pgrx::error!("registry generation is not validated for the active stream");
    }
    Ok(())
}

pub(crate) fn active_generation_for_load(client: &SpiClient<'_>) -> Result<i64, spi::Error> {
    let rows = client.select(
        "SELECT rg.generation AS registry_generation
         FROM synchro.sync_registry_generations rg
         JOIN synchro.sync_runtime_state rs
           ON rs.singleton = true
          AND rs.stream_generation = rg.stream_generation
         WHERE rg.state = 'active'
           AND rg.validated",
        None,
        &[],
    )?;
    let Some(row) = rows.into_iter().next() else {
        pgrx::error!("there is no validated active registry generation");
    };
    let generation = row
        .get_by_name::<i64, &str>("registry_generation")?
        .unwrap_or_else(|| pgrx::error!("active registry generation is incomplete"));
    if generation <= 0 {
        pgrx::error!("active registry generation is invalid");
    }
    Ok(generation)
}

fn same_registration_content(left: &TableRegistration, right: &TableRegistration) -> bool {
    left.relation_id == right.relation_id
        && left.registration_kind == right.registration_kind
        && left.table_id == right.table_id
        && left.primary_key_field_id == right.primary_key_field_id
        && left.table_name == right.table_name
        && left.physical_schema == right.physical_schema
        && left.physical_relation == right.physical_relation
        && left.physical_relation_oid == right.physical_relation_oid
        && left.replica_identity == right.replica_identity
        && left.composition == right.composition
        && left.membership_function == right.membership_function
        && left.membership_function_fingerprint == right.membership_function_fingerprint
        && left.max_scope_fanout == right.max_scope_fanout
        && left.pk_column == right.pk_column
        && left.pk_type == right.pk_type
        && left.pk_portable_type == right.pk_portable_type
        && left.capture_key_columns == right.capture_key_columns
        && left.updated_at_col == right.updated_at_col
        && left.deleted_at_col == right.deleted_at_col
        && left.push_policy == right.push_policy
        && left.sync_columns == right.sync_columns
        && left.exclude_columns == right.exclude_columns
        && left.has_updated_at == right.has_updated_at
        && left.has_deleted_at == right.has_deleted_at
        && same_field_content(&left.fields, &right.fields)
        && same_capture_field_content(&left.capture_fields, &right.capture_fields)
}

fn same_field_content(left: &[FieldRegistration], right: &[FieldRegistration]) -> bool {
    let mut left = left.to_vec();
    let mut right = right.to_vec();
    left.sort_by(|left, right| left.physical_column.cmp(&right.physical_column));
    right.sort_by(|left, right| left.physical_column.cmp(&right.physical_column));
    left == right
}

fn same_capture_field_content(
    left: &[CaptureFieldRegistration],
    right: &[CaptureFieldRegistration],
) -> bool {
    let mut left = left.to_vec();
    let mut right = right.to_vec();
    left.sort_by(|left, right| left.physical_column.cmp(&right.physical_column));
    right.sort_by(|left, right| left.physical_column.cmp(&right.physical_column));
    left == right
}

fn registration_from_row(row: &SpiHeapTupleData<'_>) -> Result<TableRegistration, spi::Error> {
    let registry_generation = row
        .get_by_name::<i64, &str>("registry_generation")?
        .unwrap_or_else(|| pgrx::error!("registry entry has no generation"));
    let relation_id = row
        .get_by_name::<String, &str>("relation_id")?
        .unwrap_or_else(|| pgrx::error!("registry entry has no relation ID"));
    let registration_kind = row
        .get_by_name::<String, &str>("registration_kind")?
        .as_deref()
        .and_then(RegistrationKind::parse)
        .unwrap_or_else(|| pgrx::error!("registry entry has an invalid registration kind"));
    let table_id = row
        .get_by_name::<String, &str>("table_id")?
        .unwrap_or_default();
    let primary_key_field_id = row
        .get_by_name::<String, &str>("primary_key_field_id")?
        .unwrap_or_default();
    let table_name = row
        .get_by_name::<String, &str>("table_name")?
        .unwrap_or_else(|| pgrx::error!("registry entry has no logical table name"));
    let physical_schema = row
        .get_by_name::<String, &str>("physical_schema")?
        .unwrap_or_else(|| pgrx::error!("registry entry has no physical schema"));
    let physical_relation = row
        .get_by_name::<String, &str>("physical_relation")?
        .unwrap_or_else(|| pgrx::error!("registry entry has no physical relation"));
    let physical_relation_oid = row
        .get_by_name::<i64, &str>("physical_relation_oid")?
        .unwrap_or_else(|| pgrx::error!("registry entry has no physical relation OID"));
    let replica_identity = row
        .get_by_name::<String, &str>("replica_identity")?
        .unwrap_or_else(|| pgrx::error!("registry entry has no replica identity"));
    let composition = match row.get_by_name::<String, &str>("composition")?.as_deref() {
        Some("single_scope") => CompositionClass::SingleScope,
        Some("multi_scope") => CompositionClass::MultiScope,
        None if registration_kind == RegistrationKind::CaptureDependency => {
            CompositionClass::SingleScope
        }
        _ => pgrx::error!("registry entry has invalid composition"),
    };
    let membership_function = RegisteredFunction {
        oid: row
            .get_by_name::<i64, &str>("membership_function_oid")?
            .map(checked_oid)
            .unwrap_or_else(|| {
                if registration_kind == RegistrationKind::CaptureDependency {
                    0
                } else {
                    pgrx::error!("registry entry has no membership function OID")
                }
            }),
        schema: row
            .get_by_name::<String, &str>("membership_function_schema")?
            .unwrap_or_else(|| {
                if registration_kind == RegistrationKind::CaptureDependency {
                    String::new()
                } else {
                    pgrx::error!("registry entry has no membership function schema")
                }
            }),
        name: row
            .get_by_name::<String, &str>("membership_function_name")?
            .unwrap_or_else(|| {
                if registration_kind == RegistrationKind::CaptureDependency {
                    String::new()
                } else {
                    pgrx::error!("registry entry has no membership function name")
                }
            }),
    };
    let membership_function_fingerprint = row
        .get_by_name::<Vec<u8>, &str>("membership_function_fingerprint")?
        .unwrap_or_default();
    let max_scope_fanout = row
        .get_by_name::<i32, &str>("max_scope_fanout")?
        .unwrap_or_else(|| {
            if registration_kind == RegistrationKind::CaptureDependency {
                0
            } else {
                pgrx::error!("registry entry has no scope fanout limit")
            }
        });
    let pk_column = row
        .get_by_name::<String, &str>("pk_column")?
        .unwrap_or_else(|| pgrx::error!("registry entry has no primary key column"));
    let pk_type = row
        .get_by_name::<String, &str>("pk_type")?
        .unwrap_or_else(|| pgrx::error!("registry entry has no primary key type"));
    let pk_portable_type = row
        .get_by_name::<String, &str>("pk_portable_type")?
        .unwrap_or_else(|| pgrx::error!("registry entry has no portable primary key type"));
    let capture_key_columns = row
        .get_by_name::<Vec<String>, &str>("capture_key_columns")?
        .unwrap_or_else(|| pgrx::error!("registry entry has no capture key columns"));
    let updated_at_col = row
        .get_by_name::<String, &str>("updated_at_col")?
        .unwrap_or_else(|| pgrx::error!("registry entry has no updated_at column"));
    let deleted_at_col = row
        .get_by_name::<String, &str>("deleted_at_col")?
        .unwrap_or_else(|| pgrx::error!("registry entry has no deleted_at column"));
    let push_policy_value = row
        .get_by_name::<String, &str>("push_policy")?
        .unwrap_or_else(|| pgrx::error!("registry entry has no push policy"));
    let push_policy = PushPolicy::parse(&push_policy_value)
        .unwrap_or_else(|| pgrx::error!("registry entry has an invalid push policy"));
    let sync_columns = row
        .get_by_name::<Vec<String>, &str>("sync_columns")?
        .unwrap_or_else(|| pgrx::error!("registry entry has no synced columns"));
    let exclude_columns = row
        .get_by_name::<Vec<String>, &str>("exclude_columns")?
        .unwrap_or_else(|| pgrx::error!("registry entry has no excluded columns"));
    let has_updated_at = row
        .get_by_name::<bool, &str>("has_updated_at")?
        .unwrap_or_else(|| pgrx::error!("registry entry has no updated_at state"));
    let has_deleted_at = row
        .get_by_name::<bool, &str>("has_deleted_at")?
        .unwrap_or_else(|| pgrx::error!("registry entry has no deleted_at state"));

    Ok(TableRegistration {
        registry_generation,
        relation_id,
        registration_kind,
        table_id,
        primary_key_field_id,
        table_name,
        physical_schema,
        physical_relation,
        physical_relation_oid: checked_oid(physical_relation_oid),
        replica_identity,
        composition,
        membership_function,
        membership_function_fingerprint,
        max_scope_fanout,
        pk_column,
        pk_type,
        pk_portable_type,
        capture_key_columns,
        updated_at_col,
        deleted_at_col,
        push_policy,
        sync_columns,
        exclude_columns,
        has_updated_at,
        has_deleted_at,
        fields: Vec::new(),
        capture_fields: Vec::new(),
    })
}

fn validate_registration_metadata(
    client: &SpiClient<'_>,
    registration: &TableRegistration,
) -> Result<(), spi::Error> {
    if registration.is_capture_dependency() {
        if registration.relation_id.is_empty()
            || registration.table_name.trim().is_empty()
            || registration.pk_column.trim().is_empty()
            || registration.pk_type.trim().is_empty()
            || registration.registry_generation <= 0
            || !registration.table_id.is_empty()
            || !registration.primary_key_field_id.is_empty()
            || registration.membership_function.oid != 0
            || registration.max_scope_fanout != 0
            || !registration.fields.is_empty()
            || registration.capture_fields.is_empty()
            || registration.capture_key_columns.len() != 1
            || registration.capture_key_columns[0] != registration.pk_column
        {
            pgrx::error!("capture dependency registry metadata is incomplete");
        }
        let physical = relation_by_oid(client, registration.physical_relation_oid)?
            .unwrap_or_else(|| pgrx::error!("capture dependency relation no longer exists"));
        if physical.schema != registration.physical_schema
            || physical.relation != registration.physical_relation
            || physical.replica_identity != registration.replica_identity
        {
            pgrx::error!("capture dependency relation has drifted");
        }
        validate_capture_application_ownership(client, registration.physical_relation_oid)?;
        validate_publication_owner(client, registration.physical_relation_oid)?;
        validate_relation_privileges(
            client,
            registration.physical_relation_oid,
            &PushPolicy::ReadOnly,
            false,
        )?;
        validate_relation_rls(client, registration.physical_relation_oid)?;
        let primary_key = load_and_validate_primary_key(
            client,
            registration.physical_relation_oid,
            &registration.pk_column,
        )?;
        if primary_key.sql_type != registration.pk_type
            || primary_key.portable_type != registration.pk_portable_type
        {
            pgrx::error!("capture dependency key metadata has drifted");
        }
        let captured_columns = registration
            .capture_fields
            .iter()
            .filter(|field| !field.capture_key)
            .map(|field| field.physical_column.clone())
            .collect::<Vec<_>>();
        let mut actual = build_capture_field_registrations(
            client,
            registration.physical_relation_oid,
            &registration.capture_key_columns,
            &captured_columns,
        )?;
        let mut stored = registration.capture_fields.clone();
        actual.sort_by(|left, right| left.physical_column.cmp(&right.physical_column));
        stored.sort_by(|left, right| left.physical_column.cmp(&right.physical_column));
        if actual != stored {
            pgrx::error!("capture dependency field metadata has drifted");
        }
        let relation_identity_valid = client
            .select(
                "SELECT EXISTS (
                     SELECT 1 FROM synchro.sync_logical_ids
                     WHERE logical_id = $1::uuid AND kind = 'relation'
                 ) AS valid",
                None,
                &[registration.relation_id.as_str().into()],
            )?
            .first()
            .get_by_name::<bool, &str>("valid")?
            .unwrap_or(false);
        if !relation_identity_valid {
            pgrx::error!("capture dependency relation identity is invalid");
        }
        return Ok(());
    }
    if registration.relation_id.is_empty()
        || registration.table_id.is_empty()
        || registration.primary_key_field_id.is_empty()
        || registration.table_name.trim().is_empty()
        || registration.membership_function.schema.is_empty()
        || registration.membership_function.name.is_empty()
        || registration.membership_function.oid == 0
        || registration.max_scope_fanout <= 0
        || registration.pk_column.trim().is_empty()
        || registration.pk_type.trim().is_empty()
        || registration.registry_generation <= 0
    {
        pgrx::error!("registry metadata is incomplete");
    }
    let primary_key_fields: Vec<&FieldRegistration> = registration
        .fields
        .iter()
        .filter(|field| field.primary_key)
        .collect();
    if primary_key_fields.len() != 1
        || primary_key_fields[0].field_id != registration.primary_key_field_id
        || primary_key_fields[0].physical_column != registration.pk_column
        || primary_key_fields[0].portable_type != registration.pk_portable_type
        || primary_key_fields[0].writable
    {
        pgrx::error!("registry primary key field identity is invalid");
    }
    if registration.replica_identity != "d" {
        pgrx::error!("registry replica identity is invalid");
    }
    if !matches!(
        registration.pk_portable_type.as_str(),
        "string" | "int" | "int64"
    ) {
        pgrx::error!("registry portable primary key type is invalid");
    }
    let physical = relation_by_oid(client, registration.physical_relation_oid)?
        .unwrap_or_else(|| pgrx::error!("registered physical relation no longer exists"));
    if physical.schema != registration.physical_schema
        || physical.relation != registration.physical_relation
        || physical.replica_identity != registration.replica_identity
    {
        pgrx::error!("registered physical relation has drifted");
    }
    validate_application_ownership(
        client,
        registration.physical_relation_oid,
        registration.membership_function.oid,
    )?;
    validate_publication_owner(client, registration.physical_relation_oid)?;
    validate_relation_privileges(
        client,
        registration.physical_relation_oid,
        &registration.push_policy,
        registration.has_deleted_at,
    )?;
    validate_relation_rls(client, registration.physical_relation_oid)?;

    let primary_key = load_and_validate_primary_key(
        client,
        registration.physical_relation_oid,
        &registration.pk_column,
    )?;
    if primary_key.sql_type != registration.pk_type
        || primary_key.portable_type != registration.pk_portable_type
    {
        pgrx::error!("registered primary key metadata has drifted");
    }
    validate_scope_fanout_limit(client, registration.max_scope_fanout)?;
    validate_registered_membership_function(client, registration, primary_key.type_oid)?;

    let actual_columns =
        ordered_table_columns_for_oid_in_client(client, registration.physical_relation_oid)?;
    validate_stored_column_partition(&actual_columns, registration)?;
    let stored_field_columns: std::collections::HashSet<&str> = registration
        .fields
        .iter()
        .map(|field| field.physical_column.as_str())
        .collect();
    let synced_columns: std::collections::HashSet<&str> = registration
        .sync_columns
        .iter()
        .map(String::as_str)
        .collect();
    if stored_field_columns != synced_columns {
        pgrx::error!("registered field identity does not cover the synced projection");
    }
    validate_lifecycle_fields(registration)?;
    validate_logical_id_kinds(client, registration)?;
    let mut actual_fields = build_field_registrations(
        client,
        registration.physical_relation_oid,
        &registration.sync_columns,
        &registration.pk_column,
        &registration.updated_at_col,
        &registration.deleted_at_col,
        Some(&registration.fields),
    )?;
    let mut stored_fields = registration.fields.clone();
    actual_fields.sort_by(|left, right| left.physical_column.cmp(&right.physical_column));
    stored_fields.sort_by(|left, right| left.physical_column.cmp(&right.physical_column));
    if actual_fields != stored_fields {
        pgrx::error!("registered field metadata has drifted");
    }
    Ok(())
}

fn validate_logical_id_kinds(
    client: &SpiClient<'_>,
    registration: &TableRegistration,
) -> Result<(), spi::Error> {
    let valid: bool = client
        .select(
            "SELECT EXISTS (
                 SELECT 1 FROM synchro.sync_logical_ids
                 WHERE logical_id = $1::uuid AND kind = 'relation'
             ) AND EXISTS (
                 SELECT 1 FROM synchro.sync_logical_ids
                 WHERE logical_id = $2::uuid AND kind = 'table'
             ) AND NOT EXISTS (
                 SELECT 1
                 FROM unnest($3::text[]) AS expected(field_id)
                 LEFT JOIN synchro.sync_logical_ids ids
                   ON ids.logical_id = expected.field_id::uuid
                  AND ids.kind = 'field'
                 WHERE ids.logical_id IS NULL
             ) AS valid",
            None,
            &[
                registration.relation_id.as_str().into(),
                registration.table_id.as_str().into(),
                registration
                    .fields
                    .iter()
                    .map(|field| field.field_id.as_str())
                    .collect::<Vec<_>>()
                    .into(),
            ],
        )?
        .first()
        .get_by_name("valid")?
        .unwrap_or(false);
    if !valid {
        pgrx::error!("registry logical identity ledger is invalid");
    }
    Ok(())
}

fn validate_loaded_registration(
    client: &SpiClient<'_>,
    registration: &TableRegistration,
) -> Result<(), spi::Error> {
    validate_registration_metadata(client, registration)?;
    validate_capture_triggers(client, registration)?;
    validate_publication_membership(client, registration.physical_relation_oid)?;
    Ok(())
}

pub(crate) fn validate_capture_triggers(
    client: &SpiClient<'_>,
    registration: &TableRegistration,
) -> Result<(), spi::Error> {
    if !capture_triggers_match(client, registration)? {
        pgrx::error!("registered relation is missing required capture triggers");
    }
    Ok(())
}

fn capture_triggers_match(
    client: &SpiClient<'_>,
    registration: &TableRegistration,
) -> Result<bool, spi::Error> {
    let rows = client.select(
        "SELECT tg.tgname::text AS trigger_name,
                 tg.tgenabled::text AS trigger_enabled,
                 tg.tgtype::integer AS trigger_type,
                 tg.tgnargs::integer AS argument_count,
                 p.proname::text AS function_name,
                pg_catalog.pg_get_triggerdef(tg.oid, true) AS trigger_definition
         FROM pg_catalog.pg_trigger tg
         JOIN pg_catalog.pg_proc p ON p.oid = tg.tgfoid
         JOIN pg_catalog.pg_namespace pn ON pn.oid = p.pronamespace
         WHERE tg.tgrelid = $1::oid
           AND NOT tg.tgisinternal
           AND pn.nspname = 'synchro'
           AND tg.tgname = ANY($2)",
        None,
        &[
            i64::from(registration.physical_relation_oid).into(),
            vec![
                PRIMARY_KEY_GUARD_TRIGGER.to_string(),
                CAPTURE_FENCE_TRIGGER.to_string(),
                CAPTURE_TRUNCATE_TRIGGER.to_string(),
            ]
            .into(),
        ],
    )?;
    let mut found_guard = false;
    let mut found_fence = false;
    let mut found_truncate = false;
    for row in rows {
        let name = row
            .get_by_name::<String, &str>("trigger_name")?
            .unwrap_or_default();
        let enabled = row
            .get_by_name::<String, &str>("trigger_enabled")?
            .unwrap_or_default();
        let function = row
            .get_by_name::<String, &str>("function_name")?
            .unwrap_or_default();
        let trigger_type = row
            .get_by_name::<i32, &str>("trigger_type")?
            .unwrap_or_default();
        let argument_count = row
            .get_by_name::<i32, &str>("argument_count")?
            .unwrap_or_default();
        let definition = row
            .get_by_name::<String, &str>("trigger_definition")?
            .unwrap_or_default();
        if enabled != "O" {
            return Ok(false);
        }
        match name.as_str() {
            PRIMARY_KEY_GUARD_TRIGGER
                if function == PRIMARY_KEY_GUARD_TRIGGER
                    && trigger_type == 19
                    && argument_count == 1
                    && definition.contains(&format!("'{}'", registration.pk_column)) =>
            {
                if found_guard {
                    return Ok(false);
                }
                found_guard = true;
            }
            CAPTURE_FENCE_TRIGGER
                if function == CAPTURE_FENCE_TRIGGER
                    && trigger_type == 29
                    && argument_count == 5
                    && definition.contains(&format!("'{}'", registration.relation_id))
                    && definition
                        .contains(&format!("'{}'", registration.registration_kind.as_str()))
                    && definition.contains(&format!(
                        "'{}'",
                        if registration.is_synced() {
                            registration.table_id.as_str()
                        } else {
                            ""
                        }
                    ))
                    && registration
                        .capture_key_columns
                        .iter()
                        .all(|column| definition.contains(column))
                    && definition.contains(&format!(
                        "'{}'",
                        if registration.is_synced() && registration.has_deleted_at {
                            registration.deleted_at_col.as_str()
                        } else {
                            ""
                        }
                    )) =>
            {
                if found_fence {
                    return Ok(false);
                }
                found_fence = true;
            }
            CAPTURE_TRUNCATE_TRIGGER
                if function == CAPTURE_TRUNCATE_TRIGGER
                    && trigger_type == 34
                    && argument_count == 1
                    && definition.contains(&format!("'{}'", registration.relation_id)) =>
            {
                if found_truncate {
                    return Ok(false);
                }
                found_truncate = true;
            }
            _ => return Ok(false),
        }
    }
    Ok(found_guard && found_fence && found_truncate)
}

pub(crate) fn validate_publication_membership(
    client: &SpiClient<'_>,
    relation_oid: u32,
) -> Result<(), spi::Error> {
    let publication = configured_publication_name();
    if !publication_exists(client, &publication)?
        || publication_is_for_all_tables(client, &publication)?
        || !publication_contains_relation(client, &publication, relation_oid)?
    {
        pgrx::error!("registered relation is not an exact publication member");
    }
    Ok(())
}

fn validate_stored_column_partition(
    actual_columns: &[String],
    registration: &TableRegistration,
) -> Result<(), spi::Error> {
    validate_unique_columns(
        &registration.table_name,
        "sync_columns",
        &registration.sync_columns,
    )
    .unwrap_or_else(|message| pgrx::error!("registry metadata is invalid: {}", message));
    validate_unique_columns(
        &registration.table_name,
        "exclude_columns",
        &registration.exclude_columns,
    )
    .unwrap_or_else(|message| pgrx::error!("registry metadata is invalid: {}", message));

    let actual: std::collections::HashSet<&str> =
        actual_columns.iter().map(String::as_str).collect();
    let sync: std::collections::HashSet<&str> = registration
        .sync_columns
        .iter()
        .map(String::as_str)
        .collect();
    let excluded: std::collections::HashSet<&str> = registration
        .exclude_columns
        .iter()
        .map(String::as_str)
        .collect();
    if sync.is_empty()
        || !sync.contains(registration.pk_column.as_str())
        || !sync.is_disjoint(&excluded)
        || sync
            .union(&excluded)
            .copied()
            .collect::<std::collections::HashSet<_>>()
            != actual
    {
        pgrx::error!("registered synced column metadata has drifted");
    }
    if registration.has_updated_at != actual.contains(registration.updated_at_col.as_str())
        || registration.has_deleted_at != actual.contains(registration.deleted_at_col.as_str())
    {
        pgrx::error!("registered lifecycle column metadata has drifted");
    }
    if registration.has_updated_at && !sync.contains(registration.updated_at_col.as_str()) {
        pgrx::error!("registered updated_at column is not synced");
    }
    if registration.has_deleted_at && !sync.contains(registration.deleted_at_col.as_str()) {
        pgrx::error!("registered deleted_at column is not synced");
    }
    Ok(())
}

fn ordered_table_columns_for_oid_in_client(
    client: &SpiClient<'_>,
    relation_oid: u32,
) -> Result<Vec<String>, spi::Error> {
    let rows = client.select(
        "SELECT a.attname::text AS attname
         FROM pg_catalog.pg_attribute a
         WHERE a.attrelid = $1::oid
           AND a.attnum > 0
           AND NOT a.attisdropped
         ORDER BY a.attnum",
        None,
        &[i64::from(relation_oid).into()],
    )?;
    let mut columns = Vec::new();
    for row in rows {
        let column = row
            .get_by_name::<String, &str>("attname")?
            .unwrap_or_else(|| pgrx::error!("registered relation has an unnamed column"));
        columns.push(column);
    }
    if columns.is_empty() {
        pgrx::error!("registered relation has no columns");
    }
    Ok(columns)
}

fn checked_oid(value: i64) -> u32 {
    u32::try_from(value)
        .unwrap_or_else(|_| pgrx::error!("catalog returned an invalid relation OID"))
}

fn normalize_synced_columns(
    actual_columns: &[String],
    table_name: &str,
    pk_column: &str,
    updated_at_col: &str,
    deleted_at_col: &str,
    requested_sync_columns: &[String],
    requested_exclude_columns: &[String],
) -> Result<(Vec<String>, Vec<String>), String> {
    let has_updated_at = actual_columns.iter().any(|column| column == updated_at_col);
    let has_deleted_at = actual_columns.iter().any(|column| column == deleted_at_col);
    if !requested_sync_columns.is_empty() && !requested_exclude_columns.is_empty() {
        return Err(format!(
            "table {table_name} registration cannot specify both sync_columns and exclude_columns"
        ));
    }

    validate_unique_columns(table_name, "sync_columns", requested_sync_columns)?;
    validate_unique_columns(table_name, "exclude_columns", requested_exclude_columns)?;

    let actual_column_set: std::collections::HashSet<&str> = actual_columns
        .iter()
        .map(|column| column.as_str())
        .collect();
    for column in requested_sync_columns {
        if !actual_column_set.contains(column.as_str()) {
            return Err(format!(
                "table {table_name} sync column {column} does not exist"
            ));
        }
    }
    for column in requested_exclude_columns {
        if !actual_column_set.contains(column.as_str()) {
            return Err(format!(
                "table {table_name} excluded column {column} does not exist"
            ));
        }
    }

    let requested_sync_set: std::collections::HashSet<&str> = requested_sync_columns
        .iter()
        .map(|column| column.as_str())
        .collect();
    let requested_exclude_set: std::collections::HashSet<&str> = requested_exclude_columns
        .iter()
        .map(|column| column.as_str())
        .collect();
    let sync_columns: Vec<String> = if requested_sync_set.is_empty() {
        actual_columns
            .iter()
            .filter(|column| !requested_exclude_set.contains(column.as_str()))
            .cloned()
            .collect()
    } else {
        actual_columns
            .iter()
            .filter(|column| requested_sync_set.contains(column.as_str()))
            .cloned()
            .collect()
    };

    ensure_required_sync_column(table_name, pk_column, &sync_columns, "primary key")?;
    if has_updated_at {
        ensure_required_sync_column(table_name, updated_at_col, &sync_columns, "updated_at")?;
    }
    if has_deleted_at {
        ensure_required_sync_column(table_name, deleted_at_col, &sync_columns, "deleted_at")?;
    }

    let sync_column_set: std::collections::HashSet<&str> =
        sync_columns.iter().map(|column| column.as_str()).collect();
    let exclude_columns = actual_columns
        .iter()
        .filter(|column| !sync_column_set.contains(column.as_str()))
        .cloned()
        .collect();
    Ok((sync_columns, exclude_columns))
}

fn validate_unique_columns(
    table_name: &str,
    label: &str,
    columns: &[String],
) -> Result<(), String> {
    let mut seen = std::collections::HashSet::new();
    for column in columns {
        if !seen.insert(column.as_str()) {
            return Err(format!(
                "table {table_name} {label} contains duplicate column {column}"
            ));
        }
    }
    Ok(())
}

fn ensure_required_sync_column(
    table_name: &str,
    column_name: &str,
    sync_columns: &[String],
    label: &str,
) -> Result<(), String> {
    if sync_columns.iter().any(|column| column == column_name) {
        return Ok(());
    }
    Err(format!(
        "table {table_name} {label} column {column_name} must be part of the synced column set"
    ))
}
