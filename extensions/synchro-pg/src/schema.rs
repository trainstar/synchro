use pgrx::prelude::*;
use pgrx::spi::SpiClient;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use synchro_core::contract::{
    ColumnSchema, LifecycleSchema, SchemaAction, SchemaManifest, SchemaRef, SchemaTransitionClass,
    SchemaUnsupportedReason, TableSchema,
};

const SCHEMA_MANIFEST_DOMAIN: &[u8] = b"synchro:v3:schema-manifest:v1\0";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ManifestBody {
    schema_version: i64,
    parent_schema: Option<SchemaRef>,
    transition_class: SchemaTransitionClass,
    compatibility_floor: i64,
    tables: Vec<TableSchema>,
}

#[derive(Debug)]
struct StoredManifest {
    version: i64,
    hash: String,
    body: ManifestBody,
    affected_scopes: Vec<String>,
}

pub(crate) struct PendingManifest {
    pub(crate) version: i64,
    pub(crate) hash: String,
    pub(crate) canonical_body: String,
}

pub(crate) struct SchemaLineageDecision {
    pub(crate) action: SchemaAction,
    pub(crate) reason: Option<SchemaUnsupportedReason>,
    pub(crate) affected_scopes: Vec<String>,
}

/// Return the canonical portable schema manifest for registered synced tables.
#[pg_extern]
fn synchro_schema_manifest() -> pgrx::JsonB {
    Spi::connect_mut(|client| {
        let stored = load_or_publish_latest_manifest(client);
        let manifest = immutable_manifest(stored);

        pgrx::JsonB(serde_json::json!({
            "schema_version": manifest.schema_version,
            "schema_hash": manifest.schema_hash,
            "server_time": server_time_str(client),
            "manifest": manifest,
        }))
    })
}

/// Return a simplified list of registered tables with sync metadata.
#[pg_extern]
fn synchro_tables() -> pgrx::JsonB {
    Spi::connect(|client| {
        let (schema_version, schema_hash) = crate::pull::get_latest_schema(client);

        let tup_table = match client.select(
            "SELECT r.table_name, r.push_policy, r.pk_column, r.updated_at_col, \
             r.deleted_at_col, r.sync_columns, r.exclude_columns,
             r.has_updated_at, r.has_deleted_at \
              FROM synchro.sync_registry r
              JOIN synchro.sync_registry_generations rg
               ON rg.generation = r.registry_generation
              JOIN synchro.sync_runtime_state rs
               ON rs.singleton = true
              AND rs.stream_generation = rg.stream_generation
             WHERE rg.state = 'active' AND rg.validated
               AND r.registration_kind = 'synced'
             ORDER BY r.table_name",
            None,
            &[],
        ) {
            Ok(t) => t,
            Err(e) => pgrx::error!("querying sync_registry: {}", e),
        };

        let mut tables: Vec<serde_json::Value> = Vec::new();
        for row in tup_table {
            let table_name: String = row
                .get_by_name::<String, &str>("table_name")
                .unwrap_or(None)
                .unwrap_or_default();
            let push_policy: String = row
                .get_by_name::<String, &str>("push_policy")
                .unwrap_or(None)
                .unwrap_or_default();
            let pk_column: String = row
                .get_by_name::<String, &str>("pk_column")
                .unwrap_or(None)
                .unwrap_or_default();
            let updated_at_col: String = row
                .get_by_name::<String, &str>("updated_at_col")
                .unwrap_or(None)
                .unwrap_or_default();
            let deleted_at_col: String = row
                .get_by_name::<String, &str>("deleted_at_col")
                .unwrap_or(None)
                .unwrap_or_default();
            let sync_columns: Vec<String> = row
                .get_by_name::<Vec<String>, &str>("sync_columns")
                .unwrap_or(None)
                .unwrap_or_default();
            let exclude_columns: Vec<String> = row
                .get_by_name::<Vec<String>, &str>("exclude_columns")
                .unwrap_or(None)
                .unwrap_or_default();
            let has_updated_at: bool = row
                .get_by_name::<bool, &str>("has_updated_at")
                .unwrap_or(None)
                .unwrap_or(false);
            let has_deleted_at: bool = row
                .get_by_name::<bool, &str>("has_deleted_at")
                .unwrap_or(None)
                .unwrap_or(false);

            tables.push(serde_json::json!({
                "table_name": table_name,
                "push_policy": push_policy,
                "dependencies": [],
                "pk_column": pk_column,
                "updated_at_column": optional_sync_column_name(has_updated_at, &updated_at_col),
                "deleted_at_column": optional_sync_column_name(has_deleted_at, &deleted_at_col),
                "sync_columns": sync_columns,
                "exclude_columns": exclude_columns,
            }));
        }

        pgrx::JsonB(serde_json::json!({
            "tables": tables,
            "server_time": server_time_str(client),
            "schema_version": schema_version,
            "schema_hash": schema_hash,
        }))
    })
}

/// Return debug state for a specific client.
///
/// Includes client info, bucket checkpoints, member counts, checksums,
/// and changelog statistics.
#[pg_extern]
fn synchro_debug(p_user_id: &str, p_client_id: &str) -> pgrx::JsonB {
    Spi::connect_mut(|client| {
        // Load client info.
        let client_info = load_client_debug(client, p_user_id, p_client_id);

        // Load bucket details.
        let bucket_subs: Vec<String> = client_info
            .get("bucket_subs")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let buckets = load_bucket_details(client, p_user_id, p_client_id, &bucket_subs);

        // Changelog stats.
        let changelog_stats = load_changelog_stats(client);

        pgrx::JsonB(serde_json::json!({
            "client": client_info,
            "buckets": buckets,
            "changelog_stats": changelog_stats,
            "server_time": server_time_str(client),
        }))
    })
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn server_time_str(client: &SpiClient<'_>) -> String {
    let _ = client;
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

pub(crate) fn build_schema_manifest_for_generation(
    client: &SpiClient<'_>,
    generation: i64,
) -> Vec<TableSchema> {
    let mut tables = Vec::new();
    let registrations = crate::registry::load_registry_generation_from_client(client, generation)
        .unwrap_or_else(|error| pgrx::error!("loading registry for schema manifest: {}", error));

    for registration in registrations {
        if !registration.is_synced() {
            continue;
        }
        tables.push(TableSchema {
            table_id: registration.table_id.clone(),
            relation_id: registration.relation_id.clone(),
            name: registration.table_name.clone(),
            primary_key_field_id: registration.primary_key_field_id.clone(),
            lifecycle: LifecycleSchema {
                created_at_field_id: field_id_for_column(&registration.fields, "created_at"),
                updated_at_field_id: registration
                    .has_updated_at
                    .then(|| {
                        field_id_for_column(&registration.fields, &registration.updated_at_col)
                    })
                    .flatten(),
                deleted_at_field_id: registration
                    .has_deleted_at
                    .then(|| {
                        field_id_for_column(&registration.fields, &registration.deleted_at_col)
                    })
                    .flatten(),
            },
            composition: registration.composition,
            fields: manifest_columns(&registration.fields),
            indexes: Vec::new(),
        });
    }
    tables.sort_by(|left, right| left.table_id.as_bytes().cmp(right.table_id.as_bytes()));

    tables
}

pub(crate) fn publish_schema_manifest(client: &mut SpiClient<'_>) -> Result<(), spi::Error> {
    crate::registry::acquire_registry_write_lock(client)?;
    let registry_generation = crate::registry::active_generation_for_load(client)?;
    let tables = build_schema_manifest_for_generation(client, registry_generation);
    let parent = load_latest_manifest(client);
    if parent
        .as_ref()
        .is_some_and(|stored| stored.body.tables == tables)
    {
        return Ok(());
    }

    let schema_version = parent.as_ref().map_or(1, |stored| {
        stored
            .version
            .checked_add(1)
            .unwrap_or_else(|| pgrx::error!("schema version allocation overflow"))
    });
    let transition_class =
        classify_transition(client, registry_generation, parent.as_ref(), &tables)?;
    if transition_class == SchemaTransitionClass::Class4 {
        validate_class_4_projection_transition(
            client,
            registry_generation,
            parent.as_ref(),
            &tables,
        )?;
    }
    let affected_scopes = if transition_class == SchemaTransitionClass::Class3 {
        load_current_scope_ids(client)?
    } else {
        Vec::new()
    };
    let compatibility_floor = if transition_class == SchemaTransitionClass::Class2 {
        parent
            .as_ref()
            .map(|stored| stored.body.compatibility_floor)
            .unwrap_or(schema_version)
    } else {
        schema_version
    };
    let body = ManifestBody {
        schema_version,
        parent_schema: parent.as_ref().map(|stored| SchemaRef {
            version: stored.version,
            hash: stored.hash.clone(),
        }),
        transition_class,
        compatibility_floor,
        tables,
    };
    let encoded = serde_json_canonicalizer::to_vec(&body)
        .unwrap_or_else(|error| pgrx::error!("canonicalizing schema manifest: {}", error));
    let mut hasher = Sha256::new();
    hasher.update(SCHEMA_MANIFEST_DOMAIN);
    hasher.update(&encoded);
    let schema_hash = format!("{:x}", hasher.finalize());
    let canonical_body = String::from_utf8(encoded)
        .unwrap_or_else(|error| pgrx::error!("encoding canonical schema manifest: {}", error));
    client.update(
        "INSERT INTO synchro.sync_schema_manifest (
             schema_version, schema_hash, canonical_manifest_body,
             parent_schema_version, parent_schema_hash, transition_class,
             compatibility_floor, registry_generation, affected_scopes
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        None,
        &[
            schema_version.into(),
            schema_hash.as_str().into(),
            canonical_body.as_str().into(),
            parent.as_ref().map(|stored| stored.version).into(),
            parent.as_ref().map(|stored| stored.hash.as_str()).into(),
            transition_class_name(transition_class).into(),
            compatibility_floor.into(),
            registry_generation.into(),
            affected_scopes.into(),
        ],
    )?;
    if matches!(
        transition_class,
        SchemaTransitionClass::Class2 | SchemaTransitionClass::Class4
    ) {
        crate::materialize::migrate_schema_digests(client, registry_generation)
            .unwrap_or_else(|error| pgrx::error!("migrating schema digests: {}", error));
    }
    Ok(())
}

/// Build the immutable manifest for a pending registry generation. The record
/// remains outside manifest history until the registry generation activates.
pub(crate) fn prepare_pending_manifest(
    client: &SpiClient<'_>,
    registry_generation: i64,
) -> Result<Option<PendingManifest>, String> {
    let tables = build_schema_manifest_for_generation(client, registry_generation);
    let parent = load_latest_manifest(client);
    if parent
        .as_ref()
        .is_some_and(|stored| stored.body.tables == tables)
    {
        return Ok(None);
    }
    let version = parent.as_ref().map_or(1, |stored| {
        stored
            .version
            .checked_add(1)
            .unwrap_or_else(|| pgrx::error!("schema version allocation overflow"))
    });
    let transition_class =
        classify_transition(client, registry_generation, parent.as_ref(), &tables)
            .map_err(|_| "classifying pending schema manifest failed".to_string())?;
    if transition_class == SchemaTransitionClass::Class4 {
        return Err(
            "pending registry generation has an incompatible schema transition".to_string(),
        );
    }
    let compatibility_floor = if transition_class == SchemaTransitionClass::Class2 {
        parent
            .as_ref()
            .map(|stored| stored.body.compatibility_floor)
            .unwrap_or(version)
    } else {
        version
    };
    let body = ManifestBody {
        schema_version: version,
        parent_schema: parent.as_ref().map(|stored| SchemaRef {
            version: stored.version,
            hash: stored.hash.clone(),
        }),
        transition_class,
        compatibility_floor,
        tables,
    };
    let encoded = serde_json_canonicalizer::to_vec(&body)
        .map_err(|_| "canonicalizing pending schema manifest failed".to_string())?;
    let mut hasher = Sha256::new();
    hasher.update(SCHEMA_MANIFEST_DOMAIN);
    hasher.update(&encoded);
    let hash = format!("{:x}", hasher.finalize());
    let canonical_body = String::from_utf8(encoded)
        .map_err(|_| "encoding pending schema manifest failed".to_string())?;
    Ok(Some(PendingManifest {
        version,
        hash,
        canonical_body,
    }))
}

/// Publish a previously verified pending manifest in the same transaction that
/// activates its registry and projection state.
pub(crate) fn publish_pending_manifest(
    client: &mut SpiClient<'_>,
    registry_generation: i64,
    version: i64,
    hash: &str,
    canonical_body: &str,
    affected_scopes: Vec<String>,
) -> Result<(), String> {
    let parent = load_latest_manifest(client);
    let body: ManifestBody = serde_json::from_str(canonical_body)
        .map_err(|_| "pending schema manifest is invalid".to_string())?;
    let canonical = serde_json_canonicalizer::to_vec(&body)
        .map_err(|_| "canonicalizing pending schema manifest failed".to_string())?;
    let tables = build_schema_manifest_for_generation(client, registry_generation);
    let transition_class =
        classify_transition(client, registry_generation, parent.as_ref(), &tables)
            .map_err(|_| "classifying pending schema manifest failed".to_string())?;
    let compatibility_floor = if transition_class == SchemaTransitionClass::Class2 {
        parent
            .as_ref()
            .map(|stored| stored.body.compatibility_floor)
            .unwrap_or(version)
    } else {
        version
    };
    if canonical != canonical_body.as_bytes()
        || body.schema_version != version
        || transition_class == SchemaTransitionClass::Class4
        || body.transition_class != transition_class
        || body.compatibility_floor != compatibility_floor
        || body.parent_schema.as_ref().map(|value| value.version)
            != parent.as_ref().map(|value| value.version)
        || body.parent_schema.as_ref().map(|value| value.hash.as_str())
            != parent.as_ref().map(|value| value.hash.as_str())
        || body.tables != tables
    {
        return Err("pending schema manifest binding changed".to_string());
    }
    let mut hasher = Sha256::new();
    hasher.update(SCHEMA_MANIFEST_DOMAIN);
    hasher.update(&canonical);
    if format!("{:x}", hasher.finalize()) != hash {
        return Err("pending schema manifest hash changed".to_string());
    }
    let manifest_affected_scopes = if body.transition_class == SchemaTransitionClass::Class3 {
        affected_scopes
    } else {
        Vec::new()
    };
    let inserted = client
        .update(
            "INSERT INTO synchro.sync_schema_manifest (
                 schema_version, schema_hash, canonical_manifest_body,
                 parent_schema_version, parent_schema_hash, transition_class,
                 compatibility_floor, registry_generation, affected_scopes
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            None,
            &[
                version.into(),
                hash.into(),
                canonical_body.into(),
                parent.as_ref().map(|value| value.version).into(),
                parent.as_ref().map(|value| value.hash.as_str()).into(),
                transition_class_name(transition_class).into(),
                compatibility_floor.into(),
                registry_generation.into(),
                manifest_affected_scopes.into(),
            ],
        )
        .map_err(|_| "publishing pending schema manifest failed".to_string())?
        .len();
    if inserted != 1 {
        return Err("pending schema manifest was not published".to_string());
    }
    Ok(())
}

pub(crate) fn generation_requires_projection_bootstrap(
    client: &SpiClient<'_>,
    registry_generation: i64,
) -> Result<bool, spi::Error> {
    let tables = build_schema_manifest_for_generation(client, registry_generation);
    let parent = load_latest_manifest(client);
    let schema_transition = if parent
        .as_ref()
        .is_some_and(|stored| stored.body.tables == tables)
    {
        None
    } else {
        Some(classify_transition(
            client,
            registry_generation,
            parent.as_ref(),
            &tables,
        )?)
    };
    let rows = client.select(
        "WITH active AS (
             SELECT generation
             FROM synchro.sync_registry_generations
             WHERE state = 'active' AND validated
             ORDER BY generation DESC
             LIMIT 1
         )
          SELECT target.registration_kind,
                 target.relation_id::text AS relation_id,
                 target.physical_schema::text AS physical_schema,
                 target.physical_relation::text AS physical_relation,
                 target.physical_relation_oid::bigint AS physical_relation_oid
         FROM synchro.sync_registry target
         CROSS JOIN active
         LEFT JOIN synchro.sync_registry source
           ON source.registry_generation = active.generation
          AND source.relation_id = target.relation_id
         WHERE target.registry_generation = $1
           AND (
               source.relation_id IS NULL
               OR source.registration_kind IS DISTINCT FROM target.registration_kind
               OR source.sync_columns IS DISTINCT FROM target.sync_columns
               OR source.capture_key_columns IS DISTINCT FROM target.capture_key_columns
               OR EXISTS (
                   (SELECT field_id, physical_column, portable_type, native_json,
                           decimal_precision, decimal_scale, nullable,
                           writable, primary_key
                    FROM synchro.sync_registry_fields
                    WHERE registry_generation = $1
                      AND relation_id = target.relation_id
                    EXCEPT
                     SELECT field_id, physical_column, portable_type, native_json,
                           decimal_precision, decimal_scale, nullable,
                           writable, primary_key
                    FROM synchro.sync_registry_fields
                    WHERE registry_generation = active.generation
                      AND relation_id = target.relation_id)
                   UNION ALL
                    (SELECT field_id, physical_column, portable_type, native_json,
                           decimal_precision, decimal_scale, nullable,
                           writable, primary_key
                    FROM synchro.sync_registry_fields
                    WHERE registry_generation = active.generation
                      AND relation_id = target.relation_id
                    EXCEPT
                     SELECT field_id, physical_column, portable_type, native_json,
                           decimal_precision, decimal_scale, nullable,
                           writable, primary_key
                    FROM synchro.sync_registry_fields
                    WHERE registry_generation = $1
                      AND relation_id = target.relation_id)
               )
               OR EXISTS (
                   (SELECT physical_column, portable_type, nullable, capture_key
                    FROM synchro.sync_capture_dependency_fields
                    WHERE registry_generation = $1
                      AND relation_id = target.relation_id
                    EXCEPT
                    SELECT physical_column, portable_type, nullable, capture_key
                    FROM synchro.sync_capture_dependency_fields
                    WHERE registry_generation = active.generation
                      AND relation_id = target.relation_id)
                   UNION ALL
                   (SELECT physical_column, portable_type, nullable, capture_key
                    FROM synchro.sync_capture_dependency_fields
                    WHERE registry_generation = active.generation
                      AND relation_id = target.relation_id
                    EXCEPT
                    SELECT physical_column, portable_type, nullable, capture_key
                    FROM synchro.sync_capture_dependency_fields
                    WHERE registry_generation = $1
                      AND relation_id = target.relation_id)
               )
           )
         ORDER BY target.relation_id",
        None,
        &[registry_generation.into()],
    )?;
    let parent_tables: std::collections::HashMap<&str, &TableSchema> = parent
        .as_ref()
        .map(|stored| {
            stored
                .body
                .tables
                .iter()
                .map(|table| (table.table_id.as_str(), table))
                .collect()
        })
        .unwrap_or_default();
    for row in rows {
        let registration_kind = row
            .get_by_name::<String, &str>("registration_kind")?
            .unwrap_or_else(|| pgrx::error!("projection bootstrap registration kind is missing"));
        if registration_kind == "synced" {
            match schema_transition {
                Some(SchemaTransitionClass::Initial | SchemaTransitionClass::Class3) => {}
                Some(SchemaTransitionClass::Class4) => {
                    // Manifest publication refuses a class 4 reshape over
                    // retained rows, so the same predicate must route the
                    // generation to the operator bootstrap. Issue #43.
                    let relation_id = row
                        .get_by_name::<String, &str>("relation_id")?
                        .unwrap_or_else(|| {
                            pgrx::error!("projection bootstrap relation identity is missing")
                        });
                    let Some(table) = tables.iter().find(|table| table.relation_id == relation_id)
                    else {
                        continue;
                    };
                    if class_4_table_requires_bootstrap(
                        client,
                        registry_generation,
                        parent_tables.get(table.table_id.as_str()).copied(),
                        table,
                    )? {
                        return Ok(true);
                    }
                    continue;
                }
                _ => continue,
            }
        }
        let schema = row
            .get_by_name::<String, &str>("physical_schema")?
            .unwrap_or_else(|| pgrx::error!("projection bootstrap source schema is missing"));
        let relation = row
            .get_by_name::<String, &str>("physical_relation")?
            .unwrap_or_else(|| pgrx::error!("projection bootstrap source relation is missing"));
        let relation_oid = row
            .get_by_name::<i64, &str>("physical_relation_oid")?
            .map(|value| {
                u32::try_from(value).unwrap_or_else(|_| {
                    pgrx::error!("projection bootstrap source relation has an invalid OID")
                })
            })
            .unwrap_or_else(|| {
                pgrx::error!("projection bootstrap source relation has no physical OID")
            });
        if relation_is_nonempty(client, &schema, &relation, relation_oid)? {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn load_latest_schema_manifest(client: &SpiClient<'_>) -> SchemaManifest {
    let stored = load_latest_manifest(client)
        .unwrap_or_else(|| pgrx::error!("there is no published schema manifest"));
    immutable_manifest(stored)
}

pub(crate) fn ensure_schema_manifest(client: &mut SpiClient<'_>) {
    let _ = load_or_publish_latest_manifest(client);
}

pub(crate) fn resolve_schema_lineage(
    client: &SpiClient<'_>,
    source: &SchemaRef,
) -> SchemaLineageDecision {
    let history = load_manifest_history(client);
    let Some(latest) = history.first() else {
        pgrx::error!("there is no published schema manifest");
    };
    if latest.version == source.version && latest.hash == source.hash {
        return SchemaLineageDecision {
            action: SchemaAction::None,
            reason: None,
            affected_scopes: Vec::new(),
        };
    }
    if !history
        .iter()
        .any(|manifest| manifest.version == source.version && manifest.hash == source.hash)
    {
        return SchemaLineageDecision {
            action: SchemaAction::Unsupported,
            reason: Some(SchemaUnsupportedReason::UnknownSchemaLineage),
            affected_scopes: Vec::new(),
        };
    }

    let mut current_version = latest.version;
    let mut current_hash = latest.hash.clone();
    let mut affected_scopes = Vec::new();
    for _ in 0..history.len() {
        if current_version == source.version && current_hash == source.hash {
            affected_scopes
                .sort_by(|left: &String, right: &String| left.as_bytes().cmp(right.as_bytes()));
            affected_scopes.dedup();
            return SchemaLineageDecision {
                action: SchemaAction::Replace,
                reason: None,
                affected_scopes,
            };
        }
        let Some(current) = history
            .iter()
            .find(|manifest| manifest.version == current_version && manifest.hash == current_hash)
        else {
            return SchemaLineageDecision {
                action: SchemaAction::Unsupported,
                reason: Some(SchemaUnsupportedReason::UnknownSchemaLineage),
                affected_scopes: Vec::new(),
            };
        };
        match current.body.transition_class {
            SchemaTransitionClass::Class2 => {}
            SchemaTransitionClass::Class3 => {
                affected_scopes.extend(current.affected_scopes.iter().cloned());
            }
            SchemaTransitionClass::Class4 => {
                return SchemaLineageDecision {
                    action: SchemaAction::Unsupported,
                    reason: Some(SchemaUnsupportedReason::IncompatibleSchemaTransition),
                    affected_scopes: Vec::new(),
                };
            }
            SchemaTransitionClass::Initial => {
                return SchemaLineageDecision {
                    action: SchemaAction::Unsupported,
                    reason: Some(SchemaUnsupportedReason::UnknownSchemaLineage),
                    affected_scopes: Vec::new(),
                };
            }
        }
        let Some(parent) = &current.body.parent_schema else {
            return SchemaLineageDecision {
                action: SchemaAction::Unsupported,
                reason: Some(SchemaUnsupportedReason::UnknownSchemaLineage),
                affected_scopes: Vec::new(),
            };
        };
        current_version = parent.version;
        current_hash = parent.hash.clone();
    }

    SchemaLineageDecision {
        action: SchemaAction::Unsupported,
        reason: Some(SchemaUnsupportedReason::UnknownSchemaLineage),
        affected_scopes: Vec::new(),
    }
}

fn load_or_publish_latest_manifest(client: &mut SpiClient<'_>) -> StoredManifest {
    if let Some(stored) = load_latest_manifest(client) {
        return stored;
    }
    publish_schema_manifest(client)
        .unwrap_or_else(|error| pgrx::error!("publishing initial schema manifest: {}", error));
    load_latest_manifest(client)
        .unwrap_or_else(|| pgrx::error!("initial schema manifest was not published"))
}

fn load_latest_manifest(client: &SpiClient<'_>) -> Option<StoredManifest> {
    load_manifest_history(client).into_iter().next()
}

fn load_manifest_history(client: &SpiClient<'_>) -> Vec<StoredManifest> {
    let rows = client
        .select(
            "SELECT schema_version, schema_hash, canonical_manifest_body,
                    parent_schema_version, parent_schema_hash, transition_class,
                    compatibility_floor, affected_scopes
              FROM synchro.sync_schema_manifest
             ORDER BY schema_version DESC",
            None,
            &[],
        )
        .unwrap_or_else(|error| pgrx::error!("loading schema manifest: {}", error));
    let mut manifests = Vec::new();
    for row in rows {
        let version = row
            .get_by_name::<i64, &str>("schema_version")
            .unwrap_or_else(|error| pgrx::error!("reading schema manifest version: {}", error))
            .unwrap_or_else(|| pgrx::error!("schema manifest has no version"));
        let hash = row
            .get_by_name::<String, &str>("schema_hash")
            .unwrap_or_else(|error| pgrx::error!("reading schema manifest hash: {}", error))
            .unwrap_or_else(|| pgrx::error!("schema manifest has no hash"));
        let body_text = row
            .get_by_name::<String, &str>("canonical_manifest_body")
            .unwrap_or_else(|error| pgrx::error!("reading schema manifest body: {}", error))
            .unwrap_or_else(|| pgrx::error!("schema manifest has no body"));
        let parent_version = row
            .get_by_name::<i64, &str>("parent_schema_version")
            .unwrap_or_else(|error| {
                pgrx::error!("reading schema manifest parent version: {}", error)
            });
        let parent_hash = row
            .get_by_name::<String, &str>("parent_schema_hash")
            .unwrap_or_else(|error| pgrx::error!("reading schema manifest parent hash: {}", error));
        let transition_class = row
            .get_by_name::<String, &str>("transition_class")
            .unwrap_or_else(|error| pgrx::error!("reading schema transition class: {}", error))
            .unwrap_or_else(|| pgrx::error!("schema transition class is missing"));
        let compatibility_floor = row
            .get_by_name::<i64, &str>("compatibility_floor")
            .unwrap_or_else(|error| pgrx::error!("reading schema compatibility floor: {}", error))
            .unwrap_or_else(|| pgrx::error!("schema compatibility floor is missing"));
        let mut affected_scopes = row
            .get_by_name::<Vec<String>, &str>("affected_scopes")
            .unwrap_or_else(|error| pgrx::error!("reading affected schema scopes: {}", error))
            .unwrap_or_else(|| pgrx::error!("affected schema scopes are missing"));
        affected_scopes.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        if affected_scopes.windows(2).any(|pair| pair[0] == pair[1]) {
            pgrx::error!("affected schema scopes contain duplicates");
        }

        let body: ManifestBody = serde_json::from_str(&body_text)
            .unwrap_or_else(|error| pgrx::error!("decoding stored schema manifest: {}", error));
        if body.schema_version != version {
            pgrx::error!("stored schema manifest version does not match its body");
        }
        if body.parent_schema.as_ref().map(|parent| parent.version) != parent_version
            || body
                .parent_schema
                .as_ref()
                .map(|parent| parent.hash.as_str())
                != parent_hash.as_deref()
            || transition_class_name(body.transition_class) != transition_class
            || body.compatibility_floor != compatibility_floor
        {
            pgrx::error!("stored schema manifest metadata does not match its body");
        }
        if body.transition_class != SchemaTransitionClass::Class3 && !affected_scopes.is_empty() {
            pgrx::error!("only a class 3 schema manifest can have affected scopes");
        }
        let canonical = serde_json_canonicalizer::to_vec(&body).unwrap_or_else(|error| {
            pgrx::error!("canonicalizing stored schema manifest: {}", error)
        });
        if canonical != body_text.as_bytes() {
            pgrx::error!("stored schema manifest body is not canonical");
        }
        let mut hasher = Sha256::new();
        hasher.update(SCHEMA_MANIFEST_DOMAIN);
        hasher.update(&canonical);
        if format!("{:x}", hasher.finalize()) != hash {
            pgrx::error!("stored schema manifest hash does not match its body");
        }
        manifests.push(StoredManifest {
            version,
            hash,
            body,
            affected_scopes,
        });
    }
    manifests
}

fn immutable_manifest(stored: StoredManifest) -> SchemaManifest {
    let manifest = SchemaManifest {
        schema_version: stored.version,
        schema_hash: stored.hash,
        parent_schema: stored.body.parent_schema,
        transition_class: stored.body.transition_class,
        compatibility_floor: stored.body.compatibility_floor,
        tables: stored.body.tables,
    };
    if let Err(error) = manifest.validate() {
        pgrx::error!("stored schema manifest violates the contract: {}", error);
    }
    manifest
}

fn transition_class_name(transition_class: SchemaTransitionClass) -> &'static str {
    match transition_class {
        SchemaTransitionClass::Initial => "initial",
        SchemaTransitionClass::Class2 => "class_2",
        SchemaTransitionClass::Class3 => "class_3",
        SchemaTransitionClass::Class4 => "class_4",
    }
}

fn load_current_scope_ids(client: &SpiClient<'_>) -> Result<Vec<String>, spi::Error> {
    let rows = client.select(
        "SELECT DISTINCT scope_id
         FROM (
             SELECT pg_catalog.unnest(bucket_subs) AS scope_id FROM synchro.sync_clients
             UNION ALL
              SELECT scope_id FROM synchro.sync_shared_scopes
         ) scopes",
        None,
        &[],
    )?;
    let mut scope_ids = Vec::new();
    for row in rows {
        let scope_id = row
            .get_by_name::<String, &str>("scope_id")?
            .unwrap_or_else(|| pgrx::error!("affected schema scope is missing"));
        scope_ids.push(scope_id);
    }
    scope_ids.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    scope_ids.dedup();
    Ok(scope_ids)
}

fn classify_transition(
    client: &SpiClient<'_>,
    registry_generation: i64,
    parent: Option<&StoredManifest>,
    tables: &[TableSchema],
) -> Result<SchemaTransitionClass, spi::Error> {
    let Some(parent) = parent else {
        return Ok(SchemaTransitionClass::Initial);
    };
    let parent_tables: std::collections::HashMap<&str, &TableSchema> = parent
        .body
        .tables
        .iter()
        .map(|table| (table.table_id.as_str(), table))
        .collect();
    let child_tables: std::collections::HashMap<&str, &TableSchema> = tables
        .iter()
        .map(|table| (table.table_id.as_str(), table))
        .collect();
    if parent_tables
        .keys()
        .any(|table_id| !child_tables.contains_key(table_id))
    {
        return Ok(SchemaTransitionClass::Class4);
    }

    let mut requires_rebuild = false;
    for (table_id, child) in &child_tables {
        let Some(prior) = parent_tables.get(table_id) else {
            if manifest_relation_is_nonempty(client, registry_generation, &child.relation_id)? {
                requires_rebuild = true;
            }
            continue;
        };
        if prior.relation_id != child.relation_id
            || prior.name != child.name
            || prior.primary_key_field_id != child.primary_key_field_id
            || prior.lifecycle != child.lifecycle
        {
            return Ok(SchemaTransitionClass::Class4);
        }
        if prior.composition != child.composition {
            requires_rebuild = true;
        }
        let prior_fields: std::collections::HashMap<&str, &ColumnSchema> = prior
            .fields
            .iter()
            .map(|field| (field.field_id.as_str(), field))
            .collect();
        let child_fields: std::collections::HashMap<&str, &ColumnSchema> = child
            .fields
            .iter()
            .map(|field| (field.field_id.as_str(), field))
            .collect();
        if prior_fields
            .keys()
            .any(|field_id| !child_fields.contains_key(field_id))
        {
            return Ok(SchemaTransitionClass::Class4);
        }
        for (field_id, field) in &child_fields {
            let Some(prior_field) = prior_fields.get(field_id) else {
                if !field.nullable {
                    requires_rebuild = true;
                }
                continue;
            };
            if prior_field.name != field.name
                || prior_field.type_name != field.type_name
                || prior_field.nullable && !field.nullable
                || prior_field.writable && !field.writable
            {
                return Ok(SchemaTransitionClass::Class4);
            }
        }
    }
    if requires_rebuild {
        Ok(SchemaTransitionClass::Class3)
    } else {
        Ok(SchemaTransitionClass::Class2)
    }
}

fn validate_class_4_projection_transition(
    client: &SpiClient<'_>,
    registry_generation: i64,
    parent: Option<&StoredManifest>,
    tables: &[TableSchema],
) -> Result<(), spi::Error> {
    let Some(parent) = parent else {
        return Ok(());
    };
    let parent_tables: std::collections::HashMap<&str, &TableSchema> = parent
        .body
        .tables
        .iter()
        .map(|table| (table.table_id.as_str(), table))
        .collect();
    for table in tables {
        if class_4_table_requires_bootstrap(
            client,
            registry_generation,
            parent_tables.get(table.table_id.as_str()).copied(),
            table,
        )? {
            pgrx::error!("class 4 transition requires projection bootstrap");
        }
    }
    Ok(())
}

/// A class 4 transition that changes more than field removal cannot replay
/// retained rows through the reshaped projection. Over a nonempty or retained
/// relation it completes only through the operator projection bootstrap, and
/// manifest publication and bootstrap preparation share this one predicate.
fn class_4_table_requires_bootstrap(
    client: &SpiClient<'_>,
    registry_generation: i64,
    prior: Option<&TableSchema>,
    table: &TableSchema,
) -> Result<bool, spi::Error> {
    let requires_baseline =
        prior.is_none_or(|prior| prior != table && !is_field_removal_only(prior, table));
    Ok(requires_baseline
        && (manifest_relation_is_nonempty(client, registry_generation, &table.relation_id)?
            || relation_has_retained_projection(client, &table.relation_id)?))
}

fn is_field_removal_only(parent: &TableSchema, child: &TableSchema) -> bool {
    parent.table_id == child.table_id
        && parent.relation_id == child.relation_id
        && parent.name == child.name
        && parent.primary_key_field_id == child.primary_key_field_id
        && parent.lifecycle == child.lifecycle
        && parent.composition == child.composition
        && parent.indexes == child.indexes
        && child.fields.len() < parent.fields.len()
        && child
            .fields
            .iter()
            .all(|field| parent.fields.contains(field))
}

fn relation_has_retained_projection(
    client: &SpiClient<'_>,
    relation_id: &str,
) -> Result<bool, spi::Error> {
    client
        .select(
            "SELECT EXISTS (
                 SELECT relation_id FROM synchro.sync_captured_rows
                 WHERE relation_id = $1::uuid
                 UNION ALL
                 SELECT relation_id FROM synchro.sync_captured_projections
                 WHERE relation_id = $1::uuid
             ) AS present",
            None,
            &[relation_id.into()],
        )?
        .first()
        .get_by_name::<bool, &str>("present")
        .map(|value| value.unwrap_or(false))
}

fn manifest_relation_is_nonempty(
    client: &SpiClient<'_>,
    registry_generation: i64,
    relation_id: &str,
) -> Result<bool, spi::Error> {
    let relation = client
        .select(
            "SELECT physical_schema::text AS physical_schema,
                    physical_relation::text AS physical_relation,
                    physical_relation_oid::bigint AS physical_relation_oid
              FROM synchro.sync_registry
              WHERE registry_generation = $1
                AND relation_id = $2::uuid
               AND registration_kind = 'synced'",
            None,
            &[registry_generation.into(), relation_id.into()],
        )?
        .first();
    let schema = relation
        .get_by_name::<String, &str>("physical_schema")?
        .unwrap_or_else(|| pgrx::error!("schema manifest relation has no physical schema"));
    let physical_relation = relation
        .get_by_name::<String, &str>("physical_relation")?
        .unwrap_or_else(|| pgrx::error!("schema manifest relation has no physical name"));
    let relation_oid = relation
        .get_by_name::<i64, &str>("physical_relation_oid")?
        .map(|value| {
            u32::try_from(value)
                .unwrap_or_else(|_| pgrx::error!("schema manifest relation has an invalid OID"))
        })
        .unwrap_or_else(|| pgrx::error!("schema manifest relation has no physical OID"));
    relation_is_nonempty(client, &schema, &physical_relation, relation_oid)
}

fn relation_is_nonempty(
    client: &SpiClient<'_>,
    schema: &str,
    relation: &str,
    expected_oid: u32,
) -> Result<bool, spi::Error> {
    let qualified = crate::registry::qualified_relation_name(schema, relation);
    let result = client
        .select(
            &format!(
                "SELECT pg_catalog.to_regclass($1::text)::oid::bigint AS resolved_relation_oid,
                        EXISTS (SELECT 1 FROM {qualified} LIMIT 1) AS nonempty"
            ),
            None,
            &[qualified.as_str().into()],
        )?
        .first();
    let resolved_oid = result
        .get_by_name::<i64, &str>("resolved_relation_oid")?
        .map(|value| {
            u32::try_from(value)
                .unwrap_or_else(|_| pgrx::error!("schema manifest relation has an invalid OID"))
        })
        .unwrap_or_else(|| pgrx::error!("schema manifest relation no longer exists"));
    if resolved_oid != expected_oid {
        pgrx::error!("schema manifest relation identity changed");
    }
    result
        .get_by_name::<bool, &str>("nonempty")
        .map(|value| value.unwrap_or(false))
}

fn manifest_columns(fields: &[crate::registry::FieldRegistration]) -> Vec<ColumnSchema> {
    let mut columns: Vec<ColumnSchema> = fields
        .iter()
        .map(|field| ColumnSchema {
            field_id: field.field_id.clone(),
            name: field.physical_column.clone(),
            type_name: field.portable_type.clone(),
            nullable: field.nullable,
            writable: field.writable,
            precision: field.decimal_precision,
            scale: field.decimal_scale,
        })
        .collect();
    columns.sort_by(|left, right| left.field_id.as_bytes().cmp(right.field_id.as_bytes()));
    columns
}

fn field_id_for_column(
    fields: &[crate::registry::FieldRegistration],
    physical_column: &str,
) -> Option<String> {
    fields
        .iter()
        .find(|field| field.physical_column == physical_column)
        .map(|field| field.field_id.clone())
}

fn optional_sync_column_name(enabled: bool, column_name: &str) -> Option<String> {
    if !enabled {
        return None;
    }

    let trimmed = column_name.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn load_client_debug(client: &SpiClient<'_>, user_id: &str, client_id: &str) -> serde_json::Value {
    let tup = match client.select(
        "SELECT id::pg_catalog.text, client_id, user_id, platform, app_version, is_active, \
          last_sync_at::pg_catalog.text, last_pull_at::pg_catalog.text, bucket_subs \
          FROM synchro.sync_clients WHERE user_id = $1 AND client_id = $2",
        None,
        &[user_id.into(), client_id.into()],
    ) {
        Ok(t) => t,
        Err(e) => pgrx::error!("querying client: {}", e),
    };

    if let Some(row) = tup.into_iter().next() {
        let id: String = row
            .get_by_name::<String, &str>("id")
            .unwrap_or(None)
            .unwrap_or_default();
        let cid: String = row
            .get_by_name::<String, &str>("client_id")
            .unwrap_or(None)
            .unwrap_or_default();
        let platform: String = row
            .get_by_name::<String, &str>("platform")
            .unwrap_or(None)
            .unwrap_or_default();
        let app_version: String = row
            .get_by_name::<String, &str>("app_version")
            .unwrap_or(None)
            .unwrap_or_default();
        let is_active: bool = row
            .get_by_name::<bool, &str>("is_active")
            .unwrap_or(None)
            .unwrap_or(false);
        let last_sync_at: Option<String> = row
            .get_by_name::<String, &str>("last_sync_at")
            .unwrap_or(None);
        let last_pull_at: Option<String> = row
            .get_by_name::<String, &str>("last_pull_at")
            .unwrap_or(None);
        let bucket_subs: Vec<String> = row
            .get_by_name::<Vec<String>, &str>("bucket_subs")
            .unwrap_or(None)
            .unwrap_or_default();

        serde_json::json!({
            "id": id,
            "client_id": cid,
            "platform": platform,
            "app_version": app_version,
            "is_active": is_active,
            "last_sync_at": last_sync_at,
            "last_pull_at": last_pull_at,
            "bucket_subs": bucket_subs,
        })
    } else {
        pgrx::error!("client not found: {}/{}", user_id, client_id);
    }
}

fn load_bucket_details(
    client: &mut SpiClient<'_>,
    user_id: &str,
    client_id: &str,
    bucket_subs: &[String],
) -> Vec<serde_json::Value> {
    let cp_tup = match client.select(
        "SELECT bucket_id,
                pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
                    'stream_generation', stream_generation,
                    'position_kind', position_kind,
                    'commit_lsn', commit_lsn::pg_catalog.text,
                    'event_ordinal', event_ordinal,
                    'effect_ordinal', effect_ordinal
                )) AS checkpoint
         FROM synchro.sync_client_checkpoints \
         WHERE user_id = $1 AND client_id = $2",
        None,
        &[user_id.into(), client_id.into()],
    ) {
        Ok(t) => t,
        Err(_) => return vec![],
    };

    let mut checkpoints: std::collections::HashMap<String, serde_json::Value> =
        std::collections::HashMap::new();
    for row in cp_tup {
        let bid: String = row
            .get_by_name::<String, &str>("bucket_id")
            .unwrap_or(None)
            .unwrap_or_default();
        let checkpoint = row
            .get_by_name::<pgrx::JsonB, &str>("checkpoint")
            .unwrap_or(None)
            .map(|value| value.0)
            .unwrap_or(serde_json::Value::Null);
        checkpoints.insert(bid, checkpoint);
    }

    let mut buckets: Vec<serde_json::Value> = Vec::new();
    let scope_digests =
        crate::pull::compute_bucket_checksums(client, bucket_subs).unwrap_or_default();
    for bid in bucket_subs {
        // Member count.
        let member_count: i64 = match client.select(
            "SELECT pg_catalog.count(*) AS cnt FROM synchro.sync_bucket_edges WHERE bucket_id = $1",
            None,
            &[bid.as_str().into()],
        ) {
            Ok(tup) => tup.first().get_one::<i64>().ok().flatten().unwrap_or(0),
            Err(_) => 0,
        };

        buckets.push(serde_json::json!({
            "bucket_id": bid,
            "checkpoint": checkpoints.get(bid).cloned(),
            "member_count": member_count,
            "checksum": scope_digests.get(bid).copied(),
        }));
    }
    buckets
}

fn load_changelog_stats(_client: &SpiClient<'_>) -> serde_json::Value {
    // Single query for all three stats (consistent snapshot).
    match Spi::get_three_with_args::<i64, i64, i64>(
        "SELECT COALESCE(pg_catalog.min(seq), 0), COALESCE(pg_catalog.max(seq), 0), \
                pg_catalog.count(*) FROM synchro.sync_changelog",
        &[],
    ) {
        Ok((min_seq, max_seq, total)) => serde_json::json!({
            "min_seq": min_seq.unwrap_or(0),
            "max_seq": max_seq.unwrap_or(0),
            "total_entries": total.unwrap_or(0),
        }),
        Err(_) => serde_json::json!({
            "min_seq": 0,
            "max_seq": 0,
            "total_entries": 0,
        }),
    }
}
