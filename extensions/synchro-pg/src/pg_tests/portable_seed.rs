#[pg_test]
fn test_portable_scope_declarations_preserve_eligibility() {
    setup_test_tables();
    register_shared_scope("catalog", true);
    register_shared_scope("runtime-only", false);

    let portable: Option<Vec<String>> = Spi::get_one(
        "SELECT array_agg(scope_id ORDER BY scope_id)
             FROM sync_shared_scopes WHERE portable",
    )
    .unwrap();
    assert_eq!(portable, Some(vec!["catalog".to_string()]));
}

fn corrupt_mac(token: &str) -> String {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;

    let mut parts = token.split('.').map(str::to_string).collect::<Vec<_>>();
    assert_eq!(parts.len(), 3);
    let payload = parts[1].clone();
    let mut mac = URL_SAFE_NO_PAD.decode(&parts[2]).expect("decode test MAC");
    assert_eq!(mac.len(), 32);
    mac[0] ^= 1;
    parts[2] = URL_SAFE_NO_PAD.encode(mac);
    assert_eq!(parts[1], payload);
    parts.join(".")
}

#[pg_test]
fn test_idempotent_registration_keeps_seed_receipts() {
    setup_test_tables();
    register_shared_scope("global", true);

    let snapshot = || -> pgrx::JsonB {
        Spi::get_one(
            "SELECT jsonb_build_object(
                 'generation', (
                     SELECT generation
                     FROM sync_registry_generations
                     WHERE state = 'active' AND validated
                 ),
                 'manifest', synchro_schema_manifest() - 'server_time',
                 'generation_count', (SELECT count(*) FROM sync_registry_generations),
                 'manifest_count', (SELECT count(*) FROM sync_schema_manifest),
                 'scope_state', (
                     SELECT jsonb_build_object(
                         'stream_generation', stream_generation,
                         'membership_generation', membership_generation,
                         'retention_generation', retention_generation
                     )
                     FROM sync_scope_state
                     WHERE scope_id = 'global'
                 ),
                 'scope_row', (
                     SELECT ctid::text
                     FROM sync_shared_scopes
                     WHERE scope_id = 'global'
                 )
             )",
        )
        .unwrap()
        .expect("idempotent registration state snapshot")
    };
    let before = snapshot();
    let receipt = mint_portable_seed_receipt("global");
    let membership_function: String = Spi::get_one(
        "SELECT format('%I.%I', membership_function_schema, membership_function_name)
         FROM sync_registry
         WHERE registry_generation = (
             SELECT generation
             FROM sync_registry_generations
             WHERE state = 'active' AND validated
         )
           AND table_name = 'test_orders'",
    )
    .unwrap()
    .expect("test_orders membership function");

    register_shared_scope("global", true);
    Spi::run_with_args(
        "SELECT synchro.synchro_register_table(
             'public.test_orders', $1, 'single_scope',
             'id', 'updated_at', 'deleted_at', 'enabled',
             ARRAY['internal_notes'], ARRAY[]::text[], 8
         )",
        &[membership_function.into()],
    )
    .unwrap();

    let after = snapshot();
    assert_eq!(after.0, before.0);

    let response = connect_client(
        "portable-seed-user",
        json!({
            "client_id": "portable-seed-client",
            "platform": "test",
            "app_version": "1.0.0",
            "protocol_version": 3,
            "schema": { "version": 0, "hash": "" },
            "scope_set_version": 0,
            "known_scopes": {},
            "seed_receipts": { "global": receipt }
        }),
    );
    assert!(response.get("error").is_none(), "{response}");
    let scope = response["scopes"]["add"]
        .as_array()
        .and_then(|scopes| {
            scopes
                .iter()
                .find(|scope| scope["id"].as_str() == Some("global"))
        })
        .expect("portable scope assignment");
    assert!(scope["cursor"].as_str().is_some(), "{response}");
}

#[pg_test]
fn test_unverifiable_seed_receipt_degrades_to_rebuild() {
    setup_test_tables();
    register_shared_scope("global", true);
    let receipt = mint_portable_seed_receipt("global");

    let response = connect_client(
        "stale-receipt-user",
        json!({
            "client_id": "stale-receipt-client",
            "platform": "test",
            "app_version": "1.0.0",
            "protocol_version": 3,
            "schema": { "version": 0, "hash": "" },
            "scope_set_version": 0,
            "known_scopes": {},
            "seed_receipts": { "global": corrupt_mac(&receipt) }
        }),
    );
    assert!(response.get("error").is_none(), "{response}");
    let scope = response["scopes"]["add"]
        .as_array()
        .and_then(|scopes| {
            scopes
                .iter()
                .find(|scope| scope["id"].as_str() == Some("global"))
        })
        .expect("portable scope assignment after corruption");
    assert!(scope["cursor"].is_null(), "{response}");
}

#[pg_test]
fn test_registration_attribute_change_advances_generation() {
    setup_test_tables();
    let active_generation: i64 = Spi::get_one(
        "SELECT generation
         FROM sync_registry_generations
         WHERE state = 'active' AND validated",
    )
    .unwrap()
    .expect("active registry generation before registration change");
    let membership_function: String = Spi::get_one_with_args(
        "SELECT format('%I.%I', membership_function_schema, membership_function_name)
         FROM sync_registry
         WHERE registry_generation = $1
           AND table_name = 'test_orders'",
        &[active_generation.into()],
    )
    .unwrap()
    .expect("test_orders membership function");

    Spi::run_with_args(
        "SELECT synchro.synchro_register_table(
             'public.test_orders', $1, 'single_scope',
             'id', 'updated_at', 'deleted_at', 'enabled',
             ARRAY['internal_notes'], ARRAY[]::text[], 4
         )",
        &[membership_function.into()],
    )
    .unwrap();

    let pending_generation: i64 = Spi::get_one(
        "SELECT generation
         FROM sync_registry_generations
         WHERE state = 'pending' AND validated
         ORDER BY generation DESC
         LIMIT 1",
    )
    .unwrap()
    .expect("pending registry generation after registration change");
    assert!(pending_generation > active_generation);
}

fn mint_portable_seed_receipt(scope_id: &str) -> String {
    let (_, schema_hash) = latest_schema_ref();
    let binding: pgrx::JsonB = Spi::get_one_with_args(
        "SELECT jsonb_build_object(
             'stream_generation', scope.stream_generation,
             'membership_generation', scope.membership_generation,
             'retention_generation', scope.retention_generation,
             'registry_generation', progress.registry_generation,
             'materialized_commit_lsn', progress.materialized_commit_lsn::text,
             'key_id', key.key_id,
             'secret', key.secret
         )
         FROM sync_shared_scopes shared
         JOIN sync_scope_state scope ON scope.scope_id = shared.scope_id
         JOIN sync_wal_progress progress
           ON progress.singleton = true
          AND progress.stream_generation = scope.stream_generation
         JOIN sync_token_keys key
           ON key.purpose = 'seed_continuation'
          AND key.state = 'active'
         WHERE shared.scope_id = $1 AND shared.portable",
        &[scope_id.into()],
    )
    .unwrap()
    .expect("portable seed receipt binding");
    let values = &binding.0;
    let snapshot_boundary = match values["materialized_commit_lsn"].as_str() {
        Some(commit_lsn) => crate::seed_token::SeedSnapshotBoundary {
            position_kind: "transaction_end".to_string(),
            commit_lsn: Some(commit_lsn.to_string()),
        },
        None => crate::seed_token::SeedSnapshotBoundary {
            position_kind: "generation_start".to_string(),
            commit_lsn: None,
        },
    };
    let receipt = crate::seed_token::SeedContinuationPayload {
        kind: "portable_seed_continuation".to_string(),
        version: 1,
        key_id: values["key_id"]
            .as_str()
            .expect("portable seed continuation key ID")
            .to_string(),
        export_id: "00000000-0000-0000-0000-000000000001".to_string(),
        export_manifest_hash: "1".repeat(64),
        schema_hash,
        scope_id: scope_id.to_string(),
        registry_generation: values["registry_generation"]
            .as_i64()
            .expect("portable seed registry generation")
            .to_string(),
        membership_generation: values["membership_generation"]
            .as_i64()
            .expect("portable seed membership generation")
            .to_string(),
        retention_generation: values["retention_generation"]
            .as_i64()
            .expect("portable seed retention generation")
            .to_string(),
        stream_generation: values["stream_generation"]
            .as_str()
            .expect("portable seed stream generation")
            .to_string(),
        snapshot_boundary,
        cardinality: "0".to_string(),
        checksum: synchro_core::checksum::ChecksumObject::new(
            synchro_core::checksum::Sha256Digest::from_lower_hex(&"3".repeat(64))
                .expect("portable seed receipt checksum"),
        ),
        issued_at: "2026-08-15T00:00:00.000000Z".to_string(),
    };
    crate::seed_token::issue_continuation(
        &receipt,
        values["secret"]
            .as_str()
            .expect("portable seed continuation key"),
    )
    .expect("issue portable seed continuation receipt")
}

#[pg_test]
fn test_granted_user_scope_reaches_one_user_and_revokes() {
    setup_test_tables();
    Spi::run_with_args(
        "SELECT synchro_grant_user_scope($1, $2)",
        &["granted-user".into(), "team:alpha".into()],
    )
    .unwrap();

    let granted = connect_client(
        "granted-user",
        json!({
            "client_id": "granted-client",
            "platform": "test",
            "app_version": "1.0.0",
            "protocol_version": 3,
            "schema": { "version": 0, "hash": "" },
            "scope_set_version": 0,
            "known_scopes": {}
        }),
    );
    assert!(granted.get("error").is_none(), "{granted}");
    let added = granted["scopes"]["add"]
        .as_array()
        .map(|scopes| {
            scopes
                .iter()
                .any(|scope| scope["id"].as_str() == Some("team:alpha"))
        })
        .unwrap_or(false);
    assert!(added, "granted scope is absent from the assignment: {granted}");

    // A granted scope reaches only the user it was granted to.
    let other = connect_client(
        "ungranted-user",
        json!({
            "client_id": "ungranted-client",
            "platform": "test",
            "app_version": "1.0.0",
            "protocol_version": 3,
            "schema": { "version": 0, "hash": "" },
            "scope_set_version": 0,
            "known_scopes": {}
        }),
    );
    let leaked = other["scopes"]["add"]
        .as_array()
        .map(|scopes| {
            scopes
                .iter()
                .any(|scope| scope["id"].as_str() == Some("team:alpha"))
        })
        .unwrap_or(false);
    assert!(!leaked, "granted scope reached another user: {other}");

    let assigned_version = granted["scope_set_version"].as_i64().expect("scope set version");
    Spi::run_with_args(
        "SELECT synchro_revoke_user_scope($1, $2)",
        &["granted-user".into(), "team:alpha".into()],
    )
    .unwrap();

    let revoked = connect_client(
        "granted-user",
        json!({
            "client_id": "granted-client",
            "platform": "test",
            "app_version": "1.0.0",
            "protocol_version": 3,
            "client_generation": client_generation("granted-user", "granted-client"),
            "schema": schema_ref_value(),
            "scope_set_version": assigned_version,
            "known_scopes": {
                "team:alpha": scope_cursor_ref("granted-user", "granted-client", "team:alpha", 0)
            }
        }),
    );
    assert!(revoked.get("error").is_none(), "{revoked}");
    let removed = revoked["scopes"]["remove"]
        .as_array()
        .map(|scopes| scopes.iter().any(|scope| scope.as_str() == Some("team:alpha")))
        .unwrap_or(false);
    assert!(removed, "revoked scope was not removed: {revoked}");
    assert!(
        revoked["scope_set_version"].as_i64().unwrap_or(0) > assigned_version,
        "revocation did not advance the scope set version: {revoked}"
    );
}
