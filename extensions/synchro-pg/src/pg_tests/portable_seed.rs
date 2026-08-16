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

#[pg_test]
fn test_portable_seed_tokens_reject_mac_only_corruption() {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;
    use synchro_core::checksum::{ChecksumObject, Sha256Digest};

    fn corrupt_mac(token: &str) -> String {
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

    let boundary = crate::seed_token::SeedSnapshotBoundary {
        position_kind: "generation_start".to_string(),
        commit_lsn: None,
    };
    let secret = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let page = crate::seed_token::issue_page(
        &crate::seed_token::SeedPagePayload {
            kind: "portable_seed_page".to_string(),
            version: 1,
            key_id: "seed-page-v1".to_string(),
            export_id: "00000000-0000-0000-0000-000000000001".to_string(),
            transaction_nonce: URL_SAFE_NO_PAD.encode([7_u8; 32]),
            export_manifest_hash: "1".repeat(64),
            schema_hash: "2".repeat(64),
            scope_id: "catalog".to_string(),
            registry_generation: "1".to_string(),
            membership_generation: "2".to_string(),
            retention_generation: "3".to_string(),
            stream_generation: "stream-1".to_string(),
            snapshot_boundary: boundary.clone(),
            next_row_ordinal: "0".to_string(),
            page_limit: "100".to_string(),
        },
        secret,
    )
    .expect("issue page token");
    let receipt = crate::seed_token::issue_continuation(
        &crate::seed_token::SeedContinuationPayload {
            kind: "portable_seed_continuation".to_string(),
            version: 1,
            key_id: "seed-continuation-v1".to_string(),
            export_id: "00000000-0000-0000-0000-000000000001".to_string(),
            export_manifest_hash: "1".repeat(64),
            schema_hash: "2".repeat(64),
            scope_id: "catalog".to_string(),
            registry_generation: "1".to_string(),
            membership_generation: "2".to_string(),
            retention_generation: "3".to_string(),
            stream_generation: "stream-1".to_string(),
            snapshot_boundary: boundary,
            cardinality: "0".to_string(),
            checksum: ChecksumObject::new(
                Sha256Digest::from_lower_hex(&"3".repeat(64)).expect("test checksum"),
            ),
            issued_at: "2026-08-15T00:00:00.000000Z".to_string(),
        },
        secret,
    )
    .expect("issue continuation receipt");

    assert!(crate::seed_token::verify_page(&page, secret).is_ok());
    assert!(crate::seed_token::verify_page(&corrupt_mac(&page), secret).is_err());
    assert!(crate::seed_token::verify_continuation(&receipt, secret).is_ok());
    assert!(crate::seed_token::verify_continuation(&corrupt_mac(&receipt), secret).is_err());
}
