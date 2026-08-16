    #[pg_test]
    fn test_scope_checksum_matches_authored_canonical_vector() {
        let corpus: Value = serde_json::from_str(include_str!(
            "../../../../conformance/vectors/canonical-v1.json"
        ))
        .expect("parse canonical vector corpus");
        let vector = corpus["vectors"]
            .as_array()
            .expect("canonical vector array")
            .iter()
            .find(|vector| vector["vector_id"] == "VEC-SCOPE-ALTERED-PAIRING-001")
            .expect("authored scope digest vector");
        let input = &vector["input"];
        let schema_hash = synchro_core::checksum::SchemaHash::from_lower_hex(
            input["schema_hash"].as_str().expect("vector schema hash"),
        )
        .expect("decode vector schema hash");
        let entries = input["entries"]
            .as_array()
            .expect("vector scope entries")
            .iter()
            .map(|entry| {
                let identity = synchro_core::checksum::RowIdentity::from_lower_hex(
                    entry["row_identity_hex"]
                        .as_str()
                        .expect("vector row identity"),
                )
                .expect("decode vector row identity");
                let digest = synchro_core::checksum::Sha256Digest::from_lower_hex(
                    entry["row_digest_hex"]
                        .as_str()
                        .expect("vector row digest"),
                )
                .expect("decode vector row digest");
                synchro_core::checksum::ScopeDigestEntry::new(identity, digest)
            })
            .collect::<Vec<_>>();
        let digest = synchro_core::checksum::scope_digest(
            schema_hash,
            input["scope_id"].as_str().expect("vector scope ID"),
            &entries,
        )
        .expect("compute vector scope digest");

        assert_eq!(
            digest.to_lower_hex(),
            vector["expected"]["expected_sha256"]
                .as_str()
                .expect("vector expected digest")
        );
    }
