    #[pg_test]
    fn test_pull_acknowledges_only_a_presented_issued_cursor() {
        setup_pull_fixtures();
        let first = pull_client(
            "u1",
            "c1",
            1,
            json!({ "user:u1": scope_cursor_ref("u1", "c1", "user:u1", 0) }),
            100,
        );
        let issued = first["scope_cursors"]["user:u1"]
            .as_str()
            .expect("issued scope cursor");
        let after_issuance: Option<String> = Spi::get_one(
            "SELECT position_kind FROM sync_client_checkpoints
             WHERE user_id = 'u1' AND client_id = 'c1' AND bucket_id = 'user:u1'",
        )
        .unwrap();
        assert_eq!(after_issuance.as_deref(), Some("generation_start"));

        let second = pull_client(
            "u1",
            "c1",
            1,
            json!({ "user:u1": { "cursor": issued } }),
            100,
        );
        assert!(second["changes"].as_array().unwrap().is_empty());
        let after_presentation: Option<String> = Spi::get_one(
            "SELECT position_kind FROM sync_client_checkpoints
             WHERE user_id = 'u1' AND client_id = 'c1' AND bucket_id = 'user:u1'",
        )
        .unwrap();
        assert_eq!(after_presentation.as_deref(), Some("transaction_end"));
    }

    #[pg_test]
    fn test_verify_only_cursor_key_accepts_existing_cursor() {
        setup_test_tables();

        let issued = issued_scope_cursor("u1", "c1", "global", 0);

        Spi::run(
            "UPDATE sync_token_keys
             SET state = 'verify_only'
             WHERE purpose = 'incremental_cursor' AND state = 'active';
             INSERT INTO sync_token_keys (key_id, purpose, secret, state)
             VALUES (
                 'incremental-cursor-v2', 'incremental_cursor',
                 '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
                 'active'
             )",
        )
        .unwrap();

        let parsed = Spi::connect(|client| {
            let context = test_scope_cursor_context(client, "u1", "c1", "global");
            crate::cursor_token::parse_scope_cursor(client, &context, &issued)
        })
        .unwrap();

        assert!(matches!(
            parsed,
            crate::cursor_token::ParsedScopeCursor::Current(_)
        ));
    }
