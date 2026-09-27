    const PUSH_REQUEST_LIMIT_OCTETS: usize = 1_048_576;
    const PUSH_LIMIT_MUTATIONS: usize = 18;
    // The canonical form of this double has 20 octets. serde_json writes 21 octets.
    const PUSH_LIMIT_DOUBLE: f64 = 18_446_744_073_709_552_000.0;

    fn push_limit_request(
        user_id: &str,
        client_id: &str,
        label: &str,
        canonical_octets: usize,
    ) -> Value {
        let build = |padding: &[usize]| {
            let mutations = padding
                .iter()
                .enumerate()
                .map(|(index, length)| {
                    push_mutation(
                        (user_id, client_id),
                        &format!("{label}:{index}"),
                        "test_portable_type_contract",
                        "insert",
                        &test_uuid(&format!("push-limit-row:{label}:{index}")),
                        None,
                        Some(&[
                            ("user_id", json!(user_id)),
                            ("label", json!("x".repeat(*length))),
                            ("col_double", json!(PUSH_LIMIT_DOUBLE)),
                        ]),
                    )
                })
                .collect();
            push_request(user_id, client_id, label, mutations)
        };
        let canonical_len =
            |request: &Value| serde_json_canonicalizer::to_vec(request).unwrap().len();

        let mut padding = vec![0; PUSH_LIMIT_MUTATIONS];
        let missing = canonical_octets - canonical_len(&build(&padding));
        for (index, length) in padding.iter_mut().enumerate() {
            *length = missing / PUSH_LIMIT_MUTATIONS
                + usize::from(index < missing % PUSH_LIMIT_MUTATIONS);
        }
        let request = build(&padding);
        assert_eq!(canonical_len(&request), canonical_octets);
        request
    }

    #[pg_test]
    fn test_push_request_limit_measures_canonical_octets() {
        setup_portable_type_contract_table();
        let user_id = "push-limit-user";
        let client_id = "push-limit-client";
        register_client(user_id, client_id);

        let over = push_limit_request(user_id, client_id, "over", PUSH_REQUEST_LIMIT_OCTETS + 1);
        let rejected = execute_push(user_id, &over);
        assert_eq!(
            rejected.json["error"]["code"].as_str(),
            Some("invalid_request")
        );
        assert_eq!(rejected.json["error"]["retryable"].as_bool(), Some(false));
        assert_eq!(push_ledger_counts(user_id, client_id), (0, 0));

        let exact = push_limit_request(user_id, client_id, "exact", PUSH_REQUEST_LIMIT_OCTETS);
        let parsed: Value =
            Spi::get_one_with_args::<pgrx::JsonB>("SELECT $1::jsonb", &[exact.to_string().into()])
                .unwrap()
                .expect("JSONB request")
                .0;
        assert!(serde_json::to_vec(&parsed).unwrap().len() > PUSH_REQUEST_LIMIT_OCTETS);

        let accepted = execute_push(user_id, &exact);
        let outcomes = accepted.json["accepted"]
            .as_array()
            .expect("accepted outcomes");
        assert_eq!(outcomes.len(), PUSH_LIMIT_MUTATIONS);
        assert!(outcomes
            .iter()
            .all(|outcome| outcome["status"] == "applied"));
        assert_eq!(
            push_ledger_counts(user_id, client_id),
            (1, PUSH_LIMIT_MUTATIONS as i64)
        );
        // Each outcome echoes its full row, so this response is larger than the request limit.
        assert!(accepted.raw.len() > PUSH_REQUEST_LIMIT_OCTETS);

        let replay = execute_push(user_id, &exact);
        assert_eq!(replay.raw, accepted.raw);
        assert_eq!(
            push_ledger_counts(user_id, client_id),
            (1, PUSH_LIMIT_MUTATIONS as i64)
        );
    }
