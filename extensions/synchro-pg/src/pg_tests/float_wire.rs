    use synchro_core::checksum::{row_digest, CanonicalRow, ChecksumObject, SchemaHash};

    // Each binary64 value and its RFC 8785 text. serde_json writes a different text for the first six.
    const FLOAT_WIRE_CASES: [(f64, &str); 9] = [
        (5.0, "5"),
        (-0.0, "0"),
        (0.000001, "0.000001"),
        (0.0000015, "0.0000015"),
        (18_446_744_073_709_552_000.0, "18446744073709552000"),
        (1e20, "100000000000000000000"),
        (1e-7, "1e-7"),
        (1.5, "1.5"),
        (1e21, "1e+21"),
    ];

    /// Computes the expected checksum from row text that contains the literal RFC 8785 float text.
    fn float_oracle_checksum(
        table_name: &str,
        row: &Value,
        float_text: &str,
        record_id: &str,
        version: &str,
        schema_hash: &str,
    ) -> Value {
        let float_field = field_id(table_name, "col_double");
        let mut row = row.clone();
        row[float_field.as_str()] = json!("float-placeholder");
        let row_text = serde_json::to_string(&row)
            .unwrap()
            .replace("\"float-placeholder\"", float_text);
        let table = Spi::connect(|client| {
            let registry = crate::registry::load_registry_from_client(client)?;
            let registration = registry
                .iter()
                .find(|registration| registration.table_name == table_name)
                .expect("float oracle registration");
            Ok::<_, spi::Error>(crate::pull::canonical_table(registration).unwrap())
        })
        .unwrap();
        let canonical_row =
            CanonicalRow::from_json(serde_json::to_string(record_id).unwrap(), &row_text).unwrap();
        let digest = row_digest(
            SchemaHash::from_lower_hex(schema_hash).unwrap(),
            &table,
            &canonical_row,
            version,
        )
        .unwrap();
        serde_json::to_value(ChecksumObject::new(digest)).unwrap()
    }

    fn stored_double(record_id: &str) -> f64 {
        Spi::get_one_with_args(
            "SELECT col_double FROM test_portable_type_contract WHERE id = $1::uuid",
            &[record_id.into()],
        )
        .unwrap()
        .expect("stored float value")
    }

    #[pg_test]
    fn test_push_accepts_rfc8785_float_values() {
        setup_portable_type_contract_table();
        let user_id = "float-user";
        let client_id = "float-client";
        register_client(user_id, client_id);
        let record_ids: Vec<String> = (0..FLOAT_WIRE_CASES.len())
            .map(|index| test_uuid(&format!("float-push-row:{index}")))
            .collect();
        let mutations = FLOAT_WIRE_CASES
            .iter()
            .zip(&record_ids)
            .enumerate()
            .map(|(index, ((value, _), record_id))| {
                push_mutation(
                    (user_id, client_id),
                    &format!("float-push:{index}"),
                    "test_portable_type_contract",
                    "insert",
                    record_id,
                    None,
                    Some(&[("user_id", json!(user_id)), ("col_double", json!(value))]),
                )
            })
            .collect();

        let response = push_client(user_id, client_id, "float-push", mutations);

        let accepted = response.json["accepted"]
            .as_array()
            .expect("accepted outcomes");
        assert_eq!(accepted.len(), FLOAT_WIRE_CASES.len());
        for ((outcome, (value, text)), record_id) in
            accepted.iter().zip(FLOAT_WIRE_CASES).zip(&record_ids)
        {
            assert_eq!(outcome["status"], "applied");
            assert_eq!(
                outcome["row_checksum"],
                float_oracle_checksum(
                    "test_portable_type_contract",
                    &outcome["server_row"],
                    text,
                    record_id,
                    outcome["server_version"].as_str().unwrap(),
                    outcome["outcome_schema"]["hash"].as_str().unwrap(),
                )
            );
            assert_eq!(stored_double(record_id), value);
        }
    }

    #[pg_test]
    fn test_push_rejects_fractional_int_wire_value() {
        setup_portable_type_contract_table();
        let user_id = "float-user";
        let client_id = "float-client";
        register_client(user_id, client_id);

        let response = push_client(
            user_id,
            client_id,
            "fractional-int",
            vec![push_mutation(
                (user_id, client_id),
                "fractional-int",
                "test_portable_type_contract",
                "insert",
                &test_uuid("fractional-int-row"),
                None,
                Some(&[("user_id", json!(user_id)), ("col_integer", json!(5.0))]),
            )],
        );

        assert_eq!(
            response.json["rejected"][0]["code"].as_str(),
            Some("validation_failed")
        );
    }

    #[pg_test]
    fn test_hydrated_row_checksum_uses_rfc8785_float_text() {
        setup_portable_type_contract_table();
        let (_, schema_hash) = latest_schema_ref();
        for (index, (value, text)) in FLOAT_WIRE_CASES.iter().enumerate() {
            let record_id = test_uuid(&format!("float-hydrate-row:{index}"));
            Spi::run_with_args(
                "INSERT INTO test_portable_type_contract (id, user_id, col_double)
                 VALUES ($1::uuid, 'float-user', $2)",
                &[record_id.as_str().into(), (*value).into()],
            )
            .unwrap();

            let hydrated = Spi::connect(|client| {
                let registry = crate::registry::load_registry_from_client(client)?;
                Ok::<_, spi::Error>(
                    crate::pull::hydrate_records(
                        client,
                        "test_portable_type_contract",
                        &[record_id.as_str()],
                        &registry,
                    )
                    .expect("hydrate float row")
                    .remove(0),
                )
            })
            .unwrap();

            assert_eq!(
                hydrated["row_checksum"],
                float_oracle_checksum(
                    "test_portable_type_contract",
                    &hydrated["data"],
                    text,
                    &record_id,
                    hydrated["server_version"].as_str().unwrap(),
                    &schema_hash,
                )
            );
        }
    }

    fn float_wal_image(
        registration: &TableRegistration,
        record_id: &str,
        float_text: &str,
    ) -> TupleImage {
        registration
            .fields
            .iter()
            .map(|field| {
                let value = match field.physical_column.as_str() {
                    "id" => TupleValue::Text(record_id.as_bytes().to_vec()),
                    "user_id" => TupleValue::Text(b"float-user".to_vec()),
                    "label" => TupleValue::Text(Vec::new()),
                    "col_double" => TupleValue::Text(float_text.as_bytes().to_vec()),
                    "updated_at" => TupleValue::Text(b"2000-01-01 00:00:00+00".to_vec()),
                    _ => TupleValue::Null,
                };
                (field.physical_column.clone(), value)
            })
            .collect()
    }

    #[pg_test]
    fn test_wal_materialization_captures_rfc8785_float_values() {
        setup_portable_type_contract_table();
        let (_, schema_hash) = latest_schema_ref();
        let registration = Spi::connect(|client| {
            crate::registry::load_registry_from_client(client).map(|registry| {
                registry
                    .into_iter()
                    .find(|registration| registration.table_name == "test_portable_type_contract")
                    .expect("float WAL registration")
            })
        })
        .unwrap();
        let xid: String = Spi::get_one("SELECT pg_current_xact_id()::text")
            .unwrap()
            .expect("float WAL transaction xid");
        let relation = RelationKey::new(
            registration.physical_schema.clone(),
            registration.physical_relation.clone(),
            registration.physical_relation_oid,
        );
        let commit_lsn = 0x400u64;
        let mut events = Vec::new();
        let mut messages = Vec::new();
        let mut record_ids = Vec::new();
        for (offset, (value, _)) in FLOAT_WIRE_CASES.iter().enumerate() {
            let record_id = test_uuid(&format!("float-wal-row:{offset}"));
            let fence_id = test_uuid(&format!("float-wal-fence:{offset}"));
            let row_version = test_uuid(&format!("float-wal-version:{offset}"));
            let dml_ordinal = i64::try_from(offset + 1).unwrap();
            Spi::run_with_args(
                "INSERT INTO test_portable_type_contract (id, user_id, col_double)
                 VALUES ($1::uuid, 'float-user', $2)",
                &[record_id.as_str().into(), (*value).into()],
            )
            .unwrap();
            let source_text: String = Spi::get_one_with_args(
                "SELECT col_double::text FROM test_portable_type_contract WHERE id = $1::uuid",
                &[record_id.as_str().into()],
            )
            .unwrap()
            .expect("float source text");
            Spi::run_with_args(
                "INSERT INTO synchro.sync_write_fences (
                     fence_id, transaction_xid, dml_ordinal, relation_id,
                     registration_kind, table_id,
                     physical_schema, physical_relation, physical_relation_oid,
                     operation, old_record_id, new_record_id, row_version
                 ) VALUES (
                     $1::uuid, pg_current_xact_id(), $2, $3::uuid,
                     'synced', $4::uuid, $5, $6, $7::oid,
                     'insert', NULL, $8, $9::uuid
                 )",
                &[
                    fence_id.as_str().into(),
                    dml_ordinal.into(),
                    registration.relation_id.as_str().into(),
                    registration.table_id.as_str().into(),
                    registration.physical_schema.as_str().into(),
                    registration.physical_relation.as_str().into(),
                    i64::from(registration.physical_relation_oid).into(),
                    record_id.as_str().into(),
                    row_version.as_str().into(),
                ],
            )
            .unwrap();
            events.push(WalEvent {
                operation: ChangeOperation::Insert,
                relation: relation.clone(),
                event_ordinal: u64::try_from(offset).unwrap(),
                before: None,
                after: Some(float_wal_image(&registration, &record_id, &source_text)),
            });
            messages.push(WalLogicalMessage {
                prefix: "synchro_fence".to_string(),
                content: serde_json::to_vec(&json!({
                    "fence_id": fence_id,
                    "dml_ordinal": dml_ordinal,
                    "registration_kind": "synced",
                    "relation_id": registration.relation_id,
                    "table_id": registration.table_id,
                    "physical_schema": registration.physical_schema,
                    "physical_relation": registration.physical_relation,
                    "physical_relation_oid": registration.physical_relation_oid,
                    "operation": "insert",
                    "old_record_id": Value::Null,
                    "new_record_id": record_id,
                    "old_capture_key": Value::Null,
                    "new_capture_key": Value::Null,
                    "row_version": row_version,
                }))
                .unwrap(),
                message_lsn: commit_lsn,
            });
            record_ids.push(record_id);
        }
        let transaction = WalTransaction {
            xid: xid.parse().unwrap(),
            final_lsn: commit_lsn,
            commit_lsn,
            end_lsn: commit_lsn + 1,
            commit_timestamp: 0,
            events,
            truncates: Vec::new(),
            messages,
        };

        Spi::run(
            "UPDATE synchro.sync_wal_progress
             SET generation_start_lsn = '0/1', processed_end_lsn = '0/1'
             WHERE singleton",
        )
        .expect("bind float WAL progress");
        Spi::connect_mut(|client| {
            crate::bgworker::materialize_transaction_for_test(client, &transaction)
        })
        .expect("materialize float WAL transaction");

        for ((_, text), record_id) in FLOAT_WIRE_CASES.iter().zip(&record_ids) {
            let captured: pgrx::JsonB = Spi::get_one_with_args(
                "SELECT jsonb_build_object(
                     'row_data', row_data,
                     'row_version', row_version::text,
                     'checksum', encode(checksum, 'hex')
                 )
                 FROM synchro.sync_captured_rows
                 WHERE record_id = $1",
                &[record_id.as_str().into()],
            )
            .unwrap()
            .expect("captured float row");
            let expected = float_oracle_checksum(
                "test_portable_type_contract",
                &captured.0["row_data"],
                text,
                record_id,
                captured.0["row_version"].as_str().unwrap(),
                &schema_hash,
            );
            assert_eq!(captured.0["checksum"], expected["digest"]);
        }
    }
