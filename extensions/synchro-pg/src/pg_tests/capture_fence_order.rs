    const A1: &str = "f1810000-0000-4000-8000-000000000001";
    const A2: &str = "f1810000-0000-4000-8000-000000000002";
    const A3: &str = "f1810000-0000-4000-8000-000000000003";
    const P: &str = "f1810000-0000-4000-8000-000000000011";
    const C: &str = "f1810000-0000-4000-8000-000000000012";

    fn seed_fence_orders(ids: &[&str]) {
        let rows = ids
            .iter()
            .map(|id| format!("('{id}', 'user-a', 'seed')"))
            .collect::<Vec<_>>()
            .join(", ");
        Spi::run(&format!(
            "INSERT INTO test_orders (id, user_id, title) VALUES {rows}"
        ))
        .expect("seed capture fence orders");
    }

    fn create_fence_row_trigger(
        table: &str,
        name: &str,
        timing: &str,
        event: &str,
        condition: &str,
        body: &str,
    ) {
        let result = if timing == "BEFORE" { "NEW" } else { "NULL" };
        Spi::run(&format!(
            "CREATE FUNCTION public.{name}() RETURNS trigger LANGUAGE plpgsql AS $trigger$
             BEGIN
                 {body}
                 RETURN {result};
             END
             $trigger$;
             CREATE TRIGGER {name} {timing} {event} ON public.{table}
                 FOR EACH ROW {condition} EXECUTE FUNCTION public.{name}()"
        ))
        .expect("create capture fence test trigger");
    }

    fn capture_fence_sqlstate(statements: &[&str]) -> String {
        Spi::run(
            "CREATE OR REPLACE FUNCTION public.capture_fence_sqlstate(p_statements text[])
             RETURNS text LANGUAGE plpgsql AS $$
             DECLARE
                 v_statement text;
             BEGIN
                 BEGIN
                     FOREACH v_statement IN ARRAY p_statements LOOP
                         EXECUTE v_statement;
                     END LOOP;
                 EXCEPTION WHEN OTHERS THEN
                     RETURN SQLSTATE;
                 END;
                 RETURN '00000';
             END
             $$",
        )
        .expect("create capture fence sqlstate helper");
        let statements = statements
            .iter()
            .map(|statement| (*statement).to_owned())
            .collect::<Vec<String>>();
        Spi::get_one_with_args::<String>(
            "SELECT public.capture_fence_sqlstate($1)",
            &[statements.into()],
        )
        .expect("capture fence sqlstate query")
        .expect("capture fence sqlstate")
    }

    fn synced_fence_operations(table: &str, record_id: &str) -> Option<String> {
        Spi::get_one_with_args(
            "SELECT string_agg(operation, ',' ORDER BY dml_ordinal)
             FROM synchro.sync_write_fences
             WHERE physical_relation = $1::name AND $2 IN (old_record_id, new_record_id)",
            &[table.into(), record_id.into()],
        )
        .expect("synced capture fence operations")
    }

    fn capture_dependency_fence_operations(table: &str, id: i32) -> Option<String> {
        Spi::get_one_with_args(
            "SELECT string_agg(operation, ',' ORDER BY dml_ordinal)
             FROM synchro.sync_write_fences
             WHERE physical_relation = $1::name
               AND COALESCE(new_capture_key, old_capture_key) = jsonb_build_object('id', $2)",
            &[table.into(), id.into()],
        )
        .expect("capture dependency fence operations")
    }

    fn capture_dependency_target(table: &str, id: i32) -> Option<i32> {
        Spi::get_one_with_args(
            &format!("SELECT target_id FROM public.{table} WHERE id = $1"),
            &[id.into()],
        )
        .expect("capture dependency target")
    }

    fn assert_version_matches_heap(table: &str, record_id: &str) {
        let (version_deleted, heap_deleted) = Spi::get_two_with_args::<bool, bool>(
            &format!(
                "SELECT (SELECT deleted FROM synchro.sync_row_versions WHERE record_id = $1),
                        NOT EXISTS (
                            SELECT 1 FROM public.{table}
                            WHERE id = $1::uuid AND deleted_at IS NULL
                        )"
            ),
            &[record_id.into()],
        )
        .expect("row version and heap state");
        assert_eq!(
            version_deleted, heap_deleted,
            "row version of {record_id} does not match the heap"
        );
    }

    #[pg_test]
    fn capture_fence_rejects_nested_revive_before_outer_delete_fence() {
        setup_test_tables();
        seed_fence_orders(&[A1, A2]);
        create_fence_row_trigger(
            "test_orders",
            "trigger_fence_t1",
            "AFTER",
            "UPDATE",
            &format!("WHEN (NEW.id = '{A1}')"),
            &format!("UPDATE test_orders SET deleted_at = NULL WHERE id = '{A2}';"),
        );

        let sqlstate = capture_fence_sqlstate(&[&format!(
            "UPDATE test_orders SET deleted_at = CASE WHEN id = '{A2}' THEN now() END
             WHERE id IN ('{A1}', '{A2}')"
        )]);

        assert_eq!(sqlstate, "27000");
    }

    #[pg_test]
    fn capture_fence_rejects_nested_delete_before_outer_update_fence() {
        setup_test_tables();
        seed_fence_orders(&[A1, A2]);
        create_fence_row_trigger(
            "test_orders",
            "trigger_fence_t2",
            "AFTER",
            "UPDATE",
            &format!("WHEN (NEW.id = '{A1}')"),
            &format!("UPDATE test_orders SET deleted_at = now() WHERE id = '{A2}';"),
        );

        let sqlstate = capture_fence_sqlstate(&[&format!(
            "UPDATE test_orders SET title = 'outer' WHERE id IN ('{A1}', '{A2}')"
        )]);

        assert_eq!(sqlstate, "27000");
    }

    #[pg_test]
    fn capture_fence_rejects_two_nested_updates_before_outer_fence() {
        setup_test_tables();
        seed_fence_orders(&[A1, A2]);
        create_fence_row_trigger(
            "test_orders",
            "trigger_fence_t3",
            "AFTER",
            "UPDATE",
            &format!("WHEN (NEW.id = '{A1}')"),
            &format!(
                "UPDATE test_orders SET deleted_at = NULL WHERE id = '{A2}';
                 UPDATE test_orders SET deleted_at = now() WHERE id = '{A2}';"
            ),
        );

        let sqlstate = capture_fence_sqlstate(&[&format!(
            "UPDATE test_orders SET deleted_at = CASE WHEN id = '{A2}' THEN now() END
             WHERE id IN ('{A1}', '{A2}')"
        )]);

        assert_eq!(sqlstate, "27000");
    }

    #[pg_test]
    fn capture_fence_rejects_nested_insert_before_outer_delete_fence() {
        setup_test_tables();
        seed_fence_orders(&[A1, A2]);
        create_fence_row_trigger(
            "test_orders",
            "trigger_fence_t4",
            "AFTER",
            "DELETE",
            &format!("WHEN (OLD.id = '{A1}')"),
            &format!(
                "INSERT INTO test_orders (id, user_id, title) VALUES ('{A2}', 'user-a', 'nested');"
            ),
        );

        let sqlstate = capture_fence_sqlstate(&[&format!(
            "DELETE FROM test_orders WHERE id IN ('{A1}', '{A2}')"
        )]);

        assert_eq!(sqlstate, "27000");
    }

    #[pg_test]
    fn capture_fence_rejects_nested_insert_delete_before_delete_fence() {
        setup_test_tables();
        seed_fence_orders(&[A1, A2]);
        create_fence_row_trigger(
            "test_orders",
            "trigger_fence_t5",
            "AFTER",
            "DELETE",
            &format!("WHEN (OLD.id = '{A1}')"),
            &format!(
                "INSERT INTO test_orders (id, user_id, title) VALUES ('{A2}', 'user-a', 'nested');
                 DELETE FROM test_orders WHERE id = '{A2}';"
            ),
        );

        let sqlstate = capture_fence_sqlstate(&[&format!(
            "DELETE FROM test_orders WHERE id IN ('{A1}', '{A2}')"
        )]);

        assert_eq!(sqlstate, "27000");
    }

    #[pg_test]
    fn capture_fence_rejects_nested_update_before_outer_insert_fence() {
        setup_test_tables();
        create_fence_row_trigger(
            "test_orders",
            "trigger_fence_t6",
            "AFTER",
            "INSERT",
            &format!("WHEN (NEW.id = '{A1}')"),
            &format!("UPDATE test_orders SET title = 'nested' WHERE id = '{A2}';"),
        );

        let sqlstate = capture_fence_sqlstate(&[&format!(
            "INSERT INTO test_orders (id, user_id, title)
             VALUES ('{A1}', 'user-a', 'outer'), ('{A2}', 'user-a', 'outer')"
        )]);

        assert_eq!(sqlstate, "27000");
    }

    #[pg_test]
    fn capture_fence_rejects_nested_delete_before_outer_insert_fence() {
        setup_test_tables();
        create_fence_row_trigger(
            "test_orders",
            "trigger_fence_t7",
            "AFTER",
            "INSERT",
            &format!("WHEN (NEW.id = '{A1}')"),
            &format!("DELETE FROM test_orders WHERE id = '{A2}';"),
        );

        let sqlstate = capture_fence_sqlstate(&[&format!(
            "INSERT INTO test_orders (id, user_id, title)
             VALUES ('{A1}', 'user-a', 'outer'), ('{A2}', 'user-a', 'outer')"
        )]);

        assert_eq!(sqlstate, "27000");
    }

    #[pg_test]
    fn capture_fence_rejects_second_level_write_before_outer_fence() {
        setup_test_tables();
        seed_fence_orders(&[A1, A2, A3]);
        create_fence_row_trigger(
            "test_orders",
            "trigger_fence_a1",
            "AFTER",
            "UPDATE",
            &format!("WHEN (NEW.id = '{A1}')"),
            &format!("UPDATE test_orders SET title = 'nested' WHERE id = '{A3}';"),
        );
        create_fence_row_trigger(
            "test_orders",
            "trigger_fence_a3",
            "AFTER",
            "UPDATE",
            &format!("WHEN (NEW.id = '{A3}')"),
            &format!("UPDATE test_orders SET title = 'nested' WHERE id = '{A2}';"),
        );

        let sqlstate = capture_fence_sqlstate(&[&format!(
            "UPDATE test_orders SET title = 'outer' WHERE id IN ('{A1}', '{A2}')"
        )]);

        assert_eq!(sqlstate, "27000");
    }

    #[pg_test]
    fn capture_fence_rejects_write_inside_record_body() {
        Spi::run("CREATE TYPE public.test_fence_mood AS ENUM ('calm', 'busy')")
            .expect("create capture fence mood type");
        let table = create_capture_dependency_table(false);
        Spi::run(&format!(
            "ALTER TABLE public.{table}
                 ADD COLUMN mood public.test_fence_mood NOT NULL DEFAULT 'calm'"
        ))
        .expect("add capture fence mood column");
        register_capture_dependency_table(&table);
        Spi::run(&format!(
            "INSERT INTO public.{table} (id, target_id) VALUES (1, 7)"
        ))
        .expect("seed capture fence cast row");
        Spi::run(&format!(
            "CREATE FUNCTION public.test_fence_mood_json(public.test_fence_mood)
             RETURNS json LANGUAGE plpgsql SECURITY DEFINER AS $mood$
             DECLARE
                 v_target text := current_setting('test.fence_cast_write', true);
             BEGIN
                 IF v_target IS NOT NULL AND v_target <> ''
                    AND EXISTS (
                        SELECT 1 FROM public.{table}
                        WHERE id = v_target::integer AND target_id = 8
                    ) THEN
                     PERFORM set_config('test.fence_cast_write', '', true);
                     UPDATE public.{table} SET target_id = target_id + 1
                     WHERE id = v_target::integer;
                 END IF;
                 RETURN to_json($1::text);
             END
             $mood$;
             CREATE CAST (public.test_fence_mood AS json)
                 WITH FUNCTION public.test_fence_mood_json(public.test_fence_mood)"
        ))
        .expect("create capture fence mood cast");

        let sqlstate = capture_fence_sqlstate(&[
            "SELECT set_config('test.fence_cast_write', '1', true)",
            &format!("UPDATE public.{table} SET target_id = 8 WHERE id = 1"),
        ]);

        assert_eq!(sqlstate, "27000");
    }

    #[pg_test]
    fn capture_fence_rejects_write_after_aborted_nested_write() {
        setup_test_tables();
        seed_fence_orders(&[A1, A2]);
        create_fence_row_trigger(
            "test_orders",
            "trigger_fence_t10",
            "AFTER",
            "UPDATE",
            &format!("WHEN (NEW.id = '{A1}')"),
            &format!(
                "BEGIN
                     UPDATE test_orders SET deleted_at = now() WHERE id = '{A2}';
                     RAISE EXCEPTION 'abort nested write' USING ERRCODE = 'P0001';
                 EXCEPTION WHEN raise_exception THEN
                     NULL;
                 END;
                 UPDATE test_orders SET deleted_at = now() WHERE id = '{A2}';"
            ),
        );

        let sqlstate = capture_fence_sqlstate(&[&format!(
            "UPDATE test_orders SET title = 'outer' WHERE id IN ('{A1}', '{A2}')"
        )]);

        assert_eq!(sqlstate, "27000");
    }

    #[pg_test]
    fn capture_fence_rejects_same_row_after_trigger_before_fence() {
        setup_test_tables();
        seed_fence_orders(&[A1]);
        create_fence_row_trigger(
            "test_orders",
            "audit_touch_self",
            "AFTER",
            "UPDATE",
            "WHEN (NEW.title = 'outer')",
            "UPDATE test_orders SET title = 'inner' WHERE id = NEW.id;",
        );

        let sqlstate = capture_fence_sqlstate(&[&format!(
            "UPDATE test_orders SET title = 'outer' WHERE id = '{A1}'"
        )]);

        assert_eq!(sqlstate, "27000");
    }

    #[pg_test]
    fn capture_fence_rejects_cte_trigger_write_before_outer_fence() {
        setup_test_tables();
        seed_fence_orders(&[A1]);
        Spi::run(
            "CREATE TABLE public.test_fence_links (id integer PRIMARY KEY, order_id uuid NOT NULL)",
        )
        .expect("create capture fence link table");
        create_fence_row_trigger(
            "test_fence_links",
            "trigger_fence_link",
            "BEFORE",
            "INSERT",
            "",
            "UPDATE test_orders SET title = 'linked' WHERE id = NEW.order_id;",
        );

        let sqlstate = capture_fence_sqlstate(&[&format!(
            "WITH changed AS (
                 UPDATE test_orders SET title = 'cte' WHERE id = '{A1}' RETURNING id
             )
             INSERT INTO public.test_fence_links (id, order_id) SELECT 1, id FROM changed"
        )]);

        assert_eq!(sqlstate, "27000");
    }

    #[pg_test]
    fn capture_fence_accepts_repeated_nested_writes_in_order() {
        setup_test_tables();
        seed_fence_orders(&[A3]);
        create_fence_row_trigger(
            "test_orders",
            "trigger_fence_p1",
            "AFTER",
            "INSERT",
            &format!("WHEN (NEW.id IN ('{A1}', '{A2}'))"),
            &format!("UPDATE test_orders SET title = title || '-nested' WHERE id = '{A3}';"),
        );

        let sqlstate = capture_fence_sqlstate(&[&format!(
            "INSERT INTO test_orders (id, user_id, title)
             VALUES ('{A1}', 'user-a', 'outer'), ('{A2}', 'user-a', 'outer')"
        )]);

        assert_eq!(sqlstate, "00000");
        assert_eq!(
            synced_fence_operations("test_orders", A3).as_deref(),
            Some("insert,update,update")
        );
        for id in [A1, A2, A3] {
            assert_version_matches_heap("test_orders", id);
        }
    }

    #[pg_test]
    fn capture_fence_accepts_before_insert_delete_of_same_key() {
        let table = create_capture_dependency_table(false);
        register_capture_dependency_table(&table);
        Spi::run(&format!(
            "INSERT INTO public.{table} (id, target_id) VALUES (1, 7)"
        ))
        .expect("seed capture fence replacement row");
        create_fence_row_trigger(
            &table,
            "trigger_fence_replace",
            "BEFORE",
            "INSERT",
            "",
            "EXECUTE format('DELETE FROM %I.%I WHERE id = $1', TG_TABLE_SCHEMA, TG_TABLE_NAME)
                 USING NEW.id;",
        );

        let sqlstate = capture_fence_sqlstate(&[&format!(
            "INSERT INTO public.{table} (id, target_id) VALUES (1, 8)"
        )]);

        assert_eq!(sqlstate, "00000");
        assert_eq!(
            capture_dependency_fence_operations(&table, 1).as_deref(),
            Some("insert,delete,insert")
        );
        assert_eq!(capture_dependency_target(&table, 1), Some(8));
    }

    #[pg_test]
    fn capture_fence_accepts_cte_delete_then_insert_of_same_key() {
        let table = create_capture_dependency_table(false);
        register_capture_dependency_table(&table);
        Spi::run(&format!(
            "INSERT INTO public.{table} (id, target_id) VALUES (1, 7)"
        ))
        .expect("seed capture fence CTE row");

        let sqlstate = capture_fence_sqlstate(&[&format!(
            "WITH removed AS (
                 DELETE FROM public.{table} WHERE id = 1 RETURNING id, target_id
             )
             INSERT INTO public.{table} (id, target_id) SELECT id, target_id + 1 FROM removed"
        )]);

        assert_eq!(sqlstate, "00000");
        assert_eq!(
            capture_dependency_fence_operations(&table, 1).as_deref(),
            Some("insert,delete,insert")
        );
        assert_eq!(capture_dependency_target(&table, 1), Some(8));
    }

    #[pg_test]
    fn capture_fence_accepts_sequential_writes_of_one_key() {
        let table = create_capture_dependency_table(false);
        register_capture_dependency_table(&table);

        let sqlstate = capture_fence_sqlstate(&[
            &format!("INSERT INTO public.{table} (id, target_id) VALUES (1, 7)"),
            &format!("UPDATE public.{table} SET target_id = 8 WHERE id = 1"),
            &format!("DELETE FROM public.{table} WHERE id = 1"),
            &format!("INSERT INTO public.{table} (id, target_id) VALUES (1, 9)"),
        ]);

        assert_eq!(sqlstate, "00000");
        assert_eq!(
            capture_dependency_fence_operations(&table, 1).as_deref(),
            Some("insert,update,delete,insert")
        );
        assert_eq!(capture_dependency_target(&table, 1), Some(9));
    }

    #[pg_test]
    fn capture_fence_accepts_cascade_after_same_statement_update() {
        setup_test_tables();
        Spi::run(
            "CREATE TABLE public.test_fence_parents (
                 id uuid PRIMARY KEY,
                 user_id text NOT NULL,
                 title text NOT NULL DEFAULT '',
                 updated_at timestamptz NOT NULL DEFAULT now(),
                 deleted_at timestamptz
             );
             CREATE TABLE public.test_fence_children (
                 id uuid PRIMARY KEY,
                 parent_id uuid NOT NULL REFERENCES public.test_fence_parents (id) ON DELETE CASCADE,
                 user_id text NOT NULL,
                 title text NOT NULL DEFAULT '',
                 updated_at timestamptz NOT NULL DEFAULT now(),
                 deleted_at timestamptz
             )",
        )
        .expect("create capture fence cascade tables");
        for table in ["test_fence_parents", "test_fence_children"] {
            Spi::run(&format!(
                "SELECT synchro.synchro_prepare_projection_view(
                    'public.{table}', '{table}', ARRAY['user_id']
                 );
                 SELECT tests.register_test_table(
                    '{table}',
                    $$SELECT 'user:' || (user_id #>> '{{}}') FROM synchro_projection.{table} WHERE record_id = p_key::text$$,
                    'single_scope',
                    'id', 'updated_at', 'deleted_at', 'enabled'
                 )"
            ))
            .expect("register capture fence cascade table");
        }
        activate_pending_registry_for_test();
        Spi::run(&format!(
            "INSERT INTO public.test_fence_parents (id, user_id) VALUES ('{P}', 'user-a')"
        ))
        .expect("insert capture fence parent");
        Spi::run(&format!(
            "INSERT INTO public.test_fence_children (id, parent_id, user_id)
             VALUES ('{C}', '{P}', 'user-a')"
        ))
        .expect("insert capture fence child");

        let sqlstate = capture_fence_sqlstate(&[&format!(
            "WITH removed AS (
                 DELETE FROM public.test_fence_parents WHERE id = '{P}' RETURNING id
             )
             UPDATE public.test_fence_children SET title = 'cascade'
             WHERE parent_id = '{P}' AND EXISTS (SELECT 1 FROM removed)"
        )]);

        assert_eq!(sqlstate, "00000");
        let present = Spi::get_one::<i64>(&format!(
            "SELECT (SELECT count(*) FROM public.test_fence_parents WHERE id = '{P}')
                  + (SELECT count(*) FROM public.test_fence_children WHERE id = '{C}')"
        ))
        .expect("capture fence cascade row count");
        assert_eq!(present, Some(0));
        let deleted = Spi::get_one_with_args::<bool>(
            "SELECT count(*) = 2 AND bool_and(deleted)
             FROM synchro.sync_row_versions WHERE record_id IN ($1, $2)",
            &[P.into(), C.into()],
        )
        .expect("capture fence cascade versions");
        assert_eq!(deleted, Some(true));
        assert_eq!(
            synced_fence_operations("test_fence_children", C).as_deref(),
            Some("insert,update,delete")
        );
        assert_eq!(
            synced_fence_operations("test_fence_parents", P).as_deref(),
            Some("insert,delete")
        );
    }

    #[pg_test]
    fn capture_fence_accepts_aborted_nested_update() {
        setup_test_tables();
        seed_fence_orders(&[A1, A2]);
        create_fence_row_trigger(
            "test_orders",
            "trigger_fence_p6",
            "AFTER",
            "UPDATE",
            &format!("WHEN (NEW.id = '{A1}')"),
            &format!(
                "BEGIN
                     UPDATE test_orders SET deleted_at = now() WHERE id = '{A2}';
                     RAISE EXCEPTION 'abort nested write' USING ERRCODE = 'P0001';
                 EXCEPTION WHEN raise_exception THEN
                     NULL;
                 END;"
            ),
        );

        let sqlstate = capture_fence_sqlstate(&[&format!(
            "UPDATE test_orders SET title = 'outer' WHERE id IN ('{A1}', '{A2}')"
        )]);

        assert_eq!(sqlstate, "00000");
        assert_eq!(
            synced_fence_operations("test_orders", A2).as_deref(),
            Some("insert,update")
        );
        for id in [A1, A2] {
            assert_version_matches_heap("test_orders", id);
        }
    }

    #[pg_test]
    fn capture_fence_accepts_aborted_nested_insert_after_delete() {
        setup_test_tables();
        seed_fence_orders(&[A1, A2]);
        create_fence_row_trigger(
            "test_orders",
            "trigger_fence_p7",
            "AFTER",
            "DELETE",
            &format!("WHEN (OLD.id = '{A1}')"),
            &format!(
                "BEGIN
                     INSERT INTO test_orders (id, user_id, title)
                     VALUES ('{A2}', 'user-a', 'nested');
                     RAISE EXCEPTION 'abort nested write' USING ERRCODE = 'P0001';
                 EXCEPTION WHEN raise_exception THEN
                     NULL;
                 END;"
            ),
        );

        let sqlstate = capture_fence_sqlstate(&[&format!(
            "DELETE FROM test_orders WHERE id IN ('{A1}', '{A2}')"
        )]);

        assert_eq!(sqlstate, "00000");
        assert_eq!(
            synced_fence_operations("test_orders", A2).as_deref(),
            Some("insert,delete")
        );
        for id in [A1, A2] {
            assert_version_matches_heap("test_orders", id);
        }
    }

    #[pg_test]
    fn capture_fence_accepts_same_row_after_trigger_after_fence() {
        setup_test_tables();
        seed_fence_orders(&[A1]);
        create_fence_row_trigger(
            "test_orders",
            "trigger_touch_self",
            "AFTER",
            "UPDATE",
            "WHEN (NEW.title = 'outer')",
            "UPDATE test_orders SET title = 'inner' WHERE id = NEW.id;",
        );

        let sqlstate = capture_fence_sqlstate(&[&format!(
            "UPDATE test_orders SET title = 'outer' WHERE id = '{A1}'"
        )]);

        assert_eq!(sqlstate, "00000");
        assert_eq!(
            synced_fence_operations("test_orders", A1).as_deref(),
            Some("insert,update,update")
        );
        let title = Spi::get_one_with_args::<String>(
            "SELECT title FROM test_orders WHERE id = $1::uuid",
            &[A1.into()],
        )
        .expect("capture fence touched title");
        assert_eq!(title.as_deref(), Some("inner"));
        assert_version_matches_heap("test_orders", A1);
    }

    #[pg_test]
    fn capture_fence_rejects_copy_freeze() {
        let table = create_capture_dependency_table(false);
        register_capture_dependency_table(&table);

        Spi::run(&format!(
            "DO $copy$
             BEGIN
                 EXECUTE 'TRUNCATE public.{table}';
                 EXECUTE 'COPY public.{table} (id, target_id) FROM PROGRAM ''echo 5,9'' WITH (FORMAT csv, FREEZE true)';
                 PERFORM set_config('test.copy_freeze_sqlstate', '00000', true);
             EXCEPTION WHEN OTHERS THEN
                 PERFORM set_config('test.copy_freeze_sqlstate', SQLSTATE, true);
             END
             $copy$"
        ))
        .expect("run capture fence COPY FREEZE");

        let sqlstate = Spi::get_one::<String>("SELECT current_setting('test.copy_freeze_sqlstate')")
            .expect("capture fence COPY FREEZE sqlstate");
        assert_eq!(sqlstate.as_deref(), Some("0A000"));
    }

    #[pg_test]
    fn capture_fence_rejects_deferrable_primary_key() {
        setup_test_tables();
        Spi::run(
            "ALTER TABLE public.test_orders DROP CONSTRAINT test_orders_pkey;
             ALTER TABLE public.test_orders ADD PRIMARY KEY (id) DEFERRABLE",
        )
        .expect("replace capture fence primary key");

        let sqlstate = capture_fence_sqlstate(&[&format!(
            "INSERT INTO test_orders (id, user_id, title) VALUES ('{A1}', 'user-a', 'seed')"
        )]);

        assert_eq!(sqlstate, "55000");
    }

    #[pg_test]
    fn capture_fence_rejects_key_change_by_later_before_trigger() {
        setup_test_tables();
        seed_fence_orders(&[A1]);
        create_fence_row_trigger(
            "test_orders",
            "zz_change_order_id",
            "BEFORE",
            "UPDATE",
            "",
            &format!("NEW.id := '{A3}';"),
        );

        let sqlstate = capture_fence_sqlstate(&[&format!(
            "UPDATE test_orders SET title = 'changed' WHERE id = '{A1}'"
        )]);

        assert_eq!(sqlstate, "23514");
    }

    #[pg_test]
    fn capture_fence_rejects_partition_row_move() {
        Spi::run(
            "CREATE TABLE public.test_fence_parts (
                 id integer PRIMARY KEY,
                 target_id integer NOT NULL
             ) PARTITION BY RANGE (id);
             CREATE TABLE public.test_fence_parts_low PARTITION OF public.test_fence_parts
                 FOR VALUES FROM (0) TO (100);
             CREATE TABLE public.test_fence_parts_high PARTITION OF public.test_fence_parts
                 FOR VALUES FROM (100) TO (200);
             GRANT SELECT ON TABLE public.test_fence_parts TO synchro_owner;
             ALTER TABLE public.test_fence_parts ENABLE ROW LEVEL SECURITY;
             CREATE POLICY test_fence_parts_policy ON public.test_fence_parts
                 AS PERMISSIVE FOR ALL TO synchro_owner
                 USING (true) WITH CHECK (true)",
        )
        .expect("create capture fence partitioned table");
        register_capture_dependency_table("test_fence_parts");
        Spi::run("INSERT INTO public.test_fence_parts (id, target_id) VALUES (5, 7)")
            .expect("seed capture fence partition row");
        create_fence_row_trigger(
            "test_fence_parts",
            "zz_move_part",
            "BEFORE",
            "UPDATE",
            "",
            "NEW.id := NEW.id + 100;",
        );

        let sqlstate = capture_fence_sqlstate(&[
            "UPDATE public.test_fence_parts SET target_id = 8 WHERE id = 5",
        ]);

        assert_eq!(sqlstate, "23514");
    }
