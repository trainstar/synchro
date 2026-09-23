    mod query_counts {
        use std::cell::Cell;

        use pgrx::pg_sys;

        thread_local! {
            static ACTIVE: Cell<bool> = const { Cell::new(false) };
            static DEPTH: Cell<usize> = const { Cell::new(0) };
            static COUNT: Cell<usize> = const { Cell::new(0) };
            static TARGET_DEPTH: Cell<usize> = const { Cell::new(0) };
            static PRIOR_START: Cell<pg_sys::ExecutorStart_hook_type> = const { Cell::new(None) };
            static PRIOR_END: Cell<pg_sys::ExecutorEnd_hook_type> = const { Cell::new(None) };
        }

        struct Hooks {
            start: pg_sys::ExecutorStart_hook_type,
            end: pg_sys::ExecutorEnd_hook_type,
        }

        impl Drop for Hooks {
            fn drop(&mut self) {
                unsafe {
                    pg_sys::ExecutorStart_hook = self.start;
                    pg_sys::ExecutorEnd_hook = self.end;
                }
                DEPTH.set(0);
                ACTIVE.set(false);
            }
        }

        #[pgrx::pg_guard]
        unsafe extern "C-unwind" fn start(query: *mut pg_sys::QueryDesc, flags: i32) {
            if DEPTH.get() == TARGET_DEPTH.get() {
                COUNT.set(COUNT.get() + 1);
            }
            DEPTH.set(DEPTH.get() + 1);
            match PRIOR_START.get() {
                Some(prior) => prior(query, flags),
                None => pg_sys::standard_ExecutorStart(query, flags),
            }
        }

        #[pgrx::pg_guard]
        unsafe extern "C-unwind" fn end(query: *mut pg_sys::QueryDesc) {
            match PRIOR_END.get() {
                Some(prior) => prior(query),
                None => pg_sys::standard_ExecutorEnd(query),
            }
            DEPTH.set(DEPTH.get() - 1);
        }

        // Depth one counts SPI work inside one SQL entry point, not SQL-function internals.
        pub(super) fn measure<T>(depth: usize, operation: impl FnOnce() -> T) -> (T, usize) {
            assert!(!ACTIVE.replace(true), "query measurement must not nest");
            let hooks = unsafe {
                Hooks {
                    start: pg_sys::ExecutorStart_hook,
                    end: pg_sys::ExecutorEnd_hook,
                }
            };
            PRIOR_START.set(hooks.start);
            PRIOR_END.set(hooks.end);
            COUNT.set(0);
            TARGET_DEPTH.set(depth);
            unsafe {
                pg_sys::ExecutorStart_hook = Some(start);
                pg_sys::ExecutorEnd_hook = Some(end);
            }
            let result = operation();
            let count = COUNT.get();
            drop(hooks);
            (result, count)
        }
    }

    #[pg_test]
    fn query_measurement_observes_database_execution() {
        Spi::run(
            "CREATE FUNCTION pg_temp.query_count_probe() RETURNS integer
             LANGUAGE SQL VOLATILE SET search_path = pg_catalog AS 'SELECT 42'",
        )
        .expect("create query measurement control");
        let (value, outer) = query_counts::measure(0, || {
            Spi::get_one::<i32>("SELECT pg_temp.query_count_probe()")
        });
        assert_eq!(value.expect("execute query control"), Some(42));
        assert_eq!(outer, 1);
        let (_, inner) = query_counts::measure(1, || {
            Spi::get_one::<i32>("SELECT pg_temp.query_count_probe()")
                .expect("execute nested query control")
        });
        assert_eq!(inner, 1);
        let (_, empty) = query_counts::measure(0, || ());
        assert_eq!(empty, 0);
        let nested = std::panic::catch_unwind(|| {
            query_counts::measure(0, || query_counts::measure(0, || ()))
        });
        assert!(nested.is_err());
        let (_, restored) = query_counts::measure(0, || {
            Spi::get_one::<i32>("SELECT 42").expect("execute after measurement rejection")
        });
        assert_eq!(restored, 1);
    }
