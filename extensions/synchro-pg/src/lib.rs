use pgrx::prelude::*;

mod bgworker;
mod bucketing;
mod client;
mod compaction;
mod pull;
mod push;
mod rebuild;
mod registry;
mod schema;
mod wal_decoder;

pgrx::pg_module_magic!();

/// Extension initialization. Called when the shared library is loaded.
#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    // GUC registration and background worker setup will be added here.
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_extension_loads() {
        // Smoke test: extension loaded successfully.
        assert!(true);
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
