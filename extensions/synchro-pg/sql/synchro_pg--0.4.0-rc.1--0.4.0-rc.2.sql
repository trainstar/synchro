ALTER TABLE synchro.sync_assignment_function
    ALTER COLUMN max_scopes DROP NOT NULL;

ALTER FUNCTION synchro.synchro_register_assignment_function(TEXT, INTEGER)
    CALLED ON NULL INPUT;

UPDATE synchro.sync_extension_build
SET installed_fingerprint = synchro.synchro_build_fingerprint(),
    installed_at = pg_catalog.now()
WHERE singleton;
