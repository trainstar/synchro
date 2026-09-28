-- Same-relation impact declarations are valid. A clean installation names the
-- remaining table check without a numeric suffix.
ALTER TABLE synchro.sync_membership_dependencies
    DROP CONSTRAINT sync_membership_dependencies_check;
ALTER TABLE synchro.sync_membership_dependencies
    RENAME CONSTRAINT sync_membership_dependencies_check1 TO sync_membership_dependencies_check;

UPDATE synchro.sync_extension_build
SET installed_fingerprint = synchro.synchro_build_fingerprint(),
    installed_at = pg_catalog.now()
WHERE singleton;
