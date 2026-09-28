-- The WAL worker reads the tables that this update alters, in its own lock
-- order, during a poll and during startup after the restart that precedes the
-- update. The exclusive worker gate waits for those worker transactions and
-- holds the next one until the update commits, so they cannot deadlock.
SELECT pg_catalog.pg_advisory_xact_lock(2002873458);

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

-- Validation records whether a registry edge needs source values. Existing
-- rows keep NULL, which is unknown evidence. A pending generation with NULL
-- needs a verified bootstrap before activation.
ALTER TABLE synchro.sync_registry_generations
    ADD COLUMN source_requirement SMALLINT CHECK (source_requirement IN (0, 1, 2));

-- Format 2 transaction fingerprints bind the row boundary of each logical
-- message. Records from earlier releases keep format 1, which the worker
-- accepts only for those records.
ALTER TABLE synchro.sync_wal_transactions
    ADD COLUMN content_hash_format SMALLINT NOT NULL DEFAULT 1
        CHECK (content_hash_format IN (1, 2));
ALTER TABLE synchro.sync_wal_transactions
    ALTER COLUMN content_hash_format DROP DEFAULT;
