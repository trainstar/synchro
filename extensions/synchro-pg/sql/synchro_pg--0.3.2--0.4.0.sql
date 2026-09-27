-- Update synchro_pg from 0.3.2 to 0.4.0.
-- Stop if psql runs this file. ALTER EXTENSION removes the \echo line.
\echo Use "ALTER EXTENSION synchro_pg UPDATE TO '0.4.0'" to load this file. \quit

UPDATE synchro.sync_extension_build
SET installed_fingerprint = synchro.synchro_build_fingerprint(),
    installed_at = pg_catalog.now()
WHERE singleton;
