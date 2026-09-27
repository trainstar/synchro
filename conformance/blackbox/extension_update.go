package blackbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// ExtensionUpdateResult contains the facts that an extension update observes.
type ExtensionUpdateResult struct {
	VersionBeforeUpdate               string
	ReadyBeforeUpdate                 bool
	ExtensionObjectsStateBeforeUpdate string
	VersionAfterUpdate                string
}

// ExtensionCatalogObservation contains normalized extension object lines.
type ExtensionCatalogObservation struct {
	Updated []string
	Clean   []string
}

// UpdateExtension installs the environment bundle over the update baseline
// bundle, restarts PostgreSQL, runs the documented update statement, and then
// requires capture readiness.
func (h *Harness) UpdateExtension(ctx context.Context) (ExtensionUpdateResult, error) {
	result, err := h.ApplyExtensionUpdate(ctx)
	if err != nil {
		return ExtensionUpdateResult{}, err
	}
	if err := h.FinishExtensionUpdate(ctx); err != nil {
		return ExtensionUpdateResult{}, err
	}
	return result, nil
}

// ApplyExtensionUpdate installs the environment bundle over the update
// baseline bundle, restarts PostgreSQL, and runs the documented update
// statement. It does not require capture readiness, so a caller can observe
// retained predecessor state before FinishExtensionUpdate.
func (h *Harness) ApplyExtensionUpdate(ctx context.Context) (ExtensionUpdateResult, error) {
	if h == nil || ctx == nil || !h.sourceReady || h.config.UpdateBaselineExtensionArtifact == "" ||
		h.attached || h.extensionUpdated || h.adapter != nil {
		return ExtensionUpdateResult{}, errors.New("isolated extension update is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return ExtensionUpdateResult{}, errors.New("isolated extension update context expired")
	}
	if err := h.installEnvironmentExtension(ctx); err != nil {
		return ExtensionUpdateResult{}, err
	}
	if err := h.restartPostgres(ctx); err != nil {
		return ExtensionUpdateResult{}, err
	}
	database, err := h.openDatabase(ctx, h.names.Database, h.env.Admin, false)
	if err != nil {
		return ExtensionUpdateResult{}, errors.New("connect for extension update failed")
	}
	defer database.Close()
	var result ExtensionUpdateResult
	if result.VersionBeforeUpdate, err = readExtensionVersion(ctx, database); err != nil {
		return ExtensionUpdateResult{}, err
	}
	var objectsState sql.NullString
	if err := database.QueryRowContext(ctx, `
		SELECT (health->>'ready')::boolean,
		       health->'checks'->'extension_objects_stale'->>'state'
		FROM (SELECT synchro.synchro_health_detail() AS health) state`,
	).Scan(&result.ReadyBeforeUpdate, &objectsState); err != nil {
		return ExtensionUpdateResult{}, fmt.Errorf("read extension health before update failed: %w", err)
	}
	if !objectsState.Valid {
		return ExtensionUpdateResult{}, errors.New("extension objects health before update is unavailable")
	}
	result.ExtensionObjectsStateBeforeUpdate = objectsState.String
	if _, err := database.ExecContext(ctx, "ALTER EXTENSION synchro_pg UPDATE"); err != nil {
		return ExtensionUpdateResult{}, fmt.Errorf("update synchro_pg extension failed: %w", err)
	}
	h.extensionUpdated = true
	if result.VersionAfterUpdate, err = readExtensionVersion(ctx, database); err != nil {
		return ExtensionUpdateResult{}, err
	}
	return result, nil
}

// FinishExtensionUpdate waits for the WAL worker and capture readiness after
// ApplyExtensionUpdate, then starts the adapter.
func (h *Harness) FinishExtensionUpdate(ctx context.Context) error {
	if h == nil || ctx == nil || !h.sourceReady || !h.extensionUpdated ||
		h.extensionUpdateCompleted || h.adapter != nil {
		return errors.New("isolated extension update completion is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return errors.New("isolated extension update completion context expired")
	}
	if err := h.waitForWorker(ctx); err != nil {
		return err
	}
	if err := h.verifyCaptureReadiness(ctx); err != nil {
		return err
	}
	if !h.config.SkipAdapter {
		if err := h.startAdapter(ctx); err != nil {
			return err
		}
	}
	h.extensionUpdateCompleted = true
	return nil
}

func readExtensionVersion(ctx context.Context, database *sql.DB) (string, error) {
	var version string
	if err := database.QueryRowContext(ctx, `
		SELECT extversion FROM pg_catalog.pg_extension WHERE extname = 'synchro_pg'`,
	).Scan(&version); err != nil {
		return "", fmt.Errorf("read synchro_pg extension version failed: %w", err)
	}
	return version, nil
}

// ObserveExtensionCatalogs compares the updated extension in the harness
// database with a clean installation in the postgres database.
func (h *Harness) ObserveExtensionCatalogs(ctx context.Context) (ExtensionCatalogObservation, error) {
	if h == nil || ctx == nil || !h.extensionUpdateCompleted {
		return ExtensionCatalogObservation{}, errors.New("extension catalog observation is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return ExtensionCatalogObservation{}, errors.New("extension catalog observation context expired")
	}
	clean, err := h.openDatabase(ctx, "postgres", h.env.Admin, false)
	if err != nil {
		return ExtensionCatalogObservation{}, errors.New("connect for clean extension installation failed")
	}
	defer clean.Close()
	if !h.cleanExtension {
		if _, err := clean.ExecContext(ctx, "CREATE EXTENSION synchro_pg"); err != nil {
			return ExtensionCatalogObservation{}, fmt.Errorf("create clean synchro_pg extension failed: %w", err)
		}
		h.cleanExtension = true
	}
	updated, err := h.openDatabase(ctx, h.names.Database, h.env.Admin, false)
	if err != nil {
		return ExtensionCatalogObservation{}, errors.New("connect for updated extension observation failed")
	}
	defer updated.Close()
	var observation ExtensionCatalogObservation
	if observation.Updated, err = readExtensionCatalogSnapshot(ctx, updated); err != nil {
		return ExtensionCatalogObservation{}, fmt.Errorf("observe updated extension catalogs: %w", err)
	}
	if observation.Clean, err = readExtensionCatalogSnapshot(ctx, clean); err != nil {
		return ExtensionCatalogObservation{}, fmt.Errorf("observe clean extension catalogs: %w", err)
	}
	return observation, nil
}

// extensionMembersSQL selects each object that belongs to synchro_pg.
const extensionMembersSQL = `
WITH extension AS (
	SELECT extension.oid
	FROM pg_catalog.pg_extension extension
	WHERE extension.extname = 'synchro_pg'
), members AS (
	SELECT dependency.classid, dependency.objid, dependency.objsubid
	FROM pg_catalog.pg_depend dependency
	JOIN extension ON dependency.refobjid = extension.oid
	WHERE dependency.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
	  AND dependency.deptype = 'e'
), member_relations AS (
	SELECT relation.*
	FROM members
	JOIN pg_catalog.pg_class relation ON relation.oid = members.objid
	WHERE members.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
)
`

// extensionCatalogCheckSQL names each member kind that the snapshot does
// not describe.
const extensionCatalogCheckSQL = extensionMembersSQL + `
SELECT 'extension count ' || pg_catalog.count(*)::text
FROM pg_catalog.pg_extension
WHERE extname = 'synchro_pg'
HAVING pg_catalog.count(*) <> 1
UNION ALL
SELECT 'member catalog ' || members.classid::pg_catalog.regclass::text
FROM members
WHERE members.classid NOT IN (
	'pg_catalog.pg_namespace'::pg_catalog.regclass,
	'pg_catalog.pg_proc'::pg_catalog.regclass,
	'pg_catalog.pg_class'::pg_catalog.regclass,
	'pg_catalog.pg_type'::pg_catalog.regclass
)
UNION ALL
SELECT 'member function kind ' || procedure.prokind::text
FROM members
JOIN pg_catalog.pg_proc procedure ON procedure.oid = members.objid
WHERE members.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass
  AND procedure.prokind NOT IN ('f', 'p')
UNION ALL
SELECT 'member type kind ' || type.typtype::text
FROM members
JOIN pg_catalog.pg_type type ON type.oid = members.objid
WHERE members.classid = 'pg_catalog.pg_type'::pg_catalog.regclass
  AND type.typtype NOT IN ('c', 'e', 'b')
`

// extensionCatalogSnapshotSQL gives one line for each compared catalog fact.
// The ACL, option, and role lists are sorted so that their order does not
// change the line.
var extensionCatalogSnapshotSQL = map[string]string{
	"extension": `
SELECT pg_catalog.format('extension|relocatable=%s|namespace=%s|config=[%s]',
	extension.extrelocatable,
	extension.extnamespace::pg_catalog.regnamespace::text,
	COALESCE((
		SELECT pg_catalog.string_agg(pg_catalog.format('%s:%L', config.relation::text, config.condition), ',' ORDER BY config.relation::text COLLATE "C")
		FROM (
			SELECT relation::pg_catalog.regclass AS relation, condition
			FROM ROWS FROM (pg_catalog.unnest(extension.extconfig), pg_catalog.unnest(extension.extcondition)) AS entry(relation, condition)
		) config
	), ''))
FROM pg_catalog.pg_extension extension
WHERE extension.extname = 'synchro_pg'`,
	"member": extensionMembersSQL + `
SELECT pg_catalog.format('member|%s|%s',
	members.classid::pg_catalog.regclass::text,
	pg_catalog.pg_describe_object(members.classid, members.objid, members.objsubid))
FROM members`,
	"schema": extensionMembersSQL + `
SELECT pg_catalog.format('schema|%s|owner=%s|acl=%s',
	namespace.nspname,
	namespace.nspowner::pg_catalog.regrole::text,
	` + sortedACLSQL("namespace.nspacl") + `)
FROM members
JOIN pg_catalog.pg_namespace namespace ON namespace.oid = members.objid
WHERE members.classid = 'pg_catalog.pg_namespace'::pg_catalog.regclass`,
	"function": extensionMembersSQL + `
SELECT pg_catalog.format('function|%s|owner=%s|acl=%s|definition=%s',
	procedure.oid::pg_catalog.regprocedure::text,
	procedure.proowner::pg_catalog.regrole::text,
	` + sortedACLSQL("procedure.proacl") + `,
	pg_catalog.pg_get_functiondef(procedure.oid))
FROM members
JOIN pg_catalog.pg_proc procedure ON procedure.oid = members.objid
WHERE members.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass`,
	"relation": extensionMembersSQL + `
SELECT pg_catalog.format('relation|%s|kind=%s|owner=%s|acl=%s|options=%s|persistence=%s|rls=%s|force_rls=%s|replica_identity=%s|view=%L',
	relation.oid::pg_catalog.regclass::text,
	relation.relkind,
	relation.relowner::pg_catalog.regrole::text,
	` + sortedACLSQL("relation.relacl") + `,
	CASE WHEN relation.reloptions IS NULL THEN 'default' ELSE '[' || COALESCE((
		SELECT pg_catalog.string_agg(option, ',' ORDER BY option COLLATE "C")
		FROM pg_catalog.unnest(relation.reloptions) option
	), '') || ']' END,
	relation.relpersistence,
	relation.relrowsecurity,
	relation.relforcerowsecurity,
	relation.relreplident,
	CASE WHEN relation.relkind = 'v' THEN pg_catalog.pg_get_viewdef(relation.oid, true) END)
FROM member_relations relation`,
	"column": extensionMembersSQL + `
SELECT pg_catalog.format('column|%s|%s|type=%s|not_null=%s|identity=%L|generated=%L|default=%L|collation=%L|acl=%s',
	relation.oid::pg_catalog.regclass::text,
	attribute.attname,
	pg_catalog.format_type(attribute.atttypid, attribute.atttypmod),
	attribute.attnotnull,
	attribute.attidentity::text,
	attribute.attgenerated::text,
	pg_catalog.pg_get_expr(default_value.adbin, default_value.adrelid),
	CASE WHEN attribute.attcollation <> 0 THEN attribute.attcollation::pg_catalog.regcollation::text END,
	` + sortedACLSQL("attribute.attacl") + `)
FROM member_relations relation
JOIN pg_catalog.pg_attribute attribute ON attribute.attrelid = relation.oid
LEFT JOIN pg_catalog.pg_attrdef default_value
	ON default_value.adrelid = attribute.attrelid AND default_value.adnum = attribute.attnum
WHERE attribute.attnum > 0 AND NOT attribute.attisdropped`,
	"constraint": extensionMembersSQL + `
SELECT pg_catalog.format('constraint|%s|%s|%s',
	relation.oid::pg_catalog.regclass::text,
	constraint_row.conname,
	pg_catalog.pg_get_constraintdef(constraint_row.oid))
FROM member_relations relation
JOIN pg_catalog.pg_constraint constraint_row ON constraint_row.conrelid = relation.oid
WHERE relation.relkind IN ('r', 'p')`,
	"index": extensionMembersSQL + `
SELECT pg_catalog.format('index|%s|%s',
	relation.oid::pg_catalog.regclass::text,
	pg_catalog.pg_get_indexdef(index_row.indexrelid))
FROM member_relations relation
JOIN pg_catalog.pg_index index_row ON index_row.indrelid = relation.oid
WHERE relation.relkind IN ('r', 'p')`,
	"trigger": extensionMembersSQL + `
SELECT pg_catalog.format('trigger|%s|%s|enabled=%s|%s',
	relation.oid::pg_catalog.regclass::text,
	trigger_row.tgname,
	trigger_row.tgenabled,
	pg_catalog.pg_get_triggerdef(trigger_row.oid))
FROM member_relations relation
JOIN pg_catalog.pg_trigger trigger_row ON trigger_row.tgrelid = relation.oid
WHERE relation.relkind IN ('r', 'p') AND NOT trigger_row.tgisinternal`,
	"policy": extensionMembersSQL + `
SELECT pg_catalog.format('policy|%s|%s|command=%s|permissive=%s|roles=[%s]|using=%L|check=%L',
	relation.oid::pg_catalog.regclass::text,
	policy.polname,
	policy.polcmd,
	policy.polpermissive,
	COALESCE((
		SELECT pg_catalog.string_agg(role_name, ',' ORDER BY role_name COLLATE "C")
		FROM (
			SELECT CASE WHEN role_oid = 0 THEN 'public' ELSE role_oid::pg_catalog.regrole::text END AS role_name
			FROM pg_catalog.unnest(policy.polroles) role_oid
		) roles
	), ''),
	pg_catalog.pg_get_expr(policy.polqual, policy.polrelid),
	pg_catalog.pg_get_expr(policy.polwithcheck, policy.polrelid))
FROM member_relations relation
JOIN pg_catalog.pg_policy policy ON policy.polrelid = relation.oid
WHERE relation.relkind IN ('r', 'p')`,
	"sequence": extensionMembersSQL + `, sequences AS (
	SELECT relation.oid FROM member_relations relation
	UNION
	SELECT dependency.objid
	FROM member_relations relation
	JOIN pg_catalog.pg_depend dependency
		ON dependency.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass
	   AND dependency.refobjid = relation.oid
	   AND dependency.refobjsubid > 0
	   AND dependency.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
	   AND dependency.deptype IN ('a', 'i')
	WHERE relation.relkind IN ('r', 'p')
)
SELECT pg_catalog.format('sequence|%s|type=%s|start=%s|increment=%s|maximum=%s|minimum=%s|cache=%s|cycle=%s',
	sequence.seqrelid::pg_catalog.regclass::text,
	sequence.seqtypid::pg_catalog.regtype::text,
	sequence.seqstart,
	sequence.seqincrement,
	sequence.seqmax,
	sequence.seqmin,
	sequence.seqcache,
	sequence.seqcycle)
FROM sequences
JOIN pg_catalog.pg_sequence sequence ON sequence.seqrelid = sequences.oid`,
	"type": extensionMembersSQL + `
SELECT pg_catalog.format('type|%s|kind=%s|owner=%s|acl=%s|attributes=%L|labels=%L',
	type.oid::pg_catalog.regtype::text,
	type.typtype,
	type.typowner::pg_catalog.regrole::text,
	` + sortedACLSQL("type.typacl") + `,
	CASE WHEN type.typtype = 'c' THEN COALESCE((
		SELECT pg_catalog.string_agg(pg_catalog.format('%s:%s', attribute.attname, pg_catalog.format_type(attribute.atttypid, attribute.atttypmod)), ',' ORDER BY attribute.attnum)
		FROM pg_catalog.pg_attribute attribute
		WHERE attribute.attrelid = type.typrelid AND attribute.attnum > 0 AND NOT attribute.attisdropped
	), '') END,
	CASE WHEN type.typtype = 'e' THEN COALESCE((
		SELECT pg_catalog.string_agg(label.enumlabel, ',' ORDER BY label.enumsortorder)
		FROM pg_catalog.pg_enum label
		WHERE label.enumtypid = type.oid
	), '') END)
FROM members
JOIN pg_catalog.pg_type type ON type.oid = members.objid
WHERE members.classid = 'pg_catalog.pg_type'::pg_catalog.regclass`,
	"comment": extensionMembersSQL + `
SELECT pg_catalog.format('comment|%s|%L',
	pg_catalog.pg_describe_object(description.classoid, description.objoid, description.objsubid),
	description.description)
FROM pg_catalog.pg_description description
WHERE EXISTS (
	SELECT 1 FROM members
	WHERE members.classid = description.classoid AND members.objid = description.objoid
)`,
	"init_privs": extensionMembersSQL + `
SELECT pg_catalog.format('init_privs|%s|type=%s|acl=%s',
	pg_catalog.pg_describe_object(privilege.classoid, privilege.objoid, privilege.objsubid),
	privilege.privtype,
	` + sortedACLSQL("privilege.initprivs") + `)
FROM pg_catalog.pg_init_privs privilege
WHERE EXISTS (
	SELECT 1 FROM members
	WHERE members.classid = privilege.classoid AND members.objid = privilege.objoid
)`,
	"default_acl": `
SELECT pg_catalog.format('default_acl|role=%s|namespace=%s|type=%s|acl=%s',
	acl.defaclrole::pg_catalog.regrole::text,
	CASE WHEN acl.defaclnamespace = 0 THEN '' ELSE acl.defaclnamespace::pg_catalog.regnamespace::text END,
	acl.defaclobjtype,
	` + sortedACLSQL("acl.defaclacl") + `)
FROM pg_catalog.pg_default_acl acl`,
}

// sortedACLSQL renders an ACL as "default" when it is NULL. Otherwise it
// renders the sorted aclitem text values.
func sortedACLSQL(column string) string {
	return `CASE WHEN ` + column + ` IS NULL THEN 'default' ELSE '[' || COALESCE((
		SELECT pg_catalog.string_agg(item::text, ',' ORDER BY item::text COLLATE "C")
		FROM pg_catalog.unnest(` + column + `) item
	), '') || ']' END`
}

var extensionCatalogLineEscaper = strings.NewReplacer(`\`, `\\`, "\n", `\n`, "\r", `\r`, "\t", `\t`)

func readExtensionCatalogSnapshot(ctx context.Context, database *sql.DB) (_ []string, returnedErr error) {
	tx, err := database.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin extension catalog snapshot: %w", err)
	}
	defer func() {
		returnedErr = errors.Join(returnedErr, tx.Rollback())
	}()
	// A fixed search path makes each rendered name independent of role and
	// database settings.
	if _, err := tx.ExecContext(ctx, "SET LOCAL search_path = pg_catalog"); err != nil {
		return nil, fmt.Errorf("set extension catalog search path: %w", err)
	}
	unsupported, err := queryExtensionCatalogLines(ctx, tx, extensionCatalogCheckSQL)
	if err != nil {
		return nil, fmt.Errorf("check extension members: %w", err)
	}
	if len(unsupported) != 0 {
		sort.Strings(unsupported)
		return nil, fmt.Errorf("extension snapshot does not support %s", strings.Join(unsupported, ", "))
	}
	kinds := make([]string, 0, len(extensionCatalogSnapshotSQL))
	for kind := range extensionCatalogSnapshotSQL {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	var lines []string
	for _, kind := range kinds {
		kindLines, err := queryExtensionCatalogLines(ctx, tx, extensionCatalogSnapshotSQL[kind])
		if err != nil {
			return nil, fmt.Errorf("read extension %s catalog: %w", kind, err)
		}
		lines = append(lines, kindLines...)
	}
	sort.Strings(lines)
	return lines, nil
}

func queryExtensionCatalogLines(ctx context.Context, tx *sql.Tx, query string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return nil, err
		}
		lines = append(lines, extensionCatalogLineEscaper.Replace(line))
	}
	return lines, rows.Err()
}
