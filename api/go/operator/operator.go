// Package operator coordinates projection bootstrap operations.
package operator

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

const (
	streamResetOperatorLockKey int64 = 0x7273_746f
	pollInterval                     = 50 * time.Millisecond
	cleanupTimeout                   = 10 * time.Second
)

// Config contains the connections for one coordinator.
type Config struct {
	OperatorDB        *sql.DB
	WorkerDB          *sql.DB
	ReplicationConfig *pgconn.Config
}

// Coordinator runs projection bootstrap operations.
type Coordinator struct {
	operatorDB        *sql.DB
	workerDB          *sql.DB
	replicationConfig *pgconn.Config
}

type queryRower interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type workerSlotConnection interface {
	queryRower
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

type recoveryPlan struct {
	retiredSlotName string
	activated       bool
}

type interruptedProjectionBootstrap struct {
	bootstrapID              string
	sourceStreamGeneration   string
	targetStreamGeneration   string
	sourceRegistryGeneration int64
	registryGeneration       sql.NullInt64
	activeSlotName           string
	candidateSlotName        string
	schemaVersion            sql.NullInt64
	schemaHash               sql.NullString
	activationBarrier        sql.NullString
	affectedScopesJSON       string
	lifecycle                string
}

// New creates a projection bootstrap coordinator.
func New(config Config) (*Coordinator, error) {
	if config.OperatorDB == nil || config.WorkerDB == nil || config.ReplicationConfig == nil {
		return nil, errors.New("projection bootstrap dependencies are required")
	}
	if config.OperatorDB == config.WorkerDB {
		return nil, errors.New("projection bootstrap database roles must be separate")
	}
	replicationConfig := config.ReplicationConfig.Copy()
	if replicationConfig.User == "" || replicationConfig.Database == "" {
		return nil, errors.New("projection bootstrap replication identity is required")
	}
	if replicationConfig.RuntimeParams == nil {
		replicationConfig.RuntimeParams = make(map[string]string)
	}
	replicationConfig.RuntimeParams["replication"] = "database"
	return &Coordinator{
		operatorDB:        config.OperatorDB,
		workerDB:          config.WorkerDB,
		replicationConfig: replicationConfig,
	}, nil
}

// RunProjectionBootstrap activates one pending Class 3 registry generation.
func (coordinator *Coordinator) RunProjectionBootstrap(ctx context.Context, registryGeneration int64) (_ ProjectionBootstrapResult, returnedErr error) {
	if coordinator == nil || coordinator.operatorDB == nil || coordinator.workerDB == nil || coordinator.replicationConfig == nil {
		return ProjectionBootstrapResult{}, errors.New("projection bootstrap coordinator is unavailable")
	}
	if ctx == nil || registryGeneration <= 0 {
		return ProjectionBootstrapResult{}, errors.New("projection bootstrap input is invalid")
	}
	if err := coordinator.verifyOperatorIdentity(ctx); err != nil {
		return ProjectionBootstrapResult{}, err
	}
	workerState, workerSlots, err := coordinator.acquireWorkerConnections(ctx)
	if err != nil {
		return ProjectionBootstrapResult{}, err
	}
	defer func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		returnedErr = errors.Join(returnedErr, releaseWorkerConnections(cleanupContext, workerState, workerSlots))
	}()
	operationLock, err := coordinator.acquireOperationLock(ctx)
	if err != nil {
		return ProjectionBootstrapResult{}, err
	}
	defer func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		returnedErr = errors.Join(returnedErr, releaseOperationLock(cleanupContext, operationLock))
	}()
	if err := cleanupAbortedProjectionBootstrapSlots(ctx, workerState, workerSlots); err != nil {
		return ProjectionBootstrapResult{}, err
	}

	recovered, err := coordinator.recoverInterruptedProjectionBootstrap(
		ctx,
		operationLock,
		workerState,
		workerSlots,
		registryGeneration,
	)
	if err != nil {
		return ProjectionBootstrapResult{}, err
	}
	if recovered != nil {
		if recovered.RegistryGeneration != registryGeneration {
			return ProjectionBootstrapResult{}, errors.New("recovered projection bootstrap generation differs")
		}
		return *recovered, nil
	}
	if err := downgradeOperationLock(ctx, operationLock); err != nil {
		return ProjectionBootstrapResult{}, err
	}

	activeSlot, sourceStreamGeneration, err := loadActiveStream(ctx, workerState)
	if err != nil {
		return ProjectionBootstrapResult{}, err
	}
	candidateSlot, err := newCandidateSlotName(activeSlot)
	if err != nil {
		return ProjectionBootstrapResult{}, err
	}
	if err := requireReplicationSlotAbsent(ctx, workerSlots, candidateSlot); err != nil {
		return ProjectionBootstrapResult{}, err
	}
	var preparedRaw []byte
	if err := coordinator.operatorDB.QueryRowContext(
		ctx,
		"SELECT synchro.synchro_prepare_projection_bootstrap($1, $2)",
		registryGeneration,
		candidateSlot,
	).Scan(&preparedRaw); err != nil {
		return ProjectionBootstrapResult{}, errors.New("prepare projection bootstrap failed")
	}
	prepared, err := parsePrepared(preparedRaw, registryGeneration, candidateSlot)
	if err != nil {
		if uuidPattern.MatchString(prepared.BootstrapID) {
			cleanupContext, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
			defer cancel()
			cleanupErr := coordinator.abortProjectionBootstrap(
				cleanupContext,
				coordinator.operatorDB,
				prepared.BootstrapID,
				candidateSlot,
			)
			return ProjectionBootstrapResult{}, errors.Join(err, cleanupErr)
		}
		return ProjectionBootstrapResult{}, err
	}

	var sourceLockConnection *sql.Conn
	var snapshotTransaction *sql.Tx
	var activationTransaction *sql.Tx
	var replicationConnection *pgconn.PgConn
	slotCreated := false
	activationAttempted := false
	activated := false
	defer func() {
		if returnedErr == nil {
			return
		}
		cleanupContext, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		var cleanupErrors []error
		if activationTransaction != nil {
			if err := activationTransaction.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
				cleanupErrors = append(cleanupErrors, errors.New("rollback projection bootstrap activation failed"))
			}
		}
		if snapshotTransaction != nil {
			if err := snapshotTransaction.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
				cleanupErrors = append(cleanupErrors, errors.New("rollback projection bootstrap snapshot failed"))
			}
		}
		if replicationConnection != nil {
			if err := replicationConnection.Close(cleanupContext); err != nil {
				cleanupErrors = append(cleanupErrors, errors.New("close projection bootstrap replication connection failed"))
			}
		}

		activationStateKnown := !activationAttempted || activated
		if activationAttempted && !activated {
			observed, observeErr := projectionBootstrapIsActivated(
				cleanupContext,
				workerState,
				prepared.BootstrapID,
			)
			if observeErr != nil {
				cleanupErrors = append(cleanupErrors, observeErr)
				activationStateKnown = false
			} else {
				activated = observed
				activationStateKnown = true
			}
		}
		if activated {
			if err := dropInactiveReplicationSlot(cleanupContext, workerSlots, candidateSlot); err != nil {
				cleanupErrors = append(cleanupErrors, errors.New("drop activated projection bootstrap candidate slot failed"))
			} else if err := completeProjectionBootstrapCleanup(
				cleanupContext,
				coordinator.operatorDB,
				prepared.BootstrapID,
			); err != nil {
				cleanupErrors = append(cleanupErrors, errors.New("complete activated projection bootstrap cleanup failed"))
			}
		} else if activationStateKnown {
			if err := coordinator.abortProjectionBootstrap(
				cleanupContext,
				coordinator.operatorDB,
				prepared.BootstrapID,
				candidateSlot,
			); err != nil {
				cleanupErrors = append(cleanupErrors, errors.New("abort failed projection bootstrap failed"))
			}
			if slotCreated {
				if err := dropInactiveReplicationSlot(cleanupContext, workerSlots, candidateSlot); err != nil {
					cleanupErrors = append(cleanupErrors, errors.New("drop failed projection bootstrap candidate slot failed"))
				}
			}
		}
		if err := closeSourceLocks(cleanupContext, sourceLockConnection); err != nil {
			cleanupErrors = append(cleanupErrors, err)
		}
		returnedErr = errors.Join(returnedErr, errors.Join(cleanupErrors...))
	}()

	sourceLockConnection, err = coordinator.operatorDB.Conn(ctx)
	if err != nil {
		return ProjectionBootstrapResult{}, errors.New("open projection bootstrap source lock connection failed")
	}
	var locked bool
	if err := sourceLockConnection.QueryRowContext(
		ctx,
		"SELECT synchro.synchro_lock_stream_reset_sources($1::uuid)",
		prepared.BootstrapID,
	).Scan(&locked); err != nil || !locked {
		return ProjectionBootstrapResult{}, errors.New("lock projection bootstrap sources failed")
	}
	beforeMarker, err := coordinator.markSnapshot(ctx, prepared.BootstrapID, "before")
	if err != nil {
		return ProjectionBootstrapResult{}, err
	}

	replicationConnection, err = pgconn.ConnectConfig(ctx, coordinator.replicationConfig.Copy())
	if err != nil {
		return ProjectionBootstrapResult{}, errors.New("open projection bootstrap replication connection failed")
	}
	consistentPoint, snapshotName, err := createExportedSnapshotSlot(ctx, replicationConnection, candidateSlot)
	if err != nil {
		return ProjectionBootstrapResult{}, err
	}
	slotCreated = true
	afterMarker, err := coordinator.markSnapshot(ctx, prepared.BootstrapID, "after")
	if err != nil {
		return ProjectionBootstrapResult{}, err
	}

	snapshotTransaction, err = coordinator.operatorDB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return ProjectionBootstrapResult{}, errors.New("begin projection bootstrap snapshot transaction failed")
	}
	if _, err := snapshotTransaction.ExecContext(ctx, "SET TRANSACTION SNAPSHOT "+quotePostgresLiteral(snapshotName)); err != nil {
		return ProjectionBootstrapResult{}, errors.New("import projection bootstrap snapshot failed")
	}
	var stagedRaw []byte
	if err := snapshotTransaction.QueryRowContext(
		ctx,
		"SELECT synchro.synchro_stage_projection_bootstrap($1::uuid, $2, $3, $4, $5, $6::uuid, $7, $8::uuid)",
		prepared.BootstrapID,
		candidateSlot,
		consistentPoint,
		snapshotName,
		beforeMarker.XID,
		beforeMarker.Nonce,
		afterMarker.XID,
		afterMarker.Nonce,
	).Scan(&stagedRaw); err != nil {
		return ProjectionBootstrapResult{}, errors.New("stage projection bootstrap baseline failed")
	}
	if err := parseStage(stagedRaw, prepared.BootstrapID, registryGeneration, candidateSlot, consistentPoint); err != nil {
		return ProjectionBootstrapResult{}, err
	}
	if err := snapshotTransaction.Commit(); err != nil {
		return ProjectionBootstrapResult{}, errors.New("commit projection bootstrap baseline failed")
	}
	snapshotTransaction = nil
	if err := replicationConnection.Close(ctx); err != nil {
		return ProjectionBootstrapResult{}, errors.New("close projection bootstrap replication connection failed")
	}
	replicationConnection = nil
	if err := closeSourceLocks(ctx, sourceLockConnection); err != nil {
		return ProjectionBootstrapResult{}, err
	}
	sourceLockConnection = nil

	markerLSN, err := coordinator.emitProjectionBootstrapWALMarker(ctx, prepared.BootstrapID)
	if err != nil {
		return ProjectionBootstrapResult{}, err
	}
	if err := waitForMainBoundary(ctx, workerState, sourceStreamGeneration, markerLSN); err != nil {
		return ProjectionBootstrapResult{}, err
	}
	var barrierRaw []byte
	if err := coordinator.operatorDB.QueryRowContext(
		ctx,
		"SELECT synchro.synchro_request_projection_bootstrap_barrier($1::uuid)",
		prepared.BootstrapID,
	).Scan(&barrierRaw); err != nil {
		return ProjectionBootstrapResult{}, errors.New("request projection bootstrap barrier failed")
	}
	barrier, err := parseBarrier(barrierRaw, prepared.BootstrapID, sourceStreamGeneration)
	if err != nil {
		return ProjectionBootstrapResult{}, err
	}
	if err := coordinator.waitForCandidate(ctx, prepared.BootstrapID, candidateSlot, barrier); err != nil {
		return ProjectionBootstrapResult{}, err
	}

	activationTransaction, err = coordinator.operatorDB.BeginTx(ctx, nil)
	if err != nil {
		return ProjectionBootstrapResult{}, errors.New("begin projection bootstrap activation transaction failed")
	}
	activationAttempted = true
	var activatedRaw []byte
	if err := activationTransaction.QueryRowContext(
		ctx,
		"SELECT synchro.synchro_activate_projection_bootstrap($1::uuid)",
		prepared.BootstrapID,
	).Scan(&activatedRaw); err != nil {
		return ProjectionBootstrapResult{}, errors.New("activate projection bootstrap failed")
	}
	result, err := parseActivation(
		activatedRaw,
		prepared.BootstrapID,
		registryGeneration,
		candidateSlot,
		barrier,
	)
	if err != nil {
		return ProjectionBootstrapResult{}, err
	}
	if err := activationTransaction.Commit(); err != nil {
		return ProjectionBootstrapResult{}, errors.New("commit projection bootstrap activation failed")
	}
	activationTransaction = nil
	activated = true
	if err := dropInactiveReplicationSlot(ctx, workerSlots, candidateSlot); err != nil {
		return ProjectionBootstrapResult{}, errors.New("retire projection bootstrap candidate slot failed")
	}
	if err := completeProjectionBootstrapCleanup(ctx, coordinator.operatorDB, prepared.BootstrapID); err != nil {
		return ProjectionBootstrapResult{}, errors.New("complete projection bootstrap cleanup failed")
	}
	result.SourceStreamGeneration = sourceStreamGeneration
	result.ActiveSlotName = activeSlot
	return result, nil
}

func (coordinator *Coordinator) verifyOperatorIdentity(ctx context.Context) error {
	var login string
	var database string
	var authorized bool
	if err := coordinator.operatorDB.QueryRowContext(ctx, `
		WITH login AS (
			SELECT oid, rolcanlogin, rolreplication, rolsuper, rolcreatedb,
			       rolcreaterole, rolbypassrls
			FROM pg_catalog.pg_roles
			WHERE rolname = session_user
		), operator_group AS (
			SELECT oid, rolcanlogin, rolreplication, rolsuper, rolcreatedb,
			       rolcreaterole, rolbypassrls
			FROM pg_catalog.pg_roles
			WHERE rolname = 'synchro_operator'
		)
		SELECT session_user::text, pg_catalog.current_database()::text,
		       current_user = session_user
		       AND login.rolcanlogin AND NOT login.rolreplication
		       AND NOT login.rolsuper AND NOT login.rolcreatedb
		       AND NOT login.rolcreaterole AND NOT login.rolbypassrls
		       AND NOT operator_group.rolcanlogin AND NOT operator_group.rolreplication
		       AND NOT operator_group.rolsuper AND NOT operator_group.rolcreatedb
		       AND NOT operator_group.rolcreaterole AND NOT operator_group.rolbypassrls
		       AND (SELECT count(*) = 1 AND bool_and(membership.roleid = operator_group.oid)
		            FROM pg_catalog.pg_auth_members membership
		            WHERE membership.member = login.oid)
		       AND pg_catalog.pg_has_role(login.oid, operator_group.oid, 'MEMBER')
		       AND NOT EXISTS (
		           SELECT 1 FROM pg_catalog.pg_roles other_role
		           WHERE other_role.oid <> login.oid
		             AND other_role.oid <> operator_group.oid
		             AND pg_catalog.pg_has_role(login.oid, other_role.oid, 'MEMBER')
		       )
		FROM login CROSS JOIN operator_group`).Scan(&login, &database, &authorized); err != nil ||
		!authorized || login == coordinator.replicationConfig.User || database != coordinator.replicationConfig.Database {
		return errors.New("projection bootstrap operator identity is invalid")
	}
	return nil
}

func (coordinator *Coordinator) acquireOperationLock(ctx context.Context) (*sql.Conn, error) {
	connection, err := coordinator.operatorDB.Conn(ctx)
	if err != nil {
		return nil, errors.New("open projection bootstrap operation lock connection failed")
	}
	var locked bool
	if err := connection.QueryRowContext(
		ctx,
		"SELECT pg_catalog.pg_try_advisory_lock($1::bigint)",
		streamResetOperatorLockKey,
	).Scan(&locked); err != nil || !locked {
		_ = connection.Close()
		return nil, errors.New("another candidate operation is active")
	}
	return connection, nil
}

func releaseOperationLock(ctx context.Context, connection *sql.Conn) error {
	if connection == nil {
		return nil
	}
	var cleanupErrors []error
	if _, err := connection.ExecContext(ctx, "SELECT pg_catalog.pg_advisory_unlock_all()"); err != nil {
		cleanupErrors = append(cleanupErrors, errors.New("unlock projection bootstrap operation failed"))
		discardSQLConnection(connection)
		return errors.Join(cleanupErrors...)
	}
	if err := connection.Close(); err != nil {
		cleanupErrors = append(cleanupErrors, errors.New("close projection bootstrap operation lock connection failed"))
	}
	return errors.Join(cleanupErrors...)
}

func downgradeOperationLock(ctx context.Context, connection *sql.Conn) error {
	if _, err := connection.ExecContext(
		ctx,
		"SELECT pg_catalog.pg_advisory_lock_shared($1::bigint)",
		streamResetOperatorLockKey,
	); err != nil {
		discardSQLConnection(connection)
		return errors.New("acquire shared projection bootstrap operation lock failed")
	}
	var unlocked bool
	if err := connection.QueryRowContext(
		ctx,
		"SELECT pg_catalog.pg_advisory_unlock($1::bigint)",
		streamResetOperatorLockKey,
	).Scan(&unlocked); err != nil || !unlocked {
		discardSQLConnection(connection)
		return errors.New("release exclusive projection bootstrap operation lock failed")
	}
	return nil
}

func (coordinator *Coordinator) acquireWorkerConnections(ctx context.Context) (*sql.Conn, *sql.Conn, error) {
	stateConnection, err := coordinator.workerDB.Conn(ctx)
	if err != nil {
		return nil, nil, errors.New("open projection bootstrap worker state connection failed")
	}
	if _, err := stateConnection.ExecContext(ctx, "SET ROLE synchro_worker"); err != nil {
		discardSQLConnection(stateConnection)
		return nil, nil, errors.New("activate projection bootstrap worker role failed")
	}
	slotConnection, err := coordinator.workerDB.Conn(ctx)
	if err != nil {
		cleanupContext, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		if _, resetErr := stateConnection.ExecContext(cleanupContext, "RESET ROLE"); resetErr != nil {
			discardSQLConnection(stateConnection)
		} else {
			_ = stateConnection.Close()
		}
		return nil, nil, errors.New("open projection bootstrap worker slot connection failed")
	}
	if _, err := slotConnection.ExecContext(ctx, "RESET ROLE"); err != nil {
		discardSQLConnection(slotConnection)
		cleanupContext, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		if _, resetErr := stateConnection.ExecContext(cleanupContext, "RESET ROLE"); resetErr != nil {
			discardSQLConnection(stateConnection)
		} else {
			_ = stateConnection.Close()
		}
		return nil, nil, errors.New("activate projection bootstrap worker login failed")
	}
	var workerLogin string
	var workerDatabase string
	var workerAuthorized bool
	if err := slotConnection.QueryRowContext(ctx, `
		WITH login AS (
			SELECT oid, rolcanlogin, rolreplication, rolinherit, rolsuper,
			       rolcreatedb, rolcreaterole, rolbypassrls
			FROM pg_catalog.pg_roles WHERE rolname = session_user
		), worker_group AS (
			SELECT oid, rolcanlogin, rolreplication, rolsuper,
			       rolcreatedb, rolcreaterole, rolbypassrls
			FROM pg_catalog.pg_roles WHERE rolname = 'synchro_worker'
		)
		SELECT session_user::text, pg_catalog.current_database()::text,
		       current_user = session_user
		       AND login.rolcanlogin AND login.rolreplication AND NOT login.rolinherit
		       AND NOT login.rolsuper AND NOT login.rolcreatedb
		       AND NOT login.rolcreaterole AND NOT login.rolbypassrls
		       AND NOT worker_group.rolcanlogin AND NOT worker_group.rolreplication
		       AND NOT worker_group.rolsuper AND NOT worker_group.rolcreatedb
		       AND NOT worker_group.rolcreaterole AND NOT worker_group.rolbypassrls
		       AND (SELECT count(*) = 1 AND bool_and(membership.roleid = worker_group.oid)
		            FROM pg_catalog.pg_auth_members membership
		            WHERE membership.member = login.oid)
		       AND pg_catalog.pg_has_role(login.oid, worker_group.oid, 'SET')
		       AND NOT EXISTS (
		           SELECT 1 FROM pg_catalog.pg_roles other_role
		           WHERE other_role.oid <> login.oid
		             AND other_role.oid <> worker_group.oid
		             AND pg_catalog.pg_has_role(login.oid, other_role.oid, 'MEMBER')
		       )
		       AND NOT EXISTS (
		           SELECT 1 FROM pg_catalog.pg_roles other_login
		           WHERE other_login.rolcanlogin AND other_login.rolreplication
		             AND NOT other_login.rolsuper AND other_login.oid <> login.oid
		       )
		FROM login CROSS JOIN worker_group`).Scan(
		&workerLogin,
		&workerDatabase,
		&workerAuthorized,
	); err != nil ||
		!workerAuthorized ||
		workerLogin != coordinator.replicationConfig.User ||
		workerDatabase != coordinator.replicationConfig.Database {
		_ = slotConnection.Close()
		cleanupContext, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		if _, resetErr := stateConnection.ExecContext(cleanupContext, "RESET ROLE"); resetErr != nil {
			discardSQLConnection(stateConnection)
		} else {
			_ = stateConnection.Close()
		}
		return nil, nil, errors.New("projection bootstrap worker identity is invalid")
	}
	return stateConnection, slotConnection, nil
}

func releaseWorkerConnections(ctx context.Context, stateConnection, slotConnection *sql.Conn) error {
	var cleanupErrors []error
	if stateConnection != nil {
		if _, err := stateConnection.ExecContext(ctx, "RESET ROLE"); err != nil {
			cleanupErrors = append(cleanupErrors, errors.New("reset projection bootstrap worker role failed"))
			discardSQLConnection(stateConnection)
		} else if err := stateConnection.Close(); err != nil {
			cleanupErrors = append(cleanupErrors, errors.New("close projection bootstrap worker state connection failed"))
		}
	}
	if slotConnection != nil {
		if err := slotConnection.Close(); err != nil {
			cleanupErrors = append(cleanupErrors, errors.New("close projection bootstrap worker slot connection failed"))
		}
	}
	return errors.Join(cleanupErrors...)
}

func discardSQLConnection(connection *sql.Conn) {
	_ = connection.Raw(func(any) error { return driver.ErrBadConn })
	_ = connection.Close()
}

func loadActiveStream(ctx context.Context, workerState queryRower) (string, string, error) {
	var activeSlot string
	var streamGeneration string
	if err := workerState.QueryRowContext(
		ctx,
		"SELECT active_slot_name::text, stream_generation FROM synchro.sync_runtime_state WHERE singleton",
	).Scan(&activeSlot, &streamGeneration); err != nil || !validSlotName(activeSlot) || streamGeneration == "" {
		return "", "", errors.New("load active projection bootstrap stream failed")
	}
	return activeSlot, streamGeneration, nil
}

func (coordinator *Coordinator) markSnapshot(ctx context.Context, bootstrapID, phase string) (snapshotMarker, error) {
	var raw []byte
	if err := coordinator.operatorDB.QueryRowContext(
		ctx,
		"SELECT synchro.synchro_mark_stream_reset_snapshot($1::uuid, $2)",
		bootstrapID,
		phase,
	).Scan(&raw); err != nil {
		return snapshotMarker{}, errors.New("mark projection bootstrap snapshot failed")
	}
	return parseSnapshotMarker(raw)
}

func (coordinator *Coordinator) emitProjectionBootstrapWALMarker(ctx context.Context, bootstrapID string) (string, error) {
	var raw []byte
	if err := coordinator.operatorDB.QueryRowContext(
		ctx,
		"SELECT synchro.synchro_emit_projection_bootstrap_barrier($1::uuid)",
		bootstrapID,
	).Scan(&raw); err != nil {
		return "", errors.New("emit projection bootstrap WAL marker failed")
	}
	return parseWALMarker(raw, bootstrapID)
}

func createExportedSnapshotSlot(ctx context.Context, connection *pgconn.PgConn, slotName string) (string, string, error) {
	if connection == nil || !validSlotName(slotName) {
		return "", "", errors.New("projection bootstrap replication slot input is invalid")
	}
	results, err := connection.Exec(
		ctx,
		"CREATE_REPLICATION_SLOT \""+slotName+"\" LOGICAL pgoutput EXPORT_SNAPSHOT",
	).ReadAll()
	if err != nil || len(results) != 1 || len(results[0].Rows) != 1 || len(results[0].Rows[0]) != 4 {
		return "", "", errors.New("create projection bootstrap replication slot failed")
	}
	row := results[0].Rows[0]
	consistentPoint := string(row[1])
	snapshotName := string(row[2])
	if string(row[0]) != slotName || consistentPoint == "" || !validSnapshotName(snapshotName) || string(row[3]) != "pgoutput" {
		return "", "", errors.New("projection bootstrap replication slot response is invalid")
	}
	return consistentPoint, snapshotName, nil
}

func quotePostgresLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func closeSourceLocks(ctx context.Context, connection *sql.Conn) error {
	if connection == nil {
		return nil
	}
	var cleanupErrors []error
	if _, err := connection.ExecContext(ctx, "SELECT pg_catalog.pg_advisory_unlock_all()"); err != nil {
		cleanupErrors = append(cleanupErrors, errors.New("unlock projection bootstrap sources failed"))
		discardSQLConnection(connection)
		return errors.Join(cleanupErrors...)
	}
	if err := connection.Close(); err != nil {
		cleanupErrors = append(cleanupErrors, errors.New("close projection bootstrap source lock connection failed"))
	}
	return errors.Join(cleanupErrors...)
}

func waitForMainBoundary(ctx context.Context, workerState queryRower, streamGeneration, markerLSN string) error {
	err := waitUntil(ctx, func(attemptContext context.Context) (bool, error) {
		var ready bool
		if err := workerState.QueryRowContext(attemptContext, `
			SELECT COALESCE(stream_generation = $1 AND materialized_end_lsn >= $2::pg_lsn, false)
			FROM synchro.sync_wal_progress
			WHERE singleton`, streamGeneration, markerLSN).Scan(&ready); err != nil {
			return false, errors.New("read main projection bootstrap boundary failed")
		}
		return ready, nil
	})
	if err != nil {
		return fmt.Errorf("wait for main projection bootstrap boundary failed: %w", err)
	}
	return nil
}

func (coordinator *Coordinator) waitForCandidate(ctx context.Context, bootstrapID, candidateSlot, barrier string) error {
	err := waitUntil(ctx, func(attemptContext context.Context) (bool, error) {
		var raw []byte
		if err := coordinator.operatorDB.QueryRowContext(
			attemptContext,
			"SELECT synchro.synchro_projection_bootstrap_status($1::uuid)",
			bootstrapID,
		).Scan(&raw); err != nil {
			return false, errors.New("read projection bootstrap status failed")
		}
		status, err := parseStatus(raw, bootstrapID, candidateSlot)
		if err != nil {
			return false, err
		}
		return candidateReady(status, barrier), nil
	})
	if err != nil {
		return fmt.Errorf("wait for projection bootstrap candidate failed: %w", err)
	}
	return nil
}

func waitUntil(ctx context.Context, condition func(context.Context) (bool, error)) error {
	if ctx == nil {
		return errors.New("wait context is required")
	}
	for {
		if ctx.Err() != nil {
			return errors.New("projection bootstrap wait stopped")
		}
		ready, err := condition(ctx)
		if err != nil {
			return err
		}
		if ready {
			return nil
		}
		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return errors.New("projection bootstrap wait stopped")
		case <-timer.C:
		}
	}
}

func dropInactiveReplicationSlot(ctx context.Context, workerSlots workerSlotConnection, slotName string) error {
	if !validSlotName(slotName) {
		return errors.New("projection bootstrap candidate slot is invalid")
	}
	return waitUntil(ctx, func(attemptContext context.Context) (bool, error) {
		var activePID sql.NullInt64
		var valid bool
		err := workerSlots.QueryRowContext(
			attemptContext,
			`SELECT active_pid,
			        slot_type = 'logical' AND plugin = 'pgoutput'
			        AND datoid = (SELECT oid FROM pg_catalog.pg_database
			                     WHERE datname = pg_catalog.current_database())
			        AND NOT temporary AS valid
			 FROM pg_catalog.pg_replication_slots WHERE slot_name = $1`,
			slotName,
		).Scan(&activePID, &valid)
		if errors.Is(err, sql.ErrNoRows) {
			return true, nil
		}
		if err != nil {
			return false, errors.New("inspect projection bootstrap candidate slot failed")
		}
		if !valid {
			return false, errors.New("projection bootstrap candidate slot binding is invalid")
		}
		if activePID.Valid {
			return false, nil
		}
		if _, err := workerSlots.ExecContext(
			attemptContext,
			"SELECT pg_catalog.pg_drop_replication_slot($1)",
			slotName,
		); err != nil {
			var postgresError *pgconn.PgError
			if errors.As(err, &postgresError) && postgresError.Code == "55006" {
				return false, nil
			}
			return false, errors.New("drop projection bootstrap candidate slot failed")
		}
		return true, nil
	})
}

func requireReplicationSlotAbsent(ctx context.Context, workerSlots queryRower, slotName string) error {
	var present bool
	if err := workerSlots.QueryRowContext(
		ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = $1)",
		slotName,
	).Scan(&present); err != nil {
		return errors.New("inspect projection bootstrap candidate slot failed")
	}
	if present {
		return errors.New("projection bootstrap candidate slot already exists")
	}
	return nil
}

func cleanupAbortedProjectionBootstrapSlots(ctx context.Context, workerState queryRower, workerSlots workerSlotConnection) error {
	for {
		var slotName string
		err := workerState.QueryRowContext(ctx, `
			SELECT reset.candidate_slot_name::text
			FROM synchro.sync_stream_resets reset
			JOIN pg_catalog.pg_replication_slots slot
			  ON slot.slot_name = reset.candidate_slot_name::text
			 AND slot.slot_type = 'logical'
			 AND slot.plugin = reset.plugin
			 AND slot.datoid = reset.database_oid
			 AND NOT slot.temporary
			WHERE reset.operation_kind = 'projection_bootstrap'
			  AND reset.lifecycle = 'aborted'
			ORDER BY reset.aborted_at, reset.reset_id
			LIMIT 1`).Scan(&slotName)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil || !validSlotName(slotName) {
			return errors.New("load aborted projection bootstrap slot failed")
		}
		if err := dropInactiveReplicationSlot(ctx, workerSlots, slotName); err != nil {
			return errors.New("discard aborted projection bootstrap slot failed")
		}
	}
}

func projectionBootstrapIsActivated(ctx context.Context, workerState queryRower, bootstrapID string) (bool, error) {
	var activated bool
	if err := workerState.QueryRowContext(ctx, `
		SELECT lifecycle IN ('activated', 'cleanup_complete')
		FROM synchro.sync_stream_resets
		WHERE reset_id = $1::uuid AND operation_kind = 'projection_bootstrap'`, bootstrapID).Scan(&activated); err != nil {
		return false, errors.New("read projection bootstrap activation state failed")
	}
	return activated, nil
}

func (coordinator *Coordinator) abortProjectionBootstrap(ctx context.Context, querier queryRower, bootstrapID, candidateSlot string) error {
	var raw []byte
	if err := querier.QueryRowContext(
		ctx,
		"SELECT synchro.synchro_abort_projection_bootstrap($1::uuid)",
		bootstrapID,
	).Scan(&raw); err != nil {
		return errors.New("abort projection bootstrap failed")
	}
	return parseAbort(raw, bootstrapID, candidateSlot)
}

func completeProjectionBootstrapCleanup(ctx context.Context, querier queryRower, bootstrapID string) error {
	var complete bool
	if err := querier.QueryRowContext(
		ctx,
		"SELECT synchro.synchro_complete_projection_bootstrap_cleanup($1::uuid)",
		bootstrapID,
	).Scan(&complete); err != nil || !complete {
		return errors.New("complete projection bootstrap cleanup failed")
	}
	return nil
}

func projectionBootstrapRecoveryPlan(lifecycle, activeSlot, candidateSlot string) (recoveryPlan, error) {
	if !validSlotName(activeSlot) || !validSlotName(candidateSlot) || activeSlot == candidateSlot {
		return recoveryPlan{}, errors.New("interrupted projection bootstrap slots are invalid")
	}
	plan := recoveryPlan{retiredSlotName: candidateSlot}
	switch lifecycle {
	case "preparing", "baseline_staged", "catching_up":
		return plan, nil
	case "activated":
		plan.activated = true
		return plan, nil
	default:
		return recoveryPlan{}, errors.New("interrupted projection bootstrap lifecycle is invalid")
	}
}

func (coordinator *Coordinator) recoverInterruptedProjectionBootstrap(
	ctx context.Context,
	operationLock *sql.Conn,
	workerState queryRower,
	workerSlots workerSlotConnection,
	requestedRegistryGeneration int64,
) (*ProjectionBootstrapResult, error) {
	var interrupted interruptedProjectionBootstrap
	err := workerState.QueryRowContext(ctx, `
		SELECT reset_id::text, source_stream_generation, target_stream_generation,
		       source_registry_generation, target_registry_generation,
		       old_slot_name::text, candidate_slot_name::text,
		       target_schema_version, target_schema_hash, activation_barrier::text,
		       COALESCE(to_jsonb(affected_scopes), '[]'::jsonb)::text,
		       lifecycle
		FROM synchro.sync_stream_resets
		WHERE operation_kind = 'projection_bootstrap'
		  AND lifecycle IN ('preparing', 'baseline_staged', 'catching_up', 'activated')
		ORDER BY prepared_at
		LIMIT 1`).Scan(
		&interrupted.bootstrapID,
		&interrupted.sourceStreamGeneration,
		&interrupted.targetStreamGeneration,
		&interrupted.sourceRegistryGeneration,
		&interrupted.registryGeneration,
		&interrupted.activeSlotName,
		&interrupted.candidateSlotName,
		&interrupted.schemaVersion,
		&interrupted.schemaHash,
		&interrupted.activationBarrier,
		&interrupted.affectedScopesJSON,
		&interrupted.lifecycle,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.New("load interrupted projection bootstrap failed")
	}
	if !uuidPattern.MatchString(interrupted.bootstrapID) ||
		interrupted.sourceStreamGeneration == "" ||
		interrupted.targetStreamGeneration != interrupted.sourceStreamGeneration ||
		interrupted.sourceRegistryGeneration <= 0 {
		return nil, errors.New("interrupted projection bootstrap state is invalid")
	}
	if !interrupted.registryGeneration.Valid ||
		interrupted.registryGeneration.Int64 != requestedRegistryGeneration {
		return nil, errors.New("interrupted projection bootstrap generation differs")
	}
	plan, err := projectionBootstrapRecoveryPlan(
		interrupted.lifecycle,
		interrupted.activeSlotName,
		interrupted.candidateSlotName,
	)
	if err != nil {
		return nil, err
	}
	if !plan.activated {
		if err := coordinator.abortProjectionBootstrap(
			ctx,
			operationLock,
			interrupted.bootstrapID,
			interrupted.candidateSlotName,
		); err != nil {
			return nil, errors.New("abort interrupted projection bootstrap failed")
		}
		if err := dropInactiveReplicationSlot(ctx, workerSlots, plan.retiredSlotName); err != nil {
			return nil, errors.New("discard interrupted projection bootstrap slot failed")
		}
		return nil, nil
	}
	result, err := recoveredProjectionBootstrapResult(interrupted)
	if err != nil {
		return nil, err
	}
	if err := dropInactiveReplicationSlot(ctx, workerSlots, plan.retiredSlotName); err != nil {
		return nil, errors.New("retire interrupted projection bootstrap slot failed")
	}
	if err := completeProjectionBootstrapCleanup(ctx, operationLock, interrupted.bootstrapID); err != nil {
		return nil, errors.New("complete interrupted projection bootstrap cleanup failed")
	}
	return &result, nil
}

func recoveredProjectionBootstrapResult(interrupted interruptedProjectionBootstrap) (ProjectionBootstrapResult, error) {
	if !interrupted.registryGeneration.Valid || interrupted.registryGeneration.Int64 <= 0 ||
		!interrupted.activationBarrier.Valid || interrupted.activationBarrier.String == "" {
		return ProjectionBootstrapResult{}, errors.New("recovered projection bootstrap state is invalid")
	}
	var schemaVersion *int64
	if interrupted.schemaVersion.Valid {
		value := interrupted.schemaVersion.Int64
		schemaVersion = &value
	}
	var schemaHash *string
	if interrupted.schemaHash.Valid {
		value := interrupted.schemaHash.String
		schemaHash = &value
	}
	if !validSchemaPair(schemaVersion, schemaHash) {
		return ProjectionBootstrapResult{}, errors.New("recovered projection bootstrap schema is invalid")
	}
	var affectedScopes []string
	decoder := json.NewDecoder(strings.NewReader(interrupted.affectedScopesJSON))
	if err := decoder.Decode(&affectedScopes); err != nil || len(affectedScopes) == 0 || decoder.Decode(&struct{}{}) != io.EOF {
		return ProjectionBootstrapResult{}, errors.New("recovered projection bootstrap scopes are invalid")
	}
	return ProjectionBootstrapResult{
		BootstrapID:            interrupted.bootstrapID,
		RegistryGeneration:     interrupted.registryGeneration.Int64,
		SourceStreamGeneration: interrupted.sourceStreamGeneration,
		ActiveSlotName:         interrupted.activeSlotName,
		CandidateSlotName:      interrupted.candidateSlotName,
		SchemaVersion:          schemaVersion,
		SchemaHash:             schemaHash,
		ActivationBarrier:      interrupted.activationBarrier.String,
		AffectedScopes:         append([]string(nil), affectedScopes...),
	}, nil
}
