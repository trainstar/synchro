// Package operator coordinates projection bootstrap operations.
package operator

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
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
	Present                  bool     `json:"present"`
	BootstrapID              *string  `json:"bootstrap_id"`
	SourceStreamGeneration   *string  `json:"source_stream_generation"`
	TargetStreamGeneration   *string  `json:"target_stream_generation"`
	SourceRegistryGeneration *int64   `json:"source_registry_generation"`
	RegistryGeneration       *int64   `json:"target_registry_generation"`
	ActiveSlotName           *string  `json:"old_slot_name"`
	CandidateSlotName        *string  `json:"candidate_slot_name"`
	SchemaVersion            *int64   `json:"target_schema_version"`
	SchemaHash               *string  `json:"target_schema_hash"`
	ActivationBarrier        *string  `json:"activation_barrier"`
	AffectedScopes           []string `json:"affected_scopes"`
	Lifecycle                *string  `json:"lifecycle"`
}

type projectionBootstrapActiveStream struct {
	ActiveSlotName   string `json:"active_slot_name"`
	StreamGeneration string `json:"stream_generation"`
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
	if err := requireReplicationSlotAbsent(ctx, workerState, candidateSlot); err != nil {
		return ProjectionBootstrapResult{}, err
	}
	var preparedRaw []byte
	if err := coordinator.operatorDB.QueryRowContext(
		ctx,
		"SELECT synchro.synchro_prepare_projection_bootstrap($1, $2)",
		registryGeneration,
		candidateSlot,
	).Scan(&preparedRaw); err != nil {
		return ProjectionBootstrapResult{}, fmt.Errorf("prepare projection bootstrap failed: %w", err)
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
				cleanupErrors = append(cleanupErrors, fmt.Errorf("rollback projection bootstrap activation failed: %w", err))
			}
		}
		if snapshotTransaction != nil {
			if err := snapshotTransaction.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("rollback projection bootstrap snapshot failed: %w", err))
			}
		}
		if replicationConnection != nil {
			if err := replicationConnection.Close(cleanupContext); err != nil {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("close projection bootstrap replication connection failed: %w", err))
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
				cleanupErrors = append(cleanupErrors, fmt.Errorf("drop activated projection bootstrap candidate slot failed: %w", err))
			} else if err := completeProjectionBootstrapCleanup(
				cleanupContext,
				coordinator.operatorDB,
				prepared.BootstrapID,
			); err != nil {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("complete activated projection bootstrap cleanup failed: %w", err))
			}
		} else if activationStateKnown {
			if err := coordinator.abortProjectionBootstrap(
				cleanupContext,
				coordinator.operatorDB,
				prepared.BootstrapID,
				candidateSlot,
			); err != nil {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("abort failed projection bootstrap failed: %w", err))
			}
			if slotCreated {
				if err := dropInactiveReplicationSlot(cleanupContext, workerSlots, candidateSlot); err != nil {
					cleanupErrors = append(cleanupErrors, fmt.Errorf("drop failed projection bootstrap candidate slot failed: %w", err))
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
		return ProjectionBootstrapResult{}, fmt.Errorf("open projection bootstrap source lock connection failed: %w", err)
	}
	var locked bool
	if err := sourceLockConnection.QueryRowContext(
		ctx,
		"SELECT synchro.synchro_lock_stream_reset_sources($1::uuid)",
		prepared.BootstrapID,
	).Scan(&locked); err != nil || !locked {
		if err != nil {
			return ProjectionBootstrapResult{}, fmt.Errorf("lock projection bootstrap sources failed: %w", err)
		}
		return ProjectionBootstrapResult{}, errors.New("lock projection bootstrap sources failed: lock was not acquired")
	}
	beforeMarker, err := coordinator.markSnapshot(ctx, prepared.BootstrapID, "before")
	if err != nil {
		return ProjectionBootstrapResult{}, err
	}

	replicationConnection, err = pgconn.ConnectConfig(ctx, coordinator.replicationConfig.Copy())
	if err != nil {
		return ProjectionBootstrapResult{}, fmt.Errorf("open projection bootstrap replication connection failed: %w", err)
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
		return ProjectionBootstrapResult{}, fmt.Errorf("begin projection bootstrap snapshot transaction failed: %w", err)
	}
	if _, err := snapshotTransaction.ExecContext(ctx, "SET TRANSACTION SNAPSHOT "+quotePostgresLiteral(snapshotName)); err != nil {
		return ProjectionBootstrapResult{}, fmt.Errorf("import projection bootstrap snapshot failed: %w", err)
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
		return ProjectionBootstrapResult{}, fmt.Errorf("stage projection bootstrap baseline failed: %w", err)
	}
	if err := parseStage(stagedRaw, prepared.BootstrapID, registryGeneration, candidateSlot, consistentPoint); err != nil {
		return ProjectionBootstrapResult{}, err
	}
	if err := snapshotTransaction.Commit(); err != nil {
		return ProjectionBootstrapResult{}, fmt.Errorf("commit projection bootstrap baseline failed: %w", err)
	}
	snapshotTransaction = nil
	if err := replicationConnection.Close(ctx); err != nil {
		return ProjectionBootstrapResult{}, fmt.Errorf("close projection bootstrap replication connection failed: %w", err)
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
		return ProjectionBootstrapResult{}, fmt.Errorf("request projection bootstrap barrier failed: %w", err)
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
		return ProjectionBootstrapResult{}, fmt.Errorf("begin projection bootstrap activation transaction failed: %w", err)
	}
	activationAttempted = true
	var activatedRaw []byte
	if err := activationTransaction.QueryRowContext(
		ctx,
		"SELECT synchro.synchro_activate_projection_bootstrap($1::uuid)",
		prepared.BootstrapID,
	).Scan(&activatedRaw); err != nil {
		return ProjectionBootstrapResult{}, fmt.Errorf("activate projection bootstrap failed: %w", err)
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
		return ProjectionBootstrapResult{}, fmt.Errorf("commit projection bootstrap activation failed: %w", err)
	}
	activationTransaction = nil
	activated = true
	if err := dropInactiveReplicationSlot(ctx, workerSlots, candidateSlot); err != nil {
		return ProjectionBootstrapResult{}, fmt.Errorf("retire projection bootstrap candidate slot failed: %w", err)
	}
	if err := completeProjectionBootstrapCleanup(ctx, coordinator.operatorDB, prepared.BootstrapID); err != nil {
		return ProjectionBootstrapResult{}, fmt.Errorf("complete projection bootstrap cleanup failed: %w", err)
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
		if err != nil {
			return fmt.Errorf("verify projection bootstrap operator identity failed: %w", err)
		}
		return errors.New("projection bootstrap operator identity is invalid")
	}
	return nil
}

func (coordinator *Coordinator) acquireOperationLock(ctx context.Context) (*sql.Conn, error) {
	connection, err := coordinator.operatorDB.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("open projection bootstrap operation lock connection failed: %w", err)
	}
	var locked bool
	if err := connection.QueryRowContext(
		ctx,
		"SELECT pg_catalog.pg_try_advisory_lock($1::bigint)",
		streamResetOperatorLockKey,
	).Scan(&locked); err != nil || !locked {
		_ = connection.Close()
		if err != nil {
			return nil, fmt.Errorf("acquire projection bootstrap operation lock failed: %w", err)
		}
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
		cleanupErrors = append(cleanupErrors, fmt.Errorf("unlock projection bootstrap operation failed: %w", err))
		discardSQLConnection(connection)
		return errors.Join(cleanupErrors...)
	}
	if err := connection.Close(); err != nil {
		cleanupErrors = append(cleanupErrors, fmt.Errorf("close projection bootstrap operation lock connection failed: %w", err))
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
		return fmt.Errorf("acquire shared projection bootstrap operation lock failed: %w", err)
	}
	var unlocked bool
	if err := connection.QueryRowContext(
		ctx,
		"SELECT pg_catalog.pg_advisory_unlock($1::bigint)",
		streamResetOperatorLockKey,
	).Scan(&unlocked); err != nil || !unlocked {
		discardSQLConnection(connection)
		if err != nil {
			return fmt.Errorf("release exclusive projection bootstrap operation lock failed: %w", err)
		}
		return errors.New("release exclusive projection bootstrap operation lock failed")
	}
	return nil
}

func (coordinator *Coordinator) acquireWorkerConnections(ctx context.Context) (*sql.Conn, *sql.Conn, error) {
	stateConnection, err := coordinator.workerDB.Conn(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("open projection bootstrap worker state connection failed: %w", err)
	}
	if _, err := stateConnection.ExecContext(ctx, "SET ROLE synchro_worker"); err != nil {
		discardSQLConnection(stateConnection)
		return nil, nil, fmt.Errorf("activate projection bootstrap worker role failed: %w", err)
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
		return nil, nil, fmt.Errorf("open projection bootstrap worker slot connection failed: %w", err)
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
		return nil, nil, fmt.Errorf("activate projection bootstrap worker login failed: %w", err)
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
		if err != nil {
			return nil, nil, fmt.Errorf("verify projection bootstrap worker identity failed: %w", err)
		}
		return nil, nil, errors.New("projection bootstrap worker identity is invalid")
	}
	return stateConnection, slotConnection, nil
}

func releaseWorkerConnections(ctx context.Context, stateConnection, slotConnection *sql.Conn) error {
	var cleanupErrors []error
	if stateConnection != nil {
		if _, err := stateConnection.ExecContext(ctx, "RESET ROLE"); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("reset projection bootstrap worker role failed: %w", err))
			discardSQLConnection(stateConnection)
		} else if err := stateConnection.Close(); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("close projection bootstrap worker state connection failed: %w", err))
		}
	}
	if slotConnection != nil {
		if err := slotConnection.Close(); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("close projection bootstrap worker slot connection failed: %w", err))
		}
	}
	return errors.Join(cleanupErrors...)
}

func discardSQLConnection(connection *sql.Conn) {
	_ = connection.Raw(func(any) error { return driver.ErrBadConn })
	_ = connection.Close()
}

func loadActiveStream(ctx context.Context, workerState queryRower) (string, string, error) {
	var raw []byte
	if err := workerState.QueryRowContext(
		ctx,
		"SELECT synchro.synchro_projection_bootstrap_active_stream()",
	).Scan(&raw); err != nil {
		return "", "", fmt.Errorf("load active projection bootstrap stream failed: %w", err)
	}
	var stream projectionBootstrapActiveStream
	if decodeStrictObject(raw, &stream, "active_slot_name", "stream_generation") != nil ||
		!validSlotName(stream.ActiveSlotName) || stream.StreamGeneration == "" {
		return "", "", errors.New("load active projection bootstrap stream failed: state is invalid")
	}
	return stream.ActiveSlotName, stream.StreamGeneration, nil
}

func (coordinator *Coordinator) markSnapshot(ctx context.Context, bootstrapID, phase string) (snapshotMarker, error) {
	var raw []byte
	if err := coordinator.operatorDB.QueryRowContext(
		ctx,
		"SELECT synchro.synchro_mark_stream_reset_snapshot($1::uuid, $2)",
		bootstrapID,
		phase,
	).Scan(&raw); err != nil {
		return snapshotMarker{}, fmt.Errorf("mark projection bootstrap snapshot failed: %w", err)
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
		return "", fmt.Errorf("emit projection bootstrap WAL marker failed: %w", err)
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
	if err != nil {
		return "", "", fmt.Errorf("create projection bootstrap replication slot failed: %w", err)
	}
	if len(results) != 1 || len(results[0].Rows) != 1 || len(results[0].Rows[0]) != 4 {
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
		cleanupErrors = append(cleanupErrors, fmt.Errorf("unlock projection bootstrap sources failed: %w", err))
		discardSQLConnection(connection)
		return errors.Join(cleanupErrors...)
	}
	if err := connection.Close(); err != nil {
		cleanupErrors = append(cleanupErrors, fmt.Errorf("close projection bootstrap source lock connection failed: %w", err))
	}
	return errors.Join(cleanupErrors...)
}

func waitForMainBoundary(ctx context.Context, workerState queryRower, streamGeneration, markerLSN string) error {
	err := waitUntil(ctx, func(attemptContext context.Context) (bool, error) {
		var ready bool
		if err := workerState.QueryRowContext(
			attemptContext,
			"SELECT synchro.synchro_projection_bootstrap_main_boundary($1, $2)",
			streamGeneration,
			markerLSN,
		).Scan(&ready); err != nil {
			return false, fmt.Errorf("read main projection bootstrap boundary failed: %w", err)
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
			return false, fmt.Errorf("read projection bootstrap status failed: %w", err)
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
			return fmt.Errorf("projection bootstrap wait stopped: %w", ctx.Err())
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
			return fmt.Errorf("projection bootstrap wait stopped: %w", ctx.Err())
		case <-timer.C:
		}
	}
}

func dropInactiveReplicationSlot(ctx context.Context, workerSlots workerSlotConnection, slotName string) error {
	if !validSlotName(slotName) {
		return errors.New("projection bootstrap candidate slot is invalid")
	}
	return waitUntil(ctx, func(attemptContext context.Context) (bool, error) {
		var raw []byte
		err := workerSlots.QueryRowContext(
			attemptContext,
			"SELECT synchro.synchro_projection_bootstrap_slot_drop_state($1)",
			slotName,
		).Scan(&raw)
		if err != nil {
			return false, fmt.Errorf("inspect projection bootstrap candidate slot failed: %w", err)
		}
		state, err := parseSlotDropState(raw)
		if err != nil {
			return false, err
		}
		if !state.Present {
			return true, nil
		}
		if !state.Valid {
			return false, errors.New("projection bootstrap candidate slot binding is invalid")
		}
		if state.Active {
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
			return false, fmt.Errorf("drop projection bootstrap candidate slot failed: %w", err)
		}
		return true, nil
	})
}

func requireReplicationSlotAbsent(ctx context.Context, workerSlots queryRower, slotName string) error {
	var absent bool
	if err := workerSlots.QueryRowContext(
		ctx,
		"SELECT synchro.synchro_projection_bootstrap_slot_absent($1)",
		slotName,
	).Scan(&absent); err != nil {
		return fmt.Errorf("inspect projection bootstrap candidate slot failed: %w", err)
	}
	if !absent {
		return errors.New("projection bootstrap candidate slot already exists")
	}
	return nil
}

func cleanupAbortedProjectionBootstrapSlots(ctx context.Context, workerState queryRower, workerSlots workerSlotConnection) error {
	for {
		var slotName sql.NullString
		err := workerState.QueryRowContext(
			ctx,
			"SELECT synchro.synchro_projection_bootstrap_next_aborted_slot()",
		).Scan(&slotName)
		if err != nil {
			return fmt.Errorf("load aborted projection bootstrap slot failed: %w", err)
		}
		if !slotName.Valid {
			return nil
		}
		if !validSlotName(slotName.String) {
			return errors.New("load aborted projection bootstrap slot failed: slot name is invalid")
		}
		if err := dropInactiveReplicationSlot(ctx, workerSlots, slotName.String); err != nil {
			return fmt.Errorf("discard aborted projection bootstrap slot failed: %w", err)
		}
	}
}

func projectionBootstrapIsActivated(ctx context.Context, workerState queryRower, bootstrapID string) (bool, error) {
	var activated bool
	if err := workerState.QueryRowContext(
		ctx,
		"SELECT synchro.synchro_projection_bootstrap_is_activated($1::uuid)",
		bootstrapID,
	).Scan(&activated); err != nil {
		return false, fmt.Errorf("read projection bootstrap activation state failed: %w", err)
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
		return fmt.Errorf("abort projection bootstrap failed: %w", err)
	}
	return parseAbort(raw, bootstrapID, candidateSlot)
}

func completeProjectionBootstrapCleanup(ctx context.Context, querier queryRower, bootstrapID string) error {
	var complete bool
	if err := querier.QueryRowContext(
		ctx,
		"SELECT synchro.synchro_complete_projection_bootstrap_cleanup($1::uuid)",
		bootstrapID,
	).Scan(&complete); err != nil {
		return fmt.Errorf("complete projection bootstrap cleanup failed: %w", err)
	} else if !complete {
		return errors.New("complete projection bootstrap cleanup failed: extension returned false")
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
	var raw []byte
	err := workerState.QueryRowContext(
		ctx,
		"SELECT synchro.synchro_projection_bootstrap_interrupted()",
	).Scan(&raw)
	if err != nil {
		return nil, fmt.Errorf("load interrupted projection bootstrap failed: %w", err)
	}
	var interrupted interruptedProjectionBootstrap
	if decodeStrictObject(raw, &interrupted,
		"present", "bootstrap_id", "source_stream_generation", "target_stream_generation",
		"source_registry_generation", "target_registry_generation", "old_slot_name",
		"candidate_slot_name", "target_schema_version", "target_schema_hash",
		"activation_barrier", "affected_scopes", "lifecycle",
	) != nil {
		return nil, errors.New("interrupted projection bootstrap response is invalid")
	}
	if !interrupted.Present {
		return nil, nil
	}
	if interrupted.BootstrapID == nil ||
		interrupted.SourceStreamGeneration == nil ||
		interrupted.TargetStreamGeneration == nil ||
		interrupted.SourceRegistryGeneration == nil ||
		interrupted.ActiveSlotName == nil ||
		interrupted.CandidateSlotName == nil ||
		interrupted.Lifecycle == nil ||
		!uuidPattern.MatchString(*interrupted.BootstrapID) ||
		*interrupted.SourceStreamGeneration == "" ||
		*interrupted.TargetStreamGeneration != *interrupted.SourceStreamGeneration ||
		*interrupted.SourceRegistryGeneration <= 0 {
		return nil, errors.New("interrupted projection bootstrap state is invalid")
	}
	if interrupted.RegistryGeneration == nil ||
		*interrupted.RegistryGeneration != requestedRegistryGeneration {
		return nil, errors.New("interrupted projection bootstrap generation differs")
	}
	plan, err := projectionBootstrapRecoveryPlan(
		*interrupted.Lifecycle,
		*interrupted.ActiveSlotName,
		*interrupted.CandidateSlotName,
	)
	if err != nil {
		return nil, err
	}
	if !plan.activated {
		if err := coordinator.abortProjectionBootstrap(
			ctx,
			operationLock,
			*interrupted.BootstrapID,
			*interrupted.CandidateSlotName,
		); err != nil {
			return nil, fmt.Errorf("abort interrupted projection bootstrap failed: %w", err)
		}
		if err := dropInactiveReplicationSlot(ctx, workerSlots, plan.retiredSlotName); err != nil {
			return nil, fmt.Errorf("discard interrupted projection bootstrap slot failed: %w", err)
		}
		return nil, nil
	}
	result, err := recoveredProjectionBootstrapResult(interrupted)
	if err != nil {
		return nil, err
	}
	if err := dropInactiveReplicationSlot(ctx, workerSlots, plan.retiredSlotName); err != nil {
		return nil, fmt.Errorf("retire interrupted projection bootstrap slot failed: %w", err)
	}
	if err := completeProjectionBootstrapCleanup(ctx, operationLock, *interrupted.BootstrapID); err != nil {
		return nil, fmt.Errorf("complete interrupted projection bootstrap cleanup failed: %w", err)
	}
	return &result, nil
}

func recoveredProjectionBootstrapResult(interrupted interruptedProjectionBootstrap) (ProjectionBootstrapResult, error) {
	if interrupted.BootstrapID == nil ||
		interrupted.RegistryGeneration == nil || *interrupted.RegistryGeneration <= 0 ||
		interrupted.ActivationBarrier == nil || *interrupted.ActivationBarrier == "" ||
		interrupted.SourceStreamGeneration == nil ||
		interrupted.ActiveSlotName == nil ||
		interrupted.CandidateSlotName == nil {
		return ProjectionBootstrapResult{}, errors.New("recovered projection bootstrap state is invalid")
	}
	if !validSchemaPair(interrupted.SchemaVersion, interrupted.SchemaHash) {
		return ProjectionBootstrapResult{}, errors.New("recovered projection bootstrap schema is invalid")
	}
	if len(interrupted.AffectedScopes) == 0 {
		return ProjectionBootstrapResult{}, errors.New("recovered projection bootstrap scopes are invalid")
	}
	return ProjectionBootstrapResult{
		BootstrapID:            *interrupted.BootstrapID,
		RegistryGeneration:     *interrupted.RegistryGeneration,
		SourceStreamGeneration: *interrupted.SourceStreamGeneration,
		ActiveSlotName:         *interrupted.ActiveSlotName,
		CandidateSlotName:      *interrupted.CandidateSlotName,
		SchemaVersion:          interrupted.SchemaVersion,
		SchemaHash:             interrupted.SchemaHash,
		ActivationBarrier:      *interrupted.ActivationBarrier,
		AffectedScopes:         append([]string(nil), interrupted.AffectedScopes...),
	}, nil
}
