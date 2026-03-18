package synchro

import "errors"

var (
	// ErrUpgradeRequired indicates the client version is too old.
	ErrUpgradeRequired = errors.New("synchro: client upgrade required")

	// ErrOwnershipViolation indicates the user does not own the record.
	ErrOwnershipViolation = errors.New("synchro: ownership violation")

	// ErrRecordNotFound indicates the record does not exist.
	ErrRecordNotFound = errors.New("synchro: record not found")

	// ErrConflict indicates a conflict during push.
	ErrConflict = errors.New("synchro: conflict")

	// ErrStaleClient indicates the client's checkpoint is too far behind.
	ErrStaleClient = errors.New("synchro: stale client")

	// ErrSchemaMismatch indicates client and server schema contracts differ.
	ErrSchemaMismatch = errors.New("synchro: schema mismatch")

	// ErrUnsupportedSchemaFeature indicates the schema contains unsupported constructs.
	ErrUnsupportedSchemaFeature = errors.New("synchro: unsupported schema feature")

	// ErrTableNotRegistered indicates the table is not in the registry.
	ErrTableNotRegistered = errors.New("synchro: table not registered")

	// ErrTableReadOnly indicates the table does not accept push operations.
	ErrTableReadOnly = errors.New("synchro: table is read-only")

	// ErrInvalidOperation indicates an unknown push operation type.
	ErrInvalidOperation = errors.New("synchro: invalid operation")

	// ErrClientNotRegistered indicates the client has not been registered.
	ErrClientNotRegistered = errors.New("synchro: client not registered")

	// ErrSnapshotRequired indicates the client must rebuild from a full snapshot.
	// Deprecated: Use bucket rebuild instead. Kept for legacy client compatibility.
	ErrSnapshotRequired = errors.New("synchro: snapshot required")
)
