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

	// ErrUnregisteredParent indicates a table references a parent not in the registry.
	ErrUnregisteredParent = errors.New("synchro: unregistered parent table")

	// ErrCycleDetected indicates a cycle exists in the parent chain.
	ErrCycleDetected = errors.New("synchro: cycle detected in parent chain")

	// ErrOrphanedChain indicates the parent chain root has no OwnerColumn.
	ErrOrphanedChain = errors.New("synchro: orphaned parent chain")

	// ErrMissingOwnership indicates a pushable table has no ownership path.
	ErrMissingOwnership = errors.New("synchro: pushable table has no ownership path")

	// ErrMissingParentFKCol indicates ParentTable is set without ParentFKCol.
	ErrMissingParentFKCol = errors.New("synchro: ParentTable set without ParentFKCol")

	// ErrRedundantProtected indicates a ProtectedColumns entry is redundant.
	ErrRedundantProtected = errors.New("synchro: redundant protected column")

	// ErrInvalidPushPolicy indicates an unsupported push policy value.
	ErrInvalidPushPolicy = errors.New("synchro: invalid push policy")

	// ErrInvalidBucketConfig indicates contradictory bucket configuration.
	ErrInvalidBucketConfig = errors.New("synchro: invalid bucket configuration")
)
