import { Platform } from 'react-native';
import type { EventSubscription } from 'react-native';
import NativeSynchro from './NativeSynchro';
import {
  InvalidResponseError,
  SynchroError,
  mapNativeError,
  parseSyncFailure,
} from './errors';
import { assertValidSQLiteBindParams } from './sqliteValues';
import { SYNC_OPERATION_KINDS, SYNC_STATUS_TYPES } from './types';
import type {
  Row,
  ExecResult,
  BatchResult,
  SQLStatement,
  SQLiteBindValue,
  ColumnDef,
  TableOptions,
  ReadTransaction,
  Transaction,
  SyncStatus,
  SyncStatusType,
  SyncEvent,
  SyncOperationKind,
  SchemaAction,
  MutationStatus,
  MutationRejectionCode,
  PendingMutationInspection,
  RejectedMutationInspection,
  ClientStateInspection,
  ScopeStateInspection,
  ScopeRowInspection,
  RebuildAttemptInspection,
  TransportObservation,
  TransportObservationSnapshot,
  TransportOperationClass,
  ConflictEvent,
  SynchroConfig,
  Unsubscribe,
  AsyncUnsubscribe,
} from './types';

let observerCounter = 0;
let activeNativeOwner: object | null = null;
const inspectionOwners = new WeakMap<SynchroClient, object>();
const inspectionCapacities = new WeakMap<SynchroClient, number>();
const inspectionRequireNewDatabases = new WeakMap<SynchroClient, boolean>();

function nextObserverID(): string {
  return `obs_${++observerCounter}_${Date.now()}`;
}

function nullableRow(row: unknown): Row | null {
  return row == null ? null : (row as Row);
}

type NativeSyncStatus = {
  status: unknown;
  retryAt: unknown;
  operation?: unknown;
  failure?: unknown;
};

type NativeSyncEvent = {
  type: unknown;
  from: unknown;
  to: unknown;
  operation: unknown;
  attempt: unknown;
  retryAt: unknown;
  source: unknown;
  target: unknown;
  action: unknown;
  mutationID: unknown;
  tableID: unknown;
  mutationStatus: unknown;
  rejectionCode: unknown;
  scopeID: unknown;
  rebuildID: unknown;
  failure: unknown;
};

const SYNC_EVENT_TYPES = [
  'state_changed',
  'backoff',
  'schema_applying',
  'schema_applied',
  'mutation_accepted',
  'mutation_rejected',
  'rebuild_requested',
  'rebuild_completed',
  'failure',
] as const;

type SyncEventType = (typeof SYNC_EVENT_TYPES)[number];

const SCHEMA_ACTIONS: readonly SchemaAction[] = [
  'none',
  'replace',
  'rebuild_local',
  'unsupported',
];

const MUTATION_STATUSES: readonly MutationStatus[] = [
  'applied',
  'conflict',
  'rejected_terminal',
];

const MUTATION_REJECTION_CODES: readonly MutationRejectionCode[] = [
  'version_conflict',
  'row_already_exists',
  'row_deleted',
  'row_not_found',
  'schema_incompatible',
  'policy_rejected',
  'validation_failed',
  'table_not_synced',
];

const MUTATION_OPERATIONS = ['insert', 'upsert', 'update', 'delete'] as const;
const LOCAL_MUTATION_STATUSES = [
  'pending',
  'sealed',
  'superseded_before_send',
  'cancelled_before_send',
  'blocked_by_predecessor',
  'server_rejected',
] as const;

const TRANSPORT_OPERATION_CLASSES: readonly TransportOperationClass[] = [
  'connect',
  'pull',
  'push',
  'checkpoint',
  'schemas',
  'rebuild',
  'other',
];

const LEGAL_SYNC_STATUS_TRANSITIONS: Readonly<
  Record<SyncStatusType, readonly SyncStatusType[]>
> = {
  uninitialized: ['local_ready', 'error', 'stopped'],
  local_ready: ['connecting', 'error', 'stopped'],
  connecting: ['schema_applying', 'ready', 'backoff', 'error', 'stopped'],
  schema_applying: ['ready', 'rebuilding', 'error', 'stopped'],
  ready: ['connecting', 'pushing', 'pulling', 'rebuilding', 'error', 'stopped'],
  pushing: ['pushing', 'ready', 'pulling', 'connecting', 'backoff', 'error', 'stopped'],
  pulling: ['pulling', 'ready', 'rebuilding', 'connecting', 'backoff', 'error', 'stopped'],
  rebuilding: ['rebuilding', 'ready', 'connecting', 'backoff', 'error', 'stopped'],
  backoff: ['connecting', 'pushing', 'pulling', 'rebuilding', 'error', 'stopped'],
  error: ['local_ready', 'stopped'],
  stopped: ['local_ready'],
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function requiredString(value: unknown, name: string): string {
  if (typeof value !== 'string') {
    throw new InvalidResponseError(`Native bridge returned an invalid ${name}`);
  }
  return value;
}

function nullableString(value: unknown, name: string): string | null {
  if (value == null) {
    return null;
  }
  return requiredString(value, name);
}

function requiredSafeInteger(value: unknown, name: string): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value)) {
    throw new InvalidResponseError(`Native bridge returned an invalid ${name}`);
  }
  return value;
}

function requiredNonnegativeSafeInteger(value: unknown, name: string): number {
  const integer = requiredSafeInteger(value, name);
  if (integer < 0) {
    throw new InvalidResponseError(`Native bridge returned an invalid ${name}`);
  }
  return integer;
}

function nullableSafeInteger(value: unknown, name: string): number | null {
  return value == null ? null : requiredSafeInteger(value, name);
}

function parseDate(value: unknown, name: string): Date | null {
  const stringValue = nullableString(value, name);
  if (stringValue === null) {
    return null;
  }
  const date = new Date(stringValue);
  if (Number.isNaN(date.getTime())) {
    throw new InvalidResponseError(`Native bridge returned an invalid ${name}`);
  }
  return date;
}

function parseStatus(value: unknown): SyncStatus | null {
  if (!isRecord(value) || typeof value.status !== 'string') {
    throw new InvalidResponseError('Native bridge returned an invalid sync status');
  }
  if (!SYNC_STATUS_TYPES.includes(value.status as SyncStatusType)) {
    return null;
  }
  const status = value.status as SyncStatusType;
  const retryAt = parseDate(value.retryAt, 'retryAt');
  const operationValue = nullableString(value.operation, 'status operation');
  const operation = operationValue === null
    ? null
    : SYNC_OPERATION_KINDS.includes(operationValue as SyncOperationKind)
      ? (operationValue as SyncOperationKind)
      : (() => {
          throw new InvalidResponseError('Native bridge returned an invalid status operation');
        })();
  const failure = value.failure == null ? null : parseSyncFailure(value.failure);

  if (status === 'backoff') {
    if (retryAt === null || operation === null || failure !== null) {
      throw new InvalidResponseError('Native bridge returned invalid backoff status details');
    }
    return { status, retryAt, operation, failure: null };
  }
  if (status === 'error') {
    if (retryAt !== null || operation !== null || failure === null) {
      throw new InvalidResponseError('Native bridge returned invalid error status details');
    }
    return { status, retryAt: null, operation: null, failure };
  }
  if (retryAt !== null || operation !== null || failure !== null) {
    throw new InvalidResponseError('Native bridge returned invalid state detail fields');
  }
  return { status, retryAt: null, operation: null, failure: null };
}

function parseSchemaRef(value: unknown, name: string) {
  if (!isRecord(value) || typeof value.version !== 'number' || !Number.isSafeInteger(value.version)) {
    throw new InvalidResponseError(`Native bridge returned an invalid ${name}`);
  }
  return {
    version: value.version,
    hash: requiredString(value.hash, `${name} hash`),
  };
}

function parseMutationStatus(value: unknown): MutationStatus {
  if (!MUTATION_STATUSES.includes(value as MutationStatus)) {
    throw new InvalidResponseError('Native bridge returned an invalid mutation status');
  }
  return value as MutationStatus;
}

function parseRejectionCode(value: unknown): MutationRejectionCode | null {
  if (value == null) {
    return null;
  }
  if (!MUTATION_REJECTION_CODES.includes(value as MutationRejectionCode)) {
    throw new InvalidResponseError('Native bridge returned an invalid mutation rejection code');
  }
  return value as MutationRejectionCode;
}

function parseSchemaAction(value: unknown): SchemaAction {
  if (!SCHEMA_ACTIONS.includes(value as SchemaAction)) {
    throw new InvalidResponseError('Native bridge returned an invalid schema action');
  }
  return value as SchemaAction;
}

function parseSyncEvent(value: NativeSyncEvent): SyncEvent | null {
  if (typeof value.type !== 'string') {
    throw new InvalidResponseError('Native bridge returned an invalid sync event');
  }
  if (!SYNC_EVENT_TYPES.includes(value.type as SyncEventType)) {
    return null;
  }
  switch (value.type as SyncEventType) {
    case 'state_changed': {
      if (typeof value.from !== 'string' || typeof value.to !== 'string') {
        throw new InvalidResponseError('Native bridge returned an invalid lifecycle event');
      }
      if (
        !SYNC_STATUS_TYPES.includes(value.from as SyncStatusType) ||
        !SYNC_STATUS_TYPES.includes(value.to as SyncStatusType)
      ) {
        return null;
      }
      const from = value.from as SyncStatusType;
      const to = value.to as SyncStatusType;
      if (!LEGAL_SYNC_STATUS_TRANSITIONS[from].includes(to)) {
        throw new InvalidResponseError('Native bridge returned an invalid lifecycle event');
      }
      return {
        type: 'state_changed',
        from,
        to,
      };
    }
    case 'backoff': {
      const attempt = value.attempt;
      if (typeof attempt !== 'number' || !Number.isSafeInteger(attempt) || attempt < 0) {
        throw new InvalidResponseError('Native bridge returned an invalid backoff attempt');
      }
      const retryAt = parseDate(value.retryAt, 'backoff retryAt');
      if (retryAt === null) {
        throw new InvalidResponseError('Native bridge returned a missing backoff retryAt');
      }
      return {
        type: 'backoff',
        operation: SYNC_OPERATION_KINDS.includes(value.operation as SyncOperationKind)
          ? (value.operation as SyncOperationKind)
          : (() => {
              throw new InvalidResponseError('Native bridge returned an invalid backoff operation');
            })(),
        attempt,
        retryAt,
      };
    }
    case 'schema_applying':
    case 'schema_applied':
      return {
        type: value.type as 'schema_applying' | 'schema_applied',
        source: parseSchemaRef(value.source, 'schema source'),
        target: parseSchemaRef(value.target, 'schema target'),
        action: parseSchemaAction(value.action),
      };
    case 'mutation_accepted':
    case 'mutation_rejected': {
      const mutationID = requiredString(value.mutationID, 'mutation ID');
      return {
        type: value.type as 'mutation_accepted' | 'mutation_rejected',
        mutationID,
        tableID: requiredString(value.tableID, 'mutation table ID'),
        status: parseMutationStatus(value.mutationStatus),
        rejectionCode: parseRejectionCode(value.rejectionCode),
      };
    }
    case 'rebuild_requested':
    case 'rebuild_completed':
      return {
        type: value.type as 'rebuild_requested' | 'rebuild_completed',
        scopeID: requiredString(value.scopeID, 'rebuild scope ID'),
        rebuildID: requiredString(value.rebuildID, 'rebuild ID'),
      };
    case 'failure': {
      return { type: 'failure', failure: parseSyncFailure(value.failure) };
    }
  }
}

function parseNativeDTO<T>(json: string): T {
  try {
    return JSON.parse(json) as T;
  } catch {
    throw new InvalidResponseError('Native bridge returned malformed JSON');
  }
}

function parseNativeArray(json: string, name: string): unknown[] {
  const value = parseNativeDTO<unknown>(json);
  if (!Array.isArray(value)) {
    throw new InvalidResponseError(`Native bridge returned invalid ${name}`);
  }
  return value;
}

function parsePendingMutationInspection(
  value: unknown,
  index: number
): PendingMutationInspection {
  const name = `pending mutation at index ${index}`;
  if (!isRecord(value)) {
    throw new InvalidResponseError(`Native bridge returned an invalid ${name}`);
  }
  if (!MUTATION_OPERATIONS.includes(value.operation as never)) {
    throw new InvalidResponseError(`Native bridge returned an invalid ${name} operation`);
  }
  if (!LOCAL_MUTATION_STATUSES.includes(value.status as never)) {
    throw new InvalidResponseError(`Native bridge returned an invalid ${name} status`);
  }
  if (!Array.isArray(value.authoredFields)) {
    throw new InvalidResponseError(`Native bridge returned invalid ${name} fields`);
  }

  const authoredFields = value.authoredFields.map((field, fieldIndex) => {
    if (!isRecord(field) || !Object.prototype.hasOwnProperty.call(field, 'value')) {
      throw new InvalidResponseError(
        `Native bridge returned an invalid ${name} field at index ${fieldIndex}`
      );
    }
    return {
      fieldID: requiredString(field.fieldID, `${name} field ID`),
      logicalType: requiredString(field.logicalType, `${name} field logical type`),
      value: field.value as PendingMutationInspection['authoredFields'][number]['value'],
    };
  });

  return {
    mutationID: requiredString(value.mutationID, `${name} ID`),
    localOrder: requiredSafeInteger(value.localOrder, `${name} local order`),
    tableID: requiredString(value.tableID, `${name} table ID`),
    tableName: requiredString(value.tableName, `${name} table name`),
    recordID: requiredString(value.recordID, `${name} record ID`),
    primaryKeyFieldID: requiredString(
      value.primaryKeyFieldID,
      `${name} primary-key field ID`
    ),
    primaryKeyLogicalType: requiredString(
      value.primaryKeyLogicalType,
      `${name} primary-key logical type`
    ),
    operation: value.operation as PendingMutationInspection['operation'],
    authoredSchema: parseSchemaRef(value.authoredSchema, `${name} authored schema`),
    baseVersion: nullableString(value.baseVersion, `${name} base version`),
    clientVersion: requiredString(value.clientVersion, `${name} client version`),
    status: value.status as PendingMutationInspection['status'],
    sourceKind: requiredString(value.sourceKind, `${name} source kind`),
    dependsOnMutationID: nullableString(
      value.dependsOnMutationID,
      `${name} predecessor mutation ID`
    ),
    normalizedMutationID: nullableString(
      value.normalizedMutationID,
      `${name} normalized mutation ID`
    ),
    sealedBatchID: nullableString(value.sealedBatchID, `${name} sealed batch ID`),
    sealedOrdinal: nullableSafeInteger(value.sealedOrdinal, `${name} sealed ordinal`),
    authoredFields,
  };
}

function parseRejectedMutationInspection(
  value: unknown,
  index: number
): RejectedMutationInspection {
  const name = `rejected mutation at index ${index}`;
  if (!isRecord(value)) {
    throw new InvalidResponseError(`Native bridge returned an invalid ${name}`);
  }
  const code = parseRejectionCode(value.code);
  if (code === null) {
    throw new InvalidResponseError(`Native bridge returned a missing ${name} code`);
  }
  return {
    mutationID: requiredString(value.mutationID, `${name} ID`),
    tableName: requiredString(value.tableName, `${name} table name`),
    recordID: requiredString(value.recordID, `${name} record ID`),
    status: parseMutationStatus(value.status),
    code,
    message: nullableString(value.message, `${name} message`),
    serverRowJSON: nullableString(value.serverRowJSON, `${name} server row JSON`),
    serverVersion: nullableString(value.serverVersion, `${name} server version`),
    mutationJSON: requiredString(value.mutationJSON, `${name} mutation JSON`),
    rejectionJSON: requiredString(value.rejectionJSON, `${name} rejection JSON`),
    createdAt: requiredString(value.createdAt, `${name} creation time`),
    updatedAt: requiredString(value.updatedAt, `${name} update time`),
  };
}

function parseScopeStateInspection(value: unknown, index: number): ScopeStateInspection {
  const name = `scope state at index ${index}`;
  if (!isRecord(value)) {
    throw new InvalidResponseError(`Native bridge returned an invalid ${name}`);
  }
  return {
    scopeID: requiredString(value.scope_id, `${name} ID`),
    cursor: nullableString(value.cursor, `${name} cursor`),
    checksum: nullableString(value.checksum, `${name} checksum`),
    localChecksum: requiredString(value.local_checksum, `${name} local checksum`),
    generation: requiredSafeInteger(value.generation, `${name} generation`),
  };
}

function parseScopeRowInspection(value: unknown, index: number): ScopeRowInspection {
  const name = `scope row at index ${index}`;
  if (!isRecord(value)) {
    throw new InvalidResponseError(`Native bridge returned an invalid ${name}`);
  }
  return {
    scopeID: requiredString(value.scope_id, `${name} scope ID`),
    tableName: requiredString(value.table_name, `${name} table name`),
    recordID: requiredString(value.record_id, `${name} record ID`),
    checksum: requiredString(value.checksum, `${name} checksum`),
    generation: requiredSafeInteger(value.generation, `${name} generation`),
  };
}

function parseRebuildAttemptInspection(value: unknown, index: number): RebuildAttemptInspection {
  const name = `rebuild attempt at index ${index}`;
  if (!isRecord(value)) {
    throw new InvalidResponseError(`Native bridge returned an invalid ${name}`);
  }
  return {
    scopeID: requiredString(value.scope_id, `${name} scope ID`),
    rebuildID: requiredString(value.rebuild_id, `${name} rebuild ID`),
    clientGeneration: requiredSafeInteger(value.client_generation, `${name} client generation`),
    schemaVersion: requiredSafeInteger(value.schema_version, `${name} schema version`),
    schemaHash: requiredString(value.schema_hash, `${name} schema hash`),
    generation: requiredSafeInteger(value.generation, `${name} generation`),
    cursor: nullableString(value.cursor, `${name} cursor`),
    pageLimit: requiredSafeInteger(value.page_limit, `${name} page limit`),
  };
}

export function parseClientStateInspection(value: unknown): ClientStateInspection {
  if (!isRecord(value)) {
    throw new InvalidResponseError('Native bridge returned invalid client state inspection');
  }
  const schema = value.schema == null ? null : parseSchemaRef(value.schema, 'client schema');
  if (!Array.isArray(value.scope_states) || !Array.isArray(value.scope_rows) || !Array.isArray(value.rebuild_attempts)) {
    throw new InvalidResponseError('Native bridge returned incomplete client state inspection');
  }
  return {
    schema,
    scopeStates: value.scope_states.map(parseScopeStateInspection),
    scopeRows: value.scope_rows.map(parseScopeRowInspection),
    rebuildAttempts: value.rebuild_attempts.map(parseRebuildAttemptInspection),
    applicationRowCount: requiredNonnegativeSafeInteger(
      value.application_row_count,
      'application row count'
    ),
    mutationLedgerCount: requiredNonnegativeSafeInteger(
      value.mutation_ledger_count,
      'mutation ledger count'
    ),
    mutationOutcomeCount: requiredNonnegativeSafeInteger(
      value.mutation_outcome_count,
      'mutation outcome count'
    ),
    sealedBatchCount: requiredNonnegativeSafeInteger(
      value.sealed_batch_count,
      'sealed batch count'
    ),
    rejectedMutationCount: requiredNonnegativeSafeInteger(
      value.rejected_mutation_count,
      'rejected mutation count'
    ),
    scopeStateCount: requiredNonnegativeSafeInteger(
      value.scope_state_count,
      'scope state count'
    ),
    scopeRowCount: requiredNonnegativeSafeInteger(
      value.scope_row_count,
      'scope row count'
    ),
    provenanceCount: requiredNonnegativeSafeInteger(
      value.provenance_count,
      'provenance count'
    ),
    rowMetadataCount: requiredNonnegativeSafeInteger(
      value.row_metadata_count,
      'row metadata count'
    ),
    rebuildAttemptCount: requiredNonnegativeSafeInteger(
      value.rebuild_attempt_count,
      'rebuild attempt count'
    ),
    rebuildReceiptCount: requiredNonnegativeSafeInteger(
      value.rebuild_receipt_count,
      'rebuild receipt count'
    ),
    provenanceMaintenanceWorkCursor: requiredNonnegativeInt64String(
      value.provenance_maintenance_work_cursor,
      'provenance maintenance work cursor'
    ),
  };
}

function requiredNonnegativeInt64String(value: unknown, name: string): string {
  const maximum = '9223372036854775807';
  if (
    typeof value !== 'string' ||
    !/^(0|[1-9][0-9]*)$/.test(value) ||
    value.length > maximum.length ||
    (value.length === maximum.length && value > maximum)
  ) {
    throw new InvalidResponseError(`Native bridge returned invalid ${name}`);
  }
  return value;
}

function optionalTransportFacts(value: unknown, name: string): Record<string, import('./types').JSONValue> | undefined {
  if (value == null) {
    return undefined;
  }
  if (!isRecord(value)) {
    throw new InvalidResponseError(`Native bridge returned invalid ${name}`);
  }
  return value as Record<string, import('./types').JSONValue>;
}

function optionalTransportRequestFacts(
  value: unknown,
  name: string
): Record<string, import('./types').JSONValue> | undefined {
  const facts = optionalTransportFacts(value, name);
  const mutationCount = facts?.mutation_count;
  if (
    facts !== undefined &&
    Object.prototype.hasOwnProperty.call(facts, 'mutation_count') &&
    (typeof mutationCount !== 'number' ||
      !Number.isSafeInteger(mutationCount) ||
      mutationCount < 0)
  ) {
    throw new InvalidResponseError(`Native bridge returned invalid ${name} mutation count`);
  }
  return facts;
}

function parseTransportObservation(value: unknown, index: number): TransportObservation {
  const name = `transport observation at index ${index}`;
  if (!isRecord(value) || !TRANSPORT_OPERATION_CLASSES.includes(value.operation_class as TransportOperationClass)) {
    throw new InvalidResponseError(`Native bridge returned an invalid ${name}`);
  }
  const cursorFingerprints = value.cursor_fingerprints;
  if (cursorFingerprints !== undefined && (!Array.isArray(cursorFingerprints) || !cursorFingerprints.every((item) => typeof item === 'string'))) {
    throw new InvalidResponseError(`Native bridge returned invalid ${name} cursor fingerprints`);
  }
  const cursorFingerprintsComplete = value.cursor_fingerprints_complete;
  if (cursorFingerprintsComplete !== undefined && typeof cursorFingerprintsComplete !== 'boolean') {
    throw new InvalidResponseError(`Native bridge returned invalid ${name} cursor completeness`);
  }
  return {
    sequence: requiredSafeInteger(value.sequence, `${name} sequence`),
    operationClass: value.operation_class as TransportOperationClass,
    statusCode: requiredSafeInteger(value.status_code, `${name} status code`),
    durationNanoseconds: requiredSafeInteger(value.duration_nanoseconds, `${name} duration`),
    ...(cursorFingerprints === undefined ? {} : { cursorFingerprints: [...cursorFingerprints] as string[] }),
    ...(cursorFingerprintsComplete === undefined ? {} : { cursorFingerprintsComplete }),
    ...(value.request_facts == null ? {} : { requestFacts: optionalTransportRequestFacts(value.request_facts, `${name} request facts`)! }),
    ...(value.rebuild_response_facts == null ? {} : { rebuildResponseFacts: optionalTransportFacts(value.rebuild_response_facts, `${name} rebuild response facts`)! }),
    ...(value.pull_response_facts == null ? {} : { pullResponseFacts: optionalTransportFacts(value.pull_response_facts, `${name} pull response facts`)! }),
  };
}

export function parseTransportObservationSnapshot(value: unknown): TransportObservationSnapshot {
  if (!isRecord(value) || !Array.isArray(value.observations) || typeof value.overflowed !== 'boolean') {
    throw new InvalidResponseError('Native bridge returned invalid transport observations');
  }
  return {
    observations: value.observations.map(parseTransportObservation),
    overflowed: value.overflowed,
    sequenceCheckpoint: requiredSafeInteger(value.sequence_checkpoint, 'transport sequence checkpoint'),
  };
}

function checkedParams(params: readonly SQLiteBindValue[]): readonly SQLiteBindValue[] {
  return assertValidSQLiteBindParams(params);
}

export class SynchroClient {
  private readonly nativeOwner = {};
  private readonly config: SynchroConfig;
  private authSubscription: EventSubscription | null = null;

  constructor(config: SynchroConfig) {
    this.config = config;
    inspectionOwners.set(this, this.nativeOwner);
  }

  private get native(): typeof NativeSynchro {
    if (activeNativeOwner !== null && activeNativeOwner !== this.nativeOwner) {
      throw new SynchroError(
        'CLIENT_ALREADY_ACTIVE',
        'Another SynchroClient owns the native runtime'
      );
    }
    return NativeSynchro;
  }

  private installAuthSubscription(): void {
    this.authSubscription?.remove();
    const owner = this.nativeOwner;
    this.authSubscription = NativeSynchro.onAuthRequest(
      (event: { requestID: string }) => {
        this.config
          .authProvider()
          .then((token) => {
            if (activeNativeOwner === owner) {
              NativeSynchro.resolveAuthRequest(event.requestID, token);
            }
          })
          .catch((err) => {
            if (activeNativeOwner === owner) {
              NativeSynchro.rejectAuthRequest(
                event.requestID,
                err instanceof Error ? err.message : String(err)
              );
            }
          });
      }
    );
  }

  private releaseNativeOwnership(): void {
    this.authSubscription?.remove();
    this.authSubscription = null;
    if (activeNativeOwner === this.nativeOwner) {
      activeNativeOwner = null;
    }
  }

  async initialize(): Promise<void> {
    if (activeNativeOwner !== null && activeNativeOwner !== this.nativeOwner) {
      throw new SynchroError(
        'CLIENT_ALREADY_ACTIVE',
        'Another SynchroClient owns the native runtime'
      );
    }
    activeNativeOwner = this.nativeOwner;
    this.installAuthSubscription();
    try {
      await this.native.initialize({
        dbPath: this.config.dbPath,
        serverURL: this.config.serverURL,
        clientID: this.config.clientID,
        platform: this.config.platform ?? Platform.OS,
        appVersion: this.config.appVersion,
        syncInterval: this.config.syncInterval ?? 30,
        pushDebounce: this.config.pushDebounce ?? 0.5,
        maxRetryAttempts: this.config.maxRetryAttempts ?? 5,
        pullPageSize: this.config.pullPageSize ?? 100,
        pushBatchSize: this.config.pushBatchSize ?? 100,
        seedDatabasePath: this.config.seedDatabasePath,
        transportObservationCapacity: inspectionCapacities.get(this) ?? 0,
        requireNewDatabase: inspectionRequireNewDatabases.get(this) ?? false,
      });
    } catch (error) {
      this.releaseNativeOwnership();
      throw mapNativeError(error);
    }
  }

  // Core SQL

  async query(sql: string, params?: readonly SQLiteBindValue[]): Promise<Row[]> {
    try {
      return [...(await this.native.query(sql, checkedParams(params ?? [])))] as Row[];
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  async queryOne(sql: string, params?: readonly SQLiteBindValue[]): Promise<Row | null> {
    try {
      return nullableRow(await this.native.queryOne(sql, checkedParams(params ?? [])));
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  async execute(sql: string, params?: readonly SQLiteBindValue[]): Promise<ExecResult> {
    try {
      return await this.native.execute(sql, checkedParams(params ?? []));
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  async executeAuthoredWrite(
    tableID: string,
    operation: string,
    fieldIDs: readonly string[],
    sql: string,
    values?: readonly SQLiteBindValue[]
  ): Promise<ExecResult> {
    try {
      return await this.native.executeAuthoredWrite(
        tableID,
        operation,
        fieldIDs,
        sql,
        checkedParams(values ?? [])
      );
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  async executeBatch(statements: SQLStatement[]): Promise<BatchResult> {
    try {
      const payload = statements.map((s) => ({
        sql: s.sql,
        params: checkedParams(s.params ?? []),
      }));
      return await this.native.executeBatch(payload);
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  // Transactions

  async writeTransaction<T>(
    callback: (tx: Transaction) => Promise<T>
  ): Promise<T> {
    let txID: string;
    try {
      txID = await this.native.beginWriteTransaction();
    } catch (error) {
      throw mapNativeError(error);
    }

    try {
      const tx: Transaction = {
        query: async (sql, params) => {
          return [...(await this.native.txQuery(txID, sql, checkedParams(params ?? [])))] as Row[];
        },
        queryOne: async (sql, params) => {
          return nullableRow(
            await this.native.txQueryOne(txID, sql, checkedParams(params ?? []))
          );
        },
        execute: async (sql, params) => {
          return await this.native.txExecute(txID, sql, checkedParams(params ?? []));
        },
      };
      const result = await callback(tx);
      await this.native.commitTransaction(txID);
      return result;
    } catch (error) {
      try {
        await this.native.rollbackTransaction(txID);
      } catch {
        // rollback best-effort
      }
      throw mapNativeError(error);
    }
  }

  async readTransaction<T>(
    callback: (tx: ReadTransaction) => Promise<T>
  ): Promise<T> {
    let txID: string;
    try {
      txID = await this.native.beginReadTransaction();
    } catch (error) {
      throw mapNativeError(error);
    }

    try {
      const tx: ReadTransaction = {
        query: async (sql, params) => {
          return [...(await this.native.txQuery(txID, sql, checkedParams(params ?? [])))] as Row[];
        },
        queryOne: async (sql, params) => {
          return nullableRow(
            await this.native.txQueryOne(txID, sql, checkedParams(params ?? []))
          );
        },
      };
      const result = await callback(tx);
      await this.native.commitTransaction(txID);
      return result;
    } catch (error) {
      try {
        await this.native.rollbackTransaction(txID);
      } catch {
        // rollback best-effort
      }
      throw mapNativeError(error);
    }
  }

  // Schema, local-only

  async createTable(
    name: string,
    columns: ColumnDef[],
    options?: TableOptions
  ): Promise<void> {
    try {
      await this.native.createTable(
        name,
        JSON.stringify(columns),
        options ? JSON.stringify(options) : null
      );
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  async alterTable(name: string, addColumns: ColumnDef[]): Promise<void> {
    try {
      await this.native.alterTable(name, JSON.stringify(addColumns));
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  async createIndex(
    table: string,
    columns: string[],
    unique = false
  ): Promise<void> {
    try {
      await this.native.createIndex(table, columns, unique);
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  // Observation

  async onChange(tables: string[], callback: () => void): Promise<AsyncUnsubscribe> {
    const observerID = nextObserverID();

    const subscription = this.native.onChange(
      (event: { observerID: string }) => {
        if (event.observerID === observerID) {
          callback();
        }
      }
    );

    try {
      await this.native.addChangeObserver(observerID, tables);
    } catch (error) {
      subscription.remove();
      throw mapNativeError(error);
    }

    return async () => {
      subscription.remove();
      try {
        await NativeSynchro.removeObserver(observerID);
      } catch (error) {
        throw mapNativeError(error);
      }
    };
  }

  async watch(
    sql: string,
    params: readonly SQLiteBindValue[] | undefined,
    tables: string[],
    callback: (rows: Row[]) => void
  ): Promise<AsyncUnsubscribe> {
    const observerID = nextObserverID();

    const subscription = this.native.onQueryResult(
      (event) => {
        if (event.observerID === observerID) {
          callback([...(event.rows as readonly Row[])]);
        }
      }
    );

    try {
      await this.native.addQueryObserver(
        observerID,
        sql,
        checkedParams(params ?? []),
        tables
      );
    } catch (error) {
      subscription.remove();
      throw mapNativeError(error);
    }

    return async () => {
      subscription.remove();
      try {
        await NativeSynchro.removeObserver(observerID);
      } catch (error) {
        throw mapNativeError(error);
      }
    };
  }

  // Lifecycle

  async close(): Promise<void> {
    if (activeNativeOwner !== null && activeNativeOwner !== this.nativeOwner) {
      throw new SynchroError(
        'CLIENT_ALREADY_ACTIVE',
        'Another SynchroClient owns the native runtime'
      );
    }
    try {
      await NativeSynchro.close();
      this.releaseNativeOwnership();
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  async getPath(): Promise<string> {
    try {
      return await this.native.getPath();
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  async pendingChangeCount(): Promise<number> {
    try {
      return await this.native.pendingChangeCount();
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  async getSyncStatus(): Promise<SyncStatus> {
    try {
      const status = parseNativeDTO<NativeSyncStatus>(
        await this.native.getSyncStatus()
      );
      const parsed = parseStatus(status);
      if (parsed === null) {
        throw new InvalidResponseError('Native bridge returned an unknown sync status');
      }
      return parsed;
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  async inspectPendingMutations(): Promise<PendingMutationInspection[]> {
    try {
      return parseNativeArray(
        await this.native.inspectPendingMutations(),
        'pending mutation inspection'
      ).map(parsePendingMutationInspection);
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  async inspectRetainedMutations(): Promise<PendingMutationInspection[]> {
    try {
      return parseNativeArray(
        await this.native.inspectRetainedMutations(),
        'retained mutation inspection'
      ).map(parsePendingMutationInspection);
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  async inspectRejectedMutations(): Promise<RejectedMutationInspection[]> {
    try {
      return parseNativeArray(
        await this.native.inspectRejectedMutations(),
        'rejected mutation inspection'
      ).map(parseRejectedMutationInspection);
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  async clearRejectedMutations(): Promise<void> {
    try {
      await this.native.clearRejectedMutations();
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  // Sync

  async start(): Promise<void> {
    try {
      await this.native.start();
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  async stop(): Promise<void> {
    try {
      await this.native.stop();
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  async enterBackground(): Promise<void> {
    try {
      await this.native.enterBackground();
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  async enterForeground(): Promise<void> {
    try {
      await this.native.enterForeground();
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  async retryAfterError(): Promise<void> {
    try {
      await this.native.retryAfterError();
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  async resetSchemaAndStart(): Promise<void> {
    try {
      await this.native.resetSchemaAndStart();
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  async syncNow(): Promise<void> {
    try {
      await this.native.syncNow();
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  // Status and events

  onStatusChange(callback: (status: SyncStatus) => void): Unsubscribe {
    const subscription = this.native.onStatusChange((event) => {
      const status = parseStatus(event);
      if (status !== null) callback(status);
    });
    return () => subscription.remove();
  }

  onSyncEvent(callback: (event: SyncEvent) => void): Unsubscribe {
    const subscription = this.native.onSyncEvent((event) => {
      const parsed = parseSyncEvent(event);
      if (parsed !== null) callback(parsed);
    });
    return () => subscription.remove();
  }

  onConflict(callback: (event: ConflictEvent) => void): Unsubscribe {
    const subscription = this.native.onConflict(
      (event: {
        table: string;
        recordID: string;
        clientDataJson: string | null;
        serverDataJson: string | null;
      }) => {
        callback({
          table: event.table,
          recordID: event.recordID,
          clientData: event.clientDataJson
            ? JSON.parse(event.clientDataJson)
            : null,
          serverData: event.serverDataJson
            ? JSON.parse(event.serverDataJson)
            : null,
        });
      }
    );
    return () => subscription.remove();
  }

}

export function configureInspection(
  client: SynchroClient,
  transportObservationCapacity: number,
  requireNewDatabase: boolean
): void {
  const owner = inspectionOwners.get(client);
  if (
    owner !== undefined &&
    activeNativeOwner === owner &&
    transportObservationCapacity === 0 &&
    !requireNewDatabase
  ) {
    return;
  }
  if (
    owner === undefined ||
    activeNativeOwner === owner ||
    typeof requireNewDatabase !== 'boolean' ||
    !Number.isSafeInteger(transportObservationCapacity) ||
    transportObservationCapacity < 0 ||
    transportObservationCapacity > 512
  ) {
    throw new InvalidResponseError('Inspection configuration is invalid');
  }
  inspectionCapacities.set(client, transportObservationCapacity);
  inspectionRequireNewDatabases.set(client, requireNewDatabase);
}

export function nativeForInspection(client: SynchroClient): typeof NativeSynchro {
  const owner = inspectionOwners.get(client);
  if (owner === undefined || activeNativeOwner !== owner) {
    throw new SynchroError('NOT_CONNECTED', 'SynchroClient is not initialized');
  }
  return NativeSynchro;
}
