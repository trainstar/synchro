import {
  configureInspection,
  nativeForInspection,
  parseClientStateInspection,
  parseRetainedMutationInspection,
  parseRetainedRejectionInspection,
  parseTransportObservationSnapshot,
  SynchroClient,
} from './SynchroClient';
import { InvalidResponseError, mapNativeError } from './errors';
import { assertValidSQLiteBindParams } from './sqliteValues';
import type {
  ClientStateInspection,
  RetainedMutationInspection,
  RetainedRejectionInspection,
  Row,
  SQLStatement,
  TransportObservationSnapshot,
  TransportOperationClass,
  MigrationCheckpoint,
  MigrationJournalInspection,
  PhysicalSchemaColumnInspection,
} from './types';

const TRANSPORT_OPERATION_CLASSES: readonly TransportOperationClass[] = [
  'connect',
  'pull',
  'push',
  'checkpoint',
  'schemas',
  'rebuild',
  'other',
];

export interface DurableStateInspection {
  row_metadata: {
    table_name: string;
    record_id: string;
    server_version: string;
    row_checksum: string | null;
  } | null;
  rebuild_receipts: Array<{
    rebuild_id_fingerprint: string;
    page_count: number;
    returned_record_count: number;
    request_chain_expected: string[];
    request_chain_observed: string[];
    record_identities_hex: string[];
    received_row_checksums: string[];
    computed_row_checksums: string[];
    computed_scope_checksum: string | null;
    final_scope_checksum: string | null;
    stored_scope_checksum: string | null;
    local_scope_checksum: string | null;
  }>;
}

/**
 * One read-only snapshot of client state and retained details. A detail list is
 * null when its record count exceeds the native capture bound. `applicationRows`
 * holds the rows that the requested read statements returned inside the snapshot.
 */
export interface ClientStateSnapshotInspection {
  clientState: ClientStateInspection;
  captureOverflowed: boolean;
  retainedMutations: RetainedMutationInspection[] | null;
  rejectedMutations: RetainedRejectionInspection[] | null;
  applicationRows: Row[];
  migrationJournal: MigrationJournalInspection | null;
  migrationJournalTruncated: boolean;
  physicalSchema: PhysicalSchemaColumnInspection[];
  physicalSchemaTruncated: boolean;
}

export interface SynchroInspectionOptions {
  transportObservationCapacity?: number;
  requireNewDatabase?: boolean;
}

export class SynchroInspection {
  constructor(
    private readonly client: SynchroClient,
    options: SynchroInspectionOptions = {}
  ) {
    configureInspection(
      client,
      options.transportObservationCapacity ?? 0,
      options.requireNewDatabase ?? false
    );
  }

  async captureSnapshot(rowStatements: SQLStatement[] = []): Promise<ClientStateSnapshotInspection> {
    try {
      const statements = rowStatements.map((statement) => ({
        sql: statement.sql,
        params: assertValidSQLiteBindParams(statement.params ?? []),
      }));
      const result = await nativeForInspection(this.client).inspectClientStateSnapshot(statements);
      const snapshot = requireRecord(parseJSON(result.inspection), 'client state snapshot');
      const migration = parseMigrationCapture(snapshot);
      const clientState = requireRecord(snapshot.client_state, 'client state');
      if (typeof clientState.capture_overflowed !== 'boolean') {
        throw new InvalidResponseError('Native bridge returned invalid capture bound');
      }
      return {
        ...migration,
        clientState: parseClientStateInspection(clientState),
        captureOverflowed: clientState.capture_overflowed,
        retainedMutations:
          nullableArray(snapshot.retained_mutations, 'retained mutations')?.map(parseRetainedMutationInspection) ?? null,
        rejectedMutations:
          nullableArray(snapshot.rejected_mutations, 'rejected mutations')?.map(parseRetainedRejectionInspection) ?? null,
        applicationRows: [...result.applicationRows] as Row[],
      };
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  async durableState(tableName: string, recordID: string): Promise<DurableStateInspection> {
    if (tableName.length === 0 || recordID.length === 0) {
      throw new InvalidResponseError('Durable-state identity is invalid');
    }
    try {
      return parseDurableState(
        parseJSON(await nativeForInspection(this.client).inspectDurableState(tableName, recordID))
      );
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  async processIdentity(): Promise<string> {
    try {
      const value = await nativeForInspection(this.client).getProcessIdentity();
      if (!/^(?:ios|android)-app:[1-9][0-9]*$/.test(value)) {
        throw new InvalidResponseError('Native bridge returned invalid process identity');
      }
      return value;
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  async transportObservations(): Promise<TransportObservationSnapshot> {
    try {
      return parseTransportObservationSnapshot(
        parseJSON(await nativeForInspection(this.client).inspectTransportObservations())
      );
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  async armTransportPause(operationClass: TransportOperationClass | MigrationCheckpoint): Promise<void> {
    requirePauseTarget(operationClass);
    try {
      await nativeForInspection(this.client).armTransportPause(operationClass);
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  async awaitTransportPause(operationClass: TransportOperationClass | MigrationCheckpoint, timeoutMs: number): Promise<void> {
    requirePauseTarget(operationClass);
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 60_000) {
      throw new InvalidResponseError('Transport pause request is invalid');
    }
    try {
      await nativeForInspection(this.client).awaitTransportPause(operationClass, timeoutMs);
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  async resumeTransportPause(): Promise<void> {
    try {
      await nativeForInspection(this.client).resumeTransportPause();
    } catch (error) {
      throw mapNativeError(error);
    }
  }
}

function parseJSON(source: string): unknown {
  try {
    return JSON.parse(source) as unknown;
  } catch {
    throw new InvalidResponseError('Native bridge returned malformed inspection JSON');
  }
}

function parseDurableState(value: unknown): DurableStateInspection {
  const proof = requireRecord(value, 'durable state');
  if (
    Object.keys(proof).length !== 2 ||
    !Object.prototype.hasOwnProperty.call(proof, 'row_metadata') ||
    !Array.isArray(proof.rebuild_receipts)
  ) {
    throw new InvalidResponseError('Native bridge returned invalid durable state');
  }
  if (proof.row_metadata !== null) {
    const metadata = requireRecord(proof.row_metadata, 'row metadata');
    if (
      Object.keys(metadata).length !== 4 ||
      typeof metadata.table_name !== 'string' ||
      typeof metadata.record_id !== 'string' ||
      typeof metadata.server_version !== 'string' ||
      (metadata.row_checksum !== null && typeof metadata.row_checksum !== 'string')
    ) {
      throw new InvalidResponseError('Native bridge returned invalid row metadata');
    }
  }
  for (const receiptValue of proof.rebuild_receipts) {
    const receipt = requireRecord(receiptValue, 'rebuild receipt');
    if (
      Object.keys(receipt).length !== 12 ||
      typeof receipt.rebuild_id_fingerprint !== 'string' ||
      !isNonnegativeSafeInteger(receipt.page_count) ||
      !isNonnegativeSafeInteger(receipt.returned_record_count) ||
      !isStringArray(receipt.request_chain_expected) ||
      !isStringArray(receipt.request_chain_observed) ||
      !isStringArray(receipt.record_identities_hex) ||
      !isStringArray(receipt.received_row_checksums) ||
      !isStringArray(receipt.computed_row_checksums) ||
      !isNullableString(receipt.computed_scope_checksum) ||
      !isNullableString(receipt.final_scope_checksum) ||
      !isNullableString(receipt.stored_scope_checksum) ||
      !isNullableString(receipt.local_scope_checksum)
    ) {
      throw new InvalidResponseError('Native bridge returned invalid rebuild receipt');
    }
  }
  return proof as unknown as DurableStateInspection;
}

function nullableArray(value: unknown, name: string): unknown[] | null {
  if (value === null) {
    return null;
  }
  if (!Array.isArray(value)) {
    throw new InvalidResponseError(`Native bridge returned invalid ${name}`);
  }
  return value;
}

function requireRecord(value: unknown, name: string): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new InvalidResponseError(`Native bridge returned invalid ${name}`);
  }
  return value as Record<string, unknown>;
}

function isNonnegativeSafeInteger(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}

function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((item) => typeof item === 'string');
}

function isNullableString(value: unknown): value is string | null {
  return value === null || typeof value === 'string';
}

function requirePauseTarget(value: TransportOperationClass | MigrationCheckpoint): void {
  if (value !== 'migration_prepared' && value !== 'migration_committed' &&
      !TRANSPORT_OPERATION_CLASSES.includes(value as TransportOperationClass)) {
    throw new InvalidResponseError('Pause target is invalid');
  }
}

function parseMigrationCapture(snapshot: Record<string, unknown>): Pick<ClientStateSnapshotInspection,
  'migrationJournal' | 'migrationJournalTruncated' | 'physicalSchema' | 'physicalSchemaTruncated'> {
  if (typeof snapshot.migration_journal_truncated !== 'boolean' ||
      typeof snapshot.physical_schema_truncated !== 'boolean' || !Array.isArray(snapshot.physical_schema) ||
      snapshot.physical_schema.length > 512) {
    throw new InvalidResponseError('Native bridge returned invalid migration capture');
  }
  const journal = snapshot.migration_journal;
  if (journal !== null) {
    const value = requireRecord(journal, 'migration journal');
    for (const key of ['source', 'target']) {
      const schema = requireRecord(value[key], 'migration schema');
      if (!isNonnegativeSafeInteger(schema.version) || typeof schema.hash !== 'string') {
        throw new InvalidResponseError('Native bridge returned invalid migration schema');
      }
    }
    const stored = requireRecord(value.stored, 'stored migration bindings');
    if (typeof value.action !== 'string' || typeof value.phase !== 'string' ||
        Object.values(stored).some((item) => typeof item !== 'string') ||
        ['journal_version', 'target_manifest_json', 'affected_scopes_json', 'scope_cursor_updates_json',
          'migration_plan_version', 'migration_plan_json', 'migration_plan_hash'].some((key) => typeof stored[key] !== 'string')) {
      throw new InvalidResponseError('Native bridge returned invalid migration journal');
    }
  }
  const physicalSchema = snapshot.physical_schema.map((item) => {
    const column = requireRecord(item, 'physical schema column');
    if (typeof column.table_name !== 'string' || typeof column.name !== 'string' || typeof column.type !== 'string' ||
        typeof column.not_null !== 'boolean' || !isNonnegativeSafeInteger(column.primary_key_position)) {
      throw new InvalidResponseError('Native bridge returned invalid physical schema column');
    }
    return column as unknown as PhysicalSchemaColumnInspection;
  });
  if (snapshot.migration_journal_truncated && journal !== null) {
    throw new InvalidResponseError('Native bridge returned truncated migration details');
  }
  return {
    migrationJournal: journal as MigrationJournalInspection | null,
    migrationJournalTruncated: snapshot.migration_journal_truncated,
    physicalSchema,
    physicalSchemaTruncated: snapshot.physical_schema_truncated,
  };
}

export { sha256Hex } from './digest';
export { isCanonicalBase64Url, isCanonicalInt64 } from './sqliteValues';

export type {
  ClientStateInspection,
  RebuildAttemptInspection,
  ScopeRowInspection,
  ScopeStateInspection,
  TransportObservation,
  TransportObservationSnapshot,
  TransportOperationClass,
  MigrationCheckpoint,
  MigrationJournalInspection,
  PhysicalSchemaColumnInspection,
} from './types';
