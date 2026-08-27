import {
  configureInspection,
  nativeForInspection,
  parseClientStateInspection,
  parseTransportObservationSnapshot,
  SynchroClient,
} from './SynchroClient';
import { InvalidResponseError, mapNativeError } from './errors';
import type {
  ClientStateInspection,
  TransportObservationSnapshot,
  TransportOperationClass,
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

  async clientState(): Promise<ClientStateInspection> {
    try {
      return parseClientStateInspection(parseJSON(await nativeForInspection(this.client).inspectClientState()));
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

  async armTransportPause(operationClass: TransportOperationClass): Promise<void> {
    requireTransportOperationClass(operationClass);
    try {
      await nativeForInspection(this.client).armTransportPause(operationClass);
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  async awaitTransportPause(operationClass: TransportOperationClass, timeoutMs: number): Promise<void> {
    requireTransportOperationClass(operationClass);
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

function requireTransportOperationClass(value: TransportOperationClass): void {
  if (!TRANSPORT_OPERATION_CLASSES.includes(value)) {
    throw new InvalidResponseError('Transport operation class is invalid');
  }
}

export type {
  ClientStateInspection,
  RebuildAttemptInspection,
  ScopeRowInspection,
  ScopeStateInspection,
  TransportObservation,
  TransportObservationSnapshot,
  TransportOperationClass,
} from './types';
