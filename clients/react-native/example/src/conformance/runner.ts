import { SynchroClient, SynchroError } from '@trainstar/synchro-react-native';
import {
  isCanonicalBase64Url,
  isCanonicalInt64,
  sha256Hex,
  SynchroInspection,
} from '@trainstar/synchro-react-native/inspection';
import type {
  ClientStateInspection,
  DurableStateInspection,
  ScopeRowInspection,
  TransportObservationSnapshot,
} from '@trainstar/synchro-react-native/inspection';
import type {
  PendingMutationInspection,
  RejectedMutationInspection,
  Row,
  SQLiteBindValue,
  SyncEvent,
  SyncFailure,
  SyncStatus,
} from '@trainstar/synchro-react-native';
import type {
  ConformanceCommand,
  ConformanceErrorCode,
  JSONScalar,
  LifecycleOperation,
  RowSelector,
  ScenarioOperation,
  SynchronizeCompletion,
  SynchronizeMethod,
} from './types';
import {
  assertBoundedJSON,
  isCallID,
  isLifecycleOperation,
  isSynchronizeMethod,
} from './types';

const MAXIMUM_CAPTURE_VALUES = 256;
const MAXIMUM_EVENTS = 256;
const POLL_INTERVAL_MS = 100;
const COMPLETION_TIMEOUT_MS = 30000;
const IDENTIFIER_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;
const DATABASE_NAME_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;

export class ConformanceUnavailableError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ConformanceUnavailableError';
  }
}

export class ConformanceCommandError extends Error {
  // execution_failed is a catch-all, so the originating message is kept. Without
  // it a device failure reports only the code and cannot be diagnosed.
  constructor(readonly code: ConformanceErrorCode, cause?: unknown) {
    super(cause === undefined ? code : `${code}: ${describeCause(cause)}`);
    this.name = 'ConformanceCommandError';
  }
}

function describeCause(cause: unknown): string {
  if (cause instanceof Error) {
    const code = (cause as { code?: unknown }).code;
    return typeof code === 'string' ? `${code} ${cause.message}` : cause.message;
  }
  return String(cause);
}

export type RawSyncStatus = {
  state: string;
  retry_at: string | null;
  operation: string | null;
  failure: RawFailure | null;
};

export type RawFailure = {
  operation: string;
  code: string;
  retryable: boolean;
  recovery_action: string;
};

export type RawProcessIdentity = {
  process_id: string;
  database_identity_fingerprint: string;
};

export type RawEvent = Record<string, JSONScalar | Record<string, JSONScalar> | null>;

export type ConformanceActionResult =
  | { kind: 'opened'; status: RawSyncStatus; process: RawProcessIdentity }
  | { kind: 'local-action'; rows_affected: number; process: RawProcessIdentity }
  | {
      kind: 'synchronized';
      completion: SynchronizeCompletion;
      status: RawSyncStatus;
      process: RawProcessIdentity;
    }
  | { kind: 'call-begun'; call_id: string; state: 'in_flight'; process: RawProcessIdentity }
  | {
      kind: 'call-completed';
      call_id: string;
      state: 'completed';
      completion: SynchronizeCompletion;
      status: RawSyncStatus;
      process: RawProcessIdentity;
    }
  | { kind: 'awaited'; status: RawSyncStatus; process: RawProcessIdentity }
  | {
      kind: 'lifecycle';
      operation: LifecycleOperation;
      status: RawSyncStatus;
      process: RawProcessIdentity;
    }
  | { kind: 'capture'; capture: ConformanceCapture; process: RawProcessIdentity };

export interface ConformanceCapture {
  application_rows?: Row[];
  pending_mutations?: PendingMutationInspection[];
  rejected_mutations?: RejectedMutationInspection[];
  client_state?: ClientStateInspection;
  durable_proof?: RawDurableProof;
  provenance?: ScopeRowInspection[];
  request_trace?: TransportObservationSnapshot;
  sync_status?: RawSyncStatus;
  sync_events?: RawEvent[];
}

export interface RawDurableProof {
  row_metadata: DurableStateInspection['row_metadata'];
  rebuild_receipt_proofs: Array<{
    rebuild_id_fingerprint: string;
    page_count: number;
    returned_record_count: number;
    request_chain_valid: boolean;
    records_in_canonical_order: boolean;
    row_checksums_valid: boolean;
    scope_checksum_valid: boolean;
    final_checksum_matches_local: boolean;
  }>;
}

export interface PublicConformanceRunnerOptions {
  serverURL: string;
  authToken: string;
  appVersion: string;
}

interface ClientSession {
  runtime: ConformanceCommand['runtime'];
  requireNewDatabase: boolean;
  client: SynchroClient | null;
  inspection: SynchroInspection | null;
  events: SyncEvent[];
  unsubscribeEvents: (() => void) | null;
}

interface ClientCall {
  clientKey: string;
  task: Promise<Error | undefined>;
}

interface CompletionObservation {
  completion: SynchronizeCompletion;
  status: SyncStatus;
}

/**
 * PublicConformanceRunner maps one closed host action to public JavaScript APIs.
 * It returns raw facts only. It does not load scenarios or evaluate outcomes.
 */
export class PublicConformanceRunner {
  private readonly sessions = new Map<string, ClientSession>();
  private readonly calls = new Map<string, ClientCall>();
  private activeClientKey: string | null = null;

  constructor(private readonly options: PublicConformanceRunnerOptions) {}

  async execute(command: ConformanceCommand): Promise<ConformanceActionResult> {
    try {
      requirePairedRuntimeConnection(command.runtime);
      const action = command.action.action;
      switch (`${action.actor}/${action.command}`) {
        case 'client/open':
          return this.boundedResult(await this.open(command));
        case 'client/execute-step':
          return this.boundedResult(await this.executeLocalAction(command));
        case 'client/synchronize-step':
          return this.boundedResult(await this.synchronize(command));
        case 'client/begin-call':
          return this.boundedResult(await this.beginCall(command));
        case 'client/await-call':
          return this.boundedResult(await this.awaitCall(command));
        case 'client/lifecycle':
          return this.boundedResult(await this.lifecycle(command));
        case 'observer/await-step':
          return this.boundedResult(await this.awaitStep(command));
        case 'observer/capture':
          return this.boundedResult(await this.capture(command));
        default:
          throw new ConformanceUnavailableError('host action is not exposed by the public JavaScript client');
      }
    } catch (error) {
      if (error instanceof ConformanceCommandError) {
        throw error;
      }
      if (error instanceof ConformanceUnavailableError) {
        throw new ConformanceCommandError('unavailable');
      }
      throw new ConformanceCommandError('execution_failed', error);
    }
  }

  async close(): Promise<void> {
    const sessions = [...this.sessions.values()];
    const calls = [...this.calls.values()];
    await Promise.all(calls.map(async ({ task }) => {
      await task;
    }));
    this.sessions.clear();
    this.calls.clear();
    this.activeClientKey = null;
    for (const session of sessions) {
      session.unsubscribeEvents?.();
      if (session.client !== null) {
        await session.client.close();
      }
    }
  }

  private async open(command: ConformanceCommand): Promise<ConformanceActionResult> {
    const clientKey = requireClientKey(command);
    const parameters = command.action.action.parameters;
    const mode = requiredString(parameters.database_mode);
    if ((mode !== 'create' && mode !== 'reuse') || this.sessions.has(clientKey)) {
      throw new ConformanceCommandError('invalid_command');
    }
    const databasePath = appPrivateDatabasePath(command.runtime.database_path);
    if (mode === 'create' && command.runtime.seed_database_path !== undefined) {
      throw new ConformanceCommandError('invalid_command');
    }
    this.sessions.set(clientKey, {
      runtime: {
        ...command.runtime,
        database_path: databasePath,
        ...(command.runtime.seed_database_path === undefined
          ? {}
          : { seed_database_path: appPrivateSeedPath(command.runtime.seed_database_path) }),
      },
      requireNewDatabase: mode === 'create',
      client: null,
      inspection: null,
      events: [],
      unsubscribeEvents: null,
    });
    let client: SynchroClient;
    try {
      client = await this.activate(clientKey);
    } catch (error) {
      if (error instanceof SynchroError && error.code === 'INVALID_CONFIG') {
        this.sessions.delete(clientKey);
        throw new ConformanceCommandError('invalid_command');
      }
      throw error;
    }
    return {
      kind: 'opened',
      status: rawStatus(await client.getSyncStatus()),
      process: await this.processIdentity(clientKey, client),
    };
  }

  private async executeLocalAction(command: ConformanceCommand): Promise<ConformanceActionResult> {
    const clientKey = requireClientKey(command);
    const operation = requireOneLocalOperation(command);
    const client = await this.activate(clientKey);
    const action = decodeLocalAction(operation);
    const rowsAffected = await client.writeTransaction(async (transaction) => {
      const result = await transaction.execute(action.sql, action.values);
      return result.rowsAffected;
    });
    return {
      kind: 'local-action',
      rows_affected: rowsAffected,
      process: await this.processIdentity(clientKey, client),
    };
  }

  private async synchronize(command: ConformanceCommand): Promise<ConformanceActionResult> {
    const clientKey = requireClientKey(command);
    const method = requireSynchronizeMethod(command.action.action.parameters.method);
    const client = await this.activate(clientKey);
    const invocationError = await this.invokeSynchronizeMethod(client, method).then(
      () => undefined,
      (error: unknown) => toError(error)
    );
    const observation = await this.waitForCompletion(client, invocationError);
    return {
      kind: 'synchronized',
      completion: observation.completion,
      status: rawStatus(observation.status),
      process: await this.processIdentity(clientKey, client),
    };
  }

  private async beginCall(command: ConformanceCommand): Promise<ConformanceActionResult> {
    const clientKey = requireClientKey(command);
    const parameters = command.action.action.parameters;
    const callID = requireCallID(parameters.call_id);
    if (this.calls.has(callID) || [...this.calls.values()].some((call) => call.clientKey === clientKey)) {
      throw new ConformanceCommandError('invalid_command');
    }
    const client = await this.activate(clientKey);
    const method = requireSynchronizeMethod(parameters.method);
    const task = this.invokeSynchronizeMethod(client, method).then(
      () => undefined,
      (error: unknown) => toError(error)
    );
    this.calls.set(callID, { clientKey, task });
    return {
      kind: 'call-begun',
      call_id: callID,
      state: 'in_flight',
      process: await this.processIdentity(clientKey, client),
    };
  }

  private async awaitCall(command: ConformanceCommand): Promise<ConformanceActionResult> {
    const clientKey = requireClientKey(command);
    const callID = requireCallID(command.action.action.parameters.call_id);
    const call = this.calls.get(callID);
    if (call === undefined || call.clientKey !== clientKey) {
      throw new ConformanceCommandError('invalid_command');
    }
    const client = await this.activate(clientKey);
    const invocationError = await call.task;
    const observation = await this.waitForCompletion(client, invocationError);
    this.calls.delete(callID);
    return {
      kind: 'call-completed',
      call_id: callID,
      state: 'completed',
      completion: observation.completion,
      status: rawStatus(observation.status),
      process: await this.processIdentity(clientKey, client),
    };
  }

  private async lifecycle(command: ConformanceCommand): Promise<ConformanceActionResult> {
    const clientKey = requireClientKey(command);
    const operation = command.action.action.parameters.operation;
    if (!isLifecycleOperation(operation) || [...this.calls.values()].some((call) => call.clientKey === clientKey)) {
      throw new ConformanceCommandError('invalid_command');
    }
    const client = await this.activate(clientKey);
    switch (operation) {
      case 'stop':
        await client.stop();
        break;
      case 'enter-background':
        await client.enterBackground();
        break;
      case 'enter-foreground':
        await client.enterForeground();
        break;
    }
    return {
      kind: 'lifecycle',
      operation,
      status: rawStatus(await client.getSyncStatus()),
      process: await this.processIdentity(clientKey, client),
    };
  }

  private async awaitStep(command: ConformanceCommand): Promise<ConformanceActionResult> {
    const clientKey = requireClientKey(command);
    const callID = command.action.action.parameters.call_id;
    if (callID !== undefined && (!isCallID(callID) || this.calls.get(callID)?.clientKey !== clientKey)) {
      throw new ConformanceCommandError('invalid_command');
    }
    const client = await this.activate(clientKey);
    return {
      kind: 'awaited',
      status: rawStatus(await client.getSyncStatus()),
      process: await this.processIdentity(clientKey, client),
    };
  }

  private async capture(command: ConformanceCommand): Promise<ConformanceActionResult> {
    const parameters = command.action.action.parameters;
    const clientKeys = requiredStringArray(parameters.client_keys);
    const sources = requiredStringArray(parameters.sources);
    if (clientKeys.length !== 1 || clientKeys[0] !== command.runtime.client_key) {
      throw new ConformanceCommandError('invalid_command');
    }
    const client = await this.activate(clientKeys[0]);
    const inspection = this.requireInspection(clientKeys[0]);
    const capture: ConformanceCapture = {};
    for (const source of sources) {
      switch (source) {
        case 'application-rows':
          capture.application_rows = await captureRows(client, decodeSelectors(parameters.row_selectors));
          break;
        case 'pending-mutations':
          capture.pending_mutations = bounded(await client.inspectPendingMutations());
          break;
        case 'rejected-mutations':
          capture.rejected_mutations = bounded(await client.inspectRejectedMutations());
          break;
        case 'sync-status':
          capture.sync_status = rawStatus(await client.getSyncStatus());
          break;
        case 'sync-events':
          capture.sync_events = bounded(rawEvents(this.requireSession(clientKeys[0]).events));
          break;
        case 'scope-state':
          capture.client_state = await inspection.clientState();
          break;
        case 'durable-proof': {
          const state = await inspection.clientState();
          const identity = durableProofIdentity(parameters.durable_proof_identity, state.scopeRows);
          try {
            capture.durable_proof = normalizeDurableProof(
              await inspection.durableState(identity.tableName, identity.recordID)
            );
          } catch {
            throw new ConformanceCommandError('capture_inspection_failed');
          }
          break;
        }
        case 'provenance':
          capture.provenance = bounded((await inspection.clientState()).scopeRows);
          break;
        case 'request-trace':
          capture.request_trace = await inspection.transportObservations();
          break;
        default:
          throw new ConformanceUnavailableError('capture source is not exposed by the public JavaScript client');
      }
    }
    return {
      kind: 'capture',
      capture,
      process: await this.processIdentity(clientKeys[0], client),
    };
  }

  private async activate(clientKey: string): Promise<SynchroClient> {
    const session = this.requireSession(clientKey);
    if (this.activeClientKey === clientKey && session.client !== null) {
      return session.client;
    }
    if (this.calls.size !== 0) {
      throw new ConformanceUnavailableError('the public bridge exposes one active native client');
    }
    if (this.activeClientKey !== null) {
      const active = this.requireSession(this.activeClientKey);
      active.unsubscribeEvents?.();
      active.unsubscribeEvents = null;
      await active.client?.close();
      active.client = null;
      active.inspection = null;
    }
    const client = new SynchroClient({
      dbPath: session.runtime.database_path,
      serverURL: session.runtime.server_url ?? this.options.serverURL,
      authProvider: async () => session.runtime.auth_token ?? this.options.authToken,
      clientID: session.runtime.client_id,
      appVersion: this.options.appVersion,
      syncInterval: 3600,
      pushDebounce: 3600,
      seedDatabasePath: session.runtime.seed_database_path,
    });
    const inspection = new SynchroInspection(client, {
      transportObservationCapacity: 512,
      requireNewDatabase: session.requireNewDatabase,
    });
    await client.initialize();
    session.requireNewDatabase = false;
    session.client = client;
    session.inspection = inspection;
    session.unsubscribeEvents = client.onSyncEvent((event) => {
      if (session.events.length >= MAXIMUM_EVENTS) {
        session.events.shift();
      }
      session.events.push(event);
    });
    this.activeClientKey = clientKey;
    return client;
  }

  private requireSession(clientKey: string): ClientSession {
    const session = this.sessions.get(clientKey);
    if (session === undefined) {
      throw new ConformanceCommandError('invalid_command');
    }
    return session;
  }

  private requireInspection(clientKey: string): SynchroInspection {
    const inspection = this.requireSession(clientKey).inspection;
    if (inspection === null) {
      throw new ConformanceCommandError('invalid_command');
    }
    return inspection;
  }

  private async invokeSynchronizeMethod(
    client: SynchroClient,
    method: SynchronizeMethod
  ): Promise<void> {
    switch (method) {
      case 'start':
        return client.start();
      case 'sync-now':
        return client.syncNow();
      case 'retry-after-error':
        return client.retryAfterError();
      case 'reset-schema-and-start':
        return client.resetSchemaAndStart();
    }
  }

  private async waitForCompletion(
    client: SynchroClient,
    invocationError?: Error
  ): Promise<CompletionObservation> {
    if (invocationError !== undefined) {
      throw invocationError;
    }
    const deadline = Date.now() + COMPLETION_TIMEOUT_MS;
    let lastStatus = 'none';
    while (Date.now() < deadline) {
      const status = await client.getSyncStatus();
      lastStatus = status.status;
      if (status.status === 'ready') {
        return { completion: 'idle', status };
      }
      if (status.status === 'backoff') {
        return { completion: 'blocked', status };
      }
      if (status.status === 'error') {
        return { completion: 'error', status };
      }
      await sleep(POLL_INTERVAL_MS);
    }
    // Name the status the client rested in. A bare code cannot separate a slow
    // synchronization from one that never left its starting state.
    throw new ConformanceCommandError(
      'execution_failed',
      new Error(`sync did not complete within ${COMPLETION_TIMEOUT_MS} ms, last status ${lastStatus}`)
    );
  }

  private async processIdentity(
    clientKey: string,
    client: SynchroClient
  ): Promise<RawProcessIdentity> {
    return {
      process_id: await this.requireInspection(clientKey).processIdentity(),
      database_identity_fingerprint: sha256Hex(await client.getPath()),
    };
  }

  private boundedResult(result: ConformanceActionResult): ConformanceActionResult {
    try {
      assertBoundedJSON(result, 64 * 1024);
      return result;
    } catch {
      throw new ConformanceCommandError('execution_failed');
    }
  }
}

function requirePairedRuntimeConnection(runtime: ConformanceCommand['runtime']): void {
  const hasServerURL = runtime.server_url !== undefined;
  const hasAuthToken = runtime.auth_token !== undefined;
  if (hasServerURL !== hasAuthToken) {
    throw new ConformanceCommandError('invalid_command');
  }
}

function requireClientKey(command: ConformanceCommand): string {
  const clientKey = requiredString(command.action.action.parameters.client_key);
  if (clientKey !== command.runtime.client_key) {
    throw new ConformanceCommandError('invalid_command');
  }
  return clientKey;
}

function requireOneLocalOperation(command: ConformanceCommand): ScenarioOperation {
  if (command.action.steps.length !== 1) {
    throw new ConformanceCommandError('invalid_command');
  }
  const operation = command.action.steps[0].operation;
  if (operation.contract_operation !== 'local' || operation.name !== 'write') {
    throw new ConformanceUnavailableError('local operation is not exposed by the public JavaScript client');
  }
  return operation;
}

function decodeLocalAction(operation: ScenarioOperation): {
  sql: string;
  values: SQLiteBindValue[];
} {
  const payload = operation.payload as Record<string, unknown>;
  const tableName = requiredIdentifier(payload.table_id);
  // The contract authors pk as a field identifier to value object, the same
  // shape as columns, so it normalizes through the same decoder. A registered
  // relation carries exactly one primary key column.
  const primaryKeyColumns = decodeColumns(decodeColumnValues(payload.pk));
  if (primaryKeyColumns.length !== 1) {
    throw new ConformanceCommandError('invalid_command');
  }
  const primaryKeyField = primaryKeyColumns[0].name;
  const primaryKeyValue = primaryKeyColumns[0].value;
  const action = requiredString(payload.operation);
  const columns = payload.columns === undefined ? [] : decodeColumnValues(payload.columns);
  const values: SQLiteBindValue[] = [];
  switch (action) {
    case 'insert': {
      const fields = decodeColumns(columns);
      const names = [primaryKeyField, ...fields.map((field) => field.name)];
      values.push(primaryKeyValue, ...fields.map((field) => field.value));
      return {
        sql: `INSERT INTO ${quoteIdentifier(tableName)} (${names.map(quoteIdentifier).join(', ')}) VALUES (${names.map(() => '?').join(', ')})`,
        values,
      };
    }
    case 'update': {
      const fields = decodeColumns(columns);
      if (fields.length === 0) {
        throw new ConformanceCommandError('invalid_command');
      }
      values.push(...fields.map((field) => field.value), primaryKeyValue);
      return {
        sql: `UPDATE ${quoteIdentifier(tableName)} SET ${fields.map((field) => `${quoteIdentifier(field.name)} = ?`).join(', ')} WHERE ${quoteIdentifier(primaryKeyField)} = ?`,
        values,
      };
    }
    case 'delete':
      if (columns.length !== 0) {
        throw new ConformanceCommandError('invalid_command');
      }
      return {
        sql: `DELETE FROM ${quoteIdentifier(tableName)} WHERE ${quoteIdentifier(primaryKeyField)} = ?`,
        values: [primaryKeyValue],
      };
    default:
      throw new ConformanceUnavailableError('local write operation is not exposed by the public JavaScript client');
  }
}

function decodeColumnValues(value: unknown): unknown[] {
  if (Array.isArray(value)) {
    return value;
  }
  const fields = requiredRecord(value);
  return Object.entries(fields).map(([field_id, fieldValue]) => ({
    field_id,
    value: fieldValue,
  }));
}

function decodeColumns(values: unknown[]): Array<{ name: string; value: SQLiteBindValue }> {
  const fields = values.map((value) => {
    const column = requiredRecord(value);
    return { name: requiredIdentifier(column.field_id), value: sqliteBindValue(column.value) };
  });
  if (new Set(fields.map((field) => field.name)).size !== fields.length) {
    throw new ConformanceCommandError('invalid_command');
  }
  return fields.sort((left, right) => left.name.localeCompare(right.name));
}

function decodeSelectors(value: unknown): RowSelector[] {
  if (value === undefined) {
    throw new ConformanceUnavailableError('application row capture requires public row selectors');
  }
  const selectors = requiredArray(value).map((item) => {
    const selector = requiredRecord(item);
    const primaryKey = selector.primary_key;
    if (!isJSONScalar(primaryKey)) {
      throw new ConformanceCommandError('invalid_command');
    }
    return {
      table_name: requiredIdentifier(selector.table_name),
      primary_key_field: requiredIdentifier(selector.primary_key_field),
      primary_key: primaryKey,
    };
  });
  if (new Set(selectors.map((selector) => `${selector.table_name}\u0000${selector.primary_key_field}\u0000${String(selector.primary_key)}`)).size !== selectors.length) {
    throw new ConformanceCommandError('invalid_command');
  }
  return selectors;
}

function durableProofIdentity(
  value: unknown,
  scopeRows: ScopeRowInspection[]
): { tableName: string; recordID: string } {
  if (value === undefined) {
    if (scopeRows.length !== 1) {
      throw new ConformanceCommandError('capture_inspection_failed');
    }
    return { tableName: scopeRows[0].tableName, recordID: scopeRows[0].recordID };
  }
  const identity = requiredRecord(value);
  if (Object.keys(identity).length !== 2) {
    throw new ConformanceCommandError('invalid_command');
  }
  return {
    tableName: requiredIdentifier(identity.table_name),
    recordID: requiredString(identity.record_id),
  };
}

async function captureRows(client: SynchroClient, selectors: RowSelector[]): Promise<Row[]> {
  const rows: Row[] = [];
  for (const selector of selectors) {
    const result = await client.query(
      `SELECT * FROM ${quoteIdentifier(selector.table_name)} WHERE ${quoteIdentifier(selector.primary_key_field)} = ?`,
      [sqliteBindValue(selector.primary_key)]
    );
    rows.push(...result);
    if (rows.length > MAXIMUM_CAPTURE_VALUES) {
      throw new ConformanceCommandError('execution_failed');
    }
  }
  return rows;
}

function rawStatus(status: SyncStatus): RawSyncStatus {
  return {
    state: status.status,
    retry_at: status.retryAt?.toISOString() ?? null,
    operation: status.operation,
    failure: status.failure === null ? null : rawFailure(status.failure),
  };
}

function rawFailure(failure: SyncFailure): RawFailure {
  return {
    operation: failure.operation,
    code: failure.code,
    retryable: failure.retryable,
    recovery_action: failure.recoveryAction,
  };
}

function rawEvents(events: SyncEvent[]): RawEvent[] {
  return events.map((event): RawEvent => {
    switch (event.type) {
      case 'state_changed':
        return { type: event.type, from: event.from, to: event.to };
      case 'backoff':
        return {
          type: event.type,
          operation: event.operation,
          attempt: event.attempt,
          retry_at: event.retryAt.toISOString(),
        };
      case 'schema_applying':
      case 'schema_applied':
        return {
          type: event.type,
          source: { version: event.source.version, hash: event.source.hash },
          target: { version: event.target.version, hash: event.target.hash },
          action: event.action,
        };
      case 'mutation_accepted':
      case 'mutation_rejected':
        return {
          type: event.type,
          mutation_id: event.mutationID,
          table_id: event.tableID,
          rejection_code: event.rejectionCode,
        };
      case 'rebuild_requested':
      case 'rebuild_completed':
        return { type: event.type, scope_id: event.scopeID, rebuild_id: event.rebuildID };
      case 'failure':
        return { type: event.type, failure: rawFailure(event.failure) };
    }
  });
}

function bounded<T>(values: T[]): T[] {
  if (values.length > MAXIMUM_CAPTURE_VALUES) {
    throw new ConformanceCommandError('execution_failed');
  }
  return values;
}

function requiredRecord(value: unknown): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new ConformanceCommandError('invalid_command');
  }
  return value as Record<string, unknown>;
}

function requiredArray(value: unknown): unknown[] {
  if (!Array.isArray(value) || value.length > MAXIMUM_CAPTURE_VALUES) {
    throw new ConformanceCommandError('invalid_command');
  }
  return value;
}

function requiredStringArray(value: unknown): string[] {
  const result = requiredArray(value).map(requiredString);
  if (new Set(result).size !== result.length) {
    throw new ConformanceCommandError('invalid_command');
  }
  return result;
}

function requiredString(value: unknown): string {
  if (typeof value !== 'string' || value.length === 0 || value.length > 4096) {
    throw new ConformanceCommandError('invalid_command');
  }
  return value;
}

function requireSynchronizeMethod(value: unknown): SynchronizeMethod {
  if (!isSynchronizeMethod(value)) {
    throw new ConformanceCommandError('invalid_command');
  }
  return value;
}

function requireCallID(value: unknown): string {
  if (!isCallID(value)) {
    throw new ConformanceCommandError('invalid_command');
  }
  return value;
}

function requiredIdentifier(value: unknown): string {
  const identifier = requiredString(value);
  if (!IDENTIFIER_PATTERN.test(identifier) || identifier.startsWith('_synchro_')) {
    throw new ConformanceCommandError('invalid_command');
  }
  return identifier;
}

function quoteIdentifier(identifier: string): string {
  return `"${identifier}"`;
}

function sqliteBindValue(value: unknown): SQLiteBindValue {
  if (
    value === null ||
    typeof value === 'string' ||
    typeof value === 'boolean' ||
    (typeof value === 'number' && Number.isFinite(value) && (!Number.isInteger(value) || Number.isSafeInteger(value)))
  ) {
    return value;
  }
  const typed = requiredRecord(value);
  if (
    typed.type === 'bytes' &&
    typeof typed.base64 === 'string' &&
    isCanonicalBase64Url(typed.base64) &&
    Object.keys(typed).length === 2
  ) {
    return { type: 'bytes', base64: typed.base64 };
  }
  if (
    typed.type === 'int64' &&
    typeof typed.value === 'string' &&
    isCanonicalInt64(typed.value) &&
    Object.keys(typed).length === 2
  ) {
    return { type: 'int64', value: typed.value };
  }
  throw new ConformanceUnavailableError('local value is not a public SQLite bind value');
}

function isJSONScalar(value: unknown): value is JSONScalar {
  return (
    value === null ||
    typeof value === 'string' ||
    typeof value === 'boolean' ||
    (typeof value === 'number' &&
      Number.isFinite(value) &&
      (!Number.isInteger(value) || Number.isSafeInteger(value)))
  );
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function appPrivateDatabasePath(value: string): string {
  const name = requiredString(value);
  if (!DATABASE_NAME_PATTERN.test(name) || name.includes('..')) {
    throw new ConformanceCommandError('invalid_command');
  }
  return name.endsWith('.db') ? name : `${name}.db`;
}

function appPrivateSeedPath(value: string): string {
  const name = requiredString(value);
  if (!DATABASE_NAME_PATTERN.test(name) || name.includes('..')) {
    throw new ConformanceCommandError('invalid_command');
  }
  return name;
}

function normalizeDurableProof(value: DurableStateInspection): RawDurableProof {
  return {
    row_metadata: value.row_metadata,
    rebuild_receipt_proofs: value.rebuild_receipts.map((receipt) => ({
      rebuild_id_fingerprint: receipt.rebuild_id_fingerprint,
      page_count: receipt.page_count,
      returned_record_count: receipt.returned_record_count,
      request_chain_valid: equalStrings(
        receipt.request_chain_expected,
        receipt.request_chain_observed
      ),
      records_in_canonical_order:
        new Set(receipt.record_identities_hex).size === receipt.record_identities_hex.length &&
        equalStrings(receipt.record_identities_hex, [...receipt.record_identities_hex].sort()),
      row_checksums_valid: equalStrings(
        receipt.received_row_checksums,
        receipt.computed_row_checksums
      ),
      scope_checksum_valid:
        receipt.computed_scope_checksum !== null &&
        receipt.computed_scope_checksum === receipt.final_scope_checksum,
      final_checksum_matches_local:
        receipt.final_scope_checksum !== null &&
        receipt.final_scope_checksum === receipt.stored_scope_checksum &&
        receipt.final_scope_checksum === receipt.local_scope_checksum,
    })),
  };
}

function equalStrings(left: string[], right: string[]): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}
