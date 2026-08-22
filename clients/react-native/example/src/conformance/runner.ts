import { SynchroClient } from '@trainstar/synchro-react-native';
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
  constructor(readonly code: ConformanceErrorCode) {
    super(code);
    this.name = 'ConformanceCommandError';
  }
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
  sync_status?: RawSyncStatus;
  sync_events?: RawEvent[];
}

export interface PublicConformanceRunnerOptions {
  serverURL: string;
  authToken: string;
  appVersion: string;
}

interface ClientSession {
  runtime: ConformanceCommand['runtime'];
  client: SynchroClient | null;
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
  private readonly createdDatabasePaths = new Set<string>();
  private activeClientKey: string | null = null;
  private readonly processID = `rn-${Date.now()}-${Math.random().toString(36).slice(2, 12)}`;

  constructor(private readonly options: PublicConformanceRunnerOptions) {}

  async execute(command: ConformanceCommand): Promise<ConformanceActionResult> {
    try {
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
      throw new ConformanceCommandError('execution_failed');
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
    if (mode === 'create' && this.createdDatabasePaths.has(databasePath)) {
      throw new ConformanceCommandError('invalid_command');
    }
    if (mode === 'create') {
      this.createdDatabasePaths.add(databasePath);
    }
    this.sessions.set(clientKey, {
      runtime: {
        ...command.runtime,
        database_path: databasePath,
        ...(command.runtime.seed_database_path === undefined
          ? {}
          : { seed_database_path: appPrivateSeedPath(command.runtime.seed_database_path) }),
      },
      client: null,
      events: [],
      unsubscribeEvents: null,
    });
    const client = await this.activate(clientKey);
    return {
      kind: 'opened',
      status: rawStatus(await client.getSyncStatus()),
      process: await this.processIdentity(client),
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
      process: await this.processIdentity(client),
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
      process: await this.processIdentity(client),
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
      process: await this.processIdentity(client),
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
      process: await this.processIdentity(client),
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
      process: await this.processIdentity(client),
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
      process: await this.processIdentity(client),
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
        default:
          throw new ConformanceUnavailableError('capture source is not exposed by the public JavaScript client');
      }
    }
    return { kind: 'capture', capture, process: await this.processIdentity(client) };
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
    }
    const client = new SynchroClient({
      dbPath: session.runtime.database_path,
      serverURL: this.options.serverURL,
      authProvider: async () => this.options.authToken,
      clientID: session.runtime.client_id,
      appVersion: this.options.appVersion,
      syncInterval: 3600,
      pushDebounce: 3600,
      seedDatabasePath: session.runtime.seed_database_path,
    });
    await client.initialize();
    session.client = client;
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
    while (Date.now() < deadline) {
      const status = await client.getSyncStatus();
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
    throw new ConformanceCommandError('execution_failed');
  }

  private async processIdentity(client: SynchroClient): Promise<RawProcessIdentity> {
    return {
      process_id: this.processID,
      database_identity_fingerprint: sha256(await client.getPath()),
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
  const primaryKey = requiredRecord(payload.pk);
  const primaryKeyField = requiredIdentifier(primaryKey.field_id);
  const primaryKeyValue = sqliteBindValue(primaryKey.value);
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
    isCanonicalBase64URL(typed.base64) &&
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

function isCanonicalBase64URL(value: string): boolean {
  if (!/^[A-Za-z0-9_-]*$/.test(value) || value.length % 4 === 1) {
    return false;
  }
  const alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_';
  const last = alphabet.indexOf(value[value.length - 1]);
  if (value.length % 4 === 2) {
    return (last & 0x0f) === 0;
  }
  if (value.length % 4 === 3) {
    return (last & 0x03) === 0;
  }
  return true;
}

function isCanonicalInt64(value: string): boolean {
  if (!/^(?:0|-?[1-9][0-9]*)$/.test(value)) {
    return false;
  }
  try {
    const parsed = BigInt(value);
    return (
      parsed >= -(BigInt(1) << BigInt(63)) &&
      parsed <= (BigInt(1) << BigInt(63)) - BigInt(1) &&
      parsed.toString() === value
    );
  } catch {
    return false;
  }
}

function sha256(value: string): string {
  const bytes = utf8(value);
  const bitLength = bytes.length * 8;
  bytes.push(0x80);
  while ((bytes.length + 8) % 64 !== 0) {
    bytes.push(0);
  }
  for (let shift = 56; shift >= 0; shift -= 8) {
    bytes.push(Math.floor(bitLength / 2 ** shift) & 0xff);
  }
  const hash = [
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
    0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
  ];
  const constants = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
  ];
  for (let offset = 0; offset < bytes.length; offset += 64) {
    const words = new Array<number>(64).fill(0);
    for (let index = 0; index < 16; index += 1) {
      words[index] = ((bytes[offset + index * 4] << 24) | (bytes[offset + index * 4 + 1] << 16) | (bytes[offset + index * 4 + 2] << 8) | bytes[offset + index * 4 + 3]) >>> 0;
    }
    for (let index = 16; index < 64; index += 1) {
      const a = words[index - 15];
      const b = words[index - 2];
      const s0 = rotateRight(a, 7) ^ rotateRight(a, 18) ^ (a >>> 3);
      const s1 = rotateRight(b, 17) ^ rotateRight(b, 19) ^ (b >>> 10);
      words[index] = (words[index - 16] + s0 + words[index - 7] + s1) >>> 0;
    }
    let [a, b, c, d, e, f, g, h] = hash;
    for (let index = 0; index < 64; index += 1) {
      const s1 = rotateRight(e, 6) ^ rotateRight(e, 11) ^ rotateRight(e, 25);
      const choice = (e & f) ^ (~e & g);
      const temp1 = (h + s1 + choice + constants[index] + words[index]) >>> 0;
      const s0 = rotateRight(a, 2) ^ rotateRight(a, 13) ^ rotateRight(a, 22);
      const majority = (a & b) ^ (a & c) ^ (b & c);
      const temp2 = (s0 + majority) >>> 0;
      h = g;
      g = f;
      f = e;
      e = (d + temp1) >>> 0;
      d = c;
      c = b;
      b = a;
      a = (temp1 + temp2) >>> 0;
    }
    hash[0] = (hash[0] + a) >>> 0;
    hash[1] = (hash[1] + b) >>> 0;
    hash[2] = (hash[2] + c) >>> 0;
    hash[3] = (hash[3] + d) >>> 0;
    hash[4] = (hash[4] + e) >>> 0;
    hash[5] = (hash[5] + f) >>> 0;
    hash[6] = (hash[6] + g) >>> 0;
    hash[7] = (hash[7] + h) >>> 0;
  }
  return hash.map((word) => word.toString(16).padStart(8, '0')).join('');
}

function rotateRight(value: number, shift: number): number {
  return (value >>> shift) | (value << (32 - shift));
}

function utf8(value: string): number[] {
  const bytes: number[] = [];
  for (let index = 0; index < value.length; index += 1) {
    let code = value.charCodeAt(index);
    if (code >= 0xd800 && code <= 0xdbff && index + 1 < value.length) {
      const low = value.charCodeAt(index + 1);
      if (low >= 0xdc00 && low <= 0xdfff) {
        code = 0x10000 + ((code - 0xd800) << 10) + low - 0xdc00;
        index += 1;
      }
    }
    if (code < 0x80) {
      bytes.push(code);
    } else if (code < 0x800) {
      bytes.push(0xc0 | (code >> 6), 0x80 | (code & 0x3f));
    } else if (code < 0x10000) {
      bytes.push(0xe0 | (code >> 12), 0x80 | ((code >> 6) & 0x3f), 0x80 | (code & 0x3f));
    } else {
      bytes.push(0xf0 | (code >> 18), 0x80 | ((code >> 12) & 0x3f), 0x80 | ((code >> 6) & 0x3f), 0x80 | (code & 0x3f));
    }
  }
  return bytes;
}
