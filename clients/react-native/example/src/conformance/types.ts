export type JSONScalar = null | boolean | number | string;
export type JSONValue = JSONScalar | JSONValue[] | { [key: string]: JSONValue };
export type JSONObject = { [key: string]: JSONValue };

export const MAXIMUM_COMMAND_BYTES = 1024 * 1024;
export const MAXIMUM_RESULT_BYTES = 1024 * 1024;
export const MAXIMUM_JSON_DEPTH = 24;
export const MAXIMUM_OBJECT_KEYS = 256;
export const MAXIMUM_ARRAY_ITEMS = 512;
export const MAXIMUM_VALUE_BYTES = 1024 * 1024;

export type SynchronizeMethod =
  | 'start'
  | 'sync-now'
  | 'retry-after-error'
  | 'reset-schema-and-start';
export type SynchronizeCompletion = 'idle' | 'blocked' | 'error';
export type LifecycleOperation = 'stop' | 'enter-background' | 'enter-foreground';

export interface ScenarioOperation {
  contract_operation: string;
  name: string;
  payload: JSONObject;
}

export interface ScenarioStep {
  operation: ScenarioOperation;
}

export interface ConformanceAction {
  actor: string;
  command: string;
  parameters: JSONObject;
}

export interface NativeManifestAction {
  action: ConformanceAction;
  steps: ScenarioStep[];
}

export interface ConformanceRuntime {
  client_key: string;
  database_path: string;
  client_id: string;
  seed_database_path?: string;
  server_url?: string;
  auth_token?: string;
}

export interface ConformanceCommand {
  schema_version: 1;
  action: NativeManifestAction;
  runtime: ConformanceRuntime;
}

export type ConformanceErrorCode =
  | 'invalid_command'
  | 'unavailable'
  | 'execution_failed'
  | 'capture_query_failed'
  | 'capture_row_cardinality'
  | 'capture_inspection_failed';

export interface ConformanceEnvelope {
  schema_version: 1;
  outcome: 'passed' | 'error';
  result: JSONValue | null;
  error_code: ConformanceErrorCode | null;
}

const SYNCHRONIZE_METHODS: readonly SynchronizeMethod[] = [
  'start',
  'sync-now',
  'retry-after-error',
  'reset-schema-and-start',
];
const LIFECYCLE_OPERATIONS: readonly LifecycleOperation[] = [
  'stop',
  'enter-background',
  'enter-foreground',
];
const CALL_ID_PATTERN = /^[a-z][a-z0-9_-]{0,127}$/;

export function isSynchronizeMethod(value: unknown): value is SynchronizeMethod {
  return typeof value === 'string' && SYNCHRONIZE_METHODS.includes(value as SynchronizeMethod);
}

export function isLifecycleOperation(value: unknown): value is LifecycleOperation {
  return typeof value === 'string' && LIFECYCLE_OPERATIONS.includes(value as LifecycleOperation);
}

export function isCallID(value: unknown): value is string {
  return typeof value === 'string' && CALL_ID_PATTERN.test(value);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function exactKeys(
  value: Record<string, unknown>,
  required: readonly string[],
  optional: readonly string[],
  name: string
): void {
  const allowed = new Set([...required, ...optional]);
  if (
    required.some((key) => !Object.prototype.hasOwnProperty.call(value, key)) ||
    Object.keys(value).some((key) => !allowed.has(key))
  ) {
    throw new Error(`${name} has invalid members`);
  }
}

function requiredRecord(value: unknown, name: string): Record<string, unknown> {
  if (!isRecord(value)) {
    throw new Error(`${name} must be an object`);
  }
  return value;
}

function requiredString(value: unknown, name: string, maximum = 4096): string {
  if (typeof value !== 'string' || value.length === 0 || utf8ByteLength(value) > maximum) {
    throw new Error(`${name} must be a bounded nonempty string`);
  }
  return value;
}

function requiredArray(value: unknown, name: string): unknown[] {
  if (!Array.isArray(value) || value.length > MAXIMUM_ARRAY_ITEMS) {
    throw new Error(`${name} must be a bounded array`);
  }
  return value;
}

function jsonObject(value: unknown, name: string): JSONObject {
  return requiredRecord(value, name) as JSONObject;
}

function decodeOperation(value: unknown, name: string): ScenarioOperation {
  const record = requiredRecord(value, name);
  exactKeys(record, ['contract_operation', 'name', 'payload'], [], name);
  return {
    contract_operation: requiredString(record.contract_operation, `${name}.contract_operation`, 128),
    name: requiredString(record.name, `${name}.name`, 128),
    payload: jsonObject(record.payload, `${name}.payload`),
  };
}

function decodeStep(value: unknown, name: string): ScenarioStep {
  const record = requiredRecord(value, name);
  exactKeys(record, ['operation'], [], name);
  return { operation: decodeOperation(record.operation, `${name}.operation`) };
}

function decodeAction(value: unknown): ConformanceAction {
  const record = requiredRecord(value, 'action.action');
  exactKeys(record, ['actor', 'command', 'parameters'], [], 'action.action');
  return {
    actor: requiredString(record.actor, 'action.action.actor', 32),
    command: requiredString(record.command, 'action.action.command', 64),
    parameters: jsonObject(record.parameters, 'action.action.parameters'),
  };
}

function decodeManifestAction(value: unknown): NativeManifestAction {
  const record = requiredRecord(value, 'action');
  exactKeys(record, ['action', 'steps'], [], 'action');
  return {
    action: decodeAction(record.action),
    steps: requiredArray(record.steps, 'action.steps').map((step, index) =>
      decodeStep(step, `action.steps[${index}]`)
    ),
  };
}

function optionalString(value: unknown, name: string, maximum = 4096): string | undefined {
  return value === undefined ? undefined : requiredString(value, name, maximum);
}

function decodeRuntime(value: unknown): ConformanceRuntime {
  const record = requiredRecord(value, 'runtime');
  exactKeys(
    record,
    ['client_key', 'database_path', 'client_id'],
    ['seed_database_path', 'server_url', 'auth_token'],
    'runtime'
  );
  const seedDatabasePath = optionalString(record.seed_database_path, 'runtime.seed_database_path');
  const serverURL = optionalString(record.server_url, 'runtime.server_url');
  const authToken = optionalString(record.auth_token, 'runtime.auth_token', 16384);
  return {
    client_key: requiredString(record.client_key, 'runtime.client_key', 128),
    database_path: requiredString(record.database_path, 'runtime.database_path'),
    client_id: requiredString(record.client_id, 'runtime.client_id', 128),
    ...(seedDatabasePath === undefined ? {} : { seed_database_path: seedDatabasePath }),
    ...(serverURL === undefined ? {} : { server_url: serverURL }),
    ...(authToken === undefined ? {} : { auth_token: authToken }),
  };
}

/** Parses one sanitized host command without loading scenario identity or expectations. */
export function parseConformanceCommand(source: string): ConformanceCommand {
  if (utf8ByteLength(source) > MAXIMUM_COMMAND_BYTES) {
    throw new Error('command exceeds its byte bound');
  }
  rejectDuplicateJSONMembers(source);
  return decodeConformanceCommand(JSON.parse(source));
}

export function decodeConformanceCommand(value: unknown): ConformanceCommand {
  assertBoundedJSON(value, MAXIMUM_COMMAND_BYTES);
  const record = requiredRecord(value, 'command');
  exactKeys(record, ['schema_version', 'action', 'runtime'], [], 'command');
  if (record.schema_version !== 1) {
    throw new Error('command.schema_version must be 1');
  }
  return {
    schema_version: 1,
    action: decodeManifestAction(record.action),
    runtime: decodeRuntime(record.runtime),
  };
}

export function encodeConformanceEnvelope(envelope: ConformanceEnvelope): string {
  const keys = Object.keys(envelope);
  if (
    keys.length !== 4 ||
    !['schema_version', 'outcome', 'result', 'error_code'].every((key) => keys.includes(key)) ||
    envelope.schema_version !== 1 ||
    (envelope.outcome === 'passed') !== (envelope.result !== null && envelope.error_code === null) ||
    (envelope.outcome === 'error') !== (envelope.result === null && envelope.error_code !== null)
  ) {
    throw new Error('conformance response envelope is invalid');
  }
  assertBoundedJSON(envelope, MAXIMUM_RESULT_BYTES);
  return JSON.stringify(envelope);
}

export function assertBoundedJSON(value: unknown, maximumBytes: number): void {
  let values = 0;
  const visit = (current: unknown, depth: number): void => {
    values += 1;
    if (values > 16384 || depth > MAXIMUM_JSON_DEPTH) {
      throw new Error('JSON value exceeds its structural bound');
    }
    if (current === null || typeof current === 'boolean') return;
    if (typeof current === 'number') {
      if (!Number.isFinite(current)) throw new Error('JSON number is not finite');
      return;
    }
    if (typeof current === 'string') {
      if (utf8ByteLength(current) > MAXIMUM_VALUE_BYTES) {
        throw new Error('JSON string exceeds its byte bound');
      }
      return;
    }
    if (Array.isArray(current)) {
      if (current.length > MAXIMUM_ARRAY_ITEMS) throw new Error('JSON array exceeds its item bound');
      current.forEach((item) => visit(item, depth + 1));
      return;
    }
    if (!isRecord(current)) throw new Error('JSON value is unsupported');
    const entries = Object.entries(current);
    if (entries.length > MAXIMUM_OBJECT_KEYS) throw new Error('JSON object exceeds its key bound');
    entries.forEach(([key, child]) => {
      if (utf8ByteLength(key) > 128) throw new Error('JSON key exceeds its byte bound');
      visit(child, depth + 1);
    });
  };
  visit(value, 0);
  const encoded = JSON.stringify(value);
  if (typeof encoded !== 'string' || utf8ByteLength(encoded) > maximumBytes) {
    throw new Error('JSON value exceeds its byte bound');
  }
}

function rejectDuplicateJSONMembers(source: string): void {
  let index = 0;
  const whitespace = () => {
    while (/\s/.test(source[index] ?? '')) index += 1;
  };
  const stringToken = (): string => {
    const start = index;
    if (source[index++] !== '"') throw new Error('JSON string is invalid');
    while (index < source.length) {
      const character = source[index++];
      if (character === '"') return JSON.parse(source.slice(start, index)) as string;
      if (character === '\\') {
        if (source[index] === 'u') index += 5;
        else index += 1;
      }
    }
    throw new Error('JSON string is incomplete');
  };
  const value = (): void => {
    whitespace();
    if (source[index] === '{') {
      index += 1;
      whitespace();
      const members = new Set<string>();
      if (source[index] === '}') {
        index += 1;
        return;
      }
      while (true) {
        whitespace();
        const key = stringToken();
        if (members.has(key)) throw new Error('JSON object contains a duplicate member');
        members.add(key);
        whitespace();
        if (source[index++] !== ':') throw new Error('JSON object is invalid');
        value();
        whitespace();
        const delimiter = source[index++];
        if (delimiter === '}') return;
        if (delimiter !== ',') throw new Error('JSON object is invalid');
      }
    }
    if (source[index] === '[') {
      index += 1;
      whitespace();
      if (source[index] === ']') {
        index += 1;
        return;
      }
      while (true) {
        value();
        whitespace();
        const delimiter = source[index++];
        if (delimiter === ']') return;
        if (delimiter !== ',') throw new Error('JSON array is invalid');
      }
    }
    if (source[index] === '"') {
      stringToken();
      return;
    }
    const start = index;
    while (index < source.length && !/[\s,}\]]/.test(source[index])) index += 1;
    if (start === index) throw new Error('JSON value is invalid');
  };
  value();
  whitespace();
  if (index !== source.length) throw new Error('JSON contains trailing data');
}

export function utf8ByteLength(value: string): number {
  let length = 0;
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code < 0x80) length += 1;
    else if (code < 0x800) length += 2;
    else if (code >= 0xd800 && code <= 0xdbff && index + 1 < value.length) {
      const low = value.charCodeAt(index + 1);
      if (low >= 0xdc00 && low <= 0xdfff) {
        length += 4;
        index += 1;
      } else length += 3;
    } else length += 3;
  }
  return length;
}
