import { readFileSync } from 'node:fs';
import { join } from 'node:path';

const root = join(__dirname, '..');
const ios = readFileSync(join(root, 'ios', 'SynchroModule.swift'), 'utf8');
const android = readFileSync(
  join(root, 'android', 'src', 'main', 'kotlin', 'com', 'trainstar', 'synchro', 'rn', 'SynchroModule.kt'),
  'utf8'
);
const codegen = readFileSync(join(root, 'src', 'NativeSynchro.ts'), 'utf8');

const statuses = [
  'uninitialized',
  'local_ready',
  'connecting',
  'schema_applying',
  'ready',
  'pushing',
  'pulling',
  'rebuilding',
  'backoff',
  'error',
  'stopped',
] as const;

const swiftStatusCases = [
  'uninitialized',
  'localReady',
  'connecting',
  'schemaApplying',
  'ready',
  'pushing',
  'pulling',
  'rebuilding',
  'backoff',
  'error',
  'stopped',
] as const;

const events = [
  ['stateChanged', 'StateChanged', 'state_changed'],
  ['backoff', 'Backoff', 'backoff'],
  ['schemaApplying', 'SchemaApplying', 'schema_applying'],
  ['schemaApplied', 'SchemaApplied', 'schema_applied'],
  ['mutationAccepted', 'MutationAccepted', 'mutation_accepted'],
  ['mutationRejected', 'MutationRejected', 'mutation_rejected'],
  ['rebuildRequested', 'RebuildRequested', 'rebuild_requested'],
  ['rebuildCompleted', 'RebuildCompleted', 'rebuild_completed'],
  ['failure', 'Failure', 'failure'],
] as const;

const errorCodes = [
  'NOT_CONNECTED',
  'SCHEMA_NOT_LOADED',
  'TABLE_NOT_SYNCED',
  'UPGRADE_REQUIRED',
  'SCHEMA_MISMATCH',
  'PUSH_REJECTED',
  'NETWORK_ERROR',
  'SERVER_ERROR',
  'DATABASE_ERROR',
  'INVALID_RESPONSE',
  'INVALID_SEED',
  'SYNC_BLOCKED',
  'UNSUPPORTED_SCHEMA',
  'INVALID_STATE_TRANSITION',
  'INVALID_CONFIG',
  'ALREADY_STARTED',
  'NOT_STARTED',
] as const;

describe('native React Native parity contract', () => {
  it('maps every lifecycle state explicitly on both native platforms', () => {
    statuses.forEach((status) => {
      expect(ios).toContain(`.${swiftStatusCases[statuses.indexOf(status)]}`);
      const kotlinState = status
        .split('_')
        .map((part) => part[0].toUpperCase() + part.slice(1))
        .join('');
      expect(android).toContain(`SyncStatus.${kotlinState}`);
    });
  });

  it('maps every native sync event case explicitly on both native platforms', () => {
    events.forEach(([swiftCase, kotlinCase, wireType]) => {
      expect(ios).toContain(`case .${swiftCase}`);
      expect(ios).toContain(`"${wireType}"`);
      expect(android).toContain(`is SyncEvent.${kotlinCase}`);
      expect(android).toContain(`"${wireType}"`);
    });
    expect(ios).toContain('payload["tableID"] = mutation.tableID');
    expect(android).toContain('payload.putString("tableID", event.mutation.tableID)');
    expect(ios).toContain('payload["rebuildID"] = rebuild.rebuildID');
    expect(android).toContain('payload.putString("rebuildID", event.rebuild.rebuildID)');
  });

  it('maps every shared native error code explicitly on both native platforms', () => {
    errorCodes.forEach((code) => {
      expect(ios).toContain(`"${code}"`);
      expect(android).toContain(`"${code}"`);
    });
  });

  it('declares every lifecycle call and typed event in the codegen interface', () => {
    [
      'enterBackground(): Promise<void>;',
      'enterForeground(): Promise<void>;',
      'retryAfterError(): Promise<void>;',
      'resetSchemaAndStart(): Promise<void>;',
      'readonly onStatusChange:',
      'readonly onSyncEvent:',
      'readonly onConflict:',
    ].forEach((declaration) => expect(codegen).toContain(declaration));
  });
});
