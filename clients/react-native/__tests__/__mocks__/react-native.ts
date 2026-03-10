// Mock for react-native TurboModule + Codegen EventEmitter

export type EventSubscription = { remove: () => void };

const listeners: Record<string, Array<(...args: any[]) => void>> = {};

// Helper to create a mock EventEmitter function (Codegen pattern)
function createEventEmitter(eventName: string) {
  return jest.fn((callback: (...args: any[]) => void) => {
    if (!listeners[eventName]) listeners[eventName] = [];
    listeners[eventName].push(callback);
    return {
      remove: jest.fn(() => {
        const idx = listeners[eventName]?.indexOf(callback);
        if (idx !== undefined && idx >= 0) listeners[eventName].splice(idx, 1);
      }),
    };
  });
}

// The mock native module — tests can override individual methods
export const mockNativeModule: Record<string, jest.Mock> = {
  initialize: jest.fn().mockResolvedValue(undefined),
  close: jest.fn().mockResolvedValue(undefined),
  getPath: jest.fn().mockResolvedValue('/mock/path'),
  query: jest.fn().mockResolvedValue('[]'),
  queryOne: jest.fn().mockResolvedValue(null),
  execute: jest.fn().mockResolvedValue({ rowsAffected: 0 }),
  executeBatch: jest.fn().mockResolvedValue({ totalRowsAffected: 0 }),
  beginWriteTransaction: jest.fn().mockResolvedValue('tx-1'),
  beginReadTransaction: jest.fn().mockResolvedValue('tx-1'),
  txQuery: jest.fn().mockResolvedValue('[]'),
  txQueryOne: jest.fn().mockResolvedValue(null),
  txExecute: jest.fn().mockResolvedValue({ rowsAffected: 0 }),
  commitTransaction: jest.fn().mockResolvedValue(undefined),
  rollbackTransaction: jest.fn().mockResolvedValue(undefined),
  createTable: jest.fn().mockResolvedValue(undefined),
  alterTable: jest.fn().mockResolvedValue(undefined),
  createIndex: jest.fn().mockResolvedValue(undefined),
  addChangeObserver: jest.fn().mockResolvedValue(undefined),
  addQueryObserver: jest.fn().mockResolvedValue(undefined),
  removeObserver: jest.fn().mockResolvedValue(undefined),
  checkpoint: jest.fn().mockResolvedValue(undefined),
  start: jest.fn().mockResolvedValue(undefined),
  stop: jest.fn().mockResolvedValue(undefined),
  syncNow: jest.fn().mockResolvedValue(undefined),
  pendingChangeCount: jest.fn().mockResolvedValue(0),
  resolveAuthRequest: jest.fn(),
  rejectAuthRequest: jest.fn(),
  resolveSnapshotRequest: jest.fn(),
  addListener: jest.fn(),
  removeListeners: jest.fn(),
  // Codegen EventEmitter pattern
  onStatusChange: createEventEmitter('onStatusChange'),
  onConflict: createEventEmitter('onConflict'),
  onAuthRequest: createEventEmitter('onAuthRequest'),
  onSnapshotRequired: createEventEmitter('onSnapshotRequired'),
  onChange: createEventEmitter('onChange'),
  onQueryResult: createEventEmitter('onQueryResult'),
};

export const TurboModuleRegistry = {
  getEnforcing: jest.fn(() => mockNativeModule),
};

export const Platform = {
  OS: 'ios',
  select: jest.fn((obj: any) => obj.ios),
};

export function resetNativeModuleMockState() {
  Object.values(mockNativeModule).forEach((value) => {
    if (typeof value?.mockClear === 'function') {
      value.mockClear();
    }
  });
  Object.keys(listeners).forEach((eventName) => {
    delete listeners[eventName];
  });
}

// Helper: emit a native event to all JS listeners
export function emitNativeEvent(eventName: string, data: any) {
  listeners[eventName]?.forEach((cb) => cb(data));
}
