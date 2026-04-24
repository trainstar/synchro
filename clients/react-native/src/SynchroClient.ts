import { Platform } from 'react-native';
import type { EventSubscription } from 'react-native';
import NativeSynchro from './NativeSynchro';
import { mapNativeError } from './errors';
import type {
  Row,
  ExecResult,
  BatchResult,
  SQLStatement,
  SQLiteBindValue,
  ColumnDef,
  TableOptions,
  Transaction,
  SyncStatus,
  SyncStatusType,
  ConflictEvent,
  SynchroConfig,
  Unsubscribe,
} from './types';

let observerCounter = 0;
function nextObserverID(): string {
  return `obs_${++observerCounter}_${Date.now()}`;
}

function nullableRow(row: unknown): Row | null {
  return row == null ? null : (row as Row);
}

export class SynchroClient {
  private readonly native = NativeSynchro;
  private readonly config: SynchroConfig;
  private authSubscription: EventSubscription | null = null;

  constructor(config: SynchroConfig) {
    this.config = config;

    // Wire auth callback using Codegen EventEmitter pattern
    this.authSubscription = this.native.onAuthRequest(
      (event: { requestID: string }) => {
        this.config
          .authProvider()
          .then((token) => {
            this.native.resolveAuthRequest(event.requestID, token);
          })
          .catch((err) => {
            this.native.rejectAuthRequest(
              event.requestID,
              err instanceof Error ? err.message : String(err)
            );
          });
      }
    );
  }

  async initialize(): Promise<void> {
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
      });
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  // Core SQL

  async query(sql: string, params?: readonly SQLiteBindValue[]): Promise<Row[]> {
    try {
      return [...(await this.native.query(sql, params ?? []))] as Row[];
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  async queryOne(sql: string, params?: readonly SQLiteBindValue[]): Promise<Row | null> {
    try {
      return nullableRow(await this.native.queryOne(sql, params ?? []));
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  async execute(sql: string, params?: readonly SQLiteBindValue[]): Promise<ExecResult> {
    try {
      return await this.native.execute(sql, params ?? []);
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  async executeBatch(statements: SQLStatement[]): Promise<BatchResult> {
    try {
      const payload = statements.map((s) => ({
        sql: s.sql,
        params: s.params ?? [],
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
          return [...(await this.native.txQuery(txID, sql, params ?? []))] as Row[];
        },
        queryOne: async (sql, params) => {
          return nullableRow(await this.native.txQueryOne(txID, sql, params ?? []));
        },
        execute: async (sql, params) => {
          return await this.native.txExecute(txID, sql, params ?? []);
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
    callback: (tx: Transaction) => Promise<T>
  ): Promise<T> {
    let txID: string;
    try {
      txID = await this.native.beginReadTransaction();
    } catch (error) {
      throw mapNativeError(error);
    }

    try {
      const tx: Transaction = {
        query: async (sql, params) => {
          return [...(await this.native.txQuery(txID, sql, params ?? []))] as Row[];
        },
        queryOne: async (sql, params) => {
          return nullableRow(await this.native.txQueryOne(txID, sql, params ?? []));
        },
        execute: async (sql, params) => {
          return await this.native.txExecute(txID, sql, params ?? []);
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

  onChange(tables: string[], callback: () => void): Unsubscribe {
    const observerID = nextObserverID();

    const subscription = this.native.onChange(
      (event: { observerID: string }) => {
        if (event.observerID === observerID) {
          callback();
        }
      }
    );

    this.native.addChangeObserver(observerID, tables);

    return () => {
      subscription.remove();
      this.native.removeObserver(observerID);
    };
  }

  watch(
    sql: string,
    params: readonly SQLiteBindValue[] | undefined,
    tables: string[],
    callback: (rows: Row[]) => void
  ): Unsubscribe {
    const observerID = nextObserverID();

    const subscription = this.native.onQueryResult(
      (event) => {
        if (event.observerID === observerID) {
          callback([...(event.rows as readonly Row[])]);
        }
      }
    );

    this.native.addQueryObserver(
      observerID,
      sql,
      params ?? [],
      tables
    );

    return () => {
      subscription.remove();
      this.native.removeObserver(observerID);
    };
  }

  // Lifecycle

  async close(): Promise<void> {
    try {
      await this.native.close();
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

  async syncNow(): Promise<void> {
    try {
      await this.native.syncNow();
    } catch (error) {
      throw mapNativeError(error);
    }
  }

  // Status and events

  onStatusChange(callback: (status: SyncStatus) => void): Unsubscribe {
    const subscription = this.native.onStatusChange(
      (event: { status: string; retryAt: string | null }) => {
        callback({
          status: event.status as SyncStatusType,
          retryAt: event.retryAt ? new Date(event.retryAt) : null,
        });
      }
    );
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
