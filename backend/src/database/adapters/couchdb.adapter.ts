// backend/src/database/adapters/couchdb.adapter.ts

import { IBaseDatabaseInspector } from '../inspection/base-inspector';
import {
  DatabaseConnection,
  QueryResult,
  TableInfo,
  ColumnMetadata,
  DatabaseConfig,
  InspectionOptions,
  QueryExecutionOptions,
  DatabaseVersionInfo,
} from '../types/inspection.types';

import nano, { ServerScope, DocumentScope } from 'nano';

// ---------------------------------------------------------------------------
// CouchDBConnection – satisfies DatabaseConnection
// ---------------------------------------------------------------------------
export class CouchDBConnection implements DatabaseConnection {
  private server: ServerScope | null = null;
  private db: DocumentScope<any> | null = null;
  private dbName: string = '';
  private _connected = false;

  async connect(server: ServerScope, dbName: string): Promise<void> {
    this.server = server;
    this.dbName = dbName;
    this.db = server.use(dbName);
    this._connected = true;
  }

  get connected(): boolean {
    return this._connected;
  }

  async disconnect(): Promise<void> {
    this.db = null;
    this.server = null;
    this._connected = false;
  }

  getServer(): ServerScope | null {
    return this.server;
  }

  getDb(): DocumentScope<any> | null {
    return this.db;
  }

  getDbName(): string {
    return this.dbName;
  }
}

// ---------------------------------------------------------------------------
// CouchDBInspector – all low‑level CouchDB operations
// ---------------------------------------------------------------------------
class CouchDBInspector {
  private connection: CouchDBConnection;
  private server: ServerScope;
  private config: DatabaseConfig;

  constructor(config: DatabaseConfig) {
    this.config = {
      dbname: config.dbname,
      host: config.host || 'localhost',
      port: config.port || '5984',
      user: config.user || '',
      password: config.password || '',
      ssl: config.ssl || false,
    };
    this.connection = new CouchDBConnection();

    const protocol = this.config.ssl ? 'https' : 'http';
    const auth = this.config.user
      ? `${this.config.user}:${this.config.password}@`
      : '';
    const url = `${protocol}://${auth}${this.config.host}:${this.config.port}`;
    // nano(url) returns ServerScope – use an assertion to clean up the outdated type
    this.server = nano(url) as unknown as ServerScope;
  }

  getConnection(): CouchDBConnection {
    return this.connection;
  }

  // ---------------------------------------------------------------------------
  // Connection / test helpers
  // ---------------------------------------------------------------------------
  async connect(): Promise<void> {
    // info() is only available on ServerScope – cast to any to bypass broken types
    await (this.server as any).info();
    await this.connection.connect(this.server, this.config.dbname);
  }

  async testConnection(): Promise<{ success: boolean; version?: string; error?: string }> {
    try {
      const info = await (this.server as any).info();
      return { success: true, version: info.version };
    } catch (error: any) {
      return { success: false, error: error.message || String(error) };
    }
  }

  // ---------------------------------------------------------------------------
  // Table / collection inspection
  // ---------------------------------------------------------------------------
  async getTables(): Promise<TableInfo[]> {
    // db.list() wrongly typed as Request – cast to string[]
    const dbList = (await this.server.db.list()) as unknown as string[];
    const tables: TableInfo[] = [];

    for (const dbName of dbList) {
      if (dbName.startsWith('_')) continue; // skip internal databases

      try {
        const db = this.server.use(dbName);
        const info = (await db.info()) as any; // info again typed as Request

        tables.push({
          schemaname: '',
          tablename: dbName,
          tabletype: 'doc',
          columns: [],
          comment: '',
          rowCount: info.doc_count ?? 0,
          size: (info.data_size ?? 0).toString(),   // TableInfo.size is a string
          originalData: info,
        });
      } catch (_) {
        // Inaccessible database – push minimal entry
        tables.push({
          schemaname: '',
          tablename: dbName,
          tabletype: 'doc',
          columns: [],
          comment: '',
          rowCount: 0,
          size: '0',
          originalData: null,
        });
      }
    }
    return tables;
  }

  async sampleColumns(dbName: string): Promise<ColumnMetadata[]> {
    const db = this.server.use(dbName);
    try {
      const result = (await db.list({ include_docs: true, limit: 1 })) as any;
      if (!result.rows || result.rows.length === 0) return [];
      const doc: any = result.rows[0].doc;
      if (!doc || typeof doc !== 'object') return [];
      const keys = Object.keys(doc).filter((k) => !k.startsWith('_'));
      return keys.map((key, idx) => ({
        name: key,
        type: typeof doc[key],
        dataType: typeof doc[key],
        nullable: true,
        ordinalPosition: idx + 1,
      } as ColumnMetadata));
    } catch {
      return [];
    }
  }

  // ---------------------------------------------------------------------------
  // Database info
  // ---------------------------------------------------------------------------
  async getDatabaseInfo(): Promise<{
    version: string;
    name: string;
    encoding: string;
    collation: string;
  }> {
    const info = await (this.server as any).info();
    return {
      version: info.version ?? '',
      name: info.vendor?.name ?? 'CouchDB',
      encoding: '',
      collation: '',
    };
  }

  // ---------------------------------------------------------------------------
  // Query execution (Mango JSON)
  // ---------------------------------------------------------------------------
  async executeQuery(
    sql: string,
    _params: any[],
    options?: { maxRows?: number; timeout?: number; autoDisconnect?: boolean }
  ): Promise<QueryResult> {
    const db = this.connection.getDb();
    if (!db) throw new Error('Not connected to a database.');

    if (!sql.trim().startsWith('{')) {
      throw new Error('Only Mango JSON queries are supported (string starting with "{").');
    }

    let query: any;
    try {
      query = JSON.parse(sql);
    } catch {
      throw new Error('Invalid Mango JSON query.');
    }

    if (options?.maxRows) {
      query.limit = options.maxRows;
    }

    try {
      // db.find() response is typed as Request – cast to any
      const response = (await db.find(query)) as any;
      const rows = response.docs ?? [];

      // fields must be an array of { name, type }
      const fields = rows.length > 0
        ? Object.keys(rows[0]).map((key) => ({ name: key, type: 'string' }))
        : [];

      return {
        success: true,
        rows,
        fields,
        rowCount: rows.length,
        executionTime: 0,
      };
    } catch (error: any) {
      return {
        success: false,
        error: error.message || String(error),
        rows: [],
        fields: [],
      };
    }
  }

  async executeTransaction(
    queries: Array<{ sql: string; params?: any[] }>
  ): Promise<QueryResult[]> {
    const results: QueryResult[] = [];
    for (const q of queries) {
      results.push(await this.executeQuery(q.sql, q.params || []));
    }
    return results;
  }

  setDatabase(dbName: string): void {
    this.config.dbname = dbName;
  }
}

// ---------------------------------------------------------------------------
// CouchDBAdapter – implements IBaseDatabaseInspector
// ---------------------------------------------------------------------------
export class CouchDBAdapter implements IBaseDatabaseInspector {
  private inspector: CouchDBInspector | null = null;
  private connectionInstance: CouchDBConnection | null = null;

  async connect(config: DatabaseConfig): Promise<DatabaseConnection> {
    try {
      this.inspector = new CouchDBInspector(config);
      await this.inspector.connect();
      this.connectionInstance = this.inspector.getConnection();
      return this.connectionInstance as unknown as DatabaseConnection;
    } catch (error) {
      throw new Error(
        `Failed to connect to CouchDB: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  async disconnect(_connection: DatabaseConnection): Promise<void> {
    try {
      if (this.connectionInstance) {
        await this.connectionInstance.disconnect();
      }
      this.inspector = null;
      this.connectionInstance = null;
    } catch (error) {
      console.error('Error disconnecting from CouchDB:', error);
    }
  }

  async testConnection(
    config: DatabaseConfig
  ): Promise<{ success: boolean; version?: string; error?: string }> {
    const tempInspector = new CouchDBInspector(config);
    return tempInspector.testConnection();
  }

  async getTables(
    _connection: DatabaseConnection,
    _options?: InspectionOptions
  ): Promise<TableInfo[]> {
    if (!this.inspector) throw new Error('Inspector not initialized. Call connect() first.');
    return this.inspector.getTables();
  }

  async getTableColumns(
    _connection: DatabaseConnection,
    tables: TableInfo[]
  ): Promise<TableInfo[]> {
    return tables;
  }

  async getDatabaseInfo(_connection: DatabaseConnection): Promise<DatabaseVersionInfo> {
    if (!this.inspector) throw new Error('Inspector not initialized.');
    const info = await this.inspector.getDatabaseInfo();
    return {
      version: info.version,
      name: info.name,
      encoding: info.encoding,
      collation: info.collation,
    };
  }

  async executeQuery(
    _connection: DatabaseConnection,
    sql: string,
    options?: QueryExecutionOptions
  ): Promise<QueryResult> {
    if (!this.inspector) throw new Error('Inspector not initialized.');
    const params = options?.params || [];
    const inspectorOptions = {
      maxRows: options?.maxRows,
      timeout: options?.timeout,
      autoDisconnect: options?.autoDisconnect,
    };
    return this.inspector.executeQuery(sql, params, inspectorOptions);
  }

  async executeTransaction(
    _connection: DatabaseConnection,
    queries: Array<{ sql: string; params?: any[] }>
  ): Promise<QueryResult[]> {
    if (!this.inspector) throw new Error('Inspector not initialized.');
    return this.inspector.executeTransaction(queries);
  }

  async getTableConstraints(
    _connection: DatabaseConnection,
    _schema: string,
    _table: string
  ): Promise<any[]> {
    return [];
  }

  async getSchemas(_connection: DatabaseConnection): Promise<string[]> {
    return [];
  }

  async getFunctions(_connection: DatabaseConnection, _schema?: string): Promise<any[]> {
    return [];
  }

  async getIndexes(_connection: DatabaseConnection, tableName?: string): Promise<any[]> {
    if (!this.inspector || !this.connectionInstance) throw new Error('Not connected.');
    const db = tableName
      ? (this.inspector as any).server.use(tableName)
      : this.connectionInstance.getDb();
    if (!db) return [];
    try {
      const result = (await db.index()) as any;
      return result.indexes || [];
    } catch {
      return [];
    }
  }
}

export default CouchDBAdapter;