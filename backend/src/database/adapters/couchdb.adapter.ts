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
  DatabaseVersionInfo
} from '../types/inspection.types';

// We'll use the nano library for CouchDB access.
// If nano is not yet installed, add: npm install nano @types/nano
import nano, { DocumentScope, ServerScope, MangoQuery, MangoResponse } from 'nano';

// --- CouchDB Connection Wrapper ---
export class CouchDBConnection implements DatabaseConnection {
  private server: ServerScope | null = null;
  private db: DocumentScope<any> | null = null;
  private dbName: string = '';

  constructor() {}

  /**
   * Initializes the server connection and sets the current database.
   * Actual HTTP connections are lazily managed by nano.
   */
  async connect(server: ServerScope, dbName: string): Promise<void> {
    this.server = server;
    this.dbName = dbName;
    this.db = server.use(dbName);
  }

  async disconnect(): Promise<void> {
    this.db = null;
    this.server = null;
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

// --- CouchDB Inspector ---
class CouchDBInspector {
  private connection: CouchDBConnection;
  private config: DatabaseConfig;
  private server: ServerScope;

  constructor(config: DatabaseConfig) {
    this.config = {
      dbname: config.dbname,
      host: config.host || 'localhost',
      port: config.port || '5984',
      user: config.user || '',
      password: config.password || '',
      schema: config.schema || '' // not used
    };
    this.connection = new CouchDBConnection();

    const protocol = (config.ssl || this.config.port === '6984') ? 'https' : 'http';
    const auth = this.config.user ? `${this.config.user}:${this.config.password}@` : '';
    const url = `${protocol}://${auth}${this.config.host}:${this.config.port}`;
    this.server = nano(url);
  }

  getConnection(): CouchDBConnection {
    return this.connection;
  }

  async connect(): Promise<void> {
    // Validate the database exists (or let it be created later). We'll just test connection.
    await this.server.info(); // will throw if unreachable
    await this.connection.connect(this.server, this.config.dbname);
  }

  setSchema(_schema: string): void {
    // CouchDB doesn't use schemas. Ignore.
  }

  async testConnection(): Promise<{ success: boolean; version?: string; error?: string }> {
    try {
      const info = await this.server.info();
      return {
        success: true,
        version: info.version
      };
    } catch (err: any) {
      return {
        success: false,
        error: err.message || 'Unknown error'
      };
    }
  }

  /**
   * Get list of databases as "tables".
   */
  async getTables(): Promise<TableInfo[]> {
    const dbList: string[] = await this.server.db.list();
    const tables: TableInfo[] = [];

    for (const dbName of dbList) {
      try {
        const db = this.server.use(dbName);
        const info = await db.info();
        tables.push({
          schemaname: '', // no schemas in CouchDB
          tablename: dbName,
          tabletype: 'doc',
          columns: [], // schema‑less – we could optionally infer from a design doc
          comment: '',
          rowCount: info.doc_count,
          size: Number(info.data_size || 0),
          originalData: info
        });
      } catch (e) {
        // Some system databases may be inaccessible; skip silently or push with minimal info
        tables.push({
          schemaname: '',
          tablename: dbName,
          tabletype: 'doc',
          columns: [],
          comment: '',
          rowCount: 0,
          size: 0,
          originalData: null
        });
      }
    }

    return tables;
  }

  async getDatabaseInfo(): Promise<{
    version: string;
    name: string;
    encoding: string;
    collation: string;
  }> {
    const info = await this.server.info();
    return {
      version: info.version,
      name: info.vendor?.name || 'CouchDB',
      encoding: '', // not applicable
      collation: '' // not applicable
    };
  }

  /**
   * Execute a Mango query.
   * The `sql` parameter should be a JSON string representing a valid Mango query.
   * If the string starts with '{', it is parsed as JSON; otherwise it is treated as a raw CouchDB API path.
   * Params are currently ignored (or could be used for query variable substitution).
   */
  async executeQuery(
    sql: string,
    params: any[],
    options?: { maxRows?: number; timeout?: number; autoDisconnect?: boolean }
  ): Promise<QueryResult> {
    const db = this.connection.getDb();
    if (!db) throw new Error('Not connected to a database. Call connect() first.');

    let query: MangoQuery;
    if (sql.trim().startsWith('{')) {
      try {
        query = JSON.parse(sql);
      } catch (e) {
        throw new Error('Invalid Mango JSON query.');
      }
    } else {
      // Fallback: treat sql as a path relative to the database, e.g., "/_all_docs?limit=10"
      // This allows basic operations without JSON.
      query = {}; // Placeholder – we'll handle raw requests differently.
      // For now, let's just support Mango JSON; raise an error for anything else.
      throw new Error('Only Mango JSON queries are supported (string starting with "{").');
    }

    if (options?.maxRows) {
      query.limit = options.maxRows;
    }

    try {
      const response: MangoResponse<any> = await db.find(query);
      return {
        success: true,
        rows: response.docs,
        fields: response.docs.length > 0 ? Object.keys(response.docs[0]) : [],
        rowCount: response.docs.length,
        executionTime: 0
      };
    } catch (error: any) {
      return {
        success: false,
        error: error.message || String(error),
        rows: [],
        fields: []
      };
    }
  }

  /**
   * Execute multiple queries sequentially (CouchDB has no transactions).
   */
  async executeTransaction(
    queries: Array<{ sql: string; params?: any[] }>
  ): Promise<QueryResult[]> {
    const results: QueryResult[] = [];
    for (const q of queries) {
      results.push(await this.executeQuery(q.sql, q.params || []));
    }
    return results;
  }
}

// --- CouchDB Adapter (Implements IBaseDatabaseInspector) ---
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

  async getTables(_connection: DatabaseConnection, options?: InspectionOptions): Promise<TableInfo[]> {
    if (!this.inspector) throw new Error('Inspector not initialized. Call connect() first.');
    // CouchDB ignores schema option
    return this.inspector.getTables();
  }

  async getTableColumns(_connection: DatabaseConnection, tables: TableInfo[]): Promise<TableInfo[]> {
    // Tables already contain column stubs; nothing extra to fetch.
    return tables;
  }

  async getDatabaseInfo(_connection: DatabaseConnection): Promise<DatabaseVersionInfo> {
    if (!this.inspector) throw new Error('Inspector not initialized. Call connect() first.');
    const info = await this.inspector.getDatabaseInfo();
    return {
      version: info.version,
      name: info.name,
      encoding: info.encoding,
      collation: info.collation
    };
  }

  async executeQuery(
    _connection: DatabaseConnection,
    sql: string,
    options?: QueryExecutionOptions
  ): Promise<QueryResult> {
    if (!this.inspector) throw new Error('Inspector not initialized.');
    const params = options?.params || [];
    const pgOptions = {
      maxRows: options?.maxRows,
      timeout: options?.timeout,
      autoDisconnect: options?.autoDisconnect
    };
    return this.inspector.executeQuery(sql, params, pgOptions);
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
    // CouchDB has no relational constraints.
    return [];
  }

  async getSchemas(_connection: DatabaseConnection): Promise<string[]> {
    // CouchDB does not have schemas; return an empty string as a dummy schema.
    return [''];
  }

  // --- Optional CouchDB-specific extensions (mirroring the PostgreSQL adapter) ---

  async getFunctions(_connection: DatabaseConnection, _schema?: string): Promise<any[]> {
    // CouchDB does not have stored procedures; return empty.
    return [];
  }

  async getIndexes(connection: DatabaseConnection, tableName?: string): Promise<any[]> {
    if (!this.inspector) throw new Error('Inspector not initialized.');
    const db = this.connectionInstance?.getDb();
    if (!db) throw new Error('Not connected to a database.');

    const targetDbName = tableName || this.connectionInstance?.getDbName();
    if (!targetDbName) return [];

    // Use the server to query the specific database's indexes.
    const targetDb = this.inspector['server'].use(targetDbName); // access server via inspector
    try {
      const indexInfo = await targetDb.index();
      return indexInfo.indexes || [];
    } catch (e) {
      return [];
    }
  }
}

export default CouchDBAdapter;