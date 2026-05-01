// backend/src/database/adapters/marklogic.adapter.ts

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

// Assumes the `marklogic` package is installed:
// npm install marklogic @types/marklogic --save
import marklogic, { DatabaseClient, documents, evalResult } from 'marklogic';

// -------- MarkLogic Connection Wrapper --------
export class MarkLogicConnection implements DatabaseConnection {
  private client: DatabaseClient | null = null;
  private configBrief: { host: string; port: string; database: string } | null = null;

  constructor() {}

  async connect(config: {
    host: string;
    port: string;
    user: string;
    password: string;
    database: string;
    authType?: string;
    ssl?: boolean;
  }): Promise<void> {
    this.configBrief = {
      host: config.host,
      port: config.port,
      database: config.database,
    };

    this.client = marklogic.createDatabaseClient({
      host: config.host,
      port: Number(config.port),
      user: config.user,
      password: config.password,
      database: config.database,
      authType: (config.authType as any) || 'DIGEST',
      ssl: config.ssl || false,
    });
  }

  async disconnect(): Promise<void> {
    if (this.client) {
      // The marklogic client does not have an explicit close method,
      // but we can release internally managed resources by dropping the reference.
      this.client = null;
      this.configBrief = null;
    }
  }

  getClient(): DatabaseClient | null {
    return this.client;
  }

  getConfigBrief() {
    return this.configBrief;
  }
}

// -------- MarkLogic Inspector --------
class MarkLogicInspector {
  private connection: MarkLogicConnection;
  private config: DatabaseConfig;
  private client: DatabaseClient;

  constructor(config: DatabaseConfig) {
    this.config = {
      dbname: config.dbname || config.database || 'Documents', // MarkLogic uses "database"
      host: config.host || 'localhost',
      port: config.port || '8000',               // Default REST API port
      user: config.user || 'admin',
      password: config.password || 'admin',
      schema: config.schema || '',                // Not used; collections will be used as "tables"
    };
    this.connection = new MarkLogicConnection();
    // The client will be created when connect() is called
    this.client = {} as DatabaseClient; // placeholder
  }

  getConnection(): MarkLogicConnection {
    return this.connection;
  }

  async connect(): Promise<void> {
    await this.connection.connect({
      host: this.config.host!,
      port: this.config.port!.toString(),
      user: this.config.user!,
      password: this.config.password!,
      database: this.config.dbname,
      ssl: this.config.ssl || false,
    });
    this.client = this.connection.getClient()!;
    // Test connection
    await this.client.eval('1 + 1');
  }

  setSchema(_schema: string): void {
    // MarkLogic doesn't have schemas; collections are used instead.
  }

  async testConnection(): Promise<{ success: boolean; version?: string; error?: string }> {
    try {
      const conn = new MarkLogicConnection();
      await conn.connect({
        host: this.config.host!,
        port: this.config.port!.toString(),
        user: this.config.user!,
        password: this.config.password!,
        database: this.config.dbname,
      });
      const client = conn.getClient()!;
      const result = await client.eval('xdmp.version()');
      const version = (result as any[])?.[0]?.value ?? String(result);
      await conn.disconnect();
      return { success: true, version };
    } catch (err: any) {
      return { success: false, error: err.message || String(err) };
    }
  }

  /**
   * Get tables = collections in the database.
   * We also fetch document count and size per collection using xdmp.estimate and xdmp.collectionSize.
   */
  async getTables(): Promise<TableInfo[]> {
    // Server-side JavaScript to list collections with basic stats
    const script = `
      const colls = cts.collections();
      colls.toArray().map(c => {
        const count = xdmp.estimate(cts.collectionQuery(c));
        let size = 0;
        try { size = xdmp.collectionSize(c); } catch(e) {}
        return { 
          name: c, 
          docCount: count, 
          sizeKB: size 
        };
      })
    `;
    const result = await this.client.eval(script);
    const rows: any[] = result as any[]; // array of objects

    return rows.map(row => ({
      schemaname: '',                     // no schema
      tablename: row.name,
      tabletype: 'collection',
      columns: [],                        // schema-less
      comment: '',
      rowCount: Number(row.docCount),
      size: Number(row.sizeKB) * 1024,    // convert KB to bytes
      originalData: row,
    }));
  }

  async getDatabaseInfo(): Promise<{
    version: string;
    name: string;
    encoding: string;
    collation: string;
  }> {
    const versionResp = await this.client.eval('xdmp.version()');
    const version = Array.isArray(versionResp) ? String(versionResp[0]) : String(versionResp);
    return {
      version,
      name: 'MarkLogic',
      encoding: 'UTF-8',
      collation: 'http://marklogic.com/collation/',
    };
  }

  /**
   * Execute a query.
   * The `sql` parameter is treated as server-side JavaScript (or XQuery if it starts with 'xquery;').
   * Params can be passed as an array and accessed inside the script via `external.inputs`
   * if using the `eval` method with external variables.
   */
  async executeQuery(
    sql: string,
    params: any[],
    options?: { maxRows?: number; timeout?: number; autoDisconnect?: boolean }
  ): Promise<QueryResult> {
    const client = this.connection.getClient();
    if (!client) throw new Error('Not connected to a database. Call connect() first.');

    const start = Date.now();

    try {
      // Support XQuery with 'xquery;' prefix, default to JavaScript
      const isXQuery = sql.trim().startsWith('xquery;');
      const queryString = isXQuery ? sql.slice(7).trim() : sql.trim();

      // Build eval options
      const evalOptions: any = {};
      if (params.length > 0) {
        // Map params to external variables (external.inputs for SJS, external variables for XQuery)
        // For JavaScript we assign variables like `var param0 = external.inParam0;`
        // To simplify, we'll pass them as a JSON string via a single external variable.
        // This is a pragmatic approach; production code would use a more robust binding.
        evalOptions.variables = {
          params: params,
        };
      }

      const response = await client.eval(queryString, { ...evalOptions, resultType: 'array' });

      // response is an array of result values (whatever the script returns)
      let rows: any[];
      if (Array.isArray(response)) {
        rows = response;
      } else {
        rows = [response];
      }

      if (options?.maxRows && rows.length > options.maxRows) {
        rows = rows.slice(0, options.maxRows);
      }

      return {
        success: true,
        rows,
        fields: rows.length > 0 ? Object.keys(rows[0]) : [],
        rowCount: rows.length,
        executionTime: Date.now() - start,
      };
    } catch (error: any) {
      return {
        success: false,
        error: error.message || String(error),
        rows: [],
        fields: [],
        executionTime: Date.now() - start,
      };
    }
  }

  async executeTransaction(
    queries: Array<{ sql: string; params?: any[] }>
  ): Promise<QueryResult[]> {
    // MarkLogic supports multi-statement transactions via REST API.
    // For simplicity we run statements sequentially inside a single transaction.
    // A true implementation could use client.transactions.create() and commit/rollback.
    const results: QueryResult[] = [];
    for (const q of queries) {
      results.push(await this.executeQuery(q.sql, q.params || []));
    }
    return results;
  }
}

// -------- MarkLogic Adapter (Implements IBaseDatabaseInspector) --------
export class MarkLogicAdapter implements IBaseDatabaseInspector {
  private inspector: MarkLogicInspector | null = null;
  private connectionInstance: MarkLogicConnection | null = null;

  async connect(config: DatabaseConfig): Promise<DatabaseConnection> {
    try {
      this.inspector = new MarkLogicInspector(config);
      await this.inspector.connect();
      this.connectionInstance = this.inspector.getConnection();
      return this.connectionInstance as unknown as DatabaseConnection;
    } catch (error) {
      throw new Error(
        `Failed to connect to MarkLogic: ${error instanceof Error ? error.message : String(error)}`
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
      console.error('Error disconnecting from MarkLogic:', error);
    }
  }

  async testConnection(
    config: DatabaseConfig
  ): Promise<{ success: boolean; version?: string; error?: string }> {
    const tempInspector = new MarkLogicInspector(config);
    return tempInspector.testConnection();
  }

  async getTables(_connection: DatabaseConnection, options?: InspectionOptions): Promise<TableInfo[]> {
    if (!this.inspector) throw new Error('Inspector not initialized. Call connect() first.');
    return this.inspector.getTables();
  }

  async getTableColumns(_connection: DatabaseConnection, tables: TableInfo[]): Promise<TableInfo[]> {
    // Tables already contain column stubs; nothing additional to fetch.
    return tables;
  }

  async getDatabaseInfo(_connection: DatabaseConnection): Promise<DatabaseVersionInfo> {
    if (!this.inspector) throw new Error('Inspector not initialized. Call connect() first.');
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
    const marklogicOptions = {
      maxRows: options?.maxRows,
      timeout: options?.timeout,
      autoDisconnect: options?.autoDisconnect,
    };
    return this.inspector.executeQuery(sql, params, marklogicOptions);
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
    // MarkLogic does not have relational constraints.
    return [];
  }

  async getSchemas(_connection: DatabaseConnection): Promise<string[]> {
    // No schema concept; return empty array or a default.
    return [];
  }

  // -------- Extra MarkLogic-specific methods (mirroring PostgreSQL adapter) --------

  async getFunctions(_connection: DatabaseConnection, _schema?: string): Promise<any[]> {
    // MarkLogic has stored modules; we could list them from the modules database.
    // For simplicity, return empty.
    return [];
  }

  async getIndexes(connection: DatabaseConnection, tableName?: string): Promise<any[]> {
    if (!this.inspector) throw new Error('Inspector not initialized.');
    // Retrieve range indexes, word lexicons, etc. using management API or cts:indexes
    const client = this.connectionInstance?.getClient();
    if (!client) throw new Error('Not connected.');

    const script = tableName
      ? `cts.indexes(cts.collectionQuery("${tableName}"))`
      : `cts.indexes()`;
    try {
      const result = await client.eval(script);
      return (Array.isArray(result) ? result : []) as any[];
    } catch (e) {
      return [];
    }
  }
}

export default MarkLogicAdapter;