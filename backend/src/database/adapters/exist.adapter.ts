// backend/src/database/adapters/exist.adapter.ts

import { IBaseDatabaseInspector } from '../inspection/base-inspector';
import {
  DatabaseConnection,
  QueryResult,
  TableInfo,
  DatabaseConfig,
  InspectionOptions,
  QueryExecutionOptions,
  DatabaseVersionInfo
} from '../types/inspection.types';

// Assumes the @existdb/node-exist package is installed:
// npm install @existdb/node-exist --save
import exist, { Connection as ExistClient, NodeExistOptions, ExistResult } from '@existdb/node-exist';

// -------- ExistDB Connection Wrapper --------
export class ExistConnection implements DatabaseConnection {
  private client: ExistClient | null = null;
  private config: {
    host: string;
    port: string;
    database: string;
    user: string;
    password: string;
  } | null = null;

  constructor() {}

  async connect(config: {
    host: string;
    port: string;
    database: string;
    user: string;
    password: string;
  }): Promise<void> {
    this.config = config;
    const options: NodeExistOptions = {
      host: config.host,
      port: Number(config.port),
      path: `/exist/xmlrpc`,    // XML‑RPC endpoint
      basicAuth: {
        user: config.user,
        pass: config.password,
      },
    };
    this.client = exist.connect(options);
    // Test connection: a simple query will authenticate and validate
    await this.client.resources.list('/db');
  }

  async disconnect(): Promise<void> {
    // No persistent connection to release; drop references
    this.client = null;
    this.config = null;
  }

  getClient(): ExistClient | null {
    return this.client;
  }

  getDatabase(): string {
    return this.config?.database || '';
  }
}

// -------- ExistDB Inspector --------
class ExistInspector {
  private connection: ExistConnection;
  private config: DatabaseConfig;

  constructor(config: DatabaseConfig) {
    this.config = {
      host: config.host || 'localhost',
      port: config.port || '8080',     // HTTP default
      dbname: config.dbname || 'db',   // default collection /db
      user: config.user || 'admin',
      password: config.password || '',
      schema: '',                      // not used
    };
    this.connection = new ExistConnection();
  }

  getConnection(): ExistConnection {
    return this.connection;
  }

  async connect(): Promise<void> {
    await this.connection.connect({
      host: this.config.host!,
      port: this.config.port!.toString(),
      database: this.config.dbname!,
      user: this.config.user!,
      password: this.config.password!,
    });
  }

  setSchema(_schema: string): void {
    // eXist-db uses collections as schema; ignored
  }

  async testConnection(): Promise<{ success: boolean; version?: string; error?: string }> {
    const tempConnection = new ExistConnection();
    try {
      await tempConnection.connect({
        host: this.config.host!,
        port: this.config.port!.toString(),
        database: this.config.dbname!,
        user: this.config.user!,
        password: this.config.password!,
      });
      const client = tempConnection.getClient()!;
      // Retrieve version via query
      const result = await client.query('system:get-version()', {
        output: 'json',
      });
      const version = result?.version || '';
      await tempConnection.disconnect();
      return { success: true, version };
    } catch (err: any) {
      return { success: false, error: err.message || String(err) };
    }
  }

  /**
   * "Tables" are top‑level collections inside the configured database.
   */
  async getTables(): Promise<TableInfo[]> {
    const client = this.connection.getClient();
    if (!client) throw new Error('Not connected');

    // List child collections of /db/{dbname}
    const path = `/db/${this.config.dbname}`;
    let children: string[] = [];
    try {
      children = await client.resources.listCollections(path);
    } catch (err) {
      // Fallback: return the database root as a single collection
      return [
        {
          schemaname: '',
          tablename: this.config.dbname!,
          tabletype: 'collection',
          columns: [],
          comment: '',
          rowCount: 0,
          size: 0,
          originalData: null,
        },
      ];
    }

    const tables: TableInfo[] = [];
    for (const child of children) {
      // child may be a full path like /db/{dbname}/child; extract name
      const name = child.split('/').pop() || child;
      // Attempt to get resource count and size
      let docCount = 0;
      let size = 0;
      try {
        const resources = await client.resources.list(child);
        docCount = resources.length;
        // Size computation omitted for brevity
      } catch (e) {
        // ignore
      }
      tables.push({
        schemaname: '',
        tablename: name,
        tabletype: 'collection',
        columns: [],
        comment: '',
        rowCount: docCount,
        size: size,
        originalData: { child, childPath: child },
      });
    }
    return tables;
  }

  async getTableColumns(tables: TableInfo[]): Promise<TableInfo[]> {
    // eXist-db is schema‑less; columns are returned empty.
    // However, we could optionally retrieve element names from a sample document.
    return tables;
  }

  async getDatabaseInfo(): Promise<{
    version: string;
    name: string;
    encoding: string;
    collation: string;
  }> {
    const client = this.connection.getClient();
    if (!client) throw new Error('Not connected');
    const result = await client.query('system:get-version()', { output: 'json' });
    const version = typeof result === 'string' ? result : (result as any).version || '';
    return {
      version,
      name: 'eXist-db',
      encoding: 'UTF-8',
      collation: '',
    };
  }

  /**
   * Execute an XQuery expression.
   * The `sql` parameter contains the XQuery string.
   * `params` are passed as external variables via the REST API.
   */
  async executeQuery(
    sql: string,
    params: any[] = [],
    options?: { maxRows?: number; timeout?: number; autoDisconnect?: boolean }
  ): Promise<QueryResult> {
    const client = this.connection.getClient();
    if (!client) throw new Error('Not connected');

    // Build external variables for the query
    const externalVariables: Record<string, any> = {};
    params.forEach((val, idx) => {
      externalVariables[`param${idx}`] = val;
    });

    try {
      const start = Date.now();
      const result: ExistResult = await client.query(sql, {
        variables: externalVariables,
        output: 'json', // prefer JSON output
      });

      // The result may be a JSON string or already parsed object
      let parsedResult: any;
      if (typeof result === 'string') {
        try {
          parsedResult = JSON.parse(result);
        } catch {
          // Treat as raw string
          parsedResult = { raw: result };
        }
      } else {
        parsedResult = result;
      }

      // Convert result to rows. XQuery can return any sequence; we'll wrap it in an array.
      let rows: any[];
      if (Array.isArray(parsedResult)) {
        rows = parsedResult;
      } else {
        rows = [parsedResult];
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
        executionTime: 0,
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
}

// -------- ExistDB Adapter (Implements IBaseDatabaseInspector) --------
export class ExistAdapter implements IBaseDatabaseInspector {
  private inspector: ExistInspector | null = null;
  private connectionInstance: ExistConnection | null = null;

  async connect(config: DatabaseConfig): Promise<DatabaseConnection> {
    try {
      this.inspector = new ExistInspector(config);
      await this.inspector.connect();
      this.connectionInstance = this.inspector.getConnection();
      return this.connectionInstance as unknown as DatabaseConnection;
    } catch (error) {
      throw new Error(`Failed to connect to eXist-db: ${error instanceof Error ? error.message : String(error)}`);
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
      console.error('Error disconnecting from eXist-db:', error);
    }
  }

  async testConnection(config: DatabaseConfig): Promise<{ success: boolean; version?: string; error?: string }> {
    const tempInspector = new ExistInspector(config);
    return tempInspector.testConnection();
  }

  async getTables(_connection: DatabaseConnection, options?: InspectionOptions): Promise<TableInfo[]> {
    if (!this.inspector) throw new Error('Inspector not initialized. Call connect() first.');
    return this.inspector.getTables();
  }

  async getTableColumns(_connection: DatabaseConnection, tables: TableInfo[]): Promise<TableInfo[]> {
    if (!this.inspector) throw new Error('Inspector not initialized. Call connect() first.');
    return this.inspector.getTableColumns(tables);
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
    const existOptions = {
      maxRows: options?.maxRows,
      timeout: options?.timeout,
      autoDisconnect: options?.autoDisconnect,
    };
    return this.inspector.executeQuery(sql, params, existOptions);
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
    // No relational constraints.
    return [];
  }

  async getSchemas(_connection: DatabaseConnection): Promise<string[]> {
    // eXist-db doesn't have schemas beyond collections.
    return [];
  }

  // -------- eXist-db specific extensions --------

  async getFunctions(_connection: DatabaseConnection, _schema?: string): Promise<any[]> {
    if (!this.inspector) throw new Error('Inspector not initialized.');
    // List registered XQuery functions (requires util:registered-functions())
    try {
      const result = await this.connectionInstance!
        .getClient()!
        .query('util:registered-functions()', { output: 'json' });
      return Array.isArray(result) ? result : [];
    } catch {
      return [];
    }
  }

  async getIndexes(_connection: DatabaseConnection, tableName?: string): Promise<any[]> {
    if (!this.inspector) throw new Error('Inspector not initialized.');
    // Retrieve index definitions using ft:get-indexes()
    const client = this.connectionInstance?.getClient();
    if (!client) return [];
    try {
      const result = await client.query(
        `ft:get-indexes() ! map:entry(@type, .)`,
        { output: 'json' }
      );
      return Array.isArray(result) ? result : [];
    } catch {
      return [];
    }
  }
}

export default ExistAdapter;