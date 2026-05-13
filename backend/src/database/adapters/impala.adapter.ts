// backend/src/database/adapters/impala.adapter.ts

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
import { execSync } from 'child_process';

// --------------------------------------------------
// Impala CLI Options Helper
// --------------------------------------------------
interface ImpalaCliOpts {
  host: string;
  port: string;
  database?: string;
  kerberos?: string;      // Kerberos principal if needed
  ssl?: boolean;
  caCert?: string;
  queryOptions?: Record<string, string>;
}

function buildConnectionArgs(opts: ImpalaCliOpts): string[] {
  const args = [
    `--protocol=hs2`,                     // use HiveServer2 protocol
    `-i ${opts.host}:${opts.port}`,
    `--delimited`,
    `--print_header`,
    `--output_delimiter=\t`
  ];
  if (opts.database) {
    args.push(`-d ${opts.database}`);
  }
  if (opts.ssl) {
    args.push(`--ssl`);
  }
  if (opts.caCert) {
    args.push(`--ca_cert=${opts.caCert}`);
  }
  if (opts.kerberos) {
    args.push(`-k`, `--principal=${opts.kerberos}`);
  }
  // Additional query options can be passed via `--query_option`
  if (opts.queryOptions) {
    for (const [key, value] of Object.entries(opts.queryOptions)) {
      args.push(`--query_option=${key}=${value}`);
    }
  }
  return args;
}

// --------------------------------------------------
// Impala Connection Wrapper
// --------------------------------------------------
export class ImpalaConnection implements DatabaseConnection {
  private host: string;
  private port: string;
  private database: string;
  private ssl: boolean;
  private caCert?: string;
  private kerberos?: string;
  private queryOptions?: Record<string, string>;
  connected: boolean = false;  // public to match interface

  constructor(config: {
    host: string;
    port: string;
    database: string;
    ssl?: boolean;
    caCert?: string;
    kerberos?: string;
    queryOptions?: Record<string, string>;
  }) {
    this.host = config.host;
    this.port = config.port;
    this.database = config.database;
    this.ssl = config.ssl || false;
    this.caCert = config.caCert;
    this.kerberos = config.kerberos;
    this.queryOptions = config.queryOptions || {};
    this.connected = false;
  }

  async connect(): Promise<void> {
    // Validate that impala-shell is available
    try {
      execSync('impala-shell --version', { stdio: 'pipe' });
    } catch (err) {
      throw new Error('impala-shell not found in PATH. Install Cloudera Impala Shell to proceed.');
    }
    // Quick connectivity test: list databases (no output parsing)
    await this.executeCommand('SHOW DATABASES', true);
    this.connected = true;
  }

  async disconnect(): Promise<void> {
    // No persistent session to close; just mark disconnected
    this.connected = false;
  }

  isConnected(): boolean {
    return this.connected;
  }

  getCliArgs(): string[] {
    return buildConnectionArgs({
      host: this.host,
      port: this.port,
      database: this.database,
      ssl: this.ssl,
      caCert: this.caCert,
      kerberos: this.kerberos,
      queryOptions: this.queryOptions,
    });
  }

  /**
   * Execute an impala-shell command and return stdout as string.
   */
  executeCommand(query: string, ignoreError = false): string {
    const args = this.getCliArgs();
    args.push(`-q "${query.replace(/"/g, '\\"')}"`);
    const cmd = `impala-shell ${args.join(' ')}`;
    try {
      const stdout = execSync(cmd, { maxBuffer: 10 * 1024 * 1024, timeout: 30000 });
      return stdout.toString('utf8');
    } catch (error: any) {
      if (ignoreError) return '';
      throw new Error(`Impala command failed: ${error.message}\nCommand: ${cmd}`);
    }
  }

  getDatabase(): string {
    return this.database;
  }
}

// --------------------------------------------------
// Impala Inspector
// --------------------------------------------------
class ImpalaInspector {
  private connection: ImpalaConnection;
  private config: DatabaseConfig;

  constructor(config: DatabaseConfig) {
    this.config = {
      host: config.host || 'localhost',
      port: config.port || '21050',    // default HiveServer2 port
      dbname: config.dbname || 'default',
      user: config.user || '',
      password: config.password || '',
      schema: '',                      // Impala uses 'database' for schema
      ssl: config.ssl || false,
      caCert: config.caCert || '',
      kerberos: config.kerberos || '',
      queryOptions: config.queryOptions || {},
    };
    this.connection = new ImpalaConnection({
      host: this.config.host!,
      port: this.config.port!.toString(),
      database: this.config.dbname,
      ssl: this.config.ssl,
      caCert: this.config.caCert,
      kerberos: this.config.kerberos,
      queryOptions: this.config.queryOptions,
    });
  }

  getConnection(): ImpalaConnection {
    return this.connection;
  }

  async connect(): Promise<void> {
    await this.connection.connect();
  }

  setSchema(_schema: string): void {
    // In Impala, schema = database, so we could switch database, but we'll keep original behavior.
    // For now, ignore.
  }

  async testConnection(): Promise<{ success: boolean; version?: string; error?: string }> {
    try {
      // Test connection without full connection lifecycle
      const conn = new ImpalaConnection({
        host: this.config.host!,
        port: this.config.port!.toString(),
        database: this.config.dbname,
        ssl: this.config.ssl,
        caCert: this.config.caCert,
        kerberos: this.config.kerberos,
        queryOptions: this.config.queryOptions,
      });
      const output = conn.executeCommand('SELECT version()', false);
      // Parse version from first line after header
      const lines = output.trim().split('\n');
      if (lines.length > 1) {
        const version = lines[1].split('\t')[0]; // first column
        return { success: true, version };
      }
      return { success: true, version: 'unknown' };
    } catch (error: any) {
      return { success: false, error: error.message || String(error) };
    }
  }

  /**
   * Parse tab-delimited output from impala-shell.
   * Returns rows as array of objects, and fields as array of { name, type }.
   * (type is set to 'string' by default because impala-shell doesn't provide metadata)
   */
  public parseTabDelimited(stdout: string): {
    rows: any[];
    fields: { name: string; type: string }[];
  } {
    const lines = stdout.trim().split('\n');
    if (lines.length === 0) return { rows: [], fields: [] };

    const headers = lines[0].split('\t');
    const fields = headers.map(h => ({ name: h, type: 'string' }));  // type unknown
    const rows: any[] = [];
    for (let i = 1; i < lines.length; i++) {
      const values = lines[i].split('\t');
      const row: any = {};
      headers.forEach((h, idx) => {
        row[h] = values[idx] || null;
      });
      rows.push(row);
    }
    return { rows, fields };
  }

  async getTables(): Promise<TableInfo[]> {
    const query = `SHOW TABLES`;
    const stdout = this.connection.executeCommand(query);
    const { rows } = this.parseTabDelimited(stdout);
    const tables: TableInfo[] = [];
    for (const row of rows) {
      const tableName = row['name'] || row['table_name'] || '';
      // SHOW TABLES only gives the name; we'll need extra DESCRIBE to get columns later.
      tables.push({
        schemaname: '',                // Impala doesn't use schemas beyond database
        tablename: tableName,
        tabletype: 'TABLE',
        columns: [],
        comment: '',
        rowCount: 0,
        size: '0',                    // string, matching TableInfo.size type
        originalData: row,
      });
    }
    return tables;
  }

  async getTableColumns(tables: TableInfo[]): Promise<TableInfo[]> {
    const enriched: TableInfo[] = [];
    for (const t of tables) {
      const cols: ColumnMetadata[] = [];
      try {
        const stdout = this.connection.executeCommand(`DESCRIBE ${t.tablename}`);
        const { rows: descRows } = this.parseTabDelimited(stdout);
        // Typical Impala DESCRIBE columns: name, type, comment
        descRows.forEach((dr, idx) => {
          cols.push({
            name: dr.name || dr.col_name || '',
            type: dr.type || dr.data_type || '',
            dataType: dr.type || dr.data_type || '',
            nullable: dr.nullable === 'YES',
            default: dr.default || null,
            comment: dr.comment || '',
            length: undefined,
            precision: undefined,
            scale: undefined,
            ordinalPosition: idx + 1,
            // isIdentity removed – not part of ColumnMetadata
          });
        });
      } catch (e) {
        // Keep columns empty if DESCRIBE fails
      }
      enriched.push({ ...t, columns: cols });
    }
    return enriched;
  }

  async getDatabaseInfo(): Promise<{ version: string; name: string; encoding: string; collation: string }> {
    const stdout = this.connection.executeCommand('SELECT version()');
    const lines = stdout.split('\n');
    let version = 'unknown';
    if (lines.length > 1) {
      version = lines[1].split('\t')[0];
    }
    return { version, name: 'Impala', encoding: 'UTF-8', collation: '' };
  }

  async executeQuery(
    sql: string,
    params: any[] = [],
    options?: { maxRows?: number; timeout?: number; autoDisconnect?: boolean }
  ): Promise<QueryResult> {
    const start = Date.now();
    try {
      // Impala-shell doesn't natively support parameterised queries, so we inject them carefully.
      // Convert ? placeholders to literals (insecure but works for controlled usage).
      let processedSql = sql;
      if (params.length > 0) {
        let idx = 0;
        processedSql = sql.replace(/\?/g, () => {
          const val = params[idx++];
          if (val === undefined || val === null) return 'NULL';
          if (typeof val === 'number') return String(val);
          // strings: escape single quotes and wrap
          return `'${String(val).replace(/'/g, "\\'")}'`;
        });
      }

      const stdout = this.connection.executeCommand(processedSql);
      const parsed = this.parseTabDelimited(stdout);
      let rows = parsed.rows;
      if (options?.maxRows && rows.length > options.maxRows) {
        rows = rows.slice(0, options.maxRows);
      }
      return {
        success: true,
        rows,
        fields: parsed.fields,   // now correctly typed as { name: string; type: string }[]
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
    const results: QueryResult[] = [];
    for (const q of queries) {
      results.push(await this.executeQuery(q.sql, q.params || []));
    }
    return results;
  }
}

// --------------------------------------------------
// Impala Adapter (Implements IBaseDatabaseInspector)
// --------------------------------------------------
export class ImpalaAdapter implements IBaseDatabaseInspector {
  private inspector: ImpalaInspector | null = null;
  private connectionInstance: ImpalaConnection | null = null;

  async connect(config: DatabaseConfig): Promise<DatabaseConnection> {
    try {
      this.inspector = new ImpalaInspector(config);
      await this.inspector.connect();
      this.connectionInstance = this.inspector.getConnection();
      return this.connectionInstance as unknown as DatabaseConnection;
    } catch (error) {
      throw new Error(`Failed to connect to Impala: ${error instanceof Error ? error.message : String(error)}`);
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
      console.error('Error disconnecting from Impala:', error);
    }
  }

  async testConnection(config: DatabaseConfig): Promise<{ success: boolean; version?: string; error?: string }> {
    const tempInspector = new ImpalaInspector(config);
    return tempInspector.testConnection();
  }

  async getTables(_connection: DatabaseConnection, _options?: InspectionOptions): Promise<TableInfo[]> {
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
    const impalaOptions = {
      maxRows: options?.maxRows,
      timeout: options?.timeout,
      autoDisconnect: options?.autoDisconnect,
    };
    return this.inspector.executeQuery(sql, params, impalaOptions);
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
    // Impala supports SHOW CONSTRAINTS, but we can implement later.
    if (!this.inspector) throw new Error('Inspector not initialized.');
    try {
      const stdout = this.connectionInstance!.executeCommand(`SHOW CONSTRAINTS ON ${_table}`);
      const parsed = this.inspector.parseTabDelimited(stdout);
      return parsed.rows;
    } catch {
      return [];
    }
  }

  async getSchemas(_connection: DatabaseConnection): Promise<string[]> {
    // In Impala, schemas are databases. Use SHOW DATABASES.
    if (!this.inspector) throw new Error('Inspector not initialized.');
    const stdout = this.connectionInstance!.executeCommand('SHOW DATABASES');
    const parsed = this.inspector.parseTabDelimited(stdout);
    return parsed.rows.map((r: any) => r.name || r.database_name || '');
  }

  // ---------- Impala-specific extras ----------
  async getFunctions(_connection: DatabaseConnection, _schema?: string): Promise<any[]> {
    // Impala supports SHOW FUNCTIONS
    if (!this.inspector) throw new Error('Inspector not initialized.');
    const stdout = this.connectionInstance!.executeCommand('SHOW FUNCTIONS');
    const parsed = this.inspector.parseTabDelimited(stdout);
    return parsed.rows;
  }

  async getIndexes(_connection: DatabaseConnection, tableName?: string): Promise<any[]> {
    // Impala doesn't have traditional indexes; it uses partitions.
    // We'll return SHOW PARTITIONS if table provided.
    if (!this.inspector) throw new Error('Inspector not initialized.');
    if (tableName) {
      try {
        const stdout = this.connectionInstance!.executeCommand(`SHOW PARTITIONS ${tableName}`);
        const parsed = this.inspector.parseTabDelimited(stdout);
        return parsed.rows;
      } catch {
        return [];
      }
    }
    return [];
  }
}

export default ImpalaAdapter;