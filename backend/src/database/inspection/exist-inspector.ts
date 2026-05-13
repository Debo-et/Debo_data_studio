/**
 * Enhanced eXist-db Schema Inspector with Query Execution
 * Comprehensive structural inspection using eXist's REST API
 * TypeScript implementation optimized for DatabaseMetadataWizard integration
 *
 * eXist-db is a native XML database. This inspector exposes a set of
 * fixed virtual tables that describe the database structure:
 *   - collections     : all accessible collections
 *   - documents       : documents with metadata
 *   - indexes         : configured indexes
 *   - configuration   : database configuration details
 *
 * Arbitrary XQuery queries can be executed via the executeQuery() method.
 * XQuery parameters can be passed as an array and are bound as external variables.
 */

import axios, { AxiosInstance, AxiosRequestConfig, AxiosError } from 'axios';

// ---------------------------------------------------------------------------
// Re‑use identical type definitions and utilities from the PostgreSQL inspector
// ---------------------------------------------------------------------------

interface DatabaseConfig {
  dbname?: string;            // Optional: root collection path (default '/db')
  host?: string;
  port?: string;
  user?: string;
  password?: string;
  schema?: string;            // alias for dbname
}

interface ColumnInfo {
  name: string;
  type: string;
  nullable?: boolean;
  default?: string;
  comment?: string;
  isIdentity?: boolean;
  length?: number;
  precision?: number;
  scale?: number;
}

class TableInfo {
  public tabletype: string = 'collection';
  public comment?: string;
  public rowCount?: number;
  public size?: string;

  constructor(
    public schemaname: string,
    public tablename: string,
    public columns: ColumnInfo[] = [],
    public next: TableInfo | null = null
  ) {}

  get num_columns(): number {
    return this.columns.length;
  }

  get column_names(): string[] {
    return this.columns.map(col => col.name);
  }

  get column_types(): string[] {
    return this.columns.map(col => col.type);
  }

  getColumn(name: string): ColumnInfo | undefined {
    return this.columns.find(col => col.name === name);
  }

  hasColumn(name: string): boolean {
    return this.columns.some(col => col.name === name);
  }
}

interface QueryResult {
  success: boolean;
  rows?: any[];
  rowCount?: number;
  fields?: Array<{
    name: string;
    type: string;
  }>;
  executionTime?: number;
  error?: string;
  affectedRows?: number;
  command?: string;
}

function getErrorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  if (typeof error === 'string') return error;
  return 'Unknown error occurred';
}

function isError(error: unknown): error is Error {
  return error instanceof Error;
}

// ---------------------------------------------------------------------------
// Logger (identical implementation)
// ---------------------------------------------------------------------------
class Logger {
  static logLevel: 'DEBUG' | 'INFO' | 'WARN' | 'ERROR' = 'INFO';

  static setLogLevel(level: 'DEBUG' | 'INFO' | 'WARN' | 'ERROR'): void {
    this.logLevel = level;
  }

  static debug(fmt: string, ...args: any[]): void {
    if (this.logLevel === 'DEBUG') {
      const message = this.formatMessage(fmt, args);
      console.debug(`[DEBUG] ${message}`);
    }
  }

  static info(fmt: string, ...args: any[]): void {
    if (['DEBUG', 'INFO'].includes(this.logLevel)) {
      const message = this.formatMessage(fmt, args);
      console.log(`[INFO] ${message}`);
    }
  }

  static warn(fmt: string, ...args: any[]): void {
    if (['DEBUG', 'INFO', 'WARN'].includes(this.logLevel)) {
      const message = this.formatMessage(fmt, args);
      console.warn(`[WARN] ${message}`);
    }
  }

  static error(fmt: string, ...args: any[]): void {
    const message = this.formatMessage(fmt, args);
    console.error(`[ERROR] ${message}`);
  }

  static fatal(fmt: string, ...args: any[]): never {
    const message = this.formatMessage(fmt, args);
    console.error(`[FATAL] ${message}`);
    throw new Error(message);
  }

  private static formatMessage(fmt: string, args: any[]): string {
    return fmt.replace(/%(\w)/g, (_, specifier) => {
      if (args.length === 0) return `%${specifier}`;
      const arg = args.shift();
      return String(arg);
    });
  }
}

// ---------------------------------------------------------------------------
// eXist‑specific error classes
// ---------------------------------------------------------------------------
class ExistConnectionError extends Error {
  constructor(
    message: string,
    public code?: string,
    public originalError?: unknown
  ) {
    super(message);
    this.name = 'ExistConnectionError';
  }
}

class ExistQueryError extends Error {
  constructor(
    message: string,
    public sql: string,
    public parameters: any[] = [],
    public originalError?: unknown
  ) {
    super(message);
    this.name = 'ExistQueryError';
  }
}

// ---------------------------------------------------------------------------
// eXist-db Connection Manager – wraps Axios HTTP client
// ---------------------------------------------------------------------------
class ExistConnection {
  private http: AxiosInstance | null = null;
  private isConnected: boolean = false;
  private readonly maxRetries: number = 3;
  private baseUrl: string = '';

  constructor(private config: DatabaseConfig) {}

  /**
   * Initialises the Axios client and tests the connection by retrieving the server version.
   */
  async connect(): Promise<void> {
    if (this.isConnected && this.http) {
      Logger.debug('connection already established');
      return;
    }

    const host = this.config.host || 'localhost';
    const port = this.config.port || '8080';
    this.baseUrl = `http://${host}:${port}/exist/restxq`;

    const axiosConfig: AxiosRequestConfig = {
      baseURL: this.baseUrl,
      auth: this.config.user && this.config.password
        ? { username: this.config.user, password: this.config.password }
        : undefined,
      timeout: 30000,
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
      },
    };

    let lastError: unknown = null;

    for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
      try {
        Logger.debug('connection attempt %d/%d', attempt, this.maxRetries);
        this.http = axios.create(axiosConfig);

        // Test with a simple XQuery: retrieve version number
        const response = await this.http.get('/exist/restxq/version');
        if (response.status === 200) {
          this.isConnected = true;
          Logger.info('successfully connected to eXist-db at %s:%s (attempt %d)', host, port, attempt);
          return;
        } else {
          throw new Error(`Server responded with status ${response.status}`);
        }
      } catch (error) {
        lastError = error;
        this.isConnected = false;
        this.http = null;

        Logger.warn('connection attempt %d failed: %s', attempt, getErrorMessage(error));

        if (attempt < this.maxRetries) {
          const backoffTime = Math.min(1000 * Math.pow(2, attempt - 1), 10000);
          Logger.debug('waiting %d ms before retry', backoffTime);
          await new Promise(resolve => setTimeout(resolve, backoffTime));
        }
      }
    }

    throw new ExistConnectionError(
      `Failed to connect to eXist-db after ${this.maxRetries} attempts: ${getErrorMessage(lastError)}`,
      'ECONNFAILED',
      lastError
    );
  }

  /**
   * Health check using a simple XQuery.
   */
  async checkHealth(): Promise<boolean> {
    if (!this.isConnected || !this.http) return false;
    try {
      const response = await this.http.get('/exist/restxq/version');
      return response.status === 200;
    } catch (error) {
      this.isConnected = false;
      Logger.error('connection health check failed: %s', getErrorMessage(error));
      return false;
    }
  }

  get connectionStatus(): { connected: boolean; healthy?: boolean } {
    return {
      connected: this.isConnected,
      healthy: this.isConnected ? undefined : false,
    };
  }

  /**
   * Execute an XQuery statement.
   * Parameters are bound as external variables using the REST API.
   * The XQuery string may contain `$p0`, `$p1`, … placeholders that are
   * replaced by the values in the `params` array.
   */
  async query(sql: string, params?: any[]): Promise<any> {
    const safeParams = params ?? [];
    this.validateQueryParameters(sql, safeParams);

    if (!this.isConnected || !this.http) {
      throw new ExistConnectionError('Database not connected');
    }

    try {
      Logger.debug('executing XQuery: %s with %d parameters',
        sql.substring(0, 100) + (sql.length > 100 ? '...' : ''),
        safeParams.length);

      // Build the URL for REST XQuery execution
      const queryUrl = '/exist/restxq/execute';

      // Pass external variables as JSON in the request body
      const body: Record<string, any> = {
        query: sql,
        variables: {},
      };

      safeParams.forEach((val, idx) => {
        body.variables[`p${idx}`] = val;
      });

      const startTime = Date.now();
      const response = await this.http.post(queryUrl, body);
      const duration = Date.now() - startTime;

      const data = response.data;
      // The result shape depends on the XQuery, but we expect a JSON object with 'columns' and 'rows'
      // or a simple array of objects.
      let rows: any[] = [];
      let fields: Array<{ name: string; type: string }> = [];

      if (Array.isArray(data)) {
        rows = data;
        if (rows.length > 0) {
          fields = Object.keys(rows[0]).map(key => ({ name: key, type: 'string' }));
        }
      } else if (data?.rows && data?.columns) {
        rows = data.rows;
        fields = data.columns.map((col: any) => ({
          name: col.name || col,
          type: col.type || 'string',
        }));
      }

      Logger.debug('query completed in %d ms, %d rows returned', duration, rows.length);

      return {
        rows,
        rowCount: rows.length,
        fields,
        executionTime: duration,
      };
    } catch (error) {
      if (this.isConnectionError(error)) {
        this.isConnected = false;
        Logger.warn('connection lost during query');
      }

      const queryError = new ExistQueryError(
        `Query execution failed: ${getErrorMessage(error)}`,
        sql,
        safeParams,
        error
      );

      Logger.error('query failed: %s', getErrorMessage(error));
      Logger.debug('failed query: %s', sql);
      Logger.debug('parameters: %o', safeParams);

      throw queryError;
    }
  }

  private validateQueryParameters(sql: string, params?: any[]): void {
    if (!sql || typeof sql !== 'string') {
      throw new ExistQueryError('XQuery must be a non-empty string', sql, params);
    }
  }

  private isConnectionError(error: unknown): boolean {
    if (axios.isAxiosError(error)) {
      const axiosError = error as AxiosError;
      if (!axiosError.response) {
        // No response – network error
        return true;
      }
      const status = axiosError.response.status;
      return status === 0 || status === 502 || status === 503;
    }
    const errorMessage = getErrorMessage(error).toLowerCase();
    return ['econnreset', 'econnrefused', 'socket', 'network', 'timeout'].some(term =>
      errorMessage.includes(term)
    );
  }

  async disconnect(): Promise<void> {
    this.http = null;
    this.isConnected = false;
    Logger.info('eXist-db connection closed');
  }

  getClient(): AxiosInstance | null {
    return this.http;
  }

  getConfig(): Omit<DatabaseConfig, 'password'> {
    const { password, ...safeConfig } = this.config;
    return safeConfig;
  }
}

// ---------------------------------------------------------------------------
// Enhanced eXist-db Schema Inspector
// ---------------------------------------------------------------------------
class ExistSchemaInspector {
  private connection: ExistConnection;
  private rootCollection: string;

  constructor(config: DatabaseConfig) {
    this.rootCollection = config.dbname || config.schema || '/db';
    this.connection = new ExistConnection({
      ...config,
      dbname: this.rootCollection,
    });
  }

  static setLogLevel(level: 'DEBUG' | 'INFO' | 'WARN' | 'ERROR'): void {
    Logger.setLogLevel(level);
  }

  static validateConnectionConfig(config: DatabaseConfig): string[] {
    const errors: string[] = [];
    if (config.port) {
      const port = parseInt(config.port, 10);
      if (isNaN(port)) errors.push('Port must be a valid number');
      else if (port < 1 || port > 65535) errors.push('Port must be between 1 and 65535');
    }
    return errors;
  }

  /**
   * Test connection and retrieve eXist version.
   */
  async testConnection(): Promise<{ success: boolean; version?: string; error?: string }> {
    try {
      await this.connection.connect();
      const client = this.connection.getClient();
      if (!client) throw new Error('Client not available');

      const response = await client.get('/exist/restxq/version');
      // typical response: { exist: { version: '5.3.1', build: '20220315' } }
      const version = response.data?.exist?.version || response.data?.version || JSON.stringify(response.data);
      Logger.info('connection test successful – eXist version: %s', version);
      return { success: true, version };
    } catch (error) {
      const errorMessage = getErrorMessage(error);
      Logger.error('connection test failed: %s', errorMessage);
      return { success: false, error: errorMessage };
    } finally {
      await this.connection.disconnect();
    }
  }

  /**
   * Execute an arbitrary XQuery query.
   * Parameters are passed as an array and bound to `$p0`, `$p1`, …
   */
  async executeQuery(
    sql: string,
    params: any[] = [],
    options: {
      maxRows?: number;
      timeout?: number;
      autoDisconnect?: boolean;
    } = {}
  ): Promise<QueryResult> {
    const startTime = Date.now();
    const { maxRows, autoDisconnect = false } = options;

    try {
      await this.connection.connect();

      Logger.info('executing arbitrary XQuery');
      Logger.debug('query: %s', sql.substring(0, 200) + (sql.length > 200 ? '...' : ''));

      let finalSql = sql;
      // XQuery does not use LIMIT, but we can wrap the result in a subsequence if needed.
      // For simplicity, we just note the limitation.
      if (maxRows && !sql.toUpperCase().includes('LIMIT')) {
        // No direct LIMIT in XQuery; user should handle via XQuery sequences
        Logger.warn('maxRows not enforced for XQuery – adjust your query accordingly');
      }

      const result = await this.connection.query(finalSql, params);
      const executionTime = Date.now() - startTime;

      const queryResult: QueryResult = {
        success: true,
        rows: result.rows,
        rowCount: result.rowCount,
        fields: result.fields,
        executionTime,
        affectedRows: result.rowCount,
        command: 'XQUERY',
      };

      Logger.info('query executed successfully in %d ms, returned %d rows',
        executionTime, result.rowCount);

      return queryResult;
    } catch (error) {
      const executionTime = Date.now() - startTime;
      const errorMessage = getErrorMessage(error);

      Logger.error('query execution failed in %d ms: %s', executionTime, errorMessage);

      return {
        success: false,
        executionTime,
        error: errorMessage,
        rows: [],
        rowCount: 0,
      };
    } finally {
      if (autoDisconnect) {
        await this.connection.disconnect();
      }
    }
  }

  async executeQueryAndDisconnect(sql: string, params: any[] = []): Promise<QueryResult> {
    return this.executeQuery(sql, params, { autoDisconnect: true });
  }

  /**
   * Execute a batch of XQuery statements sequentially.
   */
  async executeTransaction(queries: Array<{ sql: string; params?: any[] }>): Promise<QueryResult[]> {
    await this.connection.connect();

    Logger.info('starting batch of %d queries', queries.length);
    const results: QueryResult[] = [];

    for (let i = 0; i < queries.length; i++) {
      const { sql, params = [] } = queries[i];
      const startTime = Date.now();
      try {
        const result = await this.connection.query(sql, params);
        const executionTime = Date.now() - startTime;
        results.push({
          success: true,
          rows: result.rows,
          rowCount: result.rowCount,
          fields: result.fields,
          executionTime,
          affectedRows: result.rowCount,
          command: 'XQUERY',
        });
      } catch (error) {
        Logger.error('batch failed at query %d: %s', i + 1, getErrorMessage(error));
        throw error;
      }
    }

    Logger.info('batch completed successfully');
    return results;
  }

  /**
   * Retrieves the fixed set of virtual tables that describe the database.
   */
  async getTables(): Promise<TableInfo[]> {
    await this.connection.connect();
    const tables: TableInfo[] = [];

    // --- Collections table ---
    const collectionsTable = new TableInfo(this.rootCollection, 'collections');
    collectionsTable.tabletype = 'system';
    collectionsTable.comment = 'All collections in the database';
    collectionsTable.columns = [
      { name: 'path', type: 'string' },
      { name: 'name', type: 'string' },
      { name: 'created', type: 'dateTime' },
      { name: 'owner', type: 'string' },
      { name: 'group', type: 'string' },
      { name: 'permissions', type: 'string' },
    ];
    tables.push(collectionsTable);

    // --- Documents table ---
    const docsTable = new TableInfo(this.rootCollection, 'documents');
    docsTable.tabletype = 'system';
    docsTable.comment = 'Documents in the database with metadata';
    docsTable.columns = [
      { name: 'path', type: 'string' },
      { name: 'name', type: 'string' },
      { name: 'mime-type', type: 'string' },
      { name: 'size', type: 'integer' },
      { name: 'created', type: 'dateTime' },
      { name: 'last-modified', type: 'dateTime' },
      { name: 'owner', type: 'string' },
      { name: 'group', type: 'string' },
      { name: 'permissions', type: 'string' },
    ];
    tables.push(docsTable);

    // --- Indexes table ---
    const indexesTable = new TableInfo(this.rootCollection, 'indexes');
    indexesTable.tabletype = 'system';
    indexesTable.comment = 'Configured indexes';
    indexesTable.columns = [
      { name: 'index-type', type: 'string' },
      { name: 'target', type: 'string' },
      { name: 'parameter', type: 'string' },
    ];
    tables.push(indexesTable);

    // --- Configuration table ---
    const configTable = new TableInfo(this.rootCollection, 'configuration');
    configTable.tabletype = 'system';
    configTable.comment = 'Database configuration parameters';
    configTable.columns = [
      { name: 'parameter', type: 'string' },
      { name: 'value', type: 'string' },
    ];
    tables.push(configTable);

    Logger.info('returning %d virtual tables for eXist-db', tables.length);
    return tables;
  }

  /**
   * Get a specific virtual table by name.
   */
  async getTable(tableName: string): Promise<TableInfo | null> {
    const tables = await this.getTables();
    return tables.find(t => t.tablename === tableName) || null;
  }

  /**
   * Retrieve eXist-db version and root collection info.
   */
  async getDatabaseInfo(): Promise<{
    version: string;
    name: string;
    encoding?: string;
    collation?: string;
  }> {
    await this.connection.connect();
    const client = this.connection.getClient();
    if (!client) throw new Error('Not connected');

    const response = await client.get('/exist/restxq/version');
    const version = response.data?.exist?.version || 'unknown';
    return {
      version,
      name: this.rootCollection,
      encoding: undefined,
      collation: undefined,
    };
  }

  getConnection(): ExistConnection {
    return this.connection;
  }

  getCurrentSchema(): string {
    return this.rootCollection;
  }

  setSchema(collection: string): void {
    this.rootCollection = collection;
    Logger.debug('root collection set to: %s', collection);
  }

  static flattenTableList(tables: TableInfo | TableInfo[]): TableInfo[] {
    if (Array.isArray(tables)) return tables;
    const result: TableInfo[] = [];
    let current: TableInfo | null = tables;
    while (current) {
      result.push(current);
      current = current.next;
    }
    return result;
  }

  static toStandardizedFormat(tables: TableInfo[]): any[] {
    return tables.map(table => ({
      schemaname: table.schemaname,
      tablename: table.tablename,
      tabletype: table.tabletype,
      columns: table.columns.map(col => ({
        name: col.name,
        type: col.type,
        nullable: col.nullable,
        default: col.default,
        comment: col.comment,
        length: col.length,
        precision: col.precision,
        scale: col.scale,
      })),
      comment: table.comment,
      rowCount: table.rowCount,
      size: table.size,
      originalData: table,
    }));
  }
}

// ---------------------------------------------------------------------------
// Exports – exactly mirroring the other inspectors
// ---------------------------------------------------------------------------
export {
  ExistSchemaInspector,
  ExistConnection,
  TableInfo,
  Logger,
  ExistConnectionError,
  ExistQueryError,
  getErrorMessage,
  isError,
};

export type {
  ColumnInfo,
  DatabaseConfig,
  QueryResult,
};

export default ExistSchemaInspector;