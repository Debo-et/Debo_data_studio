/**
 * Enhanced MarkLogic Schema Inspector with Query Execution
 * Comprehensive schema inspection (SQL‑based) with robust error handling and connection management
 * TypeScript implementation optimized for DatabaseMetadataWizard integration
 *
 * MarkLogic is accessed via the official REST API client. By default, the client
 * connects to a REST application server on port 8000. SQL queries are executed
 * using the client.sql() method. The “tables” are SQL views / tables that exist
 * in the configured MarkLogic schema (default: “public”).
 */

import { Client, ClientConfiguration } from 'marklogic';

// ---------------------------------------------------------------------------
// Re‑use identical type definitions and utilities from the PostgreSQL inspector
// ---------------------------------------------------------------------------

interface DatabaseConfig {
  dbname: string;            // MarkLogic database name (used as schema in SQL context)
  host?: string;
  port?: string;
  user?: string;
  password?: string;
  schema?: string;           // SQL schema name (default: 'public')
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
  public tabletype: string = 'table';
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
// MarkLogic‑specific error classes
// ---------------------------------------------------------------------------
class MarkLogicConnectionError extends Error {
  constructor(
    message: string,
    public code?: string,
    public originalError?: unknown
  ) {
    super(message);
    this.name = 'MarkLogicConnectionError';
  }
}

class MarkLogicQueryError extends Error {
  constructor(
    message: string,
    public sql: string,
    public parameters: any[] = [],
    public originalError?: unknown
  ) {
    super(message);
    this.name = 'MarkLogicQueryError';
  }
}

// ---------------------------------------------------------------------------
// MarkLogic Connection Manager – wraps the official Node.js client
// ---------------------------------------------------------------------------
class MarkLogicConnection {
  private client: Client | null = null;
  private isConnected: boolean = false;
  private readonly maxRetries: number = 3;

  constructor(private config: DatabaseConfig) {}

  /**
   * Initialises the MarkLogic client and verifies connectivity with a simple SQL query.
   */
  async connect(): Promise<void> {
    if (this.isConnected && this.client) {
      Logger.debug('connection already established');
      return;
    }

    const mlConfig: ClientConfiguration = {
      host: this.config.host || 'localhost',
      port: this.config.port ? parseInt(this.config.port, 10) : 8000, // default REST API port
      user: this.config.user,
      password: this.config.password,
      authType: 'DIGEST', // MarkLogic default authentication
      ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
    };

    let lastError: unknown = null;

    for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
      try {
        Logger.debug('connection attempt %d/%d', attempt, this.maxRetries);
        this.client = new Client(mlConfig);

        // Verify connectivity by executing a trivial SQL query
        await this.client.sql.query('SELECT 1 AS test');
        this.isConnected = true;

        Logger.info('successfully connected to MarkLogic at %s:%d (attempt %d)',
          mlConfig.host, mlConfig.port, attempt);
        return;

      } catch (error) {
        lastError = error;
        this.isConnected = false;
        if (this.client) {
          try { this.client = null; } catch (_) {}
        }

        Logger.warn('connection attempt %d failed: %s', attempt, getErrorMessage(error));

        if (attempt < this.maxRetries) {
          const backoffTime = Math.min(1000 * Math.pow(2, attempt - 1), 10000);
          Logger.debug('waiting %d ms before retry', backoffTime);
          await new Promise(resolve => setTimeout(resolve, backoffTime));
        }
      }
    }

    throw new MarkLogicConnectionError(
      `Failed to connect to MarkLogic after ${this.maxRetries} attempts: ${getErrorMessage(lastError)}`,
      'ECONNFAILED',
      lastError
    );
  }

  /**
   * Health check using a simple SQL query.
   */
  async checkHealth(): Promise<boolean> {
    if (!this.isConnected || !this.client) return false;
    try {
      const result = await this.client.sql.query('SELECT 1 AS healthcheck');
      return result?.rows?.length > 0 && result.rows[0].healthcheck === 1;
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
   * Execute a SQL query (MarkLogic’s SQL dialect).
   * Parameters should be provided as an array; the MarkLogic client expects an array of values.
   */
  async query(sql: string, params?: any[]): Promise<any> {
    const safeParams = params ?? [];
    this.validateQueryParameters(sql, safeParams);

    if (!this.isConnected || !this.client) {
      throw new MarkLogicConnectionError('Database not connected');
    }

    try {
      Logger.debug('executing query: %s with %d parameters',
        sql.substring(0, 100) + (sql.length > 100 ? '...' : ''),
        safeParams.length);

      const startTime = Date.now();
      const response = await this.client.sql.query(sql, safeParams);
      const duration = Date.now() - startTime;

      Logger.debug('query completed in %d ms, %d rows returned',
        duration, response.rows?.length || 0);

      return response;
    } catch (error) {
      if (this.isConnectionError(error)) {
        this.isConnected = false;
        Logger.warn('connection lost during query');
      }

      const queryError = new MarkLogicQueryError(
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
      throw new MarkLogicQueryError('SQL query must be a non-empty string', sql, params);
    }

    const safeParams = params ?? [];
    const placeholderCount = (sql.match(/\?/g) || []).length;
    if (placeholderCount !== safeParams.length) {
      Logger.warn('parameter count mismatch: %d placeholders, %d parameters',
        placeholderCount, safeParams.length);
    }
  }

  private isConnectionError(error: unknown): boolean {
    const errorMessage = getErrorMessage(error).toLowerCase();
    const connectionErrors = [
      'econnreset', 'econnrefused', 'epipe', 'etimedout',
      'connection', 'connect', 'socket', 'network', 'terminated',
      'unauthorized', 'authentication',
    ];
    return connectionErrors.some(term => errorMessage.includes(term));
  }

  async disconnect(): Promise<void> {
    // The MarkLogic client has no explicit close method; we simply release the reference.
    this.isConnected = false;
    this.client = null;
    Logger.info('MarkLogic client disconnected');
  }

  getClient(): Client | null {
    return this.client;
  }

  getConfig(): Omit<DatabaseConfig, 'password'> {
    const { password, ...safeConfig } = this.config;
    return safeConfig;
  }
}

// ---------------------------------------------------------------------------
// Enhanced MarkLogic Schema Inspector (SQL‑centric)
// ---------------------------------------------------------------------------
class MarkLogicSchemaInspector {
  private connection: MarkLogicConnection;
  private schema: string;   // SQL schema name (default: 'public')

  constructor(config: DatabaseConfig) {
    // MarkLogic uses the concept of a “SQL schema” to group tables/views.
    // We default to 'public' if not provided.
    this.schema = config.schema || config.dbname || 'public';
    this.connection = new MarkLogicConnection({
      ...config,
      // dbname is not directly used; the SQL operations target the schema.
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
   * Test connection and retrieve MarkLogic server version.
   */
  async testConnection(): Promise<{ success: boolean; version?: string; error?: string }> {
    try {
      await this.connection.connect();
      const client = this.connection.getClient();
      if (!client) throw new Error('Client not available');

      // Use the management API to get version; SQL also exposes some server info.
      // We'll use a simple SQL query to retrieve the MarkLogic version string.
      const result = await client.sql.query('SELECT xdmp.version() AS version');
      const version = result.rows?.[0]?.version || 'unknown';

      Logger.info('connection test successful – MarkLogic version: %s', version);
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
   * Execute an arbitrary SQL query (MarkLogic SQL dialect).
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

      Logger.info('executing arbitrary MarkLogic SQL query');
      Logger.debug('query: %s', sql.substring(0, 200) + (sql.length > 200 ? '...' : ''));

      let finalSql = sql;
      if (maxRows && sql.trim().toUpperCase().startsWith('SELECT')) {
        finalSql = this.applyRowLimit(sql, maxRows);
      }

      const response = await this.connection.query(finalSql, params);
      const executionTime = Date.now() - startTime;

      // MarkLogic SQL response has 'columns' and 'rows' arrays
      const fields = (response.columns || []).map((col: any) => ({
        name: col.name,
        type: col.type,
      }));

      const rows = (response.rows || []).map((row: any[]) => {
        const obj: Record<string, any> = {};
        fields.forEach((field: any, idx: number) => {
          obj[field.name] = row[idx];
        });
        return obj;
      });

      const rowCount = rows.length;

      Logger.info('query executed successfully in %d ms, returned %d rows', executionTime, rowCount);

      return {
        success: true,
        rows,
        rowCount,
        fields,
        executionTime,
        affectedRows: rowCount,
        command: finalSql.trim().split(/\s+/)[0].toUpperCase(),
      };
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

  private applyRowLimit(sql: string, maxRows: number): string {
    const trimmed = sql.trim();
    const upperSql = trimmed.toUpperCase();

    if (upperSql.startsWith('SELECT') && !upperSql.includes('LIMIT')) {
      // MarkLogic SQL uses standard LIMIT clause
      return `${trimmed} LIMIT ${maxRows}`;
    }
    return sql;
  }

  async executeQueryAndDisconnect(sql: string, params: any[] = []): Promise<QueryResult> {
    return this.executeQuery(sql, params, { autoDisconnect: true });
  }

  /**
   * Execute multiple queries sequentially (MarkLogic does not support multi‑statement transactions via SQL).
   */
  async executeTransaction(queries: Array<{ sql: string; params?: any[] }>): Promise<QueryResult[]> {
    await this.connection.connect();

    Logger.info('starting batch of %d queries', queries.length);
    const results: QueryResult[] = [];

    for (let i = 0; i < queries.length; i++) {
      const { sql, params = [] } = queries[i];
      const startTime = Date.now();
      try {
        const response = await this.connection.query(sql, params);
        const executionTime = Date.now() - startTime;

        const fields = (response.columns || []).map((col: any) => ({
          name: col.name,
          type: col.type,
        }));
        const rows = (response.rows || []).map((row: any[]) => {
          const obj: Record<string, any> = {};
          fields.forEach((field: any, idx: number) => {
            obj[field.name] = row[idx];
          });
          return obj;
        });

        results.push({
          success: true,
          rows,
          rowCount: rows.length,
          fields,
          executionTime,
          affectedRows: rows.length,
          command: sql.trim().split(/\s+/)[0].toUpperCase(),
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
   * Retrieves all tables/views in the configured MarkLogic SQL schema.
   */
  async getTables(): Promise<TableInfo[]> {
    await this.connection.connect();

    // Query the SQL standard information_schema (MarkLogic provides it)
    const tablesSql = `
      SELECT table_schema, table_name, table_type, obj_description(table_name) AS comment
      FROM information_schema.tables
      WHERE table_schema = ?
      ORDER BY table_name
    `;

    try {
      const response = await this.connection.query(tablesSql, [this.schema]);
      const rows = response.rows || [];

      Logger.info('found %d tables/views in schema "%s"', rows.length, this.schema);

      const tables: TableInfo[] = [];

      for (const row of rows) {
        // row is an array [table_schema, table_name, table_type, comment]
        const schemaname = row[0] as string;
        const tablename = row[1] as string;
        const tabletype = (row[2] as string).toLowerCase() === 'view' ? 'view' : 'table';
        const comment = row[3] as string;

        const table = new TableInfo(schemaname, tablename);
        table.tabletype = tabletype;
        table.comment = comment || undefined;

        // Retrieve columns and additional metadata
        await this.getTableColumns(table);
        await this.enrichTableMetadata(table);

        tables.push(table);
      }

      return tables;
    } catch (error) {
      Logger.error('Failed to retrieve tables: %s', getErrorMessage(error));
      throw error;
    }
  }

  /**
   * Populate columns from information_schema.columns for the given table.
   */
  private async getTableColumns(table: TableInfo): Promise<void> {
    const columnsSql = `
      SELECT column_name, data_type, is_nullable, column_default, character_maximum_length,
             numeric_precision, numeric_scale, ordinal_position
      FROM information_schema.columns
      WHERE table_schema = ? AND table_name = ?
      ORDER BY ordinal_position
    `;

    try {
      const response = await this.connection.query(columnsSql, [table.schemaname, table.tablename]);
      const rows = response.rows || [];

      for (const row of rows) {
        const colName = row[0] as string;
        const dataType = row[1] as string;
        const isNullable = (row[2] as string).toUpperCase() === 'YES';
        const defaultValue = row[3] as string | null;
        const maxLength = row[4] as number | null;
        const precision = row[5] as number | null;
        const scale = row[6] as number | null;

        const columnInfo: ColumnInfo = {
          name: colName,
          type: dataType,
          nullable: isNullable,
          default: defaultValue || undefined,
          length: maxLength || undefined,
          precision: precision || undefined,
          scale: scale || undefined,
          comment: undefined,
          isIdentity: false,
        };

        table.columns.push(columnInfo);
      }

      Logger.debug('retrieved %d columns for table %s', table.columns.length, table.tablename);
    } catch (error) {
      Logger.warn('column retrieval failed for table %s: %s', table.tablename, getErrorMessage(error));
    }
  }

  /**
   * Enrich table metadata with approximate row count (optional, can be expensive).
   * For MarkLogic we can use the cts:count-aggregate equivalent or a simple COUNT(*).
   * We'll only do it if the table is not a view (to avoid heavy computation).
   */
  private async enrichTableMetadata(table: TableInfo): Promise<void> {
    if (table.tabletype === 'view') {
      // Views may not have a meaningful row count
      return;
    }

    try {
      const countSql = `SELECT COUNT(*) AS cnt FROM "${table.schemaname}"."${table.tablename}"`;
      const response = await this.connection.query(countSql);
      if (response.rows && response.rows.length > 0) {
        table.rowCount = response.rows[0][0] as number;
      }
    } catch (error) {
      Logger.warn('failed to retrieve row count for table %s: %s', table.tablename, getErrorMessage(error));
    }
  }

  /**
   * Get a specific table by name.
   */
  async getTable(tableName: string): Promise<TableInfo | null> {
    const allTables = await this.getTables();
    return allTables.find(t => t.tablename === tableName) || null;
  }

  /**
   * Retrieve MarkLogic server version and database info via SQL.
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

    const versionRow = await client.sql.query('SELECT xdmp.version() AS version');
    const version = versionRow.rows?.[0]?.[0] || 'unknown';

    return {
      version,
      name: this.schema,      // current SQL schema
      encoding: undefined,
      collation: undefined,
    };
  }

  getConnection(): MarkLogicConnection {
    return this.connection;
  }

  getCurrentSchema(): string {
    return this.schema;
  }

  setSchema(schema: string): void {
    this.schema = schema;
    Logger.debug('SQL schema set to: %s', schema);
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
// Exports – exactly mirroring the PostgreSQL/Hive/Impala/Elasticsearch inspector
// ---------------------------------------------------------------------------
export {
  MarkLogicSchemaInspector,
  MarkLogicConnection,
  TableInfo,
  Logger,
  MarkLogicConnectionError,
  MarkLogicQueryError,
  getErrorMessage,
  isError,
};

export type {
  ColumnInfo,
  DatabaseConfig,
  QueryResult,
};

export default MarkLogicSchemaInspector;