/**
 * Enhanced Cloudera Impala Schema Inspector with Query Execution
 * Comprehensive schema inspection with robust error handling and connection management
 * TypeScript implementation optimized for DatabaseMetadataWizard integration
 *
 * Impala is accessed via the HiveServer2 Thrift protocol (same as Hive),
 * so we reuse the 'hive-driver' package with the appropriate endpoint.
 * Default port: 21050 (Impala Daemon)
 */

import { HiveClient } from 'hive-driver';

// Infer the exact config type expected by HiveClient's constructor
// This resolves the missing export error for 'HiveClientConfig'
type HiveClientConfig = ConstructorParameters<typeof HiveClient>[0];

// ---------------------------------------------------------------------------
// Re‑use identical type definitions and utilities from the PostgreSQL inspector
// ---------------------------------------------------------------------------

interface DatabaseConfig {
  dbname: string;            // Impala database (schema) to connect to
  host?: string;
  port?: string;
  user?: string;
  password?: string;
  schema?: string;           // alias for dbname, defaults to 'default'
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
// Logger (identical to the original)
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
// Impala‑specific error classes
// ---------------------------------------------------------------------------
class ImpalaConnectionError extends Error {
  constructor(
    message: string,
    public code?: string,
    public originalError?: unknown
  ) {
    super(message);
    this.name = 'ImpalaConnectionError';
  }
}

class ImpalaQueryError extends Error {
  constructor(
    message: string,
    public sql: string,
    public parameters: any[] = [],
    public originalError?: unknown
  ) {
    super(message);
    this.name = 'ImpalaQueryError';
  }
}

// ---------------------------------------------------------------------------
// Impala Connection Manager – wraps HiveClient (HiveServer2)
// ---------------------------------------------------------------------------
class ImpalaConnection {
  private client: HiveClient | null = null;
  private isConnected: boolean = false;
  private readonly maxRetries: number = 3;

  constructor(private config: DatabaseConfig) {}

  /**
   * Initialises the HiveClient to talk to an Impala Daemon via HiveServer2.
   * Default port is 21050; default database is 'default'.
   */
  async connect(): Promise<void> {
    if (this.isConnected && this.client) {
      Logger.debug('connection already established');
      return;
    }

    const impalaConfig: HiveClientConfig = {
      host: this.config.host || 'localhost',
      port: this.config.port ? parseInt(this.config.port, 10) : 21050, // Impala default
      username: this.config.user,
      password: this.config.password,
      databaseName: this.config.dbname || 'default',
      maxRetries: 1,
      retryInitialDelay: 1000,
      connectTimeout: 15000,
      socketTimeout: 30000,
      // Impala supports SASL PLAIN when authentication is required
      sasl: this.config.user ? { mechanism: 'PLAIN' } : undefined,
    };

    let lastError: unknown = null;

    for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
      try {
        Logger.debug('connection attempt %d/%d', attempt, this.maxRetries);

        this.client = new HiveClient(impalaConfig);
        this.client.on('error', (err: Error) => {
          Logger.error('Unexpected HiveClient error: %s', getErrorMessage(err));
          this.isConnected = false;
        });

        await this.client.connect();
        this.isConnected = true;

        Logger.info('successfully connected to Impala at %s:%d (attempt %d)',
          impalaConfig.host, impalaConfig.port, attempt);
        return;

      } catch (error) {
        lastError = error;
        this.isConnected = false;
        if (this.client) {
          try { await this.client.close(); } catch (_) {}
          this.client = null;
        }

        Logger.warn('connection attempt %d failed: %s', attempt, getErrorMessage(error));

        if (attempt < this.maxRetries) {
          const backoffTime = Math.min(1000 * Math.pow(2, attempt - 1), 10000);
          Logger.debug('waiting %d ms before retry', backoffTime);
          await new Promise(resolve => setTimeout(resolve, backoffTime));
        }
      }
    }

    throw new ImpalaConnectionError(
      `Failed to connect to Impala after ${this.maxRetries} attempts: ${getErrorMessage(lastError)}`,
      'ECONNFAILED',
      lastError
    );
  }

  async checkHealth(): Promise<boolean> {
    if (!this.isConnected || !this.client) return false;
    try {
      const result = await this.client.query('SELECT 1 AS health_check');
      return result.rows.length > 0 && result.rows[0].health_check === 1;
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
   * Execute a query. Impala (via HiveServer2) uses positional `?` placeholders.
   */
  async query(sql: string, params?: any[]): Promise<any> {
    const safeParams = params ?? [];
    this.validateQueryParameters(sql, safeParams);

    if (!this.isConnected || !this.client) {
      throw new ImpalaConnectionError('Database not connected');
    }

    try {
      Logger.debug('executing query: %s with %d parameters',
        sql.substring(0, 100) + (sql.length > 100 ? '...' : ''),
        safeParams.length);

      const startTime = Date.now();
      const result = await this.client.query(sql, safeParams);
      const duration = Date.now() - startTime;

      Logger.debug('query completed in %d ms, %d rows returned', duration, result.rows.length);
      return result;
    } catch (error) {
      if (this.isConnectionError(error)) {
        this.isConnected = false;
        Logger.warn('connection lost during query');
      }

      const queryError = new ImpalaQueryError(
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
      throw new ImpalaQueryError('SQL query must be a non-empty string', sql, params);
    }

    const placeholderCount = (sql.match(/\?/g) || []).length;
    const safeParams = params ?? [];
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
      'transport', 'thrift',
    ];
    return connectionErrors.some(term => errorMessage.includes(term));
  }

  async disconnect(): Promise<void> {
    if (this.client) {
      try {
        Logger.debug('closing Impala connection');
        await this.client.close();
        Logger.info('Impala connection closed successfully');
      } catch (error) {
        Logger.error('error closing Impala client: %s', getErrorMessage(error));
      } finally {
        this.isConnected = false;
        this.client = null;
      }
    }
  }

  getClient(): HiveClient | null {
    return this.client;
  }

  getConfig(): Omit<DatabaseConfig, 'password'> {
    const { password, ...safeConfig } = this.config;
    return safeConfig;
  }
}

// ---------------------------------------------------------------------------
// Enhanced Cloudera Impala Schema Inspector
// ---------------------------------------------------------------------------
class ImpalaSchemaInspector {
  private connection: ImpalaConnection;
  private schema: string;

  constructor(config: DatabaseConfig) {
    const dbName = config.dbname || config.schema || 'default';
    this.connection = new ImpalaConnection({ ...config, dbname: dbName });
    this.schema = dbName;
  }

  static setLogLevel(level: 'DEBUG' | 'INFO' | 'WARN' | 'ERROR'): void {
    Logger.setLogLevel(level);
  }

  static validateConnectionConfig(config: DatabaseConfig): string[] {
    const errors: string[] = [];

    if (!config.dbname || config.dbname.trim() === '') {
      errors.push('Database name (Impala database) is required');
    }

    if (config.port) {
      const port = parseInt(config.port, 10);
      if (isNaN(port)) errors.push('Port must be a valid number');
      else if (port < 1 || port > 65535) errors.push('Port must be between 1 and 65535');
    }

    const dbName = config.dbname || config.schema || 'default';
    if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(dbName)) {
      errors.push('Database name contains invalid characters');
    }

    return errors;
  }

  /**
   * Test connection and retrieve Impala version.
   */
  async testConnection(): Promise<{ success: boolean; version?: string; error?: string }> {
    try {
      await this.connection.connect();
      // Impala provides version() just like Hive
      const result = await this.connection.query('SELECT version() AS version');
      if (result.rows && result.rows.length > 0) {
        const version = result.rows[0].version;
        Logger.info('connection test successful – Impala version: %s', version);
        return { success: true, version };
      }
      return { success: false, error: 'No version information returned' };
    } catch (error) {
      const errorMessage = getErrorMessage(error);
      Logger.error('connection test failed: %s', errorMessage);
      return { success: false, error: errorMessage };
    } finally {
      await this.connection.disconnect();
    }
  }

  /**
   * Execute arbitrary Impala SQL.
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

      Logger.info('executing arbitrary Impala SQL query');
      Logger.debug('query: %s', sql.substring(0, 200) + (sql.length > 200 ? '...' : ''));

      let finalSql = sql;
      if (maxRows && sql.trim().toUpperCase().startsWith('SELECT')) {
        finalSql = this.applyRowLimit(sql, maxRows);
      }

      const result = await this.connection.query(finalSql, params);
      const executionTime = Date.now() - startTime;

      const fields = (result.schema || []).map((col: any) => ({
        name: col.name,
        type: col.type,
      }));

      const queryResult: QueryResult = {
        success: true,
        rows: result.rows,
        rowCount: result.rows.length,
        fields,
        executionTime,
        affectedRows: result.rows.length,
        command: finalSql.trim().split(/\s+/)[0].toUpperCase(),
      };

      Logger.info('query executed successfully in %d ms, returned %d rows',
        executionTime, result.rows.length);

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

  private applyRowLimit(sql: string, maxRows: number): string {
    const trimmed = sql.trim();
    const upperSql = trimmed.toUpperCase();

    if (upperSql.startsWith('SELECT') && !upperSql.includes('LIMIT')) {
      return `${trimmed} LIMIT ${maxRows}`;
    }
    return sql;
  }

  async executeQueryAndDisconnect(sql: string, params: any[] = []): Promise<QueryResult> {
    return this.executeQuery(sql, params, { autoDisconnect: true });
  }

  /**
   * Execute a batch of queries sequentially.
   * Impala does not support multi‑statement transactions; each query is run independently.
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
          rowCount: result.rows.length,
          executionTime,
          affectedRows: result.rows.length,
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
   * Retrieves all tables in the configured Impala database.
   * Uses Impala’s `SHOW TABLES IN database` and then `DESCRIBE` for each table.
   */
  async getTables(): Promise<TableInfo[]> {
    await this.connection.connect();

    const schema = this.schema;
    // Note: SHOW TABLES is an identifier and cannot be parameterised; we trust the schema name.
    const showTablesSql = `SHOW TABLES IN \`${schema}\``;

    try {
      const tablesResult = await this.connection.query(showTablesSql);
      const tableNames: string[] = tablesResult.rows.map((row: any) => row.name);

      Logger.info('found %d tables in database "%s"', tableNames.length, schema);

      const tables: TableInfo[] = [];

      for (const tabName of tableNames) {
        const table = new TableInfo(schema, tabName);
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
   * Retrieve column metadata using `DESCRIBE table`.
   * Impala returns: name, type, comment in a fixed order.
   */
  private async getTableColumns(table: TableInfo): Promise<void> {
    const describeSql = `DESCRIBE \`${table.schemaname}\`.\`${table.tablename}\``;

    try {
      const result = await this.connection.query(describeSql);

      for (const row of result.rows) {
        const colName = (row.name || row.col_name || '').trim();
        // Impala includes partition info as additional rows; we only take real columns
        if (!colName || colName.startsWith('#') || colName === '') {
          continue;
        }

        const columnInfo: ColumnInfo = {
          name: colName,
          type: row.type || row.data_type,
          comment: row.comment || undefined,
          nullable: true,           // Impala does not enforce NOT NULL
          default: undefined,
          isIdentity: false,
        };

        this.extractTypeDetails(columnInfo, columnInfo.type);
        table.columns.push(columnInfo);
      }

      Logger.debug('retrieved %d columns for table %s', table.columns.length, table.tablename);
    } catch (error) {
      Logger.error('column retrieval failed for table %s: %s', table.tablename, getErrorMessage(error));
    }
  }

  private extractTypeDetails(column: ColumnInfo, dataType: string): void {
    const lower = dataType.toLowerCase().trim();

    if (lower.startsWith('varchar') || lower.startsWith('char') || lower.startsWith('string')) {
      const match = lower.match(/\((\d+)\)/);
      if (match) column.length = parseInt(match[1], 10);
    } else if (lower.startsWith('decimal') || lower.startsWith('numeric')) {
      const match = lower.match(/\((\d+),(\d+)\)/);
      if (match) {
        column.precision = parseInt(match[1], 10);
        column.scale = parseInt(match[2], 10);
      }
    } else if (lower.includes('timestamp')) {
      const match = lower.match(/\((\d+)\)/);
      if (match) column.precision = parseInt(match[1], 10);
    }
  }

  /**
   * Enrich table metadata with row counts and size using `SHOW TABLE STATS`.
   * This command returns one row per column + summary rows.
   */
  private async enrichTableMetadata(table: TableInfo): Promise<void> {
    try {
      const statsSql = `SHOW TABLE STATS \`${table.schemaname}\`.\`${table.tablename}\``;
      const result = await this.connection.query(statsSql);

      // The output contains rows with keys: 'Rows', 'Size', 'Format', etc.
      // We search for the summary row where the first column is empty or '# Rows'
      for (const row of result.rows) {
        const key = (row[0] || row.col_name || '').toString().trim();
        const value = (row[1] || row.data_type || '').toString().trim();

        if (key === '# Rows' || key.toLowerCase() === 'rows') {
          table.rowCount = parseInt(value, 10) || 0;
        } else if (key === '# Bytes' || key.toLowerCase() === 'size') {
          const bytes = parseInt(value, 10);
          if (!isNaN(bytes)) table.size = this.formatBytes(bytes);
        }
      }

      // Detect views: Impala views return 'VIRTUAL_VIEW' in table type via EXPLAIN or TBLPROPERTIES
      // A simple heuristic: if SHOW TABLE STATS returns no rows, it might be a view.
      if (result.rows.length === 0) {
        table.tabletype = 'view';
      }

    } catch (error) {
      Logger.warn('failed to enrich metadata for table %s: %s', table.tablename, getErrorMessage(error));
    }
  }

  private formatBytes(bytes: number): string {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }

  /**
   * Get a specific table by name.
   */
  async getTable(tableName: string): Promise<TableInfo | null> {
    const allTables = await this.getTables();
    return allTables.find(t => t.tablename === tableName) || null;
  }

  /**
   * Retrieve Impala version and current database name.
   */
  async getDatabaseInfo(): Promise<{
    version: string;
    name: string;
    encoding?: string;
    collation?: string;
  }> {
    await this.connection.connect();

    const versionResult = await this.connection.query('SELECT version() AS version');
    const version = versionResult.rows[0]?.version || 'unknown';
    const dbResult = await this.connection.query('SELECT current_database() AS name');
    const name = dbResult.rows[0]?.name || this.schema;

    return {
      version,
      name,
      encoding: undefined,
      collation: undefined,
    };
  }

  getConnection(): ImpalaConnection {
    return this.connection;
  }

  getCurrentSchema(): string {
    return this.schema;
  }

  setSchema(schema: string): void {
    this.schema = schema;
    Logger.debug('schema set to: %s', schema);
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
// Exports – exactly mirroring the PostgreSQL/Hive inspector
// ---------------------------------------------------------------------------
export {
  ImpalaSchemaInspector,
  ImpalaConnection,
  TableInfo,
  Logger,
  ImpalaConnectionError,
  ImpalaQueryError,
  getErrorMessage,
  isError,
};

export type {
  ColumnInfo,
  DatabaseConfig,
  QueryResult,
};

export default ImpalaSchemaInspector;