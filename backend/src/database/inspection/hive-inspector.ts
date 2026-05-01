/**
 * Enhanced Apache Hive Schema Inspector with Query Execution
 * Comprehensive schema inspection with robust error handling and connection management
 * TypeScript implementation optimized for DatabaseMetadataWizard integration
 */

import { HiveClient, HiveClientConfig, QueryResult as HiveQueryResult } from 'hive-driver';

// ---------------------------------------------------------------------------
// Re‑use the same interfaces and utilities where possible
// ---------------------------------------------------------------------------

// Database connection configuration (adapted for Hive)
interface DatabaseConfig {
  dbname: string;            // Hive database to connect to (schema)
  host?: string;
  port?: string;
  user?: string;
  password?: string;
  schema?: string;           // alias for dbname, defaults to 'default'
}

// Enhanced Column information structure (identical to PostgreSQL version)
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

// Enhanced Table information structure (identical)
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

// Query execution result structure (identical)
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

// ---------------------------------------------------------------------------
// Utility functions (copied from PostgreSQL inspector)
// ---------------------------------------------------------------------------
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
// Hive-specific error classes
// ---------------------------------------------------------------------------
class HiveConnectionError extends Error {
  constructor(
    message: string,
    public code?: string,
    public originalError?: unknown
  ) {
    super(message);
    this.name = 'HiveConnectionError';
  }
}

class HiveQueryError extends Error {
  constructor(
    message: string,
    public sql: string,
    public parameters: any[] = [],
    public originalError?: unknown
  ) {
    super(message);
    this.name = 'HiveQueryError';
  }
}

// ---------------------------------------------------------------------------
// Hive Connection Manager (emulates a pool with a single Thrift connection)
// ---------------------------------------------------------------------------
class HiveConnection {
  private client: HiveClient | null = null;
  private isConnected: boolean = false;
  private readonly maxRetries: number = 3;

  constructor(private config: DatabaseConfig) {}

  /**
   * Initialises the underlying HiveClient (single connection, not a pool).
   */
  async connect(): Promise<void> {
    if (this.isConnected && this.client) {
      Logger.debug('connection already established');
      return;
    }

    const hiveConfig: HiveClientConfig = {
      host: this.config.host || 'localhost',
      port: this.config.port ? parseInt(this.config.port, 10) : 10000,
      username: this.config.user,
      password: this.config.password,
      // Use the 'dbname' as the Hive database; fall back to 'default'
      databaseName: this.config.dbname || 'default',
      // Additional HiveClient options
      maxRetries: 1,
      retryInitialDelay: 1000,
      connectTimeout: 15000,
      socketTimeout: 30000,
      // SASL authentication (only PLAIN is widely supported)
      sasl: this.config.user ? { mechanism: 'PLAIN' } : undefined,
    };

    let lastError: unknown = null;

    for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
      try {
        Logger.debug('connection attempt %d/%d', attempt, this.maxRetries);

        this.client = new HiveClient(hiveConfig);

        // Attach error handler for unrecoverable transport errors
        this.client.on('error', (err: Error) => {
          Logger.error('Unexpected HiveClient error: %s', getErrorMessage(err));
          this.isConnected = false;
        });

        // Open the Thrift connection
        await this.client.connect();

        this.isConnected = true;
        Logger.info('successfully connected to Hive database "%s" (attempt %d)',
          hiveConfig.databaseName, attempt);
        return;

      } catch (error) {
        lastError = error;
        this.isConnected = false;

        // Clean up failed client
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

    throw new HiveConnectionError(
      `Failed to connect to Hive database "${this.config.dbname}" after ${this.maxRetries} attempts: ${getErrorMessage(lastError)}`,
      'ECONNFAILED',
      lastError
    );
  }

  /**
   * Health check – issues a simple query.
   */
  async checkHealth(): Promise<boolean> {
    if (!this.isConnected || !this.client) {
      return false;
    }

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
   * Execute a query. Parameters are passed as an array of values; they
   * are substituted using HiveClient’s built‑in parameterised queries.
   */
  async query(sql: string, params?: any[]): Promise<any> {
    const safeParams = params ?? [];
    this.validateQueryParameters(sql, safeParams);

    if (!this.isConnected || !this.client) {
      throw new HiveConnectionError('Database not connected');
    }

    try {
      Logger.debug('executing query: %s with %d parameters',
        sql.substring(0, 100) + (sql.length > 100 ? '...' : ''),
        safeParams.length);

      const startTime = Date.now();
      const result: HiveQueryResult = await this.client.query(sql, safeParams);
      const duration = Date.now() - startTime;

      Logger.debug('query completed in %d ms, %d rows returned',
        duration, result.rows.length);

      return result;
    } catch (error) {
      // If the error is connection‑related, mark as disconnected
      if (this.isConnectionError(error)) {
        this.isConnected = false;
        Logger.warn('connection lost during query');
      }

      const queryError = new HiveQueryError(
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
      throw new HiveQueryError('SQL query must be a non-empty string', sql, params);
    }

    // HiveClient uses positional `?` placeholders (not $1). Check count.
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
    return connectionErrors.some(term =>
      errorMessage.includes(term)
    );
  }

  /**
   * Disconnect the underlying client.
   */
  async disconnect(): Promise<void> {
    if (this.client) {
      try {
        Logger.debug('closing Hive connection');
        await this.client.close();
        Logger.info('Hive connection closed successfully');
      } catch (error) {
        Logger.error('error closing Hive client: %s', getErrorMessage(error));
      } finally {
        this.isConnected = false;
        this.client = null;
      }
    }
  }

  /**
   * Returns the underlying HiveClient (for advanced operations).
   */
  getClient(): HiveClient | null {
    return this.client;
  }

  /**
   * Return connection configuration without password.
   */
  getConfig(): Omit<DatabaseConfig, 'password'> {
    const { password, ...safeConfig } = this.config;
    return safeConfig;
  }
}

// ---------------------------------------------------------------------------
// Enhanced Apache Hive Schema Inspector
// ---------------------------------------------------------------------------
class HiveSchemaInspector {
  private connection: HiveConnection;
  private schema: string;   // This is the Hive database name

  constructor(config: DatabaseConfig) {
    // Default to 'default' if no dbname or schema given
    const dbName = config.dbname || config.schema || 'default';
    this.connection = new HiveConnection({ ...config, dbname: dbName });
    this.schema = dbName;
  }

  static setLogLevel(level: 'DEBUG' | 'INFO' | 'WARN' | 'ERROR'): void {
    Logger.setLogLevel(level);
  }

  /**
   * Validate connection parameters – basic sanity checks.
   */
  static validateConnectionConfig(config: DatabaseConfig): string[] {
    const errors: string[] = [];

    if (!config.dbname || config.dbname.trim() === '') {
      errors.push('Database name (Hive database) is required');
    }

    if (config.port) {
      const port = parseInt(config.port, 10);
      if (isNaN(port)) errors.push('Port must be a valid number');
      else if (port < 1 || port > 65535) errors.push('Port must be between 1 and 65535');
    }

    // Hive database names follow typical identifier rules
    const dbName = config.dbname || config.schema || 'default';
    if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(dbName)) {
      errors.push('Database name contains invalid characters');
    }

    return errors;
  }

  /**
   * Test connection and retrieve Hive version.
   */
  async testConnection(): Promise<{ success: boolean; version?: string; error?: string }> {
    try {
      await this.connection.connect();

      // Hive version query
      const result = await this.connection.query('SELECT version() AS version');

      if (result.rows && result.rows.length > 0) {
        const version = result.rows[0].version;
        Logger.info('connection test successful – Hive version: %s', version);
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
   * Execute an arbitrary SQL query (HiveQL).
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

      Logger.info('executing arbitrary HiveQL query');
      Logger.debug('query: %s', sql.substring(0, 200) + (sql.length > 200 ? '...' : ''));

      let finalSql = sql;
      if (maxRows && sql.trim().toUpperCase().startsWith('SELECT')) {
        finalSql = this.applyRowLimit(sql, maxRows);
      }

      const result = await this.connection.query(finalSql, params);
      const executionTime = Date.now() - startTime;

      // HiveClient returns result.schema as an array of { name, type }
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
        affectedRows: result.rows.length,   // Hive generally returns all rows
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

  /**
   * Apply a LIMIT clause if not already present.
   */
  private applyRowLimit(sql: string, maxRows: number): string {
    const trimmed = sql.trim();
    const upperSql = trimmed.toUpperCase();

    if (upperSql.startsWith('SELECT') && !upperSql.includes('LIMIT')) {
      return `${trimmed} LIMIT ${maxRows}`;
    }
    return sql;
  }

  /**
   * Convenience method: executeQueryAndDisconnect
   */
  async executeQueryAndDisconnect(sql: string, params: any[] = []): Promise<QueryResult> {
    return this.executeQuery(sql, params, { autoDisconnect: true });
  }

  /**
   * Execute multiple queries in a transaction.
   * Note: Hive does not support full ACID transactions in all setups,
   * but this method attempts to run the queries sequentially.
   * If any query fails, subsequent queries are not executed.
   */
  async executeTransaction(queries: Array<{ sql: string; params?: any[] }>): Promise<QueryResult[]> {
    await this.connection.connect();

    Logger.info('starting transaction with %d queries', queries.length);
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
        Logger.error('transaction failed at query %d: %s', i + 1, getErrorMessage(error));
        throw error;
      }
    }

    Logger.info('transaction completed successfully');
    return results;
  }

  /**
   * Retrieves all tables in the configured Hive database.
   * Uses `SHOW TABLES` and then `DESCRIBE FORMATTED` for each table.
   * In Hive, the "schema" is the database; table names are returned.
   */
  async getTables(): Promise<TableInfo[]> {
    await this.connection.connect();

    // Hive does not let you parameterise identifiers in SHOW TABLES,
    // so we construct the query carefully (only after validating schema name).
    const schema = this.schema;
    const showTablesSql = `SHOW TABLES IN \`${schema}\``;

    try {
      const tablesResult = await this.connection.query(showTablesSql);
      const tableNames: string[] = tablesResult.rows.map((row: any) => row.tab_name);

      Logger.info('found %d tables in database "%s"', tableNames.length, schema);

      const tables: TableInfo[] = [];

      for (const tabName of tableNames) {
        const table = new TableInfo(schema, tabName);
        // Retrieve column metadata via DESCRIBE
        await this.getTableColumns(table);
        // Optionally enrich with row counts and size estimates (Hive can provide statistics)
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
   * Get column details using `DESCRIBE table`.
   * Hive returns: col_name, data_type, comment (and sometimes extra columns).
   */
  private async getTableColumns(table: TableInfo): Promise<void> {
    const describeSql = `DESCRIBE \`${table.schemaname}\`.\`${table.tablename}\``;

    try {
      const result = await this.connection.query(describeSql);

      for (const row of result.rows) {
        // Hive DESCRIBE can also contain partition info (lines starting with '#')
        // and blank lines; we skip those.
        const colName = row.col_name?.trim();
        if (!colName || colName.startsWith('#') || colName === '') {
          continue;
        }

        // Hive DESCRIBE columns: col_name, data_type, comment
        const columnInfo: ColumnInfo = {
          name: colName,
          type: row.data_type,
          comment: row.comment || undefined,
          nullable: true,         // Hive does not enforce NOT NULL by default
          default: undefined,
          isIdentity: false,
        };

        this.extractTypeDetails(columnInfo, row.data_type);
        table.columns.push(columnInfo);
      }

      Logger.debug('retrieved %d columns for table %s',
        table.columns.length, table.tablename);
    } catch (error) {
      Logger.error('column retrieval failed for table %s: %s',
        table.tablename, getErrorMessage(error));
    }
  }

  /**
   * Parse type string like varchar(100), decimal(10,2), etc.
   */
  private extractTypeDetails(column: ColumnInfo, dataType: string): void {
    const lower = dataType.toLowerCase().trim();

    if (lower.startsWith('varchar') || lower.startsWith('char') || lower.startsWith('string')) {
      const match = lower.match(/\((\d+)\)/);
      if (match) {
        column.length = parseInt(match[1], 10);
      }
    } else if (lower.startsWith('decimal') || lower.startsWith('numeric')) {
      const match = lower.match(/\((\d+),(\d+)\)/);
      if (match) {
        column.precision = parseInt(match[1], 10);
        column.scale = parseInt(match[2], 10);
      }
    } else if (lower.includes('timestamp')) {
      const match = lower.match(/\((\d+)\)/);
      if (match) {
        column.precision = parseInt(match[1], 10);
      }
    }
  }

  /**
   * Enrich table metadata with row counts and size if statistics are available.
   * Hive can provide stats via `ANALYZE TABLE` results. A lightweight approach
   * is to query `SHOW TBLPROPERTIES` or use a `SELECT COUNT(*)` (may be expensive).
   * Here we attempt a `SHOW TBLPROPERTIES` and parse the `numRows` and `totalSize` keys.
   */
  private async enrichTableMetadata(table: TableInfo): Promise<void> {
    try {
      const propsSql = `SHOW TBLPROPERTIES \`${table.schemaname}\`.\`${table.tablename}\``;
      const result = await this.connection.query(propsSql);

      // The result has columns: prpt_name, prpt_value (depending on Hive version)
      const props: Record<string, string> = {};
      for (const row of result.rows) {
        if (row.prpt_name && row.prpt_value) {
          props[row.prpt_name.trim()] = row.prpt_value.trim();
        } else if (row[0] && row[1]) {
          props[row[0].trim()] = row[1].trim();
        }
      }

      if (props['numRows']) {
        table.rowCount = parseInt(props['numRows'], 10);
      }
      if (props['totalSize']) {
        table.size = this.formatBytes(parseInt(props['totalSize'], 10));
      }
      // Distinguish views via property 'virtual.view' if present
      if (props['virtual.view'] === 'true') {
        table.tabletype = 'view';
      } else if (props['table_type'] === 'VIRTUAL_VIEW') {
        table.tabletype = 'view';
      } else if (table.tabletype !== 'view') {
        table.tabletype = 'table';
      }

    } catch (error) {
      Logger.warn('failed to enrich metadata for table %s: %s',
        table.tablename, getErrorMessage(error));
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
   * Get Hive version and database information.
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
      encoding: undefined,    // Hive does not expose encoding in a simple way
      collation: undefined,
    };
  }

  /**
   * Get the underlying connection instance.
   */
  getConnection(): HiveConnection {
    return this.connection;
  }

  getCurrentSchema(): string {
    return this.schema;
  }

  setSchema(schema: string): void {
    this.schema = schema;
    // Update the connection’s database name as well
    const config = this.connection.getConfig();
    config.dbname = schema;
    // Recreate connection if needed? For simplicity, we only change the schema
    Logger.debug('schema set to: %s', schema);
  }

  /**
   * Flatten a possibly linked list of TableInfo.
   */
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

  /**
   * Convert to a standardised format for DatabaseMetadataWizard.
   */
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
// Exports – mirroring the PostgreSQL module
// ---------------------------------------------------------------------------
export {
  HiveSchemaInspector,
  HiveConnection,
  TableInfo,
  Logger,
  HiveConnectionError,
  HiveQueryError,
  getErrorMessage,
  isError,
};

export type {
  ColumnInfo,
  DatabaseConfig,
  QueryResult,
};

export default HiveSchemaInspector;