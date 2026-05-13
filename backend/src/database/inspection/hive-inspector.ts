/**
 * Enhanced Apache Hive Schema Inspector with Query Execution
 * Comprehensive schema inspection with robust error handling and connection management
 * TypeScript implementation optimized for DatabaseMetadataWizard integration
 *
 * NOTE: Uses the low-level 'hive-driver' Thrift client.
 * Parameterised queries (params array) are NOT supported by HiveServer2;
 * they will trigger a warning and the parameters will be ignored.
 *
 * Some IHiveSession and operation properties (getResultSetMetadata,
 * fetchResults, closeOperation, operationHandle, errorCode, errorMessage)
 * are not exposed in the public type definitions but are available at
 * runtime. They are accessed via `as any`.
 */

// eslint-disable-next-line @typescript-eslint/no-var-requires
const hiveDriver = require('hive-driver');

// Obtain the runtime objects we need. The public typings for hive-driver
// do not export them, so we extract them manually.
const HiveClient: any = hiveDriver.HiveClient;
const TCLIService: any = hiveDriver.TCLIService;
const TCLIService_types: any = hiveDriver.TCLIService_types;
const Status: any = hiveDriver.Status;

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
// Hive Connection Manager (wraps the real hive-driver Thrift client)
// ---------------------------------------------------------------------------
class HiveConnection {
  private client: any = null;   // HiveClient instance (typed as any to avoid import issues)
  private isConnected: boolean = false;
  private readonly maxRetries: number = 3;

  constructor(private config: DatabaseConfig) {}

  /**
   * Initialise the underlying HiveClient and open a Thrift connection.
   */
  async connect(): Promise<void> {
    if (this.isConnected && this.client) {
      Logger.debug('connection already established');
      return;
    }

    const host = this.config.host || 'localhost';
    const port = this.config.port ? parseInt(this.config.port, 10) : 10000;
    const username = this.config.user;
    const password = this.config.password;
    const databaseName = this.config.dbname || 'default';

    let lastError: unknown = null;

    for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
      try {
        Logger.debug('connection attempt %d/%d', attempt, this.maxRetries);

        // Create a new client each attempt
        this.client = new HiveClient(TCLIService, TCLIService_types);

        // Build connection options for the Thrift transport
        const connectOptions: any = {
          host,
          port,
        };

        // SASL PLAIN authentication (the only widely supported mechanism in hive-driver)
        if (username) {
          connectOptions.username = username;
          if (password) {
            connectOptions.password = password;
          }
          // Tell the client to use PLAIN SASL
          connectOptions.options = {
            sasl: { mechanism: 'PLAIN' },
          };
        }

        // Open the transport connection
        await this.client.connect(connectOptions);

        this.isConnected = true;
        Logger.info('successfully connected to Hive database "%s" (attempt %d)',
          databaseName, attempt);
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
      `Failed to connect to Hive database "${databaseName}" after ${this.maxRetries} attempts: ${getErrorMessage(lastError)}`,
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
      const result = await this.query('SELECT 1 AS health_check');
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
   * Execute a query and return a simplified result similar to the previous interface.
   * 
   * IMPORTANT: HiveServer2 does NOT support parameterised queries.
   * The `params` array is accepted for compatibility but will be **ignored**
   * with a warning. Do not rely on automatic escaping – interpolate values
   * safely into the SQL yourself if needed.
   */
  async query(sql: string, params?: any[]): Promise<{ rows: any[]; schema: Array<{ name: string; type: string }> }> {
    const safeParams = params ?? [];
    if (safeParams.length > 0) {
      Logger.warn(
        'Parameterised queries are not supported by Hive. ' +
        'Ignoring %d parameter(s). Use literal values or safe string interpolation.',
        safeParams.length
      );
    }

    if (!this.isConnected || !this.client) {
      throw new HiveConnectionError('Database not connected');
    }

    try {
      Logger.debug('executing query: %s',
        sql.substring(0, 100) + (sql.length > 100 ? '...' : ''));

      // Open a Hive session (required for executing statements)
      const session = await this.client.openSession({
        client_protocol: TCLIService_types.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1,
      });

      // Execute statement
      const executeResponse = await session.executeStatement(
        sql,
        { runAsync: false } // synchronous execution
      );

      // operationHandle is available at runtime but may not be in the typings
      const operation = (executeResponse as any).operationHandle;
      if (!operation) {
        throw new Error('No operation handle returned');
      }

      // Check for errors after execution – status can be a function in typings, cast to any
      if (Status.isError(executeResponse.status as any)) {
        const statusObj = executeResponse.status as any;
        throw new Error(
          `Hive execution error: ${statusObj.errorCode || ''} ${statusObj.errorMessage || ''}`
        );
      }

      // -------------------------------------------------------------------
      // NOTE:
      // The following three calls (getResultSetMetadata, fetchResults, closeOperation)
      // are present on IHiveSession at runtime but some typings may not expose them.
      // We use `as any` to avoid TypeScript errors; the runtime behaviour is unaffected.
      // -------------------------------------------------------------------

      // Fetch schema
      const metadataResponse = await (session as any).getResultSetMetadata(operation);
      const columns = metadataResponse.schema.columns;

      // Fetch all rows (paginate automatically)
      const allRows: any[] = [];
      let hasMoreRows = true;

      while (hasMoreRows) {
        const fetchResponse = await (session as any).fetchResults(operation, {
          orientation: TCLIService_types.TFetchOrientation.FETCH_NEXT,
          maxRows: 1000,
        });

        if (Status.isError(fetchResponse.status as any)) {
          throw new Error(`Fetch error: ${(fetchResponse.status as any).errorMessage}`);
        }

        if (fetchResponse.results && fetchResponse.results.rows) {
          for (const row of fetchResponse.results.rows) {
            const rowObj: any = {};
            row.colVals.forEach((val: any, idx: number) => {
              const colName = columns[idx]?.columnName || `col_${idx}`;
              rowObj[colName] = this.extractValue(val);
            });
            allRows.push(rowObj);
          }
        }

        hasMoreRows = fetchResponse.hasMoreRows ?? false;
      }

      // Close the operation and session to free resources
      await (session as any).closeOperation(operation);
      await session.close();

      // Build schema array as { name, type }
      const schema = columns.map((col: any) => ({
        name: col.columnName,
        type: col.typeDesc.types?.[0]?.primitiveEntry?.type?.toString() || 'STRING',
      }));

      return { rows: allRows, schema };
    } catch (error) {
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
      throw queryError;
    }
  }

  /**
   * Extracts a plain value from a TColumnValue object.
   */
  private extractValue(colVal: any): any {
    if (colVal.boolVal !== undefined) return colVal.boolVal;
    if (colVal.byteVal !== undefined) return colVal.byteVal;
    if (colVal.i16Val !== undefined) return colVal.i16Val;
    if (colVal.i32Val !== undefined) return colVal.i32Val;
    if (colVal.i64Val !== undefined) return colVal.i64Val;
    if (colVal.doubleVal !== undefined) return colVal.doubleVal;
    if (colVal.stringVal !== undefined) return colVal.stringVal;
    if (colVal.binaryVal !== undefined) return colVal.binaryVal;
    return null;
  }

  /**
   * Determine if an error is connection‑related.
   */
  private isConnectionError(error: unknown): boolean {
    const errorMessage = getErrorMessage(error).toLowerCase();
    const connectionErrors = [
      'econnreset', 'econnrefused', 'epipe', 'etimedout',
      'connection', 'connect', 'socket', 'network', 'terminated',
      'transport', 'thrift',
    ];
    return connectionErrors.some(term => errorMessage.includes(term));
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
  getClient(): any {
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
   * Parameterised queries are NOT supported – the `params` argument is ignored
   * (see HiveConnection.query documentation).
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

      // result.schema is already in {name, type} form
      const fields = result.schema;

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
   */
  async getTables(): Promise<TableInfo[]> {
    await this.connection.connect();

    const schema = this.schema;
    const showTablesSql = `SHOW TABLES IN \`${schema}\``;

    try {
      const tablesResult = await this.connection.query(showTablesSql);
      const tableNames: string[] = tablesResult.rows.map((row: any) => row.tab_name);

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
   * Get column details using `DESCRIBE table`.
   */
  private async getTableColumns(table: TableInfo): Promise<void> {
    const describeSql = `DESCRIBE \`${table.schemaname}\`.\`${table.tablename}\``;

    try {
      const result = await this.connection.query(describeSql);

      for (const row of result.rows) {
        const colName = row.col_name?.trim();
        if (!colName || colName.startsWith('#') || colName === '') {
          continue;
        }

        const columnInfo: ColumnInfo = {
          name: colName,
          type: row.data_type,
          comment: row.comment || undefined,
          nullable: true,
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
   */
  private async enrichTableMetadata(table: TableInfo): Promise<void> {
    try {
      const propsSql = `SHOW TBLPROPERTIES \`${table.schemaname}\`.\`${table.tablename}\``;
      const result = await this.connection.query(propsSql);

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
      if (props['virtual.view'] === 'true' || props['table_type'] === 'VIRTUAL_VIEW') {
        table.tabletype = 'view';
      } else {
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

  async getTable(tableName: string): Promise<TableInfo | null> {
    const allTables = await this.getTables();
    return allTables.find(t => t.tablename === tableName) || null;
  }

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

  getConnection(): HiveConnection {
    return this.connection;
  }

  getCurrentSchema(): string {
    return this.schema;
  }

  setSchema(schema: string): void {
    this.schema = schema;
    const config = this.connection.getConfig();
    config.dbname = schema;
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