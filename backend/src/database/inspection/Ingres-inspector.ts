/**
 * Enhanced Ingres Schema Inspector with Query Execution
 * Comprehensive schema inspection with robust error handling and connection management
 * TypeScript implementation optimized for DatabaseMetadataWizard integration
 *
 * NOTE: This implementation assumes a hypothetical Ingres driver with a callback-based
 *       query interface. Replace the driver import with the actual package you wish to use.
 *       For example: npm install ingres (or another community package).
 */

import { IngresClient } from 'ingres'; // Placeholder: actual driver may differ

// Database connection configuration
interface DatabaseConfig {
  dbname: string;           // Ingres database name (effective database)
  host?: string;            // Ingres host (default: 'localhost')
  port?: string;            // Port (default: 'II7' or '21064' depending on driver)
  user?: string;            // Username
  password?: string;        // Password
  schema?: string;          // Target schema (default: 'ingres' or user name)
  connectionTimeout?: number;
}

// Enhanced Column information structure (unchanged)
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

// Enhanced Table information structure (unchanged)
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

// Query execution result structure (unchanged)
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

// Utility function to safely extract error message (unchanged)
function getErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  } else if (typeof error === 'string') {
    return error;
  } else {
    return 'Unknown error occurred';
  }
}

// Utility function to check if error is instance of Error (unchanged)
function isError(error: unknown): error is Error {
  return error instanceof Error;
}

/**
 * Advanced logging utilities with log levels (unchanged)
 */
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

/**
 * Ingres Connection Error Types
 */
class IngresConnectionError extends Error {
  constructor(
    message: string,
    public code?: string,
    public originalError?: unknown
  ) {
    super(message);
    this.name = 'IngresConnectionError';
  }
}

class IngresQueryError extends Error {
  constructor(
    message: string,
    public sql: string,
    public parameters: any[] = [],
    public originalError?: unknown
  ) {
    super(message);
    this.name = 'IngresQueryError';
  }
}

/**
 * Ingres Connection Manager
 * Manages a single connection (similar to Vertica/Teradata managers).
 */
class IngresConnectionManager {
  private connection: any = null; // IngresClient instance
  private isConnected: boolean = false;
  private readonly maxRetries: number = 3;

  constructor(private config: DatabaseConfig) {}

  /**
   * Initializes the Ingres connection.
   */
  async connect(): Promise<void> {
    if (this.isConnected && this.connection) {
      Logger.debug('connection already established');
      return;
    }

    const connectionConfig = {
      host: this.config.host || 'localhost',
      port: this.config.port ? parseInt(this.config.port) : 21064, // Default Ingres II port
      user: this.config.user || '',
      password: this.config.password || '',
      database: this.config.dbname,
      appName: 'schema_inspector',
      timeout: this.config.connectionTimeout || 30000,
    };

    let lastError: unknown = null;

    for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
      try {
        Logger.debug('connection attempt %d/%d', attempt, this.maxRetries);

        this.connection = new IngresClient();
        await this.connection.connect(connectionConfig);

        // Test connection
        const result = await this.queryAsync('SELECT 1 AS connection_test');
        if (!result || result.rowCount === 0) {
          throw new Error('Connection test returned no rows');
        }

        this.isConnected = true;
        Logger.info('successfully connected to Ingres database "%s" (attempt %d)',
          this.config.dbname, attempt);
        return;

      } catch (error) {
        lastError = error;
        this.isConnected = false;
        if (this.connection) {
          try {
            this.connection.close();
          } catch {}
          this.connection = null;
        }

        Logger.warn('connection attempt %d failed: %s', attempt, getErrorMessage(error));

        if (attempt < this.maxRetries) {
          const backoffTime = Math.min(1000 * Math.pow(2, attempt - 1), 10000);
          Logger.debug('waiting %d ms before retry', backoffTime);
          await new Promise(resolve => setTimeout(resolve, backoffTime));
        }
      }
    }

    throw new IngresConnectionError(
      `Failed to connect to Ingres database "${this.config.dbname}" after ${this.maxRetries} attempts: ${getErrorMessage(lastError)}`,
      'ECONNFAILED',
      lastError
    );
  }

  /**
   * Internal helper to promisify the query call (assuming callback-based driver).
   */
  private queryAsync(sql: string): Promise<any> {
    return new Promise((resolve, reject) => {
      this.connection.query(sql, (err: any, rows: any[], rowCount: number, fields: any[]) => {
        if (err) {
          reject(err);
        } else {
          resolve({ rows, rowCount, fields });
        }
      });
    });
  }

  /**
   * Check connection status and health
   */
  async checkHealth(): Promise<boolean> {
    if (!this.isConnected || !this.connection) {
      return false;
    }
    try {
      const result = await this.queryAsync('SELECT 1 AS health_check');
      return result && result.rowCount > 0;
    } catch (error) {
      this.isConnected = false;
      Logger.error('connection health check failed: %s', getErrorMessage(error));
      return false;
    }
  }

  /**
   * Get connection status
   */
  get connectionStatus(): { connected: boolean; healthy?: boolean } {
    return {
      connected: this.isConnected,
      healthy: this.isConnected ? undefined : false
    };
  }

  /**
   * Execute a parameterized query with $1, $2, ... substitution.
   */
  async query(sql: string, params: any[] = []): Promise<any> {
    this.validateQueryParameters(sql, params);

    if (!this.isConnected || !this.connection) {
      throw new IngresConnectionError('Database not connected');
    }

    const finalSql = this.substituteParameters(sql, params);

    Logger.debug('executing query: %s with %d parameters',
      finalSql.substring(0, 100) + (finalSql.length > 100 ? '...' : ''),
      params.length);

    const startTime = Date.now();
    try {
      const result = await this.queryAsync(finalSql);
      const duration = Date.now() - startTime;
      Logger.debug('query completed in %d ms, %d rows returned',
        duration, result.rowCount || 0);
      return result;
    } catch (error) {
      const duration = Date.now() - startTime;
      if (this.isConnectionError(error)) {
        this.isConnected = false;
        Logger.warn('connection lost during query');
      }

      const queryError = new IngresQueryError(
        `Query execution failed: ${getErrorMessage(error)}`,
        sql,
        params,
        error
      );
      Logger.error('query failed: %s', getErrorMessage(error));
      Logger.debug('failed query: %s', finalSql);
      Logger.debug('parameters: %o', params);
      throw queryError;
    }
  }

  /**
   * Simple parameter substitution for $1, $2, ... placeholders.
   * Ingres uses single quotes for strings; we double them if they appear inside.
   */
  private substituteParameters(sql: string, params: any[]): string {
    if (!params.length) return sql;
    return sql.replace(/\$(\d+)/g, (match, numStr: string) => {
      const paramIndex = parseInt(numStr) - 1;
      if (paramIndex >= params.length) {
        Logger.warn('parameter index out of range: $%d', numStr);
        return match;
      }
      const value = params[paramIndex];
      if (value === null || value === undefined) {
        return 'NULL';
      }
      if (typeof value === 'number') {
        return String(value);
      }
      if (typeof value === 'boolean') {
        return value ? '1' : '0'; // Ingres uses 1/0 for boolean
      }
      // String escaping: double any single quotes and wrap in single quotes
      if (typeof value === 'string') {
        const escaped = value.replace(/'/g, "''");
        return `'${escaped}'`;
      }
      const escaped = String(value).replace(/'/g, "''");
      return `'${escaped}'`;
    });
  }

  /**
   * Validate query parameters (unchanged logic)
   */
  private validateQueryParameters(sql: string, params: any[] = []): void {
    if (!sql || typeof sql !== 'string') {
      throw new IngresQueryError('SQL query must be a non-empty string', sql, params);
    }
    params.forEach((param, index) => {
      if (typeof param === 'string') {
        const suspiciousPatterns = [
          /(\bDROP\b|\bDELETE\b|\bINSERT\b|\bUPDATE\b|\bALTER\b|\bCREATE\b|\bEXEC\b)/i,
          /(\-\-|\/\*|\*\/|;)/,
          /(\bUNION\b.*\bSELECT\b)/i
        ];
        for (const pattern of suspiciousPatterns) {
          if (pattern.test(param)) {
            Logger.warn('suspicious parameter detected at position %d: %s', index, param);
          }
        }
      }
    });
    const placeholderCount = (sql.match(/\$/g) || []).length;
    if (placeholderCount !== params.length) {
      Logger.warn('parameter count mismatch: %d placeholders, %d parameters',
        placeholderCount, params.length);
    }
  }

  /**
   * Determine if error is a connection-level error
   */
  private isConnectionError(error: unknown): boolean {
    const errorMessage = getErrorMessage(error).toLowerCase();
    const connectionErrors = [
      'econnreset', 'econnrefused', 'epipe', 'etimedout',
      'connection', 'connect', 'socket', 'network', 'terminated',
      'closed', 'refused', 'ingres'
    ];
    return connectionErrors.some(connError =>
      errorMessage.includes(connError.toLowerCase())
    );
  }

  /**
   * Closes the Ingres connection.
   */
  async disconnect(): Promise<void> {
    if (this.connection) {
      try {
        Logger.debug('closing Ingres connection');
        this.connection.close();
        Logger.info('connection closed successfully');
      } catch (error) {
        Logger.error('error closing connection: %s', getErrorMessage(error));
      } finally {
        this.isConnected = false;
        this.connection = null;
      }
    }
  }

  /**
   * Mock getPool to maintain interface compatibility.
   */
  getPool(): any {
    if (!this.connection) return null;
    return {
      connect: async () => ({
        query: (sql: string, params?: any[]) => this.query(sql, params),
        release: () => {},
        on: () => {},
        off: () => {},
      })
    };
  }

  /**
   * Get connection configuration (without password)
   */
  getConfig(): Omit<DatabaseConfig, 'password'> {
    const { password, ...safeConfig } = this.config;
    return safeConfig;
  }
}

/**
 * Enhanced Ingres Schema Inspector with Query Execution
 * Comprehensive metadata extraction with robust error handling
 */
class IngresSchemaInspector {
  private connection: IngresConnectionManager;
  private schema: string;

  constructor(config: DatabaseConfig) {
    this.connection = new IngresConnectionManager(config);
    // Ingres default schema is often the user name if not specified
    this.schema = config.schema || config.user || 'ingres';
  }

  /**
   * Set log level for debugging
   */
  static setLogLevel(level: 'DEBUG' | 'INFO' | 'WARN' | 'ERROR'): void {
    Logger.setLogLevel(level);
  }

  /**
   * Validate database connection parameters
   */
  static validateConnectionConfig(config: DatabaseConfig): string[] {
    const errors: string[] = [];

    if (!config.dbname || config.dbname.trim() === '') {
      errors.push('Database name is required');
    }

    if (config.port) {
      const port = parseInt(config.port);
      if (isNaN(port)) {
        errors.push('Port must be a valid number');
      } else if (port < 1 || port > 65535) {
        errors.push('Port must be between 1 and 65535');
      }
    }

    if (config.schema && !/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(config.schema)) {
      errors.push('Schema name contains invalid characters');
    }

    return errors;
  }

  /**
   * Test connection without full schema inspection
   */
  async testConnection(): Promise<{ success: boolean; version?: string; error?: string }> {
    try {
      await this.connection.connect();

      // Ingres version info via dbmsinfo('_version') or dbmsinfo('version')
      let version = 'Unknown';
      try {
        const result = await this.connection.query("SELECT dbmsinfo('_version') AS version");
        if (result.rows && result.rows.length > 0) {
          version = result.rows[0].version;
        }
      } catch {
        // Fallback
        version = 'Ingres (version unknown)';
      }

      Logger.info('connection test successful - database: %s, version: %s',
        this.config.dbname, version);

      return {
        success: true,
        version: version
      };
    } catch (error) {
      const errorMessage = getErrorMessage(error);
      Logger.error('connection test failed: %s', errorMessage);
      return {
        success: false,
        error: errorMessage
      };
    } finally {
      await this.connection.disconnect();
    }
  }

  /**
   * Executes arbitrary SQL query against the Ingres database
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

      Logger.info('executing arbitrary SQL query');
      Logger.debug('query: %s', sql.substring(0, 200) + (sql.length > 200 ? '...' : ''));

      let finalSql = sql;
      if (maxRows && sql.trim().toUpperCase().startsWith('SELECT')) {
        finalSql = this.applyRowLimit(sql, maxRows);
      }

      const result = await this.connection.query(finalSql, params);
      const executionTime = Date.now() - startTime;

      const fields = result.fields?.map((field: any) => ({
        name: field.name,
        type: field.type || field.dataType
      })) || [];

      const queryResult: QueryResult = {
        success: true,
        rows: result.rows,
        rowCount: result.rowCount ?? undefined,
        fields,
        executionTime,
        affectedRows: result.rowCount ?? undefined,
        command: 'generic'
      };

      Logger.info('query executed successfully in %d ms, returned %d rows',
        executionTime, result.rowCount || 0);

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
        rowCount: 0
      };
    } finally {
      if (autoDisconnect) {
        await this.connection.disconnect();
      }
    }
  }

  /**
   * Apply row limit to SELECT queries (Ingres uses FETCH FIRST n ROWS ONLY or LIMIT n).
   * We'll use FETCH FIRST syntax for broader compatibility.
   */
  private applyRowLimit(sql: string, maxRows: number): string {
    const trimmed = sql.trim();
    const upperSql = trimmed.toUpperCase();

    if (upperSql.startsWith('SELECT')) {
      // Remove any existing FETCH FIRST clause
      const fetchRegex = /FETCH\s+FIRST\s+\d+\s+ROWS\s+ONLY$/i;
      let baseSql = trimmed.replace(fetchRegex, '').trim();
      // Avoid adding to subqueries; simplest approach: append
      return `${baseSql} FETCH FIRST ${maxRows} ROWS ONLY`;
    }
    return sql;
  }

  /**
   * Execute a query with auto-disconnect (convenience method)
   */
  async executeQueryAndDisconnect(sql: string, params: any[] = []): Promise<QueryResult> {
    return this.executeQuery(sql, params, { autoDisconnect: true });
  }

  /**
   * Execute multiple queries in a transaction
   */
  async executeTransaction(queries: Array<{ sql: string; params?: any[] }>): Promise<QueryResult[]> {
    await this.connection.connect();
    const pool = this.connection.getPool();
    if (!pool) throw new Error('Pool not available');

    const client = await pool.connect();

    try {
      await client.query('BEGIN WORK');
      Logger.info('starting transaction with %d queries', queries.length);

      const results: QueryResult[] = [];

      for (let i = 0; i < queries.length; i++) {
        const { sql, params = [] } = queries[i];
        Logger.debug('executing transaction query %d/%d', i + 1, queries.length);

        const startTime = Date.now();
        try {
          const result = await client.query(sql, params);
          const executionTime = Date.now() - startTime;

          results.push({
            success: true,
            rows: result.rows,
            rowCount: result.rowCount ?? undefined,
            executionTime,
            affectedRows: result.rowCount ?? undefined,
            command: 'transaction-query'
          });
        } catch (error) {
          await client.query('ROLLBACK WORK');
          Logger.error('transaction failed at query %d: %s', i + 1, getErrorMessage(error));
          throw error;
        }
      }

      await client.query('COMMIT WORK');
      Logger.info('transaction completed successfully');
      return results;
    } catch (error) {
      try {
        await client.query('ROLLBACK WORK');
      } catch (rollbackError) {
        Logger.warn('rollback failed: %s', getErrorMessage(rollbackError));
      }
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Retrieves all tables from the database with comprehensive metadata
   */
  async getTables(): Promise<TableInfo[]> {
    await this.connection.connect();

    const query = `
      SELECT 
        TRIM(schema_name) AS schemaname,
        TRIM(table_name) AS tablename,
        CASE 
          WHEN table_type = 'T' THEN 'table'
          WHEN table_type = 'V' THEN 'view'
          ELSE 'other'
        END AS tabletype,
        table_remarks AS comment,
        NULL AS size,
        CAST(0 AS BIGINT) AS estimated_rows
      FROM iitables
      WHERE schema_name = $1
        AND table_type IN ('T', 'V')
      ORDER BY schema_name, table_name
    `;

    try {
      const result = await this.connection.query(query, [this.schema]);
      const tables: TableInfo[] = [];

      Logger.info('found %d tables in schema "%s"', result.rows.length, this.schema);

      for (const row of result.rows) {
        const table = new TableInfo(row.schemaname, row.tablename);
        table.tabletype = row.tabletype;
        table.comment = row.comment?.trim() || undefined;

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
   * Enhanced column retrieval from iicolumns
   */
  private async getTableColumns(table: TableInfo): Promise<void> {
    const query = `
      SELECT
        TRIM(column_name) AS column_name,
        TRIM(column_datatype) AS column_type,
        column_length,
        column_scale,
        CASE column_nulls WHEN 'Y' THEN 1 ELSE 0 END AS is_nullable,
        column_default_val AS column_default,
        column_remarks AS comment,
        CASE WHEN column_sequence > 0 THEN 1 ELSE 0 END AS is_identity
      FROM iicolumns
      WHERE schema_name = $1
        AND table_name = $2
      ORDER BY column_sequence
    `;

    try {
      const result = await this.connection.query(query, [table.schemaname, table.tablename]);

      for (const row of result.rows) {
        const columnInfo: ColumnInfo = {
          name: row.column_name,
          type: row.column_type,
          nullable: row.is_nullable === 1,
          default: row.column_default?.trim() || undefined,
          comment: row.comment?.trim() || undefined,
          isIdentity: row.is_identity === 1,
          length: row.column_length ? parseInt(row.column_length) : undefined,
          precision: row.column_length ? parseInt(row.column_length) : undefined,
          scale: row.column_scale ? parseInt(row.column_scale) : undefined
        };

        table.columns.push(columnInfo);
      }

      Logger.debug('retrieved %d columns for table %s',
        result.rows.length, table.tablename);
    } catch (error) {
      Logger.error('column retrieval failed for table %s: %s',
        table.tablename, getErrorMessage(error));
    }
  }

  /**
   * Enrich table with size and row count (using system procedure ifavailable)
   */
  private async enrichTableMetadata(table: TableInfo): Promise<void> {
    try {
      // Attempt to get row count from statistics (may not be present)
      const countQuery = `
        SELECT num_rows 
        FROM iitables 
        WHERE schema_name = $1 AND table_name = $2
      `;
      const countResult = await this.connection.query(countQuery, [table.schemaname, table.tablename]);
      if (countResult.rows.length > 0) {
        const rc = parseInt(countResult.rows[0].num_rows, 10);
        if (!isNaN(rc)) {
          table.rowCount = rc;
        }
      }

      // Table size is not directly available in Ingres system catalogs; skip.
      table.size = 'N/A';
    } catch (error) {
      Logger.warn('failed to enrich metadata for table %s: %s', table.tablename, getErrorMessage(error));
    }
  }

  /**
   * Get specific table metadata by name
   */
  async getTable(tableName: string): Promise<TableInfo | null> {
    const allTables = await this.getTables();
    return allTables.find(table => table.tablename === tableName) || null;
  }

  /**
   * Get database version and information
   */
  async getDatabaseInfo(): Promise<{
    version: string;
    name: string;
    encoding: string;
    collation: string;
  }> {
    await this.connection.connect();

    try {
      let version = 'Unknown';
      try {
        const verResult = await this.connection.query("SELECT dbmsinfo('_version') AS version");
        if (verResult.rows.length > 0) version = verResult.rows[0].version;
      } catch {}

      return {
        version,
        name: this.config.dbname,
        encoding: 'ISO-8859-1',  // Ingres default
        collation: 'N/A'
      };
    } catch (error) {
      Logger.error('failed to get database info: %s', getErrorMessage(error));
      throw error;
    }
  }

  /**
   * Get the connection instance for external management
   */
  getConnection(): IngresConnectionManager {
    return this.connection;
  }

  /**
   * Get current schema
   */
  getCurrentSchema(): string {
    return this.schema;
  }

  /**
   * Set schema for table discovery
   */
  setSchema(schema: string): void {
    this.schema = schema;
    Logger.debug('schema set to: %s', schema);
  }

  /**
   * Utility method to convert table list to array (unchanged)
   */
  static flattenTableList(tables: TableInfo | TableInfo[]): TableInfo[] {
    if (Array.isArray(tables)) {
      return tables;
    }
    const result: TableInfo[] = [];
    let current: TableInfo | null = tables;
    while (current) {
      result.push(current);
      current = current.next;
    }
    return result;
  }

  /**
   * Generate standardized table metadata for DatabaseMetadataWizard (unchanged)
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
        scale: col.scale
      })),
      comment: table.comment,
      rowCount: table.rowCount,
      size: table.size,
      originalData: table
    }));
  }
}

// Export enhanced functionality
export {
  IngresSchemaInspector,
  IngresConnectionManager as IngresConnection,
  TableInfo,
  Logger,
  IngresConnectionError,
  IngresQueryError,
  getErrorMessage,
  isError
};

export type {
  ColumnInfo,
  DatabaseConfig,
  QueryResult
};

// Default export for convenience
export default IngresSchemaInspector;