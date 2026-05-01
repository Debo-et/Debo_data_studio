/**
 * Enhanced Teradata Schema Inspector with Query Execution
 * Comprehensive schema inspection with robust error handling and connection management
 * TypeScript implementation optimized for DatabaseMetadataWizard integration
 * 
 * NOTE: This implementation uses the `teradata` npm package (https://www.npmjs.com/package/teradata)
 *       Install with: npm install teradata
 */

import { TeradataConnection, TeradataConnectionConfig } from 'teradata';

// Database connection configuration (adapted for Teradata)
interface DatabaseConfig {
  dbname: string;           // Teradata database name
  host?: string;            // Teradata host (default: 'localhost')
  port?: string;            // Port (default: '1025')
  user?: string;            // Username
  password?: string;        // Password
  schema?: string;          // Target schema/database (defaults to dbname)
  logmech?: string;         // Logon mechanism (LDAP, TD2, etc.)
  charset?: string;         // Character set
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
 * Teradata Connection Error Types (parallel to PostgreSQL errors)
 */
class TeradataConnectionError extends Error {
  constructor(
    message: string,
    public code?: string,
    public originalError?: unknown
  ) {
    super(message);
    this.name = 'TeradataConnectionError';
  }
}

class TeradataQueryError extends Error {
  constructor(
    message: string,
    public sql: string,
    public parameters: any[] = [],
    public originalError?: unknown
  ) {
    super(message);
    this.name = 'TeradataQueryError';
  }
}

/**
 * Teradata Connection Manager
 * Replaces PostgreSQL pool with a single Teradata connection.
 */
class TeradataConnectionManager {
  private connection: TeradataConnection | null = null;
  private isConnected: boolean = false;
  private readonly maxRetries: number = 3;

  constructor(private config: DatabaseConfig) {}

  /**
   * Initializes the Teradata connection.
   */
  async connect(): Promise<void> {
    if (this.isConnected && this.connection) {
      Logger.debug('connection already established');
      return;
    }

    const connectionConfig: TeradataConnectionConfig = {
      host: this.config.host || 'localhost',
      port: this.config.port ? parseInt(this.config.port) : 1025,
      user: this.config.user || '',
      password: this.config.password || '',
      logmech: this.config.logmech || 'TD2',
      charset: this.config.charset || 'UTF8',
    };

    let lastError: unknown = null;

    for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
      try {
        Logger.debug('connection attempt %d/%d', attempt, this.maxRetries);

        this.connection = new TeradataConnection();
        // Connect asynchronously (the driver's connect method returns a promise)
        await this.connection.connect(connectionConfig);

        // Test the connection with a simple query
        const result = await this.queryUnsafe('SELECT 1 as connection_test');
        if (!result || result.rowCount === 0) {
          throw new Error('Connection test returned no rows');
        }

        this.isConnected = true;
        Logger.info('successfully connected to Teradata database "%s" (attempt %d)',
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

    throw new TeradataConnectionError(
      `Failed to connect to Teradata database "${this.config.dbname}" after ${this.maxRetries} attempts: ${getErrorMessage(lastError)}`,
      'ECONNFAILED',
      lastError
    );
  }

  /**
   * Internal method: execute query without parameter substitution (for health checks, etc.)
   */
  private async queryUnsafe(sql: string): Promise<any> {
    if (!this.connection) {
      throw new TeradataConnectionError('No active Teradata connection');
    }
    return new Promise((resolve, reject) => {
      this.connection!.query(sql, (err, rows, rowCount, fields) => {
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
      const result = await this.queryUnsafe('SELECT 1 as health_check');
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
   * Execute a parameterized query.
   * Supports $1, $2, ... placeholders.
   * WARNING: Simple placeholder replacement – use with trusted parameters only.
   */
  async query(sql: string, params: any[] = []): Promise<any> {
    this.validateQueryParameters(sql, params);

    if (!this.isConnected || !this.connection) {
      throw new TeradataConnectionError('Database not connected');
    }

    // Substitute parameters
    const finalSql = this.substituteParameters(sql, params);

    Logger.debug('executing query: %s with %d parameters',
      finalSql.substring(0, 100) + (finalSql.length > 100 ? '...' : ''),
      params.length);

    const startTime = Date.now();
    try {
      const result = await this.queryUnsafe(finalSql);
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

      const queryError = new TeradataQueryError(
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
   * Uses Teradata's literal quoting rules for strings and numbers.
   */
  private substituteParameters(sql: string, params: any[]): string {
    if (!params.length) return sql;
    let idx = 1;
    const result = sql.replace(/\$(\d+)/g, (match, numStr: string) => {
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
        return value ? '1' : '0';
      }
      // String escaping: double any single quotes and wrap in single quotes
      if (typeof value === 'string') {
        const escaped = value.replace(/'/g, "''");
        return `'${escaped}'`;
      }
      // fallback: convert to string and quote
      const escaped = String(value).replace(/'/g, "''");
      return `'${escaped}'`;
    });
    return result;
  }

  /**
   * Validate query parameters to prevent basic injection patterns (unchanged logic)
   */
  private validateQueryParameters(sql: string, params: any[] = []): void {
    if (!sql || typeof sql !== 'string') {
      throw new TeradataQueryError('SQL query must be a non-empty string', sql, params);
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
   * Determine if error is a connection-level error (adapted for Teradata)
   */
  private isConnectionError(error: unknown): boolean {
    const errorMessage = getErrorMessage(error).toLowerCase();
    const connectionErrors = [
      'econnreset', 'econnrefused', 'epipe', 'etimedout',
      'connection', 'connect', 'socket', 'network', 'terminated',
      'stream closed', 'login failure'
    ];
    return connectionErrors.some(connError =>
      errorMessage.includes(connError.toLowerCase())
    );
  }

  /**
   * Closes the Teradata connection.
   */
  async disconnect(): Promise<void> {
    if (this.connection) {
      try {
        Logger.debug('closing Teradata connection');
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
   * Mock `getPool` method to maintain compatibility with the original interface.
   * Returns the underlying Teradata connection wrapped in a minimal compatible object
   * for transaction handling.
   */
  getPool(): any {
    if (!this.connection) return null;
    return {
      connect: async () => {
        // Return the same connection for client-like access
        return {
          query: (sql: string, params?: any[]) => this.query(sql, params),
          release: () => {}, // no-op
          on: () => {},
          off: () => {},
        };
      }
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
 * Enhanced Teradata Schema Inspector with Query Execution
 * Comprehensive metadata extraction with robust error handling
 */
class TeradataSchemaInspector {
  private connection: TeradataConnectionManager;
  private schema: string;

  constructor(config: DatabaseConfig) {
    this.connection = new TeradataConnectionManager(config);
    // In Teradata, schema is equivalent to database. If not provided, use the dbname.
    this.schema = config.schema || config.dbname;
  }

  /**
   * Set log level for debugging
   */
  static setLogLevel(level: 'DEBUG' | 'INFO' | 'WARN' | 'ERROR'): void {
    Logger.setLogLevel(level);
  }

  /**
   * Validate database connection parameters before connection
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

    if (config.schema && !/^[a-zA-Z_][a-zA-Z0-9_$#]*$/.test(config.schema)) {
      errors.push('Schema (database) name contains invalid characters');
    }

    return errors;
  }

  /**
   * Test connection without full schema inspection
   */
  async testConnection(): Promise<{ success: boolean; version?: string; error?: string }> {
    try {
      await this.connection.connect();

      const result = await this.connection.query('SELECT InfoData as version FROM DBC.DBCInfoV WHERE InfoKey = \'VERSION\'');

      if (result.rows && result.rows.length > 0) {
        const version = result.rows[0].version;
        const database = this.schema;

        Logger.info('connection test successful - database: %s, version: %s',
          database, version);

        return {
          success: true,
          version: version
        };
      }

      return { success: false, error: 'No data returned from version query' };
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
   * Executes arbitrary SQL query against the Teradata database
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

      // Fields mapping – Teradata driver returns fields as an array of { name, type, ... }
      const fields = result.fields?.map((field: any) => ({
        name: field.name,
        type: field.type
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
   * Apply row limit to SELECT queries (Teradata uses TOP N)
   */
  private applyRowLimit(sql: string, maxRows: number): string {
    const trimmed = sql.trim();
    const upperSql = trimmed.toUpperCase();

    if (upperSql.startsWith('SELECT')) {
      // Teradata syntax: SELECT TOP 100 * FROM ...
      if (!upperSql.includes('TOP')) {
        // Insert TOP after SELECT
        return trimmed.replace(/^SELECT/i, `SELECT TOP ${maxRows}`);
      } else {
        // Update existing TOP
        const topMatch = trimmed.match(/TOP\s+(\d+)/i);
        if (topMatch) {
          const currentLimit = parseInt(topMatch[1]);
          if (currentLimit > maxRows) {
            return trimmed.replace(/TOP\s+\d+/i, `TOP ${maxRows}`);
          }
        }
      }
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
   * Execute multiple queries in a transaction (Teradata transaction handling)
   */
  async executeTransaction(queries: Array<{ sql: string; params?: any[] }>): Promise<QueryResult[]> {
    await this.connection.connect();
    const pool = this.connection.getPool();
    if (!pool) throw new Error('Pool not available');

    // Simulated client (same connection)
    const client = await pool.connect();
    
    try {
      await client.query('BT;'); // Begin Transaction (Teradata mode)
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
          await client.query('ROLLBACK;');
          Logger.error('transaction failed at query %d: %s', i + 1, getErrorMessage(error));
          throw error;
        }
      }

      await client.query('ET;'); // End Transaction
      Logger.info('transaction completed successfully');
      return results;
    } catch (error) {
      try {
        await client.query('ROLLBACK;');
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
        TRIM(DatabaseName) AS schemaname,
        TRIM(TableName) AS tablename,
        TableKind,
        CommentString,
        CAST(NULL AS VARCHAR(50)) AS size,   -- filled later
        CAST(0 AS BIGINT) AS estimated_rows   -- filled later
      FROM DBC.TablesV
      WHERE DatabaseName = $1
        AND TableKind IN ('T','V','M','P')    -- Table, View, Macro, Procedure? Keep typical objects
        AND TableName NOT LIKE 'SYS%'
      ORDER BY DatabaseName, TableName
    `;

    try {
      const result = await this.connection.query(query, [this.schema]);
      const tables: TableInfo[] = [];

      Logger.info('found %d tables in schema "%s"', result.rows.length, this.schema);

      for (const row of result.rows) {
        const table = new TableInfo(row.schemaname, row.tablename);
        // Map Teradata TableKind to descriptive type
        switch (row.TableKind) {
          case 'T': table.tabletype = 'table'; break;
          case 'V': table.tabletype = 'view'; break;
          case 'M': table.tabletype = 'macro'; break;
          case 'P': table.tabletype = 'stored procedure'; break;
          default: table.tabletype = row.TableKind.toLowerCase();
        }
        table.comment = row.CommentString?.trim() || undefined;

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
   * Enhanced column retrieval from DBC.ColumnsV
   */
  private async getTableColumns(table: TableInfo): Promise<void> {
    const query = `
      SELECT
        TRIM(ColumnName) AS ColumnName,
        TRIM(ColumnType) AS ColumnType,
        ColumnLength,
        Nullable,
        ColumnFormat,
        CommentString,
        DefaultValue,
        IdColType
      FROM DBC.ColumnsV
      WHERE DatabaseName = $1
        AND TableName = $2
      ORDER BY ColumnId
    `;

    try {
      const result = await this.connection.query(query, [table.schemaname, table.tablename]);

      for (const row of result.rows) {
        const columnInfo: ColumnInfo = {
          name: row.ColumnName,
          type: row.ColumnType,
          nullable: row.Nullable === 'Y',
          default: row.DefaultValue?.trim() || undefined,
          comment: row.CommentString?.trim() || undefined,
          isIdentity: (row.IdColType || '').trim().length > 0,
          length: row.ColumnLength
        };

        // Extract precision/scale from ColumnType if present (e.g., 'DECIMAL(10,2)')
        this.extractTypeDetails(columnInfo, row.ColumnType);
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
   * Extract precision, scale, length from Teradata type string
   */
  private extractTypeDetails(column: ColumnInfo, typeString: string): void {
    // Handles patterns like "DECIMAL(10,2)", "VARCHAR(100) CHARACTER SET LATIN", etc.
    const precisionScaleMatch = typeString.match(/\((\d+),(\d+)\)/);
    if (precisionScaleMatch) {
      column.precision = parseInt(precisionScaleMatch[1], 10);
      column.scale = parseInt(precisionScaleMatch[2], 10);
    }
    const lengthMatch = typeString.match(/\((\d+)\)/);
    if (lengthMatch && !precisionScaleMatch) {
      column.length = parseInt(lengthMatch[1], 10);
    }
  }

  /**
   * Enrich table with size and estimated row count
   */
  private async enrichTableMetadata(table: TableInfo): Promise<void> {
    try {
      // Get table size from DBC.TableSizeV (CurrentPerm)
      const sizeQuery = `
        SELECT
          CAST(CurrentPerm AS DECIMAL(18,0)) AS totalBytes
        FROM DBC.TableSizeV
        WHERE DatabaseName = $1
          AND TableName = $2
      `;
      const sizeResult = await this.connection.query(sizeQuery, [table.schemaname, table.tablename]);
      if (sizeResult.rows.length > 0) {
        const bytes = parseInt(sizeResult.rows[0].totalBytes, 10);
        if (!isNaN(bytes)) {
          table.size = this.formatBytes(bytes);
        }
      }

      // Attempt to get row count from statistics (may be empty)
      const statsQuery = `
        SELECT
          CAST(RowCount AS BIGINT) AS rowCount
        FROM DBC.StatsV
        WHERE DatabaseName = $1
          AND TableName = $2
      `;
      const statsResult = await this.connection.query(statsQuery, [table.schemaname, table.tablename]);
      if (statsResult.rows.length > 0) {
        const rc = parseInt(statsResult.rows[0].rowCount, 10);
        if (!isNaN(rc)) {
          table.rowCount = rc;
        }
      }
    } catch (error) {
      Logger.warn('failed to enrich metadata for table %s: %s', table.tablename, getErrorMessage(error));
    }
  }

  /**
   * Format bytes to human-readable string
   */
  private formatBytes(bytes: number): string {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
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

    const query = `
      SELECT
        InfoData AS version
      FROM DBC.DBCInfoV
      WHERE InfoKey = 'VERSION'
    `;

    try {
      const result = await this.connection.query(query);
      const version = result.rows[0]?.version || 'Unknown';
      return {
        version,
        name: this.schema,
        encoding: 'Unicode',  // DBCInfo may not provide charset, assume UTF-8
        collation: 'Not Supported' // Teradata collation is per-column
      };
    } catch (error) {
      Logger.error('failed to get database info: %s', getErrorMessage(error));
      throw error;
    }
  }

  /**
   * Get the connection instance for external management
   */
  getConnection(): TeradataConnectionManager {
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
  TeradataSchemaInspector,
  TeradataConnectionManager as TeradataConnection,
  TableInfo,
  Logger,
  TeradataConnectionError,
  TeradataQueryError,
  getErrorMessage,
  isError
};

export type {
  ColumnInfo,
  DatabaseConfig,
  QueryResult
};

// Default export for convenience
export default TeradataSchemaInspector;