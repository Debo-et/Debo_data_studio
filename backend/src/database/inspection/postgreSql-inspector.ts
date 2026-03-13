/**
 * Enhanced PostgreSQL Schema Inspector with Query Execution
 * Comprehensive schema inspection with robust error handling and connection management
 * TypeScript implementation optimized for DatabaseMetadataWizard integration
 */

import { Pool, PoolConfig } from 'pg';

// Database connection configuration
interface DatabaseConfig {
  dbname: string;
  pghost?: string;
  pgport?: string;
  pguser?: string;
  password?: string;
  schema?: string;
}

// Enhanced Column information structure
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

// Enhanced Table information structure
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

  // Convert to similar structure as C implementation for compatibility
  get column_names(): string[] {
    return this.columns.map(col => col.name);
  }

  get column_types(): string[] {
    return this.columns.map(col => col.type);
  }

  // Enhanced: Get column by name
  getColumn(name: string): ColumnInfo | undefined {
    return this.columns.find(col => col.name === name);
  }

  // Enhanced: Check if column exists
  hasColumn(name: string): boolean {
    return this.columns.some(col => col.name === name);
  }
}

// Query execution result structure
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

// Utility function to safely extract error message
function getErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  } else if (typeof error === 'string') {
    return error;
  } else {
    return 'Unknown error occurred';
  }
}

// Utility function to check if error is instance of Error
function isError(error: unknown): error is Error {
  return error instanceof Error;
}

/**
 * Advanced logging utilities with log levels
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
 * PostgreSQL Connection Error Types
 */
class PostgreSQLConnectionError extends Error {
  constructor(
    message: string,
    public code?: string,
    public originalError?: unknown
  ) {
    super(message);
    this.name = 'PostgreSQLConnectionError';
  }
}

class PostgreSQLQueryError extends Error {
  constructor(
    message: string,
    public sql: string,
    public parameters: any[] = [],
    public originalError?: unknown
  ) {
    super(message);
    this.name = 'PostgreSQLQueryError';
  }
}

/**
 * PostgreSQL Connection Manager using a connection pool
 */
class PostgreSQLConnection {
  private pool: Pool | null = null;
  private isConnected: boolean = false;
  private readonly maxRetries: number = 3;

  constructor(private config: DatabaseConfig) {}

  /**
   * Initializes the connection pool
   */
  async connect(): Promise<void> {
    if (this.isConnected && this.pool) {
      Logger.debug('connection already established');
      return;
    }

    const poolConfig: PoolConfig = {
      database: this.config.dbname,
      host: this.config.pghost || 'localhost',
      port: this.config.pgport ? parseInt(this.config.pgport) : 5432,
      user: this.config.pguser,
      password: this.config.password,
      application_name: 'schema_inspector',
      connectionTimeoutMillis: 15000,
      query_timeout: 30000,
      idle_in_transaction_session_timeout: 60000,
      // Pool settings
      max: 10,                     // Maximum number of clients in the pool
      idleTimeoutMillis: 300000,    // Close idle clients after 5 minutes
      keepAlive: true,
      keepAliveInitialDelayMillis: 30000,
      ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
    };

    let lastError: unknown = null;

    for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
      try {
        Logger.debug('connection attempt %d/%d', attempt, this.maxRetries);

        this.pool = new Pool(poolConfig);

        // Attach pool-level error handler
        this.pool.on('error', (err) => {
          Logger.error('Unexpected error on idle client: %s', getErrorMessage(err));
          // The pool will automatically replace the dead client
        });

        // Test the connection by acquiring a client and running a simple query
        const client = await this.pool.connect();
        try {
          // Attach a temporary error handler to this specific client (for safety)
          const errorHandler = (err: Error) => {
            Logger.error(`Client connection error during test: ${err.message}`);
            client.release(err);
          };
          client.once('error', errorHandler);

          await client.query('SELECT 1 as connection_test');
          client.off('error', errorHandler);
        } finally {
          client.release();
        }

        this.isConnected = true;
        Logger.info('successfully connected to database "%s" (attempt %d)',
          this.config.dbname, attempt);
        return;

      } catch (error) {
        lastError = error;
        this.isConnected = false;
        if (this.pool) {
          await this.pool.end().catch(() => {});
          this.pool = null;
        }

        Logger.warn('connection attempt %d failed: %s', attempt, getErrorMessage(error));

        if (attempt < this.maxRetries) {
          const backoffTime = Math.min(1000 * Math.pow(2, attempt - 1), 10000);
          Logger.debug('waiting %d ms before retry', backoffTime);
          await new Promise(resolve => setTimeout(resolve, backoffTime));
        }
      }
    }

    throw new PostgreSQLConnectionError(
      `Failed to connect to database "${this.config.dbname}" after ${this.maxRetries} attempts: ${getErrorMessage(lastError)}`,
      'ECONNFAILED',
      lastError
    );
  }

  /**
   * Check connection status and health
   */
  async checkHealth(): Promise<boolean> {
    if (!this.isConnected || !this.pool) {
      return false;
    }

    try {
      const client = await this.pool.connect();
      try {
        const errorHandler = (err: Error) => {
          Logger.error(`Client connection error during health check: ${err.message}`);
          client.release(err);
        };
        client.once('error', errorHandler);

        const result = await client.query('SELECT 1 as health_check');
        client.off('error', errorHandler);
        return result.rows.length > 0 && result.rows[0].health_check === 1;
      } finally {
        client.release();
      }
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
   * Executes SQL query using a client from the pool
   */
  async query(sql: string, params: any[] = []): Promise<any> {
    this.validateQueryParameters(sql, params);

    if (!this.isConnected || !this.pool) {
      throw new PostgreSQLConnectionError('Database not connected');
    }

    const client = await this.pool.connect();
    const errorHandler = (err: Error) => {
      Logger.error(`Client connection error during query: ${err.message}`);
      // Release the client with error (pool will discard it)
      client.release(err);
    };
    client.once('error', errorHandler);

    try {
      Logger.debug('executing query: %s with %d parameters',
        sql.substring(0, 100) + (sql.length > 100 ? '...' : ''),
        params.length);

      const startTime = Date.now();
      const result = await client.query(sql, params);
      const duration = Date.now() - startTime;

      Logger.debug('query completed in %d ms, %d rows returned',
        duration, result.rows.length);

      return result;
    } catch (error) {
      // If the error indicates a lost connection, mark as disconnected
      if (this.isConnectionError(error)) {
        this.isConnected = false;
        Logger.warn('connection lost during query');
      }

      const queryError = new PostgreSQLQueryError(
        `Query execution failed: ${getErrorMessage(error)}`,
        sql,
        params,
        error
      );

      Logger.error('query failed: %s', getErrorMessage(error));
      Logger.debug('failed query: %s', sql);
      Logger.debug('parameters: %o', params);

      throw queryError;
    } finally {
      client.off('error', errorHandler);
      // Only release if not already released by errorHandler
      if (!(client as any).releaseCalled) {
        client.release();
      }
    }
  }

  /**
   * Validate query parameters to prevent SQL injection and syntax errors
   */
  private validateQueryParameters(sql: string, params: any[]): void {
    if (!sql || typeof sql !== 'string') {
      throw new PostgreSQLQueryError('SQL query must be a non-empty string', sql, params);
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
      'ECONNRESET', 'ECONNREFUSED', 'EPIPE', 'ETIMEDOUT',
      'connection', 'connect', 'socket', 'network', 'terminated'
    ];

    return connectionErrors.some(connError =>
      errorMessage.includes(connError.toLowerCase())
    );
  }

  /**
   * Closes all connections in the pool
   */
  async disconnect(): Promise<void> {
    if (this.pool) {
      try {
        Logger.debug('closing connection pool');
        await this.pool.end();
        Logger.info('connection pool closed successfully');
      } catch (error) {
        Logger.error('error closing pool: %s', getErrorMessage(error));
      } finally {
        this.isConnected = false;
        this.pool = null;
      }
    }
  }

  /**
   * Get the underlying pool (for advanced operations)
   */
  getPool(): Pool | null {
    return this.pool;
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
 * Enhanced PostgreSQL Schema Inspector with Query Execution
 * Comprehensive metadata extraction with robust error handling
 */
class PostgreSQLSchemaInspector {
  private connection: PostgreSQLConnection;
  private schema: string = 'public';

  constructor(config: DatabaseConfig) {
    this.connection = new PostgreSQLConnection(config);
    this.schema = config.schema || 'public';
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

    if (config.pgport) {
      const port = parseInt(config.pgport);
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

      const result = await this.connection.query('SELECT version() as version, current_database() as database');

      if (result.rows && result.rows.length > 0) {
        const version = result.rows[0].version;
        const database = result.rows[0].database;

        Logger.info('connection test successful - database: %s, version: %s',
          database, version.split(',')[0]);

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
   * Executes arbitrary SQL query against the PostgreSQL database
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

      if (maxRows && sql.trim().toUpperCase().startsWith('SELECT')) {
        sql = this.applyRowLimit(sql, maxRows);
      }

      const result = await this.connection.query(sql, params);
      const executionTime = Date.now() - startTime;

      const fields = result.fields?.map((field: any) => ({
        name: field.name,
        type: field.dataTypeID ? `OID:${field.dataTypeID}` : field.dataTypeName || 'unknown'
      })) || [];

      const queryResult: QueryResult = {
        success: true,
        rows: result.rows,
        rowCount: result.rowCount ?? undefined,
        fields,
        executionTime,
        affectedRows: result.rowCount ?? undefined,
        command: result.command
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
   * Apply row limit to SELECT queries
   */
  private applyRowLimit(sql: string, maxRows: number): string {
    const trimmed = sql.trim();
    const upperSql = trimmed.toUpperCase();

    if (upperSql.startsWith('SELECT')) {
      if (!upperSql.includes('LIMIT')) {
        return `${trimmed} LIMIT ${maxRows}`;
      } else {
        const limitMatch = trimmed.match(/LIMIT\s+(\d+)/i);
        if (limitMatch) {
          const currentLimit = parseInt(limitMatch[1]);
          if (currentLimit > maxRows) {
            return trimmed.replace(/LIMIT\s+\d+/i, `LIMIT ${maxRows}`);
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
   * Execute multiple queries in a transaction
   */
  async executeTransaction(queries: Array<{ sql: string; params?: any[] }>): Promise<QueryResult[]> {
    await this.connection.connect();
    const pool = this.connection.getPool();
    if (!pool) throw new Error('Pool not available');

    const client = await pool.connect();
    const errorHandler = (err: Error) => {
      Logger.error(`Client connection error during transaction: ${err.message}`);
      client.release(err);
    };
    client.once('error', errorHandler);

    try {
      await client.query('BEGIN');
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
            command: result.command
          });
        } catch (error) {
          await client.query('ROLLBACK');
          Logger.error('transaction failed at query %d: %s', i + 1, getErrorMessage(error));
          throw error;
        }
      }

      await client.query('COMMIT');
      Logger.info('transaction completed successfully');
      return results;
    } catch (error) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackError) {
        Logger.warn('rollback failed: %s', getErrorMessage(rollbackError));
      }
      throw error;
    } finally {
      client.off('error', errorHandler);
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
        n.nspname as schemaname, 
        c.relname as tablename, 
        c.oid,
        CASE 
          WHEN c.relkind = 'r' THEN 'table'
          WHEN c.relkind = 'v' THEN 'view'
          WHEN c.relkind = 'm' THEN 'materialized view'
          WHEN c.relkind = 'f' THEN 'foreign table'
          WHEN c.relkind = 'p' THEN 'partitioned table'
          ELSE 'other'
        END as tabletype,
        obj_description(c.oid) as comment,
        pg_size_pretty(pg_total_relation_size(c.oid)) as size,
        (SELECT reltuples FROM pg_class WHERE oid = c.oid) as estimated_rows
      FROM pg_class c
      JOIN pg_namespace n ON c.relnamespace = n.oid
      WHERE c.relkind IN ('r','v','m','f','p')
      AND n.nspname = $1
      AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
      AND n.nspname NOT LIKE 'pg_temp_%' 
      AND n.nspname NOT LIKE 'pg_toast_temp_%'
      AND has_schema_privilege(n.oid, 'USAGE')
      AND has_table_privilege(c.oid, 'SELECT')
      ORDER BY n.nspname, c.relname
    `;

    try {
      const result = await this.connection.query(query, [this.schema]);
      const tables: TableInfo[] = [];

      Logger.info('found %d tables in schema "%s"', result.rows.length, this.schema);

      for (const row of result.rows) {
        const table = new TableInfo(row.schemaname, row.tablename);
        table.tabletype = row.tabletype;
        table.comment = row.comment;
        table.size = row.size;
        table.rowCount = parseInt(row.estimated_rows) || 0;

        await this.getTableColumns(table, row.oid);
        await this.enrichTableMetadata(table, row.oid);

        tables.push(table);
      }

      return tables;
    } catch (error) {
      Logger.error('Failed to retrieve tables: %s', getErrorMessage(error));
      throw error;
    }
  }

  /**
   * Enhanced column retrieval with comprehensive metadata
   */
  private async getTableColumns(table: TableInfo, tableOid: number): Promise<void> {
    const query = `
      SELECT 
        a.attname,
        pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
        a.attnotnull AS is_not_null,
        a.attnum AS column_number,
        pg_catalog.col_description(a.attrelid, a.attnum) AS column_comment,
        CASE 
          WHEN a.attidentity IN ('a', 'd') THEN true
          ELSE false
        END AS is_identity,
        CASE 
          WHEN a.atthasdef THEN pg_get_expr(ad.adbin, ad.adrelid)
          ELSE NULL
        END AS column_default,
        CASE 
          WHEN t.typtype = 'e' THEN 'enum'
          WHEN t.typtype = 'd' THEN 'domain'
          ELSE 'base'
        END AS type_category,
        a.atttypmod AS type_modifier,
        COALESCE(c.collname, 'default') AS collation
      FROM pg_attribute a
      LEFT JOIN pg_attrdef ad ON (a.attrelid = ad.adrelid AND a.attnum = ad.adnum)
      LEFT JOIN pg_type t ON a.atttypid = t.oid
      LEFT JOIN pg_collation c ON a.attcollation = c.oid
      WHERE a.attrelid = $1 
        AND a.attnum > 0 
        AND NOT a.attisdropped
      ORDER BY a.attnum
    `;

    try {
      const result = await this.connection.query(query, [tableOid]);

      for (const row of result.rows) {
        const columnInfo: ColumnInfo = {
          name: row.attname,
          type: row.data_type,
          nullable: !row.is_not_null,
          default: row.column_default,
          comment: row.column_comment,
          isIdentity: row.is_identity
        };

        this.extractTypeDetails(columnInfo, row.data_type, row.type_modifier);
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
   * Extract additional type details from PostgreSQL type information
   */
  private extractTypeDetails(column: ColumnInfo, dataType: string, typeModifier: number): void {
    if (dataType.startsWith('character') || dataType.startsWith('varchar')) {
      const match = dataType.match(/\((\d+)\)/);
      if (match) {
        column.length = parseInt(match[1]);
      }
    }
    else if (dataType.startsWith('numeric') || dataType.startsWith('decimal')) {
      const match = dataType.match(/\((\d+),(\d+)\)/);
      if (match) {
        column.precision = parseInt(match[1]);
        column.scale = parseInt(match[2]);
      }
    }
    else if (dataType.includes('timestamp')) {
      const match = dataType.match(/\((\d+)\)/);
      if (match) {
        column.precision = parseInt(match[1]);
      }
    }

    if (typeModifier > -1) {
      if (dataType === 'varchar' || dataType === 'char' || dataType.startsWith('character')) {
        column.length = typeModifier - 4;
      }
    }
  }

  /**
   * Enrich table with additional metadata
   */
  private async enrichTableMetadata(table: TableInfo, tableOid: number): Promise<void> {
    try {
      const indexQuery = `
        SELECT 
          COUNT(*) as index_count,
          string_agg(i.indexname, ', ') as index_names
        FROM pg_indexes i
        WHERE i.schemaname = $1 AND i.tablename = $2
      `;
      await this.connection.query(indexQuery, [table.schemaname, table.tablename]);

      const constraintQuery = `
        SELECT 
          conname as constraint_name,
          contype as constraint_type
        FROM pg_constraint
        WHERE conrelid = $1
        ORDER BY contype, conname
      `;
      await this.connection.query(constraintQuery, [tableOid]);
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

    const query = `
      SELECT 
        version() as version,
        current_database() as name,
        pg_encoding_to_char(encoding) as encoding,
        datcollate as collation
      FROM pg_database 
      WHERE datname = current_database()
    `;

    try {
      const result = await this.connection.query(query);
      return result.rows[0];
    } catch (error) {
      Logger.error('failed to get database info: %s', getErrorMessage(error));
      throw error;
    }
  }

  /**
   * Get the connection instance for external management
   */
  getConnection(): PostgreSQLConnection {
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
   * Utility method to convert table list to array 
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
   * Generate standardized table metadata for DatabaseMetadataWizard
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
  PostgreSQLSchemaInspector,
  PostgreSQLConnection,
  TableInfo,
  Logger,
  PostgreSQLConnectionError,
  PostgreSQLQueryError,
  getErrorMessage,
  isError
};

export type {
  ColumnInfo,
  DatabaseConfig,
  QueryResult
};

// Default export for convenience
export default PostgreSQLSchemaInspector;