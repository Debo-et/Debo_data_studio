/**
 * VectorWise Schema Inspector with Query Execution
 * Comprehensive schema inspection adapted for Actian Vector (VectorWise) databases.
 * Preserves the design pattern and feature set of the PostgreSQL inspector while
 * using VectorWise‑specific system catalogs and wire‑protocol connectivity.
 * TypeScript implementation optimized for DatabaseMetadataWizard integration.
 */

import { Pool, PoolConfig } from 'pg';

// ---------- Types and interfaces (same as PostgreSQL inspector) ----------
interface DatabaseConfig {
  dbname: string;
  pghost?: string;
  pgport?: string;
  pguser?: string;
  password?: string;
  schema?: string;
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
  fields?: Array<{ name: string; type: string }>;
  executionTime?: number;
  error?: string;
  affectedRows?: number;
  command?: string;
}

// ---------- Utilities ----------
function getErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  } else if (typeof error === 'string') {
    return error;
  }
  return 'Unknown error occurred';
}

function isError(error: unknown): error is Error {
  return error instanceof Error;
}

// ---------- Logger (identical to PSQL version) ----------
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

// ---------- Error classes ----------
class VectorWiseConnectionError extends Error {
  constructor(
    message: string,
    public code?: string,
    public originalError?: unknown
  ) {
    super(message);
    this.name = 'VectorWiseConnectionError';
  }
}

class VectorWiseQueryError extends Error {
  constructor(
    message: string,
    public sql: string,
    public parameters: any[] = [],
    public originalError?: unknown
  ) {
    super(message);
    this.name = 'VectorWiseQueryError';
  }
}

// ---------- Connection Manager (pg‑based, adapted for VectorWise) ----------
class VectorWiseConnection {
  private pool: Pool | null = null;
  private isConnected: boolean = false;
  private readonly maxRetries: number = 3;

  constructor(private config: DatabaseConfig) {}

  async connect(): Promise<void> {
    if (this.isConnected && this.pool) {
      Logger.debug('connection already established');
      return;
    }

    // Default port for Actian Vector wire‑protocol is typically 27832
    const poolConfig: PoolConfig = {
      database: this.config.dbname,
      host: this.config.pghost || 'localhost',
      port: this.config.pgport ? parseInt(this.config.pgport) : 27832,
      user: this.config.pguser,
      password: this.config.password,
      application_name: 'vectorwise_inspector',
      connectionTimeoutMillis: 15000,
      query_timeout: 30000,
      idle_in_transaction_session_timeout: 60000,
      max: 10,
      idleTimeoutMillis: 300000,
      keepAlive: true,
      keepAliveInitialDelayMillis: 30000,
      ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
    };

    let lastError: unknown = null;

    for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
      try {
        Logger.debug(`connection attempt %d/%d`, attempt, this.maxRetries);
        this.pool = new Pool(poolConfig);

        this.pool.on('error', (err) => {
          Logger.error(`Unexpected error on idle client: %s`, getErrorMessage(err));
        });

        const client = await this.pool.connect();
        try {
          const errorHandler = (err: Error) => {
            Logger.error(`Client connection error during test: ${err.message}`);
            client.release(err);
          };
          client.once('error', errorHandler);
          await client.query('SELECT 1 AS connection_test');
          client.off('error', errorHandler);
        } finally {
          client.release();
        }

        this.isConnected = true;
        Logger.info(`successfully connected to database "%s" (attempt %d)`, this.config.dbname, attempt);
        return;
      } catch (error) {
        lastError = error;
        this.isConnected = false;
        if (this.pool) {
          await this.pool.end().catch(() => {});
          this.pool = null;
        }
        Logger.warn(`connection attempt %d failed: %s`, attempt, getErrorMessage(error));
        if (attempt < this.maxRetries) {
          const backoffTime = Math.min(1000 * Math.pow(2, attempt - 1), 10000);
          Logger.debug(`waiting %d ms before retry`, backoffTime);
          await new Promise(resolve => setTimeout(resolve, backoffTime));
        }
      }
    }

    throw new VectorWiseConnectionError(
      `Failed to connect to database "${this.config.dbname}" after ${this.maxRetries} attempts: ${getErrorMessage(lastError)}`,
      'ECONNFAILED',
      lastError
    );
  }

  async checkHealth(): Promise<boolean> {
    if (!this.isConnected || !this.pool) return false;
    try {
      const client = await this.pool.connect();
      try {
        const errorHandler = (err: Error) => {
          Logger.error(`Client connection error during health check: ${err.message}`);
          client.release(err);
        };
        client.once('error', errorHandler);
        const result = await client.query('SELECT 1 AS health_check');
        client.off('error', errorHandler);
        return result.rows.length > 0 && result.rows[0].health_check === 1;
      } finally {
        client.release();
      }
    } catch (error) {
      this.isConnected = false;
      Logger.error(`connection health check failed: %s`, getErrorMessage(error));
      return false;
    }
  }

  get connectionStatus(): { connected: boolean; healthy?: boolean } {
    return {
      connected: this.isConnected,
      healthy: this.isConnected ? undefined : false
    };
  }

  async query(sql: string, params?: any[]): Promise<any> {
    const safeParams = params ?? [];
    this.validateQueryParameters(sql, safeParams);

    if (!this.isConnected || !this.pool) {
      throw new VectorWiseConnectionError('Database not connected');
    }

    const client = await this.pool.connect();
    const errorHandler = (err: Error) => {
      Logger.error(`Client connection error during query: ${err.message}`);
      client.release(err);
    };
    client.once('error', errorHandler);

    try {
      Logger.debug('executing query: %s with %d parameters',
        sql.substring(0, 100) + (sql.length > 100 ? '...' : ''),
        safeParams.length);

      const startTime = Date.now();
      const result = await client.query(sql, safeParams);
      const duration = Date.now() - startTime;

      Logger.debug('query completed in %d ms, %d rows returned', duration, result.rows.length);
      return result;
    } catch (error) {
      if (this.isConnectionError(error)) {
        this.isConnected = false;
        Logger.warn('connection lost during query');
      }

      const queryError = new VectorWiseQueryError(
        `Query execution failed: ${getErrorMessage(error)}`,
        sql,
        safeParams,
        error
      );
      Logger.error('query failed: %s', getErrorMessage(error));
      Logger.debug('failed query: %s', sql);
      Logger.debug('parameters: %o', safeParams);
      throw queryError;
    } finally {
      client.off('error', errorHandler);
      if (!(client as any).releaseCalled) {
        client.release();
      }
    }
  }

  private validateQueryParameters(sql: string, params?: any[]): void {
    if (!sql || typeof sql !== 'string') {
      throw new VectorWiseQueryError('SQL query must be a non-empty string', sql, params);
    }
    const safeParams = params ?? [];
    safeParams.forEach((param, index) => {
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
    if (placeholderCount !== safeParams.length) {
      Logger.warn('parameter count mismatch: %d placeholders, %d parameters',
        placeholderCount, safeParams.length);
    }
  }

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

  getPool(): Pool | null {
    return this.pool;
  }

  getConfig(): Omit<DatabaseConfig, 'password'> {
    const { password, ...safeConfig } = this.config;
    return safeConfig;
  }
}

// ---------- VectorWise Schema Inspector ----------
class VectorWiseSchemaInspector {
  private connection: VectorWiseConnection;
  private schema: string = 'public';

  constructor(config: DatabaseConfig) {
    this.connection = new VectorWiseConnection(config);
    // VectorWise does not have the concept of a “public” schema by default;
    // most installations use the schema equal to the database name or the user.
    // We default to 'public' but can be overridden. A realistic default might be
    // the database name, but we keep compatibility with the PSQL interface.
    this.schema = config.schema || 'public';
  }

  static setLogLevel(level: 'DEBUG' | 'INFO' | 'WARN' | 'ERROR'): void {
    Logger.setLogLevel(level);
  }

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

  async testConnection(): Promise<{ success: boolean; version?: string; error?: string }> {
    try {
      await this.connection.connect();
      // Could use VECTORWISE_VERSION() if available, fallback to VERSION()
      const result = await this.connection.query(
        'SELECT VERSION() AS version, CURRENT_DATABASE() AS database'
      );
      if (result.rows && result.rows.length > 0) {
        const version = result.rows[0].version;
        const database = result.rows[0].database;
        Logger.info('connection test successful - database: %s, version: %s',
          database, version.split(',')[0]);
        return { success: true, version: version };
      }
      return { success: false, error: 'No data returned from version query' };
    } catch (error) {
      const errorMessage = getErrorMessage(error);
      Logger.error('connection test failed: %s', errorMessage);
      return { success: false, error: errorMessage };
    } finally {
      await this.connection.disconnect();
    }
  }

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

  async executeQueryAndDisconnect(sql: string, params: any[] = []): Promise<QueryResult> {
    return this.executeQuery(sql, params, { autoDisconnect: true });
  }

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

  // ----------------------------------------------------------------
  // Schema inspection – VectorWise‑specific system catalog queries
  // ----------------------------------------------------------------

  /**
   * Retrieves all tables from the database.
   * Uses information_schema + optional VectorWise internal catalog
   * for row count and size estimation.
   */
  async getTables(): Promise<TableInfo[]> {
    await this.connection.connect();

    // Attempt to use a VectorWise internal view if it exists; otherwise fall back
    // to a simpler information_schema query.
    const query = `
      SELECT
        t.table_schema AS schemaname,
        t.table_name  AS tablename,
        t.table_type  AS tabletype,
        NULL          AS comment,
        NULL          AS size,
        NULL          AS cardinality
      FROM information_schema.tables t
      WHERE t.table_schema = $1
        AND t.table_type IN ('BASE TABLE', 'VIEW')
      ORDER BY t.table_name
    `;

    try {
      const result = await this.connection.query(query, [this.schema]);
      Logger.info('found %d tables in schema "%s"', result.rows.length, this.schema);
      const tables: TableInfo[] = [];

      for (const row of result.rows) {
        const table = new TableInfo(row.schemaname, row.tablename);
        // Normalize table type
        if (row.tabletype === 'VIEW') {
          table.tabletype = 'view';
        } else {
          table.tabletype = 'table';
        }

        // Enrich with row count and size using VectorWise internal catalog (if available)
        await this.enrichTableCardinality(table);

        // Retrieve columns
        await this.getTableColumns(table);

        tables.push(table);
      }

      return tables;
    } catch (error) {
      Logger.error('Failed to retrieve tables: %s', getErrorMessage(error));
      throw error;
    }
  }

  /**
   * Enrich table with row count and size using VECTORWISE.II_TABLE
   * or fallback to a cheap estimate.
   */
  private async enrichTableCardinality(table: TableInfo): Promise<void> {
    try {
      const result = await this.connection.query(
        `SELECT cardinality, total_size
         FROM VECTORWISE.II_TABLE
         WHERE table_name = $1 AND table_owner = $2`,
        [table.tablename, this.schema]
      );

      if (result.rows.length > 0) {
        table.rowCount = result.rows[0].cardinality;
        table.size = result.rows[0].total_size;
      }
    } catch {
      // Fallback: use COUNT(*) estimate (lightweight for columnar stores)
      try {
        const countResult = await this.connection.query(
          `SELECT COUNT(*) AS cnt FROM ${this.quoteIdentifier(this.schema)}.${this.quoteIdentifier(table.tablename)}`
        );
        table.rowCount = parseInt(countResult.rows[0].cnt, 10);
      } catch {
        table.rowCount = 0;
      }
      table.size = 'unknown';
    }
  }

  /**
   * Retrieve column metadata using information_schema.columns,
   * with length/precision derived from standard properties.
   */
  private async getTableColumns(table: TableInfo): Promise<void> {
    const query = `
      SELECT
        column_name,
        data_type,
        is_nullable,
        column_default,
        character_maximum_length,
        numeric_precision,
        numeric_scale
        -- VectorWise does not natively store column comments,
        -- so we omit comment/identity.
      FROM information_schema.columns
      WHERE table_schema = $1 AND table_name = $2
      ORDER BY ordinal_position
    `;

    try {
      const result = await this.connection.query(query, [this.schema, table.tablename]);
      Logger.debug('retrieved %d columns for table %s', result.rows.length, table.tablename);

      for (const row of result.rows) {
        const colInfo: ColumnInfo = {
          name: row.column_name,
          type: row.data_type,
          nullable: row.is_nullable === 'YES',
          default: row.column_default,
          comment: undefined,          // not available
          isIdentity: false            // sparse support; leave false
        };

        if (row.character_maximum_length) {
          colInfo.length = parseInt(row.character_maximum_length, 10);
        } else if (row.numeric_precision) {
          colInfo.precision = parseInt(row.numeric_precision, 10);
          if (row.numeric_scale != null) {
            colInfo.scale = parseInt(row.numeric_scale, 10);
          }
        }

        table.columns.push(colInfo);
      }
    } catch (error) {
      Logger.error('column retrieval failed for table %s: %s', table.tablename, getErrorMessage(error));
    }
  }

  /**
   * Get specific table metadata by name.
   */
  async getTable(tableName: string): Promise<TableInfo | null> {
    const allTables = await this.getTables();
    return allTables.find(table => table.tablename === tableName) || null;
  }

  /**
   * Get basic database information.
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
        VERSION()            AS version,
        CURRENT_DATABASE()    AS name,
        'UTF8'                AS encoding,   -- VectorWise always uses UTF‑8 internally
        'en_US.UTF-8'         AS collation   -- default collation; adjust as needed
    `;

    try {
      const result = await this.connection.query(query);
      return result.rows[0];
    } catch (error) {
      Logger.error('failed to get database info: %s', getErrorMessage(error));
      throw error;
    }
  }

  getConnection(): VectorWiseConnection {
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

  private quoteIdentifier(name: string): string {
    return `"${name.replace(/"/g, '""')}"`;
  }
}

// Export everything – mirrors the PostgreSQL inspector's public API.
export {
  VectorWiseSchemaInspector,
  VectorWiseConnection,
  TableInfo,
  Logger,
  VectorWiseConnectionError,
  VectorWiseQueryError,
  getErrorMessage,
  isError
};

export type {
  ColumnInfo,
  DatabaseConfig,
  QueryResult
};

export default VectorWiseSchemaInspector;