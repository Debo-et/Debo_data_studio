/**
 * H2 Database Schema Inspector with Query Execution
 * Comprehensive schema inspection adapted for H2 databases using JDBC.
 * Preserves the design pattern and feature set of the PostgreSQL inspector
 * while replacing the pg‑pool with an H2‑compatible JDBC connection pool.
 */

import * as jdbc from 'jdbc';

// ---------- Types and interfaces (same as PostgreSQL inspector) ----------
interface DatabaseConfig {
  dbname: string;                // H2 JDBC URL or database name (e.g., 'jdbc:h2:~/test' or '~/test')
  pghost?: string;               // Not used directly; kept for interface compatibility
  pgport?: string;               // Not used
  pguser?: string;               // Username for H2 (default: 'sa')
  password?: string;             // Password (default: '')
  schema?: string;               // Schema name (default: 'PUBLIC')
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

// ---------- Logger (identical) ----------
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
class H2ConnectionError extends Error {
  constructor(
    message: string,
    public code?: string,
    public originalError?: unknown
  ) {
    super(message);
    this.name = 'H2ConnectionError';
  }
}

class H2QueryError extends Error {
  constructor(
    message: string,
    public sql: string,
    public parameters: any[] = [],
    public originalError?: unknown
  ) {
    super(message);
    this.name = 'H2QueryError';
  }
}

// ---------- H2 JDBConnection Manager (JDBC pool) ----------
class H2Connection {
  private pool: jdbc.Pool | null = null;
  private isConnected: boolean = false;
  private readonly maxRetries: number = 3;
  private jdbcUrl: string;

  constructor(private config: DatabaseConfig) {
    this.jdbcUrl = H2Connection.buildJdbcUrl(config);
  }

  /**
   * Build the JDBC URL from config.
   * If dbname is already a full JDBC URL, use it as is;
   * otherwise, assume it is a file path and prefix with 'jdbc:h2:'.
   */
  private static buildJdbcUrl(config: DatabaseConfig): string {
    const dbname = config.dbname.trim();
    if (dbname.startsWith('jdbc:')) {
      return dbname;
    }
    // Default to embedded file URL
    return `jdbc:h2:${dbname}`;
  }

  async connect(): Promise<void> {
    if (this.isConnected && this.pool) {
      Logger.debug('connection already established');
      return;
    }

    let lastError: unknown = null;

    for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
      try {
        Logger.debug(`connection attempt %d/%d`, attempt, this.maxRetries);

        // Create JDBC pool
        this.pool = new jdbc.Pool({
          url: this.jdbcUrl,
          user: this.config.pguser || 'sa',
          password: this.config.password || '',
          minpoolsize: 1,
          maxpoolsize: 10,
          // other options
        });

        // Test connection
        const conn = await this.getConnectionFromPool();
        try {
          await this.executeOnConnection(conn, 'SELECT 1 AS connection_test', []);
        } finally {
          await this.releaseConnection(conn);
        }

        this.isConnected = true;
        Logger.info(`successfully connected to H2 database "%s" (attempt %d)`, this.config.dbname, attempt);
        return;
      } catch (error) {
        lastError = error;
        this.isConnected = false;
        if (this.pool) {
          await this.closePool().catch(() => {});
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

    throw new H2ConnectionError(
      `Failed to connect to H2 database "${this.config.dbname}" after ${this.maxRetries} attempts: ${getErrorMessage(lastError)}`,
      'ECONNFAILED',
      lastError
    );
  }

  private getConnectionFromPool(): Promise<jdbc.Connection> {
    return new Promise<jdbc.Connection>((resolve, reject) => {
      if (!this.pool) return reject(new Error('Pool not initialized'));
      this.pool.getConnection((err, conn) => {
        if (err) return reject(err);
        resolve(conn);
      });
    });
  }

  private releaseConnection(conn: jdbc.Connection): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      if (!conn) return resolve();
      conn.release((err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  private executeOnConnection(
    conn: jdbc.Connection,
    sql: string,
    params: any[]
  ): Promise<any> {
    return new Promise<any>((resolve, reject) => {
      conn.executeQuery(sql, params, (err, result) => {
        if (err) return reject(err);
        resolve(result);
      });
    });
  }

  async checkHealth(): Promise<boolean> {
    if (!this.isConnected || !this.pool) return false;
    try {
      const conn = await this.getConnectionFromPool();
      try {
        await this.executeOnConnection(conn, 'SELECT 1 AS health_check', []);
        return true;
      } finally {
        await this.releaseConnection(conn);
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

  /**
   * Execute a query and return a normalized result object.
   * Works with both SELECT and DML statements using the JDBC driver.
   */
  async query(sql: string, params?: any[]): Promise<any> {
    const safeParams = params ?? [];
    this.validateQueryParameters(sql, safeParams);

    if (!this.isConnected || !this.pool) {
      throw new H2ConnectionError('Database not connected');
    }

    const conn = await this.getConnectionFromPool();
    try {
      Logger.debug('executing query: %s with %d parameters',
        sql.substring(0, 100) + (sql.length > 100 ? '...' : ''),
        safeParams.length);

      const startTime = Date.now();
      const result = await this.executeOnConnection(conn, sql, safeParams);
      const duration = Date.now() - startTime;

      // Normalize result shape
      const rows = result.data || [];
      const fields = result.metaData
        ? result.metaData.map((col: any) => ({
            name: col.label || col.columnName,
            type: col.columnTypeName || 'unknown'
          }))
        : [];
      const rowCount = rows.length;
      const affectedRows = result.updateCount ?? undefined;
      const command = this.inferCommand(sql);

      Logger.debug('query completed in %d ms, %d rows returned/affected', duration, rowCount || affectedRows || 0);

      return { rows, rowCount, fields, command, affectedRows };
    } catch (error) {
      const queryError = new H2QueryError(
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
      await this.releaseConnection(conn);
    }
  }

  private inferCommand(sql: string): string {
    const upper = sql.trim().toUpperCase();
    if (upper.startsWith('SELECT')) return 'SELECT';
    if (upper.startsWith('INSERT')) return 'INSERT';
    if (upper.startsWith('UPDATE')) return 'UPDATE';
    if (upper.startsWith('DELETE')) return 'DELETE';
    if (upper.startsWith('CREATE')) return 'CREATE';
    if (upper.startsWith('DROP')) return 'DROP';
    if (upper.startsWith('ALTER')) return 'ALTER';
    if (upper.startsWith('BEGIN') || upper.startsWith('COMMIT') || upper.startsWith('ROLLBACK')) return 'TRANSACTION';
    return 'UNKNOWN';
  }

  private validateQueryParameters(sql: string, params?: any[]): void {
    if (!sql || typeof sql !== 'string') {
      throw new H2QueryError('SQL query must be a non-empty string', sql, params);
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

    // JDBC uses ? placeholders; check count
    const placeholderCount = (sql.match(/\?/g) || []).length;
    if (placeholderCount !== safeParams.length) {
      Logger.warn('parameter count mismatch: %d placeholders, %d parameters',
        placeholderCount, safeParams.length);
    }
  }

  async disconnect(): Promise<void> {
    if (this.pool) {
      await this.closePool();
      this.isConnected = false;
      this.pool = null;
      Logger.info('connection pool closed successfully');
    }
  }

  private async closePool(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      if (this.pool) {
        this.pool.close((err) => {
          if (err) {
            Logger.error('error closing pool: %s', getErrorMessage(err));
          }
          resolve();
        });
      } else {
        resolve();
      }
    });
  }

  getPool(): jdbc.Pool | null {
    return this.pool;
  }

  getConfig(): Omit<DatabaseConfig, 'password'> {
    const { password, ...safeConfig } = this.config;
    return safeConfig;
  }
}

// ---------- H2 Schema Inspector ----------
class H2SchemaInspector {
  private connection: H2Connection;
  private schema: string = 'PUBLIC';   // H2 default schema

  constructor(config: DatabaseConfig) {
    this.connection = new H2Connection(config);
    this.schema = (config.schema || 'PUBLIC').toUpperCase();
  }

  static setLogLevel(level: 'DEBUG' | 'INFO' | 'WARN' | 'ERROR'): void {
    Logger.setLogLevel(level);
  }

  static validateConnectionConfig(config: DatabaseConfig): string[] {
    const errors: string[] = [];
    if (!config.dbname || config.dbname.trim() === '') {
      errors.push('Database name/URL is required');
    }
    return errors;
  }

  async testConnection(): Promise<{ success: boolean; version?: string; error?: string }> {
    try {
      await this.connection.connect();
      const result = await this.connection.query('SELECT H2VERSION() AS version FROM DUAL');
      if (result.rows && result.rows.length > 0) {
        return { success: true, version: result.rows[0].version };
      }
      return { success: false, error: 'No version returned' };
    } catch (error) {
      return { success: false, error: getErrorMessage(error) };
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
      if (maxRows && sql.trim().toUpperCase().startsWith('SELECT') && !sql.toUpperCase().includes('LIMIT')) {
        finalSql = `${sql} LIMIT ${maxRows}`;
      }

      const result = await this.connection.query(finalSql, params);
      const executionTime = Date.now() - startTime;

      const queryResult: QueryResult = {
        success: true,
        rows: result.rows,
        rowCount: result.rowCount,
        fields: result.fields,
        executionTime,
        affectedRows: result.affectedRows,
        command: result.command
      };

      Logger.info('query executed successfully in %d ms, returned %d rows', executionTime, result.rowCount || 0);
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

  async executeQueryAndDisconnect(sql: string, params: any[] = []): Promise<QueryResult> {
    return this.executeQuery(sql, params, { autoDisconnect: true });
  }

  async executeTransaction(queries: Array<{ sql: string; params?: any[] }>): Promise<QueryResult[]> {
    await this.connection.connect();
    const pool = this.connection.getPool();
    if (!pool) throw new H2ConnectionError('No connection pool');

    const conn = await this.getConnectionFromPool();
    try {
      await this.executeOnConnection(conn, 'BEGIN TRANSACTION', []);

      const results: QueryResult[] = [];
      for (let i = 0; i < queries.length; i++) {
        const { sql, params = [] } = queries[i];
        Logger.debug('executing transaction query %d/%d', i + 1, queries.length);
        const startTime = Date.now();
        const result = await this.executeOnConnection(conn, sql, params);
        results.push({
          success: true,
          rows: result.data || [],
          fields: result.metaData
            ? result.metaData.map((col: any) => ({
                name: col.label || col.columnName,
                type: col.columnTypeName || 'unknown'
              }))
            : [],
          rowCount: result.data ? result.data.length : 0,
          affectedRows: result.updateCount ?? undefined,
          executionTime: Date.now() - startTime,
          command: this.inferCommand(sql)
        });
      }
      await this.executeOnConnection(conn, 'COMMIT', []);
      Logger.info('transaction completed successfully');
      return results;
    } catch (error) {
      try {
        await this.executeOnConnection(conn, 'ROLLBACK', []);
      } catch (rollbackError) {
        Logger.warn('rollback failed: %s', getErrorMessage(rollbackError));
      }
      throw error;
    } finally {
      await this.releaseConnection(conn);
    }
  }

  private inferCommand(sql: string): string {
    const upper = sql.trim().toUpperCase();
    if (upper.startsWith('SELECT')) return 'SELECT';
    if (upper.startsWith('INSERT')) return 'INSERT';
    if (upper.startsWith('UPDATE')) return 'UPDATE';
    if (upper.startsWith('DELETE')) return 'DELETE';
    if (upper.startsWith('CREATE')) return 'CREATE';
    if (upper.startsWith('DROP')) return 'DROP';
    if (upper.startsWith('ALTER')) return 'ALTER';
    return 'UNKNOWN';
  }

  private getConnectionFromPool(): Promise<jdbc.Connection> {
    return new Promise<jdbc.Connection>((resolve, reject) => {
      const pool = this.connection.getPool();
      if (!pool) return reject(new Error('Pool not available'));
      pool.getConnection((err, conn) => {
        if (err) return reject(err);
        resolve(conn);
      });
    });
  }

  private releaseConnection(conn: jdbc.Connection): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      conn.release((err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  private executeOnConnection(conn: jdbc.Connection, sql: string, params: any[]): Promise<any> {
    return new Promise<any>((resolve, reject) => {
      conn.executeQuery(sql, params, (err, result) => {
        if (err) reject(err);
        else resolve(result);
      });
    });
  }

  /**
   * Retrieves all tables from the specified schema.
   */
  async getTables(): Promise<TableInfo[]> {
    await this.connection.connect();

    const query = `
      SELECT TABLE_NAME, TABLE_TYPE 
      FROM INFORMATION_SCHEMA.TABLES 
      WHERE TABLE_SCHEMA = ?
      AND TABLE_TYPE IN ('TABLE', 'VIEW', 'BASE TABLE')
      ORDER BY TABLE_NAME
    `;

    try {
      const result = await this.connection.query(query, [this.schema]);
      Logger.info('found %d tables in schema "%s"', result.rows.length, this.schema);
      const tables: TableInfo[] = [];

      for (const row of result.rows) {
        const tableName = row.TABLE_NAME;
        const table = new TableInfo(this.schema, tableName);
        if (row.TABLE_TYPE === 'VIEW') {
          table.tabletype = 'view';
        }

        // Retrieve columns
        const columns = await this.getTableColumns(tableName);
        table.columns = columns;

        // Row count estimate
        try {
          const countResult = await this.connection.query(
            `SELECT COUNT(*) AS cnt FROM "${this.schema}"."${tableName}"`,
            []
          );
          table.rowCount = countResult.rows[0]?.cnt ?? 0;
        } catch (e) {
          table.rowCount = 0;
        }

        // Table comment (H2 supports comments)
        try {
          const commentResult = await this.connection.query(
            `SELECT REMARKS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?`,
            [this.schema, tableName]
          );
          table.comment = commentResult.rows?.[0]?.REMARKS || undefined;
        } catch (e) {
          // ignore
        }

        tables.push(table);
      }

      return tables;
    } catch (error) {
      Logger.error('Failed to retrieve tables: %s', getErrorMessage(error));
      throw error;
    }
  }

  private async getTableColumns(tableName: string): Promise<ColumnInfo[]> {
    const query = `
      SELECT 
        COLUMN_NAME, 
        TYPE_NAME, 
        IS_NULLABLE, 
        COLUMN_DEFAULT, 
        REMARKS,
        IS_IDENTITY,
        CHARACTER_MAXIMUM_LENGTH,
        NUMERIC_PRECISION,
        NUMERIC_SCALE
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
      ORDER BY ORDINAL_POSITION
    `;

    const result = await this.connection.query(query, [this.schema, tableName]);
    const columns: ColumnInfo[] = [];

    for (const row of result.rows) {
      const col: ColumnInfo = {
        name: row.COLUMN_NAME,
        type: row.TYPE_NAME || 'VARCHAR',
        nullable: row.IS_NULLABLE === 'YES',
        default: row.COLUMN_DEFAULT,
        comment: row.REMARKS || undefined,
        isIdentity: row.IS_IDENTITY === 'YES',
        length: row.CHARACTER_MAXIMUM_LENGTH ?? undefined,
        precision: row.NUMERIC_PRECISION ?? undefined,
        scale: row.NUMERIC_SCALE ?? undefined
      };

      columns.push(col);
    }

    return columns;
  }

  async getTable(tableName: string): Promise<TableInfo | null> {
    const allTables = await this.getTables();
    return allTables.find(table => table.tablename === tableName) || null;
  }

  async getDatabaseInfo(): Promise<{
    version: string;
    name: string;
    encoding: string;
    collation: string;
  }> {
    await this.connection.connect();
    const versionResult = await this.connection.query(
      'SELECT H2VERSION() AS version, DATABASE() AS name FROM DUAL'
    );
    return {
      version: versionResult.rows[0]?.version || 'unknown',
      name: versionResult.rows[0]?.name || 'unknown',
      encoding: 'UTF-8',
      collation: 'default'
    };
  }

  getConnection(): H2Connection {
    return this.connection;
  }

  getCurrentSchema(): string {
    return this.schema;
  }

  setSchema(schema: string): void {
    this.schema = schema.toUpperCase();
    Logger.debug('schema set to: %s', this.schema);
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
}

// ---------- Exports ----------
export {
  H2SchemaInspector,
  H2Connection,
  TableInfo,
  Logger,
  H2ConnectionError,
  H2QueryError,
  getErrorMessage,
  isError
};

export type {
  ColumnInfo,
  DatabaseConfig,
  QueryResult
};

export default H2SchemaInspector;