/**
 * Microsoft Access Schema Inspector with Query Execution
 * Comprehensive schema inspection adapted for Access databases via ODBC.
 * Preserves the design pattern and feature set of the PostgreSQL inspector while
 * replacing the pg‑pool with ODBC connections. Designed as a drop‑in component.
 */

import * as odbc from 'odbc';

// ---------- Types and interfaces (consistent with the PostgreSQL inspector) ----------
interface DatabaseConfig {
  dbname: string;                // For Access this is the full path to the .mdb/.accdb file
  pghost?: string;               // Ignored for Access (kept for interface compatibility)
  pgport?: string;               // Ignored
  pguser?: string;               // Optional user name (for secured databases)
  password?: string;             // Optional password
  schema?: string;               // Access does not have schemas; keep for API consistency
}

interface ColumnInfo {
  name: string;
  type: string;
  nullable?: boolean;
  default?: string;
  comment?: string;              // Not natively available in Access
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
class AccessConnectionError extends Error {
  constructor(
    message: string,
    public code?: string,
    public originalError?: unknown
  ) {
    super(message);
    this.name = 'AccessConnectionError';
  }
}

class AccessQueryError extends Error {
  constructor(
    message: string,
    public sql: string,
    public parameters: any[] = [],
    public originalError?: unknown
  ) {
    super(message);
    this.name = 'AccessQueryError';
  }
}

// ----------------------------------------------------------------------
// AccessConnection: ODBC‑based connection manager (pool‑like for safety)
// ----------------------------------------------------------------------
type OdbcPool = odbc.Pool;

class AccessConnection {
  private pool: OdbcPool | null = null;
  private isConnected: boolean = false;
  private readonly maxRetries: number = 3;
  private connectionString: string;

  constructor(private config: DatabaseConfig) {
    this.connectionString = AccessConnection.buildConnectionString(config);
  }

  private static buildConnectionString(config: DatabaseConfig): string {
    // Default ODBC driver string for Access (adjust for your environment)
    const driver = process.env.ACCESS_ODBC_DRIVER || 'Microsoft Access Driver (*.mdb, *.accdb)';
    let connStr = `DRIVER={${driver}};DBQ=${config.dbname};`;
    if (config.pguser) connStr += `UID=${config.pguser};`;
    if (config.password) connStr += `PWD=${config.password};`;
    // Use Exclusive=Yes to avoid locking issues from multiple pool connections? Optional.
    connStr += 'Exclusive=Yes;';
    connStr += 'ExtendedAnsiSQL=1;';  // enables some SQL-92 features
    return connStr;
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

      // odbc.pool returns a Promise<Pool> → await it
      this.pool = await odbc.pool(this.connectionString);

      // Test the connection by acquiring a connection and executing a simple query
      const connection = await this.getConnectionFromPool();
      try {
        await new Promise<void>((resolve, reject) => {
          connection.query('SELECT 1 AS connection_test', (err) => {
            if (err) reject(err);
            else resolve();
          });
        });
      } finally {
        connection.close();
      }

      this.isConnected = true;
      Logger.info(`successfully connected to database "%s" (attempt %d)`, this.config.dbname, attempt);
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

  throw new AccessConnectionError(
    `Failed to connect to database "${this.config.dbname}" after ${this.maxRetries} attempts: ${getErrorMessage(lastError)}`,
    'ECONNFAILED',
    lastError
  );
}

  private getConnectionFromPool(): Promise<odbc.Connection> {
    if (!this.pool) throw new Error('Pool not initialized');
    return new Promise<odbc.Connection>((resolve, reject) => {
      this.pool!.connect((err, connection) => {
        if (err) reject(err);
        else resolve(connection);
        return undefined; // explicitly return undefined to satisfy type
      });
    });
  }

  async checkHealth(): Promise<boolean> {
    if (!this.isConnected || !this.pool) return false;
    try {
      const connection = await this.getConnectionFromPool();
      try {
        await new Promise<void>((resolve, reject) => {
          connection.query('SELECT 1 AS health_check', (err) => {
            if (err) reject(err);
            else resolve();
          });
        });
        return true;
      } finally {
        connection.close();
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

  // Wrapper around ODBC query that returns a pg‑like result object
  async query(sql: string, params?: any[]): Promise<any> {
    const safeParams = params ?? [];
    this.validateQueryParameters(sql, safeParams);

    if (!this.isConnected || !this.pool) {
      throw new AccessConnectionError('Database not connected');
    }

    const connection = await this.getConnectionFromPool();
    try {
      Logger.debug('executing query: %s with %d parameters',
        sql.substring(0, 100) + (sql.length > 100 ? '...' : ''),
        safeParams.length);

      const startTime = Date.now();
      // Cast the result to odbc.Result<unknown> because for our queries we always expect a Result, not a Cursor.
      const result: odbc.Result<unknown> = await new Promise<odbc.Result<unknown>>((resolve, reject) => {
        connection.query(sql, safeParams, (err, result) => {
          if (err) reject(err);
          else resolve(result as odbc.Result<unknown>);
        });
      });
      const duration = Date.now() - startTime;

      // Normalise result shape to match pg's result
      const normalised = {
        rows: result as any[],
        rowCount: result.length,
        fields: result.columns
          ? result.columns.map((col: any) => ({
              name: col.name,
              type: col.dataTypeName || 'unknown'
            }))
          : []
      };

      Logger.debug('query completed in %d ms, %d rows returned', duration, normalised.rows.length);
      return normalised;
    } catch (error) {
      if (this.isConnectionError(error)) {
        this.isConnected = false;
        Logger.warn('connection lost during query');
      }

      const queryError = new AccessQueryError(
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
      connection.close();
    }
  }

  private validateQueryParameters(sql: string, params?: any[]): void {
    if (!sql || typeof sql !== 'string') {
      throw new AccessQueryError('SQL query must be a non-empty string', sql, params);
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
    // Access uses ? placeholders, not $1. We'll check for ? count.
    const placeholderCount = (sql.match(/\?/g) || []).length;
    if (placeholderCount !== safeParams.length) {
      Logger.warn('parameter count mismatch: %d placeholders, %d parameters', placeholderCount, safeParams.length);
    }
  }

  private isConnectionError(error: unknown): boolean {
    const errorMessage = getErrorMessage(error).toLowerCase();
    const connectionErrors = [
      'ECONNRESET', 'ECONNREFUSED', 'EPIPE', 'ETIMEDOUT',
      'connection', 'connect', 'socket', 'network', 'terminated',
      'could not find installable isam', 'not a valid file name',
      'could not lock file', 'unrecognized database format'
    ];
    return connectionErrors.some(connError => errorMessage.includes(connError.toLowerCase()));
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
    return new Promise<void>((resolve) => {
      if (this.pool) {
        this.pool.close(() => {
          resolve();
        });
        this.pool = null;
      } else {
        resolve();
      }
    });
  }

  getPool(): OdbcPool | null {
    return this.pool;
  }

  getConfig(): Omit<DatabaseConfig, 'password'> {
    const { password, ...safeConfig } = this.config;
    return safeConfig;
  }
}

// ----------------------------------------------------------------------
// AccessSchemaInspector: tables/columns via ODBC metadata functions
// ----------------------------------------------------------------------
class AccessSchemaInspector {
  private connection: AccessConnection;
  private schema: string = 'public';   // Access has no schemas; kept for compatibility

  constructor(config: DatabaseConfig) {
    this.connection = new AccessConnection(config);
    this.schema = config.schema || 'public';
  }

  static setLogLevel(level: 'DEBUG' | 'INFO' | 'WARN' | 'ERROR'): void {
    Logger.setLogLevel(level);
  }

  static validateConnectionConfig(config: DatabaseConfig): string[] {
    const errors: string[] = [];
    if (!config.dbname || config.dbname.trim() === '') {
      errors.push('Database path (dbname) is required');
    }
    // Host and port are ignored, schema is optional and not validated further.
    return errors;
  }

  async testConnection(): Promise<{ success: boolean; version?: string; error?: string }> {
    try {
      await this.connection.connect();
      // Access does not have a VERSION() function. We just confirm the connection.
      // We can return a synthetic version string from the ODBC driver.
      const syntheticVersion = 'Microsoft Access (via ODBC)';
      Logger.info('connection test successful - database: %s', this.connection.getConfig().dbname);
      return { success: true, version: syntheticVersion };
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

      const queryResult: QueryResult = {
        success: true,
        rows: result.rows,
        rowCount: result.rowCount,
        fields: result.fields,
        executionTime,
        affectedRows: result.rowCount ?? undefined,
        command: undefined // ODBC may not provide this
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

  private applyRowLimit(sql: string, maxRows: number): string {
    // Access uses TOP or SELECT TOP n. We'll insert TOP after SELECT.
    const trimmed = sql.trim();
    const upperSql = trimmed.toUpperCase().replace(/\s+/g, ' ');
    if (upperSql.startsWith('SELECT ') && !upperSql.includes(' TOP ')) {
      // Insert TOP after SELECT
      return trimmed.replace(/^SELECT\s+/i, `SELECT TOP ${maxRows} `);
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

    const connection = await this.getConnectionFromPool();
    try {
      // Access supports BEGIN TRANSACTION / COMMIT but we must use the same connection.
      await this.executeOnConnection(connection, 'BEGIN TRANSACTION', []);
      Logger.info('starting transaction with %d queries', queries.length);
      const results: QueryResult[] = [];

      for (let i = 0; i < queries.length; i++) {
        const { sql, params = [] } = queries[i];
        Logger.debug('executing transaction query %d/%d', i + 1, queries.length);
        const startTime = Date.now();
        try {
          const result = await this.executeOnConnection(connection, sql, params);
          results.push({
            success: true,
            rows: result.rows,
            rowCount: result.rowCount,
            executionTime: Date.now() - startTime,
            affectedRows: result.rowCount ?? undefined
          });
        } catch (error) {
          await this.executeOnConnection(connection, 'ROLLBACK', []).catch(() => {});
          Logger.error('transaction failed at query %d: %s', i + 1, getErrorMessage(error));
          throw error;
        }
      }

      await this.executeOnConnection(connection, 'COMMIT', []);
      Logger.info('transaction completed successfully');
      return results;
    } catch (error) {
      try {
        await this.executeOnConnection(connection, 'ROLLBACK', []).catch(() => {});
      } catch (rollbackError) {
        Logger.warn('rollback failed: %s', getErrorMessage(rollbackError));
      }
      throw error;
    } finally {
      connection.close();
    }
  }

  private async executeOnConnection(connection: odbc.Connection, sql: string, params: any[]): Promise<any> {
    return new Promise<any>((resolve, reject) => {
      connection.query(sql, params, (err, result) => {
        if (err) reject(err);
        else resolve({
          rows: result as any[],
          rowCount: (result as odbc.Result<unknown>).length,
          fields: (result as odbc.Result<unknown>).columns
            ? (result as odbc.Result<unknown>).columns.map((col: any) => ({
              name: col.name,
              type: col.dataTypeName || 'unknown'
            }))
            : []
        });
      });
    });
  }

  private getConnectionFromPool(): Promise<odbc.Connection> {
    const pool = this.connection.getPool();
    if (!pool) throw new Error('Pool not available');
    return new Promise<odbc.Connection>((resolve, reject) => {
      pool.connect((err, connection) => {
        if (err) reject(err);
        else resolve(connection);
        return undefined; // satisfy type
      });
    });
  }

  // ----------------------------------------------------------------
  // Schema inspection – uses ODBC catalog functions
  // ----------------------------------------------------------------
  async getTables(): Promise<TableInfo[]> {
    await this.connection.connect();
    const pool = this.connection.getPool();
    if (!pool) throw new Error('No connection pool');

    return new Promise<TableInfo[]>((resolve, reject) => {
      pool.connect((err, conn) => {
        if (err) {
          reject(err);
          return undefined;
        }
        // Use ODBC tables function – callback must not be async and must return undefined.
        conn.tables(null, null, null, 'TABLE', (err, tableData) => {
          if (err) {
            conn.close();
            reject(err);
            return undefined;
          }
          // Process asynchronously inside an IIFE to avoid Promise<void> return.
          (async () => {
            try {
              const tables: TableInfo[] = [];
              for (const row of tableData as any[]) {
                // Filter out system tables (names starting with 'MSys' or '~')
                const tableName = row.TABLE_NAME;
                if (tableName && (tableName.startsWith('MSys') || tableName.startsWith('~'))) continue;

                const table = new TableInfo('', tableName); // schema empty
                if (row.TABLE_TYPE === 'VIEW') table.tabletype = 'view';
                else table.tabletype = 'table';

                // Get columns for this table
                const columns = await this.getColumnsForTable(conn, tableName);
                table.columns = columns;

                // Estimate row count (optional)
                try {
                  const countResult = await new Promise<any>((res, rej) => {
                    conn.query(`SELECT COUNT(*) AS cnt FROM [${tableName}]`, (err, result) => {
                      if (err) rej(err);
                      else res(result);
                    });
                  });
                  table.rowCount = countResult[0]?.cnt ?? 0;
                } catch (countErr) {
                  table.rowCount = 0;
                }

                tables.push(table);
              }
              conn.close();
              resolve(tables);
            } catch (e) {
              conn.close();
              reject(e);
            }
          })();
          return undefined; // satisfy return type
        });
        return undefined;
      });
    });
  }

  private async getColumnsForTable(conn: odbc.Connection, tableName: string): Promise<ColumnInfo[]> {
    return new Promise<ColumnInfo[]>((resolve, _reject) => {
      conn.columns(null, null, tableName, null, (err, colsData) => {
        if (err) {
          // If columns cannot be fetched, return empty array to not break the flow
          resolve([]);
          return undefined;
        }
        const columns: ColumnInfo[] = [];
        for (const col of colsData as any[]) {
          columns.push({
            name: col.COLUMN_NAME,
            type: col.TYPE_NAME,
            nullable: col.NULLABLE === 1 || col.NULLABLE === 'YES' ? true : false,
            default: undefined,           // Not easily retrieved via ODBC columns
            comment: undefined,
            isIdentity: col.AUTO_INCREMENT === 'YES' ? true : false,
            length: col.COLUMN_SIZE || undefined,
            precision: col.NUM_PRECISION || undefined,
            scale: col.NUM_SCALE || undefined
          });
        }
        resolve(columns);
        return undefined;
      });
    });
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
    return {
      version: 'Microsoft Access (ODBC)',
      name: this.connection.getConfig().dbname,
      encoding: 'UTF-8',
      collation: 'General'
    };
  }

  getConnection(): AccessConnection {
    return this.connection;
  }

  getCurrentSchema(): string {
    return this.schema;
  }

  setSchema(schema: string): void {
    this.schema = schema;
    Logger.debug('schema set to: %s (ignored for Access)', schema);
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

// Export everything (mirrors the PostgreSQL inspector's public API)
export {
  AccessSchemaInspector,
  AccessConnection,
  TableInfo,
  Logger,
  AccessConnectionError,
  AccessQueryError,
  getErrorMessage,
  isError
};

export type {
  ColumnInfo,
  DatabaseConfig,
  QueryResult
};

export default AccessSchemaInspector;