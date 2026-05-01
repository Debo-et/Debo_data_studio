/**
 * SQLite Schema Inspector with Query Execution
 * Comprehensive schema inspection adapted for SQLite databases.
 * Preserves the design pattern and feature set of the PostgreSQL inspector
 * while replacing the pg‑pool with a single SQLite connection.
 */

import * as sqlite3 from 'sqlite3';

// ---------- Types and interfaces (same as PostgreSQL inspector) ----------
interface DatabaseConfig {
  dbname: string;                // Path to the SQLite database file (or ':memory:')
  pghost?: string;               // Ignored for SQLite (kept for interface compatibility)
  pgport?: string;               // Ignored
  pguser?: string;               // Ignored
  password?: string;             // Ignored (SQLite does not need authentication)
  schema?: string;               // SQLite does not have schemas; kept for API consistency
}

interface ColumnInfo {
  name: string;
  type: string;
  nullable?: boolean;
  default?: string;
  comment?: string;              // SQLite does not support column comments natively
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
class SQLiteConnectionError extends Error {
  constructor(
    message: string,
    public code?: string,
    public originalError?: unknown
  ) {
    super(message);
    this.name = 'SQLiteConnectionError';
  }
}

class SQLiteQueryError extends Error {
  constructor(
    message: string,
    public sql: string,
    public parameters: any[] = [],
    public originalError?: unknown
  ) {
    super(message);
    this.name = 'SQLiteQueryError';
  }
}

// ---------- SQLite Connection Manager (single connection, no pool) ----------
class SQLiteConnection {
  private db: sqlite3.Database | null = null;
  private isConnected: boolean = false;
  private readonly maxRetries: number = 3;

  constructor(private config: DatabaseConfig) {}

  /**
   * Opens a connection to the SQLite database (single file).
   */
  async connect(): Promise<void> {
    if (this.isConnected && this.db) {
      Logger.debug('connection already established');
      return;
    }

    let lastError: unknown = null;

    for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
      try {
        Logger.debug(`connection attempt %d/%d`, attempt, this.maxRetries);

        this.db = new sqlite3.Database(
          this.config.dbname,
          sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE,
          (err) => {
            if (err) throw err;
          }
        );

        // Test the connection with a simple query
        await this.runQuery('SELECT 1 AS connection_test', []);

        this.isConnected = true;
        Logger.info(`successfully connected to database "%s" (attempt %d)`, this.config.dbname, attempt);
        return;
      } catch (error) {
        lastError = error;
        this.isConnected = false;
        // Close the database handle if it was opened
        if (this.db) {
          await this.closeDatabase().catch(() => {});
          this.db = null;
        }
        Logger.warn(`connection attempt %d failed: %s`, attempt, getErrorMessage(error));
        if (attempt < this.maxRetries) {
          const backoffTime = Math.min(1000 * Math.pow(2, attempt - 1), 10000);
          Logger.debug(`waiting %d ms before retry`, backoffTime);
          await new Promise(resolve => setTimeout(resolve, backoffTime));
        }
      }
    }

    throw new SQLiteConnectionError(
      `Failed to connect to database "${this.config.dbname}" after ${this.maxRetries} attempts: ${getErrorMessage(lastError)}`,
      'ECONNFAILED',
      lastError
    );
  }

  /**
   * Helper: run a query that returns rows (SELECT) and resolve with a result object.
   */
  private async runQuery(sql: string, params: any[]): Promise<{ rows: any[]; fields: Array<{name:string;type:string}> }> {
    if (!this.db) throw new SQLiteConnectionError('Database not connected');
    return new Promise((resolve, reject) => {
      this.db!.all(sql, params, (err, rows) => {
        if (err) return reject(err);
        // sqlite3 does not provide field metadata for .all; we can try db.each or use db.prepare.
        // We'll extract column names from the first row if available, otherwise empty.
        const fields: Array<{name:string;type:string}> = [];
        if (rows && rows.length > 0) {
          const colNames = Object.keys(rows[0]);
          fields.push(...colNames.map(name => ({ name, type: typeof rows[0][name] })));
        }
        resolve({ rows, fields });
      });
    });
  }

  /**
   * Helper: run a non-SELECT statement and return affected rows.
   */
  private async runStatement(sql: string, params: any[]): Promise<{ changes: number }> {
    if (!this.db) throw new SQLiteConnectionError('Database not connected');
    return new Promise((resolve, reject) => {
      this.db!.run(sql, params, function (err) {
        if (err) return reject(err);
        resolve({ changes: this.changes });
      });
    });
  }

  async checkHealth(): Promise<boolean> {
    if (!this.isConnected || !this.db) return false;
    try {
      await this.runQuery('SELECT 1 AS health_check', []);
      return true;
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
   * Execute a query and return a result object that mimics pg's result shape.
   * For SELECT, returns rows and fields; for other commands, returns affected rows.
   */
  async query(sql: string, params?: any[]): Promise<any> {
    const safeParams = params ?? [];
    this.validateQueryParameters(sql, safeParams);

    if (!this.isConnected || !this.db) {
      throw new SQLiteConnectionError('Database not connected');
    }

    const startTime = Date.now();
    const isSelect = sql.trim().toUpperCase().startsWith('SELECT') ||
                     sql.trim().toUpperCase().startsWith('PRAGMA') ||
                     sql.trim().toUpperCase().startsWith('EXPLAIN') ||
                     sql.trim().toUpperCase().startsWith('WITH');
    try {
      Logger.debug('executing query: %s with %d parameters',
        sql.substring(0, 100) + (sql.length > 100 ? '...' : ''),
        safeParams.length);

      let result: any;
      if (isSelect) {
        const queryResult = await this.runQuery(sql, safeParams);
        result = {
          rows: queryResult.rows,
          rowCount: queryResult.rows.length,
          fields: queryResult.fields,
          command: 'SELECT'
        };
      } else {
        const stmtResult = await this.runStatement(sql, safeParams);
        // Determine command type from the SQL prefix
        const upperSql = sql.trim().toUpperCase();
        let command = 'UNKNOWN';
        if (upperSql.startsWith('INSERT')) command = 'INSERT';
        else if (upperSql.startsWith('UPDATE')) command = 'UPDATE';
        else if (upperSql.startsWith('DELETE')) command = 'DELETE';
        else if (upperSql.startsWith('CREATE')) command = 'CREATE';
        else if (upperSql.startsWith('DROP')) command = 'DROP';
        else if (upperSql.startsWith('ALTER')) command = 'ALTER';
        else if (upperSql.startsWith('BEGIN') || upperSql.startsWith('COMMIT') || upperSql.startsWith('ROLLBACK'))
          command = 'TRANSACTION';

        result = {
          rows: [],
          rowCount: stmtResult.changes,
          fields: [],
          command
        };
      }

      const duration = Date.now() - startTime;
      Logger.debug('query completed in %d ms, %d rows returned/affected', duration, result.rowCount || 0);
      return result;
    } catch (error) {
      const queryError = new SQLiteQueryError(
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

  /**
   * Validate query parameters to prevent SQL injection (basic checks).
   */
  private validateQueryParameters(sql: string, params?: any[]): void {
    if (!sql || typeof sql !== 'string') {
      throw new SQLiteQueryError('SQL query must be a non-empty string', sql, params);
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

    // SQLite uses ? placeholders, not $1. Check that count matches.
    const placeholderCount = (sql.match(/\?/g) || []).length;
    if (placeholderCount !== safeParams.length) {
      Logger.warn('parameter count mismatch: %d placeholders, %d parameters',
        placeholderCount, safeParams.length);
    }
  }

  async disconnect(): Promise<void> {
    if (this.db) {
      await this.closeDatabase();
      this.isConnected = false;
      this.db = null;
      Logger.info('connection closed successfully');
    }
  }

  private async closeDatabase(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      if (this.db) {
        this.db.close((err) => {
          if (err) {
            Logger.error('error closing database: %s', getErrorMessage(err));
          }
          resolve();
        });
      } else {
        resolve();
      }
    });
  }

  /**
   * For transactions we need to reuse the same connection. Provide a method
   * to get the raw db object (not recommended externally but used by inspector).
   */
  getDatabase(): sqlite3.Database | null {
    return this.db;
  }

  /**
   * Provide a connection pool interface (returns self) for consistency.
   */
  getPool(): SQLiteConnection {
    return this;
  }

  getConfig(): Omit<DatabaseConfig, 'password'> {
    const { password, ...safeConfig } = this.config;
    return safeConfig;
  }
}

// ---------- SQLite Schema Inspector ----------
class SQLiteSchemaInspector {
  private connection: SQLiteConnection;
  private schema: string = 'main';   // SQLite uses 'main' as default schema; kept for API compatibility

  constructor(config: DatabaseConfig) {
    this.connection = new SQLiteConnection(config);
    this.schema = config.schema || 'main';
  }

  static setLogLevel(level: 'DEBUG' | 'INFO' | 'WARN' | 'ERROR'): void {
    Logger.setLogLevel(level);
  }

  static validateConnectionConfig(config: DatabaseConfig): string[] {
    const errors: string[] = [];
    if (!config.dbname || config.dbname.trim() === '') {
      errors.push('Database path (dbname) is required');
    }
    // Host/port/etc. not used, schema optional.
    return errors;
  }

  async testConnection(): Promise<{ success: boolean; version?: string; error?: string }> {
    try {
      await this.connection.connect();
      const result = await this.connection.query('SELECT sqlite_version() AS version');
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
      if (maxRows && finalSql.trim().toUpperCase().startsWith('SELECT') && !finalSql.toUpperCase().includes('LIMIT')) {
        finalSql = `${finalSql} LIMIT ${maxRows}`;
      }

      const result = await this.connection.query(finalSql, params);
      const executionTime = Date.now() - startTime;

      const queryResult: QueryResult = {
        success: true,
        rows: result.rows,
        rowCount: result.rowCount,
        fields: result.fields || [],
        executionTime,
        affectedRows: result.rowCount ?? undefined,
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

  /**
   * SQLite supports transactions on the same connection.
   */
  async executeTransaction(queries: Array<{ sql: string; params?: any[] }>): Promise<QueryResult[]> {
    await this.connection.connect();
    const db = this.connection.getDatabase();
    if (!db) throw new SQLiteConnectionError('Database not connected');

    // Begin transaction
    await this.runRawQuery(db, 'BEGIN TRANSACTION', []);

    const results: QueryResult[] = [];
    try {
      for (let i = 0; i < queries.length; i++) {
        const { sql, params = [] } = queries[i];
        Logger.debug('executing transaction query %d/%d', i + 1, queries.length);
        const startTime = Date.now();
        const result = await this.runRawQuery(db, sql, params);
        results.push({
          success: true,
          rows: result.rows,
          rowCount: result.rowCount,
          fields: result.fields,
          executionTime: Date.now() - startTime,
          affectedRows: result.rowCount ?? undefined,
          command: result.command
        });
      }
      await this.runRawQuery(db, 'COMMIT', []);
      Logger.info('transaction completed successfully');
      return results;
    } catch (error) {
      try {
        await this.runRawQuery(db, 'ROLLBACK', []);
      } catch (rollbackError) {
        Logger.warn('rollback failed: %s', getErrorMessage(rollbackError));
      }
      throw error;
    }
  }

  /**
   * Low‑level query execution on the raw database object.
   */
  private async runRawQuery(
    db: sqlite3.Database,
    sql: string,
    params: any[]
  ): Promise<any> {
    const isSelect = sql.trim().toUpperCase().startsWith('SELECT') ||
                     sql.trim().toUpperCase().startsWith('PRAGMA') ||
                     sql.trim().toUpperCase().startsWith('EXPLAIN') ||
                     sql.trim().toUpperCase().startsWith('WITH');
    return new Promise((resolve, reject) => {
      if (isSelect) {
        db.all(sql, params, (err, rows) => {
          if (err) return reject(err);
          const fields: Array<{name:string;type:string}> = [];
          if (rows && rows.length > 0) {
            const colNames = Object.keys(rows[0]);
            fields.push(...colNames.map(name => ({ name, type: typeof rows[0][name] })));
          }
          resolve({ rows, rowCount: rows.length, fields, command: 'SELECT' });
        });
      } else {
        db.run(sql, params, function(err) {
          if (err) return reject(err);
          let command = 'UNKNOWN';
          const upperSql = sql.trim().toUpperCase();
          if (upperSql.startsWith('INSERT')) command = 'INSERT';
          else if (upperSql.startsWith('UPDATE')) command = 'UPDATE';
          else if (upperSql.startsWith('DELETE')) command = 'DELETE';
          resolve({ rows: [], rowCount: this.changes, fields: [], command });
        });
      }
    });
  }

  /**
   * Retrieves all user tables (excluding sqlite_ internal tables).
   */
  async getTables(): Promise<TableInfo[]> {
    await this.connection.connect();

    const query = `
      SELECT name FROM sqlite_master
      WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
      ORDER BY name
    `;

    try {
      const result = await this.connection.query(query, []);
      Logger.info('found %d tables', result.rows.length);
      const tables: TableInfo[] = [];

      for (const row of result.rows) {
        const tableName = row.name;
        const table = new TableInfo(this.schema, tableName);
        table.tabletype = 'table';

        // Retrieve columns
        const columns = await this.getTableColumns(tableName);
        table.columns = columns;

        // Row count (optional, can be expensive on very large tables)
        try {
          const countResult = await this.connection.query(`SELECT COUNT(*) AS cnt FROM "${tableName}"`, []);
          table.rowCount = countResult.rows[0]?.cnt ?? 0;
        } catch (e) {
          table.rowCount = 0;
        }

        // SQLite does not provide table size or comments
        table.size = undefined;
        table.comment = undefined;

        tables.push(table);
      }

      return tables;
    } catch (error) {
      Logger.error('Failed to retrieve tables: %s', getErrorMessage(error));
      throw error;
    }
  }

  /**
   * Get column metadata using PRAGMA table_info().
   */
  private async getTableColumns(tableName: string): Promise<ColumnInfo[]> {
    const pragmaResult = await this.connection.query(`PRAGMA table_info("${tableName}")`, []);
    const columns: ColumnInfo[] = [];

    for (const row of pragmaResult.rows) {
      // row: cid, name, type, notnull, dflt_value, pk
      const col: ColumnInfo = {
        name: row.name,
        type: row.type || 'BLOB',   // SQLite types are flexible
        nullable: row.notnull === 0,
        default: row.dflt_value,
        comment: undefined,
        isIdentity: row.pk === 1 ? true : false, // primary key as identity
        length: undefined,
        precision: undefined,
        scale: undefined
      };

      // Try to extract length/precision from type, e.g., "VARCHAR(255)" or "NUMERIC(10,2)"
      if (col.type) {
        const match = col.type.match(/\((\d+)(?:,(\d+))?\)/);
        if (match) {
          if (match[2] !== undefined) {
            col.precision = parseInt(match[1], 10);
            col.scale = parseInt(match[2], 10);
          } else {
            col.length = parseInt(match[1], 10);
          }
        }
      }

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
    const versionResult = await this.connection.query('SELECT sqlite_version() AS version', []);
    return {
      version: versionResult.rows[0]?.version || 'unknown',
      name: this.connection.getConfig().dbname,
      encoding: 'UTF-8',
      collation: 'BINARY'
    };
  }

  getConnection(): SQLiteConnection {
    return this.connection;
  }

  getCurrentSchema(): string {
    return this.schema;
  }

  setSchema(schema: string): void {
    this.schema = schema;  // For SQLite, 'main' is the usual schema
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
}

// Export everything (mirrors the PostgreSQL inspector's public API)
export {
  SQLiteSchemaInspector,
  SQLiteConnection,
  TableInfo,
  Logger,
  SQLiteConnectionError,
  SQLiteQueryError,
  getErrorMessage,
  isError
};

export type {
  ColumnInfo,
  DatabaseConfig,
  QueryResult
};

export default SQLiteSchemaInspector;