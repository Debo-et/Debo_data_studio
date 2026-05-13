/**
 * Enhanced Neo4j Schema Inspector with Query Execution
 * Comprehensive node‑label inspection with robust error handling and connection management
 * TypeScript implementation optimized for DatabaseMetadataWizard integration
 *
 * Neo4j is a graph database. This inspector treats each distinct node label as a
 * “table” and each property key as a “column”. Metadata is gathered using Cypher
 * queries (and optionally APOC for precise property types).
 */

import neo4j, { Driver, Session, Config as Neo4jConfig } from 'neo4j-driver';

// ---------------------------------------------------------------------------
// Re‑use identical type definitions and utilities from the PostgreSQL inspector
// ---------------------------------------------------------------------------

interface DatabaseConfig {
  dbname: string;            // Neo4j database name (default: "neo4j")
  host?: string;
  port?: string;
  user?: string;
  password?: string;
  schema?: string;           // alias for dbname (used as database name)
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
  public tabletype: string = 'node_label';
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
// Neo4j‑specific error classes
// ---------------------------------------------------------------------------
class Neo4jConnectionError extends Error {
  constructor(
    message: string,
    public code?: string,
    public originalError?: unknown
  ) {
    super(message);
    this.name = 'Neo4jConnectionError';
  }
}

class Neo4jQueryError extends Error {
  constructor(
    message: string,
    public sql: string,
    public parameters: any[] = [],
    public originalError?: unknown
  ) {
    super(message);
    this.name = 'Neo4jQueryError';
  }
}

// ---------------------------------------------------------------------------
// Neo4j Connection Manager – wraps the official driver and session pool
// ---------------------------------------------------------------------------
class Neo4jConnection {
  private driver: Driver | null = null;
  private isConnected: boolean = false;
  private readonly maxRetries: number = 3;

  constructor(private config: DatabaseConfig) {}

  /**
   * Initialises the Neo4j driver and verifies connectivity with a simple Cypher query.
   */
  async connect(): Promise<void> {
    if (this.isConnected && this.driver) {
      Logger.debug('connection already established');
      return;
    }

    const boltUri = `bolt://${this.config.host || 'localhost'}:${this.config.port || '7687'}`;
    const neo4jConfig: Neo4jConfig = {
      maxConnectionLifetime: 30 * 60 * 1000,
      maxConnectionPoolSize: 50,
      connectionAcquisitionTimeout: 15000,
      connectionTimeout: 15000,
      logging: {
        level: 'warn',
        logger: (_level: string, message: string) => Logger.debug('neo4j-driver: %s', message),
      },
    };

    let lastError: unknown = null;

    for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
      try {
        Logger.debug('connection attempt %d/%d', attempt, this.maxRetries);
        this.driver = neo4j.driver(
          boltUri,
          neo4j.auth.basic(this.config.user || 'neo4j', this.config.password || ''),
          neo4jConfig
        );

        // Verify connectivity with a simple query
        const session = this.driver.session({ database: this.config.dbname || 'neo4j' });
        try {
          await session.run('RETURN 1 AS result');
        } finally {
          await session.close();
        }

        this.isConnected = true;
        Logger.info('successfully connected to Neo4j at %s (attempt %d)', boltUri, attempt);
        return;

      } catch (error) {
        lastError = error;
        this.isConnected = false;
        if (this.driver) {
          await this.driver.close().catch(() => {});
          this.driver = null;
        }

        Logger.warn('connection attempt %d failed: %s', attempt, getErrorMessage(error));

        if (attempt < this.maxRetries) {
          const backoffTime = Math.min(1000 * Math.pow(2, attempt - 1), 10000);
          Logger.debug('waiting %d ms before retry', backoffTime);
          await new Promise(resolve => setTimeout(resolve, backoffTime));
        }
      }
    }

    throw new Neo4jConnectionError(
      `Failed to connect to Neo4j after ${this.maxRetries} attempts: ${getErrorMessage(lastError)}`,
      'ECONNFAILED',
      lastError
    );
  }

  /**
   * Health check with a simple Cypher query.
   */
  async checkHealth(): Promise<boolean> {
    if (!this.isConnected || !this.driver) return false;
    const session = this.driver.session({ database: this.config.dbname || 'neo4j' });
    try {
      const result = await session.run('RETURN 1 AS health');
      return result.records.length > 0 && result.records[0].get('health') === 1;
    } catch (error) {
      this.isConnected = false;
      Logger.error('connection health check failed: %s', getErrorMessage(error));
      return false;
    } finally {
      await session.close();
    }
  }

  get connectionStatus(): { connected: boolean; healthy?: boolean } {
    return {
      connected: this.isConnected,
      healthy: this.isConnected ? undefined : false,
    };
  }

  /**
   * Execute a Cypher query. The `sql` argument is the Cypher statement.
   * Positional parameters from `params` (if any) are mapped to named
   * parameters `$p0`, `$p1`, … as expected by this inspector.
   */
  async query(sql: string, params?: any[]): Promise<any> {
    const safeParams = params ?? [];
    this.validateQueryParameters(sql, safeParams);

    if (!this.isConnected || !this.driver) {
      throw new Neo4jConnectionError('Database not connected');
    }

    const database = this.config.dbname || 'neo4j';
    const session = this.driver.session({ database });

    try {
      Logger.debug('executing Cypher: %s with %d parameters',
        sql.substring(0, 100) + (sql.length > 100 ? '...' : ''),
        safeParams.length);

      // Convert positional parameters to a named‑parameter object
      const parameterObject: Record<string, any> = {};
      safeParams.forEach((val, idx) => {
        parameterObject[`p${idx}`] = val;
      });

      const startTime = Date.now();
      const result = await session.run(sql, parameterObject);
      const duration = Date.now() - startTime;

      // Transform records to plain objects for consistency
      const rows: any[] = result.records.map(record => {
        const obj: Record<string, any> = {};
        record.keys.forEach(key => {
          obj[key as string] = record.get(key as string);
        });
        return obj;
      });

      // Build fields from the first record if available
      const fields = result.records.length > 0
        ? result.records[0].keys.map(key => ({ name: key, type: 'unknown' }))
        : [];

      Logger.debug('query completed in %d ms, %d rows returned', duration, rows.length);

      return {
        rows,
        rowCount: rows.length,
        fields,
        executionTime: duration,
      };
    } catch (error) {
      if (this.isConnectionError(error)) {
        this.isConnected = false;
        Logger.warn('connection lost during query');
      }

      const queryError = new Neo4jQueryError(
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
      await session.close();
    }
  }

  private validateQueryParameters(sql: string, params?: any[]): void {
    if (!sql || typeof sql !== 'string') {
      throw new Neo4jQueryError('Cypher query must be a non-empty string', sql, params);
    }
    // No further checks needed – the driver handles named parameter binding
  }

  private isConnectionError(error: unknown): boolean {
    const errorMessage = getErrorMessage(error).toLowerCase();
    const connectionErrors = [
      'econnreset', 'econnrefused', 'epipe', 'etimedout',
      'connection', 'connect', 'socket', 'network', 'terminated',
      'authentication', 'unauthorized', 'bolt protocol',
    ];
    return connectionErrors.some(term => errorMessage.includes(term));
  }

  async disconnect(): Promise<void> {
    if (this.driver) {
      try {
        Logger.debug('closing Neo4j driver');
        await this.driver.close();
        Logger.info('Neo4j driver closed successfully');
      } catch (error) {
        Logger.error('error closing driver: %s', getErrorMessage(error));
      } finally {
        this.isConnected = false;
        this.driver = null;
      }
    }
  }

  getDriver(): Driver | null {
    return this.driver;
  }

  getConfig(): Omit<DatabaseConfig, 'password'> {
    const { password, ...safeConfig } = this.config;
    return safeConfig;
  }
}

// ---------------------------------------------------------------------------
// Enhanced Neo4j Schema Inspector (graph‑to‑relational mapping)
// ---------------------------------------------------------------------------
class Neo4jSchemaInspector {
  private connection: Neo4jConnection;
  private database: string;   // Neo4j database name (default: "neo4j")

  constructor(config: DatabaseConfig) {
    this.database = config.dbname || config.schema || 'neo4j';
    this.connection = new Neo4jConnection({
      ...config,
      dbname: this.database,
    });
  }

  static setLogLevel(level: 'DEBUG' | 'INFO' | 'WARN' | 'ERROR'): void {
    Logger.setLogLevel(level);
  }

  static validateConnectionConfig(config: DatabaseConfig): string[] {
    const errors: string[] = [];

    if (config.port) {
      const port = parseInt(config.port, 10);
      if (isNaN(port)) errors.push('Port must be a valid number');
      else if (port < 1 || port > 65535) errors.push('Port must be between 1 and 65535');
    }

    return errors;
  }

  /**
   * Test connection and retrieve Neo4j server version.
   */
  async testConnection(): Promise<{ success: boolean; version?: string; error?: string }> {
    try {
      await this.connection.connect();
      const driver = this.connection.getDriver();
      if (!driver) throw new Error('Driver not available');

      const session = driver.session({ database: this.database });
      try {
        const result = await session.run('CALL dbms.components() YIELD versions RETURN versions[0] AS version');
        const version = result.records[0]?.get('version') || 'unknown';
        Logger.info('connection test successful – Neo4j version: %s', version);
        return { success: true, version };
      } finally {
        await session.close();
      }
    } catch (error) {
      const errorMessage = getErrorMessage(error);
      Logger.error('connection test failed: %s', errorMessage);
      return { success: false, error: errorMessage };
    } finally {
      await this.connection.disconnect();
    }
  }

  /**
   * Execute an arbitrary Cypher query.
   * Positional parameters are mapped to `$p0`, `$p1`, … .
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

      Logger.info('executing arbitrary Cypher query');
      Logger.debug('query: %s', sql.substring(0, 200) + (sql.length > 200 ? '...' : ''));

      let finalSql = sql;
      if (maxRows && (sql.trim().toUpperCase().startsWith('MATCH') || sql.trim().toUpperCase().startsWith('RETURN'))) {
        // Cypher uses LIMIT, same as SQL
        if (!finalSql.toUpperCase().includes('LIMIT')) {
          finalSql = `${finalSql} LIMIT ${maxRows}`;
        }
      }

      const result = await this.connection.query(finalSql, params);
      const executionTime = Date.now() - startTime;

      const queryResult: QueryResult = {
        success: true,
        rows: result.rows,
        rowCount: result.rowCount,
        fields: result.fields,
        executionTime,
        affectedRows: result.rowCount,
        command: 'CYPHER',
      };

      Logger.info('query executed successfully in %d ms, returned %d rows',
        executionTime, result.rowCount);

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

  async executeQueryAndDisconnect(sql: string, params: any[] = []): Promise<QueryResult> {
    return this.executeQuery(sql, params, { autoDisconnect: true });
  }

  /**
   * Execute a batch of Cypher queries sequentially (Neo4j does not support multi‑statement transactions in the same way).
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
          rowCount: result.rowCount,
          fields: result.fields,
          executionTime,
          affectedRows: result.rowCount,
          command: 'CYPHER',
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
   * Retrieves all node labels as “tables”. Optionally, also fetches
   * property keys and infers types using APOC procedures if available.
   */
  async getTables(): Promise<TableInfo[]> {
    await this.connection.connect();
    const driver = this.connection.getDriver();
    if (!driver) throw new Error('Not connected');

    const session = driver.session({ database: this.database });
    try {
      // Get all distinct node labels
      const labelResult = await session.run('CALL db.labels()');
      const labels: string[] = labelResult.records.map(record => record.get('label'));

      Logger.info('found %d node labels in database "%s"', labels.length, this.database);

      const tables: TableInfo[] = [];

      for (const label of labels) {
        const table = new TableInfo(this.database, label);
        table.tabletype = 'node_label';
        tables.push(table);

        // Gather property keys and types
        await this.getTableColumns(table, session);
        // Enrich with node count (row count)
        await this.enrichTableMetadata(table, session);
      }

      return tables;
    } catch (error) {
      Logger.error('Failed to retrieve labels: %s', getErrorMessage(error));
      throw error;
    } finally {
      await session.close();
    }
  }

  /**
   * Populate columns from a node label. Tries APOC first, then falls back
   * to a simple property key listing (without types).
   */
  private async getTableColumns(table: TableInfo, session: Session): Promise<void> {
    try {
      // Attempt to use APOC’s meta.nodeTypeProperties for full type info
      const apocResult = await session.run(
        `CALL apoc.meta.nodeTypeProperties({labels: [$label]})`,
        { label: table.tablename }
      );

      if (apocResult.records.length > 0) {
        for (const record of apocResult.records) {
          const propName = record.get('propertyName');
          const propTypes = record.get('propertyTypes'); // list of types
          // Use the first type or "mixed"
          const type = propTypes?.length > 0 ? propTypes[0] : 'Any';

          table.columns.push({
            name: propName,
            type,
            nullable: true,
            default: undefined,
            comment: undefined,
            isIdentity: false,
          });
        }
        Logger.debug('retrieved %d columns for label %s via APOC', table.columns.length, table.tablename);
        return;
      }
    } catch (error) {
      Logger.warn('APOC not available for label %s, falling back to property key listing', table.tablename);
    }

    // Fallback: get distinct property keys from a sample of nodes (limit 100)
    try {
      const fallbackResult = await session.run(
        `MATCH (n:\`${table.tablename}\`) RETURN keys(n) AS keys LIMIT 100`
      );
      const keySet = new Set<string>();
      for (const record of fallbackResult.records) {
        const keys = record.get('keys') as string[];
        keys.forEach(k => keySet.add(k));
      }

      for (const key of keySet) {
        table.columns.push({
          name: key,
          type: 'Any',
          nullable: true,
          default: undefined,
          comment: undefined,
          isIdentity: false,
        });
      }
      Logger.debug('retrieved %d columns for label %s via sampling', table.columns.length, table.tablename);
    } catch (error) {
      Logger.warn('failed to retrieve columns for label %s: %s', table.tablename, getErrorMessage(error));
    }
  }

  /**
   * Enrich with node count for the label.
   */
  private async enrichTableMetadata(table: TableInfo, session: Session): Promise<void> {
    try {
      const countResult = await session.run(
        `MATCH (n:\`${table.tablename}\`) RETURN count(n) AS count`
      );
      table.rowCount = countResult.records[0]?.get('count').toNumber() || 0;
    } catch (error) {
      Logger.warn('failed to get node count for label %s: %s', table.tablename, getErrorMessage(error));
    }
  }

  /**
   * Get a specific label (table) metadata.
   */
  async getTable(labelName: string): Promise<TableInfo | null> {
    const allTables = await this.getTables();
    return allTables.find(t => t.tablename === labelName) || null;
  }

  /**
   * Retrieve Neo4j version and database name.
   */
  async getDatabaseInfo(): Promise<{
    version: string;
    name: string;
    encoding?: string;
    collation?: string;
  }> {
    await this.connection.connect();
    const driver = this.connection.getDriver();
    if (!driver) throw new Error('Not connected');

    const session = driver.session({ database: this.database });
    try {
      const versionResult = await session.run('CALL dbms.components() YIELD versions RETURN versions[0] AS version');
      const version = versionResult.records[0]?.get('version') || 'unknown';
      return {
        version,
        name: this.database,
        encoding: undefined,
        collation: undefined,
      };
    } finally {
      await session.close();
    }
  }

  getConnection(): Neo4jConnection {
    return this.connection;
  }

  getCurrentSchema(): string {
    return this.database;
  }

  setSchema(database: string): void {
    this.database = database;
    // update the connection’s stored database name as well
    (this.connection as any).config.dbname = database;
    Logger.debug('database set to: %s', database);
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
// Exports – exactly mirroring the other inspectors
// ---------------------------------------------------------------------------
export {
  Neo4jSchemaInspector,
  Neo4jConnection,
  TableInfo,
  Logger,
  Neo4jConnectionError,
  Neo4jQueryError,
  getErrorMessage,
  isError,
};

export type {
  ColumnInfo,
  DatabaseConfig,
  QueryResult,
};

export default Neo4jSchemaInspector;