/**
 * Apache Cassandra Schema Inspector with Query Execution
 * CQL-based implementation using the DataStax Node.js driver.
 * Follows the design pattern of the PostgreSQL inspector for DatabaseMetadataWizard integration.
 */

import { Client, types, auth, execution, mapping } from 'cassandra-driver';

// ============================================================================
// Configuration & Core Types
// ============================================================================

/** Cassandra connection configuration */
interface CassandraConfig {
  contactPoints?: string[];    // e.g., ['127.0.0.1:9042']
  localDataCenter?: string;    // required for DSE / modern Cassandra
  keyspace?: string;           // default keyspace (schema)
  username?: string;
  password?: string;
  protocolOptions?: { port?: number };
  socketOptions?: { connectTimeout?: number; readTimeout?: number };
  maxRetries?: number;
  retryDelay?: number;
}

/** Column information (matches PostgreSQL inspector's ColumnInfo) */
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
  // Cassandra specific
  kind?: 'partition_key' | 'clustering_key' | 'regular' | 'static';
  clusteringOrder?: 'asc' | 'desc';
}

/** Table metadata – exactly same as PostgreSQL TableInfo */
class TableInfo {
  public tabletype: string = 'table';
  public comment?: string;
  public rowCount?: number;
  public size?: string;    // approximate size in bytes (or formatted)

  constructor(
    public schemaname: string,   // keyspace
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

/** Query execution result */
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

// ============================================================================
// Utilities & Logging (same as PostgreSQL version)
// ============================================================================

function getErrorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  if (typeof error === 'string') return error;
  return 'Unknown error occurred';
}

function isError(error: unknown): error is Error {
  return error instanceof Error;
}

class Logger {
  static logLevel: 'DEBUG' | 'INFO' | 'WARN' | 'ERROR' = 'INFO';

  static setLogLevel(level: 'DEBUG' | 'INFO' | 'WARN' | 'ERROR'): void {
    this.logLevel = level;
  }

  static debug(fmt: string, ...args: any[]): void {
    if (this.logLevel === 'DEBUG') {
      console.debug(`[DEBUG] ${this.format(fmt, args)}`);
    }
  }

  static info(fmt: string, ...args: any[]): void {
    if (['DEBUG', 'INFO'].includes(this.logLevel)) {
      console.log(`[INFO] ${this.format(fmt, args)}`);
    }
  }

  static warn(fmt: string, ...args: any[]): void {
    if (['DEBUG', 'INFO', 'WARN'].includes(this.logLevel)) {
      console.warn(`[WARN] ${this.format(fmt, args)}`);
    }
  }

  static error(fmt: string, ...args: any[]): void {
    console.error(`[ERROR] ${this.format(fmt, args)}`);
  }

  static fatal(fmt: string, ...args: any[]): never {
    const msg = this.format(fmt, args);
    console.error(`[FATAL] ${msg}`);
    throw new Error(msg);
  }

  private static format(fmt: string, args: any[]): string {
    return fmt.replace(/%(\w)/g, (_, spec) => {
      if (args.length === 0) return `%${spec}`;
      const val = args.shift();
      return String(val);
    });
  }
}

// Custom errors
class CassandraConnectionError extends Error {
  constructor(message: string, public code?: string, public originalError?: unknown) {
    super(message);
    this.name = 'CassandraConnectionError';
  }
}

class CassandraQueryError extends Error {
  constructor(message: string, public cql: string, public originalError?: unknown) {
    super(message);
    this.name = 'CassandraQueryError';
  }
}

// ============================================================================
// Cassandra Connection Manager (wraps driver Client)
// ============================================================================

class CassandraConnection {
  private client: Client | null = null;
  private isConnected: boolean = false;
  private config: Required<Omit<CassandraConfig, 'username' | 'password'>> &
    Pick<CassandraConfig, 'username' | 'password'>;

  constructor(config: CassandraConfig) {
    const defaulted = {
      contactPoints: ['127.0.0.1:9042'],
      localDataCenter: 'datacenter1',
      keyspace: undefined,
      protocolOptions: { port: 9042 },
      socketOptions: { connectTimeout: 30000, readTimeout: 120000 },
      maxRetries: 3,
      retryDelay: 1000,
      ...config,
    };
    this.config = {
      contactPoints: defaulted.contactPoints!,
      localDataCenter: defaulted.localDataCenter!,
      keyspace: defaulted.keyspace,
      protocolOptions: defaulted.protocolOptions!,
      socketOptions: defaulted.socketOptions!,
      maxRetries: defaulted.maxRetries!,
      retryDelay: defaulted.retryDelay!,
      username: config.username,
      password: config.password,
    };
  }

  /** Establish connection and optionally set keyspace */
  async connect(): Promise<void> {
    if (this.isConnected && this.client && !this.client.isShuttingDown) {
      Logger.debug('Cassandra connection already established');
      return;
    }

    let lastError: unknown = null;

    for (let attempt = 1; attempt <= this.config.maxRetries; attempt++) {
      try {
        Logger.debug('Cassandra connection attempt %d/%d', attempt, this.config.maxRetries);

        const authProvider = this.config.username && this.config.password
          ? new auth.PlainTextAuthProvider(this.config.username, this.config.password)
          : undefined;

        this.client = new Client({
          contactPoints: this.config.contactPoints,
          localDataCenter: this.config.localDataCenter,
          keyspace: this.config.keyspace,
          authProvider,
          protocolOptions: this.config.protocolOptions,
          socketOptions: this.config.socketOptions,
          pooling: {
            coreConnectionsPerHost: { [types.distance.local]: 2, [types.distance.remote]: 1 },
          },
        });

        await this.client.connect();
        this.isConnected = true;
        Logger.info('Successfully connected to Cassandra cluster (attempt %d)', attempt);
        return;
      } catch (error) {
        lastError = error;
        this.isConnected = false;
        if (this.client) {
          await this.client.shutdown().catch(() => {});
          this.client = null;
        }
        Logger.warn('Connection attempt %d failed: %s', attempt, getErrorMessage(error));
        if (attempt < this.config.maxRetries) {
          const backoff = Math.min(1000 * Math.pow(2, attempt - 1), 10000);
          await new Promise(resolve => setTimeout(resolve, backoff));
        }
      }
    }

    throw new CassandraConnectionError(
      `Failed to connect to Cassandra after ${this.config.maxRetries} attempts: ${getErrorMessage(lastError)}`,
      'ECONNFAILED',
      lastError
    );
  }

  /** Health check by executing a lightweight query */
  async checkHealth(): Promise<boolean> {
    if (!this.isConnected || !this.client) return false;
    try {
      const result = await this.client.execute('SELECT release_version FROM system.local');
      return result.rows.length > 0;
    } catch (error) {
      this.isConnected = false;
      Logger.error('Health check failed: %s', getErrorMessage(error));
      return false;
    }
  }

  /** Execute a CQL query with proper error handling */
  async execute(cql: string, params: any[] = [], options?: execution.QueryOptions): Promise<execution.ResultSet> {
    if (!this.isConnected || !this.client) {
      throw new CassandraConnectionError('Not connected to Cassandra');
    }
    try {
      Logger.debug('Executing CQL: %s', cql.substring(0, 200) + (cql.length > 200 ? '...' : ''));
      const result = await this.client.execute(cql, params, options);
      return result;
    } catch (error) {
      throw new CassandraQueryError(`Query failed: ${getErrorMessage(error)}`, cql, error);
    }
  }

  /** Get the underlying driver client (for advanced use) */
  getClient(): Client | null {
    return this.client;
  }

  /** Close connection gracefully */
  async disconnect(): Promise<void> {
    if (this.client) {
      Logger.debug('Shutting down Cassandra client');
      await this.client.shutdown();
      this.client = null;
      this.isConnected = false;
      Logger.info('Cassandra connection closed');
    }
  }

  get connectionStatus(): { connected: boolean; healthy?: boolean } {
    return { connected: this.isConnected, healthy: this.isConnected ? undefined : false };
  }

  getConfig(): Omit<CassandraConfig, 'password'> {
    const { password, ...safe } = this.config;
    return safe;
  }
}

// ============================================================================
// Cassandra Schema Inspector (main facade)
// ============================================================================

class CassandraSchemaInspector {
  private connection: CassandraConnection;
  private keyspace: string;   // equivalent to schema

  constructor(config: CassandraConfig) {
    this.connection = new CassandraConnection(config);
    this.keyspace = config.keyspace || '';
  }

  static setLogLevel(level: 'DEBUG' | 'INFO' | 'WARN' | 'ERROR'): void {
    Logger.setLogLevel(level);
  }

  static validateConnectionConfig(config: CassandraConfig): string[] {
    const errors: string[] = [];
    if (config.contactPoints && (!Array.isArray(config.contactPoints) || config.contactPoints.length === 0)) {
      errors.push('contactPoints must be a non-empty array');
    }
    if (config.localDataCenter && typeof config.localDataCenter !== 'string') {
      errors.push('localDataCenter must be a string');
    }
    if (config.keyspace && !/^[a-zA-Z][a-zA-Z0-9_]*$/.test(config.keyspace)) {
      errors.push('keyspace name contains invalid characters');
    }
    return errors;
  }

  /** Test connection and retrieve Cassandra version */
  async testConnection(): Promise<{ success: boolean; version?: string; error?: string }> {
    try {
      await this.connection.connect();
      const result = await this.connection.execute('SELECT release_version FROM system.local');
      const version = result.rows[0]?.release_version;
      Logger.info('Connection test successful, Cassandra version: %s', version);
      return { success: true, version };
    } catch (error) {
      const msg = getErrorMessage(error);
      Logger.error('Connection test failed: %s', msg);
      return { success: false, error: msg };
    } finally {
      await this.connection.disconnect();
    }
  }

  /**
   * Execute arbitrary CQL query (SELECT, INSERT, UPDATE, DELETE, etc.)
   * Supports automatic pagination and row limits.
   */
  async executeQuery(
    cql: string,
    params: any[] = [],
    options: { maxRows?: number; timeout?: number; autoDisconnect?: boolean } = {}
  ): Promise<QueryResult> {
    const startTime = Date.now();
    const { maxRows = 10000, autoDisconnect = false } = options;

    try {
      await this.connection.connect();

      let finalCql = cql;
      let rowLimitApplied = false;

      // Apply row limit only to SELECT queries that don't already have a LIMIT
      const upperCql = cql.trim().toUpperCase();
      if (upperCql.startsWith('SELECT') && !upperCql.includes('LIMIT') && maxRows) {
        finalCql = `${cql} LIMIT ${maxRows}`;
        rowLimitApplied = true;
        Logger.debug('Auto-applied LIMIT %d to SELECT query', maxRows);
      }

      const result = await this.connection.execute(finalCql, params, { prepare: true });
      const executionTime = Date.now() - startTime;

      // Convert rows to plain objects (driver returns Row instances)
      const rows = result.rows.map(row => (row ? (row as any).toJSON() : {}));

      const fields = result.columns
        ? result.columns.map(col => ({ name: col.name, type: col.type?.toString() || 'unknown' }))
        : [];

      Logger.info('Query executed in %d ms, %d rows returned', executionTime, rows.length);

      return {
        success: true,
        rows,
        rowCount: rows.length,
        fields,
        executionTime,
        affectedRows: result.rowLength,
        command: result.info?.queriedHost ? 'CQL' : 'CQL',
      };
    } catch (error) {
      const executionTime = Date.now() - startTime;
      const errorMessage = getErrorMessage(error);
      Logger.error('Query execution failed: %s', errorMessage);
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

  /** Convenience: auto-disconnect after query */
  async executeQueryAndDisconnect(cql: string, params: any[] = []): Promise<QueryResult> {
    return this.executeQuery(cql, params, { autoDisconnect: true });
  }

  /**
   * Execute a BATCH statement (Cassandra's atomic batch for logged batches).
   * @param queries Array of { cql, params } objects.
   * @param batchType 'LOGGED' (default) or 'UNLOGGED' or 'COUNTER'
   */
  async executeTransaction(
    queries: Array<{ sql: string; params?: any[] }>,
    batchType: 'LOGGED' | 'UNLOGGED' | 'COUNTER' = 'LOGGED'
  ): Promise<QueryResult[]> {
    const results: QueryResult[] = [];
    await this.connection.connect();

    try {
      // Build a single BATCH statement
      let batchCql = `BEGIN ${batchType} BATCH\n`;
      const allParams: any[] = [];
      for (const q of queries) {
        // Replace parameter placeholders with positional markers
        const paramCount = (q.params || []).length;
        const placeholders = paramCount ? `[${Array(paramCount).fill('?').join(',')}]` : '';
        batchCql += `${q.sql};\n`;
        if (q.params) allParams.push(...q.params);
      }
      batchCql += 'APPLY BATCH;';

      const startTime = Date.now();
      await this.connection.execute(batchCql, allParams, { prepare: true });
      const execTime = Date.now() - startTime;

      // For each query, produce a synthetic success result (Cassandra batch is atomic)
      for (let i = 0; i < queries.length; i++) {
        results.push({
          success: true,
          executionTime: execTime / queries.length,
          affectedRows: 1,
          command: batchType,
        });
      }
      Logger.info('Batch transaction executed successfully with %d statements', queries.length);
      return results;
    } catch (error) {
      Logger.error('Batch transaction failed: %s', getErrorMessage(error));
      throw new CassandraQueryError(`Transaction failed: ${getErrorMessage(error)}`, 'BATCH', error);
    } finally {
      // Do not auto-disconnect; leave connection open unless user calls disconnect
    }
  }

  /**
   * Retrieve all tables in the current keyspace (or all keyspaces if no keyspace set).
   * Returns TableInfo[] for each table.
   */
  async getTables(keyspace?: string): Promise<TableInfo[]> {
    const targetKeyspace = keyspace || this.keyspace;
    if (!targetKeyspace) {
      throw new CassandraConnectionError('No keyspace specified. Set keyspace in config or use setSchema()');
    }

    await this.connection.connect();

    // Query tables from system_schema
    const tablesQuery = `
      SELECT 
        table_name,
        keyspace_name
      FROM system_schema.tables 
      WHERE keyspace_name = ?
    `;
    const tablesResult = await this.connection.execute(tablesQuery, [targetKeyspace]);
    const tables: TableInfo[] = [];

    for (const row of tablesResult.rows) {
      const tableName = row.table_name;
      const columns = await this.getTableColumns(targetKeyspace, tableName);
      const table = new TableInfo(targetKeyspace, tableName, columns);

      // Optionally fetch approximate row count (via `SELECT COUNT(*)` can be expensive, skip or add as option)
      table.rowCount = 0;
      table.size = 'N/A';
      // Add comment if available (Cassandra does not have table comments by default, skip)
      tables.push(table);
    }

    Logger.info('Found %d tables in keyspace "%s"', tables.length, targetKeyspace);
    return tables;
  }

  /** Retrieve column definitions for a given table */
  private async getTableColumns(keyspace: string, table: string): Promise<ColumnInfo[]> {
    const columnsQuery = `
      SELECT 
        column_name,
        type,
        kind,
        clustering_order,
        position
      FROM system_schema.columns 
      WHERE keyspace_name = ? AND table_name = ?
      ORDER BY position
    `;
    const result = await this.connection.execute(columnsQuery, [keyspace, table]);
    const columns: ColumnInfo[] = [];

    for (const row of result.rows) {
      let typeStr = row.type;
      // Handle collection types/frozzen etc. - keep as string
      if (typeof typeStr === 'object') typeStr = typeStr.toString();

      const column: ColumnInfo = {
        name: row.column_name,
        type: typeStr,
        nullable: row.kind !== 'partition_key' && row.kind !== 'clustering_key', // primary key columns are not nullable
        kind: row.kind,
        clusteringOrder: row.clustering_order,
      };
      columns.push(column);
    }
    return columns;
  }

  /** Get a single table by name */
  async getTable(tableName: string, keyspace?: string): Promise<TableInfo | null> {
    const targetKeyspace = keyspace || this.keyspace;
    if (!targetKeyspace) throw new Error('Keyspace not set');
    const allTables = await this.getTables(targetKeyspace);
    return allTables.find(t => t.tablename === tableName) || null;
  }

  /** Get cluster information (C* version, cluster name, partitioner, etc.) */
  async getDatabaseInfo(): Promise<{
    version: string;
    name: string;
    encoding: string;
    collation: string;
  }> {
    await this.connection.connect();
    const result = await this.connection.execute(`
      SELECT cluster_name, release_version, partitioner
      FROM system.local
    `);
    const row = result.rows[0];
    return {
      version: row?.release_version || 'unknown',
      name: row?.cluster_name || 'unknown',
      encoding: 'UTF-8',
      collation: 'binary',
    };
  }

  /** Get the active connection manager */
  getConnection(): CassandraConnection {
    return this.connection;
  }

  /** Get current keyspace (schema) */
  getCurrentSchema(): string {
    return this.keyspace;
  }

  /** Set keyspace for subsequent metadata operations (global) */
  setSchema(keyspace: string): void {
    this.keyspace = keyspace;
    Logger.debug('Keyspace set to: %s', keyspace);
  }

  /** Utility: Convert TableInfo[] to standardized metadata format */
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
        kind: col.kind,
        clusteringOrder: col.clusteringOrder,
      })),
      comment: table.comment,
      rowCount: table.rowCount,
      size: table.size,
      originalData: table,
    }));
  }
}

// ============================================================================
// Exports
// ============================================================================

export {
  CassandraSchemaInspector,
  CassandraConnection,
  TableInfo,
  Logger,
  CassandraConnectionError,
  CassandraQueryError,
  getErrorMessage,
  isError,
};

export type {
  ColumnInfo,
  CassandraConfig,
  QueryResult,
};

export default CassandraSchemaInspector;