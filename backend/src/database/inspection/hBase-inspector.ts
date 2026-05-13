/**
 * Apache HBase Schema Inspector with Query Execution
 * REST API based implementation for HBase, following the design pattern of PostgreSQL inspector.
 * Supports namespace/schema abstraction, table metadata, column families, and data scanning.
 * Optimized for DatabaseMetadataWizard integration.
 */

import axios, { AxiosInstance, AxiosRequestConfig, AxiosError } from 'axios';

// ============================================================================
// Configuration & Core Types
// ============================================================================

/** HBase connection configuration (REST Gateway) */
interface HBaseConfig {
  restUrl?: string;          // e.g., http://localhost:8080
  namespace?: string;        // default namespace (schema equivalent)
  username?: string;         // optional basic auth
  password?: string;
  timeout?: number;          // request timeout in ms (default 30000)
  maxRetries?: number;
  retryDelay?: number;
}

/** Column family / column information (similar to PostgreSQL ColumnInfo) */
interface ColumnInfo {
  name: string;              // family name (or family:qualifier if needed)
  type: string;              // 'column family' or actual type info
  nullable?: boolean;
  default?: string;
  comment?: string;          // optional description
  isIdentity?: boolean;
  length?: number;
  precision?: number;
  scale?: number;
  // HBase specific extensions
  maxVersions?: number;
  compression?: string;
  ttl?: number;
  blockCache?: boolean;
}

/** Table metadata (identical structure to PostgreSQL inspector's TableInfo) */
class TableInfo {
  public tabletype: string = 'table';   // 'table' or 'view' (always table for HBase)
  public comment?: string;
  public rowCount?: number;
  public size?: string;                 // optional size info from HBase

  constructor(
    public schemaname: string,          // namespace
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

/** Result of a query execution (scan or get) */
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
// Utilities & Logging
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

// Custom error classes
class HBaseConnectionError extends Error {
  constructor(message: string, public code?: string, public originalError?: unknown) {
    super(message);
    this.name = 'HBaseConnectionError';
  }
}

class HBaseQueryError extends Error {
  constructor(message: string, public command: string, public originalError?: unknown) {
    super(message);
    this.name = 'HBaseQueryError';
  }
}

// ============================================================================
// HBase Connection Manager (REST client)
// ============================================================================

class HBaseConnection {
  private client: AxiosInstance | null = null;
  private isConnected: boolean = false;
  private config: Required<Omit<HBaseConfig, 'username' | 'password'>> &
    Pick<HBaseConfig, 'username' | 'password'>;

  constructor(config: HBaseConfig) {
    const defaulted = {
      restUrl: 'http://localhost:8080',
      namespace: 'default',
      timeout: 30000,
      maxRetries: 3,
      retryDelay: 1000,
      ...config,
    };
    this.config = {
      restUrl: defaulted.restUrl!,
      namespace: defaulted.namespace!,
      timeout: defaulted.timeout!,
      maxRetries: defaulted.maxRetries!,
      retryDelay: defaulted.retryDelay!,
      username: config.username,
      password: config.password,
    };
  }

  /** Establish connection – creates Axios instance and tests connectivity */
  async connect(): Promise<void> {
    if (this.isConnected && this.client) {
      Logger.debug('HBase connection already established');
      return;
    }

    const axiosConfig: AxiosRequestConfig = {
      baseURL: this.config.restUrl,
      timeout: this.config.timeout,
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
      },
    };

    if (this.config.username && this.config.password) {
      axiosConfig.auth = {
        username: this.config.username,
        password: this.config.password,
      };
    }

    let lastError: unknown = null;
    for (let attempt = 1; attempt <= this.config.maxRetries; attempt++) {
      try {
        Logger.debug('HBase connection attempt %d/%d', attempt, this.config.maxRetries);
        this.client = axios.create(axiosConfig);

        // Test connectivity: get version info
        const versionRes = await this.client.get('/version/cluster');
        if (versionRes.status !== 200) {
          throw new Error(`Unexpected status ${versionRes.status}`);
        }

        this.isConnected = true;
        Logger.info('Successfully connected to HBase REST at %s (attempt %d)',
          this.config.restUrl, attempt);
        return;
      } catch (error) {
        lastError = error;
        this.isConnected = false;
        this.client = null;

        Logger.warn('Connection attempt %d failed: %s', attempt, getErrorMessage(error));
        if (attempt < this.config.maxRetries) {
          const backoff = Math.min(1000 * Math.pow(2, attempt - 1), 10000);
          await new Promise(resolve => setTimeout(resolve, backoff));
        }
      }
    }

    throw new HBaseConnectionError(
      `Failed to connect to HBase REST after ${this.config.maxRetries} attempts: ${getErrorMessage(lastError)}`,
      'ECONNFAILED',
      lastError
    );
  }

  /** Health check by requesting cluster status */
  async checkHealth(): Promise<boolean> {
    if (!this.isConnected || !this.client) return false;
    try {
      const res = await this.client.get('/status/cluster');
      return res.status === 200;
    } catch (error) {
      this.isConnected = false;
      Logger.error('Health check failed: %s', getErrorMessage(error));
      return false;
    }
  }

  /**
   * Public method to retrieve the HBase cluster version string.
   * Replaces the previous pattern of casting to `any` and calling a private method.
   */
  public async getClusterVersion(): Promise<string> {
    if (!this.isConnected) {
      throw new HBaseConnectionError('Not connected - cannot retrieve cluster version');
    }
    const data = await this.request<{ version?: string }>('GET', '/version/cluster');
    return data?.version ?? 'unknown';
  }

  /** Execute a REST request with retries */
  private async request<T>(method: string, path: string, data?: any): Promise<T> {
    if (!this.client) throw new HBaseConnectionError('Not connected');
    try {
      const response = await this.client.request({ method, url: path, data });
      return response.data;
    } catch (error) {
      const axiosErr = error as AxiosError;
      throw new HBaseQueryError(
        `REST request failed: ${axiosErr.message}`,
        `${method} ${path}`,
        error
      );
    }
  }

  /** Perform an HBase scan using the scanner API */
  async scan(
    table: string,
    options: {
      startRow?: string;
      endRow?: string;
      columns?: string[];    // e.g., ["cf:col1", "cf:col2"]
      limit?: number;
      batchSize?: number;
      reversed?: boolean;
    } = {}
  ): Promise<{ rows: any[]; count: number }> {
    const scannerConfig: any = {};
    if (options.startRow) scannerConfig.startRow = options.startRow;
    if (options.endRow) scannerConfig.endRow = options.endRow;
    if (options.columns) scannerConfig.columns = options.columns;
    if (options.batchSize) scannerConfig.batch = options.batchSize;
    if (options.reversed) scannerConfig.reversed = options.reversed;

    const limit = options.limit ?? 0;
    const batchSize = options.batchSize ?? 100;

    try {
      // 1. Create scanner
      const scannerRes = await this.request<any>(
        'POST',
        `/${encodeURIComponent(table)}/scanner`,
        scannerConfig
      );
      const scannerId = scannerRes?.scannerId;
      if (!scannerId) throw new Error('No scannerId returned');

      const rows: any[] = [];
      let fetched = 0;
      let stop = false;

      while (!stop) {
        const fetchRes = await this.request<any>(
          'GET',
          `/${encodeURIComponent(table)}/scanner/${scannerId}`,
          undefined
        );
        const chunk = fetchRes?.rows || [];
        rows.push(...chunk);
        fetched += chunk.length;

        if (chunk.length === 0 || (limit > 0 && fetched >= limit)) {
          stop = true;
        }
        // Prevent infinite loops in case of unexpected behaviour
        if (chunk.length < batchSize && chunk.length > 0) {
          stop = true;
        }
      }

      // 2. Delete scanner
      await this.request('DELETE', `/${encodeURIComponent(table)}/scanner/${scannerId}`);

      const finalRows = limit > 0 ? rows.slice(0, limit) : rows;
      return { rows: finalRows, count: finalRows.length };
    } catch (error) {
      throw new HBaseQueryError(`Scan failed on ${table}`, 'SCAN', error);
    }
  }

  /** Get a single row from a table */
  async get(table: string, rowKey: string, columns?: string[]): Promise<any> {
    let url = `/${encodeURIComponent(table)}/${encodeURIComponent(rowKey)}`;
    if (columns && columns.length) {
      url += `?column=${columns.map(c => encodeURIComponent(c)).join(',')}`;
    }
    const data = await this.request<any>('GET', url);
    return data?.Row || null;
  }

  /** List all tables (optionally filtered by namespace) */
  async listTables(namespace?: string): Promise<string[]> {
    const ns = namespace ?? this.config.namespace;
    const path = ns === 'default'
      ? '/'
      : `/namespaces/${encodeURIComponent(ns)}/tables`;
    const result = await this.request<any>('GET', path);
    const tables: string[] = [];
    if (result.tables) {
      for (const t of result.tables) {
        const name = typeof t === 'string' ? t : t.name;
        tables.push(name);
      }
    } else if (Array.isArray(result)) {
      tables.push(...result);
    }
    // names may include namespace:table, extract simple name
    return tables.map(t => t.includes(':') ? t.split(':')[1] : t);
  }

  /** Describe table schema: column families */
  async describeTable(table: string, namespace?: string): Promise<ColumnInfo[]> {
    const ns = namespace ?? this.config.namespace;
    const fullName = ns === 'default' ? table : `${ns}:${table}`;
    const schema = await this.request<any>('GET', `/${encodeURIComponent(fullName)}/schema`);
    const columns: ColumnInfo[] = [];
    if (schema?.ColumnSchema) {
      for (const cf of schema.ColumnSchema) {
        const col: ColumnInfo = {
          name: cf.name,
          type: 'column family',
          nullable: true,
          comment: cf.DESCRIPTION || undefined,
          maxVersions: cf.VERSIONS ? parseInt(cf.VERSIONS) : undefined,
          compression: cf.COMPRESSION,
          ttl: cf.TTL ? parseInt(cf.TTL) : undefined,
          blockCache: cf.BLOCKCACHE === 'true',
        };
        columns.push(col);
      }
    }
    return columns;
  }

  /** Close connection (release client, no persistent pool) */
  async disconnect(): Promise<void> {
    if (this.client) {
      Logger.debug('Closing HBase REST client');
      this.client = null;
      this.isConnected = false;
      Logger.info('HBase connection closed');
    }
  }

  getConfig(): Omit<HBaseConfig, 'password'> {
    const { password, ...safe } = this.config;
    return safe;
  }

  get connectionStatus(): { connected: boolean; healthy?: boolean } {
    return { connected: this.isConnected, healthy: this.isConnected ? undefined : false };
  }
}

// ============================================================================
// HBase Schema Inspector (main facade)
// ============================================================================

class HBaseSchemaInspector {
  private connection: HBaseConnection;
  private namespace: string;

  constructor(config: HBaseConfig) {
    this.connection = new HBaseConnection(config);
    this.namespace = config.namespace || 'default';
  }

  static setLogLevel(level: 'DEBUG' | 'INFO' | 'WARN' | 'ERROR'): void {
    Logger.setLogLevel(level);
  }

  static validateConnectionConfig(config: HBaseConfig): string[] {
    const errors: string[] = [];
    if (config.restUrl && !config.restUrl.startsWith('http')) {
      errors.push('restUrl must start with http:// or https://');
    }
    if (config.namespace && !/^[a-zA-Z0-9_]+$/.test(config.namespace)) {
      errors.push('namespace contains invalid characters');
    }
    return errors;
  }

  /** Test connectivity and retrieve HBase version */
  async testConnection(): Promise<{ success: boolean; version?: string; error?: string }> {
    try {
      await this.connection.connect();
      const version = await this.connection.getClusterVersion();
      Logger.info('HBase connection test successful, version: %s', version);
      return { success: true, version };
    } catch (error) {
      const msg = getErrorMessage(error);
      Logger.error('Connection test failed: %s', msg);
      return { success: false, error: msg };
    } finally {
      await this.connection.disconnect();
    }
  }

  /** Execute a query against HBase. Supports SCAN and GET commands using shell-like syntax. */
  async executeQuery(
    sql: string,
    _params: any[] = [],
    options: { maxRows?: number; timeout?: number; autoDisconnect?: boolean } = {}
  ): Promise<QueryResult> {
    const startTime = Date.now();
    const { maxRows, autoDisconnect = false } = options;
    try {
      await this.connection.connect();

      let command = sql.trim();
      let resultRows: any[] = [];
      let commandType = '';

      // Parse commands: SCAN "table" ...  or  GET "table" "rowkey"
      const scanMatch = command.match(/^SCAN\s+['"]([^'"]+)['"](.*)$/i);
      const getMatch = command.match(/^GET\s+['"]([^'"]+)['"]\s+['"]([^'"]+)['"](.*)$/i);

      if (scanMatch) {
        // SCAN command
        const table = scanMatch[1];
        const rest = scanMatch[2];
        const scanOptions = this.parseScanOptions(rest, maxRows);
        const result = await this.connection.scan(table, scanOptions);
        resultRows = result.rows;
        commandType = 'SCAN';
      } else if (getMatch) {
        // GET command
        const table = getMatch[1];
        const rowKey = getMatch[2];
        const columnsPart = getMatch[3].trim();
        let columns: string[] | undefined;
        if (columnsPart && columnsPart.toUpperCase().startsWith('COLUMNS')) {
          const colMatch = columnsPart.match(/COLUMNS\s+['"]([^'"]+)['"]/i);
          if (colMatch) {
            columns = colMatch[1].split(',').map(c => c.trim());
          }
        }
        const row = await this.connection.get(table, rowKey, columns);
        resultRows = row ? [row] : [];
        commandType = 'GET';
      } else {
        throw new HBaseQueryError(
          'Unsupported query. Use SCAN \'table\' [STARTROW \'row\'] [ENDROW \'row\'] [LIMIT n] [COLUMNS \'cf:col1,cf:col2\'] or GET \'table\' \'rowkey\'',
          command
        );
      }

      const executionTime = Date.now() - startTime;
      const fields = this.extractFieldsFromRows(resultRows);

      return {
        success: true,
        rows: resultRows,
        rowCount: resultRows.length,
        fields,
        executionTime,
        command: commandType,
        affectedRows: resultRows.length,
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
  async executeQueryAndDisconnect(sql: string, params: any[] = []): Promise<QueryResult> {
    return this.executeQuery(sql, params, { autoDisconnect: true });
  }

  /** Parse SCAN command options from string */
  private parseScanOptions(rest: string, defaultLimit?: number): any {
    const options: any = {};
    const limit = defaultLimit ? Math.min(defaultLimit, 1000) : undefined;

    const startMatch = rest.match(/STARTROW\s+['"]([^'"]+)['"]/i);
    const endMatch = rest.match(/ENDROW\s+['"]([^'"]+)['"]/i);
    const limitMatch = rest.match(/LIMIT\s+(\d+)/i);
    const columnsMatch = rest.match(/COLUMNS\s+['"]([^'"]+)['"]/i);
    const reversedMatch = rest.match(/REVERSED\s+true/i);

    if (startMatch) options.startRow = startMatch[1];
    if (endMatch) options.endRow = endMatch[1];
    if (limitMatch) options.limit = parseInt(limitMatch[1], 10);
    else if (limit) options.limit = limit;
    if (columnsMatch) options.columns = columnsMatch[1].split(',').map(c => c.trim());
    if (reversedMatch) options.reversed = true;

    return options;
  }

  private extractFieldsFromRows(rows: any[]): Array<{ name: string; type: string }> {
    if (!rows || rows.length === 0) return [];
    const fieldSet = new Set<string>();
    for (const row of rows) {
      if (row.key) fieldSet.add('key');
      if (row.cell) {
        for (const cell of row.cell) {
          const colName = `${cell.column}`;
          fieldSet.add(colName);
        }
      }
    }
    return Array.from(fieldSet).map(name => ({ name, type: 'string' }));
  }

  /** Retrieve all tables (with column families) */
  async getTables(): Promise<TableInfo[]> {
    await this.connection.connect();
    const tables: TableInfo[] = [];

    try {
      const tableNames = await this.connection.listTables(this.namespace);
      Logger.info('Found %d tables in namespace "%s"', tableNames.length, this.namespace);

      for (const tableName of tableNames) {
        const columns = await this.connection.describeTable(tableName, this.namespace);
        const table = new TableInfo(this.namespace, tableName, columns);
        table.tabletype = 'table';
        // Optionally try to fetch row count estimate (not directly available, set 0)
        table.rowCount = 0;
        table.size = 'N/A';
        tables.push(table);
      }
      return tables;
    } catch (error) {
      Logger.error('Failed to retrieve tables: %s', getErrorMessage(error));
      throw error;
    }
  }

  /** Get a single table by name */
  async getTable(tableName: string): Promise<TableInfo | null> {
    const allTables = await this.getTables();
    return allTables.find(t => t.tablename === tableName) || null;
  }

  /** Get cluster information (version, name, etc.) */
  async getDatabaseInfo(): Promise<{
    version: string;
    name: string;
    encoding: string;
    collation: string;
  }> {
    await this.connection.connect();
    try {
      const version = await this.connection.getClusterVersion();
      return {
        version,
        name: `HBase_${this.namespace}`,
        encoding: 'UTF-8',
        collation: 'binary',
      };
    } catch (error) {
      Logger.error('Failed to get database info: %s', getErrorMessage(error));
      throw error;
    }
  }

  /** Direct access to connection (for advanced use) */
  getConnection(): HBaseConnection {
    return this.connection;
  }

  /** Get current namespace (schema) */
  getCurrentSchema(): string {
    return this.namespace;
  }

  /** Set namespace (schema) for operations */
  setSchema(schema: string): void {
    this.namespace = schema;
    Logger.debug('Namespace set to: %s', schema);
  }

  /** Transaction (not supported in HBase) */
  async executeTransaction(): Promise<QueryResult[]> {
    throw new Error('Transactions are not supported in HBase');
  }

  /** Convert TableInfo list to standardized format for metadata wizard */
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
        maxVersions: col.maxVersions,
        compression: col.compression,
        ttl: col.ttl,
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
  HBaseSchemaInspector,
  HBaseConnection,
  TableInfo,
  Logger,
  HBaseConnectionError,
  HBaseQueryError,
  getErrorMessage,
  isError,
};

export type {
  ColumnInfo,
  HBaseConfig,
  QueryResult,
};

export default HBaseSchemaInspector;