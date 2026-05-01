/**
 * Enhanced Elasticsearch Schema Inspector with Query Execution
 * Comprehensive index/field inspection with robust error handling and connection management
 * TypeScript implementation optimized for DatabaseMetadataWizard integration
 */

import { Client, ClientOptions } from '@elastic/elasticsearch';

// ---------------------------------------------------------------------------
// Re‑use identical type definitions and utilities from the PostgreSQL inspector
// ---------------------------------------------------------------------------

interface DatabaseConfig {
  dbname: string;            // index pattern to inspect (e.g. "logs-*", or "*" for all)
  host?: string;
  port?: string;
  user?: string;
  password?: string;
  schema?: string;           // alias for dbname (not used directly, kept for compatibility)
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
  public tabletype: string = 'index';
  public comment?: string;
  public rowCount?: number;
  public size?: string;

  constructor(
    public schemaname: string,   // Elasticsearch has no schema; we set this to ''
    public tablename: string,    // index name
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
// Elasticsearch‑specific error classes
// ---------------------------------------------------------------------------
class ElasticsearchConnectionError extends Error {
  constructor(
    message: string,
    public code?: string,
    public originalError?: unknown
  ) {
    super(message);
    this.name = 'ElasticsearchConnectionError';
  }
}

class ElasticsearchQueryError extends Error {
  constructor(
    message: string,
    public sql: string,
    public parameters: any[] = [],
    public originalError?: unknown
  ) {
    super(message);
    this.name = 'ElasticsearchQueryError';
  }
}

// ---------------------------------------------------------------------------
// Elasticsearch Connection Manager
// ---------------------------------------------------------------------------
class ElasticsearchConnection {
  private client: Client | null = null;
  private isConnected: boolean = false;
  private readonly maxRetries: number = 3;

  constructor(private config: DatabaseConfig) {}

  /**
   * Initialises the Elasticsearch client and pings the cluster.
   */
  async connect(): Promise<void> {
    if (this.isConnected && this.client) {
      Logger.debug('connection already established');
      return;
    }

    const clientOptions: ClientOptions = {
      node: `http://${this.config.host || 'localhost'}:${this.config.port || '9200'}`,
      auth: this.config.user && this.config.password
        ? { username: this.config.user, password: this.config.password }
        : undefined,
      requestTimeout: 30000,
      maxRetries: 1,
      // Elasticsearch client automatically manages connection pool
    };

    let lastError: unknown = null;

    for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
      try {
        Logger.debug('connection attempt %d/%d', attempt, this.maxRetries);
        this.client = new Client(clientOptions);

        // Ping to verify connectivity
        const pingResult = await this.client.ping();
        if (!pingResult) {
          throw new Error('Cluster ping returned falsy');
        }

        this.isConnected = true;
        Logger.info('successfully connected to Elasticsearch at %s (attempt %d)',
          clientOptions.node, attempt);
        return;

      } catch (error) {
        lastError = error;
        this.isConnected = false;
        if (this.client) {
          try { await this.client.close(); } catch (_) {}
          this.client = null;
        }

        Logger.warn('connection attempt %d failed: %s', attempt, getErrorMessage(error));

        if (attempt < this.maxRetries) {
          const backoffTime = Math.min(1000 * Math.pow(2, attempt - 1), 10000);
          Logger.debug('waiting %d ms before retry', backoffTime);
          await new Promise(resolve => setTimeout(resolve, backoffTime));
        }
      }
    }

    throw new ElasticsearchConnectionError(
      `Failed to connect to Elasticsearch after ${this.maxRetries} attempts: ${getErrorMessage(lastError)}`,
      'ECONNFAILED',
      lastError
    );
  }

  /**
   * Check health by pinging the cluster.
   */
  async checkHealth(): Promise<boolean> {
    if (!this.isConnected || !this.client) return false;
    try {
      const pingResult = await this.client.ping();
      return pingResult === true;
    } catch (error) {
      this.isConnected = false;
      Logger.error('connection health check failed: %s', getErrorMessage(error));
      return false;
    }
  }

  get connectionStatus(): { connected: boolean; healthy?: boolean } {
    return {
      connected: this.isConnected,
      healthy: this.isConnected ? undefined : false,
    };
  }

  /**
   * Execute a raw Elasticsearch query or SQL.
   * - If `sql` starts with '{', treat it as a JSON DSL and run `client.search`.
   * - Otherwise treat it as Elasticsearch SQL and use `client.sql.query`.
   * Parameters are ignored for raw DSL; for SQL, they are passed as `params` in the SQL request.
   */
  async query(sql: string, params?: any[]): Promise<any> {
    const safeParams = params ?? [];
    if (!this.isConnected || !this.client) {
      throw new ElasticsearchConnectionError('Database not connected');
    }

    try {
      // Detect if the query is a JSON DSL body
      const trimmed = sql.trim();
      if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
        return this.executeDSLQuery(trimmed);
      } else {
        return this.executeSQLQuery(trimmed, safeParams);
      }
    } catch (error) {
      throw this.wrapQueryError(error, sql, safeParams);
    }
  }

  private async executeDSLQuery(dsl: string): Promise<any> {
    let body: any;
    try {
      body = JSON.parse(dsl);
    } catch (parseError) {
      throw new ElasticsearchQueryError(
        `Invalid JSON DSL: ${getErrorMessage(parseError)}`,
        dsl,
        []
      );
    }

    // Determine if it's a search request (default) or other endpoint
    // For simplicity we assume the body is a search request
    const index = this.config.dbname || '*';
    Logger.debug('executing DSL search on index %s', index);

    const startTime = Date.now();
    const response = await this.client!.search({
      index,
      body,
    });
    const duration = Date.now() - startTime;

    Logger.debug('DSL query completed in %d ms, %d hits', duration, response.hits.hits.length);

    return {
      rows: response.hits.hits.map(hit => ({ _id: hit._id, _index: hit._index, ...hit._source })),
      rowCount: response.hits.total,
      fields: [],  // DSL response does not directly offer a schema
      executionTime: duration,
    };
  }

  private async executeSQLQuery(sql: string, params: any[]): Promise<any> {
    Logger.debug('executing Elasticsearch SQL: %s', sql);
    const startTime = Date.now();

    const response = await this.client!.sql.query({
      query: sql,
      params: params.length > 0 ? params : undefined,
    });

    const duration = Date.now() - startTime;
    Logger.debug('SQL query completed in %d ms, %d rows', duration, response.rows?.length || 0);

    const fields = (response.columns || []).map((col: any) => ({
      name: col.name,
      type: col.type,
    }));

    // Transform rows into object arrays for consistency
    const rows = (response.rows || []).map((row: any[]) => {
      const obj: Record<string, any> = {};
      fields.forEach((field: any, idx: number) => {
        obj[field.name] = row[idx];
      });
      return obj;
    });

    return {
      rows,
      rowCount: rows.length,
      fields,
      executionTime: duration,
    };
  }

  private wrapQueryError(error: unknown, sql: string, params: any[]): Error {
    const message = getErrorMessage(error);
    return new ElasticsearchQueryError(
      `Query execution failed: ${message}`,
      sql,
      params,
      error
    );
  }

  async disconnect(): Promise<void> {
    if (this.client) {
      try {
        Logger.debug('closing Elasticsearch client');
        await this.client.close();
        Logger.info('Elasticsearch client closed successfully');
      } catch (error) {
        Logger.error('error closing client: %s', getErrorMessage(error));
      } finally {
        this.isConnected = false;
        this.client = null;
      }
    }
  }

  getClient(): Client | null {
    return this.client;
  }

  getConfig(): Omit<DatabaseConfig, 'password'> {
    const { password, ...safeConfig } = this.config;
    return safeConfig;
  }
}

// ---------------------------------------------------------------------------
// Enhanced Elasticsearch Schema Inspector
// ---------------------------------------------------------------------------
class ElasticsearchSchemaInspector {
  private connection: ElasticsearchConnection;
  private indexPattern: string;   // replica of dbname for readability

  constructor(config: DatabaseConfig) {
    // dbname is the index pattern to inspect (default '*')
    this.indexPattern = config.dbname || '*';
    this.connection = new ElasticsearchConnection({
      ...config,
      dbname: this.indexPattern,
    });
  }

  static setLogLevel(level: 'DEBUG' | 'INFO' | 'WARN' | 'ERROR'): void {
    Logger.setLogLevel(level);
  }

  static validateConnectionConfig(config: DatabaseConfig): string[] {
    const errors: string[] = [];

    // Elasticsearch requires at least a host, but we can default to localhost
    // No strict dbname requirement because '*' is valid

    if (config.port) {
      const port = parseInt(config.port, 10);
      if (isNaN(port)) errors.push('Port must be a valid number');
      else if (port < 1 || port > 65535) errors.push('Port must be between 1 and 65535');
    }

    return errors;
  }

  /**
   * Test connection and retrieve cluster info.
   */
  async testConnection(): Promise<{ success: boolean; version?: string; error?: string }> {
    try {
      await this.connection.connect();
      const client = this.connection.getClient();
      if (!client) return { success: false, error: 'Client not initialised' };

      const info = await client.info();
      const version = info.version.number;

      Logger.info('connection test successful – Elasticsearch version: %s', version);
      return { success: true, version };
    } catch (error) {
      const errorMessage = getErrorMessage(error);
      Logger.error('connection test failed: %s', errorMessage);
      return { success: false, error: errorMessage };
    } finally {
      await this.connection.disconnect();
    }
  }

  /**
   * Execute arbitrary query (DSL or SQL, auto‑detected).
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

      Logger.info('executing Elasticsearch query');
      Logger.debug('query: %s', sql.substring(0, 200) + (sql.length > 200 ? '...' : ''));

      // For SQL, we can append a LIMIT if not present
      let finalSql = sql;
      if (maxRows && !sql.trim().startsWith('{')) {
        // Only for SQL queries
        finalSql = this.applyRowLimit(sql, maxRows);
      }

      const result = await this.connection.query(finalSql, params);
      const executionTime = Date.now() - startTime;

      const queryResult: QueryResult = {
        success: true,
        rows: result.rows,
        rowCount: result.rowCount || result.rows?.length,
        fields: result.fields,
        executionTime,
        affectedRows: result.rows?.length,
        command: 'SELECT', // approximate
      };

      Logger.info('query executed successfully in %d ms, returned %d rows',
        executionTime, queryResult.rowCount);

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

  private applyRowLimit(sql: string, maxRows: number): string {
    const trimmed = sql.trim();
    const upperSql = trimmed.toUpperCase();

    if (upperSql.startsWith('SELECT') && !upperSql.includes('LIMIT')) {
      return `${trimmed} LIMIT ${maxRows}`;
    }
    return sql;
  }

  async executeQueryAndDisconnect(sql: string, params: any[] = []): Promise<QueryResult> {
    return this.executeQuery(sql, params, { autoDisconnect: true });
  }

  /**
   * Execute multiple queries in sequence.
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
          rowCount: result.rowCount || result.rows?.length,
          fields: result.fields,
          executionTime,
          affectedRows: result.rows?.length,
          command: 'SELECT',
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
   * Retrieves all indices matching the configured pattern as "tables".
   * Excludes system indices (starting with '.') if pattern is '*'.
   */
  async getTables(): Promise<TableInfo[]> {
    await this.connection.connect();
    const client = this.connection.getClient();
    if (!client) throw new Error('Not connected');

    try {
      // Use cat.indices to list all indices matching pattern
      const catResponse = await client.cat.indices({
        index: this.indexPattern,
        format: 'json',
        h: 'index,status,health,docs.count,store.size',
      });

      const indices = Array.isArray(catResponse) ? catResponse : [];
      Logger.info('found %d indices matching pattern "%s"', indices.length, this.indexPattern);

      const tables: TableInfo[] = [];

      for (const idx of indices) {
        // Filter out system indices if we used '*', otherwise keep all
        if (this.indexPattern === '*' && idx.index?.startsWith('.')) {
          continue;
        }

        const table = new TableInfo('', idx.index!);
        table.rowCount = parseInt(idx['docs.count'] || '0', 10);
        table.size = idx['store.size'] || undefined;
        table.tabletype = 'index';   // we'll detect alias later

        // Retrieve mapping as columns
        await this.getTableColumns(table);
        // Enrich with aliases if any
        await this.enrichTableMetadata(table);

        tables.push(table);
      }

      return tables;
    } catch (error) {
      Logger.error('Failed to retrieve indices: %s', getErrorMessage(error));
      throw error;
    }
  }

  /**
   * Populate columns from the index mapping.
   */
  private async getTableColumns(table: TableInfo): Promise<void> {
    const client = this.connection.getClient();
    if (!client) return;

    try {
      const mappingResponse = await client.indices.getMapping({
        index: table.tablename,
      });

      const mappings = mappingResponse[table.tablename]?.mappings?.properties;
      if (!mappings) {
        Logger.warn('no mapping found for index %s', table.tablename);
        return;
      }

      // Extract fields recursively? For simplicity, only top-level fields.
      for (const [fieldName, fieldDef] of Object.entries(mappings)) {
        const col: ColumnInfo = {
          name: fieldName,
          type: (fieldDef as any).type || 'object',
          nullable: true,  // Elasticsearch doesn't have NOT NULL
          comment: undefined,
          isIdentity: false,
        };

        // Handle some sub‑type details
        if (col.type === 'text' || col.type === 'keyword') {
          col.length = undefined; // text fields have no max length
        } else if (col.type === 'date') {
          // no extra
        } else if (col.type === 'integer' || col.type === 'long') {
          col.precision = 64; // rough
        } else if (col.type === 'float' || col.type === 'double') {
          col.precision = 53; // double precision
        }

        table.columns.push(col);
      }

      Logger.debug('retrieved %d columns for index %s', table.columns.length, table.tablename);
    } catch (error) {
      Logger.warn('failed to get mapping for %s: %s', table.tablename, getErrorMessage(error));
    }
  }

  /**
   * Enrich with alias info and verify document count.
   */
  private async enrichTableMetadata(table: TableInfo): Promise<void> {
    const client = this.connection.getClient();
    if (!client) return;

    try {
      // Check if it's an alias
      const aliasResponse = await client.indices.getAlias({
        index: table.tablename,
      });

      // If the response contains entries and the key matches, it might be an alias pointing to another index,
      // but single-index alias will have its own name. We'll simply note if there are aliases.
      const aliases = aliasResponse[table.tablename]?.aliases;
      if (aliases && Object.keys(aliases).length > 0) {
        // The index itself is a concrete index, but could have aliases.
        // However, in Elasticsearch, an alias can be listed as an index, but
        // it won't have a mapping. If we got mapping earlier it’s either a real index
        // or an alias pointing to one. We'll set tabletype to 'alias' only if no mapping was found.
        // For simplicity, we'll leave it as 'index' because we already got columns.
      }
    } catch (error) {
      // ignore
    }
  }

  /**
   * Get a specific index (table) metadata.
   */
  async getTable(indexName: string): Promise<TableInfo | null> {
    const allTables = await this.getTables();
    return allTables.find(t => t.tablename === indexName) || null;
  }

  /**
   * Get Elasticsearch version and cluster name.
   */
  async getDatabaseInfo(): Promise<{
    version: string;
    name: string;
    encoding?: string;
    collation?: string;
  }> {
    await this.connection.connect();
    const client = this.connection.getClient();
    if (!client) throw new Error('Not connected');

    const info = await client.info();
    return {
      version: info.version.number,
      name: info.cluster_name,
      encoding: undefined,
      collation: undefined,
    };
  }

  // ----- New methods required by ElasticsearchAdapter -----

  /**
   * Returns a list of index names matching the current pattern.
   */
  async getIndices(): Promise<string[]> {
    await this.connection.connect();
    const client = this.connection.getClient();
    if (!client) throw new Error('Not connected');

    const catResponse = await client.cat.indices({
      index: this.indexPattern,
      format: 'json',
      h: 'index',
    });
    return (Array.isArray(catResponse) ? catResponse : [])
      .map((idx: any) => idx.index)
      .filter((name: string) => this.indexPattern !== '*' || !name.startsWith('.'));
  }

  /**
   * Returns the raw mapping for a given index.
   */
  async getMapping(index: string): Promise<Record<string, any>> {
    await this.connection.connect();
    const client = this.connection.getClient();
    if (!client) throw new Error('Not connected');
    return client.indices.getMapping({ index });
  }

  /**
   * Execute a raw search request.
   */
  async search(index: string, body: any): Promise<any> {
    await this.connection.connect();
    const client = this.connection.getClient();
    if (!client) throw new Error('Not connected');
    return client.search({ index, body });
  }

  /**
   * Execute a count request.
   */
  async count(index: string, body: any): Promise<any> {
    await this.connection.connect();
    const client = this.connection.getClient();
    if (!client) throw new Error('Not connected');
    return client.count({ index, body });
  }

  /**
   * Get cluster health.
   */
  async getClusterHealth(): Promise<any> {
    await this.connection.connect();
    const client = this.connection.getClient();
    if (!client) throw new Error('Not connected');
    return client.cluster.health();
  }

  /**
   * Execute a raw Elasticsearch SQL query (using _sql endpoint).
   * This delegates to executeQuery with auto-connection management.
   */
  async executeSQL(sql: string, options?: { maxRows?: number }): Promise<QueryResult> {
    return this.executeQuery(sql, [], options);
  }

  /**
   * Alias for getDatabaseInfo that returns the shape expected by the adapter.
   */
  async getClusterInfo(): Promise<{ cluster_name: string; version: { number: string } }> {
    const info = await this.getDatabaseInfo();
    return {
      cluster_name: info.name,
      version: { number: info.version },
    };
  }

  // ----- End of new methods -----

  getConnection(): ElasticsearchConnection {
    return this.connection;
  }

  /**
   * For Elasticsearch, "schema" is not directly applicable,
   * but we can allow changing the index pattern.
   */
  getCurrentSchema(): string {
    return this.indexPattern;
  }

  setSchema(pattern: string): void {
    this.indexPattern = pattern;
    // Update connection's dbname as well
    (this.connection as any).config.dbname = pattern;
    Logger.debug('index pattern set to: %s', pattern);
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
// Exports – exactly mirroring the PostgreSQL/Hive/Impala inspector
// ---------------------------------------------------------------------------
export {
  ElasticsearchSchemaInspector,
  ElasticsearchConnection,
  TableInfo,
  Logger,
  ElasticsearchConnectionError,
  ElasticsearchQueryError,
  getErrorMessage,
  isError,
};

export type {
  ColumnInfo,
  DatabaseConfig,
  QueryResult,
};

export default ElasticsearchSchemaInspector;