/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Enhanced CouchDB Schema Inspector with Query Execution
 * Comprehensive schema inspection with robust error handling and connection management
 * TypeScript implementation optimized for DatabaseMetadataWizard integration
 *
 * This module mirrors the PostgreSQL inspector design and adapts it for CouchDB,
 * treating each CouchDB database as a "table" (collection) and documents as rows.
 */

import nano, { DocumentScope, MangoQuery, MangoSelector } from 'nano';

// CouchDB connection configuration
interface CouchDbConfig {
  /** URL of the CouchDB instance (e.g., 'http://admin:password@localhost:5984') */
  url: string;
  /** Database name to inspect (optional, can be set later) */
  database?: string;
  /** Additional nano options */
  requestDefaults?: any;
}

// Enhanced Field information structure (analogous to ColumnInfo)
interface FieldInfo {
  name: string;
  type: string;               // Primary observed JSON type
  types?: string[];           // All observed types for this field
  nullable?: boolean;         // True if field is missing in some documents
  occurrence?: number;        // Estimated percentage of documents containing this field
  comment?: string;
}

// Enhanced Database information structure (analogous to TableInfo/CollectionInfo)
class DatabaseInfo {
  public documentCount?: number;
  public size?: string;               // Active database size on disk (if available)
  public dataSize?: number;           // Size of uncompressed data (bytes)
  public docDelCount?: number;        // Number of deleted documents
  public updateSeq?: string;          // Current update sequence
  public isSystem?: boolean;          // True for system databases (starts with '_')
  public indexes?: Array<{
    name: string;
    type: string;                     // 'json' for Mango indexes, 'special' for built-in
    fields?: Record<string, string>;  // e.g., {"name": "asc"}
    def?: MangoQuery;
  }>;

  constructor(
    public dbname: string,
    public fields: FieldInfo[] = [],
    public next: DatabaseInfo | null = null
  ) {}

  get num_fields(): number {
    return this.fields.length;
  }

  get field_names(): string[] {
    return this.fields.map(f => f.name);
  }

  get field_types(): string[] {
    return this.fields.map(f => f.type);
  }

  getField(name: string): FieldInfo | undefined {
    return this.fields.find(f => f.name === name);
  }

  hasField(name: string): boolean {
    return this.fields.some(f => f.name === name);
  }
}

// Query execution result structure (analogous to QueryResult)
interface CouchDbQueryResult {
  success: boolean;
  rows?: any[];
  rowCount?: number;
  executionTime?: number;
  error?: string;
  affectedRows?: number;
  command?: string;
  /** For document insertion/update responses */
  docId?: string;
  rev?: string;
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
 * Advanced logging utilities with log levels (identical to PostgreSQL implementation)
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
 * CouchDB-specific error classes (mirroring PostgreSQL/MongoDB equivalents)
 */
class CouchDbConnectionError extends Error {
  constructor(
    message: string,
    public code?: string,
    public originalError?: unknown
  ) {
    super(message);
    this.name = 'CouchDbConnectionError';
  }
}

class CouchDbQueryError extends Error {
  constructor(
    message: string,
    public query: string | object,
    public originalError?: unknown
  ) {
    super(message);
    this.name = 'CouchDbQueryError';
  }
}

/**
 * CouchDB Connection Manager (analogous to PostgreSQLConnection)
 */
class CouchDbConnection {
  private server: nano.ServerScope | null = null;
  private db: DocumentScope<any> | null = null;
  private isConnected: boolean = false;
  private readonly maxRetries: number = 3;

  constructor(private config: CouchDbConfig) {}

  /**
   * Initializes the connection to the CouchDB server and optionally a database
   */
  async connect(): Promise<void> {
    if (this.isConnected && this.server) {
      Logger.debug('connection already established');
      return;
    }

    let lastError: unknown = null;

    for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
      try {
        Logger.debug('connection attempt %d/%d', attempt, this.maxRetries);

        // Connect to server using nano
        this.server = nano({
          url: this.config.url,
          requestDefaults: this.config.requestDefaults || {},
        });

        // Verify connectivity by requesting server info
        await this.server.info();

        this.isConnected = true;
        Logger.info('successfully connected to CouchDB server (attempt %d)', attempt);

        // If a database name was provided, open it
        if (this.config.database) {
          await this.useDatabase(this.config.database);
        }

        return;
      } catch (error) {
        lastError = error;
        this.isConnected = false;
        this.server = null;
        this.db = null;

        Logger.warn('connection attempt %d failed: %s', attempt, getErrorMessage(error));

        if (attempt < this.maxRetries) {
          const backoffTime = Math.min(1000 * Math.pow(2, attempt - 1), 10000);
          Logger.debug('waiting %d ms before retry', backoffTime);
          await new Promise(resolve => setTimeout(resolve, backoffTime));
        }
      }
    }

    throw new CouchDbConnectionError(
      `Failed to connect to CouchDB server after ${this.maxRetries} attempts: ${getErrorMessage(lastError)}`,
      'ECONNFAILED',
      lastError
    );
  }

  /**
   * Switch to a specific database
   */
  async useDatabase(dbName: string): Promise<void> {
    if (!this.server) throw new CouchDbConnectionError('Not connected to server');
    this.db = this.server.db.use(dbName);
    // Verify the database exists by requesting its info
    try {
      await this.db.info();
      Logger.debug('using database "%s"', dbName);
    } catch (error) {
      throw new CouchDbConnectionError(`Database "${dbName}" not accessible: ${getErrorMessage(error)}`);
    }
  }

  /**
   * Check connection health using the server info endpoint
   */
  async checkHealth(): Promise<boolean> {
    if (!this.isConnected || !this.server) {
      return false;
    }

    try {
      await this.server.info();
      return true;
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
      healthy: this.isConnected ? undefined : false,
    };
  }

  /**
   * Execute a Mango query or raw HTTP request (mirrors PostgreSQL query method)
   *
   * @param query - A JSON string representing a Mango query, an object, or a database name for a simple find.
   * @param params - For simple find, the selector object; parsed from JSON if string.
   *                For Mango query, optional array parameters (not natively supported in Mango,
   *                but we can pass additional options like limit, etc.)
   * @param options - Additional options (limit, skip, sort, fields, etc.)
   */
  async query(
    query: string | object,
    params?: any,
    options?: {
      limit?: number;
      skip?: number;
      sort?: any[];
      fields?: string[];
      autoDisconnect?: boolean;  // not used here, managed externally
    }
  ): Promise<CouchDbQueryResult> {
    if (!this.isConnected || !this.server) {
      throw new CouchDbConnectionError('Database not connected');
    }

    let mangoQuery: MangoQuery;
    let db: DocumentScope<any>;

    // Interpret query
    if (typeof query === 'string') {
      if (query.trim().startsWith('{')) {
        // Assume it's a JSON Mango query
        try {
          mangoQuery = JSON.parse(query);
        } catch (error) {
          throw new CouchDbQueryError('Failed to parse Mango query JSON', query, error);
        }
        // If query has a "database" field, use that, else must have db set already
        const specifiedDb = (mangoQuery as any).database;
        if (specifiedDb) {
          db = this.server.db.use(specifiedDb);
          delete (mangoQuery as any).database;
        } else if (this.db) {
          db = this.db;
        } else {
          throw new CouchDbQueryError('No database specified and no default database set', query);
        }
      } else {
        // Interpret as database name for a simple find
        const dbName = query.trim();
        db = this.server.db.use(dbName);
        // Build a simple Mango query with selector from params
        const selector: MangoSelector = params && typeof params === 'object' ? params : {};
        mangoQuery = {
          selector,
          limit: options?.limit,
          skip: options?.skip,
          sort: options?.sort,
          fields: options?.fields,
        };
      }
    } else if (typeof query === 'object') {
      // It's an object, assume MangoQuery
      mangoQuery = query as MangoQuery;
      if (!this.db) throw new CouchDbQueryError('No default database set', query);
      db = this.db;
    } else {
      throw new CouchDbQueryError('Invalid query type', query);
    }

    // Execute the Mango query
    try {
      Logger.debug('executing CouchDB query: %j', mangoQuery);
      const startTime = Date.now();
      const result = await db.find(mangoQuery);
      const duration = Date.now() - startTime;

      Logger.debug('query completed in %d ms, %d docs returned', duration, result.docs.length);

      return {
        success: true,
        rows: result.docs,
        rowCount: result.docs.length,
        executionTime: duration,
        command: 'find',
      };
    } catch (error) {
      if (this.isConnectionError(error)) {
        this.isConnected = false;
        Logger.warn('connection lost during query');
      }

      throw new CouchDbQueryError(
        `Query execution failed: ${getErrorMessage(error)}`,
        mangoQuery,
        error
      );
    }
  }

  /**
   * Insert a document into a database (returns doc id and rev)
   */
  async insert(
    doc: any,
    dbName?: string
  ): Promise<{ id: string; rev: string }> {
    if (!this.server) throw new CouchDbConnectionError('Not connected');
    const targetDb = dbName ? this.server.db.use(dbName) : this.db;
    if (!targetDb) throw new CouchDbConnectionError('No database specified');
    try {
      const response = await targetDb.insert(doc);
      return { id: response.id, rev: response.rev };
    } catch (error) {
      throw new CouchDbQueryError('Insert failed', doc, error);
    }
  }

  /**
   * Get a document by ID
   */
  async getDocument(
    id: string,
    dbName?: string
  ): Promise<any> {
    if (!this.server) throw new CouchDbConnectionError('Not connected');
    const targetDb = dbName ? this.server.db.use(dbName) : this.db;
    if (!targetDb) throw new CouchDbConnectionError('No database specified');
    try {
      return await targetDb.get(id);
    } catch (error) {
      throw new CouchDbQueryError(`Failed to get document ${id}`, { id }, error);
    }
  }

  /**
   * Check if error is connection-level
   */
  private isConnectionError(error: unknown): boolean {
    const errorMessage = getErrorMessage(error).toLowerCase();
    const keywords = ['econnrefused', 'econnreset', 'socket', 'timeout', 'network',
                     'connection', 'connect', 'closed', 'unreachable'];
    return keywords.some(kw => errorMessage.includes(kw));
  }

  /**
   * Close the connection (no-op for nano, as it's HTTP, but we reset state)
   */
  async disconnect(): Promise<void> {
    if (this.server) {
      Logger.debug('closing CouchDB connection (resetting state)');
      this.isConnected = false;
      this.server = null;
      this.db = null;
      Logger.info('connection closed');
    }
  }

  /**
   * Get the underlying server instance
   */
  getServer(): nano.ServerScope | null {
    return this.server;
  }

  /**
   * Get current database scope
   */
  getDb(): DocumentScope<any> | null {
    return this.db;
  }

  /**
   * Get connection configuration (without password? URL may contain credentials, be careful)
   */
  getConfig(): CouchDbConfig {
    return { ...this.config, url: '***' }; // mask URL for safety
  }
}

/**
 * Enhanced CouchDB Schema Inspector with Query Execution
 * Comprehensive metadata extraction with robust error handling
 */
class CouchDbSchemaInspector {
  private connection: CouchDbConnection;
  private currentDatabase: string | null;

  constructor(config: CouchDbConfig) {
    this.connection = new CouchDbConnection(config);
    this.currentDatabase = config.database || null;
  }

  /**
   * Set log level for debugging
   */
  static setLogLevel(level: 'DEBUG' | 'INFO' | 'WARN' | 'ERROR'): void {
    Logger.setLogLevel(level);
  }

  /**
   * Validate connection configuration
   */
  static validateConnectionConfig(config: CouchDbConfig): string[] {
    const errors: string[] = [];
    if (!config.url || config.url.trim() === '') {
      errors.push('URL is required');
    }
    return errors;
  }

  /**
   * Test connection without full schema inspection
   */
  async testConnection(): Promise<{ success: boolean; version?: string; error?: string }> {
    try {
      await this.connection.connect();
      const server = this.connection.getServer();
      if (server) {
        const info = await server.info();
        Logger.info('connection test successful - version: %s', info.version);
        return { success: true, version: info.version };
      }
      return { success: false, error: 'No server instance' };
    } catch (error) {
      const errorMessage = getErrorMessage(error);
      Logger.error('connection test failed: %s', errorMessage);
      return { success: false, error: errorMessage };
    } finally {
      await this.connection.disconnect();
    }
  }

  /**
   * Executes an arbitrary CouchDB query (Mango or simple find) against the server.
   *
   * @param sql - A JSON string representing a Mango query, a database name (for simple find), or a MangoQuery object.
   * @param params - For simple find, the selector object; for Mango, options like limit/skip/sort.
   * @param options - Additional options (limit, etc.)
   */
  async executeQuery(
    sql: string | object,
    params?: any,
    options?: {
      limit?: number;
      autoDisconnect?: boolean;
    }
  ): Promise<CouchDbQueryResult> {
    const startTime = Date.now();
    const { autoDisconnect = false } = options || {};

    try {
      await this.connection.connect();

      // Convert params if it's a string for database name (legacy usage)
      let queryOption = options;
      let queryParams = params;
      if (typeof sql === 'string' && !sql.trim().startsWith('{') && params === undefined) {
        // No params: might be just database name
        // but we treat as simple find with empty selector
        queryParams = {};
      }

      const result = await this.connection.query(sql, queryParams, {
        ...queryOption,
        limit: options?.limit,
      });

      return result;
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

  /**
   * Execute a query and auto-disconnect
   */
  async executeQueryAndDisconnect(sql: string | object, params?: any): Promise<CouchDbQueryResult> {
    return this.executeQuery(sql, params, { autoDisconnect: true });
  }

  /**
   * Execute multiple Mango queries sequentially (CouchDB does not support multi-document transactions natively)
   */
  async executeTransaction(queries: Array<{ sql: string | object; params?: any }>): Promise<CouchDbQueryResult[]> {
    await this.connection.connect();
    const results: CouchDbQueryResult[] = [];

    for (let i = 0; i < queries.length; i++) {
      const { sql, params } = queries[i];
      try {
        const res = await this.executeQuery(sql, params);
        results.push(res);
        if (!res.success) break;
      } catch (error) {
        results.push({
          success: false,
          error: getErrorMessage(error),
          executionTime: 0,
          rows: [],
          rowCount: 0,
        });
        break;
      }
    }
    return results;
  }

  /**
   * Retrieves all non-system databases with schema inference (analogous to getTables/getCollections)
   */
  async getDatabases(): Promise<DatabaseInfo[]> {
    await this.connection.connect();
    const server = this.connection.getServer();
    if (!server) throw new CouchDbConnectionError('Not connected');

    try {
      const allDbs = await server.db.list();
      Logger.info('found %d databases total', allDbs.length);

      const databases: DatabaseInfo[] = [];

      for (const dbName of allDbs) {
        const dbInfo = new DatabaseInfo(dbName);
        dbInfo.isSystem = dbName.startsWith('_');

        // Retrieve database info (document count, data size, etc.)
        try {
          const db = server.db.use(dbName);
          const info = await db.info();
          dbInfo.documentCount = info.doc_count;
          dbInfo.docDelCount = info.doc_del_count;
          dbInfo.updateSeq = info.update_seq;
          dbInfo.size = this.formatBytes(info.disk_size || 0);
          dbInfo.dataSize = info.data_size;
        } catch (error) {
          Logger.warn('failed to get info for database %s: %s', dbName, getErrorMessage(error));
        }

        // Retrieve indexes (design documents and Mango indexes)
        try {
          const db = server.db.use(dbName);
          const indexes = await db.index();
          if (indexes && indexes.indexes) {
            dbInfo.indexes = indexes.indexes.map((idx: any) => ({
              name: idx.name,
              type: idx.type,
              fields: idx.def?.fields,
              def: idx.def,
            }));
          }
        } catch (error) {
          Logger.warn('failed to get indexes for %s: %s', dbName, getErrorMessage(error));
        }

        // Infer fields by sampling documents
        await this.inferDatabaseFields(dbInfo);

        databases.push(dbInfo);
      }

      return databases;
    } catch (error) {
      Logger.error('Failed to retrieve databases: %s', getErrorMessage(error));
      throw error;
    }
  }

  /**
   * Infer field metadata by sampling documents
   */
  private async inferDatabaseFields(database: DatabaseInfo): Promise<void> {
    const server = this.connection.getServer();
    if (!server) return;

    try {
      const db = server.db.use(database.dbname);
      // Use _all_docs with include_docs to get a sample (up to 1000 docs)
      const sampleSize = 1000;
      const result = await db.list({ include_docs: true, limit: sampleSize });
      const docs = result.rows.map(row => row.doc);

      if (!docs || docs.length === 0) {
        database.fields = [];
        return;
      }

      const fieldStats: Record<string, { types: Set<string>; count: number }> = {};

      for (const doc of docs) {
        // Skip design docs
        if (doc._id && doc._id.startsWith('_design/')) continue;
        this.flattenFields(doc, '', fieldStats);
      }

      const fields: FieldInfo[] = [];
      for (const [path, stats] of Object.entries(fieldStats)) {
        const typesArray = Array.from(stats.types);
        fields.push({
          name: path,
          type: typesArray[0] || 'unknown',
          types: typesArray.length > 1 ? typesArray : undefined,
          nullable: stats.count < docs.length,
          occurrence: (stats.count / docs.length) * 100,
        });
      }

      database.fields = fields;
      Logger.debug('inferred %d fields for database %s', fields.length, database.dbname);
    } catch (error) {
      Logger.error('field inference failed for database %s: %s', database.dbname, getErrorMessage(error));
    }
  }

  /**
   * Flatten a JSON document to path->type mapping (recursive)
   */
  private flattenFields(
    obj: any,
    prefix: string,
    stats: Record<string, { types: Set<string>; count: number }>
  ): void {
    // Ignore special CouchDB fields _id and _rev for schema purposes? (optional)
    // We'll still include them as they are important. Alternatively, remove them.
    if (obj === null || typeof obj !== 'object' || obj instanceof Date) {
      const type = this.getJsonType(obj);
      const key = prefix || 'value';
      if (!stats[key]) {
        stats[key] = { types: new Set(), count: 0 };
      }
      stats[key].types.add(type);
      stats[key].count++;
      return;
    }

    if (Array.isArray(obj)) {
      const key = prefix || 'value';
      if (!stats[key]) {
        stats[key] = { types: new Set(), count: 0 };
      }
      stats[key].types.add('array');
      stats[key].count++;
      if (obj.length > 0) {
        this.flattenFields(obj[0], prefix, stats);
      }
      return;
    }

    // Object
    for (const [field, value] of Object.entries(obj)) {
      // Optionally skip _rev to avoid noise (but we'll keep it)
      const nestedKey = prefix ? `${prefix}.${field}` : field;
      this.flattenFields(value, nestedKey, stats);
    }
  }

  private getJsonType(value: any): string {
    if (value === null) return 'null';
    if (typeof value === 'string') return 'string';
    if (typeof value === 'number') return 'number';
    if (typeof value === 'boolean') return 'boolean';
    if (value instanceof Date) return 'date';
    if (Array.isArray(value)) return 'array';
    if (typeof value === 'object') return 'object';
    return 'unknown';
  }

  private formatBytes(bytes: number): string {
    if (bytes === 0) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    const exponent = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
    return (bytes / Math.pow(1024, exponent)).toFixed(2) + ' ' + units[exponent];
  }

  /**
   * Get specific database metadata by name
   */
  async getDatabase(dbName: string): Promise<DatabaseInfo | null> {
    const all = await this.getDatabases();
    return all.find(db => db.dbname === dbName) || null;
  }

  /**
   * Get server information
   */
  async getServerInfo(): Promise<{
    version: string;
    vendor: any;
    features: any;
    uuid: string;
  }> {
    await this.connection.connect();
    const server = this.connection.getServer();
    if (!server) throw new CouchDbConnectionError('Not connected');
    try {
      const info = await server.info();
      return {
        version: info.version,
        vendor: info.vendor,
        features: info.features,
        uuid: info.uuid,
      };
    } catch (error) {
      Logger.error('failed to get server info: %s', getErrorMessage(error));
      throw error;
    }
  }

  /**
   * Get the connection instance for external management
   */
  getConnection(): CouchDbConnection {
    return this.connection;
  }

  /**
   * Set default database
   */
  async setDatabase(dbName: string): Promise<void> {
    await this.connection.connect();
    await this.connection.useDatabase(dbName);
    this.currentDatabase = dbName;
    Logger.debug('current database set to: %s', dbName);
  }

  getCurrentDatabase(): string | null {
    return this.currentDatabase;
  }

  /**
   * Utility to flatten database list (analogous to flattenTableList)
   */
  static flattenDatabaseList(databases: DatabaseInfo | DatabaseInfo[]): DatabaseInfo[] {
    if (Array.isArray(databases)) {
      return databases;
    }
    const result: DatabaseInfo[] = [];
    let current: DatabaseInfo | null = databases;
    while (current) {
      result.push(current);
      current = current.next;
    }
    return result;
  }

  /**
   * Generate standardized metadata format
   */
  static toStandardizedFormat(databases: DatabaseInfo[]): any[] {
    return databases.map(db => ({
      dbname: db.dbname,
      isSystem: db.isSystem,
      documentCount: db.documentCount,
      size: db.size,
      dataSize: db.dataSize,
      fields: db.fields.map(field => ({
        name: field.name,
        type: field.type,
        types: field.types,
        nullable: field.nullable,
        occurrence: field.occurrence,
      })),
      indexes: db.indexes,
      originalData: db,
    }));
  }
}

export {
  CouchDbSchemaInspector,
  CouchDbConnection,
  DatabaseInfo,
  Logger,
  CouchDbConnectionError,
  CouchDbQueryError,
  getErrorMessage,
  isError,
};

export type {
  CouchDbConfig,
  FieldInfo,
  CouchDbQueryResult,
};

// Default export for convenience
export default CouchDbSchemaInspector;