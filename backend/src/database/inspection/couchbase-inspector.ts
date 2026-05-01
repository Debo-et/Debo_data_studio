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
 * Enhanced Couchbase Schema Inspector with Query Execution
 * Comprehensive schema inspection with robust error handling and connection management
 * TypeScript implementation optimized for DatabaseMetadataWizard integration
 *
 * This module mirrors the PostgreSQL inspector design and the MongoDB variant,
 * adapted for Couchbase's document-oriented, bucket/scope/collection architecture.
 */

import {
  Cluster,
  connect,
  QueryOptions,
  QueryResult as CBQueryResult,
  SearchQuery,
  SearchOptions,
} from 'couchbase';

// Couchbase connection configuration
interface CouchbaseConfig {
  /** Connection string (full connstr or simply host:port) */
  connectionString: string;
  /** Username for authentication */
  username: string;
  /** Password for authentication */
  password: string;
  /** Bucket to inspect (default: 'travel-sample') */
  bucket: string;
  /** Scope within the bucket (default: '_default') */
  scope?: string;
  /** Collection within the scope (default: '_default') */
  collection?: string;
}

// Enhanced Field information structure (analogous to ColumnInfo)
interface FieldInfo {
  name: string;
  type: string;               // Primary observed JSON type
  types?: string[];           // All observed JSON types (e.g., ['string', 'null'])
  nullable?: boolean;         // True if field is missing in some documents
  occurrence?: number;        // Estimated percentage of presence in sampled docs
  comment?: string;
}

// Enhanced Collection information structure (analogous to TableInfo)
class CollectionInfo {
  public documentCount?: number;
  public size?: string;       // Approximate data size (static from bucket stats if available)
  public isSystem?: boolean;
  public indexes?: Array<{ name: string; fields: string[]; isPrimary: boolean }>;

  constructor(
    public bucketName: string,
    public scopeName: string,
    public collectionName: string,
    public fields: FieldInfo[] = [],
    public next: CollectionInfo | null = null
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
interface CouchbaseQueryResult {
  success: boolean;
  rows?: any[];
  rowCount?: number;
  meta?: any;
  executionTime?: number;
  error?: string;
  affectedRows?: number;   // For mutations
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
 * Couchbase-specific error classes (mirroring PostgreSQL/MongoDB equivalents)
 */
class CouchbaseConnectionError extends Error {
  constructor(
    message: string,
    public code?: string,
    public originalError?: unknown
  ) {
    super(message);
    this.name = 'CouchbaseConnectionError';
  }
}

class CouchbaseQueryError extends Error {
  constructor(
    message: string,
    public query: string,
    public originalError?: unknown
  ) {
    super(message);
    this.name = 'CouchbaseQueryError';
  }
}

/**
 * Couchbase Connection Manager using Cluster (analogous to PostgreSQLConnection / MongoConnection)
 */
class CouchbaseConnection {
  private cluster: Cluster | null = null;
  private isConnected: boolean = false;
  private readonly maxRetries: number = 3;

  constructor(private config: CouchbaseConfig) {}

  /**
   * Initializes the connection to the Couchbase cluster
   */
  async connect(): Promise<void> {
    if (this.isConnected && this.cluster) {
      Logger.debug('connection already established');
      return;
    }

    let lastError: unknown = null;

    for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
      try {
        Logger.debug('connection attempt %d/%d', attempt, this.maxRetries);

        this.cluster = await connect(this.config.connectionString, {
          username: this.config.username,
          password: this.config.password,
          configProfile: 'wanDevelopment', // appropriate for most dev/test environments
        });

        // Verify connectivity by fetching a simple service status
        const pingResult = await this.cluster.ping();
        if (pingResult === undefined) {
          throw new Error('Ping returned undefined');
        }

        this.isConnected = true;
        Logger.info('successfully connected to cluster at "%s" (attempt %d)',
          this.config.connectionString, attempt);
        return;

      } catch (error) {
        lastError = error;
        this.isConnected = false;
        if (this.cluster) {
          await this.cluster.close().catch(() => {});
          this.cluster = null;
        }

        Logger.warn('connection attempt %d failed: %s', attempt, getErrorMessage(error));

        if (attempt < this.maxRetries) {
          const backoffTime = Math.min(1000 * Math.pow(2, attempt - 1), 10000);
          Logger.debug('waiting %d ms before retry', backoffTime);
          await new Promise(resolve => setTimeout(resolve, backoffTime));
        }
      }
    }

    throw new CouchbaseConnectionError(
      `Failed to connect to Couchbase cluster after ${this.maxRetries} attempts: ${getErrorMessage(lastError)}`,
      'ECONNFAILED',
      lastError
    );
  }

  /**
   * Check connection health with a cluster ping
   */
  async checkHealth(): Promise<boolean> {
    if (!this.isConnected || !this.cluster) {
      return false;
    }

    try {
      const pingResult = await this.cluster.ping();
      return pingResult !== undefined;
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
   * Execute a N1QL query (or a raw SQL++ string)
   * This mirrors the `query` method of PostgreSQL/MongoDB.
   */
  async query(
    statement: string,
    options?: QueryOptions
  ): Promise<CBQueryResult<any>> {
    if (!this.isConnected || !this.cluster) {
      throw new CouchbaseConnectionError('Database not connected');
    }

    try {
      Logger.debug('executing N1QL query: %s', statement.substring(0, 200));
      const startTime = Date.now();
      const result = await this.cluster.query(statement, options);
      const duration = Date.now() - startTime;
      Logger.debug('query completed in %d ms', duration);
      return result;
    } catch (error) {
      if (this.isConnectionError(error)) {
        this.isConnected = false;
        Logger.warn('connection lost during query');
      }

      const queryError = new CouchbaseQueryError(
        `Query execution failed: ${getErrorMessage(error)}`,
        statement,
        error
      );
      Logger.error('query failed: %s', getErrorMessage(error));
      throw queryError;
    }
  }

  /**
   * Check if the error is a connection-level error
   */
  private isConnectionError(error: unknown): boolean {
    const errorMessage = getErrorMessage(error).toLowerCase();
    const connectionErrors = [
      'timeout', 'connect', 'socket', 'network', 'closed',
      'access', 'auth', 'temporary', 'busy'
    ];
    return connectionErrors.some(keyword =>
      errorMessage.includes(keyword)
    );
  }

  /**
   * Close the cluster connection
   */
  async disconnect(): Promise<void> {
    if (this.cluster) {
      try {
        Logger.debug('closing Couchbase cluster connection');
        await this.cluster.close();
        Logger.info('connection closed successfully');
      } catch (error) {
        Logger.error('error closing connection: %s', getErrorMessage(error));
      } finally {
        this.isConnected = false;
        this.cluster = null;
      }
    }
  }

  /**
   * Get the underlying Cluster object
   */
  getCluster(): Cluster | null {
    return this.cluster;
  }

  /**
   * Get connection configuration (without password)
   */
  getConfig(): Omit<CouchbaseConfig, 'password'> {
    const { password, ...safeConfig } = this.config;
    return safeConfig;
  }

  /**
   * Get the bucket instance (needed for direct operations)
   */
  getBucket() {
    if (!this.cluster) return null;
    return this.cluster.bucket(this.config.bucket);
  }
}

/**
 * Enhanced Couchbase Schema Inspector with Query Execution
 * Comprehensive metadata extraction with robust error handling
 */
class CouchbaseSchemaInspector {
  private connection: CouchbaseConnection;
  private bucket: string;
  private scope: string;
  private collection: string;

  constructor(config: CouchbaseConfig) {
    this.connection = new CouchbaseConnection(config);
    this.bucket = config.bucket || 'travel-sample';
    this.scope = config.scope || '_default';
    this.collection = config.collection || '_default';
  }

  /**
   * Set log level for debugging
   */
  static setLogLevel(level: 'DEBUG' | 'INFO' | 'WARN' | 'ERROR'): void {
    Logger.setLogLevel(level);
  }

  /**
   * Validate connection configuration parameters
   */
  static validateConnectionConfig(config: CouchbaseConfig): string[] {
    const errors: string[] = [];

    if (!config.connectionString || config.connectionString.trim() === '') {
      errors.push('connectionString is required');
    }
    if (!config.username || config.username.trim() === '') {
      errors.push('username is required');
    }
    if (!config.password) {
      errors.push('password is required');
    }
    if (!config.bucket || config.bucket.trim() === '') {
      errors.push('bucket name is required');
    }

    return errors;
  }

  /**
   * Test connection without full schema inspection
   */
  async testConnection(): Promise<{ success: boolean; version?: string; error?: string }> {
    try {
      await this.connection.connect();

      // Run a simple query to retrieve cluster version info
      const result = await this.connection.query('SELECT 1 as test');

      // For version, run a separate query or read from cluster info
      const versionResult = await this.connection.query(
        'SELECT RAW MIN(v) FROM system:indexes LET v = SPLIT(version(), "-")[0]'
      );
      const version = versionResult.rows[0] || 'unknown';

      Logger.info('connection test successful - bucket: %s, version: %s',
        this.bucket, version);

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
   * Executes an arbitrary N1QL (SQL++) query against the Couchbase cluster.
   *
   * @param sql - The N1QL query string.
   * @param params - Positional parameters ($1, $2, ...) or named parameters.
   * @param options - Execution options (maxRows, timeout, autoDisconnect, etc.).
   */
  async executeQuery(
    sql: string,
    params: any[] = [],
    options: {
      maxRows?: number;
      timeout?: number;
      autoDisconnect?: boolean;
    } = {}
  ): Promise<CouchbaseQueryResult> {
    const startTime = Date.now();
    const { maxRows, autoDisconnect = false } = options;

    try {
      await this.connection.connect();

      // Build query options
      const queryOptions: QueryOptions = {
        parameters: params,
        timeout: options.timeout ? options.timeout * 1000 : undefined,
      };

      // If maxRows is specified, modify the query to LIMIT if not already present.
      let finalSql = sql;
      if (maxRows && !sql.toUpperCase().includes('LIMIT')) {
        finalSql = `${sql} LIMIT ${maxRows}`;
        Logger.debug('applied maxRows limit %d to query', maxRows);
      }

      Logger.debug('executing Couchbase query: %s', finalSql.substring(0, 200));
      const result = await this.connection.query(finalSql, queryOptions);
      const executionTime = Date.now() - startTime;

      const rows = result.rows;
      const rowCount = rows.length;

      return {
        success: true,
        rows,
        rowCount,
        meta: result.meta,
        executionTime,
        affectedRows: result.meta?.metrics?.mutationCount || 0,
      };
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
   * Execute a query with auto-disconnect (convenience)
   */
  async executeQueryAndDisconnect(sql: string, params: any[] = []): Promise<CouchbaseQueryResult> {
    return this.executeQuery(sql, params, { autoDisconnect: true });
  }

  /**
   * Execute multiple queries in a transaction (Couchbase transactions not fully supported yet,
   * so this runs queries sequentially without explicit ACID transaction).
   * For Couchbase 7.0+, you can use `transactions.run()`, but we stick to sequential execution for simplicity.
   */
  async executeTransaction(queries: Array<{ sql: string; params?: any[] }>): Promise<CouchbaseQueryResult[]> {
    await this.connection.connect();
    const results: CouchbaseQueryResult[] = [];

    for (let i = 0; i < queries.length; i++) {
      const { sql, params = [] } = queries[i];
      Logger.debug('executing transaction query %d/%d', i + 1, queries.length);
      const res = await this.executeQuery(sql, params, { autoDisconnect: false });
      results.push(res);
      if (!res.success) {
        Logger.error('transaction failed at query %d: %s', i + 1, res.error);
        break;
      }
    }

    return results;
  }

  /**
   * Retrieves all collections across all scopes within the configured bucket
   * (analogous to getTables / getCollections)
   */
  async getCollections(): Promise<CollectionInfo[]> {
    await this.connection.connect();
    const bucketObj = this.connection.getBucket();
    if (!bucketObj) throw new CouchbaseConnectionError('No bucket access');

    try {
      // Use query to list all scopes and collections
      const query = `SELECT s.name AS scope, c.name AS collection
                     FROM system:keyspaces AS k
                     UNNEST k.scopes AS s
                     UNNEST s.collections AS c
                     WHERE k.name = $1
                     ORDER BY s.name, c.name`;
      const result = await this.connection.query(query, {
        parameters: [this.bucket],
      });

      Logger.info('found %d collections in bucket "%s"', result.rows.length, this.bucket);

      const collections: CollectionInfo[] = [];

      for (const row of result.rows) {
        const scopeName = row.scope;
        const collectionName = row.collection;

        const collInfo = new CollectionInfo(this.bucket, scopeName, collectionName);
        collInfo.isSystem = scopeName.startsWith('_');

        // Enrich with document count and index info
        try {
          const docCountResult = await this.connection.query(
            `SELECT COUNT(*) AS cnt FROM \`${this.bucket}\`.\`${scopeName}\`.\`${collectionName}\``
          );
          collInfo.documentCount = docCountResult.rows[0]?.cnt || 0;
        } catch (e) {
          Logger.warn('failed to get document count for %s.%s.%s: %s',
            this.bucket, scopeName, collectionName, getErrorMessage(e));
        }

        // Fetch indexes
        try {
          const idxResult = await this.connection.query(
            `SELECT idx.name, idx.index_key AS fields, idx.is_primary
             FROM system:indexes AS idx
             WHERE idx.keyspace_id = $1 AND idx.bucket_id = $2 AND idx.scope_id = $3
             ORDER BY idx.name`,
            { parameters: [collectionName, this.bucket, scopeName] }
          );
          collInfo.indexes = idxResult.rows.map((r: any) => ({
            name: r.name,
            fields: r.fields,
            isPrimary: r.is_primary,
          }));
        } catch (e) {
          Logger.warn('failed to get indexes for %s.%s.%s: %s',
            this.bucket, scopeName, collectionName, getErrorMessage(e));
        }

        // Infer fields by sampling documents
        await this.inferCollectionFields(collInfo);

        collections.push(collInfo);
      }

      return collections;
    } catch (error) {
      Logger.error('Failed to retrieve collections: %s', getErrorMessage(error));
      throw error;
    }
  }

  /**
   * Infer field metadata by sampling documents (analogous to MongoDB's inferCollectionFields)
   */
  private async inferCollectionFields(collection: CollectionInfo): Promise<void> {
    try {
      // Sample up to 1000 documents
      const sampleSize = 1000;
      const col = this.connection.getBucket()
        ?.scope(collection.scopeName)
        ?.collection(collection.collectionName);
      if (!col) return;

      // A generic N1QL query to fetch up to sampleSize documents
      const query = `SELECT RAW d FROM \`${collection.bucketName}\`.\`${collection.scopeName}\`.\`${collection.collectionName}\` AS d LIMIT ${sampleSize}`;
      const result = await this.connection.query(query);
      const docs = result.rows;

      if (!docs || docs.length === 0) {
        collection.fields = [];
        return;
      }

      const fieldStats: Record<string, { types: Set<string>; count: number }> = {};

      for (const doc of docs) {
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

      collection.fields = fields;
      Logger.debug('inferred %d fields for collection %s.%s.%s',
        fields.length, collection.bucketName, collection.scopeName, collection.collectionName);
    } catch (error) {
      Logger.error('field inference failed for collection %s.%s.%s: %s',
        collection.bucketName, collection.scopeName, collection.collectionName, getErrorMessage(error));
    }
  }

  /**
   * Flatten a JSON document to path->types mapping
   */
  private flattenFields(
    obj: any,
    prefix: string,
    stats: Record<string, { types: Set<string>; count: number }>
  ): void {
    if (obj === null || typeof obj !== 'object' || obj instanceof Date) {
      // scalar
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
      // Optionally sample first element for element type inference (simplified)
      if (obj.length > 0) {
        this.flattenFields(obj[0], prefix, stats);
      }
      return;
    }

    // Object
    for (const [field, value] of Object.entries(obj)) {
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

  /**
   * Get specific collection metadata by name (within default scope)
   */
  async getCollection(collectionName: string, scopeName?: string): Promise<CollectionInfo | null> {
    const scope = scopeName || this.scope;
    const allCollections = await this.getCollections();
    return allCollections.find(
      c => c.scopeName === scope && c.collectionName === collectionName
    ) || null;
  }

  /**
   * Get database/cluster information
   */
  async getDatabaseInfo(): Promise<{
    version: string;
    clusterName: string;
    license: string;
    bucketCount: number;
  }> {
    await this.connection.connect();

    try {
      // Get cluster version
      const versionResult = await this.connection.query(
        'SELECT RAW MIN(SPLIT(version(), "-")[0]) FROM system:indexes'
      );
      const version = versionResult.rows[0] || 'unknown';

      // Get cluster name from system:pools_info? Not directly available via query, so we'll approximate.
      // Alternatively, use a simple config request.
      const cluster = this.connection.getCluster();
      const info = cluster ? await cluster.ping() : null;
      const clusterName = (cluster as any)?.configuration?.clusterName || 'unknown';

      const bucketResult = await this.connection.query(
        'SELECT RAW COUNT(*) FROM system:keyspaces WHERE `bucket` IS NOT MISSING'
      );
      const bucketCount = bucketResult.rows[0] || 0;

      return { version, clusterName, license: 'Enterprise', bucketCount };
    } catch (error) {
      Logger.error('failed to get database info: %s', getErrorMessage(error));
      throw error;
    }
  }

  /**
   * Get the connection instance for external management
   */
  getConnection(): CouchbaseConnection {
    return this.connection;
  }

  /**
   * Get current bucket / scope / collection
   */
  getCurrentContext(): { bucket: string; scope: string; collection: string } {
    return {
      bucket: this.bucket,
      scope: this.scope,
      collection: this.collection,
    };
  }

  /**
   * Utility to flatten list (analogous to flattenTableList / flattenCollectionList)
   */
  static flattenCollectionList(collections: CollectionInfo | CollectionInfo[]): CollectionInfo[] {
    if (Array.isArray(collections)) {
      return collections;
    }
    const result: CollectionInfo[] = [];
    let current: CollectionInfo | null = collections;
    while (current) {
      result.push(current);
      current = current.next;
    }
    return result;
  }

  /**
   * Generate standardized metadata format (analogous to toStandardizedFormat)
   */
  static toStandardizedFormat(collections: CollectionInfo[]): any[] {
    return collections.map(coll => ({
      bucketName: coll.bucketName,
      scopeName: coll.scopeName,
      collectionName: coll.collectionName,
      fields: coll.fields.map(field => ({
        name: field.name,
        type: field.type,
        types: field.types,
        nullable: field.nullable,
        occurrence: field.occurrence,
      })),
      documentCount: coll.documentCount,
      size: coll.size,
      indexes: coll.indexes,
      originalData: coll,
    }));
  }
}

export {
  CouchbaseSchemaInspector,
  CouchbaseConnection,
  CollectionInfo,
  Logger,
  CouchbaseConnectionError,
  CouchbaseQueryError,
  getErrorMessage,
  isError,
};

export type {
  CouchbaseConfig,
  FieldInfo,
  CouchbaseQueryResult,
};

// Default export for convenience
export default CouchbaseSchemaInspector;