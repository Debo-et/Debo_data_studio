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
 * Enhanced MongoDB Schema Inspector with Query Execution
 * Comprehensive schema inspection with robust error handling and connection management
 * TypeScript implementation optimized for DatabaseMetadataWizard integration
 *
 * This module mirrors the PostgreSQL inspector design and preserves the same
 * architectural patterns, adapted for MongoDB's document-oriented model.
 */

import { MongoClient, Db, Document, ClientSession, FindCursor } from 'mongodb';

// MongoDB connection configuration
interface MongoDatabaseConfig {
  /** Connection URI (supersedes individual host/port/credentials when provided) */
  uri?: string;
  host?: string;
  port?: number;
  user?: string;
  password?: string;
  /** Database name (required) */
  database: string;
  /** Authentication database (defaults to 'admin') */
  authSource?: string;
  /** Replica set name */
  replicaSet?: string;
  /** Enable TLS/SSL */
  ssl?: boolean;
  /** Additional MongoClient options */
  options?: Record<string, any>;
}

// Enhanced Field information structure (analogous to ColumnInfo)
interface FieldInfo {
  name: string;
  type: string;               // Primary BSON type (e.g., 'string', 'number', 'object')
  types?: string[];           // All observed BSON types for this field
  nullable?: boolean;         // True if field is missing in some documents
  /** Estimated percentage of occurrence in sampled documents */
  occurrence?: number;
  /** Any metadata or comment (not native to MongoDB, may come from sampling) */
  comment?: string;
  /** For array fields, the type of elements */
  arrayElementType?: string;
}

// Enhanced Collection information structure (analogous to TableInfo)
class CollectionInfo {
  public collectiontype: string = 'collection';
  public comment?: string;
  public rowCount?: number;      // Estimated document count
  public size?: string;          // Storage size in human-readable format
  public avgObjectSize?: number; // Average document size in bytes
  public isSystemCollection?: boolean;
  public indexes?: Array<{ name: string; key: Record<string, any>; unique?: boolean }>;

  constructor(
    public databasename: string,
    public collectionname: string,
    public fields: FieldInfo[] = [],
    public next: CollectionInfo | null = null
  ) {}

  get num_fields(): number {
    return this.fields.length;
  }

  /** List of field names (analogous to column_names in TableInfo) */
  get field_names(): string[] {
    return this.fields.map(f => f.name);
  }

  /** List of field primary types (analogous to column_types) */
  get field_types(): string[] {
    return this.fields.map(f => f.type);
  }

  /** Get field by name */
  getField(name: string): FieldInfo | undefined {
    return this.fields.find(f => f.name === name);
  }

  /** Check if a field exists */
  hasField(name: string): boolean {
    return this.fields.some(f => f.name === name);
  }
}

// Query execution result structure (analogous to QueryResult)
interface MongoQueryResult {
  success: boolean;
  /** Documents returned (for find/aggregate) */
  documents?: any[];
  /** Count of documents returned */
  rowCount?: number;
  /** Operation metadata */
  command?: string;
  executionTime?: number;
  error?: string;
  /** For write operations */
  affectedRows?: number;
  /** Inserted ID(s) if applicable */
  insertedIds?: any[];
}

// Utility function to safely extract error message (identical to PostgreSQL implementation)
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
 * MongoDB-specific error classes (mirroring PostgreSQL equivalents)
 */
class MongoDBConnectionError extends Error {
  constructor(
    message: string,
    public code?: string,
    public originalError?: unknown
  ) {
    super(message);
    this.name = 'MongoDBConnectionError';
  }
}

class MongoDBQueryError extends Error {
  constructor(
    message: string,
    public command: any,
    public originalError?: unknown
  ) {
    super(message);
    this.name = 'MongoDBQueryError';
  }
}

/**
 * MongoDB Connection Manager using MongoClient (analogous to PostgreSQLConnection)
 */
class MongoConnection {
  private client: MongoClient | null = null;
  private db: Db | null = null;
  private isConnected: boolean = false;
  private readonly maxRetries: number = 3;

  constructor(private config: MongoDatabaseConfig) {}

  /**
   * Build connection URI from config if not provided explicitly
   */
  private buildUri(): string {
    if (this.config.uri) {
      return this.config.uri;
    }
    const host = this.config.host || 'localhost';
    const port = this.config.port || 27017;
    const credentials = this.config.user && this.config.password
      ? `${encodeURIComponent(this.config.user)}:${encodeURIComponent(this.config.password)}@`
      : '';
    let uri = `mongodb://${credentials}${host}:${port}`;
    const params: string[] = [];
    if (this.config.authSource) {
      params.push(`authSource=${encodeURIComponent(this.config.authSource)}`);
    }
    if (this.config.replicaSet) {
      params.push(`replicaSet=${encodeURIComponent(this.config.replicaSet)}`);
    }
    if (this.config.ssl) {
      params.push('ssl=true');
    }
    if (params.length > 0) {
      uri += `/?${params.join('&')}`;
    }
    return uri;
  }

  /**
   * Initializes the connection and obtains the database handle
   */
  async connect(): Promise<void> {
    if (this.isConnected && this.client && this.db) {
      Logger.debug('connection already established');
      return;
    }

    let lastError: unknown = null;

    for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
      try {
        Logger.debug('connection attempt %d/%d', attempt, this.maxRetries);

        const uri = this.buildUri();
        this.client = new MongoClient(uri, {
          appName: 'schema_inspector',
          connectTimeoutMS: 15000,
          serverSelectionTimeoutMS: 15000,
          ...this.config.options
        });

        // Attach connection-level error handlers
        this.client.on('error', (err) => {
          Logger.error('Unexpected MongoClient error: %s', getErrorMessage(err));
        });

        await this.client.connect();

        this.db = this.client.db(this.config.database);
        // Verify connection with a ping command
        await this.db.command({ ping: 1 });

        this.isConnected = true;
        Logger.info('successfully connected to database "%s" (attempt %d)',
          this.config.database, attempt);
        return;

      } catch (error) {
        lastError = error;
        this.isConnected = false;
        if (this.client) {
          await this.client.close().catch(() => {});
          this.client = null;
          this.db = null;
        }

        Logger.warn('connection attempt %d failed: %s', attempt, getErrorMessage(error));

        if (attempt < this.maxRetries) {
          const backoffTime = Math.min(1000 * Math.pow(2, attempt - 1), 10000);
          Logger.debug('waiting %d ms before retry', backoffTime);
          await new Promise(resolve => setTimeout(resolve, backoffTime));
        }
      }
    }

    throw new MongoDBConnectionError(
      `Failed to connect to database "${this.config.database}" after ${this.maxRetries} attempts: ${getErrorMessage(lastError)}`,
      'ECONNFAILED',
      lastError
    );
  }

  /**
   * Check connection health using a ping command
   */
  async checkHealth(): Promise<boolean> {
    if (!this.isConnected || !this.db) {
      return false;
    }

    try {
      const result = await this.db.command({ ping: 1 });
      return result.ok === 1;
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
   * Execute a raw MongoDB command (analogous to query() in PostgreSQL)
   * Accepts a command document and optional options.
   */
  async executeCommand(
    command: Document,
    options?: { session?: ClientSession }
  ): Promise<any> {
    if (!this.isConnected || !this.db) {
      throw new MongoDBConnectionError('Database not connected');
    }

    try {
      Logger.debug('executing MongoDB command: %j', command);

      const startTime = Date.now();
      const result = await this.db.command(command, options);
      const duration = Date.now() - startTime;

      Logger.debug('command completed in %d ms', duration);
      return result;
    } catch (error) {
      if (this.isConnectionError(error)) {
        this.isConnected = false;
        Logger.warn('connection lost during command execution');
      }

      const queryError = new MongoDBQueryError(
        `Command execution failed: ${getErrorMessage(error)}`,
        command,
        error
      );

      Logger.error('command failed: %s', getErrorMessage(error));
      Logger.debug('failed command: %j', command);

      throw queryError;
    }
  }

  /**
   * Determine if error is a connection-level error
   */
  private isConnectionError(error: unknown): boolean {
    const errorMessage = getErrorMessage(error).toLowerCase();
    const connectionErrors = [
      'ECONNRESET', 'ECONNREFUSED', 'EPIPE', 'ETIMEDOUT',
      'connection', 'connect', 'socket', 'network', 'terminated',
      'mongoNetworkError', 'MongoNetworkError'
    ];

    return connectionErrors.some(connError =>
      errorMessage.includes(connError.toLowerCase())
    );
  }

  /**
   * Close the database connection
   */
  async disconnect(): Promise<void> {
    if (this.client) {
      try {
        Logger.debug('closing MongoDB connection');
        await this.client.close();
        Logger.info('connection closed successfully');
      } catch (error) {
        Logger.error('error closing connection: %s', getErrorMessage(error));
      } finally {
        this.isConnected = false;
        this.client = null;
        this.db = null;
      }
    }
  }

  /**
   * Get the underlying MongoClient (for advanced operations)
   */
  getClient(): MongoClient | null {
    return this.client;
  }

  /**
   * Get the underlying Db instance
   */
  getDb(): Db | null {
    return this.db;
  }

  /**
   * Get connection configuration (without password)
   */
  getConfig(): Omit<MongoDatabaseConfig, 'password'> {
    const { password, ...safeConfig } = this.config;
    return safeConfig;
  }
}

/**
 * Enhanced MongoDB Schema Inspector with Query Execution
 * Comprehensive metadata extraction with robust error handling
 */
class MongoSchemaInspector {
  private connection: MongoConnection;
  private database: string;

  constructor(config: MongoDatabaseConfig) {
    this.connection = new MongoConnection(config);
    this.database = config.database;
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
  static validateConnectionConfig(config: MongoDatabaseConfig): string[] {
    const errors: string[] = [];

    if (!config.database || config.database.trim() === '') {
      errors.push('Database name is required');
    }

    if (config.port !== undefined) {
      if (config.port < 1 || config.port > 65535) {
        errors.push('Port must be between 1 and 65535');
      }
    }

    if (config.uri && config.host) {
      errors.push('Both URI and host specified – please provide either uri or individual connection parameters');
    }

    return errors;
  }

  /**
   * Test connection without full schema inspection (analogous to PostgreSQL testConnection)
   */
  async testConnection(): Promise<{ success: boolean; version?: string; error?: string }> {
    try {
      await this.connection.connect();

      const buildInfo = await this.connection.executeCommand({ buildInfo: 1 });

      Logger.info('connection test successful - database: %s, version: %s',
        this.database, buildInfo.version);

      return {
        success: true,
        version: buildInfo.version
      };
    } catch (error) {
      const errorMessage = getErrorMessage(error);
      Logger.error('connection test failed: %s', errorMessage);
      return {
        success: false,
        error: errorMessage
      };
    } finally {
      await this.connection.disconnect();
    }
  }

  /**
   * Executes an arbitrary MongoDB query against the database.
   *
   * This method is designed to resemble the PostgreSQL executeQuery in signature,
   * but the nature of MongoDB queries is different. The `sql` parameter is
   * interpreted as a JSON string representing a MongoDB command document,
   * or an alternative string format for convenience:
   *   - If the string starts with '{', it is parsed as JSON to obtain a command.
   *   - Otherwise it is treated as a collection name, and `params` is used as
   *     filter (for a simple find). In that case `options` can contain projection,
   *     sort, etc.
   *
   * For complex queries (aggregations, write operations) it is recommended to
   * pass a JSON command string or use `executeTransaction`/`executeCommand` directly.
   *
   * @param sql - JSON command string or collection name
   * @param params - For simple find: filter document; for JSON command: ignored
   * @param options - extra options (maxRows, timeout, autoDisconnect, etc.)
   */
  async executeQuery(
    sql: string,
    params: any[] = [],
    options: {
      maxRows?: number;
      timeout?: number;
      autoDisconnect?: boolean;
    } = {}
  ): Promise<MongoQueryResult> {
    const startTime = Date.now();
    const { maxRows, autoDisconnect = false } = options;

    try {
      await this.connection.connect();
      const db = this.connection.getDb();
      if (!db) throw new MongoDBConnectionError('Database not connected');

      let commandDoc: Document;
      let collectionName = '';

      // Interpret the string
      if (sql.trim().startsWith('{')) {
        // JSON command
        try {
          commandDoc = JSON.parse(sql);
        } catch (parseError) {
          return {
            success: false,
            executionTime: Date.now() - startTime,
            error: `Failed to parse JSON command: ${getErrorMessage(parseError)}`
          };
        }
      } else {
        // Assume it's a collection name for a find operation
        collectionName = sql.trim();
        if (!collectionName) {
          return {
            success: false,
            executionTime: Date.now() - startTime,
            error: 'Collection name cannot be empty'
          };
        }
        const filter = params?.[0] ?? {};
        commandDoc = { find: collectionName, filter };
        if (params.length > 1) {
          // subsequent parameters could contain other find options, but for simplicity we ignore
          Logger.warn('additional params ignored in simple find; use JSON command for full control');
        }
      }

      Logger.debug('executing MongoDB operation: %j', commandDoc);

      let result: any;
      let documents: any[] = [];
      let rowCount = 0;

      // Perform operation
      if (commandDoc.find && !commandDoc.aggregate) {
        // find command
        const collName = commandDoc.find;
        const filter = commandDoc.filter || {};
        const projection = commandDoc.projection;
        const sort = commandDoc.sort;
        const limit = maxRows || commandDoc.limit || 0;
        const skip = commandDoc.skip || 0;
        const coll = db.collection(collName);

        let cursor: FindCursor = coll.find(filter).project(projection ?? {}).sort(sort ?? {}).skip(skip);
        if (limit > 0) cursor = cursor.limit(limit);

        documents = await cursor.toArray();
        rowCount = documents.length;
        result = { ok: 1, n: rowCount };
      } else if (commandDoc.aggregate) {
        // aggregation pipeline
        const collName = commandDoc.aggregate;
        const pipeline = commandDoc.pipeline || [];
        const coll = db.collection(collName);
        const cursor = coll.aggregate(pipeline);
        documents = await cursor.toArray();
        rowCount = documents.length;
        result = { ok: 1, n: rowCount };
      } else if (commandDoc.insert || commandDoc.update || commandDoc.delete) {
        // Write operations via generic command – not all write ops are supported
        // via db.command; for safety we'll delegate to collection methods for insert/update/delete
        // This path is a bit limited; for full write support use executeTransaction.
        // For now we'll run the raw command and hope the server understands (some write commands work)
        result = await db.command(commandDoc);
        rowCount = result.n || 0;
      } else {
        // Generic command (e.g., ping, buildInfo, etc.)
        result = await db.command(commandDoc);
        documents = result?.cursor?.firstBatch || [];
        rowCount = documents.length;
      }

      const executionTime = Date.now() - startTime;

      return {
        success: true,
        documents,
        rowCount,
        executionTime,
        command: commandDoc?.find || commandDoc?.aggregate || commandDoc?.insert,
        affectedRows: rowCount
      };
    } catch (error) {
      const executionTime = Date.now() - startTime;
      return {
        success: false,
        executionTime,
        error: getErrorMessage(error),
        documents: [],
        rowCount: 0
      };
    } finally {
      if (autoDisconnect) {
        await this.connection.disconnect();
      }
    }
  }

  /**
   * Execute a query with auto-disconnect (convenience method)
   */
  async executeQueryAndDisconnect(sql: string, params: any[] = []): Promise<MongoQueryResult> {
    return this.executeQuery(sql, params, { autoDisconnect: true });
  }

  /**
   * Execute multiple operations in a MongoDB transaction
   * Each query entry should be a JSON string command or a simple find string.
   */
  async executeTransaction(queries: Array<{ sql: string; params?: any[] }>): Promise<MongoQueryResult[]> {
    await this.connection.connect();
    const client = this.connection.getClient();
    if (!client) throw new MongoDBConnectionError('No client available');

    const session = client.startSession();
    const results: MongoQueryResult[] = [];

    try {
      await session.withTransaction(async () => {
        for (let i = 0; i < queries.length; i++) {
          const { sql, params = [] } = queries[i];
          Logger.debug('executing transaction operation %d/%d', i + 1, queries.length);

          // Reuse executeQuery logic but within the same session (by using underlying executeCommand)
          const startTime = Date.now();
          try {
            let commandDoc: Document;
            if (sql.trim().startsWith('{')) {
              commandDoc = JSON.parse(sql);
            } else {
              // simple find string
              const filter = params?.[0] ?? {};
              commandDoc = { find: sql, filter };
            }

            const result = await this.connection.executeCommand(commandDoc, { session });
            const executionTime = Date.now() - startTime;
            results.push({
              success: true,
              documents: result?.cursor?.firstBatch || [],
              rowCount: result.n || 0,
              executionTime,
              command: commandDoc.find
            });
          } catch (error) {
            results.push({
              success: false,
              executionTime: Date.now() - startTime,
              error: getErrorMessage(error)
            });
            throw error; // abort transaction
          }
        }
      });
    } catch (error) {
      Logger.error('transaction failed: %s', getErrorMessage(error));
    } finally {
      await session.endSession();
    }

    return results;
  }

  /**
   * Retrieves all collections from the database with comprehensive metadata
   * (analogous to getTables)
   */
  async getCollections(): Promise<CollectionInfo[]> {
    await this.connection.connect();
    const db = this.connection.getDb();
    if (!db) throw new MongoDBConnectionError('Database not connected');

    try {
      const collectionsCursor = db.listCollections();
      const collections = await collectionsCursor.toArray();

      Logger.info('found %d collections in database "%s"', collections.length, this.database);

      const collectionInfos: CollectionInfo[] = [];

      for (const coll of collections) {
        const collName = coll.name;
        const isSystem = collName.startsWith('system.') || collName === 'system';

        const info = new CollectionInfo(this.database, collName);
        info.collectiontype = coll.type === 'view' ? 'view' : 'collection';
        info.isSystemCollection = isSystem;
        info.comment = (coll as any).comment; // rare, but may exist

        // Get storage stats
        try {
          const stats = await db.command({ collStats: collName });
          info.size = this.formatBytes(stats.size);
          info.rowCount = stats.count;
          info.avgObjectSize = stats.avgObjSize;
        } catch (statsError) {
          Logger.warn('failed to get stats for collection %s: %s', collName, getErrorMessage(statsError));
        }

        // Get index information
        try {
          const indexesResult = await db.collection(collName).indexes();
          info.indexes = indexesResult.map((idx: any) => ({
            name: idx.name,
            key: idx.key,
            unique: idx.unique || false
          }));
        } catch (idxError) {
          Logger.warn('failed to get indexes for %s: %s', collName, getErrorMessage(idxError));
        }

        // Infer fields by sampling documents
        await this.inferCollectionFields(info);

        collectionInfos.push(info);
      }

      return collectionInfos;
    } catch (error) {
      Logger.error('Failed to retrieve collections: %s', getErrorMessage(error));
      throw error;
    }
  }

  /**
   * Infer field metadata by sampling documents (analogous to getTableColumns)
   * This is an approximation because MongoDB is schema-less.
   */
  private async inferCollectionFields(collection: CollectionInfo): Promise<void> {
    const db = this.connection.getDb();
    if (!db) return;

    try {
      const coll = db.collection(collection.collectionname);
      // Sample up to 1000 documents (configurable)
      const sampleSize = 1000;
      const docs = await coll.find().limit(sampleSize).toArray();

      if (docs.length === 0) {
        collection.fields = [];
        return;
      }

      const fieldStats: Record<string, {
        types: Set<string>;
        count: number;
        isArrayElement?: boolean;
        elementType?: string;
      }> = {};

      for (const doc of docs) {
        this.flattenFields(doc, '', fieldStats);
      }

      // Build FieldInfo[]
      const fields: FieldInfo[] = [];
      for (const [path, stats] of Object.entries(fieldStats)) {
        // Determine primary type (the most frequent)
        // For simplicity we'll take the first type, or you could count frequencies.
        // Here we just record all observed types.
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
      Logger.debug('inferred %d fields for collection %s', fields.length, collection.collectionname);
    } catch (error) {
      Logger.error('field inference failed for collection %s: %s',
        collection.collectionname, getErrorMessage(error));
    }
  }

  /**
   * Recursively flatten document fields into a stats map.
   */
  private flattenFields(
    obj: any,
    prefix: string,
    stats: Record<string, { types: Set<string>; count: number }>
  ): void {
    if (obj === null || typeof obj !== 'object' || obj instanceof Date || obj instanceof ObjectId) {
      // scalar value
      const type = this.getBsonType(obj);
      const key = prefix || 'value';
      if (!stats[key]) {
        stats[key] = { types: new Set(), count: 0 };
      }
      stats[key].types.add(type);
      stats[key].count++;
      return;
    }

    if (Array.isArray(obj)) {
      // Array: mark the field as array and recursively process items
      const key = prefix || 'value';
      if (!stats[key]) {
        stats[key] = { types: new Set(), count: 0 };
      }
      stats[key].types.add('array');
      stats[key].count++;
      // Optionally examine first element to infer element type
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

  private getBsonType(value: any): string {
    if (value === null) return 'null';
    if (value instanceof Date) return 'date';
    if (value instanceof ObjectId) return 'objectid';
    if (typeof value === 'string') return 'string';
    if (typeof value === 'number') return 'number';
    if (typeof value === 'boolean') return 'boolean';
    if (Array.isArray(value)) return 'array';
    if (typeof value === 'object') return 'object';
    return typeof value;
  }

  private formatBytes(bytes: number): string {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }

  /**
   * Get specific collection metadata by name
   */
  async getCollection(collectionName: string): Promise<CollectionInfo | null> {
    const allCollections = await this.getCollections();
    return allCollections.find(coll => coll.collectionname === collectionName) || null;
  }

  /**
   * Get database information
   */
  async getDatabaseInfo(): Promise<{
    name: string;
    version: string;
    storageEngine?: string;
    collections: number;
  }> {
    await this.connection.connect();

    try {
      const buildInfo = await this.connection.executeCommand({ buildInfo: 1 });
      const dbStats = await this.connection.executeCommand({ dbStats: 1 });

      return {
        name: this.database,
        version: buildInfo.version,
        storageEngine: dbStats.storageEngine?.name,
        collections: dbStats.collections || 0
      };
    } catch (error) {
      Logger.error('failed to get database info: %s', getErrorMessage(error));
      throw error;
    }
  }

  /**
   * Get the connection instance for external management
   */
  getConnection(): MongoConnection {
    return this.connection;
  }

  /**
   * Get current database name
   */
  getCurrentDatabase(): string {
    return this.database;
  }

  /**
   * Utility method to convert collection list to flat array (analogous to flattenTableList)
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
    return collections.map(collection => ({
      databasename: collection.databasename,
      collectionname: collection.collectionname,
      collectiontype: collection.collectiontype,
      fields: collection.fields.map(field => ({
        name: field.name,
        type: field.type,
        types: field.types,
        nullable: field.nullable,
        occurrence: field.occurrence
      })),
      comment: collection.comment,
      rowCount: collection.rowCount,
      size: collection.size,
      avgObjectSize: collection.avgObjectSize,
      indexes: collection.indexes,
      originalData: collection
    }));
  }
}

// Import ObjectId for BSON type detection (needed in flattenFields)
import { ObjectId } from 'mongodb';

export {
  MongoSchemaInspector,
  MongoConnection,
  CollectionInfo,
  Logger,
  MongoDBConnectionError,
  MongoDBQueryError,
  getErrorMessage,
  isError
};

export type {
  MongoDatabaseConfig,
  FieldInfo,
  MongoQueryResult
};

// Default export for convenience
export default MongoSchemaInspector;