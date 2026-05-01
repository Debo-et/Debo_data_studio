// backend/src/database/adapters/mongo.adapter.ts

import { IBaseDatabaseInspector } from '../inspection/base-inspector';
import {
  DatabaseConnection,
  QueryResult,
  TableInfo,
  ColumnMetadata,
  DatabaseConfig,
  InspectionOptions,
  QueryExecutionOptions,
  DatabaseVersionInfo
} from '../types/inspection.types';

// ------------------------------------------------------------------
// Internal MongoDB connection wrapper (can be moved to its own module)
// ------------------------------------------------------------------
import { MongoClient, Db, Collection, Document, ServerApiVersion } from 'mongodb';

class MongoDBConnection {
  private client: MongoClient;
  private db: Db | null = null;

  constructor(private uri: string, private dbName: string) {
    this.client = new MongoClient(uri, {
      serverApi: { version: ServerApiVersion.v1, strict: true, deprecationErrors: true },
      // Additional options can be passed via connect
    });
  }

  async connect(): Promise<void> {
    try {
      await this.client.connect();
      this.db = this.client.db(this.dbName);
    } catch (err) {
      throw new Error(`MongoDB connection failed: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  async disconnect(): Promise<void> {
    await this.client.close();
  }

  get nativeClient(): MongoClient {
    return this.client;
  }

  get nativeDb(): Db {
    if (!this.db) throw new Error('Not connected yet');
    return this.db;
  }

  // For the adapter, we expose a dummy connection object that conforms to DatabaseConnection
  toBase(): DatabaseConnection {
    return {
      // Minimal placeholder; the actual driver is accessed internally
      _connection: this,
    } as unknown as DatabaseConnection;
  }
}

// ------------------------------------------------------------------
// MongoDB Adapter (fully mirrors the PostgreSQL adapter pattern)
// ------------------------------------------------------------------
export class MongoDBAdapter implements IBaseDatabaseInspector {
  private connectionInstance: MongoDBConnection | null = null;

  /**
   * Connect to a MongoDB database.
   * The config object uses `dbname` for the database name.
   * A connection string is built from host/port/user/password, or a full URI can be supplied via `config.host`.
   */
  async connect(config: DatabaseConfig): Promise<DatabaseConnection> {
    try {
      // Build connection URI from config fields (similar logic as the hypothetical inspector)
      const host = config.host || 'localhost';
      const port = config.port || '27017';
      const user = config.user || '';
      const password = config.password || '';
      const dbName = config.dbname || 'admin';

      let uri: string;
      if (host.startsWith('mongodb://') || host.startsWith('mongodb+srv://')) {
        uri = host; // Full URI provided
      } else {
        const credentials = user ? `${encodeURIComponent(user)}:${encodeURIComponent(password)}@` : '';
        uri = `mongodb://${credentials}${host}:${port}/${dbName}?authSource=${config.schema || 'admin'}`;
      }

      this.connectionInstance = new MongoDBConnection(uri, dbName);
      await this.connectionInstance.connect();

      return this.connectionInstance.toBase();
    } catch (error) {
      throw new Error(`Failed to connect to MongoDB: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Disconnect from the MongoDB server.
   */
  async disconnect(_connection: DatabaseConnection): Promise<void> {
    try {
      if (this.connectionInstance) {
        await this.connectionInstance.disconnect();
      }
      this.connectionInstance = null;
    } catch (error) {
      console.error('Error disconnecting from MongoDB:', error);
    }
  }

  /**
   * Test connection without full database inspection.
   */
  async testConnection(config: DatabaseConfig): Promise<{ success: boolean; version?: string; error?: string }> {
    let testConnectionInstance: MongoDBConnection | null = null;
    try {
      const host = config.host || 'localhost';
      const port = config.port || '27017';
      const user = config.user || '';
      const password = config.password || '';
      const dbName = config.dbname || 'admin';

      let uri: string;
      if (host.startsWith('mongodb://') || host.startsWith('mongodb+srv://')) {
        uri = host;
      } else {
        const credentials = user ? `${encodeURIComponent(user)}:${encodeURIComponent(password)}@` : '';
        uri = `mongodb://${credentials}${host}:${port}/${dbName}?authSource=${config.schema || 'admin'}`;
      }

      testConnectionInstance = new MongoDBConnection(uri, dbName);
      await testConnectionInstance.connect();

      // Run server status to get version
      const adminDb = testConnectionInstance.nativeClient.db('admin');
      const serverInfo = await adminDb.command({ buildInfo: 1 });
      const version = serverInfo?.version;

      await testConnectionInstance.disconnect();

      return {
        success: true,
        version,
      };
    } catch (error) {
      if (testConnectionInstance) {
        try { await testConnectionInstance.disconnect(); } catch (_) { /* ignore */ }
      }
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error),
      };
    }
  }

  /**
   * Get collections as tables.
   * Each collection is sampled (first 1 doc) to infer field names and types.
   */
  async getTables(_connection: DatabaseConnection, options?: InspectionOptions): Promise<TableInfo[]> {
    if (!this.connectionInstance) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    try {
      const db = this.connectionInstance.nativeDb;
      const schema = options?.schema; // In MongoDB context, this could be used to switch database
      let targetDb = db;
      if (schema) {
        targetDb = this.connectionInstance.nativeClient.db(schema);
      }

      const collections = await targetDb.listCollections().toArray();
      const tables: TableInfo[] = [];

      for (const col of collections) {
        const collection = targetDb.collection(col.name);
        let columns: ColumnMetadata[] = [];
        // Sample one document to guess fields (if collection not empty)
        try {
          const sampleDoc = await collection.findOne({}, { projection: { _id: 0 } });
          if (sampleDoc) {
            columns = Object.keys(sampleDoc).map((key, idx) => ({
              name: key,
              type: typeof sampleDoc[key],
              dataType: typeof sampleDoc[key],
              nullable: true,
              ordinalPosition: idx + 1,
            } as ColumnMetadata));
          }
        } catch (_) {
          // ignore sampling errors
        }
        // If no sample found, include minimal _id column
        if (columns.length === 0) {
          columns = [{
            name: '_id',
            type: 'ObjectId',
            dataType: 'ObjectId',
            nullable: false,
            ordinalPosition: 1
          } as ColumnMetadata];
        }

        tables.push({
          schemaname: targetDb.databaseName,
          tablename: col.name,
          tabletype: col.type === 'view' ? 'VIEW' : 'TABLE',
          columns,
          comment: (col as any).comment,
          rowCount: undefined, // Not immediately available
          size: undefined,
          originalData: col
        });
      }

      return tables;
    } catch (error) {
      throw new Error(`Failed to get MongoDB collections: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Columns are already embedded in getTables() – pass through.
   */
  async getTableColumns(_connection: DatabaseConnection, tables: TableInfo[]): Promise<TableInfo[]> {
    return tables;
  }

  /**
   * Database info via MongoDB's buildInfo and serverStatus.
   */
  async getDatabaseInfo(_connection: DatabaseConnection): Promise<DatabaseVersionInfo> {
    if (!this.connectionInstance) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    try {
      const adminDb = this.connectionInstance.nativeClient.db('admin');
      const [buildInfo, serverStatus] = await Promise.all([
        adminDb.command({ buildInfo: 1 }),
        adminDb.command({ serverStatus: 1 }),
      ]);

      return {
        version: buildInfo?.version || 'unknown',
        name: `MongoDB ${buildInfo?.version}`,
        encoding: undefined, // not applicable
        collation: undefined, // not applicable
      };
    } catch (error) {
      throw new Error(`Failed to get MongoDB info: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Execute a MongoDB query via a JSON command string.
   * Supports operations: find, aggregate, count, insert, update, delete.
   * The `sql` parameter is a JSON string with the following shape:
   * {
   *   "collection": "users",
   *   "action": "find",
   *   "filter": {},
   *   "projection": {},
   *   "sort": {},
   *   "limit": 10,
   *   "skip": 0
   * }
   * For aggregate, use { "collection": "...", "action": "aggregate", "pipeline": [] }
   * Returns QueryResult with `rows` containing documents.
   */
  async executeQuery(
    _connection: DatabaseConnection,
    sql: string,
    options?: QueryExecutionOptions
  ): Promise<QueryResult> {
    if (!this.connectionInstance) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    try {
      const db = this.connectionInstance.nativeDb;
      const queryObj = JSON.parse(sql);
      const collectionName = queryObj.collection;
      if (!collectionName) throw new Error('Missing "collection" in query JSON');

      const collection = db.collection(collectionName);
      const maxRows = options?.maxRows ?? 1000;
      let rows: any[] = [];

      switch (queryObj.action) {
        case 'find': {
          const cursor = collection
            .find(queryObj.filter || {})
            .project(queryObj.projection || {})
            .sort(queryObj.sort || {})
            .limit(Math.min(queryObj.limit || maxRows, maxRows))
            .skip(queryObj.skip || 0);
          rows = await cursor.toArray();
          break;
        }
        case 'aggregate': {
          const cursor = collection.aggregate(queryObj.pipeline || [], {
            maxTimeMS: options?.timeout,
          });
          rows = await cursor.toArray();
          break;
        }
        case 'count': {
          const count = await collection.countDocuments(queryObj.filter || {});
          rows = [{ count }];
          break;
        }
        case 'insert': {
          const docs = queryObj.documents;
          if (!Array.isArray(docs) || docs.length === 0) throw new Error('Missing "documents" array for insert');
          const result = await collection.insertMany(docs);
          rows = [{ insertedCount: result.insertedCount, insertedIds: result.insertedIds }];
          break;
        }
        case 'update': {
          const result = await collection.updateMany(queryObj.filter || {}, queryObj.update || {});
          rows = [{ matchedCount: result.matchedCount, modifiedCount: result.modifiedCount }];
          break;
        }
        case 'delete': {
          const result = await collection.deleteMany(queryObj.filter || {});
          rows = [{ deletedCount: result.deletedCount }];
          break;
        }
        default:
          throw new Error(`Unsupported MongoDB action: ${queryObj.action}`);
      }

      return {
        success: true,
        rows,
        fields: rows.length > 0 ? Object.keys(rows[0]) : [],
        rowCount: rows.length,
      };
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error),
        rows: [],
      };
    }
  }

  /**
   * Execute multiple queries in a MongoDB transaction.
   * Each query must be a JSON command (same as executeQuery).
   */
  async executeTransaction(
    _connection: DatabaseConnection,
    queries: Array<{ sql: string; params?: any[] }>
  ): Promise<QueryResult[]> {
    if (!this.connectionInstance) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    const client = this.connectionInstance.nativeClient;
    const session = client.startSession();
    try {
      session.startTransaction();
      const results: QueryResult[] = [];

      for (const q of queries) {
        // We pass the same _connection (though not used) and options empty
        const res = await this.executeQuery(_connection, q.sql);
        results.push(res);
        if (!res.success) {
          await session.abortTransaction();
          throw new Error(`Transaction aborted due to failure: ${res.error}`);
        }
      }

      await session.commitTransaction();
      return results;
    } catch (error) {
      try { await session.abortTransaction(); } catch (_) { /* ignore */ }
      throw new Error(`MongoDB transaction failed: ${error instanceof Error ? error.message : String(error)}`);
    } finally {
      session.endSession();
    }
  }

  /**
   * MongoDB does not enforce relational constraints; return an empty array.
   */
  async getTableConstraints(_connection: DatabaseConnection, _schema: string, _table: string): Promise<any[]> {
    return [];
  }

  /**
   * Get MongoDB databases (schemas).
   */
  async getSchemas(connection: DatabaseConnection): Promise<string[]> {
    if (!this.connectionInstance) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    try {
      const adminDb = this.connectionInstance.nativeClient.db('admin');
      const result = await adminDb.admin().listDatabases();
      return result.databases.map((db: any) => db.name);
    } catch (error) {
      console.error('Failed to get MongoDB databases:', error);
      return [this.connectionInstance.nativeDb.databaseName];
    }
  }

  /**
   * MongoDB has no UDFs; return empty array.
   */
  async getFunctions(connection: DatabaseConnection, database?: string): Promise<any[]> {
    return []; // No functions equivalent
  }

  /**
   * List indexes for a given collection (table).
   * If tableName is not provided, returns indexes for all collections in current db.
   */
  async getIndexes(connection: DatabaseConnection, tableName?: string): Promise<any[]> {
    if (!this.connectionInstance) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    try {
      const db = this.connectionInstance.nativeDb;
      if (tableName) {
        const indexes = await db.collection(tableName).indexes();
        return indexes.map(idx => ({
          ...idx,
          schema: db.databaseName,
          table: tableName,
        }));
      } else {
        const collections = await db.listCollections().toArray();
        let allIndexes: any[] = [];
        for (const col of collections) {
          const indexes = await db.collection(col.name).indexes();
          allIndexes = allIndexes.concat(
            indexes.map(idx => ({
              ...idx,
              schema: db.databaseName,
              table: col.name,
            }))
          );
        }
        return allIndexes;
      }
    } catch (error) {
      console.error('Failed to get MongoDB indexes:', error);
      return [];
    }
  }
}

export default MongoDBAdapter;