// backend/src/database/adapters/couchbase.adapter.ts

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

import { CouchbaseSchemaInspector, CouchbaseConnection } from '../inspection/couchbase-inspector';

/**
 * Couchbase Database Adapter
 *
 * Implements IBaseDatabaseInspector for Couchbase clusters.
 * Mapping of Couchbase concepts:
 *   - Schema   → Scope (within a bucket)
 *   - Table    → Collection
 *   - Column   → Key of a sampled document
 *   - Query    → N1QL (SQL for JSON)
 *   - Index    → GSI index
 *   - Transaction → N1QL ACID transactions (if supported by the cluster)
 */
export class CouchbaseAdapter implements IBaseDatabaseInspector {
  private inspector: CouchbaseSchemaInspector | null = null;
  private connectionInstance: CouchbaseConnection | null = null;

  /**
   * Connect to a Couchbase cluster.
   */
  async connect(config: DatabaseConfig): Promise<DatabaseConnection> {
    try {
      const connStr = config.host?.startsWith('couchbase://')
        ? config.host
        : `couchbase://${config.host || 'localhost'}`;

      this.inspector = new CouchbaseSchemaInspector({
        connectionString: connStr,
        username: config.user || 'Administrator',
        password: config.password || '',
        bucket: config.dbname,          // <-- fixed: use "bucket" not "bucketName"
        scope: config.schema,
      });

      await this.inspector.getConnection().connect();
      this.connectionInstance = this.inspector.getConnection();

      return this.connectionInstance as unknown as DatabaseConnection;
    } catch (error) {
      throw new Error(
        `Failed to connect to Couchbase: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  /**
   * Disconnect from the cluster.
   */
  async disconnect(_connection: DatabaseConnection): Promise<void> {
    try {
      if (this.connectionInstance) {
        await this.connectionInstance.disconnect();
      }
      this.inspector = null;
      this.connectionInstance = null;
    } catch (error) {
      console.error('Error disconnecting from Couchbase:', error);
    }
  }

  /**
   * Test connection by retrieving cluster info.
   */
  async testConnection(
    config: DatabaseConfig
  ): Promise<{ success: boolean; version?: string; error?: string }> {
    let testInspector: CouchbaseSchemaInspector | null = null;
    try {
      const connStr = config.host?.startsWith('couchbase://')
        ? config.host
        : `couchbase://${config.host || 'localhost'}`;

      testInspector = new CouchbaseSchemaInspector({
        connectionString: connStr,
        username: config.user || 'Administrator',
        password: config.password || '',
        bucket: config.dbname,          // <-- fixed: use "bucket"
        scope: config.schema,
      });

      const result = await testInspector.testConnection();
      return result;
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error),
      };
    } finally {
      if (testInspector) {
        try {
          await testInspector.getConnection().disconnect();
        } catch (_) { /* ignore */ }
      }
    }
  }

  /**
   * Get all collections in the current bucket (and optional scope).
   * Columns are inferred from a sample document (limit 1) per collection.
   */
  async getTables(
    _connection: DatabaseConnection,
    options?: InspectionOptions
  ): Promise<TableInfo[]> {
    if (!this.inspector) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    // getCollections() has no arguments – returns all collections
    const allCollections = await this.inspector.getCollections();

    // Optionally filter by scope
    const collections = options?.schema
      ? allCollections.filter(c => c.scopeName === options.schema)
      : allCollections;

    const tables: TableInfo[] = [];

    for (const col of collections) {
      let columns: ColumnMetadata[] = [];

      try {
        // Sample one document via N1QL query
        const query = `SELECT * FROM \`${col.bucketName}\`.\`${col.scopeName}\`.\`${col.collectionName}\` LIMIT 1`;
        const sampleResult = await this.inspector.executeQuery(query);
        const rows = sampleResult?.rows ?? [];
        const doc = rows[0]?.[col.collectionName] ?? rows[0]; // strip collection alias
        if (doc && typeof doc === 'object' && !Array.isArray(doc)) {
          columns = Object.keys(doc).map((key, idx) => ({
            name: key,
            type: typeof doc[key],
            dataType: typeof doc[key],
            nullable: true,
            ordinalPosition: idx + 1,
          } as ColumnMetadata));
        }
      } catch (_) { /* ignore sampling errors */ }

      // Fallback if no columns could be inferred
      if (columns.length === 0) {
        columns = [{
          name: 'id',
          type: 'string',
          dataType: 'string',
          nullable: false,
          ordinalPosition: 1,
        } as ColumnMetadata];
      }

      tables.push({
        schemaname: col.scopeName,      // scope = schema
        tablename: col.collectionName,  // collection = table
        tabletype: 'TABLE',
        columns,
        comment: undefined,
        rowCount: undefined,
        size: undefined,
        originalData: col,
      });
    }

    return tables;
  }

  /**
   * getTableColumns is a pass‑through (columns already in the table info).
   */
  async getTableColumns(
    _connection: DatabaseConnection,
    tables: TableInfo[]
  ): Promise<TableInfo[]> {
    return tables;
  }

  /**
   * Get cluster information (name, version).
   */
  async getDatabaseInfo(_connection: DatabaseConnection): Promise<DatabaseVersionInfo> {
    if (!this.inspector) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    // Use getDatabaseInfo() instead of getClusterInfo()
    const info = await this.inspector.getDatabaseInfo();
    return {
      version: info.version,
      name: info.clusterName,
      encoding: undefined,
      collation: undefined,
    };
  }

  /**
   * Execute a N1QL query.
   */
  async executeQuery(
    _connection: DatabaseConnection,
    sql: string,
    options?: QueryExecutionOptions
  ): Promise<QueryResult> {
    if (!this.inspector) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    const maxRows = options?.maxRows;
    const timeout = options?.timeout;
    const params = options?.params ?? [];

    // Use executeQuery (not executeN1QL)
    return await this.inspector.executeQuery(sql, params, { maxRows, timeout });
  }

  /**
   * Execute multiple N1QL statements inside a transaction.
   * Uses sequential execution (Couchbase ACID transactions not yet fully supported here).
   */
  async executeTransaction(
    _connection: DatabaseConnection,
    queries: Array<{ sql: string; params?: any[] }>
  ): Promise<QueryResult[]> {
    if (!this.inspector) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    try {
      return await this.inspector.executeTransaction(queries);
    } catch (error) {
      throw new Error(
        `Couchbase transaction failed: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  /**
   * Couchbase has no constraints – return an empty array.
   */
  async getTableConstraints(
    _connection: DatabaseConnection,
    _schema: string,
    _table: string
  ): Promise<any[]> {
    return [];
  }

  /**
   * Get scopes (schemas) within the current bucket.
   * Derive unique scope names from collections.
   */
  async getSchemas(_connection: DatabaseConnection): Promise<string[]> {
    if (!this.inspector) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    try {
      const collections = await this.inspector.getCollections();
      const scopes = [...new Set(collections.map(c => c.scopeName))];
      return scopes;
    } catch (error) {
      console.error('Failed to get Couchbase scopes:', error);
      return [];
    }
  }

  /**
   * Couchbase has no user-defined functions – return empty.
   */
  async getFunctions(_connection: DatabaseConnection, _database?: string): Promise<any[]> {
    return [];
  }

  /**
   * Get GSI indexes for a specific collection or all collections.
   */
  async getIndexes(connection: DatabaseConnection, tableName?: string): Promise<any[]> {
    if (!this.inspector) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    try {
      // Use getCurrentContext().bucket instead of getCurrentBucket()
      const bucket = this.inspector.getCurrentContext().bucket;
      let query = `SELECT * FROM system:indexes WHERE bucket_id = $1`;
      const params: any[] = [bucket];
      if (tableName) {
        query += ` AND keyspace_id = $2`;
        params.push(tableName);
      }
      const result = await this.executeQuery(connection, query, { params });
      return result.success ? result.rows || [] : [];
    } catch (error) {
      console.error('Failed to get Couchbase indexes:', error);
      return [];
    }
  }
}

export default CouchbaseAdapter;