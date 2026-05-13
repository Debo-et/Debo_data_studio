// backend/src/database/adapters/hive.adapter.ts

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

import { HiveSchemaInspector, HiveConnection } from '../inspection/hive-inspector';

/**
 * Hive Database Adapter
 *
 * Implements IBaseDatabaseInspector for Apache Hive (HiveServer2 / Thrift).
 * Mapping of Hive concepts:
 *   - Schema   → Hive database
 *   - Table    → Hive table / view
 *   - Column   → Hive column descriptor
 *   - Query    → HiveQL (SQL)
 *   - Transaction → not fully ACID in all setups; executed as batch
 */
export class HiveAdapter implements IBaseDatabaseInspector {
  private inspector: HiveSchemaInspector | null = null;
  private connectionInstance: HiveConnection | null = null;

  /**
   * Connect to Hive via the configured Thrift server.
   *
   * The config object expects:
   * - dbname   → Hive database name (default: 'default')
   * - host     → Thrift server host (default: 'localhost')
   * - port     → Thrift server port (default: 10000)
   * - user     → username (optional)
   * - password → password (optional)
   * - schema   → alias for dbname (fallback)
   */
  async connect(config: DatabaseConfig): Promise<DatabaseConnection> {
    try {
      this.inspector = new HiveSchemaInspector({
        dbname: config.dbname || config.schema || 'default',
        host: config.host || 'localhost',
        port: config.port ? config.port.toString() : '10000',
        user: config.user,
        password: config.password,
        schema: config.schema           // pass through for compatibility
      });

      await this.inspector.getConnection().connect();
      this.connectionInstance = this.inspector.getConnection();

      return this.connectionInstance as unknown as DatabaseConnection;
    } catch (error) {
      throw new Error(
        `Failed to connect to Hive: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  /**
   * Disconnect from the Hive server.
   */
  async disconnect(_connection: DatabaseConnection): Promise<void> {
    try {
      if (this.connectionInstance) {
        await this.connectionInstance.disconnect();
      }
      this.inspector = null;
      this.connectionInstance = null;
    } catch (error) {
      console.error('Error disconnecting from Hive:', error);
    }
  }

  /**
   * Test the connection by fetching the Hive version.
   */
  async testConnection(
    config: DatabaseConfig
  ): Promise<{ success: boolean; version?: string; error?: string }> {
    let testInspector: HiveSchemaInspector | null = null;
    try {
      testInspector = new HiveSchemaInspector({
        dbname: config.dbname || config.schema || 'default',
        host: config.host || 'localhost',
        port: config.port ? config.port.toString() : '10000',
        user: config.user,
        password: config.password
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
        } catch (_) {
          /* ignore */
        }
      }
    }
  }

  /**
   * Retrieve all tables and views in the configured database.
   * If options.schema is provided, the inspector switches to that database.
   */
  async getTables(
    _connection: DatabaseConnection,
    options?: InspectionOptions
  ): Promise<TableInfo[]> {
    if (!this.inspector) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    try {
      // Switch database if requested (schema = Hive database)
      if (options?.schema) {
        this.inspector.setSchema(options.schema);
      }

      const tables = await this.inspector.getTables();

      // Map Hive TableInfo to the adapter's TableInfo format
      return tables.map((table) => ({
        schemaname: table.schemaname,
        tablename: table.tablename,
        tabletype: table.tabletype || 'table',
        columns: (table.columns || []).map((col, idx) => ({
          name: col.name,
          type: col.type,
          dataType: col.type,
          nullable: col.nullable ?? true,     // Hive doesn't enforce NOT NULL by default
          default: col.default,
          comment: col.comment,
          length: col.length,
          precision: col.precision,
          scale: col.scale,
          ordinalPosition: idx + 1,
          isPrimaryKey: false,                // Hive does not have primary keys
        } as ColumnMetadata)),
        comment: table.comment,
        rowCount: table.rowCount,
        size: table.size,
        originalData: table,
      }));
    } catch (error) {
      throw new Error(
        `Failed to get tables from Hive: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  /**
   * getTableColumns is a pass‑through – columns are already included in getTables.
   */
  async getTableColumns(
    _connection: DatabaseConnection,
    tables: TableInfo[]
  ): Promise<TableInfo[]> {
    return tables;
  }

  /**
   * Retrieve Hive version and current database name.
   */
  async getDatabaseInfo(_connection: DatabaseConnection): Promise<DatabaseVersionInfo> {
    if (!this.inspector) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    try {
      const info = await this.inspector.getDatabaseInfo();
      return {
        version: info.version,
        name: info.name,
        encoding: info.encoding,
        collation: info.collation,
      };
    } catch (error) {
      throw new Error(
        `Failed to get Hive info: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  /**
   * Execute a HiveQL query.
   *
   * @param sql    HiveQL statement (supports ? placeholders for params)
   * @param options  { maxRows, timeout, params, autoDisconnect }
   */
  async executeQuery(
    _connection: DatabaseConnection,
    sql: string,
    options?: QueryExecutionOptions
  ): Promise<QueryResult> {
    if (!this.inspector) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    const hiveOptions = {
      maxRows: options?.maxRows,
      timeout: options?.timeout,
      autoDisconnect: options?.autoDisconnect,
    };
    const params = options?.params || [];

    return await this.inspector.executeQuery(sql, params, hiveOptions);
  }

  /**
   * Execute multiple HiveQL statements sequentially.
   * Not fully atomic – uses best‑effort batching.
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
        `Hive transaction failed: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  /**
   * Hive does not enforce relational constraints – always returns an empty array.
   */
  async getTableConstraints(
    _connection: DatabaseConnection,
    _schema: string,
    _table: string
  ): Promise<any[]> {
    return [];
  }

  /**
   * List all available Hive databases (schemas).
   */
  async getSchemas(_connection: DatabaseConnection): Promise<string[]> {
    if (!this.connectionInstance) {
      throw new Error('Not connected. Call connect() first.');
    }

    try {
      const result = await this.connectionInstance.query('SHOW DATABASES');
      // SHOW DATABASES returns rows with column "database_name"
      if (result && result.rows) {
        return result.rows.map((row: any) => row.database_name || row[0]);
      }
      return [];
    } catch (error) {
      console.error('Failed to list Hive databases:', error);
      return [];
    }
  }

  /**
   * Hive does not have user‑defined functions in the traditional RDBMS sense.
   * (UDFs exist but are JAR‑based; not easily queried via metadata.)
   */
  async getFunctions(_connection: DatabaseConnection, _database?: string): Promise<any[]> {
    // Could potentially use SHOW FUNCTIONS in some Hive versions, but not reliable.
    return [];
  }

  /**
   * Hive does not have native indexes – return empty.
   */
  async getIndexes(_connection: DatabaseConnection, _tableName?: string): Promise<any[]> {
    return [];
  }
}

export default HiveAdapter;