// backend/src/database/adapters/hbase.adapter.ts

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
// Import the hypothetical HBase inspector (to be implemented)
// ------------------------------------------------------------------
import { HBaseSchemaInspector, HBaseConnection } from '../inspection/hBase-inspector';

/**
 * Apache HBase Adapter
 * Implements IBaseDatabaseInspector for HBase, mapping its data model
 * (tables → column families, no SQL, namespaces as schemas) to the
 * common interface.
 */
export class HBaseAdapter implements IBaseDatabaseInspector {
  private inspector: HBaseSchemaInspector | null = null;
  private connectionInstance: HBaseConnection | null = null;

  /**
   * Connect to HBase via the configured ZooKeeper quorum.
   *
   * The config object expects:
   * - dbname        → unused (HBase doesn’t have a default “database”)
   * - host          → ZooKeeper quorum (string, default 'localhost')
   * - port          → ZooKeeper client port (string/number, default '2181')
   * - user / password → if security enabled (e.g., Kerberos/Simple)
   * - schema        → initial namespace (default: 'default')
   * Additional HBase‑specific options can be passed via the inspector.
   */
  async connect(config: DatabaseConfig): Promise<DatabaseConnection> {
    try {
      this.inspector = new HBaseSchemaInspector({
        zookeeperQuorum: config.host || 'localhost',
        zookeeperPort: Number(config.port) || 2181,
        user: config.user,
        password: config.password,
        namespace: config.schema || 'default', // HBase namespace
        // Extra configuration (if any) can be spread here
      });

      await this.inspector.getConnection().connect();
      this.connectionInstance = this.inspector.getConnection();

      return this.connectionInstance as unknown as DatabaseConnection;
    } catch (error) {
      throw new Error(
        `Failed to connect to HBase: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  /**
   * Disconnect from the HBase cluster.
   */
  async disconnect(_connection: DatabaseConnection): Promise<void> {
    try {
      if (this.connectionInstance) {
        await this.connectionInstance.disconnect();
      }
      this.inspector = null;
      this.connectionInstance = null;
    } catch (error) {
      console.error('Error disconnecting from HBase:', error);
    }
  }

  /**
   * Test the connection by fetching the HBase version.
   */
  async testConnection(
    config: DatabaseConfig
  ): Promise<{ success: boolean; version?: string; error?: string }> {
    let testInspector: HBaseSchemaInspector | null = null;
    try {
      testInspector = new HBaseSchemaInspector({
        zookeeperQuorum: config.host || 'localhost',
        zookeeperPort: Number(config.port) || 2181,
        user: config.user,
        password: config.password,
        namespace: config.schema || 'default',
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
   * Retrieve all tables in the current (or specified) namespace.
   * Each column family is represented as a ColumnMetadata row.
   */
  async getTables(
    _connection: DatabaseConnection,
    options?: InspectionOptions
  ): Promise<TableInfo[]> {
    if (!this.inspector) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    try {
      // Switch namespace if requested
      if (options?.schema) {
        this.inspector.setNamespace(options.schema);
      }

      const tables = await this.inspector.getTables();
      return tables.map((table) => ({
        schemaname: table.schemaname ?? options?.schema ?? 'default',
        tablename: table.tablename,
        tabletype: 'TABLE', // HBase does not have views
        columns: (table.columns || []).map((col, idx) => ({
          name: col.name, // Column family name
          type: col.type || 'columnfamily',
          dataType: col.dataType || col.type || 'columnfamily',
          nullable: true,
          comment: col.comment,
          length: col.length,
          precision: col.precision,
          scale: col.scale,
          ordinalPosition: col.ordinalPosition ?? idx + 1,
        } as ColumnMetadata)),
        comment: table.comment,
        rowCount: table.rowCount, // May be undefined (expensive to compute)
        size: table.size,
        originalData: table,
      }));
    } catch (error) {
      throw new Error(
        `Failed to get tables from HBase: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  /**
   * getTableColumns is a pass‑through because getTables already includes columns.
   */
  async getTableColumns(
    _connection: DatabaseConnection,
    tables: TableInfo[]
  ): Promise<TableInfo[]> {
    return tables;
  }

  /**
   * Retrieve HBase version and cluster information.
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
        encoding: undefined, // Not applicable
        collation: undefined, // Not applicable
      };
    } catch (error) {
      throw new Error(
        `Failed to get HBase info: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  /**
   * Execute a HBase operation expressed as a JSON command.
   *
   * The `sql` parameter must be a JSON string describing the action:
   * {
   *   "table": "my_table",
   *   "action": "get" | "scan" | "put" | "delete" | "count" | "list",
   *   ...other parameters per action
   * }
   *
   * Examples:
   * - get: { "table":"t", "action":"get", "rowKey":"row1" }
   * - scan: { "table":"t", "action":"scan", "startRow":"", "stopRow":"", "limit":10 }
   * - put: { "table":"t", "action":"put", "rowKey":"r1", "columns":{ "cf:qual":"value" } }
   * - delete: { "table":"t", "action":"delete", "rowKey":"r1" }
   *
   * Returns a QueryResult with rows as arrays of key‑value objects.
   */
  async executeQuery(
    _connection: DatabaseConnection,
    sql: string,
    options?: QueryExecutionOptions
  ): Promise<QueryResult> {
    if (!this.inspector) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    try {
      const command = JSON.parse(sql);
      const tableName = command.table;
      if (!tableName) throw new Error('Missing "table" in HBase command');

      const maxRows = options?.maxRows ?? 1000;
      let rows: any[] = [];

      switch (command.action) {
        case 'get': {
          const rowKey = command.rowKey;
          if (!rowKey) throw new Error('"rowKey" required for get');
          const result = await this.inspector.getRow(tableName, rowKey);
          rows = result ? [result] : [];
          break;
        }
        case 'scan': {
          const scanner = await this.inspector.scanTable(tableName, {
            startRow: command.startRow,
            stopRow: command.stopRow,
            limit: Math.min(command.limit || maxRows, maxRows),
          });
          rows = scanner;
          break;
        }
        case 'put': {
          const { rowKey, columns } = command;
          if (!rowKey || !columns) throw new Error('"rowKey" and "columns" are required for put');
          await this.inspector.putRow(tableName, rowKey, columns);
          rows = [{ success: true }];
          break;
        }
        case 'delete': {
          const rowKey = command.rowKey;
          if (!rowKey) throw new Error('"rowKey" required for delete');
          await this.inspector.deleteRow(tableName, rowKey);
          rows = [{ success: true }];
          break;
        }
        case 'count': {
          const count = await this.inspector.getRowCount(tableName);
          rows = [{ count }];
          break;
        }
        default:
          throw new Error(`Unsupported HBase action: ${command.action}`);
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
   * Execute multiple HBase commands in sequence.
   * HBase does not support true multi‑row transactions across different rows,
   * so commands are executed sequentially; if one fails, subsequent commands
   * are still attempted (best effort). A true atomic transaction must be
   * implemented inside the inspector using checkAndMutate if needed.
   */
  async executeTransaction(
    _connection: DatabaseConnection,
    queries: Array<{ sql: string; params?: any[] }>
  ): Promise<QueryResult[]> {
    if (!this.inspector) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    const results: QueryResult[] = [];
    for (const q of queries) {
      const res = await this.executeQuery(_connection, q.sql);
      results.push(res);
    }
    return results;
  }

  /**
   * HBase does not enforce relational constraints – always returns an empty array.
   */
  async getTableConstraints(
    _connection: DatabaseConnection,
    _schema: string,
    _table: string
  ): Promise<any[]> {
    return [];
  }

  /**
   * Map HBase namespaces to the “schema” concept.
   */
  async getSchemas(connection: DatabaseConnection): Promise<string[]> {
    if (!this.inspector) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    try {
      return await this.inspector.listNamespaces();
    } catch (error) {
      console.error('Failed to list HBase namespaces:', error);
      return ['default'];
    }
  }

  /**
   * HBase has no user‑defined functions – return an empty array.
   */
  async getFunctions(connection: DatabaseConnection, database?: string): Promise<any[]> {
    return [];
  }

  /**
   * HBase does not have native secondary indexes; return an empty array.
   * (Phoenix or other SQL layers could be queried separately.)
   */
  async getIndexes(connection: DatabaseConnection, tableName?: string): Promise<any[]> {
    return [];
  }
}

export default HBaseAdapter;