// backend/src/database/adapters/cassandra.adapter.ts

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

import { CassandraSchemaInspector, CassandraConnection } from '../inspection/cassandra-inspector';

export class CassandraAdapter implements IBaseDatabaseInspector {
  private inspector: CassandraSchemaInspector | null = null;
  private connectionInstance: CassandraConnection | null = null;

  async connect(config: DatabaseConfig): Promise<DatabaseConnection> {
    try {
      this.inspector = new CassandraSchemaInspector({
        contactPoints: [config.host || 'localhost'],
        localDataCenter: (config as any).localDataCenter || 'datacenter1',
        username: config.user,
        password: config.password || (config.user ? '' : undefined),
        keyspace: config.dbname || config.schema,
        protocolOptions: {
          port: Number(config.port) || 9042,
        },
      });

      await this.inspector.getConnection().connect();
      this.connectionInstance = this.inspector.getConnection();

      return this.connectionInstance as unknown as DatabaseConnection;
    } catch (error) {
      throw new Error(
        `Failed to connect to Cassandra: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  async disconnect(_connection: DatabaseConnection): Promise<void> {
    try {
      if (this.connectionInstance) {
        await this.connectionInstance.disconnect();
      }
      this.inspector = null;
      this.connectionInstance = null;
    } catch (error) {
      console.error('Error disconnecting from Cassandra:', error);
    }
  }

  async testConnection(
    config: DatabaseConfig
  ): Promise<{ success: boolean; version?: string; error?: string }> {
    let testInspector: CassandraSchemaInspector | null = null;
    try {
      testInspector = new CassandraSchemaInspector({
        contactPoints: [config.host || 'localhost'],
        localDataCenter: (config as any).localDataCenter || 'datacenter1',
        username: config.user,
        password: config.password || (config.user ? '' : undefined),
        keyspace: config.dbname || config.schema,
        protocolOptions: {
          port: Number(config.port) || 9042,
        },
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

  async getTables(
    _connection: DatabaseConnection,
    options?: InspectionOptions
  ): Promise<TableInfo[]> {
    if (!this.inspector) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    try {
      if (options?.schema) {
        this.inspector.setSchema(options.schema);
      }

      const tables = await this.inspector.getTables();
      return tables.map((table) => ({
        schemaname: table.schemaname ?? options?.schema ?? '',
        tablename: table.tablename,
        tabletype: table.tabletype || 'TABLE',
        columns: (table.columns || []).map((col, idx) => ({
          name: col.name,
          type: col.type,
          dataType: col.type,
          nullable: col.kind !== 'partition_key' && col.kind !== 'clustering_key', // ← fixed spelling
          default: col.default,
          comment: col.comment,
          length: col.length,
          precision: col.precision,
          scale: col.scale,
          ordinalPosition: (col as any).position ?? idx + 1,
          isPrimaryKey: col.kind === 'partition_key' || col.kind === 'clustering_key', // ← fixed spelling
        } as ColumnMetadata)),
        comment: table.comment,
        rowCount: undefined,
        size: undefined,
        originalData: table,
      }));
    } catch (error) {
      throw new Error(
        `Failed to get tables from Cassandra: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  async getTableColumns(
    _connection: DatabaseConnection,
    tables: TableInfo[]
  ): Promise<TableInfo[]> {
    return tables;
  }

  async getDatabaseInfo(_connection: DatabaseConnection): Promise<DatabaseVersionInfo> {
    if (!this.inspector) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    try {
      const info = await this.inspector.getDatabaseInfo();
      return {
        version: info.version,
        name: info.name,
        encoding: undefined,
        collation: undefined,
      };
    } catch (error) {
      throw new Error(
        `Failed to get Cassandra info: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  async executeQuery(
    _connection: DatabaseConnection,
    sql: string,
    options?: QueryExecutionOptions
  ): Promise<QueryResult> {
    if (!this.inspector) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    const cassandraOptions = {
      maxRows: options?.maxRows,
      timeout: options?.timeout,
      autoDisconnect: options?.autoDisconnect,
    };
    const params = options?.params || [];

    return await this.inspector.executeQuery(sql, params, cassandraOptions);
  }

  async executeTransaction(
    _connection: DatabaseConnection,
    queries: Array<{ sql: string; params?: any[] }>
  ): Promise<QueryResult[]> {
    if (!this.inspector) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    try {
      return await this.inspector.executeTransaction(queries, 'LOGGED');
    } catch (error) {
      throw new Error(
        `Cassandra transaction (batch) failed: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  async getTableConstraints(
    _connection: DatabaseConnection,
    _schema: string,
    _table: string
  ): Promise<any[]> {
    return [];
  }

  async getSchemas(connection: DatabaseConnection): Promise<string[]> {
    if (!this.inspector) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    try {
      const result = await this.executeQuery(
        connection,
        'SELECT keyspace_name FROM system_schema.keyspaces'
      );
      if (result.success && result.rows) {
        return result.rows.map((row: any) => row.keyspace_name);
      }
      return [];
    } catch (error) {
      console.error('Failed to get Cassandra keyspaces:', error);
      return [];
    }
  }

  async getFunctions(connection: DatabaseConnection, keyspace?: string): Promise<any[]> {
    let query = `
      SELECT keyspace_name, function_name, argument_types, return_type,
             language, body
      FROM system_schema.functions
    `;
    const params: any[] = [];
    if (keyspace) {
      query += ' WHERE keyspace_name = ?';
      params.push(keyspace);
    }
    const result = await this.executeQuery(connection, query, { params });
    return result.success && result.rows ? result.rows : [];
  }

  async getIndexes(connection: DatabaseConnection, tableName?: string): Promise<any[]> {
    if (!this.inspector) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }
    const keyspace = this.inspector.getCurrentSchema();

    let query = `
      SELECT keyspace_name, table_name, index_name, kind, options
      FROM system_schema.indexes
      WHERE keyspace_name = ?
    `;
    const params: any[] = [keyspace];
    if (tableName) {
      query += ' AND table_name = ?';
      params.push(tableName);
    }
    const result = await this.executeQuery(connection, query, { params });
    return result.success && result.rows ? result.rows : [];
  }
}

export default CassandraAdapter;