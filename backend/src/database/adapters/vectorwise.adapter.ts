// backend/src/database/adapters/vectorwise.adapter.ts

import { IBaseDatabaseInspector } from '../inspection/base-inspector';
import {
  DatabaseConnection,
  QueryResult,
  TableInfo,
  ColumnMetadata,
  DatabaseConfig,
  InspectionOptions,
  QueryExecutionOptions,
  DatabaseVersionInfo,
} from '../types/inspection.types';

import odbc from 'odbc';

// ============================================================================
// VectorWise Connection Wrapper
// ============================================================================
export class VectorWiseConnection implements DatabaseConnection {
  private connection: odbc.Connection | null = null;
  private config: DatabaseConfig;

  constructor(config: DatabaseConfig) {
    this.config = config;
  }

  async connect(): Promise<void> {
    try {
      // Build ODBC connection string
      // Typical DSN-less format for VectorWise (Actian Vector)
      const connString =
        `Driver={VectorWise};` +
        `Host=${this.config.host || 'localhost'};` +
        `Port=${this.config.port || '5432'};` +
        `Database=${this.config.dbname};` +
        `User=${this.config.user};` +
        `Password=${this.config.password};`;
      this.connection = await odbc.connect(connString);
    } catch (error) {
      throw new Error(`VectorWise connection failed: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  async disconnect(): Promise<void> {
    if (this.connection) {
      await this.connection.close();
      this.connection = null;
    }
  }

  async query(sql: string, params: any[] = []): Promise<any[]> {
    if (!this.connection) {
      throw new Error('Connection not established');
    }
    const result = await this.connection.query(sql, params);
    return result as any[];
  }

  async execute(sql: string, params: any[] = []): Promise<odbc.QueryResult> {
    if (!this.connection) {
      throw new Error('Connection not established');
    }
    return await this.connection.query(sql, params);
  }

  async beginTransaction(): Promise<void> {
    if (!this.connection) throw new Error('No connection');
    await this.connection.beginTransaction();
  }

  async commit(): Promise<void> {
    if (!this.connection) throw new Error('No connection');
    await this.connection.commitTransaction();
  }

  async rollback(): Promise<void> {
    if (!this.connection) throw new Error('No connection');
    await this.connection.rollbackTransaction();
  }

  get underlyingConnection(): odbc.Connection | null {
    return this.connection;
  }
}

// ============================================================================
// VectorWise Schema Inspector
// ============================================================================
export class VectorWiseSchemaInspector {
  private connection: VectorWiseConnection;
  private schema: string;

  constructor(config: DatabaseConfig) {
    this.connection = new VectorWiseConnection(config);
    this.schema = config.schema || 'public';
  }

  getConnection(): VectorWiseConnection {
    return this.connection;
  }

  setSchema(schema: string): void {
    this.schema = schema;
  }

  async testConnection(): Promise<{ success: boolean; version?: string; error?: string }> {
    try {
      await this.connection.connect();
      const rows = await this.connection.query('SELECT version() as version');
      const version = rows[0]?.version;
      await this.connection.disconnect();
      return { success: true, version };
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error),
      };
    }
  }

  async getTables(): Promise<any[]> {
    const sql = `
      SELECT 
        table_catalog as database_name,
        table_schema as schemaname,
        table_name as tablename,
        table_type as tabletype,
        (SELECT obj_description(('"' || table_schema || '"."' || table_name || '"')::regclass)) as comment
      FROM information_schema.tables
      WHERE table_schema = ?
        AND table_type IN ('BASE TABLE', 'VIEW')
      ORDER BY table_schema, table_name
    `;
    const tables = await this.connection.query(sql, [this.schema]);

    const result = [];
    for (const table of tables) {
      const columns = await this.getTableColumns(table.schemaname, table.tablename);
      const rowCount = await this.getRowCount(table.schemaname, table.tablename);
      result.push({
        schemaname: table.schemaname,
        tablename: table.tablename,
        tabletype: table.tabletype === 'VIEW' ? 'view' : 'table',
        columns,
        comment: table.comment,
        rowCount,
        size: null, // VectorWise size info not implemented here
      });
    }
    return result;
  }

  private async getTableColumns(schema: string, table: string): Promise<any[]> {
    const sql = `
      SELECT 
        column_name as name,
        data_type as type,
        is_nullable = 'YES' as nullable,
        column_default as default,
        character_maximum_length as length,
        numeric_precision as precision,
        numeric_scale as scale,
        ordinal_position
      FROM information_schema.columns
      WHERE table_schema = ? AND table_name = ?
      ORDER BY ordinal_position
    `;
    const cols = await this.connection.query(sql, [schema, table]);
    return cols.map((col: any) => ({
      name: col.name,
      type: col.type,
      nullable: col.nullable,
      default: col.default,
      comment: null,   // VectorWise does not store comments in information_schema easily
      length: col.length,
      precision: col.precision,
      scale: col.scale,
      isIdentity: false, // Not derived from this query
      ordinalPosition: col.ordinal_position,
    }));
  }

  private async getRowCount(schema: string, table: string): Promise<number> {
    try {
      const rows = await this.connection.query(`SELECT COUNT(*) as cnt FROM "${schema}"."${table}"`);
      return rows[0]?.cnt || 0;
    } catch {
      return 0;
    }
  }

  async getDatabaseInfo(): Promise<{ version: string; name: string; encoding: string; collation: string }> {
    const versionRows = await this.connection.query('SELECT version() as version');
    const version = versionRows[0]?.version || 'Unknown';
    const dbNameRows = await this.connection.query('SELECT current_database() as db');
    const name = dbNameRows[0]?.db || 'unknown';
    // VectorWise encoding/collation not standard; provide placeholders
    return { version, name, encoding: 'UTF-8', collation: 'en_US' };
  }

  async executeQuery(sql: string, params: any[] = [], options?: { maxRows?: number; timeout?: number; autoDisconnect?: boolean }): Promise<QueryResult> {
    try {
      if (options?.timeout) {
        // ODBC doesn't support query timeout directly, but we can use AbortController with a promise race
        // For simplicity, we ignore timeout in this example
      }
      const result = await this.connection.execute(sql, params);
      let rows = result as any[];
      if (options?.maxRows && rows.length > options.maxRows) {
        rows = rows.slice(0, options.maxRows);
      }
      return {
        success: true,
        rows,
        rowCount: rows.length,
        fields: result.length ? Object.keys(result[0]) : [],
      };
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error),
        rows: [],
        rowCount: 0,
        fields: [],
      };
    }
  }

  async executeTransaction(queries: Array<{ sql: string; params?: any[] }>): Promise<QueryResult[]> {
    const results: QueryResult[] = [];
    try {
      await this.connection.beginTransaction();
      for (const q of queries) {
        const res = await this.executeQuery(q.sql, q.params || []);
        results.push(res);
        if (!res.success) throw new Error(res.error);
      }
      await this.connection.commit();
      return results;
    } catch (error) {
      await this.connection.rollback();
      throw error;
    }
  }
}

// ============================================================================
// VectorWise Adapter – implements IBaseDatabaseInspector
// ============================================================================
export class VectorWiseAdapter implements IBaseDatabaseInspector {
  private inspector: VectorWiseSchemaInspector | null = null;
  private connectionInstance: VectorWiseConnection | null = null;

  async connect(config: DatabaseConfig): Promise<DatabaseConnection> {
    try {
      this.inspector = new VectorWiseSchemaInspector(config);
      await this.inspector.getConnection().connect();
      this.connectionInstance = this.inspector.getConnection();
      return this.connectionInstance as unknown as DatabaseConnection;
    } catch (error) {
      throw new Error(`Failed to connect to VectorWise: ${error instanceof Error ? error.message : String(error)}`);
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
      console.error('Error disconnecting from VectorWise:', error);
    }
  }

  async testConnection(config: DatabaseConfig): Promise<{ success: boolean; version?: string; error?: string }> {
    const tempInspector = new VectorWiseSchemaInspector(config);
    return await tempInspector.testConnection();
  }

  async getTables(_connection: DatabaseConnection, options?: InspectionOptions): Promise<TableInfo[]> {
    if (!this.inspector) throw new Error('Inspector not initialized. Call connect() first.');
    if (options?.schema) this.inspector.setSchema(options.schema);
    const tables = await this.inspector.getTables();
    return tables.map((table: any) => ({
      schemaname: table.schemaname,
      tablename: table.tablename,
      tabletype: table.tabletype,
      columns: table.columns.map((col: any) => ({
        name: col.name,
        type: col.type,
        dataType: col.type,
        nullable: col.nullable,
        default: col.default,
        comment: col.comment,
        length: col.length,
        precision: col.precision,
        scale: col.scale,
        isIdentity: col.isIdentity,
        ordinalPosition: col.ordinalPosition,
      } as ColumnMetadata)),
      comment: table.comment,
      rowCount: table.rowCount,
      size: table.size,
      originalData: table,
    }));
  }

  async getTableColumns(_connection: DatabaseConnection, tables: TableInfo[]): Promise<TableInfo[]> {
    // Table info already includes columns from getTables()
    return tables;
  }

  async getDatabaseInfo(_connection: DatabaseConnection): Promise<DatabaseVersionInfo> {
    if (!this.inspector) throw new Error('Inspector not initialized. Call connect() first.');
    const info = await this.inspector.getDatabaseInfo();
    return {
      version: info.version,
      name: info.name,
      encoding: info.encoding,
      collation: info.collation,
    };
  }

  async executeQuery(
    _connection: DatabaseConnection,
    sql: string,
    options?: QueryExecutionOptions
  ): Promise<QueryResult> {
    if (!this.inspector) throw new Error('Inspector not initialized');
    const pgOptions = {
      maxRows: options?.maxRows,
      timeout: options?.timeout,
      autoDisconnect: options?.autoDisconnect,
    };
    const params = options?.params || [];
    return await this.inspector.executeQuery(sql, params, pgOptions);
  }

  async executeTransaction(
    _connection: DatabaseConnection,
    queries: Array<{ sql: string; params?: any[] }>
  ): Promise<QueryResult[]> {
    if (!this.inspector) throw new Error('Inspector not initialized');
    return await this.inspector.executeTransaction(queries);
  }

  async getTableConstraints(_connection: DatabaseConnection, _schema: string, _table: string): Promise<any[]> {
    if (!this.inspector) throw new Error('Inspector not initialized');
    // Query information_schema for constraints
    const sql = `
      SELECT 
        constraint_name, constraint_type, table_schema, table_name
      FROM information_schema.table_constraints
      WHERE table_schema = ? AND table_name = ?
    `;
    const result = await this.inspector.executeQuery(sql, [_schema, _table]);
    return result.success ? result.rows : [];
  }

  async getSchemas(connection: DatabaseConnection): Promise<string[]> {
    const result = await this.executeQuery(connection, `
      SELECT schema_name 
      FROM information_schema.schemata 
      WHERE schema_name NOT IN ('information_schema', 'pg_catalog')
      ORDER BY schema_name
    `);
    if (result.success && result.rows) {
      return result.rows.map((row: any) => row.schema_name);
    }
    return ['public'];
  }

  // Additional VectorWise-specific methods
  async getFunctions(connection: DatabaseConnection, schema?: string): Promise<any[]> {
    const schemaFilter = schema ? `AND specific_schema = '${schema}'` : '';
    const sql = `
      SELECT 
        specific_schema as schema,
        specific_name as function_name,
        data_type as return_type,
        parameter_style,
        language
      FROM information_schema.routines
      WHERE routine_type = 'FUNCTION'
        AND specific_schema NOT IN ('information_schema', 'pg_catalog')
        ${schemaFilter}
      ORDER BY specific_schema, specific_name
    `;
    const result = await this.executeQuery(connection, sql);
    return result.success ? result.rows : [];
  }

  async getIndexes(connection: DatabaseConnection, tableName?: string): Promise<any[]> {
    // VectorWise exposes index info through sys tables; simplified version using information_schema
    // This is a stub – production systems would need proper catalog queries.
    const whereClause = tableName ? `AND t.table_name = '${tableName}'` : '';
    const sql = `
      SELECT 
        t.table_schema as schema_name,
        t.table_name,
        'PRIMARY' as index_type,
        c.column_name
      FROM information_schema.table_constraints tc
      JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
      JOIN information_schema.tables t ON t.table_name = ccu.table_name
      WHERE tc.constraint_type = 'PRIMARY KEY'
        AND t.table_schema NOT IN ('information_schema', 'pg_catalog')
        ${whereClause}
      UNION ALL
      SELECT 
        t.table_schema, t.table_name, 'UNIQUE', c.column_name
      FROM information_schema.table_constraints tc
      JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
      JOIN information_schema.tables t ON t.table_name = ccu.table_name
      WHERE tc.constraint_type = 'UNIQUE'
        AND t.table_schema NOT IN ('information_schema', 'pg_catalog')
        ${whereClause}
    `;
    const result = await this.executeQuery(connection, sql);
    return result.success ? result.rows : [];
  }
}

export default VectorWiseAdapter;