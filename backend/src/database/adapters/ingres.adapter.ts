// backend/src/database/adapters/ingres.adapter.ts

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
// Ingres Connection Wrapper
// ============================================================================
export class IngresConnection implements DatabaseConnection {
  private connection: odbc.Connection | null = null;
  private config: DatabaseConfig;

  constructor(config: DatabaseConfig) {
    this.config = config;
  }

  async connect(): Promise<void> {
    try {
      // Typical Ingres ODBC DSN-less connection string
      // Adjust based on your Ingres installation
      const connString =
        `Driver={Ingres};` +
        `Server=${this.config.host || 'localhost'};` +
        `Port=${this.config.port || 'II7'};` + // Ingres default port alias
        `Database=${this.config.dbname};` +
        `User=${this.config.user};` +
        `Password=${this.config.password};`;
      this.connection = await odbc.connect(connString);
    } catch (error) {
      throw new Error(`Ingres connection failed: ${error instanceof Error ? error.message : String(error)}`);
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
    // Ingres ODBC supports parameterized queries with '?' placeholders
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
// Ingres Schema Inspector
// ============================================================================
export class IngresSchemaInspector {
  private connection: IngresConnection;
  private schema: string;

  constructor(config: DatabaseConfig) {
    this.connection = new IngresConnection(config);
    this.schema = config.schema?.toUpperCase() || 'PUBLIC'; // Ingres uses uppercase schema names
  }

  getConnection(): IngresConnection {
    return this.connection;
  }

  setSchema(schema: string): void {
    this.schema = schema.toUpperCase();
  }

  async testConnection(): Promise<{ success: boolean; version?: string; error?: string }> {
    try {
      await this.connection.connect();
      const rows = await this.connection.query('SELECT DBMSINFO(\'VERSION\') AS version');
      const version = rows[0]?.VERSION;
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
    // Ingres system catalogs: iitables, iiuser, iicolumns
    // Filter by current schema (database owner)
    const sql = `
      SELECT 
        t.table_owner AS schemaname,
        t.table_name AS tablename,
        CASE WHEN t.table_type = 'V' THEN 'VIEW' ELSE 'TABLE' END AS tabletype,
        COALESCE(r.comment, '') AS comment
      FROM iitables t
      LEFT JOIN iiremarks r ON r.object = t.table_name AND r.schema = t.table_owner AND r.object_type = 'T'
      WHERE t.table_owner = ?
        AND t.table_type IN ('T', 'V')
      ORDER BY t.table_owner, t.table_name
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
        size: null, // Ingres size not easily available
      });
    }
    return result;
  }

  private async getTableColumns(schema: string, table: string): Promise<any[]> {
    // iicolumns provides column metadata
    const sql = `
      SELECT 
        c.column_name AS name,
        c.data_type AS type,
        c.nullable AS nullable,
        c.default_value AS default,
        c.column_length AS length,
        c.decimal_digits AS scale,
        c.column_position AS ordinal_position
      FROM iicolumns c
      WHERE c.table_owner = ? AND c.table_name = ?
      ORDER BY c.column_position
    `;
    const cols = await this.connection.query(sql, [schema, table]);
    return cols.map((col: any) => ({
      name: col.name,
      type: col.type,
      nullable: col.nullable === 1,
      default: col.default,
      comment: null, // Ingres remarks can be fetched from iiremarks separately
      length: col.length,
      precision: null, // not directly available in this query
      scale: col.scale,
      isIdentity: false, // would need iikeys etc.
      ordinalPosition: col.ordinal_position,
    }));
  }

  private async getRowCount(schema: string, table: string): Promise<number> {
    try {
      // Use double quotes for case-sensitive identifiers
      const rows = await this.connection.query(`SELECT COUNT(*) AS cnt FROM "${schema}"."${table}"`);
      return rows[0]?.CNT || 0;
    } catch {
      return 0;
    }
  }

  async getDatabaseInfo(): Promise<{ version: string; name: string; encoding: string; collation: string }> {
    const versionRows = await this.connection.query('SELECT DBMSINFO(\'VERSION\') AS version');
    const version = versionRows[0]?.VERSION || 'Unknown';
    const dbRows = await this.connection.query('SELECT DB_NAME() AS dbname');
    const name = dbRows[0]?.DBNAME || 'unknown';
    // Ingres defaults
    return { version, name, encoding: 'UTF8', collation: 'BINARY' };
  }

  async executeQuery(
    sql: string,
    params: any[] = [],
    options?: { maxRows?: number; timeout?: number; autoDisconnect?: boolean }
  ): Promise<QueryResult> {
    try {
      if (options?.timeout) {
        // ODBC timeout can be set via connection property, but for simplicity we ignore
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
        fields: rows.length ? Object.keys(rows[0]) : [],
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
// Ingres Adapter – implements IBaseDatabaseInspector
// ============================================================================
export class IngresAdapter implements IBaseDatabaseInspector {
  private inspector: IngresSchemaInspector | null = null;
  private connectionInstance: IngresConnection | null = null;

  async connect(config: DatabaseConfig): Promise<DatabaseConnection> {
    try {
      this.inspector = new IngresSchemaInspector(config);
      await this.inspector.getConnection().connect();
      this.connectionInstance = this.inspector.getConnection();
      return this.connectionInstance as unknown as DatabaseConnection;
    } catch (error) {
      throw new Error(`Failed to connect to Ingres: ${error instanceof Error ? error.message : String(error)}`);
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
      console.error('Error disconnecting from Ingres:', error);
    }
  }

  async testConnection(config: DatabaseConfig): Promise<{ success: boolean; version?: string; error?: string }> {
    const tempInspector = new IngresSchemaInspector(config);
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
    // Columns are already fetched in getTables()
    return tables;
  }

  async getDatabaseInfo(_connection: DatabaseConnection): Promise<DatabaseVersionInfo> {
    if (!this.inspector) throw new Error('Inspector not initialized');
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
    const opts = {
      maxRows: options?.maxRows,
      timeout: options?.timeout,
      autoDisconnect: options?.autoDisconnect,
    };
    const params = options?.params || [];
    return await this.inspector.executeQuery(sql, params, opts);
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
    // Query Ingres constraints from iiconstraints and iiconstraint_columns
    const sql = `
      SELECT 
        c.constraint_name,
        c.constraint_type,
        cc.column_name
      FROM iiconstraints c
      LEFT JOIN iiconstraint_columns cc 
        ON cc.constraint_name = c.constraint_name AND cc.table_name = c.table_name
      WHERE c.table_owner = ? AND c.table_name = ?
    `;
    const result = await this.inspector.executeQuery(sql, [_schema, _table]);
    return result.success ? result.rows : [];
  }

  async getSchemas(connection: DatabaseConnection): Promise<string[]> {
    // In Ingres, schemas are effectively database owners
    const sql = `
      SELECT DISTINCT table_owner AS schema_name
      FROM iitables
      WHERE table_owner NOT IN ('$ingres', 'sys', 'maintenance')
      ORDER BY table_owner
    `;
    const result = await this.executeQuery(connection, sql);
    if (result.success && result.rows) {
      return result.rows.map((row: any) => row.SCHEMA_NAME);
    }
    return ['public'];
  }

  // Additional Ingres-specific methods
  async getFunctions(connection: DatabaseConnection, schema?: string): Promise<any[]> {
    // Ingres stores procedures in iiprocedures
    const schemaFilter = schema ? `AND procedure_owner = '${schema}'` : '';
    const sql = `
      SELECT 
        procedure_owner AS schema,
        procedure_name,
        procedure_type,
        return_type
      FROM iiprocedures
      WHERE procedure_owner NOT IN ('$ingres', 'sys')
        ${schemaFilter}
      ORDER BY procedure_owner, procedure_name
    `;
    const result = await this.executeQuery(connection, sql);
    return result.success ? result.rows : [];
  }

  async getIndexes(connection: DatabaseConnection, tableName?: string): Promise<any[]> {
    const filter = tableName ? `AND idx.table_name = '${tableName}'` : '';
    const sql = `
      SELECT 
        idx.table_owner AS schema_name,
        idx.table_name,
        idx.index_name,
        idx.unique_rule AS is_unique,
        CASE WHEN idx.index_name = ii.relname THEN 'PRIMARY' ELSE 'INDEX' END AS index_type,
        key.column_name
      FROM iiindexes idx
      LEFT JOIN iikeys key ON key.index_name = idx.index_name
      WHERE idx.table_owner NOT IN ('$ingres', 'sys')
        ${filter}
      ORDER BY idx.table_owner, idx.table_name, idx.index_name
    `;
    const result = await this.executeQuery(connection, sql);
    return result.success ? result.rows : [];
  }
}

export default IngresAdapter;