// backend/src/database/adapters/vertica.adapter.ts

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
// Vertica Connection Wrapper (ODBC)
// ============================================================================
export class VerticaConnection implements DatabaseConnection {
  private connection: odbc.Connection | null = null;
  private config: DatabaseConfig;

  constructor(config: DatabaseConfig) {
    this.config = config;
  }

  // ---------- Fix 1: implement required 'connected' property ----------
  get connected(): boolean {
    return this.connection !== null;
  }

  async connect(): Promise<void> {
    try {
      // Vertica ODBC connection string
      // Format: Driver={Vertica};Server=host;Port=5433;Database=dbname;UID=user;PWD=password;
      const connString =
        `Driver={Vertica};` +
        `Server=${this.config.host || 'localhost'};` +
        `Port=${this.config.port || '5433'};` +
        `Database=${this.config.dbname};` +
        `UID=${this.config.user};` +
        `PWD=${this.config.password};`;
      this.connection = await odbc.connect(connString);
    } catch (error) {
      throw new Error(`Vertica connection failed: ${error instanceof Error ? error.message : String(error)}`);
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

  // ---------- Fix 2: return Promise<any[]> instead of odbc.QueryResult ----------
  async execute(sql: string, params: any[] = []): Promise<any[]> {
    if (!this.connection) {
      throw new Error('Connection not established');
    }
    return await this.connection.query(sql, params);
  }

  async beginTransaction(): Promise<void> {
    if (!this.connection) throw new Error('No connection');
    await this.connection.beginTransaction();
  }

  // ---------- Fix 3: odbc uses commit() and rollback(), not commitTransaction/rollbackTransaction ----------
  async commit(): Promise<void> {
    if (!this.connection) throw new Error('No connection');
    await this.connection.commit();
  }

  async rollback(): Promise<void> {
    if (!this.connection) throw new Error('No connection');
    await this.connection.rollback();
  }

  get underlyingConnection(): odbc.Connection | null {
    return this.connection;
  }
}

// ============================================================================
// Vertica Schema Inspector
// ============================================================================
export class VerticaSchemaInspector {
  private connection: VerticaConnection;
  private schema: string;

  constructor(config: DatabaseConfig) {
    this.connection = new VerticaConnection(config);
    this.schema = config.schema || 'public';
  }

  getConnection(): VerticaConnection {
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
  const sql = `SELECT ...`; // your existing query
  const tables = await this.connection.query(sql, [this.schema]);

  const result: any[] = [];   // <-- explicit type added here
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
      size: null,
    });
  }
  return result;
}

  private async getTableColumns(schema: string, table: string): Promise<any[]> {
    const sql = `
      SELECT 
        column_name AS name,
        data_type AS type,
        is_nullable = 't' AS nullable,
        data_default AS default,
        description AS comment,
        character_maximum_length AS length,
        numeric_precision AS precision,
        numeric_scale AS scale,
        ordinal_position
      FROM v_catalog.columns
      WHERE table_schema = ? AND table_name = ?
      ORDER BY ordinal_position
    `;
    const cols = await this.connection.query(sql, [schema, table]);
    return cols.map((col: any) => ({
      name: col.name,
      type: col.type,
      nullable: col.nullable,
      default: col.default,
      comment: col.comment,
      length: col.length,
      precision: col.precision,
      scale: col.scale,
      isIdentity: false, // Vertica identity info not in this query
      ordinalPosition: col.ordinal_position,
    }));
  }

  private async getRowCount(schema: string, table: string): Promise<number> {
    try {
      // Use projection statistics for performance, but fallback to count(*)
      const sql = `
        SELECT projection_row_count AS cnt
        FROM v_catalog.projections
        WHERE projection_schema = ? AND table_name = ?
        LIMIT 1
      `;
      const rows = await this.connection.query(sql, [schema, table]);
      if (rows && rows[0]?.cnt !== undefined) {
        return rows[0].cnt;
      }
      // Fallback to exact count
      const countRows = await this.connection.query(`SELECT COUNT(*) AS cnt FROM "${schema}"."${table}"`);
      return countRows[0]?.CNT || 0;
    } catch {
      return 0;
    }
  }

  async getDatabaseInfo(): Promise<{ version: string; name: string; encoding: string; collation: string }> {
    const versionRows = await this.connection.query('SELECT version() as version');
    const version = versionRows[0]?.version || 'Unknown';
    const nameRows = await this.connection.query('SELECT current_database() as db');
    const name = nameRows[0]?.db || 'unknown';
    // Vertica doesn't expose standard encoding/collation; provide defaults
    return { version, name, encoding: 'UTF8', collation: 'en_US' };
  }

  async executeQuery(
    sql: string,
    params: any[] = [],
    options?: { maxRows?: number; timeout?: number; autoDisconnect?: boolean }
  ): Promise<QueryResult> {
    try {
      if (options?.timeout) {
        // Vertica ODBC may support timeout, but not implemented here
      }
      const result = await this.connection.execute(sql, params);
      let rows = result as any[];
      if (options?.maxRows && rows.length > options.maxRows) {
        rows = rows.slice(0, options.maxRows);
      }

      // ---------- Fix 4: convert string[] fields to { name, type }[] ----------
      const fields = rows.length
        ? Object.keys(rows[0]).map(key => ({
            name: key,
            type: typeof rows[0][key],
          }))
        : [];

      return {
        success: true,
        rows,
        rowCount: rows.length,
        fields,
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
// Vertica Adapter – implements IBaseDatabaseInspector
// ============================================================================
export class VerticaAdapter implements IBaseDatabaseInspector {
  private inspector: VerticaSchemaInspector | null = null;
  private connectionInstance: VerticaConnection | null = null;

  async connect(config: DatabaseConfig): Promise<DatabaseConnection> {
    try {
      this.inspector = new VerticaSchemaInspector(config);
      await this.inspector.getConnection().connect();
      this.connectionInstance = this.inspector.getConnection();
      return this.connectionInstance as unknown as DatabaseConnection;
    } catch (error) {
      throw new Error(`Failed to connect to Vertica: ${error instanceof Error ? error.message : String(error)}`);
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
      console.error('Error disconnecting from Vertica:', error);
    }
  }

  async testConnection(config: DatabaseConfig): Promise<{ success: boolean; version?: string; error?: string }> {
    const tempInspector = new VerticaSchemaInspector(config);
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
    // Columns already fetched in getTables()
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

  // ---------- Fix 5: proper narrowing for optional rows in all returning methods ----------
  async getTableConstraints(_connection: DatabaseConnection, _schema: string, _table: string): Promise<any[]> {
    if (!this.inspector) throw new Error('Inspector not initialized');
    const sql = `
      SELECT 
        constraint_name,
        constraint_type,
        column_name
      FROM v_catalog.constraint_columns
      WHERE table_schema = ? AND table_name = ?
    `;
    const result = await this.inspector.executeQuery(sql, [_schema, _table]);
    return result.success && Array.isArray(result.rows) ? result.rows : [];
  }

  async getSchemas(connection: DatabaseConnection): Promise<string[]> {
    const sql = `
      SELECT schema_name
      FROM v_catalog.schemata
      WHERE schema_name NOT IN ('v_internal', 'v_catalog', 'v_monitor')
      ORDER BY schema_name
    `;
    const result = await this.executeQuery(connection, sql);
    if (result.success && Array.isArray(result.rows)) {
      return result.rows.map((row: any) => row.schema_name);
    }
    return ['public'];
  }

  async getFunctions(connection: DatabaseConnection, schema?: string): Promise<any[]> {
    // Caution: schema interpolation – ideally use parameterised queries
    const schemaFilter = schema ? `AND function_schema = '${schema}'` : '';
    const sql = `
      SELECT 
        function_schema AS schema,
        function_name,
        return_type,
        language,
        volatility,
        is_strict
      FROM v_catalog.functions
      WHERE function_schema NOT IN ('v_catalog', 'v_monitor')
        ${schemaFilter}
      ORDER BY function_schema, function_name
    `;
    const result = await this.executeQuery(connection, sql);
    return result.success && Array.isArray(result.rows) ? result.rows : [];
  }

  async getIndexes(connection: DatabaseConnection, tableName?: string): Promise<any[]> {
    // Caution: tableName interpolation – ideally use parameterised queries
    const tableFilter = tableName ? `AND table_name = '${tableName}'` : '';
    const sql = `
      SELECT 
        projection_schema AS schema_name,
        projection_name,
        table_name,
        is_super_projection,
        sort_keys,
        segmentation_spec
      FROM v_catalog.projections
      WHERE projection_schema NOT IN ('v_catalog', 'v_monitor')
        ${tableFilter}
      ORDER BY projection_schema, table_name, projection_name
    `;
    const result = await this.executeQuery(connection, sql);
    return result.success && Array.isArray(result.rows) ? result.rows : [];
  }
}

export default VerticaAdapter;