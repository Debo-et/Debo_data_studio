// backend/src/database/adapters/h2.adapter.ts

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

import JDBC from 'jdbc';
import path from 'path';

// ============================================================================
// H2 Connection Wrapper (using JDBC)
// ============================================================================
export class H2Connection implements DatabaseConnection {
  private jdbc: any = null;
  private config: DatabaseConfig;
  private connected: boolean = false;

  constructor(config: DatabaseConfig) {
    this.config = config;
  }

  async connect(): Promise<void> {
    try {
      // H2 connection URL formats:
      // Embedded: jdbc:h2:~/test
      // TCP: jdbc:h2:tcp://localhost/~/test
      // Use embedded if no host specified, else TCP
      let url: string;
      if (!this.config.host || this.config.host === 'localhost') {
        // Embedded mode - file path
        const dbPath = this.config.dbname || '~/test';
        url = `jdbc:h2:${dbPath}`;
      } else {
        // TCP mode
        const port = this.config.port || '9092';
        const dbPath = this.config.dbname || '~/test';
        url = `jdbc:h2:tcp://${this.config.host}:${port}/${dbPath}`;
      }

      // Add optional parameters
      if (this.config.schema) {
        url += `;SCHEMA=${this.config.schema.toUpperCase()}`;
      }

      const jdbcConfig = {
        url: url,
        user: this.config.user || 'sa',
        password: this.config.password || '',
        // Path to H2 JDBC driver JAR - adjust as needed
        libpath: path.join(__dirname, '../../drivers/h2-2.2.224.jar'),
        // Driver class
        drivername: 'org.h2.Driver',
      };

      this.jdbc = new JDBC(jdbcConfig);
      await new Promise((resolve, reject) => {
        this.jdbc.initialize((err: Error) => {
          if (err) reject(err);
          else resolve(null);
        });
      });

      await new Promise((resolve, reject) => {
        this.jdbc.open((err: Error, conn: any) => {
          if (err) reject(err);
          else {
            this.connected = true;
            resolve(conn);
          }
        });
      });
    } catch (error) {
      throw new Error(`H2 connection failed: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  async disconnect(): Promise<void> {
    if (this.jdbc && this.connected) {
      await new Promise((resolve, reject) => {
        this.jdbc.close((err: Error) => {
          if (err) reject(err);
          else {
            this.connected = false;
            resolve(null);
          }
        });
      });
    }
  }

  async query(sql: string, params: any[] = []): Promise<any[]> {
    if (!this.jdbc || !this.connected) {
      throw new Error('Connection not established');
    }

    // Parameterized query
    const result = await new Promise<any>((resolve, reject) => {
      this.jdbc.query(sql, params, (err: Error, results: any) => {
        if (err) reject(err);
        else resolve(results);
      });
    });

    // result is an object with 'results' array and 'metadata'
    if (result && result.results && Array.isArray(result.results)) {
      return result.results;
    }
    return [];
  }

  async execute(sql: string, params: any[] = []): Promise<{ rows: any[]; affectedRows?: number }> {
    const rows = await this.query(sql, params);
    return { rows };
  }

  async beginTransaction(): Promise<void> {
    await this.execute('SET AUTOCOMMIT FALSE');
  }

  async commit(): Promise<void> {
    await this.execute('COMMIT');
    await this.execute('SET AUTOCOMMIT TRUE');
  }

  async rollback(): Promise<void> {
    await this.execute('ROLLBACK');
    await this.execute('SET AUTOCOMMIT TRUE');
  }

  get underlyingConnection(): any {
    return this.jdbc;
  }
}

// ============================================================================
// H2 Schema Inspector
// ============================================================================
export class H2SchemaInspector {
  private connection: H2Connection;
  private schema: string;

  constructor(config: DatabaseConfig) {
    this.connection = new H2Connection(config);
    this.schema = (config.schema || 'PUBLIC').toUpperCase();
  }

  getConnection(): H2Connection {
    return this.connection;
  }

  setSchema(schema: string): void {
    this.schema = schema.toUpperCase();
  }

  async testConnection(): Promise<{ success: boolean; version?: string; error?: string }> {
    try {
      await this.connection.connect();
      const rows = await this.connection.query('SELECT H2VERSION() AS version');
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
    const sql = `
      SELECT 
        TABLE_SCHEMA AS schemaname,
        TABLE_NAME AS tablename,
        TABLE_TYPE AS tabletype,
        REMARKS AS comment
      FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_SCHEMA = ?
        AND TABLE_TYPE IN ('TABLE', 'VIEW')
      ORDER BY TABLE_SCHEMA, TABLE_NAME
    `;
    const tables = await this.connection.query(sql, [this.schema]);

    const result = [];
    for (const table of tables) {
      const columns = await this.getTableColumns(table.SCHEMANAME, table.TABLENAME);
      const rowCount = await this.getRowCount(table.SCHEMANAME, table.TABLENAME);
      result.push({
        schemaname: table.SCHEMANAME,
        tablename: table.TABLENAME,
        tabletype: table.TABLETYPE === 'VIEW' ? 'view' : 'table',
        columns,
        comment: table.COMMENT,
        rowCount,
        size: null, // H2 size not easily retrieved
      });
    }
    return result;
  }

  private async getTableColumns(schema: string, table: string): Promise<any[]> {
    const sql = `
      SELECT 
        COLUMN_NAME AS name,
        TYPE_NAME AS type,
        IS_NULLABLE AS nullable,
        COLUMN_DEFAULT AS default,
        REMARKS AS comment,
        CHARACTER_MAXIMUM_LENGTH AS length,
        NUMERIC_PRECISION AS precision,
        NUMERIC_SCALE AS scale,
        ORDINAL_POSITION AS ordinal_position
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
      ORDER BY ORDINAL_POSITION
    `;
    const cols = await this.connection.query(sql, [schema, table]);
    return cols.map((col: any) => ({
      name: col.NAME,
      type: col.TYPE,
      nullable: col.NULLABLE === 'YES' || col.NULLABLE === true,
      default: col.DEFAULT,
      comment: col.COMMENT,
      length: col.LENGTH,
      precision: col.PRECISION,
      scale: col.SCALE,
      isIdentity: false, // H2 can have IDENTITY columns but not from this query
      ordinalPosition: col.ORDINAL_POSITION,
    }));
  }

  private async getRowCount(schema: string, table: string): Promise<number> {
    try {
      // H2 uses double quotes for case-sensitive identifiers
      const rows = await this.connection.query(`SELECT COUNT(*) AS cnt FROM "${schema}"."${table}"`);
      return rows[0]?.CNT || 0;
    } catch {
      return 0;
    }
  }

  async getDatabaseInfo(): Promise<{ version: string; name: string; encoding: string; collation: string }> {
    const versionRows = await this.connection.query('SELECT H2VERSION() AS version');
    const version = versionRows[0]?.VERSION || 'Unknown';
    const nameRows = await this.connection.query('SELECT DATABASE_PATH() AS path');
    const name = nameRows[0]?.PATH || 'unknown';
    return { version, name, encoding: 'UTF-8', collation: 'BINARY' };
  }

  async executeQuery(
    sql: string,
    params: any[] = [],
    options?: { maxRows?: number; timeout?: number; autoDisconnect?: boolean }
  ): Promise<QueryResult> {
    try {
      // H2 does not support query timeout via JDBC in node-jdbc easily; ignore for now
      let rows = await this.connection.query(sql, params);
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
// H2 Adapter – implements IBaseDatabaseInspector
// ============================================================================
export class H2Adapter implements IBaseDatabaseInspector {
  private inspector: H2SchemaInspector | null = null;
  private connectionInstance: H2Connection | null = null;

  async connect(config: DatabaseConfig): Promise<DatabaseConnection> {
    try {
      this.inspector = new H2SchemaInspector(config);
      await this.inspector.getConnection().connect();
      this.connectionInstance = this.inspector.getConnection();
      return this.connectionInstance as unknown as DatabaseConnection;
    } catch (error) {
      throw new Error(`Failed to connect to H2: ${error instanceof Error ? error.message : String(error)}`);
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
      console.error('Error disconnecting from H2:', error);
    }
  }

  async testConnection(config: DatabaseConfig): Promise<{ success: boolean; version?: string; error?: string }> {
    const tempInspector = new H2SchemaInspector(config);
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
    // Columns already included from getTables()
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
    const sql = `
      SELECT 
        CONSTRAINT_NAME,
        CONSTRAINT_TYPE,
        COLUMN_LIST
      FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
      WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    `;
    const result = await this.inspector.executeQuery(sql, [_schema, _table]);
    return result.success ? result.rows : [];
  }

  async getSchemas(connection: DatabaseConnection): Promise<string[]> {
    const sql = `
      SELECT SCHEMA_NAME
      FROM INFORMATION_SCHEMA.SCHEMATA
      WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA')
      ORDER BY SCHEMA_NAME
    `;
    const result = await this.executeQuery(connection, sql);
    if (result.success && result.rows) {
      return result.rows.map((row: any) => row.SCHEMA_NAME);
    }
    return ['PUBLIC'];
  }

  // H2-specific additional methods
  async getFunctions(connection: DatabaseConnection, schema?: string): Promise<any[]> {
    const schemaFilter = schema ? `AND ALIAS_SCHEMA = '${schema}'` : '';
    const sql = `
      SELECT 
        ALIAS_SCHEMA AS schema,
        ALIAS_NAME AS function_name,
        JAVA_CLASS,
        JAVA_METHOD,
        RETURNS_RESULT AS returns_result
      FROM INFORMATION_SCHEMA.FUNCTION_ALIASES
      WHERE ALIAS_SCHEMA NOT IN ('INFORMATION_SCHEMA')
        ${schemaFilter}
      ORDER BY ALIAS_SCHEMA, ALIAS_NAME
    `;
    const result = await this.executeQuery(connection, sql);
    return result.success ? result.rows : [];
  }

  async getIndexes(connection: DatabaseConnection, tableName?: string): Promise<any[]> {
    const tableFilter = tableName ? `AND TABLE_NAME = '${tableName}'` : '';
    const sql = `
      SELECT 
        TABLE_SCHEMA AS schema_name,
        TABLE_NAME,
        INDEX_NAME,
        NON_UNIQUE,
        COLUMN_NAME,
        ORDINAL_POSITION
      FROM INFORMATION_SCHEMA.INDEXES
      WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
        ${tableFilter}
      ORDER BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, ORDINAL_POSITION
    `;
    const result = await this.executeQuery(connection, sql);
    return result.success ? result.rows : [];
  }
}

export default H2Adapter;