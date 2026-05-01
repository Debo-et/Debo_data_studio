// backend/src/database/adapters/hsqldb.adapter.ts

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

// JDBC library imports
import * as JDBC from 'jdbc';
const jinst = require('jdbc/lib/jinst');

// ============================================================================
// HSQLDB Connection Wrapper
// ============================================================================
export class HSQLDBConnection {
  private jdbc: any;
  private isConnected: boolean = false;

  constructor(private config: DatabaseConfig) {}

  async connect(): Promise<void> {
    if (this.isConnected) return;

    if (!jinst.isJvmCreated()) {
      // Java runtime options
      jinst.addOption('-Xrs');
      // Setup classpath - path to hsqldb.jar
      if (this.config.driverPath) {
        jinst.setupClasspath([this.config.driverPath]);
      } else {
        jinst.setupClasspath(['./drivers/hsqldb.jar']);
      }
    }

    // JDBC connection URL
    const url = this.buildJdbcUrl();

    this.jdbc = new JDBC({
      url: url,
      user: this.config.user || 'SA',
      password: this.config.password || '',
      minpoolsize: 1,
      maxpoolsize: 10,
    });

    return new Promise((resolve, reject) => {
      this.jdbc.initialize((err: Error) => {
        if (err) {
          reject(new Error(`Failed to initialize JDBC: ${err.message}`));
        } else {
          this.isConnected = true;
          resolve();
        }
      });
    });
  }

  async disconnect(): Promise<void> {
    return new Promise((resolve, reject) => {
      if (this.jdbc && this.isConnected) {
        this.jdbc.close((err: Error) => {
          if (err) reject(err);
          else {
            this.isConnected = false;
            this.jdbc = null;
            resolve();
          }
        });
      } else {
        resolve();
      }
    });
  }

  async executeQuery(sql: string, params: any[] = []): Promise<any[]> {
    return new Promise((resolve, reject) => {
      if (!this.jdbc) reject(new Error('Connection not initialized'));

      this.jdbc.reserve((err: Error, conn: any) => {
        if (err) reject(err);

        conn.query(sql, params, (err: Error, results: any) => {
          if (err) {
            this.jdbc.release(conn);
            reject(err);
          } else {
            this.jdbc.release(conn);
            resolve(results);
          }
        });
      });
    });
  }

  async executeUpdate(sql: string, params: any[] = []): Promise<any> {
    return new Promise((resolve, reject) => {
      if (!this.jdbc) reject(new Error('Connection not initialized'));

      this.jdbc.reserve((err: Error, conn: any) => {
        if (err) reject(err);

        conn.update(sql, params, (err: Error, result: any) => {
          if (err) {
            this.jdbc.release(conn);
            reject(err);
          } else {
            this.jdbc.release(conn);
            resolve(result);
          }
        });
      });
    });
  }

  private buildJdbcUrl(): string {
    const host = this.config.host || 'localhost';
    const port = this.config.port || '9001';
    const dbname = this.config.dbname;

    // Determine HSQLDB mode
    if (dbname === ':memory:') {
      return 'jdbc:hsqldb:mem:testdb';
    } else if (this.config.isServer) {
      return `jdbc:hsqldb:hsql://${host}:${port}/${dbname}`;
    } else {
      return `jdbc:hsqldb:file:${dbname}`;
    }
  }
}

// ============================================================================
// HSQLDB Schema Inspector
// ============================================================================
export class HSQLDBSchemaInspector {
  private connection: HSQLDBConnection | null = null;
  private schema: string;

  constructor(config: DatabaseConfig) {
    this.schema = config.schema || 'PUBLIC';
    this.connection = new HSQLDBConnection(config);
  }

  getConnection(): HSQLDBConnection {
    if (!this.connection) {
      throw new Error('Connection not initialized');
    }
    return this.connection;
  }

  async testConnection(): Promise<{ success: boolean; version?: string; error?: string }> {
    try {
      const testConn = new HSQLDBConnection({ dbname: ':memory:', schema: this.schema });
      await testConn.connect();
      const result = await testConn.executeQuery('SELECT DATABASE_VERSION() as version FROM (VALUES(1))');
      await testConn.disconnect();
      return {
        success: true,
        version: result[0]?.VERSION,
      };
    } catch (err) {
      return {
        success: false,
        error: err instanceof Error ? err.message : String(err),
      };
    }
  }

  async getTables(): Promise<any[]> {
    const conn = this.getConnection();
    const query = `
      SELECT
        TABLE_SCHEMA,
        TABLE_NAME,
        TABLE_TYPE,
        REMARKS
      FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_SCHEMA = '${this.schema}'
      ORDER BY TABLE_NAME
    `;

    const tablesList = await conn.executeQuery(query);
    const tables: any[] = [];

    for (const row of tablesList) {
      const columns = await this.getTableColumns(row.TABLE_NAME);
      const rowCount = await this.getRowCount(row.TABLE_NAME);

      tables.push({
        schemaname: row.TABLE_SCHEMA,
        tablename: row.TABLE_NAME,
        tabletype: row.TABLE_TYPE === 'VIEW' ? 'VIEW' : 'TABLE',
        columns: columns,
        comment: row.REMARKS,
        rowCount: rowCount,
        size: null, // Not available via JDBC easily
        originalData: row,
      });
    }

    return tables;
  }

  private async getTableColumns(tableName: string): Promise<ColumnMetadata[]> {
    const conn = this.getConnection();
    const query = `
      SELECT
        COLUMN_NAME,
        DATA_TYPE,
        IS_NULLABLE,
        COLUMN_DEFAULT,
        ORDINAL_POSITION
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = '${this.schema}'
        AND TABLE_NAME = '${tableName}'
      ORDER BY ORDINAL_POSITION
    `;

    const columns = await conn.executeQuery(query);
    return columns.map((col: any, idx: number) => ({
      name: col.COLUMN_NAME,
      type: col.DATA_TYPE,
      dataType: col.DATA_TYPE,
      nullable: col.IS_NULLABLE === 'YES',
      default: col.COLUMN_DEFAULT,
      comment: null,
      length: null,
      precision: null,
      scale: null,
      isIdentity: false, // Not directly available
      ordinalPosition: idx + 1,
    }));
  }

  private async getRowCount(tableName: string): Promise<number> {
    try {
      const conn = this.getConnection();
      const result = await conn.executeQuery(`SELECT COUNT(*) as count FROM ${this.schema}.${tableName}`);
      return result[0]?.COUNT || 0;
    } catch {
      return 0;
    }
  }

  async getDatabaseInfo(): Promise<{ version: string; name: string; encoding?: string; collation?: string }> {
    const conn = this.getConnection();
    const result = await conn.executeQuery(
      'SELECT DATABASE_VERSION() as version, DATABASE_NAME() as name FROM (VALUES(1))'
    );
    return {
      version: result[0]?.VERSION || 'unknown',
      name: result[0]?.NAME || this.schema,
      encoding: 'UTF-8',
      collation: 'UCS_BASIC',
    };
  }

  async executeQuery(
    sql: string,
    params: any[] = [],
    options?: { maxRows?: number; timeout?: number; autoDisconnect?: boolean }
  ): Promise<QueryResult> {
    const conn = this.getConnection();
    const startTime = Date.now();

    try {
      let finalSql = sql;
      if (options?.maxRows && !sql.toLowerCase().includes('limit')) {
        finalSql = `SELECT * FROM (${sql}) LIMIT ${options.maxRows}`;
      }

      const rows = await conn.executeQuery(finalSql, params);
      return {
        success: true,
        rows: rows,
        rowCount: rows.length,
        duration: Date.now() - startTime,
        sql: finalSql,
      };
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error),
        duration: Date.now() - startTime,
        sql,
      };
    }
  }

  async executeTransaction(queries: Array<{ sql: string; params?: any[] }>): Promise<QueryResult[]> {
    const conn = this.getConnection();
    const results: QueryResult[] = [];

    try {
      await conn.executeUpdate('BEGIN TRANSACTION');

      for (const query of queries) {
        const result = await this.executeQuery(query.sql, query.params || []);
        results.push(result);
        if (!result.success) {
          await conn.executeUpdate('ROLLBACK');
          throw new Error(`Transaction failed at: ${query.sql}. Error: ${result.error}`);
        }
      }

      await conn.executeUpdate('COMMIT');
      return results;
    } catch (error) {
      await conn.executeUpdate('ROLLBACK').catch(() => {});
      throw error;
    }
  }

  setSchema(schema: string): void {
    this.schema = schema.toUpperCase(); // HSQLDB uses uppercase schema names
  }
}

// ============================================================================
// HSQLDB Adapter (implements IBaseDatabaseInspector)
// ============================================================================
export class HSQLDBAdapter implements IBaseDatabaseInspector {
  private inspector: HSQLDBSchemaInspector | null = null;
  private connectionInstance: HSQLDBConnection | null = null;

  async connect(config: DatabaseConfig): Promise<DatabaseConnection> {
    try {
      this.inspector = new HSQLDBSchemaInspector(config);
      await this.inspector.getConnection().connect();
      this.connectionInstance = this.inspector.getConnection();
      return this.connectionInstance as unknown as DatabaseConnection;
    } catch (error) {
      throw new Error(`Failed to connect to HSQLDB: ${error instanceof Error ? error.message : String(error)}`);
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
      console.error('Error disconnecting from HSQLDB:', error);
    }
  }

  async testConnection(config: DatabaseConfig): Promise<{ success: boolean; version?: string; error?: string }> {
    const inspector = new HSQLDBSchemaInspector(config);
    return inspector.testConnection();
  }

  async getTables(_connection: DatabaseConnection, options?: InspectionOptions): Promise<TableInfo[]> {
    if (!this.inspector) throw new Error('Inspector not initialized. Call connect() first.');

    try {
      if (options?.schema) this.inspector.setSchema(options.schema);
      return await this.inspector.getTables();
    } catch (error) {
      throw new Error(`Failed to get tables: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  async getTableColumns(_connection: DatabaseConnection, tables: TableInfo[]): Promise<TableInfo[]> {
    // Columns are already included in getTables()
    return tables;
  }

  async getDatabaseInfo(_connection: DatabaseConnection): Promise<DatabaseVersionInfo> {
    if (!this.inspector) throw new Error('Inspector not initialized. Call connect() first.');

    try {
      const info = await this.inspector.getDatabaseInfo();
      return {
        version: info.version,
        name: info.name,
        encoding: info.encoding,
        collation: info.collation,
      };
    } catch (error) {
      throw new Error(`Failed to get database info: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  async executeQuery(
    _connection: DatabaseConnection,
    sql: string,
    options?: QueryExecutionOptions
  ): Promise<QueryResult> {
    if (!this.inspector) throw new Error('Inspector not initialized. Call connect() first.');

    const params = options?.params || [];
    return await this.inspector.executeQuery(sql, params, {
      maxRows: options?.maxRows,
      timeout: options?.timeout,
      autoDisconnect: options?.autoDisconnect,
    });
  }

  async executeTransaction(
    _connection: DatabaseConnection,
    queries: Array<{ sql: string; params?: any[] }>
  ): Promise<QueryResult[]> {
    if (!this.inspector) throw new Error('Inspector not initialized. Call connect() first.');
    return await this.inspector.executeTransaction(queries);
  }

  async getTableConstraints(
    _connection: DatabaseConnection,
    schema: string,
    table: string
  ): Promise<any[]> {
    if (!this.inspector) throw new Error('Inspector not initialized. Call connect() first.');

    try {
      const conn = this.inspector.getConnection();
      const query = `
        SELECT
          CONSTRAINT_NAME,
          CONSTRAINT_TYPE,
          COLUMN_NAME,
          REFERENCED_TABLE_NAME,
          REFERENCED_COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = '${schema}' AND TABLE_NAME = '${table}'
      `;
      const constraints = await conn.executeQuery(query);
      return constraints;
    } catch (error) {
      console.error('Failed to get constraints:', error);
      return [];
    }
  }

  async getSchemas(_connection: DatabaseConnection): Promise<string[]> {
    if (!this.inspector) throw new Error('Inspector not initialized. Call connect() first.');

    try {
      const conn = this.inspector.getConnection();
      const result = await conn.executeQuery('SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA');
      return result.map((row: any) => row.SCHEMA_NAME);
    } catch (error) {
      console.error('Failed to get schemas:', error);
      return ['PUBLIC'];
    }
  }

  async getFunctions(_connection: DatabaseConnection, schema?: string): Promise<any[]> {
    if (!this.inspector) throw new Error('Inspector not initialized. Call connect() first.');

    try {
      const conn = this.inspector.getConnection();
      const schemaFilter = schema ? `AND ROUTINE_SCHEMA = '${schema}'` : '';
      const query = `
        SELECT
          ROUTINE_SCHEMA,
          ROUTINE_NAME,
          ROUTINE_TYPE,
          DATA_TYPE as RETURN_TYPE
        FROM INFORMATION_SCHEMA.ROUTINES
        WHERE ROUTINE_SCHEMA NOT IN ('SYSTEM', 'INFORMATION_SCHEMA')
        ${schemaFilter}
      `;
      const functions = await conn.executeQuery(query);
      return functions;
    } catch (error) {
      console.error('Failed to get functions:', error);
      return [];
    }
  }

  async getIndexes(_connection: DatabaseConnection, tableName?: string): Promise<any[]> {
    if (!this.inspector) throw new Error('Inspector not initialized. Call connect() first.');

    try {
      const conn = this.inspector.getConnection();
      let query = `
        SELECT
          TABLE_SCHEMA,
          TABLE_NAME,
          INDEX_NAME,
          TYPE as INDEX_TYPE,
          IS_UNIQUE
        FROM INFORMATION_SCHEMA.INDEXES
        WHERE TABLE_SCHEMA = '${this.inspector['schema']}'
      `;
      if (tableName) query += ` AND TABLE_NAME = '${tableName}'`;
      const indexes = await conn.executeQuery(query);
      return indexes;
    } catch (error) {
      console.error('Failed to get indexes:', error);
      return [];
    }
  }
}

export default HSQLDBAdapter;