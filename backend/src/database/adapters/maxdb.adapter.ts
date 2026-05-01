// backend/src/database/adapters/maxdb.adapter.ts

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

import * as JDBC from 'jdbc';
const jinst = require('jdbc/lib/jinst');

// ============================================================================
// MaxDB (SAP DB) Connection Wrapper
// ============================================================================
export class MaxDBConnection {
  private jdbc: any;
  private isConnected = false;
  private readonly config: DatabaseConfig;

  constructor(config: DatabaseConfig) {
    this.config = config;
  }

  async connect(): Promise<void> {
    if (this.isConnected) return;

    // Initialize JVM if not already done
    if (!jinst.isJvmCreated()) {
      jinst.addOption('-Xrs');
      const maxdbJar = this.config.driverPath || './drivers/sapdbc.jar';
      jinst.setupClasspath([maxdbJar]);
    }

    // Build the correct JDBC URL based on mode
    const url = this.buildJdbcUrl();

    this.jdbc = new JDBC({
      url: url,
      user: this.config.user || '',
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
    const port = this.config.port || '7200'; // Default MaxDB network port[reference:3]
    const dbname = this.config.dbname;

    // Determine mode: embedded vs network
    const useEmbedded = this.config.embedded === true ||
                        (!this.config.host && !this.config.port);

    if (useEmbedded) {
      // Embedded/file mode: Use file-based connection
      return `jdbc:sapdb:${dbname}`;
    } else {
      // Network server mode (TCP/IP)[reference:4][reference:5]
      return `jdbc:sapdb://${host}:${port}/${dbname}`;
    }
  }
}

// ============================================================================
// MaxDB Schema Inspector
// ============================================================================
export class MaxDBSchemaInspector {
  private connection: MaxDBConnection | null = null;
  private schema: string;

  constructor(config: DatabaseConfig) {
    this.schema = (config.schema || 'SAPDB').toUpperCase(); // MaxDB uses uppercase by default
    this.connection = new MaxDBConnection(config);
  }

  getConnection(): MaxDBConnection {
    if (!this.connection) {
      throw new Error('Connection not initialized');
    }
    return this.connection;
  }

  async testConnection(): Promise<{ success: boolean; version?: string; error?: string }> {
    try {
      const testConn = new MaxDBConnection({
        dbname: 'MEMORYDB',
        embedded: true
      });
      await testConn.connect();
      const result = await testConn.executeQuery('SELECT * FROM DOMAIN.DATABASENAME');
      await testConn.disconnect();
      return {
        success: true,
        version: result[0]?.NAME || 'MaxDB'
      };
    } catch (err) {
      return {
        success: false,
        error: err instanceof Error ? err.message : String(err)
      };
    }
  }

  async getTables(): Promise<any[]> {
    const conn = this.getConnection();

    const query = `
      SELECT
        TABLESCHEMA,
        TABLENAME,
        TABLETYPE
      FROM DOMAIN.TABLES
      WHERE TABLESCHEMA = '${this.schema}'
        AND TABLETYPE IN ('T', 'V')
      ORDER BY TABLENAME
    `;

    const tablesList = await conn.executeQuery(query);
    const tables: any[] = [];

    for (const row of tablesList) {
      const columns = await this.getTableColumns(row.TABLENAME);
      const rowCount = await this.getRowCount(row.TABLENAME);

      tables.push({
        schemaname: row.TABLESCHEMA,
        tablename: row.TABLENAME,
        tabletype: row.TABLETYPE === 'V' ? 'VIEW' : 'TABLE',
        columns: columns,
        comment: null,
        rowCount: rowCount,
        size: null,
        originalData: row
      });
    }

    return tables;
  }

  private async getTableColumns(tableName: string): Promise<ColumnMetadata[]> {
    const conn = this.getConnection();
    const query = `
      SELECT
        COLUMNNAME,
        COLUMNDATATYPE,
        NULLABLE,
        DEFAULTVALUE,
        COLUMNOFFSET
      FROM DOMAIN.COLUMNS
      WHERE TABLESCHEMA = '${this.schema}'
        AND TABLENAME = '${tableName}'
      ORDER BY COLUMNOFFSET
    `;

    const columns = await conn.executeQuery(query);
    return columns.map((col: any, idx: number) => ({
      name: col.COLUMNNAME,
      type: col.COLUMNDATATYPE,
      dataType: col.COLUMNDATATYPE,
      nullable: col.NULLABLE === 'Y',
      default: col.DEFAULTVALUE,
      comment: null,
      length: null,
      precision: null,
      scale: null,
      isIdentity: col.DEFAULTVALUE?.toUpperCase().includes('GENERATED BY DEFAULT'),
      ordinalPosition: col.COLUMNOFFSET || idx + 1
    }));
  }

  private async getRowCount(tableName: string): Promise<number> {
    try {
      const conn = this.getConnection();
      const result = await conn.executeQuery(
        `SELECT COUNT(*) as cnt FROM ${this.schema}.${tableName}`
      );
      return result[0]?.CNT || 0;
    } catch {
      return 0;
    }
  }

  async getDatabaseInfo(): Promise<{
    version: string;
    name: string;
    encoding?: string;
    collation?: string
  }> {
    const conn = this.getConnection();
    const versionResult = await conn.executeQuery('SELECT * FROM DOMAIN.DBVERSIONINFO');
    const nameResult = await conn.executeQuery('SELECT * FROM DOMAIN.DATABASENAME');
    return {
      version: versionResult[0]?.VERSIONSTRING || 'unknown',
      name: nameResult[0]?.NAME || this.schema,
      encoding: 'UTF-8',
      collation: 'UCS_BASIC'
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
      // Emulate LIMIT using MaxDB's FETCH FIRST syntax[reference:6]
      if (options?.maxRows && !sql.toLowerCase().includes('fetch')) {
        finalSql = `${sql} FETCH FIRST ${options.maxRows} ROWS ONLY`;
      }

      let rows: any[];
      if (options?.timeout) {
        const timeoutPromise = new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error(`Query timeout after ${options.timeout}ms`)), options.timeout)
        );
        rows = await Promise.race([conn.executeQuery(finalSql, params), timeoutPromise]) as any[];
      } else {
        rows = await conn.executeQuery(finalSql, params);
      }
      
      return {
        success: true,
        rows: rows,
        rowCount: rows.length,
        duration: Date.now() - startTime,
        sql: finalSql
      };
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error),
        duration: Date.now() - startTime,
        sql
      };
    }
  }

  async executeTransaction(
    queries: Array<{ sql: string; params?: any[] }>
  ): Promise<QueryResult[]> {
    const conn = this.getConnection();
    const results: QueryResult[] = [];

    try {
      await conn.executeUpdate('BEGIN TRANSACTION');

      for (const query of queries) {
        const result = await this.executeQuery(query.sql, query.params || []);
        results.push(result);
        if (!result.success) {
          await conn.executeUpdate('ROLLBACK');
          throw new Error(`Transaction failed: ${result.error}`);
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
    this.schema = schema.toUpperCase();
  }
}

// ============================================================================
// MaxDB Adapter (implements IBaseDatabaseInspector)
// ============================================================================
export class MaxDBAdapter implements IBaseDatabaseInspector {
  private inspector: MaxDBSchemaInspector | null = null;
  private connectionInstance: MaxDBConnection | null = null;

  async connect(config: DatabaseConfig): Promise<DatabaseConnection> {
    try {
      this.inspector = new MaxDBSchemaInspector(config);
      await this.inspector.getConnection().connect();
      this.connectionInstance = this.inspector.getConnection();
      return this.connectionInstance as unknown as DatabaseConnection;
    } catch (error) {
      throw new Error(`Failed to connect to MaxDB: ${error instanceof Error ? error.message : String(error)}`);
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
      console.error('Error disconnecting from MaxDB:', error);
    }
  }

  async testConnection(config: DatabaseConfig): Promise<{ success: boolean; version?: string; error?: string }> {
    const inspector = new MaxDBSchemaInspector(config);
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
        collation: info.collation
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
      autoDisconnect: options?.autoDisconnect
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
          CONSTRAINTNAME,
          TYPE,
          COLUMNNAME,
          REFERENCED_TABLE
        FROM DOMAIN.KEYCONSTRAINTS
        WHERE TABLESCHEMA = '${schema}'
          AND TABLENAME = '${table}'
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
      const result = await conn.executeQuery('SELECT SCHEMANAME FROM DOMAIN.SCHEMAS ORDER BY SCHEMANAME');
      return result.map((row: any) => row.SCHEMANAME);
    } catch (error) {
      console.error('Failed to get schemas:', error);
      return ['SAPDB'];
    }
  }

  async getFunctions(_connection: DatabaseConnection, schema?: string): Promise<any[]> {
    // MaxDB stores functions in DOMAIN.PROCEDURES
    try {
      const conn = this.inspector!.getConnection();
      const schemaFilter = schema ? `AND s.SCHEMANAME = '${schema}'` : '';
      const query = `
        SELECT
          SCHEMANAME,
          PROCEDURENAME,
          RETURNTYPE
        FROM DOMAIN.PROCEDURES
        WHERE SCHEMANAME NOT IN ('SYS', 'DOMAIN')
        ${schemaFilter}
      `;
      return await conn.executeQuery(query);
    } catch (error) {
      console.error('Failed to get functions:', error);
      return [];
    }
  }

  async getIndexes(_connection: DatabaseConnection, tableName?: string): Promise<any[]> {
    try {
      const conn = this.inspector!.getConnection();
      let query = `
        SELECT
          TABLESCHEMA,
          TABLENAME,
          INDEXNAME,
          ISUNIQUE
        FROM DOMAIN.INDEXES
        WHERE 1=1
      `;
      if (tableName) {
        query += ` AND TABLENAME = '${tableName}'`;
      }
      const indexes = await conn.executeQuery(query);
      return indexes;
    } catch (error) {
      console.error('Failed to get indexes:', error);
      return [];
    }
  }
}

export default MaxDBAdapter;