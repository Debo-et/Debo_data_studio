// backend/src/database/adapters/javadb.adapter.ts

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
// JavaDB (Apache Derby) Connection Wrapper
// ============================================================================
export class JavaDBConnection {
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
      // Use default Derby JAR location or custom from config
      const derbyJar = this.config.driverPath || './drivers/derby.jar';
      jinst.setupClasspath([derbyJar]);
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
    const port = this.config.port || '1527';   // Default Derby network port
    const dbname = this.config.dbname;
    const createFlag = this.config.createDatabase ? ';create=true' : '';

    // Determine mode: network client vs embedded
    // Embedded mode is used when 'embedded' is explicitly true OR no host/port provided
    const useEmbedded = this.config.embedded === true ||
                        (!this.config.host && !this.config.port);

    if (useEmbedded) {
      // Embedded mode: jdbc:derby:databaseName[;attributes]
      return `jdbc:derby:${dbname}${createFlag}`;
    } else {
      // Network client mode: jdbc:derby://host:port/databaseName[;attributes]
      return `jdbc:derby://${host}:${port}/${dbname}${createFlag}`;
    }
  }
}

// ============================================================================
// JavaDB Schema Inspector
// ============================================================================
export class JavaDBSchemaInspector {
  private connection: JavaDBConnection | null = null;
  private schema: string;

  constructor(config: DatabaseConfig) {
    this.schema = (config.schema || 'APP').toUpperCase(); // Derby uses uppercase schemas
    this.connection = new JavaDBConnection(config);
  }

  getConnection(): JavaDBConnection {
    if (!this.connection) {
      throw new Error('Connection not initialized');
    }
    return this.connection;
  }

  async testConnection(): Promise<{ success: boolean; version?: string; error?: string }> {
    try {
      const testConn = new JavaDBConnection({
        dbname: 'memory:testDB',
        embedded: true,
        createDatabase: true
      });
      await testConn.connect();
      const result = await testConn.executeQuery('VALUES (CURRENT_TIMESTAMP)');
      await testConn.disconnect();
      return {
        success: true,
        version: 'Derby (version info not available via simple test)'
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

    // Query using Derby system catalogs
    const query = `
      SELECT
        s.SCHEMANAME,
        t.TABLENAME,
        t.TABLETYPE
      FROM SYS.SYSTABLES t
      JOIN SYS.SYSSCHEMAS s ON t.SCHEMAID = s.SCHEMAID
      WHERE s.SCHEMANAME = '${this.schema}'
        AND t.TABLETYPE IN ('T', 'V')
      ORDER BY t.TABLENAME
    `;

    const tablesList = await conn.executeQuery(query);
    const tables: any[] = [];

    for (const row of tablesList) {
      const columns = await this.getTableColumns(row.TABLENAME);
      const rowCount = await this.getRowCount(row.TABLENAME);

      tables.push({
        schemaname: row.SCHEMANAME,
        tablename: row.TABLENAME,
        tabletype: row.TABLETYPE === 'V' ? 'VIEW' : 'TABLE',
        columns: columns,
        comment: null,               // Derby does not store table comments
        rowCount: rowCount,
        size: null,                  // Not easily available via JDBC
        originalData: row
      });
    }

    return tables;
  }

  private async getTableColumns(tableName: string): Promise<ColumnMetadata[]> {
    const conn = this.getConnection();
    const query = `
      SELECT
        c.COLUMNNAME,
        c.COLUMNDATATYPE,
        c.COLUMNDEFAULT,
        c.NULLS
      FROM SYS.SYSCOLUMNS c
      JOIN SYS.SYSTABLES t ON c.REFERENCEID = t.TABLEID
      JOIN SYS.SYSSCHEMAS s ON t.SCHEMAID = s.SCHEMAID
      WHERE s.SCHEMANAME = '${this.schema}'
        AND t.TABLENAME = '${tableName}'
      ORDER BY c.COLUMNNUMBER
    `;

    const columns = await conn.executeQuery(query);
    return columns.map((col: any, idx: number) => ({
      name: col.COLUMNNAME,
      type: col.COLUMNDATATYPE,
      dataType: col.COLUMNDATATYPE,
      nullable: col.NULLS === 'Y',
      default: col.COLUMNDEFAULT,
      comment: null,
      length: null,
      precision: null,
      scale: null,
      isIdentity: false,                   // Requires additional identity column check
      ordinalPosition: idx + 1
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
    const versionResult = await conn.executeQuery(
      'SELECT GETDATABASEPRODUCTVERSION() AS VERSION FROM SYSIBM.SYSDUMMY1'
    );
    const dbName = this.schema;
    return {
      version: versionResult[0]?.VERSION || 'unknown',
      name: dbName,
      encoding: 'UTF-8',
      collation: 'UCS_BASIC'               // Default collation in Derby
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
      // Apply LIMIT emulation using Derby's proprietary syntax (fetch first)
      if (options?.maxRows && !sql.toLowerCase().includes('fetch')) {
        finalSql = `${sql} FETCH FIRST ${options.maxRows} ROWS ONLY`;
      }

      const rows = await conn.executeQuery(finalSql, params);
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
      await conn.executeUpdate('BEGIN');

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
// JavaDB Adapter (implements IBaseDatabaseInspector)
// ============================================================================
export class JavaDBAdapter implements IBaseDatabaseInspector {
  private inspector: JavaDBSchemaInspector | null = null;
  private connectionInstance: JavaDBConnection | null = null;

  async connect(config: DatabaseConfig): Promise<DatabaseConnection> {
    try {
      this.inspector = new JavaDBSchemaInspector(config);
      await this.inspector.getConnection().connect();
      this.connectionInstance = this.inspector.getConnection();
      return this.connectionInstance as unknown as DatabaseConnection;
    } catch (error) {
      throw new Error(`Failed to connect to JavaDB: ${error instanceof Error ? error.message : String(error)}`);
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
      console.error('Error disconnecting from JavaDB:', error);
    }
  }

  async testConnection(config: DatabaseConfig): Promise<{ success: boolean; version?: string; error?: string }> {
    const inspector = new JavaDBSchemaInspector(config);
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
          c.CONSTRAINTNAME,
          c.TYPE AS CONSTRAINT_TYPE,
          k.COLUMNNAME
        FROM SYS.SYSCONSTRAINTS c
        JOIN SYS.SYSKEYS k ON c.CONSTRAINTID = k.CONSTRAINTID
        JOIN SYS.SYSTABLES t ON c.TABLEID = t.TABLEID
        JOIN SYS.SYSSCHEMAS s ON t.SCHEMAID = s.SCHEMAID
        WHERE s.SCHEMANAME = '${schema}'
          AND t.TABLENAME = '${table}'
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
      const result = await conn.executeQuery('SELECT SCHEMANAME FROM SYS.SYSSCHEMAS');
      return result.map((row: any) => row.SCHEMANAME);
    } catch (error) {
      console.error('Failed to get schemas:', error);
      return ['APP'];
    }
  }

  async getFunctions(_connection: DatabaseConnection, schema?: string): Promise<any[]> {
    // Derby stores routines in SYS.SYSROUTINES
    try {
      const conn = this.inspector!.getConnection();
      const schemaFilter = schema ? `AND s.SCHEMANAME = '${schema}'` : '';
      const query = `
        SELECT
          s.SCHEMANAME,
          r.ROUTINENAME,
          r.ROUTINETYPE,
          r.RETURNTYPE
        FROM SYS.SYSROUTINES r
        JOIN SYS.SYSSCHEMAS s ON r.SCHEMAID = s.SCHEMAID
        WHERE 1=1 ${schemaFilter}
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
          s.SCHEMANAME,
          t.TABLENAME,
          c.CONGLOMERATENAME AS INDEXNAME,
          c.ISCONSTRAINT
        FROM SYS.SYSCONGLOMERATES c
        JOIN SYS.SYSTABLES t ON c.TABLEID = t.TABLEID
        JOIN SYS.SYSSCHEMAS s ON t.SCHEMAID = s.SCHEMAID
        WHERE c.ISINDEX = TRUE
      `;
      if (tableName) {
        query += ` AND t.TABLENAME = '${tableName}'`;
      }
      const indexes = await conn.executeQuery(query);
      return indexes;
    } catch (error) {
      console.error('Failed to get indexes:', error);
      return [];
    }
  }
}

export default JavaDBAdapter;