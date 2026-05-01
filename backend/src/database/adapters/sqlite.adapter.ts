// backend/src/database/adapters/sqlite.adapter.ts

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
import sqlite3 from 'sqlite3';
import { promisify } from 'util';

// ============================================================================
// SQLite Connection Wrapper
// ============================================================================

export class SQLiteConnection {
  private db: sqlite3.Database | null = null;
  private isConnected = false;

  constructor(private readonly dbPath: string) {}

  async connect(): Promise<void> {
    if (this.isConnected) return;

    return new Promise((resolve, reject) => {
      this.db = new sqlite3.Database(this.dbPath, (err) => {
        if (err) {
          reject(new Error(`Failed to connect to SQLite database: ${err.message}`));
        } else {
          this.isConnected = true;
          resolve();
        }
      });
    });
  }

  async disconnect(): Promise<void> {
    if (!this.db || !this.isConnected) return;

    return new Promise((resolve, reject) => {
      this.db!.close((err) => {
        if (err) {
          reject(new Error(`Error disconnecting from SQLite: ${err.message}`));
        } else {
          this.isConnected = false;
          this.db = null;
          resolve();
        }
      });
    });
  }

  async run(sql: string, params: any[] = []): Promise<{ lastID?: number; changes?: number }> {
    if (!this.db) throw new Error('Database not connected');

    return new Promise((resolve, reject) => {
      this.db!.run(sql, params, function(this: sqlite3.RunResult, err) {
        if (err) reject(err);
        else resolve({ lastID: this.lastID, changes: this.changes });
      });
    });
  }

  async all<T = any>(sql: string, params: any[] = []): Promise<T[]> {
    if (!this.db) throw new Error('Database not connected');

    return new Promise((resolve, reject) => {
      this.db!.all(sql, params, (err, rows) => {
        if (err) reject(err);
        else resolve(rows as T[]);
      });
    });
  }

  async get<T = any>(sql: string, params: any[] = []): Promise<T | undefined> {
    if (!this.db) throw new Error('Database not connected');

    return new Promise((resolve, reject) => {
      this.db!.get(sql, params, (err, row) => {
        if (err) reject(err);
        else resolve(row as T);
      });
    });
  }

  isConnectedFlag(): boolean {
    return this.isConnected;
  }
}

// ============================================================================
// SQLite Schema Inspector
// ============================================================================

export interface SQLiteInspectorConfig {
  dbPath: string;          // file path or ':memory:'
  schema?: string;         // SQLite only has 'main' and 'temp', default 'main'
}

export class SQLiteSchemaInspector {
  private connection: SQLiteConnection | null = null;
  private schema: string;

  constructor(config: SQLiteInspectorConfig) {
    this.schema = config.schema || 'main';
    this.connection = new SQLiteConnection(config.dbPath);
  }

  getConnection(): SQLiteConnection {
    if (!this.connection) {
      throw new Error('Connection not initialized');
    }
    return this.connection;
  }

  async testConnection(): Promise<{ success: boolean; version?: string; error?: string }> {
    try {
      const testConn = new SQLiteConnection((this.connection as any)?.dbPath || ':memory:');
      await testConn.connect();
      const result = await testConn.get<{ sqlite_version: string }>('SELECT sqlite_version() as sqlite_version');
      await testConn.disconnect();
      return {
        success: true,
        version: result?.sqlite_version
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

    // Get all tables and views from sqlite_master
    const objects = await conn.all<{
      name: string;
      type: string;
      tbl_name: string;
      sql: string;
    }>(
      `SELECT name, type, tbl_name, sql
       FROM ${this.schema}.sqlite_master
       WHERE type IN ('table', 'view')
       ORDER BY name`
    );

    const tables: any[] = [];

    for (const obj of objects) {
      // Get column info using PRAGMA table_info
      const columnsRaw = await conn.all<{
        cid: number;
        name: string;
        type: string;
        notnull: number;
        dflt_value: string | null;
        pk: number;
      }>(`PRAGMA ${this.schema}.table_info(${obj.name})`);

      // Get row count
      let rowCount = 0;
      try {
        const countResult = await conn.get<{ count: number }>(`SELECT COUNT(*) as count FROM ${this.schema}.${obj.name}`);
        rowCount = countResult?.count || 0;
      } catch {
        rowCount = 0;
      }

      // Build column metadata
      const columns: ColumnMetadata[] = columnsRaw.map((col, idx) => ({
        name: col.name,
        type: col.type,
        dataType: col.type,
        nullable: col.notnull === 0,
        default: col.dflt_value,
        comment: null, // SQLite does not store column comments
        length: null,
        precision: null,
        scale: null,
        isIdentity: col.pk === 1, // primary key might be autoincrement, but not strictly identity
        ordinalPosition: idx + 1
      }));

      tables.push({
        schemaname: this.schema,
        tablename: obj.name,
        tabletype: obj.type === 'view' ? 'VIEW' : 'TABLE',
        columns: columns,
        comment: null, // table comment not available in SQLite
        rowCount: rowCount,
        size: null,    // per-table size not easily available in SQLite
        originalData: obj
      });
    }

    return tables;
  }

  async getDatabaseInfo(): Promise<{ version: string; name: string; encoding?: string; collation?: string }> {
    const conn = this.getConnection();
    const versionRow = await conn.get<{ sqlite_version: string }>('SELECT sqlite_version() as sqlite_version');
    const encodingRow = await conn.get<{ encoding: string }>("PRAGMA encoding");

    return {
      version: versionRow?.sqlite_version || 'unknown',
      name: (this.connection as any)?.dbPath || 'memory',
      encoding: encodingRow?.encoding,
      collation: 'BINARY' // default collation in SQLite
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
      // Apply maxRows limit if needed
      let finalSql = sql;
      if (options?.maxRows && !sql.toLowerCase().includes('limit')) {
        finalSql = `${sql} LIMIT ${options.maxRows}`;
      }

      // Timeout simulation (basic)
      if (options?.timeout) {
        // SQLite doesn't support query timeouts natively; we'll rely on JS promise race
        const timeoutPromise = new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error(`Query timeout after ${options.timeout}ms`)), options.timeout)
        );
        const rows = await Promise.race([conn.all(finalSql, params), timeoutPromise]);
        return {
          success: true,
          rows: rows as any[],
          rowCount: (rows as any[]).length,
          duration: Date.now() - startTime,
          sql: finalSql
        };
      } else {
        const rows = await conn.all(finalSql, params);
        return {
          success: true,
          rows: rows,
          rowCount: rows.length,
          duration: Date.now() - startTime,
          sql: finalSql
        };
      }
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error),
        duration: Date.now() - startTime,
        sql
      };
    }
  }

  async executeTransaction(queries: Array<{ sql: string; params?: any[] }>): Promise<QueryResult[]> {
    const conn = this.getConnection();
    const results: QueryResult[] = [];

    try {
      await conn.run('BEGIN TRANSACTION');

      for (const query of queries) {
        const result = await this.executeQuery(query.sql, query.params || []);
        results.push(result);
        if (!result.success) {
          await conn.run('ROLLBACK');
          throw new Error(`Transaction failed at query: ${query.sql}. Error: ${result.error}`);
        }
      }

      await conn.run('COMMIT');
      return results;
    } catch (error) {
      await conn.run('ROLLBACK').catch(() => {});
      throw error;
    }
  }

  setSchema(schema: string): void {
    this.schema = schema;
  }
}

// ============================================================================
// SQLite Adapter (implements IBaseDatabaseInspector)
// ============================================================================

export class SQLiteAdapter implements IBaseDatabaseInspector {
  private inspector: SQLiteSchemaInspector | null = null;
  private connectionInstance: SQLiteConnection | null = null;

  async connect(config: DatabaseConfig): Promise<DatabaseConnection> {
    try {
      // SQLite only needs dbname (file path or ':memory:')
      const dbPath = config.dbname || ':memory:';
      this.inspector = new SQLiteSchemaInspector({
        dbPath,
        schema: config.schema || 'main'
      });

      await this.inspector.getConnection().connect();
      this.connectionInstance = this.inspector.getConnection();

      return this.connectionInstance as unknown as DatabaseConnection;
    } catch (error) {
      throw new Error(`Failed to connect to SQLite: ${error instanceof Error ? error.message : String(error)}`);
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
      console.error('Error disconnecting from SQLite:', error);
    }
  }

  async testConnection(config: DatabaseConfig): Promise<{ success: boolean; version?: string; error?: string }> {
    const inspector = new SQLiteSchemaInspector({
      dbPath: config.dbname || ':memory:',
      schema: config.schema || 'main'
    });
    return inspector.testConnection();
  }

  async getTables(_connection: DatabaseConnection, options?: InspectionOptions): Promise<TableInfo[]> {
    if (!this.inspector) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    try {
      if (options?.schema) {
        this.inspector.setSchema(options.schema);
      }

      const tables = await this.inspector.getTables();

      // Convert to standard TableInfo format (already matches structure)
      return tables as TableInfo[];
    } catch (error) {
      throw new Error(`Failed to get tables: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  async getTableColumns(_connection: DatabaseConnection, tables: TableInfo[]): Promise<TableInfo[]> {
    // SQLite inspector already includes columns in getTables()
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
    if (!this.inspector) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    const params = options?.params || [];
    const pgOptions = {
      maxRows: options?.maxRows,
      timeout: options?.timeout,
      autoDisconnect: options?.autoDisconnect
    };

    return await this.inspector.executeQuery(sql, params, pgOptions);
  }

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
      throw new Error(`Transaction failed: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  async getTableConstraints(
    _connection: DatabaseConnection,
    schema: string,
    table: string
  ): Promise<any[]> {
    if (!this.inspector) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    try {
      const conn = this.inspector.getConnection();
      // Get primary keys, unique constraints, foreign keys
      const constraints = [];

      // Primary keys from table_info
      const pkInfo = await conn.all<{ name: string }>(
        `PRAGMA ${schema}.table_info(${table}) WHERE pk > 0`
      );
      if (pkInfo.length > 0) {
        constraints.push({
          type: 'PRIMARY KEY',
          columns: pkInfo.map(col => col.name),
          name: `${table}_pkey`
        });
      }

      // Unique constraints from indexes
      const indexes = await conn.all<{ name: string; unique: number; origin: string }>(
        `PRAGMA ${schema}.index_list(${table})`
      );
      for (const idx of indexes) {
        if (idx.unique && idx.origin !== 'pk') {
          const idxInfo = await conn.all<{ name: string }>(
            `PRAGMA ${schema}.index_info(${idx.name})`
          );
          constraints.push({
            type: 'UNIQUE',
            columns: idxInfo.map(col => col.name),
            name: idx.name
          });
        }
      }

      // Foreign keys
      const fks = await conn.all<any>(`PRAGMA ${schema}.foreign_key_list(${table})`);
      for (const fk of fks) {
        constraints.push({
          type: 'FOREIGN KEY',
          columns: [fk.from],
          referencedTable: fk.table,
          referencedColumns: [fk.to],
          name: `${table}_${fk.id}_fk`
        });
      }

      return constraints;
    } catch (error) {
      console.error('Failed to get constraints:', error);
      return [];
    }
  }

  async getSchemas(_connection: DatabaseConnection): Promise<string[]> {
    if (!this.inspector) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    try {
      const conn = this.inspector.getConnection();
      const schemas = await conn.all<{ name: string }>(
        "PRAGMA database_list"
      );
      return schemas.map(s => s.name);
    } catch (error) {
      console.error('Failed to get schemas:', error);
      return ['main'];
    }
  }

  // Additional SQLite-specific methods (mirroring PostgreSQL adapter)

  async getFunctions(_connection: DatabaseConnection, _schema?: string): Promise<any[]> {
    // SQLite does not have stored procedures/functions in the traditional sense.
    // Return empty array, but could be extended for custom functions.
    return [];
  }

  async getIndexes(_connection: DatabaseConnection, tableName?: string): Promise<any[]> {
    if (!this.inspector) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    try {
      const conn = this.inspector.getConnection();
      let query = `
        SELECT 
          'main' as schema_name,
          tbl_name as table_name,
          name as index_name,
          sql as index_def,
          'BTREE' as index_type,
          CAST("unique" AS INTEGER) as is_unique,
          0 as is_primary
        FROM sqlite_master
        WHERE type = 'index'
      `;
      if (tableName) {
        query += ` AND tbl_name = '${tableName}'`;
      }
      const indexes = await conn.all(query);
      return indexes;
    } catch (error) {
      console.error('Failed to get indexes:', error);
      return [];
    }
  }
}

export default SQLiteAdapter;