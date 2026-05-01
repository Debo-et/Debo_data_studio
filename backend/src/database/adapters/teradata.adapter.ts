// backend/src/database/adapters/teradata.adapter.ts

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

// Teradata Node.js driver
const teradata = require('teradatasql');

/**
 * Teradata Database Adapter
 * Implements the base inspector interface using native Teradata driver
 */
export class TeradataAdapter implements IBaseDatabaseInspector {
  private connection: any = null;
  private currentSchema: string = '';

  /**
   * Connect to Teradata database
   */
  async connect(config: DatabaseConfig): Promise<DatabaseConnection> {
    try {
      // Build Teradata connection parameters
      const connectionParams: any = {
        host: config.host || 'localhost',
        user: config.user,
        password: config.password,
        database: config.dbname || '',
        log: '0' // Minimal logging
      };

      // Add port if specified (Teradata default is 1025)
      if (config.port) {
        connectionParams.port = config.port;
      }

      // Optional: set log mechanism if provided
      if (config.logonMech) {
        connectionParams.logmech = config.logonMech;
      }

      // Create connection
      this.connection = teradata.connect(connectionParams);
      
      // Set default schema if provided
      if (config.schema) {
        await this.setSchema(config.schema);
        this.currentSchema = config.schema;
      }

      return this.connection as unknown as DatabaseConnection;
    } catch (error) {
      throw new Error(`Failed to connect to Teradata: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Set current schema/database for the session
   */
  private async setSchema(schema: string): Promise<void> {
    if (!this.connection) {
      throw new Error('Not connected to database');
    }
    await this.executeQueryDirect(`DATABASE "${schema}"`);
  }

  /**
   * Disconnect from Teradata database
   */
  async disconnect(_connection: DatabaseConnection): Promise<void> {
    try {
      if (this.connection) {
        this.connection.close();
      }
      this.connection = null;
    } catch (error) {
      console.error('Error disconnecting from Teradata:', error);
    }
  }

  /**
   * Test connection without full schema inspection
   */
  async testConnection(config: DatabaseConfig): Promise<{ success: boolean; version?: string; error?: string }> {
    let testConn = null;
    try {
      const connectionParams: any = {
        host: config.host || 'localhost',
        user: config.user,
        password: config.password,
        database: config.dbname || '',
        log: '0'
      };

      if (config.port) connectionParams.port = config.port;
      if (config.logonMech) connectionParams.logmech = config.logonMech;

      testConn = teradata.connect(connectionParams);
      
      // Get Teradata version
      const result = await this.executeQueryOnConnection(testConn, 'SELECT InfoData FROM DBC.DBCInfoV WHERE InfoKey = \'VERSION\'');
      const version = result.rows && result.rows.length > 0 ? result.rows[0].InfoData : 'Unknown';
      
      testConn.close();
      return { success: true, version };
    } catch (error) {
      if (testConn) {
        try { testConn.close(); } catch(e) {}
      }
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error)
      };
    }
  }

  /**
   * Execute query on a given connection (internal helper)
   */
  private async executeQueryOnConnection(conn: any, sql: string, params?: any[]): Promise<QueryResult> {
    return new Promise((resolve, reject) => {
      const callback = (err: Error | null, rows: any[]) => {
        if (err) {
          reject(err);
        } else {
          resolve({
            success: true,
            rows: rows || [],
            rowCount: rows ? rows.length : 0,
            fields: rows && rows.length > 0 ? Object.keys(rows[0]).map(name => ({ name, type: 'unknown' })) : []
          });
        }
      };

      if (params && params.length > 0) {
        conn.execute(sql, params, callback);
      } else {
        conn.execute(sql, callback);
      }
    });
  }

  /**
   * Execute arbitrary SQL query on the current connection
   */
  private async executeQueryDirect(sql: string, params?: any[]): Promise<QueryResult> {
    if (!this.connection) {
      throw new Error('Not connected to database');
    }
    return this.executeQueryOnConnection(this.connection, sql, params);
  }

  /**
   * Get tables and views from Teradata database
   */
  async getTables(_connection: DatabaseConnection, options?: InspectionOptions): Promise<TableInfo[]> {
    if (!this.connection) {
      throw new Error('Not connected to database. Call connect() first.');
    }

    try {
      const schemaFilter = options?.schema ? `AND DatabaseName = '${options.schema}'` : 
                          (this.currentSchema ? `AND DatabaseName = '${this.currentSchema}'` : '');
      
      // Query to get tables/views with their columns
      const query = `
        SELECT 
          t.DatabaseName AS schemaname,
          t.TableName AS tablename,
          t.TableKind AS tabletype,
          c.ColumnName,
          c.ColumnType,
          c.ColumnLength,
          c.DecimalTotalDigits AS precision,
          c.DecimalFractionalDigits AS scale,
          c.Nullable,
          c.DefaultValue,
          c.Comment,
          c.ColumnId AS ordinal_position
        FROM DBC.TablesV t
        LEFT JOIN DBC.ColumnsV c 
          ON t.DatabaseName = c.DatabaseName 
          AND t.TableName = c.TableName
        WHERE t.TableKind IN ('T', 'V')
          ${schemaFilter}
        ORDER BY t.DatabaseName, t.TableName, c.ColumnId
      `;

      const result = await this.executeQueryDirect(query);
      
      if (!result.success || !result.rows) {
        return [];
      }

      // Group rows by table
      const tableMap = new Map<string, any>();
      
      for (const row of result.rows) {
        const tableKey = `${row.schemaname}.${row.tablename}`;
        
        if (!tableMap.has(tableKey)) {
          tableMap.set(tableKey, {
            schemaname: row.schemaname,
            tablename: row.tablename,
            tabletype: row.tabletype === 'T' ? 'TABLE' : 'VIEW',
            columns: [],
            comment: '',
            rowCount: 0,
            size: 0
          });
        }
        
        const table = tableMap.get(tableKey);
        
        // Add column if exists
        if (row.columnname) {
          table.columns.push({
            name: row.columnname,
            type: this.mapTeradataType(row.columntype, row.columnlength, row.precision, row.scale),
            dataType: row.columntype,
            nullable: row.nullable === 'Y',
            default: row.defaultvalue,
            comment: row.comment,
            length: row.columnlength,
            precision: row.precision,
            scale: row.scale,
            isIdentity: false, // Teradata doesn't have auto-increment in the same sense
            ordinalPosition: row.ordinal_position
          } as ColumnMetadata);
        }
      }

      // Get row counts and sizes for each table
      const tables = Array.from(tableMap.values());
      for (const table of tables) {
        try {
          const countQuery = `SELECT COUNT(*) as cnt FROM "${table.schemaname}"."${table.tablename}"`;
          const countResult = await this.executeQueryDirect(countQuery);
          if (countResult.success && countResult.rows && countResult.rows.length > 0) {
            table.rowCount = parseInt(countResult.rows[0].cnt, 10);
          }
        } catch (e) {
          // Ignore count errors
          table.rowCount = 0;
        }
      }

      return tables;
    } catch (error) {
      throw new Error(`Failed to get tables: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Get columns for specific tables (already handled in getTables, but provided for interface)
   */
  async getTableColumns(_connection: DatabaseConnection, tables: TableInfo[]): Promise<TableInfo[]> {
    return tables;
  }

  /**
   * Get database version information
   */
  async getDatabaseInfo(_connection: DatabaseConnection): Promise<DatabaseVersionInfo> {
    if (!this.connection) {
      throw new Error('Not connected to database. Call connect() first.');
    }

    try {
      // Get version
      const versionResult = await this.executeQueryDirect('SELECT InfoData FROM DBC.DBCInfoV WHERE InfoKey = \'VERSION\'');
      const version = versionResult.success && versionResult.rows && versionResult.rows.length > 0 
        ? versionResult.rows[0].InfoData 
        : 'Unknown';

      // Get database name
      const dbResult = await this.executeQueryDirect('SELECT DATABASE as current_db');
      const dbName = dbResult.success && dbResult.rows && dbResult.rows.length > 0 
        ? dbResult.rows[0].current_db 
        : '';

      // Get encoding/collation info (Teradata uses server character set)
      const charsetResult = await this.executeQueryDirect('SELECT InfoData FROM DBC.DBCInfoV WHERE InfoKey = \'DBServerCharSet\'');
      const encoding = charsetResult.success && charsetResult.rows && charsetResult.rows.length > 0 
        ? charsetResult.rows[0].InfoData 
        : 'ASCII';

      return {
        version,
        name: dbName,
        encoding,
        collation: '' // Teradata doesn't expose collation easily
      };
    } catch (error) {
      throw new Error(`Failed to get database info: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Execute arbitrary SQL query
   */
  async executeQuery(
    _connection: DatabaseConnection,
    sql: string,
    options?: QueryExecutionOptions
  ): Promise<QueryResult> {
    if (!this.connection) {
      throw new Error('Not connected to database. Call connect() first.');
    }

    try {
      const params = options?.params || [];
      
      // Handle maxRows limitation
      let limitedSql = sql;
      if (options?.maxRows && options.maxRows > 0) {
        // Teradata uses TOP n syntax
        if (!limitedSql.toLowerCase().includes(' select ')) {
          limitedSql = limitedSql.replace(/select/i, `SELECT TOP ${options.maxRows}`);
        }
      }

      // Execute with timeout if specified (Teradata driver doesn't directly support timeout)
      let result: QueryResult;
      if (options?.timeout) {
        const timeoutPromise = new Promise((_, reject) => {
          setTimeout(() => reject(new Error(`Query timeout after ${options.timeout}ms`)), options.timeout);
        });
        result = await Promise.race([this.executeQueryDirect(limitedSql, params), timeoutPromise]) as QueryResult;
      } else {
        result = await this.executeQueryDirect(limitedSql, params);
      }

      // Auto-disconnect if requested
      if (options?.autoDisconnect) {
        await this.disconnect(_connection);
      }

      return result;
    } catch (error) {
      throw new Error(`Failed to execute query: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Execute multiple queries in a transaction
   */
  async executeTransaction(
    _connection: DatabaseConnection,
    queries: Array<{ sql: string; params?: any[] }>
  ): Promise<QueryResult[]> {
    if (!this.connection) {
      throw new Error('Not connected to database. Call connect() first.');
    }

    const results: QueryResult[] = [];

    try {
      // Begin transaction
      await this.executeQueryDirect('BEGIN TRANSACTION');

      // Execute each query
      for (const query of queries) {
        const result = await this.executeQueryDirect(query.sql, query.params || []);
        results.push(result);
      }

      // Commit transaction
      await this.executeQueryDirect('COMMIT');
      
      return results;
    } catch (error) {
      // Rollback on error
      try {
        await this.executeQueryDirect('ROLLBACK');
      } catch (rollbackError) {
        console.error('Error rolling back transaction:', rollbackError);
      }
      throw new Error(`Transaction failed: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Get table constraints (Primary Keys, Foreign Keys, Unique constraints)
   */
  async getTableConstraints(_connection: DatabaseConnection, schema: string, table: string): Promise<any[]> {
    if (!this.connection) {
      throw new Error('Not connected to database. Call connect() first.');
    }

    try {
      const query = `
        SELECT 
          i.DatabaseName AS schema_name,
          i.TableName AS table_name,
          i.IndexName AS constraint_name,
          i.IndexType AS constraint_type,
          i.UniqueFlag AS is_unique,
          c.ColumnName
        FROM DBC.IndicesV i
        LEFT JOIN DBC.IndexColumnsV c 
          ON i.DatabaseName = c.DatabaseName 
          AND i.TableName = c.TableName 
          AND i.IndexName = c.IndexName
        WHERE i.DatabaseName = '${schema}'
          AND i.TableName = '${table}'
          AND i.IndexType IN ('P', 'F', 'U')  -- Primary, Foreign, Unique
        ORDER BY i.IndexName, c.ColumnPosition
      `;

      const result = await this.executeQueryDirect(query);
      
      if (!result.success || !result.rows) {
        return [];
      }

      // Group constraints
      const constraintMap = new Map();
      for (const row of result.rows) {
        const key = `${row.schema_name}.${row.table_name}.${row.constraint_name}`;
        if (!constraintMap.has(key)) {
          constraintMap.set(key, {
            constraint_name: row.constraint_name,
            constraint_type: row.constraint_type === 'P' ? 'PRIMARY KEY' : 
                            (row.constraint_type === 'F' ? 'FOREIGN KEY' : 'UNIQUE'),
            is_unique: row.is_unique === 'Y',
            columns: []
          });
        }
        if (row.columnname) {
          constraintMap.get(key).columns.push(row.columnname);
        }
      }

      return Array.from(constraintMap.values());
    } catch (error) {
      console.error('Failed to get constraints:', error);
      return [];
    }
  }

  /**
   * Get schema list
   */
  async getSchemas(_connection: DatabaseConnection): Promise<string[]> {
    if (!this.connection) {
      throw new Error('Not connected to database. Call connect() first.');
    }

    try {
      // Get all databases (schemas)
      const query = `
        SELECT DatabaseName 
        FROM DBC.DatabasesV 
        WHERE DatabaseType IN ('D', 'U')  -- D = Database, U = User
          AND DatabaseName NOT IN ('DBC', 'SYSLIB', 'SYSSPATIAL', 'SYSUDTLIB', 'SYSUDTLIB', 'SYSUDTLIB')
        ORDER BY DatabaseName
      `;
      
      const result = await this.executeQueryDirect(query);
      
      if (result.success && result.rows) {
        return result.rows.map(row => row.DatabaseName);
      }
      
      return [];
    } catch (error) {
      console.error('Failed to get schemas:', error);
      return [];
    }
  }

  /**
   * Get database functions/procedures
   */
  async getFunctions(connection: DatabaseConnection, schema?: string): Promise<any[]> {
    if (!this.connection) {
      throw new Error('Not connected to database. Call connect() first.');
    }

    try {
      const schemaFilter = schema ? `AND DatabaseName = '${schema}'` : '';
      
      const query = `
        SELECT 
          DatabaseName AS schema,
          TableName AS function_name,
          TableKind AS object_type,
          Comment AS description
        FROM DBC.TablesV
        WHERE TableKind IN ('P', 'F', 'M')  -- P = Procedure, F = Function, M = Macro
          ${schemaFilter}
        ORDER BY DatabaseName, TableName
      `;
      
      const result = await this.executeQueryDirect(query);
      
      if (result.success && result.rows) {
        return result.rows.map(row => ({
          schema: row.schema,
          name: row.function_name,
          type: row.object_type === 'P' ? 'PROCEDURE' : (row.object_type === 'F' ? 'FUNCTION' : 'MACRO'),
          description: row.description || ''
        }));
      }
      
      return [];
    } catch (error) {
      console.error('Failed to get functions:', error);
      return [];
    }
  }

  /**
   * Get database indexes
   */
  async getIndexes(connection: DatabaseConnection, tableName?: string): Promise<any[]> {
    if (!this.connection) {
      throw new Error('Not connected to database. Call connect() first.');
    }

    try {
      const tableFilter = tableName ? `AND i.TableName = '${tableName}'` : '';
      
      const query = `
        SELECT 
          i.DatabaseName AS schema_name,
          i.TableName AS table_name,
          i.IndexName AS index_name,
          i.IndexType AS index_type,
          i.UniqueFlag AS is_unique,
          c.ColumnName AS column_name,
          c.ColumnPosition AS column_position
        FROM DBC.IndicesV i
        LEFT JOIN DBC.IndexColumnsV c 
          ON i.DatabaseName = c.DatabaseName 
          AND i.TableName = c.TableName 
          AND i.IndexName = c.IndexName
        WHERE i.IndexType NOT IN ('P', 'F')  -- Exclude primary/foreign keys
          ${tableFilter}
        ORDER BY i.DatabaseName, i.TableName, i.IndexName, c.ColumnPosition
      `;
      
      const result = await this.executeQueryDirect(query);
      
      if (!result.success || !result.rows) {
        return [];
      }

      // Group indexes
      const indexMap = new Map();
      for (const row of result.rows) {
        const key = `${row.schema_name}.${row.table_name}.${row.index_name}`;
        if (!indexMap.has(key)) {
          indexMap.set(key, {
            schema_name: row.schema_name,
            table_name: row.table_name,
            index_name: row.index_name,
            index_type: row.index_type === 'J' ? 'JOIN INDEX' :
                       (row.index_type === 'H' ? 'HASH INDEX' : 'SECONDARY INDEX'),
            is_unique: row.is_unique === 'Y',
            columns: []
          });
        }
        if (row.column_name) {
          indexMap.get(key).columns.push(row.column_name);
        }
      }
      
      return Array.from(indexMap.values());
    } catch (error) {
      console.error('Failed to get indexes:', error);
      return [];
    }
  }

  /**
   * Map Teradata data types to standard SQL types
   */
  private mapTeradataType(typeCode: string, length: number, precision: number, scale: number): string {
    const typeMap: { [key: string]: string } = {
      'I': 'INTEGER',
      'I8': 'BIGINT',
      'I2': 'SMALLINT',
      'F': 'FLOAT',
      'D': 'DECIMAL',
      'CV': 'VARCHAR',
      'CF': 'CHAR',
      'D': 'DATE',
      'TS': 'TIMESTAMP',
      'T': 'TIME',
      'BF': 'BYTE',
      'BV': 'VARBYTE',
      'BO': 'BLOB',
      'CL': 'CLOB'
    };
    
    let baseType = typeMap[typeCode] || 'VARCHAR';
    
    if (baseType === 'DECIMAL' && precision) {
      return `DECIMAL(${precision},${scale || 0})`;
    }
    
    if ((baseType === 'VARCHAR' || baseType === 'CHAR') && length) {
      return `${baseType}(${length})`;
    }
    
    return baseType;
  }
}

export default TeradataAdapter;