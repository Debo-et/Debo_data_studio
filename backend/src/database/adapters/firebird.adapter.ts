// backend/src/database/adapters/firebird.adapter.ts

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

/**
 * Firebird Database Adapter
 */
export class FirebirdAdapter implements IBaseDatabaseInspector {
  private connectionInstance: any = null;

  /**
   * Connect to Firebird database
   */
  async connect(config: DatabaseConfig): Promise<DatabaseConnection> {
    try {
      // Using node-firebird package
      // const firebird = await import('node-firebird');
      
      const connectionParams = {
        host: config.host || 'localhost',
        port: parseInt(config.port || '3050'),
        database: config.dbname,
        user: config.user || 'SYSDBA',
        password: config.password || 'masterkey',
        lowercase_keys: false,
        role: null,
        pageSize: 4096
      };
      
      // Mock connection
      this.connectionInstance = {
        connected: true,
        config: config,
        connectionParams: connectionParams
      };
      
      return this.connectionInstance;
    } catch (error) {
      throw new Error(`Failed to connect to Firebird: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Disconnect from Firebird database
   */
  async disconnect(_connection: DatabaseConnection): Promise<void> {
    try {
      this.connectionInstance = null;
    } catch (error) {
      console.error('Error disconnecting from Firebird:', error);
    }
  }

  /**
   * Test connection without full schema inspection
   */
  async testConnection(_config: DatabaseConfig): Promise<{ success: boolean; version?: string; error?: string }> {
    try {
      // Mock test connection
      return {
        success: true,
        version: 'Firebird 3.0'
      };
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error)
      };
    }
  }

  /**
   * Get tables and views from Firebird database
   */
  async getTables(connection: DatabaseConnection, _options?: InspectionOptions): Promise<TableInfo[]> {
    try {
      const query = `
        SELECT 
          RDB$RELATION_NAME as tablename,
          CASE RDB$VIEW_BLR
            WHEN NULL THEN 'TABLE'
            ELSE 'VIEW'
          END as tabletype,
          RDB$OWNER_NAME as schemaname,
          RDB$DESCRIPTION as comment
        FROM RDB$RELATIONS
        WHERE RDB$SYSTEM_FLAG = 0
          AND RDB$RELATION_TYPE IN (0, 1) -- 0=table, 1=view
        ORDER BY RDB$RELATION_NAME
      `;
      
      const result = await this.executeQuery(connection, query);
      
      if (!result.success) {
        throw new Error(result.error || 'Failed to get tables');
      }
      
      const tables: TableInfo[] = [];
      
      for (const row of result.rows || []) {
        const tableInfo: TableInfo = {
          schemaname: row.schemaname?.trim() || '',
          tablename: row.tablename?.trim() || '',
          tabletype: row.tabletype?.toLowerCase() || 'table',
          columns: []
        };
        
        // Get columns for this table
        const columns = await this.getTableColumnsInternal(connection, row.tablename?.trim());
        tableInfo.columns = columns;
        
        tables.push(tableInfo);
      }
      
      return tables;
    } catch (error) {
      throw new Error(`Failed to get tables: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Get columns for specific tables
   */
  async getTableColumns(connection: DatabaseConnection, tables: TableInfo[]): Promise<TableInfo[]> {
    const updatedTables = [...tables];
    
    for (const table of updatedTables) {
      try {
        const columns = await this.getTableColumnsInternal(connection, table.tablename);
        table.columns = columns;
      } catch (error) {
        console.error(`Failed to get columns for table ${table.tablename}:`, error);
        table.columns = [];
      }
    }
    
    return updatedTables;
  }

  /**
   * Get database version information
   */
  async getDatabaseInfo(connection: DatabaseConnection): Promise<DatabaseVersionInfo> {
    try {
      const query = `
        SELECT RDB$GET_CONTEXT('SYSTEM', 'ENGINE_VERSION') as version,
               RDB$GET_CONTEXT('SYSTEM', 'DB_NAME') as name
        FROM RDB$DATABASE
      `;
      
      const result = await this.executeQuery(connection, query);
      
      if (result.success && result.rows?.[0]) {
        return {
          version: result.rows[0].version,
          name: result.rows[0].name
        };
      }
      
      throw new Error('Could not retrieve database info');
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
    const startTime = Date.now();
    
    try {
      // Apply row limit for SELECT queries (Firebird uses FIRST)
      let finalSql = sql;
      if (options?.maxRows && sql.trim().toUpperCase().startsWith('SELECT')) {
        if (!finalSql.toUpperCase().includes('FIRST')) {
          const selectIndex = finalSql.toUpperCase().indexOf('SELECT');
          finalSql = finalSql.substring(0, selectIndex + 6) + 
                    ` FIRST ${options.maxRows}` + 
                    finalSql.substring(selectIndex + 6);
        }
      }
      
      // Mock execution
      const executionTime = Date.now() - startTime;
      
      return {
        success: true,
        rows: [],
        rowCount: 0,
        executionTime,
        affectedRows: 0
      };
    } catch (error) {
      const executionTime = Date.now() - startTime;
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error),
        rows: [],
        rowCount: 0,
        executionTime
      };
    }
  }

  /**
   * Execute multiple queries in a transaction
   */
  async executeTransaction(
    connection: DatabaseConnection, 
    queries: Array<{ sql: string; params?: any[] }>
  ): Promise<QueryResult[]> {
    const results: QueryResult[] = [];
    
    try {
      // Start transaction
      await this.executeQuery(connection, 'SET TRANSACTION');
      
      for (const { sql, params } of queries) {
        const result = await this.executeQuery(connection, sql, { params });
        results.push(result);
        
        if (!result.success) {
          // Rollback on error
          await this.executeQuery(connection, 'ROLLBACK');
          throw new Error(`Query failed: ${result.error}`);
        }
      }
      
      // Commit transaction
      await this.executeQuery(connection, 'COMMIT');
      
      return results;
    } catch (error) {
      // Ensure rollback on any unhandled error
      try {
        await this.executeQuery(connection, 'ROLLBACK');
      } catch (rollbackError) {
        console.error('Rollback failed:', rollbackError);
      }
      
      throw new Error(`Transaction failed: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Get table constraints
   */
  async getTableConstraints(connection: DatabaseConnection, table: string): Promise<any[]> {
    try {
      const query = `
        SELECT 
          RC.RDB$CONSTRAINT_NAME as name,
          RC.RDB$CONSTRAINT_TYPE as type,
          ISG.RDB$FIELD_NAME as column_name,
          REFC.RDB$RELATION_NAME as foreign_table,
          REFISG.RDB$FIELD_NAME as foreign_column
        FROM RDB$RELATION_CONSTRAINTS RC
        LEFT JOIN RDB$INDEX_SEGMENTS ISG ON RC.RDB$INDEX_NAME = ISG.RDB$INDEX_NAME
        LEFT JOIN RDB$REF_CONSTRAINTS REF ON RC.RDB$CONSTRAINT_NAME = REF.RDB$CONSTRAINT_NAME
        LEFT JOIN RDB$RELATION_CONSTRAINTS REFC ON REF.RDB$CONST_NAME_UQ = REFC.RDB$CONSTRAINT_NAME
        LEFT JOIN RDB$INDEX_SEGMENTS REFISG ON REFC.RDB$INDEX_NAME = REFISG.RDB$INDEX_NAME
        WHERE RC.RDB$RELATION_NAME = ?
          AND RC.RDB$CONSTRAINT_TYPE IN ('PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE')
        ORDER BY RC.RDB$CONSTRAINT_NAME
      `;
      
      const result = await this.executeQuery(connection, query, { params: [table] });
      
      if (result.success && result.rows) {
        return result.rows.map(row => ({
          name: row.name?.trim(),
          type: row.type?.trim(),
          tableName: table,
          columnName: row.column_name?.trim(),
          foreignTable: row.foreign_table?.trim(),
          foreignColumn: row.foreign_column?.trim()
        }));
      }
      
      return [];
    } catch (error) {
      console.error('Failed to get constraints:', error);
      return [];
    }
  }

  /**
   * Get schema list
   */
  async getSchemas(_connection: DatabaseConnection): Promise<string[]> {
    // Firebird doesn't have schemas in the traditional sense
    // Return empty array or single entry
    return [''];
  }

  /**
   * Internal method to get columns for a specific table
   */
  private async getTableColumnsInternal(
    connection: DatabaseConnection, 
    table: string
  ): Promise<ColumnMetadata[]> {
    try {
      const query = `
        SELECT 
          RF.RDB$FIELD_NAME as name,
          F.RDB$FIELD_TYPE as field_type,
          F.RDB$FIELD_LENGTH as length,
          F.RDB$FIELD_SCALE as scale,
          F.RDB$FIELD_PRECISION as precision,
          CASE WHEN RF.RDB$NULL_FLAG = 1 THEN 0 ELSE 1 END as nullable,
          RF.RDB$DEFAULT_VALUE as default_value,
          RF.RDB$FIELD_POSITION as ordinal_position
        FROM RDB$RELATION_FIELDS RF
        JOIN RDB$FIELDS F ON RF.RDB$FIELD_SOURCE = F.RDB$FIELD_NAME
        WHERE RF.RDB$RELATION_NAME = ?
        ORDER BY RF.RDB$FIELD_POSITION
      `;
      
      const result = await this.executeQuery(connection, query, { params: [table] });
      
      if (!result.success || !result.rows) {
        return [];
      }
      
      return result.rows.map(row => ({
        name: row.name?.trim(),
        type: this.mapFirebirdDataType(row.field_type, row.length, row.scale),
        dataType: this.getFirebirdDataTypeName(row.field_type),
        length: row.length,
        precision: row.precision,
        scale: row.scale,
        nullable: row.nullable === 1,
        default: row.default_value,
        ordinalPosition: row.ordinal_position
      } as ColumnMetadata));
    } catch (error) {
      console.error(`Failed to get columns for ${table}:`, error);
      return [];
    }
  }

  /**
   * Map Firebird data types to standard types
   */
  private mapFirebirdDataType(fieldType: number, _length: number, _scale: number): string {
    switch (fieldType) {
      case 7:  // SMALLINT
      case 8:  // INTEGER
      case 9:  // QUAD
      case 10: // FLOAT
      case 11: // DATE
      case 12: // TIME
      case 13: // CHAR
      case 14: // INT64
      case 16: // BOOLEAN
      case 27: // DOUBLE
      case 35: // TIMESTAMP
      case 37: // VARCHAR
      case 40: // CSTRING
      case 45: // BLOB_ID
      case 261: // BLOB
        if (fieldType === 13 || fieldType === 37 || fieldType === 40) return 'string';
        if (fieldType === 7 || fieldType === 8 || fieldType === 14 || fieldType === 27) return 'number';
        if (fieldType === 11 || fieldType === 12 || fieldType === 35) return 'date';
        if (fieldType === 16) return 'boolean';
        if (fieldType === 261 || fieldType === 45) return 'binary';
        return 'unknown';
      default:
        return 'unknown';
    }
  }

  /**
   * Get Firebird data type name
   */
  private getFirebirdDataTypeName(fieldType: number): string {
    const typeNames: { [key: number]: string } = {
      7: 'SMALLINT',
      8: 'INTEGER',
      9: 'QUAD',
      10: 'FLOAT',
      11: 'DATE',
      12: 'TIME',
      13: 'CHAR',
      14: 'INT64',
      16: 'BOOLEAN',
      27: 'DOUBLE',
      35: 'TIMESTAMP',
      37: 'VARCHAR',
      40: 'CSTRING',
      45: 'BLOB_ID',
      261: 'BLOB'
    };
    
    return typeNames[fieldType] || 'UNKNOWN';
  }

  /**
   * Additional Firebird-specific methods
   */
  
  /**
   * Get Firebird generators (sequences)
   */
  async getGenerators(connection: DatabaseConnection): Promise<any[]> {
    const query = `
      SELECT RDB$GENERATOR_NAME as name,
             RDB$GENERATOR_ID as id
      FROM RDB$GENERATORS
      WHERE RDB$SYSTEM_FLAG = 0
      ORDER BY RDB$GENERATOR_NAME
    `;
    
    const result = await this.executeQuery(connection, query);
    return result.success ? result.rows || [] : [];
  }

  /**
   * Get Firebird stored procedures
   */
  async getStoredProcedures(connection: DatabaseConnection): Promise<any[]> {
    const query = `
      SELECT RDB$PROCEDURE_NAME as name,
             RDB$PROCEDURE_INPUTS as input_params,
             RDB$PROCEDURE_OUTPUTS as output_params
      FROM RDB$PROCEDURES
      WHERE RDB$SYSTEM_FLAG = 0
      ORDER BY RDB$PROCEDURE_NAME
    `;
    
    const result = await this.executeQuery(connection, query);
    return result.success ? result.rows || [] : [];
  }
}

export default FirebirdAdapter;