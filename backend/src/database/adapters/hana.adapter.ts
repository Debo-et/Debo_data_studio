// backend/src/database/adapters/sap-hana.adapter.ts

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
 * SAP HANA Database Adapter
 */
export class SAPHANAAdapter implements IBaseDatabaseInspector {
  private connectionInstance: any = null;

  /**
   * Connect to SAP HANA database
   */
  async connect(config: DatabaseConfig): Promise<DatabaseConnection> {
    try {
      // Using hdb-client package (you would need to install it)
      // const hana = await import('@sap/hana-client');
      
      const connectionParams = {
        serverNode: `${config.host || 'localhost'}:${config.port || '39015'}`,
        uid: config.user,
        pwd: config.password,
        databaseName: config.dbname,
        schema: config.schema
      };
      
      // Mock connection
      this.connectionInstance = {
        connected: true,
        config: config,
        connectionParams: connectionParams
      };
      
      return this.connectionInstance;
    } catch (error) {
      throw new Error(`Failed to connect to SAP HANA: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Disconnect from SAP HANA database
   */
  async disconnect(_connection: DatabaseConnection): Promise<void> {
    try {
      this.connectionInstance = null;
    } catch (error) {
      console.error('Error disconnecting from SAP HANA:', error);
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
        version: 'SAP HANA 2.0'
      };
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error)
      };
    }
  }

  /**
   * Get tables and views from SAP HANA database
   */
  async getTables(connection: DatabaseConnection, options?: InspectionOptions): Promise<TableInfo[]> {
    try {
      const schema = options?.schema || connection.config?.schema || connection.config?.user;
      const query = `
        SELECT 
          SCHEMA_NAME as schemaname,
          TABLE_NAME as tablename,
          TABLE_TYPE as tabletype,
          COMMENTS as comment,
          RECORD_COUNT as row_count
        FROM TABLES
        WHERE SCHEMA_NAME = ?
          AND TABLE_TYPE IN ('TABLE', 'VIEW', 'COLLECTION')
          AND IS_SYSTEM_TABLE = 'FALSE'
        ORDER BY SCHEMA_NAME, TABLE_NAME
      `;
      
      const result = await this.executeQuery(connection, query, { params: [schema] });
      
      if (!result.success) {
        throw new Error(result.error || 'Failed to get tables');
      }
      
      const tables: TableInfo[] = [];
      
      for (const row of result.rows || []) {
        const tableInfo: TableInfo = {
          schemaname: row.schemaname,
          tablename: row.tablename,
          tabletype: row.tabletype.toLowerCase(),
          columns: [],
          rowCount: row.row_count
        };
        
        // Get columns for this table
        const columns = await this.getTableColumnsInternal(connection, row.schemaname, row.tablename);
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
        const columns = await this.getTableColumnsInternal(connection, table.schemaname, table.tablename);
        table.columns = columns;
      } catch (error) {
        console.error(`Failed to get columns for table ${table.schemaname}.${table.tablename}:`, error);
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
        SELECT 
          value as version,
          key as info_type
        FROM M_HOST_INFORMATION
        WHERE key = 'sql_version'
        UNION ALL
        SELECT 
          database_name as name,
          'database' as info_type
        FROM M_DATABASES
      `;
      
      const result = await this.executeQuery(connection, query);
      
      if (result.success && result.rows) {
        const version = result.rows.find((r: any) => r.info_type === 'sql_version')?.version;
        const name = result.rows.find((r: any) => r.info_type === 'database')?.name;
        
        return {
          version: version || 'Unknown',
          name: name || 'Unknown'
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
      // Apply row limit for SELECT queries (SAP HANA uses LIMIT)
      let finalSql = sql;
      if (options?.maxRows && sql.trim().toUpperCase().startsWith('SELECT')) {
        if (!finalSql.toUpperCase().includes('LIMIT')) {
          finalSql = `${finalSql} LIMIT ${options.maxRows}`;
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
      await this.executeQuery(connection, 'BEGIN TRANSACTION');
      
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
  async getTableConstraints(connection: DatabaseConnection, schema: string, table: string): Promise<any[]> {
    try {
      const query = `
        SELECT 
          CONSTRAINT_NAME as name,
          CONSTRAINT_TYPE as type,
          COLUMN_NAME as column_name,
          REFERENCED_SCHEMA as foreign_schema,
          REFERENCED_TABLE as foreign_table,
          REFERENCED_COLUMN as foreign_column
        FROM CONSTRAINT_COLUMNS
        WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?
        ORDER BY CONSTRAINT_NAME, POSITION
      `;
      
      const result = await this.executeQuery(connection, query, { 
        params: [schema, table] 
      });
      
      if (result.success && result.rows) {
        return result.rows.map(row => ({
          name: row.name,
          type: row.type,
          tableName: table,
          columnName: row.column_name,
          foreignTable: row.foreign_table,
          foreignColumn: row.foreign_column
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
  async getSchemas(connection: DatabaseConnection): Promise<string[]> {
    try {
      const query = `
        SELECT DISTINCT SCHEMA_NAME
        FROM SCHEMATA
        WHERE SCHEMA_NAME NOT LIKE 'SYS%'
          AND SCHEMA_NAME NOT IN ('_SYS_BI', '_SYS_REPO')
        ORDER BY SCHEMA_NAME
      `;
      
      const result = await this.executeQuery(connection, query);
      
      if (result.success && result.rows) {
        return result.rows.map(row => row.SCHEMA_NAME);
      }
      
      return [];
    } catch (error) {
      console.error('Failed to get schemas:', error);
      return [];
    }
  }

  /**
   * Internal method to get columns for a specific table
   */
  private async getTableColumnsInternal(
    connection: DatabaseConnection, 
    schema: string, 
    table: string
  ): Promise<ColumnMetadata[]> {
    try {
      const query = `
        SELECT 
          COLUMN_NAME as name,
          DATA_TYPE_NAME as data_type,
          LENGTH as length,
          SCALE as scale,
          IS_NULLABLE as nullable,
          DEFAULT_VALUE as default_value,
          POSITION as ordinal_position
        FROM TABLE_COLUMNS
        WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?
        ORDER BY POSITION
      `;
      
      const result = await this.executeQuery(connection, query, { params: [schema, table] });
      
      if (!result.success || !result.rows) {
        return [];
      }
      
      return result.rows.map(row => ({
        name: row.name,
        type: this.mapHanaDataType(row.data_type),
        dataType: row.data_type,
        length: row.length,
        scale: row.scale,
        nullable: row.nullable === 'TRUE',
        default: row.default_value,
        ordinalPosition: row.ordinal_position
      } as ColumnMetadata));
    } catch (error) {
      console.error(`Failed to get columns for ${schema}.${table}:`, error);
      return [];
    }
  }

  /**
   * Map SAP HANA data types to standard types
   */
  private mapHanaDataType(hanaType: string): string {
    switch (hanaType.toUpperCase()) {
      case 'VARCHAR':
      case 'NVARCHAR':
      case 'ALPHANUM':
      case 'SHORTTEXT':
      case 'TEXT':
        return 'string';
      case 'INTEGER':
      case 'BIGINT':
      case 'SMALLINT':
      case 'TINYINT':
      case 'DECIMAL':
      case 'SMALLDECIMAL':
      case 'REAL':
      case 'DOUBLE':
        return 'number';
      case 'DATE':
      case 'TIME':
      case 'SECONDDATE':
      case 'TIMESTAMP':
        return 'date';
      case 'BLOB':
      case 'CLOB':
      case 'NCLOB':
        return 'binary';
      case 'BOOLEAN':
        return 'boolean';
      default:
        return hanaType.toLowerCase();
    }
  }

  /**
   * Additional SAP HANA-specific methods
   */
  
  /**
   * Get SAP HANA system views
   */
  async getSystemViews(connection: DatabaseConnection): Promise<any[]> {
    const query = `
      SELECT 
        VIEW_NAME,
        VIEW_TYPE,
        COMMENTS
      FROM VIEWS
      WHERE SCHEMA_NAME = 'SYS'
        AND VIEW_NAME LIKE 'M_%'
      ORDER BY VIEW_NAME
    `;
    
    const result = await this.executeQuery(connection, query);
    return result.success ? result.rows || [] : [];
  }

  /**
   * Get SAP HANA calculation views
   */
  async getCalculationViews(connection: DatabaseConnection): Promise<any[]> {
    const query = `
      SELECT 
        OBJECT_NAME,
        OBJECT_TYPE,
        CREATED_BY,
        CREATE_TIME
      FROM OBJECTS
      WHERE OBJECT_TYPE = 'CALCULATIONVIEW'
        AND SCHEMA_NAME NOT LIKE 'SYS%'
      ORDER BY OBJECT_NAME
    `;
    
    const result = await this.executeQuery(connection, query);
    return result.success ? result.rows || [] : [];
  }
}

export default SAPHANAAdapter;