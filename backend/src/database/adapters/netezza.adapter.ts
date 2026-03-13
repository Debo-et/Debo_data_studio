// backend/src/database/adapters/netezza.adapter.ts

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
 * Netezza Database Adapter
 */
export class NetezzaAdapter implements IBaseDatabaseInspector {
  private connectionInstance: any = null;

  /**
   * Connect to Netezza database
   */
  async connect(config: DatabaseConfig): Promise<DatabaseConnection> {
    try {
      // Netezza uses ODBC or JDBC, we'll simulate connection
      const connectionParams = {
        host: config.host || 'localhost',
        port: config.port || '5480',
        database: config.dbname,
        user: config.user,
        password: config.password
      };
      
      // Mock connection
      this.connectionInstance = {
        connected: true,
        config: config,
        connectionParams: connectionParams
      };
      
      return this.connectionInstance;
    } catch (error) {
      throw new Error(`Failed to connect to Netezza: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Disconnect from Netezza database
   */
  async disconnect(_connection: DatabaseConnection): Promise<void> {
    try {
      this.connectionInstance = null;
    } catch (error) {
      console.error('Error disconnecting from Netezza:', error);
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
        version: 'Netezza 7.2'
      };
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error)
      };
    }
  }

  /**
   * Get tables and views from Netezza database
   */
  async getTables(connection: DatabaseConnection, options?: InspectionOptions): Promise<TableInfo[]> {
    try {
      const schema = options?.schema || 'ADMIN';
      const query = `
        SELECT 
          DATABASE as schemaname,
          TABLENAME as tablename,
          TABLETYPE as tabletype,
          OWNER,
          CREATEDATE as create_date
        FROM _V_TABLE
        WHERE DATABASE = ?
          AND TABLETYPE IN ('TABLE', 'VIEW')
          AND TABLENAME NOT LIKE '_V_%'
        ORDER BY DATABASE, TABLENAME
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
          columns: []
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
          VERSION as version,
          DATABASE as name
        FROM _V_SYSTEM
        LIMIT 1
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
      // Apply row limit for SELECT queries (Netezza uses LIMIT)
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
      await this.executeQuery(connection, 'BEGIN');
      
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
          CONSTRAINTNAME as name,
          CONSTRAINTTYPE as type,
          COLUMNNAME as column_name
        FROM _V_RELATION_CONSTRAINT
        WHERE DATABASE = ? AND TABLENAME = ?
        ORDER BY CONSTRAINTNAME
      `;
      
      const result = await this.executeQuery(connection, query, { 
        params: [schema, table] 
      });
      
      if (result.success && result.rows) {
        return result.rows.map(row => ({
          name: row.name,
          type: row.type,
          tableName: table,
          columnName: row.column_name
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
        SELECT DISTINCT DATABASE as schema_name
        FROM _V_TABLE
        WHERE DATABASE NOT IN ('SYSTEM', 'INFORMATION_SCHEMA')
        ORDER BY DATABASE
      `;
      
      const result = await this.executeQuery(connection, query);
      
      if (result.success && result.rows) {
        return result.rows.map(row => row.schema_name);
      }
      
      return ['ADMIN'];
    } catch (error) {
      console.error('Failed to get schemas:', error);
      return ['ADMIN'];
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
          COLUMNNAME as name,
          FORMATTYPE as data_type,
          LENGTH as length,
          SCALE as scale,
          ATTNULLS as nullable,
          DEFAULTVALUE as default_value,
          ATTNUM as ordinal_position
        FROM _V_RELATION_COLUMN
        WHERE DATABASE = ? AND TABLENAME = ?
        ORDER BY ORDINAL_POSITION
      `;
      
      const result = await this.executeQuery(connection, query, { params: [schema, table] });
      
      if (!result.success || !result.rows) {
        return [];
      }
      
      return result.rows.map(row => ({
        name: row.name,
        type: this.mapNetezzaDataType(row.data_type),
        dataType: row.data_type,
        length: row.length,
        scale: row.scale,
        nullable: row.nullable === 'T',
        default: row.default_value,
        ordinalPosition: row.ordinal_position
      } as ColumnMetadata));
    } catch (error) {
      console.error(`Failed to get columns for ${schema}.${table}:`, error);
      return [];
    }
  }

  /**
   * Map Netezza data types to standard types
   */
  private mapNetezzaDataType(netezzaType: string): string {
    switch (netezzaType.toUpperCase()) {
      case 'VARCHAR':
      case 'CHAR':
      case 'NCHAR':
      case 'NVARCHAR':
      case 'TEXT':
        return 'string';
      case 'INTEGER':
      case 'BIGINT':
      case 'SMALLINT':
      case 'BYTEINT':
      case 'NUMERIC':
      case 'DECIMAL':
      case 'FLOAT':
      case 'DOUBLE PRECISION':
      case 'REAL':
        return 'number';
      case 'DATE':
      case 'TIME':
      case 'TIMESTAMP':
      case 'TIMESTAMP_TZ':
        return 'date';
      case 'BOOLEAN':
        return 'boolean';
      case 'BINARY':
      case 'VARBINARY':
        return 'binary';
      default:
        return netezzaType.toLowerCase();
    }
  }

  /**
   * Additional Netezza-specific methods
   */
  
  /**
   * Get Netezza external tables
   */
  async getExternalTables(connection: DatabaseConnection): Promise<any[]> {
    const query = `
      SELECT 
        TABLENAME,
        OWNER,
        CREATEDATE
      FROM _V_TABLE
      WHERE TABLETYPE = 'EXTERNAL TABLE'
      ORDER BY TABLENAME
    `;
    
    const result = await this.executeQuery(connection, query);
    return result.success ? result.rows || [] : [];
  }

  /**
   * Get Netezza distributions
   */
  async getDistributions(connection: DatabaseConnection): Promise<any[]> {
    const query = `
      SELECT 
        DISTRIBUTION,
        DATASLICE_ID,
        SIZE_MB
      FROM _V_DATASLICE
      ORDER BY DATASLICE_ID
    `;
    
    const result = await this.executeQuery(connection, query);
    return result.success ? result.rows || [] : [];
  }
}

export default NetezzaAdapter;