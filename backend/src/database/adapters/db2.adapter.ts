// backend/src/database/adapters/db2.adapter.ts

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
 * DB2 Database Adapter
 */
export class DB2Adapter implements IBaseDatabaseInspector {
  private connectionInstance: any = null;

  /**
   * Connect to DB2 database
   */
  async connect(config: DatabaseConfig): Promise<DatabaseConnection> {
    try {
      // Using ibm_db2 package (you would need to install it)
      // const ibmdb = await import('ibm_db');
      
      const connectionString = `
        DATABASE=${config.dbname};
        HOSTNAME=${config.host || 'localhost'};
        PORT=${config.port || '50000'};
        PROTOCOL=TCPIP;
        UID=${config.user};
        PWD=${config.password};
      `.replace(/\n/g, '').trim();
      
      // Mock connection - replace with actual ibm_db2
      this.connectionInstance = {
        connected: true,
        config: config,
        connectionString: connectionString
      };
      
      return this.connectionInstance;
    } catch (error) {
      throw new Error(`Failed to connect to DB2: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Disconnect from DB2 database
   */
  async disconnect(_connection: DatabaseConnection): Promise<void> {
    try {
      this.connectionInstance = null;
    } catch (error) {
      console.error('Error disconnecting from DB2:', error);
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
        version: 'DB2 v11.5'
      };
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error)
      };
    }
  }

  /**
   * Get tables and views from DB2 database
   */
  async getTables(connection: DatabaseConnection, options?: InspectionOptions): Promise<TableInfo[]> {
    try {
      const schema = options?.schema || connection.config?.schema || connection.config?.user?.toUpperCase();
      const query = `
        SELECT 
          TABSCHEMA as schemaname,
          TABNAME as tablename,
          TYPE as tabletype,
          REMARKS as comment
        FROM SYSCAT.TABLES
        WHERE TABSCHEMA = ?
          AND TYPE IN ('T', 'V')
          AND TABSCHEMA NOT LIKE 'SYS%'
        ORDER BY TABSCHEMA, TABNAME
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
          tabletype: row.tabletype === 'T' ? 'table' : 'view',
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
          SERVICE_LEVEL as version,
          INST_NAME as name,
          DB_COLLATE_SEQ as collation
        FROM TABLE(SYSPROC.ENV_GET_INST_INFO()) AS INSTANCEINFO
      `;
      
      const result = await this.executeQuery(connection, query);
      
      if (result.success && result.rows?.[0]) {
        return {
          version: result.rows[0].version,
          name: result.rows[0].name,
          collation: result.rows[0].collation
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
      // Apply row limit for SELECT queries (DB2 uses FETCH FIRST)
      let finalSql = sql;
      if (options?.maxRows && sql.trim().toUpperCase().startsWith('SELECT')) {
        if (!finalSql.toUpperCase().includes('FETCH FIRST')) {
          finalSql = `${finalSql} FETCH FIRST ${options.maxRows} ROWS ONLY`;
        }
      }
      
      // Mock execution - replace with actual DB2 driver calls
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
          CONSTNAME as name,
          TYPE as type,
          COLNAME as column_name,
          REFTABNAME as foreign_table,
          REFKEYNAME as foreign_key
        FROM SYSCAT.REFERENCES
        WHERE TABSCHEMA = ? AND TABNAME = ?
        UNION ALL
        SELECT 
          CONSTNAME as name,
          'PRIMARY KEY' as type,
          COLNAME as column_name,
          NULL as foreign_table,
          NULL as foreign_key
        FROM SYSCAT.KEYCOLUSE
        WHERE TABSCHEMA = ? AND TABNAME = ? AND CONSTNAME LIKE '%PK%'
        ORDER BY name
      `;
      
      const result = await this.executeQuery(connection, query, { 
        params: [schema, table, schema, table] 
      });
      
      if (result.success && result.rows) {
        return result.rows.map(row => ({
          name: row.name,
          type: row.type,
          tableName: table,
          columnName: row.column_name,
          foreignTable: row.foreign_table,
          foreignColumn: row.foreign_key
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
        SELECT DISTINCT SCHEMANAME as schema_name
        FROM SYSCAT.SCHEMATA
        WHERE SCHEMANAME NOT LIKE 'SYS%'
        ORDER BY SCHEMANAME
      `;
      
      const result = await this.executeQuery(connection, query);
      
      if (result.success && result.rows) {
        return result.rows.map(row => row.schema_name);
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
          COLNAME as name,
          TYPENAME as data_type,
          LENGTH as length,
          SCALE as scale,
          NULLS as nullable,
          DEFAULT as default_value,
          COLNO as ordinal_position,
          KEYSEQ as is_primary_key
        FROM SYSCAT.COLUMNS
        WHERE TABSCHEMA = ? AND TABNAME = ?
        ORDER BY COLNO
      `;
      
      const result = await this.executeQuery(connection, query, { params: [schema, table] });
      
      if (!result.success || !result.rows) {
        return [];
      }
      
      return result.rows.map(row => ({
        name: row.name,
        type: this.mapDB2DataType(row.data_type),
        dataType: row.data_type,
        length: row.length,
        scale: row.scale,
        nullable: row.nullable === 'Y',
        default: row.default_value,
        isPrimaryKey: row.is_primary_key > 0,
        ordinalPosition: row.ordinal_position
      } as ColumnMetadata));
    } catch (error) {
      console.error(`Failed to get columns for ${schema}.${table}:`, error);
      return [];
    }
  }

  /**
   * Map DB2 data types to standard types
   */
  private mapDB2DataType(db2Type: string): string {
    switch (db2Type.toUpperCase()) {
      case 'VARCHAR':
      case 'CHAR':
      case 'CLOB':
      case 'GRAPHIC':
      case 'VARGRAPHIC':
      case 'DBCLOB':
        return 'string';
      case 'INTEGER':
      case 'BIGINT':
      case 'SMALLINT':
      case 'DECIMAL':
      case 'NUMERIC':
      case 'FLOAT':
      case 'DOUBLE':
      case 'REAL':
        return 'number';
      case 'DATE':
      case 'TIME':
      case 'TIMESTAMP':
        return 'date';
      case 'BLOB':
      case 'BINARY':
      case 'VARBINARY':
        return 'binary';
      default:
        return db2Type.toLowerCase();
    }
  }

  /**
   * Additional DB2-specific methods
   */
  
  /**
   * Get DB2 bufferpools
   */
  async getBufferpools(connection: DatabaseConnection): Promise<any[]> {
    const query = `
      SELECT 
        BPNAME as name,
        NPAGES as pages,
        PAGESIZE as page_size,
        ESTORE as extended_storage
      FROM SYSCAT.BUFFERPOOLS
      ORDER BY BPNAME
    `;
    
    const result = await this.executeQuery(connection, query);
    return result.success ? result.rows || [] : [];
  }

  /**
   * Get DB2 tablespaces
   */
  async getTablespaces(connection: DatabaseConnection): Promise<any[]> {
    const query = `
      SELECT 
        TBSPACE as name,
        TBSPACETYPE as type,
        DATATYPE as data_type,
        EXTENTSIZE as extent_size
      FROM SYSCAT.TABLESPACES
      ORDER BY TBSPACE
    `;
    
    const result = await this.executeQuery(connection, query);
    return result.success ? result.rows || [] : [];
  }
}

export default DB2Adapter;