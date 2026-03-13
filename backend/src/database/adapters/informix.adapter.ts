// backend/src/database/adapters/informix.adapter.ts

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
 * Informix Database Adapter
 */
export class InformixAdapter implements IBaseDatabaseInspector {
  private connectionInstance: any = null;

  /**
   * Connect to Informix database
   */
  async connect(config: DatabaseConfig): Promise<DatabaseConnection> {
    try {
      // Using node-odbc for Informix
      // const odbc = await import('odbc');
      
      const connectionString = `
        DRIVER={IBM INFORMIX ODBC DRIVER};
        DATABASE=${config.dbname};
        HOST=${config.host || 'localhost'};
        SERVICE=${config.port || '9088'};
        SERVER=${config.server || 'ol_informix1210'};
        PROTOCOL=onsoctcp;
        UID=${config.user};
        PWD=${config.password};
        CLIENT_LOCALE=en_US.819;
        DB_LOCALE=en_US.819;
      `.replace(/\n/g, '').trim();
      
      // Mock connection
      this.connectionInstance = {
        connected: true,
        config: config,
        connectionString: connectionString
      };
      
      return this.connectionInstance;
    } catch (error) {
      throw new Error(`Failed to connect to Informix: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Disconnect from Informix database
   */
  async disconnect(_connection: DatabaseConnection): Promise<void> {
    try {
      this.connectionInstance = null;
    } catch (error) {
      console.error('Error disconnecting from Informix:', error);
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
        version: 'Informix 14.10'
      };
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error)
      };
    }
  }

  /**
   * Get tables and views from Informix database
   */
  async getTables(connection: DatabaseConnection, options?: InspectionOptions): Promise<TableInfo[]> {
    try {
      const schema = options?.schema || 'informix';
      const query = `
        SELECT 
          TRIM(t.tabname) as tablename,
          TRIM(t.owner) as schemaname,
          CASE t.tabtype
            WHEN 'T' THEN 'TABLE'
            WHEN 'V' THEN 'VIEW'
            WHEN 'S' THEN 'SYNONYM'
            ELSE t.tabtype
          END as tabletype,
          t.nrows as row_count
        FROM systables t
        WHERE t.tabid > 99
          AND t.tabtype IN ('T', 'V')
          AND TRIM(t.owner) = ?
        ORDER BY t.owner, t.tabname
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
        SELECT DBINFO('version', 'full') as version,
               DBINFO('dbname') as name
        FROM systables WHERE tabid = 1
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
      // Apply row limit for SELECT queries (Informix uses FIRST)
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
      await this.executeQuery(connection, 'BEGIN WORK');
      
      for (const { sql, params } of queries) {
        const result = await this.executeQuery(connection, sql, { params });
        results.push(result);
        
        if (!result.success) {
          // Rollback on error
          await this.executeQuery(connection, 'ROLLBACK WORK');
          throw new Error(`Query failed: ${result.error}`);
        }
      }
      
      // Commit transaction
      await this.executeQuery(connection, 'COMMIT WORK');
      
      return results;
    } catch (error) {
      // Ensure rollback on any unhandled error
      try {
        await this.executeQuery(connection, 'ROLLBACK WORK');
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
          c.constraint_name,
          c.constraint_type,
          cc.colname as column_name,
          r.tabname as foreign_table,
          rc.colname as foreign_column
        FROM sysconstraints c
        JOIN systables t ON c.tabid = t.tabid
        JOIN sysindexes i ON c.idxname = i.idxname
        JOIN syscolumns cc ON i.tabid = cc.tabid AND i.part1 = cc.colno
        LEFT JOIN sysconstraints rc ON c.constraint_name = rc.constraint_name AND rc.constraint_type = 'R'
        LEFT JOIN systables r ON rc.tabid = r.tabid
        LEFT JOIN syscolumns rcc ON rc.idxname = rcc.tabid AND rc.part1 = rcc.colno
        WHERE TRIM(t.owner) = ? AND TRIM(t.tabname) = ?
        ORDER BY c.constraint_name
      `;
      
      const result = await this.executeQuery(connection, query, { 
        params: [schema, table] 
      });
      
      if (result.success && result.rows) {
        return result.rows.map(row => ({
          name: row.constraint_name,
          type: row.constraint_type,
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
   * Get schema list (owners in Informix)
   */
  async getSchemas(connection: DatabaseConnection): Promise<string[]> {
    try {
      const query = `
        SELECT DISTINCT TRIM(owner) as schema_name
        FROM systables
        WHERE tabid > 99
          AND TRIM(owner) NOT IN ('informix')
        ORDER BY TRIM(owner)
      `;
      
      const result = await this.executeQuery(connection, query);
      
      if (result.success && result.rows) {
        return result.rows.map(row => row.schema_name);
      }
      
      return ['informix'];
    } catch (error) {
      console.error('Failed to get schemas:', error);
      return ['informix'];
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
          TRIM(c.colname) as name,
          TRIM(t.typname) as data_type,
          c.collength as length,
          c.colscale as scale,
          CASE WHEN c.coltype = 0 THEN 1 ELSE 0 END as nullable,
          NULL as default_value,
          c.colno as ordinal_position
        FROM syscolumns c
        JOIN systables t ON c.tabid = t.tabid
        JOIN systypes ty ON c.coltype = ty.coltype AND c.extended_id = ty.extended_id
        WHERE TRIM(t.owner) = ? AND TRIM(t.tabname) = ?
        ORDER BY c.colno
      `;
      
      const result = await this.executeQuery(connection, query, { params: [schema, table] });
      
      if (!result.success || !result.rows) {
        return [];
      }
      
      return result.rows.map(row => ({
        name: row.name,
        type: this.mapInformixDataType(row.data_type),
        dataType: row.data_type,
        length: row.length,
        scale: row.scale,
        nullable: row.nullable === 1,
        default: row.default_value,
        ordinalPosition: row.ordinal_position
      } as ColumnMetadata));
    } catch (error) {
      console.error(`Failed to get columns for ${schema}.${table}:`, error);
      return [];
    }
  }

  /**
   * Map Informix data types to standard types
   */
  private mapInformixDataType(informixType: string): string {
    switch (informixType.toUpperCase()) {
      case 'CHAR':
      case 'VARCHAR':
      case 'LVARCHAR':
      case 'NCHAR':
      case 'NVARCHAR':
      case 'TEXT':
        return 'string';
      case 'INTEGER':
      case 'INT':
      case 'SMALLINT':
      case 'BIGINT':
      case 'DECIMAL':
      case 'MONEY':
      case 'FLOAT':
      case 'SMALLFLOAT':
      case 'DOUBLE PRECISION':
      case 'REAL':
        return 'number';
      case 'DATE':
      case 'DATETIME':
      case 'INTERVAL':
        return 'date';
      case 'BOOLEAN':
        return 'boolean';
      case 'BLOB':
      case 'CLOB':
      case 'BYTE':
        return 'binary';
      default:
        return informixType.toLowerCase();
    }
  }

  /**
   * Additional Informix-specific methods
   */
  
  /**
   * Get Informix dbspaces
   */
  async getDbspaces(connection: DatabaseConnection): Promise<any[]> {
    const query = `
      SELECT 
        name,
        owner,
        is_mirrored,
        is_temp,
        size
      FROM sysdbspaces
      ORDER BY name
    `;
    
    const result = await this.executeQuery(connection, query);
    return result.success ? result.rows || [] : [];
  }

  /**
   * Get Informix chunks
   */
  async getChunks(connection: DatabaseConnection): Promise<any[]> {
    const query = `
      SELECT 
        chknum,
        dbsname,
        offset,
        size
      FROM syschunks
      ORDER BY chknum
    `;
    
    const result = await this.executeQuery(connection, query);
    return result.success ? result.rows || [] : [];
  }
}

export default InformixAdapter;