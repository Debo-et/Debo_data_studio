// backend/src/database/adapters/sybase.adapter.ts

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
 * Sybase Database Adapter
 */
export class SybaseAdapter implements IBaseDatabaseInspector {
  private connectionInstance: any = null;

  /**
   * Connect to Sybase database
   */
  async connect(config: DatabaseConfig): Promise<DatabaseConnection> {
    try {
      // Using tedious for Sybase (Sybase ASE supports TDS protocol)
      // const tedious = await import('tedious');
      
      const connectionParams = {
        server: config.host || 'localhost',
        port: parseInt(config.port || '5000'),
        authentication: {
          type: 'default',
          options: {
            userName: config.user,
            password: config.password
          }
        },
        options: {
          database: config.dbname,
          encrypt: false,
          trustServerCertificate: true
        }
      };
      
      // Mock connection
      this.connectionInstance = {
        connected: true,
        config: config,
        connectionParams: connectionParams
      };
      
      return this.connectionInstance;
    } catch (error) {
      throw new Error(`Failed to connect to Sybase: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Disconnect from Sybase database
   */
  async disconnect(_connection: DatabaseConnection): Promise<void> {
    try {
      this.connectionInstance = null;
    } catch (error) {
      console.error('Error disconnecting from Sybase:', error);
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
        version: 'Sybase ASE 16.0'
      };
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error)
      };
    }
  }

  /**
   * Get tables and views from Sybase database
   */
  async getTables(connection: DatabaseConnection, options?: InspectionOptions): Promise<TableInfo[]> {
    try {
      const schema = options?.schema || 'dbo';
      const query = `
        SELECT 
          u.name as schemaname,
          o.name as tablename,
          CASE 
            WHEN o.type = 'U' THEN 'TABLE'
            WHEN o.type = 'V' THEN 'VIEW'
            ELSE o.type
          END as tabletype,
          convert(varchar(255), null) as comment
        FROM sysobjects o
        JOIN sysusers u ON o.uid = u.uid
        WHERE o.type IN ('U', 'V')
          AND u.name = @schema
          AND o.name NOT LIKE 'sys%'
        ORDER BY u.name, o.name
      `;
      
      const result = await this.executeQuery(connection, query, { 
        params: [{ name: 'schema', value: schema }]
      });
      
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
        SELECT @@version as version, db_name() as name
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
      // Apply row limit for SELECT queries (Sybase uses TOP)
      let finalSql = sql;
      if (options?.maxRows && sql.trim().toUpperCase().startsWith('SELECT')) {
        if (!finalSql.toUpperCase().includes('TOP')) {
          const selectIndex = finalSql.toUpperCase().indexOf('SELECT');
          finalSql = finalSql.substring(0, selectIndex + 6) + 
                    ` TOP ${options.maxRows}` + 
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
      await this.executeQuery(connection, 'BEGIN TRANSACTION');
      
      for (const { sql, params } of queries) {
        const result = await this.executeQuery(connection, sql, { params });
        results.push(result);
        
        if (!result.success) {
          // Rollback on error
          await this.executeQuery(connection, 'ROLLBACK TRANSACTION');
          throw new Error(`Query failed: ${result.error}`);
        }
      }
      
      // Commit transaction
      await this.executeQuery(connection, 'COMMIT TRANSACTION');
      
      return results;
    } catch (error) {
      // Ensure rollback on any unhandled error
      try {
        await this.executeQuery(connection, 'ROLLBACK TRANSACTION');
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
          o.name as name,
          'PRIMARY KEY' as type,
          c.name as column_name,
          null as foreign_table,
          null as foreign_column
        FROM sysobjects o
        JOIN sysindexes i ON o.id = i.id
        JOIN syscolumns c ON i.id = c.id
        WHERE o.type = 'PK'
          AND OBJECT_NAME(o.parent_obj) = @table
          AND USER_NAME(o.uid) = @schema
        UNION ALL
        SELECT 
          o.name as name,
          'FOREIGN KEY' as type,
          c.name as column_name,
          OBJECT_NAME(f.rkeyid) as foreign_table,
          COL_NAME(f.rkeyid, f.rkey) as foreign_column
        FROM sysobjects o
        JOIN sysforeignkeys f ON o.id = f.constid
        JOIN syscolumns c ON f.fkeyid = c.id AND f.fkey = c.colid
        WHERE o.type = 'F'
          AND OBJECT_NAME(o.parent_obj) = @table
          AND USER_NAME(o.uid) = @schema
      `;
      
      const result = await this.executeQuery(connection, query, { 
        params: [
          { name: 'schema', value: schema },
          { name: 'table', value: table }
        ]
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
   * Get schema list (users in Sybase)
   */
  async getSchemas(connection: DatabaseConnection): Promise<string[]> {
    try {
      const query = `
        SELECT name as schema_name
        FROM sysusers
        WHERE hasdbaccess = 1
          AND name NOT IN ('dbo', 'guest', 'sys')
        ORDER BY name
      `;
      
      const result = await this.executeQuery(connection, query);
      
      if (result.success && result.rows) {
        return result.rows.map(row => row.schema_name);
      }
      
      return ['dbo'];
    } catch (error) {
      console.error('Failed to get schemas:', error);
      return ['dbo'];
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
          c.name as name,
          t.name as data_type,
          c.length as length,
          c.scale as scale,
          c.prec as precision,
          CASE WHEN c.status & 8 = 8 THEN 1 ELSE 0 END as nullable,
          text as default_value,
          c.colid as ordinal_position,
          CASE WHEN ic.index_id IS NOT NULL THEN 1 ELSE 0 END as is_primary_key
        FROM syscolumns c
        JOIN systypes t ON c.usertype = t.usertype
        LEFT JOIN sysindexes i ON c.id = i.id AND i.indid = 1
        LEFT JOIN sysindexkeys ik ON c.id = ik.id AND c.colid = ik.colid AND i.indid = ik.indid
        LEFT JOIN syscomments cm ON c.cdefault = cm.id
        LEFT JOIN (
          SELECT id, index_id 
          FROM sysindexes 
          WHERE indid = 1
        ) ic ON c.id = ic.id
        WHERE OBJECT_NAME(c.id) = @table
          AND USER_NAME(OBJECTPROPERTY(c.id, 'OwnerId')) = @schema
        ORDER BY c.colid
      `;
      
      const result = await this.executeQuery(connection, query, { 
        params: [
          { name: 'schema', value: schema },
          { name: 'table', value: table }
        ]
      });
      
      if (!result.success || !result.rows) {
        return [];
      }
      
      return result.rows.map(row => ({
        name: row.name,
        type: this.mapSybaseDataType(row.data_type),
        dataType: row.data_type,
        length: row.length,
        precision: row.precision,
        scale: row.scale,
        nullable: row.nullable === 1,
        default: row.default_value,
        isPrimaryKey: row.is_primary_key === 1,
        ordinalPosition: row.ordinal_position
      } as ColumnMetadata));
    } catch (error) {
      console.error(`Failed to get columns for ${schema}.${table}:`, error);
      return [];
    }
  }

  /**
   * Map Sybase data types to standard types
   */
  private mapSybaseDataType(sybaseType: string): string {
    switch (sybaseType.toLowerCase()) {
      case 'varchar':
      case 'char':
      case 'nchar':
      case 'nvarchar':
      case 'text':
      case 'unitext':
        return 'string';
      case 'int':
      case 'bigint':
      case 'smallint':
      case 'tinyint':
      case 'decimal':
      case 'numeric':
      case 'float':
      case 'real':
      case 'money':
      case 'smallmoney':
        return 'number';
      case 'datetime':
      case 'smalldatetime':
      case 'date':
      case 'time':
        return 'date';
      case 'binary':
      case 'varbinary':
      case 'image':
        return 'binary';
      case 'bit':
        return 'boolean';
      default:
        return sybaseType.toLowerCase();
    }
  }

  /**
   * Additional Sybase-specific methods
   */
  
  /**
   * Get Sybase database devices
   */
  async getDatabaseDevices(connection: DatabaseConnection): Promise<any[]> {
    const query = `
      SELECT 
        name,
        phyname,
        size,
        vstart
      FROM sysdevices
      WHERE status = 2 -- Disk device
      ORDER BY name
    `;
    
    const result = await this.executeQuery(connection, query);
    return result.success ? result.rows || [] : [];
  }

  /**
   * Get Sybase stored procedures
   */
  async getStoredProcedures(connection: DatabaseConnection): Promise<any[]> {
    const query = `
      SELECT 
        o.name,
        u.name as owner,
        o.crdate as created_date
      FROM sysobjects o
      JOIN sysusers u ON o.uid = u.uid
      WHERE o.type = 'P'
        AND u.name NOT IN ('dbo', 'sys')
      ORDER BY u.name, o.name
    `;
    
    const result = await this.executeQuery(connection, query);
    return result.success ? result.rows || [] : [];
  }
}

export default SybaseAdapter;