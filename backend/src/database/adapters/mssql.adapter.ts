// backend/src/database/adapters/sqlserver.adapter.ts

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
  Trivalue
} from '../types/inspection.types';

// Import from existing SQL Server inspector
import SqlServerDatabaseModule, { 
  ArchiveHandle as SqlServerArchiveHandle} from '../inspection/sqlserver-inspector';

/**
 * SQL Server Database Adapter
 */
export class SQLServerAdapter implements IBaseDatabaseInspector {
  private module: SqlServerDatabaseModule | null = null;
  private archiveHandle: SqlServerArchiveHandle | null = null;

  /**
   * Connect to SQL Server database
   */
  async connect(config: DatabaseConfig): Promise<DatabaseConnection> {
    try {
      this.module = new SqlServerDatabaseModule();
      
      // Convert undefined values to null for the ConnectDatabase method
      const dbname = config.dbname ?? null;
      const host = config.host ?? 'localhost';
      const port = config.port ?? '1433';
      const user = config.user ?? null;
      const password = config.password ?? null;
      
      // Connect using the module
      const result = await this.module.ConnectDatabase(
        dbname,
        null,
        host,
        port,
        user,
        config.password ? Trivalue.TRI_NO : Trivalue.TRI_YES,
        true,
        'sqlserver-adapter',
        null,
        null,
        password,
        dbname
      );
      
      this.archiveHandle = {
        connection: result.connection,
        remoteVersionStr: undefined,
        remoteVersion: 0,
        archiveRemoteVersion: undefined,
        minRemoteVersion: 0,
        maxRemoteVersion: 99999,
        isStandby: false
      };
      
      return result.connection as DatabaseConnection;
    } catch (error) {
      throw new Error(`Failed to connect to SQL Server: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Disconnect from SQL Server database
   */
  async disconnect(connection: DatabaseConnection): Promise<void> {
    try {
      if (this.module && connection) {
        this.module.disconnectDatabase({ connection });
      }
      this.module = null;
      this.archiveHandle = null;
    } catch (error) {
      console.error('Error disconnecting from SQL Server:', error);
    }
  }

  /**
   * Test connection without full schema inspection
   */
  async testConnection(config: DatabaseConfig): Promise<{ success: boolean; version?: string; error?: string }> {
    let tempModule: SqlServerDatabaseModule | null = null;
    
    try {
      tempModule = new SqlServerDatabaseModule();
      
      // Convert undefined values to null for the ConnectDatabase method
      const dbname = config.dbname ?? null;
      const host = config.host ?? 'localhost';
      const port = config.port ?? '1433';
      const user = config.user ?? null;
      const password = config.password ?? null;
      
      const result = await tempModule.ConnectDatabase(
        dbname,
        null,
        host,
        port,
        user,
        config.password ? Trivalue.TRI_NO : Trivalue.TRI_YES,
        true,
        'sqlserver-test',
        null,
        null,
        password,
        dbname
      );
      
      // Get version
      const versionQuery = await this.executeQueryInternal(result.connection, "SELECT @@VERSION as version");
      
      return {
        success: true,
        version: versionQuery.rows?.[0]?.version
      };
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error)
      };
    } finally {
      if (tempModule) {
        tempModule.disconnectDatabase({ connection: null });
      }
    }
  }

  /**
   * Get tables and views from SQL Server database
   */
  async getTables(_connection: DatabaseConnection, _options?: InspectionOptions): Promise<TableInfo[]> {
    if (!this.module || !this.archiveHandle) {
      throw new Error('Not connected to SQL Server');
    }

    try {
      // Get tables using the module
      const { tables } = await this.module.getTables(this.archiveHandle);
      
      // Get columns for all tables
      await this.module.getTableAttrs(this.archiveHandle, tables);
      
      // Convert to standard TableInfo format
      return tables.map(table => ({
        schemaname: table.schemaname,
        tablename: table.tablename,
        tabletype: table.tabletype,
        columns: table.columns.map(col => ({
          name: col.name,
          type: col.type,
          dataType: col.dataType,
          length: col.length,
          precision: col.precision,
          scale: col.scale,
          nullable: col.nullable,
          default: col.default,
          isPrimaryKey: col.isPrimaryKey,
          isForeignKey: col.isForeignKey,
          foreignTable: col.foreignTable,
          foreignColumn: col.foreignColumn,
          ordinalPosition: col.ordinalPosition
        } as ColumnMetadata)),
        originalData: table
      }));
    } catch (error) {
      throw new Error(`Failed to get tables: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Get columns for specific tables
   */
  async getTableColumns(_connection: DatabaseConnection, tables: TableInfo[]): Promise<TableInfo[]> {
    // Note: SQL Server module already includes columns in getTables()
    return tables;
  }

  /**
   * Get database version information
   */
  async getDatabaseInfo(connection: DatabaseConnection): Promise<DatabaseVersionInfo> {
    try {
      const query = `
        SELECT 
          @@VERSION as version,
          DB_NAME() as name,
          SERVERPROPERTY('Collation') as collation,
          SERVERPROPERTY('Edition') as edition
      `;
      
      const result = await this.executeQueryInternal(connection, query);
      
      if (result.success && result.rows?.[0]) {
        return {
          version: result.rows[0].version,
          name: result.rows[0].name,
          collation: result.rows[0].collation,
          edition: result.rows[0].edition
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
    connection: DatabaseConnection, 
    sql: string, 
    options?: QueryExecutionOptions
  ): Promise<QueryResult> {
    return this.executeQueryInternal(connection, sql, options);
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
      await this.executeQueryInternal(connection, 'BEGIN TRANSACTION');
      
      for (const { sql, params } of queries) {
        const result = await this.executeQueryInternal(connection, sql, { params });
        results.push(result);
        
        if (!result.success) {
          // Rollback on error
          await this.executeQueryInternal(connection, 'ROLLBACK TRANSACTION');
          throw new Error(`Query failed: ${result.error}`);
        }
      }
      
      // Commit transaction
      await this.executeQueryInternal(connection, 'COMMIT TRANSACTION');
      
      return results;
    } catch (error) {
      // Ensure rollback on any unhandled error
      try {
        await this.executeQueryInternal(connection, 'ROLLBACK TRANSACTION');
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
          c.name as constraint_name,
          CASE c.type 
            WHEN 'PK' THEN 'PRIMARY KEY'
            WHEN 'FK' THEN 'FOREIGN KEY'
            WHEN 'UQ' THEN 'UNIQUE'
            ELSE c.type
          END as constraint_type,
          col.name as column_name,
          OBJECT_SCHEMA_NAME(fk.referenced_object_id) as foreign_schema,
          OBJECT_NAME(fk.referenced_object_id) as foreign_table,
          ref_col.name as foreign_column
        FROM sys.objects c
        INNER JOIN sys.schemas s ON c.schema_id = s.schema_id
        LEFT JOIN sys.foreign_keys fk ON c.object_id = fk.object_id
        LEFT JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
        LEFT JOIN sys.columns col ON fkc.parent_column_id = col.column_id AND fkc.parent_object_id = col.object_id
        LEFT JOIN sys.columns ref_col ON fkc.referenced_column_id = ref_col.column_id AND fkc.referenced_object_id = ref_col.object_id
        WHERE s.name = @schema AND OBJECT_NAME(c.parent_object_id) = @table
        AND c.type IN ('PK', 'FK', 'UQ')
        ORDER BY c.name
      `;
      
      const result = await this.executeQueryInternal(connection, query, { 
        params: [
          { name: 'schema', value: schema },
          { name: 'table', value: table }
        ]
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
   * Get schema list
   */
  async getSchemas(connection: DatabaseConnection): Promise<string[]> {
    try {
      const query = `
        SELECT s.name as schema_name
        FROM sys.schemas s
        WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA', 'db_owner', 'db_accessadmin', 
                            'db_securityadmin', 'db_ddladmin', 'db_backupoperator', 
                            'db_datareader', 'db_datawriter', 'db_denydatareader', 
                            'db_denydatawriter')
        ORDER BY s.name
      `;
      
      const result = await this.executeQueryInternal(connection, query);
      
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
   * Internal query execution method
   */
  private async executeQueryInternal(
    _connection: DatabaseConnection, 
    sql: string, 
    options?: QueryExecutionOptions & { params?: Array<{ name: string; value: any }> }
  ): Promise<QueryResult> {
    const startTime = Date.now();
    
    try {
      // In a real implementation, this would use mssql or tedious
      // For now, we'll simulate the result
      
      // Apply row limit for SELECT queries
      let finalSql = sql;
      if (options?.maxRows && sql.trim().toUpperCase().startsWith('SELECT')) {
        if (!finalSql.toUpperCase().includes('TOP')) {
          const selectIndex = finalSql.toUpperCase().indexOf('SELECT');
          finalSql = finalSql.substring(0, selectIndex + 6) + 
                    ` TOP ${options.maxRows}` + 
                    finalSql.substring(selectIndex + 6);
        }
      }
      
      // Mock execution - replace with actual SQL Server driver calls
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
   * Additional SQL Server-specific methods
   */
  
  /**
   * Get SQL Server linked servers
   */
  async getLinkedServers(connection: DatabaseConnection): Promise<any[]> {
    const query = `
      SELECT 
        name,
        product,
        provider,
        data_source,
        is_linked,
        is_remote_login_enabled
      FROM sys.servers
      WHERE is_linked = 1
      ORDER BY name
    `;
    
    const result = await this.executeQueryInternal(connection, query);
    return result.success ? result.rows || [] : [];
  }

  /**
   * Get SQL Server Agent jobs
   */
  async getAgentJobs(connection: DatabaseConnection): Promise<any[]> {
    const query = `
      SELECT 
        name,
        enabled,
        date_created,
        date_modified,
        description
      FROM msdb.dbo.sysjobs
      ORDER BY name
    `;
    
    const result = await this.executeQueryInternal(connection, query);
    return result.success ? result.rows || [] : [];
  }
}

export default SQLServerAdapter;