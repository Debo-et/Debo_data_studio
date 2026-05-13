// backend/src/database/adapters/oracle.adapter.ts

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

// Import from existing Oracle inspector
import OracleDatabaseModule, { 
  ArchiveHandle as OracleArchiveHandle} from '../inspection/oracle-inspector';

/**
 * Oracle Database Adapter
 */
export class OracleAdapter implements IBaseDatabaseInspector {
  private module: OracleDatabaseModule | null = null;
  private archiveHandle: OracleArchiveHandle | null = null;

  /**
   * Connect to Oracle database
   */
  async connect(config: DatabaseConfig): Promise<DatabaseConnection> {
    try {
      this.module = new OracleDatabaseModule();
      
      // Convert undefined to null for parameters
      const dbName = config.dbname ?? config.schema ?? 'ORCL';
      const host = config.host ?? 'localhost';
      const port = config.port ?? '1521';
      const user = config.user ?? '';
      const password = config.password ?? null;
      const serviceName = config.dbname ?? null;
      
      // Connect using the module
      const result = await this.module.ConnectDatabase(
        dbName,
        null,
        host,
        port.toString(),
        user,
        password ? Trivalue.TRI_NO : Trivalue.TRI_YES,
        true,
        'oracle-adapter',
        null,
        null,
        password,
        serviceName
      );
      
      this.archiveHandle = {
        svcCtx: result.connection?.svchp,
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
      throw new Error(`Failed to connect to Oracle: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Disconnect from Oracle database
   */
  async disconnect(connection: DatabaseConnection): Promise<void> {
    try {
      if (this.module && connection) {
        this.module.disconnectDatabase({ connection });
      }
      this.module = null;
      this.archiveHandle = null;
    } catch (error) {
      console.error('Error disconnecting from Oracle:', error);
    }
  }

  /**
   * Test connection without full schema inspection
   */
  async testConnection(config: DatabaseConfig): Promise<{ success: boolean; version?: string; error?: string }> {
    let tempModule: OracleDatabaseModule | null = null;
    
    try {
      tempModule = new OracleDatabaseModule();
      
      // Convert undefined to null for parameters
      const dbName = config.dbname ?? config.schema ?? 'ORCL';
      const host = config.host ?? 'localhost';
      const port = config.port ?? '1521';
      const user = config.user ?? '';
      const password = config.password ?? null;
      const serviceName = config.dbname ?? null;
      
      const result = await tempModule.ConnectDatabase(
        dbName,
        null,
        host,
        port.toString(),
        user,
        password ? Trivalue.TRI_NO : Trivalue.TRI_YES,
        true,
        'oracle-test',
        null,
        null,
        password,
        serviceName
      );
      
      // Get version
      const versionQuery = await this.executeQueryInternal(result.connection, 'SELECT version FROM v$instance');
      
      return {
        success: true,
        version: versionQuery.rows?.[0]?.version as string | undefined
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
   * Get tables and views from Oracle database
   */
  async getTables(_connection: DatabaseConnection, _options?: InspectionOptions): Promise<TableInfo[]> {
    if (!this.module || !this.archiveHandle) {
      throw new Error('Not connected to Oracle');
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
    // Note: Oracle module already includes columns in getTables()
    return tables;
  }

  /**
   * Get database version information
   */
  async getDatabaseInfo(connection: DatabaseConnection): Promise<DatabaseVersionInfo> {
    try {
      const query = `
        SELECT 
          version,
          instance_name as name,
          host_name as host,
          status,
          logins
        FROM v$instance
      `;
      
      const result = await this.executeQueryInternal(connection, query);
      
      if (result.success && result.rows?.[0]) {
        return {
          version: result.rows[0].version as string,
          name: result.rows[0].name as string,
          // Oracle doesn't have encoding/collation in v$instance
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
      await this.executeQueryInternal(connection, 'BEGIN');
      
      for (const { sql, params } of queries) {
        const result = await this.executeQueryInternal(connection, sql, { params });
        results.push(result);
        
        if (!result.success) {
          // Rollback on error
          await this.executeQueryInternal(connection, 'ROLLBACK');
          throw new Error(`Query failed: ${result.error}`);
        }
      }
      
      // Commit transaction
      await this.executeQueryInternal(connection, 'COMMIT');
      
      return results;
    } catch (error) {
      // Ensure rollback on any unhandled error
      try {
        await this.executeQueryInternal(connection, 'ROLLBACK');
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
          cc.column_name,
          rcc.table_name as foreign_table,
          rcc.column_name as foreign_column
        FROM all_constraints c
        JOIN all_cons_columns cc ON c.constraint_name = cc.constraint_name
        LEFT JOIN all_constraints rc ON c.r_constraint_name = rc.constraint_name
        LEFT JOIN all_cons_columns rcc ON rc.constraint_name = rcc.constraint_name
        WHERE c.owner = :schema AND c.table_name = :table
        AND c.constraint_type IN ('P', 'R', 'U')
        ORDER BY c.constraint_name, cc.position
      `;
      
      const result = await this.executeQueryInternal(connection, query, {
        params: [
          { name: 'schema', value: schema },
          { name: 'table', value: table }
        ]
      });
      
      if (result.success && result.rows) {
        return result.rows.map(row => ({
          name: row.constraint_name as string,
          type: row.constraint_type === 'P' ? 'PRIMARY KEY' : 
                row.constraint_type === 'R' ? 'FOREIGN KEY' : 
                'UNIQUE',
          tableName: table,
          columnName: row.column_name as string,
          foreignTable: row.foreign_table as string,
          foreignColumn: row.foreign_column as string
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
        SELECT username as schema_name
        FROM all_users
        WHERE username NOT IN ('SYS', 'SYSTEM', 'XDB', 'CTXSYS', 'MDSYS', 'OUTLN', 'DBSNMP')
        ORDER BY username
      `;
      
      const result = await this.executeQueryInternal(connection, query);
      
      if (result.success && result.rows) {
        return result.rows.map(row => row.schema_name as string);
      }
      
      return [];
    } catch (error) {
      console.error('Failed to get schemas:', error);
      return [];
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
      // Apply row limit for SELECT queries
      let finalSql = sql;
      if (options?.maxRows && sql.trim().toUpperCase().startsWith('SELECT')) {
        if (!finalSql.toUpperCase().includes('ROWNUM') && !finalSql.toUpperCase().includes('FETCH FIRST')) {
          finalSql = `${finalSql} FETCH FIRST ${options.maxRows} ROWS ONLY`;
        }
      }
      
      // Mock execution - replace with actual Oracle driver calls
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
   * Additional Oracle-specific methods
   */
  
  /**
   * Get Oracle tablespaces
   */
  async getTablespaces(connection: DatabaseConnection): Promise<any[]> {
    const query = `
      SELECT 
        tablespace_name,
        status,
        contents,
        extent_management,
        allocation_type
      FROM dba_tablespaces
      ORDER BY tablespace_name
    `;
    
    const result = await this.executeQueryInternal(connection, query);
    return result.success ? result.rows || [] : [];
  }

  /**
   * Get Oracle sessions
   */
  async getSessions(connection: DatabaseConnection): Promise<any[]> {
    const query = `
      SELECT 
        sid,
        serial# as serial,
        username,
        status,
        program,
        machine
      FROM v$session
      WHERE username IS NOT NULL
      ORDER BY username, sid
    `;
    
    const result = await this.executeQueryInternal(connection, query);
    return result.success ? result.rows || [] : [];
  }
}

export default OracleAdapter;