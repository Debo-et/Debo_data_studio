// backend/src/database/adapters/mysql.adapter.ts

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

// Import from your existing MySQL inspector
import { 
  connectDatabaseAhx,
  disconnectDatabase,
  getTables,
  getTableAttrs,
  executeSqlQuery,
  executeSqlStatement,
  ArchiveHandle as MySQLArchiveHandle,
  ConnParams
} from '../inspection/mysql-inspector';

// Import the actual Trivalue enum from the correct location
// Assuming it's defined in mysql-inspector or inspection.types
// If not, define it locally:
enum Trivalue {
  TRI_NO = 0,
  TRI_YES = 1,
  TRI_DEFAULT = 2
}

/**
 * MySQL Database Adapter
 */
export class MySQLAdapter implements IBaseDatabaseInspector {
  private archiveHandle: MySQLArchiveHandle | null = null;

  /**
   * Connect to MySQL database
   */
  async connect(config: DatabaseConfig): Promise<DatabaseConnection> {
    try {
      // Create ArchiveHandle
      this.archiveHandle = {
        connection: null,
        remoteVersionStr: null,
        remoteVersion: 0,
        archiveRemoteVersion: null,
        minRemoteVersion: 50000, // MySQL 5.0
        maxRemoteVersion: 99999, // Allow future versions
        isStandby: false,
        dopt: null,
        savedPassword: config.password || null
      };

      // Create connection parameters - fix type issues
      const connParams: ConnParams = {
        dbname: config.dbname,
        pghost: config.host || 'localhost',
        pgport: config.port || '3306',
        // Convert undefined to null for username
        username: config.user || null,
        // Cast to any to bypass the type issue, or fix Trivalue enum
        promptPassword: (config.password ? Trivalue.TRI_NO : Trivalue.TRI_YES) as any,
        override_dbname: null
      };

      // Connect using existing MySQL inspector
      await connectDatabaseAhx(this.archiveHandle, connParams, false);

      // Create a DatabaseConnection wrapper around MySQLConnection
      if (!this.archiveHandle.connection) {
        throw new Error('Connection failed');
      }

      // Create a DatabaseConnection wrapper that includes the connected property
      const dbConnection: DatabaseConnection = {
        // Include all properties from MySQL connection
        ...this.archiveHandle.connection,
        // Add the required 'connected' property
        connected: true,
        // Add any other required properties from DatabaseConnection
        // If there are more properties needed, add them here
      };

      return dbConnection;
    } catch (error) {
      throw new Error(`Failed to connect to MySQL: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Disconnect from MySQL database
   */
  async disconnect(_connection: DatabaseConnection): Promise<void> {
    try {
      if (this.archiveHandle) {
        disconnectDatabase(this.archiveHandle);
        this.archiveHandle = null;
      }
    } catch (error) {
      console.error('Error disconnecting from MySQL:', error);
    }
  }

  /**
   * Test connection without full schema inspection
   */
  async testConnection(config: DatabaseConfig): Promise<{ success: boolean; version?: string; error?: string }> {
    let tempHandle: MySQLArchiveHandle | null = null;
    
    try {
      tempHandle = {
        connection: null,
        remoteVersionStr: null,
        remoteVersion: 0,
        archiveRemoteVersion: null,
        minRemoteVersion: 50000,
        maxRemoteVersion: 99999,
        isStandby: false,
        dopt: null,
        savedPassword: config.password || null
      };

      const connParams: ConnParams = {
        dbname: config.dbname,
        pghost: config.host || 'localhost',
        pgport: config.port || '3306',
        username: config.user || null,
        promptPassword: (config.password ? Trivalue.TRI_NO : Trivalue.TRI_YES) as any,
        override_dbname: null
      };

      await connectDatabaseAhx(tempHandle, connParams, false);
      
      return {
        success: true,
        version: tempHandle.remoteVersionStr || undefined
      };
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error)
      };
    } finally {
      if (tempHandle) {
        disconnectDatabase(tempHandle);
      }
    }
  }

  /**
   * Get tables and views from MySQL database
   */
  async getTables(_connection: DatabaseConnection, _options?: InspectionOptions): Promise<TableInfo[]> {
    if (!this.archiveHandle) {
      throw new Error('Not connected to MySQL');
    }

    try {
      // Get tables using MySQL inspector
      const { tables: mysqlTables } = await getTables(this.archiveHandle);
      
      // Get columns for all tables
      await getTableAttrs(this.archiveHandle, mysqlTables, mysqlTables.length);
      
      // Convert to standard TableInfo format
      return mysqlTables.map(table => ({
        schemaname: table.schemaname,
        tablename: table.tablename,
        tabletype: table.relkind === 'r' ? 'table' : 'view',
        columns: table.columns.map(col => ({
          name: col.name,
          type: col.type,
          dataType: col.type,
          length: col.length,
          precision: col.precision,
          scale: col.scale,
          nullable: col.nullable,
          default: col.default,
          comment: col.comment,
          isPrimaryKey: col.isPrimaryKey,
          isUnique: col.isUnique,
          isAutoIncrement: col.isAutoIncrement,
          ordinalPosition: table.columns.indexOf(col) + 1
        } as ColumnMetadata)),
        hasindex: table.hasindex,
        relpages: table.relpages,
        reltuples: table.reltuples,
        numatts: table.numatts,
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
    // Note: MySQL inspector already includes columns in getTables()
    // This method is provided for compatibility
    return tables;
  }

  /**
   * Get database version information
   */
  async getDatabaseInfo(_connection: DatabaseConnection): Promise<DatabaseVersionInfo> {
    if (!this.archiveHandle?.connection) {
      throw new Error('Not connected to MySQL');
    }

    try {
      // Execute version query
      const versionQuery = await executeSqlQuery(this.archiveHandle, 'SELECT VERSION() as version, DATABASE() as name');
      const row = versionQuery.fetchRow();
      
      if (row) {
        return {
          version: row[0] as string,
          name: row[1] as string
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
    if (!this.archiveHandle) {
      throw new Error('Not connected to MySQL');
    }

    const startTime = Date.now();
    
    try {
      // Apply row limit if specified for SELECT queries
      let finalSql = sql;
      if (options?.maxRows && sql.trim().toUpperCase().startsWith('SELECT')) {
        if (!finalSql.toUpperCase().includes('LIMIT')) {
          finalSql = `${finalSql} LIMIT ${options.maxRows}`;
        }
      }
      
      // Execute the query
      const result = await executeSqlQuery(this.archiveHandle, finalSql);
      const executionTime = Date.now() - startTime;
      
      const rows: any[] = [];
      let row: any[] | null;
      
      while ((row = result.fetchRow()) !== null) {
        rows.push(row);
      }
      
      return {
        success: true,
        rows,
        rowCount: result.numRows,
        executionTime,
        affectedRows: result.numRows
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
    _connection: DatabaseConnection, 
    queries: Array<{ sql: string; params?: any[] }>
  ): Promise<QueryResult[]> {
    if (!this.archiveHandle) {
      throw new Error('Not connected to MySQL');
    }

    const results: QueryResult[] = [];
    
    try {
      // Start transaction
      await executeSqlStatement(this.archiveHandle, 'START TRANSACTION');
      
      for (const { sql } of queries) {
        const startTime = Date.now();
        try {
          // Note: MySQL inspector doesn't support parameters in executeSqlStatement
          // For now, we'll execute the raw SQL
          await executeSqlStatement(this.archiveHandle, sql);
          const executionTime = Date.now() - startTime;
          
          results.push({
            success: true,
            executionTime,
            affectedRows: 1 // MySQL doesn't return row count for all statements
          });
        } catch (error) {
          // Rollback on any error
          await executeSqlStatement(this.archiveHandle, 'ROLLBACK');
          throw error;
        }
      }
      
      // Commit transaction
      await executeSqlStatement(this.archiveHandle, 'COMMIT');
      
      return results;
    } catch (error) {
      // Ensure rollback on any unhandled error
      try {
        await executeSqlStatement(this.archiveHandle, 'ROLLBACK');
      } catch (rollbackError) {
        console.error('Rollback failed:', rollbackError);
      }
      
      throw new Error(`Transaction failed: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Get table constraints
   */
  async getTableConstraints(connection: DatabaseConnection, _schema: string, table: string): Promise<any[]> {
    if (!this.archiveHandle) {
      throw new Error('Not connected to MySQL');
    }

    try {
      const query = `
        SELECT 
          CONSTRAINT_NAME as name,
          CONSTRAINT_TYPE as type,
          COLUMN_NAME as column_name,
          REFERENCED_TABLE_NAME as foreign_table,
          REFERENCED_COLUMN_NAME as foreign_column
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        AND CONSTRAINT_NAME != 'PRIMARY'
        ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION
      `;
      
      const result = await this.executeQuery(connection, query);
      
      if (result.success && result.rows) {
        return result.rows.map(row => ({
          name: row[0],
          type: row[1],
          tableName: table,
          columnName: row[2],
          foreignTable: row[3],
          foreignColumn: row[4]
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
    if (!this.archiveHandle) {
      throw new Error('Not connected to MySQL');
    }

    try {
      const query = 'SHOW DATABASES';
      const result = await this.executeQuery(connection, query);
      
      if (result.success && result.rows) {
        return result.rows.map(row => row[0]);
      }
      
      return [];
    } catch (error) {
      console.error('Failed to get schemas:', error);
      return [];
    }
  }

  /**
   * Additional MySQL-specific methods
   */
  
  /**
   * Get MySQL storage engines
   */
  async getStorageEngines(connection: DatabaseConnection): Promise<any[]> {
    const query = 'SHOW ENGINES';
    const result = await this.executeQuery(connection, query);
    return result.success ? result.rows || [] : [];
  }

  /**
   * Get MySQL user privileges
   */
  async getUserPrivileges(connection: DatabaseConnection): Promise<any[]> {
    const query = `
      SELECT 
        User, Host, 
        CONCAT_WS(',', 
          IF(Select_priv='Y','SELECT',''),
          IF(Insert_priv='Y','INSERT',''),
          IF(Update_priv='Y','UPDATE',''),
          IF(Delete_priv='Y','DELETE',''),
          IF(Create_priv='Y','CREATE',''),
          IF(Drop_priv='Y','DROP','')
        ) as privileges
      FROM mysql.user
      ORDER BY User, Host
    `;
    
    const result = await this.executeQuery(connection, query);
    return result.success ? result.rows || [] : [];
  }
}

export default MySQLAdapter;