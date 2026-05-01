// backend/src/database/adapters/access.adapter.ts

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

// Use the node-adodb library, which provides a Promise-based API for MS Access
const ADODB = require('node-adodb');

/**
 * Microsoft Access Database Adapter
 * Implements the base inspector interface using the node-adodb library.
 * This adapter is designed to work with .mdb and .accdb files on Windows.
 * It requires the Microsoft Access Database Engine drivers to be installed.
 */
export class AccessAdapter implements IBaseDatabaseInspector {
  private connection: any = null;        // Stores the active ADODB connection instance
  private isConnected: boolean = false;  // Tracks connection state

  /**
   * Connect to a Microsoft Access database file.
   * @param config Database configuration containing the path to the .mdb/.accdb file and optional password.
   */
  async connect(config: DatabaseConfig): Promise<DatabaseConnection> {
    try {
      // Validate that the database file path (dbname) is provided.
      const dbPath = config.dbname;
      if (!dbPath) {
        throw new Error('Database file path (dbname) is required for Access connections.');
      }

      // Determine the appropriate provider based on the file extension
      const isAccdb = dbPath.toLowerCase().endsWith('.accdb');
      const provider = isAccdb ? 'Microsoft.ACE.OLEDB.12.0' : 'Microsoft.Jet.OLEDB.4.0';

      // Build the connection string
      let connectionString = `Provider=${provider};Data Source=${dbPath};Persist Security Info=False;`;

      // Include password if provided
      if (config.password) {
        connectionString += `Jet OLEDB:Database Password=${config.password};`;
      }

      // Open the connection using node-adodb
      this.connection = ADODB.open(connectionString);
      this.isConnected = true;

      // Test the connection by fetching a simple query (e.g., a list of tables)
      await this.connection.query('SELECT 1 as test');

      return this.connection as unknown as DatabaseConnection;
    } catch (error) {
      throw new Error(`Failed to connect to Access database: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Disconnect from the Access database.
   * For node-adodb, disconnection is implicit, but we reset the internal state.
   */
  async disconnect(_connection: DatabaseConnection): Promise<void> {
    try {
      // node-adodb does not have an explicit close method.
      // Resetting the connection instance is sufficient.
      this.connection = null;
      this.isConnected = false;
    } catch (error) {
      console.error('Error disconnecting from Access database:', error);
    }
  }

  /**
   * Test the connection to the Access database without performing a full schema inspection.
   * @returns An object indicating success, the Access version, or an error message.
   */
  async testConnection(config: DatabaseConfig): Promise<{ success: boolean; version?: string; error?: string }> {
    let testConn = null;
    try {
      const dbPath = config.dbname;
      if (!dbPath) {
        throw new Error('Database file path (dbname) is required.');
      }

      const isAccdb = dbPath.toLowerCase().endsWith('.accdb');
      const provider = isAccdb ? 'Microsoft.ACE.OLEDB.12.0' : 'Microsoft.Jet.OLEDB.4.0';
      let connectionString = `Provider=${provider};Data Source=${dbPath};Persist Security Info=False;`;

      if (config.password) {
        connectionString += `Jet OLEDB:Database Password=${config.password};`;
      }

      testConn = ADODB.open(connectionString);
      await testConn.query('SELECT 1 as test');

      // Retrieve Access version information
      const versionQuery = 'SELECT @@VERSION as version';
      const versionResult = await testConn.query(versionQuery);
      const version = versionResult && versionResult.length > 0 ? versionResult[0].version : 'Unknown';

      return { success: true, version };
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error)
      };
    } finally {
      if (testConn) {
        // No explicit close needed; allow GC to clean up.
        testConn = null;
      }
    }
  }

  /**
   * Retrieve all tables and views from the Access database, including their columns.
   * @param _connection The database connection instance.
   * @param options Optional inspection parameters (e.g., schema filter).
   * @returns An array of TableInfo objects.
   */
  async getTables(_connection: DatabaseConnection, options?: InspectionOptions): Promise<TableInfo[]> {
    if (!this.connection || !this.isConnected) {
      throw new Error('Not connected to the database. Call connect() first.');
    }

    try {
      // Access stores metadata in the MSysObjects system table.
      // We filter for tables (type = 1) and system tables (type = -32768) are excluded.
      const tablesQuery = `
        SELECT 
          NAME as tablename,
          TYPE as tabletype
        FROM MSysObjects
        WHERE TYPE IN (1, 4, 6)  -- 1 = normal table, 4 = linked table, 6 = system table (filtered later)
          AND NAME NOT LIKE 'MSys%'  -- Exclude internal system tables
        ORDER BY NAME
      `;

      const tablesResult = await this.connection.query(tablesQuery);
      const tables = Array.isArray(tablesResult) ? tablesResult : [];

      const tableInfoList: TableInfo[] = [];

      for (const table of tables) {
        const tableName = table.tablename;
        const tableType = table.tabletype === 1 ? 'TABLE' : (table.tabletype === 4 ? 'LINKED TABLE' : 'VIEW');

        // Retrieve column information for each table
        const columnsQuery = `
          SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            ORDINAL_POSITION
          FROM INFORMATION_SCHEMA.COLUMNS
          WHERE TABLE_NAME = '${tableName.replace(/'/g, "''")}'
          ORDER BY ORDINAL_POSITION
        `;

        const columnsResult = await this.connection.query(columnsQuery);
        const columnsData = Array.isArray(columnsResult) ? columnsResult : [];

        const columns: ColumnMetadata[] = columnsData.map((col: any) => ({
          name: col.COLUMN_NAME,
          type: this.mapAccessType(col.DATA_TYPE, col.CHARACTER_MAXIMUM_LENGTH, col.NUMERIC_PRECISION, col.NUMERIC_SCALE),
          dataType: col.DATA_TYPE,
          nullable: col.IS_NULLABLE === 'YES',
          default: col.COLUMN_DEFAULT,
          comment: '',  // Access does not support comments on columns
          length: col.CHARACTER_MAXIMUM_LENGTH ? parseInt(col.CHARACTER_MAXIMUM_LENGTH, 10) : undefined,
          precision: col.NUMERIC_PRECISION ? parseInt(col.NUMERIC_PRECISION, 10) : undefined,
          scale: col.NUMERIC_SCALE ? parseInt(col.NUMERIC_SCALE, 10) : undefined,
          isIdentity: false, // Access does not explicitly expose auto-increment via schema
          ordinalPosition: col.ORDINAL_POSITION ? parseInt(col.ORDINAL_POSITION, 10) : undefined
        }));

        // Estimate row count for the table
        let rowCount: number | undefined = undefined;
        try {
          const countQuery = `SELECT COUNT(*) as cnt FROM "${tableName}"`;
          const countResult = await this.connection.query(countQuery);
          if (Array.isArray(countResult) && countResult.length > 0) {
            rowCount = parseInt(countResult[0].cnt, 10);
          }
        } catch (err) {
          // Ignore errors for views or tables without row count.
        }

        tableInfoList.push({
          schemaname: '',  // Access does not have schemas; treat as empty or use default
          tablename: tableName,
          tabletype: tableType,
          columns,
          comment: '',     // Access tables do not have comments
          rowCount,
          size: 0,         // File size can be retrieved via fs.stat, but omitted for simplicity
          originalData: table
        });
      }

      return tableInfoList;
    } catch (error) {
      throw new Error(`Failed to retrieve tables from Access: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Get detailed column information for the given tables.
   * (Access already provides full columns in getTables; this method is for interface compliance.)
   */
  async getTableColumns(_connection: DatabaseConnection, tables: TableInfo[]): Promise<TableInfo[]> {
    return tables;
  }

  /**
   * Obtain database version information.
   * @param _connection The database connection instance.
   * @returns An object containing the Access version and database file name.
   */
  async getDatabaseInfo(_connection: DatabaseConnection): Promise<DatabaseVersionInfo> {
    if (!this.connection || !this.isConnected) {
      throw new Error('Not connected to the database. Call connect() first.');
    }

    try {
      const versionQuery = 'SELECT @@VERSION as version';
      const versionResult = await this.connection.query(versionQuery);
      const version = versionResult && versionResult.length > 0 ? versionResult[0].version : 'Unknown';

      // Access does not have a built-in encoding/collation concept in the same way.
      return {
        version,
        name: '',    // Could be populated from the file path, but omitted for clarity
        encoding: '',
        collation: ''
      };
    } catch (error) {
      throw new Error(`Failed to retrieve database info: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Execute an arbitrary SQL query on the Access database.
   * @param _connection The database connection instance.
   * @param sql The SQL query string.
   * @param options Execution options (maxRows, timeout, autoDisconnect).
   * @returns A QueryResult object containing rows, row count, and fields.
   */
  async executeQuery(
    _connection: DatabaseConnection,
    sql: string,
    options?: QueryExecutionOptions
  ): Promise<QueryResult> {
    if (!this.connection || !this.isConnected) {
      throw new Error('Not connected to the database. Call connect() first.');
    }

    try {
      let finalSql = sql;

      // Apply maxRows limit using Access TOP syntax
      if (options?.maxRows && options.maxRows > 0) {
        // Ensure we don't double-apply a TOP clause
        if (!finalSql.toLowerCase().includes(' top ')) {
          finalSql = finalSql.replace(/select/i, `SELECT TOP ${options.maxRows}`);
        }
      }

      // Parameterized queries: node-adodb does not directly support parameters.
      // If parameters are provided, we inject them manually (with caution to avoid SQL injection).
      if (options?.params && options.params.length > 0) {
        // This is a simplified injection; for production use a safer replacement strategy.
        let paramIndex = 0;
        finalSql = finalSql.replace(/\?/g, () => {
          let param = options.params?.[paramIndex++];
          if (typeof param === 'string') {
            return `'${param.replace(/'/g, "''")}'`;
          }
          return String(param);
        });
      }

      // Handle timeout by wrapping the query in a Promise.race
      let result: any;
      if (options?.timeout) {
        const timeoutPromise = new Promise((_, reject) => {
          setTimeout(() => reject(new Error(`Query timed out after ${options.timeout} ms.`)), options.timeout);
        });
        result = await Promise.race([this.connection.query(finalSql), timeoutPromise]);
      } else {
        result = await this.connection.query(finalSql);
      }

      const rows = Array.isArray(result) ? result : [];
      const rowCount = rows.length;

      // Auto-disconnect if requested
      if (options?.autoDisconnect) {
        await this.disconnect(_connection);
      }

      return {
        success: true,
        rows,
        rowCount,
        fields: rowCount > 0 ? Object.keys(rows[0]).map(name => ({ name, type: 'unknown' })) : []
      };
    } catch (error) {
      throw new Error(`Failed to execute query: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Execute multiple queries within a transaction.
   * WARNING: Access/ADO does not support true nested transactions natively.
   * This implementation falls back to executing queries sequentially.
   */
  async executeTransaction(
    _connection: DatabaseConnection,
    queries: Array<{ sql: string; params?: any[] }>
  ): Promise<QueryResult[]> {
    if (!this.connection || !this.isConnected) {
      throw new Error('Not connected to the database. Call connect() first.');
    }

    const results: QueryResult[] = [];
    try {
      // Transaction emulation: run each query in sequence.
      for (const queryItem of queries) {
        const result = await this.executeQuery(_connection, queryItem.sql, { params: queryItem.params });
        results.push(result);
      }
      return results;
    } catch (error) {
      throw new Error(`Transaction sequence failed: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Retrieve constraints for a specific table.
   * Access does not expose constraints via INFORMATION_SCHEMA; we return an empty array as a placeholder.
   */
  async getTableConstraints(_connection: DatabaseConnection, _schema: string, _table: string): Promise<any[]> {
    // Access does not expose primary/foreign key constraints in a simple schema.
    console.warn('Table constraints are not available through the Access adapter.');
    return [];
  }

  /**
   * Retrieve available schemas. Access does not have a multi‑schema concept.
   * @returns An array with a single default schema.
   */
  async getSchemas(_connection: DatabaseConnection): Promise<string[]> {
    // Access databases do not have schemas; return a placeholder.
    return ['default'];
  }

  /**
   * Retrieve user-defined functions. Access does not have stored functions.
   * @returns An empty array.
   */
  async getFunctions(_connection: DatabaseConnection, _schema?: string): Promise<any[]> {
    return [];
  }

  /**
   * Retrieve indexes. Access does not expose index metadata via SQL.
   * @returns An empty array.
   */
  async getIndexes(_connection: DatabaseConnection, _tableName?: string): Promise<any[]> {
    return [];
  }

  /**
   * Map Access data types to a more conventional SQL type name.
   */
  private mapAccessType(dataType: string, length: number | null, precision: number | null, scale: number | null): string {
    const typeUpper = dataType.toUpperCase();
    switch (typeUpper) {
      case 'COUNTER':
        return 'AUTO_INCREMENT';
      case 'LONGTEXT':
      case 'MEMO':
        return 'TEXT';
      case 'CURRENCY':
        return 'MONEY';
      case 'YESNO':
      case 'BIT':
        return 'BOOLEAN';
      case 'DATETIME':
        return 'TIMESTAMP';
      case 'OLEOBJECT':
        return 'BLOB';
      default:
        if (length && length > 0) {
          return `${typeUpper}(${length})`;
        }
        if (precision !== null && scale !== null) {
          return `${typeUpper}(${precision},${scale})`;
        }
        return typeUpper;
    }
  }
}

export default AccessAdapter;