// backend/src/database/adapters/postgresql.adapter.ts

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

// Import the PostgreSQL inspector from your existing code
import { PostgreSQLSchemaInspector, PostgreSQLConnection } from '../inspection/postgreSql-inspector';

/**
 * PostgreSQL Database Adapter
 * Implements the base inspector interface using the PostgreSQLSchemaInspector
 */
export class PostgreSQLAdapter implements IBaseDatabaseInspector {
  private inspector: PostgreSQLSchemaInspector | null = null;
  private connectionInstance: PostgreSQLConnection | null = null;

  /**
   * Connect to PostgreSQL database
   */
  async connect(config: DatabaseConfig): Promise<DatabaseConnection> {
    try {
      // Create inspector instance
      this.inspector = new PostgreSQLSchemaInspector({
        dbname: config.dbname,
        pghost: config.host || 'localhost',
        pgport: config.port || '5432',
        pguser: config.user,
        password: config.password,
        schema: config.schema || 'public'
      });

      // Connect to database
      await this.inspector.getConnection().connect();
      this.connectionInstance = this.inspector.getConnection();
      
      return this.connectionInstance as unknown as DatabaseConnection;
    } catch (error) {
      throw new Error(`Failed to connect to PostgreSQL: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Disconnect from PostgreSQL database
   */
  async disconnect(_connection: DatabaseConnection): Promise<void> {
    try {
      if (this.connectionInstance) {
        await this.connectionInstance.disconnect();
      }
      this.inspector = null;
      this.connectionInstance = null;
    } catch (error) {
      console.error('Error disconnecting from PostgreSQL:', error);
    }
  }

  /**
   * Test connection without full schema inspection
   */
  async testConnection(config: DatabaseConfig): Promise<{ success: boolean; version?: string; error?: string }> {
    try {
      const inspector = new PostgreSQLSchemaInspector({
        dbname: config.dbname,
        pghost: config.host || 'localhost',
        pgport: config.port || '5432',
        pguser: config.user,
        password: config.password,
        schema: config.schema || 'public'
      });
      
      return await inspector.testConnection();
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error)
      };
    }
  }

  /**
   * Get tables and views from PostgreSQL database
   */
  async getTables(_connection: DatabaseConnection, options?: InspectionOptions): Promise<TableInfo[]> {
    if (!this.inspector) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    try {
      // Set schema if specified in options
      if (options?.schema) {
        this.inspector.setSchema(options.schema);
      }

      // Get tables from inspector
      const tables = await this.inspector.getTables();
      
      // Convert to standard TableInfo format
      return tables.map(table => ({
        schemaname: table.schemaname,
        tablename: table.tablename,
        tabletype: table.tabletype,
        columns: table.columns.map(col => ({
          name: col.name,
          type: col.type,
          dataType: col.type, // PostgreSQL inspector doesn't have separate dataType
          nullable: col.nullable || false,
          default: col.default,
          comment: col.comment,
          length: col.length,
          precision: col.precision,
          scale: col.scale,
          isIdentity: col.isIdentity,
          ordinalPosition: table.columns.indexOf(col) + 1
        } as ColumnMetadata)),
        comment: table.comment,
        rowCount: table.rowCount,
        size: table.size,
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
    // Note: PostgreSQL inspector already includes columns in getTables()
    // This method is provided for compatibility with the interface
    return tables;
  }

  /**
   * Get database version information
   */
  async getDatabaseInfo(_connection: DatabaseConnection): Promise<DatabaseVersionInfo> {
    if (!this.inspector) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    try {
      const info = await this.inspector.getDatabaseInfo();
      return {
        version: info.version,
        name: info.name,
        encoding: info.encoding,
        collation: info.collation
      };
    } catch (error) {
      throw new Error(`Failed to get database info: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Execute arbitrary SQL query (PostgreSQL specific feature)
   */
  // postgresql.adapter.ts – executeQuery method
async executeQuery(
  _connection: DatabaseConnection, 
  sql: string, 
  options?: QueryExecutionOptions
): Promise<QueryResult> {
  if (!this.inspector) throw new Error('Inspector not initialized');

  const pgOptions = {
    maxRows: options?.maxRows,
    timeout: options?.timeout,
    autoDisconnect: options?.autoDisconnect
  };
  const params = options?.params || [];   // <-- extract params

  return await this.inspector.executeQuery(sql, params, pgOptions);
}
  /**
   * Execute multiple queries in a transaction
   */
  async executeTransaction(
    _connection: DatabaseConnection, 
    queries: Array<{ sql: string; params?: any[] }>
  ): Promise<QueryResult[]> {
    if (!this.inspector) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    try {
      return await this.inspector.executeTransaction(queries);
    } catch (error) {
      throw new Error(`Transaction failed: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Get table constraints
   */
  async getTableConstraints(_connection: DatabaseConnection, _schema: string, _table: string): Promise<any[]> {
    if (!this.inspector) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    try {
      // This would need to be implemented in the PostgreSQL inspector
      // For now, return empty array
      return [];
    } catch (error) {
      throw new Error(`Failed to get constraints: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Get schema list
   */
  async getSchemas(connection: DatabaseConnection): Promise<string[]> {
    if (!this.inspector) {
      throw new Error('Inspector not initialized. Call connect() first.');
    }

    try {
      // Execute query to get schemas
      const query = `
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        ORDER BY schema_name
      `;
      
      const result = await this.executeQuery(connection, query);
      
      if (result.success && result.rows) {
        return result.rows.map(row => row.schema_name);
      }
      
      return ['public'];
    } catch (error) {
      console.error('Failed to get schemas:', error);
      return ['public'];
    }
  }

  /**
   * Additional PostgreSQL-specific methods
   */
  
  /**
   * Get database functions/procedures
   */
  async getFunctions(connection: DatabaseConnection, schema?: string): Promise<any[]> {
    const schemaFilter = schema ? `AND n.nspname = '${schema}'` : "AND n.nspname NOT IN ('pg_catalog', 'information_schema')";
    
    const query = `
      SELECT 
        n.nspname as schema,
        p.proname as function_name,
        pg_catalog.pg_get_function_result(p.oid) as return_type,
        pg_catalog.pg_get_function_arguments(p.oid) as arguments,
        CASE 
          WHEN p.prokind = 'a' THEN 'aggregate'
          WHEN p.prokind = 'w' THEN 'window'
          WHEN p.prorettype = 'pg_catalog.trigger'::pg_catalog.regtype THEN 'trigger'
          ELSE 'normal'
        END as function_type,
        l.lanname as language
      FROM pg_catalog.pg_proc p
      LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
      LEFT JOIN pg_catalog.pg_language l ON l.oid = p.prolang
      WHERE n.nspname NOT LIKE 'pg_%'
        ${schemaFilter}
      ORDER BY n.nspname, p.proname
    `;
    
    const result = await this.executeQuery(connection, query);
    return result.success ? result.rows || [] : [];
  }

  /**
   * Get database indexes
   */
  async getIndexes(connection: DatabaseConnection, tableName?: string): Promise<any[]> {
    const tableFilter = tableName ? `AND t.relname = '${tableName}'` : '';
    
    const query = `
      SELECT 
        n.nspname as schema_name,
        t.relname as table_name,
        i.relname as index_name,
        pg_catalog.pg_get_indexdef(i.oid) as index_def,
        a.amname as index_type,
        ix.indisunique as is_unique,
        ix.indisprimary as is_primary
      FROM pg_catalog.pg_index ix
      JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid
      JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
      JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
      JOIN pg_catalog.pg_am a ON a.oid = i.relam
      WHERE t.relkind IN ('r', 'p')
        AND n.nspname NOT IN ('pg_catalog', 'information_schema')
        ${tableFilter}
      ORDER BY n.nspname, t.relname, i.relname
    `;
    
    const result = await this.executeQuery(connection, query);
    return result.success ? result.rows || [] : [];
  }
}

export default PostgreSQLAdapter;