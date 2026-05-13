// backend/src/database/inspection/base-inspector.ts

import { 
  DatabaseConnection, 
  QueryResult, 
  TableInfo, 
  DatabaseConfig,
  InspectionOptions,
  QueryExecutionOptions,
  DatabaseVersionInfo
} from '../types/inspection.types';

/**
 * Base interface for all database inspectors
 */
export interface IBaseDatabaseInspector {
  /**
   * Connect to the database
   */
  connect(config: DatabaseConfig): Promise<DatabaseConnection>;

  /**
   * Disconnect from the database
   */
  disconnect(connection: DatabaseConnection): Promise<void>;

  /**
   * Test connection
   */
  testConnection(config: DatabaseConfig): Promise<{ success: boolean; version?: string; error?: string }>;

  /**
   * Get tables and views from the database
   */
  getTables(connection: DatabaseConnection, options?: InspectionOptions): Promise<TableInfo[]>;

  /**
   * Get columns for specific tables
   */
  getTableColumns(connection: DatabaseConnection, tables: TableInfo[]): Promise<TableInfo[]>;

  /**
   * Get database version information
   */
  getDatabaseInfo(connection: DatabaseConnection): Promise<DatabaseVersionInfo>;

  /**
   * Execute arbitrary SQL query
   */
  executeQuery(connection: DatabaseConnection, sql: string, options?: QueryExecutionOptions): Promise<QueryResult>;

  /**
   * Execute multiple queries in a transaction
   */
  executeTransaction(connection: DatabaseConnection, queries: Array<{ sql: string; params?: any[] }>): Promise<QueryResult[]>;

  /**
   * Get table constraints
   */
  getTableConstraints(connection: DatabaseConnection, schema: string, table: string): Promise<any[]>;

  /**
   * Get schema list
   */
  getSchemas(connection: DatabaseConnection): Promise<string[]>;
}

/**
 * Base inspector class with common functionality
 */
export abstract class BaseDatabaseInspector implements IBaseDatabaseInspector {
  abstract connect(config: DatabaseConfig): Promise<DatabaseConnection>;
  abstract disconnect(connection: DatabaseConnection): Promise<void>;
  abstract testConnection(config: DatabaseConfig): Promise<{ success: boolean; version?: string; error?: string }>;
  abstract getTables(connection: DatabaseConnection, options?: InspectionOptions): Promise<TableInfo[]>;
  abstract getTableColumns(connection: DatabaseConnection, tables: TableInfo[]): Promise<TableInfo[]>;
  abstract getDatabaseInfo(connection: DatabaseConnection): Promise<DatabaseVersionInfo>;
  abstract executeQuery(connection: DatabaseConnection, sql: string, options?: QueryExecutionOptions): Promise<QueryResult>;
  abstract executeTransaction(connection: DatabaseConnection, queries: Array<{ sql: string; params?: any[] }>): Promise<QueryResult[]>;
  abstract getTableConstraints(connection: DatabaseConnection, schema: string, table: string): Promise<any[]>;
  abstract getSchemas(connection: DatabaseConnection): Promise<string[]>;

  /**
   * Validate connection configuration
   */
  protected validateConfig(config: DatabaseConfig): string[] {
    const errors: string[] = [];
    
    if (!config.dbname || config.dbname.trim() === '') {
      errors.push('Database name is required');
    }
    
    if (config.port) {
      const port = parseInt(config.port);
      if (isNaN(port)) {
        errors.push('Port must be a valid number');
      } else if (port < 1 || port > 65535) {
        errors.push('Port must be between 1 and 65535');
      }
    }
    
    return errors;
  }

  /**
   * Format table information to standard format
   */
  protected formatTableInfo(tables: any[]): TableInfo[] {
    return tables.map(table => ({
      schemaname: table.schemaname || '',
      tablename: table.tablename || '',
      tabletype: table.tabletype || 'table',
      columns: table.columns || [],
      originalData: table.originalData || table,
      comment: table.comment,
      rowCount: table.rowCount,
      size: table.size,
      hasindex: table.hasindex,
      relpages: table.relpages,
      reltuples: table.reltuples,
      numatts: table.numatts
    }));
  }

  /**
   * Handle query execution errors
   */
  protected handleQueryError(error: any): QueryResult {
    const errorMessage = error instanceof Error ? error.message : String(error);
    return {
      success: false,
      error: errorMessage,
      rows: [],
      rowCount: 0,
      executionTime: 0
    };
  }

  /**
   * Apply row limit to SELECT queries
   */
  protected applyRowLimit(sql: string, maxRows: number): string {
    if (!maxRows) return sql;
    
    const trimmed = sql.trim();
    const upperSql = trimmed.toUpperCase();
    
    // Only apply to SELECT queries
    if (upperSql.startsWith('SELECT')) {
      // Check if query already has LIMIT clause
      if (!upperSql.includes('LIMIT') && !upperSql.includes('FETCH FIRST')) {
        // Simple case: add LIMIT at the end (PostgreSQL/MySQL style)
        return `${trimmed} LIMIT ${maxRows}`;
      }
    }
    
    return sql;
  }
}