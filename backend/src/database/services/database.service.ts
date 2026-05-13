// backend/src/database/services/database.service.ts

import { DatabaseConnectionFactory, DatabaseType } from '../connection.factory';
import { IBaseDatabaseInspector } from '../inspection/base-inspector';
import { DatabaseConfig, InspectionOptions, QueryExecutionOptions, TableInfo } from '../types/inspection.types';

export class DatabaseService {
  private adapters: Map<string, IBaseDatabaseInspector> = new Map();
  private activeConnections: Map<string, any> = new Map();

  /**
   * Create a database adapter for a specific database type
   */
  createAdapter(dbType: DatabaseType): IBaseDatabaseInspector {
    const adapter = DatabaseConnectionFactory.createAdapter(dbType);
    this.adapters.set(dbType, adapter);
    return adapter;
  }

  /**
   * Test database connection
   */
  async testConnection(
    dbType: DatabaseType, 
    config: DatabaseConfig
  ): Promise<{ success: boolean; version?: string; error?: string }> {
    try {
      const adapter = this.createAdapter(dbType);
      return await adapter.testConnection(config);
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error)
      };
    }
  }

  /**
   * Connect to database and return connection ID
   */
  async connect(
    dbType: DatabaseType, 
    config: DatabaseConfig
  ): Promise<{ connectionId: string; success: boolean; error?: string }> {
    try {
      const adapter = this.createAdapter(dbType);
      const connection = await adapter.connect(config);
      
      const connectionId = this.generateConnectionId(dbType, config);
      this.activeConnections.set(connectionId, { adapter, connection });
      
      return { 
        connectionId, 
        success: true 
      };
    } catch (error) {
      return {
        connectionId: '',
        success: false,
        error: error instanceof Error ? error.message : String(error)
      };
    }
  }

  /**
   * Get tables from database
   */
  async getTables(
    connectionId: string, 
    options?: InspectionOptions
  ): Promise<{ tables: TableInfo[]; success: boolean; error?: string }> {
    const connectionInfo = this.activeConnections.get(connectionId);
    
    if (!connectionInfo) {
      return { 
        tables: [], 
        success: false, 
        error: 'Connection not found or expired' 
      };
    }

    try {
      const { adapter, connection } = connectionInfo;
      const tables = await adapter.getTables(connection, options);
      return { tables, success: true };
    } catch (error) {
      return { 
        tables: [], 
        success: false, 
        error: error instanceof Error ? error.message : String(error)
      };
    }
  }

  /**
   * Execute SQL query
   */
  async executeQuery(
    connectionId: string,
    sql: string,
    options?: QueryExecutionOptions
  ): Promise<{ 
    result: any; 
    success: boolean; 
    rowCount?: number; 
    executionTime?: number; 
    error?: string 
  }> {
    const connectionInfo = this.activeConnections.get(connectionId);
    
    if (!connectionInfo) {
      return { 
        result: null, 
        success: false, 
        error: 'Connection not found or expired' 
      };
    }

    try {
      const { adapter, connection } = connectionInfo;
      const result = await adapter.executeQuery(connection, sql, options);
      return {
        result: result.rows,
        success: result.success,
        rowCount: result.rowCount,
        executionTime: result.executionTime,
        error: result.error
      };
    } catch (error) {
      return { 
        result: null, 
        success: false, 
        error: error instanceof Error ? error.message : String(error)
      };
    }
  }

  /**
   * Disconnect from database
   */
  async disconnect(connectionId: string): Promise<{ success: boolean; error?: string }> {
    const connectionInfo = this.activeConnections.get(connectionId);
    
    if (!connectionInfo) {
      return { success: true }; // Already disconnected
    }

    try {
      const { adapter, connection } = connectionInfo;
      await adapter.disconnect(connection);
      this.activeConnections.delete(connectionId);
      return { success: true };
    } catch (error) {
      return { 
        success: false, 
        error: error instanceof Error ? error.message : String(error)
      };
    }
  }

  /**
   * Get database information
   */
  async getDatabaseInfo(connectionId: string): Promise<{ 
    info: any; 
    success: boolean; 
    error?: string 
  }> {
    const connectionInfo = this.activeConnections.get(connectionId);
    
    if (!connectionInfo) {
      return { 
        info: null, 
        success: false, 
        error: 'Connection not found or expired' 
      };
    }

    try {
      const { adapter, connection } = connectionInfo;
      const info = await adapter.getDatabaseInfo(connection);
      return { info, success: true };
    } catch (error) {
      return { 
        info: null, 
        success: false, 
        error: error instanceof Error ? error.message : String(error)
      };
    }
  }

  /**
   * Get active connections
   */
  getActiveConnections(): Array<{ connectionId: string; dbType: string; config: DatabaseConfig }> {
    const connections: Array<{ connectionId: string; dbType: string; config: DatabaseConfig }> = [];
    
    for (const [connectionId, info] of this.activeConnections.entries()) {
      connections.push({
        connectionId,
        dbType: this.extractDbTypeFromConnectionId(connectionId),
        config: info.connection?.config || {}
      });
    }
    
    return connections;
  }

// database.service.ts - Update the generateConnectionId method
private generateConnectionId(dbType: DatabaseType, config: DatabaseConfig): string {
  const timestamp = Date.now();
  const random = Math.random().toString(36).substring(2, 15);
  return `${dbType}_${config.host || 'localhost'}_${config.dbname}_${timestamp}_${random}`.toLowerCase();
}

  /**
   * Extract database type from connection ID
   */
  private extractDbTypeFromConnectionId(connectionId: string): string {
    return connectionId.split('_')[0];
  }
}

// Singleton instance
export const databaseService = new DatabaseService();