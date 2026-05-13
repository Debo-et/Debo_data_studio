// client/hooks/useDatabaseApi.ts
import { useState, useCallback } from 'react';
import databaseApi, { 
  QueryResult, 
  TestConnectionResult,
  DatabaseConfig,
  DatabaseType,
  TableInfo,
  QueryExecutionOptions,
  QueryExecutionResult,
  ConnectResult  // Added import
} from '../services/database-api.service';

// In type files define ConnectionParams as DatabaseConfig alias
export interface ConnectionParams extends DatabaseConfig {}

export const useDatabaseApi = () => {
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  const testConnection = useCallback(async (
    dbType: DatabaseType,
    connectionParams: DatabaseConfig
  ): Promise<TestConnectionResult> => {
    setLoading(true);
    setError(null);
    
    try {
      const result = await databaseApi.testConnection(dbType, connectionParams);
      
      if (!result.success) {
        setError(result.error || 'Connection failed');
      }
      
      return result;
    } catch (err: any) {
      const errorMsg = err.message || 'Failed to test connection';
      setError(errorMsg);
      return {
        success: false,
        error: errorMsg
      };
    } finally {
      setLoading(false);
    }
  }, []);

  // Added: Connect function
  const connect = useCallback(async (
    dbType: DatabaseType,
    connectionParams: DatabaseConfig
  ): Promise<ConnectResult> => {
    setLoading(true);
    setError(null);
    
    try {
      const result = await databaseApi.connect(dbType, connectionParams);
      
      if (!result.success) {
        setError(result.error || 'Connection failed');
      }
      
      return result;
    } catch (err: any) {
      const errorMsg = err.message || 'Failed to connect';
      setError(errorMsg);
      return {
        connectionId: '',
        success: false,
        error: errorMsg
      };
    } finally {
      setLoading(false);
    }
  }, []);

  const executeQuery = useCallback(async (
    connectionId: string,
    query: string,
    params: any[] = []
  ): Promise<QueryResult> => {
    setLoading(true);
    setError(null);
    
    try {
      // Fix: params should be part of options
      const options: QueryExecutionOptions = { params };
      const response: QueryExecutionResult = await databaseApi.executeQuery(connectionId, query, options);
      
      if (!response.success) {
        setError(response.error || 'Query execution failed');
      }
      
      // Convert QueryExecutionResult to QueryResult
      return {
        success: response.success,
        rows: response.result?.rows || [],
        rowCount: response.result?.rowCount || response.rowCount,
        fields: response.result?.fields || [],
        executionTime: response.executionTime,
        error: response.error,
        affectedRows: response.result?.affectedRows,
        command: response.result?.command
      };
    } catch (err: any) {
      const errorMsg = err.message || 'Failed to execute query';
      setError(errorMsg);
      return {
        success: false,
        error: errorMsg,
        rows: [],
        fields: [],
        executionTime: 0
      };
    } finally {
      setLoading(false);
    }
  }, []);

  const getTables = useCallback(async (connectionId: string): Promise<{
    success: boolean;
    tables?: TableInfo[];
    error?: string;
  }> => {
    setLoading(true);
    setError(null);
    
    try {
      const result = await databaseApi.getTables(connectionId);
      
      if (!result.success) {
        setError(result.error || 'Failed to fetch tables');
      }
      
      return result;
    } catch (err: any) {
      const errorMsg = err.message || 'Failed to get tables';
      setError(errorMsg);
      return {
        success: false,
        error: errorMsg
      };
    } finally {
      setLoading(false);
    }
  }, []);

  const disconnect = useCallback(async (connectionId: string): Promise<boolean> => {
    setLoading(true);
    
    try {
      const result = await databaseApi.disconnect(connectionId);
      return result.success;
    } catch (err: any) {
      setError(err.message || 'Failed to disconnect');
      return false;
    } finally {
      setLoading(false);
    }
  }, []);

  return {
    loading,
    error,
    testConnection,
    connect,  // Added to return object
    executeQuery,
    getTables,
    disconnect,
    clearError: () => setError(null)
  };
};