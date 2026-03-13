// src/hooks/useDatabase.ts
import { useState, useEffect, useCallback } from 'react';
import { databaseService, DatabaseStatus, QueryResult } from '../api/database-service';

export const useDatabase = () => {
  const [status, setStatus] = useState<DatabaseStatus>({ isConnected: false });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Check database status on mount
  useEffect(() => {
    checkStatus();
  }, []);

  const checkStatus = useCallback(async () => {
    setLoading(true);
    try {
      const status = await databaseService.getStatus();
      setStatus(status);
      setError(status.error || null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to check database status');
    } finally {
      setLoading(false);
    }
  }, []);

  const executeQuery = useCallback(async (sql: string, params?: any[]): Promise<QueryResult> => {
    setLoading(true);
    try {
      const result = await databaseService.executeQuery(sql, params);
      if (!result.success) {
        setError(result.error || 'Query failed');
      }
      return result;
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : 'Query execution failed';
      setError(errorMsg);
      return { success: false, error: errorMsg };
    } finally {
      setLoading(false);
    }
  }, []);

  const uploadFile = useCallback(async (file: File, fileType: string) => {
    setLoading(true);
    try {
      return await databaseService.uploadFile(file, fileType);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'File upload failed');
      return { success: false, error: 'Upload failed' };
    } finally {
      setLoading(false);
    }
  }, []);

  return {
    status,
    loading,
    error,
    checkStatus,
    executeQuery,
    uploadFile,
    listTables: databaseService.listTables.bind(databaseService),
    createForeignTable: databaseService.createForeignTable.bind(databaseService),
    listForeignTables: databaseService.listForeignTables.bind(databaseService),
    dropForeignTable: databaseService.dropForeignTable.bind(databaseService),
  };
};