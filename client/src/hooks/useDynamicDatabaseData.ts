// Create a new file: src/hooks/useDynamicDatabaseData.ts
import { useState, useCallback } from 'react';
import { databaseApi } from '../services/database-api.service';
import { toast } from 'react-toastify';

export interface ForeignTableData {
  schemaname: string;
  tablename: string;
  tabletype: string;
  foreign_server: string;
  foreign_options: any;
  columns: Array<{
    column_name: string;
    data_type: string;
    character_maximum_length: number | null;
    numeric_precision: number | null;
    numeric_scale: number | null;
    is_nullable: string;
    column_default: string | null;
  }>;
}

export interface RepositoryNodeData {
  id: string;
  name: string;
  type: 'foreign-table' | 'database' | 'folder';
  metadata?: any;
  children?: RepositoryNodeData[];
  parentId?: string;
}

export function useDynamicDatabaseData() {
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [foreignTables, setForeignTables] = useState<ForeignTableData[]>([]);
  const [lastFetched, setLastFetched] = useState<Date | null>(null);

  const fetchForeignTables = useCallback(async (connectionId: string) => {
    if (!connectionId) {
      setError('No database connection available');
      return [];
    }

    setIsLoading(true);
    setError(null);

    try {
      console.log('🔄 Fetching foreign tables dynamically from PostgreSQL...');

      // First, get the list of foreign tables
      const tablesResult = await databaseApi.listForeignTables();
      
      if (!tablesResult.success || !tablesResult.tables) {
        throw new Error(tablesResult.error || 'Failed to fetch foreign tables list');
      }

      // For each foreign table, fetch column information
      const tablesWithColumns = await Promise.all(
        tablesResult.tables.map(async (table: any) => {
          try {
            const columnsResult = await databaseApi.getForeignTableColumns(
              connectionId,
              table.tablename
            );

            if (columnsResult.success && columnsResult.columns) {
              return {
                schemaname: table.schemaname || 'public',
                tablename: table.tablename,
                tabletype: 'foreign table',
                foreign_server: table.server_name || 'unknown',
                foreign_options: {},
                columns: columnsResult.columns
              };
            } else {
              return {
                schemaname: table.schemaname || 'public',
                tablename: table.tablename,
                tabletype: 'foreign table',
                foreign_server: table.server_name || 'unknown',
                foreign_options: {},
                columns: [],
                error: columnsResult.error || 'Failed to fetch columns'
              };
            }
          } catch (columnError) {
            console.warn(`Failed to fetch columns for ${table.tablename}:`, columnError);
            return {
              schemaname: table.schemaname || 'public',
              tablename: table.tablename,
              tabletype: 'foreign table',
              foreign_server: table.server_name || 'unknown',
              foreign_options: {},
              columns: [],
              error: columnError instanceof Error ? columnError.message : 'Unknown error'
            };
          }
        })
      );

      const validTables = tablesWithColumns.filter(table => !table.error);
      const failedTables = tablesWithColumns.filter(table => table.error);

      if (failedTables.length > 0) {
        console.warn(`${failedTables.length} tables failed to load columns`);
        failedTables.forEach(table => {
          console.warn(`  - ${table.tablename}: ${table.error}`);
        });
      }

      setForeignTables(validTables);
      setLastFetched(new Date());

      console.log(`✅ Retrieved ${validTables.length} foreign tables dynamically`);
      
      toast.success(`Loaded ${validTables.length} foreign tables from database`, {
        position: "bottom-right",
        autoClose: 3000,
      });

      return validTables;
    } catch (err: any) {
      const errorMessage = err instanceof Error ? err.message : 'Failed to fetch foreign tables';
      setError(errorMessage);
      
      toast.error(`❌ Failed to fetch foreign tables: ${errorMessage}`, {
        position: "bottom-right",
        autoClose: 5000,
      });
      
      console.error('Failed to fetch foreign tables:', err);
      return [];
    } finally {
      setIsLoading(false);
    }
  }, []);

  const convertForeignTableToRepositoryNode = useCallback((table: ForeignTableData): RepositoryNodeData => {
    // Map PostgreSQL column types to UI-friendly types
    const mapColumnType = (postgresType: string): string => {
      const typeLower = postgresType.toLowerCase();
      
      if (typeLower.includes('int')) return 'integer';
      if (typeLower.includes('decimal') || typeLower.includes('numeric')) return 'decimal';
      if (typeLower.includes('float') || typeLower.includes('double') || typeLower.includes('real')) return 'float';
      if (typeLower.includes('date') && !typeLower.includes('time')) return 'date';
      if (typeLower.includes('timestamp')) return 'datetime';
      if (typeLower.includes('time')) return 'time';
      if (typeLower.includes('bool')) return 'boolean';
      if (typeLower.includes('json')) return 'json';
      if (typeLower.includes('xml')) return 'xml';
      if (typeLower.includes('char') || typeLower.includes('text')) return 'string';
      
      return 'string';
    };

    return {
      id: `foreign-table-${table.schemaname}-${table.tablename}`,
      name: table.tablename,
      type: 'foreign-table' as const,
      metadata: {
        postgresTableName: table.tablename,
        schema: table.schemaname,
        foreignServer: table.foreign_server,
        columns: table.columns.map(col => ({
          name: col.column_name,
          type: mapColumnType(col.data_type),
          originalType: col.data_type,
          length: col.character_maximum_length,
          precision: col.numeric_precision,
          scale: col.numeric_scale,
          nullable: col.is_nullable === 'YES',
          defaultValue: col.column_default,
          isForeignColumn: true
        })),
        tableType: table.tabletype,
        fetchedAt: new Date().toISOString(),
        source: 'postgresql-dynamic'
      }
    };
  }, []);

  const buildRepositoryTree = useCallback((tables: ForeignTableData[]): RepositoryNodeData[] => {
    // Create the foreign tables folder
    const foreignTablesFolder: RepositoryNodeData = {
      id: 'postgresql-foreign-tables',
      name: 'PostgreSQL Foreign Tables',
      type: 'folder' as const,
      children: tables.map(convertForeignTableToRepositoryNode),
      metadata: {
        description: 'Dynamically loaded foreign tables from PostgreSQL',
        lastRefreshed: new Date().toISOString(),
        tableCount: tables.length,
        source: 'postgresql-dynamic'
      }
    };

    // Return minimal repository structure
    return [
      {
        id: 'databases',
        name: 'Databases',
        type: 'folder' as const,
        children: [foreignTablesFolder],
        metadata: {
          description: 'Database connections and foreign tables',
          dynamic: true
        }
      },
      // Add other minimal static folders
      {
        id: 'file-sources',
        name: 'File Sources',
        type: 'folder' as const,
        children: [],
        metadata: {
          description: 'Static file sources (Excel, CSV, etc.)',
          dynamic: false
        }
      },
      {
        id: 'job-designs',
        name: 'Job Designs',
        type: 'folder' as const,
        children: [],
        metadata: {
          description: 'Canvas job designs',
          dynamic: false
        }
      }
    ];
  }, [convertForeignTableToRepositoryNode]);

  const refreshData = useCallback(async (connectionId: string) => {
    const tables = await fetchForeignTables(connectionId);
    return buildRepositoryTree(tables);
  }, [fetchForeignTables, buildRepositoryTree]);

  return {
    isLoading,
    error,
    foreignTables,
    lastFetched,
    fetchForeignTables,
    convertForeignTableToRepositoryNode,
    buildRepositoryTree,
    refreshData
  };
}