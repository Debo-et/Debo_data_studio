// src/components/metadata/TableMetadataView.tsx
import React, { useState } from 'react';
import { Table, Key, Database, Calendar } from 'lucide-react';
import { TableMetadata, ColumnMetadata } from '../../types/metadata.types';

interface TableMetadataViewProps {
  metadata: TableMetadata;
  onColumnClick?: (column: ColumnMetadata) => void;
  compact?: boolean;
}

const TableMetadataView: React.FC<TableMetadataViewProps> = ({ 
  metadata, 
  onColumnClick,
  compact = false 
}) => {
  const [expandedColumns, setExpandedColumns] = useState<Set<string>>(new Set());
  const [sortConfig, setSortConfig] = useState<{ key: keyof ColumnMetadata; direction: 'asc' | 'desc' } | null>(null);

  const toggleColumn = (columnName: string) => {
    const newSet = new Set(expandedColumns);
    if (newSet.has(columnName)) {
      newSet.delete(columnName);
    } else {
      newSet.add(columnName);
    }
    setExpandedColumns(newSet);
  };

  const sortedColumns = React.useMemo(() => {
    if (!sortConfig) return metadata.columns;
    
    return [...metadata.columns].sort((a, b) => {
      const aValue = a[sortConfig.key];
      const bValue = b[sortConfig.key];
      
      if (aValue === undefined && bValue === undefined) return 0;
      if (aValue === undefined) return 1;
      if (bValue === undefined) return -1;
      
      if (typeof aValue === 'string' && typeof bValue === 'string') {
        return sortConfig.direction === 'asc' 
          ? aValue.localeCompare(bValue)
          : bValue.localeCompare(aValue);
      }
      
      return sortConfig.direction === 'asc'
        ? (aValue < bValue ? -1 : 1)
        : (bValue < aValue ? -1 : 1);
    });
  }, [metadata.columns, sortConfig]);

  const handleSort = (key: keyof ColumnMetadata) => {
    setSortConfig(current => {
      if (!current || current.key !== key) {
        return { key, direction: 'asc' };
      }
      if (current.direction === 'asc') {
        return { key, direction: 'desc' };
      }
      return null;
    });
  };

  const getDataTypeColor = (dataType: string) => {
    const type = dataType.toLowerCase();
    if (type.includes('int') || type.includes('num') || type.includes('float') || type.includes('decimal')) {
      return 'bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300';
    }
    if (type.includes('char') || type.includes('text') || type.includes('string')) {
      return 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300';
    }
    if (type.includes('date') || type.includes('time')) {
      return 'bg-purple-100 text-purple-800 dark:bg-purple-900/30 dark:text-purple-300';
    }
    if (type.includes('bool')) {
      return 'bg-amber-100 text-amber-800 dark:bg-amber-900/30 dark:text-amber-300';
    }
    return 'bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-300';
  };

  if (compact) {
    return (
      <div className="compact-table-view p-3 bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700">
        <div className="flex items-center justify-between mb-2">
          <div className="flex items-center space-x-2">
            <Table className="h-4 w-4 text-blue-500" />
            <span className="font-semibold text-sm">{metadata.tableName}</span>
          </div>
          <span className="text-xs bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300 px-2 py-1 rounded">
            {metadata.columns.length} cols
          </span>
        </div>
        <div className="text-xs text-gray-600 dark:text-gray-400 space-y-1">
          <div className="flex justify-between">
            <span>Primary Keys:</span>
            <span className="font-medium">
              {metadata.columns.filter(c => c.isKey).length}
            </span>
          </div>
          <div className="flex justify-between">
            <span>Required:</span>
            <span className="font-medium">
              {metadata.columns.filter(c => !c.nullable).length}
            </span>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="table-metadata-view w-full">
      {/* Table Header */}
      <div className="bg-gradient-to-r from-blue-50 to-indigo-50 dark:from-gray-800 dark:to-gray-900 p-4 rounded-t-lg border border-gray-200 dark:border-gray-700">
        <div className="flex items-start justify-between">
          <div className="flex items-center space-x-3">
            <div className="p-2 bg-white dark:bg-gray-800 rounded-lg shadow-sm">
              <Table className="h-6 w-6 text-blue-600 dark:text-blue-400" />
            </div>
            <div>
              <h3 className="text-lg font-bold text-gray-900 dark:text-white flex items-center">
                {metadata.tableName}
                {metadata.schemaName && (
                  <span className="ml-2 text-sm font-normal text-gray-500 dark:text-gray-400">
                    ({metadata.schemaName})
                  </span>
                )}
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400 mt-1">
                {metadata.description || 'No description provided'}
              </p>
            </div>
          </div>
          <div className="text-right">
            <div className="text-sm text-gray-500 dark:text-gray-400">Table Statistics</div>
            <div className="flex items-center space-x-4 mt-2">
              <div className="text-center">
                <div className="text-xl font-bold text-gray-900 dark:text-white">
                  {metadata.columns.length}
                </div>
                <div className="text-xs text-gray-500 dark:text-gray-400">Columns</div>
              </div>
              {metadata.rowCount !== undefined && (
                <div className="text-center">
                  <div className="text-xl font-bold text-gray-900 dark:text-white">
                    {metadata.rowCount.toLocaleString()}
                  </div>
                  <div className="text-xs text-gray-500 dark:text-gray-400">Rows</div>
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Table Properties */}
        {(metadata.lastModified || metadata.created || metadata.size) && (
          <div className="mt-4 grid grid-cols-3 gap-4 text-sm">
            {metadata.lastModified && (
              <div className="flex items-center">
                <Calendar className="h-4 w-4 text-gray-400 mr-2" />
                <div>
                  <div className="text-gray-500 dark:text-gray-400">Last Modified</div>
                  <div className="font-medium">
                    {new Date(metadata.lastModified).toLocaleDateString()}
                  </div>
                </div>
              </div>
            )}
            {metadata.created && (
              <div className="flex items-center">
                <Calendar className="h-4 w-4 text-gray-400 mr-2" />
                <div>
                  <div className="text-gray-500 dark:text-gray-400">Created</div>
                  <div className="font-medium">
                    {new Date(metadata.created).toLocaleDateString()}
                  </div>
                </div>
              </div>
            )}
            {metadata.size && (
              <div className="flex items-center">
                <Database className="h-4 w-4 text-gray-400 mr-2" />
                <div>
                  <div className="text-gray-500 dark:text-gray-400">Size</div>
                  <div className="font-medium">{metadata.size}</div>
                </div>
              </div>
            )}
          </div>
        )}
      </div>

      {/* Columns Table */}
      <div className="border-x border-gray-200 dark:border-gray-700">
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
            <thead className="bg-gray-50 dark:bg-gray-800">
              <tr>
                <th 
                  scope="col" 
                  className="px-6 py-3 text-left text-xs font-semibold text-gray-700 dark:text-gray-300 uppercase tracking-wider cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-700"
                  onClick={() => handleSort('name')}
                >
                  <div className="flex items-center">
                    Column Name
                    {sortConfig?.key === 'name' && (
                      <span className="ml-1">{sortConfig.direction === 'asc' ? '↑' : '↓'}</span>
                    )}
                  </div>
                </th>
                <th 
                  scope="col" 
                  className="px-6 py-3 text-left text-xs font-semibold text-gray-700 dark:text-gray-300 uppercase tracking-wider cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-700"
                  onClick={() => handleSort('dataType')}
                >
                  <div className="flex items-center">
                    Data Type
                    {sortConfig?.key === 'dataType' && (
                      <span className="ml-1">{sortConfig.direction === 'asc' ? '↑' : '↓'}</span>
                    )}
                  </div>
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-semibold text-gray-700 dark:text-gray-300 uppercase tracking-wider">
                  Constraints
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-semibold text-gray-700 dark:text-gray-300 uppercase tracking-wider">
                  Description
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-semibold text-gray-700 dark:text-gray-300 uppercase tracking-wider">
                  Details
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200 dark:divide-gray-700">
              {sortedColumns.map((column, index) => (
                <React.Fragment key={column.name}>
                  <tr 
                    className={`hover:bg-gray-50 dark:hover:bg-gray-800/50 cursor-pointer ${
                      index % 2 === 0 ? 'bg-white dark:bg-gray-900' : 'bg-gray-50 dark:bg-gray-900/50'
                    }`}
                    onClick={() => onColumnClick?.(column)}
                  >
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div className="flex items-center">
                        {column.isKey && (
                          <Key className="h-4 w-4 mr-2 text-amber-500 flex-shrink-0" />
                        )}
                        <div>
                          <div className="font-medium text-gray-900 dark:text-white">
                            {column.name}
                          </div>
                          {column.nullable ? (
                            <span className="text-xs text-gray-500 dark:text-gray-400">Nullable</span>
                          ) : (
                            <span className="text-xs text-red-500 dark:text-red-400 font-semibold">Required</span>
                          )}
                        </div>
                      </div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div className="flex items-center space-x-2">
                        <span className={`inline-flex items-center px-3 py-1 rounded-full text-sm font-medium ${getDataTypeColor(column.dataType)}`}>
                          {column.dataType}
                        </span>
                        {(column.length || column.precision) && (
                          <span className="text-xs text-gray-500 dark:text-gray-400">
                            ({column.length || column.precision}
                            {column.scale && `,${column.scale}`})
                          </span>
                        )}
                      </div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div className="flex flex-wrap gap-1">
                        {column.isKey && (
                          <span className="inline-flex items-center px-2 py-1 text-xs rounded bg-amber-100 text-amber-800 dark:bg-amber-900/30 dark:text-amber-300">
                            <Key className="h-3 w-3 mr-1" />
                            Primary
                          </span>
                        )}
                        {column.isUnique && (
                          <span className="inline-flex items-center px-2 py-1 text-xs rounded bg-purple-100 text-purple-800 dark:bg-purple-900/30 dark:text-purple-300">
                            Unique
                          </span>
                        )}
                        {column.defaultValue && (
                          <span className="inline-flex items-center px-2 py-1 text-xs rounded bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300">
                            Default: {column.defaultValue}
                          </span>
                        )}
                      </div>
                    </td>
                    <td className="px-6 py-4">
                      <div className="text-sm text-gray-900 dark:text-gray-300">
                        {column.description || '-'}
                      </div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          toggleColumn(column.name);
                        }}
                        className="text-blue-600 hover:text-blue-900 dark:text-blue-400 dark:hover:text-blue-300"
                      >
                        {expandedColumns.has(column.name) ? 'Hide' : 'Show'} details
                      </button>
                    </td>
                  </tr>
                  {expandedColumns.has(column.name) && (
                    <tr className="bg-blue-50 dark:bg-blue-900/10">
                      <td colSpan={5} className="px-6 py-4">
                        <div className="grid grid-cols-3 gap-4 text-sm">
                          {column.length !== undefined && (
                            <div>
                              <div className="text-gray-500 dark:text-gray-400">Length</div>
                              <div className="font-medium">{column.length}</div>
                            </div>
                          )}
                          {column.precision !== undefined && (
                            <div>
                              <div className="text-gray-500 dark:text-gray-400">Precision</div>
                              <div className="font-medium">{column.precision}</div>
                            </div>
                          )}
                          {column.scale !== undefined && (
                            <div>
                              <div className="text-gray-500 dark:text-gray-400">Scale</div>
                              <div className="font-medium">{column.scale}</div>
                            </div>
                          )}
                          {column.constraints && column.constraints.length > 0 && (
                            <div className="col-span-3">
                              <div className="text-gray-500 dark:text-gray-400">Constraints</div>
                              <div className="flex flex-wrap gap-1 mt-1">
                                {column.constraints.map((constraint, idx) => (
                                  <span key={idx} className="px-2 py-1 text-xs bg-gray-100 dark:bg-gray-800 rounded">
                                    {constraint}
                                  </span>
                                ))}
                              </div>
                            </div>
                          )}
                        </div>
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Table Footer with Statistics */}
      <div className="bg-gray-50 dark:bg-gray-800 px-6 py-4 rounded-b-lg border border-gray-200 dark:border-gray-700 border-t-0">
        <div className="flex justify-between items-center">
          <div className="text-sm text-gray-600 dark:text-gray-400">
            Showing {metadata.columns.length} columns
          </div>
          <div className="flex space-x-4">
            <div className="text-center">
              <div className="text-lg font-bold text-gray-900 dark:text-white">
                {metadata.columns.filter(c => c.isKey).length}
              </div>
              <div className="text-xs text-gray-500 dark:text-gray-400">Primary Keys</div>
            </div>
            <div className="text-center">
              <div className="text-lg font-bold text-gray-900 dark:text-white">
                {metadata.columns.filter(c => !c.nullable).length}
              </div>
              <div className="text-xs text-gray-500 dark:text-gray-400">Required</div>
            </div>
            <div className="text-center">
              <div className="text-lg font-bold text-gray-900 dark:text-white">
                {metadata.columns.filter(c => c.defaultValue).length}
              </div>
              <div className="text-xs text-gray-500 dark:text-gray-400">With Default</div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default TableMetadataView;