// src/components/TreeNode.tsx - UPDATED with database connection
// SIMPLIFIED: Shows only name and column list like a database browser

import React, { useState, useCallback, useContext, useEffect } from 'react'; // ADDED useContext, useEffect
import {
  Folder,
  FolderOpen,
  ChevronRight,
  ChevronDown,
  Table,
  Key,
  Type,
  Hash,
  Calendar,
  CheckCircle,
  MoreVertical,
  Trash2,
  AlertTriangle,
  XCircle,
  Database,
  RefreshCw
} from 'lucide-react';

// Import drag-drop utilities
import { setupDragEvent } from '../utils/drag-drop.utils';

// Import Database Context - ADDED
import { DatabaseContext } from '../App'; // Or move context to separate file


// Types
export interface RepositoryNode {
  id: string;
  name: string;
  type: 'category' | 'folder' | 'item' | 'job';
  icon?: React.ReactNode;
  children?: RepositoryNode[];
  metadata?: {
    description?: string;
    lastModified?: string;
    created?: string;
    size?: string;
    type?: string;
    dataType?: string;
    length?: number;
    precision?: number;
    scale?: number;
    isKey?: boolean;
    nullable?: boolean;
    defaultValue?: string;
    tags?: string[];
    version?: string;
    author?: string;
    status?: string;
    postgresTableName?: string;
    connection?: {
      connectionId?: string;
      [key: string]: any;
    };
    columns?: Array<{
      name: string;
      type: string;
      dataType?: string;
      length?: number;
      precision?: number;
      scale?: number;
      nullable?: boolean;
      isKey?: boolean;
      defaultValue?: string;
      description?: string;
      [key: string]: any;
    }>;
    schema?: Array<{
      name: string;
      type: string;
      [key: string]: any;
    }>;
    fields?: Array<{
      name: string;
      type: string;
      [key: string]: any;
    }>;
    [key: string]: any;
  };
  draggable?: boolean;
  droppable?: boolean;
  parentId?: string;
}

export interface TreeNodeProps {
  node: RepositoryNode;
  level: number;
  expandedNodes: Set<string>;
  selectedNode: string | null;
  searchTerm: string;
  dragState?: any;
  onToggle: (nodeId: string) => void;
  onSelect: (node: RepositoryNode) => void;
  onDoubleClick: (node: RepositoryNode) => void;
  onContextMenu: (node: RepositoryNode, position: { x: number; y: number }) => void;
  onDelete?: (node: RepositoryNode) => void;
  onDragStart?: (node: RepositoryNode, event: React.DragEvent) => void;
  onDragOver?: (node: RepositoryNode, event: React.DragEvent) => void;
  onDragLeave?: (event: React.DragEvent) => void;
  onDrop?: (node: RepositoryNode, event: React.DragEvent) => void;
  onDragEnd?: () => void;
  // ADDED: Database interaction props
  onFetchDatabaseData?: (connectionId: string) => Promise<any>;
  onRefreshDatabase?: () => void;
}



const getIconComponent = (node: RepositoryNode, isExpanded: boolean = false) => {
  const iconClass = "h-4 w-4";
  
  // Database browser icons: simplified to table and column icons
  if (node.type === 'folder' || node.type === 'category') {
    return isExpanded 
      ? <FolderOpen className={`${iconClass} text-blue-500`} /> 
      : <Folder className={`${iconClass} text-blue-500`} />;
  }
  
  // Check if this is a table/entity with columns
  const hasColumns = getColumnsFromNode(node).length > 0;
  
  if (hasColumns) {
    return <Table className={`${iconClass} text-green-600`} />;
  }
  
  // Default icon for items
  return <Table className={`${iconClass} text-gray-500`} />;
};



const getDataTypeIcon = (dataType?: string) => {
  if (!dataType) return <Type className="h-3 w-3 text-gray-500 mr-1" />;
  
  const type = dataType.toLowerCase();
  if (type.includes('int') || type.includes('num') || type.includes('float') || type.includes('decimal')) {
    return <Hash className="h-3 w-3 text-blue-500 mr-1" />;
  }
  if (type.includes('char') || type.includes('text') || type.includes('string') || type.includes('varchar')) {
    return <Type className="h-3 w-3 text-green-500 mr-1" />;
  }
  if (type.includes('date') || type.includes('time')) {
    return <Calendar className="h-3 w-3 text-purple-500 mr-1" />;
  }
  if (type.includes('bool')) {
    return <CheckCircle className="h-3 w-3 text-amber-500 mr-1" />;
  }
  return <Type className="h-3 w-3 text-gray-500 mr-1" />;
};

const getColumnsFromNode = (node: RepositoryNode) => {
  if (!node.metadata) return [];
  
  // Check for various column/schema/field structures with proper null checks
  const columns = node.metadata.columns;
  const schema = node.metadata.schema;
  const fields = node.metadata.fields;
  const attributes = node.metadata.attributes;
  const elements = node.metadata.elements;
  
  if (columns && Array.isArray(columns) && columns.length > 0) {
    return columns;
  }
  if (schema && Array.isArray(schema) && schema.length > 0) {
    return schema;
  }
  if (fields && Array.isArray(fields) && fields.length > 0) {
    return fields;
  }
  if (attributes && Array.isArray(attributes) && attributes.length > 0) {
    return attributes;
  }
  if (elements && Array.isArray(elements) && elements.length > 0) {
    return elements;
  }
  
  return [];
};

const isNodeDeletable = (node: RepositoryNode): boolean => {
  if (node.type === 'category') {
    return false;
  }
  
  const systemFolders = [
    'job-designs', 'contexts', 'metadata', 'code', 
    'sql-templates', 'business-models', 'documentation', 'recycle-bin'
  ];
  
  if (systemFolders.includes(node.id)) {
    return false;
  }
  
  if (node.parentId && systemFolders.includes(node.parentId)) {
    return false;
  }
  
  return true;
};

const TreeNode: React.FC<TreeNodeProps> = ({ 
  node, 
  level, 
  expandedNodes, 
  selectedNode, 
  searchTerm,
  dragState,
  onToggle, 
  onSelect, 
  onDoubleClick, 
  onContextMenu,
  onDelete,
  onDragStart,
  onDragOver,
  onDragLeave,
  onDrop,
  onDragEnd,
  // ADDED: Database interaction props
  onFetchDatabaseData,
  onRefreshDatabase
}) => {
  const [isDragging, setIsDragging] = useState(false);
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [deleteConfirmPosition, setDeleteConfirmPosition] = useState({ x: 0, y: 0 });
  const [isDeleting, setIsDeleting] = useState(false);
  const [showDeleteHover, setShowDeleteHover] = useState(false);
  const [isLoadingDatabaseData, setIsLoadingDatabaseData] = useState(false);
  const [databaseData, setDatabaseData] = useState<any[]>([]);
  
  // Use Database Context - ADDED
  const databaseContext = useContext(DatabaseContext);
  
  const isExpanded = expandedNodes.has(node.id);
  const isSelected = selectedNode === node.id;
  const hasChildren = node.children && node.children.length > 0;
  const columns = getColumnsFromNode(node);
  const hasColumns = columns.length > 0;
  const isDeletable = isNodeDeletable(node);
    console.log(`🟢 TreeNode ${node.id} | isExpanded: ${isExpanded} | children: ${node.children?.length ?? 0}`);
  
  // Check if this node represents a database table - ADDED
  const isDatabaseTable = node.metadata?.postgresTableName || 
                         (node.metadata?.type === 'table') ||
                         (node.type === 'item' && node.metadata?.columns);
  
  // Fetch database data when expanded - ADDED
  useEffect(() => {
    const fetchTableData = async () => {
      if (isExpanded && isDatabaseTable && databaseContext.isConnected && databaseContext.apiService) {
        setIsLoadingDatabaseData(true);
        try {
          const tableName = node.metadata?.postgresTableName || node.name;
          const connectionId = node.metadata?.connection?.connectionId;
          
          if (connectionId && tableName) {
            // Use the database API to fetch table data
            const result = await databaseContext.apiService.executeQuery(
              connectionId,
              `SELECT * FROM ${tableName} LIMIT 50`
            );
            
            if (result.success && result.result?.rows) {
              setDatabaseData(result.result.rows);
            }
          }
        } catch (error) {
          console.error('Failed to fetch database data:', error);
        } finally {
          setIsLoadingDatabaseData(false);
        }
      }
    };
    
    fetchTableData();
  }, [isExpanded, isDatabaseTable, databaseContext, node]);

  const handleDragStart = useCallback((e: React.DragEvent) => {
    e.stopPropagation();
    setIsDragging(true);
    
    // Select the node being dragged
    onSelect(node);
    
    // Use the utility function to set up drag event
    if (onDragStart) {
      setupDragEvent(node, e, onDragStart);
    }
  }, [node, onDragStart, onSelect]);

  const handleDragEnd = useCallback((e: React.DragEvent) => {
    e.stopPropagation();
    setIsDragging(false);
    if (onDragEnd) {
      onDragEnd();
    }
  }, [onDragEnd]);

  const handleDoubleClick = (e: React.MouseEvent) => {
    e.stopPropagation();
    onDoubleClick(node);
  };

  const handleDeleteClick = (e: React.MouseEvent) => {
    e.stopPropagation();
    e.preventDefault();
    
    if (!isDeletable || !onDelete) return;
    
    const rect = e.currentTarget.getBoundingClientRect();
    setDeleteConfirmPosition({
      x: rect.left + rect.width / 2,
      y: rect.top
    });
    
    setShowDeleteConfirm(true);
  };

  const handleConfirmDelete = () => {
    if (!onDelete) return;
    
    setIsDeleting(true);
    
    setTimeout(() => {
      onDelete(node);
      setShowDeleteConfirm(false);
      setIsDeleting(false);
    }, 500);
  };

  const handleCancelDelete = () => {
    setShowDeleteConfirm(false);
  };

  const handleContextMenu = (e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    onContextMenu(node, { x: e.clientX, y: e.clientY });
  };

  const handleClick = (e: React.MouseEvent) => {
    e.stopPropagation();
    if (hasChildren || hasColumns) {
      onToggle(node.id);
    }
    onSelect(node);
  };

  const handleToggle = (e: React.MouseEvent) => {
    e.stopPropagation();
    onToggle(node.id);
  };

  // ADDED: Handle database refresh
  const handleRefreshDatabase = useCallback(async () => {
    if (onRefreshDatabase) {
      onRefreshDatabase();
    }
    
    // Also refresh this specific node if it's a database table
    if (isDatabaseTable && databaseContext.apiService) {
      setIsLoadingDatabaseData(true);
      try {
        const tableName = node.metadata?.postgresTableName || node.name;
        const connectionId = node.metadata?.connection?.connectionId;
        
        if (connectionId && tableName) {
          const result = await databaseContext.apiService.executeQuery(
            connectionId,
            `SELECT * FROM ${tableName} LIMIT 50`
          );
          
          if (result.success && result.result?.rows) {
            setDatabaseData(result.result.rows);
          }
        }
      } catch (error) {
        console.error('Failed to refresh database data:', error);
      } finally {
        setIsLoadingDatabaseData(false);
      }
    }
  }, [onRefreshDatabase, isDatabaseTable, databaseContext, node]);

  const shouldShowExpandButton = hasChildren || hasColumns;

  const getDragStyles = () => {
    const styles = [];
    
    if (isDragging) {
      styles.push('bg-blue-50 dark:bg-blue-900/20 border-2 border-dashed border-blue-400');
    }
    
    if (isSelected) {
      styles.push('bg-blue-100 dark:bg-blue-900 text-blue-900 dark:text-blue-100');
    } else {
      styles.push('hover:bg-gray-100 dark:hover:bg-gray-800 text-gray-700 dark:text-gray-300');
    }
    
    if (isDeleting) {
      styles.push('bg-red-50 dark:bg-red-900/20 opacity-75');
    }
    
    return styles.join(' ');
  };

  const getCursorStyle = () => {
    if (node.draggable !== false) {
      return 'cursor-grab active:cursor-grabbing';
    }
    return 'cursor-pointer';
  };

  const getTooltipText = () => {
    if (node.type === 'job') {
      return `Job: ${node.name}`;
    }
    if (!isDeletable) {
      return `${node.metadata?.description || `Type: ${node.type}`} (System item)`;
    }
    if (isDatabaseTable) {
      return `${node.metadata?.description || `Table: ${node.name}`} (Connected to PostgreSQL)`;
    }
    return node.metadata?.description || `Type: ${node.type}`;
  };

  const getChildCountText = () => {
    if (hasChildren && node.children) {
      return `${node.children.length} item${node.children.length === 1 ? '' : 's'}`;
    }
    if (hasColumns) {
      return `${columns.length} column${columns.length === 1 ? '' : 's'}`;
    }
    return '';
  };

  return (
    <>
      <div className="relative" data-node-id={node.id}>
        {/* Main node container */}
        <div
          className={`flex flex-col px-2 py-1.5 text-sm rounded-md group transition-all duration-200 relative ${getCursorStyle()} ${getDragStyles()}`}
          style={{ 
            paddingLeft: `${level * 16 + 8}px`,
            minWidth: 'max-content',
          }}
          onClick={handleClick}
          onDoubleClick={handleDoubleClick}
          onContextMenu={handleContextMenu}
          title={getTooltipText()}
          draggable={node.draggable !== false}
          onDragStart={handleDragStart}
          onDragEnd={handleDragEnd}
          onDragOver={(e) => {
            e.preventDefault();
            e.stopPropagation();
            if (onDragOver) {
              onDragOver(node, e);
            }
          }}
          onDragLeave={(e) => {
            e.preventDefault();
            e.stopPropagation();
            if (onDragLeave) {
              onDragLeave(e);
            }
          }}
          onDrop={(e) => {
            e.preventDefault();
            e.stopPropagation();
            if (onDrop) {
              onDrop(node, e);
            }
          }}
        >
          {/* First row: Node info */}
          <div className="flex items-center w-full">
            {/* Expand/collapse button */}
            {shouldShowExpandButton ? (
              <button
                className="w-4 h-4 mr-1 flex items-center justify-center hover:bg-gray-200 dark:hover:bg-gray-700 rounded transition-colors duration-200 touch-none flex-shrink-0"
                onClick={handleToggle}
                title={isExpanded ? "Collapse" : "Expand"}
              >
                {isExpanded ? (
                  <ChevronDown className="h-3 w-3 text-gray-500 dark:text-gray-400" />
                ) : (
                  <ChevronRight className="h-3 w-3 text-gray-500 dark:text-gray-400" />
                )}
              </button>
            ) : (
              <div className="w-4 h-4 mr-1 flex-shrink-0" />
            )}
            
            {/* Node icon and name */}
            <div className="flex items-center flex-1 min-w-0 overflow-hidden">
              <div className="flex-shrink-0 w-4 h-4 mr-2 flex items-center justify-center">
                {getIconComponent(node, isExpanded)}
              </div>
              <span 
                className="font-medium whitespace-nowrap overflow-visible text-ellipsis" 
                style={{ 
                  minWidth: 'max-content',
                  maxWidth: 'none'
                }}
              >
                {node.name}
              </span>
              
              {/* Database connection indicator - ADDED */}
              {isDatabaseTable && databaseContext.isConnected && (
                <div className="ml-2 flex items-center">
                  <Database className="h-3 w-3 text-green-500" title="Connected to PostgreSQL" />
                </div>
              )}
            </div>

            {/* Child/column count badge */}
            {getChildCountText() && (
              <span className="ml-2 text-xs text-gray-400 dark:text-gray-500 group-hover:text-gray-600 dark:group-hover:text-gray-300 flex-shrink-0">
                {getChildCountText()}
              </span>
            )}

            {/* ADDED: Database refresh button */}
            {isDatabaseTable && databaseContext.isConnected && (
              <button
                className={`p-1 rounded transition-all duration-200 touch-none flex-shrink-0 ml-1 ${
                  isLoadingDatabaseData 
                    ? 'opacity-100 bg-blue-100 dark:bg-blue-900/20 text-blue-600 dark:text-blue-400' 
                    : 'opacity-0 group-hover:opacity-100 hover:bg-blue-100 dark:hover:bg-blue-900/20 text-gray-500 dark:text-gray-400 hover:text-blue-600 dark:hover:text-blue-400'
                }`}
                onClick={handleRefreshDatabase}
                disabled={isLoadingDatabaseData}
                title="Refresh database data"
              >
                {isLoadingDatabaseData ? (
                  <div className="h-3 w-3 animate-spin rounded-full border-2 border-blue-600 border-t-transparent"></div>
                ) : (
                  <RefreshCw className="h-3 w-3" />
                )}
              </button>
            )}

            {/* Context menu button */}
            <button
              className="opacity-0 group-hover:opacity-100 p-1 hover:bg-gray-200 dark:hover:bg-gray-700 rounded transition-all duration-200 touch-none flex-shrink-0 ml-1"
              onClick={(e) => {
                e.stopPropagation();
                onContextMenu(node, { x: e.clientX, y: e.clientY });
              }}
              title="More options"
            >
              <MoreVertical className="h-3 w-3 text-gray-500 dark:text-gray-400" />
            </button>

            {/* Delete button */}
            {isDeletable && onDelete && (
              <button
                className={`p-1 rounded transition-all duration-200 touch-none flex-shrink-0 ml-1 ${
                  showDeleteHover 
                    ? 'opacity-100 bg-red-100 dark:bg-red-900/20 text-red-600 dark:text-red-400' 
                    : 'opacity-0 group-hover:opacity-100 hover:bg-red-100 dark:hover:bg-red-900/20 text-gray-500 dark:text-gray-400 hover:text-red-600 dark:hover:text-red-400'
                }`}
                onClick={handleDeleteClick}
                onMouseEnter={() => setShowDeleteHover(true)}
                onMouseLeave={() => setShowDeleteHover(false)}
                title="Delete node"
              >
                <Trash2 className="h-3 w-3" />
              </button>
            )}

            {/* Non-deletable indicator */}
            {!isDeletable && (
              <div className="ml-1 opacity-0 group-hover:opacity-100 p-1 rounded transition-all duration-200 flex-shrink-0">
                <XCircle className="h-3 w-3 text-gray-300 dark:text-gray-600" />
              </div>
            )}
          </div>

          {/* Column list when expanded - Simple database-style display */}
          {isExpanded && hasColumns && (
            <div className="ml-6 pl-2 border-l border-gray-200 dark:border-gray-700 mt-1">
              <div className="space-y-0.5">
                {columns.map((column, index) => (
                  <div 
                    key={`${column.name}-${index}`}
                    className="py-1 px-2 hover:bg-gray-50 dark:hover:bg-gray-800/20 rounded flex items-center text-xs"
                  >
                    <div className="flex items-center flex-1">
                      {column.isKey && (
                        <Key className="h-2.5 w-2.5 text-red-500 mr-1.5" />
                      )}
                      <span className="font-medium text-gray-800 dark:text-gray-200 mr-2">
                        {column.name}
                      </span>
                      <span className="text-gray-400 mr-1">:</span>
                      <div className="flex items-center">
                        {getDataTypeIcon(column.dataType || column.type)}
                        <span className="text-blue-600 dark:text-blue-400 font-mono">
                          {column.dataType || column.type || 'unknown'}
                        </span>
                        {column.length && (
                          <span className="ml-1 text-gray-500 dark:text-gray-400">
                            ({column.length})
                          </span>
                        )}
                      </div>
                    </div>
                    <div className="flex items-center space-x-1 text-xs">
                      {column.nullable === false && (
                        <span className="text-red-500 text-[10px]">NOT NULL</span>
                      )}
                      {column.defaultValue && (
                        <span className="text-green-500 text-[10px]">
                          DEFAULT
                        </span>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}
          
          {/* ADDED: Database data preview for tables */}
          {isExpanded && isDatabaseTable && databaseContext.isConnected && (
            <div className="ml-6 pl-2 border-l border-blue-200 dark:border-blue-800 mt-1">
              <div className="flex items-center justify-between mb-1">
                <div className="flex items-center">
                  <Database className="h-3 w-3 text-blue-500 mr-1" />
                  <span className="text-xs font-medium text-blue-600 dark:text-blue-400">
                    PostgreSQL Data Preview
                  </span>
                </div>
                <span className="text-xs text-gray-400">
                  {isLoadingDatabaseData ? 'Loading...' : `${databaseData.length} rows`}
                </span>
              </div>
              
              {isLoadingDatabaseData ? (
                <div className="py-2 text-center">
                  <div className="inline-block h-3 w-3 animate-spin rounded-full border-2 border-blue-600 border-t-transparent"></div>
                  <span className="text-xs text-gray-500 ml-2">Loading data from PostgreSQL...</span>
                </div>
              ) : databaseData.length > 0 ? (
                <div className="overflow-x-auto max-h-40 overflow-y-auto">
                  <table className="min-w-full text-xs border border-gray-200 dark:border-gray-700">
                    <thead className="bg-gray-50 dark:bg-gray-800">
                      <tr>
                        {Object.keys(databaseData[0] || {}).slice(0, 5).map((key) => (
                          <th key={key} className="px-2 py-1 text-left font-medium text-gray-700 dark:text-gray-300 border-b">
                            {key}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {databaseData.slice(0, 3).map((row, rowIndex) => (
                        <tr key={rowIndex} className="hover:bg-gray-50 dark:hover:bg-gray-800/50">
                          {Object.keys(row).slice(0, 5).map((key) => (
                            <td key={key} className="px-2 py-1 border-b border-gray-100 dark:border-gray-800">
                              <div className="truncate max-w-[120px]" title={String(row[key])}>
                                {String(row[key])}
                              </div>
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {databaseData.length > 3 && (
                    <div className="text-center py-1 text-xs text-gray-500">
                      Showing 3 of {databaseData.length} rows
                    </div>
                  )}
                </div>
              ) : (
                <div className="py-2 text-center text-xs text-gray-500">
                  No data found in table
                </div>
              )}
            </div>
          )}
        </div>

        {/* Delete Confirmation Dialog */}
        {showDeleteConfirm && (
          <div 
            className="fixed z-[1000] bg-white dark:bg-gray-800 rounded-lg shadow-xl border border-red-200 dark:border-red-800 p-4 min-w-[280px] max-w-[320px]"
            style={{
              left: deleteConfirmPosition.x,
              top: deleteConfirmPosition.y,
              transform: 'translate(-50%, -100%)'
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-start space-x-3 mb-3">
              <div className="flex-shrink-0">
                <AlertTriangle className="h-6 w-6 text-red-500" />
              </div>
              <div className="flex-1">
                <h3 className="text-sm font-semibold text-gray-900 dark:text-white mb-1">
                  Delete "{node.name}"?
                </h3>
                <p className="text-xs text-gray-600 dark:text-gray-300 mb-2">
                  This action cannot be undone.
                </p>
                
                {hasColumns && (
                  <div className="bg-red-50 dark:bg-red-900/20 p-2 rounded mb-3">
                    <p className="text-xs text-red-700 dark:text-red-300 font-medium">
                      ⚠️ This will delete {columns.length} column{columns.length === 1 ? '' : 's'}.
                    </p>
                  </div>
                )}
              </div>
            </div>
            
            <div className="flex justify-end space-x-2">
              <button
                onClick={handleCancelDelete}
                className="px-3 py-1.5 text-xs border border-gray-300 dark:border-gray-600 rounded hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={handleConfirmDelete}
                className="px-3 py-1.5 text-xs bg-red-600 text-white rounded hover:bg-red-700 transition-colors flex items-center"
              >
                <Trash2 className="h-3 w-3 mr-1" />
                Delete
              </button>
            </div>
          </div>
        )}

        {/* Children */}
        {isExpanded && hasChildren && node.children && (
          <div className="relative" style={{ minWidth: 'max-content' }}>
            <div 
              className="absolute left-4 top-0 bottom-0 w-px bg-gray-200 dark:bg-gray-700 ml-4"
              style={{ marginLeft: `${level * 16 + 12}px` }}
            />
            
            {node.children.map(child => (
              <TreeNode
                key={child.id}
                node={child}
                level={level + 1}
                expandedNodes={expandedNodes}
                selectedNode={selectedNode}
                searchTerm={searchTerm}
                dragState={dragState}
                onToggle={onToggle}
                onSelect={onSelect}
                onDoubleClick={onDoubleClick}
                onContextMenu={onContextMenu}
                onDelete={onDelete}
                onDragStart={onDragStart}
                onDragOver={onDragOver}
                onDragLeave={onDragLeave}
                onDrop={onDrop}
                onDragEnd={onDragEnd}
                // Pass database props to children
                onFetchDatabaseData={onFetchDatabaseData}
                onRefreshDatabase={onRefreshDatabase}
              />
            ))}
          </div>
        )}

        {/* Empty state for folders */}
        {isExpanded && hasChildren && (!node.children || node.children.length === 0) && (
          <div 
            className="text-xs text-gray-400 dark:text-gray-500 italic py-1 whitespace-nowrap"
            style={{ paddingLeft: `${(level + 1) * 16 + 24}px` }}
          >
            {searchTerm ? 'No matching items' : 'Empty folder'}
          </div>
        )}
      </div>
    </>
  );
};

export default TreeNode;