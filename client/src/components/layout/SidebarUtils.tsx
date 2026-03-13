// src/components/layout/SidebarUtils.tsx

import { RepositoryNode } from '../../types/types';
import { DatabaseConfig } from '../../services/database-api.service';
import React from 'react';

// ==================== DATA TYPE UTILITIES ====================

export const isProcessingComponentNode = (node: RepositoryNode): boolean => {
  if (!node.metadata) return false;
  
  const metadata = node.metadata as any;
  const nodeType = metadata.type || '';
  const nodeName = node.name || '';
  
  const processingTypes = [
    'regex-pattern', 'ldif', 'transform', 'processing',
    'tJoin', 'tDenormalize', 'tNormalize', 'tAggregateRow', 'tSortRow', 'tFilterRow'
  ];
  
  const isProcessingType = processingTypes.some(type => 
    nodeType.toLowerCase().includes(type.toLowerCase()) ||
    nodeName.toLowerCase().includes(type.toLowerCase())
  );
  
  return isProcessingType || 
         (metadata.componentType && metadata.componentType === 'processing') ||
         (metadata.nodeType && metadata.nodeType === 'transform');
};

export const inferDataTypeFromValue = (value: string): string => {
  if (!value) return 'String';
  
  if (['true', 'false', 'yes', 'no', '1', '0'].includes(value.toLowerCase())) {
    return 'Boolean';
  }
  
  if (!isNaN(Number(value)) && value.trim() !== '') {
    return Number.isInteger(Number(value)) ? 'Integer' : 'Decimal';
  }
  
  const date = new Date(value);
  if (!isNaN(date.getTime())) {
    return 'Date';
  }
  
  return 'String';
};

export const sanitizeIdentifier = (name: string): string => {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, '_')
    .replace(/^[0-9]/, 't_$&')
    .replace(/_+/g, '_');
};

// ==================== METADATA TYPE TO COMPONENT KEY MAPPING ====================

// In SidebarUtils.ts or similar utility file
export const mapMetadataTypeToComponentKey = (metadataType: string): string => {
  const type = metadataType.toLowerCase();
  
  // Map repository metadata types to Talend component keys
  const mapping: Record<string, string> = {
    // Excel files
    'excel': 'tFileExcel',
    'xlsx': 'tFileExcel',
    'xls': 'tFileExcel',
    
    // XML files
    'xml': 'tFileXML',
    
    // Delimited files (CSV, TSV)
    'delimited': 'tFileDelimited',
    'csv': 'tFileDelimited',
    'tsv': 'tFileDelimited',
    
    // Positional files
    'positional': 'tFilePositional',
    'fixedwidth': 'tFilePositional',
    
    // Schema files
    'schema': 'tFileSchema',
    
    // JSON/AVRO/Parquet
    'json': 'tFileJsonAvroParquet',
    'avro': 'tFileJsonAvroParquet',
    'parquet': 'tFileJsonAvroParquet',
    
    // Regex files
    'regex': 'tFileRegex',
    
    // LDIF files
    'ldif': 'tFileLDIF',
    
    // Web services
    'webservice': 'tWebService',
    'api': 'tWebService',
    
    // Databases
    'database': 'tMySQL', // Default database type
    'postgresql': 'tPostgreSQL',
    'mysql': 'tMySQL',
    'oracle': 'tOracle',
    'sqlserver': 'tSQLServer',
    'db2': 'tDB2',
    
    // Directories
    'directory': 'tDirectory',
    'folder': 'tDirectory',
  };
  
  return mapping[type] || 'tFileDelimited'; // Default to a data source component
};
// ==================== DATA SOURCE COMPONENT UTILITIES ====================

// List of data source components that require role selection
export const DATA_SOURCE_COMPONENTS: string[] = [
  'tFileDelimited', 'tFilePositional', 'tFileXML', 'tFileExcel',
  'tFileSchema', 'tFileRegex', 'tFileLDIF', 'tFileJsonAvroParquet',
  'tDirectory', 'tLDAP', 'tMySQL', 'tOracle', 'tSQLServer',
  'tDB2', 'tSAPHANA', 'tSybase', 'tNetezza', 'tInformix', 'tFirebird'
];

export const isDataSourceComponent = (componentKey: string): boolean => {
  return DATA_SOURCE_COMPONENTS.includes(componentKey);
};

// ==================== METADATA UTILITIES ====================

export const determineTechnologyFromMetadata = (
  metadata: Record<string, any> | null | undefined
): string => {
  if (!metadata) return 'unknown';
  const metaType = metadata.type || '';
  if (metaType.includes('excel')) return 'excel';
  if (metaType.includes('delimited')) return 'delimited';
  if (metaType.includes('database')) return 'database';
  if (metaType.includes('xml')) return 'xml';
  if (metaType.includes('json')) return 'json';
  if (metaType.includes('avro')) return 'avro';
  if (metaType.includes('parquet')) return 'parquet';
  if (metaType.includes('web-service')) return 'webservice';
  if (metaType.includes('ldif')) return 'ldif';
  if (metaType.includes('regex')) return 'regex';
  if (metaType.includes('schema')) return 'schema';
  return 'unknown';
};

export const determineCategoryFromMetadata = (
  metadata: Record<string, any> | null | undefined
): 'input' | 'output' | 'processing' => {
  if (!metadata) return 'input';
  if (metadata.purpose?.includes('output')) return 'output';
  if (metadata.purpose?.includes('process')) return 'processing';
  return 'input';
};

// ==================== TREE UTILITIES ====================

export const createTreeFilter = (searchTerm: string) => {
  const filterNode = (node: RepositoryNode): RepositoryNode | null => {
    const matchesSearch = !searchTerm || 
      node.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
      (node.metadata?.description && 
       node.metadata.description.toLowerCase().includes(searchTerm.toLowerCase()));

    if (!matchesSearch && (!node.children || node.children.length === 0)) {
      return null;
    }

    const filteredChildren = node.children
      ?.map(filterNode)
      .filter((child): child is RepositoryNode => child !== null) || [];

    if (!matchesSearch && filteredChildren.length === 0) {
      return null;
    }

    return {
      ...node,
      children: filteredChildren.length > 0 ? filteredChildren : node.children
    };
  };

  return filterNode;
};

export const countNodesInTree = (node: RepositoryNode): number => {
  let count = 1;
  if (node.children) {
    node.children.forEach(child => {
      count += countNodesInTree(child);
    });
  }
  return count;
};

export const flattenTree = (nodes: RepositoryNode[]): RepositoryNode[] => {
  const result: RepositoryNode[] = [];
  
  const flatten = (node: RepositoryNode) => {
    result.push(node);
    if (node.children) {
      node.children.forEach(flatten);
    }
  };
  
  nodes.forEach(flatten);
  return result;
};

// ==================== DATABASE UTILITIES ====================

export const getDefaultUser = (): string => {
  try {
    const savedUser = localStorage.getItem('last_db_user');
    if (savedUser) return savedUser;
  } catch (e) {}
  
  if (typeof process !== 'undefined' && process.env) {
    return process.env.REACT_APP_DB_USER || 
           process.env.USER || 
           process.env.USERNAME || 
           'postgres';
  }
  
  return 'postgres';
};

export const validateDatabaseConfig = (config: DatabaseConfig): { valid: boolean; errors: string[] } => {
  const errors: string[] = [];
  
  if (!config.host || config.host.trim() === '') {
    errors.push('Host is required');
  }
  
  if (!config.port || config.port.trim() === '') {
    errors.push('Port is required');
  } else if (!/^\d+$/.test(config.port)) {
    errors.push('Port must be a number');
  }
  
  if (!config.dbname || config.dbname.trim() === '') {
    errors.push('Database name is required');
  }
  
  if (!config.user || config.user.trim() === '') {
    errors.push('Username is required');
  }
  
  return {
    valid: errors.length === 0,
    errors
  };
};

// ==================== DELETE UTILITIES ====================

export const calculateDeleteStats = (node: RepositoryNode): {
  nodesToDelete: number;
  hasChildren: boolean;
  nodeName: string;
  metadataCount: number;
  folderCount: number;
  itemCount: number;
  hasForeignTables: boolean;
  hasDatabaseConnections: boolean;
} => {
  let nodeCount = 1;
  let metadataCount = 0;
  let folderCount = node.type === 'folder' ? 1 : 0;
  let itemCount = node.type === 'item' ? 1 : 0;
  let hasChildren = false;
  let hasForeignTables = false;
  let hasDatabaseConnections = false;
  
  const countChildren = (n: RepositoryNode): number => {
    let count = 0;
    if (n.children && n.children.length > 0) {
      hasChildren = true;
      n.children.forEach(child => {
        count += 1;
        
        if (child.type === 'folder') folderCount++;
        if (child.type === 'item') itemCount++;
        
        if (child.metadata) {
          metadataCount++;
          if (child.metadata.postgresTableName) {
            hasForeignTables = true;
          }
          if (child.metadata.type === 'database' && child.metadata.connection) {
            hasDatabaseConnections = true;
          }
        }
        
        if (child.children) {
          count += countChildren(child);
        }
      });
    }
    return count;
  };
  
  nodeCount += countChildren(node);
  
  if (node.metadata) {
    metadataCount++;
    if (node.metadata.postgresTableName) {
      hasForeignTables = true;
    }
    if (node.metadata.type === 'database' && node.metadata.connection) {
      hasDatabaseConnections = true;
    }
  }
  
  return {
    nodesToDelete: nodeCount,
    hasChildren,
    nodeName: node.name,
    metadataCount,
    folderCount,
    itemCount,
    hasForeignTables,
    hasDatabaseConnections
  };
};

// ==================== REACT FLOW UTILITIES ====================

export const createReactFlowNodeData = (
  node: RepositoryNode,
  position: { x: number; y: number }
) => {
  const technology = determineTechnologyFromMetadata(node.metadata || null);
  const category = determineCategoryFromMetadata(node.metadata || null);
  
  // Map metadata type to component key
  const metadataType = node.metadata?.type || 'unknown';
  const componentKey = mapMetadataTypeToComponentKey(metadataType);
  
  return {
    id: `node-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
    name: node.name,
    type: node.type,
    position,
    metadata: {
      ...(node.metadata || {}),
      repositoryNodeId: node.id,
      source: 'sidebar-direct',
      technology,
      category,
      componentKey: componentKey,
      originalMetadataType: metadataType
    },
    connectionPorts: [],
    componentCategory: category,
    technology,
    size: { width: 146, height: 93 }
  };
};

export const createDragDataForNode = (node: RepositoryNode) => ({
  source: 'sidebar',
  type: 'repository-node',
  metadata: node.metadata ? JSON.parse(JSON.stringify(node.metadata)) : {},
  nodeData: {
    id: node.id,
    name: node.name,
    type: node.type,
    metadata: node.metadata
  },
  componentData: {
    name: node.name,
    type: node.type,
    metadata: node.metadata
  },
  requiresPopup: true,
  name: node.name
});

// ==================== WIZARD UTILITIES ====================

interface ColumnDefinition {
  name: string;
  type: string;
  length?: number;
}

export const createColumnNodes = (
  metadataId: string,
  columns: ColumnDefinition[]
): RepositoryNode[] => {
  return columns.map((column, index) => ({
    id: `${metadataId}-column-${index}`,
    name: column.name,
    type: 'item',
    icon: React.createElement('div', { className: 'h-4 w-4' }),
    metadata: {
      type: 'column',
      dataType: column.type,
      length: column.length,
      description: `Column: ${column.name} (${column.type}${column.length ? `, length: ${column.length}` : ''})`
    },
    draggable: true,
    droppable: false
  }));
};

// ==================== DEBUG UTILITIES ====================

export const showPostgresInstructions = (
  isPostgresConnected: boolean,
  connectionConfig: DatabaseConfig,
  hasAttemptedAutoConnect: boolean
): string => {
  return `📋 PostgreSQL Connection Instructions:

1. Ensure PostgreSQL is running on your machine:
   - Windows: Check Services for "PostgreSQL"
   - macOS: "brew services list | grep postgres"
   - Linux: "sudo systemctl status postgresql"

2. Default connection settings:
   - Host: ${connectionConfig.host}
   - Port: ${connectionConfig.port}
   - Database: ${connectionConfig.dbname}
   - Username: ${connectionConfig.user} (or your system username)
   - Password: (leave empty for local connection)

3. If connection fails:
   - Check if PostgreSQL is running
   - Verify your credentials
   - Ensure PostgreSQL allows connections (check pg_hba.conf)

Current Status: ${isPostgresConnected ? '✅ Connected' : '❌ Not Connected'}
Host: ${connectionConfig.host}:${connectionConfig.port}
Auto-connect attempted: ${hasAttemptedAutoConnect ? 'Yes' : 'No'}
    `;
};

interface CreateForeignTableFunction {
  (
    connectionId: string,
    tableName: string,
    columns: Array<{ name: string; type: string; nullable: boolean; length?: number }>,
    type: string,
    filePath: string,
    options: Record<string, any>
  ): Promise<{ success: boolean; error?: string }>;
}

export const testForeignTableCreation = async (
  currentConnectionId: string | null,
  createForeignTable: CreateForeignTableFunction,
): Promise<{ success: boolean; error?: string }> => {
  if (!currentConnectionId) {
    return { success: false, error: 'No connection ID available' };
  }
  
  const testFilePath = '/path/to/test.csv';
  
  try {
    const result = await createForeignTable(
      currentConnectionId,
      'test_foreign_table',
      [
        { name: 'id', type: 'integer', nullable: false },
        { name: 'name', type: 'varchar', length: 100, nullable: true },
        { name: 'created_at', type: 'timestamp', nullable: true }
      ],
      'delimited',
      testFilePath,
      {
        delimiter: ',',
        header: 'true'
      }
    );
    
    return { success: result.success, error: result.error };
  } catch (error: any) {
    return { success: false, error: error instanceof Error ? error.message : 'Unknown error' };
  }
};

// ==================== PERSISTENCE UTILITIES ====================

interface PersistenceService {
  loadExpandedNodes: () => Set<string>;
  loadSelectedNode: () => string | null;
  loadRepositoryData: () => RepositoryNode[] | null;
}

export const loadInitialState = (
  persistenceService: PersistenceService,
  currentJob?: any,
  getDefaultRepositoryData?: (job?: any) => RepositoryNode[]
) => {
  const savedExpandedNodes = persistenceService.loadExpandedNodes();
  const savedSelectedNode = persistenceService.loadSelectedNode();
  const savedData = persistenceService.loadRepositoryData();
  
  return {
    expandedNodes: savedExpandedNodes.size > 0 ? savedExpandedNodes : new Set<string>([
      'job-designs', 'contexts', 'metadata', 'code', 'sql-templates',
      'business-models', 'documentation', 'recycle-bin'
    ]),
    selectedNode: savedSelectedNode,
    repositoryData: savedData || (getDefaultRepositoryData ? getDefaultRepositoryData(currentJob) : [])
  };
};

// ==================== METADATA TREE ADDITION UTILITIES ====================

export const addMetadataToTree = (
  metadata: Record<string, any>,
  type: string,
  folderId: string,
  setRepositoryData: (data: RepositoryNode[] | ((prev: RepositoryNode[]) => RepositoryNode[])) => void,
  setExpandedNodes: (nodes: Set<string> | ((prev: Set<string>) => Set<string>)) => void,
  setSelectedNode: (nodeId: string | null) => void,
  notifyCanvasOfNodeUpdate?: (nodeId: string, metadata: Record<string, any>) => void,
  generateChildren?: (metadataId: string, metadata: Record<string, any>) => RepositoryNode[]
) => {
  const metadataId = `${type}-${metadata.name.toLowerCase().replace(/\s+/g, '-')}-${Date.now()}`;
  
  const children = generateChildren ? generateChildren(metadataId, metadata) : [];
  
  // Create the metadata object that matches RepositoryNodeMetadata type
  const nodeMetadata: Record<string, any> = {
    ...metadata,
    type,
    lastModified: new Date().toISOString(),
  };
  
  const newMetadataNode: RepositoryNode = {
    id: metadataId,
    name: metadata.name,
    type: 'item',
    icon: React.createElement('div', { className: 'h-4 w-4' }),
    children,
    metadata: nodeMetadata,
    draggable: true,
    droppable: true
  };
  
  setRepositoryData((prevData: RepositoryNode[]) => {
    return prevData.map((category: RepositoryNode) => {
      if (category.id === 'metadata' && category.children) {
        const updatedChildren = category.children.map((folder: RepositoryNode) => {
          if (folder.id === folderId) {
            return {
              ...folder,
              children: [...(folder.children || []), newMetadataNode]
            };
          }
          return folder;
        });
        return { ...category, children: updatedChildren };
      }
      return category;
    });
  });
  
  setExpandedNodes((prev: Set<string>) => new Set([...prev, folderId, metadataId]));
  setSelectedNode(metadataId);
  
  // Type assertion to fix the error
  notifyCanvasOfNodeUpdate?.(metadataId, nodeMetadata as Record<string, any>);
  
  return metadataId;
};

// ==================== TYPE SAFETY UTILITIES ====================

export const toSafeMetadata = (
  metadata: any | undefined
): Record<string, any> | null => {
  return metadata || null;
};

// ==================== NODE VALIDATION UTILITIES ====================

export const validateNodeMetadata = (node: RepositoryNode): boolean => {
  if (!node.metadata) return false;
  
  // Basic required fields check
  if (!node.metadata.type) return false;
  if (!node.metadata.name) return false;
  
  return true;
};

export const getNodeTechnology = (node: RepositoryNode): string => {
  return determineTechnologyFromMetadata(node.metadata || null);
};

export const getNodeCategory = (node: RepositoryNode): 'input' | 'output' | 'processing' => {
  return determineCategoryFromMetadata(node.metadata || null);
};

// ==================== METADATA TYPE GUARDS ====================

export const isMetadataDefined = (
  metadata: Record<string, any> | null | undefined
): metadata is Record<string, any> => {
  return metadata !== null && metadata !== undefined;
};

export const ensureMetadata = (
  metadata: Record<string, any> | null | undefined,
  defaultValue: Record<string, any> = {}
): Record<string, any> => {
  return metadata || defaultValue;
};

// ==================== NODE CREATION UTILITIES ====================

export const createRepositoryNode = (
  id: string,
  name: string,
  type: 'item' | 'folder',
  metadata?: Record<string, any>,
  children?: RepositoryNode[]
): RepositoryNode => {
  const nodeMetadata = metadata ? { ...metadata } : undefined;
  
  return {
    id,
    name,
    type,
    icon: React.createElement('div', { className: 'h-4 w-4' }),
    children,
    metadata: nodeMetadata,
    draggable: type === 'item',
    droppable: type === 'folder'
  };
};

// ==================== METADATA TRANSFORMATION UTILITIES ====================

export const transformToNodeMetadata = (
  metadata: Record<string, any>
): Record<string, any> => {
  // Add default properties if missing
  const transformed = { ...metadata };
  
  if (!transformed.created) {
    transformed.created = new Date().toISOString();
  }
  
  if (!transformed.lastModified) {
    transformed.lastModified = new Date().toISOString();
  }
  
  return transformed;
};

// ==================== DRAG DATA UTILITIES ====================

export const createReactFlowDragData = (node: RepositoryNode): any => {
  // Map metadata type to component key
  const metadataType = String(node.metadata?.type || node.type || 'unknown');
  const componentKey = mapMetadataTypeToComponentKey(metadataType);
  const requiresRoleSelection = isDataSourceComponent(componentKey);
  
  let componentType: 'input' | 'output' | 'processing' = 'processing';
  
  // Determine component type based on metadata
  const nodeTypeLower = metadataType.toLowerCase();
  const nodeNameLower = String(node.name || '').toLowerCase();
  
  if (nodeTypeLower.includes('input') || 
      nodeTypeLower.includes('source') ||
      nodeNameLower.includes('input')) {
    componentType = 'input';
  } else if (nodeTypeLower.includes('output') || 
             nodeTypeLower.includes('sink') ||
             nodeNameLower.includes('output')) {
    componentType = 'output';
  }
  
  const enhancedMetadata = {
    ...node.metadata,
    source: 'sidebar',
    repositoryNodeId: node.id,
    isRepositoryNode: true,
    description: node.metadata?.description || `Component: ${node.name}`,
    componentCategory: componentType,
    originalNodeName: node.name,
    originalNodeType: node.type,
    componentKey: componentKey,
    originalMetadataType: metadataType,
    requiresRoleSelection: requiresRoleSelection
  };
  
  const dragData = {
    type: 'reactflow',
    nodeType: componentKey,
    component: {
      id: node.id,
      name: node.name,
      type: componentType,
      metadata: enhancedMetadata,
      requiresRoleSelection: requiresRoleSelection
    },
    metadata: enhancedMetadata,
    source: 'sidebar',
    requiresRoleSelection: requiresRoleSelection
  };
  
  return dragData;
};