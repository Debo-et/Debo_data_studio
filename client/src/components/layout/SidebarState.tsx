// src/components/layout/SidebarState.ts

import {
  FileCode,
  Globe,
  FileText,
  Briefcase,
  BookOpen,
  FileSpreadsheet,
  FileJson,
  Cloud,
  Network,
  HardDrive,
  Search,
  Database as DatabaseIcon,
  Folder,
  Trash2,
  Code
} from 'lucide-react';
import { RepositoryNode, JobDesignMetadata } from '../../types/types';
import { DatabaseConfig } from '../../services/database-api.service';

// ==================== TYPE DEFINITIONS ====================

export interface RepositorySidebarProps {
  onNodeSelect?: (node: RepositoryNode) => void;
  onNodeDoubleClick?: (node: RepositoryNode) => void;
  onCreateItem?: (type: string, parentId?: string) => void;
  onDeleteItem?: (node: RepositoryNode) => void;
  onDragStart?: (node: RepositoryNode) => void;
  onDrop?: (target: RepositoryNode, source: RepositoryNode) => void;
  onCreateJob?: (jobName: string) => void;
  currentJob?: any;
  reactFlowInstance?: any;
  onNodeCreate?: (nodeData: any, position: { x: number; y: number }) => void;
  onNodeUpdate?: (nodeId: string, updates: any) => void;
  onEdgeUpdate?: (edgeId: string, updates: any) => void;
}

export interface DatabaseConnectionDialogProps {
  isOpen: boolean;
  onClose: () => void;
  onConnect: (config: DatabaseConfig, connectionId: string) => void;
  initialConfig?: DatabaseConfig;
}

export interface DatabaseStatus {
  isConnected: boolean;
  error?: string;
  version?: string;
}

export interface JobCreationDialogProps {
  isOpen: boolean;
  onClose: () => void;
  onCreateJob: (jobName: string) => void;
}

export interface DeleteConfirmationDialogProps {
  isOpen: boolean;
  node: RepositoryNode | null;
  onClose: () => void;
  onConfirm: () => void;
  deleteMode?: 'single' | 'recursive' | 'bulk';
}

export interface DeleteStats {
  nodesToDelete: number;
  hasChildren: boolean;
  nodeName: string;
  metadataCount: number;
  folderCount: number;
  itemCount: number;
  hasForeignTables: boolean;
  hasDatabaseConnections: boolean;
}

export interface DeletionHistoryItem {
  node: RepositoryNode;
  parentId?: string;
  deletedAt: string; // Make this required
  id: string;
  childrenCount?: number;
  timestamp: string; // Add timestamp field
}

export interface JobDesignsContextMenuProps {
  node: RepositoryNode;
  position: { x: number; y: number };
  onClose: () => void;
  onCreateJob: () => void;
}

export interface DragState {
  isDragging: boolean;
  draggedNode: RepositoryNode | null;
  dragOverNode: RepositoryNode | null;
  dropPosition: 'before' | 'inside' | 'after' | null;
}

export interface DebugInfo {
  foreignTablesCount?: number;
  lastOperation?: string;
  lastError?: string;
  lastOperationTime?: Date;
  connectionAttempts?: number;
}

export interface MetadataFormData {
  name: string;
  description?: string;
  purpose?: string;
  filePath?: string;
  [key: string]: any;
}

// ==================== INITIAL STATES ====================

export const getDefaultConnectionConfig = (): DatabaseConfig => {
  const getDefaultUser = (): string => {
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

  return {
    host: 'localhost',
    port: '5432',
    dbname: 'postgres',
    user: getDefaultUser(),
    password: '',
    schema: 'public'
  };
};

export const getDefaultRepositoryData = (currentJob?: any): RepositoryNode[] => {
  return [
    {
      id: 'job-designs',
      name: 'Job Designs',
      type: 'category',
      icon: <FileCode className="h-4 w-4" />,
      draggable: false,
      droppable: true,
      children: currentJob ? [
        {
          id: `job-${currentJob.id}`,
          name: currentJob.name,
          type: 'job',
          icon: <Folder className="h-4 w-4" />,
          metadata: {
            type: 'job-design',
            jobId: currentJob.id,
            createdAt: currentJob.createdAt,
            lastModified: currentJob.lastModified
          } as JobDesignMetadata,
          draggable: false,
          droppable: false
        }
      ] : []
    },
    {
      id: 'contexts',
      name: 'Contexts',
      type: 'category',
      icon: <Globe className="h-4 w-4" />,
      draggable: false,
      droppable: true,
    },
    {
      id: 'metadata',
      name: 'Metadata',
      type: 'category',
      icon: <DatabaseIcon className="h-4 w-4" />,
      draggable: false,
      droppable: true,
      children: [
        {
          id: 'db-connections',
          name: 'DB Connections',
          type: 'folder',
          icon: <DatabaseIcon className="h-4 w-4" />,
          draggable: true,
          droppable: true,
        },
        {
          id: 'sap-connection',
          name: 'SAP Connection',
          type: 'folder',
          icon: <DatabaseIcon className="h-4 w-4" />,
          draggable: true,
          droppable: true,
          children: []
        },
        {
          id: 'file-delimited',
          name: 'File Delimited',
          type: 'folder',
          icon: <FileText className="h-4 w-4" />,
          draggable: true,
          droppable: true,
          children: []
        },
        {
          id: 'file-positional',
          name: 'File Positional',
          type: 'folder',
          icon: <FileText className="h-4 w-4" />,
          draggable: true,
          droppable: true,
          children: []
        },
        {
          id: 'file-xml',
          name: 'File XML',
          type: 'folder',
          icon: <FileCode className="h-4 w-4" />,
          draggable: true,
          droppable: true,
          children: []
        },
        {
          id: 'file-excel',
          name: 'File Excel',
          type: 'folder',
          icon: <FileSpreadsheet className="h-4 w-4" />,
          draggable: true,
          droppable: true,
          children: []
        },
        {
          id: 'file-schema',
          name: 'File Schema',
          type: 'folder',
          icon: <FileText className="h-4 w-4" />,
          draggable: true,
          droppable: true,
          children: []
        },
        {
          id: 'file-regex',
          name: 'File Regex',
          type: 'folder',
          icon: <Search className="h-4 w-4" />,
          draggable: true,
          droppable: true,
          children: []
        },
        {
          id: 'file-ldif',
          name: 'File LDIF',
          type: 'folder',
          icon: <Network className="h-4 w-4" />,
          draggable: true,
          droppable: true,
          children: []
        },
        {
          id: 'file-json-avro-parquet',
          name: 'File JSON / Avro / Parquet',
          type: 'folder',
          icon: <FileJson className="h-4 w-4" />,
          draggable: true,
          droppable: true,
          children: []
        },
        {
          id: 'web-service',
          name: 'Web Service',
          type: 'folder',
          icon: <Globe className="h-4 w-4" />,
          draggable: true,
          droppable: true,
          children: []
        },
        {
          id: 'ldap',
          name: 'LDAP',
          type: 'folder',
          icon: <Network className="h-4 w-4" />,
          draggable: true,
          droppable: true,
          children: []
        },
        {
          id: 'ftp-sftp',
          name: 'FTP/SFTP',
          type: 'folder',
          icon: <HardDrive className="h-4 w-4" />,
          draggable: true,
          droppable: true,
          children: []
        },
        {
          id: 'salesforce',
          name: 'Salesforce',
          type: 'folder',
          icon: <Cloud className="h-4 w-4" />,
          draggable: true,
          droppable: true,
          children: [
            {
              id: 'sf-prod',
              name: 'SF_Production',
              type: 'item',
              icon: <Cloud className="h-4 w-4" />,
              draggable: true,
              droppable: false,
            }
          ]
        }
      ]
    },
    {
      id: 'code',
      name: 'Code',
      type: 'category',
      icon: <Code className="h-4 w-4" />,
      draggable: false,
      droppable: true,
    },
    {
      id: 'sql-templates',
      name: 'SQL Templates',
      type: 'category',
      icon: <FileText className="h-4 w-4" />,
      draggable: false,
      droppable: true,
      children: []
    },
    {
      id: 'business-models',
      name: 'Business Models',
      type: 'category',
      icon: <Briefcase className="h-4 w-4" />,
      draggable: false,
      droppable: true,
      children: []
    },
    {
      id: 'documentation',
      name: 'Documentation',
      type: 'category',
      icon: <BookOpen className="h-4 w-4" />,
      draggable: false,
      droppable: true,
      children: []
    },
    {
      id: 'recycle-bin',
      name: 'Recycle Bin',
      type: 'category',
      icon: <Trash2 className="h-4 w-4" />,
      draggable: false,
      droppable: true,
      children: []
    }
  ];
};

export const getInitialExpandedNodes = (): Set<string> => {
  return new Set([
    'job-designs', 'contexts', 'metadata', 'code', 'sql-templates',
    'business-models', 'documentation', 'recycle-bin'
  ]);
};

export const getInitialDragState = (): DragState => ({
  isDragging: false,
  draggedNode: null,
  dragOverNode: null,
  dropPosition: null
});

export const getInitialDebugInfo = (): DebugInfo => ({
  connectionAttempts: 0
});

export const getInitialDeletionHistory = (): DeletionHistoryItem[] => {
  try {
    const saved = localStorage.getItem('repository-deletion-history');
    return saved ? JSON.parse(saved) : [];
  } catch (e) {
    return [];
  }
};

// ==================== STATE VALIDATORS ====================

export const isNodeDeletable = (node: RepositoryNode, currentJob?: any): boolean => {
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
  
  if (node.type === 'job' && currentJob?.id === node.id.replace('job-', '')) {
    return false;
  }
  
  return true;
};

// ==================== STATE UPDATERS ====================

export const updateRepositoryDataWithJob = (
  repositoryData: RepositoryNode[],
  currentJob: any
): RepositoryNode[] => {
  if (!currentJob) return repositoryData;
  
  const jobExists = repositoryData.some(category => 
    category.children?.some(child => 
      child.metadata && 
      typeof child.metadata === 'object' && 
      'type' in child.metadata && 
      child.metadata.type === 'job-design' &&
      (child.metadata as JobDesignMetadata).jobId === currentJob.id
    )
  );
  
  if (jobExists) return repositoryData;
  
  return repositoryData.map(category => {
    if (category.id === 'job-designs') {
      const jobNode: RepositoryNode = {
        id: `job-${currentJob.id}`,
        name: currentJob.name,
        type: 'job',
        icon: <Folder className="h-4 w-4" />,
        metadata: {
          type: 'job-design',
          jobId: currentJob.id,
          createdAt: currentJob.createdAt,
          lastModified: currentJob.lastModified
        } as JobDesignMetadata,
        draggable: false,
        droppable: false
      };
      
      return {
        ...category,
        children: [...(category.children || []), jobNode]
      };
    }
    return category;
  });
};

// ==================== STATE FILTERS ====================

export const filterRepositoryDataBySearch = (
  nodes: RepositoryNode[],
  searchTerm: string
): RepositoryNode[] => {
  if (!searchTerm.trim()) return nodes;
  
  const filtered = nodes.filter(node => {
    const matches = node.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
                   (node.metadata?.description && 
                    node.metadata.description.toLowerCase().includes(searchTerm.toLowerCase()));
    
    if (node.children) {
      const filteredChildren = filterRepositoryDataBySearch(node.children, searchTerm);
      return matches || filteredChildren.length > 0;
    }
    
    return matches;
  });
  
  return filtered;
};