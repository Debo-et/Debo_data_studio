import React, { useState, useCallback, useEffect, useRef, useContext } from 'react';
import { toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';

// Import modular components
import ExcelMetadataWizard from '../../Wizard/ExcelMetadataWizard';
import XMLMetadataWizard from '../../Wizard/XMLMetadataWizard';
import DelimitedFileMetadataWizard from '../../Wizard/DelimitedFileMetadataWizard';
import PositionalFileMetadataWizard from '../../Wizard/PositionalFileMetadataWizard';
import FileSchemaMetadataWizard from '../../Wizard/FileSchemaMetadataWizard';
import JsonAvroParquetMetadataWizard from '../../Wizard/JsonAvroParquetMetadataWizard';
import RegexMetadataWizard from '../../Wizard/RegexMetadataWizard';
import LDIFMetadataWizard from '../../Wizard/LDIFMetadataWizard';
import WebServiceMetadataWizard from '../../Wizard/WebServiceMetadataWizard';
import DatabaseMetadataWizard from '../../Wizard/DatabaseMetadataWizard';

// Import React components from Wizard
import ContextMenu from '../../Wizard/ContextMenu';

// Import split modules
import * as Layout from './SidebarLayout';
import * as State from './SidebarState';
import * as Handlers from './SidebarHandlers';
import * as Utils from './SidebarUtils';

// Import types
import {
  RepositoryNode,
  ExcelMetadataFormData,
  DeletionHistoryItem
} from '../../types/types';

// Import icons
import { Plus, Layers, RefreshCw } from 'lucide-react';
import * as XLSX from 'xlsx';

// Import Canvas Design Service and Hook
import { CanvasDesignManager } from '../../services/canvas-design.service';
import { useCanvasDesign } from '../../hooks/useCanvasDesign';
import { COMPONENT_REGISTRY, getCategoryColor } from '@/pages/ComponentRegistry';

// ==================== DATABASE CONTEXT & FOREIGN TABLE HELPERS ====================
import { DatabaseContext } from '../../App';
import { DatabaseApiService } from '../../services/database-api.service';
import { ColumnDefinition } from '../../api/postgres-foreign-table';

// ==================== REVERSE METADATA IMPORTS ====================
import {
  getForeignTablesInSchema,
  reverseForeignTableMetadata
} from '../../api/postgres-foreign-table-reverse';

// ==================== CANVAS PERSISTENCE ====================
import { canvasPersistence } from '../../services/canvas-persistence.service';

// ============================================================================
// 🛡️ MODULE-LEVEL CACHE – survives component remounts
// ============================================================================
let __globalRepositoryCache: RepositoryNode[] | null = null;

// ============================================================================
// Interface
// ============================================================================
interface ReactFlowDragData {
  type: 'reactflow-component';
  componentId: string;
  source: 'sidebar' | 'rightPanel';
  metadata?: Record<string, any>;
}

interface EnhancedRepositorySidebarProps extends Omit<State.RepositorySidebarProps, 'onDragStart' | 'onDrop' | 'onCreateJob'> {
  onCreateJob?: (jobName: string, jobId?: string) => void;
  onCanvasDesignCreate?: (design: any) => void;
  onCanvasDesignDelete?: (designId: string) => void;
  onCanvasDesignSwitch?: (designId: string | null) => void;
  onCanvasDesignDuplicate?: (sourceId: string, newDesign: any) => void;
  activeCanvasDesignId?: string | null;
  activeCanvasId?: string | null;
  // NEW: Callback to notify App to load a canvas by ID
  onCanvasSelect?: (canvasId: string) => void;
}

// ============================================================================
// ✅ EXACT REPOSITORY STRUCTURE FROM LOGS – 8 root nodes, file categories under "metadata"
// ============================================================================
const getDefaultRepositoryStructure = (): RepositoryNode[] => {
  return [
    {
      id: 'job-designs',
      name: 'Job Designs',
      type: 'folder',
      children: [],
      draggable: false,
      metadata: { type: 'folder', count: 0, system: true }
    },
    {
      id: 'contexts',
      name: 'Contexts',
      type: 'folder',
      children: [],
      draggable: false,
      metadata: { type: 'folder', count: 0, system: true }
    },
    {
      id: 'metadata',
      name: 'Metadata',
      type: 'folder',
      children: [
        { id: 'db-connections', name: 'Database Connections', type: 'folder', children: [], draggable: false, metadata: { type: 'folder', count: 0 } },
        { id: 'sap-connection', name: 'SAP Connection', type: 'folder', children: [], draggable: false, metadata: { type: 'folder', count: 0 } },
        { id: 'file-delimited', name: 'Delimited Files', type: 'category', children: [], draggable: false, metadata: { type: 'category', count: 0 } },
        { id: 'file-positional', name: 'Positional Files', type: 'category', children: [], draggable: false, metadata: { type: 'category', count: 0 } },
        { id: 'file-xml', name: 'XML Files', type: 'category', children: [], draggable: false, metadata: { type: 'category', count: 0 } },
        { id: 'file-excel', name: 'Excel Files', type: 'category', children: [], draggable: false, metadata: { type: 'category', count: 0 } },
        { id: 'file-schema', name: 'File Schema', type: 'category', children: [], draggable: false, metadata: { type: 'category', count: 0 } },
        { id: 'file-regex', name: 'Regex Patterns', type: 'category', children: [], draggable: false, metadata: { type: 'category', count: 0 } },
        { id: 'file-ldif', name: 'LDIF', type: 'category', children: [], draggable: false, metadata: { type: 'category', count: 0 } },
        { id: 'file-json-avro-parquet', name: 'JSON/Avro/Parquet', type: 'category', children: [], draggable: false, metadata: { type: 'category', count: 0 } },
        { id: 'web-service', name: 'Web Service', type: 'category', children: [], draggable: false, metadata: { type: 'category', count: 0 } },
        { id: 'ldap', name: 'LDAP', type: 'folder', children: [], draggable: false, metadata: { type: 'folder', count: 0 } },
        { id: 'ftp-sftp', name: 'FTP/SFTP', type: 'folder', children: [], draggable: false, metadata: { type: 'folder', count: 0 } },
        { id: 'salesforce', name: 'Salesforce', type: 'folder', children: [], draggable: false, metadata: { type: 'folder', count: 0 } }
      ],
      draggable: false,
      metadata: { type: 'folder', count: 0, system: true }
    },
    {
      id: 'code',
      name: 'Code',
      type: 'folder',
      children: [],
      draggable: false,
      metadata: { type: 'folder', count: 0, system: true }
    },
    {
      id: 'sql-templates',
      name: 'SQL Templates',
      type: 'folder',
      children: [],
      draggable: false,
      metadata: { type: 'folder', count: 0, system: true }
    },
    {
      id: 'business-models',
      name: 'Business Models',
      type: 'folder',
      children: [],
      draggable: false,
      metadata: { type: 'folder', count: 0, system: true }
    },
    {
      id: 'documentation',
      name: 'Documentation',
      type: 'folder',
      children: [],
      draggable: false,
      metadata: { type: 'folder', count: 0, system: true }
    },
    {
      id: 'recycle-bin',
      name: 'Recycle Bin',
      type: 'folder',
      children: [],
      draggable: false,
      metadata: { type: 'folder', count: 0, system: true }
    }
  ];
};

// ============================================================================
// Helper: map fileType to repository folder ID (these are now nested under "metadata")
// ============================================================================
function determineFolderId(fileType: string): string | null {
  switch (fileType?.toLowerCase()) {
    case 'excel': return 'file-excel';
    case 'xml': return 'file-xml';
    case 'csv':
    case 'tsv':
    case 'delimited':
    case 'txt': return 'file-delimited';
    case 'fixed':
    case 'positional': return 'file-positional';
    case 'schema': return 'file-schema';
    case 'json':
    case 'avro':
    case 'parquet': return 'file-json-avro-parquet';
    case 'regex': return 'file-regex';
    case 'ldif': return 'file-ldif';
    case 'web-service': return 'web-service';
    case 'database': return 'db-connections';   // ✅ correct folder under "metadata"
    default: return null;
  }
}

// ============================================================================
// Helper: create a RepositoryNode from reversed foreign table metadata
// ============================================================================
function createNodeFromMetadata(
  meta: any,
  folderId: string,
  connectionId: string
): RepositoryNode {
  const columns = (meta.columns || []).map((col: any) => ({
    name: col.name,
    dataType: col.type,
    type: col.type,
    length: col.length,
    precision: col.precision,
    scale: col.scale,
    nullable: col.nullable ?? true,
    defaultValue: col.defaultValue,
  }));

  const nodeId = `${meta.fileType}-${Date.now()}-${Math.random().toString(36).substring(2, 8)}`;

  return {
    id: nodeId,
    name: meta.tableName,
    type: 'item',
    metadata: {
      ...meta,
      postgresTableName: meta.tableName,
      filePath: meta.filePath,
      options: meta.options,
      columns,
      oid: meta.oid,
      createdAt: new Date().toISOString(),
      lastModified: new Date().toISOString(),
      connection: { connectionId },
    },
    draggable: true,
    droppable: false,
    parentId: folderId,
  };
}

// ============================================================================
// Helper: get the most recent active PostgreSQL connection ID
// ============================================================================
async function getActivePostgresConnectionId(apiService: DatabaseApiService): Promise<string | null> {
  try {
    const connections = await apiService.getActiveConnections();
    const pgConnections = connections.filter(c => 
      c.dbType === 'postgresql' || c.dbType === 'postgres'
    );
    if (pgConnections.length === 0) return null;
    pgConnections.sort((a, b) => {
      const extractTimestamp = (id: string) => {
        const parts = id.split('_');
        const timestamp = parts[3];
        return timestamp ? parseInt(timestamp, 10) : 0;
      };
      return extractTimestamp(b.connectionId) - extractTimestamp(a.connectionId);
    });
    return pgConnections[0].connectionId;
  } catch (error) {
    console.error('Failed to get active PostgreSQL connection:', error);
    return null;
  }
}

// ============================================================================
// 🔍 RECURSIVE TREE UTILITIES – find & update nested nodes
// ============================================================================
function findNodeAndParent(
  nodes: RepositoryNode[],
  nodeId: string,
  parent: RepositoryNode | null = null
): { node: RepositoryNode; parent: RepositoryNode | null; path: number[] } | null {
  for (let i = 0; i < nodes.length; i++) {
    const node = nodes[i];
    if (node.id === nodeId) {
      return { node, parent, path: [i] };
    }
    if (node.children && node.children.length > 0) {
      const found = findNodeAndParent(node.children, nodeId, node);
      if (found) {
        return { ...found, path: [i, ...found.path] };
      }
    }
  }
  return null;
}

function updateNodeInTree(
  nodes: RepositoryNode[],
  targetId: string,
  updater: (node: RepositoryNode) => RepositoryNode
): RepositoryNode[] {
  return nodes.map(node => {
    if (node.id === targetId) {
      return updater(node);
    }
    if (node.children && node.children.length > 0) {
      return {
        ...node,
        children: updateNodeInTree(node.children, targetId, updater)
      };
    }
    return node;
  });
}

// ============================================================================
// Main Component
// ============================================================================
const RepositorySidebar: React.FC<EnhancedRepositorySidebarProps> = ({
  onNodeSelect,
  onCreateItem,
  currentJob,
  onCreateJob,
  reactFlowInstance,
  onNodeCreate,
  onNodeUpdate,
  onCanvasDesignDelete,
  onCanvasDesignSwitch,
  activeCanvasDesignId,
  onCanvasSelect, // NEW
}) => {
  // ==================== DATABASE CONTEXT ====================
  const databaseContext = useContext(DatabaseContext);
  const { apiService, isConnected, testConnection } = databaseContext;

  // ==================== STATE MANAGEMENT ====================
  // 🛡️ ENHANCED INITIALIZER: uses global cache + merges saved data, never loses categories
  const [repositoryData, _setRepositoryData] = useState<RepositoryNode[]>(() => {
    // 1. Restore from global cache if available (remount protection)
    if (__globalRepositoryCache) {
      console.log('♻️ Restoring repository data from global cache');
      return __globalRepositoryCache;
    }

    // 2. Start with the full 8-root structure from logs
    const fullStructure = getDefaultRepositoryStructure();

    // 3. Try to merge any saved data from State, but NEVER drop categories
    try {
      const savedData = State.getDefaultRepositoryData(currentJob);
      if (savedData && Array.isArray(savedData) && savedData.length > 0) {
        // Simple merge – we trust the cache for remounts
        __globalRepositoryCache = fullStructure;
        return fullStructure;
      }
    } catch (error) {
      console.warn('Failed to load repository data from State:', error);
    }

    // 4. Fallback to full structure
    __globalRepositoryCache = fullStructure;
    return fullStructure;
  });

  // 🛡️ WRAPPED SETTER – updates cache + prevents destructive resets to default empty structure
  const defaultStructure = useRef(getDefaultRepositoryStructure()).current;
  const setRepositoryData = useCallback((value: React.SetStateAction<RepositoryNode[]>) => {
    _setRepositoryData(prev => {
      const next = typeof value === 'function' ? value(prev) : value;

      // 🛡️ GUARD: Prevent resetting to the default empty structure if we already have non-zero children
      const hasNonZeroChildren = (nodes: RepositoryNode[]): boolean => {
        for (const node of nodes) {
          if (node.children && node.children.length > 0) return true;
          if (node.children && hasNonZeroChildren(node.children)) return true;
        }
        return false;
      };

      const isDefaultReset = JSON.stringify(next) === JSON.stringify(defaultStructure);
      if (isDefaultReset && hasNonZeroChildren(prev)) {
        console.warn('⛔ Prevented reset of repositoryData to default empty structure');
        __globalRepositoryCache = prev;
        return prev;
      }

      // ✅ Normal update – store in cache and return
      __globalRepositoryCache = next;
      return next;
    });
  }, [defaultStructure]);

  const [expandedNodes, setExpandedNodes] = useState<Set<string>>(() => {
    const rootNodeIds = repositoryData.map(node => node.id);
    return new Set(rootNodeIds);
  });

  const [expandedMetadataNodes, setExpandedMetadataNodes] = useState<Set<string>>(() => new Set());
  const [selectedNode, setSelectedNode] = useState<string | null>(null);
  const [isSyncing, setIsSyncing] = useState(false);

  // Metadata settings state
  const [metadataSettings] = useState({
    showAll: false,
    autoExpandNew: true,
    compactMode: false,
    showIcons: true,
    maxItemsVisible: 10,
    sortOrder: 'asc' as const,
  });

  const [showMetadataSettings, setShowMetadataSettings] = useState(false);

  // Canvas Design State using hook
  const { deleteDesign } = useCanvasDesign(currentJob?.id);
  const [activeDesignId, setActiveDesignId] = useState<string | null>(activeCanvasDesignId || null);

  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [nodeToDelete, setNodeToDelete] = useState<RepositoryNode | null>(null);
  const [, setDeletionHistory] = useState<DeletionHistoryItem[]>([]);

  const [jobDesignContextMenu, setJobDesignContextMenu] = useState<{
    node: RepositoryNode;
    position: { x: number; y: number };
  } | null>(null);

  const [searchTerm] = useState('');
  const [contextMenu, setContextMenu] = useState<{ node: RepositoryNode; position: { x: number; y: number } } | null>(null);
  const [, setJobCreationDialogOpen] = useState(false);

  // Wizard states
  const [excelWizardOpen, setExcelWizardOpen] = useState(false);
  const [xmlWizardOpen, setXmlWizardOpen] = useState(false);
  const [delimitedWizardOpen, setDelimitedWizardOpen] = useState(false);
  const [positionalWizardOpen, setPositionalWizardOpen] = useState(false);
  const [fileSchemaWizardOpen, setFileSchemaWizardOpen] = useState(false);
  const [jsonAvroParquetWizardOpen, setJsonAvroParquetWizardOpen] = useState(false);
  const [regexWizardOpen, setRegexWizardOpen] = useState(false);
  const [ldifWizardOpen, setLdifWizardOpen] = useState(false);
  const [webServiceWizardOpen, setWebServiceWizardOpen] = useState(false);
  const [databaseWizardOpen, setDatabaseWizardOpen] = useState(false);

  const contextMenuRef = useRef<HTMLDivElement>(null);
  const jobDesignContextMenuRef = useRef<HTMLDivElement>(null);

  // ==================== CANVAS LOADING INSIDE SIDEBAR (NEW) ====================
  // Sidebar.tsx – add state variables
const [, setCanvasLoading] = useState(false);
const [, setCanvasError] = useState<string | null>(null);
const [isDeletingForeignTable, setIsDeletingForeignTable] = useState(false);
const [isDeleting, setIsDeleting] = useState(false);

useEffect(() => {
  if (!isConnected) return;

  const loadCanvases = async () => {
    setCanvasLoading(true);
    setCanvasError(null);
    try {
      console.log('🎨 [Sidebar] Loading canvases from database...');
      const canvasesList = await canvasPersistence.listCanvases();
      console.log('🎨 [Sidebar] Fetched canvases:', canvasesList);

      if (canvasesList.length === 0) {
        console.log('🎨 [Sidebar] No canvases found.');
        return;
      }

      // Update repository tree with canvas nodes
      setRepositoryData(prev => {
        const jobDesignsIndex = prev.findIndex(n => n.id === 'job-designs');
        if (jobDesignsIndex === -1) {
          console.warn('🎨 [Sidebar] job-designs folder not found!');
          return prev;
        }

        const jobDesignsNode = prev[jobDesignsIndex];
        const nonCanvasChildren = (jobDesignsNode.children || []).filter(
          child => child.type !== 'canvas'
        );
        const canvasNodes = canvasesList.map(canvas => ({
          id: `canvas-${canvas.id}`,
          name: canvas.name,
          type: 'canvas' as const,
          metadata: {
            canvasId: canvas.id,
            createdAt: canvas.updated_at,
            updatedAt: canvas.updated_at,
          },
          draggable: true,
          droppable: false,
          parentId: 'job-designs',
        }));

        const newChildren = [...canvasNodes, ...nonCanvasChildren];
        console.log('🎨 [Sidebar] New children under job-designs:', newChildren.map(c => ({ id: c.id, name: c.name })));

        const updatedNode = {
          ...jobDesignsNode,
          children: newChildren,
          metadata: {
            ...jobDesignsNode.metadata,
            count: newChildren.length,
          },
        };
        const newTree = [...prev];
        newTree[jobDesignsIndex] = updatedNode;
        return newTree;
      });

      // Expand the Job Designs folder
      setExpandedNodes(prev => new Set([...prev, 'job-designs']));

      // Notify parent to load the most recent canvas
      if (onCanvasSelect && canvasesList.length > 0) {
        onCanvasSelect(canvasesList[0].id);
      }
    } catch (error: any) {
      console.error('❌ [Sidebar] Failed to load canvases:', error);
      setCanvasError(error.message || 'Failed to load canvases');
      toast.error('Failed to load canvases. Please refresh.');
    } finally {
      setCanvasLoading(false);
    }
  };

  loadCanvases();
}, [isConnected, setRepositoryData, setExpandedNodes, onCanvasSelect]);
  // ==================== METADATA CREATION EVENT LISTENER ====================
  useEffect(() => {
    const handleMetadataCreated = (event: CustomEvent) => {
      console.log('📥 metadata-created event received:', event.detail);
      const { metadata, type, folderId } = event.detail;

      const nodeId = `${type}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

      const newNode: RepositoryNode = {
        id: nodeId,
        name: metadata.name || `New ${type}`,
        type: 'item',
        metadata: {
          ...metadata,
          type: type,
          createdAt: new Date().toISOString(),
          lastModified: new Date().toISOString()
        },
        draggable: true,
        droppable: false,
        parentId: folderId
      };

      console.log('🆕 New repository node created:', newNode);

      setRepositoryData(prev => {
        // Recursively find the folder and add the child
        const found = findNodeAndParent(prev, folderId);
        if (!found) {
          console.warn(`Folder ${folderId} not found, cannot add node`);
          return prev;
        }
        return updateNodeInTree(prev, folderId, folder => {
          const existingChildren = folder.children || [];
          const isDuplicate = existingChildren.some(
            child => child.id === nodeId || child.name === newNode.name
          );
          if (!isDuplicate) {
            return {
              ...folder,
              children: [...existingChildren, newNode],
              metadata: {
                ...folder.metadata,
                count: (folder.metadata?.count || 0) + 1
              }
            };
          }
          return folder;
        });
      });

      setSelectedNode(nodeId);

      toast.success(`✅ "${newNode.name}" created successfully`, {
        position: "bottom-right",
        autoClose: 3000,
      });
    };

    const listener = handleMetadataCreated as EventListener;
    window.addEventListener('metadata-created', listener);
    return () => {
      window.removeEventListener('metadata-created', listener);
    };
  }, [setRepositoryData]);

  // ==================== METADATA EXPANSION HANDLERS ====================
  const handleToggleMetadata = useCallback((nodeId: string) => {
    setExpandedMetadataNodes(prev => {
      const newSet = new Set(prev);
      if (newSet.has(nodeId)) {
        newSet.delete(nodeId);
      } else {
        newSet.add(nodeId);
      }
      return newSet;
    });
  }, []);

  // ==================== HELPER FUNCTIONS ====================
  const createNodeInTree = useCallback((type: string, parentId: string, name?: string): RepositoryNode => {
    const nodeName = name || (type === 'folder' ? 'New Folder' : type === 'job' ? 'New Job' : 'New Item');
    return {
      id: `${type}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
      name: nodeName,
      type: type as any,
      children: type === 'folder' || type === 'job-designs' ? [] : undefined,
      parentId: parentId
    };
  }, []);

  const updateRepositoryTree = useCallback((parentId: string, newNode: RepositoryNode) => {
    setRepositoryData(prev => {
      const found = findNodeAndParent(prev, parentId);
      if (!found) {
        console.warn(`Parent ${parentId} not found, cannot add node`);
        return prev;
      }
      return updateNodeInTree(prev, parentId, node => ({
        ...node,
        children: [...(node.children || []), newNode],
        metadata: {
          ...node.metadata,
          count: (node.metadata?.count || 0) + 1
        }
      }));
    });
    setExpandedNodes(prev => new Set([...prev, parentId]));
  }, []);

  // ==================== CANVAS DESIGN FUNCTIONS ====================
  const handleDeleteCanvasDesign = useCallback((designId: string, designName: string) => {
    if (window.confirm(`⚠️ Delete Canvas Design\n\nAre you sure you want to delete the design "${designName}"?\n\nThis will remove all canvas nodes, connections, and persisted state. This action cannot be undone.`)) {
      const success = deleteDesign(designId);
      if (success && onCanvasDesignDelete) {
        onCanvasDesignDelete(designId);
      }

      setRepositoryData(prev => {
        const removeNode = (nodes: RepositoryNode[]): RepositoryNode[] => {
          return nodes.filter(n => {
            if (n.type === 'job' && n.metadata?.canvasDesignId === designId) {
              return false;
            }
            if (n.children && n.children.length > 0) {
              n.children = removeNode(n.children);
            }
            return true;
          });
        };
        return removeNode(prev);
      });

      if (activeDesignId === designId) {
        setActiveDesignId(null);
        if (onCanvasDesignSwitch) {
          onCanvasDesignSwitch(null);
        }
      }

      toast.success(`🗑️ Canvas design "${designName}" deleted`, {
        position: "bottom-right",
        autoClose: 3000,
      });

      window.dispatchEvent(new CustomEvent('repository-updated'));
    }
  }, [deleteDesign, onCanvasDesignDelete, onCanvasDesignSwitch, activeDesignId, setRepositoryData]);

  // ==================== OTHER HANDLERS ====================
  const handleCreateItem = useCallback((type: string, parentId?: string) => {
    if (type === 'job') {
      setJobCreationDialogOpen(true);
    } else if (parentId) {
      const newNode = createNodeInTree(type, parentId);
      updateRepositoryTree(parentId, newNode);
    }

    if (onCreateItem) {
      onCreateItem(type, parentId);
    }

    setContextMenu(null);
    setJobDesignContextMenu(null);
  }, [createNodeInTree, updateRepositoryTree, onCreateItem]);

const handleCreateJobWizard = useCallback(() => {
  const jobName = window.prompt('Enter job name:');
  if (!jobName || !jobName.trim()) {
    setContextMenu(null);
    return;
  }

  // 1. Create the new job node
  const newNode: RepositoryNode = {
    id: `job-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
    name: jobName.trim(),
    type: 'job',
    children: [],               // jobs can have canvas children later
    metadata: {
      createdAt: new Date().toISOString(),
      // you may add more fields (author, version, etc.) as needed
    },
    draggable: true,
    droppable: true,            // allow canvas nodes to be added
    parentId: 'job-designs',    // hardcoded because the wizard is only for this folder
  };

  // 2. Update the repository tree
  setRepositoryData(prev => {
    const updated = updateNodeInTree(prev, 'job-designs', folder => ({
      ...folder,
      children: [...(folder.children || []), newNode],
      metadata: {
        ...folder.metadata,
        count: (folder.metadata?.count || 0) + 1,
      },
    }));
    return updated;
  });

  // 3. Ensure the folder is expanded
  setExpandedNodes(prev => new Set([...prev, 'job-designs']));

  // 4. (Optional) select the new job
  setSelectedNode(newNode.id);

  // 5. Call the parent callback (if any)
  onCreateJob?.(jobName.trim());

  // 6. Close the context menu
  setContextMenu(null);
}, [onCreateJob, setRepositoryData, setExpandedNodes, setSelectedNode]);

  // ==================== EVENT HANDLERS ====================
  const handleToggle = useCallback((nodeId: string) => {
    Handlers.handleToggle(nodeId, expandedNodes, setExpandedNodes);
  }, [expandedNodes]);

  const handleSelect = useCallback((node: RepositoryNode) => {
    Handlers.handleSelect(node, setSelectedNode, onNodeSelect);
  }, [onNodeSelect]);

  const handleContextMenu = useCallback((node: RepositoryNode, position: { x: number; y: number }) => {
    const adjustedX = Math.min(position.x, window.innerWidth - 220);
    const adjustedY = Math.min(position.y, window.innerHeight - 250);

    if (node.type === 'job') {
      setJobDesignContextMenu({ node, position: { x: adjustedX, y: adjustedY } });
    } else {
      setContextMenu({ node, position: { x: adjustedX, y: adjustedY } });
    }
  }, []);

  const handleDeleteItem = useCallback((node: RepositoryNode) => {
    handleDeleteNode(node);
  }, []);

  const handleDeleteNode = useCallback((node: RepositoryNode) => {
  // Job deletion (canvas) – unchanged
  if (node.type === 'job') {
    const designId = node.metadata?.canvasDesignId;
    if (designId) {
      const design = CanvasDesignManager.getDesign(designId);
      handleDeleteCanvasDesign(designId, design?.name || node.name);
    }
    setRepositoryData(prev => {
      const removeNode = (nodes: RepositoryNode[]): RepositoryNode[] => {
        return nodes.filter(n => n.id !== node.id).map(n => ({
          ...n,
          children: n.children ? removeNode(n.children) : undefined
        }));
      };
      return removeNode(prev);
    });
    return;
  }

  // Check if this node represents a PostgreSQL foreign table
  const isForeignTable = node.metadata?.postgresTableName && node.metadata?.connection?.connectionId;
  if (isForeignTable) {
    setNodeToDelete(node);
    setIsDeletingForeignTable(true);
    setDeleteDialogOpen(true);
    return;
  }

    Handlers.handleDeleteNodeRequest(
    node,
    currentJob,
    State.isNodeDeletable,
    setNodeToDelete,
    setDeleteDialogOpen
  );
}, [currentJob, handleDeleteCanvasDesign, setRepositoryData]);

 const confirmDeleteNode = useCallback(async () => {
  if (!nodeToDelete || isDeleting) return;

  // Foreign table deletion path
  if (isDeletingForeignTable) {
    setIsDeleting(true);

    const connectionId = nodeToDelete.metadata?.connection?.connectionId;
    const tableName = nodeToDelete.metadata?.postgresTableName;
    const schema = nodeToDelete.metadata?.schema || 'public';

    if (!connectionId || !tableName) {
      toast.error('Cannot delete: missing connection or table name.');
      setIsDeletingForeignTable(false);
      setDeleteDialogOpen(false);
      setNodeToDelete(null);
      setIsDeleting(false);
      return;
    }

    // Guard against missing apiService
    if (!apiService) {
      toast.error('Database API service is not available.');
      setIsDeletingForeignTable(false);
      setDeleteDialogOpen(false);
      setNodeToDelete(null);
      setIsDeleting(false);
      return;
    }

    try {
      const sql = `DROP FOREIGN TABLE IF EXISTS ${schema}.${tableName} CASCADE;`;
      const result = await apiService.executeQuery(connectionId, sql);

      if (!result.success) {
        throw new Error(result.error || 'Failed to drop foreign table');
      }

      // Success: remove from UI using the existing handler
      await Handlers.confirmDeleteNode(
        nodeToDelete,
        repositoryData,
        selectedNode,
        setRepositoryData,
        setExpandedNodes,
        setSelectedNode,
        setDeletionHistory,
        setDeleteDialogOpen,
        setNodeToDelete,
        async () => {}, // onSuccess (already handled)
        () => {}        // onError
      );

      toast.success(`Foreign table "${tableName}" dropped successfully.`);
    } catch (error: any) {
      toast.error(`Failed to delete: ${error.message}`);
      console.error('Foreign table deletion error:', error);
      setDeleteDialogOpen(false);
      setNodeToDelete(null);
    } finally {
      setIsDeletingForeignTable(false);
      setIsDeleting(false);
    }
  } else {
    // Regular deletion (non-foreign table)
    await Handlers.confirmDeleteNode(
      nodeToDelete,
      repositoryData,
      selectedNode,
      setRepositoryData,
      setExpandedNodes,
      setSelectedNode,
      setDeletionHistory,
      setDeleteDialogOpen,
      setNodeToDelete,
      async () => {},
      () => {}
    );
  }
}, [
  nodeToDelete,
  isDeletingForeignTable,
  repositoryData,
  selectedNode,
  setRepositoryData,
  setExpandedNodes,
  setSelectedNode,
  setDeletionHistory,
  setDeleteDialogOpen,
  setNodeToDelete,
  apiService,
  isDeleting
]);
  // ==================== DRAG HANDLER ====================
  const handleDragStart = useCallback((node: RepositoryNode, event: React.DragEvent) => {
    let componentKey = 'delimited-file'; // Default fallback

    if (node.metadata?.type) {
      const metadataType = node.metadata.type.toLowerCase();
      const metadataTypeMap: Record<string, string> = {
        'excel': 'excel-file',
        'xml': 'xml-file',
        'delimited': 'delimited-file',
        'positional': 'positional-file',
        'schema': 'file-schema-input',
        'json': 'json-avro-parquet',
        'avro': 'json-avro-parquet',
        'parquet': 'json-avro-parquet',
        'regex': 'regex-input',
        'ldif': 'ldap-input',
        'database': 'database-input',
        'web-service': 'web-service-input',
        'ldap': 'ldap-input',
        'directory': 'directory-input'
      };
      componentKey = metadataTypeMap[metadataType] || 'delimited-file';
    } else if (node.name || node.metadata?.name || node.metadata?.fileName) {
      const fileName = (node.name || node.metadata?.name || node.metadata?.fileName || '').toLowerCase();
      if (fileName.endsWith('.xlsx') || fileName.endsWith('.xls') || node.metadata?.fileType === 'excel') {
        componentKey = 'excel-file';
      } else if (fileName.endsWith('.xml') || node.metadata?.fileType === 'xml') {
        componentKey = 'xml-file';
      } else if (fileName.endsWith('.json') || fileName.endsWith('.avro') || fileName.endsWith('.parquet')) {
        componentKey = 'json-avro-parquet';
      } else if (fileName.endsWith('.csv') || fileName.endsWith('.tsv') || node.metadata?.delimiter) {
        componentKey = 'delimited-file';
      } else if (node.metadata?.fixedWidth || node.metadata?.positional) {
        componentKey = 'positional-file';
      } else if (node.metadata?.schema || fileName.endsWith('.schema')) {
        componentKey = 'file-schema-input';
      } else if (node.metadata?.connection || node.metadata?.databaseType) {
        componentKey = 'database-input';
      }
    }

    const legacyComponentMap: Record<string, string> = {
      'excel': 'excel-file',
      'xml': 'xml-file',
      'delimited': 'delimited-file',
      'positional': 'positional-file',
      'json-avro-parquet': 'json-avro-parquet',
      'database': 'database-input',
      'web-service': 'web-service-input',
      'regex': 'regex-input',
      'ldif': 'ldap-input',
      'schema': 'file-schema-input',
      'folder': 'directory-input',
    };

    if (componentKey === 'delimited-file' && node.type in legacyComponentMap) {
      componentKey = legacyComponentMap[node.type];
    }

    const componentDef = COMPONENT_REGISTRY[componentKey] || COMPONENT_REGISTRY['delimited-file'];

    const dragMetadata: Record<string, any> = {
      description: componentDef.description,
      category: componentDef.category,
      createdAt: new Date().toISOString(),
      version: '1.0',
      isRepositoryNode: true,
      componentCategory: componentDef.category,
      originalNodeName: node.name,
      originalNodeType: node.type,
      talendDefinition: componentDef,
      defaultWidth: componentDef.defaultDimensions.width,
      defaultHeight: componentDef.defaultDimensions.height,
      defaultRole: componentDef.defaultRole,
      repositoryNodeId: node.id,
      repositoryNodeType: node.type,
      hasExistingMetadata: !!node.metadata,
      detectedComponentType: componentKey,
      ...(node.metadata && {
        repositoryMetadata: node.metadata,
        fileType: node.metadata.type || node.metadata.fileType,
        fileName: node.metadata.name || node.metadata.fileName,
        schema: node.metadata.schema || node.metadata.columns || node.metadata.fields,
        ...(node.metadata.type === 'excel' && {
          excelMetadata: {
            sheet: node.metadata.sheet,
            hasHeaders: node.metadata.hasHeaders,
            range: node.metadata.range
          }
        }),
        ...(node.metadata.type === 'xml' && {
          xmlMetadata: {
            rootElement: node.metadata.rootElement,
            xpath: node.metadata.xpath,
            namespace: node.metadata.namespace
          }
        }),
        ...(node.metadata.type === 'delimited' && {
          delimitedMetadata: {
            delimiter: node.metadata.delimiter,
            quoteChar: node.metadata.quoteChar,
            escapeChar: node.metadata.escapeChar
          }
        })
      })
    };

    const dragData: ReactFlowDragData = {
      type: 'reactflow-component',
      componentId: componentKey,
      source: 'sidebar',
      metadata: dragMetadata
    };

    event.dataTransfer.clearData();
    event.dataTransfer.setData('application/reactflow', JSON.stringify(dragData));
    event.dataTransfer.effectAllowed = 'copy';

    const dragImage = document.createElement('div');
    const categoryColor = getCategoryColor(componentDef.category);

    dragImage.style.cssText = `
      position: absolute;
      top: -1000px;
      left: -1000px;
      background: linear-gradient(135deg, ${categoryColor}15 0%, ${categoryColor}08 100%);
      border: 2px solid ${categoryColor}40;
      color: #374151;
      padding: 8px 12px;
      border-radius: 12px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.1);
      font-size: 12px;
      font-weight: 600;
      z-index: 10000;
      display: flex;
      align-items: center;
      gap: 8px;
      min-width: 120px;
    `;

    dragImage.innerHTML = `
      <div style="
        width: 24px;
        height: 24px;
        border-radius: 50%;
        background: linear-gradient(135deg, ${categoryColor} 0%, ${categoryColor}CC 100%);
        display: flex;
        align-items: center;
        justify-content: center;
        color: white;
        flex-shrink: 0;
      ">${componentDef.displayName.charAt(0)}</div>
      <span style="white-space: nowrap;">${componentDef.displayName}</span>
    `;

    document.body.appendChild(dragImage);
    event.dataTransfer.setDragImage(dragImage, 60, 15);
    setTimeout(() => document.body.removeChild(dragImage), 0);

    const nodeElement = event.currentTarget as HTMLElement;
    if (nodeElement) {
      nodeElement.classList.add('dragging');
      setTimeout(() => {
        nodeElement.classList.remove('dragging');
      }, 100);
    }

    console.log('📤 Enhanced drag started:', {
      nodeName: node.name,
      nodeType: node.type,
      nodeMetadata: node.metadata,
      detectedComponentKey: componentKey,
      componentDisplayName: componentDef.displayName,
      dragData
    });
  }, []);

  // ==================== REACT FLOW INTEGRATION ====================
  const createNodeDirectly = useCallback((node: RepositoryNode, position: { x: number, y: number }) => {
    const nodeData = Utils.createReactFlowNodeData(node, position);
    if (onNodeCreate) {
      onNodeCreate(nodeData, position);
    } else {
      const event = new CustomEvent('sidebar-node-create', {
        detail: { nodeData, position }
      });
      window.dispatchEvent(event);
    }
  }, [onNodeCreate]);

  const findNodeByIdWrapper = useCallback((nodes: RepositoryNode[], nodeId: string): RepositoryNode | null => {
    const found = findNodeAndParent(nodes, nodeId);
    return found ? found.node : null;
  }, []);

  // ==================== WIZARD OPEN HANDLERS ====================
  const handleOpenExcelWizard = useCallback(() => {
    console.log('🔵 Opening Excel wizard');
    setExcelWizardOpen(true);
    setContextMenu(null);
  }, []);

  const handleOpenXMLWizard = useCallback(() => {
    console.log('🔵 Opening XML wizard');
    setXmlWizardOpen(true);
    setContextMenu(null);
  }, []);

  const handleOpenDelimitedWizard = useCallback(() => {
    console.log('🔵 Opening Delimited wizard');
    setDelimitedWizardOpen(true);
    setContextMenu(null);
  }, []);

  const handleOpenPositionalWizard = useCallback(() => {
    console.log('🔵 Opening Positional wizard');
    setPositionalWizardOpen(true);
    setContextMenu(null);
  }, []);

  const handleOpenFileSchemaWizard = useCallback(() => {
    console.log('🔵 Opening File Schema wizard');
    setFileSchemaWizardOpen(true);
    setContextMenu(null);
  }, []);

  const handleOpenJsonAvroParquetWizard = useCallback(() => {
    console.log('🔵 Opening JSON/Avro/Parquet wizard');
    setJsonAvroParquetWizardOpen(true);
    setContextMenu(null);
  }, []);

  const handleOpenRegexWizard = useCallback(() => {
    console.log('🔵 Opening Regex wizard');
    setRegexWizardOpen(true);
    setContextMenu(null);
  }, []);

  const handleOpenLDIFWizard = useCallback(() => {
    console.log('🔵 Opening LDIF wizard');
    setLdifWizardOpen(true);
    setContextMenu(null);
  }, []);

  const handleOpenWebServiceWizard = useCallback(() => {
    console.log('🔵 Opening Web Service wizard');
    setWebServiceWizardOpen(true);
    setContextMenu(null);
  }, []);

  const handleOpenDatabaseWizard = useCallback(() => {
    console.log('🔵 Opening Database wizard');
    setDatabaseWizardOpen(true);
    setContextMenu(null);
  }, []);

  // ==================== CREATE CANVAS FROM JOB ====================
  const handleCreateCanvasFromJob = useCallback(async (jobNode: RepositoryNode) => {
    const jobName = jobNode.name;

    try {
      // 1. Check for existing canvas with the same name (optional, but good UX)
      const existing = await canvasPersistence.getCanvasByName(jobName);
      if (existing) {
        toast.error(`A canvas named "${jobName}" already exists.`);
        return;
      }

      // 2. Initial empty canvas data
      const initialData = {
        nodes: [],
        edges: [],
        viewport: { x: 0, y: 0, zoom: 1 }
      };

      // 3. Save via canvasPersistence – passing ownerId = null (no auth yet)
      const newCanvas = await canvasPersistence.saveCanvas(
        jobName,                     // canvas name (matches job name)
        initialData,                 // empty React Flow state
        {
          description: `Created from job: ${jobName}`,
          tags: ['job-design', 'auto-created'],
          otherUiState: { jobId: jobNode.id }
        },
        null                         // ownerId – will be replaced with actual user ID when auth is added
      );

      if (!newCanvas) {
        throw new Error('Failed to save canvas – no record returned.');
      }

      // 4. Create a child node under the job to represent the canvas
      const canvasNode: RepositoryNode = {
        id: `canvas-${newCanvas.id}`,
        name: jobName,
        type: 'canvas',
        metadata: {
          canvasId: newCanvas.id,    // store the database ID for later loading
          createdAt: new Date().toISOString()
        },
        draggable: false,
        droppable: false,
        parentId: jobNode.id
      };

      // 5. Update repository tree (PREPEND canvas node so newest appears at top)
      setRepositoryData(prev => {
        const updateNode = (nodes: RepositoryNode[]): RepositoryNode[] => {
          return nodes.map(node => {
            if (node.id === jobNode.id) {
              return {
                ...node,
                children: [canvasNode, ...(node.children || [])], // prepend
                metadata: {
                  ...node.metadata,
                  count: (node.metadata?.count || 0) + 1
                }
              };
            }
            if (node.children) {
              return { ...node, children: updateNode(node.children) };
            }
            return node;
          });
        };
        return updateNode(prev);
      });

      // Ensure the job folder is expanded so the user sees the new canvas node
      setExpandedNodes(prev => new Set([...prev, jobNode.id]));

      toast.success(`Canvas "${jobName}" created.`);

       //OPTIONAL: Automatically load the new canvas
       if (onCanvasSelect) {
         onCanvasSelect(newCanvas.id);
       }
    } catch (error: any) {
      console.error('Failed to create canvas:', error);
      toast.error(`Failed to create canvas: ${error.message || 'Unknown error'}`);
    }
  }, [setRepositoryData, setExpandedNodes, onCanvasSelect]);

  // ==================== WIZARD SAVE HANDLERS (WITH FOREIGN TABLE CREATION) ====================
  const handleSaveExcelMetadata = useCallback(async (metadata: ExcelMetadataFormData) => {
  if (!apiService) {
    toast.error('❌ Database API service is not initialized.', { autoClose: 5000 });
    return;
  }

  try {
    // 1. Ensure PostgreSQL connection is active
    console.log('🔍 [Excel] Checking PostgreSQL connection...');
    if (!isConnected) {
      const connected = await testConnection();
      if (!connected) {
        toast.error('❌ No active PostgreSQL connection. Please connect first.', { autoClose: 5000 });
        return;
      }
    }

    const connectionId = await getActivePostgresConnectionId(apiService);
    if (!connectionId) {
      toast.error('❌ No active PostgreSQL connection found.', { autoClose: 5000 });
      return;
    }
    console.log('✅ [Excel] Using connection ID:', connectionId);

    // 2. Validate that we have a file and a selected sheet
    if (!metadata.file) {
      toast.error('❌ No Excel file provided.', { autoClose: 5000 });
      return;
    }
    if (!metadata.selectedSheet) {
      toast.error('❌ No sheet selected.', { autoClose: 5000 });
      return;
    }
    console.log('📄 [Excel] File:', metadata.file.name, 'Size:', metadata.file.size, 'Sheet:', metadata.selectedSheet);

    // 3. Read the Excel file as ArrayBuffer
    console.log('📖 [Excel] Reading Excel file...');
    const fileReader = new FileReader();
    const fileBuffer = await new Promise<ArrayBuffer>((resolve, reject) => {
      fileReader.onload = () => resolve(fileReader.result as ArrayBuffer);
      fileReader.onerror = () => reject(fileReader.error);
      fileReader.readAsArrayBuffer(metadata.file);
    });
    console.log('✅ [Excel] File read complete, size:', fileBuffer.byteLength, 'bytes');

    // 4. Parse the workbook and extract the chosen sheet
    console.log('📊 [Excel] Parsing workbook...');
    const workbook = XLSX.read(new Uint8Array(fileBuffer), { type: 'array' });
    const worksheet = workbook.Sheets[metadata.selectedSheet];
    if (!worksheet) {
      toast.error(`❌ Sheet "${metadata.selectedSheet}" not found in workbook.`, { autoClose: 5000 });
      return;
    }
    console.log('✅ [Excel] Sheet found');

    // 5. Convert the sheet to CSV
    console.log('🔄 [Excel] Converting sheet to CSV...');
    const csvString = XLSX.utils.sheet_to_csv(worksheet, { FS: ',' });
    console.log('✅ [Excel] CSV conversion complete, length:', csvString.length, 'characters');

    // 6. Upload CSV to backend – use the original file name (without extension) as CSV filename
    const formData = new FormData();
    // Extract base name from the original Excel file (remove extension)
    const originalFileName = metadata.file.name;
    const baseName = originalFileName.replace(/\.[^/.]+$/, ""); // removes last dot extension
    // Sanitize the base name to be filesystem-safe
    const safeBaseName = baseName.replace(/[^a-z0-9]/gi, '_');
    const csvFileName = `${safeBaseName}.csv`;
    const csvBlob = new Blob([csvString], { type: 'text/csv' });
    formData.append('file', csvBlob, csvFileName);

    console.log('📤 [Excel] Uploading CSV to /api/upload-csv...');
    const uploadResponse = await fetch('http://localhost:3000/api/upload-csv', {
      method: 'POST',
      body: formData,
    });

    if (!uploadResponse.ok) {
      const errorText = await uploadResponse.text();
      console.error('❌ [Excel] Upload failed with status:', uploadResponse.status);
      console.error('❌ [Excel] Response body:', errorText);
      throw new Error(`CSV upload failed (${uploadResponse.status}): ${errorText}`);
    }

    const uploadResult = await uploadResponse.json();
    const csvPath = uploadResult.filePath;
    console.log('✅ [Excel] CSV uploaded successfully, path:', csvPath);

    // 7. Prepare column definitions (from wizard's inferred schema)
    const columns = metadata.schema.map(col => ({
      name: col.name,
      type: col.type,          // app type like 'String', 'Integer', etc.
      length: col.length,
      nullable: true,          // default; can be refined later
    }));
    console.log('📋 [Excel] Column definitions:', columns);

    // 8. Options for the delimited FDW – include the custom tag
    const options = {
      delimiter: ',',
      header: metadata.hasHeaders ? 'true' : 'false',
      format: 'csv',
      original_file_type: 'excel',   // tag for reverse‑engineering
    };

    // 9. Create the foreign table using the existing 'fdw_delimited' server
    console.log('🛠️ [Excel] Creating foreign table with name:', metadata.name);
    const result = await apiService.createForeignTable(
      connectionId,
      metadata.name,            // table name
      columns,
      'delimited',              // fileType – triggers fdw_delimited server
      csvPath,
      options
    );

    if (!result.success) {
      throw new Error(result.error || 'Foreign table creation failed');
    }

    console.log('✅ [Excel] Foreign table created successfully, result:', result);

    // 10. Store the resulting PostgreSQL table name in metadata
    metadata.postgresTableName = result.tableName || metadata.name;

    // --- NEW: Insert metadata record into data_source_metadata table ---
    const insertResult = await apiService.insertDataSourceMetadata(connectionId, {
      name: metadata.name,
      type: 'excel',
      filePath: csvPath,
      foreignTableName: result.tableName || metadata.name,
      options: {
        ...options,
        original_file_type: 'excel',
        sheet: metadata.selectedSheet,
        hasHeaders: metadata.hasHeaders,
        // include any other relevant metadata
      }
    });

    if (!insertResult.success) {
      // Log warning but don't fail the overall operation – the foreign table already exists.
      console.warn('⚠️ [Excel] Metadata insertion failed (non‑fatal):', insertResult.error);
      toast.warn(`Foreign table created, but metadata tracking failed: ${insertResult.error}`, { autoClose: 5000 });
    } else {
      console.log('✅ [Excel] Data source metadata recorded with ID:', insertResult.id);
    }
    // --- END NEW ---

    // 11. Dispatch event to add node to the repository (still under "Excel Files")
    window.dispatchEvent(new CustomEvent('metadata-created', {
      detail: {
        metadata,
        type: 'excel',
        folderId: 'file-excel'
      }
    }));

    toast.success(`✅ Excel foreign table "${metadata.name}" created successfully!`, { autoClose: 5000 });

  } catch (error: any) {
    console.error('❌ [Excel] Failed to create Excel foreign table:', error);
    toast.error(`❌ ${error.message || 'Could not create foreign table'}`, { autoClose: 5000 });
  }
}, [apiService, isConnected, testConnection]);

  // Helper to convert a file via a backend endpoint
  const convertFileViaBackend = useCallback(async (
    file: File,
    endpoint: string,
    additionalFormData?: Record<string, string>
  ): Promise<string> => {
    const formData = new FormData();
    formData.append('file', file);
    if (additionalFormData) {
      Object.entries(additionalFormData).forEach(([key, value]) => {
        formData.append(key, value);
      });
    }
    const response = await fetch(`http://localhost:3000${endpoint}`, {
      method: 'POST',
      body: formData,
    });
    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Conversion failed (${response.status}): ${errorText}`);
    }
    const result = await response.json();
    return result.filePath; // assume backend returns { filePath: string }
  }, []);

  // Delimited files are already text; just upload original file as CSV
  const handleSaveDelimitedMetadata = useCallback(async (metadata: any) => {
    if (!apiService) { toast.error('❌ API service not available'); return; }
    try {
      if (!isConnected && !(await testConnection())) {
        toast.error('❌ No active PostgreSQL connection');
        return;
      }
      const connectionId = await getActivePostgresConnectionId(apiService);
      if (!connectionId) throw new Error('No PostgreSQL connection');

      if (!metadata.file) { toast.error('❌ No file provided'); return; }

      // Upload original file as CSV
      const formData = new FormData();
      formData.append('file', metadata.file);
      const uploadRes = await fetch('http://localhost:3000/api/upload-csv', {
        method: 'POST',
        body: formData,
      });
      if (!uploadRes.ok) throw new Error('Upload failed');
      const { filePath } = await uploadRes.json();

      const columns = (metadata.schema || []).map((col: any) => ({
        name: col.name,
        type: col.type,
        length: col.length,
        nullable: true,
      }));
      if (columns.length === 0) { toast.error('❌ No columns defined'); return; }

      const options = {
        delimiter: metadata.delimiter || ',',
        header: metadata.hasHeaders ? 'true' : 'false',
        format: 'csv',
      };

      const result = await apiService.createForeignTable(
        connectionId,
        metadata.name,
        columns,
        'delimited',
        filePath,
        options
      );
      if (!result.success) throw new Error(result.error);

      metadata.postgresTableName = result.tableName || metadata.name;

      await apiService.insertDataSourceMetadata(connectionId, {
        name: metadata.name,
        type: 'delimited',
        filePath,
        foreignTableName: result.tableName || metadata.name,
        options,
      });

      window.dispatchEvent(new CustomEvent('metadata-created', {
        detail: { metadata, type: 'delimited', folderId: 'file-delimited' }
      }));
      toast.success(`✅ Delimited table "${metadata.name}" created`);
    } catch (error: any) {
      toast.error(`❌ ${error.message}`);
    }
  }, [apiService, isConnected, testConnection]);

// Utility function to send errors to Consul (implement as needed)
const reportErrorToConsul = (error: any) => {
  // Example: send error message and context to Consul
  // You might use fetch to POST to Consul's API or a custom endpoint
  console.error('Reporting to Consul:', error); // Placeholder
};

// Modified function
const handleSaveXMLMetadata = useCallback(async (metadata: any) => {
  if (!apiService) {
    reportErrorToConsul('❌ API service not available');
    return;
  }
  try {
    if (!isConnected && !(await testConnection())) {
      reportErrorToConsul('❌ No active PostgreSQL connection');
      return;
    }
    const connectionId = await getActivePostgresConnectionId(apiService);
    if (!connectionId) throw new Error('No PostgreSQL connection');

    if (!metadata.file) {
      reportErrorToConsul('❌ No file provided');
      return;
    }

    // Convert XML to CSV via backend
    const csvPath = await convertFileViaBackend(
      metadata.file,
      '/api/convert/xml',
      {
        rowXPath: metadata.rowXPath || '',
        columns: JSON.stringify(metadata.schema || []),
      }
    );

    const columns = (metadata.schema || []).map((col: any) => ({
      name: col.name,
      type: col.type,
      nullable: true,
    }));
    if (columns.length === 0) {
      reportErrorToConsul('❌ No columns defined');
      return;
    }

    const options = { format: 'xml' };

    const result = await apiService.createForeignTable(
      connectionId,
      metadata.name,
      columns,
      'xml',
      csvPath,
      options
    );
    if (!result.success) throw new Error(result.error);

    metadata.postgresTableName = result.tableName || metadata.name;

    await apiService.insertDataSourceMetadata(connectionId, {
      name: metadata.name,
      type: 'xml',
      filePath: csvPath,
      foreignTableName: result.tableName || metadata.name,
      options,
    });

    window.dispatchEvent(new CustomEvent('metadata-created', {
      detail: { metadata, type: 'xml', folderId: 'file-xml' }
    }));
    toast.success(`✅ XML table "${metadata.name}" created`);
  } catch (error: any) {
    reportErrorToConsul(`❌ ${error.message}`);
  }
}, [apiService, isConnected, testConnection, convertFileViaBackend]);

  // Positional: convert via backend
 const handleSavePositionalMetadata = useCallback(async (metadata: any) => {
  if (!apiService) {
    reportErrorToConsul('❌ API service not available');
    return;
  }
  try {
    if (!isConnected && !(await testConnection())) {
      reportErrorToConsul('❌ No active PostgreSQL connection');
      return;
    }
    const connectionId = await getActivePostgresConnectionId(apiService);
    if (!connectionId) throw new Error('No PostgreSQL connection');

    if (!metadata.file) {
      reportErrorToConsul('❌ No file provided');
      return;
    }

    // Convert positional file to CSV
    const csvPath = await convertFileViaBackend(
      metadata.file,
      '/api/convert/positional',
      {
        columns: JSON.stringify(metadata.schema || []), // ✅ fixed property
      }
    );

    // Map columns for foreign table creation
    const mapDataType = (type: string): string => {
      const typeMap: Record<string, string> = {
        'String': 'TEXT',
        'Integer': 'INTEGER',
        'Decimal': 'NUMERIC',
        'Date': 'DATE',
        'Boolean': 'BOOLEAN',
      };
      return typeMap[type] || 'TEXT';
    };

    const columns = (metadata.schema || []).map((col: any) => ({
      name: col.name,
      type: mapDataType(col.type),
      length: col.length,
      nullable: true,
    }));

    if (columns.length === 0) {
      reportErrorToConsul('❌ No columns defined');
      return;
    }

    const options = { format: 'fixed' };

    const result = await apiService.createForeignTable(
      connectionId,
      metadata.name,
      columns,
      'positional',
      csvPath,
      options
    );
    if (!result.success) throw new Error(result.error);

    metadata.postgresTableName = result.tableName || metadata.name;

    await apiService.insertDataSourceMetadata(connectionId, {
      name: metadata.name,
      type: 'positional',
      filePath: csvPath,
      foreignTableName: result.tableName || metadata.name,
      options,
    });

    window.dispatchEvent(
      new CustomEvent('metadata-created', {
        detail: { metadata, type: 'positional', folderId: 'file-positional' },
      })
    );
    toast.success(`✅ Positional table "${metadata.name}" created`);
  } catch (error: any) {
    reportErrorToConsul(`❌ ${error.message}`);
  }
}, [apiService, isConnected, testConnection, convertFileViaBackend]);
  // File Schema: likely a combination of schema file + data file. We'll assume metadata contains both.
  const handleSaveFileSchemaMetadata = useCallback(async (metadata: any) => {
    if (!apiService) { toast.error('❌ API service not available'); return; }
    try {
      if (!isConnected && !(await testConnection())) {
        toast.error('❌ No active PostgreSQL connection');
        return;
      }
      const connectionId = await getActivePostgresConnectionId(apiService);
      if (!connectionId) throw new Error('No PostgreSQL connection');

      if (!metadata.schemaFile || !metadata.dataFile) {
        toast.error('❌ Both schema and data files required');
        return;
      }

      // Upload both files as multipart
      const formData = new FormData();
      formData.append('schemaFile', metadata.schemaFile);
      formData.append('dataFile', metadata.dataFile);
      formData.append('dataFormat', metadata.dataFormat || 'delimited');
      if (metadata.delimiter) formData.append('delimiter', metadata.delimiter);
      // etc.

      const response = await fetch('http://localhost:3000/api/convert/schema', {
        method: 'POST',
        body: formData,
      });
      if (!response.ok) throw new Error('Schema conversion failed');
      const { filePath } = await response.json();

      const columns = (metadata.columns || []).map((col: any) => ({
        name: col.name,
        type: col.type,
        nullable: true,
      }));
      if (columns.length === 0) { toast.error('❌ No columns defined'); return; }

      const options = { format: metadata.dataFormat || 'csv' };

      const result = await apiService.createForeignTable(
        connectionId,
        metadata.name,
        columns,
        'schema', // or appropriate type? The FDW might be 'fdw_schema'
        filePath,
        options
      );
      if (!result.success) throw new Error(result.error);

      metadata.postgresTableName = result.tableName || metadata.name;

      await apiService.insertDataSourceMetadata(connectionId, {
        name: metadata.name,
        type: 'schema',
        filePath,
        foreignTableName: result.tableName || metadata.name,
        options,
      });

      window.dispatchEvent(new CustomEvent('metadata-created', {
        detail: { metadata, type: 'schema', folderId: 'file-schema' }
      }));
      toast.success(`✅ Schema table "${metadata.name}" created`);
    } catch (error: any) {
      toast.error(`❌ ${error.message}`);
    }
  }, [apiService, isConnected, testConnection]);

  // JSON/Avro/Parquet
  const handleSaveJsonAvroParquetMetadata = useCallback(async (metadata: any) => {
  if (!apiService) {
    toast.error('❌ API service not available');
    return;
  }
  try {
    if (!isConnected && !(await testConnection())) {
      toast.error('❌ No active PostgreSQL connection');
      return;
    }
    const connectionId = await getActivePostgresConnectionId(apiService);
    if (!connectionId) throw new Error('No PostgreSQL connection');

    if (!metadata.file) {
      toast.error('❌ No file provided');
      return;
    }

    const format = metadata.format || 'json'; // json/avro/parquet
    // ✅ FIXED: arguments now (file, endpoint, additionalFormData)
    const csvPath = await convertFileViaBackend(
      metadata.file,
      '/api/convert/structured',
      { format }
    );

    const columns = (metadata.schema || []).map((col: any) => ({
      name: col.name,
      type: col.type,
      nullable: true,
    }));
    if (columns.length === 0) {
      toast.error('❌ No columns defined');
      return;
    }

    const options = { format };

    const result = await apiService.createForeignTable(
      connectionId,
      metadata.name,
      columns,
      'json-avro-parquet', // this maps to fdw_multiformat
      csvPath,
      options
    );
    if (!result.success) throw new Error(result.error);

    metadata.postgresTableName = result.tableName || metadata.name;

    await apiService.insertDataSourceMetadata(connectionId, {
      name: metadata.name,
      type: format,
      filePath: csvPath,
      foreignTableName: result.tableName || metadata.name,
      options,
    });

    window.dispatchEvent(
      new CustomEvent('metadata-created', {
        detail: { metadata, type: format, folderId: 'file-json-avro-parquet' },
      })
    );
    toast.success(`✅ ${format.toUpperCase()} table "${metadata.name}" created`);
  } catch (error: any) {
    toast.error(`❌ ${error.message}`);
  }
}, [apiService, isConnected, testConnection, convertFileViaBackend]);

  // Regex
  const handleSaveRegexMetadata = useCallback(async (metadata: any) => {
  if (!apiService) {
    toast.error('❌ API service not available');
    return;
  }
  try {
    if (!isConnected && !(await testConnection())) {
      toast.error('❌ No active PostgreSQL connection');
      return;
    }
    const connectionId = await getActivePostgresConnectionId(apiService);
    if (!connectionId) throw new Error('No PostgreSQL connection');

    if (!metadata.file) {
      toast.error('❌ No file provided');
      return;
    }

    // ✅ FIXED: arguments now (file, endpoint, additionalFormData)
    const csvPath = await convertFileViaBackend(
      metadata.file,
      '/api/convert/regex',
      {
        pattern: metadata.pattern,
        flags: metadata.flags || '',
        columns: JSON.stringify(metadata.columns || []), // optional column names
      }
    );

    const columns = (metadata.columns || []).map((col: any) => ({
      name: col.name,
      type: col.type || 'text',
      nullable: true,
    }));
    if (columns.length === 0) {
      toast.error('❌ No columns defined');
      return;
    }

    const options = { pattern: metadata.pattern, flags: metadata.flags };

    const result = await apiService.createForeignTable(
      connectionId,
      metadata.name,
      columns,
      'regex',
      csvPath,
      options
    );
    if (!result.success) throw new Error(result.error);

    metadata.postgresTableName = result.tableName || metadata.name;

    await apiService.insertDataSourceMetadata(connectionId, {
      name: metadata.name,
      type: 'regex',
      filePath: csvPath,
      foreignTableName: result.tableName || metadata.name,
      options,
    });

    window.dispatchEvent(
      new CustomEvent('metadata-created', {
        detail: { metadata, type: 'regex', folderId: 'file-regex' },
      })
    );
    toast.success(`✅ Regex table "${metadata.name}" created`);
  } catch (error: any) {
    toast.error(`❌ ${error.message}`);
  }
}, [apiService, isConnected, testConnection, convertFileViaBackend]);
  // LDIF
  const handleSaveLDIFMetadata = useCallback(async (metadata: any) => {
  if (!apiService) {
    reportErrorToConsul('❌ API service not available');
    return;
  }
  try {
    if (!isConnected && !(await testConnection())) {
      toast.error('❌ No active PostgreSQL connection');
      return;
    }
    const connectionId = await getActivePostgresConnectionId(apiService);
    if (!connectionId) throw new Error('No PostgreSQL connection');

    if (!metadata.file) {
      toast.error('❌ No file provided');
      return;
    }

    // ✅ FIXED: arguments now (file, endpoint, additionalFormData)
    const csvPath = await convertFileViaBackend(
      metadata.file,
      '/api/convert/ldif',
      {} // may pass additional parameters if needed
    );

    const columns = (metadata.columns || []).map((col: any) => ({
      name: col.name,
      type: col.type || 'text',
      nullable: true,
    }));
    if (columns.length === 0) {
      toast.error('❌ No columns defined');
      return;
    }

    const options = {};

    const result = await apiService.createForeignTable(
      connectionId,
      metadata.name,
      columns,
      'ldif',
      csvPath,
      options
    );
    if (!result.success) throw new Error(result.error);

    metadata.postgresTableName = result.tableName || metadata.name;

    await apiService.insertDataSourceMetadata(connectionId, {
      name: metadata.name,
      type: 'ldif',
      filePath: csvPath,
      foreignTableName: result.tableName || metadata.name,
      options,
    });

    window.dispatchEvent(
      new CustomEvent('metadata-created', {
        detail: { metadata, type: 'ldif', folderId: 'file-ldif' },
      })
    );
    toast.success(`✅ LDIF table "${metadata.name}" created`);
  } catch (error: any) {
    toast.error(`❌ ${error.message}`);
  }
}, [apiService, isConnected, testConnection, convertFileViaBackend]);


function createDatabaseTableNode(
  tableDef: any,
  localTableName: string,
  connectionId: string,
  remoteDbType: string,
  remoteConn: any,
  schema: string
): RepositoryNode {
  const columns = (tableDef.columns || []).map((col: any) => ({
    name: col.name,
    dataType: col.type,
    type: col.type,
    length: col.length,
    precision: col.precision,
    scale: col.scale,
    nullable: col.nullable ?? true,
    defaultValue: col.defaultValue,
  }));

  const nodeId = `${remoteDbType}-${Date.now()}-${Math.random().toString(36).substring(2, 8)}`;

  return {
    id: nodeId,
    name: tableDef.tablename, // original remote table name
    type: 'item',
    metadata: {
      postgresTableName: localTableName,
      remoteDbType,
      remoteConnection: remoteConn,
      remoteTable: tableDef.tablename,
      remoteSchema: schema,
      columns,
      connection: { connectionId },
      createdAt: new Date().toISOString(),
      lastModified: new Date().toISOString(),
    },
    draggable: true,
    droppable: false,
  };
}

  // Database (already existing, keep as is)
// Inside RepositorySidebar component

const handleSaveDatabaseMetadata = useCallback(async (metadata: any) => {
  if (!apiService) {
    toast.error('❌ Database API service is not initialized.');
    return;
  }

  try {
    if (!isConnected && !(await testConnection())) {
      toast.error('❌ No active PostgreSQL connection.');
      return;
    }

    const connectionId = await getActivePostgresConnectionId(apiService);
    if (!connectionId) {
      toast.error('❌ No active PostgreSQL connection found.');
      return;
    }

    const remoteDbType = metadata.dbType;
    const remoteConn = metadata.connection;
    const allTables = metadata.tables || [];
    const selectedTableIds = metadata.selectedTables || [];

    if (selectedTableIds.length === 0) {
      toast.error('❌ No tables selected.');
      return;
    }

    let createdCount = 0;
    const createdTableNodes: RepositoryNode[] = [];

    for (const tableId of selectedTableIds) {
      const [schema, tableName] = tableId.includes('.') ? tableId.split('.') : ['public', tableId];
      const tableDef = allTables.find(
        (t: any) => `${t.schemaname}.${t.tablename}` === tableId || t.tablename === tableName
      );

      if (!tableDef) {
        console.warn(`Table definition not found for ${tableId}, skipping.`);
        continue;
      }

      const columns = tableDef.columns.map((col: any) => ({
        name: col.name,
        type: col.type,
        length: col.length,
        precision: col.precision,
        scale: col.scale,
        nullable: col.nullable ?? true,
        defaultValue: col.defaultValue,
      }));

      if (columns.length === 0) {
        toast.error(`❌ No columns defined for table ${tableName}.`);
        continue;
      }

      const options: Record<string, string> = {
        host: remoteConn.host || 'localhost',
        port: String(remoteConn.port || '5432'),
        dbname: remoteConn.dbname || '',
        table_name: tableName,
        user: remoteConn.user || '',
        password: remoteConn.password || '',
      };
      if (schema && schema !== 'public') {
        options.schema_name = schema;
      }

      const localTableName = `${metadata.name}_${tableName}`.toLowerCase().replace(/[^a-z0-9_]/g, '_');

      const result = await apiService.createForeignTable(
        connectionId,
        localTableName,
        columns,
        remoteDbType,
        '', // filePath not needed for database source
        options
      );

      if (!result.success) {
        throw new Error(`Failed to create foreign table for ${tableName}: ${result.error}`);
      }

      const tableNode = createDatabaseTableNode(
        tableDef,
        result.tableName || localTableName,
        connectionId,
        remoteDbType,
        remoteConn,
        schema
      );
      createdTableNodes.push(tableNode);
      createdCount++;
    }

    if (createdCount === 0) {
      toast.error('❌ No tables were created.');
      return;
    }

    // Update repository tree (create folder for the connection, add table nodes)
    let newParentFolderId: string | null = null;

    setRepositoryData(prev => {
      const folderId = 'db-connections';
      const dbConnectionsNode = findNodeAndParent(prev, folderId)?.node;
      if (!dbConnectionsNode) return prev;

      const connectionFolderName = metadata.name;
      let parentFolder = dbConnectionsNode.children?.find(
        child => child.name === connectionFolderName && child.type === 'folder'
      );

      if (!parentFolder) {
        const newFolderId = `folder-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
        parentFolder = {
          id: newFolderId,
          name: connectionFolderName,
          type: 'folder',
          children: [],
          parentId: folderId,
          metadata: {
            connectionId,
            dbType: remoteDbType,
            connectionDetails: remoteConn,
            createdAt: new Date().toISOString(),
          },
        };
        newParentFolderId = newFolderId;
        prev = updateNodeInTree(prev, folderId, node => ({
          ...node,
          children: [...(node.children || []), parentFolder],
          metadata: { ...node.metadata, count: (node.metadata?.count || 0) + 1 },
        }));
      }

      for (const tableNode of createdTableNodes) {
        const exists = parentFolder.children?.some(child => child.id === tableNode.id);
        if (!exists) {
          prev = updateNodeInTree(prev, parentFolder.id, node => ({
            ...node,
            children: [...(node.children || []), tableNode],
            metadata: { ...node.metadata, count: (node.metadata?.count || 0) + 1 },
          }));
        }
      }

      return prev;
    });

    if (newParentFolderId) {
      setExpandedNodes(prev => new Set([...prev, newParentFolderId]));
    }

    // ✅ NEW: Save metadata to the new schema tables
    const metadataToSave = {
      name: metadata.name,
      purpose: metadata.purpose,
      description: metadata.description,
      dbType: remoteDbType,
      connectionId: metadata.useExistingConnection ? metadata.selectedConnectionId : undefined,
      connection: metadata.useExistingConnection ? undefined : {
        host: remoteConn.host,
        port: remoteConn.port,
        dbname: remoteConn.dbname,
        user: remoteConn.user,
        password: remoteConn.password,
        schema: remoteConn.schema,
      },
      tables: allTables,
      selectedTables: selectedTableIds,
      tableSelections: metadata.tableSelections,
    };

    const saveResult = await apiService.saveDatabaseMetadata(metadataToSave);
    if (!saveResult.success) {
      toast.warn(`⚠️ Foreign tables created, but metadata storage failed: ${saveResult.error}`);
      console.warn('Metadata storage error:', saveResult.error);
    } else {
      console.log('✅ Database metadata saved with ID:', saveResult.metadataEntryId);
    }

    toast.success(`✅ Created ${createdCount} foreign table(s).`);
  } catch (error: any) {
    console.error('❌ Failed to create database foreign tables:', error);
    toast.error(`❌ ${error.message || 'Could not create foreign tables'}`);
  }
}, [apiService, isConnected, testConnection, setRepositoryData, setExpandedNodes]);

  const handleSaveWebServiceMetadata = useCallback(async (metadata: any) => {
    window.dispatchEvent(new CustomEvent('metadata-created', {
      detail: {
        metadata,
        type: 'web-service',
        folderId: 'web-service'
      }
    }));
    toast.success(`✅ Web service "${metadata.name}" saved`, { autoClose: 3000 });
  }, []);

  // ============================================================================
  // ✅ FIXED: INITIAL SYNC FROM POSTGRESQL – now works with nested categories
  // ============================================================================
useEffect(() => {
  if (!apiService || !isConnected || isSyncing) {
    console.log('⏭️ Sync skipped:', { hasApiService: !!apiService, isConnected, isSyncing });
    return;
  }

  const syncExistingForeignTables = async () => {
    setIsSyncing(true);
    console.log('🔄 Starting foreign table synchronization (with data_source_metadata lookup)...');

    try {
      const connectionId = await getActivePostgresConnectionId(apiService);
      if (!connectionId) {
        console.warn('⚠️ No active PostgreSQL connection found, sync aborted');
        return;
      }
      console.log('🔌 Using connection ID:', connectionId);

      const schema = 'public';
      console.log(`🔍 Fetching foreign tables from schema "${schema}"...`);

      const tables = await getForeignTablesInSchema(apiService, connectionId, schema);
      console.log(`📋 Found ${tables.length} foreign tables:`, tables.map(t => t.tablename));

      if (tables.length === 0) {
        console.log('📭 No foreign tables to sync');
        return;
      }

      const metadataPromises = tables.map(async (table) => {
        try {
          const meta = await reverseForeignTableMetadata(apiService, connectionId, schema, table.tablename);
          console.log(`🔍 Before override: ${table.tablename} fileType = ${meta.fileType}`);

          // --- Look up original type from data_source_metadata (ignore connection_id) ---
          try {
            const sql = `SELECT type, file_path, options
                         FROM data_source_metadata
                         WHERE foreign_table_name = $1`; // No connection_id filter
            const params = [meta.tableName];
            console.log(`🔎 Executing metadata lookup for ${meta.tableName} with SQL:`, sql, 'params:', params);

            const lookupResult = await apiService.executeQuery(connectionId, sql, { params });
            console.log(`📦 Raw lookupResult for ${meta.tableName}:`, lookupResult);

            if (lookupResult.success) {
              let rows: any[] = [];
              if (Array.isArray(lookupResult.rows)) {
                rows = lookupResult.rows;
                console.log(`📊 Extracted rows from lookupResult.rows (length ${rows.length})`);
              } else if (lookupResult.result && Array.isArray(lookupResult.result)) {
                rows = lookupResult.result;
                console.log(`📊 Extracted rows from lookupResult.result (length ${rows.length})`);
              } else if (lookupResult.result?.rows && Array.isArray(lookupResult.result.rows)) {
                rows = lookupResult.result.rows;
                console.log(`📊 Extracted rows from lookupResult.result.rows (length ${rows.length})`);
              } else {
                console.log(`❓ No array found in any expected location. Full structure:`, lookupResult);
              }

              if (rows.length > 0) {
                const stored = rows[0];
                console.log(`📦 Found stored metadata for ${meta.tableName}:`, stored);
                console.log(`   stored.type = "${stored.type}"`);
                meta.fileType = stored.type;
                if (stored.file_path) meta.filePath = stored.file_path;
                if (stored.options) meta.options = { ...meta.options, ...stored.options };
                console.log(`✅ After override: ${meta.tableName} fileType set to "${meta.fileType}"`);
              } else {
                console.log(`ℹ️ No stored metadata rows found for ${meta.tableName} – using inferred type`);
              }
            } else {
              console.warn(`⚠️ Metadata lookup failed for ${meta.tableName}: ${lookupResult.error}`);
            }
          } catch (lookupErr: any) {
            if (lookupErr.message?.includes('relation "data_source_metadata" does not exist')) {
              console.warn('⚠️ data_source_metadata table not found. Run ./standalone-pg-builder.sh --setup-tables');
            } else {
              console.warn(`⚠️ Unexpected error during metadata lookup for ${meta.tableName}:`, lookupErr.message);
            }
          }
          // --- End lookup ---

          console.log(`✅ Final for ${table.tablename}: fileType = ${meta.fileType}`);
          return { ...meta, oid: table.oid };
        } catch (err) {
          console.error(`❌ Failed to reverse metadata for ${table.tablename}:`, err);
          return null;
        }
      });

      const metadataList = await Promise.all(metadataPromises);
      const validMetadata = metadataList.filter((m): m is NonNullable<typeof m> & { oid: number } => m !== null);

      if (validMetadata.length === 0) {
        console.log('📭 No valid metadata could be retrieved');
        return;
      }

      const updates: Array<{ folderId: string; newNode: RepositoryNode }> = [];
      const foldersToExpand = new Set<string>();

      validMetadata.forEach((meta) => {
        const folderId = determineFolderId(meta.fileType);
        console.log(`🗂️ For table ${meta.tableName} with fileType "${meta.fileType}" → folderId = ${folderId}`);
        if (!folderId) {
          console.warn(`Unknown file type "${meta.fileType}" for table ${meta.tableName}, skipping`);
          return;
        }

        foldersToExpand.add(folderId);
        const newNode = createNodeFromMetadata(meta, folderId, connectionId);
        updates.push({ folderId, newNode });
        console.log(`✅ Prepared node for ${meta.tableName} -> folder ${folderId}`);
      });

      setRepositoryData((prev) => {
        let newTree = prev;
        updates.forEach(({ folderId, newNode }) => {
          const found = findNodeAndParent(newTree, folderId);
          if (!found) {
            console.warn(`⚠️ Category folder "${folderId}" not found in repository tree, skipping`);
            return;
          }
          newTree = updateNodeInTree(newTree, folderId, (folder) => {
            const existingNames = new Set(folder.children.map((c) => c.name));
            if (existingNames.has(newNode.name)) {
              console.log(`⏭️ Node with name "${newNode.name}" already exists in ${folderId}, skipping`);
              return folder;
            }
            return {
              ...folder,
              children: [...folder.children, newNode].sort((a, b) => a.name.localeCompare(b.name)),
              metadata: { ...folder.metadata, count: (folder.metadata?.count || 0) + 1 }
            };
          });
        });
        return newTree;
      });

      setExpandedNodes((prev) => {
        const next = new Set(prev);
        foldersToExpand.forEach((id) => next.add(id));
        console.log('🔽 Expanded nodes after sync:', Array.from(next));
        return next;
      });

      console.log('✅ Foreign table synchronization complete');
    } catch (error) {
      console.error('❌ Sync failed with unexpected error:', error);
    } finally {
      setIsSyncing(false);
    }
  };

  syncExistingForeignTables();
}, [apiService, isConnected, setRepositoryData]);


useEffect(() => {
  if (!databaseContext.apiService || !databaseContext.isConnected) return;

  const loadDatabaseMetadataEntries = async () => {
    try {
      const result = await databaseContext.apiService.getDatabaseMetadataEntries();
      if (!result.success || !result.entries) {
        console.warn('Failed to load metadata entries:', result.error);
        return;
      }

      console.log(`📚 Loaded ${result.entries.length} database metadata entries`);

      // Build nodes for each entry
      const newFolderNodes: RepositoryNode[] = [];

      for (const entry of result.entries) {
        const folderId = `db-folder-${entry.id}`;
        const folderName = entry.name;

        // Build table nodes with columns and ordering
        const tableNodes: RepositoryNode[] = entry.tables.map((table: any) => {
          const columns = table.columns.map((col: any) => ({
            name: col.name,
            type: col.dataType,
            dataType: col.dataType,
            length: col.length,
            precision: col.precision,
            scale: col.scale,
            nullable: col.nullable,
            defaultValue: col.defaultValue,
            position: col.position,
          }));

          return {
            id: `db-table-${entry.id}-${table.id}`,
            name: table.tableName,
            type: 'item',
            draggable: true,
            droppable: false,
            metadata: {
              entryId: entry.id,
              tableId: table.id,
              schemaName: table.schemaName,
              tableName: table.tableName,
              tableType: table.tableType,
              columns: columns,
              postgresTableName: table.tableName, // actual foreign table name (same as table name)
              connection: {
                connectionId: entry.connection.id,
                host: entry.connection.host,
                port: entry.connection.port,
                dbname: entry.connection.databaseName,
                user: entry.connection.username,
                schema: entry.connection.schema,
              },
              dbType: entry.dbType,
              position: table.position, // store position for sorting
            },
            parentId: folderId,
          };
        });

        // Sort table nodes by stored position
        tableNodes.sort((a, b) => (a.metadata?.position || 0) - (b.metadata?.position || 0));

        // Create folder node
        const folderNode: RepositoryNode = {
          id: folderId,
          name: folderName,
          type: 'folder',
          children: tableNodes,
          draggable: false,
          droppable: true,
          metadata: {
            entryId: entry.id,
            connectionId: entry.connection.id,
            connectionDetails: entry.connection,
            dbType: entry.dbType,
            createdAt: entry.createdAt,
            updatedAt: entry.updatedAt,
          },
        };

        newFolderNodes.push(folderNode);
      }

      // Merge into repositoryData
      setRepositoryData(prev => {
        const dbConnectionsIndex = prev.findIndex(n => n.id === 'db-connections');
        if (dbConnectionsIndex === -1) return prev;

        const dbConnectionsNode = prev[dbConnectionsIndex];
        // Remove any existing folder nodes that came from metadata entries (by checking metadata.entryId)
        const existingChildren = dbConnectionsNode.children || [];
        const filteredChildren = existingChildren.filter(child => {
          // Keep system folders or items without entryId metadata
          return !(child.metadata && child.metadata.entryId);
        });

        // Add new folder nodes (skip duplicates)
        const newChildren = [...filteredChildren];
        for (const folderNode of newFolderNodes) {
          const exists = newChildren.some(c => c.metadata?.entryId === folderNode.metadata?.entryId);
          if (!exists) {
            newChildren.push(folderNode);
          }
        }

        // Sort folder nodes alphabetically (or by creation date if preferred)
        newChildren.sort((a, b) => a.name.localeCompare(b.name));

        const updatedDbConnectionsNode = {
          ...dbConnectionsNode,
          children: newChildren,
          metadata: {
            ...dbConnectionsNode.metadata,
            count: newChildren.length,
          },
        };

        const newTree = [...prev];
        newTree[dbConnectionsIndex] = updatedDbConnectionsNode;
        return newTree;
      });

    } catch (error) {
      console.error('Error loading metadata entries:', error);
    }
  };

  loadDatabaseMetadataEntries();
}, [databaseContext.apiService, databaseContext.isConnected, setRepositoryData]);
  // ==================== METADATA EXPANSION ON NEW NODES ====================
  useEffect(() => {
    if (metadataSettings.autoExpandNew) {
      const newNodesWithMetadata = repositoryData
        .flatMap(node => node.children || [])
        .filter(child => child.metadata && !expandedMetadataNodes.has(child.id));

      if (newNodesWithMetadata.length > 0) {
        setExpandedMetadataNodes(prev => {
          const newSet = new Set(prev);
          newNodesWithMetadata.forEach(node => newSet.add(node.id));
          return newSet;
        });
      }
    }
  }, [repositoryData, metadataSettings.autoExpandNew, expandedMetadataNodes]);

  useEffect(() => {
    if (activeCanvasDesignId !== undefined) {
      setActiveDesignId(activeCanvasDesignId);
    }
  }, [activeCanvasDesignId]);

  useEffect(() => {
    console.log('📊 repositoryData changed. Excel children count:',
      repositoryData.find(n => n.id === 'metadata')?.children?.find(c => c.id === 'file-excel')?.children?.length ?? 0,
      '\nFull repositoryData:', repositoryData
    );
  }, [repositoryData]);

  useEffect(() => {
  if (!databaseContext.apiService || !databaseContext.isConnected) return;

  const loadDatabaseMetadataEntries = async () => {
    try {
      const result = await databaseContext.apiService.getDatabaseMetadataEntries();
      if (!result.success || !result.entries) {
        console.warn('Failed to load metadata entries:', result.error);
        return;
      }

      console.log(`📚 Loaded ${result.entries.length} database metadata entries`);

      // Build nodes for each entry
      const newFolderNodes: RepositoryNode[] = [];

      for (const entry of result.entries) {
        const folderId = `db-folder-${entry.id}`;
        const folderName = entry.name;

        // Build table nodes with columns and ordering
        const tableNodes: RepositoryNode[] = entry.tables.map((table: any) => {
          const columns = table.columns.map((col: any) => ({
            name: col.name,
            type: col.dataType,
            dataType: col.dataType,
            length: col.length,
            precision: col.precision,
            scale: col.scale,
            nullable: col.nullable,
            defaultValue: col.defaultValue,
            position: col.position,
          }));

          return {
            id: `db-table-${entry.id}-${table.id}`,
            name: table.tableName,
            type: 'item',
            draggable: true,
            droppable: false,
            metadata: {
              entryId: entry.id,
              tableId: table.id,
              schemaName: table.schemaName,
              tableName: table.tableName,
              tableType: table.tableType,
              columns: columns,
              postgresTableName: table.tableName, // actual foreign table name (same as table name)
              connection: {
                connectionId: entry.connection.id,
                host: entry.connection.host,
                port: entry.connection.port,
                dbname: entry.connection.databaseName,
                user: entry.connection.username,
                schema: entry.connection.schema,
              },
              dbType: entry.dbType,
              position: table.position, // store position for sorting
            },
            parentId: folderId,
          };
        });

        // Sort table nodes by stored position
        tableNodes.sort((a, b) => (a.metadata?.position || 0) - (b.metadata?.position || 0));

        // Create folder node
        const folderNode: RepositoryNode = {
          id: folderId,
          name: folderName,
          type: 'folder',
          children: tableNodes,
          draggable: false,
          droppable: true,
          metadata: {
            entryId: entry.id,
            connectionId: entry.connection.id,
            connectionDetails: entry.connection,
            dbType: entry.dbType,
            createdAt: entry.createdAt,
            updatedAt: entry.updatedAt,
          },
        };

        newFolderNodes.push(folderNode);
      }

      // Merge into repositoryData
      setRepositoryData(prev => {
        const dbConnectionsIndex = prev.findIndex(n => n.id === 'db-connections');
        if (dbConnectionsIndex === -1) return prev;

        const dbConnectionsNode = prev[dbConnectionsIndex];
        // Remove any existing folder nodes that came from metadata entries (by checking metadata.entryId)
        const existingChildren = dbConnectionsNode.children || [];
        const filteredChildren = existingChildren.filter(child => {
          // Keep system folders or items without entryId metadata
          return !(child.metadata && child.metadata.entryId);
        });

        // Add new folder nodes (skip duplicates)
        const newChildren = [...filteredChildren];
        for (const folderNode of newFolderNodes) {
          const exists = newChildren.some(c => c.metadata?.entryId === folderNode.metadata?.entryId);
          if (!exists) {
            newChildren.push(folderNode);
          }
        }

        // Sort folder nodes alphabetically (or by creation date if preferred)
        newChildren.sort((a, b) => a.name.localeCompare(b.name));

        const updatedDbConnectionsNode = {
          ...dbConnectionsNode,
          children: newChildren,
          metadata: {
            ...dbConnectionsNode.metadata,
            count: newChildren.length,
          },
        };

        const newTree = [...prev];
        newTree[dbConnectionsIndex] = updatedDbConnectionsNode;
        return newTree;
      });

    } catch (error) {
      console.error('Error loading metadata entries:', error);
    }
  };

  loadDatabaseMetadataEntries();
}, [databaseContext.apiService, databaseContext.isConnected, setRepositoryData]);

useEffect(() => {
  const apiService = databaseContext.apiService;
  const isConnected = databaseContext.isConnected;
  if (!apiService || !isConnected) {
    console.log('[Metadata Load] Skipped: apiService or connection not ready');
    return;
  }

  const loadDatabaseMetadataEntries = async () => {
    console.log('[Metadata Load] Starting load...');

    try {
      console.log('[Metadata Load] Fetching entries from /api/database/metadata');
      const result = await apiService.getDatabaseMetadataEntries();
      console.log('[Metadata Load] API response:', result);

      if (!result.success) {
        console.warn('[Metadata Load] API failed:', result.error);
        return;
      }

      if (!result.entries || result.entries.length === 0) {
        console.log('[Metadata Load] No entries found');
        return;
      }

      console.log(`[Metadata Load] Received ${result.entries.length} entries`);
      if (result.entries.length > 0) {
        console.log('[Metadata Load] Sample entry:', {
          id: result.entries[0].id,
          name: result.entries[0].name,
          tablesCount: result.entries[0].tables?.length,
          firstTable: result.entries[0].tables?.[0]?.tableName,
        });
      }

      const newFolderNodes: RepositoryNode[] = [];

      for (const entry of result.entries) {
        console.log(`[Metadata Load] Building folder for entry: ${entry.name} (id: ${entry.id})`);
        const folderId = `db-folder-${entry.id}`;

        const tableNodes: RepositoryNode[] = (entry.tables || []).map((table: any, idx: number) => {
          const columns = (table.columns || []).map((col: any) => ({
            name: col.name,
            type: col.dataType,
            dataType: col.dataType,
            length: col.length,
            precision: col.precision,
            scale: col.scale,
            nullable: col.nullable,
            defaultValue: col.defaultValue,
            position: col.position,
          }));

          return {
            id: `db-table-${entry.id}-${table.id || idx}`,
            name: table.tableName,
            type: 'item' as const,
            draggable: true,
            droppable: false,
            metadata: {
              entryId: entry.id,
              tableId: table.id,
              schemaName: table.schemaName,
              tableName: table.tableName,
              tableType: table.tableType,
              columns: columns,
              postgresTableName: table.tableName,
              connection: {
                connectionId: entry.connection?.id,
                host: entry.connection?.host,
                port: entry.connection?.port,
                dbname: entry.connection?.databaseName,
                user: entry.connection?.username,
                schema: entry.connection?.schema,
              },
              dbType: entry.dbType,
              position: table.position ?? idx,
            },
            parentId: folderId,
          };
        });

        tableNodes.sort((a, b) => (a.metadata?.position || 0) - (b.metadata?.position || 0));
        console.log(`[Metadata Load] Built ${tableNodes.length} tables for folder ${entry.name}`);

        const folderNode: RepositoryNode = {
          id: folderId,
          name: entry.name,
          type: 'folder' as const,
          children: tableNodes,
          draggable: false,
          droppable: true,
          metadata: {
            entryId: entry.id,
            connectionId: entry.connection?.id,
            connectionDetails: entry.connection,
            dbType: entry.dbType,
            createdAt: entry.createdAt,
            updatedAt: entry.updatedAt,
          },
        };
        newFolderNodes.push(folderNode);
      }

      console.log(`[Metadata Load] Created ${newFolderNodes.length} folder nodes`);

      setRepositoryData(prev => {
        // Locate db-connections recursively
        const dbConnectionsNodeInfo = findNodeAndParent(prev, 'db-connections');
        if (!dbConnectionsNodeInfo) {
          console.error('[Metadata Load] db-connections folder not found in repository tree!');
          return prev;
        }

        const dbConnectionsNode = dbConnectionsNodeInfo.node;
        const existingChildren = dbConnectionsNode.children || [];

        // Filter out any existing metadata‑based folders (those with entryId metadata)
        const filteredChildren = existingChildren.filter(child => {
          const hasEntryId = child.metadata && (child.metadata as any).entryId;
          if (hasEntryId) {
            console.log(`[Metadata Load] Removing old folder: ${child.name} (entryId: ${(child.metadata as any).entryId})`);
          }
          return !hasEntryId;
        });

        // Add new folder nodes, skipping duplicates
        const newChildren = [...filteredChildren];
        for (const folderNode of newFolderNodes) {
          const exists = newChildren.some(c => (c.metadata as any)?.entryId === folderNode.metadata?.entryId);
          if (!exists) {
            newChildren.push(folderNode);
            console.log(`[Metadata Load] Added folder: ${folderNode.name}`);
          } else {
            console.log(`[Metadata Load] Skipping duplicate folder: ${folderNode.name}`);
          }
        }

        // Sort folders alphabetically (optional)
        newChildren.sort((a, b) => a.name.localeCompare(b.name));

        const updatedDbConnectionsNode = {
          ...dbConnectionsNode,
          children: newChildren,
          metadata: {
            ...dbConnectionsNode.metadata,
            count: newChildren.length,
          },
        };

        // Apply the update in the tree using the existing helper
        const newTree = updateNodeInTree(prev, 'db-connections', () => updatedDbConnectionsNode);
        return newTree;
      });

    } catch (error) {
      console.error('[Metadata Load] Unexpected error:', error);
    }
  };

  loadDatabaseMetadataEntries();
}, [databaseContext.apiService, databaseContext.isConnected, setRepositoryData]);

  // 🔥 Force expand metadata and any category that has children
  useEffect(() => {
    setExpandedNodes(prev => {
      const next = new Set(prev);
      const addChildrenWithContent = (nodes: RepositoryNode[]) => {
        nodes.forEach(node => {
          if (node.children?.length) {
            next.add(node.id);
            addChildrenWithContent(node.children);
          }
        });
      };
      addChildrenWithContent(repositoryData);
      return next;
    });
  }, [repositoryData]);

  // ==================== GLOBAL CONTEXT MENU FALLBACK ====================
  useEffect(() => {
    const container = document.getElementById('app-sidebar');
    if (!container) return;

    const handleContainerContextMenu = (e: MouseEvent) => {
      const target = e.target as HTMLElement;
      const nodeElement = target.closest('[data-node-id]');
      if (nodeElement) {
        const nodeId = nodeElement.getAttribute('data-node-id');
        if (!nodeId) return;

        const found = findNodeAndParent(repositoryData, nodeId);
        if (found?.node) {
          e.preventDefault();
          e.stopPropagation();
          handleContextMenu(found.node, { x: e.clientX, y: e.clientY });
        }
      }
    };

    container.addEventListener('contextmenu', handleContainerContextMenu);
    return () => container.removeEventListener('contextmenu', handleContainerContextMenu);
  }, [repositoryData, handleContextMenu]);

  // ==================== 🛡️ DIRECT CONTEXT MENU CLICK HANDLER ====================
  // Guarantees that clicking a menu item opens the wizard, regardless of ContextMenu's internal implementation.
  useEffect(() => {
    if (!contextMenuRef.current) return;

    const menuContainer = contextMenuRef.current;
    const handleMenuItemClick = (e: MouseEvent) => {
      const target = e.target as HTMLElement;
      // Try to find the button that was clicked
      const button = target.closest('button');
      if (!button) return;

      const buttonText = button.textContent?.trim().toLowerCase() || '';
      console.log('📌 Context menu button clicked:', buttonText);

      // Map button text to wizard open handler
      if (buttonText.includes('excel')) {
        handleOpenExcelWizard();
      } else if (buttonText.includes('xml')) {
        handleOpenXMLWizard();
      } else if (buttonText.includes('delimited') || buttonText.includes('csv')) {
        handleOpenDelimitedWizard();
      } else if (buttonText.includes('positional')) {
        handleOpenPositionalWizard();
      } else if (buttonText.includes('schema')) {
        handleOpenFileSchemaWizard();
      } else if (buttonText.includes('json') || buttonText.includes('avro') || buttonText.includes('parquet')) {
        handleOpenJsonAvroParquetWizard();
      } else if (buttonText.includes('regex')) {
        handleOpenRegexWizard();
      } else if (buttonText.includes('ldif')) {
        handleOpenLDIFWizard();
      } else if (buttonText.includes('web service')) {
        handleOpenWebServiceWizard();
      } else if (buttonText.includes('database')) {
        handleOpenDatabaseWizard();
      }
    };

    menuContainer.addEventListener('click', handleMenuItemClick);
    return () => menuContainer.removeEventListener('click', handleMenuItemClick);
  }, [
    contextMenu,
    handleOpenExcelWizard,
    handleOpenXMLWizard,
    handleOpenDelimitedWizard,
    handleOpenPositionalWizard,
    handleOpenFileSchemaWizard,
    handleOpenJsonAvroParquetWizard,
    handleOpenRegexWizard,
    handleOpenLDIFWizard,
    handleOpenWebServiceWizard,
    handleOpenDatabaseWizard
  ]);

  // ==================== RENDER ====================
  return (
    <>
      <div
        className="flex-none h-full flex flex-col bg-white dark:bg-gray-900 border-r border-gray-200 dark:border-gray-800 overflow-hidden sidebar-container"
        style={{
          width: '280px',
          minWidth: '280px',
          maxWidth: '280px',
          flexShrink: 0,
          flexGrow: 0,
          position: 'relative',
          height: '100%'
        }}
        id="app-sidebar"
        aria-label="Application sidebar"
        data-testid="sidebar-container"
        data-sidebar-mode="default"
      >
        {/* Header */}
        <div className="flex-none flex items-center justify-between p-3 border-b border-gray-200 dark:border-gray-800">
          <h2 className="text-sm font-semibold text-gray-900 dark:text-white">
            Repository
          </h2>
          <div className="flex items-center space-x-1">
            <button
              className="h-7 w-7 p-0 text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200 flex items-center justify-center disabled:opacity-50 disabled:cursor-not-allowed"
              title="Refresh repository"
              aria-label="Refresh repository"
            >
              <RefreshCw className="h-4 w-4" />
            </button>

            <button
              onClick={() => handleCreateItem('folder')}
              className="h-7 w-7 p-0 text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200 flex items-center justify-center"
              title="Create new folder"
              aria-label="Create new folder"
            >
              <Plus className="h-4 w-4" />
            </button>

            <button
              onClick={() => setShowMetadataSettings(!showMetadataSettings)}
              className="h-7 w-7 p-0 text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200 flex items-center justify-center"
              title="Metadata display settings"
              aria-label="Metadata display settings"
            >
              <Layers className="h-4 w-4" />
            </button>
          </div>
        </div>

        {/* Tree View */}
        <div className="flex-1 overflow-y-auto min-h-0">
          {repositoryData.length === 0 ? (
            <div className="p-4 text-center text-gray-500">
              <Layers className="h-8 w-8 mx-auto mb-2 text-gray-400" />
              <p>No repository data available.</p>
              <button
                onClick={() => setRepositoryData(getDefaultRepositoryStructure())}
                className="mt-2 px-3 py-1 text-sm bg-blue-500 text-white rounded hover:bg-blue-600"
                aria-label="Load default repository structure"
              >
                Load Default Structure
              </button>
            </div>
          ) : (
            <Layout.TreeView
              key={`tree-${repositoryData.map(c => c.children?.length ?? 0).join('-')}-${Array.from(expandedNodes).sort().join('-')}`}
              repositoryData={repositoryData}
              expandedNodes={expandedNodes}
              selectedNode={selectedNode}
              searchTerm={searchTerm}
              expandedMetadataNodes={expandedMetadataNodes}
              onToggle={handleToggle}
              onSelect={handleSelect}
              onDoubleClick={undefined}
              onContextMenu={handleContextMenu}
              onDelete={handleDeleteNode}
              onDragStart={handleDragStart}
              onToggleMetadata={handleToggleMetadata}
            />
          )}
        </div>

        {/* React Flow Integration Panel */}
        <Layout.ReactFlowIntegrationPanel
          reactFlowInstance={reactFlowInstance}
          onNodeCreate={onNodeCreate}
          onNodeUpdate={onNodeUpdate}
          selectedNode={selectedNode}
          findNodeById={findNodeByIdWrapper}
          repositoryData={repositoryData}
          createNodeDirectly={createNodeDirectly}
          isDebugOpen={false}
          setIsDebugOpen={() => {}}
        />

        {/* Metadata Expansion Status */}
        <div className="p-2 border-t border-gray-200 dark:border-gray-800 bg-gray-50 dark:bg-gray-800/50">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-2">
              <Layers className="h-3 w-3 text-gray-500" />
              <span className="text-xs text-gray-600 dark:text-gray-400">
                Repository: {repositoryData.reduce((acc, cat) => acc + (cat.children?.length || 0), 0)} items
              </span>
            </div>
            <button
              onClick={() => setShowMetadataSettings(true)}
              className="text-xs px-2 py-1 bg-gray-200 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded hover:bg-gray-300 dark:hover:bg-gray-600"
              aria-label="Open metadata settings"
            >
              Settings
            </button>
          </div>
        </div>

        {/* Context Menus */}
        {contextMenu && (
          <div ref={contextMenuRef}>
            <ContextMenu
              node={contextMenu.node}
              position={contextMenu.position}
              onClose={() => setContextMenu(null)}
              onCreateItem={handleCreateItem}
              onDelete={handleDeleteItem}
              onOpenExcelWizard={handleOpenExcelWizard}
              onOpenXMLWizard={handleOpenXMLWizard}
              onOpenDelimitedWizard={handleOpenDelimitedWizard}
              onOpenPositionalWizard={handleOpenPositionalWizard}
              onOpenFileSchemaWizard={handleOpenFileSchemaWizard}
              onOpenJsonAvroParquetWizard={handleOpenJsonAvroParquetWizard}
              onOpenRegexWizard={handleOpenRegexWizard}
              onOpenLDIFWizard={handleOpenLDIFWizard}
              onCreateJobWizard={handleCreateJobWizard} 
              onOpenWebServiceWizard={handleOpenWebServiceWizard}
              onOpenDatabaseWizard={handleOpenDatabaseWizard}
            />
          </div>
        )}

        {/* Job Design Context Menu */}
        {jobDesignContextMenu && (
          <div
            ref={jobDesignContextMenuRef}
            className="fixed z-[9999] bg-white dark:bg-gray-800 rounded-lg shadow-xl border border-gray-200 dark:border-gray-700 py-2 min-w-[200px]"
            style={{
              left: `${jobDesignContextMenu.position.x}px`,
              top: `${jobDesignContextMenu.position.y}px`
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <div className="px-3 py-2 border-b border-gray-100 dark:border-gray-700">
              <p className="text-sm font-medium text-gray-900 dark:text-white">
                {jobDesignContextMenu.node.name}
              </p>
            </div>
            {/* 🔥 New "Create Canvas" button */}
            <button
              onClick={() => handleCreateCanvasFromJob(jobDesignContextMenu.node)}
              className="w-full text-left px-4 py-2 text-sm text-gray-700 hover:bg-gray-100 dark:text-gray-200 dark:hover:bg-gray-700"
            >
              Create Canvas
            </button>
            <div className="border-t border-gray-100 dark:border-gray-700 pt-2 mt-2">
              <button
                onClick={() => setJobDesignContextMenu(null)}
                className="w-full px-3 py-1 text-xs text-center text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-300"
              >
                Close
              </button>
            </div>
          </div>
        )}
      </div>

      {/* Dialogs */}
<Layout.DeleteConfirmationDialog
  isOpen={deleteDialogOpen}
  node={nodeToDelete}
  onClose={() => {
    setDeleteDialogOpen(false);
    setNodeToDelete(null);
    setIsDeletingForeignTable(false);
    setIsDeleting(false);
  }}
  onConfirm={confirmDeleteNode}
  calculateDeleteStats={Utils.calculateDeleteStats}
/>

      {/* Wizards */}
      <ExcelMetadataWizard
        isOpen={excelWizardOpen}
        onClose={() => setExcelWizardOpen(false)}
        onSave={handleSaveExcelMetadata}
      />

      <XMLMetadataWizard
        isOpen={xmlWizardOpen}
        onClose={() => setXmlWizardOpen(false)}
        onSave={handleSaveXMLMetadata}
      />

      <DelimitedFileMetadataWizard
        isOpen={delimitedWizardOpen}
        onClose={() => setDelimitedWizardOpen(false)}
        onSave={handleSaveDelimitedMetadata}
      />

      <PositionalFileMetadataWizard
        isOpen={positionalWizardOpen}
        onClose={() => setPositionalWizardOpen(false)}
        onSave={handleSavePositionalMetadata}
      />

      <FileSchemaMetadataWizard
        isOpen={fileSchemaWizardOpen}
        onClose={() => setFileSchemaWizardOpen(false)}
        onSave={handleSaveFileSchemaMetadata}
      />

      <JsonAvroParquetMetadataWizard
        isOpen={jsonAvroParquetWizardOpen}
        onClose={() => setJsonAvroParquetWizardOpen(false)}
        onSave={handleSaveJsonAvroParquetMetadata}
      />

      <RegexMetadataWizard
        isOpen={regexWizardOpen}
        onClose={() => setRegexWizardOpen(false)}
        onSave={handleSaveRegexMetadata}
      />

      <LDIFMetadataWizard
        isOpen={ldifWizardOpen}
        onClose={() => setLdifWizardOpen(false)}
        onSave={handleSaveLDIFMetadata}
      />

      <WebServiceMetadataWizard
        isOpen={webServiceWizardOpen}
        onClose={() => setWebServiceWizardOpen(false)}
        onSave={handleSaveWebServiceMetadata}
      />

      <DatabaseMetadataWizard
        isOpen={databaseWizardOpen}
        onClose={() => setDatabaseWizardOpen(false)}
        onSave={handleSaveDatabaseMetadata}
      />

      <Layout.ToastContainerWrapper />
    </>
  );
};

// ============================================================================
// Helper functions used in wizard save handlers
// ============================================================================
function generateForeignTableName(metadata: any, type: string): string {
  const base = metadata.name || metadata.fileName || `etl_${type}`;
  return base
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, '_')
    .replace(/_{2,}/g, '_')
    .replace(/^_|_$/g, '')
    .substring(0, 63);
}

function extractColumnsFromMetadata(metadata: any, _type: string): ColumnDefinition[] {
  const columnsArray = metadata.columns || metadata.schema || metadata.fields || [];
  if (!Array.isArray(columnsArray) || columnsArray.length === 0) {
    return [];
  }
  return columnsArray.map((col: any) => ({
    name: col.name || col.columnName || 'column',
    type: col.type || col.dataType || 'TEXT',
    length: col.length || col.maxLength,
    precision: col.precision,
    scale: col.scale,
    nullable: col.nullable !== false,
    defaultValue: col.defaultValue,
  }));
}

function extractFilePath(metadata: any, _type: string): string {
  return metadata.filePath || metadata.fileName || metadata.path || '';
}

function extractOptions(metadata: any, _type: string): Record<string, string> {
  const options: Record<string, string> = {};
  if (metadata.delimiter) options.delimiter = metadata.delimiter;
  if (metadata.header !== undefined) options.header = metadata.header ? 'true' : 'false';
  if (metadata.encoding) options.encoding = metadata.encoding;
  if (metadata.sheet) options.sheet = metadata.sheet;
  if (metadata.recordLength) options.recordLength = String(metadata.recordLength);
  if (metadata.pattern) options.pattern = metadata.pattern;
  if (metadata.format) options.format = metadata.format;
  if (metadata.textQualifier) options.textQualifier = metadata.textQualifier;
  if (metadata.escape) options.escape = metadata.escape;
  if (metadata.compression) options.compression = metadata.compression;
  if (metadata.flags) options.flags = metadata.flags;
  if (metadata.schemaType) options.schemaType = metadata.schemaType;
  return options;
}

export default RepositorySidebar;