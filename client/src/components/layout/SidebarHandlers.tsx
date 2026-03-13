// src/components/layout/SidebarHandlers.ts

import { RepositoryNode, DeletionHistoryItem } from '../../types/types';
import { DatabaseConfig } from '../../services/database-api.service';
import { DatabaseApiService } from '../../services/database-api.service';
import { persistenceService } from '../../services/persistence.service';
import { toast } from 'react-toastify';
import React from 'react';

// ==================== PERSISTENCE HANDLERS ====================

export const handleManualSave = (
  repositoryData: RepositoryNode[],
  expandedNodes: Set<string>,
  selectedNode: string | null,
  showToast: boolean = true
) => {
  persistenceService.saveRepositoryData(repositoryData);
  persistenceService.saveExpandedNodes(expandedNodes);
  persistenceService.saveSelectedNode(selectedNode);
  
  if (showToast) {
    toast.success('💾 Repository state saved!', {
      position: "bottom-right",
      autoClose: 3000,
    });
  }
};

export const handleResetRepository = (
  currentJob: any,
  getDefaultRepositoryData: (job?: any) => RepositoryNode[],
  setRepositoryData: React.Dispatch<React.SetStateAction<RepositoryNode[]>>,
  setExpandedNodes: React.Dispatch<React.SetStateAction<Set<string>>>,
  setSelectedNode: React.Dispatch<React.SetStateAction<string | null>>,
  setDeletionHistory: React.Dispatch<React.SetStateAction<DeletionHistoryItem[]>>
): boolean => {
  if (window.confirm('⚠️ Reset repository to defaults?\n\nThis will clear all saved metadata and restore the default structure. This action cannot be undone.')) {
    setRepositoryData(getDefaultRepositoryData(currentJob));
    setExpandedNodes(new Set([
      'job-designs', 'contexts', 'metadata', 'code', 'sql-templates',
      'business-models', 'documentation', 'recycle-bin'
    ]));
    setSelectedNode(null);
    
    persistenceService.clearAll();
    setDeletionHistory([]);
    
    toast.success('✅ Repository reset to defaults!', {
      position: "bottom-right",
      autoClose: 3000,
    });
    return true;
  }
  return false;
};

export const exportRepositoryData = (
  repositoryData: RepositoryNode[],
  expandedNodes: Set<string>,
  selectedNode: string | null
) => {
  const exportData = {
    repositoryData,
    expandedNodes: Array.from(expandedNodes),
    selectedNode,
    exportDate: new Date().toISOString(),
    version: '1.0'
  };
  
  const dataStr = JSON.stringify(exportData, null, 2);
  const dataUri = 'data:application/json;charset=utf-8,'+ encodeURIComponent(dataStr);
  
  const exportFileDefaultName = `repository-backup-${new Date().toISOString().split('T')[0]}.json`;
  
  const linkElement = document.createElement('a');
  linkElement.setAttribute('href', dataUri);
  linkElement.setAttribute('download', exportFileDefaultName);
  linkElement.click();
  
  toast.success(`📥 Repository exported to ${exportFileDefaultName}`, {
    position: "bottom-right",
    autoClose: 3000,
  });
};

// ==================== DELETE HANDLERS ====================

export const deleteNodeRecursive = (nodes: RepositoryNode[], nodeId: string): RepositoryNode[] => {
  return nodes.filter(node => {
    if (node.id === nodeId) return false;
    
    if (node.children) {
      node.children = deleteNodeRecursive(node.children, nodeId);
    }
    
    return true;
  });
};

export const findAllNodesToDelete = (node: RepositoryNode): RepositoryNode[] => {
  const nodes: RepositoryNode[] = [node];
  
  const collectChildren = (n: RepositoryNode) => {
    if (n.children) {
      n.children.forEach(child => {
        nodes.push(child);
        collectChildren(child);
      });
    }
  };
  
  collectChildren(node);
  return nodes;
};

export const findParentId = (
  nodes: RepositoryNode[],
  nodeId: string,
  parentId?: string
): string | undefined => {
  for (const node of nodes) {
    if (node.id === nodeId) return parentId;
    if (node.children) {
      const found = findParentId(node.children, nodeId, node.id);
      if (found) return found;
    }
  }
  return undefined;
};

export const findNodeById = (nodes: RepositoryNode[], nodeId: string): RepositoryNode | null => {
  for (const node of nodes) {
    if (node.id === nodeId) return node;
    if (node.children) {
      const found = findNodeById(node.children, nodeId);
      if (found) return found;
    }
  }
  return null;
};

export const handleDeleteNodeRequest = (
  node: RepositoryNode,
  currentJob: any,
  isNodeDeletable: (node: RepositoryNode, currentJob?: any) => boolean,
  setNodeToDelete: React.Dispatch<React.SetStateAction<RepositoryNode | null>>,
  setDeleteDialogOpen: React.Dispatch<React.SetStateAction<boolean>>
) => {
  if (!isNodeDeletable(node, currentJob)) {
    toast.warning(`Cannot delete system item: ${node.name}`, {
      position: "bottom-right",
      autoClose: 3000,
    });
    return false;
  }
  
  if (node.type === 'job' && currentJob?.id === node.id.replace('job-', '')) {
    toast.error('Cannot delete active job. Please switch to another job first.', {
      position: "bottom-right",
      autoClose: 4000,
    });
    return false;
  }
  
  setNodeToDelete(node);
  setDeleteDialogOpen(true);
  return true;
};

export const confirmDeleteNode = async (
  nodeToDelete: RepositoryNode,
  repositoryData: RepositoryNode[],
  selectedNode: string | null,
  setRepositoryData: React.Dispatch<React.SetStateAction<RepositoryNode[]>>,
  setExpandedNodes: React.Dispatch<React.SetStateAction<Set<string>>>,
  setSelectedNode: React.Dispatch<React.SetStateAction<string | null>>,
  setDeletionHistory: React.Dispatch<React.SetStateAction<DeletionHistoryItem[]>>,
  setDeleteDialogOpen: React.Dispatch<React.SetStateAction<boolean>>,
  setNodeToDelete: React.Dispatch<React.SetStateAction<RepositoryNode | null>>,
  cleanupNodeResources: (node: RepositoryNode) => Promise<void>,
  handleManualSave: () => void
): Promise<boolean> => {
  if (!nodeToDelete) return false;
  
  try {
    const nodesToDelete = findAllNodesToDelete(nodeToDelete);
    const parentId = findParentId(repositoryData, nodeToDelete.id);
    
    const toastId = toast.loading(`Deleting "${nodeToDelete.name}"...`, {
      position: "bottom-right",
    });
    
    const deletionItem: DeletionHistoryItem = {
      node: nodeToDelete,
      parentId,
      deletedAt: new Date().toISOString(),
      id: `deletion-${Date.now()}`,
      childrenCount: nodesToDelete.length - 1,
      timestamp: new Date().toISOString()
    };
    
    setDeletionHistory((prev: DeletionHistoryItem[]) => [...prev, deletionItem]);
    
    for (const node of nodesToDelete) {
      await cleanupNodeResources(node);
    }
    
    setRepositoryData((prev: RepositoryNode[]) => deleteNodeRecursive(prev, nodeToDelete.id));
    
    setExpandedNodes((prev: Set<string>) => {
      const newSet = new Set(prev);
      nodesToDelete.forEach(node => newSet.delete(node.id));
      return newSet;
    });
    
    if (selectedNode === nodeToDelete.id) {
      setSelectedNode(null);
    }
    
    handleManualSave();
    
    toast.update(toastId, {
      render: `✅ Successfully deleted "${nodeToDelete.name}"${nodesToDelete.length > 1 ? ` and ${nodesToDelete.length - 1} child item${nodesToDelete.length - 1 === 1 ? '' : 's'}` : ''}`,
      type: 'success',
      isLoading: false,
      autoClose: 3000,
      closeButton: true,
    });
    
    console.log(`🗑️ Deleted node: ${nodeToDelete.name} (${nodeToDelete.id}) with ${nodesToDelete.length - 1} children`);
    
    return true;
  } catch (error: any) {
    toast.error(`Failed to delete: ${error.message}`, {
      position: "bottom-right",
      autoClose: 4000,
    });
    console.error('Delete error:', error);
    return false;
  } finally {
    setDeleteDialogOpen(false);
    setNodeToDelete(null);
  }
};

export const handleUndoLastDeletion = (
  deletionHistory: DeletionHistoryItem[],
  setDeletionHistory: React.Dispatch<React.SetStateAction<DeletionHistoryItem[]>>,
  setRepositoryData: React.Dispatch<React.SetStateAction<RepositoryNode[]>>,
  setExpandedNodes: React.Dispatch<React.SetStateAction<Set<string>>>
): boolean => {
  if (deletionHistory.length === 0) return false;
  
  const lastDeletion = deletionHistory[deletionHistory.length - 1];
  
  try {
    if (lastDeletion.parentId) {
      const restoreToParent = (nodes: RepositoryNode[]): RepositoryNode[] => {
        return nodes.map(node => {
          if (node.id === lastDeletion.parentId) {
            return {
              ...node,
              children: [...(node.children || []), lastDeletion.node]
            };
          }
          
          if (node.children) {
            return {
              ...node,
              children: restoreToParent(node.children)
            };
          }
          
          return node;
        });
      };
      
      setRepositoryData((prev: RepositoryNode[]) => restoreToParent(prev));
      setExpandedNodes((prev: Set<string>) => new Set([...prev, lastDeletion.node.id]));
      setDeletionHistory((prev: DeletionHistoryItem[]) => prev.slice(0, -1));
      
      toast.success(`Restored "${lastDeletion.node.name}"`, {
        position: "bottom-right",
        autoClose: 3000,
      });
      
      console.log(`↩️ Restored node: ${lastDeletion.node.name}`);
      return true;
    }
    return false;
  } catch (error: any) {
    toast.error(`Failed to restore: ${error.message}`, {
      position: "bottom-right",
      autoClose: 4000,
    });
    return false;
  }
};

export const handleClearDeletionHistory = (
  deletionHistory: DeletionHistoryItem[],
  setDeletionHistory: React.Dispatch<React.SetStateAction<DeletionHistoryItem[]>>
): boolean => {
  if (deletionHistory.length === 0) return false;
  
  if (window.confirm(`Clear all deletion history (${deletionHistory.length} items)? This will permanently remove undo capability.`)) {
    setDeletionHistory([]);
    toast.info('Deletion history cleared', {
      position: "bottom-right",
      autoClose: 3000,
    });
    return true;
  }
  return false;
};

// ==================== DATABASE HANDLERS ====================

export const checkPostgresAvailability = async (
  checkBackendHealth: () => Promise<boolean>,
  connections: any[],
  getDatabaseInfo: (connectionId: string) => Promise<any>,
  refreshForeignTablesCount: () => Promise<void>,
  setIsCheckingPostgres: React.Dispatch<React.SetStateAction<boolean>>,
  setIsPostgresConnected: React.Dispatch<React.SetStateAction<boolean>>,
  setConnectionError: React.Dispatch<React.SetStateAction<string | null>>,
  setCurrentConnectionId: React.Dispatch<React.SetStateAction<string | null>>,
  setConnectionConfig: React.Dispatch<React.SetStateAction<DatabaseConfig>>,
  setDbStatus: React.Dispatch<React.SetStateAction<any>>
): Promise<boolean> => {
  try {
    setIsCheckingPostgres(true);
    
    const backendHealthy = await checkBackendHealth();
    if (!backendHealthy) {
      setIsPostgresConnected(false);
      setConnectionError('Backend server is not responding. Please start the backend server.');
      setDbStatus({ isConnected: false, error: 'Backend not responding' });
      return false;
    }
    
    const activeConnections = connections;
    const isConnected = activeConnections.length > 0;
    
    if (isConnected && activeConnections[0]) {
      const connection = activeConnections[0];
      setIsPostgresConnected(true);
      setConnectionError(null);
      setCurrentConnectionId(connection.connectionId);
      setConnectionConfig(connection.config);
      
      const infoResult = await getDatabaseInfo(connection.connectionId);
      if (infoResult.success && infoResult.info) {
        setDbStatus({
          isConnected: true,
          version: infoResult.info.version
        });
      } else {
        setDbStatus({
          isConnected: true,
          error: infoResult.error
        });
      }
      
      await refreshForeignTablesCount();
      return true;
    } else {
      setIsPostgresConnected(false);
      setConnectionError('No active PostgreSQL connections');
      setCurrentConnectionId(null);
      setDbStatus({ isConnected: false, error: 'No active connections' });
      return false;
    }
    
  } catch (error: any) {
    console.error('Failed to check PostgreSQL availability:', error);
    setIsPostgresConnected(false);
    setConnectionError(`Connection failed: ${error instanceof Error ? error.message : 'Unknown error'}`);
    setDbStatus({ isConnected: false, error: 'Connection failed' });
    return false;
  } finally {
    setIsCheckingPostgres(false);
  }
};

export const handleConnectToDatabase = async (
  config: DatabaseConfig,
  connectionId: string,
  refreshConnections: () => Promise<void>,
  setIsCheckingPostgres: React.Dispatch<React.SetStateAction<boolean>>,
  setConnectionConfig: React.Dispatch<React.SetStateAction<DatabaseConfig>>,
  setCurrentConnectionId: React.Dispatch<React.SetStateAction<string | null>>,
  setIsPostgresConnected: React.Dispatch<React.SetStateAction<boolean>>,
  setConnectionError: React.Dispatch<React.SetStateAction<string | null>>,
  setDbStatus: React.Dispatch<React.SetStateAction<any>>
): Promise<boolean> => {
  try {
    setIsCheckingPostgres(true);
    
    setConnectionConfig(config);
    setCurrentConnectionId(connectionId);
    setIsPostgresConnected(true);
    setConnectionError(null);
    setDbStatus({
      isConnected: true,
      version: 'Connected via Backend API'
    });
    
    await refreshConnections();
    return true;
  } catch (error: any) {
    console.error('Connection error:', error);
    setConnectionError(`Connection error: ${error instanceof Error ? error.message : 'Unknown error'}`);
    setIsPostgresConnected(false);
    setCurrentConnectionId(null);
    return false;
  } finally {
    setIsCheckingPostgres(false);
  }
};

export const handleDisconnect = async (
  currentConnectionId: string | null,
  disconnect: (id: string) => Promise<any>,
  refreshConnections: () => Promise<void>,
  setIsPostgresConnected: React.Dispatch<React.SetStateAction<boolean>>,
  setConnectionError: React.Dispatch<React.SetStateAction<string | null>>,
  setDbStatus: React.Dispatch<React.SetStateAction<any>>,
  setCurrentConnectionId: React.Dispatch<React.SetStateAction<string | null>>
): Promise<boolean> => {
  try {
    if (currentConnectionId) {
      const result = await disconnect(currentConnectionId);
      if (result.success) {
        setIsPostgresConnected(false);
        setConnectionError(null);
        setDbStatus({ isConnected: false });
        setCurrentConnectionId(null);
        
        await refreshConnections();
        return true;
      }
    }
    return false;
  } catch (error: any) {
    console.error('Disconnect error:', error);
    return false;
  }
};

// ==================== DRAG AND DROP HANDLERS ====================

export const handleDragStart = (
  node: RepositoryNode,
  event: React.DragEvent,
  onDragStart?: (node: RepositoryNode) => void,
  setDragState?: React.Dispatch<React.SetStateAction<any>>
) => {
  const target = event.currentTarget as HTMLElement;
  target.classList.add("dragging");
  
  const dragData = {
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
  };
  
  event.dataTransfer.setData('application/json', JSON.stringify(dragData));
  event.dataTransfer.setData('application/reactflow', JSON.stringify({
    type: 'reactflow',
    source: 'sidebar',
    component: {
      name: node.name,
      type: node.type || 'processing',
      metadata: node.metadata
    }
  }));
  event.dataTransfer.setData('text/plain', node.name);
  event.dataTransfer.setData('sidebar-with-popup', 'true');
  event.dataTransfer.effectAllowed = 'copyMove';
  
  const dragImage = document.createElement('div');
  dragImage.style.cssText = `
    position: absolute;
    top: -1000px;
    left: -1000px;
    padding: 8px 12px;
    background: white;
    border: 2px solid #3b82f6;
    border-radius: 6px;
    box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    font-size: 12px;
    white-space: nowrap;
    z-index: 9999;
  `;
  dragImage.textContent = `Add: ${node.name} (Choose Input/Output)`;
  document.body.appendChild(dragImage);
  
  const dragImageWidth = dragImage.offsetWidth;
  const dragImageHeight = dragImage.offsetHeight;
  event.dataTransfer.setDragImage(dragImage, dragImageWidth / 2, dragImageHeight / 2);
  
  setTimeout(() => {
    if (document.body.contains(dragImage)) {
      document.body.removeChild(dragImage);
    }
  }, 0);
  
  if (setDragState) {
    setDragState({
      isDragging: true,
      draggedNode: node,
      dragOverNode: null,
      dropPosition: null
    });
  }
  
  onDragStart?.(node);
};

export const handleDragOverLogic = (
  node: RepositoryNode,
  event: React.DragEvent,
  dragState: any,
  setDragState: React.Dispatch<React.SetStateAction<any>>
) => {
  event.preventDefault();
  
  if (!dragState.draggedNode || dragState.draggedNode.id === node.id) {
    return;
  }
  
  const rect = (event.currentTarget as HTMLElement).getBoundingClientRect();
  const relativeY = event.clientY - rect.top;
  const heightThird = rect.height / 3;
  
  let dropPosition: 'before' | 'inside' | 'after' = 'inside';
  
  if (relativeY < heightThird) {
    dropPosition = 'before';
  } else if (relativeY > heightThird * 2) {
    dropPosition = 'after';
  } else {
    dropPosition = 'inside';
  }
  
  if (dropPosition === 'inside' && (node.type === 'item' || !node.droppable)) {
    return;
  }
  
  setDragState({
    ...dragState,
    dragOverNode: node,
    dropPosition
  });
};

export const handleDragLeave = (
  event: React.DragEvent,
  setDragState: React.Dispatch<React.SetStateAction<any>>
) => {
  if (!event.currentTarget.contains(event.relatedTarget as Node)) {
    setDragState((prev: any) => ({
      ...prev,
      dragOverNode: null,
      dropPosition: null
    }));
  }
};

export const handleDrop = (
  targetNode: RepositoryNode,
  event: React.DragEvent,
  dragState: any,
  onDrop?: (target: RepositoryNode, source: RepositoryNode) => void,
  setDragState?: React.Dispatch<React.SetStateAction<any>>
): boolean => {
  event.preventDefault();
  
  try {
    const dragData = event.dataTransfer.getData('application/json');
    if (!dragData) return false;
    
    const parsedData = JSON.parse(dragData);
    if (parsedData.type !== 'repository-node') return false;
    
    const sourceNode = parsedData.node as RepositoryNode;
    
    if (sourceNode.id === targetNode.id) return false;
    
    if (dragState.dropPosition === 'inside' && 
        (targetNode.type === 'item' || !targetNode.droppable)) {
      return false;
    }
    
    onDrop?.(targetNode, sourceNode);
    
    console.log(`Dropped ${sourceNode.name} ${dragState.dropPosition} ${targetNode.name}`);
    return true;
    
  } catch (error: any) {
    console.error('Drop error:', error);
    return false;
  } finally {
    if (setDragState) {
      setDragState({
        isDragging: false,
        draggedNode: null,
        dragOverNode: null,
        dropPosition: null
      });
    }
  }
};

export const handleDragEnd = (setDragState?: React.Dispatch<React.SetStateAction<any>>) => {
  document.querySelectorAll('.dragging').forEach(el => {
    el.classList.remove('dragging');
  });
  
  if (setDragState) {
    setDragState({
      isDragging: false,
      draggedNode: null,
      dragOverNode: null,
      dropPosition: null
    });
  }
};

// ==================== EVENT HANDLERS ====================

export const handleToggle = (
  nodeId: string,
  _expandedNodes: Set<string>,
  setExpandedNodes: React.Dispatch<React.SetStateAction<Set<string>>>
) => {
  setExpandedNodes((prev: Set<string>) => {
    const newSet = new Set(prev);
    if (newSet.has(nodeId)) {
      newSet.delete(nodeId);
    } else {
      newSet.add(nodeId);
    }
    return newSet;
  });
};

export const handleSelect = (
  node: RepositoryNode,
  setSelectedNode: React.Dispatch<React.SetStateAction<string | null>>,
  onNodeSelect?: (node: RepositoryNode) => void
) => {
  setSelectedNode(node.id);
  onNodeSelect?.(node);
};

export const handleDoubleClick = (
  node: RepositoryNode,
  onToggle: (nodeId: string) => void,
  onNodeDoubleClick?: (node: RepositoryNode) => void,
  onNodeCreate?: (nodeData: any, position: { x: number; y: number }) => void,
  createNodeDirectly?: (node: RepositoryNode, position: { x: number; y: number }) => void
) => {
  if (node.type === 'folder' || node.type === 'category') {
    onToggle(node.id);
  }
  
  if (node.type === 'item' && node.metadata && onNodeCreate && createNodeDirectly) {
    const position = { x: 100, y: 100 };
    createNodeDirectly(node, position);
  }
  
  onNodeDoubleClick?.(node);
};

export const handleContextMenu = (
  node: RepositoryNode,
  position: { x: number; y: number },
  onCreateJob?: (jobName: string) => void,
  setContextMenu?: React.Dispatch<React.SetStateAction<{ node: RepositoryNode; position: { x: number; y: number } } | null>>,
  setJobDesignsContextMenu?: React.Dispatch<React.SetStateAction<{ node: RepositoryNode; position: { x: number; y: number } } | null>>
) => {
  if (node.id === 'job-designs' && onCreateJob) {
    setJobDesignsContextMenu?.({ node, position });
    setContextMenu?.(null);
  } else {
    setContextMenu?.({ node, position });
    setJobDesignsContextMenu?.(null);
  }
};

// ==================== AUTO-CONNECT HANDLERS ====================

export const autoConnectToPostgreSQL = async (
  hasAttemptedAutoConnect: boolean,
  autoConnectInProgress: boolean,
  isPostgresConnected: boolean,
  checkBackendHealth: () => Promise<boolean>,
  refreshConnections: () => Promise<void>,
  connections: any[],
  getDefaultConnectionConfig: () => DatabaseConfig,
  setIsCheckingPostgres: React.Dispatch<React.SetStateAction<boolean>>,
  setIsPostgresConnected: React.Dispatch<React.SetStateAction<boolean>>,
  setConnectionError: React.Dispatch<React.SetStateAction<string | null>>,
  setConnectionConfig: React.Dispatch<React.SetStateAction<DatabaseConfig>>,
  setCurrentConnectionId: React.Dispatch<React.SetStateAction<string | null>>,
  setDbStatus: React.Dispatch<React.SetStateAction<any>>,
  setHasAttemptedAutoConnect: React.Dispatch<React.SetStateAction<boolean>>,
  setAutoConnectInProgress: React.Dispatch<React.SetStateAction<boolean>>,
  refreshForeignTablesCount: () => Promise<void>
): Promise<boolean> => {
  if (hasAttemptedAutoConnect || autoConnectInProgress || isPostgresConnected) {
    return false;
  }
  
  try {
    setAutoConnectInProgress(true);
    setIsCheckingPostgres(true);
    
    const backendHealthy = await checkBackendHealth();
    if (!backendHealthy) {
      console.log('Backend not responding, skipping auto-connect');
      setAutoConnectInProgress(false);
      setHasAttemptedAutoConnect(true);
      setIsCheckingPostgres(false);
      return false;
    }
    
    await refreshConnections();
    if (connections.length > 0) {
      const connection = connections[0];
      setIsPostgresConnected(true);
      setConnectionError(null);
      setCurrentConnectionId(connection.connectionId);
      setConnectionConfig(connection.config);
      console.log('✅ Using existing PostgreSQL connection');
      setAutoConnectInProgress(false);
      setHasAttemptedAutoConnect(true);
      setIsCheckingPostgres(false);
      return true;
    } else {
      console.log('🔄 Attempting auto-connection to PostgreSQL...');
      
      const apiService = new DatabaseApiService();
      const defaultConfig = getDefaultConnectionConfig();
      
      const testResult = await apiService.testConnection('postgresql', defaultConfig);
      
      if (testResult.success) {
        const connectResult = await apiService.connect('postgresql', defaultConfig);
        
        if (connectResult.success && connectResult.connectionId) {
          setIsPostgresConnected(true);
          setConnectionError(null);
          setConnectionConfig(defaultConfig);
          setCurrentConnectionId(connectResult.connectionId);
          setDbStatus({
            isConnected: true,
            version: testResult.version
          });
          
          try {
            localStorage.setItem('last_db_user', defaultConfig.user || '');
            localStorage.setItem('last_db_host', defaultConfig.host || '');
            localStorage.setItem('last_db_port', defaultConfig.port || '');
          } catch (e) {}
          
          await refreshConnections();
          await refreshForeignTablesCount();
          
          console.log('✅ PostgreSQL auto-connected successfully!');
          setAutoConnectInProgress(false);
          setHasAttemptedAutoConnect(true);
          setIsCheckingPostgres(false);
          return true;
        } else {
          console.warn('⚠️ Auto-connect failed: Could not establish connection');
          setConnectionError(`Auto-connect failed: ${connectResult.error || 'Unknown error'}`);
          setAutoConnectInProgress(false);
          setHasAttemptedAutoConnect(true);
          setIsCheckingPostgres(false);
          return false;
        }
      } else {
        console.warn('⚠️ Auto-connect test failed: PostgreSQL may not be running');
        setConnectionError(`Auto-connect test failed: ${testResult.error || 'Please ensure PostgreSQL is running locally'}`);
        setAutoConnectInProgress(false);
        setHasAttemptedAutoConnect(true);
        setIsCheckingPostgres(false);
        return false;
      }
    }
  } catch (error: any) {
    console.error('❌ Auto-connect error:', error);
    setConnectionError(`Auto-connect error: ${error instanceof Error ? error.message : 'Unknown error'}`);
    setAutoConnectInProgress(false);
    setHasAttemptedAutoConnect(true);
    setIsCheckingPostgres(false);
    return false;
  }
};