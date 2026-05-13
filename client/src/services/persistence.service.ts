// src/services/persistence.service.ts - UPDATED WITH METADATA EXPANSION AND CANVAS STATE

import React from 'react';
import { RepositoryNode } from '../types/types';
import * as LucideIcons from 'lucide-react';
import type { Node as ReactFlowNode, Edge, Viewport } from 'reactflow';

// Define a type for the props that all Lucide icons accept
interface LucideIconProps {
  className?: string;
  size?: number | string;
  color?: string;
  strokeWidth?: number | string;
  [key: string]: any;
}

// Types for metadata persistence
export interface MetadataDisplaySettings {
  showAll: boolean;
  autoExpandNew: boolean;
  compactMode: boolean;
  showIcons: boolean;
  maxItemsVisible: number;
  sortOrder: 'priority' | 'alphabetical' | 'type' | 'asc';
}

export interface RepositoryPersistenceData {
  repository: RepositoryNode[];
  expandedNodes: string[];
  selectedNode: string | null;
  expandedMetadataNodes: string[];
  metadataDisplaySettings: MetadataDisplaySettings;
  lastSaved: string;
  version: string;
}

export interface DeletionHistoryItem {
  node: any;
  deletedAt: string;
  timestamp?: string;
  parentId?: string;
  previousParent?: any;
  restoreData?: {
    children?: RepositoryNode[];
    expanded?: boolean;
    metadataExpanded?: boolean;
  };
}

export interface CanvasState {
  nodes: ReactFlowNode[];
  edges: Edge[];
  viewport: Viewport;
  lastSaved?: string;
  version?: string;
}

// Filter out non-icon exports from LucideIcons
const createIconMap = () => {
  const iconMap: Record<string, React.ComponentType<LucideIconProps>> = {};
  
  const validIconNames = [
    'FileCode', 'Globe', 'Code', 'FileText', 'Briefcase', 'BookOpen',
    'Trash2', 'Plus', 'FileSpreadsheet', 'FileJson', 'Cloud', 'Network',
    'HardDrive', 'Search', 'Users', 'Key', 'Server', 'Table', 'Columns',
    'RefreshCw', 'AlertCircle', 'CheckCircle', 'Database', 'Info',
    'FolderOpen', 'FolderPlus', 'Folder', 'X', 'Eye', 'EyeOff', 'Settings',
    'Layers', 'Calendar', 'Tag', 'Hash', 'Text', 'Check', 'Clock', 'User',
    'ArrowRight', 'ArrowDown', 'MoreVertical', 'GripVertical', 'AlertTriangle',
    'Cpu', 'FileSpreadsheet', 'FileCode', 'FileText', 'Search', 'Network'
  ];

  validIconNames.forEach(iconName => {
    if (iconName in LucideIcons) {
      iconMap[iconName] = LucideIcons[iconName as keyof typeof LucideIcons] as any;
    }
  });

  return iconMap;
};

export interface PersistenceConfig {
  // Repository data persistence
  repositoryDataKey: string;
  expandedNodesKey: string;
  selectedNodeKey: string;
  
  // Metadata expansion state
  expandedMetadataNodesKey: string;
  metadataDisplaySettingsKey: string;
  deletionHistoryKey: string;
  
  // RightPanel drag-and-drop data persistence
  rightPanelDragStateKey: string;
  
  // Canvas state persistence
  canvasStateKey: string;
  
  // General app state
  appStateKey: string;
  
  // Backup and settings
  persistenceSettingsKey: string;
  lastBackupKey: string;
}

export interface AppPersistenceState {
  repositoryData: RepositoryNode[];
  expandedNodes: Set<string>;
  selectedNode: string | null;
  expandedMetadataNodes: Set<string>;
  metadataDisplaySettings: MetadataDisplaySettings;
  deletionHistory: DeletionHistoryItem[];
  rightPanelDragState: any;
  canvasState: CanvasState | null;
}

export interface PersistenceSettings {
  autoSave: boolean;
  debounceTime: number;
  backupInterval: number;
  maxHistory: number;
  metadataAutoExpand: boolean;
  metadataCompactMode: boolean;
}

export class PersistenceService {
  private static instance: PersistenceService;
  private config: PersistenceConfig = {
    repositoryDataKey: 'app_repository_data',
    expandedNodesKey: 'app_expanded_nodes',
    selectedNodeKey: 'app_selected_node',
    expandedMetadataNodesKey: 'app_expanded_metadata_nodes',
    metadataDisplaySettingsKey: 'app_metadata_display_settings',
    deletionHistoryKey: 'app_deletion_history',
    rightPanelDragStateKey: 'app_rightpanel_dragstate',
    canvasStateKey: 'app_canvas_state',
    appStateKey: 'app_global_state',
    persistenceSettingsKey: 'app_persistence_settings',
    lastBackupKey: 'app_last_backup_timestamp'
  };

  private defaultPersistenceSettings: PersistenceSettings = {
    autoSave: true,
    debounceTime: 300,
    backupInterval: 30000,
    maxHistory: 50,
    metadataAutoExpand: true,
    metadataCompactMode: false
  };

  private defaultMetadataSettings: MetadataDisplaySettings = {
    showAll: false,
    autoExpandNew: true,
    compactMode: false,
    showIcons: true,
    maxItemsVisible: 10,
    sortOrder: 'priority'
  };

  private iconMap: Record<string, React.ComponentType<LucideIconProps>>;

  private constructor() {
    this.iconMap = createIconMap();
  }

  static getInstance(): PersistenceService {
    if (!PersistenceService.instance) {
      PersistenceService.instance = new PersistenceService();
    }
    return PersistenceService.instance;
  }

  // ==================== CANVAS STATE PERSISTENCE ====================

  saveCanvasState(nodes: ReactFlowNode[], edges: Edge[], viewport: Viewport): void {
    try {
      const canvasState: CanvasState = {
        nodes: this.sanitizeForStorage(nodes),
        edges: this.sanitizeForStorage(edges),
        viewport: this.sanitizeForStorage(viewport),
        lastSaved: new Date().toISOString(),
        version: '1.0'
      };
      
      localStorage.setItem(this.config.canvasStateKey, JSON.stringify(canvasState));
      console.log('💾 Canvas state saved to localStorage');
    } catch (error) {
      console.error('❌ Failed to save canvas state:', error);
    }
  }

  loadCanvasState(): CanvasState | null {
    try {
      const data = localStorage.getItem(this.config.canvasStateKey);
      if (!data) return null;
      
      const parsedData = JSON.parse(data);
      return {
        nodes: parsedData.nodes || [],
        edges: parsedData.edges || [],
        viewport: parsedData.viewport || { x: 0, y: 0, zoom: 1 },
        lastSaved: parsedData.lastSaved,
        version: parsedData.version
      };
    } catch (error) {
      console.error('❌ Failed to load canvas state:', error);
      return null;
    }
  }

  clearCanvasState(): void {
    try {
      localStorage.removeItem(this.config.canvasStateKey);
      console.log('🧹 Canvas state cleared');
    } catch (error) {
      console.error('❌ Failed to clear canvas state:', error);
    }
  }

  // ==================== REPOSITORY DATA PERSISTENCE ====================

  saveRepositoryData(data: RepositoryNode[]): void {
    try {
      const serializableData = this.sanitizeRepositoryData(data);
      
      const persistenceData: RepositoryPersistenceData = {
        repository: serializableData,
        expandedNodes: Array.from(this.loadExpandedNodes()),
        selectedNode: this.loadSelectedNode(),
        expandedMetadataNodes: Array.from(this.loadExpandedMetadataNodes()),
        metadataDisplaySettings: this.loadMetadataDisplaySettings(),
        lastSaved: new Date().toISOString(),
        version: '2.0'
      };
      
      localStorage.setItem(this.config.repositoryDataKey, JSON.stringify(persistenceData));
      console.log('💾 Repository data saved to localStorage');
    } catch (error) {
      console.error('❌ Failed to save repository data:', error);
    }
  }

  loadRepositoryData(): RepositoryNode[] | null {
    try {
      const data = localStorage.getItem(this.config.repositoryDataKey);
      if (!data) return null;
      
      const parsedData = JSON.parse(data);
      
      // Handle both old and new format
      if (Array.isArray(parsedData)) {
        // Old format - just an array
        return this.hydrateRepositoryData(parsedData);
      } else if (parsedData.repository && Array.isArray(parsedData.repository)) {
        // New format - full persistence data object
        return this.hydrateRepositoryData(parsedData.repository);
      }
      
      return null;
    } catch (error) {
      console.error('❌ Failed to load repository data:', error);
      return null;
    }
  }

  saveExpandedNodes(expandedNodes: Set<string>): void {
    try {
      const nodesArray = Array.from(expandedNodes);
      localStorage.setItem(this.config.expandedNodesKey, JSON.stringify(nodesArray));
    } catch (error) {
      console.error('❌ Failed to save expanded nodes:', error);
    }
  }

  loadExpandedNodes(): Set<string> {
    try {
      const data = localStorage.getItem(this.config.expandedNodesKey);
      if (!data) return new Set();
      
      const nodesArray = JSON.parse(data);
      return new Set(nodesArray);
    } catch (error) {
      console.error('❌ Failed to load expanded nodes:', error);
      return new Set();
    }
  }

  saveSelectedNode(selectedNode: string | null): void {
    try {
      localStorage.setItem(this.config.selectedNodeKey, JSON.stringify(selectedNode));
    } catch (error) {
      console.error('❌ Failed to save selected node:', error);
    }
  }

  loadSelectedNode(): string | null {
    try {
      const data = localStorage.getItem(this.config.selectedNodeKey);
      if (!data) return null;
      
      return JSON.parse(data);
    } catch (error) {
      console.error('❌ Failed to load selected node:', error);
      return null;
    }
  }

  // ==================== METADATA EXPANSION PERSISTENCE ====================

  saveExpandedMetadataNodes(expandedMetadataNodes: Set<string>): void {
    try {
      const nodesArray = Array.from(expandedMetadataNodes);
      localStorage.setItem(this.config.expandedMetadataNodesKey, JSON.stringify(nodesArray));
      console.log('💾 Metadata expansion state saved:', nodesArray.length, 'nodes');
    } catch (error) {
      console.error('❌ Failed to save expanded metadata nodes:', error);
    }
  }

  loadExpandedMetadataNodes(): Set<string> {
    try {
      const data = localStorage.getItem(this.config.expandedMetadataNodesKey);
      if (!data) return new Set();
      
      const nodesArray = JSON.parse(data);
      console.log('📋 Loaded metadata expansion state:', nodesArray.length, 'nodes');
      return new Set(nodesArray);
    } catch (error) {
      console.error('❌ Failed to load expanded metadata nodes:', error);
      return new Set();
    }
  }

  saveMetadataDisplaySettings(settings: Partial<MetadataDisplaySettings>): void {
    try {
      const currentSettings = this.loadMetadataDisplaySettings();
      const mergedSettings = { ...currentSettings, ...settings };
      localStorage.setItem(this.config.metadataDisplaySettingsKey, JSON.stringify(mergedSettings));
      console.log('💾 Metadata display settings saved:', mergedSettings);
    } catch (error) {
      console.error('❌ Failed to save metadata display settings:', error);
    }
  }

  loadMetadataDisplaySettings(): MetadataDisplaySettings {
    try {
      const data = localStorage.getItem(this.config.metadataDisplaySettingsKey);
      if (!data) return { ...this.defaultMetadataSettings };
      
      const settings = JSON.parse(data);
      return { ...this.defaultMetadataSettings, ...settings };
    } catch (error) {
      console.error('❌ Failed to load metadata display settings:', error);
      return { ...this.defaultMetadataSettings };
    }
  }

  toggleMetadataExpansion(nodeId: string): Set<string> {
    const current = this.loadExpandedMetadataNodes();
    const newSet = new Set(current);
    
    if (newSet.has(nodeId)) {
      newSet.delete(nodeId);
    } else {
      newSet.add(nodeId);
    }
    
    this.saveExpandedMetadataNodes(newSet);
    return newSet;
  }

  expandAllMetadataNodes(nodeIds: string[]): void {
    const newSet = new Set(nodeIds);
    this.saveExpandedMetadataNodes(newSet);
    console.log('📖 Expanded all metadata nodes:', nodeIds.length);
  }

  collapseAllMetadataNodes(): void {
    this.saveExpandedMetadataNodes(new Set());
    console.log('📘 Collapsed all metadata nodes');
  }

  // ==================== DELETION HISTORY PERSISTENCE ====================

  saveDeletionHistory(history: DeletionHistoryItem[]): void {
    try {
      const maxHistory = this.loadPersistenceSettings().maxHistory;
      const trimmedHistory = history.slice(-maxHistory);
      
      localStorage.setItem(this.config.deletionHistoryKey, JSON.stringify(trimmedHistory));
    } catch (error) {
      console.error('❌ Failed to save deletion history:', error);
    }
  }

  loadDeletionHistory(): DeletionHistoryItem[] {
    try {
      const data = localStorage.getItem(this.config.deletionHistoryKey);
      if (!data) return [];
      
      const history = JSON.parse(data) as DeletionHistoryItem[];
      return history.map(item => ({
        ...item,
        deletedAt: item.deletedAt || item.timestamp || new Date().toISOString()
      }));
    } catch (error) {
      console.error('❌ Failed to load deletion history:', error);
      return [];
    }
  }

  addToDeletionHistory(node: any, parentId?: string, previousParent?: any): void {
    try {
      const history = this.loadDeletionHistory();
      const deletionItem: DeletionHistoryItem = {
        node: this.sanitizeForStorage(node),
        deletedAt: new Date().toISOString(),
        parentId,
        previousParent
      };
      
      history.push(deletionItem);
      this.saveDeletionHistory(history);
      console.log('🗑️ Added to deletion history:', node.name);
    } catch (error) {
      console.error('❌ Failed to add to deletion history:', error);
    }
  }

  clearDeletionHistory(): void {
    try {
      localStorage.removeItem(this.config.deletionHistoryKey);
      console.log('🗑️ Deletion history cleared');
    } catch (error) {
      console.error('❌ Failed to clear deletion history:', error);
    }
  }

  // ==================== PERSISTENCE SETTINGS ====================

  savePersistenceSettings(settings: Partial<PersistenceSettings>): void {
    try {
      const currentSettings = this.loadPersistenceSettings();
      const mergedSettings = { ...currentSettings, ...settings };
      
      localStorage.setItem(this.config.persistenceSettingsKey, JSON.stringify(mergedSettings));
      console.log('💾 Persistence settings saved:', mergedSettings);
    } catch (error) {
      console.error('❌ Failed to save persistence settings:', error);
    }
  }

  loadPersistenceSettings(): PersistenceSettings {
    try {
      const data = localStorage.getItem(this.config.persistenceSettingsKey);
      if (!data) return { ...this.defaultPersistenceSettings };
      
      const settings = JSON.parse(data);
      return { ...this.defaultPersistenceSettings, ...settings };
    } catch (error) {
      console.error('❌ Failed to load persistence settings:', error);
      return { ...this.defaultPersistenceSettings };
    }
  }

  // ==================== RIGHTPANEL DRAG STATE PERSISTENCE ====================

  saveRightPanelDragState(state: any): void {
    try {
      localStorage.setItem(this.config.rightPanelDragStateKey, JSON.stringify(state));
      console.log('💾 RightPanel drag state saved');
    } catch (error) {
      console.error('❌ Failed to save RightPanel drag state:', error);
    }
  }

  loadRightPanelDragState(): any {
    try {
      const data = localStorage.getItem(this.config.rightPanelDragStateKey);
      if (!data) return null;
      
      return JSON.parse(data);
    } catch (error) {
      console.error('❌ Failed to load RightPanel drag state:', error);
      return null;
    }
  }

  // ==================== COMPLETE APP STATE BACKUP/RESTORE ====================

  saveCompleteState(state: AppPersistenceState): void {
    try {
      const serializedState = {
        ...state,
        repositoryData: state.repositoryData ? this.sanitizeRepositoryData(state.repositoryData) : [],
        expandedNodes: state.expandedNodes ? Array.from(state.expandedNodes) : [],
        expandedMetadataNodes: state.expandedMetadataNodes ? Array.from(state.expandedMetadataNodes) : [],
        canvasState: state.canvasState ? this.sanitizeForStorage(state.canvasState) : null
      };
      
      localStorage.setItem(this.config.appStateKey, JSON.stringify(serializedState));
      console.log('💾 Complete app state saved');
    } catch (error) {
      console.error('❌ Failed to save complete app state:', error);
    }
  }

  loadCompleteState(): AppPersistenceState {
    try {
      const data = localStorage.getItem(this.config.appStateKey);
      if (!data) {
        return this.getDefaultAppState();
      }
      
      const parsedState = JSON.parse(data);
      return {
        repositoryData: parsedState.repositoryData 
          ? this.hydrateRepositoryData(parsedState.repositoryData) 
          : [],
        expandedNodes: parsedState.expandedNodes 
          ? new Set(parsedState.expandedNodes) 
          : new Set(),
        selectedNode: parsedState.selectedNode || null,
        expandedMetadataNodes: parsedState.expandedMetadataNodes 
          ? new Set(parsedState.expandedMetadataNodes) 
          : new Set(),
        metadataDisplaySettings: parsedState.metadataDisplaySettings 
          ? { ...this.defaultMetadataSettings, ...parsedState.metadataDisplaySettings }
          : { ...this.defaultMetadataSettings },
        deletionHistory: parsedState.deletionHistory || [],
        rightPanelDragState: parsedState.rightPanelDragState || null,
        canvasState: parsedState.canvasState || null
      };
    } catch (error) {
      console.error('❌ Failed to load complete app state:', error);
      return this.getDefaultAppState();
    }
  }

  private getDefaultAppState(): AppPersistenceState {
    return {
      repositoryData: [],
      expandedNodes: new Set(),
      selectedNode: null,
      expandedMetadataNodes: new Set(),
      metadataDisplaySettings: { ...this.defaultMetadataSettings },
      deletionHistory: [],
      rightPanelDragState: null,
      canvasState: null
    };
  }

  // ==================== BACKUP AND RESTORE ====================

  createBackup(): RepositoryPersistenceData {
    const backup: RepositoryPersistenceData = {
      repository: this.sanitizeRepositoryData(this.loadRepositoryData() || []),
      expandedNodes: Array.from(this.loadExpandedNodes()),
      selectedNode: this.loadSelectedNode(),
      expandedMetadataNodes: Array.from(this.loadExpandedMetadataNodes()),
      metadataDisplaySettings: this.loadMetadataDisplaySettings(),
      lastSaved: new Date().toISOString(),
      version: '2.0'
    };
    
    try {
      localStorage.setItem(this.config.lastBackupKey, new Date().toISOString());
    } catch (error) {
      console.error('❌ Failed to save backup timestamp:', error);
    }
    
    return backup;
  }

  exportToFile(filename?: string): void {
    try {
      const backup = this.createBackup();
      const blob = new Blob([JSON.stringify(backup, null, 2)], {
        type: 'application/json'
      });
      
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename || `repository-backup-${new Date().toISOString().slice(0, 10)}.json`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
      
      console.log('💾 Repository exported to file');
    } catch (error) {
      console.error('❌ Error exporting repository:', error);
    }
  }

  importFromFile(file: File): Promise<boolean> {
    return new Promise((resolve) => {
      const reader = new FileReader();
      reader.onload = (e) => {
        try {
          const content = e.target?.result as string;
          const data = JSON.parse(content) as RepositoryPersistenceData;
          
          if (!data.repository || !Array.isArray(data.repository)) {
            throw new Error('Invalid repository format');
          }
          
          this.saveRepositoryData(this.hydrateRepositoryData(data.repository));
          
          if (data.expandedNodes && Array.isArray(data.expandedNodes)) {
            this.saveExpandedNodes(new Set(data.expandedNodes));
          }
          
          if (data.selectedNode !== undefined) {
            this.saveSelectedNode(data.selectedNode);
          }
          
          if (data.expandedMetadataNodes && Array.isArray(data.expandedMetadataNodes)) {
            this.saveExpandedMetadataNodes(new Set(data.expandedMetadataNodes));
          }
          
          if (data.metadataDisplaySettings) {
            this.saveMetadataDisplaySettings(data.metadataDisplaySettings);
          }
          
          console.log('📥 Repository imported from file');
          resolve(true);
        } catch (error) {
          console.error('❌ Error importing repository:', error);
          resolve(false);
        }
      };
      reader.readAsText(file);
    });
  }

  // ==================== UTILITY METHODS ====================

  clearAll(): void {
    try {
      Object.values(this.config).forEach(key => {
        localStorage.removeItem(key);
      });
      console.log('🧹 All persistence data cleared');
    } catch (error) {
      console.error('❌ Failed to clear persistence data:', error);
    }
  }

  getStorageStatistics(): {
    totalKeys: number;
    repositorySize: number;
    expandedNodesCount: number;
    expandedMetadataNodesCount: number;
    deletionHistoryCount: number;
    lastBackup: string | null;
    canvasStateExists: boolean;
  } {
    try {
      const expandedNodes = this.loadExpandedNodes();
      const expandedMetadataNodes = this.loadExpandedMetadataNodes();
      const deletionHistory = this.loadDeletionHistory();
      const repositoryData = this.loadRepositoryData();
      
      const repositorySize = repositoryData 
        ? JSON.stringify(this.sanitizeRepositoryData(repositoryData)).length 
        : 0;
      
      const lastBackup = localStorage.getItem(this.config.lastBackupKey);
      const canvasStateExists = !!localStorage.getItem(this.config.canvasStateKey);
      
      return {
        totalKeys: Object.keys(localStorage).filter(key => 
          key.startsWith('app_')
        ).length,
        repositorySize,
        expandedNodesCount: expandedNodes.size,
        expandedMetadataNodesCount: expandedMetadataNodes.size,
        deletionHistoryCount: deletionHistory.length,
        lastBackup,
        canvasStateExists
      };
    } catch (error) {
      console.error('❌ Error getting storage statistics:', error);
      return {
        totalKeys: 0,
        repositorySize: 0,
        expandedNodesCount: 0,
        expandedMetadataNodesCount: 0,
        deletionHistoryCount: 0,
        lastBackup: null,
        canvasStateExists: false
      };
    }
  }

  compactStorage(): void {
    try {
      const statsBefore = this.getStorageStatistics();
      
      const keysToRemove: string[] = [];
      for (let i = 0; i < localStorage.length; i++) {
        const key = localStorage.key(i);
        if (key && (key.startsWith('temp_') || key.includes('_old'))) {
          keysToRemove.push(key);
        }
      }
      
      keysToRemove.forEach(key => localStorage.removeItem(key));
      
      const repositoryData = this.loadRepositoryData();
      if (repositoryData) {
        this.saveRepositoryData(repositoryData);
      }
      
      const statsAfter = this.getStorageStatistics();
      
      console.log('🗜️ Storage compacted:', {
        keysRemoved: keysToRemove.length,
        before: statsBefore.repositorySize,
        after: statsAfter.repositorySize,
        savings: statsBefore.repositorySize - statsAfter.repositorySize
      });
    } catch (error) {
      console.error('❌ Error compacting storage:', error);
    }
  }

  autoBackupIfNeeded(): void {
    try {
      const settings = this.loadPersistenceSettings();
      if (!settings.autoSave) return;
      
      const lastBackup = localStorage.getItem(this.config.lastBackupKey);
      const now = new Date().getTime();
      
      if (!lastBackup || (now - new Date(lastBackup).getTime()) > settings.backupInterval) {
        this.createBackup();
        console.log('🔄 Auto-backup created');
      }
    } catch (error) {
      console.error('❌ Error during auto-backup:', error);
    }
  }

  // ==================== HELPER METHODS FOR REACT COMPONENTS ====================

  private sanitizeRepositoryData(data: RepositoryNode[]): any[] {
    return data.map(node => {
      const sanitizedNode: any = { ...node };
      
      // Remove React components (not serializable)
      if (sanitizedNode.icon) {
        sanitizedNode.iconType = this.extractIconType(sanitizedNode.icon);
        delete sanitizedNode.icon;
      }
      
      // Recursively sanitize children
      if (sanitizedNode.children && Array.isArray(sanitizedNode.children)) {
        sanitizedNode.children = this.sanitizeRepositoryData(sanitizedNode.children);
      }
      
      return sanitizedNode;
    });
  }

  private hydrateRepositoryData(data: any[]): RepositoryNode[] {
    return data.map(node => {
      const hydratedNode: any = { ...node };
      
      // Restore React icon components
      if (hydratedNode.iconType && typeof hydratedNode.iconType === 'string') {
        const iconName = hydratedNode.iconType;
        
        if (this.iconMap[iconName]) {
          const IconComponent = this.iconMap[iconName];
          hydratedNode.icon = React.createElement(IconComponent, { className: "h-4 w-4" });
        }
      }
      delete hydratedNode.iconType;
      
      // Recursively hydrate children
      if (hydratedNode.children && Array.isArray(hydratedNode.children)) {
        hydratedNode.children = this.hydrateRepositoryData(hydratedNode.children);
      }
      
      return hydratedNode;
    });
  }

  private sanitizeForStorage(data: any): any {
    if (data === null || data === undefined) {
      return data;
    }
    
    if (Array.isArray(data)) {
      return data.map(item => this.sanitizeForStorage(item));
    }
    
    if (typeof data === 'object') {
      const sanitized: Record<string, any> = {};
      Object.keys(data).forEach(key => {
        if (
          !(data[key] instanceof Element) &&
          typeof data[key] !== 'function' &&
          !(data[key] instanceof globalThis.Node)
        ) {
          sanitized[key] = this.sanitizeForStorage(data[key]);
        }
      });
      return sanitized;
    }
    
    return data;
  }

  private extractIconType(icon: any): string | null {
    if (icon && icon.type) {
      if (icon.type.displayName) {
        return icon.type.displayName;
      }
      if (icon.type.name) {
        return icon.type.name;
      }
      if (typeof icon.type === 'function') {
        return icon.type.name || null;
      }
    }
    return null;
  }
}

export const persistenceService = PersistenceService.getInstance();