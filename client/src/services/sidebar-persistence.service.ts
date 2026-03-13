// Enhanced SidebarPersistenceService with better migration, sync, and debugging
import { RepositoryNode } from '@/types/types';
import { MetadataDisplaySettings, persistenceService } from './persistence.service';

export interface SidebarPersistedState {
  repositoryData: RepositoryNode[];
  expandedNodes: string[];
  selectedNode: string | null;
  expandedMetadataNodes: string[];
  metadataDisplaySettings: MetadataDisplaySettings;
  lastSaved: string;
  version: string;
  sidebarWidth?: number;
  isCollapsed?: boolean;
  visiblePanels?: string[];
  metadataStatistics?: {
    totalNodesWithMetadata: number;
    expandedMetadataCount: number;
    totalMetadataProperties: number;
  };
}

export class SidebarPersistenceService {
  private static instance: SidebarPersistenceService;
  private readonly storageKey = 'sidebar_complete_state_v3';
  
  // Enhanced legacy keys for backward compatibility
  private legacyKeys = [
    'sidebar_state',
    'sidebar_complete_state_v2',
    'sidebar_complete_state_v1',
    'app_repository_data',
    'app_expanded_nodes',
    'app_selected_node',
    'app_expanded_metadata_nodes',
    'app_metadata_display_settings',
    'repository_data',
    'repository_state'
  ];

  private constructor() {
    // Initialize on creation
    this.initialize();
  }

  static getInstance(): SidebarPersistenceService {
    if (!SidebarPersistenceService.instance) {
      SidebarPersistenceService.instance = new SidebarPersistenceService();
    }
    return SidebarPersistenceService.instance;
  }

  // ==================== INITIALIZATION ====================
  private initialize(): void {
    console.log('🔄 Initializing SidebarPersistenceService');
    this.migrateAndCleanup();
  }

  // ==================== ENHANCED MIGRATION ====================
  private migrateAndCleanup(): void {
    try {
      // Check if we have current data already
      const currentData = localStorage.getItem(this.storageKey);
      if (currentData) {
        console.log('✅ Current sidebar state already exists, skipping migration');
        return;
      }

      console.log('🔍 No current sidebar state found, starting enhanced migration...');
      
      // Try migration from all possible sources
      const migratedData = this.migrateFromAllSources();
      
      if (migratedData) {
        console.log('✅ Successfully migrated sidebar state from legacy sources');
        this.saveCompleteState(migratedData);
        this.cleanupLegacyData();
      } else {
        console.log('📝 No legacy data found to migrate, will create new state on first save');
      }
    } catch (error) {
      console.error('❌ Error during migration:', error);
    }
  }

  private migrateFromAllSources(): Omit<SidebarPersistedState, 'lastSaved' | 'version'> | null {
    const migrationSources = [
      { key: 'sidebar_complete_state_v2', handler: this.convertV2ToV3.bind(this) },
      { key: 'sidebar_state', handler: this.convertLegacyToV3.bind(this) },
      { key: 'app_global_state', handler: this.convertAppStateToV3.bind(this) },
      { key: 'app_repository_data', handler: this.convertRepositoryDataToV3.bind(this) }
    ];

    for (const source of migrationSources) {
      try {
        const data = localStorage.getItem(source.key);
        if (data) {
          console.log(`📋 Attempting migration from ${source.key}`);
          const parsed = JSON.parse(data);
          const migrated = source.handler(parsed);
          if (migrated) {
            console.log(`✅ Successfully migrated from ${source.key}`);
            return migrated;
          }
        }
      } catch (error) {
        console.warn(`⚠️ Failed to migrate from ${source.key}:`, error);
      }
    }

    // Fallback: Check for any repository-like data
    const anyRepositoryData = this.findAnyRepositoryData();
    if (anyRepositoryData) {
      console.log('📦 Found repository data in other storage locations');
      return {
        repositoryData: anyRepositoryData,
        expandedNodes: [],
        selectedNode: null,
        expandedMetadataNodes: [],
        metadataDisplaySettings: this.getDefaultMetadataSettings()
      };
    }

    return null;
  }

  private findAnyRepositoryData(): RepositoryNode[] | null {
    // Search all localStorage for repository-like data
    const allKeys: string[] = [];
    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      if (key) allKeys.push(key);
    }

    // Look for keys that might contain repository data
    const candidateKeys = allKeys.filter(key => 
      key.includes('repository') || 
      key.includes('node') || 
      key.includes('tree') ||
      key.includes('data')
    );

    for (const key of candidateKeys) {
      try {
        const data = localStorage.getItem(key);
        if (data) {
          const parsed = JSON.parse(data);
          if (Array.isArray(parsed) && parsed.length > 0 && parsed[0].id && parsed[0].name) {
            console.log(`📦 Found repository data in key: ${key}`);
            return parsed;
          }
        }
      } catch (error) {
        // Silently continue
      }
    }

    return null;
  }

  private convertV2ToV3(v2Data: any): Omit<SidebarPersistedState, 'lastSaved' | 'version'> {
    console.log('Converting V2 to V3:', v2Data);
    return {
      repositoryData: v2Data.repositoryData || v2Data.repository || [],
      expandedNodes: Array.isArray(v2Data.expandedNodes) 
        ? v2Data.expandedNodes 
        : v2Data.expandedNodes ? Array.from(v2Data.expandedNodes) : [],
      selectedNode: v2Data.selectedNode || null,
      expandedMetadataNodes: Array.isArray(v2Data.expandedMetadataNodes)
        ? v2Data.expandedMetadataNodes
        : v2Data.expandedMetadataNodes ? Array.from(v2Data.expandedMetadataNodes) : [],
      metadataDisplaySettings: v2Data.metadataDisplaySettings || 
                               v2Data.metadataSettings || 
                               this.getDefaultMetadataSettings()
    };
  }

  private convertLegacyToV3(legacyData: any): Omit<SidebarPersistedState, 'lastSaved' | 'version'> {
    console.log('Converting legacy format:', legacyData);
    return {
      repositoryData: legacyData.repositoryData || legacyData.repository || [],
      expandedNodes: Array.isArray(legacyData.expandedNodes) 
        ? legacyData.expandedNodes 
        : legacyData.expandedNodes ? Array.from(legacyData.expandedNodes) : [],
      selectedNode: legacyData.selectedNode || null,
      expandedMetadataNodes: Array.isArray(legacyData.expandedMetadataNodes)
        ? legacyData.expandedMetadataNodes
        : legacyData.expandedMetadataNodes ? Array.from(legacyData.expandedMetadataNodes) : [],
      metadataDisplaySettings: legacyData.metadataDisplaySettings || 
                               legacyData.metadataSettings || 
                               this.getDefaultMetadataSettings()
    };
  }

  private convertAppStateToV3(appState: any): Omit<SidebarPersistedState, 'lastSaved' | 'version'> {
    console.log('Converting app state to V3:', appState);
    return {
      repositoryData: appState.repositoryData || [],
      expandedNodes: appState.expandedNodes ? Array.from(appState.expandedNodes) : [],
      selectedNode: appState.selectedNode || null,
      expandedMetadataNodes: appState.expandedMetadataNodes ? 
        Array.from(appState.expandedMetadataNodes) : [],
      metadataDisplaySettings: appState.metadataDisplaySettings || 
                               this.getDefaultMetadataSettings()
    };
  }

  private convertRepositoryDataToV3(repoData: any): Omit<SidebarPersistedState, 'lastSaved' | 'version'> {
    console.log('Converting repository data to V3');
    return {
      repositoryData: Array.isArray(repoData) ? repoData : 
                     (repoData.repositoryData || repoData.repository || []),
      expandedNodes: [],
      selectedNode: null,
      expandedMetadataNodes: [],
      metadataDisplaySettings: this.getDefaultMetadataSettings()
    };
  }

  private getDefaultMetadataSettings(): MetadataDisplaySettings {
    return {
      showAll: false,
      autoExpandNew: true,
      compactMode: false,
      showIcons: true,
      maxItemsVisible: 10,
      sortOrder: 'priority'
    };
  }

  // ==================== MAIN PERSISTENCE METHODS ====================
  saveCompleteState(state: Omit<SidebarPersistedState, 'lastSaved' | 'version'>): void {
    try {
      // Calculate metadata statistics
      const metadataStats = this.calculateMetadataStatistics(state.repositoryData);
      
      const completeState: SidebarPersistedState = {
        ...state,
        expandedNodes: Array.isArray(state.expandedNodes) 
          ? state.expandedNodes 
          : Array.from(state.expandedNodes || []),
        expandedMetadataNodes: Array.isArray(state.expandedMetadataNodes)
          ? state.expandedMetadataNodes
          : Array.from(state.expandedMetadataNodes || []),
        lastSaved: new Date().toISOString(),
        version: '3.0',
        metadataStatistics: metadataStats
      };

      // Serialize with compression for large data
      const serializedState = JSON.stringify(completeState);
      
      // Check storage capacity
      if (serializedState.length > 5 * 1024 * 1024) { // 5MB limit
        console.warn('⚠️ Sidebar state is large, compressing...');
        this.saveCompressedState(completeState);
      } else {
        localStorage.setItem(this.storageKey, serializedState);
      }
      
      console.log('💾 Sidebar state saved:', {
        nodes: state.repositoryData.length,
        expanded: completeState.expandedNodes.length,
        metadataExpanded: completeState.expandedMetadataNodes.length,
        size: `${(serializedState.length / 1024).toFixed(2)} KB`
      });

      // Optional: Sync with main persistence service for compatibility
      this.syncWithMainPersistence(state);

    } catch (error) {
      console.error('❌ Failed to save sidebar state:', error);
      this.handleStorageError(error, state);
    }
  }

  private saveCompressedState(state: SidebarPersistedState): void {
    try {
      // Simple compression by removing whitespace
      const compressed = JSON.stringify(state);
      localStorage.setItem(this.storageKey, compressed);
      
      // Also store a backup in sessionStorage
      sessionStorage.setItem(`${this.storageKey}_backup`, compressed);
      
      console.log('🗜️ Compressed sidebar state saved');
    } catch (error) {
      console.error('❌ Failed to save compressed state:', error);
      // Fall back to regular save
      localStorage.setItem(this.storageKey, JSON.stringify(state));
    }
  }

  private syncWithMainPersistence(state: Omit<SidebarPersistedState, 'lastSaved' | 'version'>): void {
    try {
      persistenceService.saveCompleteState({
        repositoryData: state.repositoryData,
        expandedNodes: new Set(state.expandedNodes),
        selectedNode: state.selectedNode,
        expandedMetadataNodes: new Set(state.expandedMetadataNodes),
        metadataDisplaySettings: state.metadataDisplaySettings,
        deletionHistory: [],
        rightPanelDragState: null,
        canvasState: null
      });
    } catch (error) {
      console.warn('⚠️ Could not sync with main persistence:', error);
    }
  }

  loadCompleteState(): SidebarPersistedState | null {
    try {
      // Try to load from current format
      const data = localStorage.getItem(this.storageKey);
      
      if (data) {
        const parsed = this.validateAndParse(data);
        console.log('📂 Loaded sidebar state from current format:', {
          nodes: parsed.repositoryData.length,
          expanded: parsed.expandedNodes.length,
          version: parsed.version
        });
        return parsed;
      }

      // Try to load from backup
      const backupData = sessionStorage.getItem(`${this.storageKey}_backup`);
      if (backupData) {
        console.log('📦 Loading from sessionStorage backup');
        const parsed = this.validateAndParse(backupData);
        this.saveCompleteState(parsed); // Restore to localStorage
        return parsed;
      }

      // Auto-migrate if no data found
      console.log('🔍 No current sidebar state found, triggering auto-migration');
      const migrated = this.migrateFromAllSources();
      if (migrated) {
        this.saveCompleteState(migrated);
        return {
          ...migrated,
          lastSaved: new Date().toISOString(),
          version: '3.0'
        };
      }

      console.log('📝 No sidebar state found anywhere');
      return null;

    } catch (error) {
      console.error('❌ Failed to load sidebar state:', error);
      return this.loadFromBackupOrRecover();
    }
  }

  private validateAndParse(data: string): SidebarPersistedState {
    try {
      const parsed = JSON.parse(data);
      
      // Validate structure
      if (!parsed.repositoryData || !Array.isArray(parsed.repositoryData)) {
        throw new Error('Invalid repository data structure');
      }

      // Ensure all required fields exist
      const validatedState: SidebarPersistedState = {
        repositoryData: parsed.repositoryData,
        expandedNodes: Array.isArray(parsed.expandedNodes) ? parsed.expandedNodes : [],
        selectedNode: parsed.selectedNode || null,
        expandedMetadataNodes: Array.isArray(parsed.expandedMetadataNodes) ? 
          parsed.expandedMetadataNodes : [],
        metadataDisplaySettings: {
          ...this.getDefaultMetadataSettings(),
          ...(parsed.metadataDisplaySettings || {})
        },
        lastSaved: parsed.lastSaved || new Date().toISOString(),
        version: parsed.version || '1.0'
      };

      // Recalculate metadata statistics
      validatedState.metadataStatistics = this.calculateMetadataStatistics(validatedState.repositoryData);

      // Upgrade old versions
      if (!parsed.version || parsed.version === '1.0' || parsed.version === '2.0') {
        console.log(`🔄 Upgrading sidebar state from version ${parsed.version || '1.0'} to 3.0`);
        this.saveCompleteState(validatedState);
      }

      return validatedState;
    } catch (error) {
      console.error('❌ Validation error:', error);
      throw new Error(`Invalid sidebar state: ${error.message}`);
    }
  }

  private loadFromBackupOrRecover(): SidebarPersistedState | null {
    console.log('🔄 Attempting recovery from backup...');
    
    // Check for any backup data
    const backupSources = [
      `${this.storageKey}_backup`,
      'sidebar_state_backup',
      'app_repository_data_backup'
    ];

    for (const source of backupSources) {
      try {
        const data = localStorage.getItem(source) || sessionStorage.getItem(source);
        if (data) {
          console.log(`📦 Found backup in ${source}`);
          const parsed = JSON.parse(data);
          if (parsed.repositoryData) {
            const recoveredState: SidebarPersistedState = {
              repositoryData: parsed.repositoryData || [],
              expandedNodes: [],
              selectedNode: null,
              expandedMetadataNodes: [],
              metadataDisplaySettings: this.getDefaultMetadataSettings(),
              lastSaved: new Date().toISOString(),
              version: '3.0'
            };
            this.saveCompleteState(recoveredState);
            return recoveredState;
          }
        }
      } catch (error) {
        // Continue to next source
      }
    }

    // Last resort: check for any data in the system
    const anyData = this.findAnyRepositoryData();
    if (anyData) {
      console.log('🚨 Emergency recovery: Found repository data in system');
      const emergencyState: SidebarPersistedState = {
        repositoryData: anyData,
        expandedNodes: [],
        selectedNode: null,
        expandedMetadataNodes: [],
        metadataDisplaySettings: this.getDefaultMetadataSettings(),
        lastSaved: new Date().toISOString(),
        version: '3.0'
      };
      this.saveCompleteState(emergencyState);
      return emergencyState;
    }

    return null;
  }

  // ==================== UTILITY METHODS ====================
  clear(): void {
    try {
      localStorage.removeItem(this.storageKey);
      sessionStorage.removeItem(`${this.storageKey}_backup`);
      this.cleanupLegacyData();
      console.log('🧹 Sidebar state cleared');
    } catch (error) {
      console.error('❌ Failed to clear sidebar state:', error);
    }
  }

  cleanupLegacyData(): void {
    let cleanedCount = 0;
    this.legacyKeys.forEach(key => {
      if (localStorage.getItem(key)) {
        localStorage.removeItem(key);
        cleanedCount++;
      }
      if (sessionStorage.getItem(key)) {
        sessionStorage.removeItem(key);
        cleanedCount++;
      }
    });
    console.log(`🗑️ Cleaned up ${cleanedCount} legacy keys`);
  }

  private calculateMetadataStatistics(repositoryData: RepositoryNode[]): {
    totalNodesWithMetadata: number;
    expandedMetadataCount: number;
    totalMetadataProperties: number;
  } {
    let totalNodesWithMetadata = 0;
    let totalMetadataProperties = 0;
    
    const countMetadata = (nodes: RepositoryNode[]) => {
      nodes.forEach(node => {
        if (node.metadata && Object.keys(node.metadata).length > 0) {
          totalNodesWithMetadata++;
          totalMetadataProperties += Object.keys(node.metadata).length;
        }
        if (node.children) {
          countMetadata(node.children);
        }
      });
    };
    
    countMetadata(repositoryData);
    
    return {
      totalNodesWithMetadata,
      expandedMetadataCount: 0, // This should be calculated elsewhere with expandedMetadataNodes
      totalMetadataProperties
    };
  }

  private handleStorageError(error: any, state: Omit<SidebarPersistedState, 'lastSaved' | 'version'>): void {
    console.error('💾 Storage error details:', error);
    
    if (error.name === 'QuotaExceededError' || error.code === 22) {
      console.warn('⚠️ Storage quota exceeded, attempting to free space...');
      
      // Try to clear old data
      this.cleanupLegacyData();
      
      // Try to save minimal version
      const minimalState = {
        ...state,
        repositoryData: state.repositoryData.slice(0, 50) // Keep only first 50 items
      };
      
      try {
        localStorage.setItem(this.storageKey, JSON.stringify({
          ...minimalState,
          lastSaved: new Date().toISOString(),
          version: '3.0',
          truncated: true
        }));
        console.log('✅ Saved truncated sidebar state');
      } catch (retryError) {
        console.error('❌ Could not save even truncated state:', retryError);
      }
    }
  }

  // ==================== PARTIAL UPDATES ====================
  savePartialState(updates: Partial<SidebarPersistedState>): void {
    const currentState = this.loadCompleteState();
    if (currentState) {
      this.saveCompleteState({
        repositoryData: updates.repositoryData || currentState.repositoryData,
        expandedNodes: updates.expandedNodes || currentState.expandedNodes,
        selectedNode: updates.selectedNode !== undefined ? 
          updates.selectedNode : currentState.selectedNode,
        expandedMetadataNodes: updates.expandedMetadataNodes || currentState.expandedMetadataNodes,
        metadataDisplaySettings: {
          ...currentState.metadataDisplaySettings,
          ...(updates.metadataDisplaySettings || {})
        }
      });
    } else {
      // Create new state with defaults and updates
      this.saveCompleteState({
        repositoryData: updates.repositoryData || [],
        expandedNodes: Array.isArray(updates.expandedNodes) 
          ? updates.expandedNodes 
          : updates.expandedNodes ? Array.from(updates.expandedNodes) : [],
        selectedNode: updates.selectedNode || null,
        expandedMetadataNodes: Array.isArray(updates.expandedMetadataNodes)
          ? updates.expandedMetadataNodes
          : updates.expandedMetadataNodes ? Array.from(updates.expandedMetadataNodes) : [],
        metadataDisplaySettings: {
          ...this.getDefaultMetadataSettings(),
          ...(updates.metadataDisplaySettings || {})
        }
      });
    }
  }

  // ==================== DEBOUNCED SAVE ====================
  private debounceTimeout: NodeJS.Timeout | null = null;
  
  saveWithDebounce(state: Omit<SidebarPersistedState, 'lastSaved' | 'version'>, delay: number = 1000): void {
    if (this.debounceTimeout) {
      clearTimeout(this.debounceTimeout);
    }
    
    this.debounceTimeout = setTimeout(() => {
      this.saveCompleteState(state);
      this.debounceTimeout = null;
    }, delay);
  }

  // ==================== DIAGNOSTICS AND DEBUGGING ====================
  getStateInfo(): {
    exists: boolean;
    version: string | null;
    lastSaved: string | null;
    size: number;
    nodeCount: number;
    expandedCount: number;
    metadataExpandedCount: number;
  } {
    try {
      const data = localStorage.getItem(this.storageKey);
      if (!data) {
        return { 
          exists: false, 
          version: null, 
          lastSaved: null, 
          size: 0,
          nodeCount: 0,
          expandedCount: 0,
          metadataExpandedCount: 0
        };
      }
      
      const parsed = JSON.parse(data);
      const nodeCount = parsed.repositoryData?.length || 0;
      const expandedCount = Array.isArray(parsed.expandedNodes) ? parsed.expandedNodes.length : 0;
      const metadataExpandedCount = Array.isArray(parsed.expandedMetadataNodes) ? 
        parsed.expandedMetadataNodes.length : 0;
      
      return {
        exists: true,
        version: parsed.version || null,
        lastSaved: parsed.lastSaved || null,
        size: data.length,
        nodeCount,
        expandedCount,
        metadataExpandedCount
      };
    } catch {
      return { 
        exists: false, 
        version: null, 
        lastSaved: null, 
        size: 0,
        nodeCount: 0,
        expandedCount: 0,
        metadataExpandedCount: 0
      };
    }
  }

  getDiagnostics(): any {
    const stateInfo = this.getStateInfo();
    const allKeys: string[] = [];
    
    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      if (key) allKeys.push(key);
    }
    
    const sidebarKeys = allKeys.filter(key => 
      key.includes('sidebar') || key.includes('repository') || key.includes('app_')
    );
    
    const sidebarData: Record<string, any> = {};
    sidebarKeys.forEach(key => {
      try {
        const data = localStorage.getItem(key);
        sidebarData[key] = data ? JSON.parse(data) : null;
      } catch (e) {
        sidebarData[key] = 'Error parsing';
      }
    });
    
    return {
      timestamp: new Date().toISOString(),
      stateInfo,
      sidebarKeys,
      sidebarKeyCount: sidebarKeys.length,
      totalLocalStorageKeys: localStorage.length,
      sampleData: Object.keys(sidebarData).reduce((acc, key) => {
        if (key === this.storageKey) {
          const data = sidebarData[key];
          acc[key] = {
            exists: !!data,
            nodeCount: data?.repositoryData?.length || 0,
            expandedCount: Array.isArray(data?.expandedNodes) ? data.expandedNodes.length : 0
          };
        } else {
          acc[key] = sidebarData[key] !== null ? 'Present' : 'Missing';
        }
        return acc;
      }, {} as Record<string, any>)
    };
  }

  // ==================== EXPORT/IMPORT ====================
  exportState(): string {
    const state = this.loadCompleteState();
    if (!state) {
      throw new Error('No state to export');
    }
    
    const exportData = {
      ...state,
      exportVersion: '1.0',
      exportedAt: new Date().toISOString(),
      source: 'SidebarPersistenceService'
    };
    
    return JSON.stringify(exportData, null, 2);
  }

  importState(jsonString: string): boolean {
    try {
      const imported = JSON.parse(jsonString);
      
      if (!imported.repositoryData || !Array.isArray(imported.repositoryData)) {
        throw new Error('Invalid import data: missing repositoryData array');
      }
      
      this.saveCompleteState({
        repositoryData: imported.repositoryData,
        expandedNodes: imported.expandedNodes || [],
        selectedNode: imported.selectedNode || null,
        expandedMetadataNodes: imported.expandedMetadataNodes || [],
        metadataDisplaySettings: imported.metadataDisplaySettings || this.getDefaultMetadataSettings()
      });
      
      console.log('✅ Successfully imported sidebar state');
      return true;
    } catch (error) {
      console.error('❌ Failed to import sidebar state:', error);
      return false;
    }
  }

  // ==================== BACKUP MANAGEMENT ====================
  createBackup(): string {
    const state = this.loadCompleteState();
    if (!state) {
      throw new Error('No state to backup');
    }
    
    const backup = {
      ...state,
      backupId: `backup_${Date.now()}`,
      backupCreated: new Date().toISOString()
    };
    
    const backupKey = `${this.storageKey}_backup_${Date.now()}`;
    localStorage.setItem(backupKey, JSON.stringify(backup));
    
    console.log(`💾 Created backup: ${backupKey}`);
    return backupKey;
  }

  listBackups(): string[] {
    const backups: string[] = [];
    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      if (key && key.startsWith(`${this.storageKey}_backup_`)) {
        backups.push(key);
      }
    }
    return backups.sort().reverse(); // Newest first
  }

  restoreBackup(backupKey: string): boolean {
    try {
      const backupData = localStorage.getItem(backupKey);
      if (!backupData) {
        throw new Error(`Backup not found: ${backupKey}`);
      }
      
      const backup = JSON.parse(backupData);
      this.saveCompleteState({
        repositoryData: backup.repositoryData,
        expandedNodes: backup.expandedNodes,
        selectedNode: backup.selectedNode,
        expandedMetadataNodes: backup.expandedMetadataNodes,
        metadataDisplaySettings: backup.metadataDisplaySettings
      });
      
      console.log(`✅ Restored from backup: ${backupKey}`);
      return true;
    } catch (error) {
      console.error(`❌ Failed to restore backup ${backupKey}:`, error);
      return false;
    }
  }

  cleanupOldBackups(maxBackups: number = 5): void {
    const backups = this.listBackups();
    if (backups.length > maxBackups) {
      const toDelete = backups.slice(maxBackups);
      toDelete.forEach(key => {
        localStorage.removeItem(key);
        console.log(`🗑️ Deleted old backup: ${key}`);
      });
    }
  }
}

export const sidebarPersistenceService = SidebarPersistenceService.getInstance();