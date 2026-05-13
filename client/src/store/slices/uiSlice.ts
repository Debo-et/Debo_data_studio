// src/store/slices/uiSlice.ts - UPDATED with flexible metadata
import { createSlice, PayloadAction } from '@reduxjs/toolkit';

// Enhanced Component Metadata Interface
export interface ComponentMetadata {
  id: string;
  name: string;
  type: string;
  description: string;
  category: string;
  icon: string;
  metadata: {
    // Canvas-specific properties (for components on canvas)
    position?: { x: number; y: number };
    size?: { width: number; height: number };
    connectionPorts?: Array<{
      id: string;
      type: 'input' | 'output';
      side: 'left' | 'right';
      position: number;
    }>;
    
    // Repository-specific properties (for components in sidebar)
    repositorySize?: string; // For repository nodes (like "10KB")
    lastModified?: string;
    dataType?: string;
    length?: number;
    precision?: number;
    scale?: number;
    isKey?: boolean;
    nullable?: boolean;
    defaultValue?: string;
    
    // Common properties
    source: 'canvas-double-click' | 'repository-double-click' | 'component-palette';
    timestamp?: string;
    
    // Flexible additional properties
    canvasNodeId?: string;
    canvasNodeData?: any;
    children?: any[];
    isProcessingComponent?: boolean;
    description?: string;
    [key: string]: any;
  };
}

// Extended UI State
interface UIState {
  sidebarOpen: boolean;
  propertiesPanelOpen: boolean;
  consoleHeight: number;
  theme: 'light' | 'dark';
  zoomLevel: number;
  isCanvasDragging: boolean;
  lastSaved: number | null;
  
  // NEW: Right Panel State
  rightPanelView: 'components' | 'node-properties' | 'job-properties';
  nodePropertiesTab: 'basic' | 'advanced';
  selectedComponentMetadata: ComponentMetadata | null;
  
  // NEW: Modal States
  isMapEditorOpen: boolean;
  isSchemaEditorOpen: boolean;
  isPreviewOpen: boolean;
  
  // NEW: Panel Sizes
  leftPanelWidth: number;
  rightPanelWidth: number;
  bottomPanelHeight: number;
  
  // NEW: Canvas State
  canvasGridSize: number;
  showGrid: boolean;
  snapToGrid: boolean;
  
  // NEW: Component Settings
  lastEditedComponent: {
    id: string | null;
    type: string | null;
    timestamp: number | null;
  };
}

const initialState: UIState = {
  sidebarOpen: true,
  propertiesPanelOpen: true,
  consoleHeight: 200,
  theme: 'light',
  zoomLevel: 1,
  isCanvasDragging: false,
  lastSaved: null,
  
  // NEW Initial Values
  rightPanelView: 'components',
  nodePropertiesTab: 'basic',
  selectedComponentMetadata: null,
  
  isMapEditorOpen: false,
  isSchemaEditorOpen: false,
  isPreviewOpen: false,
  
  leftPanelWidth: 280,
  rightPanelWidth: 320,
  bottomPanelHeight: 200,
  
  canvasGridSize: 20,
  showGrid: true,
  snapToGrid: true,
  
  lastEditedComponent: {
    id: null,
    type: null,
    timestamp: null
  }
};

const uiSlice = createSlice({
  name: 'ui',
  initialState,
  reducers: {
    // Existing Reducers
    toggleSidebar: (state) => {
      state.sidebarOpen = !state.sidebarOpen;
    },
    
    togglePropertiesPanel: (state) => {
      state.propertiesPanelOpen = !state.propertiesPanelOpen;
    },
    
    setConsoleHeight: (state, action: PayloadAction<number>) => {
      state.consoleHeight = Math.max(100, Math.min(500, action.payload));
    },
    
    setTheme: (state, action: PayloadAction<'light' | 'dark'>) => {
      state.theme = action.payload;
    },
    
    setZoomLevel: (state, action: PayloadAction<number>) => {
      state.zoomLevel = Math.max(0.1, Math.min(3, action.payload));
    },
    
    setCanvasDragging: (state, action: PayloadAction<boolean>) => {
      state.isCanvasDragging = action.payload;
    },
    
    setLastSaved: (state, action: PayloadAction<number | null>) => {
      state.lastSaved = action.payload;
    },
    
    // NEW: Right Panel Reducers
    setRightPanelView: (state, action: PayloadAction<UIState['rightPanelView']>) => {
      state.rightPanelView = action.payload;
    },
    
    setNodePropertiesTab: (state, action: PayloadAction<UIState['nodePropertiesTab']>) => {
      state.nodePropertiesTab = action.payload;
    },
    
    setSelectedComponentMetadata: (state, action: PayloadAction<ComponentMetadata | null>) => {
      state.selectedComponentMetadata = action.payload;
      
      // Update last edited component
      if (action.payload) {
        state.lastEditedComponent = {
          id: action.payload.id,
          type: action.payload.type,
          timestamp: Date.now()
        };
      }
    },
    
    clearSelectedComponentMetadata: (state) => {
      state.selectedComponentMetadata = null;
    },
    
    // NEW: Modal Reducers
    openMapEditor: (state) => {
      state.isMapEditorOpen = true;
    },
    
    closeMapEditor: (state) => {
      state.isMapEditorOpen = false;
    },
    
    openSchemaEditor: (state) => {
      state.isSchemaEditorOpen = true;
    },
    
    closeSchemaEditor: (state) => {
      state.isSchemaEditorOpen = false;
    },
    
    openPreview: (state) => {
      state.isPreviewOpen = true;
    },
    
    closePreview: (state) => {
      state.isPreviewOpen = false;
    },
    
    // NEW: Panel Size Reducers
    setLeftPanelWidth: (state, action: PayloadAction<number>) => {
      state.leftPanelWidth = Math.max(200, Math.min(500, action.payload));
    },
    
    setRightPanelWidth: (state, action: PayloadAction<number>) => {
      state.rightPanelWidth = Math.max(200, Math.min(500, action.payload));
    },
    
    setBottomPanelHeight: (state, action: PayloadAction<number>) => {
      state.bottomPanelHeight = Math.max(100, Math.min(500, action.payload));
    },
    
    // NEW: Canvas Settings Reducers
    setCanvasGridSize: (state, action: PayloadAction<number>) => {
      state.canvasGridSize = Math.max(10, Math.min(50, action.payload));
    },
    
    toggleGrid: (state) => {
      state.showGrid = !state.showGrid;
    },
    
    toggleSnapToGrid: (state) => {
      state.snapToGrid = !state.snapToGrid;
    },
    
    // NEW: Component History
    setLastEditedComponent: (state, action: PayloadAction<{id: string, type: string}>) => {
      state.lastEditedComponent = {
        id: action.payload.id,
        type: action.payload.type,
        timestamp: Date.now()
      };
    },
    
    clearLastEditedComponent: (state) => {
      state.lastEditedComponent = {
        id: null,
        type: null,
        timestamp: null
      };
    },
    
    // NEW: Quick Actions
    switchToComponentsView: (state) => {
      state.rightPanelView = 'components';
      state.nodePropertiesTab = 'basic';
      state.selectedComponentMetadata = null;
    },
    
    switchToNodeProperties: (state) => {
      state.rightPanelView = 'node-properties';
      state.nodePropertiesTab = 'basic';
    },
    
    switchToAdvancedNodeProperties: (state) => {
      state.rightPanelView = 'node-properties';
      state.nodePropertiesTab = 'advanced';
    },
    
    switchToJobProperties: (state) => {
      state.rightPanelView = 'job-properties';
    },
    
    // Enhanced Reset
    resetUI: (state) => {
      // Preserve theme and panel sizes
      const { theme, leftPanelWidth, rightPanelWidth, bottomPanelHeight } = state;
      
      return {
        ...initialState,
        theme,
        leftPanelWidth,
        rightPanelWidth,
        bottomPanelHeight
      };
    },
    
    // NEW: Batch Update
    updateUISettings: (state, action: PayloadAction<Partial<UIState>>) => {
      return {
        ...state,
        ...action.payload
      };
    },
  },
});

export const {
  // Original Actions
  toggleSidebar,
  togglePropertiesPanel,
  setConsoleHeight,
  setTheme,
  setZoomLevel,
  setCanvasDragging,
  setLastSaved,
  resetUI,
  
  // New Actions
  setRightPanelView,
  setNodePropertiesTab,
  setSelectedComponentMetadata,
  clearSelectedComponentMetadata,
  
  openMapEditor,
  closeMapEditor,
  openSchemaEditor,
  closeSchemaEditor,
  openPreview,
  closePreview,
  
  setLeftPanelWidth,
  setRightPanelWidth,
  setBottomPanelHeight,
  
  setCanvasGridSize,
  toggleGrid,
  toggleSnapToGrid,
  
  setLastEditedComponent,
  clearLastEditedComponent,
  
  switchToComponentsView,
  switchToNodeProperties,
  switchToAdvancedNodeProperties,
  switchToJobProperties,
  
  updateUISettings,
} = uiSlice.actions;

// Selectors
export const selectUI = (state: { ui: UIState }) => state.ui;
export const selectRightPanelView = (state: { ui: UIState }) => state.ui.rightPanelView;
export const selectNodePropertiesTab = (state: { ui: UIState }) => state.ui.nodePropertiesTab;
export const selectSelectedComponentMetadata = (state: { ui: UIState }) => state.ui.selectedComponentMetadata;
export const selectIsMapEditorOpen = (state: { ui: UIState }) => state.ui.isMapEditorOpen;
export const selectIsSchemaEditorOpen = (state: { ui: UIState }) => state.ui.isSchemaEditorOpen;
export const selectIsPreviewOpen = (state: { ui: UIState }) => state.ui.isPreviewOpen;
export const selectLastEditedComponent = (state: { ui: UIState }) => state.ui.lastEditedComponent;
export const selectCanvasSettings = (state: { ui: UIState }) => ({
  gridSize: state.ui.canvasGridSize,
  showGrid: state.ui.showGrid,
  snapToGrid: state.ui.snapToGrid
});

export default uiSlice.reducer;