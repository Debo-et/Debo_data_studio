// CanvasContext.tsx
import React, { createContext, useContext, useState, useCallback } from 'react';
import { Viewport } from 'reactflow';
import { ValidationSummary } from '../validation'; // Adjust path as needed

// Define the CanvasData interface with all required properties
interface CanvasData {
  nodes: any[];
  edges: any[];
  connections: EnhancedCanvasConnection[];
  validationSummary: ValidationSummary | null;
  viewport: Viewport;
  sqlGeneration: Record<string, SQLGenerationState>;
}

// SQL Generation State interface
interface SQLGenerationState {
  sql: string;
  isValid: boolean;
  errors: string[];
  warnings: string[];
  lastGenerated: string | null;
  isGenerating: boolean;
  hasDefaultMappings: boolean;
  mappingStats?: any;
}

// Enhanced Canvas Connection interface
interface EnhancedCanvasConnection {
  id: string;
  sourceNodeId: string;
  sourcePortId: string;
  targetNodeId: string;
  targetPortId: string;
  status: 'valid' | 'invalid' | 'pending';
  metadata?: Record<string, any>;
}

// Canvas Context Type
interface CanvasContextType {
  canvasData: CanvasData;
  updateCanvasData: (data: Partial<CanvasData>) => void;
  requestCanvasData: () => CanvasData;
  triggerSQLGeneration: () => void;
  getCanvasSummary: () => CanvasSummary;
  exportCanvasData: () => CanvasExportData;
  
  // NEW: Synchronization methods
  syncNodesAndEdges: (nodes: any[], edges: any[]) => void;
  subscribeToCanvasUpdates: (callback: (data: CanvasData) => void) => () => void;
  getCanvasState: () => { nodes: any[], edges: any[] };
}

// Canvas Summary interface
interface CanvasSummary {
  nodeCount: number;
  edgeCount: number;
  connectionCount: number;
  validationStatus: 'valid' | 'invalid' | 'pending';
  validationErrors: number;
  validationWarnings: number;
  sqlGeneratedNodes: number;
  viewport: Viewport;
}

// Canvas Export Data interface
interface CanvasExportData {
  nodes: any[];
  connections: EnhancedCanvasConnection[];
  edges: any[];
  validationSummary: ValidationSummary | null;
  sqlGeneration: Record<string, SQLGenerationState>;
  viewport: Viewport;
  nodeCount: number;
  edgeCount: number;
  connectionCount: number;
  isValid: boolean;
  validationErrors: number;
  validationWarnings: number;
}

// Create the context
const CanvasContext = createContext<CanvasContextType | undefined>(undefined);

// Initial state for canvas data
const initialCanvasData: CanvasData = {
  nodes: [],
  edges: [],
  connections: [],
  validationSummary: null,
  viewport: { x: 0, y: 0, zoom: 1 },
  sqlGeneration: {}
};

export const CanvasProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [canvasData, setCanvasData] = useState<CanvasData>(initialCanvasData);
  const [subscribers, setSubscribers] = useState<Array<(data: CanvasData) => void>>([]);

  // Notify all subscribers when canvas data changes
  const notifySubscribers = useCallback((data: CanvasData) => {
    subscribers.forEach(callback => callback(data));
  }, [subscribers]);

  // Subscribe to canvas updates
  const subscribeToCanvasUpdates = useCallback((callback: (data: CanvasData) => void) => {
    setSubscribers(prev => [...prev, callback]);
    
    // Return unsubscribe function
    return () => {
      setSubscribers(prev => prev.filter(cb => cb !== callback));
    };
  }, []);

  // Helper function to create EnhancedCanvasConnection from Edge
  const createEnhancedConnectionFromEdge = (edge: any): EnhancedCanvasConnection => {
    return {
      id: edge.id,
      sourceNodeId: edge.source,
      sourcePortId: edge.sourceHandle || '',
      targetNodeId: edge.target,
      targetPortId: edge.targetHandle || '',
      status: 'valid', // TypeScript now knows this is the literal 'valid'
      metadata: edge.data || {}
    };
  };

  // Sync nodes and edges with context
  const syncNodesAndEdges = useCallback((nodes: any[], edges: any[]) => {
    setCanvasData(prev => {
      // Create EnhancedCanvasConnection array from edges
      const connections: EnhancedCanvasConnection[] = edges.map(createEnhancedConnectionFromEdge);
      
      const newData: CanvasData = {
        ...prev,
        nodes,
        edges,
        connections,
        // Preserve other properties from previous state
        validationSummary: prev.validationSummary,
        viewport: prev.viewport,
        sqlGeneration: prev.sqlGeneration
      };
      
      // Notify subscribers
      setTimeout(() => notifySubscribers(newData), 0);
      
      return newData;
    });
  }, [notifySubscribers]);

  // Get current canvas state
  const getCanvasState = useCallback(() => ({
    nodes: canvasData.nodes,
    edges: canvasData.edges
  }), [canvasData]);

  // Update canvas data (partial updates allowed)
  const updateCanvasData = useCallback((data: Partial<CanvasData>) => {
    setCanvasData(prev => {
      const newData: CanvasData = { ...prev, ...data };
      setTimeout(() => notifySubscribers(newData), 0);
      return newData;
    });
  }, [notifySubscribers]);

  // Request current canvas data
  const requestCanvasData = useCallback(() => canvasData, [canvasData]);

  // Trigger SQL generation
  const triggerSQLGeneration = useCallback(() => {
    const event = new CustomEvent('generate-sql');
    window.dispatchEvent(event);
  }, []);

  // Get canvas summary
  const getCanvasSummary = useCallback((): CanvasSummary => {
    const validationStatus = canvasData.validationSummary?.isValid ? 'valid' : 'invalid';
    
    return {
      nodeCount: canvasData.nodes.length,
      edgeCount: canvasData.edges.length,
      connectionCount: canvasData.connections.length,
      validationStatus,
      validationErrors: canvasData.validationSummary?.counts?.errors || 0,
      validationWarnings: canvasData.validationSummary?.counts?.warnings || 0,
      sqlGeneratedNodes: Object.keys(canvasData.sqlGeneration).filter(
        key => canvasData.sqlGeneration[key]?.sql
      ).length,
      viewport: canvasData.viewport
    };
  }, [canvasData]);

  // Export complete canvas data
  const exportCanvasData = useCallback((): CanvasExportData => {
    return {
      nodes: canvasData.nodes,
      connections: canvasData.connections,
      edges: canvasData.edges,
      validationSummary: canvasData.validationSummary,
      sqlGeneration: canvasData.sqlGeneration,
      viewport: canvasData.viewport,
      nodeCount: canvasData.nodes.length,
      edgeCount: canvasData.edges.length,
      connectionCount: canvasData.connections.length,
      isValid: canvasData.validationSummary?.isValid || false,
      validationErrors: canvasData.validationSummary?.counts?.errors || 0,
      validationWarnings: canvasData.validationSummary?.counts?.warnings || 0
    };
  }, [canvasData]);

  return (
    <CanvasContext.Provider
      value={{
        canvasData,
        updateCanvasData,
        requestCanvasData,
        triggerSQLGeneration,
        getCanvasSummary,
        exportCanvasData,
        // NEW: Synchronization methods
        syncNodesAndEdges,
        subscribeToCanvasUpdates,
        getCanvasState
      }}
    >
      {children}
    </CanvasContext.Provider>
  );
};

// Custom hook to use the canvas context
export const useCanvas = (): CanvasContextType => {
  const context = useContext(CanvasContext);
  if (!context) {
    throw new Error('useCanvas must be used within CanvasProvider');
  }
  return context;
};