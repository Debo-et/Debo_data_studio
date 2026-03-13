// Update nodesSlice.ts - Add React Flow compatibility
import { createSlice, PayloadAction } from '@reduxjs/toolkit';
import { Node, Edge } from '@xyflow/react';

export interface NodeData {
  label: string;
  nodeType: string;
  description?: string;
  enabled?: boolean;
  executionOrder?: number;
  timeout?: number;
  parameters?: {
    [key: string]: any;
  };
  [key: string]: any;
}

export interface CustomNode extends Node<NodeData> {
  // Extended with your custom properties
}

interface NodesState {
  nodes: CustomNode[];
  edges: Edge[]; // NEW: Add edges for React Flow
  selectedNodeId: string | null;
  selectedEdgeId: string | null; // NEW: For edge selection
  lastUpdated: number;
  // React Flow specific state
  viewport: { x: number; y: number; zoom: number };
}

const initialState: NodesState = {
  nodes: [
    {
      id: '1',
      type: 'customNode',
      position: { x: 100, y: 100 },
      data: { 
        label: 'Start Node',
        nodeType: 'input',
        description: 'Initial data source',
        enabled: true,
        executionOrder: 0,
        timeout: 30,
        parameters: {
          filePath: '/data/input.csv',
          format: 'csv',
          encoding: 'utf-8'
        }
      },
    },
  ],
  edges: [], // NEW: Initialize empty edges array
  selectedNodeId: null,
  selectedEdgeId: null,
  lastUpdated: Date.now(),
  viewport: { x: 0, y: 0, zoom: 1 }, // NEW: Viewport state
};

const nodesSlice = createSlice({
  name: 'nodes',
  initialState,
  reducers: {
    // Existing node reducers
    addNode: (state, action: PayloadAction<CustomNode>) => {
      state.nodes.push(action.payload);
      state.lastUpdated = Date.now();
    },
    
    updateNode: (state, action: PayloadAction<{ id: string; data: Partial<NodeData> }>) => {
      const node = state.nodes.find(n => n.id === action.payload.id);
      if (node) {
        node.data = { ...node.data, ...action.payload.data };
        state.lastUpdated = Date.now();
      }
    },
    
    deleteNode: (state, action: PayloadAction<string>) => {
      state.nodes = state.nodes.filter(node => node.id !== action.payload);
      // Also remove connected edges
      state.edges = state.edges.filter(
        edge => edge.source !== action.payload && edge.target !== action.payload
      );
      if (state.selectedNodeId === action.payload) {
        state.selectedNodeId = null;
      }
      state.lastUpdated = Date.now();
    },
    
    updateNodePosition: (state, action: PayloadAction<{ id: string; position: { x: number; y: number } }>) => {
      const node = state.nodes.find(n => n.id === action.payload.id);
      if (node) {
        node.position = action.payload.position;
        state.lastUpdated = Date.now();
      }
    },
    
    // NEW: React Flow edge reducers
    addEdge: (state, action: PayloadAction<Edge>) => {
      state.edges.push(action.payload);
      state.lastUpdated = Date.now();
    },
    
    updateEdge: (state, action: PayloadAction<{ id: string; updates: Partial<Edge> }>) => {
      const edge = state.edges.find(e => e.id === action.payload.id);
      if (edge) {
        Object.assign(edge, action.payload.updates);
        state.lastUpdated = Date.now();
      }
    },
    
    deleteEdge: (state, action: PayloadAction<string>) => {
      state.edges = state.edges.filter(edge => edge.id !== action.payload);
      if (state.selectedEdgeId === action.payload) {
        state.selectedEdgeId = null;
      }
      state.lastUpdated = Date.now();
    },
    
    setEdges: (state, action: PayloadAction<Edge[]>) => {
      state.edges = action.payload;
      state.lastUpdated = Date.now();
    },
    
    // Selection management
    setSelectedNode: (state, action: PayloadAction<string | null>) => {
      state.selectedNodeId = action.payload;
      if (action.payload) {
        state.selectedEdgeId = null; // Deselect edge when selecting node
      }
    },
    
    setSelectedEdge: (state, action: PayloadAction<string | null>) => {
      state.selectedEdgeId = action.payload;
      if (action.payload) {
        state.selectedNodeId = null; // Deselect node when selecting edge
      }
    },
    
    clearSelection: (state) => {
      state.selectedNodeId = null;
      state.selectedEdgeId = null;
    },
    
    // React Flow viewport
    setViewport: (state, action: PayloadAction<{ x: number; y: number; zoom: number }>) => {
      state.viewport = action.payload;
    },
    
    // Bulk operations
    setNodes: (state, action: PayloadAction<CustomNode[]>) => {
      state.nodes = action.payload;
      state.lastUpdated = Date.now();
    },
    
    moveNodes: (state, action: PayloadAction<{ nodeIds: string[]; delta: { x: number; y: number } }>) => {
      action.payload.nodeIds.forEach(nodeId => {
        const node = state.nodes.find(n => n.id === nodeId);
        if (node) {
          node.position.x += action.payload.delta.x;
          node.position.y += action.payload.delta.y;
        }
      });
      state.lastUpdated = Date.now();
    },
    
    // Sync with React Flow state
    syncWithReactFlow: (state, action: PayloadAction<{ nodes: CustomNode[]; edges: Edge[] }>) => {
      state.nodes = action.payload.nodes;
      state.edges = action.payload.edges;
      state.lastUpdated = Date.now();
    },
  },
});

export const {
  addNode,
  updateNode,
  deleteNode,
  updateNodePosition,
  addEdge,
  updateEdge,
  deleteEdge,
  setEdges,
  setSelectedNode,
  setSelectedEdge,
  clearSelection,
  setViewport,
  setNodes,
  moveNodes,
  syncWithReactFlow,
} = nodesSlice.actions;

// Selectors for React Flow integration
export const selectNodesForReactFlow = (state: { nodes: NodesState }) => state.nodes.nodes;
export const selectEdgesForReactFlow = (state: { nodes: NodesState }) => state.nodes.edges;
export const selectSelectedNodeId = (state: { nodes: NodesState }) => state.nodes.selectedNodeId;
export const selectSelectedEdgeId = (state: { nodes: NodesState }) => state.nodes.selectedEdgeId;

export default nodesSlice.reducer;