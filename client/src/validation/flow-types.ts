// FILE: src/flow/types.ts
/**
 * React Flow-compatible types that preserve original validation semantics
 */

import { Node, Edge, Connection } from 'reactflow';
import {
  ValidationErrorCode,
  ValidationLevel,
  GraphState as OriginalGraphState,
  GraphNode as OriginalGraphNode,
  GraphEdge as OriginalGraphEdge
} from '../validation/types';

// Validation Node Data type
export interface ValidationNodeData {
  name: string;
  label?: string;
  technology?: string;
  componentCategory?: string;
  schema?: {
    columns: Array<{
      name: string;
      type: string;
      nullable: boolean;
      constraints?: string[];
    }>;
    primaryKey?: string[];
    foreignKeys?: Array<{
      column: string;
      referenceTable: string;
      referenceColumn: string;
    }>;
  };
  validationStatus?: 'valid' | 'warning' | 'error';
  metadata?: {
    validationStatus?: 'valid' | 'warning' | 'error';
    lastValidated?: string;
    [key: string]: any;
  };
  [key: string]: any;
}

// Extended React Flow Node with guaranteed type
export type ValidationNode = Node<ValidationNodeData> & {
  type: string; // Ensure type is always a string, not undefined
};

// Validation Edge Data type
export interface ValidationEdgeData {
  label?: string;
  dataType?: string;
  mapping?: Record<string, string>;
  validationStatus?: 'valid' | 'warning' | 'error';
  metadata?: {
    validationStatus?: 'valid' | 'warning' | 'error';
    validationErrors?: string[];
    [key: string]: any;
  };
  [key: string]: any;
}

// Extended React Flow Edge
export type ValidationEdge = Edge<ValidationEdgeData> & {
  sourceHandle?: string;
  targetHandle?: string;
};

// Port definition for React Flow handles
export interface PortDefinition {
  id: string;
  type: 'source' | 'target';
  dataType?: string;
  label?: string;
  position: 'top' | 'bottom' | 'left' | 'right';
  maxConnections?: number | null;
}

// Validation result for React Flow integration
export interface ReactFlowValidationResult {
  id: string;
  code: ValidationErrorCode;
  level: ValidationLevel | 'error' | 'warning' | 'info';
  message: string;
  details?: string;
  nodeIds: string[];
  edgeIds: string[];
  timestamp: string;
  fixSuggestion?: string;
  context?: Record<string, any>;
}

// Connection validation request
export interface ConnectionValidationRequest {
  source: string;
  target: string;
  sourceHandle?: string | null;
  targetHandle?: string | null;
}

// Async validation function signature
export type AsyncValidator = (
  source: ValidationNode,
  target: ValidationNode,
  connection: Connection
) => Promise<ReactFlowValidationResult[]>;

// React Flow graph state
export interface ReactFlowGraphState {
  nodes: ValidationNode[];
  edges: ValidationEdge[];
  metadata?: {
    name: string;
    description?: string;
    validationMode?: 'strict' | 'lenient' | 'warn-only';
    [key: string]: any;
  };
}

// Helper to normalize handle values
export const normalizeHandle = (handle: string | null | undefined): string | undefined => {
  return handle === null ? undefined : handle;
};

// Convert ReactFlowGraphState to original GraphState
export function convertToOriginalState(state: ReactFlowGraphState): OriginalGraphState {
  return {
    nodes: state.nodes.map(node => ({
      id: node.id,
      type: node.type,
      position: node.position,
      data: {
        ...node.data,
        name: node.data.name || node.id,
      },
      metadata: node.data.metadata
    }) as OriginalGraphNode),
    edges: state.edges.map(edge => ({
      id: edge.id,
      source: edge.source,
      target: edge.target,
      sourceHandle: normalizeHandle(edge.sourceHandle), // Convert null to undefined
      targetHandle: normalizeHandle(edge.targetHandle), // Convert null to undefined
      data: edge.data,
      metadata: edge.data?.metadata
    }) as OriginalGraphEdge),
    metadata: state.metadata
  };
}