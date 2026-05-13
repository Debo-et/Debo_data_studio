// FILE: src/flow/ValidationProvider.tsx
/**
 * Provides validation context and hooks for React Flow
 * OMITTED:
 * - Original caching mechanism (React Flow state is source of truth)
 * - Batch validation modes (simplified to synchronous validation)
 */

import React, { createContext, useContext, useCallback, useMemo } from 'react';
import { Connection } from 'reactflow';
import { SchemaRegistry, DefaultSchemas, DefaultConnectionRules } from '../validation/schemaRegistry';
import { ValidationEngine, ValidationEngineConfig } from '../validation/validationEngine';
import {
  ValidationNode,
  ValidationEdge,
  ReactFlowValidationResult,
  AsyncValidator} from './flow-types';
import { GraphState, GraphNode, GraphEdge, ValidationErrorCode } from '../validation/types';

interface ValidationContextType {
  schemaRegistry: SchemaRegistry;
  validationEngine: ValidationEngine;
  
  // Core validation functions
  validateConnection: (
    connection: Connection,
    nodes: ValidationNode[],
    edges: ValidationEdge[]
  ) => {
    isValid: boolean;
    errors: ReactFlowValidationResult[];
    warnings: ReactFlowValidationResult[];
  };
  
  validateNode: (
    nodeId: string,
    nodes: ValidationNode[],
    edges: ValidationEdge[]
  ) => {
    isValid: boolean;
    errors: ReactFlowValidationResult[];
    warnings: ReactFlowValidationResult[];
  };
  
  validateGraph: (
    nodes: ValidationNode[],
    edges: ValidationEdge[]
  ) => {
    isValid: boolean;
    errors: ReactFlowValidationResult[];
    warnings: ReactFlowValidationResult[];
    summary: any;
  };
  
  // Async validation support
  validateAsync?: AsyncValidator;
  
  // ETL-specific validation
  validateETLConnection: (
    sourceNode: ValidationNode,
    targetNode: ValidationNode,
    connection: Connection
  ) => ReactFlowValidationResult[];
  
  // Cycle detection
  wouldCauseCycle: (
    sourceId: string,
    targetId: string,
    edges: ValidationEdge[]
  ) => boolean;
  
  // Port validation
  validatePort: (
    portId: string,
    portType: 'source' | 'target',
    node: ValidationNode,
    edges: ValidationEdge[]
  ) => boolean;
  
  // Error display utilities
  getErrorMessages: (results: ReactFlowValidationResult[]) => string[];
  getFixSuggestions: (results: ReactFlowValidationResult[]) => string[];
}

const ValidationContext = createContext<ValidationContextType | undefined>(undefined);

interface ValidationProviderProps {
  children: React.ReactNode;
  config?: Partial<ValidationEngineConfig>;
  asyncValidator?: AsyncValidator;
}

export const ValidationProvider: React.FC<ValidationProviderProps> = ({
  children,
  config,
  asyncValidator
}) => {
  // Initialize schema registry with defaults
  const schemaRegistry = useMemo(() => {
    const registry = new SchemaRegistry();
    registry.registerSchemas(DefaultSchemas);
    DefaultConnectionRules.forEach(rule => registry.registerConnectionRule(rule));
    return registry;
  }, []);

  // Initialize validation engine
  const validationEngine = useMemo(() => {
    return new ValidationEngine({
      schemaRegistry,
      mode: 'strict',
      enableCaching: false, // Disable caching for React Flow
      enableETLValidation: true,
      ...config
    });
  }, [schemaRegistry, config]);

  // Helper function to convert handle value (string | null | undefined) to (string | undefined)
  const normalizeHandle = (handle: string | null | undefined): string | undefined => {
    return handle === null ? undefined : handle;
  };

  // Convert React Flow state to original GraphState for validation
  const convertToGraphState = useCallback((
    nodes: ValidationNode[],
    edges: ValidationEdge[]
  ): GraphState => {
    return {
      nodes: nodes.map(node => ({
        id: node.id,
        type: node.type || 'default',
        position: node.position,
        data: {
          ...node.data,
          name: node.data.name || node.id,
        },
        metadata: node.data.metadata
      } as GraphNode)),
      edges: edges.map(edge => ({
        id: edge.id,
        source: edge.source,
        target: edge.target,
        sourceHandle: normalizeHandle(edge.sourceHandle), // Convert null to undefined
        targetHandle: normalizeHandle(edge.targetHandle), // Convert null to undefined
        data: edge.data || {},
        metadata: edge.data?.metadata
      } as GraphEdge))
    };
  }, []);

  // Main connection validation
  const validateConnection = useCallback((
    connection: Connection,
    nodes: ValidationNode[],
    edges: ValidationEdge[]
  ) => {
    const { source, target } = connection;
    
    if (!source || !target) {
      return {
        isValid: false,
        errors: [{
          id: 'invalid-connection',
          code: ValidationErrorCode.INVALID_CONNECTION_TYPE,
          level: 'error',
          message: 'Source and target nodes are required',
          nodeIds: [],
          edgeIds: [],
          timestamp: new Date().toISOString()
        } as ReactFlowValidationResult],
        warnings: []
      };
    }

    const sourceNode = nodes.find(n => n.id === source);
    const targetNode = nodes.find(n => n.id === target);

    if (!sourceNode || !targetNode) {
      return {
        isValid: false,
        errors: [{
          id: 'node-not-found',
          code: ValidationErrorCode.INVALID_CONNECTION_TYPE,
          level: 'error',
          message: 'Source or target node not found',
          nodeIds: [],
          edgeIds: [],
          timestamp: new Date().toISOString()
        } as ReactFlowValidationResult],
        warnings: []
      };
    }

    // Create temporary edge for validation

    const state = convertToGraphState(nodes, edges);
    const summary = validationEngine.validateConnection(source, target, state);
    
    // Convert original validation results to React Flow format
    const errors = summary.results
      .filter(r => r.level === 'error')
      .map(r => ({
        ...r,
        nodeIds: r.nodeIds || [],
        edgeIds: r.edgeIds || []
      } as ReactFlowValidationResult));

    const warnings = summary.results
      .filter(r => r.level === 'warning')
      .map(r => ({
        ...r,
        nodeIds: r.nodeIds || [],
        edgeIds: r.edgeIds || []
      } as ReactFlowValidationResult));

    return {
      isValid: summary.isValid,
      errors,
      warnings
    };
  }, [validationEngine, convertToGraphState]);

  // Node validation
  const validateNode = useCallback((
    nodeId: string,
    nodes: ValidationNode[],
    edges: ValidationEdge[]
  ) => {
    const state = convertToGraphState(nodes, edges);
    const node = nodes.find(n => n.id === nodeId);
    
    if (!node) {
      return {
        isValid: false,
        errors: [],
        warnings: []
      };
    }

    const summary = validationEngine.getNodeValidationStatus(nodeId, state);
    
    return {
      isValid: summary.isValid,
      errors: summary.results
        .filter(r => r.level === 'error')
        .map(r => ({ ...r } as ReactFlowValidationResult)),
      warnings: summary.results
        .filter(r => r.level === 'warning')
        .map(r => ({ ...r } as ReactFlowValidationResult))
    };
  }, [validationEngine, convertToGraphState]);

  // Graph validation
  const validateGraph = useCallback((
    nodes: ValidationNode[],
    edges: ValidationEdge[]
  ) => {
    const state = convertToGraphState(nodes, edges);
    const summary = validationEngine.validateGraph(state);
    
    return {
      isValid: summary.isValid,
      errors: summary.results
        .filter(r => r.level === 'error')
        .map(r => ({ ...r } as ReactFlowValidationResult)),
      warnings: summary.results
        .filter(r => r.level === 'warning')
        .map(r => ({ ...r } as ReactFlowValidationResult)),
      summary
    };
  }, [validationEngine, convertToGraphState]);

  // ETL-specific connection validation
  const validateETLConnection = useCallback((
    sourceNode: ValidationNode,
    targetNode: ValidationNode  ) => {
    const state = convertToGraphState([sourceNode, targetNode], []);
    const summary = validationEngine.validateConnection(
      sourceNode.id,
      targetNode.id,
      state
    );
    
    return summary.results.map(r => ({ ...r } as ReactFlowValidationResult));
  }, [validationEngine, convertToGraphState]);

  // Cycle detection
  const wouldCauseCycle = useCallback((
    sourceId: string,
    targetId: string,
    edges: ValidationEdge[]
  ) => {
    // Simplified cycle detection for React Flow
    // Check if target can reach source (would create cycle)
    const visited = new Set<string>();
    const stack = [targetId];
    
    while (stack.length > 0) {
      const current = stack.pop()!;
      
      if (current === sourceId) {
        return true;
      }
      
      if (visited.has(current)) continue;
      visited.add(current);
      
      // Find all nodes that current connects to
      const outgoingEdges = edges.filter(e => e.source === current);
      outgoingEdges.forEach(edge => {
        if (!visited.has(edge.target)) {
          stack.push(edge.target);
        }
      });
    }
    
    return false;
  }, []);

  // Port validation
  const validatePort = useCallback((
    portId: string,
    portType: 'source' | 'target',
    _node: ValidationNode,
    edges: ValidationEdge[]
  ) => {
    const portConnections = edges.filter(edge => 
      (portType === 'source' && edge.sourceHandle === portId) ||
      (portType === 'target' && edge.targetHandle === portId)
    ).length;
    
    // Default: allow one connection per port
    return portConnections === 0;
  }, []);

  // Utility functions
  const getErrorMessages = useCallback((results: ReactFlowValidationResult[]) => {
    return results.filter(r => r.level === 'error').map(r => r.message);
  }, []);

  const getFixSuggestions = useCallback((results: ReactFlowValidationResult[]) => {
    return results
      .filter(r => r.level === 'error')
      .map(r => r.fixSuggestion || 'No fix suggestion available')
      .filter(Boolean);
  }, []);

  const contextValue: ValidationContextType = {
    schemaRegistry,
    validationEngine,
    validateConnection,
    validateNode,
    validateGraph,
    validateAsync: asyncValidator,
    validateETLConnection,
    wouldCauseCycle,
    validatePort,
    getErrorMessages,
    getFixSuggestions
  };

  return (
    <ValidationContext.Provider value={contextValue}>
      {children}
    </ValidationContext.Provider>
  );
};

export const useValidation = () => {
  const context = useContext(ValidationContext);
  if (!context) {
    throw new Error('useValidation must be used within ValidationProvider');
  }
  return context;
};