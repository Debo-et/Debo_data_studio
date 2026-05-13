// src/validation/baseValidationRule.ts

import {
  ValidationRule,
  GraphNode,
  GraphEdge,
  GraphState,
  NodeSchema,
  ValidationResult
} from './types';
import { SchemaRegistry } from './schemaRegistry';

export abstract class BaseValidationRule implements ValidationRule {
  abstract id: string;
  abstract name: string;
  abstract description: string;
  abstract severity: 'error' | 'warning';
  
  appliesTo(_node: GraphNode, _edge: GraphEdge, _graph: GraphState): boolean {
    return true; // Override in subclasses
  }
  
  abstract validate(node: GraphNode, edge: GraphEdge, graph: GraphState): ValidationResult[];
  
  getFixSuggestion(_node: GraphNode, _edge: GraphEdge, _graph: GraphState): string | null {
    return null;
  }
  
  protected getNodeById(nodeId: string, graph: GraphState): GraphNode | undefined {
    return graph.nodes.find(n => n.id === nodeId);
  }
  
  protected getNodeSchema(node: GraphNode, schemaRegistry: SchemaRegistry): NodeSchema | undefined {
    return schemaRegistry.getSchema(node.type);
  }
  
  protected countIncomingConnections(nodeId: string, graph: GraphState): number {
    return graph.edges.filter(edge => edge.target === nodeId).length;
  }
  
  protected countOutgoingConnections(nodeId: string, graph: GraphState): number {
    return graph.edges.filter(edge => edge.source === nodeId).length;
  }
  
  protected countPortConnections(portId: string, graph: GraphState): number {
    return graph.edges.filter(edge => 
      edge.sourceHandle === portId || edge.targetHandle === portId
    ).length;
  }
}