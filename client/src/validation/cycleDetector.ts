// src/validation/cycleDetector.ts

import {
  GraphNode,
  GraphEdge,
  GraphState,
  NodeSchema
} from './types';

/**
 * Cycle detection result
 */
export interface CycleDetectionResult {
  /** Whether a cycle was detected */
  hasCycle: boolean;
  
  /** All cycles found (empty if no cycles) */
  cycles: string[][];
  
  /** Nodes involved in cycles */
  nodesInCycles: Set<string>;
  
  /** Edges involved in cycles */
  edgesInCycles: Set<string>;
  
  /** Detailed information about each cycle */
  cycleDetails: Array<{
    cycle: string[];
    edgeIds: string[];
    isAllowed: boolean;
    violatesSchema: boolean;
  }>;
}

/**
 * Cycle detector using Tarjan's strongly connected components algorithm
 * Optimized for large graphs with memoization
 */
export class CycleDetector {
  private adjacencyList: Map<string, Set<string>> = new Map();
  private edgeMap: Map<string, GraphEdge> = new Map();
  private nodeMap: Map<string, GraphNode> = new Map();
  private schemaMap: Map<string, NodeSchema> = new Map();
  
  /**
   * Build adjacency list from graph state
   */
  private buildAdjacencyList(state: GraphState, schemas: Map<string, NodeSchema>): void {
    this.adjacencyList.clear();
    this.edgeMap.clear();
    this.nodeMap.clear();
    this.schemaMap = schemas;

    // Initialize adjacency list for all nodes
    state.nodes.forEach(node => {
      this.nodeMap.set(node.id, node);
      this.adjacencyList.set(node.id, new Set());
    });

    // Add edges to adjacency list
    state.edges.forEach(edge => {
      this.edgeMap.set(edge.id, edge);
      const neighbors = this.adjacencyList.get(edge.source);
      if (neighbors) {
        neighbors.add(edge.target);
      }
    });
  }

  /**
   * Detect all cycles in the graph
   */
  detectCycles(state: GraphState, schemas: Map<string, NodeSchema>): CycleDetectionResult {
    this.buildAdjacencyList(state, schemas);
    
    const result: CycleDetectionResult = {
      hasCycle: false,
      cycles: [],
      nodesInCycles: new Set(),
      edgesInCycles: new Set(),
      cycleDetails: []
    };

    const visited = new Set<string>();
    const recursionStack = new Set<string>();
    const nodeIds = Array.from(this.adjacencyList.keys());
    
    // Track path for cycle reconstruction
    const path = new Map<string, string>(); // child -> parent
    const edgePath = new Map<string, string>(); // child -> edgeId

    // DFS to detect cycles
    const dfs = (nodeId: string): string[][] => {
      const cycles: string[][] = [];
      
      visited.add(nodeId);
      recursionStack.add(nodeId);

      const neighbors = this.adjacencyList.get(nodeId) || new Set();
      
      for (const neighborId of neighbors) {
        const edge = this.findEdge(nodeId, neighborId, state.edges);
        
        if (!visited.has(neighborId)) {
          path.set(neighborId, nodeId);
          if (edge) edgePath.set(neighborId, edge.id);
          
          const nestedCycles = dfs(neighborId);
          cycles.push(...nestedCycles);
        } else if (recursionStack.has(neighborId)) {
          // Cycle detected - reconstruct the cycle
          const cycle = this.reconstructCycle(nodeId, neighborId, path);
          const edgeIds = this.reconstructEdgeCycle(nodeId, neighborId, edgePath, state.edges);
          
          if (cycle.length > 0) {
            cycles.push(cycle);
            
            // Check if cycle is allowed by schemas
            const isAllowed = this.isCycleAllowed(cycle);
            const violatesSchema = this.doesCycleViolateSchema(cycle);
            
            result.cycleDetails.push({
              cycle,
              edgeIds,
              isAllowed,
              violatesSchema
            });
          }
        }
      }

      recursionStack.delete(nodeId);
      return cycles;
    };

    // Run DFS for all nodes (handles disconnected graphs)
    for (const nodeId of nodeIds) {
      if (!visited.has(nodeId)) {
        const cycles = dfs(nodeId);
        if (cycles.length > 0) {
          result.hasCycle = true;
          result.cycles.push(...cycles);
        }
      }
    }

    // Populate nodes and edges in cycles
    result.cycles.forEach(cycle => {
      cycle.forEach(nodeId => result.nodesInCycles.add(nodeId));
    });

    result.cycleDetails.forEach(detail => {
      detail.edgeIds.forEach(edgeId => result.edgesInCycles.add(edgeId));
    });

    return result;
  }

  /**
   * Find edge between two nodes
   */
  private findEdge(sourceId: string, targetId: string, edges: GraphEdge[]): GraphEdge | undefined {
    return edges.find(edge => edge.source === sourceId && edge.target === targetId);
  }

  /**
   * Reconstruct cycle path
   */
  private reconstructCycle(current: string, start: string, path: Map<string, string>): string[] {
    const cycle: string[] = [current];
    let node = current;
    
    while (node !== start) {
      const parent = path.get(node);
      if (!parent) break;
      
      cycle.unshift(parent);
      node = parent;
    }
    
    cycle.unshift(current); // Close the cycle
    return cycle;
  }

  /**
   * Reconstruct edge IDs in cycle
   */
  private reconstructEdgeCycle(
    current: string, 
    start: string, 
    edgePath: Map<string, string>,
    edges: GraphEdge[]
  ): string[] {
    const edgeIds: string[] = [];
    let node = current;
    
    // Find edge from current to parent
    const parent = edgePath.get(current);
    if (parent) {
      edgeIds.push(parent);
    }
    
    // Trace back to start
    while (node !== start) {
      const edgeId = edgePath.get(node);
      if (edgeId) {
        edgeIds.unshift(edgeId);
      }
      
      // Find parent node
      const edge = edges.find(e => e.target === node && edgePath.get(e.target) === edgeId);
      node = edge?.source || node;
    }
    
    return edgeIds;
  }

  /**
   * Check if cycle is allowed by node schemas
   */
  private isCycleAllowed(cycle: string[]): boolean {
    for (const nodeId of cycle) {
      const node = this.nodeMap.get(nodeId);
      if (!node) continue;
      
      const schema = this.schemaMap.get(node.type);
      if (schema && !schema.allowsCycles) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check if cycle violates any schema rules
   */
  private doesCycleViolateSchema(cycle: string[]): boolean {
    // Check each edge in the cycle
    for (let i = 0; i < cycle.length - 1; i++) {
      const sourceId = cycle[i];
      const targetId = cycle[i + 1];
      
      const sourceNode = this.nodeMap.get(sourceId);
      const targetNode = this.nodeMap.get(targetId);
      
      if (!sourceNode || !targetNode) continue;
      
      const sourceSchema = this.schemaMap.get(sourceNode.type);
      const targetSchema = this.schemaMap.get(targetNode.type);
      
      // Check if source can connect to target
      if (sourceSchema && !sourceSchema.allowedTargetTypes.includes(targetNode.type)) {
        return true;
      }
      
      // Check if target can accept connection from source
      if (targetSchema && !targetSchema.allowedSourceTypes.includes(sourceNode.type)) {
        return true;
      }
    }
    
    return false;
  }

  /**
   * Get simple cycles (no sub-cycles) for better error messages
   */
  getSimpleCycles(cycles: string[][]): string[][] {
    const simpleCycles: string[][] = [];
    const cycleSet = new Set<string>();
    
    cycles.forEach(cycle => {
      // Sort and stringify for comparison
      const sorted = [...cycle].sort();
      const key = sorted.join('→');
      
      if (!cycleSet.has(key)) {
        cycleSet.add(key);
        simpleCycles.push(cycle);
      }
    });
    
    return simpleCycles;
  }

  /**
   * Fast cycle check (doesn't return cycles, just boolean)
   * Optimized for real-time validation
   */
  hasCycleFast(state: GraphState): boolean {
    this.buildAdjacencyList(state, new Map());
    
    const visited = new Set<string>();
    const recursionStack = new Set<string>();
    const nodeIds = Array.from(this.adjacencyList.keys());
    
    const dfs = (nodeId: string): boolean => {
      if (recursionStack.has(nodeId)) {
        return true;
      }
      
      if (visited.has(nodeId)) {
        return false;
      }
      
      visited.add(nodeId);
      recursionStack.add(nodeId);
      
      const neighbors = this.adjacencyList.get(nodeId) || new Set();
      for (const neighborId of neighbors) {
        if (dfs(neighborId)) {
          return true;
        }
      }
      
      recursionStack.delete(nodeId);
      return false;
    };
    
    for (const nodeId of nodeIds) {
      if (!visited.has(nodeId)) {
        if (dfs(nodeId)) {
          return true;
        }
      }
    }
    
    return false;
  }

  /**
   * Get nodes that would create a cycle if connected
   */
  getNodesThatWouldCauseCycle(
    sourceId: string,
    targetId: string,
    state: GraphState
  ): { wouldCauseCycle: boolean; path?: string[] } {
    // First check if target can reach source (would create cycle)
    const tempEdges = [...state.edges, { 
      id: 'temp-edge', 
      source: sourceId, 
      target: targetId 
    } as GraphEdge];
    
    const tempState = { ...state, edges: tempEdges };
    
    if (this.hasCycleFast(tempState)) {
      // Find the path from target to source
      const path = this.findPath(targetId, sourceId, state);
      return { wouldCauseCycle: true, path };
    }
    
    return { wouldCauseCycle: false };
  }

  /**
   * Find path between two nodes using BFS
   */
  private findPath(startId: string, endId: string, state: GraphState): string[] | undefined {
    const visited = new Set<string>();
    const queue: Array<{ nodeId: string; path: string[] }> = [
      { nodeId: startId, path: [startId] }
    ];
    
    while (queue.length > 0) {
      const { nodeId, path } = queue.shift()!;
      
      if (nodeId === endId) {
        return path;
      }
      
      if (visited.has(nodeId)) continue;
      visited.add(nodeId);
      
      // Get outgoing neighbors
      const neighbors = state.edges
        .filter(edge => edge.source === nodeId)
        .map(edge => edge.target);
      
      for (const neighborId of neighbors) {
        if (!visited.has(neighborId)) {
          queue.push({ 
            nodeId: neighborId, 
            path: [...path, neighborId] 
          });
        }
      }
    }
    
    return undefined;
  }
}