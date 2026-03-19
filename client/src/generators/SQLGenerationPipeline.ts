// src/generators/SQLGenerationPipeline.ts

import {
  CanvasNode,
  CanvasConnection,
  NodeType,
  ConnectionStatus,
  PostgreSQLDataType} from '../types/pipeline-types';
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';
import { SQLGeneratorFactory } from './SQLGeneratorFactory';   // FIXED: import from separate file

// ==================== TYPES & INTERFACES ====================

export interface PipelineGenerationOptions {
  postgresVersion: string;
  includeComments: boolean;
  formatSQL: boolean;
  optimize: boolean;
  parallelExecution: boolean;
  maxParallelDepth: number;
  useCTEs: boolean;
  materializeIntermediate: boolean;
  wrapInTransaction: boolean;
  validateSyntax: boolean;
  generateExplainPlan: boolean;
  logLevel: 'none' | 'error' | 'warn' | 'info' | 'debug';
  progressCallback?: (progress: PipelineProgress) => void;
}

export interface PipelineProgress {
  stage: 'initializing' | 'graph_construction' | 'topological_sort' | 'planning' | 'generation' | 'optimization' | 'validation' | 'formatting';
  progress: number; // 0-100
  message: string;
  currentNodes?: string[];
  stats?: {
    nodesProcessed: number;
    totalNodes: number;
    fragmentsGenerated: number;
    errors: number;
    warnings: number;
  };
}

export interface ExecutionPlan {
  topologicalOrder: string[];
  parallelGroups: string[][];
  criticalPath: string[];
  estimatedCost: number;
  memoryFootprint: {
    estimated: number; // bytes
    peak: number;
  };
  timingEstimates: {
    sequential: number; // milliseconds
    parallel: number;
    recommendedParallelism: number;
  };
  optimizationOpportunities: Array<{
    type: string;
    description: string;
    potentialImprovement: number; // percentage
  }>;
  dependencies: Record<string, string[]>;
}

export interface PipelineGenerationResult {
  pipelineId: string;
  generatedAt: string;
  sql: string;
  fragments: Map<string, GeneratedSQLFragment>;
  executionPlan: ExecutionPlan;
  optimizationSummary: OptimizationSummary;
  warnings: PipelineWarning[];
  errors: PipelineError[];
  validationResult: PipelineValidationResult;
  metadata: {
    totalNodes: number;
    totalConnections: number;
    generationTimeMs: number;
    memoryUsageMb: number;
    formatted: boolean;
  };
}

export interface OptimizationSummary {
  cteFlattened: number;
  subqueriesConverted: number;
  predicatePushdowns: number;
  materializedCTEs: number;
  warningsSuppressed: number;
  estimatedPerformanceGain: number; // percentage
}

export interface PipelineWarning {
  code: string;
  message: string;
  nodeId?: string;
  connectionId?: string;
  severity: 'LOW' | 'MEDIUM' | 'HIGH';
  suggestion?: string;
}

export interface PipelineError {
  code: string;
  message: string;
  nodeId?: string;
  connectionId?: string;
  recoverable: boolean;
  recoverySuggestion?: string;
}

export interface PipelineValidationResult {
  isValid: boolean;
  schemaCompatible: boolean;
  syntaxValid: boolean;
  performanceAcceptable: boolean;
  issues: Array<{
    type: 'schema' | 'syntax' | 'performance' | 'dependency';
    message: string;
    severity: 'ERROR' | 'WARNING' | 'INFO';
    location?: string;
  }>;
}

export interface DependencyGraph {
  nodes: Map<string, CanvasNode>;
  adjacencyList: Map<string, Set<string>>; // nodeId -> outgoing edges
  reverseAdjacencyList: Map<string, Set<string>>; // nodeId -> incoming edges
  indegree: Map<string, number>;
  outdegree: Map<string, number>;
  nodeLevels: Map<string, number>; // Topological level
  connectionMap: Map<string, CanvasConnection>; // connectionId -> connection
  nodeConnections: Map<string, string[]>; // nodeId -> connectionIds
}

export interface CTEChainEntry {
  nodeId: string;
  cteName: string;
  sql: string;
  materialized: boolean;
  dependencies: string[];
  columns: Array<{ name: string; dataType: PostgreSQLDataType }>;
  estimatedRows: number;
  shouldMaterialize: boolean;
}

export interface PipelineContext {
  options: PipelineGenerationOptions;
  nodeGenerators: Map<string, BaseSQLGenerator>;
  schemaCache: Map<string, Array<{ name: string; dataType: PostgreSQLDataType }>>;
  fragmentCache: Map<string, GeneratedSQLFragment>;
  cteChain: Map<string, CTEChainEntry>;
  warnings: PipelineWarning[];
  errors: PipelineError[];
  logger: PipelineLogger;
  stats: {
    startTime: number;
    fragmentsGenerated: number;
    cacheHits: number;
    cacheMisses: number;
    optimizationApplied: number;
  };
}

// ==================== LOGGER ====================

class PipelineLogger {
  private logLevel: string;
  private logs: Array<{ timestamp: Date; level: string; message: string; data?: any }> = [];

  constructor(level: string) {
    this.logLevel = level;
  }

  error(message: string, data?: any) {
    this.log('error', message, data);
  }

  warn(message: string, data?: any) {
    if (['warn', 'info', 'debug'].includes(this.logLevel)) {
      this.log('warn', message, data);
    }
  }

  info(message: string, data?: any) {
    if (['info', 'debug'].includes(this.logLevel)) {
      this.log('info', message, data);
    }
  }

  debug(message: string, data?: any) {
    if (this.logLevel === 'debug') {
      this.log('debug', message, data);
    }
  }

  private log(level: string, message: string, data?: any) {
    const entry = { timestamp: new Date(), level, message, data };
    this.logs.push(entry);
    
    // In production, this would use a proper logging framework
    if (level === 'error') {
      console.error(`[Pipeline] ${message}`, data);
    } else if (level === 'warn') {
      console.warn(`[Pipeline] ${message}`, data);
    } else if (level === 'info') {
      console.info(`[Pipeline] ${message}`, data);
    } else {
      console.debug(`[Pipeline] ${message}`, data);
    }
  }

  getLogs(): Array<{ timestamp: Date; level: string; message: string; data?: any }> {
    return this.logs;
  }

  clear() {
    this.logs = [];
  }
}

// ==================== MAIN PIPELINE CLASS ====================

/**
 * SQL Generation Pipeline orchestrator
 * Implements DAG-based execution planning with PostgreSQL optimizations
 */
export class SQLGenerationPipeline {
  private nodes: CanvasNode[];
  private connections: CanvasConnection[];
  private options: PipelineGenerationOptions;
  private logger: PipelineLogger;
  private memoizationCache = new Map<string, any>();
  private generationStartTime: number = 0;

  constructor(
    nodes: CanvasNode[],
    connections: CanvasConnection[],
    options: Partial<PipelineGenerationOptions> = {}
  ) {
    this.nodes = nodes;
    this.connections = connections;
    this.options = {
      postgresVersion: '14.0',
      includeComments: true,
      formatSQL: true,
      optimize: true,
      parallelExecution: true,
      maxParallelDepth: 4,
      useCTEs: true,
      materializeIntermediate: false,
      wrapInTransaction: false,
      validateSyntax: true,
      generateExplainPlan: true,
      logLevel: 'info',
      ...options
    };
    this.logger = new PipelineLogger(this.options.logLevel);
  }

  // ==================== PUBLIC API ====================

  /**
   * Generate complete pipeline SQL with DAG-based execution planning
   */
  public async generate(): Promise<PipelineGenerationResult> {
    this.generationStartTime = Date.now();
    this.logger.info('Starting pipeline generation');
    
    const result: PipelineGenerationResult = {
      pipelineId: `pipeline-${Date.now()}`,
      generatedAt: new Date().toISOString(),
      sql: '',
      fragments: new Map(),
      executionPlan: {} as ExecutionPlan,
      optimizationSummary: {
        cteFlattened: 0,
        subqueriesConverted: 0,
        predicatePushdowns: 0,
        materializedCTEs: 0,
        warningsSuppressed: 0,
        estimatedPerformanceGain: 0
      },
      warnings: [],
      errors: [],
      validationResult: {
        isValid: false,
        schemaCompatible: false,
        syntaxValid: false,
        performanceAcceptable: false,
        issues: []
      },
      metadata: {
        totalNodes: this.nodes.length,
        totalConnections: this.connections.length,
        generationTimeMs: 0,
        memoryUsageMb: 0,
        formatted: this.options.formatSQL
      }
    };

    try {
      this.updateProgress('initializing', 0, 'Initializing pipeline generation');

      // 1. Build dependency graph
      this.updateProgress('graph_construction', 10, 'Building dependency graph');
      const dependencyGraph = this.buildDependencyGraph();
      
      // 2. Validate DAG (no cycles)
      this.updateProgress('graph_construction', 20, 'Validating DAG structure');
      const cycles = this.detectCycles(dependencyGraph);
      if (cycles.length > 0) {
        throw new Error(`Pipeline contains circular dependencies: ${JSON.stringify(cycles)}`);
      }

      // 3. Perform topological sort
      this.updateProgress('topological_sort', 30, 'Performing topological sort');
      const topologicalOrder = this.topologicalSort(dependencyGraph);
      
      // 4. Detect parallel execution groups
      this.updateProgress('planning', 40, 'Detecting parallel execution groups');
      const parallelGroups = this.detectParallelGroups(dependencyGraph, topologicalOrder);
      
      // 5. Create execution plan
      this.updateProgress('planning', 50, 'Creating execution plan');
      const executionPlan = this.createExecutionPlan(
        dependencyGraph,
        topologicalOrder,
        parallelGroups
      );
      result.executionPlan = executionPlan;

      // 6. Create pipeline context
      const context: PipelineContext = {
        options: this.options,
        nodeGenerators: new Map(),
        schemaCache: new Map(),
        fragmentCache: new Map(),
        cteChain: new Map(),
        warnings: [],
        errors: [],
        logger: this.logger,
        stats: {
          startTime: Date.now(),
          fragmentsGenerated: 0,
          cacheHits: 0,
          cacheMisses: 0,
          optimizationApplied: 0
        }
      };

      // 7. Generate SQL fragments for each node
      this.updateProgress('generation', 60, 'Generating SQL fragments');
      const fragments = await this.generatePipelineFragments(
        dependencyGraph,
        topologicalOrder,
        context
      );
      result.fragments = fragments;

      // 8. Apply optimizations if enabled
      if (this.options.optimize) {
        this.updateProgress('optimization', 70, 'Applying optimizations');
        const optimizationResult = await this.applyOptimizations(fragments, dependencyGraph, context);
        result.optimizationSummary = optimizationResult.summary;
        result.warnings.push(...optimizationResult.warnings);
      }

      // 9. Build final SQL
      this.updateProgress('generation', 80, 'Building final SQL');
      result.sql = await this.buildFinalSQL(
        fragments,
        dependencyGraph,
        topologicalOrder,
        context
      );

      // 10. Validate pipeline
      this.updateProgress('validation', 90, 'Validating pipeline');
      result.validationResult = await this.validatePipeline(result.sql, fragments, dependencyGraph);
      
      // 11. Format SQL if requested
      if (this.options.formatSQL) {
        this.updateProgress('formatting', 95, 'Formatting SQL');
        result.sql = this.formatSQL(result.sql);
      }

      // 12. Collect warnings and errors
      result.warnings.push(...context.warnings);
      result.errors.push(...context.errors);

      // 13. Update metadata
      result.metadata.generationTimeMs = Date.now() - this.generationStartTime;
      result.metadata.memoryUsageMb = this.getMemoryUsage();

      this.updateProgress('formatting', 100, 'Pipeline generation complete');
      this.logger.info('Pipeline generation completed successfully');

      return result;

    } catch (error) {
      this.logger.error('Pipeline generation failed', error);
      
      result.errors.push({
        code: 'PIPELINE_GENERATION_FAILED',
        message: error instanceof Error ? error.message : 'Unknown error',
        recoverable: false
      });

      result.validationResult.isValid = false;
      result.metadata.generationTimeMs = Date.now() - this.generationStartTime;
      
      return result;
    }
  }

  /**
   * Generate pipeline SQL with memoization (cached generation)
   */
  public async generateWithCache(cacheKey: string): Promise<PipelineGenerationResult> {
    const cacheHit = this.memoizationCache.get(cacheKey);
    if (cacheHit && this.isCacheValid(cacheHit)) {
      this.logger.info(`Cache hit for key: ${cacheKey}`);
      return cacheHit;
    }

    this.logger.info(`Cache miss for key: ${cacheKey}, generating fresh`);
    const result = await this.generate();
    
    this.memoizationCache.set(cacheKey, result);
    this.cleanupMemoizationCache(); // Keep cache size manageable
    
    return result;
  }

  /**
   * Get pipeline statistics
   */
  public getStats(): {
    cacheSize: number;
    generationTime: number;
    nodesProcessed: number;
  } {
    return {
      cacheSize: this.memoizationCache.size,
      generationTime: this.generationStartTime ? Date.now() - this.generationStartTime : 0,
      nodesProcessed: this.nodes.length
    };
  }

  /**
   * Clear memoization cache
   */
  public clearCache(): void {
    this.memoizationCache.clear();
    this.logger.info('Memoization cache cleared');
  }

  // ==================== HELPER METHODS FOR TYPE CHECKS ====================

  /**
   * Check if a node is an output node
   */
  private isOutputNode(node: CanvasNode): boolean {
    // Handle string literal since NodeType.OUTPUT might not be in the union
    return node.type === 'output';
  }

  /**
   * Check if a node is an input node
   */
  private isInputNode(node: CanvasNode): boolean {
    // Handle string literal since NodeType.INPUT might not be in the union
    return node.type === 'input';
  }

  // ==================== DAG CONSTRUCTION & ANALYSIS ====================

  /**
   * Build dependency graph from nodes and connections
   */
  private buildDependencyGraph(): DependencyGraph {
    const startTime = Date.now();
    this.logger.debug('Building dependency graph');

    const graph: DependencyGraph = {
      nodes: new Map(),
      adjacencyList: new Map(),
      reverseAdjacencyList: new Map(),
      indegree: new Map(),
      outdegree: new Map(),
      nodeLevels: new Map(),
      connectionMap: new Map(),
      nodeConnections: new Map()
    };

    // Initialize maps for all nodes
    this.nodes.forEach(node => {
      graph.nodes.set(node.id, node);
      graph.adjacencyList.set(node.id, new Set());
      graph.reverseAdjacencyList.set(node.id, new Set());
      graph.indegree.set(node.id, 0);
      graph.outdegree.set(node.id, 0);
      graph.nodeConnections.set(node.id, []);
    });

    // Add edges based on connections
    this.connections.forEach(connection => {
      if (connection.status === ConnectionStatus.INVALID) {
        this.logger.warn(`Skipping invalid connection: ${connection.id}`);
        return;
      }

      const sourceNode = graph.nodes.get(connection.sourceNodeId);
      const targetNode = graph.nodes.get(connection.targetNodeId);

      if (!sourceNode || !targetNode) {
        this.logger.error(`Connection ${connection.id} references non-existent node`);
        return;
      }

      // Add edge: source -> target
      graph.adjacencyList.get(sourceNode.id)!.add(targetNode.id);
      graph.reverseAdjacencyList.get(targetNode.id)!.add(sourceNode.id);
      
      // Update degrees
      graph.outdegree.set(sourceNode.id, graph.outdegree.get(sourceNode.id)! + 1);
      graph.indegree.set(targetNode.id, graph.indegree.get(targetNode.id)! + 1);

      // Track connections
      graph.connectionMap.set(connection.id, connection);
      graph.nodeConnections.get(sourceNode.id)!.push(connection.id);
      graph.nodeConnections.get(targetNode.id)!.push(connection.id); // FIXED: was targetNodeId, now targetNode.id
    });

    // Calculate topological levels
    this.calculateNodeLevels(graph);

    const elapsed = Date.now() - startTime;
    this.logger.debug(`Dependency graph built in ${elapsed}ms`);
    this.logger.info(`Graph stats: ${graph.nodes.size} nodes, ${this.connections.length} connections`);

    return graph;
  }

  /**
   * Detect cycles in the dependency graph
   */
  private detectCycles(graph: DependencyGraph): string[][] {
    const cycles: string[][] = [];
    const visited = new Set<string>();
    const recursionStack = new Set<string>();
    const path: string[] = [];

    const dfs = (nodeId: string): boolean => {
      if (!visited.has(nodeId)) {
        visited.add(nodeId);
        recursionStack.add(nodeId);
        path.push(nodeId);

        const neighbors = graph.adjacencyList.get(nodeId) || new Set();
        for (const neighbor of neighbors) {
          if (!visited.has(neighbor)) {
            if (dfs(neighbor)) {
              return true;
            }
          } else if (recursionStack.has(neighbor)) {
            // Cycle detected
            const cycleStart = path.indexOf(neighbor);
            if (cycleStart !== -1) {
              const cycle = path.slice(cycleStart);
              cycles.push([...cycle]);
            }
            return true;
          }
        }
      }

      recursionStack.delete(nodeId);
      path.pop();
      return false;
    };

    for (const nodeId of graph.nodes.keys()) {
      if (!visited.has(nodeId)) {
        dfs(nodeId);
      }
    }

    if (cycles.length > 0) {
      this.logger.warn(`Detected ${cycles.length} cycle(s) in dependency graph`);
      cycles.forEach((cycle, index) => {
        this.logger.warn(`Cycle ${index + 1}: ${cycle.join(' -> ')}`);
      });
    }

    return cycles;
  }

  /**
   * Perform topological sort using Kahn's algorithm
   */
  private topologicalSort(graph: DependencyGraph): string[] {
    const startTime = Date.now();
    this.logger.debug('Performing topological sort');

    // Create copies to avoid mutating original
    const indegree = new Map(graph.indegree);
    const adjacencyList = new Map(
      Array.from(graph.adjacencyList.entries()).map(([k, v]) => [k, new Set(v)])
    );

    const queue: string[] = [];
    const result: string[] = [];

    // Initialize queue with nodes having 0 indegree
    for (const [nodeId, degree] of indegree.entries()) {
      if (degree === 0) {
        queue.push(nodeId);
      }
    }

    // Process nodes
    while (queue.length > 0) {
      // Sort queue for deterministic output (optional)
      queue.sort();

      const nodeId = queue.shift()!;
      result.push(nodeId);

      const neighbors = adjacencyList.get(nodeId) || new Set();
      for (const neighbor of neighbors) {
        indegree.set(neighbor, indegree.get(neighbor)! - 1);
        if (indegree.get(neighbor) === 0) {
          queue.push(neighbor);
        }
      }
    }

    // Check if all nodes were processed
    if (result.length !== graph.nodes.size) {
      this.logger.error('Topological sort failed - graph may have cycles');
      throw new Error('Pipeline contains circular dependencies');
    }

    const elapsed = Date.now() - startTime;
    this.logger.debug(`Topological sort completed in ${elapsed}ms`);
    this.logger.info(`Topological order: ${result.join(' -> ')}`);

    return result;
  }

  /**
   * Calculate topological levels for each node
   */
  private calculateNodeLevels(graph: DependencyGraph): void {
    // Perform BFS from sources (nodes with indegree 0)
    const visited = new Set<string>();
    const queue: Array<[string, number]> = [];

    // Initialize with sources at level 0
    for (const [nodeId, indegree] of graph.indegree.entries()) {
      if (indegree === 0) {
        queue.push([nodeId, 0]);
        visited.add(nodeId);
      }
    }

    while (queue.length > 0) {
      const [nodeId, level] = queue.shift()!;
      graph.nodeLevels.set(nodeId, level);

      const neighbors = graph.adjacencyList.get(nodeId) || new Set();
      for (const neighbor of neighbors) {
        if (!visited.has(neighbor)) {
          visited.add(neighbor);
          queue.push([neighbor, level + 1]);
        } else {
          // Update level if we found a longer path
          const currentLevel = graph.nodeLevels.get(neighbor) || 0;
          if (level + 1 > currentLevel) {
            graph.nodeLevels.set(neighbor, level + 1);
          }
        }
      }
    }

    // Ensure all nodes have a level
    for (const nodeId of graph.nodes.keys()) {
      if (!graph.nodeLevels.has(nodeId)) {
        graph.nodeLevels.set(nodeId, -1); // Mark as unreachable
      }
    }
  }

  /**
   * Detect parallel execution groups
   */
  private detectParallelGroups(
    graph: DependencyGraph,
    topologicalOrder: string[]
  ): string[][] {
    const startTime = Date.now();
    this.logger.debug('Detecting parallel execution groups');

    // Group nodes by topological level
    const levelGroups = new Map<number, string[]>();
    
    for (const nodeId of topologicalOrder) {
      const level = graph.nodeLevels.get(nodeId) || 0;
      if (!levelGroups.has(level)) {
        levelGroups.set(level, []);
      }
      levelGroups.get(level)!.push(nodeId);
    }

    // Convert to array and sort by level
    const groups = Array.from(levelGroups.entries())
      .sort(([levelA], [levelB]) => levelA - levelB)
      .map(([, nodes]) => nodes);

    // Further analyze within groups for finer parallelism
    const refinedGroups: string[][] = [];
    
    for (const group of groups) {
      if (group.length <= 1) {
        refinedGroups.push(group);
        continue;
      }

      // Check dependencies within group
      const independentSubgroups: string[][] = [];
      const visitedInGroup = new Set<string>();
      
      for (const nodeId of group) {
        if (visitedInGroup.has(nodeId)) continue;
        
        const subgroup = [nodeId];
        visitedInGroup.add(nodeId);
        
        // Find other nodes in group that don't depend on this node
        for (const otherNodeId of group) {
          if (otherNodeId === nodeId || visitedInGroup.has(otherNodeId)) continue;
          
          // Check if there's any dependency between nodes
          const dependsOn = this.checkDependency(graph, otherNodeId, nodeId);
          const dependsOnReverse = this.checkDependency(graph, nodeId, otherNodeId);
          
          if (!dependsOn && !dependsOnReverse) {
            subgroup.push(otherNodeId);
            visitedInGroup.add(otherNodeId);
          }
        }
        
        independentSubgroups.push(subgroup);
      }
      
      refinedGroups.push(...independentSubgroups);
    }

    const elapsed = Date.now() - startTime;
    this.logger.debug(`Parallel groups detected in ${elapsed}ms`);
    
    // Log group information
    refinedGroups.forEach((group, index) => {
      if (group.length > 1) {
        this.logger.info(`Parallel group ${index}: ${group.join(', ')}`);
      }
    });

    return refinedGroups;
  }

  /**
   * Check if nodeA depends on nodeB (directly or indirectly)
   */
  private checkDependency(graph: DependencyGraph, nodeA: string, nodeB: string): boolean {
    // Quick check for direct dependency
    if (graph.adjacencyList.get(nodeA)?.has(nodeB)) {
      return true;
    }

    // BFS for indirect dependencies
    const visited = new Set<string>();
    const queue = [nodeA];
    
    while (queue.length > 0) {
      const current = queue.shift()!;
      if (current === nodeB) return true;
      
      const neighbors = graph.adjacencyList.get(current) || new Set();
      for (const neighbor of neighbors) {
        if (!visited.has(neighbor)) {
          visited.add(neighbor);
          queue.push(neighbor);
        }
      }
    }
    
    return false;
  }

  /**
   * Create comprehensive execution plan
   */
  private createExecutionPlan(
    graph: DependencyGraph,
    topologicalOrder: string[],
    parallelGroups: string[][]
  ): ExecutionPlan {
    this.logger.debug('Creating execution plan');

    // Calculate critical path (longest path through DAG)
    const criticalPath = this.calculateCriticalPath(graph, topologicalOrder);
    
    // Estimate costs
    const estimatedCost = this.estimateExecutionCost(graph, topologicalOrder);
    
    // Calculate memory footprint
    const memoryFootprint = this.estimateMemoryFootprint(graph);
    
    // Calculate timing estimates
    const timingEstimates = this.estimateTiming(graph, topologicalOrder, parallelGroups);
    
    // Find optimization opportunities
    const optimizationOpportunities = this.findOptimizationOpportunities(graph);

    return {
      topologicalOrder,
      parallelGroups,
      criticalPath,
      estimatedCost,
      memoryFootprint,
      timingEstimates,
      optimizationOpportunities,
      dependencies: this.extractDependencyMap(graph)
    };
  }

  /**
   * Calculate critical path through DAG
   */
  private calculateCriticalPath(graph: DependencyGraph, _topologicalOrder: string[]): string[] {
    // Simple approach: longest path based on node levels
    const maxLevel = Math.max(...Array.from(graph.nodeLevels.values()).filter(l => l >= 0));
    
    // Find nodes at max level and trace back
    const criticalNodes: string[] = [];
    for (const [nodeId, level] of graph.nodeLevels.entries()) {
      if (level === maxLevel) {
        criticalNodes.push(nodeId);
      }
    }

    // For now, return the first critical node chain
    // In a full implementation, we would trace back through predecessors
    return criticalNodes.length > 0 ? [criticalNodes[0]] : [];
  }

  /**
   * Estimate execution cost
   */
  private estimateExecutionCost(graph: DependencyGraph, topologicalOrder: string[]): number {
    // Simple heuristic based on node types and connections
    let cost = 0;
    
    for (const nodeId of topologicalOrder) {
      const node = graph.nodes.get(nodeId)!;
      const connections = graph.nodeConnections.get(nodeId) || [];
      
      // Base cost per node type
      switch (node.type) {
        case NodeType.JOIN:
        case NodeType.MAP:
          cost += 100;
          break;
        case NodeType.AGGREGATE_ROW:
          cost += 150;
          break;
        case NodeType.FILTER_ROW:
          cost += 50;
          break;
        default:
          cost += 10;
      }
      
      // Additional cost for multiple connections
      cost += connections.length * 5;
      
      // Cost based on node level (deeper = more expensive due to data volume)
      const level = graph.nodeLevels.get(nodeId) || 0;
      cost += level * 20;
    }
    
    return cost;
  }

  /**
   * Estimate memory footprint
   */
  private estimateMemoryFootprint(graph: DependencyGraph): { estimated: number; peak: number } {
    // Simple heuristic: assume 1KB per column in each node
    let estimated = 0;
    let peak = 0;
    let current = 0;
    
    for (const [nodeId] of graph.indegree.entries()) {
      const node = graph.nodes.get(nodeId)!;
      const columnCount = node.metadata?.tableMapping?.columns?.length || 10;
      
      // Memory for this node's output
      const nodeMemory = columnCount * 1024; // 1KB per column
      current += nodeMemory;
      peak = Math.max(peak, current);
      
      // Memory is released after nodes that depend on this node are processed
      const outdegree = graph.outdegree.get(nodeId) || 0;
      if (outdegree === 0) {
        current -= nodeMemory;
      }
      
      estimated += nodeMemory;
    }
    
    return {
      estimated,
      peak: Math.max(estimated / 4, peak) // Conservative estimate
    };
  }

  /**
   * Estimate timing for sequential and parallel execution
   */
  private estimateTiming(
    graph: DependencyGraph,
    topologicalOrder: string[],
    parallelGroups: string[][]
  ): { sequential: number; parallel: number; recommendedParallelism: number } {
    // Estimate sequential time
    let sequential = 0;
    for (const nodeId of topologicalOrder) {
      const node = graph.nodes.get(nodeId)!;
      sequential += this.estimateNodeExecutionTime(node);
    }
    
    // Estimate parallel time
    let parallel = 0;
    
    for (const group of parallelGroups) {
      let groupTime = 0;
      for (const nodeId of group) {
        const node = graph.nodes.get(nodeId)!;
        groupTime = Math.max(groupTime, this.estimateNodeExecutionTime(node));
      }
      parallel += groupTime;
    }
    
    // Calculate recommended parallelism
    const totalWork = sequential;
    const recommendedParallelism = Math.min(
      this.options.maxParallelDepth,
      Math.ceil(totalWork / parallel)
    );
    
    return {
      sequential: Math.round(sequential),
      parallel: Math.round(parallel),
      recommendedParallelism
    };
  }

  /**
   * Estimate node execution time based on type
   */
  private estimateNodeExecutionTime(node: CanvasNode): number {
    switch (node.type) {
      case NodeType.JOIN:
        return 500; // ms
      case NodeType.MAP:
        return 300;
      case NodeType.AGGREGATE_ROW:
        return 700;
      case NodeType.FILTER_ROW:
        return 100;
      case NodeType.SORT_ROW:
        return 600;
      default:
        return 200;
    }
  }

  /**
   * Find optimization opportunities
   */
  private findOptimizationOpportunities(graph: DependencyGraph): Array<{
    type: string;
    description: string;
    potentialImprovement: number;
  }> {
    const opportunities: Array<{
      type: string;
      description: string;
      potentialImprovement: number;
    }> = [];

    // Look for patterns that could be optimized
    
    // 1. Sequential filters that could be combined
    let filterCount = 0;
    for (const node of graph.nodes.values()) {
      if (node.type === NodeType.FILTER_ROW) {
        filterCount++;
      }
    }
    
    if (filterCount > 1) {
      opportunities.push({
        type: 'FILTER_COMBINATION',
        description: `Multiple filters (${filterCount}) that could be combined`,
        potentialImprovement: Math.min(30, filterCount * 10)
      });
    }
    
    // 2. Small joins that could be subqueries
    for (const [nodeId, indegree] of graph.indegree.entries()) {
      const node = graph.nodes.get(nodeId)!;
      if (node.type === NodeType.JOIN && indegree === 2) {
        const neighbors = graph.adjacencyList.get(nodeId)?.size || 0;
        if (neighbors === 1) {
          opportunities.push({
            type: 'JOIN_TO_SUBQUERY',
            description: `Join node ${nodeId} used only once, could be a subquery`,
            potentialImprovement: 15
          });
        }
      }
    }
    
    // 3. Materialization opportunities
    for (const [nodeId, outdegree] of graph.outdegree.entries()) {
      if (outdegree > 3) {
        opportunities.push({
          type: 'MATERIALIZATION',
          description: `Node ${nodeId} has ${outdegree} dependents, could benefit from materialization`,
          potentialImprovement: 25
        });
      }
    }
    
    return opportunities;
  }

  /**
   * Extract dependency map from graph
   */
  private extractDependencyMap(graph: DependencyGraph): Record<string, string[]> {
    const dependencies: Record<string, string[]> = {};
    
    for (const [nodeId, incoming] of graph.reverseAdjacencyList.entries()) {
      dependencies[nodeId] = Array.from(incoming);
    }
    
    return dependencies;
  }

  // ==================== SQL GENERATION ====================

  /**
   * Generate SQL fragments for all nodes in pipeline
   */
  private async generatePipelineFragments(
    graph: DependencyGraph,
    topologicalOrder: string[],
    context: PipelineContext
  ): Promise<Map<string, GeneratedSQLFragment>> {
    const fragments = new Map<string, GeneratedSQLFragment>();
    const totalNodes = topologicalOrder.length;
    
    this.logger.info(`Generating SQL fragments for ${totalNodes} nodes`);

    for (let i = 0; i < topologicalOrder.length; i++) {
      const nodeId = topologicalOrder[i];
      const node = graph.nodes.get(nodeId)!;
      
      this.updateProgress(
        'generation',
        60 + (i / totalNodes) * 20,
        `Generating SQL for node: ${node.name} (${node.type})`
      );

      // Get or create generator for node type
      let generator = context.nodeGenerators.get(node.type);
      if (!generator) {
        generator = SQLGeneratorFactory.createGenerator(node.type, {
          postgresVersion: this.options.postgresVersion,
          includeComments: this.options.includeComments,
          formatSQL: this.options.formatSQL
        });
        
        // FIXED: Handle undefined generator (unsupported node type)
        if (!generator) {
          const errorMsg = `No SQL generator available for node type: ${node.type}`;
          this.logger.error(errorMsg);
          context.errors.push({
            code: 'UNSUPPORTED_NODE_TYPE',
            message: errorMsg,
            nodeId: node.id,
            recoverable: false
          });
          continue; // skip this node
        }
        
        context.nodeGenerators.set(node.type, generator);
      }

      // Get incoming connections
      const incomingNodeIds = Array.from(graph.reverseAdjacencyList.get(nodeId) || []);
      const incomingConnections = this.connections.filter(conn => 
        incomingNodeIds.includes(conn.sourceNodeId) && conn.targetNodeId === nodeId
      );

      // Get schema from upstream nodes
      const upstreamSchema = this.calculateUpstreamSchema(nodeId, graph, context);

      // Create generation context
      const generationContext: SQLGenerationContext = {
        node,
        connection: incomingConnections[0], // Primary connection
        indentLevel: graph.nodeLevels.get(nodeId) || 0,
        parameters: new Map(),
        options: {
          includeComments: this.options.includeComments,
          formatSQL: this.options.formatSQL,
          targetDialect: 'POSTGRESQL',
          postgresVersion: this.options.postgresVersion,
          useCTEs: this.options.useCTEs,
          optimizeForReadability: true,
          includeExecutionPlan: this.options.generateExplainPlan,
          parameterizeValues: true,
          maxLineLength: 80
        }
      };

      // Generate SQL fragment
      const fragment = await this.generateNodeFragment(
        node,
        generator,
        generationContext,
        upstreamSchema,
        context
      );

      fragments.set(nodeId, fragment);
      context.fragmentCache.set(nodeId, fragment);
      context.stats.fragmentsGenerated++;

      // Update schema cache
      context.schemaCache.set(nodeId, this.extractOutputSchema(fragment, node));

      // Handle errors and warnings
      if (fragment.errors.length > 0) {
        fragment.errors.forEach(error => {
          context.errors.push({
            code: error.code,
            message: error.message,
            nodeId,
            recoverable: error.severity !== 'ERROR',
            recoverySuggestion: error.suggestion
          });
        });
      }

      if (fragment.warnings.length > 0) {
        fragment.warnings.forEach(warning => {
          context.warnings.push({
            code: 'FRAGMENT_WARNING',
            message: warning,
            nodeId,
            severity: 'LOW',
            suggestion: 'Review node configuration'
          });
        });
      }

      // Log progress
      this.logger.debug(`Generated fragment for node ${nodeId} (${node.name})`);
    }

    this.logger.info(`Generated ${fragments.size} SQL fragments`);
    return fragments;
  }

  /**
   * Generate SQL fragment for a single node
   */
  private async generateNodeFragment(
    node: CanvasNode,
    generator: BaseSQLGenerator,
    context: SQLGenerationContext,
    upstreamSchema: Array<{ name: string; dataType: PostgreSQLDataType }>,
    pipelineContext: PipelineContext
  ): Promise<GeneratedSQLFragment> {
    const cacheKey = this.createFragmentCacheKey(node, upstreamSchema);
    
    // Check cache first
    const cachedFragment = pipelineContext.fragmentCache.get(cacheKey);
    if (cachedFragment) {
      pipelineContext.stats.cacheHits++;
      pipelineContext.logger.debug(`Cache hit for fragment: ${cacheKey}`);
      return cachedFragment;
    }

    pipelineContext.stats.cacheMisses++;
    pipelineContext.logger.debug(`Generating new fragment: ${cacheKey}`);

    try {
      // Generate SQL using the appropriate generator
      const fragment = generator.generateSQL(context);
      
      // Add CTE entry if using CTEs
      if (this.options.useCTEs) {
        const cteEntry: CTEChainEntry = {
          nodeId: node.id,
          cteName: this.generateCTEName(node),
          sql: fragment.sql,
          materialized: this.shouldMaterializeCTE(node, pipelineContext),
          dependencies: fragment.dependencies,
          columns: upstreamSchema,
          estimatedRows: this.estimateRowCount(node),
          shouldMaterialize: this.shouldMaterializeCTE(node, pipelineContext)
        };
        
        pipelineContext.cteChain.set(node.id, cteEntry);
      }
      
      // Cache the fragment
      pipelineContext.fragmentCache.set(cacheKey, fragment);
      
      return fragment;
      
    } catch (error) {
      pipelineContext.logger.error(`Failed to generate fragment for node ${node.id}`, error);
      
      return {
        sql: '',
        dependencies: [],
        parameters: new Map(),
        errors: [{
          code: 'GENERATION_FAILED',
          message: error instanceof Error ? error.message : 'Unknown generation error',
          severity: 'ERROR'
        }],
        warnings: [],
        metadata: {
          generatedAt: new Date().toISOString(),
          fragmentType: 'error',
          lineCount: 0
        }
      };
    }
  }

  /**
   * Calculate upstream schema for a node
   */
  private calculateUpstreamSchema(
    nodeId: string,
    graph: DependencyGraph,
    context: PipelineContext
  ): Array<{ name: string; dataType: PostgreSQLDataType }> {
    const incomingNodes = Array.from(graph.reverseAdjacencyList.get(nodeId) || []);
    
    if (incomingNodes.length === 0) {
      // Source node - get schema from node metadata
      const node = graph.nodes.get(nodeId)!;
      return this.extractNodeSchema(node);
    }
    
    // Merge schemas from all upstream nodes
    const mergedSchema = new Map<string, { name: string; dataType: PostgreSQLDataType }>();
    
    for (const upstreamNodeId of incomingNodes) {
      const upstreamSchema = context.schemaCache.get(upstreamNodeId);
      if (upstreamSchema) {
        upstreamSchema.forEach(column => {
          mergedSchema.set(column.name, column);
        });
      }
    }
    
    // Apply schema mappings from connections
    const connections = this.connections.filter(conn => 
      conn.targetNodeId === nodeId && incomingNodes.includes(conn.sourceNodeId)
    );
    
    connections.forEach(connection => {
      if (connection.dataFlow.schemaMappings) {
        connection.dataFlow.schemaMappings.forEach(mapping => {
          const sourceColumn = mergedSchema.get(mapping.sourceColumn);
          if (sourceColumn) {
            mergedSchema.set(mapping.targetColumn, {
              name: mapping.targetColumn,
              dataType: mapping.dataTypeConversion?.to || sourceColumn.dataType
            });
          }
        });
      }
    });
    
    return Array.from(mergedSchema.values());
  }

  /**
   * Extract schema from node metadata
   */
  private extractNodeSchema(node: CanvasNode): Array<{ name: string; dataType: PostgreSQLDataType }> {
    if (node.metadata?.tableMapping?.columns) {
      return node.metadata.tableMapping.columns.map(col => ({
        name: col.name,
        dataType: col.dataType
      }));
    }
    
    // Default schema for unknown nodes
    return [
      { name: 'id', dataType: PostgreSQLDataType.INTEGER },
      { name: 'created_at', dataType: PostgreSQLDataType.TIMESTAMP }
    ];
  }

  /**
   * Extract output schema from generated fragment
   */
  private extractOutputSchema(
    fragment: GeneratedSQLFragment,
    node: CanvasNode
  ): Array<{ name: string; dataType: PostgreSQLDataType }> {
    // Try to extract from fragment metadata
    if (fragment.metadata.fragmentType === 'complete_sql') {
      // Parse SELECT clause to extract columns
      const selectMatch = fragment.sql.match(/SELECT\s+(.+?)\s+FROM/i);
      if (selectMatch) {
        const columns = selectMatch[1].split(',').map(col => {
          const parts = col.trim().split(/\s+as\s+/i);
          const name = parts[parts.length - 1].trim().replace(/["`]/g, '');
          return { name, dataType: PostgreSQLDataType.VARCHAR }; // Default type
        });
        return columns;
      }
    }
    
    // Fallback to node schema
    return this.extractNodeSchema(node);
  }

  /**
   * Create cache key for fragment memoization
   */
  private createFragmentCacheKey(
    node: CanvasNode,
    upstreamSchema: Array<{ name: string; dataType: PostgreSQLDataType }>
  ): string {
    const schemaHash = upstreamSchema
      .map(col => `${col.name}:${col.dataType}`)
      .join('|');
    
    return `${node.id}:${node.type}:${schemaHash}:${JSON.stringify(node.metadata || {})}`;
  }

  /**
   * Check if cache entry is still valid
   */
  private isCacheValid(cachedResult: PipelineGenerationResult): boolean {
    // Check if cache is older than 5 minutes
    const cacheAge = Date.now() - new Date(cachedResult.generatedAt).getTime();
    return cacheAge < 5 * 60 * 1000; // 5 minutes
  }

  // ==================== OPTIMIZATION METHODS ====================

  /**
   * Apply optimizations to generated fragments
   */
  private async applyOptimizations(
    fragments: Map<string, GeneratedSQLFragment>,
    graph: DependencyGraph,
    context: PipelineContext
  ): Promise<{
    summary: OptimizationSummary;
    warnings: PipelineWarning[];
  }> {
    const summary: OptimizationSummary = {
      cteFlattened: 0,
      subqueriesConverted: 0,
      predicatePushdowns: 0,
      materializedCTEs: 0,
      warningsSuppressed: 0,
      estimatedPerformanceGain: 0
    };

    const warnings: PipelineWarning[] = [];

    this.logger.info('Applying optimizations');

    // 1. CTE Flattening
    if (this.options.useCTEs) {
      summary.cteFlattened = this.applyCTEFlattening(fragments, graph, context);
      context.logger.info(`Flattened ${summary.cteFlattened} CTEs`);
    }

    // 2. Subquery to JOIN conversion
    summary.subqueriesConverted = this.convertSubqueriesToJoins(fragments, graph, context);
    context.logger.info(`Converted ${summary.subqueriesConverted} subqueries to JOINs`);

    // 3. Predicate pushdown
    summary.predicatePushdowns = this.applyPredicatePushdown(fragments, graph, context);
    context.logger.info(`Applied ${summary.predicatePushdowns} predicate pushdowns`);

    // 4. Materialize expensive CTEs
    if (this.options.materializeIntermediate) {
      summary.materializedCTEs = this.materializeExpensiveCTEs(fragments, graph, context);
      context.logger.info(`Materialized ${summary.materializedCTEs} CTEs`);
    }

    // 5. Estimate performance gain
    summary.estimatedPerformanceGain = this.estimatePerformanceGain(summary);

    // Update context stats
    context.stats.optimizationApplied = 
      summary.cteFlattened + 
      summary.subqueriesConverted + 
      summary.predicatePushdowns;

    return { summary, warnings };
  }

  /**
   * Apply CTE flattening optimization
   */
  private applyCTEFlattening(
    fragments: Map<string, GeneratedSQLFragment>,
    graph: DependencyGraph,
    context: PipelineContext
  ): number {
    let flattenedCount = 0;
    
    // Identify CTEs that are used only once and can be inlined
    for (const [nodeId, fragment] of fragments.entries()) {
      const outgoingNodes = graph.adjacencyList.get(nodeId);
      
      // If this CTE is used by only one node, consider flattening
      if (outgoingNodes?.size === 1) {
        const dependentNodeId = Array.from(outgoingNodes)[0];
        const dependentFragment = fragments.get(dependentNodeId);
        
        if (dependentFragment && this.isCTEFlattenable(fragment, dependentFragment)) {
          // Inline the CTE into the dependent fragment
          const flattenedSQL = this.inlineCTE(fragment.sql, dependentFragment.sql);
          
          fragments.set(dependentNodeId, {
            ...dependentFragment,
            sql: flattenedSQL,
            dependencies: dependentFragment.dependencies.filter(dep => dep !== nodeId)
          });
          
          flattenedCount++;
          context.logger.debug(`Flattened CTE from ${nodeId} into ${dependentNodeId}`);
        }
      }
    }
    
    return flattenedCount;
  }

  /**
   * Check if CTE can be flattened
   */
  private isCTEFlattenable(
    cteFragment: GeneratedSQLFragment,
    dependentFragment: GeneratedSQLFragment
  ): boolean {
    // Don't flatten if CTE has errors
    if (cteFragment.errors.length > 0) return false;
    
    // Don't flatten if CTE is complex (has subqueries, multiple joins, etc.)
    const cteComplexity = this.estimateSQLComplexity(cteFragment.sql);
    if (cteComplexity > 50) return false; // Arbitrary threshold
    
    // Don't flatten if dependent fragment is already complex
    const dependentComplexity = this.estimateSQLComplexity(dependentFragment.sql);
    if (dependentComplexity > 100) return false;
    
    return true;
  }

  /**
   * Inline CTE into dependent SQL
   */
  private inlineCTE(cteSQL: string, dependentSQL: string): string {
    // Simple inlining: replace CTE reference with subquery
    // This is a simplified version - real implementation would parse SQL properly
    return dependentSQL.replace(/WITH\s+[\s\S]*?SELECT/i, (match) => {
      // Extract the CTE name and replace it with subquery
      return match + ` (${cteSQL})`;
    });
  }

  /**
   * Convert subqueries to JOINs where possible
   */
  private convertSubqueriesToJoins(
    fragments: Map<string, GeneratedSQLFragment>,
    _graph: DependencyGraph,
    context: PipelineContext
  ): number {
    let convertedCount = 0;
    
    for (const [nodeId, fragment] of fragments.entries()) {
      if (fragment.errors.length > 0) continue;
      
      // Look for EXISTS subqueries
      const existsPattern = /WHERE\s+EXISTS\s*\(\s*SELECT\s+1\s+FROM\s+([^\s)]+)\s+WHERE\s+([^)]+)\)/gi;
      let match: RegExpExecArray | null;
      
      while ((match = existsPattern.exec(fragment.sql)) !== null) {
        const [, subqueryTable, condition] = match;
        
        // Convert to JOIN
        const joinSQL = `INNER JOIN ${subqueryTable} ON ${condition}`;
        const convertedSQL = fragment.sql.replace(match[0], joinSQL);
        
        fragments.set(nodeId, {
          ...fragment,
          sql: convertedSQL
        });
        
        convertedCount++;
        context.logger.debug(`Converted EXISTS subquery to JOIN in ${nodeId}`);
      }
    }
    
    return convertedCount;
  }

  /**
   * Apply predicate pushdown optimization
   */
  private applyPredicatePushdown(
    fragments: Map<string, GeneratedSQLFragment>,
    graph: DependencyGraph,
    context: PipelineContext
  ): number {
    let pushdownCount = 0;
    
    // For each node, try to push its WHERE conditions to upstream nodes
    for (const [nodeId, fragment] of fragments.entries()) {
      const incomingNodes = Array.from(graph.reverseAdjacencyList.get(nodeId) || []);
      
      for (const upstreamNodeId of incomingNodes) {
        const upstreamFragment = fragments.get(upstreamNodeId);
        if (!upstreamFragment) continue;
        
        // Extract WHERE clause from current fragment
        const whereClause = this.extractWhereClause(fragment.sql);
        if (!whereClause) continue;
        
        // Check if WHERE clause can be pushed to upstream
        if (this.canPushPredicateToUpstream(whereClause, upstreamFragment, fragment)) {
          // Push predicate to upstream
          const pushedSQL = this.pushPredicateToSQL(upstreamFragment.sql, whereClause);
          
          fragments.set(upstreamNodeId, {
            ...upstreamFragment,
            sql: pushedSQL
          });
          
          // Remove pushed predicate from current fragment if possible
          const cleanedSQL = this.removePushedPredicate(fragment.sql, whereClause);
          fragments.set(nodeId, {
            ...fragment,
            sql: cleanedSQL
          });
          
          pushdownCount++;
          context.logger.debug(`Pushed predicate from ${nodeId} to ${upstreamNodeId}`);
        }
      }
    }
    
    return pushdownCount;
  }

  /**
   * Check if predicate can be pushed to upstream node
   */
  private canPushPredicateToUpstream(
    whereClause: string,
    upstreamFragment: GeneratedSQLFragment,
    _currentFragment: GeneratedSQLFragment
  ): boolean {
    // Check if predicate references only columns available in upstream
    const upstreamColumns = this.extractReferencedColumns(upstreamFragment.sql);
    const predicateColumns = this.extractReferencedColumns(whereClause);
    
    // All predicate columns must be in upstream
    return predicateColumns.every(col => upstreamColumns.includes(col));
  }

  /**
   * Push predicate into SQL query
   */
  private pushPredicateToSQL(sql: string, whereClause: string): string {
    // Add WHERE clause or combine with existing
    if (sql.includes('WHERE')) {
      return sql.replace(/WHERE\s+(.+)/i, `WHERE ($1) AND (${whereClause})`);
    } else if (sql.includes('FROM')) {
      return sql.replace(/FROM\s+([^\s;]+)/i, `FROM $1 WHERE ${whereClause}`);
    }
    return sql + ` WHERE ${whereClause}`;
  }

  /**
   * Remove pushed predicate from SQL
   */
  private removePushedPredicate(sql: string, whereClause: string): string {
    // Simplified removal - real implementation would parse SQL
    return sql.replace(new RegExp(`\\s*WHERE\\s*${whereClause.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}`, 'i'), '');
  }

  /**
   * Materialize expensive CTEs
   */
  private materializeExpensiveCTEs(
    fragments: Map<string, GeneratedSQLFragment>,
    graph: DependencyGraph,
    context: PipelineContext
  ): number {
    let materializedCount = 0;
    
    for (const [nodeId, fragment] of fragments.entries()) {
      const outgoingNodes = graph.adjacencyList.get(nodeId);
      
      // Materialize if node has multiple dependents and is expensive
      if (outgoingNodes && outgoingNodes.size > 1) {
        const complexity = this.estimateSQLComplexity(fragment.sql);
        if (complexity > 30) { // Threshold for "expensive"
          // Add MATERIALIZED keyword to CTE
          const materializedSQL = fragment.sql.replace(/WITH\s+(\w+)\s+AS\s*\(/i, 'WITH $1 AS MATERIALIZED (');
          
          fragments.set(nodeId, {
            ...fragment,
            sql: materializedSQL
          });
          
          materializedCount++;
          context.logger.debug(`Materialized CTE for node ${nodeId}`);
        }
      }
    }
    
    return materializedCount;
  }

  /**
   * Estimate performance gain from optimizations
   */
  private estimatePerformanceGain(summary: OptimizationSummary): number {
    let gain = 0;
    
    // CTE flattening: up to 20% gain per flattened CTE
    gain += summary.cteFlattened * 15;
    
    // Subquery conversion: up to 50% gain per conversion
    gain += summary.subqueriesConverted * 40;
    
    // Predicate pushdown: up to 30% gain per pushdown
    gain += summary.predicatePushdowns * 25;
    
    // Materialization: up to 40% gain for expensive repeated CTEs
    gain += summary.materializedCTEs * 35;
    
    return Math.min(gain, 80); // Cap at 80% total improvement
  }
/**
 * Sanitize and quote an identifier for PostgreSQL.
 * Replaces invalid characters with underscore, ensures it doesn't start with a digit,
 * and always double-quotes to avoid reserved keyword conflicts.
 */
private sanitizeIdentifier(identifier: string): string {
  if (!identifier) return '""';
  // Replace any character that is not alphanumeric or underscore with underscore
  let sanitized = identifier.replace(/[^a-zA-Z0-9_]/g, '_');
  // If it starts with a digit, prefix with underscore
  if (/^[0-9]/.test(sanitized)) {
    sanitized = '_' + sanitized;
  }
  // Double-quote and escape any embedded double quotes
  return `"${sanitized.replace(/"/g, '""')}"`;
}
  // ==================== FINAL SQL ASSEMBLY ====================

  /**
   * Build final SQL from fragments – now produces a single INSERT INTO ... SELECT statement
   */
  private async buildFinalSQL(
    fragments: Map<string, GeneratedSQLFragment>,
    graph: DependencyGraph,
    topologicalOrder: string[],
    context: PipelineContext
  ): Promise<string> {
    this.logger.info('Building final SQL');

    // 1. Build mapping nodeId -> CTE name
    const cteNameMap = new Map<string, string>();
    for (const nodeId of topologicalOrder) {
      const node = graph.nodes.get(nodeId)!;
      cteNameMap.set(nodeId, this.generateCTEName(node));
    }

    // 2. Modify each fragment to replace upstream node IDs with their CTE names
    const modifiedFragments = new Map<string, GeneratedSQLFragment>();
    for (const nodeId of topologicalOrder) {
      const fragment = fragments.get(nodeId);
      if (!fragment) continue;

      let sql = fragment.sql;
      // Replace references to all upstream nodes (dependencies)
      const incomingNodes = Array.from(graph.reverseAdjacencyList.get(nodeId) || []);
      for (const sourceId of incomingNodes) {
        if (cteNameMap.has(sourceId)) {
          sql = this.replaceNodeReferences(sql, new Map([[sourceId, cteNameMap.get(sourceId)!]]));
        }
      }
      modifiedFragments.set(nodeId, { ...fragment, sql });
    }

    // 3. Identify the output node (first node with type OUTPUT, else last node)
    let outputNode: CanvasNode | undefined;
    for (const nodeId of topologicalOrder) {
      const node = graph.nodes.get(nodeId);
      if (node && this.isOutputNode(node)) {
        outputNode = node;
        break;
      }
    }
    if (!outputNode) {
      outputNode = graph.nodes.get(topologicalOrder[topologicalOrder.length - 1]);
      this.logger.warn('No output node found; using last node as output');
    }

    // 4. Get target table name (fallback to sanitized node name)
    const targetTable = this.sanitizeIdentifier(this.getTargetTableName(outputNode));

    // 5. Get output column list from the final fragment
    const finalFragment = modifiedFragments.get(outputNode.id);
    if (!finalFragment) {
      throw new Error(`No fragment generated for output node ${outputNode.id}`);
    }
    const outputColumns = this.extractOutputSchema(finalFragment, outputNode);
    const columnNames = outputColumns.map(col => col.name);
    const insertCols = columnNames.map(name => this.sanitizeIdentifier(name)).join(', ');
    const selectCols = columnNames.map(name => this.sanitizeIdentifier(name)).join(', ');

    // 6. Build the CTE definitions (WITH clause)
    const cteDefinitions: string[] = [];
    for (const nodeId of topologicalOrder) {
      const fragment = modifiedFragments.get(nodeId);
      if (!fragment) continue;

      const cteName = cteNameMap.get(nodeId)!;
      const materialized = this.shouldMaterializeCTE(graph.nodes.get(nodeId)!, context);
      const materializedKeyword = materialized ? 'MATERIALIZED ' : '';

      let cteSql = fragment.sql.trim();
      if (cteSql.endsWith(';')) {
        cteSql = cteSql.slice(0, -1);
      }

      cteDefinitions.push(`${cteName} AS ${materializedKeyword}(\n${this.indent(cteSql, 2)}\n)`);
    }

    // 7. Assemble final SQL
    let finalSQL = '';
    if (this.options.wrapInTransaction) {
      finalSQL += 'BEGIN;\n\n';
    }

    if (this.options.includeComments) {
      finalSQL += this.generatePipelineHeader();
    }

    if (cteDefinitions.length > 0) {
      finalSQL += `WITH\n${cteDefinitions.join(',\n')}\n\n`;
    }

    finalSQL += `INSERT INTO ${targetTable} (${insertCols})\nSELECT ${selectCols} FROM ${cteNameMap.get(outputNode.id)};`;

    if (this.options.wrapInTransaction) {
      finalSQL += '\n\nCOMMIT;';
    }

    if (this.options.generateExplainPlan) {
      finalSQL += '\n\n' + this.generateExplainPlan();
    }

    this.logger.info('Final SQL built successfully');
    return finalSQL;
  }

  /**
   * Replace occurrences of a node ID with its CTE name in a SQL string.
   */
  private replaceNodeReferences(sql: string, nodeIdToCteName: Map<string, string>): string {
    let result = sql;
    for (const [nodeId, cteName] of nodeIdToCteName.entries()) {
      // Use word boundaries that also work with hyphens
      const regex = new RegExp(`(?<![a-zA-Z0-9_])${this.escapeRegExp(nodeId)}(?![a-zA-Z0-9_])`, 'g');
      result = result.replace(regex, cteName);
    }
    return result;
  }

  /**
   * Escape a string for use in a regular expression.
   */
  private escapeRegExp(str: string): string {
    return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }

  /**
   * Get the target table name for an output node.
   * Falls back to sanitized node name.
   */
private getTargetTableName(node: CanvasNode): string {
  // 1. Use explicitly stored targetTableName (set in handleToolbarRun)
  if (node.metadata?.targetTableName) {
    return node.metadata.targetTableName;
  }
  // 2. Fallback to node.name (sanitized)
  const name = node.name || node.id;
  return name.toLowerCase().replace(/\s+/g, '_').replace(/[^a-z0-9_]/g, '');
}

  // ==================== VALIDATION ====================

  /**
   * Validate entire pipeline
   */
  private async validatePipeline(
    sql: string,
    fragments: Map<string, GeneratedSQLFragment>,
    graph: DependencyGraph
  ): Promise<PipelineValidationResult> {
    const issues: Array<{
      type: 'schema' | 'syntax' | 'performance' | 'dependency';
      message: string;
      severity: 'ERROR' | 'WARNING' | 'INFO';
      location?: string;
    }> = [];

    this.logger.info('Validating pipeline');

    // 1. Validate schema compatibility
    const schemaIssues = this.validateSchemaCompatibility(graph);
    issues.push(...schemaIssues);

    // 2. Validate SQL syntax if enabled
    if (this.options.validateSyntax) {
      const syntaxIssues = await this.validateSQLSyntax(sql);
      issues.push(...syntaxIssues);
    }

    // 3. Check for performance issues
    const performanceIssues = this.validatePerformance(fragments, graph);
    issues.push(...performanceIssues);

    // 4. Validate dependencies
    const dependencyIssues = this.validateDependencies(graph);
    issues.push(...dependencyIssues);

    // Determine overall validity
    const hasErrors = issues.some(issue => issue.severity === 'ERROR');
    const hasSchemaErrors = schemaIssues.some(issue => issue.severity === 'ERROR');
    const hasSyntaxErrors = issues.some(issue => 
      issue.type === 'syntax' && issue.severity === 'ERROR'
    );

    return {
      isValid: !hasErrors,
      schemaCompatible: !hasSchemaErrors,
      syntaxValid: !hasSyntaxErrors,
      performanceAcceptable: performanceIssues.every(issue => issue.severity !== 'ERROR'),
      issues
    };
  }

  /**
   * Validate schema compatibility throughout pipeline
   */
  private validateSchemaCompatibility(_graph: DependencyGraph): Array<{
    type: 'schema';
    message: string;
    severity: 'ERROR' | 'WARNING' | 'INFO';
    location?: string;
  }> {
    const issues: Array<{
      type: 'schema';
      message: string;
      severity: 'ERROR' | 'WARNING' | 'INFO';
      location?: string;
    }> = [];

    // Check each connection for schema compatibility
    for (const connection of this.connections) {
      if (connection.status === ConnectionStatus.INVALID) {
        issues.push({
          type: 'schema',
          message: `Connection ${connection.id} is marked as invalid`,
          severity: 'ERROR',
          location: `connection:${connection.id}`
        });
        continue;
      }

      // Validate data flow
      if (connection.dataFlow.schemaMappings) {
        // Check for unmapped required columns
        // This would require full schema information from nodes
      }
    }

    return issues;
  }

  /**
   * Validate SQL syntax
   */
  private async validateSQLSyntax(sql: string): Promise<Array<{
    type: 'syntax';
    message: string;
    severity: 'ERROR' | 'WARNING' | 'INFO';
    location?: string;
  }>> {
    const issues: Array<{
      type: 'syntax';
      message: string;
      severity: 'ERROR' | 'WARNING' | 'INFO';
      location?: string;
    }> = [];

    // Basic syntax checks
    const lines = sql.split('\n');
    
    // Check for common syntax issues
    let openParens = 0;
    let openQuotes = false;
    
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      const lineNumber = i + 1;
      
      // Check parentheses
      openParens += (line.match(/\(/g) || []).length;
      openParens -= (line.match(/\)/g) || []).length;
      
      // Check quotes
      const singleQuotes = (line.match(/'/g) || []).length;
      if (singleQuotes % 2 !== 0) {
        openQuotes = !openQuotes;
      }
      
      // Check for semicolon in middle of statement
      if (line.trim().endsWith(';') && i < lines.length - 1) {
        const nextLine = lines[i + 1].trim();
        if (nextLine && !nextLine.startsWith('--')) {
          issues.push({
            type: 'syntax',
            message: `Semicolon before end of statement at line ${lineNumber}`,
            severity: 'WARNING',
            location: `line:${lineNumber}`
          });
        }
      }
    }
    
    // Check for unbalanced parentheses
    if (openParens > 0) {
      issues.push({
        type: 'syntax',
        message: `Unclosed parentheses detected (${openParens} open)`,
        severity: 'ERROR'
      });
    }
    
    // Check for unclosed quotes
    if (openQuotes) {
      issues.push({
        type: 'syntax',
        message: 'Unclosed single quote detected',
        severity: 'ERROR'
      });
    }
    
    return issues;
  }

  /**
   * Validate performance characteristics
   */
  private validatePerformance(
    fragments: Map<string, GeneratedSQLFragment>,
    graph: DependencyGraph
  ): Array<{
    type: 'performance';
    message: string;
    severity: 'ERROR' | 'WARNING' | 'INFO';
    location?: string;
  }> {
    const issues: Array<{
      type: 'performance';
      message: string;
      severity: 'ERROR' | 'WARNING' | 'INFO';
      location?: string;
    }> = [];

    // Check for expensive patterns
    for (const [nodeId, fragment] of fragments.entries()) {
      if (fragment.errors.length > 0) continue;
      
      const sql = fragment.sql;
      const node = graph.nodes.get(nodeId)!;
      
      // Check for cartesian products
      if (this.detectCartesianProduct(sql)) {
        issues.push({
          type: 'performance',
          message: `Potential cartesian product detected in node ${node.name}`,
          severity: 'ERROR',
          location: `node:${nodeId}`
        });
      }
      
      // Check for expensive LIKE patterns
      const expensiveLikePatterns = sql.match(/LIKE\s+'%[^']*%[^']*%'/gi);
      if (expensiveLikePatterns) {
        issues.push({
          type: 'performance',
          message: `Expensive LIKE pattern with multiple wildcards in node ${node.name}`,
          severity: 'WARNING',
          location: `node:${nodeId}`
        });
      }
      
      // Check for OR conditions that might benefit from UNION
      const orCount = (sql.match(/\bOR\b/gi) || []).length;
      if (orCount > 5) {
        issues.push({
          type: 'performance',
          message: `Multiple OR conditions (${orCount}) in node ${node.name} may impact performance`,
          severity: 'WARNING',
          location: `node:${nodeId}`
        });
      }
    }
    
    return issues;
  }

  /**
   * Validate dependencies
   */
  private validateDependencies(graph: DependencyGraph): Array<{
    type: 'dependency';
    message: string;
    severity: 'ERROR' | 'WARNING' | 'INFO';
    location?: string;
  }> {
    const issues: Array<{
      type: 'dependency';
      message: string;
      severity: 'ERROR' | 'WARNING' | 'INFO';
      location?: string;
    }> = [];

    // Check for nodes with no outgoing connections (sinks)
    for (const [nodeId, outdegree] of graph.outdegree.entries()) {
      if (outdegree === 0) {
        const node = graph.nodes.get(nodeId)!;
        if (!this.isOutputNode(node)) {
          issues.push({
            type: 'dependency',
            message: `Node ${node.name} has no outgoing connections (potential dead end)`,
            severity: 'WARNING',
            location: `node:${nodeId}`
          });
        }
      }
    }
    
    // Check for nodes with no incoming connections (sources)
    for (const [nodeId, indegree] of graph.indegree.entries()) {
      if (indegree === 0) {
        const node = graph.nodes.get(nodeId)!;
        if (!this.isInputNode(node)) {
          issues.push({
            type: 'dependency',
            message: `Node ${node.name} has no incoming connections (potential source node)`,
            severity: 'INFO',
            location: `node:${nodeId}`
          });
        }
      }
    }
    
    return issues;
  }

  // ==================== HELPER METHODS ====================

  /**
   * Update progress callback if provided
   */
  private updateProgress(
    stage: PipelineProgress['stage'],
    progress: number,
    message: string,
    stats?: Partial<PipelineProgress['stats']>
  ): void {
    if (this.options.progressCallback) {
      const progressUpdate: PipelineProgress = {
        stage,
        progress: Math.min(100, Math.max(0, progress)),
        message,
        stats: stats ? {
          nodesProcessed: 0,
          totalNodes: this.nodes.length,
          fragmentsGenerated: 0,
          errors: 0,
          warnings: 0,
          ...stats
        } : undefined
      };
      
      this.options.progressCallback(progressUpdate);
    }
  }

  /**
   * Generate CTE name from node
   */
  private generateCTEName(node: CanvasNode): string {
    return `cte_${node.id.replace(/[^a-zA-Z0-9]/g, '_')}_${node.name
      .toLowerCase()
      .replace(/\s+/g, '_')
      .replace(/[^a-z0-9_]/g, '')
      .slice(0, 20)}`;
  }

  /**
   * Generate temporary table name
   */
  private generateTempTableName(node: CanvasNode): string {
    return `tmp_${node.id.replace(/[^a-zA-Z0-9]/g, '_')}`;
  }

  /**
   * Determine if CTE should be materialized
   */
  private shouldMaterializeCTE(node: CanvasNode, _context: PipelineContext): boolean {
    // Materialize if explicitly requested
    if (this.options.materializeIntermediate) return true;
    
    // Materialize if node is complex
    const complexity = this.estimateNodeComplexity(node);
    return complexity > 50; // Arbitrary threshold
  }

  /**
   * Estimate node complexity
   */
  private estimateNodeComplexity(node: CanvasNode): number {
    let complexity = 0;
    
    // Base complexity by node type
    switch (node.type) {
      case NodeType.JOIN:
        complexity = 70;
        break;
      case NodeType.MAP:
        complexity = 40;
        break;
      case NodeType.AGGREGATE_ROW:
        complexity = 60;
        break;
      case NodeType.FILTER_ROW:
        complexity = 20;
        break;
      default:
        complexity = 30;
    }
    
    // Increase complexity based on metadata
    if (node.metadata?.transformationRules?.length) {
      complexity += node.metadata.transformationRules.length * 5;
    }
    
    if (node.metadata?.schemaMappings?.length) {
      complexity += node.metadata.schemaMappings.length * 3;
    }
    
    return complexity;
  }

  /**
   * Estimate SQL complexity
   */
  private estimateSQLComplexity(sql: string): number {
    if (!sql) return 0;
    
    let complexity = 0;
    
    // Count joins
    complexity += (sql.match(/\bJOIN\b/gi) || []).length * 20;
    
    // Count subqueries
    complexity += (sql.match(/\(\s*SELECT/gi) || []).length * 15;
    
    // Count aggregate functions
    complexity += (sql.match(/\b(SUM|AVG|COUNT|MIN|MAX|STDDEV|VARIANCE)\s*\(/gi) || []).length * 10;
    
    // Count WHERE conditions
    const whereMatch = sql.match(/WHERE\s+(.+?)(?=\b(?:GROUP BY|ORDER BY|LIMIT|$))/i);
    if (whereMatch) {
      const whereClause = whereMatch[1];
      complexity += (whereClause.match(/\bAND\b/gi) || []).length * 5;
      complexity += (whereClause.match(/\bOR\b/gi) || []).length * 8;
    }
    
    return complexity;
  }

  /**
   * Estimate row count for node
   */
  private estimateRowCount(node: CanvasNode): number {
    // Simple heuristic based on node type
    // Use string literals for INPUT since NodeType.INPUT might not be in the union
    if (this.isInputNode(node)) {
      return 10000; // Assume input nodes have lots of data
    }
    
    switch (node.type) {
      case NodeType.FILTER_ROW:
        return 5000;  // Filters reduce data
      case NodeType.AGGREGATE_ROW:
        return 100;   // Aggregates significantly reduce rows
      case NodeType.JOIN:
        return 20000; // Joins can increase rows
      default:
        return 1000;
    }
  }

  /**
   * Detect cartesian product in SQL
   */
  private detectCartesianProduct(sql: string): boolean {
    // Look for FROM clause with multiple tables but no JOIN conditions
    const fromMatch = sql.match(/FROM\s+([^(\s]+(?:\s*,\s*[^(\s]+)+)/i);
    if (fromMatch) {
      const tables = fromMatch[1].split(',').map(t => t.trim());
      if (tables.length > 1) {
        // Check if there's a WHERE clause connecting the tables
        const whereMatch = sql.match(/WHERE\s+(.+)/i);
        if (!whereMatch) {
          return true; // No WHERE clause with multiple tables = cartesian product
        }
        
        // Check if WHERE clause contains joins between tables
        const whereClause = whereMatch[1];
        const hasJoinCondition = tables.some((table1, i) => 
          tables.slice(i + 1).some(table2 => 
            whereClause.includes(`${table1}.`) && whereClause.includes(`${table2}.`)
          )
        );
        
        return !hasJoinCondition;
      }
    }
    
    return false;
  }

  /**
   * Extract WHERE clause from SQL
   */
  private extractWhereClause(sql: string): string | null {
    const whereMatch = sql.match(/WHERE\s+(.+?)(?=\b(?:GROUP BY|ORDER BY|HAVING|LIMIT|$))/i);
    return whereMatch ? whereMatch[1].trim() : null;
  }

  /**
   * Extract referenced columns from SQL
   */
  private extractReferencedColumns(sql: string): string[] {
    const columns: string[] = [];
    const columnPattern = /\b([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\b/g;
    
    let match: RegExpExecArray | null;
    while ((match = columnPattern.exec(sql)) !== null) {
      columns.push(match[0]); // Full column reference with table
    }
    
    return columns;
  }

  /**
   * Generate pipeline header comments
   */
  private generatePipelineHeader(): string {
    return [
      '/*',
      ' * PostgreSQL SQL Pipeline',
      ` * Generated: ${new Date().toISOString()}`,
      ` * Nodes: ${this.nodes.length}`,
      ` * Connections: ${this.connections.length}`,
      ` * PostgreSQL Version: ${this.options.postgresVersion}`,
      ' *',
      ' * Generated by SQLGenerationPipeline',
      ' */\n\n'
    ].join('\n');
  }

  /**
   * Generate EXPLAIN plan for pipeline
   */
  private generateExplainPlan(): string {
    return [
      '/*',
      ' * EXECUTION PLAN HINTS:',
      ' * 1. Consider adding indexes on frequently joined columns',
      ' * 2. Monitor memory usage for materialized CTEs',
      ' * 3. Use appropriate work_mem settings for complex operations',
      ' * 4. Consider partitioning for large datasets',
      ' *',
      ' * To analyze performance:',
      ' * EXPLAIN ANALYZE <query>;',
      ' *',
      ' * For memory optimization:',
      ' * SET work_mem = "64MB"; -- Adjust based on complexity',
      ' */'
    ].join('\n');
  }

  /**
   * Format SQL with proper indentation
   */
  private formatSQL(sql: string): string {
    // Basic SQL formatting with indentation
    const lines = sql.split('\n');
    const formattedLines: string[] = [];
    let indentLevel = 0;
    const indentSize = 2;
    
    for (const line of lines) {
      const trimmed = line.trim();
      
      if (!trimmed) {
        formattedLines.push('');
        continue;
      }
      
      // Decrease indent for closing parentheses and keywords
      if (trimmed.startsWith(')') || 
          trimmed.startsWith('END') ||
          trimmed.startsWith('ELSE') ||
          trimmed.match(/^\s*(FROM|WHERE|GROUP BY|HAVING|ORDER BY|LIMIT|OFFSET)/i)) {
        indentLevel = Math.max(0, indentLevel - 1);
      }
      
      // Add indentation
      const indent = ' '.repeat(indentLevel * indentSize);
      formattedLines.push(indent + trimmed);
      
      // Increase indent for opening parentheses and keywords
      if (trimmed.endsWith('(') ||
          trimmed.match(/^\s*(SELECT|FROM|WHERE|GROUP BY|HAVING|ORDER BY|LIMIT|OFFSET)/i) ||
          trimmed.startsWith('CASE') ||
          trimmed.startsWith('WHEN')) {
        indentLevel++;
      }
    }
    
    return formattedLines.join('\n');
  }

  /**
   * Indent text by specified number of spaces
   */
  private indent(text: string, spaces: number): string {
    const indent = ' '.repeat(spaces);
    return text.split('\n').map(line => indent + line).join('\n');
  }

  /**
   * Get current memory usage (simplified)
   */
  private getMemoryUsage(): number {
    // Simplified memory estimation
    if (typeof process !== 'undefined' && process.memoryUsage) {
      return Math.round(process.memoryUsage().heapUsed / 1024 / 1024);
    }
    return 0;
  }

  /**
   * Cleanup memoization cache (LRU strategy)
   */
  private cleanupMemoizationCache(): void {
    const maxCacheSize = 50;
    
    if (this.memoizationCache.size > maxCacheSize) {
      // Remove oldest entries (simple LRU)
      const keys = Array.from(this.memoizationCache.keys());
      const keysToRemove = keys.slice(0, this.memoizationCache.size - maxCacheSize);
      
      keysToRemove.forEach(key => {
        this.memoizationCache.delete(key);
      });
      
      this.logger.info(`Cleaned up ${keysToRemove.length} cache entries`);
    }
  }
}

// ==================== USAGE EXAMPLE ====================

/**
 * Example usage of SQLGenerationPipeline
 */
export async function generatePipelineSQL(
  nodes: CanvasNode[],
  connections: CanvasConnection[],
  options?: Partial<PipelineGenerationOptions>
): Promise<PipelineGenerationResult> {
  const pipeline = new SQLGenerationPipeline(nodes, connections, options);
  
  // Add progress callback if needed
  if (options?.progressCallback) {
    const originalCallback = options.progressCallback;
    options.progressCallback = (progress) => {
      console.log(`[${progress.stage}] ${progress.progress}% - ${progress.message}`);
      originalCallback(progress);
    };
  }
  
  return pipeline.generate();
}

/**
 * Generate pipeline SQL with caching
 */
export async function generatePipelineSQLWithCache(
  pipelineId: string,
  nodes: CanvasNode[],
  connections: CanvasConnection[],
  options?: Partial<PipelineGenerationOptions>
): Promise<PipelineGenerationResult> {
  const pipeline = new SQLGenerationPipeline(nodes, connections, options);
  const cacheKey = `${pipelineId}:${JSON.stringify(options)}`;
  return pipeline.generateWithCache(cacheKey);
}