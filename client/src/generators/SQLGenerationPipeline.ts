// src/generators/SQLGenerationPipeline.ts

import {
  CanvasNode,
  CanvasConnection,
  ConnectionStatus,
  PostgreSQLDataType,
} from '../types/pipeline-types';
import {
  BaseSQLGenerator,
  SQLGenerationContext,
  GeneratedSQLFragment,
} from './BaseSQLGenerator';
import { SQLGeneratorFactory } from './SQLGeneratorFactory';
import { OutputSQLGenerator } from './OutputSQLGenerator';

// Import unified types for adapter conversion
import {
  UnifiedCanvasNode,
  UnifiedCanvasConnection,
  NodeType,
} from '../types/unified-pipeline.types';
import { globalLogger } from '../utils/Logger';

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
  stage:
    | 'initializing'
    | 'graph_construction'
    | 'topological_sort'
    | 'planning'
    | 'generation'
    | 'optimization'
    | 'validation'
    | 'formatting';
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
  private logs: Array<{
    timestamp: Date;
    level: string;
    message: string;
    data?: any;
  }> = [];

  constructor(level: string) {
    this.logLevel = level;
  }

  error(message: string, data?: any) {
    this.log('error', message, data);
    globalLogger.error(`[Pipeline] ${message}`, data);
  }

  warn(message: string, data?: any) {
    if (['warn', 'info', 'debug'].includes(this.logLevel)) {
      this.log('warn', message, data);
      globalLogger.warn(`[Pipeline] ${message}`, data);
    }
  }

  info(message: string, data?: any) {
    if (['info', 'debug'].includes(this.logLevel)) {
      this.log('info', message, data);
      globalLogger.info(`[Pipeline] ${message}`, data);
    }
  }

  debug(message: string, data?: any) {
    if (this.logLevel === 'debug') {
      this.log('debug', message, data);
      globalLogger.debug(`[Pipeline] ${message}`, data);
    }
  }

  private log(level: string, message: string, data?: any) {
    const entry = { timestamp: new Date(), level, message, data };
    this.logs.push(entry);
  }

  getLogs(): Array<{
    timestamp: Date;
    level: string;
    message: string;
    data?: any;
  }> {
    return this.logs;
  }

  clear() {
    this.logs = [];
  }
}

// ==================== MAIN PIPELINE CLASS ====================

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
      useCTEs: false,
      materializeIntermediate: false,
      wrapInTransaction: false,
      validateSyntax: true,
      generateExplainPlan: true,
      logLevel: 'info',
      ...options,
    };
    this.logger = new PipelineLogger(this.options.logLevel);
    globalLogger.info(`SQLGenerationPipeline initialized with options`, this.options);
  }

  // ==================== PUBLIC API ====================

  public async generate(): Promise<PipelineGenerationResult> {
    this.generationStartTime = Date.now();
    this.logger.info('Starting pipeline generation');

    console.log('=== PIPELINE GENERATION INPUT ===');
    console.log('Nodes:', this.nodes);
    console.log('Connections:', this.connections);
    console.log('Options:', this.options);

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
        estimatedPerformanceGain: 0,
      },
      warnings: [],
      errors: [],
      validationResult: {
        isValid: false,
        schemaCompatible: false,
        syntaxValid: false,
        performanceAcceptable: false,
        issues: [],
      },
      metadata: {
        totalNodes: this.nodes.length,
        totalConnections: this.connections.length,
        generationTimeMs: 0,
        memoryUsageMb: 0,
        formatted: this.options.formatSQL,
      },
    };

    try {
      this.updateProgress('initializing', 0, 'Initializing pipeline generation');

      this.updateProgress('graph_construction', 10, 'Building dependency graph');
      const dependencyGraph = this.buildDependencyGraph();

      this.updateProgress('graph_construction', 20, 'Validating DAG structure');
      const cycles = this.detectCycles(dependencyGraph);
      if (cycles.length > 0) {
        this.logger.error(`Pipeline contains circular dependencies: ${JSON.stringify(cycles)}`);
        throw new Error(
          `Pipeline contains circular dependencies: ${JSON.stringify(cycles)}`
        );
      }

      this.validateUserDefinedOutputNames(dependencyGraph);

      this.updateProgress('topological_sort', 30, 'Performing topological sort');
      const topologicalOrder = this.topologicalSort(dependencyGraph);

      this.updateProgress('planning', 40, 'Detecting parallel execution groups');
      const parallelGroups = this.detectParallelGroups(dependencyGraph, topologicalOrder);

      this.updateProgress('planning', 50, 'Creating execution plan');
      const executionPlan = this.createExecutionPlan(
        dependencyGraph,
        topologicalOrder,
        parallelGroups
      );
      result.executionPlan = executionPlan;

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
          optimizationApplied: 0,
        },
      };

      this.updateProgress('generation', 60, 'Generating SQL fragments');
      const fragments = await this.generatePipelineFragments(
        dependencyGraph,
        topologicalOrder,
        context
      );
      result.fragments = fragments;

      if (this.options.optimize) {
        this.updateProgress('optimization', 70, 'Applying optimizations');
        const optimizationResult = await this.applyOptimizations(
          fragments,
          dependencyGraph,
          context
        );
        result.optimizationSummary = optimizationResult.summary;
        result.warnings.push(...optimizationResult.warnings);
      }

      this.updateProgress('generation', 80, 'Building final SQL');
      result.sql = await this.buildFinalSQL(
        fragments,
        dependencyGraph,
        topologicalOrder,
        context
      );

      this.updateProgress('validation', 90, 'Validating pipeline');
      result.validationResult = await this.validatePipeline(
        result.sql,
        fragments,
        dependencyGraph
      );

      if (this.options.formatSQL) {
        this.updateProgress('formatting', 95, 'Formatting SQL');
        result.sql = this.formatSQL(result.sql);
      }

      result.warnings.push(...context.warnings);
      result.errors.push(...context.errors);

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
        recoverable: false,
      });

      result.validationResult.isValid = false;
      result.metadata.generationTimeMs = Date.now() - this.generationStartTime;

      return result;
    }
  }

  public async generateWithCache(cacheKey: string): Promise<PipelineGenerationResult> {
    const cacheHit = this.memoizationCache.get(cacheKey);
    if (cacheHit && this.isCacheValid(cacheHit)) {
      this.logger.info(`Cache hit for key: ${cacheKey}`);
      return cacheHit;
    }

    this.logger.info(`Cache miss for key: ${cacheKey}, generating fresh`);
    const result = await this.generate();

    this.memoizationCache.set(cacheKey, result);
    this.cleanupMemoizationCache();

    return result;
  }

  public getStats(): {
    cacheSize: number;
    generationTime: number;
    nodesProcessed: number;
  } {
    return {
      cacheSize: this.memoizationCache.size,
      generationTime: this.generationStartTime ? Date.now() - this.generationStartTime : 0,
      nodesProcessed: this.nodes.length,
    };
  }

  public clearCache(): void {
    this.memoizationCache.clear();
    this.logger.info('Memoization cache cleared');
  }

  // ==================== TYPE ADAPTERS ====================

  private toUnifiedNode(node: CanvasNode): UnifiedCanvasNode {
    const nodeType = Object.values(NodeType).includes(node.type as NodeType)
      ? (node.type as NodeType)
      : NodeType.UNKNOWN;

    const metadata = node.metadata || {};
    const configuration = (metadata as any).configuration || { type: 'OTHER', config: {} };

    const unifiedMetadata: UnifiedCanvasNode['metadata'] = {
      configuration,
      description: metadata.description,
      tags: metadata.tags,
      version: metadata.version,
      createdBy: metadata.createdBy,
      createdAt: metadata.createdAt,
      updatedAt: metadata.updatedAt,
      ...metadata,
    };

    return {
      id: node.id,
      name: node.name,
      type: nodeType,
      position: node.position || { x: 0, y: 0 },
      size: node.size || { width: 200, height: 100 },
      connectionPorts: node.connectionPorts,
      metadata: unifiedMetadata,
      status: node.status,
      draggable: node.draggable,
      droppable: node.droppable,
      dragType: node.dragType,
      technology: node.technology,
      schemaName: node.schemaName,
      tableName: node.tableName,
      fileName: node.fileName,
      sheetName: node.sheetName,
      visualProperties: node.visualProperties,
    };
  }

  private toUnifiedConnection(conn: CanvasConnection): UnifiedCanvasConnection {
    const connMetadata = conn.metadata || {};
    const createdAt = connMetadata.createdAt || new Date().toISOString();

    return {
      id: conn.id,
      sourceNodeId: conn.sourceNodeId,
      sourcePortId: conn.sourcePortId,
      targetNodeId: conn.targetNodeId,
      targetPortId: conn.targetPortId,
      dataFlow: conn.dataFlow || { schemaMappings: [] },
      status: (conn.status as ConnectionStatus) || ConnectionStatus.UNVALIDATED,
      errors: conn.errors,
      metrics: conn.metrics,
      metadata: {
        description: connMetadata.description,
        createdBy: connMetadata.createdBy,
        createdAt,
        updatedAt: connMetadata.updatedAt,
      },
    };
  }

  // ==================== HELPER METHODS FOR TYPE CHECKS ====================

  private isOutputNode(node: CanvasNode): boolean {
    return node.type === 'output';
  }

  private isInputNode(node: CanvasNode): boolean {
    return node.type === 'input';
  }

  private isAutoGeneratedName(name: string): boolean {
    const autoPatterns = [
      /^node-\d+-\w+$/,
      /^t[A-Z]\w+_\d+$/,
      /^New\s+(?:Job|Folder|Item)/i,
      /^output_\d+$/,
      /^.*_OUTPUT_\d+$/i,
      /^cte_[a-f0-9]+_/i,
      /^tmp_[a-f0-9]+_/i,
    ];
    return autoPatterns.some((pattern) => pattern.test(name));
  }

  private validateUserDefinedOutputNames(graph: DependencyGraph): void {
    const outputNodes = Array.from(graph.nodes.values()).filter((node) =>
      this.isOutputNode(node)
    );
    const invalidNodes = outputNodes.filter((node) => this.isAutoGeneratedName(node.name));

    if (invalidNodes.length > 0) {
      const nodeList = invalidNodes
        .map((n) => `"${n.name}" (id: ${n.id})`)
        .join(', ');
      this.logger.error(`Output nodes with auto-generated names: ${nodeList}`);
      throw new Error(
        `Output nodes with auto‑generated names are not allowed for SQL generation. ` +
          `Please rename the following node(s) to a meaningful name: ${nodeList}`
      );
    }
  }

  // ==================== DAG CONSTRUCTION & ANALYSIS ====================

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
      nodeConnections: new Map(),
    };

    this.nodes.forEach((node) => {
      graph.nodes.set(node.id, node);
      graph.adjacencyList.set(node.id, new Set());
      graph.reverseAdjacencyList.set(node.id, new Set());
      graph.indegree.set(node.id, 0);
      graph.outdegree.set(node.id, 0);
      graph.nodeConnections.set(node.id, []);
    });

    this.connections.forEach((connection) => {
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

      graph.adjacencyList.get(sourceNode.id)!.add(targetNode.id);
      graph.reverseAdjacencyList.get(targetNode.id)!.add(sourceNode.id);

      graph.outdegree.set(sourceNode.id, graph.outdegree.get(sourceNode.id)! + 1);
      graph.indegree.set(targetNode.id, graph.indegree.get(targetNode.id)! + 1);

      graph.connectionMap.set(connection.id, connection);
      graph.nodeConnections.get(sourceNode.id)!.push(connection.id);
      graph.nodeConnections.get(targetNode.id)!.push(connection.id);
    });

    this.calculateNodeLevels(graph);

    const elapsed = Date.now() - startTime;
    this.logger.debug(`Dependency graph built in ${elapsed}ms`);
    this.logger.info(
      `Graph stats: ${graph.nodes.size} nodes, ${this.connections.length} connections`
    );

    this.logger.debug('Graph adjacency list:', Array.from(graph.adjacencyList.entries()).map(([k, v]) => ({ [k]: Array.from(v) })));

    return graph;
  }

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

  private topologicalSort(graph: DependencyGraph): string[] {
    const startTime = Date.now();
    this.logger.debug('Performing topological sort');

    const indegree = new Map(graph.indegree);
    const adjacencyList = new Map(
      Array.from(graph.adjacencyList.entries()).map(([k, v]) => [k, new Set(v)])
    );

    const queue: string[] = [];
    const result: string[] = [];

    for (const [nodeId, degree] of indegree.entries()) {
      if (degree === 0) {
        queue.push(nodeId);
      }
    }

    this.logger.debug(`Initial queue (nodes with indegree 0): ${queue.join(', ')}`);

    while (queue.length > 0) {
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

    if (result.length !== graph.nodes.size) {
      this.logger.error('Topological sort failed - graph may have cycles');
      throw new Error('Pipeline contains circular dependencies');
    }

    const elapsed = Date.now() - startTime;
    this.logger.debug(`Topological sort completed in ${elapsed}ms`);
    this.logger.info(`Topological order: ${result.join(' -> ')}`);

    return result;
  }

  private calculateNodeLevels(graph: DependencyGraph): void {
    const visited = new Set<string>();
    const queue: Array<[string, number]> = [];

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
          const currentLevel = graph.nodeLevels.get(neighbor) || 0;
          if (level + 1 > currentLevel) {
            graph.nodeLevels.set(neighbor, level + 1);
          }
        }
      }
    }

    for (const nodeId of graph.nodes.keys()) {
      if (!graph.nodeLevels.has(nodeId)) {
        graph.nodeLevels.set(nodeId, -1);
      }
    }
  }

  private detectParallelGroups(
    graph: DependencyGraph,
    topologicalOrder: string[]
  ): string[][] {
    const startTime = Date.now();
    this.logger.debug('Detecting parallel execution groups');

    const levelGroups = new Map<number, string[]>();

    for (const nodeId of topologicalOrder) {
      const level = graph.nodeLevels.get(nodeId) || 0;
      if (!levelGroups.has(level)) {
        levelGroups.set(level, []);
      }
      levelGroups.get(level)!.push(nodeId);
    }

    const groups = Array.from(levelGroups.entries())
      .sort(([levelA], [levelB]) => levelA - levelB)
      .map(([, nodes]) => nodes);

    const refinedGroups: string[][] = [];

    for (const group of groups) {
      if (group.length <= 1) {
        refinedGroups.push(group);
        continue;
      }

      const independentSubgroups: string[][] = [];
      const visitedInGroup = new Set<string>();

      for (const nodeId of group) {
        if (visitedInGroup.has(nodeId)) continue;

        const subgroup = [nodeId];
        visitedInGroup.add(nodeId);

        for (const otherNodeId of group) {
          if (otherNodeId === nodeId || visitedInGroup.has(otherNodeId)) continue;

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

    refinedGroups.forEach((group, index) => {
      if (group.length > 1) {
        this.logger.info(`Parallel group ${index}: ${group.join(', ')}`);
      }
    });

    return refinedGroups;
  }

  private checkDependency(graph: DependencyGraph, nodeA: string, nodeB: string): boolean {
    if (graph.adjacencyList.get(nodeA)?.has(nodeB)) {
      return true;
    }

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

  private createExecutionPlan(
    graph: DependencyGraph,
    topologicalOrder: string[],
    parallelGroups: string[][]
  ): ExecutionPlan {
    this.logger.debug('Creating execution plan');

    const criticalPath = this.calculateCriticalPath(graph, topologicalOrder);
    const estimatedCost = this.estimateExecutionCost(graph, topologicalOrder);
    const memoryFootprint = this.estimateMemoryFootprint(graph);
    const timingEstimates = this.estimateTiming(graph, topologicalOrder, parallelGroups);
    const optimizationOpportunities = this.findOptimizationOpportunities(graph);

    return {
      topologicalOrder,
      parallelGroups,
      criticalPath,
      estimatedCost,
      memoryFootprint,
      timingEstimates,
      optimizationOpportunities,
      dependencies: this.extractDependencyMap(graph),
    };
  }

  private calculateCriticalPath(graph: DependencyGraph, _topologicalOrder: string[]): string[] {
    const maxLevel = Math.max(
      ...Array.from(graph.nodeLevels.values()).filter((l) => l >= 0)
    );

    const criticalNodes: string[] = [];
    for (const [nodeId, level] of graph.nodeLevels.entries()) {
      if (level === maxLevel) {
        criticalNodes.push(nodeId);
      }
    }

    return criticalNodes.length > 0 ? [criticalNodes[0]] : [];
  }

  private estimateExecutionCost(graph: DependencyGraph, topologicalOrder: string[]): number {
    let cost = 0;

    for (const nodeId of topologicalOrder) {
      const node = graph.nodes.get(nodeId)!;
      const connections = graph.nodeConnections.get(nodeId) || [];

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

      cost += connections.length * 5;
      const level = graph.nodeLevels.get(nodeId) || 0;
      cost += level * 20;
    }

    return cost;
  }

  private estimateMemoryFootprint(graph: DependencyGraph): {
    estimated: number;
    peak: number;
  } {
    let estimated = 0;
    let peak = 0;
    let current = 0;

    for (const [nodeId] of graph.indegree.entries()) {
      const node = graph.nodes.get(nodeId)!;
      const columnCount = node.metadata?.tableMapping?.columns?.length || 10;

      const nodeMemory = columnCount * 1024;
      current += nodeMemory;
      peak = Math.max(peak, current);

      const outdegree = graph.outdegree.get(nodeId) || 0;
      if (outdegree === 0) {
        current -= nodeMemory;
      }

      estimated += nodeMemory;
    }

    return {
      estimated,
      peak: Math.max(estimated / 4, peak),
    };
  }

  private estimateTiming(
    graph: DependencyGraph,
    topologicalOrder: string[],
    parallelGroups: string[][]
  ): { sequential: number; parallel: number; recommendedParallelism: number } {
    let sequential = 0;
    for (const nodeId of topologicalOrder) {
      const node = graph.nodes.get(nodeId)!;
      sequential += this.estimateNodeExecutionTime(node);
    }

    let parallel = 0;
    for (const group of parallelGroups) {
      let groupTime = 0;
      for (const nodeId of group) {
        const node = graph.nodes.get(nodeId)!;
        groupTime = Math.max(groupTime, this.estimateNodeExecutionTime(node));
      }
      parallel += groupTime;
    }

    const totalWork = sequential;
    const recommendedParallelism = Math.min(
      this.options.maxParallelDepth,
      Math.ceil(totalWork / parallel)
    );

    return {
      sequential: Math.round(sequential),
      parallel: Math.round(parallel),
      recommendedParallelism,
    };
  }

  private estimateNodeExecutionTime(node: CanvasNode): number {
    switch (node.type) {
      case NodeType.JOIN:
        return 500;
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
        potentialImprovement: Math.min(30, filterCount * 10),
      });
    }

    for (const [nodeId, indegree] of graph.indegree.entries()) {
      const node = graph.nodes.get(nodeId)!;
      if (node.type === NodeType.JOIN && indegree === 2) {
        const neighbors = graph.adjacencyList.get(nodeId)?.size || 0;
        if (neighbors === 1) {
          opportunities.push({
            type: 'JOIN_TO_SUBQUERY',
            description: `Join node ${nodeId} used only once, could be a subquery`,
            potentialImprovement: 15,
          });
        }
      }
    }

    for (const [nodeId, outdegree] of graph.outdegree.entries()) {
      if (outdegree > 3) {
        opportunities.push({
          type: 'MATERIALIZATION',
          description: `Node ${nodeId} has ${outdegree} dependents, could benefit from materialization`,
          potentialImprovement: 25,
        });
      }
    }

    return opportunities;
  }

  private extractDependencyMap(graph: DependencyGraph): Record<string, string[]> {
    const dependencies: Record<string, string[]> = {};

    for (const [nodeId, incoming] of graph.reverseAdjacencyList.entries()) {
      dependencies[nodeId] = Array.from(incoming);
    }

    return dependencies;
  }

  // ==================== SQL GENERATION ====================

// src/generators/SQLGenerationPipeline.ts

private async generatePipelineFragments(
  graph: DependencyGraph,
  topologicalOrder: string[],
  context: PipelineContext
): Promise<Map<string, GeneratedSQLFragment>> {
  const fragments = new Map<string, GeneratedSQLFragment>();
  const totalNodes = topologicalOrder.length;

  this.logger.info(`Generating SQL fragments for ${totalNodes} nodes`);

  // Pre‑compute CTE alias map from the pipeline context
  const cteAliasMap = new Map<string, string>();
  for (const [nodeId, entry] of context.cteChain.entries()) {
    cteAliasMap.set(nodeId, entry.cteName);
  }

  // Pre‑compute a stable alias for every node (used for derived table references)
  const nodeAliasMap = new Map<string, string>();
  for (const nodeId of topologicalOrder) {
    const alias = `cte_${nodeId.replace(/[^a-zA-Z0-9]/g, '_')}`;
    nodeAliasMap.set(nodeId, alias);
  }

  for (let i = 0; i < topologicalOrder.length; i++) {
    const nodeId = topologicalOrder[i];
    const node = graph.nodes.get(nodeId)!;

    this.updateProgress(
      'generation',
      60 + (i / totalNodes) * 20,
      `Generating SQL for node: ${node.name} (${node.type})`
    );

    this.logger.debug(`Processing node ${nodeId} (${node.type}): ${node.name}`);

    let generator = context.nodeGenerators.get(node.type);
    if (!generator) {
      generator = SQLGeneratorFactory.createGenerator(node.type, {
        postgresVersion: this.options.postgresVersion,
        includeComments: this.options.includeComments,
        formatSQL: this.options.formatSQL,
      });

      if (!generator) {
        const errorMsg = `No SQL generator available for node type: ${node.type}`;
        this.logger.error(errorMsg);
        context.errors.push({
          code: 'UNSUPPORTED_NODE_TYPE',
          message: errorMsg,
          nodeId: node.id,
          recoverable: false,
        });
        continue;
      }

      context.nodeGenerators.set(node.type, generator);
    }

    const incomingNodeIds = Array.from(graph.reverseAdjacencyList.get(nodeId) || []);
    const incomingConnections = this.connections.filter(
      (conn) => incomingNodeIds.includes(conn.sourceNodeId) && conn.targetNodeId === nodeId
    );

    // ==================== NEW: Determine active output port for conditional split ====================
    const outgoingConnections = this.connections.filter(
      (conn) => conn.sourceNodeId === nodeId && graph.nodes.has(conn.targetNodeId)
    );

    let activeOutputPort: string | undefined;
    if (node.type === NodeType.CONDITIONAL_SPLIT) {
      if (outgoingConnections.length === 1) {
        activeOutputPort = outgoingConnections[0].targetPortId;
        this.logger.debug(
          `Conditional split ${nodeId}: active output port = ${activeOutputPort}`
        );
      } else {
        this.logger.warn(
          `Conditional split ${nodeId} has ${outgoingConnections.length} outgoing edges; cannot determine active branch.`
        );
      }
    }
    // =================================================================================================

    const upstreamSchema = this.calculateUpstreamSchema(nodeId, graph, context);

    const unifiedNode = this.toUnifiedNode(node);
    const unifiedConnection = incomingConnections[0]
      ? this.toUnifiedConnection(incomingConnections[0])
      : undefined;

    const generationContext: SQLGenerationContext = {
      node: unifiedNode,
      connection: unifiedConnection,
      indentLevel: graph.nodeLevels.get(nodeId) || 0,
      parameters: new Map(),
      upstreamSchema,
      cteAliasMap,
      incomingNodeIds,
      nodeAliasMap,
      activeOutputPort,   // <-- Pass the branch hint
      options: {
        includeComments: this.options.includeComments,
        formatSQL: this.options.formatSQL,
        targetDialect: 'POSTGRESQL',
        postgresVersion: this.options.postgresVersion,
        useCTEs: this.options.useCTEs,
        optimizeForReadability: true,
        includeExecutionPlan: this.options.generateExplainPlan,
        parameterizeValues: true,
        maxLineLength: 80,
      },
    };

    const fragment = await this.generateNodeFragment(
      unifiedNode,
      generator,
      generationContext,
      upstreamSchema,
      context
    );

    this.logger.debug(`Fragment for node ${nodeId} (${node.type}):`, fragment.sql);

    fragments.set(nodeId, fragment);
    context.fragmentCache.set(nodeId, fragment);
    context.stats.fragmentsGenerated++;

    context.schemaCache.set(nodeId, this.extractOutputSchema(fragment, node, upstreamSchema));

    // Forward fragment errors and warnings to pipeline context
    if (fragment.errors.length > 0) {
      fragment.errors.forEach((err) => {
        context.errors.push({
          code: err.code,
          message: err.message,
          nodeId: node.id,
          recoverable: err.severity !== 'ERROR',
          recoverySuggestion: err.suggestion,
        });
      });
    }

    if (fragment.warnings.length > 0) {
      fragment.warnings.forEach((warn) => {
        context.warnings.push({
          code: 'FRAGMENT_WARNING',
          message: warn,
          nodeId: node.id,
          severity: 'LOW',
        });
      });
    }

    this.logger.debug(`Generated fragment for node ${nodeId} (${node.name})`);
  }

  this.logger.info(`Generated ${fragments.size} SQL fragments`);
  return fragments;
}
private async generateNodeFragment(
    node: UnifiedCanvasNode,
    generator: BaseSQLGenerator,
    context: SQLGenerationContext,
    upstreamSchema: Array<{ name: string; dataType: PostgreSQLDataType }>,
    pipelineContext: PipelineContext
  ): Promise<GeneratedSQLFragment> {
    const cacheKey = this.createFragmentCacheKey(node, upstreamSchema);

    const cachedFragment = pipelineContext.fragmentCache.get(cacheKey);
    if (cachedFragment) {
      pipelineContext.stats.cacheHits++;
      pipelineContext.logger.debug(`Cache hit for fragment: ${cacheKey}`);
      return cachedFragment;
    }

    pipelineContext.stats.cacheMisses++;
    pipelineContext.logger.debug(`Generating new fragment: ${cacheKey}`);

    try {
      const fragment = generator.generateSQL(context);

      if (this.options.useCTEs) {
        const cteEntry: CTEChainEntry = {
          nodeId: node.id,
          cteName: this.generateCTENameFromUnified(node),
          sql: fragment.sql,
          materialized: this.shouldMaterializeCTEFromUnified(node, pipelineContext),
          dependencies: fragment.dependencies,
          columns: upstreamSchema,
          estimatedRows: this.estimateRowCountFromUnified(node),
          shouldMaterialize: this.shouldMaterializeCTEFromUnified(node, pipelineContext),
        };

        pipelineContext.cteChain.set(node.id, cteEntry);
      }

      pipelineContext.fragmentCache.set(cacheKey, fragment);

      return fragment;
    } catch (error) {
      pipelineContext.logger.error(`Failed to generate fragment for node ${node.id}`, error);

      return {
        sql: '',
        dependencies: [],
        parameters: new Map(),
        errors: [
          {
            code: 'GENERATION_FAILED',
            message: error instanceof Error ? error.message : 'Unknown generation error',
            severity: 'ERROR',
          },
        ],
        warnings: [],
        metadata: {
          generatedAt: new Date().toISOString(),
          fragmentType: 'error',
          lineCount: 0,
        },
      };
    }
  }

  private calculateUpstreamSchema(
    nodeId: string,
    graph: DependencyGraph,
    context: PipelineContext
  ): Array<{ name: string; dataType: PostgreSQLDataType }> {
    const incomingNodes = Array.from(graph.reverseAdjacencyList.get(nodeId) || []);

    if (incomingNodes.length === 0) {
      const node = graph.nodes.get(nodeId)!;
      return this.extractNodeSchema(node);
    }

    const mergedSchema = new Map<string, { name: string; dataType: PostgreSQLDataType }>();

    for (const upstreamNodeId of incomingNodes) {
      const upstreamSchema = context.schemaCache.get(upstreamNodeId);
      if (upstreamSchema) {
        upstreamSchema.forEach((column) => {
          mergedSchema.set(column.name, column);
        });
      }
    }

    const connections = this.connections.filter(
      (conn) => conn.targetNodeId === nodeId && incomingNodes.includes(conn.sourceNodeId)
    );

    connections.forEach((connection) => {
      if (connection.dataFlow.schemaMappings) {
        connection.dataFlow.schemaMappings.forEach((mapping) => {
          const sourceColumn = mergedSchema.get(mapping.sourceColumn);
          if (sourceColumn) {
            mergedSchema.set(mapping.targetColumn, {
              name: mapping.targetColumn,
              dataType: mapping.dataTypeConversion?.to || sourceColumn.dataType,
            });
          }
        });
      }
    });

    return Array.from(mergedSchema.values());
  }

  private extractNodeSchema(
    node: CanvasNode
  ): Array<{ name: string; dataType: PostgreSQLDataType }> {
    if (node.metadata?.tableMapping?.columns) {
      return node.metadata.tableMapping.columns.map((col: any) => ({
        name: col.name,
        dataType: col.dataType,
      }));
    }
    return [];
  }

  /**
 * Extract the output schema of a node from its generated SQL fragment.
 * If the SELECT clause uses '*' or 'table.*' exclusively, the output schema
 * is assumed to be identical to the upstream schema (if available).
 * Otherwise, it attempts to parse explicit column expressions.
 */
private extractOutputSchema(
  fragment: GeneratedSQLFragment,
  node: CanvasNode,
  upstreamSchema?: Array<{ name: string; dataType: PostgreSQLDataType }>
): Array<{ name: string; dataType: PostgreSQLDataType }> {
  // Input nodes have a well‑defined schema from their table mapping
  if (this.isInputNode(node)) {
    const columns = node.metadata?.tableMapping?.columns;
    if (columns && Array.isArray(columns)) {
      return columns.map((col: any) => ({
        name: col.name,
        dataType: col.dataType || PostgreSQLDataType.VARCHAR,
      }));
    }
  }

  // Try to extract the SELECT clause from the SQL fragment
  const selectMatch = fragment.sql.match(/SELECT\s+(.*?)(?:\s+FROM|$)/is);
  if (selectMatch) {
    const selectClause = selectMatch[1];
    const cleaned = selectClause.replace(/\s+/g, ' ').trim();

    // Split the SELECT list by commas, respecting parentheses (simplified)
    const items = cleaned.split(',').map(s => s.trim());

    // Check if every item is either '*' or a qualified star (e.g., "table".*)
    const allStars = items.every(item => 
      item === '*' || /^"?[a-zA-Z_][a-zA-Z0-9_]*"?\.\*$/.test(item)
    );

    // If the SELECT list consists only of star expansions, the output schema
    // is exactly the upstream schema (all columns are passed through unchanged).
    if (allStars && upstreamSchema && upstreamSchema.length > 0) {
      return upstreamSchema;
    }

    // Otherwise, parse explicit column expressions.
    // For each column item, take the alias if present, else the last identifier.
    const columns = items.map(item => {
      // Match "expression AS alias" or "expression alias"
      const aliasMatch = item.match(/\s+AS\s+([^\s,]+)$/i) || item.match(/\s+([^\s,]+)$/);
      const name = aliasMatch ? aliasMatch[1].replace(/["`]/g, '') : item.replace(/["`]/g, '');
      // Note: data type inference would require deeper analysis; we default to VARCHAR.
      return { name, dataType: PostgreSQLDataType.VARCHAR };
    });

    return columns;
  }

  // Fallback: use the upstream schema if available
  if (upstreamSchema && upstreamSchema.length > 0) {
    return upstreamSchema;
  }
  return [];
}

  private createFragmentCacheKey(
    node: UnifiedCanvasNode,
    upstreamSchema: Array<{ name: string; dataType: PostgreSQLDataType }>
  ): string {
    const schemaHash = upstreamSchema
      .map((col) => `${col.name}:${col.dataType}`)
      .join('|');

    return `${node.id}:${node.type}:${schemaHash}:${JSON.stringify(node.metadata || {})}`;
  }

  private isCacheValid(cachedResult: PipelineGenerationResult): boolean {
    const cacheAge = Date.now() - new Date(cachedResult.generatedAt).getTime();
    return cacheAge < 5 * 60 * 1000; // 5 minutes
  }

  // ==================== OPTIMIZATION METHODS ====================

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
      estimatedPerformanceGain: 0,
    };

    const warnings: PipelineWarning[] = [];

    this.logger.info('Applying optimizations');

    if (this.options.useCTEs) {
      summary.cteFlattened = this.applyCTEFlattening(fragments, graph, context);
      context.logger.info(`Flattened ${summary.cteFlattened} CTEs`);
    }

    summary.subqueriesConverted = this.convertSubqueriesToJoins(fragments, graph, context);
    context.logger.info(`Converted ${summary.subqueriesConverted} subqueries to JOINs`);

    summary.predicatePushdowns = this.applyPredicatePushdown(fragments, graph, context);
    context.logger.info(`Applied ${summary.predicatePushdowns} predicate pushdowns`);

    if (this.options.materializeIntermediate) {
      summary.materializedCTEs = this.materializeExpensiveCTEs(fragments, graph, context);
      context.logger.info(`Materialized ${summary.materializedCTEs} CTEs`);
    }

    summary.estimatedPerformanceGain = this.estimatePerformanceGain(summary);

    context.stats.optimizationApplied =
      summary.cteFlattened +
      summary.subqueriesConverted +
      summary.predicatePushdowns;

    return { summary, warnings };
  }

  private applyCTEFlattening(
    fragments: Map<string, GeneratedSQLFragment>,
    graph: DependencyGraph,
    context: PipelineContext
  ): number {
    let flattenedCount = 0;

    for (const [nodeId, fragment] of fragments.entries()) {
      const outgoingNodes = graph.adjacencyList.get(nodeId);

      if (outgoingNodes?.size === 1) {
        const dependentNodeId = Array.from(outgoingNodes)[0];
        const dependentFragment = fragments.get(dependentNodeId);

        if (dependentFragment && this.isCTEFlattenable(fragment, dependentFragment)) {
          const flattenedSQL = this.inlineCTE(fragment.sql, dependentFragment.sql);

          fragments.set(dependentNodeId, {
            ...dependentFragment,
            sql: flattenedSQL,
            dependencies: dependentFragment.dependencies.filter((dep) => dep !== nodeId),
          });

          flattenedCount++;
          context.logger.debug(`Flattened CTE from ${nodeId} into ${dependentNodeId}`);
        }
      }
    }

    return flattenedCount;
  }

  private isCTEFlattenable(
    cteFragment: GeneratedSQLFragment,
    dependentFragment: GeneratedSQLFragment
  ): boolean {
    if (cteFragment.errors.length > 0) return false;

    const cteComplexity = this.estimateSQLComplexity(cteFragment.sql);
    if (cteComplexity > 50) return false;

    const dependentComplexity = this.estimateSQLComplexity(dependentFragment.sql);
    if (dependentComplexity > 100) return false;

    return true;
  }

  private inlineCTE(cteSQL: string, dependentSQL: string): string {
    return dependentSQL.replace(/WITH\s+[\s\S]*?SELECT/i, (match) => {
      return match + ` (${cteSQL})`;
    });
  }

  private convertSubqueriesToJoins(
    fragments: Map<string, GeneratedSQLFragment>,
    _graph: DependencyGraph,
    context: PipelineContext
  ): number {
    let convertedCount = 0;

    for (const [nodeId, fragment] of fragments.entries()) {
      if (fragment.errors.length > 0) continue;

      const existsPattern =
        /WHERE\s+EXISTS\s*\(\s*SELECT\s+1\s+FROM\s+([^\s)]+)\s+WHERE\s+([^)]+)\)/gi;
      let match: RegExpExecArray | null;

      while ((match = existsPattern.exec(fragment.sql)) !== null) {
        const [, subqueryTable, condition] = match;

        const joinSQL = `INNER JOIN ${subqueryTable} ON ${condition}`;
        const convertedSQL = fragment.sql.replace(match[0], joinSQL);

        fragments.set(nodeId, {
          ...fragment,
          sql: convertedSQL,
        });

        convertedCount++;
        context.logger.debug(`Converted EXISTS subquery to JOIN in ${nodeId}`);
      }
    }

    return convertedCount;
  }

  private applyPredicatePushdown(
    fragments: Map<string, GeneratedSQLFragment>,
    graph: DependencyGraph,
    context: PipelineContext
  ): number {
    let pushdownCount = 0;

    for (const [nodeId, fragment] of fragments.entries()) {
      const incomingNodes = Array.from(graph.reverseAdjacencyList.get(nodeId) || []);

      for (const upstreamNodeId of incomingNodes) {
        const upstreamFragment = fragments.get(upstreamNodeId);
        if (!upstreamFragment) continue;

        const whereClause = this.extractWhereClause(fragment.sql);
        if (!whereClause) continue;

        if (this.canPushPredicateToUpstream(whereClause, upstreamFragment, fragment)) {
          const pushedSQL = this.pushPredicateToSQL(upstreamFragment.sql, whereClause);

          fragments.set(upstreamNodeId, {
            ...upstreamFragment,
            sql: pushedSQL,
          });

          const cleanedSQL = this.removePushedPredicate(fragment.sql, whereClause);
          fragments.set(nodeId, {
            ...fragment,
            sql: cleanedSQL,
          });

          pushdownCount++;
          context.logger.debug(`Pushed predicate from ${nodeId} to ${upstreamNodeId}`);
        }
      }
    }

    return pushdownCount;
  }

  private canPushPredicateToUpstream(
    whereClause: string,
    upstreamFragment: GeneratedSQLFragment,
    _currentFragment: GeneratedSQLFragment
  ): boolean {
    const upstreamColumns = this.extractReferencedColumns(upstreamFragment.sql);
    const predicateColumns = this.extractReferencedColumns(whereClause);

    return predicateColumns.every((col) => upstreamColumns.includes(col));
  }

  private pushPredicateToSQL(sql: string, whereClause: string): string {
  // Remove trailing semicolon
  sql = sql.replace(/;+\s*$/, '').trim();

  // If the SQL already has a WHERE clause, combine them
  if (sql.includes('WHERE')) {
    return sql.replace(/WHERE\s+(.+)/i, `WHERE ($1) AND (${whereClause})`);
  }

  // Find the position after the FROM clause, handling aliases and subqueries
  const fromMatch = sql.match(/\bFROM\s+([^\s()]+(?:\s+AS\s+[^\s]+)?)/i);
  if (fromMatch) {
    const fromPart = fromMatch[0];           // e.g., "FROM source AS src"
    const replacement = `${fromPart} WHERE ${whereClause}`;
    return sql.replace(fromPart, replacement);
  }

  // Fallback: attach WHERE at the end
  return `${sql} WHERE ${whereClause}`;
}

  private removePushedPredicate(sql: string, whereClause: string): string {
    return sql.replace(
      new RegExp(`\\s*WHERE\\s*${whereClause.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}`, 'i'),
      ''
    );
  }

  private materializeExpensiveCTEs(
    fragments: Map<string, GeneratedSQLFragment>,
    graph: DependencyGraph,
    context: PipelineContext
  ): number {
    let materializedCount = 0;

    for (const [nodeId, fragment] of fragments.entries()) {
      const outgoingNodes = graph.adjacencyList.get(nodeId);

      if (outgoingNodes && outgoingNodes.size > 1) {
        const complexity = this.estimateSQLComplexity(fragment.sql);
        if (complexity > 30) {
          const materializedSQL = fragment.sql.replace(
            /WITH\s+(\w+)\s+AS\s*\(/i,
            'WITH $1 AS MATERIALIZED ('
          );

          fragments.set(nodeId, {
            ...fragment,
            sql: materializedSQL,
          });

          materializedCount++;
          context.logger.debug(`Materialized CTE for node ${nodeId}`);
        }
      }
    }

    return materializedCount;
  }

  private estimatePerformanceGain(summary: OptimizationSummary): number {
    let gain = 0;
    gain += summary.cteFlattened * 15;
    gain += summary.subqueriesConverted * 40;
    gain += summary.predicatePushdowns * 25;
    gain += summary.materializedCTEs * 35;
    return Math.min(gain, 80);
  }

  // ==================== FINAL SQL ASSEMBLY ====================

  private async buildFinalSQL(
    fragments: Map<string, GeneratedSQLFragment>,
    graph: DependencyGraph,
    topologicalOrder: string[],
    context: PipelineContext
  ): Promise<string> {
    this.logger.info('Building final SQL');

    if (this.options.useCTEs) {
      return this.buildFinalSQLWithCTEs(fragments, graph, topologicalOrder, context);
    } else {
      return this.buildFinalSQLWithNested(fragments, graph, topologicalOrder, context);
    }
  }

  private async buildFinalSQLWithCTEs(
    fragments: Map<string, GeneratedSQLFragment>,
    graph: DependencyGraph,
    topologicalOrder: string[],
    context: PipelineContext
  ): Promise<string> {
    const cteNameMap = new Map<string, string>();
    for (const nodeId of topologicalOrder) {
      const node = graph.nodes.get(nodeId)!;
      cteNameMap.set(nodeId, this.generateCTEName(node));
    }

    const modifiedFragments = new Map<string, GeneratedSQLFragment>();
    for (const nodeId of topologicalOrder) {
      const fragment = fragments.get(nodeId);
      if (!fragment) continue;

      let sql = fragment.sql;
      const incomingNodes = Array.from(graph.reverseAdjacencyList.get(nodeId) || []);
      for (const sourceId of incomingNodes) {
        if (cteNameMap.has(sourceId)) {
          sql = this.replaceNodeReferences(sql, new Map([[sourceId, cteNameMap.get(sourceId)!]]));
        }
      }
      modifiedFragments.set(nodeId, { ...fragment, sql });
    }

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
    if (!outputNode) {
      throw new Error('No output node found in pipeline. Cannot generate SQL.');
    }

    const incomingNodes = Array.from(graph.reverseAdjacencyList.get(outputNode.id) || []);
    if (incomingNodes.length === 0) {
      throw new Error('Output node has no incoming connections');
    }
    const sourceNodeId = incomingNodes[0];
    const sourceCteName = cteNameMap.get(sourceNodeId);
    if (!sourceCteName) {
      throw new Error(`No CTE name for source node ${sourceNodeId}`);
    }

    const cteDefinitions: string[] = [];
    for (const nodeId of topologicalOrder) {
      const node = graph.nodes.get(nodeId);
      if (!node) continue;
      if (this.isOutputNode(node)) continue;
      const fragment = modifiedFragments.get(nodeId);
      if (!fragment) continue;
      const cteName = cteNameMap.get(nodeId)!;
      const materialized = this.shouldMaterializeCTE(node, context);
      const materializedKeyword = materialized ? 'MATERIALIZED ' : '';
      let cteSql = fragment.sql.trim();
      if (cteSql.endsWith(';')) {
        cteSql = cteSql.slice(0, -1);
      }
      cteDefinitions.push(
        `${cteName} AS ${materializedKeyword}(\n${this.indent(cteSql, 2)}\n)`
      );
    }
    let withClause = '';
    if (cteDefinitions.length > 0) {
      withClause = `WITH\n${cteDefinitions.join(',\n')}\n\n`;
    }
    const sourceSQL = `${withClause}SELECT * FROM ${sourceCteName}`;

    return this.finalizeWithOutputNode(sourceSQL, outputNode, graph, context);
  }

  private async buildFinalSQLWithNested(
    fragments: Map<string, GeneratedSQLFragment>,
    graph: DependencyGraph,
    topologicalOrder: string[],
    context: PipelineContext
  ): Promise<string> {
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
    if (!outputNode) {
      throw new Error('No output node found in pipeline. Cannot generate SQL.');
    }

    const incomingNodes = Array.from(graph.reverseAdjacencyList.get(outputNode.id) || []);
    if (incomingNodes.length === 0) {
      throw new Error('Output node has no incoming connections');
    }
    const sourceNodeId = incomingNodes[0];

    const aliasMap = new Map<string, string>();
    for (const nodeId of topologicalOrder) {
      aliasMap.set(nodeId, `cte_${nodeId.replace(/[^a-zA-Z0-9]/g, '_')}`);
    }

    // Build the source SELECT without any outer alias.
    const sourceSQL = this.buildNestedQuery(sourceNodeId, graph, fragments, aliasMap);

    return this.finalizeWithOutputNode(sourceSQL, outputNode, graph, context);
  }

  /**
   * Recursively build a SELECT statement for a node, inlining all dependencies.
   * Returns the unaliased SELECT SQL.
   */
  private buildNestedQuery(
  nodeId: string,
  graph: DependencyGraph,
  fragments: Map<string, GeneratedSQLFragment>,
  aliasMap: Map<string, string>
): string {
  const node = graph.nodes.get(nodeId);
  if (!node) {
    throw new Error(`Node ${nodeId} not found`);
  }

  this.logger.debug(`Building nested query for node ${nodeId} (${node.type})`);

  // Input nodes: return the table reference (or simple SELECT) directly.
  if (this.isInputNode(node)) {
    let fragment = fragments.get(nodeId)?.sql || `SELECT * FROM ${this.sanitizeIdentifier(node.name)}`;
    fragment = fragment.replace(/;+\s*$/, '').trim();

    // If it's a trivial "SELECT * FROM table", extract the table name for cleaner SQL.
    const simpleSelectMatch = fragment.match(/^SELECT\s+\*\s+FROM\s+("?[a-zA-Z_][a-zA-Z0-9_]*"?)\s*$/i);
    if (simpleSelectMatch) {
      const tableName = simpleSelectMatch[1];
      this.logger.debug(`Input node ${nodeId} returning table reference: ${tableName}`);
      return tableName;
    }

    // Otherwise (e.g., pushdown filters) keep the SELECT as a subquery.
    this.logger.debug(`Input node ${nodeId} returning subquery: ${fragment}`);
    return fragment;
  }

  // Recursively build dependencies.
  const incomingNodes = Array.from(graph.reverseAdjacencyList.get(nodeId) || []);
  const dependencySubqueries = new Map<string, string>();

  for (const depId of incomingNodes) {
    const depSql = this.buildNestedQuery(depId, graph, fragments, aliasMap);
    const depNode = graph.nodes.get(depId)!;

    // Input nodes that are simple table references are used as-is.
    if (this.isInputNode(depNode) && !depSql.includes('SELECT')) {
      // depSql is a table name (e.g., "source")
      dependencySubqueries.set(depId, depSql);
    } else {
      // Non‑input nodes (or input with pushdown) are wrapped as derived tables.
      const depAlias = aliasMap.get(depId)!;
      dependencySubqueries.set(depId, `(${depSql}) AS ${depAlias}`);
    }
  }

  // Get the fragment SQL for this node.
  let fragmentSql = fragments.get(nodeId)?.sql || '';
  if (!fragmentSql) {
    throw new Error(`No fragment generated for node ${nodeId}`);
  }
  fragmentSql = fragmentSql.replace(/;+\s*$/, '');

  // Replace dependency references with their subquery expressions.
  for (const [depId, depSql] of dependencySubqueries.entries()) {
    const depAlias = aliasMap.get(depId)!;
    const escapedId = this.escapeRegExp(depId);
    const quotedPattern = `"${escapedId}"`;
    // Pattern captures the node ID (quoted or unquoted) and an optional dot + column part.
    // Change the pattern to match a dot followed by an identifier OR a star.
const pattern = new RegExp(`(${quotedPattern}|\\b${escapedId}\\b)(\\.(?:\\w+|\\*))?`, 'g');
    
    fragmentSql = fragmentSql.replace(pattern, (_match, _id, dotPart) => {
      if (dotPart) {
        // Qualified column reference: replace with alias + dotPart
        return `${depAlias}${dotPart}`;
      } else {
        // Standalone reference: replace with subquery
        // Note: depSql already contains the wrapping "(...) AS alias"
        return depSql;
      }
    });
  }

  this.logger.debug(`Unaliased SQL for ${nodeId}: ${fragmentSql}`);
  return fragmentSql;
}


// src/generators/SQLGenerationPipeline.ts

/**
 * Finalize the SQL generation by wrapping the source SQL with an INSERT statement
 * generated by the OutputSQLGenerator. Also handles transaction wrapping and
 * explain plan comments.
 *
 * @param sourceSQL The SQL for the source data (already built).
 * @param outputNode The output node of the pipeline.
 * @param _graph The dependency graph.
 * @param context The pipeline context containing schema cache.
 * @returns The final SQL string ready for execution.
 */
private async finalizeWithOutputNode(
  sourceSQL: string,
  outputNode: CanvasNode,
  _graph: DependencyGraph,
  context: PipelineContext
): Promise<string> {
  const outputGenerator = new OutputSQLGenerator({ postgresVersion: this.options.postgresVersion });
  const unifiedOutputNode = this.toUnifiedNode(outputNode);
  const incomingConnection = this.connections.find(
    (conn) => conn.targetNodeId === outputNode.id
  );
  const schemaMappings = incomingConnection?.dataFlow?.schemaMappings || [];

  // --- NEW: Retrieve upstream schema from the node that feeds the output ---
  let upstreamSchema: Array<{ name: string; dataType: PostgreSQLDataType }> | undefined;
  if (incomingConnection) {
    const sourceNodeId = incomingConnection.sourceNodeId;
    upstreamSchema = context.schemaCache.get(sourceNodeId);
    this.logger.debug(
      `Upstream schema for output node ${outputNode.id} from ${sourceNodeId}:`,
      upstreamSchema?.map(c => c.name)
    );
  }

  let cleanedSource = sourceSQL.trim();
  if (cleanedSource.endsWith(';')) {
    cleanedSource = cleanedSource.slice(0, -1).trim();
  }
  if (cleanedSource.startsWith('(') && cleanedSource.endsWith(')')) {
    cleanedSource = cleanedSource.slice(1, -1).trim();
  }

  // Pass upstreamSchema to the output generator
  const insertResult = outputGenerator.generateInsertSQL(
    cleanedSource,
    unifiedOutputNode,
    schemaMappings // <-- NEW parameter
  );
  let finalSQL = insertResult.sql;

  if (this.options.wrapInTransaction) {
    finalSQL = 'BEGIN;\n\n' + finalSQL + '\n\nCOMMIT;';
  }

  if (this.options.generateExplainPlan) {
    finalSQL += '\n\n' + this.generateExplainPlan();
  }

  this.logger.info('Final SQL built successfully');
  console.log('=== FINAL GENERATED SQL ===\n', finalSQL);
  return finalSQL;
}

  private replaceNodeReferences(sql: string, nodeIdToCteName: Map<string, string>): string {
    let result = sql;
    for (const [nodeId, cteName] of nodeIdToCteName.entries()) {
      const quotedPattern = `"${this.escapeRegExp(nodeId)}"`;
      const unquotedPattern = `\\b${this.escapeRegExp(nodeId)}\\b`;
      const pattern = new RegExp(`${quotedPattern}|${unquotedPattern}`, 'g');
      result = result.replace(pattern, () => cteName);
    }
    return result;
  }

  private escapeRegExp(str: string): string {
    return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }

  // ==================== VALIDATION ====================

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

    const schemaIssues = this.validateSchemaCompatibility(graph);
    issues.push(...schemaIssues);

    if (this.options.validateSyntax) {
      const syntaxIssues = await this.validateSQLSyntax(sql);
      issues.push(...syntaxIssues);
    }

    const performanceIssues = this.validatePerformance(fragments, graph);
    issues.push(...performanceIssues);

    const dependencyIssues = this.validateDependencies(graph);
    issues.push(...dependencyIssues);

    const hasErrors = issues.some((issue) => issue.severity === 'ERROR');
    const hasSchemaErrors = schemaIssues.some((issue) => issue.severity === 'ERROR');
    const hasSyntaxErrors = issues.some(
      (issue) => issue.type === 'syntax' && issue.severity === 'ERROR'
    );

    return {
      isValid: !hasErrors,
      schemaCompatible: !hasSchemaErrors,
      syntaxValid: !hasSyntaxErrors,
      performanceAcceptable: performanceIssues.every((issue) => issue.severity !== 'ERROR'),
      issues,
    };
  }

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

    for (const connection of this.connections) {
      if (connection.status === ConnectionStatus.INVALID) {
        issues.push({
          type: 'schema',
          message: `Connection ${connection.id} is marked as invalid`,
          severity: 'ERROR',
          location: `connection:${connection.id}`,
        });
      }
    }

    return issues;
  }

  private async validateSQLSyntax(sql: string): Promise<
    Array<{
      type: 'syntax';
      message: string;
      severity: 'ERROR' | 'WARNING' | 'INFO';
      location?: string;
    }>
  > {
    const issues: Array<{
      type: 'syntax';
      message: string;
      severity: 'ERROR' | 'WARNING' | 'INFO';
      location?: string;
    }> = [];

    const lines = sql.split('\n');

    let openParens = 0;
    let openQuotes = false;

    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      const lineNumber = i + 1;

      openParens += (line.match(/\(/g) || []).length;
      openParens -= (line.match(/\)/g) || []).length;

      const singleQuotes = (line.match(/'/g) || []).length;
      if (singleQuotes % 2 !== 0) {
        openQuotes = !openQuotes;
      }

      if (line.trim().endsWith(';') && i < lines.length - 1) {
        const nextLine = lines[i + 1].trim();
        if (nextLine && !nextLine.startsWith('--')) {
          issues.push({
            type: 'syntax',
            message: `Semicolon before end of statement at line ${lineNumber}`,
            severity: 'WARNING',
            location: `line:${lineNumber}`,
          });
        }
      }
    }

    if (openParens > 0) {
      issues.push({
        type: 'syntax',
        message: `Unclosed parentheses detected (${openParens} open)`,
        severity: 'ERROR',
      });
    }

    if (openQuotes) {
      issues.push({
        type: 'syntax',
        message: 'Unclosed single quote detected',
        severity: 'ERROR',
      });
    }

    return issues;
  }

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

    for (const [nodeId, fragment] of fragments.entries()) {
      if (fragment.errors.length > 0) continue;

      const sql = fragment.sql;
      const node = graph.nodes.get(nodeId)!;

      if (this.detectCartesianProduct(sql)) {
        issues.push({
          type: 'performance',
          message: `Potential cartesian product detected in node ${node.name}`,
          severity: 'ERROR',
          location: `node:${nodeId}`,
        });
      }

      const expensiveLikePatterns = sql.match(/LIKE\s+'%[^']*%[^']*%'/gi);
      if (expensiveLikePatterns) {
        issues.push({
          type: 'performance',
          message: `Expensive LIKE pattern with multiple wildcards in node ${node.name}`,
          severity: 'WARNING',
          location: `node:${nodeId}`,
        });
      }

      const orCount = (sql.match(/\bOR\b/gi) || []).length;
      if (orCount > 5) {
        issues.push({
          type: 'performance',
          message: `Multiple OR conditions (${orCount}) in node ${node.name} may impact performance`,
          severity: 'WARNING',
          location: `node:${nodeId}`,
        });
      }
    }

    return issues;
  }

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

    for (const [nodeId, outdegree] of graph.outdegree.entries()) {
      if (outdegree === 0) {
        const node = graph.nodes.get(nodeId)!;
        if (!this.isOutputNode(node)) {
          issues.push({
            type: 'dependency',
            message: `Node ${node.name} has no outgoing connections (potential dead end)`,
            severity: 'WARNING',
            location: `node:${nodeId}`,
          });
        }
      }
    }

    for (const [nodeId, indegree] of graph.indegree.entries()) {
      if (indegree === 0) {
        const node = graph.nodes.get(nodeId)!;
        if (!this.isInputNode(node)) {
          issues.push({
            type: 'dependency',
            message: `Node ${node.name} has no incoming connections (potential source node)`,
            severity: 'INFO',
            location: `node:${nodeId}`,
          });
        }
      }
    }

    return issues;
  }

  // ==================== HELPER METHODS ====================

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
        stats: stats
          ? {
              nodesProcessed: 0,
              totalNodes: this.nodes.length,
              fragmentsGenerated: 0,
              errors: 0,
              warnings: 0,
              ...stats,
            }
          : undefined,
      };

      this.options.progressCallback(progressUpdate);
    }
  }

  private generateCTEName(node: CanvasNode): string {
    return `cte_${node.id.replace(/[^a-zA-Z0-9]/g, '_')}_${node.name
      .toLowerCase()
      .replace(/\s+/g, '_')
      .replace(/[^a-z0-9_]/g, '')
      .slice(0, 20)}`;
  }

  private generateCTENameFromUnified(node: UnifiedCanvasNode): string {
    return `cte_${node.id.replace(/[^a-zA-Z0-9]/g, '_')}_${node.name
      .toLowerCase()
      .replace(/\s+/g, '_')
      .replace(/[^a-z0-9_]/g, '')
      .slice(0, 20)}`;
  }

  private shouldMaterializeCTE(node: CanvasNode, _context: PipelineContext): boolean {
    if (this.options.materializeIntermediate) return true;

    const complexity = this.estimateNodeComplexity(node);
    return complexity > 50;
  }

  private shouldMaterializeCTEFromUnified(node: UnifiedCanvasNode, _context: PipelineContext): boolean {
    if (this.options.materializeIntermediate) return true;

    let complexity = 30;
    if (node.metadata?.configuration) {
      switch (node.metadata.configuration.type) {
        case 'JOIN':
          complexity = 70;
          break;
        case 'AGGREGATE':
          complexity = 60;
          break;
        case 'FILTER':
          complexity = 20;
          break;
        case 'MAP':
          complexity = 40;
          break;
        default:
          complexity = 30;
      }
    }
    return complexity > 50;
  }

  private estimateNodeComplexity(node: CanvasNode): number {
    let complexity = 0;

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

    if (node.metadata?.transformationRules?.length) {
      complexity += node.metadata.transformationRules.length * 5;
    }

    if (node.metadata?.schemaMappings?.length) {
      complexity += node.metadata.schemaMappings.length * 3;
    }

    return complexity;
  }

  private estimateSQLComplexity(sql: string): number {
    if (!sql) return 0;

    let complexity = 0;

    complexity += (sql.match(/\bJOIN\b/gi) || []).length * 20;
    complexity += (sql.match(/\(\s*SELECT/gi) || []).length * 15;
    complexity +=
      (sql.match(/\b(SUM|AVG|COUNT|MIN|MAX|STDDEV|VARIANCE)\s*\(/gi) || []).length * 10;

    const whereMatch = sql.match(/WHERE\s+(.+?)(?=\b(?:GROUP BY|ORDER BY|LIMIT|$))/i);
    if (whereMatch) {
      const whereClause = whereMatch[1];
      complexity += (whereClause.match(/\bAND\b/gi) || []).length * 5;
      complexity += (whereClause.match(/\bOR\b/gi) || []).length * 8;
    }

    return complexity;
  }

  private estimateRowCountFromUnified(node: UnifiedCanvasNode): number {
    if (node.type === NodeType.INPUT) return 10000;
    if (node.type === NodeType.FILTER_ROW) return 5000;
    if (node.type === NodeType.AGGREGATE_ROW) return 100;
    if (node.type === NodeType.JOIN) return 20000;
    return 1000;
  }

  private detectCartesianProduct(sql: string): boolean {
    const fromMatch = sql.match(/FROM\s+([^(\s]+(?:\s*,\s*[^(\s]+)+)/i);
    if (fromMatch) {
      const tables = fromMatch[1].split(',').map((t) => t.trim());
      if (tables.length > 1) {
        const whereMatch = sql.match(/WHERE\s+(.+)/i);
        if (!whereMatch) {
          return true;
        }

        const whereClause = whereMatch[1];
        const hasJoinCondition = tables.some((table1, i) =>
          tables.slice(i + 1).some(
            (table2) =>
              whereClause.includes(`${table1}.`) && whereClause.includes(`${table2}.`)
          )
        );

        return !hasJoinCondition;
      }
    }

    return false;
  }

  private extractWhereClause(sql: string): string | null {
    const whereMatch = sql.match(/WHERE\s+(.+?)(?=\b(?:GROUP BY|ORDER BY|HAVING|LIMIT|$))/i);
    return whereMatch ? whereMatch[1].trim() : null;
  }

  private extractReferencedColumns(sql: string): string[] {
    const columns: string[] = [];
    const columnPattern = /\b([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\b/g;

    let match: RegExpExecArray | null;
    while ((match = columnPattern.exec(sql)) !== null) {
      columns.push(match[0]);
    }

    return columns;
  }


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
      ' */',
    ].join('\n');
  }

  private formatSQL(sql: string): string {
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

      if (
        trimmed.startsWith(')') ||
        trimmed.startsWith('END') ||
        trimmed.startsWith('ELSE') ||
        trimmed.match(/^\s*(FROM|WHERE|GROUP BY|HAVING|ORDER BY|LIMIT|OFFSET)/i)
      ) {
        indentLevel = Math.max(0, indentLevel - 1);
      }

      const indent = ' '.repeat(indentLevel * indentSize);
      formattedLines.push(indent + trimmed);

      if (
        trimmed.endsWith('(') ||
        trimmed.match(/^\s*(SELECT|FROM|WHERE|GROUP BY|HAVING|ORDER BY|LIMIT|OFFSET)/i) ||
        trimmed.startsWith('CASE') ||
        trimmed.startsWith('WHEN')
      ) {
        indentLevel++;
      }
    }

    return formattedLines.join('\n');
  }

  private indent(text: string, spaces: number): string {
    const indent = ' '.repeat(spaces);
    return text
      .split('\n')
      .map((line) => indent + line)
      .join('\n');
  }

  private getMemoryUsage(): number {
    if (typeof process !== 'undefined' && process.memoryUsage) {
      return Math.round(process.memoryUsage().heapUsed / 1024 / 1024);
    }
    return 0;
  }

  private cleanupMemoizationCache(): void {
    const maxCacheSize = 50;

    if (this.memoizationCache.size > maxCacheSize) {
      const keys = Array.from(this.memoizationCache.keys());
      const keysToRemove = keys.slice(0, this.memoizationCache.size - maxCacheSize);

      keysToRemove.forEach((key) => {
        this.memoizationCache.delete(key);
      });

      this.logger.info(`Cleaned up ${keysToRemove.length} cache entries`);
    }
  }

  private sanitizeIdentifier(identifier: string): string {
    if (!identifier) return '""';
    let sanitized = identifier.replace(/[^a-zA-Z0-9_]/g, '_');
    if (/^[0-9]/.test(sanitized)) {
      sanitized = '_' + sanitized;
    }
    return `"${sanitized.replace(/"/g, '""')}"`;
  }
}

// ==================== USAGE EXAMPLE ====================

export async function generatePipelineSQL(
  nodes: CanvasNode[],
  connections: CanvasConnection[],
  options?: Partial<PipelineGenerationOptions>
): Promise<PipelineGenerationResult> {
  const pipeline = new SQLGenerationPipeline(nodes, connections, options);
  return pipeline.generate();
}

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