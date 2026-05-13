// src/validation/validationEngine.ts

import {
  GraphState,
  ValidationRule,
  ValidationSummary,
  ValidationResult,
  GraphEdge
} from './types';
import { ValidationResultFactory } from './validationResult';
import { SchemaRegistry } from './schemaRegistry';
import { ValidationRuleFactory } from './validationRules';

/**
 * Validation Engine Configuration
 */
export interface ValidationEngineConfig {
  /** Enable/disable specific validation rules */
  enabledRules?: string[];
  
  /** Validation mode */
  mode?: 'strict' | 'lenient' | 'warn-only';
  
  /** Custom validation rules */
  customRules?: ValidationRule[];
  
  /** Schema registry instance */
  schemaRegistry: SchemaRegistry;
  
  /** Cache validation results */
  enableCaching?: boolean;
  
  /** Cache TTL in milliseconds */
  cacheTTL?: number;
  
  /** Enable ETL-specific validation */
  enableETLValidation?: boolean;
  
  /** ETL validation mode: 'strict' | 'relaxed' */
  etlMode?: 'strict' | 'relaxed';
}

/**
 * Cache entry for validation results
 */
interface ValidationCacheEntry {
  timestamp: number;
  graphHash: string;
  results: ValidationSummary;
}

/**
 * Main Validation Engine
 */
export class ValidationEngine {
  [x: string]: any;
  private config: ValidationEngineConfig;
  private rules: ValidationRule[] = [];
  private cache: Map<string, ValidationCacheEntry> = new Map();
  private defaultCacheTTL = 5000; // 5 seconds
  
  constructor(config: ValidationEngineConfig) {
    this.config = {
      mode: 'strict',
      enableCaching: true,
      cacheTTL: this.defaultCacheTTL,
      enableETLValidation: true,
      etlMode: 'strict',
      ...config
    };
    
    this.initializeRules();
  }
  
  /**
   * Initialize validation rules
   */
  private initializeRules(): void {
    // Add built-in rules
    const builtInRules = ValidationRuleFactory.createAllRules(this.config.schemaRegistry);
    
    // Filter by enabled rules if specified
    if (this.config.enabledRules) {
      this.rules = builtInRules.filter(rule => 
        this.config.enabledRules!.includes(rule.id)
      );
    } else {
      this.rules = builtInRules;
    }
    
    // Add custom rules
    if (this.config.customRules) {
      this.rules.push(...this.config.customRules);
    }
  }
  
  /**
   * Generate hash for graph state (for caching)
   */
  private generateGraphHash(state: GraphState): string {
    // Simple hash based on node and edge counts and IDs
    const nodeHash = state.nodes
      .map(n => `${n.id}:${n.type}`)
      .sort()
      .join('|');
    
    const edgeHash = state.edges
      .map(e => `${e.source}:${e.target}`)
      .sort()
      .join('|');
    
    return `${state.nodes.length}:${state.edges.length}:${nodeHash}:${edgeHash}`;
  }
  
  /**
   * Check if cached results are still valid
   */
  private isCacheValid(graphHash: string): boolean {
    if (!this.config.enableCaching) return false;
    
    const entry = this.cache.get(graphHash);
    if (!entry) return false;
    
    const age = Date.now() - entry.timestamp;
    return age < (this.config.cacheTTL || this.defaultCacheTTL);
  }
  
  /**
   * Get cached results
   */
  private getCachedResults(graphHash: string): ValidationSummary | null {
    if (!this.config.enableCaching) return null;
    
    const entry = this.cache.get(graphHash);
    if (entry && this.isCacheValid(graphHash)) {
      return entry.results;
    }
    
    return null;
  }
  
  /**
   * Cache validation results
   */
  private cacheResults(graphHash: string, results: ValidationSummary): void {
    if (!this.config.enableCaching) return;
    
    this.cache.set(graphHash, {
      timestamp: Date.now(),
      graphHash,
      results
    });
    
    // Clean up old cache entries
    this.cleanupCache();
  }
  
  /**
   * Clean up expired cache entries
   */
  private cleanupCache(): void {
    const now = Date.now();
    const ttl = this.config.cacheTTL || this.defaultCacheTTL;
    
    this.cache.forEach((entry, key) => {
      if (now - entry.timestamp > ttl) {
        this.cache.delete(key);
      }
    });
  }
  
  /**
   * Validate entire graph
   */
  validateGraph(state: GraphState): ValidationSummary {
    const graphHash = this.generateGraphHash(state);
    const cached = this.getCachedResults(graphHash);
    
    if (cached) {
      return cached;
    }
    
    const allResults: ValidationResult[] = [];
    
    // Run validation for each rule
    this.rules.forEach(rule => {
      // Run rule on entire graph
      const ruleResults = this.applyRuleToGraph(rule, state);
      // Flatten the array of arrays and push into allResults
      ruleResults.forEach(resultArray => {
        allResults.push(...resultArray);
      });
    });
    
    // Run ETL-specific graph topology validation if enabled
    if (this.config.enableETLValidation) {
      const etlTopologyResults = this.validateETLTopology(state);
      allResults.push(...etlTopologyResults);
    }
    
    // Filter results based on mode
    const filteredResults = this.filterResultsByMode(allResults);
    
    // Create summary
    const summary = ValidationResultFactory.createSummary(filteredResults, {
      nodeCount: state.nodes.length,
      edgeCount: state.edges.length
    });
    
    // Cache results
    this.cacheResults(graphHash, summary);
    
    return summary;
  }
  
  /**
   * Validate ETL-specific topology
   */
  validateETLTopology(state: GraphState): ValidationResult[] {
    const results: ValidationResult[] = [];
    
    if (!this.config.enableETLValidation) {
      return results;
    }
    
    // Get ETL connectivity rule
    const etlRule = this.rules.find(rule => rule.id === 'etl-connectivity');
    if (!etlRule) {
      return results;
    }
    
    // Validate each node's ETL topology
    state.nodes.forEach(node => {
      const nodeResults = etlRule.validate(node, {} as GraphEdge, state);
      results.push(...nodeResults);
    });
    
    // Validate each edge's ETL compatibility
    state.edges.forEach(edge => {
      const sourceNode = state.nodes.find(n => n.id === edge.source);
      if (sourceNode) {
        const edgeResults = etlRule.validate(sourceNode, edge, state);
        results.push(...edgeResults);
      }
    });
    
    // Apply ETL mode filtering
    return this.filterETLResultsByMode(results);
  }
  

  // Add this method to the ValidationEngine class in validationEngine.ts

/**
 * Validate a specific connection with detailed results
 */
validateSpecificConnection(
  sourceNodeId: string,
  targetNodeId: string,
  state: GraphState
): {
  isValid: boolean;
  errors: string[];
  warnings: string[];
  detailedResults: ValidationResult[];
  etlClassification: {
    source: string;
    target: string;
    allowed: boolean;
  };
} {
  const sourceNode = state.nodes.find(n => n.id === sourceNodeId);
  const targetNode = state.nodes.find(n => n.id === targetNodeId);
  
  if (!sourceNode || !targetNode) {
    return {
      isValid: false,
      errors: ['Source or target node not found'],
      warnings: [],
      detailedResults: [],
      etlClassification: {
        source: 'unknown',
        target: 'unknown',
        allowed: false
      }
    };
  }
  
  // Create temporary edge for validation
  const tempEdge: GraphEdge = {
    id: 'temp-validation-edge',
    source: sourceNodeId,
    target: targetNodeId
  };
  
  const allResults: ValidationResult[] = [];
  
  // Run validation for each rule that applies to connections
  this.rules.forEach(rule => {
    if (rule.appliesTo(sourceNode, tempEdge, state)) {
      const ruleResults = rule.validate(sourceNode, tempEdge, state);
      allResults.push(...ruleResults);
    }
  });
  
  // Get ETL classifications
  const sourceClassification = this.getETLClassification(sourceNodeId, state);
  const targetClassification = this.getETLClassification(targetNodeId, state);
  
  // Filter results based on mode
  const filteredResults = this.filterResultsByMode(allResults);
  
  const hasErrors = filteredResults.some(r => r.level === 'error');
  
  return {
    isValid: !hasErrors,
    errors: filteredResults
      .filter(r => r.level === 'error')
      .map(r => r.message),
    warnings: filteredResults
      .filter(r => r.level === 'warning')
      .map(r => r.message),
    detailedResults: filteredResults,
    etlClassification: {
      source: sourceClassification.etlCategory,
      target: targetClassification.etlCategory,
      allowed: !hasErrors
    }
  };
}
  /**
   * Filter ETL results based on ETL mode
   */
  private filterETLResultsByMode(results: ValidationResult[]): ValidationResult[] {
    if (this.config.etlMode === 'relaxed') {
      // In relaxed mode, convert ETL errors to warnings
      return results.map(result => {
        if (result.level === 'error' && 
            result.code.startsWith('ETL_') || 
            this.isETLErrorCode(result.code)) {
          return {
            ...result,
            level: 'warning'
          };
        }
        return result;
      });
    }
    
    return results;
  }
  
  /**
   * Check if error code is ETL-related
   */
  private isETLErrorCode(code: string): boolean {
    const etlCodes = [
      'INVALID_ETL_CONNECTION',
      'MULTIPLE_INPUTS_DISALLOWED',
      'FAN_OUT_DISALLOWED',
      'INVALID_MERGE_NODE_INPUTS',
      'INVALID_BRANCHING_NODE_OUTPUTS',
      'SOURCE_TO_SOURCE_DISALLOWED',
      'SINK_TO_SINK_DISALLOWED',
      'OUTPUT_TO_OUTPUT_DISALLOWED',
      'INPUT_TO_INPUT_DISALLOWED',
      'TOO_MANY_INPUTS',
      'TOO_FEW_INPUTS',
      'EXACT_INPUT_COUNT_REQUIRED',
      'OUTPUT_PORT_REUSE_DISALLOWED',
      'INPUT_PORT_MULTIPLE_CONNECTIONS',
      'MERGE_NODE_MISSING_INPUTS',
      'NON_MERGE_NODE_MULTIPLE_INPUTS',
      'BRANCHING_REQUIRED_FOR_FAN_OUT',
      'SOURCE_COMPONENT_INPUTS',
      'PROCESSING_NODE_MISSING_INPUTS',
      'SINK_COMPONENT_OUTPUTS',
      'PROCESSING_NODE_MISSING_OUTPUTS'
    ];
    
    return etlCodes.includes(code);
  }
  
  /**
   * Validate a specific connection
   */
  validateConnection(
    sourceNodeId: string,
    targetNodeId: string,
    state: GraphState
  ): ValidationSummary {
    // Create temporary edge for validation
    const tempEdge: GraphEdge = {
      id: 'temp-validation-edge',
      source: sourceNodeId,
      target: targetNodeId
    };
    
    const sourceNode = state.nodes.find(n => n.id === sourceNodeId);
    const targetNode = state.nodes.find(n => n.id === targetNodeId);
    
    if (!sourceNode || !targetNode) {
      throw new Error('Source or target node not found');
    }
    
    const allResults: ValidationResult[] = [];
    
    // Run validation for each rule that applies to connections
    this.rules.forEach(rule => {
      if (rule.appliesTo(sourceNode, tempEdge, state)) {
        const ruleResults = rule.validate(sourceNode, tempEdge, state);
        allResults.push(...ruleResults);
      }
    });
    
    // Run ETL-specific validation for this connection
    if (this.config.enableETLValidation) {
      const etlResults = this.validateETLConnection(sourceNode, targetNode, tempEdge, state);
      allResults.push(...etlResults);
    }
    
    // Filter results based on mode
    const filteredResults = this.filterResultsByMode(allResults);
    
    // Create summary focused on this connection
    return ValidationResultFactory.createSummary(filteredResults, {
      nodeCount: 2,
      edgeCount: 1
    });
  }
  
  /**
   * Validate ETL connection
   */
  private validateETLConnection(
    sourceNode: GraphNode,
    _targetNode: GraphNode,
    edge: GraphEdge,
    state: GraphState
  ): ValidationResult[] {
    const results: ValidationResult[] = [];
    
    // Get ETL connectivity rule
    const etlRule = this.rules.find(rule => rule.id === 'etl-connectivity');
    if (!etlRule) {
      return results;
    }
    
    // Validate connection with ETL rules
    const etlResults = etlRule.validate(sourceNode, edge, state);
    
    // Apply ETL mode filtering
    return this.filterETLResultsByMode(etlResults);
  }
  
  /**
   * Check if a connection would be valid
   */
  wouldConnectionBeValid(
    sourceNodeId: string,
    targetNodeId: string,
    state: GraphState
  ): { isValid: boolean; errors: string[]; warnings: string[] } {
    const summary = this.validateConnection(sourceNodeId, targetNodeId, state);
    
    // Check if there are any errors after filtering
    const hasErrors = summary.results.some(r => r.level === 'error');
    
    return {
      isValid: !hasErrors,
      errors: summary.results
        .filter(r => r.level === 'error')
        .map(r => r.message),
      warnings: summary.results
        .filter(r => r.level === 'warning')
        .map(r => r.message)
    };
  }
  
  /**
   * Apply a rule to the entire graph
   */
  private applyRuleToGraph(rule: ValidationRule, state: GraphState): ValidationResult[][] {
    const results: ValidationResult[][] = [];
    
    // Apply to all nodes and edges
    state.nodes.forEach(node => {
      if (rule.appliesTo(node, {} as GraphEdge, state)) {
        const nodeResults = rule.validate(node, {} as GraphEdge, state);
        results.push(nodeResults);
      }
    });
    
    state.edges.forEach(edge => {
      const sourceNode = state.nodes.find(n => n.id === edge.source);
      const targetNode = state.nodes.find(n => n.id === edge.target);
      
      if (sourceNode && targetNode && rule.appliesTo(sourceNode, edge, state)) {
        const edgeResults = rule.validate(sourceNode, edge, state);
        results.push(edgeResults);
      }
    });
    
    return results;
  }
  
  /**
   * Filter results based on validation mode
   */
  private filterResultsByMode(
    results: ValidationResult[]
  ): ValidationResult[] {
    switch (this.config.mode) {
      case 'strict':
        return results; // Return all results
      
      case 'lenient':
        return results.filter(r => r.level === 'error'); // Only errors
      
      case 'warn-only':
        return results.filter(r => r.level === 'warning'); // Only warnings
      
      default:
        return results;
    }
  }
  
  /**
   * Get validation status for specific node
   */
  getNodeValidationStatus(nodeId: string, state: GraphState): {
    isValid: boolean;
    errors: string[];
    warnings: string[];
    results: ValidationResult[];
  } {
    const summary = this.validateGraph(state);
    const grouped = ValidationResultFactory.groupByAffectedElement(summary.results);
    
    const nodeResults = grouped.nodes[nodeId] || [];
    
    return {
      isValid: !ValidationResultFactory.hasNodeErrors(nodeId, grouped),
      errors: nodeResults
        .filter(r => r.level === 'error')
        .map(r => r.message),
      warnings: nodeResults
        .filter(r => r.level === 'warning')
        .map(r => r.message),
      results: nodeResults
    };
  }
  
  /**
   * Get validation status for specific edge
   */
  getEdgeValidationStatus(edgeId: string, state: GraphState): {
    isValid: boolean;
    errors: string[];
    warnings: string[];
    results: ValidationResult[];
  } {
    const summary = this.validateGraph(state);
    const grouped = ValidationResultFactory.groupByAffectedElement(summary.results);
    
    const edgeResults = grouped.edges[edgeId] || [];
    
    return {
      isValid: !ValidationResultFactory.hasEdgeErrors(edgeId, grouped),
      errors: edgeResults
        .filter(r => r.level === 'error')
        .map(r => r.message),
      warnings: edgeResults
        .filter(r => r.level === 'warning')
        .map(r => r.message),
      results: edgeResults
    };
  }
  
  /**
   * Get ETL classification for a node
   */
  getETLClassification(nodeId: string, state: GraphState): {
    etlCategory: 'source' | 'sink' | 'processing' | 'merge' | 'branching' | 'unknown';
    isMultiInput?: boolean;
    isBranching?: boolean;
  } {
    const etlRule = this.rules.find(rule => rule.id === 'etl-connectivity');
    
    if (!etlRule) {
      return { etlCategory: 'unknown' };
    }
    
    const node = state.nodes.find(n => n.id === nodeId);
    if (!node) {
      return { etlCategory: 'unknown' };
    }
    
    // Use the getComponentClassification method from ETLConnectivityRule
    if ('getComponentClassification' in etlRule) {
      const classification = (etlRule as any).getComponentClassification(node);
      return classification || { etlCategory: 'unknown' };
    }
    
    return { etlCategory: 'unknown' };
  }
  
  /**
   * Validate if a node can accept multiple inputs
   */
  canNodeAcceptMultipleInputs(nodeId: string, state: GraphState): boolean {
    const classification = this.getETLClassification(nodeId, state);
    
    // Merge components can accept multiple inputs
    return classification.etlCategory === 'merge' || 
           classification.etlCategory === 'branching' ||
           classification.isMultiInput === true;
  }
  
  /**
   * Validate if a node can have multiple outputs
   */
  canNodeHaveMultipleOutputs(nodeId: string, state: GraphState): boolean {
    const classification = this.getETLClassification(nodeId, state);
    
    // Branching components can have multiple outputs
    return classification.etlCategory === 'branching' || 
           classification.isBranching === true;
  }
  
  /**
   * Check if connection violates ETL fan-out rules
   */
  checkFanOutViolation(sourceNodeId: string, state: GraphState): {
    violates: boolean;
    requiresBranching: boolean;
    currentOutputCount: number;
  } {
    const classification = this.getETLClassification(sourceNodeId, state);
    
    // Count current outputs
    const currentOutputCount = state.edges.filter(e => e.source === sourceNodeId).length;
    
    // Check if this is a branching component
    const isBranching = classification.etlCategory === 'branching' || 
                       classification.isBranching === true;
    
    // Fan-out is allowed for branching components, not for others
    const violates = currentOutputCount >= 1 && !isBranching;
    
    return {
      violates,
      requiresBranching: violates,
      currentOutputCount
    };
  }
  
  /**
   * Check if connection violates ETL multi-input rules
   */
  checkMultiInputViolation(targetNodeId: string, state: GraphState): {
    violates: boolean;
    requiresMerge: boolean;
    currentInputCount: number;
  } {
    const classification = this.getETLClassification(targetNodeId, state);
    
    // Count current inputs
    const currentInputCount = state.edges.filter(e => e.target === targetNodeId).length;
    
    // Check if this is a merge component
    const isMerge = classification.etlCategory === 'merge' || 
                   classification.isMultiInput === true;
    
    // Multiple inputs are only allowed for merge components
    const violates = currentInputCount >= 1 && !isMerge;
    
    return {
      violates,
      requiresMerge: violates,
      currentInputCount
    };
  }
  
  /**
   * Add custom validation rule
   */
  addRule(rule: ValidationRule): void {
    this.rules.push(rule);
    this.clearCache();
  }
  
  /**
   * Remove validation rule by ID
   */
  removeRule(ruleId: string): boolean {
    const index = this.rules.findIndex(rule => rule.id === ruleId);
    if (index !== -1) {
      this.rules.splice(index, 1);
      this.clearCache();
      return true;
    }
    return false;
  }
  
  /**
   * Clear validation cache
   */
  clearCache(): void {
    this.cache.clear();
  }
  
  /**
   * Update configuration
   */
  updateConfig(updates: Partial<ValidationEngineConfig>): void {
    this.config = { ...this.config, ...updates };
    
    // Reinitialize rules if schema registry changed
    if (updates.schemaRegistry || updates.enabledRules || updates.customRules) {
      this.initializeRules();
    }
    
    this.clearCache();
  }
  
  /**
   * Get current configuration
   */
  getConfig(): ValidationEngineConfig {
    return { ...this.config };
  }
  
  /**
   * Get all registered rules
   */
  getRules(): ValidationRule[] {
    return [...this.rules];
  }
  
  /**
   * Get statistics about validation performance
   */
  getStatistics(): {
    cacheSize: number;
    ruleCount: number;
    averageValidationTime?: number;
    etlValidationEnabled: boolean;
    etlMode: string;
  } {
    return {
      cacheSize: this.cache.size,
      ruleCount: this.rules.length,
      etlValidationEnabled: this.config.enableETLValidation || false,
      etlMode: this.config.etlMode || 'strict'
    };
  }
  
  /**
   * Get ETL-specific validation summary
   */
  getETLValidationSummary(state: GraphState): {
    isValid: boolean;
    errors: string[];
    warnings: string[];
    nodeClassifications: Record<string, string>;
    violations: {
      fanOut: string[];
      multiInput: string[];
      invalidConnections: string[];
    };
  } {
    const summary = this.validateGraph(state);
    
    // Filter ETL-specific results
    const etlErrors = summary.results.filter(r => 
      r.level === 'error' && this.isETLErrorCode(r.code)
    );
    
    const etlWarnings = summary.results.filter(r => 
      r.level === 'warning' && this.isETLErrorCode(r.code)
    );
    
    // Get node classifications
    const nodeClassifications: Record<string, string> = {};
    state.nodes.forEach(node => {
      const classification = this.getETLClassification(node.id, state);
      nodeClassifications[node.id] = classification.etlCategory;
    });
    
    // Identify violations
    const fanOutViolations: string[] = [];
    const multiInputViolations: string[] = [];
    const invalidConnections: string[] = [];
    
    state.nodes.forEach(node => {
      const fanOutCheck = this.checkFanOutViolation(node.id, state);
      if (fanOutCheck.violates) {
        fanOutViolations.push(`${node.data.name} (${node.type}) has ${fanOutCheck.currentOutputCount} outputs without branching`);
      }
      
      const multiInputCheck = this.checkMultiInputViolation(node.id, state);
      if (multiInputCheck.violates) {
        multiInputViolations.push(`${node.data.name} (${node.type}) has ${multiInputCheck.currentInputCount} inputs without merge capability`);
      }
    });
    
    // Check for invalid ETL connections
    state.edges.forEach(edge => {
      const sourceNode = state.nodes.find(n => n.id === edge.source);
      const targetNode = state.nodes.find(n => n.id === edge.target);
      
      if (sourceNode && targetNode) {
        const connectionCheck = this.wouldConnectionBeValid(sourceNode.id, targetNode.id, state);
        if (!connectionCheck.isValid) {
          invalidConnections.push(`${sourceNode.data.name} → ${targetNode.data.name}`);
        }
      }
    });
    
    return {
      isValid: etlErrors.length === 0,
      errors: etlErrors.map(e => e.message),
      warnings: etlWarnings.map(w => w.message),
      nodeClassifications,
      violations: {
        fanOut: fanOutViolations,
        multiInput: multiInputViolations,
        invalidConnections
      }
    };
  }
}

// Re-export GraphNode type
import { GraphNode } from './types';
export type { GraphNode };