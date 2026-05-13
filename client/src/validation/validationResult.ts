// src/validation/validationResult.ts

import {
  ValidationResult,
  ValidationSummary,
  ValidationLevel,
  ValidationErrorCode} from './types';

/**
 * Factory for creating validation results
 */
export class ValidationResultFactory {
  private static generateId(): string {
    return `val_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  /**
   * Create a validation error
   */
  static createError(
    code: ValidationErrorCode,
    message: string,
    nodeIds: string[] = [],
    edgeIds: string[] = [],
    details?: string,
    fixSuggestion?: string,
    context?: Record<string, any>
  ): ValidationResult {
    return {
      id: this.generateId(),
      code,
      level: ValidationLevel.ERROR,
      message,
      nodeIds,
      edgeIds,
      details,
      fixSuggestion,
      timestamp: new Date().toISOString(),
      context
    };
  }

  /**
   * Create a validation warning
   */
  static createWarning(
    code: ValidationErrorCode,
    message: string,
    nodeIds: string[] = [],
    edgeIds: string[] = [],
    details?: string,
    fixSuggestion?: string,
    context?: Record<string, any>
  ): ValidationResult {
    return {
      id: this.generateId(),
      code,
      level: ValidationLevel.WARNING,
      message,
      nodeIds,
      edgeIds,
      details,
      fixSuggestion,
      timestamp: new Date().toISOString(),
      context
    };
  }

  /**
   * Create validation summary
   */
  static createSummary(
    results: ValidationResult[],
    graphMetadata: { nodeCount: number; edgeCount: number }
  ): ValidationSummary {
    const errorCount = results.filter(r => r.level === ValidationLevel.ERROR).length;
    const warningCount = results.filter(r => r.level === ValidationLevel.WARNING).length;
    const infoCount = results.filter(r => r.level === ValidationLevel.INFO).length;

    return {
      isValid: errorCount === 0,
      counts: {
        errors: errorCount,
        warnings: warningCount,
        infos: infoCount
      },
      results,
      validatedAt: new Date().toISOString(),
      graphMetadata: {
        ...graphMetadata,
        validationStatus: errorCount === 0 ? 'valid' : 'invalid'
      }
    };
  }

  /**
   * Group validation results by affected element
   */
  static groupByAffectedElement(results: ValidationResult[]): {
    nodes: Record<string, ValidationResult[]>;
    edges: Record<string, ValidationResult[]>;
    global: ValidationResult[];
  } {
    const nodes: Record<string, ValidationResult[]> = {};
    const edges: Record<string, ValidationResult[]> = {};
    const global: ValidationResult[] = [];

    results.forEach(result => {
      // Group by nodes
      result.nodeIds.forEach(nodeId => {
        if (!nodes[nodeId]) nodes[nodeId] = [];
        nodes[nodeId].push(result);
      });

      // Group by edges
      result.edgeIds.forEach(edgeId => {
        if (!edges[edgeId]) edges[edgeId] = [];
        edges[edgeId].push(result);
      });

      // Global results (affect entire graph)
      if (result.nodeIds.length === 0 && result.edgeIds.length === 0) {
        global.push(result);
      }
    });

    return { nodes, edges, global };
  }

  /**
   * Filter results by severity
   */
  static filterByLevel(
    results: ValidationResult[],
    level: ValidationLevel
  ): ValidationResult[] {
    return results.filter(r => r.level === level);
  }

  /**
   * Check if a specific node has validation errors
   */
  static hasNodeErrors(
    nodeId: string,
    groupedResults: ReturnType<typeof this.groupByAffectedElement>
  ): boolean {
    const nodeResults = groupedResults.nodes[nodeId] || [];
    return nodeResults.some(r => r.level === ValidationLevel.ERROR);
  }

  /**
   * Check if a specific edge has validation errors
   */
  static hasEdgeErrors(
    edgeId: string,
    groupedResults: ReturnType<typeof this.groupByAffectedElement>
  ): boolean {
    const edgeResults = groupedResults.edges[edgeId] || [];
    return edgeResults.some(r => r.level === ValidationLevel.ERROR);
  }
}

/**
 * Pre-defined validation messages
 */
export const ValidationMessages = {
  // ==================== CYCLE-RELATED ====================
  [ValidationErrorCode.CYCLE_DETECTED]: (nodeNames: string[]) =>
    `Cycle detected involving nodes: ${nodeNames.join(' → ')}`,
  
  [ValidationErrorCode.DISALLOWED_CYCLE]: (nodeName: string) =>
    `Node "${nodeName}" does not allow cycles`,
  
  // ==================== CONNECTION-RELATED ====================
  [ValidationErrorCode.INVALID_CONNECTION_TYPE]: (sourceType: string, targetType: string) =>
    `Connection from "${sourceType}" to "${targetType}" is not allowed`,
  
  [ValidationErrorCode.INVALID_SOURCE_TYPE]: (sourceType: string, allowedTypes: string[]) =>
    `Node type "${sourceType}" cannot be used as source. Allowed: ${allowedTypes.join(', ')}`,
  
  [ValidationErrorCode.INVALID_TARGET_TYPE]: (targetType: string, allowedTypes: string[]) =>
    `Node type "${targetType}" cannot be used as target. Allowed: ${allowedTypes.join(', ')}`,
  
  [ValidationErrorCode.MAX_INCOMING_EXCEEDED]: (nodeName: string, max: number, current: number) =>
    `Node "${nodeName}" exceeds maximum incoming connections (max: ${max}, current: ${current})`,
  
  [ValidationErrorCode.MAX_OUTGOING_EXCEEDED]: (nodeName: string, max: number, current: number) =>
    `Node "${nodeName}" exceeds maximum outgoing connections (max: ${max}, current: ${current})`,
  
  [ValidationErrorCode.PORT_CONNECTION_LIMIT]: (portId: string, max: number) =>
    `Port "${portId}" has reached maximum connection limit (max: ${max})`,
  
  // ==================== SCHEMA-RELATED ====================
  [ValidationErrorCode.SCHEMA_INCOMPATIBILITY]: (sourceNode: string, targetNode: string) =>
    `Schema incompatibility between "${sourceNode}" and "${targetNode}"`,
  
  [ValidationErrorCode.DATA_TYPE_MISMATCH]: (columnName: string, sourceType: string, targetType: string) =>
    `Data type mismatch for column "${columnName}": ${sourceType} → ${targetType}`,
  
  // ==================== TOPOLOGY-RELATED ====================
  [ValidationErrorCode.DISCONNECTED_NODE]: (nodeName: string) =>
    `Node "${nodeName}" is disconnected from the main flow`,
  
  [ValidationErrorCode.DEAD_END]: (nodeName: string) =>
    `Node "${nodeName}" is a dead end (no outgoing connections)`,
  
  // ==================== ETL-SPECIFIC ====================
  [ValidationErrorCode.INVALID_ETL_CONNECTION]: (sourceCat: string, targetCat: string) =>
    `Invalid ETL connection: ${sourceCat} → ${targetCat}`,
  
  [ValidationErrorCode.MULTIPLE_INPUTS_DISALLOWED]: (nodeName: string, maxInputs: number) =>
    `Node "${nodeName}" cannot accept more than ${maxInputs} input(s)`,
  
  [ValidationErrorCode.FAN_OUT_DISALLOWED]: (nodeName: string) =>
    `Node "${nodeName}" cannot have multiple outputs without branching component`,
  
  [ValidationErrorCode.INVALID_MERGE_NODE_INPUTS]: (nodeName: string, required: number, current: number) =>
    `Merge node "${nodeName}" requires ${required} inputs, found ${current}`,
  
  [ValidationErrorCode.INVALID_BRANCHING_NODE_OUTPUTS]: (nodeName: string, minOutputs: number) =>
    `Branching node "${nodeName}" must have at least ${minOutputs} outputs`,
  
  [ValidationErrorCode.SOURCE_TO_SOURCE_DISALLOWED]: (sourceName: string, targetName: string) =>
    `Source "${sourceName}" cannot connect to source "${targetName}"`,
  
  [ValidationErrorCode.SINK_TO_SINK_DISALLOWED]: (sourceName: string, targetName: string) =>
    `Sink "${sourceName}" cannot connect to sink "${targetName}"`,
  
  [ValidationErrorCode.OUTPUT_TO_OUTPUT_DISALLOWED]: () =>
    'Output port cannot connect to output port',
  
  [ValidationErrorCode.INPUT_TO_INPUT_DISALLOWED]: () =>
    'Input port cannot connect to input port',
  
  [ValidationErrorCode.TOO_MANY_INPUTS]: (nodeName: string, maxInputs: number, current: number) =>
    `Node "${nodeName}" has ${current} inputs, maximum is ${maxInputs}`,
  
  [ValidationErrorCode.TOO_FEW_INPUTS]: (nodeName: string, minInputs: number, current: number) =>
    `Node "${nodeName}" has ${current} inputs, minimum required is ${minInputs}`,
  
  [ValidationErrorCode.EXACT_INPUT_COUNT_REQUIRED]: (nodeName: string, required: number, current: number) =>
    `Node "${nodeName}" requires exactly ${required} inputs, found ${current}`,
  
  [ValidationErrorCode.OUTPUT_PORT_REUSE_DISALLOWED]: (portId: string) =>
    `Output port "${portId}" is already connected`,
  
  [ValidationErrorCode.INPUT_PORT_MULTIPLE_CONNECTIONS]: (portId: string) =>
    `Input port "${portId}" already has a connection`,
  
  [ValidationErrorCode.MERGE_NODE_MISSING_INPUTS]: (nodeName: string) =>
    `Merge node "${nodeName}" requires multiple inputs`,
  
  [ValidationErrorCode.NON_MERGE_NODE_MULTIPLE_INPUTS]: (nodeName: string) =>
    `Non-merge node "${nodeName}" cannot accept multiple inputs`,
  
  [ValidationErrorCode.BRANCHING_REQUIRED_FOR_FAN_OUT]: (nodeName: string) =>
    `Fan-out requires branching component, but "${nodeName}" is not a branching component`,
  
  [ValidationErrorCode.SOURCE_COMPONENT_INPUTS]: (nodeName: string) =>
    `Source component "${nodeName}" should not have incoming connections`,
  
  [ValidationErrorCode.PROCESSING_NODE_MISSING_INPUTS]: (nodeName: string) =>
    `Processing component "${nodeName}" is missing input connections`,
  
  [ValidationErrorCode.SINK_COMPONENT_OUTPUTS]: (nodeName: string) =>
    `Sink component "${nodeName}" should not have outgoing connections`,
  
  [ValidationErrorCode.PROCESSING_NODE_MISSING_OUTPUTS]: (nodeName: string) =>
    `Processing component "${nodeName}" is missing output connections`
};

/**
 * Validation fix suggestions
 */
export const FixSuggestions = {
  [ValidationErrorCode.CYCLE_DETECTED]: 'Remove one of the edges in the cycle',
  
  [ValidationErrorCode.INVALID_CONNECTION_TYPE]: 'Connect to a compatible node type',
  
  [ValidationErrorCode.MAX_INCOMING_EXCEEDED]: 'Remove some incoming connections or increase the limit',
  
  [ValidationErrorCode.MAX_OUTGOING_EXCEEDED]: 'Remove some outgoing connections or increase the limit',
  
  [ValidationErrorCode.SCHEMA_INCOMPATIBILITY]: 'Add a transformation node between incompatible nodes',
  
  [ValidationErrorCode.DISCONNECTED_NODE]: 'Connect the node to the main flow or remove it',
  
  [ValidationErrorCode.DEAD_END]: 'Add an output node or connect to downstream processing',
  
  // ==================== ETL-SPECIFIC FIXES ====================
  [ValidationErrorCode.INVALID_ETL_CONNECTION]: 'Connect to a compatible ETL component type',
  
  [ValidationErrorCode.MULTIPLE_INPUTS_DISALLOWED]: 'Use a merge component (tJoin, tMap, tUnite) to combine multiple inputs',
  
  [ValidationErrorCode.FAN_OUT_DISALLOWED]: 'Insert a tReplicate component to split the flow',
  
  [ValidationErrorCode.INVALID_MERGE_NODE_INPUTS]: 'Connect the required number of inputs to this merge component',
  
  [ValidationErrorCode.SOURCE_TO_SOURCE_DISALLOWED]: 'Connect source to processing or merge components',
  
  [ValidationErrorCode.SINK_TO_SINK_DISALLOWED]: 'Sink components should only receive connections',
  
  [ValidationErrorCode.OUTPUT_TO_OUTPUT_DISALLOWED]: 'Connect output ports to input ports only',
  
  [ValidationErrorCode.INPUT_TO_INPUT_DISALLOWED]: 'Connect input ports to output ports only',
  
  [ValidationErrorCode.TOO_MANY_INPUTS]: 'Reduce the number of input connections or use a merge component',
  
  [ValidationErrorCode.TOO_FEW_INPUTS]: 'Add more input connections',
  
  [ValidationErrorCode.EXACT_INPUT_COUNT_REQUIRED]: 'Connect exactly the required number of inputs',
  
  [ValidationErrorCode.OUTPUT_PORT_REUSE_DISALLOWED]: 'Use a branching component for multiple outputs',
  
  [ValidationErrorCode.INPUT_PORT_MULTIPLE_CONNECTIONS]: 'Remove duplicate connection or use a merge component',
  
  [ValidationErrorCode.MERGE_NODE_MISSING_INPUTS]: 'Connect multiple sources to this merge component',
  
  [ValidationErrorCode.NON_MERGE_NODE_MULTIPLE_INPUTS]: 'Use a merge component to combine inputs before connecting',
  
  [ValidationErrorCode.BRANCHING_REQUIRED_FOR_FAN_OUT]: 'Insert a tReplicate component between these nodes',
  
  [ValidationErrorCode.SOURCE_COMPONENT_INPUTS]: 'Consider if this should be a processing component instead',
  
  [ValidationErrorCode.PROCESSING_NODE_MISSING_INPUTS]: 'Connect a source or upstream processing component',
  
  [ValidationErrorCode.SINK_COMPONENT_OUTPUTS]: 'Remove outgoing connections or change component type',
  
  [ValidationErrorCode.PROCESSING_NODE_MISSING_OUTPUTS]: 'Connect to a downstream processing, merge, or sink component'
};