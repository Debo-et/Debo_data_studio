// src/validation/ETLConnectivityRule.ts

import {
  GraphNode,
  GraphEdge,
  GraphState,
  ValidationResult,
  ValidationErrorCode
} from './types';
import { ValidationResultFactory, ValidationMessages } from './validationResult';
import { BaseValidationRule } from './baseValidationRule';

/**
 * ETL Connectivity Rule
 * Enforces strict ETL component connectivity and topology rules
 */
export class ETLConnectivityRule extends BaseValidationRule {
  id = 'etl-connectivity';
  name = 'ETL Connectivity Validation';
  description = 'Validates ETL component connectivity, topology, and port usage rules';
  severity: 'error' | 'warning' = 'error';
  
  // Component classification mapping
  private componentClassification: Map<string, {
    etlCategory: 'source' | 'sink' | 'processing' | 'merge' | 'branching';
    isMultiInput?: boolean;
    isBranching?: boolean;
    exactInputs?: number;
    allowsUnlimitedInputs?: boolean;
    allowsLookupInputs?: boolean;
    allowsMultipleMainOutputs?: boolean;
    minInputs?: number;
    maxInputs?: number | null;
    minOutputs?: number;
    maxOutputs?: number | null;
  }> = new Map();
  
  constructor() {
    super();
    this.initializeComponentClassification();
  }
  
  private initializeComponentClassification(): void {
    // ==================== Data Source Components ====================
    // Examples: file-delimited, file-xml, mysql, oracle, directory/services
    // Can act as input OR output (entry or exit points only)
    this.componentClassification.set('excel', { 
      etlCategory: 'source',
      minInputs: 0,
      maxInputs: 1,
      minOutputs: 0,
      maxOutputs: 5
    });
    
    this.componentClassification.set('database', { 
      etlCategory: 'source',
      minInputs: 0,
      maxInputs: 1,
      minOutputs: 0,
      maxOutputs: 5
    });
    
    this.componentClassification.set('csv', { 
      etlCategory: 'source',
      minInputs: 0,
      maxInputs: 1,
      minOutputs: 0,
      maxOutputs: 5
    });
    
    this.componentClassification.set('delimited', { 
      etlCategory: 'source',
      minInputs: 0,
      maxInputs: 1,
      minOutputs: 0,
      maxOutputs: 5
    });
    
    this.componentClassification.set('xml', { 
      etlCategory: 'source',
      minInputs: 0,
      maxInputs: 1,
      minOutputs: 0,
      maxOutputs: 5
    });
    
    this.componentClassification.set('json', { 
      etlCategory: 'source',
      minInputs: 0,
      maxInputs: 1,
      minOutputs: 0,
      maxOutputs: 5
    });
    
    this.componentClassification.set('webservice', { 
      etlCategory: 'source',
      minInputs: 0,
      maxInputs: 1,
      minOutputs: 0,
      maxOutputs: 5
    });
    
    this.componentClassification.set('ldif', { 
      etlCategory: 'source',
      minInputs: 0,
      maxInputs: 1,
      minOutputs: 0,
      maxOutputs: 5
    });
    
    this.componentClassification.set('regex', { 
      etlCategory: 'source',
      minInputs: 0,
      maxInputs: 1,
      minOutputs: 0,
      maxOutputs: 5
    });
    
    // ==================== Data Sink Components ====================
    this.componentClassification.set('output', { 
      etlCategory: 'sink',
      minInputs: 1,
      maxInputs: 5,
      minOutputs: 0,
      maxOutputs: 0  // Sinks cannot have outputs
    });
    
    // ==================== Processing Components ====================
    // Default behavior: 1 main input, 1 main output
    // Examples: tSortRow, tFilterRow, tAggregateRow, tNormalize, tConvertType
    this.componentClassification.set('tsortrow', { 
      etlCategory: 'processing',
      minInputs: 1,
      maxInputs: 1,
      minOutputs: 1,
      maxOutputs: 1
    });
    
    this.componentClassification.set('tfilterrow', { 
      etlCategory: 'processing',
      minInputs: 1,
      maxInputs: 1,
      minOutputs: 1,
      maxOutputs: 2  // Can have reject output
    });
    
    this.componentClassification.set('taggregaterow', { 
      etlCategory: 'processing',
      minInputs: 1,
      maxInputs: 1,
      minOutputs: 1,
      maxOutputs: 1
    });
    
    this.componentClassification.set('tnormalize', { 
      etlCategory: 'processing',
      minInputs: 1,
      maxInputs: 1,
      minOutputs: 1,
      maxOutputs: 1
    });
    
    this.componentClassification.set('tconverttype', { 
      etlCategory: 'processing',
      minInputs: 1,
      maxInputs: 1,
      minOutputs: 1,
      maxOutputs: 1
    });
    
    this.componentClassification.set('map', { 
      etlCategory: 'processing',
      minInputs: 1,
      maxInputs: 1,
      minOutputs: 1,
      maxOutputs: 1
    });
    
    // ==================== Multi-Input / Merge Components ====================
    // tMap: 1 main + N lookup inputs, multiple outputs allowed
    this.componentClassification.set('tmap', { 
      etlCategory: 'merge',
      isMultiInput: true,
      allowsLookupInputs: true,
      allowsUnlimitedInputs: true,
      minInputs: 1,  // At least 1 main input
      maxInputs: null, // Unlimited inputs
      minOutputs: 1,
      maxOutputs: null  // Multiple outputs allowed
    });
    
    // tJoin: exactly 2 inputs (1 main + 1 lookup), 1 output
    this.componentClassification.set('tjoin', { 
      etlCategory: 'merge',
      isMultiInput: true,
      exactInputs: 2,  // Exactly 2 inputs
      minOutputs: 1,
      maxOutputs: 1
    });
    
    // tUnite, tFlowMerge, tMatchGroup: N inputs → 1 output
    this.componentClassification.set('tunite', { 
      etlCategory: 'merge',
      isMultiInput: true,
      allowsUnlimitedInputs: true,
      minInputs: 2,  // At least 2 inputs to make sense
      maxInputs: null,
      minOutputs: 1,
      maxOutputs: 1
    });
    
    this.componentClassification.set('tflowmerge', { 
      etlCategory: 'merge',
      isMultiInput: true,
      allowsUnlimitedInputs: true,
      minInputs: 2,
      maxInputs: null,
      minOutputs: 1,
      maxOutputs: 1
    });
    
    this.componentClassification.set('tmatchgroup', { 
      etlCategory: 'merge',
      isMultiInput: true,
      allowsUnlimitedInputs: true,
      minInputs: 1,
      maxInputs: null,
      minOutputs: 1,
      maxOutputs: 1
    });
    
    // ==================== Branching Components ====================
    // tReplicate: 1 input → N outputs
    this.componentClassification.set('treplicate', { 
      etlCategory: 'branching',
      isBranching: true,
      allowsMultipleMainOutputs: true,
      minInputs: 1,
      maxInputs: 1,
      minOutputs: 2,  // At least 2 outputs for branching
      maxOutputs: null  // Unlimited outputs
    });
    
    // Additional common components
    this.componentClassification.set('tfileinputdelimited', { etlCategory: 'source' });
    this.componentClassification.set('tfileinputxml', { etlCategory: 'source' });
    this.componentClassification.set('tfileinputjson', { etlCategory: 'source' });
    this.componentClassification.set('tfileoutputdelimited', { etlCategory: 'sink' });
    this.componentClassification.set('tfileoutputxml', { etlCategory: 'sink' });
    this.componentClassification.set('tfileoutputjson', { etlCategory: 'sink' });
    this.componentClassification.set('tmysqlinput', { etlCategory: 'source' });
    this.componentClassification.set('tmysqloutput', { etlCategory: 'sink' });
    this.componentClassification.set('toracleinput', { etlCategory: 'source' });
    this.componentClassification.set('toracleoutput', { etlCategory: 'sink' });
    this.componentClassification.set('tdirectorylist', { etlCategory: 'source' });
    this.componentClassification.set('tservices', { etlCategory: 'source' });
  }
  
  appliesTo(_node: GraphNode, edge: GraphEdge, _graph: GraphState): boolean {
    return !!edge.source && !!edge.target;
  }
  
  validate(_node: GraphNode, edge: GraphEdge, graph: GraphState): ValidationResult[] {
    const results: ValidationResult[] = [];
    
    const sourceNode = this.getNodeById(edge.source, graph);
    const targetNode = this.getNodeById(edge.target, graph);
    
    if (!sourceNode || !targetNode) {
      return results;
    }
    
    const sourceType = sourceNode.type.toLowerCase();
    const targetType = targetNode.type.toLowerCase();
    
    // Get component classifications
    const sourceClass = this.componentClassification.get(sourceType);
    const targetClass = this.componentClassification.get(targetType);
    
    // Skip validation for unclassified components
    if (!sourceClass || !targetClass) {
      return results;
    }
    
    // Validate ETL category-based connections
    results.push(...this.validateCategoryConnections(
      sourceNode, sourceClass, 
      targetNode, targetClass, 
      edge
    ));
    
    // Validate port usage rules
    results.push(...this.validatePortUsage(
      sourceNode, targetNode, edge, graph
    ));
    
    // Validate multi-input rules for target node
    results.push(...this.validateMultiInputRules(
      targetNode, targetClass, graph
    ));
    
    // Validate branching rules for source node
    results.push(...this.validateBranchingRules(
      sourceNode, sourceClass, graph
    ));
    
    return results;
  }
  
  /**
   * Validate connections based on ETL categories
   */
  private validateCategoryConnections(
    sourceNode: GraphNode,
    sourceClass: any,
    targetNode: GraphNode,
    targetClass: any,
    edge: GraphEdge
  ): ValidationResult[] {
    const results: ValidationResult[] = [];
    
    const sourceCat = sourceClass.etlCategory;
    const targetCat = targetClass.etlCategory;
    
    // ==================== Invalid Connections (Must Be Rejected) ====================
    
    // ❌ Source → Source
    if (sourceCat === 'source' && targetCat === 'source') {
      results.push(
        ValidationResultFactory.createError(
          ValidationErrorCode.SOURCE_TO_SOURCE_DISALLOWED,
          ValidationMessages[ValidationErrorCode.SOURCE_TO_SOURCE_DISALLOWED](
            sourceNode.data.name,
            targetNode.data.name
          ),
          [sourceNode.id, targetNode.id],
          [edge.id],
          `Data source components cannot connect to other data source components. Example: tFileInputDelimited → tFileInputCSV`,
          'Connect source to processing or merge components instead',
          {
            sourceType: sourceNode.type,
            targetType: targetNode.type,
            rule: 'source-to-source-disallowed'
          }
        )
      );
    }
    
    // ❌ Sink → Sink
    if (sourceCat === 'sink' && targetCat === 'sink') {
      results.push(
        ValidationResultFactory.createError(
          ValidationErrorCode.SINK_TO_SINK_DISALLOWED,
          ValidationMessages[ValidationErrorCode.SINK_TO_SINK_DISALLOWED](
            sourceNode.data.name,
            targetNode.data.name
          ),
          [sourceNode.id, targetNode.id],
          [edge.id],
          `Data sink components cannot connect to other data sink components. Example: tFileOutputDelimited → tMysqlOutput`,
          'Sink components should only receive connections, not make them',
          {
            sourceType: sourceNode.type,
            targetType: targetNode.type,
            rule: 'sink-to-sink-disallowed'
          }
        )
      );
    }
    
    // ❌ Output → Output (Port-level validation is handled elsewhere)
    // ❌ Input → Input (Port-level validation is handled elsewhere)
    
    // ==================== Validate Allowed Connections ====================
    const allowedTargets = this.getAllowedTargetCategories(sourceCat);
    if (!allowedTargets.includes(targetCat)) {
      results.push(
        ValidationResultFactory.createError(
          ValidationErrorCode.INVALID_ETL_CONNECTION,
          ValidationMessages[ValidationErrorCode.INVALID_ETL_CONNECTION](
            sourceCat,
            targetCat
          ),
          [sourceNode.id, targetNode.id],
          [edge.id],
          `ETL rules prohibit ${sourceCat} → ${targetCat} connections`,
          `Allowed targets for ${sourceCat} components: ${allowedTargets.join(', ')}`,
          {
            sourceCategory: sourceCat,
            targetCategory: targetCat,
            allowedTargets
          }
        )
      );
    }
    
    return results;
  }
  
  /**
   * Get allowed target categories for a source category
   */
  private getAllowedTargetCategories(sourceCategory: string): string[] {
    const rules: Record<string, string[]> = {
      // Source → Processing, Merge
      'source': ['processing', 'merge'],           
      
      // Processing → Processing, Merge, Sink
      'processing': ['processing', 'merge', 'sink'], 
      
      // Merge → Processing, Merge, Sink
      'merge': ['processing', 'merge', 'sink'],     
      
      // Branching → Processing, Merge, Sink
      'branching': ['processing', 'merge', 'sink'], 
      
      // Sink cannot connect to anything
      'sink': []                                   
    };
    
    return rules[sourceCategory] || [];
  }
  
  /**
   * Validate port usage rules
   */
  private validatePortUsage(
    sourceNode: GraphNode,
    targetNode: GraphNode,
    edge: GraphEdge,
    graph: GraphState
  ): ValidationResult[] {
    const results: ValidationResult[] = [];
    
    // Check if source port is already used
    if (edge.sourceHandle) {
      const sourcePortConnections = this.countPortConnectionsByHandle(edge.sourceHandle, graph);
      if (sourcePortConnections > 1) {
        // Only allow multiple connections from branching components
        const sourceClass = this.componentClassification.get(sourceNode.type.toLowerCase());
        if (!sourceClass?.isBranching) {
          results.push(
            ValidationResultFactory.createError(
              ValidationErrorCode.OUTPUT_PORT_REUSE_DISALLOWED,
              ValidationMessages[ValidationErrorCode.OUTPUT_PORT_REUSE_DISALLOWED](
                edge.sourceHandle
              ),
              [sourceNode.id],
              [edge.id],
              'Each output port may be used only once (except for branching components like tReplicate)',
              'Use tReplicate component for fan-out connections',
              {
                portId: edge.sourceHandle,
                currentConnections: sourcePortConnections,
                componentType: sourceNode.type
              }
            )
          );
        }
      }
    }
    
    // Check if target port already has a connection
    if (edge.targetHandle) {
      const targetPortConnections = this.countPortConnectionsByHandle(edge.targetHandle, graph);
      if (targetPortConnections > 1) {
        results.push(
          ValidationResultFactory.createError(
            ValidationErrorCode.INPUT_PORT_MULTIPLE_CONNECTIONS,
            ValidationMessages[ValidationErrorCode.INPUT_PORT_MULTIPLE_CONNECTIONS](
              edge.targetHandle
            ),
            [targetNode.id],
            [edge.id],
            'Each input port may accept only one connection',
            'Remove existing connection or use a merge component',
            {
              portId: edge.targetHandle,
              currentConnections: targetPortConnections,
              componentType: targetNode.type
            }
          )
        );
      }
    }
    
    return results;
  }
  
  /**
   * Count connections for a specific port handle
   */
  private countPortConnectionsByHandle(portHandle: string, graph: GraphState): number {
    return graph.edges.filter(edge => 
      edge.sourceHandle === portHandle || edge.targetHandle === portHandle
    ).length;
  }
  
  /**
   * Validate multi-input rules for merge components
   */
  private validateMultiInputRules(
    node: GraphNode,
    nodeClass: any,
    graph: GraphState
  ): ValidationResult[] {
    const results: ValidationResult[] = [];
    const incomingCount = this.countIncomingConnections(node.id, graph);
    
    // For merge components
    if (nodeClass.isMultiInput) {
      // Check exact input count (e.g., tJoin requires exactly 2)
      if (nodeClass.exactInputs && incomingCount > nodeClass.exactInputs) {
        results.push(
          ValidationResultFactory.createError(
            ValidationErrorCode.EXACT_INPUT_COUNT_REQUIRED,
            ValidationMessages[ValidationErrorCode.EXACT_INPUT_COUNT_REQUIRED](
              node.data.name,
              nodeClass.exactInputs,
              incomingCount
            ),
            [node.id],
            [],
            `${node.type} must have exactly ${nodeClass.exactInputs} input connections (1 main + 1 lookup)`,
            `Connect exactly ${nodeClass.exactInputs} sources to this component`,
            {
              requiredInputs: nodeClass.exactInputs,
              currentInputs: incomingCount,
              componentType: node.type
            }
          )
        );
      }
      
      // Check minimum inputs for merge components
      if (nodeClass.minInputs && incomingCount < nodeClass.minInputs) {
        results.push(
          ValidationResultFactory.createError(
            ValidationErrorCode.TOO_FEW_INPUTS,
            ValidationMessages[ValidationErrorCode.TOO_FEW_INPUTS](
              node.data.name,
              nodeClass.minInputs,
              incomingCount
            ),
            [node.id],
            [],
            `${node.type} requires at least ${nodeClass.minInputs} input connections`,
            `Connect at least ${nodeClass.minInputs} sources to this component`,
            {
              minInputs: nodeClass.minInputs,
              currentInputs: incomingCount,
              componentType: node.type
            }
          )
        );
      }
      
      // For tJoin specifically - validate exactly 2 inputs
      if (node.type.toLowerCase() === 'tjoin') {
        if (incomingCount !== 2) {
          results.push(
            ValidationResultFactory.createError(
              ValidationErrorCode.INVALID_MERGE_NODE_INPUTS,
              ValidationMessages[ValidationErrorCode.INVALID_MERGE_NODE_INPUTS](
                node.data.name,
                2,
                incomingCount
              ),
              [node.id],
              [],
              'tJoin must have exactly 2 input connections (1 main + 1 lookup)',
              'Connect exactly 2 sources to this tJoin component',
              {
                requiredInputs: 2,
                currentInputs: incomingCount,
                componentType: 'tJoin'
              }
            )
          );
        }
      }
      
      // For tMap - validate at least 1 input
      if (node.type.toLowerCase() === 'tmap') {
        if (incomingCount < 1) {
          results.push(
            ValidationResultFactory.createError(
              ValidationErrorCode.MERGE_NODE_MISSING_INPUTS,
              ValidationMessages[ValidationErrorCode.MERGE_NODE_MISSING_INPUTS](
                node.data.name
              ),
              [node.id],
              [],
              'tMap requires at least 1 main input connection',
              'Connect at least 1 source to this tMap component',
              {
                minInputs: 1,
                currentInputs: incomingCount,
                componentType: 'tMap'
              }
            )
          );
        }
      }
    } 
    // For non-merge components
    else if (!nodeClass.isMultiInput && incomingCount > 1) {
      results.push(
        ValidationResultFactory.createError(
          ValidationErrorCode.NON_MERGE_NODE_MULTIPLE_INPUTS,
          ValidationMessages[ValidationErrorCode.NON_MERGE_NODE_MULTIPLE_INPUTS](
            node.data.name
          ),
          [node.id],
          [],
          'Non-merge components can only accept one input connection',
          'Use a merge component (tJoin, tMap, tUnite) to combine multiple inputs',
          {
            maxAllowedInputs: 1,
            currentInputs: incomingCount,
            componentType: node.type
          }
        )
      );
    }
    
    return results;
  }
  
  /**
   * Validate branching rules
   */
  private validateBranchingRules(
    node: GraphNode,
    nodeClass: any,
    graph: GraphState
  ): ValidationResult[] {
    const results: ValidationResult[] = [];
    const outgoingCount = this.countOutgoingConnections(node.id, graph);
    
    // Check for fan-out without branching component
    if (outgoingCount > 1 && !nodeClass.isBranching) {
      results.push(
        ValidationResultFactory.createError(
          ValidationErrorCode.BRANCHING_REQUIRED_FOR_FAN_OUT,
          ValidationMessages[ValidationErrorCode.BRANCHING_REQUIRED_FOR_FAN_OUT](
            node.data.name
          ),
          [node.id],
          [],
          'Fan-out (one output to multiple inputs) requires a branching component like tReplicate',
          'Insert a tReplicate component to split the flow',
          {
            currentOutputs: outgoingCount,
            requiresBranching: true,
            componentType: node.type
          }
        )
      );
    }
    
    // Check minimum outputs for branching components
    if (nodeClass.isBranching && outgoingCount < 2) {
      results.push(
        ValidationResultFactory.createError(
          ValidationErrorCode.INVALID_BRANCHING_NODE_OUTPUTS,
          ValidationMessages[ValidationErrorCode.INVALID_BRANCHING_NODE_OUTPUTS](
            node.data.name,
            2
          ),
          [node.id],
          [],
          'Branching components like tReplicate must have at least 2 outputs',
          'Connect at least 2 downstream components',
          {
            minOutputs: 2,
            currentOutputs: outgoingCount,
            componentType: node.type
          }
        )
      );
    }
    
    return results;
  }
  
  /**
   * Validate overall graph topology for ETL rules
   */
  validateGraphTopology(graph: GraphState): ValidationResult[] {
    const results: ValidationResult[] = [];
    
    // Check each node's connections
    graph.nodes.forEach(node => {
      const nodeClass = this.componentClassification.get(node.type.toLowerCase());
      if (!nodeClass) return;
      
      const incomingCount = this.countIncomingConnections(node.id, graph);
      const outgoingCount = this.countOutgoingConnections(node.id, graph);
      
      // Validate input counts
      results.push(...this.validateNodeInputCount(node, nodeClass, incomingCount));
      
      // Validate output counts
      results.push(...this.validateNodeOutputCount(node, nodeClass, outgoingCount));
    });
    
    return results;
  }
  
  private validateNodeInputCount(
    node: GraphNode,
    nodeClass: any,
    incomingCount: number
  ): ValidationResult[] {
    const results: ValidationResult[] = [];
    
    // Source components typically shouldn't have incoming connections
    if (nodeClass.etlCategory === 'source' && incomingCount > 0) {
      results.push(
        ValidationResultFactory.createWarning(
          ValidationErrorCode.SOURCE_COMPONENT_INPUTS,
          ValidationMessages[ValidationErrorCode.SOURCE_COMPONENT_INPUTS](
            node.data.name
          ),
          [node.id],
          [],
          'Source components typically should not have incoming connections',
          'Consider if this component should be a processing or sink component instead'
        )
      );
    }
    
    // Processing components require exactly 1 input
    if (nodeClass.etlCategory === 'processing' && incomingCount !== 1) {
      results.push(
        ValidationResultFactory.createError(
          ValidationErrorCode.PROCESSING_NODE_MISSING_INPUTS,
          ValidationMessages[ValidationErrorCode.PROCESSING_NODE_MISSING_INPUTS](
            node.data.name
          ),
          [node.id],
          [],
          'Processing components require exactly one input connection',
          'Connect exactly one upstream component'
        )
      );
    }
    
    // Merge components should have multiple inputs
    if (nodeClass.etlCategory === 'merge' && incomingCount < 2) {
      results.push(
        ValidationResultFactory.createWarning(
          ValidationErrorCode.MERGE_NODE_MISSING_INPUTS,
          `Merge component "${node.data.name}" has only ${incomingCount} input(s)`,
          [node.id],
          [],
          'Merge components are designed to combine multiple inputs',
          'Consider connecting more sources or using a processing component instead'
        )
      );
    }
    
    // Branching components require exactly 1 input
    if (nodeClass.etlCategory === 'branching' && incomingCount !== 1) {
      results.push(
        ValidationResultFactory.createError(
          ValidationErrorCode.BRANCHING_NODE_INPUT_MISSING,
          `Branching component "${node.data.name}" must have exactly 1 input`,
          [node.id],
          [],
          'Branching components like tReplicate require exactly one input',
          'Connect exactly one upstream component'
        )
      );
    }
    
    // Sink components require at least 1 input
    if (nodeClass.etlCategory === 'sink' && incomingCount === 0) {
      results.push(
        ValidationResultFactory.createError(
          ValidationErrorCode.PROCESSING_NODE_MISSING_INPUTS,
          `Sink component "${node.data.name}" has no input connections`,
          [node.id],
          [],
          'Sink components require at least one input connection',
          'Connect an upstream component'
        )
      );
    }
    
    return results;
  }
  
  private validateNodeOutputCount(
    node: GraphNode,
    nodeClass: any,
    outgoingCount: number
  ): ValidationResult[] {
    const results: ValidationResult[] = [];
    
    // Source components should have outputs
    if (nodeClass.etlCategory === 'source' && outgoingCount === 0) {
      results.push(
        ValidationResultFactory.createWarning(
          ValidationErrorCode.PROCESSING_NODE_MISSING_OUTPUTS,
          `Source component "${node.data.name}" has no output connections`,
          [node.id],
          [],
          'Source components typically should have output connections',
          'Connect to a downstream processing, merge, or sink component'
        )
      );
    }
    
    // Processing components should have outputs
    if (nodeClass.etlCategory === 'processing' && outgoingCount === 0) {
      results.push(
        ValidationResultFactory.createWarning(
          ValidationErrorCode.PROCESSING_NODE_MISSING_OUTPUTS,
          ValidationMessages[ValidationErrorCode.PROCESSING_NODE_MISSING_OUTPUTS](
            node.data.name
          ),
          [node.id],
          [],
          'Processing components typically should have output connections',
          'Connect to a downstream processing, merge, or sink component'
        )
      );
    }
    
    // Merge components should have outputs
    if (nodeClass.etlCategory === 'merge' && outgoingCount === 0) {
      results.push(
        ValidationResultFactory.createError(
          ValidationErrorCode.MERGE_NODE_OUTPUT_MISSING,
          `Merge component "${node.data.name}" has no output connections`,
          [node.id],
          [],
          'Merge components must have at least one output connection',
          'Connect to a downstream component'
        )
      );
    }
    
    // Branching components should have multiple outputs
    if (nodeClass.etlCategory === 'branching' && outgoingCount < 2) {
      results.push(
        ValidationResultFactory.createWarning(
          ValidationErrorCode.INVALID_BRANCHING_NODE_OUTPUTS,
          `Branching component "${node.data.name}" has only ${outgoingCount} output(s)`,
          [node.id],
          [],
          'Branching components should have multiple outputs to split the flow',
          'Connect to multiple downstream components'
        )
      );
    }
    
    // Sink components should NOT have outputs
    if (nodeClass.etlCategory === 'sink' && outgoingCount > 0) {
      results.push(
        ValidationResultFactory.createError(
          ValidationErrorCode.SINK_COMPONENT_OUTPUTS,
          ValidationMessages[ValidationErrorCode.SINK_COMPONENT_OUTPUTS](
            node.data.name
          ),
          [node.id],
          [],
          'Sink components should not have outgoing connections',
          'Remove outgoing connections or change component type'
        )
      );
    }
    
    return results;
  }
  
  /**
   * Get component classification for a node
   */
  getComponentClassification(node: GraphNode): {
    etlCategory: 'source' | 'sink' | 'processing' | 'merge' | 'branching' | 'unknown';
    isMultiInput?: boolean;
    isBranching?: boolean;
  } {
    const nodeClass = this.componentClassification.get(node.type.toLowerCase());
    
    if (!nodeClass) {
      return { etlCategory: 'unknown' };
    }
    
    return {
      etlCategory: nodeClass.etlCategory,
      isMultiInput: nodeClass.isMultiInput,
      isBranching: nodeClass.isBranching
    };
  }
  
  /**
   * Check if a connection between two nodes would be valid
   */
  wouldConnectionBeValid(
    sourceNode: GraphNode,
    targetNode: GraphNode,
    graph: GraphState
  ): { isValid: boolean; errors: string[]; warnings: string[] } {
    const errors: string[] = [];
    const warnings: string[] = [];
    
    const sourceType = sourceNode.type.toLowerCase();
    const targetType = targetNode.type.toLowerCase();
    
    const sourceClass = this.componentClassification.get(sourceType);
    const targetClass = this.componentClassification.get(targetType);
    
    if (!sourceClass || !targetClass) {
      return { isValid: true, errors, warnings }; // Skip unclassified
    }
    
    const sourceCat = sourceClass.etlCategory;
    const targetCat = targetClass.etlCategory;
    
    // Check invalid connections
    if (sourceCat === 'source' && targetCat === 'source') {
      errors.push('Source components cannot connect to other source components');
    }
    
    if (sourceCat === 'sink' && targetCat === 'sink') {
      errors.push('Sink components cannot connect to other sink components');
    }
    
    // Check allowed connections
    const allowedTargets = this.getAllowedTargetCategories(sourceCat);
    if (!allowedTargets.includes(targetCat)) {
      errors.push(`Connection from ${sourceCat} to ${targetCat} is not allowed`);
    }
    
    // Check fan-out rules
    if (!sourceClass.isBranching) {
      const outgoingCount = this.countOutgoingConnections(sourceNode.id, graph);
      if (outgoingCount >= 1) {
        errors.push('Fan-out without tReplicate is forbidden. Use tReplicate for multiple outputs');
      }
    }
    
    // Check multi-input rules
    if (!targetClass.isMultiInput) {
      const incomingCount = this.countIncomingConnections(targetNode.id, graph);
      if (incomingCount >= 1) {
        errors.push('Multiple inputs into a non-merge component is forbidden');
      }
    }
    
    // Check tJoin specific rules
    if (targetType === 'tjoin') {
      const incomingCount = this.countIncomingConnections(targetNode.id, graph);
      if (incomingCount >= 2) {
        errors.push('tJoin can only accept exactly 2 inputs (1 main + 1 lookup)');
      }
    }
    
    return {
      isValid: errors.length === 0,
      errors,
      warnings
    };
  }
}