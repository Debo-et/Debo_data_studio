// src/validation/validationRules.ts

import {
  ValidationRule,
  GraphNode,
  GraphEdge,
  GraphState,
  ValidationErrorCode,
  ValidationResult
} from './types';
import { ValidationResultFactory, ValidationMessages, FixSuggestions } from './validationResult';
import { SchemaRegistry } from './schemaRegistry';
import { CycleDetector } from './cycleDetector';
import { ETLConnectivityRule } from './ETLConnectivityRule';
import { BaseValidationRule } from './baseValidationRule';

/**
 * Base validation rule with common functionality
 */

/**
 * Cycle Detection Rule
 */
export class CycleDetectionRule extends BaseValidationRule {
  id = 'cycle-detection';
  name = 'Cycle Detection';
  description = 'Detects cycles in the graph';
  severity: 'error' | 'warning' = 'error';
  
  private cycleDetector = new CycleDetector();
  
  appliesTo(_node: GraphNode, _edge: GraphEdge, graph: GraphState): boolean {
    return graph.edges.length > 0;
  }
  
  validate(_node: GraphNode, _edge: GraphEdge, graph: GraphState): ValidationResult[] {
    const results: ValidationResult[] = [];
    const cycleResult = this.cycleDetector.detectCycles(graph, new Map());
    
    if (cycleResult.hasCycle) {
      cycleResult.cycleDetails.forEach(detail => {
        const nodeNames = detail.cycle.map(nodeId => {
          const node = this.getNodeById(nodeId, graph);
          return node?.data.name || nodeId;
        });
        
        results.push(
          ValidationResultFactory.createError(
            ValidationErrorCode.CYCLE_DETECTED,
            ValidationMessages[ValidationErrorCode.CYCLE_DETECTED](nodeNames),
            detail.cycle,
            detail.edgeIds,
            `Cycle detected: ${detail.cycle.join(' → ')}`,
            FixSuggestions[ValidationErrorCode.CYCLE_DETECTED],
            { cycle: detail.cycle }
          )
        );
      });
    }
    
    return results;
  }
}

/**
 * Node Type Compatibility Rule
 */
export class NodeTypeCompatibilityRule extends BaseValidationRule {
  id = 'node-type-compatibility';
  name = 'Node Type Compatibility';
  description = 'Validates that node types can connect to each other';
  severity: 'error' | 'warning' = 'error';
  
  constructor(private schemaRegistry: SchemaRegistry) {
    super();
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
    
    // Check if connection is allowed by schema registry
    const connectionCheck = this.schemaRegistry.isConnectionAllowed(
      sourceNode.type,
      targetNode.type
    );
    
    if (!connectionCheck.allowed) {
      results.push(
        ValidationResultFactory.createError(
          ValidationErrorCode.INVALID_CONNECTION_TYPE,
          ValidationMessages[ValidationErrorCode.INVALID_CONNECTION_TYPE](
            sourceNode.type,
            targetNode.type
          ),
          [sourceNode.id, targetNode.id],
          [edge.id],
          `Connection from ${sourceNode.type} to ${targetNode.type} is prohibited`,
          FixSuggestions[ValidationErrorCode.INVALID_CONNECTION_TYPE],
          {
            sourceType: sourceNode.type,
            targetType: targetNode.type,
            rule: connectionCheck.rule
          }
        )
      );
    }
    
    // Check source node schema
    const sourceSchema = this.getNodeSchema(sourceNode, this.schemaRegistry);
    if (sourceSchema) {
      if (!sourceSchema.allowedTargetTypes.includes(targetNode.type)) {
        results.push(
          ValidationResultFactory.createError(
            ValidationErrorCode.INVALID_TARGET_TYPE,
            ValidationMessages[ValidationErrorCode.INVALID_TARGET_TYPE](
              targetNode.type,
              sourceSchema.allowedTargetTypes
            ),
            [sourceNode.id, targetNode.id],
            [edge.id],
            `Node type "${sourceNode.type}" cannot connect to "${targetNode.type}"`,
            `Connect to one of: ${sourceSchema.allowedTargetTypes.join(', ')}`,
            {
              sourceType: sourceNode.type,
              targetType: targetNode.type,
              allowedTargetTypes: sourceSchema.allowedTargetTypes
            }
          )
        );
      }
    }
    
    // Check target node schema
    const targetSchema = this.getNodeSchema(targetNode, this.schemaRegistry);
    if (targetSchema) {
      if (!targetSchema.allowedSourceTypes.includes(sourceNode.type)) {
        results.push(
          ValidationResultFactory.createError(
            ValidationErrorCode.INVALID_SOURCE_TYPE,
            ValidationMessages[ValidationErrorCode.INVALID_SOURCE_TYPE](
              sourceNode.type,
              targetSchema.allowedSourceTypes
            ),
            [sourceNode.id, targetNode.id],
            [edge.id],
            `Node type "${targetNode.type}" cannot accept connections from "${sourceNode.type}"`,
            `Acceptable source types: ${targetSchema.allowedSourceTypes.join(', ')}`,
            {
              sourceType: sourceNode.type,
              targetType: targetNode.type,
              allowedSourceTypes: targetSchema.allowedSourceTypes
            }
          )
        );
      }
    }
    
    return results;
  }
}

/**
 * Cardinality Validation Rule
 */
export class CardinalityRule extends BaseValidationRule {
  id = 'cardinality-validation';
  name = 'Cardinality Validation';
  description = 'Validates connection limits per node and port';
  severity: 'error' | 'warning' = 'error';
  
  constructor(private schemaRegistry: SchemaRegistry) {
    super();
  }
  
  appliesTo(node: GraphNode, _edge: GraphEdge, _graph: GraphState): boolean {
    return !!node;
  }
  
  validate(node: GraphNode, _edge: GraphEdge, graph: GraphState): ValidationResult[] {
    const results: ValidationResult[] = [];
    const schema = this.getNodeSchema(node, this.schemaRegistry);
    
    if (!schema) return results;
    
    // Check incoming connections
    if (schema.maxIncomingConnections !== null) {
      const incomingCount = this.countIncomingConnections(node.id, graph);
      if (incomingCount > schema.maxIncomingConnections) {
        results.push(
          ValidationResultFactory.createError(
            ValidationErrorCode.MAX_INCOMING_EXCEEDED,
            ValidationMessages[ValidationErrorCode.MAX_INCOMING_EXCEEDED](
              node.data.name,
              schema.maxIncomingConnections,
              incomingCount
            ),
            [node.id],
            [],
            `Node has ${incomingCount} incoming connections, maximum is ${schema.maxIncomingConnections}`,
            FixSuggestions[ValidationErrorCode.MAX_INCOMING_EXCEEDED],
            {
              maxIncoming: schema.maxIncomingConnections,
              currentIncoming: incomingCount,
              nodeType: node.type
            }
          )
        );
      }
    }
    
    // Check outgoing connections
    if (schema.maxOutgoingConnections !== null) {
      const outgoingCount = this.countOutgoingConnections(node.id, graph);
      if (outgoingCount > schema.maxOutgoingConnections) {
        results.push(
          ValidationResultFactory.createError(
            ValidationErrorCode.MAX_OUTGOING_EXCEEDED,
            ValidationMessages[ValidationErrorCode.MAX_OUTGOING_EXCEEDED](
              node.data.name,
              schema.maxOutgoingConnections,
              outgoingCount
            ),
            [node.id],
            [],
            `Node has ${outgoingCount} outgoing connections, maximum is ${schema.maxOutgoingConnections}`,
            FixSuggestions[ValidationErrorCode.MAX_OUTGOING_EXCEEDED],
            {
              maxOutgoing: schema.maxOutgoingConnections,
              currentOutgoing: outgoingCount,
              nodeType: node.type
            }
          )
        );
      }
    }
    
    // Check port-specific limits
    if (schema.portRules) {
      Object.entries(schema.portRules).forEach(([portId, rule]) => {
        const portConnections = this.countPortConnections(portId, graph);
        if (rule.maxConnections !== null && portConnections > rule.maxConnections) {
          results.push(
            ValidationResultFactory.createError(
              ValidationErrorCode.PORT_CONNECTION_LIMIT,
              ValidationMessages[ValidationErrorCode.PORT_CONNECTION_LIMIT](
                portId,
                rule.maxConnections
              ),
              [node.id],
              [],
              `Port "${portId}" has ${portConnections} connections, maximum is ${rule.maxConnections}`,
              'Remove some connections from this port',
              {
                portId,
                maxConnections: rule.maxConnections,
                currentConnections: portConnections,
                portType: rule.portType
              }
            )
          );
        }
      });
    }
    
    return results;
  }
}

/**
 * Schema Compatibility Rule
 */
export class SchemaCompatibilityRule extends BaseValidationRule {
  id = 'schema-compatibility';
  name = 'Schema Compatibility';
  description = 'Validates data schema compatibility between connected nodes';
  severity: 'error' | 'warning' = 'error';
  
  appliesTo(_node: GraphNode, edge: GraphEdge, _graph: GraphState): boolean {
    return !!edge.source && !!edge.target;
  }
  
  validate(_node: GraphNode, edge: GraphEdge, graph: GraphState): ValidationResult[] {
    const results: ValidationResult[] = [];
    
    const sourceNode = this.getNodeById(edge.source, graph);
    const targetNode = this.getNodeById(edge.target, graph);
    
    if (!sourceNode || !targetNode) return results;
    
    const sourceSchema = sourceNode.data.schema;
    const targetSchema = targetNode.data.schema;
    
    // Only validate if both nodes have schemas
    if (!sourceSchema || !targetSchema) return results;
    
    // Check column compatibility
    const sourceColumns = new Map(sourceSchema.columns.map(col => [col.name, col]));
    const targetColumns = new Map(targetSchema.columns.map(col => [col.name, col]));
    
    // Check required columns in target
    targetSchema.columns
      .filter(col => !col.nullable && !sourceColumns.has(col.name))
      .forEach(col => {
        results.push(
          ValidationResultFactory.createError(
            ValidationErrorCode.REQUIRED_COLUMN_MISSING,
            `Required column "${col.name}" is missing from source`,
            [sourceNode.id, targetNode.id],
            [edge.id],
            `Target requires column "${col.name}" but it's not available in source`,
            `Add column "${col.name}" to source or make it nullable in target`,
            {
              columnName: col.name,
              columnType: col.type,
              sourceNode: sourceNode.data.name,
              targetNode: targetNode.data.name
            }
          )
        );
      });
    
    // Check data type compatibility for mapped columns
    if (edge.data?.mapping) {
      Object.entries(edge.data.mapping).forEach(([sourceCol, targetCol]) => {
        const sourceColDef = sourceColumns.get(sourceCol);
        const targetColDef = targetColumns.get(targetCol);
        
        if (sourceColDef && targetColDef) {
          if (!this.areDataTypesCompatible(sourceColDef.type, targetColDef.type)) {
            results.push(
              ValidationResultFactory.createError(
                ValidationErrorCode.DATA_TYPE_MISMATCH,
                ValidationMessages[ValidationErrorCode.DATA_TYPE_MISMATCH](
                  sourceCol,
                  sourceColDef.type,
                  targetColDef.type
                ),
                [sourceNode.id, targetNode.id],
                [edge.id],
                `Column "${sourceCol}" (${sourceColDef.type}) cannot be mapped to "${targetCol}" (${targetColDef.type})`,
                'Add a data type conversion or use compatible columns',
                {
                  sourceColumn: sourceCol,
                  targetColumn: targetCol,
                  sourceType: sourceColDef.type,
                  targetType: targetColDef.type
                }
              )
            );
          }
        }
      });
    }
    
    // Check foreign key constraints
    if (targetSchema.foreignKeys) {
      targetSchema.foreignKeys.forEach(fk => {
        const sourceHasColumn = sourceColumns.has(fk.column);
        if (!sourceHasColumn) {
          results.push(
            ValidationResultFactory.createWarning(
              ValidationErrorCode.SCHEMA_INCOMPATIBILITY,
              `Foreign key column "${fk.column}" not found in source`,
              [sourceNode.id, targetNode.id],
              [edge.id],
              `Target expects foreign key "${fk.column}" referencing ${fk.referenceTable}.${fk.referenceColumn}`,
              'Add the foreign key column or adjust the schema',
              {
                foreignKey: fk,
                sourceNode: sourceNode.data.name,
                targetNode: targetNode.data.name
              }
            )
          );
        }
      });
    }
    
    return results;
  }
  
  private areDataTypesCompatible(sourceType: string, targetType: string): boolean {
    const compatibilityMap: Record<string, string[]> = {
      'string': ['string', 'text', 'varchar', 'char'],
      'number': ['number', 'int', 'integer', 'float', 'decimal', 'double'],
      'boolean': ['boolean', 'bool', 'bit'],
      'date': ['date', 'datetime', 'timestamp'],
      'any': ['string', 'number', 'boolean', 'date', 'any']
    };
    
    const sourceTypes = compatibilityMap[sourceType.toLowerCase()] || [sourceType];
    const targetTypes = compatibilityMap[targetType.toLowerCase()] || [targetType];
    
    return sourceTypes.some(st => targetTypes.includes(st));
  }
}

/**
 * Graph Topology Rule
 */
export class GraphTopologyRule extends BaseValidationRule {
  id = 'graph-topology';
  name = 'Graph Topology';
  description = 'Validates overall graph structure and connectivity';
  severity: 'error' | 'warning' = 'warning';
  
  appliesTo(_node: GraphNode, _edge: GraphEdge, graph: GraphState): boolean {
    return graph.nodes.length > 0;
  }
  
  validate(_node: GraphNode, _edge: GraphEdge, graph: GraphState): ValidationResult[] {
    const results: ValidationResult[] = [];
    
    // Find disconnected nodes (no incoming or outgoing connections)
    const connectedNodeIds = new Set<string>();
    
    graph.edges.forEach(edge => {
      connectedNodeIds.add(edge.source);
      connectedNodeIds.add(edge.target);
    });
    
    graph.nodes.forEach(node => {
      if (!connectedNodeIds.has(node.id)) {
        results.push(
          ValidationResultFactory.createWarning(
            ValidationErrorCode.DISCONNECTED_NODE,
            ValidationMessages[ValidationErrorCode.DISCONNECTED_NODE](node.data.name),
            [node.id],
            [],
            `Node "${node.data.name}" is not connected to any other node`,
            FixSuggestions[ValidationErrorCode.DISCONNECTED_NODE],
            {
              nodeType: node.type,
              nodeCategory: node.data.componentCategory
            }
          )
        );
      }
    });
    
    // Find dead ends (nodes with no outgoing connections)
    const nodesWithOutgoing = new Set(graph.edges.map(edge => edge.source));
    
    graph.nodes.forEach(node => {
      if (!nodesWithOutgoing.has(node.id)) {
        // Check if it's supposed to have outgoing connections
        if (node.data.componentCategory !== 'output') {
          results.push(
            ValidationResultFactory.createWarning(
              ValidationErrorCode.DEAD_END,
              ValidationMessages[ValidationErrorCode.DEAD_END](node.data.name),
              [node.id],
              [],
              `Node "${node.data.name}" has no outgoing connections`,
              FixSuggestions[ValidationErrorCode.DEAD_END],
              {
                nodeType: node.type,
                nodeCategory: node.data.componentCategory
              }
            )
          );
        }
      }
    });
    
    return results;
  }
}

/**
 * ETL Connectivity Rule (Already imported and defined separately)
 * This rule is imported from './ETLConnectivityRule'
 */

/**
 * Factory for creating all built-in validation rules
 */
export class ValidationRuleFactory {
  static createAllRules(schemaRegistry: SchemaRegistry): ValidationRule[] {
    return [
      new CycleDetectionRule(),
      new NodeTypeCompatibilityRule(schemaRegistry),
      new CardinalityRule(schemaRegistry),
      new SchemaCompatibilityRule(),
      new GraphTopologyRule(),
      new ETLConnectivityRule() // ETL connectivity validation
    ];
  }
  
  static createRuleById(id: string, schemaRegistry: SchemaRegistry): ValidationRule | null {
    const rules = this.createAllRules(schemaRegistry);
    return rules.find(rule => rule.id === id) || null;
  }
  
  /**
   * Create rules specifically for ETL validation
   */
  static createETLValidationRules(schemaRegistry: SchemaRegistry): ValidationRule[] {
    return [
      new ETLConnectivityRule(),
      new NodeTypeCompatibilityRule(schemaRegistry),
      new CardinalityRule(schemaRegistry),
      new CycleDetectionRule()
    ];
  }
  
  /**
   * Create rules for basic validation (non-ETL)
   */
  static createBasicValidationRules(schemaRegistry: SchemaRegistry): ValidationRule[] {
    return [
      new CycleDetectionRule(),
      new NodeTypeCompatibilityRule(schemaRegistry),
      new CardinalityRule(schemaRegistry),
      new GraphTopologyRule()
    ];
  }
}