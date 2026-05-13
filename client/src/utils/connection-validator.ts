// src/utils/connection-validator.ts

// REMOVED: import { describe, it } from 'node:test'; // This is the problematic line
import {
  CanvasNode,
  CanvasConnection,
  ConnectionPort,
  PortType,
  PortSide,
  PostgreSQLDataType,
  ConnectionValidationResult,
  SchemaMapping,
  PostgresColumn,
  ConnectionStatus
} from '../types/pipeline-types';

// ==================== ERROR TYPES ====================

/**
 * Base validation error
 */
class ValidationError extends Error {
  constructor(
    message: string,
    public readonly errorCode: string,
    public readonly severity: 'ERROR' | 'WARNING' | 'INFO' = 'ERROR'
  ) {
    super(message);
    this.name = 'ValidationError';
  }
}

/**
 * Port compatibility error
 */
class PortCompatibilityError extends ValidationError {
  constructor(
    message: string,
    public readonly sourcePortId: string,
    public readonly targetPortId: string,
    public readonly violationType: 'TYPE_MISMATCH' | 'SELF_CONNECTION' | 'MAX_CONNECTIONS' | 'PORT_DIRECTION'
  ) {
    super(message, `PORT_${violationType}`);
  }
}

/**
 * Data type compatibility error
 */
class DataTypeCompatibilityError extends ValidationError {
  constructor(
    message: string,
    public readonly sourceType: PostgreSQLDataType,
    public readonly targetType: PostgreSQLDataType,
    public readonly columnName?: string
  ) {
    super(message, 'DATA_TYPE_INCOMPATIBLE');
  }
}

/**
 * Schema compatibility error
 */
class SchemaCompatibilityError extends ValidationError {
  constructor(
    message: string,
    public readonly sourceColumn?: string,
    public readonly targetColumn?: string,
    public readonly constraintType?: 'NULLABLE' | 'PRIMARY_KEY' | 'UNIQUE' | 'CHECK'
  ) {
    super(message, 'SCHEMA_INCOMPATIBLE');
  }
}

/**
 * Circular dependency error
 */
class CircularDependencyError extends ValidationError {
  constructor(
    message: string,
    public readonly cyclePath: string[]
  ) {
    super(message, 'CIRCULAR_DEPENDENCY');
  }
}

// ==================== TYPES & INTERFACES ====================

/**
 * Connection validation context
 */
export interface ValidationContext {
  nodes: Map<string, CanvasNode>;
  connections: Map<string, CanvasConnection>;
  existingConnections: CanvasConnection[];
}

/**
 * Port compatibility result
 */
export interface PortCompatibilityResult {
  isValid: boolean;
  errors: PortCompatibilityError[];
  warnings: string[];
}

/**
 * Validation issue interface for UI display
 */
export interface ValidationIssue {
  code: string;
  message: string;
  severity: 'ERROR' | 'WARNING' | 'INFO';
  suggestion?: string;
}

/**
 * Data type compatibility matrix
 */
type TypeCompatibilityMatrix = {
  [source in PostgreSQLDataType]?: {
    [target in PostgreSQLDataType]?: {
      compatible: boolean;
      requiresCast: boolean;
      castExpression?: string;
      dataLossRisk: 'NONE' | 'LOW' | 'MEDIUM' | 'HIGH';
      notes?: string;
    };
  };
};

// ==================== DATA TYPE COMPATIBILITY MATRIX ====================

/**
 * PostgreSQL data type compatibility matrix
 * Defines valid type conversions with risk assessment
 */
const POSTGRES_TYPE_COMPATIBILITY: TypeCompatibilityMatrix = {
  // INTEGER family
  [PostgreSQLDataType.INTEGER]: {
    [PostgreSQLDataType.INTEGER]: { compatible: true, requiresCast: false, dataLossRisk: 'NONE' },
    [PostgreSQLDataType.BIGINT]: { compatible: true, requiresCast: true, castExpression: '::bigint', dataLossRisk: 'NONE' },
    [PostgreSQLDataType.DECIMAL]: { compatible: true, requiresCast: true, castExpression: '::decimal', dataLossRisk: 'NONE' },
    [PostgreSQLDataType.NUMERIC]: { compatible: true, requiresCast: true, castExpression: '::numeric', dataLossRisk: 'NONE' },
    [PostgreSQLDataType.REAL]: { compatible: true, requiresCast: true, castExpression: '::real', dataLossRisk: 'LOW' },
    [PostgreSQLDataType.DOUBLE_PRECISION]: { compatible: true, requiresCast: true, castExpression: '::double precision', dataLossRisk: 'LOW' },
    [PostgreSQLDataType.VARCHAR]: { compatible: true, requiresCast: true, castExpression: '::varchar', dataLossRisk: 'NONE' },
    [PostgreSQLDataType.TEXT]: { compatible: true, requiresCast: true, castExpression: '::text', dataLossRisk: 'NONE' }
  },
  
  // BIGINT family
  [PostgreSQLDataType.BIGINT]: {
    [PostgreSQLDataType.BIGINT]: { compatible: true, requiresCast: false, dataLossRisk: 'NONE' },
    [PostgreSQLDataType.DECIMAL]: { compatible: true, requiresCast: true, castExpression: '::decimal', dataLossRisk: 'NONE' },
    [PostgreSQLDataType.NUMERIC]: { compatible: true, requiresCast: true, castExpression: '::numeric', dataLossRisk: 'NONE' },
    [PostgreSQLDataType.VARCHAR]: { compatible: true, requiresCast: true, castExpression: '::varchar', dataLossRisk: 'NONE' },
    [PostgreSQLDataType.TEXT]: { compatible: true, requiresCast: true, castExpression: '::text', dataLossRisk: 'NONE' },
    [PostgreSQLDataType.INTEGER]: { compatible: true, requiresCast: true, castExpression: '::integer', dataLossRisk: 'HIGH', notes: 'Risk of overflow' }
  },
  
  // DECIMAL/NUMERIC family
  [PostgreSQLDataType.DECIMAL]: {
    [PostgreSQLDataType.DECIMAL]: { compatible: true, requiresCast: false, dataLossRisk: 'NONE' },
    [PostgreSQLDataType.NUMERIC]: { compatible: true, requiresCast: false, dataLossRisk: 'NONE' },
    [PostgreSQLDataType.REAL]: { compatible: true, requiresCast: true, castExpression: '::real', dataLossRisk: 'MEDIUM' },
    [PostgreSQLDataType.DOUBLE_PRECISION]: { compatible: true, requiresCast: true, castExpression: '::double precision', dataLossRisk: 'LOW' },
    [PostgreSQLDataType.VARCHAR]: { compatible: true, requiresCast: true, castExpression: '::varchar', dataLossRisk: 'NONE' },
    [PostgreSQLDataType.TEXT]: { compatible: true, requiresCast: true, castExpression: '::text', dataLossRisk: 'NONE' }
  },
  
  // VARCHAR/TEXT family
  [PostgreSQLDataType.VARCHAR]: {
    [PostgreSQLDataType.VARCHAR]: { compatible: true, requiresCast: false, dataLossRisk: 'NONE' },
    [PostgreSQLDataType.TEXT]: { compatible: true, requiresCast: true, castExpression: '::text', dataLossRisk: 'NONE' },
    [PostgreSQLDataType.CHAR]: { compatible: true, requiresCast: true, castExpression: '::char', dataLossRisk: 'LOW' },
    [PostgreSQLDataType.INTEGER]: { compatible: true, requiresCast: true, castExpression: '::integer', dataLossRisk: 'HIGH', notes: 'Requires valid integer string' },
    [PostgreSQLDataType.DATE]: { compatible: true, requiresCast: true, castExpression: '::date', dataLossRisk: 'HIGH', notes: 'Requires valid date format' },
    [PostgreSQLDataType.TIMESTAMP]: { compatible: true, requiresCast: true, castExpression: '::timestamp', dataLossRisk: 'HIGH', notes: 'Requires valid timestamp format' }
  },
  
  // DATE/TIMESTAMP family
  [PostgreSQLDataType.DATE]: {
    [PostgreSQLDataType.DATE]: { compatible: true, requiresCast: false, dataLossRisk: 'NONE' },
    [PostgreSQLDataType.TIMESTAMP]: { compatible: true, requiresCast: true, castExpression: '::timestamp', dataLossRisk: 'NONE' },
    [PostgreSQLDataType.TIMESTAMPTZ]: { compatible: true, requiresCast: true, castExpression: '::timestamptz', dataLossRisk: 'NONE' },
    [PostgreSQLDataType.VARCHAR]: { compatible: true, requiresCast: true, castExpression: '::varchar', dataLossRisk: 'NONE' },
    [PostgreSQLDataType.TEXT]: { compatible: true, requiresCast: true, castExpression: '::text', dataLossRisk: 'NONE' }
  },
  
  // JSON family
  [PostgreSQLDataType.JSON]: {
    [PostgreSQLDataType.JSON]: { compatible: true, requiresCast: false, dataLossRisk: 'NONE' },
    [PostgreSQLDataType.JSONB]: { compatible: true, requiresCast: true, castExpression: '::jsonb', dataLossRisk: 'NONE' },
    [PostgreSQLDataType.TEXT]: { compatible: true, requiresCast: true, castExpression: '::text', dataLossRisk: 'NONE' },
    [PostgreSQLDataType.VARCHAR]: { compatible: true, requiresCast: true, castExpression: '::varchar', dataLossRisk: 'NONE' }
  },
  
  // BOOLEAN
  [PostgreSQLDataType.BOOLEAN]: {
    [PostgreSQLDataType.BOOLEAN]: { compatible: true, requiresCast: false, dataLossRisk: 'NONE' },
    [PostgreSQLDataType.INTEGER]: { compatible: true, requiresCast: true, castExpression: 'CASE WHEN ? THEN 1 ELSE 0 END', dataLossRisk: 'NONE' },
    [PostgreSQLDataType.VARCHAR]: { compatible: true, requiresCast: true, castExpression: 'CASE WHEN ? THEN \'true\' ELSE \'false\' END', dataLossRisk: 'NONE' },
    [PostgreSQLDataType.TEXT]: { compatible: true, requiresCast: true, castExpression: 'CASE WHEN ? THEN \'true\' ELSE \'false\' END', dataLossRisk: 'NONE' }
  }
};

// ==================== PURE UTILITY FUNCTIONS ====================

/**
 * Pure function to get a port from a node
 */
const getPortFromNode = (node: CanvasNode, portId: string): ConnectionPort | null => {
  return node.connectionPorts?.find(port => port.id === portId) || null;
};

/**
 * Pure function to check if connection is to self
 */
const isSelfConnection = (
  sourceNodeId: string,
  targetNodeId: string
): boolean => {
  return sourceNodeId === targetNodeId;
};

/**
 * Pure function to count existing connections for a port
 */
const countPortConnections = (
  portId: string,
  connections: CanvasConnection[],
  isSource: boolean
): number => {
  return connections.filter(conn => 
    isSource ? conn.sourcePortId === portId : conn.targetPortId === portId
  ).length;
};

// ==================== CONNECTION VALIDATOR CLASS ====================

export class ConnectionValidator {
  // Private constructor to prevent instantiation (pure static class)
  private constructor() {}

  /**
   * Validate port compatibility
   * - Ensures output ports connect only to input ports
   * - Prevents self-connections
   * - Enforces maximum connections per port
   */
  static validatePortCompatibility(
    sourceNode: CanvasNode,
    sourcePortId: string,
    targetNode: CanvasNode,
    targetPortId: string,
    existingConnections: CanvasConnection[]
  ): PortCompatibilityResult {
    const errors: PortCompatibilityError[] = [];
    const warnings: string[] = [];

    // Get ports
    const sourcePort = getPortFromNode(sourceNode, sourcePortId);
    const targetPort = getPortFromNode(targetNode, targetPortId);

    if (!sourcePort) {
      errors.push(new PortCompatibilityError(
        `Source port ${sourcePortId} not found in node ${sourceNode.name}`,
        sourcePortId,
        targetPortId,
        'TYPE_MISMATCH'
      ));
    }

    if (!targetPort) {
      errors.push(new PortCompatibilityError(
        `Target port ${targetPortId} not found in node ${targetNode.name}`,
        sourcePortId,
        targetPortId,
        'TYPE_MISMATCH'
      ));
    }

    if (!sourcePort || !targetPort) {
      return { isValid: false, errors, warnings };
    }

    // 1. Check port direction compatibility
    if (sourcePort.type !== PortType.OUTPUT) {
      errors.push(new PortCompatibilityError(
        `Source port must be OUTPUT type, got ${sourcePort.type}`,
        sourcePortId,
        targetPortId,
        'PORT_DIRECTION'
      ));
    }

    if (targetPort.type !== PortType.INPUT) {
      errors.push(new PortCompatibilityError(
        `Target port must be INPUT type, got ${targetPort.type}`,
        sourcePortId,
        targetPortId,
        'PORT_DIRECTION'
      ));
    }

    // 2. Check for self-connection
    if (isSelfConnection(sourceNode.id, targetNode.id)) {
      errors.push(new PortCompatibilityError(
        `Cannot connect node to itself: ${sourceNode.name}`,
        sourcePortId,
        targetPortId,
        'SELF_CONNECTION'
      ));
    }

    // 3. Check maximum connections for source port
    const sourceConnections = countPortConnections(sourcePortId, existingConnections, true);
    if (sourcePort.maxConnections && sourceConnections >= sourcePort.maxConnections) {
      errors.push(new PortCompatibilityError(
        `Source port ${sourcePortId} has reached maximum connections (${sourcePort.maxConnections})`,
        sourcePortId,
        targetPortId,
        'MAX_CONNECTIONS'
      ));
    }

    // 4. Check maximum connections for target port
    const targetConnections = countPortConnections(targetPortId, existingConnections, false);
    if (targetPort.maxConnections && targetConnections >= targetPort.maxConnections) {
      errors.push(new PortCompatibilityError(
        `Target port ${targetPortId} has reached maximum connections (${targetPort.maxConnections})`,
        sourcePortId,
        targetPortId,
        'MAX_CONNECTIONS'
      ));
    }

    // 5. Check for duplicate connection
    const isDuplicate = existingConnections.some(conn =>
      conn.sourceNodeId === sourceNode.id &&
      conn.sourcePortId === sourcePortId &&
      conn.targetNodeId === targetNode.id &&
      conn.targetPortId === targetPortId
    );

    if (isDuplicate) {
      warnings.push(`Connection already exists between ${sourceNode.name}:${sourcePortId} and ${targetNode.name}:${targetPortId}`);
    }

    // 6. Check port side compatibility (optional optimization)
    if (sourcePort.side === PortSide.RIGHT && targetPort.side === PortSide.LEFT) {
      // Good flow: right → left
    } else if (sourcePort.side === PortSide.BOTTOM && targetPort.side === PortSide.TOP) {
      // Good flow: bottom → top
    } else {
      warnings.push(`Unusual port alignment: ${sourcePort.side} → ${targetPort.side}. Consider reorienting nodes.`);
    }

    return {
      isValid: errors.length === 0,
      errors,
      warnings
    };
  }

  /**
   * Validate data type compatibility between source and target columns
   * Returns compatibility score (0-100) with suggested casts
   */
  static validateDataTypeCompatibility(
    sourceType: PostgreSQLDataType,
    targetType: PostgreSQLDataType,
    _columnName?: string
  ): {
    compatible: boolean;
    score: number;
    requiresCast: boolean;
    castExpression?: string;
    dataLossRisk: 'NONE' | 'LOW' | 'MEDIUM' | 'HIGH';
    notes?: string;
  } {
    // Same type - perfect compatibility
    if (sourceType === targetType) {
      return {
        compatible: true,
        score: 100,
        requiresCast: false,
        dataLossRisk: 'NONE'
      };
    }

    // Check compatibility matrix
    const sourceCompatibility = POSTGRES_TYPE_COMPATIBILITY[sourceType];
    if (!sourceCompatibility) {
      return {
        compatible: false,
        score: 0,
        requiresCast: false,
        dataLossRisk: 'HIGH',
        notes: `Source type ${sourceType} not supported for compatibility checking`
      };
    }

    const targetCompatibility = sourceCompatibility[targetType];
    if (!targetCompatibility) {
      return {
        compatible: false,
        score: 0,
        requiresCast: false,
        dataLossRisk: 'HIGH',
        notes: `Cannot convert ${sourceType} to ${targetType}`
      };
    }

    // Calculate compatibility score based on data loss risk
    const riskScoreMap = {
      'NONE': 100,
      'LOW': 80,
      'MEDIUM': 60,
      'HIGH': 30
    };

    return {
      compatible: targetCompatibility.compatible,
      score: riskScoreMap[targetCompatibility.dataLossRisk],
      requiresCast: targetCompatibility.requiresCast,
      castExpression: targetCompatibility.castExpression,
      dataLossRisk: targetCompatibility.dataLossRisk,
      notes: targetCompatibility.notes
    };
  }

  /**
   * Validate schema compatibility between source and target tables
   * Compares column names, types, and constraints
   */
  static validateSchemaCompatibility(
    sourceColumns: PostgresColumn[],
    targetColumns: PostgresColumn[]
  ): {
    isValid: boolean;
    compatibilityScore: number;
    errors: SchemaCompatibilityError[];
    warnings: string[];
    suggestedMappings: SchemaMapping[];
    typeCompatibility: Array<{
      sourceColumn: string;
      sourceType: PostgreSQLDataType;
      targetColumn: string;
      targetType: PostgreSQLDataType;
      isCompatible: boolean;
      suggestedConversion?: string;
    }>;
  } {
    const errors: SchemaCompatibilityError[] = [];
    const warnings: string[] = [];
    const suggestedMappings: SchemaMapping[] = [];
    const typeCompatibility: Array<{
      sourceColumn: string;
      sourceType: PostgreSQLDataType;
      targetColumn: string;
      targetType: PostgreSQLDataType;
      isCompatible: boolean;
      suggestedConversion?: string;
    }> = [];

    // Create maps for quick lookup
    const sourceColumnMap = new Map(sourceColumns.map(col => [col.name.toLowerCase(), col]));
    const targetColumnMap = new Map(targetColumns.map(col => [col.name.toLowerCase(), col]));

    let compatibleColumns = 0;
    const totalColumns = Math.max(sourceColumns.length, targetColumns.length);

    // 1. Check for exact name matches
    for (const sourceCol of sourceColumns) {
      const targetCol = targetColumnMap.get(sourceCol.name.toLowerCase());
      
      if (targetCol) {
        // Check data type compatibility
        const typeCompat = this.validateDataTypeCompatibility(
          sourceCol.dataType,
          targetCol.dataType,
          sourceCol.name
        );

        typeCompatibility.push({
          sourceColumn: sourceCol.name,
          sourceType: sourceCol.dataType,
          targetColumn: targetCol.name,
          targetType: targetCol.dataType,
          isCompatible: typeCompat.compatible,
          suggestedConversion: typeCompat.castExpression
        });

        if (typeCompat.compatible) {
          compatibleColumns++;
          
          // Check NULL constraint compatibility
          if (sourceCol.nullable && !targetCol.nullable) {
            errors.push(new SchemaCompatibilityError(
              `Column ${sourceCol.name} is nullable in source but NOT NULL in target`,
              sourceCol.name,
              targetCol.name,
              'NULLABLE'
            ));
          }

          // Check PRIMARY KEY constraint
          if (sourceCol.isPrimaryKey && !targetCol.isPrimaryKey) {
            warnings.push(`Column ${sourceCol.name} is PRIMARY KEY in source but not in target`);
          }

          suggestedMappings.push({
            sourceColumn: sourceCol.name,
            targetColumn: targetCol.name,
            transformation: typeCompat.requiresCast ? typeCompat.castExpression : undefined,
            isRequired: !sourceCol.nullable
          });
        } else {
          errors.push(new DataTypeCompatibilityError(
            `Incompatible types for column ${sourceCol.name}: ${sourceCol.dataType} → ${targetCol.dataType}`,
            sourceCol.dataType,
            targetCol.dataType,
            sourceCol.name
          ));
        }
      } else {
        warnings.push(`Source column ${sourceCol.name} has no matching target column`);
      }
    }

    // 2. Check for unmapped target columns
    for (const targetCol of targetColumns) {
      if (!sourceColumnMap.has(targetCol.name.toLowerCase())) {
        if (!targetCol.nullable && !targetCol.defaultValue) {
          errors.push(new SchemaCompatibilityError(
            `Required target column ${targetCol.name} has no source mapping and no default value`,
            undefined,
            targetCol.name,
            'NULLABLE'
          ));
        } else {
          warnings.push(`Target column ${targetCol.name} has no source mapping`);
        }
      }
    }

    // 3. Suggest mappings based on name similarity (fuzzy matching)
    if (sourceColumnMap.size > 0 && targetColumnMap.size > 0) {
      for (const sourceCol of sourceColumns) {
        if (!targetColumnMap.has(sourceCol.name.toLowerCase())) {
          // Find similar column names
          const similarColumns = targetColumns.filter(targetCol => {
            const sourceName = sourceCol.name.toLowerCase();
            const targetName = targetCol.name.toLowerCase();
            
            // Simple similarity check (could be enhanced with Levenshtein distance)
            return sourceName.includes(targetName) || 
                   targetName.includes(sourceName) ||
                   sourceName.replace(/_/g, '') === targetName.replace(/_/g, '');
          });

          if (similarColumns.length === 1) {
            const targetCol = similarColumns[0];
            const typeCompat = this.validateDataTypeCompatibility(
              sourceCol.dataType,
              targetCol.dataType,
              sourceCol.name
            );

            if (typeCompat.compatible) {
              suggestedMappings.push({
                sourceColumn: sourceCol.name,
                targetColumn: targetCol.name,
                transformation: typeCompat.requiresCast ? typeCompat.castExpression : undefined,
                isRequired: !sourceCol.nullable
              });
              warnings.push(`Suggested mapping: ${sourceCol.name} → ${targetCol.name} (similar names)`);
            }
          }
        }
      }
    }

    // Calculate compatibility score
    const compatibilityScore = totalColumns > 0 
      ? Math.round((compatibleColumns / totalColumns) * 100)
      : 100;

    const isValid = errors.length === 0;

    return {
      isValid,
      compatibilityScore,
      errors,
      warnings,
      suggestedMappings,
      typeCompatibility
    };
  }

  /**
   * Detect connection cycles using DFS
   * Returns list of cycles found in the graph
   */
  static detectConnectionCycles(
    nodes: CanvasNode[],
    connections: CanvasConnection[]
  ): {
    hasCycles: boolean;
    cycles: string[][];
    topologicalOrder: string[];
    errors: CircularDependencyError[];
  } {
    const errors: CircularDependencyError[] = [];
    const cycles: string[][] = [];
    const topologicalOrder: string[] = [];

    // Build adjacency list
    const adjacencyList = new Map<string, string[]>();
    const nodeIds = new Set<string>();

    // Initialize adjacency list
    nodes.forEach(node => {
      nodeIds.add(node.id);
      adjacencyList.set(node.id, []);
    });

    // Build edges (source → target)
    connections.forEach(conn => {
      const neighbors = adjacencyList.get(conn.sourceNodeId) || [];
      neighbors.push(conn.targetNodeId);
      adjacencyList.set(conn.sourceNodeId, neighbors);
    });

    // Kahn's algorithm for topological sort and cycle detection
    const inDegree = new Map<string, number>();
    
    // Calculate in-degree for each node
    nodeIds.forEach(nodeId => {
      inDegree.set(nodeId, 0);
    });

    connections.forEach(conn => {
      const currentInDegree = inDegree.get(conn.targetNodeId) || 0;
      inDegree.set(conn.targetNodeId, currentInDegree + 1);
    });

    // Queue for nodes with in-degree 0
    const queue: string[] = [];
    inDegree.forEach((degree, nodeId) => {
      if (degree === 0) {
        queue.push(nodeId);
      }
    });

    // Process queue
    while (queue.length > 0) {
      const nodeId = queue.shift()!;
      topologicalOrder.push(nodeId);

      const neighbors = adjacencyList.get(nodeId) || [];
      for (const neighbor of neighbors) {
        const newInDegree = (inDegree.get(neighbor) || 0) - 1;
        inDegree.set(neighbor, newInDegree);
        
        if (newInDegree === 0) {
          queue.push(neighbor);
        }
      }
    }

    // Check for cycles (nodes not in topological order)
    if (topologicalOrder.length !== nodeIds.size) {
      // Find nodes in cycles
      const cycleNodes = new Set<string>();
      inDegree.forEach((degree, nodeId) => {
        if (degree > 0) {
          cycleNodes.add(nodeId);
        }
      });

      // Find actual cycles using DFS on remaining nodes
      const visited = new Set<string>();
      const recursionStack = new Set<string>();

      const dfsFindCycle = (nodeId: string, path: string[]): boolean => {
        if (recursionStack.has(nodeId)) {
          // Found a cycle
          const cycleStart = path.indexOf(nodeId);
          const cycle = path.slice(cycleStart);
          cycles.push([...cycle, nodeId]);
          return true;
        }

        if (visited.has(nodeId)) {
          return false;
        }

        visited.add(nodeId);
        recursionStack.add(nodeId);
        path.push(nodeId);

        const neighbors = adjacencyList.get(nodeId) || [];
        for (const neighbor of neighbors) {
          if (cycleNodes.has(neighbor)) {
            if (dfsFindCycle(neighbor, path)) {
              return true;
            }
          }
        }

        recursionStack.delete(nodeId);
        path.pop();
        return false;
      };

      cycleNodes.forEach(nodeId => {
        if (!visited.has(nodeId)) {
          dfsFindCycle(nodeId, []);
        }
      });

      // Create errors for each cycle found
      cycles.forEach(cycle => {
        const cyclePath = cycle.map(id => {
          const node = nodes.find(n => n.id === id);
          return node ? node.name : id;
        });
        
        errors.push(new CircularDependencyError(
          `Circular dependency detected: ${cyclePath.join(' → ')}`,
          cycle
        ));
      });
    }

    return {
      hasCycles: cycles.length > 0,
      cycles,
      topologicalOrder,
      errors
    };
  }

  /**
   * Comprehensive validation of a new connection
   */
  static validateNewConnection(
    sourceNode: CanvasNode,
    sourcePortId: string,
    targetNode: CanvasNode,
    targetPortId: string,
    context: ValidationContext
  ): ConnectionValidationResult {
    const allErrors: (ValidationError | DataTypeCompatibilityError | SchemaCompatibilityError)[] = [];
    const allWarnings: string[] = [];
    let compatibilityScore = 100;

    // 1. Port compatibility validation
    const portResult = this.validatePortCompatibility(
      sourceNode,
      sourcePortId,
      targetNode,
      targetPortId,
      context.existingConnections
    );

    if (!portResult.isValid) {
      allErrors.push(...portResult.errors);
      compatibilityScore = Math.min(compatibilityScore, 30);
    }
    allWarnings.push(...portResult.warnings);

    // 2. Data type compatibility (if metadata available)
    const sourcePort = getPortFromNode(sourceNode, sourcePortId);
    const targetPort = getPortFromNode(targetNode, targetPortId);

    if (sourcePort?.dataType && targetPort?.dataType) {
      const typeResult = this.validateDataTypeCompatibility(
        sourcePort.dataType,
        targetPort.dataType
      );

      if (!typeResult.compatible) {
        allErrors.push(new DataTypeCompatibilityError(
          `Port data type incompatible: ${sourcePort.dataType} → ${targetPort.dataType}`,
          sourcePort.dataType,
          targetPort.dataType
        ));
        compatibilityScore = Math.min(compatibilityScore, typeResult.score);
      } else if (typeResult.score < 100) {
        allWarnings.push(`Data type conversion required: ${sourcePort.dataType} → ${targetPort.dataType} (risk: ${typeResult.dataLossRisk})`);
        compatibilityScore = Math.min(compatibilityScore, typeResult.score);
      }
    }

    // 3. Schema compatibility (if table metadata available)
    let schemaCompatibility = {
      compatibleColumns: 0,
      incompatibleColumns: 0,
      typeCompatibility: [] as any[]
    };

    if (sourceNode.metadata?.tableMapping && targetNode.metadata?.tableMapping) {
      const schemaResult = this.validateSchemaCompatibility(
        sourceNode.metadata.tableMapping.columns,
        targetNode.metadata.tableMapping.columns
      );

      if (!schemaResult.isValid) {
        allErrors.push(...schemaResult.errors);
      }

      allWarnings.push(...schemaResult.warnings);
      compatibilityScore = Math.min(compatibilityScore, schemaResult.compatibilityScore);
      schemaCompatibility = {
        compatibleColumns: schemaResult.typeCompatibility.filter(tc => tc.isCompatible).length,
        incompatibleColumns: schemaResult.typeCompatibility.filter(tc => !tc.isCompatible).length,
        typeCompatibility: schemaResult.typeCompatibility
      };
    }

    // 4. Cycle detection (with new hypothetical connection)
    const hypotheticalConnections = [
      ...context.existingConnections,
      {
        id: 'temp',
        sourceNodeId: sourceNode.id,
        sourcePortId,
        targetNodeId: targetNode.id,
        targetPortId,
        dataFlow: { schemaMappings: [] },
        status: ConnectionStatus.PENDING
      } as CanvasConnection
    ];

    const cycleResult = this.detectConnectionCycles(
      Array.from(context.nodes.values()),
      hypotheticalConnections
    );

    if (cycleResult.hasCycles) {
      allErrors.push(...cycleResult.errors);
      compatibilityScore = 0;
    }

    // 5. Performance implications (simplified)
    const performanceImplications = {
      estimatedLatencyMs: 0,
      potentialBottleneck: false,
      recommendations: [] as string[]
    };

    // Estimate latency based on node types and schema size
    if (sourceNode.metadata?.tableMapping) {
      const columnCount = sourceNode.metadata.tableMapping.columns.length;
      performanceImplications.estimatedLatencyMs = columnCount * 5; // Simplified estimation
      
      if (columnCount > 50) {
        performanceImplications.potentialBottleneck = true;
        performanceImplications.recommendations.push('Consider filtering columns before processing');
      }
    }

    return {
      isValid: allErrors.length === 0,
      compatibilityScore,
      errors: allErrors.map(err => err.message),
      warnings: allWarnings,
      info: cycleResult.hasCycles ? ['Cycle detection performed'] : [],
      schemaCompatibility,
      performanceImplications,
      timestamp: new Date().toISOString()
    };
  }

  /**
   * Batch validate all connections in a pipeline
   */
  static validatePipelineConnections(
    nodes: CanvasNode[],
    connections: CanvasConnection[]
  ): {
    valid: boolean;
    validationResults: Map<string, ConnectionValidationResult>;
    summary: {
      totalConnections: number;
      validConnections: number;
      invalidConnections: number;
      warnings: number;
      averageCompatibilityScore: number;
    };
  } {
    const validationResults = new Map<string, ConnectionValidationResult>();
    const nodeMap = new Map(nodes.map(node => [node.id, node]));
    
    let totalScore = 0;
    let validCount = 0;
    let warningCount = 0;

    for (const connection of connections) {
      const sourceNode = nodeMap.get(connection.sourceNodeId);
      const targetNode = nodeMap.get(connection.targetNodeId);

      if (!sourceNode || !targetNode) {
        const result: ConnectionValidationResult = {
          isValid: false,
          compatibilityScore: 0,
          errors: ['Source or target node not found'],
          warnings: [],
          info: [],
          schemaCompatibility: { compatibleColumns: 0, incompatibleColumns: 0, typeCompatibility: [] },
          performanceImplications: { estimatedLatencyMs: 0, potentialBottleneck: false, recommendations: [] },
          timestamp: new Date().toISOString()
        };
        validationResults.set(connection.id, result);
        continue;
      }

      const context: ValidationContext = {
        nodes: nodeMap,
        connections: new Map(connections.map(c => [c.id, c])),
        existingConnections: connections.filter(c => c.id !== connection.id)
      };

      const result = this.validateNewConnection(
        sourceNode,
        connection.sourcePortId,
        targetNode,
        connection.targetPortId,
        context
      );

      validationResults.set(connection.id, result);
      
      if (result.isValid) {
        validCount++;
      }
      
      totalScore += result.compatibilityScore;
      warningCount += result.warnings.length;
    }

    const averageScore = connections.length > 0 
      ? Math.round(totalScore / connections.length)
      : 100;

    return {
      valid: validCount === connections.length,
      validationResults,
      summary: {
        totalConnections: connections.length,
        validConnections: validCount,
        invalidConnections: connections.length - validCount,
        warnings: warningCount,
        averageCompatibilityScore: averageScore
      }
    };
  }
}

// ==================== REMOVED UNIT TEST STUBS ====================

// The following unit test code has been removed because it uses Node.js-specific
// testing modules that are not compatible with browser environments:

// import { describe, it } from 'node:test';
// describe('ConnectionValidator', () => { ... });
// 
// If you need unit tests, create a separate test file (e.g., connection-validator.test.ts)
// and use a testing framework compatible with your environment (Jest, Vitest, etc.).

// ==================== HELPER FUNCTIONS ====================

/**
 * Pure function to create a validation context
 */
export const createValidationContext = (
  nodes: CanvasNode[],
  connections: CanvasConnection[]
): ValidationContext => ({
  nodes: new Map(nodes.map(node => [node.id, node])),
  connections: new Map(connections.map(conn => [conn.id, conn])),
  existingConnections: connections
});

/**
 * Pure function to filter connections by node
 */
export const getNodeConnections = (
  nodeId: string,
  connections: CanvasConnection[]
): { incoming: CanvasConnection[]; outgoing: CanvasConnection[] } => {
  const incoming = connections.filter(conn => conn.targetNodeId === nodeId);
  const outgoing = connections.filter(conn => conn.sourceNodeId === nodeId);
  return { incoming, outgoing };
};

/**
 * Pure function to check if a connection already exists
 */
export const connectionExists = (
  sourceNodeId: string,
  sourcePortId: string,
  targetNodeId: string,
  targetPortId: string,
  connections: CanvasConnection[]
): boolean => {
  return connections.some(conn =>
    conn.sourceNodeId === sourceNodeId &&
    conn.sourcePortId === sourcePortId &&
    conn.targetNodeId === targetNodeId &&
    conn.targetPortId === targetPortId
  );
};

/**
 * Pure function to get the data type risk level
 */
export const getDataTypeRiskLevel = (
  sourceType: PostgreSQLDataType,
  targetType: PostgreSQLDataType
): 'SAFE' | 'WARNING' | 'DANGEROUS' => {
  const compat = POSTGRES_TYPE_COMPATIBILITY[sourceType]?.[targetType];
  if (!compat) return 'DANGEROUS';
  
  switch (compat.dataLossRisk) {
    case 'NONE': return 'SAFE';
    case 'LOW': return 'SAFE';
    case 'MEDIUM': return 'WARNING';
    case 'HIGH': return 'DANGEROUS';
    default: return 'DANGEROUS';
  }
};

// ==================== EXPORT ERROR TYPES ====================

// Export all error types for external use
export {
  ValidationError,
  PortCompatibilityError,
  DataTypeCompatibilityError,
  SchemaCompatibilityError,
  CircularDependencyError
};

// Note: ValidationIssue is already exported as an interface above
// Remove duplicate export to avoid conflict
export type { ConnectionValidationResult };