// src/pages/canvas.utils.ts
import { Edge, Node } from 'reactflow';
import { MarkerType } from 'reactflow';
import {
  ComponentConfiguration,
  NodeType,
  DataSourceType,
  PostgreSQLDataType,
  FieldSchema,
  DataType,
  PostgresColumn,
} from '../types/unified-pipeline.types';
import { GraphState, GraphNode as OriginalGraphNode, GraphEdge as OriginalGraphEdge } from '../validation/types';
import databaseApi from '../services/database-api.service';
import { CanvasNodeData } from './canvas.types';

// ==================== CONSTANTS ====================
export const DEFAULT_NODE_WIDTH = 200;
export const DEFAULT_NODE_HEIGHT = 120;

// ==================== COLUMN / SCHEMA UTILITIES ====================

/**
 * Convert DataType from metadata to PostgreSQLDataType
 */
export function mapDataTypeToPostgreSQL(type: DataType | string): PostgreSQLDataType {
  const upper = type.toString().toUpperCase();
  switch (upper) {
    case 'STRING': return PostgreSQLDataType.TEXT;
    case 'INTEGER': return PostgreSQLDataType.INTEGER;
    case 'DECIMAL': return PostgreSQLDataType.NUMERIC;
    case 'BOOLEAN': return PostgreSQLDataType.BOOLEAN;
    case 'DATE': return PostgreSQLDataType.DATE;
    case 'TIMESTAMP': return PostgreSQLDataType.TIMESTAMP;
    case 'BINARY': return PostgreSQLDataType.BYTEA;
    default: return PostgreSQLDataType.TEXT;
  }
}

/**
 * Convert FieldSchema[] to PostgresColumn[]
 */
export function fieldsToPostgresColumns(fields: FieldSchema[]): PostgresColumn[] {
  return fields.map(f => ({
    name: f.name,
    dataType: mapDataTypeToPostgreSQL(f.type),
    nullable: f.nullable,
    defaultValue: f.defaultValue,
    length: f.length,
    precision: f.precision,
    scale: f.scale,
    isPrimaryKey: f.isKey,
    comment: f.description
  }));
}

/**
 * Extract columns from drag metadata (flexible)
 */
export function extractColumnsFromDragData(metadata: any): any[] {
  if (!metadata) return [];
  if (Array.isArray(metadata.columns)) return metadata.columns;
  if (Array.isArray(metadata.schema)) return metadata.schema;
  if (Array.isArray(metadata.fields)) return metadata.fields;
  if (Array.isArray(metadata.extractedColumns)) return metadata.extractedColumns;
  if (metadata.repositoryMetadata?.schema && Array.isArray(metadata.repositoryMetadata.schema)) return metadata.repositoryMetadata.schema;
  if (metadata.fullRepositoryMetadata?.schema && Array.isArray(metadata.fullRepositoryMetadata.schema)) return metadata.fullRepositoryMetadata.schema;
  if (metadata.schema && Array.isArray(metadata.schema)) return metadata.schema;
  if (metadata.dragMetadata?.repositoryMetadata?.schema && Array.isArray(metadata.dragMetadata.repositoryMetadata.schema)) return metadata.dragMetadata.repositoryMetadata.schema;
  return [];
}

/**
 * Get repository metadata from a node (nested)
 */
export function getNodeRepositoryMetadata(node: Node<CanvasNodeData>): any {
  return node.data?.metadata?.fullRepositoryMetadata ||
         node.data?.metadata?.repositoryMetadata ||
         node.data?.metadata?.dragMetadata?.repositoryMetadata;
}

/**
 * Get a sensible table name from a node
 */
export function getNodeTableName(node: Node<CanvasNodeData>): string {
  const metadata = getNodeRepositoryMetadata(node);
  return metadata?.tableName || metadata?.name || node.data?.name || node.id;
}

/**
 * Extract columns from a node's output schema or repository metadata
 */
export function extractColumnsFromNode(node: Node<CanvasNodeData>): Array<{
  id: string;
  name: string;
  type: string;
  isKey: boolean;
  expression?: string;
  originalMetadata?: any;
}> {
  if (!node?.data) return [];
  const repoMetadata = getNodeRepositoryMetadata(node);
  if (repoMetadata?.columns && Array.isArray(repoMetadata.columns)) {
    return repoMetadata.columns.map((col: any, index: number) => ({
      id: col.id || `${node.id}_${col.name}_${index}`,
      name: col.name || `Column_${index + 1}`,
      type: col.type || 'string',
      isKey: col.isKey || col.primaryKey || false,
      expression: col.expression,
      originalMetadata: col
    }));
  }
  const data = node.data;
  if (data.metadata?.schemas?.output?.fields) {
    return data.metadata.schemas.output.fields.map((field: FieldSchema, index: number) => ({
      id: field.id || `${node.id}_${field.name}_${index}`,
      name: field.name || `Field_${index + 1}`,
      type: field.type,
      isKey: field.isKey || false,
      expression: field.description
    }));
  }
  return [];
}

/**
 * Get connected nodes (input or output)
 */
export function getConnectedNodes(
  nodeId: string,
  edges: Edge[],
  nodes: Node[],
  direction: 'input' | 'output'
): Node<CanvasNodeData>[] {
  const connectedNodeIds: string[] = [];
  if (direction === 'input') {
    edges.filter(edge => edge.target === nodeId).forEach(edge => {
      if (edge.source && !connectedNodeIds.includes(edge.source)) connectedNodeIds.push(edge.source);
    });
  } else {
    edges.filter(edge => edge.source === nodeId).forEach(edge => {
      if (edge.target && !connectedNodeIds.includes(edge.target)) connectedNodeIds.push(edge.target);
    });
  }
  return connectedNodeIds
    .map(id => nodes.find(n => n.id === id))
    .filter((node): node is Node<CanvasNodeData> => node !== undefined);
}

// ==================== INITIAL CONFIGURATION ====================

/**
 * Create initial component configuration based on component ID and role
 */
export function createInitialComponentConfiguration(
  componentId: string,
  role: 'INPUT' | 'OUTPUT' | 'TRANSFORM',
  metadata?: any
): ComponentConfiguration {
  const nodeId = `${componentId}_${Date.now()}`;
  const timestamp = new Date().toISOString();

  switch (componentId) {
    case 'tMap':
      return {
        type: 'MAP',
        config: {
          version: "1.0",
          transformations: [],
          joins: [],
          lookups: [],
          filters: [],
          variables: [],
          outputSchema: {
            id: `${nodeId}_output_schema`,
            name: 'Map Output Schema',
            alias: '',
            fields: [],
            isTemporary: false,
            isMaterialized: false,
            persistenceLevel: 'MEMORY',
            metadata: {}
          },
          sqlGeneration: {
            requiresDistinct: false,
            requiresAggregation: false,
            requiresWindowFunction: false,
            requiresSubquery: false,
            estimatedRowMultiplier: 1.0,
            parallelizable: true,
            batchSize: 1000,
            memoryHint: 'MEDIUM'
          },
          compilerMetadata: {
            lastModified: timestamp,
            createdBy: 'canvas',
            mappingCount: 0,
            validationStatus: 'VALID',
            warnings: [],
            dependencies: [],
            columnDependencies: {},
            compiledSql: undefined,
            compilationTimestamp: undefined
          }
        }
      };

    case 'tJoin':
      return {
        type: 'JOIN',
        config: {
          version: "1.0",
          joinType: 'INNER',
          joinConditions: [],
          joinHints: {
            enableJoinHint: false,
            joinHint: undefined,
            maxParallelism: undefined,
            memoryGrant: undefined
          },
          outputSchema: {
            fields: [],
            deduplicateFields: true,
            fieldAliases: {}
          },
          sqlGeneration: {
            joinAlgorithm: 'HASH',
            estimatedJoinCardinality: 1.0,
            nullHandling: 'INCLUDE',
            requiresSort: false,
            canParallelize: true
          },
          compilerMetadata: {
            lastModified: timestamp,
            joinCardinality: undefined,
            optimizationApplied: false,
            warnings: []
          }
        }
      };

    case 'tFilterRow':
      return {
        type: 'FILTER',
        config: {
          version: "1.0",
          filterConditions: [],
          filterLogic: 'AND',
          optimization: {
            pushDown: true,
            indexable: true,
            estimatedSelectivity: 1.0
          },
          sqlGeneration: {
            whereClause: '',
            parameterized: false,
            requiresSubquery: false,
            canUseIndex: true
          },
          compilerMetadata: {
            lastModified: timestamp,
            estimatedRowReduction: 0,
            warnings: []
          }
        }
      };

    case 'tLookup':
      return {
        type: 'LOOKUP',
        config: {
          version: "1.0",
          lookupType: 'SIMPLE',
          lookupKeyFields: [],
          lookupReturnFields: [],
          lookupTable: '',
          cache: {
            enabled: true,
            cacheSize: 1000,
            cacheType: 'LRU',
            ttlSeconds: undefined
          },
          fallback: {
            failOnMissing: false,
            defaultValue: undefined,
            defaultValueStrategy: 'NULL'
          },
          outputSchema: {
            fields: [],
            prefixLookupFields: false
          },
          sqlGeneration: {
            joinType: 'LEFT',
            requiresDistinct: false,
            estimatedCacheHitRate: 0.5,
            canParallelize: true,
            batchSize: 100
          },
          compilerMetadata: {
            lastModified: timestamp,
            cacheStatistics: undefined,
            warnings: []
          }
        }
      };

    case 'tAggregateRow':
      return {
        type: 'AGGREGATE',
        config: {
          version: "1.0",
          groupByFields: [],
          aggregateFunctions: [],
          havingConditions: undefined,
          optimization: {
            canUseIndex: true,
            requiresSort: true,
            estimatedGroupCount: 10,
            memoryHint: 'MEDIUM'
          },
          outputSchema: {
            fields: [],
            groupByFields: [],
            aggregateFields: []
          },
          sqlGeneration: {
            groupByClause: '',
            aggregateClause: '',
            havingClause: undefined,
            requiresWindowFunction: false,
            parallelizable: true,
            sortRequired: true
          },
          compilerMetadata: {
            lastModified: timestamp,
            estimatedCardinality: 0,
            warnings: []
          }
        }
      };

    case 'tSortRow':
      return {
        type: 'SORT',
        config: {
          version: "1.0",
          sortFields: [],
          performance: {
            estimatedRowCount: 1000,
            memoryRequired: undefined,
            canParallelize: false
          },
          sqlGeneration: {
            orderByClause: '',
            requiresDistinct: false,
            limitOffset: undefined
          },
          compilerMetadata: {
            lastModified: timestamp,
            sortComplexity: 'SIMPLE',
            warnings: []
          }
        }
      };

    default:
      // For input/output components
      if (role === 'INPUT') {
        return {
          type: 'INPUT',
          config: {
            version: "1.0",
            sourceType: DataSourceType.POSTGRESQL,
            sourceDetails: {
              connectionString: undefined,
              tableName: metadata?.postgresTableName || metadata?.tableName || metadata?.name || '',
              filePath: undefined,
              format: undefined,
              encoding: undefined,
              delimiter: undefined,
              hasHeader: undefined
            },
            pushdown: {
              enabled: false,
              filterClause: undefined,
              columnSelection: undefined,
              limit: undefined
            },
            schema: {
              id: `${nodeId}_input_schema`,
              name: 'Input Schema',
              alias: '',
              fields: [],
              isTemporary: false,
              isMaterialized: false,
              metadata: {}
            },
            sqlGeneration: {
              fromClause: '',
              alias: '',
              isTemporary: false,
              estimatedRowCount: 1000,
              parallelizable: true
            },
            compilerMetadata: {
              lastModified: timestamp,
              sourceValidated: false,
              warnings: []
            }
          }
        };
      } else if (role === 'OUTPUT') {
        return {
          type: 'OUTPUT',
          config: {
            version: "1.0",
            targetType: DataSourceType.POSTGRESQL,
            targetDetails: {
              connectionString: undefined,
              tableName: metadata?.postgresTableName || metadata?.tableName || metadata?.name || '',
              filePath: undefined,
              format: undefined,
              mode: 'APPEND'
            },
            writeOptions: {
              batchSize: 1000,
              commitInterval: undefined,
              truncateFirst: false,
              createTable: true
            },
            schemaMapping: [],
            sqlGeneration: {
              insertStatement: '',
              mergeStatement: undefined,
              requiresTransaction: true,
              parallelizable: true,
              batchOptimized: true
            },
            compilerMetadata: {
              lastModified: timestamp,
              targetValidated: false,
              warnings: []
            }
          }
        };
      }

      // Default transform component
      return {
        type: 'OTHER',
        config: {
          ...metadata?.configuration || {},
          version: "1.0",
          compilerMetadata: {
            lastModified: timestamp,
            warnings: []
          }
        }
      };
  }
}

// ==================== EDGE CREATION ====================

/**
 * Determine relation type based on target node type
 */
export function determineRelationType(
  _sourceNode: Node<CanvasNodeData>,
  targetNode: Node<CanvasNodeData>
): string {
  const targetType = targetNode.data?.type;
  if (targetType === NodeType.JOIN) return 'JOIN';
  if (targetType === NodeType.FILTER_ROW) return 'FILTER';
  if (targetType === NodeType.MAP) return 'MAPPING';
  if (targetType === NodeType.LOOKUP) return 'LOOKUP';
  if (targetType === NodeType.SPLIT_ROW) return 'SPLIT';
  return 'FLOW';
}

/**
 * Create default edge configuration based on relation type
 */
export function createDefaultEdgeConfig(
  relationType: string,
  sourceNode: Node<CanvasNodeData>,
  targetNode: Node<CanvasNodeData>
): Record<string, any> {
  const sourceFields = sourceNode.data?.metadata?.schemas?.output?.fields?.map(f => f.name) || [];
  const targetFields = targetNode.data?.metadata?.schemas?.input?.[0]?.fields?.map(f => f.name) || [];

  switch (relationType) {
    case 'JOIN':
      return {
        joinType: 'INNER',
        joinConditions: [],
        joinAlias: `join_${Date.now()}`
      };
    case 'FILTER':
      return {
        filterConditions: [],
        filterLogic: 'AND'
      };
    case 'MAPPING':
      const fieldMappings = sourceFields
        .filter(sourceField => targetFields.includes(sourceField))
        .map(sourceField => ({
          id: `mapping-${Date.now()}-${sourceField}`,
          sourceField,
          targetField: sourceField,
          transformation: undefined,
          isRequired: false
        }));
      return {
        mappingId: `mapping_${Date.now()}`,
        fieldMappings: fieldMappings.length > 0 ? fieldMappings : [],
        preserveUnmappedFields: true,
        strictMapping: false
      };
    case 'LOOKUP':
      return {
        lookupKeyFields: [],
        lookupReturnFields: [],
        lookupCacheSize: 1000,
        lookupFailOnMissing: false
      };
    case 'SPLIT':
      return {
        splitConditions: [],
        defaultOutput: 'main'
      };
    default:
      return {
        dataFlowOrder: 1,
        isConditional: false
      };
  }
}

/**
 * Create a new edge with rich metadata
 */
export function createEdgeWithMetadata(
  connection: {
    source: string;
    target: string;
    sourceHandle?: string | null;
    targetHandle?: string | null;
  },
  sourceNode: Node<CanvasNodeData>,
  targetNode: Node<CanvasNodeData>
): Edge {
  const edgeId = `edge-${connection.source}-${connection.target}-${Date.now()}`;
  const relationType = determineRelationType(sourceNode, targetNode);
  const sourceFields = sourceNode.data?.metadata?.schemas?.output?.fields?.map(f => f.name) || [];
  const targetFields = targetNode.data?.metadata?.schemas?.input?.[0]?.fields?.map(f => f.name) || [];

  const edgeMeta = {
    relationType,
    configuration: createDefaultEdgeConfig(relationType, sourceNode, targetNode),
    schemaValidation: {
      sourceFields,
      targetFields,
      isSchemaCompatible: sourceFields.length > 0 && targetFields.length > 0,
      validationErrors: [],
      fieldMapping: sourceFields.map((sourceField, index) => ({
        sourceField,
        targetField: targetFields[index] || sourceField,
        compatible: true,
        typeConversion: undefined
      }))
    },
    compilerMetadata: {
      edgeId,
      version: "1.0",
      dataFlow: {
        isConditional: false,
        conditionExpression: undefined,
        dataFlowOrder: 1,
        batchSize: undefined,
        parallelExecution: false
      },
      sqlGeneration: {
        joinCondition: undefined,
        filterCondition: undefined,
        mappingExpression: undefined,
        caseExpression: undefined
      },
      optimization: {
        canPushDown: true,
        canMerge: true,
        estimatedSelectivity: 1.0
      },
      lineage: {
        sourceNodeId: sourceNode.id,
        targetNodeId: targetNode.id,
        fieldsTransferred: sourceFields,
        transformationApplied: false
      }
    },
    metadata: {
      created: new Date().toISOString(),
      description: `Connection from ${sourceNode.data?.name || sourceNode.id} to ${targetNode.data?.name || targetNode.id}`,
      sourceComponent: sourceNode.data?.type,
      targetComponent: targetNode.data?.type,
      sourceNodeId: sourceNode.id,
      targetNodeId: targetNode.id,
      sourceTable: getNodeTableName(sourceNode),
      targetTable: getNodeTableName(targetNode)
    }
  };

  return {
    id: edgeId,
    type: 'smoothstep' as const,
    animated: false,
    style: { strokeWidth: 2 },
    markerEnd: {
      type: MarkerType.ArrowClosed,
      width: 20,
      height: 20,
    },
    data: edgeMeta,
    source: connection.source,
    target: connection.target,
    sourceHandle: connection.sourceHandle || null,
    targetHandle: connection.targetHandle || null
  };
}

// ==================== VALIDATION / GRAPH UTILITIES ====================

/**
 * Convert React Flow nodes/edges to validation engine GraphState
 */
export function convertToGraphState(nodes: Node[], edges: Edge[]): GraphState {
  return {
    nodes: nodes.map(node => {
      const data = node.data as CanvasNodeData;
      const outputSchema = data.metadata?.schemas?.output;
      const schema = outputSchema ? {
        columns: outputSchema.fields.map(f => ({
          name: f.name,
          type: f.type,
          nullable: f.nullable,
          isKey: f.isKey,
          length: f.length,
          precision: f.precision,
          scale: f.scale,
          defaultValue: f.defaultValue,
          description: f.description,
        }))
      } : { columns: [] };

      return {
        id: node.id,
        type: node.type || 'talendNode',
        position: node.position,
        data: {
          name: data.name || node.id,
          technology: data.type,
          componentCategory: data.componentCategory?.toLowerCase(),
          schema,
        },
        metadata: data.metadata,
      } as OriginalGraphNode;
    }),
    edges: edges.map(edge => ({
      id: edge.id,
      source: edge.source,
      target: edge.target,
      sourceHandle: edge.sourceHandle || null,
      targetHandle: edge.targetHandle || null,
      data: edge.data || {},
      metadata: edge.data?.metadata,
    } as unknown as OriginalGraphEdge)),
  };
}

/**
 * Simple cycle detection (DFS)
 */
export function wouldCauseCycle(sourceId: string, targetId: string, edges: Edge[]): boolean {
  const visited = new Set<string>();
  const stack = [targetId];
  while (stack.length > 0) {
    const current = stack.pop()!;
    if (current === sourceId) return true;
    if (visited.has(current)) continue;
    visited.add(current);
    edges.filter(e => e.source === current).forEach(edge => {
      if (!visited.has(edge.target)) stack.push(edge.target);
    });
  }
  return false;
}

// ==================== NODE TYPE MAPPING ====================

/**
 * Map component key (from registry) to NodeType enum
 */
export function mapComponentKeyToNodeType(key: string, role?: 'INPUT' | 'OUTPUT' | 'TRANSFORM'): NodeType {
  if (role === 'INPUT') return NodeType.INPUT;
  if (role === 'OUTPUT') return NodeType.OUTPUT;
  switch (key) {
    case 'tMap': return NodeType.MAP;
    case 'tJoin': return NodeType.JOIN;
    case 'tDenormalize': return NodeType.DENORMALIZE;
    case 'tNormalize': return NodeType.NORMALIZE;
    case 'tAggregateRow': return NodeType.AGGREGATE_ROW;
    case 'tSortRow': return NodeType.SORT_ROW;
    case 'tFilterRow': return NodeType.FILTER_ROW;
    case 'tFilterColumns': return NodeType.FILTER_COLUMNS;
    case 'tReplace': return NodeType.REPLACE;
    case 'tReplaceList': return NodeType.REPLACE_LIST;
    case 'tConvertType': return NodeType.CONVERT_TYPE;
    case 'tExtractDelimitedFields': return NodeType.EXTRACT_DELIMITED_FIELDS;
    case 'tExtractRegexFields': return NodeType.EXTRACT_REGEX_FIELDS;
    case 'tExtractJSONFields': return NodeType.EXTRACT_JSON_FIELDS;
    case 'tExtractXMLField': return NodeType.EXTRACT_XML_FIELD;
    case 'tParseRecordSet': return NodeType.PARSE_RECORD_SET;
    case 'tSplitRow': return NodeType.SPLIT_ROW;
    case 'tPivotToColumnsDelimited': return NodeType.PIVOT_TO_COLUMNS_DELIMITED;
    case 'tUnpivotRow': return NodeType.UNPIVOT_ROW;
    case 'tDenormalizeSortedRow': return NodeType.DENORMALIZE_SORTED_ROW;
    case 'tUniqRow': return NodeType.UNIQ_ROW;
    case 'tSampleRow': return NodeType.SAMPLE_ROW;
    case 'tSchemaComplianceCheck': return NodeType.SCHEMA_COMPLIANCE_CHECK;
    case 'tAddCRCRow': return NodeType.ADD_CRC_ROW;
    case 'tAddCRC': return NodeType.ADD_CRC;
    case 'tStandardizeRow': return NodeType.STANDARDIZE_ROW;
    case 'tDataMasking': return NodeType.DATA_MASKING;
    case 'tAssert': return NodeType.ASSERT;
    case 'tFlowToIterate': return NodeType.FLOW_TO_ITERATE;
    case 'tIterateToFlow': return NodeType.ITERATE_TO_FLOW;
    case 'tReplicate': return NodeType.REPLICATE;
    case 'tUnite': return NodeType.UNITE;
    case 'tFlowMerge': return NodeType.FLOW_MERGE;
    case 'tFlowMeter': return NodeType.FLOW_METER;
    case 'tFlowMeterCatcher': return NodeType.FLOW_METER_CATCHER;
    case 'tMatchGroup': return NodeType.MATCH_GROUP;
    case 'tRowGenerator': return NodeType.ROW_GENERATOR;
    case 'tNormalizeNumber': return NodeType.NORMALIZE_NUMBER;
    case 'tFileLookup': return NodeType.FILE_LOOKUP;
    case 'tCacheIn': return NodeType.CACHE_IN;
    case 'tCacheOut': return NodeType.CACHE_OUT;
    case 'tRecordMatching': return NodeType.RECORD_MATCHING;
    case 'tLookup': return NodeType.LOOKUP;
    default:
      console.warn(`Unknown component key: ${key}, defaulting to TRANSFORM`);
      return NodeType.TRANSFORM;
  }
}

// ==================== DATABASE CONNECTION ====================

/**
 * Get the currently active PostgreSQL connection ID (if any)
 */
export async function getActivePostgresConnectionId(): Promise<string | null> {
  try {
    const connections = await databaseApi.getActiveConnections();
    const pgConnection = connections.find(c => 
      c.dbType === 'postgresql' || c.dbType === 'postgres'
    );
    return pgConnection ? pgConnection.connectionId : null;
  } catch (error) {
    console.error('Failed to get active PostgreSQL connection:', error);
    return null;
  }
}