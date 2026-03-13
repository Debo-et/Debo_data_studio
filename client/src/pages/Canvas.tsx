// src/pages/Canvas.tsx (FIXED)
import React, { useCallback, useEffect, useRef, useState, forwardRef, useImperativeHandle } from 'react';
import ReactFlow, {
  Controls,
  Background,
  BackgroundVariant,
  ConnectionMode,
  ReactFlowInstance,
  NodeTypes,
  useReactFlow,
  ConnectionLineType,
  SelectionMode,
  addEdge,
  applyNodeChanges,
  applyEdgeChanges,
  NodeChange,
  EdgeChange,
  Connection,
  Edge,
  MarkerType,
  Node,
  Viewport
} from 'reactflow';
import 'reactflow/dist/style.css';

import { motion } from 'framer-motion';
import { Suspense } from 'react';

import MapEditor from '../components/Editor/MapEditor';

// Import unified architecture
import TalendNode from './TalendNode';
import { COMPONENT_REGISTRY, ComponentDefinition, getCategoryColor } from './ComponentRegistry';
import { nameGenerator } from './NameGenerator';

// Import components
import RoleSelectionModal from './RoleSelectionPopup';
import { useCanvas } from './CanvasContext';

// Validation imports
import { ValidationEngine } from '../validation/validationEngine';
import { SchemaRegistry } from '../validation/schemaRegistry';
import { DefaultSchemas, DefaultConnectionRules } from '../validation/schemaRegistry';
import { GraphState, GraphNode as OriginalGraphNode, GraphEdge as OriginalGraphEdge } from '../validation/types';

// Import UNIFIED metadata types and strategies
import {
  UnifiedCanvasNode,
  UnifiedCanvasConnection,
  UnifiedNodeMetadata,
  ComponentConfiguration,
  MapComponentConfiguration,
  JoinComponentConfiguration,
  FilterComponentConfiguration,
  LookupComponentConfiguration,
  AggregateComponentConfiguration,
  SortComponentConfiguration,
  InputComponentConfiguration,
  OutputComponentConfiguration,
  NodeType,
  DataSourceType,
  PostgreSQLDataType,
  FieldSchema,
  SchemaDefinition,
  DataType,
  isMapConfig,
  isJoinConfig,
  isFilterConfig,
  isLookupConfig,
  isAggregateConfig,
  isSortConfig,
  isInputConfig,
  isOutputConfig,
  getComponentConfig,
  PostgresColumn,
  NodeStatus
} from '../types/unified-pipeline.types';

// Import column extraction utilities
import { getConnectedColumns, MapEditorPayload } from '../utils/columnExtraction';

// Import persistence service
import { canvasPersistence, CanvasRecord } from '../services/canvas-persistence.service';

// Import SQL generation
import { useAppDispatch } from '../hooks';
import { addLog } from '../store/slices/logsSlice';
import { generatePipelineSQL, PipelineGenerationResult } from '../generators/SQLGenerationPipeline';
import { CanvasNode as PipelineCanvasNode, CanvasConnection as PipelineCanvasConnection, ConnectionStatus } from '../types/pipeline-types'; // We'll keep the old types for the generator for now

// ==================== TYPES ====================
interface ReactFlowDragData {
  type: 'reactflow-component';
  componentId: string;
  source: 'sidebar' | 'rightPanel';
  metadata?: Record<string, any>;
}

// Our node data is now UnifiedCanvasNode
type CanvasNodeData = UnifiedCanvasNode;

interface ExtendedCanvasProps {
  job?: any;
  onJobUpdate?: (updates: any) => void;
  jobDesign?: any;
  onJobDesignUpdate?: (design: any) => void;
  validateConnection?: (connection: Connection, nodes: Node[], edges: Edge[]) => {
    isValid: boolean;
    errors: string[];
    warnings: string[];
  };
  canvasId?: string;
  onNodeMetadataUpdate?: (nodeId: string, metadata: UnifiedNodeMetadata) => void;
  onEdgeMetadataUpdate?: (edgeId: string, metadata: any) => void;
}

interface TableDefinition {
  id: string;
  name: string;
  type: 'input' | 'output';
  columns: Array<{
    id: string;
    name: string;
    type: string;
    isKey: boolean;
    expression?: string;
  }>;
}

interface WizardConfig {
  currentStep: number;
  inputFlow: string;
  schemaColumns: any[];
  groupingKeys: any[];
  survivorshipRules: any[];
  outputMapping: Record<string, any>;
  outputTableName: string;
}

interface SQLPreviewState {
  isVisible: boolean;
  sql: string;
  title: string;
  nodeId?: string;
  nodeName?: string;
}

interface ValidationSummary {
  isValid: boolean;
  results: any[];
  counts: {
    errors: number;
    warnings: number;
    info: number;
  };
}

interface ConnectionFeedback {
  isVisible: boolean;
  message: string;
  type: 'success' | 'error' | 'info' | 'warning';
  position: { x: number; y: number };
}

interface PendingRoleSelection {
  nodeId: string;
  componentId: string;
  displayName: string;
  position: { x: number; y: number };
  dropPosition: { x: number; y: number };
  componentDef: ComponentDefinition;
  nodeData: CanvasNodeData;
}

interface MapEditorState {
  isOpen: boolean;
  data: MapEditorPayload | null;
  nodeMetadata?: CanvasNodeData;
}

interface CanvasState {
  nodes: Node[];
  edges: Edge[];
  selectedNodeId: string | null;
  showMapEditor: boolean;
  selectedNodeForMapEditor: any;
  sqlPreview: SQLPreviewState;
  validationSummary: ValidationSummary | null;
  validationMode: 'strict' | 'lenient' | 'warn-only';
  isValidating: boolean;
  connectionFeedback: ConnectionFeedback;
  pendingDrop: any | null;
  showMatchGroupWizard: boolean;
  selectedNodeForMatchGroupWizard: any;
  viewport: { x: number; y: number; zoom: number };
  mapEditorState: MapEditorState;
  autoSaveStatus: 'idle' | 'saving' | 'saved' | 'error';
  lastSavedAt?: string;
}

// ==================== NODE TYPES ====================
const nodeTypes: NodeTypes = {
  talendNode: TalendNode,
};

// ==================== METADATA HELPER FUNCTIONS ====================

/**
 * Create initial component configuration based on component type and role.
 * This now builds the discriminated union configuration.
 */
const createInitialComponentConfiguration = (
  componentId: string,
  role: 'INPUT' | 'OUTPUT' | 'TRANSFORM',
  metadata?: any
): ComponentConfiguration => {
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
            sourceType: DataSourceType.POSTGRESQL, // default, can be overridden
            sourceDetails: {
              connectionString: undefined,
              tableName: '',
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
              tableName: '',
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
};

// Helper to convert DataType from metadata to PostgreSQLDataType
const mapDataTypeToPostgreSQL = (type: DataType | string): PostgreSQLDataType => {
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
};

// Helper to convert metadata fields to PostgresColumn[]
const fieldsToPostgresColumns = (fields: FieldSchema[]): PostgresColumn[] => {
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
};

// Helper to extract columns from drag metadata (unchanged)
const extractColumnsFromDragData = (metadata: any): any[] => {
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
};

// Helper to get repository metadata from node
const getNodeRepositoryMetadata = (node: Node<CanvasNodeData>): any => {
  return node.data?.metadata?.fullRepositoryMetadata ||
         node.data?.metadata?.repositoryMetadata ||
         node.data?.metadata?.dragMetadata?.repositoryMetadata;
};

// Helper to get table name from node
const getNodeTableName = (node: Node<CanvasNodeData>): string => {
  const metadata = getNodeRepositoryMetadata(node);
  return metadata?.tableName || metadata?.name || node.data?.name || node.id;
};

// Determine relation type based on component roles
const determineRelationType = (_sourceNode: Node<CanvasNodeData>, targetNode: Node<CanvasNodeData>): string => {
  const targetType = targetNode.data?.type;
  if (targetType === NodeType.JOIN) return 'JOIN';
  if (targetType === NodeType.FILTER_ROW) return 'FILTER';
  if (targetType === NodeType.MAP) return 'MAPPING';
  if (targetType === NodeType.LOOKUP) return 'LOOKUP';
  if (targetType === NodeType.SPLIT_ROW) return 'SPLIT';
  return 'FLOW';
};

// Create default edge configuration (unchanged but uses NodeType)
const createDefaultEdgeConfig = (
  relationType: string,
  sourceNode: Node<CanvasNodeData>,
  targetNode: Node<CanvasNodeData>
): Record<string, any> => {
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
};

// Create edge with metadata (now stores configuration in data.metadata)
const createEdgeWithMetadata = (
  connection: {
    source: string;
    target: string;
    sourceHandle?: string | null;
    targetHandle?: string | null;
  },
  sourceNode: Node<CanvasNodeData>,
  targetNode: Node<CanvasNodeData>
): Edge => {
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
    data: edgeMeta, // store full edge metadata
    source: connection.source,
    target: connection.target,
    sourceHandle: connection.sourceHandle || null,
    targetHandle: connection.targetHandle || null
  };
};

// ==================== UTILITY FUNCTIONS ====================
const extractColumnsFromNode = (node: Node<CanvasNodeData>): Array<{
  id: string;
  name: string;
  type: string;
  isKey: boolean;
  expression?: string;
  originalMetadata?: any;
}> => {
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
};

const getConnectedNodes = (
  nodeId: string,
  edges: Edge[],
  nodes: Node[],
  direction: 'input' | 'output'
): Node<CanvasNodeData>[] => {
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
};

const getMapNodeConnections = (
  mapNode: Node<CanvasNodeData>,
  nodes: Node<CanvasNodeData>[],
  edges: Edge[]
): { inputNodes: Node<CanvasNodeData>[]; outputNodes: Node<CanvasNodeData>[] } => {
  return {
    inputNodes: getConnectedNodes(mapNode.id, edges, nodes, 'input'),
    outputNodes: getConnectedNodes(mapNode.id, edges, nodes, 'output')
  };
};

// Convert React Flow nodes to validation engine GraphState
const convertToGraphState = (nodes: Node[], edges: Edge[]): GraphState => {
  return {
    nodes: nodes.map(node => {
      const data = node.data as CanvasNodeData;
      
      // Build schema with columns array for validation engine
      const outputSchema = data.metadata?.schemas?.output;
      const schema = outputSchema ? {
        columns: outputSchema.fields.map(f => ({
          name: f.name,
          type: f.type,
          // include other relevant fields if needed
          nullable: f.nullable,
          isKey: f.isKey,
          length: f.length,
          precision: f.precision,
          scale: f.scale,
          defaultValue: f.defaultValue,
          description: f.description,
        }))
      } : { columns: [] }; // Always provide a columns array

      return {
        id: node.id,
        type: node.type || 'talendNode',
        position: node.position,
        data: {
          name: data.name || node.id,
          technology: data.type,
          componentCategory: data.componentCategory?.toLowerCase(),
          schema, // now guaranteed to have a columns property
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
};

// Cycle detection
const wouldCauseCycle = (sourceId: string, targetId: string, edges: Edge[]): boolean => {
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
};

// Map component key to NodeType enum
const mapComponentKeyToNodeType = (key: string, role?: 'INPUT' | 'OUTPUT' | 'TRANSFORM'): NodeType => {
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
};

// ==================== CANVAS COMPONENT ====================
const Canvas = forwardRef<{ forceSave: () => Promise<void> }, ExtendedCanvasProps>(({
  job,
  canvasId,
  validateConnection: propValidateConnection,
  onNodeMetadataUpdate,
  onEdgeMetadataUpdate
}, ref) => {
  // Get sync methods from CanvasContext
  const { syncNodesAndEdges, updateCanvasData } = useCanvas();
  const { screenToFlowPosition, addNodes, setViewport } = useReactFlow();

  // React Flow refs
  const reactFlowWrapper = useRef<HTMLDivElement>(null);
  const [reactFlowInstance, setReactFlowInstance] = useState<ReactFlowInstance | null>(null);
  const validationEngineRef = useRef<ValidationEngine | null>(null);
  const schemaRegistryRef = useRef<SchemaRegistry | null>(null);

  // Auto-save refs and state
  const saveTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const isSavingRef = useRef<boolean>(false);
  const lastSaveStateRef = useRef<string>('');

  // Track last loaded canvas/job to avoid redundant loads
  const lastLoadedCanvasIdRef = useRef<string | null>(null);
  const lastLoadedJobIdRef = useRef<string | null>(null);

  // NEW: Store the canvas ID locally after first save (for job‑based canvases)
  const localCanvasIdRef = useRef<string | null>(null);

  // Redux dispatch for logs
  const dispatch = useAppDispatch();

  // State management
  const [nodes, setNodes] = useState<Node[]>([]);
  const [edges, setEdges] = useState<Edge[]>([]);
  const [state, setState] = useState<CanvasState>({
    nodes: [],
    edges: [],
    selectedNodeId: null,
    showMapEditor: false,
    selectedNodeForMapEditor: null,
    sqlPreview: {
      isVisible: false,
      sql: '',
      title: '',
      nodeId: undefined,
      nodeName: undefined
    },
    validationSummary: null,
    validationMode: 'strict',
    isValidating: false,
    connectionFeedback: {
      isVisible: false,
      message: '',
      type: 'info',
      position: { x: 0, y: 0 }
    },
    pendingDrop: null,
    showMatchGroupWizard: false,
    selectedNodeForMatchGroupWizard: null,
    viewport: {
      x: 0,
      y: 0,
      zoom: 1  // Start at 1:1 scale
    },
    // Map editor state with metadata
    mapEditorState: {
      isOpen: false,
      data: null
    },
    // Auto-save status
    autoSaveStatus: 'idle',
    lastSavedAt: undefined
  });

  // Role selection state
  const [pendingRoleSelection, setPendingRoleSelection] = useState<PendingRoleSelection | null>(null);

  // ==================== SYNC WITH CANVAS CONTEXT ====================
  // Sync nodes and edges with CanvasContext whenever they change
  useEffect(() => {
    if (nodes.length > 0 || edges.length > 0) {
      console.log('🔄 Syncing canvas state with context:', {
        nodes: nodes.length,
        edges: edges.length
      });
      syncNodesAndEdges(nodes, edges);
    }
  }, [nodes, edges, syncNodesAndEdges]);

  /**
   * Show validation feedback
   */
  const showValidationFeedback = useCallback((
    message: string,
    type: 'success' | 'error' | 'info' | 'warning',
    position?: { x: number; y: number }
  ) => {
    const feedbackPosition = position || { x: 100, y: 100 };

    setState(prev => ({
      ...prev,
      connectionFeedback: {
        isVisible: true,
        message,
        type,
        position: feedbackPosition
      }
    }));

    // Auto-hide feedback
    setTimeout(() => {
      setState(prev => ({
        ...prev,
        connectionFeedback: {
          ...prev.connectionFeedback,
          isVisible: false
        }
      }));
    }, 3000);
  }, []);

  // ==================== AUTO-SAVE PERSISTENCE ====================
  /**
   * Save canvas state function – now supports updating by canvasId
   * and always updates the same record once an ID is known.
   */
  const saveCanvasState = useCallback(async () => {
    // We need either a job (for auto‑named saves) OR a canvasId (for named canvas saves)
    if ((!job && !canvasId) || isSavingRef.current) {
      return;
    }

    try {
      isSavingRef.current = true;

      const currentStateHash = JSON.stringify({
        nodes: nodes.map(n => ({
          id: n.id,
          position: n.position,
          data: n.data
        })),
        edges: edges.map(e => ({
          id: e.id,
          source: e.source,
          target: e.target,
          data: e.data
        })),
        viewport: state.viewport
      });

      // Only save if state has changed
      if (currentStateHash === lastSaveStateRef.current) {
        console.log('🔄 No changes detected, skipping save');
        return;
      }

      console.log('💾 Saving canvas state:', {
        canvasId: canvasId || localCanvasIdRef.current || 'new',
        nodeCount: nodes.length,
        edgeCount: edges.length,
        jobName: job?.name
      });

      setState(prev => ({
        ...prev,
        autoSaveStatus: 'saving'
      }));

      let savedRecord: CanvasRecord | null = null;

      // Case 1: Use provided canvasId (from props)
      if (canvasId) {
        await canvasPersistence.updateCanvas(canvasId, {
          nodes,
          edges,
          viewport: state.viewport
        });
        savedRecord = { id: canvasId } as CanvasRecord; // we don't need the full record
      }
      // Case 2: Use stored local ID from previous save
      else if (localCanvasIdRef.current) {
        await canvasPersistence.updateCanvas(localCanvasIdRef.current, {
          nodes,
          edges,
          viewport: state.viewport
        });
        savedRecord = { id: localCanvasIdRef.current } as CanvasRecord;
      }
      // Case 3: First save – call saveCanvas (will find existing by name or create new)
      else if (job) {
        const canvasName = job.name;
        savedRecord = await canvasPersistence.saveCanvas(
          canvasName,
          {
            nodes,
            edges,
            viewport: state.viewport
          },
          {
            description: `Auto-saved from job: ${job?.name}`,
            tags: [job?.name || 'unknown', 'auto-save', 'canvas'],
            compilerMetadata: {},
            otherUiState: {
              jobId: job?.id,
              jobName: job?.name,
              nodeCount: nodes.length,
              edgeCount: edges.length,
              savedAt: new Date().toISOString()
            }
          }
        );
      } else {
        throw new Error('No canvas target (neither job nor canvasId)');
      }

      if (savedRecord && savedRecord.id) {
        console.log('✅ Canvas saved successfully (ID: ' + savedRecord.id + ')');
        lastSaveStateRef.current = currentStateHash;

        // Store the canvas ID for future updates
        localCanvasIdRef.current = savedRecord.id;

        setState(prev => ({
          ...prev,
          autoSaveStatus: 'saved',
          lastSavedAt: new Date().toISOString()
        }));

        showValidationFeedback(
          `Saved canvas (${nodes.length} nodes, ${edges.length} edges)`,
          'success',
          { x: 100, y: 100 }
        );
      } else if (canvasId) {
        // When canvasId was provided, we don't have a full record but the update succeeded
        lastSaveStateRef.current = currentStateHash;
        setState(prev => ({
          ...prev,
          autoSaveStatus: 'saved',
          lastSavedAt: new Date().toISOString()
        }));
      } else {
        throw new Error('Save returned null result');
      }
    } catch (error: any) {
      console.error('❌ Failed to save canvas:', error);
      setState(prev => ({
        ...prev,
        autoSaveStatus: 'error'
      }));

      showValidationFeedback(
        `Save failed: ${error.message}`,
        'error',
        { x: 100, y: 100 }
      );
    } finally {
      isSavingRef.current = false;
    }
  }, [job, canvasId, nodes, edges, state.viewport, showValidationFeedback]);

  /**
   * Debounced auto-save function
   */
  const debouncedAutoSave = useCallback(() => {
    // Clear existing timeout
    if (saveTimeoutRef.current) {
      clearTimeout(saveTimeoutRef.current);
    }

    // Don't save if no job and no canvasId, or already saving
    if ((!job && !canvasId) || isSavingRef.current) {
      return;
    }

    // Set new timeout for auto-save
    saveTimeoutRef.current = setTimeout(() => {
      saveCanvasState();
    }, 1000); // 1-second debounce
  }, [job, canvasId, saveCanvasState]);

  // Expose forceSave method to parent via ref
  useImperativeHandle(ref, () => ({
    forceSave: saveCanvasState
  }), [saveCanvasState]);

  // Sync canvasId prop to local ref
  useEffect(() => {
    if (canvasId) {
      localCanvasIdRef.current = canvasId;
    }
  }, [canvasId]);

  // ==================== INITIALIZATION & LOADING ====================
  useEffect(() => {
    const initializeCanvas = async () => {
      // Guard: require either a canvasId or a job to load
      if (!canvasId && !job) {
        console.log('ℹ️ [Canvas] No canvasId or job provided – skipping load.');
        return;
      }

      // Check if we already loaded this specific canvas/job
      if (canvasId) {
        if (lastLoadedCanvasIdRef.current === canvasId) {
          console.log('ℹ️ [Canvas] Already loaded canvas', canvasId);
          return;
        }
      } else if (job?.id) {
        if (lastLoadedJobIdRef.current === job.id) {
          console.log('ℹ️ [Canvas] Already loaded job', job.id);
          return;
        }
      }

      try {
        let loadedNodes: Node[] = [];
        let loadedEdges: Edge[] = [];
        let loadedViewport: Viewport = { x: 0, y: 0, zoom: 1 };

        if (canvasId) {
          console.log(`📂 [Canvas] Loading canvas by ID: ${canvasId}`);
          const canvasData = await canvasPersistence.getCanvas(canvasId);
          if (canvasData) {
            loadedNodes = canvasData.reactFlow.nodes || [];
            loadedEdges = canvasData.reactFlow.edges || [];
            loadedViewport = canvasData.reactFlow.viewport || { x: 0, y: 0, zoom: 1 };
            localCanvasIdRef.current = canvasId;
            console.log(`✅ [Canvas] Loaded ${loadedNodes.length} nodes, ${loadedEdges.length} edges from canvas ${canvasId}.`);
          } else {
            console.warn(`⚠️ [Canvas] Canvas with id ${canvasId} not found or malformed – starting fresh.`);
            showValidationFeedback('Canvas data not found. Starting with empty canvas.', 'warning', { x: 100, y: 100 });
          }
        } else if (job) {
          // Load saved canvas by job name (old behavior)
          const canvasName = job.name;
          console.log(`📂 [Canvas] Loading canvas by name: ${canvasName}`);
          const savedData = await canvasPersistence.getCanvasByName(canvasName);
          if (savedData) {
            loadedNodes = savedData.data.reactFlow.nodes || [];
            loadedEdges = savedData.data.reactFlow.edges || [];
            loadedViewport = savedData.data.reactFlow.viewport || { x: 0, y: 0, zoom: 1 };
            localCanvasIdRef.current = savedData.id;
            console.log(`✅ [Canvas] Loaded ${loadedNodes.length} nodes, ${loadedEdges.length} edges from job canvas.`);
          } else {
            console.log('ℹ️ [Canvas] No saved canvas found for job – starting fresh.');
          }
        }

        // Update local React Flow state
        setNodes(loadedNodes);
        setEdges(loadedEdges);

        // Update viewport if we have an instance
        if (loadedViewport && reactFlowInstance) {
          setViewport(loadedViewport);
        }

        // Update local state (last saved time, viewport)
        setState(prev => ({
          ...prev,
          viewport: loadedViewport,
          lastSavedAt: canvasId ? new Date().toISOString() : undefined
        }));

        // Sync with CanvasContext
        updateCanvasData({
          nodes: loadedNodes,
          edges: loadedEdges,
          viewport: loadedViewport
        });

        // Show user feedback
        if (loadedNodes.length > 0 || loadedEdges.length > 0) {
          showValidationFeedback(
            `Loaded canvas with ${loadedNodes.length} nodes, ${loadedEdges.length} edges`,
            'success',
            { x: 100, y: 100 }
          );
        }

        // Mark as loaded for this canvas/job
        if (canvasId) {
          lastLoadedCanvasIdRef.current = canvasId;
          lastLoadedJobIdRef.current = null;
        } else if (job?.id) {
          lastLoadedJobIdRef.current = job.id;
          lastLoadedCanvasIdRef.current = null;
        }

        console.log('✅ [Canvas] Initialization complete.');
      } catch (error) {
        console.error('❌ [Canvas] Failed to load canvas:', error);
        showValidationFeedback('Failed to load canvas. Starting fresh.', 'error', { x: 100, y: 100 });
      }
    };

    initializeCanvas();

    // Cleanup: save final state before unmounting
    return () => {
      if ((job || canvasId) && (lastLoadedCanvasIdRef.current || lastLoadedJobIdRef.current)) {
        console.log('💾 [Canvas] Unmounting – saving final state.');
        saveCanvasState();
      }
      if (saveTimeoutRef.current) {
        clearTimeout(saveTimeoutRef.current);
      }
    };
  }, [job, canvasId, reactFlowInstance, setViewport, updateCanvasData, saveCanvasState, showValidationFeedback]);

  // Initialize name generator with existing nodes
  useEffect(() => {
    const nodeLabels = nodes
      .filter(node => node.type === 'talendNode')
      .map(node => node.data?.name)
      .filter(Boolean);

    nameGenerator.initializeFromExistingNodes(nodeLabels);
  }, [nodes]);

  // ==================== VALIDATION INITIALIZATION ====================
  useEffect(() => {
    // Initialize schema registry
    const registry = new SchemaRegistry();
    registry.registerSchemas(DefaultSchemas);
    DefaultConnectionRules.forEach(rule => registry.registerConnectionRule(rule));
    schemaRegistryRef.current = registry;

    // Initialize validation engine with proper configuration
    const engine = new ValidationEngine({
      schemaRegistry: registry,
      mode: state.validationMode,
      enableCaching: true,
      cacheTTL: 3000,
      enableETLValidation: true,
      etlMode: 'strict'
    });

    validationEngineRef.current = engine;

    // Validate initial state if nodes exist
    if (nodes.length > 0) {
      const graphState = convertToGraphState(nodes, edges);
      const summary = engine.validateGraph(graphState);

      if (!summary.isValid) {
        console.warn('Initial graph validation failed:', summary);
      }
    }

    return () => {
      validationEngineRef.current?.clearCache();
    };
  }, [state.validationMode, nodes.length, edges.length]);

  // ==================== VALIDATION FEEDBACK ====================

  // ==================== VALIDATION FUNCTIONS ====================

  /**
   * React Flow's isValidConnection callback for visual feedback during drag
   */
  const isValidConnection = useCallback((connection: Connection): boolean => {
    if (!connection.source || !connection.target) {
      return false;
    }

    // Quick check for self-connection
    if (connection.source === connection.target) {
      return false;
    }

    // Quick check for cycles
    if (wouldCauseCycle(connection.source, connection.target, edges)) {
      return false;
    }

    // Use validation engine for comprehensive validation
    if (validationEngineRef.current) {
      try {
        const state = convertToGraphState(nodes, edges);
        const result = validationEngineRef.current.validateSpecificConnection(
          connection.source,
          connection.target,
          state
        );
        return result.isValid;
      } catch (error) {
        console.error('Validation engine error in isValidConnection:', error);
      }
    }

    // Fallback to schema registry validation
    const sourceNode = nodes.find(n => n.id === connection.source);
    const targetNode = nodes.find(n => n.id === connection.target);

    if (!sourceNode || !targetNode) return false;

    const sourceType = sourceNode.data?.type || sourceNode.type || 'unknown';
    const targetType = targetNode.data?.type || targetNode.type || 'unknown';

    // Check connection rules
    const connectionCheck = schemaRegistryRef.current?.isConnectionAllowed(sourceType, targetType);
    if (connectionCheck && !connectionCheck.allowed) {
      return false;
    }

    // Check ETL rules
    const etlCheck = schemaRegistryRef.current?.isETLConnectionAllowed(sourceType, targetType);
    if (etlCheck && !etlCheck.allowed) {
      return false;
    }

    return true;
  }, [nodes, edges, wouldCauseCycle]);

  // ==================== NEW: TMap HANDLER FUNCTIONS WITH METADATA ====================
  /**
   * Handle tMap node double-click to open MapEditor with connected columns and metadata
   */
  /**
 * Handle tMap node double-click to open MapEditor with connected columns and metadata
 */
const handleTMapDoubleClick = useCallback((nodeId: string) => {
  console.log('🎯 Opening tMap editor with metadata for node:', nodeId);

  const tMapNode = nodes.find(n => n.id === nodeId);
  if (!tMapNode) {
    console.error('❌ tMap node not found:', nodeId);
    return;
  }

  const nodeData = tMapNode.data as CanvasNodeData;
  const mapConfig = getComponentConfig(nodeData, 'MAP');

  console.log('🔍 tMap node metadata:', {
    id: tMapNode.id,
    label: nodeData.name,
    configurationType: nodeData.metadata?.configuration.type,
    transformationCount: mapConfig ? mapConfig.transformations.length : 0
  });

  const editorData = getConnectedColumns(nodeId, nodes, edges);
  console.log('🔍 getConnectedColumns output:', JSON.stringify(editorData, null, 2));

  // ✅ FIX: Pass only the fields that MapEditor expects (name and type)
  // Remove any extra properties like sourceSchema that may cause errors.
  const transformedData = {
    ...editorData,
    inputColumns: editorData.inputColumns.map(col => ({
      name: col.name,
      type: col.type || 'STRING'  // ensure type is defined
    })),
    outputColumns: editorData.outputColumns.map(col => ({
      name: col.name,
      type: col.type || 'STRING'
    }))
  };

  console.log('📊 Editor data prepared with metadata:', {
    nodeId: editorData.nodeId,
    inputColumns: editorData.inputColumns.length,
    outputColumns: editorData.outputColumns.length,
    existingTransformations: mapConfig ? mapConfig.transformations.length : 0
  });

  setState(prev => ({
    ...prev,
    mapEditorState: {
      isOpen: true,
      data: transformedData,   // use simplified data
      nodeMetadata: nodeData
    },
  }));

  showValidationFeedback(
    `Opening Map Editor for ${nodeData.name || nodeId}`,
    'info',
    { x: 100, y: 100 }
  );
}, [nodes, edges, showValidationFeedback]);
  /**
   * Handle MapEditor save with metadata strategies
   */
  const handleMapEditorSave = useCallback((config: MapComponentConfiguration) => {
    const { mapEditorState } = state;
    if (!mapEditorState.isOpen || !mapEditorState.nodeMetadata) {
      console.error('❌ No map editor state or node metadata found');
      return;
    }

    const nodeId = mapEditorState.nodeMetadata.id;
    const previousHash = mapEditorState.nodeMetadata.metadata?.compilerMetadata?.lastModified || '';

    console.log('💾 Saving map configuration with metadata:', {
      nodeId,
      transformationCount: config.transformations.length,
      previousHash
    });

    // Create updated configuration union
    const updatedConfig: ComponentConfiguration = { type: 'MAP', config };

    // Update node metadata
    setNodes(prev => {
      const updatedNodes = prev.map(node => {
        if (node.id === nodeId) {
          const updatedMetadata: UnifiedNodeMetadata = {
            ...node.data.metadata,
            configuration: updatedConfig,
            compilerMetadata: {
              ...(node.data.metadata?.compilerMetadata || {}),
              lastModified: new Date().toISOString()
            }
          };
          const updatedNodeData: CanvasNodeData = {
            ...node.data,
            metadata: updatedMetadata
          };

          // Notify parent component about node update
          if (onNodeMetadataUpdate) {
            onNodeMetadataUpdate(nodeId, updatedMetadata);
          }

          return { ...node, data: updatedNodeData };
        }
        return node;
      });

      // Sync updated nodes with context
      syncNodesAndEdges(updatedNodes, edges);

      return updatedNodes;
    });

    // Trigger auto-save after map editor save
    debouncedAutoSave();

    // Show success feedback
    showValidationFeedback(
      `Saved ${config.transformations.length} transformations for ${mapEditorState.nodeMetadata.name}`,
      'success',
      { x: 100, y: 100 }
    );

    // Close the editor
    setState(prev => ({
      ...prev,
      mapEditorState: {
        isOpen: false,
        data: null,
        nodeMetadata: undefined
      }
    }));
  }, [state.mapEditorState, onNodeMetadataUpdate, showValidationFeedback, syncNodesAndEdges, edges, debouncedAutoSave]);

  /**
   * Close the tMap MapEditor modal
   */
  const closeTMapEditor = useCallback(() => {
    setState(prev => ({
      ...prev,
      mapEditorState: {
        isOpen: false,
        data: null,
        nodeMetadata: undefined
      }
    }));
  }, []);

  // ==================== UNIFIED REACT FLOW DROPZONE WITH METADATA ====================
  const onDrop = useCallback(
    (event: React.DragEvent) => {
      event.preventDefault();

      if (!reactFlowInstance) return;

      // Get drop position
      const position = screenToFlowPosition({
        x: event.clientX,
        y: event.clientY,
      });

      // Get drag data
      const reactFlowData = event.dataTransfer.getData('application/reactflow');

      if (!reactFlowData) {
        console.warn('No React Flow data in drop event');
        return;
      }

      try {
        const data: ReactFlowDragData = JSON.parse(reactFlowData);

        if (data.type !== 'reactflow-component') {
          console.warn('Invalid drag data type:', data.type);
          return;
        }

        // Get component definition from unified registry
        const componentDef = COMPONENT_REGISTRY[data.componentId];
        if (!componentDef) {
          console.warn(`Component not found in registry: ${data.componentId}`);
          return;
        }

        // ====== GET NAME FROM METADATA IF AVAILABLE ======
        // Try to get the name from metadata in this order:
        // 1. originalNodeName from metadata
        // 2. name from repositoryMetadata
        // 3. displayName from component definition
        let baseName = componentDef.displayName;

        if (data.metadata?.originalNodeName) {
          baseName = data.metadata.originalNodeName;
          console.log('🔤 Using originalNodeName from metadata:', baseName);
        } else if (data.metadata?.repositoryMetadata?.name) {
          baseName = data.metadata.repositoryMetadata.name;
          console.log('🔤 Using repositoryMetadata.name:', baseName);
        } else if (data.metadata?.name) {
          baseName = data.metadata.name;
          console.log('🔤 Using direct name property:', baseName);
        }

        // Clean up the base name (remove any role prefixes if present)
        const cleanBaseName = baseName
          .replace(/_(INPUT|OUTPUT|TRANSFORM)_/i, '_')
          .replace(/_+$/, ''); // Remove trailing underscores

        console.log('🔤 Clean base name for naming:', cleanBaseName);

        // Check if component is in 'input' category
        const isInputCategoryComponent = componentDef.category === 'input';

        // Generate deterministic name using NameGenerator with CLEAN base name
        const label = nameGenerator.generate(
          cleanBaseName,
          isInputCategoryComponent ? 'TRANSFORM' : componentDef.defaultRole
        );

        // Extract instance number from generated name
        const extracted = nameGenerator.extractBaseName(label);
        const instanceNumber = extracted?.instance || 1;

        // ====== END OF NEW NAME LOGIC ======

        // Extract columns from drag metadata
        const columns = extractColumnsFromDragData(data.metadata);

        // Determine component role
        const componentRole = isInputCategoryComponent ? 'TRANSFORM' : componentDef.defaultRole;

        // Create initial component configuration
        const configuration = createInitialComponentConfiguration(
          componentDef.id,
          componentRole,
          data.metadata
        );

        // Build fields from columns if any
        let fields: FieldSchema[] = [];
        if (columns.length > 0) {
          fields = columns.map((col: any, index: number) => ({
            id: col.id || `${cleanBaseName}_${index}`,
            name: col.name || `Column_${index + 1}`,
            type: col.type || 'STRING',
            length: col.length,
            precision: col.precision,
            scale: col.scale,
            nullable: col.nullable !== false,
            isKey: col.isKey || col.primaryKey || false,
            defaultValue: col.defaultValue,
            description: col.description || col.originalDescription,
            originalName: col.originalName,
            transformation: col.expression,
            metadata: { original: col }
          }));
        }

        // Build schemas based on component type
        const schemas: UnifiedNodeMetadata['schemas'] = {};

        if (componentRole === 'INPUT') {
          schemas.output = {
            id: `${cleanBaseName}_output_schema`,
            name: `${label} Output Schema`,
            fields,
            isTemporary: false,
            isMaterialized: false,
            metadata: {
              source: 'repository',
              columnCount: fields.length,
              keyColumns: fields.filter(f => f.isKey).length
            }
          };
          // Also update input config schema
          if (configuration.type === 'INPUT') {
            configuration.config.schema = schemas.output;
          }
        } else if (componentRole === 'OUTPUT') {
          schemas.input = [{
            id: `${cleanBaseName}_input_schema`,
            name: `${label} Input Schema`,
            fields,
            isTemporary: false,
            isMaterialized: false,
            metadata: {
              source: 'repository',
              columnCount: fields.length,
              keyColumns: fields.filter(f => f.isKey).length
            }
          }];
        } else {
          // Transform components get both input and output schemas
          schemas.input = [{
            id: `${cleanBaseName}_input_schema`,
            name: `${label} Input Schema`,
            fields,
            isTemporary: false,
            isMaterialized: false,
            metadata: {
              source: 'repository',
              columnCount: fields.length
            }
          }];
          schemas.output = {
            id: `${cleanBaseName}_output_schema`,
            name: `${label} Output Schema`,
            fields: [...fields],
            isTemporary: false,
            isMaterialized: false,
            metadata: {
              source: 'repository',
              columnCount: fields.length,
              keyColumns: fields.filter(f => f.isKey).length
            }
          };
        }

        // Create node data with unified metadata model
        // FIX: Added componentKey and componentType fields to ensure TalendNode renders the correct icon and color.
        const nodeData: CanvasNodeData = {
          id: `node-${Date.now()}-${cleanBaseName}`,
          name: label,
          type: mapComponentKeyToNodeType(componentDef.id, componentRole),
          nodeType: componentRole === 'INPUT' ? 'input' : componentRole === 'OUTPUT' ? 'output' : 'transform',
          componentCategory: componentDef.category, // now includes 'transform' in the union
          componentKey: componentDef.id,            // <-- FIX: needed for TalendNode icon lookup
          componentType: componentRole,              // <-- FIX: needed for TalendNode category color (uppercase)
          position,
          size: { width: componentDef.defaultDimensions.width * 2, height: componentDef.defaultDimensions.height * 2 },
          // Use defaultPorts if available, otherwise empty array (cast to any to avoid TS error if not in type)
          connectionPorts: (componentDef as any).defaultPorts || [],
          metadata: {
            configuration,
            schemas,
            description: componentDef.description,
            tags: [],
            version: '1.0',
            createdBy: 'canvas',
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString(),
            displayName: componentDef.displayName,
            cleanType: cleanBaseName,
            scaleFactor: 0.6,
            visualScaling: { fontSizeScale: 0.6, iconScale: 0.6, handleScale: 0.6 },
            // Preserve all repository metadata
            repositoryNodeId: data.metadata?.repositoryNodeId,
            repositoryNodeType: data.metadata?.repositoryNodeType,
            originalNodeName: data.metadata?.originalNodeName,
            originalNodeType: data.metadata?.originalNodeType,
            fullRepositoryMetadata: data.metadata?.repositoryMetadata,
            extractedColumns: columns,
            dragMetadata: data.metadata,
            source: data.source,
            sourceType: data.metadata?.sourceType,
            category: componentDef.category,
            isDataSource: componentDef.category === 'input' || componentDef.category === 'output',
          },
          status: NodeStatus.IDLE,
          draggable: true,
          droppable: false,
          technology: componentDef.id,
          visualProperties: {
            color: getCategoryColor(componentDef.category),
            icon: componentDef.icon, // now accepted as ReactNode
          }
        };

        // Create React Flow node with DOUBLED dimensions
        const newNode: Node<CanvasNodeData> = {
          id: nodeData.id,
          type: 'talendNode',
          position,
          data: nodeData,
          style: {
            width: nodeData.size.width,
            height: nodeData.size.height,
          },
          draggable: true,
          selectable: true,
          connectable: true,
        };

        // Add node using React Flow API
        addNodes(newNode);

        // Get updated nodes and sync with context
        const updatedNodes = [...nodes, newNode];
        setTimeout(() => {
          syncNodesAndEdges(updatedNodes, edges);
        }, 0);

        // Notify parent component about node creation
        if (onNodeMetadataUpdate) {
          onNodeMetadataUpdate(newNode.id, nodeData.metadata!);
        }

        // Trigger auto-save after adding node
        debouncedAutoSave();

        // If component is in 'input' category, show role selection popup
        if (isInputCategoryComponent) {
          setPendingRoleSelection({
            nodeId: newNode.id,
            componentId: componentDef.id,
            displayName: cleanBaseName,
            position: position,
            dropPosition: { x: event.clientX, y: event.clientY },
            componentDef,
            nodeData
          });

          // Show info feedback
          showValidationFeedback(
            `Please select role for ${cleanBaseName}`,
            'info',
            position
          );
        } else {
          // For non-input components, show success feedback
          showValidationFeedback(
            `Added ${label}`,
            'success',
            position
          );
        }

      } catch (error) {
        console.error('Error processing drop:', error);
        showValidationFeedback(
          'Failed to add component',
          'error',
          { x: event.clientX, y: event.clientY }
        );
      }
    },
    [reactFlowInstance, screenToFlowPosition, addNodes, showValidationFeedback, onNodeMetadataUpdate, nodes, edges, syncNodesAndEdges, debouncedAutoSave]
  );

  const onDragOver = useCallback((event: React.DragEvent) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'copy';
  }, []);

  // ==================== REACT FLOW HANDLERS WITH SYNCHRONIZATION AND PERSISTENCE ====================
  const onNodesChange = useCallback(
    (changes: NodeChange[]) => {
      // Update nodes normally
      const updatedNodes = applyNodeChanges(changes, nodes);
      setNodes(updatedNodes);

      // Immediately sync with context
      syncNodesAndEdges(updatedNodes, edges);

      // ✅ Trigger auto-save after node changes
      if (job || canvasId) {
        debouncedAutoSave();
      }

      // Check if any pending role selection node was deleted
      changes.forEach(change => {
        if (change.type === 'remove' && pendingRoleSelection?.nodeId === change.id) {
          setPendingRoleSelection(null);
        }
      });
    },
    [nodes, edges, pendingRoleSelection, syncNodesAndEdges, job, canvasId, debouncedAutoSave]
  );

  const onEdgesChange = useCallback(
    (changes: EdgeChange[]) => {
      const updatedEdges = applyEdgeChanges(changes, edges);
      setEdges(updatedEdges);

      // Immediately sync with context
      syncNodesAndEdges(nodes, updatedEdges);

      // ✅ Trigger auto-save after edge changes
      if (job || canvasId) {
        debouncedAutoSave();
      }
    },
    [nodes, edges, syncNodesAndEdges, job, canvasId, debouncedAutoSave]
  );

  // Handle viewport changes for persistence
  const onMove = useCallback((_event: MouseEvent | TouchEvent | null, viewport: Viewport) => {
    setState(prev => ({
      ...prev,
      viewport
    }));

    // ✅ Trigger auto-save after viewport changes
    if (job || canvasId) {
      debouncedAutoSave();
    }
  }, [job, canvasId, debouncedAutoSave]);

  const onConnect = useCallback(
    (connection: Connection) => {
      // Guard against null source or target
      if (!connection.source || !connection.target) {
        showValidationFeedback('Source and target are required', 'error');
        return;
      }

      // Type guard to ensure non-null strings
      if (typeof connection.source !== 'string' || typeof connection.target !== 'string') {
        showValidationFeedback('Invalid connection parameters', 'error');
        return;
      }

      // Find source and target nodes
      const sourceNode = nodes.find(n => n.id === connection.source);
      const targetNode = nodes.find(n => n.id === connection.target);

      if (!sourceNode || !targetNode) {
        showValidationFeedback('Source or target node not found', 'error');
        return;
      }

      // Check for self-connection
      if (connection.source === connection.target) {
        showValidationFeedback('Cannot connect a node to itself', 'error');
        return;
      }

      // Check for cycles
      if (wouldCauseCycle(connection.source, connection.target, edges)) {
        showValidationFeedback('Connection would create a cycle', 'error');
        return;
      }

      // Validate connection using custom validation if provided
      if (propValidateConnection) {
        const validation = propValidateConnection(connection, nodes, edges);
        if (!validation.isValid) {
          showValidationFeedback(validation.errors[0] || 'Invalid connection', 'error');
          return;
        }
        if (validation.warnings.length > 0) {
          validation.warnings.forEach(warning => {
            showValidationFeedback(warning, 'warning');
          });
        }
      }

      // Create edge with metadata - now with guaranteed non-null source/target
      const newEdge = createEdgeWithMetadata(
        {
          ...connection,
          source: connection.source as string,
          target: connection.target as string
        },
        sourceNode as Node<CanvasNodeData>,
        targetNode as Node<CanvasNodeData>
      );

      // Add the edge
      const updatedEdges = addEdge(newEdge, edges);
      setEdges(updatedEdges);

      // Immediately sync with context
      syncNodesAndEdges(nodes, updatedEdges);

      // Notify parent component about edge creation
      if (onEdgeMetadataUpdate) {
        onEdgeMetadataUpdate(newEdge.id, newEdge.data);
      }

      // ✅ Trigger auto-save after creating connection
      if (job || canvasId) {
        debouncedAutoSave();
      }

      // Show success feedback
      showValidationFeedback(
        `Created ${newEdge.data.relationType} connection`,
        'success',
        { x: 100, y: 100 }
      );

      // DO NOT open configuration modal automatically
      // User will double-click edge to configure
    },
    [
      nodes,
      edges,
      propValidateConnection,
      showValidationFeedback,
      wouldCauseCycle,
      onEdgeMetadataUpdate,
      syncNodesAndEdges,
      job,
      canvasId,
      debouncedAutoSave
    ]
  );

  const onSelectionChange = useCallback(({ nodes: selectedNodes }: { nodes: Node[]; edges: Edge[] }) => {
    setState(prev => ({
      ...prev,
      selectedNodeId: selectedNodes.length > 0 ? selectedNodes[0].id : null
    }));
  }, []);

  // ==================== NEW: VIEWPORT CENTERING AND ZOOM ====================
  /**
   * Center nodes on initial load or when nodes change
   */
  const centerNodes = useCallback(() => {
    if (nodes.length > 0 && reactFlowInstance && reactFlowWrapper.current) {
      const container = reactFlowWrapper.current;
      if (!container) return;

      const containerWidth = container.clientWidth;
      const containerHeight = container.clientHeight;

      // If we have nodes, calculate their bounds
      const nodePositions = nodes.map(node => node.position);
      const nodeWidths = nodes.map(node =>
        node.style?.width ? parseFloat(node.style.width as string) : 100
      );
      const nodeHeights = nodes.map(node =>
        node.style?.height ? parseFloat(node.style.height as string) : 100
      );

      // Find the bounds of all nodes
      const minX = Math.min(...nodePositions.map((p, _i) => p.x));
      const maxX = Math.max(...nodePositions.map((p, i) => p.x + nodeWidths[i]));
      const minY = Math.min(...nodePositions.map((p, _i) => p.y));
      const maxY = Math.max(...nodePositions.map((p, i) => p.y + nodeHeights[i]));

      const nodesWidth = maxX - minX;
      const nodesHeight = maxY - minY;
      const centerX = minX + nodesWidth / 2;
      const centerY = minY + nodesHeight / 2;

      // Calculate viewport to center nodes at 1:1 scale
      const viewportX = containerWidth / 2 - centerX;
      const viewportY = containerHeight / 2 - centerY;

      const newViewport = { x: viewportX, y: viewportY, zoom: 1 };

      reactFlowInstance.setViewport(newViewport);

      // Update state
      setState(prev => ({
        ...prev,
        viewport: newViewport
      }));

      // Trigger auto-save after centering
      if (job || canvasId) {
        debouncedAutoSave();
      }
    }
  }, [nodes, reactFlowInstance, reactFlowWrapper, job, canvasId, debouncedAutoSave]);

  // Auto-center nodes when they're first loaded
  useEffect(() => {
    if (nodes.length > 0 && reactFlowInstance && (!state.viewport || state.viewport.zoom !== 1)) {
      // Small delay to ensure container is rendered
      const timer = setTimeout(() => {
        centerNodes();
      }, 100);

      return () => clearTimeout(timer);
    }
  }, [nodes, reactFlowInstance, centerNodes, state.viewport]);

  // ==================== NEW: ROLE SELECTION HANDLERS WITH METADATA AND SYNCHRONIZATION ====================
  const handleRoleSelect = useCallback((selectedRole: 'INPUT' | 'OUTPUT') => {
    if (!pendingRoleSelection) return;

    // Find the node to update
    const nodeToUpdate = nodes.find(n => n.id === pendingRoleSelection.nodeId);
    if (!nodeToUpdate) return;

    // Generate new label with correct role
    const newLabel = nameGenerator.generate(
      pendingRoleSelection.componentId,
      selectedRole
    );

    const extracted = nameGenerator.extractBaseName(newLabel);
    const instanceNumber = extracted?.instance || 1;

    // Get columns from metadata if available
    const columns = extractColumnsFromDragData(pendingRoleSelection.nodeData.metadata);
    const fields: FieldSchema[] = columns.length > 0
      ? columns.map((col: any, index: number) => ({
          id: col.id || `${pendingRoleSelection.componentId}_${index}`,
          name: col.name || `Column_${index + 1}`,
          type: col.type || 'STRING',
          length: col.length,
          precision: col.precision,
          scale: col.scale,
          nullable: col.nullable !== false,
          isKey: col.isKey || col.primaryKey || false,
          defaultValue: col.defaultValue,
          description: col.description,
          originalName: col.originalName,
          transformation: col.expression,
          metadata: { original: col }
        }))
      : [];

    // Create new configuration based on selected role
    const newConfiguration = createInitialComponentConfiguration(
      pendingRoleSelection.componentId,
      selectedRole,
      pendingRoleSelection.nodeData.metadata
    );

    // Update schemas based on selected role
    const schemas: UnifiedNodeMetadata['schemas'] = {};

    if (selectedRole === 'INPUT') {
      schemas.output = fields.length > 0 ? {
        id: `${pendingRoleSelection.componentId}_output_schema`,
        name: `${newLabel} Output Schema`,
        fields,
        isTemporary: false,
        isMaterialized: false,
        metadata: {
          source: 'repository',
          columnCount: fields.length,
          keyColumns: fields.filter(f => f.isKey).length
        }
      } : undefined;

      // Update input configuration schema
      if (newConfiguration.type === 'INPUT') {
        (newConfiguration.config as InputComponentConfiguration).schema = {
          id: `${pendingRoleSelection.componentId}_schema`,
          name: `${newLabel} Schema`,
          fields,
          isTemporary: false,
          isMaterialized: false
        };
      }
    } else if (selectedRole === 'OUTPUT') {
      schemas.input = fields.length > 0 ? [{
        id: `${pendingRoleSelection.componentId}_input_schema`,
        name: `${newLabel} Input Schema`,
        fields,
        isTemporary: false,
        isMaterialized: false,
        metadata: {
          source: 'repository',
          columnCount: fields.length,
          keyColumns: fields.filter(f => f.isKey).length
        }
      }] : undefined;
    }

    // Update the node with the selected role
    setNodes((nds) => {
      const updatedNodes = nds.map((node) => {
        if (node.id === pendingRoleSelection.nodeId) {
          const updatedData: CanvasNodeData = {
            ...pendingRoleSelection.nodeData,
            name: newLabel,
            nodeType: selectedRole === 'INPUT' ? 'input' : 'output',
            componentCategory: pendingRoleSelection.componentDef.category,
            componentType: selectedRole,               // <-- FIX: update componentType to reflect new role
            metadata: {
              ...pendingRoleSelection.nodeData.metadata,
              configuration: newConfiguration,
              schemas,
              updatedAt: new Date().toISOString(),
              isDataSource: true,
              roleSelectedAt: new Date().toISOString(),
              userSelectedRole: selectedRole
            },
            compilerMetadata: {
              ...(pendingRoleSelection.nodeData.metadata?.compilerMetadata || {}),
              lastModified: new Date().toISOString()
            }
          };

          // Notify parent component about node update
          if (onNodeMetadataUpdate) {
            onNodeMetadataUpdate(node.id, updatedData.metadata!);
          }

          return {
            ...node,
            data: updatedData
          };
        }
        return node;
      });

      // Sync updated nodes with context
      syncNodesAndEdges(updatedNodes, edges);

      return updatedNodes;
    });

    // Trigger auto-save after role selection
    debouncedAutoSave();

    // Clear pending selection
    setPendingRoleSelection(null);

    // Show success feedback
    showValidationFeedback(
      `Role set to ${selectedRole} for ${pendingRoleSelection.displayName}`,
      'success',
      pendingRoleSelection.position
    );
  }, [pendingRoleSelection, showValidationFeedback, onNodeMetadataUpdate, nodes, edges, syncNodesAndEdges, debouncedAutoSave]);

  const handleRoleCancel = useCallback(() => {
    if (!pendingRoleSelection) return;

    // Remove the temporary node since user cancelled
    setNodes((nds) => {
      const updatedNodes = nds.filter(node => node.id !== pendingRoleSelection.nodeId);

      // Sync updated nodes with context
      syncNodesAndEdges(updatedNodes, edges);

      return updatedNodes;
    });

    // Decrement the counter since we're removing the temporary node
    nameGenerator.decrementCounter(pendingRoleSelection.componentId, 'TRANSFORM');

    // Trigger auto-save after cancellation
    debouncedAutoSave();

    // Clear pending selection
    setPendingRoleSelection(null);

    // Show cancellation feedback
    showValidationFeedback(
      'Component placement cancelled',
      'info',
      pendingRoleSelection.position
    );
  }, [pendingRoleSelection, showValidationFeedback, edges, syncNodesAndEdges, debouncedAutoSave]);

  // ==================== NODE AND EDGE UPDATE FUNCTIONS WITH METADATA AND SYNCHRONIZATION ====================
  const handleNodeUpdate = useCallback((nodeId: string, updatedData: Partial<CanvasNodeData>) => {
    setNodes(prev => {
      const updatedNodes = prev.map(node => {
        if (node.id === nodeId) {
          const newNodeData = {
            ...node.data,
            ...updatedData,
            metadata: {
              ...node.data.metadata,
              ...updatedData.metadata,
              updatedAt: new Date().toISOString()
            },
            compilerMetadata: {
              ...(node.data.metadata?.compilerMetadata || {}),
              lastModified: new Date().toISOString(),
              incremental: {
                ...(node.data.metadata?.compilerMetadata?.incremental || {}),
                requiresFullRecompile: true,
                changedFields: Object.keys(updatedData)
              }
            }
          };

          // Notify parent component
          if (onNodeMetadataUpdate) {
            onNodeMetadataUpdate(nodeId, newNodeData.metadata!);
          }

          return {
            ...node,
            data: newNodeData
          };
        }
        return node;
      });

      // Sync updated nodes with context
      syncNodesAndEdges(updatedNodes, edges);

      return updatedNodes;
    });

    // Trigger auto-save after node update
    debouncedAutoSave();
  }, [onNodeMetadataUpdate, syncNodesAndEdges, edges, debouncedAutoSave]);

  // ==================== MAP EDITOR FUNCTIONS ====================
  const handleCloseMapEditor = useCallback(() => {
    setState(prev => ({
      ...prev,
      showMapEditor: false,
      selectedNodeForMapEditor: null
    }));
  }, []);

  const handleCloseMatchGroupWizard = useCallback(() => {
    setState(prev => ({
      ...prev,
      showMatchGroupWizard: false,
      selectedNodeForMatchGroupWizard: null
    }));
  }, []);

  // ==================== UPDATED DOUBLE-CLICK HANDLER ====================
  /**
   * Handles double-click on TMap nodes to open MapEditor with connected columns
   */
  const handleCanvasNodeDoubleClick = useCallback((event: CustomEvent) => {
    const { componentMetadata, nodeMetadata } = event.detail;
    const metadata = nodeMetadata || componentMetadata;

    if (metadata) {
      // NEW: First check if it's a tMap node
      if (metadata.componentKey === 'tMap') {
        handleTMapDoubleClick(metadata.id);
        return; // Don't proceed to old Map component logic
      }

      // Existing logic for other Map components continues here...
      const isMapComponent = metadata.name?.includes('Map') ||
                            metadata.type === 'tMap' ||
                            metadata.componentKey?.includes('Map');

      if (isMapComponent) {
        const node = nodes.find(n => n.id === metadata.id);
        if (node) {
          // Get connected input and output nodes
          const { inputNodes, outputNodes } = getMapNodeConnections(
            node as Node<CanvasNodeData>,
            nodes as Node<CanvasNodeData>[],
            edges
          );

          // Extract columns from connected nodes with proper direction and de-duplication
          const sourceTables: TableDefinition[] = inputNodes.map((inputNode, index) => {
            const columns = extractColumnsFromNode(inputNode);

            // De-duplicate columns by name
            const uniqueColumns = columns.filter((col, idx, arr) =>
              arr.findIndex(c => c.name === col.name) === idx
            );

            return {
              id: `input-${inputNode.id}`,
              name: inputNode.data.name || `Input ${index + 1}`,
              type: 'input' as const,
              columns: uniqueColumns.map(col => ({
                ...col,
                id: `${inputNode.id}_${col.name}`.replace(/[^a-zA-Z0-9]/g, '_')
              }))
            };
          });

          const targetTables: TableDefinition[] = outputNodes.map((outputNode, index) => {
            const columns = extractColumnsFromNode(outputNode);

            // De-duplicate columns by name
            const uniqueColumns = columns.filter((col, idx, arr) =>
              arr.findIndex(c => c.name === col.name) === idx
            );

            return {
              id: `output-${outputNode.id}`,
              name: outputNode.data.name || `Output ${index + 1}`,
              type: 'output' as const,
              columns: uniqueColumns.map(col => ({
                ...col,
                id: `${outputNode.id}_${col.name}`.replace(/[^a-zA-Z0-9]/g, '_')
              }))
            };
          });

          // If no connected nodes found, show warning
          if (sourceTables.length === 0 && targetTables.length === 0) {
            showValidationFeedback(
              'No connected input or output nodes found. Please connect nodes first.',
              'warning',
              { x: 100, y: 100 }
            );
            return;
          }

          // Store canvas context with source and target tables
          const canvasContext = {
            sourceTables,
            targetTables,
            inputNodeIds: inputNodes.map(n => n.id),
            outputNodeIds: outputNodes.map(n => n.id),
            lastOpened: new Date().toISOString()
          };

          setState(prev => ({
            ...prev,
            showMapEditor: true,
            selectedNodeForMapEditor: {
              ...node,
              canvasContext,
              sourceTables,
              targetTables
            }
          }));
        }
      }
    }
  }, [nodes, edges, showValidationFeedback, handleTMapDoubleClick]);

  // Add event listeners for node and edge double-click
  useEffect(() => {
    window.addEventListener('canvas-node-double-click', handleCanvasNodeDoubleClick as EventListener);

    // Add tMap double-click handler
    const handleTMapDoubleClickEvent = (event: Event) => {
      const customEvent = event as CustomEvent;
      console.log('Received tMap double-click event:', customEvent.detail);
      handleTMapDoubleClick(customEvent.detail.nodeId);
    };

    window.addEventListener('canvas-tmap-double-click', handleTMapDoubleClickEvent);

    // Add edge double-click handler
    const handleEdgeDoubleClick = (event: CustomEvent) => {
      const { edgeId } = event.detail;

      // Here you would typically open the edge configuration UI
      showValidationFeedback(
        `Double-clicked edge: ${edgeId}. Open configuration UI here.`,
        'info',
        { x: 100, y: 100 }
      );
    };

    window.addEventListener('canvas-edge-double-click', handleEdgeDoubleClick as EventListener);

    return () => {
      window.removeEventListener('canvas-node-double-click', handleCanvasNodeDoubleClick as EventListener);
      window.removeEventListener('canvas-tmap-double-click', handleTMapDoubleClickEvent);
      window.removeEventListener('canvas-edge-double-click', handleEdgeDoubleClick as EventListener);
    };
  }, [handleCanvasNodeDoubleClick, handleTMapDoubleClick, showValidationFeedback]);

  // ==================== NEW: TMap EDITOR MODAL WITH METADATA ====================
  /**
   * Render tMap Editor modal with metadata support
   */
  const renderTMapEditorModal = () => {
    if (!state.mapEditorState.isOpen || !state.mapEditorState.data) return null;

    const { nodeId, inputColumns, outputColumns } = state.mapEditorState.data;
    const nodeMetadata = state.mapEditorState.nodeMetadata;
    const nodeLabel = nodeMetadata?.name || nodeId;

    // Extract initial configuration from metadata
    const initialConfig = nodeMetadata && isMapConfig(nodeMetadata.metadata?.configuration)
      ? nodeMetadata.metadata.configuration.config
      : undefined;

    return (
      <div className="fixed inset-0 z-[9999] bg-black bg-opacity-70 flex items-center justify-center p-4">
        <motion.div
          initial={{ opacity: 0, scale: 0.95 }}
          animate={{ opacity: 1, scale: 1 }}
          className="bg-white rounded-xl shadow-2xl w-full max-w-6xl h-[90vh] overflow-hidden"
        >
          <div className="flex justify-between items-center p-6 border-b bg-gradient-to-r from-blue-50 to-gray-50">
            <div>
              <h2 className="text-xl font-bold flex items-center">
                <span className="mr-2">🗺️</span>
                Map Editor
                <span className="ml-2 text-xs bg-purple-100 text-purple-800 px-2 py-0.5 rounded">
                  Metadata Strategy v1.0
                </span>
              </h2>
              <p className="text-sm text-gray-600 mt-1">
                Editing tMap node: <span className="font-semibold text-blue-600">{nodeLabel}</span>
                <span className="ml-3 text-xs bg-gray-100 text-gray-800 px-2 py-0.5 rounded">
                  {inputColumns.length} input columns • {outputColumns.length} output columns
                </span>
                {initialConfig && (
                  <span className="ml-3 text-xs bg-green-100 text-green-800 px-2 py-0.5 rounded">
                    {initialConfig.transformations.length} existing transformations
                  </span>
                )}
              </p>
            </div>
            <button
              onClick={closeTMapEditor}
              className="p-2 hover:bg-gray-100 rounded-full transition-colors"
              title="Close Map Editor"
            >
              <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>

          <div className="h-full overflow-hidden">
            <Suspense fallback={
              <div className="flex items-center justify-center h-full">
                <div className="text-center">
                  <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500 mx-auto mb-4"></div>
                  <p className="text-gray-600">Loading Map Editor with Metadata...</p>
                </div>
              </div>
            }>
              <MapEditor
                nodeId={nodeId}
                nodeMetadata={nodeMetadata}
                inputColumns={inputColumns}
                outputColumns={outputColumns}
                initialConfig={initialConfig}
                onClose={closeTMapEditor}
                onSave={handleMapEditorSave}
              />
            </Suspense>
          </div>
        </motion.div>
      </div>
    );
  };

  // ==================== SQL GENERATION ON RUN ====================
  useEffect(() => {
    const handleToolbarRun = async (event: Event) => {
      const customEvent = event as CustomEvent;
      const { jobName, nodes: runNodes, edges: runEdges } = customEvent.detail;

      console.log(`🎬 Canvas received run request for job: ${jobName}`);

      try {
        // Convert React Flow nodes (UnifiedCanvasNode) to the old PipelineCanvasNode format
        // that the SQL generators still expect. We populate the legacy fields from the unified metadata.
        const canvasNodes: PipelineCanvasNode[] = runNodes.map((node: Node<CanvasNodeData>) => {
          const unified = node.data;
          const config = unified.metadata?.configuration;
          const nodeType = unified.type;

          // Extract legacy fields based on node type
          let joinConfig: any = undefined;
          let filterConfig: any = undefined;
          let aggregationConfig: any = undefined;
          let sortConfig: any = undefined;
          let transformationRules: any = undefined;
          let schemaMappings: any = undefined;
          let tableMapping: any = undefined;

          if (config) {
            if (isJoinConfig(config)) {
              joinConfig = {
                type: config.config.joinType,
                condition: config.config.joinConditions.map(j => `${j.leftTable}.${j.leftField} = ${j.rightTable}.${j.rightField}`).join(' AND '),
                leftAlias: config.config.joinConditions[0]?.leftTable,
                rightAlias: config.config.joinConditions[0]?.rightTable,
                whereClause: undefined
              };
              transformationRules = config.config.joinConditions.map((jc, idx) => ({
                id: jc.id,
                type: 'join',
                params: jc,
                order: idx
              }));
            } else if (isFilterConfig(config)) {
              filterConfig = {
                condition: config.config.filterConditions.map(fc => `${fc.field} ${fc.operator} ${fc.value}`).join(` ${config.config.filterLogic} `),
                operation: 'INCLUDE',
                parameters: config.config.filterConditions.reduce((acc, fc) => {
                  if (fc.valueType === 'PARAMETER') acc[fc.field] = fc.value;
                  return acc;
                }, {} as Record<string, any>)
              };
              transformationRules = config.config.filterConditions.map((fc, idx) => ({
                id: fc.id,
                type: 'filter',
                params: fc,
                order: idx
              }));
            } else if (isAggregateConfig(config)) {
              aggregationConfig = {
                groupBy: config.config.groupByFields,
                aggregates: config.config.aggregateFunctions.map(af => ({
                  column: af.field,
                  function: af.function,
                  alias: af.alias
                })),
                having: config.config.havingConditions?.map(hc => `${hc.field} ${hc.operator} ${hc.value}`).join(' AND ')
              };
              transformationRules = config.config.aggregateFunctions.map((af, idx) => ({
                id: af.id,
                type: 'aggregate',
                params: af,
                order: idx
              }));
            } else if (isSortConfig(config)) {
              sortConfig = {
                columns: config.config.sortFields.map(sf => ({
                  column: sf.field,
                  direction: sf.direction,
                  nullsFirst: sf.nullsFirst
                })),
                limit: config.config.sqlGeneration.limitOffset?.limit,
                offset: config.config.sqlGeneration.limitOffset?.offset
              };
            } else if (isMapConfig(config)) {
              // Convert map transformations to transformationRules
              transformationRules = config.config.transformations.map(t => ({
                id: t.id,
                type: 'map',
                params: t,
                order: t.position
              }));
              schemaMappings = config.config.transformations.map(t => ({
                sourceColumn: t.sourceField,
                targetColumn: t.targetField,
                transformation: t.expression,
                dataTypeConversion: t.dataType ? { from: 'unknown', to: mapDataTypeToPostgreSQL(t.dataType) } : undefined,
                isRequired: true,
                defaultValue: t.defaultValue
              }));
            } else if (isInputConfig(config)) {
              // For input nodes, build tableMapping
              const fields = config.config.schema?.fields || [];
              tableMapping = {
                schema: 'public',
                name: config.config.sourceDetails.tableName || unified.name,
                columns: fieldsToPostgresColumns(fields)
              };
            } else if (isOutputConfig(config)) {
              // Output nodes might not need mapping here, but we can provide schemaMapping if needed
              schemaMappings = config.config.schemaMapping;
            }
          }

          return {
            id: unified.id,
            name: unified.name,
            type: nodeType,
            metadata: {
              joinConfig,
              filterConfig,
              aggregationConfig,
              sortConfig,
              transformationRules,
              schemaMappings,
              tableMapping,
              postgresConfig: unified.metadata?.postgresConfig,
              description: unified.metadata?.description
            }
          };
        });

        // Convert edges (similar extraction could be done if needed)
        const canvasConnections: PipelineCanvasConnection[] = runEdges.map((edge: Edge) => ({
          id: edge.id,
          sourceNodeId: edge.source,
          targetNodeId: edge.target,
          status: ConnectionStatus.VALID,
          dataFlow: {
            schemaMappings: edge.data?.configuration?.fieldMappings || [],
            relationType: edge.data?.relationType,
          },
          metadata: edge.data?.metadata || {}
        }));

        // Run the pipeline
        const result: PipelineGenerationResult = await generatePipelineSQL(
          canvasNodes,
          canvasConnections,
          {
            includeComments: true,
            formatSQL: true,
            useCTEs: true,
            logLevel: 'info',
            progressCallback: (progress) => {
              dispatch(addLog({
                level: 'INFO',
                message: `[${progress.stage}] ${progress.progress}% - ${progress.message}`,
                source: 'SQL Generation'
              }));
            }
          }
        );

        // Log outcome
        if (result.errors.length === 0) {
          dispatch(addLog({
            level: 'SUCCESS',
            message: `✅ SQL generation successful for job ${jobName}`,
            source: 'SQL Generation'
          }));
          dispatch(addLog({
            level: 'INFO',
            message: `📄 Generated SQL (${result.sql.length} characters)`,
            source: 'SQL Generation'
          }));
        } else {
          result.errors.forEach(error => {
            dispatch(addLog({
              level: 'ERROR',
              message: `❌ [${error.code}] ${error.message}`,
              source: 'SQL Generation'
            }));
          });
        }
console.log('📝 [Canvas Run] Full SQL:', result.sql);
        // Dispatch completion event
        window.dispatchEvent(new CustomEvent('run-complete', {
          detail: {
            success: result.errors.length === 0,
            sql: result.sql,
            errors: result.errors,
            warnings: result.warnings
          }
        }));

      } catch (error: any) {
        console.error('Error generating SQL:', error);
        dispatch(addLog({
          level: 'ERROR',
          message: `❌ SQL generation failed: ${error.message}`,
          source: 'SQL Generation'
        }));
        window.dispatchEvent(new CustomEvent('run-complete', {
          detail: {
            success: false,
            error: error.message
          }
        }));
      }
    };

    window.addEventListener('toolbar-run', handleToolbarRun);

    return () => {
      window.removeEventListener('toolbar-run', handleToolbarRun);
    };
  }, [dispatch]); // Note: we don't depend on nodes/edges because they are passed in the event

  // ==================== RENDER COMPONENTS ====================
  const renderConnectionFeedback = () => {
    if (!state.connectionFeedback.isVisible) return null;

    const { message, type, position } = state.connectionFeedback;
    const colors = {
      success: 'bg-green-100 border-green-400 text-green-700',
      error: 'bg-red-100 border-red-400 text-red-700',
      info: 'bg-blue-100 border-blue-400 text-blue-700',
      warning: 'bg-yellow-100 border-yellow-400 text-yellow-700'
    };

    return (
      <div className="fixed z-[10000]" style={{ left: position.x, top: position.y }}>
        <div className={`px-3 py-2 rounded-lg border ${colors[type]} shadow-lg animate-fadeIn`}>
          <div className="text-sm font-medium">{message}</div>
        </div>
      </div>
    );
  };

  const renderRoleSelectionPopup = () => {
    if (!pendingRoleSelection) return null;

    return (
      <RoleSelectionModal
        componentType={pendingRoleSelection.componentId}
        displayName={pendingRoleSelection.displayName}
        position={pendingRoleSelection.dropPosition}
        onSelect={handleRoleSelect}
        onCancel={handleRoleCancel}
      />
    );
  };

  const renderMapEditorModal = () => {
    if (!state.showMapEditor || !state.selectedNodeForMapEditor) return null;

    const sourceTables = state.selectedNodeForMapEditor.sourceTables || [];
    const targetTables = state.selectedNodeForMapEditor.targetTables || [];

    return (
      <div className="map-editor-modal fixed inset-0 z-[9999] bg-black bg-opacity-70 flex items-center justify-center p-4">
        <motion.div
          initial={{ opacity: 0, scale: 0.95, y: -20 }}
          animate={{ opacity: 1, scale: 1, y: 0 }}
          exit={{ opacity: 0, scale: 0.95, y: 20 }}
          transition={{ duration: 0.2 }}
          className="map-editor-content relative w-full h-full max-w-[95vw] max-h-[95vh] bg-white rounded-xl shadow-2xl overflow-hidden"
          onClick={(e) => e.stopPropagation()}
        >
          <button
            onClick={handleCloseMapEditor}
            className="absolute top-4 right-4 z-50 bg-red-500 hover:bg-red-600 text-white rounded-full w-8 h-8 flex items-center justify-center shadow-lg transition-all"
            title="Close Map Editor"
            aria-label="Close Map Editor"
          >
            ✕
          </button>

          <Suspense
            fallback={
              <div className="flex items-center justify-center h-full">
                <div className="text-center">
                  <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500 mx-auto mb-4"></div>
                  <p className="text-gray-600">Loading Map Editor...</p>
                </div>
              </div>
            }
          >
            <MapEditor
              sourceTables={sourceTables}
              targetTables={targetTables}
              initialConfig={state.selectedNodeForMapEditor.metadata?.mapConfiguration}
              onClose={handleCloseMapEditor}
            />
          </Suspense>
        </motion.div>
      </div>
    );
  };

  const renderMatchGroupWizard = () => {
    if (!state.showMatchGroupWizard || !state.selectedNodeForMatchGroupWizard) return null;

    return (
      <div className="fixed inset-0 z-[9999] bg-black bg-opacity-70 flex items-center justify-center p-4">
        <motion.div
          initial={{ opacity: 0, scale: 0.95 }}
          animate={{ opacity: 1, scale: 1 }}
          className="bg-white rounded-xl shadow-2xl w-full max-w-4xl h-[80vh]"
          onClick={(e) => e.stopPropagation()}
        >
          <div className="p-6">
            <div className="flex justify-between items-center mb-6">
              <h2 className="text-xl font-bold">Match Group Configuration Wizard</h2>
              <button
                onClick={handleCloseMatchGroupWizard}
                className="text-gray-500 hover:text-gray-700"
              >
                ✕
              </button>
            </div>
            <p className="text-gray-600 mb-4">
              Configure match group settings for: {state.selectedNodeForMatchGroupWizard.name}
            </p>
            <div className="text-center py-8">
              <p className="text-gray-500">Match Group Wizard UI would be implemented here</p>
              <button
                onClick={() => {
                  if (state.selectedNodeForMatchGroupWizard) {
                    const config: WizardConfig = {
                      currentStep: 1,
                      inputFlow: 'default',
                      schemaColumns: [],
                      groupingKeys: [],
                      survivorshipRules: [],
                      outputMapping: {},
                      outputTableName: 'matched_output'
                    };

                    // Merge existing metadata to preserve required fields like 'configuration'
                    const existingMetadata = state.selectedNodeForMatchGroupWizard.metadata || {};
                    handleNodeUpdate(state.selectedNodeForMatchGroupWizard.id, {
                      metadata: {
                        ...existingMetadata,
                        matchGroupConfig: config,
                        lastConfigured: new Date().toISOString()
                      }
                    });
                  }
                  handleCloseMatchGroupWizard();
                }}
                className="mt-4 px-4 py-2 bg-purple-600 text-white rounded hover:bg-purple-700"
              >
                Save Configuration
              </button>
            </div>
          </div>
        </motion.div>
      </div>
    );
  };

  // Render auto-save status indicator
  const renderAutoSaveStatus = () => {
    if ((!job && !canvasId) || state.autoSaveStatus === 'idle') return null;

    const statusConfig = {
      saving: { text: 'Saving...', color: 'bg-yellow-400', icon: '⏳' },
      saved: { text: 'Saved', color: 'bg-green-400', icon: '✅' },
      error: { text: 'Save failed', color: 'bg-red-400', icon: '❌' }
    };

    const config = statusConfig[state.autoSaveStatus];

    return (
      <div className="fixed bottom-4 right-4 z-50 flex items-center space-x-2">
        <div className={`${config.color} text-white px-3 py-2 rounded-lg shadow-lg flex items-center space-x-2`}>
          <span>{config.icon}</span>
          <span className="text-sm font-medium">{config.text}</span>
          {state.lastSavedAt && state.autoSaveStatus === 'saved' && (
            <span className="text-xs opacity-80">
              {new Date(state.lastSavedAt).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
            </span>
          )}
        </div>
      </div>
    );
  };

  // ==================== MAIN RENDER ====================
  return (
    <>
      {/* Role Selection Popup */}
      {renderRoleSelectionPopup()}

      {/* Connection Feedback Overlay */}
      {renderConnectionFeedback()}

      {/* Auto-save Status Indicator */}
      {renderAutoSaveStatus()}

      {/* tMap Editor Modal with Metadata */}
      {renderTMapEditorModal()}

      {/* Map Editor Modal */}
      {renderMapEditorModal()}

      {/* Match Group Wizard */}
      {renderMatchGroupWizard()}

      {/* Main React Flow Canvas WITH ENHANCED VALIDATION AND METADATA */}
      <div
        ref={reactFlowWrapper}
        className={`relative w-full h-full canvas-container bg-gray-50`}
        style={{
          width: '100%',
          height: '100%',
          position: 'relative',
          cursor: 'default'
        }}
      >

        {/* ReactFlow WITH ENHANCED VALIDATION AND METADATA */}
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          onInit={setReactFlowInstance}
          nodeTypes={nodeTypes}
          onSelectionChange={onSelectionChange}
          // ENHANCED VALIDATION INTEGRATION
          isValidConnection={isValidConnection}
          // Edge double-click handler
          onEdgeDoubleClick={(event, edge) => {
            event.stopPropagation();
            const customEvent = new CustomEvent('canvas-edge-double-click', {
              detail: {
                edgeId: edge.id,
                edgeMetadata: edge.data
              }
            });
            window.dispatchEvent(customEvent);
          }}
          // UNIFIED REACT FLOW DROP HANDLERS
          onDrop={onDrop}
          onDragOver={onDragOver}
          // ✅ Viewport change handler for persistence
          onMove={onMove}

          connectionMode={ConnectionMode.Loose}
          connectionLineType={ConnectionLineType.SmoothStep}
          snapToGrid={true}
          snapGrid={[15, 15]}

          // ========== FIXED VIEWPORT CONFIGURATION ==========
          // Set default viewport to 1:1 scale and center
          defaultViewport={{
            x: state.viewport.x,
            y: state.viewport.y,
            zoom: state.viewport.zoom
          }}

          minZoom={0.1}
          maxZoom={4}
          defaultEdgeOptions={{
            animated: false,
            style: { strokeWidth: 2 },
            markerEnd: {
              type: MarkerType.ArrowClosed,
              width: 20,
              height: 20,
            },
          }}
          proOptions={{
            hideAttribution: true,
          }}
          selectionMode={SelectionMode.Partial}
          deleteKeyCode={['Delete', 'Backspace']}
          multiSelectionKeyCode={['Control', 'Meta']}
          selectionKeyCode={['Shift']}
          nodesDraggable={true}
          nodesConnectable={true}
          elementsSelectable={true}
          selectNodesOnDrag={true}
          panOnDrag={[1, 2]}
          panOnScroll={true}
          zoomOnScroll={true}
          zoomOnDoubleClick={false}
          onlyRenderVisibleElements={false}
          style={{
            width: '100%',
            height: '100%',
            position: 'relative'
          }}
        >
          <Background variant={BackgroundVariant.Dots} gap={20} size={1} color="#e5e7eb" />
          <Controls />
        </ReactFlow>
      </div>
      <style dangerouslySetInnerHTML={{ __html: `
  @keyframes fadeIn {
    from { opacity: 0; transform: translateY(-10px); }
    to { opacity: 1; transform: translateY(0); }
  }

  .animate-fadeIn {
    animation: fadeIn 0.3s ease-out;
  }

.react-flow__node-talendNode {
  transform-box: fill-box !important;
  transform-origin: center !important;
  box-sizing: border-box !important;
}

  .react-flow__node-talendNode:hover {
    transform: translateY(-1px) scale(1.02);
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.12);
  }

  .react-flow__node-talendNode.selected {
    box-shadow: 0 0 0 2px #3b82f6, 0 4px 12px rgba(0, 0, 0, 0.12);
    z-index: 10;
  }

  .react-flow__handle {
    opacity: 0.7;
    transition: opacity 0.2s ease, transform 0.2s ease;
    border: 1.5px solid white;
    width: 8px !important;
    height: 8px !important;
  }

  .react-flow__handle:hover {
    opacity: 1;
    transform: scale(1.2);
  }

  .react-flow__edge-path {
    stroke-width: 2;
    stroke: #4b5563;
  }

  .react-flow__edge.selected .react-flow__edge-path {
    stroke: #3b82f6;
    stroke-width: 2.5;
  }

  /* Metadata highlight for tMap nodes */
  .react-flow__node-talendNode[data-component-key="tMap"] {
    border-width: 3px;
    border-color: #8b5cf6;
  }

  /* Validation feedback styles */
  .validation-error-handle {
    background-color: #ef4444 !important;
    border-color: #dc2626 !important;
  }

  .validation-warning-handle {
    background-color: #f59e0b !important;
    border-color: #d97706 !important;
  }

  .validation-valid-handle {
    background-color: #10b981 !important;
    border-color: #059669 !important;
  }

  /* Metadata status indicator */
  .metadata-status-indicator {
    position: absolute;
    top: 2px;
    right: 2px;
    width: 6px;
    height: 6px;
    border-radius: 50%;
    z-index: 10;
  }

  .metadata-valid {
    background-color: #10b981;
  }

  .metadata-warning {
    background-color: #f59e0b;
  }

  .metadata-error {
    background-color: #ef4444;
  }

  /* Canvas container should always fill available space */
  .canvas-container {
    position: absolute !important;
    width: 100% !important;
    height: 100% !important;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
  }

  /* Ensure React Flow fills the container */
  .react-flow {
    width: 100% !important;
    height: 100% !important;
    position: absolute !important;
  }

  .react-flow__pane {
    width: 100% !important;
    height: 100% !important;
  }

  /* Console overlay compatibility */
  .console-interaction-active .react-flow__viewport,
  .console-interaction-active .react-flow__nodes,
  .console-interaction-active .react-flow__edges,
  .console-interaction-active .react-flow__pane {
    pointer-events: none !important;
  }
`}} />
    </>
  );
});

export default Canvas;