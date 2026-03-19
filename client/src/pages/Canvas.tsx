// src/pages/Canvas.tsx (FULL with all editors integrated)
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

import MapEditor from '../components/Editor/Mapping/MapEditor';

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
  UnifiedNodeMetadata,
  ComponentConfiguration,
  MapComponentConfiguration,
  JoinComponentConfiguration,
  FilterComponentConfiguration,
  AggregateComponentConfiguration,
  SortComponentConfiguration,
  InputComponentConfiguration,
  NodeType,
  DataSourceType,
  PostgreSQLDataType,
  FieldSchema,
  SchemaDefinition,
  DataType,
  isMapConfig,
  isJoinConfig,
  isFilterConfig,
  isAggregateConfig,
  isSortConfig,
  isInputConfig,
  isOutputConfig,
  getComponentConfig,
  PostgresColumn,
  NodeStatus,
  ReplaceComponentConfiguration,
  ExtractDelimitedFieldsConfiguration,
  ExtractJSONFieldsConfiguration,
  ExtractXMLFieldConfiguration,
  ConvertComponentConfiguration,
  NormalizeNumberComponentConfiguration,
  NormalizeComponentConfiguration,
  ReplicateComponentConfiguration,
  MatchGroupComponentConfiguration,
  FilterColumnsComponentConfiguration,
  FileLookupComponentConfiguration,
  UnpivotRowComponentConfiguration,
  UniteComponentConfiguration,
  UniqRowComponentConfiguration,
  DenormalizeSortedRowComponentConfiguration,
  DenormalizeComponentConfiguration,
  PivotToColumnsDelimitedConfiguration,
} from '../types/unified-pipeline.types';

// Import column extraction utilities
import { getConnectedColumns, MapEditorPayload } from '../utils/columnExtraction';

// Import persistence service
import { canvasPersistence, CanvasRecord } from '../services/canvas-persistence.service';

// Import SQL generation
import { useAppDispatch } from '../hooks';
import { addLog } from '../store/slices/logsSlice';
import { generatePipelineSQL, PipelineGenerationResult } from '../generators/SQLGenerationPipeline';
import { CanvasNode as PipelineCanvasNode, CanvasConnection as PipelineCanvasConnection, ConnectionStatus } from '../types/pipeline-types';

// NEW: Import database API for execution
import databaseApi from '../services/database-api.service';
import { ClientQueryExecutionResult } from '../services/database-api.types';

// ==================== IMPORT ALL EDITORS ====================
import { SortEditor } from '../components/Editor/JoinsAndLookups/SortEditor';
import ReplaceEditor from '../components/Editor/Mapping/ReplaceEditor';
import JoinEditor from '../components/Editor/JoinsAndLookups/JoinEditor';
import { FilterRowConfigModal } from '../components/Editor/JoinsAndLookups/FilterRowConfigModal';
import ExtractXMLFieldEditor from '../components/Editor/Parsing/ExtractXMLFieldEditor';
import { ExtractJSONFieldsEditor } from '../components/Editor/Parsing/ExtractJSONFieldsEditor';
import { ExtractDelimitedFieldsConfigModal } from '../components/Editor/Parsing/ExtractDelimitedFieldsConfigModal';
import { AggregateEditor } from '../components/Editor/Aggregates/AggregateEditor'; // NEW: replaced old import
import { ConvertTypeEditor } from '../components/Editor/Mapping/ConvertTypeEditor';
import ReplaceListEditor from '../components/Editor/Mapping/ReplaceListEditor';
import NormalizeNumberEditor from '../components/Editor/Mapping/NormalizeNumberEditor';
import NormalizeEditor from '../components/Editor/Mapping/NormalizeEditor';
import ReplicateEditor from '../components/Editor/JoinsAndLookups/ReplicateEditor';
import { RecordMatchingEditor } from '../components/Editor/JoinsAndLookups/RecordMatchingEditor';
import { MatchGroupEditor } from '../components/Editor/JoinsAndLookups/MatchGroupEditor';
import FilterColumnsEditor from '../components/Editor/JoinsAndLookups/FilterColumnsEditor';
import { FileLookupEditor } from '../components/Editor/JoinsAndLookups/FileLookupEditor';

// ==================== NEW EDITORS ====================
import UnpivotRowEditor from '../components/Editor/Aggregates/UnpivotRowEditor';
import { UniteEditor } from '../components/Editor/Aggregates/UniteEditor';
import UniqRowEditor from '../components/Editor/Aggregates/UniqRowEditor';
import SplitRowEditor from '../components/Editor/Aggregates/SplitRowEditor';
import { PivotToColumnsDelimitedEditor } from '../components/Editor/Aggregates/PivotToColumnsDelimitedEditor';
import { DenormalizeSortedRowEditor } from '../components/Editor/Aggregates/DenormalizeSortedRowEditor';
import { DenormalizeEditor } from '../components/Editor/Aggregates/DenormalizeEditor';

// ==================== TYPES ====================
interface ReactFlowDragData {
  type: 'reactflow-component';
  componentId: string;
  source: 'sidebar' | 'rightPanel';
  metadata?: Record<string, any>;
}

// Our node data is now UnifiedCanvasNode
type CanvasNodeData = UnifiedCanvasNode;

// Helper: SimpleColumn for editors
interface SimpleColumn {
  name: string;
  type?: string;
  id?: string;
}

// Interface for UniteEditor input schemas
interface InputSchema {
  id: string;
  name: string;
  fields: Array<{ name: string; type: string; nullable: boolean }>;
}

// Union type for active editor (extended with new types)
type ActiveEditor =
  | null
  | { type: 'map'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; outputColumns: SimpleColumn[]; initialConfig?: MapComponentConfiguration }
  | { type: 'sort'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: SortComponentConfiguration }
  | { type: 'replace'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; outputColumns: SimpleColumn[]; initialConfig?: ReplaceComponentConfiguration }
  | { type: 'join'; nodeId: string; nodeMetadata: CanvasNodeData; leftSchema: { id: string; name: string; fields: FieldSchema[] }; rightSchema: { id: string; name: string; fields: FieldSchema[] }; initialConfig?: JoinComponentConfiguration }
  | { type: 'filter'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: FilterComponentConfiguration }
  | { type: 'extractXML'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: ExtractXMLFieldConfiguration }
  | { type: 'extractJSON'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: ExtractJSONFieldsConfiguration }
  | { type: 'extractDelimited'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: ExtractDelimitedFieldsConfiguration }
  | { type: 'convert'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; outputColumns: SimpleColumn[]; initialConfig?: ConvertComponentConfiguration }
  | { type: 'aggregate'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: AggregateComponentConfiguration }
  | { type: 'replaceList'; nodeId: string; nodeMetadata: CanvasNodeData; inputSchema: SchemaDefinition; initialConfig?: ReplaceComponentConfiguration }
  | { type: 'normalizeNumber'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: NormalizeNumberComponentConfiguration }
  | { type: 'normalize'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: NormalizeComponentConfiguration }
  | { type: 'replicate'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: ReplicateComponentConfiguration }
  | { type: 'recordMatching'; nodeId: string; nodeMetadata: CanvasNodeData; inputFields: FieldSchema[]; initialConfig?: MatchGroupComponentConfiguration }
  | { type: 'matchGroup'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: MatchGroupComponentConfiguration }
  | { type: 'filterColumns'; nodeId: string; nodeMetadata: CanvasNodeData; inputSchema?: SchemaDefinition; initialConfig?: FilterColumnsComponentConfiguration }
  | { type: 'fileLookup'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: FileLookupComponentConfiguration }
  // ==================== NEW TYPES ====================
  | { type: 'unpivotRow'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: UnpivotRowComponentConfiguration }
  | { type: 'unite'; nodeId: string; nodeMetadata: CanvasNodeData; inputSchemas: InputSchema[]; initialConfig?: UniteComponentConfiguration }
  | { type: 'uniqRow'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: UniqRowComponentConfiguration }
  | { type: 'splitRow'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: NormalizeComponentConfiguration }
  | { type: 'pivotToColumnsDelimited'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: PivotToColumnsDelimitedConfiguration }
  | { type: 'denormalizeSortedRow'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: DenormalizeSortedRowComponentConfiguration }
  | { type: 'denormalize'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: DenormalizeComponentConfiguration };

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

// ==================== HELPER: GET ACTIVE POSTGRES CONNECTION ====================
const getActivePostgresConnectionId = async (): Promise<string | null> => {
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
      zoom: 1
    },
    mapEditorState: {
      isOpen: false,
      data: null
    },
    autoSaveStatus: 'idle',
    lastSavedAt: undefined
  });

  // Role selection state
  const [pendingRoleSelection, setPendingRoleSelection] = useState<PendingRoleSelection | null>(null);

  // ==================== ACTIVE EDITOR STATE ====================
  const [activeEditor, setActiveEditor] = useState<ActiveEditor>(null);

  // ==================== SYNC WITH CANVAS CONTEXT ====================
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
  const saveCanvasState = useCallback(async () => {
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

      if (canvasId) {
        await canvasPersistence.updateCanvas(canvasId, {
          nodes,
          edges,
          viewport: state.viewport
        });
        savedRecord = { id: canvasId } as CanvasRecord;
      }
      else if (localCanvasIdRef.current) {
        await canvasPersistence.updateCanvas(localCanvasIdRef.current, {
          nodes,
          edges,
          viewport: state.viewport
        });
        savedRecord = { id: localCanvasIdRef.current } as CanvasRecord;
      }
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

  const debouncedAutoSave = useCallback(() => {
    if (saveTimeoutRef.current) {
      clearTimeout(saveTimeoutRef.current);
    }

    if ((!job && !canvasId) || isSavingRef.current) {
      return;
    }

    saveTimeoutRef.current = setTimeout(() => {
      saveCanvasState();
    }, 1000);
  }, [job, canvasId, saveCanvasState]);

  useImperativeHandle(ref, () => ({
    forceSave: saveCanvasState
  }), [saveCanvasState]);

  useEffect(() => {
    if (canvasId) {
      localCanvasIdRef.current = canvasId;
    }
  }, [canvasId]);

  // ==================== INITIALIZATION & LOADING ====================
  useEffect(() => {
    const initializeCanvas = async () => {
      if (!canvasId && !job) {
        console.log('ℹ️ [Canvas] No canvasId or job provided – skipping load.');
        return;
      }

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

        setNodes(loadedNodes);
        setEdges(loadedEdges);

        if (loadedViewport && reactFlowInstance) {
          setViewport(loadedViewport);
        }

        setState(prev => ({
          ...prev,
          viewport: loadedViewport,
          lastSavedAt: canvasId ? new Date().toISOString() : undefined
        }));

        updateCanvasData({
          nodes: loadedNodes,
          edges: loadedEdges,
          viewport: loadedViewport
        });

        if (loadedNodes.length > 0 || loadedEdges.length > 0) {
          showValidationFeedback(
            `Loaded canvas with ${loadedNodes.length} nodes, ${loadedEdges.length} edges`,
            'success',
            { x: 100, y: 100 }
          );
        }

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

  useEffect(() => {
    const nodeLabels = nodes
      .filter(node => node.type === 'talendNode')
      .map(node => node.data?.name)
      .filter(Boolean);

    nameGenerator.initializeFromExistingNodes(nodeLabels);
  }, [nodes]);

  // ==================== VALIDATION INITIALIZATION ====================
  useEffect(() => {
    const registry = new SchemaRegistry();
    registry.registerSchemas(DefaultSchemas);
    DefaultConnectionRules.forEach(rule => registry.registerConnectionRule(rule));
    schemaRegistryRef.current = registry;

    const engine = new ValidationEngine({
      schemaRegistry: registry,
      mode: state.validationMode,
      enableCaching: true,
      cacheTTL: 3000,
      enableETLValidation: true,
      etlMode: 'strict'
    });

    validationEngineRef.current = engine;

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

  // ==================== VALIDATION FUNCTIONS ====================

  const isValidConnection = useCallback((connection: Connection): boolean => {
    if (!connection.source || !connection.target) {
      return false;
    }

    if (connection.source === connection.target) {
      return false;
    }

    if (wouldCauseCycle(connection.source, connection.target, edges)) {
      return false;
    }

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

    const sourceNode = nodes.find(n => n.id === connection.source);
    const targetNode = nodes.find(n => n.id === connection.target);

    if (!sourceNode || !targetNode) return false;

    const sourceType = sourceNode.data?.type || sourceNode.type || 'unknown';
    const targetType = targetNode.data?.type || targetNode.type || 'unknown';

    const connectionCheck = schemaRegistryRef.current?.isConnectionAllowed(sourceType, targetType);
    if (connectionCheck && !connectionCheck.allowed) {
      return false;
    }

    const etlCheck = schemaRegistryRef.current?.isETLConnectionAllowed(sourceType, targetType);
    if (etlCheck && !etlCheck.allowed) {
      return false;
    }

    return true;
  }, [nodes, edges, wouldCauseCycle]);

  // ==================== NEW: TMap HANDLER FUNCTIONS WITH METADATA ====================
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

    const transformedData = {
      ...editorData,
      inputColumns: editorData.inputColumns.map(col => ({
        name: col.name,
        type: col.type || 'STRING'
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
        data: transformedData,
        nodeMetadata: nodeData
      },
    }));

    showValidationFeedback(
      `Opening Map Editor for ${nodeData.name || nodeId}`,
      'info',
      { x: 100, y: 100 }
    );
  }, [nodes, edges, showValidationFeedback]);

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

    const updatedConfig: ComponentConfiguration = { type: 'MAP', config };

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

          if (onNodeMetadataUpdate) {
            onNodeMetadataUpdate(nodeId, updatedMetadata);
          }

          return { ...node, data: updatedNodeData };
        }
        return node;
      });

      syncNodesAndEdges(updatedNodes, edges);

      return updatedNodes;
    });

    debouncedAutoSave();

    showValidationFeedback(
      `Saved ${config.transformations.length} transformations for ${mapEditorState.nodeMetadata.name}`,
      'success',
      { x: 100, y: 100 }
    );

    setState(prev => ({
      ...prev,
      mapEditorState: {
        isOpen: false,
        data: null,
        nodeMetadata: undefined
      }
    }));
  }, [state.mapEditorState, onNodeMetadataUpdate, showValidationFeedback, syncNodesAndEdges, edges, debouncedAutoSave]);

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

      const position = screenToFlowPosition({
        x: event.clientX,
        y: event.clientY,
      });

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

        const componentDef = COMPONENT_REGISTRY[data.componentId];
        if (!componentDef) {
          console.warn(`Component not found in registry: ${data.componentId}`);
          return;
        }

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

        const cleanBaseName = baseName
          .replace(/_(INPUT|OUTPUT|TRANSFORM)_/i, '_')
          .replace(/_+$/, '');

        console.log('🔤 Clean base name for naming:', cleanBaseName);

        const isInputCategoryComponent = componentDef.category === 'input';

        const label = nameGenerator.generate(
          cleanBaseName,
          isInputCategoryComponent ? 'TRANSFORM' : componentDef.defaultRole
        );

        const columns = extractColumnsFromDragData(data.metadata);

        const componentRole = isInputCategoryComponent ? 'TRANSFORM' : componentDef.defaultRole;

        const configuration = createInitialComponentConfiguration(
          componentDef.id,
          componentRole,
          data.metadata
        );

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

        const nodeData: CanvasNodeData = {
          id: `node-${Date.now()}-${cleanBaseName}`,
          name: label,
          type: mapComponentKeyToNodeType(componentDef.id, componentRole),
          nodeType: componentRole === 'INPUT' ? 'input' : componentRole === 'OUTPUT' ? 'output' : 'transform',
          componentCategory: componentDef.category,
          componentKey: componentDef.id,
          componentType: componentRole,
          position,
          size: { width: componentDef.defaultDimensions.width * 2, height: componentDef.defaultDimensions.height * 2 },
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
            icon: componentDef.icon,
          }
        };

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

        addNodes(newNode);

        const updatedNodes = [...nodes, newNode];
        setTimeout(() => {
          syncNodesAndEdges(updatedNodes, edges);
        }, 0);

        if (onNodeMetadataUpdate) {
          onNodeMetadataUpdate(newNode.id, nodeData.metadata!);
        }

        debouncedAutoSave();

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

          showValidationFeedback(
            `Please select role for ${cleanBaseName}`,
            'info',
            position
          );
        } else {
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
      const updatedNodes = applyNodeChanges(changes, nodes);
      setNodes(updatedNodes);

      syncNodesAndEdges(updatedNodes, edges);

      if (job || canvasId) {
        debouncedAutoSave();
      }

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

      syncNodesAndEdges(nodes, updatedEdges);

      if (job || canvasId) {
        debouncedAutoSave();
      }
    },
    [nodes, edges, syncNodesAndEdges, job, canvasId, debouncedAutoSave]
  );

  const onMove = useCallback((_event: MouseEvent | TouchEvent | null, viewport: Viewport) => {
    setState(prev => ({
      ...prev,
      viewport
    }));

    if (job || canvasId) {
      debouncedAutoSave();
    }
  }, [job, canvasId, debouncedAutoSave]);

  const onConnect = useCallback(
    (connection: Connection) => {
      if (!connection.source || !connection.target) {
        showValidationFeedback('Source and target are required', 'error');
        return;
      }

      if (typeof connection.source !== 'string' || typeof connection.target !== 'string') {
        showValidationFeedback('Invalid connection parameters', 'error');
        return;
      }

      const sourceNode = nodes.find(n => n.id === connection.source);
      const targetNode = nodes.find(n => n.id === connection.target);

      if (!sourceNode || !targetNode) {
        showValidationFeedback('Source or target node not found', 'error');
        return;
      }

      if (connection.source === connection.target) {
        showValidationFeedback('Cannot connect a node to itself', 'error');
        return;
      }

      if (wouldCauseCycle(connection.source, connection.target, edges)) {
        showValidationFeedback('Connection would create a cycle', 'error');
        return;
      }

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

      const newEdge = createEdgeWithMetadata(
        {
          ...connection,
          source: connection.source as string,
          target: connection.target as string
        },
        sourceNode as Node<CanvasNodeData>,
        targetNode as Node<CanvasNodeData>
      );

      const updatedEdges = addEdge(newEdge, edges);
      setEdges(updatedEdges);

      syncNodesAndEdges(nodes, updatedEdges);

      if (onEdgeMetadataUpdate) {
        onEdgeMetadataUpdate(newEdge.id, newEdge.data);
      }

      if (job || canvasId) {
        debouncedAutoSave();
      }

      showValidationFeedback(
        `Created ${newEdge.data.relationType} connection`,
        'success',
        { x: 100, y: 100 }
      );
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
  const centerNodes = useCallback(() => {
    if (nodes.length > 0 && reactFlowInstance && reactFlowWrapper.current) {
      const container = reactFlowWrapper.current;
      if (!container) return;

      const containerWidth = container.clientWidth;
      const containerHeight = container.clientHeight;

      const nodePositions = nodes.map(node => node.position);
      const nodeWidths = nodes.map(node =>
        node.style?.width ? parseFloat(node.style.width as string) : 100
      );
      const nodeHeights = nodes.map(node =>
        node.style?.height ? parseFloat(node.style.height as string) : 100
      );

      const minX = Math.min(...nodePositions.map((p, _i) => p.x));
      const maxX = Math.max(...nodePositions.map((p, i) => p.x + nodeWidths[i]));
      const minY = Math.min(...nodePositions.map((p, _i) => p.y));
      const maxY = Math.max(...nodePositions.map((p, i) => p.y + nodeHeights[i]));

      const nodesWidth = maxX - minX;
      const nodesHeight = maxY - minY;
      const centerX = minX + nodesWidth / 2;
      const centerY = minY + nodesHeight / 2;

      const viewportX = containerWidth / 2 - centerX;
      const viewportY = containerHeight / 2 - centerY;

      const newViewport = { x: viewportX, y: viewportY, zoom: 1 };

      reactFlowInstance.setViewport(newViewport);

      setState(prev => ({
        ...prev,
        viewport: newViewport
      }));

      if (job || canvasId) {
        debouncedAutoSave();
      }
    }
  }, [nodes, reactFlowInstance, reactFlowWrapper, job, canvasId, debouncedAutoSave]);

  useEffect(() => {
    if (nodes.length > 0 && reactFlowInstance && (!state.viewport || state.viewport.zoom !== 1)) {
      const timer = setTimeout(() => {
        centerNodes();
      }, 100);

      return () => clearTimeout(timer);
    }
  }, [nodes, reactFlowInstance, centerNodes, state.viewport]);

  // ==================== NEW: ROLE SELECTION HANDLERS WITH METADATA AND SYNCHRONIZATION ====================
  const handleRoleSelect = useCallback((selectedRole: 'INPUT' | 'OUTPUT') => {
    if (!pendingRoleSelection) return;

    const nodeToUpdate = nodes.find(n => n.id === pendingRoleSelection.nodeId);
    if (!nodeToUpdate) return;

    const newLabel = nameGenerator.generate(
      pendingRoleSelection.componentId,
      selectedRole
    );

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

    const newConfiguration = createInitialComponentConfiguration(
      pendingRoleSelection.componentId,
      selectedRole,
      pendingRoleSelection.nodeData.metadata
    );

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

    setNodes((nds) => {
      const updatedNodes = nds.map((node) => {
        if (node.id === pendingRoleSelection.nodeId) {
          const updatedData: CanvasNodeData = {
            ...pendingRoleSelection.nodeData,
            name: newLabel,
            nodeType: selectedRole === 'INPUT' ? 'input' : 'output',
            componentCategory: pendingRoleSelection.componentDef.category,
            componentType: selectedRole,
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

      syncNodesAndEdges(updatedNodes, edges);

      return updatedNodes;
    });

    debouncedAutoSave();

    setPendingRoleSelection(null);

    showValidationFeedback(
      `Role set to ${selectedRole} for ${pendingRoleSelection.displayName}`,
      'success',
      pendingRoleSelection.position
    );
  }, [pendingRoleSelection, showValidationFeedback, onNodeMetadataUpdate, nodes, edges, syncNodesAndEdges, debouncedAutoSave]);

  const handleRoleCancel = useCallback(() => {
    if (!pendingRoleSelection) return;

    setNodes((nds) => {
      const updatedNodes = nds.filter(node => node.id !== pendingRoleSelection.nodeId);

      syncNodesAndEdges(updatedNodes, edges);

      return updatedNodes;
    });

    nameGenerator.decrementCounter(pendingRoleSelection.componentId, 'TRANSFORM');

    debouncedAutoSave();

    setPendingRoleSelection(null);

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

      syncNodesAndEdges(updatedNodes, edges);

      return updatedNodes;
    });

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
  const handleCanvasNodeDoubleClick = useCallback((event: CustomEvent) => {
    const { componentMetadata, nodeMetadata } = event.detail;
    const metadata = nodeMetadata || componentMetadata;

    if (!metadata || !metadata.id) return;

    const node = nodes.find(n => n.id === metadata.id) as Node<CanvasNodeData> | undefined;
    if (!node) return;

    const nodeData = node.data;
    const nodeType = nodeData.type;

    if (nodeType === NodeType.MAP) {
      handleTMapDoubleClick(metadata.id);
      return;
    }

    const inputNodes = getConnectedNodes(metadata.id, edges, nodes, 'input');
    const inputColumns = inputNodes.flatMap(n => extractColumnsFromNode(n));
    const uniqueInputColumns = inputColumns.filter((col, idx, self) =>
      self.findIndex(c => c.name === col.name) === idx
    );

    const simpleInputColumns: SimpleColumn[] = uniqueInputColumns.map(({ name, type, id }) => ({ name, type, id }));

    const getInitialConfig = <T,>(type: string): T | undefined => {
      if (type === 'FILTER') return getComponentConfig(nodeData, 'FILTER') as T;
      if (type === 'SORT') return getComponentConfig(nodeData, 'SORT') as T;
      if (type === 'AGGREGATE') return getComponentConfig(nodeData, 'AGGREGATE') as T;
      if (type === 'CONVERT') return getComponentConfig(nodeData, 'CONVERT') as T;
      if (type === 'REPLACE') return getComponentConfig(nodeData, 'REPLACE') as T;
      if (type === 'EXTRACT_JSON_FIELDS') return getComponentConfig(nodeData, 'EXTRACT_JSON_FIELDS') as T;
      if (type === 'EXTRACT_DELIMITED') return getComponentConfig(nodeData, 'EXTRACT_DELIMITED') as T;
      if (type === 'EXTRACT_XML_FIELD') return getComponentConfig(nodeData, 'EXTRACT_XML_FIELD') as T;
      if (type === 'REPLACE_LIST') return getComponentConfig(nodeData, 'REPLACE_LIST') as T;
      if (type === 'NORMALIZE_NUMBER') return getComponentConfig(nodeData, 'NORMALIZE_NUMBER') as T;
      if (type === 'NORMALIZE') return getComponentConfig(nodeData, 'NORMALIZE') as T;
      if (type === 'REPLICATE') return getComponentConfig(nodeData, 'REPLICATE') as T;
      if (type === 'MATCH_GROUP') return getComponentConfig(nodeData, 'MATCH_GROUP') as T;
      if (type === 'FILTER_COLUMNS') return getComponentConfig(nodeData, 'FILTER_COLUMNS') as T;
      if (type === 'FILE_LOOKUP') return getComponentConfig(nodeData, 'FILE_LOOKUP') as T;
      // NEW
      if (type === 'UNPIVOT_ROW') return getComponentConfig(nodeData, 'UNPIVOT_ROW') as T;
      if (type === 'UNITE') return getComponentConfig(nodeData, 'UNITE') as T;
      if (type === 'UNIQ_ROW') return getComponentConfig(nodeData, 'UNIQ_ROW') as T;
      if (type === 'SPLIT_ROW') return getComponentConfig(nodeData, 'NORMALIZE') as T; // SplitRow uses NORMALIZE config
      if (type === 'PIVOT_TO_COLUMNS_DELIMITED') return getComponentConfig(nodeData, 'PIVOT_TO_COLUMNS_DELIMITED') as T;
      if (type === 'DENORMALIZE_SORTED_ROW') return getComponentConfig(nodeData, 'DENORMALIZE_SORTED_ROW') as T;
      if (type === 'DENORMALIZE') return getComponentConfig(nodeData, 'DENORMALIZE') as T;
      return undefined;
    };

    if (nodeType === NodeType.JOIN) {
      if (inputNodes.length < 2) {
        showValidationFeedback('Join requires at least two input connections.', 'error');
        return;
      }
      const leftNode = inputNodes[0];
      const rightNode = inputNodes[1];

      const leftSchema: SchemaDefinition = {
        id: leftNode.id,
        name: leftNode.data.name || leftNode.id,
        fields: extractColumnsFromNode(leftNode).map((col, idx) => ({
          id: col.id || `${leftNode.id}_${col.name}_${idx}`,
          name: col.name,
          type: (col.type as DataType) || 'STRING',
          nullable: true,
          isKey: false,
        })),
        isTemporary: false,
        isMaterialized: false,
      };

      const rightSchema: SchemaDefinition = {
        id: rightNode.id,
        name: rightNode.data.name || rightNode.id,
        fields: extractColumnsFromNode(rightNode).map((col, idx) => ({
          id: col.id || `${rightNode.id}_${col.name}_${idx}`,
          name: col.name,
          type: (col.type as DataType) || 'STRING',
          nullable: true,
          isKey: false,
        })),
        isTemporary: false,
        isMaterialized: false,
      };

      const joinConfig = getComponentConfig(nodeData, 'JOIN');
      setActiveEditor({
        type: 'join',
        nodeId: node.id,
        nodeMetadata: nodeData,
        leftSchema,
        rightSchema,
        initialConfig: joinConfig,
      });
      return;
    }

    switch (nodeType) {
      case NodeType.SORT_ROW:
        setActiveEditor({
          type: 'sort',
          nodeId: node.id,
          nodeMetadata: nodeData,
          inputColumns: simpleInputColumns,
          initialConfig: getInitialConfig<SortComponentConfiguration>('SORT'),
        });
        break;

      case NodeType.REPLACE:
        setActiveEditor({
          type: 'replace',
          nodeId: node.id,
          nodeMetadata: nodeData,
          inputColumns: simpleInputColumns,
          outputColumns: simpleInputColumns,
          initialConfig: getInitialConfig<ReplaceComponentConfiguration>('REPLACE'),
        });
        break;

      case NodeType.FILTER_ROW:
        setActiveEditor({
          type: 'filter',
          nodeId: node.id,
          nodeMetadata: nodeData,
          inputColumns: simpleInputColumns,
          initialConfig: getInitialConfig<FilterComponentConfiguration>('FILTER'),
        });
        break;

      case NodeType.AGGREGATE_ROW:
        setActiveEditor({
          type: 'aggregate',
          nodeId: node.id,
          nodeMetadata: nodeData,
          inputColumns: simpleInputColumns,
          initialConfig: getInitialConfig<AggregateComponentConfiguration>('AGGREGATE'),
        });
        break;

      case NodeType.CONVERT_TYPE:
        setActiveEditor({
          type: 'convert',
          nodeId: node.id,
          nodeMetadata: nodeData,
          inputColumns: simpleInputColumns,
          outputColumns: simpleInputColumns,
          initialConfig: getInitialConfig<ConvertComponentConfiguration>('CONVERT'),
        });
        break;

      case NodeType.EXTRACT_DELIMITED_FIELDS:
        setActiveEditor({
          type: 'extractDelimited',
          nodeId: node.id,
          nodeMetadata: nodeData,
          inputColumns: simpleInputColumns,
          initialConfig: getInitialConfig<ExtractDelimitedFieldsConfiguration>('EXTRACT_DELIMITED'),
        });
        break;

      case NodeType.EXTRACT_JSON_FIELDS:
        setActiveEditor({
          type: 'extractJSON',
          nodeId: node.id,
          nodeMetadata: nodeData,
          inputColumns: simpleInputColumns,
          initialConfig: getInitialConfig<ExtractJSONFieldsConfiguration>('EXTRACT_JSON_FIELDS'),
        });
        break;

      case NodeType.EXTRACT_XML_FIELD:
        setActiveEditor({
          type: 'extractXML',
          nodeId: node.id,
          nodeMetadata: nodeData,
          inputColumns: simpleInputColumns,
          initialConfig: getInitialConfig<ExtractXMLFieldConfiguration>('EXTRACT_XML_FIELD'),
        });
        break;

      case NodeType.NORMALIZE:
        setActiveEditor({
          type: 'normalize',
          nodeId: node.id,
          nodeMetadata: nodeData,
          inputColumns: simpleInputColumns,
          initialConfig: getInitialConfig<NormalizeComponentConfiguration>('NORMALIZE'),
        });
        break;

      case NodeType.NORMALIZE_NUMBER:
        setActiveEditor({
          type: 'normalizeNumber',
          nodeId: node.id,
          nodeMetadata: nodeData,
          inputColumns: simpleInputColumns,
          initialConfig: getInitialConfig<NormalizeNumberComponentConfiguration>('NORMALIZE_NUMBER'),
        });
        break;

      case NodeType.REPLACE_LIST:
        const inputSchema: SchemaDefinition = {
          id: `input-${node.id}`,
          name: `Input Schema for ${nodeData.name}`,
          fields: uniqueInputColumns.map((col, idx) => ({
            id: col.id || `${node.id}_${col.name}_${idx}`,
            name: col.name,
            type: (col.type as DataType) || 'STRING',
            nullable: true,
            isKey: false,
          })),
          isTemporary: false,
          isMaterialized: false,
        };
        setActiveEditor({
          type: 'replaceList',
          nodeId: node.id,
          nodeMetadata: nodeData,
          inputSchema,
          initialConfig: getInitialConfig<ReplaceComponentConfiguration>('REPLACE_LIST'),
        });
        break;

      case NodeType.REPLICATE:
        setActiveEditor({
          type: 'replicate',
          nodeId: node.id,
          nodeMetadata: nodeData,
          inputColumns: simpleInputColumns,
          initialConfig: getInitialConfig<ReplicateComponentConfiguration>('REPLICATE'),
        });
        break;

      case NodeType.RECORD_MATCHING:
        const inputFields: FieldSchema[] = uniqueInputColumns.map((col, idx) => ({
          id: col.id || `${node.id}_${col.name}_${idx}`,
          name: col.name,
          type: (col.type as DataType) || 'STRING',
          nullable: true,
          isKey: false,
        }));
        setActiveEditor({
          type: 'recordMatching',
          nodeId: node.id,
          nodeMetadata: nodeData,
          inputFields,
          initialConfig: getInitialConfig<MatchGroupComponentConfiguration>('MATCH_GROUP'),
        });
        break;

      case NodeType.MATCH_GROUP:
        setActiveEditor({
          type: 'matchGroup',
          nodeId: node.id,
          nodeMetadata: nodeData,
          inputColumns: simpleInputColumns,
          initialConfig: getInitialConfig<MatchGroupComponentConfiguration>('MATCH_GROUP'),
        });
        break;

      case NodeType.FILTER_COLUMNS:
        const filterInputSchema: SchemaDefinition = {
          id: `input-${node.id}`,
          name: `Input Schema for ${nodeData.name}`,
          fields: uniqueInputColumns.map((col, idx) => ({
            id: col.id || `${node.id}_${col.name}_${idx}`,
            name: col.name,
            type: (col.type as DataType) || 'STRING',
            nullable: true,
            isKey: false,
          })),
          isTemporary: false,
          isMaterialized: false,
        };
        setActiveEditor({
          type: 'filterColumns',
          nodeId: node.id,
          nodeMetadata: nodeData,
          inputSchema: filterInputSchema,
          initialConfig: getInitialConfig<FilterColumnsComponentConfiguration>('FILTER_COLUMNS'),
        });
        break;

      case NodeType.FILE_LOOKUP:
        setActiveEditor({
          type: 'fileLookup',
          nodeId: node.id,
          nodeMetadata: nodeData,
          inputColumns: simpleInputColumns,
          initialConfig: getInitialConfig<FileLookupComponentConfiguration>('FILE_LOOKUP'),
        });
        break;

      // ==================== NEW CASES ====================
      case NodeType.UNPIVOT_ROW:
        setActiveEditor({
          type: 'unpivotRow',
          nodeId: node.id,
          nodeMetadata: nodeData,
          inputColumns: simpleInputColumns,
          initialConfig: getInitialConfig<UnpivotRowComponentConfiguration>('UNPIVOT_ROW'),
        });
        break;

      case NodeType.UNITE: {
        const inputSchemas: InputSchema[] = inputNodes.map(n => {
          const schema = n.data.metadata?.schemas?.output;
          return {
            id: n.id,
            name: n.data.name || n.id,
            fields: schema?.fields.map(f => ({ name: f.name, type: f.type, nullable: f.nullable })) || [],
          };
        });
        setActiveEditor({
          type: 'unite',
          nodeId: node.id,
          nodeMetadata: nodeData,
          inputSchemas,
          initialConfig: getInitialConfig<UniteComponentConfiguration>('UNITE'),
        });
        break;
      }

      case NodeType.UNIQ_ROW:
        setActiveEditor({
          type: 'uniqRow',
          nodeId: node.id,
          nodeMetadata: nodeData,
          inputColumns: simpleInputColumns,
          initialConfig: getInitialConfig<UniqRowComponentConfiguration>('UNIQ_ROW'),
        });
        break;

      case NodeType.SPLIT_ROW:
        setActiveEditor({
          type: 'splitRow',
          nodeId: node.id,
          nodeMetadata: nodeData,
          inputColumns: simpleInputColumns,
          initialConfig: getInitialConfig<NormalizeComponentConfiguration>('SPLIT_ROW'),
        });
        break;

      case NodeType.PIVOT_TO_COLUMNS_DELIMITED:
        setActiveEditor({
          type: 'pivotToColumnsDelimited',
          nodeId: node.id,
          nodeMetadata: nodeData,
          inputColumns: simpleInputColumns,
          initialConfig: getInitialConfig<PivotToColumnsDelimitedConfiguration>('PIVOT_TO_COLUMNS_DELIMITED'),
        });
        break;

      case NodeType.DENORMALIZE_SORTED_ROW:
        setActiveEditor({
          type: 'denormalizeSortedRow',
          nodeId: node.id,
          nodeMetadata: nodeData,
          inputColumns: simpleInputColumns,
          initialConfig: getInitialConfig<DenormalizeSortedRowComponentConfiguration>('DENORMALIZE_SORTED_ROW'),
        });
        break;

      case NodeType.DENORMALIZE:
        setActiveEditor({
          type: 'denormalize',
          nodeId: node.id,
          nodeMetadata: nodeData,
          inputColumns: simpleInputColumns,
          initialConfig: getInitialConfig<DenormalizeComponentConfiguration>('DENORMALIZE'),
        });
        break;

      default:
        showValidationFeedback(`No editor available for node type ${nodeType}`, 'info');
    }
  }, [nodes, edges, handleTMapDoubleClick, showValidationFeedback]);

  useEffect(() => {
    window.addEventListener('canvas-node-double-click', handleCanvasNodeDoubleClick as EventListener);

    const handleTMapDoubleClickEvent = (event: Event) => {
      const customEvent = event as CustomEvent;
      console.log('Received tMap double-click event:', customEvent.detail);
      handleTMapDoubleClick(customEvent.detail.nodeId);
    };

    window.addEventListener('canvas-tmap-double-click', handleTMapDoubleClickEvent);

    const handleEdgeDoubleClick = (event: CustomEvent) => {
      const { edgeId } = event.detail;
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

  // ==================== SQL GENERATION ON RUN ====================
  useEffect(() => {
    const handleToolbarRun = async (event: Event) => {
      const customEvent = event as CustomEvent;
      const { jobName, nodes: runNodes, edges: runEdges } = customEvent.detail;

      console.log(`🎬 Canvas received run request for job: ${jobName}`);
      dispatch(addLog({ level: 'INFO', message: `Starting run for job: ${jobName}`, source: 'Canvas' }));

      try {
        const canvasNodes: PipelineCanvasNode[] = runNodes.map((node: Node<CanvasNodeData>) => {
          const unified = node.data;
          const config = unified.metadata?.configuration;
          const nodeType = unified.type;

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
              const fields = config.config.schema?.fields || [];
              const sourceTableName = config.config.sourceDetails.tableName ||
                                      unified.metadata?.postgresTableName ||
                                      unified.metadata?.fullRepositoryMetadata?.postgresTableName ||
                                      unified.name;
              tableMapping = {
                schema: 'public',
                name: sourceTableName,
                columns: fieldsToPostgresColumns(fields)
              };
            } else if (isOutputConfig(config)) {
              schemaMappings = config.config.schemaMapping;
            }
          }

          let targetTableName: string | undefined;
          if (config && isOutputConfig(config)) {
            targetTableName = config.config.targetDetails.tableName ||
                              unified.metadata?.postgresTableName ||
                              unified.metadata?.fullRepositoryMetadata?.postgresTableName ||
                              unified.name;
          }

          let sourceTableName: string | undefined;
          if (config && isInputConfig(config)) {
            sourceTableName = tableMapping?.name || unified.name;
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
              description: unified.metadata?.description,
              targetTableName,
              sourceTableName
            }
          };
        });

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

        const generationResult: PipelineGenerationResult = await generatePipelineSQL(
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

        console.log('📝 [Canvas Run] Full SQL:', generationResult.sql);

        if (generationResult.errors.length > 0) {
          generationResult.errors.forEach(error => {
            dispatch(addLog({
              level: 'ERROR',
              message: `❌ [${error.code}] ${error.message}`,
              source: 'SQL Generation'
            }));
          });
          window.dispatchEvent(new CustomEvent('run-complete', {
            detail: {
              success: false,
              errors: generationResult.errors,
              sql: generationResult.sql
            }
          }));
          return;
        }

        const connectionId = await getActivePostgresConnectionId();
        if (!connectionId) {
          const errorMsg = 'No active PostgreSQL connection. Please connect to a database first.';
          dispatch(addLog({ level: 'ERROR', message: errorMsg, source: 'Run' }));
          window.dispatchEvent(new CustomEvent('run-complete', {
            detail: { success: false, error: errorMsg }
          }));
          return;
        }

        dispatch(addLog({ level: 'INFO', message: 'Executing generated SQL...', source: 'Run' }));
        const executionResult: ClientQueryExecutionResult = await databaseApi.executeQuery(
          connectionId,
          generationResult.sql,
          { maxRows: 1000 }
        );

        if (executionResult.success) {
          const rowCount = executionResult.result?.rowCount ?? 0;
          const fieldCount = executionResult.result?.fields?.length ?? 0;
          dispatch(addLog({
            level: 'SUCCESS',
            message: `✅ SQL executed successfully. Rows affected: ${rowCount}, Fields: ${fieldCount}`,
            source: 'Run'
          }));
          if (executionResult.result?.rows && executionResult.result.rows.length > 0) {
            dispatch(addLog({
              level: 'DEBUG',
              message: `First row: ${JSON.stringify(executionResult.result.rows[0])}`,
              source: 'Run'
            }));
          }
        } else {
          dispatch(addLog({
            level: 'ERROR',
            message: `❌ Execution failed: ${executionResult.error}`,
            source: 'Run'
          }));
        }

        window.dispatchEvent(new CustomEvent('run-complete', {
          detail: {
            success: executionResult.success,
            sql: generationResult.sql,
            executionResult,
            errors: generationResult.errors
          }
        }));

      } catch (error: any) {
        console.error('Error during run workflow:', error);
        dispatch(addLog({
          level: 'ERROR',
          message: `❌ Run failed: ${error.message || 'Unknown error'}`,
          source: 'Run'
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
  }, [dispatch]);

  // ==================== EDITOR SAVE HANDLERS ====================
  const updateNodeConfiguration = useCallback((nodeId: string, configUnion: ComponentConfiguration) => {
    setNodes(prev => {
      const updatedNodes = prev.map(node => {
        if (node.id === nodeId) {
          const updatedMetadata: UnifiedNodeMetadata = {
            ...node.data.metadata,
            configuration: configUnion,
            compilerMetadata: {
              ...(node.data.metadata?.compilerMetadata || {}),
              lastModified: new Date().toISOString(),
            },
          };
          const updatedNodeData: CanvasNodeData = {
            ...node.data,
            metadata: updatedMetadata,
          };
          if (onNodeMetadataUpdate) {
            onNodeMetadataUpdate(nodeId, updatedMetadata);
          }
          return { ...node, data: updatedNodeData };
        }
        return node;
      });
      syncNodesAndEdges(updatedNodes, edges);
      return updatedNodes;
    });
    debouncedAutoSave();
    showValidationFeedback('Configuration saved', 'success', { x: 100, y: 100 });
  }, [onNodeMetadataUpdate, syncNodesAndEdges, edges, debouncedAutoSave, showValidationFeedback]);

  const handleSortEditorSave = useCallback((config: SortComponentConfiguration) => {
    if (!activeEditor || activeEditor.type !== 'sort') return;
    const { nodeId } = activeEditor;
    updateNodeConfiguration(nodeId, { type: 'SORT', config });
    setActiveEditor(null);
  }, [activeEditor, updateNodeConfiguration]);

  const handleReplaceEditorSave = useCallback((config: ReplaceComponentConfiguration) => {
    if (!activeEditor || activeEditor.type !== 'replace') return;
    const { nodeId } = activeEditor;
    updateNodeConfiguration(nodeId, { type: 'REPLACE', config });
    setActiveEditor(null);
  }, [activeEditor, updateNodeConfiguration]);

  const handleJoinEditorSave = useCallback((config: JoinComponentConfiguration) => {
    if (!activeEditor || activeEditor.type !== 'join') return;
    const { nodeId } = activeEditor;
    updateNodeConfiguration(nodeId, { type: 'JOIN', config });
    setActiveEditor(null);
  }, [activeEditor, updateNodeConfiguration]);

  const handleFilterEditorSave = useCallback((config: FilterComponentConfiguration) => {
    if (!activeEditor || activeEditor.type !== 'filter') return;
    const { nodeId } = activeEditor;
    updateNodeConfiguration(nodeId, { type: 'FILTER', config });
    setActiveEditor(null);
  }, [activeEditor, updateNodeConfiguration]);

  const handleExtractXMLSave = useCallback((config: ExtractXMLFieldConfiguration) => {
    if (!activeEditor || activeEditor.type !== 'extractXML') return;
    const { nodeId } = activeEditor;
    updateNodeConfiguration(nodeId, { type: 'EXTRACT_XML_FIELD', config });
    setActiveEditor(null);
  }, [activeEditor, updateNodeConfiguration]);

  const handleExtractJSONSave = useCallback((config: ExtractJSONFieldsConfiguration) => {
    if (!activeEditor || activeEditor.type !== 'extractJSON') return;
    const { nodeId } = activeEditor;
    updateNodeConfiguration(nodeId, { type: 'EXTRACT_JSON_FIELDS', config });
    setActiveEditor(null);
  }, [activeEditor, updateNodeConfiguration]);

  const handleExtractDelimitedSave = useCallback((config: ExtractDelimitedFieldsConfiguration) => {
    if (!activeEditor || activeEditor.type !== 'extractDelimited') return;
    const { nodeId } = activeEditor;
    updateNodeConfiguration(nodeId, { type: 'EXTRACT_DELIMITED', config });
    setActiveEditor(null);
  }, [activeEditor, updateNodeConfiguration]);

  const handleConvertEditorSave = useCallback((config: ConvertComponentConfiguration) => {
    if (!activeEditor || activeEditor.type !== 'convert') return;
    const { nodeId } = activeEditor;
    updateNodeConfiguration(nodeId, { type: 'CONVERT', config });
    setActiveEditor(null);
  }, [activeEditor, updateNodeConfiguration]);

  const handleAggregateEditorSave = useCallback((config: AggregateComponentConfiguration) => {
    if (!activeEditor || activeEditor.type !== 'aggregate') return;
    const { nodeId } = activeEditor;
    updateNodeConfiguration(nodeId, { type: 'AGGREGATE', config });
    setActiveEditor(null);
  }, [activeEditor, updateNodeConfiguration]);

  const handleReplaceListEditorSave = useCallback((config: ReplaceComponentConfiguration) => {
    if (!activeEditor || activeEditor.type !== 'replaceList') return;
    updateNodeConfiguration(activeEditor.nodeId, { type: 'REPLACE_LIST', config });
    setActiveEditor(null);
  }, [activeEditor, updateNodeConfiguration]);

  const handleNormalizeNumberEditorSave = useCallback((config: NormalizeNumberComponentConfiguration) => {
    if (!activeEditor || activeEditor.type !== 'normalizeNumber') return;
    updateNodeConfiguration(activeEditor.nodeId, { type: 'NORMALIZE_NUMBER', config });
    setActiveEditor(null);
  }, [activeEditor, updateNodeConfiguration]);

  const handleNormalizeEditorSave = useCallback((config: NormalizeComponentConfiguration) => {
    if (!activeEditor || activeEditor.type !== 'normalize') return;
    updateNodeConfiguration(activeEditor.nodeId, { type: 'NORMALIZE', config });
    setActiveEditor(null);
  }, [activeEditor, updateNodeConfiguration]);

  const handleReplicateEditorSave = useCallback((config: ReplicateComponentConfiguration) => {
    if (!activeEditor || activeEditor.type !== 'replicate') return;
    updateNodeConfiguration(activeEditor.nodeId, { type: 'REPLICATE', config });
    setActiveEditor(null);
  }, [activeEditor, updateNodeConfiguration]);

  const handleRecordMatchingEditorSave = useCallback((config: MatchGroupComponentConfiguration) => {
    if (!activeEditor || activeEditor.type !== 'recordMatching') return;
    updateNodeConfiguration(activeEditor.nodeId, { type: 'MATCH_GROUP', config });
    setActiveEditor(null);
  }, [activeEditor, updateNodeConfiguration]);

  const handleMatchGroupEditorSave = useCallback((config: MatchGroupComponentConfiguration) => {
    if (!activeEditor || activeEditor.type !== 'matchGroup') return;
    updateNodeConfiguration(activeEditor.nodeId, { type: 'MATCH_GROUP', config });
    setActiveEditor(null);
  }, [activeEditor, updateNodeConfiguration]);

  const handleFilterColumnsEditorSave = useCallback((config: FilterColumnsComponentConfiguration) => {
    if (!activeEditor || activeEditor.type !== 'filterColumns') return;
    updateNodeConfiguration(activeEditor.nodeId, { type: 'FILTER_COLUMNS', config });
    setActiveEditor(null);
  }, [activeEditor, updateNodeConfiguration]);

  const handleFileLookupEditorSave = useCallback((config: FileLookupComponentConfiguration) => {
    if (!activeEditor || activeEditor.type !== 'fileLookup') return;
    updateNodeConfiguration(activeEditor.nodeId, { type: 'FILE_LOOKUP', config });
    setActiveEditor(null);
  }, [activeEditor, updateNodeConfiguration]);

  // ==================== NEW SAVE HANDLERS ====================
  const handleUnpivotRowEditorSave = useCallback((config: UnpivotRowComponentConfiguration) => {
    if (!activeEditor || activeEditor.type !== 'unpivotRow') return;
    updateNodeConfiguration(activeEditor.nodeId, { type: 'UNPIVOT_ROW', config });
    setActiveEditor(null);
  }, [activeEditor, updateNodeConfiguration]);

  const handleUniteEditorSave = useCallback((config: UniteComponentConfiguration) => {
    if (!activeEditor || activeEditor.type !== 'unite') return;
    updateNodeConfiguration(activeEditor.nodeId, { type: 'UNITE', config });
    setActiveEditor(null);
  }, [activeEditor, updateNodeConfiguration]);

  const handleUniqRowEditorSave = useCallback((config: UniqRowComponentConfiguration) => {
    if (!activeEditor || activeEditor.type !== 'uniqRow') return;
    updateNodeConfiguration(activeEditor.nodeId, { type: 'UNIQ_ROW', config });
    setActiveEditor(null);
  }, [activeEditor, updateNodeConfiguration]);

  const handleSplitRowEditorSave = useCallback((config: NormalizeComponentConfiguration) => {
    if (!activeEditor || activeEditor.type !== 'splitRow') return;
    updateNodeConfiguration(activeEditor.nodeId, { type: 'NORMALIZE', config }); // SplitRow uses NORMALIZE config
    setActiveEditor(null);
  }, [activeEditor, updateNodeConfiguration]);

  const handlePivotToColumnsDelimitedEditorSave = useCallback((config: PivotToColumnsDelimitedConfiguration) => {
    if (!activeEditor || activeEditor.type !== 'pivotToColumnsDelimited') return;
    updateNodeConfiguration(activeEditor.nodeId, { type: 'PIVOT_TO_COLUMNS_DELIMITED', config });
    setActiveEditor(null);
  }, [activeEditor, updateNodeConfiguration]);

  const handleDenormalizeSortedRowEditorSave = useCallback((config: DenormalizeSortedRowComponentConfiguration) => {
    if (!activeEditor || activeEditor.type !== 'denormalizeSortedRow') return;
    updateNodeConfiguration(activeEditor.nodeId, { type: 'DENORMALIZE_SORTED_ROW', config });
    setActiveEditor(null);
  }, [activeEditor, updateNodeConfiguration]);

  const handleDenormalizeEditorSave = useCallback((config: DenormalizeComponentConfiguration) => {
    if (!activeEditor || activeEditor.type !== 'denormalize') return;
    updateNodeConfiguration(activeEditor.nodeId, { type: 'DENORMALIZE', config });
    setActiveEditor(null);
  }, [activeEditor, updateNodeConfiguration]);

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

  // ==================== RENDER tMap EDITOR MODAL ====================
  const renderTMapEditorModal = () => {
    if (!state.mapEditorState.isOpen || !state.mapEditorState.data) return null;

    const { nodeId, inputColumns, outputColumns } = state.mapEditorState.data;
    const nodeMetadata = state.mapEditorState.nodeMetadata;
    const nodeLabel = nodeMetadata?.name || nodeId;

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

  // ==================== RENDER ACTIVE EDITOR MODALS ====================
  const renderActiveEditor = () => {
    if (!activeEditor) return null;

    switch (activeEditor.type) {
      case 'sort':
        return (
          <SortEditor
            nodeId={activeEditor.nodeId}
            nodeMetadata={activeEditor.nodeMetadata}
            inputColumns={activeEditor.inputColumns}
            initialConfig={activeEditor.initialConfig}
            onClose={() => setActiveEditor(null)}
            onSave={handleSortEditorSave}
          />
        );
      case 'replace':
        return (
          <ReplaceEditor
            nodeId={activeEditor.nodeId}
            nodeMetadata={activeEditor.nodeMetadata}
            inputColumns={activeEditor.inputColumns}
            outputColumns={activeEditor.outputColumns}
            initialConfig={activeEditor.initialConfig}
            onClose={() => setActiveEditor(null)}
            onSave={handleReplaceEditorSave}
          />
        );
      case 'join':
        return (
          <JoinEditor
            nodeId={activeEditor.nodeId}
            nodeName={activeEditor.nodeMetadata.name}
            leftSchema={activeEditor.leftSchema}
            rightSchema={activeEditor.rightSchema}
            initialConfig={activeEditor.initialConfig}
            onClose={() => setActiveEditor(null)}
            onSave={handleJoinEditorSave}
          />
        );
      case 'filter':
        return (
          <FilterRowConfigModal
            isOpen={true}
            onClose={() => setActiveEditor(null)}
            nodeId={activeEditor.nodeId}
            nodeName={activeEditor.nodeMetadata.name}
            inputColumns={activeEditor.inputColumns}
            initialConfig={activeEditor.initialConfig}
            onSave={handleFilterEditorSave}
          />
        );
      case 'extractXML':
        return (
          <ExtractXMLFieldEditor
            nodeId={activeEditor.nodeId}
            nodeName={activeEditor.nodeMetadata.name}
            inputColumns={activeEditor.inputColumns}
            initialConfig={activeEditor.initialConfig}
            onClose={() => setActiveEditor(null)}
            onSave={handleExtractXMLSave}
          />
        );
      case 'extractJSON':
        return (
          <ExtractJSONFieldsEditor
            nodeId={activeEditor.nodeId}
            nodeMetadata={activeEditor.nodeMetadata}
            inputColumns={activeEditor.inputColumns}
            initialConfig={activeEditor.initialConfig}
            onClose={() => setActiveEditor(null)}
            onSave={handleExtractJSONSave}
          />
        );
      case 'extractDelimited':
        return (
          <ExtractDelimitedFieldsConfigModal
            isOpen={true}
            onClose={() => setActiveEditor(null)}
            nodeId={activeEditor.nodeId}
            nodeName={activeEditor.nodeMetadata.name}
            inputColumns={activeEditor.inputColumns}
            initialConfig={activeEditor.initialConfig}
            onSave={handleExtractDelimitedSave}
          />
        );
      case 'convert':
        return (
          <ConvertTypeEditor
            nodeId={activeEditor.nodeId}
            nodeMetadata={activeEditor.nodeMetadata}
            inputColumns={activeEditor.inputColumns}
            outputColumns={activeEditor.outputColumns}
            initialConfig={activeEditor.initialConfig}
            onClose={() => setActiveEditor(null)}
            onSave={handleConvertEditorSave}
          />
        );
      case 'aggregate':
        return (
          <AggregateEditor
            nodeId={activeEditor.nodeId}
            nodeMetadata={activeEditor.nodeMetadata}
            inputColumns={activeEditor.inputColumns}
            initialConfig={activeEditor.initialConfig}
            onClose={() => setActiveEditor(null)}
            onSave={handleAggregateEditorSave}
          />
        );
      case 'replaceList':
        return (
          <ReplaceListEditor
            nodeId={activeEditor.nodeId}
            initialConfig={activeEditor.initialConfig}
            inputSchema={activeEditor.inputSchema}
            onSave={handleReplaceListEditorSave}
            onClose={() => setActiveEditor(null)}
          />
        );
      case 'normalizeNumber':
        return (
          <NormalizeNumberEditor
            nodeId={activeEditor.nodeId}
            nodeMetadata={activeEditor.nodeMetadata}
            inputColumns={activeEditor.inputColumns}
            initialConfig={activeEditor.initialConfig}
            onClose={() => setActiveEditor(null)}
            onSave={handleNormalizeNumberEditorSave}
          />
        );
      case 'normalize':
        return (
          <NormalizeEditor
            nodeId={activeEditor.nodeId}
            nodeMetadata={activeEditor.nodeMetadata}
            inputColumns={activeEditor.inputColumns}
            initialConfig={activeEditor.initialConfig}
            onClose={() => setActiveEditor(null)}
            onSave={handleNormalizeEditorSave}
          />
        );
      case 'replicate':
        return (
          <ReplicateEditor
            nodeId={activeEditor.nodeId}
            nodeMetadata={activeEditor.nodeMetadata}
            inputColumns={activeEditor.inputColumns}
            initialConfig={activeEditor.initialConfig}
            onClose={() => setActiveEditor(null)}
            onSave={handleReplicateEditorSave}
          />
        );
      case 'recordMatching':
        return (
          <RecordMatchingEditor
            nodeId={activeEditor.nodeId}
            nodeMetadata={activeEditor.nodeMetadata}
            inputFields={activeEditor.inputFields}
            initialConfig={activeEditor.initialConfig}
            onClose={() => setActiveEditor(null)}
            onSave={handleRecordMatchingEditorSave}
          />
        );
      case 'matchGroup':
        return (
          <MatchGroupEditor
            nodeId={activeEditor.nodeId}
            nodeMetadata={activeEditor.nodeMetadata}
            inputColumns={activeEditor.inputColumns}
            initialConfig={activeEditor.initialConfig}
            onClose={() => setActiveEditor(null)}
            onSave={handleMatchGroupEditorSave}
          />
        );
      case 'filterColumns':
        return (
          <FilterColumnsEditor
            isOpen={true}
            onClose={() => setActiveEditor(null)}
            onSave={handleFilterColumnsEditorSave}
            nodeId={activeEditor.nodeId}
            initialConfig={activeEditor.initialConfig}
            inputSchema={activeEditor.inputSchema}
          />
        );
      case 'fileLookup':
        return (
          <FileLookupEditor
            nodeId={activeEditor.nodeId}
            nodeMetadata={activeEditor.nodeMetadata}
            inputColumns={activeEditor.inputColumns}
            initialConfig={activeEditor.initialConfig}
            onClose={() => setActiveEditor(null)}
            onSave={handleFileLookupEditorSave}
          />
        );

      // ==================== NEW CASES ====================
      case 'unpivotRow':
        return (
          <UnpivotRowEditor
            nodeId={activeEditor.nodeId}
            nodeMetadata={activeEditor.nodeMetadata}
            inputColumns={activeEditor.inputColumns}
            initialConfig={activeEditor.initialConfig}
            onClose={() => setActiveEditor(null)}
            onSave={handleUnpivotRowEditorSave}
          />
        );

      case 'unite':
        return (
          <UniteEditor
            nodeId={activeEditor.nodeId}
            nodeMetadata={activeEditor.nodeMetadata}
            inputSchemas={activeEditor.inputSchemas}
            initialConfig={activeEditor.initialConfig}
            onClose={() => setActiveEditor(null)}
            onSave={handleUniteEditorSave}
          />
        );

      case 'uniqRow':
        return (
          <UniqRowEditor
            nodeId={activeEditor.nodeId}
            nodeMetadata={activeEditor.nodeMetadata}
            inputColumns={activeEditor.inputColumns}
            initialConfig={activeEditor.initialConfig}
            onClose={() => setActiveEditor(null)}
            onSave={handleUniqRowEditorSave}
          />
        );

      case 'splitRow':
        return (
          <SplitRowEditor
            nodeId={activeEditor.nodeId}
            nodeName={activeEditor.nodeMetadata.name}
            inputColumns={activeEditor.inputColumns}
            initialConfig={activeEditor.initialConfig}
            onClose={() => setActiveEditor(null)}
            onSave={handleSplitRowEditorSave}
          />
        );

      case 'pivotToColumnsDelimited':
        return (
          <PivotToColumnsDelimitedEditor
            nodeId={activeEditor.nodeId}
            nodeMetadata={activeEditor.nodeMetadata}
            inputColumns={activeEditor.inputColumns}
            initialConfig={activeEditor.initialConfig}
            onClose={() => setActiveEditor(null)}
            onSave={handlePivotToColumnsDelimitedEditorSave}
          />
        );

      case 'denormalizeSortedRow':
        return (
          <DenormalizeSortedRowEditor
            nodeId={activeEditor.nodeId}
            nodeMetadata={activeEditor.nodeMetadata}
            inputColumns={activeEditor.inputColumns}
            initialConfig={activeEditor.initialConfig}
            onClose={() => setActiveEditor(null)}
            onSave={handleDenormalizeSortedRowEditorSave}
          />
        );

      case 'denormalize':
        return (
          <DenormalizeEditor
            nodeId={activeEditor.nodeId}
            nodeMetadata={activeEditor.nodeMetadata}
            inputColumns={activeEditor.inputColumns}
            initialConfig={activeEditor.initialConfig}
            onClose={() => setActiveEditor(null)}
            onSave={handleDenormalizeEditorSave}
          />
        );

      default:
        return null;
    }
  };

  // ==================== MAIN RENDER ====================
  return (
    <>
      {renderRoleSelectionPopup()}
      {renderConnectionFeedback()}
      {renderAutoSaveStatus()}
      {renderTMapEditorModal()}
      {renderMapEditorModal()}
      {renderMatchGroupWizard()}
      {renderActiveEditor()}

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
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          onInit={setReactFlowInstance}
          nodeTypes={nodeTypes}
          onSelectionChange={onSelectionChange}
          isValidConnection={isValidConnection}
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
          onDrop={onDrop}
          onDragOver={onDragOver}
          onMove={onMove}
          connectionMode={ConnectionMode.Loose}
          connectionLineType={ConnectionLineType.SmoothStep}
          snapToGrid={true}
          snapGrid={[15, 15]}
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

  .react-flow__node-talendNode[data-component-key="tMap"] {
    border-width: 3px;
    border-color: #8b5cf6;
  }

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

  .canvas-container {
    position: absolute !important;
    width: 100% !important;
    height: 100% !important;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
  }

  .react-flow {
    width: 100% !important;
    height: 100% !important;
    position: absolute !important;
  }

  .react-flow__pane {
    width: 100% !important;
    height: 100% !important;
  }

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