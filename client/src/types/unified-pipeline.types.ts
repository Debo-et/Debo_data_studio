// src/types/unified-pipeline.types.ts
// ============================================================================
// MERGED TYPE SYSTEM FOR CANVAS + SQL GENERATION
// Combines metadata.ts (rich component configurations) and pipeline-types.ts
// ============================================================================

import React from 'react'; // ← Added for React.ReactNode

// -------------------- ENUMS (from pipeline-types) --------------------
export enum NodeType {
  // Data Transformation
  JOIN = 'tJoin',
  DENORMALIZE = 'tDenormalize',
  NORMALIZE = 'tNormalize',
  AGGREGATE_ROW = 'tAggregateRow',
  SORT_ROW = 'tSortRow',
  FILTER_ROW = 'tFilterRow',
  FILTER_COLUMNS = 'tFilterColumns',
  
  // Field Manipulation
  REPLACE = 'tReplace',
  REPLACE_LIST = 'tReplaceList',
  CONVERT_TYPE = 'tConvertType',
  EXTRACT_DELIMITED_FIELDS = 'tExtractDelimitedFields',
  EXTRACT_REGEX_FIELDS = 'tExtractRegexFields',
  EXTRACT_JSON_FIELDS = 'tExtractJSONFields',
  EXTRACT_XML_FIELD = 'tExtractXMLField',
  
  // Row & Record Processing
  PARSE_RECORD_SET = 'tParseRecordSet',
  SPLIT_ROW = 'tSplitRow',
  PIVOT_TO_COLUMNS_DELIMITED = 'tPivotToColumnsDelimited',
  UNPIVOT_ROW = 'tUnpivotRow',
  DENORMALIZE_SORTED_ROW = 'tDenormalizeSortedRow',
  UNIQ_ROW = 'tUniqRow',
  SAMPLE_ROW = 'tSampleRow',
  
  // Validation & Quality
  SCHEMA_COMPLIANCE_CHECK = 'tSchemaComplianceCheck',
  ADD_CRC_ROW = 'tAddCRCRow',
  ADD_CRC = 'tAddCRC',
  STANDARDIZE_ROW = 'tStandardizeRow',
  DATA_MASKING = 'tDataMasking',
  ASSERT = 'tAssert',
  
  // Flow & Orchestration
  FLOW_TO_ITERATE = 'tFlowToIterate',
  ITERATE_TO_FLOW = 'tIterateToFlow',
  REPLICATE = 'tReplicate',
  UNITE = 'tUnite',
  FLOW_MERGE = 'tFlowMerge',
  FLOW_METER = 'tFlowMeter',
  FLOW_METER_CATCHER = 'tFlowMeterCatcher',
  MATCH_GROUP = 'tMatchGroup',
  
  // System & Generation
  ROW_GENERATOR = 'tRowGenerator',
  NORMALIZE_NUMBER = 'tNormalizeNumber',
  FILE_LOOKUP = 'tFileLookup',
  CACHE_IN = 'tCacheIn',
  CACHE_OUT = 'tCacheOut',
  RECORD_MATCHING = 'tRecordMatching',
  MAP = 'tMap',
  
  // Input/Output
  INPUT = 'input',
  OUTPUT = 'output',
  LOOKUP = 'lookup',
  
  // Generic
  TRANSFORM = 'transform',
  UNKNOWN = 'unknown',
  SELECT = 'SELECT',
  JSON = 'JSON',
  JSONB = 'JSONB'
}

export enum PortType {
  INPUT = 'input',
  OUTPUT = 'output'
}

export enum PortSide {
  LEFT = 'left',
  RIGHT = 'right',
  TOP = 'top',
  BOTTOM = 'bottom'
}

export enum ConnectionStatus {
  VALID = 'valid',
  INVALID = 'invalid',
  WARNING = 'warning',
  PENDING = 'pending',
  UNVALIDATED = 'unvalidated',
  ACTIVE = 'ACTIVE',
  VALIDATED = 'VALIDATED',
  PENDING_VALIDATION = 'PENDING_VALIDATION'
}

export enum PostgreSQLDataType {
  // Numeric
  SMALLINT = 'SMALLINT',
  INTEGER = 'INTEGER',
  BIGINT = 'BIGINT',
  DECIMAL = 'DECIMAL',
  NUMERIC = 'NUMERIC',
  REAL = 'REAL',
  DOUBLE_PRECISION = 'DOUBLE PRECISION',
  SERIAL = 'SERIAL',
  BIGSERIAL = 'BIGSERIAL',
  
  // Character
  VARCHAR = 'VARCHAR',
  CHAR = 'CHAR',
  TEXT = 'TEXT',
  
  // Binary
  BYTEA = 'BYTEA',
  
  // Date/Time
  TIMESTAMP = 'TIMESTAMP',
  TIMESTAMPTZ = 'TIMESTAMPTZ',
  DATE = 'DATE',
  TIME = 'TIME',
  TIMETZ = 'TIMETZ',
  INTERVAL = 'INTERVAL',
  
  // Boolean
  BOOLEAN = 'BOOLEAN',
  
  // JSON
  JSON = 'JSON',
  JSONB = 'JSONB',
  
  // UUID
  UUID = 'UUID',
  
  // Network
  INET = 'INET',
  CIDR = 'CIDR',
  MACADDR = 'MACADDR',
  
  // Geometric
  POINT = 'POINT',
  LINE = 'LINE',
  LSEG = 'LSEG',
  BOX = 'BOX',
  PATH = 'PATH',
  POLYGON = 'POLYGON',
  CIRCLE = 'CIRCLE',
  
  // Bit String
  BIT = 'BIT',
  VARBIT = 'VARBIT',
  
  // Text Search
  TSVECTOR = 'TSVECTOR',
  TSQUERY = 'TSQUERY',
  
  // XML
  XML = 'XML',
  
  // Arrays
  ARRAY = 'ARRAY'
}

export enum NodeStatus {
  IDLE = 'idle',
  RUNNING = 'running',
  COMPLETED = 'completed',
  ERROR = 'error',
  WARNING = 'warning',
  DISABLED = 'disabled'
}

export enum DataSourceType {
  POSTGRESQL = 'postgresql',
  MYSQL = 'mysql',
  SQLSERVER = 'sqlserver',
  ORACLE = 'oracle',
  SQLITE = 'sqlite',
  CSV = 'csv',
  EXCEL = 'excel',
  JSON = 'json',
  XML = 'xml',
  PARQUET = 'parquet',
  AVRO = 'avro',
  WEBSERVICE = 'webservice',
  KAFKA = 'kafka'
}

// -------------------- COMMON INTERFACES (from both) --------------------
export interface NodePosition {
  x: number;
  y: number;
}

export interface NodeSize {
  width: number;
  height: number;
}

export interface ConnectionPort {
  id: string;
  type: PortType;
  side: PortSide;
  position: number;          // percentage from top/left (0-100)
  dataType?: PostgreSQLDataType;
  label?: string;
  maxConnections?: number;
  isConnected?: boolean;
}

// -------------------- POSTGRESQL TABLE / COLUMN (from pipeline-types) --------------------
export interface PostgresColumn {
  name: string;
  dataType: PostgreSQLDataType;
  nullable: boolean;
  defaultValue?: string;
  length?: number;
  precision?: number;
  scale?: number;
  isPrimaryKey?: boolean;
  isUnique?: boolean;
  foreignKey?: {
    referencedTable: string;
    referencedColumn: string;
    onDelete: 'CASCADE' | 'SET NULL' | 'RESTRICT' | 'NO ACTION';
    onUpdate: 'CASCADE' | 'SET NULL' | 'RESTRICT' | 'NO ACTION';
  };
  checkConstraint?: string;
  comment?: string;
}

export interface PostgresTable {
  schema: string;
  name: string;
  columns: PostgresColumn[];
  primaryKey?: string[];
  uniqueConstraints?: Array<{
    name: string;
    columns: string[];
  }>;
  indexes?: Array<{
    name: string;
    columns: string[];
    isUnique: boolean;
    method: 'BTREE' | 'HASH' | 'GIN' | 'GIST' | 'SPGIST' | 'BRIN';
  }>;
  comment?: string;
}

// -------------------- SCHEMA MAPPING & TRANSFORMATION RULES (from pipeline-types) --------------------
export interface SchemaMapping {
  sourceColumn: string;
  targetColumn: string;
  transformation?: string;
  dataTypeConversion?: {
    from: PostgreSQLDataType;
    to: PostgreSQLDataType;
    params?: Record<string, any>;
  };
  defaultValue?: string;
  isRequired: boolean;
}

export interface TransformationRule<T = any> {
  id: string;
  type: string;
  params: T;
  order: number;
  condition?: string;
  errorHandling?: 'ABORT' | 'SKIP' | 'USE_DEFAULT' | 'LOG_AND_CONTINUE';
}

// -------------------- COMPONENT-SPECIFIC CONFIGURATIONS (from metadata.ts) --------------------
// These are the detailed configuration objects used by the canvas and stored in FlowNodeMeta.

export interface MapTransformation {
  id: string;
  sourceField: string;
  sourceTable?: string;
  targetField: string;
  expression: string;
  expressionType: 'SQL' | 'FUNCTION' | 'CONSTANT' | 'REFERENCE' | 'VARIABLE';
  dataType: DataType;               // from metadata.ts (see below)
  isDirectMapping: boolean;
  validationRules?: string[];
  position: number;
  nullHandling: 'KEEP_NULL' | 'DEFAULT_VALUE' | 'ERROR';
  defaultValue?: string;
}

export interface MapJoinCondition {
  id: string;
  leftTable: string;
  leftField: string;
  rightTable: string;
  rightField: string;
  operator: '=' | '!=' | '<' | '>' | '<=' | '>=' | 'LIKE' | 'IN';
  joinType: 'INNER' | 'LEFT' | 'RIGHT' | 'FULL';
  position: number;
}

export interface MapLookupConfig {
  id: string;
  lookupTable: string;
  keyFields: string[];
  returnFields: string[];
  cacheSize: number;
  defaultValue?: string;
  failOnMissing: boolean;
  lookupType: 'SIMPLE' | 'RANGE' | 'MULTIPLE';
}

export interface MapFilterCondition {
  id: string;
  field: string;
  operator: '=' | '!=' | '<' | '>' | '<=' | '>=' | 'LIKE' | 'IN' | 'IS_NULL' | 'NOT_NULL';
  value?: string | number | boolean;
  valueType: 'CONSTANT' | 'FIELD' | 'PARAMETER' | 'VARIABLE';
  logicGroup?: number;
  position: number;
}

export interface MapVariable {
  id: string;
  name: string;
  type: DataType;
  expression: string;
  scope: 'ROW' | 'SESSION' | 'GLOBAL';
  isConstant: boolean;
}

export interface MapComponentConfiguration {
  version: string;
  transformations: MapTransformation[];
  joins?: MapJoinCondition[];
  lookups?: MapLookupConfig[];
  filters?: MapFilterCondition[];
  variables?: MapVariable[];
  outputSchema: SchemaDefinition & { persistenceLevel: 'MEMORY' | 'TEMPORARY' | 'PERSISTENT' };
  sqlGeneration: {
    requiresDistinct: boolean;
    requiresAggregation: boolean;
    requiresWindowFunction: boolean;
    requiresSubquery: boolean;
    estimatedRowMultiplier: number;
    joinOptimizationHint?: string;
    parallelizable: boolean;
    batchSize?: number;
    memoryHint?: 'LOW' | 'MEDIUM' | 'HIGH';
  };
  compilerMetadata: {
    lastModified: string;
    createdBy: string;
    mappingCount: number;
    validationStatus: 'VALID' | 'WARNING' | 'ERROR';
    warnings?: string[];
    dependencies: string[];
    columnDependencies: Record<string, string[]>;
    compiledSql?: string;
    compilationTimestamp?: string;
  };
}

export interface JoinComponentConfiguration {
  version: string;
  joinType: 'INNER' | 'LEFT' | 'RIGHT' | 'FULL' | 'CROSS';
  joinConditions: Array<{
    id: string;
    leftTable: string;
    leftField: string;
    rightTable: string;
    rightField: string;
    operator: '=' | '!=' | '<' | '>' | '<=' | '>=' | 'LIKE';
    position: number;
  }>;
  joinHints: {
    enableJoinHint: boolean;
    joinHint?: string;
    maxParallelism?: number;
    memoryGrant?: number;
  };
  outputSchema: {
    fields: FieldSchema[];
    deduplicateFields: boolean;
    fieldAliases: Record<string, string>;
  };
  sqlGeneration: {
    joinAlgorithm: 'HASH' | 'MERGE' | 'NESTED_LOOP';
    estimatedJoinCardinality: number;
    nullHandling: 'INCLUDE' | 'EXCLUDE' | 'TREAT_AS_FALSE';
    requiresSort: boolean;
    canParallelize: boolean;
  };
  compilerMetadata: {
    lastModified: string;
    joinCardinality?: number;
    optimizationApplied: boolean;
    warnings?: string[];
  };
}

export interface FilterComponentConfiguration {
  version: string;
  filterConditions: Array<{
    id: string;
    field: string;
    operator: '=' | '!=' | '<' | '>' | '<=' | '>=' | 'LIKE' | 'IN' | 'IS_NULL' | 'NOT_NULL';
    value?: string | number | boolean;
    valueType: 'CONSTANT' | 'FIELD' | 'PARAMETER' | 'VARIABLE';
    logicGroup: number;
    position: number;
  }>;
  filterLogic: 'AND' | 'OR' | string;
  optimization: {
    pushDown: boolean;
    indexable: boolean;
    estimatedSelectivity: number;
  };
  sqlGeneration: {
    whereClause: string;
    parameterized: boolean;
    requiresSubquery: boolean;
    canUseIndex: boolean;
  };
  compilerMetadata: {
    lastModified: string;
    estimatedRowReduction: number;
    warnings?: string[];
  };
}

export interface LookupComponentConfiguration {
  version: string;
  lookupType: 'SIMPLE' | 'RANGE' | 'MULTIPLE' | 'FUZZY';
  lookupKeyFields: string[];
  lookupReturnFields: string[];
  lookupTable: string;
  cache: {
    enabled: boolean;
    cacheSize: number;
    cacheType: 'LRU' | 'FIFO' | 'TTL';
    ttlSeconds?: number;
  };
  fallback: {
    failOnMissing: boolean;
    defaultValue?: string;
    defaultValueStrategy: 'NULL' | 'DEFAULT' | 'ERROR';
  };
  outputSchema: {
    fields: FieldSchema[];
    prefixLookupFields: boolean;
  };
  sqlGeneration: {
    joinType: 'LEFT' | 'INNER';
    requiresDistinct: boolean;
    estimatedCacheHitRate: number;
    canParallelize: boolean;
    batchSize?: number;
  };
  compilerMetadata: {
    lastModified: string;
    cacheStatistics?: {
      hits: number;
      misses: number;
      size: number;
    };
    warnings?: string[];
  };
}

export interface AggregateComponentConfiguration {
  version: string;
  groupByFields: string[];
  aggregateFunctions: Array<{
    id: string;
    function: 'SUM' | 'COUNT' | 'AVG' | 'MIN' | 'MAX' | 'COUNT_DISTINCT' | 'STDDEV';
    field: string;
    alias: string;
    distinct: boolean;
  }>;
  havingConditions?: Array<{
    id: string;
    field: string;
    operator: '=' | '!=' | '<' | '>' | '<=' | '>=';
    value: string | number;
  }>;
  optimization: {
    canUseIndex: boolean;
    requiresSort: boolean;
    estimatedGroupCount: number;
    memoryHint?: 'LOW' | 'MEDIUM' | 'HIGH';
  };
  outputSchema: {
    fields: FieldSchema[];
    groupByFields: string[];
    aggregateFields: string[];
  };
  sqlGeneration: {
    groupByClause: string;
    aggregateClause: string;
    havingClause?: string;
    requiresWindowFunction: boolean;
    parallelizable: boolean;
    sortRequired: boolean;
  };
  compilerMetadata: {
    lastModified: string;
    estimatedCardinality: number;
    warnings?: string[];
  };
}

export interface SortComponentConfiguration {
  version: string;
  sortFields: Array<{
    field: string;
    direction: 'ASC' | 'DESC';
    nullsFirst: boolean;
    position: number;
  }>;
  performance: {
    estimatedRowCount: number;
    memoryRequired?: number;
    canParallelize: boolean;
  };
  sqlGeneration: {
    orderByClause: string;
    requiresDistinct: boolean;
    limitOffset?: {
      limit: number;
      offset: number;
    };
  };
  compilerMetadata: {
    lastModified: string;
    sortComplexity: 'SIMPLE' | 'MEDIUM' | 'COMPLEX';
    warnings?: string[];
  };
}

export interface InputComponentConfiguration {
  version: string;
  sourceType: DataSourceType;                 // using enum from pipeline-types
  sourceDetails: {
    connectionString?: string;
    tableName?: string;
    filePath?: string;
    format?: 'CSV' | 'JSON' | 'PARQUET' | 'AVRO';
    encoding?: string;
    delimiter?: string;
    hasHeader?: boolean;
  };
  pushdown: {
    enabled: boolean;
    filterClause?: string;
    columnSelection?: string[];
    limit?: number;
  };
  schema: SchemaDefinition;
  sqlGeneration: {
    fromClause: string;
    alias: string;
    isTemporary: boolean;
    estimatedRowCount: number;
    parallelizable: boolean;
  };
  compilerMetadata: {
    lastModified: string;
    sourceValidated: boolean;
    warnings?: string[];
  };
}

export interface OutputComponentConfiguration {
  version: string;
  targetType: DataSourceType;
  targetDetails: {
    connectionString?: string;
    tableName: string;
    filePath?: string;
    format?: 'CSV' | 'JSON' | 'PARQUET' | 'AVRO';
    mode: 'APPEND' | 'OVERWRITE' | 'ERROR_IF_EXISTS' | 'IGNORE';
  };
  writeOptions: {
    batchSize: number;
    commitInterval?: number;
    truncateFirst: boolean;
    createTable: boolean;
  };
  schemaMapping: SchemaMapping[];
  sqlGeneration: {
    insertStatement: string;
    mergeStatement?: string;
    requiresTransaction: boolean;
    parallelizable: boolean;
    batchOptimized: boolean;
  };
  compilerMetadata: {
    lastModified: string;
    targetValidated: boolean;
    warnings?: string[];
  };
}

// Union type for all component configurations
export type ComponentConfiguration =
  | { type: 'MAP'; config: MapComponentConfiguration }
  | { type: 'JOIN'; config: JoinComponentConfiguration }
  | { type: 'FILTER'; config: FilterComponentConfiguration }
  | { type: 'LOOKUP'; config: LookupComponentConfiguration }
  | { type: 'AGGREGATE'; config: AggregateComponentConfiguration }
  | { type: 'SORT'; config: SortComponentConfiguration }
  | { type: 'INPUT'; config: InputComponentConfiguration }
  | { type: 'OUTPUT'; config: OutputComponentConfiguration }
  | { type: 'OTHER'; config: Record<string, any> };

// -------------------- FIELD / SCHEMA DEFINITIONS (from metadata.ts) --------------------
export type DataType = 'STRING' | 'INTEGER' | 'DECIMAL' | 'BOOLEAN' | 'DATE' | 'TIMESTAMP' | 'BINARY';

export interface FieldSchema {
  id: string;
  name: string;
  type: DataType;
  length?: number;
  precision?: number;
  scale?: number;
  nullable: boolean;
  isKey: boolean;
  defaultValue?: string;
  description?: string;
  originalName?: string;
  transformation?: string;
  metadata?: Record<string, any>;
}

export interface SchemaDefinition {
  id: string;
  name: string;
  alias?: string;
  fields: FieldSchema[];
  sourceComponentId?: string;
  isTemporary: boolean;
  isMaterialized: boolean;
  rowCount?: number;
  metadata?: Record<string, any>;
}

// -------------------- NODE METADATA (unified) --------------------
// This replaces both FlowNodeMeta and the old NodeMetadata.
export interface UnifiedNodeMetadata {
  // Core component configuration (discriminated union)
  configuration: ComponentConfiguration;

  // Input/output schemas
  schemas?: {
    input?: SchemaDefinition[];
    output?: SchemaDefinition;
    transformed?: SchemaDefinition;
  };

  // Validation state
  validation?: {
    isValid: boolean;
    errors: string[];
    warnings: string[];
  };

  // PostgreSQL-specific configuration (optional)
  postgresConfig?: {
    targetSchema: string;
    createTable: 'IF_NOT_EXISTS' | 'DROP_AND_CREATE' | 'TRUNCATE' | 'APPEND';
    isolationLevel?: 'READ_UNCOMMITTED' | 'READ_COMMITTED' | 'REPEATABLE_READ' | 'SERIALIZABLE';
    batchSize?: number;
    disableConstraints?: boolean;
    onConflict?: {
      targetColumns: string[];
      action: 'DO_NOTHING' | 'DO_UPDATE';
      updateColumns?: string[];
    };
  };

  // Source system metadata (for input nodes)
  sourceMetadata?: {
    type: DataSourceType;
    connectionString?: string;
    query?: string;
    filePath?: string;
    sheetName?: string;
    headers?: boolean;
    delimiter?: string;
  };

  // General metadata
  description?: string;
  tags?: string[];
  version?: string;
  createdBy?: string;
  createdAt?: string;
  updatedAt?: string;
  iconUrl?: string;
  displayName?: string;
  cleanType?: string;
  scaleFactor?: number;
  visualScaling?: {
    fontSizeScale: number;
    iconScale: number;
    handleScale: number;
  };
  originalBaseName?: string;
  originalDisplayName?: string;
  repositoryNodeId?: string;
  repositoryNodeType?: string;
  originalNodeName?: string;
  originalNodeType?: string;
  fullRepositoryMetadata?: any;
  extractedColumns?: any[];
  dragMetadata?: any;
  source?: string;
  sourceType?: string;
  category?: string;
  isDataSource?: boolean;
  [key: string]: any; // allow extra
}

// -------------------- MAIN CANVAS NODE (unified) --------------------
export interface UnifiedCanvasNode {
  id: string;
  name: string;
  type: NodeType;                       // from pipeline-types enum
  nodeType?: 'input' | 'output' | 'transform' | 'process';
  componentType?: 'processing' | 'standardized' | 'palette-component' | 'sidebar-item';
  componentCategory?: 'output' | 'input' | 'process' | 'transform' | undefined; // ← added 'transform'

  position: NodePosition;
  size: NodeSize;
  connectionPorts?: ConnectionPort[];

  // The unified metadata (contains configuration and schemas)
  metadata?: UnifiedNodeMetadata;

  status?: NodeStatus;
  draggable?: boolean;
  droppable?: boolean;
  dragType?: string;
  technology?: string;
  schemaName?: string;
  tableName?: string;
  fileName?: string;
  sheetName?: string;

  visualProperties?: {
    color?: string;
    icon?: React.ReactNode;   // ← changed from string | undefined to ReactNode
    borderColor?: string;
    backgroundColor?: string;
  };

  // For backward compatibility during transition
  // These fields will be populated from metadata when needed
  // but are kept here to avoid breaking existing code
  joinConfig?: any;
  filterConfig?: any;
  aggregationConfig?: any;
  sortConfig?: any;
  transformationRules?: any;
  schemaMappings?: any;
  tableMapping?: any;
}

// -------------------- EDGE / CONNECTION (unified) --------------------
export interface UnifiedCanvasConnection {
  id: string;
  sourceNodeId: string;
  sourcePortId: string;
  targetNodeId: string;
  targetPortId: string;

  dataFlow: {
    schemaMappings: SchemaMapping[];
    transformationRules?: TransformationRule[];
    expectedVolume?: number;
    qualityRules?: Array<{
      rule: string;
      severity: 'ERROR' | 'WARNING' | 'INFO';
    }>;
  };

  status: ConnectionStatus;
  errors?: Array<{
    message: string;
    severity: 'ERROR' | 'WARNING';
    timestamp: string;
    details?: Record<string, any>;
  }>;
  metrics?: {
    latencyMs?: number;
    throughputRowsPerSecond?: number;
    dataSizeBytes?: number;
    lastUpdated?: string;
  };
  metadata?: {
    description?: string;
    createdBy?: string;
    createdAt: string;
    updatedAt?: string;
    relationType?: string;               // e.g., 'JOIN', 'FILTER'
    joinCondition?: string;
    filterCondition?: string;
    [key: string]: any;
  };
}

// -------------------- SQL GENERATION TYPES (from pipeline-types) --------------------
export interface GeneratedSQLFragment {
  sql: string;
  dependencies: string[];
  parameters: Map<string, any>;
  errors: Array<{ code: string; message: string; severity: 'ERROR' | 'WARNING'; suggestion?: string }>;
  warnings: string[];
  metadata: {
    generatedAt: string;
    fragmentType: string;
    lineCount: number;
    [key: string]: any;
  };
}

export interface SQLGenerationJob {
  id: string;
  name: string;
  status: 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';
  createdAt: string;
  startedAt?: string;
  completedAt?: string;
  nodes: UnifiedCanvasNode[];
  connections: UnifiedCanvasConnection[];
  sqlGenerated: string[];
  errors: string[];
  warnings: string[];
  settings: {
    includeComments: boolean;
    formatSQL: boolean;
    validateSQL: boolean;
    targetDialect: 'postgresql' | 'mysql' | 'sqlserver' | 'oracle';
  };
}

export interface PipelineGenerationResult {
  success: boolean;
  sql: string[];
  executionPlans: Array<{
    nodeId: string;
    sql: string;
    plan: any;
  }>;
  warnings: string[];
  errors: string[];
  jobId?: string;
  generatedAt: string;
  statistics: {
    totalNodes: number;
    totalConnections: number;
    generationTimeMs: number;
    validationTimeMs: number;
  };
}

// -------------------- TYPE GUARDS (for ComponentConfiguration) --------------------
export function isMapConfig(config: ComponentConfiguration): config is { type: 'MAP'; config: MapComponentConfiguration } {
  return config.type === 'MAP';
}

export function isJoinConfig(config: ComponentConfiguration): config is { type: 'JOIN'; config: JoinComponentConfiguration } {
  return config.type === 'JOIN';
}

export function isFilterConfig(config: ComponentConfiguration): config is { type: 'FILTER'; config: FilterComponentConfiguration } {
  return config.type === 'FILTER';
}

export function isLookupConfig(config: ComponentConfiguration): config is { type: 'LOOKUP'; config: LookupComponentConfiguration } {
  return config.type === 'LOOKUP';
}

export function isAggregateConfig(config: ComponentConfiguration): config is { type: 'AGGREGATE'; config: AggregateComponentConfiguration } {
  return config.type === 'AGGREGATE';
}

export function isSortConfig(config: ComponentConfiguration): config is { type: 'SORT'; config: SortComponentConfiguration } {
  return config.type === 'SORT';
}

export function isInputConfig(config: ComponentConfiguration): config is { type: 'INPUT'; config: InputComponentConfiguration } {
  return config.type === 'INPUT';
}

export function isOutputConfig(config: ComponentConfiguration): config is { type: 'OUTPUT'; config: OutputComponentConfiguration } {
  return config.type === 'OUTPUT';
}

// -------------------- UTILITY FUNCTIONS --------------------
/**
 * Extracts the component-specific configuration from a node's metadata
 * and returns it in a type-safe way. Example:
 *   const mapConfig = getComponentConfig(node, 'MAP');
 *   if (mapConfig) { ... }
 */
export function getComponentConfig<T extends ComponentConfiguration['type']>(
  node: UnifiedCanvasNode,
  expectedType: T
): Extract<ComponentConfiguration, { type: T }>['config'] | undefined {
  if (!node.metadata?.configuration) return undefined;
  const conf = node.metadata.configuration;
  if (conf.type === expectedType) {
    return (conf as any).config;
  }
  return undefined;
}

/**
 * Helper to convert the old pipeline-types node to the unified node.
 * Useful during migration.
 */
export function toUnifiedCanvasNode(oldNode: any): UnifiedCanvasNode {
  // Implement mapping as needed
  return {
    id: oldNode.id,
    name: oldNode.name,
    type: oldNode.type,
    position: oldNode.position,
    size: oldNode.size,
    connectionPorts: oldNode.connectionPorts,
    metadata: {
      configuration: oldNode.metadata?.configuration || { type: 'OTHER', config: {} },
      ...oldNode.metadata,
    },
    status: oldNode.status,
    draggable: oldNode.draggable,
    droppable: oldNode.droppable,
    dragType: oldNode.dragType,
    technology: oldNode.technology,
    schemaName: oldNode.schemaName,
    tableName: oldNode.tableName,
    fileName: oldNode.fileName,
    sheetName: oldNode.sheetName,
    visualProperties: oldNode.visualProperties,
  };
}