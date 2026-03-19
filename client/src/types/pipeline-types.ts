// src/types/pipeline-types.ts

/**
 * Node-based data pipeline system for PostgreSQL SQL generation
 */

// ==================== ENUMS ====================

/**
 * Processing node types (40+ types)
 */
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
  
  // Input/Output Nodes
  INPUT = 'input',
  OUTPUT = 'output',
  LOOKUP = 'lookup',
  
  // Generic
  TRANSFORM = 'transform',
  UNKNOWN = 'unknown',
  SELECT = "SELECT",
  JSON = "JSON",
  JSONB = "JSONB"
}

/**
 * Port types for connections
 */
export enum PortType {
  INPUT = 'input',
  OUTPUT = 'output'
}

/**
 * Port side positioning
 */
export enum PortSide {
  LEFT = 'left',
  RIGHT = 'right',
  TOP = 'top',
  BOTTOM = 'bottom'
}

/**
 * Connection validation status
 */
export enum ConnectionStatus {
  VALID = 'valid',
  INVALID = 'invalid',
  WARNING = 'warning',
  PENDING = 'pending',
  UNVALIDATED = 'unvalidated',
  ACTIVE = "ACTIVE",
  VALIDATED = "VALIDATED",
  PENDING_VALIDATION = "PENDING_VALIDATION"
}

/**
 * PostgreSQL data types
 */
export enum PostgreSQLDataType {
  // Numeric Types
  SMALLINT = 'SMALLINT',
  INTEGER = 'INTEGER',
  BIGINT = 'BIGINT',
  DECIMAL = 'DECIMAL',
  NUMERIC = 'NUMERIC',
  REAL = 'REAL',
  DOUBLE_PRECISION = 'DOUBLE PRECISION',
  SERIAL = 'SERIAL',
  BIGSERIAL = 'BIGSERIAL',
  
  // Character Types
  VARCHAR = 'VARCHAR',
  CHAR = 'CHAR',
  TEXT = 'TEXT',
  
  // Binary Types
  BYTEA = 'BYTEA',
  
  // Date/Time Types
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
  
  // Network Address
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
  
  // Arrays (suffix with [])
  ARRAY = 'ARRAY'
}

/**
 * Node execution status
 */
export enum NodeStatus {
  IDLE = 'idle',
  RUNNING = 'running',
  COMPLETED = 'completed',
  ERROR = 'error',
  WARNING = 'warning',
  DISABLED = 'disabled'
}

/**
 * Data source types
 */
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

// ==================== INTERFACES ====================

/**
 * Position in 2D space
 */
export interface NodePosition {
  /** X coordinate */
  x: number;
  /** Y coordinate */
  y: number;
}

/**
 * Node dimensions
 */
export interface NodeSize {
  /** Width in pixels */
  width: number;
  /** Height in pixels */
  height: number;
}

/**
 * Connection port for data flow
 */
export interface ConnectionPort {
  /** Unique port identifier */
  id: string;
  /** Port type (input/output) */
  type: PortType;
  /** Side of the node where port is located */
  side: PortSide;
  /** Position percentage from top/left (0-100) */
  position: number;
  /** Data type accepted/provided by this port */
  dataType?: PostgreSQLDataType;
  /** Port label for display */
  label?: string;
  /** Maximum connections allowed */
  maxConnections?: number;
  /** Whether port is currently connected */
  isConnected?: boolean;
}

/**
 * PostgreSQL column definition
 */
export interface PostgresColumn {
  /** Column name */
  name: string;
  /** PostgreSQL data type */
  dataType: PostgreSQLDataType;
  /** Whether column allows NULL */
  nullable: boolean;
  /** Default value */
  defaultValue?: string;
  /** Maximum length for character types */
  length?: number;
  /** Precision for numeric types */
  precision?: number;
  /** Scale for numeric types */
  scale?: number;
  /** Whether column is part of primary key */
  isPrimaryKey?: boolean;
  /** Whether column is unique */
  isUnique?: boolean;
  /** Foreign key constraint */
  foreignKey?: {
    referencedTable: string;
    referencedColumn: string;
    onDelete: 'CASCADE' | 'SET NULL' | 'RESTRICT' | 'NO ACTION';
    onUpdate: 'CASCADE' | 'SET NULL' | 'RESTRICT' | 'NO ACTION';
  };
  /** Check constraint */
  checkConstraint?: string;
  /** Column comment/description */
  comment?: string;
}

/**
 * PostgreSQL table definition
 */
export interface PostgresTable {
  /** Schema name */
  schema: string;
  /** Table name */
  name: string;
  /** Column definitions */
  columns: PostgresColumn[];
  /** Primary key columns */
  primaryKey?: string[];
  /** Unique constraints */
  uniqueConstraints?: Array<{
    name: string;
    columns: string[];
  }>;
  /** Index definitions */
  indexes?: Array<{
    name: string;
    columns: string[];
    isUnique: boolean;
    method: 'BTREE' | 'HASH' | 'GIN' | 'GIST' | 'SPGIST' | 'BRIN';
  }>;
  /** Table comment/description */
  comment?: string;
}

/**
 * Schema mapping between source and target
 */
export interface SchemaMapping {
  /** Source column name */
  sourceColumn: string;
  /** Target column name */
  targetColumn: string;
  /** Data transformation expression */
  transformation?: string;
  /** Data type conversion */
  dataTypeConversion?: {
    from: PostgreSQLDataType;
    to: PostgreSQLDataType;
    params?: Record<string, any>;
  };
  /** Default value if source is null */
  defaultValue?: string;
  /** Whether mapping is required */
  isRequired: boolean;
}

/**
 * Transformation rule for data processing
 */
export interface TransformationRule<T = any> {
  /** Rule identifier */
  id: string;
  /** Rule type */
  type: string;
  /** Rule parameters */
  params: T;
  /** Execution order */
  order: number;
  /** Condition for rule application */
  condition?: string;
  /** Error handling strategy */
  errorHandling?: 'ABORT' | 'SKIP' | 'USE_DEFAULT' | 'LOG_AND_CONTINUE';
}

/**
 * Join configuration
 */
export interface JoinConfig {
  /** Join type: INNER, LEFT, RIGHT, FULL, CROSS */
  type: 'INNER' | 'LEFT' | 'RIGHT' | 'FULL' | 'CROSS';
  /** Left table alias */
  leftAlias?: string;
  /** Right table alias */
  rightAlias?: string;
  /** Join condition */
  condition: string;
  /** Additional WHERE clause */
  whereClause?: string;
}

/**
 * Filter configuration
 */
export interface FilterConfig {
  /** Filter condition (SQL WHERE clause) */
  condition: string;
  /** Parameters for prepared statement */
  parameters?: Record<string, any>;
  /** Whether to include or exclude matching rows */
  operation: 'INCLUDE' | 'EXCLUDE';
}

/**
 * Aggregation configuration
 */
export interface AggregationConfig {
  /** Group by columns */
  groupBy: string[];
  /** Aggregate functions */
  aggregates: Array<{
    column: string;
    function: 'SUM' | 'AVG' | 'COUNT' | 'MIN' | 'MAX' | 'STDDEV' | 'VARIANCE';
    alias: string;
  }>;
  /** Having clause */
  having?: string;
}

/**
 * Sort configuration
 */
export interface SortConfig {
  /** Sort columns and directions */
  columns: Array<{
    column: string;
    direction: 'ASC' | 'DESC';
    nullsFirst?: boolean;
  }>;
  /** Maximum rows to return */
  limit?: number;
  /** Offset for pagination */
  offset?: number;
}

/**
 * Match Group configuration for survivorship
 */
export interface MatchGroupConfig {
  /** Input flow identifier */
  inputFlow: string;
  /** Schema columns */
  schemaColumns: Array<{
    name: string;
    dataType: PostgreSQLDataType;
    isGroupingKey?: boolean;
    survivorshipRule?: 'MIN' | 'MAX' | 'FIRST' | 'LAST' | 'CONCAT' | 'SUM' | 'AVG';
  }>;
  /** Grouping keys */
  groupingKeys: string[];
  /** Survivorship rules */
  survivorshipRules: Array<{
    column: string;
    rule: 'MIN' | 'MAX' | 'FIRST' | 'LAST' | 'CONCAT' | 'SUM' | 'AVG';
    params?: Record<string, any>;
  }>;
  /** Output table name */
  outputTableName: string;
  /** Deduplication strategy */
  deduplication: 'KEEP_FIRST' | 'KEEP_LAST' | 'KEEP_ALL' | 'MERGE';
}

/**
 * Map Editor configuration
 */
export interface MapEditorConfig {
  /** Source tables */
  sourceTables: PostgresTable[];
  /** Target tables */
  targetTables: PostgresTable[];
  /** Column mappings */
  columnMappings: SchemaMapping[];
  /** Transformation rules */
  transformations: TransformationRule[];
  /** Variables for expressions */
  variables: Record<string, any>;
}

/**
 * Node metadata with PostgreSQL-specific configuration
 */
export interface NodeMetadata {
  /** PostgreSQL table mapping */
  tableMapping?: PostgresTable;
  /** Schema mappings for data flow */
  schemaMappings?: SchemaMapping[];
  /** Transformation rules */
  transformationRules?: TransformationRule[];
  
  // Node-specific configurations
  joinConfig?: JoinConfig;
  filterConfig?: FilterConfig;
  aggregationConfig?: AggregationConfig;
  sortConfig?: SortConfig;
  matchGroupConfig?: MatchGroupConfig;
  mapEditorConfig?: MapEditorConfig;
  targetTableName?: string;
  sourceTableName?: string; 
  
  // PostgreSQL-specific
  postgresConfig?: {
    /** Target schema */
    targetSchema: string;
    /** Table creation strategy */
    createTable: 'IF_NOT_EXISTS' | 'DROP_AND_CREATE' | 'TRUNCATE' | 'APPEND';
    /** Transaction isolation level */
    isolationLevel?: 'READ_UNCOMMITTED' | 'READ_COMMITTED' | 'REPEATABLE_READ' | 'SERIALIZABLE';
    /** Batch size for inserts/updates */
    batchSize?: number;
    /** Enable/disable constraints during load */
    disableConstraints?: boolean;
    /** Use ON CONFLICT for upserts */
    onConflict?: {
      targetColumns: string[];
      action: 'DO_NOTHING' | 'DO_UPDATE';
      updateColumns?: string[];
    };
  };
  
  // Source system metadata
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
}

/**
 * Node validation result
 */
export interface NodeValidationResult {
  /** Whether node is valid */
  isValid: boolean;
  /** Node identifier */
  nodeId: string;
  /** Node type */
  nodeType: NodeType;
  /** Validation issues */
  issues: string[];
  /** Suggestions for improvement */
  suggestions: string[];
  /** PostgreSQL compatibility */
  postgresCompatibility: {
    compatible: boolean;
    issues: string[];
    requiredExtensions: string[];
  };
  /** Metadata */
  metadata: {
    validatedAt: string;
    validatorVersion: string;
  };
}



/**
 * Main Canvas Node interface
 */
export interface CanvasNode {
  /** Unique node identifier */
  id: string;
  /** Node display name */
  name: string;
  /** Node type from enum */
type: NodeType | string;  // Allow both NodeType and string
  /** Node subtype/category */
  nodeType?: 'input' | 'output' | 'transform' | 'process';
  /** Component type for UI */
  componentType?: 'processing' | 'standardized' | 'palette-component' | 'sidebar-item';
  /** Component category for organization */
  componentCategory?: 'input' | 'output' | 'process';
  
  /** Position on canvas */
  position: NodePosition;
  /** Node dimensions */
  size: NodeSize;
  
  /** Connection ports */
  connectionPorts?: ConnectionPort[];
  
  /** Node metadata including PostgreSQL config */
  metadata?: NodeMetadata;
  
  /** Node execution status */
  status?: NodeStatus;
  
  /** UI/interaction properties */
  draggable?: boolean;
  droppable?: boolean;
  dragType?: string;
  
  /** Technology/data source */
  technology?: string;
  
  /** Schema/table information */
  schemaName?: string;
  tableName?: string;
  fileName?: string;
  sheetName?: string;
  
  /** Visual properties */
  visualProperties?: {
    color?: string;
    icon?: string;
    borderColor?: string;
    backgroundColor?: string;
  };
}

/**
 * Canvas connection between nodes
 */
export interface CanvasConnection {
  /** Unique connection identifier */
  id: string;
  /** Source node ID */
  sourceNodeId: string;
  /** Source port ID */
  sourcePortId: string;
  /** Target node ID */
  targetNodeId: string;
  /** Target port ID */
  targetPortId: string;
  
  /** Data flow metadata */
  dataFlow: {
    /** Schema mappings for this connection */
    schemaMappings: SchemaMapping[];
    /** Data transformation rules */
    transformationRules?: TransformationRule[];
    /** Expected data volume */
    expectedVolume?: number;
    /** Data quality rules */
    qualityRules?: Array<{
      rule: string;
      severity: 'ERROR' | 'WARNING' | 'INFO';
    }>;
  };
  
  /** Connection status */
  status: ConnectionStatus;
  
  /** Error tracking */
  errors?: Array<{
    message: string;
    severity: 'ERROR' | 'WARNING';
    timestamp: string;
    details?: Record<string, any>;
  }>;
  
  /** Performance metrics */
  metrics?: {
    latencyMs?: number;
    throughputRowsPerSecond?: number;
    dataSizeBytes?: number;
    lastUpdated?: string;
  };
  
  /** Connection metadata */
  metadata?: {
    description?: string;
    createdBy?: string;
    createdAt?: string;
    updatedAt?: string;
  };
}

/**
 * Connection validation result
 */
export interface ConnectionValidationResult {
  /** Whether connection is valid */
  isValid: boolean;
  /** Overall validation score (0-100) */
  compatibilityScore: number;
  
  /** Error messages */
  errors: string[];
  /** Warning messages */
  warnings: string[];
  /** Informational messages */
  info: string[];
  
  /** Schema compatibility details */
  schemaCompatibility: {
    /** Number of compatible columns */
    compatibleColumns: number;
    /** Number of incompatible columns */
    incompatibleColumns: number;
    /** Data type compatibility matrix */
    typeCompatibility: Array<{
      sourceColumn: string;
      sourceType: PostgreSQLDataType;
      targetColumn: string;
      targetType: PostgreSQLDataType;
      isCompatible: boolean;
      suggestedConversion?: string;
    }>;
  };
  
  /** Performance implications */
  performanceImplications?: {
    estimatedLatencyMs: number;
    potentialBottleneck: boolean;
    recommendations: string[];
  };
  
  /** Validation timestamp */
  timestamp: string;
}

/**
 * Generated SQL with metadata
 */
export interface GeneratedSQL {
  /** SQL script string */
  id: string;
  sql: string;
  
  /** SQL type */
  type: 'DDL' | 'DML' | 'DQL' | 'PROCEDURE' | 'FUNCTION' | 'TRIGGER';
  
  /** Dependencies (node IDs that this SQL depends on) */
  dependencies: string[];
  
  /** Execution plan metadata */
  executionPlan: {
    /** Estimated cost */
    estimatedCost?: number;
    /** Execution steps */
    steps: Array<{
      step: number;
      operation: string;
      description: string;
      estimatedRows?: number;
      estimatedCost?: number;
    }>;
    /** Parallel execution opportunities */
    canParallelize: boolean;
    /** Recommended indexes */
    recommendedIndexes?: Array<{
      table: string;
      columns: string[];
      type: 'BTREE' | 'HASH' | 'GIN' | 'GIST';
    }>;
  };
  
  /** Performance hints */
  performanceHints?: string[];
  
  /** Validation results */
  validation?: {
    syntaxValid: boolean;
    semanticValid: boolean;
    warnings: string[];
  };
  
  /** Metadata */
  metadata: {
    generatedAt: string;
    generatorVersion: string;
    nodeId: string;
    nodeType: NodeType;
    parameters?: Record<string, any>;
    updatedAt?: string;
  };
}

/**
 * Complete pipeline configuration
 */
export interface DataPipeline {
  /** Pipeline identifier */
  id: string;
  /** Pipeline name */
  name: string;
  /** Pipeline description */
  description?: string;
  
  /** Canvas nodes */
  nodes: CanvasNode[];
  /** Connections between nodes */
  connections: CanvasConnection[];
  
  /** Variables for parameterization */
  variables: Record<string, any>;
  
  /** PostgreSQL target configuration */
  postgresTarget: {
    host: string;
    port: number;
    database: string;
    schema: string;
    username: string;
    sslMode?: 'disable' | 'allow' | 'prefer' | 'require' | 'verify-ca' | 'verify-full';
    connectionPool?: {
      min: number;
      max: number;
    };
  };
  
  /** Execution configuration */
  executionConfig: {
    /** Execution mode */
    mode: 'SEQUENTIAL' | 'PARALLEL' | 'HYBRID';
    /** Maximum parallel branches */
    maxParallelBranches?: number;
    /** Error handling strategy */
    errorHandling: 'STOP_ON_ERROR' | 'CONTINUE_ON_ERROR' | 'LOG_AND_CONTINUE';
    /** Retry configuration */
    retryConfig?: {
      maxRetries: number;
      retryDelayMs: number;
      backoffMultiplier: number;
    };
    /** Transaction management */
    transactionManagement: {
      useTransactions: boolean;
      autoCommit: boolean;
      isolationLevel: 'READ_COMMITTED' | 'REPEATABLE_READ' | 'SERIALIZABLE';
    };
  };
  
  /** Metadata */
  metadata: {
    createdBy: string;
    createdAt: string;
    updatedBy?: string;
    updatedAt?: string;
    version: string;
    tags?: string[];
  };
  
  /** Generated SQL scripts (cached) */
  generatedSQL?: GeneratedSQL[];
}

// ==================== TYPE GUARDS ====================

/**
 * Type guard for CanvasNode
 */
export function isCanvasNode(node: any): node is CanvasNode {
  return (
    node &&
    typeof node.id === 'string' &&
    typeof node.name === 'string' &&
    Object.values(NodeType).includes(node.type) &&
    typeof node.position === 'object' &&
    typeof node.position.x === 'number' &&
    typeof node.position.y === 'number' &&
    typeof node.size === 'object' &&
    typeof node.size.width === 'number' &&
    typeof node.size.height === 'number'
  );
}

/**
 * Type guard for CanvasConnection
 */
export function isCanvasConnection(conn: any): conn is CanvasConnection {
  return (
    conn &&
    typeof conn.id === 'string' &&
    typeof conn.sourceNodeId === 'string' &&
    typeof conn.sourcePortId === 'string' &&
    typeof conn.targetNodeId === 'string' &&
    typeof conn.targetPortId === 'string' &&
    Object.values(ConnectionStatus).includes(conn.status)
  );
}

/**
 * Type guard for ConnectionValidationResult
 */
export function isConnectionValidationResult(result: any): result is ConnectionValidationResult {
  return (
    result &&
    typeof result.isValid === 'boolean' &&
    typeof result.compatibilityScore === 'number' &&
    Array.isArray(result.errors) &&
    Array.isArray(result.warnings) &&
    Array.isArray(result.info) &&
    typeof result.timestamp === 'string'
  );
}

/**
 * Type guard for GeneratedSQL
 */
export function isGeneratedSQL(sql: any): sql is GeneratedSQL {
  return (
    sql &&
    typeof sql.sql === 'string' &&
    Array.isArray(sql.dependencies) &&
    typeof sql.executionPlan === 'object' &&
    typeof sql.metadata === 'object' &&
    typeof sql.metadata.generatedAt === 'string'
  );
}

/**
 * Type guard for Processing Node
 */
export function isProcessingNode(node: CanvasNode): boolean {
  const processingTypes = [
    NodeType.JOIN,
    NodeType.DENORMALIZE,
    NodeType.NORMALIZE,
    NodeType.AGGREGATE_ROW,
    NodeType.SORT_ROW,
    NodeType.FILTER_ROW,
    NodeType.FILTER_COLUMNS,
    NodeType.REPLACE,
    NodeType.REPLACE_LIST,
    NodeType.CONVERT_TYPE,
    NodeType.EXTRACT_DELIMITED_FIELDS,
    NodeType.EXTRACT_REGEX_FIELDS,
    NodeType.EXTRACT_JSON_FIELDS,
    NodeType.EXTRACT_XML_FIELD,
    NodeType.PARSE_RECORD_SET,
    NodeType.SPLIT_ROW,
    NodeType.PIVOT_TO_COLUMNS_DELIMITED,
    NodeType.UNPIVOT_ROW,
    NodeType.DENORMALIZE_SORTED_ROW,
    NodeType.UNIQ_ROW,
    NodeType.SAMPLE_ROW,
    NodeType.SCHEMA_COMPLIANCE_CHECK,
    NodeType.ADD_CRC_ROW,
    NodeType.ADD_CRC,
    NodeType.STANDARDIZE_ROW,
    NodeType.DATA_MASKING,
    NodeType.ASSERT,
    NodeType.FLOW_TO_ITERATE,
    NodeType.ITERATE_TO_FLOW,
    NodeType.REPLICATE,
    NodeType.UNITE,
    NodeType.FLOW_MERGE,
    NodeType.FLOW_METER,
    NodeType.FLOW_METER_CATCHER,
    NodeType.MATCH_GROUP,
    NodeType.ROW_GENERATOR,
    NodeType.NORMALIZE_NUMBER,
    NodeType.FILE_LOOKUP,
    NodeType.CACHE_IN,
    NodeType.CACHE_OUT,
    NodeType.RECORD_MATCHING,
    NodeType.MAP
  ];
  
  return processingTypes.includes(node.type as NodeType);
}

/**
 * Type guard for Input Node
 */
export function isInputNode(node: CanvasNode): boolean {
  return node.nodeType === 'input' || node.componentCategory === 'input';
}

/**
 * Type guard for Output Node
 */
export function isOutputNode(node: CanvasNode): boolean {
  return node.nodeType === 'output' || node.componentCategory === 'output';
}

/**
 * Type guard for Transform Node
 */
export function isTransformNode(node: CanvasNode): boolean {
  return node.nodeType === 'transform' || node.componentCategory === 'process';
}

// ==================== UTILITY TYPES ====================

/**
 * Partial type for node updates
 */
export type PartialCanvasNode = Partial<CanvasNode> & Pick<CanvasNode, 'id'>;

/**
 * Partial type for connection updates
 */
export type PartialCanvasConnection = Partial<CanvasConnection> & Pick<CanvasConnection, 'id'>;

/**
 * Node position update payload
 */
export interface NodePositionUpdate {
  nodeId: string;
  position: NodePosition;
}

/**
 * Connection validation request
 */
export interface ConnectionValidationRequest {
  sourceNodeId: string;
  sourcePortId: string;
  targetNodeId: string;
  targetPortId: string;
  validateSchema: boolean;
  validatePerformance: boolean;
}

/**
 * SQL generation request
 */
export interface SQLGenerationRequest {
  nodeIds: string[];
  includeDependencies: boolean;
  includeComments: boolean;
  format: boolean;
  targetDialect: 'POSTGRESQL' | 'MYSQL' | 'SQLSERVER' | 'ORACLE';
}




/**
 * Pipeline execution result
 */
export interface PipelineExecutionResult {
  pipelineId: string;
  executionId: string;
  status: 'SUCCESS' | 'PARTIAL_SUCCESS' | 'FAILED' | 'CANCELLED';
  startTime: string;
  endTime?: string;
  durationMs?: number;
  nodesExecuted: number;
  nodesSucceeded: number;
  nodesFailed: number;
  generatedSQL: GeneratedSQL[];
  errors?: Array<{
    nodeId: string;
    nodeName: string;
    error: string;
    timestamp: string;
  }>;
  warnings?: string[];
  statistics?: {
    totalRowsProcessed: number;
    totalBytesProcessed: number;
    averageThroughputRowsPerSecond: number;
  };
}

// ==================== HELPER FUNCTIONS ====================

/**
 * Get default port configuration for a node type
 */
export function getDefaultPorts(nodeType: NodeType): ConnectionPort[] {
  switch (nodeType) {
    case NodeType.INPUT:
      return [
        {
          id: 'output-1',
          type: PortType.OUTPUT,
          side: PortSide.RIGHT,
          position: 50,
          maxConnections: 10
        }
      ];
      
    case NodeType.OUTPUT:
      return [
        {
          id: 'input-1',
          type: PortType.INPUT,
          side: PortSide.LEFT,
          position: 50,
          maxConnections: 1
        }
      ];
      
    case NodeType.JOIN:
    case NodeType.MAP:
    case NodeType.MATCH_GROUP:
      return [
        {
          id: 'input-1',
          type: PortType.INPUT,
          side: PortSide.LEFT,
          position: 30,
          maxConnections: 1
        },
        {
          id: 'input-2',
          type: PortType.INPUT,
          side: PortSide.LEFT,
          position: 70,
          maxConnections: 1
        },
        {
          id: 'output-1',
          type: PortType.OUTPUT,
          side: PortSide.RIGHT,
          position: 50,
          maxConnections: 10
        }
      ];
      
    default:
      return [
        {
          id: 'input-1',
          type: PortType.INPUT,
          side: PortSide.LEFT,
          position: 50,
          maxConnections: 1
        },
        {
          id: 'output-1',
          type: PortType.OUTPUT,
          side: PortSide.RIGHT,
          position: 50,
          maxConnections: 10
        }
      ];
  }
}

/**
 * Get default node size for a node type
 */
export function getDefaultNodeSize(nodeType: NodeType): NodeSize {
  switch (nodeType) {
    case NodeType.INPUT:
    case NodeType.OUTPUT:
      return { width: 180, height: 100 };
      
    case NodeType.JOIN:
    case NodeType.MAP:
    case NodeType.MATCH_GROUP:
      return { width: 200, height: 120 };
      
    default:
      return { width: 160, height: 90 };
  }
}

/**
 * Create a new canvas node with defaults
 */
export function createCanvasNode(
  type: NodeType,
  name: string,
  position: NodePosition
): CanvasNode {
  return {
    id: `node-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
    name,
    type,
    nodeType: type === NodeType.INPUT ? 'input' : 
              type === NodeType.OUTPUT ? 'output' : 'transform',
    componentCategory: type === NodeType.INPUT ? 'input' : 
                      type === NodeType.OUTPUT ? 'output' : 'process',
    position,
    size: getDefaultNodeSize(type),
    connectionPorts: getDefaultPorts(type),
    status: NodeStatus.IDLE,
    draggable: true,
    droppable: false,
    metadata: {
      description: `${name} - ${type.replace('t', '')}`,
      version: '1.0.0',
      createdAt: new Date().toISOString()
    }
  };
}

/**
 * Create a connection between two nodes
 */
export function createCanvasConnection(
  sourceNodeId: string,
  sourcePortId: string,
  targetNodeId: string,
  targetPortId: string
): CanvasConnection {
  return {
    id: `conn-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
    sourceNodeId,
    sourcePortId,
    targetNodeId,
    targetPortId,
    dataFlow: {
      schemaMappings: []
    },
    status: ConnectionStatus.UNVALIDATED,
    metadata: {
      createdAt: new Date().toISOString()
    }
  };
}

// ==================== ADDITIONAL TYPES ====================

/**
 * Unified CanvasNode type that works for both repository and pipeline
 */
export interface UnifiedCanvasNode {
  id: string;
  name: string;
  type: string | NodeType; // Accept both string and NodeType enum
  nodeType?: 'input' | 'output' | 'transform' | 'process';
  componentType?: 'processing' | 'standardized' | 'palette-component' | 'sidebar-item';
  componentCategory?: 'input' | 'output' | 'process';
  position: NodePosition;
  size: NodeSize;
  connectionPorts?: ConnectionPort[];
  metadata?: NodeMetadata;
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
    icon?: string;
    borderColor?: string;
    backgroundColor?: string;
  };
}

/**
 * Unified CanvasConnection type
 */
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
  };
}

/**
 * SQL Generation Job
 */
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

/**
 * Pipeline Generation Options
 */
export interface PipelineGenerationOptions {
  includeComments: boolean;
  formatSQL: boolean;
  validateSQL: boolean;
  generateExecutionPlan: boolean;
  optimizeQueries: boolean;
  targetDialect: 'postgresql' | 'mysql' | 'sqlserver' | 'oracle';
  batchSize: number;
  timeout: number;
  connectionConfig?: {
    host: string;
    port: number;
    database: string;
    username: string;
    password?: string;
    sslMode: 'disable' | 'allow' | 'prefer' | 'require' | 'verify-ca' | 'verify-full';
  };
}

/**
 * Pipeline Generation Result
 */
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

/**
 * Helper function to convert string type to NodeType enum
 */
export function toNodeType(type: string): NodeType {
  // Check if the type is already a NodeType enum value
  if (Object.values(NodeType).includes(type as NodeType)) {
    return type as NodeType;
  }
  
  // Try to map common string types to NodeType enum
  const typeMap: Record<string, NodeType> = {
    'join': NodeType.JOIN,
    'input': NodeType.INPUT,
    'output': NodeType.OUTPUT,
    'transform': NodeType.TRANSFORM,
    'filter': NodeType.FILTER_ROW,
    'aggregate': NodeType.AGGREGATE_ROW,
    'sort': NodeType.SORT_ROW,
    'map': NodeType.MAP,
    'lookup': NodeType.LOOKUP,
  };
  
  return typeMap[type.toLowerCase()] || NodeType.UNKNOWN;
}

/**
 * Helper function to convert CanvasNode to UnifiedCanvasNode
 */
export function toUnifiedCanvasNode(node: any): UnifiedCanvasNode {
  return {
    id: node.id,
    name: node.name,
    type: typeof node.type === 'string' ? toNodeType(node.type) : node.type,
    nodeType: node.nodeType,
    componentType: node.componentType,
    componentCategory: node.componentCategory,
    position: node.position,
    size: node.size,
    connectionPorts: node.connectionPorts,
    metadata: node.metadata,
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