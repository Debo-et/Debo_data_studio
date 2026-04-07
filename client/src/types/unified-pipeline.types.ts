// src/types/unified-pipeline.types.ts
// ============================================================================
// MERGED TYPE SYSTEM FOR CANVAS + SQL GENERATION
// Combines metadata.ts (rich component configurations) and pipeline-types.ts
// ============================================================================

import React from 'react'; // ← Added for React.ReactNode
import { DataType } from './metadata';

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
  CACHE = 'CACHE',
  SURVIVORSHIP_RULE = 'SURVIVORSHIP_RULE',
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

// Add to src/types/unified-pipeline.types.ts

export interface ConvertRule {
  id: string;
  sourceColumn: string;
  sourceTable?: string;          // optional, if multiple input tables
  targetColumn: string;
  targetType: DataType | PostgreSQLDataType;
  parameters?: {
    length?: number;
    precision?: number;
    scale?: number;
    format?: string;
    defaultValue?: any;
    nullHandling?: 'KEEP_NULL' | 'DEFAULT' | 'FAIL';
    onError?: 'SKIP_ROW' | 'FAIL_JOB' | 'USE_DEFAULT' | 'SET_NULL';
    trim?: boolean;
    pad?: {
      direction: 'LEFT' | 'RIGHT';
      length: number;
      padChar: string;
    };
  };
  position: number;
}

export interface ConvertComponentConfiguration {
  version: string;
  rules: ConvertRule[];
  outputSchema: SchemaDefinition;
  sqlGeneration: {
    requiresCasting: boolean;
    usesConditionalLogic: boolean;
    estimatedRowMultiplier: number;
  };
  compilerMetadata: {
    lastModified: string;
    createdBy: string;
    ruleCount: number;
    validationStatus: 'VALID' | 'WARNING' | 'ERROR';
    warnings?: string[];
    dependencies: string[];
    compiledSql?: string;
  };
}
export interface ExtractDelimitedFieldsConfiguration {
  version: string;
  sourceColumn: string;               // The input column containing the delimited string
  delimiter: string;                   // Delimiter character(s) (e.g., ",", "\t", "|")
  quoteChar?: string;                   // Optional quote character for CSV style
  escapeChar?: string;                  // Optional escape character
  trimWhitespace: boolean;               // Trim whitespace from extracted values
  nullIfEmpty: boolean;                  // Treat empty string as NULL
  outputColumns: Array<{
    id: string;
    name: string;                        // Output column name
    type: DataType;                       // Data type (string, integer, date, etc.)
    length?: number;
    precision?: number;
    scale?: number;
    position: number;                     // Order (1‑based)
  }>;
  errorHandling: 'fail' | 'skip' | 'setNull';
  parallelization: boolean;
  batchSize?: number;
  sqlGeneration?: {
    // Placeholder for future SQL preview
    extractExpression?: string;
  };
  compilerMetadata?: {
    lastModified: string;
    // Other metadata
  };
}

// ==================== EXTRACT JSON FIELDS CONFIGURATION ====================
export interface ExtractJSONFieldsConfiguration {
  version: string;
  sourceColumn: string;               // The input column containing JSON
  jsonPath?: string;                   // Optional base JSONPath
  jsonType?: string; 
  outputColumns: Array<{
    id: string;
    name: string;                        // Output column name
    jsonPath: string;                    // JSONPath to extract value
    type: DataType;                       // Data type (STRING, INTEGER, DATE, etc.)
    length?: number;
    precision?: number;
    scale?: number;
    defaultValue?: string;
    nullable: boolean;
  }>;
  errorHandling: 'fail' | 'skip' | 'setNull';
  parallelization: boolean;
  batchSize?: number;
  sqlGeneration?: {
    extractExpression?: string;
  };
  compilerMetadata?: {
    lastModified: string;
    createdBy: string;
    ruleCount: number;
    validationStatus: 'VALID' | 'WARNING' | 'ERROR';
    warnings?: string[];
    dependencies: string[];
    compiledSql?: string;
  };
}
// ==================== EXTRACT XML FIELD CONFIGURATION ====================
export interface XPathExpression {
  id: string;
  outputColumn: string;                // Name of the output column
  xpath: string;                        // XPath expression
  dataType: DataType;                    // Target data type (STRING, INTEGER, DATE, etc.)
  length?: number;                       // For string types
  precision?: number;                    // For numeric types
  scale?: number;
  nullable: boolean;
  defaultValue?: string;
  position: number;                      // Order in output schema
}

export interface NamespaceMapping {
  prefix: string;                        // e.g., "ns"
  uri: string;                            // e.g., "http://example.com/ns"
}

export interface ExtractXMLFieldConfiguration {
  version: string;
  sourceColumn: string;                   // Name of the input XML column
  xpathExpressions: XPathExpression[];
  namespaceMappings: NamespaceMapping[];  // Optional prefix-to-URI mappings
  errorHandling: 'fail' | 'skipRow' | 'setNull';
  parallelization: boolean;
  batchSize?: number;                     // Rows per batch
  // SQL generation metadata (filled by compiler)
  sqlGeneration?: {
    extractExpressions: string[];          // Generated SQL for each column
  };
  compilerMetadata?: {
    lastModified: string;
    createdBy: string;
    ruleCount: number;
    validationStatus: 'VALID' | 'WARNING' | 'ERROR';
    warnings: string[];
    dependencies: string[];
    compiledSql?: string;
  };
}

export interface NormalizeComponentConfiguration {
  version: string;
  sourceColumn: string;               // the column to normalize
  delimiter: string;
  trimValues: boolean;
  treatEmptyAsNull: boolean;
  quoteChar?: string;
  escapeChar?: string;
  outputColumnName: string;            // name of the normalized value column
  addRowNumber: boolean;
  rowNumberColumnName?: string;        // e.g., "row_index"
  keepColumns: string[];               // list of input columns to propagate
  errorHandling: 'fail' | 'skip' | 'setNull';
  batchSize?: number;
  parallelization: boolean;
  // SQL generation metadata (filled by compiler)
  sqlGeneration?: {
    unnestExpression: string;
  };
  compilerMetadata?: {
    lastModified: string;
    createdBy: string;
    validationStatus: 'VALID' | 'WARNING' | 'ERROR';
    warnings?: string[];
  };
}

export interface UniqRowComponentConfiguration {
  version: string;
  /** Fields that define a unique row */
  keyFields: string[];
  /** Which occurrence to keep */
  keepStrategy: 'FIRST' | 'LAST';
  /** Whether NULLs in key fields are considered equal */
  treatNullsAsEqual: boolean;
  /** Optional sort order to determine first/last (if empty, natural order is used) */
  sortFields?: Array<{ field: string; direction: 'ASC' | 'DESC' }>;
  /** Whether to add a column with the duplicate count */
  outputDuplicateCount?: boolean;
  /** Name of the duplicate count column (if enabled) */
  duplicateCountColumnName?: string;
  /** SQL generation metadata (filled by compiler) */
  sqlGeneration?: {
    distinctClause: string;
    windowFunction?: string;
    orderByClause?: string;
  };
  compilerMetadata?: {
    lastModified: string;
    createdBy: string;
    validationStatus: 'VALID' | 'WARNING' | 'ERROR';
    warnings?: string[];
    dependencies: string[];
  };
}

// ==================== tFilterColumns Configuration ====================
export interface FilterColumn {
  id: string;                   // unique identifier
  originalName: string;         // name as it comes from input
  newName?: string;             // optional rename
  selected: boolean;            // whether to keep the column
  position: number;             // order in output schema
}

export interface FilterColumnsComponentConfiguration {
  version: string;
  columns: FilterColumn[];      // complete list (selected ones form output)
  options: {
    caseSensitive: boolean;     // when matching columns (if needed)
    keepAllByDefault: boolean;  // if true, all columns are initially selected
    errorOnMissingColumn: boolean;  // fail if an expected column is missing
  };
  outputSchema: SchemaDefinition;   // derived from selected columns
  sqlGeneration: {
    selectClause: string;       // generated SELECT clause
    estimatedRowMultiplier: number;
  };
  compilerMetadata: {
    lastModified: string;
    createdBy: string;
    validationStatus: 'VALID' | 'WARNING' | 'ERROR';
    warnings: string[];
    dependencies: string[];     // input columns used
  };
}


export interface FileLookupComponentConfiguration {
  version: string;
  file: {
    path: string;
    format: 'CSV' | 'EXCEL' | 'JSON' | 'PARQUET' | 'AVRO';
    options: Record<string, any>; // e.g., delimiter, sheet, header row
  };
  keyMappings: Array<{
    inputField: string;          // field from main flow
    fileColumn: string;           // column in lookup file
    operator?: '=' | '!=' | '<' | '>' | '<=' | '>=';
  }>;
  returnFields: Array<{
    fileColumn: string;
    outputName: string;            // name in output schema
    dataType?: DataType;            // optional override
  }>;
  cache: {
    enabled: boolean;
    size: number;                  // max entries
    ttlSeconds?: number;            // time-to-live (0 = infinite)
    type: 'LRU' | 'FIFO' | 'NONE';
  };
  fallback: {
    onMissing: 'NULL' | 'DEFAULT' | 'FAIL';
    defaultValue?: any;              // used if onMissing = 'DEFAULT'
  };
  errorHandling: 'FAIL' | 'SKIP_ROW' | 'LOG_CONTINUE';
  parallelization: {
    enabled: boolean;
    maxThreads: number;
    batchSize: number;
  };
  outputSchema: SchemaDefinition;   // derived from input + return fields
  sqlGeneration?: {
    joinExpression?: string;         // SQL fragment for lookup
  };
  compilerMetadata: {
    lastModified: string;
    validationStatus: 'VALID' | 'WARNING' | 'ERROR';
    warnings: string[];
  };
}

export interface ReplicateComponentConfiguration {
  version: string;
  /** Whether to add a branch identifier column */
  addBranchIdentifier?: boolean;
  /** Name of the branch identifier column (default "branch_id") */
  branchIdentifierColumnName?: string;
  /** Output schema (derived from input) */
  outputSchema: SchemaDefinition;
  /** SQL generation hints */
  sqlGeneration?: {
    passthrough: boolean;
    estimatedRowMultiplier: number;
  };
  /** Compiler metadata */
  compilerMetadata: {
    lastModified: string;
    createdBy: string;
    validationStatus: 'VALID' | 'WARNING' | 'ERROR';
    warnings?: string[];
  };
}

export interface UniteComponentConfiguration {
  version: string;
  /** Union mode: ALL (include duplicates) or DISTINCT (remove duplicates) */
  unionMode: 'ALL' | 'DISTINCT';
  /** Whether to add a column identifying the source flow */
  addSourceColumn: boolean;
  /** Name of the source column (required if addSourceColumn = true) */
  sourceColumnName?: string;
  /** Data type of the source column (default STRING) */
  sourceColumnType?: DataType;
  /** How to handle schema differences: 
   * - 'strict' – all input schemas must be identical
   * - 'flexible' – union of all columns (missing columns become NULL)
   */
  schemaHandling: 'strict' | 'flexible';
  /** Compiler metadata (filled automatically) */
  compilerMetadata?: {
    lastModified: string;
    createdBy: string;
    validationStatus: 'VALID' | 'WARNING' | 'ERROR';
    warnings?: string[];
    dependencies: string[];
  };
}

export interface UnpivotRowComponentConfiguration {
  version: string;
  /** Columns to keep as is (key columns) */
  keyColumns: string[];
  /** Columns to unpivot into rows */
  unpivotColumns: string[];
  /** Name of the output column that will hold the original column names */
  columnNameColumn: string;    // e.g. "attribute"
  /** Name of the output column that will hold the values */
  valueColumn: string;         // e.g. "value"
  /** Optional data type for the value column (if not specified, uses input type) */
  valueDataType?: DataType;
  /** Whether to include rows where the value is NULL */
  nullHandling: 'INCLUDE' | 'EXCLUDE';
  /** Generated output schema */
  outputSchema: SchemaDefinition;
  /** SQL generation hints */
  sqlGeneration: {
    requiresUnnest: boolean;
    estimatedRowMultiplier: number;
    parallelizable: boolean;
  };
  /** Compiler metadata */
  compilerMetadata: {
    lastModified: string;
    createdBy: string;
    keyCount: number;
    unpivotCount: number;
    validationStatus: 'VALID' | 'WARNING' | 'ERROR';
    warnings: string[];
    dependencies: string[];
    compiledSql?: string;
  };
}

export interface DenormalizeSortedRowComponentConfiguration {
  version: string;
  groupByFields: string[];
  sortKeys: Array<{
    field: string;
    direction: 'ASC' | 'DESC';
    nullsFirst: boolean;
    position: number;
  }>;
  denormalizedColumns: Array<{
    sourceField: string;
    outputField: string;
    aggregation: 'FIRST' | 'LAST' | 'ARRAY' | 'STRING_AGG' | 'JSON_AGG' | 'OBJECT_AGG' | 'SUM' | 'AVG' | 'MIN' | 'MAX';
    separator?: string;          // for STRING_AGG
    distinct?: boolean;           // optional, for ARRAY_AGG DISTINCT
  }>;
  outputSchema: SchemaDefinition;
  errorHandling: 'fail' | 'skip' | 'setNull';
  batchSize: number;
  parallelization: boolean;
  compilerMetadata: {
    lastModified: string;
    validationStatus: 'VALID' | 'WARNING' | 'ERROR';
    warnings: string[];
    dependencies: string[];
    estimatedOutputRows?: number;
    compiledSql?: string;
  };
}

export enum MatchType {
  EXACT = 'exact',
  FUZZY = 'fuzzy',
  EXACT_IGNORE_CASE = 'exact_ignore_case',
  SOUNDEX = 'soundex',
  METAPHONE = 'metaphone',
  LEVENSHTEIN = 'levenshtein',
  JARO_WINKLER = 'jaro_winkler',
}

export enum SurvivorshipRuleType {
  FIRST = 'first',
  LAST = 'last',
  MAX = 'max',
  MIN = 'min',
  SUM = 'sum',
  AVG = 'avg',
  CONCAT = 'concat',
  MOST_FREQUENT = 'most_frequent',
  ANY_NON_NULL = 'any_non_null',
  COALESCE = 'coalesce',
}

export interface MatchKey {
  id: string;
  field: string;                     // input field name
  matchType: MatchType;
  threshold?: number;                 // for fuzzy (0.0-1.0)
  caseSensitive: boolean;
  ignoreNull: boolean;
  weight?: number;                    // relative importance
  blockingKey?: boolean;               // use for blocking (performance)
}

export interface SurvivorshipRule {
  id: string;
  field: string;                      // output field name
  ruleType: SurvivorshipRuleType;
  params?: {
    separator?: string;                // for CONCAT
    orderBy?: string;                  // field to determine FIRST/LAST
    orderDirection?: 'ASC' | 'DESC';
    defaultValue?: any;
  };
  sourceField?: string;                // if output field name differs from input
}

export interface MatchGroupComponentConfiguration {
  version: string;
  matchKeys: MatchKey[];
  survivorshipRules: SurvivorshipRule[];
  outputFields: string[];               // subset of fields to include (if empty, all input fields with survivorship)
  globalOptions: {
    matchThreshold?: number;              // overall confidence threshold (0-1)
    maxMatchesPerRecord?: number;         // limit matches
    nullHandling: 'match' | 'no_match' | 'ignore';
    outputMode: 'all_matches' | 'best_match' | 'groups_only';
    includeMatchDetails: boolean;          // add column with match info
    parallelization: boolean;
    batchSize: number;
  };
  sqlGeneration?: {
    groupByClause?: string;
    matchExpression?: string;
    survivorshipExpressions?: Record<string, string>;
  };
  compilerMetadata: {
    lastModified: string;
    createdBy: string;
    matchKeyCount: number;
    ruleCount: number;
    validationStatus: 'VALID' | 'WARNING' | 'ERROR';
    warnings?: string[];
    dependencies: string[];
    compiledSql?: string;
  };
}
export interface ExtractRegexRule {
  id: string;
  groupIndex: number;          // 1-based capturing group index
  columnName: string;
  dataType: DataType;           // e.g., STRING, INTEGER, DECIMAL, DATE, BOOLEAN
  length?: number;              // for string types
  precision?: number;           // for decimal
  scale?: number;               // for decimal
  nullable: boolean;
  defaultValue?: string;
  position: number;             // order in output schema
}

export interface ExtractRegexFieldsConfiguration {
  version: string;
  sourceColumn: string;          // name of input column
  regexPattern: string;
  caseInsensitive: boolean;
  multiline: boolean;
  dotAll: boolean;
  rules: ExtractRegexRule[];     // one per capturing group
  errorHandling: {
    onNoMatch: 'fail' | 'skipRow' | 'useDefault';
    onConversionError: 'fail' | 'skipRow' | 'setNull';
  };
  parallelization: boolean;
  batchSize?: number;
  outputSchema: SchemaDefinition; // derived from rules
  sqlGeneration?: {
    canPushDown: boolean;        // whether regex can be executed in DB (depends on dialect)
    estimatedRowMultiplier: number;
  };
  compilerMetadata: {
    lastModified: string;
    createdBy: string;
    validationStatus: 'VALID' | 'WARNING' | 'ERROR';
    warnings: string[];
    dependencies: string[];      // input columns used
    compiledSql?: string;
  };
}


// src/types/unified-pipeline.types.ts

export interface ParseRecordSetColumn {
  id: string;
  name: string;                // output column name
  type: DataType;               // e.g., STRING, INTEGER, DATE
  length?: number;
  precision?: number;
  scale?: number;
  nullable: boolean;
  defaultValue?: string;
  position: number;             // order in the output schema
  // optionally, a field index (1‑based) if the source has no header
  fieldIndex?: number;
}

export interface ParseRecordSetComponentConfiguration {
  version: string;

  // Input column selection
  sourceColumn: string;          // name of the column containing the record set

  // Delimiter settings
  recordDelimiter: string;        // e.g., "\n", "|", "~"
  fieldDelimiter: string;          // e.g., ",", "\t"
  quoteChar?: string;              // optional quote character for quoted fields
  escapeChar?: string;             // optional escape character

  // Header handling
  hasHeader: boolean;              // whether the first record contains column names
  // If hasHeader = false, the user must define columns manually (with fieldIndex)
  // If hasHeader = true, the column names are taken from the first record,
  // but the user can still override types, etc.

  // Data cleaning
  trimWhitespace: boolean;         // trim values after splitting
  nullIfEmpty: boolean;            // treat empty strings as NULL

  // Output column definitions (if hasHeader = false, these are required)
  columns: ParseRecordSetColumn[];

  // Error handling & performance
  errorHandling: 'fail' | 'skipRow' | 'setNull';
  parallelization: boolean;
  batchSize?: number;

  // Generated output schema (filled by the system)
  outputSchema: SchemaDefinition;

  // SQL generation metadata (filled by compiler)
  sqlGeneration?: {
    unnestExpression?: string;      // PostgreSQL UNNEST with regexp_split_to_table
    estimatedRowMultiplier: number;
  };

  // Compiler metadata
  compilerMetadata: {
    lastModified: string;
    createdBy: string;
    validationStatus: 'VALID' | 'WARNING' | 'ERROR';
    warnings: string[];
    dependencies: string[];          // input columns used
    compiledSql?: string;
  };
}
export interface NormalizeNumberRule {
  id: string;
  sourceColumn: string;
  targetColumn: string;               // output column name (may be same as source)
  method: 'minmax' | 'zscore' | 'decimalscaling' | 'log' | 'robust' | 'round' | 'custom';
  parameters?: {
    // Min‑Max
    min?: number;
    max?: number;
    // Logarithm
    logBase?: 'e' | '10';
    // Round / Ceil / Floor
    decimalPlaces?: number;
    roundingMode?: 'round' | 'ceil' | 'floor';
    // Custom expression
    expression?: string;               // SQL expression using {column} placeholders
  };
  nullHandling: 'KEEP_NULL' | 'DEFAULT_VALUE' | 'ERROR';
  defaultValue?: number | string;
  outlierHandling?: 'CLIP' | 'REMOVE' | 'NONE';
  outputDataType: PostgreSQLDataType;  // target PostgreSQL data type
  position: number;                    // order in output schema
}

export interface NormalizeNumberComponentConfiguration {
  version: string;
  rules: NormalizeNumberRule[];
  globalOptions?: {
    nullHandling?: 'KEEP_NULL' | 'DEFAULT_VALUE' | 'ERROR';
    outlierHandling?: 'CLIP' | 'REMOVE' | 'NONE';
    defaultDataType?: PostgreSQLDataType;
  };
  outputSchema: SchemaDefinition;
  sqlGeneration: {
    requiresCustomExpression: boolean;
    estimatedRowMultiplier: number;
    parallelizable: boolean;
  };
  compilerMetadata: {
    lastModified: string;
    createdBy: string;
    ruleCount: number;
    validationStatus: 'VALID' | 'WARNING' | 'ERROR';
    warnings?: string[];
    dependencies: string[];
    compiledSql?: string;
  };
}

export interface DenormalizeComponentConfiguration {
  version: string;
  sourceColumn: string;
  delimiter: string;
  trimValues: boolean;
  treatEmptyAsNull: boolean;
  quoteChar?: string;
  escapeChar?: string;
  outputColumnName: string;
  addRowNumber: boolean;
  rowNumberColumnName?: string;
  keepColumns: string[];
  errorHandling: 'fail' | 'skip' | 'setNull';
  batchSize?: number;
  parallelization: boolean;
  sqlGeneration?: {
    unnestExpression: string;
  };
  compilerMetadata?: {
    lastModified: string;
    createdBy: string;
    validationStatus: 'VALID' | 'WARNING' | 'ERROR';
    warnings?: string[];
  };
}

export interface SampleRowComponentConfiguration {
  version: string;
  samplingMethod: 'firstRows' | 'percentage' | 'random';
  sampleValue: number;          // e.g., 10 rows or 25 (for percentage)
  randomSeed?: number;          // optional, for reproducibility
  // Optional advanced settings
  ignoreEmptyRows?: boolean;
  includeHeader?: boolean;
  // Output schema derived from input
  outputSchema: SchemaDefinition;
  // SQL generation hints
  sqlGeneration?: {
    limitClause?: string;       // e.g., "LIMIT 10"
    sampleClause?: string;      // e.g., "TABLESAMPLE SYSTEM(10)"
    estimatedRowMultiplier: number;
  };
  compilerMetadata: {
    lastModified: string;
    createdBy: string;
    validationStatus: 'VALID' | 'WARNING' | 'ERROR';
    warnings?: string[];
    dependencies: string[];
    compiledSql?: string;
  };
}

// ----------------------------------------------------------------------
// tSchemaComplianceCheck Configuration
// ----------------------------------------------------------------------

export interface ComplianceValidationRule {
  id: string;                     // unique identifier
  type: 'type' | 'null' | 'pattern' | 'expression' | 'range' | 'custom';
  params?: Record<string, any>;
  errorMessage?: string;          // custom error message if rule fails
}

export interface ExpectedColumn {
  id: string;
  name: string;                   // expected column name
  dataType: DataType;             // expected PostgreSQL data type
  nullable: boolean;              // if false, presence of NULL causes failure
  length?: number;                // for string types
  precision?: number;             // for numeric/decimal
  scale?: number;
  // additional validation rules (optional)
  validationRules?: ComplianceValidationRule[];
  // custom SQL expression to validate the column (e.g., "age > 0 AND age < 120")
  expression?: string;
  // whether this column is required to exist in the input
  required: boolean;
  // default value to use if column is missing (and required=false)
  defaultValue?: string;
}

export interface SchemaComplianceCheckConfiguration {
  version: string;

  // Expected schema definition
  expectedSchema: ExpectedColumn[];

  // Validation mode
  mode: 'strict' | 'lenient';
  // strict: all columns must exactly match the expected schema (no extra columns)
  // lenient: extra columns are ignored, missing columns use default if provided

  // Action when a row fails validation
  errorHandling: 'fail' | 'skipRow' | 'rejectFlow' | 'markInvalid';
  // fail          : stop the job
  // skipRow       : discard the row (no output)
  // rejectFlow    : send invalid rows to a separate reject output
  // markInvalid   : add a boolean column (e.g., "_valid") to indicate compliance

  // If errorHandling = 'rejectFlow', define the reject output schema
  rejectOutput?: {
    enabled: boolean;
    schema: SchemaDefinition;     // will be auto‑generated from input + error columns
    addErrorDetails: boolean;     // add columns like "_error_messages", "_failed_rules"
  };

  // Output schema (for valid rows). If not specified, it's the input schema.
  outputSchema?: SchemaDefinition;

  // Additional options
  options?: {
    caseSensitiveColumnNames?: boolean;   // default false
    trimWhitespace?: boolean;              // trim values before validation
    nullIfEmptyString?: boolean;           // treat "" as NULL
    continueOnFirstError?: boolean;        // stop validation after first failure per row
    maxErrorsPerRow?: number;              // limit error collection
  };

  // SQL generation hints (filled by compiler)
  sqlGeneration?: {
    whereClause?: string;                 // generated filter for valid rows
    rejectWhereClause?: string;           // generated filter for reject rows
    estimatedRowMultiplier: number;
  };

  compilerMetadata: {
    lastModified: string;
    createdBy: string;
    validationStatus: 'VALID' | 'WARNING' | 'ERROR';
    warnings?: string[];
    dependencies: string[];
    compiledSql?: string;
  };
}


export interface AddCRCRowComponentConfiguration {
  version: string;                     // e.g., "1.0"

  /** Which columns to include in CRC calculation (empty = all columns) */
  includedColumns: string[];

  /** CRC algorithm to use */
  algorithm: 'CRC32' | 'CRC16' | 'CRC8';

  /** Name of the output column for CRC value (e.g., "crc") */
  outputColumnName: string;

  /** How to handle NULL values in input columns */
  nullHandling: 'SKIP_ROW' | 'USE_DEFAULT' | 'TREAT_AS_EMPTY';

  /** Default value to use if nullHandling = USE_DEFAULT (applies per column) */
  defaultValue?: string;

  /** Character encoding for string conversion (default UTF-8) */
  characterEncoding?: string;

  /** Whether to compute CRC on the entire row (concatenated values) */
  computeOnWholeRow?: boolean;

  /** Optional separator to use when concatenating columns for whole row */
  columnSeparator?: string;

  /** Output schema (input columns + new CRC column) */
  outputSchema: SchemaDefinition;

  /** SQL generation hints (filled by compiler) */
  sqlGeneration?: {
    canPushDown: boolean;
    requiresExpression: boolean;
  };

  /** Compiler metadata */
  compilerMetadata: {
    lastModified: string;
    createdBy: string;
    validationStatus: 'VALID' | 'WARNING' | 'ERROR';
    warnings?: string[];
    dependencies: string[];
    compiledSql?: string;
  };
}

export interface DataMaskingRule {
  id: string;
  column: string;
  maskingType: 'REPLACE' | 'RANDOM' | 'NULLIFY' | 'HASH' | 'EMAIL' | 'CREDIT_CARD' | 'PHONE' | 'SSN' | 'CUSTOM';
  parameters?: {
    replaceValue?: string;          // for REPLACE
    randomType?: 'STRING' | 'NUMBER' | 'UUID';
    randomLength?: number;
    hashAlgorithm?: 'MD5' | 'SHA1' | 'SHA256' | 'SHA512';
    customExpression?: string;      // SQL expression for CUSTOM
  };
  position: number;
}

export interface DataMaskingComponentConfiguration {
  version: string;
  rules: DataMaskingRule[];
  outputSchema: SchemaDefinition;
  sqlGeneration?: {
    selectExpressions: string[];
    estimatedRowMultiplier: number;
  };
  compilerMetadata: {
    lastModified: string;
    createdBy: string;
    ruleCount: number;
    validationStatus: 'VALID' | 'WARNING' | 'ERROR';
    warnings?: string[];
    dependencies: string[];
    compiledSql?: string;
  };
}

// Update ComponentConfiguration union:
export type ComponentConfiguration =
  | { type: 'MAP'; config: MapComponentConfiguration }
  | { type: 'JOIN'; config: JoinComponentConfiguration }
  | { type: 'FILTER'; config: FilterComponentConfiguration }
  | { type: 'LOOKUP'; config: LookupComponentConfiguration }
  | { type: 'AGGREGATE'; config: AggregateComponentConfiguration }
  | { type: 'SORT'; config: SortComponentConfiguration }
  | { type: 'INPUT'; config: InputComponentConfiguration }
  | { type: 'OUTPUT'; config: OutputComponentConfiguration }
  | { type: 'CONVERT'; config: ConvertComponentConfiguration }
  | { type: 'REPLACE'; config: ReplaceComponentConfiguration }
  | { type: 'REPLACE_LIST'; config: ReplaceComponentConfiguration }              // ✅ added
  | { type: 'FILTER_COLUMNS'; config: FilterColumnsComponentConfiguration }      // ✅ added
  | { type: 'PIVOT_TO_COLUMNS_DELIMITED'; config: PivotToColumnsDelimitedConfiguration } // ✅ added
  | { type: 'DENORMALIZE'; config: DenormalizeComponentConfiguration }           // ✅ added
  | { type: 'OTHER'; config: Record<string, any> }
  | { type: 'EXTRACT_DELIMITED'; config: ExtractDelimitedFieldsConfiguration }
  | { type: 'EXTRACT_JSON_FIELDS'; config: ExtractJSONFieldsConfiguration }
  | { type: 'EXTRACT_XML_FIELD'; config: ExtractXMLFieldConfiguration }
  | { type: 'NORMALIZE'; config: NormalizeComponentConfiguration }
  | { type: 'FILE_LOOKUP'; config: FileLookupComponentConfiguration }
  | { type: 'REPLICATE'; config: ReplicateComponentConfiguration }
  | { type: 'UNITE'; config: UniteComponentConfiguration }
  | { type: 'UNIQ_ROW'; config: UniqRowComponentConfiguration }
  | { type: 'UNPIVOT_ROW'; config: UnpivotRowComponentConfiguration }
  | { type: 'MATCH_GROUP'; config: MatchGroupComponentConfiguration }
  | { type: 'EXTRACT_REGEX_FIELDS'; config: ExtractRegexFieldsConfiguration }
  | { type: 'DENORMALIZE_SORTED_ROW'; config: DenormalizeSortedRowComponentConfiguration }
  | { type: 'SAMPLE_ROW'; config: SampleRowComponentConfiguration }
  | { type: 'NORMALIZE_NUMBER'; config: NormalizeNumberComponentConfiguration }
  | { type: 'ADD_CRC_ROW'; config: AddCRCRowComponentConfiguration }
  | { type: 'DATA_MASKING'; config: DataMaskingComponentConfiguration }
  | { type: 'SCHEMA_COMPLIANCE_CHECK'; config: SchemaComplianceCheckConfiguration };// ✅ only one

// Type guard
export function isReplaceConfig(config: ComponentConfiguration): config is { type: 'REPLACE'; config: ReplaceComponentConfiguration } {
  return config.type === 'REPLACE';
}

export function isExtractDelimitedConfig(
  config: ComponentConfiguration
): config is { type: 'EXTRACT_DELIMITED'; config: ExtractDelimitedFieldsConfiguration } {
  return config.type === 'EXTRACT_DELIMITED';
}

export function isExtractXMLFieldConfig(config: ComponentConfiguration): config is { type: 'EXTRACT_XML_FIELD'; config: ExtractXMLFieldConfiguration } {
  return config.type === 'EXTRACT_XML_FIELD';
}

export function isNormalizeConfig(config: ComponentConfiguration): config is { type: 'NORMALIZE'; config: NormalizeComponentConfiguration } {
  return config.type === 'NORMALIZE';
}

export function isUniqRowConfig(config: ComponentConfiguration): config is { type: 'UNIQ_ROW'; config: UniqRowComponentConfiguration } {
  return config.type === 'UNIQ_ROW';
}


export function isFileLookupConfig(
  config: ComponentConfiguration
): config is { type: 'FILE_LOOKUP'; config: FileLookupComponentConfiguration } {
  return config.type === 'FILE_LOOKUP';
}

export function isReplicateConfig(
  config: ComponentConfiguration
): config is { type: 'REPLICATE'; config: ReplicateComponentConfiguration } {
  return config.type === 'REPLICATE';
}

export function isUniteConfig(config: ComponentConfiguration): config is { type: 'UNITE'; config: UniteComponentConfiguration } {
  return config.type === 'UNITE';
}

export function isUnpivotRowConfig(config: ComponentConfiguration): config is { type: 'UNPIVOT_ROW'; config: UnpivotRowComponentConfiguration } {
  return config.type === 'UNPIVOT_ROW';
}

export function isDenormalizeSortedRowConfig(config: ComponentConfiguration): config is { type: 'DENORMALIZE_SORTED_ROW'; config: DenormalizeSortedRowComponentConfiguration } {
  return config.type === 'DENORMALIZE_SORTED_ROW';
}

export function isMatchGroupConfig(config: ComponentConfiguration): config is { type: 'MATCH_GROUP'; config: MatchGroupComponentConfiguration } {
  return config.type === 'MATCH_GROUP';
}


export function isNormalizeNumberConfig(
  config: ComponentConfiguration
): config is { type: 'NORMALIZE_NUMBER'; config: NormalizeNumberComponentConfiguration } {
  return config.type === 'NORMALIZE_NUMBER';
}

export function isSampleRowConfig(
  config: ComponentConfiguration
): config is { type: 'SAMPLE_ROW'; config: SampleRowComponentConfiguration } {
  return config.type === 'SAMPLE_ROW';
}

export function isSchemaComplianceCheckConfig(
  config: ComponentConfiguration
): config is { type: 'SCHEMA_COMPLIANCE_CHECK'; config: SchemaComplianceCheckConfiguration } {
  return config.type === 'SCHEMA_COMPLIANCE_CHECK';
}

export function isAddCRCRowConfig(
  config: ComponentConfiguration
): config is { type: 'ADD_CRC_ROW'; config: AddCRCRowComponentConfiguration } {
  return config.type === 'ADD_CRC_ROW';
}

export function isDataMaskingConfig(config: ComponentConfiguration): config is { type: 'DATA_MASKING'; config: DataMaskingComponentConfiguration } {
  return config.type === 'DATA_MASKING';
}
export interface PivotToColumnsDelimitedConfiguration {
  version: string;                      // e.g., "1.0"

  // Input column selection
  sourceColumn: string;                  // Column containing delimited data

  // Delimiter settings
  delimiter: string;                      // Between pairs (default: ",")
  keyValueSeparator: string;              // Between key and value (default: ":")

  // Column generation strategy
  columnGeneration: 'fromKeys' | 'fixedList';
  fixedColumns?: string[];                 // Required if columnGeneration = 'fixedList'

  // Missing key handling
  missingKeyHandling: 'omit' | 'null' | 'default';
  defaultValue?: string;                   // Used if missingKeyHandling = 'default'

  // Value type conversion (applied to all pivoted values)
  valueType: 'string' | 'integer' | 'decimal' | 'date' | 'boolean';

  // Optional column prefix
  columnPrefix?: string;                    // e.g., "pivot_" → pivot_name, pivot_age

  // Data cleaning options
  trimWhitespace: boolean;                  // Trim keys and values
  caseSensitiveKeys: boolean;               // Treat "Name" and "name" as different

  // Advanced execution options
  errorHandling: 'fail' | 'skip' | 'setNull';
  parallelization: boolean;
  batchSize?: number;                        // Rows per batch

  // Compiler metadata (filled by the system)
  compilerMetadata?: {
    lastModified: string;
    validationStatus: 'VALID' | 'WARNING' | 'ERROR';
    warnings?: string[];
  };
}

export interface ReplaceRule {
  id: string;
  column: string;
  searchValue: string;
  replacement: string;
  caseSensitive: boolean;
  regex: boolean;
  scope: 'all' | 'first' | 'last';
  position: number;
}

export interface ReplaceComponentConfiguration {
  version: string;
  rules: ReplaceRule[];
  globalOptions?: {
    errorHandling?: 'fail' | 'skip' | 'default';
    emptyValueHandling?: 'skip' | 'default' | 'null';
    parallelization?: boolean;
    maxThreads?: number;
    batchSize?: number;
  };
  outputSchema: SchemaDefinition;
  sqlGeneration: {
    requiresRegex?: boolean;
    estimatedRowMultiplier?: number;
  };
  compilerMetadata: {
    lastModified: string;
    createdBy: string;
    ruleCount: number;
    validationStatus: 'VALID' | 'WARNING' | 'ERROR';
    warnings?: string[];
    dependencies: string[];
    compiledSql?: string;
  };
}

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

export type { DataType };

