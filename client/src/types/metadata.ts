// src/types/metadata.ts - COMPLETE METADATA DEFINITION FOR SQL COMPILER

// ==================== CORE TYPES ====================
export type ComponentRole = 'INPUT' | 'TRANSFORM' | 'OUTPUT';
export type RelationType = 'FLOW' | 'JOIN' | 'LOOKUP' | 'FILTER' | 'ITERATE' | 'MAPPING' | 'SPLIT';
export type DataType = 'STRING' | 'INTEGER' | 'DECIMAL' | 'BOOLEAN' | 'DATE' | 'TIMESTAMP' | 'BINARY';
export type NodeStatus = 'default' | 'success' | 'error' | 'warning';

// ==================== FIELD AND SCHEMA DEFINITIONS ====================
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
  // Enhanced for SQL generation
  originalName?: string; // Original field name before transformation
  transformation?: string; // SQL expression that generated this field
  metadata?: Record<string, any>; // Additional field-level metadata
}

export interface SchemaDefinition {
  id: string;
  name: string;
  alias?: string; // SQL alias
  fields: FieldSchema[];
  sourceComponentId?: string;
  // SQL generation metadata
  isTemporary: boolean;
  isMaterialized: boolean;
  rowCount?: number;
  metadata?: Record<string, any>;
}

// ==================== COMPONENT-SPECIFIC CONFIGURATIONS ====================

// 1. MAP COMPONENT (tMap) CONFIGURATION
export interface MapTransformation {
  id: string;
  sourceField: string;
  sourceTable?: string; // For joins/lookups
  targetField: string;
  expression: string; // SQL expression (e.g., "UPPER(source_field)", "source_1 + source_2")
  expressionType: 'SQL' | 'FUNCTION' | 'CONSTANT' | 'REFERENCE' | 'VARIABLE';
  dataType: DataType;
  isDirectMapping: boolean;
  validationRules?: string[];
  position: number; // For deterministic ordering
  // Additional transformation context
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

// 1. MAP COMPONENT (tMap) CONFIGURATION
export interface MapComponentConfiguration {
  version: string; // Schema version for compatibility
  // Core transformations
  transformations: MapTransformation[];
  joins?: MapJoinCondition[];
  lookups?: MapLookupConfig[];
  filters?: MapFilterCondition[];
  variables?: MapVariable[];
  
  // Output schema definition - Use SchemaDefinition with persistenceLevel
  outputSchema: SchemaDefinition & {
    persistenceLevel: 'MEMORY' | 'TEMPORARY' | 'PERSISTENT';
  };
  
  // SQL generation metadata
  sqlGeneration: {
    requiresDistinct: boolean;
    requiresAggregation: boolean;
    requiresWindowFunction: boolean;
    requiresSubquery: boolean;
    estimatedRowMultiplier: number;
    joinOptimizationHint?: string;
    // Execution hints
    parallelizable: boolean;
    batchSize?: number;
    memoryHint?: 'LOW' | 'MEDIUM' | 'HIGH';
  };
  
  // Compiler metadata
  compilerMetadata: {
    lastModified: string;
    createdBy: string;
    mappingCount: number;
    validationStatus: 'VALID' | 'WARNING' | 'ERROR';
    warnings?: string[];
    dependencies: string[]; // Dependent node IDs
    columnDependencies: Record<string, string[]>; // target column -> source columns
    // Cache for compiled SQL
    compiledSql?: string;
    compilationTimestamp?: string;
  };
}

// 2. JOIN COMPONENT (tJoin) CONFIGURATION
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
  
  // Join hints and optimization
  joinHints: {
    enableJoinHint: boolean;
    joinHint?: string; // e.g., "HASH", "MERGE", "LOOP"
    maxParallelism?: number;
    memoryGrant?: number; // KB
  };
  
  // Output schema
  outputSchema: {
    fields: FieldSchema[];
    deduplicateFields: boolean; // Whether to prefix duplicate field names
    fieldAliases: Record<string, string>; // field -> alias mapping
  };
  
  // SQL generation
  sqlGeneration: {
    joinAlgorithm: 'HASH' | 'MERGE' | 'NESTED_LOOP';
    estimatedJoinCardinality: number;
    nullHandling: 'INCLUDE' | 'EXCLUDE' | 'TREAT_AS_FALSE';
    // Performance hints
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

// 3. FILTER COMPONENT (tFilterRow) CONFIGURATION
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
  filterLogic: 'AND' | 'OR' | string; // Can be complex like "(A AND B) OR C"
  
  // Filter optimization
  optimization: {
    pushDown: boolean; // Can filter be pushed down to source
    indexable: boolean; // Can use index
    estimatedSelectivity: number; // 0-1, estimated rows that pass
  };
  
  // SQL generation
  sqlGeneration: {
    whereClause: string; // Pre-computed WHERE clause
    parameterized: boolean;
    requiresSubquery: boolean;
    canUseIndex: boolean;
  };
  
  compilerMetadata: {
    lastModified: string;
    estimatedRowReduction: number; // Percentage of rows filtered out
    warnings?: string[];
  };
}

// 4. LOOKUP COMPONENT (tLookup) CONFIGURATION
export interface LookupComponentConfiguration {
  version: string;
  lookupType: 'SIMPLE' | 'RANGE' | 'MULTIPLE' | 'FUZZY';
  lookupKeyFields: string[];
  lookupReturnFields: string[];
  lookupTable: string;
  
  // Cache configuration
  cache: {
    enabled: boolean;
    cacheSize: number;
    cacheType: 'LRU' | 'FIFO' | 'TTL';
    ttlSeconds?: number;
  };
  
  // Fallback behavior
  fallback: {
    failOnMissing: boolean;
    defaultValue?: string;
    defaultValueStrategy: 'NULL' | 'DEFAULT' | 'ERROR';
  };
  
  // Output schema
  outputSchema: {
    fields: FieldSchema[];
    prefixLookupFields: boolean; // Whether to prefix with lookup table name
  };
  
  // SQL generation
  sqlGeneration: {
    joinType: 'LEFT' | 'INNER'; // Lookups typically use LEFT JOIN
    requiresDistinct: boolean;
    estimatedCacheHitRate: number;
    // Performance
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

// 5. AGGREGATE COMPONENT (tAggregateRow) CONFIGURATION
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
  
  // Having clause
  havingConditions?: Array<{
    id: string;
    field: string;
    operator: '=' | '!=' | '<' | '>' | '<=' | '>=';
    value: string | number;
  }>;
  
  // Optimization
  optimization: {
    canUseIndex: boolean;
    requiresSort: boolean;
    estimatedGroupCount: number;
    memoryHint?: 'LOW' | 'MEDIUM' | 'HIGH';
  };
  
  // Output schema
  outputSchema: {
    fields: FieldSchema[];
    groupByFields: string[];
    aggregateFields: string[];
  };
  
  // SQL generation
  sqlGeneration: {
    groupByClause: string;
    aggregateClause: string;
    havingClause?: string;
    requiresWindowFunction: boolean;
    // Performance
    parallelizable: boolean;
    sortRequired: boolean;
  };
  
  compilerMetadata: {
    lastModified: string;
    estimatedCardinality: number;
    warnings?: string[];
  };
}

// 6. SORT COMPONENT (tSortRow) CONFIGURATION
export interface SortComponentConfiguration {
  version: string;
  sortFields: Array<{
    field: string;
    direction: 'ASC' | 'DESC';
    nullsFirst: boolean;
    position: number;
  }>;
  
  // Performance hints
  performance: {
    estimatedRowCount: number;
    memoryRequired?: number; // KB
    canParallelize: boolean;
  };
  
  // SQL generation
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

// 7. INPUT COMPONENT CONFIGURATION
export interface InputComponentConfiguration {
  version: string;
  sourceType: 'DATABASE' | 'FILE' | 'API' | 'STREAM';
  sourceDetails: {
    connectionString?: string;
    tableName?: string;
    filePath?: string;
    format?: 'CSV' | 'JSON' | 'PARQUET' | 'AVRO';
    encoding?: string;
    delimiter?: string;
    hasHeader?: boolean;
  };
  
  // Query/filter pushdown
  pushdown: {
    enabled: boolean;
    filterClause?: string;
    columnSelection?: string[];
    limit?: number;
  };
  
  // Schema definition
  schema: SchemaDefinition;
  
  // SQL generation
  sqlGeneration: {
    fromClause: string;
    alias: string;
    isTemporary: boolean;
    // Performance
    estimatedRowCount: number;
    parallelizable: boolean;
  };
  
  compilerMetadata: {
    lastModified: string;
    sourceValidated: boolean;
    warnings?: string[];
  };
}

// 8. OUTPUT COMPONENT CONFIGURATION
export interface OutputComponentConfiguration {
  version: string;
  targetType: 'DATABASE' | 'FILE' | 'API' | 'STREAM';
  targetDetails: {
    connectionString?: string;
    tableName: string;
    filePath?: string;
    format?: 'CSV' | 'JSON' | 'PARQUET' | 'AVRO';
    mode: 'APPEND' | 'OVERWRITE' | 'ERROR_IF_EXISTS' | 'IGNORE';
  };
  
  // Write options
  writeOptions: {
    batchSize: number;
    commitInterval?: number;
    truncateFirst: boolean;
    createTable: boolean;
  };
  
  // Schema mapping
  schemaMapping: Array<{
    sourceField: string;
    targetField: string;
    transformation?: string;
  }>;
  
  // SQL generation
  sqlGeneration: {
    insertStatement: string;
    mergeStatement?: string;
    requiresTransaction: boolean;
    // Performance
    parallelizable: boolean;
    batchOptimized: boolean;
  };
  
  compilerMetadata: {
    lastModified: string;
    targetValidated: boolean;
    warnings?: string[];
  };
}

// ==================== UNIFIED COMPONENT CONFIGURATION ====================
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

// ==================== RELATION CONFIGURATIONS ====================
export interface JoinCondition {
  id: string;
  leftField: string;
  rightField: string;
  operator: '=' | '!=' | '<' | '>' | '<=' | '>=' | 'LIKE' | 'IN';
  value?: string | number;
  isActive: boolean;
}

export interface FilterCondition {
  id: string;
  field: string;
  operator: '=' | '!=' | '<' | '>' | '<=' | '>=' | 'LIKE' | 'IN' | 'IS_NULL' | 'NOT_NULL';
  value?: string | number | boolean;
  valueType?: 'CONSTANT' | 'FIELD' | 'PARAMETER';
  isActive: boolean;
  logicGroup?: number;
}

export interface FieldMapping {
  id: string;
  sourceField: string;
  targetField: string;
  transformation?: string;
  defaultValue?: string;
  isRequired: boolean;
}

// ==================== NODE METADATA ====================
export interface FlowNodeMeta {
  // Component identification
  componentKey: string;
  componentType: ComponentRole;
  instanceNumber: number;
  
  // UI Display
  label: string;
  status: NodeStatus;
  
  // Unified component configuration
  configuration: ComponentConfiguration;
  
  // Schema information - PRESERVES ORIGINAL AND TRANSFORMED SCHEMAS
  schemas: {
    // Original schemas from sources
    input?: SchemaDefinition[];
    // Current output schema (may be transformed)
    output?: SchemaDefinition;
    // Transformed schema (after this component's processing)
    transformed?: SchemaDefinition;
    // Schema validation state
    validation?: {
      isValid: boolean;
      errors: string[];
      warnings: string[];
    };
  };
  
  // Code generation hints - COMPILER OPTIMIZATION METADATA
  codegen: {
    alias?: string; // SQL alias for this component
    requiresJoinOptimization?: boolean;
    supportsParallelism?: boolean;
    estimatedRowCount?: number;
    hint?: string;
    
    // Enhanced SQL generation metadata
    sqlGeneration: {
      requiresSubquery: boolean;
      requiresCTE: boolean;
      requiresTemporaryTable: boolean;
      joinPaths: string[]; // e.g., ["table1 JOIN table2 ON ..."]
      columnDependencies: Record<string, string[]>; // target column -> source columns
      // Performance hints
      parallelizable: boolean;
      batchSize?: number;
      memoryHint?: 'LOW' | 'MEDIUM' | 'HIGH';
      // Optimization flags
      filterPushdown: boolean;
      projectionPushdown: boolean;
      joinReorder: boolean;
    };
  };
  
  // Execution metadata
  execution: {
    lastExecuted?: string;
    executionTime?: number;
    rowsProcessed?: number;
    errors?: string[];
    warnings?: string[];
  };
  
  // COMPILER-FRIENDLY METADATA
  compilerMetadata: {
    // Node identity and versioning
    nodeId: string;
    version: string;
    createdAt: string;
    lastModified: string;
    
    // Dependencies and lineage
    dependencies: {
      upstream: string[]; // Node IDs that this node depends on
      downstream: string[]; // Node IDs that depend on this node
      dataSources: string[]; // Original source IDs
    };
    
    // Transformation lineage
    transformationLineage: Array<{
      targetField: string;
      sourceFields: string[];
      transformation: string;
      componentId: string; // Which component performed this
    }>;
    
    // SQL generation state
    sqlState: {
      compiledSql?: string;
      compilationTimestamp?: string;
      compilationStatus: 'NOT_COMPILED' | 'COMPILING' | 'COMPILED' | 'ERROR';
      compilationErrors?: string[];
      parameterCount: number;
      // Cached SQL fragments
      selectClause?: string;
      fromClause?: string;
      whereClause?: string;
      groupByClause?: string;
      orderByClause?: string;
    };
    
    // Optimization metadata
    optimization: {
      appliedOptimizations: string[]; // e.g., ["filter_pushdown", "join_reorder"]
      availableOptimizations: string[];
      estimatedCost: number;
      // Statistics for cost-based optimization
      statistics?: {
        rowCount: number;
        distinctValues?: Record<string, number>;
        nullCount?: Record<string, number>;
        minMax?: Record<string, { min: any; max: any }>;
      };
    };
    
    // Validation state
    validation: {
      isValid: boolean;
      errors: string[];
      warnings: string[];
      validationTimestamp: string;
      schemaCompatible: boolean;
    };
    
    // Metadata for incremental compilation
    incremental: {
      lastCompilationHash: string;
      dependenciesHash: string;
      requiresFullRecompile: boolean;
      changedFields: string[];
    };
  };
  
  // General metadata
  metadata: Record<string, any>;
}

// ==================== EDGE METADATA ====================
export interface FlowEdgeMeta {
  // Relation type
  relationType: RelationType;
  
  // Relation configuration - TYPE-SAFE BASED ON RELATION TYPE
  configuration: Record<string, any>;
  
  // Schema validation
  schemaValidation: {
    sourceFields: string[];
    targetFields: string[];
    isSchemaCompatible: boolean;
    validationErrors?: string[];
    // Field-level compatibility
    fieldMapping: Array<{
      sourceField: string;
      targetField: string;
      compatible: boolean;
      typeConversion?: string; // e.g., "INTEGER -> DECIMAL"
    }>;
  };
  
  // COMPILER-FRIENDLY EDGE METADATA
  compilerMetadata: {
    edgeId: string;
    version: string;
    
    // Data flow metadata
    dataFlow: {
      isConditional: boolean;
      conditionExpression?: string;
      dataFlowOrder: number;
      batchSize?: number;
      parallelExecution: boolean;
    };
    
    // SQL generation
    sqlGeneration: {
      joinCondition?: string;
      filterCondition?: string;
      mappingExpression?: string;
      // For conditional edges
      caseExpression?: string;
    };
    
    // Optimization
    optimization: {
      canPushDown: boolean;
      canMerge: boolean; // Can this edge be merged with node transformations
      estimatedSelectivity: number; // For filter edges
    };
    
    // Lineage tracking
    lineage: {
      sourceNodeId: string;
      targetNodeId: string;
      fieldsTransferred: string[];
      transformationApplied: boolean;
    };
  };
  
  // Metadata
  metadata: Record<string, any>;
}

// ==================== METADATA UPDATE STRATEGIES ====================

/**
 * Interface for metadata update operations
 * Ensures all updates are serializable and compiler-friendly
 */
export interface MetadataUpdate<T extends ComponentConfiguration['type']> {
  nodeId: string;
  timestamp: string;
  operation: 'CREATE' | 'UPDATE' | 'DELETE' | 'PARTIAL_UPDATE';
  componentType: T;
  
  // Update data - only includes changed fields
  updates: Partial<
    T extends 'MAP' ? MapComponentConfiguration :
    T extends 'JOIN' ? JoinComponentConfiguration :
    T extends 'FILTER' ? FilterComponentConfiguration :
    T extends 'LOOKUP' ? LookupComponentConfiguration :
    T extends 'AGGREGATE' ? AggregateComponentConfiguration :
    T extends 'SORT' ? SortComponentConfiguration :
    T extends 'INPUT' ? InputComponentConfiguration :
    T extends 'OUTPUT' ? OutputComponentConfiguration :
    never
  >;
  
  // For partial updates, track what changed
  changedFields: string[];
  
  // Lineage information
  lineage: {
    previousHash: string;
    newHash: string;
    parentOperationId?: string;
  };
  
  // Validation result
  validation?: {
    isValid: boolean;
    errors?: string[];
    warnings?: string[];
  };
}

/**
 * Type-safe metadata update creator
 */
export function createMetadataUpdate<T extends ComponentConfiguration['type']>(
  nodeId: string,
  componentType: T,
  updates: MetadataUpdate<T>['updates'],
  previousHash: string
): MetadataUpdate<T> {
  const timestamp = new Date().toISOString();
  const newHash = computeMetadataHash(updates);
  
  // Determine operation type
  const operation = previousHash ? 'UPDATE' : 'CREATE';
  
  // Extract changed fields
  const changedFields = Object.keys(updates).filter(key => 
    updates[key as keyof typeof updates] !== undefined
  );
  
  return {
    nodeId,
    timestamp,
    operation,
    componentType,
    updates,
    changedFields,
    lineage: {
      previousHash,
      newHash,
      parentOperationId: undefined
    }
  } as MetadataUpdate<T>;
}

/**
 * Helper to compute deterministic hash for metadata
 */
function computeMetadataHash(data: any): string {
  // Simple deterministic hash for metadata changes
  const jsonString = JSON.stringify(data, Object.keys(data).sort());
  return btoa(jsonString).substring(0, 32);
}

/**
 * Apply metadata update to a node
 * Returns updated node metadata with proper versioning
 */
export function applyMetadataUpdate<T extends ComponentConfiguration['type']>(
  nodeMeta: FlowNodeMeta,
  update: MetadataUpdate<T>
): FlowNodeMeta {
  const updatedMeta = { ...nodeMeta };
  
  // Update configuration based on component type
  if (update.componentType === 'MAP' && nodeMeta.configuration.type === 'MAP') {
    updatedMeta.configuration = {
      type: 'MAP',
      config: {
        ...nodeMeta.configuration.config,
        ...update.updates
      } as MapComponentConfiguration
    };
  }
  // Add similar handlers for other component types...
  
  // Update compiler metadata
  updatedMeta.compilerMetadata = {
    ...nodeMeta.compilerMetadata,
    lastModified: update.timestamp,
    incremental: {
      ...nodeMeta.compilerMetadata?.incremental,
      lastCompilationHash: update.lineage.newHash,
      requiresFullRecompile: true,
      changedFields: update.changedFields
    },
    sqlState: {
      ...nodeMeta.compilerMetadata?.sqlState,
      compilationStatus: 'NOT_COMPILED' // Mark as needing recompilation
    }
  };
  
  // Update execution metadata
  updatedMeta.execution = {
    ...nodeMeta.execution,
    lastExecuted: undefined, // Clear execution state since metadata changed
    errors: undefined,
    warnings: undefined
  };
  
  return updatedMeta;
}

/**
 * Create a partial update for map transformations
 * Optimized for incremental updates
 */
export function createMapTransformationUpdate(
  nodeId: string,
  transformations: MapTransformation[],
  outputSchema: SchemaDefinition & { persistenceLevel: 'MEMORY' | 'TEMPORARY' | 'PERSISTENT' }, // ADD THIS TYPE
  previousHash: string
): MetadataUpdate<'MAP'> {
  const timestamp = new Date().toISOString();
  
  return createMetadataUpdate<'MAP'>(nodeId, 'MAP', {
    transformations,
    outputSchema: {
      ...outputSchema, // This already includes persistenceLevel now
      fields: outputSchema.fields.map(field => ({
        ...field,
        // Store transformation expression in field metadata
        transformation: transformations
          .find(t => t.targetField === field.name)
          ?.expression
      }))
    },
    compilerMetadata: {
      lastModified: timestamp, // ADD THIS
      createdBy: 'system', // ADD THIS
      mappingCount: transformations.length,
      validationStatus: 'VALID' as const,
      warnings: [], // ADD THIS
      dependencies: [], // ADD THIS
      columnDependencies: computeColumnDependencies(transformations),
      compiledSql: undefined,
      compilationTimestamp: undefined
    }
  }, previousHash);
}

/**
 * Compute column dependencies for SQL generation
 */
export function computeColumnDependencies(
  transformations: MapTransformation[]
): Record<string, string[]> {
  const dependencies: Record<string, string[]> = {};
  
  transformations.forEach(trans => {
    // Extract source columns from expression
    // Simple regex to find column references like {column} or column_name
    const columnRefs = extractColumnReferences(trans.expression);
    dependencies[trans.targetField] = columnRefs;
  });
  
  return dependencies;
}

/**
 * Extract column references from SQL expression
 */
function extractColumnReferences(expression: string): string[] {
  // Match patterns like: {column}, column_name, table.column
  const patterns = [
    /\{([^}]+)\}/g,            // {column_name}
    /([a-zA-Z_][a-zA-Z0-9_]*)(?=\s*(?:[+/*-]|$|,|\)))/g, // column_name
    /([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)/g // table.column
  ];
  
  const references: string[] = [];
  
  patterns.forEach(pattern => {
    let match;
    while ((match = pattern.exec(expression)) !== null) {
      references.push(match[1] || match[0]);
    }
  });
  
  return [...new Set(references)]; // Deduplicate
}

/**
 * Create edge metadata update for schema changes
 */
export function createEdgeMetadataUpdate(
  edgeMeta: FlowEdgeMeta,
  sourceFields: string[],
  targetFields: string[]
): FlowEdgeMeta {
  const fieldMapping = sourceFields.map((sourceField, index) => ({
    sourceField,
    targetField: targetFields[index] || sourceField,
    compatible: true, // Would need type checking
    typeConversion: undefined
  }));
  
  return {
    ...edgeMeta,
    schemaValidation: {
      sourceFields,
      targetFields,
      isSchemaCompatible: fieldMapping.every(fm => fm.compatible),
      fieldMapping,
      validationErrors: undefined
    },
    compilerMetadata: {
      ...edgeMeta.compilerMetadata,
      lineage: {
        ...edgeMeta.compilerMetadata?.lineage,
        fieldsTransferred: sourceFields
      }
    }
  };
}

/**
 * Type guard for component configuration
 */
export function isMapConfiguration(
  config: ComponentConfiguration
): config is { type: 'MAP'; config: MapComponentConfiguration } {
  return config.type === 'MAP';
}

export function isJoinConfiguration(
  config: ComponentConfiguration
): config is { type: 'JOIN'; config: JoinComponentConfiguration } {
  return config.type === 'JOIN';
}

// Similar type guards for other component types...

/**
 * Utility to extract SQL generation hints from configuration
 */
export function extractSqlGenerationHints(
  config: ComponentConfiguration
): FlowNodeMeta['codegen']['sqlGeneration'] {
  switch (config.type) {
    case 'MAP':
      return {
        requiresSubquery: config.config.sqlGeneration.requiresSubquery,
        requiresCTE: config.config.sqlGeneration.requiresWindowFunction,
        requiresTemporaryTable: false,
        joinPaths: (config.config.joins || []).map(j => // ADD NULL CHECK
          `${j.leftTable} ${j.joinType} JOIN ${j.rightTable} ON ${j.leftTable}.${j.leftField} = ${j.rightTable}.${j.rightField}`
        ),
        columnDependencies: config.config.compilerMetadata.columnDependencies,
        parallelizable: config.config.sqlGeneration.parallelizable,
        batchSize: config.config.sqlGeneration.batchSize,
        memoryHint: config.config.sqlGeneration.memoryHint,
        filterPushdown: false,
        projectionPushdown: true,
        joinReorder: (config.config.joins?.length || 0) > 1 // ADD NULL CHECK
      };
    
    case 'JOIN':
      return {
        requiresSubquery: false,
        requiresCTE: false,
        requiresTemporaryTable: config.config.sqlGeneration.joinAlgorithm === 'HASH',
        joinPaths: [
          `${config.config.joinConditions[0]?.leftTable || 'table1'} ${config.config.joinType} JOIN ${config.config.joinConditions[0]?.rightTable || 'table2'}`
        ],
        columnDependencies: {},
        parallelizable: config.config.sqlGeneration.canParallelize,
        batchSize: undefined,
        memoryHint: 'MEDIUM',
        filterPushdown: true,
        projectionPushdown: true,
        joinReorder: config.config.joinConditions.length > 1
      };
    
    // Handle other component types...
    
    default:
      return {
        requiresSubquery: false,
        requiresCTE: false,
        requiresTemporaryTable: false,
        joinPaths: [],
        columnDependencies: {},
        parallelizable: false,
        batchSize: undefined,
        memoryHint: 'LOW',
        filterPushdown: false,
        projectionPushdown: false,
        joinReorder: false
      };
  }
}

/**
 * Create initial node metadata for a component
 */
export function createInitialNodeMetadata(
  componentKey: string,
  componentType: ComponentRole,
  label: string,
  configuration: ComponentConfiguration
): FlowNodeMeta {
  const nodeId = `node-${Date.now()}-${componentKey}`;
  const timestamp = new Date().toISOString();
  
  return {
    componentKey,
    componentType,
    instanceNumber: 1,
    label,
    status: 'default',
    configuration,
    schemas: {
      input: undefined,
      output: undefined,
      transformed: undefined
    },
    codegen: {
      alias: label.replace(/[^a-zA-Z0-9]/g, '_').toLowerCase(),
      estimatedRowCount: 1000,
      sqlGeneration: extractSqlGenerationHints(configuration)
    },
    execution: {},
    compilerMetadata: {
      nodeId,
      version: '1.0',
      createdAt: timestamp,
      lastModified: timestamp,
      dependencies: {
        upstream: [],
        downstream: [],
        dataSources: []
      },
      transformationLineage: [],
      sqlState: {
        compilationStatus: 'NOT_COMPILED',
        parameterCount: 0
      },
      optimization: {
        appliedOptimizations: [],
        availableOptimizations: ['filter_pushdown', 'projection_pushdown'],
        estimatedCost: 1.0
      },
      validation: {
        isValid: true,
        errors: [],
        warnings: [],
        validationTimestamp: timestamp,
        schemaCompatible: true
      },
      incremental: {
        lastCompilationHash: '',
        dependenciesHash: '',
        requiresFullRecompile: true,
        changedFields: []
      }
    },
    metadata: {}
  };
}