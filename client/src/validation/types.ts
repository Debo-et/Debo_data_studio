// src/validation/types.ts

/**
 * Node Schema Definition
 * Defines validation rules for a specific node type
 */
export interface NodeSchema {
  /** Unique identifier for the schema */
  id: string;
  
  /** Node type this schema applies to */
  nodeType: string;
  
  /** Display name for UI */
  displayName: string;
  
  /** Category: input, processing, output, etc. */
  category: 'input' | 'processing' | 'output' | 'transform' | 'lookup' | 'match';
  
  /** Whether cycles involving this node are allowed */
  allowsCycles: boolean;
  
  /** Maximum number of incoming connections (null = unlimited) */
  maxIncomingConnections: number | null;
  
  /** Maximum number of outgoing connections (null = unlimited) */
  maxOutgoingConnections: number | null;
  
  /** Allowed source node types that can connect TO this node */
  allowedSourceTypes: string[];
  
  /** Allowed target node types this node can connect TO */
  allowedTargetTypes: string[];
  
  
  /** Port-specific validation rules */
  portRules?: {
    [portId: string]: {
      portType: 'input' | 'output';
      dataTypes: string[]; // e.g., ['string', 'number', 'boolean', 'any']
      maxConnections: number | null;
    };
  };

  
  /** Custom validation function */
  customValidator?: (node: GraphNode, graph: GraphState) => ValidationResult[];
}

/**
 * Connection Rule between specific node types
 */
export interface ConnectionRule {
  sourceType: string;
  targetType: string;
  
  /** Whether this connection type is allowed */
  allowed: boolean;
  
  /** Cardinality: how many connections of this type are allowed */
  maxConnections?: number;
  
  /** Data type compatibility requirements */
  requiredDataTypeMatch?: boolean;
  
  /** Schema-specific validation */
  schemaValidation?: {
    sourceSchema?: string;
    targetSchema?: string;
    columnMapping?: Record<string, string>;
  };

}

/**
 * Graph Node with metadata
 */
export interface GraphNode {
  id: string;
  type: string;
  position: { x: number; y: number };
  
  /** Node-specific data */
  data: {
    name: string;
    label?: string;
    technology?: string;
    componentCategory?: string;
    
    /** Schema information for data nodes */
    schema?: {
      columns: Array<{
        name: string;
        type: string;
        nullable: boolean;
        constraints?: string[];
      }>;
      primaryKey?: string[];
      foreignKeys?: Array<{
        column: string;
        referenceTable: string;
        referenceColumn: string;
      }>;
    };
    
    /** Custom properties */
    [key: string]: any;
  };
  
  /** Connection ports */
  ports?: Array<{
    id: string;
    type: 'input' | 'output';
    dataType?: string;
    label?: string;
  }>;
  
  /** Metadata for validation */
  metadata?: {
    validationStatus?: 'valid' | 'warning' | 'error';
    lastValidated?: string;
    [key: string]: any;
  };
}

/**
 * Graph Edge/Connection
 */
export interface GraphEdge {
  id: string;
  source: string;        // Node ID
  target: string;        // Node ID
  sourceHandle?: string; // Port ID
  targetHandle?: string; // Port ID
  
  /** Edge data */
  data?: {
    label?: string;
    dataType?: string;
    mapping?: Record<string, string>;
    [key: string]: any;
  };
  
  /** Metadata for validation */
  metadata?: {
    validationStatus?: 'valid' | 'warning' | 'error';
    validationErrors?: string[];
    [key: string]: any;
  };
}

/**
 * Complete Graph State
 */
export interface GraphState {
  nodes: GraphNode[];
  edges: GraphEdge[];
  
  /** Graph metadata */
  metadata?: {
    name: string;
    description?: string;
    validationMode?: 'strict' | 'lenient' | 'warn-only';
    [key: string]: any;
  };
}

/**
 * Validation Rule Definition
 */
export interface ValidationRule {
  id: string;
  name: string;
  description: string;
  severity: 'error' | 'warning';
  
  /** Predicate to check if rule applies */
  appliesTo: (node: GraphNode, edge: GraphEdge, graph: GraphState) => boolean;
  
  /** Validation function */
  validate: (node: GraphNode, edge: GraphEdge, graph: GraphState) => ValidationResult[];
  
  /** Optional fix suggestion */
  getFixSuggestion?: (node: GraphNode, edge: GraphEdge, graph: GraphState) => string | null;
}

/**
 * Validation Error/Warning Levels
 */
export enum ValidationLevel {
  ERROR = 'error',
  WARNING = 'warning',
  INFO = 'info'
}

/**
 * Validation Error Code Enumeration
 */
export enum ValidationErrorCode {
  // Cycle-related errors
  CYCLE_DETECTED = 'CYCLE_DETECTED',
  DISALLOWED_CYCLE = 'DISALLOWED_CYCLE',
  
  // Connection-related errors
  INVALID_CONNECTION_TYPE = 'INVALID_CONNECTION_TYPE',
  INVALID_SOURCE_TYPE = 'INVALID_SOURCE_TYPE',
  INVALID_TARGET_TYPE = 'INVALID_TARGET_TYPE',
  MAX_INCOMING_EXCEEDED = 'MAX_INCOMING_EXCEEDED',
  MAX_OUTGOING_EXCEEDED = 'MAX_OUTGOING_EXCEEDED',
  PORT_CONNECTION_LIMIT = 'PORT_CONNECTION_LIMIT',
  MULTIPLE_CONNECTIONS_DISALLOWED = 'MULTIPLE_CONNECTIONS_DISALLOWED',
  
  // Schema-related errors
  SCHEMA_INCOMPATIBILITY = 'SCHEMA_INCOMPATIBILITY',
  DATA_TYPE_MISMATCH = 'DATA_TYPE_MISMATCH',
  REQUIRED_COLUMN_MISSING = 'REQUIRED_COLUMN_MISSING',
  
  // Topology errors
  DISCONNECTED_NODE = 'DISCONNECTED_NODE',
  MULTIPLE_PATHS = 'MULTIPLE_PATHS',
  DEAD_END = 'DEAD_END',
  
  // Custom validation errors
  CUSTOM_VALIDATION_FAILED = 'CUSTOM_VALIDATION_FAILED',
  INVALID_ETL_CONNECTION = "INVALID_ETL_CONNECTION",
  MULTIPLE_INPUTS_DISALLOWED = "MULTIPLE_INPUTS_DISALLOWED",
  FAN_OUT_DISALLOWED = "FAN_OUT_DISALLOWED",
  INVALID_MERGE_NODE_INPUTS = "INVALID_MERGE_NODE_INPUTS",
  INVALID_BRANCHING_NODE_OUTPUTS = "INVALID_BRANCHING_NODE_OUTPUTS",
  SOURCE_TO_SOURCE_DISALLOWED = "SOURCE_TO_SOURCE_DISALLOWED",
  SINK_TO_SINK_DISALLOWED = "SINK_TO_SINK_DISALLOWED",
  OUTPUT_TO_OUTPUT_DISALLOWED = "OUTPUT_TO_OUTPUT_DISALLOWED",
  INPUT_TO_INPUT_DISALLOWED = "INPUT_TO_INPUT_DISALLOWED",
  TOO_MANY_INPUTS = "TOO_MANY_INPUTS",
  TOO_FEW_INPUTS = "TOO_FEW_INPUTS",
  EXACT_INPUT_COUNT_REQUIRED = "EXACT_INPUT_COUNT_REQUIRED",
  OUTPUT_PORT_REUSE_DISALLOWED = "OUTPUT_PORT_REUSE_DISALLOWED",
  INPUT_PORT_MULTIPLE_CONNECTIONS = "INPUT_PORT_MULTIPLE_CONNECTIONS",
  MERGE_NODE_MISSING_INPUTS = "MERGE_NODE_MISSING_INPUTS",
  NON_MERGE_NODE_MULTIPLE_INPUTS = "NON_MERGE_NODE_MULTIPLE_INPUTS",
  BRANCHING_REQUIRED_FOR_FAN_OUT = "BRANCHING_REQUIRED_FOR_FAN_OUT",
  SOURCE_COMPONENT_INPUTS = "SOURCE_COMPONENT_INPUTS",
  PROCESSING_NODE_MISSING_INPUTS = "PROCESSING_NODE_MISSING_INPUTS",
  SINK_COMPONENT_OUTPUTS = "SINK_COMPONENT_OUTPUTS",
  PROCESSING_NODE_MISSING_OUTPUTS = "PROCESSING_NODE_MISSING_OUTPUTS",
  BRANCHING_NODE_INPUT_MISSING = "BRANCHING_NODE_INPUT_MISSING",
  MERGE_NODE_OUTPUT_MISSING = "MERGE_NODE_OUTPUT_MISSING"
}


// Add this to the NodeSchema interface in types.ts
export interface NodeSchema {
  /** Unique identifier for the schema */
  id: string;
  
  /** Node type this schema applies to */
  nodeType: string;
  
  /** Display name for UI */
  displayName: string;
  
  /** Category: input, processing, output, etc. */
  category: 'input' | 'processing' | 'output' | 'transform' | 'lookup' | 'match';
  
  /** ETL-specific category */
  etlCategory?: 'source' | 'sink' | 'processing' | 'merge' | 'branching' | 'unknown';
  
  /** Whether cycles involving this node are allowed */
  allowsCycles: boolean;
  
  /** Maximum number of incoming connections (null = unlimited) */
  maxIncomingConnections: number | null;
  
  /** Maximum number of outgoing connections (null = unlimited) */
  maxOutgoingConnections: number | null;
  
  /** Allowed source node types that can connect TO this node */
  allowedSourceTypes: string[];
  
  /** Allowed target node types this node can connect TO */
  allowedTargetTypes: string[];
  
  /** ETL-specific connection rules */
  etlConnectionRules?: {
    allowedSourceCategories: string[];
    allowedTargetCategories: string[];
    requiresMergeNodeForMultipleInputs?: boolean;
    allowsUnlimitedInputs?: boolean;
    exactInputCount?: number;
    requiresBranchingNodeForFanOut?: boolean;
    allowsMultipleMainOutputs?: boolean;
  };
  
  /** ETL port definitions */
  etlPorts?: {
    mainInput?: {
      id: string;
      minConnections: number;
      maxConnections: number | null;
      required: boolean;
    };
    mainOutput?: {
      id: string;
      minConnections: number;
      maxConnections: number | null;
      allowsFanOut: boolean;
    };
    lookupInputs?: Array<{
      id: string;
      maxConnections: number;
    }>;
  };
  
  /** Component-specific metadata */
  componentMetadata?: {
    technology?: string;
    version?: string;
    description?: string;
    isMultiInput?: boolean;
    isBranching?: boolean;
    isMerge?: boolean;
    allowsLookupInputs?: boolean;
    allowsUnlimitedInputs?: boolean;
    allowsMultipleMainOutputs?: boolean;
    requiresExactInputs?: number;
    inputPortCount?: number | null;
    outputPortCount?: number | null;
    [key: string]: any;
  };
  
  /** Port-specific validation rules */
  portRules?: {
    [portId: string]: {
      portType: 'input' | 'output';
      dataTypes: string[];
      maxConnections: number | null;
    };
  };
  
  /** Custom validation function */
  customValidator?: (node: GraphNode, graph: GraphState) => ValidationResult[];
}
/**
 * Validation Result Interface
 */
export interface ValidationResult {
  /** Unique identifier for this validation result */
  id: string;
  
  /** Error/Warning code */
  code: ValidationErrorCode;
  
  /** Severity level */
  level: ValidationLevel | 'error' | 'warning' | 'info';
  
  /** Human-readable message */
  message: string;
  
  /** Technical details for debugging */
  details?: string;
  
  /** Affected node IDs */
  nodeIds: string[];
  
  /** Affected edge IDs */
  edgeIds: string[];
  
  /** Timestamp of validation */
  timestamp: string;
  
  /** Suggestion for fixing the issue */
  fixSuggestion?: string;
  
  /** Additional context data */
  context?: Record<string, any>;
}

/**
 * Validation Summary
 */
export interface ValidationSummary {
  /** Overall validation status */
  isValid: boolean;
  
  /** Counts by level */
  counts: {
    errors: number;
    warnings: number;
    infos: number;
  };
  
  /** All validation results */
  results: ValidationResult[];
  
  /** Validation timestamp */
  validatedAt: string;
  
  /** Graph metadata at time of validation */
  graphMetadata: {
    nodeCount: number;
    edgeCount: number;
    [key: string]: any;
  };
}