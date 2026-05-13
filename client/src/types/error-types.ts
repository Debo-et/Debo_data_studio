// src/types/error-types.ts

/**
 * Comprehensive error handling system for PostgreSQL SQL Generator
 */

// ==================== ERROR CODES ENUM ====================

export enum ErrorCode {
  // Connection Errors (1000-1999)
  CONNECTION_VALIDATION_FAILED = 1000,
  CONNECTION_TYPE_MISMATCH = 1001,
  CONNECTION_SCHEMA_MISMATCH = 1002,
  CONNECTION_CIRCULAR_DEPENDENCY = 1003,
  CONNECTION_MISSING_NODE = 1004,
  
  // SQL Generation Errors (2000-2999)
  SQL_GENERATION_FAILED = 2000,
  SQL_SYNTAX_ERROR = 2001,
  SQL_SEMANTIC_ERROR = 2002,
  SQL_TYPE_MISMATCH = 2003,
  SQL_FUNCTION_NOT_SUPPORTED = 2004,
  SQL_FEATURE_NOT_SUPPORTED = 2005,
  SQL_PARAMETER_MISSING = 2006,
  
  // Node Validation Errors (3000-3999)
  NODE_CONFIGURATION_INVALID = 3000,
  NODE_SCHEMA_INVALID = 3001,
  NODE_TYPE_UNSUPPORTED = 3002,
  NODE_DEPENDENCY_MISSING = 3003,
  NODE_METADATA_INVALID = 3004,
  
  // PostgreSQL Execution Errors (4000-4999)
  POSTGRESQL_SYNTAX_ERROR = 4000,
  POSTGRESQL_FEATURE_NOT_SUPPORTED = 4001,
  POSTGRESQL_TYPE_ERROR = 4002,
  POSTGRESQL_CONSTRAINT_VIOLATION = 4003,
  POSTGRESQL_PERMISSION_DENIED = 4004,
  POSTGRESQL_CONNECTION_ERROR = 4005,
  POSTGRESQL_QUERY_TIMEOUT = 4006,
  POSTGRESQL_OUT_OF_MEMORY = 4007,
  
  // Pipeline Errors (5000-5999)
  PIPELINE_CYCLIC_DEPENDENCY = 5000,
  PIPELINE_NODE_MISSING = 5001,
  PIPELINE_GENERATION_FAILED = 5002,
  PIPELINE_VALIDATION_FAILED = 5003,
  PIPELINE_OPTIMIZATION_FAILED = 5004,
  
  // Recovery Errors (6000-6999)
  RECOVERY_AUTO_FIX_FAILED = 6000,
  RECOVERY_MANUAL_INTERVENTION_REQUIRED = 6001,
  RECOVERY_SCHEMA_ADJUSTMENT_FAILED = 6002,
  
  // System Errors (9000-9999)
  SYSTEM_UNEXPECTED_ERROR = 9000,
  SYSTEM_CONFIGURATION_ERROR = 9001,
  SYSTEM_RESOURCE_UNAVAILABLE = 9002,
}

// ==================== ERROR SEVERITY ====================

export enum ErrorSeverity {
  INFO = 'INFO',
  WARNING = 'WARNING',
  ERROR = 'ERROR',
  CRITICAL = 'CRITICAL'
}

// ==================== ERROR TYPES ====================

export interface BaseError {
  id: string;
  code: ErrorCode;
  severity: ErrorSeverity;
  message: string;
  timestamp: string;
  context: Record<string, any>;
  stackTrace?: string;
  recoverySuggestion?: string;
  userFriendlyMessage: string;
}

export interface ConnectionError extends BaseError {
  type: 'connection';
  connectionId?: string;
  sourceNodeId?: string;
  targetNodeId?: string;
  validationResult?: any;
}

export interface SQLGenerationError extends BaseError {
  type: 'sql_generation';
  nodeId?: string;
  fragmentType?: string;
  sqlSnippet?: string;
  lineNumber?: number;
  columnNumber?: number;
}

export interface PostgreSQLExecutionError extends BaseError {
  type: 'postgresql_execution';
  query?: string;
  postgresErrorCode?: string;
  schema?: string;
  table?: string;
  column?: string;
  constraint?: string;
}

export interface NodeValidationError extends BaseError {
  type: 'node_validation';
  nodeId: string;
  nodeType: string;
  validationDetails: Record<string, any>;
}

export interface PipelineError extends BaseError {
  type: 'pipeline';
  pipelineId?: string;
  nodeIds?: string[];
  stage?: string;
  executionPlan?: any;
}

export interface RecoveryError extends BaseError {
  type: 'recovery';
  originalError: BaseError;
  recoveryAttempts: number;
  recoveryStrategy?: RecoveryStrategy;
}

export type AppError = 
  | ConnectionError 
  | SQLGenerationError 
  | PostgreSQLExecutionError 
  | NodeValidationError 
  | PipelineError 
  | RecoveryError;

// ==================== RECOVERY STRATEGIES ====================

export enum RecoveryStrategy {
  AUTO_TYPE_CAST = 'auto_type_cast',
  AUTO_NULL_HANDLING = 'auto_null_handling',
  AUTO_SCHEMA_ADJUSTMENT = 'auto_schema_adjustment',
  AUTO_QUERY_SIMPLIFICATION = 'auto_query_simplification',
  AUTO_COLUMN_MAPPING = 'auto_column_mapping',
  AUTO_FUNCTION_REPLACEMENT = 'auto_function_replacement',
  MANUAL_INTERVENTION = 'manual_intervention',
}

export interface RecoveryAction {
  strategy: RecoveryStrategy;
  description: string;
  implementation: (error: AppError) => Promise<RecoveryResult>;
  confidence: number; // 0-1
  estimatedTimeMs: number;
}

export interface RecoveryResult {
  success: boolean;
  recoveredError?: AppError;
  appliedStrategy?: RecoveryStrategy;
  changes: Array<{
    type: string;
    description: string;
    before?: any;
    after?: any;
  }>;
  warnings: string[];
}

// ==================== SCHEMA TYPES ====================

export interface TransformationRule {
  type: 'cast' | 'coalesce' | 'expression';
  expression: string;
  parameters?: Record<string, any>;
}

export interface SchemaMapping {
  sourceColumn: string;
  targetColumn: string;
  dataTypeConversion?: {
    sourceType: string;
    targetType: string;
  };
  transformation?: TransformationRule;
  defaultValue?: any;
  isRequired?: boolean;
}

export type PostgreSQLDataType = 
  | 'integer' | 'bigint' | 'serial' | 'bigserial'
  | 'numeric' | 'decimal' | 'real' | 'double precision'
  | 'varchar' | 'text' | 'char'
  | 'boolean' | 'bool'
  | 'date' | 'timestamp' | 'timestamptz' | 'time' | 'timetz'
  | 'interval' | 'json' | 'jsonb'
  | 'uuid' | 'bytea';
  
// ==================== ERROR CONTEXT ====================

export interface ErrorContext {
  userId?: string;
  sessionId?: string;
  environment: 'development' | 'staging' | 'production';
  component: string;
  node?: {
    id: string;
    type: string;
    name: string;
  };
  connection?: {
    id: string;
    sourceId: string;
    targetId: string;
  };
  pipeline?: {
    id: string;
    name: string;
    stage: string;
  };
  postgresVersion?: string;
  // Additional properties for recovery service
  fullNode?: any; // Full CanvasNode object if available
  fullConnection?: any; // Full CanvasConnection object if available
}

// ==================== ERROR REPORT ====================

export interface ErrorReport {
  error: AppError;
  context: ErrorContext;
  metadata: {
    reportedAt: string;
    source: 'client' | 'server' | 'pipeline';
    userAgent?: string;
    ipAddress?: string;
    platform?: string;
  };
  analytics: {
    errorCount: number;
    firstOccurrence: string;
    lastOccurrence: string;
    frequency: number; // errors per hour
  };
}

// ==================== ERROR BOUNDARY STATE ====================

export interface ErrorBoundaryState {
  hasError: boolean;
  error: AppError | null;
  errorInfo?: any;
  recoveryAttempts: number;
  recoveryStatus: 'idle' | 'recovering' | 'recovered' | 'failed';
  userActionRequired: boolean;
  recoverySuggestions: RecoveryAction[];
}

// ==================== ERROR UTILITIES ====================

export class ErrorFactory {
  static createConnectionError(
    code: ErrorCode,
    message: string,
    context: Partial<ConnectionError> = {}
  ): ConnectionError {
    return {
      id: `conn-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
      code,
      severity: this.getSeverityForCode(code),
      message,
      timestamp: new Date().toISOString(),
      context: {},
      userFriendlyMessage: this.getUserFriendlyMessage(code, context),
      type: 'connection',
      ...context
    };
  }

  static createSQLGenerationError(
    code: ErrorCode,
    message: string,
    context: Partial<SQLGenerationError> = {}
  ): SQLGenerationError {
    return {
      id: `sql-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
      code,
      severity: this.getSeverityForCode(code),
      message,
      timestamp: new Date().toISOString(),
      context: {},
      userFriendlyMessage: this.getUserFriendlyMessage(code, context),
      type: 'sql_generation',
      ...context
    };
  }

  static createPostgreSQLExecutionError(
    code: ErrorCode,
    message: string,
    context: Partial<PostgreSQLExecutionError> = {}
  ): PostgreSQLExecutionError {
    return {
      id: `pg-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
      code,
      severity: this.getSeverityForCode(code),
      message,
      timestamp: new Date().toISOString(),
      context: {},
      userFriendlyMessage: this.getUserFriendlyMessage(code, context),
      type: 'postgresql_execution',
      ...context
    };
  }

  private static getSeverityForCode(code: ErrorCode): ErrorSeverity {
    const codeRange = Math.floor(code / 1000);
    
    switch (codeRange) {
      case 1: // Connection errors
        return ErrorSeverity.ERROR;
      case 2: // SQL generation errors
        return ErrorSeverity.ERROR;
      case 3: // Node validation errors
        return ErrorSeverity.WARNING;
      case 4: // PostgreSQL execution errors
        return ErrorSeverity.ERROR;
      case 5: // Pipeline errors
        return ErrorSeverity.CRITICAL;
      case 6: // Recovery errors
        return ErrorSeverity.WARNING;
      case 9: // System errors
        return ErrorSeverity.CRITICAL;
      default:
        return ErrorSeverity.ERROR;
    }
  }

  private static getUserFriendlyMessage(code: ErrorCode, context: any): string {
    const messages: Record<ErrorCode, string> = {
      // Connection Errors
      [ErrorCode.CONNECTION_VALIDATION_FAILED]: 'Connection validation failed. Please check the connection settings.',
      [ErrorCode.CONNECTION_TYPE_MISMATCH]: 'Data type mismatch between connected nodes. Consider adding a type conversion.',
      [ErrorCode.CONNECTION_SCHEMA_MISMATCH]: 'Schema mismatch between nodes. Please verify column mappings.',
      [ErrorCode.CONNECTION_CIRCULAR_DEPENDENCY]: 'Circular dependency detected. Please review the pipeline connections.',
      [ErrorCode.CONNECTION_MISSING_NODE]: 'Connection references a missing node.',
      
      // SQL Generation Errors
      [ErrorCode.SQL_GENERATION_FAILED]: 'Failed to generate SQL. Please check node configurations.',
      [ErrorCode.SQL_SYNTAX_ERROR]: 'SQL syntax error detected. The generated query is invalid.',
      [ErrorCode.SQL_SEMANTIC_ERROR]: 'SQL semantic error. The query logic is incorrect.',
      [ErrorCode.SQL_TYPE_MISMATCH]: 'Type mismatch in SQL expression.',
      [ErrorCode.SQL_FUNCTION_NOT_SUPPORTED]: 'Function not supported by the target database.',
      [ErrorCode.SQL_FEATURE_NOT_SUPPORTED]: 'Feature not supported by the target PostgreSQL version.',
      [ErrorCode.SQL_PARAMETER_MISSING]: 'Required parameter is missing.',
      
      // Node Validation Errors
      [ErrorCode.NODE_CONFIGURATION_INVALID]: 'Node configuration is invalid.',
      [ErrorCode.NODE_SCHEMA_INVALID]: 'Node schema is invalid.',
      [ErrorCode.NODE_TYPE_UNSUPPORTED]: 'Node type is not supported.',
      [ErrorCode.NODE_DEPENDENCY_MISSING]: 'Required dependency is missing.',
      [ErrorCode.NODE_METADATA_INVALID]: 'Node metadata is invalid.',
      
      // PostgreSQL Execution Errors
      [ErrorCode.POSTGRESQL_SYNTAX_ERROR]: 'PostgreSQL syntax error in generated query.',
      [ErrorCode.POSTGRESQL_FEATURE_NOT_SUPPORTED]: 'PostgreSQL feature not available in the target version.',
      [ErrorCode.POSTGRESQL_TYPE_ERROR]: 'PostgreSQL data type error.',
      [ErrorCode.POSTGRESQL_CONSTRAINT_VIOLATION]: 'Constraint violation in PostgreSQL query.',
      [ErrorCode.POSTGRESQL_PERMISSION_DENIED]: 'Permission denied for PostgreSQL operation.',
      [ErrorCode.POSTGRESQL_CONNECTION_ERROR]: 'PostgreSQL connection error.',
      [ErrorCode.POSTGRESQL_QUERY_TIMEOUT]: 'PostgreSQL query timed out.',
      [ErrorCode.POSTGRESQL_OUT_OF_MEMORY]: 'PostgreSQL out of memory.',
      
      // Pipeline Errors
      [ErrorCode.PIPELINE_CYCLIC_DEPENDENCY]: 'Pipeline contains circular dependencies.',
      [ErrorCode.PIPELINE_NODE_MISSING]: 'Pipeline node is missing.',
      [ErrorCode.PIPELINE_GENERATION_FAILED]: 'Pipeline generation failed.',
      [ErrorCode.PIPELINE_VALIDATION_FAILED]: 'Pipeline validation failed.',
      [ErrorCode.PIPELINE_OPTIMIZATION_FAILED]: 'Pipeline optimization failed.',
      
      // Recovery Errors
      [ErrorCode.RECOVERY_AUTO_FIX_FAILED]: 'Automatic fix failed. Manual intervention required.',
      [ErrorCode.RECOVERY_MANUAL_INTERVENTION_REQUIRED]: 'Manual intervention required to fix the error.',
      [ErrorCode.RECOVERY_SCHEMA_ADJUSTMENT_FAILED]: 'Schema adjustment failed.',
      
      // System Errors
      [ErrorCode.SYSTEM_UNEXPECTED_ERROR]: 'An unexpected error occurred.',
      [ErrorCode.SYSTEM_CONFIGURATION_ERROR]: 'System configuration error.',
      [ErrorCode.SYSTEM_RESOURCE_UNAVAILABLE]: 'System resource unavailable.',
    };

    let message = messages[code] || 'An unknown error occurred.';
    
    // Add context-specific information
    if (context?.nodeId) {
      message += ` (Node: ${context.nodeId})`;
    }
    if (context?.connectionId) {
      message += ` (Connection: ${context.connectionId})`;
    }
    if (context?.column) {
      message += ` (Column: ${context.column})`;
    }
    
    return message;
  }
}