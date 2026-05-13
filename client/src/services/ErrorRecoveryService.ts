// src/services/ErrorRecoveryService.ts

import {
  AppError,
  RecoveryAction,
  RecoveryResult,
  RecoveryStrategy,
  ErrorCode,
  ConnectionError,
  SQLGenerationError,
  PostgreSQLExecutionError,
  SchemaMapping
} from '../types/error-types';

/**
 * Service for automatic error detection and recovery
 * Implements intelligent fix suggestions and automatic corrections
 */
export class ErrorRecoveryService {
  private recoveryHistory: Map<string, RecoveryResult[]> = new Map();
  private autoFixEnabled: boolean = true;
  private maxRecoveryAttempts: number = 3;

  // ==================== PUBLIC API ====================

  /**
   * Analyze error and suggest recovery actions
   */
  public analyzeError(error: AppError): RecoveryAction[] {
    const actions: RecoveryAction[] = [];

    switch (error.type) {
      case 'connection':
        actions.push(...this.analyzeConnectionError(error));
        break;
      case 'sql_generation':
        actions.push(...this.analyzeSQLGenerationError(error));
        break;
      case 'postgresql_execution':
        actions.push(...this.analyzePostgreSQLError(error));
        break;
      case 'node_validation':
        actions.push(...this.analyzeNodeValidationError(error));
        break;
    }

    // Sort by confidence (highest first)
    return actions.sort((a, b) => b.confidence - a.confidence);
  }

  /**
   * Attempt automatic recovery for an error
   */
  public async attemptAutoRecovery(
    error: AppError,
    _context: {
      nodes?: Array<{
        id: string;
        type: string;
        name: string;
        position?: { x: number; y: number };
        size?: { width: number; height: number };
        [key: string]: any; // Allow additional properties
      }>;
      connections?: Array<{
        id: string;
        sourceNodeId: string;
        targetNodeId: string;
        sourcePortId?: string;
        targetPortId?: string;
        [key: string]: any; // Allow additional properties
      }>;
      schema?: any;
    } = {}
  ): Promise<RecoveryResult> {
    if (!this.autoFixEnabled) {
      return {
        success: false,
        changes: [],
        warnings: ['Auto-recovery is disabled']
      };
    }

    const errorHistory = this.recoveryHistory.get(error.id) || [];
    if (errorHistory.length >= this.maxRecoveryAttempts) {
      return {
        success: false,
        changes: [],
        warnings: ['Maximum recovery attempts reached']
      };
    }

    const actions = this.analyzeError(error);
    const bestAction = actions.find(action => action.confidence > 0.7);

    if (!bestAction) {
      return {
        success: false,
        changes: [],
        warnings: ['No confident recovery action available']
      };
    }

    try {
      const result = await bestAction.implementation(error);
      
      // Store recovery attempt
      errorHistory.push(result);
      this.recoveryHistory.set(error.id, errorHistory);

      return result;
    } catch (recoveryError) {
      return {
        success: false,
        changes: [],
        warnings: [`Recovery failed: ${recoveryError}`]
      };
    }
  }

  /**
   * Apply a specific recovery strategy
   */
  public async applyRecoveryStrategy(
    error: AppError,
    strategy: RecoveryStrategy,
    context: any = {}
  ): Promise<RecoveryResult> {
    const implementation = this.getRecoveryImplementation(strategy);
    
    if (!implementation) {
      return {
        success: false,
        changes: [],
        warnings: [`Recovery strategy ${strategy} not implemented`]
      };
    }

    return implementation(error, context);
  }

  /**
   * Attempt batch recovery for multiple errors
   */
  public async attemptBatchRecovery(
    errors: AppError[],
    context: any
  ): Promise<BatchRecoveryResult> {
    const results: RecoveryResult[] = [];
    const recoveredErrors: string[] = [];

    for (const error of errors) {
      const result = await this.attemptAutoRecovery(error, context);
      results.push(result);
      
      if (result.success) {
        recoveredErrors.push(error.id);
      }
    }

    return {
      totalErrors: errors.length,
      recoveredErrors,
      results
    };
  }

  /**
   * Get recovery history for an error
   */
  public getRecoveryHistory(errorId: string): RecoveryResult[] {
    return this.recoveryHistory.get(errorId) || [];
  }

  /**
   * Enable/disable auto-recovery
   */
  public setAutoRecovery(enabled: boolean): void {
    this.autoFixEnabled = enabled;
  }

  // ==================== ERROR ANALYSIS ====================

  private analyzeConnectionError(error: ConnectionError): RecoveryAction[] {
    const actions: RecoveryAction[] = [];

    switch (error.code) {
      case ErrorCode.CONNECTION_TYPE_MISMATCH:
        actions.push(
          this.createAutoTypeCastAction(error),
          this.createColumnMappingAction(error)
        );
        break;

      case ErrorCode.CONNECTION_SCHEMA_MISMATCH:
        actions.push(
          this.createSchemaAdjustmentAction(error),
          this.createColumnMappingAction(error),
          this.createQuerySimplificationAction(error)
        );
        break;

      case ErrorCode.CONNECTION_CIRCULAR_DEPENDENCY:
        actions.push(this.createManualInterventionAction(error));
        break;

      case ErrorCode.CONNECTION_VALIDATION_FAILED:
        actions.push(
          this.createSchemaAdjustmentAction(error),
          this.createAutoNullHandlingAction(error)
        );
        break;
    }

    return actions;
  }

  private analyzeSQLGenerationError(error: SQLGenerationError): RecoveryAction[] {
    const actions: RecoveryAction[] = [];

    switch (error.code) {
      case ErrorCode.SQL_TYPE_MISMATCH:
        actions.push(
          this.createAutoTypeCastAction(error),
          this.createFunctionReplacementAction(error)
        );
        break;

      case ErrorCode.SQL_FUNCTION_NOT_SUPPORTED:
        actions.push(
          this.createFunctionReplacementAction(error),
          this.createQuerySimplificationAction(error)
        );
        break;

      case ErrorCode.SQL_FEATURE_NOT_SUPPORTED:
        actions.push(
          this.createFunctionReplacementAction(error),
          this.createQuerySimplificationAction(error)
        );
        break;

      case ErrorCode.SQL_SYNTAX_ERROR:
        actions.push(
          this.createQuerySimplificationAction(error),
          this.createManualInterventionAction(error)
        );
        break;
    }

    return actions;
  }

  private analyzePostgreSQLError(error: PostgreSQLExecutionError): RecoveryAction[] {
    const actions: RecoveryAction[] = [];

    if (error.postgresErrorCode) {
      // PostgreSQL error codes (see: https://www.postgresql.org/docs/current/errcodes-appendix.html)
      switch (error.postgresErrorCode) {
        case '22007': // invalid_datetime_format
        case '22008': // datetime_field_overflow
          actions.push(this.createAutoTypeCastAction(error));
          break;

        case '22003': // numeric_value_out_of_range
          actions.push(this.createFunctionReplacementAction(error));
          break;

        case '23502': // not_null_violation
          actions.push(this.createAutoNullHandlingAction(error));
          break;

        case '23503': // foreign_key_violation
          actions.push(this.createManualInterventionAction(error));
          break;

        case '23505': // unique_violation
          actions.push(
            this.createAutoNullHandlingAction(error),
            this.createQuerySimplificationAction(error)
          );
          break;

        case '42703': // undefined_column
          actions.push(
            this.createSchemaAdjustmentAction(error),
            this.createColumnMappingAction(error)
          );
          break;

        case '42883': // undefined_function
          actions.push(this.createFunctionReplacementAction(error));
          break;

        case '54000': // program_limit_exceeded (e.g., too many columns)
          actions.push(this.createQuerySimplificationAction(error));
          break;
      }
    }

    return actions;
  }

  private analyzeNodeValidationError(error: any): RecoveryAction[] {
    return [
      this.createSchemaAdjustmentAction(error),
      this.createManualInterventionAction(error)
    ];
  }

  // ==================== RECOVERY ACTION CREATORS ====================

  private createAutoTypeCastAction(_error: AppError): RecoveryAction {
    return {
      strategy: RecoveryStrategy.AUTO_TYPE_CAST,
      description: 'Automatically add type casting to resolve type mismatches',
      implementation: async (err) => this.autoTypeCastRecovery(err),
      confidence: 0.8,
      estimatedTimeMs: 100
    };
  }

  private createAutoNullHandlingAction(_error: AppError): RecoveryAction {
    return {
      strategy: RecoveryStrategy.AUTO_NULL_HANDLING,
      description: 'Add COALESCE or NULL handling to prevent null errors',
      implementation: async (err) => this.autoNullHandlingRecovery(err),
      confidence: 0.9,
      estimatedTimeMs: 50
    };
  }

  private createSchemaAdjustmentAction(_error: AppError): RecoveryAction {
    return {
      strategy: RecoveryStrategy.AUTO_SCHEMA_ADJUSTMENT,
      description: 'Adjust schema mappings to resolve column mismatches',
      implementation: async (err) => this.schemaAdjustmentRecovery(err),
      confidence: 0.7,
      estimatedTimeMs: 200
    };
  }

  private createQuerySimplificationAction(_error: AppError): RecoveryAction {
    return {
      strategy: RecoveryStrategy.AUTO_QUERY_SIMPLIFICATION,
      description: 'Simplify complex queries to avoid PostgreSQL limitations',
      implementation: async (err) => this.querySimplificationRecovery(err),
      confidence: 0.6,
      estimatedTimeMs: 300
    };
  }

  private createColumnMappingAction(_error: AppError): RecoveryAction {
    return {
      strategy: RecoveryStrategy.AUTO_COLUMN_MAPPING,
      description: 'Suggest column mappings based on name and type similarity',
      implementation: async (err) => this.columnMappingRecovery(err),
      confidence: 0.75,
      estimatedTimeMs: 150
    };
  }

  private createFunctionReplacementAction(_error: AppError): RecoveryAction {
    return {
      strategy: RecoveryStrategy.AUTO_FUNCTION_REPLACEMENT,
      description: 'Replace unsupported functions with PostgreSQL equivalents',
      implementation: async (err) => this.functionReplacementRecovery(err),
      confidence: 0.85,
      estimatedTimeMs: 100
    };
  }

  private createManualInterventionAction(_error: AppError): RecoveryAction {
    return {
      strategy: RecoveryStrategy.MANUAL_INTERVENTION,
      description: 'Manual intervention required for complex error resolution',
      implementation: async (err) => this.manualInterventionRecovery(err),
      confidence: 1.0,
      estimatedTimeMs: 5000 // Estimated time for user to fix
    };
  }

  // ==================== RECOVERY IMPLEMENTATIONS ====================

  private async autoTypeCastRecovery(error: AppError): Promise<RecoveryResult> {
    const changes = [];
    
    if (error.type === 'connection' && error.context?.typeMismatch) {
      const { sourceType, targetType } = error.context.typeMismatch;
      
      // Suggest type casting
      const castExpression = this.suggestTypeCast(sourceType, targetType);
      
      changes.push({
        type: 'type_cast_addition',
        description: `Added type cast from ${sourceType} to ${targetType}`,
        before: null,
        after: castExpression
      });
    }

    return {
      success: changes.length > 0,
      changes,
      warnings: []
    };
  }

  private async autoNullHandlingRecovery(error: AppError): Promise<RecoveryResult> {
    const changes = [];
    
    if (error.type === 'postgresql_execution' && error.column) {
      changes.push({
        type: 'null_handling_addition',
        description: `Added COALESCE for column ${error.column}`,
        before: error.column,
        after: `COALESCE(${error.column}, '')`
      });
    }

    return {
      success: changes.length > 0,
      changes,
      warnings: []
    };
  }

  private async schemaAdjustmentRecovery(error: AppError): Promise<RecoveryResult> {
    const changes = [];
    
    if (error.type === 'connection' && error.context?.schemaMismatch) {
      const { sourceColumns, targetColumns } = error.context.schemaMismatch;
      
      // Find matching columns by name similarity
      const suggestedMappings = this.suggestColumnMappings(sourceColumns, targetColumns);
      
      changes.push({
        type: 'schema_mapping_addition',
        description: 'Added suggested column mappings',
        before: [],
        after: suggestedMappings
      });
    }

    return {
      success: changes.length > 0,
      changes,
      warnings: []
    };
  }

  private async querySimplificationRecovery(error: AppError): Promise<RecoveryResult> {
    const changes = [];
    
    if (error.type === 'sql_generation' && error.sqlSnippet) {
      // Simplify complex WHERE clauses
      const simplified = this.simplifySQL(error.sqlSnippet);
      
      changes.push({
        type: 'query_simplification',
        description: 'Simplified complex SQL expression',
        before: error.sqlSnippet,
        after: simplified
      });
    }

    return {
      success: changes.length > 0,
      changes,
      warnings: []
    };
  }

  private async columnMappingRecovery(error: AppError): Promise<RecoveryResult> {
    const changes = [];
    
    if (error.type === 'connection' && error.context?.columns) {
      const mappings = this.generateColumnMappings(error.context.columns);
      
      changes.push({
        type: 'column_mapping_generation',
        description: 'Generated automatic column mappings',
        before: [],
        after: mappings
      });
    }

    return {
      success: changes.length > 0,
      changes,
      warnings: []
    };
  }

  private async functionReplacementRecovery(error: AppError): Promise<RecoveryResult> {
    const changes = [];
    
    if (error.type === 'sql_generation' && error.context?.functionName) {
      const replacement = this.getPostgreSQLFunctionReplacement(error.context.functionName);
      
      if (replacement) {
        changes.push({
          type: 'function_replacement',
          description: `Replaced ${error.context.functionName} with PostgreSQL equivalent`,
          before: error.context.functionName,
          after: replacement
        });
      }
    }

    return {
      success: changes.length > 0,
      changes,
      warnings: []
    };
  }

  private async manualInterventionRecovery(_error: AppError): Promise<RecoveryResult> {
    return {
      success: false,
      changes: [],
      warnings: ['Manual intervention required for this error type']
    };
  }

  // ==================== HELPER METHODS ====================

  private suggestTypeCast(sourceType: string, targetType: string): string {
    const typeMap: Record<string, Record<string, string>> = {
      'integer': {
        'varchar': '::varchar',
        'numeric': '::numeric',
        'float': '::float'
      },
      'varchar': {
        'integer': '::integer',
        'numeric': '::numeric',
        'date': '::date'
      },
      'date': {
        'timestamp': '::timestamp',
        'varchar': '::varchar'
      }
    };

    return typeMap[sourceType]?.[targetType] || '::' + targetType;
  }

  private suggestColumnMappings(sourceColumns: any[], targetColumns: any[]): any[] {
    const mappings = [];
    
    for (const source of sourceColumns) {
      // Find best matching target column
      const bestMatch = targetColumns.reduce((best, target) => {
        const score = this.calculateColumnSimilarity(source, target);
        return score > best.score ? { column: target, score } : best;
      }, { column: null, score: 0 });

      if (bestMatch.score > 0.5) {
        mappings.push({
          sourceColumn: source.name,
          targetColumn: bestMatch.column.name,
          confidence: bestMatch.score
        });
      }
    }

    return mappings;
  }

  private calculateColumnSimilarity(col1: any, col2: any): number {
    let score = 0;
    
    // Name similarity
    const name1 = col1.name.toLowerCase();
    const name2 = col2.name.toLowerCase();
    
    if (name1 === name2) score += 0.4;
    else if (name1.includes(name2) || name2.includes(name1)) score += 0.3;
    else if (this.levenshteinDistance(name1, name2) < 3) score += 0.2;

    // Type similarity
    if (col1.dataType === col2.dataType) score += 0.3;
    else if (this.areTypesCompatible(col1.dataType, col2.dataType)) score += 0.2;

    return Math.min(score, 1.0);
  }

  private levenshteinDistance(a: string, b: string): number {
    const matrix = Array(b.length + 1).fill(null).map(() => Array(a.length + 1).fill(null));

    for (let i = 0; i <= a.length; i++) matrix[0][i] = i;
    for (let j = 0; j <= b.length; j++) matrix[j][0] = j;

    for (let j = 1; j <= b.length; j++) {
      for (let i = 1; i <= a.length; i++) {
        const indicator = a[i - 1] === b[j - 1] ? 0 : 1;
        matrix[j][i] = Math.min(
          matrix[j][i - 1] + 1,
          matrix[j - 1][i] + 1,
          matrix[j - 1][i - 1] + indicator
        );
      }
    }

    return matrix[b.length][a.length];
  }

  private areTypesCompatible(type1: string, type2: string): boolean {
    const compatibleGroups = [
      ['integer', 'bigint', 'numeric', 'decimal'],
      ['varchar', 'text', 'char'],
      ['date', 'timestamp', 'timestamptz'],
      ['boolean', 'bool']
    ];

    return compatibleGroups.some(group => 
      group.includes(type1.toLowerCase()) && group.includes(type2.toLowerCase())
    );
  }

  private simplifySQL(sql: string): string {
    // Remove unnecessary parentheses
    let simplified = sql.replace(/\(\(([^)]+)\)\)/g, '($1)');
    
    // Simplify AND/OR chains
    simplified = simplified.replace(/(\w+)\s*=\s*(\w+)\s+AND\s+\1\s*=\s*(\w+)/g, '$1 IN ($2, $3)');
    
    // Replace multiple ORs with IN
    simplified = simplified.replace(/(\w+)\s*=\s*(\w+)\s+OR\s+(\w+)\s*=\s*(\w+)/g, (match, col1, val1, col2, val2) => {
      if (col1 === col2) return `${col1} IN (${val1}, ${val2})`;
      return match;
    });

    return simplified;
  }

  private generateColumnMappings(columns: any[]): SchemaMapping[] {
    return columns.map(col => ({
      sourceColumn: col.name,
      targetColumn: col.name,
      dataTypeConversion: col.dataTypeConversion,
      transformation: col.transformation,
      defaultValue: col.defaultValue
    }));
  }

  private getPostgreSQLFunctionReplacement(functionName: string): string | null {
    const replacements: Record<string, string> = {
      'GETDATE': 'CURRENT_TIMESTAMP',
      'LEN': 'LENGTH',
      'SUBSTRING': 'SUBSTR',
      'ISNULL': 'COALESCE',
      'CONVERT': 'CAST',
      'TOP': 'LIMIT'
    };

    return replacements[functionName.toUpperCase()] || null;
  }

  private getRecoveryImplementation(strategy: RecoveryStrategy): 
    ((error: AppError, context: any) => Promise<RecoveryResult>) | null {
    
    const implementations = {
      [RecoveryStrategy.AUTO_TYPE_CAST]: this.autoTypeCastRecovery.bind(this),
      [RecoveryStrategy.AUTO_NULL_HANDLING]: this.autoNullHandlingRecovery.bind(this),
      [RecoveryStrategy.AUTO_SCHEMA_ADJUSTMENT]: this.schemaAdjustmentRecovery.bind(this),
      [RecoveryStrategy.AUTO_QUERY_SIMPLIFICATION]: this.querySimplificationRecovery.bind(this),
      [RecoveryStrategy.AUTO_COLUMN_MAPPING]: this.columnMappingRecovery.bind(this),
      [RecoveryStrategy.AUTO_FUNCTION_REPLACEMENT]: this.functionReplacementRecovery.bind(this),
      [RecoveryStrategy.MANUAL_INTERVENTION]: this.manualInterventionRecovery.bind(this),
    };

    return implementations[strategy] || null;
  }
}

// ==================== BATCH RECOVERY RESULT ====================

interface BatchRecoveryResult {
  totalErrors: number;
  recoveredErrors: string[];
  results: RecoveryResult[];
}

// ==================== GLOBAL ERROR RECOVERY SERVICE ====================

let globalErrorRecoveryService: ErrorRecoveryService | null = null;

export function getErrorRecoveryService(): ErrorRecoveryService {
  if (!globalErrorRecoveryService) {
    globalErrorRecoveryService = new ErrorRecoveryService();
  }
  return globalErrorRecoveryService;
}

export function setErrorRecoveryService(service: ErrorRecoveryService): void {
  globalErrorRecoveryService = service;
}