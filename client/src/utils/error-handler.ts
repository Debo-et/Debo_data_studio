// src/utils/error-handler.ts

import {
  AppError,
  ConnectionError,
  SQLGenerationError,
  PostgreSQLExecutionError,
  ErrorCode,
  ErrorSeverity,
  ErrorContext
} from '../types/error-types';
import { getErrorRecoveryService } from '../services/ErrorRecoveryService';
import { getErrorReportingService } from '../services/ErrorReportingService';

/**
 * Centralized error handling utilities for PostgreSQL SQL Generator
 * Provides consistent error creation, logging, and recovery
 */

// ==================== ERROR CREATION ====================

export class ErrorHandler {
  private static instance: ErrorHandler;
  private recoveryService = getErrorRecoveryService();
  private reportingService = getErrorReportingService();

  private constructor() {}

  static getInstance(): ErrorHandler {
    if (!ErrorHandler.instance) {
      ErrorHandler.instance = new ErrorHandler();
    }
    return ErrorHandler.instance;
  }

  // ==================== ERROR CREATION METHODS ====================

  createConnectionError(
    code: ErrorCode,
    message: string,
    context: Partial<ConnectionError> = {}
  ): ConnectionError {
    const error: ConnectionError = {
      id: `conn-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
      code,
      severity: this.determineSeverity(code, context),
      message,
      timestamp: new Date().toISOString(),
      context: context.context || {},
      userFriendlyMessage: this.createUserFriendlyMessage(code, message, context),
      type: 'connection',
      connectionId: context.connectionId,
      sourceNodeId: context.sourceNodeId,
      targetNodeId: context.targetNodeId,
      validationResult: context.validationResult
    };

    // Add stack trace in development
    if (process.env.NODE_ENV === 'development') {
      error.stackTrace = new Error().stack;
    }

    return error;
  }

  createSQLGenerationError(
    code: ErrorCode,
    message: string,
    context: Partial<SQLGenerationError> = {}
  ): SQLGenerationError {
    const error: SQLGenerationError = {
      id: `sql-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
      code,
      severity: this.determineSeverity(code, context),
      message,
      timestamp: new Date().toISOString(),
      context: context.context || {},
      userFriendlyMessage: this.createUserFriendlyMessage(code, message, context),
      type: 'sql_generation',
      nodeId: context.nodeId,
      fragmentType: context.fragmentType,
      sqlSnippet: context.sqlSnippet,
      lineNumber: context.lineNumber,
      columnNumber: context.columnNumber
    };

    if (process.env.NODE_ENV === 'development') {
      error.stackTrace = new Error().stack;
    }

    return error;
  }

  createPostgreSQLExecutionError(
    code: ErrorCode,
    message: string,
    context: Partial<PostgreSQLExecutionError> = {}
  ): PostgreSQLExecutionError {
    const error: PostgreSQLExecutionError = {
      id: `pg-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
      code,
      severity: this.determineSeverity(code, context),
      message,
      timestamp: new Date().toISOString(),
      context: context.context || {},
      userFriendlyMessage: this.createUserFriendlyMessage(code, message, context),
      type: 'postgresql_execution',
      query: context.query,
      postgresErrorCode: context.postgresErrorCode,
      schema: context.schema,
      table: context.table,
      column: context.column,
      constraint: context.constraint
    };

    if (process.env.NODE_ENV === 'development') {
      error.stackTrace = new Error().stack;
    }

    return error;
  }

  // ==================== ERROR HANDLING METHODS ====================

  async handleError(
    error: AppError,
    context: ErrorContext,
    options: {
      autoRecover?: boolean;
      reportError?: boolean;
      logToConsole?: boolean;
    } = {}
  ): Promise<HandleErrorResult> {
    const {
      autoRecover = true,
      reportError = true,
      logToConsole = process.env.NODE_ENV === 'development'
    } = options;

    const result: HandleErrorResult = {
      error,
      handled: false,
      recovered: false,
      recoveryResult: null,
      reported: false
    };

    try {
      // Log to console if enabled
      if (logToConsole) {
        this.logErrorToConsole(error, context);
      }

      // Report error if enabled
      if (reportError) {
        await this.reportingService.reportError(error, context);
        result.reported = true;
      }

      // Attempt auto-recovery if enabled
      if (autoRecover && this.shouldAttemptRecovery(error)) {
        const recoveryContext = {
          nodes: context.node ? [{
            id: context.node.id,
            type: context.node.type,
            name: context.node.name,
            position: { x: 0, y: 0 }, // Default position
            size: { width: 200, height: 100 }, // Default size
            ...(context as any).fullNode // If full node is available in context
          }] : [],
          connections: context.connection ? [{
            id: context.connection.id,
            sourceNodeId: context.connection.sourceId,
            sourcePortId: `port-${context.connection.sourceId}-out`,
            targetNodeId: context.connection.targetId,
            targetPortId: `port-${context.connection.targetId}-in`,
            ...(context as any).fullConnection // If full connection is available in context
          }] : []
        };

        const recoveryResult = await this.recoveryService.attemptAutoRecovery(error, recoveryContext);

        result.recoveryResult = recoveryResult;
        result.recovered = recoveryResult.success;
        result.handled = true;
      }

      // If not recovered, determine if manual intervention is needed
      if (!result.recovered) {
        result.handled = this.determineIfHandled(error);
      }

    } catch (handlingError) {
      console.error('Error handling failed:', handlingError);
      result.handled = false;
    }

    return result;
  }

  async handleErrors(
    errors: AppError[],
    context: ErrorContext,
    options: {
      stopOnCritical?: boolean;
      batchRecovery?: boolean;
    } = {}
  ): Promise<HandleErrorResult[]> {
    const {
      stopOnCritical = true,
      batchRecovery = false
    } = options;

    const results: HandleErrorResult[] = [];
    let shouldStop = false;

    for (const error of errors) {
      if (shouldStop) {
        results.push({
          error,
          handled: false,
          recovered: false,
          recoveryResult: null,
          reported: false,
          skipped: true
        });
        continue;
      }

      const result = await this.handleError(error, context, {
        autoRecover: !batchRecovery,
        reportError: true
      });

      results.push(result);

      // Stop on critical errors if configured
      if (stopOnCritical && error.severity === ErrorSeverity.CRITICAL) {
        shouldStop = true;
      }
    }

    // Attempt batch recovery if enabled
    if (batchRecovery && results.some(r => !r.recovered)) {
      const unrecoveredErrors = results
        .filter(r => !r.recovered && !r.skipped)
        .map(r => r.error);

      if (unrecoveredErrors.length > 0) {
        const batchResult = await this.recoveryService.attemptBatchRecovery(
          unrecoveredErrors,
          context
        );

        // Update results with batch recovery
        results.forEach(result => {
          if (batchResult.recoveredErrors.includes(result.error.id)) {
            result.recovered = true;
            result.handled = true;
          }
        });
      }
    }

    return results;
  }

  // ==================== ERROR UTILITY METHODS ====================

  isConnectionError(error: any): error is ConnectionError {
    return error?.type === 'connection';
  }

  isSQLGenerationError(error: any): error is SQLGenerationError {
    return error?.type === 'sql_generation';
  }

  isPostgreSQLExecutionError(error: any): error is PostgreSQLExecutionError {
    return error?.type === 'postgresql_execution';
  }

  isRecoverableError(error: AppError): boolean {
    const nonRecoverableCodes = [
      ErrorCode.CONNECTION_CIRCULAR_DEPENDENCY,
      ErrorCode.PIPELINE_CYCLIC_DEPENDENCY,
      ErrorCode.SYSTEM_UNEXPECTED_ERROR,
      ErrorCode.SYSTEM_CONFIGURATION_ERROR
    ];

    return !nonRecoverableCodes.includes(error.code);
  }

  shouldLogError(error: AppError): boolean {
    // Always log critical errors
    if (error.severity === ErrorSeverity.CRITICAL) {
      return true;
    }

    // Log errors in development
    if (process.env.NODE_ENV === 'development') {
      return true;
    }

    // Only log errors and warnings in production
    return error.severity === ErrorSeverity.ERROR || 
           error.severity === ErrorSeverity.WARNING;
  }

  // ==================== PRIVATE METHODS ====================

  private determineSeverity(code: ErrorCode, context: any): ErrorSeverity {
    const codeRange = Math.floor(code / 1000);
    
    switch (codeRange) {
      case 1: // Connection errors
        if (context?.connectionId) {
          return ErrorSeverity.ERROR;
        }
        return ErrorSeverity.WARNING;
        
      case 2: // SQL generation errors
        if (context?.nodeId) {
          return ErrorSeverity.ERROR;
        }
        return ErrorSeverity.WARNING;
        
      case 3: // Node validation errors
        return ErrorSeverity.WARNING;
        
      case 4: // PostgreSQL execution errors
        if (context?.postgresErrorCode?.startsWith('23')) { // Constraint violations
          return ErrorSeverity.ERROR;
        }
        if (context?.postgresErrorCode?.startsWith('42')) { // Syntax errors
          return ErrorSeverity.CRITICAL;
        }
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

  private createUserFriendlyMessage(
    code: ErrorCode,
    technicalMessage: string,
    context: any
  ): string {
    let message = '';
    
    switch (code) {
      case ErrorCode.CONNECTION_TYPE_MISMATCH:
        message = `Data type mismatch between connected nodes.`;
        if (context.sourceType && context.targetType) {
          message += ` (${context.sourceType} → ${context.targetType})`;
        }
        break;
        
      case ErrorCode.CONNECTION_SCHEMA_MISMATCH:
        message = `Schema mismatch between nodes.`;
        if (context.sourceColumns && context.targetColumns) {
          message += ` Found ${context.sourceColumns.length} source columns, ${context.targetColumns.length} target columns.`;
        }
        break;
        
      case ErrorCode.SQL_TYPE_MISMATCH:
        message = `Type mismatch in SQL expression.`;
        if (context.expression) {
          message += ` Check: ${context.expression}`;
        }
        break;
        
      case ErrorCode.SQL_FUNCTION_NOT_SUPPORTED:
        message = `Function not supported by the target database.`;
        if (context.functionName) {
          message += ` Function: ${context.functionName}`;
        }
        break;
        
      case ErrorCode.POSTGRESQL_SYNTAX_ERROR:
        message = `PostgreSQL syntax error detected.`;
        if (context.lineNumber && context.columnNumber) {
          message += ` At line ${context.lineNumber}, column ${context.columnNumber}`;
        }
        break;
        
      default:
        message = technicalMessage;
    }
    
    return message;
  }

  private shouldAttemptRecovery(error: AppError): boolean {
    // Don't attempt recovery for critical system errors
    if (error.severity === ErrorSeverity.CRITICAL) {
      return false;
    }
    
    // Don't attempt recovery for circular dependencies
    if (error.code === ErrorCode.CONNECTION_CIRCULAR_DEPENDENCY ||
        error.code === ErrorCode.PIPELINE_CYCLIC_DEPENDENCY) {
      return false;
    }
    
    // Attempt recovery for other errors
    return this.isRecoverableError(error);
  }

  private determineIfHandled(error: AppError): boolean {
    // Errors with recovery suggestions are considered handled
    const recoveryActions = this.recoveryService.analyzeError(error);
    return recoveryActions.length > 0;
  }

  private logErrorToConsole(error: AppError, context: ErrorContext): void {
    const styles = {
      error: 'background: #ffebee; color: #d32f2f; padding: 2px 4px; border-radius: 2px;',
      warning: 'background: #fff3e0; color: #f57c00; padding: 2px 4px; border-radius: 2px;',
      info: 'background: #e3f2fd; color: #1976d2; padding: 2px 4px; border-radius: 2px;',
      critical: 'background: #fce4ec; color: #ad1457; padding: 2px 4px; border-radius: 2px; font-weight: bold;'
    };

    const style = styles[error.severity.toLowerCase() as keyof typeof styles];
    
    console.groupCollapsed(
      `%c${ErrorCode[error.code]}%c ${error.userFriendlyMessage}`,
      style,
      'font-weight: normal;'
    );
    
    console.log('Error Details:', error);
    console.log('Context:', context);
    console.log('Timestamp:', new Date(error.timestamp).toLocaleString());
    
    if (error.stackTrace) {
      console.log('Stack Trace:', error.stackTrace);
    }
    
    console.groupEnd();
  }
}

// ==================== RESULT INTERFACES ====================

interface HandleErrorResult {
  error: AppError;
  handled: boolean;
  recovered: boolean;
  recoveryResult: any | null;
  reported: boolean;
  skipped?: boolean;
}

// ==================== GLOBAL ERROR HANDLER ====================

let globalErrorHandler: ErrorHandler | null = null;

export function getErrorHandler(): ErrorHandler {
  if (!globalErrorHandler) {
    globalErrorHandler = ErrorHandler.getInstance();
  }
  return globalErrorHandler;
}

export function setErrorHandler(handler: ErrorHandler): void {
  globalErrorHandler = handler;
}

// ==================== ERROR BOUNDARY UTILITIES ====================

// Remove or comment out the withErrorBoundary function if you don't have ConnectionErrorBoundary component
// export function withErrorBoundary<P extends object>(
//   Component: React.ComponentType<P>,
//   errorBoundaryProps?: any
// ): React.ComponentType<P> {
//   return function WithErrorBoundaryWrapper(props: P) {
//     return (
//       <ConnectionErrorBoundary {...errorBoundaryProps}>
//         <Component {...props} />
//       </ConnectionErrorBoundary>
//     );
//   };
// }

// Alternative: Create a simpler error boundary utility that doesn't depend on a specific component
export function createErrorBoundary(Component: React.ComponentType<any>) {
  return Component; // Simple passthrough for now
}

export function createErrorContext(
  component: string,
  node?: any,
  connection?: any,
  pipeline?: any
): ErrorContext {
  return {
    component,
    environment: process.env.NODE_ENV as any || 'development',
    node: node ? {
      id: node.id,
      type: node.type,
      name: node.name
    } : undefined,
    connection: connection ? {
      id: connection.id,
      sourceId: connection.sourceNodeId,
      targetId: connection.targetNodeId
    } : undefined,
    pipeline: pipeline ? {
      id: pipeline.id,
      name: pipeline.name,
      stage: pipeline.stage
    } : undefined,
    // Include full objects for recovery service
    fullNode: node,
    fullConnection: connection
  };
}

// ==================== ERROR POLYFILLS ====================

// Ensure global error handlers are set up
if (typeof window !== 'undefined') {
  window.addEventListener('load', () => {
    
    console.log('Error handling system initialized');
  });
}