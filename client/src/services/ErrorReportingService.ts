// src/services/ErrorReportingService.ts

import {
  AppError,
  ErrorReport,
  ErrorContext,
  ErrorCode,
  ErrorSeverity,
  ErrorFactory  // Add this import
} from '../types/error-types';

/**
 * Service for error reporting and analytics
 * Sends errors to backend, analytics, and logging services
 */
export class ErrorReportingService {
  private backendEndpoint: string;
  private analyticsEndpoint: string;
  private logEndpoint: string;
  private errorQueue: ErrorReport[] = [];
  private isProcessing: boolean = false;
  private maxRetries: number = 3;
  private retryDelay: number = 1000; // ms

  constructor() {
    this.backendEndpoint = process.env.REACT_APP_ERROR_REPORTING_ENDPOINT || '/api/errors';
    this.analyticsEndpoint = process.env.REACT_APP_ANALYTICS_ENDPOINT || '/api/analytics';
    this.logEndpoint = process.env.REACT_APP_LOG_ENDPOINT || '/api/logs';
  }

  // ==================== PUBLIC API ====================

  /**
   * Report an error to all configured endpoints
   */
  public async reportError(
    error: AppError,
    context: Partial<ErrorContext>
  ): Promise<void> {
    const report = this.createErrorReport(error, context);
    
    // Add to queue
    this.errorQueue.push(report);
    
    // Process queue asynchronously
    this.processQueue();
    
    // Log to console in development
    if (process.env.NODE_ENV === 'development') {
      this.logToConsole(report);
    }
  }

  /**
   * Report multiple errors in batch
   */
  public async reportErrors(
    errors: AppError[],
    context: Partial<ErrorContext>
  ): Promise<void> {
    const reports = errors.map(error => 
      this.createErrorReport(error, context)
    );
    
    this.errorQueue.push(...reports);
    this.processQueue();
  }

  /**
   * Get error analytics
   */
  public async getErrorAnalytics(
    startDate: Date,
    endDate: Date
  ): Promise<ErrorAnalytics> {
    try {
      const response = await fetch(
        `${this.analyticsEndpoint}/errors?start=${startDate.toISOString()}&end=${endDate.toISOString()}`
      );
      return await response.json();
    } catch (error) {
      console.error('Failed to fetch error analytics:', error);
      return this.getLocalAnalytics(startDate, endDate);
    }
  }

  /**
   * Clear error queue
   */
  public clearQueue(): void {
    this.errorQueue = [];
  }

  /**
   * Get queue size
   */
  public getQueueSize(): number {
    return this.errorQueue.length;
  }

  // ==================== PRIVATE METHODS ====================

  private async processQueue(): Promise<void> {
    if (this.isProcessing || this.errorQueue.length === 0) {
      return;
    }

    this.isProcessing = true;

    try {
      while (this.errorQueue.length > 0) {
        const report = this.errorQueue.shift();
        if (!report) continue;

        await this.sendToBackend(report);
        await this.sendToAnalytics(report);
        await this.sendToLogs(report);
      }
    } catch (error) {
      console.error('Error processing error queue:', error);
    } finally {
      this.isProcessing = false;
    }
  }

  private async sendToBackend(
    report: ErrorReport,
    retryCount: number = 0
  ): Promise<void> {
    try {
      const response = await fetch(this.backendEndpoint, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Error-Source': 'sql-generator-client'
        },
        body: JSON.stringify(report)
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      console.log('Error reported to backend:', report.error.id);
    } catch (error) {
      if (retryCount < this.maxRetries) {
        await this.delay(this.retryDelay * (retryCount + 1));
        await this.sendToBackend(report, retryCount + 1);
      } else {
        console.error('Failed to report error to backend after retries:', error);
        this.storeForLater(report);
      }
    }
  }

  private async sendToAnalytics(report: ErrorReport): Promise<void> {
    try {
      const analyticsData = this.extractAnalyticsData(report);
      
      await fetch(this.analyticsEndpoint, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(analyticsData)
      });
    } catch (error) {
      console.warn('Failed to send error to analytics:', error);
    }
  }

  private async sendToLogs(report: ErrorReport): Promise<void> {
    try {
      const logData = this.extractLogData(report);
      
      await fetch(this.logEndpoint, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(logData)
      });
    } catch (error) {
      console.warn('Failed to send error to logs:', error);
    }
  }

  private createErrorReport(
    error: AppError,
    context: Partial<ErrorContext>
  ): ErrorReport {
    const fullContext: ErrorContext = {
      environment: process.env.NODE_ENV as any || 'development',
      component: 'unknown',
      ...context
    };

    return {
      error,
      context: fullContext,
      metadata: {
        reportedAt: new Date().toISOString(),
        source: 'client',
        userAgent: navigator.userAgent,
        platform: navigator.platform
      },
      analytics: {
        errorCount: 1,
        firstOccurrence: error.timestamp,
        lastOccurrence: error.timestamp,
        frequency: 0
      }
    };
  }

  private extractAnalyticsData(report: ErrorReport): any {
    return {
      event: 'error_occurred',
      error_code: report.error.code,
      error_type: report.error.type,
      severity: report.error.severity,
      component: report.context.component,
      environment: report.context.environment,
      timestamp: report.metadata.reportedAt,
      user_agent: report.metadata.userAgent,
      platform: report.metadata.platform,
      session_id: report.context.sessionId,
      user_id: report.context.userId
    };
  }

  private extractLogData(report: ErrorReport): any {
    return {
      level: this.getLogLevel(report.error.severity),
      message: report.error.message,
      error_code: ErrorCode[report.error.code],
      stack_trace: report.error.stackTrace,
      context: report.error.context,
      metadata: {
        reported_at: report.metadata.reportedAt,
        source: report.metadata.source
      }
    };
  }

  private getLogLevel(severity: ErrorSeverity): string {
    switch (severity) {
      case ErrorSeverity.INFO: return 'info';
      case ErrorSeverity.WARNING: return 'warn';
      case ErrorSeverity.ERROR: return 'error';
      case ErrorSeverity.CRITICAL: return 'critical';
      default: return 'error';
    }
  }

  private storeForLater(report: ErrorReport): void {
    try {
      const storedErrors = this.getStoredErrors();
      storedErrors.push(report);
      
      localStorage.setItem('pending_error_reports', JSON.stringify(storedErrors));
      
      // Limit stored errors to prevent storage overflow
      if (storedErrors.length > 100) {
        storedErrors.splice(0, 50); // Keep last 50 errors
      }
    } catch (error) {
      console.error('Failed to store error for later:', error);
    }
  }

  private getStoredErrors(): ErrorReport[] {
    try {
      const stored = localStorage.getItem('pending_error_reports');
      return stored ? JSON.parse(stored) : [];
    } catch {
      return [];
    }
  }

  private async retryPendingErrors(): Promise<void> {
    const pendingErrors = this.getStoredErrors();
    
    if (pendingErrors.length === 0) return;

    console.log(`Retrying ${pendingErrors.length} pending error reports...`);

    for (const report of pendingErrors) {
      try {
        await this.sendToBackend(report);
        await this.removeStoredError(report);
      } catch (error) {
        console.warn('Failed to retry pending error:', error);
      }
    }
  }

  private removeStoredError(report: ErrorReport): void {
    const storedErrors = this.getStoredErrors();
    const index = storedErrors.findIndex(e => e.error.id === report.error.id);
    
    if (index !== -1) {
      storedErrors.splice(index, 1);
      localStorage.setItem('pending_error_reports', JSON.stringify(storedErrors));
    }
  }

  private getLocalAnalytics(
    startDate: Date,
    endDate: Date
  ): ErrorAnalytics {
    const storedErrors = this.getStoredErrors();
    const filteredErrors = storedErrors.filter(report => {
      const reportedAt = new Date(report.metadata.reportedAt);
      return reportedAt >= startDate && reportedAt <= endDate;
    });

    const errorCounts: Record<ErrorCode, number> = {} as any;
    const severityCounts: Record<ErrorSeverity, number> = {} as any;
    const componentCounts: Record<string, number> = {};

    filteredErrors.forEach(report => {
      // Count errors by code
      errorCounts[report.error.code] = (errorCounts[report.error.code] || 0) + 1;
      
      // Count errors by severity
      severityCounts[report.error.severity] = (severityCounts[report.error.severity] || 0) + 1;
      
      // Count errors by component
      const component = report.context.component;
      componentCounts[component] = (componentCounts[component] || 0) + 1;
    });

    return {
      period: {
        start: startDate.toISOString(),
        end: endDate.toISOString()
      },
      totalErrors: filteredErrors.length,
      errorCounts,
      severityCounts,
      componentCounts,
      mostCommonError: this.getMostCommonError(errorCounts),
      averageFrequency: this.calculateFrequency(filteredErrors)
    };
  }

  private getMostCommonError(errorCounts: Record<ErrorCode, number>): {
    code: ErrorCode;
    count: number;
    percentage: number;
  } | null {
    if (Object.keys(errorCounts).length === 0) return null;

    const totalErrors = Object.values(errorCounts).reduce((a, b) => a + b, 0);
    const [code, count] = Object.entries(errorCounts).reduce(
      (max, [code, count]) => count > max[1] ? [parseInt(code), count] : max,
      [0, 0]
    );

    return {
      code: code as ErrorCode,
      count,
      percentage: (count / totalErrors) * 100
    };
  }

  private calculateFrequency(errors: ErrorReport[]): number {
    if (errors.length < 2) return 0;

    const timestamps = errors
      .map(report => new Date(report.metadata.reportedAt).getTime())
      .sort((a, b) => a - b);

    const totalDuration = timestamps[timestamps.length - 1] - timestamps[0];
    const hours = totalDuration / (1000 * 60 * 60);

    return hours > 0 ? errors.length / hours : 0;
  }

  private logToConsole(report: ErrorReport): void {
    const { error } = report;
    
    const styles = {
      error: 'color: #d32f2f; font-weight: bold;',
      warning: 'color: #f57c00; font-weight: bold;',
      info: 'color: #1976d2; font-weight: bold;',
      critical: 'color: #ad1457; font-weight: bold;'
    };

    const style = styles[error.severity.toLowerCase() as keyof typeof styles] || styles.error;

    console.groupCollapsed(`%c${ErrorCode[error.code]} - ${error.message}`, style);
    console.log('Error Details:', error);
    console.log('Context:', report.context);
    console.log('Metadata:', report.metadata);
    console.groupEnd();
  }

  private delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  // ==================== INITIALIZATION ====================

  /**
   * Initialize error reporting service
   */
  public initialize(): void {
    // Set up periodic retry of pending errors
    setInterval(() => this.retryPendingErrors(), 5 * 60 * 1000); // Every 5 minutes

    // Set up beforeunload handler to report pending errors
    window.addEventListener('beforeunload', () => {
      if (this.errorQueue.length > 0) {
        this.reportErrorsSync();
      }
    });

// In the window error handler
window.addEventListener('error', (event) => {
  const error = ErrorFactory.createConnectionError(
    ErrorCode.SYSTEM_UNEXPECTED_ERROR,
    event.message,
    {
      context: {  // Pass the properties in the context object
        filename: event.filename,
        lineno: event.lineno,
        colno: event.colno
      }
    }
  );
  
  this.reportError(error, {
    component: 'window_error_handler'
  });
});

// In the global promise rejection handler
window.addEventListener('unhandledrejection', (event) => {
  const error = ErrorFactory.createConnectionError(
    ErrorCode.SYSTEM_UNEXPECTED_ERROR,
    event.reason?.message || 'Unhandled promise rejection',
    {
      context: {  // Pass the reason in the context object
        reason: event.reason
      }
    }
  );
  
  this.reportError(error, {
    component: 'unhandled_rejection'
  });
});

    console.log('Error reporting service initialized');
  }

  private reportErrorsSync(): void {
    // In a real implementation, this would use sendBeacon or similar
    // For now, we'll just log to console
    console.log(`Reporting ${this.errorQueue.length} errors synchronously`);
  }
}

// ==================== ANALYTICS INTERFACES ====================

interface ErrorAnalytics {
  period: {
    start: string;
    end: string;
  };
  totalErrors: number;
  errorCounts: Record<ErrorCode, number>;
  severityCounts: Record<ErrorSeverity, number>;
  componentCounts: Record<string, number>;
  mostCommonError: {
    code: ErrorCode;
    count: number;
    percentage: number;
  } | null;
  averageFrequency: number;
}

// ==================== GLOBAL ERROR REPORTING SERVICE ====================

let globalErrorReportingService: ErrorReportingService | null = null;

export function getErrorReportingService(): ErrorReportingService {
  if (!globalErrorReportingService) {
    globalErrorReportingService = new ErrorReportingService();
    globalErrorReportingService.initialize();
  }
  return globalErrorReportingService;
}

export function setErrorReportingService(service: ErrorReportingService): void {
  globalErrorReportingService = service;
}