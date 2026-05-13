// src/components/ConnectionErrorBoundary.tsx

import { Component, ErrorInfo, ReactNode } from 'react';
import {
  AppError,
  ErrorBoundaryState,
  RecoveryAction,
  ErrorCode,
  ErrorSeverity
} from '../types/error-types';
import { getErrorRecoveryService, ErrorRecoveryService } from '../services/ErrorRecoveryService';
import { ErrorReportingService } from '../services/ErrorReportingService';
import { CanvasConnection } from '../types/pipeline-types';

// ==================== PROPS INTERFACE ====================

interface ConnectionErrorBoundaryProps {
  children: ReactNode;
  connection?: CanvasConnection;
  onError?: (error: AppError) => void;
  onRecovery?: (result: any) => void;
  showDetails?: boolean;
  autoRecover?: boolean;
  className?: string;
}

// ==================== ERROR BOUNDARY COMPONENT ====================

/**
 * Error Boundary specifically for connection-related errors
 * Provides user-friendly error messages and recovery options
 */
export class ConnectionErrorBoundary extends Component<
  ConnectionErrorBoundaryProps,
  ErrorBoundaryState
> {
  private errorRecoveryService: ErrorRecoveryService;
  private errorReportingService: ErrorReportingService;

  constructor(props: ConnectionErrorBoundaryProps) {
    super(props);
    
    this.errorRecoveryService = getErrorRecoveryService();
    this.errorReportingService = new ErrorReportingService();
    
    this.state = {
      hasError: false,
      error: null,
      recoveryAttempts: 0,
      recoveryStatus: 'idle',
      userActionRequired: false,
      recoverySuggestions: []
    };
  }

  // ==================== LIFECYCLE METHODS ====================

  static getDerivedStateFromError(error: any): Partial<ErrorBoundaryState> {
    // Convert generic error to AppError
    const appError = ErrorFactory.createConnectionError(
      ErrorCode.CONNECTION_VALIDATION_FAILED,
      error.message,
      { stackTrace: error.stack }
    );

    return {
      hasError: true,
      error: appError,
      recoveryStatus: 'idle',
      userActionRequired: true
    };
  }

  componentDidCatch(error: Error, errorInfo: ErrorInfo): void {
    // Convert to AppError
    const appError = ErrorFactory.createConnectionError(
      ErrorCode.CONNECTION_VALIDATION_FAILED,
      error.message,
      {
        stackTrace: error.stack,
        componentStack: errorInfo.componentStack
      }
    );

    // Update state
    this.setState({
      error: appError,
      recoverySuggestions: this.errorRecoveryService.analyzeError(appError)
    });

    // Report error
    this.errorReportingService.reportError(appError, {
      component: 'ConnectionErrorBoundary',
      environment: process.env.NODE_ENV as any
    });

    // Notify parent
    if (this.props.onError) {
      this.props.onError(appError);
    }

    // Attempt auto-recovery if enabled
    if (this.props.autoRecover) {
      this.attemptAutoRecovery();
    }
  }

  // ==================== RECOVERY METHODS ====================

  private attemptAutoRecovery = async (): Promise<void> => {
    if (!this.state.error) return;

    this.setState({ recoveryStatus: 'recovering' });

    try {
      const result = await this.errorRecoveryService.attemptAutoRecovery(
        this.state.error,
        {
          connections: this.props.connection ? [this.props.connection] : []
        }
      );

      if (result.success) {
        this.setState({
          recoveryStatus: 'recovered',
          userActionRequired: false
        });

        // Notify parent of recovery
        if (this.props.onRecovery) {
          this.props.onRecovery(result);
        }
      } else {
        this.setState({
          recoveryStatus: 'failed',
          recoveryAttempts: this.state.recoveryAttempts + 1
        });
      }
    } catch (recoveryError) {
      this.setState({
        recoveryStatus: 'failed',
        recoveryAttempts: this.state.recoveryAttempts + 1
      });
    }
  };

  private handleManualRecovery = async (action: RecoveryAction): Promise<void> => {
    if (!this.state.error) return;

    this.setState({ recoveryStatus: 'recovering' });

    try {
      const result = await action.implementation(this.state.error);

      if (result.success) {
        this.setState({
          recoveryStatus: 'recovered',
          userActionRequired: false
        });

        // Notify parent
        if (this.props.onRecovery) {
          this.props.onRecovery(result);
        }
      } else {
        this.setState({
          recoveryStatus: 'failed',
          recoveryAttempts: this.state.recoveryAttempts + 1
        });
      }
    } catch (error) {
      this.setState({
        recoveryStatus: 'failed',
        recoveryAttempts: this.state.recoveryAttempts + 1
      });
    }
  };

  private handleRetry = (): void => {
    this.setState({
      hasError: false,
      error: null,
      recoveryStatus: 'idle',
      recoveryAttempts: 0,
      userActionRequired: false,
      recoverySuggestions: []
    });
  };

  private handleIgnore = (): void => {
    this.setState({
      hasError: false,
      userActionRequired: false
    });
  };

  private handleReportBug = (): void => {
    if (this.state.error) {
      // Remove the userAction property that doesn't exist in ErrorContext
      this.errorReportingService.reportError(this.state.error, {
        component: 'ConnectionErrorBoundary',
        environment: process.env.NODE_ENV as any
      });
    }
  };

  // ==================== RENDER METHODS ====================

  private renderErrorDetails(): ReactNode {
    if (!this.state.error || !this.props.showDetails) return null;

    const { error } = this.state;

    return (
      <div className="error-details">
        <div className="error-header">
          <h3>Error Details</h3>
          <span className={`error-severity severity-${error.severity.toLowerCase()}`}>
            {error.severity}
          </span>
        </div>
        
        <div className="error-info">
          <p><strong>Code:</strong> {ErrorCode[error.code]}</p>
          <p><strong>Message:</strong> {error.message}</p>
          <p><strong>Time:</strong> {new Date(error.timestamp).toLocaleString()}</p>
          
          {error.context && Object.keys(error.context).length > 0 && (
            <div className="error-context">
              <strong>Context:</strong>
              <pre>{JSON.stringify(error.context, null, 2)}</pre>
            </div>
          )}
          
          {error.stackTrace && (
            <div className="error-stack">
              <strong>Stack Trace:</strong>
              <pre>{error.stackTrace}</pre>
            </div>
          )}
        </div>
      </div>
    );
  }

  private renderRecoverySuggestions(): ReactNode {
    if (this.state.recoverySuggestions.length === 0) return null;

    return (
      <div className="recovery-suggestions">
        <h4>Suggested Fixes</h4>
        <div className="suggestion-list">
          {this.state.recoverySuggestions.map((action, index) => (
            <div key={index} className="suggestion-item">
              <div className="suggestion-header">
                <span className="suggestion-title">{action.description}</span>
                <span className="confidence-badge">
                  Confidence: {(action.confidence * 100).toFixed(0)}%
                </span>
              </div>
              <div className="suggestion-footer">
                <span className="estimated-time">
                  Estimated: {action.estimatedTimeMs}ms
                </span>
                <button
                  className="apply-suggestion-btn"
                  onClick={() => this.handleManualRecovery(action)}
                  disabled={this.state.recoveryStatus === 'recovering'}
                >
                  Apply Fix
                </button>
              </div>
            </div>
          ))}
        </div>
      </div>
    );
  }

  private renderRecoveryStatus(): ReactNode {
    const { recoveryStatus } = this.state;

    if (recoveryStatus === 'idle') return null;

    const statusConfig = {
      recovering: {
        message: 'Attempting to recover...',
        className: 'status-recovering',
        icon: '🔄'
      },
      recovered: {
        message: 'Successfully recovered!',
        className: 'status-recovered',
        icon: '✅'
      },
      failed: {
        message: 'Recovery failed. Manual intervention may be required.',
        className: 'status-failed',
        icon: '❌'
      }
    };

    const config = statusConfig[recoveryStatus];

    return (
      <div className={`recovery-status ${config.className}`}>
        <span className="status-icon">{config.icon}</span>
        <span className="status-message">{config.message}</span>
      </div>
    );
  }

  private renderErrorContent(): ReactNode {
    const { error } = this.state;

    if (!error) return null;

    return (
      <div className={`connection-error-boundary ${this.props.className || ''}`}>
        <div className="error-container">
          <div className="error-header">
            <h2 className="error-title">Connection Error</h2>
            <div className="error-actions">
              <button
                className="error-action-btn retry-btn"
                onClick={this.handleRetry}
              >
                Retry Connection
              </button>
              <button
                className="error-action-btn ignore-btn"
                onClick={this.handleIgnore}
              >
                Ignore Error
              </button>
              <button
                className="error-action-btn report-btn"
                onClick={this.handleReportBug}
              >
                Report Bug
              </button>
            </div>
          </div>

          <div className="error-content">
            <div className="user-message">
              <div className="error-icon">⚠️</div>
              <div className="message-content">
                <p className="message-text">{error.userFriendlyMessage}</p>
                {error.recoverySuggestion && (
                  <p className="recovery-suggestion">
                    <strong>Suggestion:</strong> {error.recoverySuggestion}
                  </p>
                )}
              </div>
            </div>

            {this.renderRecoveryStatus()}
            {this.renderRecoverySuggestions()}
            {this.renderErrorDetails()}
          </div>

          {this.props.autoRecover && this.state.recoveryStatus === 'idle' && (
            <div className="auto-recovery-section">
              <button
                className="auto-recover-btn"
                onClick={this.attemptAutoRecovery}
              >
                Try Automatic Recovery
              </button>
            </div>
          )}
        </div>
      </div>
    );
  }

  render(): ReactNode {
    if (this.state.hasError) {
      return this.renderErrorContent();
    }

    return this.props.children;
  }
}

// ==================== CSS STYLES ====================

const styles = `
.connection-error-boundary {
  border: 2px solid #ff6b6b;
  border-radius: 8px;
  background: linear-gradient(135deg, #fff5f5 0%, #ffe6e6 100%);
  padding: 20px;
  margin: 10px 0;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
}

.error-container {
  max-width: 800px;
  margin: 0 auto;
}

.error-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
  border-bottom: 1px solid #ffcccb;
  padding-bottom: 15px;
}

.error-title {
  margin: 0;
  color: #d32f2f;
  font-size: 1.5em;
}

.error-actions {
  display: flex;
  gap: 10px;
}

.error-action-btn {
  padding: 8px 16px;
  border: none;
  border-radius: 4px;
  cursor: pointer;
  font-weight: 500;
  transition: all 0.2s ease;
}

.retry-btn {
  background: #4caf50;
  color: white;
}

.retry-btn:hover {
  background: #388e3c;
}

.ignore-btn {
  background: #ff9800;
  color: white;
}

.ignore-btn:hover {
  background: #f57c00;
}

.report-btn {
  background: #2196f3;
  color: white;
}

.report-btn:hover {
  background: #1976d2;
}

.error-content {
  background: white;
  border-radius: 6px;
  padding: 20px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
}

.user-message {
  display: flex;
  align-items: flex-start;
  gap: 15px;
  margin-bottom: 20px;
  padding: 15px;
  background: #f9f9f9;
  border-radius: 6px;
}

.error-icon {
  font-size: 24px;
}

.message-content {
  flex: 1;
}

.message-text {
  margin: 0 0 10px 0;
  color: #333;
  font-size: 1.1em;
}

.recovery-suggestion {
  margin: 0;
  color: #666;
  font-style: italic;
}

.recovery-status {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 12px;
  border-radius: 6px;
  margin-bottom: 20px;
  font-weight: 500;
}

.status-recovering {
  background: #e3f2fd;
  color: #1565c0;
  border: 1px solid #90caf9;
}

.status-recovered {
  background: #e8f5e9;
  color: #2e7d32;
  border: 1px solid #a5d6a7;
}

.status-failed {
  background: #ffebee;
  color: #c62828;
  border: 1px solid #ef9a9a;
}

.recovery-suggestions {
  margin-bottom: 20px;
}

.recovery-suggestions h4 {
  margin: 0 0 15px 0;
  color: #333;
}

.suggestion-list {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.suggestion-item {
  border: 1px solid #e0e0e0;
  border-radius: 6px;
  padding: 15px;
  background: #fafafa;
  transition: all 0.2s ease;
}

.suggestion-item:hover {
  background: #f5f5f5;
  border-color: #bdbdbd;
}

.suggestion-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 10px;
}

.suggestion-title {
  font-weight: 500;
  color: #333;
}

.confidence-badge {
  background: #e8f5e9;
  color: #2e7d32;
  padding: 4px 8px;
  border-radius: 12px;
  font-size: 0.85em;
  font-weight: 500;
}

.suggestion-footer {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-top: 10px;
}

.estimated-time {
  color: #666;
  font-size: 0.9em;
}

.apply-suggestion-btn {
  padding: 6px 12px;
  background: #2196f3;
  color: white;
  border: none;
  border-radius: 4px;
  cursor: pointer;
  font-size: 0.9em;
  transition: background 0.2s ease;
}

.apply-suggestion-btn:hover:not(:disabled) {
  background: #1976d2;
}

.apply-suggestion-btn:disabled {
  background: #bdbdbd;
  cursor: not-allowed;
}

.auto-recovery-section {
  margin-top: 20px;
  text-align: center;
}

.auto-recover-btn {
  padding: 10px 20px;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  color: white;
  border: none;
  border-radius: 4px;
  cursor: pointer;
  font-weight: 500;
  transition: all 0.3s ease;
}

.auto-recover-btn:hover {
  transform: translateY(-1px);
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
}

.error-details {
  margin-top: 20px;
  padding-top: 20px;
  border-top: 1px solid #e0e0e0;
}

.error-details .error-header {
  border: none;
  padding: 0;
  margin-bottom: 15px;
}

.error-details h3 {
  margin: 0;
  color: #333;
}

.error-severity {
  padding: 4px 8px;
  border-radius: 4px;
  font-size: 0.85em;
  font-weight: 500;
}

.severity-error {
  background: #ffebee;
  color: #c62828;
}

.severity-warning {
  background: #fff3e0;
  color: #ef6c00;
}

.severity-critical {
  background: #fce4ec;
  color: #ad1457;
}

.error-info {
  background: #f8f9fa;
  padding: 15px;
  border-radius: 4px;
  font-family: 'Monaco', 'Menlo', monospace;
  font-size: 0.9em;
}

.error-info p {
  margin: 0 0 8px 0;
}

.error-context,
.error-stack {
  margin-top: 15px;
}

.error-context pre,
.error-stack pre {
  background: #2d2d2d;
  color: #f8f8f2;
  padding: 10px;
  border-radius: 4px;
  overflow-x: auto;
  font-size: 0.85em;
  margin: 8px 0 0 0;
}
`;

// Inject styles
if (typeof document !== 'undefined') {
  const styleSheet = document.createElement('style');
  styleSheet.textContent = styles;
  document.head.appendChild(styleSheet);
}

// ==================== FACTORY FUNCTIONS ====================

class ErrorFactory {
  static createConnectionError(
    code: ErrorCode,
    message: string,
    context: any = {}
  ): AppError {
    return {
      id: `error-${Date.now()}`,
      code,
      severity: ErrorSeverity.ERROR, // Use ErrorSeverity enum instead of string
      message,
      timestamp: new Date().toISOString(),
      context,
      userFriendlyMessage: message,
      recoverySuggestion: undefined, // Add missing property
      type: 'connection' as const,
      // Add any other required properties from AppError interface
      ...(context.stackTrace && { stackTrace: context.stackTrace })
    };
  }
}

// ==================== EXPORTS ====================

export default ConnectionErrorBoundary;