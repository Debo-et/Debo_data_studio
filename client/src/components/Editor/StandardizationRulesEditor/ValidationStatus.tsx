// ValidationStatus.tsx
import React from 'react';
import { CheckCircle, AlertTriangle, Info, XCircle } from 'lucide-react';
import { ValidationResult } from '../../../types/types';

interface ValidationStatusProps {
  activeRulesCount: number;
  totalRulesCount: number;
  validationResults: ValidationResult[];
}

const ValidationStatus: React.FC<ValidationStatusProps> = ({
  activeRulesCount,
  totalRulesCount,
  validationResults
}) => {
  const errorCount = validationResults.filter(r => r.type === 'error').length;
  const warningCount = validationResults.filter(r => r.type === 'warning').length;
  const infoCount = validationResults.filter(r => r.type === 'info').length;

  const getStatusColor = () => {
    if (errorCount > 0) return 'text-red-600 dark:text-red-400';
    if (warningCount > 0) return 'text-amber-600 dark:text-amber-400';
    return 'text-green-600 dark:text-green-400';
  };

  const getStatusIcon = () => {
    if (errorCount > 0) return <XCircle className="w-5 h-5" />;
    if (warningCount > 0) return <AlertTriangle className="w-5 h-5" />;
    return <CheckCircle className="w-5 h-5" />;
  };

  return (
    <div className="flex items-center space-x-4">
      {/* Rule Count */}
      <div className="flex items-center space-x-2">
        <div className="text-sm text-gray-600 dark:text-gray-400">Rules:</div>
        <div className="flex items-center space-x-1">
          <span className="font-medium text-gray-900 dark:text-white">{activeRulesCount}</span>
          <span className="text-gray-400">/</span>
          <span className="font-medium text-gray-900 dark:text-white">{totalRulesCount}</span>
          <span className="text-xs text-gray-500">active</span>
        </div>
      </div>

      {/* Schema Compatibility */}
      <div className="flex items-center space-x-2">
        <CheckCircle className="w-4 h-4 text-green-500" />
        <span className="text-sm text-gray-600 dark:text-gray-400">Schema: Compatible</span>
      </div>

      {/* Validation Status */}
      <div className={`flex items-center space-x-2 ${getStatusColor()}`}>
        {getStatusIcon()}
        <div className="text-sm">
          {errorCount > 0 && `${errorCount} error${errorCount !== 1 ? 's' : ''}`}
          {errorCount === 0 && warningCount > 0 && `${warningCount} warning${warningCount !== 1 ? 's' : ''}`}
          {errorCount === 0 && warningCount === 0 && 'Valid'}
        </div>
      </div>

      {/* Detailed Counts */}
      {(errorCount > 0 || warningCount > 0) && (
        <div className="flex items-center space-x-3">
          {errorCount > 0 && (
            <div className="flex items-center space-x-1">
              <XCircle className="w-4 h-4 text-red-500" />
              <span className="text-sm text-red-600 dark:text-red-400">{errorCount}</span>
            </div>
          )}
          {warningCount > 0 && (
            <div className="flex items-center space-x-1">
              <AlertTriangle className="w-4 h-4 text-amber-500" />
              <span className="text-sm text-amber-600 dark:text-amber-400">{warningCount}</span>
            </div>
          )}
          {infoCount > 0 && (
            <div className="flex items-center space-x-1">
              <Info className="w-4 h-4 text-blue-500" />
              <span className="text-sm text-blue-600 dark:text-blue-400">{infoCount}</span>
            </div>
          )}
        </div>
      )}

      {/* Status Message */}
      <div className="text-sm text-gray-500 dark:text-gray-400">
        {errorCount === 0 && warningCount === 0 
          ? 'All rules are valid and ready to use'
          : errorCount > 0
          ? 'Please fix errors before saving'
          : 'Review warnings for potential issues'
        }
      </div>
    </div>
  );
};

export default ValidationStatus;