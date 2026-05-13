// src/components/SQLPreviewModal.tsx

import React, { useState } from 'react';
import { motion } from 'framer-motion';
import { SQLPreviewState } from '../utils/canvasUtils';

interface SQLPreviewModalProps {
  sqlState: SQLPreviewState;
  onClose: () => void;
  onRegenerate: (nodeId: string) => void;
}

const SQLPreviewModal: React.FC<SQLPreviewModalProps> = ({ sqlState, onClose, onRegenerate }) => {
  const [copied, setCopied] = useState(false);

  const handleCopy = () => {
    navigator.clipboard.writeText(sqlState.sql);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const handleExport = () => {
    const blob = new Blob([sqlState.sql], { type: 'text/sql' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${sqlState.nodeName.replace(/\s+/g, '_')}_generated.sql`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  return (
    <div className="fixed inset-0 bg-black bg-opacity-70 flex items-center justify-center z-[10000] p-4">
      <motion.div
        initial={{ opacity: 0, scale: 0.95, y: -20 }}
        animate={{ opacity: 1, scale: 1, y: 0 }}
        exit={{ opacity: 0, scale: 0.95, y: 20 }}
        transition={{ duration: 0.2 }}
        className="bg-white rounded-xl shadow-2xl w-full max-w-6xl max-h-[90vh] flex flex-col"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b">
          <div>
            <h2 className="text-xl font-bold text-gray-900 flex items-center">
              <span className="mr-3">💾</span>
              Generated PostgreSQL SQL
            </h2>
            <p className="text-sm text-gray-600 mt-1">
              {sqlState.nodeName} • {new Date().toLocaleString()}
              {sqlState.hasDefaultMappings && (
                <span className="ml-2 text-xs bg-yellow-100 text-yellow-800 px-2 py-0.5 rounded">
                  Includes default positional mappings
                </span>
              )}
            </p>
          </div>
          <div className="flex items-center space-x-3">
            <button
              onClick={() => onRegenerate(sqlState.nodeId)}
              className="px-4 py-2 text-sm bg-blue-500 text-white rounded-lg hover:bg-blue-600 transition-colors flex items-center"
            >
              <span className="mr-2">🔄</span>
              Regenerate
            </button>
            <button
              onClick={onClose}
              className="w-8 h-8 flex items-center justify-center text-gray-500 hover:text-gray-700 hover:bg-gray-100 rounded-full"
            >
              ✕
            </button>
          </div>
        </div>

        {/* Stats Bar */}
        <div className="bg-gray-50 px-6 py-3 border-b flex items-center justify-between">
          <div className="flex items-center space-x-4 text-sm">
            <div className="flex items-center">
              <div className="w-3 h-3 rounded-full bg-green-500 mr-2"></div>
              <span className="text-gray-700">Valid SQL</span>
            </div>
            {sqlState.hasDefaultMappings && (
              <div className="flex items-center">
                <div className="w-3 h-3 rounded-full bg-yellow-500 mr-2"></div>
                <span className="text-gray-700">Auto-mapped columns</span>
              </div>
            )}
            <div className="text-gray-500">
              {sqlState.sql.split('\n').length} lines
            </div>
          </div>
          <div className="flex items-center space-x-2">
            <button
              onClick={handleExport}
              className="px-3 py-1.5 text-xs border border-gray-300 rounded hover:bg-gray-50 flex items-center"
            >
              <span className="mr-1">📥</span>
              Export
            </button>
            <button
              onClick={handleCopy}
              className={`px-3 py-1.5 text-xs rounded flex items-center ${
                copied 
                  ? 'bg-green-500 text-white' 
                  : 'bg-blue-500 text-white hover:bg-blue-600'
              }`}
            >
              <span className="mr-1">{copied ? '✓' : '📋'}</span>
              {copied ? 'Copied!' : 'Copy SQL'}
            </button>
          </div>
        </div>

        {/* SQL Content */}
        <div className="flex-1 overflow-hidden flex">
          {/* SQL Editor */}
          <div className="flex-1 overflow-auto">
            <div className="p-6">
              <div className="bg-gray-900 text-gray-100 rounded-lg overflow-hidden font-mono text-sm">
                <div className="px-4 py-3 bg-gray-800 border-b border-gray-700 flex items-center justify-between">
                  <div className="text-xs text-gray-400">
                    PostgreSQL INSERT INTO ... SELECT
                  </div>
                  <div className="text-xs text-gray-400">
                    Set-based operation for optimal performance
                  </div>
                </div>
                <pre className="p-4 overflow-x-auto whitespace-pre-wrap">
                  {sqlState.sql}
                </pre>
              </div>
            </div>
          </div>

          {/* Sidebar - Errors & Warnings */}
          {(sqlState.errors.length > 0 || sqlState.warnings.length > 0) && (
            <div className="w-80 border-l overflow-auto bg-gray-50">
              <div className="p-4">
                {sqlState.errors.length > 0 && (
                  <div className="mb-6">
                    <h3 className="font-semibold text-red-700 mb-2 flex items-center">
                      <span className="mr-2">❌</span>
                      Errors ({sqlState.errors.length})
                    </h3>
                    <div className="space-y-2">
                      {sqlState.errors.map((error, index) => (
                        <div key={index} className="bg-red-50 border border-red-200 rounded p-3 text-sm">
                          {error}
                        </div>
                      ))}
                    </div>
                  </div>
                )}
                
                {sqlState.warnings.length > 0 && (
                  <div>
                    <h3 className="font-semibold text-yellow-700 mb-2 flex items-center">
                      <span className="mr-2">⚠️</span>
                      Warnings ({sqlState.warnings.length})
                    </h3>
                    <div className="space-y-2">
                      {sqlState.warnings.map((warning, index) => (
                        <div key={index} className="bg-yellow-50 border border-yellow-200 rounded p-3 text-sm">
                          {warning}
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {sqlState.hasDefaultMappings && (
                  <div className="mt-6 p-4 bg-blue-50 border border-blue-200 rounded-lg">
                    <h4 className="font-semibold text-blue-800 mb-2 flex items-center">
                      <span className="mr-2">🤖</span>
                      Automatic Mapping Applied
                    </h4>
                    <p className="text-sm text-blue-700">
                      Default positional mapping was applied to unmapped columns. 
                      First source column → First target column, second → second, etc.
                    </p>
                  </div>
                )}

                <div className="mt-6 p-4 bg-green-50 border border-green-200 rounded-lg">
                  <h4 className="font-semibold text-green-800 mb-2">Performance Notes</h4>
                  <ul className="text-sm text-green-700 space-y-1">
                    <li>• Uses PostgreSQL set-based INSERT INTO ... SELECT</li>
                    <li>• No row-by-row processing (cursors)</li>
                    <li>• Optimal for large datasets</li>
                    <li>• Transaction-safe with automatic rollback on error</li>
                  </ul>
                </div>
              </div>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="border-t p-4 flex items-center justify-between">
          <div className="text-xs text-gray-500 flex items-center">
            <span className="mr-2">⚡</span>
            Generated at {new Date().toLocaleTimeString()} • Ready for execution
          </div>
          <div className="flex items-center space-x-3">
            <button
              onClick={onClose}
              className="px-4 py-2 text-sm border border-gray-300 rounded hover:bg-gray-50"
            >
              Close
            </button>
            <button
              onClick={() => {
                onRegenerate(sqlState.nodeId);
                onClose();
              }}
              className="px-4 py-2 text-sm bg-purple-500 text-white rounded hover:bg-purple-600"
            >
              Regenerate & Close
            </button>
          </div>
        </div>
      </motion.div>
    </div>
  );
};

export default SQLPreviewModal;