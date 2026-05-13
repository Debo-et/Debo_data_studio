import React, { useReducer, useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { ChevronDown, ChevronUp, Plus, Trash2, Copy, AlertCircle } from 'lucide-react';

// ==================== TYPES ====================

interface SimpleColumn {
  name: string;
  type?: string;
  id?: string;
}

interface ReplaceRule {
  id: string;
  column: string;
  searchValue: string;
  replacement: string;
  caseSensitive: boolean;
  regex: boolean;
  scope: 'all' | 'first' | 'last';
  position: number;
}

interface ReplaceComponentConfiguration {
  version: string;
  rules: ReplaceRule[];
  globalOptions?: {
    errorHandling?: 'fail' | 'skip' | 'default';
    emptyValueHandling?: 'skip' | 'default' | 'null';
    parallelization?: boolean;
    maxThreads?: number;
    batchSize?: number;
  };
  outputSchema: any; // simplified for now
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

interface ReplaceEditorProps {
  nodeId: string;
  nodeMetadata?: any;
  inputColumns: SimpleColumn[];
  outputColumns: SimpleColumn[];
  initialConfig?: ReplaceComponentConfiguration;
  onClose: () => void;
  onSave: (config: ReplaceComponentConfiguration) => void;
}

// ==================== UTILITIES ====================

const generateId = (): string => {
  return `rule-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
};

const defaultRule = (columns: SimpleColumn[]): ReplaceRule => ({
  id: generateId(),
  column: columns.length > 0 ? columns[0].name : '*',
  searchValue: '',
  replacement: '',
  caseSensitive: false,
  regex: false,
  scope: 'all',
  position: 0,
});

// ==================== REDUCER ====================

type ReplaceAction =
  | { type: 'ADD_RULE'; columns: SimpleColumn[] }
  | { type: 'UPDATE_RULE'; id: string; updates: Partial<ReplaceRule> }
  | { type: 'DELETE_RULE'; id: string }
  | { type: 'MOVE_RULE'; id: string; direction: 'up' | 'down' }
  | { type: 'DUPLICATE_RULE'; id: string }
  | { type: 'SET_RULES'; rules: ReplaceRule[] };

const replaceReducer = (state: ReplaceRule[], action: ReplaceAction): ReplaceRule[] => {
  switch (action.type) {
    case 'ADD_RULE': {
      const newRule = { ...defaultRule(action.columns), position: state.length };
      return [...state, newRule];
    }
    case 'UPDATE_RULE': {
      return state.map(rule =>
        rule.id === action.id ? { ...rule, ...action.updates } : rule
      );
    }
    case 'DELETE_RULE': {
      return state.filter(rule => rule.id !== action.id).map((rule, idx) => ({ ...rule, position: idx }));
    }
    case 'MOVE_RULE': {
      const index = state.findIndex(r => r.id === action.id);
      if (index === -1) return state;
      const newIndex = action.direction === 'up' ? index - 1 : index + 1;
      if (newIndex < 0 || newIndex >= state.length) return state;
      const newState = [...state];
      [newState[index], newState[newIndex]] = [newState[newIndex], newState[index]];
      return newState.map((rule, idx) => ({ ...rule, position: idx }));
    }
    case 'DUPLICATE_RULE': {
      const rule = state.find(r => r.id === action.id);
      if (!rule) return state;
      const newRule = { ...rule, id: generateId(), position: state.length };
      return [...state, newRule];
    }
    case 'SET_RULES':
      return action.rules;
    default:
      return state;
  }
};

// ==================== VALIDATION ====================

interface ValidationMessage {
  id: string;
  type: 'info' | 'warning' | 'error';
  message: string;
  ruleId?: string;
}

const validateRules = (rules: ReplaceRule[]): ValidationMessage[] => {
  const messages: ValidationMessage[] = [];

  rules.forEach((rule, index) => {
    if (!rule.searchValue.trim()) {
      messages.push({
        id: `rule-${rule.id}-empty-search`,
        type: 'warning',
        message: `Rule ${index + 1}: search value is empty (will match nothing)`,
        ruleId: rule.id,
      });
    }
    if (rule.regex && rule.searchValue.trim()) {
      // Basic regex syntax check (optional)
      try {
        new RegExp(rule.searchValue, rule.caseSensitive ? '' : 'i');
      } catch (e) {
        messages.push({
          id: `rule-${rule.id}-invalid-regex`,
          type: 'error',
          message: `Rule ${index + 1}: invalid regular expression: ${(e as Error).message}`,
          ruleId: rule.id,
        });
      }
    }
    // No column selected? column is never empty because we use dropdown.
  });

  return messages;
};

// ==================== MAIN COMPONENT ====================

const ReplaceEditor: React.FC<ReplaceEditorProps> = ({
  nodeId,
  inputColumns,
  outputColumns,
  initialConfig,
  onClose,
  onSave,
}) => {
  const [rules, dispatch] = useReducer(replaceReducer, initialConfig?.rules || []);
  const [showPreview, setShowPreview] = useState(false);
  const [validationMessages, setValidationMessages] = useState<ValidationMessage[]>([]);
  const [autoSaveStatus] = useState<'idle' | 'saving' | 'saved'>('idle');

  // Validation on every change
  useEffect(() => {
    setValidationMessages(validateRules(rules));
  }, [rules]);

  // Prepare all columns for dropdown (including "*")
  const columnOptions = ['*', ...inputColumns.map(col => col.name)];

  const handleSave = () => {
    const config: ReplaceComponentConfiguration = {
      version: '1.0',
      rules,
      globalOptions: initialConfig?.globalOptions || {
        errorHandling: 'fail',
        emptyValueHandling: 'skip',
        parallelization: false,
        maxThreads: 4,
        batchSize: 1000,
      },
      outputSchema: initialConfig?.outputSchema || {
        id: `${nodeId}_output`,
        name: 'Output Schema',
        fields: outputColumns.map((col, idx) => ({
          id: col.id || `col_${idx}`,
          name: col.name,
          type: col.type || 'STRING',
          nullable: true,
          isKey: false,
        })),
        isTemporary: false,
        isMaterialized: false,
        metadata: {},
      },
      sqlGeneration: {
        requiresRegex: rules.some(r => r.regex),
        estimatedRowMultiplier: 1.0,
      },
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        createdBy: 'replace-editor',
        ruleCount: rules.length,
        validationStatus: validationMessages.some(m => m.type === 'error') ? 'ERROR' : validationMessages.length > 0 ? 'WARNING' : 'VALID',
        warnings: validationMessages.filter(m => m.type === 'warning').map(m => m.message),
        dependencies: [],
        compiledSql: undefined,
      },
    };
    onSave(config);
  };

  return (
    <div className="fixed inset-0 z-[9999] bg-black/80 flex items-center justify-center p-4">
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        className="bg-white dark:bg-gray-900 rounded-xl shadow-2xl w-full max-w-6xl max-h-[90vh] flex flex-col overflow-hidden"
      >
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b bg-gradient-to-r from-blue-50 to-gray-50 dark:from-gray-800 dark:to-gray-900">
          <div>
            <h2 className="text-lg font-bold flex items-center">
              <span className="mr-2">🔄</span>
              Replace Editor
              <span className="ml-2 text-xs bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200 px-2 py-0.5 rounded">
                v1.0
              </span>
            </h2>
            <p className="text-sm text-gray-600 dark:text-gray-400 mt-1">
              Node: <span className="font-mono">{nodeId}</span> • {inputColumns.length} input columns • {outputColumns.length} output columns
            </p>
          </div>
          <button
            onClick={onClose}
            className="p-2 hover:bg-gray-100 dark:hover:bg-gray-800 rounded-full"
          >
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        {/* Toolbar */}
        <div className="flex items-center justify-between px-6 py-3 border-b bg-gray-50 dark:bg-gray-800/50">
          <div className="flex items-center space-x-3">
            <button
              onClick={() => dispatch({ type: 'ADD_RULE', columns: inputColumns })}
              className="px-3 py-1.5 bg-blue-500 hover:bg-blue-600 text-white text-sm rounded flex items-center"
            >
              <Plus className="w-4 h-4 mr-1" />
              Add Rule
            </button>
            <button
              onClick={() => setShowPreview(!showPreview)}
              className="px-3 py-1.5 border border-gray-300 dark:border-gray-600 text-sm rounded hover:bg-gray-100 dark:hover:bg-gray-800"
            >
              {showPreview ? 'Hide Preview' : 'Show Preview'}
            </button>
          </div>
          <div className="text-sm text-gray-500 dark:text-gray-400">
            {rules.length} rule{rules.length !== 1 ? 's' : ''}
          </div>
        </div>

        {/* Main content: rules table */}
        <div className="flex-1 overflow-y-auto p-6">
          <table className="w-full border-collapse">
            <thead className="sticky top-0 bg-gray-100 dark:bg-gray-800">
              <tr>
                <th className="px-2 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 w-12">#</th>
                <th className="px-2 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400">Column</th>
                <th className="px-2 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400">Search</th>
                <th className="px-2 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400">Replace</th>
                <th className="px-2 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 w-16">Case</th>
                <th className="px-2 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 w-16">Regex</th>
                <th className="px-2 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 w-24">Scope</th>
                <th className="px-2 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 w-24">Actions</th>
              </tr>
            </thead>
            <tbody>
              {rules.map((rule, index) => (
                <tr key={rule.id} className="border-b border-gray-200 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-800/50">
                  <td className="px-2 py-2 text-sm text-gray-500">
                    {index + 1}
                  </td>
                  <td className="px-2 py-2">
                    <select
                      value={rule.column}
                      onChange={(e) => dispatch({ type: 'UPDATE_RULE', id: rule.id, updates: { column: e.target.value } })}
                      className="bg-gray-800 border border-gray-700 text-white text-sm rounded px-2 py-1 w-full"
                    >
                      {columnOptions.map(col => (
                        <option key={col} value={col}>{col}</option>
                      ))}
                    </select>
                  </td>
                  <td className="px-2 py-2">
                    <input
                      type="text"
                      value={rule.searchValue}
                      onChange={(e) => dispatch({ type: 'UPDATE_RULE', id: rule.id, updates: { searchValue: e.target.value } })}
                      placeholder="text or pattern"
                      className="bg-gray-800 border border-gray-700 text-white text-sm rounded px-2 py-1 w-full"
                    />
                  </td>
                  <td className="px-2 py-2">
                    <input
                      type="text"
                      value={rule.replacement}
                      onChange={(e) => dispatch({ type: 'UPDATE_RULE', id: rule.id, updates: { replacement: e.target.value } })}
                      placeholder="replacement"
                      className="bg-gray-800 border border-gray-700 text-white text-sm rounded px-2 py-1 w-full"
                    />
                  </td>
                  <td className="px-2 py-2 text-center">
                    <input
                      type="checkbox"
                      checked={rule.caseSensitive}
                      onChange={(e) => dispatch({ type: 'UPDATE_RULE', id: rule.id, updates: { caseSensitive: e.target.checked } })}
                      className="h-4 w-4"
                    />
                  </td>
                  <td className="px-2 py-2 text-center">
                    <input
                      type="checkbox"
                      checked={rule.regex}
                      onChange={(e) => dispatch({ type: 'UPDATE_RULE', id: rule.id, updates: { regex: e.target.checked } })}
                      className="h-4 w-4"
                    />
                  </td>
                  <td className="px-2 py-2">
                    <select
                      value={rule.scope}
                      onChange={(e) => dispatch({ type: 'UPDATE_RULE', id: rule.id, updates: { scope: e.target.value as any } })}
                      className="bg-gray-800 border border-gray-700 text-white text-sm rounded px-2 py-1 w-full"
                    >
                      <option value="all">all</option>
                      <option value="first">first</option>
                      <option value="last">last</option>
                    </select>
                  </td>
                  <td className="px-2 py-2">
                    <div className="flex items-center space-x-1">
                      <button
                        onClick={() => dispatch({ type: 'MOVE_RULE', id: rule.id, direction: 'up' })}
                        disabled={index === 0}
                        className="p-1 text-gray-400 hover:text-white disabled:opacity-30"
                        title="Move up"
                      >
                        <ChevronUp className="w-4 h-4" />
                      </button>
                      <button
                        onClick={() => dispatch({ type: 'MOVE_RULE', id: rule.id, direction: 'down' })}
                        disabled={index === rules.length - 1}
                        className="p-1 text-gray-400 hover:text-white disabled:opacity-30"
                        title="Move down"
                      >
                        <ChevronDown className="w-4 h-4" />
                      </button>
                      <button
                        onClick={() => dispatch({ type: 'DUPLICATE_RULE', id: rule.id })}
                        className="p-1 text-gray-400 hover:text-white"
                        title="Duplicate"
                      >
                        <Copy className="w-4 h-4" />
                      </button>
                      <button
                        onClick={() => dispatch({ type: 'DELETE_RULE', id: rule.id })}
                        className="p-1 text-red-400 hover:text-red-600"
                        title="Delete"
                      >
                        <Trash2 className="w-4 h-4" />
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
              {rules.length === 0 && (
                <tr>
                  <td colSpan={8} className="text-center py-8 text-gray-500">
                    No rules defined. Click "Add Rule" to create one.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        {/* Preview Panel (collapsible) */}
        {showPreview && (
          <div className="border-t border-gray-200 dark:border-gray-700 p-4 bg-gray-50 dark:bg-gray-800/30">
            <h3 className="text-sm font-medium mb-2">Preview (first 3 rows)</h3>
            <div className="text-xs text-gray-500 italic">
              Preview would show sample data with replacements applied.
              <br />
              (Implementation requires sample data from source.)
            </div>
          </div>
        )}

        {/* Status Bar */}
        <div className="border-t border-gray-200 dark:border-gray-700 px-6 py-2 bg-gray-50 dark:bg-gray-800/50 flex items-center justify-between text-xs">
          <div className="flex items-center space-x-4">
            {validationMessages.length > 0 ? (
              validationMessages.map(msg => (
                <div key={msg.id} className={`flex items-center ${
                  msg.type === 'error' ? 'text-red-500' : msg.type === 'warning' ? 'text-yellow-500' : 'text-blue-500'
                }`}>
                  <AlertCircle className="w-3 h-3 mr-1" />
                  <span>{msg.message}</span>
                </div>
              ))
            ) : (
              <span className="text-green-500">✓ No validation issues</span>
            )}
          </div>
          <div className="text-gray-400">
            {autoSaveStatus === 'saving' ? 'Saving...' : autoSaveStatus === 'saved' ? 'Saved' : ''}
          </div>
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end space-x-3 px-6 py-4 border-t border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-800/50">
          <button
            onClick={onClose}
            className="px-4 py-2 border border-gray-300 dark:border-gray-600 rounded hover:bg-gray-100 dark:hover:bg-gray-800"
          >
            Cancel
          </button>
          <button
            onClick={handleSave}
            className="px-4 py-2 bg-gradient-to-r from-green-500 to-green-600 text-white rounded hover:from-green-600 hover:to-green-700"
          >
            Save Configuration
          </button>
        </div>
      </motion.div>
    </div>
  );
};

export default ReplaceEditor;