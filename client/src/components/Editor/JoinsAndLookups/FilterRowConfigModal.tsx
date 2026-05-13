// src/components/Editor/FilterRowConfigModal.tsx

import React, { useReducer, useCallback, useMemo, useEffect } from 'react';
import { motion } from 'framer-motion';
import { Plus, GripVertical, Trash2, ChevronRight } from 'lucide-react';

// UI components that exist in your project
import { Button } from '../../ui/Button';
import { Card, CardContent } from '../../ui/card';
import { Badge } from '../../ui/badge';
import { Input } from '../../ui/input';
import { Label } from '../../ui/label';

// Types – only import the final configuration type
import { FilterComponentConfiguration } from '../../../types/unified-pipeline.types';

// ==================== Local Types ====================
type FilterOperator = '=' | '!=' | '<' | '>' | '<=' | '>=' | 'LIKE' | 'IN' | 'IS_NULL' | 'NOT_NULL';

interface UICondition {
  id: string;
  field: string;
  operator: FilterOperator;
  value: string;            // always a string in the UI (converted from config if needed)
  position: number;
}

interface FilterEditorState {
  filterLogic: 'AND' | 'OR';
  conditions: UICondition[];
  optimization: {
    pushDown: boolean;
    indexable: boolean;
    estimatedSelectivity: number;
  };
  errorHandling: 'fail' | 'skip' | 'default';
}

interface FilterRowConfigModalProps {
  isOpen: boolean;
  onClose: () => void;
  nodeId: string;
  nodeName: string;
  inputColumns: Array<{ name: string; type: string }>;
  initialConfig?: FilterComponentConfiguration;
  onSave: (config: FilterComponentConfiguration) => void;
}

// ==================== Constants ====================
const OPERATORS: { value: FilterOperator; label: string }[] = [
  { value: '=', label: '=' },
  { value: '!=', label: '≠' },
  { value: '<', label: '<' },
  { value: '>', label: '>' },
  { value: '<=', label: '≤' },
  { value: '>=', label: '≥' },
  { value: 'LIKE', label: 'LIKE' },
  { value: 'IN', label: 'IN' },
  { value: 'IS_NULL', label: 'IS NULL' },
  { value: 'NOT_NULL', label: 'IS NOT NULL' },
];

const ERROR_HANDLING_OPTIONS = [
  { value: 'fail', label: 'Fail on error' },
  { value: 'skip', label: 'Skip row on error' },
  { value: 'default', label: 'Use default value' },
];

// ==================== Reducer ====================
type FilterEditorAction =
  | { type: 'ADD_CONDITION' }
  | { type: 'REMOVE_CONDITION'; id: string }
  | { type: 'UPDATE_CONDITION'; id: string; field: keyof UICondition; value: any }
  | { type: 'REORDER_CONDITIONS'; fromIndex: number; toIndex: number }
  | { type: 'SET_FILTER_LOGIC'; logic: 'AND' | 'OR' }
  | { type: 'UPDATE_OPTIMIZATION'; key: keyof FilterEditorState['optimization']; value: any }
  | { type: 'SET_ERROR_HANDLING'; value: 'fail' | 'skip' | 'default' }
  | { type: 'LOAD_INITIAL'; config: FilterComponentConfiguration; columns: Array<{ name: string; type: string }> };

const generateId = () => `cond-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

const createEmptyCondition = (): UICondition => ({
  id: generateId(),
  field: '',
  operator: '=',
  value: '',
  position: 0,
});

const filterReducer = (state: FilterEditorState, action: FilterEditorAction): FilterEditorState => {
  switch (action.type) {
    case 'ADD_CONDITION': {
      const newCondition = createEmptyCondition();
      newCondition.position = state.conditions.length;
      return {
        ...state,
        conditions: [...state.conditions, newCondition],
      };
    }
    case 'REMOVE_CONDITION':
      return {
        ...state,
        conditions: state.conditions.filter(c => c.id !== action.id).map((c, idx) => ({ ...c, position: idx })),
      };
    case 'UPDATE_CONDITION':
      return {
        ...state,
        conditions: state.conditions.map(c =>
          c.id === action.id ? { ...c, [action.field]: action.value } : c
        ),
      };
    case 'REORDER_CONDITIONS': {
      const { fromIndex, toIndex } = action;
      const reordered = [...state.conditions];
      const [moved] = reordered.splice(fromIndex, 1);
      reordered.splice(toIndex, 0, moved);
      reordered.forEach((c, idx) => (c.position = idx));
      return { ...state, conditions: reordered };
    }
    case 'SET_FILTER_LOGIC':
      return { ...state, filterLogic: action.logic };
    case 'UPDATE_OPTIMIZATION':
      return {
        ...state,
        optimization: { ...state.optimization, [action.key]: action.value },
      };
    case 'SET_ERROR_HANDLING':
      return { ...state, errorHandling: action.value };
    case 'LOAD_INITIAL': {
      const { config, columns } = action;
      const columnNames = columns.map(c => c.name);
      const conditions: UICondition[] = (config.filterConditions || [])
        .filter(c => columnNames.includes(c.field))
        .map((c, idx) => ({
          id: c.id || generateId(),
          field: c.field,
          operator: c.operator as FilterOperator,
          // Convert any non‑string value to string for display
          value: c.value !== undefined && c.value !== null ? String(c.value) : '',
          position: idx,
        }));
      return {
        filterLogic: config.filterLogic === 'OR' ? 'OR' : 'AND',
        conditions,
        optimization: {
          pushDown: config.optimization?.pushDown ?? true,
          indexable: config.optimization?.indexable ?? true,
          estimatedSelectivity: config.optimization?.estimatedSelectivity ?? 1.0,
        },
        errorHandling: 'fail',
      };
    }
    default:
      return state;
  }
};

// ==================== Helper: Generate SQL WHERE preview ====================
const generateWhereClause = (conditions: UICondition[], logic: 'AND' | 'OR'): string => {
  if (conditions.length === 0) return 'No conditions';

  const parts = conditions.map(cond => {
    const field = cond.field || '<column>';
    const op = cond.operator;
    const val = cond.value; // always a string now

    if (op === 'IS_NULL' || op === 'NOT_NULL') {
      return `${field} ${op.replace('_', ' ')}`;
    }
    if (op === 'IN') {
      const values = val
        .split(',')
        .map(v => `'${v.trim()}'`)
        .join(', ');
      return `${field} IN (${values})`;
    }
    // For other operators, quote the value if it's a non‑empty string
    const quotedVal = val ? `'${val}'` : '';
    return `${field} ${op} ${quotedVal}`;
  });

  return parts.join(` ${logic} `);
};

// ==================== Main Component ====================
export const FilterRowConfigModal: React.FC<FilterRowConfigModalProps> = ({
  isOpen,
  onClose,
  nodeId,
  nodeName,
  inputColumns,
  initialConfig,
  onSave,
}) => {
  const [state, dispatch] = useReducer(filterReducer, {
    filterLogic: 'AND',
    conditions: [],
    optimization: {
      pushDown: true,
      indexable: true,
      estimatedSelectivity: 1.0,
    },
    errorHandling: 'fail',
  });

  // Load initial config when modal opens
  useEffect(() => {
    if (isOpen && initialConfig) {
      dispatch({ type: 'LOAD_INITIAL', config: initialConfig, columns: inputColumns });
    } else if (isOpen && !initialConfig) {
      dispatch({ type: 'ADD_CONDITION' });
    }
  }, [isOpen, initialConfig, inputColumns]);

  const addCondition = useCallback(() => {
    dispatch({ type: 'ADD_CONDITION' });
  }, []);

  const removeCondition = useCallback((id: string) => {
    dispatch({ type: 'REMOVE_CONDITION', id });
  }, []);

  const updateCondition = useCallback((id: string, field: keyof UICondition, value: any) => {
    dispatch({ type: 'UPDATE_CONDITION', id, field, value });
  }, []);

  const handleDragStart = useCallback((e: React.DragEvent, index: number) => {
    e.dataTransfer.setData('text/plain', index.toString());
  }, []);

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
  }, []);

  const handleDrop = useCallback((e: React.DragEvent, targetIndex: number) => {
    e.preventDefault();
    const fromIndex = parseInt(e.dataTransfer.getData('text/plain'), 10);
    if (fromIndex === targetIndex) return;
    dispatch({ type: 'REORDER_CONDITIONS', fromIndex, toIndex: targetIndex });
  }, []);

  const handleSave = useCallback(() => {
    const config: FilterComponentConfiguration = {
      version: '1.0',
      filterConditions: state.conditions.map(c => ({
        id: c.id,
        field: c.field,
        operator: c.operator,
        value: c.value, // string; the config accepts string | number | boolean, but string is safe
        valueType: 'CONSTANT',
        logicGroup: 0,
        position: c.position,
      })),
      filterLogic: state.filterLogic,
      optimization: {
        pushDown: state.optimization.pushDown,
        indexable: state.optimization.indexable,
        estimatedSelectivity: state.optimization.estimatedSelectivity,
      },
      sqlGeneration: {
        whereClause: generateWhereClause(state.conditions, state.filterLogic),
        parameterized: false,
        requiresSubquery: false,
        canUseIndex: state.optimization.indexable,
      },
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        estimatedRowReduction: 0,
        warnings: [],
      },
    };
    onSave(config);
  }, [state, onSave]);

  const sqlPreview = useMemo(
    () => generateWhereClause(state.conditions, state.filterLogic),
    [state.conditions, state.filterLogic]
  );

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-[9999] bg-black bg-opacity-70 flex items-center justify-center p-4">
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        className="bg-white dark:bg-gray-900 rounded-xl shadow-2xl w-full max-w-5xl h-[90vh] flex flex-col overflow-hidden"
      >
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b dark:border-gray-700">
          <div>
            <h2 className="text-lg font-bold flex items-center gap-2">
              <span>🔍 Filter Editor</span>
              <Badge variant="outline">tFilterRow</Badge>
            </h2>
            <p className="text-sm text-gray-500 dark:text-gray-400">
              Node: <span className="font-mono text-blue-600">{nodeName}</span> (ID: {nodeId})
            </p>
          </div>
          <div className="flex gap-2">
            <Button variant="outline" onClick={onClose}>
              Cancel
            </Button>
            <Button onClick={handleSave}>Save</Button>
          </div>
        </div>

        {/* Main area */}
        <div className="flex flex-1 overflow-hidden">
          {/* Left panel: column reference */}
          <div className="w-64 border-r dark:border-gray-700 p-4 bg-gray-50 dark:bg-gray-800/50 overflow-auto">
            <h3 className="text-sm font-medium mb-3">Available Columns</h3>
            {inputColumns.length === 0 ? (
              <p className="text-xs text-gray-500 italic">No input columns found.</p>
            ) : (
              <ul className="space-y-1">
                {inputColumns.map(col => (
                  <li key={col.name} className="text-xs font-mono text-gray-700 dark:text-gray-300 flex justify-between">
                    <span>{col.name}</span>
                    <span className="text-gray-500">({col.type})</span>
                  </li>
                ))}
              </ul>
            )}
          </div>

          {/* Right panel: condition builder */}
          <div className="flex-1 p-4 overflow-auto">
            <div className="space-y-4">
              {/* List of conditions */}
              {state.conditions.map((cond, index) => (
                <div
                  key={cond.id}
                  className="flex items-start gap-2 p-3 border rounded-lg dark:border-gray-700 bg-white dark:bg-gray-800"
                  draggable
                  onDragStart={(e) => handleDragStart(e, index)}
                  onDragOver={handleDragOver}
                  onDrop={(e) => handleDrop(e, index)}
                >
                  <div className="cursor-move mt-2 text-gray-400 hover:text-gray-600">
                    <GripVertical className="h-4 w-4" />
                  </div>
                  <div className="flex-1 grid grid-cols-4 gap-2">
                    {/* Column select - native select */}
                    <select
                      value={cond.field}
                      onChange={(e) => updateCondition(cond.id, 'field', e.target.value)}
                      className="px-3 py-2 text-sm border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
                    >
                      <option value="">Select column</option>
                      {inputColumns.map(col => (
                        <option key={col.name} value={col.name}>
                          {col.name}
                        </option>
                      ))}
                    </select>

                    {/* Operator select - native select */}
                    <select
                      value={cond.operator}
                      onChange={(e) => updateCondition(cond.id, 'operator', e.target.value as FilterOperator)}
                      className="px-3 py-2 text-sm border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
                    >
                      {OPERATORS.map(op => (
                        <option key={op.value} value={op.value}>
                          {op.label}
                        </option>
                      ))}
                    </select>

                    {/* Value input (only for operators that need a value) */}
                    {cond.operator !== 'IS_NULL' && cond.operator !== 'NOT_NULL' && (
                      <Input
                        value={cond.value}   // now always a string
                        onChange={(e) => updateCondition(cond.id, 'value', e.target.value)}
                        placeholder="value"
                        className="col-span-2"
                      />
                    )}
                    {(cond.operator === 'IS_NULL' || cond.operator === 'NOT_NULL') && (
                      <div className="col-span-2 text-xs text-gray-500 italic flex items-center">
                        No value needed
                      </div>
                    )}
                  </div>

                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => removeCondition(cond.id)}
                    className="text-red-500 hover:text-red-700 hover:bg-red-50 dark:hover:bg-red-900/20"
                  >
                    <Trash2 className="h-4 w-4" />
                  </Button>
                </div>
              ))}

              {/* Add condition button */}
              <Button variant="outline" size="sm" onClick={addCondition}>
                <Plus className="h-4 w-4 mr-2" /> Add Condition
              </Button>

              {/* Global logic selector */}
              <div className="flex items-center gap-4 mt-4">
                <Label className="w-24">Filter Logic</Label>
                <select
                  value={state.filterLogic}
                  onChange={(e) => dispatch({ type: 'SET_FILTER_LOGIC', logic: e.target.value as 'AND' | 'OR' })}
                  className="px-3 py-2 text-sm border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
                >
                  <option value="AND">ALL conditions (AND)</option>
                  <option value="OR">ANY conditions (OR)</option>
                </select>
              </div>

              {/* Advanced options */}
              <details className="mt-4 border rounded p-3 dark:border-gray-700">
                <summary className="cursor-pointer text-sm font-medium flex items-center gap-1">
                  <ChevronRight className="h-4 w-4" /> Advanced
                </summary>
                <div className="mt-3 space-y-3">
                  <div className="flex items-center gap-2">
                    <input
                      type="checkbox"
                      id="pushdown"
                      checked={state.optimization.pushDown}
                      onChange={(e) =>
                        dispatch({ type: 'UPDATE_OPTIMIZATION', key: 'pushDown', value: e.target.checked })
                      }
                      className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                    />
                    <Label htmlFor="pushdown">Enable pushdown</Label>
                  </div>
                  <div className="flex items-center gap-2">
                    <input
                      type="checkbox"
                      id="indexable"
                      checked={state.optimization.indexable}
                      onChange={(e) =>
                        dispatch({ type: 'UPDATE_OPTIMIZATION', key: 'indexable', value: e.target.checked })
                      }
                      className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                    />
                    <Label htmlFor="indexable">Indexable</Label>
                  </div>
                  <div className="grid grid-cols-2 gap-2 items-center">
                    <Label htmlFor="selectivity">Estimated selectivity</Label>
                    <Input
                      id="selectivity"
                      type="number"
                      min="0"
                      max="1"
                      step="0.1"
                      value={state.optimization.estimatedSelectivity}
                      onChange={(e) =>
                        dispatch({
                          type: 'UPDATE_OPTIMIZATION',
                          key: 'estimatedSelectivity',
                          value: parseFloat(e.target.value),
                        })
                      }
                    />
                  </div>
                  <div className="grid grid-cols-2 gap-2 items-center">
                    <Label htmlFor="errorHandling">Error handling</Label>
                    <select
                      id="errorHandling"
                      value={state.errorHandling}
                      onChange={(e) =>
                        dispatch({ type: 'SET_ERROR_HANDLING', value: e.target.value as 'fail' | 'skip' | 'default' })
                      }
                      className="px-3 py-2 text-sm border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
                    >
                      {ERROR_HANDLING_OPTIONS.map(opt => (
                        <option key={opt.value} value={opt.value}>
                          {opt.label}
                        </option>
                      ))}
                    </select>
                  </div>
                </div>
              </details>

              {/* SQL Preview */}
              <Card className="mt-4">
                <CardContent className="p-4">
                  <h4 className="text-sm font-medium mb-2">SQL WHERE Preview</h4>
                  <pre className="text-xs bg-gray-100 dark:bg-gray-800 p-3 rounded overflow-auto max-h-32 font-mono">
                    {sqlPreview}
                  </pre>
                </CardContent>
              </Card>
            </div>
          </div>
        </div>
      </motion.div>
    </div>
  );
};