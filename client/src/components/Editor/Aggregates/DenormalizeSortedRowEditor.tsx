import React, { useReducer, useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import {
  X,
  Plus,
  Trash2,
  GripVertical,
  ChevronDown,
  ChevronUp,
  AlertCircle,
  Check,
} from 'lucide-react';

// Import UI components (adjust based on your project)
import { Button } from '../../ui/Button';
import { Input } from '../../ui/input';
import { Label } from '../../ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../ui/select';
import { Checkbox } from '../../ui/checkbox';
import { Badge } from '../../ui/badge';
import { Card, CardContent, CardHeader, CardTitle } from '../../ui/card';

// Import types
import {
  DenormalizeSortedRowComponentConfiguration,
  SchemaDefinition,
  FieldSchema,
  DataType,
} from '../../../types/unified-pipeline.types';

// Simple column interface for input
interface SimpleColumn {
  name: string;
  type?: string;
  id?: string;
}

// Props
interface DenormalizeSortedRowEditorProps {
  nodeId: string;
  nodeMetadata: any; // or CanvasNodeData
  inputColumns: SimpleColumn[];
  initialConfig?: DenormalizeSortedRowComponentConfiguration;
  onClose: () => void;
  onSave: (config: DenormalizeSortedRowComponentConfiguration) => void;
}

// ==================== Sort Key ====================
interface SortKey {
  id: string;
  field: string;
  direction: 'ASC' | 'DESC';
  nullsFirst: boolean;
  position: number;
}

// ==================== Denormalized Column ====================
interface DenormColumn {
  id: string;
  sourceField: string;
  outputField: string;
  aggregation: 'FIRST' | 'LAST' | 'ARRAY' | 'STRING_AGG' | 'JSON_AGG' | 'OBJECT_AGG' | 'SUM' | 'AVG' | 'MIN' | 'MAX';
  separator?: string;
  distinct?: boolean;
  position: number;
}

// ==================== State ====================
interface EditorState {
  groupByFields: string[];
  sortKeys: SortKey[];
  denormColumns: DenormColumn[];
  errorHandling: 'fail' | 'skip' | 'setNull';
  batchSize: number;
  parallelization: boolean;
  warnings: string[];
  isDirty: boolean;
}

type EditorAction =
  | { type: 'SET_GROUP_BY'; fields: string[] }
  | { type: 'ADD_SORT_KEY' }
  | { type: 'UPDATE_SORT_KEY'; id: string; updates: Partial<SortKey> }
  | { type: 'REMOVE_SORT_KEY'; id: string }
  | { type: 'REORDER_SORT_KEYS'; newOrder: SortKey[] }
  | { type: 'ADD_DENORM_COLUMN' }
  | { type: 'UPDATE_DENORM_COLUMN'; id: string; updates: Partial<DenormColumn> }
  | { type: 'REMOVE_DENORM_COLUMN'; id: string }
  | { type: 'REORDER_DENORM_COLUMNS'; newOrder: DenormColumn[] }
  | { type: 'SET_ERROR_HANDLING'; value: 'fail' | 'skip' | 'setNull' }
  | { type: 'SET_BATCH_SIZE'; value: number }
  | { type: 'SET_PARALLELIZATION'; value: boolean }
  | { type: 'SET_DIRTY' }
  | { type: 'LOAD_CONFIG'; config: DenormalizeSortedRowComponentConfiguration };

const generateId = () => `item-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

const createDefaultSortKey = (availableFields: string[], position: number): SortKey => ({
  id: generateId(),
  field: availableFields[0] || '',
  direction: 'ASC',
  nullsFirst: false,
  position,
});

const createDefaultDenormColumn = (availableFields: string[], position: number): DenormColumn => ({
  id: generateId(),
  sourceField: availableFields[0] || '',
  outputField: '',
  aggregation: 'FIRST',
  separator: ',',
  distinct: false,
  position,
});

const initialState = (inputColumns: SimpleColumn[]): EditorState => {
  const fieldNames = inputColumns.map(col => col.name);
  return {
    groupByFields: fieldNames.length > 0 ? [fieldNames[0]] : [],
    sortKeys: fieldNames.length > 0 ? [createDefaultSortKey(fieldNames, 0)] : [],
    denormColumns: fieldNames.length > 0 ? [createDefaultDenormColumn(fieldNames, 0)] : [],
    errorHandling: 'fail',
    batchSize: 1000,
    parallelization: false,
    warnings: [],
    isDirty: false,
  };
};

const reducer = (state: EditorState, action: EditorAction): EditorState => {
  switch (action.type) {
    case 'SET_GROUP_BY':
      return { ...state, groupByFields: action.fields, isDirty: true };
    case 'ADD_SORT_KEY': {
      const newKey = createDefaultSortKey(state.sortKeys.map(k => k.field), state.sortKeys.length);
      return { ...state, sortKeys: [...state.sortKeys, newKey], isDirty: true };
    }
    case 'UPDATE_SORT_KEY': {
      const updated = state.sortKeys.map(k =>
        k.id === action.id ? { ...k, ...action.updates } : k
      );
      return { ...state, sortKeys: updated, isDirty: true };
    }
    case 'REMOVE_SORT_KEY': {
      const filtered = state.sortKeys.filter(k => k.id !== action.id);
      return { ...state, sortKeys: filtered, isDirty: true };
    }
    case 'REORDER_SORT_KEYS':
      return { ...state, sortKeys: action.newOrder, isDirty: true };
    case 'ADD_DENORM_COLUMN': {
      const newCol = createDefaultDenormColumn(state.denormColumns.map(c => c.sourceField), state.denormColumns.length);
      return { ...state, denormColumns: [...state.denormColumns, newCol], isDirty: true };
    }
    case 'UPDATE_DENORM_COLUMN': {
      const updated = state.denormColumns.map(c =>
        c.id === action.id ? { ...c, ...action.updates } : c
      );
      return { ...state, denormColumns: updated, isDirty: true };
    }
    case 'REMOVE_DENORM_COLUMN': {
      const filtered = state.denormColumns.filter(c => c.id !== action.id);
      return { ...state, denormColumns: filtered, isDirty: true };
    }
    case 'REORDER_DENORM_COLUMNS':
      return { ...state, denormColumns: action.newOrder, isDirty: true };
    case 'SET_ERROR_HANDLING':
      return { ...state, errorHandling: action.value, isDirty: true };
    case 'SET_BATCH_SIZE':
      return { ...state, batchSize: action.value, isDirty: true };
    case 'SET_PARALLELIZATION':
      return { ...state, parallelization: action.value, isDirty: true };
    case 'SET_DIRTY':
      return { ...state, isDirty: true };
    case 'LOAD_CONFIG': {
      const config = action.config;
      return {
        groupByFields: config.groupByFields,
        sortKeys: config.sortKeys.map(k => ({ ...k, id: generateId() })),
        denormColumns: config.denormalizedColumns.map((c, idx) => ({
          id: generateId(),
          sourceField: c.sourceField,
          outputField: c.outputField,
          aggregation: c.aggregation,
          separator: c.separator,
          distinct: c.distinct,
          position: idx,
        })),
        errorHandling: config.errorHandling,
        batchSize: config.batchSize,
        parallelization: config.parallelization,
        warnings: config.compilerMetadata?.warnings || [],
        isDirty: false,
      };
    }
    default:
      return state;
  }
};

// ==================== Helper: Build output schema ====================
const buildOutputSchema = (
  state: EditorState,
  inputColumns: SimpleColumn[]
): SchemaDefinition => {
  const groupByFields = state.groupByFields.map(fieldName => {
    const source = inputColumns.find(col => col.name === fieldName);
    return {
      id: `out_${fieldName}`,
      name: fieldName,
      type: (source?.type as DataType) || 'STRING',
      nullable: true,
      isKey: true,
    } as FieldSchema;
  });

  const denormFields = state.denormColumns.map((col, _idx) => {
    const source = inputColumns.find(c => c.name === col.sourceField);
    // Determine output type based on aggregation
    let outputType: DataType = 'STRING';
    if (col.aggregation === 'SUM' || col.aggregation === 'AVG' || col.aggregation === 'MIN' || col.aggregation === 'MAX') {
      outputType = 'DECIMAL';
    } else if (col.aggregation === 'ARRAY') {
      outputType = 'STRING'; // but actually array – we'll keep as STRING for now; real system would use array type
    } else if (col.aggregation === 'JSON_AGG' || col.aggregation === 'OBJECT_AGG') {
      outputType = 'STRING';
    } else {
      outputType = source?.type as DataType || 'STRING';
    }

    return {
      id: col.id,
      name: col.outputField || `${col.aggregation.toLowerCase()}_${col.sourceField}`,
      type: outputType,
      nullable: true,
      isKey: false,
      description: `Denormalized via ${col.aggregation}`,
    } as FieldSchema;
  });

  return {
    id: `denorm_output_${Date.now()}`,
    name: 'Denormalized Output',
    fields: [...groupByFields, ...denormFields],
    isTemporary: false,
    isMaterialized: false,
  };
};

// ==================== Main Component ====================
export const DenormalizeSortedRowEditor: React.FC<DenormalizeSortedRowEditorProps> = ({
  nodeId,
  nodeMetadata,
  inputColumns,
  onClose,
  onSave,
}) => {
  const [state, dispatch] = useReducer(reducer, inputColumns, initialState);
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [validationErrors, setValidationErrors] = useState<string[]>([]);

  const fieldNames = inputColumns.map(col => col.name);
  const nonKeyFields = fieldNames.filter(f => !state.groupByFields.includes(f));

  // Validate on every change
  useEffect(() => {
    const errors: string[] = [];
    if (state.groupByFields.length === 0) {
      errors.push('At least one group-by key is required.');
    }
    if (state.denormColumns.length === 0) {
      errors.push('At least one denormalized column is required.');
    }
    // Check for duplicate output names
    const outputNames = state.denormColumns.map(c => c.outputField || `${c.aggregation.toLowerCase()}_${c.sourceField}`);
    const duplicates = outputNames.filter((name, idx) => outputNames.indexOf(name) !== idx);
    if (duplicates.length > 0) {
      errors.push(`Duplicate output column names: ${duplicates.join(', ')}`);
    }
    // Warn if FIRST/LAST used without sort keys
    const usesFirstLast = state.denormColumns.some(c => c.aggregation === 'FIRST' || c.aggregation === 'LAST');
    if (usesFirstLast && state.sortKeys.length === 0) {
      errors.push('FIRST/LAST aggregations require at least one sort key to define order.');
    }
    setValidationErrors(errors);
  }, [state]);

  const handleSave = () => {
    if (validationErrors.length > 0) return;

    const outputSchema = buildOutputSchema(state, inputColumns);
    const dependencies = [
      ...state.groupByFields,
      ...state.denormColumns.map(c => c.sourceField),
    ];

    const config: DenormalizeSortedRowComponentConfiguration = {
      version: '1.0',
      groupByFields: state.groupByFields,
      sortKeys: state.sortKeys.map((k, idx) => ({
        field: k.field,
        direction: k.direction,
        nullsFirst: k.nullsFirst,
        position: idx,
      })),
      denormalizedColumns: state.denormColumns.map((c, _idx) => ({
        sourceField: c.sourceField,
        outputField: c.outputField || `${c.aggregation.toLowerCase()}_${c.sourceField}`,
        aggregation: c.aggregation,
        separator: c.separator,
        distinct: c.distinct,
      })),
      outputSchema,
      errorHandling: state.errorHandling,
      batchSize: state.batchSize,
      parallelization: state.parallelization,
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        validationStatus: validationErrors.length === 0 ? 'VALID' : 'ERROR',
        warnings: state.warnings,
        dependencies,
        estimatedOutputRows: undefined, // can be computed later
      },
    };

    onSave(config);
  };

  // Move sort key up/down (for reordering)
  const moveSortKey = (id: string, direction: 'up' | 'down') => {
    const index = state.sortKeys.findIndex(k => k.id === id);
    if (index === -1) return;
    const newKeys = [...state.sortKeys];
    if (direction === 'up' && index > 0) {
      [newKeys[index - 1], newKeys[index]] = [newKeys[index], newKeys[index - 1]];
    } else if (direction === 'down' && index < newKeys.length - 1) {
      [newKeys[index], newKeys[index + 1]] = [newKeys[index + 1], newKeys[index]];
    }
    // Update positions
    newKeys.forEach((k, i) => (k.position = i));
    dispatch({ type: 'REORDER_SORT_KEYS', newOrder: newKeys });
  };

  const moveDenormColumn = (id: string, direction: 'up' | 'down') => {
    const index = state.denormColumns.findIndex(c => c.id === id);
    if (index === -1) return;
    const newCols = [...state.denormColumns];
    if (direction === 'up' && index > 0) {
      [newCols[index - 1], newCols[index]] = [newCols[index], newCols[index - 1]];
    } else if (direction === 'down' && index < newCols.length - 1) {
      [newCols[index], newCols[index + 1]] = [newCols[index + 1], newCols[index]];
    }
    newCols.forEach((c, i) => (c.position = i));
    dispatch({ type: 'REORDER_DENORM_COLUMNS', newOrder: newCols });
  };

  return (
    <div className="fixed inset-0 z-[9999] bg-black bg-opacity-70 flex items-center justify-center p-4">
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        className="bg-white dark:bg-gray-900 rounded-xl shadow-2xl w-full max-w-6xl max-h-[90vh] overflow-hidden flex flex-col"
      >
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b bg-gradient-to-r from-blue-50 to-gray-50 dark:from-gray-800 dark:to-gray-900">
          <div>
            <h2 className="text-xl font-bold flex items-center gap-2">
              <span>🔄 Denormalize (Sorted Row)</span>
              <Badge variant="outline" className="text-xs">
                {nodeMetadata?.name || nodeId}
              </Badge>
              {validationErrors.length === 0 ? (
                <Badge className="bg-green-100 text-green-800">Valid</Badge>
              ) : (
                <Badge className="bg-red-100 text-red-800">Errors</Badge>
              )}
            </h2>
            <p className="text-sm text-gray-600 dark:text-gray-400 mt-1">
              Collapse multiple sorted rows into one row per group.
            </p>
          </div>
          <button
            onClick={onClose}
            className="p-2 hover:bg-gray-200 dark:hover:bg-gray-700 rounded-full transition-colors"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        {/* Main content – scrollable */}
        <div className="flex-1 overflow-y-auto p-6 space-y-6">
          {/* Group By Keys */}
          <Card>
            <CardHeader>
              <CardTitle className="text-sm font-medium">Group By Keys</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="border rounded-md p-4 bg-gray-50 dark:bg-gray-800">
                <div className="flex flex-wrap gap-2">
                  {fieldNames.map(field => (
                    <label key={field} className="flex items-center gap-2 text-sm">
                      <input
                        type="checkbox"
                        checked={state.groupByFields.includes(field)}
                        onChange={(e) => {
                          const newGroup = e.target.checked
                            ? [...state.groupByFields, field]
                            : state.groupByFields.filter(f => f !== field);
                          dispatch({ type: 'SET_GROUP_BY', fields: newGroup });
                        }}
                        className="rounded border-gray-300"
                      />
                      {field}
                    </label>
                  ))}
                </div>
                <p className="text-xs text-gray-500 mt-2">
                  One row will be produced for each unique combination of these keys.
                </p>
              </div>
            </CardContent>
          </Card>

          {/* Sort Order */}
          <Card>
            <CardHeader className="flex flex-row items-center justify-between">
              <CardTitle className="text-sm font-medium">Sort Order (within groups)</CardTitle>
              <Button
                variant="outline"
                size="sm"
                onClick={() => dispatch({ type: 'ADD_SORT_KEY' })}
              >
                <Plus className="h-4 w-4 mr-1" /> Add Sort Key
              </Button>
            </CardHeader>
            <CardContent>
              <div className="space-y-2">
                {state.sortKeys.map((key, idx) => (
                  <div key={key.id} className="flex items-center gap-2 border rounded-md p-2 bg-white dark:bg-gray-800">
                    <div className="cursor-grab text-gray-400">
                      <GripVertical className="h-5 w-5" />
                    </div>
                    <div className="flex-1 grid grid-cols-4 gap-2">
                      <Select
                        value={key.field}
                        onValueChange={(val) =>
                          dispatch({ type: 'UPDATE_SORT_KEY', id: key.id, updates: { field: val } })
                        }
                      >
                        <SelectTrigger>
                          <SelectValue placeholder="Column" />
                        </SelectTrigger>
                        <SelectContent>
                          {fieldNames.map(f => (
                            <SelectItem key={f} value={f}>{f}</SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                      <Select
                        value={key.direction}
                        onValueChange={(val: 'ASC' | 'DESC') =>
                          dispatch({ type: 'UPDATE_SORT_KEY', id: key.id, updates: { direction: val } })
                        }
                      >
                        <SelectTrigger>
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="ASC">ASC</SelectItem>
                          <SelectItem value="DESC">DESC</SelectItem>
                        </SelectContent>
                      </Select>
                      <div className="flex items-center gap-2">
                        <Checkbox
                          id={`nulls-${key.id}`}
                          checked={key.nullsFirst}
                          onChange={(e) =>
                            dispatch({ type: 'UPDATE_SORT_KEY', id: key.id, updates: { nullsFirst: e.target.checked } })
                          }
                        />
                        <Label htmlFor={`nulls-${key.id}`}>Nulls First</Label>
                      </div>
                      <div className="flex items-center gap-1">
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => moveSortKey(key.id, 'up')}
                          disabled={idx === 0}
                        >
                          <ChevronUp className="h-4 w-4" />
                        </Button>
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => moveSortKey(key.id, 'down')}
                          disabled={idx === state.sortKeys.length - 1}
                        >
                          <ChevronDown className="h-4 w-4" />
                        </Button>
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => dispatch({ type: 'REMOVE_SORT_KEY', id: key.id })}
                          className="text-red-500 hover:text-red-700"
                        >
                          <Trash2 className="h-4 w-4" />
                        </Button>
                      </div>
                    </div>
                  </div>
                ))}
                {state.sortKeys.length === 0 && (
                  <p className="text-sm text-gray-500 italic">No sort keys defined. Add at least one for predictable FIRST/LAST aggregation.</p>
                )}
              </div>
            </CardContent>
          </Card>

          {/* Denormalized Columns */}
          <Card>
            <CardHeader className="flex flex-row items-center justify-between">
              <CardTitle className="text-sm font-medium">Denormalized Columns</CardTitle>
              <Button
                variant="outline"
                size="sm"
                onClick={() => dispatch({ type: 'ADD_DENORM_COLUMN' })}
              >
                <Plus className="h-4 w-4 mr-1" /> Add Column
              </Button>
            </CardHeader>
            <CardContent>
              <div className="space-y-2">
                {state.denormColumns.map((col, idx) => (
                  <div key={col.id} className="flex items-center gap-2 border rounded-md p-2 bg-white dark:bg-gray-800">
                    <div className="cursor-grab text-gray-400">
                      <GripVertical className="h-5 w-5" />
                    </div>
                    <div className="flex-1 grid grid-cols-6 gap-2">
                      <Select
                        value={col.sourceField}
                        onValueChange={(val) =>
                          dispatch({ type: 'UPDATE_DENORM_COLUMN', id: col.id, updates: { sourceField: val } })
                        }
                      >
                        <SelectTrigger>
                          <SelectValue placeholder="Source column" />
                        </SelectTrigger>
                        <SelectContent>
                          {nonKeyFields.map(f => (
                            <SelectItem key={f} value={f}>{f}</SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                      <Input
                        placeholder="Output name"
                        value={col.outputField}
                        onChange={(e) =>
                          dispatch({ type: 'UPDATE_DENORM_COLUMN', id: col.id, updates: { outputField: e.target.value } })
                        }
                      />
                      <Select
                        value={col.aggregation}
                        onValueChange={(val: any) =>
                          dispatch({ type: 'UPDATE_DENORM_COLUMN', id: col.id, updates: { aggregation: val } })
                        }
                      >
                        <SelectTrigger>
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="FIRST">FIRST</SelectItem>
                          <SelectItem value="LAST">LAST</SelectItem>
                          <SelectItem value="ARRAY">ARRAY</SelectItem>
                          <SelectItem value="STRING_AGG">STRING_AGG</SelectItem>
                          <SelectItem value="JSON_AGG">JSON_AGG</SelectItem>
                          <SelectItem value="OBJECT_AGG">OBJECT_AGG</SelectItem>
                          <SelectItem value="SUM">SUM</SelectItem>
                          <SelectItem value="AVG">AVG</SelectItem>
                          <SelectItem value="MIN">MIN</SelectItem>
                          <SelectItem value="MAX">MAX</SelectItem>
                        </SelectContent>
                      </Select>
                      {col.aggregation === 'STRING_AGG' && (
                        <Input
                          placeholder="Separator"
                          value={col.separator || ','}
                          onChange={(e) =>
                            dispatch({ type: 'UPDATE_DENORM_COLUMN', id: col.id, updates: { separator: e.target.value } })
                          }
                          className="w-20"
                        />
                      )}
                      {(col.aggregation === 'ARRAY' || col.aggregation === 'STRING_AGG') && (
                        <div className="flex items-center gap-2">
                          <Checkbox
                            id={`distinct-${col.id}`}
                            checked={col.distinct}
                            onChange={(e) =>
                              dispatch({ type: 'UPDATE_DENORM_COLUMN', id: col.id, updates: { distinct: e.target.checked } })
                            }
                          />
                          <Label htmlFor={`distinct-${col.id}`}>Distinct</Label>
                        </div>
                      )}
                      <div className="flex items-center gap-1">
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => moveDenormColumn(col.id, 'up')}
                          disabled={idx === 0}
                        >
                          <ChevronUp className="h-4 w-4" />
                        </Button>
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => moveDenormColumn(col.id, 'down')}
                          disabled={idx === state.denormColumns.length - 1}
                        >
                          <ChevronDown className="h-4 w-4" />
                        </Button>
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => dispatch({ type: 'REMOVE_DENORM_COLUMN', id: col.id })}
                          className="text-red-500 hover:text-red-700"
                        >
                          <Trash2 className="h-4 w-4" />
                        </Button>
                      </div>
                    </div>
                  </div>
                ))}
                {state.denormColumns.length === 0 && (
                  <p className="text-sm text-gray-500 italic">No denormalized columns defined.</p>
                )}
              </div>
            </CardContent>
          </Card>

          {/* Output Schema Preview */}
          <Card>
            <CardHeader>
              <CardTitle className="text-sm font-medium">Output Schema Preview</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="border rounded-md overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="bg-gray-100 dark:bg-gray-800">
                    <tr>
                      <th className="px-4 py-2 text-left">Column Name</th>
                      <th className="px-4 py-2 text-left">Type</th>
                      <th className="px-4 py-2 text-left">Key</th>
                      <th className="px-4 py-2 text-left">Source</th>
                    </tr>
                  </thead>
                  <tbody>
                    {state.groupByFields.map(field => (
                      <tr key={field} className="border-t">
                        <td className="px-4 py-2 font-medium">{field}</td>
                        <td className="px-4 py-2">{inputColumns.find(c => c.name === field)?.type || 'STRING'}</td>
                        <td className="px-4 py-2">✓</td>
                        <td className="px-4 py-2">Group Key</td>
                      </tr>
                    ))}
                    {state.denormColumns.map(col => {
                      const outName = col.outputField || `${col.aggregation.toLowerCase()}_${col.sourceField}`;
                      const sourceType = inputColumns.find(c => c.name === col.sourceField)?.type || 'STRING';
                      let outType = sourceType;
                      if (col.aggregation === 'ARRAY') outType = `${sourceType}[]`;
                      if (col.aggregation === 'JSON_AGG') outType = 'JSON';
                      if (col.aggregation === 'OBJECT_AGG') outType = 'JSON';
                      if (['SUM','AVG','MIN','MAX'].includes(col.aggregation)) outType = 'DECIMAL';
                      return (
                        <tr key={col.id} className="border-t">
                          <td className="px-4 py-2">{outName}</td>
                          <td className="px-4 py-2">{outType}</td>
                          <td className="px-4 py-2"></td>
                          <td className="px-4 py-2">{col.aggregation} of {col.sourceField}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </CardContent>
          </Card>

          {/* Advanced Options */}
          <Card>
            <CardHeader
              className="cursor-pointer"
              onClick={() => setShowAdvanced(!showAdvanced)}
            >
              <div className="flex items-center justify-between">
                <CardTitle className="text-sm font-medium">Advanced Options</CardTitle>
                <ChevronDown className={`h-4 w-4 transition-transform ${showAdvanced ? 'rotate-180' : ''}`} />
              </div>
            </CardHeader>
            {showAdvanced && (
              <CardContent className="space-y-4">
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <Label>Error Handling</Label>
                    <Select
                      value={state.errorHandling}
                      onValueChange={(val: any) => dispatch({ type: 'SET_ERROR_HANDLING', value: val })}
                    >
                      <SelectTrigger>
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="fail">Fail on error</SelectItem>
                        <SelectItem value="skip">Skip row</SelectItem>
                        <SelectItem value="setNull">Set NULL and continue</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <div>
                    <Label>Batch Size</Label>
                    <Input
                      type="number"
                      min={1}
                      value={state.batchSize}
                      onChange={(e) => dispatch({ type: 'SET_BATCH_SIZE', value: parseInt(e.target.value) || 1000 })}
                    />
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  <Checkbox
                    id="parallel"
                    checked={state.parallelization}
                    onChange={(e) =>
                      dispatch({ type: 'SET_PARALLELIZATION', value: e.target.checked })
                    }
                  />
                  <Label htmlFor="parallel">Enable parallel processing</Label>
                </div>
              </CardContent>
            )}
          </Card>

          {/* Validation Errors */}
          {validationErrors.length > 0 && (
            <div className="bg-red-50 border border-red-200 rounded-lg p-4">
              <div className="flex items-center gap-2 text-red-800">
                <AlertCircle className="h-5 w-5" />
                <h3 className="font-semibold">Validation Errors</h3>
              </div>
              <ul className="list-disc list-inside text-sm text-red-700 mt-2">
                {validationErrors.map((err, i) => (
                  <li key={i}>{err}</li>
                ))}
              </ul>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between px-6 py-4 border-t bg-gray-50 dark:bg-gray-800">
          <div className="text-sm text-gray-500">
            {state.isDirty && <span className="text-yellow-600">Unsaved changes</span>}
          </div>
          <div className="flex gap-2">
            <Button variant="outline" onClick={onClose}>
              Cancel
            </Button>
            <Button
              onClick={handleSave}
              disabled={validationErrors.length > 0}
              className="bg-green-600 hover:bg-green-700 text-white"
            >
              <Check className="h-4 w-4 mr-2" />
              Save Configuration
            </Button>
          </div>
        </div>
      </motion.div>
    </div>
  );
};