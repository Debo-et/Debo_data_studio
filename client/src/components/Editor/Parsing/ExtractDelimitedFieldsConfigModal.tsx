// src/components/Editor/ExtractDelimitedFieldsConfigModal.tsx
import React, { useReducer, useCallback, useMemo, useEffect, useState } from 'react'; // ✅ Added useState
import { motion } from 'framer-motion';
import { Plus, GripVertical, Trash2 } from 'lucide-react';

// UI components that exist in your project
import { Button } from '../../ui/Button';
import { Card, CardContent } from '../../ui/card';
import { Badge } from '../../ui/badge';
import { Input } from '../../ui/input';
import { Label } from '../../ui/label';

// Types – note: we do NOT import DataType as a value; it's only a type.
import { ExtractDelimitedFieldsConfiguration } from '../../../types/unified-pipeline.types';
// import { DataType } from '../../types/metadata'; // REMOVED – we use string literals instead

// ==================== Local Types ====================
// Define a local union of allowed data type strings (must match the actual DataType union)
type DataTypeOption = 'STRING' | 'INTEGER' | 'DECIMAL' | 'BOOLEAN' | 'DATE' | 'TIMESTAMP';

interface UIOutputColumn {
  id: string;
  name: string;
  type: DataTypeOption;          // use local union
  length?: number;
  precision?: number;
  scale?: number;
  position: number;
}

interface ExtractEditorState {
  sourceColumn: string;
  delimiter: string;
  quoteChar?: string;
  escapeChar?: string;
  trimWhitespace: boolean;
  nullIfEmpty: boolean;
  outputColumns: UIOutputColumn[];
  errorHandling: 'fail' | 'skip' | 'setNull';
  parallelization: boolean;
  batchSize?: number;
}

// ==================== Modal Props ====================
interface ExtractDelimitedFieldsConfigModalProps {
  isOpen: boolean;
  onClose: () => void;
  nodeId: string;
  nodeName: string;
  inputColumns: Array<{ name: string; type: string }>;
  initialConfig?: ExtractDelimitedFieldsConfiguration;
  onSave: (config: ExtractDelimitedFieldsConfiguration) => void;
}

// ==================== Constants ====================
const COMMON_DELIMITERS = [
  { label: 'Comma', value: ',' },
  { label: 'Tab', value: '\t' },
  { label: 'Pipe', value: '|' },
  { label: 'Space', value: ' ' },
  { label: 'Semicolon', value: ';' },
];

// Use string literals that match the actual DataType union
const DATA_TYPE_OPTIONS: { value: DataTypeOption; label: string }[] = [
  { value: 'STRING', label: 'String' },
  { value: 'INTEGER', label: 'Integer' },
  { value: 'DECIMAL', label: 'Decimal' },
  { value: 'BOOLEAN', label: 'Boolean' },
  { value: 'DATE', label: 'Date' },
  { value: 'TIMESTAMP', label: 'Timestamp' },
];

const ERROR_HANDLING_OPTIONS = [
  { value: 'fail', label: 'Fail on error' },
  { value: 'skip', label: 'Skip row on error' },
  { value: 'setNull', label: 'Set extracted value to NULL' },
];

// ==================== Reducer ====================
type EditorAction =
  | { type: 'SET_SOURCE_COLUMN'; value: string }
  | { type: 'SET_DELIMITER'; value: string }
  | { type: 'SET_QUOTE_CHAR'; value?: string }
  | { type: 'SET_ESCAPE_CHAR'; value?: string }
  | { type: 'SET_TRIM'; value: boolean }
  | { type: 'SET_NULL_IF_EMPTY'; value: boolean }
  | { type: 'ADD_COLUMN' }
  | { type: 'REMOVE_COLUMN'; id: string }
  | { type: 'UPDATE_COLUMN'; id: string; field: keyof UIOutputColumn; value: any }
  | { type: 'REORDER_COLUMNS'; fromIndex: number; toIndex: number }
  | { type: 'SET_ERROR_HANDLING'; value: 'fail' | 'skip' | 'setNull' }
  | { type: 'SET_PARALLELIZATION'; value: boolean }
  | { type: 'SET_BATCH_SIZE'; value?: number }
  | { type: 'LOAD_INITIAL'; config: ExtractDelimitedFieldsConfiguration; columns: Array<{ name: string; type: string }> };

const generateId = () => `col-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

const createEmptyColumn = (position: number): UIOutputColumn => ({
  id: generateId(),
  name: `Field${position}`,
  type: 'STRING',      // default type as string literal
  position,
});

const initialState: ExtractEditorState = {
  sourceColumn: '',
  delimiter: ',',
  quoteChar: undefined,
  escapeChar: undefined,
  trimWhitespace: true,
  nullIfEmpty: false,
  outputColumns: [createEmptyColumn(1)],
  errorHandling: 'fail',
  parallelization: false,
  batchSize: 1000,
};

const extractReducer = (state: ExtractEditorState, action: EditorAction): ExtractEditorState => {
  switch (action.type) {
    case 'SET_SOURCE_COLUMN':
      return { ...state, sourceColumn: action.value };
    case 'SET_DELIMITER':
      return { ...state, delimiter: action.value };
    case 'SET_QUOTE_CHAR':
      return { ...state, quoteChar: action.value };
    case 'SET_ESCAPE_CHAR':
      return { ...state, escapeChar: action.value };
    case 'SET_TRIM':
      return { ...state, trimWhitespace: action.value };
    case 'SET_NULL_IF_EMPTY':
      return { ...state, nullIfEmpty: action.value };
    case 'ADD_COLUMN': {
      const newPosition = state.outputColumns.length + 1;
      return {
        ...state,
        outputColumns: [...state.outputColumns, createEmptyColumn(newPosition)],
      };
    }
    case 'REMOVE_COLUMN':
      if (state.outputColumns.length <= 1) return state;
      return {
        ...state,
        outputColumns: state.outputColumns
          .filter(col => col.id !== action.id)
          .map((col, idx) => ({ ...col, position: idx + 1 })),
      };
    case 'UPDATE_COLUMN':
      return {
        ...state,
        outputColumns: state.outputColumns.map(col =>
          col.id === action.id ? { ...col, [action.field]: action.value } : col
        ),
      };
    case 'REORDER_COLUMNS': {
      const { fromIndex, toIndex } = action;
      const reordered = [...state.outputColumns];
      const [moved] = reordered.splice(fromIndex, 1);
      reordered.splice(toIndex, 0, moved);
      reordered.forEach((col, idx) => (col.position = idx + 1));
      return { ...state, outputColumns: reordered };
    }
    case 'SET_ERROR_HANDLING':
      return { ...state, errorHandling: action.value };
    case 'SET_PARALLELIZATION':
      return { ...state, parallelization: action.value };
    case 'SET_BATCH_SIZE':
      return { ...state, batchSize: action.value };
    case 'LOAD_INITIAL': {
      const { config, columns } = action;
      const sourceCol = columns.find(c => c.name === config.sourceColumn) ? config.sourceColumn : '';
      return {
        sourceColumn: sourceCol,
        delimiter: config.delimiter || ',',
        quoteChar: config.quoteChar,
        escapeChar: config.escapeChar,
        trimWhitespace: config.trimWhitespace ?? true,
        nullIfEmpty: config.nullIfEmpty ?? false,
        outputColumns: config.outputColumns.map((col, idx) => ({
          id: col.id || generateId(),
          name: col.name,
          type: col.type as DataTypeOption,    // cast to our local union (safe if strings match)
          length: col.length,
          precision: col.precision,
          scale: col.scale,
          position: idx + 1,
        })),
        errorHandling: config.errorHandling || 'fail',
        parallelization: config.parallelization ?? false,
        batchSize: config.batchSize ?? 1000,
      };
    }
    default:
      return state;
  }
};

// ==================== Helper: Generate example output ====================
const generateExample = (state: ExtractEditorState): string => {
  if (!state.sourceColumn) return 'Select a source column';
  const delimiter = state.delimiter === '\t' ? '\\t' : state.delimiter;
  return `Extract from "${state.sourceColumn}" using delimiter "${delimiter}" → ${
    state.outputColumns.map(c => c.name).join(', ')
  }`;
};

// ==================== Main Component ====================
export const ExtractDelimitedFieldsConfigModal: React.FC<ExtractDelimitedFieldsConfigModalProps> = ({
  isOpen,
  onClose,
  nodeId,
  nodeName,
  inputColumns,
  initialConfig,
  onSave,
}) => {
  const [state, dispatch] = useReducer(extractReducer, initialState);
  const [activeTab, setActiveTab] = useState<'basic' | 'columns' | 'advanced'>('basic');

  useEffect(() => {
    if (isOpen && initialConfig) {
      dispatch({ type: 'LOAD_INITIAL', config: initialConfig, columns: inputColumns });
    } else if (isOpen && !initialConfig) {
      if (inputColumns.length > 0) {
        dispatch({ type: 'SET_SOURCE_COLUMN', value: inputColumns[0].name });
      }
    }
  }, [isOpen, initialConfig, inputColumns]);

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
    dispatch({ type: 'REORDER_COLUMNS', fromIndex, toIndex: targetIndex });
  }, []);

  const handleSave = useCallback(() => {
    const config: ExtractDelimitedFieldsConfiguration = {
      version: '1.0',
      sourceColumn: state.sourceColumn,
      delimiter: state.delimiter,
      quoteChar: state.quoteChar,
      escapeChar: state.escapeChar,
      trimWhitespace: state.trimWhitespace,
      nullIfEmpty: state.nullIfEmpty,
      outputColumns: state.outputColumns.map(col => ({
        id: col.id,
        name: col.name,
        type: col.type,          // this is a string literal, matching DataType union
        length: col.length,
        precision: col.precision,
        scale: col.scale,
        position: col.position,
      })),
      errorHandling: state.errorHandling,
      parallelization: state.parallelization,
      batchSize: state.batchSize,
      compilerMetadata: {
        lastModified: new Date().toISOString(),
      },
    };
    onSave(config);
  }, [state, onSave]);

  const exampleText = useMemo(() => generateExample(state), [state]);

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
              <span>🔍 Extract Delimited Fields</span>
              <Badge variant="outline">tExtractDelimitedFields</Badge>
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

        {/* Tab Navigation */}
        <div className="flex border-b dark:border-gray-700 px-6 pt-2">
          <button
            className={`px-4 py-2 text-sm font-medium ${
              activeTab === 'basic'
                ? 'text-blue-600 border-b-2 border-blue-600'
                : 'text-gray-500 hover:text-gray-700'
            }`}
            onClick={() => setActiveTab('basic')}
          >
            Basic
          </button>
          <button
            className={`px-4 py-2 text-sm font-medium ${
              activeTab === 'columns'
                ? 'text-blue-600 border-b-2 border-blue-600'
                : 'text-gray-500 hover:text-gray-700'
            }`}
            onClick={() => setActiveTab('columns')}
          >
            Output Columns
          </button>
          <button
            className={`px-4 py-2 text-sm font-medium ${
              activeTab === 'advanced'
                ? 'text-blue-600 border-b-2 border-blue-600'
                : 'text-gray-500 hover:text-gray-700'
            }`}
            onClick={() => setActiveTab('advanced')}
          >
            Advanced
          </button>
        </div>

        {/* Main content area */}
        <div className="flex flex-1 overflow-hidden">
          {/* Left panel: column reference */}
          <div className="w-64 border-r dark:border-gray-700 p-4 bg-gray-50 dark:bg-gray-800/50 overflow-auto">
            <h3 className="text-sm font-medium mb-3">Available Input Columns</h3>
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

          {/* Right panel: configuration */}
          <div className="flex-1 p-6 overflow-auto">
            {activeTab === 'basic' && (
              <div className="space-y-6">
                {/* Source column */}
                <div className="space-y-2">
                  <Label htmlFor="sourceColumn">Source Column</Label>
                  <select
                    id="sourceColumn"
                    value={state.sourceColumn}
                    onChange={(e) => dispatch({ type: 'SET_SOURCE_COLUMN', value: e.target.value })}
                    className="w-full px-3 py-2 text-sm border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100"
                  >
                    <option value="">Select column</option>
                    {inputColumns.map(col => (
                      <option key={col.name} value={col.name}>
                        {col.name} ({col.type})
                      </option>
                    ))}
                  </select>
                </div>

                {/* Delimiter */}
                <div className="space-y-2">
                  <Label>Delimiter</Label>
                  <div className="flex gap-2 flex-wrap">
                    {COMMON_DELIMITERS.map(d => (
                      <Button
                        key={d.value}
                        type="button"
                        variant={state.delimiter === d.value ? 'default' : 'outline'}
                        size="sm"
                        onClick={() => dispatch({ type: 'SET_DELIMITER', value: d.value })}
                      >
                        {d.label}
                      </Button>
                    ))}
                  </div>
                  <Input
                    value={state.delimiter}
                    onChange={(e) => dispatch({ type: 'SET_DELIMITER', value: e.target.value })}
                    placeholder="Custom delimiter"
                    className="mt-2"
                  />
                </div>

                {/* Quote & Escape */}
                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label htmlFor="quoteChar">Quote Character (optional)</Label>
                    <Input
                      id="quoteChar"
                      value={state.quoteChar || ''}
                      onChange={(e) => dispatch({ type: 'SET_QUOTE_CHAR', value: e.target.value || undefined })}
                      placeholder='e.g., "'
                      maxLength={1}
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="escapeChar">Escape Character (optional)</Label>
                    <Input
                      id="escapeChar"
                      value={state.escapeChar || ''}
                      onChange={(e) => dispatch({ type: 'SET_ESCAPE_CHAR', value: e.target.value || undefined })}
                      placeholder="e.g., \\"
                      maxLength={1}
                    />
                  </div>
                </div>

                {/* Checkboxes - using native input */}
                <div className="space-y-3">
                  <div className="flex items-center space-x-2">
                    <input
                      type="checkbox"
                      id="trim"
                      checked={state.trimWhitespace}
                      onChange={(e) => dispatch({ type: 'SET_TRIM', value: e.target.checked })}
                      className="h-4 w-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                    />
                    <Label htmlFor="trim" className="cursor-pointer">
                      Trim whitespace from extracted values
                    </Label>
                  </div>
                  <div className="flex items-center space-x-2">
                    <input
                      type="checkbox"
                      id="nullIfEmpty"
                      checked={state.nullIfEmpty}
                      onChange={(e) => dispatch({ type: 'SET_NULL_IF_EMPTY', value: e.target.checked })}
                      className="h-4 w-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                    />
                    <Label htmlFor="nullIfEmpty" className="cursor-pointer">
                      Treat empty strings as NULL
                    </Label>
                  </div>
                </div>

                {/* Example preview */}
                <Card>
                  <CardContent className="p-4">
                    <p className="text-sm font-medium mb-1">Preview</p>
                    <pre className="text-xs bg-gray-100 dark:bg-gray-800 p-2 rounded">
                      {exampleText}
                    </pre>
                  </CardContent>
                </Card>
              </div>
            )}

            {activeTab === 'columns' && (
              <div className="space-y-4">
                <div className="flex justify-between items-center">
                  <h3 className="text-sm font-medium">Output Column Definitions</h3>
                  <Button size="sm" onClick={() => dispatch({ type: 'ADD_COLUMN' })}>
                    <Plus className="h-4 w-4 mr-1" /> Add Column
                  </Button>
                </div>

                {state.outputColumns.map((col, index) => (
                  <div
                    key={col.id}
                    className="flex items-start gap-2 p-3 border rounded-lg dark:border-gray-700 bg-white dark:bg-gray-800"
                    draggable
                    onDragStart={(e) => handleDragStart(e, index)}
                    onDragOver={handleDragOver}
                    onDrop={(e) => handleDrop(e, index)}
                  >
                    <div className="cursor-move mt-2 text-gray-400 hover:text-gray-600">
                      <GripVertical className="h-4 w-4" />
                    </div>
                    <div className="flex-1 grid grid-cols-1 sm:grid-cols-5 gap-2">
                      {/* Column name */}
                      <Input
                        value={col.name}
                        onChange={(e) =>
                          dispatch({ type: 'UPDATE_COLUMN', id: col.id, field: 'name', value: e.target.value })
                        }
                        placeholder="Column name"
                        className="col-span-2"
                      />
                      {/* Data type */}
                      <select
                        value={col.type}
                        onChange={(e) =>
                          dispatch({
                            type: 'UPDATE_COLUMN',
                            id: col.id,
                            field: 'type',
                            value: e.target.value as DataTypeOption,
                          })
                        }
                        className="px-3 py-2 text-sm border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100"
                      >
                        {DATA_TYPE_OPTIONS.map(opt => (
                          <option key={opt.value} value={opt.value}>
                            {opt.label}
                          </option>
                        ))}
                      </select>
                      {/* Length (for string/decimal) */}
                      <Input
                        type="number"
                        value={col.length || ''}
                        onChange={(e) =>
                          dispatch({
                            type: 'UPDATE_COLUMN',
                            id: col.id,
                            field: 'length',
                            value: e.target.value ? parseInt(e.target.value, 10) : undefined,
                          })
                        }
                        placeholder="Length"
                        min={1}
                        disabled={col.type !== 'STRING' && col.type !== 'DECIMAL'}
                      />
                      {/* Delete button */}
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => dispatch({ type: 'REMOVE_COLUMN', id: col.id })}
                        className="text-red-500 hover:text-red-700"
                        disabled={state.outputColumns.length <= 1}
                      >
                        <Trash2 className="h-4 w-4" />
                      </Button>
                    </div>
                  </div>
                ))}

                <p className="text-xs text-gray-500 mt-2">
                  Drag rows to reorder. At least one output column is required.
                </p>
              </div>
            )}

            {activeTab === 'advanced' && (
              <div className="space-y-6">
                <div className="space-y-2">
                  <Label htmlFor="errorHandling">Error Handling</Label>
                  <select
                    id="errorHandling"
                    value={state.errorHandling}
                    onChange={(e) =>
                      dispatch({ type: 'SET_ERROR_HANDLING', value: e.target.value as any })
                    }
                    className="w-full px-3 py-2 text-sm border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100"
                  >
                    {ERROR_HANDLING_OPTIONS.map(opt => (
                      <option key={opt.value} value={opt.value}>
                        {opt.label}
                      </option>
                    ))}
                  </select>
                </div>

                <div className="flex items-center space-x-2">
                  <input
                    type="checkbox"
                    id="parallel"
                    checked={state.parallelization}
                    onChange={(e) =>
                      dispatch({ type: 'SET_PARALLELIZATION', value: e.target.checked })
                    }
                    className="h-4 w-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                  />
                  <Label htmlFor="parallel" className="cursor-pointer">
                    Enable parallel processing
                  </Label>
                </div>

                <div className="space-y-2">
                  <Label htmlFor="batchSize">Batch Size</Label>
                  <Input
                    id="batchSize"
                    type="number"
                    value={state.batchSize || ''}
                    onChange={(e) =>
                      dispatch({
                        type: 'SET_BATCH_SIZE',
                        value: e.target.value ? parseInt(e.target.value, 10) : undefined,
                      })
                    }
                    min={1}
                    placeholder="1000"
                  />
                </div>

                <Card>
                  <CardContent className="p-4">
                    <h4 className="text-sm font-medium mb-2">SQL Preview (simplified)</h4>
                    <pre className="text-xs bg-gray-100 dark:bg-gray-800 p-3 rounded overflow-auto max-h-32 font-mono">
                      {`-- Extract delimited fields from column "${state.sourceColumn}"\n`}
                      {state.outputColumns.map((col, i) => {
                        const pos = i + 1;
                        return `  split_part(${state.sourceColumn}, '${state.delimiter}', ${pos}) AS ${col.name} -- ${col.type}\n`;
                      }).join('')}
                    </pre>
                  </CardContent>
                </Card>
              </div>
            )}
          </div>
        </div>
      </motion.div>
    </div>
  );
};