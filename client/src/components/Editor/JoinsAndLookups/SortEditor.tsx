// src/components/Editor/SortEditor.tsx
import React, { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import {
  ChevronUp,
  ChevronDown,
  Trash2,
  Plus,
  AlertCircle,
  X,
  Save,
  GripVertical,
  Hash,
  Type,
  Calendar,
  CheckCircle
} from 'lucide-react';

import {
  SortComponentConfiguration,
  UnifiedCanvasNode} from '../../../types/unified-pipeline.types';

// ----------------------------------------------------------------------
// Helper: get data type icon
const getDataTypeIcon = (type: string) => {
  const t = type.toLowerCase();
  if (t.includes('int') || t.includes('num') || t.includes('float') || t.includes('decimal'))
    return <Hash className="h-3 w-3 text-blue-500 mr-1" />;
  if (t.includes('char') || t.includes('text') || t.includes('string') || t.includes('varchar'))
    return <Type className="h-3 w-3 text-green-500 mr-1" />;
  if (t.includes('date') || t.includes('time'))
    return <Calendar className="h-3 w-3 text-purple-500 mr-1" />;
  if (t.includes('bool'))
    return <CheckCircle className="h-3 w-3 text-amber-500 mr-1" />;
  return <Type className="h-3 w-3 text-gray-500 mr-1" />;
};

// ----------------------------------------------------------------------
export interface SortEditorProps {
  nodeId: string;
  nodeMetadata: UnifiedCanvasNode;
  inputColumns: Array<{ name: string; type: string; id?: string }>;
  initialConfig?: SortComponentConfiguration;
  onClose: () => void;
  onSave: (config: SortComponentConfiguration) => void;
}

export const SortEditor: React.FC<SortEditorProps> = ({
  nodeMetadata,
  inputColumns,
  initialConfig,
  onClose,
  onSave
}) => {
  // --------------------------------------------------------------------
  // State
  const [sortFields, setSortFields] = useState<
    Array<{
      field: string;
      direction: 'ASC' | 'DESC';
      nullsFirst: boolean;
      position: number;
    }>
  >([]);
  const [limit, setLimit] = useState<number | undefined>(undefined);
  const [offset, setOffset] = useState<number | undefined>(undefined);
  const [availableColumns, setAvailableColumns] = useState<
    Array<{ name: string; type: string; id?: string }>
  >([]);
  const [selectedColumn, setSelectedColumn] = useState<string>('');

  // Performance estimates (can be calculated or shown as static)
  const [estimatedRowCount] = useState<number>(1000);
  const [memoryRequired] = useState<number | undefined>(undefined);

  // Validation warnings
  const [warnings, setWarnings] = useState<string[]>([]);

  // --------------------------------------------------------------------
  // Effects
  useEffect(() => {
    // Initialize from existing configuration
    if (initialConfig) {
      setSortFields(initialConfig.sortFields.map((f, idx) => ({ ...f, position: idx })));
      setLimit(initialConfig.sqlGeneration.limitOffset?.limit);
      setOffset(initialConfig.sqlGeneration.limitOffset?.offset);
    }

    // Build available columns from input columns
    setAvailableColumns(inputColumns);
  }, [initialConfig, inputColumns]);

  // Validate configuration on changes
  useEffect(() => {
    const newWarnings: string[] = [];
    if (sortFields.length === 0) {
      newWarnings.push('No sort fields defined – the output order will be unspecified.');
    }
    if (limit !== undefined && limit < 0) {
      newWarnings.push('Limit must be a non‑negative number.');
    }
    if (offset !== undefined && offset < 0) {
      newWarnings.push('Offset must be a non‑negative number.');
    }
    setWarnings(newWarnings);
  }, [sortFields, limit, offset]);

  // --------------------------------------------------------------------
  // Handlers
  const handleAddField = () => {
    if (!selectedColumn) return;
    // Avoid duplicates
    if (sortFields.some(f => f.field === selectedColumn)) {
      setWarnings(prev => [...prev, `Column "${selectedColumn}" is already in the sort list.`]);
      return;
    }
    const newField = {
      field: selectedColumn,
      direction: 'ASC' as const,
      nullsFirst: false,
      position: sortFields.length
    };
    setSortFields([...sortFields, newField]);
    setSelectedColumn('');
  };

  const handleRemoveField = (index: number) => {
    const updated = sortFields.filter((_, i) => i !== index);
    // Re‑assign positions
    setSortFields(updated.map((f, idx) => ({ ...f, position: idx })));
  };

  const handleMoveUp = (index: number) => {
    if (index === 0) return;
    const updated = [...sortFields];
    [updated[index - 1], updated[index]] = [updated[index], updated[index - 1]];
    setSortFields(updated.map((f, idx) => ({ ...f, position: idx })));
  };

  const handleMoveDown = (index: number) => {
    if (index === sortFields.length - 1) return;
    const updated = [...sortFields];
    [updated[index], updated[index + 1]] = [updated[index + 1], updated[index]];
    setSortFields(updated.map((f, idx) => ({ ...f, position: idx })));
  };

  const handleDirectionChange = (index: number, direction: 'ASC' | 'DESC') => {
    const updated = [...sortFields];
    updated[index].direction = direction;
    setSortFields(updated);
  };

  const handleNullsFirstChange = (index: number, nullsFirst: boolean) => {
    const updated = [...sortFields];
    updated[index].nullsFirst = nullsFirst;
    setSortFields(updated);
  };

  const handleSave = () => {
    // Build final configuration
    const config: SortComponentConfiguration = {
      version: '1.0',
      sortFields: sortFields.map((f, idx) => ({
        field: f.field,
        direction: f.direction,
        nullsFirst: f.nullsFirst,
        position: idx
      })),
      performance: {
        estimatedRowCount,
        memoryRequired,
        canParallelize: true // default
      },
      sqlGeneration: {
        orderByClause: sortFields
          .map(f => `${f.field} ${f.direction}${f.nullsFirst ? ' NULLS FIRST' : ' NULLS LAST'}`)
          .join(', '),
        requiresDistinct: false,
        limitOffset: (limit !== undefined || offset !== undefined) ? {
          limit: limit ?? 0,
          offset: offset ?? 0
        } : undefined
      },
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        sortComplexity: sortFields.length <= 2 ? 'SIMPLE' : sortFields.length <= 5 ? 'MEDIUM' : 'COMPLEX',
        warnings: warnings.length ? warnings : undefined
      }
    };
    onSave(config);
  };

  // --------------------------------------------------------------------
  // Render
  return (
    <div className="fixed inset-0 z-[9999] bg-black bg-opacity-70 flex items-center justify-center p-4">
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        className="bg-white rounded-xl shadow-2xl w-full max-w-4xl max-h-[90vh] overflow-hidden flex flex-col"
      >
        {/* Header */}
        <div className="flex justify-between items-center p-6 border-b bg-gradient-to-r from-blue-50 to-gray-50">
          <div>
            <h2 className="text-xl font-bold flex items-center">
              <span className="mr-2">↕️</span>
              Sort Row Configuration
            </h2>
            <p className="text-sm text-gray-600 mt-1">
              Node: <span className="font-semibold text-blue-600">{nodeMetadata.name}</span>
              <span className="ml-3 text-xs bg-gray-100 text-gray-800 px-2 py-0.5 rounded">
                {inputColumns.length} available columns
              </span>
            </p>
          </div>
          <button
            onClick={onClose}
            className="p-2 hover:bg-gray-200 rounded-full transition-colors"
            title="Close"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        {/* Main Content */}
        <div className="flex-1 overflow-y-auto p-6 space-y-6">
          {/* Warning banner */}
          {warnings.length > 0 && (
            <div className="bg-yellow-50 border border-yellow-300 text-yellow-800 px-4 py-3 rounded-lg flex items-start">
              <AlertCircle className="h-5 w-5 mr-2 flex-shrink-0 mt-0.5" />
              <div className="text-sm">
                <strong>Configuration warnings:</strong>
                <ul className="list-disc list-inside mt-1">
                  {warnings.map((w, i) => <li key={i}>{w}</li>)}
                </ul>
              </div>
            </div>
          )}

          {/* Add sort field */}
          <div className="bg-gray-50 p-4 rounded-lg border border-gray-200">
            <h3 className="text-sm font-semibold text-gray-700 mb-3 flex items-center">
              <Plus className="h-4 w-4 mr-1" /> Add Sort Field
            </h3>
            <div className="flex items-end space-x-3">
              <div className="flex-1">
                <label className="block text-xs text-gray-500 mb-1">Select column</label>
                <select
                  className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-300"
                  value={selectedColumn}
                  onChange={(e) => setSelectedColumn(e.target.value)}
                >
                  <option value="">-- choose a column --</option>
                  {availableColumns.map(col => (
                    <option key={col.name} value={col.name}>
                      {col.name} ({col.type})
                    </option>
                  ))}
                </select>
              </div>
              <button
                onClick={handleAddField}
                disabled={!selectedColumn}
                className="bg-blue-600 hover:bg-blue-700 disabled:bg-gray-400 text-white px-4 py-2 rounded-md text-sm font-medium flex items-center"
              >
                <Plus className="h-4 w-4 mr-1" /> Add
              </button>
            </div>
          </div>

          {/* Sort fields list */}
          <div>
            <h3 className="text-sm font-semibold text-gray-700 mb-3">Sort Order</h3>
            {sortFields.length === 0 ? (
              <div className="text-center text-gray-500 py-8 border-2 border-dashed border-gray-300 rounded-lg">
                No sort fields defined. Add a column above.
              </div>
            ) : (
              <div className="space-y-2">
                {sortFields.map((field, idx) => {
                  const colInfo = availableColumns.find(c => c.name === field.field);
                  return (
                    <div
                      key={idx}
                      className="flex items-center bg-white border border-gray-200 rounded-lg p-3 shadow-sm hover:shadow transition-shadow"
                    >
                      <GripVertical className="h-5 w-5 text-gray-400 mr-2 cursor-move" />
                      <div className="flex-1 flex items-center">
                        <div className="flex items-center w-40 truncate mr-4">
                          {colInfo && getDataTypeIcon(colInfo.type)}
                          <span className="font-medium text-gray-800">{field.field}</span>
                          {colInfo && (
                            <span className="ml-2 text-xs text-gray-500">{colInfo.type}</span>
                          )}
                        </div>
                        <div className="flex items-center space-x-4 mr-4">
                          <select
                            value={field.direction}
                            onChange={(e) => handleDirectionChange(idx, e.target.value as 'ASC' | 'DESC')}
                            className="border border-gray-300 rounded px-2 py-1 text-xs focus:outline-none focus:ring-1 focus:ring-blue-300"
                          >
                            <option value="ASC">ASC</option>
                            <option value="DESC">DESC</option>
                          </select>
                          <label className="flex items-center text-xs space-x-1">
                            <input
                              type="checkbox"
                              checked={field.nullsFirst}
                              onChange={(e) => handleNullsFirstChange(idx, e.target.checked)}
                              className="rounded"
                            />
                            <span>Nulls first</span>
                          </label>
                        </div>
                      </div>
                      <div className="flex items-center space-x-1">
                        <button
                          onClick={() => handleMoveUp(idx)}
                          disabled={idx === 0}
                          className="p-1 hover:bg-gray-100 rounded disabled:opacity-30"
                          title="Move up"
                        >
                          <ChevronUp className="h-4 w-4" />
                        </button>
                        <button
                          onClick={() => handleMoveDown(idx)}
                          disabled={idx === sortFields.length - 1}
                          className="p-1 hover:bg-gray-100 rounded disabled:opacity-30"
                          title="Move down"
                        >
                          <ChevronDown className="h-4 w-4" />
                        </button>
                        <button
                          onClick={() => handleRemoveField(idx)}
                          className="p-1 hover:bg-red-100 text-red-600 rounded"
                          title="Remove"
                        >
                          <Trash2 className="h-4 w-4" />
                        </button>
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </div>

          {/* Limit / Offset */}
          <div className="grid grid-cols-2 gap-4 bg-gray-50 p-4 rounded-lg border border-gray-200">
            <div>
              <label className="block text-xs text-gray-500 mb-1">Limit (rows)</label>
              <input
                type="number"
                min="0"
                value={limit ?? ''}
                onChange={(e) => setLimit(e.target.value ? parseInt(e.target.value, 10) : undefined)}
                className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-300"
                placeholder="No limit"
              />
            </div>
            <div>
              <label className="block text-xs text-gray-500 mb-1">Offset (rows)</label>
              <input
                type="number"
                min="0"
                value={offset ?? ''}
                onChange={(e) => setOffset(e.target.value ? parseInt(e.target.value, 10) : undefined)}
                className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-300"
                placeholder="No offset"
              />
            </div>
          </div>

          {/* Performance info */}
          <div className="bg-blue-50 p-4 rounded-lg border border-blue-200">
            <h4 className="text-xs font-semibold text-blue-800 uppercase tracking-wider mb-2">
              Performance Estimate
            </h4>
            <div className="grid grid-cols-2 gap-2 text-sm">
              <div>
                <span className="text-gray-600">Estimated rows:</span>
                <span className="ml-2 font-mono text-blue-800">{estimatedRowCount.toLocaleString()}</span>
              </div>
              <div>
                <span className="text-gray-600">Memory required:</span>
                <span className="ml-2 font-mono text-blue-800">
                  {memoryRequired ? `${memoryRequired} MB` : 'unknown'}
                </span>
              </div>
              <div>
                <span className="text-gray-600">Parallelizable:</span>
                <span className="ml-2 font-mono text-blue-800">Yes</span>
              </div>
              <div>
                <span className="text-gray-600">Sort complexity:</span>
                <span className="ml-2 font-mono text-blue-800">
                  {sortFields.length <= 2 ? 'SIMPLE' : sortFields.length <= 5 ? 'MEDIUM' : 'COMPLEX'}
                </span>
              </div>
            </div>
          </div>
        </div>

        {/* Footer */}
        <div className="flex justify-end items-center space-x-3 p-6 border-t bg-gray-50">
          <button
            onClick={onClose}
            className="px-4 py-2 border border-gray-300 rounded-md text-sm hover:bg-gray-100 transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={handleSave}
            className="px-6 py-2 bg-gradient-to-r from-green-500 to-green-600 text-white rounded-md text-sm font-medium flex items-center hover:from-green-600 hover:to-green-700 transition-all shadow-sm"
          >
            <Save className="h-4 w-4 mr-2" />
            Save Configuration
          </button>
        </div>
      </motion.div>
    </div>
  );
};