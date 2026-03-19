// src/components/Editor/ExtractJSONFieldsEditor.tsx
import React, { useState, useCallback, useEffect } from 'react';
import { ChevronDown, ChevronRight, Plus, Trash2, AlertCircle } from 'lucide-react';

// Import types
import { DataType } from '../../../types/metadata';
import { ExtractJSONFieldsConfiguration } from '../../../types/unified-pipeline.types';
import { SimpleColumn } from '../Mapping/MapEditor'; // Reuse the SimpleColumn interface

// PostgreSQL data types (same as in MapEditor)
const DATA_TYPE_OPTIONS: Array<{ value: DataType; label: string }> = [
  { value: 'STRING', label: 'STRING' },
  { value: 'INTEGER', label: 'INTEGER' },
  { value: 'DECIMAL', label: 'DECIMAL' },
  { value: 'BOOLEAN', label: 'BOOLEAN' },
  { value: 'DATE', label: 'DATE' },
  { value: 'TIMESTAMP', label: 'TIMESTAMP' },
  { value: 'BINARY', label: 'BINARY' },
];

// Helper to generate unique IDs
const generateId = (prefix = 'json-col') => `${prefix}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

interface ExtractJSONFieldsEditorProps {
  nodeId?: string;
  nodeMetadata?: any;
  inputColumns: SimpleColumn[];          // Available columns from input schema
  initialConfig?: ExtractJSONFieldsConfiguration;
  onClose: () => void;
  onSave: (config: ExtractJSONFieldsConfiguration) => void;
}

export const ExtractJSONFieldsEditor: React.FC<ExtractJSONFieldsEditorProps> = ({
  nodeId,
  inputColumns,
  initialConfig,
  onClose,
  onSave,
}) => {
  // ---------- State ----------
  const [sourceColumn, setSourceColumn] = useState<string>(
    initialConfig?.sourceColumn || (inputColumns[0]?.name || '')
  );
  const [baseJsonPath, setBaseJsonPath] = useState<string>(initialConfig?.jsonPath || '');
  const [outputColumns, setOutputColumns] = useState<ExtractJSONFieldsConfiguration['outputColumns']>(
    initialConfig?.outputColumns || []
  );
  const [errorHandling, setErrorHandling] = useState<ExtractJSONFieldsConfiguration['errorHandling']>(
    initialConfig?.errorHandling || 'fail'
  );
  const [parallelization, setParallelization] = useState<boolean>(
    initialConfig?.parallelization ?? true
  );
  const [batchSize, setBatchSize] = useState<number | undefined>(
    initialConfig?.batchSize || 1000
  );

  // Validation warnings
  const [warnings, setWarnings] = useState<string[]>([]);

  // UI state for expand/collapse (optional)
  const [advancedOpen, setAdvancedOpen] = useState(false);

  // ---------- Validation ----------
  const validate = useCallback(() => {
    const newWarnings: string[] = [];
    if (!sourceColumn) {
      newWarnings.push('Source column is required');
    }
    if (outputColumns.length === 0) {
      newWarnings.push('At least one output column must be defined');
    }
    outputColumns.forEach((col, idx) => {
      if (!col.name.trim()) {
        newWarnings.push(`Output column #${idx + 1} has no name`);
      }
      if (!col.jsonPath.trim()) {
        newWarnings.push(`Output column "${col.name || idx}" has no JSONPath`);
      }
    });
    setWarnings(newWarnings);
    return newWarnings.length === 0;
  }, [sourceColumn, outputColumns]);

  // ---------- Handlers ----------
  const handleAddColumn = useCallback(() => {
    const newCol: ExtractJSONFieldsConfiguration['outputColumns'][0] = {
      id: generateId(),
      name: '',
      jsonPath: '',
      type: 'STRING',
      nullable: true,
    };
    setOutputColumns([...outputColumns, newCol]);
  }, [outputColumns]);

  const handleRemoveColumn = useCallback((id: string) => {
    setOutputColumns(outputColumns.filter(col => col.id !== id));
  }, [outputColumns]);

  const handleColumnChange = useCallback(
    (id: string, field: keyof ExtractJSONFieldsConfiguration['outputColumns'][0], value: any) => {
      setOutputColumns(
        outputColumns.map(col =>
          col.id === id ? { ...col, [field]: value } : col
        )
      );
    },
    [outputColumns]
  );

  const handleSave = useCallback(() => {
    if (!validate()) return;

    const config: ExtractJSONFieldsConfiguration = {
      version: '1.0',
      sourceColumn,
      jsonPath: baseJsonPath || undefined,
      outputColumns,
      errorHandling,
      parallelization,
      batchSize: parallelization ? batchSize : undefined,
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        createdBy: 'extract-json-editor',
        ruleCount: outputColumns.length,
        validationStatus: warnings.length === 0 ? 'VALID' : 'WARNING',
        warnings,
        dependencies: [sourceColumn],
        compiledSql: undefined,
      },
    };
    onSave(config);
  }, [sourceColumn, baseJsonPath, outputColumns, errorHandling, parallelization, batchSize, validate, warnings, onSave]);

  // Validate whenever relevant state changes
  useEffect(() => {
    validate();
  }, [sourceColumn, outputColumns, validate]);

  // ---------- Render ----------
  return (
    <div className="fixed inset-0 z-[9999] bg-gray-900/80 flex flex-col">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b bg-gradient-to-r from-blue-50 to-gray-50 sticky top-0 z-20">
        <div className="flex items-center space-x-3">
          <div className="text-lg font-bold text-blue-700 flex items-center">
            <span className="mr-2">🔍</span>
            Extract JSON Fields
            <span className="ml-2 text-xs bg-green-100 text-green-800 px-2 py-0.5 rounded">
              Compiler Metadata v1.0
            </span>
          </div>
          <div className="text-xs text-gray-600 bg-white px-2 py-1 rounded border">
            Node: {nodeId || 'Unknown'}
          </div>
          <div className="text-xs text-gray-600 bg-white px-2 py-1 rounded border">
            Output columns: {outputColumns.length}
          </div>
        </div>
        <div className="flex items-center gap-2">
          <button
            className="px-4 py-2 text-sm border border-gray-300 rounded hover:bg-gray-50 transition-colors"
            onClick={onClose}
          >
            Cancel
          </button>
          <button
            className="px-4 py-2 text-sm bg-gradient-to-r from-green-500 to-green-600 text-white rounded hover:from-green-600 hover:to-green-700 transition-all shadow-sm hover:shadow"
            onClick={handleSave}
          >
            Save & Compile
          </button>
        </div>
      </div>

      {/* Main content */}
      <div className="flex-1 overflow-auto p-6">
        <div className="max-w-5xl mx-auto space-y-6">
          {/* Source Column Selection */}
          <div className="bg-white rounded-lg border shadow-sm p-4">
            <h3 className="text-sm font-semibold text-gray-800 mb-3 flex items-center">
              <span className="w-2 h-2 bg-blue-500 rounded-full mr-2"></span>
              JSON Source
            </h3>
            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="block text-xs font-medium text-gray-700 mb-1">
                  Source Column <span className="text-red-500">*</span>
                </label>
                <select
                  value={sourceColumn}
                  onChange={(e) => setSourceColumn(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                >
                  {inputColumns.length === 0 && (
                    <option value="" disabled>No input columns available</option>
                  )}
                  {inputColumns.map((col) => (
                    <option key={col.id || col.name} value={col.name}>
                      {col.name} {col.type ? `(${col.type})` : ''}
                    </option>
                  ))}
                </select>
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-700 mb-1">
                  Base JSONPath (optional)
                </label>
                <input
                  type="text"
                  value={baseJsonPath}
                  onChange={(e) => setBaseJsonPath(e.target.value)}
                  placeholder="e.g., $.store.book"
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                />
                <p className="text-xs text-gray-500 mt-1">
                  Prepended to each column's JSONPath
                </p>
              </div>
            </div>
          </div>

          {/* Output Columns Table */}
          <div className="bg-white rounded-lg border shadow-sm">
            <div className="flex items-center justify-between p-4 border-b">
              <h3 className="text-sm font-semibold text-gray-800 flex items-center">
                <span className="w-2 h-2 bg-purple-500 rounded-full mr-2"></span>
                Output Columns
                <span className="ml-2 text-xs bg-gray-100 text-gray-600 px-2 py-0.5 rounded">
                  {outputColumns.length} defined
                </span>
              </h3>
              <button
                onClick={handleAddColumn}
                className="inline-flex items-center px-3 py-1.5 text-xs bg-blue-600 text-white rounded hover:bg-blue-700 transition-colors"
              >
                <Plus className="w-3 h-3 mr-1" />
                Add Column
              </button>
            </div>

            {outputColumns.length === 0 ? (
              <div className="p-8 text-center text-gray-500">
                <p className="text-sm">No output columns defined.</p>
                <p className="text-xs mt-1">Click "Add Column" to start extracting JSON fields.</p>
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="bg-gray-50 border-b">
                    <tr>
                      <th className="px-4 py-2 text-left text-xs font-medium text-gray-500">Name</th>
                      <th className="px-4 py-2 text-left text-xs font-medium text-gray-500">JSONPath</th>
                      <th className="px-4 py-2 text-left text-xs font-medium text-gray-500">Type</th>
                      <th className="px-4 py-2 text-left text-xs font-medium text-gray-500">Nullable</th>
                      <th className="px-4 py-2 text-left text-xs font-medium text-gray-500">Default</th>
                      <th className="px-4 py-2 text-center text-xs font-medium text-gray-500 w-16">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {outputColumns.map((col) => (
                      <tr key={col.id} className="border-b hover:bg-gray-50">
                        <td className="px-4 py-2">
                          <input
                            type="text"
                            value={col.name}
                            onChange={(e) => handleColumnChange(col.id, 'name', e.target.value)}
                            placeholder="column_name"
                            className="w-full px-2 py-1 border border-gray-300 rounded text-sm focus:outline-none focus:ring-1 focus:ring-blue-500"
                          />
                        </td>
                        <td className="px-4 py-2">
                          <input
                            type="text"
                            value={col.jsonPath}
                            onChange={(e) => handleColumnChange(col.id, 'jsonPath', e.target.value)}
                            placeholder="$.field"
                            className="w-full px-2 py-1 border border-gray-300 rounded text-sm font-mono focus:outline-none focus:ring-1 focus:ring-blue-500"
                          />
                        </td>
                        <td className="px-4 py-2">
                          <select
                            value={col.type}
                            onChange={(e) => handleColumnChange(col.id, 'type', e.target.value as DataType)}
                            className="w-full px-2 py-1 border border-gray-300 rounded text-sm focus:outline-none focus:ring-1 focus:ring-blue-500"
                          >
                            {DATA_TYPE_OPTIONS.map(opt => (
                              <option key={opt.value} value={opt.value}>{opt.label}</option>
                            ))}
                          </select>
                        </td>
                        <td className="px-4 py-2 text-center">
                          <input
                            type="checkbox"
                            checked={col.nullable}
                            onChange={(e) => handleColumnChange(col.id, 'nullable', e.target.checked)}
                            className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                          />
                        </td>
                        <td className="px-4 py-2">
                          <input
                            type="text"
                            value={col.defaultValue || ''}
                            onChange={(e) => handleColumnChange(col.id, 'defaultValue', e.target.value)}
                            placeholder="NULL"
                            className="w-full px-2 py-1 border border-gray-300 rounded text-sm focus:outline-none focus:ring-1 focus:ring-blue-500"
                          />
                        </td>
                        <td className="px-4 py-2 text-center">
                          <button
                            onClick={() => handleRemoveColumn(col.id)}
                            className="text-red-500 hover:text-red-700 transition-colors"
                            title="Remove column"
                          >
                            <Trash2 className="w-4 h-4" />
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>

          {/* Advanced Options (collapsible) */}
          <div className="bg-white rounded-lg border shadow-sm">
            <button
              onClick={() => setAdvancedOpen(!advancedOpen)}
              className="w-full flex items-center justify-between p-4 text-left"
            >
              <h3 className="text-sm font-semibold text-gray-800 flex items-center">
                <span className="w-2 h-2 bg-gray-500 rounded-full mr-2"></span>
                Advanced Options
              </h3>
              {advancedOpen ? (
                <ChevronDown className="w-4 h-4 text-gray-500" />
              ) : (
                <ChevronRight className="w-4 h-4 text-gray-500" />
              )}
            </button>
            {advancedOpen && (
              <div className="p-4 border-t space-y-4">
                <div>
                  <label className="block text-xs font-medium text-gray-700 mb-1">
                    Error Handling
                  </label>
                  <select
                    value={errorHandling}
                    onChange={(e) => setErrorHandling(e.target.value as any)}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  >
                    <option value="fail">Fail job on error</option>
                    <option value="skip">Skip row on error</option>
                    <option value="setNull">Set NULL and continue</option>
                  </select>
                </div>
                <div className="flex items-center space-x-4">
                  <label className="flex items-center space-x-2 text-sm">
                    <input
                      type="checkbox"
                      checked={parallelization}
                      onChange={(e) => setParallelization(e.target.checked)}
                      className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                    />
                    <span className="text-sm text-gray-700">Enable parallelization</span>
                  </label>
                  {parallelization && (
                    <div className="flex items-center space-x-2">
                      <label className="text-sm text-gray-700">Batch size:</label>
                      <input
                        type="number"
                        value={batchSize}
                        onChange={(e) => setBatchSize(parseInt(e.target.value) || 1000)}
                        min="1"
                        max="100000"
                        className="w-24 px-2 py-1 border border-gray-300 rounded text-sm focus:outline-none focus:ring-1 focus:ring-blue-500"
                      />
                    </div>
                  )}
                </div>
              </div>
            )}
          </div>

          {/* Validation Warnings */}
          {warnings.length > 0 && (
            <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
              <div className="flex items-start">
                <AlertCircle className="w-5 h-5 text-yellow-600 mr-2 flex-shrink-0 mt-0.5" />
                <div>
                  <h4 className="text-sm font-medium text-yellow-800">Validation warnings</h4>
                  <ul className="mt-1 text-xs text-yellow-700 list-disc list-inside">
                    {warnings.map((w, i) => (
                      <li key={i}>{w}</li>
                    ))}
                  </ul>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Status bar */}
      <div className="flex items-center justify-between px-4 py-2 border-t bg-gray-50 text-xs text-gray-600">
        <div className="flex items-center space-x-4">
          <span>Status: {warnings.length === 0 ? 'Ready' : 'Warnings'}</span>
          <span>Output columns: {outputColumns.length}</span>
          {sourceColumn && <span>Source: {sourceColumn}</span>}
        </div>
        <div className="text-gray-400">
          Shortcut: Ctrl+S to save
        </div>
      </div>
    </div>
  );
};