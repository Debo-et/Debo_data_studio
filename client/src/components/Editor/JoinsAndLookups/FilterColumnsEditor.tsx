import React, { useState, useEffect, useCallback, useMemo } from 'react';
import { ChevronUp, ChevronDown, AlertCircle } from 'lucide-react';
import {
  FilterColumnsComponentConfiguration,
  FilterColumn,
  SchemaDefinition,
  FieldSchema,
  DataType,
} from '../../../types/unified-pipeline.types';

// Helper to generate unique IDs
const generateId = (prefix = 'col') => `${prefix}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

interface FilterColumnsEditorProps {
  isOpen: boolean;
  onClose: () => void;
  onSave: (config: FilterColumnsComponentConfiguration) => void;
  nodeId?: string;
  initialConfig?: Partial<FilterColumnsComponentConfiguration>;
  inputSchema?: SchemaDefinition; // from the node's incoming connection
}

const FilterColumnsEditor: React.FC<FilterColumnsEditorProps> = ({
  isOpen,
  onClose,
  onSave,
  nodeId,
  initialConfig,
  inputSchema,
}) => {
  // State for columns list
  const [columns, setColumns] = useState<FilterColumn[]>([]);
  const [activeTab, setActiveTab] = useState<'basic' | 'advanced'>('basic');
  const [options, setOptions] = useState({
    caseSensitive: false,
    keepAllByDefault: true,
    errorOnMissingColumn: true,
  });

  // Statistics
  const selectedCount = useMemo(() => columns.filter(c => c.selected).length, [columns]);
  const totalCount = columns.length;
  const selectionPercentage = totalCount > 0 ? Math.round((selectedCount / totalCount) * 100) : 0;

  // Initialize from input schema or initial config
  useEffect(() => {
    if (inputSchema?.fields && inputSchema.fields.length > 0) {
      const initialColumns: FilterColumn[] = inputSchema.fields.map((field, idx) => ({
        id: generateId(),
        originalName: field.name,
        newName: field.name, // default to original name
        selected: options.keepAllByDefault,
        position: idx,
      }));
      setColumns(initialColumns);
    } else if (initialConfig?.columns) {
      // Restore from saved config
      setColumns(initialConfig.columns.map(col => ({ ...col, id: col.id || generateId() })));
      if (initialConfig.options) setOptions(initialConfig.options);
    }
  }, [inputSchema, initialConfig, options.keepAllByDefault]);

  // Handlers
  const toggleColumn = useCallback((id: string) => {
    setColumns(prev => prev.map(col =>
      col.id === id ? { ...col, selected: !col.selected } : col
    ));
  }, []);

  const updateNewName = useCallback((id: string, newName: string) => {
    setColumns(prev => prev.map(col =>
      col.id === id ? { ...col, newName } : col
    ));
  }, []);

  const moveColumn = useCallback((id: string, direction: 'up' | 'down') => {
    setColumns(prev => {
      const index = prev.findIndex(c => c.id === id);
      if (index === -1) return prev;
      const newIndex = direction === 'up' ? index - 1 : index + 1;
      if (newIndex < 0 || newIndex >= prev.length) return prev;

      const newColumns = [...prev];
      // Swap positions
      [newColumns[index], newColumns[newIndex]] = [newColumns[newIndex], newColumns[index]];
      // Update position numbers
      return newColumns.map((col, idx) => ({ ...col, position: idx }));
    });
  }, []);

  const selectAll = useCallback(() => {
    setColumns(prev => prev.map(col => ({ ...col, selected: true })));
  }, []);

  const selectNone = useCallback(() => {
    setColumns(prev => prev.map(col => ({ ...col, selected: false })));
  }, []);

  // Generate output schema from selected columns
  const generateOutputSchema = useCallback((): SchemaDefinition => {
    const selected = columns.filter(c => c.selected).sort((a, b) => a.position - b.position);
    const fields: FieldSchema[] = selected.map(col => ({
      id: col.id,
      name: col.newName || col.originalName,
      type: (inputSchema?.fields.find(f => f.name === col.originalName)?.type as DataType) || 'STRING',
      nullable: true, // could be derived from input
      isKey: false,
      description: `From original column ${col.originalName}`,
      metadata: { originalName: col.originalName },
    }));
    return {
      id: `output-${nodeId || 'unknown'}`,
      name: 'Filtered Output',
      alias: '',
      fields,
      isTemporary: true,
      isMaterialized: false,
      metadata: {},
    };
  }, [columns, inputSchema, nodeId]);

  // Validate configuration
  const validate = useCallback((): { isValid: boolean; warnings: string[] } => {
    const warnings: string[] = [];
    if (selectedCount === 0) {
      warnings.push('No columns selected – output will be empty.');
    }
    // Check for duplicate new names
    const newNames = columns.filter(c => c.selected).map(c => c.newName || c.originalName);
    const duplicates = newNames.filter((name, idx) => newNames.indexOf(name) !== idx);
    if (duplicates.length > 0) {
      warnings.push(`Duplicate output column names: ${[...new Set(duplicates)].join(', ')}`);
    }
    return {
      isValid: warnings.length === 0,
      warnings,
    };
  }, [columns, selectedCount]);

  // Build final configuration
  const buildConfig = useCallback((): FilterColumnsComponentConfiguration => {
    const validation = validate();
    const outputSchema = generateOutputSchema();
    // Generate SQL SELECT clause (simplified)
    const selectItems = columns
      .filter(c => c.selected)
      .sort((a, b) => a.position - b.position)
      .map(c => `${c.originalName} AS "${c.newName || c.originalName}"`)
      .join(', ');
    const selectClause = selectItems || '*'; // fallback

    return {
      version: '1.0',
      columns: columns.map(col => ({ ...col })), // copy
      options,
      outputSchema,
      sqlGeneration: {
        selectClause,
        estimatedRowMultiplier: 1.0,
      },
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        createdBy: 'filter-columns-editor',
        validationStatus: validation.isValid ? 'VALID' : 'WARNING',
        warnings: validation.warnings,
        dependencies: columns.filter(c => c.selected).map(c => c.originalName),
      },
    };
  }, [columns, options, generateOutputSchema, validate]);

  const handleSave = useCallback(() => {
    const config = buildConfig();
    onSave(config);
  }, [buildConfig, onSave]);

  // Keyboard shortcuts
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
      if (e.key === 's' && e.ctrlKey) {
        e.preventDefault();
        handleSave();
      }
    };
    if (isOpen) {
      document.addEventListener('keydown', handleKeyDown);
      return () => document.removeEventListener('keydown', handleKeyDown);
    }
  }, [isOpen, onClose, handleSave]);

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-[9999] bg-gray-900/80 flex flex-col">
      {/* Toolbar */}
      <div className="flex items-center justify-between px-4 py-3 border-b bg-gradient-to-r from-indigo-50 to-gray-50">
        <div className="flex items-center space-x-3">
          <span className="text-lg font-bold text-indigo-700 flex items-center">
            <span className="mr-2">🔽</span>
            tFilterColumns Editor
          </span>
          <div className="text-xs text-gray-600 bg-white px-2 py-1 rounded border">
            Node: {nodeId || 'Unknown'}
          </div>
          <div className="text-xs bg-blue-100 text-blue-800 px-2 py-1 rounded border border-blue-200">
            {selectedCount} / {totalCount} columns selected ({selectionPercentage}%)
          </div>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={onClose}
            className="px-3 py-1.5 text-sm border border-gray-300 rounded hover:bg-gray-50"
          >
            Cancel
          </button>
          <button
            onClick={handleSave}
            className="px-3 py-1.5 text-sm bg-gradient-to-r from-green-500 to-green-600 text-white rounded hover:from-green-600 hover:to-green-700"
          >
            Save
          </button>
        </div>
      </div>

      {/* Tabs */}
      <div className="flex border-b bg-white px-4">
        <button
          onClick={() => setActiveTab('basic')}
          className={`px-4 py-2 text-sm font-medium ${
            activeTab === 'basic'
              ? 'text-indigo-600 border-b-2 border-indigo-600'
              : 'text-gray-500 hover:text-gray-700'
          }`}
        >
          Basic
        </button>
        <button
          onClick={() => setActiveTab('advanced')}
          className={`px-4 py-2 text-sm font-medium ${
            activeTab === 'advanced'
              ? 'text-indigo-600 border-b-2 border-indigo-600'
              : 'text-gray-500 hover:text-gray-700'
          }`}
        >
          Advanced
        </button>
      </div>

      {/* Main Content */}
      <div className="flex-1 overflow-auto p-4 bg-gray-50">
        {activeTab === 'basic' ? (
          <div className="bg-white rounded-lg shadow border">
            <div className="px-4 py-3 border-b bg-gray-50 flex items-center justify-between">
              <h3 className="font-medium text-gray-700">Select and rename columns</h3>
              <div className="space-x-2">
                <button
                  onClick={selectAll}
                  className="text-xs px-2 py-1 bg-indigo-100 text-indigo-700 rounded hover:bg-indigo-200"
                >
                  Select All
                </button>
                <button
                  onClick={selectNone}
                  className="text-xs px-2 py-1 bg-gray-100 text-gray-700 rounded hover:bg-gray-200"
                >
                  Select None
                </button>
              </div>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead className="bg-gray-100 text-gray-600">
                  <tr>
                    <th className="px-4 py-2 text-left w-12">#</th>
                    <th className="px-4 py-2 text-left">Include</th>
                    <th className="px-4 py-2 text-left">Original Name</th>
                    <th className="px-4 py-2 text-left">Output Name</th>
                    <th className="px-4 py-2 text-left w-24">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {columns.map((col, index) => (
                    <tr key={col.id} className="border-t hover:bg-gray-50">
                      <td className="px-4 py-2 text-gray-500">{index + 1}</td>
                      <td className="px-4 py-2">
                        <input
                          type="checkbox"
                          checked={col.selected}
                          onChange={() => toggleColumn(col.id)}
                          className="rounded border-gray-300 text-indigo-600 focus:ring-indigo-500"
                        />
                      </td>
                      <td className="px-4 py-2 font-mono text-gray-700">{col.originalName}</td>
                      <td className="px-4 py-2">
                        <input
                          type="text"
                          value={col.newName || ''}
                          onChange={(e) => updateNewName(col.id, e.target.value)}
                          placeholder={col.originalName}
                          className="w-full px-2 py-1 border border-gray-300 rounded text-sm focus:outline-none focus:ring-1 focus:ring-indigo-500"
                          disabled={!col.selected}
                        />
                      </td>
                      <td className="px-4 py-2">
                        <div className="flex items-center space-x-1">
                          <button
                            onClick={() => moveColumn(col.id, 'up')}
                            disabled={index === 0}
                            className="p-1 text-gray-500 hover:text-gray-700 disabled:opacity-30"
                            title="Move up"
                          >
                            <ChevronUp className="w-4 h-4" />
                          </button>
                          <button
                            onClick={() => moveColumn(col.id, 'down')}
                            disabled={index === columns.length - 1}
                            className="p-1 text-gray-500 hover:text-gray-700 disabled:opacity-30"
                            title="Move down"
                          >
                            <ChevronDown className="w-4 h-4" />
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        ) : (
          <div className="bg-white rounded-lg shadow p-6 space-y-6">
            <h3 className="font-medium text-gray-700 mb-4">Advanced Options</h3>
            <div className="space-y-4">
              <label className="flex items-center space-x-2">
                <input
                  type="checkbox"
                  checked={options.caseSensitive}
                  onChange={(e) => setOptions({ ...options, caseSensitive: e.target.checked })}
                  className="rounded border-gray-300 text-indigo-600"
                />
                <span className="text-sm text-gray-700">Case‑sensitive column matching</span>
              </label>
              <label className="flex items-center space-x-2">
                <input
                  type="checkbox"
                  checked={options.keepAllByDefault}
                  onChange={(e) => setOptions({ ...options, keepAllByDefault: e.target.checked })}
                  className="rounded border-gray-300 text-indigo-600"
                />
                <span className="text-sm text-gray-700">Select all columns by default</span>
              </label>
              <label className="flex items-center space-x-2">
                <input
                  type="checkbox"
                  checked={options.errorOnMissingColumn}
                  onChange={(e) => setOptions({ ...options, errorOnMissingColumn: e.target.checked })}
                  className="rounded border-gray-300 text-indigo-600"
                />
                <span className="text-sm text-gray-700">Error if a selected column is missing</span>
              </label>
            </div>
          </div>
        )}
      </div>

      {/* Status Bar */}
      <div className="flex items-center justify-between px-4 py-2 border-t bg-gray-50 text-xs text-gray-600">
        <div className="flex items-center space-x-3">
          <span className="flex items-center">
            <span className="mr-1">✔️</span>
            {selectedCount} columns selected
          </span>
          {validate().warnings.length > 0 && (
            <span className="flex items-center text-yellow-600">
              <AlertCircle className="w-3 h-3 mr-1" />
              {validate().warnings[0]}
            </span>
          )}
        </div>
        <div className="flex items-center space-x-3">
          <kbd className="px-1.5 py-0.5 bg-white border rounded">Esc</kbd>
          <span>Cancel</span>
          <kbd className="px-1.5 py-0.5 bg-white border rounded">Ctrl+S</kbd>
          <span>Save</span>
        </div>
      </div>
    </div>
  );
};

export default FilterColumnsEditor;