import React, { useState, useEffect } from 'react';
import { X, Check, ChevronDown, ChevronUp, Plus, Trash2 } from 'lucide-react';
import { UniqRowComponentConfiguration } from '@/types/unified-pipeline.types';

export interface SimpleColumn {
  name: string;
  type?: string;
  id?: string;
}

interface UniqRowEditorProps {
  nodeId: string;
  nodeMetadata: any;               // unified canvas node data
  inputColumns: SimpleColumn[];
  initialConfig?: UniqRowComponentConfiguration;
  onClose: () => void;
  onSave: (config: UniqRowComponentConfiguration) => void;
}

const UniqRowEditor: React.FC<UniqRowEditorProps> = ({
  inputColumns,
  initialConfig,
  onClose,
  onSave
}) => {
  const [keyFields, setKeyFields] = useState<string[]>([]);
  const [keepStrategy, setKeepStrategy] = useState<'FIRST' | 'LAST'>('FIRST');
  const [treatNullsAsEqual, setTreatNullsAsEqual] = useState<boolean>(true);
  const [sortFields, setSortFields] = useState<Array<{ field: string; direction: 'ASC' | 'DESC' }>>([]);
  const [outputDuplicateCount, setOutputDuplicateCount] = useState<boolean>(false);
  const [duplicateCountColumnName, setDuplicateCountColumnName] = useState<string>('duplicate_count');
  const [showAdvanced, setShowAdvanced] = useState<boolean>(false);
  const [validationError, setValidationError] = useState<string | null>(null);

  // Load existing configuration
  useEffect(() => {
    if (initialConfig) {
      setKeyFields(initialConfig.keyFields || []);
      setKeepStrategy(initialConfig.keepStrategy || 'FIRST');
      setTreatNullsAsEqual(initialConfig.treatNullsAsEqual !== false);
      setSortFields(initialConfig.sortFields || []);
      setOutputDuplicateCount(initialConfig.outputDuplicateCount || false);
      setDuplicateCountColumnName(initialConfig.duplicateCountColumnName || 'duplicate_count');
    }
  }, [initialConfig]);

  const handleSave = () => {
    if (keyFields.length === 0) {
      setValidationError('At least one key field must be selected.');
      return;
    }

    const config: UniqRowComponentConfiguration = {
      version: '1.0',
      keyFields,
      keepStrategy,
      treatNullsAsEqual,
      sortFields: sortFields.length > 0 ? sortFields : undefined,
      outputDuplicateCount: outputDuplicateCount || undefined,
      duplicateCountColumnName: outputDuplicateCount ? duplicateCountColumnName : undefined,
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        createdBy: 'canvas',
        validationStatus: 'VALID',
        warnings: [],
        dependencies: keyFields,
      }
    };
    onSave(config);
  };

  const addSortField = () => {
    if (inputColumns.length === 0) return;
    const newField = { field: inputColumns[0].name, direction: 'ASC' as const };
    setSortFields([...sortFields, newField]);
  };

  const removeSortField = (index: number) => {
    setSortFields(sortFields.filter((_, i) => i !== index));
  };

  const updateSortField = (index: number, field: string, direction: 'ASC' | 'DESC') => {
    const updated = [...sortFields];
    updated[index] = { field, direction };
    setSortFields(updated);
  };

  return (
    <div className="fixed inset-0 z-[10000] bg-black bg-opacity-60 flex items-center justify-center p-4">
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-3xl max-h-[90vh] flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b bg-gradient-to-r from-indigo-50 to-white">
          <div>
            <h2 className="text-xl font-bold text-gray-800 flex items-center">
              <span className="mr-2">🔍</span>
              tUniqRow Configuration
            </h2>
            <p className="text-sm text-gray-600 mt-1">
              Remove duplicate rows based on selected fields
            </p>
          </div>
          <button
            onClick={onClose}
            className="p-2 hover:bg-gray-100 rounded-full transition-colors"
            title="Close"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto p-6 space-y-6">
          {/* Key Fields Selection */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Fields to consider for uniqueness <span className="text-red-500">*</span>
            </label>
            <div className="border border-gray-300 rounded-lg p-3 max-h-48 overflow-y-auto">
              {inputColumns.length === 0 ? (
                <p className="text-gray-500 italic">No input columns available</p>
              ) : (
                <div className="grid grid-cols-2 gap-2">
                  {inputColumns.map(col => (
                    <label key={col.name} className="flex items-center space-x-2 text-sm">
                      <input
                        type="checkbox"
                        checked={keyFields.includes(col.name)}
                        onChange={(e) => {
                          if (e.target.checked) {
                            setKeyFields([...keyFields, col.name]);
                          } else {
                            setKeyFields(keyFields.filter(f => f !== col.name));
                          }
                        }}
                        className="rounded border-gray-300 text-indigo-600 focus:ring-indigo-500"
                      />
                      <span className="truncate">{col.name}</span>
                      <span className="text-xs text-gray-400">{col.type}</span>
                    </label>
                  ))}
                </div>
              )}
            </div>
            {validationError && <p className="text-sm text-red-600 mt-1">{validationError}</p>}
          </div>

          {/* Keep Strategy */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Keep which occurrence?
            </label>
            <div className="flex space-x-4">
              <label className="flex items-center space-x-2">
                <input
                  type="radio"
                  name="keepStrategy"
                  value="FIRST"
                  checked={keepStrategy === 'FIRST'}
                  onChange={() => setKeepStrategy('FIRST')}
                  className="text-indigo-600 focus:ring-indigo-500"
                />
                <span>First occurrence</span>
              </label>
              <label className="flex items-center space-x-2">
                <input
                  type="radio"
                  name="keepStrategy"
                  value="LAST"
                  checked={keepStrategy === 'LAST'}
                  onChange={() => setKeepStrategy('LAST')}
                  className="text-indigo-600 focus:ring-indigo-500"
                />
                <span>Last occurrence</span>
              </label>
            </div>
          </div>

          {/* Treat Nulls as Equal */}
          <div>
            <label className="flex items-center space-x-2">
              <input
                type="checkbox"
                checked={treatNullsAsEqual}
                onChange={(e) => setTreatNullsAsEqual(e.target.checked)}
                className="rounded border-gray-300 text-indigo-600 focus:ring-indigo-500"
              />
              <span className="text-sm font-medium text-gray-700">Treat null values as equal</span>
            </label>
            <p className="text-xs text-gray-500 ml-6 mt-1">
              If unchecked, rows with null in key fields are considered distinct from each other.
            </p>
          </div>

          {/* Advanced Options Toggle */}
          <div>
            <button
              onClick={() => setShowAdvanced(!showAdvanced)}
              className="flex items-center text-sm text-indigo-600 hover:text-indigo-800"
            >
              {showAdvanced ? <ChevronUp className="h-4 w-4 mr-1" /> : <ChevronDown className="h-4 w-4 mr-1" />}
              {showAdvanced ? 'Hide Advanced' : 'Show Advanced'}
            </button>
          </div>

          {showAdvanced && (
            <div className="space-y-4 border-t pt-4">
              {/* Sort Fields (to define order for first/last) */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Sort order (to determine first/last)
                </label>
                <p className="text-xs text-gray-500 mb-2">
                  If no sort fields defined, the natural order of the input is used.
                </p>
                <div className="space-y-2">
                  {sortFields.map((sf, index) => (
                    <div key={index} className="flex items-center space-x-2">
                      <select
                        value={sf.field}
                        onChange={(e) => updateSortField(index, e.target.value, sf.direction)}
                        className="flex-1 border border-gray-300 rounded px-2 py-1 text-sm"
                      >
                        {inputColumns.map(col => (
                          <option key={col.name} value={col.name}>{col.name}</option>
                        ))}
                      </select>
                      <select
                        value={sf.direction}
                        onChange={(e) => updateSortField(index, sf.field, e.target.value as 'ASC' | 'DESC')}
                        className="border border-gray-300 rounded px-2 py-1 text-sm w-24"
                      >
                        <option value="ASC">ASC</option>
                        <option value="DESC">DESC</option>
                      </select>
                      <button
                        onClick={() => removeSortField(index)}
                        className="p-1 text-red-500 hover:text-red-700"
                      >
                        <Trash2 className="h-4 w-4" />
                      </button>
                    </div>
                  ))}
                  <button
                    onClick={addSortField}
                    className="flex items-center text-sm text-indigo-600 hover:text-indigo-800"
                  >
                    <Plus className="h-4 w-4 mr-1" />
                    Add sort field
                  </button>
                </div>
              </div>

              {/* Output duplicate count */}
              <div>
                <label className="flex items-center space-x-2">
                  <input
                    type="checkbox"
                    checked={outputDuplicateCount}
                    onChange={(e) => setOutputDuplicateCount(e.target.checked)}
                    className="rounded border-gray-300 text-indigo-600 focus:ring-indigo-500"
                  />
                  <span className="text-sm font-medium text-gray-700">Output duplicate count column</span>
                </label>
                {outputDuplicateCount && (
                  <div className="mt-2 ml-6">
                    <label className="block text-xs text-gray-600 mb-1">Column name:</label>
                    <input
                      type="text"
                      value={duplicateCountColumnName}
                      onChange={(e) => setDuplicateCountColumnName(e.target.value)}
                      className="border border-gray-300 rounded px-2 py-1 text-sm w-64"
                      placeholder="duplicate_count"
                    />
                  </div>
                )}
              </div>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end px-6 py-4 border-t bg-gray-50">
          <button
            onClick={onClose}
            className="px-4 py-2 text-sm border border-gray-300 rounded hover:bg-gray-100 transition-colors mr-2"
          >
            Cancel
          </button>
          <button
            onClick={handleSave}
            className="px-4 py-2 text-sm bg-gradient-to-r from-indigo-500 to-indigo-600 text-white rounded hover:from-indigo-600 hover:to-indigo-700 transition-all flex items-center"
          >
            <Check className="h-4 w-4 mr-1" />
            Save Configuration
          </button>
        </div>
      </div>
    </div>
  );
};

export default UniqRowEditor;