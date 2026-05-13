import React, { useState, useCallback, useMemo } from 'react';
import { motion } from 'framer-motion';
import { DataType } from '../../../types/metadata';
import { UnpivotRowComponentConfiguration } from '../../../types/unified-pipeline.types';

// Reuse SimpleColumn from MapEditor or define locally
interface SimpleColumn {
  name: string;
  type?: string;
  id?: string;
}

interface UnpivotRowEditorProps {
  nodeId: string;
  nodeMetadata: any;
  inputColumns: SimpleColumn[];
  initialConfig?: UnpivotRowComponentConfiguration;
  onClose: () => void;
  onSave: (config: UnpivotRowComponentConfiguration) => void;
}

// Helper to map string to DataType
const mapStringToDataType = (type: string): DataType => {
  const upper = type.toUpperCase();
  if (upper.includes('INT')) return 'INTEGER';
  if (upper.includes('DEC') || upper.includes('NUM')) return 'DECIMAL';
  if (upper.includes('BOOL')) return 'BOOLEAN';
  if (upper.includes('DATE')) return 'DATE';
  if (upper.includes('TIMESTAMP')) return 'TIMESTAMP';
  if (upper.includes('BIN')) return 'BINARY';
  return 'STRING';
};

const UnpivotRowEditor: React.FC<UnpivotRowEditorProps> = ({
  nodeId,
  inputColumns,
  initialConfig,
  onClose,
  onSave
}) => {
  // State
  const [keyColumns, setKeyColumns] = useState<string[]>(initialConfig?.keyColumns || []);
  const [unpivotColumns, setUnpivotColumns] = useState<string[]>(initialConfig?.unpivotColumns || []);
  const [columnNameColumn, setColumnNameColumn] = useState(initialConfig?.columnNameColumn || 'attribute');
  const [valueColumn, setValueColumn] = useState(initialConfig?.valueColumn || 'value');
  const [valueDataType, setValueDataType] = useState<DataType | ''>(initialConfig?.valueDataType || '');
  const [nullHandling, setNullHandling] = useState<'INCLUDE' | 'EXCLUDE'>(initialConfig?.nullHandling || 'EXCLUDE');

  // Derived data
  const availableColumns = useMemo(() => inputColumns.map(c => c.name), [inputColumns]);

  // Toggle a column as key (removes from unpivot if present)
  const toggleKey = useCallback((col: string) => {
    setKeyColumns(prev => {
      if (prev.includes(col)) {
        return prev.filter(c => c !== col);
      } else {
        // Remove from unpivot if present
        setUnpivotColumns(up => up.filter(c => c !== col));
        return [...prev, col];
      }
    });
  }, []);

  // Toggle a column as unpivot (removes from key if present)
  const toggleUnpivot = useCallback((col: string) => {
    setUnpivotColumns(prev => {
      if (prev.includes(col)) {
        return prev.filter(c => c !== col);
      } else {
        // Remove from key if present
        setKeyColumns(k => k.filter(c => c !== col));
        return [...prev, col];
      }
    });
  }, []);

  // Select/unselect all columns as unpivot
  const selectAllUnpivot = useCallback(() => {
    setUnpivotColumns(availableColumns);
    setKeyColumns([]);
  }, [availableColumns]);

  const clearAll = useCallback(() => {
    setKeyColumns([]);
    setUnpivotColumns([]);
  }, []);

  // Validation
  const isValid = keyColumns.length > 0 && unpivotColumns.length > 0 && columnNameColumn.trim() !== '' && valueColumn.trim() !== '';

  // Generate output schema preview
  const outputSchemaPreview = useMemo(() => {
    const cols = [
      ...keyColumns.map(name => ({ name, type: inputColumns.find(c => c.name === name)?.type || 'STRING' })),
      { name: columnNameColumn, type: 'STRING' },
      { name: valueColumn, type: valueDataType || (unpivotColumns.length > 0 ? inputColumns.find(c => c.name === unpivotColumns[0])?.type || 'STRING' : 'STRING') }
    ];
    return cols;
  }, [keyColumns, unpivotColumns, columnNameColumn, valueColumn, valueDataType, inputColumns]);

  // Build configuration on save
  const handleSave = () => {
    if (!isValid) return;

    const config: UnpivotRowComponentConfiguration = {
      version: "1.0",
      keyColumns,
      unpivotColumns,
      columnNameColumn,
      valueColumn,
      valueDataType: valueDataType || undefined,
      nullHandling,
      outputSchema: {
        id: `${nodeId}_output_schema`,
        name: 'Unpivot Output Schema',
        fields: outputSchemaPreview.map((col, idx) => ({
          id: `out-${idx}`,
          name: col.name,
          type: mapStringToDataType(col.type),
          nullable: true,
          isKey: keyColumns.includes(col.name),
          description: `Unpivoted column`
        })),
        isTemporary: false,
        isMaterialized: false
      },
      sqlGeneration: {
        requiresUnnest: true,
        estimatedRowMultiplier: Math.max(1, unpivotColumns.length),
        parallelizable: true
      },
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        createdBy: 'unpivot-editor',
        keyCount: keyColumns.length,
        unpivotCount: unpivotColumns.length,
        validationStatus: isValid ? 'VALID' : 'WARNING',
        warnings: isValid ? [] : ['Key columns and unpivot columns required'],
        dependencies: [],
        compiledSql: undefined
      }
    };

    onSave(config);
  };

  // Render column list with checkboxes
  const renderColumnList = () => (
    <div className="flex-1 border rounded-lg overflow-hidden">
      <div className="bg-gradient-to-r from-gray-100 to-gray-200 px-4 py-3 font-semibold border-b flex items-center justify-between">
        <span>Input Columns ({availableColumns.length})</span>
        <div className="space-x-2">
          <button
            onClick={selectAllUnpivot}
            className="text-xs bg-blue-100 text-blue-700 px-2 py-1 rounded hover:bg-blue-200"
          >
            All Unpivot
          </button>
          <button
            onClick={clearAll}
            className="text-xs bg-gray-200 text-gray-700 px-2 py-1 rounded hover:bg-gray-300"
          >
            Clear
          </button>
        </div>
      </div>
      <div className="overflow-y-auto max-h-96">
        {inputColumns.map(col => (
          <div key={col.name} className="flex items-center px-4 py-2 border-b hover:bg-gray-50">
            <div className="flex-1">
              <span className="font-medium">{col.name}</span>
              <span className="ml-2 text-xs text-gray-500">{col.type}</span>
            </div>
            <div className="flex space-x-3">
              <label className="flex items-center space-x-1 text-xs">
                <input
                  type="checkbox"
                  checked={keyColumns.includes(col.name)}
                  onChange={() => toggleKey(col.name)}
                  className="rounded border-gray-300"
                />
                <span>Key</span>
              </label>
              <label className="flex items-center space-x-1 text-xs">
                <input
                  type="checkbox"
                  checked={unpivotColumns.includes(col.name)}
                  onChange={() => toggleUnpivot(col.name)}
                  className="rounded border-gray-300"
                />
                <span>Unpivot</span>
              </label>
            </div>
          </div>
        ))}
      </div>
    </div>
  );

  // Render output settings
  const renderOutputSettings = () => (
    <div className="space-y-4 p-4 border rounded-lg bg-gray-50">
      <h3 className="font-semibold text-sm">Output Columns</h3>
      <div className="grid grid-cols-2 gap-4">
        <div>
          <label className="block text-xs font-medium text-gray-700">Column Name Column</label>
          <input
            type="text"
            value={columnNameColumn}
            onChange={e => setColumnNameColumn(e.target.value)}
            className="mt-1 block w-full text-sm border border-gray-300 rounded-md px-3 py-2"
            placeholder="e.g. attribute"
          />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-700">Value Column</label>
          <input
            type="text"
            value={valueColumn}
            onChange={e => setValueColumn(e.target.value)}
            className="mt-1 block w-full text-sm border border-gray-300 rounded-md px-3 py-2"
            placeholder="e.g. value"
          />
        </div>
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div>
          <label className="block text-xs font-medium text-gray-700">Value Data Type (optional)</label>
          <select
            value={valueDataType}
            onChange={e => setValueDataType(e.target.value as DataType | '')}
            className="mt-1 block w-full text-sm border border-gray-300 rounded-md px-3 py-2"
          >
            <option value="">Same as source</option>
            <option value="STRING">STRING</option>
            <option value="INTEGER">INTEGER</option>
            <option value="DECIMAL">DECIMAL</option>
            <option value="BOOLEAN">BOOLEAN</option>
            <option value="DATE">DATE</option>
            <option value="TIMESTAMP">TIMESTAMP</option>
          </select>
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-700">Null Handling</label>
          <select
            value={nullHandling}
            onChange={e => setNullHandling(e.target.value as 'INCLUDE' | 'EXCLUDE')}
            className="mt-1 block w-full text-sm border border-gray-300 rounded-md px-3 py-2"
          >
            <option value="INCLUDE">Include NULL values</option>
            <option value="EXCLUDE">Exclude NULL values</option>
          </select>
        </div>
      </div>

      <div className="pt-2">
        <h4 className="text-xs font-medium text-gray-700 mb-2">Output Schema Preview</h4>
        <div className="bg-white border rounded-md text-xs">
          <div className="grid grid-cols-2 gap-2 p-2 border-b font-medium">
            <div>Column</div>
            <div>Type</div>
          </div>
          {outputSchemaPreview.map(col => (
            <div key={col.name} className="grid grid-cols-2 gap-2 p-2 border-b last:border-b-0">
              <div>{col.name}</div>
              <div>{col.type}</div>
            </div>
          ))}
        </div>
        <p className="text-xs text-gray-500 mt-2">
          {keyColumns.length} key columns, {unpivotColumns.length} unpivot columns → {keyColumns.length + 2} output columns
        </p>
      </div>
    </div>
  );

  return (
    <div className="fixed inset-0 z-[10000] bg-black bg-opacity-70 flex items-center justify-center p-4">
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        className="bg-white rounded-xl shadow-2xl w-full max-w-5xl max-h-[90vh] overflow-hidden flex flex-col"
      >
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b bg-gradient-to-r from-blue-50 to-gray-50">
          <div>
            <h2 className="text-xl font-bold flex items-center">
              <span className="mr-2">🔄</span>
              Unpivot Row Configuration
              <span className="ml-2 text-xs bg-purple-100 text-purple-800 px-2 py-0.5 rounded">
                tUnpivotRow
              </span>
            </h2>
            <p className="text-sm text-gray-600 mt-1">
              Transform columns into rows – configure which columns to unpivot.
            </p>
          </div>
          <button
            onClick={onClose}
            className="p-2 hover:bg-gray-100 rounded-full transition-colors"
          >
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto p-6">
          <div className="grid grid-cols-2 gap-6">
            {/* Left: column selection */}
            {renderColumnList()}

            {/* Right: output settings */}
            {renderOutputSettings()}
          </div>
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end gap-3 p-4 border-t bg-gray-50">
          <button
            onClick={onClose}
            className="px-4 py-2 text-sm border border-gray-300 rounded hover:bg-gray-50 transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={handleSave}
            disabled={!isValid}
            className={`px-4 py-2 text-sm rounded transition-colors ${
              isValid
                ? 'bg-gradient-to-r from-blue-500 to-blue-600 text-white hover:from-blue-600 hover:to-blue-700'
                : 'bg-gray-300 text-gray-500 cursor-not-allowed'
            }`}
          >
            Save Configuration
          </button>
        </div>
      </motion.div>
    </div>
  );
};

export default UnpivotRowEditor;