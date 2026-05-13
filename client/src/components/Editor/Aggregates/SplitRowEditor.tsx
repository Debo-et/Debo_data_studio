// src/components/Editor/SplitRowEditor.tsx
import React, { useState, useEffect } from 'react';
import { SimpleColumn } from '../Mapping/MapEditor';
import { NormalizeComponentConfiguration } from '../../../types/unified-pipeline.types';

interface SplitRowEditorProps {
  nodeId: string;
  nodeName?: string;
  inputColumns: SimpleColumn[];
  initialConfig?: NormalizeComponentConfiguration;
  onClose: () => void;
  onSave: (config: NormalizeComponentConfiguration) => void;
}

const SplitRowEditor: React.FC<SplitRowEditorProps> = ({
  nodeId,
  nodeName,
  inputColumns,
  initialConfig,
  onClose,
  onSave,
}) => {
  const [sourceColumn, setSourceColumn] = useState(initialConfig?.sourceColumn || '');
  const [delimiter, setDelimiter] = useState(initialConfig?.delimiter || ',');
  const [quoteChar, setQuoteChar] = useState(initialConfig?.quoteChar || '');
  const [escapeChar, setEscapeChar] = useState(initialConfig?.escapeChar || '');
  const [trimValues, setTrimValues] = useState(initialConfig?.trimValues ?? true);
  const [treatEmptyAsNull, setTreatEmptyAsNull] = useState(initialConfig?.treatEmptyAsNull ?? false);
  const [outputColumnName, setOutputColumnName] = useState(initialConfig?.outputColumnName || 'split_value');
  const [addRowNumber, setAddRowNumber] = useState(initialConfig?.addRowNumber ?? false);
  const [rowNumberColumnName, setRowNumberColumnName] = useState(initialConfig?.rowNumberColumnName || 'row_index');
  const [keepColumns, setKeepColumns] = useState<string[]>(initialConfig?.keepColumns || inputColumns.map(c => c.name));
  const [errorHandling, setErrorHandling] = useState<'fail' | 'skip' | 'setNull'>(initialConfig?.errorHandling || 'fail');
  const [parallelization, setParallelization] = useState(initialConfig?.parallelization ?? true);
  const [batchSize, setBatchSize] = useState(initialConfig?.batchSize || 1000);

  const [errors, setErrors] = useState<Record<string, string>>({});

  useEffect(() => {
    const newErrors: Record<string, string> = {};
    if (!sourceColumn) newErrors.sourceColumn = 'Source column is required';
    if (!delimiter.trim()) newErrors.delimiter = 'Delimiter is required';
    if (!outputColumnName.trim()) newErrors.outputColumnName = 'Output column name is required';
    if (keepColumns.includes(outputColumnName) || (addRowNumber && rowNumberColumnName === outputColumnName)) {
      newErrors.outputColumnName = 'Output column name must be unique';
    }
    if (addRowNumber) {
      if (!rowNumberColumnName.trim()) newErrors.rowNumberColumnName = 'Row number column name is required';
      if (keepColumns.includes(rowNumberColumnName) || rowNumberColumnName === outputColumnName) {
        newErrors.rowNumberColumnName = 'Row number column name must be unique';
      }
    }
    setErrors(newErrors);
  }, [sourceColumn, delimiter, outputColumnName, keepColumns, addRowNumber, rowNumberColumnName]);

  const handleSave = () => {
    if (Object.keys(errors).length > 0) return;
    const config: NormalizeComponentConfiguration = {
      version: '1.0',
      sourceColumn,
      delimiter,
      trimValues,
      treatEmptyAsNull,
      ...(quoteChar && { quoteChar }),
      ...(escapeChar && { escapeChar }),
      outputColumnName,
      addRowNumber,
      ...(addRowNumber && { rowNumberColumnName }),
      keepColumns,
      errorHandling,
      parallelization,
      batchSize,
      // sqlGeneration is omitted – it is optional and filled by the compiler later
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        createdBy: 'user',
        validationStatus: 'VALID',
        warnings: [],
      },
    };
    onSave(config);
  };

  const toggleAllKeep = (checked: boolean) => {
    setKeepColumns(checked ? inputColumns.map(c => c.name) : []);
  };

  return (
    <div className="fixed inset-0 z-[9999] bg-black bg-opacity-70 flex items-center justify-center p-4">
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-5xl max-h-[90vh] overflow-hidden flex flex-col">
        {/* Header */}
        <div className="p-4 border-b bg-gradient-to-r from-blue-50 to-gray-50 flex justify-between items-center">
          <div>
            <h2 className="text-lg font-bold text-gray-800 flex items-center">
              <span className="mr-2">🔀</span> Split Row Configuration
            </h2>
            <p className="text-sm text-gray-600">
              Node: <span className="font-semibold">{nodeName || nodeId}</span> • {inputColumns.length} input columns
            </p>
          </div>
          <button onClick={onClose} className="p-2 hover:bg-gray-200 rounded-full">✕</button>
        </div>

        {/* Main content */}
        <div className="flex-1 overflow-auto p-6">
          <div className="grid grid-cols-2 gap-8">
            {/* Left: Form */}
            <div className="space-y-6">
              <div>
                <label className="block text-sm font-medium mb-1">Source Column <span className="text-red-500">*</span></label>
                <select
                  value={sourceColumn}
                  onChange={(e) => setSourceColumn(e.target.value)}
                  className="w-full p-2 border rounded focus:ring-2 focus:ring-blue-500"
                >
                  <option value="">Select column...</option>
                  {inputColumns.map(col => (
                    <option key={col.name} value={col.name}>{col.name} ({col.type})</option>
                  ))}
                </select>
                {errors.sourceColumn && <p className="text-xs text-red-500 mt-1">{errors.sourceColumn}</p>}
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium mb-1">Delimiter <span className="text-red-500">*</span></label>
                  <input
                    type="text"
                    value={delimiter}
                    onChange={(e) => setDelimiter(e.target.value)}
                    className="w-full p-2 border rounded"
                    placeholder="e.g., ,"
                  />
                  {errors.delimiter && <p className="text-xs text-red-500 mt-1">{errors.delimiter}</p>}
                </div>
                <div>
                  <label className="block text-sm font-medium mb-1">Quote Character</label>
                  <input
                    type="text"
                    value={quoteChar}
                    onChange={(e) => setQuoteChar(e.target.value)}
                    className="w-full p-2 border rounded"
                    placeholder="e.g., &quot;"
                    maxLength={1}
                  />
                </div>
              </div>

              <div>
                <label className="block text-sm font-medium mb-1">Escape Character</label>
                <input
                  type="text"
                  value={escapeChar}
                  onChange={(e) => setEscapeChar(e.target.value)}
                  className="w-full p-2 border rounded"
                  placeholder="e.g., \\"
                  maxLength={1}
                />
              </div>

              <div className="flex items-center space-x-6">
                <label className="flex items-center">
                  <input type="checkbox" checked={trimValues} onChange={(e) => setTrimValues(e.target.checked)} className="mr-2" />
                  Trim values
                </label>
                <label className="flex items-center">
                  <input type="checkbox" checked={treatEmptyAsNull} onChange={(e) => setTreatEmptyAsNull(e.target.checked)} className="mr-2" />
                  Treat empty as null
                </label>
              </div>

              <div>
                <label className="block text-sm font-medium mb-1">Output Column Name <span className="text-red-500">*</span></label>
                <input
                  type="text"
                  value={outputColumnName}
                  onChange={(e) => setOutputColumnName(e.target.value)}
                  className="w-full p-2 border rounded"
                />
                {errors.outputColumnName && <p className="text-xs text-red-500 mt-1">{errors.outputColumnName}</p>}
              </div>

              <div>
                <label className="flex items-center">
                  <input type="checkbox" checked={addRowNumber} onChange={(e) => setAddRowNumber(e.target.checked)} className="mr-2" />
                  Add row number column
                </label>
                {addRowNumber && (
                  <div className="mt-2 ml-6">
                    <label className="block text-sm font-medium mb-1">Row Number Column Name</label>
                    <input
                      type="text"
                      value={rowNumberColumnName}
                      onChange={(e) => setRowNumberColumnName(e.target.value)}
                      className="w-full p-2 border rounded"
                    />
                    {errors.rowNumberColumnName && <p className="text-xs text-red-500 mt-1">{errors.rowNumberColumnName}</p>}
                  </div>
                )}
              </div>

              <div>
                <label className="block text-sm font-medium mb-2">Keep Columns</label>
                <div className="flex items-center mb-2">
                  <button onClick={() => toggleAllKeep(true)} className="text-xs text-blue-600 mr-2">Select All</button>
                  <button onClick={() => toggleAllKeep(false)} className="text-xs text-gray-600">Clear All</button>
                </div>
                <div className="max-h-40 overflow-y-auto border rounded p-2">
                  {inputColumns.map(col => (
                    <label key={col.name} className="flex items-center py-1">
                      <input
                        type="checkbox"
                        checked={keepColumns.includes(col.name)}
                        onChange={(e) => {
                          if (e.target.checked) {
                            setKeepColumns([...keepColumns, col.name]);
                          } else {
                            setKeepColumns(keepColumns.filter(c => c !== col.name));
                          }
                        }}
                        className="mr-2"
                      />
                      <span className="text-sm">{col.name}</span>
                      <span className="text-xs text-gray-500 ml-2">({col.type})</span>
                    </label>
                  ))}
                </div>
              </div>

              <div>
                <label className="block text-sm font-medium mb-1">Error Handling</label>
                <div className="space-y-1">
                  <label className="flex items-center">
                    <input type="radio" name="error" value="fail" checked={errorHandling === 'fail'} onChange={() => setErrorHandling('fail')} className="mr-2" />
                    Fail job
                  </label>
                  <label className="flex items-center">
                    <input type="radio" name="error" value="skip" checked={errorHandling === 'skip'} onChange={() => setErrorHandling('skip')} className="mr-2" />
                    Skip row
                  </label>
                  <label className="flex items-center">
                    <input type="radio" name="error" value="setNull" checked={errorHandling === 'setNull'} onChange={() => setErrorHandling('setNull')} className="mr-2" />
                    Set null
                  </label>
                </div>
              </div>

              <div>
                <label className="flex items-center">
                  <input type="checkbox" checked={parallelization} onChange={(e) => setParallelization(e.target.checked)} className="mr-2" />
                  Enable parallel processing
                </label>
                {parallelization && (
                  <div className="mt-2 ml-6">
                    <label className="block text-sm font-medium mb-1">Batch Size</label>
                    <input
                      type="number"
                      value={batchSize}
                      onChange={(e) => setBatchSize(parseInt(e.target.value) || 1000)}
                      className="w-full p-2 border rounded"
                      min={1}
                      step={100}
                    />
                  </div>
                )}
              </div>
            </div>

            {/* Right: Schema Preview */}
            <div className="space-y-6">
              <div>
                <h3 className="text-sm font-semibold mb-2">Input Schema</h3>
                <div className="border rounded divide-y max-h-60 overflow-y-auto">
                  {inputColumns.map(col => (
                    <div key={col.name} className={`p-2 text-sm ${col.name === sourceColumn ? 'bg-blue-50' : ''}`}>
                      <span className="font-medium">{col.name}</span>
                      <span className="text-gray-500 ml-2">({col.type})</span>
                      {col.name === sourceColumn && <span className="ml-2 text-xs text-blue-600">(source)</span>}
                    </div>
                  ))}
                </div>
              </div>

              <div>
                <h3 className="text-sm font-semibold mb-2">Output Schema Preview</h3>
                <div className="border rounded divide-y max-h-60 overflow-y-auto">
                  {keepColumns.map(colName => {
                    const col = inputColumns.find(c => c.name === colName);
                    return (
                      <div key={colName} className="p-2 text-sm">
                        <span className="font-medium">{colName}</span>
                        <span className="text-gray-500 ml-2">({col?.type || '?'})</span>
                      </div>
                    );
                  })}
                  <div className="p-2 text-sm bg-green-50">
                    <span className="font-medium text-green-700">{outputColumnName}</span>
                    <span className="text-gray-500 ml-2">(string) ← split values</span>
                  </div>
                  {addRowNumber && (
                    <div className="p-2 text-sm bg-yellow-50">
                      <span className="font-medium text-yellow-700">{rowNumberColumnName}</span>
                      <span className="text-gray-500 ml-2">(integer)</span>
                    </div>
                  )}
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Footer */}
        <div className="p-4 border-t flex justify-end space-x-3">
          <button onClick={onClose} className="px-4 py-2 border rounded hover:bg-gray-50">Cancel</button>
          <button
            onClick={handleSave}
            disabled={Object.keys(errors).length > 0}
            className="px-4 py-2 bg-gradient-to-r from-green-500 to-green-600 text-white rounded hover:from-green-600 hover:to-green-700 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            Save & Compile
          </button>
        </div>
      </div>
    </div>
  );
};

export default SplitRowEditor;