import React, { useState, useEffect, useMemo } from 'react';
import { ChevronDown, ChevronUp, X } from 'lucide-react';
import { UnifiedCanvasNode, NormalizeComponentConfiguration } from '../../../types/unified-pipeline.types';

// SimpleColumn interface (same as in MapEditor)
interface SimpleColumn {
  name: string;
  type?: string;
  id?: string;
}

interface NormalizeEditorProps {
  nodeId: string;
  nodeMetadata: UnifiedCanvasNode;
  inputColumns: SimpleColumn[];       // from connected upstream nodes
  initialConfig?: NormalizeComponentConfiguration;
  onClose: () => void;
  onSave: (config: NormalizeComponentConfiguration) => void;
}

// Helper: generate a unique ID

// Default configuration
const DEFAULT_CONFIG: NormalizeComponentConfiguration = {
  version: '1.0',
  sourceColumn: '',
  delimiter: ',',
  trimValues: true,
  treatEmptyAsNull: false,
  outputColumnName: '', // will be set after sourceColumn selection
  addRowNumber: false,
  rowNumberColumnName: 'row_index',
  keepColumns: [], // all columns except source by default
  errorHandling: 'fail',
  batchSize: 1000,
  parallelization: false,
};

// Mock sample data for preview (simulate first few rows from source)
const createSampleData = (columns: SimpleColumn[]) => {
  if (columns.length === 0) return [];
  const sampleRows = [
    { id: 1, name: 'Alice', tags: 'a,b,c', department: 'Engineering', salary: 75000 },
    { id: 2, name: 'Bob', tags: 'x,y', department: 'Marketing', salary: 65000 },
    { id: 3, name: 'Charlie', tags: 'p,q,r,s', department: 'Sales', salary: 70000 },
  ];
  // filter columns that exist in our mock data
  return sampleRows.map(row => {
    const filtered: Record<string, any> = {};
    columns.forEach(col => {
      if (col.name in row) {
        filtered[col.name] = row[col.name as keyof typeof row];
      } else {
        filtered[col.name] = `[${col.name}]`; // placeholder
      }
    });
    return filtered;
  });
};

const NormalizeEditor: React.FC<NormalizeEditorProps> = ({
  nodeId,
  nodeMetadata,
  inputColumns,
  initialConfig,
  onClose,
  onSave,
}) => {
  // State for all form fields
  const [sourceColumn, setSourceColumn] = useState(initialConfig?.sourceColumn || '');
  const [delimiter, setDelimiter] = useState(initialConfig?.delimiter || DEFAULT_CONFIG.delimiter);
  const [trimValues, setTrimValues] = useState(initialConfig?.trimValues ?? DEFAULT_CONFIG.trimValues);
  const [treatEmptyAsNull, setTreatEmptyAsNull] = useState(initialConfig?.treatEmptyAsNull ?? DEFAULT_CONFIG.treatEmptyAsNull);
  const [quoteChar, setQuoteChar] = useState(initialConfig?.quoteChar || '');
  const [escapeChar, setEscapeChar] = useState(initialConfig?.escapeChar || '');
  const [outputColumnName, setOutputColumnName] = useState(initialConfig?.outputColumnName || '');
  const [addRowNumber, setAddRowNumber] = useState(initialConfig?.addRowNumber ?? DEFAULT_CONFIG.addRowNumber);
  const [rowNumberColumnName, setRowNumberColumnName] = useState(initialConfig?.rowNumberColumnName || DEFAULT_CONFIG.rowNumberColumnName);
  const [keepColumns, setKeepColumns] = useState<string[]>(() => {
    if (initialConfig?.keepColumns) return initialConfig.keepColumns;
    // default: keep all except source column (if source column known)
    return inputColumns.map(col => col.name).filter(name => name !== sourceColumn);
  });
  const [errorHandling, setErrorHandling] = useState<'fail' | 'skip' | 'setNull'>(initialConfig?.errorHandling || DEFAULT_CONFIG.errorHandling);
  const [batchSize, setBatchSize] = useState(initialConfig?.batchSize ?? DEFAULT_CONFIG.batchSize);
  const [parallelization, setParallelization] = useState(initialConfig?.parallelization ?? DEFAULT_CONFIG.parallelization);

  // UI state
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [validationErrors, setValidationErrors] = useState<Record<string, string>>({});

  // When source column changes, update outputColumnName if not set, and adjust keepColumns
  useEffect(() => {
    if (sourceColumn && !outputColumnName) {
      setOutputColumnName(sourceColumn); // default to source column name
    }
    // ensure keepColumns does not include the source column
    setKeepColumns(prev => prev.filter(col => col !== sourceColumn));
  }, [sourceColumn, outputColumnName]);

  // Sample data for preview
  const sampleData = useMemo(() => createSampleData(inputColumns), [inputColumns]);

  // Generate normalized preview rows based on current settings
  // Generate normalized preview rows based on current settings
const previewRows = useMemo(() => {
  if (!sourceColumn || !delimiter || !outputColumnName) return [];

  const rows: any[] = [];
  sampleData.forEach((row) => {
    const value = row[sourceColumn];
    if (value === undefined || value === null) return;

    const parts = String(value).split(delimiter);
    parts.forEach((part, partIdx) => {
      let normalizedValue: string | null = part;
      if (trimValues) normalizedValue = normalizedValue.trim();
      if (treatEmptyAsNull && normalizedValue === '') normalizedValue = null;

      const newRow: any = {};
      // keep selected columns
      keepColumns.forEach(col => {
        newRow[col] = row[col];
      });
      // add normalized column
      newRow[outputColumnName] = normalizedValue;
      // add row number if requested
      if (addRowNumber) {
        newRow[rowNumberColumnName || 'row_index'] = partIdx + 1;
      }
      rows.push(newRow);
    });
  });
  return rows;
}, [sourceColumn, delimiter, trimValues, treatEmptyAsNull, keepColumns, outputColumnName, addRowNumber, rowNumberColumnName, sampleData]);

  // Validate form before save
  const validate = (): boolean => {
    const errors: Record<string, string> = {};
    if (!sourceColumn) errors.sourceColumn = 'Source column is required.';
    if (!delimiter) errors.delimiter = 'Delimiter cannot be empty.';
    if (!outputColumnName) errors.outputColumnName = 'Output column name is required.';
    if (addRowNumber && !rowNumberColumnName) errors.rowNumberColumnName = 'Row number column name is required.';
    if (keepColumns.length === 0) errors.keepColumns = 'At least one column must be kept.';
    setValidationErrors(errors);
    return Object.keys(errors).length === 0;
  };

  const handleSave = () => {
    if (!validate()) return;

    const config: NormalizeComponentConfiguration = {
      version: '1.0',
      sourceColumn,
      delimiter,
      trimValues,
      treatEmptyAsNull,
      quoteChar: quoteChar || undefined,
      escapeChar: escapeChar || undefined,
      outputColumnName,
      addRowNumber,
      rowNumberColumnName: addRowNumber ? rowNumberColumnName : undefined,
      keepColumns,
      errorHandling,
      batchSize,
      parallelization,
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        createdBy: 'canvas',
        validationStatus: 'VALID',
        warnings: [],
      },
    };
    onSave(config);
  };

  // Toggle keep column
  const toggleKeepColumn = (colName: string) => {
    setKeepColumns(prev =>
      prev.includes(colName)
        ? prev.filter(c => c !== colName)
        : [...prev, colName]
    );
  };

  // Available columns for keep (all except source)

  return (
    <div className="fixed inset-0 z-[9999] bg-black bg-opacity-70 flex items-center justify-center p-4">
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-6xl max-h-[90vh] flex flex-col">
        {/* Header */}
        <div className="flex justify-between items-center p-6 border-b bg-gradient-to-r from-blue-50 to-gray-50">
          <div>
            <h2 className="text-xl font-bold flex items-center">
              <span className="mr-2">🔀</span>
              tNormalize Configuration
              <span className="ml-2 text-xs bg-purple-100 text-purple-800 px-2 py-0.5 rounded">
                v{initialConfig?.version || '1.0'}
              </span>
            </h2>
            <p className="text-sm text-gray-600 mt-1">
              Node: <span className="font-semibold text-blue-600">{nodeMetadata.name || nodeId}</span>
              <span className="ml-3 text-xs bg-gray-100 text-gray-800 px-2 py-0.5 rounded">
                {inputColumns.length} input columns
              </span>
            </p>
          </div>
          <button
            onClick={onClose}
            className="p-2 hover:bg-gray-100 rounded-full transition-colors"
            title="Close"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Main Content - Two Column Layout */}
        <div className="flex flex-1 overflow-hidden">
          {/* Left: Input Schema Panel */}
          <div className="w-1/3 border-r bg-gray-50 p-4 overflow-y-auto">
            <h3 className="font-semibold text-gray-700 mb-3 flex items-center">
              <span className="mr-2">📋</span> Input Schema
            </h3>
            <p className="text-xs text-gray-500 mb-4">
              Select columns to keep (propagated to output rows). The source column itself will be normalized.
            </p>
            <div className="space-y-2">
              {inputColumns.map(col => {
                const isSource = col.name === sourceColumn;
                const isKept = keepColumns.includes(col.name);
                return (
                  <div
                    key={col.name}
                    className={`flex items-center p-2 rounded border ${
                      isSource
                        ? 'bg-blue-50 border-blue-300'
                        : 'bg-white border-gray-200'
                    }`}
                  >
                    <input
                      type="checkbox"
                      checked={isKept}
                      onChange={() => toggleKeepColumn(col.name)}
                      disabled={isSource}
                      className="mr-3 h-4 w-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                    />
                    <div className="flex-1">
                      <span className="font-medium text-sm">{col.name}</span>
                      {col.type && (
                        <span className="ml-2 text-xs bg-gray-200 text-gray-700 px-1.5 py-0.5 rounded">
                          {col.type}
                        </span>
                      )}
                    </div>
                    {isSource && (
                      <span className="text-xs bg-blue-100 text-blue-800 px-2 py-0.5 rounded">
                        source
                      </span>
                    )}
                  </div>
                );
              })}
            </div>
            {validationErrors.keepColumns && (
              <p className="text-xs text-red-500 mt-2">{validationErrors.keepColumns}</p>
            )}
          </div>

          {/* Right: Configuration Settings */}
          <div className="w-2/3 p-4 overflow-y-auto">
            <div className="grid grid-cols-2 gap-6">
              {/* Left column of settings */}
              <div className="space-y-4">
                {/* Source Column */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Source Column <span className="text-red-500">*</span>
                  </label>
                  <select
                    value={sourceColumn}
                    onChange={(e) => setSourceColumn(e.target.value)}
                    className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                  >
                    <option value="">-- Select column --</option>
                    {inputColumns.map(col => (
                      <option key={col.name} value={col.name}>
                        {col.name} {col.type ? `(${col.type})` : ''}
                      </option>
                    ))}
                  </select>
                  {validationErrors.sourceColumn && (
                    <p className="text-xs text-red-500 mt-1">{validationErrors.sourceColumn}</p>
                  )}
                </div>

                {/* Delimiter */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Delimiter <span className="text-red-500">*</span>
                  </label>
                  <div className="flex space-x-2">
                    <input
                      type="text"
                      value={delimiter}
                      onChange={(e) => setDelimiter(e.target.value)}
                      className="flex-1 px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                      placeholder="e.g., , | \t"
                      maxLength={5}
                    />
                    <button
                      type="button"
                      onClick={() => setDelimiter(',')}
                      className="px-3 py-2 border border-gray-300 rounded-md bg-gray-50 hover:bg-gray-100 text-sm"
                    >
                      ,
                    </button>
                    <button
                      type="button"
                      onClick={() => setDelimiter('|')}
                      className="px-3 py-2 border border-gray-300 rounded-md bg-gray-50 hover:bg-gray-100 text-sm"
                    >
                      |
                    </button>
                    <button
                      type="button"
                      onClick={() => setDelimiter('\t')}
                      className="px-3 py-2 border border-gray-300 rounded-md bg-gray-50 hover:bg-gray-100 text-sm"
                    >
                      \t
                    </button>
                  </div>
                  {validationErrors.delimiter && (
                    <p className="text-xs text-red-500 mt-1">{validationErrors.delimiter}</p>
                  )}
                </div>

                {/* Trim values */}
                <div className="flex items-center">
                  <input
                    type="checkbox"
                    id="trimValues"
                    checked={trimValues}
                    onChange={(e) => setTrimValues(e.target.checked)}
                    className="h-4 w-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                  />
                  <label htmlFor="trimValues" className="ml-2 text-sm text-gray-700">
                    Trim whitespace from values
                  </label>
                </div>

                {/* Treat empty as null */}
                <div className="flex items-center">
                  <input
                    type="checkbox"
                    id="treatEmptyAsNull"
                    checked={treatEmptyAsNull}
                    onChange={(e) => setTreatEmptyAsNull(e.target.checked)}
                    className="h-4 w-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                  />
                  <label htmlFor="treatEmptyAsNull" className="ml-2 text-sm text-gray-700">
                    Treat empty string as NULL
                  </label>
                </div>

                {/* Quote character */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Quote character (optional)
                  </label>
                  <input
                    type="text"
                    value={quoteChar}
                    onChange={(e) => setQuoteChar(e.target.value)}
                    className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                    placeholder='e.g., "'
                    maxLength={1}
                  />
                </div>

                {/* Escape character */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Escape character (optional)
                  </label>
                  <input
                    type="text"
                    value={escapeChar}
                    onChange={(e) => setEscapeChar(e.target.value)}
                    className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                    placeholder='e.g., \'
                    maxLength={1}
                  />
                </div>
              </div>

              {/* Right column of settings */}
              <div className="space-y-4">
                {/* Output column name */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Output Column Name <span className="text-red-500">*</span>
                  </label>
                  <input
                    type="text"
                    value={outputColumnName}
                    onChange={(e) => setOutputColumnName(e.target.value)}
                    className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                    placeholder="e.g., normalized_value"
                  />
                  {validationErrors.outputColumnName && (
                    <p className="text-xs text-red-500 mt-1">{validationErrors.outputColumnName}</p>
                  )}
                </div>

                {/* Add row number column */}
                <div className="flex items-center">
                  <input
                    type="checkbox"
                    id="addRowNumber"
                    checked={addRowNumber}
                    onChange={(e) => {
                      setAddRowNumber(e.target.checked);
                      if (!e.target.checked) setRowNumberColumnName('');
                    }}
                    className="h-4 w-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                  />
                  <label htmlFor="addRowNumber" className="ml-2 text-sm text-gray-700">
                    Add row number column
                  </label>
                </div>

                {addRowNumber && (
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Row number column name
                    </label>
                    <input
                      type="text"
                      value={rowNumberColumnName}
                      onChange={(e) => setRowNumberColumnName(e.target.value)}
                      className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                      placeholder="e.g., row_index"
                    />
                    {validationErrors.rowNumberColumnName && (
                      <p className="text-xs text-red-500 mt-1">{validationErrors.rowNumberColumnName}</p>
                    )}
                  </div>
                )}

                {/* Advanced Options Toggle */}
                <div className="pt-4">
                  <button
                    type="button"
                    onClick={() => setShowAdvanced(!showAdvanced)}
                    className="flex items-center text-sm text-gray-600 hover:text-gray-900"
                  >
                    {showAdvanced ? <ChevronUp className="h-4 w-4 mr-1" /> : <ChevronDown className="h-4 w-4 mr-1" />}
                    Advanced Options
                  </button>
                </div>

                {showAdvanced && (
                  <div className="space-y-4 mt-2 p-4 border rounded-lg bg-gray-50">
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">
                        Error Handling
                      </label>
                      <select
                        value={errorHandling}
                        onChange={(e) => setErrorHandling(e.target.value as any)}
                        className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                      >
                        <option value="fail">Fail (stop job)</option>
                        <option value="skip">Skip row</option>
                        <option value="setNull">Set NULL and continue</option>
                      </select>
                    </div>

                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">
                        Batch Size
                      </label>
                      <input
                        type="number"
                        value={batchSize}
                        onChange={(e) => setBatchSize(parseInt(e.target.value) || 1000)}
                        min="1"
                        max="100000"
                        className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                      />
                    </div>

                    <div className="flex items-center">
                      <input
                        type="checkbox"
                        id="parallelization"
                        checked={parallelization}
                        onChange={(e) => setParallelization(e.target.checked)}
                        className="h-4 w-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                      />
                      <label htmlFor="parallelization" className="ml-2 text-sm text-gray-700">
                        Enable parallel processing
                      </label>
                    </div>
                  </div>
                )}
              </div>
            </div>

            {/* Preview Section */}
            <div className="mt-6 border-t pt-6">
              <h3 className="font-semibold text-gray-700 mb-3 flex items-center">
                <span className="mr-2">👁️</span> Preview (first 3 rows after normalization)
              </h3>
              <div className="bg-gray-50 p-4 rounded-lg overflow-auto max-h-60">
                {previewRows.length > 0 ? (
                  <table className="min-w-full text-xs border-collapse">
                    <thead className="bg-gray-200">
                      <tr>
                        {Object.keys(previewRows[0]).map(key => (
                          <th key={key} className="px-3 py-2 text-left font-medium text-gray-700 border">
                            {key}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {previewRows.slice(0, 10).map((row, idx) => (
                        <tr key={idx} className="hover:bg-gray-100">
                          {Object.values(row).map((val, colIdx) => (
                            <td key={colIdx} className="px-3 py-2 border border-gray-200">
                              {val === null ? <span className="text-gray-400 italic">NULL</span> : String(val)}
                            </td>
                          ))}
                        </tr>
                      ))}
                      {previewRows.length > 10 && (
                        <tr>
                          <td colSpan={Object.keys(previewRows[0]).length} className="px-3 py-2 text-center text-gray-500">
                            ... and {previewRows.length - 10} more rows
                          </td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                ) : (
                  <p className="text-center text-gray-500 py-4">
                    No preview data available. Select source column and delimiter.
                  </p>
                )}
              </div>
            </div>
          </div>
        </div>

        {/* Footer */}
        <div className="flex justify-end items-center p-4 border-t bg-gray-50 space-x-3">
          <button
            onClick={onClose}
            className="px-4 py-2 text-sm border border-gray-300 rounded-md hover:bg-gray-100"
          >
            Cancel
          </button>
          <button
            onClick={handleSave}
            className="px-4 py-2 text-sm bg-gradient-to-r from-blue-500 to-blue-600 text-white rounded-md hover:from-blue-600 hover:to-blue-700"
          >
            Save Configuration
          </button>
        </div>
      </div>
    </div>
  );
};

export default NormalizeEditor;