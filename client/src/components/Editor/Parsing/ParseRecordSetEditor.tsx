// src/components/Editor/Parsing/ParseRecordSetEditor.tsx
import React, { useState, useCallback, useMemo } from 'react';
import { DataType, ParseRecordSetComponentConfiguration, ParseRecordSetColumn, FieldSchema } from '../../../types/unified-pipeline.types';
import { SimpleColumn } from '../../../pages/canvas.types';

interface ParseRecordSetEditorProps {
  nodeId: string;
  nodeMetadata: any;
  inputColumns: SimpleColumn[];
  initialConfig?: ParseRecordSetComponentConfiguration;
  onClose: () => void;
  onSave: (config: ParseRecordSetComponentConfiguration) => void;
}

export const ParseRecordSetEditor: React.FC<ParseRecordSetEditorProps> = ({
  nodeId,
  nodeMetadata,
  inputColumns,
  initialConfig,
  onClose,
  onSave
}) => {
  // State
  const [sourceColumn, setSourceColumn] = useState(initialConfig?.sourceColumn || '');
  const [recordDelimiter, setRecordDelimiter] = useState(initialConfig?.recordDelimiter || '\n');
  const [fieldDelimiter, setFieldDelimiter] = useState(initialConfig?.fieldDelimiter || ',');
  const [quoteChar, setQuoteChar] = useState(initialConfig?.quoteChar || '');
  const [escapeChar, setEscapeChar] = useState(initialConfig?.escapeChar || '');
  const [hasHeader, setHasHeader] = useState(initialConfig?.hasHeader ?? true);
  const [trimWhitespace, setTrimWhitespace] = useState(initialConfig?.trimWhitespace ?? true);
  const [nullIfEmpty, setNullIfEmpty] = useState(initialConfig?.nullIfEmpty ?? true);
  const [errorHandling, setErrorHandling] = useState<'fail' | 'skipRow' | 'setNull'>(
    initialConfig?.errorHandling || 'fail'
  );
  const [parallelization, setParallelization] = useState(initialConfig?.parallelization ?? true);
  const [batchSize, setBatchSize] = useState<number>(initialConfig?.batchSize || 1000);
  const [activeTab, setActiveTab] = useState<'basic' | 'columns' | 'advanced' | 'preview'>('basic');

  // Columns state
  const [columns, setColumns] = useState<ParseRecordSetColumn[]>(
    initialConfig?.columns || []
  );

  // Preview state
  const [previewData, setPreviewData] = useState<string[][]>([]);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewError, setPreviewError] = useState<string | null>(null);

  // Helper: generate output schema from columns (used for saving)
  const outputSchema = useMemo(() => ({
    id: `${nodeId}_output_schema`,
    name: `${nodeMetadata.name || 'ParseRecordSet'} Output Schema`,
    fields: columns.map((col): FieldSchema => ({
      id: col.id,
      name: col.name,
      type: col.type,
      length: col.length,
      precision: col.precision,
      scale: col.scale,
      nullable: col.nullable,
      isKey: false,
      defaultValue: col.defaultValue,
      description: '',
      originalName: col.name
    })),
    isTemporary: false,
    isMaterialized: false
  }), [nodeId, nodeMetadata.name, columns]);

  // Column management functions
  const addColumn = () => {
    const newId = `col-${Date.now()}-${Math.random().toString(36).substr(2, 6)}`;
    setColumns([
      ...columns,
      {
        id: newId,
        name: `column_${columns.length + 1}`,
        type: 'STRING' as DataType,
        length: 255,
        precision: undefined,
        scale: undefined,
        nullable: true,
        defaultValue: undefined,
        position: columns.length,
        fieldIndex: columns.length + 1
      }
    ]);
  };

  const updateColumn = (index: number, updates: Partial<ParseRecordSetColumn>) => {
    const newColumns = [...columns];
    newColumns[index] = { ...newColumns[index], ...updates };
    setColumns(newColumns);
  };

  const deleteColumn = (index: number) => {
    setColumns(columns.filter((_, i) => i !== index));
  };

  const moveColumn = (index: number, direction: 'up' | 'down') => {
    if (direction === 'up' && index === 0) return;
    if (direction === 'down' && index === columns.length - 1) return;
    const newColumns = [...columns];
    const target = direction === 'up' ? index - 1 : index + 1;
    [newColumns[index], newColumns[target]] = [newColumns[target], newColumns[index]];
    // Update positions
    newColumns.forEach((col, idx) => col.position = idx);
    setColumns(newColumns);
  };

  // Import columns from header (uses the first record of the source column)
  const importFromHeader = useCallback(async () => {
    if (!sourceColumn) {
      setPreviewError('Please select a source column first.');
      return;
    }
    // In a real implementation, you would fetch sample data from the source column.
    // For now, we simulate with a hardcoded sample or fetch from backend.
    setPreviewLoading(true);
    setPreviewError(null);
    try {
      // Simulate fetching first record from the source column.
      // Replace with actual API call to get sample data.
      const sampleRecord = "name,age,city\nJohn,30,New York";
      const lines = sampleRecord.split(recordDelimiter);
      if (lines.length === 0) {
        setPreviewError('No data available to import header.');
        return;
      }
      const headerLine = lines[0];
      const fields = splitFields(headerLine, fieldDelimiter, quoteChar, escapeChar);
      const newColumns: ParseRecordSetColumn[] = fields.map((field, idx) => ({
        id: `col-${Date.now()}-${idx}`,
        name: sanitizeColumnName(field),
        type: 'STRING' as DataType,
        length: 255,
        nullable: true,
        position: idx,
        fieldIndex: idx + 1
      }));
      setColumns(newColumns);
    } catch (err: any) {
      setPreviewError(err.message || 'Failed to import header');
    } finally {
      setPreviewLoading(false);
    }
  }, [sourceColumn, recordDelimiter, fieldDelimiter, quoteChar, escapeChar]);

  // Helper: split a line into fields considering quotes and escapes
  const splitFields = (line: string, delimiter: string, quoteChar: string, escapeChar: string): string[] => {
    // Simple implementation; replace with a proper CSV parser if needed
    if (!delimiter) return [line];
    const regex = new RegExp(`(?:${quoteChar}([^${escapeChar}${quoteChar}]*(?:${escapeChar}[^${escapeChar}${quoteChar}]*)*)${quoteChar}|[^${delimiter}]+)`, 'g');
    const matches = line.match(regex);
    return matches ? matches.map(m => m.replace(new RegExp(`^${quoteChar}|${quoteChar}$`, 'g'), '')) : [];
  };

  const sanitizeColumnName = (name: string): string => {
    return name.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase();
  };

  // Load preview data
  const loadPreview = useCallback(async () => {
    if (!sourceColumn) {
      setPreviewError('Please select a source column.');
      return;
    }
    setPreviewLoading(true);
    setPreviewError(null);
    try {
      // In real implementation, call an API to get sample rows from the source column.
      // For now, simulate with a small dataset.
      const sampleData = [
        "John,30,New York",
        "Jane,25,Los Angeles",
        "Bob,35,Chicago"
      ];
      const parsedRows: string[][] = [];
      for (const line of sampleData) {
        const fields = splitFields(line, fieldDelimiter, quoteChar, escapeChar);
        if (trimWhitespace) {
          fields.forEach((f, i) => fields[i] = f.trim());
        }
        parsedRows.push(fields);
      }
      setPreviewData(parsedRows);
    } catch (err: any) {
      setPreviewError(err.message || 'Failed to load preview');
    } finally {
      setPreviewLoading(false);
    }
  }, [sourceColumn, fieldDelimiter, quoteChar, escapeChar, trimWhitespace]);

  // Save handler
  const handleSave = () => {
    // Basic validation
    if (!sourceColumn) {
      alert('Please select a source column.');
      return;
    }
    if (columns.length === 0) {
      alert('Please define at least one output column.');
      return;
    }

    const config: ParseRecordSetComponentConfiguration = {
      version: '1.0',
      sourceColumn,
      recordDelimiter,
      fieldDelimiter,
      quoteChar: quoteChar || undefined,
      escapeChar: escapeChar || undefined,
      hasHeader,
      columns,
      trimWhitespace,
      nullIfEmpty,
      errorHandling,
      parallelization,
      batchSize,
      outputSchema,
      sqlGeneration: {
        unnestExpression: undefined, // Will be filled by SQL generator
        estimatedRowMultiplier: columns.length
      },
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        createdBy: 'parse-record-set-editor',
        validationStatus: 'VALID',
        warnings: [],
        dependencies: [sourceColumn]
      }
    };
    onSave(config);
  };

  // Render functions for tabs
  const renderBasicTab = () => (
    <div className="space-y-4 p-4">
      <div>
        <label className="block text-sm font-medium mb-1">Source Column</label>
        <select
          value={sourceColumn}
          onChange={(e) => setSourceColumn(e.target.value)}
          className="w-full p-2 border rounded focus:ring-2 focus:ring-blue-500"
        >
          <option value="">Select column...</option>
          {inputColumns.map(col => (
            <option key={col.id || col.name} value={col.name}>{col.name}</option>
          ))}
        </select>
      </div>
      <div>
        <label className="block text-sm font-medium mb-1">Record Delimiter</label>
        <div className="flex space-x-4">
          <label className="flex items-center space-x-1">
            <input type="radio" name="recordDelimiter" value="\n" checked={recordDelimiter === '\n'} onChange={() => setRecordDelimiter('\n')} />
            <span>Newline (\n)</span>
          </label>
          <label className="flex items-center space-x-1">
            <input type="radio" name="recordDelimiter" value="\r" checked={recordDelimiter === '\r'} onChange={() => setRecordDelimiter('\r')} />
            <span>Carriage Return (\r)</span>
          </label>
          <label className="flex items-center space-x-1">
            <input type="radio" name="recordDelimiter" value="custom" checked={!['\n', '\r'].includes(recordDelimiter)} onChange={() => setRecordDelimiter('')} />
            <span>Custom</span>
          </label>
        </div>
        {!['\n', '\r'].includes(recordDelimiter) && (
          <input
            type="text"
            value={recordDelimiter}
            onChange={(e) => setRecordDelimiter(e.target.value)}
            placeholder="Custom delimiter"
            className="mt-2 p-2 border rounded w-full"
          />
        )}
      </div>
      <div>
        <label className="block text-sm font-medium mb-1">Field Delimiter</label>
        <div className="flex space-x-4 flex-wrap">
          <label className="flex items-center space-x-1">
            <input type="radio" name="fieldDelimiter" value="," checked={fieldDelimiter === ','} onChange={() => setFieldDelimiter(',')} />
            <span>Comma (,)</span>
          </label>
          <label className="flex items-center space-x-1">
            <input type="radio" name="fieldDelimiter" value="\t" checked={fieldDelimiter === '\t'} onChange={() => setFieldDelimiter('\t')} />
            <span>Tab (\t)</span>
          </label>
          <label className="flex items-center space-x-1">
            <input type="radio" name="fieldDelimiter" value="|" checked={fieldDelimiter === '|'} onChange={() => setFieldDelimiter('|')} />
            <span>Pipe (|)</span>
          </label>
          <label className="flex items-center space-x-1">
            <input type="radio" name="fieldDelimiter" value=";" checked={fieldDelimiter === ';'} onChange={() => setFieldDelimiter(';')} />
            <span>Semicolon (;)</span>
          </label>
          <label className="flex items-center space-x-1">
            <input type="radio" name="fieldDelimiter" value="custom" checked={!['', '\t', '|', ';'].includes(fieldDelimiter)} onChange={() => setFieldDelimiter('')} />
            <span>Custom</span>
          </label>
        </div>
        {!['', '\t', '|', ';'].includes(fieldDelimiter) && (
          <input
            type="text"
            value={fieldDelimiter}
            onChange={(e) => setFieldDelimiter(e.target.value)}
            placeholder="Custom delimiter"
            className="mt-2 p-2 border rounded w-full"
          />
        )}
      </div>
      <div className="grid grid-cols-2 gap-4">
        <div>
          <label className="block text-sm font-medium mb-1">Quote Character (optional)</label>
          <input type="text" value={quoteChar} onChange={(e) => setQuoteChar(e.target.value)} className="w-full p-2 border rounded" />
        </div>
        <div>
          <label className="block text-sm font-medium mb-1">Escape Character (optional)</label>
          <input type="text" value={escapeChar} onChange={(e) => setEscapeChar(e.target.value)} className="w-full p-2 border rounded" />
        </div>
      </div>
      <div className="flex items-center">
        <input type="checkbox" id="hasHeader" checked={hasHeader} onChange={(e) => setHasHeader(e.target.checked)} className="mr-2" />
        <label htmlFor="hasHeader" className="text-sm">First record contains column names</label>
      </div>
    </div>
  );

  const renderColumnsTab = () => (
    <div className="p-4">
      {columns.length === 0 ? (
        <div className="text-center py-8 text-gray-500">
          No columns defined. Click "Add Column" to begin.
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="min-w-full border-collapse border text-sm">
            <thead>
              <tr className="bg-gray-100">
                <th className="p-2 border">Name</th>
                <th className="p-2 border">Data Type</th>
                <th className="p-2 border">Length</th>
                <th className="p-2 border">Precision</th>
                <th className="p-2 border">Scale</th>
                <th className="p-2 border">Nullable</th>
                <th className="p-2 border">Default Value</th>
                {!hasHeader && <th className="p-2 border">Field Index</th>}
                <th className="p-2 border">Actions</th>
              </tr>
            </thead>
            <tbody>
              {columns.map((col, idx) => (
                <tr key={col.id}>
                  <td className="p-2 border"><input type="text" value={col.name} onChange={(e) => updateColumn(idx, { name: e.target.value })} className="w-full p-1 border rounded" /></td>
                  <td className="p-2 border">
                    <select value={col.type} onChange={(e) => updateColumn(idx, { type: e.target.value as DataType })} className="w-full p-1 border rounded">
                      <option value="STRING">STRING</option>
                      <option value="INTEGER">INTEGER</option>
                      <option value="DECIMAL">DECIMAL</option>
                      <option value="DATE">DATE</option>
                      <option value="TIMESTAMP">TIMESTAMP</option>
                      <option value="BOOLEAN">BOOLEAN</option>
                      <option value="BINARY">BINARY</option>
                    </select>
                  </td>
                  <td className="p-2 border"><input type="number" value={col.length || ''} onChange={(e) => updateColumn(idx, { length: e.target.value ? parseInt(e.target.value) : undefined })} className="w-full p-1 border rounded" disabled={col.type !== 'STRING'} /></td>
                  <td className="p-2 border"><input type="number" value={col.precision || ''} onChange={(e) => updateColumn(idx, { precision: e.target.value ? parseInt(e.target.value) : undefined })} className="w-full p-1 border rounded" disabled={col.type !== 'DECIMAL'} /></td>
                  <td className="p-2 border"><input type="number" value={col.scale || ''} onChange={(e) => updateColumn(idx, { scale: e.target.value ? parseInt(e.target.value) : undefined })} className="w-full p-1 border rounded" disabled={col.type !== 'DECIMAL'} /></td>
                  <td className="p-2 border text-center"><input type="checkbox" checked={col.nullable} onChange={(e) => updateColumn(idx, { nullable: e.target.checked })} /></td>
                  <td className="p-2 border"><input type="text" value={col.defaultValue || ''} onChange={(e) => updateColumn(idx, { defaultValue: e.target.value })} className="w-full p-1 border rounded" /></td>
                  {!hasHeader && (
                    <td className="p-2 border"><input type="number" value={col.fieldIndex || ''} onChange={(e) => updateColumn(idx, { fieldIndex: e.target.value ? parseInt(e.target.value) : undefined })} className="w-full p-1 border rounded" /></td>
                  )}
                  <td className="p-2 border text-center whitespace-nowrap">
                    <button onClick={() => moveColumn(idx, 'up')} className="mr-1 px-1 py-0.5 bg-gray-200 rounded">↑</button>
                    <button onClick={() => moveColumn(idx, 'down')} className="mr-1 px-1 py-0.5 bg-gray-200 rounded">↓</button>
                    <button onClick={() => deleteColumn(idx)} className="text-red-500">✕</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <div className="mt-4 flex space-x-2">
        <button onClick={addColumn} className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600">Add Column</button>
        {hasHeader && (
          <button onClick={importFromHeader} className="px-4 py-2 bg-green-500 text-white rounded hover:bg-green-600">Import from Header</button>
        )}
      </div>
    </div>
  );

  const renderAdvancedTab = () => (
    <div className="space-y-4 p-4">
      <div className="flex items-center"><input type="checkbox" checked={trimWhitespace} onChange={(e) => setTrimWhitespace(e.target.checked)} className="mr-2" /><label>Trim whitespace</label></div>
      <div className="flex items-center"><input type="checkbox" checked={nullIfEmpty} onChange={(e) => setNullIfEmpty(e.target.checked)} className="mr-2" /><label>Treat empty as NULL</label></div>
      <div>
        <label className="block text-sm font-medium mb-1">Error handling</label>
        <div className="flex space-x-4">
          <label className="flex items-center space-x-1"><input type="radio" name="errorHandling" value="fail" checked={errorHandling === 'fail'} onChange={() => setErrorHandling('fail')} /><span>Fail job</span></label>
          <label className="flex items-center space-x-1"><input type="radio" name="errorHandling" value="skipRow" checked={errorHandling === 'skipRow'} onChange={() => setErrorHandling('skipRow')} /><span>Skip row</span></label>
          <label className="flex items-center space-x-1"><input type="radio" name="errorHandling" value="setNull" checked={errorHandling === 'setNull'} onChange={() => setErrorHandling('setNull')} /><span>Set null</span></label>
        </div>
      </div>
      <div className="flex items-center"><input type="checkbox" checked={parallelization} onChange={(e) => setParallelization(e.target.checked)} className="mr-2" /><label>Parallelization</label></div>
      <div>
        <label className="block text-sm font-medium mb-1">Batch size</label>
        <input type="number" value={batchSize} onChange={(e) => setBatchSize(parseInt(e.target.value))} className="p-2 border rounded w-32" min={1} />
      </div>
    </div>
  );

  const renderPreviewTab = () => (
    <div className="p-4">
      <div className="mb-4 flex items-center justify-between">
        <button onClick={loadPreview} className="px-3 py-1 bg-blue-500 text-white rounded hover:bg-blue-600 text-sm" disabled={previewLoading}>Refresh Preview</button>
        {previewLoading && <span className="text-sm text-gray-500">Loading...</span>}
        {previewError && <span className="text-sm text-red-500">{previewError}</span>}
      </div>
      {previewData.length > 0 ? (
        <div className="overflow-x-auto">
          <table className="min-w-full border-collapse border text-sm">
            <thead>
              <tr className="bg-gray-100">
                {columns.map(col => (
                  <th key={col.id} className="p-2 border">{col.name}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {previewData.map((row, idx) => (
                <tr key={idx}>
                  {columns.map((col, colIdx) => {
                    const field = row[colIdx] !== undefined ? row[colIdx] : '';
                    return <td key={col.id} className="p-2 border">{field}</td>;
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="text-center py-8 text-gray-500">No preview data. Click "Refresh Preview" to load sample.</div>
      )}
    </div>
  );

  return (
    <div className="fixed inset-0 z-[10000] bg-black bg-opacity-70 flex items-center justify-center p-4">
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-6xl h-[90vh] flex flex-col">
        {/* Header */}
        <div className="flex justify-between items-center p-6 border-b bg-gradient-to-r from-blue-50 to-gray-50">
          <div>
            <h2 className="text-xl font-bold flex items-center">
              <span className="mr-2">📄</span>
              Parse Record Set Configuration
              <span className="ml-2 text-xs bg-purple-100 text-purple-800 px-2 py-0.5 rounded">tParseRecordSet</span>
            </h2>
            <p className="text-sm text-gray-600 mt-1">Node: <span className="font-semibold">{nodeMetadata.name}</span></p>
          </div>
          <button onClick={onClose} className="p-2 hover:bg-gray-100 rounded-full">✕</button>
        </div>

        {/* Tabs */}
        <div className="flex border-b">
          {(['basic', 'columns', 'advanced', 'preview'] as const).map(tab => (
            <button
              key={tab}
              className={`px-4 py-2 text-sm font-medium ${activeTab === tab ? 'border-b-2 border-blue-500 text-blue-600' : 'text-gray-500 hover:text-gray-700'}`}
              onClick={() => setActiveTab(tab)}
            >
              {tab.charAt(0).toUpperCase() + tab.slice(1)}
            </button>
          ))}
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto">
          {activeTab === 'basic' && renderBasicTab()}
          {activeTab === 'columns' && renderColumnsTab()}
          {activeTab === 'advanced' && renderAdvancedTab()}
          {activeTab === 'preview' && renderPreviewTab()}
        </div>

        {/* Footer */}
        <div className="flex justify-end gap-2 p-4 border-t">
          <button onClick={onClose} className="px-4 py-2 border rounded hover:bg-gray-100">Cancel</button>
          <button onClick={handleSave} className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600">Save</button>
        </div>
      </div>
    </div>
  );
};