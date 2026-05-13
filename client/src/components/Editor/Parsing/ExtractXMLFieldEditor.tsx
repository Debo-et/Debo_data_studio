import React, { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import {
  XPathExpression,
  NamespaceMapping,
  ExtractXMLFieldConfiguration,
} from '../../../types/unified-pipeline.types';

interface ExtractXMLFieldEditorProps {
  nodeId: string;
  nodeName: string;
  inputColumns: Array<{ name: string; type: string }>; // from connected input nodes
  initialConfig?: ExtractXMLFieldConfiguration;
  onSave: (config: ExtractXMLFieldConfiguration) => void;
  onClose: () => void;
}

const ExtractXMLFieldEditor: React.FC<ExtractXMLFieldEditorProps> = ({
  nodeName,
  inputColumns,
  initialConfig,
  onSave,
  onClose,
}) => {
  const [sourceColumn, setSourceColumn] = useState(initialConfig?.sourceColumn || '');
  const [expressions, setExpressions] = useState<XPathExpression[]>(
    initialConfig?.xpathExpressions || []
  );
  const [namespaces, setNamespaces] = useState<NamespaceMapping[]>(
    initialConfig?.namespaceMappings || []
  );
  const [errorHandling, setErrorHandling] = useState<'fail' | 'skipRow' | 'setNull'>(
    initialConfig?.errorHandling || 'fail'
  );
  const [parallel, setParallel] = useState(initialConfig?.parallelization || false);
  const [batchSize, setBatchSize] = useState(initialConfig?.batchSize || 1000);
  const [validationErrors, setValidationErrors] = useState<string[]>([]);

  // Validation on changes
  useEffect(() => {
    const errors: string[] = [];
    if (!sourceColumn) errors.push('Source column is required.');
    if (expressions.length === 0) errors.push('At least one output column is required.');
    const names = new Set<string>();
    expressions.forEach((expr, idx) => {
      if (!expr.outputColumn.trim()) errors.push(`Row ${idx + 1}: Output column name cannot be empty.`);
      if (names.has(expr.outputColumn)) errors.push(`Duplicate output column name: ${expr.outputColumn}`);
      names.add(expr.outputColumn);
      if (!expr.xpath.trim()) errors.push(`Row ${idx + 1}: XPath cannot be empty.`);
    });
    setValidationErrors(errors);
  }, [sourceColumn, expressions]);

  const handleAddExpression = () => {
    const newExpr: XPathExpression = {
      id: `expr-${Date.now()}-${Math.random().toString(36).substr(2, 6)}`,
      outputColumn: `col_${expressions.length + 1}`,
      xpath: '',
      dataType: 'STRING',
      nullable: true,
      position: expressions.length,
    };
    setExpressions([...expressions, newExpr]);
  };

  const handleUpdateExpression = (id: string, updates: Partial<XPathExpression>) => {
    setExpressions(expressions.map(e => (e.id === id ? { ...e, ...updates } : e)));
  };

  const handleRemoveExpression = (id: string) => {
    setExpressions(expressions.filter(e => e.id !== id).map((e, idx) => ({ ...e, position: idx })));
  };

  const handleAddNamespace = () => {
    setNamespaces([...namespaces, { prefix: '', uri: '' }]);
  };

  const handleUpdateNamespace = (index: number, updates: Partial<NamespaceMapping>) => {
    setNamespaces(namespaces.map((ns, i) => (i === index ? { ...ns, ...updates } : ns)));
  };

  const handleRemoveNamespace = (index: number) => {
    setNamespaces(namespaces.filter((_, i) => i !== index));
  };

  const handleSave = () => {
    if (validationErrors.length > 0) {
      // Optionally show a toast – but we already display the error bar
      return;
    }
    const config: ExtractXMLFieldConfiguration = {
      version: '1.0',
      sourceColumn,
      xpathExpressions: expressions,
      namespaceMappings: namespaces,
      errorHandling,
      parallelization: parallel,
      batchSize: parallel ? batchSize : undefined,
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        createdBy: 'canvas-user',
        ruleCount: expressions.length,
        validationStatus: validationErrors.length ? 'ERROR' : 'VALID',
        warnings: [],
        dependencies: [],
      },
    };
    onSave(config);
  };

  return (
    <div className="fixed inset-0 z-[9999] bg-black/70 flex items-center justify-center p-4">
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        className="bg-gray-900 rounded-xl shadow-2xl w-full max-w-6xl max-h-[90vh] flex flex-col text-white"
      >
        {/* Header */}
        <div className="flex justify-between items-center p-4 border-b border-gray-700">
          <div>
            <h2 className="text-xl font-bold flex items-center">
              <span className="mr-2">📄</span>
              XML Field Extractor
            </h2>
            <p className="text-sm text-gray-400">
              Node: <span className="text-blue-400">{nodeName}</span> • Input columns: {inputColumns.length}
            </p>
          </div>
          <button onClick={onClose} className="p-2 hover:bg-gray-800 rounded-full" title="Close">
            ✕
          </button>
        </div>

        {/* Main content - scrollable */}
        <div className="flex-1 overflow-y-auto p-4 grid grid-cols-2 gap-4">
          {/* Left: Input schema */}
          <div className="bg-gray-800 rounded-lg p-4">
            <h3 className="font-semibold mb-2">Input Schema</h3>
            <div className="mb-4">
              <label className="block text-sm text-gray-400 mb-1">Source XML Column</label>
              <select
                value={sourceColumn}
                onChange={(e) => setSourceColumn(e.target.value)}
                className="w-full bg-gray-700 border border-gray-600 rounded px-3 py-2"
              >
                <option value="">Select column...</option>
                {inputColumns.map(col => (
                  <option key={col.name} value={col.name}>
                    {col.name} ({col.type})
                  </option>
                ))}
              </select>
            </div>
            <div className="text-sm text-gray-400">
              <p>Available columns from upstream nodes.</p>
              <p className="mt-2">The selected column must contain valid XML.</p>
            </div>
          </div>

          {/* Right: Output columns table */}
          <div className="bg-gray-800 rounded-lg p-4">
            <div className="flex justify-between items-center mb-2">
              <h3 className="font-semibold">Output Columns</h3>
              <button
                onClick={handleAddExpression}
                className="px-3 py-1 bg-blue-600 hover:bg-blue-700 rounded text-sm"
              >
                + Add Column
              </button>
            </div>
            <div className="overflow-auto max-h-80">
              <table className="w-full text-sm">
                <thead className="bg-gray-700">
                  <tr>
                    <th className="p-2 text-left">Name</th>
                    <th className="p-2 text-left">XPath</th>
                    <th className="p-2 text-left">Type</th>
                    <th className="p-2 text-left">Length</th>
                    <th className="p-2 text-center">Nullable</th>
                    <th className="p-2 text-center">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {expressions.map((expr, _idx) => (
                    <tr key={expr.id} className="border-t border-gray-700">
                      <td className="p-2">
                        <input
                          type="text"
                          value={expr.outputColumn}
                          onChange={(e) =>
                            handleUpdateExpression(expr.id, { outputColumn: e.target.value })
                          }
                          className="w-24 bg-gray-700 border border-gray-600 rounded px-2 py-1"
                        />
                      </td>
                      <td className="p-2">
                        <input
                          type="text"
                          value={expr.xpath}
                          onChange={(e) =>
                            handleUpdateExpression(expr.id, { xpath: e.target.value })
                          }
                          className="w-40 bg-gray-700 border border-gray-600 rounded px-2 py-1"
                          placeholder="/root/element"
                        />
                      </td>
                      <td className="p-2">
                        <select
                          value={expr.dataType}
                          onChange={(e) =>
                            handleUpdateExpression(expr.id, { dataType: e.target.value as any })
                          }
                          className="bg-gray-700 border border-gray-600 rounded px-2 py-1"
                        >
                          <option value="STRING">STRING</option>
                          <option value="INTEGER">INTEGER</option>
                          <option value="DECIMAL">DECIMAL</option>
                          <option value="DATE">DATE</option>
                          <option value="TIMESTAMP">TIMESTAMP</option>
                          <option value="BOOLEAN">BOOLEAN</option>
                        </select>
                      </td>
                      <td className="p-2">
                        <input
                          type="number"
                          value={expr.length || ''}
                          onChange={(e) =>
                            handleUpdateExpression(expr.id, { length: parseInt(e.target.value) || undefined })
                          }
                          className="w-16 bg-gray-700 border border-gray-600 rounded px-2 py-1"
                          placeholder="Len"
                        />
                      </td>
                      <td className="p-2 text-center">
                        <input
                          type="checkbox"
                          checked={expr.nullable}
                          onChange={(e) =>
                            handleUpdateExpression(expr.id, { nullable: e.target.checked })
                          }
                        />
                      </td>
                      <td className="p-2 text-center">
                        <button
                          onClick={() => handleRemoveExpression(expr.id)}
                          className="text-red-500 hover:text-red-400"
                          title="Remove"
                        >
                          🗑️
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* Namespace mappings (full width) */}
          <div className="col-span-2 bg-gray-800 rounded-lg p-4">
            <h3 className="font-semibold mb-2">Namespace Mappings</h3>
            <div className="space-y-2">
              {namespaces.map((ns, idx) => (
                <div key={idx} className="flex gap-2 items-center">
                  <input
                    type="text"
                    value={ns.prefix}
                    onChange={(e) => handleUpdateNamespace(idx, { prefix: e.target.value })}
                    placeholder="Prefix"
                    className="bg-gray-700 border border-gray-600 rounded px-2 py-1 w-24"
                  />
                  <input
                    type="text"
                    value={ns.uri}
                    onChange={(e) => handleUpdateNamespace(idx, { uri: e.target.value })}
                    placeholder="URI"
                    className="bg-gray-700 border border-gray-600 rounded px-2 py-1 flex-1"
                  />
                  <button
                    onClick={() => handleRemoveNamespace(idx)}
                    className="text-red-500 hover:text-red-400"
                  >
                    ✕
                  </button>
                </div>
              ))}
              <button
                onClick={handleAddNamespace}
                className="text-sm text-blue-400 hover:text-blue-300"
              >
                + Add Namespace
              </button>
            </div>
          </div>

          {/* Error handling & performance */}
          <div className="col-span-2 bg-gray-800 rounded-lg p-4">
            <h3 className="font-semibold mb-2">Error Handling & Performance</h3>
            <div className="grid grid-cols-3 gap-4">
              <div>
                <label className="block text-sm text-gray-400 mb-1">On extraction error</label>
                <select
                  value={errorHandling}
                  onChange={(e) => setErrorHandling(e.target.value as any)}
                  className="w-full bg-gray-700 border border-gray-600 rounded px-2 py-1"
                >
                  <option value="fail">Fail job</option>
                  <option value="skipRow">Skip row</option>
                  <option value="setNull">Set NULL and continue</option>
                </select>
              </div>
              <div className="flex items-center">
                <input
                  type="checkbox"
                  checked={parallel}
                  onChange={(e) => setParallel(e.target.checked)}
                  id="parallel"
                  className="mr-2"
                />
                <label htmlFor="parallel">Enable parallel processing</label>
              </div>
              {parallel && (
                <div>
                  <label className="block text-sm text-gray-400 mb-1">Batch size</label>
                  <input
                    type="number"
                    value={batchSize}
                    onChange={(e) => setBatchSize(parseInt(e.target.value) || 1000)}
                    className="w-full bg-gray-700 border border-gray-600 rounded px-2 py-1"
                    min="1"
                  />
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Footer */}
        <div className="border-t border-gray-700 p-4 flex justify-end gap-2">
          <button
            onClick={onClose}
            className="px-4 py-2 border border-gray-600 rounded hover:bg-gray-800"
          >
            Cancel
          </button>
          <button
            onClick={handleSave}
            disabled={validationErrors.length > 0}
            className="px-4 py-2 bg-green-600 hover:bg-green-700 rounded disabled:opacity-50 disabled:cursor-not-allowed"
          >
            Save & Compile
          </button>
        </div>

        {/* Validation errors bar */}
        {validationErrors.length > 0 && (
          <div className="bg-red-900/50 border-t border-red-700 p-2 text-sm text-red-200">
            {validationErrors.map((err, i) => (
              <div key={i}>⚠️ {err}</div>
            ))}
          </div>
        )}
      </motion.div>
    </div>
  );
};

export default ExtractXMLFieldEditor;