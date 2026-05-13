import React, { useState, useCallback, useMemo } from 'react';
import { motion } from 'framer-motion';
import { SimpleColumn } from './MapEditor'; // or define locally
import { DataType, NormalizeNumberComponentConfiguration, NormalizeNumberRule, PostgreSQLDataType } from '../../../types/unified-pipeline.types';

interface NormalizeNumberEditorProps {
  nodeId: string;
  nodeMetadata: any;
  inputColumns: SimpleColumn[];
  initialConfig?: NormalizeNumberComponentConfiguration;
  onClose: () => void;
  onSave: (config: NormalizeNumberComponentConfiguration) => void;
}

// Helper to generate unique IDs
const generateId = () => `rule-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

const NormalizeNumberEditor: React.FC<NormalizeNumberEditorProps> = ({
  nodeId,
  nodeMetadata,
  inputColumns,
  initialConfig,
  onClose,
  onSave
}) => {
  // State for selected columns (by name)
  const [selectedColumns, setSelectedColumns] = useState<Set<string>>(() => {
    if (initialConfig?.rules) {
      return new Set(initialConfig.rules.map(r => r.sourceColumn));
    }
    return new Set();
  });

  // State for rules (keyed by source column)
  const [rules, setRules] = useState<Map<string, NormalizeNumberRule>>(() => {
    const map = new Map<string, NormalizeNumberRule>();
    if (initialConfig?.rules) {
      initialConfig.rules.forEach(rule => {
        map.set(rule.sourceColumn, rule);
      });
    }
    return map;
  });

  // Global options
  const [globalNullHandling, setGlobalNullHandling] = useState<'KEEP_NULL' | 'DEFAULT_VALUE' | 'ERROR'>(
    initialConfig?.globalOptions?.nullHandling || 'KEEP_NULL'
  );
  const [globalOutlierHandling, setGlobalOutlierHandling] = useState<'CLIP' | 'REMOVE' | 'NONE'>(
    initialConfig?.globalOptions?.outlierHandling || 'NONE'
  );
  const [globalDefaultDataType] = useState<PostgreSQLDataType>(
    initialConfig?.globalOptions?.defaultDataType || PostgreSQLDataType.DOUBLE_PRECISION
  );

  // Output naming strategy
  const [outputNaming, setOutputNaming] = useState<'replace' | 'suffix' | 'prefix'>('suffix');
  const [nameSuffix, setNameSuffix] = useState('_norm');
  const [namePrefix, setNamePrefix] = useState('norm_');

  // Preview data (mock – would come from actual data in real implementation)
  const [previewRows] = useState<any[]>([
    { sales: 120.5, quantity: 45, discount: 0.15 },
    { sales: 45.0, quantity: 12, discount: 0.05 },
    { sales: 230.0, quantity: 78, discount: 0.25 },
    { sales: 67.8, quantity: 23, discount: 0.10 },
    { sales: 189.2, quantity: 56, discount: 0.20 },
  ]);

  // Method configuration for each selected column (if rules exist)
  const getRuleForColumn = useCallback((colName: string): NormalizeNumberRule | undefined => {
    return rules.get(colName);
  }, [rules]);

  const updateRule = useCallback((colName: string, updates: Partial<NormalizeNumberRule>) => {
    setRules(prev => {
      const newMap = new Map(prev);
      const existing = newMap.get(colName);
      if (existing) {
        newMap.set(colName, { ...existing, ...updates });
      } else {
        // Create new rule with defaults
        const newRule: NormalizeNumberRule = {
          id: generateId(),
          sourceColumn: colName,
          targetColumn: colName, // will be adjusted later based on naming
          method: 'minmax',
          parameters: { min: 0, max: 1 },
          nullHandling: globalNullHandling,
          outlierHandling: globalOutlierHandling,
          outputDataType: globalDefaultDataType,
          position: newMap.size,
        };
        newMap.set(colName, { ...newRule, ...updates });
      }
      return newMap;
    });
  }, [globalNullHandling, globalOutlierHandling, globalDefaultDataType]);

  const removeRule = useCallback((colName: string) => {
    setRules(prev => {
      const newMap = new Map(prev);
      newMap.delete(colName);
      // Reassign positions
      let i = 0;
      newMap.forEach(rule => { rule.position = i++; });
      return newMap;
    });
  }, []);

  // Handle column selection toggle
  const toggleColumn = useCallback((colName: string) => {
    setSelectedColumns(prev => {
      const newSet = new Set(prev);
      if (newSet.has(colName)) {
        newSet.delete(colName);
        // Remove rule
        removeRule(colName);
      } else {
        newSet.add(colName);
        // Add rule with defaults
        updateRule(colName, {});
      }
      return newSet;
    });
  }, [removeRule, updateRule]);

  const selectAll = useCallback(() => {
    inputColumns.forEach(col => {
      if (!selectedColumns.has(col.name)) {
        toggleColumn(col.name);
      }
    });
  }, [inputColumns, selectedColumns, toggleColumn]);

  const clearAll = useCallback(() => {
    inputColumns.forEach(col => {
      if (selectedColumns.has(col.name)) {
        toggleColumn(col.name);
      }
    });
  }, [inputColumns, selectedColumns, toggleColumn]);

  // Generate final rules with target column names based on naming strategy
  const generateFinalRules = useCallback((): NormalizeNumberRule[] => {
    const finalRules: NormalizeNumberRule[] = [];
    rules.forEach(rule => {
      let targetColumn = rule.sourceColumn;
      if (outputNaming === 'suffix') {
        targetColumn = rule.sourceColumn + nameSuffix;
      } else if (outputNaming === 'prefix') {
        targetColumn = namePrefix + rule.sourceColumn;
      }
      // Use global settings if rule doesn't override
      finalRules.push({
        ...rule,
        targetColumn,
        nullHandling: rule.nullHandling || globalNullHandling,
        outlierHandling: rule.outlierHandling || globalOutlierHandling,
        outputDataType: rule.outputDataType || globalDefaultDataType,
      });
    });
    return finalRules.sort((a, b) => a.position - b.position);
  }, [rules, outputNaming, nameSuffix, namePrefix, globalNullHandling, globalOutlierHandling, globalDefaultDataType]);

  // Validation
  const validation = useMemo(() => {
    const warnings: string[] = [];
    const finalRules = generateFinalRules();
    if (finalRules.length === 0) {
      warnings.push('No columns selected for normalization.');
    }
    finalRules.forEach(rule => {
      if (rule.method === 'minmax') {
        const min = rule.parameters?.min;
        const max = rule.parameters?.max;
        if (min === undefined || max === undefined || min >= max) {
          warnings.push(`Column "${rule.sourceColumn}": Min‑Max requires min < max.`);
        }
      }
      if (rule.method === 'custom' && !rule.parameters?.expression) {
        warnings.push(`Column "${rule.sourceColumn}": Custom expression is empty.`);
      }
    });
    return {
      isValid: warnings.length === 0,
      warnings,
    };
  }, [generateFinalRules]);

  // Save handler
  const handleSave = () => {
    const finalRules = generateFinalRules();
    const config: NormalizeNumberComponentConfiguration = {
      version: '1.0',
      rules: finalRules,
      globalOptions: {
        nullHandling: globalNullHandling,
        outlierHandling: globalOutlierHandling,
        defaultDataType: globalDefaultDataType,
      },
      outputSchema: {
        id: `${nodeId}_output_schema`,
        name: 'Normalized Output',
        alias: '',
        fields: finalRules.map(rule => ({
          id: rule.id,
          name: rule.targetColumn,
          type: mapPostgresToDataType(rule.outputDataType), // helper to map PostgreSQLDataType to DataType
          nullable: rule.nullHandling !== 'ERROR',
          isKey: false,
          description: `Normalized from ${rule.sourceColumn} using ${rule.method}`,
        })),
        isTemporary: false,
        isMaterialized: false,
        metadata: {},
      },
      sqlGeneration: {
        requiresCustomExpression: finalRules.some(r => r.method === 'custom'),
        estimatedRowMultiplier: 1.0,
        parallelizable: true,
      },
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        createdBy: 'user',
        ruleCount: finalRules.length,
        validationStatus: validation.isValid ? 'VALID' : 'WARNING',
        warnings: validation.warnings,
        dependencies: finalRules.map(r => r.sourceColumn),
        compiledSql: undefined,
      },
    };
    onSave(config);
  };

  // Helper to map PostgreSQLDataType to DataType (simplified)
  const mapPostgresToDataType = (pgType: PostgreSQLDataType): DataType => {
    const str = pgType.toString().toUpperCase();
    if (str.includes('INT') || str.includes('SERIAL')) return 'INTEGER';
    if (str.includes('DECIMAL') || str.includes('NUMERIC') || str.includes('FLOAT') || str.includes('DOUBLE')) return 'DECIMAL';
    if (str.includes('BOOL')) return 'BOOLEAN';
    if (str.includes('DATE')) return 'DATE';
    if (str.includes('TIME')) return 'TIMESTAMP';
    return 'STRING';
  };

  // Render method parameters for a given rule
  const renderMethodParams = (rule: NormalizeNumberRule) => {
    switch (rule.method) {
      case 'minmax':
        return (
          <div className="flex space-x-2">
            <div>
              <label className="block text-xs text-gray-600">Min</label>
              <input
                type="number"
                value={rule.parameters?.min ?? 0}
                onChange={(e) => updateRule(rule.sourceColumn, { parameters: { ...rule.parameters, min: parseFloat(e.target.value) } })}
                className="w-20 px-2 py-1 text-sm border rounded"
              />
            </div>
            <div>
              <label className="block text-xs text-gray-600">Max</label>
              <input
                type="number"
                value={rule.parameters?.max ?? 1}
                onChange={(e) => updateRule(rule.sourceColumn, { parameters: { ...rule.parameters, max: parseFloat(e.target.value) } })}
                className="w-20 px-2 py-1 text-sm border rounded"
              />
            </div>
          </div>
        );
      case 'zscore':
        return <div className="text-sm text-gray-500">No parameters needed</div>;
      case 'decimalscaling':
        return <div className="text-sm text-gray-500">No parameters needed</div>;
      case 'log':
        return (
          <div>
            <label className="block text-xs text-gray-600">Base</label>
            <select
              value={rule.parameters?.logBase || 'e'}
              onChange={(e) => updateRule(rule.sourceColumn, { parameters: { ...rule.parameters, logBase: e.target.value as 'e' | '10' } })}
              className="px-2 py-1 text-sm border rounded"
            >
              <option value="e">Natural (e)</option>
              <option value="10">Base 10</option>
            </select>
          </div>
        );
      case 'robust':
        return <div className="text-sm text-gray-500">No parameters needed</div>;
      case 'round':
        return (
          <div className="flex space-x-2">
            <div>
              <label className="block text-xs text-gray-600">Decimals</label>
              <input
                type="number"
                min="0"
                step="1"
                value={rule.parameters?.decimalPlaces ?? 0}
                onChange={(e) => updateRule(rule.sourceColumn, { parameters: { ...rule.parameters, decimalPlaces: parseInt(e.target.value) } })}
                className="w-16 px-2 py-1 text-sm border rounded"
              />
            </div>
            <div>
              <label className="block text-xs text-gray-600">Mode</label>
              <select
                value={rule.parameters?.roundingMode || 'round'}
                onChange={(e) => updateRule(rule.sourceColumn, { parameters: { ...rule.parameters, roundingMode: e.target.value as any } })}
                className="px-2 py-1 text-sm border rounded"
              >
                <option value="round">Round</option>
                <option value="ceil">Ceil</option>
                <option value="floor">Floor</option>
              </select>
            </div>
          </div>
        );
      case 'custom':
        return (
          <div>
            <label className="block text-xs text-gray-600">Expression</label>
            <textarea
              value={rule.parameters?.expression || ''}
              onChange={(e) => updateRule(rule.sourceColumn, { parameters: { ...rule.parameters, expression: e.target.value } })}
              className="w-full px-2 py-1 text-sm font-mono border rounded h-20"
              placeholder="e.g., ({column} - 0.5) * 2"
            />
            <p className="text-xs text-gray-500 mt-1">Use {'{column}'} to reference the source column.</p>
          </div>
        );
      default:
        return null;
    }
  };

  return (
    <div className="fixed inset-0 z-[9999] bg-gray-900/80 flex items-center justify-center p-4">
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        className="bg-white rounded-xl shadow-2xl w-full max-w-6xl h-[90vh] flex flex-col overflow-hidden"
      >
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b bg-gradient-to-r from-indigo-50 to-gray-50">
          <div>
            <h2 className="text-xl font-bold flex items-center">
              <span className="mr-2">🔢</span>
              Normalize Number
              <span className="ml-3 text-xs bg-indigo-100 text-indigo-800 px-2 py-0.5 rounded">
                tNormalizeNumber
              </span>
            </h2>
            <p className="text-sm text-gray-600 mt-1">
              Node: <span className="font-semibold">{nodeMetadata?.name || nodeId}</span>
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

        {/* Main Content */}
        <div className="flex-1 flex overflow-hidden">
          {/* Left Panel: Column Selection */}
          <div className="w-1/4 border-r bg-gray-50 overflow-auto">
            <div className="p-4">
              <h3 className="font-semibold text-gray-700 mb-2">Input Columns</h3>
              <div className="mb-3 flex space-x-2">
                <button onClick={selectAll} className="text-xs px-2 py-1 bg-blue-100 text-blue-700 rounded">Select All</button>
                <button onClick={clearAll} className="text-xs px-2 py-1 bg-gray-200 text-gray-700 rounded">Clear All</button>
              </div>
              <div className="space-y-1">
                {inputColumns.map(col => (
                  <label key={col.name} className="flex items-center space-x-2 p-1 hover:bg-gray-100 rounded cursor-pointer">
                    <input
                      type="checkbox"
                      checked={selectedColumns.has(col.name)}
                      onChange={() => toggleColumn(col.name)}
                      className="rounded"
                    />
                    <span className="text-sm">{col.name}</span>
                    <span className="text-xs text-gray-500 ml-auto">{col.type}</span>
                  </label>
                ))}
              </div>
            </div>
          </div>

          {/* Middle Panel: Method Configuration */}
          <div className="w-2/4 border-r bg-white overflow-auto p-4">
            <h3 className="font-semibold text-gray-700 mb-3">Normalization Methods</h3>
            {selectedColumns.size === 0 ? (
              <div className="text-center text-gray-500 py-10">Select columns to configure normalization.</div>
            ) : (
              <div className="space-y-4">
                {Array.from(selectedColumns).map(colName => {
                  const rule = getRuleForColumn(colName) || {
                    id: '',
                    sourceColumn: colName,
                    targetColumn: colName,
                    method: 'minmax',
                    parameters: { min: 0, max: 1 },
                    nullHandling: globalNullHandling,
                    outlierHandling: globalOutlierHandling,
                    outputDataType: globalDefaultDataType,
                    position: 0,
                  };
                  return (
                    <div key={colName} className="border rounded-lg p-3 bg-gray-50">
                      <div className="flex items-center justify-between mb-2">
                        <span className="font-medium">{colName}</span>
                        <button onClick={() => removeRule(colName)} className="text-xs text-red-500 hover:text-red-700">Remove</button>
                      </div>
                      <div className="grid grid-cols-2 gap-3">
                        <div>
                          <label className="block text-xs text-gray-600">Method</label>
                          <select
                            value={rule.method}
                            onChange={(e) => updateRule(colName, { method: e.target.value as any })}
                            className="w-full px-2 py-1 text-sm border rounded"
                          >
                            <option value="minmax">Min‑Max</option>
                            <option value="zscore">Z‑Score</option>
                            <option value="decimalscaling">Decimal Scaling</option>
                            <option value="log">Logarithm</option>
                            <option value="robust">Robust Scaling</option>
                            <option value="round">Round / Ceil / Floor</option>
                            <option value="custom">Custom Expression</option>
                          </select>
                        </div>
                        <div>
                          <label className="block text-xs text-gray-600">Output Type</label>
                          <select
                            value={rule.outputDataType}
                            onChange={(e) => updateRule(colName, { outputDataType: e.target.value as PostgreSQLDataType })}
                            className="w-full px-2 py-1 text-sm border rounded"
                          >
                            <option value="DOUBLE PRECISION">DOUBLE PRECISION</option>
                            <option value="NUMERIC">NUMERIC</option>
                            <option value="REAL">REAL</option>
                            <option value="INTEGER">INTEGER</option>
                          </select>
                        </div>
                      </div>
                      <div className="mt-2">
                        {renderMethodParams(rule)}
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </div>

          {/* Right Panel: Preview & Global Settings */}
          <div className="w-1/4 bg-white overflow-auto p-4">
            <h3 className="font-semibold text-gray-700 mb-3">Global Settings</h3>
            <div className="space-y-3">
              <div>
                <label className="block text-xs text-gray-600">Null Handling</label>
                <select
                  value={globalNullHandling}
                  onChange={(e) => setGlobalNullHandling(e.target.value as any)}
                  className="w-full px-2 py-1 text-sm border rounded"
                >
                  <option value="KEEP_NULL">Keep NULL</option>
                  <option value="DEFAULT_VALUE">Replace with default</option>
                  <option value="ERROR">Error on NULL</option>
                </select>
              </div>
              {globalNullHandling === 'DEFAULT_VALUE' && (
                <div>
                  <label className="block text-xs text-gray-600">Default Value</label>
                  <input type="number" className="w-full px-2 py-1 text-sm border rounded" placeholder="0" />
                </div>
              )}
              <div>
                <label className="block text-xs text-gray-600">Outlier Handling</label>
                <select
                  value={globalOutlierHandling}
                  onChange={(e) => setGlobalOutlierHandling(e.target.value as any)}
                  className="w-full px-2 py-1 text-sm border rounded"
                >
                  <option value="NONE">None</option>
                  <option value="CLIP">Clip to range</option>
                  <option value="REMOVE">Remove rows</option>
                </select>
              </div>
              <div>
                <label className="block text-xs text-gray-600">Output Naming</label>
                <select
                  value={outputNaming}
                  onChange={(e) => setOutputNaming(e.target.value as any)}
                  className="w-full px-2 py-1 text-sm border rounded"
                >
                  <option value="replace">Replace original</option>
                  <option value="suffix">Add suffix</option>
                  <option value="prefix">Add prefix</option>
                </select>
              </div>
              {outputNaming === 'suffix' && (
                <div>
                  <label className="block text-xs text-gray-600">Suffix</label>
                  <input
                    type="text"
                    value={nameSuffix}
                    onChange={(e) => setNameSuffix(e.target.value)}
                    className="w-full px-2 py-1 text-sm border rounded"
                  />
                </div>
              )}
              {outputNaming === 'prefix' && (
                <div>
                  <label className="block text-xs text-gray-600">Prefix</label>
                  <input
                    type="text"
                    value={namePrefix}
                    onChange={(e) => setNamePrefix(e.target.value)}
                    className="w-full px-2 py-1 text-sm border rounded"
                  />
                </div>
              )}

              <h3 className="font-semibold text-gray-700 mt-4 mb-2">Preview</h3>
              <div className="text-xs border rounded overflow-hidden">
                <table className="w-full">
                  <thead className="bg-gray-100">
                    <tr>
                      {Array.from(selectedColumns).slice(0, 3).map(col => (
                        <th key={col} className="px-2 py-1 text-left">{col}</th>
                      ))}
                      <th className="px-2 py-1 text-left">→</th>
                    </tr>
                  </thead>
                  <tbody>
                    {previewRows.slice(0, 5).map((row, idx) => (
                      <tr key={idx} className="border-t">
                        {Array.from(selectedColumns).slice(0, 3).map(col => (
                          <td key={col} className="px-2 py-1">{row[col]?.toFixed(2)}</td>
                        ))}
                        <td className="px-2 py-1 text-green-600">✓</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <p className="text-xs text-gray-500 mt-1">Preview shows first 5 rows (mock data).</p>
            </div>
          </div>
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between px-6 py-3 border-t bg-gray-50">
          <div className="flex items-center space-x-2">
            {!validation.isValid && (
              <div className="text-xs text-yellow-700 bg-yellow-100 px-2 py-1 rounded">
                ⚠️ {validation.warnings.length} warning(s)
              </div>
            )}
          </div>
          <div className="flex space-x-2">
            <button onClick={onClose} className="px-4 py-2 text-sm border border-gray-300 rounded hover:bg-gray-50">
              Cancel
            </button>
            <button
              onClick={handleSave}
              disabled={!validation.isValid}
              className={`px-4 py-2 text-sm rounded transition-colors ${
                validation.isValid
                  ? 'bg-gradient-to-r from-indigo-500 to-indigo-600 text-white hover:from-indigo-600 hover:to-indigo-700'
                  : 'bg-gray-300 text-gray-500 cursor-not-allowed'
              }`}
            >
              Save Configuration
            </button>
          </div>
        </div>
      </motion.div>
    </div>
  );
};

export default NormalizeNumberEditor;