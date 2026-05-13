// src/components/Editor/AggregateEditor.tsx
import React, { useState, useMemo, useEffect } from 'react';
import { motion } from 'framer-motion';
import {
  X,
  Plus,
  Trash2,
  AlertCircle,
  CheckCircle,
  Hash,
  Type,
  Calendar,
  ToggleLeft,
  ArrowUpDown,
  FunctionSquare,
  Filter,
  Save,
  XCircle,
} from 'lucide-react';
import {
  AggregateComponentConfiguration,
  FieldSchema,
  DataType,
} from '../../../types/unified-pipeline.types';

// ----------------------------------------------------------------------
// Types
// ----------------------------------------------------------------------

export interface SimpleColumn {
  name: string;
  type?: string;
  id?: string;
}

export interface AggregateEditorProps {
  nodeId: string;
  nodeMetadata?: any;
  inputColumns: SimpleColumn[];
  initialConfig?: AggregateComponentConfiguration;
  onClose: () => void;
  onSave: (config: AggregateComponentConfiguration) => void;
}

// Local state for an aggregation row
interface AggregationRow {
  id: string;
  function: 'SUM' | 'COUNT' | 'AVG' | 'MIN' | 'MAX' | 'COUNT_DISTINCT' | 'STDDEV';
  field: string;          // name of input column
  alias: string;
  distinct: boolean;
}

// Local state for a HAVING condition
interface HavingCondition {
  id: string;
  field: string;          // name of output column (aggregate or group by)
  operator: '=' | '!=' | '<' | '>' | '<=' | '>=';
  value: string | number;
}

// ----------------------------------------------------------------------
// Helper functions
// ----------------------------------------------------------------------

const generateId = (prefix: string = 'agg') => `${prefix}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

const mapStringToDataType = (type: string): DataType => {
  const upper = type.toUpperCase();
  if (upper.includes('INT')) return 'INTEGER';
  if (upper.includes('DECIMAL') || upper.includes('NUMERIC') || upper.includes('FLOAT')) return 'DECIMAL';
  if (upper.includes('BOOL')) return 'BOOLEAN';
  if (upper.includes('DATE')) return 'DATE';
  if (upper.includes('TIME')) return 'TIMESTAMP';
  return 'STRING';
};

const getAggregateReturnType = (func: string, inputType: DataType): DataType => {
  switch (func) {
    case 'COUNT':
    case 'COUNT_DISTINCT':
      return 'INTEGER';
    case 'SUM':
    case 'AVG':
      if (inputType === 'INTEGER') return 'DECIMAL'; // SUM/AVG of integers can be decimal
      if (inputType === 'DECIMAL') return 'DECIMAL';
      return 'DECIMAL'; // fallback
    case 'MIN':
    case 'MAX':
      return inputType; // same as input
    case 'STDDEV':
      return 'DECIMAL';
    default:
      return 'STRING';
  }
};

// ----------------------------------------------------------------------
// Main Component
// ----------------------------------------------------------------------

export const AggregateEditor: React.FC<AggregateEditorProps> = ({
  nodeId,
  nodeMetadata,
  inputColumns,
  initialConfig,
  onClose,
  onSave,
}) => {
  // --------------------------------------------------------------------
  // State
  // --------------------------------------------------------------------
  const [groupByFields, setGroupByFields] = useState<string[]>(() => {
    if (initialConfig) return [...initialConfig.groupByFields];
    return [];
  });

  const [aggregations, setAggregations] = useState<AggregationRow[]>(() => {
    if (initialConfig && initialConfig.aggregateFunctions.length > 0) {
      return initialConfig.aggregateFunctions.map((af) => ({
        id: af.id,
        function: af.function,
        field: af.field,
        alias: af.alias,
        distinct: af.distinct,
      }));
    }
    // start with one empty row
    return [
      {
        id: generateId('agg'),
        function: 'COUNT',
        field: '',
        alias: '',
        distinct: false,
      },
    ];
  });

  const [havingConditions, setHavingConditions] = useState<HavingCondition[]>(() => {
    if (initialConfig && initialConfig.havingConditions) {
      return initialConfig.havingConditions.map((hc) => ({
        id: hc.id,
        field: hc.field,
        operator: hc.operator,
        value: hc.value,
      }));
    }
    return [];
  });

  // Derived: list of output columns (group by + aggregates)
  const outputColumns = useMemo(() => {
    const columns: FieldSchema[] = [];

    // Group by columns – keep original type
    groupByFields.forEach((fieldName, idx) => {
      const inputCol = inputColumns.find((c) => c.name === fieldName);
      columns.push({
        id: `groupby-${idx}-${fieldName}`,
        name: fieldName,
        type: inputCol ? mapStringToDataType(inputCol.type || 'STRING') : 'STRING',
        nullable: false,
        isKey: true,
        description: `Group by ${fieldName}`,
      });
    });

    // Aggregate columns
    aggregations.forEach((agg, idx) => {
      const inputCol = inputColumns.find((c) => c.name === agg.field);
      const inputType = inputCol ? mapStringToDataType(inputCol.type || 'STRING') : 'STRING';
      const returnType = getAggregateReturnType(agg.function, inputType);
      const alias = agg.alias || `${agg.function}_${agg.field}`;
      columns.push({
        id: `agg-${idx}-${alias}`,
        name: alias,
        type: returnType,
        nullable: true,
        isKey: false,
        description: `${agg.function} of ${agg.field}`,
      });
    });

    return columns;
  }, [groupByFields, aggregations, inputColumns]);

  // Validation messages
  const [warnings, setWarnings] = useState<string[]>([]);
  const [errors, setErrors] = useState<string[]>([]);

  // --------------------------------------------------------------------
  // Validation
  // --------------------------------------------------------------------
  useEffect(() => {
    const newErrors: string[] = [];
    const newWarnings: string[] = [];

    // At least one group by or one aggregation? (Aggregation without group by is allowed – single row)
    if (groupByFields.length === 0 && aggregations.length === 0) {
      newErrors.push('Specify at least one group‑by column or one aggregation.');
    }

    // Aggregations must have a field selected and an alias
    aggregations.forEach((agg, idx) => {
      if (!agg.field) {
        newErrors.push(`Aggregation #${idx + 1}: field is required.`);
      }
      if (!agg.alias) {
        newWarnings.push(`Aggregation #${idx + 1}: using default alias.`);
      }
    });

    // Check for duplicate output column names
    const outputNames = outputColumns.map((c) => c.name);
    const duplicates = outputNames.filter((name, i) => outputNames.indexOf(name) !== i);
    if (duplicates.length > 0) {
      newErrors.push(`Duplicate output column names: ${duplicates.join(', ')}.`);
    }

    // Having conditions must reference existing output columns
    havingConditions.forEach((hc, idx) => {
      if (!outputNames.includes(hc.field)) {
        newErrors.push(`HAVING condition #${idx + 1}: column "${hc.field}" does not exist in output.`);
      }
    });

    setErrors(newErrors);
    setWarnings(newWarnings);
  }, [groupByFields, aggregations, havingConditions, outputColumns]);

  // --------------------------------------------------------------------
  // Handlers
  // --------------------------------------------------------------------

  const toggleGroupBy = (fieldName: string) => {
    setGroupByFields((prev) =>
      prev.includes(fieldName) ? prev.filter((f) => f !== fieldName) : [...prev, fieldName]
    );
  };

  const addAggregation = () => {
    setAggregations((prev) => [
      ...prev,
      {
        id: generateId('agg'),
        function: 'COUNT',
        field: '',
        alias: '',
        distinct: false,
      },
    ]);
  };

  const removeAggregation = (id: string) => {
    if (aggregations.length > 1) {
      setAggregations((prev) => prev.filter((a) => a.id !== id));
    }
  };

  const updateAggregation = (id: string, updates: Partial<AggregationRow>) => {
    setAggregations((prev) =>
      prev.map((a) => (a.id === id ? { ...a, ...updates } : a))
    );
  };

  const addHavingCondition = () => {
    setHavingConditions((prev) => [
      ...prev,
      {
        id: generateId('having'),
        field: '',
        operator: '=',
        value: '',
      },
    ]);
  };

  const removeHavingCondition = (id: string) => {
    setHavingConditions((prev) => prev.filter((h) => h.id !== id));
  };

  const updateHavingCondition = (id: string, updates: Partial<HavingCondition>) => {
    setHavingConditions((prev) =>
      prev.map((h) => (h.id === id ? { ...h, ...updates } : h))
    );
  };

  const handleSave = () => {
    if (errors.length > 0) {
      // Optionally show a toast or alert
      return;
    }

    // Build aggregateFunctions array
    const aggregateFunctions = aggregations.map((agg) => ({
      id: agg.id,
      function: agg.function,
      field: agg.field,
      alias: agg.alias || `${agg.function}_${agg.field}`,
      distinct: agg.distinct,
    }));

    // Build havingConditions array (if any)
    const having = havingConditions.length > 0
      ? havingConditions.map((hc) => ({
          id: hc.id,
          field: hc.field,
          operator: hc.operator,
          value: hc.value,
        }))
      : undefined;

    // Build output schema fields
    const fields: FieldSchema[] = outputColumns.map((col) => ({
      ...col,
      id: col.id,
      name: col.name,
      type: col.type,
      nullable: col.nullable,
      isKey: col.isKey,
    }));

    const config: AggregateComponentConfiguration = {
      version: '1.0',
      groupByFields,
      aggregateFunctions,
      havingConditions: having,
      optimization: {
        canUseIndex: true, // can be derived later
        requiresSort: groupByFields.length > 0,
        estimatedGroupCount: 1000, // placeholder
        memoryHint: 'MEDIUM',
      },
      outputSchema: {
        fields,
        groupByFields,
        aggregateFields: aggregateFunctions.map((af) => af.alias),
      },
      sqlGeneration: {
        groupByClause: groupByFields.join(', '),
        aggregateClause: aggregateFunctions
          .map((af) => `${af.distinct ? 'DISTINCT ' : ''}${af.function}(${af.field}) AS ${af.alias}`)
          .join(', '),
        havingClause: havingConditions
          .map((hc) => `${hc.field} ${hc.operator} ${typeof hc.value === 'string' ? `'${hc.value}'` : hc.value}`)
          .join(' AND '),
        requiresWindowFunction: false,
        parallelizable: true,
        sortRequired: groupByFields.length > 0,
      },
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        estimatedCardinality: 1000,
        warnings: warnings.length > 0 ? warnings : undefined,
      },
    };

    onSave(config);
  };

  // --------------------------------------------------------------------
  // Render helpers
  // --------------------------------------------------------------------

  const renderDataTypeIcon = (type: DataType) => {
    switch (type) {
      case 'INTEGER':
      case 'DECIMAL':
        return <Hash className="h-3 w-3 text-blue-500" />;
      case 'BOOLEAN':
        return <ToggleLeft className="h-3 w-3 text-amber-500" />;
      case 'DATE':
      case 'TIMESTAMP':
        return <Calendar className="h-3 w-3 text-purple-500" />;
      default:
        return <Type className="h-3 w-3 text-gray-500" />;
    }
  };

  // --------------------------------------------------------------------
  // JSX
  // --------------------------------------------------------------------

  return (
    <div className="fixed inset-0 z-[9999] bg-gray-900/80 flex items-center justify-center p-4">
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        className="bg-white dark:bg-gray-800 rounded-xl shadow-2xl w-full max-w-6xl max-h-[90vh] overflow-hidden flex flex-col"
      >
        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b bg-gradient-to-r from-blue-50 to-gray-50 dark:from-gray-700 dark:to-gray-800">
          <div className="flex items-center space-x-3">
            <FunctionSquare className="h-6 w-6 text-blue-600" />
            <div>
              <h2 className="text-xl font-bold text-gray-900 dark:text-white">Aggregate Configuration</h2>
              <p className="text-sm text-gray-600 dark:text-gray-300">
                Node: {nodeMetadata?.name || nodeId} • {inputColumns.length} input columns
              </p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-2 hover:bg-gray-200 dark:hover:bg-gray-700 rounded-full transition-colors"
          >
            <X className="h-5 w-5 text-gray-600 dark:text-gray-300" />
          </button>
        </div>

        {/* Main content (scrollable) */}
        <div className="flex-1 overflow-y-auto p-6">
          <div className="grid grid-cols-3 gap-6">
            {/* Left: Input Schema */}
            <div className="col-span-1 border rounded-lg bg-gray-50 dark:bg-gray-900 p-4">
              <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-200 mb-3 flex items-center">
                <ArrowUpDown className="h-4 w-4 mr-2" /> Input Columns
              </h3>
              <div className="space-y-2 max-h-96 overflow-y-auto">
                {inputColumns.map((col) => {
                  const dataType = mapStringToDataType(col.type || 'STRING');
                  const isGrouped = groupByFields.includes(col.name);
                  return (
                    <div
                      key={col.name}
                      className={`flex items-center justify-between p-2 rounded border cursor-pointer transition-colors ${
                        isGrouped
                          ? 'bg-blue-100 border-blue-300 dark:bg-blue-900/30 dark:border-blue-700'
                          : 'hover:bg-gray-100 dark:hover:bg-gray-800 border-gray-200 dark:border-gray-700'
                      }`}
                      onClick={() => toggleGroupBy(col.name)}
                    >
                      <div className="flex items-center space-x-2">
                        {renderDataTypeIcon(dataType)}
                        <span className="text-sm font-medium text-gray-800 dark:text-gray-200">{col.name}</span>
                      </div>
                      <div className="flex items-center space-x-2">
                        <span className="text-xs text-gray-500 dark:text-gray-400">{col.type || dataType}</span>
                        {isGrouped && <CheckCircle className="h-4 w-4 text-blue-600" />}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>

            {/* Middle & Right: Group by, Aggregations, Having */}
            <div className="col-span-2 space-y-6">
              {/* Group By */}
              <div className="border rounded-lg p-4">
                <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-200 mb-3">Group By Columns</h3>
                {groupByFields.length === 0 ? (
                  <p className="text-sm text-gray-500 italic">No columns selected – full aggregation (single row).</p>
                ) : (
                  <div className="flex flex-wrap gap-2">
                    {groupByFields.map((field) => (
                      <span
                        key={field}
                        className="inline-flex items-center px-3 py-1 rounded-full text-xs font-medium bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200"
                      >
                        {field}
                        <button
                          onClick={() => toggleGroupBy(field)}
                          className="ml-2 text-blue-600 hover:text-blue-800 dark:text-blue-300 dark:hover:text-blue-100"
                        >
                          <XCircle className="h-3 w-3" />
                        </button>
                      </span>
                    ))}
                  </div>
                )}
              </div>

              {/* Aggregations */}
              <div className="border rounded-lg p-4">
                <div className="flex items-center justify-between mb-3">
                  <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-200">Aggregations</h3>
                  <button
                    onClick={addAggregation}
                    className="px-3 py-1 text-xs bg-blue-600 text-white rounded hover:bg-blue-700 flex items-center"
                  >
                    <Plus className="h-3 w-3 mr-1" /> Add Aggregation
                  </button>
                </div>
                <div className="space-y-3">
                  {aggregations.map((agg, _idx) => (
                    <div key={agg.id} className="grid grid-cols-12 gap-2 items-center p-2 bg-gray-50 dark:bg-gray-900 rounded">
                      {/* Function */}
                      <select
                        value={agg.function}
                        onChange={(e) => updateAggregation(agg.id, { function: e.target.value as any })}
                        className="col-span-2 p-1 text-xs border rounded bg-white dark:bg-gray-800"
                      >
                        <option value="SUM">SUM</option>
                        <option value="COUNT">COUNT</option>
                        <option value="AVG">AVG</option>
                        <option value="MIN">MIN</option>
                        <option value="MAX">MAX</option>
                        <option value="COUNT_DISTINCT">COUNT DISTINCT</option>
                        <option value="STDDEV">STDDEV</option>
                      </select>
                      {/* Field */}
                      <select
                        value={agg.field}
                        onChange={(e) => updateAggregation(agg.id, { field: e.target.value })}
                        className="col-span-3 p-1 text-xs border rounded bg-white dark:bg-gray-800"
                      >
                        <option value="">Select field</option>
                        {inputColumns.map((col) => (
                          <option key={col.name} value={col.name}>
                            {col.name}
                          </option>
                        ))}
                      </select>
                      {/* Alias */}
                      <input
                        type="text"
                        value={agg.alias}
                        onChange={(e) => updateAggregation(agg.id, { alias: e.target.value })}
                        placeholder="Alias"
                        className="col-span-3 p-1 text-xs border rounded bg-white dark:bg-gray-800"
                      />
                      {/* Distinct */}
                      <label className="col-span-2 flex items-center text-xs space-x-1">
                        <input
                          type="checkbox"
                          checked={agg.distinct}
                          onChange={(e) => updateAggregation(agg.id, { distinct: e.target.checked })}
                          className="rounded"
                        />
                        <span>Distinct</span>
                      </label>
                      {/* Remove */}
                      <button
                        onClick={() => removeAggregation(agg.id)}
                        className="col-span-1 p-1 text-red-600 hover:bg-red-50 rounded"
                        disabled={aggregations.length === 1}
                      >
                        <Trash2 className="h-3 w-3" />
                      </button>
                    </div>
                  ))}
                </div>
              </div>

              {/* Having Conditions */}
              <div className="border rounded-lg p-4">
                <div className="flex items-center justify-between mb-3">
                  <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-200">HAVING Conditions</h3>
                  <button
                    onClick={addHavingCondition}
                    className="px-3 py-1 text-xs bg-purple-600 text-white rounded hover:bg-purple-700 flex items-center"
                  >
                    <Plus className="h-3 w-3 mr-1" /> Add Condition
                  </button>
                </div>
                {havingConditions.length === 0 ? (
                  <p className="text-sm text-gray-500 italic">No HAVING conditions – all groups will be kept.</p>
                ) : (
                  <div className="space-y-3">
                    {havingConditions.map((hc) => (
                      <div key={hc.id} className="grid grid-cols-12 gap-2 items-center p-2 bg-gray-50 dark:bg-gray-900 rounded">
                        {/* Field (output column) */}
                        <select
                          value={hc.field}
                          onChange={(e) => updateHavingCondition(hc.id, { field: e.target.value })}
                          className="col-span-3 p-1 text-xs border rounded bg-white dark:bg-gray-800"
                        >
                          <option value="">Select column</option>
                          {outputColumns.map((col) => (
                            <option key={col.id} value={col.name}>
                              {col.name}
                            </option>
                          ))}
                        </select>
                        {/* Operator */}
                        <select
                          value={hc.operator}
                          onChange={(e) => updateHavingCondition(hc.id, { operator: e.target.value as any })}
                          className="col-span-2 p-1 text-xs border rounded bg-white dark:bg-gray-800"
                        >
                          <option value="=">=</option>
                          <option value="!=">!=</option>
                          <option value="<">&lt;</option>
                          <option value=">">&gt;</option>
                          <option value="<=">&lt;=</option>
                          <option value=">=">&gt;=</option>
                        </select>
                        {/* Value */}
                        <input
                          type="text"
                          value={hc.value}
                          onChange={(e) => updateHavingCondition(hc.id, { value: e.target.value })}
                          placeholder="Value"
                          className="col-span-4 p-1 text-xs border rounded bg-white dark:bg-gray-800"
                        />
                        {/* Remove */}
                        <button
                          onClick={() => removeHavingCondition(hc.id)}
                          className="col-span-1 p-1 text-red-600 hover:bg-red-50 rounded"
                        >
                          <Trash2 className="h-3 w-3" />
                        </button>
                      </div>
                    ))}
                  </div>
                )}
              </div>

              {/* Output Schema Preview */}
              <div className="border rounded-lg p-4 bg-gray-50 dark:bg-gray-900">
                <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-200 mb-3 flex items-center">
                  <Filter className="h-4 w-4 mr-2" /> Output Schema Preview
                </h3>
                <div className="max-h-40 overflow-y-auto">
                  <table className="w-full text-xs">
                    <thead className="text-left text-gray-600 dark:text-gray-300 border-b">
                      <tr>
                        <th className="pb-2">Name</th>
                        <th className="pb-2">Type</th>
                        <th className="pb-2">Nullable</th>
                        <th className="pb-2">Key</th>
                      </tr>
                    </thead>
                    <tbody>
                      {outputColumns.map((col) => (
                        <tr key={col.id} className="border-b border-gray-200 dark:border-gray-700">
                          <td className="py-1">{col.name}</td>
                          <td className="py-1 flex items-center">
                            {renderDataTypeIcon(col.type)} {col.type}
                          </td>
                          <td className="py-1">{col.nullable ? '✓' : '✗'}</td>
                          <td className="py-1">{col.isKey ? '✓' : ''}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </div>

          {/* Validation Messages */}
          {(errors.length > 0 || warnings.length > 0) && (
            <div className="mt-6 p-3 border rounded-lg">
              {errors.map((err, i) => (
                <div key={i} className="flex items-center text-red-600 text-sm">
                  <AlertCircle className="h-4 w-4 mr-2 flex-shrink-0" />
                  {err}
                </div>
              ))}
              {warnings.map((warn, i) => (
                <div key={i} className="flex items-center text-yellow-600 text-sm">
                  <AlertCircle className="h-4 w-4 mr-2 flex-shrink-0" />
                  {warn}
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="border-t p-4 flex items-center justify-between bg-gray-50 dark:bg-gray-900">
          <div className="text-xs text-gray-500 dark:text-gray-400">
            {errors.length === 0 ? '✅ Configuration valid' : `⚠️ ${errors.length} error(s)`}
          </div>
          <div className="flex space-x-3">
            <button
              onClick={onClose}
              className="px-4 py-2 text-sm border border-gray-300 dark:border-gray-600 rounded hover:bg-gray-100 dark:hover:bg-gray-800"
            >
              Cancel
            </button>
            <button
              onClick={handleSave}
              disabled={errors.length > 0}
              className={`px-4 py-2 text-sm rounded flex items-center ${
                errors.length > 0
                  ? 'bg-gray-300 text-gray-500 cursor-not-allowed'
                  : 'bg-gradient-to-r from-blue-500 to-blue-600 text-white hover:from-blue-600 hover:to-blue-700'
              }`}
            >
              <Save className="h-4 w-4 mr-2" />
              Save Configuration
            </button>
          </div>
        </div>
      </motion.div>
    </div>
  );
};