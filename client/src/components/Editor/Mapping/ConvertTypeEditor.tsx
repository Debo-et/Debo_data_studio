// src/components/Editor/ConvertTypeEditor.tsx
import React, { useState, useCallback, useEffect, useMemo } from 'react';
import { motion } from 'framer-motion';
import { v4 as uuidv4 } from 'uuid';
import {
  X,
  Plus,
  Trash2,
  AlertCircle,
  RotateCcw,
  ChevronDown,
  ChevronUp,
  Save,
  Eye,
  EyeOff,
} from 'lucide-react';

import {
  UnifiedCanvasNode,
  ConvertComponentConfiguration,
  ConvertRule,
  DataType,
  PostgreSQLDataType,
  FieldSchema,
} from '../../../types/unified-pipeline.types';

// ----------------------------------------------------------------------
// Types
// ----------------------------------------------------------------------

interface ColumnInfo {
  name: string;
  type: string;
}

interface ConvertTypeEditorProps {
  nodeId: string;
  nodeMetadata: UnifiedCanvasNode;
  inputColumns: ColumnInfo[];
  outputColumns?: ColumnInfo[];
  initialConfig?: ConvertComponentConfiguration;
  onClose: () => void;
  onSave: (config: ConvertComponentConfiguration) => void;
}

interface UIRule extends ConvertRule {
  uiId: string;
  expanded: boolean;
  validationError?: string;
}

// ----------------------------------------------------------------------
// Constants
// ----------------------------------------------------------------------

const DATA_TYPE_OPTIONS: Array<{ value: string; label: string; category: string }> = [
  // Abstract types
  { value: 'STRING', label: 'STRING (abstract)', category: 'Abstract' },
  { value: 'INTEGER', label: 'INTEGER (abstract)', category: 'Abstract' },
  { value: 'DECIMAL', label: 'DECIMAL (abstract)', category: 'Abstract' },
  { value: 'BOOLEAN', label: 'BOOLEAN (abstract)', category: 'Abstract' },
  { value: 'DATE', label: 'DATE (abstract)', category: 'Abstract' },
  { value: 'TIMESTAMP', label: 'TIMESTAMP (abstract)', category: 'Abstract' },
  { value: 'BINARY', label: 'BINARY (abstract)', category: 'Abstract' },
  // PostgreSQL concrete types
  { value: PostgreSQLDataType.INTEGER, label: 'INTEGER', category: 'Numeric' },
  { value: PostgreSQLDataType.BIGINT, label: 'BIGINT', category: 'Numeric' },
  { value: PostgreSQLDataType.SMALLINT, label: 'SMALLINT', category: 'Numeric' },
  { value: PostgreSQLDataType.DECIMAL, label: 'DECIMAL', category: 'Numeric' },
  { value: PostgreSQLDataType.NUMERIC, label: 'NUMERIC', category: 'Numeric' },
  { value: PostgreSQLDataType.REAL, label: 'REAL', category: 'Numeric' },
  { value: PostgreSQLDataType.DOUBLE_PRECISION, label: 'DOUBLE PRECISION', category: 'Numeric' },
  { value: PostgreSQLDataType.VARCHAR, label: 'VARCHAR', category: 'Character' },
  { value: PostgreSQLDataType.CHAR, label: 'CHAR', category: 'Character' },
  { value: PostgreSQLDataType.TEXT, label: 'TEXT', category: 'Character' },
  { value: PostgreSQLDataType.DATE, label: 'DATE', category: 'Date/Time' },
  { value: PostgreSQLDataType.TIMESTAMP, label: 'TIMESTAMP', category: 'Date/Time' },
  { value: PostgreSQLDataType.TIMESTAMPTZ, label: 'TIMESTAMPTZ', category: 'Date/Time' },
  { value: PostgreSQLDataType.TIME, label: 'TIME', category: 'Date/Time' },
  { value: PostgreSQLDataType.BOOLEAN, label: 'BOOLEAN', category: 'Boolean' },
  { value: PostgreSQLDataType.BYTEA, label: 'BYTEA', category: 'Binary' },
  { value: PostgreSQLDataType.JSON, label: 'JSON', category: 'JSON' },
  { value: PostgreSQLDataType.JSONB, label: 'JSONB', category: 'JSON' },
  { value: PostgreSQLDataType.UUID, label: 'UUID', category: 'UUID' },
];

// Group options by category for the select
const groupedTypeOptions = DATA_TYPE_OPTIONS.reduce((acc, opt) => {
  if (!acc[opt.category]) acc[opt.category] = [];
  acc[opt.category].push(opt);
  return acc;
}, {} as Record<string, typeof DATA_TYPE_OPTIONS>);

// ----------------------------------------------------------------------
// Helper Functions
// ----------------------------------------------------------------------

const generateDefaultRule = (position: number, sourceColumn?: string): UIRule => ({
  id: uuidv4(),
  uiId: uuidv4(),
  sourceColumn: sourceColumn || '',
  targetColumn: sourceColumn || '',
  targetType: 'STRING' as DataType,
  position,
  expanded: false,
});


// ----------------------------------------------------------------------
// Main Component
// ----------------------------------------------------------------------

export const ConvertTypeEditor: React.FC<ConvertTypeEditorProps> = ({
  nodeId,
  nodeMetadata,
  inputColumns,
  initialConfig,
  onClose,
  onSave,
}) => {
  // State
  const [rules, setRules] = useState<UIRule[]>(() => {
    if (initialConfig?.rules) {
      return initialConfig.rules.map(r => ({
        ...r,
        uiId: uuidv4(),
        expanded: false,
      }));
    }
    // Default: one rule per input column
    return inputColumns.map((col, idx) => generateDefaultRule(idx, col.name));
  });

  const [showSchemaPreview, setShowSchemaPreview] = useState(true);
  const [validationErrors, setValidationErrors] = useState<string[]>([]);
  const [isSaving, setIsSaving] = useState(false);

  // Derived data
  const inputColumnNames = useMemo(() => inputColumns.map(c => c.name), [inputColumns]);

  const outputSchemaFields = useMemo((): FieldSchema[] => {
    return rules
      .sort((a, b) => a.position - b.position)
      .map(rule => ({
        id: `field-${rule.id}`,
        name: rule.targetColumn,
        type: (rule.targetType as DataType) || 'STRING',
        nullable: rule.parameters?.nullHandling !== 'FAIL',
        isKey: false,
        description: `Converted from ${rule.sourceColumn}`,
      }));
  }, [rules]);

  const duplicateCheck = useMemo(() => {
    const names = outputSchemaFields.map(f => f.name);
    const duplicates = names.filter((n, i) => names.indexOf(n) !== i);
    return new Set(duplicates);
  }, [outputSchemaFields]);

  // Validation
  useEffect(() => {
    const errors: string[] = [];
    rules.forEach(rule => {
      if (!rule.sourceColumn) errors.push(`Rule ${rule.position + 1}: Source column missing`);
      if (!rule.targetColumn) errors.push(`Rule ${rule.position + 1}: Target column missing`);
      if (!rule.targetType) errors.push(`Rule ${rule.position + 1}: Target type missing`);
      if (duplicateCheck.has(rule.targetColumn)) {
        errors.push(`Duplicate output column name: ${rule.targetColumn}`);
      }
    });
    setValidationErrors(errors);
  }, [rules, duplicateCheck]);

  // Handlers
  const addRule = useCallback(() => {
    setRules(prev => {
      const newPos = prev.length;
      return [...prev, generateDefaultRule(newPos)];
    });
  }, []);

  const removeRule = useCallback((uiId: string) => {
    setRules(prev => prev.filter(r => r.uiId !== uiId).map((r, idx) => ({ ...r, position: idx })));
  }, []);

  const updateRule = useCallback((uiId: string, updates: Partial<ConvertRule>) => {
    setRules(prev =>
      prev.map(r => (r.uiId === uiId ? { ...r, ...updates } : r))
    );
  }, []);

  const toggleExpand = useCallback((uiId: string) => {
    setRules(prev =>
      prev.map(r => (r.uiId === uiId ? { ...r, expanded: !r.expanded } : r))
    );
  }, []);

  const moveRuleUp = useCallback((index: number) => {
    if (index === 0) return;
    setRules(prev => {
      const newRules = [...prev];
      [newRules[index - 1], newRules[index]] = [newRules[index], newRules[index - 1]];
      return newRules.map((r, idx) => ({ ...r, position: idx }));
    });
  }, []);

  const moveRuleDown = useCallback((index: number) => {
    if (index === rules.length - 1) return;
    setRules(prev => {
      const newRules = [...prev];
      [newRules[index], newRules[index + 1]] = [newRules[index + 1], newRules[index]];
      return newRules.map((r, idx) => ({ ...r, position: idx }));
    });
  }, [rules.length]);

  const autoMap = useCallback(() => {
    setRules(
      inputColumns.map((col, idx) => ({
        id: uuidv4(),
        uiId: uuidv4(),
        sourceColumn: col.name,
        targetColumn: col.name,
        targetType: (col.type as DataType) || 'STRING',
        position: idx,
        expanded: false,
      }))
    );
  }, [inputColumns]);

  const resetToDefaults = useCallback(() => {
    setRules(inputColumns.map((col, idx) => generateDefaultRule(idx, col.name)));
  }, [inputColumns]);

  const handleSave = useCallback(() => {
    if (validationErrors.length > 0) {
      alert('Please fix validation errors before saving.');
      return;
    }
    setIsSaving(true);

    const config: ConvertComponentConfiguration = {
      version: '1.0',
      rules: rules.map(({ uiId, expanded, validationError, ...rule }) => rule),
      outputSchema: {
        id: `output-${nodeId}`,
        name: `${nodeMetadata.name || 'Convert'} Output`,
        fields: outputSchemaFields,
        isTemporary: false,
        isMaterialized: false,
        metadata: {},
      },
      sqlGeneration: {
        requiresCasting: rules.some(r => r.targetType !== r.sourceColumn),
        usesConditionalLogic: rules.some(r => r.parameters?.onError || r.parameters?.nullHandling),
        estimatedRowMultiplier: 1.0,
      },
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        createdBy: 'convert-editor',
        ruleCount: rules.length,
        validationStatus: validationErrors.length === 0 ? 'VALID' : 'ERROR',
        warnings: validationErrors,
        dependencies: [],
      },
    };

    onSave(config);
    setIsSaving(false);
  }, [rules, outputSchemaFields, nodeId, nodeMetadata.name, validationErrors, onSave]);

  // Render
  return (
    <div className="fixed inset-0 z-[10000] bg-black/80 flex items-center justify-center p-4">
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        className="bg-gray-900 rounded-xl shadow-2xl w-full max-w-6xl h-[90vh] flex flex-col overflow-hidden"
      >
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-700 bg-gradient-to-r from-gray-800 to-gray-900">
          <div>
            <h2 className="text-xl font-bold text-white flex items-center">
              <span className="mr-2">🔄</span>
              Convert Type Editor
              <span className="ml-3 text-xs bg-purple-900/30 text-purple-300 px-2 py-0.5 rounded border border-purple-700">
                v1.0
              </span>
            </h2>
            <p className="text-sm text-gray-400 mt-1">
              Node: <span className="font-mono text-blue-400">{nodeMetadata.name || nodeId}</span>
              <span className="ml-4">
                Input: {inputColumns.length} col · Output: {rules.length} col
              </span>
            </p>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => setShowSchemaPreview(!showSchemaPreview)}
              className="p-2 text-gray-400 hover:text-white rounded hover:bg-gray-700 transition-colors"
              title={showSchemaPreview ? "Hide schema preview" : "Show schema preview"}
            >
              {showSchemaPreview ? <EyeOff className="h-5 w-5" /> : <Eye className="h-5 w-5" />}
            </button>
            <button
              onClick={onClose}
              className="p-2 text-gray-400 hover:text-white rounded hover:bg-gray-700 transition-colors"
              title="Close"
            >
              <X className="h-5 w-5" />
            </button>
          </div>
        </div>

        {/* Main Content */}
        <div className="flex flex-1 overflow-hidden">
          {/* Rules Table */}
          <div className={`flex-1 overflow-auto p-4 ${showSchemaPreview ? 'pr-2' : ''}`}>
            <div className="mb-4 flex items-center justify-between">
              <h3 className="text-sm font-medium text-gray-300">Conversion Rules</h3>
              <div className="flex gap-2">
                <button
                  onClick={autoMap}
                  className="px-3 py-1 text-xs bg-gray-800 border border-gray-700 text-gray-300 rounded hover:bg-gray-700 transition-colors"
                >
                  Auto Map
                </button>
                <button
                  onClick={resetToDefaults}
                  className="px-3 py-1 text-xs bg-gray-800 border border-gray-700 text-gray-300 rounded hover:bg-gray-700 transition-colors flex items-center"
                >
                  <RotateCcw className="h-3 w-3 mr-1" />
                  Reset
                </button>
              </div>
            </div>

            {/* Table Header */}
            <div className="grid grid-cols-12 gap-2 mb-2 px-2 text-xs font-medium text-gray-400">
              <div className="col-span-1">#</div>
              <div className="col-span-3">Input Column</div>
              <div className="col-span-3">Output Column</div>
              <div className="col-span-3">Target Type</div>
              <div className="col-span-2 text-right">Actions</div>
            </div>

            {/* Rules */}
            <div className="space-y-2">
              {rules.map((rule, index) => (
                <div key={rule.uiId} className="border border-gray-700 rounded-lg bg-gray-800/50">
                  {/* Main row */}
                  <div className="grid grid-cols-12 gap-2 items-center p-2">
                    <div className="col-span-1 text-xs text-gray-400">{index + 1}</div>
                    <div className="col-span-3">
                      <select
                        value={rule.sourceColumn}
                        onChange={(e) => updateRule(rule.uiId, { 
                          sourceColumn: e.target.value, 
                          targetColumn: e.target.value || rule.targetColumn 
                        })}
                        className="w-full bg-gray-800 border border-gray-700 text-white text-sm rounded px-2 py-1.5 focus:outline-none focus:ring-2 focus:ring-blue-500"
                      >
                        <option value="">Select column</option>
                        {inputColumnNames.map(name => (
                          <option key={name} value={name}>{name}</option>
                        ))}
                      </select>
                    </div>
                    <div className="col-span-3">
                      <input
                        type="text"
                        value={rule.targetColumn}
                        onChange={(e) => updateRule(rule.uiId, { targetColumn: e.target.value })}
                        className="w-full bg-gray-800 border border-gray-700 text-white text-sm rounded px-2 py-1.5 focus:outline-none focus:ring-2 focus:ring-blue-500"
                        placeholder="Output name"
                      />
                    </div>
                    <div className="col-span-3">
                      <select
                        value={rule.targetType}
                        onChange={(e) => updateRule(rule.uiId, { 
                          targetType: e.target.value as PostgreSQLDataType | DataType 
                        })}
                        className="w-full bg-gray-800 border border-gray-700 text-white text-sm rounded px-2 py-1.5 focus:outline-none focus:ring-2 focus:ring-blue-500"
                      >
                        <option value="">Select type</option>
                        {Object.entries(groupedTypeOptions).map(([category, opts]) => (
                          <optgroup key={category} label={category}>
                            {opts.map(opt => (
                              <option key={opt.value} value={opt.value}>{opt.label}</option>
                            ))}
                          </optgroup>
                        ))}
                      </select>
                    </div>
                    <div className="col-span-2 flex justify-end gap-1">
                      <button
                        onClick={() => toggleExpand(rule.uiId)}
                        className="p-1 text-gray-400 hover:text-white rounded hover:bg-gray-700"
                        title="Advanced settings"
                      >
                        {rule.expanded ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
                      </button>
                      <button
                        onClick={() => moveRuleUp(index)}
                        disabled={index === 0}
                        className="p-1 text-gray-400 hover:text-white rounded hover:bg-gray-700 disabled:opacity-30"
                        title="Move up"
                      >
                        ↑
                      </button>
                      <button
                        onClick={() => moveRuleDown(index)}
                        disabled={index === rules.length - 1}
                        className="p-1 text-gray-400 hover:text-white rounded hover:bg-gray-700 disabled:opacity-30"
                        title="Move down"
                      >
                        ↓
                      </button>
                      <button
                        onClick={() => removeRule(rule.uiId)}
                        className="p-1 text-red-400 hover:text-red-300 rounded hover:bg-gray-700"
                        title="Remove"
                      >
                        <Trash2 className="h-4 w-4" />
                      </button>
                    </div>
                  </div>

                  {/* Advanced parameters (expandable) */}
                  {rule.expanded && (
                    <div className="p-4 border-t border-gray-700 bg-gray-900/50 grid grid-cols-3 gap-4">
                      <div>
                        <label className="block text-xs text-gray-400 mb-1">Length</label>
                        <input
                          type="number"
                          value={rule.parameters?.length ?? ''}
                          onChange={(e) => updateRule(rule.uiId, {
                            parameters: { 
                              ...rule.parameters, 
                              length: e.target.value ? parseInt(e.target.value) : undefined 
                            }
                          })}
                          className="w-full bg-gray-800 border border-gray-700 text-white text-xs rounded px-2 py-1"
                          placeholder="e.g., 255"
                        />
                      </div>
                      <div>
                        <label className="block text-xs text-gray-400 mb-1">Precision</label>
                        <input
                          type="number"
                          value={rule.parameters?.precision ?? ''}
                          onChange={(e) => updateRule(rule.uiId, {
                            parameters: { 
                              ...rule.parameters, 
                              precision: e.target.value ? parseInt(e.target.value) : undefined 
                            }
                          })}
                          className="w-full bg-gray-800 border border-gray-700 text-white text-xs rounded px-2 py-1"
                          placeholder="e.g., 10"
                        />
                      </div>
                      <div>
                        <label className="block text-xs text-gray-400 mb-1">Scale</label>
                        <input
                          type="number"
                          value={rule.parameters?.scale ?? ''}
                          onChange={(e) => updateRule(rule.uiId, {
                            parameters: { 
                              ...rule.parameters, 
                              scale: e.target.value ? parseInt(e.target.value) : undefined 
                            }
                          })}
                          className="w-full bg-gray-800 border border-gray-700 text-white text-xs rounded px-2 py-1"
                          placeholder="e.g., 2"
                        />
                      </div>
                      <div>
                        <label className="block text-xs text-gray-400 mb-1">Format</label>
                        <input
                          type="text"
                          value={rule.parameters?.format ?? ''}
                          onChange={(e) => updateRule(rule.uiId, {
                            parameters: { ...rule.parameters, format: e.target.value }
                          })}
                          className="w-full bg-gray-800 border border-gray-700 text-white text-xs rounded px-2 py-1"
                          placeholder="e.g., yyyy-MM-dd"
                        />
                      </div>
                      <div>
                        <label className="block text-xs text-gray-400 mb-1">Default Value</label>
                        <input
                          type="text"
                          value={rule.parameters?.defaultValue ?? ''}
                          onChange={(e) => updateRule(rule.uiId, {
                            parameters: { ...rule.parameters, defaultValue: e.target.value }
                          })}
                          className="w-full bg-gray-800 border border-gray-700 text-white text-xs rounded px-2 py-1"
                          placeholder="e.g., 0"
                        />
                      </div>
                      <div>
                        <label className="block text-xs text-gray-400 mb-1">Null Handling</label>
                        <select
                          value={rule.parameters?.nullHandling || 'KEEP_NULL'}
                          onChange={(e) => updateRule(rule.uiId, {
                            parameters: { 
                              ...rule.parameters, 
                              nullHandling: e.target.value as 'KEEP_NULL' | 'DEFAULT' | 'FAIL' 
                            }
                          })}
                          className="w-full bg-gray-800 border border-gray-700 text-white text-xs rounded px-2 py-1"
                        >
                          <option value="KEEP_NULL">Keep null</option>
                          <option value="DEFAULT">Use default</option>
                          <option value="FAIL">Fail</option>
                        </select>
                      </div>
                      <div>
                        <label className="block text-xs text-gray-400 mb-1">On Error</label>
                        <select
                          value={rule.parameters?.onError || 'FAIL_JOB'}
                          onChange={(e) => updateRule(rule.uiId, {
                            parameters: { 
                              ...rule.parameters, 
                              onError: e.target.value as 'FAIL_JOB' | 'SKIP_ROW' | 'USE_DEFAULT' | 'SET_NULL' 
                            }
                          })}
                          className="w-full bg-gray-800 border border-gray-700 text-white text-xs rounded px-2 py-1"
                        >
                          <option value="FAIL_JOB">Fail job</option>
                          <option value="SKIP_ROW">Skip row</option>
                          <option value="USE_DEFAULT">Use default</option>
                          <option value="SET_NULL">Set null</option>
                        </select>
                      </div>
                      <div className="col-span-3 flex items-center gap-4">
                        <label className="flex items-center gap-2 text-xs text-gray-400">
                          <input
                            type="checkbox"
                            checked={rule.parameters?.trim || false}
                            onChange={(e) => updateRule(rule.uiId, {
                              parameters: { ...rule.parameters, trim: e.target.checked }
                            })}
                            className="rounded bg-gray-800 border-gray-700"
                          />
                          Trim
                        </label>
                        {/* Add more options as needed */}
                      </div>
                    </div>
                  )}
                </div>
              ))}
            </div>

            <button
              onClick={addRule}
              className="mt-4 w-full py-2 border border-dashed border-gray-600 text-gray-400 hover:text-white hover:border-gray-500 rounded transition-colors flex items-center justify-center"
            >
              <Plus className="h-4 w-4 mr-2" />
              Add Rule
            </button>
          </div>

          {/* Schema Preview Sidebar */}
          {showSchemaPreview && (
            <div className="w-80 border-l border-gray-700 bg-gray-800/30 p-4 overflow-y-auto">
              <h3 className="text-sm font-medium text-gray-300 mb-3 flex items-center">
                <Eye className="h-4 w-4 mr-2 text-blue-400" />
                Schema Preview
              </h3>

              {/* Input Schema */}
              <div className="mb-4 bg-gray-800/50 border border-gray-700 rounded-lg p-3">
                <div className="text-xs text-gray-400 mb-2">Input Schema</div>
                {inputColumns.map(col => (
                  <div key={col.name} className="flex justify-between text-xs py-1">
                    <span className="text-gray-300">{col.name}</span>
                    <span className="text-gray-500 font-mono">{col.type}</span>
                  </div>
                ))}
              </div>

              {/* Output Schema */}
              <div className="bg-gray-800/50 border border-gray-700 rounded-lg p-3">
                <div className="flex items-center justify-between mb-2">
                  <div className="text-xs text-gray-400">Output Schema</div>
                  <span className="text-[10px] text-gray-500 border border-gray-600 px-1 rounded">
                    {outputSchemaFields.length} cols
                  </span>
                </div>
                {outputSchemaFields.map(field => (
                  <div key={field.id} className="flex justify-between text-xs py-1">
                    <span className={duplicateCheck.has(field.name) ? 'text-red-400' : 'text-gray-300'}>
                      {field.name}
                      {duplicateCheck.has(field.name) && (
                        <AlertCircle className="inline h-3 w-3 ml-1 text-red-400" />
                      )}
                    </span>
                    <span className="text-gray-500 font-mono">{field.type}</span>
                  </div>
                ))}
                {rules.length === 0 && (
                  <div className="text-xs text-gray-500 italic py-2">No rules defined</div>
                )}
              </div>

              {/* Validation Summary */}
              {validationErrors.length > 0 && (
                <div className="mt-4 bg-red-900/20 border border-red-800 rounded-lg p-3">
                  <div className="text-xs text-red-400 flex items-center mb-1">
                    <AlertCircle className="h-3 w-3 mr-1" />
                    Validation Errors
                  </div>
                  <ul className="list-disc list-inside text-xs text-red-300 space-y-1">
                    {validationErrors.slice(0, 5).map((err, i) => (
                      <li key={i}>{err}</li>
                    ))}
                    {validationErrors.length > 5 && (
                      <li className="text-red-400">...and {validationErrors.length - 5} more</li>
                    )}
                  </ul>
                </div>
              )}
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between px-6 py-4 border-t border-gray-700 bg-gray-900">
          <div className="text-xs text-gray-500">
            {validationErrors.length === 0 ? (
              <span className="text-green-400">✓ Configuration valid</span>
            ) : (
              <span className="text-red-400">⚠️ {validationErrors.length} validation error(s)</span>
            )}
          </div>
          <div className="flex gap-3">
            <button
              onClick={onClose}
              className="px-4 py-2 text-sm border border-gray-600 text-gray-300 rounded hover:bg-gray-800 transition-colors"
            >
              Cancel
            </button>
            <button
              onClick={handleSave}
              disabled={validationErrors.length > 0 || isSaving}
              className="px-4 py-2 text-sm bg-gradient-to-r from-blue-600 to-purple-600 text-white rounded hover:from-blue-700 hover:to-purple-700 transition-all disabled:opacity-50 flex items-center"
            >
              {isSaving ? (
                <>Saving...</>
              ) : (
                <>
                  <Save className="h-4 w-4 mr-2" />
                  Save Configuration
                </>
              )}
            </button>
          </div>
        </div>
      </motion.div>
    </div>
  );
};