// src/components/Editor/JoinEditor.tsx
import React, { useState, useEffect, useMemo } from 'react';
import { motion } from 'framer-motion';
import {
  JoinComponentConfiguration,
  FieldSchema,
  PostgreSQLDataType,
  DataType,
} from '../../../types/unified-pipeline.types';
import { X, Plus, Save, AlertCircle } from 'lucide-react';

// Helper: map DataType to PostgreSQLDataType
const mapDataTypeToPostgreSQL = (type: DataType): PostgreSQLDataType => {
  switch (type) {
    case 'STRING': return PostgreSQLDataType.TEXT;
    case 'INTEGER': return PostgreSQLDataType.INTEGER;
    case 'DECIMAL': return PostgreSQLDataType.NUMERIC;
    case 'BOOLEAN': return PostgreSQLDataType.BOOLEAN;
    case 'DATE': return PostgreSQLDataType.DATE;
    case 'TIMESTAMP': return PostgreSQLDataType.TIMESTAMP;
    case 'BINARY': return PostgreSQLDataType.BYTEA;
    default: return PostgreSQLDataType.TEXT;
  }
};

// UI types for condition rows
interface ConditionRow {
  id: string;
  leftField: string;
  operator: '=' | '!=' | '<' | '>' | '<=' | '>=' | 'LIKE';
  rightField: string;
}

interface OutputFieldRow {
  id: string;
  source: 'left' | 'right';
  originalName: string;
  alias: string;
  dataType: PostgreSQLDataType;
  include: boolean;
  isKey: boolean;
}

interface JoinEditorProps {
  nodeId: string;
  nodeName: string;
  leftSchema: { id: string; name: string; fields: FieldSchema[] };
  rightSchema: { id: string; name: string; fields: FieldSchema[] };
  initialConfig?: JoinComponentConfiguration;
  onClose: () => void;
  onSave: (config: JoinComponentConfiguration) => void;
}

const JoinEditor: React.FC<JoinEditorProps> = ({
  nodeName,
  leftSchema,
  rightSchema,
  initialConfig,
  onClose,
  onSave,
}) => {
  // State for basic join settings
  const [joinType, setJoinType] = useState<'INNER' | 'LEFT' | 'RIGHT' | 'FULL' | 'CROSS'>(
    initialConfig?.joinType || 'INNER'
  );
  const [joinAlgorithm, setJoinAlgorithm] = useState<'HASH' | 'MERGE' | 'NESTED_LOOP'>(
    initialConfig?.sqlGeneration?.joinAlgorithm || 'HASH'
  );
  const [nullHandling, setNullHandling] = useState<'INCLUDE' | 'EXCLUDE' | 'TREAT_AS_FALSE'>(
    initialConfig?.sqlGeneration?.nullHandling || 'INCLUDE'
  );

  // Join conditions
  const [conditions, setConditions] = useState<ConditionRow[]>(() => {
    if (initialConfig?.joinConditions) {
      return initialConfig.joinConditions.map(c => ({
        id: c.id,
        leftField: c.leftField,
        operator: c.operator,
        rightField: c.rightField,
      }));
    }
    return [];
  });

  // Join hints
  const [enableHints, setEnableHints] = useState(initialConfig?.joinHints?.enableJoinHint || false);
  const [joinHint, setJoinHint] = useState(initialConfig?.joinHints?.joinHint || '');
  const [maxParallelism, setMaxParallelism] = useState<number | undefined>(initialConfig?.joinHints?.maxParallelism);
  const [memoryGrant, setMemoryGrant] = useState<number | undefined>(initialConfig?.joinHints?.memoryGrant);

  // Output schema fields
  const [outputFields, setOutputFields] = useState<OutputFieldRow[]>(() => {
    // Build from initial output schema or union of left+right
    const existing = initialConfig?.outputSchema;
    const fieldAliases = existing?.fieldAliases || {};

    // Create a union of all fields from both sides
    const leftFields = leftSchema.fields.map(f => ({
      id: `left-${f.id}`,
      source: 'left' as const,
      originalName: f.name,
      alias: fieldAliases[f.name] || f.name,
      dataType: mapDataTypeToPostgreSQL(f.type),
      include: existing?.fields?.some(ef => ef.name === f.name) ?? true,
      isKey: f.isKey || false,
    }));

    const rightFields = rightSchema.fields.map(f => ({
      id: `right-${f.id}`,
      source: 'right' as const,
      originalName: f.name,
      alias: fieldAliases[f.name] || f.name,
      dataType: mapDataTypeToPostgreSQL(f.type),
      include: existing?.fields?.some(ef => ef.name === f.name) ?? true,
      isKey: f.isKey || false,
    }));

    // If deduplicate, merge fields with same name (prefer left)
    const deduplicate = existing?.deduplicateFields ?? true;
    if (deduplicate) {
      const merged: OutputFieldRow[] = [];
      const seen = new Set<string>();
      [...leftFields, ...rightFields].forEach(f => {
        if (!seen.has(f.originalName)) {
          seen.add(f.originalName);
          merged.push(f);
        }
      });
      return merged;
    }
    return [...leftFields, ...rightFields];
  });

  const [deduplicateFields, setDeduplicateFields] = useState(initialConfig?.outputSchema?.deduplicateFields ?? true);

  // Validation messages
  const [validationMessages, setValidationMessages] = useState<string[]>([]);

  // Update validation whenever conditions change
  useEffect(() => {
    const msgs: string[] = [];
    conditions.forEach((cond, idx) => {
      if (!cond.leftField) msgs.push(`Condition ${idx + 1}: left field missing`);
      if (!cond.rightField) msgs.push(`Condition ${idx + 1}: right field missing`);
    });
    setValidationMessages(msgs);
  }, [conditions]);

  // Generate SQL preview
  const sqlPreview = useMemo(() => {
    const leftTable = leftSchema.name;
    const rightTable = rightSchema.name;

    const conditionClauses = conditions.map(c => {
      const leftCol = c.leftField;
      const rightCol = c.rightField;
      if (c.operator === 'LIKE') {
        return `${leftTable}.${leftCol} LIKE ${rightTable}.${rightCol}`;
      }
      return `${leftTable}.${leftCol} ${c.operator} ${rightTable}.${rightCol}`;
    });

    const whereClause = conditionClauses.length ? `ON ${conditionClauses.join(' AND ')}` : '';

    const selectColumns = outputFields
      .filter(f => f.include)
      .map(f => `${f.source === 'left' ? leftTable : rightTable}.${f.originalName} AS ${f.alias}`)
      .join(',\n  ');

    return `SELECT
  ${selectColumns || '*'}
FROM ${leftTable}
${joinType} JOIN ${rightTable}
${whereClause};`;
  }, [joinType, leftSchema, rightSchema, conditions, outputFields]);

  // Handlers
  const addCondition = () => {
    const newCond: ConditionRow = {
      id: `cond-${Date.now()}-${Math.random()}`,
      leftField: '',
      operator: '=',
      rightField: '',
    };
    setConditions([...conditions, newCond]);
  };

  const removeCondition = (id: string) => {
    setConditions(conditions.filter(c => c.id !== id));
  };

  const updateCondition = (id: string, updates: Partial<ConditionRow>) => {
    setConditions(conditions.map(c => (c.id === id ? { ...c, ...updates } : c)));
  };

  const toggleIncludeField = (fieldId: string) => {
    setOutputFields(fields => fields.map(f => (f.id === fieldId ? { ...f, include: !f.include } : f)));
  };

  const updateFieldAlias = (fieldId: string, alias: string) => {
    setOutputFields(fields => fields.map(f => (f.id === fieldId ? { ...f, alias } : f)));
  };

  const updateFieldDataType = (fieldId: string, dataType: PostgreSQLDataType) => {
    setOutputFields(fields => fields.map(f => (f.id === fieldId ? { ...f, dataType } : f)));
  };

  const autoFillAliases = () => {
    const counts: Record<string, number> = {};
    setOutputFields(fields =>
      fields.map(f => {
        const base = f.originalName;
        counts[base] = (counts[base] || 0) + 1;
        const alias = counts[base] > 1 ? `${base}_${f.source}` : base;
        return { ...f, alias };
      })
    );
  };

  const handleSave = () => {
    // Build output schema fields
    const outputFieldsList = outputFields
      .filter(f => f.include)
      .map(f => ({
        id: f.id,
        name: f.alias,
        type: (() => {
          // Convert PostgreSQLDataType back to DataType (simplified)
          if ([PostgreSQLDataType.INTEGER, PostgreSQLDataType.SMALLINT, PostgreSQLDataType.BIGINT].includes(f.dataType))
            return 'INTEGER' as DataType;
          if ([PostgreSQLDataType.DECIMAL, PostgreSQLDataType.NUMERIC, PostgreSQLDataType.REAL, PostgreSQLDataType.DOUBLE_PRECISION].includes(f.dataType))
            return 'DECIMAL' as DataType;
          if (f.dataType === PostgreSQLDataType.BOOLEAN) return 'BOOLEAN' as DataType;
          if (f.dataType === PostgreSQLDataType.DATE) return 'DATE' as DataType;
          if (f.dataType === PostgreSQLDataType.TIMESTAMP || f.dataType === PostgreSQLDataType.TIMESTAMPTZ)
            return 'TIMESTAMP' as DataType;
          if (f.dataType === PostgreSQLDataType.BYTEA) return 'BINARY' as DataType;
          return 'STRING' as DataType;
        })(),
        nullable: true,
        isKey: f.isKey,
        originalName: f.originalName,
        metadata: { source: f.source },
      }));

    // Build fieldAliases map
    const fieldAliases: Record<string, string> = {};
    outputFields.forEach(f => {
      if (f.alias !== f.originalName) {
        fieldAliases[f.originalName] = f.alias;
      }
    });

    // Construct final configuration
    const config: JoinComponentConfiguration = {
      version: '1.0',
      joinType,
      joinConditions: conditions.map((c, idx) => ({
        id: c.id,
        leftTable: leftSchema.name,
        leftField: c.leftField,
        rightTable: rightSchema.name,
        rightField: c.rightField,
        operator: c.operator,
        position: idx,
      })),
      joinHints: {
        enableJoinHint: enableHints,
        joinHint: enableHints ? joinHint : undefined,
        maxParallelism: enableHints ? maxParallelism : undefined,
        memoryGrant: enableHints ? memoryGrant : undefined,
      },
      outputSchema: {
        fields: outputFieldsList,
        deduplicateFields,
        fieldAliases,
      },
      sqlGeneration: {
        joinAlgorithm,
        estimatedJoinCardinality: 1.0,
        nullHandling,
        requiresSort: joinAlgorithm === 'MERGE',
        canParallelize: joinAlgorithm !== 'NESTED_LOOP',
      },
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        joinCardinality: undefined,
        optimizationApplied: false,
        warnings: validationMessages,
      },
    };

    onSave(config);
  };

  // Keyboard shortcuts
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
      if (e.key === 's' && e.ctrlKey) {
        e.preventDefault();
        handleSave();
      }
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [handleSave, onClose]);

  return (
    <div className="fixed inset-0 z-[9999] bg-black bg-opacity-80 flex items-center justify-center p-4">
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        className="bg-white rounded-xl shadow-2xl w-full max-w-7xl h-[90vh] flex flex-col overflow-hidden"
      >
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b bg-gradient-to-r from-blue-50 to-gray-50">
          <div>
            <h2 className="text-xl font-bold flex items-center">
              <span className="mr-2">🔗</span>
              Join Editor
              <span className="ml-3 text-xs bg-purple-100 text-purple-800 px-2 py-0.5 rounded">
                Join Configuration v1.0
              </span>
            </h2>
            <p className="text-sm text-gray-600 mt-1">
              Node: <span className="font-semibold text-blue-600">{nodeName}</span>
              <span className="ml-3 text-xs bg-gray-100 text-gray-800 px-2 py-0.5 rounded">
                {leftSchema.fields.length} left columns • {rightSchema.fields.length} right columns • {conditions.length} conditions
              </span>
            </p>
          </div>
          <button onClick={onClose} className="p-2 hover:bg-gray-200 rounded-full transition-colors">
            <X className="w-5 h-5 text-gray-600" />
          </button>
        </div>

        {/* Main 3‑column layout */}
        <div className="flex flex-1 overflow-hidden">
          {/* Left panel – Input Schemas */}
          <div className="w-1/4 border-r border-gray-200 bg-gray-50 overflow-y-auto p-4">
            <h3 className="text-sm font-semibold text-gray-700 mb-3 flex items-center">
              <span className="w-2 h-2 rounded-full bg-green-500 mr-2"></span>
              Left Input: {leftSchema.name}
            </h3>
            <div className="space-y-2 mb-6">
              {leftSchema.fields.map(f => (
                <div key={f.id} className="flex items-center justify-between text-xs bg-white p-2 rounded border">
                  <span className="font-mono text-gray-800">{f.name}</span>
                  <span className="text-gray-500">{f.type}</span>
                </div>
              ))}
            </div>

            <h3 className="text-sm font-semibold text-gray-700 mb-3 flex items-center">
              <span className="w-2 h-2 rounded-full bg-blue-500 mr-2"></span>
              Right Input: {rightSchema.name}
            </h3>
            <div className="space-y-2">
              {rightSchema.fields.map(f => (
                <div key={f.id} className="flex items-center justify-between text-xs bg-white p-2 rounded border">
                  <span className="font-mono text-gray-800">{f.name}</span>
                  <span className="text-gray-500">{f.type}</span>
                </div>
              ))}
            </div>
          </div>

          {/* Center panel – Join Configuration */}
          <div className="w-2/4 border-r border-gray-200 bg-white overflow-y-auto p-4">
            <div className="space-y-6">
              {/* Basic Settings */}
              <div className="bg-gray-50 p-4 rounded-lg">
                <h4 className="text-sm font-medium text-gray-700 mb-3">Basic Settings</h4>
                <div className="grid grid-cols-3 gap-4">
                  <div>
                    <label className="block text-xs text-gray-500 mb-1">Join Type</label>
                    <select
                      value={joinType}
                      onChange={e => setJoinType(e.target.value as any)}
                      className="w-full text-sm border border-gray-300 rounded px-2 py-1.5"
                    >
                      <option value="INNER">INNER</option>
                      <option value="LEFT">LEFT</option>
                      <option value="RIGHT">RIGHT</option>
                      <option value="FULL">FULL</option>
                      <option value="CROSS">CROSS</option>
                    </select>
                  </div>
                  <div>
                    <label className="block text-xs text-gray-500 mb-1">Algorithm</label>
                    <select
                      value={joinAlgorithm}
                      onChange={e => setJoinAlgorithm(e.target.value as any)}
                      className="w-full text-sm border border-gray-300 rounded px-2 py-1.5"
                    >
                      <option value="HASH">HASH</option>
                      <option value="MERGE">MERGE</option>
                      <option value="NESTED_LOOP">NESTED LOOP</option>
                    </select>
                  </div>
                  <div>
                    <label className="block text-xs text-gray-500 mb-1">Null Handling</label>
                    <select
                      value={nullHandling}
                      onChange={e => setNullHandling(e.target.value as any)}
                      className="w-full text-sm border border-gray-300 rounded px-2 py-1.5"
                    >
                      <option value="INCLUDE">INCLUDE</option>
                      <option value="EXCLUDE">EXCLUDE</option>
                      <option value="TREAT_AS_FALSE">TREAT AS FALSE</option>
                    </select>
                  </div>
                </div>
              </div>

              {/* Join Conditions */}
              <div>
                <div className="flex justify-between items-center mb-3">
                  <h4 className="text-sm font-medium text-gray-700">Join Conditions</h4>
                  <button
                    onClick={addCondition}
                    className="text-xs bg-blue-500 text-white px-2 py-1 rounded hover:bg-blue-600 flex items-center"
                  >
                    <Plus className="w-3 h-3 mr-1" /> Add Condition
                  </button>
                </div>
                <div className="space-y-2">
                  {conditions.map((cond, idx) => (
                    <div key={cond.id} className="flex items-center space-x-2 bg-gray-50 p-2 rounded border">
                      <span className="text-xs text-gray-500 w-6">{idx + 1}</span>
                      <select
                        value={cond.leftField}
                        onChange={e => updateCondition(cond.id, { leftField: e.target.value })}
                        className="text-xs border border-gray-300 rounded px-2 py-1 w-32"
                      >
                        <option value="">Select field</option>
                        {leftSchema.fields.map(f => (
                          <option key={f.id} value={f.name}>{f.name}</option>
                        ))}
                      </select>
                      <select
                        value={cond.operator}
                        onChange={e => updateCondition(cond.id, { operator: e.target.value as any })}
                        className="text-xs border border-gray-300 rounded px-2 py-1 w-20"
                      >
                        <option value="=">=</option>
                        <option value="!=">!=</option>
                        <option value="<">&lt;</option>
                        <option value=">">&gt;</option>
                        <option value="<=">&lt;=</option>
                        <option value=">=">&gt;=</option>
                        <option value="LIKE">LIKE</option>
                      </select>
                      <select
                        value={cond.rightField}
                        onChange={e => updateCondition(cond.id, { rightField: e.target.value })}
                        className="text-xs border border-gray-300 rounded px-2 py-1 w-32"
                      >
                        <option value="">Select field</option>
                        {rightSchema.fields.map(f => (
                          <option key={f.id} value={f.name}>{f.name}</option>
                        ))}
                      </select>
                      <button
                        onClick={() => removeCondition(cond.id)}
                        className="text-red-500 hover:text-red-700 p-1"
                      >
                        <X className="w-4 h-4" />
                      </button>
                    </div>
                  ))}
                  {conditions.length === 0 && (
                    <p className="text-xs text-gray-400 italic">No join conditions defined.</p>
                  )}
                </div>
              </div>

              {/* Join Hints (Advanced) */}
              <div className="border-t pt-4">
                <div className="flex items-center mb-2">
                  <input
                    type="checkbox"
                    id="enableHints"
                    checked={enableHints}
                    onChange={e => setEnableHints(e.target.checked)}
                    className="mr-2"
                  />
                  <label htmlFor="enableHints" className="text-sm font-medium text-gray-700">
                    Enable Join Hints (Advanced)
                  </label>
                </div>
                {enableHints && (
                  <div className="grid grid-cols-3 gap-4 ml-6">
                    <div>
                      <label className="block text-xs text-gray-500">Hint</label>
                      <input
                        type="text"
                        value={joinHint}
                        onChange={e => setJoinHint(e.target.value)}
                        className="w-full text-sm border border-gray-300 rounded px-2 py-1"
                        placeholder="e.g. MERGE"
                      />
                    </div>
                    <div>
                      <label className="block text-xs text-gray-500">Max Parallelism</label>
                      <input
                        type="number"
                        value={maxParallelism ?? ''}
                        onChange={e => setMaxParallelism(e.target.value ? parseInt(e.target.value) : undefined)}
                        className="w-full text-sm border border-gray-300 rounded px-2 py-1"
                        min={1}
                      />
                    </div>
                    <div>
                      <label className="block text-xs text-gray-500">Memory Grant (MB)</label>
                      <input
                        type="number"
                        value={memoryGrant ?? ''}
                        onChange={e => setMemoryGrant(e.target.value ? parseInt(e.target.value) : undefined)}
                        className="w-full text-sm border border-gray-300 rounded px-2 py-1"
                        min={1}
                      />
                    </div>
                  </div>
                )}
              </div>

              {/* SQL Preview */}
              <div className="border-t pt-4">
                <h4 className="text-sm font-medium text-gray-700 mb-2">SQL Preview</h4>
                <pre className="bg-gray-900 text-green-400 text-xs p-3 rounded-lg overflow-x-auto">
                  {sqlPreview}
                </pre>
              </div>
            </div>
          </div>

          {/* Right panel – Output Schema */}
          <div className="w-1/4 bg-gray-50 overflow-y-auto p-4">
            <div className="flex items-center justify-between mb-3">
              <h4 className="text-sm font-semibold text-gray-700">Output Schema</h4>
              <div className="flex items-center space-x-2">
                <label className="text-xs flex items-center">
                  <input
                    type="checkbox"
                    checked={deduplicateFields}
                    onChange={e => setDeduplicateFields(e.target.checked)}
                    className="mr-1"
                  />
                  Deduplicate
                </label>
                <button
                  onClick={autoFillAliases}
                  className="text-xs bg-gray-200 hover:bg-gray-300 px-2 py-1 rounded"
                  title="Auto‑fill aliases"
                >
                  Auto
                </button>
              </div>
            </div>

            <div className="space-y-2">
              {outputFields.map(field => (
                <div key={field.id} className="bg-white p-2 rounded border text-xs">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center">
                      <input
                        type="checkbox"
                        checked={field.include}
                        onChange={() => toggleIncludeField(field.id)}
                        className="mr-2"
                      />
                      <span className="font-mono">{field.originalName}</span>
                      <span className={`ml-2 px-1 rounded ${field.source === 'left' ? 'bg-green-100 text-green-700' : 'bg-blue-100 text-blue-700'}`}>
                        {field.source}
                      </span>
                    </div>
                    {field.isKey && <span className="text-yellow-600 text-[10px]">KEY</span>}
                  </div>
                  <div className="grid grid-cols-2 gap-2 mt-2">
                    <div>
                      <label className="block text-[10px] text-gray-500">Alias</label>
                      <input
                        type="text"
                        value={field.alias}
                        onChange={e => updateFieldAlias(field.id, e.target.value)}
                        className="w-full text-xs border border-gray-300 rounded px-1 py-0.5"
                      />
                    </div>
                    <div>
                      <label className="block text-[10px] text-gray-500">Type</label>
                      <select
                        value={field.dataType}
                        onChange={e => updateFieldDataType(field.id, e.target.value as PostgreSQLDataType)}
                        className="w-full text-xs border border-gray-300 rounded px-1 py-0.5"
                      >
                        {Object.values(PostgreSQLDataType).map(t => (
                          <option key={t} value={t}>{t}</option>
                        ))}
                      </select>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Footer */}
        <div className="border-t border-gray-200 px-6 py-3 bg-gray-50 flex items-center justify-between">
          <div className="flex items-center text-sm">
            {validationMessages.length > 0 ? (
              <div className="flex items-center text-yellow-600">
                <AlertCircle className="w-4 h-4 mr-1" />
                <span>{validationMessages.length} warning(s)</span>
              </div>
            ) : (
              <div className="text-green-600">✅ Configuration valid</div>
            )}
          </div>
          <div className="flex items-center space-x-3">
            <span className="text-xs text-gray-500">Ctrl+S to save</span>
            <button
              onClick={onClose}
              className="px-4 py-2 text-sm border border-gray-300 rounded hover:bg-gray-100 transition-colors"
            >
              Cancel
            </button>
            <button
              onClick={handleSave}
              className="px-4 py-2 text-sm bg-gradient-to-r from-green-500 to-green-600 text-white rounded hover:from-green-600 hover:to-green-700 flex items-center"
            >
              <Save className="w-4 h-4 mr-2" />
              Save & Compile
            </button>
          </div>
        </div>
      </motion.div>
    </div>
  );
};

export default JoinEditor;