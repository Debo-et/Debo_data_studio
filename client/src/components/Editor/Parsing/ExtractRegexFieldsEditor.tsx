// src/components/Editor/ExtractRegexFieldsEditor.tsx
import React, { useState, useEffect, useCallback } from 'react';
import { motion } from 'framer-motion';
import {
  ExtractRegexFieldsConfiguration,
  ExtractRegexRule,
  DataType,
  SchemaDefinition,
} from '../../../types/unified-pipeline.types';
import { Button } from '../../ui/Button';
import { Input } from '../../ui/input';
import { Label } from '../../ui/label';
import { Textarea } from '../../ui/textarea';
import { Select } from '../../ui/select';
import { Switch } from '../../ui/switch';
import { Card, CardContent, CardHeader, CardTitle } from '../../ui/card';
import { Badge } from '../../ui/badge';
import { X, RefreshCw, AlertCircle } from 'lucide-react';

// Helper to generate unique IDs
const generateId = (prefix: string = 'rule'): string => {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
};

// Helper to count capturing groups in a regex pattern
const countCapturingGroups = (pattern: string): number => {
  if (!pattern) return 0;
  let count = 0;
  let inClass = false;
  for (let i = 0; i < pattern.length; i++) {
    const ch = pattern[i];
    const next = pattern[i + 1];
    if (ch === '\\') {
      i++; // skip escaped char
      continue;
    }
    if (ch === '[') {
      inClass = true;
      continue;
    }
    if (ch === ']') {
      inClass = false;
      continue;
    }
    if (!inClass && ch === '(' && next !== '?') {
      count++;
    }
  }
  return count;
};

// Helper to convert DataType to PostgreSQL type string (for preview)

// Preview row type
interface PreviewRow {
  original: string;
  extracted: (string | null)[];
  errors?: string[];
}

interface ExtractRegexFieldsEditorProps {
  nodeId: string;
  nodeMetadata: any; // CanvasNodeData
  inputColumns: Array<{ name: string; type?: string }>;
  initialConfig?: ExtractRegexFieldsConfiguration;
  onClose: () => void;
  onSave: (config: ExtractRegexFieldsConfiguration) => void;
}

export const ExtractRegexFieldsEditor: React.FC<ExtractRegexFieldsEditorProps> = ({
  nodeId,
  inputColumns,
  initialConfig,
  onClose,
  onSave,
}) => {
  // ---------- State ----------
  const [activeTab, setActiveTab] = useState<'basic' | 'columns' | 'preview' | 'advanced'>('basic');
  
  // Form state – matches configuration structure
  const [sourceColumn, setSourceColumn] = useState(initialConfig?.sourceColumn || '');
  const [regexPattern, setRegexPattern] = useState(initialConfig?.regexPattern || '');
  const [caseInsensitive, setCaseInsensitive] = useState(initialConfig?.caseInsensitive || false);
  const [multiline, setMultiline] = useState(initialConfig?.multiline || false);
  const [dotAll, setDotAll] = useState(initialConfig?.dotAll || false);
  const [rules, setRules] = useState<ExtractRegexRule[]>(initialConfig?.rules || []);
  const [errorHandling, setErrorHandling] = useState({
    onNoMatch: initialConfig?.errorHandling?.onNoMatch || 'fail',
    onConversionError: initialConfig?.errorHandling?.onConversionError || 'fail',
  });
  const [parallelization, setParallelization] = useState(initialConfig?.parallelization || false);
  const [batchSize, setBatchSize] = useState(initialConfig?.batchSize || 1000);

  // Derived state
  const [groupCount, setGroupCount] = useState(0);
  const [regexError, setRegexError] = useState<string | null>(null);
  const [previewRows, setPreviewRows] = useState<PreviewRow[]>([]);
  const [isPreviewLoading, setIsPreviewLoading] = useState(false);

  // Update group count when regex changes
  useEffect(() => {
    try {
      // Validate regex syntax
      new RegExp(regexPattern, caseInsensitive ? 'i' : '');
      setRegexError(null);
      const count = countCapturingGroups(regexPattern);
      setGroupCount(count);
    } catch (e: any) {
      setRegexError(e.message);
      setGroupCount(0);
    }
  }, [regexPattern, caseInsensitive]);

  // Auto-sync rules with group count
  useEffect(() => {
    if (groupCount === 0) {
      setRules([]);
      return;
    }
    setRules(prev => {
      // If we have more groups than rules, add default rules
      const newRules: ExtractRegexRule[] = [];
      for (let i = 1; i <= groupCount; i++) {
        const existing = prev.find(r => r.groupIndex === i);
        if (existing) {
          newRules.push(existing);
        } else {
          newRules.push({
            id: generateId('rule'),
            groupIndex: i,
            columnName: `group${i}`,
            dataType: 'STRING',
            nullable: true,
            position: i - 1,
          });
        }
      }
      return newRules.sort((a, b) => a.groupIndex - b.groupIndex);
    });
  }, [groupCount]);

  // ---------- Handlers ----------
  const updateRule = useCallback((index: number, updates: Partial<ExtractRegexRule>) => {
    setRules(prev => prev.map((r, i) => i === index ? { ...r, ...updates } : r));
  }, []);

  const removeRule = useCallback((index: number) => {
    setRules(prev => prev.filter((_, i) => i !== index));
  }, []);

  const autoGenerateNames = useCallback(() => {
    setRules(prev => prev.map(r => ({
      ...r,
      columnName: `group${r.groupIndex}`,
    })));
  }, []);

  const validate = (): string[] => {
    const errors: string[] = [];
    if (!sourceColumn) errors.push('Source column is required.');
    if (!regexPattern) errors.push('Regular expression is required.');
    if (regexError) errors.push(`Invalid regex: ${regexError}`);
    if (rules.length === 0) errors.push('At least one capturing group is required.');
    const names = rules.map(r => r.columnName);
    const uniqueNames = new Set(names);
    if (names.length !== uniqueNames.size) errors.push('Column names must be unique.');
    return errors;
  };

  const buildConfig = (): ExtractRegexFieldsConfiguration => {
    const outputSchema: SchemaDefinition = {
      id: `${nodeId}_output_schema`,
      name: 'ExtractRegexFields Output',
      fields: rules.map((rule) => ({
        id: rule.id,
        name: rule.columnName,
        type: rule.dataType,
        length: rule.length,
        precision: rule.precision,
        scale: rule.scale,
        nullable: rule.nullable,
        isKey: false,
        defaultValue: rule.defaultValue,
        description: `Extracted from ${sourceColumn} via regex group ${rule.groupIndex}`,
      })),
      isTemporary: false,
      isMaterialized: false,
    };

    return {
      version: '1.0',
      sourceColumn,
      regexPattern,
      caseInsensitive,
      multiline,
      dotAll,
      rules,
      errorHandling,
      parallelization,
      batchSize: parallelization ? batchSize : undefined,
      outputSchema,
      sqlGeneration: {
        canPushDown: true, // optimistic; actual determination by compiler
        estimatedRowMultiplier: 1.0,
      },
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        createdBy: 'extract-regex-editor',
        validationStatus: 'VALID',
        warnings: [],
        dependencies: [sourceColumn],
        compiledSql: undefined,
      },
    };
  };

  const handleSave = () => {
    const errors = validate();
    if (errors.length > 0) {
      alert(`Validation errors:\n- ${errors.join('\n- ')}`);
      return;
    }
    onSave(buildConfig());
  };

  // Preview (simulated – in real app, would fetch sample data)
  const loadPreview = useCallback(async () => {
    setIsPreviewLoading(true);
    // Simulate sample data
    const sampleRows = [
      '2025-03-18 INFO User logged in',
      '2025-03-18 ERROR Failed to connect',
      'Invalid line without date',
    ];
    const preview: PreviewRow[] = sampleRows.map(row => {
      try {
        const regex = new RegExp(regexPattern, [
          caseInsensitive ? 'i' : '',
          multiline ? 'm' : '',
          dotAll ? 's' : '',
        ].join(''));
        const match = regex.exec(row);
        if (!match) {
          return { original: row, extracted: [], errors: ['No match'] };
        }
        // match[0] is full match, groups start at index 1
        const extracted = match.slice(1).map(g => g ?? null);
        return { original: row, extracted };
      } catch (e: any) {
        return { original: row, extracted: [], errors: [e.message] };
      }
    });
    setPreviewRows(preview);
    setIsPreviewLoading(false);
  }, [regexPattern, caseInsensitive, multiline, dotAll]);

  // ---------- Render tabs ----------
  const renderBasicTab = () => (
    <div className="space-y-6">
      <div className="space-y-2">
        <Label>Source Column</Label>
        <Select
          value={sourceColumn}
          onValueChange={setSourceColumn}
        >
          {inputColumns
            .filter(col => col.type?.toLowerCase().includes('char') || col.type?.toLowerCase().includes('text') || col.type === 'STRING')
            .map(col => (
              <option key={col.name} value={col.name}>{col.name} ({col.type || 'string'})</option>
            ))}
        </Select>
        <p className="text-xs text-gray-500">Only string/text columns are shown.</p>
      </div>

      <div className="space-y-2">
        <Label htmlFor="regex">Regular Expression</Label>
        <Textarea
          id="regex"
          value={regexPattern}
          onChange={(e) => setRegexPattern(e.target.value)}
          placeholder="e.g., (\d{4})-(\d{2})-(\d{2}) (\w+) (.*)"
          rows={4}
          className="font-mono text-sm"
        />
        {regexError && (
          <div className="flex items-center text-red-600 text-xs mt-1">
            <AlertCircle className="h-3 w-3 mr-1" />
            {regexError}
          </div>
        )}
        <p className="text-xs text-gray-500">
          Use capturing groups <span className="font-mono bg-gray-100 px-1">(…)</span>. Each group becomes a column.
        </p>
      </div>

      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm">Match Options</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="flex items-center justify-between">
            <Label htmlFor="case-insensitive" className="cursor-pointer">Case insensitive</Label>
            <Switch
              id="case-insensitive"
              checked={caseInsensitive}
              onCheckedChange={setCaseInsensitive}
            />
          </div>
          <div className="flex items-center justify-between">
            <Label htmlFor="multiline" className="cursor-pointer">Multiline mode (^ and $ match line starts/ends)</Label>
            <Switch
              id="multiline"
              checked={multiline}
              onCheckedChange={setMultiline}
            />
          </div>
          <div className="flex items-center justify-between">
            <Label htmlFor="dot-all" className="cursor-pointer">Dot matches newline</Label>
            <Switch
              id="dot-all"
              checked={dotAll}
              onCheckedChange={setDotAll}
            />
          </div>
        </CardContent>
      </Card>

      <div className="bg-blue-50 p-3 rounded-lg border border-blue-200">
        <div className="flex items-center text-sm text-blue-800">
          <span className="font-medium mr-2">Detected capturing groups:</span>
          <Badge variant="secondary" className="bg-blue-200 text-blue-800">{groupCount}</Badge>
        </div>
      </div>
    </div>
  );

  const renderColumnsTab = () => (
    <div className="space-y-4">
      <div className="flex justify-between items-center">
        <h3 className="text-sm font-medium">Output Columns (one per capturing group)</h3>
        <Button size="sm" variant="outline" onClick={autoGenerateNames}>
          Auto-generate names
        </Button>
      </div>
      <div className="border rounded-lg overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-gray-50 border-b">
            <tr>
              <th className="px-4 py-2 text-left">Group</th>
              <th className="px-4 py-2 text-left">Column Name</th>
              <th className="px-4 py-2 text-left">Data Type</th>
              <th className="px-4 py-2 text-left">Length/Precision</th>
              <th className="px-4 py-2 text-left">Nullable</th>
              <th className="px-4 py-2 text-left">Default</th>
              <th className="px-4 py-2 text-center w-10"></th>
            </tr>
          </thead>
          <tbody>
            {rules.map((rule, idx) => (
              <tr key={rule.id} className="border-b hover:bg-gray-50">
                <td className="px-4 py-2 font-mono text-gray-500">#{rule.groupIndex}</td>
                <td className="px-4 py-2">
                  <Input
                    value={rule.columnName}
                    onChange={(e) => updateRule(idx, { columnName: e.target.value })}
                    className="h-8"
                    placeholder="column_name"
                  />
                </td>
                <td className="px-4 py-2">
                  <Label className="sr-only">Data Type</Label>
                  <Select
                    value={rule.dataType}
                    onValueChange={(val: DataType) => updateRule(idx, { dataType: val })}
                  >
                    <option value="STRING">STRING</option>
                    <option value="INTEGER">INTEGER</option>
                    <option value="DECIMAL">DECIMAL</option>
                    <option value="DATE">DATE</option>
                    <option value="BOOLEAN">BOOLEAN</option>
                    <option value="TIMESTAMP">TIMESTAMP</option>
                    <option value="BINARY">BINARY</option>
                  </Select>
                </td>
                <td className="px-4 py-2">
                  {rule.dataType === 'STRING' && (
                    <Input
                      type="number"
                      value={rule.length || ''}
                      onChange={(e) => updateRule(idx, { length: e.target.value ? parseInt(e.target.value) : undefined })}
                      className="h-8 w-20"
                      placeholder="Len"
                    />
                  )}
                  {rule.dataType === 'DECIMAL' && (
                    <div className="flex space-x-1">
                      <Input
                        type="number"
                        value={rule.precision || ''}
                        onChange={(e) => updateRule(idx, { precision: e.target.value ? parseInt(e.target.value) : undefined })}
                        className="h-8 w-16"
                        placeholder="Prec"
                      />
                      <Input
                        type="number"
                        value={rule.scale || ''}
                        onChange={(e) => updateRule(idx, { scale: e.target.value ? parseInt(e.target.value) : undefined })}
                        className="h-8 w-16"
                        placeholder="Scale"
                      />
                    </div>
                  )}
                </td>
                <td className="px-4 py-2">
                  <Switch
                    checked={rule.nullable}
                    onCheckedChange={(val) => updateRule(idx, { nullable: val })}
                  />
                </td>
                <td className="px-4 py-2">
                  <Input
                    value={rule.defaultValue || ''}
                    onChange={(e) => updateRule(idx, { defaultValue: e.target.value || undefined })}
                    className="h-8"
                    placeholder="(optional)"
                  />
                </td>
                <td className="px-4 py-2 text-center">
                  <button
                    onClick={() => removeRule(idx)}
                    className="text-red-500 hover:text-red-700"
                    disabled={rules.length <= 1}
                  >
                    <X className="h-4 w-4" />
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="text-xs text-gray-500">
        Column order follows group order. Changes to regex will automatically update groups.
      </p>
    </div>
  );

  const renderPreviewTab = () => (
    <div className="space-y-4">
      <div className="flex justify-between items-center">
        <h3 className="text-sm font-medium">Sample Extraction Preview</h3>
        <Button size="sm" variant="outline" onClick={loadPreview} disabled={isPreviewLoading}>
          <RefreshCw className={`h-4 w-4 mr-2 ${isPreviewLoading ? 'animate-spin' : ''}`} />
          Refresh
        </Button>
      </div>
      {previewRows.length === 0 ? (
        <div className="text-center py-8 text-gray-500 border rounded-lg">
          Click Refresh to load sample preview (simulated).
        </div>
      ) : (
        <div className="border rounded-lg overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 border-b">
              <tr>
                <th className="px-4 py-2 text-left">Original</th>
                {rules.map((rule) => (
                  <th key={rule.id} className="px-4 py-2 text-left">{rule.columnName}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {previewRows.map((row, i) => (
                <tr key={i} className="border-b hover:bg-gray-50">
                  <td className="px-4 py-2 font-mono text-xs">{row.original}</td>
                  {row.errors ? (
                    <td colSpan={rules.length} className="px-4 py-2 text-red-500 text-xs">
                      {row.errors.join(', ')}
                    </td>
                  ) : (
                    rules.map((rule, j) => (
                      <td key={rule.id} className="px-4 py-2 font-mono text-xs">
                        {row.extracted[j] !== undefined ? row.extracted[j] : <span className="text-gray-400">NULL</span>}
                      </td>
                    ))
                  )}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );

  const renderAdvancedTab = () => (
    <div className="space-y-6">
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm">Error Handling</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-2">
            <Label>On no match (row does not match regex)</Label>
            <Select
              value={errorHandling.onNoMatch}
              onValueChange={(val: any) => setErrorHandling(prev => ({ ...prev, onNoMatch: val }))}
            >
              <option value="fail">Fail job</option>
              <option value="skipRow">Skip row (output NULLs)</option>
              <option value="useDefault">Use default values</option>
            </Select>
          </div>
          <div className="space-y-2">
            <Label>On conversion error (e.g., cannot cast to target type)</Label>
            <Select
              value={errorHandling.onConversionError}
              onValueChange={(val: any) => setErrorHandling(prev => ({ ...prev, onConversionError: val }))}
            >
              <option value="fail">Fail job</option>
              <option value="skipRow">Skip row (set NULL)</option>
              <option value="setNull">Set NULL</option>
            </Select>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm">Performance</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex items-center justify-between">
            <Label htmlFor="parallel">Parallel execution</Label>
            <Switch
              id="parallel"
              checked={parallelization}
              onCheckedChange={setParallelization}
            />
          </div>
          {parallelization && (
            <div className="space-y-2">
              <Label htmlFor="batch-size">Batch size</Label>
              <Input
                id="batch-size"
                type="number"
                value={batchSize}
                onChange={(e) => setBatchSize(parseInt(e.target.value) || 1000)}
                min={1}
                step={100}
              />
            </div>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm">SQL Generation Hints</CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-xs text-gray-600">
            The compiler will attempt to push regex extraction to PostgreSQL using
            <span className="font-mono bg-gray-100 px-1 mx-1">regexp_matches()</span>.
          </p>
        </CardContent>
      </Card>
    </div>
  );

  return (
    <div className="fixed inset-0 z-[9999] bg-black bg-opacity-70 flex items-center justify-center p-4">
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        className="bg-white rounded-xl shadow-2xl w-full max-w-6xl h-[90vh] flex flex-col overflow-hidden"
      >
        {/* Header */}
        <div className="flex justify-between items-center p-6 border-b bg-gradient-to-r from-purple-50 to-gray-50">
          <div>
            <h2 className="text-xl font-bold flex items-center">
              <span className="mr-2">🔍</span>
              tExtractRegexFields Configuration
            </h2>
            <p className="text-sm text-gray-600 mt-1">
              Extract multiple fields using regular expression capturing groups.
            </p>
          </div>
          <button
            onClick={onClose}
            className="p-2 hover:bg-gray-100 rounded-full transition-colors"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        {/* Tabs */}
        <div className="flex border-b px-6">
          {(['basic', 'columns', 'preview', 'advanced'] as const).map(tab => (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              className={`px-4 py-2 text-sm font-medium capitalize ${
                activeTab === tab
                  ? 'text-purple-700 border-b-2 border-purple-700'
                  : 'text-gray-500 hover:text-gray-700'
              }`}
            >
              {tab}
            </button>
          ))}
        </div>

        {/* Content Area */}
        <div className="flex-1 overflow-y-auto p-6">
          {activeTab === 'basic' && renderBasicTab()}
          {activeTab === 'columns' && renderColumnsTab()}
          {activeTab === 'preview' && renderPreviewTab()}
          {activeTab === 'advanced' && renderAdvancedTab()}
        </div>

        {/* Footer */}
        <div className="flex justify-end items-center p-4 border-t bg-gray-50 space-x-3">
          <div className="flex-1 text-xs text-gray-500">
            {rules.length} output column{rules.length !== 1 ? 's' : ''}
          </div>
          <Button variant="outline" onClick={onClose}>
            Cancel
          </Button>
          <Button
            variant="default"
            className="bg-gradient-to-r from-purple-600 to-purple-700 hover:from-purple-700 hover:to-purple-800"
            onClick={handleSave}
          >
            Save Configuration
          </Button>
        </div>
      </motion.div>
    </div>
  );
};