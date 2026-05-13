// src/components/Editor/ReplaceListEditor.tsx
import React, { useState, useEffect, useCallback } from 'react';
import { ReplaceComponentConfiguration, ReplaceRule } from '../../../types/unified-pipeline.types';
import { SchemaDefinition } from '../../../types/metadata';

// Assume these UI components exist (shadcn/ui or custom)
import { Button } from '../../ui/Button';
import { Badge } from '../../ui/badge';
import { Input } from '../../ui/input';
import { Label } from '../../ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../ui/select';
import { ChevronUp, ChevronDown, Trash2, Plus } from 'lucide-react';

interface ReplaceListEditorProps {
  nodeId: string;
  initialConfig?: ReplaceComponentConfiguration;
  inputSchema: SchemaDefinition;        // resolved from upstream connections
  onSave: (config: ReplaceComponentConfiguration) => void;
  onClose: () => void;
}

// ----------------------------------------------------------------------
// Rule List Item (with up/down buttons and delete)
// ----------------------------------------------------------------------
interface RuleListItemProps {
  rule: ReplaceRule;
  isSelected: boolean;
  onSelect: () => void;
  onMoveUp: () => void;
  onMoveDown: () => void;
  onDelete: () => void;
  canMoveUp: boolean;
  canMoveDown: boolean;
}

const RuleListItem: React.FC<RuleListItemProps> = ({
  rule,
  isSelected,
  onSelect,
  onMoveUp,
  onMoveDown,
  onDelete,
  canMoveUp,
  canMoveDown,
}) => {
  return (
    <div
      className={`flex items-center justify-between p-2 mb-1 rounded cursor-pointer border transition-colors ${
        isSelected
          ? 'bg-blue-50 border-blue-300'
          : 'bg-white border-gray-200 hover:bg-gray-50'
      }`}
      onClick={onSelect}
    >
      <div className="flex-1 min-w-0">
        <div className="flex items-center space-x-2">
          <span className="text-xs font-mono text-gray-500 w-6 text-right">
            {rule.position + 1}.
          </span>
          <span className="text-sm font-medium truncate">{rule.column}</span>
        </div>
        <div className="flex items-center space-x-2 text-xs text-gray-500">
          <span className="truncate max-w-[100px]">{rule.searchValue || '""'}</span>
          <span>→</span>
          <span className="truncate max-w-[100px]">{rule.replacement || '""'}</span>
        </div>
      </div>
      <div className="flex items-center space-x-1 ml-2">
        {rule.caseSensitive && (
          <span className="text-xs bg-gray-200 px-1 rounded">Aa</span>
        )}
        {rule.regex && (
          <span className="text-xs bg-purple-100 text-purple-700 px-1 rounded">.*</span>
        )}
        <button
          onClick={(e) => { e.stopPropagation(); onMoveUp(); }}
          disabled={!canMoveUp}
          className={`p-1 rounded hover:bg-gray-200 ${!canMoveUp && 'opacity-30 cursor-not-allowed'}`}
          title="Move up"
        >
          <ChevronUp className="h-4 w-4" />
        </button>
        <button
          onClick={(e) => { e.stopPropagation(); onMoveDown(); }}
          disabled={!canMoveDown}
          className={`p-1 rounded hover:bg-gray-200 ${!canMoveDown && 'opacity-30 cursor-not-allowed'}`}
          title="Move down"
        >
          <ChevronDown className="h-4 w-4" />
        </button>
        <button
          onClick={(e) => { e.stopPropagation(); onDelete(); }}
          className="p-1 rounded hover:bg-red-100 text-red-600"
          title="Delete rule"
        >
          <Trash2 className="h-4 w-4" />
        </button>
      </div>
    </div>
  );
};

// ----------------------------------------------------------------------
// Rule Editor (uses native checkboxes instead of ui/checkbox)
// ----------------------------------------------------------------------
interface RuleEditorProps {
  rule: ReplaceRule;
  columns: string[];
  onChange: (updates: Partial<ReplaceRule>) => void;
}

const RuleEditor: React.FC<RuleEditorProps> = ({ rule, columns, onChange }) => {
  return (
    <div className="space-y-4 p-2">
      <h3 className="font-medium text-sm">Edit Rule #{rule.position + 1}</h3>

      {/* Column selection */}
      <div className="space-y-2">
        <Label htmlFor="column">Column</Label>
        <Select
          value={rule.column}
          onValueChange={(value) => onChange({ column: value })}
        >
          <SelectTrigger id="column" className="w-full">
            <SelectValue placeholder="Select column" />
          </SelectTrigger>
          <SelectContent>
            {columns.map((col) => (
              <SelectItem key={col} value={col}>{col}</SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      {/* Search value */}
      <div className="space-y-2">
        <Label htmlFor="searchValue">Search value</Label>
        <Input
          id="searchValue"
          value={rule.searchValue}
          onChange={(e) => onChange({ searchValue: e.target.value })}
          placeholder="Text or regex pattern"
        />
      </div>

      {/* Replacement value */}
      <div className="space-y-2">
        <Label htmlFor="replacement">Replace with</Label>
        <Input
          id="replacement"
          value={rule.replacement}
          onChange={(e) => onChange({ replacement: e.target.value })}
          placeholder="Replacement text"
        />
      </div>

      {/* Options - using native checkboxes */}
      <div className="flex items-center space-x-6">
        <div className="flex items-center space-x-2">
          <input
            type="checkbox"
            id="caseSensitive"
            checked={rule.caseSensitive}
            onChange={(e) => onChange({ caseSensitive: e.target.checked })}
            className="h-4 w-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
          />
          <Label htmlFor="caseSensitive" className="text-sm">Case sensitive</Label>
        </div>
        <div className="flex items-center space-x-2">
          <input
            type="checkbox"
            id="regex"
            checked={rule.regex}
            onChange={(e) => onChange({ regex: e.target.checked })}
            className="h-4 w-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
          />
          <Label htmlFor="regex" className="text-sm">Regular expression</Label>
        </div>
      </div>

      {/* Scope */}
      <div className="space-y-2">
        <Label htmlFor="scope">Scope</Label>
        <Select
          value={rule.scope}
          onValueChange={(value: 'all' | 'first' | 'last') => onChange({ scope: value })}
        >
          <SelectTrigger id="scope" className="w-full">
            <SelectValue placeholder="Select scope" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All occurrences</SelectItem>
            <SelectItem value="first">First occurrence only</SelectItem>
            <SelectItem value="last">Last occurrence only</SelectItem>
          </SelectContent>
        </Select>
      </div>
    </div>
  );
};

// ----------------------------------------------------------------------
// Global Options Editor (also uses native checkbox)
// ----------------------------------------------------------------------
interface GlobalOptions {
  errorHandling?: 'fail' | 'skip' | 'default';
  emptyValueHandling?: 'skip' | 'default' | 'null';
  parallelization?: boolean;
  maxThreads?: number;
  batchSize?: number;
}

interface GlobalOptionsEditorProps {
  value: GlobalOptions;
  onChange: (options: GlobalOptions) => void;
}

const GlobalOptionsEditor: React.FC<GlobalOptionsEditorProps> = ({ value, onChange }) => {
  const update = (key: keyof GlobalOptions, val: any) => {
    onChange({ ...value, [key]: val });
  };

  return (
    <div className="grid grid-cols-2 gap-4 p-2">
      <div className="space-y-2">
        <Label htmlFor="errorHandling">Error handling</Label>
        <Select
          value={value.errorHandling || 'fail'}
          onValueChange={(val: any) => update('errorHandling', val)}
        >
          <SelectTrigger id="errorHandling">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="fail">Fail job</SelectItem>
            <SelectItem value="skip">Skip row</SelectItem>
            <SelectItem value="default">Use default value</SelectItem>
          </SelectContent>
        </Select>
      </div>

      <div className="space-y-2">
        <Label htmlFor="emptyHandling">Empty value handling</Label>
        <Select
          value={value.emptyValueHandling || 'skip'}
          onValueChange={(val: any) => update('emptyValueHandling', val)}
        >
          <SelectTrigger id="emptyHandling">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="skip">Skip row</SelectItem>
            <SelectItem value="default">Replace with default</SelectItem>
            <SelectItem value="null">Leave as null</SelectItem>
          </SelectContent>
        </Select>
      </div>

      <div className="flex items-center space-x-2">
        <input
          type="checkbox"
          id="parallel"
          checked={value.parallelization || false}
          onChange={(e) => update('parallelization', e.target.checked)}
          className="h-4 w-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
        />
        <Label htmlFor="parallel">Enable parallelization</Label>
      </div>

      {value.parallelization && (
        <div className="space-y-2">
          <Label htmlFor="maxThreads">Max threads</Label>
          <Input
            id="maxThreads"
            type="number"
            min="1"
            max="16"
            value={value.maxThreads || 4}
            onChange={(e) => update('maxThreads', parseInt(e.target.value) || 4)}
          />
        </div>
      )}

      <div className="space-y-2 col-span-2">
        <Label htmlFor="batchSize">Batch size</Label>
        <Input
          id="batchSize"
          type="number"
          min="100"
          max="10000"
          step="100"
          value={value.batchSize || 1000}
          onChange={(e) => update('batchSize', parseInt(e.target.value) || 1000)}
        />
      </div>
    </div>
  );
};

// ----------------------------------------------------------------------
// Preview Panel (simplified)
// ----------------------------------------------------------------------
interface PreviewPanelProps {
  inputSchema: SchemaDefinition;
  rules: ReplaceRule[];
}

const PreviewPanel: React.FC<PreviewPanelProps> = ({ inputSchema: _inputSchema, rules: _rules }) => {
  // In a real implementation, you might fetch a few sample rows from the canvas.
  // Here we just show a placeholder.
  return (
    <div className="mt-4 p-3 bg-gray-50 rounded border">
      <h4 className="text-sm font-medium mb-2">Preview (sample data)</h4>
      <div className="text-xs text-gray-500 italic">
        Sample data would appear here, showing the effect of rules on actual rows.
        <br />
        (Requires upstream data – not available in this mockup.)
      </div>
      <div className="mt-2 grid grid-cols-2 gap-2 text-xs">
        <div className="p-2 bg-white rounded border">Original: "john@old.com"</div>
        <div className="p-2 bg-white rounded border">After rules: "john@new.com"</div>
      </div>
    </div>
  );
};

// ----------------------------------------------------------------------
// Main Component
// ----------------------------------------------------------------------
const ReplaceListEditor: React.FC<ReplaceListEditorProps> = ({
  nodeId: _nodeId,
  initialConfig,
  inputSchema,
  onSave,
  onClose,
}) => {
  const [rules, setRules] = useState<ReplaceRule[]>([]);
  const [globalOptions, setGlobalOptions] = useState<GlobalOptions>({});
  const [selectedRuleId, setSelectedRuleId] = useState<string | null>(null);
  const [validation, setValidation] = useState<{ isValid: boolean; warnings: string[] }>({
    isValid: true,
    warnings: [],
  });

  // Initialize from initialConfig
  useEffect(() => {
    if (initialConfig) {
      setRules(initialConfig.rules.map((r, idx) => ({ ...r, position: idx })));
      setGlobalOptions(initialConfig.globalOptions || {});
      if (initialConfig.rules.length > 0) {
        setSelectedRuleId(initialConfig.rules[0].id);
      }
    } else {
      // Add a default rule
      addRule();
    }
  }, [initialConfig]);

  // Validation function
  const validate = useCallback(() => {
    const warnings: string[] = [];
    // Check for duplicate columns with overlapping patterns? (simplified)
    const columnSet = new Set<string>();
    rules.forEach((rule) => {
      if (!rule.column) {
        warnings.push(`Rule #${rule.position + 1}: no column selected`);
      } else {
        columnSet.add(rule.column);
      }
      if (rule.regex) {
        try {
          new RegExp(rule.searchValue);
        } catch {
          warnings.push(`Rule #${rule.position + 1}: invalid regular expression`);
        }
      }
    });
    setValidation({ isValid: warnings.length === 0, warnings });
  }, [rules]);

  useEffect(() => {
    validate();
  }, [rules, validate]);

  // Rule management
  const addRule = () => {
    const newRule: ReplaceRule = {
      id: `rule-${Date.now()}-${Math.random().toString(36).substr(2, 6)}`,
      column: inputSchema.fields[0]?.name || '',
      searchValue: '',
      replacement: '',
      caseSensitive: false,
      regex: false,
      scope: 'all',
      position: rules.length,
    };
    setRules([...rules, newRule]);
    setSelectedRuleId(newRule.id);
  };

  const updateRule = (id: string, updates: Partial<ReplaceRule>) => {
    setRules(rules.map(r => (r.id === id ? { ...r, ...updates } : r)));
  };

  const deleteRule = (id: string) => {
    const filtered = rules.filter(r => r.id !== id);
    const reindexed = filtered.map((r, idx) => ({ ...r, position: idx }));
    setRules(reindexed);
    if (selectedRuleId === id) {
      setSelectedRuleId(reindexed[0]?.id || null);
    }
  };

  const moveRule = (id: string, direction: 'up' | 'down') => {
    const index = rules.findIndex(r => r.id === id);
    if (index === -1) return;
    const newIndex = direction === 'up' ? index - 1 : index + 1;
    if (newIndex < 0 || newIndex >= rules.length) return;

    const newRules = [...rules];
    const [moved] = newRules.splice(index, 1);
    newRules.splice(newIndex, 0, moved);
    const reindexed = newRules.map((r, idx) => ({ ...r, position: idx }));
    setRules(reindexed);
  };

  const handleSave = () => {
    const config: ReplaceComponentConfiguration = {
      version: '1.0',
      rules,
      globalOptions,
      outputSchema: inputSchema, // You may want to clone or enrich
      sqlGeneration: {
        requiresRegex: rules.some(r => r.regex),
        estimatedRowMultiplier: 1.0,
      },
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        createdBy: 'user', // could come from auth context
        ruleCount: rules.length,
        validationStatus: validation.isValid ? 'VALID' : 'WARNING',
        warnings: validation.warnings,
        dependencies: rules.map(r => r.column),
      },
    };
    onSave(config);
    onClose();
  };

  const selectedRule = rules.find(r => r.id === selectedRuleId);

  // Available column names from input schema
  const columnNames = inputSchema.fields.map(f => f.name);

  return (
    <div className="fixed inset-0 bg-black bg-opacity-80 flex items-center justify-center z-[10000]">
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-5xl h-5/6 flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b">
          <div className="flex items-center space-x-3">
            <span className="text-2xl">🔁</span>
            <h2 className="text-lg font-bold text-gray-800">tReplaceList Configuration</h2>
            <Badge variant="outline" className="bg-blue-50">
              {rules.length} rule{rules.length !== 1 && 's'}
            </Badge>
            {!validation.isValid && (
              <Badge variant="destructive" className="bg-yellow-100 text-yellow-800 border-yellow-300">
                ⚠️ Warnings
              </Badge>
            )}
          </div>
          <div className="flex space-x-2">
            <Button variant="ghost" onClick={onClose}>
              Cancel
            </Button>
            <Button onClick={handleSave} disabled={!validation.isValid}>
              Save & Close
            </Button>
          </div>
        </div>

        {/* Main content: two columns */}
        <div className="flex flex-1 overflow-hidden">
          {/* Left: Rule list */}
          <div className="w-1/3 border-r p-4 overflow-y-auto bg-gray-50">
            <div className="flex items-center justify-between mb-4">
              <h3 className="font-semibold text-sm">Replacement Rules</h3>
              <Button size="sm" onClick={addRule} className="h-8 px-2">
                <Plus className="h-4 w-4 mr-1" /> Add Rule
              </Button>
            </div>
            <div className="space-y-1">
              {rules.map((rule, idx) => (
                <RuleListItem
                  key={rule.id}
                  rule={rule}
                  isSelected={rule.id === selectedRuleId}
                  onSelect={() => setSelectedRuleId(rule.id)}
                  onMoveUp={() => moveRule(rule.id, 'up')}
                  onMoveDown={() => moveRule(rule.id, 'down')}
                  onDelete={() => deleteRule(rule.id)}
                  canMoveUp={idx > 0}
                  canMoveDown={idx < rules.length - 1}
                />
              ))}
              {rules.length === 0 && (
                <div className="text-center text-gray-400 py-8">
                  No rules defined. Click "Add Rule" to start.
                </div>
              )}
            </div>
          </div>

          {/* Right: Rule editor */}
          <div className="w-2/3 p-6 overflow-y-auto">
            {selectedRule ? (
              <RuleEditor
                rule={selectedRule}
                columns={columnNames}
                onChange={(updates) => updateRule(selectedRule.id, updates)}
              />
            ) : (
              <div className="h-full flex items-center justify-center text-gray-400">
                Select a rule from the left to edit its properties
              </div>
            )}
          </div>
        </div>

        {/* Bottom panel: Global Options & Preview */}
        <div className="border-t p-4 bg-gray-50">
          <details className="group" open>
            <summary className="text-sm font-semibold cursor-pointer list-none flex items-center">
              <span className="mr-2">⚙️</span> Global Options
              <ChevronDown className="h-4 w-4 ml-2 group-open:rotate-180 transition-transform" />
            </summary>
            <div className="mt-3">
              <GlobalOptionsEditor value={globalOptions} onChange={setGlobalOptions} />
            </div>
          </details>

          <PreviewPanel inputSchema={inputSchema} rules={rules} />
        </div>
      </div>
    </div>
  );
};

export default ReplaceListEditor;