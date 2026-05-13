// RuleGrid.tsx
import React, { useState } from 'react';
import { ChevronDown, ChevronRight, Edit2, HelpCircle, Copy } from 'lucide-react';

// Import types from types.ts
import {
  Rule,
  LookupReference,
  SchemaField,
  OperationType,
  PatternType
} from '../../../types/types';

interface RuleGridProps {
  rules: Rule[];
  selectedRuleIds: string[];
  onSelectRule: (ids: string[]) => void;
  onUpdateRule: (ruleId: string, updates: Partial<Rule>) => void;
  onToggleRule: (ruleId: string) => void;
  onDuplicateRule: (ruleId: string) => void;
  schemaFields: SchemaField[];
  lookups: LookupReference[];
  onOpenPatternEditor: (ruleId: string) => void;
}

const operationOptions: OperationType[] = [
  'Replace',
  'Normalize',
  'Cleanse',
  'Format',
  'UPPERCASE',
  'lowercase',
  'Title Case',
  'Remove noise characters',
  'Abbreviation expansion',
  'Trim whitespace',
  'Remove duplicates',
  'Standardize date',
  'Standardize phone',
  'Standardize address'
];

const patternTypeOptions: PatternType[] = [
  'regex',
  'contains',
  'startsWith',
  'endsWith',
  'exactMatch',
  'dictionary',
  'custom'
];

const RuleGrid: React.FC<RuleGridProps> = ({
  rules,
  selectedRuleIds,
  onSelectRule,
  onUpdateRule,
  onToggleRule,
  onDuplicateRule,
  schemaFields,
  lookups,
  onOpenPatternEditor
}) => {
  const [expandedDescriptions, setExpandedDescriptions] = useState<Set<string>>(new Set());

  const toggleDescription = (ruleId: string) => {
    const newExpanded = new Set(expandedDescriptions);
    if (newExpanded.has(ruleId)) {
      newExpanded.delete(ruleId);
    } else {
      newExpanded.add(ruleId);
    }
    setExpandedDescriptions(newExpanded);
  };

  const handleSelectAll = (checked: boolean) => {
    if (checked) {
      onSelectRule(rules.map(rule => rule.id));
    } else {
      onSelectRule([]);
    }
  };

  const handleSelectRule = (ruleId: string, checked: boolean) => {
    if (checked) {
      onSelectRule([...selectedRuleIds, ruleId]);
    } else {
      onSelectRule(selectedRuleIds.filter(id => id !== ruleId));
    }
  };

  const getPriorityColor = (priority: number) => {
    if (priority >= 80) return 'bg-red-100 dark:bg-red-900/30 text-red-800 dark:text-red-300';
    if (priority >= 60) return 'bg-amber-100 dark:bg-amber-900/30 text-amber-800 dark:text-amber-300';
    if (priority >= 40) return 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-800 dark:text-yellow-300';
    if (priority >= 20) return 'bg-green-100 dark:bg-green-900/30 text-green-800 dark:text-green-300';
    return 'bg-blue-100 dark:bg-blue-900/30 text-blue-800 dark:text-blue-300';
  };

  return (
    <div className="w-full overflow-auto">
      <table className="w-full border-collapse">
        <thead className="bg-gray-50 dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700">
          <tr>
            <th className="p-3 w-12">
              <input
                type="checkbox"
                checked={rules.length > 0 && selectedRuleIds.length === rules.length}
                onChange={(e) => handleSelectAll(e.target.checked)}
                className="rounded border-gray-300 dark:border-gray-600"
              />
            </th>
            <th className="p-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
              Input Column
            </th>
            <th className="p-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
              Operation
            </th>
            <th className="p-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
              Match Pattern
            </th>
            <th className="p-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
              Replacement
            </th>
            <th className="p-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
              Lookup
            </th>
            <th className="p-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
              Priority
            </th>
            <th className="p-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
              Active
            </th>
            <th className="p-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
              Description
            </th>
            <th className="p-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
              Actions
            </th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-200 dark:divide-gray-700">
          {rules.map((rule) => (
            <tr 
              key={rule.id}
              className={`hover:bg-gray-50 dark:hover:bg-gray-800/50 ${
                selectedRuleIds.includes(rule.id) ? 'bg-blue-50 dark:bg-blue-900/20' : ''
              }`}
            >
              <td className="p-3">
                <input
                  type="checkbox"
                  checked={selectedRuleIds.includes(rule.id)}
                  onChange={(e) => handleSelectRule(rule.id, e.target.checked)}
                  className="rounded border-gray-300 dark:border-gray-600"
                />
              </td>
              
              {/* Input Column */}
              <td className="p-3">
                <select
                  value={rule.inputColumn}
                  onChange={(e) => onUpdateRule(rule.id, { inputColumn: e.target.value })}
                  className="w-full px-2 py-1 border border-gray-300 dark:border-gray-600 rounded bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
                >
                  <option value="">Select column</option>
                  {schemaFields.map((field) => (
                    <option key={field.name} value={field.name}>
                      {field.name} ({field.type})
                    </option>
                  ))}
                </select>
              </td>
              
              {/* Operation */}
              <td className="p-3">
                <select
                  value={rule.operation}
                  onChange={(e) => onUpdateRule(rule.id, { operation: e.target.value as OperationType })}
                  className="w-full px-2 py-1 border border-gray-300 dark:border-gray-600 rounded bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
                >
                  {operationOptions.map((op) => (
                    <option key={op} value={op}>
                      {op}
                    </option>
                  ))}
                </select>
              </td>
              
              {/* Match Pattern */}
              <td className="p-3">
                <div className="space-y-1">
                  <div className="flex items-center space-x-2">
                    <select
                      value={rule.patternType || 'regex'}
                      onChange={(e) => onUpdateRule(rule.id, { patternType: e.target.value as PatternType })}
                      className="px-2 py-1 border border-gray-300 dark:border-gray-600 rounded bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
                    >
                      {patternTypeOptions.map((type) => (
                        <option key={type} value={type}>
                          {type}
                        </option>
                      ))}
                    </select>
                    <button
                      onClick={() => onOpenPatternEditor(rule.id)}
                      className="p-1 hover:bg-gray-200 dark:hover:bg-gray-700 rounded"
                      title="Edit pattern"
                    >
                      <Edit2 className="w-4 h-4" />
                    </button>
                  </div>
                  <div>
                    <input
                      type="text"
                      value={rule.matchPattern}
                      onChange={(e) => onUpdateRule(rule.id, { matchPattern: e.target.value })}
                      placeholder="Enter pattern or click ... to edit"
                      className="w-full px-2 py-1 border border-gray-300 dark:border-gray-600 rounded bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm font-mono"
                    />
                  </div>
                </div>
              </td>
              
              {/* Replacement */}
              <td className="p-3">
                <input
                  type="text"
                  value={rule.replacement || ''}
                  onChange={(e) => onUpdateRule(rule.id, { replacement: e.target.value })}
                  placeholder="Replacement value or pattern"
                  className="w-full px-2 py-1 border border-gray-300 dark:border-gray-600 rounded bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
                />
                <div className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                  Use tokens: {'${COLUMN}'}
                </div>
              </td>
              
              {/* Lookup */}
              <td className="p-3">
                <select
                  value={rule.lookup?.id || ''}
                  onChange={(e) => {
                    const lookupId = e.target.value;
                    const lookup = lookups.find(l => l.id === lookupId);
                    onUpdateRule(rule.id, { lookup: lookup || undefined });
                  }}
                  className="w-full px-2 py-1 border border-gray-300 dark:border-gray-600 rounded bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
                >
                  <option value="">No lookup</option>
                  {lookups.map((lookup) => (
                    <option key={lookup.id} value={lookup.id}>
                      {lookup.name} ({lookup.type})
                    </option>
                  ))}
                </select>
              </td>
              
              {/* Priority */}
              <td className="p-3">
                <div className="flex items-center space-x-2">
                  <input
                    type="range"
                    min="1"
                    max="100"
                    value={rule.priority || 50}
                    onChange={(e) => onUpdateRule(rule.id, { priority: parseInt(e.target.value) })}
                    className="flex-1"
                  />
                  <span className={`px-2 py-1 rounded text-xs font-medium ${getPriorityColor(rule.priority || 50)}`}>
                    {rule.priority || 50}
                  </span>
                </div>
              </td>
              
              {/* Active */}
              <td className="p-3">
                <button
                  onClick={() => onToggleRule(rule.id)}
                  className={`p-1.5 rounded-full transition-colors ${
                    rule.enabled
                      ? 'bg-green-100 dark:bg-green-900/30 text-green-600 dark:text-green-400'
                      : 'bg-gray-100 dark:bg-gray-800 text-gray-400'
                  }`}
                  title={rule.enabled ? 'Disable rule' : 'Enable rule'}
                >
                  {rule.enabled ? '✓' : '✗'}
                </button>
              </td>
              
              {/* Description */}
              <td className="p-3">
                <div className="max-w-xs">
                  <div className="flex items-start">
                    <button
                      onClick={() => toggleDescription(rule.id)}
                      className="p-1 hover:bg-gray-200 dark:hover:bg-gray-700 rounded"
                    >
                      {expandedDescriptions.has(rule.id) ? (
                        <ChevronDown className="w-4 h-4" />
                      ) : (
                        <ChevronRight className="w-4 h-4" />
                      )}
                    </button>
                    <div className="ml-1 flex-1">
                      <div className="text-sm text-gray-700 dark:text-gray-300">
                        {rule.description || 'No description'}
                      </div>
                      {expandedDescriptions.has(rule.id) && (
                        <textarea
                          value={rule.description || ''}
                          onChange={(e) => onUpdateRule(rule.id, { description: e.target.value })}
                          placeholder="Add description..."
                          className="w-full mt-2 px-2 py-1 border border-gray-300 dark:border-gray-600 rounded bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
                          rows={2}
                        />
                      )}
                    </div>
                  </div>
                </div>
              </td>
              
              {/* Actions */}
              <td className="p-3">
                <div className="flex items-center space-x-1">
                  <button
                    onClick={() => onDuplicateRule(rule.id)}
                    className="p-1.5 hover:bg-gray-200 dark:hover:bg-gray-700 rounded"
                    title="Duplicate rule"
                  >
                    <Copy className="w-4 h-4" />
                  </button>
                  <button
                    onClick={() => onOpenPatternEditor(rule.id)}
                    className="p-1.5 hover:bg-gray-200 dark:hover:bg-gray-700 rounded"
                    title="Edit pattern"
                  >
                    <Edit2 className="w-4 h-4" />
                  </button>
                </div>
              </td>
            </tr>
          ))}
          
          {rules.length === 0 && (
            <tr>
              <td colSpan={10} className="p-8 text-center text-gray-500 dark:text-gray-400">
                <div className="flex flex-col items-center space-y-2">
                  <HelpCircle className="w-12 h-12" />
                  <p>No rules defined. Click "Add Rule" to create your first standardization rule.</p>
                </div>
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
};

export default RuleGrid;