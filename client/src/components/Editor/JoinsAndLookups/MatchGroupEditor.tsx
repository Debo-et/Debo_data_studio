// src/components/Editor/MatchGroupEditor.tsx
import React, { useState, useCallback, useMemo, useEffect } from 'react';
import { motion } from 'framer-motion';
import { X, ChevronDown, ChevronUp, Plus, Trash2, GripVertical } from 'lucide-react';
import {
  MatchGroupComponentConfiguration,
  MatchKey,
  MatchType,
  SurvivorshipRule,
  SurvivorshipRuleType,
} from '../../../types/unified-pipeline.types';
import { SimpleColumn } from '../Mapping/MapEditor'; // reuse SimpleColumn interface

interface MatchGroupEditorProps {
  nodeId: string;
  nodeMetadata: any;
  inputColumns: SimpleColumn[];
  initialConfig?: MatchGroupComponentConfiguration;
  onClose: () => void;
  onSave: (config: MatchGroupComponentConfiguration) => void;
}

const DEFAULT_CONFIG: MatchGroupComponentConfiguration = {
  version: '1.0',
  matchKeys: [],
  survivorshipRules: [],
  outputFields: [],
  globalOptions: {
    matchThreshold: 0.8,
    maxMatchesPerRecord: 100,
    nullHandling: 'ignore',
    outputMode: 'all_matches',
    includeMatchDetails: false,
    parallelization: true,
    batchSize: 1000,
  },
  compilerMetadata: {
    lastModified: new Date().toISOString(),
    createdBy: 'match-group-editor',
    matchKeyCount: 0,
    ruleCount: 0,
    validationStatus: 'VALID',
    dependencies: [],
  },
};

// Use enum members instead of string literals
const MATCH_TYPE_OPTIONS: { value: MatchType; label: string }[] = [
  { value: MatchType.EXACT, label: 'Exact' },
  { value: MatchType.EXACT_IGNORE_CASE, label: 'Exact (ignore case)' },
  { value: MatchType.FUZZY, label: 'Fuzzy' },
  { value: MatchType.SOUNDEX, label: 'Soundex' },
  { value: MatchType.METAPHONE, label: 'Metaphone' },
  { value: MatchType.LEVENSHTEIN, label: 'Levenshtein' },
  { value: MatchType.JARO_WINKLER, label: 'Jaro-Winkler' },
];

const SURVIVORSHIP_RULE_TYPE_OPTIONS: { value: SurvivorshipRuleType; label: string }[] = [
  { value: SurvivorshipRuleType.FIRST, label: 'First' },
  { value: SurvivorshipRuleType.LAST, label: 'Last' },
  { value: SurvivorshipRuleType.MAX, label: 'Max' },
  { value: SurvivorshipRuleType.MIN, label: 'Min' },
  { value: SurvivorshipRuleType.SUM, label: 'Sum' },
  { value: SurvivorshipRuleType.AVG, label: 'Avg' },
  { value: SurvivorshipRuleType.CONCAT, label: 'Concatenate' },
  { value: SurvivorshipRuleType.MOST_FREQUENT, label: 'Most Frequent' },
  { value: SurvivorshipRuleType.ANY_NON_NULL, label: 'Any Non‑null' },
  { value: SurvivorshipRuleType.COALESCE, label: 'Coalesce' },
];

export const MatchGroupEditor: React.FC<MatchGroupEditorProps> = ({
  nodeId,
  nodeMetadata,
  inputColumns,
  initialConfig,
  onClose,
  onSave,
}) => {
  const [config, setConfig] = useState<MatchGroupComponentConfiguration>(
    () => initialConfig || { ...DEFAULT_CONFIG }
  );
  const [expandedAdvanced, setExpandedAdvanced] = useState(false);
  const [includeAllFields, setIncludeAllFields] = useState(true);

  // Derive output fields from survivorship rules and user selection
  const outputFieldsSet = useMemo(() => {
    const fields = new Set(config.outputFields);
    config.survivorshipRules.forEach(rule => fields.add(rule.field));
    return fields;
  }, [config.outputFields, config.survivorshipRules]);

  // Update compiler metadata on changes
  useEffect(() => {
    setConfig(prev => ({
      ...prev,
      compilerMetadata: {
        ...prev.compilerMetadata,
        lastModified: new Date().toISOString(),
        matchKeyCount: prev.matchKeys.length,
        ruleCount: prev.survivorshipRules.length,
      },
    }));
  }, [config.matchKeys.length, config.survivorshipRules.length]);

  // --- Match Key handlers ---
  const addMatchKey = useCallback(() => {
    const newKey: MatchKey = {
      id: `key-${Date.now()}-${Math.random().toString(36).substr(2, 5)}`,
      field: inputColumns[0]?.name || '',
      matchType: MatchType.EXACT, // use enum
      caseSensitive: true,
      ignoreNull: false,
      weight: 1,
      blockingKey: false,
    };
    setConfig(prev => ({
      ...prev,
      matchKeys: [...prev.matchKeys, newKey],
    }));
  }, [inputColumns]);

  const updateMatchKey = useCallback((id: string, updates: Partial<MatchKey>) => {
    setConfig(prev => ({
      ...prev,
      matchKeys: prev.matchKeys.map(key =>
        key.id === id ? { ...key, ...updates } : key
      ),
    }));
  }, []);

  const removeMatchKey = useCallback((id: string) => {
    setConfig(prev => ({
      ...prev,
      matchKeys: prev.matchKeys.filter(key => key.id !== id),
    }));
  }, []);

  // --- Survivorship Rule handlers ---
  const addRule = useCallback(() => {
    const newRule: SurvivorshipRule = {
      id: `rule-${Date.now()}-${Math.random().toString(36).substr(2, 5)}`,
      field: inputColumns[0]?.name || '',
      ruleType: SurvivorshipRuleType.FIRST, // use enum
    };
    setConfig(prev => ({
      ...prev,
      survivorshipRules: [...prev.survivorshipRules, newRule],
    }));
  }, [inputColumns]);

  const updateRule = useCallback((id: string, updates: Partial<SurvivorshipRule>) => {
    setConfig(prev => ({
      ...prev,
      survivorshipRules: prev.survivorshipRules.map(rule =>
        rule.id === id ? { ...rule, ...updates } : rule
      ),
    }));
  }, []);

  const removeRule = useCallback((id: string) => {
    setConfig(prev => ({
      ...prev,
      survivorshipRules: prev.survivorshipRules.filter(rule => rule.id !== id),
    }));
  }, []);

  // --- Output field selection ---
  const toggleOutputField = useCallback((fieldName: string) => {
    setConfig(prev => {
      const set = new Set(prev.outputFields);
      if (set.has(fieldName)) {
        set.delete(fieldName);
      } else {
        set.add(fieldName);
      }
      return { ...prev, outputFields: Array.from(set) };
    });
  }, []);

  const toggleAllFields = useCallback(() => {
    setIncludeAllFields(prev => {
      const newIncludeAll = !prev;
      if (newIncludeAll) {
        // include all input columns
        setConfig(c => ({ ...c, outputFields: inputColumns.map(col => col.name) }));
      } else {
        // clear output fields, but keep those from rules
        const ruleFields = config.survivorshipRules.map(r => r.field);
        setConfig(c => ({ ...c, outputFields: ruleFields }));
      }
      return newIncludeAll;
    });
  }, [inputColumns, config.survivorshipRules]);

  // --- Save ---
  const handleSave = useCallback(() => {
    // Ensure output fields include all rule fields
    const ruleFields = config.survivorshipRules.map(r => r.field);
    const finalOutputFields = Array.from(new Set([...config.outputFields, ...ruleFields]));
    onSave({
      ...config,
      outputFields: finalOutputFields,
    });
  }, [config, onSave]);

  return (
    <div className="fixed inset-0 z-[9999] bg-gray-900/80 flex items-center justify-center p-4">
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        className="bg-gray-800 rounded-xl shadow-2xl w-full max-w-6xl max-h-[90vh] overflow-hidden flex flex-col"
      >
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-700 bg-gray-900/50">
          <div className="flex items-center gap-3">
            <h2 className="text-lg font-semibold text-white">Match Group Configuration</h2>
            <span className="text-xs bg-blue-600/20 text-blue-300 px-2 py-1 rounded border border-blue-600/30">
              {nodeMetadata?.name || nodeId}
            </span>
            <span className="text-xs bg-gray-700 text-gray-300 px-2 py-1 rounded">
              {config.matchKeys.length} key{config.matchKeys.length !== 1 ? 's' : ''}
            </span>
            <span className="text-xs bg-gray-700 text-gray-300 px-2 py-1 rounded">
              {config.survivorshipRules.length} rule{config.survivorshipRules.length !== 1 ? 's' : ''}
            </span>
          </div>
          <button
            onClick={onClose}
            className="p-1 hover:bg-gray-700 rounded-lg transition-colors text-gray-400 hover:text-white"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Main content */}
        <div className="flex-1 overflow-y-auto p-6 space-y-6">
          {/* Input Schema / Output Fields */}
          <div className="bg-gray-900/30 border border-gray-700 rounded-lg p-4">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-medium text-gray-300">Output Fields</h3>
              <label className="flex items-center gap-2 text-sm text-gray-400">
                <input
                  type="checkbox"
                  checked={includeAllFields}
                  onChange={toggleAllFields}
                  className="rounded border-gray-600 bg-gray-700 text-blue-600 focus:ring-blue-500"
                />
                Include all input columns
              </label>
            </div>
            <div className="grid grid-cols-3 gap-2 max-h-40 overflow-y-auto">
              {inputColumns.map(col => (
                <label key={col.name} className="flex items-center gap-2 text-sm text-gray-300">
                  <input
                    type="checkbox"
                    checked={outputFieldsSet.has(col.name)}
                    onChange={() => toggleOutputField(col.name)}
                    className="rounded border-gray-600 bg-gray-700 text-blue-600 focus:ring-blue-500"
                  />
                  <span className="truncate">{col.name}</span>
                  <span className="text-xs text-gray-500 ml-auto">{col.type || 'string'}</span>
                </label>
              ))}
            </div>
          </div>

          {/* Match Keys */}
          <div className="bg-gray-900/30 border border-gray-700 rounded-lg p-4">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-medium text-gray-300">Match Keys</h3>
              <button
                onClick={addMatchKey}
                className="flex items-center gap-1 px-2 py-1 text-xs bg-blue-600 hover:bg-blue-700 text-white rounded transition-colors"
              >
                <Plus className="w-3 h-3" /> Add Match Key
              </button>
            </div>
            {config.matchKeys.length === 0 ? (
              <p className="text-sm text-gray-500 italic">No match keys defined. Add at least one key to enable matching.</p>
            ) : (
              <div className="space-y-2">
                {config.matchKeys.map((key, _idx) => (
                  <div key={key.id} className="flex items-center gap-2 p-2 bg-gray-800/50 border border-gray-700 rounded-lg">
                    <GripVertical className="w-4 h-4 text-gray-500 cursor-move" />
                    <select
                      value={key.field}
                      onChange={(e) => updateMatchKey(key.id, { field: e.target.value })}
                      className="w-40 bg-gray-700 border border-gray-600 text-white text-sm rounded px-2 py-1"
                    >
                      <option value="">Select field</option>
                      {inputColumns.map(col => (
                        <option key={col.name} value={col.name}>{col.name}</option>
                      ))}
                    </select>
                    <select
                      value={key.matchType}
                      onChange={(e) => updateMatchKey(key.id, { matchType: e.target.value as MatchType })}
                      className="w-36 bg-gray-700 border border-gray-600 text-white text-sm rounded px-2 py-1"
                    >
                      {MATCH_TYPE_OPTIONS.map(opt => (
                        <option key={opt.value} value={opt.value}>{opt.label}</option>
                      ))}
                    </select>
                    {key.matchType === MatchType.FUZZY && (
                      <input
                        type="number"
                        value={key.threshold ?? 0.8}
                        onChange={(e) => updateMatchKey(key.id, { threshold: parseFloat(e.target.value) })}
                        min="0"
                        max="1"
                        step="0.05"
                        className="w-20 bg-gray-700 border border-gray-600 text-white text-sm rounded px-2 py-1"
                        placeholder="0.8"
                      />
                    )}
                    <label className="flex items-center gap-1 text-sm text-gray-300 whitespace-nowrap">
                      <input
                        type="checkbox"
                        checked={key.caseSensitive}
                        onChange={(e) => updateMatchKey(key.id, { caseSensitive: e.target.checked })}
                        className="rounded border-gray-600 bg-gray-700"
                      />
                      CS
                    </label>
                    <label className="flex items-center gap-1 text-sm text-gray-300 whitespace-nowrap">
                      <input
                        type="checkbox"
                        checked={key.ignoreNull}
                        onChange={(e) => updateMatchKey(key.id, { ignoreNull: e.target.checked })}
                        className="rounded border-gray-600 bg-gray-700"
                      />
                      Ignore NULL
                    </label>
                    <input
                      type="number"
                      value={key.weight ?? 1}
                      onChange={(e) => updateMatchKey(key.id, { weight: parseFloat(e.target.value) })}
                      min="0"
                      step="0.1"
                      className="w-16 bg-gray-700 border border-gray-600 text-white text-sm rounded px-2 py-1"
                      placeholder="Weight"
                    />
                    <label className="flex items-center gap-1 text-sm text-gray-300 whitespace-nowrap">
                      <input
                        type="checkbox"
                        checked={key.blockingKey}
                        onChange={(e) => updateMatchKey(key.id, { blockingKey: e.target.checked })}
                        className="rounded border-gray-600 bg-gray-700"
                      />
                      Block
                    </label>
                    <button
                      onClick={() => removeMatchKey(key.id)}
                      className="p-1 hover:bg-gray-700 rounded text-gray-400 hover:text-red-400"
                    >
                      <Trash2 className="w-4 h-4" />
                    </button>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Survivorship Rules */}
          <div className="bg-gray-900/30 border border-gray-700 rounded-lg p-4">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-medium text-gray-300">Survivorship Rules</h3>
              <button
                onClick={addRule}
                className="flex items-center gap-1 px-2 py-1 text-xs bg-purple-600 hover:bg-purple-700 text-white rounded transition-colors"
              >
                <Plus className="w-3 h-3" /> Add Rule
              </button>
            </div>
            {config.survivorshipRules.length === 0 ? (
              <p className="text-sm text-gray-500 italic">No survivorship rules. If none, the first record in group is used.</p>
            ) : (
              <div className="space-y-2">
                {config.survivorshipRules.map((rule, _idx) => (
                  <div key={rule.id} className="flex items-center gap-2 p-2 bg-gray-800/50 border border-gray-700 rounded-lg">
                    <GripVertical className="w-4 h-4 text-gray-500 cursor-move" />
                    <input
                      type="text"
                      value={rule.field}
                      onChange={(e) => updateRule(rule.id, { field: e.target.value })}
                      className="w-40 bg-gray-700 border border-gray-600 text-white text-sm rounded px-2 py-1"
                      placeholder="Output field"
                    />
                    <select
                      value={rule.ruleType}
                      onChange={(e) => updateRule(rule.id, { ruleType: e.target.value as SurvivorshipRuleType })}
                      className="w-40 bg-gray-700 border border-gray-600 text-white text-sm rounded px-2 py-1"
                    >
                      {SURVIVORSHIP_RULE_TYPE_OPTIONS.map(opt => (
                        <option key={opt.value} value={opt.value}>{opt.label}</option>
                      ))}
                    </select>
                    {/* Optional parameters */}
                    {rule.ruleType === SurvivorshipRuleType.CONCAT && (
                      <input
                        type="text"
                        value={rule.params?.separator ?? ','}
                        onChange={(e) => updateRule(rule.id, { params: { ...rule.params, separator: e.target.value } })}
                        className="w-24 bg-gray-700 border border-gray-600 text-white text-sm rounded px-2 py-1"
                        placeholder="Separator"
                      />
                    )}
                    {(rule.ruleType === SurvivorshipRuleType.FIRST || rule.ruleType === SurvivorshipRuleType.LAST) && (
                      <select
                        value={rule.params?.orderBy || ''}
                        onChange={(e) => updateRule(rule.id, { params: { ...rule.params, orderBy: e.target.value } })}
                        className="w-32 bg-gray-700 border border-gray-600 text-white text-sm rounded px-2 py-1"
                      >
                        <option value="">Order by...</option>
                        {inputColumns.map(col => (
                          <option key={col.name} value={col.name}>{col.name}</option>
                        ))}
                      </select>
                    )}
                    <button
                      onClick={() => removeRule(rule.id)}
                      className="p-1 hover:bg-gray-700 rounded text-gray-400 hover:text-red-400"
                    >
                      <Trash2 className="w-4 h-4" />
                    </button>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Advanced Options */}
          <div className="bg-gray-900/30 border border-gray-700 rounded-lg p-4">
            <button
              onClick={() => setExpandedAdvanced(!expandedAdvanced)}
              className="flex items-center justify-between w-full text-sm font-medium text-gray-300"
            >
              <span>Advanced Options</span>
              {expandedAdvanced ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
            </button>
            {expandedAdvanced && (
              <div className="mt-4 space-y-4">
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-xs text-gray-400 mb-1">Global Match Threshold</label>
                    <input
                      type="range"
                      min="0"
                      max="1"
                      step="0.05"
                      value={config.globalOptions.matchThreshold ?? 0.8}
                      onChange={(e) => setConfig(prev => ({
                        ...prev,
                        globalOptions: { ...prev.globalOptions, matchThreshold: parseFloat(e.target.value) }
                      }))}
                      className="w-full"
                    />
                    <span className="text-xs text-gray-300 mt-1 block">
                      {(config.globalOptions.matchThreshold ?? 0.8).toFixed(2)}
                    </span>
                  </div>
                  <div>
                    <label className="block text-xs text-gray-400 mb-1">Max Matches Per Record</label>
                    <input
                      type="number"
                      value={config.globalOptions.maxMatchesPerRecord ?? 100}
                      onChange={(e) => setConfig(prev => ({
                        ...prev,
                        globalOptions: { ...prev.globalOptions, maxMatchesPerRecord: parseInt(e.target.value) || 100 }
                      }))}
                      min="1"
                      className="w-full bg-gray-700 border border-gray-600 text-white text-sm rounded px-2 py-1"
                    />
                  </div>
                </div>
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-xs text-gray-400 mb-1">Null Handling</label>
                    <select
                      value={config.globalOptions.nullHandling}
                      onChange={(e) => setConfig(prev => ({
                        ...prev,
                        globalOptions: { ...prev.globalOptions, nullHandling: e.target.value as any }
                      }))}
                      className="w-full bg-gray-700 border border-gray-600 text-white text-sm rounded px-2 py-1"
                    >
                      <option value="match">Match</option>
                      <option value="no_match">No Match</option>
                      <option value="ignore">Ignore</option>
                    </select>
                  </div>
                  <div>
                    <label className="block text-xs text-gray-400 mb-1">Output Mode</label>
                    <select
                      value={config.globalOptions.outputMode}
                      onChange={(e) => setConfig(prev => ({
                        ...prev,
                        globalOptions: { ...prev.globalOptions, outputMode: e.target.value as any }
                      }))}
                      className="w-full bg-gray-700 border border-gray-600 text-white text-sm rounded px-2 py-1"
                    >
                      <option value="all_matches">All matches</option>
                      <option value="best_match">Best match</option>
                      <option value="groups_only">Groups only</option>
                    </select>
                  </div>
                </div>
                <div className="flex items-center gap-6">
                  <label className="flex items-center gap-2 text-sm text-gray-300">
                    <input
                      type="checkbox"
                      checked={config.globalOptions.includeMatchDetails}
                      onChange={(e) => setConfig(prev => ({
                        ...prev,
                        globalOptions: { ...prev.globalOptions, includeMatchDetails: e.target.checked }
                      }))}
                      className="rounded border-gray-600 bg-gray-700"
                    />
                    Include match details column
                  </label>
                  <label className="flex items-center gap-2 text-sm text-gray-300">
                    <input
                      type="checkbox"
                      checked={config.globalOptions.parallelization}
                      onChange={(e) => setConfig(prev => ({
                        ...prev,
                        globalOptions: { ...prev.globalOptions, parallelization: e.target.checked }
                      }))}
                      className="rounded border-gray-600 bg-gray-700"
                    />
                    Parallelize
                  </label>
                  <div>
                    <label className="text-xs text-gray-400 mr-2">Batch Size</label>
                    <input
                      type="number"
                      value={config.globalOptions.batchSize}
                      onChange={(e) => setConfig(prev => ({
                        ...prev,
                        globalOptions: { ...prev.globalOptions, batchSize: parseInt(e.target.value) || 1000 }
                      }))}
                      min="1"
                      className="w-24 bg-gray-700 border border-gray-600 text-white text-sm rounded px-2 py-1"
                    />
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end gap-3 px-6 py-4 border-t border-gray-700 bg-gray-900/50">
          <button
            onClick={onClose}
            className="px-4 py-2 text-sm text-gray-300 hover:text-white bg-gray-700 hover:bg-gray-600 rounded transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={handleSave}
            className="px-4 py-2 text-sm bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-700 hover:to-purple-700 text-white rounded transition-all"
          >
            Save Configuration
          </button>
        </div>
      </motion.div>
    </div>
  );
};