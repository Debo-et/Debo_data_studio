// StandardizationRulesEditor.tsx
import React, { useState, useEffect, useReducer, useCallback, useMemo } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { 
  Save, 
  Plus, 
  Trash2, 
  Copy, 
  Search, 
  Check, 
  AlertCircle,
  ChevronDown,
  ChevronUp,
  User,
  Calendar,
  Undo,
  Redo,
  Import,
  Upload,
  RefreshCw
} from 'lucide-react';
import { debounce } from 'lodash';

// Import internal components
import RuleGrid from './RuleGrid';
import TestPreviewPanel from './TestPreviewPanel';
import PatternEditorDialog from './PatternEditorDialog';
import ImportExportDialog from './ImportExportDialog';
import RuleSetSelector from './RuleSetSelector';
import ValidationStatus from './ValidationStatus';

// Import types that exist
import {
  RuleSet,
  Rule,
  ValidationResult,
  SchemaField,
  OperationType,
  PatternType,
  TestCase as ImportedTestCase,
  LookupReference
} from '../../../types/types';

// Import RuleGrid's Rule type to ensure compatibility

// Define ExtendedRule that's fully compatible with both Rule types
interface ExtendedRule extends Omit<Rule, 'lookup' | 'patternType' | 'operation'> {
  operation: OperationType;
  matchPattern: string;
  patternType: PatternType;
  replacement: string;
  priority: number;
  description: string;
  lookup?: LookupReference;
}

interface ExtendedRuleSet extends Omit<RuleSet, 'rules'> {
  rules: ExtendedRule[];
}

// Use the imported TestCase type
type TestCase = ImportedTestCase;

// Import mock data
import { 
  mockRuleSets, 
  mockSchemaFields, 
  mockLookups,
  initialTestCases 
} from './mockData';

// Convert mock rule sets to ExtendedRuleSet
const convertToExtendedRuleSet = (ruleSet: RuleSet): ExtendedRuleSet => {
  return {
    ...ruleSet,
    rules: ruleSet.rules.map(rule => ({
      ...rule,
      operation: (rule.operation as OperationType) || 'Replace',
      matchPattern: rule.matchPattern || '',
      replacement: rule.replacement || '',
      priority: rule.priority || 50,
      patternType: (rule.patternType || 'regex') as PatternType,
      description: rule.description || '',
      lookup: rule.lookup as LookupReference | undefined
    }))
  };
};

// Convert test cases if needed (from mock data structure to ImportedTestCase)
const convertMockTestCases = (testCases: any[]): TestCase[] => {
  return testCases.map(tc => ({
    id: tc.id,
    testValue: tc.input || tc.testValue || '',
    expectedOutput: tc.expectedOutput,
    description: tc.description || '',
    inputColumn: tc.inputColumn || 'default'
  }));
};

// Convert mock data
const extendedMockRuleSets = mockRuleSets.map(convertToExtendedRuleSet);
const convertedTestCases = convertMockTestCases(initialTestCases);

// State reducer for undo/redo
type EditorState = {
  ruleSet: ExtendedRuleSet;
  testCases: TestCase[];
  validationResults: ValidationResult[];
  history: ExtendedRuleSet[];
  historyIndex: number;
};

type EditorAction =
  | { type: 'UPDATE_RULE_SET'; payload: ExtendedRuleSet }
  | { type: 'UPDATE_RULES'; payload: ExtendedRule[] }
  | { type: 'UPDATE_TEST_CASES'; payload: TestCase[] }
  | { type: 'ADD_RULE'; payload: ExtendedRule }
  | { type: 'DELETE_RULES'; payload: string[] }
  | { type: 'DUPLICATE_RULE'; payload: string }
  | { type: 'TOGGLE_RULE'; payload: string }
  | { type: 'UPDATE_RULE'; payload: { id: string; updates: Partial<ExtendedRule> } }
  | { type: 'UNDO' }
  | { type: 'REDO' }
  | { type: 'SAVE_TO_HISTORY' }
  | { type: 'CLEAR_HISTORY' }
  | { type: 'VALIDATE_RULES' }
  | { type: 'IMPORT_RULE_SET'; payload: ExtendedRuleSet }
  | { type: 'EXPORT_RULE_SET' }
  | { type: 'RESET' };

// Initial state
const initialState: EditorState = {
  ruleSet: extendedMockRuleSets[0],
  testCases: convertedTestCases,
  validationResults: [],
  history: [extendedMockRuleSets[0]],
  historyIndex: 0
};

// Reducer function
function editorReducer(state: EditorState, action: EditorAction): EditorState {
  switch (action.type) {
    case 'UPDATE_RULE_SET':
      return {
        ...state,
        ruleSet: action.payload,
        history: [...state.history.slice(0, state.historyIndex + 1), action.payload],
        historyIndex: state.historyIndex + 1
      };

    case 'UPDATE_RULES':
      return {
        ...state,
        ruleSet: {
          ...state.ruleSet,
          rules: action.payload,
          lastModified: new Date()
        }
      };

    case 'ADD_RULE':
      return {
        ...state,
        ruleSet: {
          ...state.ruleSet,
          rules: [...state.ruleSet.rules, action.payload],
          lastModified: new Date()
        }
      };

    case 'DELETE_RULES':
      return {
        ...state,
        ruleSet: {
          ...state.ruleSet,
          rules: state.ruleSet.rules.filter(rule => !action.payload.includes(rule.id)),
          lastModified: new Date()
        }
      };

    case 'DUPLICATE_RULE':
      const ruleToDuplicate = state.ruleSet.rules.find(r => r.id === action.payload);
      if (!ruleToDuplicate) return state;
      
      const duplicatedRule: ExtendedRule = {
        ...ruleToDuplicate,
        id: `rule_${Date.now()}`,
        name: `${ruleToDuplicate.name} (Copy)`,
        description: ruleToDuplicate.description ? `${ruleToDuplicate.description} (Copy)` : 'Copied rule'
      };
      
      return {
        ...state,
        ruleSet: {
          ...state.ruleSet,
          rules: [...state.ruleSet.rules, duplicatedRule],
          lastModified: new Date()
        }
      };

    case 'TOGGLE_RULE':
      return {
        ...state,
        ruleSet: {
          ...state.ruleSet,
          rules: state.ruleSet.rules.map(rule =>
            rule.id === action.payload
              ? { ...rule, enabled: !rule.enabled }
              : rule
          ),
          lastModified: new Date()
        }
      };

    case 'UPDATE_RULE':
      return {
        ...state,
        ruleSet: {
          ...state.ruleSet,
          rules: state.ruleSet.rules.map(rule =>
            rule.id === action.payload.id
              ? { ...rule, ...action.payload.updates }
              : rule
          ),
          lastModified: new Date()
        }
      };

    case 'UNDO':
      if (state.historyIndex > 0) {
        return {
          ...state,
          ruleSet: state.history[state.historyIndex - 1],
          historyIndex: state.historyIndex - 1
        };
      }
      return state;

    case 'REDO':
      if (state.historyIndex < state.history.length - 1) {
        return {
          ...state,
          ruleSet: state.history[state.historyIndex + 1],
          historyIndex: state.historyIndex + 1
        };
      }
      return state;

    case 'SAVE_TO_HISTORY':
      return {
        ...state,
        history: [...state.history.slice(0, state.historyIndex + 1), state.ruleSet],
        historyIndex: state.historyIndex + 1
      };

    case 'UPDATE_TEST_CASES':
      return {
        ...state,
        testCases: action.payload
      };

    case 'VALIDATE_RULES':
      // In a real implementation, this would run validation logic
      return state;

    default:
      return state;
  }
}

interface StandardizationRulesEditorProps {
  initialRuleSet?: ExtendedRuleSet;
  schemaFields?: SchemaField[];
  onSave?: (ruleSet: ExtendedRuleSet) => void;
  onCancel?: () => void;
  onApply?: (ruleSet: ExtendedRuleSet) => void;
  isModal?: boolean;
}

const StandardizationRulesEditor: React.FC<StandardizationRulesEditorProps> = ({
  initialRuleSet,
  schemaFields = mockSchemaFields,
  onSave,
  onCancel,
  onApply,
  isModal = false
}) => {
  // State management
  const [state, dispatch] = useReducer(editorReducer, {
    ...initialState,
    ruleSet: initialRuleSet || initialState.ruleSet
  });

  const [selectedRuleSet, setSelectedRuleSet] = useState<string>(state.ruleSet.id);
  const [showMetadataPanel, setShowMetadataPanel] = useState(true);
  const [selectedRuleIds, setSelectedRuleIds] = useState<string[]>([]);
  const [searchQuery, setSearchQuery] = useState('');
  const [filterOperation, setFilterOperation] = useState<OperationType | 'all'>('all');
  const [showPatternEditor, setShowPatternEditor] = useState<string | null>(null);
  const [showImportExportDialog, setShowImportExportDialog] = useState<'import' | 'export' | null>(null);
  const [isValidating, setIsValidating] = useState(false);
  const [saveStatus, setSaveStatus] = useState<'idle' | 'saving' | 'saved' | 'error'>('idle');

  // Refs for auto-save
  const autoSaveTimeoutRef = React.useRef<NodeJS.Timeout>();

  // Available rule sets
  const availableRuleSets = extendedMockRuleSets;

  // Debounced save function
  const debouncedSave = useCallback(
    debounce((ruleSet: ExtendedRuleSet) => {
      if (onSave) {
        setSaveStatus('saving');
        try {
          onSave(ruleSet);
          setSaveStatus('saved');
          setTimeout(() => setSaveStatus('idle'), 2000);
        } catch (error) {
          console.error('Save failed:', error);
          setSaveStatus('error');
        }
      }
    }, 1000),
    [onSave]
  );

  // Auto-save effect
  useEffect(() => {
    if (autoSaveTimeoutRef.current) {
      clearTimeout(autoSaveTimeoutRef.current);
    }

    autoSaveTimeoutRef.current = setTimeout(() => {
      debouncedSave(state.ruleSet);
      dispatch({ type: 'SAVE_TO_HISTORY' });
    }, 3000);

    return () => {
      if (autoSaveTimeoutRef.current) {
        clearTimeout(autoSaveTimeoutRef.current);
      }
    };
  }, [state.ruleSet, debouncedSave]);

  // Handle rule set selection
  const handleRuleSetSelect = (ruleSetId: string) => {
    const ruleSet = availableRuleSets.find(rs => rs.id === ruleSetId);
    if (ruleSet) {
      setSelectedRuleSet(ruleSetId);
      dispatch({ type: 'UPDATE_RULE_SET', payload: ruleSet });
    }
  };

  // Create new rule set
  const handleNewRuleSet = () => {
    const newRuleSet: ExtendedRuleSet = {
      id: `ruleset_${Date.now()}`,
      name: 'New Rule Set',
      description: 'Describe your rule set here...',
      version: '1.0.0',
      author: 'Current User',
      created: new Date(),
      lastModified: new Date(),
      rules: [],
      inputSchema: schemaFields
    };
    
    dispatch({ type: 'UPDATE_RULE_SET', payload: newRuleSet });
    setSelectedRuleSet(newRuleSet.id);
  };

  // Add new rule
  const handleAddRule = () => {
    const newRule: ExtendedRule = {
      id: `rule_${Date.now()}`,
      name: 'New Rule',
      enabled: true,
      inputColumn: schemaFields[0]?.name || '',
      operation: 'Replace',
      matchPattern: '',
      replacement: '',  
      priority: 50,
      patternType: 'regex',
      description: 'New standardization rule'
    };
    
    dispatch({ type: 'ADD_RULE', payload: newRule });
  };

  // Delete selected rules
  const handleDeleteSelected = () => {
    if (selectedRuleIds.length > 0) {
      if (window.confirm(`Delete ${selectedRuleIds.length} selected rule(s)?`)) {
        dispatch({ type: 'DELETE_RULES', payload: selectedRuleIds });
        setSelectedRuleIds([]);
      }
    }
  };

  // Duplicate selected rule
  const handleDuplicateRule = (ruleId: string) => {
    dispatch({ type: 'DUPLICATE_RULE', payload: ruleId });
  };

 // In the handleUpdateRule function, update the type conversion:
const handleUpdateRule = (ruleId: string, updates: Partial<Rule>) => {
  // Convert Rule updates to ExtendedRule updates
  const extendedUpdates: Partial<ExtendedRule> = {
    ...updates,
    operation: updates.operation as OperationType | undefined,
    patternType: updates.patternType as PatternType | undefined,
    replacement: updates.replacement || '',
    priority: updates.priority || 50,
    description: updates.description || ''
  };
  
  // If there's a lookup object, ensure it has the source property
  if (updates.lookup) {
    // Ensure the lookup object matches the LookupReference type from types.ts
    extendedUpdates.lookup = {
      ...updates.lookup,
      source: (updates.lookup as any).source || '' // Provide default or get from updates
    };
  }
  
  dispatch({ type: 'UPDATE_RULE', payload: { id: ruleId, updates: extendedUpdates } });
};

  // Toggle rule enabled state
  const handleToggleRule = (ruleId: string) => {
    dispatch({ type: 'TOGGLE_RULE', payload: ruleId });
  };

  // Run validation
  const handleValidate = async () => {
    setIsValidating(true);
    // Simulate validation
    await new Promise(resolve => setTimeout(resolve, 1000));
    dispatch({ type: 'VALIDATE_RULES' });
    setIsValidating(false);
  };

  // Handle save
  const handleSave = () => {
    setSaveStatus('saving');
    if (onSave) {
      onSave(state.ruleSet);
    }
    setSaveStatus('saved');
    setTimeout(() => setSaveStatus('idle'), 2000);
  };

  // Handle apply
  const handleApply = () => {
    if (onApply) {
      onApply(state.ruleSet);
    }
  };

  // Handle cancel
  const handleCancel = () => {
    if (onCancel) {
      if (state.historyIndex > 0) {
        const hasChanges = state.ruleSet !== state.history[0];
        if (hasChanges && !window.confirm('You have unsaved changes. Are you sure you want to cancel?')) {
          return;
        }
      }
      onCancel();
    }
  };

  // Keyboard shortcuts
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      // Ctrl+S to save
      if ((e.ctrlKey || e.metaKey) && e.key === 's') {
        e.preventDefault();
        handleSave();
      }
      
      // Ctrl+Z to undo
      if ((e.ctrlKey || e.metaKey) && e.key === 'z' && !e.shiftKey) {
        e.preventDefault();
        dispatch({ type: 'UNDO' });
      }
      
      // Ctrl+Shift+Z or Ctrl+Y to redo
      if (((e.ctrlKey || e.metaKey) && e.shiftKey && e.key === 'Z') || 
          ((e.ctrlKey || e.metaKey) && e.key === 'y')) {
        e.preventDefault();
        dispatch({ type: 'REDO' });
      }
      
      // Delete key to delete selected rules
      if (e.key === 'Delete' && selectedRuleIds.length > 0) {
        e.preventDefault();
        handleDeleteSelected();
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [selectedRuleIds, state.historyIndex]);

  // Filter rules based on search and filter
  const filteredRules = useMemo(() => {
    return state.ruleSet.rules.filter(rule => {
      const matchesSearch = searchQuery === '' || 
        rule.inputColumn.toLowerCase().includes(searchQuery.toLowerCase()) ||
        (rule.description && rule.description.toLowerCase().includes(searchQuery.toLowerCase())) ||
        rule.matchPattern.toLowerCase().includes(searchQuery.toLowerCase());
      
      const matchesFilter = filterOperation === 'all' || rule.operation === filterOperation;
      
      return matchesSearch && matchesFilter && rule.enabled;
    });
  }, [state.ruleSet.rules, searchQuery, filterOperation]);

  // Count active rules
  const activeRulesCount = state.ruleSet.rules.filter(r => r.enabled).length;
  const totalRulesCount = state.ruleSet.rules.length;

  return (
    <div className={`flex flex-col h-full bg-gray-50 dark:bg-gray-900 ${isModal ? 'rounded-lg' : ''}`}>
      {/* HEADER & GLOBAL CONFIGURATION ZONE */}
      <div className="border-b border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 p-4 space-y-4">
        {/* Rule Set Selector Row */}
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-4">
            <RuleSetSelector
              ruleSets={availableRuleSets}
              selectedRuleSetId={selectedRuleSet}
              onSelect={handleRuleSetSelect}
              onNewRuleSet={handleNewRuleSet}
            />
            
            {/* Save Status Indicator */}
            {saveStatus === 'saving' && (
              <div className="flex items-center text-amber-600 dark:text-amber-400 text-sm">
                <RefreshCw className="w-4 h-4 animate-spin mr-2" />
                Saving...
              </div>
            )}
            {saveStatus === 'saved' && (
              <div className="flex items-center text-green-600 dark:text-green-400 text-sm">
                <Check className="w-4 h-4 mr-2" />
                Saved
              </div>
            )}
            {saveStatus === 'error' && (
              <div className="flex items-center text-red-600 dark:text-red-400 text-sm">
                <AlertCircle className="w-4 h-4 mr-2" />
                Save failed
              </div>
            )}
          </div>

          {/* Action Buttons */}
          <div className="flex items-center space-x-2">
            <button
              onClick={handleValidate}
              disabled={isValidating}
              className="px-3 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 flex items-center space-x-2"
            >
              {isValidating ? (
                <RefreshCw className="w-4 h-4 animate-spin" />
              ) : (
                <Check className="w-4 h-4" />
              )}
              <span>Validate</span>
            </button>

            <button
              onClick={() => setShowImportExportDialog('import')}
              className="px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-700 flex items-center space-x-2"
            >
              <Import className="w-4 h-4" />
              <span>Import</span>
            </button>

            <button
              onClick={() => setShowImportExportDialog('export')}
              className="px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-700 flex items-center space-x-2"
            >
              <Upload className="w-4 h-4" />
              <span>Export</span>
            </button>

            <button
              onClick={handleSave}
              className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 flex items-center space-x-2"
            >
              <Save className="w-4 h-4" />
              <span>Save</span>
            </button>
          </div>
        </div>

        {/* Metadata & Description Panel */}
        <AnimatePresence>
          {showMetadataPanel && (
            <motion.div
              initial={{ opacity: 0, height: 0 }}
              animate={{ opacity: 1, height: 'auto' }}
              exit={{ opacity: 0, height: 0 }}
              className="pt-4 border-t border-gray-200 dark:border-gray-700"
            >
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div className="space-y-2">
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300">
                    Rule Set Name
                  </label>
                  <input
                    type="text"
                    value={state.ruleSet.name}
                    onChange={(e) => {
                      const updated = { ...state.ruleSet, name: e.target.value };
                      dispatch({ type: 'UPDATE_RULE_SET', payload: updated });
                    }}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-white"
                    placeholder="Enter rule set name"
                  />
                </div>

                <div className="space-y-2">
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300">
                    Version
                  </label>
                  <input
                    type="text"
                    value={state.ruleSet.version}
                    onChange={(e) => {
                      const updated = { ...state.ruleSet, version: e.target.value };
                      dispatch({ type: 'UPDATE_RULE_SET', payload: updated });
                    }}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-white"
                    placeholder="1.0.0"
                  />
                </div>

                <div className="md:col-span-2 space-y-2">
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300">
                    Description
                  </label>
                  <textarea
                    value={state.ruleSet.description}
                    onChange={(e) => {
                      const updated = { ...state.ruleSet, description: e.target.value };
                      dispatch({ type: 'UPDATE_RULE_SET', payload: updated });
                    }}
                    rows={2}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-white resize-none"
                    placeholder="Describe the purpose of this rule set..."
                  />
                </div>

                <div className="text-sm text-gray-500 dark:text-gray-400">
                  <div className="flex items-center space-x-2">
                    <User className="w-4 h-4" />
                    <span>Author: {state.ruleSet.author}</span>
                  </div>
                  <div className="flex items-center space-x-2 mt-1">
                    <Calendar className="w-4 h-4" />
                    <span>Last modified: {state.ruleSet.lastModified.toLocaleDateString()}</span>
                  </div>
                </div>
              </div>
            </motion.div>
          )}
        </AnimatePresence>

        {/* Toggle Metadata Panel Button */}
        <div className="flex justify-center">
          <button
            onClick={() => setShowMetadataPanel(!showMetadataPanel)}
            className="text-sm text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300 flex items-center"
          >
            {showMetadataPanel ? (
              <>
                <ChevronUp className="w-4 h-4 mr-1" />
                Hide Metadata
              </>
            ) : (
              <>
                <ChevronDown className="w-4 h-4 mr-1" />
                Show Metadata
              </>
            )}
          </button>
        </div>
      </div>

      {/* CENTRAL RULES DEFINITION GRID */}
      <div className="flex-1 overflow-hidden">
        {/* Grid Controls */}
        <div className="border-b border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-800 p-3">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-4">
              <div className="flex items-center space-x-2">
                <button
                  onClick={handleAddRule}
                  className="px-3 py-1.5 bg-blue-600 text-white rounded-lg hover:bg-blue-700 flex items-center space-x-2"
                >
                  <Plus className="w-4 h-4" />
                  <span>Add Rule</span>
                </button>

                <button
                  onClick={handleDeleteSelected}
                  disabled={selectedRuleIds.length === 0}
                  className="px-3 py-1.5 bg-red-600 text-white rounded-lg hover:bg-red-700 disabled:opacity-50 flex items-center space-x-2"
                >
                  <Trash2 className="w-4 h-4" />
                  <span>Delete Selected ({selectedRuleIds.length})</span>
                </button>

                <button
                  onClick={() => selectedRuleIds.length === 1 && handleDuplicateRule(selectedRuleIds[0])}
                  disabled={selectedRuleIds.length !== 1}
                  className="px-3 py-1.5 border border-gray-300 dark:border-gray-600 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-700 disabled:opacity-50 flex items-center space-x-2"
                >
                  <Copy className="w-4 h-4" />
                  <span>Duplicate</span>
                </button>
              </div>

              {/* Undo/Redo */}
              <div className="flex items-center space-x-1">
                <button
                  onClick={() => dispatch({ type: 'UNDO' })}
                  disabled={state.historyIndex === 0}
                  className="p-1.5 rounded hover:bg-gray-200 dark:hover:bg-gray-700 disabled:opacity-50"
                  title="Undo (Ctrl+Z)"
                >
                  <Undo className="w-4 h-4" />
                </button>
                <button
                  onClick={() => dispatch({ type: 'REDO' })}
                  disabled={state.historyIndex === state.history.length - 1}
                  className="p-1.5 rounded hover:bg-gray-200 dark:hover:bg-gray-700 disabled:opacity-50"
                  title="Redo (Ctrl+Y)"
                >
                  <Redo className="w-4 h-4" />
                </button>
              </div>
            </div>

            {/* Search and Filter */}
            <div className="flex items-center space-x-4">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
                <input
                  type="text"
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  placeholder="Search rules..."
                  className="pl-10 pr-4 py-1.5 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-white w-64"
                />
              </div>

              <select
                value={filterOperation}
                onChange={(e) => setFilterOperation(e.target.value as OperationType | 'all')}
                className="px-3 py-1.5 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-white"
              >
                <option value="all">All Operations</option>
                <option value="Replace">Replace</option>
                <option value="Normalize">Normalize</option>
                <option value="Cleanse">Cleanse</option>
                <option value="Format">Format</option>
                <option value="UPPERCASE">UPPERCASE</option>
                <option value="lowercase">lowercase</option>
                <option value="Title Case">Title Case</option>
                <option value="Remove noise characters">Remove noise characters</option>
                <option value="Abbreviation expansion">Abbreviation expansion</option>
                <option value="Trim whitespace">Trim whitespace</option>
                <option value="Remove duplicates">Remove duplicates</option>
                <option value="Standardize date">Standardize date</option>
                <option value="Standardize phone">Standardize phone</option>
                <option value="Standardize address">Standardize address</option>
              </select>
            </div>
          </div>
        </div>

        {/* Rule Grid */}
        <div className="h-full overflow-auto">
          <RuleGrid
            rules={filteredRules as any[]} // Cast to any to avoid type conflicts
            selectedRuleIds={selectedRuleIds}
            onSelectRule={setSelectedRuleIds}
            onUpdateRule={handleUpdateRule}
            onToggleRule={handleToggleRule}
            onDuplicateRule={handleDuplicateRule}
            schemaFields={schemaFields}
            lookups={mockLookups}
            onOpenPatternEditor={setShowPatternEditor}
          />
        </div>
      </div>

      {/* BOTTOM TESTING, PREVIEW & DIAGNOSTICS ZONE */}
      <div className="border-t border-gray-200 dark:border-gray-700 flex-1 min-h-64 max-h-96">
        <TestPreviewPanel
          ruleSet={state.ruleSet}
          testCases={state.testCases}
          onTestCasesUpdate={(testCases) => {
            dispatch({ type: 'UPDATE_TEST_CASES', payload: testCases });
          }}
        />
      </div>

      {/* FOOTER AREA */}
      <div className="border-t border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 p-4">
        <div className="flex items-center justify-between">
          {/* Status/Warnings Strip */}
          <ValidationStatus
            activeRulesCount={activeRulesCount}
            totalRulesCount={totalRulesCount}
            validationResults={state.validationResults}
          />

          {/* Action Buttons */}
          <div className="flex items-center space-x-3">
            <button
              onClick={handleCancel}
              className="px-4 py-2 border border-gray-300 dark:border-gray-600 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-700"
            >
              Cancel
            </button>
            
            {onApply && (
              <button
                onClick={handleApply}
                className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
              >
                Apply
              </button>
            )}
            
            <button
              onClick={handleSave}
              className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 flex items-center space-x-2"
            >
              <Save className="w-4 h-4" />
              <span>OK</span>
            </button>
          </div>
        </div>
      </div>

      {/* DIALOGS */}
      <AnimatePresence>
        {showPatternEditor && (
          <PatternEditorDialog
            ruleId={showPatternEditor}
            initialPattern={state.ruleSet.rules.find(r => r.id === showPatternEditor)?.matchPattern || ''}
            patternType={state.ruleSet.rules.find(r => r.id === showPatternEditor)?.patternType || 'regex'}
            onClose={() => setShowPatternEditor(null)}
            onSave={(pattern, patternType) => {
              handleUpdateRule(showPatternEditor, { 
                matchPattern: pattern, 
                patternType: patternType as PatternType 
              });
            }}
          />
        )}

        {showImportExportDialog && (
          <ImportExportDialog
            mode={showImportExportDialog}
            ruleSet={state.ruleSet}
            onClose={() => setShowImportExportDialog(null)}
            onImport={(importedRuleSet) => {
              const extendedRuleSet = convertToExtendedRuleSet(importedRuleSet);
              dispatch({ type: 'UPDATE_RULE_SET', payload: extendedRuleSet });
              setSelectedRuleSet(extendedRuleSet.id);
            }}
          />
        )}
      </AnimatePresence>
    </div>
  );
};

export default StandardizationRulesEditor;