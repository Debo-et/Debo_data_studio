import React, { useState, useRef, useEffect, useCallback } from 'react';
import {
  X, HelpCircle, Undo2, Redo2, Search, ChevronRight,
  Columns, Type, Calendar, CheckSquare, Hash, ChevronDown,
  GripVertical, Plus, Trash2, RefreshCw, Settings, 
  Save, Check, AlertCircle, Loader2, Maximize2, Minimize2
} from 'lucide-react';

// ============ Type Definitions ============

interface ColumnSchema {
  id: string;
  name: string;
  type: 'string' | 'number' | 'date' | 'boolean' | 'object';
  isKey: boolean;
  schemaId: string;
  schemaName: string;
  hasRule: boolean;
}

interface TransformationRule {
  id: string;
  targetColumn: string;
  function: TransformationFunction;
  priority: number;
  expression?: string;
  isValid: boolean;
  error?: string;
}

interface PreviewRow {
  id: string;
  [key: string]: any;
  _metadata?: {
    isMerged: boolean;
    isConflict: boolean;
    sourceRowIds: string[];
  };
}

type TransformationFunction = 
  | 'most_frequent' 
  | 'longest' 
  | 'latest' 
  | 'earliest'
  | 'min' 
  | 'max' 
  | 'concat' 
  | 'sum'
  | 'average'
  | 'custom_expression';

interface MatchingSettings {
  blockingKeys: string[];
  similarityThreshold: number;
  algorithm: 'deterministic' | 'probabilistic';
  similarityMetrics: {
    jaroWinkler: boolean;
    levenshtein: boolean;
    exact: boolean;
  };
}

interface SurvivorshipSettings {
  customFunctions: string[];
  rejectionBehavior: 'keep_as_rejected' | 'exclude' | 'create_new_column';
  columnPrecedence: string[];
}

interface DataTransformationEditorProps {
  componentName: string;
  initialSchemas: ColumnSchema[];
  onSave: (rules: TransformationRule[], settings: EditorSettings) => void;
  onClose: () => void;
  initialData?: PreviewRow[];
  dockable?: boolean;
  initialSize?: { width: number; height: number };
}

interface EditorSettings {
  matching: MatchingSettings;
  survivorship: SurvivorshipSettings;
}

// ============ Sub-Components ============

const ResizablePanel: React.FC<{
  children: React.ReactNode;
  initialSize: number;
  minSize: number;
  maxSize: number;
  direction: 'horizontal' | 'vertical';
  onResize?: (size: number) => void;
}> = ({ children, initialSize, minSize, maxSize, direction, onResize }) => {
  const [size, setSize] = useState(initialSize);
  const [isResizing, setIsResizing] = useState(false);
  const panelRef = useRef<HTMLDivElement>(null);

  const handleMouseDown = (e: React.MouseEvent) => {
    e.preventDefault();
    setIsResizing(true);
  };

  useEffect(() => {
    const handleMouseMove = (e: MouseEvent) => {
      if (!isResizing || !panelRef.current) return;

      const containerRect = panelRef.current.parentElement?.getBoundingClientRect();
      if (!containerRect) return;

      let newSize;
      if (direction === 'horizontal') {
        newSize = e.clientX - containerRect.left;
      } else {
        newSize = e.clientY - containerRect.top;
      }

      newSize = Math.max(minSize, Math.min(maxSize, newSize));
      setSize(newSize);
      onResize?.(newSize);
    };

    const handleMouseUp = () => {
      setIsResizing(false);
    };

    if (isResizing) {
      document.addEventListener('mousemove', handleMouseMove);
      document.addEventListener('mouseup', handleMouseUp);
    }

    return () => {
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
    };
  }, [isResizing, direction, minSize, maxSize, onResize]);

  return (
    <div
      ref={panelRef}
      className={`relative ${direction === 'horizontal' ? 'flex' : ''}`}
      style={direction === 'horizontal' ? { width: size } : { height: size }}
    >
      {children}
      <div
        className={`absolute ${direction === 'horizontal' 
          ? 'right-0 top-0 w-1 cursor-col-resize' 
          : 'bottom-0 left-0 h-1 cursor-row-resize'
        } hover:bg-blue-400 transition-colors ${isResizing ? 'bg-blue-500' : 'bg-gray-300'}`}
        onMouseDown={handleMouseDown}
      />
    </div>
  );
};

const ColumnIcon: React.FC<{ type: ColumnSchema['type'] }> = ({ type }) => {
  const icons = {
    string: <Type className="w-4 h-4" />,
    number: <Hash className="w-4 h-4" />,
    date: <Calendar className="w-4 h-4" />,
    boolean: <CheckSquare className="w-4 h-4" />,
    object: <Columns className="w-4 h-4" />,
  };
  
  const colors = {
    string: 'text-green-600',
    number: 'text-blue-600',
    date: 'text-purple-600',
    boolean: 'text-red-600',
    object: 'text-yellow-600',
  };

  return (
    <div className={`${colors[type]} flex items-center justify-center`}>
      {icons[type]}
    </div>
  );
};

const SchemaSelector: React.FC<{
  schemas: ColumnSchema[];
  onDragStart: (column: ColumnSchema) => void;
  searchQuery: string;
  onSearchChange: (query: string) => void;
}> = ({ schemas, onDragStart, searchQuery, onSearchChange }) => {
  const [expandedSchemas, setExpandedSchemas] = useState<Set<string>>(
    new Set(schemas.map(s => s.schemaId))
  );

  const toggleSchema = (schemaId: string) => {
    setExpandedSchemas(prev => {
      const next = new Set(prev);
      if (next.has(schemaId)) {
        next.delete(schemaId);
      } else {
        next.add(schemaId);
      }
      return next;
    });
  };

  const filteredSchemas = schemas.filter(col =>
    col.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
    col.schemaName.toLowerCase().includes(searchQuery.toLowerCase())
  );

  const groupedSchemas = filteredSchemas.reduce((acc, col) => {
    if (!acc[col.schemaId]) {
      acc[col.schemaId] = {
        name: col.schemaName,
        columns: []
      };
    }
    acc[col.schemaId].columns.push(col);
    return acc;
  }, {} as Record<string, { name: string; columns: ColumnSchema[] }>);

  return (
    <div className="h-full flex flex-col">
      <div className="p-3 border-b">
        <div className="relative">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-gray-400" />
          <input
            type="text"
            placeholder="Search columns..."
            className="w-full pl-9 pr-3 py-2 border rounded-lg text-sm"
            value={searchQuery}
            onChange={(e) => onSearchChange(e.target.value)}
          />
        </div>
      </div>
      
      <div className="flex-1 overflow-auto p-2">
        {Object.entries(groupedSchemas).map(([schemaId, { name, columns }]) => (
          <div key={schemaId} className="mb-3">
            <button
              onClick={() => toggleSchema(schemaId)}
              className="flex items-center justify-between w-full p-2 hover:bg-gray-100 rounded"
            >
              <span className="font-medium text-sm">{name}</span>
              {expandedSchemas.has(schemaId) ? (
                <ChevronDown className="w-4 h-4" />
              ) : (
                <ChevronRight className="w-4 h-4" />
              )}
            </button>
            
            {expandedSchemas.has(schemaId) && (
              <div className="ml-2 space-y-1">
                {columns.map(column => (
                  <div
                    key={column.id}
                    draggable
                    onDragStart={(e) => {
                      e.dataTransfer.setData('text/plain', column.id);
                      onDragStart(column);
                    }}
                    className="flex items-center p-2 hover:bg-blue-50 rounded border border-transparent hover:border-blue-200 cursor-move group"
                  >
                    <GripVertical className="w-4 h-4 text-gray-400 mr-2 group-hover:text-gray-600" />
                    <ColumnIcon type={column.type} />
                    <span className="ml-2 text-sm flex-1">{column.name}</span>
                    {column.isKey && (
                      <span className="text-xs bg-yellow-100 text-yellow-800 px-2 py-1 rounded">Key</span>
                    )}
                    {column.hasRule && (
                      <div className="w-2 h-2 rounded-full bg-green-500 ml-2" />
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
};

const RulesEditor: React.FC<{
  rules: TransformationRule[];
  columns: ColumnSchema[];
  onRulesChange: (rules: TransformationRule[]) => void;
}> = ({ rules, columns, onRulesChange }) => {
  const [draggedRule, setDraggedRule] = useState<string | null>(null);

  const addRule = () => {
    const newRule: TransformationRule = {
      id: `rule_${Date.now()}`,
      targetColumn: '',
      function: 'most_frequent',
      priority: rules.length + 1,
      isValid: false,
    };
    onRulesChange([...rules, newRule]);
  };

  const updateRule = (id: string, updates: Partial<TransformationRule>) => {
    onRulesChange(
      rules.map(rule => 
        rule.id === id ? { ...rule, ...updates } : rule
      )
    );
  };

  const deleteRule = (id: string) => {
    onRulesChange(rules.filter(rule => rule.id !== id));
  };

  const moveRule = (fromIndex: number, toIndex: number) => {
    const newRules = [...rules];
    const [removed] = newRules.splice(fromIndex, 1);
    newRules.splice(toIndex, 0, removed);
    // Update priorities
    const updatedRules = newRules.map((rule, index) => ({
      ...rule,
      priority: index + 1
    }));
    onRulesChange(updatedRules);
  };

  const handleDragStart = (e: React.DragEvent, ruleId: string) => {
    setDraggedRule(ruleId);
    e.dataTransfer.setData('text/plain', ruleId);
  };

  const handleDragOver = (e: React.DragEvent, _index: number) => {
    e.preventDefault();
  };

  const handleDrop = (e: React.DragEvent, targetIndex: number) => {
    e.preventDefault();
    if (!draggedRule) return;

    const draggedIndex = rules.findIndex(r => r.id === draggedRule);
    if (draggedIndex !== -1 && draggedIndex !== targetIndex) {
      moveRule(draggedIndex, targetIndex);
    }
    setDraggedRule(null);
  };

  return (
    <div className="h-full flex flex-col">
      <div className="p-4 border-b">
        <div className="flex justify-between items-center">
          <h3 className="font-semibold">Transformation Rules</h3>
          <button
            onClick={addRule}
            className="flex items-center gap-2 px-3 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 text-sm"
          >
            <Plus className="w-4 h-4" />
            Add Rule
          </button>
        </div>
      </div>

      <div className="flex-1 overflow-auto">
        <table className="w-full">
          <thead className="sticky top-0 bg-white border-b">
            <tr className="text-left text-sm text-gray-600">
              <th className="p-3 w-8"></th>
              <th className="p-3">Target Column</th>
              <th className="p-3">Function</th>
              <th className="p-3">Priority</th>
              <th className="p-3">Expression</th>
              <th className="p-3 w-16">Actions</th>
            </tr>
          </thead>
          <tbody>
            {rules.map((rule, index) => (
              <tr
                key={rule.id}
                draggable
                onDragStart={(e) => handleDragStart(e, rule.id)}
                onDragOver={(e) => handleDragOver(e, index)}
                onDrop={(e) => handleDrop(e, index)}
                className={`border-b hover:bg-gray-50 ${
                  !rule.isValid ? 'bg-red-50' : ''
                }`}
              >
                <td className="p-3">
                  <GripVertical className="w-4 h-4 text-gray-400 cursor-move" />
                </td>
                <td className="p-3">
                  <select
                    value={rule.targetColumn}
                    onChange={(e) => updateRule(rule.id, { targetColumn: e.target.value })}
                    className="w-full px-3 py-2 border rounded text-sm"
                  >
                    <option value="">Select column...</option>
                    {columns.map(col => (
                      <option key={col.id} value={col.name}>
                        {col.name} ({col.type})
                      </option>
                    ))}
                  </select>
                </td>
                <td className="p-3">
                  <select
                    value={rule.function}
                    onChange={(e) => updateRule(rule.id, { 
                      function: e.target.value as TransformationFunction 
                    })}
                    className="w-full px-3 py-2 border rounded text-sm"
                  >
                    <option value="most_frequent">Most Frequent</option>
                    <option value="longest">Longest</option>
                    <option value="latest">Latest</option>
                    <option value="earliest">Earliest</option>
                    <option value="min">Minimum</option>
                    <option value="max">Maximum</option>
                    <option value="concat">Concatenate</option>
                    <option value="sum">Sum</option>
                    <option value="average">Average</option>
                    <option value="custom_expression">Custom Expression</option>
                  </select>
                </td>
                <td className="p-3">
                  <div className="flex items-center">
                    <input
                      type="number"
                      value={rule.priority}
                      onChange={(e) => updateRule(rule.id, { 
                        priority: parseInt(e.target.value) 
                      })}
                      className="w-20 px-3 py-2 border rounded text-sm"
                      min="1"
                    />
                    <div className="flex flex-col ml-1">
                      <button
                        onClick={() => updateRule(rule.id, { 
                          priority: rule.priority + 1 
                        })}
                        className="w-6 h-3 flex items-center justify-center border rounded-t"
                      >
                        ↑
                      </button>
                      <button
                        onClick={() => updateRule(rule.id, { 
                          priority: Math.max(1, rule.priority - 1) 
                        })}
                        className="w-6 h-3 flex items-center justify-center border rounded-b"
                      >
                        ↓
                      </button>
                    </div>
                  </div>
                </td>
                <td className="p-3">
                  {rule.function === 'custom_expression' ? (
                    <button
                      onClick={() => {/* Open expression editor */}}
                      className="w-full px-3 py-2 border rounded text-sm text-left text-blue-600 hover:bg-blue-50"
                    >
                      {rule.expression || 'Edit expression...'}
                    </button>
                  ) : (
                    <span className="text-gray-500 text-sm">-</span>
                  )}
                </td>
                <td className="p-3">
                  <button
                    onClick={() => deleteRule(rule.id)}
                    className="p-2 text-red-600 hover:bg-red-50 rounded"
                  >
                    <Trash2 className="w-4 h-4" />
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

const PreviewPanel: React.FC<{
  previewData: PreviewRow[];
  columns: ColumnSchema[];
  isExpanded: boolean;
  onToggleExpand: () => void;
}> = ({ previewData, columns, isExpanded, onToggleExpand }) => {
  const [activeTab, setActiveTab] = useState<'preview' | 'test'>('preview');

  return (
    <div className={`h-full flex flex-col ${isExpanded ? 'absolute inset-4 z-50 bg-white border rounded-lg shadow-xl' : ''}`}>
      <div className="flex items-center justify-between p-4 border-b">
        <div className="flex space-x-2">
          <button
            className={`px-4 py-2 rounded-t-lg ${activeTab === 'preview' ? 'bg-blue-100 text-blue-700' : 'text-gray-600'}`}
            onClick={() => setActiveTab('preview')}
          >
            Preview
          </button>
          <button
            className={`px-4 py-2 rounded-t-lg ${activeTab === 'test' ? 'bg-blue-100 text-blue-700' : 'text-gray-600'}`}
            onClick={() => setActiveTab('test')}
          >
            Test
          </button>
        </div>
        <div className="flex items-center gap-2">
          <button className="p-2 hover:bg-gray-100 rounded">
            <RefreshCw className="w-4 h-4" />
          </button>
          <button
            onClick={onToggleExpand}
            className="p-2 hover:bg-gray-100 rounded"
          >
            {isExpanded ? (
              <Minimize2 className="w-4 h-4" />
            ) : (
              <Maximize2 className="w-4 h-4" />
            )}
          </button>
        </div>
      </div>

      <div className="flex-1 overflow-auto p-4">
        {activeTab === 'preview' && (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="sticky top-0 bg-white">
                <tr className="border-b">
                  <th className="p-2 text-left">ID</th>
                  {columns.map(col => (
                    <th key={col.id} className="p-2 text-left">
                      <div className="flex items-center gap-2">
                        {col.name}
                        {col.hasRule && (
                          <span className="text-xs bg-green-100 text-green-800 px-2 rounded">
                            Rule
                          </span>
                        )}
                      </div>
                    </th>
                  ))}
                  <th className="p-2 text-left">Status</th>
                </tr>
              </thead>
              <tbody>
                {previewData.slice(0, 10).map(row => (
                  <tr 
                    key={row.id} 
                    className={`border-b hover:bg-gray-50 ${
                      row._metadata?.isConflict ? 'bg-yellow-50' : ''
                    }`}
                  >
                    <td className="p-2">{row.id}</td>
                    {columns.map(col => (
                      <td key={col.id} className="p-2">
                        <div className={`${
                          row._metadata?.isMerged && row[col.name] 
                            ? 'bg-blue-50 text-blue-800 px-2 py-1 rounded'
                            : ''
                        }`}>
                          {row[col.name]?.toString() || '-'}
                        </div>
                      </td>
                    ))}
                    <td className="p-2">
                      {row._metadata?.isConflict && (
                        <span className="text-xs bg-red-100 text-red-800 px-2 py-1 rounded">
                          Conflict
                        </span>
                      )}
                      {row._metadata?.isMerged && !row._metadata?.isConflict && (
                        <span className="text-xs bg-green-100 text-green-800 px-2 py-1 rounded">
                          Merged
                        </span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {activeTab === 'test' && (
          <div className="space-y-4">
            <div className="border rounded-lg p-4">
              <h4 className="font-medium mb-2">Test Configuration</h4>
              <div className="space-y-2">
                <div>
                  <label className="block text-sm text-gray-600 mb-1">
                    Test Data Source
                  </label>
                  <select className="w-full px-3 py-2 border rounded text-sm">
                    <option>Sample Data</option>
                    <option>File Upload</option>
                    <option>Database Connection</option>
                  </select>
                </div>
                <button className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700">
                  Run Test
                </button>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

const AdvancedSettings: React.FC<{
  settings: EditorSettings;
  onSettingsChange: (settings: EditorSettings) => void;
  columns: ColumnSchema[];
}> = ({ settings, onSettingsChange, columns }) => {
  const [activeTab, setActiveTab] = useState<'matching' | 'survivorship'>('matching');
  const [isExpanded, setIsExpanded] = useState(false);

  const updateMatchingSettings = (updates: Partial<MatchingSettings>) => {
    onSettingsChange({
      ...settings,
      matching: { ...settings.matching, ...updates }
    });
  };

  const updateSurvivorshipSettings = (updates: Partial<SurvivorshipSettings>) => {
    onSettingsChange({
      ...settings,
      survivorship: { ...settings.survivorship, ...updates }
    });
  };

  return (
    <div className="border-t">
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        className="w-full flex items-center justify-between p-4 hover:bg-gray-50"
      >
        <div className="flex items-center gap-2">
          <Settings className="w-4 h-4" />
          <span className="font-medium">Advanced Settings</span>
        </div>
        <ChevronDown className={`w-4 h-4 transition-transform ${isExpanded ? 'rotate-180' : ''}`} />
      </button>

      {isExpanded && (
        <div className="px-4 pb-4 animate-slideDown">
          <div className="flex border-b mb-4">
            <button
              className={`px-4 py-2 ${activeTab === 'matching' ? 'border-b-2 border-blue-600 text-blue-600' : 'text-gray-600'}`}
              onClick={() => setActiveTab('matching')}
            >
              Matching Settings
            </button>
            <button
              className={`px-4 py-2 ${activeTab === 'survivorship' ? 'border-b-2 border-blue-600 text-blue-600' : 'text-gray-600'}`}
              onClick={() => setActiveTab('survivorship')}
            >
              Survivorship Settings
            </button>
          </div>

          {activeTab === 'matching' && (
            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium mb-2">
                  Blocking Keys
                </label>
                <select
                  multiple
                  value={settings.matching.blockingKeys}
                  onChange={(e) => {
                    const selected = Array.from(e.target.selectedOptions, opt => opt.value);
                    updateMatchingSettings({ blockingKeys: selected });
                  }}
                  className="w-full px-3 py-2 border rounded text-sm min-h-[100px]"
                >
                  {columns.map(col => (
                    <option key={col.id} value={col.name}>
                      {col.name} ({col.type})
                    </option>
                  ))}
                </select>
                <p className="text-xs text-gray-500 mt-1">
                  Hold Ctrl/Cmd to select multiple columns
                </p>
              </div>

              <div>
                <label className="block text-sm font-medium mb-2">
                  Similarity Threshold: {settings.matching.similarityThreshold}%
                </label>
                <input
                  type="range"
                  min="0"
                  max="100"
                  value={settings.matching.similarityThreshold}
                  onChange={(e) => updateMatchingSettings({ 
                    similarityThreshold: parseInt(e.target.value) 
                  })}
                  className="w-full"
                />
              </div>

              <div>
                <label className="block text-sm font-medium mb-2">
                  Algorithm
                </label>
                <div className="flex gap-4">
                  <label className="flex items-center">
                    <input
                      type="radio"
                      checked={settings.matching.algorithm === 'deterministic'}
                      onChange={() => updateMatchingSettings({ algorithm: 'deterministic' })}
                      className="mr-2"
                    />
                    Deterministic
                  </label>
                  <label className="flex items-center">
                    <input
                      type="radio"
                      checked={settings.matching.algorithm === 'probabilistic'}
                      onChange={() => updateMatchingSettings({ algorithm: 'probabilistic' })}
                      className="mr-2"
                    />
                    Probabilistic
                  </label>
                </div>
              </div>

              <div>
                <label className="block text-sm font-medium mb-2">
                  Similarity Metrics
                </label>
                <div className="space-y-2">
                  <label className="flex items-center">
                    <input
                      type="checkbox"
                      checked={settings.matching.similarityMetrics.jaroWinkler}
                      onChange={(e) => updateMatchingSettings({
                        similarityMetrics: {
                          ...settings.matching.similarityMetrics,
                          jaroWinkler: e.target.checked
                        }
                      })}
                      className="mr-2"
                    />
                    Jaro-Winkler
                  </label>
                  <label className="flex items-center">
                    <input
                      type="checkbox"
                      checked={settings.matching.similarityMetrics.levenshtein}
                      onChange={(e) => updateMatchingSettings({
                        similarityMetrics: {
                          ...settings.matching.similarityMetrics,
                          levenshtein: e.target.checked
                        }
                      })}
                      className="mr-2"
                    />
                    Levenshtein
                  </label>
                  <label className="flex items-center">
                    <input
                      type="checkbox"
                      checked={settings.matching.similarityMetrics.exact}
                      onChange={(e) => updateMatchingSettings({
                        similarityMetrics: {
                          ...settings.matching.similarityMetrics,
                          exact: e.target.checked
                        }
                      })}
                      className="mr-2"
                    />
                    Exact Match
                  </label>
                </div>
              </div>
            </div>
          )}

          {activeTab === 'survivorship' && (
            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium mb-2">
                  Custom Functions
                </label>
                <textarea
                  value={settings.survivorship.customFunctions.join('\n')}
                  onChange={(e) => updateSurvivorshipSettings({
                    customFunctions: e.target.value.split('\n').filter(line => line.trim())
                  })}
                  className="w-full px-3 py-2 border rounded text-sm font-mono min-h-[100px]"
                  placeholder="Enter custom functions, one per line..."
                />
              </div>

              <div>
                <label className="block text-sm font-medium mb-2">
                  Rejection Behavior
                </label>
                <select
                  value={settings.survivorship.rejectionBehavior}
                  onChange={(e) => updateSurvivorshipSettings({
                    rejectionBehavior: e.target.value as SurvivorshipSettings['rejectionBehavior']
                  })}
                  className="w-full px-3 py-2 border rounded text-sm"
                >
                  <option value="keep_as_rejected">Keep as Rejected</option>
                  <option value="exclude">Exclude from Output</option>
                  <option value="create_new_column">Create New Column</option>
                </select>
              </div>

              <div>
                <label className="block text-sm font-medium mb-2">
                  Column Precedence
                </label>
                <div className="border rounded p-2 space-y-2">
                  {settings.survivorship.columnPrecedence.map((column, index) => (
                    <div key={column} className="flex items-center justify-between p-2 bg-gray-50 rounded">
                      <span className="text-sm">{column}</span>
                      <div className="flex gap-1">
                        {index > 0 && (
                          <button
                            onClick={() => {
                              const newOrder = [...settings.survivorship.columnPrecedence];
                              [newOrder[index], newOrder[index - 1]] = [newOrder[index - 1], newOrder[index]];
                              updateSurvivorshipSettings({ columnPrecedence: newOrder });
                            }}
                            className="p-1 hover:bg-gray-200 rounded"
                          >
                            ↑
                          </button>
                        )}
                        {index < settings.survivorship.columnPrecedence.length - 1 && (
                          <button
                            onClick={() => {
                              const newOrder = [...settings.survivorship.columnPrecedence];
                              [newOrder[index], newOrder[index + 1]] = [newOrder[index + 1], newOrder[index]];
                              updateSurvivorshipSettings({ columnPrecedence: newOrder });
                            }}
                            className="p-1 hover:bg-gray-200 rounded"
                          >
                            ↓
                          </button>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

// ============ Main Component ============

const DataTransformationEditor: React.FC<DataTransformationEditorProps> = ({
  componentName,
  initialSchemas,
  onSave,
  onClose,
  initialData = [],
  initialSize = { width: 1200, height: 800 }
}) => {
  const [size] = useState(initialSize);
  const [] = useState(false);
  const [leftPanelSize, setLeftPanelSize] = useState(300);
  const [rightPanelSize, setRightPanelSize] = useState(400);
  const [previewExpanded, setPreviewExpanded] = useState(false);
  
  const [schemas] = useState<ColumnSchema[]>(initialSchemas);
  const [searchQuery, setSearchQuery] = useState('');
  
  const [rules, setRules] = useState<TransformationRule[]>([
    {
      id: 'rule_1',
      targetColumn: '',
      function: 'most_frequent',
      priority: 1,
      isValid: false
    }
  ]);
  
  const [previewData] = useState<PreviewRow[]>(initialData);
  const [settings, setSettings] = useState<EditorSettings>({
    matching: {
      blockingKeys: [],
      similarityThreshold: 85,
      algorithm: 'deterministic',
      similarityMetrics: {
        jaroWinkler: true,
        levenshtein: true,
        exact: true
      }
    },
    survivorship: {
      customFunctions: [],
      rejectionBehavior: 'keep_as_rejected',
      columnPrecedence: []
    }
  });

  const [validationStatus, setValidationStatus] = useState({
    isValid: false,
    message: 'Configure transformation rules'
  });

  const [isProcessing, setIsProcessing] = useState(false);

  // Validation effect
  useEffect(() => {
    const hasValidRules = rules.some(rule => rule.targetColumn && rule.isValid);
    const allRequired = rules.every(rule => 
      rule.targetColumn && rule.function && rule.priority > 0
    );

    setValidationStatus({
      isValid: hasValidRules && allRequired,
      message: hasValidRules 
        ? `${rules.filter(r => r.isValid).length} rules configured`
        : 'No valid rules configured'
    });
  }, [rules]);

  const handleSave = async () => {
    setIsProcessing(true);
    try {
      await onSave(rules, settings);
      setIsProcessing(false);
    } catch (error) {
      setIsProcessing(false);
      console.error('Save failed:', error);
    }
  };

  const handleApply = () => {
    // Apply changes without closing
    console.log('Changes applied');
  };

  const handleDragStart = useCallback((column: ColumnSchema) => {
    console.log('Dragging column:', column);
  }, []);

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div
        className="bg-white rounded-lg shadow-2xl flex flex-col"
        style={{
          width: size.width,
          height: size.height,
          maxWidth: '90vw',
          maxHeight: '90vh'
        }}
      >
        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b">
          <div className="flex items-center gap-4">
            <h2 className="text-lg font-semibold">
              {componentName} – Configure
            </h2>
            <button className="p-1 hover:bg-gray-100 rounded">
              <HelpCircle className="w-5 h-5 text-gray-500" />
            </button>
          </div>
          
          <div className="flex items-center gap-2">
            <button className="p-2 hover:bg-gray-100 rounded">
              <Undo2 className="w-4 h-4" />
            </button>
            <button className="p-2 hover:bg-gray-100 rounded">
              <Redo2 className="w-4 h-4" />
            </button>
            <button
              onClick={onClose}
              className="p-2 hover:bg-red-50 rounded text-red-600"
            >
              <X className="w-5 h-5" />
            </button>
          </div>
        </div>

        {/* Main Content */}
        <div className="flex-1 flex overflow-hidden">
          {/* Left Panel - Schema Selector */}
          <ResizablePanel
            initialSize={leftPanelSize}
            minSize={200}
            maxSize={500}
            direction="horizontal"
            onResize={setLeftPanelSize}
          >
            <SchemaSelector
              schemas={schemas}
              onDragStart={handleDragStart}
              searchQuery={searchQuery}
              onSearchChange={setSearchQuery}
            />
          </ResizablePanel>

          {/* Center Panel - Rules Editor */}
          <div className="flex-1 overflow-hidden">
            <RulesEditor
              rules={rules}
              columns={schemas}
              onRulesChange={setRules}
            />
            
            {/* Advanced Settings */}
            <AdvancedSettings
              settings={settings}
              onSettingsChange={setSettings}
              columns={schemas}
            />
          </div>

          {/* Right Panel - Preview */}
          {!previewExpanded && (
            <ResizablePanel
              initialSize={rightPanelSize}
              minSize={300}
              maxSize={800}
              direction="horizontal"
              onResize={setRightPanelSize}
            >
              <PreviewPanel
                previewData={previewData}
                columns={schemas}
                isExpanded={previewExpanded}
                onToggleExpand={() => setPreviewExpanded(!previewExpanded)}
              />
            </ResizablePanel>
          )}
        </div>

        {/* Footer */}
        <div className="border-t p-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-4">
              <div className={`flex items-center gap-2 ${
                validationStatus.isValid ? 'text-green-600' : 'text-yellow-600'
              }`}>
                {validationStatus.isValid ? (
                  <Check className="w-4 h-4" />
                ) : (
                  <AlertCircle className="w-4 h-4" />
                )}
                <span className="text-sm">{validationStatus.message}</span>
              </div>
              
              {isProcessing && (
                <div className="flex items-center gap-2 text-blue-600">
                  <Loader2 className="w-4 h-4 animate-spin" />
                  <span className="text-sm">Processing...</span>
                </div>
              )}
            </div>
            
            <div className="flex items-center gap-3">
              <button
                onClick={onClose}
                className="px-4 py-2 border rounded-lg hover:bg-gray-50"
              >
                Cancel
              </button>
              <button
                onClick={handleApply}
                className="px-4 py-2 border border-blue-600 text-blue-600 rounded-lg hover:bg-blue-50"
              >
                Apply
              </button>
              <button
                onClick={handleSave}
                disabled={!validationStatus.isValid || isProcessing}
                className={`px-4 py-2 rounded-lg flex items-center gap-2 ${
                  validationStatus.isValid && !isProcessing
                    ? 'bg-blue-600 text-white hover:bg-blue-700'
                    : 'bg-gray-300 text-gray-500 cursor-not-allowed'
                }`}
              >
                <Save className="w-4 h-4" />
                Save Configuration
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default DataTransformationEditor;