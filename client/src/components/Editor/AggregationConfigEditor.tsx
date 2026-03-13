import React, { useState, useCallback, useMemo } from 'react';
import { 
  DragDropContext, 
  Droppable, 
  Draggable,
  DropResult,
  DraggableProvided,
  DroppableProvided 
} from 'react-beautiful-dnd';
import { 
  Plus, Trash2, GripVertical, Filter, 
  ArrowUpDown, Hash, Sigma, 
  Maximize2, Minimize2, BarChart3,
  TrendingUp, Calculator, Save} from 'lucide-react';

// ============ TYPES ============
interface SchemaField {
  id: string;
  name: string;
  type: 'string' | 'number' | 'boolean' | 'date';
}

interface AggregationConfig {
  groupFields: string[];
  aggregations: Array<{
    id: string;
    outputField: string;
    function: 'sum' | 'avg' | 'min' | 'max' | 'count' | 'count-distinct' | 'stddev' | 'variance';
    inputField: string;
    filter?: string;
    distinct?: boolean;
  }>;
  havingClause?: string;
  sortResults?: boolean;
  sortField?: string;
  sortDirection?: 'asc' | 'desc';
  includeNullGroups?: boolean;
  includeGroupCount?: boolean;
}

interface AggregationTemplate {
  id: string;
  name: string;
  description?: string;
  config: AggregationConfig;
}

// ============ CONSTANTS ============
const AGGREGATION_FUNCTIONS = [
  { value: 'sum', label: 'Sum', icon: <Sigma size={16} />, color: 'bg-blue-100 text-blue-800' },
  { value: 'avg', label: 'Average', icon: <BarChart3 size={16} />, color: 'bg-green-100 text-green-800' },
  { value: 'min', label: 'Minimum', icon: <Minimize2 size={16} />, color: 'bg-purple-100 text-purple-800' },
  { value: 'max', label: 'Maximum', icon: <Maximize2 size={16} />, color: 'bg-red-100 text-red-800' },
  { value: 'count', label: 'Count', icon: <Hash size={16} />, color: 'bg-yellow-100 text-yellow-800' },
  { value: 'count-distinct', label: 'Count Distinct', icon: <Hash size={16} />, color: 'bg-orange-100 text-orange-800' },
  { value: 'stddev', label: 'Std Dev', icon: <TrendingUp size={16} />, color: 'bg-indigo-100 text-indigo-800' },
  { value: 'variance', label: 'Variance', icon: <Calculator size={16} />, color: 'bg-pink-100 text-pink-800' },
];

const FUNCTION_TYPE_COMPATIBILITY = {
  'sum': ['number'],
  'avg': ['number'],
  'min': ['number', 'date'],
  'max': ['number', 'date'],
  'count': ['string', 'number', 'boolean', 'date'],
  'count-distinct': ['string', 'number', 'boolean', 'date'],
  'stddev': ['number'],
  'variance': ['number'],
};

// ============ MAIN COMPONENT ============
export const AggregationConfigEditor: React.FC = () => {
  const [config, setConfig] = useState<AggregationConfig>({
    groupFields: [],
    aggregations: [],
    includeNullGroups: false,
    includeGroupCount: false,
    sortResults: false,
    sortDirection: 'asc'
  });

  const [schema] = useState<SchemaField[]>([
    { id: '1', name: 'customer_id', type: 'string' },
    { id: '2', name: 'order_date', type: 'date' },
    { id: '3', name: 'product_category', type: 'string' },
    { id: '4', name: 'product_name', type: 'string' },
    { id: '5', name: 'quantity', type: 'number' },
    { id: '6', name: 'unit_price', type: 'number' },
    { id: '7', name: 'discount', type: 'number' },
    { id: '8', name: 'shipping_cost', type: 'number' },
    { id: '9', name: 'region', type: 'string' },
    { id: '10', name: 'is_premium', type: 'boolean' },
  ]);

  const [templates] = useState<AggregationTemplate[]>([
    {
      id: '1',
      name: 'Sales Summary',
      description: 'Total sales by category and region',
      config: {
        groupFields: ['product_category', 'region'],
        aggregations: [
          { id: 'a1', outputField: 'total_sales', function: 'sum', inputField: 'unit_price' },
          { id: 'a2', outputField: 'total_quantity', function: 'sum', inputField: 'quantity' },
          { id: 'a3', outputField: 'avg_price', function: 'avg', inputField: 'unit_price' },
        ],
        includeGroupCount: true,
        sortResults: true,
        sortField: 'total_sales',
        sortDirection: 'desc'
      }
    }
  ]);

  // ============ HANDLERS ============
  const handleDragEnd = useCallback((result: DropResult) => {
    const { source, destination, type } = result;

    if (!destination) return;

    if (type === 'GROUP_FIELDS') {
      const items = Array.from(config.groupFields);
      const [reorderedItem] = items.splice(source.index, 1);
      items.splice(destination.index, 0, reorderedItem);
      setConfig(prev => ({ ...prev, groupFields: items }));
    }
  }, [config.groupFields]);

  const addAggregation = () => {
    const newId = `agg_${Date.now()}`;
    setConfig(prev => ({
      ...prev,
      aggregations: [
        ...prev.aggregations,
        {
          id: newId,
          outputField: `new_field_${prev.aggregations.length + 1}`,
          function: 'sum',
          inputField: '',
          filter: '',
          distinct: false
        }
      ]
    }));
  };

  const updateAggregation = (id: string, updates: Partial<AggregationConfig['aggregations'][0]>) => {
    setConfig(prev => ({
      ...prev,
      aggregations: prev.aggregations.map(agg => 
        agg.id === id ? { ...agg, ...updates } : agg
      )
    }));
  };

  const removeAggregation = (id: string) => {
    setConfig(prev => ({
      ...prev,
      aggregations: prev.aggregations.filter(agg => agg.id !== id)
    }));
  };

  const addGroupField = (fieldName: string) => {
    if (!config.groupFields.includes(fieldName)) {
      setConfig(prev => ({
        ...prev,
        groupFields: [...prev.groupFields, fieldName]
      }));
    }
  };

  const removeGroupField = (fieldName: string) => {
    setConfig(prev => ({
      ...prev,
      groupFields: prev.groupFields.filter(f => f !== fieldName)
    }));
  };

  const getCompatibleFields = useCallback((functionType?: string) => {
    if (!functionType) return schema;
    const allowedTypes = FUNCTION_TYPE_COMPATIBILITY[functionType as keyof typeof FUNCTION_TYPE_COMPATIBILITY] || [];
    return schema.filter(field => allowedTypes.includes(field.type));
  }, [schema]);

  const outputSchema = useMemo(() => {
    const output: Array<{ name: string; type: string }> = [];
    
    // Group fields
    config.groupFields.forEach(fieldName => {
      const field = schema.find(f => f.name === fieldName);
      if (field) {
        output.push({ name: fieldName, type: field.type });
      }
    });

    // Aggregation fields
    config.aggregations.forEach(agg => {
      output.push({ 
        name: agg.outputField, 
        type: getOutputType(agg.function, agg.inputField) 
      });
    });

    // Group count
    if (config.includeGroupCount) {
      output.push({ name: 'group_count', type: 'number' });
    }

    return output;
  }, [config, schema]);

  const getOutputType = (func: string, inputField: string): string => {
    if (func === 'count' || func === 'count-distinct') return 'number';
    const field = schema.find(f => f.name === inputField);
    return func === 'avg' ? 'number' : field?.type || 'unknown';
  };

  const validateConfig = () => {
    const errors: string[] = [];

    // Check for duplicate output field names
    const outputNames = outputSchema.map(f => f.name);
    const duplicates = outputNames.filter((name, index) => outputNames.indexOf(name) !== index);
    if (duplicates.length > 0) {
      errors.push(`Duplicate output field names: ${duplicates.join(', ')}`);
    }

    // Validate aggregation input fields
    config.aggregations.forEach(agg => {
      const field = schema.find(f => f.name === agg.inputField);
      if (!field && agg.function !== 'count') {
        errors.push(`Aggregation "${agg.outputField}": Input field "${agg.inputField}" not found`);
      }
    });

    return errors;
  };

  const exportConfig = () => {
    const exportData = {
      ...config,
      aggregations: config.aggregations.map(({ id, ...rest }) => rest)
    };
    return JSON.stringify(exportData, null, 2);
  };

  const loadTemplate = (template: AggregationTemplate) => {
    setConfig({
      ...template.config,
      aggregations: template.config.aggregations.map(agg => ({
        ...agg,
        id: `agg_${Date.now()}_${Math.random()}`
      }))
    });
  };

  return (
    <DragDropContext onDragEnd={handleDragEnd}>
      <div className="flex h-screen bg-gray-50">
        {/* Left Panel - Schema */}
        <div className="w-1/4 border-r bg-white p-4">
          <div className="mb-6">
            <h2 className="text-lg font-semibold text-gray-800 mb-3">Input Schema</h2>
            <div className="space-y-2">
              {schema.map(field => (
                <div
                  key={field.id}
                  draggable
                  onDragStart={(e: React.DragEvent<HTMLDivElement>) => {
                    e.dataTransfer.setData('text/plain', field.name);
                    e.dataTransfer.setData('field-type', field.type);
                  }}
                  className="flex items-center justify-between p-3 bg-gray-50 border rounded-lg cursor-move hover:bg-blue-50 hover:border-blue-200 transition-colors group"
                >
                  <div className="flex items-center space-x-3">
                    <GripVertical className="text-gray-400 group-hover:text-gray-600" />
                    <div>
                      <div className="font-medium text-gray-800">{field.name}</div>
                      <div className="text-xs text-gray-500 capitalize">{field.type}</div>
                    </div>
                  </div>
                  <button
                    onClick={() => addGroupField(field.name)}
                    className="text-sm text-blue-600 hover:text-blue-800 opacity-0 group-hover:opacity-100 transition-opacity"
                  >
                    Add to Group
                  </button>
                </div>
              ))}
            </div>
          </div>

          {/* Templates Section */}
          <div className="mt-8">
            <div className="flex items-center justify-between mb-3">
              <h3 className="font-semibold text-gray-800">Templates</h3>
              <button className="text-sm text-blue-600 hover:text-blue-800">
                <Save size={16} />
              </button>
            </div>
            <div className="space-y-2">
              {templates.map(template => (
                <div
                  key={template.id}
                  onClick={() => loadTemplate(template)}
                  className="p-3 border rounded-lg cursor-pointer hover:bg-gray-50"
                >
                  <div className="font-medium text-gray-800">{template.name}</div>
                  {template.description && (
                    <div className="text-sm text-gray-600 mt-1">{template.description}</div>
                  )}
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Right Panel - Aggregation Builder */}
        <div className="flex-1 p-6 overflow-auto">
          <div className="max-w-6xl mx-auto">
            {/* Pipeline Visualization */}
            <div className="flex items-center justify-center mb-8">
              <div className="flex items-center space-x-4">
                {['Group', 'Aggregate', 'Filter', 'Sort'].map((step, idx) => (
                  <div key={step} className="flex items-center">
                    <div className="flex flex-col items-center">
                      <div className={`w-12 h-12 rounded-full flex items-center justify-center
                        ${idx === 0 ? 'bg-blue-100 text-blue-600' :
                          idx === 1 ? 'bg-green-100 text-green-600' :
                          idx === 2 ? 'bg-purple-100 text-purple-600' :
                          'bg-yellow-100 text-yellow-600'}`}>
                        {idx === 0 ? <Hash size={20} /> :
                         idx === 1 ? <Sigma size={20} /> :
                         idx === 2 ? <Filter size={20} /> :
                         <ArrowUpDown size={20} />}
                      </div>
                      <span className="text-sm font-medium mt-2">{step}</span>
                    </div>
                    {idx < 3 && (
                      <div className="w-16 h-1 bg-gray-300 mx-4"></div>
                    )}
                  </div>
                ))}
              </div>
            </div>

            {/* Group Fields Section */}
            <div className="mb-8">
              <div className="flex items-center justify-between mb-4">
                <h2 className="text-xl font-semibold text-gray-800">Group By</h2>
                <label className="flex items-center space-x-2 text-sm">
                  <input
                    type="checkbox"
                    checked={config.includeNullGroups}
                    onChange={(e) => setConfig(prev => ({ ...prev, includeNullGroups: e.target.checked }))}
                    className="rounded"
                  />
                  <span>Include null groups</span>
                </label>
              </div>

              <Droppable droppableId="group-fields" type="GROUP_FIELDS">
                {(provided: DroppableProvided) => (
                  <div
                    ref={provided.innerRef}
                    {...provided.droppableProps}
                    className="min-h-[120px] border-2 border-dashed border-gray-300 rounded-lg p-4 bg-gray-50"
                  >
                    {config.groupFields.length === 0 ? (
                      <div className="text-center text-gray-500 py-8">
                        Drag fields here to group by
                      </div>
                    ) : (
                      <div className="space-y-2">
                        {config.groupFields.map((fieldName, index) => {
                          const field = schema.find(f => f.name === fieldName);
                          return (
                            <Draggable key={fieldName} draggableId={fieldName} index={index}>
                              {(provided: DraggableProvided) => (
                                <div
                                  ref={provided.innerRef}
                                  {...provided.draggableProps}
                                  className="flex items-center justify-between p-3 bg-white border rounded-lg shadow-sm"
                                >
                                  <div className="flex items-center space-x-3">
                                    <div {...provided.dragHandleProps}>
                                      <GripVertical className="text-gray-400" />
                                    </div>
                                    <div className="flex items-center space-x-2">
                                      <div className="px-2 py-1 bg-blue-100 text-blue-800 text-xs font-medium rounded">
                                        {field?.type}
                                      </div>
                                      <span className="font-medium">{fieldName}</span>
                                    </div>
                                    <div className="text-sm text-gray-500">
                                      Group {index + 1}
                                    </div>
                                  </div>
                                  <button
                                    onClick={() => removeGroupField(fieldName)}
                                    className="text-red-600 hover:text-red-800"
                                  >
                                    <Trash2 size={16} />
                                  </button>
                                </div>
                              )}
                            </Draggable>
                          );
                        })}
                        {provided.placeholder}
                      </div>
                    )}
                  </div>
                )}
              </Droppable>
            </div>

            {/* Aggregations Section */}
            <div className="mb-8">
              <div className="flex items-center justify-between mb-4">
                <h2 className="text-xl font-semibold text-gray-800">Aggregations</h2>
                <button
                  onClick={addAggregation}
                  className="flex items-center space-x-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
                >
                  <Plus size={16} />
                  <span>Add Aggregation</span>
                </button>
              </div>

              <div className="space-y-4">
                {config.aggregations.map((agg, index) => {
                  const funcConfig = AGGREGATION_FUNCTIONS.find(f => f.value === agg.function);
                  return (
                    <div key={agg.id} className="border rounded-lg bg-white p-4 shadow-sm">
                      <div className="flex items-center justify-between mb-4">
                        <div className="flex items-center space-x-3">
                          <div className={`px-3 py-1 rounded-full flex items-center space-x-2 ${funcConfig?.color}`}>
                            {funcConfig?.icon}
                            <span className="font-medium">{funcConfig?.label}</span>
                          </div>
                          <div className="text-sm text-gray-500">
                            Aggregation {index + 1}
                          </div>
                        </div>
                        <button
                          onClick={() => removeAggregation(agg.id)}
                          className="text-red-600 hover:text-red-800"
                        >
                          <Trash2 size={16} />
                        </button>
                      </div>

                      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                        {/* Function Selector */}
                        <div>
                          <label className="block text-sm font-medium text-gray-700 mb-1">
                            Function
                          </label>
                          <select
                            value={agg.function}
                            onChange={(e) => updateAggregation(agg.id, { 
                              function: e.target.value as any 
                            })}
                            className="w-full p-2 border rounded-lg"
                          >
                            {AGGREGATION_FUNCTIONS.map(func => (
                              <option key={func.value} value={func.value}>
                                {func.label}
                              </option>
                            ))}
                          </select>
                        </div>

                        {/* Input Field */}
                        <div>
                          <label className="block text-sm font-medium text-gray-700 mb-1">
                            Input Field
                          </label>
                          <select
                            value={agg.inputField}
                            onChange={(e) => updateAggregation(agg.id, { 
                              inputField: e.target.value 
                            })}
                            className="w-full p-2 border rounded-lg"
                          >
                            <option value="">Select field...</option>
                            {getCompatibleFields(agg.function).map(field => (
                              <option key={field.id} value={field.name}>
                                {field.name} ({field.type})
                              </option>
                            ))}
                          </select>
                        </div>

                        {/* Output Field */}
                        <div>
                          <label className="block text-sm font-medium text-gray-700 mb-1">
                            Output Field Name
                          </label>
                          <input
                            type="text"
                            value={agg.outputField}
                            onChange={(e) => updateAggregation(agg.id, { 
                              outputField: e.target.value 
                            })}
                            className="w-full p-2 border rounded-lg"
                            placeholder="Enter field name..."
                          />
                        </div>

                        {/* Filter Expression */}
                        <div className="md:col-span-2">
                          <label className="block text-sm font-medium text-gray-700 mb-1">
                            Filter Expression (Optional)
                          </label>
                          <input
                            type="text"
                            value={agg.filter || ''}
                            onChange={(e) => updateAggregation(agg.id, { 
                              filter: e.target.value 
                            })}
                            className="w-full p-2 border rounded-lg"
                            placeholder="e.g., quantity > 0"
                          />
                        </div>

                        {/* Distinct Checkbox */}
                        <div className="flex items-center">
                          <label className="flex items-center space-x-2">
                            <input
                              type="checkbox"
                              checked={agg.distinct || false}
                              onChange={(e) => updateAggregation(agg.id, { 
                                distinct: e.target.checked 
                              })}
                              className="rounded"
                              disabled={!agg.function.includes('count')}
                            />
                            <span className="text-sm text-gray-700">Distinct</span>
                          </label>
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>

            {/* Having Clause */}
            <div className="mb-8">
              <h2 className="text-xl font-semibold text-gray-800 mb-4">Having Clause</h2>
              <div className="border rounded-lg bg-white p-4">
                <textarea
                  value={config.havingClause || ''}
                  onChange={(e) => setConfig(prev => ({ ...prev, havingClause: e.target.value }))}
                  className="w-full p-3 border rounded-lg font-mono text-sm"
                  rows={3}
                  placeholder="e.g., total_sales > 1000 AND avg_price < 500"
                />
                <div className="mt-2 text-sm text-gray-600">
                  Available aggregated fields: {outputSchema.map(f => f.name).join(', ')}
                </div>
              </div>
            </div>

            {/* Results Configuration */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
              <div>
                <h2 className="text-xl font-semibold text-gray-800 mb-4">Results</h2>
                <div className="space-y-4 bg-white border rounded-lg p-4">
                  <label className="flex items-center justify-between">
                    <div className="flex items-center space-x-2">
                      <input
                        type="checkbox"
                        checked={config.includeGroupCount || false}
                        onChange={(e) => setConfig(prev => ({ ...prev, includeGroupCount: e.target.checked }))}
                        className="rounded"
                      />
                      <span>Include group count</span>
                    </div>
                    <Hash className="text-gray-400" size={16} />
                  </label>

                  <div className="border-t pt-4">
                    <label className="flex items-center justify-between mb-2">
                      <div className="flex items-center space-x-2">
                        <input
                          type="checkbox"
                          checked={config.sortResults || false}
                          onChange={(e) => setConfig(prev => ({ 
                            ...prev, 
                            sortResults: e.target.checked 
                          }))}
                          className="rounded"
                        />
                        <span>Sort results</span>
                      </div>
                      <ArrowUpDown className="text-gray-400" size={16} />
                    </label>
                    
                    {config.sortResults && (
                      <div className="ml-6 space-y-2">
                        <select
                          value={config.sortField || ''}
                          onChange={(e) => setConfig(prev => ({ ...prev, sortField: e.target.value }))}
                          className="w-full p-2 border rounded-lg"
                        >
                          <option value="">Select field...</option>
                          {outputSchema.map(field => (
                            <option key={field.name} value={field.name}>
                              {field.name}
                            </option>
                          ))}
                        </select>
                        <select
                          value={config.sortDirection || 'asc'}
                          onChange={(e) => setConfig(prev => ({ 
                            ...prev, 
                            sortDirection: e.target.value as 'asc' | 'desc' 
                          }))}
                          className="w-full p-2 border rounded-lg"
                        >
                          <option value="asc">Ascending</option>
                          <option value="desc">Descending</option>
                        </select>
                      </div>
                    )}
                  </div>
                </div>
              </div>

              {/* Output Schema Preview */}
              <div>
                <h2 className="text-xl font-semibold text-gray-800 mb-4">Output Schema Preview</h2>
                <div className="border rounded-lg bg-white p-4">
                  <div className="space-y-2">
                    {outputSchema.map((field, index) => (
                      <div key={field.name} className="flex items-center justify-between p-2 hover:bg-gray-50">
                        <div className="flex items-center space-x-3">
                          <div className="w-6 h-6 rounded-full bg-gray-100 flex items-center justify-center text-xs">
                            {index + 1}
                          </div>
                          <div>
                            <div className="font-medium text-gray-800">{field.name}</div>
                            <div className="text-xs text-gray-500">{field.type}</div>
                          </div>
                        </div>
                        {config.groupFields.includes(field.name) ? (
                          <span className="px-2 py-1 bg-blue-100 text-blue-800 text-xs rounded">
                            Group
                          </span>
                        ) : field.name === 'group_count' ? (
                          <span className="px-2 py-1 bg-yellow-100 text-yellow-800 text-xs rounded">
                            Count
                          </span>
                        ) : (
                          <span className="px-2 py-1 bg-green-100 text-green-800 text-xs rounded">
                            Aggregation
                          </span>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            </div>

            {/* Validation and Export */}
            <div className="mt-8 pt-6 border-t">
              <div className="flex items-center justify-between">
                <div>
                  {validateConfig().length > 0 ? (
                    <div className="text-red-600">
                      <div className="font-medium mb-1">Validation Errors:</div>
                      <ul className="text-sm space-y-1">
                        {validateConfig().map((error, idx) => (
                          <li key={idx}>• {error}</li>
                        ))}
                      </ul>
                    </div>
                  ) : (
                    <div className="text-green-600 font-medium">
                      ✓ Configuration is valid
                    </div>
                  )}
                </div>
                
                <div className="space-x-4">
                  <button
                    onClick={() => navigator.clipboard.writeText(exportConfig())}
                    className="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50"
                  >
                    Copy Configuration
                  </button>
                  <button
                    onClick={() => {
                      const blob = new Blob([exportConfig()], { type: 'application/json' });
                      const url = URL.createObjectURL(blob);
                      const a = document.createElement('a');
                      a.href = url;
                      a.download = 'aggregation-config.json';
                      a.click();
                    }}
                    className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
                  >
                    Export Configuration
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </DragDropContext>
  );
};

// ============ STYLES ============
const styles = `
  body {
    margin: 0;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  }

  .drag-handle {
    cursor: grab;
  }

  .drag-handle:active {
    cursor: grabbing;
  }

  /* Custom scrollbar */
  ::-webkit-scrollbar {
    width: 8px;
    height: 8px;
  }

  ::-webkit-scrollbar-track {
    background: #f1f1f1;
  }

  ::-webkit-scrollbar-thumb {
    background: #888;
    border-radius: 4px;
  }

  ::-webkit-scrollbar-thumb:hover {
    background: #555;
  }
`;

// ============ USAGE EXAMPLE ============
export const UsageExample: React.FC = () => {
  return (
    <>
      <style>{styles}</style>
      <div className="min-h-screen bg-gray-50">
        <header className="bg-white border-b shadow-sm">
          <div className="max-w-7xl mx-auto px-4 py-4">
            <h1 className="text-2xl font-bold text-gray-900">
              Aggregate Row Configuration Editor
            </h1>
            <p className="text-gray-600 mt-1">
              Configure advanced data aggregations with grouping, filtering, and sorting
            </p>
          </div>
        </header>
        <AggregationConfigEditor />
      </div>
    </>
  );
};

export default AggregationConfigEditor;