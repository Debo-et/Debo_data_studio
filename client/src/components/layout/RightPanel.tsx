// src/components/layout/RightPanel.tsx - WITHOUT PERSISTENCE
import React, { useState, useEffect, useReducer } from 'react';
import { 
  setRightPanelView, 
  setNodePropertiesTab, 
  clearSelectedComponentMetadata 
} from '../../store/slices/uiSlice';
import { motion } from 'framer-motion';
import { useAppSelector, useAppDispatch } from '../../hooks';
import { updateNode } from '../../store/slices/nodesSlice';
import { addLog } from '../../store/slices/logsSlice';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../ui/card';
import { Input } from '../ui/input';
import { Label } from '../ui/label';
import { Textarea } from '../ui/textarea';
import { Switch } from '../ui/switch';
import { Badge } from '../ui/badge';
import { Button } from '../ui/Button';
import { 
  Database, 
  Settings, 
  RotateCcw,
  ChevronDown,
  ChevronRight,
  Grid3X3,
  Settings as SettingsIcon,
  X,
  Maximize2,
  Minimize2,
  Check,
  ChevronLeft,
  AlertCircle,
  Cpu,
  Search,
  Shield,
  GitMerge,
  Filter,
  Map} from 'lucide-react';

// Import UNIFIED Component Registry
import { 
  COMPONENT_REGISTRY, 
  getCategoryColor, 
  getComponentsBySource,
  ComponentDefinition,
  ComponentCategory 
} from '../../pages/ComponentRegistry';

// ==================== REACT FLOW DRAG-AND-DROP INTERFACES ====================
export interface ReactFlowDragData {
  type: 'reactflow-component';
  componentId: string;
  source: 'sidebar' | 'rightPanel';
  metadata?: Record<string, any>;
}

// ==================== SHARED CONSTANTS AND TYPES ====================
export type PanelComponentType = string;

export interface ComponentConfig {
  id: string;
  type: PanelComponentType;
  name: string;
  description: string;
  icon?: string;
}

export interface SchemaField {
  id: string;
  name: string;
  type: string;
  length?: number;
  nullable: boolean;
  included: boolean;
  description?: string;
  defaultValue?: any;
}

export interface FilterCondition {
  id: string;
  column: string;
  operator: 'equals' | 'notEquals' | 'contains' | 'startsWith' | 'endsWith' | 'greaterThan' | 'lessThan' | 'between';
  value: string;
  value2?: string;
  logicalConnector: 'AND' | 'OR';
}

export interface AggregationConfig {
  column: string;
  function: 'sum' | 'avg' | 'min' | 'max' | 'count' | 'countDistinct';
  outputName: string;
}

export interface TypeConversion {
  id: string;
  sourceType: string;
  targetType: string;
  format?: string;
  precision?: number;
  scale?: number;
}

export interface ReplaceRule {
  id: string;
  searchValue: string;
  replacement: string;
  caseSensitive: boolean;
  regex: boolean;
  scope: 'all' | 'first' | 'last';
}

export interface ExtractConfig {
  delimiter?: string;
  regexPattern?: string;
  xpath?: string;
  jsonPath?: string;
  outputColumns: Array<{
    name: string;
    type: string;
    path: string;
    length?: number;
  }>;
}

export interface RowGeneratorConfig {
  rowCount: number;
  seed: number;
  useSeed: boolean;
  columns: Array<{
    name: string;
    type: string;
    function: string;
    parameters: Record<string, any>;
  }>;
}

export interface AdvancedOptions {
  errorHandling: 'fail' | 'skip' | 'default';
  emptyValueHandling: 'skip' | 'default' | 'null';
  parallelization: boolean;
  maxThreads: number;
  batchSize: number;
}

export interface StatusMessage {
  id: string;
  type: 'info' | 'warning' | 'error';
  message: string;
  details?: string;
}

export interface BasicSettingsState {
  component: ComponentConfig;
  inputSchema: SchemaField[];
  outputSchema: SchemaField[];
  filterConditions: FilterCondition[];
  aggregationConfig: {
    groupByColumns: string[];
    aggregations: AggregationConfig[];
  };
  typeConversions: TypeConversion[];
  replaceRules: ReplaceRule[];
  extractConfig: ExtractConfig;
  rowGeneratorConfig: RowGeneratorConfig;
  advancedOptions: AdvancedOptions;
  status: {
    messages: StatusMessage[];
    hasWarnings: boolean;
    hasErrors: boolean;
  };
}

// ==================== COMPONENT PALETTE DATA ====================
interface PaletteComponent {
  id: string;
  name: string;
  icon: React.ReactElement;
  description: string;
  compactIcon: React.ReactElement;
  iconType?: string;
  category: ComponentCategory;
  definition: ComponentDefinition;
}

interface ComponentCategoryData {
  title: string;
  icon: React.ReactElement;
  components: PaletteComponent[];
}

interface ComponentCategories {
  [key: string]: ComponentCategoryData;
}

// ==================== REACT FLOW DRAG HANDLERS ====================
const handleReactFlowDragStart = (event: React.DragEvent, component: PaletteComponent) => {
  const definition = component.definition;
  const categoryColor = getCategoryColor(definition.category);
  
  // Create unified React Flow drag data
  const dragData: ReactFlowDragData = {
    type: 'reactflow-component',
    componentId: component.id,
    source: 'rightPanel',
    metadata: {
      description: definition.description,
      category: definition.category,
      createdAt: new Date().toISOString(),
      version: '1.0',
      isRepositoryNode: false,
      componentCategory: definition.category,
      originalNodeName: definition.displayName,
      originalNodeType: component.id,
      talendDefinition: definition,
      defaultWidth: definition.defaultDimensions.width,
      defaultHeight: definition.defaultDimensions.height,
      defaultRole: definition.defaultRole
    }
  };
  
  console.log('📤 RightPanel drag started:', {
    component: definition.displayName,
    componentId: component.id,
    dragData
  });
  
  // Clear previous data
  event.dataTransfer.clearData();
  
  // Set unified React Flow data format
  event.dataTransfer.setData('application/reactflow', JSON.stringify(dragData));
  event.dataTransfer.setData('text/plain', definition.displayName);
  
  // Set drag effect
  event.dataTransfer.effectAllowed = 'copy';
  
  // Create Talend-style drag image
  const dragImage = document.createElement('div');
  
  dragImage.style.cssText = `
    position: absolute;
    top: -1000px;
    left: -1000px;
    background: linear-gradient(135deg, ${categoryColor}15 0%, ${categoryColor}08 100%);
    border: 2px solid ${categoryColor}40;
    color: #374151;
    padding: 8px 12px;
    border-radius: 12px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.1);
    font-size: 12px;
    font-weight: 600;
    z-index: 10000;
    display: flex;
    align-items: center;
    gap: 8px;
    min-width: 120px;
  `;
  
  dragImage.innerHTML = `
    <div style="
      width: 24px;
      height: 24px;
      border-radius: 50%;
      background: linear-gradient(135deg, ${categoryColor} 0%, ${categoryColor}CC 100%);
      display: flex;
      align-items: center;
      justify-content: center;
      color: white;
      flex-shrink: 0;
    ">
      ${definition.displayName.charAt(0)}
    </div>
    <span style="white-space: nowrap;">${definition.displayName}</span>
  `;
  
  document.body.appendChild(dragImage);
  event.dataTransfer.setDragImage(dragImage, 60, 15);
  setTimeout(() => document.body.removeChild(dragImage), 0);
};

// ==================== COMPONENT PALETTE (TALEND STYLE) ====================
interface ComponentPaletteProps {
  expandedCategories: { [key: string]: boolean };
  toggleCategory: (categoryKey: string) => void;
}

const ComponentPalette: React.FC<ComponentPaletteProps> = ({ expandedCategories, toggleCategory }) => {
  // Get all components from the unified registry with source 'rightPanel'
  const allComponents = getComponentsBySource('rightPanel');
  
  // Create palette components from unified registry with proper type safety
  const paletteComponents: PaletteComponent[] = allComponents.map((definition) => {
    const iconElement = React.isValidElement(definition.icon) 
      ? definition.icon 
      : React.createElement('div', {}, definition.displayName.charAt(0));
    
    const compactIcon = React.isValidElement(definition.icon)
      ? React.cloneElement(definition.icon, { className: 'w-4 h-4' })
      : iconElement;
    
    return {
      id: definition.id,
      name: definition.displayName,
      icon: iconElement,
      compactIcon,
      description: definition.description,
      iconType: definition.category,
      category: definition.category,
      definition
    };
  });

  // Group components by category (transform only for rightPanel)
  const componentCategories: ComponentCategories = {
    mappingTransformation: {
      title: 'Mapping & Transformation',
      icon: React.createElement(Map, { className: "w-4 h-4" }),
      components: paletteComponents.filter(c => 
        ['tMap', 'tSchemaMapper', 'tTypeConverter', 'tExpression'].includes(c.id)
      )
    },
    rowProcessing: {
      title: 'Row Processing',
      icon: React.createElement(Filter, { className: "w-4 h-4" }),
      components: paletteComponents.filter(c => 
        ['tFilterRow', 'tSortRow', 'tAggregateRow', 'tNormalize', 'tDenormalize'].includes(c.id)
      )
    },
    dataCombination: {
      title: 'Data Combination',
      icon: React.createElement(GitMerge, { className: "w-4 h-4" }),
      components: paletteComponents.filter(c => 
        ['tJoin', 'tSplitRow', 'tMatchGroup'].includes(c.id)
      )
    },
    dataQuality: {
      title: 'Data Quality',
      icon: React.createElement(Shield, { className: "w-4 h-4" }),
      components: paletteComponents.filter(c => 
        ['tDataQuality', 'tRegexExtract'].includes(c.id)
      )
    },
    advancedProcessing: {
      title: 'Advanced Processing',
      icon: React.createElement(Cpu, { className: "w-4 h-4" }),
      components: paletteComponents.filter(c => 
        ['tPivot', 'tUnpivot', 'tRowGenerator', 'tWebService', 'tLookup', 'tCache', 'tHash', 'tEncrypt', 'tDecrypt'].includes(c.id)
      )
    }
  };

  return (
    <div className="h-full overflow-y-auto">
      <Card className="bg-gray-800/50 border-gray-700 backdrop-blur-sm rounded-none border-b-0 h-full">
        <CardHeader className="pb-3">
        </CardHeader>
        <CardContent className="space-y-2">
          <div className="space-y-1">
            {Object.entries(componentCategories).map(([key, category]) => (
              <div key={key} className="border border-gray-600 rounded-lg overflow-hidden">
                <button
                  onClick={() => toggleCategory(key)}
                  className="w-full flex items-center justify-between p-3 bg-gray-700/50 hover:bg-gray-600/50 transition-colors text-white"
                >
                  <div className="flex items-center space-x-3">
                    <div className="text-gray-300">{category.icon}</div>
                    <span className="text-sm font-medium">{category.title}</span>
                    <Badge variant="secondary" className="bg-gray-600 text-gray-300">
                      {category.components.length}
                    </Badge>
                  </div>
                  {expandedCategories[key] ? (
                    <ChevronDown className="h-4 w-4 text-gray-400" />
                  ) : (
                    <ChevronRight className="h-4 w-4 text-gray-400" />
                  )}
                </button>
                
                {expandedCategories[key] && (
                  <motion.div
                    initial={{ opacity: 0, height: 0 }}
                    animate={{ opacity: 1, height: 'auto' }}
                    className="bg-gray-800/30 p-2 border-t border-gray-600"
                  >
                    <div className="grid grid-cols-1 gap-0.5 max-h-60 overflow-y-auto">
                      {category.components.map(component => {
                        const categoryColor = getCategoryColor(component.category);
                        return (
                          <div
                            key={component.id}
                            draggable
                            onDragStart={(e) => handleReactFlowDragStart(e, component)}
                            className="flex items-center space-x-2 p-2 rounded-lg border border-transparent hover:bg-gray-600/30 hover:border-gray-500 transition-all cursor-grab active:cursor-grabbing group"
                            title={component.description}
                          >
                            <div 
                              className="flex-shrink-0 w-8 h-8 rounded-full flex items-center justify-center text-white shadow-sm"
                              style={{ 
                                background: `linear-gradient(135deg, ${categoryColor} 0%, ${categoryColor}CC 100%)` 
                              }}
                            >
                              {component.compactIcon}
                            </div>
                            <div className="flex-1 min-w-0">
                              <div className="text-xs font-semibold text-white truncate">
                                {component.name}
                              </div>
                              <div className="text-xs text-gray-400 truncate">
                                {component.id}
                              </div>
                            </div>
                            <div className="text-xs text-gray-500">
                              {component.definition.defaultDimensions.width}x{component.definition.defaultDimensions.height}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </motion.div>
                )}
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    </div>
  );
};

// ==================== NODE PROPERTIES PANEL ====================
// Node Properties Types
interface NodeParameters {
  [key: string]: any;
  basicSettings?: Partial<BasicSettingsState>;
}

interface NodeData {
  label: string;
  description?: string;
  nodeType: string;
  enabled: boolean;
  executionOrder: number;
  timeout: number;
  parameters: NodeParameters;
}

interface CustomNode {
  id: string;
  position: { x: number; y: number };
  data: NodeData;
}

interface FormData {
  label: string;
  description: string;
  enabled: boolean;
  executionOrder: number;
  timeout: number;
  parameters: NodeParameters;
  nodeType: string;
}

interface RightPanelProps {
  currentJob?: any | null;
  onJobUpdate?: (updates: any) => void;
}

// ==================== REDUCER FOR BASIC SETTINGS ====================
type SettingsAction = 
  | { type: 'UPDATE_SCHEMA'; schemaType: 'input' | 'output'; fields: SchemaField[] }
  | { type: 'TOGGLE_SCHEMA_FIELD'; schemaType: 'input' | 'output'; fieldId: string }
  | { type: 'ADD_FILTER_CONDITION'; condition: FilterCondition }
  | { type: 'UPDATE_FILTER_CONDITION'; id: string; updates: Partial<FilterCondition> }
  | { type: 'REMOVE_FILTER_CONDITION'; id: string }
  | { type: 'UPDATE_ADVANCED_OPTION'; key: keyof AdvancedOptions; value: any }
  | { type: 'ADD_STATUS_MESSAGE'; message: StatusMessage }
  | { type: 'CLEAR_STATUS_MESSAGES' }
  | { type: 'UPDATE_COMPONENT_CONFIG'; config: Partial<ComponentConfig> }
  | { type: 'SYNC_SCHEMAS' }
  | { type: 'RESET_TO_DEFAULTS'; nodeType: PanelComponentType }
  | { type: 'LOAD_CONFIG'; config: Partial<BasicSettingsState> };

const settingsReducer = (state: BasicSettingsState, action: SettingsAction): BasicSettingsState => {
  switch (action.type) {
    case 'UPDATE_SCHEMA':
      if (action.schemaType === 'input') {
        return { ...state, inputSchema: action.fields };
      } else {
        return { ...state, outputSchema: action.fields };
      }
    
    case 'TOGGLE_SCHEMA_FIELD':
      const schema = action.schemaType === 'input' ? state.inputSchema : state.outputSchema;
      const updatedSchema = schema.map(field => 
        field.id === action.fieldId ? { ...field, included: !field.included } : field
      );
      return action.schemaType === 'input' 
        ? { ...state, inputSchema: updatedSchema }
        : { ...state, outputSchema: updatedSchema };
    
    case 'ADD_FILTER_CONDITION':
      return { 
        ...state, 
        filterConditions: [...state.filterConditions, action.condition] 
      };
    
    case 'UPDATE_FILTER_CONDITION':
      return {
        ...state,
        filterConditions: state.filterConditions.map(condition =>
          condition.id === action.id ? { ...condition, ...action.updates } : condition
        )
      };
    
    case 'REMOVE_FILTER_CONDITION':
      return {
        ...state,
        filterConditions: state.filterConditions.filter(condition => condition.id !== action.id)
      };
    
    case 'UPDATE_ADVANCED_OPTION':
      return {
        ...state,
        advancedOptions: {
          ...state.advancedOptions,
          [action.key]: action.value
        }
      };
    
    case 'ADD_STATUS_MESSAGE':
      const newMessages = [...state.status.messages, action.message];
      return {
        ...state,
        status: {
          messages: newMessages,
          hasWarnings: newMessages.some(m => m.type === 'warning'),
          hasErrors: newMessages.some(m => m.type === 'error')
        }
      };
    
    case 'CLEAR_STATUS_MESSAGES':
      return {
        ...state,
        status: {
          messages: [],
          hasWarnings: false,
          hasErrors: false
        }
      };
    
    case 'UPDATE_COMPONENT_CONFIG':
      return {
        ...state,
        component: { ...state.component, ...action.config }
      };
    
    case 'SYNC_SCHEMAS':
      const syncedOutputSchema = state.inputSchema.map(field => ({
        ...field,
        included: state.outputSchema.find(f => f.id === field.id)?.included ?? field.included
      }));
      return {
        ...state,
        outputSchema: syncedOutputSchema,
        status: {
          ...state.status,
          messages: [...state.status.messages, {
            id: `sync-${Date.now()}`,
            type: 'info',
            message: 'Schemas synchronized successfully'
          }]
        }
      };
    
    case 'RESET_TO_DEFAULTS':
      return createDefaultSettings(action.nodeType);
    
    case 'LOAD_CONFIG':
      return {
        ...state,
        ...action.config
      };
    
    default:
      return state;
  }
};

const createDefaultSettings = (componentType: PanelComponentType): BasicSettingsState => {
  const defaultSchema = generateMockSchema();
  const definition = COMPONENT_REGISTRY[componentType];
  
  return {
    component: {
      id: `comp-${Date.now()}`,
      type: componentType,
      name: definition?.displayName || componentType,
      description: definition?.description || `Configure ${componentType}`
    },
    inputSchema: defaultSchema,
    outputSchema: defaultSchema,
    filterConditions: [],
    aggregationConfig: {
      groupByColumns: [],
      aggregations: []
    },
    typeConversions: [],
    replaceRules: [],
    extractConfig: {
      outputColumns: []
    },
    rowGeneratorConfig: {
      rowCount: 100,
      seed: 12345,
      useSeed: true,
      columns: []
    },
    advancedOptions: {
      errorHandling: 'fail',
      emptyValueHandling: 'skip',
      parallelization: false,
      maxThreads: 4,
      batchSize: 1000
    },
    status: {
      messages: [{
        id: 'init',
        type: 'info',
        message: 'Component configuration initialized'
      }],
      hasWarnings: false,
      hasErrors: false
    }
  };
};

const generateMockSchema = (): SchemaField[] => [
  { id: '1', name: 'id', type: 'integer', nullable: false, included: true, description: 'Unique identifier' },
  { id: '2', name: 'name', type: 'string', length: 100, nullable: false, included: true, description: 'Customer name' },
  { id: '3', name: 'email', type: 'string', length: 255, nullable: true, included: true, description: 'Email address' },
  { id: '4', name: 'age', type: 'integer', nullable: true, included: true, description: 'Age in years' },
  { id: '5', name: 'salary', type: 'decimal', nullable: true, included: true, description: 'Annual salary' },
  { id: '6', name: 'department', type: 'string', length: 50, nullable: true, included: false, description: 'Department name' },
  { id: '7', name: 'hire_date', type: 'date', nullable: false, included: true, description: 'Date of hire' },
];

const mapNodeTypeToComponentType = (nodeType: string): PanelComponentType => {
  return nodeType;
};

const NodePropertiesPanel: React.FC<{ onClose: () => void }> = ({ onClose }) => {
  const dispatch = useAppDispatch();
  const { nodes, selectedNodeId } = useAppSelector((state) => state.nodes);
  const selectedComponentMetadata = useAppSelector((state) => state.ui.selectedComponentMetadata);
  const [activeTab, setActiveTab] = useState<'basic' | 'advanced'>('basic');
  const [formData, setFormData] = useState<FormData>({
    label: '',
    description: '',
    enabled: true,
    executionOrder: 0,
    timeout: 30,
    parameters: {},
    nodeType: ''
  });

  const [settingsState, settingsDispatch] = useReducer(
    settingsReducer,
    createDefaultSettings('tFilterRow')
  );

  const selectedNode = nodes.find(n => n.id === selectedNodeId) as CustomNode | undefined;

  const isFromCanvas = selectedComponentMetadata?.metadata?.source === 'canvas-double-click';
  const isFromRepository = selectedComponentMetadata?.metadata?.source === 'repository-double-click';
  const isFromComponentMetadata = isFromCanvas || isFromRepository;

  useEffect(() => {
    if (selectedNode && !isFromComponentMetadata) {
      const newFormData = {
        label: selectedNode.data.label || '',
        description: selectedNode.data.description || '',
        enabled: selectedNode.data.enabled !== false,
        executionOrder: selectedNode.data.executionOrder || 0,
        timeout: selectedNode.data.timeout || 30,
        parameters: selectedNode.data.parameters || {},
        nodeType: selectedNode.data.nodeType
      };
      setFormData(newFormData);

      if (selectedNode.data.parameters?.basicSettings) {
        settingsDispatch({
          type: 'LOAD_CONFIG',
          config: selectedNode.data.parameters.basicSettings
        });
      } else {
        const componentType = mapNodeTypeToComponentType(selectedNode.data.nodeType);
        settingsDispatch({
          type: 'RESET_TO_DEFAULTS',
          nodeType: componentType
        });
      }
    } else if (selectedComponentMetadata) {
      console.log('📋 Initializing from component metadata:', selectedComponentMetadata);
      
      const componentType = mapNodeTypeToComponentType(selectedComponentMetadata.type);
      
      const newFormData = {
        label: selectedComponentMetadata.name,
        description: selectedComponentMetadata.description || `Configure ${selectedComponentMetadata.name}`,
        enabled: true,
        executionOrder: 0,
        timeout: 30,
        parameters: {
          basicSettings: selectedComponentMetadata.metadata?.canvasNodeData || {}
        },
        nodeType: selectedComponentMetadata.type
      };
      setFormData(newFormData);
      
      if (selectedComponentMetadata.metadata?.basicSettings) {
        settingsDispatch({
          type: 'LOAD_CONFIG',
          config: selectedComponentMetadata.metadata.basicSettings
        });
      } else {
        settingsDispatch({
          type: 'RESET_TO_DEFAULTS',
          nodeType: componentType
        });
      }
      
      settingsDispatch({
        type: 'ADD_STATUS_MESSAGE',
        message: {
          id: `init-${Date.now()}`,
          type: 'info',
          message: `Editing ${selectedComponentMetadata.name} configuration`,
          details: `Source: ${isFromCanvas ? 'Canvas' : 'Repository'}`
        }
      });
    }
  }, [selectedNode, selectedComponentMetadata, isFromComponentMetadata, isFromCanvas]);

  const handleInputChange = (field: keyof FormData, value: any) => {
    const newFormData = { ...formData, [field]: value };
    setFormData(newFormData);
    
    if (selectedNode) {
      dispatch(updateNode({ 
        id: selectedNode.id, 
        data: newFormData 
      }));
    }
  };

  const handleSaveSettings = () => {
    if (selectedNode) {
      const updatedParameters = {
        ...formData.parameters,
        basicSettings: settingsState
      };
      
      const updatedFormData = {
        ...formData,
        parameters: updatedParameters
      };
      
      setFormData(updatedFormData);
      dispatch(updateNode({
        id: selectedNode.id,
        data: updatedFormData
      }));
      
      dispatch(addLog({
        level: 'INFO',
        message: `Saved settings for ${formData.label}`,
        source: 'node-properties'
      }));
      
      settingsDispatch({
        type: 'ADD_STATUS_MESSAGE',
        message: {
          id: `save-${Date.now()}`,
          type: 'info',
          message: 'Configuration saved successfully'
        }
      });
    } else if (selectedComponentMetadata) {
      dispatch(addLog({
        level: 'INFO',
        message: `Settings configured for ${selectedComponentMetadata.name}`,
        source: 'node-properties'
      }));
      
      settingsDispatch({
        type: 'ADD_STATUS_MESSAGE',
        message: {
          id: `save-${Date.now()}`,
          type: 'info',
          message: 'Settings configured. Drag component to canvas to apply.',
          details: 'Component is not yet on the canvas'
        }
      });
    }
  };

  const handleResetSettings = () => {
    const componentType = mapNodeTypeToComponentType(formData.nodeType || selectedComponentMetadata?.type || 'tFilterRow');
    
    settingsDispatch({
      type: 'RESET_TO_DEFAULTS',
      nodeType: componentType
    });
    
    if (selectedNode || selectedComponentMetadata) {
      dispatch(addLog({
        level: 'INFO',
        message: `Reset settings for ${formData.label || selectedComponentMetadata?.name}`,
        source: 'node-properties'
      }));
    }
  };

  const getNodeToDisplay = () => {
    if (selectedNode) return selectedNode;
    if (selectedComponentMetadata) {
      return {
        id: selectedComponentMetadata.id,
        data: {
          label: selectedComponentMetadata.name,
          description: selectedComponentMetadata.description,
          nodeType: selectedComponentMetadata.type,
          enabled: true,
          executionOrder: 0,
          timeout: 30,
          parameters: {}
        }
      } as CustomNode;
    }
    return undefined;
  };

  const nodeToDisplay = getNodeToDisplay();
  const componentType = mapNodeTypeToComponentType(
    nodeToDisplay?.data?.nodeType || 
    selectedComponentMetadata?.type || 
    'tFilterRow'
  );
  
  const definition = COMPONENT_REGISTRY[componentType];
  const categoryColor = definition ? getCategoryColor(definition.category) : '#7c3aed';

  if (!nodeToDisplay && !selectedComponentMetadata) {
    return (
      <div className="h-full flex flex-col items-center justify-center p-8 text-center">
        <Settings className="h-12 w-12 text-gray-500 mb-4" />
        <h3 className="text-lg font-medium text-white mb-2">No Node Selected</h3>
        <p className="text-sm text-gray-400">
          Select a node on the canvas to edit its properties
        </p>
      </div>
    );
  }

  return (
    <div className="h-full flex flex-col">
      <div className="p-4 border-b border-gray-700 bg-gray-800/50">
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-3">
            <div 
              className="w-10 h-10 rounded-full flex items-center justify-center text-white font-bold text-sm shadow-md"
              style={{ 
                background: `linear-gradient(135deg, ${categoryColor} 0%, ${categoryColor}CC 100%)` 
              }}
            >
              {definition?.icon ? 
                React.cloneElement(definition.icon as React.ReactElement<any>, { className: 'w-5 h-5' }) :
                React.createElement(SettingsIcon, { className: 'w-5 h-5' })
              }
            </div>
            <div>
              <h3 className="text-lg font-medium text-white">Node Properties</h3>
              <p className="text-sm text-gray-400">
                {selectedNode ? 'Editing node on canvas' : 
                 isFromCanvas ? 'Editing canvas component' : 
                 isFromRepository ? 'Editing repository component' : 
                 'Component Configuration'}
                {selectedComponentMetadata?.metadata?.source && (
                  <span className="ml-2 text-xs text-purple-400">
                    ({selectedComponentMetadata.metadata.source})
                  </span>
                )}
              </p>
            </div>
          </div>
          <div className="flex items-center gap-2">
            {!selectedNode && selectedComponentMetadata && (
              <Badge variant="outline" className="bg-purple-900/30 text-purple-300 border-purple-700">
                <Cpu className="h-3 w-3 mr-1" />
                Preview
              </Badge>
            )}
            <Button
              variant="ghost"
              size="sm"
              onClick={() => {
                dispatch(clearSelectedComponentMetadata());
                onClose();
              }}
              className="h-8 w-8 p-0 text-gray-400 hover:text-white"
            >
              <X className="h-4 w-4" />
            </Button>
          </div>
        </div>

        <div className="flex border-b border-gray-700">
          <button
            onClick={() => setActiveTab('basic')}
            className={`flex-1 py-2 text-sm font-medium ${
              activeTab === 'basic'
                ? 'text-white border-b-2 border-blue-500'
                : 'text-gray-400 hover:text-gray-300'
            }`}
          >
            Basic
          </button>
          <button
            onClick={() => setActiveTab('advanced')}
            className={`flex-1 py-2 text-sm font-medium ${
              activeTab === 'advanced'
                ? 'text-white border-b-2 border-purple-500'
                : 'text-gray-400 hover:text-gray-300'
            }`}
          >
            Advanced
          </button>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto p-4">
        {activeTab === 'basic' ? (
          <div className="space-y-6">
            <Card className="border-gray-700 bg-gray-800/30">
              <CardHeader>
                <CardTitle className="text-sm font-medium text-white">Component Information</CardTitle>
                {!selectedNode && selectedComponentMetadata && (
                  <CardDescription className="text-yellow-400 text-xs">
                    <AlertCircle className="h-3 w-3 inline mr-1" />
                    Component not on canvas. Changes won't be saved until added to canvas.
                  </CardDescription>
                )}
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="flex items-center gap-3 mb-4 p-3 bg-gray-800/50 rounded-lg">
                  <div 
                    className="w-12 h-12 rounded-full flex items-center justify-center text-white"
                    style={{ 
                      background: `linear-gradient(135deg, ${categoryColor} 0%, ${categoryColor}CC 100%)` 
                    }}
                  >
                    {definition?.icon ? 
                      React.cloneElement(definition.icon as React.ReactElement<any>, { className: 'w-6 h-6' }) :
                      React.createElement(SettingsIcon, { className: 'w-6 h-6' })
                    }
                  </div>
                  <div>
                    <h4 className="font-semibold text-white">{definition?.displayName || componentType}</h4>
                    <p className="text-xs text-gray-400">{componentType}</p>
                    <p className="text-xs text-gray-500 mt-1">{definition?.description || 'No description available'}</p>
                  </div>
                </div>

                <div className="space-y-2">
                  <Label htmlFor="node-label">Node Name</Label>
                  <Input
                    id="node-label"
                    value={formData.label}
                    onChange={(e) => handleInputChange('label', e.target.value)}
                    className="bg-gray-800 border-gray-700 text-white"
                    placeholder="Enter node name..."
                  />
                </div>

                <div className="space-y-2">
                  <Label htmlFor="node-description">Description</Label>
                  <Textarea
                    id="node-description"
                    value={formData.description}
                    onChange={(e) => handleInputChange('description', e.target.value)}
                    className="bg-gray-800 border-gray-700 text-white"
                    placeholder="Describe this node's purpose..."
                    rows={3}
                  />
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label htmlFor="execution-order">Execution Order</Label>
                    <Input
                      id="execution-order"
                      type="number"
                      value={formData.executionOrder}
                      onChange={(e) => handleInputChange('executionOrder', parseInt(e.target.value) || 0)}
                      className="bg-gray-800 border-gray-700 text-white"
                      min="0"
                      max="100"
                      disabled={!selectedNode}
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="timeout">Timeout (seconds)</Label>
                    <Input
                      id="timeout"
                      type="number"
                      value={formData.timeout}
                      onChange={(e) => handleInputChange('timeout', parseInt(e.target.value) || 30)}
                      className="bg-gray-800 border-gray-700 text-white"
                      min="1"
                      max="3600"
                      disabled={!selectedNode}
                    />
                  </div>
                </div>

                <div className="flex items-center justify-between">
                  <Label htmlFor="node-enabled" className="text-white">
                    Node Enabled
                  </Label>
                  <Switch
                    id="node-enabled"
                    checked={formData.enabled}
                    onCheckedChange={(checked) => handleInputChange('enabled', checked)}
                    disabled={!selectedNode}
                  />
                </div>
              </CardContent>
            </Card>

            <div className="flex gap-2">
              <Button 
                variant="outline" 
                className="flex-1"
                onClick={handleResetSettings}
              >
                <RotateCcw className="h-4 w-4 mr-2" />
                Reset Defaults
              </Button>
              <Button 
                variant="default" 
                className={`flex-1 ${selectedNode ? 'bg-blue-600 hover:bg-blue-700' : 'bg-purple-600 hover:bg-purple-700'}`}
                onClick={handleSaveSettings}
              >
                <Check className="h-4 w-4 mr-2" />
                {selectedNode ? 'Save Changes' : 'Configure Settings'}
              </Button>
            </div>
          </div>
        ) : (
          <div className="space-y-4">
            {selectedComponentMetadata && (
              <Card className="border-gray-700 bg-gray-800/30">
                <CardContent className="p-4">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <div className={`p-2 rounded-lg ${isFromCanvas ? 'bg-blue-900/30' : 'bg-purple-900/30'}`}>
                        {isFromCanvas ? (
                          React.createElement(Grid3X3, { className: "h-4 w-4 text-blue-400" })
                        ) : (
                          React.createElement(Search, { className: "h-4 w-4 text-purple-400" })
                        )}
                      </div>
                      <div>
                        <p className="text-sm font-medium text-white">
                          {isFromCanvas ? 'Canvas Component' : 'Repository Component'}
                        </p>
                        <p className="text-xs text-gray-400">
                          {isFromCanvas 
                            ? 'Double-clicked from canvas' 
                            : 'Double-clicked from repository'}
                        </p>
                      </div>
                    </div>
                    {selectedComponentMetadata.metadata?.timestamp && (
                      <div className="text-xs text-gray-500">
                        {new Date(selectedComponentMetadata.metadata.timestamp).toLocaleTimeString()}
                      </div>
                    )}
                  </div>
                </CardContent>
              </Card>
            )}

            <div className="flex gap-2 pt-4">
              <Button 
                variant="outline" 
                className="flex-1"
                onClick={() => setActiveTab('basic')}
              >
                <ChevronLeft className="h-4 w-4 mr-2" />
                Back to Basic
              </Button>
              <Button 
                variant="default" 
                className={`flex-1 ${selectedNode ? 'bg-green-600 hover:bg-green-700' : 'bg-purple-600 hover:bg-purple-700'}`}
                onClick={handleSaveSettings}
              >
                <Check className="h-4 w-4 mr-2" />
                {selectedNode ? 'Save Advanced Settings' : 'Configure Advanced Settings'}
              </Button>
            </div>

            {settingsState.status.messages.length > 0 && (
              <Card className="border-gray-700 bg-gray-800/30 mt-4">
                <CardHeader className="pb-2">
                  <div className="flex items-center justify-between">
                    <CardTitle className="text-sm font-medium text-white">Status Messages</CardTitle>
                    <Button
                      size="sm"
                      variant="ghost"
                      onClick={() => settingsDispatch({ type: 'CLEAR_STATUS_MESSAGES' })}
                      className="h-6 text-xs"
                    >
                      Clear
                    </Button>
                  </div>
                </CardHeader>
                <CardContent>
                  <div className="space-y-2">
                    {settingsState.status.messages.slice(-3).map(msg => (
                      <div
                        key={msg.id}
                        className={`p-3 rounded text-sm ${
                          msg.type === 'error'
                            ? 'bg-red-900/30 text-red-300 border border-red-800'
                            : msg.type === 'warning'
                            ? 'bg-yellow-900/30 text-yellow-300 border border-yellow-800'
                            : 'bg-blue-900/30 text-blue-300 border border-blue-800'
                        }`}
                      >
                        <div className="font-medium">{msg.message}</div>
                        {msg.details && (
                          <div className="text-xs mt-1 opacity-75">{msg.details}</div>
                        )}
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>
            )}
          </div>
        )}
      </div>
    </div>
  );
};

// ==================== JOB PROPERTIES PANEL ====================
const JobPropertiesPanel: React.FC<{ job: any; onClose: () => void }> = ({ job, onClose }) => {
  const [jobName, setJobName] = useState(job?.name || 'Untitled Job');
  const [jobDescription, setJobDescription] = useState(job?.description || '');
  
  const handleSaveJob = () => {
    console.log('Job saved:', { jobName, jobDescription });
  };
  
  return (
    <div className="h-full overflow-y-auto p-4 space-y-6">
      <Card className="bg-gray-800/50 border-gray-700 backdrop-blur-sm">
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-2">
              <div className="p-2 rounded-lg bg-gradient-to-br from-purple-500 to-pink-600 shadow-md">
                <SettingsIcon className="h-4 w-4 text-white" />
              </div>
              <div>
                <CardTitle className="text-lg text-white">Job Properties</CardTitle>
                <CardDescription className="text-gray-400">
                  Configure current job
                </CardDescription>
              </div>
            </div>
            <Button
              variant="ghost"
              size="sm"
              onClick={onClose}
              className="h-8 w-8 p-0 text-gray-400 hover:text-white hover:bg-gray-700/50"
              title="Close Job Properties"
            >
              <X className="h-4 w-4" />
            </Button>
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="job-name">Job Name</Label>
            <Input
              id="job-name"
              value={jobName}
              onChange={(e) => setJobName(e.target.value)}
              placeholder="Enter job name..."
              className="bg-gray-700 border-gray-600 text-white"
            />
          </div>
          
          <div className="space-y-2">
            <Label htmlFor="job-description">Description</Label>
            <Textarea
              id="job-description"
              value={jobDescription}
              onChange={(e) => setJobDescription(e.target.value)}
              placeholder="Describe this job..."
              rows={3}
              className="bg-gray-700 border-gray-600 text-white"
            />
          </div>
          
          <div className="pt-4">
            <Button 
              onClick={handleSaveJob}
              className="w-full bg-gradient-to-r from-purple-600 to-pink-600 hover:from-purple-700 hover:to-pink-700 text-white"
            >
              Save Job Properties
            </Button>
          </div>
        </CardContent>
      </Card>
      
      {job && (
        <Card className="bg-gray-800/50 border-gray-700 backdrop-blur-sm">
          <CardHeader>
            <CardTitle className="text-lg text-white">Job Info</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="flex justify-between">
              <span className="text-gray-400">Created:</span>
              <span className="text-white">
                {job.createdAt ? new Date(job.createdAt).toLocaleDateString() : 'N/A'}
              </span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-400">Last Modified:</span>
              <span className="text-white">
                {job.lastModified ? new Date(job.lastModified).toLocaleDateString() : 'N/A'}
              </span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-400">Status:</span>
              <Badge 
                variant="secondary" 
                className={
                  job.state === 'draft' ? 'bg-yellow-500/20 text-yellow-300 border-yellow-500/30' :
                  job.state === 'running' ? 'bg-blue-500/20 text-blue-300 border-blue-500/30' :
                  job.state === 'completed' ? 'bg-green-500/20 text-green-300 border-green-500/30' :
                  'bg-gray-500/20 text-gray-300 border-gray-500/30'
                }
              >
                {job.state || 'draft'}
              </Badge>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-400">Nodes:</span>
              <span className="text-white">{job.nodes?.length || 0}</span>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
};

// ==================== MAIN RIGHT PANEL ====================
const RightPanel: React.FC<RightPanelProps> = ({ currentJob = null }) => {
  const [isExpanded, setIsExpanded] = useState(false);
  const selectedNodeId = useAppSelector((state) => state.nodes.selectedNodeId);
  
  const rightPanelView = useAppSelector((state) => state.ui.rightPanelView);
  const selectedComponentMetadata = useAppSelector((state) => state.ui.selectedComponentMetadata);
  
  const dispatch = useAppDispatch();
  
  const [expandedCategories, setExpandedCategories] = useState<{ [key: string]: boolean }>({
    mappingTransformation: true,
    rowProcessing: false,
    dataCombination: false,
    dataQuality: false,
    advancedProcessing: false
  });

  const [activeView, setActiveView] = useState<'components' | 'node-properties' | 'job-properties'>(
    rightPanelView || 'components'
  );

  useEffect(() => {
    if (selectedComponentMetadata) {
      console.log('🔄 Component metadata detected, switching to node-properties');
      const newActiveView = 'node-properties';
      setActiveView(newActiveView);
      dispatch(setRightPanelView(newActiveView));
      dispatch(setNodePropertiesTab('advanced'));
    }
  }, [selectedComponentMetadata, dispatch]);

  const handleViewChange = (view: 'components' | 'node-properties' | 'job-properties') => {
    setActiveView(view);
    dispatch(setRightPanelView(view));
  };

  const toggleCategory = (categoryKey: string) => {
    setExpandedCategories(prev => ({
      ...prev,
      [categoryKey]: !prev[categoryKey]
    }));
  };

  const toggleExpand = () => {
    setIsExpanded(!isExpanded);
  };

  useEffect(() => {
    const handleCanvasNodeDoubleClick = (event: CustomEvent) => {
      const { componentMetadata } = event.detail;
      if (componentMetadata) {
        console.log('🎯 Canvas node double-click event received:', componentMetadata);
        
        const isMapComponent = componentMetadata.name && componentMetadata.name.includes('Map') || 
                              componentMetadata.type === 'tMap';
        
        if (!isMapComponent) {
          const newActiveView = 'node-properties';
          setActiveView(newActiveView);
          dispatch(setRightPanelView(newActiveView));
          dispatch(setNodePropertiesTab('advanced'));
        }
      }
    };

    window.addEventListener('canvas-node-double-click', handleCanvasNodeDoubleClick as EventListener);
    
    return () => {
      window.removeEventListener('canvas-node-double-click', handleCanvasNodeDoubleClick as EventListener);
    };
  }, [dispatch]);

  const panelWidth = isExpanded ? 'w-96' : 'w-80';

  return (
    <motion.div 
      initial={{ opacity: 0, x: 50 }}
      animate={{ opacity: 1, x: 0 }}
      className={`${panelWidth} bg-gradient-to-b from-gray-900 to-gray-800 border-l border-gray-700 shadow-2xl flex flex-col transition-all duration-300`}
    >
      <div className="flex items-center justify-between p-3 border-b border-gray-700 bg-gray-800/80">
        <div className="flex items-center space-x-2">
          <h3 className="text-sm font-medium text-white">Right Panel</h3>
          {selectedComponentMetadata && (
            <Badge variant="secondary" className="bg-blue-600/20 text-blue-300 border-blue-600/30 text-xs">
              <Cpu className="h-3 w-3 mr-1" />
              Configuring
            </Badge>
          )}
        </div>
        <div className="flex items-center space-x-1">
          <Button
            variant="ghost"
            size="sm"
            onClick={toggleExpand}
            className="h-7 w-7 p-0 text-gray-400 hover:text-white hover:bg-gray-700/50"
            title={isExpanded ? "Collapse Panel" : "Expand Panel"}
          >
            {isExpanded ? <Minimize2 className="h-3.5 w-3.5" /> : <Maximize2 className="h-3.5 w-3.5" />}
          </Button>
        </div>
      </div>

      <div className="flex border-b border-gray-700 bg-gray-800/60">
        <button
          onClick={() => handleViewChange('components')}
          className={`flex-1 py-2 text-xs font-medium transition-colors ${
            activeView === 'components'
              ? 'text-white border-b-2 border-blue-500 bg-gray-800'
              : 'text-gray-400 hover:text-gray-300 hover:bg-gray-800/50'
          }`}
        >
          <div className="flex items-center justify-center space-x-1">
            <Grid3X3 className="h-3.5 w-3.5" />
            <span>Components</span>
          </div>
        </button>
        
        <button
          onClick={() => handleViewChange('node-properties')}
          className={`flex-1 py-2 text-xs font-medium transition-colors ${
            activeView === 'node-properties'
              ? 'text-white border-b-2 border-green-500 bg-gray-800'
              : 'text-gray-400 hover:text-gray-300 hover:bg-gray-800/50'
          }`}
          disabled={!selectedNodeId && !selectedComponentMetadata}
        >
          <div className="flex items-center justify-center space-x-1">
            <SettingsIcon className="h-3.5 w-3.5" />
            <span>Node</span>
            {(selectedNodeId || selectedComponentMetadata) && (
              <div className="w-1.5 h-1.5 rounded-full bg-green-500"></div>
            )}
          </div>
        </button>
        
        <button
          onClick={() => handleViewChange('job-properties')}
          className={`flex-1 py-2 text-xs font-medium transition-colors ${
            activeView === 'job-properties'
              ? 'text-white border-b-2 border-purple-500 bg-gray-800'
              : 'text-gray-400 hover:text-gray-300 hover:bg-gray-800/50'
          }`}
          disabled={!currentJob}
        >
          <div className="flex items-center justify-center space-x-1">
            <Database className="h-3.5 w-3.5" />
            <span>Job</span>
            {currentJob && (
              <div className="w-1.5 h-1.5 rounded-full bg-purple-500"></div>
            )}
          </div>
        </button>
      </div>

      <div className="flex-1 overflow-hidden">
        {activeView === 'components' && (
          <ComponentPalette 
            expandedCategories={expandedCategories}
            toggleCategory={toggleCategory}
          />
        )}
        
        {activeView === 'node-properties' && (selectedNodeId || selectedComponentMetadata) ? (
          <NodePropertiesPanel onClose={() => handleViewChange('components')} />
        ) : activeView === 'node-properties' && !selectedNodeId && !selectedComponentMetadata ? (
          <div className="h-full flex flex-col items-center justify-center p-6 text-center">
            <SettingsIcon className="h-12 w-12 text-gray-600 mb-4" />
            <h3 className="text-lg font-medium text-white mb-2">No Node Selected</h3>
            <p className="text-sm text-gray-400">
              Select a node on the canvas or double-click a component to edit its properties
            </p>
            <p className="text-xs text-purple-400 mt-2">
              Note: tMap components open MapEditor via double-click instead
            </p>
          </div>
        ) : null}
        
        {activeView === 'job-properties' && currentJob ? (
          <JobPropertiesPanel job={currentJob} onClose={() => handleViewChange('components')} />
        ) : activeView === 'job-properties' && !currentJob ? (
          <div className="h-full flex flex-col items-center justify-center p-6 text-center">
            <Database className="h-12 w-12 text-gray-600 mb-4" />
            <h3 className="text-lg font-medium text-white mb-2">No Job Active</h3>
            <p className="text-sm text-gray-400">
              Create or open a job to view job properties
            </p>
          </div>
        ) : null}
      </div>
    </motion.div>
  );
};

export default RightPanel;