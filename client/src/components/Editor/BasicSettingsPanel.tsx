// BasicSettingsPanel.tsx
import React, { useState, useReducer, useEffect } from 'react';
import styled from 'styled-components';

// ==================== TYPES & INTERFACES ====================
export type ComponentType = 
  | 'tFilterRow' 
  | 'tJoin' 
  | 'tAggregateRow' 
  | 'tConvertType' 
  | 'tReplace' 
  | 'tReplaceList' 
  | 'tExtractJSON' 
  | 'tExtractDelimited' 
  | 'tExtractRegex' 
  | 'tExtractXML' 
  | 'tRowGenerator'
  | 'tSchemaEditor';

export interface ComponentConfig {
  id: string;
  type: ComponentType;
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
  advancedOptions: {
    errorHandling: 'fail' | 'skip' | 'default';
    emptyValueHandling: 'skip' | 'default' | 'null';
    parallelization: boolean;
    maxThreads: number;
    batchSize: number;
  };
  status: {
    messages: StatusMessage[];
    hasWarnings: boolean;
    hasErrors: boolean;
  };
}

export interface StatusMessage {
  id: string;
  type: 'info' | 'warning' | 'error';
  message: string;
  details?: string;
}

export interface BasicSettingsPanelProps {
  componentId: string;
  componentType: ComponentType;
  initialConfig?: Partial<BasicSettingsState>;
  onSave: (config: BasicSettingsState) => void;
  onCancel: () => void;
  onApply?: (config: BasicSettingsState) => void;
}

// ==================== STYLED COMPONENTS ====================
const PanelContainer = styled.div`
  position: fixed;
  bottom: 0;
  left: 0;
  right: 0;
  height: 500px;
  background: white;
  border-top: 2px solid #007acc;
  box-shadow: 0 -2px 10px rgba(0, 0, 0, 0.1);
  display: flex;
  flex-direction: column;
  z-index: 1000;
  font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
`;

const HeaderZone = styled.div`
  background: linear-gradient(135deg, #f5f7fa 0%, #e4edf5 100%);
  padding: 12px 20px;
  border-bottom: 1px solid #d1d8e0;
  display: flex;
  align-items: center;
  justify-content: space-between;
`;

const ComponentInfo = styled.div`
  display: flex;
  align-items: center;
  gap: 12px;
`;

const ComponentIcon = styled.div<{ bgColor: string }>`
  width: 36px;
  height: 36px;
  border-radius: 6px;
  background: ${props => props.bgColor};
  color: white;
  display: flex;
  align-items: center;
  justify-content: center;
  font-weight: bold;
  font-size: 14px;
`;

const ComponentText = styled.div`
  display: flex;
  flex-direction: column;
`;

const ComponentName = styled.h3`
  margin: 0;
  font-size: 16px;
  color: #2c3e50;
  font-weight: 600;
`;

const ComponentDescription = styled.p`
  margin: 4px 0 0;
  font-size: 13px;
  color: #5d6d7e;
  max-width: 600px;
`;

const TooltipIcon = styled.span`
  display: inline-block;
  width: 16px;
  height: 16px;
  background: #3498db;
  color: white;
  border-radius: 50%;
  text-align: center;
  line-height: 16px;
  font-size: 12px;
  margin-left: 6px;
  cursor: help;
`;

const ContentContainer = styled.div`
  flex: 1;
  overflow: hidden;
  display: flex;
`;

const ConfigZone = styled.div`
  flex: 1;
  padding: 20px;
  overflow-y: auto;
  background: #f9fafb;
`;

const Section = styled.div`
  background: white;
  border: 1px solid #e0e6ed;
  border-radius: 6px;
  padding: 16px;
  margin-bottom: 16px;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.05);
`;

const SectionHeader = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
  padding-bottom: 8px;
  border-bottom: 1px solid #eaeff4;
`;

const SectionTitle = styled.h4`
  margin: 0;
  font-size: 14px;
  color: #34495e;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.5px;
`;

const CollapseButton = styled.button`
  background: none;
  border: none;
  color: #7f8c8d;
  cursor: pointer;
  font-size: 12px;
  padding: 4px 8px;

  &:hover {
    color: #3498db;
  }
`;

const FormRow = styled.div`
  display: flex;
  gap: 16px;
  margin-bottom: 12px;
  align-items: center;
`;

const FormLabel = styled.label`
  min-width: 180px;
  font-size: 13px;
  color: #2c3e50;
  font-weight: 500;
`;

const Input = styled.input`
  flex: 1;
  padding: 8px 12px;
  border: 1px solid #d1d8e0;
  border-radius: 4px;
  font-size: 13px;
  transition: border 0.2s;

  &:focus {
    outline: none;
    border-color: #3498db;
    box-shadow: 0 0 0 2px rgba(52, 152, 219, 0.2);
  }
`;

const Select = styled.select`
  flex: 1;
  padding: 8px 12px;
  border: 1px solid #d1d8e0;
  border-radius: 4px;
  font-size: 13px;
  background: white;
  cursor: pointer;

  &:focus {
    outline: none;
    border-color: #3498db;
    box-shadow: 0 0 0 2px rgba(52, 152, 219, 0.2);
  }
`;

const Checkbox = styled.input.attrs({ type: 'checkbox' })`
  margin-right: 8px;
  cursor: pointer;
`;

const Table = styled.table`
  width: 100%;
  border-collapse: collapse;
  font-size: 12px;
`;

const TableHead = styled.thead`
  background: #f8f9fa;
`;

const TableRow = styled.tr`
  border-bottom: 1px solid #eaeff4;

  &:hover {
    background: #f8f9fa;
  }
`;

const TableHeader = styled.th`
  padding: 10px 12px;
  text-align: left;
  font-weight: 600;
  color: #2c3e50;
  border-bottom: 2px solid #dee2e6;
`;

const TableCell = styled.td`
  padding: 10px 12px;
  vertical-align: middle;
`;

const ActionButtonsZone = styled.div`
  padding: 12px 20px;
  border-top: 1px solid #e0e6ed;
  border-bottom: 1px solid #e0e6ed;
  background: #f8f9fa;
  display: flex;
  gap: 10px;
  flex-wrap: wrap;
`;

const Button = styled.button<{ variant?: 'primary' | 'secondary' | 'danger' }>`
  padding: 8px 16px;
  border: 1px solid ${props => {
    if (props.variant === 'primary') return '#007acc';
    if (props.variant === 'danger') return '#e74c3c';
    return '#d1d8e0';
  }};
  border-radius: 4px;
  background: ${props => {
    if (props.variant === 'primary') return '#007acc';
    if (props.variant === 'danger') return '#e74c3c';
    return 'white';
  }};
  color: ${props => props.variant ? 'white' : '#2c3e50'};
  font-size: 13px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s;
  display: flex;
  align-items: center;
  gap: 6px;

  &:hover {
    background: ${props => {
      if (props.variant === 'primary') return '#006bb3';
      if (props.variant === 'danger') return '#c0392b';
      return '#f8f9fa';
    }};
    transform: translateY(-1px);
  }

  &:active {
    transform: translateY(0);
  }

  &:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }
`;

const FooterZone = styled.div`
  padding: 12px 20px;
  background: white;
  border-top: 1px solid #e0e6ed;
  display: flex;
  justify-content: space-between;
  align-items: center;
`;

const StatusStrip = styled.div`
  display: flex;
  align-items: center;
  gap: 12px;
  flex: 1;
`;

const StatusIcon = styled.div<{ type: 'info' | 'warning' | 'error' }>`
  width: 20px;
  height: 20px;
  border-radius: 50%;
  background: ${props => {
    switch (props.type) {
      case 'warning': return '#f39c12';
      case 'error': return '#e74c3c';
      default: return '#3498db';
    }
  }};
  color: white;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 12px;
  font-weight: bold;
`;

const StatusMessage = styled.div`
  font-size: 13px;
  color: #2c3e50;
`;

const Icon = styled.span`
  font-size: 14px;
`;

// ==================== HELPER FUNCTIONS ====================
const getComponentIcon = (type: ComponentType): { bgColor: string; label: string } => {
  const icons: Record<ComponentType, { bgColor: string; label: string }> = {
    tFilterRow: { bgColor: '#e74c3c', label: 'FR' },
    tJoin: { bgColor: '#3498db', label: 'JN' },
    tAggregateRow: { bgColor: '#2ecc71', label: 'AG' },
    tConvertType: { bgColor: '#9b59b6', label: 'CT' },
    tReplace: { bgColor: '#f39c12', label: 'RP' },
    tReplaceList: { bgColor: '#d35400', label: 'RL' },
    tExtractJSON: { bgColor: '#1abc9c', label: 'EJ' },
    tExtractDelimited: { bgColor: '#27ae60', label: 'ED' },
    tExtractRegex: { bgColor: '#16a085', label: 'ER' },
    tExtractXML: { bgColor: '#8e44ad', label: 'EX' },
    tRowGenerator: { bgColor: '#c0392b', label: 'RG' },
    tSchemaEditor: { bgColor: '#7f8c8d', label: 'SE' }
  };
  return icons[type] || { bgColor: '#95a5a6', label: '??' };
};

const getComponentDescription = (type: ComponentType): string => {
  const descriptions: Record<ComponentType, string> = {
    tFilterRow: 'Filters rows based on specified conditions. Supports multiple conditions with AND/OR logic.',
    tJoin: 'Joins data from two input flows based on key columns. Supports inner, left, right, and full outer joins.',
    tAggregateRow: 'Aggregates data using group by columns and aggregate functions (sum, avg, min, max, count).',
    tConvertType: 'Converts data types between different formats with optional formatting rules.',
    tReplace: 'Replaces text patterns in specified columns. Supports simple text replacement and regex patterns.',
    tReplaceList: 'Performs multiple replacements using a lookup table. Useful for data cleansing and standardization.',
    tExtractJSON: 'Extracts data from JSON structures using JSONPath expressions. Maps extracted values to output columns.',
    tExtractDelimited: 'Parses delimited text files (CSV, TSV). Configurable delimiter, qualifier, and encoding settings.',
    tExtractRegex: 'Extracts data using regular expressions. Supports pattern matching and capture groups.',
    tExtractXML: 'Extracts data from XML documents using XPath expressions. Handles elements, attributes, and namespaces.',
    tRowGenerator: 'Generates synthetic data rows using various generation functions. Configurable seed for reproducibility.',
    tSchemaEditor: 'Edits and manages input/output schemas. Provides column mapping and type validation.'
  };
  return descriptions[type];
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

// ==================== REDUCER FOR STATE MANAGEMENT ====================
type SettingsAction = 
  | { type: 'UPDATE_SCHEMA'; schemaType: 'input' | 'output'; fields: SchemaField[] }
  | { type: 'TOGGLE_SCHEMA_FIELD'; schemaType: 'input' | 'output'; fieldId: string }
  | { type: 'ADD_FILTER_CONDITION'; condition: FilterCondition }
  | { type: 'UPDATE_FILTER_CONDITION'; id: string; updates: Partial<FilterCondition> }
  | { type: 'REMOVE_FILTER_CONDITION'; id: string }
  | { type: 'UPDATE_ADVANCED_OPTION'; key: keyof BasicSettingsState['advancedOptions']; value: any }
  | { type: 'ADD_STATUS_MESSAGE'; message: StatusMessage }
  | { type: 'CLEAR_STATUS_MESSAGES' }
  | { type: 'UPDATE_COMPONENT_CONFIG'; config: Partial<ComponentConfig> }
  | { type: 'SYNC_SCHEMAS' };

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
      // Sync output schema to match input schema structure
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
    
    default:
      return state;
  }
};

// ==================== MODULAR COMPONENTS ====================

// 1. Header/Component Information Zone
const HeaderZoneComponent: React.FC<{
  component: ComponentConfig;
  onUpdate: (config: Partial<ComponentConfig>) => void;
}> = ({ component, onUpdate }) => {
  const iconInfo = getComponentIcon(component.type);
  const description = getComponentDescription(component.type);
  
  return (
    <HeaderZone>
      <ComponentInfo>
        <ComponentIcon bgColor={iconInfo.bgColor}>
          {iconInfo.label}
        </ComponentIcon>
        <ComponentText>
          <ComponentName>
            {component.name}
            <TooltipIcon title={description}>?</TooltipIcon>
          </ComponentName>
          <ComponentDescription>{description}</ComponentDescription>
        </ComponentText>
      </ComponentInfo>
      <div>
        <Input
          type="text"
          value={component.name}
          onChange={(e) => onUpdate({ name: e.target.value })}
          placeholder="Component name..."
          style={{ width: '200px' }}
        />
      </div>
    </HeaderZone>
  );
};

// 2. Schema Controls Sub-component
const SchemaControls: React.FC<{
  inputSchema: SchemaField[];
  outputSchema: SchemaField[];
  onToggleField: (schemaType: 'input' | 'output', fieldId: string) => void;
  onOpenSchemaEditor: () => void;
  onSyncSchemas: () => void;
}> = ({ inputSchema, outputSchema, onToggleField, onOpenSchemaEditor, onSyncSchemas }) => {
  const [activeSchema, setActiveSchema] = useState<'input' | 'output'>('input');
  const schema = activeSchema === 'input' ? inputSchema : outputSchema;
  
  return (
    <Section>
      <SectionHeader>
        <SectionTitle>Schema Configuration</SectionTitle>
        <div style={{ display: 'flex', gap: '8px' }}>
          <Button 
            variant={activeSchema === 'input' ? 'primary' : 'secondary'}
            onClick={() => setActiveSchema('input')}
          >
            Input Schema ({inputSchema.filter(f => f.included).length}/{inputSchema.length})
          </Button>
          <Button 
            variant={activeSchema === 'output' ? 'primary' : 'secondary'}
            onClick={() => setActiveSchema('output')}
          >
            Output Schema ({outputSchema.filter(f => f.included).length}/{outputSchema.length})
          </Button>
        </div>
      </SectionHeader>
      
      <ActionButtonsZone style={{ margin: '-8px 0 16px 0', border: 'none', background: 'none', padding: '0' }}>
        <Button variant="primary" onClick={onOpenSchemaEditor}>
          <Icon>✏️</Icon> Edit Schema
        </Button>
        <Button onClick={onSyncSchemas}>
          <Icon>🔄</Icon> Sync Columns
        </Button>
        <Button>
          <Icon>📋</Icon> Copy Schema
        </Button>
        <Button>
          <Icon>📤</Icon> Export Schema
        </Button>
      </ActionButtonsZone>
      
      <Table>
        <TableHead>
          <TableRow>
            <TableHeader style={{ width: '50px' }}>Include</TableHeader>
            <TableHeader style={{ width: '200px' }}>Column Name</TableHeader>
            <TableHeader style={{ width: '120px' }}>Data Type</TableHeader>
            <TableHeader style={{ width: '80px' }}>Length</TableHeader>
            <TableHeader style={{ width: '80px' }}>Nullable</TableHeader>
            <TableHeader>Description</TableHeader>
          </TableRow>
        </TableHead>
        <tbody>
          {schema.map(field => (
            <TableRow key={field.id}>
              <TableCell>
                <Checkbox
                  checked={field.included}
                  onChange={() => onToggleField(activeSchema, field.id)}
                />
              </TableCell>
              <TableCell>
                <strong>{field.name}</strong>
              </TableCell>
              <TableCell>
                <span style={{
                  background: '#e8f4fd',
                  padding: '2px 8px',
                  borderRadius: '4px',
                  fontSize: '11px',
                  color: '#007acc'
                }}>
                  {field.type}
                </span>
              </TableCell>
              <TableCell>{field.length || '-'}</TableCell>
              <TableCell>
                <span style={{
                  color: field.nullable ? '#27ae60' : '#e74c3c',
                  fontWeight: 'bold'
                }}>
                  {field.nullable ? 'Yes' : 'No'}
                </span>
              </TableCell>
              <TableCell>{field.description}</TableCell>
            </TableRow>
          ))}
        </tbody>
      </Table>
      
      <div style={{ marginTop: '12px', fontSize: '12px', color: '#7f8c8d' }}>
        <strong>Status:</strong> {schema.filter(f => f.included).length} of {schema.length} columns included
      </div>
    </Section>
  );
};

// 3. Parameter Table Sub-component (for various component types)
const ParameterTable: React.FC<{
  componentType: ComponentType;
  config: any;
  onUpdate: (config: any) => void;
}> = ({ componentType, config, onUpdate }) => {
  
  // Filter Conditions Table
  if (componentType === 'tFilterRow') {
    const conditions: FilterCondition[] = config || [];
    
    return (
      <Section>
        <SectionHeader>
          <SectionTitle>Filter Conditions</SectionTitle>
          <Button 
            onClick={() => onUpdate([
              ...conditions,
              {
                id: `cond-${Date.now()}`,
                column: '',
                operator: 'equals',
                value: '',
                logicalConnector: 'AND'
              }
            ])}
          >
            <Icon>+</Icon> Add Condition
          </Button>
        </SectionHeader>
        
        <Table>
          <TableHead>
            <TableRow>
              <TableHeader style={{ width: '30px' }}>#</TableHeader>
              <TableHeader style={{ width: '180px' }}>Column</TableHeader>
              <TableHeader style={{ width: '150px' }}>Operator</TableHeader>
              <TableHeader style={{ width: '200px' }}>Value</TableHeader>
              <TableHeader style={{ width: '100px' }}>Value 2</TableHeader>
              <TableHeader style={{ width: '100px' }}>Connector</TableHeader>
              <TableHeader style={{ width: '80px' }}>Actions</TableHeader>
            </TableRow>
          </TableHead>
          <tbody>
            {conditions.map((cond, index) => (
              <TableRow key={cond.id}>
                <TableCell>{index + 1}</TableCell>
                <TableCell>
                  <Select 
                    value={cond.column}
                    onChange={(e) => onUpdate(conditions.map(c => 
                      c.id === cond.id ? { ...c, column: e.target.value } : c
                    ))}
                  >
                    <option value="">Select column...</option>
                    <option value="name">name</option>
                    <option value="age">age</option>
                    <option value="salary">salary</option>
                    <option value="department">department</option>
                  </Select>
                </TableCell>
                <TableCell>
                  <Select 
                    value={cond.operator}
                    onChange={(e) => onUpdate(conditions.map(c => 
                      c.id === cond.id ? { ...c, operator: e.target.value as any } : c
                    ))}
                  >
                    <option value="equals">Equals</option>
                    <option value="notEquals">Not Equals</option>
                    <option value="contains">Contains</option>
                    <option value="startsWith">Starts With</option>
                    <option value="endsWith">Ends With</option>
                    <option value="greaterThan">Greater Than</option>
                    <option value="lessThan">Less Than</option>
                    <option value="between">Between</option>
                  </Select>
                </TableCell>
                <TableCell>
                  <Input 
                    type="text"
                    value={cond.value}
                    onChange={(e) => onUpdate(conditions.map(c => 
                      c.id === cond.id ? { ...c, value: e.target.value } : c
                    ))}
                    placeholder="Enter value..."
                  />
                </TableCell>
                <TableCell>
                  {cond.operator === 'between' && (
                    <Input 
                      type="text"
                      value={cond.value2 || ''}
                      onChange={(e) => onUpdate(conditions.map(c => 
                        c.id === cond.id ? { ...c, value2: e.target.value } : c
                      ))}
                      placeholder="Second value..."
                    />
                  )}
                </TableCell>
                <TableCell>
                  <Select 
                    value={cond.logicalConnector}
                    onChange={(e) => onUpdate(conditions.map(c => 
                      c.id === cond.id ? { ...c, logicalConnector: e.target.value as any } : c
                    ))}
                    disabled={index === 0}
                  >
                    <option value="AND">AND</option>
                    <option value="OR">OR</option>
                  </Select>
                </TableCell>
                <TableCell>
                  <div style={{ display: 'flex', gap: '4px' }}>
                    <Button onClick={() => onUpdate(conditions.filter(c => c.id !== cond.id))}>
                      <Icon>🗑️</Icon>
                    </Button>
                    <Button onClick={() => {
                      const newCond = { ...cond, id: `dup-${Date.now()}` };
                      const newIndex = conditions.findIndex(c => c.id === cond.id);
                      const newConditions = [...conditions];
                      newConditions.splice(newIndex + 1, 0, newCond);
                      onUpdate(newConditions);
                    }}>
                      <Icon>📋</Icon>
                    </Button>
                  </div>
                </TableCell>
              </TableRow>
            ))}
          </tbody>
        </Table>
        
        {conditions.length === 0 && (
          <div style={{ textAlign: 'center', padding: '20px', color: '#7f8c8d' }}>
            No filter conditions defined. Add at least one condition to filter rows.
          </div>
        )}
      </Section>
    );
  }
  
  // Type Conversion Table
  if (componentType === 'tConvertType') {
    const conversions: TypeConversion[] = config || [];
    
    return (
      <Section>
        <SectionHeader>
          <SectionTitle>Type Conversions</SectionTitle>
          <Button onClick={() => onUpdate([
            ...conversions,
            {
              id: `conv-${Date.now()}`,
              sourceType: 'string',
              targetType: 'integer',
              format: ''
            }
          ])}>
            <Icon>+</Icon> Add Conversion
          </Button>
        </SectionHeader>
        
        <Table>
          <TableHead>
            <TableRow>
              <TableHeader>Source Type</TableHeader>
              <TableHeader>Target Type</TableHeader>
              <TableHeader>Format</TableHeader>
              <TableHeader>Precision</TableHeader>
              <TableHeader>Scale</TableHeader>
              <TableHeader>Actions</TableHeader>
            </TableRow>
          </TableHead>
          <tbody>
            {conversions.map(conv => (
              <TableRow key={conv.id}>
                <TableCell>
                  <Select value={conv.sourceType}>
                    <option value="string">String</option>
                    <option value="integer">Integer</option>
                    <option value="decimal">Decimal</option>
                    <option value="date">Date</option>
                    <option value="boolean">Boolean</option>
                  </Select>
                </TableCell>
                <TableCell>
                  <Select value={conv.targetType}>
                    <option value="string">String</option>
                    <option value="integer">Integer</option>
                    <option value="decimal">Decimal</option>
                    <option value="date">Date</option>
                    <option value="boolean">Boolean</option>
                  </Select>
                </TableCell>
                <TableCell>
                  <Input 
                    type="text"
                    value={conv.format || ''}
                    placeholder="e.g., yyyy-MM-dd"
                  />
                </TableCell>
                <TableCell>
                  <Input 
                    type="number"
                    value={conv.precision || ''}
                    placeholder="Precision"
                    style={{ width: '80px' }}
                  />
                </TableCell>
                <TableCell>
                  <Input 
                    type="number"
                    value={conv.scale || ''}
                    placeholder="Scale"
                    style={{ width: '80px' }}
                  />
                </TableCell>
                <TableCell>
                  <Button onClick={() => onUpdate(conversions.filter(c => c.id !== conv.id))}>
                    <Icon>🗑️</Icon>
                  </Button>
                </TableCell>
              </TableRow>
            ))}
          </tbody>
        </Table>
      </Section>
    );
  }
  
  // Replace Rules Table
  if (componentType === 'tReplace' || componentType === 'tReplaceList') {
    const rules: ReplaceRule[] = config || [];
    
    return (
      <Section>
        <SectionHeader>
          <SectionTitle>Replace Rules</SectionTitle>
          <Button onClick={() => onUpdate([
            ...rules,
            {
              id: `rule-${Date.now()}`,
              searchValue: '',
              replacement: '',
              caseSensitive: false,
              regex: false,
              scope: 'all'
            }
          ])}>
            <Icon>+</Icon> Add Rule
          </Button>
        </SectionHeader>
        
        <Table>
          <TableHead>
            <TableRow>
              <TableHeader>Search Value</TableHeader>
              <TableHeader>Replacement</TableHeader>
              <TableHeader style={{ width: '100px' }}>Case Sensitive</TableHeader>
              <TableHeader style={{ width: '80px' }}>Regex</TableHeader>
              <TableHeader style={{ width: '100px' }}>Scope</TableHeader>
              <TableHeader>Actions</TableHeader>
            </TableRow>
          </TableHead>
          <tbody>
            {rules.map(rule => (
              <TableRow key={rule.id}>
                <TableCell>
                  <Input 
                    type="text"
                    value={rule.searchValue}
                    placeholder="Text to find..."
                  />
                </TableCell>
                <TableCell>
                  <Input 
                    type="text"
                    value={rule.replacement}
                    placeholder="Replacement text..."
                  />
                </TableCell>
                <TableCell>
                  <Checkbox checked={rule.caseSensitive} />
                </TableCell>
                <TableCell>
                  <Checkbox checked={rule.regex} />
                </TableCell>
                <TableCell>
                  <Select value={rule.scope}>
                    <option value="all">All</option>
                    <option value="first">First</option>
                    <option value="last">Last</option>
                  </Select>
                </TableCell>
                <TableCell>
                  <Button onClick={() => onUpdate(rules.filter(r => r.id !== rule.id))}>
                    <Icon>🗑️</Icon>
                  </Button>
                </TableCell>
              </TableRow>
            ))}
          </tbody>
        </Table>
      </Section>
    );
  }
  
  // Extract Configuration
  if (['tExtractJSON', 'tExtractDelimited', 'tExtractRegex', 'tExtractXML'].includes(componentType)) {
    const extractConfig: ExtractConfig = config || { outputColumns: [] };
    
    return (
      <Section>
        <SectionHeader>
          <SectionTitle>Extract Configuration</SectionTitle>
        </SectionHeader>
        
        <FormRow>
          <FormLabel>
            {componentType === 'tExtractDelimited' && 'Delimiter'}
            {componentType === 'tExtractRegex' && 'Regex Pattern'}
            {componentType === 'tExtractXML' && 'XPath Expression'}
            {componentType === 'tExtractJSON' && 'JSONPath Expression'}
          </FormLabel>
          <Input 
            type="text"
            placeholder={
              componentType === 'tExtractDelimited' ? 'e.g., , (comma) or \t (tab)' :
              componentType === 'tExtractRegex' ? 'e.g., ([A-Z]+)\d+' :
              componentType === 'tExtractXML' ? 'e.g., /root/items/item' :
              'e.g., $.store.book[*].title'
            }
            style={{ flex: 'none', width: '300px' }}
          />
        </FormRow>
        
        <div style={{ marginTop: '20px' }}>
          <SectionHeader style={{ marginBottom: '12px' }}>
            <SectionTitle style={{ fontSize: '13px' }}>Output Columns</SectionTitle>
            <Button onClick={() => onUpdate({
              ...extractConfig,
              outputColumns: [
                ...extractConfig.outputColumns,
                { name: '', type: 'string', path: '', length: 0 }
              ]
            })}>
              <Icon>+</Icon> Add Column
            </Button>
          </SectionHeader>
          
          <Table>
            <TableHead>
              <TableRow>
                <TableHeader>Column Name</TableHeader>
                <TableHeader>Data Type</TableHeader>
                <TableHeader>Path/Pattern</TableHeader>
                <TableHeader>Length</TableHeader>
                <TableHeader>Actions</TableHeader>
              </TableRow>
            </TableHead>
            <tbody>
              {extractConfig.outputColumns.map((col, index) => (
                <TableRow key={index}>
                  <TableCell>
                    <Input 
                      type="text"
                      value={col.name}
                      placeholder="Column name..."
                    />
                  </TableCell>
                  <TableCell>
                    <Select value={col.type}>
                      <option value="string">String</option>
                      <option value="integer">Integer</option>
                      <option value="decimal">Decimal</option>
                      <option value="boolean">Boolean</option>
                      <option value="date">Date</option>
                    </Select>
                  </TableCell>
                  <TableCell>
                    <Input 
                      type="text"
                      value={col.path}
                      placeholder="Path or pattern..."
                    />
                  </TableCell>
                  <TableCell>
                    <Input 
                      type="number"
                      value={col.length || ''}
                      placeholder="Length"
                      style={{ width: '80px' }}
                    />
                  </TableCell>
                  <TableCell>
                    <Button onClick={() => onUpdate({
                      ...extractConfig,
                      outputColumns: extractConfig.outputColumns.filter((_, i) => i !== index)
                    })}>
                      <Icon>🗑️</Icon>
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
            </tbody>
          </Table>
        </div>
      </Section>
    );
  }
  
  // Default placeholder
  return (
    <Section>
      <SectionHeader>
        <SectionTitle>Component Parameters</SectionTitle>
      </SectionHeader>
      <div style={{ textAlign: 'center', padding: '40px', color: '#7f8c8d' }}>
        Component-specific parameters will be displayed here based on the selected component type.
      </div>
    </Section>
  );
};

// 4. Advanced Options Sub-component
const AdvancedOptions: React.FC<{
  options: BasicSettingsState['advancedOptions'];
  onUpdate: (key: keyof BasicSettingsState['advancedOptions'], value: any) => void;
}> = ({ options, onUpdate }) => {
  const [expanded, setExpanded] = useState(false);
  
  return (
    <Section>
      <SectionHeader>
        <SectionTitle>Advanced Options</SectionTitle>
        <CollapseButton onClick={() => setExpanded(!expanded)}>
          {expanded ? '▲ Collapse' : '▼ Expand'}
        </CollapseButton>
      </SectionHeader>
      
      {expanded && (
        <>
          <FormRow>
            <FormLabel>Error Handling:</FormLabel>
            <Select 
              value={options.errorHandling}
              onChange={(e) => onUpdate('errorHandling', e.target.value)}
            >
              <option value="fail">Fail on Error</option>
              <option value="skip">Skip Row on Error</option>
              <option value="default">Use Default Value</option>
            </Select>
          </FormRow>
          
          <FormRow>
            <FormLabel>Empty Value Handling:</FormLabel>
            <Select 
              value={options.emptyValueHandling}
              onChange={(e) => onUpdate('emptyValueHandling', e.target.value)}
            >
              <option value="skip">Skip Empty Values</option>
              <option value="default">Use Default</option>
              <option value="null">Set to NULL</option>
            </Select>
          </FormRow>
          
          <FormRow>
            <FormLabel>
              <Checkbox 
                checked={options.parallelization}
                onChange={(e) => onUpdate('parallelization', e.target.checked)}
              />
              Enable Parallel Processing
            </FormLabel>
            {options.parallelization && (
              <>
                <FormLabel style={{ minWidth: '100px' }}>Max Threads:</FormLabel>
                <Input 
                  type="number"
                  value={options.maxThreads}
                  onChange={(e) => onUpdate('maxThreads', parseInt(e.target.value))}
                  style={{ width: '100px' }}
                  min="1"
                  max="64"
                />
                <FormLabel style={{ minWidth: '100px' }}>Batch Size:</FormLabel>
                <Input 
                  type="number"
                  value={options.batchSize}
                  onChange={(e) => onUpdate('batchSize', parseInt(e.target.value))}
                  style={{ width: '100px' }}
                  min="1"
                />
              </>
            )}
          </FormRow>
          
          <FormRow>
            <FormLabel>Log Level:</FormLabel>
            <Select>
              <option value="error">ERROR</option>
              <option value="warn">WARN</option>
              <option value="info">INFO</option>
              <option value="debug">DEBUG</option>
              <option value="trace">TRACE</option>
            </Select>
          </FormRow>
          
          <FormRow>
            <FormLabel>Performance Mode:</FormLabel>
            <Select>
              <option value="balanced">Balanced</option>
              <option value="speed">Optimize for Speed</option>
              <option value="memory">Optimize for Memory</option>
            </Select>
          </FormRow>
        </>
      )}
    </Section>
  );
};

// 5. Action Buttons Zone
const ActionButtonsZoneComponent: React.FC<{
  onOpenSchemaEditor: () => void;
  onPreview: () => void;
  onOpenRegexTester: () => void;
  onOpenLookup: () => void;
}> = ({ onOpenSchemaEditor, onPreview, onOpenRegexTester, onOpenLookup }) => {
  return (
    <ActionButtonsZone>
      <Button variant="primary" onClick={onOpenSchemaEditor}>
        <Icon>📝</Icon> Edit Schema
      </Button>
      <Button onClick={onPreview}>
        <Icon>👁️</Icon> Preview/Test
      </Button>
      <Button onClick={onOpenRegexTester}>
        <Icon>🔍</Icon> Regex Tester
      </Button>
      <Button onClick={onOpenLookup}>
        <Icon>📂</Icon> Browse Lookup
      </Button>
      <Button>
        <Icon>⚙️</Icon> Advanced Settings
      </Button>
      <Button>
        <Icon>📊</Icon> Statistics
      </Button>
      <Button>
        <Icon>📄</Icon> Documentation
      </Button>
    </ActionButtonsZone>
  );
};

// 6. Footer Zone
const FooterZoneComponent: React.FC<{
  status: BasicSettingsState['status'];
  onApply: () => void;
  onOk: () => void;
  onCancel: () => void;
  hasChanges: boolean;
}> = ({ status, onApply, onOk, onCancel, hasChanges }) => {
  const latestMessage = status.messages[status.messages.length - 1];
  
  return (
    <FooterZone>
      <StatusStrip>
        {status.hasErrors && <StatusIcon type="error">!</StatusIcon>}
        {!status.hasErrors && status.hasWarnings && <StatusIcon type="warning">!</StatusIcon>}
        {!status.hasErrors && !status.hasWarnings && latestMessage && (
          <StatusIcon type="info">i</StatusIcon>
        )}
        
        {latestMessage ? (
          <StatusMessage>
            <strong>{latestMessage.type.toUpperCase()}:</strong> {latestMessage.message}
            {latestMessage.details && (
              <span style={{ marginLeft: '8px', color: '#7f8c8d' }}>
                {latestMessage.details}
              </span>
            )}
          </StatusMessage>
        ) : (
          <StatusMessage>
            {hasChanges ? 'Configuration has unsaved changes' : 'Configuration is up to date'}
          </StatusMessage>
        )}
        
        <div style={{ display: 'flex', gap: '4px', marginLeft: 'auto' }}>
          {status.messages.length > 0 && (
            <Button onClick={() => {/* Clear messages */}} style={{ fontSize: '11px' }}>
              Clear ({status.messages.length})
            </Button>
          )}
        </div>
      </StatusStrip>
      
      <div style={{ display: 'flex', gap: '10px', marginLeft: '20px' }}>
        <Button 
          variant="primary" 
          onClick={onApply}
          disabled={!hasChanges}
        >
          Apply
        </Button>
        <Button 
          variant="primary" 
          onClick={onOk}
        >
          OK
        </Button>
        <Button 
          onClick={onCancel}
        >
          Cancel
        </Button>
      </div>
    </FooterZone>
  );
};

// ==================== MAIN COMPONENT ====================
const BasicSettingsPanel: React.FC<BasicSettingsPanelProps> = ({
  componentId,
  componentType,
  initialConfig,
  onSave,
  onCancel,
  onApply
}) => {
  // Initialize state
  const [state, dispatch] = useReducer(settingsReducer, {
    component: {
      id: componentId,
      type: componentType,
      name: `New ${componentType}`,
      description: getComponentDescription(componentType),
      ...initialConfig?.component
    },
    inputSchema: generateMockSchema(),
    outputSchema: generateMockSchema(),
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
      messages: [
        {
          id: 'init',
          type: 'info',
          message: 'Component configuration loaded successfully'
        }
      ],
      hasWarnings: false,
      hasErrors: false
    },
    ...initialConfig
  });

  const [hasChanges, setHasChanges] = useState(false);
  const [showSchemaEditor, setShowSchemaEditor] = useState(false);
  const [showPreview, setShowPreview] = useState(false);

  // Effect to detect changes
  useEffect(() => {
    // In a real app, you would compare with initial state
    setHasChanges(true);
  }, [state]);

  // Handlers
  const handleSave = () => {
    onSave(state);
    dispatch({ type: 'ADD_STATUS_MESSAGE', message: {
      id: `save-${Date.now()}`,
      type: 'info',
      message: 'Configuration saved successfully'
    }});
    setHasChanges(false);
  };

  const handleApply = () => {
    if (onApply) {
      onApply(state);
    }
    dispatch({ type: 'ADD_STATUS_MESSAGE', message: {
      id: `apply-${Date.now()}`,
      type: 'info',
      message: 'Changes applied successfully'
    }});
    setHasChanges(false);
  };

  const handleSyncSchemas = () => {
    dispatch({ type: 'SYNC_SCHEMAS' });
  };

  const handleOpenSchemaEditor = () => {
    setShowSchemaEditor(true);
  };

  const handlePreview = () => {
    setShowPreview(true);
  };

  const handleOpenRegexTester = () => {
    // Placeholder for regex tester dialog
    console.log('Opening regex tester...');
  };

  const handleOpenLookup = () => {
    // Placeholder for lookup dialog
    console.log('Opening lookup dialog...');
  };

  // Render
  return (
    <>
      <PanelContainer>
        {/* Zone 1: Header / Component Information */}
        <HeaderZoneComponent
          component={state.component}
          onUpdate={(config) => dispatch({ type: 'UPDATE_COMPONENT_CONFIG', config })}
        />

        {/* Zone 2: Configuration Options */}
        <ContentContainer>
          <ConfigZone>
            {/* Schema Controls */}
            <SchemaControls
              inputSchema={state.inputSchema}
              outputSchema={state.outputSchema}
              onToggleField={(schemaType, fieldId) => 
                dispatch({ type: 'TOGGLE_SCHEMA_FIELD', schemaType, fieldId })
              }
              onOpenSchemaEditor={handleOpenSchemaEditor}
              onSyncSchemas={handleSyncSchemas}
            />

            {/* Component-Specific Parameters */}
            <ParameterTable
              componentType={componentType}
              config={
                componentType === 'tFilterRow' ? state.filterConditions :
                componentType === 'tConvertType' ? state.typeConversions :
                ['tReplace', 'tReplaceList'].includes(componentType) ? state.replaceRules :
                ['tExtractJSON', 'tExtractDelimited', 'tExtractRegex', 'tExtractXML'].includes(componentType) ? state.extractConfig :
                undefined
              }
              onUpdate={(config) => {
                // This would be more specific in a real implementation
                console.log('Update config:', config);
              }}
            />

            {/* Advanced Options */}
            <AdvancedOptions
              options={state.advancedOptions}
              onUpdate={(key, value) => 
                dispatch({ type: 'UPDATE_ADVANCED_OPTION', key, value })
              }
            />
          </ConfigZone>
        </ContentContainer>

        {/* Zone 3: Action Buttons */}
        <ActionButtonsZoneComponent
          onOpenSchemaEditor={handleOpenSchemaEditor}
          onPreview={handlePreview}
          onOpenRegexTester={handleOpenRegexTester}
          onOpenLookup={handleOpenLookup}
        />

        {/* Zone 4: Footer / Status */}
        <FooterZoneComponent
          status={state.status}
          onApply={handleApply}
          onOk={handleSave}
          onCancel={onCancel}
          hasChanges={hasChanges}
        />
      </PanelContainer>

      {/* Modal Dialogs (Placeholders) */}
      {showSchemaEditor && (
        <div style={{
          position: 'fixed',
          top: '50%',
          left: '50%',
          transform: 'translate(-50%, -50%)',
          background: 'white',
          padding: '20px',
          borderRadius: '8px',
          boxShadow: '0 4px 20px rgba(0, 0, 0, 0.15)',
          zIndex: 2000,
          minWidth: '600px'
        }}>
          <h3>Schema Editor</h3>
          <p>Schema editor dialog would appear here...</p>
          <div style={{ display: 'flex', justifyContent: 'flex-end', gap: '10px', marginTop: '20px' }}>
            <Button variant="primary" onClick={() => setShowSchemaEditor(false)}>Save</Button>
            <Button onClick={() => setShowSchemaEditor(false)}>Cancel</Button>
          </div>
        </div>
      )}

      {showPreview && (
        <div style={{
          position: 'fixed',
          top: '50%',
          left: '50%',
          transform: 'translate(-50%, -50%)',
          background: 'white',
          padding: '20px',
          borderRadius: '8px',
          boxShadow: '0 4px 20px rgba(0, 0, 0, 0.15)',
          zIndex: 2000,
          minWidth: '800px',
          maxHeight: '600px',
          overflow: 'auto'
        }}>
          <h3>Preview Results</h3>
          <Table>
            <TableHead>
              <TableRow>
                <TableHeader>ID</TableHeader>
                <TableHeader>Name</TableHeader>
                <TableHeader>Email</TableHeader>
                <TableHeader>Age</TableHeader>
                <TableHeader>Salary</TableHeader>
                <TableHeader>Department</TableHeader>
                <TableHeader>Hire Date</TableHeader>
              </TableRow>
            </TableHead>
            <tbody>
              {[...Array(10)].map((_, i) => (
                <TableRow key={i}>
                  <TableCell>{i + 1}</TableCell>
                  <TableCell>John Doe {i}</TableCell>
                  <TableCell>john{i}@example.com</TableCell>
                  <TableCell>{25 + i}</TableCell>
                  <TableCell>${50000 + i * 1000}</TableCell>
                  <TableCell>Department {i % 3}</TableCell>
                  <TableCell>2023-01-{String(i + 1).padStart(2, '0')}</TableCell>
                </TableRow>
              ))}
            </tbody>
          </Table>
          <div style={{ display: 'flex', justifyContent: 'flex-end', gap: '10px', marginTop: '20px' }}>
            <Button variant="primary" onClick={() => setShowPreview(false)}>Close</Button>
          </div>
        </div>
      )}
    </>
  );
};

export default BasicSettingsPanel;
