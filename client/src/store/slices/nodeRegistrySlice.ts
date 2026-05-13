// src/store/slices/nodeRegistrySlice.ts

import { createSlice, createAsyncThunk, PayloadAction, createEntityAdapter, EntityState } from '@reduxjs/toolkit';
import { 
  NodeType, 
  ConnectionPort, 
  PortType,
  PortSide,
  PostgreSQLDataType,
  NodeValidationResult
} from '../../types/pipeline-types';

// ==================== ENTITY ADAPTERS ====================

export const nodeTypesAdapter = createEntityAdapter<NodeTypeDefinition, NodeType>({
  selectId: (nodeType) => nodeType.id,
});

// ==================== TYPES & INTERFACES ====================

export interface NodeTypeDefinition {
  /** Unique identifier - required for EntityAdapter */
  id: NodeType;
  /** Node type */
  type: NodeType;
  name: string;
  description: string;
  category: 'input' | 'output' | 'transform' | 'process';
  icon: string;
  color: string;
  defaultSize: { width: number; height: number };
  defaultPorts: ConnectionPort[];
  capabilities: {
    canHaveMultipleInputs: boolean;
    canHaveMultipleOutputs: boolean;
    canBeDataSource: boolean;
    canBeDataSink: boolean;
    canTransformData: boolean;
    supportsTransactions: boolean;
    requiresConfiguration: boolean;
  };
  validationRules: {
    minInputPorts: number;
    maxInputPorts: number;
    minOutputPorts: number;
    maxOutputPorts: number;
    requiredMetadata: string[];
    supportedDataTypes: PostgreSQLDataType[];
  };
  sqlTemplates: {
    create: string;
    select: string;
    insert: string;
    update: string;
    delete: string;
  };
  metadata: {
    version: string;
    author: string;
    createdAt: string;
    updatedAt: string;
    tags: string[];
    lastValidated?: string;
    validationResult?: 'valid' | 'invalid';
  };
}

export interface ConnectionRule {
  id: string;
  sourceType: NodeType;
  targetType: NodeType;
  allowed: boolean;
  validationRules: Array<{
    rule: string;
    message: string;
    severity: 'error' | 'warning' | 'info';
  }>;
  transformationHints: string[];
}

export interface NodeRegistryState {
  [x: string]: any;
  nodeTypes: EntityState<NodeTypeDefinition, NodeType>;
  connectionRules: ConnectionRule[];
  sqlTemplates: Record<string, string>;
  // Change from Record<NodeType, string> to Partial<Record<NodeType, string>>
  validators: Partial<Record<NodeType, string>>; // Maps node type to validator class name (only for registered ones)
  loaded: boolean;
  loading: boolean;
  error: string | null;
  cache: {
    lastUpdated: string;
    nodeTypeCount: number;
    ruleCount: number;
  };
}

interface RegisterNodeTypePayload extends Omit<NodeTypeDefinition, 'type' | 'id'> {
  id: NodeType;
  type: NodeType;
}

interface UpdateNodeTypePayload {
  type: NodeType;
  updates: Partial<NodeTypeDefinition>;
}

interface AddConnectionRulePayload {
  sourceType: NodeType;
  targetType: NodeType;
  allowed: boolean;
  validationRules?: ConnectionRule['validationRules'];
  transformationHints?: string[];
}

// ==================== ASYNC THUNKS ====================

export const loadNodeRegistry = createAsyncThunk<
  { nodeTypes: NodeTypeDefinition[]; connectionRules: ConnectionRule[] },
  void,
  { rejectValue: string }
>(
  'nodeRegistry/loadNodeRegistry',
  async (_, { rejectWithValue }) => {
    try {
      // In a real app, this would load from API or configuration files
      await new Promise(resolve => setTimeout(resolve, 500));
      
      // Mock data
      const mockNodeTypes: NodeTypeDefinition[] = [
        {
          id: NodeType.JOIN,
          type: NodeType.JOIN,
          name: 'Join',
          description: 'Combine data from multiple sources based on a condition',
          category: 'transform',
          icon: 'GitMerge',
          color: '#4f46e5',
          defaultSize: { width: 200, height: 120 },
          defaultPorts: [
            { id: 'input-1', type: PortType.INPUT, side: PortSide.LEFT, position: 30 },
            { id: 'input-2', type: PortType.INPUT, side: PortSide.LEFT, position: 70 },
            { id: 'output-1', type: PortType.OUTPUT, side: PortSide.RIGHT, position: 50 }
          ],
          capabilities: {
            canHaveMultipleInputs: true,
            canHaveMultipleOutputs: false,
            canBeDataSource: false,
            canBeDataSink: false,
            canTransformData: true,
            supportsTransactions: false,
            requiresConfiguration: true
          },
          validationRules: {
            minInputPorts: 2,
            maxInputPorts: 2,
            minOutputPorts: 1,
            maxOutputPorts: 1,
            requiredMetadata: ['joinConfig'],
            supportedDataTypes: Object.values(PostgreSQLDataType)
          },
          sqlTemplates: {
            create: 'CREATE TABLE {table_name} AS\nSELECT * FROM {source1} {join_type} JOIN {source2} ON {condition}',
            select: 'SELECT {columns} FROM {source1} {join_type} JOIN {source2} ON {condition}',
            insert: '',
            update: '',
            delete: ''
          },
          metadata: {
            version: '1.0.0',
            author: 'System',
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString(),
            tags: ['transformation', 'join', 'sql']
          }
        },
        {
          id: NodeType.MAP,
          type: NodeType.MAP,
          name: 'Map',
          description: 'Transform and map data between schemas',
          category: 'transform',
          icon: 'Map',
          color: '#10b981',
          defaultSize: { width: 180, height: 100 },
          defaultPorts: [
            { id: 'input-1', type: PortType.INPUT, side: PortSide.LEFT, position: 50 },
            { id: 'output-1', type: PortType.OUTPUT, side: PortSide.RIGHT, position: 50 }
          ],
          capabilities: {
            canHaveMultipleInputs: false,
            canHaveMultipleOutputs: false,
            canBeDataSource: false,
            canBeDataSink: false,
            canTransformData: true,
            supportsTransactions: false,
            requiresConfiguration: true
          },
          validationRules: {
            minInputPorts: 1,
            maxInputPorts: 1,
            minOutputPorts: 1,
            maxOutputPorts: 1,
            requiredMetadata: ['mapEditorConfig'],
            supportedDataTypes: Object.values(PostgreSQLDataType)
          },
          sqlTemplates: {
            create: 'CREATE TABLE {table_name} AS\nSELECT {mappings} FROM {source}',
            select: 'SELECT {mappings} FROM {source}',
            insert: 'INSERT INTO {target} ({columns}) SELECT {mappings} FROM {source}',
            update: '',
            delete: ''
          },
          metadata: {
            version: '1.0.0',
            author: 'System',
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString(),
            tags: ['transformation', 'mapping', 'etl']
          }
        },
        {
          id: NodeType.INPUT,
          type: NodeType.INPUT,
          name: 'Input',
          description: 'Data input source node',
          category: 'input',
          icon: 'Database',
          color: '#3b82f6',
          defaultSize: { width: 180, height: 100 },
          defaultPorts: [
            { id: 'output-1', type: PortType.OUTPUT, side: PortSide.RIGHT, position: 50, maxConnections: 10 }
          ],
          capabilities: {
            canHaveMultipleInputs: false,
            canHaveMultipleOutputs: true,
            canBeDataSource: true,
            canBeDataSink: false,
            canTransformData: false,
            supportsTransactions: false,
            requiresConfiguration: true
          },
          validationRules: {
            minInputPorts: 0,
            maxInputPorts: 0,
            minOutputPorts: 1,
            maxOutputPorts: 1,
            requiredMetadata: ['sourceMetadata'],
            supportedDataTypes: Object.values(PostgreSQLDataType)
          },
          sqlTemplates: {
            create: 'CREATE TABLE {table_name} AS\nSELECT * FROM {source_table}',
            select: 'SELECT * FROM {source_table}',
            insert: '',
            update: '',
            delete: ''
          },
          metadata: {
            version: '1.0.0',
            author: 'System',
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString(),
            tags: ['input', 'source', 'etl']
          }
        },
        {
          id: NodeType.OUTPUT,
          type: NodeType.OUTPUT,
          name: 'Output',
          description: 'Data output destination node',
          category: 'output',
          icon: 'Save',
          color: '#ef4444',
          defaultSize: { width: 180, height: 100 },
          defaultPorts: [
            { id: 'input-1', type: PortType.INPUT, side: PortSide.LEFT, position: 50, maxConnections: 1 }
          ],
          capabilities: {
            canHaveMultipleInputs: true,
            canHaveMultipleOutputs: false,
            canBeDataSource: false,
            canBeDataSink: true,
            canTransformData: false,
            supportsTransactions: true,
            requiresConfiguration: true
          },
          validationRules: {
            minInputPorts: 1,
            maxInputPorts: 1,
            minOutputPorts: 0,
            maxOutputPorts: 0,
            requiredMetadata: ['postgresConfig'],
            supportedDataTypes: Object.values(PostgreSQLDataType)
          },
          sqlTemplates: {
            create: '',
            select: '',
            insert: 'INSERT INTO {target_table} ({columns}) VALUES ({values})',
            update: '',
            delete: ''
          },
          metadata: {
            version: '1.0.0',
            author: 'System',
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString(),
            tags: ['output', 'destination', 'etl']
          }
        }
      ];

      const mockConnectionRules: ConnectionRule[] = [
        {
          id: 'rule-1',
          sourceType: NodeType.INPUT,
          targetType: NodeType.MAP,
          allowed: true,
          validationRules: [
            {
              rule: 'schema_compatible',
              message: 'Source schema must be compatible with target',
              severity: 'error'
            }
          ],
          transformationHints: ['Use explicit column mappings for better performance']
        },
        {
          id: 'rule-2',
          sourceType: NodeType.MAP,
          targetType: NodeType.OUTPUT,
          allowed: true,
          validationRules: [],
          transformationHints: ['Ensure output table exists or can be created']
        },
        {
          id: 'rule-3',
          sourceType: NodeType.INPUT,
          targetType: NodeType.JOIN,
          allowed: true,
          validationRules: [
            {
              rule: 'has_required_columns',
              message: 'Input must have columns for join condition',
              severity: 'error'
            }
          ],
          transformationHints: []
        }
      ];

      return { nodeTypes: mockNodeTypes, connectionRules: mockConnectionRules };
    } catch (error) {
      return rejectWithValue(
        error instanceof Error ? error.message : 'Failed to load node registry'
      );
    }
  }
);

export const validateNodeType = createAsyncThunk<
  { nodeType: NodeType; result: NodeValidationResult },
  NodeType,
  { rejectValue: { nodeType: NodeType; error: string } }
>(
  'nodeRegistry/validateNodeType',
  async (nodeType, { rejectWithValue, getState }) => {
    try {
      const state = getState() as { nodeRegistry: NodeRegistryState };
      const definition = nodeTypesAdapter.getSelectors().selectById(state.nodeRegistry.nodeTypes, nodeType);
      
      if (!definition) {
        return rejectWithValue({
          nodeType,
          error: `Node type ${nodeType} not found in registry`
        });
      }

      // In a real app, this would use the NodeValidatorFactory
      await new Promise(resolve => setTimeout(resolve, 200));
      
      const result: NodeValidationResult = {
        isValid: true,
        nodeId: 'registry-check',
        nodeType,
        issues: [],
        suggestions: [],
        postgresCompatibility: {
          compatible: true,
          issues: [],
          requiredExtensions: []
        },
        metadata: {
          validatedAt: new Date().toISOString(),
          validatorVersion: '1.0.0'
        }
      };

      return { nodeType, result };
    } catch (error) {
      return rejectWithValue({
        nodeType,
        error: error instanceof Error ? error.message : 'Validation failed'
      });
    }
  }
);

// ==================== SLICE DEFINITION ====================

const initialState: NodeRegistryState = {
  nodeTypes: nodeTypesAdapter.getInitialState(),
  connectionRules: [],
  sqlTemplates: {},
  validators: {}, // Now {} is valid for Partial<Record<NodeType, string>>
  loaded: false,
  loading: false,
  error: null,
  cache: {
    lastUpdated: '',
    nodeTypeCount: 0,
    ruleCount: 0
  }
};

const nodeRegistrySlice = createSlice({
  name: 'nodeRegistry',
  initialState,
  reducers: {
    // Node Type Management
    registerNodeType: (state, action: PayloadAction<RegisterNodeTypePayload>) => {
      const nodeType = action.payload;
      nodeTypesAdapter.addOne(state.nodeTypes, nodeType);
      
      // Update cache
      state.cache.lastUpdated = new Date().toISOString();
      state.cache.nodeTypeCount = nodeTypesAdapter.getSelectors().selectTotal(state.nodeTypes);
    },

    updateNodeType: (state, action: PayloadAction<UpdateNodeTypePayload>) => {
      const { type, updates } = action.payload;
      
      const existingEntity = state.nodeTypes.entities[type];
      if (existingEntity) {
        nodeTypesAdapter.updateOne(state.nodeTypes, {
          id: type,
          changes: {
            ...updates,
            metadata: {
              ...existingEntity.metadata,
              ...updates.metadata,
              updatedAt: new Date().toISOString()
            }
          }
        });
      }
      
      state.cache.lastUpdated = new Date().toISOString();
    },

    unregisterNodeType: (state, action: PayloadAction<NodeType>) => {
      nodeTypesAdapter.removeOne(state.nodeTypes, action.payload);
      
      // Remove associated rules
      state.connectionRules = state.connectionRules.filter(
        rule => rule.sourceType !== action.payload && rule.targetType !== action.payload
      );
      
      // Update cache
      state.cache.lastUpdated = new Date().toISOString();
      state.cache.nodeTypeCount = nodeTypesAdapter.getSelectors().selectTotal(state.nodeTypes);
      state.cache.ruleCount = state.connectionRules.length;
    },

    // Connection Rule Management
    addConnectionRule: (state, action: PayloadAction<AddConnectionRulePayload>) => {
      const { sourceType, targetType, allowed, validationRules = [], transformationHints = [] } = action.payload;
      
      const rule: ConnectionRule = {
        id: `rule-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
        sourceType,
        targetType,
        allowed,
        validationRules,
        transformationHints
      };
      
      state.connectionRules.push(rule);
      state.cache.ruleCount = state.connectionRules.length;
    },

    updateConnectionRule: (state, action: PayloadAction<{ ruleId: string; updates: Partial<ConnectionRule> }>) => {
      const { ruleId, updates } = action.payload;
      const ruleIndex = state.connectionRules.findIndex(rule => rule.id === ruleId);
      
      if (ruleIndex !== -1) {
        state.connectionRules[ruleIndex] = {
          ...state.connectionRules[ruleIndex],
          ...updates
        };
      }
    },

    removeConnectionRule: (state, action: PayloadAction<string>) => {
      state.connectionRules = state.connectionRules.filter(rule => rule.id !== action.payload);
      state.cache.ruleCount = state.connectionRules.length;
    },

    // SQL Template Management
    addSQLTemplate: (state, action: PayloadAction<{ key: string; template: string }>) => {
      state.sqlTemplates[action.payload.key] = action.payload.template;
    },

    updateSQLTemplate: (state, action: PayloadAction<{ key: string; template: string }>) => {
      state.sqlTemplates[action.payload.key] = action.payload.template;
    },

    removeSQLTemplate: (state, action: PayloadAction<string>) => {
      delete state.sqlTemplates[action.payload];
    },

    // Validator Management
    registerValidator: (state, action: PayloadAction<{ nodeType: NodeType; validatorClass: string }>) => {
      state.validators[action.payload.nodeType] = action.payload.validatorClass;
    },

    unregisterValidator: (state, action: PayloadAction<NodeType>) => {
      delete state.validators[action.payload];
    },

    // Cache Management
    clearRegistryCache: (state) => {
      state.cache = {
        lastUpdated: '',
        nodeTypeCount: 0,
        ruleCount: 0
      };
    },

    // Error Management
    clearRegistryError: (state) => {
      state.error = null;
    }
  },
  extraReducers: (builder) => {
    // loadNodeRegistry
    builder.addCase(loadNodeRegistry.pending, (state) => {
      state.loading = true;
      state.error = null;
    });

    builder.addCase(loadNodeRegistry.fulfilled, (state, action) => {
      state.loading = false;
      state.loaded = true;
      
      // Add node types
      nodeTypesAdapter.setAll(state.nodeTypes, action.payload.nodeTypes);
      
      // Add connection rules
      state.connectionRules = action.payload.connectionRules;
      
      // Update cache
      state.cache = {
        lastUpdated: new Date().toISOString(),
        nodeTypeCount: action.payload.nodeTypes.length,
        ruleCount: action.payload.connectionRules.length
      };
    });

    builder.addCase(loadNodeRegistry.rejected, (state, action) => {
      state.loading = false;
      state.loaded = false;
      state.error = action.payload || 'Failed to load node registry';
    });

    // validateNodeType
    builder.addCase(validateNodeType.fulfilled, (state, action) => {
      const { nodeType, result } = action.payload;
      
      // Update node type with validation result
      const existingEntity = state.nodeTypes.entities[nodeType];
      if (existingEntity) {
        nodeTypesAdapter.updateOne(state.nodeTypes, {
          id: nodeType,
          changes: {
            metadata: {
              ...existingEntity.metadata,
              lastValidated: result.metadata.validatedAt,
              validationResult: result.isValid ? 'valid' : 'invalid'
            }
          }
        });
      }
    });
  }
});

// ==================== SELECTORS ====================

export const {
  selectAll: selectAllNodeTypes,
  selectById: selectNodeTypeById,
  selectIds: selectNodeTypeIds,
  selectEntities: selectNodeTypeEntities,
  selectTotal: selectTotalNodeTypes
} = nodeTypesAdapter.getSelectors((state: { nodeRegistry: NodeRegistryState }) => state.nodeRegistry.nodeTypes);

export const selectConnectionRules = (state: { nodeRegistry: NodeRegistryState }) =>
  state.nodeRegistry.connectionRules;

export const selectConnectionRule = (sourceType: NodeType, targetType: NodeType) => 
  (state: { nodeRegistry: NodeRegistryState }) => {
    return state.nodeRegistry.connectionRules.find(
      rule => rule.sourceType === sourceType && rule.targetType === targetType
    );
  };

export const selectSQLTemplates = (state: { nodeRegistry: NodeRegistryState }) =>
  state.nodeRegistry.sqlTemplates;

export const selectSQLTemplate = (key: string) => 
  (state: { nodeRegistry: NodeRegistryState }) => state.nodeRegistry.sqlTemplates[key];

export const selectValidators = (state: { nodeRegistry: NodeRegistryState }) =>
  state.nodeRegistry.validators;

export const selectValidatorForNodeType = (nodeType: NodeType) => 
  (state: { nodeRegistry: NodeRegistryState }) => state.nodeRegistry.validators[nodeType];

export const selectNodeRegistryCache = (state: { nodeRegistry: NodeRegistryState }) =>
  state.nodeRegistry.cache;

export const selectNodeRegistryLoading = (state: { nodeRegistry: NodeRegistryState }) =>
  state.nodeRegistry.loading;

export const selectNodeRegistryLoaded = (state: { nodeRegistry: NodeRegistryState }) =>
  state.nodeRegistry.loaded;

export const selectNodeRegistryError = (state: { nodeRegistry: NodeRegistryState }) =>
  state.nodeRegistry.error;

// ==================== EXPORTS ====================

export const {
  registerNodeType,
  updateNodeType,
  unregisterNodeType,
  addConnectionRule,
  updateConnectionRule,
  removeConnectionRule,
  addSQLTemplate,
  updateSQLTemplate,
  removeSQLTemplate,
  registerValidator,
  unregisterValidator,
  clearRegistryCache,
  clearRegistryError
} = nodeRegistrySlice.actions;

export default nodeRegistrySlice.reducer;