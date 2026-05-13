// src/store/slices/connectionsSlice.ts

import { createSlice, createAsyncThunk, PayloadAction, createEntityAdapter, EntityState } from '@reduxjs/toolkit';
import { CanvasConnection, ConnectionStatus } from '../../types/pipeline-types';
import { ConnectionValidationResult } from '../../utils/connection-validator';

// ==================== ENTITY ADAPTERS ====================

// Fixed: Use ExtendedCanvasConnection for the adapter
export const connectionsAdapter = createEntityAdapter<ExtendedCanvasConnection, string>({
  selectId: (connection: ExtendedCanvasConnection) => connection.id,
  sortComparer: (a, b) => a.id.localeCompare(b.id),
});

// Create a wrapper interface for validation results with an id
interface ValidationResultWithId extends ConnectionValidationResult {
  id: string;
}

// Fixed: Explicitly specify string as the ID type for ValidationResultWithId
export const validationResultsAdapter = createEntityAdapter<ValidationResultWithId, string>({
  selectId: (result: ValidationResultWithId) => result.id,
  sortComparer: (a, b) => b.timestamp.localeCompare(a.timestamp),
});

// ==================== TYPES & INTERFACES ====================

// Define extended metadata interface that includes all the properties we need
interface ExtendedConnectionMetadata {
  description?: string;
  createdBy?: string;
  createdAt?: string;
  updatedAt?: string;
  lastValidated?: string;
  validationScore?: number;
  generatedSQL?: string;
  lastSQLGenerated?: string;
  [key: string]: any; // Allow additional properties
}

// Extended CanvasConnection type with proper metadata
export interface ExtendedCanvasConnection extends Omit<CanvasConnection, 'metadata'> {
  metadata?: ExtendedConnectionMetadata;
}

interface ConnectionHistoryEntry {
  id: string;
  timestamp: string;
  action: 'CREATE' | 'UPDATE' | 'DELETE' | 'VALIDATE';
  connectionId: string;
  previousState?: Partial<ExtendedCanvasConnection>;
  newState?: Partial<ExtendedCanvasConnection>;
}

// Fixed: Use ExtendedCanvasConnection and string as the ID type in EntityState
interface ConnectionsState extends EntityState<ExtendedCanvasConnection, string> {
  activeConnectionId: string | null;
  validationResults: EntityState<ValidationResultWithId, string>;
  validationInProgress: boolean;
  lastValidationTimestamp: string | null;
  pendingConnections: string[]; // Connection IDs waiting for validation
  connectionHistory: ConnectionHistoryEntry[];
  historyIndex: number;
  maxHistoryEntries: number;
  errors: {
    connectionErrors: Record<string, string>;
    validationErrors: Record<string, string[]>;
  };
  loadingStates: {
    validating: boolean;
    generating: boolean;
    saving: boolean;
  };
}

interface CreateConnectionPayload {
  sourceNodeId: string;
  sourcePortId: string;
  targetNodeId: string;
  targetPortId: string;
  metadata?: Partial<CanvasConnection['dataFlow']>;
}

interface UpdateConnectionPayload {
  connectionId: string;
  updates: Partial<ExtendedCanvasConnection>;
}

interface ValidateConnectionPayload {
  connectionId: string;
  validateSchema: boolean;
  validatePerformance: boolean;
}

interface BatchValidatePayload {
  connectionIds: string[];
  validateSchema: boolean;
}

interface ConnectionValidationResultPayload {
  connectionId: string;
  result: ConnectionValidationResult;
}

// Helper function to convert validation result to entity with id
const convertToValidationResultWithId = (
  result: ConnectionValidationResult,
  connectionId: string
): ValidationResultWithId => ({
  ...result,
  id: `${connectionId}-${result.timestamp}`,
});

// ==================== ASYNC THUNKS ====================

export const validateConnection = createAsyncThunk<
  ConnectionValidationResultPayload,
  ValidateConnectionPayload,
  { rejectValue: { connectionId: string; error: string } }
>(
  'connections/validateConnection',
  async (payload, { rejectWithValue, getState }) => {
    try {
      const state = getState() as { connections: ConnectionsState };
      const connection = connectionsAdapter.getSelectors().selectById(state.connections, payload.connectionId);
      
      if (!connection) {
        return rejectWithValue({
          connectionId: payload.connectionId,
          error: `Connection ${payload.connectionId} not found`
        });
      }

      // In a real app, this would call the ConnectionValidator service
      // For now, we'll simulate a validation result
      const mockResult: ConnectionValidationResult = {
        isValid: true,
        compatibilityScore: 85,
        errors: [],
        warnings: [],
        info: ['Mock validation'],
        schemaCompatibility: {
          compatibleColumns: 0,
          incompatibleColumns: 0,
          typeCompatibility: []
        },
        performanceImplications: {
          estimatedLatencyMs: 0,
          potentialBottleneck: false,
          recommendations: []
        },
        timestamp: new Date().toISOString()
      };

      return {
        connectionId: payload.connectionId,
        result: mockResult
      };
    } catch (error) {
      return rejectWithValue({
        connectionId: payload.connectionId,
        error: error instanceof Error ? error.message : 'Validation failed'
      });
    }
  }
);

export const batchValidateConnections = createAsyncThunk<
  ConnectionValidationResultPayload[],
  BatchValidatePayload,
  { rejectValue: { errors: Array<{ connectionId: string; error: string }> } }
>(
  'connections/batchValidateConnections',
  async (payload, { rejectWithValue, dispatch }) => {
    const results: ConnectionValidationResultPayload[] = [];
    const errors: Array<{ connectionId: string; error: string }> = [];

    // Validate each connection in sequence
    for (const connectionId of payload.connectionIds) {
      try {
        const result = await dispatch(validateConnection({
          connectionId,
          validateSchema: payload.validateSchema,
          validatePerformance: false
        })).unwrap();
        
        results.push(result);
      } catch (error) {
        errors.push({
          connectionId,
          error: error instanceof Error ? error.message : 'Validation failed'
        });
      }
    }

    if (errors.length > 0) {
      return rejectWithValue({ errors });
    }

    return results;
  }
);

export const generateConnectionSQL = createAsyncThunk<
  { connectionId: string; sql: string },
  { connectionId: string; includeComments: boolean },
  { rejectValue: { connectionId: string; error: string } }
>(
  'connections/generateConnectionSQL',
  async (payload, { rejectWithValue, getState }) => {
    try {
      const state = getState() as { connections: ConnectionsState };
      const connection = connectionsAdapter.getSelectors().selectById(state.connections, payload.connectionId);
      
      if (!connection) {
        return rejectWithValue({
          connectionId: payload.connectionId,
          error: `Connection ${payload.connectionId} not found`
        });
      }

      // Simulate SQL generation
      await new Promise(resolve => setTimeout(resolve, 500));
      
      const sql = `-- Connection SQL for ${connection.id}\n-- Generated at: ${new Date().toISOString()}\nSELECT * FROM connected_tables;`;

      return {
        connectionId: payload.connectionId,
        sql
      };
    } catch (error) {
      return rejectWithValue({
        connectionId: payload.connectionId,
        error: error instanceof Error ? error.message : 'SQL generation failed'
      });
    }
  }
);

// ==================== SLICE DEFINITION ====================

const initialState: ConnectionsState = connectionsAdapter.getInitialState({
  activeConnectionId: null,
  validationResults: validationResultsAdapter.getInitialState(),
  validationInProgress: false,
  lastValidationTimestamp: null,
  pendingConnections: [],
  connectionHistory: [],
  historyIndex: -1,
  maxHistoryEntries: 100,
  errors: {
    connectionErrors: {},
    validationErrors: {}
  },
  loadingStates: {
    validating: false,
    generating: false,
    saving: false
  }
});

const connectionsSlice = createSlice({
  name: 'connections',
  initialState,
  reducers: {
    // CRUD Operations
    createConnection: {
      reducer: (state, action: PayloadAction<ExtendedCanvasConnection>) => {
        const connection = action.payload;
        connectionsAdapter.addOne(state, connection);
        
        // Add to history
        const historyEntry: ConnectionHistoryEntry = {
          id: `history-${Date.now()}`,
          timestamp: new Date().toISOString(),
          action: 'CREATE',
          connectionId: connection.id,
          newState: connection
        };
        
        state.connectionHistory.push(historyEntry);
        state.historyIndex = state.connectionHistory.length - 1;
        
        // Trim history if too long
        if (state.connectionHistory.length > state.maxHistoryEntries) {
          state.connectionHistory.shift();
          state.historyIndex--;
        }
        
        // Clear any previous errors for this connection
        delete state.errors.connectionErrors[connection.id];
        delete state.errors.validationErrors[connection.id];
        
        // Mark for validation
        if (!state.pendingConnections.includes(connection.id)) {
          state.pendingConnections.push(connection.id);
        }
      },
      prepare: (payload: CreateConnectionPayload) => {
        const connectionId = `conn-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
        
        const connection: ExtendedCanvasConnection = {
          id: connectionId,
          sourceNodeId: payload.sourceNodeId,
          sourcePortId: payload.sourcePortId,
          targetNodeId: payload.targetNodeId,
          targetPortId: payload.targetPortId,
          dataFlow: {
            schemaMappings: [],
            ...payload.metadata
          },
          status: ConnectionStatus.UNVALIDATED,
          metadata: {
            createdAt: new Date().toISOString(),
            createdBy: 'user'
          }
        };
        
        return { payload: connection };
      }
    },

    updateConnection: (state, action: PayloadAction<UpdateConnectionPayload>) => {
      const { connectionId, updates } = action.payload;
      const existingConnection = connectionsAdapter.getSelectors().selectById(state, connectionId);
      
      if (!existingConnection) {
        state.errors.connectionErrors[connectionId] = 'Connection not found';
        return;
      }
      
      // Save previous state for history
      const previousState = { ...existingConnection };
      
      // Optimistic update
      connectionsAdapter.updateOne(state, {
        id: connectionId,
        changes: updates
      });
      
      // Add to history
      const historyEntry: ConnectionHistoryEntry = {
        id: `history-${Date.now()}`,
        timestamp: new Date().toISOString(),
        action: 'UPDATE',
        connectionId,
        previousState,
        newState: updates
      };
      
      state.connectionHistory.push(historyEntry);
      state.historyIndex = state.connectionHistory.length - 1;
      
      // Trim history
      if (state.connectionHistory.length > state.maxHistoryEntries) {
        state.connectionHistory.shift();
        state.historyIndex--;
      }
      
      // Mark for re-validation
      if (!state.pendingConnections.includes(connectionId)) {
        state.pendingConnections.push(connectionId);
      }
    },

    deleteConnection: (state, action: PayloadAction<string>) => {
      const connectionId = action.payload;
      const existingConnection = connectionsAdapter.getSelectors().selectById(state, connectionId);
      
      if (!existingConnection) {
        state.errors.connectionErrors[connectionId] = 'Connection not found';
        return;
      }
      
      // Save for history
      const historyEntry: ConnectionHistoryEntry = {
        id: `history-${Date.now()}`,
        timestamp: new Date().toISOString(),
        action: 'DELETE',
        connectionId,
        previousState: existingConnection
      };
      
      state.connectionHistory.push(historyEntry);
      state.historyIndex = state.connectionHistory.length - 1;
      
      // Remove connection
      connectionsAdapter.removeOne(state, connectionId);
      
      // Clear from pending validations
      state.pendingConnections = state.pendingConnections.filter(id => id !== connectionId);
      
      // Clear errors
      delete state.errors.connectionErrors[connectionId];
      delete state.errors.validationErrors[connectionId];
      
      // Clear active connection if deleted
      if (state.activeConnectionId === connectionId) {
        state.activeConnectionId = null;
      }
    },

    // Connection State Management
    setActiveConnection: (state, action: PayloadAction<string | null>) => {
      state.activeConnectionId = action.payload;
    },

    updateConnectionStatus: (
      state,
      action: PayloadAction<{ connectionId: string; status: ConnectionStatus }>
    ) => {
      const { connectionId, status } = action.payload;
      
      connectionsAdapter.updateOne(state, {
        id: connectionId,
        changes: { status }
      });
    },

    // Connection History Management
    undoConnectionChange: (state) => {
      if (state.historyIndex < 0) return;
      
      const historyEntry = state.connectionHistory[state.historyIndex];
      
      switch (historyEntry.action) {
        case 'CREATE':
          // Undo creation by deleting the connection
          connectionsAdapter.removeOne(state, historyEntry.connectionId);
          break;
          
        case 'UPDATE':
          // Undo update by restoring previous state
          if (historyEntry.previousState) {
            connectionsAdapter.updateOne(state, {
              id: historyEntry.connectionId,
              changes: historyEntry.previousState
            });
          }
          break;
          
        case 'DELETE':
          // Undo deletion by restoring connection
          if (historyEntry.previousState) {
            connectionsAdapter.addOne(state, historyEntry.previousState as ExtendedCanvasConnection);
          }
          break;
      }
      
      state.historyIndex--;
    },

    redoConnectionChange: (state) => {
      if (state.historyIndex >= state.connectionHistory.length - 1) return;
      
      state.historyIndex++;
      const historyEntry = state.connectionHistory[state.historyIndex];
      
      switch (historyEntry.action) {
        case 'CREATE':
          // Redo creation by adding the connection
          if (historyEntry.newState) {
            connectionsAdapter.addOne(state, historyEntry.newState as ExtendedCanvasConnection);
          }
          break;
          
        case 'UPDATE':
          // Redo update by applying new state
          if (historyEntry.newState) {
            connectionsAdapter.updateOne(state, {
              id: historyEntry.connectionId,
              changes: historyEntry.newState
            });
          }
          break;
          
        case 'DELETE':
          // Redo deletion by removing connection
          connectionsAdapter.removeOne(state, historyEntry.connectionId);
          break;
      }
    },

    clearConnectionHistory: (state) => {
      state.connectionHistory = [];
      state.historyIndex = -1;
    },

    // Error Management
    clearConnectionError: (state, action: PayloadAction<string>) => {
      delete state.errors.connectionErrors[action.payload];
    },

    clearAllConnectionErrors: (state) => {
      state.errors.connectionErrors = {};
      state.errors.validationErrors = {};
    },

    // Batch Operations
    deleteMultipleConnections: (state, action: PayloadAction<string[]>) => {
      const connectionIds = action.payload;
      
      connectionIds.forEach(connectionId => {
        const existingConnection = connectionsAdapter.getSelectors().selectById(state, connectionId);
        
        if (existingConnection) {
          // Add to history
          const historyEntry: ConnectionHistoryEntry = {
            id: `history-${Date.now()}-${Math.random().toString(36).substr(2, 4)}`,
            timestamp: new Date().toISOString(),
            action: 'DELETE',
            connectionId,
            previousState: existingConnection
          };
          
          state.connectionHistory.push(historyEntry);
        }
      });
      
      // Update history index
      state.historyIndex = state.connectionHistory.length - 1;
      
      // Remove all connections at once
      connectionsAdapter.removeMany(state, connectionIds);
      
      // Clear from pending validations
      state.pendingConnections = state.pendingConnections.filter(
        id => !connectionIds.includes(id)
      );
      
      // Clear active connection if deleted
      if (state.activeConnectionId && connectionIds.includes(state.activeConnectionId)) {
        state.activeConnectionId = null;
      }
      
      // Clear errors
      connectionIds.forEach(id => {
        delete state.errors.connectionErrors[id];
        delete state.errors.validationErrors[id];
      });
    },

    // Connection Validation Management
    markConnectionForValidation: (state, action: PayloadAction<string>) => {
      const connectionId = action.payload;
      
      if (!state.pendingConnections.includes(connectionId)) {
        state.pendingConnections.push(connectionId);
      }
    },

    clearPendingValidations: (state) => {
      state.pendingConnections = [];
    }
  },
  extraReducers: (builder) => {
    // validateConnection
    builder.addCase(validateConnection.pending, (state, action) => {
      state.loadingStates.validating = true;
      
      const connectionId = action.meta.arg.connectionId;
      
      // Update connection status to pending
      connectionsAdapter.updateOne(state, {
        id: connectionId,
        changes: { status: ConnectionStatus.PENDING }
      });
      
      // Clear previous errors
      delete state.errors.validationErrors[connectionId];
    });

    builder.addCase(validateConnection.fulfilled, (state, action) => {
      state.loadingStates.validating = false;
      state.lastValidationTimestamp = new Date().toISOString();
      
      const { connectionId, result } = action.payload;
      
      // Remove from pending validations
      state.pendingConnections = state.pendingConnections.filter(id => id !== connectionId);
      
      // Convert and add validation result
      const resultWithId = convertToValidationResultWithId(result, connectionId);
      validationResultsAdapter.addOne(state.validationResults, resultWithId);
      
      // Update connection status
      const newStatus = result.isValid ? ConnectionStatus.VALID : ConnectionStatus.INVALID;
      
      // Get existing connection to preserve metadata
      const existingConnection = connectionsAdapter.getSelectors().selectById(state, connectionId);
      if (existingConnection) {
        // Create updates with ExtendedCanvasConnection type
        const updates: Partial<ExtendedCanvasConnection> = {
          status: newStatus,
          metadata: {
            ...existingConnection.metadata,
            lastValidated: result.timestamp,
            validationScore: result.compatibilityScore
          }
        };
        
        connectionsAdapter.updateOne(state, {
          id: connectionId,
          changes: updates
        });
      }
      
      // Store errors if any
      if (result.errors.length > 0) {
        state.errors.validationErrors[connectionId] = result.errors;
      }
    });

    builder.addCase(validateConnection.rejected, (state, action) => {
      state.loadingStates.validating = false;
      
      if (action.payload) {
        const { connectionId, error } = action.payload;
        
        connectionsAdapter.updateOne(state, {
          id: connectionId,
          changes: { status: ConnectionStatus.INVALID }
        });
        
        state.errors.validationErrors[connectionId] = [error];
      }
    });

    // batchValidateConnections
    builder.addCase(batchValidateConnections.pending, (state) => {
      state.loadingStates.validating = true;
    });

    builder.addCase(batchValidateConnections.fulfilled, (state, action) => {
      state.loadingStates.validating = false;
      state.lastValidationTimestamp = new Date().toISOString();
      
      action.payload.forEach(({ connectionId, result }) => {
        // Remove from pending
        state.pendingConnections = state.pendingConnections.filter(id => id !== connectionId);
        
        // Convert and add result
        const resultWithId = convertToValidationResultWithId(result, connectionId);
        validationResultsAdapter.addOne(state.validationResults, resultWithId);
        
        // Update connection
        const newStatus = result.isValid ? ConnectionStatus.VALID : ConnectionStatus.INVALID;
        
        // Get existing connection to preserve metadata
        const existingConnection = connectionsAdapter.getSelectors().selectById(state, connectionId);
        if (existingConnection) {
          // Create updates with ExtendedCanvasConnection type
          const updates: Partial<ExtendedCanvasConnection> = {
            status: newStatus,
            metadata: {
              ...existingConnection.metadata,
              lastValidated: result.timestamp,
              validationScore: result.compatibilityScore
            }
          };
          
          connectionsAdapter.updateOne(state, {
            id: connectionId,
            changes: updates
          });
        }
      });
    });

    builder.addCase(batchValidateConnections.rejected, (state, action) => {
      state.loadingStates.validating = false;
      
      if (action.payload) {
        action.payload.errors.forEach(({ connectionId, error }) => {
          state.errors.validationErrors[connectionId] = [error];
          
          connectionsAdapter.updateOne(state, {
            id: connectionId,
            changes: { status: ConnectionStatus.INVALID }
          });
        });
      }
    });

    // generateConnectionSQL
    builder.addCase(generateConnectionSQL.pending, (state) => {
      state.loadingStates.generating = true;
    });

    builder.addCase(generateConnectionSQL.fulfilled, (state, action) => {
      state.loadingStates.generating = false;
      
      const { connectionId, sql } = action.payload;
      
      // Get existing connection to preserve metadata
      const existingConnection = connectionsAdapter.getSelectors().selectById(state, connectionId);
      if (existingConnection) {
        // Create updates with ExtendedCanvasConnection type
        const updates: Partial<ExtendedCanvasConnection> = {
          metadata: {
            ...existingConnection.metadata,
            generatedSQL: sql,
            lastSQLGenerated: new Date().toISOString()
          }
        };
        
        connectionsAdapter.updateOne(state, {
          id: connectionId,
          changes: updates
        });
      }
    });

    builder.addCase(generateConnectionSQL.rejected, (state, action) => {
      state.loadingStates.generating = false;
      
      if (action.payload) {
        const { connectionId, error } = action.payload;
        state.errors.connectionErrors[connectionId] = error;
      }
    });
  }
});

// ==================== SELECTORS ====================

export const {
  selectAll: selectAllConnections,
  selectById: selectConnectionById,
  selectIds: selectConnectionIds,
  selectEntities: selectConnectionEntities,
  selectTotal: selectTotalConnections
} = connectionsAdapter.getSelectors();

export const selectActiveConnection = (state: { connections: ConnectionsState }) => {
  return state.connections.activeConnectionId 
    ? connectionsAdapter.getSelectors().selectById(state.connections, state.connections.activeConnectionId)
    : null;
};

export const selectPendingValidations = (state: { connections: ConnectionsState }) => 
  state.connections.pendingConnections;

export const selectConnectionValidationResults = (state: { connections: ConnectionsState }) => 
  validationResultsAdapter.getSelectors().selectAll(state.connections.validationResults);

export const selectLatestValidationResult = (state: { connections: ConnectionsState }) => {
  const results = validationResultsAdapter.getSelectors().selectAll(state.connections.validationResults);
  return results.length > 0 ? results[0] : null;
};

export const selectConnectionValidationHistory = (state: { connections: ConnectionsState }) =>
  state.connections.connectionHistory;

export const selectCanUndo = (state: { connections: ConnectionsState }) => 
  state.connections.historyIndex >= 0;

export const selectCanRedo = (state: { connections: ConnectionsState }) => 
  state.connections.historyIndex < state.connections.connectionHistory.length - 1;

export const selectConnectionErrors = (state: { connections: ConnectionsState }) =>
  state.connections.errors;

export const selectLoadingStates = (state: { connections: ConnectionsState }) =>
  state.connections.loadingStates;

// ==================== EXPORTS ====================

export const {
  createConnection,
  updateConnection,
  deleteConnection,
  setActiveConnection,
  updateConnectionStatus,
  undoConnectionChange,
  redoConnectionChange,
  clearConnectionHistory,
  clearConnectionError,
  clearAllConnectionErrors,
  deleteMultipleConnections,
  markConnectionForValidation,
  clearPendingValidations
} = connectionsSlice.actions;

export default connectionsSlice.reducer;