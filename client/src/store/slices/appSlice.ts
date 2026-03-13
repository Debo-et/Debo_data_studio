import { createSlice, createAsyncThunk, PayloadAction } from '@reduxjs/toolkit';
import { api, NodeType, Workflow, RunWorkflowResponse } from '../../api/client';
import { webSocketService, WebSocketMessage } from '../../api/socket';
import { addLog } from './logsSlice';
import { startExecution, setNodeStatus, setWorkflowStatus, initializeNodeStatuses } from './executionSlice';
import { RootState } from '../index';

export const fetchNodeTypes = createAsyncThunk(
  'api/fetchNodeTypes',
  async (_, { rejectWithValue }) => {
    try {
      const nodeTypes = await api.getNodeTypes();
      return nodeTypes;
    } catch (error: any) {
      return rejectWithValue(error.response?.data || error.message);
    }
  }
);

export const runWorkflow = createAsyncThunk(
  'api/runWorkflow',
  async (_, { getState, dispatch, rejectWithValue }) => {
    try {
      const state = getState() as RootState;
      const { nodes } = state.nodes;
      
      const nodeIds = nodes.map(node => node.id);
      dispatch(initializeNodeStatuses(nodeIds));

      const workflow: Workflow = {
        name: `Execution-${Date.now()}`,
        nodes: nodes,
        edges: state.connections.edges,
        metadata: {
          nodeCount: nodes.length,
          connectionCount: state.connections.edges.length,
          timestamp: Date.now(),
        },
      };

      if (workflow.nodes.length === 0) {
        throw new Error('Workflow must contain at least one node');
      }

      dispatch(addLog({
        level: 'INFO',
        message: `Starting workflow execution with ${workflow.nodes.length} nodes`,
        source: 'execution'
      }));

      if (!state.api.isWebSocketConnected) {
        dispatch(connectWebSocket({
          onMessage: (message: WebSocketMessage) => {
            handleExecutionMessage(message, dispatch);
          }
        }));
        
        await new Promise(resolve => setTimeout(resolve, 500));
      }

      const response = await api.runWorkflow(workflow);
      
      dispatch(startExecution({ executionId: response.executionId }));
      
      dispatch(addLog({
        level: 'SUCCESS',
        message: `Workflow execution started: ${response.executionId}`,
        source: 'execution'
      }));

      return response;
    } catch (error: any) {
      const errorMessage = error.response?.data?.message || error.message || 'Workflow execution failed';
      
      dispatch(addLog({
        level: 'ERROR',
        message: errorMessage,
        source: 'execution'
      }));
      
      dispatch(setWorkflowStatus('failed'));
      
      return rejectWithValue(errorMessage);
    }
  }
);

export const saveWorkflow = createAsyncThunk(
  'api/saveWorkflow',
  async (workflowData: { name: string }, { getState, dispatch, rejectWithValue }) => {
    try {
      const state = getState() as RootState;
      
      const workflow: Workflow = {
        name: workflowData.name,
        nodes: state.nodes.nodes,
        edges: state.connections.edges,
      };

      dispatch(addLog({
        level: 'INFO',
        message: `Saving workflow: ${workflowData.name}`,
        source: 'persistence'
      }));

      const response = await api.saveWorkflow(workflow);
      
      dispatch(addLog({
        level: 'SUCCESS',
        message: `Workflow saved successfully with ID: ${response.id}`,
        source: 'persistence'
      }));
      
      return response;
    } catch (error: any) {
      const errorMessage = `Workflow save failed: ${error.response?.data?.message || error.message}`;
      dispatch(addLog({
        level: 'ERROR',
        message: errorMessage,
        source: 'persistence'
      }));
      return rejectWithValue(error.response?.data || error.message);
    }
  }
);

const handleExecutionMessage = (message: WebSocketMessage, dispatch: any) => {
  const { type, data } = message;
  
  switch (type) {
    case 'NODE_STARTED':
      dispatch(setNodeStatus({ nodeId: data.nodeId!, status: 'running' }));
      dispatch(addLog({
        level: 'INFO',
        message: `Node ${data.nodeId} started execution`,
        source: 'node'
      }));
      break;
      
    case 'NODE_COMPLETED':
      dispatch(setNodeStatus({ nodeId: data.nodeId!, status: 'success' }));
      dispatch(addLog({
        level: 'SUCCESS',
        message: `Node ${data.nodeId} completed successfully`,
        source: 'node'
      }));
      break;
      
    case 'NODE_FAILED':
      dispatch(setNodeStatus({ nodeId: data.nodeId!, status: 'failed' }));
      dispatch(addLog({
        level: 'ERROR',
        message: `Node ${data.nodeId} failed: ${data.error}`,
        source: 'node'
      }));
      break;
      
    case 'NODE_SKIPPED':
      dispatch(setNodeStatus({ nodeId: data.nodeId!, status: 'skipped' }));
      dispatch(addLog({
        level: 'WARN',
        message: `Node ${data.nodeId} was skipped`,
        source: 'node'
      }));
      break;
      
    case 'EXECUTION_COMPLETED':
      dispatch(setWorkflowStatus('completed'));
      dispatch(addLog({
        level: 'SUCCESS',
        message: `Workflow execution completed successfully`,
        source: 'execution'
      }));
      break;
      
    case 'EXECUTION_FAILED':
      dispatch(setWorkflowStatus('failed'));
      dispatch(addLog({
        level: 'ERROR',
        message: `Workflow execution failed: ${data.error}`,
        source: 'execution'
      }));
      break;
      
    case 'PROGRESS_UPDATE':
      break;
      
    case 'LOG_MESSAGE':
      dispatch(addLog({
        level: data.level!,
        message: data.message!,
        source: data.source || 'backend'
      }));
      break;
      
    default:
      console.warn('Unknown WebSocket message type:', type);
  }
};

interface ApiState {
  nodeTypes: NodeType[];
  currentExecution: RunWorkflowResponse | null;
  loading: {
    nodeTypes: boolean;
    workflow: boolean;
    save: boolean;
  };
  errors: {
    nodeTypes: string | null;
    workflow: string | null;
    save: string | null;
  };
  isWebSocketConnected: boolean;
  executionId: string | null; // Add this missing property
  localWorkflows: Array<{ id: string; workflow: any }>;
}

const initialState: ApiState = {
  nodeTypes: [],
  currentExecution: null,
  loading: {
    nodeTypes: false,
    workflow: false,
    save: false,
  },
  errors: {
    nodeTypes: null,
    workflow: null,
    save: null,
  },
  isWebSocketConnected: false,
  executionId: null, // Initialize executionId
  localWorkflows: [],
};

const apiSlice = createSlice({
  name: 'api',
  initialState,
  reducers: {
    connectWebSocket: (state, action: PayloadAction<{ onMessage: (message: WebSocketMessage) => void }>) => {
      if (!state.isWebSocketConnected) {
        webSocketService.connect({
          onMessage: action.payload.onMessage,
          onOpen: () => {
            state.isWebSocketConnected = true;
          },
          onClose: () => {
            state.isWebSocketConnected = false;
          },
          onError: () => {
            state.isWebSocketConnected = false;
          },
        });
      }
    },
    
    disconnectWebSocket: (state) => {
      webSocketService.disconnect();
      state.isWebSocketConnected = false;
    },
    
    setWebSocketStatus: (state, action: PayloadAction<boolean>) => {
      state.isWebSocketConnected = action.payload;
    },

    // Add the missing actions that Console.tsx is trying to use
    setWebSocketConnected: (state, action: PayloadAction<boolean>) => {
      state.isWebSocketConnected = action.payload;
    },
    
    setExecutionId: (state, action: PayloadAction<string>) => {
      state.executionId = action.payload;
    },
    
    clearErrors: (state) => {
      state.errors = {
        nodeTypes: null,
        workflow: null,
        save: null,
      };
    },
  },
  extraReducers: (builder) => {
    builder
      .addCase(fetchNodeTypes.pending, (state) => {
        state.loading.nodeTypes = true;
        state.errors.nodeTypes = null;
      })
      .addCase(fetchNodeTypes.fulfilled, (state, action) => {
        state.loading.nodeTypes = false;
        state.nodeTypes = action.payload;
      })
      .addCase(fetchNodeTypes.rejected, (state, action) => {
        state.loading.nodeTypes = false;
        state.errors.nodeTypes = action.payload as string;
      })
      .addCase(runWorkflow.pending, (state) => {
        state.loading.workflow = true;
        state.errors.workflow = null;
      })
      .addCase(runWorkflow.fulfilled, (state, action) => {
        state.loading.workflow = false;
        state.currentExecution = action.payload;
        state.executionId = action.payload.executionId;
      })
      .addCase(runWorkflow.rejected, (state, action) => {
        state.loading.workflow = false;
        state.errors.workflow = action.payload as string;
        state.executionId = null;
      })
      .addCase(saveWorkflow.pending, (state) => {
        state.loading.save = true;
        state.errors.save = null;
      })
      .addCase(saveWorkflow.fulfilled, (state) => {
        state.loading.save = false;
      })
      .addCase(saveWorkflow.rejected, (state, action) => {
        state.loading.save = false;
        state.errors.save = action.payload as string;
      });
  },
});

export const { 
  connectWebSocket, 
  disconnectWebSocket, 
  setWebSocketStatus, 
  setWebSocketConnected, // Export the new action
  setExecutionId, // Export the new action
  clearErrors 
} = apiSlice.actions;
export default apiSlice.reducer;