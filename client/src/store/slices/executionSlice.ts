import { createSlice, PayloadAction } from '@reduxjs/toolkit';

export type NodeExecutionStatus = 'idle' | 'pending' | 'running' | 'success' | 'failed' | 'skipped';
export type WorkflowExecutionStatus = 'idle' | 'running' | 'completed' | 'failed';

interface ExecutionState {
  workflowStatus: WorkflowExecutionStatus;
  executionId: string | null;
  nodeStatus: Record<string, NodeExecutionStatus>;
  activeNodeId: string | null;
  startTime: number | null;
  endTime: number | null;
  progress: number;
}

const initialState: ExecutionState = {
  workflowStatus: 'idle',
  executionId: null,
  nodeStatus: {},
  activeNodeId: null,
  startTime: null,
  endTime: null,
  progress: 0,
};

const executionSlice = createSlice({
  name: 'execution',
  initialState,
  reducers: {
    startExecution: (state, action: PayloadAction<{ executionId: string }>) => {
      state.workflowStatus = 'running';
      state.executionId = action.payload.executionId;
      state.startTime = Date.now();
      state.endTime = null;
      state.progress = 0;
      state.activeNodeId = null;
      Object.keys(state.nodeStatus).forEach(nodeId => {
        state.nodeStatus[nodeId] = 'idle';
      });
    },

    setNodeStatus: (state, action: PayloadAction<{ nodeId: string; status: NodeExecutionStatus }>) => {
      state.nodeStatus[action.payload.nodeId] = action.payload.status;
      
      if (action.payload.status === 'running') {
        state.activeNodeId = action.payload.nodeId;
      }
      
      const totalNodes = Object.keys(state.nodeStatus).length;
      if (totalNodes > 0) {
        const completedNodes = Object.values(state.nodeStatus).filter(
          status => status === 'success' || status === 'failed' || status === 'skipped'
        ).length;
        state.progress = Math.round((completedNodes / totalNodes) * 100);
      }
    },

    setActiveNode: (state, action: PayloadAction<string | null>) => {
      state.activeNodeId = action.payload;
    },

    setWorkflowStatus: (state, action: PayloadAction<WorkflowExecutionStatus>) => {
      state.workflowStatus = action.payload;
      if (action.payload === 'completed' || action.payload === 'failed') {
        state.endTime = Date.now();
        state.activeNodeId = null;
      }
    },

    updateProgress: (state, action: PayloadAction<number>) => {
      state.progress = Math.max(0, Math.min(100, action.payload));
    },

    resetExecution: (state) => {
      return {
        ...initialState,
        nodeStatus: { ...state.nodeStatus }
      };
    },

    initializeNodeStatuses: (state, action: PayloadAction<string[]>) => {
      action.payload.forEach(nodeId => {
        if (!state.nodeStatus[nodeId]) {
          state.nodeStatus[nodeId] = 'idle';
        }
      });
    },
  },
});

export const {
  startExecution,
  setNodeStatus,
  setActiveNode,
  setWorkflowStatus,
  updateProgress,
  resetExecution,
  initializeNodeStatuses,
} = executionSlice.actions;

export default executionSlice.reducer;