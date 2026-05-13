// src/store/slices/sqlGenerationSlice.ts

import { createSlice, createAsyncThunk, PayloadAction, createEntityAdapter, EntityState } from '@reduxjs/toolkit';
import { GeneratedSQL } from '../../types/pipeline-types';

// ==================== ENTITY ADAPTERS ====================

export const generatedSQLAdapter = createEntityAdapter<GeneratedSQL>({
  sortComparer: (a, b) => b.metadata.generatedAt.localeCompare(a.metadata.generatedAt),
});

// ==================== TYPES & INTERFACES ====================

export interface SQLGenerationJob {
  id: string;
  nodeIds: string[];
  connectionIds: string[];
  status: 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';
  progress: number;
  startedAt?: string;
  completedAt?: string;
  error?: string;
  generatedSQLIds: string[];
}

export interface SQLTemplate {
  id: string;
  name: string;
  description?: string;
  template: string;
  parameters: Array<{
    name: string;
    type: string;
    required: boolean;
    defaultValue?: any;
  }>;
  tags: string[];
  createdAt: string;
  updatedAt: string;
}

interface SQLGenerationState {
  generatedSQL: EntityState<GeneratedSQL, string>;
  generationJobs: SQLGenerationJob[];
  activeJobId: string | null;
  templates: SQLTemplate[];
  validationErrors: Record<string, string[]>;
  executionPlans: Record<string, any>;
  cache: {
    nodeSQLCache: Record<string, string>;
    connectionSQLCache: Record<string, string>;
    lastUpdated: Record<string, string>;
  };
  settings: {
    autoGenerate: boolean;
    includeComments: boolean;
    formatSQL: boolean;
    targetDialect: 'POSTGRESQL' | 'MYSQL' | 'SQLSERVER' | 'ORACLE';
    maxCacheSize: number;
  };
  loadingStates: {
    generating: boolean;
    validating: boolean;
    executing: boolean;
  };
  errors: {
    generationErrors: Record<string, string>;
    validationErrors: Record<string, string[]>;
    executionErrors: Record<string, string>;
  };
}

interface GenerateSQLPayload {
  nodeIds: string[];
  connectionIds?: string[];
  includeDependencies?: boolean;
  includeComments?: boolean;
  format?: boolean;
}

interface ValidateSQLPayload {
  sql: string;
  context?: {
    nodeId?: string;
    connectionId?: string;
  };
}

interface ExecuteSQLPayload {
  sqlId: string;
  connectionString?: string;
  parameters?: Record<string, any>;
}

// ==================== ASYNC THUNKS ====================

export const generatePipelineSQL = createAsyncThunk<
  GeneratedSQL[],
  GenerateSQLPayload,
  { rejectValue: { errors: Array<{ nodeId: string; error: string }> } }
>(
  'sqlGeneration/generatePipelineSQL',
  async (payload, { rejectWithValue }) => {
    try {
      await new Promise(resolve => setTimeout(resolve, 1000));
      
      const generatedSQL: GeneratedSQL[] = payload.nodeIds.map((nodeId, index) => ({
        id: `sql-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
        sql: `-- Generated SQL for node ${nodeId}\nSELECT * FROM table_${index + 1};`,
        type: 'DQL',
        dependencies: [],
        executionPlan: {
          steps: [
            { step: 1, operation: 'Scan', description: 'Scan table', estimatedRows: 1000, estimatedCost: 100 }
          ],
          canParallelize: false
        },
        performanceHints: ['Consider adding indexes on frequently queried columns'],
        validation: {
          syntaxValid: true,
          semanticValid: true,
          warnings: []
        },
        metadata: {
          generatedAt: new Date().toISOString(),
          generatorVersion: '1.0.0',
          nodeId,
          nodeType: 'transform' as any,
          parameters: payload
        }
      }));

      return generatedSQL;
    } catch (error) {
      const errors = payload.nodeIds.map(nodeId => ({
        nodeId,
        error: error instanceof Error ? error.message : 'Generation failed'
      }));
      return rejectWithValue({ errors });
    }
  }
);

export const validateGeneratedSQL = createAsyncThunk<
  { sqlId: string; validation: GeneratedSQL['validation'] },
  ValidateSQLPayload,
  { rejectValue: { sqlId?: string; error: string } }
>(
  'sqlGeneration/validateGeneratedSQL',
  async (payload, { rejectWithValue }) => {
    try {
      await new Promise(resolve => setTimeout(resolve, 300));
      
      const validation: GeneratedSQL['validation'] = {
        syntaxValid: true,
        semanticValid: true,
        warnings: payload.sql.includes('SELECT *') 
          ? ['Avoid SELECT * in production queries'] 
          : []
      };

      return {
        sqlId: payload.context?.nodeId || 'unknown',
        validation
      };
    } catch (error) {
      return rejectWithValue({
        sqlId: payload.context?.nodeId,
        error: error instanceof Error ? error.message : 'Validation failed'
      });
    }
  }
);

export const executeGeneratedSQL = createAsyncThunk<
  { sqlId: string; result: any; executionTime: number },
  ExecuteSQLPayload,
  { rejectValue: { sqlId: string; error: string } }
>(
  'sqlGeneration/executeGeneratedSQL',
  async (payload, { rejectWithValue }) => {
    try {
      await new Promise(resolve => setTimeout(resolve, 2000));
      
      const result = {
        rowsAffected: Math.floor(Math.random() * 1000),
        executionTime: 150 + Math.random() * 350
      };

      return {
        sqlId: payload.sqlId,
        result,
        executionTime: result.executionTime
      };
    } catch (error) {
      return rejectWithValue({
        sqlId: payload.sqlId,
        error: error instanceof Error ? error.message : 'Execution failed'
      });
    }
  }
);

// ==================== SLICE DEFINITION ====================

const initialState: SQLGenerationState = {
  generatedSQL: generatedSQLAdapter.getInitialState(),
  generationJobs: [],
  activeJobId: null,
  templates: [],
  validationErrors: {},
  executionPlans: {},
  cache: {
    nodeSQLCache: {},
    connectionSQLCache: {},
    lastUpdated: {}
  },
  settings: {
    autoGenerate: true,
    includeComments: true,
    formatSQL: true,
    targetDialect: 'POSTGRESQL',
    maxCacheSize: 1000
  },
  loadingStates: {
    generating: false,
    validating: false,
    executing: false
  },
  errors: {
    generationErrors: {},
    validationErrors: {},
    executionErrors: {}
  }
};

const sqlGenerationSlice = createSlice({
  name: 'sqlGeneration',
  initialState,
  reducers: {
    addGeneratedSQL: (state, action: PayloadAction<GeneratedSQL>) => {
      generatedSQLAdapter.addOne(state.generatedSQL, action.payload);
      
      const { nodeId } = action.payload.metadata;
      if (nodeId) {
        state.cache.nodeSQLCache[nodeId] = action.payload.sql;
        state.cache.lastUpdated[nodeId] = action.payload.metadata.generatedAt;
      }
    },

updateGeneratedSQL: (state, action: PayloadAction<{ sqlId: string; updates: Partial<GeneratedSQL> }>) => {
  const { sqlId, updates } = action.payload;
  const currentSQL = state.generatedSQL.entities[sqlId];
  
  if (currentSQL) {
    // Prepare the update with proper metadata merging
    const changes: Partial<GeneratedSQL> = {
      ...updates,
    };
    
    // If metadata is being updated, merge it properly
    if (updates.metadata) {
      changes.metadata = {
        ...currentSQL.metadata,
        ...updates.metadata,
        updatedAt: new Date().toISOString()
      };
    } else if (Object.keys(updates).length > 0) {
      // If other fields are being updated, update the metadata timestamp
      changes.metadata = {
        ...currentSQL.metadata,
        updatedAt: new Date().toISOString()
      };
    }
    
    generatedSQLAdapter.updateOne(state.generatedSQL, {
      id: sqlId,
      changes
    });
  }
},

    deleteGeneratedSQL: (state, action: PayloadAction<string>) => {
      const sqlId = action.payload;
      const sql = state.generatedSQL.entities[sqlId];
      
      if (sql?.metadata.nodeId) {
        delete state.cache.nodeSQLCache[sql.metadata.nodeId];
        delete state.cache.lastUpdated[sql.metadata.nodeId];
      }
      
      generatedSQLAdapter.removeOne(state.generatedSQL, sqlId);
    },

    clearGeneratedSQL: (state) => {
      generatedSQLAdapter.removeAll(state.generatedSQL);
      state.cache.nodeSQLCache = {};
      state.cache.connectionSQLCache = {};
      state.cache.lastUpdated = {};
    },

    updateSQLCache: (state, action: PayloadAction<{ key: string; sql: string; type: 'node' | 'connection' }>) => {
      const { key, sql, type } = action.payload;
      const now = new Date().toISOString();
      
      if (type === 'node') {
        state.cache.nodeSQLCache[key] = sql;
      } else {
        state.cache.connectionSQLCache[key] = sql;
      }
      
      state.cache.lastUpdated[key] = now;
      
      const cacheKeys = Object.keys(state.cache.lastUpdated);
      if (cacheKeys.length > state.settings.maxCacheSize) {
        const sortedKeys = cacheKeys.sort((a, b) => 
          state.cache.lastUpdated[a].localeCompare(state.cache.lastUpdated[b])
        );
        
        const keysToRemove = sortedKeys.slice(0, cacheKeys.length - state.settings.maxCacheSize);
        keysToRemove.forEach(key => {
          delete state.cache.nodeSQLCache[key];
          delete state.cache.connectionSQLCache[key];
          delete state.cache.lastUpdated[key];
        });
      }
    },

    clearSQLCache: (state, action: PayloadAction<string | undefined>) => {
      const key = action.payload;
      if (key) {
        delete state.cache.nodeSQLCache[key];
        delete state.cache.connectionSQLCache[key];
        delete state.cache.lastUpdated[key];
      } else {
        state.cache.nodeSQLCache = {};
        state.cache.connectionSQLCache = {};
        state.cache.lastUpdated = {};
      }
    },

    createGenerationJob: (state, action: PayloadAction<Omit<SQLGenerationJob, 'id' | 'status' | 'progress' | 'generatedSQLIds'>>) => {
      const job: SQLGenerationJob = {
        ...action.payload,
        id: `job-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
        status: 'pending',
        progress: 0,
        generatedSQLIds: []
      };
      
      state.generationJobs.push(job);
      state.activeJobId = job.id;
    },

    updateGenerationJob: (state, action: PayloadAction<{ jobId: string; updates: Partial<SQLGenerationJob> }>) => {
      const { jobId, updates } = action.payload;
      const jobIndex = state.generationJobs.findIndex(job => job.id === jobId);
      
      if (jobIndex !== -1) {
        state.generationJobs[jobIndex] = {
          ...state.generationJobs[jobIndex],
          ...updates
        };
      }
    },

    cancelGenerationJob: (state, action: PayloadAction<string>) => {
      const jobId = action.payload;
      const jobIndex = state.generationJobs.findIndex(job => job.id === jobId);
      
      if (jobIndex !== -1) {
        state.generationJobs[jobIndex].status = 'cancelled';
        state.generationJobs[jobIndex].completedAt = new Date().toISOString();
        
        if (state.activeJobId === jobId) {
          state.activeJobId = null;
        }
      }
    },

    clearCompletedJobs: (state) => {
      state.generationJobs = state.generationJobs.filter(
        job => !['completed', 'failed', 'cancelled'].includes(job.status)
      );
    },

    updateGenerationSettings: (state, action: PayloadAction<Partial<SQLGenerationState['settings']>>) => {
      state.settings = { ...state.settings, ...action.payload };
    },

    saveSQLTemplate: (state, action: PayloadAction<Omit<SQLTemplate, 'id' | 'createdAt' | 'updatedAt'>>) => {
      const now = new Date().toISOString();
      const template: SQLTemplate = {
        ...action.payload,
        id: `template-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
        createdAt: now,
        updatedAt: now
      };
      
      state.templates.push(template);
    },

    updateSQLTemplate: (state, action: PayloadAction<{ templateId: string; updates: Partial<Omit<SQLTemplate, 'id' | 'createdAt'>> }>) => {
      const { templateId, updates } = action.payload;
      const templateIndex = state.templates.findIndex(t => t.id === templateId);
      
      if (templateIndex !== -1) {
        state.templates[templateIndex] = {
          ...state.templates[templateIndex],
          ...updates,
          updatedAt: new Date().toISOString()
        };
      }
    },

    deleteSQLTemplate: (state, action: PayloadAction<string>) => {
      state.templates = state.templates.filter(t => t.id !== action.payload);
    },

    clearGenerationError: (state, action: PayloadAction<string>) => {
      delete state.errors.generationErrors[action.payload];
    },

    clearValidationErrors: (state, action: PayloadAction<string>) => {
      delete state.errors.validationErrors[action.payload];
    },

    clearAllErrors: (state) => {
      state.errors = {
        generationErrors: {},
        validationErrors: {},
        executionErrors: {}
      };
    }
  },
  extraReducers: (builder) => {
    builder.addCase(generatePipelineSQL.pending, (state, action) => {
      state.loadingStates.generating = true;
      
      const jobId = `job-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
      const job: SQLGenerationJob = {
        id: jobId,
        nodeIds: action.meta.arg.nodeIds,
        connectionIds: action.meta.arg.connectionIds || [],
        status: 'running',
        progress: 10,
        startedAt: new Date().toISOString(),
        generatedSQLIds: []
      };
      
      state.generationJobs.push(job);
      state.activeJobId = jobId;
    });

    builder.addCase(generatePipelineSQL.fulfilled, (state, action) => {
      state.loadingStates.generating = false;
      
      generatedSQLAdapter.addMany(state.generatedSQL, action.payload);
      
      const jobId = state.activeJobId;
      if (jobId) {
        const jobIndex = state.generationJobs.findIndex(j => j.id === jobId);
        if (jobIndex !== -1) {
          state.generationJobs[jobIndex] = {
            ...state.generationJobs[jobIndex],
            status: 'completed',
            progress: 100,
            completedAt: new Date().toISOString(),
            generatedSQLIds: action.payload.map(sql => sql.id)
          };
        }
        
        action.payload.forEach(sql => {
          if (sql.metadata.nodeId) {
            state.cache.nodeSQLCache[sql.metadata.nodeId] = sql.sql;
            state.cache.lastUpdated[sql.metadata.nodeId] = sql.metadata.generatedAt;
          }
        });
        
        state.activeJobId = null;
      }
    });

    builder.addCase(generatePipelineSQL.rejected, (state, action) => {
      state.loadingStates.generating = false;
      
      const jobId = state.activeJobId;
      if (jobId) {
        const jobIndex = state.generationJobs.findIndex(j => j.id === jobId);
        if (jobIndex !== -1) {
          state.generationJobs[jobIndex] = {
            ...state.generationJobs[jobIndex],
            status: 'failed',
            completedAt: new Date().toISOString(),
            error: action.error.message || 'Generation failed'
          };
        }
        
        state.activeJobId = null;
      }
      
      if (action.payload) {
        action.payload.errors.forEach(({ nodeId, error }) => {
          state.errors.generationErrors[nodeId] = error;
        });
      }
    });

    builder.addCase(validateGeneratedSQL.pending, (state) => {
      state.loadingStates.validating = true;
    });

    builder.addCase(validateGeneratedSQL.fulfilled, (state, action) => {
      state.loadingStates.validating = false;
      
      const { sqlId, validation } = action.payload;
      
      const sqlEntries = generatedSQLAdapter.getSelectors().selectAll(state.generatedSQL);
      const sqlEntry = sqlEntries.find(sql => 
        sql.metadata.nodeId === sqlId || sql.id === sqlId
      );
      
      if (sqlEntry && validation) {
        generatedSQLAdapter.updateOne(state.generatedSQL, {
          id: sqlEntry.id,
          changes: { validation }
        });
      }
      
      if (validation?.warnings && validation.warnings.length > 0) {
        state.errors.validationErrors[sqlId] = validation.warnings;
      }
    });

    builder.addCase(validateGeneratedSQL.rejected, (state, action) => {
      state.loadingStates.validating = false;
      
      if (action.payload) {
        const { sqlId, error } = action.payload;
        if (sqlId) {
          state.errors.validationErrors[sqlId] = [error];
        }
      }
    });

    builder.addCase(executeGeneratedSQL.pending, (state) => {
      state.loadingStates.executing = true;
    });

    builder.addCase(executeGeneratedSQL.fulfilled, (state, action) => {
      state.loadingStates.executing = false;
      
      const { sqlId, result, executionTime } = action.payload;
      
      state.executionPlans[sqlId] = {
        ...result,
        executionTime,
        executedAt: new Date().toISOString()
      };
    });

    builder.addCase(executeGeneratedSQL.rejected, (state, action) => {
      state.loadingStates.executing = false;
      
      if (action.payload) {
        const { sqlId, error } = action.payload;
        state.errors.executionErrors[sqlId] = error;
      }
    });
  }
});

// ==================== SELECTORS ====================

export const {
  selectAll: selectAllGeneratedSQL,
  selectById: selectGeneratedSQLById,
  selectIds: selectGeneratedSQLIds,
  selectEntities: selectGeneratedSQLEntities,
  selectTotal: selectTotalGeneratedSQL
} = generatedSQLAdapter.getSelectors((state: { sqlGeneration: SQLGenerationState }) => state.sqlGeneration.generatedSQL);

export const selectGenerationJobs = (state: { sqlGeneration: SQLGenerationState }) =>
  state.sqlGeneration.generationJobs;

export const selectActiveGenerationJob = (state: { sqlGeneration: SQLGenerationState }) => {
  if (!state.sqlGeneration.activeJobId) return null;
  return state.sqlGeneration.generationJobs.find(job => job.id === state.sqlGeneration.activeJobId);
};

export const selectSQLTemplates = (state: { sqlGeneration: SQLGenerationState }) =>
  state.sqlGeneration.templates;

export const selectSQLCache = (state: { sqlGeneration: SQLGenerationState }) =>
  state.sqlGeneration.cache;

export const selectGenerationSettings = (state: { sqlGeneration: SQLGenerationState }) =>
  state.sqlGeneration.settings;

export const selectLoadingStates = (state: { sqlGeneration: SQLGenerationState }) =>
  state.sqlGeneration.loadingStates;

export const selectSQLGenerationErrors = (state: { sqlGeneration: SQLGenerationState }) =>
  state.sqlGeneration.errors;

export const selectExecutionPlans = (state: { sqlGeneration: SQLGenerationState }) =>
  state.sqlGeneration.executionPlans;

// ==================== EXPORTS ====================

export const {
  addGeneratedSQL,
  updateGeneratedSQL,
  deleteGeneratedSQL,
  clearGeneratedSQL,
  updateSQLCache,
  clearSQLCache,
  createGenerationJob,
  updateGenerationJob,
  cancelGenerationJob,
  clearCompletedJobs,
  updateGenerationSettings,
  saveSQLTemplate,
  updateSQLTemplate,
  deleteSQLTemplate,
  clearGenerationError,
  clearValidationErrors,
  clearAllErrors
} = sqlGenerationSlice.actions;

export default sqlGenerationSlice.reducer;