import { TypedUseSelectorHook, useDispatch, useSelector } from 'react-redux';
import type { RootState, AppDispatch } from './store';

// Connections
import {
  batchValidateConnections,
  clearAllConnectionErrors,
  clearConnectionHistory,
  createConnection,
  deleteConnection,
  redoConnectionChange,
  setActiveConnection,
  undoConnectionChange,
  updateConnection,
  validateConnection
} from './slices/connectionsSlice';

// SQL Generation
import { generatePipelineSQL } from '../../src/generators/SQLGenerationPipeline';
import {
  clearGeneratedSQL,
  clearSQLCache,
  executeGeneratedSQL,
  saveSQLTemplate,
  updateGenerationSettings,
  validateGeneratedSQL
} from './slices/sqlGenerationSlice';

// Node Registry
import {
  addConnectionRule,
  addSQLTemplate,
  clearRegistryError,
  loadNodeRegistry,
  registerNodeType,
  updateConnectionRule,
  updateNodeType,
  validateNodeType
} from './slices/nodeRegistrySlice';

// ✅ SHARED DOMAIN TYPES (SINGLE SOURCE)
import type {
  CanvasNode,
  CanvasConnection,
  PipelineGenerationOptions
} from '@/types';

// ----------------------------------------------------
// Typed hooks
// ----------------------------------------------------
export const useAppDispatch = () => useDispatch<AppDispatch>();
export const useAppSelector: TypedUseSelectorHook<RootState> = useSelector;

// ----------------------------------------------------
// Connections hook
// ----------------------------------------------------
export const useConnections = () => {
  const dispatch = useAppDispatch();

  return {
    connections: useAppSelector(state => state.connections.entities),
    activeConnection: useAppSelector(state =>
      state.connections.activeConnectionId
        ? state.connections.entities[state.connections.activeConnectionId]
        : null
    ),
    pendingValidations: useAppSelector(state => state.connections.pendingConnections),
    validationResults: useAppSelector(
      state => state.connections.validationResults.entities
    ),
    connectionHistory: useAppSelector(state => state.connections.connectionHistory),
    canUndo: useAppSelector(state => state.connections.historyIndex >= 0),
    canRedo: useAppSelector(
      state =>
        state.connections.historyIndex <
        state.connections.connectionHistory.length - 1
    ),
    loadingStates: useAppSelector(state => state.connections.loadingStates),
    errors: useAppSelector(state => state.connections.errors),

    createConnection: (payload: Parameters<typeof createConnection>[0]) =>
      dispatch(createConnection(payload)),
    updateConnection: (payload: Parameters<typeof updateConnection>[0]) =>
      dispatch(updateConnection(payload)),
    deleteConnection: (payload: Parameters<typeof deleteConnection>[0]) =>
      dispatch(deleteConnection(payload)),
    setActiveConnection: (payload: Parameters<typeof setActiveConnection>[0]) =>
      dispatch(setActiveConnection(payload)),
    validateConnection: (payload: Parameters<typeof validateConnection>[0]) =>
      dispatch(validateConnection(payload)),
    batchValidateConnections: (
      payload: Parameters<typeof batchValidateConnections>[0]
    ) => dispatch(batchValidateConnections(payload)),
    undoConnectionChange: () => dispatch(undoConnectionChange()),
    redoConnectionChange: () => dispatch(redoConnectionChange()),
    clearConnectionHistory: () => dispatch(clearConnectionHistory()),
    clearAllConnectionErrors: () => dispatch(clearAllConnectionErrors())
  };
};

// ----------------------------------------------------
// SQL Generation hook
// ----------------------------------------------------
export const useSQLGeneration = () => {
  const dispatch = useAppDispatch();

  return {
    generatedSQL: useAppSelector(
      state => state.sqlGeneration.generatedSQL.entities
    ),
    generationJobs: useAppSelector(
      state => state.sqlGeneration.generationJobs
    ),

    // ✅ FIXED: no explicit type annotation
    activeGenerationJob: useAppSelector(state =>
      state.sqlGeneration.activeJobId
        ? state.sqlGeneration.generationJobs.find(
            job => job.id === state.sqlGeneration.activeJobId
          ) ?? null
        : null
    ),

    sqlTemplates: useAppSelector(state => state.sqlGeneration.templates),
    sqlCache: useAppSelector(state => state.sqlGeneration.cache),
    settings: useAppSelector(state => state.sqlGeneration.settings),
    loadingStates: useAppSelector(state => state.sqlGeneration.loadingStates),
    errors: useAppSelector(state => state.sqlGeneration.errors),
    executionPlans: useAppSelector(state => state.sqlGeneration.executionPlans),

    generatePipelineSQL: (
      nodes: CanvasNode[],
      connections: CanvasConnection[],
      options?: Partial<PipelineGenerationOptions>
    ) => generatePipelineSQL(nodes, connections, options),

    validateGeneratedSQL: (
      payload: Parameters<typeof validateGeneratedSQL>[0]
    ) => dispatch(validateGeneratedSQL(payload)),
    executeGeneratedSQL: (
      payload: Parameters<typeof executeGeneratedSQL>[0]
    ) => dispatch(executeGeneratedSQL(payload)),
    updateGenerationSettings: (
      payload: Parameters<typeof updateGenerationSettings>[0]
    ) => dispatch(updateGenerationSettings(payload)),
    saveSQLTemplate: (
      payload: Parameters<typeof saveSQLTemplate>[0]
    ) => dispatch(saveSQLTemplate(payload)),
    clearGeneratedSQL: () => dispatch(clearGeneratedSQL()),
    clearSQLCache: (
      payload?: Parameters<typeof clearSQLCache>[0]
    ) => dispatch(clearSQLCache(payload))
  };
};

// ----------------------------------------------------
// Node Registry hook
// ----------------------------------------------------
export const useNodeRegistry = () => {
  const dispatch = useAppDispatch();

  return {
    nodeTypes: useAppSelector(state => state.nodeRegistry.nodeTypes.entities),
    connectionRules: useAppSelector(state => state.nodeRegistry.connectionRules),
    sqlTemplates: useAppSelector(state => state.nodeRegistry.sqlTemplates),
    validators: useAppSelector(state => state.nodeRegistry.validators),
    cache: useAppSelector(state => state.nodeRegistry.cache),
    loading: useAppSelector(state => state.nodeRegistry.loading),
    loaded: useAppSelector(state => state.nodeRegistry.loaded),
    error: useAppSelector(state => state.nodeRegistry.error),

    loadNodeRegistry: () => dispatch(loadNodeRegistry()),
    registerNodeType: (
      payload: Parameters<typeof registerNodeType>[0]
    ) => dispatch(registerNodeType(payload)),
    updateNodeType: (
      payload: Parameters<typeof updateNodeType>[0]
    ) => dispatch(updateNodeType(payload)),
    addConnectionRule: (
      payload: Parameters<typeof addConnectionRule>[0]
    ) => dispatch(addConnectionRule(payload)),
    updateConnectionRule: (
      payload: Parameters<typeof updateConnectionRule>[0]
    ) => dispatch(updateConnectionRule(payload)),
    addSQLTemplate: (
      payload: Parameters<typeof addSQLTemplate>[0]
    ) => dispatch(addSQLTemplate(payload)),
    validateNodeType: (
      payload: Parameters<typeof validateNodeType>[0]
    ) => dispatch(validateNodeType(payload)),
    clearRegistryError: () => dispatch(clearRegistryError())
  };
};
