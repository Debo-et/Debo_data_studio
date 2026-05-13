import { Middleware, Dispatch, AnyAction, isAnyOf } from '@reduxjs/toolkit';
import { RootState } from '../store';
import { 
  createConnection, 
  updateConnection, 
  deleteConnection,
  validateConnection,
  markConnectionForValidation,
  batchValidateConnections
} from '../slices/connectionsSlice';
import { generatePipelineSQL, clearSQLCache } from '../slices/sqlGenerationSlice';

/**
 * Middleware for auto-validation on connection changes
 */
export const connectionValidationMiddleware: Middleware<
  object,
  RootState,
  Dispatch<AnyAction>
> = store => next => action => {
  const result = next(action);
  
  // Check if action is a connection change that requires validation
  const isConnectionChange = isAnyOf(createConnection, updateConnection)(action as AnyAction);
  
  if (isConnectionChange) {
    const state = store.getState();
    const connectionId = (action as AnyAction).payload?.id || (action as AnyAction).payload?.connectionId;
    
    if (connectionId) {
      const connection = state.connections.entities[connectionId];
      
      if (connection) {
        // Mark for validation
        store.dispatch(markConnectionForValidation(connection.id));
        
        // Auto-validate if setting is enabled
        if (state.sqlGeneration.settings.autoGenerate) {
          store.dispatch(validateConnection({
            connectionId: connection.id,
            validateSchema: true,
            validatePerformance: false
          }) as any);
        }
      }
    }
  }
  
  // Check if action is a delete that should clear cache
  if ((action as AnyAction).type === deleteConnection.type) {
    const connectionId = (action as AnyAction).payload;
    store.dispatch(clearSQLCache(connectionId));
  }
  
  return result;
};

/**
 * Middleware for SQL regeneration on configuration updates
 */
export const sqlRegenerationMiddleware: Middleware<
  object,
  RootState,
  Dispatch<AnyAction>
> = store => next => action => {
  const state = store.getState();
  const previousState = { ...state };
  
  const result = next(action);
  
  const newState = store.getState();
  
  // Check if nodes or connections changed in a way that requires SQL regeneration
  // FIXED: Use nodeRegistry instead of nodes
  const nodesChanged = 
    previousState.nodeRegistry.ids.length !== newState.nodeRegistry.ids.length ||
    JSON.stringify(previousState.nodeRegistry.entities) !== JSON.stringify(newState.nodeRegistry.entities);
  
  const connectionsChanged =
    previousState.connections.ids.length !== newState.connections.ids.length ||
    JSON.stringify(previousState.connections.entities) !== JSON.stringify(newState.connections.entities);
  
  // Check if specific node configurations changed
  let nodeConfigChanged = false;
  const actionType = (action as AnyAction).type;
  if (actionType.includes('updateNode') || actionType.includes('updateConnection')) {
    nodeConfigChanged = true;
  }
  
  // Trigger SQL regeneration if needed and auto-generate is enabled
  if ((nodesChanged || connectionsChanged || nodeConfigChanged) && newState.sqlGeneration.settings.autoGenerate) {
    const nodeIds = newState.nodeRegistry.ids as string[];
    const connectionIds = newState.connections.ids as string[];
    
    if (nodeIds.length > 0) {
      // Debounce regeneration to avoid excessive calls
      const debounceTime = 1000;
      
      // Clear previous timeout if exists
      if ((store as any)._sqlRegenerationTimeout) {
        clearTimeout((store as any)._sqlRegenerationTimeout);
      }
      
      (store as any)._sqlRegenerationTimeout = setTimeout(() => {
        store.dispatch(generatePipelineSQL({
          nodeIds,
          connectionIds,
          includeComments: newState.sqlGeneration.settings.includeComments,
          format: newState.sqlGeneration.settings.formatSQL
        }) as any);
      }, debounceTime);
    }
  }
  
  return result;
};

/**
 * Middleware for connection history tracking
 */
export const connectionHistoryMiddleware: Middleware<
  object,
  RootState,
  Dispatch<AnyAction>
> = _store => next => action => {
  // Track specific connection-related actions
  const actionType = (action as AnyAction).type;
  const trackedActions = [
    'connections/createConnection',
    'connections/updateConnection',
    'connections/deleteConnection',
    'connections/deleteMultipleConnections'
  ];
  
  if (trackedActions.includes(actionType)) {
    // Log to analytics or external service
    if (process.env.NODE_ENV === 'development') {
      console.log('Connection history tracked:', {
        action: actionType,
        timestamp: new Date().toISOString(),
        payload: (action as AnyAction).payload
      });
    }
  }
  
  return next(action);
};

/**
 * Middleware for error handling
 */
export const connectionErrorMiddleware: Middleware<
  object,
  RootState,
  Dispatch<AnyAction>
> = _store => next => action => {
  // Handle rejected async actions
  if ((action as AnyAction).type.endsWith('/rejected')) {
    const error = (action as AnyAction).error || (action as AnyAction).payload?.error;
    
    if (error && process.env.NODE_ENV === 'development') {
      console.error('Redux error:', {
        action: (action as AnyAction).type,
        error,
        timestamp: new Date().toISOString()
      });
    }
    
    // You could dispatch a notification action here
    // store.dispatch(addNotification({ type: 'error', message: error }));
  }
  
  return next(action);
};

/**
 * Middleware for batch validation optimization
 */
export const batchValidationMiddleware: Middleware<
  object,
  RootState,
  Dispatch<AnyAction>
> = store => next => action => {
  const state = store.getState();
  const actionType = (action as AnyAction).type;
  
  // If multiple connections are marked for validation, batch them
  if (
    actionType === 'connections/markConnectionForValidation' &&
    state.connections.pendingConnections.length >= 3
  ) {
    // Debounce batch validation
    const debounceTime = 500;
    
    if ((store as any)._batchValidationTimeout) {
      clearTimeout((store as any)._batchValidationTimeout);
    }
    
    (store as any)._batchValidationTimeout = setTimeout(() => {
      const currentState = store.getState();
      if (currentState.connections.pendingConnections.length > 0) {
        store.dispatch(batchValidateConnections({
          connectionIds: currentState.connections.pendingConnections,
          validateSchema: true
        }) as any);
      }
    }, debounceTime);
  }
  
  return next(action);
};

/**
 * Composite middleware that combines all connection-related middleware
 */
export const connectionMiddleware: Middleware[] = [
  connectionValidationMiddleware,
  sqlRegenerationMiddleware,
  connectionHistoryMiddleware,
  connectionErrorMiddleware,
  batchValidationMiddleware
];