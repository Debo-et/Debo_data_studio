// src/store/store.ts

import { configureStore, combineReducers, Action, ThunkAction } from '@reduxjs/toolkit';
import connectionsReducer from './slices/connectionsSlice';
import sqlGenerationReducer from './slices/sqlGenerationSlice';
import nodeRegistryReducer from './slices/nodeRegistrySlice';
import { connectionMiddleware } from './middleware/connectionMiddleware';

// Combine reducers
const rootReducer = combineReducers({
  connections: connectionsReducer,
  sqlGeneration: sqlGenerationReducer,
  nodeRegistry: nodeRegistryReducer,
  // Add other reducers here...
});

// Configure store with middleware
export const store = configureStore({
  reducer: rootReducer,
  middleware: (getDefaultMiddleware) =>
    getDefaultMiddleware({
      serializableCheck: {
        // Ignore these action types
        ignoredActions: [
          'connections/createConnection/pending',
          'connections/validateConnection/fulfilled'
        ],
        // Ignore these field paths in all actions
        ignoredActionPaths: ['meta.arg', 'payload.timestamp'],
        // Ignore these paths in the state
        ignoredPaths: [
          'connections.connectionHistory',
          'sqlGeneration.cache.lastUpdated',
          'nodeRegistry.cache.lastUpdated'
        ]
      },
      thunk: {
        extraArgument: {
          // Add any extra arguments for thunks here
        }
      }
    }).concat(...connectionMiddleware),
  devTools: process.env.NODE_ENV !== 'production'
});

// Export types
export type RootState = ReturnType<typeof store.getState>;
export type AppDispatch = typeof store.dispatch;
export type AppThunk<ReturnType = void> = ThunkAction<
  ReturnType,
  RootState,
  unknown,
  Action<string>
>;

// Export store
export default store;