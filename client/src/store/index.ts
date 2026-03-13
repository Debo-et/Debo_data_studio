import { configureStore } from '@reduxjs/toolkit';
import nodesReducer from './slices/nodesSlice';
import connectionsReducer from './slices/connectionsSlice';
import logsReducer from './slices/logsSlice';
import uiReducer from './slices/uiSlice';
import apiReducer from './slices/appSlice';
import executionReducer from './slices/executionSlice';

export const store = configureStore({
  reducer: {
    nodes: nodesReducer,
    connections: connectionsReducer,
    logs: logsReducer,
    ui: uiReducer,
    api: apiReducer,
    execution: executionReducer,
  },
  middleware: (getDefaultMiddleware) =>
    getDefaultMiddleware({
      serializableCheck: {
        ignoredActions: ['persist/PERSIST', 'api/connectWebSocket'],
        ignoredPaths: ['api.socket'],
      },
    }),
});

export type RootState = ReturnType<typeof store.getState>;
export type AppDispatch = typeof store.dispatch;