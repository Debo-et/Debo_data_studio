import { createSlice, PayloadAction } from '@reduxjs/toolkit';

export interface LogEntry {
  id: string;
  timestamp: number;
  level: 'INFO' | 'WARN' | 'ERROR' | 'DEBUG' | 'SUCCESS';
  message: string;
  source?: string;
}

interface LogsState {
  entries: LogEntry[];
  isAutoScrollEnabled: boolean;
  filterLevel: 'ALL' | 'INFO' | 'WARN' | 'ERROR' | 'DEBUG' | 'SUCCESS';
  isPaused: boolean;
}

const initialState: LogsState = {
  entries: [
    {
      id: '1',
      timestamp: Date.now(),
      level: 'INFO',
      message: 'DataStudio initialized - ready for workflow execution',
      source: 'system'
    },
  ],
  isAutoScrollEnabled: true,
  filterLevel: 'ALL',
  isPaused: false,
};

const logsSlice = createSlice({
  name: 'logs',
  initialState,
  reducers: {
    addLog: (state, action: PayloadAction<Omit<LogEntry, 'id' | 'timestamp'>>) => {
      if (state.isPaused) return;
      
      const newEntry: LogEntry = {
        id: `log-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
        timestamp: Date.now(),
        ...action.payload,
      };
      
      state.entries.push(newEntry);
      
      if (state.entries.length > 2000) {
        state.entries = state.entries.slice(-2000);
      }
    },
    
    // Add the missing action that Console.tsx is trying to use
    addLogEntry: (state, action: PayloadAction<LogEntry>) => {
      if (state.isPaused) return;
      
      state.entries.push(action.payload);
      
      if (state.entries.length > 2000) {
        state.entries = state.entries.slice(-2000);
      }
    },
    
    addConsoleMessage: (state, action: PayloadAction<string>) => {
      if (state.isPaused) return;
      
      const newEntry: LogEntry = {
        id: `log-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
        timestamp: Date.now(),
        level: 'INFO',
        message: action.payload,
        source: 'ui',
      };
      
      state.entries.push(newEntry);
      
      if (state.entries.length > 2000) {
        state.entries = state.entries.slice(-2000);
      }
    },
    
    clearLogs: (state) => {
      state.entries = [];
    },
    
    setAutoScroll: (state, action: PayloadAction<boolean>) => {
      state.isAutoScrollEnabled = action.payload;
    },
    
    setFilterLevel: (state, action: PayloadAction<LogsState['filterLevel']>) => {
      state.filterLevel = action.payload;
    },
    
    togglePause: (state) => {
      state.isPaused = !state.isPaused;
    },
    
    setLogs: (state, action: PayloadAction<LogEntry[]>) => {
      state.entries = action.payload;
    },
  },
});

export const {
  addLog,
  addLogEntry, // Export the new action
  addConsoleMessage,
  clearLogs,
  setAutoScroll,
  setFilterLevel,
  togglePause,
  setLogs,
} = logsSlice.actions;

export default logsSlice.reducer;