import { Node } from '@xyflow/react';

export interface NodeData {
  label: string;
  nodeType: string;
  description?: string;
  enabled?: boolean;
  executionOrder?: number;
  timeout?: number;
  parameters?: {
    [key: string]: any;
  };
  // Add an index signature
  [key: string]: any;
}

export interface CustomNode extends Node {
  data: NodeData;
}

export interface AppState {
  nodes: Array<{
    id: string;
    type: string;
    position: { x: number; y: number };
    data: NodeData;
    selected?: boolean;
  }>;
  edges: Array<{
    id: string;
    source: string;
    target: string;
  }>;
  consoleMessages: string[];
  sidebarOpen: boolean;
}

export interface ConsoleMessage {
  id: string;
  type: 'info' | 'warning' | 'error';
  message: string;
  timestamp: number;
}

export interface WorkflowExecution {
  id: string;
  status: 'running' | 'completed' | 'failed';
  nodes: Record<string, 'pending' | 'running' | 'success' | 'failed'>;
  startTime: number;
  endTime?: number;
  logs: ConsoleMessage[];
}



/**
 * Central export file for all pipeline types
 */

export * from './pipeline-types';
export * from './pipeline-types'; // Re-export all from pipeline-types

// Optional: Export commonly used type aliases
export {
  NodeType,
  PortType,
  PortSide,
  ConnectionStatus,
  PostgreSQLDataType,
  NodeStatus,
  DataSourceType
} from './pipeline-types';
