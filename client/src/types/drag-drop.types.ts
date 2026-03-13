// src/types/drag-drop.types.ts

export interface ReactFlowDragData {
  type: 'reactflow';
  nodeType: string;
  component?: {
    id: string;
    name: string;
    type: 'input' | 'output' | 'processing';
    metadata?: Record<string, any>;
  };
  metadata?: Record<string, any>;
  source?: 'sidebar' | 'palette' | 'repository';
  requiresPopup?: boolean;
}

export interface RepositoryDragData {
  type: 'repository';
  nodeId: string;
  nodeName: string;
  nodeType: string;
  metadata?: Record<string, any>;
  componentType?: string;
  source: 'sidebar';
  technology?: string;
}

export type DragData = ReactFlowDragData | RepositoryDragData;

export const REACT_FLOW_DATA_TYPE = 'application/reactflow';
export const REPOSITORY_DATA_TYPE = 'application/repository-node';