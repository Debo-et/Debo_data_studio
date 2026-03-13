// src/utils/reactFlowAdapter.ts

import { Node, Edge } from 'reactflow';
import { CanvasNode as CanvasNodeType } from './canvasUtils';

export interface ReactFlowNodeData {
  nodeData: CanvasNodeType;
  onNodeClick?: (node: CanvasNodeType) => void;
  onNodeDoubleClick?: (node: CanvasNodeType) => void;
  onSQLPreview?: (nodeId: string) => void;
  onRegenerateSQL?: (nodeId: string) => void;
}

export const convertToReactFlowNode = (
  canvasNode: CanvasNodeType,
  handlers: {
    onNodeClick?: (node: CanvasNodeType) => void;
    onNodeDoubleClick?: (node: CanvasNodeType) => void;
    onSQLPreview?: (nodeId: string) => void;
    onRegenerateSQL?: (nodeId: string) => void;
  }
): Node<ReactFlowNodeData> => {
  return {
    id: canvasNode.id,
    type: 'canvasNode',
    position: canvasNode.position,
    data: {
      nodeData: canvasNode,
      ...handlers,
    },
    draggable: true,
    selectable: true,
    connectable: true,
  };
};

export const convertToReactFlowEdge = (connection: any): Edge => {
  return {
    id: connection.id,
    source: connection.sourceNodeId,
    target: connection.targetNodeId,
    sourceHandle: connection.sourcePortId,
    targetHandle: connection.targetPortId,
    animated: connection.status === 'pending',
    style: {
      stroke: connection.status === 'invalid' ? '#ef4444' : 
              connection.status === 'pending' ? '#fbbf24' : '#6b7280',
    },
  };
};