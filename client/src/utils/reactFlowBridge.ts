// src/utils/reactFlowBridge.ts
import { Node } from '@xyflow/react';
import { CustomNode } from '../store/slices/nodesSlice';

export interface ReactFlowUpdateEvent {
  type: 'node' | 'edge' | 'selection' | 'viewport';
  payload: any;
}

class ReactFlowBridge {
  private static instance: ReactFlowBridge;
  private listeners: Map<string, Function[]> = new Map();

  private constructor() {}

  static getInstance(): ReactFlowBridge {
    if (!ReactFlowBridge.instance) {
      ReactFlowBridge.instance = new ReactFlowBridge();
    }
    return ReactFlowBridge.instance;
  }

  // Dispatch updates from React Flow to Redux
  dispatchUpdate(event: ReactFlowUpdateEvent) {
    this.emit('reactflow-update', event);
    
    // Also dispatch to Redux via custom event
    const customEvent = new CustomEvent('reactflow-update', { detail: event });
    window.dispatchEvent(customEvent);
  }

  // Dispatch updates from Redux to React Flow
  dispatchToReactFlow(event: ReactFlowUpdateEvent) {
    this.emit('redux-update', event);
    
    // Dispatch to React Flow via custom event
    const customEvent = new CustomEvent('redux-update', { detail: event });
    window.dispatchEvent(customEvent);
  }

  // Listen for updates
  on(event: string, callback: Function) {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, []);
    }
    this.listeners.get(event)!.push(callback);
    
    // Return unsubscribe function
    return () => {
      const callbacks = this.listeners.get(event) || [];
      const index = callbacks.indexOf(callback);
      if (index > -1) {
        callbacks.splice(index, 1);
      }
    };
  }

  private emit(event: string, data: any) {
    const callbacks = this.listeners.get(event) || [];
    callbacks.forEach(callback => callback(data));
  }

  // Helper to convert between Redux and React Flow formats
  static convertToReactFlowNode(node: any): Node {
    return {
      id: node.id,
      type: node.type || 'customNode',
      position: node.position,
      data: node.data,
      draggable: true,
      selectable: true,
      connectable: true,
    };
  }

  static convertFromReactFlowNode(node: Node): CustomNode {
    return {
      id: node.id,
      type: node.type || 'customNode',
      position: node.position,
      data: node.data,
    } as CustomNode;
  }
}

export default ReactFlowBridge;