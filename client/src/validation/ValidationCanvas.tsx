// FILE: src/flow/ValidationCanvas.tsx
/**
 * Main React Flow canvas with integrated validation
 * OMITTED:
 * - Complex toolbar and UI controls from original
 * - Multi-canvas support
 * - Undo/redo history integration
 */

import React, { useCallback, useState, useMemo } from 'react';
import ReactFlow, {
  MiniMap,
  Controls,
  Background,
  BackgroundVariant,
  ConnectionMode,
  ReactFlowProvider,
  NodeChange,
  EdgeChange,
  applyNodeChanges,
  applyEdgeChanges
} from 'reactflow';
import 'reactflow/dist/style.css';
import { ValidationProvider } from './ValidationProvider';
import { useValidationHandlers } from './useValidationHandlers';
import { nodeTypes } from './ValidationNode';
import { edgeTypes } from './ValidationEdge';
import { ValidationNode, ValidationEdge } from './flow-types';

interface ValidationCanvasProps {
  initialNodes?: ValidationNode[];
  initialEdges?: ValidationEdge[];
  onGraphChange?: (nodes: ValidationNode[], edges: ValidationEdge[]) => void;
  validationConfig?: any;
  children?: React.ReactNode;
}

// Inner component that uses React Flow hooks
const ValidationCanvasInner: React.FC<ValidationCanvasProps> = ({
  initialNodes: propInitialNodes = [],
  initialEdges: propInitialEdges = [],
  onGraphChange,
  children
}) => {
  const { onConnect, onConnectStart, onConnectEnd, isValidConnection } = useValidationHandlers({
    showToast: useCallback((message: string, type: 'error' | 'warning' | 'info') => {
      if (type === 'error') {
        console.error('Validation Error:', message);
        alert(`Error: ${message}`);
      } else if (type === 'warning') {
        console.warn('Validation Warning:', message);
      } else {
        console.log('Info:', message);
      }
    }, [])
  });

  const [nodes, setNodes] = useState<ValidationNode[]>(propInitialNodes);
  const [edges, setEdges] = useState<ValidationEdge[]>(propInitialEdges);

  // Handle graph changes
  const onNodesChange = useCallback(
    (changes: NodeChange[]) => {
      setNodes((nds) => {
        const updatedNodes = applyNodeChanges(changes, nds as any) as ValidationNode[];
        if (onGraphChange) {
          onGraphChange(updatedNodes, edges);
        }
        return updatedNodes;
      });
    },
    [edges, onGraphChange]
  );

  const onEdgesChange = useCallback(
    (changes: EdgeChange[]) => {
      setEdges((eds) => {
        const updatedEdges = applyEdgeChanges(changes, eds as any) as ValidationEdge[];
        if (onGraphChange) {
          onGraphChange(nodes, updatedEdges);
        }
        return updatedEdges;
      });
    },
    [nodes, onGraphChange]
  );

  // Memoize nodes and edges to prevent unnecessary re-renders
  const memoizedNodes = useMemo(() => nodes, [nodes]);
  const memoizedEdges = useMemo(() => edges, [edges]);

  return (
    <div style={{ width: '100%', height: '100%' }}>
      <ReactFlow
        nodes={memoizedNodes}
        edges={memoizedEdges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        onConnect={onConnect}
        onConnectStart={onConnectStart}
        onConnectEnd={onConnectEnd}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        isValidConnection={isValidConnection}
        connectionMode={ConnectionMode.Strict}
        fitView
        attributionPosition="bottom-right"
      >
        <Background variant={BackgroundVariant.Dots} gap={12} size={1} />
        <Controls />
        <MiniMap />
        {children}
      </ReactFlow>
    </div>
  );
};

// Main export with providers
export const ValidationCanvas: React.FC<ValidationCanvasProps> = (props) => {
  return (
    <ReactFlowProvider>
      <ValidationProvider config={props.validationConfig}>
        <ValidationCanvasInner {...props} />
      </ValidationProvider>
    </ReactFlowProvider>
  );
};

// Example usage component
export const ExampleFlow: React.FC = () => {
  const initialNodes: ValidationNode[] = [
    {
      id: 'source-1',
      type: 'validationNode',
      position: { x: 100, y: 100 },
      data: {
        name: 'Excel Source',
        componentCategory: 'source',
        technology: 'excel'
      }
    },
    {
      id: 'transform-1',
      type: 'validationNode',
      position: { x: 300, y: 100 },
      data: {
        name: 'tMap',
        componentCategory: 'transform'
      }
    },
    {
      id: 'sink-1',
      type: 'validationNode',
      position: { x: 500, y: 100 },
      data: {
        name: 'Database Output',
        componentCategory: 'sink'
      }
    }
  ];

  const initialEdges: ValidationEdge[] = [];

  const handleGraphChange = useCallback((nodes: ValidationNode[], edges: ValidationEdge[]) => {
    console.log('Graph updated:', { nodes, edges });
  }, []);

  return (
    <div style={{ width: '100vw', height: '100vh' }}>
      <ValidationCanvas
        initialNodes={initialNodes}
        initialEdges={initialEdges}
        onGraphChange={handleGraphChange}
      />
    </div>
  );
};