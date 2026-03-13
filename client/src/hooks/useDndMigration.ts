// src/hooks/useDndMigration.ts
import { useCallback, useState } from 'react';
import { useAppDispatch } from './index';
import { addNode, setSelectedNode } from '../store/slices/nodesSlice';
import { addLog } from '../store/slices/logsSlice';
import { useReactFlow } from '@xyflow/react';

export const useDndMigration = () => {
  const dispatch = useAppDispatch();
  const { screenToFlowPosition } = useReactFlow();
  const [activeDragData, setActiveDragData] = useState<any>(null);

  // Enhanced validation for drop operations
  const validateDrop = useCallback((data: any, position: { x: number; y: number }) => {
    const errors: string[] = [];

    if (!data) {
      errors.push('No drag data available');
    }

    if (!position || typeof position.x !== 'number' || typeof position.y !== 'number') {
      errors.push('Invalid drop position');
    }

    // Add any business logic validations here
    if (data?.nodeType === 'output' && !data?.hasInputConnection) {
      errors.push('Output nodes require input connections');
    }

    return {
      isValid: errors.length === 0,
      errors,
    };
  }, []);

  // Handle node creation from drag data
  const handleNodeCreation = useCallback((
    data: any,
    position: { x: number; y: number },
    _event?: MouseEvent
  ) => {
    const validation = validateDrop(data, position);
    
    if (!validation.isValid) {
      dispatch(addLog({
        level: 'WARN',
        message: `Drop rejected: ${validation.errors.join(', ')}`,
        source: 'dnd-kit'
      }));
      return null;
    }

    const nodeId = `${data.componentType || 'node'}-${Date.now()}`;
    
    const newNode = {
      id: nodeId,
      type: 'customNode',
      position,
      data: {
        label: data.label,
        nodeType: data.nodeType,
        description: data.description,
        componentType: data.componentType,
        icon: data.icon,
        // Preserve all original data
        ...data,
      },
    };

    dispatch(addNode(newNode));
    dispatch(setSelectedNode(nodeId));
    
    dispatch(addLog({
      level: 'INFO',
      message: `Added ${data.label} node to canvas at (${position.x}, ${position.y})`,
      source: 'dnd-kit'
    }));

    return newNode;
  }, [dispatch, validateDrop]);

  // Convert screen coordinates to flow position
  const getFlowPosition = useCallback((clientX: number, clientY: number) => {
    const reactFlowBounds = document.querySelector('.react-flow-wrapper')?.getBoundingClientRect();
    if (!reactFlowBounds) return { x: clientX, y: clientY };

    return screenToFlowPosition({
      x: clientX - reactFlowBounds.left,
      y: clientY - reactFlowBounds.top,
    });
  }, [screenToFlowPosition]);

  return {
    activeDragData,
    setActiveDragData,
    handleNodeCreation,
    getFlowPosition,
    validateDrop,
  };
};