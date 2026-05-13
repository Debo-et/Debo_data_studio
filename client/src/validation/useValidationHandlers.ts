// FILE: src/flow/useValidationHandlers.ts
/**
 * React Flow hooks for validation integration
 */

import { useCallback } from 'react';
import { Connection, addEdge, useReactFlow, OnConnectStart, OnConnectEnd } from 'reactflow';
import { useValidation } from './ValidationProvider';
import { ValidationEdge, ValidationNodeData, ValidationEdgeData, ValidationNode } from './flow-types';

interface UseValidationHandlersProps {
  onValidationError?: (errors: string[], connection: Connection) => void;
  onValidationWarning?: (warnings: string[], connection: Connection) => void;
  showToast?: (message: string, type: 'error' | 'warning' | 'info') => void;
}

export const useValidationHandlers = (props?: UseValidationHandlersProps) => {
  const { getNodes, getEdges, setEdges } = useReactFlow<ValidationNodeData, ValidationEdgeData>();
  const { validateConnection, wouldCauseCycle, validatePort } = useValidation();
  
  const showToast = useCallback((message: string, type: 'error' | 'warning' | 'info') => {
    // Use provided toast or default to console
    if (props?.showToast) {
      props.showToast(message, type);
    } else {
      if (type === 'error') {
        console.error('Validation Error:', message);
        alert(`Error: ${message}`);
      } else if (type === 'warning') {
        console.warn('Validation Warning:', message);
      } else {
        console.log('Info:', message);
      }
    }
  }, [props]);

  // Main connection validation handler for React Flow's onConnect
  const onConnect = useCallback(
    (connection: Connection) => {
      const nodes = getNodes() as ValidationNode[];
      const edges = getEdges() as ValidationEdge[];
      
      // Basic validation
      if (!connection.source || !connection.target) {
        showToast('Source and target are required', 'error');
        return;
      }

      // Check for self-connection
      if (connection.source === connection.target) {
        showToast('Cannot connect a node to itself', 'error');
        return;
      }

      // Check for cycles
      if (wouldCauseCycle(connection.source, connection.target, edges)) {
        showToast('Connection would create a cycle', 'error');
        return;
      }

      // Check port availability
      const sourceNode = nodes.find(n => n.id === connection.source);
      const targetNode = nodes.find(n => n.id === connection.target);
      
      if (connection.sourceHandle && sourceNode) {
        const portValid = validatePort(
          connection.sourceHandle,
          'source',
          sourceNode,
          edges
        );
        if (!portValid) {
          showToast('Source port is already connected', 'error');
          return;
        }
      }

      if (connection.targetHandle && targetNode) {
        const portValid = validatePort(
          connection.targetHandle,
          'target',
          targetNode,
          edges
        );
        if (!portValid) {
          showToast('Target port is already in use', 'error');
          return;
        }
      }

      // Run comprehensive validation
      const validation = validateConnection(connection, nodes, edges);
      
      if (!validation.isValid && validation.errors.length > 0) {
        validation.errors.forEach(error => {
          showToast(error.message, 'error');
        });
        if (props?.onValidationError) {
          props.onValidationError(
            validation.errors.map(e => e.message),
            connection
          );
        }
        return; // Reject connection
      }

      if (validation.warnings.length > 0) {
        validation.warnings.forEach(warning => {
          showToast(warning.message, 'warning');
        });
        if (props?.onValidationWarning) {
          props.onValidationWarning(
            validation.warnings.map(w => w.message),
            connection
          );
        }
        // Warnings don't block connection, just inform
      }

      // All validation passed - create the edge
      const edgeId = `edge-${connection.source}-${connection.target}-${Date.now()}`;
      const newEdge: ValidationEdge = {
        id: edgeId,
        source: connection.source,
        target: connection.target,
        sourceHandle: connection.sourceHandle || undefined, // Convert null to undefined
        targetHandle: connection.targetHandle || undefined, // Convert null to undefined
        type: 'validationEdge',
        data: {
          label: `${sourceNode?.data.name} → ${targetNode?.data.name}`,
          validationStatus: validation.warnings.length > 0 ? 'warning' : 'valid'
        } as ValidationEdgeData
      };

      setEdges((eds) => addEdge(newEdge, eds));
      
      showToast('Connection created successfully', 'info');
    },
    [getNodes, getEdges, setEdges, validateConnection, wouldCauseCycle, validatePort, showToast, props]
  );

  // Handler for validating before connection starts (optional)
  const onConnectStart: OnConnectStart = useCallback(
    (_event: React.MouseEvent | React.TouchEvent, params) => {
      // Optional: Highlight valid/invalid targets as user drags
      console.log('Connect started from:', params);
    },
    []
  );

  // Handler for connection end (optional)
  const onConnectEnd: OnConnectEnd = useCallback(
    (_event: MouseEvent | TouchEvent) => {
      // Optional: Clean up any visual feedback
      console.log('Connect ended');
    },
    []
  );

  // Custom connection line validation (optional)
  const isValidConnection = useCallback(
    (connection: Connection) => {
      const nodes = getNodes() as ValidationNode[];
      const edges = getEdges() as ValidationEdge[];
      
      // Basic checks
      if (!connection.source || !connection.target) return false;
      if (connection.source === connection.target) return false;
      
      // Quick cycle check
      if (wouldCauseCycle(connection.source, connection.target, edges)) return false;
      
      // Quick port availability check
      const sourceNode = nodes.find(n => n.id === connection.source);
      const targetNode = nodes.find(n => n.id === connection.target);
      
      if (connection.sourceHandle && sourceNode) {
        const portValid = validatePort(
          connection.sourceHandle,
          'source',
          sourceNode,
          edges
        );
        if (!portValid) return false;
      }
      
      if (connection.targetHandle && targetNode) {
        const portValid = validatePort(
          connection.targetHandle,
          'target',
          targetNode,
          edges
        );
        if (!portValid) return false;
      }
      
      return true;
    },
    [getNodes, getEdges, wouldCauseCycle, validatePort]
  );

  return {
    onConnect,
    onConnectStart,
    onConnectEnd,
    isValidConnection
  };
};