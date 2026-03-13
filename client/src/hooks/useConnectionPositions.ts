// src/hooks/useConnectionPositions.ts

import { useState, useRef, useCallback } from 'react';
import { CanvasNode, ComponentPort } from '../utils/canvasUtils';
import { PortSide } from '../types/pipeline-types';

export const useConnectionPositions = () => {
  const portPositions = useRef<Map<string, { x: number; y: number }>>(new Map());
  const nodePositions = useRef<Map<string, { x: number; y: number }>>(new Map());
  const nodeTransforms = useRef<Map<string, { x: number; y: number }>>(new Map());
  const [forceUpdate, setForceUpdate] = useState(0);

  const calculatePortAbsolutePosition = useCallback((
    node: CanvasNode, 
    port: ComponentPort, 
    transform?: { x: number; y: number }
  ): { x: number; y: number } => {
    let offsetX = 0;
    let offsetY = 0;
    
    switch (port.side) {
      case PortSide.LEFT:
        offsetX = 0;
        offsetY = (node.size.height * port.position) / 100;
        break;
      case PortSide.RIGHT:
        offsetX = node.size.width;
        offsetY = (node.size.height * port.position) / 100;
        break;
      case PortSide.TOP:
        offsetX = (node.size.width * port.position) / 100;
        offsetY = 0;
        break;
      case PortSide.BOTTOM:
        offsetX = (node.size.width * port.position) / 100;
        offsetY = node.size.height;
        break;
    }
    
    const baseX = node.position.x + offsetX;
    const baseY = node.position.y + offsetY;
    
    if (transform) {
      return {
        x: baseX + transform.x,
        y: baseY + transform.y
      };
    }
    
    return { x: baseX, y: baseY };
  }, []);

  const updateNodePosition = useCallback((nodeId: string, position: { x: number; y: number }) => {
    nodePositions.current.set(nodeId, position);
    nodeTransforms.current.delete(nodeId);
    setForceUpdate(prev => prev + 1);
  }, []);

  const updateNodeTransform = useCallback((nodeId: string, transform: { x: number; y: number } | null) => {
    if (transform) {
      nodeTransforms.current.set(nodeId, transform);
    } else {
      nodeTransforms.current.delete(nodeId);
    }
    setForceUpdate(prev => prev + 1);
  }, []);

  const updatePortPosition = useCallback((nodeId: string, portId: string, position: { x: number; y: number }) => {
    const key = `${nodeId}:${portId}`;
    portPositions.current.set(key, position);
  }, []);

  const getPortPosition = useCallback((nodeId: string, portId: string): { x: number; y: number } => {
    const key = `${nodeId}:${portId}`;
    return portPositions.current.get(key) || { x: 0, y: 0 };
  }, []);

  const getAdjustedPortPosition = useCallback((nodeId: string, portId: string, node?: CanvasNode, port?: ComponentPort): { x: number; y: number } => {
    const key = `${nodeId}:${portId}`;
    const cachedPosition = portPositions.current.get(key);
    
    if (cachedPosition) {
      const transform = nodeTransforms.current.get(nodeId);
      if (transform) {
        return {
          x: cachedPosition.x + transform.x,
          y: cachedPosition.y + transform.y
        };
      }
      return cachedPosition;
    }
    
    if (node && port) {
      const basePosition = calculatePortAbsolutePosition(node, port);
      const transform = nodeTransforms.current.get(nodeId);
      
      if (transform) {
        return {
          x: basePosition.x + transform.x,
          y: basePosition.y + transform.y
        };
      }
      return basePosition;
    }
    
    return { x: 0, y: 0 };
  }, [calculatePortAbsolutePosition]);

  const getNodeTransform = useCallback((nodeId: string) => {
    return nodeTransforms.current.get(nodeId);
  }, []);

  const clearNodePositions = useCallback(() => {
    nodePositions.current.clear();
    nodeTransforms.current.clear();
    portPositions.current.clear();
  }, []);

  const getAllPortPositions = useCallback(() => {
    return new Map(portPositions.current);
  }, []);

  return { 
    updateNodePosition, 
    updateNodeTransform,
    updatePortPosition, 
    getPortPosition,
    getAdjustedPortPosition,
    getNodeTransform,
    getAllPortPositions,
    clearNodePositions,
    forceUpdate
  };
};