// src/hooks/useReactFlowSync.ts
import { useEffect } from 'react';
import { useAppDispatch } from './useAppDispatch';
import { 
  syncWithReactFlow, 
  setSelectedNode, 
  setSelectedEdge 
} from '../store/slices/nodesSlice';

export const useReactFlowSync = (
  reactFlowNodes: any[],
  reactFlowEdges: any[],
  selectedNodeId: string | null,
  selectedEdgeId: string | null
) => {
  const dispatch = useAppDispatch();
  
  useEffect(() => {
    // Convert React Flow nodes to Redux format
    const reduxNodes = reactFlowNodes.map(node => ({
      id: node.id,
      type: node.type || 'customNode',
      position: node.position,
      data: node.data?.nodeData?.data || node.data,
    }));
    
    dispatch(syncWithReactFlow({
      nodes: reduxNodes,
      edges: reactFlowEdges
    }));
  }, [reactFlowNodes, reactFlowEdges, dispatch]);
  
  useEffect(() => {
    dispatch(setSelectedNode(selectedNodeId));
  }, [selectedNodeId, dispatch]);
  
  useEffect(() => {
    dispatch(setSelectedEdge(selectedEdgeId));
  }, [selectedEdgeId, dispatch]);
};