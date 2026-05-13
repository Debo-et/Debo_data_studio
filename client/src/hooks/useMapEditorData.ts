// src/hooks/useMapEditorData.ts - CORRECTED
import { useMemo } from 'react';
import { Node, Edge } from 'reactflow';
import { getConnectedColumns, MapEditorPayload } from '../utils/columnExtraction';

/**
 * Hook to extract connected columns for a tMap node FROM LIVE REACT FLOW CONNECTIONS
 */
export const useMapEditorData = (
  nodeId: string,
  nodes: Node[],
  edges: Edge[]
): MapEditorPayload => {
  return useMemo(() => {
    if (!nodeId || !nodes.length) {
      return { nodeId, inputColumns: [], outputColumns: [] };
    }
    
    return getConnectedColumns(nodeId, nodes, edges);
  }, [nodeId, nodes, edges]); // Proper dependency array
};