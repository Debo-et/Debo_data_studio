// src/hooks/index.ts
import { useDispatch, useSelector, TypedUseSelectorHook } from 'react-redux';
import type { RootState, AppDispatch } from '../store';
import { selectAllConnections } from '../store/slices/connectionsSlice';

export const useAppDispatch = () => useDispatch<AppDispatch>();
export const useAppSelector: TypedUseSelectorHook<RootState> = useSelector;

export const useWorkflow = () => {
  const nodes = useAppSelector((state) => state.nodes.nodes);
  const edges = useAppSelector((state) => selectAllConnections(state.connections)); // 👈 compose selector
  const selectedNodeId = useAppSelector((state) => state.nodes.selectedNodeId);
  
  return { nodes, edges, selectedNodeId };
};