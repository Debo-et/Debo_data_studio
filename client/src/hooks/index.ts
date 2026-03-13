import { useDispatch, useSelector, TypedUseSelectorHook } from 'react-redux';
import type { RootState, AppDispatch } from '../store';

export const useAppDispatch = () => useDispatch<AppDispatch>();
export const useAppSelector: TypedUseSelectorHook<RootState> = useSelector;

export const useWorkflow = () => {
  const nodes = useAppSelector((state) => state.nodes.nodes);
  const edges = useAppSelector((state) => state.connections.edges);
  const selectedNodeId = useAppSelector((state) => state.nodes.selectedNodeId);
  
  return { nodes, edges, selectedNodeId };
};

export const useUI = () => {
  const sidebarOpen = useAppSelector((state) => state.ui.sidebarOpen);
  const propertiesPanelOpen = useAppSelector((state) => state.ui.propertiesPanelOpen);
  const consoleHeight = useAppSelector((state) => state.ui.consoleHeight);
  const lastSaved = useAppSelector((state) => state.ui.lastSaved);
  
  return { sidebarOpen, propertiesPanelOpen, consoleHeight, lastSaved };
};

export const useLogs = () => {
  const entries = useAppSelector((state) => state.logs.entries);
  const filterLevel = useAppSelector((state) => state.logs.filterLevel);
  const isAutoScrollEnabled = useAppSelector((state) => state.logs.isAutoScrollEnabled);
  
  return { entries, filterLevel, isAutoScrollEnabled };
};