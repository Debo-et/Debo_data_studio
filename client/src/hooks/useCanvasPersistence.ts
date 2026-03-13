// src/hooks/useCanvasPersistence.ts
import { useCallback, useEffect, useRef } from 'react';
import type { Node, Edge, Viewport } from 'reactflow';
import { persistenceService } from '../services/persistence.service';

export const useCanvasPersistence = () => {
  const autoSaveTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const lastSavedHashRef = useRef<string>('');
  
  // Generate a hash of canvas state for change detection
  const generateStateHash = useCallback((nodes: Node[], edges: Edge[]): string => {
    const simplified = {
      nodeCount: nodes.length,
      edgeCount: edges.length,
      nodeIds: nodes.map(n => n.id).sort(),
      edgeIds: edges.map(e => e.id).sort()
    };
    return JSON.stringify(simplified);
  }, []);
  
  // Save canvas state with debouncing
  const saveCanvasState = useCallback((
    nodes: Node[], 
    edges: Edge[], 
    viewport: Viewport,
    immediate: boolean = false
  ) => {
    const currentHash = generateStateHash(nodes, edges);
    
    // Skip if no changes
    if (currentHash === lastSavedHashRef.current && !immediate) {
      return;
    }
    
    // Clear any pending auto-save
    if (autoSaveTimeoutRef.current) {
      clearTimeout(autoSaveTimeoutRef.current);
    }
    
    const saveOperation = () => {
      persistenceService.saveCanvasState(nodes, edges, viewport);
      lastSavedHashRef.current = currentHash;
      console.log('💾 Canvas state saved');
    };
    
    if (immediate) {
      saveOperation();
    } else {
      // Debounced auto-save
      autoSaveTimeoutRef.current = setTimeout(saveOperation, 1000);
    }
  }, [generateStateHash]);
  
  // Load canvas state
  const loadCanvasState = useCallback(() => {
    const saved = persistenceService.loadCanvasState();
    if (saved) {
      // Update hash to prevent immediate re-save
      lastSavedHashRef.current = generateStateHash(saved.nodes, saved.edges);
      console.log('📂 Loaded canvas state from persistence');
      return saved;
    }
    return null;
  }, [generateStateHash]);
  
  // Clear canvas state
  const clearCanvasState = useCallback(() => {
    persistenceService.clearCanvasState();
    lastSavedHashRef.current = '';
    console.log('🧹 Canvas state cleared');
  }, []);
  
  // Initialize on mount
  useEffect(() => {
    // Load initial hash from saved state
    const saved = persistenceService.loadCanvasState();
    if (saved) {
      lastSavedHashRef.current = generateStateHash(saved.nodes, saved.edges);
    }
    
    // Cleanup on unmount
    return () => {
      if (autoSaveTimeoutRef.current) {
        clearTimeout(autoSaveTimeoutRef.current);
      }
    };
  }, [generateStateHash]);
  
  return {
    saveCanvasState,
    loadCanvasState,
    clearCanvasState,
    getLastSavedHash: () => lastSavedHashRef.current
  };
};