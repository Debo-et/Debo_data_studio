import { useState, useCallback, useEffect } from 'react';
import { Node, Edge, Viewport } from 'reactflow';
import { CanvasDesignManager, CanvasDesign } from '../services/canvas-design.service';
import { toast } from 'react-toastify';

export interface UseCanvasDesignReturn {
  // State
  currentDesign: CanvasDesign | null;
  designs: CanvasDesign[];
  isLoading: boolean;
  isSaving: boolean;
  
  // Actions
  createDesign: (name: string, description?: string) => CanvasDesign | null;
  loadDesign: (designId: string) => boolean;
  saveCurrentDesign: (nodes: Node[], edges: Edge[], viewport: Viewport) => boolean;
  duplicateDesign: (designId: string, newName: string) => CanvasDesign | null;
  deleteDesign: (designId: string) => boolean;
  renameDesign: (designId: string, newName: string) => boolean;
  
  // Queries
  getDesignsByJob: (jobId: string) => CanvasDesign[];
  getDesignStatistics: () => ReturnType<typeof CanvasDesignManager.getStatistics>;
  
  // Auto-save
  enableAutoSave: (intervalMs?: number) => () => void;
}

export const useCanvasDesign = (jobId?: string): UseCanvasDesignReturn => {
  const [currentDesign, setCurrentDesign] = useState<CanvasDesign | null>(null);
  const [designs, setDesigns] = useState<CanvasDesign[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [autoSaveInterval, setAutoSaveInterval] = useState<NodeJS.Timeout | null>(null);

  // Load designs on mount and when jobId changes
  useEffect(() => {
    loadDesigns();
  }, [jobId]);

  // Load all designs
  const loadDesigns = useCallback(() => {
    setIsLoading(true);
    try {
      const allDesigns = CanvasDesignManager.getAllDesigns();
      const designArray = Object.values(allDesigns);
      
      setDesigns(designArray);
      
      // Load active design if none is current
      if (!currentDesign) {
        const active = CanvasDesignManager.getActiveDesign();
        if (active && (!jobId || active.jobId === jobId)) {
          setCurrentDesign(active);
        }
      }
    } catch (error) {
      console.error('Error loading designs:', error);
      toast.error('Failed to load canvas designs');
    } finally {
      setIsLoading(false);
    }
  }, [jobId, currentDesign]);

  // Create new design
  const createDesign = useCallback((name: string, description?: string): CanvasDesign | null => {
    if (!jobId) {
      toast.error('Job ID is required to create a design');
      return null;
    }

    try {
      const newDesign = CanvasDesignManager.createDesign(name, jobId, description);
      
      // Set as active
      CanvasDesignManager.setActiveDesign(newDesign.id);
      setCurrentDesign(newDesign);
      
      // Refresh list
      loadDesigns();
      
      toast.success(`Created design: ${name}`);
      return newDesign;
    } catch (error) {
      console.error('Error creating design:', error);
      toast.error('Failed to create design');
      return null;
    }
  }, [jobId, loadDesigns]);

  // Load specific design
  const loadDesign = useCallback((designId: string): boolean => {
    try {
      const design = CanvasDesignManager.getDesign(designId);
      if (!design) {
        toast.error('Design not found');
        return false;
      }

      CanvasDesignManager.setActiveDesign(designId);
      setCurrentDesign(design);
      
      toast.success(`Loaded design: ${design.name}`);
      return true;
    } catch (error) {
      console.error('Error loading design:', error);
      toast.error('Failed to load design');
      return false;
    }
  }, []);

  // Save current design
  const saveCurrentDesign = useCallback((
    nodes: Node[], 
    edges: Edge[], 
    viewport: Viewport
  ): boolean => {
    if (!currentDesign) {
      toast.error('No active design to save');
      return false;
    }

    setIsSaving(true);
    try {
      const success = CanvasDesignManager.saveReactFlowState(
        currentDesign.id,
        nodes,
        edges,
        viewport
      );

      if (success) {
        // Update local state
        const updatedDesign = CanvasDesignManager.getDesign(currentDesign.id);
        if (updatedDesign) {
          setCurrentDesign(updatedDesign);
        }
        
        // Refresh list
        loadDesigns();
        
        toast.success('Design saved successfully');
        return true;
      } else {
        toast.error('Failed to save design');
        return false;
      }
    } catch (error) {
      console.error('Error saving design:', error);
      toast.error('Error saving design');
      return false;
    } finally {
      setIsSaving(false);
    }
  }, [currentDesign, loadDesigns]);

  // Duplicate design
  const duplicateDesign = useCallback((designId: string, newName: string): CanvasDesign | null => {
    try {
      const newDesign = CanvasDesignManager.duplicateDesign(designId, newName, jobId);
      if (newDesign) {
        loadDesigns();
        toast.success(`Duplicated as: ${newName}`);
        return newDesign;
      }
      return null;
    } catch (error) {
      console.error('Error duplicating design:', error);
      toast.error('Failed to duplicate design');
      return null;
    }
  }, [jobId, loadDesigns]);

  // Delete design
  const deleteDesign = useCallback((designId: string): boolean => {
    if (!window.confirm('Are you sure you want to delete this design? This action cannot be undone.')) {
      return false;
    }

    try {
      const success = CanvasDesignManager.deleteDesign(designId);
      if (success) {
        // Clear current design if it was deleted
        if (currentDesign?.id === designId) {
          setCurrentDesign(null);
        }
        
        loadDesigns();
        toast.success('Design deleted');
        return true;
      }
      return false;
    } catch (error) {
      console.error('Error deleting design:', error);
      toast.error('Failed to delete design');
      return false;
    }
  }, [currentDesign, loadDesigns]);

  // Rename design
  const renameDesign = useCallback((designId: string, newName: string): boolean => {
    try {
      const design = CanvasDesignManager.getDesign(designId);
      if (!design) return false;

      const updatedDesign: CanvasDesign = {
        ...design,
        name: newName,
        state: {
          ...design.state,
          name: newName
        }
      };

      CanvasDesignManager.saveDesign(updatedDesign);
      
      // Update current design if it's the one being renamed
      if (currentDesign?.id === designId) {
        setCurrentDesign(updatedDesign);
      }
      
      loadDesigns();
      toast.success('Design renamed');
      return true;
    } catch (error) {
      console.error('Error renaming design:', error);
      toast.error('Failed to rename design');
      return false;
    }
  }, [currentDesign, loadDesigns]);

  // Get designs by job
  const getDesignsByJob = useCallback((targetJobId: string) => {
    return CanvasDesignManager.getDesignsByJobId(targetJobId);
  }, []);

  // Get statistics
  const getDesignStatistics = useCallback(() => {
    return CanvasDesignManager.getStatistics();
  }, []);

  // Enable auto-save
  const enableAutoSave = useCallback((intervalMs: number = 30000) => {
    // Clear existing interval
    if (autoSaveInterval) {
      clearInterval(autoSaveInterval);
    }

    // Set up new interval
    const interval = setInterval(() => {
      // This would be called from the parent component with current state
      console.log('Auto-save interval reached');
    }, intervalMs);

    setAutoSaveInterval(interval);

    // Return cleanup function
    return () => {
      if (interval) {
        clearInterval(interval);
      }
    };
  }, [autoSaveInterval]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (autoSaveInterval) {
        clearInterval(autoSaveInterval);
      }
    };
  }, [autoSaveInterval]);

  return {
    // State
    currentDesign,
    designs,
    isLoading,
    isSaving,
    
    // Actions
    createDesign,
    loadDesign,
    saveCurrentDesign,
    duplicateDesign,
    deleteDesign,
    renameDesign,
    
    // Queries
    getDesignsByJob,
    getDesignStatistics,
    
    // Auto-save
    enableAutoSave
  };
};