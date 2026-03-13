// Canvas Design Service - Production-grade with React Flow serialization
import { Node, Edge, Viewport, NodeTypes } from 'reactflow';
import { v4 as uuidv4 } from 'uuid';
import { toast } from 'react-toastify';

// Types for canvas persistence
export interface CanvasDesignState {
  id: string;
  name: string;
  description?: string;
  jobId: string;
  nodes: Node[];
  edges: Edge[];
  viewport: Viewport;
  nodeTypes?: NodeTypes;
  metadata: {
    version: string;
    createdAt: string;
    lastModified: string;
    nodeCount: number;
    edgeCount: number;
    canvasType: 'reactflow' | 'custom';
  };
  customData?: Record<string, any>;
}

export interface CanvasDesign {
  id: string;
  name: string;
  description?: string;
  jobId: string;
  state: CanvasDesignState;
  tags?: string[];
  isActive?: boolean;
  lastAccessed?: string;
}

export class CanvasDesignManager {
  private static readonly STORAGE_KEY = 'canvas-designs-v2';
  private static readonly ACTIVE_DESIGN_KEY = 'active-canvas-design-id';
  private static readonly VERSION = '2.0.0';

  // Create a new design with proper React Flow serialization
  static createDesign(name: string, jobId: string, description?: string): CanvasDesign {
    const defaultViewport: Viewport = { x: 0, y: 0, zoom: 1 };
    const designId = `design-${uuidv4()}`;
    const timestamp = new Date().toISOString();

    const design: CanvasDesign = {
      id: designId,
      name,
      description,
      jobId,
      state: {
        id: designId,
        name,
        jobId,
        nodes: [],
        edges: [],
        viewport: defaultViewport,
        metadata: {
          version: this.VERSION,
          createdAt: timestamp,
          lastModified: timestamp,
          nodeCount: 0,
          edgeCount: 0,
          canvasType: 'reactflow'
        }
      },
      tags: ['draft'],
      lastAccessed: timestamp
    };

    this.saveDesign(design);
    return design;
  }

  // Get all designs for a specific job
  static getDesignsByJobId(jobId: string): CanvasDesign[] {
    const allDesigns = this.getAllDesigns();
    return Object.values(allDesigns).filter(design => design.jobId === jobId);
  }

  // Get specific design
  static getDesign(designId: string): CanvasDesign | null {
    try {
      const designs = this.getAllDesigns();
      return designs[designId] || null;
    } catch (error) {
      console.error('Error getting design:', error);
      return null;
    }
  }

  // Save design with React Flow state
  static saveDesign(design: CanvasDesign): void {
    try {
      const designs = this.getAllDesigns();
      
      // Update metadata
      const updatedDesign: CanvasDesign = {
        ...design,
        state: {
          ...design.state,
          metadata: {
            ...design.state.metadata,
            lastModified: new Date().toISOString(),
            nodeCount: design.state.nodes.length,
            edgeCount: design.state.edges.length
          }
        },
        lastAccessed: new Date().toISOString()
      };

      designs[design.id] = updatedDesign;
      localStorage.setItem(this.STORAGE_KEY, JSON.stringify(designs));

      // Update active design if this is it
      if (design.isActive) {
        localStorage.setItem(this.ACTIVE_DESIGN_KEY, design.id);
      }

    } catch (error) {
      console.error('Error saving design:', error);
      toast.error('Failed to save canvas design');
    }
  }

  // Save React Flow state directly
  static saveReactFlowState(
    designId: string,
    nodes: Node[],
    edges: Edge[],
    viewport: Viewport,
    customData?: Record<string, any>
  ): boolean {
    try {
      const design = this.getDesign(designId);
      if (!design) return false;

      const updatedDesign: CanvasDesign = {
        ...design,
        state: {
          ...design.state,
          nodes: this.sanitizeNodes(nodes),
          edges: this.sanitizeEdges(edges),
          viewport,
          customData: customData || design.state.customData
        }
      };

      this.saveDesign(updatedDesign);
      return true;
    } catch (error) {
      console.error('Error saving React Flow state:', error);
      return false;
    }
  }

  // Load React Flow state
  static loadReactFlowState(designId: string): {
    nodes: Node[];
    edges: Edge[];
    viewport: Viewport;
    customData?: Record<string, any>;
  } | null {
    try {
      const design = this.getDesign(designId);
      if (!design) return null;

      return {
        nodes: design.state.nodes,
        edges: design.state.edges,
        viewport: design.state.viewport,
        customData: design.state.customData
      };
    } catch (error) {
      console.error('Error loading React Flow state:', error);
      return null;
    }
  }

  // Duplicate design
  static duplicateDesign(
    sourceDesignId: string,
    newName: string,
    newJobId?: string
  ): CanvasDesign | null {
    try {
      const sourceDesign = this.getDesign(sourceDesignId);
      if (!sourceDesign) return null;

      const newDesignId = `design-${uuidv4()}`;
      const timestamp = new Date().toISOString();

      // Deep clone nodes with new IDs
      const clonedNodes = sourceDesign.state.nodes.map(node => ({
        ...node,
        id: `${node.type}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
        selected: false // Reset selection state
      }));

      // Deep clone edges with new IDs and updated node references
      const clonedEdges = sourceDesign.state.edges.map(edge => {
        // Find corresponding cloned nodes for source/target
        const sourceNode = clonedNodes.find(n => 
          n.data?.nodeData?.id === edge.source || n.id === edge.source
        );
        const targetNode = clonedNodes.find(n => 
          n.data?.nodeData?.id === edge.target || n.id === edge.target
        );

        return {
          ...edge,
          id: `edge-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
          source: sourceNode?.id || edge.source,
          target: targetNode?.id || edge.target,
          selected: false
        };
      });

      const newDesign: CanvasDesign = {
        id: newDesignId,
        name: newName,
        description: `${sourceDesign.description || ''} (Copy)`,
        jobId: newJobId || sourceDesign.jobId,
        state: {
          id: newDesignId,
          name: newName,
          jobId: newJobId || sourceDesign.jobId,
          nodes: clonedNodes,
          edges: clonedEdges,
          viewport: { ...sourceDesign.state.viewport },
          metadata: {
            version: this.VERSION,
            createdAt: timestamp,
            lastModified: timestamp,
            nodeCount: clonedNodes.length,
            edgeCount: clonedEdges.length,
            canvasType: 'reactflow'
          },
          customData: sourceDesign.state.customData 
            ? JSON.parse(JSON.stringify(sourceDesign.state.customData))
            : undefined
        },
        tags: [...(sourceDesign.tags || []), 'copy'],
        lastAccessed: timestamp
      };

      this.saveDesign(newDesign);
      return newDesign;
    } catch (error) {
      console.error('Error duplicating design:', error);
      return null;
    }
  }

  // Delete design
  static deleteDesign(designId: string): boolean {
    try {
      const designs = this.getAllDesigns();
      
      if (!designs[designId]) return false;
      
      delete designs[designId];
      localStorage.setItem(this.STORAGE_KEY, JSON.stringify(designs));

      // Clear active design if this was active
      const activeId = localStorage.getItem(this.ACTIVE_DESIGN_KEY);
      if (activeId === designId) {
        localStorage.removeItem(this.ACTIVE_DESIGN_KEY);
      }

      // Clean up React Flow storage
      this.cleanupReactFlowStorage(designId);
      
      return true;
    } catch (error) {
      console.error('Error deleting design:', error);
      return false;
    }
  }

  // Set active design
  static setActiveDesign(designId: string): void {
    localStorage.setItem(this.ACTIVE_DESIGN_KEY, designId);
    
    // Update design's lastAccessed
    const design = this.getDesign(designId);
    if (design) {
      this.saveDesign({
        ...design,
        lastAccessed: new Date().toISOString(),
        isActive: true
      });
    }
  }

  // Get active design
  static getActiveDesign(): CanvasDesign | null {
    try {
      const activeId = localStorage.getItem(this.ACTIVE_DESIGN_KEY);
      if (!activeId) return null;
      
      return this.getDesign(activeId);
    } catch (error) {
      console.error('Error getting active design:', error);
      return null;
    }
  }

  // Get all designs
  static getAllDesigns(): Record<string, CanvasDesign> {
    try {
      const stored = localStorage.getItem(this.STORAGE_KEY);
      if (!stored) return {};

      const parsed = JSON.parse(stored);
      
      // Migration check for old format
      return this.migrateOldFormat(parsed);
    } catch (error) {
      console.error('Error getting all designs:', error);
      return {};
    }
  }

  // Get design statistics
  static getStatistics() {
    const designs = this.getAllDesigns();
    const designArray = Object.values(designs);

    return {
      totalDesigns: designArray.length,
      totalNodes: designArray.reduce((sum, d) => sum + d.state.nodes.length, 0),
      totalEdges: designArray.reduce((sum, d) => sum + d.state.edges.length, 0),
      byJob: designArray.reduce((acc, d) => {
        acc[d.jobId] = (acc[d.jobId] || 0) + 1;
        return acc;
      }, {} as Record<string, number>)
    };
  }

  // Cleanup orphaned React Flow storage
  private static cleanupReactFlowStorage(designId: string): void {
    const keysToRemove: string[] = [];
    
    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      if (key && (
        key.includes(`reactflow-${designId}`) ||
        key.includes(`canvas-${designId}`) ||
        key.includes(`rf-${designId}`)
      )) {
        keysToRemove.push(key);
      }
    }

    keysToRemove.forEach(key => localStorage.removeItem(key));
  }

  // Sanitize nodes for storage (remove React Flow internal states)
  private static sanitizeNodes(nodes: Node[]): Node[] {
    return nodes.map(node => ({
      ...node,
      selected: false,
      dragging: false,
      // Remove any internal React Flow properties that shouldn't be persisted
    }));
  }

  // Sanitize edges for storage
  private static sanitizeEdges(edges: Edge[]): Edge[] {
    return edges.map(edge => ({
      ...edge,
      selected: false,
      // Remove any internal React Flow properties
    }));
  }

  // Migrate from old JobDesignState format
  private static migrateOldFormat(parsed: any): Record<string, CanvasDesign> {
    if (!parsed || typeof parsed !== 'object') return {};

    // Check if this is old JobDesignManager format
    const firstKey = Object.keys(parsed)[0];
    const firstItem = parsed[firstKey];

    if (firstItem && 'nodes' in firstItem && 'edges' in firstItem) {
      // Already in CanvasDesign format
      return parsed;
    }

    // Old format detected - migrate
    console.log('Migrating old design format...');
    const migrated: Record<string, CanvasDesign> = {};

    Object.entries(parsed).forEach(([jobId, oldDesign]: [string, any]) => {
      const designId = `migrated-${jobId}`;
      const timestamp = new Date().toISOString();

      migrated[designId] = {
        id: designId,
        name: oldDesign.name || `Migrated Design ${jobId}`,
        jobId,
        state: {
          id: designId,
          name: oldDesign.name || `Migrated Design ${jobId}`,
          jobId,
          nodes: oldDesign.nodes || [],
          edges: oldDesign.edges || [],
          viewport: oldDesign.viewport || { x: 0, y: 0, zoom: 1 },
          metadata: {
            version: this.VERSION,
            createdAt: oldDesign.createdAt || timestamp,
            lastModified: oldDesign.lastModified || timestamp,
            nodeCount: oldDesign.nodes?.length || 0,
            edgeCount: oldDesign.edges?.length || 0,
            canvasType: 'reactflow'
          },
          customData: {
            validationSummary: oldDesign.validationSummary,
            sqlGeneration: oldDesign.sqlGeneration,
            connections: oldDesign.connections
          }
        },
        tags: ['migrated'],
        lastAccessed: timestamp
      };
    });

    // Save migrated format
    localStorage.setItem(this.STORAGE_KEY, JSON.stringify(migrated));
    return migrated;
  }
}