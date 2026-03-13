// src/components/canvas/CanvasEventsManager.tsx - FIXED VERSION WITH WORKING DRAG-AND-DROP
import { useCallback, Dispatch, SetStateAction } from 'react';
import { Node, Edge, Connection, OnConnect, Viewport, useReactFlow } from 'reactflow';
import { useAppDispatch } from '../hooks';

import {
  canvasUtils,
  CanvasNodeType,
  EnhancedCanvasConnection,
  ComponentPort
} from './CanvasUtils';

import { ValidationLevel } from '../validation';

export interface CanvasEventHandlers {
  onDragOver: (event: React.DragEvent) => void;
  onDragLeave: (event: React.DragEvent) => void;
  onDrop: (event: React.DragEvent, flowPosition?: { x: number; y: number }) => void;
  onConnect: OnConnect;
  onViewportChange: (viewport: Viewport) => void;
  onSelectionChange: (params: { nodes: Node<any>[]; edges: Edge[] }) => void;
  onNodeDragStart: (event: React.MouseEvent, node: Node<any>) => void;
  onNodeDrag: (event: React.MouseEvent, node: Node<any>) => void;
  onNodeDragStop: (event: React.MouseEvent, node: Node<any>) => void;
  handleEdgeClick: (event: React.MouseEvent, edge: Edge) => void;
  handleNodeSelection: (selectedNodes: Node<any>[]) => void;
  handleConnectionCancel: () => void;
  handleConnectionDelete: (connectionId: string) => void;
  handleCloseMapEditor: () => void;
  handleCloseMatchGroupWizard: () => void;
  handleComponentSelection: (category: 'input' | 'output') => void;
  handleCancelComponent: () => void;
  validateAllConnections: () => void;
  fixValidationIssues: () => void;
  // React Flow specific handlers
  onReactFlowDragOver: (event: React.DragEvent) => void;
  onReactFlowDragLeave: (event: React.DragEvent) => void;
  onReactFlowDrop: (event: React.DragEvent) => void;
}

export interface UseCanvasEventsProps {
  state: any;
  setState: any;
  nodes: Node<any>[];
  edges: Edge[];
  onNodesChange: any;
  onEdgesChange: any;
  setNodes: Dispatch<SetStateAction<Node<any>[]>>;
  convertToReactFlowEdge: (connection: EnhancedCanvasConnection) => Edge;
  convertToReactFlowNodeWithMetadata: (node: CanvasNodeType, metadata?: any) => Node<any>;
  generateSQLForMapNode: (node: CanvasNodeType) => Promise<void>;
  handleNodeUpdate: (nodeId: string, updates: any) => void;
  openSQLPreview: (nodeId: string) => void;
  regenerateSQL: (nodeId: string) => void;
  dispatch: ReturnType<typeof useAppDispatch>;
  onJobUpdate?: (updates: any) => void;
  calculateConnectionCountForNode: (nodeId: string) => number;
  normalizeExistingNodeNames: () => void;
}

// Helper function to generate ports based on component type and category
const generatePortsForType = (
  componentType: string,
  category: 'input' | 'output' | 'processing'
): ComponentPort[] => {
  const ports: ComponentPort[] = [];
  
  // Determine if component is a source (input), sink (output), or processor
  const isSource = category === 'input';
  const isSink = category === 'output';
  const isProcessor = category === 'processing' || 
                     componentType.includes('tMap') || 
                     componentType.includes('tJoin') ||
                     componentType.includes('tFilter');
  
  // Input ports (left side)
  if (isProcessor || isSink) {
    ports.push({
      id: `input-${Date.now()}-${Math.random().toString(36).substr(2, 4)}`,
      type: 'input',
      side: 'left',
      position: 50,
      label: 'In',
      maxConnections: isProcessor ? 999 : 5, // Processors can have multiple inputs
      required: isSink,
      dataType: 'any'
    });
  }
  
  // Output ports (right side)
  if (isProcessor || isSource) {
    ports.push({
      id: `output-${Date.now()}-${Math.random().toString(36).substr(2, 4)}`,
      type: 'output',
      side: 'right',
      position: 50,
      label: 'Out',
      maxConnections: isProcessor ? 999 : 5, // Processors can have multiple outputs
      required: isSource,
      dataType: 'any'
    });
  }
  
  // Special ports for certain components
  if (componentType.includes('tJoin')) {
    // Join has two inputs
    ports.push({
      id: `input-2-${Date.now()}-${Math.random().toString(36).substr(2, 4)}`,
      type: 'input',
      side: 'left',
      position: 30,
      label: 'In 2',
      maxConnections: 1,
      required: true,
      dataType: 'any'
    });
  }
  
  if (componentType.includes('tReplicate')) {
    // Replicate has multiple outputs
    ports.push({
      id: `output-2-${Date.now()}-${Math.random().toString(36).substr(2, 4)}`,
      type: 'output',
      side: 'right',
      position: 30,
      label: 'Out 2',
      maxConnections: 1,
      required: false,
      dataType: 'any'
    });
    ports.push({
      id: `output-3-${Date.now()}-${Math.random().toString(36).substr(2, 4)}`,
      type: 'output',
      side: 'right',
      position: 70,
      label: 'Out 3',
      maxConnections: 1,
      required: false,
      dataType: 'any'
    });
  }
  
  return ports;
};

export const useCanvasEvents = ({
  state,
  setState,
  nodes,
  onEdgesChange,
  setNodes,
  convertToReactFlowEdge,
  convertToReactFlowNodeWithMetadata,
  onJobUpdate,
  normalizeExistingNodeNames
}: UseCanvasEventsProps): CanvasEventHandlers => {
  const { screenToFlowPosition } = useReactFlow();

  // Update the createNodeFromDragData function in CanvasEventsManager.tsx

const createNodeFromDragData = useCallback((
  dragData: any,
  flowPosition: { x: number; y: number },
  category?: 'input' | 'output' | 'processing'
): CanvasNodeType | null => {
  try {
    console.log('🛠️ Creating node from drag data:', dragData);
    
    // Extract component info - handle different data structures with null checks
    let componentType = dragData?.componentType || 
                       dragData?.type || 
                       dragData?.component?.type || 
                       dragData?.nodeType || 
                       dragData?.name || 
                       'tFilterRow';
    
    // If type is 'reactflow' or 'component', look deeper
    if (componentType === 'reactflow' || componentType === 'component') {
      componentType = dragData?.component?.id || 
                     dragData?.component?.type ||
                     dragData?.nodeType ||
                     dragData?.name?.replace(/^t/, 't') || 
                     'tFilterRow';
    }
    
    // Ensure componentType is defined before checking startsWith
    if (!componentType || typeof componentType !== 'string') {
      console.warn('⚠️ Invalid component type:', componentType, 'defaulting to tFilterRow');
      componentType = 'tFilterRow';
    }
    
    // Ensure it starts with 't' if it's a component type
    if (componentType && !componentType.startsWith('t') && componentType.length > 1) {
      componentType = 't' + componentType.charAt(0).toUpperCase() + componentType.slice(1);
    }
    
    const displayName = dragData?.displayName || 
                       dragData?.name || 
                       dragData?.component?.name || 
                       componentType;
    
    // Determine category
    let nodeCategory = category || 'processing';
    if (!category) {
      if (componentType.includes('input') || componentType.includes('source')) {
        nodeCategory = 'input';
      } else if (componentType.includes('output') || componentType.includes('sink')) {
        nodeCategory = 'output';
      } else {
        nodeCategory = 'processing';
      }
    }
    
    // Generate unique node name
    const existingNodes = nodes.map(n => n.data?.nodeData).filter(Boolean) as CanvasNodeType[];
    const baseName = componentType ? componentType.replace(/^t/, '').toLowerCase() : 'component';
    const uniqueName = canvasUtils.generateSnakeCaseName(baseName, existingNodes, {
      baseName: baseName,
      metadata: dragData?.metadata
    });
    
    // Generate ports based on type and category
    const connectionPorts = generatePortsForType(componentType, nodeCategory);
    
    // Create the node
    const newNode: CanvasNodeType = {
      id: `node-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
      name: uniqueName,
      type: componentType,
      nodeType: componentType,
      componentType: componentType,
      componentCategory: nodeCategory,
      technology: componentType,
      position: flowPosition,
      size: { width: 146, height: 93 },
      connectionPorts,
      metadata: {
        ...dragData?.metadata,
        dragSource: dragData?.source || dragData?.metadata?.source || 'component-palette',
        originalDragData: dragData,
        componentData: dragData?.component || dragData?.nodeData || dragData,
        description: dragData?.description || dragData?.metadata?.description || `A ${displayName} component`,
        createdAt: new Date().toISOString(),
        version: '1.0'
      },
      status: 'active',
      draggable: true,
      droppable: true,
      dragType: 'node'
    };
    
    console.log('✅ Node created successfully:', {
      id: newNode.id,
      name: newNode.name,
      type: newNode.type,
      category: newNode.componentCategory,
      position: flowPosition,
      ports: connectionPorts.length
    });
    
    return newNode;
  } catch (error) {
    console.error('❌ Failed to create node from drag data:', error);
    console.error('Drag data that failed:', dragData);
    return null;
  }
}, [nodes]);

  // React Flow Drag and Drop Handlers
  const onReactFlowDragOver = useCallback((event: React.DragEvent) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
    setState((prev: any) => ({ ...prev, isDragOver: true }));
  }, [setState]);

  const onReactFlowDragLeave = useCallback(() => {
    setState((prev: any) => ({ ...prev, isDragOver: false }));
  }, [setState]);

  const onReactFlowDrop = useCallback((event: React.DragEvent) => {
    event.preventDefault();
    setState((prev: any) => ({ ...prev, isDragOver: false }));
    
    console.log('🎯 React Flow Drop Event Triggered');
    console.log('Event position:', { x: event.clientX, y: event.clientY });
    console.log('DataTransfer types:', Array.from(event.dataTransfer.types));
    
    // Get the node type from React Flow's expected data format
    const reactFlowData = event.dataTransfer.getData('application/reactflow');
    let nodeType = '';
    let dragData: any = null;
    
    if (reactFlowData) {
      try {
        dragData = JSON.parse(reactFlowData);
        nodeType = dragData.nodeType || dragData.component?.id || dragData.type || 'tFilterRow';
        console.log('📥 Parsed React Flow data:', dragData);
      } catch (error) {
        console.error('Error parsing React Flow drop data:', error);
      }
    }
    
    // Fallback to alternative data formats
    if (!nodeType) {
      nodeType = event.dataTransfer.getData('reactflow/node-type') || 
                 event.dataTransfer.getData('text/plain') || 
                 'tFilterRow';
    }
    
    // If we have text/plain data but no proper dragData, create a simple one
    if (!dragData && nodeType) {
      dragData = {
        type: 'reactflow',
        nodeType: nodeType,
        component: {
          id: nodeType,
          name: nodeType,
          type: 'processing',
          metadata: {
            description: `Component: ${nodeType}`,
            componentType: 'palette-component',
            source: 'right-panel'
          }
        }
      };
    }
    
    // Check if we need to show the component selection popup
    const sidebarPopupMarker = event.dataTransfer.getData('sidebar-with-popup');
    if (sidebarPopupMarker === 'true' && dragData) {
      console.log('🔄 Component requires selection popup');
      
      const targetFlowPosition = screenToFlowPosition({
        x: event.clientX,
        y: event.clientY
      });
      
      console.log('🔄 Showing component selection popup at:', targetFlowPosition);
      
      setState((prev: any) => ({
        ...prev,
        pendingDrop: {
          data: dragData,
          position: targetFlowPosition,
          defaultName: dragData.component?.name || nodeType,
          metadata: dragData.component?.metadata || {},
          technology: nodeType
        }
      }));
      
      event.dataTransfer.clearData();
      return;
    }
    
    // Process direct node creation
    if (nodeType && dragData) {
      console.log('🎯 Creating node from React Flow drop:', nodeType);
      
      const targetFlowPosition = screenToFlowPosition({
        x: event.clientX,
        y: event.clientY
      });
      
      // Create the node
      const newNode = createNodeFromDragData(dragData, targetFlowPosition);
      
      if (newNode) {
        // Convert to React Flow node and add to canvas
        const reactFlowNode = convertToReactFlowNodeWithMetadata(newNode);
        setNodes((nds) => [...nds, reactFlowNode]);
        
        // Visual feedback
        setTimeout(() => {
          const nodeElement = document.querySelector(`[data-node-id="${newNode.id}"]`);
          if (nodeElement) {
            nodeElement.classList.add('animate-pulse', 'ring-2', 'ring-green-500');
            setTimeout(() => {
              nodeElement.classList.remove('animate-pulse', 'ring-2', 'ring-green-500');
            }, 1000);
          }
        }, 100);
        
        // Normalize names after a short delay
        setTimeout(() => {
          normalizeExistingNodeNames();
        }, 500);
        
        // Update job if needed
        if (onJobUpdate) {
          onJobUpdate({
            nodes: [...nodes, reactFlowNode],
            lastModified: new Date().toISOString()
          });
        }
        
        console.log('✅ Node added via React Flow drop:', {
          id: newNode.id,
          name: newNode.name,
          type: newNode.type,
          position: targetFlowPosition
        });
      }
    }
    
    event.dataTransfer.clearData();
  }, [screenToFlowPosition, createNodeFromDragData, convertToReactFlowNodeWithMetadata, setNodes, normalizeExistingNodeNames, nodes, onJobUpdate, setState]);

  // Legacy Drag and Drop Events (for compatibility)
  const onDragOver = useCallback((event: React.DragEvent) => {
    event.preventDefault();
    event.stopPropagation();
    event.dataTransfer.dropEffect = 'copy';
    
    setState((prev: any) => ({ ...prev, isDragOver: true }));
  }, [setState]);

  const onDragLeave = useCallback((event: React.DragEvent) => {
    event.preventDefault();
    event.stopPropagation();
    
    setState((prev: any) => ({ ...prev, isDragOver: false }));
  }, [setState]);

  const onDrop = useCallback((
    event: React.DragEvent,
    flowPosition?: { x: number; y: number }
  ) => {
    event.preventDefault();
    event.stopPropagation();
    
    console.log('=== CANVAS DROP EVENT ===');
    console.log('Drop position (screen):', { x: event.clientX, y: event.clientY });
    console.log('Flow position (provided):', flowPosition);
    console.log('DataTransfer types:', Array.from(event.dataTransfer.types));
    
    setState((prev: any) => ({ ...prev, isDragOver: false }));
    
    let dragData = null;
    
    // Log all available data for debugging
    Array.from(event.dataTransfer.types).forEach(type => {
      try {
        const data = event.dataTransfer.getData(type);
        console.log(`📤 Type "${type}":`, data.length > 100 ? data.substring(0, 100) + '...' : data);
      } catch (e) {
        console.log(`📤 Type "${type}": [cannot read]`);
      }
    });
    
    // Try to get data from React Flow format first
    const reactFlowData = event.dataTransfer.getData('application/reactflow');
    if (reactFlowData) {
      try {
        dragData = JSON.parse(reactFlowData);
        console.log('📤 Parsed React Flow data:', dragData);
      } catch (error) {
        console.error('Error parsing React Flow data:', error);
      }
    }
    
    // If no React Flow data, try JSON
    if (!dragData) {
      const jsonData = event.dataTransfer.getData('application/json');
      if (jsonData) {
        try {
          dragData = JSON.parse(jsonData);
          console.log('📤 Parsed JSON data:', dragData);
        } catch (error) {
          console.error('Error parsing JSON data:', error);
        }
      }
    }
    
    // If still no data, try text/plain
    if (!dragData) {
      const textData = event.dataTransfer.getData('text/plain');
      if (textData) {
        try {
          // Try to parse as JSON first
          dragData = JSON.parse(textData);
          console.log('📤 Parsed text as JSON:', dragData);
        } catch {
          // If not JSON, create a simple component from text
          dragData = { 
            type: 'component',
            name: textData,
            source: 'text-plain',
            displayName: textData,
            description: `Component: ${textData}`,
            metadata: { source: 'text-plain' }
          };
          console.log('📤 Created component from text data:', dragData);
        }
      }
    }
    
    // Check for component-type marker
    const componentTypeMarker = event.dataTransfer.getData('component-type');
    if (componentTypeMarker && !dragData?.componentType) {
      dragData = dragData || {};
      dragData.componentType = componentTypeMarker;
    }
    
    if (!dragData) {
      console.warn('❌ No valid drag data found in any format');
      console.warn('Available types:', Array.from(event.dataTransfer.types));
      return;
    }
    
    // Check if we need to show the component selection popup
    const sidebarPopupMarker = event.dataTransfer.getData('sidebar-with-popup');
    if (sidebarPopupMarker === 'true') {
      console.log('🔄 Component requires selection popup');
      
      let canonicalType = dragData.componentType || dragData.type || 'unknown';
      let displayName = dragData.displayName || dragData.name || 'Component';
      
      // Convert screen coordinates to flow coordinates if not provided
      const targetFlowPosition = flowPosition || screenToFlowPosition({
        x: event.clientX,
        y: event.clientY
      });
      
      console.log('🔄 Showing component selection popup at:', targetFlowPosition);
      
      setState((prev: any) => ({
        ...prev,
        pendingDrop: {
          data: dragData,
          position: targetFlowPosition, // Store in flow coordinates
          defaultName: displayName,
          metadata: dragData.metadata || 
                   dragData.nodeData?.metadata || 
                   dragData.component?.metadata || 
                   {},
          technology: canonicalType || 'unknown'
        }
      }));
      
      event.dataTransfer.clearData();
      return;
    }
    
    // Process direct node creation (no popup needed)
    console.log('🎯 Creating node directly from drag data:', dragData);
    
    // Convert screen coordinates to flow coordinates if not provided
    const targetFlowPosition = flowPosition || screenToFlowPosition({
      x: event.clientX,
      y: event.clientY
    });
    
    console.log('🎯 Target flow position:', targetFlowPosition);
    
    // Create the node
    const newNode = createNodeFromDragData(dragData, targetFlowPosition);
    
    if (newNode) {
      // Convert to React Flow node and add to canvas
      const reactFlowNode = convertToReactFlowNodeWithMetadata(newNode);
      setNodes((nds) => [...nds, reactFlowNode]);
      
      // Clear drag data
      event.dataTransfer.clearData();
      
      // Visual feedback
      setTimeout(() => {
        const nodeElement = document.querySelector(`[data-node-id="${newNode.id}"]`);
        if (nodeElement) {
          nodeElement.classList.add('animate-pulse', 'ring-2', 'ring-green-500');
          setTimeout(() => {
            nodeElement.classList.remove('animate-pulse', 'ring-2', 'ring-green-500');
          }, 1000);
        }
      }, 100);
      
      // Normalize names after a short delay
      setTimeout(() => {
        normalizeExistingNodeNames();
      }, 500);
      
      // Update job if needed
      if (onJobUpdate) {
        onJobUpdate({
          nodes: [...nodes, reactFlowNode],
          lastModified: new Date().toISOString()
        });
      }
      
      // Log success
      console.log('✅ Node added to canvas:', {
        id: newNode.id,
        name: newNode.name,
        type: newNode.type,
        position: targetFlowPosition
      });
    } else {
      console.error('❌ Failed to create node from drag data');
    }
  }, [screenToFlowPosition, createNodeFromDragData, convertToReactFlowNodeWithMetadata, setNodes, normalizeExistingNodeNames, nodes, onJobUpdate, setState]);

  // React Flow Connection Event
  const onConnect: OnConnect = useCallback(
    (connection: Connection) => {
      if (!connection.source || !connection.target) return;
      
      console.log('🔗 Creating connection:', connection);
      
      const newConnection: EnhancedCanvasConnection = {
        id: `conn-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
        sourceNodeId: connection.source || '',
        sourcePortId: connection.sourceHandle || '',
        targetNodeId: connection.target || '',
        targetPortId: connection.targetHandle || '',
        status: 'valid',
        metadata: {
          createdAt: new Date().toISOString(),
          createdBy: 'reactflow',
          version: '1.0',
          validated: true
        }
      };
      
      setState((prev: any) => ({
        ...prev,
        connections: [...prev.connections, newConnection]
      }));
      
      // Add the new edge to React Flow
      const newEdge = convertToReactFlowEdge(newConnection);
      onEdgesChange([{ type: 'add', item: newEdge }]);
      
      if (onJobUpdate) {
        onJobUpdate({
          connections: [...state.connections, newConnection],
          lastModified: new Date().toISOString()
        });
      }
      
      // Visual feedback
      setTimeout(() => {
        const edgeElement = document.querySelector(`[data-edge-id="${newConnection.id}"]`);
        if (edgeElement) {
          edgeElement.classList.add('animate-pulse');
          setTimeout(() => {
            edgeElement.classList.remove('animate-pulse');
          }, 500);
        }
      }, 100);
      
      console.log('✅ Connection created:', newConnection);
    },
    [state.connections, onJobUpdate, setState, convertToReactFlowEdge, onEdgesChange]
  );

  // Viewport Change
  const onViewportChange = useCallback((viewport: Viewport) => {
    setState((prev: any) => ({ ...prev, viewport }));
  }, [setState]);

  // Selection Change
  const onSelectionChange = useCallback((params: { nodes: Node<any>[]; edges: Edge[] }) => {
    if (params.nodes.length > 0) {
      const nodeId = params.nodes[0].id;
      setState((prev: any) => ({ ...prev, selectedNodeId: nodeId }));
      
      if (state.validationSummary) {
        const nodeValidation = canvasUtils.getValidationMessages(state.validationSummary, nodeId, 'node');
        if (nodeValidation.errors.length > 0 || nodeValidation.warnings.length > 0) {
          const validationEvent = new CustomEvent('validation-details', {
            detail: {
              elementId: nodeId,
              elementType: 'node',
              errors: nodeValidation.errors,
              warnings: nodeValidation.warnings
            }
          });
          window.dispatchEvent(validationEvent);
        }
      }
      
      const customEvent = new CustomEvent('node-selected', {
        detail: { nodeId }
      });
      window.dispatchEvent(customEvent);
      
      console.log('🎯 Node selected:', nodeId);
    } else {
      setState((prev: any) => ({ 
        ...prev, 
        selectedEdgeId: null,
        selectedNodeId: null 
      }));
    }
  }, [state.validationSummary, setState]);

  // Node Drag Events
  const onNodeDragStart = useCallback((_event: React.MouseEvent, node: Node<any>) => {
    console.log('🚀 Node drag started:', node.id);
    setState((prev: any) => ({
      ...prev,
      dragState: {
        draggedNodeId: node.id,
        dragStartPosition: node.position
      }
    }));
  }, [setState]);

  const onNodeDrag = useCallback((_event: React.MouseEvent) => {
    // Intentionally empty - we could update connection positions here if needed
  }, []);

  const onNodeDragStop = useCallback((_event: React.MouseEvent, node: Node<any>) => {
    console.log('🎯 Node drag stopped:', node.id, 'at', node.position);
    
    // Update the node position in our internal state
    setNodes(nds => nds.map(n => {
      if (n.id === node.id) {
        return {
          ...n,
          position: node.position,
          data: {
            ...n.data,
            nodeData: {
              ...n.data.nodeData,
              position: node.position,
              metadata: {
                ...n.data.nodeData.metadata,
                lastMoved: new Date().toISOString()
              }
            }
          }
        };
      }
      return n;
    }));
    
    setState((prev: any) => ({
      ...prev,
      dragState: {
        draggedNodeId: null,
        dragStartPosition: { x: 0, y: 0 }
      }
    }));
    
    if (onJobUpdate) {
      onJobUpdate({
        lastModified: new Date().toISOString()
      });
    }
  }, [onJobUpdate, setState, setNodes]);

  // Edge Click Handler
  const handleEdgeClick = useCallback((event: React.MouseEvent, edge: Edge) => {
    event.stopPropagation();
    
    // Select the edge
    setState((prev: any) => ({ ...prev, selectedEdgeId: edge.id }));
    
    if (state.validationSummary) {
      const edgeValidation = canvasUtils.getValidationMessages(state.validationSummary, edge.id, 'edge');
      if (edgeValidation.errors.length > 0 || edgeValidation.warnings.length > 0) {
        const validationEvent = new CustomEvent('validation-details', {
          detail: {
            elementId: edge.id,
            elementType: 'edge',
            errors: edgeValidation.errors,
            warnings: edgeValidation.warnings
          }
        });
        window.dispatchEvent(validationEvent);
      }
    }
    
    const customEvent = new CustomEvent('edge-selected', {
      detail: { 
        edgeId: edge.id,
        sourceNodeId: edge.source,
        targetNodeId: edge.target,
        sourceHandle: edge.sourceHandle,
        targetHandle: edge.targetHandle
      }
    });
    window.dispatchEvent(customEvent);
    
    console.log('🔗 Edge selected:', edge.id);
  }, [state.validationSummary, setState]);

  // Node Selection Handler
  const handleNodeSelection = useCallback((selectedNodes: Node<any>[]) => {
    onSelectionChange({ nodes: selectedNodes, edges: [] });
  }, [onSelectionChange]);

  // Connection Management
  const handleConnectionCancel = useCallback(() => {
    setState((prev: any) => ({
      ...prev,
      pendingConnection: null,
      snapState: {
        isSnapping: false,
        candidate: null,
        snapRadius: 30,
        visualFeedback: true
      }
    }));
    
    console.log('Connection cancelled');
  }, [setState]);

  const handleConnectionDelete = useCallback((connectionId: string) => {
    setState((prev: any) => ({
      ...prev,
      connections: prev.connections.filter((conn: EnhancedCanvasConnection) => conn.id !== connectionId),
      connectionInteraction: {
        ...prev.connectionInteraction,
        selectedConnections: new Set([...prev.connectionInteraction.selectedConnections].filter(id => id !== connectionId)),
        activeConnection: null
      }
    }));
    
    // Also remove from React Flow edges
    onEdgesChange([{ type: 'remove', id: connectionId }]);
    
    console.log('🗑️ Connection deleted:', connectionId);
  }, [setState, onEdgesChange]);

  // Modal Management
  const handleCloseMapEditor = useCallback(() => {
    document.body.classList.remove('map-editor-open');
    document.body.style.overflow = 'auto';
    
    setState((prev: any) => ({
      ...prev,
      showMapEditor: false,
      selectedNodeForMapEditor: null
    }));
    
    if (state.selectedNodeForMapEditor) {
      const nodeElement = document.querySelector(`[data-node-id="${state.selectedNodeForMapEditor.id}"]`);
      if (nodeElement) {
        (nodeElement as HTMLElement).focus();
      }
    }
  }, [state.selectedNodeForMapEditor, setState]);

  const handleCloseMatchGroupWizard = useCallback(() => {
    setState((prev: any) => ({
      ...prev,
      showMatchGroupWizard: false,
      selectedNodeForMatchGroupWizard: null
    }));
  }, [setState]);

  // Component Selection
  const handleComponentSelection = useCallback((category: 'input' | 'output') => {
    if (!state.pendingDrop) {
      console.warn('No pending drop data available');
      return;
    }

    console.log('🎯 Creating component from pending drop:', state.pendingDrop);
    
    // Create the node from pending drop data
    const newNode = createNodeFromDragData(state.pendingDrop.data, state.pendingDrop.position, category);
    
    if (newNode) {
      // Update the node name to reflect the selected category
      const updatedNode = {
        ...newNode,
        name: `${newNode.name}_${category}`,
        componentCategory: category
      };
      
      // Convert to React Flow node and add to canvas
      const reactFlowNode = convertToReactFlowNodeWithMetadata(updatedNode);
      setNodes((nds) => [...nds, reactFlowNode]);
      
      // Visual feedback
      setTimeout(() => {
        const nodeElement = document.querySelector(`[data-node-id="${updatedNode.id}"]`);
        if (nodeElement) {
          nodeElement.classList.add('animate-pulse', 'ring-2', 'ring-blue-500');
          setTimeout(() => {
            nodeElement.classList.remove('animate-pulse', 'ring-2', 'ring-blue-500');
          }, 1000);
        }
      }, 100);
      
      // Normalize names after a short delay
      setTimeout(() => {
        normalizeExistingNodeNames();
      }, 500);
      
      // Update job if needed
      if (onJobUpdate) {
        onJobUpdate({
          nodes: [...nodes, reactFlowNode],
          lastModified: new Date().toISOString()
        });
      }
    }
    
    // Clear pending drop state
    setState((prev: any) => ({ ...prev, pendingDrop: null }));
  }, [state.pendingDrop, createNodeFromDragData, convertToReactFlowNodeWithMetadata, setNodes, normalizeExistingNodeNames, nodes, onJobUpdate, setState]);

  const handleCancelComponent = useCallback(() => {
    console.log('❌ Component creation cancelled');
    setState((prev: any) => ({ ...prev, pendingDrop: null }));
  }, [setState]);

  // Validation Actions
  const validateAllConnections = useCallback(() => {
    if (!state.validationSummary) {
      console.warn('No validation summary available');
      return;
    }
    
    if (state.validationSummary.isValid) {
      alert(`✅ All connections are valid!\nErrors: ${state.validationSummary.counts.errors}, Warnings: ${state.validationSummary.counts.warnings}`);
    } else {
      const errorMessages = state.validationSummary.results
        .filter((r: any) => r.level === ValidationLevel.ERROR)
        .map((r: any) => `• ${r.message}`)
        .join('\n');
      
      alert(`❌ Validation failed with ${state.validationSummary.counts.errors} error(s):\n\n${errorMessages}`);
    }
  }, [state.validationSummary]);

  const fixValidationIssues = useCallback(() => {
    if (!state.validationSummary) return;
    
    const issues = state.validationSummary.results.filter((r: any) => 
      r.level === ValidationLevel.ERROR || r.level === ValidationLevel.WARNING
    );
    
    if (issues.length === 0) {
      alert('✅ No validation issues to fix!');
      return;
    }
    
    // Auto-fix simple issues
    let fixedCount = 0;
    
    issues.forEach((issue: any) => {
      if (issue.code === 'CYCLE_DETECTED' && issue.edgeIds.length > 0) {
        handleConnectionDelete(issue.edgeIds[0]);
        fixedCount++;
      }
    });
    
    if (fixedCount > 0) {
      alert(`✅ Fixed ${fixedCount} validation issue(s)`);
    } else {
      alert('⚠️ No auto-fixable issues found. Please fix manually.');
    }
  }, [state.validationSummary, handleConnectionDelete]);

  return {
    onDragOver,
    onDragLeave,
    onDrop,
    onConnect,
    onViewportChange,
    onSelectionChange,
    onNodeDragStart,
    onNodeDrag,
    onNodeDragStop,
    handleEdgeClick,
    handleNodeSelection,
    handleConnectionCancel,
    handleConnectionDelete,
    handleCloseMapEditor,
    handleCloseMatchGroupWizard,
    handleComponentSelection,
    handleCancelComponent,
    validateAllConnections,
    fixValidationIssues,
    // React Flow specific handlers
    onReactFlowDragOver,
    onReactFlowDragLeave,
    onReactFlowDrop
  };
};