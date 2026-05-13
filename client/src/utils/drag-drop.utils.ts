// src/utils/drag-drop.utils.ts

import { RepositoryNode } from '../types/types';
import { ReactFlowDragData, RepositoryDragData } from '../types/drag-drop.types';

// Debugging function for all drag events
export const logDragEvent = (eventName: string, data: any) => {
  const timestamp = new Date().toISOString().split('T')[1].split('.')[0];
  console.log(
    `%c🎯 [DRAG-EVENT] ${timestamp} ${eventName}`,
    'background: #f0f0f0; color: #333; padding: 2px 6px; border-radius: 3px;',
    data
  );
  
  const stack = new Error().stack;
  if (stack) {
    const stackLines = stack.split('\n').slice(2, 6);
    console.log(
      `%c📝 Call Stack:`,
      'background: #e8f4f8; color: #2c5282; padding: 2px 6px; border-radius: 3px;',
      stackLines.join('\n')
    );
  }
};

// FIXED: Update the function to properly handle type comparisons
export const isProcessingComponentNode = (node: RepositoryNode): boolean => {
  if (!node.metadata) {
    logDragEvent('isProcessingComponentNode - no metadata', {
      nodeName: node.name,
      nodeType: node.type,
      result: false
    });
    return false;
  }
  
  const processingTypes = [
    'tJoin', 'tFilterRow', 'tAggregateRow', 'tSortRow', 'tReplicate',
    'Join', 'FilterRow', 'AggregateRow', 'SortRow', 'Replicate',
    'tMap', 'Map', 'tJava', 'tSystem', 'tRunjob', 'tFlowMeter'
  ];
  
  // FIX: Use type assertion to handle the comparison properly
  const nodeType = String(node.metadata?.type || node.type || '');
  const nodeName = String(node.name || '');
  
  const result = processingTypes.some(type => 
    nodeType.includes(type) || nodeName.includes(type)
  );
  
  logDragEvent('isProcessingComponentNode', {
    nodeName: node.name,
    nodeType: nodeType,
    nodeMetadataType: node.metadata.type,
    processingTypesChecked: processingTypes,
    matches: processingTypes.filter(type => 
      nodeType.includes(type) || nodeName.includes(type)
    ),
    result: result
  });
  
  return result;
};

// FIXED: Update createReactFlowDragPayload function
export const createReactFlowDragPayload = (node: RepositoryNode): ReactFlowDragData => {
  const requiresPopup = isProcessingComponentNode(node);
  
  // DETERMINE CORRECT CATEGORY BASED ON NODE TYPE
  let category: 'input' | 'output' | 'processing' = 'processing';
  
  // FIX: Use String() to avoid type comparison issues
  const nodeType = String(node.metadata?.type || node.type || '');
  const nodeName = String(node.name || '');
  
  logDragEvent('createReactFlowDragPayload - category determination', {
    nodeName: nodeName,
    nodeType: nodeType,
    metadataType: node.metadata?.type,
    metadata: node.metadata
  });
  
  // FIX: Use string methods instead of direct type comparisons
  const nodeTypeLower = nodeType.toLowerCase();
  const nodeNameLower = nodeName.toLowerCase();
  
  if (nodeTypeLower.includes('input') || 
      nodeTypeLower.includes('source') ||
      nodeNameLower.includes('input')) {
    category = 'input';
    logDragEvent('Category determined: INPUT', { 
      reason: 'Metadata/name contains input/source' 
    });
  } else if (nodeTypeLower.includes('output') || 
             nodeTypeLower.includes('sink') ||
             nodeNameLower.includes('output')) {
    category = 'output';
    logDragEvent('Category determined: OUTPUT', { 
      reason: 'Metadata/name contains output/sink' 
    });
  } else {
    logDragEvent('Category determined: PROCESSING (default)', { 
      reason: 'No input/output indicators found' 
    });
  }
  
  // Ensure metadata has proper structure
  const metadata = {
    ...node.metadata,
    source: 'sidebar',
    repositoryNodeId: node.id,
    isRepositoryNode: true,
    description: node.metadata?.description || `Component: ${node.name}`,
    componentCategory: category,
    originalNodeType: node.type,
    originalNodeName: node.name,
    dragTimestamp: Date.now()
  };
  
  const payload: ReactFlowDragData = {
    type: 'reactflow',
    nodeType: node.metadata?.type || node.type || 'unknown',
    component: {
      id: node.id,
      name: node.name,
      type: category,
      metadata: metadata
    },
    metadata: metadata,
    source: 'sidebar',
    requiresPopup
  };
  
  logDragEvent('createReactFlowDragPayload - final payload', {
    nodeId: node.id,
    nodeName: node.name,
    category: category,
    requiresPopup: requiresPopup,
    payloadKeys: Object.keys(payload),
    componentStructure: payload.component ? {
      id: payload.component.id,
      name: payload.component.name,
      type: payload.component.type
    } : 'No component defined',
    fullPayload: payload
  });
  
  return payload;
};

export const createRepositoryDragPayload = (node: RepositoryNode): RepositoryDragData => {
  const metadata = {
    ...node.metadata,
    dragTimestamp: Date.now()
  };
  
  const payload: RepositoryDragData = {
    type: 'repository',
    nodeId: node.id,
    nodeName: node.name,
    nodeType: node.type,
    metadata: metadata,
    componentType: node.metadata?.type,
    source: 'sidebar',
    technology: node.metadata?.technology || node.metadata?.type || 'unknown'
  };
  
  logDragEvent('createRepositoryDragPayload', {
    nodeName: node.name,
    payload: payload
  });
  
  return payload;
};

export const setupDragEvent = (
  node: RepositoryNode, 
  event: React.DragEvent,
  onDragStart?: (node: RepositoryNode, event: React.DragEvent) => void
): void => {
  console.group('🚀 DRAG EVENT SETUP =======================');
  logDragEvent('setupDragEvent - START', {
    node: {
      id: node.id,
      name: node.name,
      type: node.type,
      metadataKeys: node.metadata ? Object.keys(node.metadata) : []
    },
    eventType: event.type,
    dragEvent: event
  });
  
  event.dataTransfer.clearData();
  logDragEvent('Cleared existing drag data', {
    previousTypes: Array.from(event.dataTransfer.types)
  });
  
  const reactFlowData = createReactFlowDragPayload(node);
  const repositoryData = createRepositoryDragPayload(node);
  
  const componentInfo = reactFlowData.component ? {
    type: reactFlowData.component.type,
    name: reactFlowData.component.name
  } : { type: 'undefined', name: 'undefined' };
  
  logDragEvent('Created drag payloads', {
    reactFlowData: {
      type: reactFlowData.type,
      nodeType: reactFlowData.nodeType,
      requiresPopup: reactFlowData.requiresPopup,
      component: componentInfo
    },
    repositoryData: {
      type: repositoryData.type,
      nodeType: repositoryData.nodeType
    }
  });
  
  try {
    const reactFlowString = JSON.stringify(reactFlowData);
    event.dataTransfer.setData('application/reactflow', reactFlowString);
    logDragEvent('Set "application/reactflow"', {
      dataLength: reactFlowString.length,
      preview: reactFlowString.substring(0, 150) + '...'
    });
    
    const repositoryString = JSON.stringify(repositoryData);
    event.dataTransfer.setData('application/repository-node', repositoryString);
    logDragEvent('Set "application/repository-node"', {
      dataLength: repositoryString.length
    });
    
    event.dataTransfer.setData('text/plain', node.name);
    logDragEvent('Set "text/plain"', { value: node.name });
    
    event.dataTransfer.setData('reactflow/node-type', reactFlowData.nodeType);
    logDragEvent('Set "reactflow/node-type"', { value: reactFlowData.nodeType });
    
    if (reactFlowData.requiresPopup) {
      event.dataTransfer.setData('sidebar-with-popup', 'true');
      logDragEvent('Set "sidebar-with-popup" flag', { value: 'true' });
    } else {
      logDragEvent('No "sidebar-with-popup" flag needed', { requiresPopup: false });
    }
    
    event.dataTransfer.effectAllowed = 'copy';
    logDragEvent('Set drag effect', { effect: 'copy' });
    
    const allTypes = Array.from(event.dataTransfer.types);
    logDragEvent('All dataTransfer types set', allTypes);
    
    allTypes.forEach(type => {
      try {
        const data = event.dataTransfer.getData(type);
        logDragEvent(`Verified data for "${type}"`, {
          length: data.length,
          preview: data.length > 100 ? data.substring(0, 100) + '...' : data
        });
      } catch (e) {
        logDragEvent(`Error reading data for "${type}"`, { error: e });
      }
    });
    
  } catch (error) {
    logDragEvent('ERROR setting drag data', {
      error: error instanceof Error ? error.message : 'Unknown error',
      stack: error instanceof Error ? error.stack : undefined
    });
    console.error('Failed to set drag data:', error);
  }
  
  if (onDragStart) {
    logDragEvent('Calling onDragStart callback', {});
    try {
      onDragStart(node, event);
    } catch (error) {
      logDragEvent('ERROR in onDragStart callback', { error });
    }
  }
  
  console.groupEnd();
};

export const debugDropEvent = (event: React.DragEvent, source: string) => {
  console.group(`📥 DROP EVENT DEBUG - ${source} =======================`);
  
  logDragEvent('Drop Event Details', {
    clientX: event.clientX,
    clientY: event.clientY,
    source: source,
    timestamp: new Date().toISOString()
  });
  
  const dataTransferTypes = Array.from(event.dataTransfer.types);
  logDragEvent('Available dataTransfer types', dataTransferTypes);
  
  dataTransferTypes.forEach(type => {
    try {
      const data = event.dataTransfer.getData(type);
      if (type.includes('application')) {
        try {
          const parsedData = JSON.parse(data);
          logDragEvent(`Data for "${type}"`, {
            length: data.length,
            isJSON: true,
            content: parsedData
          });
        } catch (e) {
          logDragEvent(`Data for "${type}" (JSON parse failed)`, {
            length: data.length,
            error: e instanceof Error ? e.message : 'Unknown',
            preview: data.substring(0, 200)
          });
        }
      } else {
        logDragEvent(`Data for "${type}"`, {
          length: data.length,
          isJSON: false,
          content: data
        });
      }
    } catch (e) {
      logDragEvent(`Error reading data for "${type}"`, {
        error: e instanceof Error ? e.message : 'Unknown error'
      });
    }
  });
  
  console.groupEnd();
};

export const createDragImage = (node: RepositoryNode, category: 'input' | 'output' | 'processing') => {
  const dragImage = document.createElement('div');
  
  const colors = {
    input: '#4f46e5',
    output: '#059669',
    processing: '#7c3aed'
  };
  
  const icons = {
    input: '→',
    output: '←',
    processing: '⚡'
  };
  
  dragImage.style.cssText = `
    position: absolute;
    top: -1000px;
    left: -1000px;
    background: ${colors[category]};
    color: white;
    padding: 8px 12px;
    border-radius: 8px;
    border: 2px solid white;
    box-shadow: 0 4px 12px rgba(0,0,0,0.2);
    font-size: 14px;
    font-weight: 600;
    z-index: 10000;
    display: flex;
    align-items: center;
    gap: 8px;
    min-width: 120px;
    max-width: 200px;
    backdrop-filter: blur(4px);
  `;
  
  dragImage.innerHTML = `
    <span style="font-size: 16px; font-weight: bold;">${icons[category]}</span>
    <span style="flex: 1; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;">${node.name}</span>
    <span style="font-size: 10px; opacity: 0.9; text-transform: uppercase;">${category}</span>
  `;
  
  return dragImage;
};

export const validateDragData = (event: React.DragEvent): boolean => {
  const types = Array.from(event.dataTransfer.types);
  const hasReactFlowData = types.includes('application/reactflow');
  const hasRepositoryData = types.includes('application/repository-node');
  
  logDragEvent('validateDragData', {
    hasReactFlowData,
    hasRepositoryData,
    allTypes: types,
    isValid: hasReactFlowData && hasRepositoryData
  });
  
  return hasReactFlowData && hasRepositoryData;
};

export const extractDragData = (event: React.DragEvent) => {
  const result: any = {};
  
  const types = Array.from(event.dataTransfer.types);
  types.forEach(type => {
    try {
      const data = event.dataTransfer.getData(type);
      if (type.includes('application')) {
        try {
          result[type] = JSON.parse(data);
        } catch {
          result[type] = data;
        }
      } else {
        result[type] = data;
      }
    } catch (e) {
      result[type] = `Error reading: ${e instanceof Error ? e.message : 'Unknown error'}`;
    }
  });
  
  logDragEvent('extractDragData', {
    types,
    data: result
  });
  
  return result;
};

export const getComponentFromDragData = (dragData: any) => {
  if (!dragData) {
    logDragEvent('getComponentFromDragData - no dragData', {});
    return null;
  }
  
  const component = 
    dragData.component || 
    dragData.nodeData?.component || 
    dragData.metadata?.component;
  
  if (component) {
    logDragEvent('getComponentFromDragData - found component', {
      id: component.id,
      name: component.name,
      type: component.type
    });
    return component;
  }
  
  logDragEvent('getComponentFromDragData - creating fallback component', {
    dragDataKeys: Object.keys(dragData)
  });
  
  return {
    id: dragData.nodeType || dragData.type || 'unknown',
    name: dragData.nodeName || dragData.nodeType || 'Unknown Component',
    type: 'processing' as const,
    metadata: dragData.metadata || {}
  };
};

export const createFallbackDragData = (node: RepositoryNode, rawData: any = {}) => {
  logDragEvent('createFallbackDragData', {
    nodeName: node.name,
    rawDataKeys: Object.keys(rawData)
  });
  
  const fallbackData = {
    type: 'reactflow',
    nodeType: node.metadata?.type || node.type || 'unknown',
    component: {
      id: node.id,
      name: node.name,
      type: 'processing' as const,
      metadata: {
        ...node.metadata,
        source: 'sidebar-fallback',
        repositoryNodeId: node.id,
        isFallback: true,
        dragTimestamp: Date.now()
      }
    },
    metadata: {
      ...node.metadata,
      source: 'sidebar-fallback',
      repositoryNodeId: node.id,
      isFallback: true
    },
    source: 'sidebar-fallback',
    requiresPopup: isProcessingComponentNode(node)
  };
  
  return fallbackData;
};