// Updated TalendNode.tsx - WITH DOUBLED DIMENSIONS, PROPER SCALING, and SAFE NAME HANDLING
import React, { memo, useRef, useEffect, useCallback } from 'react';
import { Handle, Position, NodeProps, useUpdateNodeInternals } from 'reactflow';
import { getComponentIcon, getCategoryColor } from './ComponentRegistry';
import { UnifiedCanvasNode, NodeStatus } from '../types/unified-pipeline.types';

// Use UnifiedCanvasNode as the node data type
type TalendNodeData = UnifiedCanvasNode;

const TalendNode = memo(({ data, selected, id }: NodeProps<TalendNodeData>) => {
  // Destructure fields from unified data
  const { name, nodeType, status } = data;
  // Safely access componentKey (may be added at runtime)
  const componentKey = (data as any).componentKey;
  
  const textContainerRef = useRef<HTMLDivElement>(null);
  const updateNodeInternals = useUpdateNodeInternals();
  const nodeId = id || data.metadata?.id;
  
  // Determine category from nodeType (input/output/transform/process) – map to 'processing' for transform/process
  const rawCategory = nodeType || 'transform';
  // Map to categories expected by getCategoryColor: 'input' | 'output' | 'processing'
  const category = rawCategory === 'transform' || rawCategory === 'process' ? 'processing' : rawCategory;
  
  const categoryColor = getCategoryColor(category);
  const icon = getComponentIcon(componentKey);
  
  // Map NodeStatus to color
  const getStatusColor = () => {
    switch (status) {
      case NodeStatus.COMPLETED:
        return '#10b981'; // success green
      case NodeStatus.ERROR:
        return '#ef4444'; // error red
      case NodeStatus.WARNING:
        return '#f59e0b'; // warning orange
      default:
        return categoryColor;
    }
  };

  // Function to format the display label – safe with undefined name
  const formatDisplayLabel = useCallback(() => {
    const safeName = name || '';
    // For transform components, remove the "_TRANSFORM_" part if present
    if (category === 'processing') {
      const parts = safeName.split('_');
      if (parts.length >= 3) {
        const transformIndex = parts.indexOf('TRANSFORM');
        if (transformIndex !== -1) {
          const filteredParts = parts.filter((_part, index) => 
            index !== transformIndex
          );
          return filteredParts.join('_');
        }
      }
    }
    return safeName;
  }, [name, category]);

  // Setup ResizeObserver for text container
  useEffect(() => {
    const textContainer = textContainerRef.current;
    if (!textContainer) return;

    const observer = new ResizeObserver(() => {
      updateNodeInternals(nodeId);
    });

    observer.observe(textContainer);

    return () => {
      if (textContainer) {
        observer.unobserve(textContainer);
      }
      observer.disconnect();
    };
  }, [nodeId, updateNodeInternals]);

  const displayLabel = formatDisplayLabel();

  return (
    <div
      className={`
        talend-node 
        relative 
        border-2 
        rounded-lg 
        flex 
        flex-col 
        items-center 
        justify-start
        transition-all 
        duration-200
        ${selected ? 'ring-2 ring-blue-400 shadow-lg' : 'shadow-md'}
        talend-node-role-${category}
      `}
      style={{
        borderColor: getStatusColor(),
        backgroundColor: `${categoryColor}08`,
        background: `linear-gradient(135deg, ${categoryColor}08 0%, ${categoryColor}03 100%)`,
        width: '100%',
        height: '100%',
        padding: '12px 8px',
        minWidth: '120px',
        minHeight: '80px',
      }}
      data-id={nodeId}
      data-role={category}
      onDoubleClick={(e) => {
        e.stopPropagation();
        
        if (componentKey === 'tMap') {
          const event = new CustomEvent('canvas-tmap-double-click', {
            detail: {
              nodeId: nodeId,
              nodeMetadata: data,
            }
          });
          window.dispatchEvent(event);
        } else {
          const event = new CustomEvent('canvas-node-double-click', {
            detail: {
              nodeMetadata: data,
              componentMetadata: {
                id: nodeId,
                name: displayLabel,
                type: componentKey,
                componentKey: componentKey,
                role: category === 'processing' ? 'TRANSFORM' : category.toUpperCase(),
                metadata: data.metadata
              }
            }
          });
          window.dispatchEvent(event);
        }
      }}
    >
      {/* Input Handle – shown if not input node */}
      {category !== 'input' && (
        <Handle
          type="target"
          position={Position.Left}
          className="w-3 h-3 border-2 border-white"
          style={{ backgroundColor: categoryColor }}
        />
      )}
      
      {/* Output Handle – shown if not output node */}
      {category !== 'output' && (
        <Handle
          type="source"
          position={Position.Right}
          className="w-3 h-3 border-2 border-white"
          style={{ backgroundColor: categoryColor }}
        />
      )}
      
      {/* Icon - DOUBLED SIZE */}
      <div 
        className="w-5 h-5 rounded-full flex items-center justify-center mb-1 shadow-sm flex-shrink-0"
        style={{ 
          background: `linear-gradient(135deg, ${categoryColor} 0%, ${categoryColor}CC 100%)` 
        }}
      >
        <div className="text-white" style={{ fontSize: '1rem' }}>
          {React.isValidElement(icon) ? 
            React.cloneElement(icon as React.ReactElement<any>, { 
              className: 'w-2.5 h-2.5',
              style: { width: '2.5rem', height: '2.5rem' } 
            }) : 
            icon}
        </div>
      </div>
      
      {/* Text Container - WITH DOUBLED FONT SIZE */}
      <div 
        ref={textContainerRef}
        className="text-center w-full flex-grow-0 talend-node-label"
        style={{
          fontFamily: 'Arial',
          fontSize: '13.34pt',
          fontStyle: 'normal',
          fontWeight: 'normal',
          color: '#000000',
          wordBreak: 'break-word',
          overflowWrap: 'break-word',
          lineHeight: '1.2',
          minWidth: '80px'
        }}
      >
        <div className="font-normal talend-node-label-text">
          {displayLabel}
        </div>
        
        {/* Status Indicator - INCREASED SIZE */}
        {status && status !== NodeStatus.IDLE && (
          <div className="flex justify-center mt-0.5">
            <div 
              className="w-1 h-1 rounded-full"
              style={{ backgroundColor: getStatusColor() }}
            />
          </div>
        )}
      </div>
    </div>
  );
});

TalendNode.displayName = 'TalendNode';

export default TalendNode;