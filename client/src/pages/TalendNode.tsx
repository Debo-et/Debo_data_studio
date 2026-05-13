// Updated TalendNode.tsx – Hit-zone separation: single-click label → rename, double-click → editor
import React, { memo, useRef, useEffect, useCallback, useState } from 'react';
import { Handle, Position, NodeProps, useUpdateNodeInternals, useReactFlow } from 'reactflow';
import { getComponentIcon, getCategoryColor } from './ComponentRegistry';
import { UnifiedCanvasNode, NodeStatus } from '../types/unified-pipeline.types';

type TalendNodeData = UnifiedCanvasNode;

const TalendNode = memo(({ data, selected, id }: NodeProps<TalendNodeData>) => {
  const { name, nodeType, status } = data;
  const componentKey = (data as any).componentKey;
  
  const textContainerRef = useRef<HTMLDivElement>(null);
  const updateNodeInternals = useUpdateNodeInternals();
  const { setNodes } = useReactFlow();
  const nodeId = id || data.metadata?.id;
  
  // Rename state
  const [isRenaming, setIsRenaming] = useState(false);
  const [renameValue, setRenameValue] = useState(name || '');
  const inputRef = useRef<HTMLInputElement>(null);
  
  // Timer for single-click rename (only used for label clicks)
  const clickTimerRef = useRef<NodeJS.Timeout | null>(null);
  
  const rawCategory = nodeType || 'transform';
  const category = rawCategory === 'transform' || rawCategory === 'process' ? 'processing' : rawCategory;
  const categoryColor = getCategoryColor(category === 'processing' ? 'transform' : category);
  const icon = getComponentIcon(componentKey);
  
  const getStatusColor = () => {
    switch (status) {
      case NodeStatus.COMPLETED: return '#10b981';
      case NodeStatus.ERROR: return '#ef4444';
      case NodeStatus.WARNING: return '#f59e0b';
      default: return categoryColor;
    }
  };

  const formatDisplayLabel = useCallback(() => {
    const safeName = name || '';
    if (category === 'processing') {
      const parts = safeName.split('_');
      if (parts.length >= 3) {
        const transformIndex = parts.indexOf('TRANSFORM');
        if (transformIndex !== -1) {
          return parts.filter((_, i) => i !== transformIndex).join('_');
        }
      }
    }
    return safeName;
  }, [name, category]);

  const displayLabel = formatDisplayLabel();

  // Update rename value when name changes externally
  useEffect(() => {
    setRenameValue(name || '');
  }, [name]);

  // Focus input when renaming starts
  useEffect(() => {
    if (isRenaming && inputRef.current) {
      inputRef.current.focus();
      inputRef.current.select();
    }
  }, [isRenaming]);

  // ResizeObserver for text container
  useEffect(() => {
    const textContainer = textContainerRef.current;
    if (!textContainer) return;
    const observer = new ResizeObserver(() => updateNodeInternals(nodeId));
    observer.observe(textContainer);
    return () => {
      observer.unobserve(textContainer);
      observer.disconnect();
    };
  }, [nodeId, updateNodeInternals]);

  // --- Label Click Handler (Rename trigger) ---
  const handleLabelClick = useCallback((e: React.MouseEvent) => {
    e.stopPropagation(); // Prevent node selection from firing
    
    // Clear any pending timer
    if (clickTimerRef.current) {
      clearTimeout(clickTimerRef.current);
      clickTimerRef.current = null;
    }
    
    // Set a timer to trigger rename after a short delay (to allow double-click detection)
    clickTimerRef.current = setTimeout(() => {
      setIsRenaming(true);
      clickTimerRef.current = null;
    }, 200);
  }, []);

  // --- Double Click Handler (Editor open) ---
  const handleDoubleClick = useCallback((e: React.MouseEvent) => {
    e.stopPropagation();
    
    // Cancel any pending rename timer
    if (clickTimerRef.current) {
      clearTimeout(clickTimerRef.current);
      clickTimerRef.current = null;
    }
    
    // Do not enter rename mode
    setIsRenaming(false);
    
    // Dispatch editor open event (existing behavior)
    if (componentKey === 'tMap') {
      window.dispatchEvent(new CustomEvent('canvas-tmap-double-click', {
        detail: { nodeId, nodeMetadata: data }
      }));
    } else {
      window.dispatchEvent(new CustomEvent('canvas-node-double-click', {
        detail: {
          nodeMetadata: data,
          componentMetadata: {
            id: nodeId,
            name: displayLabel,
            type: componentKey,
            componentKey,
            role: category === 'processing' ? 'TRANSFORM' : category.toUpperCase(),
            metadata: data.metadata
          }
        }
      }));
    }
  }, [componentKey, nodeId, data, displayLabel, category]);

  // --- Rename Handlers ---
  const handleRenameSubmit = useCallback(() => {
    const newName = renameValue.trim();
    if (newName && newName !== name) {
      setNodes((nds) =>
        nds.map((node) => {
          if (node.id === nodeId) {
            return {
              ...node,
              data: {
                ...node.data,
                name: newName,
                metadata: {
                  ...node.data.metadata,
                  updatedAt: new Date().toISOString(),
                },
              },
            };
          }
          return node;
        })
      );
    }
    setIsRenaming(false);
  }, [renameValue, name, nodeId, setNodes]);

  const handleRenameCancel = useCallback(() => {
    setRenameValue(name || '');
    setIsRenaming(false);
  }, [name]);

  const handleKeyDown = useCallback((e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      handleRenameSubmit();
    } else if (e.key === 'Escape') {
      e.preventDefault();
      handleRenameCancel();
    }
  }, [handleRenameSubmit, handleRenameCancel]);

  // Cleanup timer on unmount
  useEffect(() => {
    return () => {
      if (clickTimerRef.current) {
        clearTimeout(clickTimerRef.current);
      }
    };
  }, []);

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
      onDoubleClick={handleDoubleClick} // Double-click on whole node opens editor
      // No onClick on the container – React Flow selection is handled automatically
    >
      {/* Handles */}
      {category !== 'input' && (
        <Handle
          type="target"
          position={Position.Left}
          className="w-3 h-3 border-2 border-white"
          style={{ backgroundColor: categoryColor }}
        />
      )}
      {category !== 'output' && (
        <Handle
          type="source"
          position={Position.Right}
          className="w-3 h-3 border-2 border-white"
          style={{ backgroundColor: categoryColor }}
        />
      )}
      
      {/* Icon */}
      <div 
        className="w-5 h-5 rounded-full flex items-center justify-center mb-1 shadow-sm flex-shrink-0"
        style={{ background: `linear-gradient(135deg, ${categoryColor} 0%, ${categoryColor}CC 100%)` }}
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
      
      {/* Label Container – Clickable for rename */}
      <div 
        ref={textContainerRef}
        className="text-center w-full flex-grow-0 talend-node-label"
        onClick={handleLabelClick}
        onDoubleClick={handleDoubleClick} // Allow double-click on label to also open editor
        style={{
          fontFamily: 'Arial',
          fontSize: '13.34pt',
          fontStyle: 'normal',
          fontWeight: 'normal',
          color: '#000000',
          wordBreak: 'break-word',
          overflowWrap: 'break-word',
          lineHeight: '1.2',
          minWidth: '80px',
          cursor: 'text', // Visual affordance for editability
        }}
      >
        {isRenaming ? (
          <input
            ref={inputRef}
            type="text"
            value={renameValue}
            onChange={(e) => setRenameValue(e.target.value)}
            onBlur={handleRenameSubmit}
            onKeyDown={handleKeyDown}
            className="w-full px-1 py-0.5 text-center border border-blue-400 rounded focus:outline-none focus:ring-1 focus:ring-blue-400"
            style={{ fontSize: '13.34pt', fontFamily: 'Arial' }}
            onClick={(e) => e.stopPropagation()}
            onDoubleClick={(e) => e.stopPropagation()}
          />
        ) : (
          <div className="font-normal talend-node-label-text">
            {displayLabel}
          </div>
        )}
        
        {/* Status Indicator */}
        {status && status !== NodeStatus.IDLE && !isRenaming && (
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