// FILE: src/flow/ValidationNode.tsx
/**
 * Custom React Flow node with validation support
 * OMITTED:
 * - Original complex port rendering with dynamic validation states
 * - Custom drag-and-drop behavior (uses React Flow default)
 */

import React, { useMemo } from 'react';
import { Handle, Position, NodeProps } from 'reactflow';
import { useValidation } from './ValidationProvider';
import { ValidationNode as ValidationNodeType } from './flow-types';

// CustomNodeProps that extends NodeProps but removes the type conflict
export interface CustomNodeProps extends Omit<NodeProps<ValidationNodeType['data']>, 'type'> {
  type?: string; // Make type optional to avoid conflict
  ports?: Array<{
    id: string;
    type: 'source' | 'target';
    dataType?: string;
    label?: string;
    position: Position;
    maxConnections?: number;
  }>;
}

export const ValidationNode: React.FC<CustomNodeProps> = ({
  data,
  id,
  type,
  selected,
  ports = []
}) => {
  const { validateNode } = useValidation();
  const [validationState, setValidationState] = React.useState<{
    isValid: boolean;
    errors: string[];
    warnings: string[];
  }>({ isValid: true, errors: [], warnings: [] });

  // Determine node styling based on validation state
  const nodeClasses = useMemo(() => {
    const baseClasses = 'rounded-lg border-2 p-4 min-w-[180px] transition-all duration-200';
    const selectedClasses = selected ? 'ring-2 ring-blue-400 ring-offset-2' : '';
    
    if (!validationState.isValid) {
      return `${baseClasses} border-red-500 bg-red-50 ${selectedClasses}`;
    }
    if (validationState.warnings.length > 0) {
      return `${baseClasses} border-yellow-500 bg-yellow-50 ${selectedClasses}`;
    }
    return `${baseClasses} border-gray-300 bg-white ${selectedClasses}`;
  }, [validationState, selected]);

  // Validate node on mount and when dependencies change
  React.useEffect(() => {
    // This would be called from parent component with actual node/edge data
    // For now, we'll just set default state
    setValidationState({ isValid: true, errors: [], warnings: [] });
  }, [id, validateNode]);

  // Group ports by position for better layout
  const sourcePorts = ports.filter(p => p.type === 'source');
  const targetPorts = ports.filter(p => p.type === 'target');

  return (
    <div className={nodeClasses}>
      {/* Node header */}
      <div className="flex items-center justify-between mb-2">
        <div className="font-semibold text-gray-800">{data.name}</div>
        <div className="text-xs px-2 py-1 rounded bg-gray-100 text-gray-600">
          {type || data.componentCategory || 'Node'}
        </div>
      </div>
      
      {/* Node content */}
      <div className="text-sm text-gray-600 mb-3">
        {data.label || data.componentCategory || 'No description'}
      </div>
      
      {/* Validation status indicator */}
      {(!validationState.isValid || validationState.warnings.length > 0) && (
        <div className="mb-3 p-2 rounded bg-opacity-10 text-xs">
          {!validationState.isValid && (
            <div className="text-red-600 flex items-center">
              <span className="mr-1">ⓘ</span>
              {validationState.errors[0] || 'Validation error'}
            </div>
          )}
          {validationState.warnings.length > 0 && (
            <div className="text-yellow-600 flex items-center">
              <span className="mr-1">⚠</span>
              {validationState.warnings[0]}
            </div>
          )}
        </div>
      )}
      
      {/* Ports - Targets (Inputs) */}
      <div className="mb-2">
        {targetPorts.map(port => (
          <div key={`target-${port.id}`} className="flex items-center mb-1">
            <Handle
              type="target"
              id={port.id}
              position={port.position}
              className="w-3 h-3 bg-blue-500 border-2 border-white"
              style={{ [port.position]: '-6px' }}
            />
            <span className="text-xs text-gray-700 ml-2">
              {port.label || port.id} {port.dataType && `(${port.dataType})`}
            </span>
          </div>
        ))}
      </div>
      
      {/* Ports - Sources (Outputs) */}
      <div>
        {sourcePorts.map(port => (
          <div key={`source-${port.id}`} className="flex items-center mb-1">
            <Handle
              type="source"
              id={port.id}
              position={port.position}
              className="w-3 h-3 bg-green-500 border-2 border-white"
              style={{ [port.position]: '-6px' }}
            />
            <span className="text-xs text-gray-700 ml-2">
              {port.label || port.id} {port.dataType && `(${port.dataType})`}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
};

// Create a wrapper component that handles the type conflict
export const ValidationNodeWrapper: React.FC<NodeProps<ValidationNodeType['data']>> = (props) => {
  return <ValidationNode {...props} />;
};

// Node type configuration for React Flow
export const nodeTypes = {
  validationNode: ValidationNodeWrapper,
  // Add other custom node types here
};