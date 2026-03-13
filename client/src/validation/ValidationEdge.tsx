// FILE: src/flow/ValidationEdge.tsx
/**
 * Custom React Flow edge with validation support
 * OMITTED:
 * - Complex edge path calculations (uses React Flow default)
 * - Dynamic edge styling based on data flow
 */

import React from 'react';
import { EdgeProps, BaseEdge, getBezierPath } from 'reactflow';
import { ValidationEdge as ValidationEdgeType } from './flow-types';

export const ValidationEdge: React.FC<EdgeProps<ValidationEdgeType['data']>> = ({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  data,
  markerEnd,
  style,
  selected
}) => {
  const [edgePath] = getBezierPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
  });

  // Determine edge styling based on validation status
  const edgeStyles = React.useMemo(() => {
    const baseStyle = {
      strokeWidth: 2,
      ...style
    };

    if (selected) {
      return { ...baseStyle, stroke: '#3b82f6', strokeWidth: 3 };
    }

    // If edge has validation errors in metadata
    if (data?.validationStatus === 'error') {
      return { ...baseStyle, stroke: '#ef4444', strokeDasharray: '5,5' };
    }

    if (data?.validationStatus === 'warning') {
      return { ...baseStyle, stroke: '#f59e0b' };
    }

    return { ...baseStyle, stroke: '#6b7280' };
  }, [style, selected, data]);

  // Edge label for data type or mapping
  const edgeLabel = data?.label || data?.dataType;

  return (
    <>
      <BaseEdge
        path={edgePath}
        markerEnd={markerEnd}
        style={edgeStyles}
      />
      {edgeLabel && (
        <text>
          <textPath
            href={`#${id}`}
            style={{ fontSize: '12px', fill: '#374151' }}
            startOffset="50%"
            textAnchor="middle"
          >
            {edgeLabel}
          </textPath>
        </text>
      )}
    </>
  );
};

// Edge type configuration for React Flow
export const edgeTypes = {
  validationEdge: ValidationEdge,
  // Add other custom edge types here
};