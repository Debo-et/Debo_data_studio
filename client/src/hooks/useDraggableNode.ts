import { useDraggable } from '@dnd-kit/core';
import { RepositoryNode } from '../types/types';

interface UseDraggableNodeProps {
  node: RepositoryNode;
  disabled?: boolean;
}

export const useDraggableNode = ({ node, disabled = false }: UseDraggableNodeProps) => {
  const {
    attributes,
    listeners,
    setNodeRef: setDraggableNodeRef,
    transform,
    isDragging,
  } = useDraggable({
    id: node.id,
    data: {
      type: 'repository-node',
      node,
      source: 'sidebar',
    },
    disabled: disabled || !node.draggable,
  });

  const style = transform ? {
    transform: `translate3d(${transform.x}px, ${transform.y}px, 0)`,
  } : undefined;

  return {
    attributes,
    listeners,
    setDraggableNodeRef,
    style,
    isDragging,
  };
};
