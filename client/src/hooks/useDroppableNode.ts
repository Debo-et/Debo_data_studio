import { useDroppable } from '@dnd-kit/core';
import { RepositoryNode } from '../types/types';

interface UseDroppableNodeProps {
  node: RepositoryNode;
  disabled?: boolean;
}

export const useDroppableNode = ({ node, disabled = false }: UseDroppableNodeProps) => {
  const {
    isOver,
    setNodeRef: setDroppableNodeRef,
  } = useDroppable({
    id: node.id,
    data: {
      type: 'repository-drop-target',
      node,
      accepts: ['repository-node'],
    },
    disabled: disabled || !node.droppable,
  });

  return {
    isOver,
    setDroppableNodeRef,
  };
};

