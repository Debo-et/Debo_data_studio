// src/components/Wizard/ContextMenu.tsx
import React, { useRef, useEffect } from 'react';
import {
  FilePlus,
  FolderPlus,
  Trash2,
  Copy,
  Edit,
} from 'lucide-react';
import { RepositoryNode } from '../types/types';
import { WIZARD_CONFIG, WizardConfig } from '../config/wizard-registry';

interface ContextMenuProps {
  node: RepositoryNode;
  position: { x: number; y: number };
  onClose: () => void;
  onCreateItem?: (type: string, parentId?: string) => void;
  onDelete?: (node: RepositoryNode) => void;
  onBulkDelete?: (node: RepositoryNode) => void;

  // All wizard handlers – will be captured by ...wizardHandlers
  onOpenExcelWizard?: () => void;
  onOpenXMLWizard?: () => void;
  onOpenDelimitedWizard?: () => void;
  onOpenPositionalWizard?: () => void;
  onOpenJsonAvroParquetWizard?: () => void;
  onOpenFileSchemaWizard?: () => void;
  onOpenRegexWizard?: () => void;
  onOpenLDIFWizard?: () => void;
  onOpenWebServiceWizard?: () => void;
  onOpenLDAPWizard?: () => void;
  onOpenDatabaseWizard?: () => void;
  onCreateJobWizard?: () => void;
}

// Helper: count child nodes (unchanged)
const getNodeStats = (node: RepositoryNode): { childCount: number; deletableCount: number } => {
  let childCount = 0;
  let deletableCount = 0;

  const countChildren = (n: RepositoryNode) => {
    if (n.children) {
      childCount += n.children.length;
      n.children.forEach((child) => {
        const isSystem = [
          'job-designs', 'metadata', 'contexts', 'code',
          'sql-templates', 'business-models', 'documentation', 'recycle-bin'
        ].includes(child.id);
        if (!isSystem) deletableCount++;
        if (child.children) countChildren(child);
      });
    }
  };

  countChildren(node);
  return { childCount, deletableCount };
};

const ContextMenu: React.FC<ContextMenuProps> = ({
  node,
  position,
  onClose,
  onCreateItem,
  onDelete,
  onBulkDelete,
  ...wizardHandlers
}) => {
  const menuRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    console.log('ContextMenu opened:', { id: node.id, name: node.name, type: node.type });
  }, [node]);

  // Click outside handler
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(event.target as Node)) {
        onClose();
      }
    };
    const timer = setTimeout(() => {
      document.addEventListener('click', handleClickOutside);
    }, 100);
    return () => {
      clearTimeout(timer);
      document.removeEventListener('click', handleClickOutside);
    };
  }, [onClose]);

  // Helper to get a handler function by prop name
  const getHandler = (propName: string) => {
    const handler = (wizardHandlers as any)[propName];
    return typeof handler === 'function' ? handler : undefined;
  };

  // Determine which wizards apply to this node
  const applicableWizards = WIZARD_CONFIG.filter((config: WizardConfig) => {
    if (config.id) {
      const ids = Array.isArray(config.id) ? config.id : [config.id];
      if (ids.includes(node.id)) return true;
    }
    if (config.nodeType) {
      const types = Array.isArray(config.nodeType) ? config.nodeType : [config.nodeType];
      if (types.includes(node.type)) return true;
    }
    if (config.wizardType && node.metadata?.wizardType) {
      const wTypes = Array.isArray(config.wizardType) ? config.wizardType : [config.wizardType];
      if (wTypes.includes(node.metadata.wizardType)) return true;
    }
    return false;
  });

  const creationWizards = applicableWizards.filter(w => w.isCreation);
  const actionWizards = applicableWizards.filter(w => !w.isCreation);

  const isSystemNode = (): boolean => {
    const systemIds = [
      'job-designs', 'metadata', 'contexts', 'code',
      'sql-templates', 'business-models', 'documentation', 'recycle-bin'
    ];
    return node.type === 'folder' && systemIds.includes(node.id);
  };

  const handleClick = (action: () => void) => {
    action();
    onClose();
  };

  const menuStyle: React.CSSProperties = {
    position: 'fixed',
    left: position.x,
    top: position.y,
    zIndex: 1000,
  };

  return (
    <div
      ref={menuRef}
      className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md shadow-lg py-1 min-w-48"
      style={menuStyle}
      onClick={(e) => e.stopPropagation()}
    >
      {/* Debug info – remove in production */}
      <div className="px-3 py-1 text-xs text-gray-400 bg-gray-50 dark:bg-gray-900 border-b border-gray-200 dark:border-gray-700">
        ID: {node.id}
        {node.metadata?.wizardType && ` | wizardType: ${node.metadata.wizardType}`}
      </div>

      {/* ===== CREATION SECTION ===== */}
      {(creationWizards.length > 0 || (node.type === 'folder' && !isSystemNode())) && (
        <>
          <div className="px-3 py-2 text-xs font-medium text-gray-500 dark:text-gray-400 border-b border-gray-100 dark:border-gray-700">
            New
          </div>

          {creationWizards.map((config, idx) => {
            const handler = getHandler(config.handlerProp);
            return handler ? (
              <button
                key={`creation-${idx}`}
                onClick={() => handleClick(handler)}
                className="flex items-center space-x-2 w-full px-3 py-2 text-sm text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700"
              >
                {config.icon}
                <span>{config.label}</span>
              </button>
            ) : null;
          })}

          {node.type === 'folder' && !isSystemNode() && (
            <>
              <button
                onClick={() => handleClick(() => onCreateItem?.('folder', node.id))}
                className="flex items-center space-x-2 w-full px-3 py-2 text-sm text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700"
              >
                <FolderPlus className="h-4 w-4" />
                <span>Folder</span>
              </button>
              <button
                onClick={() => handleClick(() => onCreateItem?.('item', node.id))}
                className="flex items-center space-x-2 w-full px-3 py-2 text-sm text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700"
              >
                <FilePlus className="h-4 w-4" />
                <span>Item</span>
              </button>
            </>
          )}
        </>
      )}

      {/* ===== ACTION WIZARDS (non‑creation) ===== */}
      {actionWizards.length > 0 && (
        <>
          {creationWizards.length === 0 && node.type === 'folder' && !isSystemNode() && (
            <div className="border-t border-gray-100 dark:border-gray-700 my-1" />
          )}
          {actionWizards.map((config, idx) => {
            const handler = getHandler(config.handlerProp);
            return handler ? (
              <button
                key={`action-${idx}`}
                onClick={() => handleClick(handler)}
                className="flex items-center space-x-2 w-full px-3 py-2 text-sm text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700"
              >
                {config.icon}
                <span>{config.label}</span>
              </button>
            ) : null;
          })}
        </>
      )}

      {/* ===== EDIT/COPY FOR ITEMS ===== */}
      {node.type === 'item' && (
        <>
          {actionWizards.length === 0 && creationWizards.length === 0 && (
            <div className="border-t border-gray-100 dark:border-gray-700 my-1" />
          )}
          <button
            onClick={() => handleClick(() => console.log('Edit', node))}
            className="flex items-center space-x-2 w-full px-3 py-2 text-sm text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700"
          >
            <Edit className="h-4 w-4" />
            <span>Edit</span>
          </button>
          <button
            onClick={() => handleClick(() => console.log('Copy', node))}
            className="flex items-center space-x-2 w-full px-3 py-2 text-sm text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700"
          >
            <Copy className="h-4 w-4" />
            <span>Copy</span>
          </button>
        </>
      )}

      {/* ===== DELETE SECTION – only for non‑system nodes ===== */}
      {!isSystemNode() && (
        <>
          <div className="border-t border-gray-100 dark:border-gray-700 my-1" />

          <button
            onClick={() => {
              const stats = getNodeStats(node);
              if (stats.childCount > 0) {
                if (window.confirm(`Delete "${node.name}" and all ${stats.childCount} child items?`)) {
                  handleClick(() => onDelete?.(node));
                }
              } else {
                handleClick(() => onDelete?.(node));
              }
            }}
            className="flex items-center justify-between w-full px-3 py-2 text-sm text-red-600 dark:text-red-400 hover:bg-red-50 dark:hover:bg-red-900/20"
          >
            <div className="flex items-center space-x-2">
              <Trash2 className="h-4 w-4" />
              <span>Delete</span>
            </div>
            {node.children?.length ? (
              <span className="text-xs bg-red-100 text-red-700 px-1.5 py-0.5 rounded">
                +{node.children.length}
              </span>
            ) : null}
          </button>

          {node.type === 'folder' && node.children?.length && onBulkDelete && (
            <button
              onClick={() => {
                const stats = getNodeStats(node);
                if (window.confirm(`Delete all ${stats.deletableCount} deletable items in "${node.name}"?`)) {
                  handleClick(() => onBulkDelete(node));
                }
              }}
              className="flex items-center justify-between w-full px-3 py-2 text-sm text-red-700 dark:text-red-300 hover:bg-red-100 dark:hover:bg-red-900/30"
            >
              <div className="flex items-center space-x-2">
                <Trash2 className="h-4 w-4" />
                <span>Delete All Contents</span>
              </div>
              <span className="text-xs bg-red-200 text-red-800 px-2 py-1 rounded">
                {node.children.length} items
              </span>
            </button>
          )}

          {node.type !== 'folder' && !isSystemNode() && (
            <button
              onClick={() => {
                handleClick(() => {
                  console.log('Move to recycle bin:', node.name);
                  alert(`"${node.name}" moved to Recycle Bin (simulated)`);
                });
              }}
              className="flex items-center w-full px-3 py-2 text-sm text-amber-600 dark:text-amber-400 hover:bg-amber-50 dark:hover:bg-amber-900/20"
            >
              <svg className="h-4 w-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                  d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
              </svg>
              Move to Recycle Bin
            </button>
          )}
        </>
      )}
    </div>
  );
};

export default ContextMenu;