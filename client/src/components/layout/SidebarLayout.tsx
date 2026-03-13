// src/components/layout/SidebarLayout.tsx

import React, { useState } from 'react';
import { Button } from '../ui/Button';
import { ScrollArea } from '../ui/scroll-area';
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle } from '../ui/alert-dialog';
import {
  Database as DatabaseIcon,
  Trash2,
  Plus,
  X,
  AlertTriangle,
  Undo2,
  RefreshCw,
  Info,
  CheckCircle,
  AlertCircle,
  FolderPlus
} from 'lucide-react';
import TreeNode from '../../Wizard/TreeNode';
import { DatabaseApiService } from '../../services/database-api.service';
import { DatabaseConfig } from '../../services/database-api.service';
import { RepositoryNode } from '../../types/types';
import { ToastContainer } from 'react-toastify';

// Database Connection Dialog Component
export const DatabaseConnectionDialog: React.FC<{
  isOpen: boolean;
  onClose: () => void;
  onConnect: (config: DatabaseConfig, connectionId: string) => void;
  initialConfig?: DatabaseConfig;
  getDefaultUser: () => string;
}> = ({ isOpen, onClose, onConnect, initialConfig, getDefaultUser }) => {
  const [config, setConfig] = useState<DatabaseConfig>(initialConfig || {
    host: 'localhost',
    port: '5432',
    dbname: 'postgres',
    user: getDefaultUser(),
    password: '',
    schema: 'public'
  });
  
  const [testResult, setTestResult] = useState<{success: boolean; error?: string; version?: string} | null>(null);
  const [isTesting, setIsTesting] = useState(false);
  
  const apiService = new DatabaseApiService();

  const handleTestConnection = async () => {
    try {
      setIsTesting(true);
      setTestResult(null);
      
      const result = await apiService.testConnection('postgresql', config);
      if (result.success) {
        setTestResult({success: true, version: result.version});
        
        try {
          localStorage.setItem('last_db_user', config.user || '');
        } catch (e) {}
      } else {
        setTestResult({success: false, error: result.error || 'Please check your credentials.'});
      }
    } catch (error: any) {
      setTestResult({
        success: false, 
        error: `Error: ${error instanceof Error ? error.message : 'Unknown error'}`
      });
    } finally {
      setIsTesting(false);
    }
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    
    try {
      const testResult = await apiService.testConnection('postgresql', config);
      
      if (!testResult.success) {
        alert(`❌ Connection test failed: ${testResult.error || 'Please check your credentials.'}`);
        return;
      }
      
      const connectResult = await apiService.connect('postgresql', config);
      if (connectResult.success && connectResult.connectionId) {
        try {
          localStorage.setItem('last_db_user', config.user || '');
          localStorage.setItem('last_db_host', config.host || '');
          localStorage.setItem('last_db_port', config.port || '');
        } catch (e) {}
        
        onConnect(config, connectResult.connectionId);
        onClose();
        alert(`✅ Successfully connected! Connection ID: ${connectResult.connectionId}`);
      } else {
        alert(`❌ Connection failed: ${connectResult.error || 'Unknown error'}`);
      }
    } catch (error: any) {
      alert(`❌ Connection error: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white dark:bg-gray-800 rounded-lg p-6 w-full max-w-md">
        <h2 className="text-lg font-semibold mb-4">Connect to PostgreSQL</h2>
        
        <form onSubmit={handleSubmit}>
          <div className="space-y-4">
            <div>
              <label className="block text-sm font-medium mb-1">Host</label>
              <input
                type="text"
                value={config.host}
                onChange={(e) => setConfig({...config, host: e.target.value})}
                className="w-full px-3 py-2 border rounded-md"
                placeholder="localhost"
                required
              />
            </div>
            
            <div>
              <label className="block text-sm font-medium mb-1">Port</label>
              <input
                type="text"
                value={config.port}
                onChange={(e) => setConfig({...config, port: e.target.value})}
                className="w-full px-3 py-2 border rounded-md"
                placeholder="5432"
                required
              />
            </div>
            
            <div>
              <label className="block text-sm font-medium mb-1">Database</label>
              <input
                type="text"
                value={config.dbname}
                onChange={(e) => setConfig({...config, dbname: e.target.value})}
                className="w-full px-3 py-2 border rounded-md"
                placeholder="postgres"
                required
              />
            </div>
            
            <div>
              <label className="block text-sm font-medium mb-1">Username</label>
              <input
                type="text"
                value={config.user}
                onChange={(e) => setConfig({...config, user: e.target.value})}
                className="w-full px-3 py-2 border rounded-md"
                placeholder={getDefaultUser()}
                required
              />
              <p className="text-xs text-gray-500 mt-1">
                Try: postgres, {getDefaultUser()}, or your system username
              </p>
            </div>
            
            <div>
              <label className="block text-sm font-medium mb-1">Password</label>
              <input
                type="password"
                value={config.password || ''}
                onChange={(e) => setConfig({...config, password: e.target.value})}
                className="w-full px-3 py-2 border rounded-md"
                placeholder="Leave empty for local connection"
              />
              <p className="text-xs text-gray-500 mt-1">
                For local PostgreSQL, password is often empty
              </p>
            </div>
            
            <div>
              <label className="block text-sm font-medium mb-1">Schema</label>
              <input
                type="text"
                value={config.schema || 'public'}
                onChange={(e) => setConfig({...config, schema: e.target.value})}
                className="w-full px-3 py-2 border rounded-md"
                placeholder="public"
              />
            </div>
            
            {testResult && (
              <div className={`p-2 rounded text-sm ${testResult.success ? 'bg-green-50 text-green-700' : 'bg-red-50 text-red-700'}`}>
                {testResult.success ? (
                  <>
                    <div className="font-medium">✅ Connection successful!</div>
                    {testResult.version && (
                      <div className="mt-1">PostgreSQL version: {testResult.version}</div>
                    )}
                  </>
                ) : (
                  <div className="font-medium">❌ Connection failed: {testResult.error}</div>
                )}
              </div>
            )}
            
            <div className="flex justify-between pt-4">
              <button
                type="button"
                onClick={handleTestConnection}
                disabled={isTesting}
                className="px-4 py-2 text-sm border rounded-md hover:bg-gray-50 disabled:opacity-50"
              >
                {isTesting ? 'Testing...' : 'Test Connection'}
              </button>
              
              <div className="space-x-2">
                <button
                  type="button"
                  onClick={onClose}
                  className="px-4 py-2 text-sm border rounded-md hover:bg-gray-50"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  className="px-4 py-2 text-sm bg-blue-600 text-white rounded-md hover:bg-blue-700"
                >
                  Connect
                </button>
              </div>
            </div>
          </div>
        </form>
      </div>
    </div>
  );
};

// Job Creation Dialog Component
export const JobCreationDialog: React.FC<{
  isOpen: boolean;
  onClose: () => void;
  onCreateJob: (jobName: string) => void;
}> = ({ isOpen, onClose, onCreateJob }) => {
  const [jobName, setJobName] = useState('');
  const [template, setTemplate] = useState('blank');

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (jobName.trim()) {
      let finalJobName = jobName.trim();
      if (template !== 'blank') {
        finalJobName = `${template} - ${finalJobName}`;
      }
      onCreateJob(finalJobName);
      onClose();
      setJobName('');
    }
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white dark:bg-gray-800 rounded-lg p-6 w-full max-w-md">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-semibold">Create New Job Design</h2>
          <button
            onClick={onClose}
            className="p-1 hover:bg-gray-100 rounded"
          >
            <X className="h-4 w-4" />
          </button>
        </div>
        
        <form onSubmit={handleSubmit}>
          <div className="space-y-4">
            <div>
              <label className="block text-sm font-medium mb-2">
                Job Name *
              </label>
              <input
                type="text"
                value={jobName}
                onChange={(e) => setJobName(e.target.value)}
                className="w-full px-3 py-2 border rounded-md"
                placeholder="e.g., Customer Data Pipeline"
                autoFocus
              />
            </div>

            <div>
              <label className="block text-sm font-medium mb-2">
                Template (Optional)
              </label>
              <select
                value={template}
                onChange={(e) => setTemplate(e.target.value)}
                className="w-full px-3 py-2 border rounded-md"
              >
                <option value="blank">Blank Job</option>
                <option value="ETL Pipeline">ETL Pipeline</option>
                <option value="Data Transformation">Data Transformation</option>
                <option value="Data Migration">Data Migration</option>
                <option value="Real-time Processing">Real-time Processing</option>
              </select>
            </div>

            <div className="bg-gray-50 dark:bg-gray-900 p-3 rounded text-sm">
              <p className="font-medium mb-1">What happens next:</p>
              <ul className="list-disc list-inside text-gray-600 dark:text-gray-400 space-y-1">
                <li>A new draggable canvas will be created</li>
                <li>You can add components from the sidebar or palette</li>
                <li>Job will be automatically saved</li>
                <li>You can close the job anytime and return to this view</li>
              </ul>
            </div>
            
            <div className="flex justify-end space-x-2 pt-4">
              <button
                type="button"
                onClick={onClose}
                className="px-4 py-2 text-sm border rounded-md hover:bg-gray-50"
              >
                Cancel
              </button>
              <button
                type="submit"
                disabled={!jobName.trim()}
                className="px-4 py-2 text-sm bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50"
              >
                Create Job Workspace
              </button>
            </div>
          </div>
        </form>
      </div>
    </div>
  );
};

// Job Designs Context Menu
export const JobDesignsContextMenu: React.FC<{
  node: RepositoryNode;
  position: { x: number; y: number };
  onClose: () => void;
  onCreateJob: () => void;
}> = ({ position, onClose, onCreateJob }) => {
  const handleCreateJob = () => {
    onCreateJob();
    onClose();
  };

  return (
    <div 
      className="fixed bg-white dark:bg-gray-800 shadow-lg rounded-md border border-gray-200 dark:border-gray-700 z-50 min-w-[200px]"
      style={{ 
        left: Math.min(position.x, window.innerWidth - 220), 
        top: position.y 
      }}
    >
      <div className="py-1">
        <button
          onClick={handleCreateJob}
          className="w-full text-left px-4 py-2 text-sm hover:bg-gray-100 dark:hover:bg-gray-700 flex items-center space-x-2"
        >
          <FolderPlus className="h-4 w-4" />
          <span>Create New Job</span>
        </button>
      </div>
    </div>
  );
};

// Delete Confirmation Dialog
export const DeleteConfirmationDialog: React.FC<{
  isOpen: boolean;
  node: RepositoryNode | null;
  onClose: () => void;
  onConfirm: () => void;
  calculateDeleteStats: (node: RepositoryNode) => {
    nodesToDelete: number;
    hasChildren: boolean;
    nodeName: string;
    metadataCount: number;
    folderCount: number;
    itemCount: number;
    hasForeignTables: boolean;
    hasDatabaseConnections: boolean;
  };
}> = ({ isOpen, node, onClose, onConfirm, calculateDeleteStats }) => {
  if (!node) return null;

  const stats = calculateDeleteStats(node);

  return (
    <AlertDialog open={isOpen} onOpenChange={onClose}>
      <AlertDialogContent className="max-w-md">
        <AlertDialogHeader>
          <AlertDialogTitle className="flex items-center gap-2 text-red-600">
            <AlertTriangle className="h-5 w-5" />
            Confirm Deletion
          </AlertDialogTitle>
          <AlertDialogDescription className="space-y-4">
            <div className="bg-red-50 dark:bg-red-900/20 p-4 rounded-md border border-red-200 dark:border-red-800">
              <p className="font-medium text-red-800 dark:text-red-300">
                You are about to delete "{node.name}"
              </p>
              
              {stats.hasChildren && (
                <div className="mt-3 space-y-2">
                  <div className="flex items-center text-sm text-red-700 dark:text-red-400">
                    <AlertTriangle className="h-4 w-4 mr-2 flex-shrink-0" />
                    <span>
                      This will delete <strong>{stats.nodesToDelete - 1} child item{stats.nodesToDelete - 1 === 1 ? '' : 's'}</strong> recursively.
                    </span>
                  </div>
                  
                  {stats.folderCount > 0 && (
                    <div className="text-xs text-red-600 dark:text-red-500 ml-6">
                      <span className="font-medium">Folders:</span> {stats.folderCount}
                    </div>
                  )}
                  
                  {stats.itemCount > 0 && (
                    <div className="text-xs text-red-600 dark:text-red-500 ml-6">
                      <span className="font-medium">Items:</span> {stats.itemCount}
                    </div>
                  )}
                  
                  {stats.metadataCount > 0 && (
                    <div className="text-xs text-red-600 dark:text-red-500 ml-6">
                      <span className="font-medium">Metadata items:</span> {stats.metadataCount}
                    </div>
                  )}
                  
                  {node.type === 'folder' && node.children && node.children.length > 0 && (
                    <div className="mt-2 text-xs text-red-600 dark:text-red-500">
                      <p className="font-medium mb-1">Contents preview:</p>
                      <ul className="list-disc list-inside ml-2 space-y-1">
                        {node.children.slice(0, 3).map((child, idx) => (
                          <li key={idx} className="truncate">{child.name} ({child.type})</li>
                        ))}
                        {node.children.length > 3 && (
                          <li className="italic">...and {node.children.length - 3} more</li>
                        )}
                      </ul>
                    </div>
                  )}
                </div>
              )}
              
              {stats.hasForeignTables && (
                <div className="mt-2 p-2 bg-red-100 dark:bg-red-900/30 rounded">
                  <p className="text-xs font-medium text-red-800 dark:text-red-400">
                    ⚠️ This contains PostgreSQL foreign tables that will be dropped.
                  </p>
                </div>
              )}
              
              {stats.hasDatabaseConnections && (
                <div className="mt-2 p-2 bg-red-100 dark:bg-red-900/30 rounded">
                  <p className="text-xs font-medium text-red-800 dark:text-red-400">
                    ⚠️ Database connections will be disconnected.
                  </p>
                </div>
              )}
              
              {node.metadata && (
                <div className="mt-2 p-2 bg-red-100 dark:bg-red-900/30 rounded">
                  <p className="text-xs font-medium mb-1">Node details:</p>
                  <div className="text-xs space-y-1">
                    {node.metadata.type && (
                      <div className="flex justify-between">
                        <span className="text-red-700 dark:text-red-500">Type:</span>
                        <span className="font-medium">{node.metadata.type}</span>
                      </div>
                    )}
                    {node.metadata.lastModified && (
                      <div className="flex justify-between">
                        <span className="text-red-700 dark:text-red-500">Last Modified:</span>
                        <span>{new Date(node.metadata.lastModified).toLocaleDateString()}</span>
                      </div>
                    )}
                    {node.metadata.description && (
                      <div className="mt-1 text-red-600 dark:text-red-500 truncate">
                        "{node.metadata.description}"
                      </div>
                    )}
                  </div>
                </div>
              )}
            </div>
            
            <div className="text-sm text-gray-600 dark:text-gray-400 space-y-2">
              <div className="flex items-center justify-between p-2 bg-gray-50 dark:bg-gray-800 rounded">
                <span className="font-medium">Total items to delete:</span>
                <span className="font-bold text-red-600">{stats.nodesToDelete}</span>
              </div>
              <p className="text-center font-medium">
                ⚠️ This action <strong>cannot be undone</strong>.
              </p>
            </div>
          </AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel onClick={onClose} className="border-gray-300 hover:bg-gray-50">
            Cancel
          </AlertDialogCancel>
          <AlertDialogAction
            onClick={(e) => {
              e.preventDefault();
              onConfirm();
              onClose();
            }}
            className="bg-red-600 text-white hover:bg-red-700 focus:ring-red-600"
          >
            <Trash2 className="h-4 w-4 mr-2" />
            Delete {stats.nodesToDelete} Item{stats.nodesToDelete > 1 ? 's' : ''}
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  );
};


// Delete Undo Panel
export const DeleteUndoPanel: React.FC<{
  isUndoAvailable: boolean;
  deletionHistory: Array<{ node: RepositoryNode; deletedAt: string; }>;
  onUndoLastDeletion: () => void;
  onClearDeletionHistory: () => void;
}> = ({ isUndoAvailable, deletionHistory, onUndoLastDeletion, onClearDeletionHistory }) => {
  if (!isUndoAvailable) return null;

  return (
    <div className="flex-none p-2 border-b bg-amber-50 dark:bg-amber-900/20 border-amber-200 dark:border-amber-800">
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-2">
          <Undo2 className="h-4 w-4 text-amber-600 dark:text-amber-400" />
          <span className="text-xs font-medium">
            Undo Available: {deletionHistory.length} deletion{deletionHistory.length === 1 ? '' : 's'}
          </span>
        </div>
        <div className="flex space-x-1">
          <Button
            size="sm"
            variant="outline"
            className="h-6 px-2 text-xs bg-white hover:bg-amber-100"
            onClick={onUndoLastDeletion}
            title="Undo last deletion"
          >
            <Undo2 className="h-3 w-3 mr-1" />
            Undo
          </Button>
          <Button
            size="sm"
            variant="ghost"
            className="h-6 px-2 text-xs text-amber-700 hover:text-amber-800 hover:bg-amber-100"
            onClick={onClearDeletionHistory}
            title="Clear deletion history"
          >
            <Trash2 className="h-3 w-3" />
          </Button>
        </div>
      </div>
      {deletionHistory.length > 0 && (
        <p className="text-xs mt-1 text-amber-600 dark:text-amber-400 truncate">
          Last: {deletionHistory[deletionHistory.length - 1].node.name}
        </p>
      )}
    </div>
  );
};

// Database Connection Panel
export const DatabaseConnectionPanel: React.FC<{
  healthStatus: string;
  isPostgresConnected: boolean;
  connectionError?: string | null;
  isCheckingPostgres: boolean;
  apiLoading: boolean;
  apiError?: string;
  dbStatus: { isConnected: boolean; version?: string };
  autoConnectInProgress: boolean;
  onRefresh: () => void;
  onConnect: () => void;
  onDisconnect: () => void;
  onShowInstructions: () => void;
  connectionConfig: DatabaseConfig;
}> = ({
  healthStatus,
  isPostgresConnected,
  connectionError,
  isCheckingPostgres,
  apiLoading,
  apiError,
  dbStatus,
  autoConnectInProgress,
  onRefresh,
  onConnect,
  onDisconnect,
  onShowInstructions}) => {
  // Safely access properties with fallbacks
  
  return (
    <div className={`flex-none p-2 border-b ${healthStatus === 'healthy' && isPostgresConnected ? 'bg-green-50 dark:bg-green-900/20 border-green-200 dark:border-green-800' : healthStatus === 'healthy' ? 'bg-yellow-50 dark:bg-yellow-900/20 border-yellow-200 dark:border-yellow-800' : 'bg-red-50 dark:bg-red-900/20 border-red-200 dark:border-red-800'}`}>
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-2">
          {healthStatus === 'healthy' ? (
            isPostgresConnected ? (
              <CheckCircle className="h-4 w-4 text-green-600 dark:text-green-400" />
            ) : (
              <AlertCircle className="h-4 w-4 text-yellow-600 dark:text-yellow-400" />
            )
          ) : (
            <AlertCircle className="h-4 w-4 text-red-600 dark:text-red-400" />
          )}
          <span className="text-xs font-medium">
            {healthStatus === 'healthy' ? 
              (isPostgresConnected ? 'PostgreSQL Connected' : 'PostgreSQL Not Connected') : 
              'Backend Not Responding'}
          </span>
        </div>
        <Button
          variant="ghost"
          size="sm"
          className="h-5 px-2 text-xs"
          onClick={onRefresh}
          disabled={isCheckingPostgres || apiLoading}
        >
          {isCheckingPostgres || apiLoading ? (
            <RefreshCw className="h-3 w-3 animate-spin" />
          ) : (
            <RefreshCw className="h-3 w-3" />
          )}
        </Button>
      </div>
      
      {apiError && (
        <p className="text-xs mt-1 text-red-600 dark:text-red-400">
          {apiError}
        </p>
      )}
      
      {connectionError && (
        <p className="text-xs mt-1 text-red-600 dark:text-red-400">
          {connectionError}
        </p>
      )}
      
      {autoConnectInProgress && (
        <p className="text-xs mt-1 text-blue-600 dark:text-blue-400">
          🔄 Attempting auto-connection to local PostgreSQL...
        </p>
      )}
      
      {isPostgresConnected && dbStatus.version && (
        <p className="text-xs mt-1 text-gray-600 dark:text-gray-400 truncate">
          PostgreSQL {dbStatus.version}
        </p>
      )}
      
      {healthStatus !== 'healthy' && (
        <p className="text-xs mt-1 text-red-600 dark:text-red-400">
          ⚠️ Backend server not responding. Please start the backend server.
        </p>
      )}
      
      {!isPostgresConnected && healthStatus === 'healthy' && !isCheckingPostgres && (
        <div className="mt-2 flex space-x-2">
          <Button
            size="sm"
            variant="outline"
            className="flex-1 text-xs h-6"
            onClick={onConnect}
          >
            <DatabaseIcon className="h-3 w-3 mr-1" />
            Connect to PostgreSQL
          </Button>
          <Button
            size="sm"
            variant="ghost"
            className="h-6 px-2 text-xs"
            onClick={onShowInstructions}
          >
            <Info className="h-3 w-3" />
          </Button>
        </div>
      )}
      
      {isPostgresConnected && (
        <div className="mt-2">
          <Button
            size="sm"
            variant="outline"
            className="w-full text-xs h-6"
            onClick={onDisconnect}
          >
            Disconnect
          </Button>
        </div>
      )}
    </div>
  );
};

// Drag Instructions Panel
export const DragInstructionsPanel: React.FC<{
  isDragging: boolean;
  draggedNodeName?: string;
}> = ({ isDragging, draggedNodeName }) => {
  if (!isDragging) return null;

  return (
    <div className="flex-none p-2 bg-blue-50 dark:bg-blue-900/20 border-b border-blue-200 dark:border-blue-800">
      <p className="text-xs text-blue-600 dark:text-blue-300 text-center">
        📦 Drag to create React Flow node: {draggedNodeName}
      </p>
      <p className="text-xs text-blue-500 dark:text-blue-400 text-center mt-1">
        Drop on canvas to create component
      </p>
    </div>
  );
};

// Tree View Component
export const TreeView: React.FC<{
  repositoryData: RepositoryNode[];
  expandedNodes: Set<string>;
  selectedNode: string | null;
  searchTerm: string;
  dragState?: any;  // Make optional
  expandedMetadataNodes: Set<string>;
  onToggle: (nodeId: string) => void;
  onSelect: (node: RepositoryNode) => void;
  onDoubleClick: (node: RepositoryNode) => void;
  onContextMenu: (node: RepositoryNode, position: { x: number; y: number }) => void;
  onDelete: (node: RepositoryNode) => void;
  onDragStart: (node: RepositoryNode, event: React.DragEvent) => void;
  onDragOver?: (node: RepositoryNode, event: React.DragEvent) => void;  // Make optional
  onDragLeave?: (event: React.DragEvent) => void;  // Make optional
  onDrop?: (node: RepositoryNode, event: React.DragEvent) => void;  // Make optional
  onDragEnd?: () => void;  // Make optional
  onToggleMetadata: (nodeId: string) => void;
}> = ({
  repositoryData,
  expandedNodes,
  selectedNode,
  searchTerm,
  dragState = null,  // Default value
  expandedMetadataNodes,
  onToggle,
  onSelect,
  onDoubleClick,
  onContextMenu,
  onDelete,
  onDragStart,
  onDragOver,
  onDragLeave,
  onDrop,
  onDragEnd,
  onToggleMetadata
}) => (
  <div className="flex-1 min-h-0 overflow-hidden">
    <ScrollArea className="h-full w-full">
      <div 
        className="p-2 w-full"
        onDragOver={(e) => {
          e.preventDefault();
          // Call onDragOver if provided
          if (onDragOver) {
            // You might need to pass the appropriate node here
            // For now, we'll just prevent default
          }
        }}
        onDrop={(e) => {
          e.preventDefault();
          // Call onDragEnd if provided
          if (onDragEnd) {
            onDragEnd();
          }
        }}
      >
        {repositoryData.map((node) => (
          <TreeNode
            key={node.id}
            node={node}
            level={0}
            expandedNodes={expandedNodes}
            selectedNode={selectedNode}
            searchTerm={searchTerm}
            dragState={dragState}
            expandedMetadataNodes={expandedMetadataNodes}
            onToggle={onToggle}
            onSelect={onSelect}
            onDoubleClick={onDoubleClick}
            onContextMenu={onContextMenu}
            onDelete={onDelete}
            onDragStart={onDragStart}
            onDragOver={onDragOver}
            onDragLeave={onDragLeave}
            onDrop={onDrop}
            onDragEnd={onDragEnd}
            onToggleMetadata={onToggleMetadata}
          />
        ))}
      </div>
    </ScrollArea>
  </div>
);

// Debug Panel Component
export const DebugPanel: React.FC<{
  isDebugOpen: boolean;
  setIsDebugOpen: (open: boolean) => void;
  onTestConnection: () => void;
  onListTables: () => void;
  onRefresh: () => void;
  onTestTableCreation: () => void;
  onManualSave: () => void;
  onResetRepository: () => void;
  healthStatus: string;
  isPostgresConnected: boolean;
  connectionConfig: any;
  dbStatus: any;
  currentConnectionId: string | null;
  foreignTables: any[];
  debugInfo: any;
  repositoryData: RepositoryNode[];
  expandedNodes: Set<string>;
  selectedNode: string | null;
  deletionHistory: any[];
  isUndoAvailable: boolean;
}> = ({
  isDebugOpen,
  setIsDebugOpen,
  onTestConnection,
  onListTables,
  onRefresh,
  onTestTableCreation,
  onManualSave,
  onResetRepository,
  healthStatus,
  isPostgresConnected,
  connectionConfig,
  dbStatus,
  currentConnectionId,
  foreignTables,
  debugInfo,
  repositoryData,
  expandedNodes,
  selectedNode,
  deletionHistory,
  isUndoAvailable
}) => (
  <div className="p-3 bg-gray-50 dark:bg-gray-800 border-t">
    <details 
      className="text-xs" 
      open={isDebugOpen}
      onToggle={(e) => setIsDebugOpen((e.target as HTMLDetailsElement).open)}
    >
      <summary className="cursor-pointer font-medium flex items-center">
        <Info className="h-3 w-3 mr-1" />
        Database Connection Panel
      </summary>
      <div className="mt-2 space-y-2">
        <div className="grid grid-cols-2 gap-2">
          <Button 
            size="sm" 
            variant="outline" 
            onClick={onTestConnection}
          >
            Connect
          </Button>
          
          <Button 
            size="sm" 
            variant="outline" 
            onClick={onListTables}
            disabled={!isPostgresConnected}
          >
            List Tables
          </Button>
        </div>
        
        <div className="grid grid-cols-2 gap-2">
          <Button 
            size="sm" 
            variant="outline" 
            onClick={onRefresh}
          >
            Refresh
          </Button>
          
          <Button 
            size="sm" 
            variant="outline" 
            onClick={onTestTableCreation}
            disabled={!isPostgresConnected}
          >
            Test Creation
          </Button>
        </div>
        
        <div className="text-xs text-gray-600 dark:text-gray-400 space-y-1">
          <div className="flex justify-between">
            <span>Backend Status:</span>
            <span className={healthStatus === 'healthy' ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'}>
              {healthStatus === 'healthy' ? '✅ Healthy' : '❌ Unhealthy'}
            </span>
          </div>
          
          <div className="flex justify-between">
            <span>PostgreSQL Status:</span>
            <span className={isPostgresConnected ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'}>
              {isPostgresConnected ? '✅ Connected' : '❌ Disconnected'}
            </span>
          </div>
          
          <div className="flex justify-between">
            <span>Host:</span>
            <span>{connectionConfig?.host || 'localhost'}:{connectionConfig?.port || '5432'}</span>
          </div>
          
          <div className="flex justify-between">
            <span>Database:</span>
            <span>{connectionConfig?.dbname || 'postgres'}</span>
          </div>
          
          <div className="flex justify-between">
            <span>User:</span>
            <span>{connectionConfig?.user || 'postgres'}</span>
          </div>
          
          {dbStatus.version && (
            <div className="flex justify-between">
              <span>Version:</span>
              <span>{dbStatus.version}</span>
            </div>
          )}
          
          <div className="flex justify-between">
            <span>Foreign Tables:</span>
            <span>{foreignTables.length}</span>
          </div>
          
          {debugInfo.lastOperation && (
            <div className="flex justify-between">
              <span>Last Operation:</span>
              <span className="truncate" title={debugInfo.lastOperation}>
                {debugInfo.lastOperation}
              </span>
            </div>
          )}
          
          {currentConnectionId && (
            <div className="flex justify-between">
              <span>Connection ID:</span>
              <span className="truncate font-mono text-xs" title={currentConnectionId}>
                {currentConnectionId.substring(0, 8)}...
              </span>
            </div>
          )}
        </div>
        
        <div className="pt-2 border-t border-gray-200 dark:border-gray-700 mt-2">
          <p className="text-xs font-medium mb-2">Repository Persistence</p>
          <div className="grid grid-cols-2 gap-2">
            <Button 
              size="sm" 
              variant="outline" 
              onClick={onManualSave}
              className="text-xs"
            >
              💾 Save Now
            </Button>
            <Button 
              size="sm" 
              variant="outline" 
              onClick={onResetRepository}
              className="text-xs text-red-600 hover:text-red-700"
            >
              🔄 Reset
            </Button>
          </div>
          <div className="mt-2 text-xs text-gray-500">
            <p>Items: {repositoryData.reduce((count, category) => 
              count + (category.children?.length || 0), 0)}</p>
            <p>Expanded: {expandedNodes.size} nodes</p>
            <p>Selected: {selectedNode || 'None'}</p>
          </div>
        </div>

        <div className="pt-2 border-t border-gray-200 dark:border-gray-700 mt-2">
          <p className="text-xs font-medium mb-2">Delete Statistics</p>
          <div className="text-xs text-gray-500 space-y-1">
            <div className="flex justify-between">
              <span>Deletion History:</span>
              <span>{deletionHistory.length} item{deletionHistory.length === 1 ? '' : 's'}</span>
            </div>
            <div className="flex justify-between">
              <span>Undo Available:</span>
              <span className={isUndoAvailable ? 'text-green-600' : 'text-gray-400'}>
                {isUndoAvailable ? '✅ Yes' : '❌ No'}
              </span>
            </div>
            {deletionHistory.length > 0 && (
              <div className="mt-1 text-xs">
                <p className="font-medium mb-1">Recent deletions:</p>
                <div className="max-h-20 overflow-y-auto">
                  {deletionHistory.slice(-3).reverse().map((item, idx) => (
                    <div key={idx} className="flex justify-between items-center py-1 border-b border-gray-100 dark:border-gray-700">
                      <span className="truncate flex-1 mr-2">{item.node.name}</span>
                      <span className="text-xs text-gray-400">
                        {new Date(item.deletedAt).toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'})}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </details>
  </div>
);

// React Flow Integration Panel
export const ReactFlowIntegrationPanel: React.FC<{
  reactFlowInstance: any;
  onNodeCreate: ((nodeData: any, position: { x: number; y: number }) => void) | undefined;
  onNodeUpdate: ((nodeId: string, updates: any) => void) | undefined;
  selectedNode: string | null;
  findNodeById: (nodes: RepositoryNode[], nodeId: string) => RepositoryNode | null;
  repositoryData: RepositoryNode[];
  createNodeDirectly: (node: RepositoryNode, position: { x: number; y: number }) => void;
  isDebugOpen: boolean;
  setIsDebugOpen: (open: boolean) => void;
}> = ({
  reactFlowInstance,
  onNodeCreate,
  onNodeUpdate,
  selectedNode,
  findNodeById,
  repositoryData,
  createNodeDirectly,
  isDebugOpen,
  setIsDebugOpen
}) => (
  <div className="p-3 bg-blue-50 dark:bg-blue-900/20 border-t border-blue-200 dark:border-blue-800">
    <div className="space-y-2">
      <div className="flex space-x-2">
        <Button
          size="sm"
          variant="outline"
          className="flex-1 text-xs h-6"
          onClick={() => {
            if (selectedNode) {
              const node = findNodeById(repositoryData, selectedNode);
              if (node && node.metadata) {
                createNodeDirectly(node, { x: 200, y: 200 });
              }
            }
          }}
          title="Create test node at (200, 200)"
        >
          <Plus className="h-3 w-3 mr-1" />
          Test Node
        </Button>
        <Button
          size="sm"
          variant="outline"
          className="h-6 px-2 text-xs"
          onClick={() => setIsDebugOpen(!isDebugOpen)}
        >
          <Info className="h-3 w-3" />
        </Button>
      </div>
      
      {isDebugOpen && (
        <div className="text-xs text-blue-600 dark:text-blue-400 space-y-1 mt-2 p-2 bg-blue-100 dark:bg-blue-900/30 rounded">
          <div className="flex justify-between">
            <span>React Flow Instance:</span>
            <span>{reactFlowInstance ? '✅ Available' : '❌ Not Available'}</span>
          </div>
          <div className="flex justify-between">
            <span>Drag Format:</span>
            <span>React Flow Compatible</span>
          </div>
          <div className="flex justify-between">
            <span>Node Creation:</span>
            <span>{onNodeCreate ? '✅ Direct' : '✅ Event-based'}</span>
          </div>
          <div className="flex justify-between">
            <span>Node Updates:</span>
            <span>{onNodeUpdate ? '✅ Direct' : '✅ Event-based'}</span>
          </div>
        </div>
      )}
    </div>
  </div>
);

// Toast Container Wrapper
export const ToastContainerWrapper = () => (
  <ToastContainer
    position="bottom-right"
    autoClose={3000}
    hideProgressBar={false}
    newestOnTop
    closeOnClick
    rtl={false}
    pauseOnFocusLoss
    draggable
    pauseOnHover
    theme="light"
  />
);