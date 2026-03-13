import React, { useState, useEffect, useCallback, Suspense, useRef } from 'react';
import { ReactFlowProvider } from 'reactflow';
import {
  DndContext,
  DragOverlay,
  useSensor,
  useSensors,
  PointerSensor,
  KeyboardSensor,
  DragStartEvent,
  DragEndEvent,
  UniqueIdentifier,
  closestCenter,
} from '@dnd-kit/core';
import {
  sortableKeyboardCoordinates,
} from '@dnd-kit/sortable';

// Import CanvasProvider
import { CanvasProvider } from './pages/CanvasContext';

// Import Job Design Types
import { JobDesignState } from './types/types';

// Import Database API Service
import { DatabaseApiService } from './services/database-api.service';

// Import Canvas Persistence Service (still used indirectly via Sidebar)
import { canvasPersistence } from './services/canvas-persistence.service';

// Lazy load components
const Toolbar = React.lazy(() => import('./components/layout/Toolbar'));
const Console = React.lazy(() => import('./components/layout/Console'));
const Sidebar = React.lazy(() => import('./components/layout/Sidebar'));
const Canvas = React.lazy(() => import('./pages/Canvas'));
const RightPanel = React.lazy(() => import('./components/layout/RightPanel'));
const ErrorBoundary = React.lazy(() => import('./components/ErrorBoundary'));
const WorkspacePlaceholder = React.lazy(() => import('./components/layout/WorkspacePlaceholder'));

// Extended Window interface
interface ExtendedWindow extends Window {
  electron?: {
    isElectron: boolean;
    platform: string;
  };
  process?: {
    type?: string;
    versions?: {
      electron?: string;
    };
  };
}

declare const window: ExtendedWindow;

// Utility to detect Electron environment
const isElectron = (): boolean => {
  if (typeof window !== 'undefined' && window.process && window.process.type === 'renderer') {
    return true;
  }
  
  if (typeof navigator !== 'undefined' && navigator.userAgent.toLowerCase().includes('electron')) {
    return true;
  }
  
  if (typeof window !== 'undefined' && (window as any).require) {
    return true;
  }
  
  if (typeof window !== 'undefined' && window.electron) {
    return true;
  }
  
  return false;
};

// Drag Preview Component
interface GlobalDragPreviewProps {
  data: any;
  isSidebarDrag: boolean;
  isCanvasDrag: boolean;
}

const GlobalDragPreview: React.FC<GlobalDragPreviewProps> = ({ data, isSidebarDrag, isCanvasDrag }) => {
  if (!data) return null;

  const label = data?.label || data?.name || data?.node?.name || 'Unnamed';
  const nodeType = data?.nodeType || data?.node?.nodeType || 'node';

  const getNodeIcon = (type: string) => {
    switch (type) {
      case 'input': return '📥';
      case 'output': return '📤';
      case 'transform': return '🔄';
      case 'filter': return '🔍';
      case 'join': return '🔗';
      case 'repository-node': return '📁';
      default: return '📄';
    }
  };

  return (
    <div className="bg-white border-2 border-blue-500 rounded-lg shadow-lg p-3 min-w-[140px]">
      <div className="flex items-center space-x-2">
        <div className="text-lg">
          {getNodeIcon(nodeType)}
        </div>
        <div className="flex-1 min-w-0">
          <div className="text-sm font-semibold text-gray-800 truncate">
            {label}
          </div>
          <div className="text-xs text-gray-600 capitalize">
            {nodeType}
          </div>
          <div className={`text-xs px-2 py-1 rounded mt-1 ${
            isSidebarDrag ? 'bg-blue-100 text-blue-600' : 
            isCanvasDrag ? 'bg-green-100 text-green-600' : 
            'bg-gray-100 text-gray-600'
          }`}>
            {isSidebarDrag ? 'From Sidebar' : isCanvasDrag ? 'Moving Node' : 'Dragging'}
          </div>
        </div>
      </div>
    </div>
  );
};

// Loading Spinner Component
const LoadingSpinner: React.FC<{ message?: string }> = ({ message = "Loading..." }) => (
  <div className="flex flex-col items-center justify-center h-screen bg-gray-50">
    <div className="animate-spin rounded-full h-16 w-16 border-b-2 border-blue-600 mb-4"></div>
    <h2 className="text-xl font-semibold text-gray-700 mb-2">
      Debo Data Studio
    </h2>
    <p className="text-gray-500">{message}</p>
  </div>
);

// PostgreSQL Connection Error Component
const PostgreSQLConnectionError: React.FC<{ error: string; onRetry: () => void }> = ({ error, onRetry }) => {
  const [showDetails, setShowDetails] = useState(false);
  
  // Parse installation script for connection parameters
  const getConnectionParams = useCallback(() => {
    const params = {
      host: 'localhost',
      port: '5432',
      database: 'postgres',
      user: 'postgres',
    };
    
    if (typeof window !== 'undefined') {
      const storedUser = localStorage.getItem('last_db_user');
      if (storedUser) {
        params.user = storedUser;
      } else {
        const systemUser = process.env.REACT_APP_DB_USER || 
                          (process.platform === 'win32' ? process.env.USERNAME : process.env.USER) || 
                          'postgres';
        params.user = systemUser;
      }
    }
    
    return params;
  }, []);
  
  const connectionParams = getConnectionParams();
  
  return (
    <div className="h-screen flex items-center justify-center bg-gradient-to-br from-red-50 to-red-100 p-8">
      <div className="max-w-4xl w-full bg-white rounded-xl shadow-2xl p-8">
        <div className="flex items-center mb-6">
          <div className="bg-red-100 p-3 rounded-full mr-4">
            <svg className="w-10 h-10 text-red-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.782 16.5c-.77.833.192 2.5 1.732 2.5z" />
            </svg>
          </div>
          <div>
            <h1 className="text-3xl font-bold text-red-700">PostgreSQL Connection Failed</h1>
            <p className="text-gray-600 mt-1">Application cannot start without database connection</p>
          </div>
        </div>
        
        <div className="bg-red-50 border-l-4 border-red-500 p-4 mb-6">
          <div className="flex">
            <div className="flex-shrink-0">
              <svg className="h-5 w-5 text-red-400" fill="currentColor" viewBox="0 0 20 20">
                <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clipRule="evenodd" />
              </svg>
            </div>
            <div className="ml-3">
              <p className="text-sm text-red-700 font-medium">Error: {error}</p>
            </div>
          </div>
        </div>
        
        <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
          <div className="space-y-6">
            <div>
              <h3 className="text-lg font-semibold text-gray-800 mb-3">Connection Parameters</h3>
              <div className="space-y-2">
                <div className="flex justify-between border-b pb-2">
                  <span className="font-medium text-gray-600">Host:</span>
                  <span className="font-mono bg-gray-100 px-2 py-1 rounded">{connectionParams.host}</span>
                </div>
                <div className="flex justify-between border-b pb-2">
                  <span className="font-medium text-gray-600">Port:</span>
                  <span className="font-mono bg-gray-100 px-2 py-1 rounded">{connectionParams.port}</span>
                </div>
                <div className="flex justify-between border-b pb-2">
                  <span className="font-medium text-gray-600">Database:</span>
                  <span className="font-mono bg-gray-100 px-2 py-1 rounded">{connectionParams.database}</span>
                </div>
                <div className="flex justify-between border-b pb-2">
                  <span className="font-medium text-gray-600">Username:</span>
                  <span className="font-mono bg-gray-100 px-2 py-1 rounded">{connectionParams.user}</span>
                </div>
              </div>
            </div>
            
            <div>
              <h3 className="text-lg font-semibold text-gray-800 mb-3">Troubleshooting Steps</h3>
              <ol className="list-decimal list-inside space-y-2 text-gray-700">
                <li>Ensure PostgreSQL is running locally</li>
                <li>Verify the installation script completed successfully</li>
                <li>Check if port 5432 is available and not blocked</li>
                <li>Try using your system username instead of 'postgres'</li>
              </ol>
            </div>
          </div>
          
          <div className="space-y-6">
            <div>
              <h3 className="text-lg font-semibold text-gray-800 mb-3">Quick Commands</h3>
              <div className="space-y-3">
                <div className="bg-gray-50 p-4 rounded-lg">
                  <p className="text-sm text-gray-500 mb-1">Start PostgreSQL (if using standalone installer):</p>
                  <code className="block bg-black text-green-400 p-2 rounded text-sm font-mono">
                    ./standalone-pg-builder.sh --start
                  </code>
                </div>
                
                <div className="bg-gray-50 p-4 rounded-lg">
                  <p className="text-sm text-gray-500 mb-1">Check PostgreSQL status:</p>
                  <code className="block bg-black text-green-400 p-2 rounded text-sm font-mono">
                    ./standalone-pg-builder.sh --status
                  </code>
                </div>
                
                <div className="bg-gray-50 p-4 rounded-lg">
                  <p className="text-sm text-gray-500 mb-1">Test connection manually:</p>
                  <code className="block bg-black text-green-400 p-2 rounded text-sm font-mono">
                    psql -h localhost -p 5432 -U {connectionParams.user} -d postgres
                  </code>
                </div>
              </div>
            </div>
            
            <div>
              <button
                onClick={() => setShowDetails(!showDetails)}
                className="text-blue-600 hover:text-blue-800 font-medium flex items-center mb-3"
              >
                <svg className={`w-5 h-5 mr-2 transition-transform ${showDetails ? 'rotate-90' : ''}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                </svg>
                Technical Details
              </button>
              
              {showDetails && (
                <div className="bg-gray-50 p-4 rounded-lg">
                  <p className="text-sm text-gray-600 mb-2">
                    The application requires a local PostgreSQL instance installed via the standalone-pg-builder.sh script.
                  </p>
                  <p className="text-sm text-gray-600">
                    Connection parameters are derived from: <code className="bg-gray-200 px-1 rounded">standalone-pg-builder.sh</code>
                  </p>
                </div>
              )}
            </div>
            
            <div className="flex space-x-4 pt-4">
              <button
                onClick={onRetry}
                className="flex-1 bg-blue-600 hover:bg-blue-700 text-white font-semibold py-3 px-6 rounded-lg transition-colors flex items-center justify-center"
              >
                <svg className="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
                </svg>
                Retry Connection
              </button>
              
              <button
                onClick={() => window.location.reload()}
                className="flex-1 bg-gray-200 hover:bg-gray-300 text-gray-800 font-semibold py-3 px-6 rounded-lg transition-colors"
              >
                Reload Page
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

// Database Context
export const DatabaseContext = React.createContext<{
  [x: string]: any;
  apiService: DatabaseApiService | null;
  isConnected: boolean;
  connectionError: string | null;
  testConnection: () => Promise<boolean>;
}>({
  apiService: null,
  isConnected: false,
  connectionError: null,
  testConnection: async () => false,
});

// Database Provider Component
const DatabaseProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [apiService] = useState(() => new DatabaseApiService());
  const [isConnected, setIsConnected] = useState(false);
  const [connectionError, setConnectionError] = useState<string | null>(null);
  const [isInitializing, setIsInitializing] = useState(true);

  // Test PostgreSQL connection with parameters from installation script
  const testConnection = useCallback(async (): Promise<boolean> => {
    try {
      setConnectionError(null);
      setIsInitializing(true);

      // First, check if backend is running
      console.log('🔍 Checking backend health...');
      const health = await apiService.testHealth();
      if (!health || (health.status !== 'OK' && health.status !== 'DEGRADED')) {
        throw new Error('Backend server is not responding. Please ensure the backend is running.');
      }

      console.log('✅ Backend is running');

      // Get connection parameters (matching standalone-pg-builder.sh defaults)
      const config = DatabaseApiService.createLocalPostgresConfig();
      
      console.log('🔌 Testing PostgreSQL connection with parameters:', {
        host: config.host,
        port: config.port,
        dbname: config.dbname,
        user: config.user,
        // Don't log password
      });

      // Test connection
      const result = await apiService.testConnection('postgresql', config);
      
      if (result.success) {
        console.log('✅ PostgreSQL connection successful');
        
        // Try to actually connect
        const connectResult = await apiService.connect('postgresql', config);
        if (connectResult.success) {
          console.log('✅ PostgreSQL connection established and stored');
          
          // Store successful user for future connections
          if (config.user && typeof window !== 'undefined') {
            localStorage.setItem('last_db_user', config.user);
          }
          
          setIsConnected(true);
          return true;
        } else {
          throw new Error(`Failed to connect: ${connectResult.error}`);
        }
      } else {
        throw new Error(`Connection test failed: ${result.error}`);
      }
    } catch (error: any) {
      console.error('❌ PostgreSQL connection failed:', error);
      
      let errorMessage = error.message || 'Unknown connection error';
      
      // Provide helpful error messages based on common issues
      if (errorMessage.includes('role') && errorMessage.includes('does not exist')) {
        const systemUser = process.platform === 'win32' ? process.env.USERNAME : process.env.USER;
        errorMessage = `PostgreSQL user not found. Try using your system username: "${systemUser || 'your-username'}"`;
      } else if (errorMessage.includes('ECONNREFUSED') || errorMessage.includes('connection refused')) {
        errorMessage = 'PostgreSQL is not running. Please start it with: ./standalone-pg-builder.sh --start';
      } else if (errorMessage.includes('timeout') || errorMessage.includes('ECONNABORTED')) {
        errorMessage = 'Connection timeout. Ensure PostgreSQL is running on port 5432.';
      } else if (errorMessage.includes('Network Error')) {
        errorMessage = 'Network error. Check if backend server is running.';
      }
      
      setConnectionError(errorMessage);
      setIsConnected(false);
      return false;
    } finally {
      setIsInitializing(false);
    }
  }, [apiService]);

  // Initialize connection on mount
  useEffect(() => {
    const initializeConnection = async () => {
      await testConnection();
    };
    
    initializeConnection();
  }, [testConnection]);

  // Provide database context
  const contextValue = {
    apiService,
    isConnected,
    connectionError,
    testConnection,
  };

  if (isInitializing) {
    return <LoadingSpinner message="Initializing database connection..." />;
  }

  return (
    <DatabaseContext.Provider value={contextValue}>
      {children}
    </DatabaseContext.Provider>
  );
};

// Main App Content Component
const AppContent: React.FC = () => {
  const [appReady, setAppReady] = useState(false);
  const [currentJob, setCurrentJob] = useState<any | null>(null);
  const [showCanvas, setShowCanvas] = useState(false);
  const [jobs, setJobs] = useState<any[]>([]);
  const [reactFlowKey, setReactFlowKey] = useState(0);
  
  // activeCanvasId is string | undefined
  const [activeCanvasId, setActiveCanvasId] = useState<string | undefined>(undefined);
  
  // Use database context
  const databaseContext = React.useContext(DatabaseContext);
  
  // ==================== JOB DESIGN STATE MANAGEMENT ====================
  const [jobDesigns, setJobDesigns] = useState<Record<string, JobDesignState>>({});
  const [activeJobDesignId, setActiveJobDesignId] = useState<string | null>(null);
  
  // Console floating panel state
  const [isConsoleFloating, setIsConsoleFloating] = useState<boolean>(false);

  // Canvas ref and save status
  const canvasRef = useRef<{ forceSave: () => Promise<void> }>(null);
  const [saveStatus, setSaveStatus] = useState<'idle' | 'saving' | 'saved' | 'error'>('idle');
  
  // Auto-reset saved/error status after 2 seconds
  useEffect(() => {
    if (saveStatus === 'saved' || saveStatus === 'error') {
      const timer = setTimeout(() => setSaveStatus('idle'), 2000);
      return () => clearTimeout(timer);
    }
  }, [saveStatus]);

  // Save handler
  const handleSaveDesign = useCallback(async () => {
    if (!canvasRef.current) {
      console.warn('Canvas not ready');
      return;
    }
    setSaveStatus('saving');
    try {
      await canvasRef.current.forceSave();
      setSaveStatus('saved');
    } catch (error) {
      console.error('Save failed:', error);
      setSaveStatus('error');
    }
  }, []);

  // Initialize app on mount (only database check)
  useEffect(() => {
    const initializeApp = async () => {
      try {
        console.log('🚀 Initializing application...');
        
        // Check database connection
        if (!databaseContext.isConnected) {
          if (databaseContext.connectionError) {
            // Connection already failed during initialization
            console.error('❌ Database connection failed during initialization');
            return;
          }
          
          // Try to establish connection
          const connected = await databaseContext.testConnection();
          if (!connected) {
            console.error('❌ Database connection failed, cannot proceed');
            return;
          }
        }
        
        console.log('✅ Database connection verified');
        
        // Start with empty state (no persistence)
        setJobs([]);
        setJobDesigns({});
        
        console.log('✅ Application initialization complete');
        setAppReady(true);
        
      } catch (error) {
        console.error('❌ Error initializing app:', error);
      }
    };

    initializeApp();
  }, [databaseContext]);

  // ==================== CANVAS STATE INITIALIZATION ====================
  useEffect(() => {
    if (currentJob && activeJobDesignId) {
      // Dispatch loaded state to Canvas component
      const loadEvent = new CustomEvent('load-canvas-state', {
        detail: {
          jobId: currentJob.id,
          designId: activeJobDesignId,
          state: null // No saved state
        }
      });
      window.dispatchEvent(loadEvent);
    }
  }, [currentJob, activeJobDesignId]);

  // ==================== JOB DESIGN HANDLERS ====================
  
  const handleJobDesignCreate = useCallback((jobId: string, jobName: string, canvasState: JobDesignState) => {
    console.log('🎯 Creating job design:', jobId, jobName);
    
    const newDesign: JobDesignState = {
      ...canvasState,
      jobId,
      name: jobName,
      createdAt: new Date().toISOString(),
      lastModified: new Date().toISOString(),
      version: '1.0'
    };
    
    setJobDesigns(prev => ({
      ...prev,
      [jobId]: newDesign
    }));
    
    // Set as active design
    setActiveJobDesignId(jobId);
    
    console.log(`✅ Created job design for "${jobName}"`);
  }, []);
  
  const handleJobDesignDelete = useCallback((jobId: string) => {
    console.log('🗑️ Deleting job design:', jobId);
    
    setJobDesigns(prev => {
      const newDesigns = { ...prev };
      delete newDesigns[jobId];
      
      // Clear active design if it's the one being deleted
      if (activeJobDesignId === jobId) {
        setActiveJobDesignId(null);
      }
      
      return newDesigns;
    });
    
    console.log(`✅ Deleted job design: ${jobId}`);
  }, [activeJobDesignId]);
  
  const handleJobDesignSwitch = useCallback((jobId: string | null, design: JobDesignState | null) => {
    console.log('🔄 Switching to job design:', jobId);
    
    setActiveJobDesignId(jobId);
    
    if (jobId && design) {
      // Update local job designs state if design is provided
      setJobDesigns(prev => ({
        ...prev,
        [jobId]: design
      }));
      
      console.log(`✅ Switched to job design: ${jobId}`);
    }
  }, []);
  
  const handleJobDesignUpdate = useCallback((design: JobDesignState) => {
    const jobId = design.jobId;
    if (!jobId) {
      console.error('Cannot update job design without jobId');
      return;
    }
    
    console.log('💾 Updating job design:', jobId);
    
    const updatedDesign: JobDesignState = {
      ...design,
      lastModified: new Date().toISOString()
    };
    
    setJobDesigns(prev => ({
      ...prev,
      [jobId]: updatedDesign
    }));
    
    console.log(`✅ Updated job design: ${jobId}`);
  }, []);
  
  const handleJobDesignDuplicate = useCallback((sourceJobId: string, targetJobId: string, targetJobName: string) => {
    console.log('📋 Duplicating job design:', sourceJobId, 'to', targetJobId);
    
    const sourceDesign = jobDesigns[sourceJobId];
    if (!sourceDesign) {
      console.error('Source job design not found:', sourceJobId);
      return;
    }
    
    // Create a deep copy of the design with new IDs for nodes
    const duplicatedDesign: JobDesignState = {
      ...sourceDesign,
      jobId: targetJobId,
      name: targetJobName,
      createdAt: new Date().toISOString(),
      lastModified: new Date().toISOString(),
      nodes: sourceDesign.nodes ? sourceDesign.nodes.map(node => ({
        ...node,
        id: `${node.type || 'node'}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`
      })) : [],
      // Update connections to use new node IDs if needed
      connections: sourceDesign.connections ? sourceDesign.connections.map(conn => ({
        ...conn,
        id: `conn-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`
      })) : []
    };
    
    setJobDesigns(prev => ({
      ...prev,
      [targetJobId]: duplicatedDesign
    }));
    
    // Switch to the new design
    handleJobDesignSwitch(targetJobId, duplicatedDesign);
    
    console.log(`✅ Duplicated job design to "${targetJobName}"`);
  }, [jobDesigns, handleJobDesignSwitch]);

  // ==================== JOB MANAGEMENT HANDLERS ====================

  // Handle creating a new job WITH job design
  const handleCreateJob = useCallback((jobName: string, jobId?: string) => {
    const newJobId = jobId || `job-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    const newJob: any = {
      id: newJobId,
      name: jobName,
      createdAt: new Date().toISOString(),
      lastModified: new Date().toISOString(),
      state: 'draft',
      nodes: [],
      connections: [],
      variables: []
    };
    
    setCurrentJob(newJob);
    setShowCanvas(true);
    setReactFlowKey(prev => prev + 1);
    
    setJobs(prev => [...prev, newJob]);
    
    // Create empty job design for the new job
    const emptyDesign: JobDesignState = {
      nodes: [],
      edges: [],
      connections: [],
      viewport: { x: 0, y: 0, zoom: 1 },
      validationSummary: null,
      sqlGeneration: {},
      lastModified: new Date().toISOString(),
      createdAt: new Date().toISOString(),
      version: '1.0',
      name: jobName,
      jobId: newJobId
    };
    
    handleJobDesignCreate(newJobId, jobName, emptyDesign);
    
    console.log(`✅ Created new job with design: ${jobName}`);
  }, [handleJobDesignCreate]);

  // Handle loading job WITH its design
  const handleLoadJob = useCallback((jobId: string) => {
    const job = jobs.find(j => j.id === jobId);
    if (job) {
      setCurrentJob(job);
      setShowCanvas(true);
      setReactFlowKey(prev => prev + 1);
      
      // Load the job's design
      const design = jobDesigns[jobId];
      if (design) {
        setActiveJobDesignId(jobId);
        console.log(`📂 Loaded job with design: ${job.name}`);
      } else {
        // Create default design if none exists
        const defaultDesign: JobDesignState = {
          nodes: job.nodes || [],
          edges: [],
          connections: job.connections || [],
          viewport: { x: 0, y: 0, zoom: 1 },
          validationSummary: null,
          sqlGeneration: {},
          lastModified: new Date().toISOString(),
          createdAt: new Date().toISOString(),
          version: '1.0',
          name: job.name,
          jobId: job.id
        };
        
        setJobDesigns(prev => ({
          ...prev,
          [jobId]: defaultDesign
        }));
        setActiveJobDesignId(jobId);
        console.log(`📂 Created default design for job: ${job.name}`);
      }
    }
  }, [jobs, jobDesigns]);

  // Handle closing job AND its design
  const handleCloseJob = useCallback(() => {
    setCurrentJob(null);
    setShowCanvas(false);
    setActiveJobDesignId(null);
    setActiveCanvasId(undefined);
    console.log('📭 Closed current job and design');
  }, []);

  // Handle deleting job AND its design
  const handleDeleteJob = useCallback((jobId: string) => {
    if (!confirm(`Are you sure you want to delete this job and its design? This action cannot be undone.`)) {
      return;
    }
    
    setJobs(prev => prev.filter(job => job.id !== jobId));
    
    // Also delete the job design
    handleJobDesignDelete(jobId);
    
    if (currentJob?.id === jobId) {
      setCurrentJob(null);
      setShowCanvas(false);
      setActiveJobDesignId(null);
      setActiveCanvasId(undefined);
    }
    
    console.log(`🗑️ Deleted job and design: ${jobId}`);
  }, [currentJob, handleJobDesignDelete]);

  // === Handler for selecting a node in the sidebar (job or canvas) ===
  const handleNodeSelect = useCallback((node: RepositoryNode) => {
    if (node.type === 'canvas') {
      const canvasId = node.metadata?.canvasId;
      if (canvasId) {
        setActiveCanvasId(canvasId);
        // Ensure the canvas view is shown (if not already)
        if (!showCanvas) setShowCanvas(true);
      }
    } else if (node.type === 'job') {
      // Clear canvas selection when a job is selected
      setActiveCanvasId(undefined);
    }
  }, [showCanvas]);

  // ==================== NEW: Canvas selection callback from Sidebar ====================
  const handleCanvasSelect = useCallback((canvasId: string) => {
    setActiveCanvasId(canvasId);
    setShowCanvas(true);
  }, []);

  // ==================== CONSOLE FLOATING PANEL HANDLING ====================
  
  const handleToggleConsoleFloating = useCallback(() => {
    const newState = !isConsoleFloating;
    setIsConsoleFloating(newState);
  }, [isConsoleFloating]);

  // Handle console drag/resize events to disable canvas interactions
  useEffect(() => {
    const handleMouseDown = (e: MouseEvent) => {
      const target = e.target as HTMLElement;
      
      // Check if the target is a console resize handle or drag handle
      const isConsoleResizeHandle = target.closest('.cursor-row-resize') || 
                                    target.closest('.cursor-col-resize') || 
                                    target.closest('.cursor-nwse-resize');
      
      const isConsoleDragHandle = target.closest('.cursor-move') || 
                                  target.closest('.console-header');
      
      if ((isConsoleResizeHandle || isConsoleDragHandle) && isConsoleFloating) {
        // Add class to body to disable canvas interactions
        document.body.classList.add('console-interaction-active');
        
        const handleMouseUp = () => {
          document.body.classList.remove('console-interaction-active');
          document.removeEventListener('mouseup', handleMouseUp);
        };
        
        document.addEventListener('mouseup', handleMouseUp);
      }
    };
    
    document.addEventListener('mousedown', handleMouseDown);
    
    return () => {
      document.removeEventListener('mousedown', handleMouseDown);
    };
  }, [isConsoleFloating]);

  // ==================== KEYBOARD SHORTCUT HANDLER ====================
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      // Ctrl+Shift+N for new job
      if (e.ctrlKey && e.shiftKey && e.key === 'N') {
        e.preventDefault();
        const jobName = prompt('Enter new job name:');
        if (jobName && jobName.trim()) {
          handleCreateJob(jobName.trim());
        }
      }

      // Ctrl+Shift+G for generate all SQL
      if (e.ctrlKey && e.shiftKey && e.key === 'G') {
        e.preventDefault();
        if (currentJob) {
          const event = new CustomEvent('generate-all-sql');
          window.dispatchEvent(event);
        }
      }
      
      // Ctrl+Shift+F to toggle console floating mode
      if (e.ctrlKey && e.shiftKey && e.key === 'F') {
        e.preventDefault();
        handleToggleConsoleFloating();
        
        const consoleEvent = new CustomEvent('console-log', {
          detail: {
            message: `Console is now ${!isConsoleFloating ? 'floating' : 'docked'}`,
            type: 'info'
          }
        });
        window.dispatchEvent(consoleEvent);
      }

      // Ctrl+S for save canvas
      if (e.ctrlKey && !e.shiftKey && e.key === 's') {
        e.preventDefault();
        if (currentJob && canvasRef.current) {
          handleSaveDesign();
        }
      }
    };
    
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [currentJob, handleCreateJob, isConsoleFloating, handleToggleConsoleFloating, handleSaveDesign]);

  // Add CSS for drag operations and animations
  useEffect(() => {
    const styleElement = document.createElement('style');
    styleElement.textContent = `
      .drag-in-progress {
        user-select: none !important;
        -webkit-user-select: none !important;
      }
      
      .electron-env {
        background-color: white;
      }

      .react-flow__minimap {
        background-color: white;
        border: 1px solid #e5e7eb;
      }

      .react-flow__controls {
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
      }

      @keyframes fade-in {
        from {
          opacity: 0;
          transform: translateY(-10px);
        }
        to {
          opacity: 1;
          transform: translateY(0);
        }
      }

      .animate-fade-in {
        animation: fade-in 0.3s ease-out;
      }

      /* React Flow customizations */
      .react-flow__node {
        cursor: move !important;
      }

      .react-flow__handle {
        width: 12px;
        height: 12px;
        border: 2px solid white;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.2);
      }

      .react-flow__handle-left {
        left: -6px;
      }

      .react-flow__handle-right {
        right: -6px;
      }

      .react-flow__handle-top {
        top: -6px;
      }

      .react-flow__handle-bottom {
        bottom: -6px;
      }

      .react-flow__edge-path {
        stroke-width: 2;
      }

      /* Custom scrollbar for React Flow */
      .reactflow-wrapper {
        scrollbar-width: thin;
        scrollbar-color: #cbd5e1 #f1f5f9;
      }

      .reactflow-wrapper::-webkit-scrollbar {
        width: 8px;
        height: 8px;
      }

      .reactflow-wrapper::-webkit-scrollbar-track {
        background: #f1f5f9;
      }

      .reactflow-wrapper::-webkit-scrollbar-thumb {
        background-color: #cbd5e1;
        border-radius: 4px;
      }
      
      /* Job design status indicator */
      .job-design-active {
        border-left: 3px solid #3b82f6 !important;
        background-color: #eff6ff !important;
      }
      
      /* Console floating panel isolation */
      .console-interaction-active .reactflow-wrapper,
      .console-interaction-active .react-flow__pane,
      .console-interaction-active .react-flow__nodes,
      .console-interaction-active .react-flow__edges,
      .console-interaction-active .react-flow__viewport {
        pointer-events: none !important;
      }
      
      /* Console resize handles */
      .cursor-row-resize {
        cursor: row-resize !important;
      }
      
      .cursor-col-resize {
        cursor: col-resize !important;
      }
      
      .cursor-nwse-resize {
        cursor: nwse-resize !important;
      }
      
      .cursor-move {
        cursor: move !important;
      }
      
      .cursor-grabbing {
        cursor: grabbing !important;
      }
      
      .cursor-grab {
        cursor: grab !important;
      }
      
      /* Floating console shadow */
      .floating-console-overlay {
        box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3), 
                    0 0 0 1px rgba(255, 255, 255, 0.1) !important;
      }
      
      /* Canvas container fixes for overlay console */
      .canvas-container {
        position: absolute !important;
        width: 100% !important;
        height: 100% !important;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
      }
      
      /* Ensure React Flow fills the canvas container */
      .react-flow {
        position: absolute !important;
        width: 100% !important;
        height: 100% !important;
      }
      
      .react-flow__viewport {
        width: 100% !important;
        height: 100% !important;
      }
      
      .react-flow__renderer {
        width: 100% !important;
        height: 100% !important;
      }
      
      /* Docked console as overlay */
      .docked-console-overlay {
        position: absolute !important;
        bottom: 0;
        left: 0;
        right: 0;
        height: 12rem;
        z-index: 20;
        pointer-events: auto;
        background-color: #111827; /* gray-900 */
        border-top: 1px solid #374151; /* gray-700 */
      }
      
      /* Floating console overlay */
      .floating-console-overlay {
        z-index: 10000 !important;
        pointer-events: auto;
        position: fixed !important;
      }
      
      /* Make sure dragged items maintain their size */
      .react-flow__node {
        transform-box: fill-box;
        transform-origin: center;
      }
      
      .react-flow__node.dragging {
        z-index: 1000 !important;
      }
      
      /* Ensure drag previews are not affected by console */
      .react-flow__handle {
        pointer-events: auto !important;
      }
    `;
    document.head.appendChild(styleElement);

    if (isElectron()) {
      document.body.classList.add('electron-env');
    }
    
    return () => {
      document.head.removeChild(styleElement);
    };
  }, []);

  // Check if database connection failed
  if (!databaseContext.isConnected && databaseContext.connectionError) {
    return (
      <PostgreSQLConnectionError 
        error={databaseContext.connectionError}
        onRetry={databaseContext.testConnection}
      />
    );
  }

  if (!appReady) {
    return <LoadingSpinner message="Initializing React Flow application..." />;
  }

  return (
    <div className="h-screen flex flex-col bg-white overflow-hidden relative">
      {/* Top Toolbar */}
      <div className="flex-none">
        <Suspense fallback={<div className="h-16 bg-gray-100 animate-pulse"></div>}>
          <Toolbar 
            currentJob={currentJob}
            onCloseJob={handleCloseJob}
            onLoadJob={handleLoadJob}
            onDeleteJob={handleDeleteJob}
            jobs={jobs}
            onSaveDesign={handleSaveDesign}
            saveStatus={saveStatus}
            canSaveDesign={!!currentJob}
          />
        </Suspense>
      </div>
      
      {/* Main Content Area */}
      <div className="flex-1 flex overflow-hidden min-h-0 relative">
        {/* Left Sidebar */}
        <div className="flex-none w-64 border-r border-gray-200">
          <Suspense fallback={<div className="h-full bg-gray-50 animate-pulse"></div>}>
            <Sidebar 
              onCreateJob={handleCreateJob}
              currentJob={currentJob}
              onJobDesignCreate={handleJobDesignCreate}
              onJobDesignDelete={handleJobDesignDelete}
              onJobDesignSwitch={handleJobDesignSwitch}
              onJobDesignDuplicate={handleJobDesignDuplicate}
              activeJobDesignId={activeJobDesignId}
              onNodeSelect={handleNodeSelect}
              activeCanvasId={activeCanvasId}
              onCanvasSelect={handleCanvasSelect}  // NEW: callback to load canvas
            />
          </Suspense>
        </div>
        
        {/* Canvas Container - Takes full available space */}
        <div className="flex-1 flex flex-col min-w-0 relative">
          {/* Canvas Area - Always full size */}
          <div className="absolute inset-0 border border-gray-200 rounded-lg m-2">
            {showCanvas ? (
              <Suspense fallback={
                <div className="h-full bg-gradient-to-br from-gray-50 to-gray-100 animate-pulse flex items-center justify-center">
                  <div className="text-center">
                    <div className="inline-block animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mb-4"></div>
                    <p className="text-gray-600">Loading React Flow Canvas...</p>
                  </div>
                </div>
              }>
                <Canvas 
                  ref={canvasRef}
                  key={`${currentJob?.id}-${reactFlowKey}`}
                  job={currentJob}
                  canvasId={activeCanvasId}
                  onJobUpdate={(updates) => {
                    if (currentJob) {
                      const updatedJob = { ...currentJob, ...updates };
                      setCurrentJob(updatedJob);
                      setJobs(prev => prev.map(job => job.id === currentJob.id ? updatedJob : job));
                    }
                  }}
                  jobDesign={activeJobDesignId ? jobDesigns[activeJobDesignId] : undefined}
                  onJobDesignUpdate={handleJobDesignUpdate}
                />
              </Suspense>
            ) : (
              <Suspense fallback={<div className="h-full bg-gray-50 animate-pulse"></div>}>
                <WorkspacePlaceholder onCreateJob={handleCreateJob} />
              </Suspense>
            )}
          </div>
          
          {/* Console - DOCKED MODE (floats over canvas) */}
          {!isConsoleFloating && (
            <div className="docked-console-overlay">
              <Suspense fallback={<div className="h-full bg-gray-100 animate-pulse"></div>}>
                <Console 
                  isFloating={isConsoleFloating}
                  onToggleFloating={handleToggleConsoleFloating}
                />
              </Suspense>
            </div>
          )}
        </div>
        
        {/* Right Panel */}
        <div className="flex-none w-80 border-l border-gray-200">
          <Suspense fallback={<div className="h-full bg-gray-50 animate-pulse"></div>}>
            <RightPanel 
              currentJob={currentJob}
              onJobUpdate={(updates) => {
                if (currentJob) {
                  const updatedJob = { ...currentJob, ...updates };
                  setCurrentJob(updatedJob);
                  setJobs(prev => prev.map(job => job.id === currentJob.id ? updatedJob : job));
                }
              }}
            />
          </Suspense>
        </div>
      </div>

      {/* Console - FLOATING MODE (fixed overlay) */}
      {isConsoleFloating && (
        <Suspense fallback={null}>
          <Console 
            isFloating={isConsoleFloating}
            onToggleFloating={handleToggleConsoleFloating}
          />
        </Suspense>
      )}
    </div>
  );
};

// Main App Component
const App: React.FC = () => {
  const [globalDragState, setGlobalDragState] = useState<{
    activeId: UniqueIdentifier | null;
    activeData: any;
    isSidebarDrag: boolean;
    isCanvasDrag: boolean;
  }>({
    activeId: null,
    activeData: null,
    isSidebarDrag: false,
    isCanvasDrag: false,
  });

  const sensors = useSensors(
    useSensor(PointerSensor, {
      activationConstraint: {
        distance: 3,
      },
    }),
    useSensor(KeyboardSensor, {
      coordinateGetter: sortableKeyboardCoordinates,
    })
  );

  const handleGlobalDragStart = (event: DragStartEvent) => {
    const { active } = event;
    const dragData = active.data.current;
    
    const isSidebarDrag = dragData?.source === 'sidebar' || 
                         dragData?.componentType === 'repository-node';
    const isCanvasDrag = dragData?.source === 'canvas' || 
                        dragData?.type === 'canvas-node';
    
    document.body.classList.add('drag-in-progress');
    
    setGlobalDragState({
      activeId: active.id,
      activeData: dragData,
      isSidebarDrag,
      isCanvasDrag,
    });
  };

  const handleGlobalDragEnd = (event: DragEndEvent) => {
    const { over } = event;
    
    document.body.classList.remove('drag-in-progress');

    if (globalDragState.isSidebarDrag && over?.data?.current?.type === "canvas-drop-target") {
      console.log('Sidebar item dropped on canvas:', globalDragState.activeData);
    }

    setGlobalDragState({
      activeId: null,
      activeData: null,
      isSidebarDrag: false,
      isCanvasDrag: false,
    });
  };

  const handleGlobalDragCancel = () => {
    document.body.classList.remove('drag-in-progress');
    setGlobalDragState({
      activeId: null,
      activeData: null,
      isSidebarDrag: false,
      isCanvasDrag: false,
    });
  };

  return (
    <React.Suspense fallback={<LoadingSpinner />}>
      <ErrorBoundary>
        <DndContext
          sensors={sensors}
          collisionDetection={closestCenter}
          onDragStart={handleGlobalDragStart}
          onDragEnd={handleGlobalDragEnd}
          onDragCancel={handleGlobalDragCancel}
        >
          {/* Wrap the entire application with DatabaseProvider */}
          <DatabaseProvider>
            {/* Wrap the entire application with CanvasProvider */}
            <CanvasProvider>
              <ReactFlowProvider>
                <AppContent />
                
                {/* Global Drag Overlay */}
                <DragOverlay 
                  dropAnimation={{
                    duration: 250,
                    easing: 'cubic-bezier(0.18, 0.67, 0.6, 1.22)',
                  }}
                  zIndex={9999}
                >
                  {globalDragState.activeId && (
                    <GlobalDragPreview 
                      data={globalDragState.activeData} 
                      isSidebarDrag={globalDragState.isSidebarDrag}
                      isCanvasDrag={globalDragState.isCanvasDrag}
                    />
                  )}
                </DragOverlay>
              </ReactFlowProvider>
            </CanvasProvider>
          </DatabaseProvider>
        </DndContext>
      </ErrorBoundary>
    </React.Suspense>
  );
};

export default App;