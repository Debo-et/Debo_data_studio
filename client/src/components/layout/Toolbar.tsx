// src/components/layout/Toolbar.tsx
import React, { useState, useCallback, useEffect } from 'react';
import { Button } from '../ui/Button';
import { Bug, PlayCircle, RefreshCw, FileText, Save } from 'lucide-react';
import { useCanvas } from '../../pages/CanvasContext';
import { Node, Edge } from 'reactflow';
import { sqlGenerator, SQLGenerationResult } from '../../services/sql-generation.service';
import { useAppDispatch } from '../../hooks';
import { addLog } from '../../store/slices/logsSlice';

interface JobConfig {
  id: string;
  name: string;
  state: 'draft' | 'running' | 'completed' | 'failed';
}

interface ToolbarProps {
  currentJob: JobConfig | null;
  onConsoleLog?: (message: string, type?: 'info' | 'error' | 'warning' | 'success' | 'debug') => void;
  onSaveDesign?: () => void;
  saveStatus?: 'idle' | 'saving' | 'saved' | 'error';
  canSaveDesign?: boolean;
}

const Toolbar: React.FC<ToolbarProps> = ({
  currentJob,
  onConsoleLog,
  onSaveDesign,
  saveStatus = 'idle'}) => {
  const [isDebugging, setIsDebugging] = useState(false);
  const [isRunning, setIsRunning] = useState(false);
  
  const { canvasData, requestCanvasData } = useCanvas();
  
  const [localCanvasData, setLocalCanvasData] = useState({
    nodes: [] as any[],
    edges: [] as any[],
    connections: [] as any[]
  });
  
  const [canvasSyncStatus, setCanvasSyncStatus] = useState<'connected' | 'disconnected' | 'syncing'>('disconnected');
  
  const [generatedSQL, setGeneratedSQL] = useState<string>('');
  const [sqlGenerationResult, setSqlGenerationResult] = useState<SQLGenerationResult | null>(null);

  const dispatch = useAppDispatch();

  const logToConsole = useCallback((message: string, type: 'info' | 'error' | 'warning' | 'success' | 'debug' = 'info') => {
    const levelMap: Record<typeof type, 'ERROR' | 'INFO' | 'WARN' | 'DEBUG' | 'SUCCESS'> = {
      error: 'ERROR',
      info: 'INFO',
      warning: 'WARN',
      success: 'SUCCESS',
      debug: 'DEBUG'
    };
    
    const logEntry = {
      id: `log-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
      timestamp: Date.now(),
      level: levelMap[type],
      message,
      source: currentJob ? currentJob.name : 'Toolbar'
    };
    dispatch(addLog(logEntry));
    
    if (onConsoleLog) {
      onConsoleLog(message, type);
    }
    
    if (process.env.NODE_ENV === 'development') {
      const logMethods = {
        'error': console.error,
        'warning': console.warn,
        'info': console.info,
        'debug': console.debug,
        'success': console.log
      };
      (logMethods[type] || console.log)(`[${type.toUpperCase()}] ${message}`);
    }
  }, [dispatch, currentJob, onConsoleLog]);

  useEffect(() => {
    if (canvasData) {
      setLocalCanvasData({
        nodes: canvasData.nodes || [],
        edges: canvasData.edges || [],
        connections: canvasData.connections || []
      });
      setCanvasSyncStatus('connected');
    }
  }, [canvasData]);

  const requestDataFromCanvas = useCallback(async (dataType: 'all' | 'nodes' | 'connections' = 'all') => {
    const requestId = `req-${Date.now()}`;
    setCanvasSyncStatus('syncing');
    logToConsole(`📤 Requesting ${dataType} data from Canvas...`, 'debug');
    
    const requestEvent = new CustomEvent('request-canvas-data', {
      detail: {
        requestId,
        dataType,
        requester: 'Toolbar',
        timestamp: new Date().toISOString()
      }
    });
    window.dispatchEvent(requestEvent);
    
    try {
      const contextData = requestCanvasData();
      if (contextData) {
        logToConsole(`📥 Received canvas data from context`, 'debug');
        setLocalCanvasData({
          nodes: contextData.nodes || [],
          edges: contextData.edges || [],
          connections: contextData.connections || []
        });
        setCanvasSyncStatus('connected');
        logToConsole(`✅ Canvas sync complete: ${contextData.nodes.length} nodes, ${contextData.edges.length} edges`, 'success');
        return contextData;
      }
    } catch (error) {
      logToConsole(`Failed to get data from context: ${error}`, 'error');
    }
    
    return new Promise((resolve) => {
      const handleResponse = (event: Event) => {
        const customEvent = event as CustomEvent<{
          requestId: string;
          data: any;
          timestamp: string;
          source: string;
        }>;
        
        if (customEvent.detail.requestId === requestId) {
          window.removeEventListener('canvas-data-response', handleResponse as EventListener);
          setLocalCanvasData(prev => ({
            ...prev,
            ...customEvent.detail.data
          }));
          setCanvasSyncStatus('connected');
          logToConsole(`✅ Received canvas data for request: ${requestId}`, 'debug');
          resolve(customEvent.detail.data);
        }
      };
      
      window.addEventListener('canvas-data-response', handleResponse as EventListener);
      
      setTimeout(() => {
        window.removeEventListener('canvas-data-response', handleResponse as EventListener);
        logToConsole('⚠️ Canvas data request timed out', 'warning');
        setCanvasSyncStatus('disconnected');
        resolve(null);
      }, 3000);
    });
  }, [requestCanvasData, logToConsole]);

  const handleDebug = async () => {
    if (!currentJob) {
      logToConsole('No active job to debug', 'error');
      return;
    }

    await requestDataFromCanvas('all');
    
    if (localCanvasData.nodes.length === 0) {
      logToConsole('Empty workflow graph. Add nodes to the canvas first.', 'warning');
      return;
    }

    setIsDebugging(true);
    
    logToConsole('🚀 STARTING DEBUG ANALYSIS', 'success');
    logToConsole(`📁 Job: ${currentJob.name}`, 'info');
    logToConsole(`📊 Graph: ${localCanvasData.nodes.length} nodes, ${localCanvasData.edges.length} edges, ${localCanvasData.connections.length} connections`, 'info');
    logToConsole('─'.repeat(50), 'debug');
    
    try {
      logToConsole('🔍 Parsing workflow graph for SQL generation...', 'info');
      
      const inputNodes = localCanvasData.nodes.filter((node: Node) => {
        const data = node.data;
        return data?.componentType === 'INPUT' && 
               data?.configuration?.type === 'INPUT';
      });
      
      if (inputNodes.length === 0) {
        logToConsole('❌ No input data source found. Add at least one input component (file or database).', 'error');
        return;
      }
      
      const outputNodes = localCanvasData.nodes.filter((node: Node) => {
        const data = node.data;
        return data?.componentType === 'OUTPUT' && 
               data?.configuration?.type === 'OUTPUT';
      });
      
      if (outputNodes.length === 0) {
        logToConsole('❌ No output destination found. Add at least one output component.', 'error');
        return;
      }
      
      logToConsole(`📥 Found ${inputNodes.length} input source(s)`, 'success');
      logToConsole(`📤 Found ${outputNodes.length} output destination(s)`, 'success');
      
      logToConsole('🧱 Building PostgreSQL SQL from workflow components...', 'info');
      const sqlResult = sqlGenerator.generateSQLFromGraph(
        localCanvasData.nodes as Node[],
        localCanvasData.edges as Edge[]
      );
      console.log('📝 [Toolbar Debug] Generated SQL:', sqlResult.sql);
      setSqlGenerationResult(sqlResult);
      
      if (sqlResult.success) {
        logToConsole('✅ SQL GENERATION SUCCESSFUL', 'success');
        logToConsole(`📝 Generated ${sqlResult.sql.split('SELECT').length - 1} SELECT statements`, 'info');
        logToConsole(`📝 Generated ${sqlResult.sql.split('INSERT').length - 1} INSERT statements`, 'info');
         logToConsole('📄 Full generated SQL:', 'info'); // optional header
  logToConsole(sqlResult.sql, 'info');
        if (sqlResult.executionPlan) {
          logToConsole('📋 EXECUTION PLAN:', 'info');
          sqlResult.executionPlan.steps.forEach(step => {
            logToConsole(`  Step ${step.step}: ${step.nodeType} - ${step.description}`, 'debug');
          });
          logToConsole(`  Estimated rows: ${sqlResult.executionPlan.estimatedRows}`, 'info');
          logToConsole(`  Total steps: ${sqlResult.executionPlan.steps.length}`, 'info');
        }
        
        if (sqlResult.warnings.length > 0) {
          logToConsole('⚠️ WARNINGS:', 'warning');
          sqlResult.warnings.forEach(warning => {
            logToConsole(`  • ${warning}`, 'warning');
          });
        }
        
        if (sqlResult.messages.length > 0) {
          sqlResult.messages.forEach(message => {
            logToConsole(`  ℹ️ ${message}`, 'info');
          });
        }
        
        logToConsole('💾 Saving generated SQL script...', 'info');
        const savedFilename = await sqlGenerator.saveSQLScript(sqlResult.sql, currentJob.name);
        if (savedFilename) {
          logToConsole(`✅ SQL script saved as: ${savedFilename}`, 'success');
          
          localStorage.setItem(`generated_sql_${currentJob.id}`, sqlResult.sql);
          localStorage.setItem(`generated_sql_timestamp_${currentJob.id}`, new Date().toISOString());
          
          setGeneratedSQL(sqlResult.sql);
        }
        
        const sqlPreview = sqlResult.sql.substring(0, 500) + (sqlResult.sql.length > 500 ? '...' : '');
        logToConsole('📄 Generated SQL (preview):', 'debug');
        sqlPreview.split('\n').forEach(line => {
          if (line.trim()) {
            logToConsole(line, 'info');
          }
        });
        
        if (sqlResult.sql.length > 500) {
          logToConsole(`... (${sqlResult.sql.length - 500} more characters)`, 'info');
        }
        
        logToConsole('─'.repeat(50), 'debug');
        logToConsole('✅ DEBUG ANALYSIS COMPLETE', 'success');
        logToConsole('Workflow validated and PostgreSQL script generated successfully', 'success');
        
      } else {
        logToConsole('❌ SQL GENERATION FAILED', 'error');
        sqlResult.errors.forEach(error => {
          logToConsole(`  • ${error}`, 'error');
        });
        
        if (sqlResult.messages.length > 0) {
          sqlResult.messages.forEach(message => {
            logToConsole(`  ℹ️ ${message}`, 'info');
          });
        }
        
        if (sqlResult.warnings.length > 0) {
          logToConsole('⚠️ WARNINGS:', 'warning');
          sqlResult.warnings.forEach(warning => {
            logToConsole(`  • ${warning}`, 'warning');
          });
        }
      }
      
    } catch (error: any) {
      logToConsole(`❌ Debug analysis failed: ${error.message || 'Unknown error'}`, 'error');
      logToConsole(`Stack trace: ${error.stack}`, 'debug');
    } finally {
      setIsDebugging(false);
    }
  };

  const handleRun = async () => {
  await requestDataFromCanvas('all');

  if (localCanvasData.nodes.length === 0) {
    logToConsole('Empty workflow graph. Add nodes to the canvas first.', 'warning');
    return;
  }

  const jobName = currentJob?.name ?? 'Unknown Job';
  if (!window.confirm(`Run job "${jobName}"?\n\nThis will generate the SQL script and mark the job as executed.`)) {
    logToConsole('❌ Job execution cancelled by user', 'warning');
    return;
  }

  setIsRunning(true);

  logToConsole('🚀 STARTING JOB EXECUTION', 'success');
  logToConsole(`📁 Job: ${jobName}`, 'info');
  logToConsole(`📊 Graph: ${localCanvasData.nodes.length} nodes, ${localCanvasData.connections.length} connections`, 'info');
  logToConsole('─'.repeat(50), 'debug');

  // Dispatch event to Canvas
  window.dispatchEvent(new CustomEvent('toolbar-run', {
    detail: {
      jobId: currentJob?.id,
      jobName,
      nodes: localCanvasData.nodes,
      edges: localCanvasData.edges,
      connections: localCanvasData.connections
    }
  }));

  // Listen for completion
  const onRunComplete = (event: Event) => {
    const customEvent = event as CustomEvent;
    const { success, sql, errors } = customEvent.detail;
    setIsRunning(false);
    if (success) {
      logToConsole('✅ JOB EXECUTION COMPLETE', 'success');
      if (sql) {
        logToConsole('📄 Generated SQL length: ' + sql.length, 'info');
      }
    } else {
      logToConsole('❌ JOB EXECUTION FAILED', 'error');
      if (errors?.length) {
        errors.forEach((err: any) => logToConsole(`  • ${err.message}`, 'error'));
      }
    }
    window.removeEventListener('run-complete', onRunComplete as EventListener);
  };

  window.addEventListener('run-complete', onRunComplete as EventListener);

  // Safety timeout
  setTimeout(() => {
    window.removeEventListener('run-complete', onRunComplete as EventListener);
    setIsRunning(false);
    logToConsole('⚠️ Run operation timed out', 'warning');
  }, 30000);
};
  const handleViewSQL = () => {
    if (!generatedSQL && sqlGenerationResult?.sql) {
      setGeneratedSQL(sqlGenerationResult.sql);
    }
    
    if (!generatedSQL) {
      logToConsole('No SQL has been generated yet. Run Debug first.', 'warning');
      return;
    }
    
    logToConsole('📄 Opening generated SQL in new tab...', 'info');
    
    const blob = new Blob([generatedSQL], { type: 'application/sql' });
    const url = URL.createObjectURL(blob);
    const newWindow = window.open(url, '_blank');
    
    if (!newWindow) {
      logToConsole('❌ Popup blocked. Please allow popups for this site.', 'error');
      const a = document.createElement('a');
      a.href = url;
      a.download = `generated_${currentJob?.name.replace(/[^a-zA-Z0-9]/g, '_') ?? 'unknown'}_${Date.now()}.sql`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      logToConsole('📥 SQL script downloaded instead', 'success');
    }
    
    URL.revokeObjectURL(url);
  };

  const getCanvasStats = () => {
    return {
      nodeCount: localCanvasData.nodes.length,
      edgeCount: localCanvasData.edges.length,
      connectionCount: localCanvasData.connections.length
    };
  };

  const canvasStats = getCanvasStats();
  const hasValidSQL = generatedSQL || sqlGenerationResult?.success;

  return (
    <div className="flex items-center justify-between px-4 py-2.5 bg-gradient-to-r from-gray-900 to-gray-800 border-b border-gray-700 shadow-xl">
      {/* Left section - Job info and sync */}
      <div className="flex items-center space-x-4">
        <div className={`flex items-center text-sm ${currentJob ? '' : 'opacity-50'}`}>
          <div className={`w-2 h-2 rounded-full mr-2 ${currentJob ? (
            canvasSyncStatus === 'connected' ? 'bg-green-400 animate-pulse' :
            canvasSyncStatus === 'syncing' ? 'bg-yellow-400 animate-pulse' :
            'bg-red-400'
          ) : 'bg-gray-500'}`}></div>
          <span className={`font-medium ${currentJob ? 'text-gray-300' : 'text-gray-500'}`}>
            {currentJob ? `Job: ${currentJob.name.substring(0, 20)}${currentJob.name.length > 20 ? '...' : ''}` : 'No Job'}
          </span>
        </div>

        <div className="w-px h-6 bg-gray-700"></div>

        <Button
          variant="ghost"
          size="sm"
          className="h-8 px-2.5 text-gray-400 hover:text-white hover:bg-gray-700"
          onClick={() => requestDataFromCanvas('all')}
          disabled={!currentJob}
          title="Sync with Canvas"
        >
          <RefreshCw className={`h-4 w-4 ${canvasSyncStatus === 'syncing' ? 'animate-spin' : ''}`} />
        </Button>
      </div>

      {/* Center section - Main actions */}
      <div className="flex items-center space-x-4">
        {/* Save button - always enabled */}
        <Button
          variant="ghost"
          size="sm"
          className={`h-8 px-3 ${
            saveStatus === 'saving' ? 'text-yellow-400 bg-yellow-400/10' :
            saveStatus === 'saved' ? 'text-green-400 bg-green-400/10' :
            saveStatus === 'error' ? 'text-red-400 bg-red-400/10' :
            currentJob ? 'text-blue-400 hover:text-blue-300 hover:bg-gray-700' : 'text-gray-500'
          }`}
          onClick={onSaveDesign}
          title={currentJob ? "Save current canvas (Ctrl+S)" : "No job selected – saving may not work"}
        >
          {saveStatus === 'saving' ? (
            <>
              <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-current mr-2"></div>
              <span>Saving...</span>
            </>
          ) : saveStatus === 'saved' ? (
            <>
              <svg className="h-4 w-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
              </svg>
              <span>Saved</span>
            </>
          ) : saveStatus === 'error' ? (
            <>
              <svg className="h-4 w-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
              <span>Error</span>
            </>
          ) : (
            <>
              <Save className="h-4 w-4 mr-2" />
              <span>Save</span>
            </>
          )}
        </Button>

        {hasValidSQL && (
          <Button
            variant="ghost"
            size="sm"
            className="h-8 px-3 text-purple-400 hover:text-purple-300 hover:bg-gray-700"
            onClick={handleViewSQL}
            disabled={!hasValidSQL}
            title="View generated SQL script"
          >
            <FileText className="h-4 w-4 mr-2" />
            <span>View SQL</span>
          </Button>
        )}

        {/* Debug button - always enabled */}
        <Button
          size="sm"
          className={`h-8 px-3 ${
            isDebugging ? 'bg-blue-700 text-white' : 'bg-gradient-to-r from-blue-600 to-cyan-600 hover:from-blue-700 hover:to-cyan-700 text-white shadow-lg'
          } ${!currentJob ? 'opacity-50' : ''}`}
          onClick={handleDebug}
          title={currentJob ? "Debug - Generate PostgreSQL SQL from workflow" : "No job selected – debug may not work properly"}
        >
          {isDebugging ? (
            <>
              <div className="animate-spin rounded-full h-3 w-3 border-b-2 border-white mr-2"></div>
              <span className="whitespace-nowrap">Debugging...</span>
            </>
          ) : (
            <>
              <Bug className="h-4 w-4 mr-2" />
              <span className="whitespace-nowrap">Debug</span>
            </>
          )}
        </Button>

        {/* Run button - always enabled */}
        <Button
          size="sm"
          className={`h-8 px-3 ${
            isRunning ? 'bg-green-700 text-white' : 
            'bg-gradient-to-r from-green-600 to-emerald-600 hover:from-green-700 hover:to-emerald-700 text-white shadow-lg'
          } ${!currentJob ? 'opacity-50' : ''}`}
          onClick={handleRun}
          title={!currentJob ? "No job selected – run will still attempt to generate SQL" : "Run - Generate (if needed) and simulate execution of workflow"}
        >
          {isRunning ? (
            <>
              <div className="animate-spin rounded-full h-3 w-3 border-b-2 border-white mr-2"></div>
              <span className="whitespace-nowrap">Running...</span>
            </>
          ) : (
            <>
              <PlayCircle className="h-4 w-4 mr-2" />
              <span className="whitespace-nowrap">Run</span>
            </>
          )}
        </Button>
      </div>

      {/* Right section - Stats and info */}
      <div className="flex items-center space-x-4">
        <div className="hidden md:flex items-center text-xs text-gray-400">
          <span className="mr-3">Nodes: {canvasStats.nodeCount}</span>
          <span>Connections: {canvasStats.connectionCount}</span>
        </div>
        
        {sqlGenerationResult && (
          <div className={`hidden sm:flex text-xs px-2 py-1 rounded ${
            sqlGenerationResult.success ? 'bg-green-400/10 text-green-400 border border-green-400/20' :
            'bg-red-400/10 text-red-400 border border-red-400/20'
          }`}>
            {sqlGenerationResult.success ? 'SQL Ready' : 'SQL Failed'}
          </div>
        )}
        
        {currentJob && (
          <div className={`text-xs px-2 py-1 rounded ${
            currentJob.state === 'completed' ? 'bg-green-400/10 text-green-400' :
            currentJob.state === 'running' ? 'bg-yellow-400/10 text-yellow-400' :
            currentJob.state === 'failed' ? 'bg-red-400/10 text-red-400' :
            'bg-blue-400/10 text-blue-400'
          }`}>
            {currentJob.state.toUpperCase()}
          </div>
        )}
      </div>
    </div>
  );
};

export default Toolbar;