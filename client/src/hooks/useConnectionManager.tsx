// src/hooks/useConnectionManager.ts

import { useState, useCallback, useMemo, useRef, useEffect } from 'react';
import React from 'react'; // Added React import for ConnectionErrorBoundary
import { useAppDispatch, useAppSelector } from '../store/hooks';
import {
  CanvasNode,
  CanvasConnection,
  ConnectionPort,
  ConnectionStatus,
  NodePosition,
  ConnectionValidationResult,
  SchemaMapping
} from '../types/pipeline-types';
import {
  ConnectionValidator,
  ValidationIssue,
  PortCompatibilityResult,
  PortCompatibilityError
} from '../utils/connection-validator';
import {
  createConnection,
  updateConnection,
  deleteConnection,
  deleteMultipleConnections,
  validateConnection,
  batchValidateConnections,
  selectAllConnections} from '../store/slices/connectionsSlice';
import {
  generatePipelineSQL,
  updateSQLCache
} from '../store/slices/sqlGenerationSlice';

// ==================== TYPES & INTERFACES ====================

export interface ConnectionDragState {
  isDragging: boolean;
  sourceNodeId: string | null;
  sourcePortId: string | null;
  targetNodeId: string | null;
  targetPortId: string | null;
  dragStartPosition: NodePosition | null;
  currentPosition: NodePosition | null;
  snapToPort: ConnectionPort | null;
  validationResult: PortCompatibilityResult | null;
}

export interface ConnectionEventHandlers {
  onConnectionStart: (sourceNodeId: string, sourcePortId: string, position: NodePosition) => void;
  onConnectionDrag: (position: NodePosition) => void;
  onConnectionEnd: (targetNodeId: string | null, targetPortId: string | null) => Promise<ConnectionResult>;
  onConnectionCancel: () => void;
}

export interface ConnectionResult {
  success: boolean;
  connection?: CanvasConnection;
  errors: ValidationIssue[];
  warnings: string[];
  suggestions: string[];
}

export interface ConnectionQuery {
  nodeId?: string;
  portId?: string;
  status?: ConnectionStatus;
  sourceNodeId?: string;
  targetNodeId?: string;
}

export interface ValidationOptions {
  validateSchema?: boolean;
  validatePerformance?: boolean;
  debounceMs?: number;
  autoValidate?: boolean;
}

export interface UseConnectionManagerReturn extends ConnectionEventHandlers {
  [x: string]: any;
  // State
  dragState: ConnectionDragState;
  isConnecting: boolean;
  activeConnectionId: string | null;
  validationQueue: string[];
  
  // Connection Operations
  connections: Record<string, CanvasConnection>;
  createConnection: (payload: {
    sourceNodeId: string;
    sourcePortId: string;
    targetNodeId: string;
    targetPortId: string;
    metadata?: any;
  }) => Promise<ConnectionResult>;
  updateConnection: (connectionId: string, updates: Partial<CanvasConnection>) => Promise<ConnectionResult>;
  deleteConnection: (connectionId: string) => Promise<boolean>;
  batchDeleteConnections: (connectionIds: string[]) => Promise<number>;
  validateConnection: (connectionId: string, options?: ValidationOptions) => Promise<ConnectionValidationResult>;
  validateAllConnections: (options?: ValidationOptions) => Promise<Map<string, ConnectionValidationResult>>;
  
  // Utility Functions
  findConnections: (query: ConnectionQuery) => CanvasConnection[];
  getNodeConnections: (nodeId: string) => {
    incoming: CanvasConnection[];
    outgoing: CanvasConnection[];
    all: CanvasConnection[];
  };
  getUpstreamNodes: (nodeId: string) => CanvasNode[];
  getDownstreamNodes: (nodeId: string) => CanvasNode[];
  getExecutionOrder: () => string[];
  hasCircularDependencies: () => boolean;
  getCircularDependencies: () => string[][];
  suggestSchemaMappings: (sourceNodeId: string, targetNodeId: string) => SchemaMapping[];
  
  // Event Handlers
  onConnectionStatusChange: (connectionId: string, status: ConnectionStatus) => void;
  onConnectionError: (connectionId: string, error: Error, recoveryAction?: () => void) => void;
  onValidationComplete: (results: Map<string, ConnectionValidationResult>) => void;
  
  // Performance
  clearCache: () => void;
  getStats: () => ConnectionStats;
}

export interface ConnectionStats {
  totalConnections: number;
  validConnections: number;
  invalidConnections: number;
  pendingValidations: number;
  circularDependencies: number;
  averageValidationTime: number;
  cacheHits: number;
  cacheMisses: number;
}

export interface UseConnectionManagerProps {
  nodes: CanvasNode[];
  options?: {
    autoValidate?: boolean;
    validationDebounceMs?: number;
    enableSnapToPort?: boolean;
    snapDistance?: number;
    maxConnectionsPerPort?: number;
    onConnectionCreated?: (connection: CanvasConnection) => void;
    onConnectionDeleted?: (connectionId: string) => void;
    onValidationResults?: (results: Map<string, ConnectionValidationResult>) => void;
  };
}

// ==================== CONSTANTS ====================

const DEFAULT_SNAP_DISTANCE = 30; // pixels
const DEFAULT_DEBOUNCE_MS = 300;

// ==================== HOOK IMPLEMENTATION ====================

export function useConnectionManager({
  nodes,
  options = {}
}: UseConnectionManagerProps): UseConnectionManagerReturn {
  const {
    autoValidate = true,
    validationDebounceMs = DEFAULT_DEBOUNCE_MS,
    enableSnapToPort = true,
    snapDistance = DEFAULT_SNAP_DISTANCE,
    onConnectionCreated,
    onConnectionDeleted,
    onValidationResults
  } = options;

  // ==================== STATE & REFS ====================
  
  const dispatch = useAppDispatch();
  
  // Fixed: Use proper selector to get connections from the Redux state
  const connections = useAppSelector(state => 
    selectAllConnections(state.connections)
  );
  
  const connectionsMap = useMemo(() => 
    connections.reduce((acc, conn) => {
      acc[conn.id] = conn;
      return acc;
    }, {} as Record<string, CanvasConnection>),
    [connections]
  );

  const [dragState, setDragState] = useState<ConnectionDragState>({
    isDragging: false,
    sourceNodeId: null,
    sourcePortId: null,
    targetNodeId: null,
    targetPortId: null,
    dragStartPosition: null,
    currentPosition: null,
    snapToPort: null,
    validationResult: null
  });

  const [activeConnectionId] = useState<string | null>(null);
  const [validationQueue, setValidationQueue] = useState<string[]>([]);
  const [stats, setStats] = useState<ConnectionStats>({
    totalConnections: 0,
    validConnections: 0,
    invalidConnections: 0,
    pendingValidations: 0,
    circularDependencies: 0,
    averageValidationTime: 0,
    cacheHits: 0,
    cacheMisses: 0
  });

  // Refs for performance and cleanup
  const validationTimeoutRef = useRef<NodeJS.Timeout>();
  const nodeCacheRef = useRef<Map<string, CanvasNode>>(new Map());
  const portCacheRef = useRef<Map<string, ConnectionPort>>(new Map());
  const validationCacheRef = useRef<Map<string, ConnectionValidationResult>>(new Map());
  const executionOrderCacheRef = useRef<string[] | null>(null);
  const circularDepsCacheRef = useRef<{ hasCycles: boolean; cycles: string[][] } | null>(null);
  
  // Stats refs
  const statsRef = useRef(stats);
  const validationTimesRef = useRef<number[]>([]);

  // ==================== UTILITY FUNCTIONS ====================

  const getNodeById = useCallback((nodeId: string): CanvasNode | undefined => {
    if (nodeCacheRef.current.has(nodeId)) {
      return nodeCacheRef.current.get(nodeId);
    }
    
    const node = nodes.find(n => n.id === nodeId);
    if (node) {
      nodeCacheRef.current.set(nodeId, node);
    }
    
    return node;
  }, [nodes]);

  const getPortById = useCallback((nodeId: string, portId: string): ConnectionPort | undefined => {
    const cacheKey = `${nodeId}:${portId}`;
    if (portCacheRef.current.has(cacheKey)) {
      return portCacheRef.current.get(cacheKey);
    }
    
    const node = getNodeById(nodeId);
    const port = node?.connectionPorts?.find(p => p.id === portId);
    
    if (port) {
      portCacheRef.current.set(cacheKey, port);
    }
    
    return port;
  }, [getNodeById]);

  const findNearestPort = useCallback((
    position: NodePosition,
    excludeNodeId?: string
  ): { nodeId: string; port: ConnectionPort; distance: number } | null => {
    let nearestPort: { nodeId: string; port: ConnectionPort; distance: number } | null = null;
    
    nodes.forEach(node => {
      if (node.id === excludeNodeId) return;
      
      node.connectionPorts?.forEach(port => {
        // Calculate port position based on node position and port side
        let portX = node.position.x;
        let portY = node.position.y;
        
        switch (port.side) {
          case 'left':
            portX -= 10;
            portY += (node.size.height * port.position) / 100;
            break;
          case 'right':
            portX += node.size.width + 10;
            portY += (node.size.height * port.position) / 100;
            break;
          case 'top':
            portX += (node.size.width * port.position) / 100;
            portY -= 10;
            break;
          case 'bottom':
            portX += (node.size.width * port.position) / 100;
            portY += node.size.height + 10;
            break;
        }
        
        const distance = Math.sqrt(
          Math.pow(position.x - portX, 2) + Math.pow(position.y - portY, 2)
        );
        
        if (distance <= snapDistance && (!nearestPort || distance < nearestPort.distance)) {
          nearestPort = {
            nodeId: node.id,
            port,
            distance
          };
        }
      });
    });
    
    return nearestPort;
  }, [nodes, snapDistance]);

  const validatePortCompatibilityInRealTime = useCallback((
    sourceNodeId: string,
    sourcePortId: string,
    targetNodeId: string | null,
    targetPortId: string | null
  ): PortCompatibilityResult => {
    if (!targetNodeId || !targetPortId) {
      return {
        isValid: false,
        errors: [],
        warnings: ['No target port selected']
      };
    }

    const sourceNode = getNodeById(sourceNodeId);
    const targetNode = getNodeById(targetNodeId);
    
if (!sourceNode || !targetNode) {
  const error = new PortCompatibilityError(
    `Source or target node not found`,
    sourcePortId,
    targetPortId || '',
    'TYPE_MISMATCH'  // Use one of the valid violation types
  );
  
  return {
    isValid: false,
    errors: [error],
    warnings: []
  };
    }

    const existingConnections = connections.filter(
      conn => conn.sourceNodeId === sourceNodeId && conn.sourcePortId === sourcePortId
    );

    return ConnectionValidator.validatePortCompatibility(
      sourceNode,
      sourcePortId,
      targetNode,
      targetPortId,
      existingConnections
    );
  }, [getNodeById, connections]);

  const updateStats = useCallback((updates: Partial<ConnectionStats>) => {
    statsRef.current = { ...statsRef.current, ...updates };
    setStats(statsRef.current);
  }, []);

  // ==================== CONNECTION LIFECYCLE ====================

  const handleCreateConnection = useCallback(async ({
    sourceNodeId,
    sourcePortId,
    targetNodeId,
    targetPortId,
    metadata
  }: {
    sourceNodeId: string;
    sourcePortId: string;
    targetNodeId: string;
    targetPortId: string;
    metadata?: any;
  }): Promise<ConnectionResult> => {
    try {
      // Validate before creating
      const validationResult = validatePortCompatibilityInRealTime(
        sourceNodeId,
        sourcePortId,
        targetNodeId,
        targetPortId
      );

      if (!validationResult.isValid) {
        return {
          success: false,
          errors: validationResult.errors.map(err => ({
            code: err.errorCode,
            message: err.message,
            severity: err.severity
          })),
          warnings: validationResult.warnings,
          suggestions: []
        };
      }

      // Dispatch to Redux
      const result = dispatch(createConnection({
        sourceNodeId,
        sourcePortId,
        targetNodeId,
        targetPortId,
        metadata
      }));

      const connection = result.payload as CanvasConnection;
      
      // Update stats
      updateStats({
        totalConnections: statsRef.current.totalConnections + 1
      });

      // Auto-validate if enabled
      if (autoValidate) {
        setValidationQueue(prev => [...prev, connection.id]);
      }

      // Call callback
      onConnectionCreated?.(connection);

      return {
        success: true,
        connection,
        errors: [],
        warnings: validationResult.warnings,
        suggestions: []
      };
    } catch (error) {
      console.error('Failed to create connection:', error);
      
      return {
        success: false,
        errors: [{
          code: 'CREATION_FAILED',
          message: error instanceof Error ? error.message : 'Connection creation failed',
          severity: 'ERROR'
        }],
        warnings: [],
        suggestions: ['Try again or check node configurations']
      };
    }
  }, [dispatch, autoValidate, validatePortCompatibilityInRealTime, onConnectionCreated, updateStats]);

  const handleUpdateConnection = useCallback(async (
    connectionId: string,
    updates: Partial<CanvasConnection>
  ): Promise<ConnectionResult> => {
    try {
      const existingConnection = connectionsMap[connectionId];
      if (!existingConnection) {
        return {
          success: false,
          errors: [{
            code: 'NOT_FOUND',
            message: `Connection ${connectionId} not found`,
            severity: 'ERROR'
          }],
          warnings: [],
          suggestions: []
        };
      }

      // Dispatch update
      dispatch(updateConnection({
        connectionId,
        updates
      }));

      // Mark for re-validation
      if (autoValidate) {
        setValidationQueue(prev => [...prev, connectionId]);
      }

      return {
        success: true,
        connection: { ...existingConnection, ...updates },
        errors: [],
        warnings: [],
        suggestions: []
      };
    } catch (error) {
      console.error('Failed to update connection:', error);
      
      return {
        success: false,
        errors: [{
          code: 'UPDATE_FAILED',
          message: error instanceof Error ? error.message : 'Connection update failed',
          severity: 'ERROR'
        }],
        warnings: [],
        suggestions: ['Try again or check connection state']
      };
    }
  }, [dispatch, autoValidate, connectionsMap]);

  const handleDeleteConnection = useCallback(async (connectionId: string): Promise<boolean> => {
    try {
      dispatch(deleteConnection(connectionId));
      
      // Update stats
      updateStats({
        totalConnections: Math.max(0, statsRef.current.totalConnections - 1)
      });

      // Clear from validation queue
      setValidationQueue(prev => prev.filter(id => id !== connectionId));

      // Clear caches
      validationCacheRef.current.delete(connectionId);
      executionOrderCacheRef.current = null;
      circularDepsCacheRef.current = null;

      // Call callback
      onConnectionDeleted?.(connectionId);

      return true;
    } catch (error) {
      console.error('Failed to delete connection:', error);
      return false;
    }
  }, [dispatch, onConnectionDeleted, updateStats]);

  const handleBatchDeleteConnections = useCallback(async (connectionIds: string[]): Promise<number> => {
    try {
      dispatch(deleteMultipleConnections(connectionIds));
      
      const deletedCount = connectionIds.length;
      updateStats({
        totalConnections: Math.max(0, statsRef.current.totalConnections - deletedCount)
      });

      // Clear from validation queue
      setValidationQueue(prev => prev.filter(id => !connectionIds.includes(id)));

      // Clear caches
      connectionIds.forEach(id => validationCacheRef.current.delete(id));
      executionOrderCacheRef.current = null;
      circularDepsCacheRef.current = null;

      return deletedCount;
    } catch (error) {
      console.error('Failed to batch delete connections:', error);
      return 0;
    }
  }, [dispatch, updateStats]);

  // ==================== VALIDATION HOOKS ====================

  const handleValidateConnection = useCallback(async (
    connectionId: string,
    options: ValidationOptions = {}
  ): Promise<ConnectionValidationResult> => {
    const startTime = performance.now();
    
    try {
      // Check cache first
      const cacheKey = `${connectionId}:${JSON.stringify(options)}`;
      if (validationCacheRef.current.has(cacheKey)) {
        updateStats({
          cacheHits: statsRef.current.cacheHits + 1
        });
        return validationCacheRef.current.get(cacheKey)!;
      }

      updateStats({
        cacheMisses: statsRef.current.cacheMisses + 1
      });

      const actionResult = await dispatch(validateConnection({
        connectionId,
        validateSchema: options.validateSchema ?? true,
        validatePerformance: options.validatePerformance ?? false
      })).unwrap();

      // Fixed: Extract the result from the payload
      const result = actionResult.result;

      const validationTime = performance.now() - startTime;
      validationTimesRef.current.push(validationTime);
      
      // Keep only last 100 times for average
      if (validationTimesRef.current.length > 100) {
        validationTimesRef.current.shift();
      }

      const averageTime = validationTimesRef.current.reduce((a, b) => a + b, 0) / validationTimesRef.current.length;
      
      updateStats({
        averageValidationTime: Math.round(averageTime),
        validConnections: result.isValid ? statsRef.current.validConnections + 1 : statsRef.current.validConnections,
        invalidConnections: !result.isValid ? statsRef.current.invalidConnections + 1 : statsRef.current.invalidConnections
      });

      // Cache the result
      validationCacheRef.current.set(cacheKey, result);

      return result;
    } catch (error) {
      console.error('Validation failed:', error);
      
      const fallbackResult: ConnectionValidationResult = {
        isValid: false,
        compatibilityScore: 0,
        errors: ['Validation failed: ' + (error instanceof Error ? error.message : 'Unknown error')],
        warnings: [],
        info: [],
        schemaCompatibility: { compatibleColumns: 0, incompatibleColumns: 0, typeCompatibility: [] },
        performanceImplications: { estimatedLatencyMs: 0, potentialBottleneck: false, recommendations: [] },
        timestamp: new Date().toISOString()
      };
      
      return fallbackResult;
    }
  }, [dispatch, updateStats]);

  const handleValidateAllConnections = useCallback(async (
    options: ValidationOptions = {}
  ): Promise<Map<string, ConnectionValidationResult>> => {
    const results = new Map<string, ConnectionValidationResult>();
    
    // Use batch validation for efficiency
    const batchResult = await dispatch(batchValidateConnections({
      connectionIds: Object.keys(connectionsMap),
      validateSchema: options.validateSchema ?? true
    }));

    if (batchValidateConnections.fulfilled.match(batchResult)) {
      batchResult.payload.forEach(({ connectionId, result }) => {
        results.set(connectionId, result);
        
        // Update cache
        const cacheKey = `${connectionId}:${JSON.stringify(options)}`;
        validationCacheRef.current.set(cacheKey, result);
      });

      // Trigger SQL regeneration for valid connections
      const validConnections = Array.from(results.entries())
        .filter(([, result]) => result.isValid)
        .map(([connectionId]) => connectionId);

      if (validConnections.length > 0) {
        const nodeIds = validConnections.flatMap(connId => {
          const conn = connectionsMap[connId];
          return conn ? [conn.sourceNodeId, conn.targetNodeId] : [];
        });
        
        const uniqueNodeIds = [...new Set(nodeIds)];
        
        dispatch(generatePipelineSQL({
          nodeIds: uniqueNodeIds,
          connectionIds: validConnections,
          includeComments: true,
          format: true
        }));
      }

      // Call completion callback
      onValidationResults?.(results);
    }

    return results;
  }, [dispatch, connectionsMap, onValidationResults]);

  // Debounced validation
  useEffect(() => {
    if (validationQueue.length === 0 || validationDebounceMs <= 0) return;

    if (validationTimeoutRef.current) {
      clearTimeout(validationTimeoutRef.current);
    }

    validationTimeoutRef.current = setTimeout(() => {
      const connectionsToValidate = [...validationQueue];
      setValidationQueue([]);

      connectionsToValidate.forEach(connectionId => {
        handleValidateConnection(connectionId, {
          validateSchema: true,
          validatePerformance: false
        }).catch(console.error);
      });
    }, validationDebounceMs);

    return () => {
      if (validationTimeoutRef.current) {
        clearTimeout(validationTimeoutRef.current);
      }
    };
  }, [validationQueue, validationDebounceMs, handleValidateConnection]);

  // ==================== DRAG-AND-DROP HANDLERS ====================

  const handleConnectionStart = useCallback((
    sourceNodeId: string,
    sourcePortId: string,
    position: NodePosition
  ) => {
    const sourceNode = getNodeById(sourceNodeId);
    const sourcePort = getPortById(sourceNodeId, sourcePortId);

    if (!sourceNode || !sourcePort) {
      console.error('Invalid source node or port');
      return;
    }

    // Check if source port can have more connections
    const existingConnections = connections.filter(
      conn => conn.sourceNodeId === sourceNodeId && conn.sourcePortId === sourcePortId
    );

    if (sourcePort.maxConnections && existingConnections.length >= sourcePort.maxConnections) {
      console.warn(`Source port ${sourcePortId} has reached maximum connections`);
      return;
    }

    setDragState({
      isDragging: true,
      sourceNodeId,
      sourcePortId,
      targetNodeId: null,
      targetPortId: null,
      dragStartPosition: position,
      currentPosition: position,
      snapToPort: null,
      validationResult: null
    });

    updateStats({
      pendingValidations: statsRef.current.pendingValidations + 1
    });
  }, [getNodeById, getPortById, connections, updateStats]);

  const handleConnectionDrag = useCallback((position: NodePosition) => {
    setDragState(prev => {
      if (!prev.isDragging) return prev;

      const newState = { ...prev, currentPosition: position };
      
      // Find nearest port for snapping
      if (enableSnapToPort && prev.sourceNodeId) {
        const nearestPort = findNearestPort(position, prev.sourceNodeId);
        
        if (nearestPort) {
          newState.snapToPort = nearestPort.port;
          newState.targetNodeId = nearestPort.nodeId;
          newState.targetPortId = nearestPort.port.id;
          
          // Validate compatibility in real-time
          newState.validationResult = validatePortCompatibilityInRealTime(
            prev.sourceNodeId!,
            prev.sourcePortId!,
            nearestPort.nodeId,
            nearestPort.port.id
          );
        } else {
          newState.snapToPort = null;
          newState.targetNodeId = null;
          newState.targetPortId = null;
          newState.validationResult = null;
        }
      }
      
      return newState;
    });
  }, [enableSnapToPort, findNearestPort, validatePortCompatibilityInRealTime]);

  const handleConnectionEnd = useCallback(async (
    targetNodeId: string | null,
    targetPortId: string | null
  ): Promise<ConnectionResult> => {
    if (!dragState.sourceNodeId || !dragState.sourcePortId) {
      return {
        success: false,
        errors: [{
          code: 'INCOMPLETE_DRAG',
          message: 'Drag operation incomplete',
          severity: 'ERROR'
        }],
        warnings: [],
        suggestions: []
      };
    }

    const finalTargetNodeId = targetNodeId || dragState.targetNodeId;
    const finalTargetPortId = targetPortId || dragState.targetPortId;

    if (!finalTargetNodeId || !finalTargetPortId) {
      setDragState({
        isDragging: false,
        sourceNodeId: null,
        sourcePortId: null,
        targetNodeId: null,
        targetPortId: null,
        dragStartPosition: null,
        currentPosition: null,
        snapToPort: null,
        validationResult: null
      });
      
      updateStats({
        pendingValidations: Math.max(0, statsRef.current.pendingValidations - 1)
      });
      
      return {
        success: false,
        errors: [{
          code: 'NO_TARGET',
          message: 'Connection dropped without valid target',
          severity: 'ERROR'
        }],
        warnings: [],
        suggestions: ['Drop on a compatible port to create connection']
      };
    }

    // Create the connection
    const result = await handleCreateConnection({
      sourceNodeId: dragState.sourceNodeId,
      sourcePortId: dragState.sourcePortId,
      targetNodeId: finalTargetNodeId,
      targetPortId: finalTargetPortId
    });

    // Reset drag state
    setDragState({
      isDragging: false,
      sourceNodeId: null,
      sourcePortId: null,
      targetNodeId: null,
      targetPortId: null,
      dragStartPosition: null,
      currentPosition: null,
      snapToPort: null,
      validationResult: null
    });

    updateStats({
      pendingValidations: Math.max(0, statsRef.current.pendingValidations - 1)
    });

    return result;
  }, [dragState, handleCreateConnection, updateStats]);

  const handleConnectionCancel = useCallback(() => {
    setDragState({
      isDragging: false,
      sourceNodeId: null,
      sourcePortId: null,
      targetNodeId: null,
      targetPortId: null,
      dragStartPosition: null,
      currentPosition: null,
      snapToPort: null,
      validationResult: null
    });

    updateStats({
      pendingValidations: Math.max(0, statsRef.current.pendingValidations - 1)
    });
  }, [updateStats]);

  // ==================== UTILITY FUNCTIONS ====================

  const findConnections = useCallback((query: ConnectionQuery): CanvasConnection[] => {
    return connections.filter(conn => {
      if (query.nodeId && conn.sourceNodeId !== query.nodeId && conn.targetNodeId !== query.nodeId) {
        return false;
      }
      if (query.portId && conn.sourcePortId !== query.portId && conn.targetPortId !== query.portId) {
        return false;
      }
      if (query.status && conn.status !== query.status) {
        return false;
      }
      if (query.sourceNodeId && conn.sourceNodeId !== query.sourceNodeId) {
        return false;
      }
      if (query.targetNodeId && conn.targetNodeId !== query.targetNodeId) {
        return false;
      }
      return true;
    });
  }, [connections]);

  const getNodeConnections = useCallback((nodeId: string) => {
    const incoming = connections.filter(conn => conn.targetNodeId === nodeId);
    const outgoing = connections.filter(conn => conn.sourceNodeId === nodeId);
    const all = [...incoming, ...outgoing];
    
    return { incoming, outgoing, all };
  }, [connections]);

  const getUpstreamNodes = useCallback((nodeId: string): CanvasNode[] => {
    const visited = new Set<string>();
    const upstreamNodes: CanvasNode[] = [];
    
    const traverse = (currentNodeId: string) => {
      const incomingConnections = connections.filter(conn => conn.targetNodeId === currentNodeId);
      
      incomingConnections.forEach(conn => {
        const sourceNode = getNodeById(conn.sourceNodeId);
        if (sourceNode && !visited.has(sourceNode.id)) {
          visited.add(sourceNode.id);
          upstreamNodes.push(sourceNode);
          traverse(sourceNode.id);
        }
      });
    };
    
    traverse(nodeId);
    return upstreamNodes;
  }, [connections, getNodeById]);

  const getDownstreamNodes = useCallback((nodeId: string): CanvasNode[] => {
    const visited = new Set<string>();
    const downstreamNodes: CanvasNode[] = [];
    
    const traverse = (currentNodeId: string) => {
      const outgoingConnections = connections.filter(conn => conn.sourceNodeId === currentNodeId);
      
      outgoingConnections.forEach(conn => {
        const targetNode = getNodeById(conn.targetNodeId);
        if (targetNode && !visited.has(targetNode.id)) {
          visited.add(targetNode.id);
          downstreamNodes.push(targetNode);
          traverse(targetNode.id);
        }
      });
    };
    
    traverse(nodeId);
    return downstreamNodes;
  }, [connections, getNodeById]);

  const getExecutionOrder = useCallback((): string[] => {
    if (executionOrderCacheRef.current) {
      return executionOrderCacheRef.current;
    }

    const { cycles, topologicalOrder } = ConnectionValidator.detectConnectionCycles(
      nodes,
      connections
    );

    if (cycles.length > 0) {
      console.warn('Circular dependencies detected, execution order may be ambiguous');
    }

    executionOrderCacheRef.current = topologicalOrder;
    updateStats({ circularDependencies: cycles.length });

    return topologicalOrder;
  }, [nodes, connections, updateStats]);

  const hasCircularDependencies = useCallback((): boolean => {
    if (circularDepsCacheRef.current) {
      return circularDepsCacheRef.current.hasCycles;
    }

    const { hasCycles } = ConnectionValidator.detectConnectionCycles(nodes, connections);
    circularDepsCacheRef.current = { hasCycles, cycles: [] };
    
    return hasCycles;
  }, [nodes, connections]);

  const getCircularDependencies = useCallback((): string[][] => {
    if (circularDepsCacheRef.current?.cycles) {
      return circularDepsCacheRef.current.cycles;
    }

    const { cycles } = ConnectionValidator.detectConnectionCycles(nodes, connections);
    circularDepsCacheRef.current = { hasCycles: cycles.length > 0, cycles };
    
    return cycles;
  }, [nodes, connections]);

  const suggestSchemaMappings = useCallback((
    sourceNodeId: string,
    targetNodeId: string
  ): SchemaMapping[] => {
    const sourceNode = getNodeById(sourceNodeId);
    const targetNode = getNodeById(targetNodeId);
    
    if (!sourceNode?.metadata?.tableMapping || !targetNode?.metadata?.tableMapping) {
      return [];
    }

    const sourceColumns = sourceNode.metadata.tableMapping.columns;
    const targetColumns = targetNode.metadata.tableMapping.columns;

    // Simple matching by name for now
    const mappings: SchemaMapping[] = [];
    
    sourceColumns.forEach(sourceCol => {
      const matchingTarget = targetColumns.find(
        targetCol => targetCol.name.toLowerCase() === sourceCol.name.toLowerCase()
      );
      
      if (matchingTarget) {
        mappings.push({
          sourceColumn: sourceCol.name,
          targetColumn: matchingTarget.name,
          isRequired: !sourceCol.nullable,
          transformation: sourceCol.dataType !== matchingTarget.dataType 
            ? `CAST(${sourceCol.name} AS ${matchingTarget.dataType})`
            : undefined
        });
      }
    });

    return mappings;
  }, [getNodeById]);

  // ==================== EVENT HANDLERS ====================

  const handleConnectionStatusChange = useCallback((connectionId: string, status: ConnectionStatus) => {
    dispatch(updateConnection({
      connectionId,
      updates: { status }
    }));

    // Clear validation cache for this connection
    const cacheKeys = Array.from(validationCacheRef.current.keys())
      .filter(key => key.startsWith(connectionId));
    
    cacheKeys.forEach(key => validationCacheRef.current.delete(key));
  }, [dispatch]);

  const handleConnectionError = useCallback((
    connectionId: string,
    error: Error,
    recoveryAction?: () => void
  ) => {
    console.error(`Connection error for ${connectionId}:`, error);
    
    // Update connection status
    handleConnectionStatusChange(connectionId, ConnectionStatus.INVALID);
    
    // Auto-attempt recovery if possible
    if (recoveryAction) {
      setTimeout(recoveryAction, 1000);
    }
  }, [handleConnectionStatusChange]);

  const handleValidationComplete = useCallback((results: Map<string, ConnectionValidationResult>) => {
    // Update SQL cache for valid connections
    results.forEach((result, connectionId) => {
      if (result.isValid && result.compatibilityScore > 80) {
        dispatch(updateSQLCache({
          key: connectionId,
          sql: `-- Validated connection ${connectionId}\n-- Score: ${result.compatibilityScore}`,
          type: 'connection'
        }));
      }
    });

    // Trigger any post-validation hooks
    onValidationResults?.(results);
  }, [dispatch, onValidationResults]);

  const handleClearCache = useCallback(() => {
    nodeCacheRef.current.clear();
    portCacheRef.current.clear();
    validationCacheRef.current.clear();
    executionOrderCacheRef.current = null;
    circularDepsCacheRef.current = null;
    
    updateStats({
      cacheHits: 0,
      cacheMisses: 0,
      averageValidationTime: 0
    });
    validationTimesRef.current = [];
  }, [updateStats]);

  const handleGetStats = useCallback((): ConnectionStats => {
    return {
      ...statsRef.current,
      totalConnections: connections.length,
      pendingValidations: validationQueue.length
    };
  }, [connections.length, validationQueue.length]);

  // ==================== EFFECTS ====================

  // Auto-validate on connections change
  useEffect(() => {
    if (autoValidate && connections.length > 0) {
      const newConnections = connections.filter(conn => 
        conn.status === ConnectionStatus.UNVALIDATED || conn.status === ConnectionStatus.PENDING
      );
      
      if (newConnections.length > 0) {
        setValidationQueue(prev => [
          ...prev,
          ...newConnections.map(conn => conn.id).filter(id => !prev.includes(id))
        ]);
      }
    }
  }, [connections, autoValidate]);

  // Update stats on connections change
  useEffect(() => {
    const validCount = connections.filter(conn => 
      conn.status === ConnectionStatus.VALID
    ).length;
    
    const invalidCount = connections.filter(conn => 
      conn.status === ConnectionStatus.INVALID
    ).length;

    updateStats({
      totalConnections: connections.length,
      validConnections: validCount,
      invalidConnections: invalidCount
    });
  }, [connections, updateStats]);

  // ==================== RETURN VALUE ====================

  return {
    // State
    dragState,
    isConnecting: dragState.isDragging,
    activeConnectionId,
    validationQueue,
    
    // Connection Operations
    connections: connectionsMap,
    createConnection: handleCreateConnection,
    updateConnection: handleUpdateConnection,
    deleteConnection: handleDeleteConnection,
    batchDeleteConnections: handleBatchDeleteConnections,
    validateConnection: handleValidateConnection,
    validateAllConnections: handleValidateAllConnections,
    
    // Drag-and-drop Event Handlers
    onConnectionStart: handleConnectionStart,
    onConnectionDrag: handleConnectionDrag,
    onConnectionEnd: handleConnectionEnd,
    onConnectionCancel: handleConnectionCancel,
    
    // Utility Functions
    findConnections,
    getNodeConnections,
    getUpstreamNodes,
    getDownstreamNodes,
    getExecutionOrder,
    hasCircularDependencies,
    getCircularDependencies,
    suggestSchemaMappings,
    
    // Event Handlers
    onConnectionStatusChange: handleConnectionStatusChange,
    onConnectionError: handleConnectionError,
    onValidationComplete: handleValidationComplete,
    
    // Performance
    clearCache: handleClearCache,
    getStats: handleGetStats
  };
}

// ==================== ERROR BOUNDARY COMPONENT ====================

interface ConnectionErrorBoundaryProps {
  children: React.ReactNode;
  onError?: (error: Error, errorInfo: React.ErrorInfo) => void;
  fallback?: React.ReactNode;
}

interface ConnectionErrorBoundaryState {
  hasError: boolean;
  error?: Error;
}

export class ConnectionErrorBoundary extends React.Component<
  ConnectionErrorBoundaryProps,
  ConnectionErrorBoundaryState
> {
  constructor(props: ConnectionErrorBoundaryProps) {
    super(props);
    this.state = { hasError: false };
  }

  static getDerivedStateFromError(error: Error): ConnectionErrorBoundaryState {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, errorInfo: React.ErrorInfo) {
    console.error('ConnectionErrorBoundary caught an error:', error, errorInfo);
    this.props.onError?.(error, errorInfo);
  }

  render() {
    if (this.state.hasError) {
      return this.props.fallback || (
        <div className="connection-error-boundary">
          <h3>Connection Manager Error</h3>
          <p>{this.state.error?.message || 'An unexpected error occurred'}</p>
          <button onClick={() => this.setState({ hasError: false })}>
            Try Again
          </button>
        </div>
      );
    }

    return this.props.children;
  }
}

// ==================== UTILITY HOOKS ====================

export function useConnectionQuery(manager: UseConnectionManagerReturn, query: ConnectionQuery) {
  return useMemo(() => manager.findConnections(query), [manager, query]);
}

export function useNodeConnections(manager: UseConnectionManagerReturn, nodeId: string) {
  return useMemo(() => manager.getNodeConnections(nodeId), [manager, nodeId]);
}

export function useExecutionOrder(manager: UseConnectionManagerReturn) {
  return useMemo(() => manager.getExecutionOrder(), [manager]);
}

export function useConnectionValidation(
  manager: UseConnectionManagerReturn,
  connectionId: string,
  options?: ValidationOptions
) {
  const [validationResult, setValidationResult] = useState<ConnectionValidationResult | null>(null);
  const [isValidating, setIsValidating] = useState(false);

  const validate = useCallback(async () => {
    setIsValidating(true);
    try {
      const result = await manager.validateConnection(connectionId, options);
      setValidationResult(result);
      return result;
    } finally {
      setIsValidating(false);
    }
  }, [manager, connectionId, options]);

  useEffect(() => {
    if (options?.autoValidate !== false) {
      validate();
    }
  }, [validate, options?.autoValidate]);

  return { validationResult, isValidating, validate };
}

