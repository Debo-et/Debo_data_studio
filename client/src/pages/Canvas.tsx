// src/pages/Canvas.tsx
import React, {
  useCallback,
  useEffect,
  useRef,
  useState,
  forwardRef,
  useImperativeHandle,
} from 'react';
import ReactFlow, {
  Controls,
  Background,
  BackgroundVariant,
  ConnectionMode,
  ReactFlowInstance,
  NodeTypes,
  useReactFlow,
  ConnectionLineType,
  SelectionMode,
  addEdge,
  applyNodeChanges,
  applyEdgeChanges,
  NodeChange,
  EdgeChange,
  Connection,
  Edge,
  MarkerType,
  Node,
  Viewport,
} from 'reactflow';
// @ts-ignore
import 'reactflow/dist/style.css';

import TalendNode from './TalendNode';
import { COMPONENT_REGISTRY, getCategoryColor } from './ComponentRegistry';
import { nameGenerator } from './NameGenerator';
import { useCanvas } from './CanvasContext';
import { ValidationEngine } from '../validation/validationEngine';
import { SchemaRegistry } from '../validation/schemaRegistry';
import { DefaultSchemas, DefaultConnectionRules } from '../validation/schemaRegistry';
import { getConnectedColumns } from '../utils/columnExtraction';
import { canvasPersistence, CanvasRecord } from '../services/canvas-persistence.service';
import { useAppDispatch } from '../hooks';
import { addLog } from '../store/slices/logsSlice';
import { generatePipelineSQL, PipelineGenerationResult } from '../generators/SQLGenerationPipeline';
import { CanvasNode as PipelineCanvasNode, CanvasConnection as PipelineCanvasConnection, ConnectionStatus } from '../types/pipeline-types';
import { NodeType, isMapConfig, isJoinConfig, isFilterConfig, isAggregateConfig, DataType, isSortConfig, isInputConfig,isConvertConfig,  isOutputConfig, NodeStatus, UnifiedNodeMetadata, ComponentConfiguration, FilterColumnsComponentConfiguration, isAddCRCRowConfig, isSchemaComplianceCheckConfig, isDataMaskingConfig, isSampleRowConfig, ParseRecordSetComponentConfiguration, ExtractRegexFieldsConfiguration, isDenormalizeSortedRowConfig, DenormalizeComponentConfiguration, PivotToColumnsDelimitedConfiguration, isNormalizeConfig, isUniqRowConfig, isUniteConfig, isReplicateConfig, isNormalizeNumberConfig, isExtractXMLFieldConfig, ExtractJSONFieldsConfiguration, isExtractDelimitedConfig, ReplaceComponentConfiguration, isReplaceConfig, isLookupConfig, NormalizeComponentConfiguration } from '../types/unified-pipeline.types';
import {
  ActiveEditor,
  CanvasNodeData,
  CanvasState,
  ExtendedCanvasProps,
  PendingRoleSelection,
  ReactFlowDragData,
  SimpleColumn,
} from './canvas.types';
import {
  createInitialComponentConfiguration,
  createEdgeWithMetadata,
  extractColumnsFromDragData,
  extractColumnsFromNode,
  getConnectedNodes,
  convertToGraphState,
  wouldCauseCycle,
  mapComponentKeyToNodeType,
  getActivePostgresConnectionId,
  fieldsToPostgresColumns,
  mapDataTypeToPostgreSQL,
} from './canvas.utils';
import {
  ConnectionFeedback,
  AutoSaveStatus,
  RoleSelectionPopup,
  MapEditorModal,
  TMapEditorModal,
  MatchGroupWizard,
  ActiveEditorRenderer,
} from './canvas.modals';
import databaseApi from '@/services/database-api.service';
import { mapToPostgresType } from '@/api/postgres-foreign-table';

const nodeTypes: NodeTypes = {
  talendNode: TalendNode,
};

const Canvas = forwardRef<{ forceSave: () => Promise<void> }, ExtendedCanvasProps>(({
  job,
  canvasId,
  validateConnection: propValidateConnection,
  onNodeMetadataUpdate,
  onEdgeMetadataUpdate,
}, ref) => {
  const { syncNodesAndEdges, updateCanvasData } = useCanvas();
  const { screenToFlowPosition, addNodes, setViewport } = useReactFlow();
  const reactFlowWrapper = useRef<HTMLDivElement>(null);
  const [reactFlowInstance, setReactFlowInstance] = useState<ReactFlowInstance | null>(null);
  const validationEngineRef = useRef<ValidationEngine | null>(null);
  const schemaRegistryRef = useRef<SchemaRegistry | null>(null);
  const saveTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const isSavingRef = useRef<boolean>(false);
  const lastSaveStateRef = useRef<string>('');
  const lastLoadedCanvasIdRef = useRef<string | null>(null);
  const lastLoadedJobIdRef = useRef<string | null>(null);
  const localCanvasIdRef = useRef<string | null>(null);
  const dispatch = useAppDispatch();

  const [nodes, setNodes] = useState<Node[]>([]);
  const [edges, setEdges] = useState<Edge[]>([]);
  const [state, setState] = useState<CanvasState>({
    nodes: [],
    edges: [],
    selectedNodeId: null,
    showMapEditor: false,
    selectedNodeForMapEditor: null,
    sqlPreview: { isVisible: false, sql: '', title: '' },
    validationSummary: null,
    validationMode: 'strict',
    isValidating: false,
    connectionFeedback: { isVisible: false, message: '', type: 'info', position: { x: 0, y: 0 } },
    pendingDrop: null,
    showMatchGroupWizard: false,
    selectedNodeForMatchGroupWizard: null,
    viewport: { x: 0, y: 0, zoom: 1 },
    mapEditorState: { isOpen: false, data: null },
    autoSaveStatus: 'idle',
    lastSavedAt: undefined,
  });

  const [pendingRoleSelection, setPendingRoleSelection] = useState<PendingRoleSelection | null>(null);
  const [activeEditor, setActiveEditor] = useState<ActiveEditor>(null);

  // ==================== SYNC WITH CONTEXT ====================
  useEffect(() => {
    if (nodes.length || edges.length) {
      syncNodesAndEdges(nodes, edges);
    }
  }, [nodes, edges, syncNodesAndEdges]);

  // ==================== AUTO-SAVE ====================
  const saveCanvasState = useCallback(async () => {
    if ((!job && !canvasId) || isSavingRef.current) return;
    try {
      isSavingRef.current = true;
      const currentStateHash = JSON.stringify({
        nodes: nodes.map(n => ({ id: n.id, position: n.position, data: n.data })),
        edges: edges.map(e => ({ id: e.id, source: e.source, target: e.target, data: e.data })),
        viewport: state.viewport,
      });
      if (currentStateHash === lastSaveStateRef.current) return;

      setState(prev => ({ ...prev, autoSaveStatus: 'saving' }));

      let savedRecord: CanvasRecord | null = null;
      if (canvasId) {
        await canvasPersistence.updateCanvas(canvasId, { nodes, edges, viewport: state.viewport });
      } else if (localCanvasIdRef.current) {
        await canvasPersistence.updateCanvas(localCanvasIdRef.current, { nodes, edges, viewport: state.viewport });
      } else if (job) {
        savedRecord = await canvasPersistence.saveCanvas(
          job.name,
          { nodes, edges, viewport: state.viewport },
          { description: `Auto-saved from job: ${job?.name}`, tags: [job?.name || 'unknown', 'auto-save', 'canvas'], compilerMetadata: {}, otherUiState: {} }
        );
        if (savedRecord?.id) localCanvasIdRef.current = savedRecord.id;
      } else {
        throw new Error('No canvas target');
      }

      lastSaveStateRef.current = currentStateHash;
      setState(prev => ({ ...prev, autoSaveStatus: 'saved', lastSavedAt: new Date().toISOString() }));
      showValidationFeedback(`Saved (${nodes.length} nodes, ${edges.length} edges)`, 'success', { x: 100, y: 100 });
    } catch (error: any) {
      console.error('Save failed:', error);
      setState(prev => ({ ...prev, autoSaveStatus: 'error' }));
      showValidationFeedback(`Save failed: ${error.message}`, 'error', { x: 100, y: 100 });
    } finally {
      isSavingRef.current = false;
    }
  }, [job, canvasId, nodes, edges, state.viewport]);

  const debouncedAutoSave = useCallback(() => {
    if (saveTimeoutRef.current) clearTimeout(saveTimeoutRef.current);
    if ((!job && !canvasId) || isSavingRef.current) return;
    saveTimeoutRef.current = setTimeout(() => saveCanvasState(), 1000);
  }, [job, canvasId, saveCanvasState]);

  useImperativeHandle(ref, () => ({ forceSave: saveCanvasState }), [saveCanvasState]);

  useEffect(() => {
    if (canvasId) localCanvasIdRef.current = canvasId;
  }, [canvasId]);

  // ==================== LOAD INITIAL CANVAS ====================
  useEffect(() => {
    const initializeCanvas = async () => {
      if (!canvasId && !job) return;
      if (canvasId && lastLoadedCanvasIdRef.current === canvasId) return;
      if (job?.id && lastLoadedJobIdRef.current === job.id) return;

      try {
        let loadedNodes: Node[] = [];
        let loadedEdges: Edge[] = [];
        let loadedViewport: Viewport = { x: 0, y: 0, zoom: 1 };

        if (canvasId) {
          const canvasData = await canvasPersistence.getCanvas(canvasId);
          if (canvasData) {
            loadedNodes = canvasData.reactFlow.nodes || [];
            loadedEdges = canvasData.reactFlow.edges || [];
            loadedViewport = canvasData.reactFlow.viewport || { x: 0, y: 0, zoom: 1 };
            localCanvasIdRef.current = canvasId;
          } else {
            showValidationFeedback('Canvas data not found. Starting fresh.', 'warning', { x: 100, y: 100 });
          }
        } else if (job) {
          const savedData = await canvasPersistence.getCanvasByName(job.name);
          if (savedData) {
            loadedNodes = savedData.data.reactFlow.nodes || [];
            loadedEdges = savedData.data.reactFlow.edges || [];
            loadedViewport = savedData.data.reactFlow.viewport || { x: 0, y: 0, zoom: 1 };
            localCanvasIdRef.current = savedData.id;
          }
        }

        setNodes(loadedNodes);
        setEdges(loadedEdges);
        if (loadedViewport && reactFlowInstance) setViewport(loadedViewport);
        setState(prev => ({ ...prev, viewport: loadedViewport, lastSavedAt: canvasId ? new Date().toISOString() : undefined }));
        updateCanvasData({ nodes: loadedNodes, edges: loadedEdges, viewport: loadedViewport });

        if (loadedNodes.length || loadedEdges.length) {
          showValidationFeedback(`Loaded ${loadedNodes.length} nodes, ${loadedEdges.length} edges`, 'success', { x: 100, y: 100 });
        }

        if (canvasId) {
          lastLoadedCanvasIdRef.current = canvasId;
          lastLoadedJobIdRef.current = null;
        } else if (job?.id) {
          lastLoadedJobIdRef.current = job.id;
          lastLoadedCanvasIdRef.current = null;
        }
      } catch (error) {
        console.error('Load failed:', error);
        showValidationFeedback('Failed to load canvas. Starting fresh.', 'error', { x: 100, y: 100 });
      }
    };
    initializeCanvas();
    return () => {
      if ((job || canvasId) && (lastLoadedCanvasIdRef.current || lastLoadedJobIdRef.current)) saveCanvasState();
      if (saveTimeoutRef.current) clearTimeout(saveTimeoutRef.current);
    };
  }, [job, canvasId, reactFlowInstance, setViewport, updateCanvasData, saveCanvasState]);

  // ==================== VALIDATION SETUP ====================
  useEffect(() => {
    const registry = new SchemaRegistry();
    registry.registerSchemas(DefaultSchemas);
    DefaultConnectionRules.forEach(rule => registry.registerConnectionRule(rule));
    schemaRegistryRef.current = registry;
    validationEngineRef.current = new ValidationEngine({
      schemaRegistry: registry,
      mode: state.validationMode,
      enableCaching: true,
      cacheTTL: 3000,
      enableETLValidation: true,
      etlMode: 'strict',
    });
    if (nodes.length) {
      const graphState = convertToGraphState(nodes, edges);
      const summary = validationEngineRef.current.validateGraph(graphState);
      if (!summary.isValid) console.warn('Initial graph validation failed:', summary);
    }
    return () => validationEngineRef.current?.clearCache();
  }, [state.validationMode, nodes.length, edges.length]);

  // ==================== FEEDBACK ====================
  const showValidationFeedback = useCallback((
    message: string,
    type: 'success' | 'error' | 'info' | 'warning',
    position?: { x: number; y: number }
  ) => {
    const pos = position || { x: 100, y: 100 };
    setState(prev => ({
      ...prev,
      connectionFeedback: { isVisible: true, message, type, position: pos },
    }));
    setTimeout(() => {
      setState(prev => ({ ...prev, connectionFeedback: { ...prev.connectionFeedback, isVisible: false } }));
    }, 3000);
  }, []);

  // ==================== CONNECTION VALIDATION ====================
  const isValidConnection = useCallback((connection: Connection): boolean => {
    if (!connection.source || !connection.target || connection.source === connection.target) return false;
    if (wouldCauseCycle(connection.source, connection.target, edges)) return false;
    if (validationEngineRef.current) {
      const state = convertToGraphState(nodes, edges);
      const result = validationEngineRef.current.validateSpecificConnection(connection.source, connection.target, state);
      return result.isValid;
    }
    const sourceNode = nodes.find(n => n.id === connection.source);
    const targetNode = nodes.find(n => n.id === connection.target);
    if (!sourceNode || !targetNode) return false;
    const sourceType = sourceNode.data?.type || sourceNode.type || 'unknown';
    const targetType = targetNode.data?.type || targetNode.type || 'unknown';
    const connectionCheck = schemaRegistryRef.current?.isConnectionAllowed(sourceType, targetType);
    if (connectionCheck && !connectionCheck.allowed) return false;
    const etlCheck = schemaRegistryRef.current?.isETLConnectionAllowed(sourceType, targetType);
    if (etlCheck && !etlCheck.allowed) return false;
    return true;
  }, [nodes, edges]);

  // ==================== EVENT HANDLERS ====================
  const onNodesChange = useCallback((changes: NodeChange[]) => {
    const updated = applyNodeChanges(changes, nodes);
    setNodes(updated);
    syncNodesAndEdges(updated, edges);
    if (job || canvasId) debouncedAutoSave();
    changes.forEach(change => {
      if (change.type === 'remove' && pendingRoleSelection?.nodeId === change.id) setPendingRoleSelection(null);
    });
  }, [nodes, edges, pendingRoleSelection, syncNodesAndEdges, job, canvasId, debouncedAutoSave]);

  const onEdgesChange = useCallback((changes: EdgeChange[]) => {
    const updated = applyEdgeChanges(changes, edges);
    setEdges(updated);
    syncNodesAndEdges(nodes, updated);
    if (job || canvasId) debouncedAutoSave();
  }, [nodes, edges, syncNodesAndEdges, job, canvasId, debouncedAutoSave]);

  const onMove = useCallback((_event: MouseEvent | TouchEvent | null, viewport: Viewport) => {
    setState(prev => ({ ...prev, viewport }));
    if (job || canvasId) debouncedAutoSave();
  }, [job, canvasId, debouncedAutoSave]);

  const onConnect = useCallback((connection: Connection) => {
    if (!connection.source || !connection.target) {
      showValidationFeedback('Source and target required', 'error'); return;
    }
    const sourceNode = nodes.find(n => n.id === connection.source);
    const targetNode = nodes.find(n => n.id === connection.target);
    if (!sourceNode || !targetNode) {
      showValidationFeedback('Source or target node not found', 'error'); return;
    }
    if (connection.source === connection.target) {
      showValidationFeedback('Cannot connect node to itself', 'error'); return;
    }
    if (wouldCauseCycle(connection.source, connection.target, edges)) {
      showValidationFeedback('Connection would create a cycle', 'error'); return;
    }
    if (propValidateConnection) {
      const validation = propValidateConnection(connection, nodes, edges);
      if (!validation.isValid) {
        showValidationFeedback(validation.errors[0] || 'Invalid connection', 'error'); return;
      }
      validation.warnings.forEach(w => showValidationFeedback(w, 'warning'));
    }
    const newEdge = createEdgeWithMetadata(
      { ...connection, source: connection.source!, target: connection.target! },
      sourceNode as Node<CanvasNodeData>,
      targetNode as Node<CanvasNodeData>
    );
    const updatedEdges = addEdge(newEdge, edges);
    setEdges(updatedEdges);
    syncNodesAndEdges(nodes, updatedEdges);
    if (onEdgeMetadataUpdate) onEdgeMetadataUpdate(newEdge.id, newEdge.data);
    if (job || canvasId) debouncedAutoSave();
    showValidationFeedback(`Created ${newEdge.data.relationType} connection`, 'success', { x: 100, y: 100 });
  }, [nodes, edges, propValidateConnection, showValidationFeedback, onEdgeMetadataUpdate, syncNodesAndEdges, job, canvasId, debouncedAutoSave]);

  const onSelectionChange = useCallback(({ nodes: selectedNodes }: { nodes: Node[]; edges: Edge[] }) => {
    setState(prev => ({ ...prev, selectedNodeId: selectedNodes.length ? selectedNodes[0].id : null }));
  }, []);

  const onDrop = useCallback((event: React.DragEvent) => {
  event.preventDefault();
  if (!reactFlowInstance) return;
  const position = screenToFlowPosition({ x: event.clientX, y: event.clientY });
  const reactFlowData = event.dataTransfer.getData('application/reactflow');
  if (!reactFlowData) return;
  try {
    const data: ReactFlowDragData = JSON.parse(reactFlowData);
    if (data.type !== 'reactflow-component') return;
    const componentDef = COMPONENT_REGISTRY[data.componentId];
    if (!componentDef) return;

    let baseName = data.metadata?.originalNodeName || data.metadata?.repositoryMetadata?.name || data.metadata?.name || componentDef.displayName;
    const cleanBaseName = baseName.replace(/_(INPUT|OUTPUT|TRANSFORM)_/i, '_').replace(/_+$/, '');
    const isInputCategory = componentDef.category === 'input';
    const label = nameGenerator.generate(cleanBaseName, isInputCategory ? 'TRANSFORM' : componentDef.defaultRole);
    const columns = extractColumnsFromDragData(data.metadata);
    const componentRole = isInputCategory ? 'TRANSFORM' : componentDef.defaultRole;
    const configuration = createInitialComponentConfiguration(componentDef.id, componentRole, data.metadata);

    const fields = columns.map((col: any, idx) => ({
      id: col.id || `${cleanBaseName}_${idx}`,
      name: col.name || `Column_${idx + 1}`,
      type: (col.type || 'STRING') as DataType,
      length: col.length,
      precision: col.precision,
      scale: col.scale,
      nullable: col.nullable !== false,
      isKey: col.isKey || col.primaryKey || false,
      defaultValue: col.defaultValue,
      description: col.description,
      originalName: col.originalName,
      transformation: col.expression,
      metadata: { original: col },
    }));

    const schemas: any = {};
    if (componentRole === 'INPUT') {
      schemas.output = { id: `${cleanBaseName}_output_schema`, name: `${label} Output Schema`, fields, isTemporary: false, isMaterialized: false };
      if (configuration.type === 'INPUT') configuration.config.schema = schemas.output;
    } else if (componentRole === 'OUTPUT') {
      schemas.input = [{ id: `${cleanBaseName}_input_schema`, name: `${label} Input Schema`, fields, isTemporary: false, isMaterialized: false }];
    } else {
      schemas.input = [{ id: `${cleanBaseName}_input_schema`, name: `${label} Input Schema`, fields, isTemporary: false, isMaterialized: false }];
      schemas.output = { id: `${cleanBaseName}_output_schema`, name: `${label} Output Schema`, fields, isTemporary: false, isMaterialized: false };
    }

    const nodeData: CanvasNodeData = {
      id: `node-${Date.now()}-${cleanBaseName}`,
      name: label,
      type: mapComponentKeyToNodeType(componentDef.id, componentRole),
      nodeType: componentRole === 'INPUT' ? 'input' : componentRole === 'OUTPUT' ? 'output' : 'transform',
      componentCategory: componentDef.category,
      position,
      size: { width: componentDef.defaultDimensions.width * 2, height: componentDef.defaultDimensions.height * 2 },
      metadata: {
        configuration,
        schemas,
        description: componentDef.description,
        tags: [],
        version: '1.0',
        createdBy: 'canvas',
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
        displayName: componentDef.displayName,
        cleanType: cleanBaseName,
        scaleFactor: 0.6,
        visualScaling: { fontSizeScale: 0.6, iconScale: 0.6, handleScale: 0.6 },
        repositoryNodeId: data.metadata?.repositoryNodeId,
        repositoryNodeType: data.metadata?.repositoryNodeType,
        originalNodeName: data.metadata?.originalNodeName,
        originalNodeType: data.metadata?.originalNodeType,
        fullRepositoryMetadata: data.metadata?.repositoryMetadata,
        extractedColumns: columns,
        dragMetadata: data.metadata,
        source: data.source,
        sourceType: data.metadata?.sourceType,
        category: componentDef.category,
        isDataSource: componentDef.category === 'input' || componentDef.category === 'output',
      },
      status: NodeStatus.IDLE,
      draggable: true,
      technology: componentDef.id,
      visualProperties: { color: getCategoryColor(componentDef.category), icon: componentDef.icon },
    };

    // 🔧 NEW: For output components, copy the table name from repository metadata
    if (componentDef.category === 'output' && data.metadata?.repositoryMetadata?.postgresTableName) {
      nodeData.metadata!.postgresTableName = data.metadata.repositoryMetadata.postgresTableName;
    }

    const newNode: Node<CanvasNodeData> = { 
      id: nodeData.id, 
      type: 'talendNode', 
      position, 
      data: nodeData, 
      style: { width: nodeData.size.width, height: nodeData.size.height }, 
      draggable: true, 
      selectable: true, 
      connectable: true 
    };
    addNodes(newNode);
    const updatedNodes = [...nodes, newNode];
    setTimeout(() => syncNodesAndEdges(updatedNodes, edges), 0);
    if (onNodeMetadataUpdate) onNodeMetadataUpdate(newNode.id, nodeData.metadata!);
    debouncedAutoSave();

    if (isInputCategory) {
      setPendingRoleSelection({
        nodeId: newNode.id,
        componentId: componentDef.id,
        displayName: cleanBaseName,
        position,
        dropPosition: { x: event.clientX, y: event.clientY },
        componentDef,
        nodeData,
      });
      showValidationFeedback(`Please select role for ${cleanBaseName}`, 'info', position);
    } else {
      showValidationFeedback(`Added ${label}`, 'success', position);
    }
  } catch (error) {
    console.error('Drop error:', error);
    showValidationFeedback('Failed to add component', 'error', { x: event.clientX, y: event.clientY });
  }
}, [reactFlowInstance, screenToFlowPosition, addNodes, nodes, edges, syncNodesAndEdges, onNodeMetadataUpdate, showValidationFeedback, debouncedAutoSave]);

  const onDragOver = useCallback((event: React.DragEvent) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'copy';
  }, []);

  const handleNodeUpdate = useCallback((nodeId: string, updatedData: Partial<CanvasNodeData>) => {
    setNodes(prev => {
      const updated = prev.map(node => {
        if (node.id === nodeId) {
          const newData = { ...node.data, ...updatedData, metadata: { ...node.data.metadata, ...updatedData.metadata, updatedAt: new Date().toISOString() } };
          if (onNodeMetadataUpdate) onNodeMetadataUpdate(nodeId, newData.metadata!);
          return { ...node, data: newData };
        }
        return node;
      });
      syncNodesAndEdges(updated, edges);
      return updated;
    });
    debouncedAutoSave();
  }, [onNodeMetadataUpdate, syncNodesAndEdges, edges, debouncedAutoSave]);

  // ==================== DOUBLE-CLICK HANDLER ====================
  const handleCanvasNodeDoubleClick = useCallback((event: CustomEvent) => {
    const { componentMetadata, nodeMetadata } = event.detail;
    const metadata = nodeMetadata || componentMetadata;
    if (!metadata?.id) return;
    const node = nodes.find(n => n.id === metadata.id) as Node<CanvasNodeData> | undefined;
    if (!node) return;

    const nodeData = node.data;
    const nodeType = nodeData.type;

    if (nodeType === NodeType.MAP) {
      const editorData = getConnectedColumns(metadata.id, nodes, edges);
      const transformedData = {
        ...editorData,
        inputColumns: editorData.inputColumns.map(col => ({ name: col.name, type: col.type || 'STRING' })),
        outputColumns: editorData.outputColumns.map(col => ({ name: col.name, type: col.type || 'STRING' })),
      };
      setState(prev => ({ ...prev, mapEditorState: { isOpen: true, data: transformedData, nodeMetadata: nodeData } }));
      showValidationFeedback(`Opening Map Editor for ${nodeData.name || metadata.id}`, 'info', { x: 100, y: 100 });
      return;
    }

    const inputNodes = getConnectedNodes(metadata.id, edges, nodes, 'input');
    const inputColumns = inputNodes.flatMap(n => extractColumnsFromNode(n));
    const uniqueInputColumns = inputColumns.filter((col, idx, self) => self.findIndex(c => c.name === col.name) === idx);
    const simpleInputColumns: SimpleColumn[] = uniqueInputColumns.map(({ name, type, id }) => ({ name, type, id }));

    const getInitialConfig = <T,>(type: string): T | undefined => {
      if (type === 'FILTER') return nodeData.metadata?.configuration?.type === 'FILTER' ? (nodeData.metadata.configuration as any).config : undefined;
      if (type === 'SORT') return nodeData.metadata?.configuration?.type === 'SORT' ? (nodeData.metadata.configuration as any).config : undefined;
      if (type === 'AGGREGATE') return nodeData.metadata?.configuration?.type === 'AGGREGATE' ? (nodeData.metadata.configuration as any).config : undefined;
      if (type === 'CONVERT') return nodeData.metadata?.configuration?.type === 'CONVERT' ? (nodeData.metadata.configuration as any).config : undefined;
      if (type === 'REPLACE') return nodeData.metadata?.configuration?.type === 'REPLACE' ? (nodeData.metadata.configuration as any).config : undefined;
      if (type === 'EXTRACT_JSON_FIELDS') return nodeData.metadata?.configuration?.type === 'EXTRACT_JSON_FIELDS' ? (nodeData.metadata.configuration as any).config : undefined;
      if (type === 'EXTRACT_DELIMITED') return nodeData.metadata?.configuration?.type === 'EXTRACT_DELIMITED' ? (nodeData.metadata.configuration as any).config : undefined;
      if (type === 'EXTRACT_XML_FIELD') return nodeData.metadata?.configuration?.type === 'EXTRACT_XML_FIELD' ? (nodeData.metadata.configuration as any).config : undefined;
      if (type === 'REPLACE_LIST') return nodeData.metadata?.configuration?.type === 'REPLACE_LIST' ? (nodeData.metadata.configuration as any).config : undefined;
      if (type === 'NORMALIZE_NUMBER') return nodeData.metadata?.configuration?.type === 'NORMALIZE_NUMBER' ? (nodeData.metadata.configuration as any).config : undefined;
      if (type === 'NORMALIZE') return nodeData.metadata?.configuration?.type === 'NORMALIZE' ? (nodeData.metadata.configuration as any).config : undefined;
      if (type === 'REPLICATE') return nodeData.metadata?.configuration?.type === 'REPLICATE' ? (nodeData.metadata.configuration as any).config : undefined;
      if (type === 'MATCH_GROUP') return nodeData.metadata?.configuration?.type === 'MATCH_GROUP' ? (nodeData.metadata.configuration as any).config : undefined;
      if (type === 'FILTER_COLUMNS') return nodeData.metadata?.configuration?.type === 'FILTER_COLUMNS' ? (nodeData.metadata.configuration as any).config : undefined;
      if (type === 'FILE_LOOKUP') return nodeData.metadata?.configuration?.type === 'FILE_LOOKUP' ? (nodeData.metadata.configuration as any).config : undefined;
      if (type === 'UNPIVOT_ROW') return nodeData.metadata?.configuration?.type === 'UNPIVOT_ROW' ? (nodeData.metadata.configuration as any).config : undefined;
      if (type === 'UNITE') return nodeData.metadata?.configuration?.type === 'UNITE' ? (nodeData.metadata.configuration as any).config : undefined;
      if (type === 'UNIQ_ROW') return nodeData.metadata?.configuration?.type === 'UNIQ_ROW' ? (nodeData.metadata.configuration as any).config : undefined;
      if (type === 'SPLIT_ROW') return nodeData.metadata?.configuration?.type === 'NORMALIZE' ? (nodeData.metadata.configuration as any).config : undefined;
      if (type === 'PIVOT_TO_COLUMNS_DELIMITED') return nodeData.metadata?.configuration?.type === 'PIVOT_TO_COLUMNS_DELIMITED' ? (nodeData.metadata.configuration as any).config : undefined;
      if (type === 'DENORMALIZE_SORTED_ROW') return nodeData.metadata?.configuration?.type === 'DENORMALIZE_SORTED_ROW' ? (nodeData.metadata.configuration as any).config : undefined;
      if (type === 'DENORMALIZE') return nodeData.metadata?.configuration?.type === 'DENORMALIZE' ? (nodeData.metadata.configuration as any).config : undefined;
      // New types
      if (type === 'EXTRACT_REGEX_FIELDS') return nodeData.metadata?.configuration?.type === 'EXTRACT_REGEX_FIELDS' ? (nodeData.metadata.configuration as any).config : undefined;
      if (type === 'PARSE_RECORD_SET') return nodeData.metadata?.configuration?.type === 'PARSE_RECORD_SET' ? (nodeData.metadata.configuration as any).config : undefined;
      if (type === 'SAMPLE_ROW') return nodeData.metadata?.configuration?.type === 'SAMPLE_ROW' ? (nodeData.metadata.configuration as any).config : undefined;
      if (type === 'SCHEMA_COMPLIANCE_CHECK') return nodeData.metadata?.configuration?.type === 'SCHEMA_COMPLIANCE_CHECK' ? (nodeData.metadata.configuration as any).config : undefined;
      if (type === 'ADD_CRC_ROW') return nodeData.metadata?.configuration?.type === 'ADD_CRC_ROW' ? (nodeData.metadata.configuration as any).config : undefined;
      if (type === 'DATA_MASKING') return nodeData.metadata?.configuration?.type === 'DATA_MASKING' ? (nodeData.metadata.configuration as any).config : undefined;
      return undefined;
    };

    if (nodeType === NodeType.JOIN) {
      if (inputNodes.length < 2) {
        showValidationFeedback('Join requires at least two input connections.', 'error'); return;
      }
      const leftNode = inputNodes[0];
      const rightNode = inputNodes[1];
      const leftSchema = {
        id: leftNode.id,
        name: leftNode.data.name || leftNode.id,
        fields: extractColumnsFromNode(leftNode).map((col, idx) => ({ id: col.id || `${leftNode.id}_${col.name}_${idx}`, name: col.name, type: (col.type as any) || 'STRING', nullable: true, isKey: false })),
      };
      const rightSchema = {
        id: rightNode.id,
        name: rightNode.data.name || rightNode.id,
        fields: extractColumnsFromNode(rightNode).map((col, idx) => ({ id: col.id || `${rightNode.id}_${col.name}_${idx}`, name: col.name, type: (col.type as any) || 'STRING', nullable: true, isKey: false })),
      };
      setActiveEditor({ type: 'join', nodeId: node.id, nodeMetadata: nodeData, leftSchema, rightSchema, initialConfig: getInitialConfig('JOIN') });
      return;
    }

    switch (nodeType) {
      case NodeType.SORT_ROW:
        setActiveEditor({ type: 'sort', nodeId: node.id, nodeMetadata: nodeData, inputColumns: simpleInputColumns, initialConfig: getInitialConfig('SORT') }); break;
      case NodeType.REPLACE:
        setActiveEditor({ type: 'replace', nodeId: node.id, nodeMetadata: nodeData, inputColumns: simpleInputColumns, outputColumns: simpleInputColumns, initialConfig: getInitialConfig('REPLACE') }); break;
      case NodeType.FILTER_ROW:
        setActiveEditor({ type: 'filter', nodeId: node.id, nodeMetadata: nodeData, inputColumns: simpleInputColumns, initialConfig: getInitialConfig('FILTER') }); break;
      case NodeType.AGGREGATE_ROW:
        setActiveEditor({ type: 'aggregate', nodeId: node.id, nodeMetadata: nodeData, inputColumns: simpleInputColumns, initialConfig: getInitialConfig('AGGREGATE') }); break;
      case NodeType.CONVERT_TYPE:
        setActiveEditor({ type: 'convert', nodeId: node.id, nodeMetadata: nodeData, inputColumns: simpleInputColumns, outputColumns: simpleInputColumns, initialConfig: getInitialConfig('CONVERT') }); break;
      case NodeType.EXTRACT_DELIMITED_FIELDS:
        setActiveEditor({ type: 'extractDelimited', nodeId: node.id, nodeMetadata: nodeData, inputColumns: simpleInputColumns, initialConfig: getInitialConfig('EXTRACT_DELIMITED') }); break;
      case NodeType.EXTRACT_JSON_FIELDS:
        setActiveEditor({ type: 'extractJSON', nodeId: node.id, nodeMetadata: nodeData, inputColumns: simpleInputColumns, initialConfig: getInitialConfig('EXTRACT_JSON_FIELDS') }); break;
      case NodeType.EXTRACT_XML_FIELD:
        setActiveEditor({ type: 'extractXML', nodeId: node.id, nodeMetadata: nodeData, inputColumns: simpleInputColumns, initialConfig: getInitialConfig('EXTRACT_XML_FIELD') }); break;
      case NodeType.NORMALIZE:
        setActiveEditor({ type: 'normalize', nodeId: node.id, nodeMetadata: nodeData, inputColumns: simpleInputColumns, initialConfig: getInitialConfig('NORMALIZE') }); break;
      case NodeType.NORMALIZE_NUMBER:
        setActiveEditor({ type: 'normalizeNumber', nodeId: node.id, nodeMetadata: nodeData, inputColumns: simpleInputColumns, initialConfig: getInitialConfig('NORMALIZE_NUMBER') }); break;
      case NodeType.REPLACE_LIST:
        setActiveEditor({
          type: 'replaceList',
          nodeId: node.id,
          nodeMetadata: nodeData,
          inputSchema: {
            id: `input-${node.id}`,
            name: `Input Schema for ${nodeData.name}`,
            fields: uniqueInputColumns.map((col, idx) => ({ id: col.id || `${node.id}_${col.name}_${idx}`, name: col.name, type: (col.type as any) || 'STRING', nullable: true, isKey: false })),
            isTemporary: false, isMaterialized: false,
          },
          initialConfig: getInitialConfig('REPLACE_LIST'),
        }); break;
      case NodeType.REPLICATE:
        setActiveEditor({ type: 'replicate', nodeId: node.id, nodeMetadata: nodeData, inputColumns: simpleInputColumns, initialConfig: getInitialConfig('REPLICATE') }); break;
      case NodeType.RECORD_MATCHING:
        setActiveEditor({
          type: 'recordMatching',
          nodeId: node.id,
          nodeMetadata: nodeData,
          inputFields: uniqueInputColumns.map((col, idx) => ({ id: col.id || `${node.id}_${col.name}_${idx}`, name: col.name, type: (col.type as any) || 'STRING', nullable: true, isKey: false })),
          initialConfig: getInitialConfig('MATCH_GROUP'),
        }); break;
      case NodeType.MATCH_GROUP:
        setActiveEditor({ type: 'matchGroup', nodeId: node.id, nodeMetadata: nodeData, inputColumns: simpleInputColumns, initialConfig: getInitialConfig('MATCH_GROUP') }); break;
      case NodeType.FILTER_COLUMNS:
        setActiveEditor({
          type: 'filterColumns',
          nodeId: node.id,
          nodeMetadata: nodeData,
          inputSchema: {
            id: `input-${node.id}`,
            name: `Input Schema for ${nodeData.name}`,
            fields: uniqueInputColumns.map((col, idx) => ({ id: col.id || `${node.id}_${col.name}_${idx}`, name: col.name, type: (col.type as any) || 'STRING', nullable: true, isKey: false })),
            isTemporary: false, isMaterialized: false,
          },
          initialConfig: getInitialConfig('FILTER_COLUMNS'),
        }); break;
      case NodeType.FILE_LOOKUP:
        setActiveEditor({ type: 'fileLookup', nodeId: node.id, nodeMetadata: nodeData, inputColumns: simpleInputColumns, initialConfig: getInitialConfig('FILE_LOOKUP') }); break;
      case NodeType.UNPIVOT_ROW:
        setActiveEditor({ type: 'unpivotRow', nodeId: node.id, nodeMetadata: nodeData, inputColumns: simpleInputColumns, initialConfig: getInitialConfig('UNPIVOT_ROW') }); break;
      case NodeType.UNITE:
        setActiveEditor({
          type: 'unite',
          nodeId: node.id,
          nodeMetadata: nodeData,
          inputSchemas: inputNodes.map(n => {
            const schema = n.data.metadata?.schemas?.output;
            return { id: n.id, name: n.data.name || n.id, fields: schema?.fields.map(f => ({ name: f.name, type: f.type, nullable: f.nullable })) || [] };
          }),
          initialConfig: getInitialConfig('UNITE'),
        }); break;
      case NodeType.UNIQ_ROW:
        setActiveEditor({ type: 'uniqRow', nodeId: node.id, nodeMetadata: nodeData, inputColumns: simpleInputColumns, initialConfig: getInitialConfig('UNIQ_ROW') }); break;
      case NodeType.SPLIT_ROW:
        setActiveEditor({ type: 'splitRow', nodeId: node.id, nodeMetadata: nodeData, inputColumns: simpleInputColumns, initialConfig: getInitialConfig('SPLIT_ROW') }); break;
      case NodeType.PIVOT_TO_COLUMNS_DELIMITED:
        setActiveEditor({ type: 'pivotToColumnsDelimited', nodeId: node.id, nodeMetadata: nodeData, inputColumns: simpleInputColumns, initialConfig: getInitialConfig('PIVOT_TO_COLUMNS_DELIMITED') }); break;
      case NodeType.DENORMALIZE_SORTED_ROW:
        setActiveEditor({ type: 'denormalizeSortedRow', nodeId: node.id, nodeMetadata: nodeData, inputColumns: simpleInputColumns, initialConfig: getInitialConfig('DENORMALIZE_SORTED_ROW') }); break;
      case NodeType.DENORMALIZE:
        setActiveEditor({ type: 'denormalize', nodeId: node.id, nodeMetadata: nodeData, inputColumns: simpleInputColumns, initialConfig: getInitialConfig('DENORMALIZE') }); break;
      // New cases for missing node types
      case NodeType.EXTRACT_REGEX_FIELDS:
        setActiveEditor({ type: 'extractRegex', nodeId: node.id, nodeMetadata: nodeData, inputColumns: simpleInputColumns, initialConfig: getInitialConfig('EXTRACT_REGEX_FIELDS') }); break;
      case NodeType.PARSE_RECORD_SET:
        setActiveEditor({ type: 'parseRecordSet', nodeId: node.id, nodeMetadata: nodeData, inputColumns: simpleInputColumns, initialConfig: getInitialConfig('PARSE_RECORD_SET') }); break;
      case NodeType.SAMPLE_ROW:
        setActiveEditor({ type: 'sampleRow', nodeId: node.id, nodeMetadata: nodeData, inputColumns: simpleInputColumns, initialConfig: getInitialConfig('SAMPLE_ROW') }); break;
      case NodeType.SCHEMA_COMPLIANCE_CHECK:
        setActiveEditor({ type: 'schemaComplianceCheck', nodeId: node.id, nodeMetadata: nodeData, inputColumns: simpleInputColumns, initialConfig: getInitialConfig('SCHEMA_COMPLIANCE_CHECK') }); break;
      case NodeType.ADD_CRC_ROW:
        setActiveEditor({ type: 'addCRCRow', nodeId: node.id, nodeMetadata: nodeData, inputColumns: simpleInputColumns, initialConfig: getInitialConfig('ADD_CRC_ROW') }); break;
      case NodeType.DATA_MASKING:
        setActiveEditor({ type: 'dataMasking', nodeId: node.id, nodeMetadata: nodeData, inputColumns: simpleInputColumns, initialConfig: getInitialConfig('DATA_MASKING') }); break;
      default:
        showValidationFeedback(`No editor available for ${nodeType}`, 'info');
    }
  }, [nodes, edges, showValidationFeedback]);

  useEffect(() => {
    window.addEventListener('canvas-node-double-click', handleCanvasNodeDoubleClick as EventListener);
    const handleTMapDoubleClickEvent = (event: Event) => {
      const customEvent = event as CustomEvent;
      const editorData = getConnectedColumns(customEvent.detail.nodeId, nodes, edges);
      const transformedData = {
        ...editorData,
        inputColumns: editorData.inputColumns.map(col => ({ name: col.name, type: col.type || 'STRING' })),
        outputColumns: editorData.outputColumns.map(col => ({ name: col.name, type: col.type || 'STRING' })),
      };
      setState(prev => ({ ...prev, mapEditorState: { isOpen: true, data: transformedData, nodeMetadata: nodes.find(n => n.id === customEvent.detail.nodeId)?.data } }));
    };
    window.addEventListener('canvas-tmap-double-click', handleTMapDoubleClickEvent);
    return () => {
      window.removeEventListener('canvas-node-double-click', handleCanvasNodeDoubleClick as EventListener);
      window.removeEventListener('canvas-tmap-double-click', handleTMapDoubleClickEvent);
    };
  }, [handleCanvasNodeDoubleClick, nodes, edges]);

const handleRoleCancel = useCallback(() => {
  if (!pendingRoleSelection) return;
  setNodes(prev => {
    const updated = prev.filter(node => node.id !== pendingRoleSelection.nodeId);
    syncNodesAndEdges(updated, edges);
    return updated;
  });
  nameGenerator.decrementCounter(pendingRoleSelection.componentId, 'TRANSFORM');
  debouncedAutoSave();
  setPendingRoleSelection(null);
  showValidationFeedback('Component placement cancelled', 'info', pendingRoleSelection.position);
}, [pendingRoleSelection, edges, syncNodesAndEdges, debouncedAutoSave, showValidationFeedback]);

const handleRoleSelect = useCallback((selectedRole: 'INPUT' | 'OUTPUT') => {
  if (!pendingRoleSelection) return;
  const nodeToUpdate = nodes.find(n => n.id === pendingRoleSelection.nodeId);
  if (!nodeToUpdate) return;

  // Use actual name from repository metadata
  const actualName = pendingRoleSelection.nodeData.metadata?.fullRepositoryMetadata?.name ||
                     pendingRoleSelection.nodeData.metadata?.repositoryMetadata?.name ||
                     pendingRoleSelection.displayName;

  // Create configuration with correct role
  const newConfiguration = createInitialComponentConfiguration(
    pendingRoleSelection.componentId,
    selectedRole,
    pendingRoleSelection.nodeData.metadata
  );

  // Extract columns for schema
  const columns = extractColumnsFromDragData(pendingRoleSelection.nodeData.metadata);
  const fields = columns.map((col: any, idx) => ({
    id: col.id || `${pendingRoleSelection.componentId}_${idx}`,
    name: col.name || `Column_${idx + 1}`,
    type: col.type || 'STRING',
    length: col.length,
    precision: col.precision,
    scale: col.scale,
    nullable: col.nullable !== false,
    isKey: col.isKey || col.primaryKey || false,
    defaultValue: col.defaultValue,
    description: col.description,
    originalName: col.originalName,
    transformation: col.expression,
    metadata: { original: col },
  }));

  // Build schemas based on selected role
  const schemas: any = {};
  if (selectedRole === 'INPUT') {
    schemas.output = {
      id: `${pendingRoleSelection.componentId}_output_schema`,
      name: `${actualName} Output Schema`,
      fields,
      isTemporary: false,
      isMaterialized: false
    };
    if (newConfiguration.type === 'INPUT') (newConfiguration.config as any).schema = schemas.output;
  } else {
    schemas.input = [{
      id: `${pendingRoleSelection.componentId}_input_schema`,
      name: `${actualName} Input Schema`,
      fields,
      isTemporary: false,
      isMaterialized: false
    }];
  }

  // Determine node types
  const nodeTypeEnum = selectedRole === 'INPUT' ? NodeType.INPUT : NodeType.OUTPUT;
  const visualNodeType = selectedRole === 'INPUT' ? 'input' : 'output';

  // Update the node
  setNodes(prev => {
    const updated = prev.map(node => {
      if (node.id === pendingRoleSelection.nodeId) {
        const updatedData = {
          ...pendingRoleSelection.nodeData,
          name: actualName,
          type: nodeTypeEnum,
          nodeType: visualNodeType,
          componentType: selectedRole,
          metadata: {
            ...pendingRoleSelection.nodeData.metadata,
            configuration: newConfiguration,
            schemas,
            updatedAt: new Date().toISOString(),
            isDataSource: true,
            userSelectedRole: selectedRole
          }
        };
        if (onNodeMetadataUpdate) onNodeMetadataUpdate(node.id, updatedData.metadata!);
        return { ...node, data: updatedData };
      }
      return node;
    });
    syncNodesAndEdges(updated, edges);
    return updated;
  });

  debouncedAutoSave();
  setPendingRoleSelection(null);
  showValidationFeedback(`Role set to ${selectedRole} for ${actualName}`, 'success', pendingRoleSelection.position);
}, [pendingRoleSelection, nodes, edges, onNodeMetadataUpdate, syncNodesAndEdges, debouncedAutoSave, showValidationFeedback]);

  // ==================== MAP EDITOR SAVE ====================
const handleMapEditorSave = useCallback((config: any) => {
  const { mapEditorState } = state;
  if (!mapEditorState.isOpen || !mapEditorState.nodeMetadata) return;

  const nodeId = mapEditorState.nodeMetadata.id;

  // Helper to map PostgreSQL type to DataType union
  const mapPostgreSQLToDataType = (pgType: string): DataType => {
    const upper = pgType.toUpperCase();
    switch (upper) {
      case 'TEXT':
      case 'VARCHAR':
      case 'CHAR':
        return 'STRING';
      case 'INTEGER':
      case 'SMALLINT':
      case 'BIGINT':
      case 'SERIAL':
        return 'INTEGER';
      case 'DECIMAL':
      case 'NUMERIC':
      case 'REAL':
      case 'DOUBLE PRECISION':
        return 'DECIMAL';
      case 'BOOLEAN':
        return 'BOOLEAN';
      case 'DATE':
        return 'DATE';
      case 'TIMESTAMP':
      case 'TIMESTAMPTZ':
        return 'TIMESTAMP';
      case 'JSON':
      default:
        return 'STRING';
    }
  };

  // Find connected output node(s) and get target schema
  const outgoingEdges = edges.filter(edge => edge.source === nodeId);
  const outputNodes = outgoingEdges
    .map(edge => nodes.find(n => n.id === edge.target))
    .filter((n): n is Node<CanvasNodeData> =>
      n !== undefined && n.data?.nodeType === 'output'
    );

  // Build column name -> PostgreSQL data type map
  const targetTypeMap = new Map<string, string>();
  for (const outNode of outputNodes) {
    const inputSchemas = outNode.data.metadata?.schemas?.input;
    if (inputSchemas && inputSchemas.length > 0) {
      const fields = inputSchemas[0].fields;
      for (const field of fields) {
        const pgType = mapToPostgresType(field.type, field.length, field.precision, field.scale);
        targetTypeMap.set(field.name, pgType);
      }
    }
  }

  setNodes(prev => {
    const updated = prev.map(node => {
      if (node.id === nodeId) {
        const tMapData = node.data as CanvasNodeData;
        const currentOutputSchema = tMapData.metadata?.schemas?.output;
        let updatedOutputSchema = currentOutputSchema;

        if (currentOutputSchema && targetTypeMap.size > 0) {
          const updatedFields = currentOutputSchema.fields.map(field => {
            const expectedType = targetTypeMap.get(field.name);
            if (expectedType && expectedType !== 'TEXT') {
              const dataType = mapPostgreSQLToDataType(expectedType);
              return { ...field, type: dataType };
            }
            return field;
          });
          updatedOutputSchema = { ...currentOutputSchema, fields: updatedFields };
        }

        // Build the new metadata with explicit ComponentConfiguration cast
        const updatedMetadata: UnifiedNodeMetadata = {
          ...(tMapData.metadata || {}),
          configuration: { type: 'MAP', config } as ComponentConfiguration,
          schemas: {
            ...(tMapData.metadata?.schemas || {}),
            output: updatedOutputSchema,
          },
          compilerMetadata: {
            ...(tMapData.metadata?.compilerMetadata || {}),
            lastModified: new Date().toISOString(),
          },
        };

        if (onNodeMetadataUpdate) onNodeMetadataUpdate(nodeId, updatedMetadata);
        return { ...node, data: { ...tMapData, metadata: updatedMetadata } };
      }
      return node;
    });
    syncNodesAndEdges(updated, edges);
    return updated;
  });

  debouncedAutoSave();
  showValidationFeedback(
    `Saved ${config.transformations.length} transformations with proper data types`,
    'success',
    { x: 100, y: 100 }
  );
  setState(prev => ({
    ...prev,
    mapEditorState: { isOpen: false, data: null, nodeMetadata: undefined },
  }));
}, [state.mapEditorState, onNodeMetadataUpdate, syncNodesAndEdges, edges, nodes, debouncedAutoSave, showValidationFeedback]);

  const closeTMapEditor = useCallback(() => {
    setState(prev => ({ ...prev, mapEditorState: { isOpen: false, data: null, nodeMetadata: undefined } }));
  }, []);

  // ==================== GENERIC CONFIGURATION UPDATE ====================
  const updateNodeConfiguration = useCallback((nodeId: string, configUnion: any) => {
    setNodes(prev => {
      const updated = prev.map(node => {
        if (node.id === nodeId) {
          const updatedMetadata = { ...node.data.metadata, configuration: configUnion, compilerMetadata: { ...node.data.metadata?.compilerMetadata, lastModified: new Date().toISOString() } };
          if (onNodeMetadataUpdate) onNodeMetadataUpdate(nodeId, updatedMetadata);
          return { ...node, data: { ...node.data, metadata: updatedMetadata } };
        }
        return node;
      });
      syncNodesAndEdges(updated, edges);
      return updated;
    });
    debouncedAutoSave();
    showValidationFeedback('Configuration saved', 'success', { x: 100, y: 100 });
  }, [onNodeMetadataUpdate, syncNodesAndEdges, edges, debouncedAutoSave, showValidationFeedback]);

  // ==================== SQL GENERATION ON RUN ====================
  useEffect(() => {
    const handleToolbarRun = async (event: Event) => {
      const customEvent = event as CustomEvent;
      const { jobName, nodes: runNodes, edges: runEdges } = customEvent.detail;
      dispatch(addLog({ level: 'INFO', message: `Starting run for job: ${jobName}`, source: 'Canvas' }));
      try {
        // Inside Canvas.tsx, replace the existing mapping block in handleToolbarRun with the following:

// Inside Canvas.tsx, in handleToolbarRun, replace the mapping block with:

const canvasNodes: PipelineCanvasNode[] = runNodes.map((node: Node<CanvasNodeData>) => {
  const unified = node.data;
  const config = unified.metadata?.configuration;
  const nodeType = unified.type;

  // Base metadata that every node shares
  const baseMetadata: any = {
    postgresConfig: unified.metadata?.postgresConfig,
    description: unified.metadata?.description,
  };

  // If no configuration exists, return a minimal node (pipeline may use fallback)
  if (!config) {
    console.warn(`[Canvas] Node ${unified.id} (${unified.name}) has no configuration.`);
    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: baseMetadata,
    };
  }

  // ----- CONFIGURATION EXTRACTION PER NODE TYPE -----
  // INPUT / OUTPUT (handled separately via role)
  if (isInputConfig(config)) {
    const fields = config.config.schema?.fields || [];
    const sourceTableName =
      config.config.sourceDetails.tableName ||
      unified.metadata?.postgresTableName ||
      unified.metadata?.fullRepositoryMetadata?.postgresTableName ||
      unified.name;

    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: {
        ...baseMetadata,
        tableMapping: {
          schema: 'public',
          name: sourceTableName,
          columns: fieldsToPostgresColumns(fields),
        },
        sourceTableName,
      },
    };
  }

  if (isOutputConfig(config)) {
    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: {
        ...baseMetadata,
        schemaMappings: config.config.schemaMapping,
        targetTableName: config.config.targetDetails.tableName,
      },
    };
  }

  // 2. TRANSFORMATION NODES – full mapping for every config type

  // tMap
  if (isMapConfig(config)) {
    const mapConf = config.config;
    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: {
        ...baseMetadata,
        transformationRules: mapConf.transformations.map((t, idx) => ({
          id: t.id,
          type: 'map',
          params: t,
          order: idx,
        })),
        schemaMappings: mapConf.transformations.map((t) => ({
          sourceColumn: t.sourceField,
          targetColumn: t.targetField,
          transformation: t.expression,
          dataTypeConversion: t.dataType
            ? { from: 'unknown', to: mapDataTypeToPostgreSQL(t.dataType) }
            : undefined,
          isRequired: true,
          defaultValue: t.defaultValue,
        })),
        joinConfig: mapConf.joins
          ? {
              type: mapConf.joins.length > 0 ? 'INNER' : undefined,
              condition: mapConf.joins
                .map((j) => `${j.leftTable}.${j.leftField} = ${j.rightTable}.${j.rightField}`)
                .join(' AND '),
            }
          : undefined,
        filterConfig: mapConf.filters
          ? {
              condition: mapConf.filters
                .map((f) => `${f.field} ${f.operator} ${f.value}`)
                .join(` ${mapConf.filters.length > 1 ? 'AND' : ''} `),
            }
          : undefined,
      },
    };
  }

  // tJoin
  if (isJoinConfig(config)) {
    const joinConf = config.config;
    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: {
        ...baseMetadata,
        joinConfig: {
          type: joinConf.joinType,
          condition: joinConf.joinConditions
            .map((jc) => `${jc.leftTable}.${jc.leftField} = ${jc.rightTable}.${jc.rightField}`)
            .join(' AND '),
        },
        transformationRules: joinConf.joinConditions.map((jc, idx) => ({
          id: jc.id,
          type: 'join',
          params: jc,
          order: idx,
        })),
      },
    };
  }

  // tFilterRow
  if (isFilterConfig(config)) {
    const filterConf = config.config;
    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: {
        ...baseMetadata,
        filterConfig: {
          condition: filterConf.filterConditions
            .map((fc) => `${fc.field} ${fc.operator} ${fc.value}`)
            .join(` ${filterConf.filterLogic} `),
          operation: 'INCLUDE',
        },
        transformationRules: filterConf.filterConditions.map((fc, idx) => ({
          id: fc.id,
          type: 'filter',
          params: fc,
          order: idx,
        })),
      },
    };
  }

  // tAggregateRow
  if (isAggregateConfig(config)) {
    const aggConf = config.config;
    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: {
        ...baseMetadata,
        aggregationConfig: {
          groupBy: aggConf.groupByFields,
          aggregates: aggConf.aggregateFunctions.map((af) => ({
            column: af.field,
            function: af.function,
            alias: af.alias,
            distinct: af.distinct,
          })),
        },
        transformationRules: aggConf.aggregateFunctions.map((af, idx) => ({
          id: af.id,
          type: 'aggregate',
          params: af,
          order: idx,
        })),
      },
    };
  }

  // tSortRow
  if (isSortConfig(config)) {
    const sortConf = config.config;
    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: {
        ...baseMetadata,
        sortConfig: {
          columns: sortConf.sortFields.map((sf) => ({
            column: sf.field,
            direction: sf.direction,
            nullsFirst: sf.nullsFirst,
          })),
        },
      },
    };
  }

  // tConvertType
  if (isConvertConfig(config)) {
    const convConf = config.config;
    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: {
        ...baseMetadata,
        convertConfig: {
          conversions: convConf.rules.map((r) => ({
            sourceColumn: r.sourceColumn,
            targetType: r.targetType,
            targetAlias: r.targetColumn,
          })),
        },
      },
    };
  }

  // tReplace
  if (isReplaceConfig(config)) {
    const replConf = config.config;
    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: {
        ...baseMetadata,
        replaceConfig: {
          rules: replConf.rules.map((r) => ({
            field: r.column,
            searchValue: r.searchValue,
            replacement: r.replacement,
            regex: r.regex,
          })),
        },
      },
    };
  }

  // tReplaceList (same config type as Replace, but test expects replaceListConfig)
  if (config.type === 'REPLACE_LIST') {
    const replConf = config.config as ReplaceComponentConfiguration; // same shape
    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: {
        ...baseMetadata,
        replaceListConfig: {
          column: replConf.rules[0]?.column,
          pairs: replConf.rules.map((r) => ({
            search: r.searchValue,
            replace: r.replacement,
            regex: r.regex,
          })),
        },
      },
    };
  }

  // tExtractDelimitedFields
  if (isExtractDelimitedConfig(config)) {
    const extConf = config.config;
    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: {
        ...baseMetadata,
        extractDelimitedConfig: {
          sourceColumn: extConf.sourceColumn,
          delimiter: extConf.delimiter,
          outputColumns: extConf.outputColumns.map((c) => ({
            name: c.name,
            position: c.position,
            type: c.type,
          })),
        },
      },
    };
  }

  // tExtractJSONFields
  if (config.type === 'EXTRACT_JSON_FIELDS') {
    const jsonConf = config.config as ExtractJSONFieldsConfiguration;
    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: {
        ...baseMetadata,
        extractJSONConfig: {
          sourceColumn: jsonConf.sourceColumn,
          mappings: jsonConf.outputColumns.map((c) => ({
            jsonPath: c.jsonPath,
            targetColumn: c.name,
            dataType: c.type,
          })),
        },
      },
    };
  }

  // tExtractXMLField
  if (isExtractXMLFieldConfig(config)) {
    const xmlConf = config.config;
    const first = xmlConf.xpathExpressions[0];
    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: {
        ...baseMetadata,
        extractXMLConfig: {
          sourceColumn: xmlConf.sourceColumn,
          xpath: first?.xpath,
          targetColumn: first?.outputColumn,
        },
      },
    };
  }

  // tNormalize (split row)
  if (isNormalizeConfig(config)) {
    const normConf = config.config;
    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: {
        ...baseMetadata,
        normalizeConfig: {
          sourceColumn: normConf.sourceColumn,
          decimalSeparator: '.',
          groupingSeparator: ',',
        },
      },
    };
  }

  // tNormalizeNumber
  if (isNormalizeNumberConfig(config)) {
    const normNumConf = config.config;
    const firstRule = normNumConf.rules[0];
    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: {
        ...baseMetadata,
        normalizeNumberConfig: {
          sourceColumn: firstRule?.sourceColumn,
          targetType: firstRule?.outputDataType,
        },
      },
    };
  }

  // tReplicate
  if (isReplicateConfig(config)) {
    // Number of copies may be stored in a custom property; default to 2 if missing.
    const copies = (unified.metadata as any)?.replicateCopies || 2;
    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: {
        ...baseMetadata,
        replicateConfig: {
          numberOfCopies: copies,
        },
      },
    };
  }

  // tUnite
  if (isUniteConfig(config)) {
    const uniteConf = config.config;
    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: {
        ...baseMetadata,
        uniteConfig: {
          unionAll: uniteConf.unionMode === 'ALL',
          setOperation: uniteConf.unionMode === 'ALL' ? 'UNION' : 'UNION',
        },
      },
    };
  }

  // tUniqRow
  if (isUniqRowConfig(config)) {
    const uniqConf = config.config;
    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: {
        ...baseMetadata,
        uniqRowConfig: {
          keyColumns: uniqConf.keyFields,
        },
      },
    };
  }

  // tSplitRow (same as Normalize in tests)
// tSplitRow (same as Normalize in tests)
if (nodeType === NodeType.SPLIT_ROW && isNormalizeConfig(config)) {
  const normConf = (config as { type: 'NORMALIZE'; config: NormalizeComponentConfiguration }).config;
  return {
    id: unified.id,
    name: unified.name,
    type: nodeType,
    metadata: {
      ...baseMetadata,
      splitRowConfig: {
        splitColumn: normConf.sourceColumn,
        delimiter: normConf.delimiter,
        outputColumns: [normConf.outputColumnName],
      },
    },
  };
}

  // tPivotToColumnsDelimited
  if (config.type === 'PIVOT_TO_COLUMNS_DELIMITED') {
    const pivotConf = config.config as PivotToColumnsDelimitedConfiguration;
    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: {
        ...baseMetadata,
        pivotToColumnsDelimitedConfig: {
          pivotColumn: pivotConf.sourceColumn,
          valueColumn: 'value', // may need to be stored elsewhere
          delimiter: pivotConf.delimiter,
          pivotValues: pivotConf.pivotValues || [],
        },
      },
    };
  }

  // tDenormalize
  if (config.type === 'DENORMALIZE') {
    const denormConf = config.config as DenormalizeComponentConfiguration;
    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: {
        ...baseMetadata,
        denormalizeConfig: {
          keyColumns: denormConf.keepColumns,
          denormalizeColumn: denormConf.sourceColumn,
          delimiter: denormConf.delimiter,
        },
      },
    };
  }

  // tDenormalizeSortedRow
  if (isDenormalizeSortedRowConfig(config)) {
    const denormConf = config.config;
    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: {
        ...baseMetadata,
        denormalizeSortedRowConfig: {
          groupByFields: denormConf.groupByFields,
          sortKeys: denormConf.sortKeys.map((sk) => ({
            field: sk.field,
            direction: sk.direction,
            nullsFirst: sk.nullsFirst,
          })),
          aggregations: denormConf.denormalizedColumns.map((dc) => ({
            sourceField: dc.sourceField,
            outputField: dc.outputField,
            aggregation: dc.aggregation,
            separator: dc.separator,
          })),
        },
      },
    };
  }

  // tExtractRegexFields
  if (config.type === 'EXTRACT_REGEX_FIELDS') {
    const regexConf = config.config as ExtractRegexFieldsConfiguration;
    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: {
        ...baseMetadata,
        extractRegexConfig: {
          sourceColumn: regexConf.sourceColumn,
          regexPattern: regexConf.regexPattern,
          outputColumns: regexConf.rules.map((r) => ({
            name: r.columnName,
            position: r.position,
          })),
        },
      },
    };
  }

  // tParseRecordSet
  if (config.type === 'PARSE_RECORD_SET') {
    const parseConf = config.config as ParseRecordSetComponentConfiguration;
    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: {
        ...baseMetadata,
        parseRecordSetConfig: {
          sourceColumn: parseConf.sourceColumn,
          recordType: 'delimited',
          delimiter: parseConf.recordDelimiter,
          targetColumns: parseConf.columns.map((c) => ({
            name: c.name,
            path: `$.${c.name}`,
            type: c.type,
          })),
        },
      },
    };
  }

  // tSampleRow
  if (isSampleRowConfig(config)) {
    const sampleConf = config.config;
    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: {
        ...baseMetadata,
        sampleRowConfig: {
          sampleSize: sampleConf.sampleValue,
          isAbsolute: sampleConf.samplingMethod === 'firstRows',
        },
      },
    };
  }

  // tDataMasking
  if (isDataMaskingConfig(config)) {
    const maskConf = config.config;
    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: {
        ...baseMetadata,
        dataMaskingConfig: {
          rules: maskConf.rules.map((r) => ({
            field: r.column,
            maskType: r.maskingType,
            parameters: r.parameters,
          })),
        },
      },
    };
  }

  // tSchemaComplianceCheck
  if (isSchemaComplianceCheckConfig(config)) {
    const schemaConf = config.config;
    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: {
        ...baseMetadata,
        schemaComplianceConfig: {
          expectedSchema: schemaConf.expectedSchema.map((ec) => ({
            name: ec.name,
            type: ec.dataType,
            nullable: ec.nullable,
          })),
        },
      },
    };
  }

  // tAddCRCRow
  if (isAddCRCRowConfig(config)) {
    const crcConf = config.config;
    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: {
        ...baseMetadata,
        addCRCRowConfig: {
          algorithm: crcConf.algorithm,
          outputColumnName: crcConf.outputColumnName,
          includedColumns: crcConf.includedColumns,
        },
      },
    };
  }

  // tFilterColumns
  if (config.type === 'FILTER_COLUMNS') {
    const fcConf = config.config as FilterColumnsComponentConfiguration;
    const includedColumns = fcConf.columns.filter((c) => c.selected).map((c) => c.originalName);
    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: {
        ...baseMetadata,
        filterColumnsConfig: {
          includedColumns,
        },
      },
    };
  }

  // tLookup
  if (isLookupConfig(config)) {
    const lookupConf = config.config;
    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: {
        ...baseMetadata,
        lookupConfig: {
          lookupTable: lookupConf.lookupTable,
          keyMapping: lookupConf.lookupKeyFields.map((kf, idx) => ({
            sourceColumn: kf,
            targetColumn: lookupConf.lookupReturnFields[idx] || kf,
          })),
          outputColumns: lookupConf.lookupReturnFields,
        },
      },
    };
  }

  // tCacheIn / tCacheOut
  if (nodeType === NodeType.CACHE_IN || nodeType === NodeType.CACHE_OUT) {
    const cacheConf = (unified.metadata as any)?.cacheConfig || { cacheName: 'default_cache' };
    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: {
        ...baseMetadata,
        cacheConfig: cacheConf,
      },
    };
  }

  // tRowGenerator – not yet in unified types
  if (nodeType === NodeType.ROW_GENERATOR) {
    const genConf = (unified.metadata as any)?.rowGeneratorConfig || {
      rowCount: 100,
      columns: [],
    };
    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: {
        ...baseMetadata,
        rowGeneratorConfig: genConf,
      },
    };
  }

  // tConditionalSplit
  if (nodeType === NodeType.CONDITIONAL_SPLIT) {
    const splitConf = (unified.metadata as any)?.conditionalSplitConfig || { conditions: [] };
    return {
      id: unified.id,
      name: unified.name,
      type: nodeType,
      metadata: {
        ...baseMetadata,
        conditionalSplitConfig: splitConf,
      },
    };
  }

  // Fallback for any other node types with config
  console.warn(`[Canvas] No specific mapping for node type ${nodeType}. Using fallback.`);
  return {
    id: unified.id,
    name: unified.name,
    type: nodeType,
    metadata: baseMetadata,
  };
});
        const canvasConnections: PipelineCanvasConnection[] = runEdges.map((edge: Edge) => ({
          id: edge.id,
          sourceNodeId: edge.source,
          targetNodeId: edge.target,
          status: ConnectionStatus.VALID,
          dataFlow: { schemaMappings: edge.data?.configuration?.fieldMappings || [], relationType: edge.data?.relationType },
          metadata: edge.data?.metadata || {},
        }));

        const generationResult: PipelineGenerationResult = await generatePipelineSQL(canvasNodes, canvasConnections, { includeComments: true, formatSQL: true, useCTEs: true, logLevel: 'info', progressCallback: (progress) => dispatch(addLog({ level: 'INFO', message: `[${progress.stage}] ${progress.progress}% - ${progress.message}`, source: 'SQL Generation' })) });
        if (generationResult.errors.length) {
          generationResult.errors.forEach(error => dispatch(addLog({ level: 'ERROR', message: `❌ [${error.code}] ${error.message}`, source: 'SQL Generation' })));
          window.dispatchEvent(new CustomEvent('run-complete', { detail: { success: false, errors: generationResult.errors, sql: generationResult.sql } }));
          return;
        }

        const connectionId = await getActivePostgresConnectionId();
        if (!connectionId) {
          dispatch(addLog({ level: 'ERROR', message: 'No active PostgreSQL connection. Please connect to a database first.', source: 'Run' }));
          window.dispatchEvent(new CustomEvent('run-complete', { detail: { success: false, error: 'No active PostgreSQL connection' } }));
          return;
        }

        dispatch(addLog({ level: 'INFO', message: 'Executing generated SQL...', source: 'Run' }));
        const executionResult = await databaseApi.executeQuery(connectionId, generationResult.sql, { maxRows: 1000 });
        if (executionResult.success) {
          dispatch(addLog({ level: 'SUCCESS', message: `✅ SQL executed successfully. Rows affected: ${executionResult.result?.rowCount ?? 0}, Fields: ${executionResult.result?.fields?.length ?? 0}`, source: 'Run' }));
        } else {
          dispatch(addLog({ level: 'ERROR', message: `❌ Execution failed: ${executionResult.error}`, source: 'Run' }));
        }
        window.dispatchEvent(new CustomEvent('run-complete', { detail: { success: executionResult.success, sql: generationResult.sql, executionResult, errors: generationResult.errors } }));
      } catch (error: any) {
        console.error('Run error:', error);
        dispatch(addLog({ level: 'ERROR', message: `❌ Run failed: ${error.message || 'Unknown error'}`, source: 'Run' }));
        window.dispatchEvent(new CustomEvent('run-complete', { detail: { success: false, error: error.message } }));
      }
    };
    window.addEventListener('toolbar-run', handleToolbarRun);
    return () => window.removeEventListener('toolbar-run', handleToolbarRun);
  }, [dispatch]);

  // ==================== CENTER NODES ====================
  const centerNodes = useCallback(() => {
    if (nodes.length && reactFlowInstance && reactFlowWrapper.current) {
      const container = reactFlowWrapper.current;
      const containerWidth = container.clientWidth;
      const containerHeight = container.clientHeight;
      const nodePositions = nodes.map(node => node.position);
      const nodeWidths = nodes.map(node => (node.style?.width ? parseFloat(node.style.width as string) : 100));
      const nodeHeights = nodes.map(node => (node.style?.height ? parseFloat(node.style.height as string) : 100));
      const minX = Math.min(...nodePositions.map((p, _i) => p.x));
      const maxX = Math.max(...nodePositions.map((p, i) => p.x + nodeWidths[i]));
      const minY = Math.min(...nodePositions.map((p, _i) => p.y));
      const maxY = Math.max(...nodePositions.map((p, i) => p.y + nodeHeights[i]));
      const nodesWidth = maxX - minX;
      const nodesHeight = maxY - minY;
      const centerX = minX + nodesWidth / 2;
      const centerY = minY + nodesHeight / 2;
      const viewportX = containerWidth / 2 - centerX;
      const viewportY = containerHeight / 2 - centerY;
      const newViewport = { x: viewportX, y: viewportY, zoom: 1 };
      reactFlowInstance.setViewport(newViewport);
      setState(prev => ({ ...prev, viewport: newViewport }));
      if (job || canvasId) debouncedAutoSave();
    }
  }, [nodes, reactFlowInstance, reactFlowWrapper, job, canvasId, debouncedAutoSave]);

  useEffect(() => {
    if (nodes.length && reactFlowInstance && (!state.viewport || state.viewport.zoom !== 1)) {
      const timer = setTimeout(() => centerNodes(), 100);
      return () => clearTimeout(timer);
    }
  }, [nodes, reactFlowInstance, centerNodes, state.viewport]);

  // ==================== RENDER ====================
  return (
    <>
      <RoleSelectionPopup pending={pendingRoleSelection} onSelect={handleRoleSelect} onCancel={handleRoleCancel} />
      <ConnectionFeedback feedback={state.connectionFeedback} />
      <AutoSaveStatus status={state.autoSaveStatus} lastSavedAt={state.lastSavedAt} enabled={!!(job || canvasId)} />
      <TMapEditorModal
        isOpen={state.mapEditorState.isOpen}
        data={state.mapEditorState.data}
        nodeMetadata={state.mapEditorState.nodeMetadata}
        onClose={closeTMapEditor}
        onSave={handleMapEditorSave}
      />
      <MapEditorModal
        isOpen={state.showMapEditor}
        selectedNode={state.selectedNodeForMapEditor}
        onClose={() => setState(prev => ({ ...prev, showMapEditor: false, selectedNodeForMapEditor: null }))}
        onNodeUpdate={handleNodeUpdate}
      />
      <MatchGroupWizard
        isOpen={state.showMatchGroupWizard}
        selectedNode={state.selectedNodeForMatchGroupWizard}
        onClose={() => setState(prev => ({ ...prev, showMatchGroupWizard: false, selectedNodeForMatchGroupWizard: null }))}
        onNodeUpdate={handleNodeUpdate}
      />
      <ActiveEditorRenderer
        editor={activeEditor}
        onClose={() => setActiveEditor(null)}
        onSaveSort={(config) => { updateNodeConfiguration(activeEditor!.nodeId, { type: 'SORT', config }); setActiveEditor(null); }}
        onSaveReplace={(config) => { updateNodeConfiguration(activeEditor!.nodeId, { type: 'REPLACE', config }); setActiveEditor(null); }}
        onSaveJoin={(config) => { updateNodeConfiguration(activeEditor!.nodeId, { type: 'JOIN', config }); setActiveEditor(null); }}
        onSaveFilter={(config) => { updateNodeConfiguration(activeEditor!.nodeId, { type: 'FILTER', config }); setActiveEditor(null); }}
        onSaveExtractXML={(config) => { updateNodeConfiguration(activeEditor!.nodeId, { type: 'EXTRACT_XML_FIELD', config }); setActiveEditor(null); }}
        onSaveExtractJSON={(config) => { updateNodeConfiguration(activeEditor!.nodeId, { type: 'EXTRACT_JSON_FIELDS', config }); setActiveEditor(null); }}
        onSaveExtractDelimited={(config) => { updateNodeConfiguration(activeEditor!.nodeId, { type: 'EXTRACT_DELIMITED', config }); setActiveEditor(null); }}
        onSaveConvert={(config) => { updateNodeConfiguration(activeEditor!.nodeId, { type: 'CONVERT', config }); setActiveEditor(null); }}
        onSaveAggregate={(config) => { updateNodeConfiguration(activeEditor!.nodeId, { type: 'AGGREGATE', config }); setActiveEditor(null); }}
        onSaveReplaceList={(config) => { updateNodeConfiguration(activeEditor!.nodeId, { type: 'REPLACE_LIST', config }); setActiveEditor(null); }}
        onSaveNormalizeNumber={(config) => { updateNodeConfiguration(activeEditor!.nodeId, { type: 'NORMALIZE_NUMBER', config }); setActiveEditor(null); }}
        onSaveNormalize={(config) => { updateNodeConfiguration(activeEditor!.nodeId, { type: 'NORMALIZE', config }); setActiveEditor(null); }}
        onSaveReplicate={(config) => { updateNodeConfiguration(activeEditor!.nodeId, { type: 'REPLICATE', config }); setActiveEditor(null); }}
        onSaveRecordMatching={(config) => { updateNodeConfiguration(activeEditor!.nodeId, { type: 'MATCH_GROUP', config }); setActiveEditor(null); }}
        onSaveMatchGroup={(config) => { updateNodeConfiguration(activeEditor!.nodeId, { type: 'MATCH_GROUP', config }); setActiveEditor(null); }}
        onSaveFilterColumns={(config) => { updateNodeConfiguration(activeEditor!.nodeId, { type: 'FILTER_COLUMNS', config }); setActiveEditor(null); }}
        onSaveFileLookup={(config) => { updateNodeConfiguration(activeEditor!.nodeId, { type: 'FILE_LOOKUP', config }); setActiveEditor(null); }}
        onSaveUnpivotRow={(config) => { updateNodeConfiguration(activeEditor!.nodeId, { type: 'UNPIVOT_ROW', config }); setActiveEditor(null); }}
        onSaveUnite={(config) => { updateNodeConfiguration(activeEditor!.nodeId, { type: 'UNITE', config }); setActiveEditor(null); }}
        onSaveUniqRow={(config) => { updateNodeConfiguration(activeEditor!.nodeId, { type: 'UNIQ_ROW', config }); setActiveEditor(null); }}
        onSaveSplitRow={(config) => { updateNodeConfiguration(activeEditor!.nodeId, { type: 'NORMALIZE', config }); setActiveEditor(null); }}
        onSavePivotToColumnsDelimited={(config) => { updateNodeConfiguration(activeEditor!.nodeId, { type: 'PIVOT_TO_COLUMNS_DELIMITED', config }); setActiveEditor(null); }}
        onSaveDenormalizeSortedRow={(config) => { updateNodeConfiguration(activeEditor!.nodeId, { type: 'DENORMALIZE_SORTED_ROW', config }); setActiveEditor(null); }}
        onSaveDenormalize={(config) => { updateNodeConfiguration(activeEditor!.nodeId, { type: 'DENORMALIZE', config }); setActiveEditor(null); }}
        // New callbacks for added editors
        onSaveExtractRegex={(config) => { updateNodeConfiguration(activeEditor!.nodeId, { type: 'EXTRACT_REGEX_FIELDS', config }); setActiveEditor(null); }}
        onSaveParseRecordSet={(config) => { updateNodeConfiguration(activeEditor!.nodeId, { type: 'PARSE_RECORD_SET', config }); setActiveEditor(null); }}
        onSaveSampleRow={(config) => { updateNodeConfiguration(activeEditor!.nodeId, { type: 'SAMPLE_ROW', config }); setActiveEditor(null); }}
        onSaveSchemaComplianceCheck={(config) => { updateNodeConfiguration(activeEditor!.nodeId, { type: 'SCHEMA_COMPLIANCE_CHECK', config }); setActiveEditor(null); }}
        onSaveAddCRCRow={(config) => { updateNodeConfiguration(activeEditor!.nodeId, { type: 'ADD_CRC_ROW', config }); setActiveEditor(null); }}
        onSaveDataMasking={(config) => { updateNodeConfiguration(activeEditor!.nodeId, { type: 'DATA_MASKING', config }); setActiveEditor(null); }}
      />

      <div ref={reactFlowWrapper} className="relative w-full h-full canvas-container bg-gray-50">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          onInit={setReactFlowInstance}
          nodeTypes={nodeTypes}
          onSelectionChange={onSelectionChange}
          isValidConnection={isValidConnection}
          onEdgeDoubleClick={(event, edge) => {
            event.stopPropagation();
            window.dispatchEvent(new CustomEvent('canvas-edge-double-click', { detail: { edgeId: edge.id, edgeMetadata: edge.data } }));
          }}
          onDrop={onDrop}
          onDragOver={onDragOver}
          onMove={onMove}
          connectionMode={ConnectionMode.Loose}
          connectionLineType={ConnectionLineType.SmoothStep}
          snapToGrid={true}
          snapGrid={[15, 15]}
          defaultViewport={{ x: state.viewport.x, y: state.viewport.y, zoom: state.viewport.zoom }}
          minZoom={0.1}
          maxZoom={4}
          defaultEdgeOptions={{ animated: false, style: { strokeWidth: 2 }, markerEnd: { type: MarkerType.ArrowClosed, width: 20, height: 20 } }}
          proOptions={{ hideAttribution: true }}
          selectionMode={SelectionMode.Partial}
          deleteKeyCode={['Delete', 'Backspace']}
          multiSelectionKeyCode={['Control', 'Meta']}
          selectionKeyCode={['Shift']}
          nodesDraggable={true}
          nodesConnectable={true}
          elementsSelectable={true}
          selectNodesOnDrag={true}
          panOnDrag={[1, 2]}
          panOnScroll={true}
          zoomOnScroll={true}
          zoomOnDoubleClick={false}
          onlyRenderVisibleElements={false}
        >
          <Background variant={BackgroundVariant.Dots} gap={20} size={1} color="#e5e7eb" />
          <Controls />
        </ReactFlow>
      </div>
    </>
  );
});

export default Canvas;