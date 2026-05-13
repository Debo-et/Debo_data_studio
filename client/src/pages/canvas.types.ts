// src/pages/canvas.types.ts
import { Node, Edge, Connection } from 'reactflow';
import {
  UnifiedCanvasNode,
  UnifiedNodeMetadata,
  MapComponentConfiguration,
  SortComponentConfiguration,
  ReplaceComponentConfiguration,
  JoinComponentConfiguration,
  FilterComponentConfiguration,
  ExtractXMLFieldConfiguration,
  ExtractJSONFieldsConfiguration,
  ExtractDelimitedFieldsConfiguration,
  ConvertComponentConfiguration,
  AggregateComponentConfiguration,
  NormalizeNumberComponentConfiguration,
  NormalizeComponentConfiguration,
  ReplicateComponentConfiguration,
  MatchGroupComponentConfiguration,
  FilterColumnsComponentConfiguration,
  FileLookupComponentConfiguration,
  UnpivotRowComponentConfiguration,
  UniteComponentConfiguration,
  UniqRowComponentConfiguration,
  DenormalizeSortedRowComponentConfiguration,
  DenormalizeComponentConfiguration,
  PivotToColumnsDelimitedConfiguration,
  FieldSchema,
  SchemaDefinition,
  ExtractRegexFieldsConfiguration,
  ParseRecordSetComponentConfiguration,
  SampleRowComponentConfiguration,
  SchemaComplianceCheckConfiguration,
  AddCRCRowComponentConfiguration,
  DataMaskingComponentConfiguration,
} from '../types/unified-pipeline.types';
import { ComponentDefinition } from './ComponentRegistry';

// Drag data from sidebar
export interface ReactFlowDragData {
  type: 'reactflow-component';
  componentId: string;
  source: 'sidebar' | 'rightPanel';
  metadata?: Record<string, any>;
}

// Simple column representation for editors
export interface SimpleColumn {
  name: string;
  type?: string;
  id?: string;
}

// Input schema for Unite editor
export interface InputSchema {
  id: string;
  name: string;
  fields: Array<{ name: string; type: string; nullable: boolean }>;
}

// Active editor union type
export type ActiveEditor =
  | null
  | { type: 'map'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; outputColumns: SimpleColumn[]; initialConfig?: MapComponentConfiguration }
  | { type: 'sort'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: SortComponentConfiguration }
  | { type: 'replace'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; outputColumns: SimpleColumn[]; initialConfig?: ReplaceComponentConfiguration }
  | { type: 'join'; nodeId: string; nodeMetadata: CanvasNodeData; leftSchema: { id: string; name: string; fields: FieldSchema[] }; rightSchema: { id: string; name: string; fields: FieldSchema[] }; initialConfig?: JoinComponentConfiguration }
  | { type: 'filter'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: FilterComponentConfiguration }
  | { type: 'extractXML'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: ExtractXMLFieldConfiguration }
  | { type: 'extractJSON'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: ExtractJSONFieldsConfiguration }
  | { type: 'extractDelimited'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: ExtractDelimitedFieldsConfiguration }
  | { type: 'convert'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; outputColumns: SimpleColumn[]; initialConfig?: ConvertComponentConfiguration }
  | { type: 'aggregate'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: AggregateComponentConfiguration }
  | { type: 'replaceList'; nodeId: string; nodeMetadata: CanvasNodeData; inputSchema: SchemaDefinition; initialConfig?: ReplaceComponentConfiguration }
  | { type: 'normalizeNumber'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: NormalizeNumberComponentConfiguration }
  | { type: 'normalize'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: NormalizeComponentConfiguration }
  | { type: 'replicate'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: ReplicateComponentConfiguration }
  | { type: 'recordMatching'; nodeId: string; nodeMetadata: CanvasNodeData; inputFields: FieldSchema[]; initialConfig?: MatchGroupComponentConfiguration }
  | { type: 'matchGroup'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: MatchGroupComponentConfiguration }
  | { type: 'filterColumns'; nodeId: string; nodeMetadata: CanvasNodeData; inputSchema?: SchemaDefinition; initialConfig?: FilterColumnsComponentConfiguration }
  | { type: 'fileLookup'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: FileLookupComponentConfiguration }
  | { type: 'unpivotRow'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: UnpivotRowComponentConfiguration }
  | { type: 'unite'; nodeId: string; nodeMetadata: CanvasNodeData; inputSchemas: InputSchema[]; initialConfig?: UniteComponentConfiguration }
  | { type: 'uniqRow'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: UniqRowComponentConfiguration }
  | { type: 'splitRow'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: NormalizeComponentConfiguration }
  | { type: 'pivotToColumnsDelimited'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: PivotToColumnsDelimitedConfiguration }
  | { type: 'denormalizeSortedRow'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: DenormalizeSortedRowComponentConfiguration }
  | { type: 'denormalize'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: DenormalizeComponentConfiguration }
  // New editor types for missing node types
  | { type: 'extractRegex'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: ExtractRegexFieldsConfiguration }
  | { type: 'parseRecordSet'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: ParseRecordSetComponentConfiguration }
  | { type: 'sampleRow'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: SampleRowComponentConfiguration }
  | { type: 'schemaComplianceCheck'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: SchemaComplianceCheckConfiguration }
  | { type: 'addCRCRow'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: AddCRCRowComponentConfiguration }
  | { type: 'dataMasking'; nodeId: string; nodeMetadata: CanvasNodeData; inputColumns: SimpleColumn[]; initialConfig?: DataMaskingComponentConfiguration };

// Wizard configuration (example)
export interface WizardConfig {
  currentStep: number;
  inputFlow: string;
  schemaColumns: any[];
  groupingKeys: any[];
  survivorshipRules: any[];
  outputMapping: Record<string, any>;
  outputTableName: string;
}

// SQL preview state
export interface SQLPreviewState {
  isVisible: boolean;
  sql: string;
  title: string;
  nodeId?: string;
  nodeName?: string;
}

// Validation summary
export interface ValidationSummary {
  isValid: boolean;
  results: any[];
  counts: {
    errors: number;
    warnings: number;
    info: number;
  };
}

// Connection feedback popup
export interface ConnectionFeedback {
  isVisible: boolean;
  message: string;
  type: 'success' | 'error' | 'info' | 'warning';
  position: { x: number; y: number };
}

// Pending role selection after drop
export interface PendingRoleSelection {
  nodeId: string;
  componentId: string;
  displayName: string;
  position: { x: number; y: number };
  dropPosition: { x: number; y: number };
  componentDef: ComponentDefinition;
  nodeData: CanvasNodeData;
}

// Map editor internal state
export interface MapEditorState {
  isOpen: boolean;
  data: MapEditorPayload | null;
  nodeMetadata?: CanvasNodeData;
}

// Payload for map editor (from columnExtraction)
export interface MapEditorPayload {
  nodeId: string;
  inputColumns: SimpleColumn[];
  outputColumns: SimpleColumn[];
  // ... other fields
}

// Main canvas state
export interface CanvasState {
  nodes: Node[];
  edges: Edge[];
  selectedNodeId: string | null;
  showMapEditor: boolean;
  selectedNodeForMapEditor: any;
  sqlPreview: SQLPreviewState;
  validationSummary: ValidationSummary | null;
  validationMode: 'strict' | 'lenient' | 'warn-only';
  isValidating: boolean;
  connectionFeedback: ConnectionFeedback;
  pendingDrop: any | null;
  showMatchGroupWizard: boolean;
  selectedNodeForMatchGroupWizard: any;
  viewport: { x: number; y: number; zoom: number };
  mapEditorState: MapEditorState;
  autoSaveStatus: 'idle' | 'saving' | 'saved' | 'error';
  lastSavedAt?: string;
}

// Props for the Canvas component
export interface ExtendedCanvasProps {
  job?: any;
  onJobUpdate?: (updates: any) => void;
  jobDesign?: any;
  onJobDesignUpdate?: (design: any) => void;
  validateConnection?: (connection: Connection, nodes: Node[], edges: Edge[]) => {
    isValid: boolean;
    errors: string[];
    warnings: string[];
  };
  canvasId?: string;
  onNodeMetadataUpdate?: (nodeId: string, metadata: UnifiedNodeMetadata) => void;
  onEdgeMetadataUpdate?: (edgeId: string, metadata: any) => void;
}

// Helper type to avoid repeating CanvasNodeData
export type CanvasNodeData = UnifiedCanvasNode;