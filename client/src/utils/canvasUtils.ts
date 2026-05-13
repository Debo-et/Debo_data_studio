// src/utils/canvasUtils.ts
import { NodeType, PortType, PortSide, NodeStatus } from '../types/pipeline-types';

// ==================== TYPES & INTERFACES ====================

export interface ComponentPort {
  id: string;
  type: PortType;
  side: PortSide;
  position: number;
  label?: string;
  dataType?: string;
  maxConnections?: number;
  isConnected?: boolean;
}

export interface CanvasNode {
  id: string;
  name: string;
  type: NodeType | string;
  nodeType?: string;
  componentType?: string;
  componentCategory?: 'input' | 'output' | 'process';
  technology?: string;
  connectionPorts?: ComponentPort[];
  schemaName?: string;
  tableName?: string;
  fileName?: string;
  sheetName?: string;
  position: { x: number; y: number };
  size: { width: number; height: number };
  metadata?: any;
  status?: string;
  draggable?: boolean;
  droppable?: boolean;
  dragType?: string;
  visualProperties?: {
    color?: string;
    icon?: string;
    borderColor?: string;
    backgroundColor?: string;
  };
}

export interface PendingDrop {
  data: any;
  position: { x: number; y: number };
  metadata: any;
  technology: string;
  defaultName: string;
  componentType?: 'processing' | 'standardized';
  category?: 'source' | 'process' | 'destination';
}

export interface ConnectionInteraction {
  contextMenu: {
    connection: any | null;
    position: { x: number; y: number };
    isVisible: boolean;
  };
  validationOverlay: {
    connection: any | null;
    validationResult: any | null;
    position: { x: number; y: number };
    isVisible: boolean;
  };
  selectedConnections: Set<string>;
  hoveredConnection: string | null;
  activeConnection: string | null;
}

export interface WizardConfig {
  currentStep: number;
  inputFlow: string;
  inputFlowId?: string;
  schemaColumns: any[];
  groupingKeys: any[];
  survivorshipRules: any[];
  outputMapping: Record<string, string>;
  outputTableName: string;
  previewData?: any[];
}

export interface SQLGenerationState {
  sql: string;
  isValid: boolean;
  errors: string[];
  warnings: string[];
  lastGenerated: string | null;
  isGenerating: boolean;
  hasDefaultMappings: boolean;
  mappingStats?: {
    totalSourceColumns: number;
    totalTargetColumns: number;
    mappedColumns: number;
    unmappedColumns: number;
    defaultMappings: number;
    mappingPercentage: number;
  };
}

export interface SQLPreviewState {
  isVisible: boolean;
  sql: string;
  nodeName: string;
  nodeId: string;
  errors: string[];
  warnings: string[];
  hasDefaultMappings: boolean;
}

export interface PendingConnection {
  sourceNodeId: string;
  sourcePortId: string;
  startPosition: { x: number; y: number };
  currentPosition: { x: number; y: number };
  targetNodeId?: string;     // Add this
  targetPortId?: string;     // Add this
}

export interface ConnectionSnapState {
  isSnapping: boolean;
  candidate: {
    nodeId: string;
    portId: string;
    position: { x: number; y: number };
    distance?: number;
    portType?: PortType;
  } | null;
  snapRadius: number;
  visualFeedback: boolean;
}

export interface PortConnectionCandidate {
  nodeId: string;
  portId: string;
  portType: PortType;
  position: { x: number; y: number };
  distance: number;
}

export interface CanvasProps {
  job: {
    id: string;
    name: string;
    nodes: any[];
    connections: any[];
    variables: any[];
  };
  onJobUpdate: (updates: any) => void;
}

// ==================== COMPONENT REGISTRY ====================

const COMPONENT_REGISTRY: Record<string, {
  canonicalType: string;
  type: string;
  baseName: string;
  category: 'source' | 'process' | 'destination';
  displayName: string;
  isDataSource?: boolean;
  isProcessing?: boolean;
}> = {
  // ───── Data Sources ─────
  'excel': { canonicalType: 'excel', type: 'Excel', baseName: 'excel', category: 'source', displayName: 'Excel', isDataSource: true },
  'delimited': { canonicalType: 'delimited', type: 'CSV', baseName: 'delimited', category: 'source', displayName: 'CSV', isDataSource: true },
  'csv': { canonicalType: 'csv', type: 'CSV', baseName: 'csv', category: 'source', displayName: 'CSV', isDataSource: true },
  'database': { canonicalType: 'database', type: 'Database', baseName: 'database', category: 'source', displayName: 'Database', isDataSource: true },
  'db': { canonicalType: 'database', type: 'Database', baseName: 'database', category: 'source', displayName: 'Database', isDataSource: true },
  'xml': { canonicalType: 'xml', type: 'XML', baseName: 'xml', category: 'source', displayName: 'XML', isDataSource: true },
  'json': { canonicalType: 'json', type: 'JSON', baseName: 'json', category: 'source', displayName: 'JSON', isDataSource: true },
  'avro': { canonicalType: 'avro', type: 'Avro', baseName: 'avro', category: 'source', displayName: 'Avro', isDataSource: true },
  'parquet': { canonicalType: 'parquet', type: 'Parquet', baseName: 'parquet', category: 'source', displayName: 'Parquet', isDataSource: true },
  'webservice': { canonicalType: 'webservice', type: 'WebService', baseName: 'webservice', category: 'source', displayName: 'Web Service', isDataSource: true },
  'web-service': { canonicalType: 'webservice', type: 'WebService', baseName: 'webservice', category: 'source', displayName: 'Web Service', isDataSource: true },
  'api': { canonicalType: 'api', type: 'API', baseName: 'api', category: 'source', displayName: 'API', isDataSource: true },
  'ldif': { canonicalType: 'ldif', type: 'LDIF', baseName: 'ldif', category: 'source', displayName: 'LDIF', isDataSource: true },
  'regex': { canonicalType: 'regex', type: 'Regex', baseName: 'regex', category: 'source', displayName: 'Regex', isDataSource: true },
  'schema': { canonicalType: 'schema', type: 'Schema', baseName: 'schema', category: 'source', displayName: 'Schema', isDataSource: true },
  'file': { canonicalType: 'file', type: 'File', baseName: 'file', category: 'source', displayName: 'File', isDataSource: true },
  'table': { canonicalType: 'table', type: 'Table', baseName: 'table', category: 'source', displayName: 'Table', isDataSource: true },
  'input': { canonicalType: 'input', type: 'Input', baseName: 'input', category: 'source', displayName: 'Input', isDataSource: true },
  'source': { canonicalType: 'source', type: 'Source', baseName: 'source', category: 'source', displayName: 'Source', isDataSource: true },
  
  // ───── Data Processing ─────
  'map': { canonicalType: 'map', type: 'Map', baseName: 'map', category: 'process', displayName: 'Map', isProcessing: true },
  'tmap': { canonicalType: 'map', type: 'Map', baseName: 'map', category: 'process', displayName: 'Map', isProcessing: true },
  'join': { canonicalType: 'join', type: 'Join', baseName: 'join', category: 'process', displayName: 'Join', isProcessing: true },
  'tjoin': { canonicalType: 'join', type: 'Join', baseName: 'join', category: 'process', displayName: 'Join', isProcessing: true },
  'filter': { canonicalType: 'filter', type: 'Filter', baseName: 'filter', category: 'process', displayName: 'Filter', isProcessing: true },
  'tfilter': { canonicalType: 'filter', type: 'Filter', baseName: 'filter', category: 'process', displayName: 'Filter', isProcessing: true },
  'sort': { canonicalType: 'sort', type: 'Sort', baseName: 'sort', category: 'process', displayName: 'Sort', isProcessing: true },
  'tsort': { canonicalType: 'sort', type: 'Sort', baseName: 'sort', category: 'process', displayName: 'Sort', isProcessing: true },
  'aggregate': { canonicalType: 'aggregate', type: 'Aggregate', baseName: 'aggregate', category: 'process', displayName: 'Aggregate', isProcessing: true },
  'taggregate': { canonicalType: 'aggregate', type: 'Aggregate', baseName: 'aggregate', category: 'process', displayName: 'Aggregate', isProcessing: true },
  'matchgroup': { canonicalType: 'matchgroup', type: 'Match Group', baseName: 'matchgroup', category: 'process', displayName: 'Match Group', isProcessing: true },
  'tmatchgroup': { canonicalType: 'matchgroup', type: 'Match Group', baseName: 'matchgroup', category: 'process', displayName: 'Match Group', isProcessing: true },
  'match-group': { canonicalType: 'matchgroup', type: 'Match Group', baseName: 'matchgroup', category: 'process', displayName: 'Match Group', isProcessing: true },
  'normalize': { canonicalType: 'normalize', type: 'Normalize', baseName: 'normalize', category: 'process', displayName: 'Normalize', isProcessing: true },
  'denormalize': { canonicalType: 'denormalize', type: 'Denormalize', baseName: 'denormalize', category: 'process', displayName: 'Denormalize', isProcessing: true },
  'replace': { canonicalType: 'replace', type: 'Replace', baseName: 'replace', category: 'process', displayName: 'Replace', isProcessing: true },
  'convert': { canonicalType: 'convert', type: 'Convert', baseName: 'convert', category: 'process', displayName: 'Convert', isProcessing: true },
  'extract': { canonicalType: 'extract', type: 'Extract', baseName: 'extract', category: 'process', displayName: 'Extract', isProcessing: true },
  'parse': { canonicalType: 'parse', type: 'Parse', baseName: 'parse', category: 'process', displayName: 'Parse', isProcessing: true },
  'split': { canonicalType: 'split', type: 'Split', baseName: 'split', category: 'process', displayName: 'Split', isProcessing: true },
  'pivot': { canonicalType: 'pivot', type: 'Pivot', baseName: 'pivot', category: 'process', displayName: 'Pivot', isProcessing: true },
  'sample': { canonicalType: 'sample', type: 'Sample', baseName: 'sample', category: 'process', displayName: 'Sample', isProcessing: true },
  'uniq': { canonicalType: 'unique', type: 'Unique', baseName: 'uniq', category: 'process', displayName: 'Unique', isProcessing: true },
  'unique': { canonicalType: 'unique', type: 'Unique', baseName: 'unique', category: 'process', displayName: 'Unique', isProcessing: true },
  'match': { canonicalType: 'match', type: 'Match', baseName: 'match', category: 'process', displayName: 'Match', isProcessing: true },
  'generator': { canonicalType: 'generator', type: 'Generator', baseName: 'generator', category: 'process', displayName: 'Generator', isProcessing: true },
  'lookup': { canonicalType: 'lookup', type: 'Lookup', baseName: 'lookup', category: 'process', displayName: 'Lookup', isProcessing: true },
  'cache': { canonicalType: 'cache', type: 'Cache', baseName: 'cache', category: 'process', displayName: 'Cache', isProcessing: true },
  'matching': { canonicalType: 'matching', type: 'Matching', baseName: 'matching', category: 'process', displayName: 'Matching', isProcessing: true },
  'transform': { canonicalType: 'transform', type: 'Transform', baseName: 'transform', category: 'process', displayName: 'Transform', isProcessing: true },
  'processing': { canonicalType: 'processing', type: 'Processing', baseName: 'processing', category: 'process', displayName: 'Processing', isProcessing: true },
  'palette-component': { canonicalType: 'processing', type: 'Processing', baseName: 'processing', category: 'process', displayName: 'Processing', isProcessing: true },
  'process': { canonicalType: 'process', type: 'Process', baseName: 'process', category: 'process', displayName: 'Process', isProcessing: true },
  
  // ───── Data Destinations ─────
  'output': { canonicalType: 'output', type: 'Output', baseName: 'output', category: 'destination', displayName: 'Output' },
  'sink': { canonicalType: 'sink', type: 'Sink', baseName: 'sink', category: 'destination', displayName: 'Sink' },
  'target': { canonicalType: 'target', type: 'Target', baseName: 'target', category: 'destination', displayName: 'Target' },
  'writer': { canonicalType: 'writer', type: 'Writer', baseName: 'writer', category: 'destination', displayName: 'Writer' },
  'destination': { canonicalType: 'destination', type: 'Destination', baseName: 'destination', category: 'destination', displayName: 'Destination' },
  
  // ───── Default ─────
  'default': { canonicalType: 'component', type: 'Component', baseName: 'component', category: 'process', displayName: 'Component' },
  'component': { canonicalType: 'component', type: 'Component', baseName: 'component', category: 'process', displayName: 'Component' }
};

// ==================== HELPER FUNCTIONS ====================

export const getTechnologyDisplayName = (tech: string): string => {
  const displayNames: { [key: string]: string } = {
    'excel': 'Excel File',
    'delimited': 'Delimited File',
    'database': 'Database Table',
    'xml': 'XML File',
    'json': 'JSON File',
    'avro': 'Avro File',
    'parquet': 'Parquet File',
    'webservice': 'Web Service',
    'ldif': 'LDIF File',
    'regex': 'Regex File',
    'schema': 'Schema File',
    'matchgroup': 'Match Group',
    'unknown': 'Unknown'
  };
  return displayNames[tech] || tech;
};

export const isProcessingComponentNode = (node: CanvasNode): boolean => {
  const nodeTypeStr = typeof node.type === 'string' ? node.type : NodeType[node.type];
  const processingTypes = [
    'tJoin', 'Join', 'tDenormalize', 'Denormalize', 'tNormalize', 'Normalize',
    'tAggregateRow', 'AggregateRow', 'tSortRow', 'SortRow', 'tFilterRow', 'FilterRow',
    'tFilterColumns', 'FilterColumns',
    'tReplace', 'Replace', 'tReplaceList', 'ReplaceList', 'tConvertType', 'ConvertType',
    'tExtractDelimitedFields', 'ExtractDelimitedFields', 'tExtractRegexFields', 'ExtractRegexFields',
    'tExtractJSONFields', 'ExtractJSONFields', 'tExtractXMLField', 'ExtractXMLField',
    'tParseRecordSet', 'ParseRecordSet', 'tSplitRow', 'SplitRow', 
    'tPivotToColumnsDelimited', 'PivotToColumnsDelimited', 'tUnpivotRow', 'UnpivotRow',
    'tDenormalizeSortedRow', 'DenormalizeSortedRow', 'tUniqRow', 'UniqRow',
    'tSampleRow', 'SampleRow',
    'tSchemaComplianceCheck', 'SchemaComplianceCheck', 'tAddCRCRow', 'AddCRCRow',
    'tAddCRC', 'AddCRC', 'tStandardizeRow', 'StandardizeRow', 'tDataMasking', 'DataMasking',
    'tAssert', 'Assert',
    'tFlowToIterate', 'FlowToIterate', 'tIterateToFlow', 'IterateToFlow',
    'tReplicate', 'Replicate', 'tUnite', 'Unite', 'tFlowMerge', 'FlowMerge',
    'tFlowMeter', 'FlowMeter', 'tFlowMeterCatcher', 'FlowMeterCatcher',
    'tMatchGroup', 'MatchGroup',
    'tRowGenerator', 'RowGenerator', 'tNormalizeNumber', 'NormalizeNumber',
    'tFileLookup', 'FileLookup', 'tCacheIn', 'CacheIn', 'tCacheOut', 'CacheOut',
    'tRecordMatching', 'RecordMatching',
    'Map', 'tMap'
  ];
  
  return processingTypes.some(type => 
    nodeTypeStr.includes(type) || 
    node.name.includes(type) ||
    (node.metadata?.componentData?.name && node.metadata.componentData.name.includes(type))
  );
};

export const isMatchGroupComponent = (node: CanvasNode): boolean => {
  const nodeTypeStr = typeof node.type === 'string' ? node.type : NodeType[node.type];
  const matchGroupTypes = [
    'tMatchGroup',
    'MatchGroup',
    'tRecordMatching',
    'matchgroup',
    'match-group',
    'Match Group'
  ];
  
  return matchGroupTypes.some(type => 
    nodeTypeStr.includes(type) || 
    node.name.includes(type) ||
    (node.metadata?.componentData?.name && 
     node.metadata.componentData.name.toLowerCase().includes('matchgroup')) ||
    (node.metadata?.originalId && 
     node.metadata.originalId.toLowerCase().includes('matchgroup'))
  );
};

export const isMapComponent = (node: CanvasNode): boolean => {
  const nodeTypeStr = typeof node.type === 'string' ? node.type : NodeType[node.type];
  const mapTypes = [
    'Map', 'tMap', 'map'
  ];
  
  return mapTypes.some(type => 
    nodeTypeStr.includes(type) || 
    node.name.includes(type) ||
    (node.metadata?.componentData?.name && 
     node.metadata.componentData.name.toLowerCase().includes('map'))
  );
};

export const determineComponentCategory = (componentType: string): string => {
  const componentTypeLower = componentType.toLowerCase();
  
  if (componentTypeLower.includes('join') || 
      componentTypeLower.includes('denormalize') ||
      componentTypeLower.includes('normalize') ||
      componentTypeLower.includes('aggregate') ||
      componentTypeLower.includes('sort') ||
      componentTypeLower.includes('filter')) {
    return 'Data Transformation';
  }
  
  if (componentTypeLower.includes('replace') ||
      componentTypeLower.includes('convert') ||
      componentTypeLower.includes('extract')) {
    return 'Field Manipulation';
  }
  
  if (componentTypeLower.includes('parse') ||
      componentTypeLower.includes('split') ||
      componentTypeLower.includes('pivot') ||
      componentTypeLower.includes('sample') ||
      componentTypeLower.includes('uniq')) {
    return 'Row & Record Processing';
  }
  
  if (componentTypeLower.includes('schema') ||
      componentTypeLower.includes('crc') ||
      componentTypeLower.includes('standardize') ||
      componentTypeLower.includes('assert') ||
      componentTypeLower.includes('masking')) {
    return 'Validation & Quality';
  }
  
  if (componentTypeLower.includes('flow') ||
      componentTypeLower.includes('iterate') ||
      componentTypeLower.includes('replicate') ||
      componentTypeLower.includes('unite') ||
      componentTypeLower.includes('merge') ||
      componentTypeLower.includes('meter') ||
      componentTypeLower.includes('matchgroup')) {
    return 'Flow & Orchestration';
  }
  
  if (componentTypeLower.includes('generator') ||
      componentTypeLower.includes('lookup') ||
      componentTypeLower.includes('cache') ||
      componentTypeLower.includes('matching') ||
      componentTypeLower.includes('normalizenumber')) {
    return 'System & Generation';
  }
  
  return 'Processing Component';
};

export const determineComponentIcon = (componentType: string): string => {
  const iconMap: Record<string, string> = {
    'join': 'GitBranch',
    'denormalize': 'Database',
    'normalize': 'Settings',
    'aggregate': 'Hash',
    'sort': 'Filter',
    'filter': 'Funnel',
    'replace': 'Shuffle',
    'convert': 'Type',
    'extract': 'Zap',
    'schema': 'Table',
    'crc': 'Check',
    'assert': 'AlertCircle',
    'matchgroup': 'GitCompare',
    'flow': 'GitMerge',
    'iterate': 'RefreshCw',
    'generator': 'Zap',
    'lookup': 'Search',
    'cache': 'Database',
    'map': 'Map',
    'default': 'Cpu'
  };
  
  const componentTypeLower = componentType.toLowerCase();
  
  for (const [key, icon] of Object.entries(iconMap)) {
    if (componentTypeLower.includes(key)) {
      return icon;
    }
  }
  
  return 'Cpu';
};

export const getComponentColor = (componentType: string): string => {
  const componentTypeLower = componentType.toLowerCase();
  
  if (componentTypeLower.includes('join')) return '#3b82f6'; // Blue
  if (componentTypeLower.includes('filter')) return '#10b981'; // Green
  if (componentTypeLower.includes('sort')) return '#8b5cf6'; // Purple
  if (componentTypeLower.includes('aggregate')) return '#f59e0b'; // Amber
  if (componentTypeLower.includes('transform')) return '#ef4444'; // Red
  if (componentTypeLower.includes('match')) return '#ec4899'; // Pink
  if (componentTypeLower.includes('map')) return '#059669'; // Emerald
  
  return '#6b7280'; // Gray
};

export const getComponentShape = (componentType: string): 'circle' | 'square' | 'diamond' | 'triangle' => {
  const componentTypeLower = componentType.toLowerCase();
  
  if (componentTypeLower.includes('join')) return 'diamond';
  if (componentTypeLower.includes('filter')) return 'triangle';
  if (componentTypeLower.includes('aggregate')) return 'circle';
  if (componentTypeLower.includes('transform')) return 'square';
  
  return 'circle';
};

// ==================== NEW: SNAKE_CASE NAMING HELPER FUNCTIONS ====================

/**
 * Convert string to snake_case
 */
export const toSnakeCase = (str: string): string => {
  if (!str) return '';
  
  return str
    .toString()
    .toLowerCase()
    .replace(/\s+/g, '_')          // Replace spaces with underscores
    .replace(/[^a-z0-9_]/g, '')   // Remove special characters
    .replace(/_+/g, '_')          // Replace multiple underscores with single
    .replace(/^_+|_+$/g, '');     // Trim underscores from start/end
};

/**
 * Extract base name from metadata for snake_case conversion
 */
export const extractBaseNameFromMetadata = (metadata: any): string => {
  if (!metadata) return 'component';
  
  // Priority: metadata.name > metadata.displayName > metadata.type
  const name = metadata.name || 
               metadata.displayName || 
               metadata.type || 
               'component';
  
  return toSnakeCase(name);
};

/**
 * Get metadata name from drag data with priority
 */
export const getMetadataNameFromDragData = (dragData: any): string => {
  if (!dragData) return 'component';
  
  // Priority 1: Direct metadata.name
  if (dragData.metadata?.name) {
    return dragData.metadata.name;
  }
  
  // Priority 2: Component metadata.name
  if (dragData.component?.metadata?.name) {
    return dragData.component.metadata.name;
  }
  
  // Priority 3: Node metadata.name
  if (dragData.node?.metadata?.name) {
    return dragData.node.metadata.name;
  }
  
  // Priority 4: Name property
  if (dragData.name) {
    return dragData.name;
  }
  
  // Priority 5: Display name
  if (dragData.metadata?.displayName) {
    return dragData.metadata.displayName;
  }
  
  // Priority 6: Type or technology
  if (dragData.type) {
    return dragData.type;
  }
  
  if (dragData.technology) {
    return dragData.technology;
  }
  
  // Fallback
  return 'component';
};

/**
 * Generate snake_case name with auto-incrementing suffix
 */
export const generateSnakeCaseName = (
  baseName: string,
  existingNodes: CanvasNode[],
  metadata: any = {}
): string => {
  if (!baseName || baseName === 'component') {
    baseName = 'component';
  }
  
  // Clean the base name
  const cleanBaseName = toSnakeCase(baseName);
  
  // Find all existing nodes with the same base name pattern
  const matchingNodes = existingNodes.filter(node => {
    const nodeName = node.name || '';
    const nodeBaseName = metadata?.baseName || 
                        node.metadata?.baseName || 
                        extractBaseNameFromMetadata(node.metadata);
    
    // Check if node name starts with the base name
    return nodeName.startsWith(cleanBaseName) || 
           nodeBaseName === cleanBaseName;
  });
  
  // If no matching nodes, return base name without suffix
  if (matchingNodes.length === 0) {
    return cleanBaseName;
  }
  
  // Extract numeric suffixes from existing nodes
  const suffixes = matchingNodes.map(node => {
    const nodeName = node.name || '';
    
    if (nodeName === cleanBaseName) {
      return 0; // Base name without suffix
    }
    
    // Extract numeric suffix from names like "excel_1", "excel_2"
    const match = nodeName.match(new RegExp(`^${cleanBaseName}_(\\d+)$`));
    return match ? parseInt(match[1], 10) : 0;
  });
  
  // Find the highest suffix
  const maxSuffix = Math.max(0, ...suffixes);
  
  // If base name exists without suffix, start from _1
  if (suffixes.includes(0) && maxSuffix === 0) {
    return `${cleanBaseName}_1`;
  }
  
  // Otherwise increment the highest suffix
  return `${cleanBaseName}_${maxSuffix + 1}`;
};

// ==================== COMPONENT CATEGORIZATION & NAMING FUNCTIONS ====================

/**
 * Gets a clean component name for display
 */
export const getCleanComponentName = (componentType: string): string => {
  if (!componentType) return 'Component';
  
  return componentType
    .replace(/^t/, '')  // Remove leading 't'
    .replace(/([A-Z])/g, ' $1')  // Add spaces before capital letters
    .replace(/^./, str => str.toUpperCase())  // Capitalize first letter
    .trim();
};

/**
 * Count existing nodes by canonical type
 */
export const countExistingNodesByCanonicalType = (
  existingNodes: CanvasNode[],
  canonicalType: string
): number => {
  return existingNodes.filter(node => {
    return node.metadata?.canonicalType === canonicalType;
  }).length;
};

/**
 * Generate canonical sequential name
 */
export const generateCanonicalSequentialName = (
  canonicalType: string,
  existingNodes: CanvasNode[]
): string => {
  const count = countExistingNodesByCanonicalType(existingNodes, canonicalType);
  const nextNumber = count + 1;
  
  return `${canonicalType}_${nextNumber}`;
};

/**
 * Categorizes a component type as data source, data processing, or data destination
 */
export const categorizeComponentType = (componentType: string): 'source' | 'process' | 'destination' => {
  const { category } = extractComponentTypeFromDragData({ type: componentType });
  return category;
};

/**
 * Gets a clean base name for component naming
 */
export const getCleanComponentBaseName = (componentType: string): string => {
  if (!componentType) return 'component';
  
  const { baseName } = extractComponentTypeFromDragData({ type: componentType });
  return baseName;
};

/**
 * Gets the display name for a component type
 */
export const getComponentDisplayName = (componentType: string): string => {
  const { displayName } = extractComponentTypeFromDragData({ type: componentType });
  return displayName;
};

/**
 * Checks if a component type is a data source
 */
export const isDataSourceComponent = (componentType: string): boolean => {
  const typeLower = componentType.toLowerCase();
  const entry = Object.values(COMPONENT_REGISTRY).find(entry => 
    entry.canonicalType === typeLower || 
    entry.type.toLowerCase() === typeLower || 
    entry.baseName === typeLower ||
    entry.displayName.toLowerCase() === typeLower
  );
  
  return entry?.isDataSource || false;
};

/**
 * Checks if a component type is a processing component
 */
export const isProcessingComponent = (componentType: string): boolean => {
  const typeLower = componentType.toLowerCase();
  const entry = Object.values(COMPONENT_REGISTRY).find(entry => 
    entry.canonicalType === typeLower || 
    entry.type.toLowerCase() === typeLower || 
    entry.baseName === typeLower ||
    entry.displayName.toLowerCase() === typeLower
  );
  
  return entry?.isProcessing || false;
};

/**
 * Extracts component type from drag data with proper categorization - ENHANCED VERSION
 */
export function extractComponentTypeFromDragData(
  dragData: any
): {
  canonicalType: string;
  type: string;
  baseName: string;
  category: 'source' | 'process' | 'destination';
  displayName: string;
  cleanType: string;
  metadataName: string;
  snakeCaseName: string;
  isDataSource?: boolean;
  isProcessing?: boolean;
} {
  let rawType = '';
  let metadataName = '';
  
  if (typeof dragData === 'string') {
    rawType = dragData;
    metadataName = dragData;
  } else if (dragData?.type && typeof dragData.type === 'string') {
    rawType = dragData.type;
    metadataName = getMetadataNameFromDragData(dragData);
  } else if (dragData?.component?.type) {
    rawType = dragData.component.type;
    metadataName = getMetadataNameFromDragData(dragData);
  } else if (dragData?.metadata?.type) {
    rawType = dragData.metadata.type;
    metadataName = getMetadataNameFromDragData(dragData);
  } else if (dragData?.metadata?.technology) {
    rawType = dragData.metadata.technology;
    metadataName = getMetadataNameFromDragData(dragData);
  } else if (dragData?.technology) {
    rawType = dragData.technology;
    metadataName = getMetadataNameFromDragData(dragData);
  } else if (dragData?.name) {
    rawType = dragData.name;
    metadataName = dragData.name;
  } else if (dragData?.component?.name) {
    rawType = dragData.component.name;
    metadataName = dragData.component.name;
  } else if (dragData?.node?.type) {
    rawType = dragData.node.type;
    metadataName = getMetadataNameFromDragData(dragData);
  } else if (dragData?.node?.metadata?.type) {
    rawType = dragData.node.metadata.type;
    metadataName = getMetadataNameFromDragData(dragData);
  }
  
  // Extract metadata name separately
  if (!metadataName) {
    metadataName = getMetadataNameFromDragData(dragData);
  }
  
  if (rawType.startsWith('t') && rawType.length > 1) {
    rawType = rawType.substring(1);
  }
  
  const normalized = rawType.toLowerCase().trim();
  const snakeCaseName = toSnakeCase(metadataName);
  
  if (COMPONENT_REGISTRY[normalized]) {
    return {
      ...COMPONENT_REGISTRY[normalized],
      cleanType: getCleanComponentName(COMPONENT_REGISTRY[normalized].type),
      metadataName: metadataName,
      snakeCaseName: snakeCaseName
    };
  }
  
  for (const [key, entry] of Object.entries(COMPONENT_REGISTRY)) {
    if (entry.canonicalType === normalized ||
        entry.type.toLowerCase() === normalized ||
        entry.displayName.toLowerCase() === normalized ||
        normalized.includes(key) || 
        key.includes(normalized)) {
      return {
        ...entry,
        cleanType: getCleanComponentName(entry.type),
        metadataName: metadataName,
        snakeCaseName: snakeCaseName
      };
    }
  }
  
  const cleanType = getCleanComponentName(rawType);
  const canonicalType = cleanType.toLowerCase().replace(/\s+/g, '');
  
  let category: 'source' | 'process' | 'destination' = 'process';
  if (normalized.includes('input') || normalized.includes('source') || 
      normalized.includes('excel') || normalized.includes('csv') || 
      normalized.includes('database') || normalized.includes('file')) {
    category = 'source';
  } else if (normalized.includes('output') || normalized.includes('destination') || 
             normalized.includes('sink') || normalized.includes('target')) {
    category = 'destination';
  }
  
  return {
    canonicalType: canonicalType,
    type: cleanType,
    baseName: canonicalType,
    category,
    displayName: cleanType,
    cleanType,
    metadataName: metadataName,
    snakeCaseName: snakeCaseName,
    isDataSource: category === 'source',
    isProcessing: category === 'process'
  };
}

/**
 * Gets base component name from drag data
 */
export const getBaseComponentName = (dragData: any): string => {
  const { baseName } = extractComponentTypeFromDragData(dragData);
  return baseName;
};

export const generateComponentName = (technology: string, _category: string, existingNodes: CanvasNode[]): string => {
  const { canonicalType } = extractComponentTypeFromDragData({ type: technology });
  
  return generateCanonicalSequentialName(canonicalType, existingNodes);
};

export const generateProcessingComponentName = (baseName: string, existingNodes: CanvasNode[]): string => {
  const { canonicalType } = extractComponentTypeFromDragData({ type: baseName });
  return generateCanonicalSequentialName(canonicalType, existingNodes);
};

export const analyzeMetadata = (metadata: any): { technology: string } => {
  const metaType = metadata?.type || '';
  
  let technology = 'unknown';
  if (metaType.includes('excel')) technology = 'excel';
  else if (metaType.includes('delimited')) technology = 'delimited';
  else if (metaType.includes('database')) technology = 'database';
  else if (metaType.includes('xml')) technology = 'xml';
  else if (metaType.includes('json')) technology = 'json';
  else if (metaType.includes('avro')) technology = 'avro';
  else if (metaType.includes('parquet')) technology = 'parquet';
  else if (metaType.includes('web-service')) technology = 'webservice';
  else if (metaType.includes('ldif')) technology = 'ldif';
  else if (metaType.includes('regex')) technology = 'regex';
  else if (metaType.includes('schema')) technology = 'schema';
  else if (metaType.includes('matchgroup')) technology = 'matchgroup';
  
  return { technology };
};

export const createStandardizedComponent = (
  metadata: any, 
  position: { x: number, y: number },
  existingNodes: CanvasNode[],
  category: 'input' | 'output'
): CanvasNode => {
  const { technology } = analyzeMetadata(metadata);
  
  const { canonicalType, displayName, category: compCategory, cleanType } = extractComponentTypeFromDragData({ type: technology });
  
  const componentName = generateCanonicalSequentialName(canonicalType, existingNodes);
  
  const schemaName = metadata?.schemaName || metadata?.connection?.schema;
  const tableName = metadata?.tableName || metadata?.selectedTables?.[0]?.split('.')?.[1];
  const fileName = metadata?.filePath?.split('/').pop() || metadata?.fileName;
  const sheetName = metadata?.selectedSheet;
  
  const connectionPorts: ComponentPort[] = [];
  if (category === 'input') {
    connectionPorts.push({
      id: `output-${Date.now()}`,
      type: PortType.OUTPUT,
      side: PortSide.RIGHT,
      position: 50,
      label: 'Output',
      maxConnections: 5
    });
  } else if (category === 'output') {
    connectionPorts.push({
      id: `input-${Date.now()}`,
      type: PortType.INPUT,
      side: PortSide.LEFT,
      position: 50,
      label: 'Input',
      maxConnections: 1
    });
  }
  
  const standardSize = { width: 48, height: 30 };
  
  return {
    id: `component-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
    name: componentName,
    type: canonicalType,
    nodeType: 'transform',
    componentType: category,
    componentCategory: category,
    technology: technology,
    position: position,
    size: standardSize,
    connectionPorts: connectionPorts,
    schemaName: schemaName,
    tableName: tableName,
    fileName: fileName,
    sheetName: sheetName,
    metadata: {
      ...metadata,
      source: 'repository',
      originalId: metadata.id,
      description: `Standardized ${category} component for ${technology}`,
      originalDimensions: { width: 200, height: 133 },
      scaledDimensions: standardSize,
      scaleFactor: 0.25,
      visualScaling: {
        widthScale: 0.25,
        heightScale: 0.25,
        fontSizeScale: 0.25,
        iconScale: 0.25,
        paddingScale: 0.25,
        handleScale: 0.25
      },
      dragData: {
        type: 'canvas-node',
        source: 'canvas',
        canBeReDragged: true,
        nodeType: 'transform'
      },
      canonicalType: canonicalType,
      displayName: displayName,
      cleanType: cleanType,
      category: compCategory,
      nameLocked: true
    },
    status: NodeStatus.IDLE,
    draggable: true,
    droppable: false,
    dragType: 'canvas-node'
  };
};

export const createProcessingComponent = (
  componentData: any,
  position: { x: number; y: number },
  existingNodes: CanvasNode[]
): CanvasNode => {
  const { canonicalType, type, displayName, cleanType, metadataName, snakeCaseName } = extractComponentTypeFromDragData(componentData);
  
  const componentName = generateSnakeCaseName(
    snakeCaseName || canonicalType,
    existingNodes,
    {
      baseName: snakeCaseName,
      metadata: componentData.metadata
    }
  );
  
  const connectionPorts: ComponentPort[] = [
    {
      id: `input-${Date.now()}`,
      type: PortType.INPUT,
      side: PortSide.LEFT,
      position: 50,
      label: 'Input',
      maxConnections: 1
    },
    {
      id: `output-${Date.now()}`,
      type: PortType.OUTPUT,
      side: PortSide.RIGHT,
      position: 50,
      label: 'Output',
      maxConnections: 5
    }
  ];

  const visualConfig = componentData.visualConfig || {};
  
  const processingComponentSize = { 
    width: 38,
    height: 10
  };

  return {
    id: `processing-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
    name: componentName,
    type: canonicalType,
    nodeType: 'transform',
    componentType: 'processing',
    componentCategory: 'process',
    technology: canonicalType,
    position: position,
    size: processingComponentSize,
    connectionPorts: connectionPorts,
    metadata: {
      description: componentData.component?.metadata?.description || `Processing component: ${type}`,
      originalId: componentData.component?.id || componentData.id,
      source: 'component-palette',
      componentData: componentData.component || componentData,
      visualProperties: componentData.visualConfig,
      originalAppearance: {
        classes: visualConfig.classes,
        tailwindClasses: visualConfig.tailwindClasses,
        width: visualConfig.width,
        height: visualConfig.height
      },
      originalDimensions: { width: 150, height: 40 },
      scaledDimensions: processingComponentSize,
      scaleFactor: 0.25,
      visualScaling: {
        widthScale: 0.25,
        heightScale: 0.25,
        fontSizeScale: 0.25,
        iconScale: 0.25,
        paddingScale: 0.25,
        handleScale: 0.25
      },
      isNew: true,
      dragData: {
        type: 'canvas-node',
        source: 'canvas',
        canBeReDragged: true,
        nodeType: 'transform'
      },
      canonicalType: canonicalType,
      displayName: displayName,
      cleanType: cleanType,
      metadataName: metadataName,
      baseName: snakeCaseName,
      category: 'process',
      nameLocked: true
    },
    status: NodeStatus.IDLE,
    draggable: true,
    droppable: false,
    dragType: 'canvas-node'
  };
};

export const alignToGrid = (position: { x: number, y: number }, gridSize: number = 20): { x: number; y: number } => {
  const snapX = Math.round(position.x / gridSize) * gridSize;
  const snapY = Math.round(position.y / gridSize) * gridSize;
  
  const minDistance = gridSize;
  const boundedX = Math.max(minDistance, snapX);
  const boundedY = Math.max(minDistance, snapY);
  
  return { x: boundedX, y: boundedY };
};

export const convertCanvasNodeToTablePanel = (canvasNode: any): any => {
  if (!canvasNode) return null;
  
  const tableType = canvasNode.componentCategory === 'input' ? 'input' :
                    canvasNode.componentCategory === 'output' ? 'output' :
                    'lookup';
  
  return {
    id: `table-from-canvas-${canvasNode.id}`,
    name: canvasNode.name,
    type: tableType,
    columns: canvasNode.metadata?.columns?.map((col: any, index: number) => ({
      id: `col-from-canvas-${canvasNode.id}-${index}`,
      name: col.name || `Column_${index + 1}`,
      type: col.type || 'string',
      metadata: col
    })) || [],
    position: { x: 50, y: 100 },
    size: { width: 300, height: 400 },
    metadata: {
      canvasNodeId: canvasNode.id,
      source: 'canvas',
      originalData: canvasNode
    }
  };
};

export const extractTablesFromCanvasNodes = (nodes: any[]): { sourceTables: any[], targetTables: any[] } => {
  const sourceTables: any[] = [];
  const targetTables: any[] = [];
  
  nodes.forEach(node => {
    const table = convertCanvasNodeToTablePanel(node);
    if (table) {
      if (table.type === 'input' || table.type === 'lookup') {
        sourceTables.push(table);
      } else if (table.type === 'output' || table.type === 'reject') {
        targetTables.push(table);
      }
    }
  });
  
  return { sourceTables, targetTables };
};

export const showConnectionSuccess = (_p0?: string) => {
  const feedbackElement = document.createElement('div');
  feedbackElement.className = 'connection-success-feedback';
  feedbackElement.style.cssText = `
    position: fixed;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
    background: rgba(34, 197, 94, 0.9);
    color: white;
    padding: 8px 16px;
    border-radius: 8px;
    font-size: 14px;
    z-index: 9999;
    pointer-events: none;
    animation: fadeOut 1s ease-out forwards;
  `;
  
  const style = document.createElement('style');
  style.textContent = `
    @keyframes fadeOut {
      0% { opacity: 1; }
      70% { opacity: 1; }
      100% { opacity: 0; }
    }
  `;
  document.head.appendChild(style);
  
  feedbackElement.textContent = '✅ Connection established';
  document.body.appendChild(feedbackElement);
  
  setTimeout(() => {
    if (feedbackElement.parentNode) {
      feedbackElement.parentNode.removeChild(feedbackElement);
    }
    document.head.removeChild(style);
  }, 1000);
};

export const showConnectionError = (message: string) => {
  const feedbackElement = document.createElement('div');
  feedbackElement.className = 'connection-error-feedback';
  feedbackElement.style.cssText = `
    position: fixed;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
    background: rgba(239, 68, 68, 0.9);
    color: white;
    padding: 8px 16px;
    border-radius: 8px;
    font-size: 14px;
    z-index: 9999;
    pointer-events: none;
    animation: fadeOut 2s ease-out forwards;
  `;
  
  feedbackElement.textContent = `❌ ${message}`;
  document.body.appendChild(feedbackElement);
  
  setTimeout(() => {
    if (feedbackElement.parentNode) {
      feedbackElement.parentNode.removeChild(feedbackElement);
    }
  }, 2000);
};

// ==================== PORT POSITION CALCULATION FUNCTIONS ====================

export const calculatePortAbsolutePosition = (
  node: CanvasNode, 
  port: ComponentPort, 
  nodePositionOverride?: { x: number; y: number },
  canvasScroll?: { x: number; y: number }
): { x: number; y: number } => {
  const nodePosition = nodePositionOverride || node.position;
  
  let offsetX = 0;
  let offsetY = 0;
  
  switch (port.side) {
    case PortSide.LEFT:
      offsetX = 0;
      offsetY = (node.size.height * port.position) / 100;
      break;
    case PortSide.RIGHT:
      offsetX = node.size.width;
      offsetY = (node.size.height * port.position) / 100;
      break;
    case PortSide.TOP:
      offsetX = (node.size.width * port.position) / 100;
      offsetY = 0;
      break;
    case PortSide.BOTTOM:
      offsetX = (node.size.width * port.position) / 100;
      offsetY = node.size.height;
      break;
  }
  
  const scrollOffsetX = canvasScroll?.x || 0;
  const scrollOffsetY = canvasScroll?.y || 0;
  
  return {
    x: nodePosition.x + offsetX + scrollOffsetX,
    y: nodePosition.y + offsetY + scrollOffsetY
  };
};

export const calculatePortRelativePosition = (
  nodeSize: { width: number; height: number },
  port: ComponentPort
): { offsetX: number; offsetY: number } => {
  let offsetX = 0;
  let offsetY = 0;
  
  switch (port.side) {
    case PortSide.LEFT:
      offsetX = 0;
      offsetY = (nodeSize.height * port.position) / 100;
      break;
    case PortSide.RIGHT:
      offsetX = nodeSize.width;
      offsetY = (nodeSize.height * port.position) / 100;
      break;
    case PortSide.TOP:
      offsetX = (nodeSize.width * port.position) / 100;
      offsetY = 0;
      break;
    case PortSide.BOTTOM:
      offsetX = (nodeSize.width * port.position) / 100;
      offsetY = nodeSize.height;
      break;
  }
  
  return { offsetX, offsetY };
};

export const getVisualPortPosition = (
  absolutePosition: { x: number; y: number },
  element?: HTMLElement | null
): { x: number; y: number } => {
  if (!element) return absolutePosition;
  
  const rect = element.getBoundingClientRect();
  const style = window.getComputedStyle(element);
  const transform = style.transform;
  
  if (transform && transform.includes('translate(-50%, -50%)')) {
    return absolutePosition;
  }
  
  return {
    x: rect.left + rect.width / 2,
    y: rect.top + rect.height / 2
  };
};

export const findClosestPort = (
  position: { x: number; y: number },
  nodes: CanvasNode[],
  portPositions: Map<string, { x: number; y: number }>,
  excludeNodeId?: string,
  excludePortId?: string,
  requiredType?: PortType,
  snapDistance: number = 30
): ConnectionSnapState['candidate'] | null => {
  let closestPort: ConnectionSnapState['candidate'] = null;
  let closestDistance = Infinity;
  
  portPositions.forEach((portPos, key) => {
    const [nodeId, portId] = key.split(':');
    
    if (nodeId === excludeNodeId || portId === excludePortId) return;
    
    const node = nodes.find(n => n.id === nodeId);
    const port = node?.connectionPorts?.find(p => p.id === portId);
    
    if (!node || !port) return;
    
    if (requiredType && port.type !== requiredType) return;
    
    const distance = Math.sqrt(
      Math.pow(position.x - portPos.x, 2) + 
      Math.pow(position.y - portPos.y, 2)
    );
    
    if (distance <= snapDistance && distance < closestDistance) {
      closestDistance = distance;
      closestPort = {
        nodeId,
        portId,
        position: portPos,
        distance,
        portType: port.type
      };
    }
  });
  
  return closestPort;
};

export const validateConnection = (
  sourceNodeId: string,
  sourcePortId: string,
  targetNodeId: string,
  targetPortId: string,
  nodes: CanvasNode[],
  connections: Array<{ sourceNodeId: string; sourcePortId: string; targetNodeId: string; targetPortId: string }>
): { isValid: boolean; reason?: string; errors?: string[] } => {
  const sourceNode = nodes.find(n => n.id === sourceNodeId);
  const targetNode = nodes.find(n => n.id === targetNodeId);
  
  if (!sourceNode || !targetNode) {
    return { isValid: false, reason: 'Nodes not found' };
  }
  
  const sourcePort = sourceNode.connectionPorts?.find(p => p.id === sourcePortId);
  const targetPort = targetNode.connectionPorts?.find(p => p.id === targetPortId);
  
  if (!sourcePort || !targetPort) {
    return { isValid: false, reason: 'Ports not found' };
  }
  
  if (sourceNodeId === targetNodeId) {
    return { isValid: false, reason: 'Cannot connect to same node' };
  }
  
  if (sourcePort.type === targetPort.type) {
    return { 
      isValid: false, 
      reason: 'Cannot connect same port types',
      errors: [`${sourcePort.type} port cannot connect to ${targetPort.type} port`]
    };
  }
  
  const existingConnection = connections.find(conn =>
    (conn.sourceNodeId === sourceNodeId && conn.sourcePortId === sourcePortId &&
     conn.targetNodeId === targetNodeId && conn.targetPortId === targetPortId) ||
    (conn.sourceNodeId === targetNodeId && conn.sourcePortId === targetPortId &&
     conn.targetNodeId === sourceNodeId && conn.targetPortId === sourcePortId)
  );
  
  if (existingConnection) {
    return { isValid: false, reason: 'Connection already exists' };
  }
  
  const sourceConnections = connections.filter(c => 
    (c.sourceNodeId === sourceNodeId && c.sourcePortId === sourcePortId) ||
    (c.targetNodeId === sourceNodeId && c.targetPortId === sourcePortId)
  ).length;
  
  const targetConnections = connections.filter(c => 
    (c.sourceNodeId === targetNodeId && c.sourcePortId === targetPortId) ||
    (c.targetNodeId === targetNodeId && c.targetPortId === targetPortId)
  ).length;
  
  if (sourcePort.maxConnections && sourceConnections >= sourcePort.maxConnections) {
    return { isValid: false, reason: 'Source port has reached maximum connections' };
  }
  
  if (targetPort.maxConnections && targetConnections >= targetPort.maxConnections) {
    return { isValid: false, reason: 'Target port has reached maximum connections' };
  }
  
  return { isValid: true };
};

export const truncateText = (text: string, maxLength: number): string => {
  if (!text || text.length <= maxLength) return text;
  return text.substring(0, maxLength) + '...';
};

export const calculateMaxCharsPerLine = (nodeWidth: number): number => {
  const avgCharWidth = 6 * 0.2;
  const availableWidth = nodeWidth - 8;
  return Math.max(3, Math.floor(availableWidth / avgCharWidth));
};

export const viewportToCanvasCoordinates = (
  viewportPos: { x: number; y: number },
  canvasScroll: { x: number; y: number },
  canvasOffset?: { left: number; top: number }
): { x: number; y: number } => {
  const offsetX = canvasOffset?.left || 0;
  const offsetY = canvasOffset?.top || 0;
  
  return {
    x: viewportPos.x - offsetX + canvasScroll.x,
    y: viewportPos.y - offsetY + canvasScroll.y
  };
};

export const canvasToViewportCoordinates = (
  canvasPos: { x: number; y: number },
  canvasScroll: { x: number; y: number },
  canvasOffset?: { left: number; top: number }
): { x: number; y: number } => {
  const offsetX = canvasOffset?.left || 0;
  const offsetY = canvasOffset?.top || 0;
  
  return {
    x: canvasPos.x + offsetX - canvasScroll.x,
    y: canvasPos.y + offsetY - canvasScroll.y
  };
};

export const getEventCanvasPosition = (
  event: React.MouseEvent | React.TouchEvent,
  canvasElement: HTMLElement | null
): { x: number; y: number } => {
  if (!canvasElement) return { x: 0, y: 0 };
  
  const rect = canvasElement.getBoundingClientRect();
  const scrollX = canvasElement.scrollLeft;
  const scrollY = canvasElement.scrollTop;
  
  const clientX = 'touches' in event 
    ? event.touches[0].clientX 
    : event.clientX;
  const clientY = 'touches' in event 
    ? event.touches[0].clientY 
    : event.clientY;
  
  return {
    x: clientX - rect.left + scrollX,
    y: clientY - rect.top + scrollY
  };
};

export { PortSide, NodeType, PortType, NodeStatus };