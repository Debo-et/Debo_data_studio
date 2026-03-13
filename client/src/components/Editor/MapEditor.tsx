// src/components/Editor/MapEditor.tsx - COMPLETE VERSION WITH METADATA STRATEGIES
import React, { useState, useRef, useEffect, useCallback, useMemo } from "react";

/* =======================
   Types - Enhanced with Metadata Strategies
======================= */

import { 
  FieldSchema, 
  DataType,
  MapTransformation,
  MapComponentConfiguration} from "../../types/metadata";

// Simplified Column interface for tMap integration
export interface SimpleColumn {
  name: string;
  type?: string;
  id?: string;
}

// ColumnMapping type for backward compatibility
export interface ColumnMapping {
  id: string;
  sourceTable?: string;
  sourceColumn?: string;
  targetColumn: string;
  expression?: string;
  transformation?: string;
  dataType: string;
  isDirectMapping: boolean;
}

// Extended ColumnDefinition with all metadata
export interface ColumnDefinition extends Omit<FieldSchema, 'isKey'> {
  expression?: string;
  isKey?: boolean;
  postgresType?: string;
  // Metadata for compilation
  transformationId?: string;
  sourceField?: string;
  validationStatus?: 'VALID' | 'WARNING' | 'ERROR';
}

export interface TableDefinition {
  id: string;
  name: string;
  alias?: string;
  type?: 'input' | 'output' | 'lookup' | 'reject';
  columns: ColumnDefinition[];
  // SchemaDefinition properties
  sourceComponentId?: string;
  isTemporary?: boolean;
  isMaterialized?: boolean;
  metadata?: Record<string, any>;
  // Additional metadata for SQL generation
  sourceNodeId?: string;
}

export interface Variable {
  id: string;
  name: string;
  expression: string;
  type?: DataType;
}

export interface Wire {
  id: string;
  sourceTableId: string;
  sourceColumnId: string;
  targetTableId: string;
  targetColumnId: string;
  transformation?: string;
  color?: string;
  // Metadata for compilation
  isActive: boolean;
  position?: number;
}

/* =======================
   MapEditor Props Interface
======================= */

interface MapEditorProps {
  // Simplified props for tMap integration
  inputColumns?: SimpleColumn[];
  outputColumns?: SimpleColumn[];
  
  // Existing props for backward compatibility
  sourceTables?: TableDefinition[];
  targetTables?: TableDefinition[];
  initialTables?: TableDefinition[];
  initialConfig?: MapComponentConfiguration;
  onClose?: () => void;
  onSave?: (config: MapComponentConfiguration) => void;
  
  // NEW: Node metadata for saving
  nodeId?: string;
  nodeMetadata?: any;
}

/* =======================
   PostgreSQL Data Types
======================= */

const POSTGRES_DATA_TYPES = [
  // Numeric Types
  { value: 'SMALLINT', label: 'SMALLINT', dataType: 'INTEGER' as DataType },
  { value: 'INTEGER', label: 'INTEGER', dataType: 'INTEGER' as DataType },
  { value: 'BIGINT', label: 'BIGINT', dataType: 'INTEGER' as DataType },
  { value: 'DECIMAL', label: 'DECIMAL', dataType: 'DECIMAL' as DataType },
  { value: 'NUMERIC', label: 'NUMERIC', dataType: 'DECIMAL' as DataType },
  { value: 'REAL', label: 'REAL', dataType: 'DECIMAL' as DataType },
  { value: 'DOUBLE PRECISION', label: 'DOUBLE PRECISION', dataType: 'DECIMAL' as DataType },
  { value: 'SERIAL', label: 'SERIAL', dataType: 'INTEGER' as DataType },
  { value: 'BIGSERIAL', label: 'BIGSERIAL', dataType: 'INTEGER' as DataType },
  
  // Character Types
  { value: 'CHAR', label: 'CHAR', dataType: 'STRING' as DataType },
  { value: 'CHARACTER', label: 'CHARACTER', dataType: 'STRING' as DataType },
  { value: 'VARCHAR', label: 'VARCHAR', dataType: 'STRING' as DataType },
  { value: 'TEXT', label: 'TEXT', dataType: 'STRING' as DataType },
  
  // Date/Time Types
  { value: 'DATE', label: 'DATE', dataType: 'DATE' as DataType },
  { value: 'TIME', label: 'TIME', dataType: 'STRING' as DataType },
  { value: 'TIMESTAMP', label: 'TIMESTAMP', dataType: 'TIMESTAMP' as DataType },
  { value: 'TIMESTAMPTZ', label: 'TIMESTAMPTZ', dataType: 'TIMESTAMP' as DataType },
  { value: 'INTERVAL', label: 'INTERVAL', dataType: 'STRING' as DataType },
  
  // Boolean Type
  { value: 'BOOLEAN', label: 'BOOLEAN', dataType: 'BOOLEAN' as DataType },
  
  // Binary Types
  { value: 'BYTEA', label: 'BYTEA', dataType: 'BINARY' as DataType },
  
  // JSON Types
  { value: 'JSON', label: 'JSON', dataType: 'STRING' as DataType },
  { value: 'JSONB', label: 'JSONB', dataType: 'STRING' as DataType },
  
  // UUID Type
  { value: 'UUID', label: 'UUID', dataType: 'STRING' as DataType },
];

/* =======================
   Highlight Color Management
======================= */

const HIGHLIGHT_COLORS = [
  '#FFDDC1', // Light orange
  '#C1FFD7', // Light green
  '#C1E1FF', // Light blue
  '#E0C1FF', // Light purple
  '#FFFDC1', // Light yellow
  '#FFC1E1', // Light pink
  '#C1FFF9', // Light cyan
  '#D5FFC1', // Light lime
  '#FFC1C1', // Light red
  '#E8C1FF', // Light lavender
  '#C1F0FF', // Light sky blue
  '#FFEDC1', // Light peach
];

class ColorManager {
  private availableColors: string[];
  private wireColorMap: Map<string, string>;
  
  constructor() {
    this.availableColors = [...HIGHLIGHT_COLORS];
    this.wireColorMap = new Map();
  }
  
  assignColor(wireId: string): string {
    if (this.wireColorMap.has(wireId)) {
      return this.wireColorMap.get(wireId)!;
    }
    
    if (this.availableColors.length === 0) {
      this.availableColors = [...HIGHLIGHT_COLORS].filter(
        color => !Array.from(this.wireColorMap.values()).includes(color)
      );
      
      if (this.availableColors.length === 0) {
        return HIGHLIGHT_COLORS[0];
      }
    }
    
    const color = this.availableColors.shift()!;
    this.wireColorMap.set(wireId, color);
    return color;
  }
  
  releaseColor(wireId: string): void {
    const color = this.wireColorMap.get(wireId);
    if (color) {
      this.wireColorMap.delete(wireId);
      this.availableColors.unshift(color);
    }
  }
  
  getColor(wireId: string): string | undefined {
    return this.wireColorMap.get(wireId);
  }
  
  clear(): void {
    this.availableColors = [...HIGHLIGHT_COLORS];
    this.wireColorMap.clear();
  }
}

/* =======================
   Helper Functions
======================= */

const generateId = (prefix: string = 'wire'): string => {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
};

const mapStringToDataType = (type: string): DataType => {
  const upperType = type.toUpperCase();
  
  if (upperType.includes('INT') || upperType.includes('SERIAL')) {
    return 'INTEGER';
  }
  if (upperType.includes('DECIMAL') || upperType.includes('NUMERIC') || 
      upperType.includes('FLOAT') || upperType.includes('DOUBLE') || upperType.includes('REAL')) {
    return 'DECIMAL';
  }
  if (upperType.includes('BOOL')) {
    return 'BOOLEAN';
  }
  if (upperType.includes('DATE')) {
    return 'DATE';
  }
  if (upperType.includes('TIMESTAMP')) {
    return 'TIMESTAMP';
  }
  if (upperType.includes('BINARY') || upperType.includes('BLOB') || upperType.includes('BYTEA')) {
    return 'BINARY';
  }
  return 'STRING';
};

const mapDataTypeToDefaultPostgresType = (dataType: DataType): string => {
  switch (dataType) {
    case 'INTEGER': return 'INTEGER';
    case 'DECIMAL': return 'DECIMAL';
    case 'STRING': return 'VARCHAR';
    case 'BOOLEAN': return 'BOOLEAN';
    case 'DATE': return 'DATE';
    case 'TIMESTAMP': return 'TIMESTAMP';
    case 'BINARY': return 'BYTEA';
    default: return 'VARCHAR';
  }
};

const findPostgresTypeByValue = (value: string) => {
  return POSTGRES_DATA_TYPES.find(t => t.value === value);
};

/* =======================
   Expression Editor Modal Component
======================= */

interface ExpressionEditorModalProps {
  column: ColumnDefinition;
  onSave: (expression: string) => void;
  onClose: () => void;
}

const ExpressionEditorModal: React.FC<ExpressionEditorModalProps> = ({
  column,
  onSave,
  onClose
}) => {
  const [expression, setExpression] = useState(column.expression || '');
  const [isFullscreen, setIsFullscreen] = useState(false);
  
  const handleSave = () => {
    onSave(expression);
    onClose();
  };
  
  const pgFunctions = [
    // String Functions
    'CONCAT(', 'SUBSTRING(', 'TRIM(', 'UPPER(', 'LOWER(', 'LENGTH(', 'REPLACE(', 
    'POSITION(', 'COALESCE(', 'NULLIF(', 'FORMAT(',
    // Numeric Functions
    'ABS(', 'ROUND(', 'CEIL(', 'FLOOR(', 'MOD(', 'POWER(', 'SQRT(', 'RANDOM()',
    // Date Functions
    'CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'EXTRACT(', 'DATE_TRUNC(',
    'AGE(', 'TO_CHAR(', 'TO_DATE(', 'TO_TIMESTAMP(',
    // Conditional
    'CASE WHEN', 'COALESCE(', 'NULLIF(', 'GREATEST(', 'LEAST(',
    // Type Conversion
    'CAST(', '::INTEGER', '::VARCHAR', '::DATE', '::NUMERIC'
  ];
  
  return (
    <div className={`fixed inset-0 z-[10000] bg-black bg-opacity-80 flex items-center justify-center p-4 ${isFullscreen ? 'p-0' : ''}`}>
      <div className={`bg-white rounded-xl shadow-2xl flex flex-col ${isFullscreen ? 'w-full h-full' : 'w-full max-w-3xl h-96'}`}>
        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b bg-gradient-to-r from-blue-50 to-gray-50">
          <div>
            <h2 className="text-lg font-bold text-gray-800 flex items-center">
              <span className="mr-2">📝</span>
              Expression Editor
            </h2>
            <p className="text-sm text-gray-600 mt-1">
              Column: <span className="font-semibold text-blue-600">{column.name}</span>
              <span className="ml-2 text-xs bg-gray-100 text-gray-800 px-2 py-0.5 rounded">
                {column.type}
              </span>
            </p>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => setIsFullscreen(!isFullscreen)}
              className="p-2 hover:bg-gray-100 rounded-lg transition-colors"
              title={isFullscreen ? "Exit Fullscreen" : "Enter Fullscreen"}
            >
              {isFullscreen ? '⤢' : '⤡'}
            </button>
            <button
              onClick={onClose}
              className="p-2 hover:bg-gray-100 rounded-lg transition-colors"
              title="Close"
            >
              ✕
            </button>
          </div>
        </div>
        
        {/* Main Content */}
        <div className="flex flex-1 overflow-hidden">
          {/* Function Palette */}
          <div className="w-64 border-r bg-gray-50 overflow-y-auto p-4">
            <h3 className="text-sm font-semibold text-gray-700 mb-3">PostgreSQL Functions</h3>
            <div className="space-y-2">
              {pgFunctions.map((func, idx) => (
                <button
                  key={idx}
                  onClick={() => {
                    const textarea = document.getElementById('expression-editor') as HTMLTextAreaElement;
                    if (textarea) {
                      const start = textarea.selectionStart;
                      const end = textarea.selectionEnd;
                      const newExpression = expression.substring(0, start) + func + expression.substring(end);
                      setExpression(newExpression);
                      setTimeout(() => {
                        textarea.focus();
                        textarea.setSelectionRange(start + func.length, start + func.length);
                      }, 0);
                    }
                  }}
                  className="w-full text-left text-xs font-mono bg-white border border-gray-200 px-3 py-2 rounded hover:bg-blue-50 hover:border-blue-300 transition-colors truncate"
                >
                  {func}
                </button>
              ))}
            </div>
            
            <div className="mt-6 pt-4 border-t">
              <h3 className="text-sm font-semibold text-gray-700 mb-2">Syntax Tips</h3>
              <ul className="text-xs text-gray-600 space-y-1">
                <li>• Use || for string concatenation</li>
                <li>• Use :: for type casting</li>
                <li>• Use COALESCE() for null handling</li>
                <li>• Use CASE for conditional logic</li>
              </ul>
            </div>
          </div>
          
          {/* Editor Area */}
          <div className="flex-1 flex flex-col">
            <div className="p-4 border-b">
              <label className="text-sm font-medium text-gray-700 mb-2">
                PostgreSQL Expression
              </label>
              <div className="relative">
                <textarea
                  id="expression-editor"
                  value={expression}
                  onChange={(e) => setExpression(e.target.value)}
                  className="w-full h-48 font-mono text-sm border border-gray-300 rounded-lg p-3 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 resize-none"
                  placeholder={`Enter PostgreSQL expression for ${column.name}...`}
                  spellCheck="false"
                />
                {!expression && (
                  <div className="absolute inset-0 pointer-events-none flex items-center justify-center text-gray-400">
                    Example: UPPER(first_name) || ' ' || UPPER(last_name)
                  </div>
                )}
              </div>
            </div>
            
            {/* Preview/Help */}
            <div className="flex-1 p-4 overflow-y-auto">
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <h4 className="text-sm font-medium text-gray-700 mb-2">Examples</h4>
                  <div className="space-y-2">
                    <div className="text-xs font-mono bg-gray-100 p-2 rounded border">
                      <div className="text-gray-500">String concatenation:</div>
                      <div>CONCAT(first_name, ' ', last_name)</div>
                    </div>
                    <div className="text-xs font-mono bg-gray-100 p-2 rounded border">
                      <div className="text-gray-500">Type casting:</div>
                      <div>quantity::VARCHAR || ' units'</div>
                    </div>
                    <div className="text-xs font-mono bg-gray-100 p-2 rounded border">
                      <div className="text-gray-500">Conditional:</div>
                      <div>CASE WHEN active THEN 'Yes' ELSE 'No' END</div>
                    </div>
                  </div>
                </div>
                <div>
                  <h4 className="text-sm font-medium text-gray-700 mb-2">Current Value</h4>
                  <div className="text-xs font-mono bg-blue-50 p-3 rounded border border-blue-200">
                    {expression || <span className="text-gray-400 italic">No expression defined</span>}
                  </div>
                  <div className="mt-3 text-xs text-gray-500">
                    <div className="font-medium mb-1">Available shortcuts:</div>
                    <div>• Ctrl+Enter: Save</div>
                    <div>• Esc: Cancel</div>
                    <div>• Tab: Autocomplete</div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
        
        {/* Footer */}
        <div className="flex items-center justify-between p-4 border-t bg-gray-50">
          <div className="text-xs text-gray-600">
            Expression will be validated against PostgreSQL syntax
          </div>
          <div className="flex gap-2">
            <button
              onClick={onClose}
              className="px-4 py-2 text-sm border border-gray-300 rounded hover:bg-gray-50 transition-colors"
            >
              Cancel
            </button>
            <button
              onClick={handleSave}
              className="px-4 py-2 text-sm bg-gradient-to-r from-blue-500 to-blue-600 text-white rounded hover:from-blue-600 hover:to-blue-700 transition-all"
            >
              Save Expression
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

/* =======================
   MapEditor Component
======================= */

const MapEditor: React.FC<MapEditorProps> = ({ 
  inputColumns = [],
  outputColumns = [],
  sourceTables = [],
  targetTables = [],
  initialConfig,
  nodeId,
  nodeMetadata,
  onClose, 
  onSave 
}) => {
  // State management
  const [inputTable] = useState<TableDefinition>(() => {
    if (inputColumns.length > 0) {
      const columns: ColumnDefinition[] = inputColumns.map((col, index) => ({
        id: col.id || `input-col-${index}`,
        name: col.name,
        type: mapStringToDataType(col.type || 'STRING'),
        length: undefined,
        precision: undefined,
        scale: undefined,
        nullable: true,
        isKey: false,
        expression: undefined,
        description: undefined,
        postgresType: col.type || mapDataTypeToDefaultPostgresType(mapStringToDataType(col.type || 'STRING'))
      }));
      
      return {
        id: 'input-schema',
        name: 'Input Table',
        alias: '',
        columns,
        isTemporary: false,
        isMaterialized: false,
        metadata: {}
      };
    }
    
    return sourceTables[0] || {
      id: 'input-schema',
      name: 'Input Table',
      alias: '',
      columns: [],
      isTemporary: false,
      isMaterialized: false,
      metadata: {}
    };
  });
  
  const [outputTable, setOutputTable] = useState<TableDefinition>(() => {
    if (outputColumns.length > 0) {
      const columns: ColumnDefinition[] = outputColumns.map((col, index) => ({
        id: col.id || `output-col-${index}`,
        name: col.name,
        type: mapStringToDataType(col.type || 'STRING'),
        length: undefined,
        precision: undefined,
        scale: undefined,
        nullable: true,
        isKey: false,
        expression: undefined,
        description: undefined,
        postgresType: col.type || mapDataTypeToDefaultPostgresType(mapStringToDataType(col.type || 'STRING'))
      }));
      
      return {
        id: 'output-schema',
        name: 'Output Table',
        alias: '',
        columns,
        isTemporary: false,
        isMaterialized: false,
        metadata: {}
      };
    }
    
    if (targetTables.length > 0 && targetTables[0].columns.length > 0) {
      const firstTable = targetTables[0];
      const columns: ColumnDefinition[] = firstTable.columns.map((col, _index) => ({
        ...col,
        postgresType: col.postgresType || mapDataTypeToDefaultPostgresType(col.type)
      }));
      
      return {
        ...firstTable,
        id: firstTable.id || 'output-schema',
        name: firstTable.name || 'Output Table',
        alias: firstTable.alias || '',
        columns,
        isTemporary: firstTable.isTemporary !== undefined ? firstTable.isTemporary : false,
        isMaterialized: firstTable.isMaterialized !== undefined ? firstTable.isMaterialized : false,
        metadata: firstTable.metadata || {}
      };
    }
    
    return {
      id: 'output-schema',
      name: 'Output Table',
      alias: '',
      columns: [],
      isTemporary: false,
      isMaterialized: false,
      metadata: {}
    };
  });
  
  const [variables, setVariables] = useState<Variable[]>([]);
  const [wires, setWires] = useState<Wire[]>(() => {
    // Initialize wires from initialConfig if provided
    if (initialConfig?.transformations) {
      return initialConfig.transformations.map(trans => ({
        id: trans.id || generateId(),
        sourceTableId: 'input-schema',
        sourceColumnId: inputTable.columns.find(c => c.name === trans.sourceField)?.id || '',
        targetTableId: 'output-schema',
        targetColumnId: outputTable.columns.find(c => c.name === trans.targetField)?.id || '',
        transformation: trans.expression,
        isActive: true,
        position: trans.position
      }));
    }
    return [];
  });
  
  // UI State
  const [selectedColumnId, setSelectedColumnId] = useState<string | null>(null);
  const [mappingMode, setMappingMode] = useState<'idle' | 'selecting-source' | 'selecting-target'>('idle');
  const [activeSourceColumn, setActiveSourceColumn] = useState<string | null>(null);
  const [hoveredWireId, setHoveredWireId] = useState<string | null>(null);
  const [expressionEditorState, setExpressionEditorState] = useState<{
    isOpen: boolean;
    column: ColumnDefinition | null;
  }>({
    isOpen: false,
    column: null
  });
  
  // Resizable panel state
  const [schemaPanelHeight, setSchemaPanelHeight] = useState<number>(256);
  const [isResizing, setIsResizing] = useState<boolean>(false);
  const resizeContainerRef = useRef<HTMLDivElement>(null);
  
  // Color manager
  const colorManagerRef = useRef<ColorManager>(new ColorManager());

  // ==================== COMPILER METADATA FUNCTIONS ====================

  /**
   * Generate MapTransformation objects from wires and expressions
   */
  const generateTransformations = useCallback((): MapTransformation[] => {
    const transformations: MapTransformation[] = [];

    outputTable.columns.forEach((col, index) => {
      const columnWires = wires.filter(w => w.targetColumnId === col.id);
      
      if (columnWires.length === 0 && col.expression) {
        // Expression-only transformation (no wire)
        transformations.push({
          id: generateId('trans'),
          sourceField: '',
          targetField: col.name,
          expression: col.expression,
          expressionType: 'SQL',
          dataType: col.type,
          isDirectMapping: false,
          validationRules: [],
          position: index,
          nullHandling: 'KEEP_NULL',
          defaultValue: undefined
        });
      } else if (columnWires.length > 0) {
        // Wire-based transformations
        columnWires.forEach((wire, wireIndex) => {
          const sourceColumn = inputTable.columns.find(c => c.id === wire.sourceColumnId);
          
          transformations.push({
            id: wire.id,
            sourceField: sourceColumn?.name || '',
            sourceTable: 'input',
            targetField: col.name,
            expression: col.expression || (sourceColumn ? `{${sourceColumn.name}}` : ''),
            expressionType: col.expression ? 'SQL' : 'REFERENCE',
            dataType: col.type,
            isDirectMapping: !col.expression,
            validationRules: [],
            position: index * 100 + wireIndex, // Ensure deterministic ordering
            nullHandling: 'KEEP_NULL',
            defaultValue: undefined
          });
        });
      }
    });

    return transformations;
  }, [inputTable, outputTable, wires]);

  /**
   * Extract column references from SQL expression
   */
  const extractColumnReferences = useCallback((expression: string): string[] => {
    const patterns = [
      /\{([^}]+)\}/g,            // {column_name}
      /([a-zA-Z_][a-zA-Z0-9_]*)(?=\s*(?:[+/*-]|$|,|\)))/g, // column_name
      /([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)/g // table.column
    ];
    
    const references: string[] = [];
    
    patterns.forEach(pattern => {
      let match;
      while ((match = pattern.exec(expression)) !== null) {
        references.push(match[1] || match[0]);
      }
    });
    
    return [...new Set(references)];
  }, []);

  /**
   * Compute column dependencies for SQL generation
   */
  const computeColumnDependenciesFromTransformations = useCallback((transformations: MapTransformation[]): Record<string, string[]> => {
    const dependencies: Record<string, string[]> = {};
    
    transformations.forEach(trans => {
      let sources: string[] = [];
      
      if (trans.expressionType === 'REFERENCE' && trans.sourceField) {
        sources = [trans.sourceField];
      } else if (trans.expressionType === 'SQL' && trans.expression) {
        sources = extractColumnReferences(trans.expression);
      }
      
      dependencies[trans.targetField] = sources;
    });
    
    return dependencies;
  }, [extractColumnReferences]);

  /**
   * Validate transformations for compiler
   */
  const validateTransformations = useCallback((transformations: MapTransformation[]): { isValid: boolean; warnings: string[] } => {
    const warnings: string[] = [];
    
    // Check for unmapped output columns
    const outputColumns = outputTable.columns.map(c => c.name);
    const mappedColumns = transformations.map(t => t.targetField);
    const unmappedColumns = outputColumns.filter(col => !mappedColumns.includes(col));
    
    if (unmappedColumns.length > 0) {
      warnings.push(`Unmapped output columns: ${unmappedColumns.join(', ')}`);
    }
    
    // Check for duplicate transformations on same target
    const targetCounts: Record<string, number> = {};
    transformations.forEach(t => {
      targetCounts[t.targetField] = (targetCounts[t.targetField] || 0) + 1;
    });
    
    Object.entries(targetCounts).forEach(([target, count]) => {
      if (count > 1) {
        warnings.push(`Multiple transformations for column "${target}"`);
      }
    });
    
    return {
      isValid: warnings.length === 0,
      warnings
    };
  }, [outputTable.columns]);

  /**
   * Generate complete MapComponentConfiguration for compiler
   */
  const generateMapComponentConfiguration = useCallback((): MapComponentConfiguration => {
    const transformations = generateTransformations();
    const validation = validateTransformations(transformations);
    const columnDependencies = computeColumnDependenciesFromTransformations(transformations);
    
    // Determine SQL generation requirements
    const requiresAggregation = transformations.some(t => 
      t.expression?.toUpperCase().includes('SUM(') ||
      t.expression?.toUpperCase().includes('COUNT(') ||
      t.expression?.toUpperCase().includes('AVG(') ||
      t.expression?.toUpperCase().includes('MIN(') ||
      t.expression?.toUpperCase().includes('MAX(')
    );
    
    const requiresWindowFunction = transformations.some(t => 
      t.expression?.toUpperCase().includes('OVER(') ||
      t.expression?.toUpperCase().includes('ROW_NUMBER()') ||
      t.expression?.toUpperCase().includes('RANK()')
    );
    
    const requiresDistinct = transformations.some(t => 
      t.expression?.toUpperCase().includes('DISTINCT')
    );
    
    // Count active mappings
    const activeMappings = wires.filter(w => w.isActive).length;
    
    // Get output table properties with defaults
    const tableId = outputTable.id || 'output-schema';
    const tableName = outputTable.name || 'Output Schema';
    
    const config: MapComponentConfiguration = {
      version: "1.0",
      transformations,
      joins: [],
      lookups: [],
      filters: [],
      variables: variables.map(v => ({
        id: v.id,
        name: v.name,
        type: v.type || 'STRING',
        expression: v.expression,
        scope: 'ROW',
        isConstant: false
      })),
      
      // Output schema definition - MUST INCLUDE ALL SchemaDefinition properties
      outputSchema: {
        // Required SchemaDefinition properties
        id: tableId,
        name: tableName,
        alias: outputTable.alias || '',
        fields: outputTable.columns.map(col => {
          const transformation = transformations.find(t => t.targetField === col.name);
          
          return {
            id: col.id || `${tableId}_${col.name}`,
            name: col.name,
            type: col.type,
            length: col.length,
            precision: col.precision,
            scale: col.scale,
            nullable: col.nullable !== false,
            isKey: col.isKey || false,
            defaultValue: col.defaultValue,
            description: col.expression || col.description,
            originalName: col.name,
            transformation: transformation?.expression,
            metadata: {
              sourceField: transformation?.sourceField,
              expressionType: transformation?.expressionType,
              position: transformation?.position,
              ...col.metadata
            }
          };
        }),
        sourceComponentId: outputTable.sourceNodeId,
        isTemporary: outputTable.isTemporary || false,
        isMaterialized: outputTable.isMaterialized || false,
        rowCount: undefined,
        metadata: outputTable.metadata || {},
        // Required persistenceLevel property
        persistenceLevel: 'MEMORY' as const
      },
      
      // SQL generation metadata
      sqlGeneration: {
        requiresDistinct,
        requiresAggregation,
        requiresWindowFunction,
        requiresSubquery: requiresAggregation || requiresWindowFunction,
        estimatedRowMultiplier: 1.0,
        parallelizable: true,
        batchSize: 1000,
        memoryHint: 'MEDIUM' as const,
        joinOptimizationHint: undefined
      },
      
      // Compiler metadata
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        createdBy: 'map-editor',
        mappingCount: activeMappings,
        validationStatus: validation.isValid ? 'VALID' as const : 'WARNING' as const,
        warnings: validation.warnings,
        dependencies: [], // Will be populated by Canvas
        columnDependencies,
        compiledSql: undefined,
        compilationTimestamp: undefined
      }
    };
    
    return config;
  }, [
    generateTransformations,
    validateTransformations,
    computeColumnDependenciesFromTransformations,
    wires,
    variables,
    outputTable
  ]);

  /**
   * Save handler with metadata strategies
   */
  const handleSave = useCallback(() => {
    try {
      // Generate compiler-friendly configuration
      const compilerConfig = generateMapComponentConfiguration();
      
      // Create metadata update
      
      // Log for debugging
      console.log('🎯 Saving Map Editor Configuration:', {
        nodeId,
        transformationCount: compilerConfig.transformations.length,
        columnCount: outputTable.columns.length,
        wiresCount: wires.length,
        validationStatus: compilerConfig.compilerMetadata.validationStatus
      });
      
      // Call parent save handler with complete configuration
      onSave?.(compilerConfig);
      
    } catch (error) {
      console.error('❌ Error saving map configuration:', error);
      // Optionally show error to user
    }
  }, [
    generateMapComponentConfiguration,
    nodeId,
    nodeMetadata,
    onSave,
    outputTable,
    wires
  ]);

  // ==================== UI HELPER FUNCTIONS ====================

  // Calculate statistics
  const inputColumnCount = useMemo(() => inputTable.columns.length, [inputTable]);
  const outputColumnCount = useMemo(() => outputTable.columns.length, [outputTable]);
  const mappedColumnCount = useMemo(() => 
    outputTable.columns.filter(c => 
      c.expression || wires.some(w => w.targetColumnId === c.id && w.isActive)
    ).length,
    [outputTable, wires]
  );
  const mappingPercentage = useMemo(() => 
    outputColumnCount > 0 ? Math.round((mappedColumnCount / outputColumnCount) * 100) : 0,
    [outputColumnCount, mappedColumnCount]
  );

  // Color highlighting
  const getColumnHighlightColor = useCallback((columnId: string, isInput: boolean): string | undefined => {
    const relevantWires = wires.filter(wire => 
      isInput ? wire.sourceColumnId === columnId : wire.targetColumnId === columnId
    );
    
    if (relevantWires.length === 0) return undefined;
    
    const wireId = relevantWires[0].id;
    return colorManagerRef.current.getColor(wireId);
  }, [wires]);

  const getColumnHoverEmphasis = useCallback((columnId: string, isInput: boolean): boolean => {
    if (!hoveredWireId) return false;
    
    const hoveredWire = wires.find(w => w.id === hoveredWireId);
    if (!hoveredWire) return false;
    
    return isInput 
      ? hoveredWire.sourceColumnId === columnId
      : hoveredWire.targetColumnId === columnId;
  }, [hoveredWireId, wires]);

  const getWireIdsForColumn = useCallback((columnId: string, isInput: boolean): string[] => {
    return wires
      .filter(wire => (isInput ? wire.sourceColumnId === columnId : wire.targetColumnId === columnId))
      .map(wire => wire.id);
  }, [wires]);

  // Auto-generate expressions from wires
  const generateExpressionFromWires = useCallback((targetColumnId: string): string => {
    const columnWires = wires.filter(w => w.targetColumnId === targetColumnId && w.isActive);
    
    if (columnWires.length === 0) return "";
    
    if (columnWires.length === 1) {
      const wire = columnWires[0];
      const sourceColumn = inputTable.columns.find(c => c.id === wire.sourceColumnId);
      return sourceColumn ? sourceColumn.name : "";
    }
    
    // Multiple wires to same target - create concatenated expression
    const expressions = columnWires.map(wire => {
      const sourceColumn = inputTable.columns.find(c => c.id === wire.sourceColumnId);
      return sourceColumn ? sourceColumn.name : "";
    }).filter(Boolean);
    
    return `CONCAT(${expressions.join(', ')})`;
  }, [wires, inputTable]);

  // Update expressions when wires change
  useEffect(() => {
    const updatedColumns = outputTable.columns.map(col => {
      const expression = generateExpressionFromWires(col.id);
      if (expression && !col.expression) {
        return { ...col, expression };
      }
      return col;
    });
    
    // Only update if changes were made
    if (JSON.stringify(updatedColumns) !== JSON.stringify(outputTable.columns)) {
      setOutputTable(prev => ({
        ...prev,
        columns: updatedColumns
      }));
    }
  }, [wires, generateExpressionFromWires, outputTable.columns]);

  // Initialize color manager
  useEffect(() => {
    const updatedWires = wires.map(wire => {
      if (!wire.color) {
        const color = colorManagerRef.current.assignColor(wire.id);
        return { ...wire, color };
      }
      return wire;
    });
    
    if (updatedWires.some((wire, idx) => wire.color !== wires[idx]?.color)) {
      setWires(updatedWires);
    }
  }, [wires.length]);

  // ==================== RESIZE HANDLERS ====================

  const handleResizeStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    setIsResizing(true);
    document.body.style.userSelect = 'none';
    document.body.style.cursor = 'row-resize';
  }, []);

  const handleResizeMove = useCallback((clientY: number) => {
    if (!resizeContainerRef.current || !isResizing) return;
    
    const containerRect = resizeContainerRef.current.getBoundingClientRect();
    const containerHeight = containerRect.height;
    const containerTop = containerRect.top;
    
    const relativeY = clientY - containerTop;
    const mainAreaHeight = containerHeight - relativeY;
    
    const minHeight = 150;
    const maxHeight = containerHeight * 0.7;
    
    const newHeight = Math.max(minHeight, Math.min(maxHeight, mainAreaHeight));
    setSchemaPanelHeight(newHeight);
  }, [isResizing]);

  const handleResizeEnd = useCallback(() => {
    setIsResizing(false);
    document.body.style.userSelect = '';
    document.body.style.cursor = '';
  }, []);

  // Mouse event handlers for resize
  useEffect(() => {
    const handleMouseMove = (e: MouseEvent) => {
      if (isResizing) {
        handleResizeMove(e.clientY);
      }
    };

    const handleMouseUp = () => {
      if (isResizing) {
        handleResizeEnd();
      }
    };

    if (isResizing) {
      document.addEventListener('mousemove', handleMouseMove);
      document.addEventListener('mouseup', handleMouseUp);
    }

    return () => {
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
    };
  }, [isResizing, handleResizeMove, handleResizeEnd]);

  // ==================== MAPPING INTERACTION FUNCTIONS ====================

  const handleInputColumnClick = useCallback((columnId: string) => {
    if (mappingMode === 'selecting-target') {
      setMappingMode('idle');
      setActiveSourceColumn(null);
      return;
    }
    
    if (activeSourceColumn === columnId) {
      setMappingMode('idle');
      setActiveSourceColumn(null);
    } else {
      setActiveSourceColumn(columnId);
      setMappingMode('selecting-target');
    }
  }, [mappingMode, activeSourceColumn]);

  const handleOutputColumnClick = useCallback((columnId: string) => {
    if (mappingMode === 'selecting-target' && activeSourceColumn) {
      const existingWire = wires.find(w => w.targetColumnId === columnId && w.isActive);
      const newWires = wires.filter(w => w.targetColumnId !== columnId || !w.isActive);
      
      if (existingWire) {
        colorManagerRef.current.releaseColor(existingWire.id);
      }
      
      const newWireId = generateId();
      const newColor = colorManagerRef.current.assignColor(newWireId);
      
      newWires.push({
        id: newWireId,
        sourceTableId: 'input-schema',
        sourceColumnId: activeSourceColumn,
        targetTableId: 'output-schema',
        targetColumnId: columnId,
        color: newColor,
        isActive: true,
        position: wires.length
      });
      
      setWires(newWires);
      setMappingMode('idle');
      setActiveSourceColumn(null);
    } else if (mappingMode === 'idle') {
      setSelectedColumnId(columnId);
    }
  }, [mappingMode, activeSourceColumn, wires]);

  const cancelMappingMode = useCallback(() => {
    setMappingMode('idle');
    setActiveSourceColumn(null);
  }, []);

  // Keyboard shortcuts
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        if (expressionEditorState.isOpen) {
          setExpressionEditorState({ isOpen: false, column: null });
        } else {
          cancelMappingMode();
        }
      }
      if (e.key === 'Enter' && e.ctrlKey && expressionEditorState.isOpen && expressionEditorState.column) {
        const textarea = document.getElementById('expression-editor') as HTMLTextAreaElement;
        if (textarea) {
          handleSaveExpression(expressionEditorState.column.id, textarea.value);
          setExpressionEditorState({ isOpen: false, column: null });
        }
      }
      if (e.key === 's' && e.ctrlKey) {
        e.preventDefault();
        handleSave();
      }
    };
    
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [cancelMappingMode, expressionEditorState, handleSave]);

  // Hover highlighting
  const handleInputColumnHover = useCallback((columnId: string) => {
    const wireIds = getWireIdsForColumn(columnId, true);
    if (wireIds.length > 0) {
      setHoveredWireId(wireIds[0]);
    }
  }, [getWireIdsForColumn]);

  const handleOutputColumnHover = useCallback((columnId: string) => {
    const wireIds = getWireIdsForColumn(columnId, false);
    if (wireIds.length > 0) {
      setHoveredWireId(wireIds[0]);
    }
  }, [getWireIdsForColumn]);

  const handleColumnHoverEnd = useCallback(() => {
    setHoveredWireId(null);
  }, []);

  // Mapping management
  const removeMapping = useCallback((columnId: string, isInput: boolean) => {
    if (isInput) {
      const wiresToRemove = wires.filter(w => w.sourceColumnId === columnId);
      wiresToRemove.forEach(wire => {
        colorManagerRef.current.releaseColor(wire.id);
      });
      setWires(prev => prev.filter(w => w.sourceColumnId !== columnId));
    } else {
      const wireToRemove = wires.find(w => w.targetColumnId === columnId);
      if (wireToRemove) {
        colorManagerRef.current.releaseColor(wireToRemove.id);
        setWires(prev => prev.filter(w => w.targetColumnId !== columnId));
      }
    }
    setHoveredWireId(null);
  }, [wires]);

  // Expression handling
  const openExpressionEditor = useCallback((column: ColumnDefinition) => {
    setExpressionEditorState({
      isOpen: true,
      column
    });
  }, []);

  const handleSaveExpression = useCallback((columnId: string, expression: string) => {
    setOutputTable(prev => ({
      ...prev,
      columns: prev.columns.map(col =>
        col.id === columnId ? { ...col, expression } : col
      )
    }));
  }, []);

  const updateOutputColumnPostgresType = useCallback((columnId: string, newPostgresType: string) => {
    const pgType = findPostgresTypeByValue(newPostgresType);
    if (!pgType) return;
    
    setOutputTable(prev => ({
      ...prev,
      columns: prev.columns.map(col =>
        col.id === columnId ? { 
          ...col, 
          type: pgType.dataType,
          postgresType: newPostgresType
        } : col
      )
    }));
  }, []);

  // Auto-map by name
  const handleAutoMap = useCallback(() => {
    const newWires: Wire[] = [...wires];
    
    outputTable.columns.forEach(outputCol => {
      const matchingInput = inputTable.columns.find(inputCol => 
        inputCol.name.toLowerCase() === outputCol.name.toLowerCase()
      );
      
      if (matchingInput) {
        const existingWireIndex = newWires.findIndex(w => 
          w.targetColumnId === outputCol.id && w.isActive
        );
        
        if (existingWireIndex >= 0) {
          const existingWire = newWires[existingWireIndex];
          colorManagerRef.current.releaseColor(existingWire.id);
          newWires.splice(existingWireIndex, 1);
        }
        
        const newWireId = generateId();
        const newColor = colorManagerRef.current.assignColor(newWireId);
        
        newWires.push({
          id: newWireId,
          sourceTableId: inputTable.id,
          sourceColumnId: matchingInput.id,
          targetTableId: outputTable.id,
          targetColumnId: outputCol.id,
          color: newColor,
          isActive: true,
          position: newWires.length
        });
      }
    });
    
    setWires(newWires);
  }, [inputTable, outputTable, wires]);

  // ==================== RENDER FUNCTIONS ====================

  const renderExpressionEditorModal = () => {
    if (!expressionEditorState.isOpen || !expressionEditorState.column) return null;
    
    return (
      <ExpressionEditorModal
        column={expressionEditorState.column}
        onSave={(expression) => handleSaveExpression(expressionEditorState.column!.id, expression)}
        onClose={() => setExpressionEditorState({ isOpen: false, column: null })}
      />
    );
  };

  const renderConnectionFeedback = () => {
    if (mappingMode !== 'selecting-target' || !activeSourceColumn) return null;
    
    const sourceColumn = inputTable.columns.find(c => c.id === activeSourceColumn);
    return (
      <div className="fixed top-4 left-1/2 transform -translate-x-1/2 z-50">
        <div className="px-4 py-3 bg-blue-100 border border-blue-300 rounded-lg shadow-lg flex items-center space-x-3">
          <div className="w-3 h-3 bg-blue-500 rounded-full animate-pulse"></div>
          <div className="text-sm text-blue-800 font-medium">
            Mapping: <span className="font-bold">{sourceColumn?.name}</span> → Select output column
          </div>
          <button
            onClick={cancelMappingMode}
            className="text-xs text-blue-600 hover:text-blue-800 px-2 py-1 bg-white rounded border border-blue-300"
          >
            Cancel
          </button>
        </div>
      </div>
    );
  };

  const renderInputTable = () => (
    <div className="border-r bg-white overflow-auto flex flex-col">
      <div className="bg-gradient-to-r from-green-100 to-green-200 px-3 py-2.5 font-semibold text-green-800 border-b border-green-300 sticky top-0 flex items-center justify-between">
        <div className="flex items-center">
          <span className="mr-2">📥</span>
          <span>{inputTable.name}</span>
        </div>
        <div className="flex items-center space-x-2">
          <span className="text-xs font-normal text-green-600 bg-white px-2 py-0.5 rounded">
            Input
          </span>
          <span className="text-xs text-gray-600 bg-green-50 px-1.5 py-0.5 rounded">
            {inputColumnCount} cols
          </span>
        </div>
      </div>
      <div className="overflow-y-auto flex-1">
        {inputTable.columns.map(col => {
          const connectionCount = wires.filter(wire => wire.sourceColumnId === col.id && wire.isActive).length;
          const isActiveSource = activeSourceColumn === col.id;
          const highlightColor = getColumnHighlightColor(col.id, true);
          const isHoverEmphasized = getColumnHoverEmphasis(col.id, true);
          
          return (
            <div 
              key={col.id}
              className={`px-3 py-2.5 border-b border-gray-100 transition-all duration-200 group flex items-center justify-between relative cursor-pointer ${
                isActiveSource ? 'border-l-4 border-l-blue-500' : ''
              }`}
              style={{
                backgroundColor: highlightColor || 'transparent',
                borderLeftWidth: isHoverEmphasized ? '4px' : undefined,
                borderLeftColor: isHoverEmphasized ? '#3b82f6' : undefined,
                opacity: isHoverEmphasized ? 0.9 : 1
              }}
              onClick={() => handleInputColumnClick(col.id)}
              onMouseEnter={() => handleInputColumnHover(col.id)}
              onMouseLeave={handleColumnHoverEnd}
              title={isActiveSource ? "Click again to cancel selection" : "Click to select for mapping"}
            >
              <div className="flex-1 min-w-0">
                <div className="flex items-center">
                  <span className={`text-gray-800 font-medium truncate ${highlightColor ? 'text-gray-900' : ''}`}>
                    {col.name}
                  </span>
                  <span className={`ml-2 text-xs font-mono px-1.5 py-0.5 rounded ${
                    highlightColor 
                      ? 'bg-white/70 text-gray-700' 
                      : 'bg-gray-100 text-gray-500'
                  }`}>
                    {col.type}
                  </span>
                  {isActiveSource && (
                    <span className="ml-2 text-xs bg-blue-500 text-white px-2 py-0.5 rounded animate-pulse">
                      Selected
                    </span>
                  )}
                </div>
                <div className={`text-xs mt-0.5 truncate ${
                  highlightColor ? 'text-gray-700' : 'text-gray-500'
                }`}>
                  {connectionCount} connection{connectionCount !== 1 ? 's' : ''}
                </div>
              </div>
              
              <div className="relative ml-2 flex items-center">
                {connectionCount > 0 && (
                  <>
                    <div className="w-2 h-2 bg-green-500 rounded-full mr-2"></div>
                    <button
                      className="text-xs text-red-500 hover:text-red-700 opacity-0 group-hover:opacity-100 transition-opacity"
                      onClick={(e) => {
                        e.stopPropagation();
                        removeMapping(col.id, true);
                      }}
                      title="Remove all mappings from this column"
                    >
                      ✕
                    </button>
                  </>
                )}
              </div>
              
              {col.isKey && (
                <span className="ml-2 text-xs bg-yellow-100 text-yellow-800 px-2 py-0.5 rounded whitespace-nowrap">
                  Key
                </span>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );

  const renderOutputTable = () => (
    <div className="bg-white overflow-auto flex flex-col">
      <div className="bg-gradient-to-r from-yellow-100 to-yellow-200 px-3 py-2.5 font-semibold text-yellow-800 border-b border-yellow-300 sticky top-0 flex items-center justify-between">
        <div className="flex items-center">
          <span className="mr-2">📤</span>
          <span>{outputTable.name}</span>
        </div>
        <div className="flex items-center space-x-2">
          <span className="text-xs font-normal text-yellow-600 bg-white px-2 py-0.5 rounded">
            Output
          </span>
          <span className="text-xs text-gray-600 bg-yellow-50 px-1.5 py-0.5 rounded">
            {outputColumnCount} cols
          </span>
          <div className={`text-xs px-1.5 py-0.5 rounded ${
            mappingPercentage === 100 
              ? 'bg-green-100 text-green-800' 
              : mappingPercentage > 50 
                ? 'bg-blue-100 text-blue-800' 
                : 'bg-red-100 text-red-800'
          }`}>
            {mappingPercentage}% mapped
          </div>
        </div>
      </div>
      <div className="overflow-y-auto flex-1">
        {outputTable.columns.map(col => {
          const columnWires = wires.filter(wire => wire.targetColumnId === col.id && wire.isActive);
          const connectionCount = columnWires.length;
          const isConnected = connectionCount > 0;
          const isSelectable = mappingMode === 'selecting-target';
          const highlightColor = getColumnHighlightColor(col.id, false);
          const isHoverEmphasized = getColumnHoverEmphasis(col.id, false);
          
          return (
            <div
              key={col.id}
              className={`border-b border-gray-100 cursor-pointer transition-all duration-200 flex items-center justify-between relative ${
                selectedColumnId === col.id 
                  ? "border-l-4 border-l-blue-500" 
                  : isSelectable
                    ? "border-l-2 border-l-yellow-300"
                    : ""
              }`}
              style={{
                backgroundColor: highlightColor || 'transparent',
                borderLeftWidth: isHoverEmphasized ? '4px' : undefined,
                borderLeftColor: isHoverEmphasized ? '#3b82f6' : undefined,
                opacity: isHoverEmphasized ? 0.9 : 1
              }}
              onClick={() => handleOutputColumnClick(col.id)}
              onMouseEnter={() => handleOutputColumnHover(col.id)}
              onMouseLeave={handleColumnHoverEnd}
              title={isSelectable ? 
                "Click to map from selected input column" : 
                isConnected ? 
                "Connected - click to edit expression" : 
                "Click to select for expression editing"}
            >
              <div className="flex-1 min-w-0 px-3 py-2.5">
                <div className="flex items-center justify-between mb-1">
                  <div className="flex items-center">
                    <span className={`font-medium truncate ${highlightColor ? 'text-gray-900' : 'text-gray-800'}`}>
                      {col.name}
                    </span>
                    <span className={`ml-2 text-xs font-mono px-1.5 py-0.5 rounded ${
                      highlightColor 
                        ? 'bg-white/70 text-gray-700' 
                        : 'bg-gray-100 text-gray-500'
                    }`}>
                      {col.type}
                    </span>
                    {isSelectable && (
                      <span className="ml-2 text-xs bg-yellow-100 text-yellow-800 px-2 py-0.5 rounded animate-pulse">
                        Ready for mapping
                      </span>
                    )}
                  </div>
                  {isConnected ? (
                    <div className="flex items-center">
                      <span className={`text-xs px-2 py-0.5 rounded whitespace-nowrap mr-2 ${
                        highlightColor 
                          ? 'bg-white/70 text-gray-800 border border-gray-300' 
                          : 'bg-green-100 text-green-800'
                      }`}>
                        {connectionCount} source{connectionCount !== 1 ? 's' : ''}
                      </span>
                      <button
                        className="text-xs text-red-500 hover:text-red-700"
                        onClick={(e) => {
                          e.stopPropagation();
                          removeMapping(col.id, false);
                        }}
                        title="Remove mapping"
                      >
                        ✕
                      </button>
                    </div>
                  ) : (
                    <span className="text-xs bg-red-100 text-red-800 px-2 py-0.5 rounded whitespace-nowrap">
                      ⚠️ Unmapped
                    </span>
                  )}
                </div>
                <div className="text-xs font-mono truncate mt-1">
                  {isConnected ? (
                    <div className="flex items-center flex-wrap gap-1">
                      {columnWires.slice(0, 2).map(wire => {
                        const sourceColumn = inputTable.columns.find(c => c.id === wire.sourceColumnId);
                        return sourceColumn ? (
                          <span 
                            key={wire.id} 
                            className={`text-xs px-1.5 py-0.5 rounded ${
                              highlightColor 
                                ? 'bg-white/70 text-gray-700 border border-gray-300' 
                                : 'text-green-600 bg-green-50'
                            }`}
                          >
                            ← {sourceColumn.name}
                          </span>
                        ) : null;
                      })}
                      {connectionCount > 2 && (
                        <span className="text-xs text-gray-500 bg-gray-100 px-1.5 py-0.5 rounded">
                          +{connectionCount - 2} more
                        </span>
                      )}
                    </div>
                  ) : (
                    <span className="text-gray-400 italic">No connections</span>
                  )}
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );

  const renderSchemaComparison = () => (
    <div className="grid grid-cols-2 gap-6 h-full">
      {/* Input Schema */}
      <div className="h-full flex flex-col border rounded-lg overflow-hidden">
        <div className="bg-gradient-to-r from-green-50 to-green-100 px-4 py-3 border-b flex items-center justify-between">
          <h3 className="font-semibold text-gray-800 text-sm flex items-center">
            <span className="mr-2">📥</span>
            Input Schema: {inputTable.name}
          </h3>
          <div className="text-xs text-gray-500 bg-white px-2 py-1 rounded border">
            {inputColumnCount} columns
          </div>
        </div>
        <div className="overflow-y-auto flex-1">
          <table className="w-full border-collapse">
            <thead className="sticky top-0 bg-white">
              <tr className="bg-gray-50">
                <th className="border px-3 py-2.5 text-left text-xs font-semibold text-gray-700 w-12">#</th>
                <th className="border px-3 py-2.5 text-left text-xs font-semibold text-gray-700">Column Name</th>
                <th className="border px-3 py-2.5 text-left text-xs font-semibold text-gray-700">Data Type</th>
                <th className="border px-3 py-2.5 text-left text-xs font-semibold text-gray-700 w-20">Key</th>
                <th className="border px-3 py-2.5 text-left text-xs font-semibold text-gray-700 w-24">Connections</th>
                <th className="border px-3 py-2.5 text-left text-xs font-semibold text-gray-700 w-24">Action</th>
              </tr>
            </thead>
            <tbody>
              {inputTable.columns.map((col, index) => {
                const columnWires = wires.filter(wire => wire.sourceColumnId === col.id && wire.isActive);
                const isActiveSource = activeSourceColumn === col.id;
                const highlightColor = getColumnHighlightColor(col.id, true);
                const isHoverEmphasized = getColumnHoverEmphasis(col.id, true);
                
                return (
                  <tr 
                    key={col.id} 
                    className={`hover:bg-gray-50 transition-colors duration-200 border-b ${
                      isActiveSource ? 'bg-blue-50' : ''
                    }`}
                    style={{
                      backgroundColor: highlightColor || undefined,
                      borderLeft: isHoverEmphasized ? '4px solid #3b82f6' : undefined
                    }}
                    onMouseEnter={() => handleInputColumnHover(col.id)}
                    onMouseLeave={handleColumnHoverEnd}
                  >
                    <td className="border px-3 py-2.5 text-xs text-gray-500 text-center">{index + 1}</td>
                    <td className="border px-3 py-2.5">
                      <div className={`font-medium ${highlightColor ? 'text-gray-900' : 'text-gray-800'}`}>
                        {col.name}
                      </div>
                    </td>
                    <td className="border px-3 py-2.5">
                      <span className={`text-xs font-mono px-2 py-1 rounded ${
                        highlightColor 
                          ? 'bg-white/70 text-gray-700 border border-gray-300' 
                          : 'bg-blue-50 text-blue-700'
                      }`}>
                        {col.type}
                      </span>
                    </td>
                    <td className="border px-3 py-2.5 text-center">
                      {col.isKey ? (
                        <span className="text-xs bg-yellow-100 text-yellow-800 px-2 py-1 rounded">✓ Key</span>
                      ) : (
                        <span className="text-xs text-gray-400">—</span>
                      )}
                    </td>
                    <td className="border px-3 py-2.5 text-center">
                      {columnWires.length > 0 ? (
                        <span className={`text-xs px-2 py-1 rounded ${
                          highlightColor
                            ? 'bg-white/70 text-gray-800 border border-gray-300'
                            : 'bg-green-100 text-green-800'
                        }`}>
                          {columnWires.length} target{columnWires.length !== 1 ? 's' : ''}
                        </span>
                      ) : (
                        <span className="text-xs text-gray-400">—</span>
                      )}
                    </td>
                    <td className="border px-3 py-2.5 text-center">
                      <button
                        className={`text-xs px-3 py-1 rounded transition-colors ${
                          isActiveSource
                            ? 'bg-blue-100 text-blue-700 border border-blue-300'
                            : 'bg-green-100 text-green-700 hover:bg-green-200 border border-green-300'
                        }`}
                        onClick={() => handleInputColumnClick(col.id)}
                      >
                        {isActiveSource ? 'Selected' : 'Select'}
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
      
      {/* Output Schema */}
      <div className="h-full flex flex-col border rounded-lg overflow-hidden">
        <div className="bg-gradient-to-r from-yellow-50 to-yellow-100 px-4 py-3 border-b flex items-center justify-between">
          <h3 className="font-semibold text-gray-800 text-sm flex items-center">
            <span className="mr-2">📤</span>
            Output Schema: {outputTable.name}
          </h3>
          <div className="text-xs text-gray-500 bg-white px-2 py-1 rounded border">
            {outputColumnCount} columns ({mappedColumnCount} mapped)
          </div>
        </div>
        <div className="overflow-y-auto flex-1">
          <table className="w-full border-collapse">
            <thead className="sticky top-0 bg-white">
              <tr className="bg-gray-50">
                <th className="border px-3 py-2.5 text-left text-xs font-semibold text-gray-700 w-12">#</th>
                <th className="border px-3 py-2.5 text-left text-xs font-semibold text-gray-700">Column Name</th>
                <th className="border px-3 py-2.5 text-left text-xs font-semibold text-gray-700">Data Type</th>
                <th className="border px-3 py-2.5 text-left text-xs font-semibold text-gray-700">Expression</th>
                <th className="border px-3 py-2.5 text-left text-xs font-semibold text-gray-700 w-24">Status</th>
                <th className="border px-3 py-2.5 text-left text-xs font-semibold text-gray-700 w-32">Actions</th>
              </tr>
            </thead>
            <tbody>
              {outputTable.columns.map((col, index) => {
                const columnWires = wires.filter(w => w.targetColumnId === col.id && w.isActive);
                const highlightColor = getColumnHighlightColor(col.id, false);
                const isHoverEmphasized = getColumnHoverEmphasis(col.id, false);
                const currentPostgresType = col.postgresType || mapDataTypeToDefaultPostgresType(col.type);
                
                return (
                  <tr 
                    key={col.id} 
                    className={`hover:bg-gray-50 transition-colors duration-200 border-b cursor-pointer ${
                      selectedColumnId === col.id ? 'bg-blue-50' : ''
                    }`}
                    style={{
                      backgroundColor: highlightColor || undefined,
                      borderLeft: isHoverEmphasized ? '4px solid #3b82f6' : undefined
                    }}
                    onDoubleClick={() => openExpressionEditor(col)}
                    onMouseEnter={() => handleOutputColumnHover(col.id)}
                    onMouseLeave={handleColumnHoverEnd}
                    title="Double-click to edit expression"
                  >
                    <td className="border px-3 py-2.5 text-xs text-gray-500 text-center">{index + 1}</td>
                    <td className="border px-3 py-2.5">
                      <div className={`font-medium ${highlightColor ? 'text-gray-900' : 'text-gray-800'}`}>
                        {col.name}
                      </div>
                    </td>
                    <td className="border px-3 py-2.5">
                      <select
                        value={currentPostgresType}
                        onChange={(e) => updateOutputColumnPostgresType(col.id, e.target.value)}
                        className={`text-xs font-mono px-2 py-1 rounded border focus:outline-none focus:ring-2 focus:ring-blue-300 focus:border-blue-300 w-full max-w-[180px] ${
                          highlightColor
                            ? 'bg-white/70 text-gray-700 border-gray-300'
                            : 'bg-blue-50 text-blue-700 border-blue-200 hover:bg-blue-100'
                        }`}
                        onClick={(e) => e.stopPropagation()}
                        title="Select PostgreSQL data type"
                      >
                        {POSTGRES_DATA_TYPES.map((type) => (
                          <option key={type.value} value={type.value}>
                            {type.label}
                          </option>
                        ))}
                      </select>
                    </td>
                    <td className="border px-3 py-2.5">
                      <div 
                        className="text-xs font-mono text-gray-600 truncate max-w-xs cursor-text hover:bg-gray-50 px-2 py-1 rounded"
                        onClick={(e) => {
                          e.stopPropagation();
                          openExpressionEditor(col);
                        }}
                        title="Click to edit expression"
                      >
                        {col.expression ? (
                          <span className="text-green-600">{col.expression}</span>
                        ) : (
                          <span className="text-gray-400 italic">Not defined</span>
                        )}
                      </div>
                    </td>
                    <td className="border px-3 py-2.5 text-center">
                      {columnWires.length > 0 ? (
                        <span className={`text-xs px-2 py-1 rounded ${
                          highlightColor
                            ? 'bg-white/70 text-gray-800 border border-gray-300'
                            : 'bg-green-100 text-green-800'
                        }`}>
                          {columnWires.length} source{columnWires.length !== 1 ? 's' : ''}
                        </span>
                      ) : (
                        <span className="text-xs bg-red-100 text-red-800 px-2 py-1 rounded">Unmapped</span>
                      )}
                    </td>
                    <td className="border px-3 py-2.5">
                      <div className="flex gap-1">
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            openExpressionEditor(col);
                          }}
                          className="text-xs px-2 py-1 bg-blue-100 text-blue-700 rounded border border-blue-300 hover:bg-blue-200 transition-colors"
                          title="Edit expression"
                        >
                          Edit
                        </button>
                        {col.expression && (
                          <button
                            onClick={(e) => {
                              e.stopPropagation();
                              handleSaveExpression(col.id, '');
                            }}
                            className="text-xs px-2 py-1 bg-red-100 text-red-700 rounded border border-red-300 hover:bg-red-200 transition-colors"
                            title="Clear expression"
                          >
                            Clear
                          </button>
                        )}
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );

  // ==================== MAIN RENDER ====================

  return (
    <>
      {renderExpressionEditorModal()}
      {renderConnectionFeedback()}

      {/* Main MapEditor Container */}
      <div 
        className="fixed inset-0 z-[9999] bg-gray-900/80 flex flex-col"
        ref={resizeContainerRef}
      >
        {/* Toolbar */}
        <div className="flex items-center justify-between px-4 py-3 border-b bg-gradient-to-r from-blue-50 to-gray-50 sticky top-0 z-20">
          <div className="flex items-center space-x-3">
            <div className="text-lg font-bold text-blue-700 flex items-center">
              <span className="mr-2">🗺️</span>
              Map Editor
              <span className="ml-2 text-xs bg-green-100 text-green-800 px-2 py-0.5 rounded">
                Compiler Metadata v1.0
              </span>
            </div>
            <div className="text-xs text-gray-600 bg-white px-2 py-1 rounded border">
              Node: {nodeId || 'Unknown'}
            </div>
            <div className="text-xs text-gray-600 bg-white px-2 py-1 rounded border">
              Transformations: {generateTransformations().length}
            </div>
            <div className="text-xs text-gray-600 bg-white px-2 py-1 rounded border">
              Wires: {wires.filter(w => w.isActive).length}
            </div>
            
            {mappingMode === 'selecting-target' && activeSourceColumn && (
              <div className="flex items-center bg-blue-100 border border-blue-300 px-3 py-1 rounded">
                <div className="animate-pulse w-2 h-2 bg-blue-600 rounded-full mr-2"></div>
                <span className="text-xs text-blue-800 font-medium">
                  Click an output column to map
                </span>
                <button 
                  className="ml-2 text-xs text-blue-700 hover:text-blue-900"
                  onClick={cancelMappingMode}
                >
                  ✕ Cancel
                </button>
              </div>
            )}
          </div>
          
          <div className="flex items-center gap-3">
            <div className="flex items-center bg-white border border-gray-300 rounded px-2 py-1">
              <div className="flex items-center mr-3">
                <div className="w-2 h-2 rounded-full bg-green-500 mr-1.5"></div>
                <span className="text-xs text-gray-700">Mapped: {mappedColumnCount}/{outputColumnCount} ({mappingPercentage}%)</span>
              </div>
              <div className="h-4 w-px bg-gray-300 mx-2"></div>
              <div className="flex items-center">
                <div className="w-2 h-2 rounded-full bg-blue-500 mr-1.5"></div>
                <span className="text-xs text-gray-700">Input: {inputColumnCount} columns</span>
              </div>
            </div>
            
            <button 
              className="px-4 py-2 bg-gradient-to-r from-blue-500 to-blue-600 text-white text-sm rounded hover:from-blue-600 hover:to-blue-700 transition-all shadow-sm hover:shadow"
              onClick={handleAutoMap}
            >
              Auto Map
            </button>
            
            <div className="h-6 w-px bg-gray-300 mx-1"></div>
            
            <button
              className="px-4 py-2 text-sm border border-gray-300 rounded hover:bg-gray-50 transition-colors"
              onClick={onClose}
            >
              Cancel
            </button>
            <button
              className="px-4 py-2 text-sm bg-gradient-to-r from-green-500 to-green-600 text-white rounded hover:from-green-600 hover:to-green-700 transition-all shadow-sm hover:shadow"
              onClick={handleSave}
            >
              Save & Compile
            </button>
          </div>
        </div>

        {/* Main mapping area */}
        <div 
          className="grid grid-cols-3 flex-1 overflow-hidden min-h-0 relative"
          style={{ 
            height: `calc(100% - 56px - ${schemaPanelHeight}px - 28px)`,
            transition: isResizing ? 'none' : 'height 0.2s ease'
          }}
        >
          {renderInputTable()}
          
          {/* Variables Panel */}
          <div className="border-r bg-white overflow-auto flex flex-col">
            <div className="bg-gradient-to-r from-gray-100 to-gray-200 px-3 py-2.5 font-semibold text-gray-800 border-b border-gray-300 sticky top-0">
              <div className="flex items-center justify-between">
                <div className="flex items-center">
                  <span className="mr-2">📊</span>
                  <span>Variables & Functions</span>
                </div>
                <div className="flex items-center space-x-1">
                  <button 
                    className="text-xs bg-blue-100 text-blue-700 px-2 py-0.5 rounded hover:bg-blue-200 transition-colors"
                    onClick={() => {
                      const newVar: Variable = {
                        id: generateId('var'),
                        name: `var_${variables.length + 1}`,
                        expression: '',
                        type: 'STRING'
                      };
                      setVariables([...variables, newVar]);
                    }}
                    title="Add variable"
                  >
                    + Var
                  </button>
                  <button 
                    className="text-xs bg-purple-100 text-purple-700 px-2 py-0.5 rounded hover:bg-purple-200 transition-colors"
                    onClick={() => console.log("Add function")}
                    title="Add function"
                  >
                    + Func
                  </button>
                </div>
              </div>
            </div>
            <div className="overflow-y-auto flex-1 p-3">
              {variables.length === 0 ? (
                <div className="text-center text-gray-500 py-6 h-full flex flex-col items-center justify-center">
                  <div className="text-3xl mb-3">📝</div>
                  <p className="text-sm font-medium mb-1">No variables defined</p>
                  <p className="text-xs text-gray-400 mb-3 max-w-xs">
                    Create variables for reusable expressions or add custom functions
                  </p>
                </div>
              ) : (
                <div className="space-y-2">
                  <div className="text-xs font-medium text-gray-700 mb-2">Variables</div>
                  {variables.map(variable => (
                    <div key={variable.id} className="p-2 border border-gray-200 rounded hover:bg-gray-50">
                      <div className="font-medium text-gray-800 text-sm truncate">{variable.name}</div>
                      <div className="text-xs text-gray-600 font-mono truncate mt-1">{variable.expression}</div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
          
          {renderOutputTable()}
        </div>

        {/* Resize Handle */}
        <div
          className={`h-3 flex items-center justify-center cursor-row-resize transition-colors ${
            isResizing 
              ? 'bg-blue-200 border-t border-b border-blue-300' 
              : 'bg-gray-100 hover:bg-gray-200 hover:border-t hover:border-b hover:border-gray-300'
          }`}
          onMouseDown={handleResizeStart}
          title="Drag to resize schema panel"
        >
          <div className="flex items-center space-x-1">
            <div className={`w-16 h-0.5 rounded-full ${
              isResizing ? 'bg-blue-500' : 'bg-gray-400'
            }`}></div>
            <div className={`text-xs ${
              isResizing ? 'text-blue-600 font-medium' : 'text-gray-500'
            }`}>
              {Math.round(schemaPanelHeight)}px
            </div>
            <div className={`w-16 h-0.5 rounded-full ${
              isResizing ? 'bg-blue-500' : 'bg-gray-400'
            }`}></div>
          </div>
        </div>

        {/* Bottom panel - Schema Editor */}
        <div 
          className="border-t bg-white flex flex-col shadow-inner"
          style={{ 
            height: `${schemaPanelHeight}px`,
            transition: isResizing ? 'none' : 'height 0.2s ease',
            minHeight: '150px'
          }}
        >
          <div className="flex border-b bg-gradient-to-r from-gray-50 to-gray-100">
            <div className="px-5 py-3 text-sm font-medium bg-white border-t-2 border-t-blue-500 text-blue-700 shadow-sm">
              <div className="flex items-center">
                <span className="mr-2">📋</span>
                Schema Comparison
              </div>
            </div>
            
            <div className="ml-auto flex items-center px-4">
              <div className="text-xs text-gray-500 flex items-center">
                <span className="mr-2">🎨</span>
                <span>
                  Same-color rows are mapped together
                </span>
              </div>
              <button
                onClick={() => setSchemaPanelHeight(256)}
                className="ml-3 text-xs text-gray-500 hover:text-gray-700 bg-gray-100 hover:bg-gray-200 px-2 py-1 rounded transition-colors"
                title="Reset to default height"
              >
                Reset Height
              </button>
            </div>
          </div>

          <div className="flex-1 overflow-auto p-4">
            {renderSchemaComparison()}
          </div>
        </div>

        {/* Status bar */}
        <div className="flex items-center justify-between px-4 py-2 border-t bg-gray-50 text-xs text-gray-600">
          <div className="flex items-center space-x-4">
            <div className="flex items-center">
              <div className="w-2 h-2 rounded-full bg-green-500 mr-1.5"></div>
              <span>
                {mappingMode === 'selecting-target' ? 'Mapping mode active' : 
                 hoveredWireId ? 'Hovering mapping' : 
                 expressionEditorState.isOpen ? 'Editing expression' : 'Ready'}
              </span>
            </div>
            <div className="flex items-center space-x-3">
              <div>
                <span className="text-gray-500">Transformations:</span>
                <span className="font-medium ml-1">{generateTransformations().length}</span>
              </div>
              <div>
                <span className="text-gray-500">Dependencies:</span>
                <span className="font-medium ml-1">
                  {Object.keys(computeColumnDependenciesFromTransformations(generateTransformations())).length}
                </span>
              </div>
              <div>
                <span className="text-gray-500">Validation:</span>
                <span className={`font-medium ml-1 ${
                  validateTransformations(generateTransformations()).isValid 
                    ? 'text-green-600' 
                    : 'text-yellow-600'
                }`}>
                  {validateTransformations(generateTransformations()).isValid ? 'Valid' : 'Warnings'}
                </span>
              </div>
            </div>
          </div>
          <div className="flex items-center space-x-3">
            <div className="text-gray-500">Shortcuts:</div>
            <div className="flex items-center space-x-1">
              <kbd className="px-1.5 py-0.5 bg-white border rounded text-xs">Esc</kbd>
              <span className="text-gray-500">Cancel</span>
            </div>
            <div className="flex items-center space-x-1">
              <kbd className="px-1.5 py-0.5 bg-white border rounded text-xs">Ctrl+S</kbd>
              <span className="text-gray-500">Save</span>
            </div>
            <div className="flex items-center space-x-1">
              <kbd className="px-1.5 py-0.5 bg-white border rounded text-xs">Double-click</kbd>
              <span className="text-gray-500">Edit expression</span>
            </div>
          </div>
        </div>
      </div>

      {isResizing && (
        <div className="fixed inset-0 z-[10000] cursor-row-resize"></div>
      )}
    </>
  );
};

export default MapEditor;