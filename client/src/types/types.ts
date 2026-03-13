// types.ts
// Base types
export interface RepositoryNode {
  id: string;
  name: string;
  type: 'category' | 'folder' | 'item' | 'job'| 'canvas'; 
  icon?: React.ReactNode;
  children?: RepositoryNode[];
  metadata?: RepositoryNodeMetadata;
  jobDesignId?: string; // NEW: Reference to job design
  draggable?: boolean;
  droppable?: boolean;
  parentId?: string;
  description?: string; // Added for job design info display
}

export interface JobConfig {
  id: string;
  name: string;
  createdAt: string;
  lastModified: string;
  state: 'draft' | 'running' | 'completed' | 'failed';
  nodes?: any[];
  connections?: any[];
  variables?: any[];
  isFavorite?: boolean;
  isPinned?: boolean;
  description?: string;
  tags?: string[];
}

// NEW: Job Design State for canvas persistence
export interface JobDesignState {
  jobId?: string;                    // Reference to the job ID
  name?: string;                     // Job name
  nodes: any[];                      // Canvas nodes
  edges: any[];                      // Canvas edges
  connections: any[];                // Data connections
  viewport: {                       // Canvas viewport state
    x: number;
    y: number;
    zoom: number;
  };
  validationSummary: any | null;     // Validation results
  sqlGeneration: Record<string, any>; // Generated SQL
  lastModified: string;              // Last modified timestamp
  createdAt: string;                 // Creation timestamp
  version: string;                   // Version identifier
}

// Job Design Metadata - MOVED UP so it's defined before RepositoryNodeMetadata
export interface JobDesignMetadata extends CommonMetadata {
  type: 'job-design';
  jobId: string;
  jobDesignId?: string; // NEW: Link to JobDesignState
  createdAt: string;
  lastModified: string;
}

// Union type for all possible metadata
export type RepositoryNodeMetadata = 
  | CommonMetadata
  | ExcelMetadata
  | XMLMetadata
  | DelimitedMetadata
  | XMLAttributeMetadata
  | XMLElementMetadata
  | ColumnMetadata
  | PositionalFieldMetadata
  | FileSchemaMetadata
  | SchemaFieldMetadata
  | JsonAvroParquetMetadata
  | JsonFieldMetadata
  | AvroFieldMetadata
  | ParquetColumnMetadata
  | LDIFMetadata
  | LDIFEntryMetadata
  | LDIFAttributeMetadata
  | WebServiceMetadata
  | WebServiceEndpointMetadata
  | WebServiceParameterMetadata
  | LDAPMetadata
  | LDAPConnectionMetadata
  | LDAPSchemaMetadata
  | LDAPObjectClassMetadata
  | DatabaseMetadata
  | DatabaseTableMetadataWizard
  | DatabaseColumnMetadataWizard
  | FileRegexMetadata
  | RegexPatternMetadata
  | (JobDesignMetadata & { postgresTableName?: string });

// Common metadata properties shared by all nodes
export interface CommonMetadata {
  description?: string;
  lastModified?: string;
  size?: string;
  purpose?: string;
  filePath?: string;
  type?: string; // Added: makes type property available on all metadata
  postgresTableName?: string; // Added: makes postgresTableName available on all metadata
  connection?: {
    connectionId?: string;
    [key: string]: any;
  };
  [key: string]: any; // Added: allows additional properties
}

// Add Positional Metadata types
export interface PositionalMetadata extends CommonMetadata {
  type: 'positional';
  encoding: string;
  recordLength: number;
  schema: PositionalColumnDefinition[];
  sampleData: string[];
  hasHeaders: boolean;
}

export interface PositionalFieldMetadata extends CommonMetadata {
  type: 'positional-field';
  dataType: string;
  start: number;
  length: number;
  endPosition: number;
  description: string;
}

// Excel specific metadata
export interface ExcelMetadata extends CommonMetadata {
  type: 'excel';
  selectedSheet?: string;
  hasHeaders?: boolean;
  schema?: Array<{ name: string; type: string; length?: number }>;
}

// XML specific metadata
export interface XMLMetadata extends CommonMetadata {
  type: 'xml';
  rootElement?: string;
  schemaType?: string;
  totalElements?: number;
  totalAttributes?: number;
  structure?: any[];
}

// XML Element metadata (for individual elements)
export interface XMLElementMetadata extends CommonMetadata {
  type: 'xml-element';
  elementType: string;
  path: string;
  level: number;
  hasChildren: boolean;
  hasAttributes: boolean;
  sampleData?: string;
}

// XML Attribute metadata
export interface XMLAttributeMetadata extends CommonMetadata {
  type: 'xml-attribute';
  attributeOf: string;
  value: string;
  inferredType: string;
}

// Delimited file metadata
export interface DelimitedMetadata extends CommonMetadata {
  type: 'delimited';
  delimiter?: string;
  hasHeaders?: boolean;
  textQualifier?: string;
  encoding?: string;
  schema?: Array<{ name: string; type: string; length?: number; sampleValues: string[] }>;
  sampleData?: string[][];
}

// Column metadata (for Excel and Delimited columns)
export interface ColumnMetadata extends CommonMetadata {
  type: 'column';
  dataType?: string;
  length?: number;
  sampleValues?: string[];
}

// Excel Metadata Wizard Types
export interface ExcelMetadataFormData {
  name: string;
  purpose: string;
  description: string;
  file: File | null;
  filePath: string;
  readExcel2007: boolean;
  selectedSheet: string;
  hasHeaders: boolean;
  schema: Array<{
    nullable: boolean; name: string; type: string; length?: number 
  }>;
}

export interface ExcelMetadataWizardProps {
  isOpen: boolean;
  onClose: () => void;
  onSave: (metadata: ExcelMetadataFormData) => void;
}

export interface SheetData {
  name: string;
  headers: string[];
  data: any[][];
  rowCount: number;
  columnCount: number;
}

// XML Metadata Wizard Types
export interface XMLElement {
  name: string;
  type: string;
  path: string;
  level: number;
  hasChildren: boolean;
  hasAttributes: boolean;
  sampleData?: string;
}

export interface XMLMetadataFormData {
  name: string;
  purpose: string;
  description: string;
  file: File | null;
  filePath: string;
  rootElement: string;
  namespace: string;
  schemaType: 'inferred' | 'custom';
  elements: XMLElement[];
  attributes: Array<{
    name: string;
    value: string;
    element: string;
  }>;
  structure: any[];
}

export interface XMLMetadataWizardProps {
  isOpen: boolean;
  onClose: () => void;
  onSave: (metadata: XMLMetadataFormData) => void;
}

// Delimited File Metadata Wizard Types
export interface DelimitedFileMetadataFormData {
  name: string;
  purpose: string;
  description: string;
  file: File | null;
  filePath: string;
  delimiter: string;
  hasHeaders: boolean;
  textQualifier: string;
  encoding: string;
  schema: Array<{
    nullable: boolean; 
    name: string; 
    type: string; 
    length?: number;
    sampleValues: string[];
  }>;
  sampleData: string[][];
}

// Positional File Metadata Types
export interface PositionalColumnDefinition {
  name: string;
  type: string;
  start: number;  // 1-based position
  length: number;
  description?: string;
}

export interface PositionalFileMetadataFormData {
  name: string;
  purpose: string;
  description: string;
  file: File | null;
  filePath: string;
  encoding: string;
  schema: PositionalColumnDefinition[];
  sampleData: string[];
  recordLength: number;
  hasHeaders: boolean;
}

export interface PositionalFileMetadataWizardProps {
  isOpen: boolean;
  onClose: () => void;
  onSave: (metadata: PositionalFileMetadataFormData) => void;
}

export interface DelimitedFileMetadataWizardProps {
  isOpen: boolean;
  onClose: () => void;
  onSave: (metadata: DelimitedFileMetadataFormData) => void;
}

// File Schema Metadata Types
export interface SchemaFieldDefinition {
  name: string;
  type: string;
  length?: number;
  precision?: number;
  scale?: number;
  nullable: boolean;
  description?: string;
  defaultValue?: string;
  constraints?: string[];
}

export interface FileSchemaMetadataFormData {
  name: string;
  purpose: string;
  description: string;
  file: File | null;
  filePath: string;
  schemaType: 'database' | 'json' | 'avro' | 'protobuf' | 'custom';
  fields: SchemaFieldDefinition[];
  totalFields: number;
  version: string;
  encoding: string;
}

export interface FileSchemaMetadataWizardProps {
  isOpen: boolean;
  onClose: () => void;
  onSave: (metadata: FileSchemaMetadataFormData) => void;
}

export interface FileSchemaMetadata extends CommonMetadata {
  type: 'file-schema';
  schemaType: string;
  fields: SchemaFieldDefinition[];
  totalFields: number;
  version: string;
  encoding: string;
}

export interface SchemaFieldMetadata extends CommonMetadata {
  type: 'schema-field';
  dataType: string;
  length?: number;
  precision?: number;
  scale?: number;
  nullable: boolean;
  defaultValue?: string;
  constraints?: string[];
}

// JSON/AVRO/Parquet Types
export interface JsonFieldDefinition {
  name: string;
  type: string;
  path: string;
  level: number;
  sampleValue?: string;
  description?: string;
  nullable: boolean;
}

export interface AvroFieldDefinition {
  name: string;
  type: string | any;
  path: string;
  level: number;
  sampleValue?: string;
  description?: string;
  nullable: boolean;
  logicalType?: string;
}

export interface ParquetColumnDefinition {
  name: string;
  type: string;
  path: string;
  level: number;
  sampleValue?: string;
  description?: string;
  nullable: boolean;
  compression?: string;
}

export interface JsonAvroParquetMetadataFormData {
  name: string;
  purpose: string;
  description: string;
  file: File | null;
  filePath: string;
  format: 'json' | 'avro' | 'parquet';
  schema: JsonFieldDefinition[] | AvroFieldDefinition[] | ParquetColumnDefinition[];
  totalFields: number;
  recordCount: number;
  encoding: string;
  compression: string;
  sampleData: any[];
}

export interface JsonAvroParquetMetadataWizardProps {
  isOpen: boolean;
  onClose: () => void;
  onSave: (metadata: JsonAvroParquetMetadataFormData) => void;
}

export interface JsonAvroParquetMetadata extends CommonMetadata {
  type: 'json-avro-parquet';
  format: string;
  schema: JsonFieldDefinition[] | AvroFieldDefinition[] | ParquetColumnDefinition[];
  totalFields: number;
  recordCount: number;
  encoding: string;
  compression: string;
  sampleData: any[];
}

export interface JsonFieldMetadata extends CommonMetadata {
  type: 'json-field';
  dataType: string;
  path: string;
  level: number;
  sampleValue?: string;
  nullable: boolean;
}

export interface AvroFieldMetadata extends CommonMetadata {
  type: 'avro-field';
  dataType: string;
  path: string;
  level: number;
  sampleValue?: string;
  nullable: boolean;
  logicalType?: string;
}

export interface ParquetColumnMetadata extends CommonMetadata {
  type: 'parquet-column';
  dataType: string;
  path: string;
  level: number;
  sampleValue?: string;
  nullable: boolean;
  compression?: string;
}

// File Regex Metadata Types
export interface RegexPatternDefinition {
  id: string;
  name: string;
  pattern: string;
  description: string;
  flags: string;
  sampleMatches: string[];
  matchCount: number;
  fieldType: string;
  validationRules?: string[];
}

export interface RegexTestResult {
  patternId: string;
  matches: Array<{
    match: string;
    index: number;
    groups: string[];
  }>;
  totalMatches: number;
}

export interface FileRegexMetadataFormData {
  name: string;
  purpose: string;
  description: string;
  file: File | null;
  filePath: string;
  encoding: string;
  patterns: RegexPatternDefinition[];
  sampleData: string[];
  totalPatterns: number;
  validationMode: 'strict' | 'lenient' | 'custom';
  caseSensitive: boolean;
  multiline: boolean;
  global: boolean;
}

export interface FileRegexMetadataWizardProps {
  isOpen: boolean;
  onClose: () => void;
  onSave: (data: FileRegexMetadataFormData) => void;
}

export interface FileRegexMetadata extends CommonMetadata {
  type: 'file-regex';
  patterns: RegexPatternDefinition[];
  totalPatterns: number;
  encoding: string;
  validationMode: string;
  caseSensitive: boolean;
  multiline: boolean;
  global: boolean;
  sampleData: string[];
}

export interface RegexPatternMetadata extends CommonMetadata {
  type: 'regex-pattern';
  pattern: string;
  flags: string;
  fieldType: string;
  sampleMatches: string[];
  matchCount: number;
  validationRules?: string[];
}

export interface LDIFMetadata extends CommonMetadata {
  type: 'ldif';
  totalEntries: number;
  totalAttributes: number;
  encoding: string;
  baseDN?: string;
  entries: LDIFEntry[];
  schema?: LDIFSchemaDefinition[];
}

export interface LDIFEntryMetadata extends CommonMetadata {
  type: 'ldif-entry';
  dn: string;
  objectClasses: string[];
  attributes: LDIFAttribute[];
  entryIndex: number;
}

export interface LDIFAttributeMetadata extends CommonMetadata {
  type: 'ldif-attribute';
  attributeName: string;
  attributeType: string;
  values: string[];
  isMultiValued: boolean;
  isBinary: boolean;
  dn: string;
}

export interface LDIFEntry {
  dn: string;
  objectClasses: string[];
  attributes: LDIFAttribute[];
  entryIndex: number;
}

export interface LDIFAttribute {
  name: string;
  values: string[];
  type: string;
  isMultiValued: boolean;
  isBinary: boolean;
}

export interface LDIFSchemaDefinition {
  name: string;
  type: string;
  description?: string;
  required: boolean;
  multiValued: boolean;
  syntax?: string;
}

// LDIF Wizard Types
export interface LDIFMetadataFormData {
  name: string;
  purpose: string;
  description: string;
  file: File | null;
  filePath: string;
  encoding: string;
  baseDN: string;
  entries: LDIFEntry[];
  schema: LDIFSchemaDefinition[];
  totalEntries: number;
  totalAttributes: number;
  validationMode: 'strict' | 'lenient' | 'custom';
}

export interface LDIFMetadataWizardProps {
  isOpen: boolean;
  onClose: () => void;
  onSave: (metadata: LDIFMetadataFormData) => void;
}

// Web Service Metadata Types
export interface WebServiceMetadata extends CommonMetadata {
  type: 'web-service';
  serviceType: 'REST' | 'SOAP' | 'GraphQL';
  baseUrl: string;
  authenticationType: 'none' | 'basic' | 'bearer' | 'apiKey' | 'oauth2';
  endpoints: WebServiceEndpoint[];
  totalEndpoints: number;
  version?: string;
  headers?: Array<{ name: string; value: string; required: boolean }>;
  rateLimit?: {
    requests: number;
    period: string;
  };
}

export interface WebServiceEndpointMetadata extends CommonMetadata {
  type: 'web-service-endpoint';
  endpointPath: string;
  httpMethod: 'GET' | 'POST' | 'PUT' | 'DELETE' | 'PATCH';
  parameters: WebServiceParameter[];
  responseType: string;
  description: string;
  requiresAuth: boolean;
}

export interface WebServiceParameterMetadata extends CommonMetadata {
  type: 'web-service-parameter';
  parameterName: string;
  parameterType: string;
  dataType: string;
  required: boolean;
  location: 'query' | 'path' | 'body' | 'header';
  description: string;
  exampleValue?: string;
}

export interface WebServiceEndpoint {
  id: string;
  path: string;
  method: string;
  description: string;
  parameters: WebServiceParameter[];
  responseType: string;
  requiresAuth: boolean;
  sampleRequest?: string;
  sampleResponse?: string;
}

export interface WebServiceParameter {
  name: string;
  type: string;
  dataType: string;
  required: boolean;
  location: string;
  description: string;
  exampleValue?: string;
}

export interface WebServiceSchemaDefinition {
  name: string;
  type: string;
  description?: string;
  required: boolean;
  example?: string;
}

// Web Service Wizard Types
export interface WebServiceMetadataFormData {
  name: string;
  purpose: string;
  description: string;
  serviceType: 'REST' | 'SOAP' | 'GraphQL';
  baseUrl: string;
  authenticationType: 'none' | 'basic' | 'bearer' | 'apiKey' | 'oauth2';
  endpoints: WebServiceEndpoint[];
  headers: Array<{ name: string; value: string; required: boolean }>;
  rateLimit: {
    requests: number;
    period: string;
  };
  version: string;
  totalEndpoints: number;
  validationMode: 'strict' | 'lenient' | 'custom';
}

export interface WebServiceMetadataWizardProps {
  isOpen: boolean;
  onClose: () => void;
  onSave: (metadata: WebServiceMetadataFormData) => void;
}

// LDAP Metadata Types
export interface LDAPMetadata extends CommonMetadata {
  type: 'ldap';
  connectionType: 'Active Directory' | 'OpenLDAP' | 'ApacheDS' | 'Other';
  serverUrl: string;
  baseDN: string;
  authenticationType: 'simple' | 'SASL' | 'GSSAPI';
  port: number;
  useSSL: boolean;
  connections: LDAPConnection[];
  schemas: LDAPSchema[];
  totalConnections: number;
  version?: string;
}

export interface LDAPConnectionMetadata extends CommonMetadata {
  type: 'ldap-connection';
  connectionName: string;
  serverUrl: string;
  baseDN: string;
  authenticationType: string;
  port: number;
  useSSL: boolean;
  bindDN?: string;
  timeout: number;
  connectionPool: boolean;
  status: 'connected' | 'disconnected' | 'error';
}

export interface LDAPSchemaMetadata extends CommonMetadata {
  type: 'ldap-schema';
  schemaName: string;
  objectClasses: LDAPObjectClass[];
  attributeTypes: LDAPAttributeType[];
  matchingRules: string[];
  schemaType: 'core' | 'cosine' | 'inetorgperson' | 'custom';
}

export interface LDAPObjectClassMetadata extends CommonMetadata {
  type: 'ldap-objectclass';
  objectClassName: string;
  oid: string;
  description: string;
  superior: string[];
  objectClassType: 'structural' | 'auxiliary' | 'abstract';
  mandatoryAttributes: string[];
  optionalAttributes: string[];
}

export interface LDAPConnection {
  id: string;
  name: string;
  serverUrl: string;
  baseDN: string;
  authenticationType: string;
  port: number;
  useSSL: boolean;
  bindDN?: string;
  timeout: number;
  connectionPool: boolean;
  status: 'connected' | 'disconnected' | 'error';
}

export interface LDAPSchema {
  name: string;
  type: string;
  schemaType: string;
  objectClasses?: LDAPObjectClass[];
  attributeTypes?: LDAPAttributeType[];
  matchingRules?: string[];
}

export interface LDAPObjectClass {
  name: string;
  oid: string;
  description: string;
  superior: string[];
  type: 'structural' | 'auxiliary' | 'abstract';
  mandatoryAttributes: string[];
  optionalAttributes: string[];
}

export interface LDAPAttributeType {
  name: string;
  oid: string;
  description: string;
  syntax: string;
  singleValue: boolean;
  equality: string;
  ordering: string;
  substring: string;
}

// LDAP Metadata Wizard Types
export interface LDAPMetadataFormData {
  name: string;
  purpose: string;
  description: string;
  file: File | null;
  filePath: string;
  connectionType: 'Active Directory' | 'OpenLDAP' | 'ApacheDS' | 'Other';
  serverUrl: string;
  baseDN: string;
  authenticationType: 'simple' | 'SASL' | 'GSSAPI';
  port: number;
  useSSL: boolean;
  connections: LDAPConnection[];
  schemas: LDAPSchema[];
  bindDN?: string;
  bindPassword?: string;
  timeout: number;
  connectionPool: boolean;
  version: string;
  totalConnections: number;
  validationMode: 'strict' | 'lenient' | 'custom';
}

export interface LDAPMetadataWizardProps {
  isOpen: boolean;
  onClose: () => void;
  onSave: (metadata: LDAPMetadataFormData) => void;
}

// Database Types
export interface DatabaseConnectionConfig {
  dbType: string;
  host: string;
  port: number;
  database: string;
  username: string;
  password: string;
  schema?: string;
  connectionId?: string;
  additionalParams?: Record<string, any>;
}

export interface DatabaseTableMetadata {
  schemaname: string;
  tablename: string;
  tabletype: string;
  columns: DatabaseColumnMetadata[];
  num_columns: number;
  description?: string;
}

export interface DatabaseColumnMetadata {
  name: string;
  type: string;
  length?: number;
  precision?: number;
  scale?: number;
  nullable: boolean;
  default?: string;
  description?: string;
}

export interface DatabaseMetadata extends CommonMetadata {
  type: 'database';
  connection: DatabaseConnectionConfig;
  tables: DatabaseTableMetadataWizard[];
  selectedTables: string[];
  totalTables: number;
  totalColumns: number;
  version?: string;
  connectionTested: boolean;
}

export interface DatabaseTableMetadataWizard extends CommonMetadata {
  type: 'database-table';
  schemaname: string;
  tablename: string;
  tabletype: string;
  columns: DatabaseColumnMetadata[];
  num_columns: number;
}

export interface DatabaseColumnMetadataWizard extends CommonMetadata {
  type: 'database-column';
  columnName: string;
  dataType: string;
  length?: number;
  precision?: number;
  scale?: number;
  nullable: boolean;
  defaultValue?: string;
  tableName: string;
  schemaName: string;
}

export interface DatabaseMetadataFormData {
  name: string;
  purpose: string;
  description: string;
  dbType: string;
  connection: DatabaseConnectionConfig;
  selectedTables: string[];
  tableSelections: Record<string, {
    include: boolean;
    selectedColumns: string[];
  }>;
  schemaInference: Record<string, {
    name: string;
    type: string;
    length?: number;
    precision?: number;
    scale?: number;
    nullable: boolean;
    defaultValue?: string;
    description?: string;
  }>;
  connectionTested: boolean;
  previewData?: Record<string, any[]>;
  availableTables?: DatabaseTableMetadata[];
}

export interface DatabaseMetadataWizardProps {
  isOpen: boolean;
  onClose: () => void;
  onSave: (metadata: DatabaseMetadataFormData) => void;
}

// Drag and Drop Types
export interface DragState {
  isDragging: boolean;
  draggedNode: RepositoryNode | null;
  dragOverNode: RepositoryNode | null;
  dropPosition: 'before' | 'inside' | 'after' | null;
}

// Schema Types
export type DataType = 
  | 'String' 
  | 'Integer' 
  | 'Float' 
  | 'Boolean' 
  | 'Date' 
  | 'DateTime' 
  | 'Decimal' 
  | 'Object' 
  | 'Array';

export interface SchemaColumn {
  id: string;
  name: string;
  type: DataType;
  length?: number;
  precision?: number;
  nullable: boolean;
  isKey: boolean;
  isPrimary: boolean;
  isUnique: boolean;
  defaultValue?: string;
  comment?: string;
  isEditing?: boolean;
  validation?: {
    isValid: boolean;
    message?: string;
  };
}

export interface ValidationResult {
  type: 'error' | 'warning' | 'info';
  isValid: boolean;
  errors: string[];
  warnings: string[];
  message: string;
  ruleId: string;
  duplicateColumns: string[];
  typeMismatches: Array<{ column: string; expected: DataType; value: string }>;
}

export interface SchemaEditorDialogProps {
  open: boolean;
  componentName: string;
  initialSchema?: SchemaColumn[];
  onSave: (schema: SchemaColumn[]) => void;
  onCancel: () => void;
  onSchemaChange?: (schema: SchemaColumn[]) => void;
}

export interface CellEditState {
  rowIndex: number;
  columnKey: keyof SchemaColumn;
  value: any;
}

export interface AdvancedOptions {
  dateFormat: string;
  numberPattern: string;
  enforceLengthPrecision: boolean;
  enforceNullable: boolean;
}

export type PatternType = 
  | 'regex'
  | 'contains'
  | 'startsWith'
  | 'endsWith'
  | 'exactMatch'
  | 'dictionary'
  | 'custom';

// Rule Engine Types
export interface Rule {
  id: string;
  name: string;
  enabled: boolean;
  inputColumn: string;
  operation: string;
  matchPattern: string;
  replacement?: string;
  priority?: number;
  description?: string;
  patternType?: PatternType;
  lookup?: LookupReference;
}

export interface RuleSet {
  id: string;
  name: string;
  description: string;
  rules: Rule[];
  version: string;
  created: Date;
  lastModified: Date;
  author: string;
  inputSchema?: SchemaField[];
}

// Data Quality Types
export interface SchemaField {
  name: string;
  type: string;
  required?: boolean;
  description?: string;
  nullable?: boolean;
}

export interface LookupReference {
  id: string;
  name: string;
  type: string;
  source: string;
  description?: string;
}

export type OperationType = 
  | 'Replace'
  | 'Normalize'
  | 'Cleanse'
  | 'Format'
  | 'UPPERCASE'
  | 'lowercase'
  | 'Title Case'
  | 'Remove noise characters'
  | 'Abbreviation expansion'
  | 'Trim whitespace'
  | 'Remove duplicates'
  | 'Standardize date'
  | 'Standardize phone'
  | 'Standardize address';

export interface TestCase {
  id: string;
  testValue: string;
  expectedOutput: string;
  description?: string;
  inputColumn: string;
}


export interface SQLGenerationJob {
  id: string;
  name: string;
  status: 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';
  createdAt: string;
  startedAt?: string;
  completedAt?: string;
  nodes: CanvasNode[];
  connections: CanvasConnection[];
  sqlGenerated: string[];
  errors: string[];
  warnings: string[];
  settings: {
    includeComments: boolean;
    formatSQL: boolean;
    validateSQL: boolean;
    targetDialect: 'postgresql' | 'mysql' | 'sqlserver' | 'oracle';
  };
}

export interface PipelineGenerationOptions {
  includeComments: boolean;
  formatSQL: boolean;
  validateSQL: boolean;
  generateExecutionPlan: boolean;
  optimizeQueries: boolean;
  targetDialect: 'postgresql' | 'mysql' | 'sqlserver' | 'oracle';
  batchSize: number;
  timeout: number;
  connectionConfig?: {
    host: string;
    port: number;
    database: string;
    username: string;
    password?: string;
    sslMode: 'disable' | 'allow' | 'prefer' | 'require' | 'verify-ca' | 'verify-full';
  };
}

export interface PipelineGenerationResult {
  success: boolean;
  sql: string[];
  executionPlans: Array<{
    nodeId: string;
    sql: string;
    plan: any;
  }>;
  warnings: string[];
  errors: string[];
  jobId?: string;
  generatedAt: string;
  statistics: {
    totalNodes: number;
    totalConnections: number;
    generationTimeMs: number;
    validationTimeMs: number;
  };
}

// Canvas Types (if not already imported from pipeline-types.ts)
export interface CanvasNode {
  id: string;
  name: string;
  type: string;
  position: { x: number; y: number };
  size: { width: number; height: number };
  metadata?: any;
}

export interface CanvasConnection {
  id: string;
  sourceNodeId: string;
  sourcePortId: string;
  targetNodeId: string;
  targetPortId: string;
  dataFlow: {
    schemaMappings: Array<{
      sourceColumn: string;
      targetColumn: string;
      transformation?: string;
    }>;
  };
  status: string;
}

export interface DeletionHistoryItem {
  node: RepositoryNode;
  timestamp: string;
  parentId?: string;
  deletedAt: string;  // Add this
  id?: string;         // Add this
  childrenCount?: number; // Add this
}

// SQL Generation Slice State Types
export interface SQLGenerationState {
  generatedSQL: {
    entities: Record<string, string>;
    ids: string[];
  };
  generationJobs: SQLGenerationJob[];
  activeJobId?: string;
  templates: Array<{
    id: string;
    name: string;
    sql: string;
    description?: string;
    createdAt: string;
    tags: string[];
  }>;
  cache: Record<string, any>;
  settings: {
    autoValidate: boolean;
    autoFormat: boolean;
    defaultDialect: string;
    timeout: number;
  };
  loadingStates: Record<string, boolean>;
  errors: Array<{
    id: string;
    message: string;
    code: string;
    timestamp: string;
  }>;
  executionPlans: Record<string, any>;
}

// NEW: Repository Sidebar Props for job-canvas integration
export interface RepositorySidebarProps {
  onNodeSelect?: (node: RepositoryNode) => void;
  onNodeDoubleClick?: (node: RepositoryNode) => void;
  onCreateItem?: (type: string, parentId?: string) => void;
  onDragStart?: (node: RepositoryNode) => void;
  onDrop?: (draggedNode: RepositoryNode, targetNode: RepositoryNode, position: 'before' | 'inside' | 'after') => void;
  onCreateJob: (jobName: string, jobId?: string) => void; // Updated to accept jobId
  currentJob?: RepositoryNode | null;
  reactFlowInstance?: any;
  onNodeCreate?: (nodeData: any, position: { x: number, y: number }) => void;
  onNodeUpdate?: (nodeId: string, updates: any) => void;
}

// NEW: Tree View Props for job design info display
export interface TreeViewProps {
  repositoryData: RepositoryNode[];
  expandedNodes: Set<string>;
  selectedNode: string | null;
  searchTerm: string;
  dragState: DragState;
  onToggle: (nodeId: string) => void;
  onSelect: (node: RepositoryNode) => void;
  onDoubleClick: (node: RepositoryNode) => void;
  onContextMenu: (node: RepositoryNode, position: { x: number; y: number }) => void;
  onDelete: (node: RepositoryNode) => void;
  onDragStart: (node: RepositoryNode, event: React.DragEvent) => void;
  onDragOver: (node: RepositoryNode, event: React.DragEvent) => void;
  onDragLeave: (event: React.DragEvent) => void;
  onDrop: (targetNode: RepositoryNode, event: React.DragEvent) => void;
  onDragEnd: () => void;
  // Add these new props for job design info:
  getJobDesignInfo?: (jobId: string) => string;
  activeDesignId?: string | null;
}

// NEW: Debug Panel Props for job designs
export interface DebugPanelProps {
  isDebugOpen: boolean;
  setIsDebugOpen: (open: boolean) => void;
  onTestConnection: () => void;
  onListTables: () => void;
  onRefresh: () => void;
  onTestTableCreation: () => void;
  onManualSave: () => void;
  onResetRepository: () => void;
  healthStatus: any;
  isPostgresConnected: boolean;
  connectionConfig: any;
  dbStatus: any;
  currentConnectionId: string | null;
  foreignTables: any[];
  debugInfo: any;
  repositoryData: RepositoryNode[];
  expandedNodes: Set<string>;
  selectedNode: string | null;
  deletionHistory: DeletionHistoryItem[];
  isUndoAvailable: boolean;
  // Add these new props for job designs:
  jobDesigns?: Record<string, JobDesignState>;
  activeDesignId?: string | null;
}

// Note: JobDesignState interface is already defined at the top of this file
// after JobConfig interface