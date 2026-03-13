// src/types/metadata.types.ts

export interface ColumnMetadata {
  name: string;
  dataType: string;
  nullable: boolean;
  isKey?: boolean;
  isPrimary?: boolean;
  isUnique?: boolean;
  length?: number;
  precision?: number;
  scale?: number;
  defaultValue?: string;
  description?: string;
  constraints?: string[];
  sampleValues?: string[];
}

export interface TableMetadata {
  tableName: string;
  schemaName?: string;
  tableType?: string;
  columns: ColumnMetadata[];
  rowCount?: number;
  size?: string;
  lastModified?: string;
  created?: string;
  description?: string;
  indexes?: Array<{
    name: string;
    columns: string[];
    type: string;
    isUnique: boolean;
  }>;
  foreignKeys?: Array<{
    name: string;
    columns: string[];
    referencedTable: string;
    referencedColumns: string[];
  }>;
  additionalProperties?: Record<string, any>;
}

export interface FileMetadata {
  fileName: string;
  fileType: string;
  filePath?: string;
  size: string;
  encoding?: string;
  lastModified: string;
  created: string;
  rowCount?: number;
  columnCount?: number;
  sheetName?: string;
  delimiter?: string;
  hasHeaders?: boolean;
  schema?: ColumnMetadata[];
}

export interface ProcessingComponentMetadata {
  componentType: string;
  category: string;
  description: string;
  version?: string;
  author?: string;
  status?: string;
  inputSchema?: ColumnMetadata[];
  outputSchema?: ColumnMetadata[];
  parameters?: Record<string, any>;
  configuration?: Record<string, any>;
}

export type MetadataUnion = 
  | TableMetadata 
  | FileMetadata 
  | ProcessingComponentMetadata 
  | Record<string, any>;

// Helper functions for metadata processing
export const isTableMetadata = (metadata: any): metadata is TableMetadata => {
  return metadata && 
         (metadata.columns !== undefined || 
          metadata.schema !== undefined || 
          metadata.tableName !== undefined);
};

export const isFileMetadata = (metadata: any): metadata is FileMetadata => {
  return metadata && 
         (metadata.fileType !== undefined || 
          metadata.fileName !== undefined);
};

export const isProcessingComponentMetadata = (metadata: any): metadata is ProcessingComponentMetadata => {
  return metadata && 
         (metadata.componentType !== undefined || 
          metadata.category !== undefined);
};

export const normalizeTableMetadata = (metadata: any): TableMetadata | null => {
  if (!metadata) return null;
  
  // Extract from various metadata structures
  const columns: ColumnMetadata[] = [];
  
  if (metadata.columns && Array.isArray(metadata.columns)) {
    columns.push(...metadata.columns.map((col: any) => ({
      name: col.name || col.columnName || 'Unknown',
      dataType: col.type || col.dataType || 'unknown',
      nullable: col.nullable !== false,
      isKey: col.isKey || col.isPrimary || false,
      length: col.length,
      precision: col.precision,
      scale: col.scale,
      defaultValue: col.defaultValue || col.default,
      description: col.description || col.comment
    })));
  } else if (metadata.schema && Array.isArray(metadata.schema)) {
    columns.push(...metadata.schema.map((col: any) => ({
      name: col.name || 'Unknown',
      dataType: col.type || 'unknown',
      nullable: col.nullable !== false,
      length: col.length,
      description: col.description
    })));
  } else if (metadata.fields && Array.isArray(metadata.fields)) {
    columns.push(...metadata.fields.map((field: any) => ({
      name: field.name || 'Unknown',
      dataType: field.type || 'unknown',
      nullable: field.nullable !== false,
      description: field.description
    })));
  }
  
  if (columns.length === 0) return null;
  
  return {
    tableName: metadata.name || metadata.tableName || 'Untitled Table',
    schemaName: metadata.schemaName || metadata.schema,
    tableType: metadata.tableType || metadata.type,
    columns,
    rowCount: metadata.rowCount,
    size: metadata.size,
    lastModified: metadata.lastModified,
    created: metadata.created,
    description: metadata.description
  };
};