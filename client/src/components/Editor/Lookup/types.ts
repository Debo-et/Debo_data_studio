// types.ts
export interface LookupConfig {
  keyField: string;
  filePath: string;
  fieldMapping: Record<string, string>;
  fileType: 'csv' | 'json' | 'excel' | 'parquet';
  cacheSize?: number;
  reloadOnChange?: boolean;
  joinType?: 'inner' | 'left';
}

export interface FileInfo {
  name: string;
  size: number;
  type: string;
  lastModified: Date;
  previewData: any[];
  columns: string[];
  rowCount: number;
}

export interface SchemaField {
  name: string;
  type: string;
  sampleValues: any[];
}

export interface ConnectionLine {
  from: { x: number; y: number };
  to: { x: number; y: number };
  fromField: string;
  toField: string;
}

export interface TestResult {
  matched: number;
  unmatched: number;
  total: number;
  sampleOutput: any[];
  executionTime: number;
  cacheHitRate?: number;
}