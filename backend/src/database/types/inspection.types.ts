// backend/src/database/types/inspection.types.ts

/**
 * Common inspection types shared across all database adapters
 */

// Connection and result types
export interface DatabaseConnection {
  connected: boolean;
  [key: string]: any;
}

export interface QueryResult {
  success: boolean;
  rows?: any[];
  rowCount?: number;
  fields?: Array<{
    name: string;
    type: string;
  }>;
  executionTime?: number;
  error?: string;
  affectedRows?: number;
  command?: string;
}

// Common metadata types
export interface ColumnMetadata {
  name: string;
  type: string;
  dataType?: string;
  length?: number;
  precision?: number;
  scale?: number;
  nullable: boolean;
  default?: string;
  isPrimaryKey?: boolean;
  isUnique?: boolean;
  isAutoIncrement?: boolean;
  isForeignKey?: boolean;
  foreignTable?: string;
  foreignColumn?: string;
  comment?: string;
  ordinalPosition: number;
}

export interface TableInfo {
  schemaname: string;
  tablename: string;
  tabletype: string;
  columns: ColumnMetadata[];
  originalData?: any;
  comment?: string;
  rowCount?: number;
  size?: string;
  hasindex?: boolean;
  relpages?: number;
  reltuples?: number;
  numatts?: number;
}

export interface ConstraintInfo {
  name: string;
  type: string;
  tableName: string;
  columnName: string;
  foreignTable?: string;
  foreignColumn?: string;
}

// Database configuration types
export interface DatabaseConfig {
  dbname: string;
  host?: string;
  port?: string;
  user?: string;
  password?: string;
  schema?: string;
  [key: string]: any;
}

// Inspection options
export interface InspectionOptions {
  includeViews?: boolean;
  includeSystemTables?: boolean;
  schema?: string;
  maxRows?: number;
}

// Archive handle (compatible with your existing inspector code)
export interface ArchiveHandle {
  connection?: DatabaseConnection;
  remoteVersionStr?: string;
  remoteVersion?: number;
  archiveRemoteVersion?: string;
  minRemoteVersion?: number;
  maxRemoteVersion?: number;
  isStandby?: boolean;
  [key: string]: any;
}

// Trivalue enum (from your inspector code)
export enum Trivalue {
  TRI_DEFAULT = 0,
  TRI_NO = 1,
  TRI_YES = 2
}

// Query execution options
export interface QueryExecutionOptions {
  maxRows?: number;
  timeout?: number;
  autoDisconnect?: boolean;
  transaction?: boolean;
  params?: any[];
}

// Database version info
export interface DatabaseVersionInfo {
  version: string;
  name: string;
  encoding?: string;
  collation?: string;
  edition?: string;
}