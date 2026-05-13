// frontend/src/services/database-api.types.ts

// Client-side types - separate from backend types to avoid conflicts
export type DatabaseType = 
  | 'mysql'
  | 'postgresql' 
  | 'postgres'
  | 'oracle'
  | 'sqlserver'
  | 'mssql'
  | 'db2'
  | 'sap-hana'
  | 'hana'
  | 'sybase'
  | 'netezza'
  | 'informix'
  | 'firebird';

export interface ClientDatabaseConfig {
  dbname: string;
  host?: string;
  port?: string;
  user?: string;
  password?: string;
  schema?: string;
  [key: string]: any;
}

export interface ClientColumnMetadata {
  description: string;
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

export interface ClientTableInfo {
  schemaname: string;
  tablename: string;
  tabletype: string;
  columns: ClientColumnMetadata[];
  originalData?: any;
  comment?: string;
  rowCount?: number;
  size?: string;
  hasindex?: boolean;
  relpages?: number;
  reltuples?: number;
  numatts?: number;
}

export interface ClientInspectionOptions {
  includeViews?: boolean;
  includeSystemTables?: boolean;
  schema?: string;
  maxRows?: number;
}

export interface ClientQueryExecutionOptions {
  maxRows?: number;
  timeout?: number;
  autoDisconnect?: boolean;
  transaction?: boolean;
  params?: any[];
}

export interface ClientQueryResult {
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

export interface ClientDatabaseVersionInfo {
  version: string;
  name: string;
  encoding?: string;
  collation?: string;
  edition?: string;
}

export interface ClientConnectionInfo {
  connectionId: string;
  dbType: string;
  config: ClientDatabaseConfig;
}

export interface ClientTestConnectionResult {
  success: boolean;
  version?: string;
  error?: string;
}

export interface ClientConnectResult {
  connectionId: string;
  success: boolean;
  error?: string;
}

export interface ClientTableListResult {
  tables: ClientTableInfo[];
  success: boolean;
  error?: string;
}

export interface ClientQueryExecutionResult {
  result: any;
  success: boolean;
  executionTime?: number;
  error?: string;
  rows?: any[];               // ✅ query result rows
  rowCount?: number;          // ✅ number of rows affected/returned
  fields?: Array<{           // ✅ column metadata
    name: string;
    type: string;
  }>;
}

export interface ClientDatabaseInfoResult {
  info: ClientDatabaseVersionInfo;
  success: boolean;
  error?: string;
}

export interface ClientDisconnectResult {
  success: boolean;
  error?: string;
}

// Request payload types
export interface TestConnectionRequest {
  dbType: DatabaseType;
  config: ClientDatabaseConfig;
}

export interface ConnectRequest {
  dbType: DatabaseType;
  config: ClientDatabaseConfig;
}

export interface DisconnectRequest {
  connectionId: string;
}

export interface GetTablesRequest {
  connectionId: string;
  options?: ClientInspectionOptions;
}

export interface ExecuteQueryRequest {
  connectionId: string;
  sql: string;
  options?: ClientQueryExecutionOptions;
}

export interface GetDatabaseInfoRequest {
  connectionId: string;
}

export interface TransactionRequest {
  connectionId: string;
  queries: Array<{ sql: string; params?: any[] }>;
}

export interface GetConstraintsRequest {
  connectionId: string;
  schema: string;
  table: string;
}

export interface GetSchemasRequest {
  connectionId: string;
}

export interface BatchDisconnectRequest {
  connectionIds: string[];
}