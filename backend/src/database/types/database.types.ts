// backend/src/database/types/database.types.ts

import { DatabaseConnection } from './inspection.types';

/**
 * Core Database Types and Interfaces
 * These types are used across all database adapters and connection management
 */

// ===========================================================================
// Database Connection Types
// ===========================================================================

/**
 * Generic database connection result
 */
export interface ConnectionResult {
  success: boolean;
  connectionId?: string;
  error?: string;
  connection?: DatabaseConnection;
  version?: string;
}

/**
 * Database connection pool configuration
 */
export interface ConnectionPoolConfig {
  min: number;
  max: number;
  idleTimeoutMillis: number;
  connectionTimeoutMillis: number;
}

/**
 * Connection health status
 */
export interface ConnectionHealth {
  connected: boolean;
  lastHeartbeat?: Date;
  errorCount: number;
  isHealthy: boolean;
  latency?: number;
}

// ===========================================================================
// Database Query Types
// ===========================================================================

/**
 * Query parameter types
 */
export type QueryParam = string | number | boolean | Date | null | Buffer;

/**
 * Prepared statement configuration
 */
export interface PreparedStatement {
  name: string;
  text: string;
  params?: QueryParam[];
}

/**
 * Batch query configuration
 */
export interface BatchQuery {
  queries: Array<{
    sql: string;
    params?: QueryParam[];
    timeout?: number;
  }>;
  transaction?: boolean;
}

// ===========================================================================
// Database Metadata Types
// ===========================================================================

/**
 * Database object types
 */
export enum DatabaseObjectType {
  TABLE = 'TABLE',
  VIEW = 'VIEW',
  INDEX = 'INDEX',
  SEQUENCE = 'SEQUENCE',
  FUNCTION = 'FUNCTION',
  PROCEDURE = 'PROCEDURE',
  TRIGGER = 'TRIGGER',
  CONSTRAINT = 'CONSTRAINT',
  SCHEMA = 'SCHEMA',
  DATABASE = 'DATABASE',
  USER = 'USER',
  ROLE = 'ROLE'
}

/**
 * Database index information
 */
export interface IndexInfo {
  name: string;
  tableName: string;
  schemaName: string;
  columns: string[];
  isUnique: boolean;
  isPrimary: boolean;
  type: string;
  comment?: string;
}

/**
 * Database function/procedure information
 */
export interface FunctionInfo {
  name: string;
  schema: string;
  returnType: string;
  parameters: Array<{
    name: string;
    type: string;
    defaultValue?: string;
    mode?: 'IN' | 'OUT' | 'INOUT';
  }>;
  language: string;
  isDeterministic: boolean;
  securityType: 'DEFINER' | 'INVOKER';
  comment?: string;
}

/**
 * Database trigger information
 */
export interface TriggerInfo {
  name: string;
  tableName: string;
  schemaName: string;
  timing: 'BEFORE' | 'AFTER' | 'INSTEAD OF';
  events: string[];
  condition?: string;
  statement: string;
  orientation: 'ROW' | 'STATEMENT';
  enabled: boolean;
}

// ===========================================================================
// Database Configuration Types
// ===========================================================================

/**
 * SSL configuration for database connections
 */
export interface SSLConfig {
  enabled: boolean;
  ca?: string;
  cert?: string;
  key?: string;
  rejectUnauthorized?: boolean;
}

/**
 * Connection timeout configuration
 */
export interface TimeoutConfig {
  connect: number;
  query: number;
  idle: number;
  acquire: number;
}

/**
 * Database driver options
 */
export interface DriverOptions {
  [key: string]: any;
}

/**
 * Complete database configuration
 */
export interface DatabaseConfiguration {
  // Connection
  host: string;
  port: number;
  database: string;
  username: string;
  password: string;
  
  // Optional
  schema?: string;
  serviceName?: string; // For Oracle
  instanceName?: string; // For SQL Server
  applicationName?: string;
  charset?: string;
  timezone?: string;
  
  // Connection pooling
  pool?: ConnectionPoolConfig;
  
  // SSL
  ssl?: SSLConfig;
  
  // Timeouts
  timeouts?: TimeoutConfig;
  
  // Driver-specific options
  driverOptions?: DriverOptions;
  
  // Additional metadata
  metadata?: {
    tags?: string[];
    description?: string;
    environment?: 'development' | 'staging' | 'production';
  };
}

// ===========================================================================
// Database Statistics Types
// ===========================================================================

/**
 * Table statistics
 */
export interface TableStatistics {
  rowCount: number;
  sizeBytes: number;
  indexSizeBytes: number;
  toastSizeBytes?: number;
  lastAnalyzed?: Date;
  lastVacuum?: Date;
  lastAutoVacuum?: Date;
}

/**
 * Database performance statistics
 */
export interface PerformanceStats {
  queriesExecuted: number;
  totalExecutionTime: number;
  averageQueryTime: number;
  slowQueries: number;
  connectionsActive: number;
  connectionsIdle: number;
  connectionsTotal: number;
}

/**
 * Database resource usage
 */
export interface ResourceUsage {
  cpuUsage?: number;
  memoryUsage?: number;
  diskUsage?: number;
  connectionsUsage?: number;
  transactionsActive?: number;
  locksActive?: number;
}

// ===========================================================================
// Database Migration/Backup Types
// ===========================================================================

/**
 * Database migration step
 */
export interface MigrationStep {
  version: string;
  description: string;
  sql: string;
  rollbackSql?: string;
  checksum: string;
  appliedAt?: Date;
  appliedBy?: string;
}

/**
 * Database backup configuration
 */
export interface BackupConfig {
  type: 'full' | 'incremental' | 'differential';
  compression: 'none' | 'gzip' | 'zip' | 'custom';
  location: string;
  retentionDays: number;
  schedule?: string; // Cron expression
  encrypt?: boolean;
  encryptionKey?: string;
}

// ===========================================================================
// Database Error Types
// ===========================================================================

/**
 * Database error categories
 */
export enum DatabaseErrorCategory {
  CONNECTION = 'CONNECTION',
  AUTHENTICATION = 'AUTHENTICATION',
  PERMISSION = 'PERMISSION',
  SYNTAX = 'SYNTAX',
  CONSTRAINT = 'CONSTRAINT',
  DEADLOCK = 'DEADLOCK',
  TIMEOUT = 'TIMEOUT',
  RESOURCE = 'RESOURCE',
  INTEGRITY = 'INTEGRITY',
  OTHER = 'OTHER'
}

/**
 * Enhanced database error
 */
export class DatabaseError extends Error {
  public category: DatabaseErrorCategory;
  public code?: string;
  public sqlState?: string;
  public query?: string;
  public parameters?: QueryParam[];
  public originalError?: Error;

  constructor(
    message: string,
    category: DatabaseErrorCategory = DatabaseErrorCategory.OTHER,
    options?: {
      code?: string;
      sqlState?: string;
      query?: string;
      parameters?: QueryParam[];
      originalError?: Error;
    }
  ) {
    super(message);
    this.name = 'DatabaseError';
    this.category = category;
    
    if (options) {
      this.code = options.code;
      this.sqlState = options.sqlState;
      this.query = options.query;
      this.parameters = options.parameters;
      this.originalError = options.originalError;
    }
  }
}

/**
 * Connection error
 */
export class ConnectionError extends DatabaseError {
  constructor(
    message: string,
    options?: {
      code?: string;
      sqlState?: string;
      originalError?: Error;
    }
  ) {
    super(message, DatabaseErrorCategory.CONNECTION, options);
    this.name = 'ConnectionError';
  }
}

/**
 * Query error
 */
export class QueryError extends DatabaseError {
  constructor(
    message: string,
    options?: {
      code?: string;
      sqlState?: string;
      query?: string;
      parameters?: QueryParam[];
      originalError?: Error;
    }
  ) {
    super(message, DatabaseErrorCategory.SYNTAX, options);
    this.name = 'QueryError';
  }
}

// ===========================================================================
// Database Events and Observability
// ===========================================================================

/**
 * Database event types
 */
export enum DatabaseEventType {
  CONNECTION_OPENED = 'CONNECTION_OPENED',
  CONNECTION_CLOSED = 'CONNECTION_CLOSED',
  QUERY_STARTED = 'QUERY_STARTED',
  QUERY_COMPLETED = 'QUERY_COMPLETED',
  TRANSACTION_STARTED = 'TRANSACTION_STARTED',
  TRANSACTION_COMMITTED = 'TRANSACTION_COMMITTED',
  TRANSACTION_ROLLED_BACK = 'TRANSACTION_ROLLED_BACK',
  ERROR_OCCURRED = 'ERROR_OCCURRED'
}

/**
 * Database event
 */
export interface DatabaseEvent {
  type: DatabaseEventType;
  timestamp: Date;
  connectionId?: string;
  databaseType?: string;
  databaseName?: string;
  duration?: number;
  query?: string;
  error?: Error;
  metadata?: Record<string, any>;
}

/**
 * Event listener type
 */
export type DatabaseEventListener = (event: DatabaseEvent) => void;

// ===========================================================================
// Database Driver Specific Types
// ===========================================================================

/**
 * MySQL specific types
 */
export interface MySQLSpecificConfig {
  socketPath?: string;
  compress?: boolean;
  decimalNumbers?: boolean;
  multipleStatements?: boolean;
  timezone?: string;
  charset?: string;
  ssl?: SSLConfig;
}

/**
 * PostgreSQL specific types
 */
export interface PostgreSQLSpecificConfig {
  ssl?: SSLConfig;
  binary?: boolean;
  client_encoding?: string;
  application_name?: string;
  fallback_application_name?: string;
  connectionTimeoutMillis?: number;
  statement_timeout?: number;
  idle_in_transaction_session_timeout?: number;
}

/**
 * Oracle specific types
 */
export interface OracleSpecificConfig {
  connectString?: string;
  externalAuth?: boolean;
  edition?: string;
  events?: boolean;
  poolAlias?: string;
  stmtCacheSize?: number;
}

/**
 * SQL Server specific types
 */
export interface SQLServerSpecificConfig {
  instanceName?: string;
  domain?: string;
  encrypt?: boolean;
  trustServerCertificate?: boolean;
  connectionTimeout?: number;
  requestTimeout?: number;
  appName?: string;
}

/**
 * DB2 specific types
 */
export interface DB2SpecificConfig {
  driver?: string;
  connectTimeout?: number;
  securityMechanism?: string;
  ssl?: SSLConfig;
}

// ===========================================================================
// Database Adapter Configuration
// ===========================================================================

/**
 * Adapter configuration
 */
export interface AdapterConfig {
  name: string;
  enabled: boolean;
  priority: number;
  features: string[];
  limitations: string[];
  driver: string;
  version?: string;
}

/**
 * Feature support matrix
 */
export interface FeatureSupport {
  schemaInspection: boolean;
  queryExecution: boolean;
  transactions: boolean;
  preparedStatements: boolean;
  streaming: boolean;
  bulkOperations: boolean;
  storedProcedures: boolean;
  userDefinedFunctions: boolean;
}

// ===========================================================================
// Utility Types
// ===========================================================================

/**
 * Database comparison result
 */
export interface DatabaseComparison {
  source: string;
  target: string;
  differences: Array<{
    type: 'TABLE' | 'COLUMN' | 'INDEX' | 'CONSTRAINT';
    name: string;
    status: 'ADDED' | 'REMOVED' | 'MODIFIED';
    details?: string;
  }>;
}

/**
 * Database validation result
 */
export interface ValidationResult {
  isValid: boolean;
  errors: string[];
  warnings: string[];
  suggestions: string[];
}

/**
 * Connection test result
 */
export interface ConnectionTestResult {
  success: boolean;
  latency?: number;
  version?: string;
  features?: string[];
  error?: string;
  timestamp: Date;
}

// ===========================================================================
// Export All Types
// ===========================================================================

export {
  // Re-export from inspection.types.ts for backward compatibility
  DatabaseConfig,
  TableInfo,
  ColumnMetadata,
  QueryResult,
  InspectionOptions,
  QueryExecutionOptions,
  DatabaseVersionInfo
} from './inspection.types';