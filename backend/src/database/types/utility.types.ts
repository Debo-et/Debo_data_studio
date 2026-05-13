// backend/src/database/types/utility.types.ts

/**
 * Utility Types for Database Operations
 */

/**
 * Generic result wrapper for database operations
 */
export interface Result<T = any> {
  success: boolean;
  data?: T;
  error?: string;
  warnings?: string[];
  metadata?: {
    executionTime?: number;
    rowCount?: number;
    affectedRows?: number;
    [key: string]: any;
  };
}

/**
 * Pagination parameters
 */
export interface PaginationParams {
  page: number;
  pageSize: number;
  total?: number;
}

/**
 * Paginated result
 */
export interface PaginatedResult<T> {
  items: T[];
  pagination: {
    page: number;
    pageSize: number;
    totalItems: number;
    totalPages: number;
    hasNext: boolean;
    hasPrevious: boolean;
  };
}

/**
 * Filter criteria
 */
export interface FilterCriteria {
  field: string;
  operator: 'eq' | 'neq' | 'gt' | 'gte' | 'lt' | 'lte' | 'like' | 'in' | 'notIn' | 'isNull' | 'isNotNull';
  value: any;
}

/**
 * Sort criteria
 */
export interface SortCriteria {
  field: string;
  direction: 'asc' | 'desc';
}

/**
 * Query builder options
 */
export interface QueryBuilderOptions {
  filters?: FilterCriteria[];
  sort?: SortCriteria[];
  pagination?: PaginationParams;
  fields?: string[];
  relations?: string[];
}

/**
 * Async operation status
 */
export interface AsyncOperationStatus {
  id: string;
  status: 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';
  progress?: number;
  message?: string;
  result?: any;
  error?: string;
  startedAt: Date;
  completedAt?: Date;
}

/**
 * Cache configuration
 */
export interface CacheConfig {
  enabled: boolean;
  ttl: number; // Time to live in seconds
  maxSize?: number;
  strategy: 'memory' | 'redis' | 'custom';
}

/**
 * Retry configuration
 */
export interface RetryConfig {
  maxAttempts: number;
  delay: number;
  backoffMultiplier?: number;
  retryableErrors?: string[];
}

/**
 * Configuration for database operations
 */
export interface OperationConfig {
  timeout?: number;
  retry?: RetryConfig;
  cache?: CacheConfig;
  transaction?: boolean;
  isolationLevel?: 'read-uncommitted' | 'read-committed' | 'repeatable-read' | 'serializable';
}

/**
 * Batch operation result
 */
export interface BatchOperationResult {
  total: number;
  successful: number;
  failed: number;
  errors: Array<{
    operation: string;
    error: string;
  }>;
  results: any[];
}

/**
 * Health check result
 */
export interface HealthCheckResult {
  status: 'healthy' | 'degraded' | 'unhealthy';
  checks: Array<{
    name: string;
    status: 'pass' | 'warn' | 'fail';
    message?: string;
    duration?: number;
  }>;
  timestamp: Date;
}