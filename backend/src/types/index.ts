export interface DatabaseConfig {
  type: DatabaseType;
  host: string;
  port: number;
  username: string;
  password: string;
  database: string;
  connectionLimit?: number;
  idleTimeout?: number;
  acquireTimeout?: number;
  ssl?: boolean;
  options?: Record<string, any>;
}

export type DatabaseType = 
  | 'mysql' 
  | 'oracle' 
  | 'mssql' 
  | 'db2' 
  | 'hana' 
  | 'sybase' 
  | 'netezza' 
  | 'informix' 
  | 'firebird';

export interface QueryRequest {
  dbIdentifier: string;
  query: string;
  parameters?: any[];
  timeout?: number;
  transaction?: boolean;
}

export interface QueryResult {
  success: boolean;
  data?: any[];
  metadata?: {
    rowCount: number;
    columns: ColumnMetadata[];
    executionTime: number;
  };
  error?: string;
  requestId: string;
  timestamp: string;
}

export interface ColumnMetadata {
  name: string;
  type: string;
  nullable: boolean;
}

export interface PoolMetrics {
  total: number;       // previously totalConnections
  available: number;   // previously idleConnections
  borrowed: number;    // previously activeConnections
  pending: number;     // previously waitingClients
}

export interface HealthStatus {
  status: 'healthy' | 'degraded' | 'unhealthy';
  databases: Record<string, DatabaseHealth>;
  timestamp: string;
}

export interface DatabaseHealth {
  status: 'connected' | 'disconnected' | 'error';
  latency?: number;
  error?: string;
}



export interface HealthStatus {
  status: 'healthy' | 'degraded' | 'unhealthy';
  databases: Record<string, DatabaseHealth>;
  timestamp: string;
  summary: {
    total: number;
    connected: number;
    error: number;
  };
}

export interface PoolMetrics {
  total: number;
  available: number;
  borrowed: number;
  pending: number;
}