import { DatabaseConfig, QueryResult } from '../../types';
import { Pool, Options } from 'generic-pool';

export abstract class DatabaseAdapter {
  protected pool: Pool<any>;
  protected config: DatabaseConfig;

  constructor(config: DatabaseConfig) {
    this.config = config;
    this.pool = this.createConnectionPool();
  }

  protected abstract createConnectionPool(): Pool<any>;
  protected abstract createSingleConnection(): Promise<any>;
  protected abstract destroyConnection(connection: any): Promise<void>;
  protected abstract validateConnection(connection: any): Promise<boolean>;

  protected getPoolConfig(): Options {
    return {
      max: this.config.connectionLimit || 10,
      min: 2,
      idleTimeoutMillis: this.config.idleTimeout || 30000,
      acquireTimeoutMillis: this.config.acquireTimeout || 30000,
      testOnBorrow: true,
      autostart: true
    };
  }
  
  public async executeQuery(query: string, params?: any[]): Promise<QueryResult> {
    const connection = await this.pool.acquire();
    const startTime = Date.now();

    try {
      const result = await this.execute(connection, query, params);
      const executionTime = Date.now() - startTime;

      return {
        success: true,
        data: result.rows,
        metadata: {
          rowCount: result.rowCount || 0,
          columns: result.metadata || [],
          executionTime
        },
        requestId: this.generateRequestId(),
        timestamp: new Date().toISOString()
      };
    } catch (error: any) {
      return {
        success: false,
        error: error.message,
        requestId: this.generateRequestId(),
        timestamp: new Date().toISOString()
      };
    } finally {
      await this.pool.release(connection);
    }
  }

  protected abstract execute(connection: any, query: string, params?: any[]): Promise<any>;

  public async testConnection(): Promise<boolean> {
    try {
      const connection = await this.createSingleConnection();
      await this.destroyConnection(connection);
      return true;
    } catch {
      return false;
    }
  }

  public getMetrics() {
    // Cast to 'any' to access methods if TypeScript definitions are incomplete
    const poolAny = this.pool as any;

    // Use generic-pool's methods to get metrics
    const total = typeof poolAny.getPoolSize === 'function' ? poolAny.getPoolSize() : 0;
    const available = typeof poolAny.availableObjectsCount === 'function' ? poolAny.availableObjectsCount() : 0;
    const pending = typeof poolAny.waitingClientsCount === 'function' ? poolAny.waitingClientsCount() : 0;
    const borrowed = total - available; // Calculate borrowed connections

    return {
      total,
      available,
      borrowed,
      pending
    };
  }

  // Alternative: More explicit version with type checking
  public getMetricsV2() {
    const poolAny = this.pool as any;
    
    // Initialize metrics with default values
    const metrics = {
      total: 0,
      available: 0,
      borrowed: 0,
      pending: 0
    };

    // Safely get each metric
    if (typeof poolAny.getPoolSize === 'function') {
      metrics.total = poolAny.getPoolSize();
    }
    
    if (typeof poolAny.availableObjectsCount === 'function') {
      metrics.available = poolAny.availableObjectsCount();
    }
    
    if (typeof poolAny.waitingClientsCount === 'function') {
      metrics.pending = poolAny.waitingClientsCount();
    }
    
    // Calculate borrowed connections
    metrics.borrowed = metrics.total - metrics.available;
    
    return metrics;
  }

  public async close(): Promise<void> {
    await this.pool.drain();
    await this.pool.clear();
  }

  private generateRequestId(): string {
    return `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
  }
}