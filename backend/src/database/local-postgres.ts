// backend/src/database/local-postgres.ts

import { Pool, PoolConfig } from 'pg';
import { Logger } from './inspection/postgreSql-inspector';

/**
 * PostgreSQL Connection Manager for Local Instance
 * Handles robust connection logic with fallback strategies and proper error handling
 */

export interface LocalPostgresConfig {
  host?: string;
  port?: number;
  user?: string;
  password?: string;
  database?: string;
  schema?: string;
  connectionLimit?: number;
  idleTimeout?: number;
  acquireTimeout?: number;
  retryAttempts?: number;
  retryDelay?: number;
}

export class LocalPostgresConnectionManager {
  private static instance: LocalPostgresConnectionManager;
  private pool: Pool | null = null;
  private isConnected: boolean = false;
  private connectionAttempts: number = 0;
  private maxRetryAttempts: number;
  private retryDelay: number;

  private defaultConfig: LocalPostgresConfig = {
    host: process.env.DB_HOST || 'localhost',
    port: parseInt(process.env.DB_PORT || '5432'),
    // Use current system user instead of hardcoded 'postgres'
    user: process.env.DB_USER || this.getSystemUser(),
    password: process.env.DB_PASSWORD || '',
    database: process.env.DB_NAME || 'postgres',
    schema: process.env.DB_SCHEMA || 'public',
    connectionLimit: parseInt(process.env.DB_CONNECTION_LIMIT || '10'),
    idleTimeout: parseInt(process.env.DB_IDLE_TIMEOUT || '30000'),
    acquireTimeout: parseInt(process.env.DB_ACQUIRE_TIMEOUT || '30000'),
    retryAttempts: parseInt(process.env.DB_RETRY_ATTEMPTS || '3'),
    retryDelay: parseInt(process.env.DB_RETRY_DELAY || '2000')
  };

  private constructor() {
    // Initialize with default values
    this.maxRetryAttempts = this.defaultConfig.retryAttempts || 3;
    this.retryDelay = this.defaultConfig.retryDelay || 2000;
  }

  /**
   * Get singleton instance
   */
  public static getInstance(): LocalPostgresConnectionManager {
    if (!LocalPostgresConnectionManager.instance) {
      LocalPostgresConnectionManager.instance = new LocalPostgresConnectionManager();
    }
    return LocalPostgresConnectionManager.instance;
  }

  /**
   * Get current system user for default PostgreSQL username
   */
  private getSystemUser(): string {
    // Try environment variables first
    const userFromEnv = process.env.USER || process.env.USERNAME || process.env.LOGNAME;
    
    if (userFromEnv) {
      return userFromEnv;
    }
    
    // Try to get user via OS command (Node.js environment only)
    try {
      const { execSync } = require('child_process');
      
      if (process.platform === 'win32') {
        // Windows
        return execSync('whoami', { encoding: 'utf8' }).trim();
      } else {
        // Unix/Linux/macOS
        return execSync('id -un', { encoding: 'utf8' }).trim();
      }
    } catch (error) {
      Logger.warn('Could not determine system user:', error instanceof Error ? error.message : String(error));
    }
    
    // Fallback to 'postgres' for compatibility
    return 'postgres';
  }

  /**
   * Initialize connection with retry logic
   */
  public async initializeConnection(config?: Partial<LocalPostgresConfig>): Promise<void> {
    const finalConfig = { ...this.defaultConfig, ...config };
    
    // Update retry settings from config
    this.maxRetryAttempts = finalConfig.retryAttempts || this.maxRetryAttempts;
    this.retryDelay = finalConfig.retryDelay || this.retryDelay;

    Logger.info('Attempting to connect to local PostgreSQL instance...');
    console.log('📊 PostgreSQL Connection Parameters:', {
      host: finalConfig.host,
      port: finalConfig.port,
      user: finalConfig.user,
      database: finalConfig.database,
      schema: finalConfig.schema,
      authentication: finalConfig.password ? 'password' : 'trust (no password)'
    });

    for (let attempt = 1; attempt <= this.maxRetryAttempts; attempt++) {
      try {
        await this.attemptConnection(finalConfig, attempt);
        return; // Connection successful
      } catch (error) {
        this.connectionAttempts++;
        const errorMessage = this.extractErrorMessage(error);
        
        console.error(`❌ Connection attempt ${attempt}/${this.maxRetryAttempts} failed: ${errorMessage}`);
        
        if (attempt < this.maxRetryAttempts) {
          console.log(`⏳ Retrying in ${this.retryDelay / 1000} seconds... (Attempt ${attempt + 1}/${this.maxRetryAttempts})`);
          await this.delay(this.retryDelay);
        } else {
          // All retries failed
          this.handleConnectionFailure(errorMessage, finalConfig);
        }
      }
    }
  }

  /**
   * Attempt a single connection
   */
  private async attemptConnection(config: LocalPostgresConfig, attempt: number): Promise<void> {
    const poolConfig: PoolConfig = {
      host: config.host,
      port: config.port,
      user: config.user,
      password: config.password,
      database: config.database,
      max: config.connectionLimit,
      ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
      application_name: 'database-metadata-wizard-backend',
      idleTimeoutMillis: config.idleTimeout || 1800000,
      connectionTimeoutMillis: config.acquireTimeout,
      keepAlive: true,
      keepAliveInitialDelayMillis: 1800000,
    };

    // Create new pool
    this.pool = new Pool(poolConfig);

    // Test connection with a simple query
    const client = await this.pool.connect();
    try {
      const result = await client.query('SELECT version() as version, current_database() as database, current_schema() as schema, current_user as user');
      
      this.isConnected = true;
      this.connectionAttempts = 0; // Reset attempts on successful connection
      
      console.log(`✅ Successfully connected to PostgreSQL (Attempt ${attempt}/${this.maxRetryAttempts})`);
      console.log(`   Database: ${result.rows[0].database}`);
      console.log(`   Schema: ${result.rows[0].schema}`);
      console.log(`   User: ${result.rows[0].user}`);
      console.log(`   Version: ${result.rows[0].version.split(',')[0]}`);
      
      // Set up pool event listeners for monitoring
      this.setupPoolEventListeners();
    } finally {
      client.release();
    }
  }

  /**
   * Extract meaningful error message from connection error
   */
  private extractErrorMessage(error: any): string {
    if (error.code) {
      switch (error.code) {
        case 'ECONNREFUSED':
          const host = error.address || 'localhost';
          const port = error.port || 5432;
          return `Connection refused to ${host}:${port}. Make sure PostgreSQL is running.`;
        case 'ETIMEDOUT':
          return 'Connection timed out. Check if PostgreSQL is accessible and not blocked by firewall.';
        case 'ENOTFOUND':
          const hostname = error.hostname || 'localhost';
          return `Host ${hostname} not found. Check your network configuration.`;
        case '28P01': // PostgreSQL authentication error
          return 'Authentication failed. Check username/password.';
        case '28000': // PostgreSQL role doesn't exist
          const attemptedUser = error.message.match(/role "([^"]+)"/)?.[1] || 'postgres';
          const currentUser = this.getSystemUser();
          return `Role "${attemptedUser}" does not exist. ` +
                 `Your standalone PostgreSQL was built for user "${currentUser}". ` +
                 `Try setting DB_USER=${currentUser} or running: ./standalone-pg-builder.sh --full`;
        case '3D000': // PostgreSQL database doesn't exist
          return 'Database does not exist.';
        default:
          return `PostgreSQL error (${error.code}): ${error.message}`;
      }
    }
    return error.message || 'Unknown connection error';
  }

  /**
   * Handle connection failure with informative message and exit
   */
  private handleConnectionFailure(errorMessage: string, config: LocalPostgresConfig): never {
    const currentUser = this.getSystemUser();
    
    console.error('');
    console.error('❌ FATAL: Failed to connect to local PostgreSQL database after ' + this.maxRetryAttempts + ' attempts.');
    console.error('');
    console.error('📋 Connection Details:');
    console.error(`   - Host: ${config.host}`);
    console.error(`   - Port: ${config.port}`);
    console.error(`   - Database: ${config.database}`);
    console.error(`   - Attempted User: ${config.user}`);
    console.error(`   - Current System User: ${currentUser}`);
    console.error('');
    console.error(`🔍 Error Details: ${errorMessage}`);
    console.error('');
    
    // Special guidance for standalone PostgreSQL
    if (errorMessage.includes('role "postgres" does not exist')) {
      console.error('🚨 STANDALONE POSTGRESQL DETECTED:');
      console.error('');
      console.error('   Your application is trying to connect as user "postgres"');
      console.error('   but your standalone PostgreSQL was built for your current user.');
      console.error('');
      console.error('   QUICK FIXES:');
      console.error('   1. Run the standalone PostgreSQL builder first:');
      console.error('        ./standalone-pg-builder.sh --full');
      console.error('');
      console.error('   2. Or set the correct user in environment:');
      console.error(`        export DB_USER=${currentUser}`);
      console.error('        export DB_PASSWORD=');
      console.error('        npm run dev:full');
      console.error('');
      console.error('   3. Or create a .env file with:');
      console.error(`        DB_USER=${currentUser}`);
      console.error('        DB_PASSWORD=');
      console.error('');
      console.error('   Then restart the application.');
      console.error('');
    } else {
      console.error('🚨 Required Action:');
      console.error('   1. Make sure PostgreSQL is installed and running on your system');
      console.error('   2. Verify PostgreSQL service is started:');
      console.error('        # For systemd (Ubuntu/Debian):');
      console.error('        sudo systemctl status postgresql');
      console.error('        sudo systemctl start postgresql');
      console.error('');
      console.error('        # For macOS with Homebrew:');
      console.error('        brew services list');
      console.error('        brew services start postgresql');
      console.error('');
      console.error('        # For Windows:');
      console.error('        Open Services (services.msc) and start "PostgreSQL" service');
      console.error('');
      console.error('   3. Check if PostgreSQL is listening on the correct port (default: 5432):');
      console.error('        sudo netstat -tlnp | grep 5432');
      console.error('');
      console.error('   4. Verify database credentials:');
      console.error(`        - Database name: "${config.database}"`);
      console.error(`        - Username: "${config.user}"`);
      console.error('        - Password: [check your configuration]');
      console.error('');
      console.error('   5. Ensure PostgreSQL accepts connections from localhost:');
      console.error('        Check pg_hba.conf and postgresql.conf configuration files');
    }
    
    console.error('');
    console.error('💡 Tip: You can also set environment variables to override defaults:');
    console.error('   DB_HOST=localhost');
    console.error('   DB_PORT=5432');
    console.error(`   DB_USER=${currentUser}  # Your system username`);
    console.error('   DB_PASSWORD=  # Leave empty for trust authentication');
    console.error('   DB_NAME=postgres');
    console.error('   DB_SCHEMA=public');
    console.error('');
    console.error('After configuring PostgreSQL, restart this backend service.');
    console.error('');
    
    process.exit(1);
  }

  /**
   * Setup pool event listeners for monitoring
   */
  private setupPoolEventListeners(): void {
    if (!this.pool) return;

    this.pool.on('connect', () => {
      Logger.debug('New client connected to pool');
    });

    this.pool.on('acquire', () => {
      Logger.debug('Client acquired from pool');
    });

    this.pool.on('release', () => {
      Logger.debug('Client released to pool');
    });

    this.pool.on('remove', () => {
      Logger.warn('Client removed from pool');
    });

    this.pool.on('error', (err) => {
      Logger.error('Pool error:', err.message);
      // Attempt to reconnect if pool error occurs
      this.handlePoolError();
    });
  }

  /**
   * Handle pool errors with reconnection logic
   */
  private async handlePoolError(): Promise<void> {
    if (this.isConnected) {
      this.isConnected = false;
      Logger.warn('Pool connection lost. Attempting to reinitialize...');
      
      try {
        await this.initializeConnection();
        Logger.info('Pool reinitialized successfully');
      } catch (error) {
        Logger.error('Failed to reinitialize pool:', error instanceof Error ? error.message : String(error));
      }
    }
  }

  /**
   * Get the connection pool for use in application
   */
  public getPool(): Pool {
    if (!this.pool || !this.isConnected) {
      throw new Error('PostgreSQL connection pool is not initialized. Call initializeConnection() first.');
    }
    return this.pool;
  }

  /**
   * Get connection status
   */
  public getStatus(): {
    connected: boolean;
    connectionAttempts: number;
    poolSize?: number;
    idleCount?: number;
    waitingCount?: number;
  } {
    const status: any = {
      connected: this.isConnected,
      connectionAttempts: this.connectionAttempts
    };

    if (this.pool) {
      status.poolSize = this.pool.totalCount;
      status.idleCount = this.pool.idleCount;
      status.waitingCount = this.pool.waitingCount;
    }

    return status;
  }

  /**
   * Execute a query using the pool
   */
  public async query(sql: string, params?: any[]): Promise<any> {
    if (!this.isConnected || !this.pool) {
      throw new Error('Database connection not established');
    }

    const client = await this.pool.connect();

    // Attach a one-time error handler to this specific client
    const errorHandler = (err: Error) => {
      Logger.error(`Client connection error during query: ${err.message}`);
      // The client is now unusable; release it (pool will discard it)
      client.release(err);
    };
    client.once('error', errorHandler);

    try {
      const startTime = Date.now();
      const result = await client.query(sql, params);
      const executionTime = Date.now() - startTime;
      Logger.debug(`Query executed in ${executionTime}ms: ${sql.substring(0, 100)}...`);
      return result;
    } catch (queryError) {
      // Query execution error (SQL syntax, etc.) – not a connection error
      throw queryError;
    } finally {
      // Remove the error handler and release the client
      client.off('error', errorHandler);
      client.release();
    }
  }

  /**
   * Test current connection
   */
  public async testConnection(): Promise<boolean> {
    if (!this.isConnected || !this.pool) {
      return false;
    }

    try {
      const client = await this.pool.connect();
      try {
        await client.query('SELECT 1 as test');
        return true;
      } finally {
        client.release();
      }
    } catch (error) {
      this.isConnected = false;
      Logger.error('Connection test failed:', error instanceof Error ? error.message : String(error));
      return false;
    }
  }

  /**
   * Gracefully shutdown the connection pool
   */
  public async shutdown(): Promise<void> {
    if (this.pool) {
      Logger.info('Shutting down PostgreSQL connection pool...');
      try {
        await this.pool.end();
        this.isConnected = false;
        this.pool = null;
        Logger.info('PostgreSQL connection pool shut down successfully');
      } catch (error) {
        Logger.error('Error shutting down pool:', error instanceof Error ? error.message : String(error));
      }
    }
  }

  /**
   * Utility method for delays
   */
  private delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * Get default configuration
   */
  public getDefaultConfig(): LocalPostgresConfig {
    return { ...this.defaultConfig };
  }

  // ==================== ENVIRONMENT VALIDATION & PROVISIONING ====================

  /**
   * Ensure required PostgreSQL extensions are installed.
   */
  public async ensureExtensions(): Promise<void> {
    const extensions = ['postgres_fdw', 'file_fdw'];
    for (const ext of extensions) {
      await this.query(`CREATE EXTENSION IF NOT EXISTS "${ext}"`);
      Logger.info(`✅ Extension "${ext}" verified/created`);
    }
  }

  /**
   * Ensure all required foreign servers exist (using file_fdw wrapper).
   */
  public async ensureForeignServers(): Promise<void> {
    const servers = [
      'fdw_excel',
      'fdw_xml',
      'fdw_delimited',
      'fdw_multiformat',
      'fdw_regex',
      'fdw_ldif',
      'fdw_positional',
      'fdw_schema'
    ];

    for (const server of servers) {
      // Check if server already exists
      const check = await this.query(
        `SELECT 1 FROM pg_foreign_server WHERE srvname = $1`,
        [server]
      );
      if (check.rowCount === 0) {
        await this.query(
          `CREATE SERVER "${server}" FOREIGN DATA WRAPPER file_fdw`
        );
        Logger.info(`✅ Foreign server "${server}" created`);
      } else {
        Logger.info(`✅ Foreign server "${server}" already exists`);
      }
    }
  }

  /**
   * Ensure core application tables exist.
   */
  public async ensureTables(): Promise<void> {
    // Create canvases table
    await this.query(`
      CREATE TABLE IF NOT EXISTS canvases (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        data JSONB NOT NULL,
        version INTEGER DEFAULT 1,
        owner_id VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    Logger.info(`✅ Table "canvases" verified/created`);

    // Create data_source_metadata table
    await this.query(`
      CREATE TABLE IF NOT EXISTS data_source_metadata (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        type VARCHAR(50) NOT NULL,
        file_path TEXT,
        foreign_table_name VARCHAR(255) NOT NULL,
        connection_id VARCHAR(255),
        options JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    Logger.info(`✅ Table "data_source_metadata" verified/created`);
  }
}

// ==================== SINGLETON INSTANCE ====================
export const localPostgres = LocalPostgresConnectionManager.getInstance();

// ==================== INITIALIZATION FUNCTION ====================
export async function initializeLocalPostgresConnection(
  config?: Partial<LocalPostgresConfig>
): Promise<void> {
  // 1. Connect to PostgreSQL
  await localPostgres.initializeConnection(config);

  // 2. After successful connection, validate and provision required objects
  try {
    await localPostgres.ensureExtensions();
    await localPostgres.ensureForeignServers();
    await localPostgres.ensureTables();
    Logger.info('✅ PostgreSQL environment fully validated and ready');
  } catch (error) {
    Logger.error('❌ Failed to setup PostgreSQL environment:', error);
    // Exit with error to prevent backend from running in an inconsistent state
    process.exit(1);
  }
}

// Helper function to get pool for use in application
export function getPostgresPool(): Pool {
  return localPostgres.getPool();
}

export default localPostgres;