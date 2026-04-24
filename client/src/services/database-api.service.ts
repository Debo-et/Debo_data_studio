// frontend/src/services/database-api.service.ts

import axios from 'axios';
import {
  DatabaseType,
  ClientDatabaseConfig,
  ClientColumnMetadata,
  ClientTableInfo,
  ClientInspectionOptions,
  ClientQueryExecutionOptions,
  ClientQueryResult,
  ClientDatabaseVersionInfo,
  ClientConnectionInfo,
  ClientTestConnectionResult,
  ClientConnectResult,
  ClientTableListResult,
  ClientQueryExecutionResult,
  ClientDatabaseInfoResult,
  ClientDisconnectResult,
  TestConnectionRequest,
  ConnectRequest,
  GetTablesRequest,
  TransactionRequest,
  GetConstraintsRequest,
  GetSchemasRequest,
  BatchDisconnectRequest
} from './database-api.types';

// Import foreign table utilities
import {
  generateForeignServerSQL,
  createForeignTableInPostgres
} from '../api/postgres-foreign-table';

// ===========================================================================
// Database API Service - WITH FOREIGN TABLE FUNCTIONALITY
// ===========================================================================

export class DatabaseApiService {
  private api: any;
  public baseUrl: string;

  constructor(baseUrl?: string) {
    // Smart baseUrl detection with proper fallbacks
    this.baseUrl = baseUrl || 
                  process.env.REACT_APP_BACKEND_URL || 
                  (process.env.NODE_ENV === 'production' ? 
                    window.location.origin : 
                    'http://localhost:3000');
    
    console.log(`📡 DatabaseApiService initialized with baseURL: ${this.baseUrl}`);
    console.log(`🌐 Frontend origin: ${window.location.origin}`);
    console.log(`🔧 Node environment: ${process.env.NODE_ENV}`);
    
    this.api = axios.create({
      baseURL: this.baseUrl,
      timeout: 10000,
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
      },
      withCredentials: false,
    });

    // Enhanced Request interceptor
    this.api.interceptors.request.use(
      (config: any) => {
        // Add Origin header for CORS
        if (typeof window !== 'undefined') {
          config.headers['Origin'] = window.location.origin;
        }
        
        // Add token if exists
        const token = localStorage.getItem('auth_token');
        if (token) {
          config.headers.Authorization = `Bearer ${token}`;
        }
        
        // Log request for debugging
        if (process.env.NODE_ENV === 'development') {
          console.log(`🚀 API Request: ${config.method?.toUpperCase()} ${config.url}`);
        }
        
        return config;
      },
      (error: any) => {
        console.error('❌ Request interceptor error:', error);
        return Promise.reject(error);
      }
    );

    // Enhanced Response interceptor
    this.api.interceptors.response.use(
      (response: any) => {
        if (process.env.NODE_ENV === 'development') {
          console.log(`✅ API Response: ${response.status} ${response.config.url}`);
        }
        return response;
      },
      (error: any) => {
        console.error('💥 API Error:', {
          url: error.config?.url,
          method: error.config?.method,
          baseURL: error.config?.baseURL,
          status: error.response?.status,
          statusText: error.response?.statusText,
          message: error.message,
          code: error.code
        });
        
        // Special handling for CORS/Network errors
        if (error.message && (
          error.message.includes('Network Error') || 
          error.message.includes('Failed to fetch') ||
          error.code === 'ERR_NETWORK' ||
          error.code === 'ECONNABORTED'
        )) {
          console.error('🛑 NETWORK/CORS ERROR DETECTED!');
          console.error('  Backend URL:', this.baseUrl);
          console.error('  Frontend Origin:', window.location.origin);
          console.error('  Error:', error.message);
        }
        
        return Promise.reject(error);
      }
    );
  }

  // ===========================================================================
  // Health Check
  // ===========================================================================

  async testHealth(): Promise<any> {
    try {
      console.log(`❤️ Testing backend health at ${this.baseUrl}/health...`);
      
      // Use fetch as fallback to check if it's an axios-specific issue
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 5000);
      
      try {
        const response = await fetch(`${this.baseUrl}/health`, {
          method: 'GET',
          mode: 'cors',
          headers: {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
          },
          signal: controller.signal
        });
        
        clearTimeout(timeoutId);
        
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const data = await response.json();
        console.log(`✅ Health check via fetch:`, data);
        
        // Also try with axios for consistency
        const axiosResponse = await this.api.get('/health');
        console.log(`✅ Health check via axios:`, axiosResponse.data);
        
        return data;
      } catch (fetchError: any) {
        clearTimeout(timeoutId);
        console.warn('Fetch health check failed, trying axios directly...');
        
        // Try axios directly
        const axiosResponse = await this.api.get('/health', { timeout: 5000 });
        console.log(`✅ Health check succeeded via axios retry:`, axiosResponse.data);
        return axiosResponse.data;
      }
      
    } catch (error: any) {
      console.error('❌ Health check failed:', error.message || error);
      
      // Provide more specific error messages
      let errorMessage = 'Health check failed';
      if (error.code === 'ECONNABORTED') {
        errorMessage = `Timeout connecting to backend at ${this.baseUrl}. Is the server running?`;
      } else if (error.code === 'ERR_NETWORK' || error.message?.includes('Network Error')) {
        errorMessage = `Cannot connect to backend at ${this.baseUrl}. Check: 1) Server running on port 3000, 2) CORS configured properly`;
      } else if (error.response) {
        errorMessage = `Backend responded with ${error.response.status}: ${error.response.statusText}`;
      }
      
      throw new Error(errorMessage);
    }
  }

  // ===========================================================================
  // Connection Management
  // ===========================================================================

  async testConnection(
    dbType: DatabaseType,
    config: ClientDatabaseConfig
  ): Promise<ClientTestConnectionResult> {
    const request: TestConnectionRequest = { dbType, config };
    try {
      console.log(`🧪 Testing connection to ${dbType} at ${config.host}:${config.port}`);
      const response = await this.api.post('/api/database/test-connection', request);
      console.log(`✅ Connection test successful:`, response.data);
      return response.data;
    } catch (error: any) {
      console.error(`❌ Connection test failed:`, error);
      return {
        success: false,
        error: this.getErrorMessage(error),
      };
    }
  }

  async connect(
    dbType: DatabaseType,
    config: ClientDatabaseConfig
  ): Promise<ClientConnectResult> {
    const request: ConnectRequest = { dbType, config };
    try {
      console.log(`🔌 Connecting to ${dbType}...`);
      const response = await this.api.post('/api/database/connect', request);
      console.log(`✅ Connection successful:`, response.data);
      return response.data;
    } catch (error: any) {
      console.error(`❌ Connection failed:`, error);
      return {
        connectionId: '',
        success: false,
        error: this.getErrorMessage(error),
      };
    }
  }

  async disconnect(connectionId: string): Promise<ClientDisconnectResult> {
    try {
      console.log(`🔌 Disconnecting ${connectionId}...`);
      const response = await this.api.delete(`/api/database/${connectionId}`);
      console.log(`✅ Disconnect successful:`, response.data);
      return response.data;
    } catch (error: any) {
      console.error(`❌ Disconnect failed:`, error);
      return {
        success: false,
        error: this.getErrorMessage(error),
      };
    }
  }

  async getActiveConnections(): Promise<ClientConnectionInfo[]> {
    try {
      console.log(`📊 Getting active connections...`);
      const response = await this.api.get('/api/database/connections/active');
      console.log(`✅ Active connections:`, response.data);
      return response.data.connections || [];
    } catch (error: any) {
      console.error('❌ Failed to get active connections:', error);
      return [];
    }
  }

  // ===========================================================================
  // Schema Inspection
  // ===========================================================================

  async getTables(
    connectionId: string,
    options?: ClientInspectionOptions
  ): Promise<ClientTableListResult> {
    const request: GetTablesRequest = { connectionId, options };
    try {
      console.log(`📋 Getting tables for connection ${connectionId}...`);
      const response = await this.api.post('/api/database/tables', request);
      console.log(`✅ Tables retrieved: ${response.data.tables?.length || 0} tables`);
      return response.data;
    } catch (error: any) {
      console.error('❌ Failed to get tables:', error);
      return {
        tables: [],
        success: false,
        error: this.getErrorMessage(error),
      };
    }
  }

  async getDatabaseInfo(connectionId: string): Promise<ClientDatabaseInfoResult> {
    try {
      console.log(`ℹ️ Getting database info for connection ${connectionId}...`);
      const response = await this.api.get(`/api/database/${connectionId}/info`);
      console.log(`✅ Database info:`, response.data);
      return response.data;
    } catch (error: any) {
      console.error('❌ Failed to get database info:', error);
      return {
        info: { version: '', name: '' },
        success: false,
        error: this.getErrorMessage(error),
      };
    }
  }

  // ===========================================================================
  // Query Execution
  // ===========================================================================

  async executeQuery(
  connectionId: string,
  sql: string,
  options?: ClientQueryExecutionOptions
): Promise<ClientQueryExecutionResult> {
  try {
    console.log(`⚡ Executing query for connection ${connectionId}...`);
    
    const requestBody: any = { sql };
    // ✅ Always include params, default to []
    requestBody.params = options?.params || [];
    
    // Include other options
    if (options?.maxRows) requestBody.maxRows = options.maxRows;
    if (options?.timeout) requestBody.timeout = options.timeout;

    const response = await this.api.post(`/api/database/${connectionId}/query`, requestBody);
    console.log(`✅ Query executed: ${response.data.rowCount || 0} rows affected`);
    return response.data;
  } catch (error: any) {
    console.error('❌ Query execution failed:', error);
    return {
      result: null,
      success: false,
      error: this.getErrorMessage(error),
    };
  }
}

  async executeTransaction(
    connectionId: string,
    queries: Array<{ sql: string; params?: any[] }>
  ): Promise<ClientQueryResult[]> {
    const request: TransactionRequest = { connectionId, queries };
    try {
      console.log(`🔄 Executing transaction with ${queries.length} queries...`);
      const response = await this.api.post('/api/database/transaction', request);
      console.log(`✅ Transaction completed successfully`);
      return response.data;
    } catch (error: any) {
      console.error('❌ Transaction failed:', error);
      throw new Error(`Transaction failed: ${this.getErrorMessage(error)}`);
    }
  }

  async executeRawQuery(
    connectionId: string,
    sql: string,
    params: any[] = []
  ): Promise<any> {
    try {
      console.log(`⚡ Executing raw query for ${connectionId}...`);
      const request = { sql, params };
      const response = await this.api.post(`/api/database/${connectionId}/raw-query`, request);
      console.log(`✅ Raw query executed`);
      return response.data;
    } catch (error: any) {
      console.error('❌ Raw query execution failed:', error);
      return {
        result: null,
        success: false,
        error: this.getErrorMessage(error),
      };
    }
  }

  // ===========================================================================
  // FOREIGN TABLE OPERATIONS - UPDATED
  // ===========================================================================

  /**
   * Create a foreign server in PostgreSQL
   */
  async createForeignServer(
    connectionId: string,
    serverName: string,
    dbType: DatabaseType,
    config: ClientDatabaseConfig
  ): Promise<{ success: boolean; error?: string; serverName?: string }> {
    try {
      // 1. Create server (without user/password)
      const serverSql = generateForeignServerSQL(serverName, dbType, {
        host: config.host,
        port: config.port,
        dbname: config.dbname,
        // user/password omitted
      });
      console.log(`📝 Creating foreign server: ${serverName}`);
      const serverResult = await this.executeQuery(connectionId, serverSql);
      if (!serverResult.success) {
        // If server already exists, we can continue; otherwise fail
        if (!serverResult.error || !serverResult.error.includes('already exists')) {
          return { success: false, error: serverResult.error };
        }
      }

      // 2. For PostgreSQL, create user mapping with credentials
      const isPostgres = dbType === 'postgresql' || dbType === 'postgres';
      if (isPostgres && config.user && config.password) {
        const userMappingSql = `CREATE USER MAPPING FOR CURRENT_USER
          SERVER ${serverName}
          OPTIONS (user '${config.user}', password '${config.password}');`;
        const mappingResult = await this.executeQuery(connectionId, userMappingSql);
        if (!mappingResult.success) {
          // If mapping already exists, ignore; otherwise warn
          if (!mappingResult.error || !mappingResult.error.includes('already exists')) {
            console.warn(`User mapping creation failed: ${mappingResult.error}`);
          }
        }
      }

      return { success: true, serverName };
    } catch (error: any) {
      console.error('❌ Failed to create foreign server:', error);
      return { success: false, error: error.message };
    }
  }

  /**
   * Create a foreign table in PostgreSQL
   * @param connectionId The PostgreSQL connection ID
   * @param tableName Name of the foreign table to create
   * @param columns Column definitions
   * @param fileType Type of the source (e.g., 'postgresql', 'mysql', 'delimited', etc.)
   * @param filePath Path to the file (empty for database sources)
   * @param options Additional options (e.g., delimiter, header, etc.)
   * @param serverName Optional server name; if provided, the foreign table will be created on that server
   */
  async createForeignTable(
    connectionId: string,
    tableName: string,
    columns: Array<{
      name: string;
      type: string;
      length?: number;
      precision?: number;
      scale?: number;
      nullable?: boolean;
      defaultValue?: string;
    }>,
    fileType: string,
    filePath: string,
    options?: Record<string, string>,
    serverName?: string
  ): Promise<{
    success: boolean;
    error?: string;
    tableName?: string;
    sql?: string;
    warnings?: string[];
  }> {
    try {
      console.log(`📝 Creating foreign table ${tableName} for ${fileType} file...`);
      const result = await createForeignTableInPostgres(
        this,
        connectionId,
        tableName,
        columns,
        fileType,
        filePath,
        options || {},
        serverName
      );
      console.log(`✅ Foreign table creation response:`, result);
      return result;
    } catch (error: any) {
      console.error('❌ Failed to create foreign table:', error);
      return {
        success: false,
        error: this.getErrorMessage(error),
      };
    }
  }

  /**
   * NEW: Create a foreign table via backend endpoint.
   * This uses the backend's dedicated route that handles SQL generation and execution
   * directly with the local PostgreSQL pool, avoiding adapter issues.
   */
  async createForeignTableViaBackend(
    connectionId: string,
    tableName: string,
    columns: Array<{
      name: string;
      type: string;
      length?: number;
      precision?: number;
      scale?: number;
      nullable?: boolean;
      defaultValue?: string;
    }>,
    fileType: string,
    filePath: string,
    options?: Record<string, string>,
    serverName?: string
  ): Promise<{
    success: boolean;
    error?: string;
    tableName?: string;
    sql?: string;
    warnings?: string[];
  }> {
    try {
      const payload = {
        connectionId,
        tableName,
        columns,
        fileType,
        filePath,
        options: options || {},
        serverName
      };
      console.log(`📝 Creating foreign table via backend: ${tableName}`);
      const response = await this.api.post('/api/database/create-foreign-table', payload);
      console.log(`✅ Foreign table creation response:`, response.data);
      return response.data;
    } catch (error: any) {
      console.error('❌ Failed to create foreign table via backend:', error);
      return {
        success: false,
        error: this.getErrorMessage(error)
      };
    }
  }

  /**
   * List all foreign tables in the database
   */
  async listForeignTables(): Promise<{
    success: boolean;
    tables?: Array<{
      schemaname: string;
      tablename: string;
      server_name: string;
    }>;
    error?: string;
  }> {
    try {
      console.log(`📋 Getting foreign tables list...`);
      const response = await this.api.get('/api/database/foreign-tables');
      console.log(`✅ Foreign tables retrieved: ${response.data.tables?.length || 0} tables`);
      return response.data;
    } catch (error: any) {
      console.error('❌ Failed to get foreign tables:', error);
      return {
        success: false,
        error: this.getErrorMessage(error),
      };
    }
  }

  /**
   * Drop a foreign table
   */
  async dropForeignTable(tableName: string): Promise<{
    success: boolean;
    error?: string;
  }> {
    try {
      console.log(`🗑️ Dropping foreign table ${tableName}...`);
      const response = await this.api.delete(`/api/database/foreign-tables/${encodeURIComponent(tableName)}`);
      console.log(`✅ Foreign table dropped:`, response.data);
      return response.data;
    } catch (error: any) {
      console.error('❌ Failed to drop foreign table:', error);
      return {
        success: false,
        error: this.getErrorMessage(error),
      };
    }
  }

  // ===========================================================================
  // NEW: CASCADING DELETE METHODS
  // ===========================================================================

  /**
   * Drops a foreign table and its associated metadata record.
   * Returns the foreign server name for possible further cleanup.
   */
  async dropForeignTableCascade(
    connectionId: string,
    tableName: string
  ): Promise<{ success: boolean; error?: string; serverName?: string }> {
    try {
      console.log(`🗑️ Cascading delete for foreign table: ${tableName}`);

      // First, retrieve the foreign server name
      const serverRes = await this.executeQuery(
        connectionId,
        `SELECT srvname 
         FROM pg_foreign_table ft
         JOIN pg_foreign_server fs ON ft.ftserver = fs.oid
         JOIN pg_class c ON ft.ftrelid = c.oid
         WHERE c.relname = $1`,
        { params: [tableName] }
      );
      const serverName = serverRes.success && serverRes.rows?.[0]?.srvname || undefined;

      // Drop foreign table with CASCADE
      const dropSql = `DROP FOREIGN TABLE IF EXISTS ${this.sanitizeIdentifier(tableName)} CASCADE`;
      const dropResult = await this.executeQuery(connectionId, dropSql);
      if (!dropResult.success) {
        throw new Error(dropResult.error || 'Failed to drop foreign table');
      }

      // Delete metadata record
      const deleteMetaSql = `DELETE FROM data_source_metadata WHERE foreign_table_name = $1`;
      const metaResult = await this.executeQuery(connectionId, deleteMetaSql, { params: [tableName] });
      if (!metaResult.success) {
        console.warn(`Could not delete metadata for ${tableName}:`, metaResult.error);
      }

      console.log(`✅ Cascading delete completed for ${tableName}, server: ${serverName || 'none'}`);
      return { success: true, serverName };
    } catch (error: any) {
      console.error('❌ dropForeignTableCascade failed:', error);
      return { success: false, error: error.message };
    }
  }

  /**
   * Drops a foreign server and its user mappings, but only if no foreign tables remain.
   */
  async dropForeignServerIfUnused(
    connectionId: string,
    serverName: string
  ): Promise<{ success: boolean; error?: string }> {
    try {
      console.log(`🔍 Checking if foreign server "${serverName}" can be dropped...`);

      // Check for remaining foreign tables on this server
      const checkSql = `
        SELECT COUNT(*) as count
        FROM pg_foreign_table ft
        JOIN pg_foreign_server fs ON ft.ftserver = fs.oid
        WHERE fs.srvname = $1
      `;
      const checkResult = await this.executeQuery(connectionId, checkSql, { params: [serverName] });
      if (!checkResult.success) {
        throw new Error(checkResult.error || 'Failed to check server usage');
      }

      const tableCount = parseInt(checkResult.rows?.[0]?.count || '0', 10);
      if (tableCount > 0) {
        console.log(`ℹ️ Server "${serverName}" still has ${tableCount} foreign table(s), skipping drop.`);
        return { success: true };
      }

      // Drop user mapping first
      const dropMappingSql = `DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER ${this.sanitizeIdentifier(serverName)}`;
      const mappingResult = await this.executeQuery(connectionId, dropMappingSql);
      if (!mappingResult.success) {
        console.warn(`Could not drop user mapping for server ${serverName}:`, mappingResult.error);
      }

      // Drop the server with CASCADE
      const dropServerSql = `DROP SERVER IF EXISTS ${this.sanitizeIdentifier(serverName)} CASCADE`;
      const serverResult = await this.executeQuery(connectionId, dropServerSql);
      if (!serverResult.success) {
        throw new Error(serverResult.error || 'Failed to drop foreign server');
      }

      console.log(`✅ Foreign server "${serverName}" dropped (unused).`);
      return { success: true };
    } catch (error: any) {
      console.error('❌ dropForeignServerIfUnused failed:', error);
      return { success: false, error: error.message };
    }
  }

  // ===========================================================================
  // END CASCADING DELETE METHODS
  // ===========================================================================

  /**
   * Test a foreign table query
   */
  async testForeignTable(
    connectionId: string,
    tableName: string,
    limit: number = 5
  ): Promise<ClientQueryExecutionResult> {
    try {
      const sanitizedTableName = this.sanitizeIdentifier(tableName);
      const sql = `SELECT * FROM ${sanitizedTableName} LIMIT ${limit};`;
      
      console.log(`🔍 Testing foreign table ${tableName} with query...`);
      return await this.executeQuery(connectionId, sql);
    } catch (error: any) {
      console.error('❌ Failed to test foreign table:', error);
      return {
        result: null,
        success: false,
        error: this.getErrorMessage(error),
      };
    }
  }

  /**
   * Get foreign table column information
   */
  async getForeignTableColumns(
    connectionId: string,
    tableName: string
  ): Promise<{
    success: boolean;
    columns?: Array<{
      column_name: string;
      data_type: string;
      character_maximum_length: number | null;
      numeric_precision: number | null;
      numeric_scale: number | null;
      is_nullable: string;
      column_default: string | null;
    }>;
    error?: string;
  }> {
    try {
      const sanitizedTableName = this.sanitizeIdentifier(tableName);
      const sql = `
        SELECT 
          column_name,
          data_type,
          character_maximum_length,
          numeric_precision,
          numeric_scale,
          is_nullable,
          column_default
        FROM information_schema.columns 
        WHERE table_name = '${sanitizedTableName}'
        ORDER BY ordinal_position;
      `;
      
      console.log(`📊 Getting foreign table columns for ${tableName}...`);
      const response = await this.executeQuery(connectionId, sql);
      
      if (response.success && response.result?.rows) {
        return {
          success: true,
          columns: response.result.rows
        };
      } else {
        return {
          success: false,
          error: response.error || 'Failed to get foreign table columns'
        };
      }
    } catch (error: any) {
      console.error('❌ Failed to get foreign table columns:', error);
      return {
        success: false,
        error: this.getErrorMessage(error),
      };
    }
  }

  /**
   * Check if FDW (Foreign Data Wrapper) servers are available
   */
  async checkFDWAvailability(
    connectionId: string,
    fileType: string
  ): Promise<{
    available: boolean;
    serverName?: string;
    error?: string;
  }> {
    try {
      // Map file type to FDW server name
      const fdwServers: Record<string, string> = {
        'excel': 'fdw_excel',
        'xml': 'fdw_xml',
        'delimited': 'fdw_delimited',
        'csv': 'fdw_delimited',
        'json': 'fdw_multiformat',
        'avro': 'fdw_multiformat',
        'parquet': 'fdw_multiformat',
        'regex': 'fdw_regex',
        'ldif': 'fdw_ldif',
        'positional': 'fdw_positional',
        'fixed': 'fdw_positional',
        'schema': 'fdw_schema'
      };
      
      const fdwServer = fdwServers[fileType.toLowerCase()] || 'fdw_delimited';
      
      const sql = `
        SELECT 
          srvname,
          fdwname,
          srvoptions
        FROM pg_foreign_server fs 
        JOIN pg_foreign_data_wrapper fdw ON fs.srvfdw = fdw.oid
        WHERE srvname = '${fdwServer}';
      `;
      
      console.log(`🔍 Checking FDW availability for ${fileType} (server: ${fdwServer})...`);
      const response = await this.executeQuery(connectionId, sql);
      
      if (response.success && response.result?.rows && response.result.rows.length > 0) {
        return {
          available: true,
          serverName: fdwServer
        };
      } else {
        return {
          available: false,
          serverName: fdwServer,
          error: `FDW server "${fdwServer}" not found. Install FDW extensions first.`
        };
      }
    } catch (error: any) {
      console.error('❌ Failed to check FDW availability:', error);
      return {
        available: false,
        error: this.getErrorMessage(error)
      };
    }
  }

  // ===========================================================================
  // PostgreSQL Specific Operations
  // ===========================================================================

  async testPostgresStatus(): Promise<any> {
    try {
      console.log(`📊 Testing PostgreSQL status...`);
      const response = await this.api.get('/api/postgres/status');
      console.log(`✅ PostgreSQL status:`, response.data);
      return response.data;
    } catch (error: any) {
      console.error('❌ Failed to get PostgreSQL status:', error);
      return {
        success: false,
        error: this.getErrorMessage(error),
        connected: false
      };
    }
  }

  async executePostgresQuery(sql: string, params: any[] = []): Promise<any> {
    try {
      console.log(`⚡ Executing PostgreSQL query...`);
      const response = await this.api.post('/api/postgres/query', { sql, params });
      console.log(`✅ PostgreSQL query executed: ${response.data.rowCount || 0} rows`);
      return response.data;
    } catch (error: any) {
      console.error('❌ Failed to execute PostgreSQL query:', error);
      throw error;
    }
  }

  // ===========================================================================
  // Advanced Operations
  // ===========================================================================

  async getTableConstraints(
    connectionId: string,
    schema: string,
    table: string
  ): Promise<any[]> {
    const request: GetConstraintsRequest = { connectionId, schema, table };
    try {
      const response = await this.api.post('/api/database/constraints', request);
      return response.data;
    } catch (error: any) {
      console.error('❌ Failed to get constraints:', error);
      return [];
    }
  }

  async getSchemas(connectionId: string): Promise<string[]> {
    const request: GetSchemasRequest = { connectionId };
    try {
      const response = await this.api.post('/api/database/schemas', request);
      return response.data;
    } catch (error: any) {
      console.error('❌ Failed to get schemas:', error);
      return [];
    }
  }

  async getFunctions(connectionId: string, schema?: string): Promise<any[]> {
    try {
      const response = await this.api.post('/api/database/functions', {
        connectionId,
        schema,
      });
      return response.data;
    } catch (error: any) {
      console.error('❌ Failed to get functions:', error);
      return [];
    }
  }

  async getIndexes(connectionId: string, tableName?: string): Promise<any[]> {
    try {
      const response = await this.api.post('/api/database/indexes', {
        connectionId,
        tableName,
      });
      return response.data;
    } catch (error: any) {
      console.error('❌ Failed to get indexes:', error);
      return [];
    }
  }

  async getTablespaces(connectionId: string): Promise<any[]> {
    try {
      const response = await this.api.post('/api/database/tablespaces', {
        connectionId,
      });
      return response.data;
    } catch (error: any) {
      console.error('❌ Failed to get tablespaces:', error);
      return [];
    }
  }

  async getSessions(connectionId: string): Promise<any[]> {
    try {
      const response = await this.api.post('/api/database/sessions', {
        connectionId,
      });
      return response.data;
    } catch (error: any) {
      console.error('❌ Failed to get sessions:', error);
      return [];
    }
  }

  async getLinkedServers(connectionId: string): Promise<any[]> {
    try {
      const response = await this.api.post('/api/database/linked-servers', {
        connectionId,
      });
      return response.data;
    } catch (error: any) {
      console.error('❌ Failed to get linked servers:', error);
      return [];
    }
  }

  async getAgentJobs(connectionId: string): Promise<any[]> {
    try {
      const response = await this.api.post('/api/database/agent-jobs', {
        connectionId,
      });
      return response.data;
    } catch (error: any) {
      console.error('❌ Failed to get agent jobs:', error);
      return [];
    }
  }

  // ===========================================================================
  // Batch Operations
  // ===========================================================================

  async batchDisconnect(connectionIds: string[]): Promise<ClientDisconnectResult[]> {
    const request: BatchDisconnectRequest = { connectionIds };
    try {
      const response = await this.api.post('/api/database/batch-disconnect', request);
      return response.data;
    } catch (error: any) {
      return connectionIds.map(() => ({
        success: false,
        error: this.getErrorMessage(error),
      }));
    }
  }
/**
 * Delete a database metadata entry by its ID.
 * This removes the saved connection configuration from the backend.
 */
async deleteDatabaseMetadata(metadataId: string): Promise<{ success: boolean; error?: string }> {
  try {
    console.log(`🗑️ Deleting database metadata entry ${metadataId}...`);
    const response = await this.api.delete(`/api/database/metadata/${metadataId}`);
    console.log(`✅ Metadata entry deleted:`, response.data);
    return response.data;
  } catch (error: any) {
    console.error('❌ Failed to delete database metadata:', error);
    return { success: false, error: this.getErrorMessage(error) };
  }
}

  async clearAllConnections(): Promise<ClientDisconnectResult> {
    try {
      const response = await this.api.post('/api/database/clear-connections');
      return response.data;
    } catch (error: any) {
      return {
        success: false,
        error: this.getErrorMessage(error),
      };
    }
  }

  async insertDataSourceMetadata(
    connectionId: string,
    metadata: {
      name: string;
      type: string;
      filePath: string;
      foreignTableName: string;
      options?: Record<string, any>;
    }
  ): Promise<{ success: boolean; error?: string; id?: string }> {
    const sql = `
      INSERT INTO data_source_metadata
        (name, type, file_path, foreign_table_name, connection_id, options, created_at, updated_at)
      VALUES
        ($1, $2, $3, $4, $5, $6::jsonb, NOW(), NOW())
      RETURNING id;
    `;
    const params = [
      metadata.name,
      metadata.type,
      metadata.filePath,
      metadata.foreignTableName,
      connectionId,
      JSON.stringify(metadata.options || {})
    ];
    try {
      const result = await this.executeQuery(connectionId, sql, { params });
      if (result.success && result.result?.rows?.length > 0) {
        return { success: true, id: result.result.rows[0].id };
      }
      return { success: false, error: result.error || 'No ID returned' };
    } catch (error: any) {
      console.error('❌ Failed to insert data source metadata:', error);
      return { success: false, error: error.message };
    }
  }

  async saveDatabaseMetadata(metadata: any): Promise<{ success: boolean; metadataEntryId?: string; error?: string }> {
    try {
      const response = await this.api.post('/api/database/metadata', metadata);
      return response.data;
    } catch (error: any) {
      console.error('Failed to save database metadata:', error);
      return {
        success: false,
        error: this.getErrorMessage(error),
      };
    }
  }

  async getDatabaseMetadataEntries(): Promise<{
    success: boolean;
    entries?: any[];
    error?: string;
  }> {
    try {
      const response = await this.api.get('/api/database/metadata');
      return response.data;
    } catch (error: any) {
      return {
        success: false,
        error: this.getErrorMessage(error),
      };
    }
  }

  // ===========================================================================
  // Diagnostic Methods
  // ===========================================================================

  /**
   * Comprehensive diagnosis of connection issues
   */
  async diagnoseConnectionIssues(): Promise<{
    issues: string[];
    suggestions: string[];
    detectedUser?: string;
  }> {
    const issues: string[] = [];
    const suggestions: string[] = [];
    
    // Detect current system user
    let detectedUser = '';
    if (typeof process !== 'undefined' && process.env) {
      detectedUser = process.platform === 'win32' ? 
        (process.env.USERNAME || '') : 
        (process.env.USER || '');
    }
    
    // Test backend connectivity
    try {
      await this.testHealth();
      suggestions.push('✅ Backend server is reachable');
    } catch (error) {
      issues.push('Cannot connect to backend server');
      suggestions.push('❌ Start backend server: node app.ts');
      suggestions.push(`❌ Backend URL: ${this.baseUrl}`);
      suggestions.push('❌ Check if backend is running on port 3000');
    }
    
    // Test PostgreSQL connection with various users
    const testUsers = [
      detectedUser, 
      'postgres', 
      'admin', 
      process.env.REACT_APP_DB_USER || '', 
      localStorage.getItem('last_db_user') || ''
    ].filter(user => user && user.trim() !== '');
    
    suggestions.push(`🔍 Testing PostgreSQL users: ${testUsers.join(', ')}`);
    
    let foundValidUser = false;
    for (const user of testUsers) {
      try {
        console.log(`🧪 Testing PostgreSQL connection with user: ${user}`);
        const testResult = await this.testConnection('postgresql', {
          host: 'localhost',
          port: '5432',
          dbname: 'postgres',
          user: user,
          password: ''
        });
        
        if (testResult.success) {
          suggestions.push(`✅ PostgreSQL accessible with user: "${user}"`);
          foundValidUser = true;
          
          // Store successful user
          if (typeof window !== 'undefined') {
            localStorage.setItem('last_db_user', user);
          }
          
          // Test if we can connect
          const connectResult = await this.connect('postgresql', {
            host: 'localhost',
            port: '5432',
            dbname: 'postgres',
            user: user,
            password: ''
          });
          
          if (connectResult.success) {
            suggestions.push(`✅ Successfully connected with user: "${user}"`);
          }
          break;
        }
      } catch (error) {
        // Continue testing other users
      }
    }
    
    if (!foundValidUser) {
      issues.push('No valid PostgreSQL user found');
      suggestions.push('💡 Run: ./standalone-pg-builder.sh --fix-permissions');
      suggestions.push('💡 Run: ./standalone-pg-builder.sh --setup-tables');
      suggestions.push(`💡 Try creating user: ${detectedUser || 'your-system-username'}`);
    }
    
    // Check for common configuration issues
    if (detectedUser && !foundValidUser) {
      suggestions.push(`💡 PostgreSQL might be running as user: "${detectedUser}"`);
      suggestions.push(`💡 Try running: ./fix-postgres-connections.sh`);
    }
    
    return {
      issues,
      suggestions,
      detectedUser: detectedUser || 'unknown'
    };
  }

  // ===========================================================================
  // Utility Methods
  // ===========================================================================

  private getErrorMessage(error: any): string {
    // First check if it's actually NOT an error
    if (error.response && error.response.status === 200) {
      console.log('⚠️ Successful response (200) incorrectly flagged as error');
      return 'Connection successful but response parsing issue';
    }
    
    // Check if it's an axios error
    if (error.response) {
      const { status, data } = error.response;
      
      // Try to get detailed error message from response
      if (data?.error) {
        // Special handling for role does not exist error
        if (typeof data.error === 'string' && data.error.includes('role "postgres" does not exist')) {
          return `PostgreSQL user "postgres" doesn't exist. Try using your system username instead. Current detected user: ${process.platform === 'win32' ? process.env.USERNAME : process.env.USER}`;
        }
        return `HTTP ${status}: ${data.error}`;
      }
      if (data?.message) return `HTTP ${status}: ${data.message}`;
      if (typeof data === 'string') return `HTTP ${status}: ${data}`;
      
      // Special handling for common HTTP errors
      switch (status) {
        case 404:
          return `Endpoint not found (404). Check if backend is running correctly.`;
        case 500:
          return `Backend server error (500). Check backend logs.`;
        case 401:
          return `Unauthorized (401). Check authentication token.`;
        case 403:
          return `Forbidden (403). Check permissions.`;
        default:
          return `HTTP ${status}: ${error.response.statusText || 'Unknown error'}`;
      }
    } else if (error.request) {
      // Network error - no response received
      if (error.code === 'ECONNABORTED') {
        return `Connection timeout to ${this.baseUrl}. Check if backend server is running.`;
      } else if (error.code === 'ERR_NETWORK') {
        return `Network error: Cannot connect to backend at ${this.baseUrl}. Ensure: 1) Backend running on port 3000, 2) CORS configured properly`;
      }
      return 'No response from server. Please check if backend is running.';
    } else if (error.message) {
      // Axios or other error
      if (error.message.includes('Network Error')) {
        return `Network Error: Cannot connect to ${this.baseUrl}. This is usually a CORS issue. Check: 1) Backend is running, 2) Backend allows CORS from ${window.location.origin}`;
      }
      
      // PostgreSQL specific error messages
      if (error.message.includes('role') && error.message.includes('does not exist')) {
        const currentUser = process.platform === 'win32' ? process.env.USERNAME : process.env.USER;
        return `PostgreSQL user error: ${error.message}. Try using: ${currentUser || 'your-system-username'}`;
      }
      
      return error.message;
    }
    return String(error);
  }

  /**
   * Sanitize PostgreSQL identifier
   */
  private sanitizeIdentifier(identifier: string): string {
    return identifier
      .replace(/[^a-zA-Z0-9_]/g, '_')
      .replace(/_{2,}/g, '_')
      .replace(/^_+|_+$/g, '')
      .toLowerCase();
  }

  /**
   * Map application data type to PostgreSQL data type
   */
  public mapToPostgresType(
    appType: string, 
    length?: number, 
    precision?: number, 
    scale?: number
  ): string {
    const typeLower = appType.toLowerCase().trim();
    
    // Integer types
    if (typeLower.includes('int') || typeLower.includes('integer') || typeLower === 'number') {
      if (typeLower.includes('bigint') || typeLower.includes('long')) return 'BIGINT';
      if (typeLower.includes('smallint')) return 'SMALLINT';
      return 'INTEGER';
    }
    
    // Decimal/Numeric types
    if (typeLower.includes('decimal') || typeLower.includes('numeric')) {
      if (precision !== undefined && scale !== undefined) {
        return `NUMERIC(${precision}, ${scale})`;
      }
      if (precision !== undefined) {
        return `NUMERIC(${precision})`;
      }
      return 'NUMERIC';
    }
    
    // Floating point types
    if (typeLower.includes('float') || typeLower.includes('double') || typeLower.includes('real')) {
      if (typeLower.includes('double') || typeLower.includes('float8')) return 'DOUBLE PRECISION';
      if (typeLower.includes('float4')) return 'REAL';
      return 'DOUBLE PRECISION';
    }
    
    // Date/Time types
    if (typeLower.includes('date') && !typeLower.includes('datetime')) {
      return 'DATE';
    }
    if (typeLower.includes('datetime') || typeLower.includes('timestamp')) {
      if (typeLower.includes('without')) return 'TIMESTAMP';
      if (typeLower.includes('with')) return 'TIMESTAMPTZ';
      return 'TIMESTAMP';
    }
    if (typeLower.includes('time')) {
      if (typeLower.includes('without')) return 'TIME';
      if (typeLower.includes('with')) return 'TIMETZ';
      return 'TIME';
    }
    
    // Boolean
    if (typeLower.includes('bool')) {
      return 'BOOLEAN';
    }
    
    // JSON types
    if (typeLower.includes('jsonb')) {
      return 'JSONB';
    }
    if (typeLower.includes('json')) {
      return 'JSON';
    }
    
    // XML
    if (typeLower.includes('xml')) {
      return 'XML';
    }
    
    // Text/String types
    if (typeLower.includes('char') || typeLower.includes('text') || typeLower.includes('string')) {
      if (typeLower.includes('var') || typeLower.includes('varchar')) {
        return length ? `VARCHAR(${length})` : 'VARCHAR';
      }
      if (typeLower.includes('char') && !typeLower.includes('var')) {
        return length ? `CHAR(${length})` : 'CHAR';
      }
      return 'TEXT';
    }
    
    // Default fallback
    return 'TEXT';
  }

  static getDatabaseDisplayName(dbType: DatabaseType): string {
    const names: Record<DatabaseType, string> = {
      mysql: 'MySQL',
      postgresql: 'PostgreSQL',
      postgres: 'PostgreSQL',
      oracle: 'Oracle',
      sqlserver: 'SQL Server',
      mssql: 'SQL Server',
      db2: 'IBM DB2',
      'sap-hana': 'SAP HANA',
      hana: 'SAP HANA',
      sybase: 'Sybase',
      netezza: 'Netezza',
      informix: 'Informix',
      firebird: 'Firebird',
    };
    return names[dbType] || dbType;
  }

  static validateConfig(config: ClientDatabaseConfig): string[] {
    const errors: string[] = [];
    
    if (!config.dbname?.trim()) {
      errors.push('Database name is required');
    }
    
    if (!config.host?.trim()) {
      errors.push('Host is required');
    }
    
    if (!config.user?.trim()) {
      errors.push('Username is required');
    }
    
    if (config.port && isNaN(Number(config.port))) {
      errors.push('Port must be a number');
    }
    
    return errors;
  }

  static createLocalPostgresConfig(): ClientDatabaseConfig {
    const getCurrentUser = (): string => {
      if (typeof window !== 'undefined') {
        try {
          const storedUser = localStorage.getItem('last_db_user');
          if (storedUser) return storedUser;
        } catch (e) {}
      }
      
      if (typeof process !== 'undefined' && process.env) {
        // CRITICAL FIX: Get current system username from OS or environment
        const systemUser = process.platform === 'win32' ? 
          process.env.USERNAME : 
          process.env.USER;
        
        return process.env.REACT_APP_DB_USER || 
               process.env.DB_USER || 
               systemUser || 
               'postgres';
      }
      
      return 'postgres';
    };
    
    return {
      host: 'localhost',
      port: '5432',
      dbname: 'postgres',
      user: getCurrentUser(),
      password: '',
      schema: 'public'
    };
  }

  static createDefaultConfig(dbType: DatabaseType): ClientDatabaseConfig {
    const defaults: Record<DatabaseType, ClientDatabaseConfig> = {
      mysql: { host: 'localhost', port: '3306', dbname: '', user: '', password: '' },
      postgresql: DatabaseApiService.createLocalPostgresConfig(),
      postgres: DatabaseApiService.createLocalPostgresConfig(),
      oracle: { host: 'localhost', port: '1521', dbname: 'ORCL', user: '', password: '' },
      sqlserver: { host: 'localhost', port: '1433', dbname: 'master', user: '', password: '' },
      mssql: { host: 'localhost', port: '1433', dbname: 'master', user: '', password: '' },
      db2: { host: 'localhost', port: '50000', dbname: '', user: '', password: '' },
      'sap-hana': { host: 'localhost', port: '30015', dbname: '', user: '', password: '' },
      hana: { host: 'localhost', port: '30015', dbname: '', user: '', password: '' },
      sybase: { host: 'localhost', port: '5000', dbname: '', user: '', password: '' },
      netezza: { host: 'localhost', port: '5480', dbname: '', user: '', password: '' },
      informix: { host: 'localhost', port: '9088', dbname: '', user: '', password: '' },
      firebird: { host: 'localhost', port: '3050', dbname: '', user: '', password: '' },
    };
    
    return { ...defaults[dbType] };
  }
}

// ===========================================================================
// React Hook for Database Operations - WITH FOREIGN TABLE SUPPORT
// ===========================================================================

import { useState, useCallback, useEffect } from 'react';

interface UseDatabaseOperationsOptions {
  baseUrl?: string;
}

export function useDatabaseOperations(options?: UseDatabaseOperationsOptions) {
  const [apiService] = useState(() => new DatabaseApiService(options?.baseUrl));
  const [connections, setConnections] = useState<ClientConnectionInfo[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [healthStatus, setHealthStatus] = useState<string>('unknown');
  const [backendUrl, setBackendUrl] = useState<string>(apiService.baseUrl);
  const [foreignTables, setForeignTables] = useState<any[]>([]);
  const [diagnostics, setDiagnostics] = useState<{
    issues: string[];
    suggestions: string[];
    detectedUser?: string;
  }>({ issues: [], suggestions: [] });

  // Check backend health on mount
  useEffect(() => {
    checkBackendHealth();
    runConnectionDiagnostics();
  }, []);

  const checkBackendHealth = useCallback(async () => {
    try {
      console.log(`🔍 Checking backend health at ${backendUrl}...`);
      const health = await apiService.testHealth();
      
      // Check if the response indicates backend is running
      if (health && (health.status === 'OK' || health.status === 'DEGRADED' || health.success === true)) {
        setHealthStatus('healthy');
        setError(null);
        console.log('✅ Backend health check successful:', health);
        return true;
      } else {
        // Backend responded but with error status
        setHealthStatus('degraded');
        setError(`Backend responded but with status: ${health?.status || 'unknown'}`);
        return false;
      }
    } catch (error: any) {
      console.error('❌ Backend health check failed:', error);
      
      // Check the actual error message
      let errorMsg = error.message || 'Backend server is not responding';
      
      if (error.message && error.message.includes('Network Error')) {
        errorMsg = `Cannot connect to backend at ${backendUrl}. Please ensure: 1) Backend is running (node app.ts), 2) CORS is configured`;
      } else if (error.message && error.message.includes('Timeout')) {
        errorMsg = `Timeout connecting to backend at ${backendUrl}. Is the server running?`;
      }
      
      setHealthStatus('unhealthy');
      setError(errorMsg);
      return false;
    }
  }, [apiService, backendUrl]);

  const runConnectionDiagnostics = useCallback(async () => {
    try {
      console.log('🔍 Running connection diagnostics...');
      const diag = await apiService.diagnoseConnectionIssues();
      setDiagnostics(diag);
      
      if (diag.issues.length > 0) {
        console.warn('⚠️ Connection issues detected:', diag.issues);
        console.log('💡 Suggestions:', diag.suggestions);
      } else {
        console.log('✅ All connection diagnostics passed');
      }
      
      return diag;
    } catch (error) {
      console.error('❌ Failed to run diagnostics:', error);
      return { issues: ['Failed to run diagnostics'], suggestions: [], detectedUser: '' };
    }
  }, [apiService]);

  const refreshConnections = useCallback(async () => {
    try {
      const activeConnections = await apiService.getActiveConnections();
      setConnections(activeConnections);
      console.log(`📊 Refreshed connections: ${activeConnections.length} active`);
    } catch (err: any) {
      console.error('❌ Failed to refresh connections:', err);
      setError(`Failed to refresh connections: ${err.message}`);
    }
  }, [apiService]);

  // ===========================================================================
  // FOREIGN TABLE OPERATIONS
  // ===========================================================================

  const createForeignServer = useCallback(async (
    connectionId: string,
    serverName: string,
    dbType: DatabaseType,
    config: ClientDatabaseConfig
  ) => {
    setLoading(true);
    setError(null);
    try {
      const result = await apiService.createForeignServer(connectionId, serverName, dbType, config);
      return result;
    } catch (err: any) {
      const errorMsg = err instanceof Error ? err.message : 'Failed to create foreign server';
      setError(errorMsg);
      return { success: false, error: errorMsg };
    } finally {
      setLoading(false);
    }
  }, [apiService]);

  const createForeignTable = useCallback(async (
    connectionId: string,
    tableName: string,
    columns: Array<{
      name: string;
      type: string;
      length?: number;
      precision?: number;
      scale?: number;
      nullable?: boolean;
      defaultValue?: string;
    }>,
    fileType: string,
    filePath: string,
    options?: Record<string, string>,
    serverName?: string
  ) => {
    setLoading(true);
    setError(null);
    try {
      console.log(`🔄 Creating foreign table ${tableName}...`);
      const result = await apiService.createForeignTable(
        connectionId,
        tableName,
        columns,
        fileType,
        filePath,
        options,
        serverName
      );
      return result;
    } catch (err: any) {
      const errorMsg = err instanceof Error ? err.message : 'Foreign table creation failed';
      setError(errorMsg);
      return { success: false, error: errorMsg };
    } finally {
      setLoading(false);
    }
  }, [apiService]);

  const createForeignTableViaBackend = useCallback(async (
    connectionId: string,
    tableName: string,
    columns: Array<{
      name: string;
      type: string;
      length?: number;
      precision?: number;
      scale?: number;
      nullable?: boolean;
      defaultValue?: string;
    }>,
    fileType: string,
    filePath: string,
    options?: Record<string, string>,
    serverName?: string
  ) => {
    setLoading(true);
    setError(null);
    try {
      console.log(`🔄 Creating foreign table via backend: ${tableName}`);
      const result = await apiService.createForeignTableViaBackend(
        connectionId,
        tableName,
        columns,
        fileType,
        filePath,
        options,
        serverName
      );
      return result;
    } catch (err: any) {
      const errorMsg = err instanceof Error ? err.message : 'Foreign table creation failed';
      setError(errorMsg);
      return { success: false, error: errorMsg };
    } finally {
      setLoading(false);
    }
  }, [apiService]);

  const listForeignTables = useCallback(async (): Promise<{
    success: boolean;
    tables?: any[];
    error?: string;
  }> => {
    setLoading(true);
    setError(null);
    try {
      console.log(`📋 Listing foreign tables...`);
      const result = await apiService.listForeignTables();
      if (result.success && result.tables) {
        setForeignTables(result.tables);
        console.log(`✅ Found ${result.tables.length} foreign tables`);
      } else {
        setError(result.error || 'Failed to list foreign tables');
      }
      return result;
    } catch (err: any) {
      const errorMsg = err instanceof Error ? err.message : 'Failed to list foreign tables';
      setError(errorMsg);
      return { success: false, error: errorMsg };
    } finally {
      setLoading(false);
    }
  }, [apiService]);

  const refreshForeignTables = useCallback(async () => {
    await listForeignTables();
  }, [listForeignTables]);

  const dropForeignTable = useCallback(async (tableName: string) => {
    setLoading(true);
    setError(null);
    try {
      console.log(`🗑️ Dropping foreign table ${tableName}...`);
      const result = await apiService.dropForeignTable(tableName);
      if (result.success) {
        await refreshForeignTables();
        console.log(`✅ Foreign table dropped: ${tableName}`);
      } else {
        setError(result.error || 'Failed to drop foreign table');
      }
      return result;
    } catch (err: any) {
      const errorMsg = err instanceof Error ? err.message : 'Failed to drop foreign table';
      setError(errorMsg);
      return { success: false, error: errorMsg };
    } finally {
      setLoading(false);
    }
  }, [apiService, refreshForeignTables]);

  const testForeignTable = useCallback(async (
    connectionId: string,
    tableName: string,
    limit: number = 5
  ) => {
    setLoading(true);
    setError(null);
    try {
      console.log(`🔍 Testing foreign table ${tableName}...`);
      const result = await apiService.testForeignTable(connectionId, tableName, limit);
      return result;
    } catch (err: any) {
      const errorMsg = err instanceof Error ? err.message : 'Failed to test foreign table';
      setError(errorMsg);
      return { success: false, error: errorMsg };
    } finally {
      setLoading(false);
    }
  }, [apiService]);

  const checkFDWAvailability = useCallback(async (
    connectionId: string,
    fileType: string
  ) => {
    setLoading(true);
    setError(null);
    try {
      console.log(`🔍 Checking FDW availability for ${fileType}...`);
      const result = await apiService.checkFDWAvailability(connectionId, fileType);
      return result;
    } catch (err: any) {
      const errorMsg = err instanceof Error ? err.message : 'Failed to check FDW availability';
      setError(errorMsg);
      return { available: false, error: errorMsg };
    } finally {
      setLoading(false);
    }
  }, [apiService]);

  // ===========================================================================
  // NEW: CASCADING DELETE HOOKS
  // ===========================================================================

  const dropForeignTableCascade = useCallback(async (
    connectionId: string,
    tableName: string
  ) => {
    setLoading(true);
    setError(null);
    try {
      const result = await apiService.dropForeignTableCascade(connectionId, tableName);
      if (result.success) {
        await refreshForeignTables();
      }
      return result;
    } catch (err: any) {
      const errorMsg = err instanceof Error ? err.message : 'Cascading delete failed';
      setError(errorMsg);
      return { success: false, error: errorMsg };
    } finally {
      setLoading(false);
    }
  }, [apiService, refreshForeignTables]);

  const dropForeignServerIfUnused = useCallback(async (
    connectionId: string,
    serverName: string
  ) => {
    setLoading(true);
    setError(null);
    try {
      const result = await apiService.dropForeignServerIfUnused(connectionId, serverName);
      return result;
    } catch (err: any) {
      const errorMsg = err instanceof Error ? err.message : 'Failed to drop foreign server';
      setError(errorMsg);
      return { success: false, error: errorMsg };
    } finally {
      setLoading(false);
    }
  }, [apiService]);

  
  // ===========================================================================
  // CONNECTION OPERATIONS
  // ===========================================================================

  const testConnection = useCallback(async (
    dbType: DatabaseType,
    config: ClientDatabaseConfig
  ): Promise<ClientTestConnectionResult> => {
    setLoading(true);
    setError(null);
    try {
      // First check backend health
      const isHealthy = await checkBackendHealth();
      if (!isHealthy) {
        return { 
          success: false, 
          error: `Backend server is not responding at ${backendUrl}. Please start the backend server.` 
        };
      }

      console.log(`🧪 Testing ${dbType} connection...`);
      const result = await apiService.testConnection(dbType, config);
      return result;
    } catch (err: any) {
      const errorMsg = err instanceof Error ? err.message : 'Connection test failed';
      setError(errorMsg);
      return { success: false, error: errorMsg };
    } finally {
      setLoading(false);
    }
  }, [apiService, checkBackendHealth, backendUrl]);

  const connect = useCallback(async (
    dbType: DatabaseType,
    config: ClientDatabaseConfig
  ): Promise<ClientConnectResult> => {
    setLoading(true);
    setError(null);
    try {
      // First check backend health
      const isHealthy = await checkBackendHealth();
      if (!isHealthy) {
        return { 
          connectionId: '', 
          success: false, 
          error: `Backend server is not responding at ${backendUrl}. Please start the backend server.` 
        };
      }

      console.log(`🔌 Connecting to ${dbType}...`);
      const result = await apiService.connect(dbType, config);
      if (result.success) {
        console.log(`✅ Connection successful, refreshing connections...`);
        await refreshConnections();
        // Update last successful user
        if (config.user && typeof window !== 'undefined') {
          localStorage.setItem('last_db_user', config.user);
        }
      }
      return result;
    } catch (err: any) {
      const errorMsg = err instanceof Error ? err.message : 'Connection failed';
      setError(errorMsg);
      return { connectionId: '', success: false, error: errorMsg };
    } finally {
      setLoading(false);
    }
  }, [apiService, refreshConnections, checkBackendHealth, backendUrl]);

  const disconnect = useCallback(async (connectionId: string): Promise<ClientDisconnectResult> => {
    setLoading(true);
    setError(null);
    try {
      console.log(`🔌 Disconnecting ${connectionId}...`);
      const result = await apiService.disconnect(connectionId);
      if (result.success) {
        await refreshConnections();
      }
      return result;
    } catch (err: any) {
      const errorMsg = err instanceof Error ? err.message : 'Disconnect failed';
      setError(errorMsg);
      return { success: false, error: errorMsg };
    } finally {
      setLoading(false);
    }
  }, [apiService, refreshConnections]);

  const getTables = useCallback(async (
    connectionId: string,
    options?: ClientInspectionOptions
  ): Promise<ClientTableListResult> => {
    setLoading(true);
    setError(null);
    try {
      console.log(`📋 Getting tables for ${connectionId}...`);
      return await apiService.getTables(connectionId, options);
    } catch (err: any) {
      const errorMsg = err instanceof Error ? err.message : 'Failed to get tables';
      setError(errorMsg);
      return { tables: [], success: false, error: errorMsg };
    } finally {
      setLoading(false);
    }
  }, [apiService]);

  const executeQuery = useCallback(async (
    connectionId: string,
    sql: string,
    options?: ClientQueryExecutionOptions
  ): Promise<ClientQueryExecutionResult> => {
    setLoading(true);
    setError(null);
    try {
      console.log(`⚡ Executing query for ${connectionId}...`);
      return await apiService.executeQuery(connectionId, sql, options);
    } catch (err: any) {
      const errorMsg = err instanceof Error ? err.message : 'Query execution failed';
      setError(errorMsg);
      return { result: null, success: false, error: errorMsg };
    } finally {
      setLoading(false);
    }
  }, [apiService]);

  const getDatabaseInfo = useCallback(async (connectionId: string): Promise<ClientDatabaseInfoResult> => {
    setLoading(true);
    setError(null);
    try {
      console.log(`ℹ️ Getting database info for ${connectionId}...`);
      return await apiService.getDatabaseInfo(connectionId);
    } catch (err: any) {
      const errorMsg = err instanceof Error ? err.message : 'Failed to get database info';
      setError(errorMsg);
      return { info: { version: '', name: '' }, success: false, error: errorMsg };
    } finally {
      setLoading(false);
    }
  }, [apiService]);

  const testPostgresStatus = useCallback(async (): Promise<any> => {
    setLoading(true);
    setError(null);
    try {
      console.log(`📊 Testing PostgreSQL status...`);
      const result = await apiService.testPostgresStatus();
      return result;
    } catch (err: any) {
      const errorMsg = err instanceof Error ? err.message : 'Failed to get PostgreSQL status';
      setError(errorMsg);
      return { success: false, error: errorMsg, connected: false };
    } finally {
      setLoading(false);
    }
  }, [apiService]);

  const clearError = useCallback(() => {
    setError(null);
  }, []);

  const updateBackendUrl = useCallback((newUrl: string) => {
    setBackendUrl(newUrl);
    console.log(`🔧 Updated backend URL to: ${newUrl}`);
  }, []);

  const diagnoseConnection = useCallback(async () => {
    console.log('🔍 Running connection diagnosis...');
    
    // Run comprehensive diagnostics
    const diag = await runConnectionDiagnostics();
    
    // Test 1: Direct fetch to backend
    try {
      const response = await fetch(`${backendUrl}/health`);
      console.log('✅ Direct fetch result:', {
        status: response.status,
        statusText: response.statusText,
        ok: response.ok,
        headers: Array.from(response.headers.entries())
      });
      
      const text = await response.text();
      console.log('📄 Raw response:', text.substring(0, 500));
    } catch (fetchError) {
      console.error('❌ Direct fetch failed:', fetchError);
    }
    
    // Test 2: Test CORS with fetch
    try {
      const corsTest = await fetch(`${backendUrl}/health`, {
        method: 'GET',
        mode: 'cors',
        headers: { 'Content-Type': 'application/json' }
      });
      console.log('✅ CORS test result:', corsTest.status, corsTest.statusText);
    } catch (corsError) {
      console.error('❌ CORS test failed:', corsError);
    }
    
    return diag;
  }, [backendUrl, runConnectionDiagnostics]);

  const getForeignTableColumns = useCallback(async (
    connectionId: string,
    tableName: string
  ) => {
    setLoading(true);
    setError(null);
    try {
      console.log(`📊 Getting foreign table columns for ${tableName}...`);
      const result = await apiService.getForeignTableColumns(connectionId, tableName);
      return result;
    } catch (err: any) {
      const errorMsg = err instanceof Error ? err.message : 'Failed to get foreign table columns';
      setError(errorMsg);
      return { success: false, error: errorMsg };
    } finally {
      setLoading(false);
    }
  }, [apiService]);

  return {
    // State
    connections,
    foreignTables,
    loading,
    error,
    healthStatus,
    backendUrl,
    diagnostics,
    
    // Connection Operations
    testConnection,
    connect,
    disconnect,
    refreshConnections,
    
    // Foreign Table Operations
    createForeignServer,
    createForeignTable,
    createForeignTableViaBackend,
    listForeignTables,
    dropForeignTable,
    testForeignTable,
    checkFDWAvailability,
    refreshForeignTables,
    getForeignTableColumns,
    
    // NEW Cascading Delete Operations
    dropForeignTableCascade,
    dropForeignServerIfUnused,
    
    // Schema Operations
    getTables,
    getDatabaseInfo,
    
    // Query Operations
    executeQuery,
    
    // PostgreSQL Operations
    testPostgresStatus,
    
    // Utility Operations
    checkBackendHealth,
    clearError,
    updateBackendUrl,
    diagnoseConnection,
    runConnectionDiagnostics,
    
    // Helper Functions
    getConnectionById: useCallback((connectionId: string) => 
      connections.find(c => c.connectionId === connectionId), 
    [connections]),
    getConnectionsByType: useCallback((dbType: string) => 
      connections.filter(c => c.dbType === dbType), 
    [connections]),
    
    // Raw API Service
    apiService,
  };
}


// ===========================================================================
// Export Default Instance
// ===========================================================================

export const databaseApi = new DatabaseApiService();

// Re-export types
export type {
  DatabaseType,
  ClientDatabaseConfig as DatabaseConfig,
  ClientColumnMetadata as ColumnMetadata,
  ClientTableInfo as TableInfo,
  ClientInspectionOptions as InspectionOptions,
  ClientQueryExecutionOptions as QueryExecutionOptions,
  ClientQueryResult as QueryResult,
  ClientDatabaseVersionInfo as DatabaseVersionInfo,
  ClientConnectionInfo as ConnectionInfo,
  ClientTestConnectionResult as TestConnectionResult,
  ClientConnectResult as ConnectResult,
  ClientTableListResult as TableListResult,
  ClientQueryExecutionResult as QueryExecutionResult,
  ClientDatabaseInfoResult as DatabaseInfoResult,
  ClientDisconnectResult as DisconnectResult,
};

export default databaseApi;