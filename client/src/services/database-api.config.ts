// frontend/src/services/database-api.config.ts

import { DatabaseApiService } from './database-api.service';

// Detect environment
const isDevelopment = process.env.NODE_ENV === 'development';
const isProduction = process.env.NODE_ENV === 'production';

// Frontend URL (where React runs)
const FRONTEND_URL = process.env.REACT_APP_FRONTEND_URL || 
                    (typeof window !== 'undefined' ? window.location.origin : 'http://localhost:3001');

// Backend URL (where Express runs)
const BACKEND_URL = process.env.REACT_APP_BACKEND_URL || 
                   (isProduction ? FRONTEND_URL : 'http://localhost:3000');

console.log('📊 Database API Configuration:', {
  environment: process.env.NODE_ENV || 'development',
  isDevelopment,
  isProduction,
  backendUrl: BACKEND_URL,
  frontendUrl: FRONTEND_URL,
  nodeEnv: process.env.NODE_ENV,
  reactAppBackendUrl: process.env.REACT_APP_BACKEND_URL,
});

// Configuration matching debug-api.js exactly
export const DATABASE_API_CONFIG = {
  development: {
    baseURL: BACKEND_URL, // http://localhost:3000
    frontendURL: FRONTEND_URL, // http://localhost:3001
    timeout: 30000,
    endpoints: {
      // All endpoints now match exactly what debug-api.js uses
      health: '/health',
      postgresStatus: '/api/postgres/status',
      testConnection: '/api/database/test-connection',
      connect: '/api/database/connect',
      disconnect: '/api/database/{connectionId}', // DELETE method
      activeConnections: '/api/database/connections/active',
      tables: '/api/database/tables',
      query: '/api/database/{connectionId}/query',
      info: '/api/database/{connectionId}/info',
      postgresQuery: '/api/postgres/query',
      schemas: '/api/database/schemas',
      constraints: '/api/database/constraints',
      functions: '/api/database/functions',
      indexes: '/api/database/indexes',
      sessions: '/api/database/sessions',
    }
  },
  production: {
    baseURL: BACKEND_URL, // Use current origin in production
    frontendURL: FRONTEND_URL,
    timeout: 60000,
    endpoints: {
      // Same endpoint structure, just different base
      health: '/health',
      postgresStatus: '/api/postgres/status',
      testConnection: '/api/database/test-connection',
      connect: '/api/database/connect',
      disconnect: '/api/database/{connectionId}',
      activeConnections: '/api/database/connections/active',
      tables: '/api/database/tables',
      query: '/api/database/{connectionId}/query',
      info: '/api/database/{connectionId}/info',
      postgresQuery: '/api/postgres/query',
      schemas: '/api/database/schemas',
      constraints: '/api/database/constraints',
      functions: '/api/database/functions',
      indexes: '/api/database/indexes',
      sessions: '/api/database/sessions',
    }
  }
};

// Helper function to get current user (browser-safe)
function getCurrentUser(): string {
  // In browser environment, we can't access system user directly
  // We'll try to get it from environment variables or use a default
  if (typeof window !== 'undefined') {
    // Browser environment
    try {
      const storedUser = localStorage.getItem('last_db_user');
      if (storedUser) return storedUser;
    } catch (e) {
      // localStorage not available
    }
  }
  
  // Try environment variables
  if (typeof process !== 'undefined' && process.env) {
    // Node.js environment (SSR or build time)
    return process.env.REACT_APP_DB_USER || 
           process.env.DB_USER || 
           process.env.USER || 
           process.env.USERNAME || 
           'postgres';
  }
  
  return 'postgres'; // Default fallback
}

// EXACTLY the same configuration used in debug-api.js LOCAL_POSTGRES_CONFIG
export const DEFAULT_POSTGRES_CONFIG = {
  dbType: 'postgresql' as const,
  config: {
    host: 'localhost',
    port: '5432',
    dbname: 'postgres',
    user: getCurrentUser(), // Get current user dynamically
    password: '', // Empty password for local connection - matches debug-api.js
    schema: 'public'
  }
};

// Static configuration for local PostgreSQL (from debug-api.js)
export const LOCAL_POSTGRES_CONFIG = {
  dbType: 'postgresql' as const,
  config: {
    host: 'localhost',
    port: '5432',
    dbname: 'postgres',
    user: getCurrentUser(), // Dynamic user
    password: '', // Empty password for local connection
    schema: 'public'
  }
};

// Factory function to create service with proper configuration
export function createDatabaseApiService(environment?: string): DatabaseApiService {
  const env = environment || process.env.NODE_ENV || 'development';
  const config = DATABASE_API_CONFIG[env as keyof typeof DATABASE_API_CONFIG] || 
                DATABASE_API_CONFIG.development;
  
  console.log(`🚀 Creating DatabaseApiService for ${env} environment`);
  console.log(`📡 Base URL: ${config.baseURL}`);
  console.log(`🌐 Frontend URL: ${config.frontendURL}`);
  console.log(`🐘 PostgreSQL Config:`, DEFAULT_POSTGRES_CONFIG.config);
  
  return new DatabaseApiService(config.baseURL);
}

// Create singleton instance
export const databaseApiInstance = createDatabaseApiService();

// Helper function to test the full connection flow (like debug-api.js test-local-postgres)
export async function testLocalPostgresConnection(): Promise<{
  health: any;
  postgresStatus: any;
  testConnection: any;
  connect: any;
  tables: any;
  info: any;
  disconnect: any;
}> {
  const api = databaseApiInstance;
  
  try {
    console.log('🚀 Testing local PostgreSQL connection (matching debug-api.js)...');
    console.log(`🌐 Backend URL: ${api.baseUrl}`);
    console.log(`👤 Using user: ${DEFAULT_POSTGRES_CONFIG.config.user}`);
    
    // 1. Test health
    console.log('\n1️⃣ Testing backend health...');
    const health = await api.testHealth();
    console.log('✅ Health check:', health);
    
    // 2. Test PostgreSQL status
    console.log('\n2️⃣ Testing PostgreSQL status...');
    const postgresStatus = await api.testPostgresStatus();
    console.log('✅ PostgreSQL status:', postgresStatus);
    
    if (!postgresStatus.connected) {
      throw new Error(`PostgreSQL not connected: ${postgresStatus.error}`);
    }
    
    // 3. Test connection
    console.log('\n3️⃣ Testing database connection...');
    const testConnection = await api.testConnection(
      DEFAULT_POSTGRES_CONFIG.dbType,
      DEFAULT_POSTGRES_CONFIG.config
    );
    console.log('✅ Connection test:', testConnection);
    
    if (!testConnection.success) {
      throw new Error(`Connection test failed: ${testConnection.error}`);
    }
    
    // 4. Connect
    console.log('\n4️⃣ Establishing connection...');
    const connect = await api.connect(
      DEFAULT_POSTGRES_CONFIG.dbType,
      DEFAULT_POSTGRES_CONFIG.config
    );
    console.log('✅ Connect:', connect);
    
    if (!connect.success || !connect.connectionId) {
      throw new Error(`Connection failed: ${connect.error}`);
    }
    
    // 5. Get tables
    console.log('\n5️⃣ Getting tables...');
    const tables = await api.getTables(connect.connectionId);
    console.log('✅ Tables:', tables.tables?.length || 0, 'tables found');
    
    // 6. Get database info
    console.log('\n6️⃣ Getting database info...');
    const info = await api.getDatabaseInfo(connect.connectionId);
    console.log('✅ Database info:', info);
    
    // 7. Test query
    console.log('\n7️⃣ Testing query...');
    const queryResult = await api.executeQuery(
      connect.connectionId,
      'SELECT 1 as test_value, current_timestamp as now'
    );
    console.log('✅ Test query:', queryResult);
    
    // 8. Disconnect
    console.log('\n8️⃣ Disconnecting...');
    const disconnect = await api.disconnect(connect.connectionId);
    console.log('✅ Disconnect:', disconnect);
    
    console.log('\n🎉 All tests completed successfully!');
    
    return {
      health,
      postgresStatus,
      testConnection,
      connect,
      tables,
      info,
      disconnect
    };
    
  } catch (error) {
    console.error('\n❌ Local PostgreSQL test failed:', error);
    console.error('\n🔧 Troubleshooting steps:');
    console.error('  1. Ensure backend is running on port 3000');
    console.error('  2. Ensure PostgreSQL is running on port 5432');
    console.error('  3. Check CORS configuration in backend');
    console.error(`  4. Test manually: curl ${api.baseUrl}/health`);
    throw error;
  }
}

// Export debug function
export const debugDatabaseAPI = {
  testLocalPostgres: testLocalPostgresConnection,
  getConfig: () => DEFAULT_POSTGRES_CONFIG,
  getApiInstance: () => databaseApiInstance,
  getBackendUrl: () => databaseApiInstance.baseUrl,
  getCurrentUser: getCurrentUser,
  printConfig: () => {
    console.log('🔧 Current Database API Configuration:');
    console.log('  Environment:', process.env.NODE_ENV);
    console.log('  Backend URL:', databaseApiInstance.baseUrl);
    console.log('  Frontend URL:', FRONTEND_URL);
    console.log('  PostgreSQL Config:', DEFAULT_POSTGRES_CONFIG.config);
  }
};