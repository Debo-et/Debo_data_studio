// frontend/src/config/api.ts
export const API_CONFIG = {
  BASE_URL: process.env.REACT_APP_API_BASE_URL || 'http://localhost:3000/api',
  TIMEOUT: 30000,
  ENDPOINTS: {
    DATABASE: {
      TEST_CONNECTION: '/database/test-connection',
      CONNECT: '/database/connect',
      DISCONNECT: '/database/disconnect',
      ACTIVE_CONNECTIONS: '/database/active-connections',
      TABLES: '/database/tables',
      QUERY: '/database/query',
      INFO: '/database/info',
    }
  }
};