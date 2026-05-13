// src/types/electron.d.ts
export interface ElectronAPI {
  postgres: {
    getConnection: () => Promise<any>;
    restart: () => Promise<boolean>;
    healthCheck: () => Promise<any>;
    onReady: (callback: (event: any, data: any) => void) => void;
    onError: (callback: (event: any, error: string) => void) => void;
    removeAllListeners: (channel: string) => void;
  };
  testDatabaseConnection: (dbType: string, connectionParams: any) => Promise<any>;
  getDatabaseTables: (dbType: string, connectionParams: any) => Promise<any>;
  getTableColumns: (dbType: string, connectionParams: any, tables: any[]) => Promise<any>;
  disconnectDatabase: (dbType: string, connectionId: string) => Promise<any>;
  cleanupDatabaseConnections: () => Promise<any>;
  getDatabaseInfo: () => Promise<any>;
  platform: string;
}

declare global {
  interface Window {
    electronAPI: ElectronAPI;
  }
}