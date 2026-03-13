declare module 'ibm_db' {
  export interface Connection {
    query(sql: string, params?: any[], callback?: (err: Error, result: any) => void): void;
    close(callback?: (err: Error) => void): void;
  }
  
  export function open(connectionString: string, callback: (err: Error, conn: Connection) => void): void;
}

declare module '@sap/hana-client' {
  export function createConnection(): any;
}

declare module 'node-firebird' {
  export function createConnection(options: any, callback: (err: Error, db: any) => void): void;
}

// Add other missing type declarations as needed