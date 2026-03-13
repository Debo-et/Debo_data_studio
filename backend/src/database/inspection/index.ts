// backend/src/database/inspection/index.ts

export { BaseDatabaseInspector, IBaseDatabaseInspector } from './base-inspector';

// Export existing inspectors (these should be moved to this folder)
export { default as MySQLInspector } from './mysql-inspector';
export { default as OracleInspector } from './oracle-inspector';
export { default as PostgreSQLInspector } from './postgreSql-inspector';
export { default as SQLServerInspector } from './sqlserver-inspector';

// Export types
export * from '../types/inspection.types';