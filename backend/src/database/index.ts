// backend/src/database/index.ts

import DB2Adapter from './adapters/db2.adapter';
import FirebirdAdapter from './adapters/firebird.adapter';
import SAPHANAAdapter from './adapters/hana.adapter';
import InformixAdapter from './adapters/informix.adapter';
import SQLServerAdapter from './adapters/mssql.adapter';
import MySQLAdapter from './adapters/mysql.adapter';
import NetezzaAdapter from './adapters/netezza.adapter';
import OracleAdapter from './adapters/oracle.adapter';
import PostgreSQLAdapter from './adapters/postgresql.adapter';
import SybaseAdapter from './adapters/sybase.adapter';

// Export adapters
export { default as MySQLAdapter } from './adapters/mysql.adapter';
export { default as PostgreSQLAdapter } from './adapters/postgresql.adapter';
export { default as OracleAdapter } from './adapters/oracle.adapter';
export { default as SQLServerAdapter } from './adapters/mssql.adapter';
export { default as DB2Adapter } from './adapters/db2.adapter';
export { default as SAPHANAAdapter } from './adapters/hana.adapter';
export { default as SybaseAdapter } from './adapters/sybase.adapter';
export { default as NetezzaAdapter } from './adapters/netezza.adapter';
export { default as InformixAdapter } from './adapters/informix.adapter';
export { default as FirebirdAdapter } from './adapters/firebird.adapter';

// Export factory
export { DatabaseConnectionFactory } from './connection.factory';

// Export types
export * from './types/database.types';
export * from './types/inspection.types';

// Export base inspector
export { IBaseDatabaseInspector, BaseDatabaseInspector } from './inspection/base-inspector';

// Export adapters as a module
export const Adapters = {
  MySQLAdapter,
  PostgreSQLAdapter,
  OracleAdapter,
  SQLServerAdapter,
  DB2Adapter,
  SAPHANAAdapter,
  SybaseAdapter,
  NetezzaAdapter,
  InformixAdapter,
  FirebirdAdapter
};