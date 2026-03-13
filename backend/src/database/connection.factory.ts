// backend/src/database/connection.factory.ts

// Add imports for new adapters
import { DB2Adapter } from './adapters/db2.adapter';
import { SAPHANAAdapter } from './adapters/hana.adapter';
import { SybaseAdapter } from './adapters/sybase.adapter';
import { NetezzaAdapter } from './adapters/netezza.adapter';
import { InformixAdapter } from './adapters/informix.adapter';
import { FirebirdAdapter } from './adapters/firebird.adapter';
import { MySQLAdapter } from './adapters/mysql.adapter';
import { PostgreSQLAdapter } from './adapters/postgresql.adapter';
import { OracleAdapter } from './adapters/oracle.adapter';
import { SQLServerAdapter } from './adapters/mssql.adapter';
import { IBaseDatabaseInspector } from './inspection/base-inspector';

// Define DatabaseType
export type DatabaseType = 
  | 'mysql'
  | 'postgresql' 
  | 'postgres'
  | 'oracle'
  | 'sqlserver'
  | 'mssql'
  | 'db2'
  | 'sap-hana'
  | 'hana'
  | 'sybase'
  | 'netezza'
  | 'informix'
  | 'firebird';

export class DatabaseConnectionFactory {
  static createAdapter(dbType: DatabaseType): IBaseDatabaseInspector {
    switch (dbType.toLowerCase()) {
      case 'mysql':
        return new MySQLAdapter();
      case 'postgresql':
      case 'postgres':
        return new PostgreSQLAdapter();
      case 'oracle':
        return new OracleAdapter();
      case 'sqlserver':
      case 'mssql':
        return new SQLServerAdapter();
      case 'db2':
        return new DB2Adapter();
      case 'sap-hana':
      case 'hana':
        return new SAPHANAAdapter();
      case 'sybase':
        return new SybaseAdapter();
      case 'netezza':
        return new NetezzaAdapter();
      case 'informix':
        return new InformixAdapter();
      case 'firebird':
        return new FirebirdAdapter();
      default:
        throw new Error(`Unsupported database type: ${dbType}`);
    }
  }
}