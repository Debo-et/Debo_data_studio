// backend/src/database/connection.factory.ts

// Relational Databases
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
import { IngresAdapter } from './adapters/ingres.adapter';
import { VectorWiseAdapter } from './adapters/vectorwise.adapter';
import { MaxDBAdapter } from './adapters/maxdb.adapter';
import { TeradataAdapter } from './adapters/teradata.adapter';
import { VerticaAdapter } from './adapters/vertica.adapter';

// NoSQL & Distributed Data Stores
import { HBaseAdapter } from './adapters/hBase.adapter';
import { CassandraAdapter } from './adapters/cassandra.adapter';
import { MongoAdapter } from './adapters/mongo.adapter';
import { CouchbaseAdapter } from './adapters/couchbase.adapter';
import { CouchDbAdapter } from './adapters/couchdb.adapter';
import { Neo4jAdapter } from './adapters/neo4j.adapter';
import { MarkLogicAdapter } from './adapters/marklogic.adapter';
import { ExistAdapter } from './adapters/exist.adapter';

// Search Engines & Big Data Tools
import { ElasticsearchAdapter } from './adapters/elasticsearch.adapter';
import { HiveAdapter } from './adapters/hive.adapter';
import { ImpalaAdapter } from './adapters/impala.adapter';

// Embedded & Lightweight Databases
import { AccessAdapter } from './adapters/access.adapter';
import { SQLiteAdapter } from './adapters/sqlite.adapter';
import { H2Adapter } from './adapters/h2.adapter';
import { HsqlDBAdapter } from './adapters/hsqldb.adapter';
import { JavaDBAdapter } from './adapters/javadb.adapter';

import { IBaseDatabaseInspector } from './inspection/base-inspector';

export type DatabaseType = 
  // Relational
  | 'mysql'
  | 'postgresql' | 'postgres'
  | 'oracle'
  | 'sqlserver' | 'mssql'
  | 'db2'
  | 'sap-hana' | 'hana'
  | 'sybase'
  | 'netezza'
  | 'informix'
  | 'firebird'
  | 'ingres'
  | 'vectorwise'
  | 'maxdb'
  | 'teradata'
  | 'vertica'
  // NoSQL
  | 'hbase'
  | 'cassandra'
  | 'mongo' | 'mongodb'
  | 'couchbase'
  | 'couchdb'
  | 'neo4j'
  | 'marklogic'
  | 'exist' | 'existdb'
  // Search & Big Data
  | 'elasticsearch'
  | 'hive'
  | 'impala'
  // Embedded
  | 'access'
  | 'sqlite'
  | 'h2'
  | 'hsqldb'
  | 'javadb' | 'derby';

export class DatabaseConnectionFactory {
  static createAdapter(dbType: DatabaseType): IBaseDatabaseInspector {
    switch (dbType.toLowerCase()) {
      // Relational
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
      case 'ingres':
        return new IngresAdapter();
      case 'vectorwise':
        return new VectorWiseAdapter();
      case 'maxdb':
        return new MaxDBAdapter();
      case 'teradata':
        return new TeradataAdapter();
      case 'vertica':
        return new VerticaAdapter();

      // NoSQL
      case 'hbase':
        return new HBaseAdapter();
      case 'cassandra':
        return new CassandraAdapter();
      case 'mongo':
      case 'mongodb':
        return new MongoAdapter();
      case 'couchbase':
        return new CouchbaseAdapter();
      case 'couchdb':
        return new CouchDbAdapter();
      case 'neo4j':
        return new Neo4jAdapter();
      case 'marklogic':
        return new MarkLogicAdapter();
      case 'exist':
      case 'existdb':
        return new ExistAdapter();

      // Search & Big Data
      case 'elasticsearch':
        return new ElasticsearchAdapter();
      case 'hive':
        return new HiveAdapter();
      case 'impala':
        return new ImpalaAdapter();

      // Embedded
      case 'access':
        return new AccessAdapter();
      case 'sqlite':
        return new SQLiteAdapter();
      case 'h2':
        return new H2Adapter();
      case 'hsqldb':
        return new HsqlDBAdapter();
      case 'javadb':
      case 'derby':
        return new JavaDBAdapter();

      default:
        throw new Error(`Unsupported database type: ${dbType}`);
    }
  }
}