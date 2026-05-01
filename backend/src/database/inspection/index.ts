// backend/src/database/inspection/index.ts

export { BaseDatabaseInspector, IBaseDatabaseInspector } from './base-inspector';

// Relational Databases (RDBMS)
export { default as SybaseInspector } from './sybase-inspector';
export { default as SQLServerInspector } from './sqlserver-inspector';
export { default as SAPHANAInspector } from './sap_hanna-inspector';
export { default as PostgreSQLInspector } from './postgreSql-inspector';
export { default as OracleInspector } from './oracle-inspector';
export { default as InformixInspector } from './informix-inspector';
export { default as FirebirdInspector } from './firebird-inspector';
export { default as DB2Inspector } from './db2-inspector';
export { default as IngresInspector } from './Ingres-inspector';
export { default as VectorWiseInspector } from './vectorWise-inspector';
export { default as MaxDBInspector } from './maxDB-inspector';
export { default as TeradataInspector } from './teradata-inspector';
export { default as VerticaInspector } from './vertica-inspector';
export { default as MySQLInspector } from './mysql-inspector';

// NoSQL & Distributed Data Stores
export { default as HBaseInspector } from './hBase-inspector';
export { default as CassandraInspector } from './cassandra-inspector';
export { default as MongoInspector } from './mongo-inspector';
export { default as CouchbaseInspector } from './couchbase-inspector';
export { default as CouchDbInspector } from './couchDb-inspector';
export { default as Neo4jInspector } from './neo4j-inspector';
export { default as MarkLogicInspector } from './markLogic-inspector';
export { default as ExistInspector } from './exist-inspector';

// Search Engines & Big Data Tools
export { default as ElasticsearchInspector } from './elasticsearch-inspector';
export { default as HiveInspector } from './hive-inspector';
export { default as ImpalaInspector } from './impala-inspector';

// Embedded & Lightweight Databases
export { default as AccessInspector } from './access-inspector';
export { default as SQLiteInspector } from './SQLite-inspector';
export { default as H2Inspector } from './h2-inspectors';
export { default as HsqlDBInspector } from './hsqlDB-inspector';
export { default as JavaDBInspector } from './javaDB-inspector';

// Additional analytics platform
export { default as NetezzaInspector } from './netezza-inspector';

// Export types (preserved from original file)
export * from '../types/inspection.types';