// backend/src/database/adapters/neo4j.adapter.ts

import { IBaseDatabaseInspector } from '../inspection/base-inspector';
import {
  DatabaseConnection,
  QueryResult,
  TableInfo,
  DatabaseConfig,
  InspectionOptions,
  QueryExecutionOptions,
  DatabaseVersionInfo
} from '../types/inspection.types';

// Assumes the neo4j-driver package is installed:
// npm install neo4j-driver
import neo4j, {
  Driver,
  Session,
  Record as Neo4jRecord   // alias to avoid shadowing built‑in Record<K,V>
} from 'neo4j-driver';

// -------- Neo4j Connection Wrapper --------
export class Neo4jConnection implements DatabaseConnection {
  private driver: Driver | null = null;
  private session: Session | null = null;
  private database: string = 'neo4j'; // default database name

  constructor() {}

  async connect(
    uri: string,
    user: string,
    password: string,
    database?: string
  ): Promise<void> {
    this.driver = neo4j.driver(uri, neo4j.auth.basic(user, password));
    this.database = database || 'neo4j';
    this.session = this.driver.session({ database: this.database });
  }

  async disconnect(): Promise<void> {
    if (this.session) {
      await this.session.close();
      this.session = null;
    }
    if (this.driver) {
      await this.driver.close();
      this.driver = null;
    }
  }

  get connected(): boolean {
    return this.session !== null;
  }

  getDriver(): Driver | null {
    return this.driver;
  }

  getSession(): Session | null {
    return this.session;
  }

  getDatabase(): string {
    return this.database;
  }
}

// -------- Neo4j Inspector --------
class Neo4jInspector {
  private connection: Neo4jConnection;
  private config: DatabaseConfig;
  private sessionFn: () => Session;

  constructor(config: DatabaseConfig) {
    this.config = {
      host: config.host || 'localhost',
      port: config.port || '7687',       // Bolt port
      user: config.user || 'neo4j',
      password: config.password || 'neo4j',
      dbname: config.dbname || 'neo4j',
      schema: '',                         // unused
    };
    this.connection = new Neo4jConnection();
    this.sessionFn = () => this.connection.getSession()!;
  }

  getConnection(): Neo4jConnection {
    return this.connection;
  }

  async connect(): Promise<void> {
    const uri = `bolt://${this.config.host}:${this.config.port}`;
    await this.connection.connect(
      uri,
      this.config.user!,
      this.config.password!,
      this.config.dbname
    );
    await this.sessionFn().run('RETURN 1');
  }

  setSchema(_schema: string): void {
    // No-op
  }

  async testConnection(): Promise<{ success: boolean; version?: string; error?: string }> {
    try {
      const driver = neo4j.driver(
        `bolt://${this.config.host}:${this.config.port}`,
        neo4j.auth.basic(this.config.user!, this.config.password!)
      );
      const session = driver.session({ database: this.config.dbname });
      try {
        const result = await session.run(
          'CALL dbms.components() YIELD versions RETURN versions[0] AS version'
        );
        const version = result.records[0]?.get('version') || 'unknown';
        await session.close();
        await driver.close();
        return { success: true, version };
      } catch (innerErr) {
        await session.close();
        await driver.close();
        throw innerErr;
      }
    } catch (err: any) {
      return { success: false, error: err.message || String(err) };
    }
  }

  async getTables(): Promise<TableInfo[]> {
    const session = this.sessionFn();
    if (!session) throw new Error('Not connected');

    const tables: TableInfo[] = [];

    // Node labels
    const labelResult = await session.run(`
      CALL db.labels() YIELD label
      RETURN label, size([label IN [$label] | 1]) AS count
      ORDER BY label
    `);
    for (const record of labelResult.records) {
      const label = record.get('label');
      const count = record.get('count').toNumber
        ? record.get('count').toNumber()
        : Number(record.get('count'));
      tables.push({
        schemaname: '',
        tablename: `:${label}`,
        tabletype: 'node-label',
        columns: [],
        comment: '',
        rowCount: count,
        size: '',
        originalData: null,
      });
    }

    // Relationship types
    const relResult = await session.run(`
      CALL db.relationshipTypes() YIELD relationshipType
      RETURN relationshipType
      ORDER BY relationshipType
    `);
    for (const record of relResult.records) {
      const relType = record.get('relationshipType');
      tables.push({
        schemaname: '',
        tablename: `:${relType}`,
        tabletype: 'relationship-type',
        columns: [],
        comment: '',
        rowCount: 0,
        size: '',
        originalData: null,
      });
    }

    return tables;
  }

  async getDatabaseInfo(): Promise<{
    version: string;
    name: string;
    encoding: string;
    collation: string;
  }> {
    const session = this.sessionFn();
    if (!session) throw new Error('Not connected');
    const result = await session.run(`
      CALL dbms.components() YIELD name, versions
      RETURN name, versions[0] AS version
    `);
    const r = result.records[0];
    return {
      version: r?.get('version') || 'unknown',
      name: r?.get('name') || 'Neo4j',
      encoding: 'UTF-8',
      collation: '',
    };
  }

  /**
   * Execute a Cypher query.
   */
  async executeQuery(
    sql: string,
    params: any[] = [],
    options?: { maxRows?: number; timeout?: number; autoDisconnect?: boolean }
  ): Promise<QueryResult> {
    const session = this.sessionFn();
    if (!session) throw new Error('Not connected');

    const parameterMap: Record<string, any> = {};
    params.forEach((val, idx) => {
      parameterMap[`p${idx}`] = val;
    });

    let finalSql = sql;
    if (sql.includes('?')) {
      let idx = 0;
      finalSql = sql.replace(/\?/g, () => `$p${idx++}`);
    }

    const start = Date.now();
    try {
      const runOptions: { timeout?: number } = {};
      if (options?.timeout) {
        runOptions.timeout = options.timeout * 1000; // seconds → ms
      }

      const result = await session.run(finalSql, parameterMap, runOptions);

      const rows = result.records.map((record: Neo4jRecord) => {
        const row: any = {};
        record.keys.forEach(key => {
          row[key] = record.get(key);
        });
        return row;
      });

      let limitedRows = rows;
      if (options?.maxRows && rows.length > options.maxRows) {
        limitedRows = rows.slice(0, options.maxRows);
      }

      // Build fields with explicit string conversion for name
      const fields = result.records.length > 0
        ? result.records[0].keys.map(key => {
            const val = result.records[0].get(key);
            let type = 'string';
            if (val === null || val === undefined) {
              type = 'null';
            } else if (neo4j.isInt(val)) {
              type = 'integer';
            } else if (typeof val === 'number') {
              type = 'float';
            } else if (val instanceof Date) {
              type = 'date';
            } else if (Array.isArray(val)) {
              type = 'array';
            } else if (typeof val === 'object') {
              type = 'object';
            } else {
              type = typeof val;
            }
            return { name: String(key), type };   // <-- String() fixes PropertyKey → string
          })
        : [];

      return {
        success: true,
        rows: limitedRows,
        fields,
        rowCount: limitedRows.length,
        executionTime: Date.now() - start,
      };
    } catch (error: any) {
      return {
        success: false,
        error: error.message || String(error),
        rows: [],
        fields: [],
        executionTime: Date.now() - start,
      };
    }
  }

  async executeTransaction(
    queries: Array<{ sql: string; params?: any[] }>
  ): Promise<QueryResult[]> {
    const results: QueryResult[] = [];
    for (const q of queries) {
      results.push(await this.executeQuery(q.sql, q.params || []));
    }
    return results;
  }
}

// -------- Neo4j Adapter --------
export class Neo4jAdapter implements IBaseDatabaseInspector {
  private inspector: Neo4jInspector | null = null;
  private connectionInstance: Neo4jConnection | null = null;

  async connect(config: DatabaseConfig): Promise<DatabaseConnection> {
    try {
      this.inspector = new Neo4jInspector(config);
      await this.inspector.connect();
      this.connectionInstance = this.inspector.getConnection();
      return this.connectionInstance as unknown as DatabaseConnection;
    } catch (error) {
      throw new Error(
        `Failed to connect to Neo4j: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  async disconnect(_connection: DatabaseConnection): Promise<void> {
    try {
      if (this.connectionInstance) {
        await this.connectionInstance.disconnect();
      }
      this.inspector = null;
      this.connectionInstance = null;
    } catch (error) {
      console.error('Error disconnecting from Neo4j:', error);
    }
  }

  async testConnection(
    config: DatabaseConfig
  ): Promise<{ success: boolean; version?: string; error?: string }> {
    const tempInspector = new Neo4jInspector(config);
    return tempInspector.testConnection();
  }

  async getTables(
    _connection: DatabaseConnection,
    _options?: InspectionOptions
  ): Promise<TableInfo[]> {
    if (!this.inspector) throw new Error('Inspector not initialized.');
    return this.inspector.getTables();
  }

  async getTableColumns(
    _connection: DatabaseConnection,
    tables: TableInfo[]
  ): Promise<TableInfo[]> {
    return tables;
  }

  async getDatabaseInfo(_connection: DatabaseConnection): Promise<DatabaseVersionInfo> {
    if (!this.inspector) throw new Error('Inspector not initialized.');
    const info = await this.inspector.getDatabaseInfo();
    return {
      version: info.version,
      name: info.name,
      encoding: info.encoding,
      collation: info.collation,
    };
  }

  async executeQuery(
    _connection: DatabaseConnection,
    sql: string,
    options?: QueryExecutionOptions
  ): Promise<QueryResult> {
    if (!this.inspector) throw new Error('Inspector not initialized.');
    const params = options?.params || [];
    const neo4jOptions = {
      maxRows: options?.maxRows,
      timeout: options?.timeout,
      autoDisconnect: options?.autoDisconnect,
    };
    return this.inspector.executeQuery(sql, params, neo4jOptions);
  }

  async executeTransaction(
    _connection: DatabaseConnection,
    queries: Array<{ sql: string; params?: any[] }>
  ): Promise<QueryResult[]> {
    if (!this.inspector) throw new Error('Inspector not initialized.');
    return this.inspector.executeTransaction(queries);
  }

  async getTableConstraints(
    _connection: DatabaseConnection,
    _schema: string,
    _table: string
  ): Promise<any[]> {
    if (!this.inspector) throw new Error('Inspector not initialized.');
    const session = this.connectionInstance?.getSession();
    if (!session) return [];
    try {
      const result = await session.run('SHOW CONSTRAINTS');
      return result.records.map(r => r.toObject());
    } catch {
      return [];
    }
  }

  async getSchemas(_connection: DatabaseConnection): Promise<string[]> {
    return [];
  }

  async getFunctions(_connection: DatabaseConnection, _schema?: string): Promise<any[]> {
    if (!this.inspector) throw new Error('Inspector not initialized.');
    const session = this.connectionInstance?.getSession();
    if (!session) return [];
    try {
      const result = await session.run('SHOW FUNCTIONS');
      return result.records.map(r => r.toObject());
    } catch {
      return [];
    }
  }

  async getIndexes(_connection: DatabaseConnection, tableName?: string): Promise<any[]> {
    if (!this.inspector) throw new Error('Inspector not initialized.');
    const session = this.connectionInstance?.getSession();
    if (!session) return [];
    let cypher = 'SHOW INDEXES';
    if (tableName) {
      const label = tableName.startsWith(':') ? tableName.substring(1) : tableName;
      cypher = `SHOW INDEXES WHERE entityType = 'NODE' AND labelsOrTypes = '${label}'`;
    }
    try {
      const result = await session.run(cypher);
      return result.records.map(r => r.toObject());
    } catch {
      return [];
    }
  }
}

export default Neo4jAdapter;