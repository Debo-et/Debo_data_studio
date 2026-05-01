// backend/src/database/adapters/neo4j.adapter.ts

import { IBaseDatabaseInspector } from '../inspection/base-inspector';
import {
  DatabaseConnection,
  QueryResult,
  TableInfo,
  ColumnMetadata,
  DatabaseConfig,
  InspectionOptions,
  QueryExecutionOptions,
  DatabaseVersionInfo
} from '../types/inspection.types';

// Assumes the neo4j-driver package is installed:
// npm install neo4j-driver
import neo4j, { Driver, Session, Result, Record } from 'neo4j-driver';

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
  private sessionFn: () => Session; // helper to get session

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
    // Verify connectivity
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
        const result = await session.run('CALL dbms.components() YIELD versions RETURN versions[0] AS version');
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

  /**
   * Get tables = node labels and relationship types.
   * Columns are left empty (schema‑less), but we include row counts and an empty size.
   */
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
      const count = record.get('count').toNumber ? record.get('count').toNumber() : Number(record.get('count'));
      tables.push({
        schemaname: '',
        tablename: `:${label}`,          // prefix with colon to indicate label
        tabletype: 'node-label',
        columns: [],
        comment: '',
        rowCount: count,
        size: 0,
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
        rowCount: 0,   // would require counting relationships for each type
        size: 0,
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
    const record = result.records[0];
    return {
      version: record?.get('version') || 'unknown',
      name: record?.get('name') || 'Neo4j',
      encoding: 'UTF-8',
      collation: '',
    };
  }

  /**
   * Execute a Cypher query.
   * The `sql` parameter contains the Cypher statement.
   * `params` are passed as query parameters.
   */
  async executeQuery(
    sql: string,
    params: any[] = [],
    options?: { maxRows?: number; timeout?: number; autoDisconnect?: boolean }
  ): Promise<QueryResult> {
    const session = this.sessionFn();
    if (!session) throw new Error('Not connected');

    // Map positional params to named parameters: $p0, $p1, ...
    const parameterMap: Record<string, any> = {};
    params.forEach((val, idx) => {
      parameterMap[`p${idx}`] = val;
    });

    // Optionally replace placeholders in the query from positional (?) to named ($p0, ...)
    // but Neo4j only supports named parameters. We'll assume the query uses $p0,... or we can transform.
    // For simplicity, we expect the caller to use named params. If positional needed, we can transform:
    // We'll attempt to replace `?` with $p0 etc. But that's fragile. We'll just document that.
    // Instead, we can provide a simple helper: if sql contains `?`, replace them sequentially.
    let finalSql = sql;
    if (sql.includes('?')) {
      let idx = 0;
      finalSql = sql.replace(/\?/g, () => `$p${idx++}`);
    }

    const start = Date.now();
    try {
      const result: Result = await session.run(finalSql, parameterMap, {
        timeout: options?.timeout ? options.timeout * 1000 : undefined, // seconds to ms
      });

      // Transform Neo4j records to plain objects
      const rows = result.records.map((record: Record) => {
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

      return {
        success: true,
        rows: limitedRows,
        fields: result.records.length > 0 ? result.records[0].keys : [],
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
    // Neo4j supports explicit transactions via session.beginTransaction().
    // For simplicity, we'll run each query sequentially within the same session.
    // A real adapter might wrap them in a transaction.
    const results: QueryResult[] = [];
    for (const q of queries) {
      results.push(await this.executeQuery(q.sql, q.params || []));
    }
    return results;
  }
}

// -------- Neo4j Adapter (Implements IBaseDatabaseInspector) --------
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

  async getTables(_connection: DatabaseConnection, options?: InspectionOptions): Promise<TableInfo[]> {
    if (!this.inspector) throw new Error('Inspector not initialized. Call connect() first.');
    return this.inspector.getTables();
  }

  async getTableColumns(_connection: DatabaseConnection, tables: TableInfo[]): Promise<TableInfo[]> {
    // Tables do not have columns defined yet. Could optionally fetch property keys for each label.
    return tables;
  }

  async getDatabaseInfo(_connection: DatabaseConnection): Promise<DatabaseVersionInfo> {
    if (!this.inspector) throw new Error('Inspector not initialized. Call connect() first.');
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
    // Neo4j constraints (uniqueness, existence, keys) can be fetched with SHOW CONSTRAINTS.
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
    // No schema concept; return empty.
    return [];
  }

  // -------- Neo4j-specific additions --------

  async getFunctions(_connection: DatabaseConnection, _schema?: string): Promise<any[]> {
    // User-defined procedures/functions can be listed with SHOW PROCEDURES / SHOW FUNCTIONS.
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

  async getIndexes(connection: DatabaseConnection, tableName?: string): Promise<any[]> {
    if (!this.inspector) throw new Error('Inspector not initialized.');
    const session = this.connectionInstance?.getSession();
    if (!session) return [];
    let cypher = 'SHOW INDEXES';
    if (tableName) {
      // tableName is like ":Label", extract label without colon
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