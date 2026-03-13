/**
 * sybase.ts - Refined Implementation
 * 
 * Comprehensive Sybase ASE schema inspection with proper error handling,
 * security, and integration with DatabaseMetadataWizard.
 */

// -------------------- Enhanced Type Definitions --------------------

export interface SybaseConnectionConfig {
  server: string;
  port: number;
  database: string;
  username: string;
  password: string;
  schema?: string;
  tds_version?: string;
  charset?: string;
  timeout?: number;
  applicationName?: string;
}

export interface SybaseTableMetadata {
  schemaname: string;
  tablename: string;
  tabletype: 'table' | 'view' | 'system table' | 'synonym';
  owner?: string;
  columns: SybaseColumnMetadata[];
  rowCount?: number;
  createDate?: Date;
  originalData?: any;
}

export interface SybaseColumnMetadata {
  name: string;
  type: string;
  sybaseType: string;
  length: number | null;
  precision: number | null;
  scale: number | null;
  nullable: boolean;
  defaultValue: string | null;
  identity: boolean;
  primaryKey: boolean;
  computed: boolean;
  collation: string | null;
  comment?: string;
  originalData?: any;
}

export interface SybaseIndexMetadata {
  name: string;
  table: string;
  schema: string;
  unique: boolean;
  primary: boolean;
  columns: string[];
}

export interface SybaseConstraintMetadata {
  name: string;
  table: string;
  schema: string;
  type: 'PRIMARY KEY' | 'FOREIGN KEY' | 'UNIQUE' | 'CHECK';
  columns: string[];
  referencedTable?: string;
  referencedSchema?: string;
}

// -------------------- Core Client Interface --------------------

export type Row = { [key: string]: any };

export interface SybaseClient {
  connect(connStr: string): Promise<any>;
  close(connection: any): Promise<void>;
  executeNonQuery(connection: any, sql: string): Promise<void>;
  executeQuery(connection: any, sql: string): Promise<Row[]>;
  executeScalar?(connection: any, sql: string): Promise<any>;
  getLastError(connection: any): Promise<string | null>;
}

export interface ArchiveHandle {
  client: SybaseClient;
  connection?: SYBASE_Connection | null;
  verbose?: boolean;
  options?: { [k: string]: any };
}

export interface SYBASE_Connection {
  connection: any;
  context?: any;
  resultType?: number | null;
  module?: SybaseModule;
  config?: SybaseConnectionConfig;
}

export interface ConnParams {
  server?: string;
  port?: number | string;
  username?: string;
  password?: string;
  database?: string;
  tds_version?: string;
  schema?: string;
  [k: string]: any;
}

// -------------------- Enhanced Sybase Module --------------------

export class SybaseModule {
  private connection: any = null;
  private client: SybaseClient;
  private config!: SybaseConnectionConfig; // Definite assignment assertion
  private isConnected: boolean = false;
  private version: string = '';
  private versionNumeric: number = 0;

  constructor(client: SybaseClient) {
    this.client = client;
  }

  /**
   * Enhanced connection with version detection and validation
   */
  async connect(config: SybaseConnectionConfig): Promise<void> {
    this.config = config;
    
    try {
      const connectionParams = this.buildConnectionParams(config);
      this.connection = await this.client.connect(connectionParams);
      this.isConnected = true;
      
      // Detect server version
      await this.detectServerVersion();
      
      // Verify connection with version-specific capabilities
      await this.validateConnection();
      
    } catch (error: unknown) {
      this.isConnected = false;
      this.connection = null;
      const errorMessage = error instanceof Error ? error.message : 'Unknown connection error';
      throw new Error(`Sybase connection failed: ${errorMessage}`);
    }
  }

  private buildConnectionParams(config: SybaseConnectionConfig): any {
    const params: any = {
      server: config.server,
      port: config.port,
      database: config.database,
      user: config.username,
      password: config.password,
      tdsVersion: config.tds_version || '7.4',
      charset: config.charset || 'utf8',
      timeout: config.timeout || 30000
    };

    if (config.applicationName) {
      params.appname = config.applicationName;
    }

    return params;
  }

  private async detectServerVersion(): Promise<void> {
    try {
      const versionResult = await this.client.executeQuery(
        this.connection, 
        "SELECT @@version as version"
      );
      
      if (versionResult.length > 0) {
        this.version = versionResult[0].version || '';
        this.versionNumeric = this.parseVersionNumber(this.version);
      }
    } catch (error: unknown) {
      console.warn('Could not detect Sybase server version:', error);
      this.version = 'Unknown';
      this.versionNumeric = 0;
    }
  }

  private parseVersionNumber(versionStr: string): number {
    const match = versionStr.match(/(\d+)\.?(\d+)\.?(\d+)?/);
    if (!match) return 0;
    
    const major = parseInt(match[1]) || 0;
    const minor = parseInt(match[2]) || 0;
    const patch = parseInt(match[3]) || 0;
    
    return major * 10000 + minor * 100 + patch;
  }

  private async validateConnection(): Promise<void> {
    try {
      // Test with a simple system table query
      await this.client.executeQuery(
        this.connection, 
        "SELECT TOP 1 name FROM sysobjects WHERE type = 'U'"
      );
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown validation error';
      throw new Error(`Connection validation failed: ${errorMessage}`);
    }
  }

  /**
   * Comprehensive table metadata retrieval
   */
  async getTables(schema?: string): Promise<SybaseTableMetadata[]> {
    this.ensureConnected();

    try {
      const sql = this.buildTableQuery(schema);
      const rows = await this.client.executeQuery(this.connection, sql);
      
      return this.normalizeTableMetadata(rows);
      
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown table retrieval error';
      throw new Error(`Failed to retrieve tables: ${errorMessage}`);
    }
  }

  private buildTableQuery(schema?: string): string {
    const schemaCondition = schema 
      ? `AND u.name = '${this.escapeSql(schema)}'`
      : '';

    return `
      SELECT 
        u.name as schemaname,
        o.name as tablename,
        CASE o.type 
          WHEN 'U' THEN 'table'
          WHEN 'V' THEN 'view' 
          WHEN 'S' THEN 'system table'
          WHEN 'SQ' THEN 'synonym'
          ELSE o.type
        END as tabletype,
        u.name as owner,
        o.crdate as createdate,
        (SELECT i.rowcnt FROM sysindexes i WHERE i.id = o.id AND i.indid = 0) as row_count
      FROM sysobjects o
      JOIN sysusers u ON o.uid = u.uid
      WHERE o.type IN ('U', 'V', 'S', 'SQ')
        ${schemaCondition}
        AND o.name NOT LIKE 'sys%'
      ORDER BY schemaname, tablename
    `;
  }

  /**
   * Comprehensive column metadata retrieval
   */
  async getTableColumns(table: SybaseTableMetadata): Promise<SybaseColumnMetadata[]> {
    this.ensureConnected();

    try {
      const sql = this.buildColumnQuery(table);
      const rows = await this.client.executeQuery(this.connection, sql);
      
      return this.normalizeColumnMetadata(rows);
      
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown column retrieval error';
      throw new Error(`Failed to retrieve columns for table ${table.schemaname}.${table.tablename}: ${errorMessage}`);
    }
  }

  private buildColumnQuery(table: SybaseTableMetadata): string {
    const escapedTableName = this.escapeSql(table.tablename);
    const escapedSchemaName = this.escapeSql(table.schemaname);

    return `
      SELECT 
        c.name as column_name,
        t.name as data_type,
        c.length,
        c.prec as precision,
        c.scale,
        CASE WHEN c.status & 8 = 8 THEN 1 ELSE 0 END as is_nullable,
        CASE WHEN c.status & 128 = 128 THEN 1 ELSE 0 END as is_identity,
        CASE WHEN c.status & 256 = 256 THEN 1 ELSE 0 END as is_computed,
        com.text as default_value,
        NULL as collation,
        NULL as comment
      FROM syscolumns c
      JOIN systypes t ON c.usertype = t.usertype
      JOIN sysobjects o ON c.id = o.id
      JOIN sysusers u ON o.uid = u.uid
      LEFT JOIN syscomments com ON c.cdefault = com.id
      WHERE o.name = '${escapedTableName}'
        AND u.name = '${escapedSchemaName}'
      ORDER BY c.colid
    `;
  }

  /**
   * Get primary key information
   */
  async getPrimaryKeys(table: SybaseTableMetadata): Promise<string[]> {
    this.ensureConnected();

    try {
      const escapedTableName = this.escapeSql(table.tablename);
      const escapedSchemaName = this.escapeSql(table.schemaname);

      const sql = `
        SELECT c.name as column_name
        FROM sysindexes i
        JOIN sysindexkeys k ON i.id = k.id AND i.indid = k.indid
        JOIN syscolumns c ON c.id = k.id AND c.colid = k.colid
        JOIN sysobjects o ON i.id = o.id
        JOIN sysusers u ON o.uid = u.uid
        WHERE i.status & 2048 = 2048
          AND o.name = '${escapedTableName}'
          AND u.name = '${escapedSchemaName}'
        ORDER BY k.keyno
      `;
      
      const rows = await this.client.executeQuery(this.connection, sql);
      return rows.map((row: any) => row.column_name);
      
    } catch (error: unknown) {
      console.warn(`Failed to retrieve primary keys for ${table.schemaname}.${table.tablename}:`, error);
      return [];
    }
  }

  /**
   * Enhanced metadata normalization
   */
  private normalizeTableMetadata(rows: any[]): SybaseTableMetadata[] {
    return rows.map(row => ({
      schemaname: row.schemaname || 'dbo',
      tablename: row.tablename,
      tabletype: this.mapTableType(row.tabletype),
      owner: row.owner,
      createDate: row.createdate,
      rowCount: row.row_count || 0,
      columns: [],
      originalData: row
    }));
  }

  private normalizeColumnMetadata(rows: any[]): SybaseColumnMetadata[] {
    return rows.map(row => ({
      name: row.column_name,
      type: sybase_type_to_pg_type(row.data_type, row.length, row.precision, row.scale),
      sybaseType: row.data_type,
      length: row.length,
      precision: row.precision,
      scale: row.scale,
      nullable: row.is_nullable === 1,
      defaultValue: row.default_value,
      identity: row.is_identity === 1,
      primaryKey: false, // Will be populated separately
      computed: row.is_computed === 1,
      collation: row.collation,
      comment: row.comment,
      originalData: row
    }));
  }

  private mapTableType(sybaseType: string): 'table' | 'view' | 'system table' | 'synonym' {
    const type = (sybaseType || '').toLowerCase();
    if (type.includes('view')) return 'view';
    if (type.includes('system')) return 'system table';
    if (type.includes('synonym')) return 'synonym';
    return 'table';
  }

  /**
   * Security utilities
   */
  private escapeSql(value: string): string {
    if (!value) return '';
    return value.replace(/'/g, "''");
  }


  private ensureConnected(): void {
    if (!this.isConnected || !this.connection) {
      throw new Error('Sybase connection is not established. Please connect first.');
    }
  }

  /**
   * Connection management
   */
  async disconnect(): Promise<void> {
    if (this.connection && this.isConnected) {
      try {
        await this.client.close(this.connection);
      } catch (error: unknown) {
        console.error('Error during Sybase disconnection:', error);
      } finally {
        this.isConnected = false;
        this.connection = null;
      }
    }
  }

  getConnectionStatus(): { isConnected: boolean; version: string; config?: SybaseConnectionConfig } {
    return {
      isConnected: this.isConnected,
      version: this.version,
      config: this.config
    };
  }

  /**
   * Utility methods for external use
   */
  getVersion(): string {
    return this.version;
  }

  getVersionNumeric(): number {
    return this.versionNumeric;
  }

  async testQuery(sql: string): Promise<Row[]> {
    this.ensureConnected();
    return await this.client.executeQuery(this.connection, sql);
  }
}

// -------------------- Legacy Function Compatibility --------------------

/**
 * ConnectDatabaseAhx - Legacy compatibility function
 */
export async function ConnectDatabaseAhx(
  AH: ArchiveHandle,
  cparams: ConnParams,
  _isReconnect: boolean = false
): Promise<void> {
  if (!AH.client) {
    throw new Error('Sybase client not available in ArchiveHandle');
  }

  const config: SybaseConnectionConfig = {
    server: cparams.server || 'localhost',
    port: parseInt(String(cparams.port)) || 5000,
    database: cparams.database || '',
    username: cparams.username || '',
    password: cparams.password || '',
    schema: cparams.schema,
    tds_version: cparams.tds_version || '7.4',
    applicationName: 'DatabaseMetadataWizard'
  };

  const sybaseModule = new SybaseModule(AH.client);
  
  try {
    await sybaseModule.connect(config);
    
    AH.connection = {
      connection: sybaseModule,
      context: null,
      resultType: null,
      module: sybaseModule,
      config
    };
    
    if (AH.verbose) {
      console.log(`Connected to Sybase ASE ${sybaseModule.getVersion()} successfully`);
    }
    
  } catch (error: unknown) {
    AH.connection = null;
    const errorMessage = error instanceof Error ? error.message : 'Unknown connection error';
    throw new Error(`Sybase connection failed: ${errorMessage}`);
  }
}

/**
 * getTables - Legacy compatibility function
 */
export async function getTables(AH: ArchiveHandle): Promise<any[]> {
  if (!AH.connection?.module) {
    throw new Error('Sybase connection not established. Call ConnectDatabaseAhx first.');
  }

  try {
    const schema = AH.connection.config?.schema;
    const tables = await AH.connection.module.getTables(schema);
    
    // Convert to legacy format if needed
    return tables.map(table => ({
      schemaname: table.schemaname,
      tablename: table.tablename,
      tabletype: table.tabletype,
      owner: table.owner,
      columns: table.columns,
      rowCount: table.rowCount,
      originalData: table.originalData
    }));
    
  } catch (error: unknown) {
    const errorMessage = error instanceof Error ? error.message : 'Unknown table retrieval error';
    throw new Error(`Failed to retrieve tables: ${errorMessage}`);
  }
}

/**
 * getTableAttrs - Legacy compatibility function with enhanced column data
 */
export async function getTableAttrs(
  AH: ArchiveHandle, 
  tables: any[]
): Promise<void> {
  if (!AH.connection?.module) {
    throw new Error('Sybase connection not established');
  }

  for (const table of tables) {
    try {
      // Get columns
      const sybaseTable: SybaseTableMetadata = {
        schemaname: table.schemaname,
        tablename: table.tablename,
        tabletype: table.tabletype as any,
        owner: table.owner,
        columns: [],
        rowCount: table.rowCount,
        originalData: table.originalData
      };
      
      const columns = await AH.connection.module.getTableColumns(sybaseTable);
      
      // Get primary keys
      const primaryKeys = await AH.connection.module.getPrimaryKeys(sybaseTable);
      
      // Mark primary key columns
      const enhancedColumns = columns.map(col => ({
        ...col,
        primaryKey: primaryKeys.includes(col.name)
      }));
      
      table.columns = enhancedColumns;
      
    } catch (error: unknown) {
      console.warn(`Failed to get columns for table ${table.schemaname}.${table.tablename}:`, error);
      table.columns = [];
    }
  }
}

/**
 * DisconnectDatabase - Legacy compatibility function
 */
export async function DisconnectDatabase(AH: ArchiveHandle): Promise<void> {
  if (AH.connection?.module) {
    try {
      await AH.connection.module.disconnect();
    } catch (error: unknown) {
      console.error('Error during Sybase disconnection:', error);
    } finally {
      AH.connection = null;
    }
  }
}

/**
 * ExecuteSqlQuery - Enhanced with module support
 */
export async function ExecuteSqlQuery(AH: ArchiveHandle, query: string): Promise<Row[]> {
  if (!AH.connection) {
    throw new Error('No connection available in ArchiveHandle');
  }

  // Use module if available
  if (AH.connection.module) {
    try {
      return await AH.connection.module.testQuery(query);
    } catch (error: unknown) {
      const diag = await AH.client.getLastError(AH.connection.connection).catch(() => null);
      const errorMessage = error instanceof Error ? error.message : 'Unknown query error';
      const msg = diag ? `Query failed: ${diag}` : errorMessage;
      throw new Error(msg);
    }
  }

  // Fallback to original client
  try {
    return await AH.client.executeQuery(AH.connection.connection, query);
  } catch (error: unknown) {
    const diag = await AH.client.getLastError(AH.connection.connection).catch(() => null);
    const errorMessage = error instanceof Error ? error.message : 'Unknown query error';
    const msg = diag ? `Query failed: ${diag}` : errorMessage;
    throw new Error(msg);
  }
}

/**
 * ExecuteSqlQueryForSingleRow - Enhanced with module support
 */
export async function ExecuteSqlQueryForSingleRow(AH: ArchiveHandle, query: string): Promise<Row | null> {
  const rows = await ExecuteSqlQuery(AH, query);
  return rows.length > 0 ? rows[0] : null;
}

/**
 * ExecuteSqlStatement - Enhanced with module support
 */
export async function ExecuteSqlStatement(AH: ArchiveHandle, query: string): Promise<void> {
  if (!AH.connection) {
    throw new Error('No connection available in ArchiveHandle');
  }

  try {
    if (AH.connection.module) {
      await AH.connection.module.testQuery(query);
    } else {
      await AH.client.executeNonQuery(AH.connection.connection, query);
    }
  } catch (error: unknown) {
    const diag = await AH.client.getLastError(AH.connection.connection).catch(() => null);
    const errorMessage = error instanceof Error ? error.message : 'Unknown statement error';
    const msg = diag ? `Statement failed: ${diag}` : errorMessage;
    throw new Error(msg);
  }
}

// -------------------- Utility Functions (Preserved) --------------------

export function notice_processor(message: string): void {
  console.error(`INFO: ${message}`);
}

export function constructConnStr(keywords: string[], values: (string | undefined | null)[]): string {
  const pairs: string[] = [];
  for (let i = 0; i < keywords.length && i < values.length; i++) {
    const k = keywords[i];
    const v = values[i];
    if (!k) continue;
    if (v === undefined || v === null) continue;
    const trimmed = String(v).trim();
    if (trimmed.length === 0) continue;
    pairs.push(`${k}=${trimmed}`);
  }
  return pairs.join(';');
}

export function sybase_version_to_numeric(versionStr: string | null): number {
  if (!versionStr) return 0;
  const match = versionStr.match(/(\d+)(\.\d+)?(\.\d+)?/);
  if (!match) return 0;
  const parts = match[0].split('.').map((p) => parseInt(p, 10) || 0);
  const major = parts[0] || 0;
  const minor = parts[1] || 0;
  const patch = parts[2] || 0;
  return major * 10000 + minor * 100 + patch;
}

export function sybase_type_to_pg_type(sybaseType: string, length?: number | null, precision?: number | null, scale?: number | null): string {
  const t = (sybaseType || '').toLowerCase();
  const L = length ?? 0;
  const P = precision ?? 0;
  const S = scale ?? 0;

  if (t.includes('varchar') || t.includes('char') || t.includes('text')) {
    if (t.includes('text') || L <= 0 || L > 10485760) {
      return 'text';
    }
    return `character varying(${L})`;
  }

  if (t.includes('int') || t === 'integer' || t === 'smallint' || t === 'tinyint') {
    if (t === 'smallint' || t === 'tinyint') return 'smallint';
    if (t === 'bigint') return 'bigint';
    return 'integer';
  }

  if (t.includes('numeric') || t.includes('decimal')) {
    if (P > 0) return `numeric(${P},${S})`;
    return 'numeric';
  }

  if (t.includes('float') || t.includes('real')) {
    if (t.includes('real')) return 'real';
    return 'double precision';
  }

  if (t.includes('datetime') || t.includes('smalldatetime') || t.includes('date') || t.includes('time')) {
    if (t.includes('date') && !t.includes('datetime')) return 'date';
    if (t.includes('time') && !t.includes('datetime')) return 'time';
    return 'timestamp without time zone';
  }

  if (t.includes('money') || t.includes('smallmoney')) {
    return 'numeric(19,4)';
  }

  if (t.includes('binary') || t.includes('image') || t.includes('varbinary')) {
    return 'bytea';
  }

  if (t.includes('bit')) {
    return 'boolean';
  }

  if (t.includes('uniqueidentifier')) {
    return 'uuid';
  }

  return 'text';
}

export async function sybase_get_single_value(AH: ArchiveHandle, query: string): Promise<string | null> {
  if (AH.client.executeScalar) {
    try {
      const v = await AH.client.executeScalar(AH.connection?.connection, query);
      if (v === null || v === undefined) return null;
      return String(v);
    } catch (err: unknown) {
      const diag = await AH.client.getLastError(AH.connection?.connection).catch(() => null);
      const errorMessage = err instanceof Error ? err.message : 'Unknown scalar error';
      const msg = diag ? `Query failed: ${diag}` : errorMessage;
      throw new Error(msg);
    }
  }

  const rows = await ExecuteSqlQuery(AH, query);
  if (!rows || rows.length === 0) return null;
  const row = rows[0];
  const keys = Object.keys(row);
  if (keys.length === 0) return null;
  const val = row[keys[0]];
  return val === null || val === undefined ? null : String(val);
}

export function GetConnection(AH: ArchiveHandle): SYBASE_Connection | null {
  return AH.connection ?? null;
}

/**
 * Enhanced diagnostic function
 */
export async function getSybaseDiagnostics(AH: ArchiveHandle): Promise<{
  version: string;
  versionNumeric: number;
  database: string;
  schema: string;
  tableCount: number;
  connectionStatus: string;
}> {
  if (!AH.connection?.module) {
    throw new Error('Sybase connection not established');
  }

  const module = AH.connection.module;
  const status = module.getConnectionStatus();
  
  // Get table count
  let tableCount = 0;
  try {
    const tables = await module.getTables(AH.connection.config?.schema);
    tableCount = tables.length;
  } catch (error: unknown) {
    tableCount = -1; // Indicate error
  }

  return {
    version: status.version,
    versionNumeric: module.getVersionNumeric(),
    database: AH.connection.config?.database || '',
    schema: AH.connection.config?.schema || 'dbo',
    tableCount,
    connectionStatus: status.isConnected ? 'Connected' : 'Disconnected'
  };
}

export function attachBasicHandlers(AH: ArchiveHandle) {
  process.on('uncaughtException', async (err) => {
    try {
      await DisconnectDatabase(AH);
    } catch (_) {
      // ignore
    }
    console.error('Uncaught Exception:', err);
    process.exit(1);
  });

  process.on('unhandledRejection', async (reason, promise) => {
    try {
      await DisconnectDatabase(AH);
    } catch (_) {
      // ignore
    }
    console.error('Unhandled Rejection at:', promise, 'reason:', reason);
    process.exit(1);
  });
}

// Export the module class for direct use
export default SybaseModule;