/**
 * SAP HANA Database Utility Functions - REFINED VERSION
 * Comprehensive TypeScript implementation with proper error handling
 */
interface SQLDBC_String {
  toString(): string;
  value?: string;
}

interface SQLDBC_ErrorH {
  getSQLState(): string;
  getErrorCode(): number;
  getErrorText(): string;
}

interface SQLDBC_Statement {
  execute(query: string): SQLDBC_Retcode;
  getResultSet(): SQLDBC_ResultSet | null;
  close(): void;
  setResultSetConcurrency?(type: number): void;
}

interface SQLDBC_ResultSet {
  stmt?: SQLDBC_Statement;
  next(): SQLDBC_Retcode;
  first(): SQLDBC_Retcode;
  close(): void;
  getObject(index: number, type: number, buffer: any, size: number, nullable: boolean): void;
  getColumnCount?(): number;
  getRowCount?(): number;
}

interface SQLDBC_Connection {
  createStatement(): SQLDBC_Statement | null;
  releaseStatement(stmt: SQLDBC_Statement): void;
  close(): void;
  error(): SQLDBC_ErrorH;
  connect(connectionString: string): SQLDBC_Retcode;
  isConnected?(): boolean;
  commit?(): void;
  rollback?(): void;
}

interface SQLDBC_Environment {
  createConnection(): SQLDBC_Connection | null;
  releaseConnection(conn: SQLDBC_Connection): void;
  getError?(): SQLDBC_ErrorH;
}

// Enhanced implementation with proper error handling
class SQLDBC_ConnectionImpl implements SQLDBC_Connection {
  private statements: SQLDBC_Statement[] = [];
  private connected: boolean = false;

  createStatement(): SQLDBC_Statement | null {
    try {
      const stmt = new SQLDBC_StatementImpl();
      this.statements.push(stmt);
      return stmt;
    } catch (error) {
      console.error('Failed to create statement:', error);
      return null;
    }
  }

  releaseStatement(stmt: SQLDBC_Statement): void {
    const index = this.statements.indexOf(stmt);
    if (index > -1) {
      this.statements.splice(index, 1);
      stmt.close();
    }
  }

  close(): void {
    this.statements.forEach(stmt => stmt.close());
    this.statements = [];
    this.connected = false;
  }

  error(): SQLDBC_ErrorH {
    return new SQLDBC_ErrorHImpl();
  }

  connect(connectionString: string): SQLDBC_Retcode {
    try {
      console.log(`Connecting to SAP HANA with: ${this.maskPassword(connectionString)}`);
      // Simulate connection logic
      this.connected = true;
      return SQLDBC_Retcode.OK;
    } catch (error) {
      console.error('Connection failed:', error);
      this.connected = false;
      return SQLDBC_Retcode.ERROR;
    }
  }

  isConnected(): boolean {
    return this.connected;
  }

  private maskPassword(connStr: string): string {
    return connStr.replace(/PWD=([^;]+)/, 'PWD=***');
  }
}

// Enhanced statement implementation
class SQLDBC_StatementImpl implements SQLDBC_Statement {
  private closed: boolean = false;
  private currentResultSet: SQLDBC_ResultSet | null = null;

  execute(query: string): SQLDBC_Retcode {
    if (this.closed) {
      throw new Error('Statement is closed');
    }
    
    try {
      console.log(`Executing SAP HANA query: ${query.substring(0, 100)}...`);
      // Simulate query execution
      this.currentResultSet = new SQLDBC_ResultSetImpl(this);
      return SQLDBC_Retcode.OK;
    } catch (error) {
      console.error('Query execution failed:', error);
      return SQLDBC_Retcode.ERROR;
    }
  }

  getResultSet(): SQLDBC_ResultSet | null {
    return this.currentResultSet;
  }

  close(): void {
    this.closed = true;
    if (this.currentResultSet) {
      this.currentResultSet.close();
      this.currentResultSet = null;
    }
  }

  setResultSetConcurrency(_type: number): void {
    // Implementation for result set concurrency
  }
}

// Enhanced result set implementation
class SQLDBC_ResultSetImpl implements SQLDBC_ResultSet {
  private currentRow: number = -1;
  private totalRows: number = 5; // Mock data
  private closed: boolean = false;

  constructor(public stmt?: SQLDBC_Statement) {}

  next(): SQLDBC_Retcode {
    if (this.closed) {
      return SQLDBC_Retcode.ERROR;
    }
    
    this.currentRow++;
    return this.currentRow < this.totalRows ? SQLDBC_Retcode.OK : SQLDBC_Retcode.NO_DATA_FOUND;
  }

  first(): SQLDBC_Retcode {
    if (this.closed) {
      return SQLDBC_Retcode.ERROR;
    }
    
    this.currentRow = -1;
    return SQLDBC_Retcode.OK;
  }

  close(): void {
    this.closed = true;
  }

  getObject(index: number, type: number, buffer: any, _size: number, _nullable: boolean): void {
    if (this.closed) {
      throw new Error('ResultSet is closed');
    }

    if (buffer && typeof buffer === 'object') {
      switch (type) {
        case SQLDBC_HOSTTYPE.ASCII:
          if (buffer.toString) {
            // Mock different values based on column index and row
            const mockValues = ['TEST_SCHEMA', 'EMPLOYEES', 'TABLE', '2.00.040.00', 'SYS'];
            buffer.toString = () => mockValues[index - 1] || `mock_value_${index}`;
          }
          break;
        case SQLDBC_HOSTTYPE.UINT4:
          if ('value' in buffer) {
            const mockNumbers = [1000, 150, 1, 20000];
            buffer.value = mockNumbers[index - 1] || 12345;
          }
          break;
        case SQLDBC_HOSTTYPE.INT4:
          if ('value' in buffer) {
            buffer.value = 100 + this.currentRow;
          }
          break;
      }
    }
  }

  getColumnCount(): number {
    return 5; // Mock column count
  }

  getRowCount(): number {
    return this.totalRows;
  }
}

// Enhanced error handling
class SQLDBC_ErrorHImpl implements SQLDBC_ErrorH {
  private errorCode: number = 0;
  private sqlState: string = "HY000";
  private errorText: string = "Mock error";

  getSQLState(): string {
    return this.sqlState;
  }

  getErrorCode(): number {
    return this.errorCode;
  }

  getErrorText(): string {
    return this.errorText;
  }

  setError(code: number, state: string, text: string): void {
    this.errorCode = code;
    this.sqlState = state;
    this.errorText = text;
  }
}

// Enhanced environment implementation
class SQLDBC_EnvironmentImpl implements SQLDBC_Environment {
  private connections: SQLDBC_Connection[] = [];

  createConnection(): SQLDBC_Connection | null {
    try {
      const conn = new SQLDBC_ConnectionImpl();
      this.connections.push(conn);
      return conn;
    } catch (error) {
      console.error('Failed to create connection:', error);
      return null;
    }
  }

  releaseConnection(conn: SQLDBC_Connection): void {
    const index = this.connections.indexOf(conn);
    if (index > -1) {
      this.connections.splice(index, 1);
    }
    conn.close();
  }

  getError(): SQLDBC_ErrorH {
    return new SQLDBC_ErrorHImpl();
  }
}

// Static methods for SQLDBC_Environment
class SQLDBC_EnvironmentStatic {
  static createInstance(): SQLDBC_Environment {
    return new SQLDBC_EnvironmentImpl();
  }

  static releaseInstance(env: SQLDBC_Environment): void {
    // Cleanup any environment resources
    if ('getError' in env) {
      console.log('Releasing SQLDBC environment');
    }
  }
}

const SQLDBC_Environment = SQLDBC_EnvironmentStatic;

// Enhanced enums
enum SQLDBC_Retcode {
  OK = 0,
  ERROR = 1,
  NO_DATA_FOUND = 100,
  TIMEOUT = 300
}

enum SQLDBC_HOSTTYPE {
  ASCII = 1,
  UINT4 = 2,
  INT4 = 3,
  UINT8 = 4,
  DOUBLE = 5,
  FLOAT = 6,
  BLOB = 7,
  CLOB = 8
}

// Enhanced interfaces for SAP HANA
interface HANA_Connection {
  connection: SQLDBC_Connection;
  environment: SQLDBC_Environment;
  error: SQLDBC_ErrorH;
  isConnected: boolean;
}

interface ArchiveHandle {
  connection: HANA_Connection | null;
  savedPassword: string | null;
  remoteVersionStr: string | null;
  remoteVersion: number;
  archiveRemoteVersion: string | null;
  minRemoteVersion: number;
  maxRemoteVersion: number;
  isStandby: boolean;
  dopt?: DumpOptions;
  lastError?: string;
}

interface DumpOptions {
  schemaOnly?: boolean;
  dataOnly?: boolean;
  includeTables?: string[];
  excludeTables?: string[];
}

interface ConnParams {
  dbname: string;
  pghost: string | null;
  pgport: string | null;
  username: string | null;
  promptPassword: Trivalue;
  override_dbname: string | null;
}

enum Trivalue {
  NO = 0,
  YES = 1,
  UNKNOWN = 2
}

interface TableInfo {
  dobj: DumpObject;
  relkind: string;
  rolname: string;
  ncheck: number;
  hasindex: boolean;
  relpages: number;
  reltuples: number;
  numatts: number;
  attnames: string[];
  atttypnames: string[];
  interesting: boolean;
  dummy_view: boolean;
  postponed_def: boolean;
  relpersistence: string;
  schemaname: string;
  tablename: string;
  tabletype: string;
  columns: ColumnInfo[];
}

interface ColumnInfo {
  name: string;
  type: string;
  length: number;
  precision: number;
  scale: number;
  nullable: boolean;
  defaultValue: string | null;
  comment: string | null;
  originalData?: any;
}

interface DumpObject {
  objType: number;
  catId: CatalogId;
  name: string;
  dump: number;
  components: number;
  schema?: string;
}

interface CatalogId {
  tableoid: number;
  oid: number;
}

// ===========================================================================
// Enhanced Constants and Configuration
// ===========================================================================

const DO_TABLE = 1;
const DUMP_COMPONENT_DEFINITION = 1;
const DUMP_COMPONENT_DATA = 2;

const HANA_DEFAULT_PORT = "30015";
const HANA_DEFAULT_HOST = "localhost";

// ===========================================================================
// Enhanced Utility Classes
// ===========================================================================

class PQExpBuffer {
  private data: string = '';

  append(str: string): void {
    this.data += str;
  }

  appendChar(ch: string): void {
    this.data += ch;
  }

  appendStr(str: string): void {
    this.data += str;
  }

  reset(): void {
    this.data = '';
  }

  getData(): string {
    return this.data;
  }

  get length(): number {
    return this.data.length;
  }

  printf(format: string, ...args: any[]): void {
    this.data += format.replace(/%s/g, () => args.shift() || '');
  }
}

// ===========================================================================
// Enhanced Low-level Utility Functions
// ===========================================================================

function notice_processor(message: string): void {
  console.log(`SAP HANA INFO: ${message}`);
}

function die_on_query_failure(AH: ArchiveHandle, query: string): never {
  let errmsg: string;
  
  if (AH.connection && AH.connection.error) {
    const error = AH.connection.error;
    errmsg = `SQLState: ${error.getSQLState()}, NativeError: ${error.getErrorCode()}, ${error.getErrorText()}`;
    pg_log_error(`SAP HANA query failed: ${errmsg}`);
  } else {
    errmsg = AH.lastError || "Unknown connection error";
    pg_log_error(`SAP HANA query failed: ${errmsg}`);
  }
  
  pg_log_error_detail(`Failed query: ${query}`);
  throw new Error(`SAP HANA Query Failed: ${errmsg}`);
}

function constructConnStr(keywords: string[], values: string[]): string {
  const buf = new PQExpBuffer();
  let firstkeyword = true;

  for (let i = 0; i < keywords.length && keywords[i]; i++) {
    if (keywords[i] === "password" || keywords[i] === "fallback_application_name") {
      continue;
    }

    if (!firstkeyword) {
      buf.appendChar(';');
    }
    firstkeyword = false;

    const key = keywords[i];
    const value = values[i];

    if (key === "host") {
      buf.append(`SERVERNODE=${value}`);
    } else if (key === "port") {
      buf.append(`PORT=${value}`);
    } else if (key === "user") {
      buf.append(`UID=${value}`);
    } else if (key === "dbname") {
      buf.append(`DATABASENAME=${value}`);
    } else {
      buf.append(`${key}=${value}`);
    }
  }

  return buf.getData();
}

// ===========================================================================
// Enhanced SQL Execution Functions
// ===========================================================================

function ExecuteSqlStatement(AH: ArchiveHandle, query: string): void {
  if (!AH.connection) {
    AH.lastError = "No database connection established";
    throw new Error(AH.lastError);
  }

  const stmt = AH.connection.connection.createStatement();
  if (!stmt) {
    AH.lastError = "Failed to create SQL statement";
    die_on_query_failure(AH, query);
  }

  try {
    const rc = stmt.execute(query);
    if (rc !== SQLDBC_Retcode.OK) {
      AH.lastError = `Statement execution failed with code: ${rc}`;
      stmt.close();
      AH.connection.connection.releaseStatement(stmt);
      die_on_query_failure(AH, query);
    }

    stmt.close();
    AH.connection.connection.releaseStatement(stmt);
  } catch (error: any) {
    AH.lastError = `Statement execution error: ${error.message}`;
    stmt.close();
    AH.connection.connection.releaseStatement(stmt);
    die_on_query_failure(AH, query);
  }
}

function ExecuteSqlQuery(AH: ArchiveHandle, query: string, _status: number): SQLDBC_ResultSet {
  if (!AH.connection) {
    AH.lastError = "No database connection established";
    throw new Error(AH.lastError);
  }

  const stmt = AH.connection.connection.createStatement();
  if (!stmt) {
    AH.lastError = "Failed to create SQL statement";
    die_on_query_failure(AH, query);
  }

  try {
    const rc = stmt.execute(query);
    if (rc !== SQLDBC_Retcode.OK) {
      AH.lastError = `Query execution failed with code: ${rc}`;
      AH.connection.connection.releaseStatement(stmt);
      die_on_query_failure(AH, query);
    }

    const result = stmt.getResultSet();
    if (!result) {
      AH.lastError = "No result set returned from query";
      AH.connection.connection.releaseStatement(stmt);
      die_on_query_failure(AH, query);
    }

    result.stmt = stmt;
    return result;
  } catch (error: any) {
    AH.lastError = `Query execution error: ${error.message}`;
    AH.connection.connection.releaseStatement(stmt);
    die_on_query_failure(AH, query);
  }
}

function ExecuteSqlQueryForSingleRow(AH: ArchiveHandle, query: string): SQLDBC_ResultSet {
  const result = ExecuteSqlQuery(AH, query, 0);
  let row_count = 0;

  try {
    while (result.next() === SQLDBC_Retcode.OK) {
      row_count++;
    }

    result.first();

    if (row_count !== 1) {
      result.close();
      if (result.stmt) {
        result.stmt.close();
        AH.connection!.connection.releaseStatement(result.stmt);
      }
      
      const message = row_count === 0 
        ? `query returned no rows instead of one: ${query}`
        : `query returned ${row_count} rows instead of one: ${query}`;
      
      AH.lastError = message;
      pg_fatal(message);
    }

    return result;
  } catch (error: any) {
    result.close();
    if (result.stmt) {
      result.stmt.close();
      AH.connection!.connection.releaseStatement(result.stmt);
    }
    AH.lastError = `Single row query error: ${error.message}`;
    throw error;
  }
}

function executeQuery(conn: SQLDBC_Connection, query: string): SQLDBC_ResultSet {
  pg_log_info(`executing ${query.substring(0, 100)}...`);

  const stmt = conn.createStatement();
  if (!stmt) {
    pg_log_error("query failed: could not create statement");
    pg_log_error_detail(`Query was: ${query}`);
    exit_nicely(1);
  }

  try {
    const rc = stmt.execute(query);
    if (rc !== SQLDBC_Retcode.OK) {
      conn.releaseStatement(stmt);
      pg_log_error("query failed");
      pg_log_error_detail(`Query was: ${query}`);
      exit_nicely(1);
    }

    const result = stmt.getResultSet();
    if (!result) {
      conn.releaseStatement(stmt);
      pg_log_error("query failed: no result set");
      pg_log_error_detail(`Query was: ${query}`);
      exit_nicely(1);
    }

    result.stmt = stmt;
    return result;
  } catch (error: any) {
    conn.releaseStatement(stmt);
    pg_log_error(`query failed: ${error.message}`);
    pg_log_error_detail(`Query was: ${query}`);
    exit_nicely(1);
  }
}

// ===========================================================================
// Enhanced Database Connection Management
// ===========================================================================

function _check_database_version(AH: ArchiveHandle): void {
  let remoteversion_str: string = "Unknown";
  let remoteversion: number = 0;
  
  try {
    const result = ExecuteSqlQuery(AH, "SELECT VALUE FROM SYS.M_SYSTEM_OVERVIEW WHERE NAME = 'Version'", 0);
    const rc = result.next();
    
    if (rc === SQLDBC_Retcode.OK) {
      const version: SQLDBC_String = { toString: () => "2.00.040.00" };
      result.getObject(1, SQLDBC_HOSTTYPE.ASCII, version, 0, true);
      remoteversion_str = version.toString();
      remoteversion = hana_version_to_numeric(remoteversion_str);
      pg_log_info(`Connected to SAP HANA version: ${remoteversion_str}`);
    }

    result.close();
    if (result.stmt) {
      result.stmt.close();
      AH.connection!.connection.releaseStatement(result.stmt);
    }

    if (remoteversion === 0 || !remoteversion_str) {
      AH.lastError = "Could not determine SAP HANA server version";
      pg_fatal("could not get server version from SAP HANA");
    }

    AH.remoteVersionStr = pg_strdup(remoteversion_str);
    AH.remoteVersion = remoteversion;
    if (!AH.archiveRemoteVersion) {
      AH.archiveRemoteVersion = AH.remoteVersionStr;
    }

    if (remoteversion < AH.minRemoteVersion || remoteversion > AH.maxRemoteVersion) {
      AH.lastError = `Server version mismatch. Server: ${remoteversion_str}, Compatible: ${AH.minRemoteVersion}-${AH.maxRemoteVersion}`;
      pg_log_error("aborting because of server version mismatch");
      pg_log_error_detail(`server version: ${remoteversion_str}; HANA_COMPAT version: HANA_COMPAT`);
      process.exit(1);
    }

    AH.isStandby = false;
  } catch (error: any) {
    AH.lastError = `Version check failed: ${error.message}`;
    throw error;
  }
}

function ConnectDatabase(
  dbname: string | null,
  _connection_string: string | null,
  pghost: string | null,
  pgport: string | null,
  pguser: string | null,
  prompt_password: Trivalue,
  fail_on_error: boolean,
  _progname: string,
  connstr: (string | null)[] | null,
  _server_version: number[] | null,
  password: string | null,
  override_dbname: string | null
): HANA_Connection | null {
  const actual_dbname = override_dbname || dbname;

  if (!actual_dbname && fail_on_error) {
    pg_fatal("Database name is required for SAP HANA connection");
  }

  const env = SQLDBC_Environment.createInstance();
  if (!env) {
    pg_fatal("could not create SAP HANA environment");
  }

  if (prompt_password === Trivalue.YES && !password) {
    password = simple_prompt("SAP HANA Password: ", false);
  }

  let conn_str: string;
  const host = pghost || HANA_DEFAULT_HOST;
  const port = pgport || HANA_DEFAULT_PORT;
  const user = pguser || "";

  if (actual_dbname) {
    conn_str = `SERVERNODE=${host}:${port};DATABASENAME=${actual_dbname};UID=${user};PWD=${password || ""}`;
  } else {
    conn_str = `SERVERNODE=${host}:${port};UID=${user};PWD=${password || ""}`;
  }

  const conn = env.createConnection();
  if (!conn) {
    SQLDBC_Environment.releaseInstance(env);
    if (fail_on_error) {
      pg_fatal("could not create SAP HANA connection");
    } else {
      return null;
    }
  }

  const rc = conn.connect(conn_str);
  if (rc !== SQLDBC_Retcode.OK) {
    if (fail_on_error) {
      const err = conn.error();
      SQLDBC_Environment.releaseInstance(env);
      pg_fatal(`SAP HANA connection failed: SQLState: ${err.getSQLState()}, Error: ${err.getErrorCode()}, ${err.getErrorText()}`);
    } else {
      env.releaseConnection(conn);
      SQLDBC_Environment.releaseInstance(env);
      return null;
    }
  }

  const hana_conn: HANA_Connection = {
    connection: conn,
    environment: env,
    error: conn.error(),
    isConnected: true
  };

  if (connstr && connstr.length > 0) {
    connstr[0] = pg_strdup(conn_str);
  }

  // Verify connection with a simple query
  try {
    const stmt = conn.createStatement();
    if (stmt) {
      const versionRc = stmt.execute("SELECT CURRENT_USER FROM DUMMY");
      if (versionRc === SQLDBC_Retcode.OK) {
        const result = stmt.getResultSet();
        if (result && result.next() === SQLDBC_Retcode.OK) {
          pg_log_info("SAP HANA connection verified successfully");
        }
        if (result) {
          result.close();
        }
      }
      conn.releaseStatement(stmt);
    }
  } catch (error) {
    pg_log_error("Connection verification query failed, but continuing...");
  }

  return hana_conn;
}

function ConnectDatabaseAhx(
  AH: ArchiveHandle,
  cparams: ConnParams,
  isReconnect: boolean
): void {
  if (AH.connection) {
    pg_log_info("Reconnecting to SAP HANA database");
    DisconnectDatabase(AH);
  }

  const prompt_password = isReconnect ? Trivalue.NO : cparams.promptPassword;
  let password = AH.savedPassword;

  if (prompt_password === Trivalue.YES && !password) {
    password = simple_prompt("SAP HANA Password: ", false);
    AH.savedPassword = password;
  }

  try {
    AH.connection = ConnectDatabase(
      cparams.dbname,
      null,
      cparams.pghost,
      cparams.pgport,
      cparams.username,
      prompt_password,
      true,
      "hana_util",
      null,
      null,
      password,
      cparams.override_dbname
    );

    if (!AH.connection) {
      throw new Error("Failed to establish SAP HANA connection");
    }

    _check_database_version(AH);
    set_archive_cancel_info(AH, AH.connection);
    
    pg_log_info(`Successfully connected to SAP HANA database: ${cparams.dbname}`);
  } catch (error: any) {
    AH.lastError = `Connection failed: ${error.message}`;
    throw error;
  }
}

function DisconnectDatabase(AH: ArchiveHandle): void {
  if (!AH.connection) {
    return;
  }

  pg_log_info("Disconnecting from SAP HANA database");

  set_archive_cancel_info(AH, null);

  try {
    AH.connection.connection.close();
    AH.connection.environment.releaseConnection(AH.connection.connection);
    SQLDBC_Environment.releaseInstance(AH.connection.environment);
    AH.connection.isConnected = false;
  } catch (error) {
    pg_log_error("Error during SAP HANA disconnection:" + error);
  } finally {
    AH.connection = null;
  }
}

function GetConnection(AH: ArchiveHandle): HANA_Connection | null {
  return AH.connection;
}

// ===========================================================================
// Enhanced Table Metadata Functions
// ===========================================================================

function getTables(AH: ArchiveHandle, numTables: number[]): TableInfo[] {
  if (!AH.connection) {
    throw new Error("No SAP HANA connection available");
  }

  const query = new PQExpBuffer();
  let ntups = 0;

  query.appendStr(
    "SELECT SCHEMA_NAME, TABLE_NAME, TABLE_TYPE, TABLE_OID, " +
    "CASE WHEN TABLE_TYPE = 'VIEW' THEN 'v' ELSE 'r' END as RELKIND, " +
    "RECORD_COUNT, IS_TEMPORARY, IS_LOGGED " +
    "FROM SYS.TABLES " +
    "WHERE SCHEMA_NAME NOT IN ('SYS', '_SYS_BI', '_SYS_BIC', '_SYS_EPM') " +
    "AND IS_SYSTEM_TABLE = 'FALSE' " +
    "ORDER BY SCHEMA_NAME, TABLE_NAME"
  );

  try {
    const result = ExecuteSqlQuery(AH, query.getData(), 0);

    // Count rows
    while (result.next() === SQLDBC_Retcode.OK) {
      ntups++;
    }
    result.first();
    numTables[0] = ntups;

    const tblinfo: TableInfo[] = new Array(ntups);
    
    for (let i = 0; i < ntups; i++) {
      tblinfo[i] = createEmptyTableInfo();
    }

    // Process each row
    for (let i = 0; i < ntups; i++) {
      const rc = result.next();
      if (rc !== SQLDBC_Retcode.OK) {
        break;
      }

      const schema_name: SQLDBC_String = { toString: () => "" };
      const table_name: SQLDBC_String = { toString: () => "" };
      const table_type: SQLDBC_String = { toString: () => "" };
      const table_oid = { value: 0 };
      const relkind: SQLDBC_String = { toString: () => "" };
      const record_count = { value: 0 };
      const is_temporary: SQLDBC_String = { toString: () => "" };
      const is_logged: SQLDBC_String = { toString: () => "" };

      result.getObject(1, SQLDBC_HOSTTYPE.ASCII, schema_name, 0, true);
      result.getObject(2, SQLDBC_HOSTTYPE.ASCII, table_name, 0, true);
      result.getObject(3, SQLDBC_HOSTTYPE.ASCII, table_type, 0, true);
      result.getObject(4, SQLDBC_HOSTTYPE.UINT4, table_oid, 0, true);
      result.getObject(5, SQLDBC_HOSTTYPE.ASCII, relkind, 0, true);
      result.getObject(6, SQLDBC_HOSTTYPE.UINT4, record_count, 0, true);
      result.getObject(7, SQLDBC_HOSTTYPE.ASCII, is_temporary, 0, true);
      result.getObject(8, SQLDBC_HOSTTYPE.ASCII, is_logged, 0, true);

      const schemaName = schema_name.toString();
      const tableName = table_name.toString();
      const tableType = table_type.toString();

      tblinfo[i].schemaname = schemaName;
      tblinfo[i].tablename = tableName;
      tblinfo[i].tabletype = tableType.toLowerCase();

      tblinfo[i].dobj.objType = DO_TABLE;
      tblinfo[i].dobj.catId.tableoid = 0;
      tblinfo[i].dobj.catId.oid = table_oid.value;
      tblinfo[i].dobj.name = `${schemaName}.${tableName}`;
      tblinfo[i].dobj.schema = schemaName;
      AssignDumpId(tblinfo[i].dobj);

      // Map SAP HANA table types to standard types
      if (tableType === "VIEW") {
        tblinfo[i].relkind = 'v';
      } else {
        tblinfo[i].relkind = 'r';
      }

      tblinfo[i].rolname = "hana_user";
      tblinfo[i].ncheck = 0;
      tblinfo[i].hasindex = false;
      tblinfo[i].relpages = 0;
      tblinfo[i].reltuples = record_count.value;

      tblinfo[i].dobj.dump = DUMP_COMPONENT_DEFINITION | DUMP_COMPONENT_DATA;
      tblinfo[i].interesting = true;
      tblinfo[i].dummy_view = false;
      tblinfo[i].postponed_def = false;
      tblinfo[i].dobj.components |= DUMP_COMPONENT_DATA;

      if (is_temporary.toString() === "TRUE") {
        tblinfo[i].relpersistence = 't';
      } else {
        tblinfo[i].relpersistence = 'p';
      }

      tblinfo[i].columns = []; // Initialize empty columns array
    }

    result.close();
    if (result.stmt) {
      result.stmt.close();
      AH.connection!.connection.releaseStatement(result.stmt);
    }

    pg_log_info(`Retrieved ${ntups} tables from SAP HANA`);
    return tblinfo;
  } catch (error: any) {
    AH.lastError = `Table metadata retrieval failed: ${error.message}`;
    throw error;
  }
}

function getTableAttrs(AH: ArchiveHandle, tblinfo: TableInfo[], numTables: number): void {
  if (!AH.connection) {
    throw new Error("No SAP HANA connection available");
  }

  const q = new PQExpBuffer();

  for (let i = 0; i < numTables; i++) {
    const tbinfo = tblinfo[i];
    
    if (!tbinfo.interesting) {
      continue;
    }

    const schema_name = tbinfo.schemaname;
    const table_name = tbinfo.tablename;

    q.reset();
    q.printf(
      `SELECT COLUMN_NAME, DATA_TYPE_NAME, LENGTH, SCALE, ` +
      `IS_NULLABLE, DEFAULT_VALUE, COMMENT ` +
      `FROM SYS.TABLE_COLUMNS ` +
      `WHERE SCHEMA_NAME = '%s' ` +
      `AND TABLE_NAME = '%s' ` +
      `ORDER BY POSITION`,
      schema_name,
      table_name
    );

    try {
      const result = ExecuteSqlQuery(AH, q.getData(), 0);
      let numatts = 0;

      // Count columns
      while (result.next() === SQLDBC_Retcode.OK) {
        numatts++;
      }
      result.first();

      tbinfo.numatts = numatts;
      tbinfo.attnames = new Array(numatts);
      tbinfo.atttypnames = new Array(numatts);
      tbinfo.columns = [];

      // Process each column
      for (let j = 0; j < numatts; j++) {
        const rc = result.next();
        if (rc !== SQLDBC_Retcode.OK) {
          break;
        }

        const column_name: SQLDBC_String = { toString: () => "" };
        const data_type_name: SQLDBC_String = { toString: () => "" };
        const length = { value: 0 };
        const scale = { value: 0 };
        const is_nullable: SQLDBC_String = { toString: () => "" };
        const default_value: SQLDBC_String = { toString: () => "" };
        const comment: SQLDBC_String = { toString: () => "" };

        result.getObject(1, SQLDBC_HOSTTYPE.ASCII, column_name, 0, true);
        result.getObject(2, SQLDBC_HOSTTYPE.ASCII, data_type_name, 0, true);
        result.getObject(3, SQLDBC_HOSTTYPE.INT4, length, 0, true);
        result.getObject(4, SQLDBC_HOSTTYPE.INT4, scale, 0, true);
        result.getObject(5, SQLDBC_HOSTTYPE.ASCII, is_nullable, 0, true);
        result.getObject(6, SQLDBC_HOSTTYPE.ASCII, default_value, 0, true);
        result.getObject(7, SQLDBC_HOSTTYPE.ASCII, comment, 0, true);

        const colName = column_name.toString();
        const dataType = data_type_name.toString();
        const isNullable = is_nullable.toString() === 'TRUE';

        tbinfo.attnames[j] = colName;
        tbinfo.atttypnames[j] = hana_type_to_pg_type(dataType, length.value, scale.value);

        // Create column info object
        const columnInfo: ColumnInfo = {
          name: colName,
          type: dataType,
          length: length.value,
          precision: length.value, // SAP HANA uses length for precision in most cases
          scale: scale.value,
          nullable: isNullable,
          defaultValue: default_value.toString() || null,
          comment: comment.toString() || null,
          originalData: {
            dataTypeName: dataType,
            length: length.value,
            scale: scale.value,
            nullable: isNullable
          }
        };

        tbinfo.columns.push(columnInfo);
      }

      result.close();
      if (result.stmt) {
        result.stmt.close();
        AH.connection!.connection.releaseStatement(result.stmt);
      }

      pg_log_info(`Retrieved ${numatts} columns for table ${schema_name}.${table_name}`);
    } catch (error: any) {
      pg_log_error(`Failed to get columns for table ${schema_name}.${table_name}: ${error.message}`);
      // Continue with next table instead of failing completely
      continue;
    }
  }
}

// ===========================================================================
// Enhanced Helper Functions
// ===========================================================================

function hana_version_to_numeric(version_str: string): number {
  if (!version_str) return 0;

  const parts = version_str.split('.');
  if (parts.length >= 2) {
    const major = parseInt(parts[0]) || 0;
    const minor = parseInt(parts[1]) || 0;
    const patch = parts.length >= 3 ? parseInt(parts[2]) || 0 : 0;
    const revision = parts.length >= 4 ? parseInt(parts[3]) || 0 : 0;
    return major * 1000000 + minor * 10000 + patch * 100 + revision;
  }
  return 0;
}

function hana_type_to_pg_type(hana_type: string, length: number, scale: number): string {
  const buf = new PQExpBuffer();

  switch (hana_type.toUpperCase()) {
    case "VARCHAR":
    case "NVARCHAR":
      buf.append(`varchar(${length})`);
      break;
    case "CHAR":
    case "NCHAR":
      buf.append(`char(${length})`);
      break;
    case "DECIMAL":
    case "NUMERIC":
      if (scale > 0) {
        buf.append(`decimal(${length},${scale})`);
      } else {
        buf.append(`decimal(${length})`);
      }
      break;
    case "INTEGER":
      buf.appendStr("integer");
      break;
    case "SMALLINT":
      buf.appendStr("smallint");
      break;
    case "BIGINT":
      buf.appendStr("bigint");
      break;
    case "TINYINT":
      buf.appendStr("tinyint");
      break;
    case "DOUBLE":
      buf.appendStr("double precision");
      break;
    case "REAL":
    case "FLOAT":
      buf.appendStr("real");
      break;
    case "DATE":
      buf.appendStr("date");
      break;
    case "TIME":
      buf.appendStr("time");
      break;
    case "TIMESTAMP":
      buf.appendStr("timestamp");
      break;
    case "SECONDDATE":
      buf.appendStr("timestamp"); // Map to timestamp
      break;
    case "BLOB":
      buf.appendStr("bytea");
      break;
    case "CLOB":
    case "NCLOB":
      buf.appendStr("text");
      break;
    case "TEXT":
      buf.appendStr("text");
      break;
    case "BINARY":
      buf.append(`bytea`); // PostgreSQL equivalent
      break;
    case "VARBINARY":
      buf.append(`bytea`);
      break;
    case "BOOLEAN":
      buf.appendStr("boolean");
      break;
    case "SMALLDECIMAL":
      buf.appendStr("numeric(16,16)");
      break;
    case "ARRAY":
      buf.appendStr("text[]");
      break;
    default:
      buf.append(hana_type.toLowerCase());
  }

  return buf.getData();
}

function hana_get_single_value(AH: ArchiveHandle, query: string): string | null {
  if (!AH.connection) {
    return null;
  }

  try {
    const result = ExecuteSqlQuery(AH, query, 0);
    const rc = result.next();
    let retval: string | null = null;

    if (rc === SQLDBC_Retcode.OK) {
      const value: SQLDBC_String = { toString: () => "" };
      result.getObject(1, SQLDBC_HOSTTYPE.ASCII, value, 0, true);
      retval = value.toString();
    }

    result.close();
    if (result.stmt) {
      result.stmt.close();
      AH.connection.connection.releaseStatement(result.stmt);
    }

    return retval;
  } catch (error) {
    pg_log_error(`Single value query failed: ${error}`);
    return null;
  }
}

// ===========================================================================
// Enhanced Utility Functions
// ===========================================================================

function pg_log_error(message: string): void {
  console.error(`[SAP HANA ERROR] ${message}`);
}

function pg_log_error_detail(message: string): void {
  console.error(`[SAP HANA DETAIL] ${message}`);
}

function pg_log_info(message: string): void {
  console.log(`[SAP HANA INFO] ${message}`);
}

function pg_fatal(message: string): never {
  console.error(`[SAP HANA FATAL] ${message}`);
  throw new Error(message);
}

function pg_strdup(str: string): string {
  return str;
}

function exit_nicely(code: number): never {
  process.exit(code);
}

function simple_prompt(_prompt: string, _echo: boolean): string {
  // In a real implementation, this would use a proper prompt library
  // For now, return a mock password
  return "mock_sap_hana_password";
}

function set_archive_cancel_info(_AH: ArchiveHandle, _conn: HANA_Connection | null): void {
  // In a real implementation, this would set up signal handlers for cancellation
  // For now, it's a no-op
}

function AssignDumpId(dobj: DumpObject): void {
  // Mock implementation for assigning dump IDs
  dobj.components = DUMP_COMPONENT_DEFINITION | DUMP_COMPONENT_DATA;
}

function createEmptyTableInfo(): TableInfo {
  return {
    dobj: {
      objType: 0,
      catId: { tableoid: 0, oid: 0 },
      name: "",
      dump: 0,
      components: 0
    },
    relkind: "",
    rolname: "",
    ncheck: 0,
    hasindex: false,
    relpages: 0,
    reltuples: 0,
    numatts: 0,
    attnames: [],
    atttypnames: [],
    interesting: false,
    dummy_view: false,
    postponed_def: false,
    relpersistence: "",
    schemaname: "",
    tablename: "",
    tabletype: "",
    columns: []
  };
}

// ===========================================================================
// Enhanced SAP HANA Module Export
// ===========================================================================

// Main module export that matches the expected interface in DatabaseMetadataWizard
const SAPHANAModule = {
  // Connection functions
  ConnectDatabase,
  ConnectDatabaseAhx,
  DisconnectDatabase,
  GetConnection,
  
  // Metadata functions
  getTables,
  getTableAttrs,
  
  // Utility functions
  hana_version_to_numeric,
  hana_type_to_pg_type,
  hana_get_single_value,
  
  // Constants
  SQLDBC_Retcode,
  SQLDBC_HOSTTYPE,
  Trivalue,
  
  // Additional SAP HANA specific utilities
  executeQuery,
  ExecuteSqlStatement,
  ExecuteSqlQuery,
  
  // Mock implementation details (not typically exported)
  _mock: {
    SQLDBC_Environment,
    createEmptyTableInfo
  }
};

export default SAPHANAModule;

// Also export individual functions for testing
export {
  notice_processor,
  die_on_query_failure,
  constructConnStr,
  ExecuteSqlStatement,
  ExecuteSqlQuery,
  ExecuteSqlQueryForSingleRow,
  executeQuery,
  ConnectDatabase,
  ConnectDatabaseAhx,
  DisconnectDatabase,
  GetConnection,
  getTables,
  getTableAttrs,
  hana_version_to_numeric,
  hana_type_to_pg_type,
  hana_get_single_value,
  SQLDBC_Retcode,
  SQLDBC_HOSTTYPE,
  Trivalue
};