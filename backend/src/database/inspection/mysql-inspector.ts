/**
 * Enhanced MySQL Schema Inspection Module
 * Proper implementation with mysql2 driver and comprehensive metadata extraction
 */

import { window } from 'rxjs';

// Safe MySQL import that works with mysql2 v3.x
let mysql: any = null;

// Dynamic import to avoid build issues
const loadMySQL = async () => {
  if (typeof window === 'undefined') {
    // Node.js environment
    const mysqlModule = await import('mysql2/promise');
    return mysqlModule;
  }
  return null;
};

// Initialize MySQL on first use
let mysqlInitialized = false;
const initializeMySQL = async () => {
  if (!mysqlInitialized) {
    const mysqlModule = await loadMySQL();
    if (mysqlModule) {
      mysql = mysqlModule;
      mysqlInitialized = true;
    }
  }
  return mysql;
};

// ===========================================================================
// Enhanced Type Definitions
// ===========================================================================

interface ArchiveHandle {
    connection: MySQLConnection | null;
    remoteVersionStr: string | null;
    remoteVersion: number;
    archiveRemoteVersion: string | null;
    minRemoteVersion: number;
    maxRemoteVersion: number;
    isStandby: boolean;
    dopt: DumpOptions | null;
    savedPassword: string | null;
}

interface DumpOptions {
    [key: string]: any;
}

interface ConnParams {
    dbname: string | null;
    pghost: string | null;
    pgport: string | null;
    username: string | null;
    promptPassword: Trivalue;
    override_dbname: string | null;
}

interface TableInfo {
    dobj: DumpObject;
    schemaname: string;
    tablename: string;
    tabletype: string;
    relkind: string;
    rolname: string;
    ncheck: number;
    hasindex: boolean;
    relpages: number;
    reltuples: number;
    interesting: boolean;
    dummy_view: boolean;
    postponed_def: boolean;
    numatts: number;
    attnames: string[];
    atttypnames: string[];
    columns: ColumnMetadata[];
}

interface ColumnMetadata {
    name: string;
    type: string;
    length: number | null;
    precision: number | null;
    scale: number | null;
    nullable: boolean;
    default: string | null;
    isPrimaryKey: boolean;
    isUnique: boolean;
    isAutoIncrement: boolean;
    comment: string | null;
}

interface DumpObject {
    objType: string;
    catId: CatalogId;
    name: string;
    schema?: string;
    dump: number;
    components: number;
}

interface CatalogId {
    tableoid: number;
    oid: number;
}

interface MySQLConnection {
    connection: any;
    serverVersion: number;
    serverInfo: string;
    error: string | null;
    errno: number;
}

interface MySQLResult {
    data: any[];
    numRows: number;
    fetchRow(): any[] | null;
    free(): void;
}

type Trivalue = 'yes' | 'no' | 'unknown';

// ===========================================================================
// Constants
// ===========================================================================

const DUMP_COMPONENT_DEFINITION = 0x01;
const DUMP_COMPONENT_DATA = 0x02;
const RELKIND_SEQUENCE = 'S';
const RELKIND_TABLE = 'r';
const RELKIND_VIEW = 'v';
const TRI_YES: Trivalue = 'yes';
const TRI_NO: Trivalue = 'no';

// MySQL-specific constants
const MYSQL_DEFAULT_PORT = 3306;
const MYSQL_MIN_VERSION = 50000; // MySQL 5.0

// ===========================================================================
// Enhanced Utility Classes
// ===========================================================================

class PQExpBuffer {
    private buffer: string = '';

    append(str: string): void {
        this.buffer += str;
    }

    appendChar(ch: string): void {
        this.buffer += ch;
    }

    reset(): void {
        this.buffer = '';
    }

    get data(): string {
        return this.buffer;
    }

    destroy(): void {
        this.buffer = '';
    }

    length(): number {
        return this.buffer.length;
    }
}

// ===========================================================================
// Enhanced Error Handling and Logging
// ===========================================================================

class MySQLError extends Error {
    public code: string;
    public errno: number;
    public sqlState: string;

    constructor(message: string, code: string = 'MYSQL_ERROR', errno: number = 0, sqlState: string = '') {
        super(message);
        this.name = 'MySQLError';
        this.code = code;
        this.errno = errno;
        this.sqlState = sqlState;
    }
}

function pgLogError(message: string): void {
    console.error(`[MySQL ERROR] ${message}`);
}

function pgLogErrorDetail(message: string): void {
    console.error(`[MySQL DETAIL] ${message}`);
}

function pgLogInfo(message: string): void {
    console.log(`[MySQL INFO] ${message}`);
}

function getProgName(): string {
    return process.argv[1] || 'mysql-utility';
}

function simplePrompt(prompt: string, _echo: boolean = true): string {
    throw new MySQLError(`Interactive prompt required: ${prompt}`, 'PROMPT_REQUIRED');
}

// ===========================================================================
// Enhanced Database Connection Management
// ===========================================================================

async function connectDatabase(
    dbname: string | null,
    _connectionString: string | null,
    pghost: string | null,
    pgport: string | null,
    pguser: string | null,
    promptPassword: Trivalue,
    failOnError: boolean,
    _progname: string,
    connstr: string[] | null,
    serverVersion: number[] | null,
    password: string | null,
    overrideDbname: string | null
): Promise<MySQLConnection> {
    try {
        // Validate required parameters
        if (!dbname && !overrideDbname) {
            throw new MySQLError('Database name is required', 'MISSING_DATABASE');
        }

        const mysql = await initializeMySQL();
        if (!mysql) {
            throw new MySQLError('MySQL module not available in this environment', 'MYSQL_UNAVAILABLE');
        }

        const connectionConfig = {
            host: pghost || 'localhost',
            port: pgport ? parseInt(pgport, 10) : MYSQL_DEFAULT_PORT,
            user: pguser || undefined,
            password: password || undefined,
            database: overrideDbname || dbname || undefined,
            charset: 'utf8mb4',
            timezone: 'Z',
            connectTimeout: 10000,
            supportBigNumbers: true,
            bigNumberStrings: true
        };

        // Handle password prompting if needed
        if (promptPassword === TRI_YES && !connectionConfig.password) {
            try {
                connectionConfig.password = simplePrompt('Password: ', false);
            } catch (promptError) {
                if (failOnError) {
                    throw new MySQLError('Password required but not provided', 'PASSWORD_REQUIRED');
                }
                return null!;
            }
        }

        pgLogInfo(`Connecting to MySQL at ${connectionConfig.host}:${connectionConfig.port}/${connectionConfig.database}`);

        const connection = await mysql.createConnection(connectionConfig);
        
        // Test the connection
        await connection.execute('SELECT 1');
        
        // Get server version information
        const [versionRows] = await connection.execute('SELECT VERSION() as version');
        const versionString = versionRows[0]?.version || 'Unknown';
        const versionMatch = versionString.match(/(\d+)\.(\d+)\.(\d+)/);
        
        let versionNumber = 0;
        if (versionMatch) {
            const major = parseInt(versionMatch[1], 10);
            const minor = parseInt(versionMatch[2], 10);
            const patch = parseInt(versionMatch[3], 10);
            versionNumber = major * 10000 + minor * 100 + patch;
        }

        if (versionNumber < MYSQL_MIN_VERSION) {
            await connection.end();
            throw new MySQLError(
                `MySQL version ${versionString} is not supported. Minimum required: 5.0`,
                'VERSION_MISMATCH'
            );
        }

        const mysqlConnection: MySQLConnection = {
            connection,
            serverVersion: versionNumber,
            serverInfo: versionString,
            error: null,
            errno: 0
        };

        if (serverVersion) {
            serverVersion[0] = versionNumber;
        }

        if (connstr) {
            const buf = new PQExpBuffer();
            if (connectionConfig.host) buf.append(`host=${connectionConfig.host};`);
            if (connectionConfig.port) buf.append(`port=${connectionConfig.port};`);
            if (connectionConfig.user) buf.append(`user=${connectionConfig.user};`);
            if (connectionConfig.database) buf.append(`database=${connectionConfig.database};`);
            connstr[0] = buf.data;
            buf.destroy();
        }

        pgLogInfo(`Successfully connected to MySQL ${versionString}`);
        return mysqlConnection;

    } catch (error: any) {
        pgLogError(`Connection failed: ${error.message}`);
        
        if (failOnError) {
            if (error instanceof MySQLError) {
                throw error;
            }
            throw new MySQLError(
                `Failed to connect to MySQL: ${error.message}`,
                'CONNECTION_FAILED',
                error.errno,
                error.sqlState
            );
        }
        return null!;
    }
}

async function connectDatabaseAhx(
    AH: ArchiveHandle,
    cparams: ConnParams,
    isReconnect: boolean
): Promise<void> {
    if (AH.connection) {
        throw new MySQLError('Already connected to a database', 'ALREADY_CONNECTED');
    }

    const promptPassword = isReconnect ? TRI_NO : cparams.promptPassword;
    const password = AH.savedPassword;

    AH.connection = await connectDatabase(
        cparams.dbname,
        null,
        cparams.pghost,
        cparams.pgport,
        cparams.username,
        promptPassword,
        true,
        getProgName(),
        null,
        null,
        password,
        cparams.override_dbname
    );

    if (!AH.connection) {
        throw new MySQLError('Failed to establish database connection', 'CONNECTION_FAILED');
    }

    await checkDatabaseVersion(AH);
    setArchiveCancelInfo(AH, AH.connection);
}

function disconnectDatabase(AH: ArchiveHandle): void {
    if (!AH.connection) {
        return;
    }

    try {
        setArchiveCancelInfo(AH, null);
        AH.connection.connection.end();
        pgLogInfo('Disconnected from MySQL database');
    } catch (error: any) {
        pgLogError(`Error during disconnect: ${error.message}`);
    } finally {
        AH.connection = null;
    }
}

async function checkDatabaseVersion(AH: ArchiveHandle): Promise<void> {
    if (!AH.connection) {
        throw new MySQLError('No database connection', 'NO_CONNECTION');
    }

    const remoteVersionStr = AH.connection.serverInfo;
    const remoteVersion = AH.connection.serverVersion;

    if (remoteVersion === 0 || !remoteVersionStr) {
        throw new MySQLError('Could not determine MySQL server version', 'VERSION_UNKNOWN');
    }

    AH.remoteVersionStr = remoteVersionStr;
    AH.remoteVersion = remoteVersion;
    
    if (!AH.archiveRemoteVersion) {
        AH.archiveRemoteVersion = AH.remoteVersionStr;
    }

    if (remoteVersion < AH.minRemoteVersion || remoteVersion > AH.maxRemoteVersion) {
        const message = `Server version mismatch. Server: ${remoteVersionStr}; Required: ${AH.minRemoteVersion}-${AH.maxRemoteVersion}`;
        pgLogError(message);
        throw new MySQLError(message, 'VERSION_MISMATCH');
    }

    AH.isStandby = false; // MySQL doesn't have standby concept like PostgreSQL
}

// ===========================================================================
// Enhanced Schema Inspection Functions
// ===========================================================================

async function getTables(AH: ArchiveHandle): Promise<{ tables: TableInfo[]; numTables: number }> {
    if (!AH.connection) {
        throw new MySQLError('No database connection', 'NO_CONNECTION');
    }

    try {
        // Get current database
        const [dbRows] = await AH.connection.connection.execute('SELECT DATABASE() as dbname');
        const currentDatabase = dbRows[0]?.dbname;
        
        if (!currentDatabase) {
            throw new MySQLError('No database selected', 'NO_DATABASE');
        }

        // Query to get all tables and views
        const query = `
            SELECT 
                TABLE_SCHEMA as table_schema,
                TABLE_NAME as table_name,
                TABLE_TYPE as table_type,
                ENGINE as engine,
                TABLE_ROWS as table_rows,
                AVG_ROW_LENGTH as avg_row_length,
                DATA_LENGTH as data_length,
                INDEX_LENGTH as index_length,
                TABLE_COLLATION as table_collation,
                CREATE_TIME as create_time,
                UPDATE_TIME as update_time,
                TABLE_COMMENT as table_comment
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = ?
            ORDER BY TABLE_TYPE, TABLE_NAME
        `;

        const [rows] = await AH.connection.connection.execute(query, [currentDatabase]);
        
        const tables: TableInfo[] = rows.map((row: any, index: number) => {
            const tableType = row.table_type === 'BASE TABLE' ? RELKIND_TABLE : 
                            row.table_type === 'VIEW' ? RELKIND_VIEW : 't';
            
            return {
                dobj: {
                    objType: tableType === RELKIND_TABLE ? 'DO_TABLE' : 'DO_VIEW',
                    catId: {
                        tableoid: 0,
                        oid: index
                    },
                    name: row.table_name,
                    schema: row.table_schema,
                    dump: DUMP_COMPONENT_DEFINITION | (tableType === RELKIND_TABLE ? DUMP_COMPONENT_DATA : 0),
                    components: DUMP_COMPONENT_DEFINITION | (tableType === RELKIND_TABLE ? DUMP_COMPONENT_DATA : 0)
                },
                schemaname: row.table_schema,
                tablename: row.table_name,
                tabletype: row.table_type,
                relkind: tableType,
                rolname: row.table_schema,
                ncheck: 0,
                hasindex: row.index_length > 0,
                relpages: Math.ceil(row.data_length / 8192), // Estimate pages
                reltuples: row.table_rows,
                interesting: true,
                dummy_view: tableType === RELKIND_VIEW,
                postponed_def: false,
                numatts: 0, // Will be populated by getTableAttrs
                attnames: [],
                atttypnames: [],
                columns: []
            };
        });

        // Assign dump IDs
        tables.forEach(table => assignDumpId(table.dobj));

        return { 
            tables, 
            numTables: tables.length 
        };

    } catch (error: any) {
        pgLogError(`Failed to retrieve tables: ${error.message}`);
        throw new MySQLError(
            `Table metadata retrieval failed: ${error.message}`,
            'METADATA_RETRIEVAL_FAILED'
        );
    }
}

async function getTableAttrs(AH: ArchiveHandle, tblinfo: TableInfo[], numTables: number): Promise<void> {
    if (!AH.connection) {
        throw new MySQLError('No database connection', 'NO_CONNECTION');
    }

    try {
        for (let i = 0; i < numTables; i++) {
            const tbinfo = tblinfo[i];

            if (tbinfo.relkind === RELKIND_SEQUENCE || !tbinfo.interesting) {
                continue;
            }

            // Query to get column information
            const columnQuery = `
                SELECT 
                    COLUMN_NAME as column_name,
                    DATA_TYPE as data_type,
                    IS_NULLABLE as is_nullable,
                    COLUMN_DEFAULT as column_default,
                    CHARACTER_MAXIMUM_LENGTH as character_maximum_length,
                    NUMERIC_PRECISION as numeric_precision,
                    NUMERIC_SCALE as numeric_scale,
                    EXTRA as extra,
                    COLUMN_KEY as column_key,
                    COLUMN_COMMENT as column_comment,
                    COLLATION_NAME as collation_name
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
            `;

            const [columnRows] = await AH.connection.connection.execute(
                columnQuery, 
                [tbinfo.schemaname, tbinfo.tablename]
            );

            // Query to get primary key information
            const pkQuery = `
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'
                ORDER BY ORDINAL_POSITION
            `;

            const [pkRows] = await AH.connection.connection.execute(
                pkQuery,
                [tbinfo.schemaname, tbinfo.tablename]
            );

            const primaryKeyColumns = new Set(pkRows.map((row: any) => row.COLUMN_NAME));

            // Query to get index information for uniqueness
            const indexQuery = `
                SELECT 
                    COLUMN_NAME,
                    NON_UNIQUE
                FROM INFORMATION_SCHEMA.STATISTICS 
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY INDEX_NAME, SEQ_IN_INDEX
            `;

            const [indexRows] = await AH.connection.connection.execute(
                indexQuery,
                [tbinfo.schemaname, tbinfo.tablename]
            );

            const uniqueColumns = new Set(
                indexRows
                    .filter((row: any) => row.NON_UNIQUE === 0)
                    .map((row: any) => row.COLUMN_NAME)
            );

            // Transform column data
            tbinfo.columns = columnRows.map((col: any) => {
                const isPrimaryKey = primaryKeyColumns.has(col.column_name);
                const isUnique = uniqueColumns.has(col.column_name) || isPrimaryKey;
                const isAutoIncrement = col.extra.includes('auto_increment');

                return {
                    name: col.column_name,
                    type: col.data_type,
                    length: col.character_maximum_length,
                    precision: col.numeric_precision,
                    scale: col.numeric_scale,
                    nullable: col.is_nullable === 'YES',
                    default: col.column_default,
                    isPrimaryKey,
                    isUnique,
                    isAutoIncrement,
                    comment: col.column_comment
                };
            });

            // Also populate the legacy arrays for compatibility
            tbinfo.numatts = tbinfo.columns.length;
            tbinfo.attnames = tbinfo.columns.map(col => col.name);
            tbinfo.atttypnames = tbinfo.columns.map(col => 
                col.length ? `${col.type}(${col.length})` : 
                col.precision ? `${col.type}(${col.precision}${col.scale ? `,${col.scale}` : ''})` :
                col.type
            );
        }

        pgLogInfo(`Successfully retrieved column metadata for ${numTables} tables`);

    } catch (error: any) {
        pgLogError(`Failed to retrieve column metadata: ${error.message}`);
        throw new MySQLError(
            `Column metadata retrieval failed: ${error.message}`,
            'COLUMN_METADATA_FAILED'
        );
    }
}

// ===========================================================================
// SQL Execution Functions (Enhanced)
// ===========================================================================

async function executeSqlStatement(AH: ArchiveHandle, query: string): Promise<void> {
    if (!AH.connection) {
        throw new MySQLError('No database connection', 'NO_CONNECTION');
    }
    
    try {
        await AH.connection.connection.execute(query);
    } catch (error: any) {
        throw new MySQLError(
            `Query execution failed: ${error.message}`,
            'QUERY_FAILED',
            error.errno,
            error.sqlState
        );
    }
}

async function executeSqlQuery(AH: ArchiveHandle, query: string, _status: number = 0): Promise<MySQLResult> {
    if (!AH.connection) {
        throw new MySQLError('No database connection', 'NO_CONNECTION');
    }

    try {
        const [rows] = await AH.connection.connection.execute(query);
        
        let currentRow = 0;
        const data = Array.isArray(rows) ? rows : [rows];
        
        const result: MySQLResult = {
            data,
            numRows: data.length,
            fetchRow(): any[] | null {
                if (currentRow < data.length) {
                    const row = data[currentRow];
                    currentRow++;
                    return Array.isArray(row) ? row : Object.values(row);
                }
                return null;
            },
            free(): void {
                // No explicit freeing needed with mysql2
            }
        };

        return result;

    } catch (error: any) {
        throw new MySQLError(
            `Query execution failed: ${error.message}\nQuery: ${query}`,
            'QUERY_FAILED',
            error.errno,
            error.sqlState
        );
    }
}

async function executeSqlQueryForSingleRow(AH: ArchiveHandle, query: string): Promise<MySQLResult> {
    const res = await executeSqlQuery(AH, query, 0);
    
    if (res.numRows !== 1) {
        const message = res.numRows === 0 
            ? `Query returned no rows instead of one: ${query}`
            : `Query returned ${res.numRows} rows instead of one: ${query}`;
        throw new MySQLError(message, 'UNEXPECTED_ROW_COUNT');
    }

    return res;
}

function dieOnQueryFailure(AH: ArchiveHandle, query: string): never {
    const errorMsg = AH.connection?.error || 'Unknown query error';
    pgLogError(`Query failed: ${errorMsg}`);
    pgLogErrorDetail(`Failed query: ${query}`);
    throw new MySQLError(`Query failed: ${errorMsg}`, 'QUERY_FAILED');
}

// ===========================================================================
// Utility Functions
// ===========================================================================

function constructConnStr(keywords: string[], values: string[]): string {
    const buf = new PQExpBuffer();
    let firstKeyword = true;

    for (let i = 0; i < keywords.length && keywords[i] !== null; i++) {
        if (keywords[i] === 'dbname' || 
            keywords[i] === 'password' || 
            keywords[i] === 'fallback_application_name') {
            continue;
        }

        if (!firstKeyword) {
            buf.appendChar(';');
        }
        firstKeyword = false;
        buf.append(`${keywords[i]}=${values[i]}`);
    }

    const connstr = buf.data;
    buf.destroy();
    return connstr;
}

function getConnection(AH: ArchiveHandle): MySQLConnection | null {
    return AH.connection;
}

function noticeProcessor(message: string): void {
    pgLogInfo(`MySQL Notice: ${message}`);
}

// ===========================================================================
// Stub Functions (To be implemented based on application needs)
// ===========================================================================

function assignDumpId(_dobj: DumpObject): void {
    // Implement based on your application's dump ID assignment logic
}

function setArchiveCancelInfo(_AH: ArchiveHandle, _conn: MySQLConnection | null): void {
    // Implement cancellation handling if needed
}

// ===========================================================================
// Export all functions and types
// ===========================================================================

// Named exports for individual imports
export {
    noticeProcessor,
    dieOnQueryFailure,
    constructConnStr,
    executeSqlStatement,
    executeSqlQuery,
    executeSqlQueryForSingleRow,
    checkDatabaseVersion,
    connectDatabase,
    connectDatabaseAhx,
    disconnectDatabase,
    getConnection,
    getTables,
    getTableAttrs,
    PQExpBuffer,
    MySQLError,
    TRI_NO,
    TRI_YES,
    DUMP_COMPONENT_DEFINITION,
    DUMP_COMPONENT_DATA,
    RELKIND_SEQUENCE,
    RELKIND_TABLE,
    RELKIND_VIEW
};

export type {
    ArchiveHandle,
    TableInfo,
    ColumnMetadata,
    ConnParams,
    MySQLConnection,
    Trivalue,
    DumpObject,
    MySQLResult
};

// Default export for backward compatibility
const MySQLModule = {
    // Core functions
    noticeProcessor,
    dieOnQueryFailure,
    constructConnStr,
    executeSqlStatement,
    executeSqlQuery,
    executeSqlQueryForSingleRow,
    checkDatabaseVersion,
    connectDatabase,
    connectDatabaseAhx,
    disconnectDatabase,
    getConnection,
    getTables,
    getTableAttrs,
    PQExpBuffer,
    MySQLError,
    
    // Constants
    TRI_NO,
    TRI_YES,
    DUMP_COMPONENT_DEFINITION,
    DUMP_COMPONENT_DATA,
    RELKIND_SEQUENCE,
    RELKIND_TABLE,
    RELKIND_VIEW,
    
    // For DatabaseMetadataWizard compatibility
    Trivalue: {
        TRI_NO,
        TRI_YES,
        TRI_UNKNOWN: 'unknown' as Trivalue
    }
};

export default MySQLModule;