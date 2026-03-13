/**
 * Netezza ODBC Utility Functions - ENHANCED VERSION
 * TypeScript translation maintaining functional equivalence with original C code
 * With comprehensive schema inspection corrections
 */

// Type definitions for ODBC constants and handles
type SQLHANDLE = number;
type SQLHENV = SQLHANDLE;
type SQLHDBC = SQLHANDLE;
type SQLHSTMT = SQLHANDLE;
type SQLRETURN = number;
type SQLSMALLINT = number;
type SQLINTEGER = number;
type SQLULEN = number;
type SQLLEN = number;

// ODBC constants
const SQL_NTS = -3;
const SQL_HANDLE_ENV = 1;
const SQL_HANDLE_DBC = 2;
const SQL_HANDLE_STMT = 3;
const SQL_MAX_MESSAGE_LENGTH = 1024;
const SQL_OV_ODBC3 = 3;
const SQL_ATTR_ODBC_VERSION = 200;
const SQL_ATTR_AUTOCOMMIT = 102;
const SQL_AUTOCOMMIT_OFF = 0;
const SQL_DRIVER_NOPROMPT = 0;
const SQL_C_CHAR = 1;
const SQL_NULL_DATA = -1;
const SQL_NO_DATA = 100;
const SQL_SUCCESS = 0;
const SQL_SUCCEEDED = (ret: SQLRETURN): boolean => ret === SQL_SUCCESS || ret === 1;

// Application constants
enum DumpComponent {
    NONE = 0,
    DEFINITION = 1,
    DATA = 2
}

enum Trivalue {
    NO = 0,
    YES = 1,
    AUTO = 2
}

// Interface definitions
interface CatalogId {
    oid: number;
}

interface DumpableObject {
    objType: number;
    catId: CatalogId;
    name: string;
    dump: DumpComponent;
    namespace?: string;
}

// ENHANCED: Added schemaname to TableInfo
interface TableInfo {
    dobj: DumpableObject;
    relkind: string;
    rolname: string;
    reltablespace: string;
    numatts: number;
    attnames: string[];
    atttypnames: string[];
    notnull_constrs: (string | null)[];
    createdate?: string;
    tablevol?: string;
    schemaname: string; // Added schema name
    row_count?: number; // Added row count
}

// ENHANCED: Added schema parameter to ConnParams
interface ConnParams {
    dbname: string;
    pghost: string;
    pgport: string;
    username: string;
    promptPassword: Trivalue;
    override_dbname?: string | null;
    schema?: string; // Added schema parameter
}

interface NetezzaResult {
    stmt: SQLHSTMT;
    num_cols: number;
    current_row: number;
    row_count: number;
    col_types: number[];
    col_sizes: SQLULEN[];
}

class ArchiveHandle {
    env: SQLHENV = 0;
    dbc: SQLHDBC = 0;
    stmt: SQLHSTMT = 0;
    remoteVersionStr: string = '';
    remoteVersion: number = 0;
    archiveRemoteVersion: string = '';
    minRemoteVersion: number = 0;
    maxRemoteVersion: number = 0;
    isStandby: boolean = false;
    savedPassword: string = '';

    constructor() {}
}

// Global state
let progname: string = 'netezza-utility';

/**
 * Low-level utility functions
 */

function notice_processor(message: string): void {
    console.log(`NETEZZA INFO: ${message}`);
}

function die_on_query_failure(AH: ArchiveHandle, query: string): never {
    const sqlstate: string = '';
    const message: string = '';
    let native: SQLINTEGER = 0;
    let len: SQLSMALLINT = 0;
    let ret: SQLRETURN;

    console.error('NETEZZA ERROR: query failed');

    // Get ODBC errors
    ret = SQLGetDiagRec(SQL_HANDLE_STMT, AH.stmt, 1, sqlstate, native, message, SQL_MAX_MESSAGE_LENGTH, len);
    while (SQL_SUCCEEDED(ret)) {
        console.error(`NETEZZA ERROR: ${message} (SQLSTATE: ${sqlstate})`);
        ret = SQLGetDiagRec(SQL_HANDLE_STMT, AH.stmt, 2, sqlstate, native, message, SQL_MAX_MESSAGE_LENGTH, len);
    }

    console.error(`NETEZZA ERROR DETAIL: Query was: ${query}`);
    process.exit(1);
}

/**
 * String buffer utility (replacement for PQExpBuffer)
 */
class StringBuffer {
    private data: string[] = [];

    append(str: string): void {
        this.data.push(str);
    }

    appendChar(ch: string): void {
        this.data.push(ch);
    }

    reset(): void {
        this.data = [];
    }

    toString(): string {
        return this.data.join('');
    }

    get length(): number {
        return this.data.join('').length;
    }
}

function createStringBuffer(): StringBuffer {
    return new StringBuffer();
}

function destroyStringBuffer(_buf: StringBuffer): void {
    // In TypeScript, we just let garbage collection handle it
}

function constructConnStr(keywords: string[], values: string[]): string {
    const buf = createStringBuffer();
    let firstkeyword = true;

    for (let i = 0; i < keywords.length && keywords[i] !== null; i++) {
        if (keywords[i] === 'dbname') {
            if (!firstkeyword) buf.appendChar(';');
            firstkeyword = false;
            buf.append(`DATABASE=${values[i]}`);
        } else if (keywords[i] === 'host') {
            if (!firstkeyword) buf.appendChar(';');
            firstkeyword = false;
            buf.append(`SERVER=${values[i]}`);
        } else if (keywords[i] === 'port') {
            if (!firstkeyword) buf.appendChar(';');
            firstkeyword = false;
            buf.append(`PORT=${values[i]}`);
        } else if (keywords[i] === 'user') {
            if (!firstkeyword) buf.appendChar(';');
            firstkeyword = false;
            buf.append(`UID=${values[i]}`);
        } else if (keywords[i] === 'password') {
            if (!firstkeyword) buf.appendChar(';');
            firstkeyword = false;
            buf.append(`PWD=${values[i]}`);
        }
    }

    const connstr = buf.toString();
    destroyStringBuffer(buf);
    return connstr;
}

/**
 * SQL Execution Functions
 */

function ExecuteSqlStatement(AH: ArchiveHandle, query: string): void {
    const ret = SQLExecDirect(AH.stmt, query, SQL_NTS);
    if (!SQL_SUCCEEDED(ret)) {
        die_on_query_failure(AH, query);
    }
}

function ExecuteSqlQuery(AH: ArchiveHandle, query: string, _exec_status: number): NetezzaResult {
    const res: NetezzaResult = {
        stmt: 0,
        num_cols: 0,
        current_row: 0,
        row_count: 0,
        col_types: [],
        col_sizes: []
    };

    // Allocate a new statement handle for this result
    let ret = SQLAllocHandle(SQL_HANDLE_STMT, AH.dbc, res.stmt);
    if (!SQL_SUCCEEDED(ret)) {
        die_on_query_failure(AH, query);
    }

    // Execute the query
    ret = SQLExecDirect(res.stmt, query, SQL_NTS);
    if (!SQL_SUCCEEDED(ret)) {
        SQLFreeHandle(SQL_HANDLE_STMT, res.stmt);
        die_on_query_failure(AH, query);
    }

    // Get number of columns
    let num_cols: SQLSMALLINT = 0;
    ret = SQLNumResultCols(res.stmt, num_cols);
    if (!SQL_SUCCEEDED(ret)) {
        SQLFreeHandle(SQL_HANDLE_STMT, res.stmt);
        die_on_query_failure(AH, query);
    }

    res.num_cols = num_cols;

    // Get column information
    if (num_cols > 0) {
        res.col_types = new Array(num_cols);
        res.col_sizes = new Array(num_cols);

        for (let i = 0; i < num_cols; i++) {
            const col_name: string = '';
            let name_len: SQLSMALLINT = 0;
            let data_type: SQLSMALLINT = 0;
            let decimal_digits: SQLSMALLINT = 0;
            let nullable: SQLSMALLINT = 0;
            let col_size: SQLULEN = 0;

            ret = SQLDescribeCol(res.stmt, i + 1, col_name, 256, name_len, data_type, col_size, decimal_digits, nullable);
            if (SQL_SUCCEEDED(ret)) {
                res.col_types[i] = data_type;
                res.col_sizes[i] = col_size;
            }
        }
    }

    // Count rows by fetching
    while (SQL_SUCCEEDED(SQLFetch(res.stmt))) {
        res.row_count++;
    }

    // Reset cursor to beginning
    SQLCloseCursor(res.stmt);
    SQLExecDirect(res.stmt, query, SQL_NTS);

    return res;
}

function ExecuteSqlQueryForSingleRow(AH: ArchiveHandle, query: string): NetezzaResult {
    const res = ExecuteSqlQuery(AH, query, 0);
    const row_count = res.row_count;

    if (row_count !== 1) {
        console.error(`NETEZZA ERROR: query returned ${row_count} rows instead of one: ${query}`);
        SQLFreeHandle(SQL_HANDLE_STMT, res.stmt);
        process.exit(1);
    }

    return res;
}

/**
 * Database Connection Management
 */

function _check_database_version(AH: ArchiveHandle): void {
    const res = ExecuteSqlQueryForSingleRow(AH, "SELECT CURRENT_VERSION");
    const version_str = getNetezzaValue(res, 0);

    AH.remoteVersionStr = version_str;
    AH.remoteVersion = parseNetezzaVersion(version_str);

    if (!AH.archiveRemoteVersion) {
        AH.archiveRemoteVersion = AH.remoteVersionStr;
    }

    // Version compatibility check
    if (AH.remoteVersion < AH.minRemoteVersion || AH.remoteVersion > AH.maxRemoteVersion) {
        console.error('NETEZZA ERROR: server version mismatch');
        console.error(`NETEZZA ERROR DETAIL: server version: ${version_str}`);
        process.exit(1);
    }

    AH.isStandby = false;
    freeNetezzaResult(res);
}

function ConnectDatabase(
    dbname: string,
    connection_string: string | null,
    pghost: string | null,
    pgport: string | null,
    pguser: string | null,
    _prompt_password: Trivalue,
    fail_on_error: boolean,
    _progname: string,
    connstr: (string | null)[] | null,
    server_version: (number | null)[] | null,
    password: string | null,
    override_dbname: string | null
): number {
    let env: SQLHENV = 0;
    let dbc: SQLHDBC = 0;
    let ret: SQLRETURN;
    const actual_dbname = override_dbname || dbname;

    // Allocate environment handle
    ret = SQLAllocHandle(SQL_HANDLE_ENV, 0, env);
    if (!SQL_SUCCEEDED(ret)) {
        if (fail_on_error) console.error('NETEZZA ERROR: could not allocate environment handle');
        return 0;
    }

    // Set ODBC version
    ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, SQL_OV_ODBC3, 0);
    if (!SQL_SUCCEEDED(ret)) {
        if (fail_on_error) console.error('NETEZZA ERROR: could not set ODBC version');
        SQLFreeHandle(SQL_HANDLE_ENV, env);
        return 0;
    }

    // Allocate connection handle
    ret = SQLAllocHandle(SQL_HANDLE_DBC, env, dbc);
    if (!SQL_SUCCEEDED(ret)) {
        if (fail_on_error) console.error('NETEZZA ERROR: could not allocate connection handle');
        SQLFreeHandle(SQL_HANDLE_ENV, env);
        return 0;
    }

    // Build connection string
    let connectionString: string;
    if (connection_string) {
        connectionString = connection_string;
    } else {
        if (pghost && pgport && pguser && password) {
            connectionString = `DRIVER={NetezzaSQL};SERVER=${pghost};PORT=${pgport};DATABASE=${actual_dbname};UID=${pguser};PWD=${password}`;
        } else if (pghost && pguser && password) {
            connectionString = `DRIVER={NetezzaSQL};SERVER=${pghost};DATABASE=${actual_dbname};UID=${pguser};PWD=${password}`;
        } else {
            connectionString = `DRIVER={NetezzaSQL};DATABASE=${actual_dbname}`;
        }
    }

    // Connect to database
    ret = SQLDriverConnect(dbc, null, connectionString, SQL_NTS, null, 0, null, SQL_DRIVER_NOPROMPT);
    if (!SQL_SUCCEEDED(ret)) {
        if (fail_on_error) {
            console.error(`NETEZZA ERROR: could not connect to database '${actual_dbname}'`);
            // Print ODBC error
            const state: string = '';
            const message: string = '';
            let native: SQLINTEGER = 0;
            let len: SQLSMALLINT = 0;
            SQLGetDiagRec(SQL_HANDLE_DBC, dbc, 1, state, native, message, 1024, len);
            console.error(`NETEZZA ERROR: ${message}`);
        }
        SQLFreeHandle(SQL_HANDLE_DBC, dbc);
        SQLFreeHandle(SQL_HANDLE_ENV, env);
        return 0;
    }

    // Set auto-commit off for better transaction control
    SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    // Create ArchiveHandle structure
    const ah = new ArchiveHandle();
    ah.env = env;
    ah.dbc = dbc;

    // Allocate a statement handle for general use
    ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, ah.stmt);
    if (!SQL_SUCCEEDED(ret)) {
        if (fail_on_error) console.error('NETEZZA ERROR: could not allocate statement handle');
        SQLDisconnect(dbc);
        SQLFreeHandle(SQL_HANDLE_DBC, dbc);
        SQLFreeHandle(SQL_HANDLE_ENV, env);
        return 0;
    }

    // Get server version
    if (server_version && server_version.length > 0) {
        const res = ExecuteSqlQueryForSingleRow(ah, "SELECT CURRENT_VERSION");
        const version_str = getNetezzaValue(res, 0);
        server_version[0] = parseNetezzaVersion(version_str);
        freeNetezzaResult(res);
    }

    // Remember connection string
    if (connstr && connstr.length > 0) {
        connstr[0] = connectionString;
    }

    // Return connection as integer handle (simplified - in reality we'd return the object)
    return ah as unknown as number;
}

// ENHANCED: ConnectDatabaseAhx with schema support and better error handling
function ConnectDatabaseAhx(AH: ArchiveHandle, cparams: ConnParams, isReconnect: boolean): void {
    if (AH.dbc) {
        console.error('NETEZZA ERROR: already connected to a database');
        return;
    }

    // Never prompt for a password during a reconnection
    const prompt_password = isReconnect ? Trivalue.NO : cparams.promptPassword;
    let password = AH.savedPassword;

    if (prompt_password === Trivalue.YES && !password) {
        // In a real implementation, we'd use a proper prompt library
        password = 'prompted_password'; // simplified
    }

    try {
        // Connect and populate AH structure
        const conn_handle = ConnectDatabase(
            cparams.dbname,
            null,
            cparams.pghost,
            cparams.pgport,
            cparams.username,
            prompt_password,
            true,
            progname,
            null,
            null,
            password,
            cparams.override_dbname ?? null
        );

        if (!conn_handle) {
            throw new Error('Failed to establish database connection');
        }

        // The connection handle is actually an ArchiveHandle pointer
        const new_ah = conn_handle as unknown as ArchiveHandle;

        // Copy the connection info to the provided AH
        AH.env = new_ah.env;
        AH.dbc = new_ah.dbc;
        AH.stmt = new_ah.stmt;

        // ENHANCED: Set schema context if specified
        if (cparams.schema && cparams.schema !== 'public') {
            try {
                const setSchemaQuery = `SET SCHEMA '${cparams.schema}'`;
                ExecuteSqlStatement(AH, setSchemaQuery);
                console.log(`NETEZZA INFO: Schema set to '${cparams.schema}'`);
            } catch (error) {
                console.warn(`NETEZZA WARNING: Could not set schema to '${cparams.schema}': ${error}`);
            }
        }

        if (password && password !== AH.savedPassword) {
            // In a real implementation, we might need to clean up
        }

        // Remember password
        if (password) {
            AH.savedPassword = password;
        }

        // Check for version mismatch
        _check_database_version(AH);
    } catch (error: any) {
        console.error('NETEZZA ERROR: Connection failed:', error.message);
        throw error;
    }
}

function DisconnectDatabase(AH: ArchiveHandle): void {
    if (!AH.dbc) return;

    try {
        // Commit any pending transaction
        SQLEndTran(SQL_HANDLE_DBC, AH.dbc, SQL_SUCCESS);

        // Free statement handle
        if (AH.stmt) {
            SQLFreeHandle(SQL_HANDLE_STMT, AH.stmt);
            AH.stmt = 0;
        }

        // Disconnect and free connection handle
        SQLDisconnect(AH.dbc);
        SQLFreeHandle(SQL_HANDLE_DBC, AH.dbc);
        AH.dbc = 0;

        // Free environment handle
        if (AH.env) {
            SQLFreeHandle(SQL_HANDLE_ENV, AH.env);
            AH.env = 0;
        }
    } catch (error) {
        console.error('NETEZZA ERROR: Error during disconnection:', error);
    }
}

function GetConnection(AH: ArchiveHandle): SQLHDBC {
    return AH.dbc;
}

/**
 * ENHANCED Table Metadata Functions with comprehensive error handling
 */

// ENHANCED: getTables function with schema support and proper query formulation
function getTables(AH: ArchiveHandle, schema?: string): TableInfo[] {
    try {
        const query = createStringBuffer();
        
        // Build schema filter - CORRECTED: Use proper Netezza system views
        let schemaFilter = "s.schemaname NOT IN ('ADMIN', 'INFORMATION_SCHEMA', 'SYSTEM', 'NETEZZA')";
        if (schema && schema !== 'public') {
            schemaFilter = `s.schemaname = '${schema}'`;
        }

        // CORRECTED: Proper Netezza system catalog query
        query.append(`SELECT 
            t.objid,
            t.tablename,
            s.schemaname,
            t.owner,
            t.tablespace,
            CASE 
                WHEN t.tabletype = 'TABLE' THEN 'r'
                WHEN t.tabletype = 'VIEW' THEN 'v' 
                WHEN t.tabletype = 'EXTERNAL TABLE' THEN 'f'
                ELSE '?'
            END as relkind,
            t.createdate,
            t.tablevol,
            COALESCE(t.rowcount, 0) as row_count
            FROM _V_TABLE t
            INNER JOIN _V_SCHEMA s ON t.schemaid = s.schemaid
            WHERE t.databasename = CURRENT_DATABASE()
            AND ${schemaFilter}
            AND t.tabletype IN ('TABLE', 'VIEW', 'EXTERNAL TABLE')
            ORDER BY s.schemaname, t.tablename`);

        const res = ExecuteSqlQuery(AH, query.toString(), 0);
        const ntups = res.row_count;

        const tblinfo: TableInfo[] = [];

        for (let i = 0; i < ntups; i++) {
            if (fetchNetezzaRow(res)) {
                const objid = getNetezzaValue(res, 0);
                const tablename = getNetezzaValue(res, 1);
                const schemaname = getNetezzaValue(res, 2);
                const owner = getNetezzaValue(res, 3);
                const tablespace = getNetezzaValue(res, 4);
                const relkind = getNetezzaValue(res, 5);
                const createdate = getNetezzaValue(res, 6);
                const tablevol = getNetezzaValue(res, 7);
                const rowcount = parseInt(getNetezzaValue(res, 8) || '0');

                tblinfo.push({
                    dobj: {
                        objType: 0, // DO_TABLE
                        catId: { oid: parseInt(objid || '0') },
                        name: tablename || '',
                        dump: (relkind === 'r' || relkind === 'f') ? 
                            (DumpComponent.DEFINITION | DumpComponent.DATA) : DumpComponent.DEFINITION,
                        namespace: schemaname // Store schema in namespace
                    },
                    relkind: relkind ? relkind[0] : '?',
                    rolname: owner || '',
                    reltablespace: tablespace || '',
                    schemaname: schemaname || 'public', // Store schema name
                    numatts: 0,
                    attnames: [],
                    atttypnames: [],
                    notnull_constrs: [],
                    createdate: createdate,
                    tablevol: tablevol,
                    row_count: rowcount
                });
            }
        }

        freeNetezzaResult(res);
        destroyStringBuffer(query);
        return tblinfo;
    } catch (error: any) {
        console.error('NETEZZA ERROR: Failed to retrieve tables:', error);
        throw new Error(`Table metadata retrieval failed: ${error.message}`);
    }
}

// ENHANCED: getTableAttrs function with comprehensive column metadata
function getTableAttrs(AH: ArchiveHandle, tblinfo: TableInfo[]): void {
    try {
        const q = createStringBuffer();

        for (const tbinfo of tblinfo) {
            // Only process tables and external tables that need column info
            if (tbinfo.relkind !== 'r' && tbinfo.relkind !== 'f') continue;

            // CORRECTED: Proper Netezza column metadata query
            q.reset();
            q.append(`SELECT 
                c.attname,
                c.format_type,
                c.attnotnull,
                c.atthasdef,
                c.adsrc as column_default,
                c.attnum,
                c.attlen,
                c.atttypmod
                FROM _V_RELATION_COLUMN c
                WHERE c.schemaname = '${tbinfo.schemaname}'
                AND c.name = '${tbinfo.dobj.name}'
                ORDER BY c.attnum`);

            const res = ExecuteSqlQuery(AH, q.toString(), 0);
            const ncols = res.row_count;
            tbinfo.numatts = ncols;

            if (ncols > 0) {
                tbinfo.attnames = [];
                tbinfo.atttypnames = [];
                tbinfo.notnull_constrs = [];

                for (let j = 0; j < ncols; j++) {
                    if (fetchNetezzaRow(res)) {
                        const colname = getNetezzaValue(res, 0);
                        const format_type = getNetezzaValue(res, 1);
                        const attnotnull = getNetezzaValue(res, 2);

                        tbinfo.attnames.push(colname || '');
                        tbinfo.atttypnames.push(format_type || '');
                        
                        // Set NOT NULL constraint
                        if (attnotnull && attnotnull.toLowerCase() === 't') {
                            tbinfo.notnull_constrs.push("NOT NULL");
                        } else {
                            tbinfo.notnull_constrs.push(null);
                        }
                    }
                }
            }

            freeNetezzaResult(res);
        }
        destroyStringBuffer(q);
    } catch (error: any) {
        console.error('NETEZZA ERROR: Failed to retrieve column metadata:', error);
        throw new Error(`Column metadata retrieval failed: ${error.message}`);
    }
}

// NEW FUNCTION: Get table count for validation
function getTableCount(AH: ArchiveHandle, schema?: string): number {
    try {
        let schemaFilter = "s.schemaname NOT IN ('ADMIN', 'INFORMATION_SCHEMA', 'SYSTEM', 'NETEZZA')";
        if (schema && schema !== 'public') {
            schemaFilter = `s.schemaname = '${schema}'`;
        }

        const query = `SELECT COUNT(*) 
                      FROM _V_TABLE t
                      INNER JOIN _V_SCHEMA s ON t.schemaid = s.schemaid
                      WHERE t.databasename = CURRENT_DATABASE()
                      AND ${schemaFilter}
                      AND t.tabletype IN ('TABLE', 'VIEW', 'EXTERNAL TABLE')`;

        const res = ExecuteSqlQueryForSingleRow(AH, query);
        const count = parseInt(getNetezzaValue(res, 0) || '0');
        freeNetezzaResult(res);
        return count;
    } catch (error: any) {
        console.error('NETEZZA ERROR: Failed to get table count:', error);
        return 0;
    }
}

// NEW FUNCTION: Validate schema existence
function validateSchema(AH: ArchiveHandle, schema: string): boolean {
    try {
        const query = `SELECT COUNT(*) FROM _V_SCHEMA WHERE schemaname = '${schema}'`;
        const res = ExecuteSqlQueryForSingleRow(AH, query);
        const count = parseInt(getNetezzaValue(res, 0) || '0');
        freeNetezzaResult(res);
        return count > 0;
    } catch (error: any) {
        console.error('NETEZZA ERROR: Failed to validate schema:', error);
        return false;
    }
}

/**
 * Netezza-specific helper functions
 */

function freeNetezzaResult(result: NetezzaResult): void {
    if (result) {
        if (result.stmt) {
            SQLFreeHandle(SQL_HANDLE_STMT, result.stmt);
        }
    }
}

function getNetezzaValue(result: NetezzaResult, col: number): string {
    if (!result || col >= result.num_cols) return '';

    let ret: SQLRETURN;
    const buffer: string = '';
    let indicator: SQLLEN = 0;

    // Fetch the row if not already fetched
    if (result.current_row === 0) {
        ret = SQLFetch(result.stmt);
        if (!SQL_SUCCEEDED(ret) || ret === SQL_NO_DATA) return '';
        result.current_row = 1;
    }

    // Get the column value
    ret = SQLGetData(result.stmt, col + 1, SQL_C_CHAR, buffer, 4096, indicator);

    if (!SQL_SUCCEEDED(ret) || indicator === SQL_NULL_DATA) return '';

    return buffer;
}

function fetchNetezzaRow(result: NetezzaResult): boolean {
    const ret = SQLFetch(result.stmt);
    if (SQL_SUCCEEDED(ret) && ret !== SQL_NO_DATA) {
        result.current_row++;
        return true;
    }
    return false;
}

function parseNetezzaVersion(version_str: string): number {
    const parts = version_str.split('.');
    
    if (parts.length >= 4) {
        const major = parseInt(parts[0]) || 0;
        const minor = parseInt(parts[1]) || 0;
        const revision = parseInt(parts[2]) || 0;
        const build = parseInt(parts[3]) || 0;
        return major * 1000000 + minor * 10000 + revision * 100 + build;
    } else if (parts.length === 3) {
        const major = parseInt(parts[0]) || 0;
        const minor = parseInt(parts[1]) || 0;
        const revision = parseInt(parts[2]) || 0;
        return major * 1000000 + minor * 10000 + revision * 100;
    } else if (parts.length === 2) {
        const major = parseInt(parts[0]) || 0;
        const minor = parseInt(parts[1]) || 0;
        return major * 1000000 + minor * 10000;
    } else {
        return 0;
    }
}

function resetNetezzaResult(result: NetezzaResult): void {
    SQLCloseCursor(result.stmt);
    result.current_row = 0;
}

// NEW FUNCTION: Enhanced data type parsing for Netezza
function parseNetezzaDataType(formatType: string): { 
    type: string; 
    length: number | null; 
    precision: number | null; 
    scale: number | null; 
} {
    if (!formatType) {
        return { type: 'unknown', length: null, precision: null, scale: null };
    }

    let type = formatType;
    let length: number | null = null;
    let precision: number | null = null;
    let scale: number | null = null;

    // Parse type modifiers from format_type (e.g., "VARCHAR(256)" or "NUMERIC(10,2)")
    const typeMatch = formatType.match(/^([^(]+)(?:\((\d+)(?:,(\d+))?\))?$/);
    if (typeMatch) {
        type = typeMatch[1].toUpperCase();
        if (typeMatch[2]) {
            if (typeMatch[3]) {
                precision = parseInt(typeMatch[2]);
                scale = parseInt(typeMatch[3]);
            } else {
                length = parseInt(typeMatch[2]);
            }
        }
    }

    // Map Netezza-specific types to standard types
    const typeMappings: { [key: string]: string } = {
        'VARCHAR': 'VARCHAR',
        'CHAR': 'CHAR',
        'NVARCHAR': 'NVARCHAR',
        'NCHAR': 'NCHAR',
        'INTEGER': 'INTEGER',
        'INT': 'INTEGER',
        'BIGINT': 'BIGINT',
        'SMALLINT': 'SMALLINT',
        'NUMERIC': 'NUMERIC',
        'DECIMAL': 'DECIMAL',
        'REAL': 'REAL',
        'DOUBLE PRECISION': 'DOUBLE',
        'FLOAT': 'FLOAT',
        'DATE': 'DATE',
        'TIME': 'TIME',
        'TIMESTAMP': 'TIMESTAMP',
        'BOOLEAN': 'BOOLEAN',
        'BYTEINT': 'BYTEINT',
        'ST_GEOMETRY': 'GEOMETRY'
    };

    return {
        type: typeMappings[type] || type,
        length,
        precision,
        scale
    };
}

// Mock ODBC function implementations (would be provided by an actual ODBC library)
function SQLAllocHandle(_handleType: number, _inputHandle: SQLHANDLE, _outputHandle: SQLHSTMT): SQLRETURN { 
    return SQL_SUCCESS; 
}

function SQLSetEnvAttr(_environmentHandle: SQLHENV, _attribute: number, _valuePtr: any, _stringLength: number): SQLRETURN { return SQL_SUCCESS; }

function SQLDriverConnect(
    _connectionHandle: SQLHDBC, 
    _windowHandle: any, 
    _inConnectionString: string, 
    _stringLength1: number, 
    _outConnectionString: string | null, 
    _bufferLength: number, 
    _stringLength2Ptr: SQLSMALLINT | null, 
    _driverCompletion: number
): SQLRETURN { return SQL_SUCCESS; }

function SQLSetConnectAttr(_connectionHandle: SQLHDBC, _attribute: number, _valuePtr: any, _stringLength: number): SQLRETURN { return SQL_SUCCESS; }

function SQLExecDirect(_statementHandle: SQLHSTMT, _statementText: string, _textLength: number): SQLRETURN { return SQL_SUCCESS; }

function SQLGetDiagRec(
    _handleType: number, 
    _handle: SQLHANDLE, 
    _recNumber: number, 
    _sqlState: string, 
    _nativeErrorPtr: SQLINTEGER, 
    _messageText: string, 
    _bufferLength: number, 
    _textLengthPtr: SQLSMALLINT
): SQLRETURN { return SQL_NO_DATA; }

function SQLNumResultCols(_statementHandle: SQLHSTMT, _columnCountPtr: SQLSMALLINT): SQLRETURN { return SQL_SUCCESS; }

function SQLDescribeCol(
    _statementHandle: SQLHSTMT, 
    _columnNumber: number, 
    _columnName: string, 
    _bufferLength: number, 
    _nameLengthPtr: SQLSMALLINT, 
    _dataTypePtr: SQLSMALLINT, 
    _columnSizePtr: SQLULEN, 
    _decimalDigitsPtr: SQLSMALLINT, 
    _nullablePtr: SQLSMALLINT
): SQLRETURN { return SQL_SUCCESS; }

function SQLFetch(_statementHandle: SQLHSTMT): SQLRETURN { return SQL_SUCCESS; }

function SQLGetData(
    _statementHandle: SQLHSTMT, 
    _columnNumber: number, 
    _targetType: number, 
    _targetValuePtr: string, 
    _bufferLength: number, 
    _strlen_or_indPtr: SQLLEN
): SQLRETURN { return SQL_SUCCESS; }

function SQLCloseCursor(_statementHandle: SQLHSTMT): SQLRETURN { return SQL_SUCCESS; }

function SQLEndTran(_handleType: number, _handle: SQLHANDLE, _completionType: number): SQLRETURN { return SQL_SUCCESS; }

function SQLDisconnect(_connectionHandle: SQLHDBC): SQLRETURN { return SQL_SUCCESS; }

function SQLFreeHandle(_handleType: number, _handle: SQLHANDLE): SQLRETURN { return SQL_SUCCESS; }

// FIXED: Create a default export object that contains all the named exports
const NetezzaModule = {
    ArchiveHandle,
    Trivalue,
    DumpComponent,
    notice_processor,
    die_on_query_failure,
    constructConnStr,
    ExecuteSqlStatement,
    ExecuteSqlQuery,
    ExecuteSqlQueryForSingleRow,
    ConnectDatabase,
    ConnectDatabaseAhx,
    DisconnectDatabase,
    GetConnection,
    getTables,
    getTableAttrs,
    getTableCount,
    validateSchema,
    freeNetezzaResult,
    getNetezzaValue,
    fetchNetezzaRow,
    parseNetezzaVersion,
    parseNetezzaDataType,
    resetNetezzaResult
};

// Export the module as default to fix the import issue
export default NetezzaModule;

// Also keep named exports for backward compatibility
export {
    ArchiveHandle, 
    Trivalue,
    DumpComponent,
    notice_processor,
    die_on_query_failure,
    constructConnStr,
    ExecuteSqlStatement,
    ExecuteSqlQuery,
    ExecuteSqlQueryForSingleRow,
    ConnectDatabase,
    ConnectDatabaseAhx,
    DisconnectDatabase,
    GetConnection,
    getTables,
    getTableAttrs,
    getTableCount,
    validateSchema,
    freeNetezzaResult,
    getNetezzaValue,
    fetchNetezzaRow,
    parseNetezzaVersion,
    parseNetezzaDataType,
    resetNetezzaResult
};
export type {
    NetezzaResult,
    TableInfo,
    ConnParams
};