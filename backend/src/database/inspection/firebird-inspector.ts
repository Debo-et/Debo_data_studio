// ===========================================================================
// Type Definitions and Interfaces
// ===========================================================================

interface ArchiveHandle {
    connection: FirebirdConnection | null;
    transaction: FirebirdTransaction | null;
    remoteVersionStr: string;
    remoteVersion: number;
    archiveRemoteVersion: string | null;
    minRemoteVersion: number;
    maxRemoteVersion: number;
    isStandby: boolean;
    savedPassword: string | null;
}

interface FirebirdConnection {
    handle: any;
}

interface FirebirdTransaction {
    handle: any;
}

interface ConnParams {
    dbname: string;
    pghost: string | null;
    pgport: string | null;
    username: string | null;
    promptPassword: Trivalue;
    override_dbname: string | null;
}

interface TableInfo {
    dobj: DatabaseObject;
    relkind: string;
    rolname: string | null;
    numatts?: number;
    attnames?: string[];
    atttypnames?: string[];
    notnull_constrs?: (string | null)[];
    columns?: DatabaseColumnMetadata[];
}

interface DatabaseObject {
    name: string;
    objType: string;
    catId: { oid: number };
    dump: number;
    schema?: string;
}

interface DatabaseColumnMetadata {
    name: string;
    type: string;
    length: number | null;
    precision: number | null;
    scale: number | null;
    nullable: boolean;
    defaultValue: string | null;
    originalData?: any;
}

interface XSQLDA {
    version: number;
    sqln: number;
    sqld: number;
    sqlvar: XSQLVAR[];
}

interface XSQLVAR {
    sqltype: number;
    sqlscale: number;
    sqllen: number;
    sqldata: Buffer;
    sqlind: number | null;
    sqlname: string;
    relname: string;
    ownername: string;
    aliasname: string;
}

enum Trivalue {
    TRI_NO = 0,
    TRI_YES = 1,
    TRI_DEFAULT = 2
}

enum FirebirdStatus {
    NO_MORE_ROWS = 100,
    SUCCESS = 0
}

enum FirebirdSqlTypes {
    SQL_TEXT = 452,
    SQL_VARYING = 448,
    SQL_SHORT = 500,
    SQL_LONG = 496,
    SQL_FLOAT = 482,
    SQL_DOUBLE = 480,
    SQL_TIMESTAMP = 510,
    SQL_BLOB = 520,
    SQL_ARRAY = 540,
    SQL_INT64 = 580,
    SQL_NULL = 32766
}

// ===========================================================================
// Constants
// ===========================================================================

const SQL_DIALECT_V6 = 3;
const SQLDA_VERSION1 = 1;
const DSQL_drop = 2;
const DSQL_close = 1;

const DUMP_COMPONENT_DEFINITION = 0x01;
const DUMP_COMPONENT_DATA = 0x02;

// ===========================================================================
// Firebird Result Class
// ===========================================================================

class FirebirdResult {
    public stmt: any;
    public sqlda: XSQLDA;
    public ncols: number;
    public eof: boolean;
    public status: number[];
    public fetched: boolean;

    constructor(stmt: any, sqlda: XSQLDA, ncols: number) {
        this.stmt = stmt;
        this.sqlda = sqlda;
        this.ncols = ncols;
        this.eof = false;
        this.fetched = false;
        this.status = new Array(20).fill(0);
    }
}

// ===========================================================================
// Utility Functions
// ===========================================================================

/**
 * Firebird doesn't have a direct notice mechanism like PostgreSQL
 */
function notice_processor(_arg: any, message: string): void {
    console.log(`FIREBIRD INFO: ${message}`);
}

/**
 * Like pg_fatal(), but with a complaint about a particular query.
 */
function die_on_query_failure(AH: ArchiveHandle, query: string): never {
    const status = fb_get_status(AH.connection);
    console.error(`FIREBIRD ERROR: query failed: ${fb_get_error_message(status)}`);
    console.error(`FIREBIRD ERROR DETAIL: Query was: ${query}`);
    throw new Error(`Query failed: ${fb_get_error_message(status)}`);
}

/**
 * Firebird version - uses service manager connection string
 */
function constructConnStr(params: ConnParams): string {
    const parts: string[] = [];
    
    if (params.pghost) {
        if (params.pgport) {
            parts.push(`${params.pghost}/${params.pgport}`);
        } else {
            parts.push(params.pghost);
        }
    }
    
    parts.push(`:${params.override_dbname || params.dbname}`);
    
    return parts.join('');
}

// ===========================================================================
// SQL Execution Functions
// ===========================================================================

/**
 * Execute SQL statement (DDL and DML)
 */
function ExecuteSqlStatement(AH: ArchiveHandle, query: string): void {
    if (!AH.connection || !AH.transaction) {
        throw new Error('Firebird connection or transaction not initialized');
    }

    const status = new Array(20).fill(0);
    let stmt: any = null;

    try {
        // Prepare statement
        if (isc_dsql_allocate_statement(status, AH.connection, stmt)) {
            die_on_query_failure(AH, query);
        }

        // Execute without output
        if (isc_dsql_execute_immediate(
            status, 
            AH.connection, 
            AH.transaction,
            query.length,
            query,
            SQL_DIALECT_V6,
            null
        )) {
            isc_dsql_free_statement(status, stmt, DSQL_drop);
            die_on_query_failure(AH, query);
        }

        isc_dsql_free_statement(status, stmt, DSQL_drop);
    } catch (error) {
        if (stmt) {
            isc_dsql_free_statement(status, stmt, DSQL_drop);
        }
        throw error;
    }
}

/**
 * Execute SQL query and return result
 */
function ExecuteSqlQuery(AH: ArchiveHandle, query: string): FirebirdResult {
    if (!AH.connection || !AH.transaction) {
        throw new Error('Firebird connection or transaction not initialized');
    }

    const status = new Array(20).fill(0);
    let stmt: any = null;

    try {
        // Allocate statement handle
        if (isc_dsql_allocate_statement(status, AH.connection, stmt)) {
            die_on_query_failure(AH, query);
        }

        const res = new FirebirdResult(stmt, {} as XSQLDA, 0);

        // Prepare the statement
        if (isc_dsql_prepare(
            status, 
            AH.transaction, 
            res.stmt,
            query.length, 
            query, 
            SQL_DIALECT_V6, 
            null
        )) {
            isc_dsql_free_statement(status, res.stmt, DSQL_drop);
            die_on_query_failure(AH, query);
        }

        // Describe output to get column information
        res.sqlda = createXSQLDA(1);
        res.sqlda.version = SQLDA_VERSION1;
        res.sqlda.sqln = 1;

        if (isc_dsql_describe(status, res.stmt, SQL_DIALECT_V6, res.sqlda)) {
            isc_dsql_free_statement(status, res.stmt, DSQL_drop);
            die_on_query_failure(AH, query);
        }

        // Allocate proper sized sqlda if needed
        if (res.sqlda.sqld > res.sqlda.sqln) {
            res.sqlda = createXSQLDA(res.sqlda.sqld);
            res.sqlda.version = SQLDA_VERSION1;
            res.sqlda.sqln = res.sqlda.sqld;
            
            if (isc_dsql_describe(status, res.stmt, SQL_DIALECT_V6, res.sqlda)) {
                isc_dsql_free_statement(status, res.stmt, DSQL_drop);
                die_on_query_failure(AH, query);
            }
        }

        res.ncols = res.sqlda.sqld;

        // Allocate data buffers for each column
        for (let i = 0; i < res.ncols; i++) {
            const sqlvar = res.sqlda.sqlvar[i];
            sqlvar.sqldata = Buffer.alloc(getBufferSizeForType(sqlvar.sqltype, sqlvar.sqllen));
        }

        // Execute the query
        if (isc_dsql_execute(status, AH.transaction, res.stmt, SQL_DIALECT_V6, res.sqlda)) {
            isc_dsql_free_statement(status, res.stmt, DSQL_drop);
            die_on_query_failure(AH, query);
        }

        return res;
    } catch (error) {
        if (stmt) {
            isc_dsql_free_statement(status, stmt, DSQL_drop);
        }
        throw error;
    }
}

/**
 * Execute an SQL query and verify that we got exactly one row back.
 */
function ExecuteSqlQueryForSingleRow(AH: ArchiveHandle, query: string): FirebirdResult {
    const res = ExecuteSqlQuery(AH, query);
    let row_count = 0;
    const status = new Array(20).fill(0);

    // Count rows by fetching
    while (!res.eof) {
        if (isc_dsql_fetch(status, res.stmt, SQL_DIALECT_V6, res.sqlda)) {
            if (status[1] === FirebirdStatus.NO_MORE_ROWS) {
                res.eof = true;
                break;
            } else {
                freeFirebirdResult(res);
                die_on_query_failure(AH, query);
            }
        }
        row_count++;
        
        // We only care if there's exactly one row
        if (row_count > 1) break;
    }

    if (row_count !== 1) {
        console.error(`FIREBIRD ERROR: query returned ${row_count} rows instead of one: ${query}`);
        freeFirebirdResult(res);
        throw new Error(`Query returned ${row_count} rows instead of one`);
    }

    // Reset for reading
    res.eof = false;
    
    // Close and re-execute to start from beginning
    isc_dsql_free_statement(status, res.stmt, DSQL_close);
    
    // Re-execute the query
    if (isc_dsql_execute(status, AH.transaction, res.stmt, SQL_DIALECT_V6, res.sqlda)) {
        freeFirebirdResult(res);
        die_on_query_failure(AH, query);
    }

    return res;
}

// ===========================================================================
// Database Connection Management
// ===========================================================================

class FirebirdDatabaseManager {
    /**
     * Firebird version check
     */
    private static _check_database_version(AH: ArchiveHandle): void {
        try {
            // Firebird version query
            const res = ExecuteSqlQueryForSingleRow(
                AH, 
                "SELECT rdb$get_context('SYSTEM', 'ENGINE_VERSION') FROM rdb$database"
            );
            
            const version_str = getFirebirdValue(res, 0);
            if (version_str) {
                AH.remoteVersionStr = version_str;
                
                // Parse Firebird version (e.g., "4.0.0.0000")
                AH.remoteVersion = parseFirebirdVersion(version_str);
                
                if (!AH.archiveRemoteVersion) {
                    AH.archiveRemoteVersion = AH.remoteVersionStr;
                }

                // Version compatibility check
                if (AH.remoteVersion < AH.minRemoteVersion ||
                    AH.remoteVersion > AH.maxRemoteVersion) {
                    console.error("FIREBIRD ERROR: server version mismatch");
                    console.error(`FIREBIRD ERROR DETAIL: server version: ${version_str}`);
                    throw new Error("Server version mismatch");
                }

                // Check database state - Firebird equivalent
                const stateRes = ExecuteSqlQueryForSingleRow(
                    AH, 
                    "SELECT MON$BACKUP_STATE FROM MON$DATABASE"
                );
                const backup_state_str = getFirebirdValue(stateRes, 0);
                const backup_state = backup_state_str ? parseInt(backup_state_str) : 0;
                AH.isStandby = (backup_state !== 0);

                freeFirebirdResult(stateRes);
            } else {
                throw new Error("Could not retrieve server version");
            }

            freeFirebirdResult(res);
        } catch (error) {
            freeFirebirdResultOnError(AH);
            throw error;
        }
    }

    /**
     * ConnectDatabase - Firebird version
     */
    static ConnectDatabase(
        dbname: string,
        _connection_string: string | null,
        pghost: string | null,
        pgport: string | null,
        pguser: string | null,
        _prompt_password: Trivalue,
        fail_on_error: boolean,
        _progname: string,
        connstr: (string | null)[] | null,
        server_version: number[] | null,
        password: string | null,
        override_dbname: string | null
    ): FirebirdConnection | null {
        let db_handle: any = null;
        let trans_handle: any = null;
        const status = new Array(20).fill(0);
        
        try {
            // Build database parameter buffer
            const dpb: number[] = [1]; // isc_dpb_version1

            // Add username if provided
            if (pguser) {
                dpb.push(28); // isc_dpb_user_name
                dpb.push(pguser.length);
                for (let i = 0; i < pguser.length; i++) {
                    dpb.push(pguser.charCodeAt(i));
                }
            }

            // Add password if provided
            if (password) {
                dpb.push(29); // isc_dpb_password
                dpb.push(password.length);
                for (let i = 0; i < password.length; i++) {
                    dpb.push(password.charCodeAt(i));
                }
            }

            // Add charset
            dpb.push(59); // isc_dpb_lc_ctype
            dpb.push(5); // UTF8 length
            const charset = 'UTF8';
            for (let i = 0; i < charset.length; i++) {
                dpb.push(charset.charCodeAt(i));
            }

            const dpb_buffer = new Uint8Array(dpb);
            const firebird_conn_str = constructConnStr({
                dbname,
                pghost,
                pgport,
                username: pguser,
                promptPassword: Trivalue.TRI_DEFAULT,
                override_dbname
            });

            // Attempt connection
            if (isc_attach_database(
                status, 
                firebird_conn_str.length,
                firebird_conn_str, 
                db_handle, 
                dpb_buffer.length, 
                dpb_buffer
            )) {
                if (fail_on_error) {
                    console.error(`FIREBIRD ERROR: could not attach to database '${firebird_conn_str}'`);
                    fb_print_error(status);
                }
                return null;
            }

            // Start a transaction
            if (isc_start_transaction(status, trans_handle, 1, db_handle, 0, null)) {
                if (fail_on_error) {
                    console.error("FIREBIRD ERROR: could not start transaction");
                    fb_print_error(status);
                }
                isc_detach_database(status, db_handle);
                return null;
            }

            // Get server version
            if (server_version && server_version.length > 0) {
                const temp_ah: ArchiveHandle = {
                    connection: { handle: db_handle },
                    transaction: { handle: trans_handle },
                    remoteVersionStr: '',
                    remoteVersion: 0,
                    archiveRemoteVersion: null,
                    minRemoteVersion: 0,
                    maxRemoteVersion: 0,
                    isStandby: false,
                    savedPassword: null
                };
                
                try {
                    const res = ExecuteSqlQueryForSingleRow(
                        temp_ah,
                        "SELECT rdb$get_context('SYSTEM', 'ENGINE_VERSION') FROM rdb$database"
                    );
                    const version_str = getFirebirdValue(res, 0);
                    server_version[0] = version_str ? parseFirebirdVersion(version_str) : 0;
                    freeFirebirdResult(res);
                } catch (error) {
                    server_version[0] = 0;
                }
            }

            // Remember connection string
            if (connstr && connstr.length > 0) {
                connstr[0] = firebird_conn_str;
            }

            return { handle: db_handle };
        } catch (error) {
            if (db_handle) {
                isc_detach_database(status, db_handle);
            }
            if (fail_on_error) {
                throw error;
            }
            return null;
        }
    }

    /**
     * Connect to database using ArchiveHandle
     */
    static ConnectDatabaseAhx(
        AH: ArchiveHandle,
        cparams: ConnParams,
        isReconnect: boolean
    ): void {
        let prompt_password: Trivalue;
        let password: string | null = null;

        if (AH.connection) {
            throw new Error("FIREBIRD ERROR: already connected to a database");
        }

        // Never prompt for a password during a reconnection
        prompt_password = isReconnect ? Trivalue.TRI_NO : cparams.promptPassword;
        password = AH.savedPassword;

        if (prompt_password === Trivalue.TRI_YES && !password) {
            password = simple_prompt("Password: ", false);
        }

        const server_version: number[] = [];
        const connstr: (string | null)[] = [];

        AH.connection = this.ConnectDatabase(
            cparams.dbname,
            null,
            cparams.pghost,
            cparams.pgport,
            cparams.username,
            prompt_password,
            true,
            "DatabaseMetadataWizard",
            connstr,
            server_version,
            password,
            cparams.override_dbname
        );

        if (!AH.connection) {
            throw new Error("Failed to connect to database");
        }

        // Start transaction for this connection
        const status = new Array(20).fill(0);
        if (isc_start_transaction(status, AH.transaction, 1, AH.connection, 0, null)) {
            this.DisconnectDatabase(AH);
            throw new Error("Could not start transaction");
        }

        if (password && password !== AH.savedPassword) {
            // free password if it was newly allocated
        }

        // Remember connection's actual password
        if (password) {
            AH.savedPassword = password;
        }

        // Check for version mismatch
        this._check_database_version(AH);
    }

    /**
     * Close the connection to the database
     */
    static DisconnectDatabase(AH: ArchiveHandle): void {
        const status = new Array(20).fill(0);

        if (!AH.connection) return;

        // Commit any pending transaction
        if (AH.transaction) {
            try {
                isc_commit_transaction(status, AH.transaction);
            } catch (error) {
                // Try rollback on commit failure
                try {
                    isc_rollback_transaction(status, AH.transaction);
                } catch (rollbackError) {
                    // Ignore rollback errors during disconnect
                }
            }
            AH.transaction = null;
        }

        // Detach database
        try {
            isc_detach_database(status, AH.connection);
        } catch (error) {
            console.error('Error detaching database:', error);
        }
        AH.connection = null;
    }

    static GetConnection(AH: ArchiveHandle): FirebirdConnection | null {
        return AH.connection;
    }
}

// ===========================================================================
// Table Metadata Functions
// ===========================================================================

class FirebirdMetadata {
    /**
     * getTables - Firebird version
     * Query RDB$RELATIONS system table
     */
    static getTables(AH: ArchiveHandle): { tables: TableInfo[], numTables: number } {
        const query = `
            SELECT
                r.rdb$relation_id,
                TRIM(r.rdb$relation_name) as relation_name,
                r.rdb$relation_type,
                r.rdb$system_flag,
                TRIM(r.rdb$owner_name) as owner_name,
                CASE r.rdb$relation_type
                    WHEN 0 THEN 'r'  -- table
                    WHEN 1 THEN 'v'  -- view
                    WHEN 2 THEN 'S'  -- system table
                    WHEN 4 THEN 'r'  -- external table
                    WHEN 5 THEN 'v'  -- virtual view
                    ELSE '?'
                END as relkind
            FROM rdb$relations r
            WHERE r.rdb$system_flag = 0
                AND r.rdb$relation_type IN (0, 1, 4, 5)
                AND r.rdb$relation_name NOT LIKE 'RDB$%'
            ORDER BY r.rdb$relation_name
        `;

        const res = ExecuteSqlQuery(AH, query);
        const ntups = countFirebirdRows(res, AH);
        const tblinfo: TableInfo[] = [];

        resetFirebirdResult(res, AH);

        for (let i = 0; i < ntups; i++) {
            if (fetchFirebirdRow(res)) {
                const relationId = getFirebirdValue(res, 0);
                const relationName = getFirebirdValue(res, 1);
                const ownerName = getFirebirdValue(res, 4);
                const relkind = getFirebirdValue(res, 5);

                if (!relationName) continue;

                const tableInfo: TableInfo = {
                    dobj: {
                        name: relationName,
                        objType: relkind === 'v' ? "DO_VIEW" : "DO_TABLE",
                        catId: { oid: relationId ? parseInt(relationId) : i },
                        dump: 0,
                        schema: ownerName || 'public'
                    },
                    relkind: relkind || 'r',
                    rolname: ownerName,
                    columns: []
                };

                // Set dump components
                if (tableInfo.relkind === 'r' || tableInfo.relkind === 'S') {
                    tableInfo.dobj.dump = DUMP_COMPONENT_DEFINITION | DUMP_COMPONENT_DATA;
                } else if (tableInfo.relkind === 'v') {
                    tableInfo.dobj.dump = DUMP_COMPONENT_DEFINITION;
                }

                tblinfo.push(tableInfo);
            }
        }

        freeFirebirdResult(res);
        return { tables: tblinfo, numTables: tblinfo.length };
    }

    /**
     * getTableAttrs - Firebird version
     * Query RDB$RELATION_FIELDS for column information
     */
    static getTableAttrs(AH: ArchiveHandle, tblinfo: TableInfo[]): void {
        for (const table of tblinfo) {
            // Only process tables and views
            if (table.relkind !== 'r' && table.relkind !== 'v' && table.relkind !== 'S') continue;
            
            const query = `
                SELECT
                    TRIM(rf.rdb$field_name) as field_name,
                    f.rdb$field_type,
                    f.rdb$field_length,
                    f.rdb$field_scale,
                    f.rdb$field_sub_type,
                    rf.rdb$null_flag,
                    rf.rdb$default_source,
                    cs.rdb$character_set_name
                FROM rdb$relation_fields rf
                JOIN rdb$fields f ON rf.rdb$field_source = f.rdb$field_name
                LEFT JOIN rdb$character_sets cs ON f.rdb$character_set_id = cs.rdb$character_set_id
                WHERE rf.rdb$relation_name = '${table.dobj.name}'
                ORDER BY rf.rdb$field_position
            `;

            try {
                const res = ExecuteSqlQuery(AH, query);
                const ncols = countFirebirdRows(res, AH);
                
                table.numatts = ncols;
                table.columns = [];
                
                if (ncols > 0) {
                    table.attnames = [];
                    table.atttypnames = [];
                    table.notnull_constrs = [];
                    
                    resetFirebirdResult(res, AH);
                    for (let j = 0; j < ncols; j++) {
                        if (fetchFirebirdRow(res)) {
                            const colName = getFirebirdValue(res, 0);
                            const fieldType = getFirebirdValue(res, 1);
                            const fieldLength = getFirebirdValue(res, 2);
                            const fieldScale = getFirebirdValue(res, 3);
                            const fieldSubType = getFirebirdValue(res, 4);
                            const nullFlag = getFirebirdValue(res, 5);
                            const defaultValue = getFirebirdValue(res, 6);
                            const charset = getFirebirdValue(res, 7);

                            if (!colName) continue;

                            const sqlType = mapFirebirdTypeToSQL(
                                fieldType, 
                                fieldSubType, 
                                fieldLength, 
                                fieldScale,
                                charset
                            );

                            table.attnames.push(colName);
                            table.atttypnames.push(sqlType);
                            table.notnull_constrs.push(nullFlag === '1' ? 'NOT NULL' : null);

                            // Create standardized column metadata
                            const column: DatabaseColumnMetadata = {
                                name: colName,
                                type: sqlType,
                                length: fieldLength ? parseInt(fieldLength) : null,
                                precision: fieldLength ? parseInt(fieldLength) : null,
                                scale: fieldScale ? Math.abs(parseInt(fieldScale)) : null,
                                nullable: nullFlag !== '1',
                                defaultValue: defaultValue ? defaultValue.trim() : null,
                                originalData: {
                                    fieldType,
                                    fieldSubType,
                                    charset
                                }
                            };

                            table.columns!.push(column);
                        }
                    }
                }
                
                freeFirebirdResult(res);
            } catch (error) {
                console.warn(`Could not retrieve columns for table ${table.dobj.name}:`, error);
                // Continue with other tables even if one fails
            }
        }
    }
}

// ===========================================================================
// Firebird-specific Helper Functions
// ===========================================================================

/**
 * Free Firebird result
 */
function freeFirebirdResult(result: FirebirdResult): void {
    if (result && result.stmt) {
        const status = new Array(20).fill(0);
        try {
            isc_dsql_free_statement(status, result.stmt, DSQL_drop);
        } catch (error) {
            console.warn('Error freeing Firebird statement:', error);
        }
    }
}

/**
 * Free result on error
 */
function freeFirebirdResultOnError(AH: ArchiveHandle): void {
    if (AH.transaction) {
        const status = new Array(20).fill(0);
        try {
            isc_rollback_transaction(status, AH.transaction);
        } catch (error) {
            // Ignore rollback errors during error cleanup
        }
    }
}

/**
 * Get value from Firebird result
 */
function getFirebirdValue(result: FirebirdResult, col: number): string | null {
    if (!result || !result.sqlda || col >= result.ncols) return null;
        
    const sqlvar = result.sqlda.sqlvar[col];
    
    // Handle NULL
    if (sqlvar.sqlind && sqlvar.sqlind === -1) return null;
    
    const sqltype = sqlvar.sqltype & ~1; // Remove NULL flag
    const buffer = sqlvar.sqldata;

    try {
        switch (sqltype) {
            case FirebirdSqlTypes.SQL_TEXT:
                return buffer.toString('utf8').trim();
                
            case FirebirdSqlTypes.SQL_VARYING:
                const length = buffer.readUInt16LE(0);
                return buffer.toString('utf8', 2, 2 + length).trim();
                
            case FirebirdSqlTypes.SQL_SHORT:
                const shortVal = buffer.readInt16LE(0);
                return applyScale(shortVal, sqlvar.sqlscale).toString();
                
            case FirebirdSqlTypes.SQL_LONG:
                const longVal = buffer.readInt32LE(0);
                return applyScale(longVal, sqlvar.sqlscale).toString();
                
            case FirebirdSqlTypes.SQL_INT64:
                const bigVal = buffer.readBigInt64LE(0);
                return applyScaleBigInt(bigVal, sqlvar.sqlscale).toString();
                
            case FirebirdSqlTypes.SQL_FLOAT:
                return buffer.readFloatLE(0).toString();
                
            case FirebirdSqlTypes.SQL_DOUBLE:
                return buffer.readDoubleLE(0).toString();
                
            case FirebirdSqlTypes.SQL_TIMESTAMP:
                return parseFirebirdTimestamp(buffer);
                
            default:
                return buffer.toString('utf8').trim();
        }
    } catch (error) {
        console.warn(`Error converting Firebird value for column ${col}, type ${sqltype}:`, error);
        return null;
    }
}

/**
 * Count rows in Firebird result
 */
function countFirebirdRows(result: FirebirdResult, AH: ArchiveHandle): number {
    let count = 0;
    const status = new Array(20).fill(0);
    
    if (!result || !result.stmt) return 0;
    
    // Execute already done, now fetch to count
    while (!result.eof) {
        if (isc_dsql_fetch(status, result.stmt, SQL_DIALECT_V6, result.sqlda)) {
            if (status[1] === FirebirdStatus.NO_MORE_ROWS) {
                result.eof = true;
                break;
            } else {
                console.error('Error fetching row during count:', fb_get_error_message(status));
                return -1;
            }
        }
        count++;
    }
    
    // Reset for potential future reading
    result.eof = false;
    isc_dsql_free_statement(status, result.stmt, DSQL_close);
    
    // Re-execute to reset cursor
    if (isc_dsql_execute(status, AH.transaction, result.stmt, SQL_DIALECT_V6, result.sqlda)) {
        console.error('Error re-executing query after count:', fb_get_error_message(status));
        return -1;
    }
    
    return count;
}

/**
 * Fetch next row from Firebird result
 */
function fetchFirebirdRow(result: FirebirdResult): boolean {
    const status = new Array(20).fill(0);
    
    if (!result || !result.stmt || result.eof) return false;
        
    if (isc_dsql_fetch(status, result.stmt, SQL_DIALECT_V6, result.sqlda)) {
        if (status[1] === FirebirdStatus.NO_MORE_ROWS) {
            result.eof = true;
            return false;
        } else {
            console.error('Error fetching row:', fb_get_error_message(status));
            return false;
        }
    }
    
    result.fetched = true;
    return true;
}

/**
 * Reset Firebird result for reading from beginning
 */
function resetFirebirdResult(result: FirebirdResult, AH: ArchiveHandle): void {
    const status = new Array(20).fill(0);
    
    if (!result || !result.stmt) return;
    
    // Close and re-execute
    isc_dsql_free_statement(status, result.stmt, DSQL_close);
    result.eof = false;
    result.fetched = false;
    
    if (isc_dsql_execute(status, AH.transaction, result.stmt, SQL_DIALECT_V6, result.sqlda)) {
        console.error('Error resetting result:', fb_get_error_message(status));
        result.eof = true;
    }
}

/**
 * Trim trailing spaces from Firebird string
 */
function trimFirebirdString(fb_string: string | null): string {
    if (!fb_string) return "";
        
    // Firebird strings are space-padded
    let len = fb_string.length;
    while (len > 0 && fb_string[len - 1] === ' ') {
        len--;
    }
    
    return fb_string.substring(0, len);
}

/**
 * Parse Firebird version string
 */
function parseFirebirdVersion(version_str: string | null): number {
    if (!version_str) return 0;
    
    const clean_version = version_str.trim();
    const parts = clean_version.split('.');
    let major = 0, minor = 0, revision = 0, build = 0;
    
    if (parts.length >= 4) {
        major = parseInt(parts[0]) || 0;
        minor = parseInt(parts[1]) || 0;
        revision = parseInt(parts[2]) || 0;
        build = parseInt(parts[3]) || 0;
        return major * 1000000 + minor * 10000 + revision * 100 + build;
    } else if (parts.length === 3) {
        major = parseInt(parts[0]) || 0;
        minor = parseInt(parts[1]) || 0;
        revision = parseInt(parts[2]) || 0;
        return major * 1000000 + minor * 10000 + revision * 100;
    } else if (parts.length === 2) {
        major = parseInt(parts[0]) || 0;
        minor = parseInt(parts[1]) || 0;
        return major * 1000000 + minor * 10000;
    } else if (parts.length === 1) {
        major = parseInt(parts[0]) || 0;
        return major * 1000000;
    } else {
        return 0;
    }
}

/**
 * Convert Firebird type to SQL type name
 */
function mapFirebirdTypeToSQL(
    fieldType: string | null, 
    subType: string | null = null, 
    length: string | null = null, 
    scale: string | null = null,
    charset: string | null = null
): string {
    if (!fieldType) return "UNKNOWN";
    
    const typeNum = parseInt(fieldType);
    const subTypeNum = subType ? parseInt(subType) : 0;
    const lengthNum = length ? parseInt(length) : 0;
    const scaleNum = scale ? parseInt(scale) : 0;

    switch (typeNum) {
        case 7:  // SMALLINT
            return scaleNum < 0 ? `NUMERIC(4, ${Math.abs(scaleNum)})` : "SMALLINT";
            
        case 8:  // INTEGER
            return scaleNum < 0 ? `NUMERIC(9, ${Math.abs(scaleNum)})` : "INTEGER";
            
        case 10: // FLOAT
            return "FLOAT";
            
        case 11: // D_FLOAT
            return "DOUBLE PRECISION";
            
        case 12: // DATE
            return "DATE";
            
        case 13: // TIME
            return "TIME";
            
        case 14: // CHAR
            if (charset && charset.toLowerCase().includes('octets')) {
                return lengthNum > 0 ? `CHAR(${lengthNum}) FOR BIT DATA` : "CHAR(1) FOR BIT DATA";
            }
            return lengthNum > 0 ? `CHAR(${lengthNum})` : "CHAR(1)";
            
        case 16: // INT64
            if (scaleNum < 0) {
                return `NUMERIC(18, ${Math.abs(scaleNum)})`;
            }
            return "BIGINT";
            
        case 27: // DOUBLE
            return "DOUBLE PRECISION";
            
        case 35: // TIMESTAMP
            return "TIMESTAMP";
            
        case 37: // VARCHAR
            if (charset && charset.toLowerCase().includes('octets')) {
                return lengthNum > 0 ? `VARCHAR(${lengthNum}) FOR BIT DATA` : "VARCHAR(255) FOR BIT DATA";
            }
            return lengthNum > 0 ? `VARCHAR(${lengthNum})` : "VARCHAR(255)";
            
        case 261: // BLOB
            switch (subTypeNum) {
                case 0: return "BLOB";
                case 1: return "TEXT";
                case 2: return "BLOB";
                default: return "BLOB";
            }
            
        default:
            console.warn(`Unknown Firebird type: ${typeNum}, subtype: ${subTypeNum}`);
            return "UNKNOWN";
    }
}

// ===========================================================================
// Additional Helper Functions
// ===========================================================================

/**
 * Get the size of buffer needed for a specific Firebird data type
 */
function getBufferSizeForType(sqltype: number, sqllen: number): number {
    const baseType = sqltype & ~1; // Remove nullable flag
    
    switch (baseType) {
        case FirebirdSqlTypes.SQL_TEXT:
            return sqllen;
        case FirebirdSqlTypes.SQL_VARYING:
            return sqllen + 2; // +2 for length prefix
        case FirebirdSqlTypes.SQL_SHORT:
            return 2;
        case FirebirdSqlTypes.SQL_LONG:
            return 4;
        case FirebirdSqlTypes.SQL_FLOAT:
            return 4;
        case FirebirdSqlTypes.SQL_DOUBLE:
            return 8;
        case FirebirdSqlTypes.SQL_TIMESTAMP:
            return 8;
        case FirebirdSqlTypes.SQL_INT64:
            return 8;
        case FirebirdSqlTypes.SQL_BLOB:
            return 8; // Blob ID
        default:
            return 256; // Default buffer size for unknown types
    }
}

/**
 * Apply scale to numeric values for proper decimal representation
 */
function applyScale(value: number, scale: number): number {
    if (scale < 0) {
        return value / Math.pow(10, Math.abs(scale));
    }
    return value;
}

/**
 * Apply scale to BigInt values for proper decimal representation
 */
function applyScaleBigInt(value: bigint, scale: number): number {
    if (scale < 0) {
        return Number(value) / Math.pow(10, Math.abs(scale));
    }
    return Number(value);
}

/**
 * Parse Firebird timestamp to ISO string
 */
function parseFirebirdTimestamp(buffer: Buffer): string {
    if (!buffer || buffer.length < 8) {
        return new Date().toISOString();
    }

    try {
        // Firebird timestamp is 64 bits: date (32) + time (32)
        const datePart = buffer.readInt32LE(0);
        const timePart = buffer.readInt32LE(4);
        
        // Convert Firebird date (days since 1858-11-17) to JavaScript Date
        const firebirdEpoch = new Date(1858, 10, 17).getTime();
        const jsDate = new Date(firebirdEpoch + datePart * 86400000);
        
        // Add time part (ten-thousandths of a second since midnight)
        if (timePart > 0) {
            const milliseconds = timePart / 10;
            jsDate.setMilliseconds(jsDate.getMilliseconds() + milliseconds);
        }
        
        return jsDate.toISOString();
    } catch (error) {
        console.error('Error parsing Firebird timestamp:', error);
        return new Date().toISOString();
    }
}

// ===========================================================================
// Mock Firebird API Functions (would be implemented by a Firebird driver)
// ===========================================================================

function isc_dsql_allocate_statement(_status: number[], _connection: FirebirdConnection | null, _stmt: any): number {
    return FirebirdStatus.SUCCESS;
}

function isc_dsql_execute_immediate(
    _status: number[], 
    _connection: FirebirdConnection | null, 
    _transaction: FirebirdTransaction | null,
    _length: number, 
    _query: string, 
    _dialect: number, 
    _sqlda: any
): number {
    return FirebirdStatus.SUCCESS;
}

function isc_dsql_free_statement(_status: number[], _stmt: any, _option: number): number {
    return FirebirdStatus.SUCCESS;
}

function isc_dsql_prepare(
    _status: number[], 
    _transaction: FirebirdTransaction | null, 
    _stmt: any,
    _length: number, 
    _query: string, 
    _dialect: number, 
    _sqlda: any
): number {
    return FirebirdStatus.SUCCESS;
}

function isc_dsql_describe(_status: number[], _stmt: any, _dialect: number, _sqlda: XSQLDA): number {
    return FirebirdStatus.SUCCESS;
}

function isc_dsql_execute(_status: number[], _transaction: FirebirdTransaction | null, _stmt: any, _dialect: number, _sqlda: XSQLDA | null): number {
    return FirebirdStatus.SUCCESS;
}

function isc_dsql_fetch(_status: number[], _stmt: any, _dialect: number, _sqlda: XSQLDA): number {
    return FirebirdStatus.SUCCESS;
}

function isc_attach_database(
    _status: number[], 
    _length: number,
    _conn_str: string, 
    _db_handle: any, 
    _dpb_length: number, 
    _dpb: Uint8Array
): number {
    return FirebirdStatus.SUCCESS;
}

function isc_start_transaction(
    _status: number[], 
    _trans_handle: any, 
    _count: number, 
    _db_handle: FirebirdConnection | null, 
    _tpb_length: number, 
    _tpb: any
): number {
    return FirebirdStatus.SUCCESS;
}

function isc_commit_transaction(_status: number[], _transaction: FirebirdTransaction | null): number {
    return FirebirdStatus.SUCCESS;
}

function isc_rollback_transaction(_status: number[], _transaction: FirebirdTransaction | null): number {
    return FirebirdStatus.SUCCESS;
}

function isc_detach_database(_status: number[], _connection: FirebirdConnection | null): number {
    return FirebirdStatus.SUCCESS;
}

function fb_get_status(_connection: FirebirdConnection | null): number[] {
    return new Array(20).fill(0);
}

function fb_get_error_message(_status: number[]): string {
    return "Firebird error occurred";
}

function fb_print_error(_status: number[]): void {
    console.error("Firebird error occurred");
}

function createXSQLDA(size: number): XSQLDA {
    const sqlvar: XSQLVAR[] = [];
    for (let i = 0; i < size; i++) {
        sqlvar.push({
            sqltype: 0,
            sqlscale: 0,
            sqllen: 0,
            sqldata: Buffer.alloc(0),
            sqlind: null,
            sqlname: '',
            relname: '',
            ownername: '',
            aliasname: ''
        });
    }
    
    return {
        version: SQLDA_VERSION1,
        sqln: size,
        sqld: 0,
        sqlvar: sqlvar
    };
}

function simple_prompt(_prompt: string, _echo: boolean): string {
    return "password";
}

// ===========================================================================
// Default Export for DatabaseMetadataWizard Compatibility
// ===========================================================================

// Create a default export object that contains all the necessary exports
// for the DatabaseMetadataWizard to work with Firebird
const FirebirdModule = {
    Trivalue,
    FirebirdDatabaseManager,
    FirebirdMetadata,
    FirebirdResult,
    ExecuteSqlStatement,
    ExecuteSqlQuery,
    ExecuteSqlQueryForSingleRow,
    notice_processor,
    die_on_query_failure,
    constructConnStr,
    freeFirebirdResult,
    getFirebirdValue,
    countFirebirdRows,
    fetchFirebirdRow,
    resetFirebirdResult,
    trimFirebirdString,
    parseFirebirdVersion,
    mapFirebirdTypeToSQL
};

export default FirebirdModule;

// Also keep named exports for other use cases
export type {
    ArchiveHandle,
    FirebirdConnection,
    FirebirdTransaction,
    ConnParams,
    TableInfo,
    DatabaseColumnMetadata
};

export {
    Trivalue,
    FirebirdDatabaseManager,
    FirebirdMetadata,
    FirebirdResult,
    ExecuteSqlStatement,
    ExecuteSqlQuery,
    ExecuteSqlQueryForSingleRow,
    notice_processor,
    die_on_query_failure,
    constructConnStr,
    freeFirebirdResult,
    getFirebirdValue,
    countFirebirdRows,
    fetchFirebirdRow,
    resetFirebirdResult,
    trimFirebirdString,
    parseFirebirdVersion,
    mapFirebirdTypeToSQL
};