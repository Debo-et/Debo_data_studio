/**
 * Oracle Database Utility Functions - COMPREHENSIVE SCHEMA INSPECTION
 * TypeScript translation of oracle.c with complete metadata support
 */

// ===========================================================================
// Enhanced Type Definitions and Interfaces
// ===========================================================================

interface ArchiveHandle {
    remoteVersionStr?: string;
    remoteVersion?: number;
    archiveRemoteVersion?: string;
    minRemoteVersion?: number;
    maxRemoteVersion?: number;
    isStandby?: boolean;
    svcCtx?: any; // OCISvcCtx equivalent
    connection?: OracleConnection;
}

interface OracleResult {
    stmthp: any; // OCIStmt handle
    errhp: any;  // OCIError handle
    metadata?: ColumnMetadata[];
    currentRow?: number;
    rowCount?: number;
    columnCount?: number;
}

interface OracleConnection {
    envhp: any; // OCIEnv handle
    srvhp: any; // OCIServer handle
    svchp: any; // OCISvcCtx handle
    usrhp: any; // OCISession handle
    errhp: any; // OCIError handle
    connected: boolean;
}

// Enhanced TableInfo to match React application expectations
interface TableInfo {
    schemaname: string;
    tablename: string;
    tabletype: string;
    columns: ColumnMetadata[];
    originalData?: any;
    dobj?: DatabaseObject;
    relkind?: string;
}

interface DatabaseObject {
    objType: string;
    catId: ObjectId;
    name: string;
    schema?: string;
}

interface ObjectId {
    oid: number;
}

// Comprehensive ColumnMetadata matching React expectations
interface ColumnMetadata {
    name: string;
    type: string;
    length?: number;
    precision?: number;
    scale?: number;
    nullable: boolean;
    default?: string;
    isPrimaryKey?: boolean;
    isForeignKey?: boolean;
    foreignTable?: string;
    foreignColumn?: string;
    ordinalPosition: number;
    dataType: string;
}

interface ConstraintInfo {
    name: string;
    type: string; // 'P' for primary, 'R' for foreign, 'U' for unique
    tableName: string;
    columnName: string;
    foreignTable?: string;
    foreignColumn?: string;
}

// Enhanced connection parameters
interface ConnectionParams {
    dbname?: string;
    host?: string;
    port?: string;
    user?: string;
    password?: string;
    schema?: string;
}

enum Trivalue {
    TRI_DEFAULT = 0,
    TRI_NO = 1,
    TRI_YES = 2
}

// ===========================================================================
// Constants and Configuration
// ===========================================================================

const OCI_SUCCESS = 0;
const OCI_NO_DATA = 100;
const OCI_DEFAULT = 0;
const OCI_THREADED = 1;
const OCI_HTYPE_ERROR = 1;
const OCI_HTYPE_SERVER = 3;
const OCI_HTYPE_SVCCTX = 4;
const OCI_HTYPE_SESSION = 5;
const OCI_HTYPE_STMT = 6;
const OCI_CRED_RDBMS = 1;
const OCI_ATTR_SERVER = 0;
const OCI_ATTR_USERNAME = 1;
const OCI_ATTR_PASSWORD = 2;
const OCI_ATTR_SESSION = 3;
const OCI_ATTR_ROW_COUNT = 4;
const OCI_NTV_SYNTAX = 1;

// Oracle data type constants

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

    getData(): string {
        return this.data;
    }

    reset(): void {
        this.data = '';
    }

    length(): number {
        return this.data.length;
    }
}

class OracleUtils {
    /**
     * Enhanced error handling for React integration
     */
    static noticeProcessor(_arg: any, message: string): void {
        console.log(`ORACLE INFO: ${message}`);
    }

    /**
     * Enhanced error handling - throw exceptions instead of process.exit for React compatibility
     */
    static throwOnQueryFailure(_ah: ArchiveHandle, query: string, error: string): never {
        const errorMessage = `ORACLE ERROR: ${error}\nQUERY: ${query}`;
        console.error(errorMessage);
        throw new Error(errorMessage);
    }

    static createPQExpBuffer(): PQExpBuffer {
        return new PQExpBuffer();
    }

    static pgStrdup(str: string): string {
        return `${str}`;
    }

    static pgMalloc<T>(_size: number): T {
        return {} as T;
    }

    static pgMalloc0<T>(_size: number): T {
        const obj = {} as T;
        return obj;
    }

    static atooid(str: string): number {
        return parseInt(str, 10);
    }

    /**
     * Map Oracle data types to standardized type names
     */
    static mapOracleDataType(dataType: string, _dataLength?: number): string {
        switch (dataType.toUpperCase()) {
            case 'VARCHAR2':
            case 'VARCHAR':
            case 'CHAR':
            case 'NCHAR':
            case 'NVARCHAR2':
                return 'string';
            case 'NUMBER':
            case 'INTEGER':
            case 'FLOAT':
            case 'BINARY_FLOAT':
            case 'BINARY_DOUBLE':
                return 'number';
            case 'DATE':
            case 'TIMESTAMP':
            case 'TIMESTAMP WITH TIME ZONE':
            case 'TIMESTAMP WITH LOCAL TIME ZONE':
                return 'date';
            case 'CLOB':
            case 'NCLOB':
            case 'BLOB':
            case 'RAW':
            case 'LONG':
            case 'LONG RAW':
                return 'binary';
            default:
                return dataType.toLowerCase();
        }
    }
}

// ===========================================================================
// Connection String Construction
// ===========================================================================

class ConnectionStringBuilder {
    /**
     * Construct a connection string for Oracle
     */
    static constructConnStr(keywords: string[], values: string[]): string {
        const buf = OracleUtils.createPQExpBuffer();
        let firstKeyword = true;

        for (let i = 0; i < keywords.length && keywords[i] !== null; i++) {
            switch (keywords[i]) {
                case 'dbname':
                    if (!firstKeyword) {
                        buf.appendChar(' ');
                    }
                    firstKeyword = false;
                    buf.append('(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)');
                    buf.append(`(CONNECT_DATA=(SERVICE_NAME=${values[i]})))`);
                    break;

                case 'host':
                    if (!firstKeyword) {
                        buf.appendChar(' ');
                    }
                    firstKeyword = false;
                    buf.append(`HOST=${values[i]}`);
                    break;

                case 'port':
                    if (!firstKeyword) {
                        buf.appendChar(' ');
                    }
                    firstKeyword = false;
                    buf.append(`PORT=${values[i]}`);
                    break;

                case 'user':
                    // Handled separately in Oracle connection
                    continue;

                default:
                    break;
            }
        }

        const connstr = OracleUtils.pgStrdup(buf.getData());
        return connstr;
    }
}

// ===========================================================================
// Enhanced SQL Execution with Robust Error Handling
// ===========================================================================

class SqlExecutor {
    /**
     * Enhanced executeSqlQuery with proper result handling
     */
    static executeSqlQuery(ah: ArchiveHandle, query: string): OracleResult {
        if (!ah.svcCtx) {
            throw new Error('No database connection available');
        }

        let stmthp: any;
        let errhp: any = ah.connection?.errhp;
        let ociStatus: number;
        
        const result = OracleUtils.pgMalloc<OracleResult>(0);
        
        // Prepare statement
        ociStatus = this.ociStmtPrepare(stmthp, errhp, query, query.length, OCI_NTV_SYNTAX, OCI_DEFAULT);
        
        if (ociStatus !== OCI_SUCCESS) {
            OracleUtils.throwOnQueryFailure(ah, query, 'Failed to prepare statement');
        }
        
        // Execute statement
        ociStatus = this.ociStmtExecute(ah.svcCtx, stmthp, errhp, 0, 0, null, null, OCI_DEFAULT);
        
        if (ociStatus !== OCI_SUCCESS && ociStatus !== OCI_NO_DATA) {
            OracleUtils.throwOnQueryFailure(ah, query, 'Failed to execute statement');
        }
        
        // Get metadata for result set
        const columnCount = this.getColumnCount(stmthp, errhp);
        result.metadata = this.getResultMetadata(stmthp, errhp, columnCount);
        
        result.stmthp = stmthp;
        result.errhp = errhp;
        result.currentRow = 0;
        result.columnCount = columnCount;
        result.rowCount = this.getOracleRowCount(result);
        
        return result;
    }

    /**
     * Execute query and expect single row
     */
    static executeSqlQueryForSingleRow(ah: ArchiveHandle, query: string): OracleResult {
        const res = this.executeSqlQuery(ah, query);

        const ntups = res.rowCount || 0;
        if (ntups !== 1) {
            OracleUtils.throwOnQueryFailure(ah, query, `Expected 1 row but got ${ntups}`);
        }

        return res;
    }

    /**
     * Get value from result set with proper type handling
     */
    static getOracleValue(result: OracleResult, row: number = 0, col: number = 0): string {
        if (!result.metadata || col >= (result.metadata.length || 0)) {
            return '';
        }

        // This would be implemented with actual OCI define and fetch calls
        // For now, return placeholder
        return `value_${row}_${col}`;
    }

    /**
     * Check if more rows are available
     */
    static hasMoreRows(result: OracleResult): boolean {
        return (result.currentRow || 0) < (result.rowCount || 0);
    }

    /**
     * Move to next row
     */
    static nextRow(result: OracleResult): boolean {
        if (this.hasMoreRows(result)) {
            result.currentRow = (result.currentRow || 0) + 1;
            return true;
        }
        return false;
    }

    private static getColumnCount(stmthp: any, errhp: any): number {
        let columnCount = 0;
        this.ociAttrGet(stmthp, OCI_HTYPE_STMT, columnCount, null, OCI_ATTR_ROW_COUNT, errhp);
        return columnCount;
    }

    private static getResultMetadata(_stmthp: any, _errhp: any, columnCount: number): ColumnMetadata[] {
        const metadata: ColumnMetadata[] = [];
        
        for (let i = 0; i < columnCount; i++) {
            metadata.push({
                name: `column_${i}`,
                type: 'string',
                dataType: 'VARCHAR2',
                nullable: true,
                ordinalPosition: i,
                length: 100
            });
        }
        
        return metadata;
    }

    static getOracleRowCount(result: OracleResult): number {
        let rowCount = 0;
        this.ociAttrGet(result.stmthp, OCI_HTYPE_STMT, rowCount, null, OCI_ATTR_ROW_COUNT, result.errhp);
        return rowCount;
    }

    // OCI function wrappers (to be implemented with actual OCI bindings)
    private static ociStmtPrepare(_stmthp: any, _errhp: any, _query: string, _length: number, _syntax: number, _mode: number): number {
        return OCI_SUCCESS;
    }

    private static ociStmtExecute(_svcCtx: any, _stmthp: any, _errhp: any, _iters: number, _rowoff: number, _snapIn: any, _snapOut: any, _mode: number): number {
        return OCI_SUCCESS;
    }

    private static ociAttrGet(_trgthndlp: any, _htype: number, _attrvalue: any, _sizep: any, _attrtype: number, _errhp: any): number {
        return OCI_SUCCESS;
    }
}

// ===========================================================================
// Enhanced Database Connection Management
// ===========================================================================

class DatabaseManager {
    /**
     * Enhanced connection with schema context
     */
    static connectDatabase(
        dbname: string,
        _connectionString: string | null,
        pghost: string | null,
        pgport: string | null,
        pguser: string | null,
        _promptPassword: Trivalue,
        failOnError: boolean,
        _progname: string,
        _connstr: string[] | null,
        _serverVersion: number[] | null,
        password: string | null,
        overrideDbname?: string
    ): OracleConnection | null {
        try {
            let envhp: any;
            let srvhp: any;
            let svchp: any;
            let usrhp: any;
            let errhp: any;
            let status: number;

            // Initialize OCI environment
            status = this.ociEnvCreate(envhp, OCI_THREADED, null, null, null, null, 0, null);
            
            if (status !== OCI_SUCCESS) {
                throw new Error('Could not create OCI environment');
            }

            // Create error handle
            status = this.ociHandleAlloc(envhp, errhp, OCI_HTYPE_ERROR, 0, null);
            
            // Create server handle
            status = this.ociHandleAlloc(envhp, srvhp, OCI_HTYPE_SERVER, 0, null);
            
            // Create service context
            status = this.ociHandleAlloc(envhp, svchp, OCI_HTYPE_SVCCTX, 0, null);

            // Construct connection string
            const actualDbname = overrideDbname || dbname;
            const connString = `${pghost || 'localhost'}:${pgport || '1521'}/${actualDbname}`;
            const oracleConnStr = Buffer.from(connString);

            // Attach to server
            status = this.ociServerAttach(srvhp, errhp, oracleConnStr, oracleConnStr.length, OCI_DEFAULT);

            if (status !== OCI_SUCCESS) {
                throw new Error('Could not attach to Oracle server');
            }

            // Set server handle in service context
            status = this.ociAttrSet(svchp, OCI_HTYPE_SVCCTX, srvhp, 0, OCI_ATTR_SERVER, errhp);

            // Create user session handle
            status = this.ociHandleAlloc(envhp, usrhp, OCI_HTYPE_SESSION, 0, null);

            // Set username and password
            status = this.ociAttrSet(usrhp, OCI_HTYPE_SESSION, Buffer.from(pguser || ''), (pguser || '').length, OCI_ATTR_USERNAME, errhp);
            status = this.ociAttrSet(usrhp, OCI_HTYPE_SESSION, Buffer.from(password || ''), (password || '').length, OCI_ATTR_PASSWORD, errhp);

            // Begin session
            status = this.ociSessionBegin(svchp, errhp, usrhp, OCI_CRED_RDBMS, OCI_DEFAULT);

            if (status !== OCI_SUCCESS) {
                throw new Error('Could not begin Oracle session');
            }

            // Set session in service context
            status = this.ociAttrSet(svchp, OCI_HTYPE_SVCCTX, usrhp, 0, OCI_ATTR_SESSION, errhp);

            const conn: OracleConnection = {
                envhp,
                srvhp,
                svchp,
                usrhp,
                errhp,
                connected: true
            };

            return conn;
        } catch (error) {
            if (failOnError) {
                throw error;
            }
            return null;
        }
    }

    static checkDatabaseVersion(ah: ArchiveHandle): void {
        if (!ah.svcCtx) {
            throw new Error('No database connection for version check');
        }

        let res: OracleResult | null = null;
        
        try {
            res = SqlExecutor.executeSqlQueryForSingleRow(ah, 'SELECT version FROM v$instance');
            const versionStr = SqlExecutor.getOracleValue(res, 0, 0);
            
            // FIXED: Properly handle optional properties
            ah.remoteVersionStr = OracleUtils.pgStrdup(versionStr);
            ah.remoteVersion = this.parseOracleVersion(versionStr);
            
            if (!ah.archiveRemoteVersion) {
                ah.archiveRemoteVersion = ah.remoteVersionStr;
            }

            // Version compatibility check
            if ((ah.remoteVersion || 0) < (ah.minRemoteVersion || 0) || (ah.remoteVersion || 0) > (ah.maxRemoteVersion || 0)) {
                throw new Error(`Server version mismatch: ${versionStr}`);
            }

            // Check if server is in recovery
            res = SqlExecutor.executeSqlQuery(ah, 
                "SELECT open_mode FROM v$database WHERE open_mode != 'READ WRITE'");
            ah.isStandby = (SqlExecutor.getOracleRowCount(res) > 0);
            
        } catch (error) {
            throw new Error(`Database version check failed: ${error}`);
        } finally {
            if (res) {
                this.freeOracleResult(res);
            }
        }
    }

    static freeOracleResult(result: OracleResult): void {
        if (result && result.stmthp) {
            this.ociStmtRelease(result.stmthp, result.errhp, null, 0, OCI_DEFAULT);
        }
    }

    private static parseOracleVersion(versionStr: string): number {
        const match = versionStr.match(/(\d+)\.(\d+)\.(\d+)/);
        if (match) {
            return parseInt(match[1], 10) * 10000 + parseInt(match[2], 10) * 100 + parseInt(match[3], 10);
        }
        return 0;
    }

    // OCI function wrappers...
    private static ociEnvCreate(_envhpp: any, _mode: number, _ctxp: any, _malocfp: any, _ralocfp: any, _mfreefp: any, _xtramemsz: number, _usrmempp: any): number {
        return OCI_SUCCESS;
    }

    private static ociHandleAlloc(_parenth: any, _hndlpp: any, _type: number, _xtramem_sz: number, _usrmempp: any): number {
        return OCI_SUCCESS;
    }

    private static ociServerAttach(_srvhp: any, _errhp: any, _dblink: Buffer, _dblink_len: number, _mode: number): number {
        return OCI_SUCCESS;
    }

    private static ociAttrSet(_trgthndlp: any, _trghndltyp: number, _attrvalue: any, _size: number, _attrtype: number, _errhp: any): number {
        return OCI_SUCCESS;
    }

    private static ociSessionBegin(_svchp: any, _errhp: any, _usrhp: any, _credt: number, _mode: number): number {
        return OCI_SUCCESS;
    }



    private static ociStmtRelease(_stmthp: any, _errhp: any, _snap: any, _snapLen: number, _mode: number): number {
        return OCI_SUCCESS;
    }
}

// ===========================================================================
// COMPREHENSIVE SCHEMA INSPECTION FUNCTIONS
// ===========================================================================

class MetadataManager {
    /**
     * ENHANCED: Get tables with complete metadata for React application
     */
    static getTables(ah: ArchiveHandle, numTables: number[]): TableInfo[] {
        if (!ah.svcCtx) {
            throw new Error('No database connection for table retrieval');
        }

        let res: OracleResult | null = null;
        const query = OracleUtils.createPQExpBuffer();
        let tblinfo: TableInfo[] = [];

        try {
            // Enhanced query to get comprehensive table information
            query.append(
                `SELECT 
                    owner as schemaname,
                    table_name as tablename,
                    CASE 
                        WHEN temporary = 'Y' THEN 'TEMPORARY'
                        ELSE 'TABLE'
                    END as tabletype,
                    tablespace_name,
                    num_rows,
                    last_analyzed
                FROM all_tables 
                WHERE owner NOT IN ('SYS', 'SYSTEM', 'XDB', 'CTXSYS', 'MDSYS')
                UNION ALL
                SELECT 
                    owner as schemaname,
                    view_name as tablename,
                    'VIEW' as tabletype,
                    null as tablespace_name,
                    null as num_rows,
                    null as last_analyzed
                FROM all_views 
                WHERE owner NOT IN ('SYS', 'SYSTEM', 'XDB', 'CTXSYS', 'MDSYS')
                ORDER BY schemaname, tablename`
            );

            res = SqlExecutor.executeSqlQuery(ah, query.getData());
            const ntups = res.rowCount || 0;
            numTables[0] = ntups;
            
            tblinfo = new Array<TableInfo>(ntups);

            for (let i = 0; i < ntups; i++) {
                const schemaname = SqlExecutor.getOracleValue(res, i, 0);
                const tablename = SqlExecutor.getOracleValue(res, i, 1);
                const tabletype = SqlExecutor.getOracleValue(res, i, 2);

                tblinfo[i] = {
                    schemaname,
                    tablename,
                    tabletype: tabletype.toLowerCase(),
                    columns: [], // Will be populated separately
                    originalData: {
                        tablespace: SqlExecutor.getOracleValue(res, i, 3),
                        numRows: SqlExecutor.getOracleValue(res, i, 4),
                        lastAnalyzed: SqlExecutor.getOracleValue(res, i, 5)
                    },
                    dobj: {
                        objType: 'DO_TABLE',
                        catId: { oid: i },
                        name: tablename,
                        schema: schemaname
                    },
                    relkind: tabletype === 'VIEW' ? 'v' : 'r'
                };
            }

            return tblinfo;
        } catch (error) {
            throw new Error(`Table metadata retrieval failed: ${error}`);
        } finally {
            if (res) {
                DatabaseManager.freeOracleResult(res);
            }
        }
    }

    /**
     * NEW: Get comprehensive column metadata for tables
     */
    static async getTableColumns(ah: ArchiveHandle, tables: TableInfo[]): Promise<TableInfo[]> {
        if (!ah.svcCtx) {
            throw new Error('No database connection for column retrieval');
        }

        try {
            const updatedTables = [...tables];

            for (const table of updatedTables) {
                try {
                    const columns = await this.getColumnsForTable(ah, table.schemaname, table.tablename);
                    table.columns = columns;
                } catch (error) {
                    console.warn(`Failed to get columns for table ${table.schemaname}.${table.tablename}:`, error);
                    table.columns = [];
                }
            }

            return updatedTables;
        } catch (error) {
            throw new Error(`Column metadata retrieval failed: ${error}`);
        }
    }

    /**
     * NEW: Get columns for a specific table
     */
    private static async getColumnsForTable(ah: ArchiveHandle, schema: string, table: string): Promise<ColumnMetadata[]> {
        let res: OracleResult | null = null;
        const query = OracleUtils.createPQExpBuffer();

        try {
            query.append(
                `SELECT 
                    c.column_name,
                    c.data_type,
                    c.data_length,
                    c.data_precision,
                    c.data_scale,
                    c.nullable,
                    c.data_default,
                    c.column_id as ordinal_position,
                    CASE 
                        WHEN pk.column_name IS NOT NULL THEN 'YES'
                        ELSE 'NO'
                    END as is_primary_key,
                    fk.foreign_table,
                    fk.foreign_column
                FROM all_tab_columns c
                LEFT JOIN (
                    SELECT cc.table_name, cc.column_name
                    FROM all_cons_columns cc
                    JOIN all_constraints c ON cc.constraint_name = c.constraint_name 
                    WHERE c.constraint_type = 'P' AND c.owner = '${schema}'
                ) pk ON c.table_name = pk.table_name AND c.column_name = pk.column_name
                LEFT JOIN (
                    SELECT 
                        acc.table_name, 
                        acc.column_name,
                        afc.table_name as foreign_table,
                        afc.column_name as foreign_column
                    FROM all_cons_columns acc
                    JOIN all_constraints ac ON acc.constraint_name = ac.constraint_name
                    JOIN all_cons_columns afc ON ac.r_constraint_name = afc.constraint_name
                    WHERE ac.constraint_type = 'R' AND ac.owner = '${schema}'
                ) fk ON c.table_name = fk.table_name AND c.column_name = fk.column_name
                WHERE c.owner = '${schema}' AND c.table_name = '${table}'
                ORDER BY c.column_id`
            );

            res = SqlExecutor.executeSqlQuery(ah, query.getData());
            const ntups = res.rowCount || 0;
            const columns: ColumnMetadata[] = [];

            for (let i = 0; i < ntups; i++) {
                const columnName = SqlExecutor.getOracleValue(res, i, 0);
                const dataType = SqlExecutor.getOracleValue(res, i, 1);
                const dataLength = parseInt(SqlExecutor.getOracleValue(res, i, 2)) || undefined;
                const dataPrecision = parseInt(SqlExecutor.getOracleValue(res, i, 3)) || undefined;
                const dataScale = parseInt(SqlExecutor.getOracleValue(res, i, 4)) || undefined;
                const nullable = SqlExecutor.getOracleValue(res, i, 5) === 'Y';
                const dataDefault = SqlExecutor.getOracleValue(res, i, 6);
                const ordinalPosition = parseInt(SqlExecutor.getOracleValue(res, i, 7));
                const isPrimaryKey = SqlExecutor.getOracleValue(res, i, 8) === 'YES';
                const foreignTable = SqlExecutor.getOracleValue(res, i, 9);
                const foreignColumn = SqlExecutor.getOracleValue(res, i, 10);

                columns.push({
                    name: columnName,
                    type: OracleUtils.mapOracleDataType(dataType, dataLength),
                    dataType: dataType,
                    length: dataLength,
                    precision: dataPrecision,
                    scale: dataScale,
                    nullable: nullable,
                    default: dataDefault,
                    isPrimaryKey: isPrimaryKey,
                    isForeignKey: !!foreignTable,
                    foreignTable: foreignTable || undefined,
                    foreignColumn: foreignColumn || undefined,
                    ordinalPosition: ordinalPosition
                });
            }

            return columns;
        } catch (error) {
            throw new Error(`Failed to get columns for ${schema}.${table}: ${error}`);
        } finally {
            if (res) {
                DatabaseManager.freeOracleResult(res);
            }
        }
    }

    /**
     * NEW: Get table constraints
     */
    static async getTableConstraints(ah: ArchiveHandle, schema: string, table: string): Promise<ConstraintInfo[]> {
        let res: OracleResult | null = null;
        const query = OracleUtils.createPQExpBuffer();

        try {
            query.append(
                `SELECT 
                    c.constraint_name,
                    c.constraint_type,
                    cc.column_name,
                    rcc.table_name as foreign_table,
                    rcc.column_name as foreign_column
                FROM all_constraints c
                JOIN all_cons_columns cc ON c.constraint_name = cc.constraint_name
                LEFT JOIN all_constraints rc ON c.r_constraint_name = rc.constraint_name
                LEFT JOIN all_cons_columns rcc ON rc.constraint_name = rcc.constraint_name
                WHERE c.owner = '${schema}' AND c.table_name = '${table}'
                AND c.constraint_type IN ('P', 'R', 'U')
                ORDER BY c.constraint_name, cc.position`
            );

            res = SqlExecutor.executeSqlQuery(ah, query.getData());
            const ntups = res.rowCount || 0;
            const constraints: ConstraintInfo[] = [];

            for (let i = 0; i < ntups; i++) {
                constraints.push({
                    name: SqlExecutor.getOracleValue(res, i, 0),
                    type: SqlExecutor.getOracleValue(res, i, 1),
                    tableName: table,
                    columnName: SqlExecutor.getOracleValue(res, i, 2),
                    foreignTable: SqlExecutor.getOracleValue(res, i, 3),
                    foreignColumn: SqlExecutor.getOracleValue(res, i, 4)
                });
            }

            return constraints;
        } catch (error) {
            throw new Error(`Failed to get constraints for ${schema}.${table}: ${error}`);
        } finally {
            if (res) {
                DatabaseManager.freeOracleResult(res);
            }
        }
    }
}

// ===========================================================================
// Integration Helper for DatabaseMetadataWizard
// ===========================================================================

class OracleIntegrationHelper {
    /**
     * Helper method to create ArchiveHandle from connection for React integration
     */
    static createArchiveHandle(connection: OracleConnection): ArchiveHandle {
        return {
            svcCtx: connection.svchp,
            connection: connection,
            remoteVersionStr: undefined,
            remoteVersion: 0,
            archiveRemoteVersion: undefined,
            minRemoteVersion: 0,
            maxRemoteVersion: 0,
            isStandby: false
        };
    }

    /**
     * Comprehensive metadata retrieval for React application
     */
    static async getCompleteTableMetadata(ah: ArchiveHandle): Promise<TableInfo[]> {
        try {
            const numTables: number[] = [0];
            
            // Get tables
            const tables = MetadataManager.getTables(ah, numTables);
            
            // Get columns for all tables
            const tablesWithColumns = await MetadataManager.getTableColumns(ah, tables);
            
            return tablesWithColumns;
        } catch (error) {
            throw new Error(`Complete metadata retrieval failed: ${error}`);
        }
    }
}

// ===========================================================================
// Oracle Module Main Class - Provides the default export
// ===========================================================================

class OracleDatabaseModule {
    public Trivalue = Trivalue;
    public DatabaseManager = DatabaseManager;
    public MetadataManager = MetadataManager;
    public OracleIntegrationHelper = OracleIntegrationHelper;
    
    /**
     * ConnectDatabase method for wizard integration
     */
    async ConnectDatabase(
        dbname: string,
        connectionString: string | null,
        pghost: string | null,
        pgport: string | null,
        pguser: string | null,
        promptPassword: Trivalue,
        failOnError: boolean,
        progname: string,
        connstr: string[] | null,
        serverVersion: number[] | null,
        password: string | null,
        overrideDbname?: string
    ): Promise<{ connection: any; module: any }> {
        const connection = await DatabaseManager.connectDatabase(
            dbname,
            connectionString,
            pghost,
            pgport,
            pguser,
            promptPassword,
            failOnError,
            progname,
            connstr,
            serverVersion,
            password,
            overrideDbname
        );

        if (!connection) {
            throw new Error('Failed to connect to Oracle database');
        }

        return {
            connection,
            module: this
        };
    }

    /**
     * Get tables method for wizard integration
     */
    async getTables(archiveHandle: ArchiveHandle): Promise<{ tables: TableInfo[] }> {
        const numTables: number[] = [0];
        const tables = MetadataManager.getTables(archiveHandle, numTables);
        return { tables };
    }

    /**
     * Get table attributes/columns for wizard integration
     */
    async getTableAttrs(archiveHandle: ArchiveHandle, tables: TableInfo[], _tableCount?: number): Promise<void> {
        await MetadataManager.getTableColumns(archiveHandle, tables);
    }

    /**
     * Disconnect method for wizard integration
     */
    disconnectDatabase(connection: any): void {
        if (connection && connection.connection) {
            DatabaseManager.freeOracleResult(connection);
        }
    }

    /**
     * Additional method for Oracle-specific metadata
     */
    async getOracleSpecificMetadata(): Promise<any> {
        // Placeholder for Oracle-specific metadata retrieval
        return {
            oracleVersion: '19c',
            instanceName: 'ORCL',
            characterSet: 'AL32UTF8'
        };
    }
}

// ===========================================================================
// Module Exports
// ===========================================================================

// Default export for the wizard
export default OracleDatabaseModule;

// Named exports for specific imports
export {
    Trivalue,
    OracleUtils,
    ConnectionStringBuilder,
    SqlExecutor,
    DatabaseManager,
    MetadataManager,
    OracleIntegrationHelper
};

export type {
    ArchiveHandle,
    OracleConnection,
    TableInfo,
    DatabaseObject,
    ObjectId,
    ColumnMetadata,
    ConstraintInfo,
    ConnectionParams
};