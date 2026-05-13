/**
 * SQL Server Database Utility Functions - COMPREHENSIVE SCHEMA INSPECTION
 * TypeScript implementation with complete metadata support for SQL Server
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
    connection?: SqlServerConnection;
}

interface SqlServerResult {
    recordsets: any[];
    recordset: any[];
    rowsAffected: number[];
    output: any;
    columns?: ColumnMetadata[];
    currentRow?: number;
    rowCount?: number;
    columnCount?: number;
}

interface SqlServerConnection {
    config: ConnectionConfig;
    connected: boolean;
    connection?: any; // Tedious Connection object
}

interface ConnectionConfig {
    server: string;
    port?: number;
    database: string;
    user: string;
    password: string;
    options: {
        encrypt: boolean;
        trustServerCertificate: boolean;
        enableArithAbort: boolean;
    };
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
    type: string; // 'PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE'
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

const SQL_SERVER_DEFAULT_PORT = 1433;

// SQL Server data type constants

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

class SqlServerUtils {
    /**
     * Enhanced error handling for React integration
     */
    static noticeProcessor(_arg: any, message: string): void {
        console.log(`SQL SERVER INFO: ${message}`);
    }

    /**
     * Enhanced error handling - throw exceptions instead of process.exit for React compatibility
     */
    static throwOnQueryFailure(_ah: ArchiveHandle, query: string, error: string): never {
        const errorMessage = `SQL SERVER ERROR: ${error}\nQUERY: ${query}`;
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
     * Map SQL Server data types to standardized type names
     */
    static mapSqlServerDataType(dataType: string, _dataLength?: number): string {
        switch (dataType.toLowerCase()) {
            case 'varchar':
            case 'nvarchar':
            case 'char':
            case 'nchar':
            case 'text':
            case 'ntext':
                return 'string';
            case 'int':
            case 'bigint':
            case 'smallint':
            case 'tinyint':
            case 'decimal':
            case 'numeric':
            case 'float':
            case 'real':
            case 'money':
            case 'smallmoney':
                return 'number';
            case 'datetime':
            case 'datetime2':
            case 'smalldatetime':
            case 'date':
            case 'time':
            case 'datetimeoffset':
                return 'date';
            case 'bit':
                return 'boolean';
            case 'uniqueidentifier':
                return 'uuid';
            case 'varbinary':
            case 'image':
            case 'binary':
                return 'binary';
            default:
                return dataType.toLowerCase();
        }
    }

    /**
     * Escape SQL Server identifiers
     */
    static quoteIdentifier(identifier: string): string {
        return `[${identifier}]`;
    }

    /**
     * Escape SQL Server string literals
     */
    static quoteLiteral(str: string): string {
        return `'${str.replace(/'/g, "''")}'`;
    }
}

// ===========================================================================
// Connection String Construction
// ===========================================================================

class ConnectionStringBuilder {
    /**
     * Construct a connection string for SQL Server
     */
    static constructConnStr(keywords: string[], values: string[]): ConnectionConfig {
        const config: ConnectionConfig = {
            server: 'localhost',
            database: 'master',
            user: '',
            password: '',
            options: {
                encrypt: false,
                trustServerCertificate: true,
                enableArithAbort: true
            }
        };

        for (let i = 0; i < keywords.length && keywords[i] !== null; i++) {
            switch (keywords[i]) {
                case 'dbname':
                    config.database = values[i];
                    break;
                case 'host':
                    config.server = values[i];
                    break;
                case 'port':
                    config.port = parseInt(values[i], 10);
                    break;
                case 'user':
                    config.user = values[i];
                    break;
                case 'password':
                    config.password = values[i];
                    break;
                default:
                    break;
            }
        }

        return config;
    }

    /**
     * Build connection string from config
     */
    static buildConnectionString(config: ConnectionConfig): string {
        const parts: string[] = [];
        
        parts.push(`Server=${config.server}`);
        if (config.port) {
            parts[parts.length - 1] += `,${config.port}`;
        }
        parts.push(`Database=${config.database}`);
        parts.push(`User Id=${config.user}`);
        parts.push(`Password=${config.password}`);
        parts.push(`Encrypt=${config.options.encrypt}`);
        parts.push(`TrustServerCertificate=${config.options.trustServerCertificate}`);
        
        return parts.join(';');
    }
}

// ===========================================================================
// Enhanced SQL Execution with Robust Error Handling
// ===========================================================================

class SqlExecutor {
    /**
     * Enhanced executeSqlQuery with proper result handling
     */
    static async executeSqlQuery(ah: ArchiveHandle, query: string): Promise<SqlServerResult> {
        if (!ah.connection) {
            throw new Error('No database connection available');
        }

        try {
            // This would be implemented with actual SQL Server driver calls
            // For now, return mock result structure
            const result = SqlServerUtils.pgMalloc<SqlServerResult>(0);
            
            // Mock implementation - in real scenario, this would use mssql or tedious
            result.recordsets = [];
            result.recordset = [];
            result.rowsAffected = [0];
            result.output = {};
            result.currentRow = 0;
            result.rowCount = 0;
            result.columnCount = 0;
            
            // Simulate metadata extraction
            result.columns = this.extractMetadataFromQuery(query);
            
            return result;
        } catch (error) {
            SqlServerUtils.throwOnQueryFailure(ah, query, `Query execution failed: ${error}`);
        }
    }

    /**
     * Execute query and expect single row
     */
    static async executeSqlQueryForSingleRow(ah: ArchiveHandle, query: string): Promise<SqlServerResult> {
        const res = await this.executeSqlQuery(ah, query);

        const ntups = res.rowCount || 0;
        if (ntups !== 1) {
            SqlServerUtils.throwOnQueryFailure(ah, query, `Expected 1 row but got ${ntups}`);
        }

        return res;
    }

    /**
     * Get value from result set with proper type handling
     */
    static getSqlServerValue(result: SqlServerResult, row: number = 0, col: number = 0): string {
        if (!result.recordset || row >= result.recordset.length || !result.columns || col >= result.columns.length) {
            return '';
        }

        const record = result.recordset[row];
        const columnName = result.columns[col].name;
        return record[columnName]?.toString() || '';
    }

    /**
     * Check if more rows are available
     */
    static hasMoreRows(result: SqlServerResult): boolean {
        return (result.currentRow || 0) < (result.rowCount || 0);
    }

    /**
     * Move to next row
     */
    static nextRow(result: SqlServerResult): boolean {
        if (this.hasMoreRows(result)) {
            result.currentRow = (result.currentRow || 0) + 1;
            return true;
        }
        return false;
    }

    /**
     * Extract metadata from query result
     */
    private static extractMetadataFromQuery(_query: string): ColumnMetadata[] {
        // In real implementation, this would extract metadata from the result set
        // For now, return mock metadata
        return [
            {
                name: 'schemaname',
                type: 'string',
                dataType: 'nvarchar',
                nullable: false,
                ordinalPosition: 1,
                length: 128
            },
            {
                name: 'tablename',
                type: 'string',
                dataType: 'nvarchar',
                nullable: false,
                ordinalPosition: 2,
                length: 128
            }
        ];
    }

    /**
     * Get row count from result
     */
    static getSqlServerRowCount(result: SqlServerResult): number {
        return result.rowsAffected[0] || 0;
    }
}

// ===========================================================================
// Enhanced Database Connection Management
// ===========================================================================

class DatabaseManager {
    /**
     * Enhanced connection with schema context
     */
    static async connectDatabase(
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
    ): Promise<SqlServerConnection | null> {
        try {
            const actualDbname = overrideDbname || dbname;
            
            const config: ConnectionConfig = {
                server: pghost || 'localhost',
                port: pgport ? parseInt(pgport, 10) : SQL_SERVER_DEFAULT_PORT,
                database: actualDbname,
                user: pguser || '',
                password: password || '',
                options: {
                    encrypt: false,
                    trustServerCertificate: true,
                    enableArithAbort: true
                }
            };

            // In real implementation, this would use mssql or tedious to establish connection
            // For now, create mock connection
            const connection: SqlServerConnection = {
                config,
                connected: true
            };

            // Verify connection by executing a simple query
            const testAh: ArchiveHandle = { connection };
            await SqlExecutor.executeSqlQuery(testAh, 'SELECT 1 AS test');

            return connection;
        } catch (error) {
            if (failOnError) {
                throw new Error(`Failed to connect to SQL Server: ${error}`);
            }
            return null;
        }
    }

    static async checkDatabaseVersion(ah: ArchiveHandle): Promise<void> {
        if (!ah.connection) {
            throw new Error('No database connection for version check');
        }

        let res: SqlServerResult | null = null;
        
        try {
            res = await SqlExecutor.executeSqlQueryForSingleRow(ah, 
                "SELECT SERVERPROPERTY('ProductVersion') AS version, SERVERPROPERTY('ProductLevel') AS level, SERVERPROPERTY('Edition') AS edition");
            const versionStr = SqlExecutor.getSqlServerValue(res, 0, 0);
            
            // FIXED: Properly handle optional properties
            ah.remoteVersionStr = SqlServerUtils.pgStrdup(versionStr);
            ah.remoteVersion = this.parseSqlServerVersion(versionStr);
            
            if (!ah.archiveRemoteVersion) {
                ah.archiveRemoteVersion = ah.remoteVersionStr;
            }

            // Version compatibility check
            if ((ah.remoteVersion || 0) < (ah.minRemoteVersion || 0) || (ah.remoteVersion || 0) > (ah.maxRemoteVersion || 0)) {
                throw new Error(`Server version mismatch: ${versionStr}`);
            }

            // Check if server is in recovery (mirroring/always on)
            res = await SqlExecutor.executeSqlQuery(ah, 
                "SELECT database_id, name FROM sys.databases WHERE state != 0 OR state IS NULL");
            ah.isStandby = (SqlExecutor.getSqlServerRowCount(res) > 0);
            
        } catch (error) {
            throw new Error(`Database version check failed: ${error}`);
        } finally {
            // Clean up resources if needed
        }
    }

    static freeSqlServerResult(_result: SqlServerResult): void {
        // Clean up resources if needed
        // SQL Server driver typically handles cleanup automatically
    }

    private static parseSqlServerVersion(versionStr: string): number {
        const match = versionStr.match(/(\d+)\.(\d+)\.(\d+)/);
        if (match) {
            return parseInt(match[1], 10) * 10000 + parseInt(match[2], 10) * 100 + parseInt(match[3], 10);
        }
        return 0;
    }

    /**
     * Disconnect from database
     */
    static disconnectDatabase(connection: SqlServerConnection): void {
        if (connection && connection.connected) {
            // Close actual connection here
            connection.connected = false;
        }
    }
}

// ===========================================================================
// COMPREHENSIVE SCHEMA INSPECTION FUNCTIONS
// ===========================================================================

class MetadataManager {
    /**
     * ENHANCED: Get tables with complete metadata for React application
     */
    static async getTables(ah: ArchiveHandle, numTables: number[]): Promise<TableInfo[]> {
        if (!ah.connection) {
            throw new Error('No database connection for table retrieval');
        }

        let res: SqlServerResult | null = null;
        const query = SqlServerUtils.createPQExpBuffer();
        let tblinfo: TableInfo[] = [];

        try {
            // Enhanced query to get comprehensive table information for SQL Server
            query.append(
                `SELECT 
                    s.name as schemaname,
                    t.name as tablename,
                    CASE 
                        WHEN t.type = 'U' THEN 'TABLE'
                        WHEN t.type = 'V' THEN 'VIEW'
                        ELSE t.type
                    END as tabletype,
                    p.rows as row_count,
                    ep.value as description
                FROM sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                LEFT JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0,1)
                LEFT JOIN sys.extended_properties ep ON t.object_id = ep.major_id AND ep.minor_id = 0 AND ep.class = 1
                WHERE t.is_ms_shipped = 0
                UNION ALL
                SELECT 
                    s.name as schemaname,
                    v.name as tablename,
                    'VIEW' as tabletype,
                    NULL as row_count,
                    ep.value as description
                FROM sys.views v
                INNER JOIN sys.schemas s ON v.schema_id = s.schema_id
                LEFT JOIN sys.extended_properties ep ON v.object_id = ep.major_id AND ep.minor_id = 0 AND ep.class = 1
                WHERE v.is_ms_shipped = 0
                ORDER BY schemaname, tablename`
            );

            res = await SqlExecutor.executeSqlQuery(ah, query.getData());
            const ntups = res.rowCount || 0;
            numTables[0] = ntups;
            
            tblinfo = new Array<TableInfo>(ntups);

            for (let i = 0; i < ntups; i++) {
                const schemaname = SqlExecutor.getSqlServerValue(res, i, 0);
                const tablename = SqlExecutor.getSqlServerValue(res, i, 1);
                const tabletype = SqlExecutor.getSqlServerValue(res, i, 2);

                tblinfo[i] = {
                    schemaname,
                    tablename,
                    tabletype: tabletype.toLowerCase(),
                    columns: [], // Will be populated separately
                    originalData: {
                        rowCount: SqlExecutor.getSqlServerValue(res, i, 3),
                        description: SqlExecutor.getSqlServerValue(res, i, 4)
                    },
                    dobj: {
                        objType: tabletype === 'VIEW' ? 'DO_VIEW' : 'DO_TABLE',
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
                DatabaseManager.freeSqlServerResult(res);
            }
        }
    }

    /**
     * NEW: Get comprehensive column metadata for tables
     */
    static async getTableColumns(ah: ArchiveHandle, tables: TableInfo[]): Promise<TableInfo[]> {
        if (!ah.connection) {
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
        let res: SqlServerResult | null = null;
        const query = SqlServerUtils.createPQExpBuffer();

        try {
            const quotedSchema = SqlServerUtils.quoteIdentifier(schema);
            const quotedTable = SqlServerUtils.quoteIdentifier(table);

            query.append(
                `SELECT 
                    c.column_id as ordinal_position,
                    c.name as column_name,
                    t.name as data_type,
                    c.max_length as character_maximum_length,
                    c.precision as numeric_precision,
                    c.scale as numeric_scale,
                    CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END as is_nullable,
                    OBJECT_DEFINITION(c.default_object_id) as column_default,
                    CASE WHEN pk.column_id IS NOT NULL THEN 'YES' ELSE 'NO' END as is_primary_key,
                    fk.referenced_table as foreign_table,
                    fk.referenced_column as foreign_column
                FROM sys.columns c
                INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                INNER JOIN sys.tables tab ON c.object_id = tab.object_id
                INNER JOIN sys.schemas s ON tab.schema_id = s.schema_id
                LEFT JOIN (
                    SELECT 
                        ic.column_id,
                        ic.object_id
                    FROM sys.index_columns ic
                    INNER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                    WHERE i.is_primary_key = 1
                ) pk ON c.column_id = pk.column_id AND c.object_id = pk.object_id
                LEFT JOIN (
                    SELECT 
                        fc.parent_column_id,
                        OBJECT_NAME(fk.referenced_object_id) as referenced_table,
                        COL_NAME(fk.referenced_object_id, fc.referenced_column_id) as referenced_column
                    FROM sys.foreign_key_columns fc
                    INNER JOIN sys.foreign_keys fk ON fc.constraint_object_id = fk.object_id
                ) fk ON c.column_id = fk.parent_column_id AND c.object_id = OBJECT_ID('${quotedSchema}.${quotedTable}')
                WHERE s.name = '${schema}' AND tab.name = '${table}'
                ORDER BY c.column_id`
            );

            res = await SqlExecutor.executeSqlQuery(ah, query.getData());
            const ntups = res.rowCount || 0;
            const columns: ColumnMetadata[] = [];

            for (let i = 0; i < ntups; i++) {
                const ordinalPosition = parseInt(SqlExecutor.getSqlServerValue(res, i, 0));
                const columnName = SqlExecutor.getSqlServerValue(res, i, 1);
                const dataType = SqlExecutor.getSqlServerValue(res, i, 2);
                const dataLength = parseInt(SqlExecutor.getSqlServerValue(res, i, 3)) || undefined;
                const dataPrecision = parseInt(SqlExecutor.getSqlServerValue(res, i, 4)) || undefined;
                const dataScale = parseInt(SqlExecutor.getSqlServerValue(res, i, 5)) || undefined;
                const nullable = SqlExecutor.getSqlServerValue(res, i, 6) === 'YES';
                const dataDefault = SqlExecutor.getSqlServerValue(res, i, 7);
                const isPrimaryKey = SqlExecutor.getSqlServerValue(res, i, 8) === 'YES';
                const foreignTable = SqlExecutor.getSqlServerValue(res, i, 9);
                const foreignColumn = SqlExecutor.getSqlServerValue(res, i, 10);

                columns.push({
                    name: columnName,
                    type: SqlServerUtils.mapSqlServerDataType(dataType, dataLength),
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
                DatabaseManager.freeSqlServerResult(res);
            }
        }
    }

    /**
     * NEW: Get table constraints
     */
    static async getTableConstraints(ah: ArchiveHandle, schema: string, table: string): Promise<ConstraintInfo[]> {
        let res: SqlServerResult | null = null;
        const query = SqlServerUtils.createPQExpBuffer();

        try {

            query.append(
                `SELECT 
                    c.name as constraint_name,
                    CASE 
                        WHEN c.type = 'PK' THEN 'PRIMARY KEY'
                        WHEN c.type = 'FK' THEN 'FOREIGN KEY'
                        WHEN c.type = 'UQ' THEN 'UNIQUE'
                        ELSE c.type
                    END as constraint_type,
                    col.name as column_name,
                    OBJECT_SCHEMA_NAME(fk.referenced_object_id) as foreign_schema,
                    OBJECT_NAME(fk.referenced_object_id) as foreign_table,
                    ref_col.name as foreign_column
                FROM sys.indexes i
                INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                INNER JOIN sys.columns col ON ic.object_id = col.object_id AND ic.column_id = col.column_id
                INNER JOIN sys.tables t ON i.object_id = t.object_id
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                LEFT JOIN sys.foreign_keys fk ON i.object_id = fk.parent_object_id AND i.name = fk.name
                LEFT JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id AND ic.column_id = fkc.parent_column_id
                LEFT JOIN sys.columns ref_col ON fkc.referenced_column_id = ref_col.column_id AND fkc.referenced_object_id = ref_col.object_id
                LEFT JOIN sys.key_constraints c ON i.object_id = c.parent_object_id AND i.index_id = c.unique_index_id
                WHERE s.name = '${schema}' AND t.name = '${table}'
                AND i.is_primary_key = 1 OR i.is_unique_constraint = 1 OR fk.object_id IS NOT NULL
                ORDER BY c.name, ic.key_ordinal`
            );

            res = await SqlExecutor.executeSqlQuery(ah, query.getData());
            const ntups = res.rowCount || 0;
            const constraints: ConstraintInfo[] = [];

            for (let i = 0; i < ntups; i++) {
                constraints.push({
                    name: SqlExecutor.getSqlServerValue(res, i, 0),
                    type: SqlExecutor.getSqlServerValue(res, i, 1),
                    tableName: table,
                    columnName: SqlExecutor.getSqlServerValue(res, i, 2),
                    foreignTable: SqlExecutor.getSqlServerValue(res, i, 4), // Skip foreign_schema for now
                    foreignColumn: SqlExecutor.getSqlServerValue(res, i, 5)
                });
            }

            return constraints;
        } catch (error) {
            throw new Error(`Failed to get constraints for ${schema}.${table}: ${error}`);
        } finally {
            if (res) {
                DatabaseManager.freeSqlServerResult(res);
            }
        }
    }

    /**
     * NEW: Get SQL Server specific metadata
     */
    static async getSqlServerSpecificMetadata(ah: ArchiveHandle): Promise<any> {
        if (!ah.connection) {
            throw new Error('No database connection for metadata retrieval');
        }

        let res: SqlServerResult | null = null;

        try {
            res = await SqlExecutor.executeSqlQueryForSingleRow(ah,
                `SELECT 
                    DB_NAME() as database_name,
                    @@SERVERNAME as server_name,
                    @@VERSION as version,
                    SERVERPROPERTY('Collation') as collation,
                    SERVERPROPERTY('EngineEdition') as engine_edition`
            );

            return {
                databaseName: SqlExecutor.getSqlServerValue(res, 0, 0),
                serverName: SqlExecutor.getSqlServerValue(res, 0, 1),
                version: SqlExecutor.getSqlServerValue(res, 0, 2),
                collation: SqlExecutor.getSqlServerValue(res, 0, 3),
                engineEdition: SqlExecutor.getSqlServerValue(res, 0, 4)
            };
        } catch (error) {
            throw new Error(`SQL Server specific metadata retrieval failed: ${error}`);
        } finally {
            if (res) {
                DatabaseManager.freeSqlServerResult(res);
            }
        }
    }
}

// ===========================================================================
// Integration Helper for DatabaseMetadataWizard
// ===========================================================================

class SqlServerIntegrationHelper {
    /**
     * Helper method to create ArchiveHandle from connection for React integration
     */
    static createArchiveHandle(connection: SqlServerConnection): ArchiveHandle {
        return {
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
            const tables = await MetadataManager.getTables(ah, numTables);
            
            // Get columns for all tables
            const tablesWithColumns = await MetadataManager.getTableColumns(ah, tables);
            
            return tablesWithColumns;
        } catch (error) {
            throw new Error(`Complete metadata retrieval failed: ${error}`);
        }
    }
}

// ===========================================================================
// SQL Server Module Main Class - Provides the default export
// ===========================================================================

class SqlServerDatabaseModule {
    public Trivalue = Trivalue;
    public DatabaseManager = DatabaseManager;
    public MetadataManager = MetadataManager;
    public SqlServerIntegrationHelper = SqlServerIntegrationHelper;
    
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
            throw new Error('Failed to connect to SQL Server database');
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
        const tables = await MetadataManager.getTables(archiveHandle, numTables);
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
            DatabaseManager.disconnectDatabase(connection.connection);
        }
    }

    /**
     * Additional method for SQL Server-specific metadata
     */
    async getSqlServerSpecificMetadata(archiveHandle: ArchiveHandle): Promise<any> {
        return await MetadataManager.getSqlServerSpecificMetadata(archiveHandle);
    }
}

// ===========================================================================
// Module Exports
// ===========================================================================

// Default export for the wizard
export default SqlServerDatabaseModule;

// Named exports for specific imports
export {
    Trivalue,
    SqlServerUtils,
    ConnectionStringBuilder,
    SqlExecutor,
    DatabaseManager,
    MetadataManager,
    SqlServerIntegrationHelper
};

export type {
    ArchiveHandle,
    SqlServerConnection,
    TableInfo,
    DatabaseObject,
    ObjectId,
    ColumnMetadata,
    ConstraintInfo,
    ConnectionParams
};