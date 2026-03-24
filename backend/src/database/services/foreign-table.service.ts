// backend/src/database/services/foreign-table.service.ts
import { localPostgres } from '../local-postgres';
import { Logger } from '../inspection/postgreSql-inspector';

export interface ColumnDefinition {
  name: string;
  type: string;
  length?: number;
  precision?: number;
  scale?: number;
  nullable?: boolean;
  defaultValue?: string;
}

export interface ForeignTableOptions {
  format?: string;
  delimiter?: string;
  header?: string;
  sheet?: string;
  encoding?: string;
  pattern?: string;
  recordLength?: string;
  // Database connection options (used when source is a database)
  host?: string;
  port?: string;
  dbname?: string;
  user?: string;
  password?: string;
  schema_name?: string;
  table_name?: string;
  [key: string]: string | undefined;
}

export interface CreateForeignTableResult {
  success: boolean;
  error?: string;
  tableName?: string;
  sql?: string;
  warnings?: string[];
}

/**
 * Set of database types that are treated as remote sources.
 */
const DATABASE_TYPES = new Set([
  'postgresql', 'postgres', 'mysql', 'oracle', 'sqlserver', 'mssql',
  'db2', 'sap-hana', 'hana', 'sybase', 'netezza', 'informix', 'firebird'
]);

/**
 * Utility function to map application data types to PostgreSQL data types
 */
export function mapToPostgresType(
  appType: string,
  length?: number,
  precision?: number,
  scale?: number
): string {
  const typeLower = appType.toLowerCase().trim();

  // Integer types
  if (typeLower.includes('int') || typeLower.includes('integer') || typeLower === 'number') {
    if (typeLower.includes('bigint') || typeLower.includes('long')) return 'BIGINT';
    if (typeLower.includes('smallint')) return 'SMALLINT';
    return 'INTEGER';
  }

  // Decimal/Numeric types
  if (typeLower.includes('decimal') || typeLower.includes('numeric')) {
    if (precision !== undefined && scale !== undefined) {
      return `NUMERIC(${precision}, ${scale})`;
    }
    if (precision !== undefined) {
      return `NUMERIC(${precision})`;
    }
    return 'NUMERIC';
  }

  // Floating point types
  if (typeLower.includes('float') || typeLower.includes('double') || typeLower.includes('real')) {
    if (typeLower.includes('double') || typeLower.includes('float8')) return 'DOUBLE PRECISION';
    if (typeLower.includes('float4')) return 'REAL';
    return 'DOUBLE PRECISION';
  }

  // Date/Time types
  if (typeLower.includes('date') && !typeLower.includes('datetime')) {
    return 'DATE';
  }
  if (typeLower.includes('datetime') || typeLower.includes('timestamp')) {
    if (typeLower.includes('without')) return 'TIMESTAMP';
    if (typeLower.includes('with')) return 'TIMESTAMPTZ';
    return 'TIMESTAMP';
  }
  if (typeLower.includes('time')) {
    if (typeLower.includes('without')) return 'TIME';
    if (typeLower.includes('with')) return 'TIMETZ';
    return 'TIME';
  }

  // Boolean
  if (typeLower.includes('bool')) {
    return 'BOOLEAN';
  }

  // JSON types
  if (typeLower.includes('jsonb')) {
    return 'JSONB';
  }
  if (typeLower.includes('json')) {
    return 'JSON';
  }

  // XML
  if (typeLower.includes('xml')) {
    return 'XML';
  }

  // Text/String types
  if (typeLower.includes('char') || typeLower.includes('text') || typeLower.includes('string')) {
    if (typeLower.includes('var') || typeLower.includes('varchar')) {
      return length ? `VARCHAR(${length})` : 'VARCHAR';
    }
    if (typeLower.includes('char') && !typeLower.includes('var')) {
      return length ? `CHAR(${length})` : 'CHAR';
    }
    return 'TEXT';
  }

  // Default fallback
  return 'TEXT';
}

/**
 * Sanitize PostgreSQL identifier
 */
export function sanitizePostgresIdentifier(identifier: string): string {
  return identifier
    .replace(/[^a-zA-Z0-9_]/g, '_')
    .replace(/_{2,}/g, '_')
    .replace(/^_+|_+$/g, '')
    .toLowerCase();
}

/**
 * Returns the correct FDW server name for a given database type.
 * - PostgreSQL → 'postgres_fdw'
 * - Others → 'ogr_fdw'
 */
function getFDWServerForDatabaseType(dbType: string): string {
  const lowerType = dbType.toLowerCase();
  return (lowerType === 'postgresql' || lowerType === 'postgres')
    ? 'postgres_fdw'
    : 'ogr_fdw';
}

/**
 * Builds a connection string for ogr_fdw based on the source database type.
 */
function buildOgrConnectionString(dbType: string, config: ForeignTableOptions): string {
  const lowerType = dbType.toLowerCase();
  switch (lowerType) {
    case 'postgresql':
    case 'postgres':
      return `PG:host=${config.host} port=${config.port} dbname=${config.dbname} user=${config.user} password=${config.password}`;
    case 'mysql':
      return `MySQL:host=${config.host} port=${config.port} dbname=${config.dbname} user=${config.user} password=${config.password}`;
    case 'oracle':
      return `OCI:${config.user}/${config.password}@${config.host}:${config.port}/${config.dbname}`;
    case 'sqlserver':
    case 'mssql':
      return `MSSQL:Server=${config.host},${config.port};Database=${config.dbname};User Id=${config.user};Password=${config.password}`;
    default:
      // Fallback to generic PG‑style – may need adjustment for other DBs
      return `PG:host=${config.host} port=${config.port} dbname=${config.dbname} user=${config.user} password=${config.password}`;
  }
}

/**
 * Builds the OPTIONS clause for a database foreign table.
 * For PostgreSQL sources, uses standard postgres_fdw options.
 * For others, uses ogr_fdw with a datasource connection string.
 */
function buildDatabaseFDWOptions(
  dbType: string,
  options: ForeignTableOptions = {}
): string {
  const lowerType = dbType.toLowerCase();
  if (lowerType === 'postgresql' || lowerType === 'postgres') {
    const opts = [];
    if (options.host) opts.push(`host '${options.host}'`);
    if (options.port) opts.push(`port '${options.port}'`);
    if (options.dbname) opts.push(`dbname '${options.dbname}'`);
    if (options.user) opts.push(`user '${options.user}'`);
    if (options.password) opts.push(`password '${options.password}'`);
    if (options.schema_name) opts.push(`schema_name '${options.schema_name}'`);
    if (options.table_name) opts.push(`table_name '${options.table_name}'`);
    return opts.join(',\n  ');
  } else {
    // ogr_fdw
    const connectionString = buildOgrConnectionString(dbType, options);
    const opts = [`datasource '${connectionString}'`];
    if (options.table_name) opts.push(`table_name '${options.table_name}'`);
    return opts.join(',\n  ');
  }
}

/**
 * Build FDW options for CREATE FOREIGN TABLE statement (file‑based sources)
 */
function buildFDWOptions(
  filePath: string,
  fileType: string,
  options: ForeignTableOptions = {}
): string {
  const baseOptions = [`filename '${filePath}'`];

  switch (fileType.toLowerCase()) {
    case 'excel':
      if (options.sheet) baseOptions.push(`sheet '${options.sheet}'`);
      if (options.header) baseOptions.push(`header '${options.header}'`);
      break;

    case 'delimited':
    case 'csv':
    case 'tsv':
    case 'txt':
      baseOptions.push(`format '${options.format || 'csv'}'`);
      baseOptions.push(`delimiter '${options.delimiter || ','}'`);
      baseOptions.push(`header '${options.header || 'true'}'`);
      if (options.encoding) baseOptions.push(`encoding '${options.encoding}'`);
      if (options.textQualifier) baseOptions.push(`quote '${options.textQualifier}'`);
      if (options.escape) baseOptions.push(`escape '${options.escape}'`);
      break;

    case 'json':
    case 'avro':
    case 'parquet':
      baseOptions.push(`format '${fileType.toLowerCase()}'`);
      if (options.compression) baseOptions.push(`compression '${options.compression}'`);
      break;

    case 'regex':
      if (options.pattern) baseOptions.push(`pattern '${options.pattern}'`);
      if (options.flags) baseOptions.push(`flags '${options.flags}'`);
      break;

    case 'positional':
    case 'fixed':
      baseOptions.push(`format 'fixed'`);
      if (options.recordLength) baseOptions.push(`record_length '${options.recordLength}'`);
      break;

    case 'schema':
      if (options.schemaType) baseOptions.push(`schema_type '${options.schemaType}'`);
      break;
  }

  return baseOptions.join(',\n  ');
}

/**
 * Generate CREATE FOREIGN TABLE SQL for a database source.
 */
function generateDatabaseForeignTableSQL(
  tableName: string,
  columns: ColumnDefinition[],
  dbType: string,
  options: ForeignTableOptions = {}
): string {
  const sanitizedTableName = sanitizePostgresIdentifier(tableName);

  const columnDefinitions = columns
    .map((col) => {
      const sanitizedColName = sanitizePostgresIdentifier(col.name);
      const pgType = mapToPostgresType(col.type, col.length, col.precision, col.scale);

      let columnDef = `${sanitizedColName} ${pgType}`;

      if (col.nullable === false) {
        columnDef += ' NOT NULL';
      }

      if (col.defaultValue !== undefined) {
        columnDef += ` DEFAULT ${col.defaultValue}`;
      }

      return columnDef;
    })
    .join(',\n  ');

  const fdwServer = getFDWServerForDatabaseType(dbType);
  const fdwOptions = buildDatabaseFDWOptions(dbType, options);

  return `-- Auto-generated foreign table for database ${dbType} source
-- Generated: ${new Date().toISOString()}
-- Table: ${tableName}

CREATE FOREIGN TABLE IF NOT EXISTS ${sanitizedTableName} (
  ${columnDefinitions}
) SERVER ${fdwServer} OPTIONS (
  ${fdwOptions}
);

COMMENT ON FOREIGN TABLE ${sanitizedTableName} IS 'Foreign table for database ${dbType} (created ${new Date().toISOString()})';
`;
}

/**
 * Generate CREATE FOREIGN TABLE SQL for a file source.
 */
function generateFileForeignTableSQL(
  tableName: string,
  columns: ColumnDefinition[],
  fileType: string,
  filePath: string,
  options: ForeignTableOptions = {}
): string {
  const sanitizedTableName = sanitizePostgresIdentifier(tableName);

  const columnDefinitions = columns
    .map((col) => {
      const sanitizedColName = sanitizePostgresIdentifier(col.name);
      const pgType = mapToPostgresType(col.type, col.length, col.precision, col.scale);

      let columnDef = `${sanitizedColName} ${pgType}`;

      if (col.nullable === false) {
        columnDef += ' NOT NULL';
      }

      if (col.defaultValue !== undefined) {
        columnDef += ` DEFAULT ${col.defaultValue}`;
      }

      return columnDef;
    })
    .join(',\n  ');

  let fdwServer = 'fdw_delimited'; // Default
  switch (fileType.toLowerCase()) {
    case 'excel':
      fdwServer = 'fdw_excel';
      break;
    case 'xml':
      fdwServer = 'fdw_xml';
      break;
    case 'json':
    case 'avro':
    case 'parquet':
      fdwServer = 'fdw_multiformat';
      break;
    case 'regex':
      fdwServer = 'fdw_regex';
      break;
    case 'ldif':
      fdwServer = 'fdw_ldif';
      break;
    case 'positional':
    case 'fixed':
      fdwServer = 'fdw_positional';
      break;
  }

  const fdwOptions = buildFDWOptions(filePath, fileType, options);

  return `-- Auto-generated foreign table for ${fileType} file
-- Source: ${filePath}
-- Generated: ${new Date().toISOString()}
-- Table: ${tableName}

CREATE FOREIGN TABLE IF NOT EXISTS ${sanitizedTableName} (
  ${columnDefinitions}
) SERVER ${fdwServer} OPTIONS (
  ${fdwOptions}
);

COMMENT ON FOREIGN TABLE ${sanitizedTableName} IS 'Foreign table for ${fileType}: ${filePath} (created ${new Date().toISOString()})';
`;
}

/**
 * Check if the PostgreSQL connection is healthy
 */
async function checkConnectionHealth(): Promise<boolean> {
  try {
    const pool = localPostgres.getPool();
    const client = await pool.connect();

    try {
      const result = await client.query('SELECT 1 as health_check');
      return result.rows.length > 0;
    } finally {
      client.release();
    }
  } catch (error) {
    Logger.error(`Connection health check failed: ${error instanceof Error ? error.message : String(error)}`);
    return false;
  }
}

/**
 * Main function to create foreign table in PostgreSQL.
 * Handles both file-based and database sources.
 */
export async function createForeignTableInPostgres(
  _connectionId: string,
  tableName: string,
  columns: ColumnDefinition[],
  fileType: string,
  filePath: string,
  options: ForeignTableOptions = {}
): Promise<CreateForeignTableResult> {
  Logger.info(`Creating foreign table: ${tableName} for ${fileType} source`);

  // Check connection health first
  const isHealthy = await checkConnectionHealth();
  if (!isHealthy) {
    return {
      success: false,
      error: 'PostgreSQL connection is not healthy. Please ensure the database is running and accessible.'
    };
  }

  try {
    // 1. Validate inputs
    if (!tableName || tableName.trim() === '') {
      return { success: false, error: 'Table name is required' };
    }

    if (columns.length === 0) {
      return { success: false, error: 'At least one column is required' };
    }

    // Detect source type
    const isDatabaseSource = filePath === '' && DATABASE_TYPES.has(fileType.toLowerCase());

    if (!isDatabaseSource && (!filePath || filePath.trim() === '')) {
      return { success: false, error: 'File path is required for file-based sources' };
    }

    // 2. Sanitize table name
    const sanitizedTableName = sanitizePostgresIdentifier(tableName);
    if (sanitizedTableName.length > 63) {
      return {
        success: false,
        error: `Table name "${tableName}" is too long after sanitization (max 63 chars)`
      };
    }

    // 3. Generate SQL based on source type
    let sql: string;
    if (isDatabaseSource) {
      sql = generateDatabaseForeignTableSQL(tableName, columns, fileType, options);
    } else {
      sql = generateFileForeignTableSQL(tableName, columns, fileType, filePath, options);
    }
    Logger.debug(`Generated SQL: ${sql}`);

    // 4. Execute SQL using localPostgres pool
    const pool = localPostgres.getPool();
    const client = await pool.connect();

    const errorHandler = (err: Error) => {
      Logger.error(`Client connection error during foreign table creation: ${err.message}`);
      client.release(err);
    };
    client.once('error', errorHandler);

    try {
      Logger.info(`Executing foreign table creation SQL for table: ${sanitizedTableName}`);

      await client.query(sql);

      Logger.info(`Foreign table "${sanitizedTableName}" created successfully`);

      return {
        success: true,
        tableName: sanitizedTableName,
        sql: sql
      };
    } catch (queryError) {
      Logger.error(`SQL execution failed: ${queryError instanceof Error ? queryError.message : String(queryError)}`);

      let errorMessage = 'Unknown error creating foreign table';
      if (queryError instanceof Error) {
        errorMessage = queryError.message;

        // Provide user-friendly error messages
        if (errorMessage.includes('already exists')) {
          errorMessage = `Table "${tableName}" already exists. Use a different name or drop the existing table first.`;
        } else if (errorMessage.includes('FDW') || errorMessage.includes('wrapper')) {
          errorMessage = `FDW server not available for ${fileType}. Make sure Foreign Data Wrappers are installed and configured.`;
        } else if (errorMessage.includes('file not found') || errorMessage.includes('No such file')) {
          errorMessage = `File not found or inaccessible: ${filePath}\nMake sure the file exists and PostgreSQL has read permissions.`;
        } else if (errorMessage.includes('permission denied')) {
          errorMessage = 'Permission denied. Check PostgreSQL user permissions for creating foreign tables.';
        } else if (errorMessage.includes('connection') || errorMessage.includes('terminated')) {
          errorMessage = 'Database connection lost during operation. Please try again.';
        }
      }

      return {
        success: false,
        error: errorMessage
      };
    } finally {
      client.off('error', errorHandler);
      client.release();
    }
  } catch (error) {
    Logger.error(`Failed to create foreign table: ${error instanceof Error ? error.message : String(error)}`);

    let errorMessage = 'Unknown error during foreign table creation';
    if (error instanceof Error) {
      errorMessage = error.message;
    }

    return {
      success: false,
      error: errorMessage
    };
  }
}

/**
 * Drop a foreign table
 */
export async function dropForeignTable(tableName: string): Promise<{
  success: boolean;
  error?: string;
}> {
  try {
    const sanitizedTableName = sanitizePostgresIdentifier(tableName);

    Logger.info(`Dropping foreign table: ${sanitizedTableName}`);

    const pool = localPostgres.getPool();
    const client = await pool.connect();

    try {
      await client.query(`DROP FOREIGN TABLE IF EXISTS ${sanitizedTableName} CASCADE;`);
      Logger.info(`Successfully dropped foreign table: ${sanitizedTableName}`);

      return { success: true };
    } catch (error) {
      Logger.error(`Failed to execute DROP statement: ${error instanceof Error ? error.message : String(error)}`);

      let errorMessage = 'Failed to drop foreign table';
      if (error instanceof Error) {
        errorMessage = error.message;
        if (errorMessage.includes('does not exist')) {
          errorMessage = `Table "${tableName}" does not exist or is not a foreign table.`;
        }
      }

      return {
        success: false,
        error: errorMessage
      };
    } finally {
      client.release();
    }
  } catch (error) {
    Logger.error(`Failed to drop foreign table: ${error instanceof Error ? error.message : String(error)}`);
    return {
      success: false,
      error: error instanceof Error ? error.message : 'Failed to drop foreign table'
    };
  }
}