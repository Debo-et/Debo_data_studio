// backend/src/services/foreign-table.service.ts
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
 * Generate CREATE FOREIGN TABLE SQL statement
 */
export function generateForeignTableSQL(
  tableName: string,
  columns: ColumnDefinition[],
  fileType: string,
  filePath: string,
  options: ForeignTableOptions = {}
): string {
  const sanitizedTableName = sanitizePostgresIdentifier(tableName);
  
  // Build column definitions
  const columnDefinitions = columns.map(col => {
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
  }).join(',\n  ');
  
  // Determine FDW server based on file type
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
  
  // Build FDW options
  const fdwOptions = [
    `filename '${filePath}'`
  ];
  
  if (fileType.toLowerCase() === 'excel') {
    if (options.sheet) fdwOptions.push(`sheet '${options.sheet}'`);
    if (options.header) fdwOptions.push(`header '${options.header}'`);
  } else if (['delimited', 'csv', 'tsv', 'txt'].includes(fileType.toLowerCase())) {
    fdwOptions.push(`format '${options.format || 'csv'}'`);
    fdwOptions.push(`delimiter '${options.delimiter || ','}'`);
    fdwOptions.push(`header '${options.header || 'true'}'`);
    if (options.encoding) fdwOptions.push(`encoding '${options.encoding}'`);
  } else if (fileType.toLowerCase() === 'positional') {
    fdwOptions.push(`format 'fixed'`);
    if (options.recordLength) fdwOptions.push(`record_length '${options.recordLength}'`);
  }
  
  // Generate SQL
  const sql = `CREATE FOREIGN TABLE IF NOT EXISTS ${sanitizedTableName} (
  ${columnDefinitions}
) SERVER ${fdwServer} OPTIONS (
  ${fdwOptions.join(',\n  ')}
);`;

  return sql;
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
 * Main function to create foreign table in PostgreSQL
 */
export async function createForeignTableInPostgres(
  _connectionId: string,
  tableName: string,
  columns: ColumnDefinition[],
  fileType: string,
  filePath: string,
  options: ForeignTableOptions = {}
): Promise<CreateForeignTableResult> {
  Logger.info(`Creating foreign table: ${tableName} for ${fileType} file`);

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
      return {
        success: false,
        error: 'Table name is required'
      };
    }

    if (columns.length === 0) {
      return {
        success: false,
        error: 'At least one column is required'
      };
    }

    if (!filePath || filePath.trim() === '') {
      return {
        success: false,
        error: 'File path is required'
      };
    }

    // 2. Sanitize table name
    const sanitizedTableName = sanitizePostgresIdentifier(tableName);
    if (sanitizedTableName.length > 63) {
      return {
        success: false,
        error: `Table name "${tableName}" is too long after sanitization (max 63 chars)`
      };
    }

    // 3. Generate SQL
    const sql = generateForeignTableSQL(tableName, columns, fileType, filePath, options);
    Logger.debug(`Generated SQL: ${sql}`);

    // 4. Execute SQL using localPostgres pool with client error handling
    const pool = localPostgres.getPool();
    const client = await pool.connect();

    // Attach temporary error handler to prevent crashes from connection errors
    const errorHandler = (err: Error) => {
      Logger.error(`Client connection error during foreign table creation: ${err.message}`);
      // Release the client with error (pool will discard it)
      client.release(err);
    };
    client.once('error', errorHandler);

    try {
      Logger.info(`Executing foreign table creation SQL for table: ${sanitizedTableName}`);

      // Execute the SQL
      await client.query(sql);

      Logger.info(`Foreign table "${sanitizedTableName}" created successfully`);

      // Return success without attempting to verify (to avoid connection issues)
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
      // Remove the error handler and release the client (if not already released by errorHandler)
      client.off('error', errorHandler);
      if (!client.release) { // check if already released
        client.release();
      }
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
      
      return {
        success: true
      };
      
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