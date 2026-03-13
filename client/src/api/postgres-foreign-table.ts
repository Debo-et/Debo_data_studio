// src/api/postgres-foreign-table.ts
// No global DatabaseApiService instance – all functions accept apiService parameter.

import { DatabaseApiService } from '../services/database-api.service';

export interface ColumnDefinition {
  name: string;
  type: string;
  length?: number;
  precision?: number;
  scale?: number;
  nullable?: boolean;
  defaultValue?: string;
}

export interface SQLExecutionResult {
  success: boolean;
  error?: string;
  data?: any[];
  rowCount?: number;
  columns?: string[];
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
 * Sanitize PostgreSQL identifier (table name, column name, etc.)
 */
export function sanitizePostgresIdentifier(identifier: string): string {
  return identifier
    .replace(/[^a-zA-Z0-9_]/g, '_')
    .replace(/_{2,}/g, '_')
    .replace(/^_+|_+$/g, '')
    .toLowerCase();
}

/**
 * Get appropriate FDW server name for file type
 */
export function getFDWServerForFileType(fileType: string): string {
  const typeLower = fileType.toLowerCase();

  switch (typeLower) {
    case 'excel':
      return 'fdw_excel';
    case 'xml':
      return 'fdw_xml';
    case 'delimited':
    case 'csv':
    case 'tsv':
    case 'txt':
      return 'fdw_delimited';
    case 'json':
    case 'avro':
    case 'parquet':
      return 'fdw_multiformat';
    case 'regex':
      return 'fdw_regex';
    case 'ldif':
      return 'fdw_ldif';
    case 'positional':
    case 'fixed':
      return 'fdw_positional';
    case 'schema':
      return 'fdw_schema';
    default:
      console.warn(`Unknown file type: ${fileType}, defaulting to fdw_delimited`);
      return 'fdw_delimited';
  }
}

/**
 * Build FDW options for CREATE FOREIGN TABLE statement
 */
export function buildFDWOptions(
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

  const fdwServer = getFDWServerForFileType(fileType);
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
 * ✅ FIXED: Execute SQL using the provided DatabaseApiService instance
 */
export async function executeSQLViaApiService(
  apiService: DatabaseApiService,
  connectionId: string,
  sql: string
): Promise<SQLExecutionResult> {
  try {
    console.log(`📤 Executing SQL via DatabaseApiService for connection ${connectionId}...`);
    const result = await apiService.executeQuery(connectionId, sql);

    const rows = Array.isArray(result.result) ? result.result : [];

    return {
      success: result.success,
      error: result.error,
      data: rows,
      rowCount: result.rowCount ?? rows.length,
    };
  } catch (error) {
    console.error('❌ Error executing SQL:', error);
    return {
      success: false,
      error: error instanceof Error ? error.message : 'Unknown error executing SQL',
    };
  }
}

/**
 * Main function to create foreign table in PostgreSQL
 */
export async function createForeignTableInPostgres(
  apiService: DatabaseApiService,
  connectionId: string,
  tableName: string,
  columns: ColumnDefinition[],
  fileType: string,
  filePath: string,
  options: ForeignTableOptions = {}
): Promise<CreateForeignTableResult> {
  try {
    if (!tableName?.trim()) {
      return { success: false, error: 'Table name is required' };
    }
    if (columns.length === 0) {
      return { success: false, error: 'At least one column is required' };
    }

    const sanitizedTableName = sanitizePostgresIdentifier(tableName);
    if (sanitizedTableName.length > 63) {
      return {
        success: false,
        error: `Table name "${tableName}" is too long after sanitization (max 63 chars).`,
      };
    }

    const sql = generateForeignTableSQL(tableName, columns, fileType, filePath, options);
    console.log('📝 Generated SQL:', sql);

    const executionResult = await executeSQLViaApiService(apiService, connectionId, sql);
    if (!executionResult.success) {
      return {
        success: false,
        error: executionResult.error || 'Failed to execute SQL',
        sql,
      };
    }

    return {
      success: true,
      tableName: sanitizedTableName,
      sql,
    };
  } catch (error) {
    console.error('❌ Error creating foreign table:', error);
    return {
      success: false,
      error: error instanceof Error ? error.message : 'Unknown error',
    };
  }
}

/**
 * Drop a foreign table
 */
export async function dropForeignTable(
  apiService: DatabaseApiService,
  connectionId: string,
  tableName: string
): Promise<SQLExecutionResult> {
  const sanitized = sanitizePostgresIdentifier(tableName);
  const sql = `DROP FOREIGN TABLE IF EXISTS ${sanitized} CASCADE;`;
  return executeSQLViaApiService(apiService, connectionId, sql);
}

/**
 * Check if FDW servers are available for a specific file type
 */
export async function checkFDWAvailability(
  apiService: DatabaseApiService,
  connectionId: string,
  fileType: string
): Promise<{ available: boolean; serverName?: string; error?: string }> {
  const fdwServer = getFDWServerForFileType(fileType);
  const sql = `
    SELECT srvname
    FROM pg_foreign_server fs
    JOIN pg_foreign_data_wrapper fdw ON fs.srvfdw = fdw.oid
    WHERE srvname = '${fdwServer}';
  `;

  const result = await executeSQLViaApiService(apiService, connectionId, sql);
  if (result.success && result.data && result.data.length > 0) {
    return { available: true, serverName: fdwServer };
  }
  return {
    available: false,
    serverName: fdwServer,
    error: `FDW server "${fdwServer}" not found.`,
  };
}