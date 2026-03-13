// src/api/postgres-foreign-table-reverse.ts
// Accepts apiService from context – no global instances.

import { DatabaseApiService } from '../services/database-api.service';
import {
  ColumnDefinition,
  ForeignTableOptions,
  executeSQLViaApiService,
  sanitizePostgresIdentifier,
} from './postgres-foreign-table';

export const FDW_SERVER_TO_FILE_TYPE: Record<string, string> = {
  fdw_excel: 'excel',
  fdw_delimited: 'csv',
  fdw_multiformat: 'json',
  fdw_xml: 'xml',
  fdw_regex: 'regex',
  fdw_ldif: 'ldif',
  fdw_positional: 'fixed',
  fdw_schema: 'schema',
};

/**
 * Reverse PostgreSQL data type to app type + length/precision/scale.
 */
export function reverseMapPostgresType(pgType: string): {
  type: string;
  length?: number;
  precision?: number;
  scale?: number;
} {
  const lower = pgType.toLowerCase().trim();

  // Numeric
  const numericMatch = lower.match(/^numeric\((\d+),\s*(\d+)\)$/);
  if (numericMatch) {
    return {
      type: 'numeric',
      precision: parseInt(numericMatch[1], 10),
      scale: parseInt(numericMatch[2], 10),
    };
  }
  const numericPrecisionMatch = lower.match(/^numeric\((\d+)\)$/);
  if (numericPrecisionMatch) {
    return { type: 'numeric', precision: parseInt(numericPrecisionMatch[1], 10) };
  }
  const decimalMatch = lower.match(/^decimal\((\d+),\s*(\d+)\)$/);
  if (decimalMatch) {
    return {
      type: 'numeric',
      precision: parseInt(decimalMatch[1], 10),
      scale: parseInt(decimalMatch[2], 10),
    };
  }
  const decimalPrecisionMatch = lower.match(/^decimal\((\d+)\)$/);
  if (decimalPrecisionMatch) {
    return { type: 'numeric', precision: parseInt(decimalPrecisionMatch[1], 10) };
  }

  // Character
  const varcharMatch = lower.match(/^(?:character varying|varchar)\((\d+)\)$/);
  if (varcharMatch) {
    return { type: 'varchar', length: parseInt(varcharMatch[1], 10) };
  }
  const charMatch = lower.match(/^(?:character|char)\((\d+)\)$/);
  if (charMatch) {
    return { type: 'char', length: parseInt(charMatch[1], 10) };
  }

  // Integer
  if (lower === 'integer' || lower === 'int') return { type: 'integer' };
  if (lower === 'bigint') return { type: 'bigint' };
  if (lower === 'smallint') return { type: 'smallint' };

  // Floating point
  if (lower === 'double precision' || lower === 'double') return { type: 'double' };
  if (lower === 'real' || lower === 'float4') return { type: 'float' };

  // Date/time
  if (lower === 'date') return { type: 'date' };
  if (lower === 'timestamp' || lower === 'timestamp without time zone')
    return { type: 'timestamp' };
  if (lower === 'timestamptz' || lower === 'timestamp with time zone')
    return { type: 'timestamptz' };
  if (lower === 'time' || lower === 'time without time zone') return { type: 'time' };
  if (lower === 'timetz' || lower === 'time with time zone') return { type: 'timetz' };

  // Boolean
  if (lower === 'boolean' || lower === 'bool') return { type: 'boolean' };

  // JSON
  if (lower === 'json') return { type: 'json' };
  if (lower === 'jsonb') return { type: 'jsonb' };

  // XML
  if (lower === 'xml') return { type: 'xml' };

  // Text fallback
  if (lower === 'text') return { type: 'text' };

  return { type: 'text' };
}

/**
 * Parse FDW options array into ForeignTableOptions object.
 */
export function parseFDWOptions(optionsArray: string[] | null): {
  options: ForeignTableOptions;
  filePath?: string;
} {
  const options: ForeignTableOptions = {};
  let filePath: string | undefined;

  if (!optionsArray) return { options, filePath };

  for (const item of optionsArray) {
    const eqIndex = item.indexOf('=');
    if (eqIndex === -1) continue;
    const key = item.substring(0, eqIndex).trim();
    let value = item.substring(eqIndex + 1).trim();
    if (value.startsWith("'") && value.endsWith("'")) {
      value = value.slice(1, -1);
    }
    options[key] = value;
    if (key === 'filename') filePath = value;
  }

  return { options, filePath };
}

/**
 * Infer file type from FDW server name and options.
 */
export function inferFileType(serverName: string, options: ForeignTableOptions): string {
  let baseType = FDW_SERVER_TO_FILE_TYPE[serverName] || 'unknown';

  if (serverName === 'fdw_delimited' && options.format) {
    const fmt = options.format.toLowerCase();
    if (fmt === 'tsv' || fmt === 'delimited' || fmt === 'csv') baseType = fmt;
  }

  if (serverName === 'fdw_multiformat' && options.format) {
    const fmt = options.format.toLowerCase();
    if (['avro', 'parquet', 'json'].includes(fmt)) baseType = fmt;
  }

  if (serverName === 'fdw_positional' && options.format === 'fixed') {
    baseType = 'fixed';
  }

  return baseType;
}

/**
 * ✅ FIXED: Get all foreign tables using provided apiService.
 */
export async function getForeignTablesInSchema(
  apiService: DatabaseApiService,
  connectionId: string,
  _schema?: string
): Promise<Array<{ schemaname: string; tablename: string; oid: number }>> {
  const sql = `
    SELECT
      n.nspname AS schemaname,
      c.relname AS tablename,
      c.oid
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE c.relkind = 'f'
    ORDER BY n.nspname, c.relname;
  `;

  console.log(`🐘 [getForeignTablesInSchema] Executing SQL (all schemas)...`);
  const result = await executeSQLViaApiService(apiService, connectionId, sql);

  if (!result.success) {
    console.error(`🐘 [getForeignTablesInSchema] Failed:`, result.error);
    throw new Error(`Failed to list foreign tables: ${result.error}`);
  }

  const mapped = (result.data || []).map((row: any) => ({
    schemaname: row.schemaname,
    tablename: row.tablename,
    oid: parseInt(row.oid, 10),
  }));

  console.log(`🐘 [getForeignTablesInSchema] Found ${mapped.length} foreign tables`);
  return mapped;
}

/**
 * ✅ FIXED: Reverse‑engineer complete UI metadata for a single foreign table.
 */
export async function reverseForeignTableMetadata(
  apiService: DatabaseApiService,
  connectionId: string,
  schema: string,
  tableName: string
): Promise<{
  tableName: string;
  columns: ColumnDefinition[];
  fileType: string;
  filePath?: string;
  options: ForeignTableOptions;
}> {
  console.log(`🔎 [reverseForeignTableMetadata] Starting for ${schema}.${tableName}`);

  const sanitizedSchema = sanitizePostgresIdentifier(schema);
  const sanitizedTable = sanitizePostgresIdentifier(tableName);

  // 1. Get table OID
  const oidQuery = `
    SELECT c.oid
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE n.nspname = '${sanitizedSchema}'
      AND c.relname = '${sanitizedTable}'
      AND c.relkind = 'f';
  `;
  const oidResult = await executeSQLViaApiService(apiService, connectionId, oidQuery);
  if (!oidResult.success || !oidResult.data || oidResult.data.length === 0) {
    throw new Error(`Foreign table "${sanitizedSchema}.${sanitizedTable}" not found.`);
  }
  const tableOid = oidResult.data[0].oid;

  // 2. Get columns
  const columnSql = `
    SELECT
      a.attname,
      pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
      a.attnotnull,
      pg_get_expr(ad.adbin, ad.adrelid) AS column_default
    FROM pg_attribute a
    LEFT JOIN pg_attrdef ad ON (a.attrelid = ad.adrelid AND a.attnum = ad.adnum)
    WHERE a.attrelid = ${tableOid}
      AND a.attnum > 0
      AND NOT a.attisdropped
    ORDER BY a.attnum;
  `;
  const columnResult = await executeSQLViaApiService(apiService, connectionId, columnSql);
  if (!columnResult.success) {
    throw new Error(`Failed to retrieve columns: ${columnResult.error}`);
  }

  const columns: ColumnDefinition[] = (columnResult.data || []).map((row: any) => {
    const { type, length, precision, scale } = reverseMapPostgresType(row.data_type);
    const colDef: ColumnDefinition = {
      name: row.attname,
      type,
      nullable: !row.attnotnull,
    };
    if (length !== undefined) colDef.length = length;
    if (precision !== undefined) colDef.precision = precision;
    if (scale !== undefined) colDef.scale = scale;
    if (row.column_default) colDef.defaultValue = row.column_default;
    return colDef;
  });

  // 3. Get FDW server and options
  const fdwSql = `
    SELECT
      fs.srvname,
      ft.ftoptions
    FROM pg_foreign_table ft
    JOIN pg_foreign_server fs ON ft.ftserver = fs.oid
    WHERE ft.ftrelid = ${tableOid};
  `;
  const fdwResult = await executeSQLViaApiService(apiService, connectionId, fdwSql);
  if (!fdwResult.success || !fdwResult.data || fdwResult.data.length === 0) {
    throw new Error(`FDW information not found for table OID ${tableOid}`);
  }

  const serverName: string = fdwResult.data[0].srvname;
  const optionsArray: string[] = fdwResult.data[0].ftoptions || [];
  const { options, filePath } = parseFDWOptions(optionsArray);
  const fileType = inferFileType(serverName, options);

  return {
    tableName: sanitizedTable,
    columns,
    fileType,
    filePath,
    options,
  };
}