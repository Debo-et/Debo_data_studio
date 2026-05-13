// src/generators/PivotSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from '../../generators/BaseSQLGenerator';
import { UnifiedCanvasNode } from '../../types/unified-pipeline.types';

// Configuration type expected by the test
export interface PivotToColumnsDelimitedConfiguration {
  version: string;
  sourceColumn: string;
  delimiter: string;
  keyValueSeparator: string;
  columnGeneration: 'fixedList';
  fixedColumns: string[];
  missingKeyHandling: 'null' | 'default' | 'omit';
  defaultValue?: string;
  valueType: 'string' | 'integer' | 'float' | 'boolean' | 'date';
  columnPrefix: string;
  trimWhitespace: boolean;
  caseSensitiveKeys: boolean;
  errorHandling: 'fail' | 'skip' | 'warn';
  parallelization: boolean;
}

/**
 * Generates SQL to pivot a delimited key-value column into fixed columns.
 * @param config - Configuration for the pivot operation
 * @param sourceTable - Name of the source table
 * @param sourceAlias - Alias for the source table in the query
 * @returns SQL SELECT statement
 */
export function generatePivotSQL(
  config: PivotToColumnsDelimitedConfiguration,
  sourceTable: string,
  sourceAlias: string
): string {
  if (!config.fixedColumns.length) {
    throw new Error('No fixed columns defined');
  }

  const {
    sourceColumn,
    delimiter,
    keyValueSeparator,
    fixedColumns,
    missingKeyHandling,
    defaultValue,
    valueType,
    columnPrefix,
    trimWhitespace,
  } = config;

  const escapedDelimiter = escapeRegex(delimiter);
  const escapedSeparator = escapeRegex(keyValueSeparator);

  const selectItems = fixedColumns.map((col) => {
    // Build regex pattern: (?:^|delimiter)key:separator([^delimiter]*)
    const pattern = `(?:^|${escapedDelimiter})${col}${escapedSeparator}([^${escapedDelimiter}]*)`;
    // regexp_match returns NULL if no match, otherwise an array; we take the first capture group
    let expression = `(regexp_match(${sourceAlias}.${quoteIdentifier(sourceColumn)}, '${pattern}'))[1]`;

    if (trimWhitespace) {
      expression = `TRIM(${expression})`;
    }

    // Handle missing keys
    if (missingKeyHandling === 'null') {
      expression = `NULLIF(${expression}, '')`;
    } else if (missingKeyHandling === 'default' && defaultValue !== undefined) {
      expression = `COALESCE(NULLIF(${expression}, ''), '${defaultValue}')`;
    }
    // For 'omit', we keep the raw expression (may be NULL)

    // Type casting
    if (valueType !== 'string') {
      const sqlType = mapValueTypeToSQL(valueType);
      expression = `CAST(${expression} AS ${sqlType})`;
    }

    const columnAlias = `${columnPrefix}${col}`;
    return `${expression} AS ${quoteIdentifier(columnAlias)}`;
  });

  const selectClause = selectItems.join(',\n        ');
  return `SELECT\n        ${selectClause}\n      FROM ${quoteIdentifier(sourceTable)} AS ${quoteIdentifier(sourceAlias)};`;
}

// --- Helper functions ---

/** Escape special regex characters in a string */
function escapeRegex(str: string): string {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/** Quote an identifier (table/column/alias) to avoid SQL injection and reserved words */
function quoteIdentifier(ident: string): string {
  // For PostgreSQL, use double quotes
  return `"${ident.replace(/"/g, '""')}"`;
}

/** Map valueType string to PostgreSQL type name */
function mapValueTypeToSQL(type: string): string {
  switch (type) {
    case 'integer': return 'INTEGER';
    case 'float': return 'FLOAT';
    case 'boolean': return 'BOOLEAN';
    case 'date': return 'DATE';
    default: return 'TEXT';
  }
}

// --- Existing class (unchanged, kept for compatibility) ---

interface PivotConfig {
  rowIdentifier: string;
  pivotColumn: string;
  valueColumn: string;
  aggregateFunction: 'SUM' | 'AVG' | 'COUNT' | 'MIN' | 'MAX';
}

export class PivotSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    const config = this.extractPivotConfig(node);
    if (!config) {
      return this.fallbackSelect(node);
    }

    const sql = `SELECT * FROM crosstab(
      'SELECT ${config.rowIdentifier}, ${config.pivotColumn}, ${config.valueColumn}
       FROM source_table
       ORDER BY 1,2',
      'SELECT DISTINCT ${config.pivotColumn} FROM source_table ORDER BY 1'
    ) AS ct(rowid ${this.getDataType(config.rowIdentifier)}, ${this.generatePivotColumns(config)});`;

    return {
      sql,
      dependencies: ['source_table'],
      parameters: new Map(),
      errors: [],
      warnings: ['Requires tablefunc extension', 'Pivot column types are inferred as text; adjust as needed'],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'pivot', lineCount: sql.split('\n').length }
    };
  }

  protected generateJoinConditions(_context: SQLGenerationContext): GeneratedSQLFragment {
    return this.emptyFragment();
  }

  protected generateWhereClause(_context: SQLGenerationContext): GeneratedSQLFragment {
    return this.emptyFragment();
  }

  protected generateHavingClause(_context: SQLGenerationContext): GeneratedSQLFragment {
    return this.emptyFragment();
  }

  protected generateOrderByClause(_context: SQLGenerationContext): GeneratedSQLFragment {
    return this.emptyFragment();
  }

  protected generateGroupByClause(_context: SQLGenerationContext): GeneratedSQLFragment {
    return this.emptyFragment();
  }

  private generatePivotColumns(_config: PivotConfig): string {
    return 'col1 text, col2 text';
  }

  private getDataType(_column: string): string {
    return 'text';
  }

  private extractPivotConfig(node: UnifiedCanvasNode): PivotConfig | null {
    const config = node.metadata?.configuration?.config;
    if (config && this.isValidPivotConfig(config)) {
      return config as PivotConfig;
    }
    return null;
  }

  private isValidPivotConfig(obj: any): obj is PivotConfig {
    return obj &&
      typeof obj.rowIdentifier === 'string' &&
      typeof obj.pivotColumn === 'string' &&
      typeof obj.valueColumn === 'string' &&
      ['SUM', 'AVG', 'COUNT', 'MIN', 'MAX'].includes(obj.aggregateFunction);
  }

  private fallbackSelect(node: UnifiedCanvasNode): GeneratedSQLFragment {
    const tableName = this.extractTableName(node);
    return {
      sql: `SELECT * FROM ${this.sanitizeIdentifier(tableName)}`,
      dependencies: [tableName],
      parameters: new Map(),
      errors: [{
        code: 'MISSING_PIVOT_CONFIG',
        message: 'Pivot configuration not found or invalid, using fallback SELECT *',
        severity: 'ERROR'
      }],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'fallback_select', lineCount: 1 }
    };
  }

  private extractTableName(node: UnifiedCanvasNode): string {
    return node.name.toLowerCase().replace(/\s+/g, '_');
  }
}