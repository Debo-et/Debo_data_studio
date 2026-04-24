// src/generators/ParseRecordSetSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';
import { UnifiedCanvasNode } from '../types/unified-pipeline.types';

interface ParseRecordSetConfig {
  sourceColumn: string;
  recordType: 'json' | 'xml' | 'delimited';
  targetColumns: Array<{ name: string; path: string; type?: string }>;
  xpath?: string;
  delimiter?: string;
}

export class ParseRecordSetSQLGenerator extends BaseSQLGenerator {
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

  // src/generators/ParseRecordSetSQLGenerator.ts

protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
  const { node, connection } = context;
  const config = this.extractConfig(node);

  if (!config) {
    return this.fallbackSelect(node);
  }

  const sourceTable = connection
    ? this.sanitizeIdentifier(connection.sourceNodeId)
    : this.sanitizeIdentifier(node.metadata?.tableName || 'source_table');

  const sourceColumn = this.sanitizeIdentifier(config.sourceColumn);
  let lateralJoin: string;

  switch (config.recordType) {
    case 'json':
      const columnsDef = config.targetColumns
        .map(c => `${this.sanitizeIdentifier(c.name)} ${c.type || 'text'}`)
        .join(', ');
      lateralJoin = `jsonb_to_recordset(${sourceColumn}::jsonb) AS parsed(${columnsDef})`;
      break;
    case 'xml':
      if (!config.xpath) {
        return this.errorFragment('xpath is required for XML parsing');
      }
      const xmlColumns = config.targetColumns
        .map(c => `${this.sanitizeIdentifier(c.name)} ${c.type || 'text'} PATH '${c.path}'`)
        .join(', ');
      lateralJoin = `xmltable('${config.xpath}' PASSING ${sourceColumn}::xml COLUMNS ${xmlColumns}) AS parsed`;
      break;
    case 'delimited':
      if (!config.delimiter) {
        return this.errorFragment('delimiter is required for delimited parsing');
      }
      lateralJoin = `regexp_split_to_table(${sourceColumn}, '${config.delimiter}') AS parsed(${this.sanitizeIdentifier(config.targetColumns[0].name)})`;
      break;
    default:
      return this.fallbackSelect(node);
  }

  // Include other columns from upstream (e.g., 'id') that are not being exploded
  const upstreamColumns = context.upstreamSchema || [];
  const otherColumns = upstreamColumns
    .filter(col => col.name !== config.sourceColumn)
    .map(col => this.sanitizeIdentifier(col.name));

  // Expand parsed columns explicitly with AS aliases to produce clean column names
  const parsedColumns = config.targetColumns
    .map(c => `parsed.${this.sanitizeIdentifier(c.name)} AS ${this.sanitizeIdentifier(c.name)}`)
    .join(', ');

  const selectList = otherColumns.length > 0
    ? [...otherColumns, parsedColumns].join(', ')
    : parsedColumns;

  const sql = `SELECT ${selectList} FROM ${sourceTable}, LATERAL ${lateralJoin}`;

  return {
    sql,
    dependencies: [sourceTable],
    parameters: new Map(),
    errors: [],
    warnings: [],
    metadata: { generatedAt: new Date().toISOString(), fragmentType: 'parse_recordset', lineCount: 1 }
  };
}

  /**
   * Extracts the ParseRecordSet configuration from the node metadata.
   * Supports both the standard unified configuration location (metadata.configuration.config)
   * and the legacy direct metadata path used by the test helper (metadata.parseRecordSetConfig).
   */
  private extractConfig(node: UnifiedCanvasNode): ParseRecordSetConfig | null {
    // 1. Try legacy direct metadata (used by test helpers and older code)
    const directConfig = (node.metadata as any)?.parseRecordSetConfig;
    if (directConfig) {
      const sourceColumn = directConfig.sourceColumn;
      const recordType = directConfig.recordType ?? 'json';
      let targetColumns = directConfig.targetColumns;
      if (!targetColumns && Array.isArray(directConfig.schema)) {
        targetColumns = directConfig.schema.map((col: any) => ({
          name: col.name,
          path: `$.${col.name}`,
          type: col.type || 'text'
        }));
      }
      if (sourceColumn && targetColumns && Array.isArray(targetColumns)) {
        return {
          sourceColumn,
          recordType,
          targetColumns,
          xpath: directConfig.xpath,
          delimiter: directConfig.delimiter
        };
      }
    }

    // 2. Fallback to unified configuration location (production pattern)
    const unifiedConfig = (node.metadata?.configuration as any)?.config;
    if (unifiedConfig) {
      const sourceColumn = unifiedConfig.sourceColumn;
      const recordType = unifiedConfig.recordType ?? 'json';
      let targetColumns = unifiedConfig.targetColumns;
      if (!targetColumns && Array.isArray(unifiedConfig.schema)) {
        targetColumns = unifiedConfig.schema.map((col: any) => ({
          name: col.name,
          path: `$.${col.name}`,
          type: col.type || 'text'
        }));
      }
      if (sourceColumn && targetColumns && Array.isArray(targetColumns)) {
        return {
          sourceColumn,
          recordType,
          targetColumns,
          xpath: unifiedConfig.xpath,
          delimiter: unifiedConfig.delimiter
        };
      }
    }

    return null;
  }

  private fallbackSelect(node: UnifiedCanvasNode): GeneratedSQLFragment {
    const sql = `SELECT * FROM source_table; -- Fallback: no parse config for node ${node.id}`;
    return {
      sql,
      dependencies: ['source_table'],
      parameters: new Map(),
      errors: [{
        code: 'MISSING_CONFIG',
        message: `ParseRecordSet node ${node.id} missing valid configuration`,
        severity: 'ERROR',
        suggestion: 'Check that sourceColumn, recordType, and targetColumns are defined'
      }],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'parse_recordset_fallback', lineCount: 1 }
    };
  }

  private errorFragment(message: string): GeneratedSQLFragment {
    return {
      sql: '',
      dependencies: [],
      parameters: new Map(),
      errors: [{
        code: 'PARSE_CONFIG_ERROR',
        message,
        severity: 'ERROR'
      }],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'parse_recordset_error', lineCount: 0 }
    };
  }
}