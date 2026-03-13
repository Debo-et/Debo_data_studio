// src/generators/ParseRecordSetSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';
import { UnifiedCanvasNode } from '../types/unified-pipeline.types';

// Extend the config interface to include optional fields used in XML and delimited cases
interface ParseRecordSetConfig {
  sourceColumn: string;
  recordType: 'json' | 'xml' | 'delimited';
  targetColumns: Array<{ name: string; path: string; type?: string }>;
  xpath?: string;      // required for XML
  delimiter?: string;  // required for delimited
}

export class ParseRecordSetSQLGenerator extends BaseSQLGenerator {
  // Implement all required abstract methods
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

  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;

    // Safely extract configuration for this node type
    const config = this.extractConfig(node);
    if (!config || !config.sourceColumn) {
      return this.fallbackSelect(node);
    }

    const source = this.sanitizeIdentifier(config.sourceColumn);
    let lateralJoin: string;

    switch (config.recordType) {
      case 'json':
        lateralJoin = `jsonb_to_recordset(${source}::jsonb) AS parsed(${config.targetColumns.map(c => `${c.name} ${c.type || 'text'}`).join(', ')})`;
        break;
      case 'xml':
        // Use xmltable
        if (!config.xpath) {
          return this.errorFragment('xpath is required for XML parsing');
        }
        const columnsDef = config.targetColumns.map(c => `${c.name} ${c.type || 'text'} PATH '${c.path}'`).join(', ');
        lateralJoin = `xmltable('${config.xpath}' PASSING ${source}::xml COLUMNS ${columnsDef}) AS parsed`;
        break;
      case 'delimited':
        // Use regexp_split_to_table or string_to_table
        if (!config.delimiter) {
          return this.errorFragment('delimiter is required for delimited parsing');
        }
        lateralJoin = `regexp_split_to_table(${source}, '${config.delimiter}') AS parsed(${config.targetColumns[0].name})`;
        break;
      default:
        return this.fallbackSelect(node);
    }

    // Collect other columns from output schema (excluding the source column)
    const otherColumns = node.metadata?.schemas?.output?.fields
      .filter(c => c.name !== config.sourceColumn)
      .map(c => this.sanitizeIdentifier(c.name)) || [];

    const selectList = [...otherColumns, 'parsed.*'].join(', ');
    const sql = `SELECT ${selectList} FROM source_table, LATERAL ${lateralJoin}`;

    return {
      sql,
      dependencies: ['source_table'],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'parse_recordset', lineCount: 1 }
    };
  }

  /**
   * Extract and validate configuration for PARSE_RECORD_SET node.
   * Assumes configuration is stored under node.metadata.configuration.config.
   */
  private extractConfig(node: UnifiedCanvasNode): ParseRecordSetConfig | null {
    const config = (node.metadata?.configuration as any)?.config;
    if (config && typeof config === 'object') {
      // Basic shape validation
      if (
        'sourceColumn' in config &&
        'recordType' in config &&
        'targetColumns' in config &&
        Array.isArray(config.targetColumns)
      ) {
        return config as ParseRecordSetConfig;
      }
    }
    return null;
  }

  /**
   * Fallback SELECT when configuration is missing/invalid.
   */
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

  /**
   * Return an error fragment for specific missing parameters.
   */
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

  /**
   * Helper to return an empty fragment (used for unused clauses).
   */
  private emptyFragment(): GeneratedSQLFragment {
    return {
      sql: '',
      dependencies: [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'empty', lineCount: 0 }
    };
  }
}