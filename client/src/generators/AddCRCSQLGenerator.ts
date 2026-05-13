// src/generators/AddCRCSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';
import { UnifiedCanvasNode } from '../types/unified-pipeline.types';

interface AddCRCConfig {
  columns?: string[];
  algorithm: 'crc32' | 'md5' | 'hashtext';
  outputColumn: string;
}

function isAddCRCConfig(obj: any): obj is AddCRCConfig {
  return obj && typeof obj === 'object'
    && typeof obj.algorithm === 'string'
    && ['crc32', 'md5', 'hashtext'].includes(obj.algorithm)
    && typeof obj.outputColumn === 'string'
    && (obj.columns === undefined || Array.isArray(obj.columns));
}

export class AddCRCSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;

    const rawConfig = node.metadata?.configuration?.config;
    const config: AddCRCConfig | undefined = isAddCRCConfig(rawConfig) ? rawConfig : undefined;

    if (!config) {
      return this.fallbackSelect(node);
    }

    const allColumns = node.metadata?.schemas?.output?.fields || [];
    const columnsToHash = config.columns?.length
      ? config.columns.map(c => this.sanitizeIdentifier(c))
      : allColumns.map(c => this.sanitizeIdentifier(c.name));

    let crcExpr: string;
    switch (config.algorithm) {
      case 'crc32':
        crcExpr = `hashtext(${columnsToHash.map(c => `COALESCE(${c}::text, '')`).join(' || ')})`;
        break;
      case 'md5':
        crcExpr = `md5(${columnsToHash.map(c => `COALESCE(${c}::text, '')`).join(' || ')})`;
        break;
      case 'hashtext':
      default:
        crcExpr = `hashtext(${columnsToHash.map(c => `COALESCE(${c}::text, '')`).join(' || ')})`;
    }

    const selectList = [
      ...allColumns.map(c => this.sanitizeIdentifier(c.name)),
      `${crcExpr} AS ${this.sanitizeIdentifier(config.outputColumn)}`
    ].join(', ');

    const sql = `SELECT ${selectList} FROM source_table`;
    return {
      sql,
      dependencies: ['source_table'],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'add_crc', lineCount: 1 }
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

  private fallbackSelect(node: UnifiedCanvasNode): GeneratedSQLFragment {
    return {
      sql: `SELECT * FROM ${this.sanitizeIdentifier(node.name)}`,
      dependencies: [node.name],
      parameters: new Map(),
      errors: [],
      warnings: ['No CRC config, using fallback'],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'crc_fallback', lineCount: 1 }
    };
  }
}