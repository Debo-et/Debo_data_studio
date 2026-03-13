// src/generators/NormalizeNumberSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';
import { UnifiedCanvasNode } from '../types/unified-pipeline.types';

interface NormalizeNumberConfig {
  column: string;
  min?: number;
  max?: number;
  targetMin?: number;
  targetMax?: number;
}

export class NormalizeNumberSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    // Use type assertion to tell TypeScript the expected shape; undefined is still possible.
    const config = node.metadata?.configuration?.config as NormalizeNumberConfig | undefined;

    if (!config || !config.column) {
      return this.fallbackSelect(node);
    }

    const col = this.sanitizeIdentifier(config.column);
    let expr = col;
    if (config.min !== undefined && config.max !== undefined) {
      const targetMin = config.targetMin ?? 0;
      const targetMax = config.targetMax ?? 1;
      expr = `((${col} - ${config.min})::float / (${config.max} - ${config.min}) * (${targetMax} - ${targetMin}) + ${targetMin})`;
    }

    const otherColumns = node.metadata?.schemas?.output?.fields
      .filter(c => c.name !== config.column)
      .map(c => this.sanitizeIdentifier(c.name)) || [];

    const selectList = [...otherColumns, `${expr} AS ${col}`].join(', ');
    const sql = `SELECT ${selectList} FROM source_table`;

    return {
      sql,
      dependencies: ['source_table'],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'normalize_number', lineCount: 1 }
    };
  }

  // Implement required abstract methods (none of these clauses are modified by this generator)
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

  // Helper to produce a basic SELECT fragment when configuration is missing
  private fallbackSelect(_node: UnifiedCanvasNode): GeneratedSQLFragment {
    return {
      sql: 'SELECT * FROM source_table',
      dependencies: ['source_table'],
      parameters: new Map(),
      errors: [], // Optionally add a warning: [{ code: 'MISSING_CONFIG', message: 'NormalizeNumber config missing, using passthrough', severity: 'WARNING' }]
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'fallback_select', lineCount: 1 }
    };
  }

  // Helper to return an empty fragment for unused clauses
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