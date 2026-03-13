// src/generators/NormalizeSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';

interface NormalizeConfig {
  arrayColumn: string;
  elementColumn: string;
}

export class NormalizeSQLGenerator extends BaseSQLGenerator {
  // Implement all required abstract methods
  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    // Safely cast configuration; handle undefined
    const config = node.metadata?.configuration?.config as NormalizeConfig | undefined;

    if (!config?.arrayColumn) {
      // Return a minimal fallback instead of calling missing method
      return {
        sql: `SELECT * FROM source_table`,
        dependencies: ['source_table'],
        parameters: new Map(),
        errors: [],
        warnings: [],
        metadata: { generatedAt: new Date().toISOString(), fragmentType: 'normalize_fallback', lineCount: 1 }
      };
    }

    const otherColumns = node.metadata?.schemas?.output?.fields
      .filter(c => c.name !== config.arrayColumn)
      .map(c => this.sanitizeIdentifier(c.name)) || [];

    // Use unnest to normalize the array
    const sql = `SELECT ${otherColumns.join(', ')}, unnest(${this.sanitizeIdentifier(config.arrayColumn)}) AS ${this.sanitizeIdentifier(config.elementColumn)} FROM source_table`;

    return {
      sql,
      dependencies: ['source_table'],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'normalize', lineCount: 1 }
    };
  }

  // Provide empty implementations for remaining clauses
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

  // Helper to return an empty fragment (similar to SelectSQLGenerator)
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