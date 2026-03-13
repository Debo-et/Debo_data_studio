// src/generators/MatchGroupSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';
import { UnifiedCanvasNode } from '../types/unified-pipeline.types';

interface MatchGroupConfig {
  groupingKeys: string[];
  survivorshipRules: Array<{ column: string; rule: 'FIRST' | 'LAST' | 'MIN' | 'MAX' | 'CONCAT' | 'SUM' | 'AVG' }>;
  sortColumn?: string;  // for FIRST/LAST
}

export class MatchGroupSQLGenerator extends BaseSQLGenerator {
  // Implement all required abstract methods from BaseSQLGenerator

  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    // Safely cast configuration to the expected type
    const config = node.metadata?.configuration?.config as MatchGroupConfig | undefined;

    if (!config || !config.groupingKeys.length) {
      return this.fallbackSelect(node);
    }

    // Use DISTINCT ON with ordering to pick the row with survivorship rules.
    // This is a simplification; full survivorship may require aggregations.
    const distinctCols = config.groupingKeys.map(c => this.sanitizeIdentifier(c)).join(', ');
    const orderBy = config.sortColumn ? `${distinctCols}, ${config.sortColumn}` : distinctCols;

    const sql = `SELECT DISTINCT ON (${distinctCols}) * FROM source_table ORDER BY ${orderBy}`;

    return {
      sql,
      dependencies: ['source_table'],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'match_group', lineCount: 1 }
    };
  }

  // The following clauses are not needed for a simple DISTINCT ON query,
  // so they return empty fragments.
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

  // Private helper to produce a basic SELECT when configuration is missing
  private fallbackSelect(_node: UnifiedCanvasNode): GeneratedSQLFragment {
    // In a real implementation, you might derive the table name from node dependencies.
    // For now, we use a placeholder 'source_table'.
    const sql = `SELECT * FROM source_table`;
    return {
      sql,
      dependencies: ['source_table'],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'fallback_select', lineCount: 1 }
    };
  }

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