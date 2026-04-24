// src/generators/RecordMatchingSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';
import { UnifiedCanvasNode } from '../types/unified-pipeline.types';

interface RecordMatchingConfig {
  leftColumns: string[];
  rightColumns: string[];
  threshold: number;
  method: 'levenshtein' | 'soundex' | 'metaphone' | 'similarity';
}

export class RecordMatchingSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    const config = (node.metadata?.configuration as { config?: RecordMatchingConfig } | undefined)?.config;

    if (!config) {
      return this.fallbackFragment(node, 'No record matching configuration found');
    }

    const leftAlias = 'left_input';
    const rightAlias = 'right_input';
    const similarityCondition = this.buildSimilarityCondition(config, leftAlias, rightAlias);

    const sql = `
SELECT
  ${leftAlias}.*,
  ${rightAlias}.*,
  similarity(${leftAlias}.${this.sanitizeIdentifier(config.leftColumns[0])},
             ${rightAlias}.${this.sanitizeIdentifier(config.rightColumns[0])}) AS similarity_score
FROM ${leftAlias}
CROSS JOIN ${rightAlias}
WHERE ${similarityCondition} > ${config.threshold}`;

    return {
      sql,
      dependencies: [leftAlias, rightAlias],
      parameters: new Map(),
      errors: [],
      warnings: ['Record matching SQL generation is a simplified placeholder'],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'record_matching_select', lineCount: sql.split('\n').length }
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

  private buildSimilarityCondition(config: RecordMatchingConfig, leftAlias: string, rightAlias: string): string {
    const leftCol = this.sanitizeIdentifier(config.leftColumns[0]);
    const rightCol = this.sanitizeIdentifier(config.rightColumns[0]);

    switch (config.method) {
      case 'levenshtein':
        return `levenshtein(${leftAlias}.${leftCol}, ${rightAlias}.${rightCol})`;
      case 'soundex':
        return `soundex(${leftAlias}.${leftCol}) = soundex(${rightAlias}.${rightCol})`;
      case 'metaphone':
        return `metaphone(${leftAlias}.${leftCol}, 4) = metaphone(${rightAlias}.${rightCol}, 4)`;
      case 'similarity':
      default:
        return `similarity(${leftAlias}.${leftCol}, ${rightAlias}.${rightCol})`;
    }
  }

  private fallbackFragment(node: UnifiedCanvasNode, reason: string): GeneratedSQLFragment {
    const sql = `-- Record matching requires valid configuration for node "${node.name}" (${node.id})\n-- Reason: ${reason}`;
    return {
      sql,
      dependencies: [],
      parameters: new Map(),
      errors: [{
        code: 'MISSING_CONFIG',
        message: reason,
        severity: 'ERROR',
        suggestion: 'Provide leftColumns, rightColumns, threshold, and method in the node configuration'
      }],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'record_matching_error', lineCount: sql.split('\n').length }
    };
  }
}