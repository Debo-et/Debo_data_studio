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
  // Implement all required abstract methods
  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    // Safely access the configuration – assume it's present and cast
    const config = (node.metadata?.configuration as { config?: RecordMatchingConfig } | undefined)?.config;

    if (!config) {
      return this.fallbackFragment(node, 'No record matching configuration found');
    }

    // For demonstration, generate a similarity join placeholder
    // In a real implementation you would use config.leftColumns, config.rightColumns, etc.
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
      dependencies: [leftAlias, rightAlias], // These would be actual table names in a real implementation
      parameters: new Map(),
      errors: [],
      warnings: ['Record matching SQL generation is a simplified placeholder'],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'record_matching_select', lineCount: sql.split('\n').length }
    };
  }

  protected generateJoinConditions(_context: SQLGenerationContext): GeneratedSQLFragment {
    // Record matching typically uses a CROSS JOIN with similarity WHERE clause,
    // so join conditions can be empty.
    return this.emptyFragment();
  }

  protected generateWhereClause(_context: SQLGenerationContext): GeneratedSQLFragment {
    // The similarity condition is placed in the WHERE clause by generateSelectStatement,
    // so we return empty here to avoid duplication.
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

  // Helper to build a similarity condition based on the method
  private buildSimilarityCondition(config: RecordMatchingConfig, leftAlias: string, rightAlias: string): string {
    // For simplicity, we only use the first column pair.
    // A real implementation would handle multiple columns and different methods.
    const leftCol = this.sanitizeIdentifier(config.leftColumns[0]);
    const rightCol = this.sanitizeIdentifier(config.rightColumns[0]);

    switch (config.method) {
      case 'levenshtein':
        return `levenshtein(${leftAlias}.${leftCol}, ${rightAlias}.${rightCol})`;
      case 'soundex':
        return `soundex(${leftAlias}.${leftCol}) = soundex(${rightAlias}.${rightCol})`;
      case 'metaphone':
        // metaphone() is not a standard PostgreSQL function; you'd need an extension.
        return `metaphone(${leftAlias}.${leftCol}, 4) = metaphone(${rightAlias}.${rightCol}, 4)`;
      case 'similarity':
      default:
        // similarity() is from pg_trgm
        return `similarity(${leftAlias}.${leftCol}, ${rightAlias}.${rightCol})`;
    }
  }

  // Fallback fragment when configuration is missing
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