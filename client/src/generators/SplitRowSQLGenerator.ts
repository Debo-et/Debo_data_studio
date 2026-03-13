// src/generators/SplitRowSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment, SQLGenerationError } from './BaseSQLGenerator';
import { UnifiedCanvasNode } from '../types/unified-pipeline.types';

interface SplitRowConfig {
  sourceColumn: string;
  delimiter: string;
  targetColumn: string;
}

export class SplitRowSQLGenerator extends BaseSQLGenerator {
  // ==================== Abstract Method Implementations ====================
  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    const config = node.metadata?.configuration?.config as SplitRowConfig | undefined;

    // Validate configuration
    if (!config?.sourceColumn || !config?.delimiter || !config?.targetColumn) {
      return this.fallbackSelect(node, 'Missing required split row configuration (sourceColumn, delimiter, targetColumn)');
    }

    const source = this.sanitizeIdentifier(config.sourceColumn);
    const target = this.sanitizeIdentifier(config.targetColumn);
    
    // Get all other columns from the output schema (if available)
    const otherColumns = node.metadata?.schemas?.output?.fields
      ?.filter(c => c.name !== config.sourceColumn)
      .map(c => this.sanitizeIdentifier(c.name)) || [];

    // Build the SELECT list: all other columns + the split values
    const selectList = otherColumns.length > 0 
      ? `${otherColumns.join(', ')}, ` 
      : '';

    // Use regexp_split_to_table to explode the delimited values into rows
    const sql = `SELECT ${selectList}regexp_split_to_table(${source}, '${this.escapeString(config.delimiter)}') AS ${target} FROM source_table`;

    return {
      sql,
      dependencies: ['source_table'],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'split_row_select',
        lineCount: sql.split('\n').length,
      },
    };
  }

  protected generateJoinConditions(_context: SQLGenerationContext): GeneratedSQLFragment {
    // Split row transformation does not introduce joins
    return this.emptyFragment();
  }

  protected generateWhereClause(_context: SQLGenerationContext): GeneratedSQLFragment {
    // No filtering applied by this node
    return this.emptyFragment();
  }

  protected generateHavingClause(_context: SQLGenerationContext): GeneratedSQLFragment {
    // No HAVING clause applicable
    return this.emptyFragment();
  }

  protected generateOrderByClause(_context: SQLGenerationContext): GeneratedSQLFragment {
    // Ordering is not part of split row logic
    return this.emptyFragment();
  }

  protected generateGroupByClause(_context: SQLGenerationContext): GeneratedSQLFragment {
    // No grouping introduced
    return this.emptyFragment();
  }

  // ==================== Helper Methods ====================
  /**
   * Creates an empty SQL fragment (used for clauses that are not applicable).
   */
  private emptyFragment(): GeneratedSQLFragment {
    return {
      sql: '',
      dependencies: [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'empty',
        lineCount: 0,
      },
    };
  }

  /**
   * Fallback select statement when configuration is invalid.
   * Generates a comment with the error and a minimal valid SELECT.
   */
  private fallbackSelect(_node: UnifiedCanvasNode, reason: string): GeneratedSQLFragment {
    const error: SQLGenerationError = {
      code: 'INVALID_SPLIT_ROW_CONFIG',
      message: reason,
      severity: 'ERROR',
      suggestion: 'Ensure sourceColumn, delimiter, and targetColumn are defined in node.metadata.configuration.config',
    };

    // Provide a minimal valid SQL (comment + SELECT *) to keep pipeline executable
    const sql = `-- ERROR: ${reason}\nSELECT * FROM source_table;`;

    return {
      sql,
      dependencies: ['source_table'],
      parameters: new Map(),
      errors: [error],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'split_row_fallback',
        lineCount: sql.split('\n').length,
      },
    };
  }
}