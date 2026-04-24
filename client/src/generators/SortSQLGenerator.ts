// src/generators/SortSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';
import { UnifiedCanvasNode, isSortConfig, SortComponentConfiguration } from '../types/unified-pipeline.types';

/**
 * PostgreSQL SORT SQL Generator
 * Handles ORDER BY clauses with NULLS FIRST/LAST and LIMIT/OFFSET
 */
export class SortSQLGenerator extends BaseSQLGenerator {
protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
  const { connection } = context;
  let sourceRef = 'source_table';
  if (connection && connection.sourceNodeId) {
    sourceRef = this.sanitizeIdentifier(connection.sourceNodeId);
  }
  return {
    sql: `SELECT * FROM ${sourceRef}`,
    dependencies: connection ? [connection.sourceNodeId] : [],
    parameters: new Map(),
    errors: [],
    warnings: [],
    metadata: { generatedAt: new Date().toISOString(), fragmentType: 'sort_select', lineCount: 1 }
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

  protected generateOrderByClause(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    const config = this.getSortConfig(node);

    if (!config || config.sortFields.length === 0) {
      return this.emptyFragment();
    }

    // Build ORDER BY clause
    const orderByClauses = config.sortFields.map(field => {
      const parts = [this.sanitizeIdentifier(field.field), field.direction];
      if (field.nullsFirst !== undefined) {
        parts.push(field.nullsFirst ? 'NULLS FIRST' : 'NULLS LAST');
      }
      return parts.join(' ');
    });

    let sql = `ORDER BY ${orderByClauses.join(', ')}`;

    // Add LIMIT / OFFSET if present
    if (config.sqlGeneration.limitOffset) {
      if (config.sqlGeneration.limitOffset.limit !== undefined) {
        sql += `\nLIMIT ${config.sqlGeneration.limitOffset.limit}`;
      }
      if (config.sqlGeneration.limitOffset.offset !== undefined) {
        sql += `\nOFFSET ${config.sqlGeneration.limitOffset.offset}`;
      }
    }

    return {
      sql,
      dependencies: [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'order_by', lineCount: sql.split('\n').length }
    };
  }

  protected generateGroupByClause(_context: SQLGenerationContext): GeneratedSQLFragment {
    return this.emptyFragment();
  }

  /**
   * Extracts SortComponentConfiguration from node metadata.
   */
private getSortConfig(node: UnifiedCanvasNode): SortComponentConfiguration | undefined {
  // Legacy direct metadata (used by test helpers)
  if (node.metadata?.sortConfig) {
    return {
      sortFields: (node.metadata.sortConfig.columns || []).map((col: any) => ({
        field: col.column,
        direction: col.direction,
        nullsFirst: col.nullsFirst,
      })),
      sqlGeneration: {
        limitOffset: node.metadata.sortConfig.limit
          ? { limit: node.metadata.sortConfig.limit, offset: node.metadata.sortConfig.offset }
          : undefined,
      },
    } as SortComponentConfiguration;
  }

  // Unified component configuration
  if (!node.metadata?.configuration) return undefined;
  const conf = node.metadata.configuration;
  return isSortConfig(conf) ? conf.config : undefined;
}
}