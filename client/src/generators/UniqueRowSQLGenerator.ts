// src/generators/UniqueRowSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';

interface UniqueConfig {
  columns?: string[];        // columns to consider for uniqueness; if empty, use all
  keep: 'first' | 'last';    // which row to keep
}

export class UniqueRowSQLGenerator extends BaseSQLGenerator {
  // Implement all required abstract methods (most return empty fragments)
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
    // ORDER BY is already included in generateSelectStatement for DISTINCT ON
    return this.emptyFragment();
  }

  protected generateGroupByClause(_context: SQLGenerationContext): GeneratedSQLFragment {
    return this.emptyFragment();
  }

  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;

    // Safely extract the unique configuration
    const config = this.extractUniqueConfig(node);

    // TODO: Replace hardcoded 'source_table' with the actual input source
    // (e.g., from the upstream node's CTE name or dependency)
    let sql: string;
    if (config?.columns?.length) {
      const cols = config.columns.map(c => this.sanitizeIdentifier(c)).join(', ');
      // ORDER BY is required for DISTINCT ON; we assume a default ordering column exists
      sql = `SELECT DISTINCT ON (${cols}) * FROM source_table ORDER BY ${cols}, id ${config.keep === 'last' ? 'DESC' : 'ASC'}`;
    } else {
      sql = `SELECT DISTINCT * FROM source_table`;
    }

    return {
      sql,
      dependencies: ['source_table'], // This should eventually be the upstream CTE/table name
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'unique_row', lineCount: 1 }
    };
  }

  /**
   * Helper to extract UniqueConfig from node metadata.
   * Returns a default config if none is found.
   */
  private extractUniqueConfig(node: any): UniqueConfig {
    const configuration = node.metadata?.configuration;
    if (!configuration) {
      // Default config when nothing is provided
      return { keep: 'first' };
    }

    // For NodeType.UNIQ_ROW, we expect the config to be stored under the 'config' property
    // of an 'OTHER' type configuration. If not, fallback to default.
    if (configuration.type === 'OTHER' && configuration.config) {
      const rawConfig = configuration.config;
      // Ensure the required fields exist
      return {
        columns: rawConfig.columns || [],
        keep: rawConfig.keep === 'last' ? 'last' : 'first',
      };
    }

    // If the node has a top-level config (legacy), try that too
    if (node.metadata?.config) {
      const rawConfig = node.metadata.config;
      return {
        columns: rawConfig.columns || [],
        keep: rawConfig.keep === 'last' ? 'last' : 'first',
      };
    }

    // Default fallback
    return { keep: 'first' };
  }

  /**
   * Returns an empty SQL fragment (used for unused clauses).
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