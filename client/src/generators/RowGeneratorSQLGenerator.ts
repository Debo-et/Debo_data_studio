// src/generators/RowGeneratorSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';

interface RowGeneratorConfig {
  rowCount: number;
  start?: number;
  step?: number;
  columns?: Array<{ name: string; expression: string }>;
}

export class RowGeneratorSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    // Safe cast: this generator is only created for ROW_GENERATOR nodes
    const config = node.metadata?.configuration?.config as RowGeneratorConfig | undefined;

    const series = `generate_series(${config?.start ?? 1}, ${config?.rowCount ?? 0}, ${config?.step ?? 1}) AS id`;
    const columns = config?.columns?.map(c => `${c.expression} AS ${this.sanitizeIdentifier(c.name)}`).join(', ') || 'id';
    const sql = `SELECT ${columns} FROM ${series}`;

    return {
      sql,
      dependencies: [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'row_generator', lineCount: 1 }
    };
  }

  // Implement required abstract methods with empty fragments
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