// src/generators/OutputSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';
import { UnifiedCanvasNode, isOutputConfig, OutputComponentConfiguration } from '../types/unified-pipeline.types';

export class OutputSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(_context: SQLGenerationContext): GeneratedSQLFragment {
    // Output nodes don't generate SELECT; they generate INSERT
    return this.emptyFragment();
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

  /**
   * Generate the full INSERT statement. This is called by the pipeline when this node is the final sink.
   */
  public generateInsertSQL(
    sourceSQL: string,
    node: UnifiedCanvasNode,
    _mappings: any[] // schema mappings from upstream connection
  ): GeneratedSQLFragment {
    const config = this.getOutputConfig(node);
    if (!config) {
      return {
        sql: '',
        dependencies: [],
        parameters: new Map(),
        errors: [{ code: 'NO_OUTPUT_CONFIG', message: 'Output node missing configuration', severity: 'ERROR' }],
        warnings: [],
        metadata: { generatedAt: new Date().toISOString(), fragmentType: 'output_error', lineCount: 0 }
      };
    }

    const targetTable = config.targetDetails.tableName;
    const targetColumns = config.schemaMapping.map(m => m.targetColumn);
    const columnList = targetColumns.map(c => this.sanitizeIdentifier(c)).join(', ');

    let sql = `INSERT INTO ${this.sanitizeIdentifier(targetTable)} (${columnList})\n`;

    // The sourceSQL should be a SELECT statement from the previous node
    sql += sourceSQL;

    // Add ON CONFLICT if specified
    if (config.writeOptions.createTable && config.targetDetails.mode === 'APPEND') {
      // no conflict handling
    } else if (config.targetDetails.mode === 'OVERWRITE') {
      sql = `TRUNCATE ${this.sanitizeIdentifier(targetTable)};\n` + sql;
    }

    return {
      sql,
      dependencies: [targetTable],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'output_insert',
        lineCount: sql.split('\n').length
      }
    };
  }

  private getOutputConfig(node: UnifiedCanvasNode): OutputComponentConfiguration | undefined {
    if (!node.metadata?.configuration) return undefined;
    const conf = node.metadata.configuration;
    return isOutputConfig(conf) ? conf.config : undefined;
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