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

    // 1. Try configuration's targetDetails.tableName
    let targetTable = config.targetDetails.tableName;

    // 2. If missing, look for postgresTableName in metadata
    if (!targetTable) {
      targetTable = node.metadata?.postgresTableName ||
                    node.metadata?.fullRepositoryMetadata?.postgresTableName;
    }

    // 3. Final fallback to node.name (sanitized)
    if (!targetTable) {
      targetTable = node.name;
    }

    const targetColumns = config.schemaMapping.map(m => m.targetColumn);
    const columnList = targetColumns.map(c => this.sanitizeIdentifier(c)).join(', ');

    let sql = `INSERT INTO ${this.sanitizeIdentifier(targetTable)} (${columnList})\n`;
    sql += sourceSQL;

    // Add truncate if overwrite mode
    if (config.targetDetails.mode === 'OVERWRITE') {
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