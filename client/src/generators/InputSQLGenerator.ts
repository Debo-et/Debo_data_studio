// src/generators/InputSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';
import { UnifiedCanvasNode, isInputConfig, InputComponentConfiguration } from '../types/unified-pipeline.types';

export class InputSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    const config = this.getInputConfig(node);
    if (!config) return this.fallbackSelect(node);

    // 1. Try configuration's sourceDetails.tableName
    let tableName = config.sourceDetails.tableName;

    // 2. If missing, look for postgresTableName in metadata
    if (!tableName) {
      tableName = node.metadata?.postgresTableName ||
                  node.metadata?.fullRepositoryMetadata?.postgresTableName;
    }

    // 3. Final fallback to node.name
    if (!tableName) {
      tableName = node.name;
    }

    const schema = node.metadata?.schemas?.output;

    // Build column list from schema or fallback to '*'
    let selectClause = '*';
    if (schema?.fields?.length) {
      const columns = schema.fields.map(f => this.sanitizeIdentifier(f.name)).join(', ');
      selectClause = columns;
    }

    // Apply column selection pushdown if enabled
    if (config.pushdown.enabled && config.pushdown.columnSelection?.length) {
      const selectedColumns = config.pushdown.columnSelection
        .map(col => this.sanitizeIdentifier(col))
        .join(', ');
      selectClause = selectedColumns;
    }

    const fromClause = this.sanitizeIdentifier(tableName);
    const alias = config.sqlGeneration.alias ? ` AS ${this.sanitizeIdentifier(config.sqlGeneration.alias)}` : '';

    let sql = `SELECT ${selectClause} FROM ${fromClause}${alias}`;

    // Apply filter pushdown
    if (config.pushdown.enabled && config.pushdown.filterClause) {
      sql += `\nWHERE ${config.pushdown.filterClause}`;
    }

    // Apply limit pushdown
    if (config.pushdown.enabled && config.pushdown.limit) {
      sql += `\nLIMIT ${config.pushdown.limit}`;
    }

    return {
      sql,
      dependencies: [tableName],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'input_select',
        lineCount: sql.split('\n').length
      }
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

  private getInputConfig(node: UnifiedCanvasNode): InputComponentConfiguration | undefined {
    if (!node.metadata?.configuration) return undefined;
    const conf = node.metadata.configuration;
    return isInputConfig(conf) ? conf.config : undefined;
  }

  private fallbackSelect(node: UnifiedCanvasNode): GeneratedSQLFragment {
    // Fallback to node.name if configuration is missing
    const tableName = node.metadata?.postgresTableName ||
                      node.metadata?.fullRepositoryMetadata?.postgresTableName ||
                      node.name;
    return {
      sql: `SELECT * FROM ${this.sanitizeIdentifier(tableName)}`,
      dependencies: [tableName],
      parameters: new Map(),
      errors: [],
      warnings: ['No input configuration found, using fallback'],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'input_fallback', lineCount: 1 }
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