// src/generators/PivotSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';
import { UnifiedCanvasNode } from '../types/unified-pipeline.types';

interface PivotConfig {
  rowIdentifier: string;   // column that identifies rows
  pivotColumn: string;     // column whose values become new columns
  valueColumn: string;     // column to aggregate
  aggregateFunction: 'SUM' | 'AVG' | 'COUNT' | 'MIN' | 'MAX';
}

export class PivotSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    
    // Safely extract config with type guard
    const config = this.extractPivotConfig(node);
    if (!config) {
      return this.fallbackSelect(node);
    }

    // This requires crosstab. We'll generate a query that uses the tablefunc extension.
    const sql = `SELECT * FROM crosstab(
      'SELECT ${config.rowIdentifier}, ${config.pivotColumn}, ${config.valueColumn}
       FROM source_table
       ORDER BY 1,2',
      'SELECT DISTINCT ${config.pivotColumn} FROM source_table ORDER BY 1'
    ) AS ct(rowid ${this.getDataType(config.rowIdentifier)}, ${this.generatePivotColumns(config)});`;

    return {
      sql,
      dependencies: ['source_table'],
      parameters: new Map(),
      errors: [],
      warnings: ['Requires tablefunc extension', 'Pivot column types are inferred as text; adjust as needed'],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'pivot', lineCount: sql.split('\n').length }
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

  private generatePivotColumns(_config: PivotConfig): string {
    // Need to know distinct pivot values; this is a placeholder.
    // In practice, we'd need to fetch distinct values or have them configured.
    return 'col1 text, col2 text'; // placeholder
  }

  private getDataType(_column: string): string {
    // Infer from schema – currently returns 'text' as a safe default.
    // TODO: Use actual schema information to determine data type.
    return 'text';
  }

  private extractPivotConfig(node: UnifiedCanvasNode): PivotConfig | null {
    // Access the configuration safely; assuming the node type is PIVOT_TO_COLUMNS_DELIMITED
    const config = node.metadata?.configuration?.config;
    if (config && this.isValidPivotConfig(config)) {
      return config as PivotConfig;
    }
    return null;
  }

  private isValidPivotConfig(obj: any): obj is PivotConfig {
    return obj &&
      typeof obj.rowIdentifier === 'string' &&
      typeof obj.pivotColumn === 'string' &&
      typeof obj.valueColumn === 'string' &&
      ['SUM', 'AVG', 'COUNT', 'MIN', 'MAX'].includes(obj.aggregateFunction);
  }

  private fallbackSelect(node: UnifiedCanvasNode): GeneratedSQLFragment {
    const tableName = this.extractTableName(node);
    return {
      sql: `SELECT * FROM ${this.sanitizeIdentifier(tableName)}`,
      dependencies: [tableName],
      parameters: new Map(),
      errors: [{
        code: 'MISSING_PIVOT_CONFIG',
        message: 'Pivot configuration not found or invalid, using fallback SELECT *',
        severity: 'ERROR'
      }],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'fallback_select', lineCount: 1 }
    };
  }

  private extractTableName(node: UnifiedCanvasNode): string {
    // TODO: Replace with extraction from unified configuration (e.g., input node table name).
    return node.name.toLowerCase().replace(/\s+/g, '_');
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