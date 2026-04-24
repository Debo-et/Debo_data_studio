// src/generators/UnpivotSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';
import { UnifiedCanvasNode } from '../types/unified-pipeline.types';

interface UnpivotConfig {
  identifierColumns: string[];      // columns that stay as row identifiers
  valueColumns: string[];           // columns to be converted into rows
  variableColumnName: string;       // name of the new column that holds the original column names
  valueColumnName: string;          // name of the new column that holds the values
}

export class UnpivotSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    const config = this.extractUnpivotConfig(node);

    if (!config) {
      return this.fallbackSelect(node);
    }

    const { identifierColumns, valueColumns, variableColumnName, valueColumnName } = config;

    // Use CROSS JOIN LATERAL with VALUES to unpivot
    const idCols = identifierColumns.map(c => this.sanitizeIdentifier(c)).join(', ');

    // Build the VALUES list for unpivoted pairs: (column_name, column_value)
    const valuePairs = valueColumns
      .map(col => `('${this.escapeString(col)}', ${this.sanitizeIdentifier(col)}::text)`)
      .join(', ');

    const lateralClause = `LATERAL (VALUES ${valuePairs}) AS unpivot(${this.sanitizeIdentifier(variableColumnName)}, ${this.sanitizeIdentifier(valueColumnName)})`;

    const sql = `SELECT ${idCols}, unpivot.* FROM source_table, ${lateralClause}`;

    return {
      sql,
      dependencies: ['source_table'],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'unpivot',
        lineCount: sql.split('\n').length,
      },
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

  private extractUnpivotConfig(node: UnifiedCanvasNode): UnpivotConfig | null {
    const config = node.metadata?.configuration?.config;
    if (!config || typeof config !== 'object') {
      return null;
    }

    const candidate = config as any;
    if (
      Array.isArray(candidate.identifierColumns) &&
      Array.isArray(candidate.valueColumns) &&
      typeof candidate.variableColumnName === 'string' &&
      typeof candidate.valueColumnName === 'string'
    ) {
      return {
        identifierColumns: candidate.identifierColumns,
        valueColumns: candidate.valueColumns,
        variableColumnName: candidate.variableColumnName,
        valueColumnName: candidate.valueColumnName,
      };
    }
    return null;
  }

  private fallbackSelect(node: UnifiedCanvasNode): GeneratedSQLFragment {
    const tableName = node.name.toLowerCase().replace(/\s+/g, '_');
    return {
      sql: `SELECT * FROM ${this.sanitizeIdentifier(tableName)}`,
      dependencies: [tableName],
      parameters: new Map(),
      errors: [{
        code: 'MISSING_UNPIVOT_CONFIG',
        message: 'Unpivot configuration not found or invalid, using fallback SELECT *',
        severity: 'ERROR'
      }],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'fallback_select', lineCount: 1 }
    };
  }
}