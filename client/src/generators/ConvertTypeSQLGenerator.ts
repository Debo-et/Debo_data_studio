// src/generators/ConvertTypeSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';
import { UnifiedCanvasNode, PostgreSQLDataType } from '../types/unified-pipeline.types';

interface ConvertTypeConfig {
  column: string;
  targetType: PostgreSQLDataType;
  format?: string;           // e.g., for date conversions
  fallbackExpression?: string;
}

// Type guard to check if a config object is a conversion configuration
function isConversionConfig(config: any): config is { conversions: ConvertTypeConfig[] } {
  return config && Array.isArray(config.conversions);
}

export class ConvertTypeSQLGenerator extends BaseSQLGenerator {
  public generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    
    // Safely extract conversions using the type guard
    let configs: ConvertTypeConfig[] = [];
    const rawConfig = node.metadata?.configuration?.config;
    if (isConversionConfig(rawConfig)) {
      configs = rawConfig.conversions;
    }

    if (configs.length === 0) {
      return this.fallbackSelect(node);
    }

    const allColumns = node.metadata?.schemas?.output?.fields || [];
    const selectExpressions = allColumns.map(col => {
      const config = configs.find(c => c.column === col.name);
      if (!config) return this.sanitizeIdentifier(col.name);

      // Build conversion expression
      let expr = this.sanitizeIdentifier(config.column);
      if (config.format) {
        // Use TO_DATE, TO_TIMESTAMP, etc.
        switch (config.targetType) {
          case PostgreSQLDataType.DATE:
            expr = `TO_DATE(${expr}, '${this.escapeString(config.format)}')`;
            break;
          case PostgreSQLDataType.TIMESTAMP:
            expr = `TO_TIMESTAMP(${expr}, '${this.escapeString(config.format)}')`;
            break;
          default:
            expr = `CAST(${expr} AS ${config.targetType})`;
        }
      } else {
        expr = `CAST(${expr} AS ${config.targetType})`;
      }

      // Apply fallback if needed
      if (config.fallbackExpression) {
        expr = `COALESCE(${expr}, ${config.fallbackExpression})`;
      }

      return `${expr} AS ${this.sanitizeIdentifier(config.column)}`;
    });

    const sql = `SELECT ${selectExpressions.join(', ')} FROM source_table`;
    return {
      sql,
      dependencies: ['source_table'],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'convert_type', lineCount: 1 }
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

  private fallbackSelect(node: UnifiedCanvasNode): GeneratedSQLFragment {
    return {
      sql: `SELECT * FROM ${this.sanitizeIdentifier(node.name)}`,
      dependencies: [node.name],
      parameters: new Map(),
      errors: [],
      warnings: ['No conversion rules, using fallback'],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'convert_fallback', lineCount: 1 }
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