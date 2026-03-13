// src/generators/DenormalizeSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';
import { UnifiedCanvasNode, NodeType } from '../types/unified-pipeline.types';

// Define the expected shape of a denormalize configuration
interface DenormalizeConfig {
  groupBy: string[];
  aggregations: Array<{
    column: string;
    function: 'string_agg' | 'json_agg' | 'array_agg';
    delimiter?: string;
    orderBy?: string;
  }>;
}

// Type guard to check if an object is a valid DenormalizeConfig
function isDenormalizeConfig(config: any): config is DenormalizeConfig {
  return (
    config &&
    Array.isArray(config.groupBy) &&
    config.groupBy.every((item: any) => typeof item === 'string') &&
    Array.isArray(config.aggregations) &&
    config.aggregations.every(
      (agg: any) =>
        typeof agg === 'object' &&
        typeof agg.column === 'string' &&
        ['string_agg', 'json_agg', 'array_agg'].includes(agg.function) &&
        (agg.delimiter === undefined || typeof agg.delimiter === 'string') &&
        (agg.orderBy === undefined || typeof agg.orderBy === 'string')
    )
  );
}

export class DenormalizeSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    const config = this.extractDenormalizeConfig(node);

    if (!config) {
      // No valid denormalize configuration – fallback to a simple SELECT
      const table = this.extractTableName(node);
      return {
        sql: `SELECT * FROM ${this.sanitizeIdentifier(table)}`,
        dependencies: [table],
        parameters: new Map(),
        errors: [],
        warnings: ['No denormalize configuration found; using SELECT *'],
        metadata: { generatedAt: new Date().toISOString(), fragmentType: 'select', lineCount: 1 }
      };
    }

    // Build select list: group by columns + aggregated columns
    const groupByCols = config.groupBy.map(c => this.sanitizeIdentifier(c));
    const aggCols = config.aggregations.map(agg => {
      let expr: string;
      if (agg.function === 'string_agg') {
        expr = `string_agg(${this.sanitizeIdentifier(agg.column)}, '${agg.delimiter || ','}'${
          agg.orderBy ? ` ORDER BY ${agg.orderBy}` : ''
        })`;
      } else if (agg.function === 'json_agg') {
        expr = `json_agg(${this.sanitizeIdentifier(agg.column)}${
          agg.orderBy ? ` ORDER BY ${agg.orderBy}` : ''
        })`;
      } else {
        expr = `array_agg(${this.sanitizeIdentifier(agg.column)}${
          agg.orderBy ? ` ORDER BY ${agg.orderBy}` : ''
        })`;
      }
      const alias = this.sanitizeIdentifier(agg.column + '_agg');
      return `${expr} AS ${alias}`;
    });

    const selectList = [...groupByCols, ...aggCols].join(', ');
    const sql = `SELECT ${selectList} FROM source_table`;

    return {
      sql,
      dependencies: ['source_table'],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'select', lineCount: 1 }
    };
  }

  protected generateGroupByClause(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    const config = this.extractDenormalizeConfig(node);

    if (!config || !config.groupBy.length) {
      return this.emptyFragment();
    }

    const groupByCols = config.groupBy.map(c => this.sanitizeIdentifier(c)).join(', ');
    const sql = `GROUP BY ${groupByCols}`;

    return {
      sql,
      dependencies: [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'group_by', lineCount: 1 }
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

  // Helper to extract and validate denormalize configuration from a node
  private extractDenormalizeConfig(node: UnifiedCanvasNode): DenormalizeConfig | null {
    // Ensure node is of the expected type (optional, but good practice)
    if (node.type !== NodeType.DENORMALIZE) {
      return null;
    }

    const configuration = node.metadata?.configuration;
    if (!configuration) return null;

    // Denormalize config should be stored under the 'OTHER' type
    if (configuration.type !== 'OTHER') return null;

    // Now configuration.config is a Record<string, any> – check if it matches our interface
    const candidate = configuration.config;
    if (isDenormalizeConfig(candidate)) {
      return candidate;
    }

    return null;
  }

  private extractTableName(node: UnifiedCanvasNode): string {
    // You can enhance this to read from input schema or node name
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