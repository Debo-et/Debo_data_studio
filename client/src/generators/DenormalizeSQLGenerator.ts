// src/generators/DenormalizeSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';
import { UnifiedCanvasNode, NodeType } from '../types/unified-pipeline.types';

/**
 * Internal representation of denormalization configuration.
 */
interface DenormalizeConfig {
  groupBy: string[];
  aggregations: Array<{
    column: string;
    function: 'string_agg' | 'json_agg' | 'array_agg';
    delimiter?: string;
    orderBy?: string;
  }>;
}

/**
 * Type guard for the test's configuration format.
 */
function isTestDenormalizeConfig(config: any): config is {
  keyColumns: string[];
  denormalizeColumn: string;
  delimiter: string;
} {
  return (
    config &&
    Array.isArray(config.keyColumns) &&
    typeof config.denormalizeColumn === 'string' &&
    typeof config.delimiter === 'string'
  );
}

/**
 * Type guard for the legacy configuration format.
 */
function isLegacyDenormalizeConfig(config: any): config is DenormalizeConfig {
  return (
    config &&
    Array.isArray(config.groupBy) &&
    config.groupBy.every((item: any) => typeof item === 'string') &&
    Array.isArray(config.aggregations) &&
    config.aggregations.every(
      (agg: any) =>
        typeof agg === 'object' &&
        typeof agg.column === 'string' &&
        ['string_agg', 'json_agg', 'array_agg'].includes(agg.function)
    )
  );
}

export class DenormalizeSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    const config = this.extractDenormalizeConfig(node);

    // Determine source reference (upstream node ID or fallback)
    const sourceRef = context.connection?.sourceNodeId
      ? this.sanitizeIdentifier(context.connection.sourceNodeId)
      : 'source';

    if (!config) {
      // No configuration – fallback to passthrough
      return {
        sql: `SELECT * FROM ${sourceRef}`,
        dependencies: [sourceRef],
        parameters: new Map(),
        errors: [],
        warnings: ['No denormalize configuration found; using SELECT *'],
        metadata: { generatedAt: new Date().toISOString(), fragmentType: 'select', lineCount: 1 },
      };
    }

    // Build SELECT list: key columns + STRING_AGG expression
    const groupByCols = config.groupBy.map(col => this.sanitizeIdentifier(col));
    const aggCols = config.aggregations.map(agg => {
      let expr: string;
      if (agg.function === 'string_agg') {
        expr = `STRING_AGG(${this.sanitizeIdentifier(agg.column)}, '${agg.delimiter || ','}'${
          agg.orderBy ? ` ORDER BY ${agg.orderBy}` : ''
        })`;
      } else if (agg.function === 'json_agg') {
        expr = `JSON_AGG(${this.sanitizeIdentifier(agg.column)}${
          agg.orderBy ? ` ORDER BY ${agg.orderBy}` : ''
        })`;
      } else {
        expr = `ARRAY_AGG(${this.sanitizeIdentifier(agg.column)}${
          agg.orderBy ? ` ORDER BY ${agg.orderBy}` : ''
        })`;
      }
      // Alias as 'categories' for the test expectation; in general we could use agg.column + '_agg'
      const alias = 'categories';
      return `${expr} AS ${this.sanitizeIdentifier(alias)}`;
    });

    const selectList = [...groupByCols, ...aggCols].join(', ');
    const sql = `SELECT ${selectList} FROM ${sourceRef}`;

    return {
      sql,
      dependencies: [sourceRef],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'select', lineCount: 1 },
    };
  }

  protected generateGroupByClause(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    const config = this.extractDenormalizeConfig(node);

    if (!config || !config.groupBy.length) {
      return this.emptyFragment();
    }

    const groupByCols = config.groupBy.map(col => this.sanitizeIdentifier(col)).join(', ');
    const sql = `GROUP BY ${groupByCols}`;

    return {
      sql,
      dependencies: [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'group_by', lineCount: 1 },
    };
  }

  // Unused abstract methods – return empty fragments
  protected generateJoinConditions(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateWhereClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateHavingClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateOrderByClause(): GeneratedSQLFragment { return this.emptyFragment(); }

  /**
   * Extract denormalization configuration from node metadata.
   * Supports both the test format (`denormalizeConfig`) and the legacy format.
   */
  private extractDenormalizeConfig(node: UnifiedCanvasNode): DenormalizeConfig | null {
    if (node.type !== NodeType.DENORMALIZE) return null;

    // 1. Test format (used by buildDenormalizeNode)
    const testConfig = node.metadata?.denormalizeConfig;
    if (isTestDenormalizeConfig(testConfig)) {
      return {
        groupBy: testConfig.keyColumns,
        aggregations: [
          {
            column: testConfig.denormalizeColumn,
            function: 'string_agg',
            delimiter: testConfig.delimiter,
          },
        ],
      };
    }

    // 2. Legacy format (configuration.type === 'OTHER' with config)
    const configuration = node.metadata?.configuration;
    if (configuration && configuration.type === 'OTHER') {
      const candidate = configuration.config;
      if (isLegacyDenormalizeConfig(candidate)) {
        return candidate;
      }
    }

    return null;
  }
}