// src/generators/NormalizeNumberSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';

interface NormalizeNumberConfig {
  sourceColumn: string;
  targetType?: 'INTEGER' | 'DECIMAL' | 'NUMERIC' | 'FLOAT' | 'DOUBLE PRECISION';
  min?: number;
  max?: number;
  targetMin?: number;
  targetMax?: number;
}

export class NormalizeNumberSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node, connection, upstreamSchema } = context;

    // Access config from the correct metadata path
    const config = (node.metadata as any)?.normalizeNumberConfig as NormalizeNumberConfig | undefined;

    // If no config or missing required source column, fallback to a simple SELECT *
    if (!config || !config.sourceColumn) {
      return this.fallbackSelect(context);
    }

    const sourceColumn = config.sourceColumn;
    const targetType = config.targetType;
    const hasScaling = config.min !== undefined && config.max !== undefined;

    // Determine the source reference (table, CTE, or subquery alias)
    const sourceRef = connection?.sourceNodeId
      ? this.sanitizeIdentifier(connection.sourceNodeId)
      : 'source_table'; // fallback, should rarely happen

    // Build column list from upstream schema, transforming the target column
    const selectParts: string[] = [];
    const allColumns = upstreamSchema || [];

    for (const col of allColumns) {
      const colName = col.name;
      const sanitized = this.sanitizeIdentifier(colName);

      if (colName === sourceColumn) {
        let expr: string;
        if (hasScaling) {
          // Min-max scaling
          const min = config.min!;
          const max = config.max!;
          const targetMin = config.targetMin ?? 0;
          const targetMax = config.targetMax ?? 1;
          expr = `((${sanitized} - ${min})::float / (${max} - ${min}) * (${targetMax} - ${targetMin}) + ${targetMin})`;
          // Optionally cast to targetType if specified
          if (targetType) {
            expr = `CAST(${expr} AS ${targetType})`;
          }
        } else if (targetType) {
          // Simple type cast
          expr = `CAST(${sanitized} AS ${targetType})`;
        } else {
          // No transformation – keep as is
          expr = sanitized;
        }
        selectParts.push(`${expr} AS ${sanitized}`);
      } else {
        selectParts.push(sanitized);
      }
    }

    // If upstream schema is empty, fallback to SELECT *
    const selectClause = selectParts.length > 0 ? selectParts.join(', ') : '*';
    const sql = `SELECT ${selectClause} FROM ${sourceRef}`;

    return {
      sql,
      dependencies: connection ? [connection.sourceNodeId] : [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'normalize_number',
        lineCount: sql.split('\n').length,
      },
    };
  }

  // Implement required abstract methods (unused)
  protected generateJoinConditions(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateWhereClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateHavingClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateOrderByClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateGroupByClause(): GeneratedSQLFragment { return this.emptyFragment(); }

  private fallbackSelect(context: SQLGenerationContext): GeneratedSQLFragment {
    const sourceRef = context.connection?.sourceNodeId
      ? this.sanitizeIdentifier(context.connection.sourceNodeId)
      : 'source_table';
    const sql = `SELECT * FROM ${sourceRef}`;
    return {
      sql,
      dependencies: context.connection ? [context.connection.sourceNodeId] : [],
      parameters: new Map(),
      errors: [],
      warnings: ['NormalizeNumber configuration missing; using SELECT *'],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'fallback_select', lineCount: 1 },
    };
  }
}