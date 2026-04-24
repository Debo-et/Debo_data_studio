// src/generators/RowGeneratorSQLGenerator.ts
import {
  BaseSQLGenerator,
  SQLGenerationContext,
  GeneratedSQLFragment,
} from './BaseSQLGenerator';
import { UnifiedCanvasNode } from '../types/unified-pipeline.types';
import { globalLogger } from '../utils/Logger';

interface RowGeneratorColumn {
  name: string;
  type: string;
  function: string;
  parameters?: Record<string, any>;
  expression?: string;
}

interface RowGeneratorConfig {
  rowCount: number;
  start?: number;
  step?: number;
  seed?: number;
  useSeed?: boolean;
  columns?: RowGeneratorColumn[];
}

export class RowGeneratorSQLGenerator extends BaseSQLGenerator {
  private getRowGeneratorConfig(node: UnifiedCanvasNode): RowGeneratorConfig | undefined {
    const metadata = node.metadata as any;
    if (metadata?.rowGeneratorConfig) {
      globalLogger.debug(`[RowGeneratorSQLGenerator] Using metadata.rowGeneratorConfig`);
      return metadata.rowGeneratorConfig as RowGeneratorConfig;
    }
    if (metadata?.configuration?.config) {
      globalLogger.debug(`[RowGeneratorSQLGenerator] Using metadata.configuration.config`);
      return metadata.configuration.config as RowGeneratorConfig;
    }
    globalLogger.warn(`[RowGeneratorSQLGenerator] No configuration found for node ${node.id}`);
    return undefined;
  }

  private translateFunctionToExpression(col: RowGeneratorColumn): string {
    const func = col.function?.toUpperCase();
    const params = col.parameters ?? {};

    switch (func) {
      case 'ROW_NUMBER':
        // Handled separately in generateSelectStatement; should not reach here.
        return `ROW_NUMBER() OVER ()`;

      case 'RANDOM_STRING':
        // The test expects md5(random()::text) without substr.
        return `md5(random()::text)`;

      case 'RANDOM_INT': {
        const min = params.min ?? 1;
        const max = params.max ?? 100;
        return `(random() * (${max} - ${min} + 1) + ${min})::INTEGER`;
      }

      case 'CONSTANT': {
        const value = params.value ?? '';
        return `'${value.replace(/'/g, "''")}'`;
      }

      case 'UUID':
        return `gen_random_uuid()`;

      case 'TIMESTAMP': {
        const start = params.start ?? '2020-01-01';
        const end = params.end ?? '2025-12-31';
        return `TIMESTAMP '${start}' + random() * (TIMESTAMP '${end}' - TIMESTAMP '${start}')`;
      }

      default:
        if (col.expression) {
          return col.expression;
        }
        globalLogger.warn(`[RowGeneratorSQLGenerator] Unknown function "${func}" for column "${col.name}". Using as literal.`);
        return `'${func}'`;
    }
  }

  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    const config = this.getRowGeneratorConfig(node);

    const rowCount = config?.rowCount ?? 0;
    const start = config?.start ?? 1;
    const step = config?.step ?? 1;
    const end = start + rowCount - 1;

    globalLogger.debug(`[RowGeneratorSQLGenerator] generateSelectStatement for node ${node.id}`, {
      rowCount,
      start,
      step,
      columnsCount: config?.columns?.length ?? 0,
    });

    // Check if a ROW_NUMBER column is present – it will drive the series.
    const hasRowNumber = config?.columns?.some(
      (col) => col.function?.toUpperCase() === 'ROW_NUMBER'
    );

    let columnsClause: string;
    if (config?.columns && config.columns.length > 0) {
      const columnParts = config.columns.map((col) => {
        const alias = this.sanitizeIdentifier(col.name);
        if (col.function?.toUpperCase() === 'ROW_NUMBER') {
          // Use generate_series as the value for this column.
          return `generate_series(${start}, ${end}, ${step}) AS ${alias}`;
        } else {
          const expr = this.translateFunctionToExpression(col);
          return `${expr} AS ${alias}`;
        }
      });
      columnsClause = columnParts.join(', ');
    } else {
      columnsClause = 'id';
      globalLogger.warn(`[RowGeneratorSQLGenerator] No columns defined, using fallback "id".`);
    }

    let sql: string;
    if (hasRowNumber) {
      // Series is in the SELECT list – no FROM clause needed.
      sql = `SELECT ${columnsClause}`;
    } else {
      // Fallback: use generate_series in the FROM clause.
      const series = `generate_series(${start}, ${end}, ${step})`;
      sql = `SELECT ${columnsClause} FROM ${series} AS series_data`;
    }

    // Store output schema for downstream validation.
    if (node.metadata && config?.columns) {
      node.metadata.outputSchema = config.columns.map((c) => ({
        name: c.name,
        dataType: c.type,
      }));
      globalLogger.debug(`[RowGeneratorSQLGenerator] Set output schema with ${config.columns.length} columns.`);
    }

    return {
      sql,
      dependencies: [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'row_generator',
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

  protected emptyFragment(): GeneratedSQLFragment {
    return {
      sql: '',
      dependencies: [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'empty',
        lineCount: 0,
      },
    };
  }
}