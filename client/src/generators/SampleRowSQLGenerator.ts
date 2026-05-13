// src/generators/SampleRowSQLGenerator.ts
import {
  BaseSQLGenerator,
  SQLGenerationContext,
  GeneratedSQLFragment,
  SQLGenerationError,
} from './BaseSQLGenerator';
import { globalLogger } from '../utils/Logger';

interface SampleRowConfig {
  sampleSize: number;
  isAbsolute: boolean;
}

export class SampleRowSQLGenerator extends BaseSQLGenerator {
  public generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node, connection, upstreamSchema } = context;
    const errors: SQLGenerationError[] = [];
    const warnings: string[] = [];

    // 1. Extract configuration
    const config = node.metadata?.sampleRowConfig as SampleRowConfig | undefined;
    if (!config || typeof config.sampleSize !== 'number') {
      errors.push({
        code: 'INVALID_SAMPLE_CONFIG',
        message: 'SampleRow node is missing valid sampleRowConfig',
        severity: 'ERROR',
      });
      return this.errorFragment('sample_row', errors, warnings);
    }

    // 2. Determine source table reference (upstream node ID)
    const sourceRef = connection?.sourceNodeId;
    if (!sourceRef) {
      errors.push({
        code: 'MISSING_SOURCE',
        message: 'SampleRow node has no incoming connection',
        severity: 'ERROR',
      });
      return this.errorFragment('sample_row', errors, warnings);
    }
    const sanitizedSource = this.sanitizeIdentifier(sourceRef);

    // 3. Build column list from upstream schema
    let columnList = '*';
    if (upstreamSchema && upstreamSchema.length > 0) {
      columnList = upstreamSchema.map(col => this.sanitizeIdentifier(col.name)).join(', ');
    } else {
      warnings.push('No upstream schema available, using SELECT *');
    }

    // 4. Build sampling clause
    let samplingClause = '';
    if (config.isAbsolute) {
      samplingClause = `LIMIT ${config.sampleSize}`;
    } else {
      samplingClause = `TABLESAMPLE SYSTEM(${config.sampleSize})`;
    }

    const sql = `SELECT ${columnList} FROM ${sanitizedSource} ${samplingClause}`.trim();

    globalLogger.debug(`[SampleRowSQLGenerator] Generated SQL: ${sql}`);

    return {
      sql,
      dependencies: [sourceRef],
      parameters: new Map(),
      errors,
      warnings,
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'sample_row',
        lineCount: 1,
      },
    };
  }

  // Required abstract method stubs (unused)
  protected generateJoinConditions(): GeneratedSQLFragment {
    return this.emptyFragment();
  }
  protected generateWhereClause(): GeneratedSQLFragment {
    return this.emptyFragment();
  }
  protected generateHavingClause(): GeneratedSQLFragment {
    return this.emptyFragment();
  }
  protected generateOrderByClause(): GeneratedSQLFragment {
    return this.emptyFragment();
  }
  protected generateGroupByClause(): GeneratedSQLFragment {
    return this.emptyFragment();
  }

  private errorFragment(
    fragmentType: string,
    errors: SQLGenerationError[],
    warnings: string[]
  ): GeneratedSQLFragment {
    return {
      sql: '',
      dependencies: [],
      parameters: new Map(),
      errors,
      warnings,
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType,
        lineCount: 0,
      },
    };
  }
}