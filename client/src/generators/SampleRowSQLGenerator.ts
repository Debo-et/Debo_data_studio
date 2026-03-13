// src/generators/SampleRowSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment, SQLGenerationError } from './BaseSQLGenerator';
import { NodeType } from '../types/unified-pipeline.types';

interface SampleConfig {
  method: 'random' | 'first' | 'systematic';
  size: number;            // number of rows
  percentage?: number;     // for random percentage
}

export class SampleRowSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    const warnings: string[] = [];
    const errors: SQLGenerationError[] = [];

    // Safely extract the sample configuration
    const config = this.extractSampleConfig(node);

    let sql: string;
    if (config?.method === 'random') {
      if (config.percentage) {
        sql = `SELECT * FROM source_table TABLESAMPLE SYSTEM(${config.percentage})`;
      } else {
        sql = `SELECT * FROM source_table ORDER BY random() LIMIT ${config.size}`;
      }
    } else if (config?.method === 'first') {
      sql = `SELECT * FROM source_table LIMIT ${config.size}`;
    } else {
      // Fallback: return all rows (or handle as error)
      sql = `SELECT * FROM source_table`;
      warnings.push('No valid sample configuration found; returning all rows.');
    }

    return {
      sql,
      dependencies: ['source_table'],
      parameters: new Map(),
      errors,
      warnings,
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'sample_row', lineCount: 1 }
    };
  }

  // Implement remaining abstract methods with empty fragments
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

  // Helper to produce an empty fragment
  private emptyFragment(): GeneratedSQLFragment {
    return {
      sql: '',
      dependencies: [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'empty',
        lineCount: 0
      }
    };
  }

  // Extract and validate sample configuration from node metadata
  private extractSampleConfig(node: any): SampleConfig | null {
    // Ensure node type matches; this generator should only be used for SAMPLE_ROW nodes
    if (node.type !== NodeType.SAMPLE_ROW) {
      return null;
    }

    const config = node.metadata?.configuration?.config;
    if (!config || typeof config !== 'object') {
      return null;
    }

    // Type guard to check if config matches SampleConfig shape
    const hasValidMethod = config.method === 'random' || config.method === 'first' || config.method === 'systematic';
    const hasValidSize = typeof config.size === 'number' && config.size > 0;
    if (!hasValidMethod || !hasValidSize) {
      return null;
    }

    // For random method, percentage is optional; if present it must be a number
    if (config.method === 'random' && config.percentage !== undefined && typeof config.percentage !== 'number') {
      return null;
    }

    return config as SampleConfig;
  }
}