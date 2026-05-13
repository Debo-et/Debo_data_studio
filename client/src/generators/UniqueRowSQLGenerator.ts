// src/generators/UniqueRowSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment, SQLGenerationError } from './BaseSQLGenerator';

interface UniqueConfig {
  columns?: string[];
  keep: 'first' | 'last';
}

export class UniqueRowSQLGenerator extends BaseSQLGenerator {
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

  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node, connection, upstreamSchema } = context;
    const errors: SQLGenerationError[] = [];
    const warnings: string[] = [];

    // Determine source reference
    let sourceRef: string;
    if (connection) {
      sourceRef = this.sanitizeIdentifier(connection.sourceNodeId);
    } else {
      sourceRef = 'unknown_source';
      warnings.push('No incoming connection; using "unknown_source" as table reference.');
    }

    const config = this.extractUniqueConfig(node);

    // If no upstream schema, fallback to SELECT *
    if (!upstreamSchema || upstreamSchema.length === 0) {
      warnings.push('No upstream schema available; using SELECT DISTINCT *');
      const sql = `SELECT DISTINCT * FROM ${sourceRef}`;
      return {
        sql,
        dependencies: connection ? [connection.sourceNodeId] : [],
        parameters: new Map(),
        errors,
        warnings,
        metadata: { generatedAt: new Date().toISOString(), fragmentType: 'unique_row', lineCount: 1 }
      };
    }

    const allColumns = upstreamSchema.map(col => col.name);
    const distinctCols = config.columns && config.columns.length > 0
      ? config.columns.filter(col => allColumns.includes(col))
      : [];

    let sql: string;
    if (distinctCols.length > 0) {
      const distinctList = distinctCols.map(c => this.sanitizeIdentifier(c)).join(', ');
      const orderDirection = config.keep === 'last' ? 'DESC' : 'ASC';
      const orderByClause = distinctCols.map(c => this.sanitizeIdentifier(c) + ' ' + orderDirection).join(', ');
      const columnList = allColumns.map(c => this.sanitizeIdentifier(c)).join(', ');
      sql = `SELECT DISTINCT ON (${distinctList}) ${columnList} FROM ${sourceRef} ORDER BY ${orderByClause}`;
    } else {
      const columnList = allColumns.map(c => this.sanitizeIdentifier(c)).join(', ');
      sql = `SELECT DISTINCT ${columnList} FROM ${sourceRef}`;
    }

    return {
      sql,
      dependencies: connection ? [connection.sourceNodeId] : [],
      parameters: new Map(),
      errors,
      warnings,
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'unique_row', lineCount: sql.split('\n').length }
    };
  }

  private extractUniqueConfig(node: any): UniqueConfig {
    // 1. Test builder pattern: metadata.uniqRowConfig.keyColumns
    if (node.metadata?.uniqRowConfig?.keyColumns) {
      return {
        columns: node.metadata.uniqRowConfig.keyColumns,
        keep: node.metadata.uniqRowConfig.keep === 'last' ? 'last' : 'first',
      };
    }

    // 2. Unified configuration wrapper
    const configuration = node.metadata?.configuration;
    if (configuration) {
      if (configuration.type === 'UNIQ_ROW' && configuration.config) {
        const cfg = configuration.config;
        return {
          columns: cfg.keyColumns || cfg.columns || [],
          keep: cfg.keep === 'last' ? 'last' : 'first',
        };
      }
      if (configuration.type === 'OTHER' && configuration.config) {
        const cfg = configuration.config;
        return {
          columns: cfg.columns || [],
          keep: cfg.keep === 'last' ? 'last' : 'first',
        };
      }
    }

    // 3. Legacy metadata locations
    if (node.metadata?.config) {
      const cfg = node.metadata.config;
      return {
        columns: cfg.keyColumns || cfg.columns || [],
        keep: cfg.keep === 'last' ? 'last' : 'first',
      };
    }

    return { keep: 'first' };
  }
}