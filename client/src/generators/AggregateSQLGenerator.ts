// src/generators/AggregateSQLGenerator.ts

import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';
import { CanvasNode, AggregationConfig, PostgreSQLDataType } from '../types/pipeline-types';

// Add this interface for SQLGenerationError if not already defined
interface SQLGenerationError {
  code: string;
  message: string;
  severity: 'ERROR' | 'WARNING';
  field?: string;
}

// Update the sourceColumns type to include the missing properties
interface SourceColumn {
  name: string;
  dataType: PostgreSQLDataType;
  isPrimaryKey?: boolean;
  isUnique?: boolean;
}

/**
 * PostgreSQL AGGREGATE SQL Generator
 * Handles GROUP BY, aggregate functions, and HAVING clauses
 */
export class AggregateSQLGenerator extends BaseSQLGenerator {
  // ==================== TEMPLATE METHOD IMPLEMENTATIONS ====================

protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
  const node = context.node as CanvasNode;  // ← Type assertion to fix UnifiedCanvasNode mismatch
  const { connection } = context;
  const aggConfig = node.metadata?.aggregationConfig as AggregationConfig | undefined;

  if (!aggConfig) {
    return this.generateFallbackSelect(context);
  }

  // ✅ NEW VALIDATION: Require at least one groupBy column or one aggregate function
  if (aggConfig.groupBy.length === 0 && aggConfig.aggregates.length === 0) {
    const error: SQLGenerationError = {
      code: 'INVALID_AGGREGATE_CONFIG',
      message: 'Aggregate node must have at least one group by column or aggregate function',
      severity: 'ERROR',
      field: 'aggregationConfig',
    };
    return this.errorFragment('aggregate_select', [error], []);
  }

  // Generate SELECT clause with aggregate functions
  const selectClause = this.generateAggregateSelect(aggConfig);
  // The source reference is the upstream node id (will be replaced by pipeline)
  const sourceRef = connection?.sourceNodeId
    ? this.sanitizeIdentifier(connection.sourceNodeId)
    : 'source';

  const sql = `SELECT ${selectClause}\nFROM ${sourceRef}`;

  return {
    sql,
    dependencies: this.extractSourceDependencies(node),
    parameters: new Map(),
    errors: [],
    warnings: [],
    metadata: {
      generatedAt: new Date().toISOString(),
      fragmentType: 'aggregate_select',
      lineCount: sql.split('\n').length,
    },
  };
}
  protected generateJoinConditions(_context: SQLGenerationContext): GeneratedSQLFragment {
    return this.createEmptyFragment('join_conditions');
  }

  protected generateWhereClause(_context: SQLGenerationContext): GeneratedSQLFragment {
    return this.createEmptyFragment('where_clause');
  }

  protected generateHavingClause(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    const aggConfig = node.metadata?.aggregationConfig as AggregationConfig | undefined;

    if (!aggConfig?.having) {
      return this.createEmptyFragment('having_clause');
    }

    const havingClause = this.optimizeHavingClause(aggConfig.having);
    return {
      sql: `HAVING ${havingClause}`,
      dependencies: [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'aggregate_having',
        lineCount: 1,
      },
    };
  }

  protected generateOrderByClause(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    if (node.metadata?.sortConfig) {
      return this.generateOrderByFromSortConfig(node.metadata.sortConfig);
    }
    return this.createEmptyFragment('order_by_clause');
  }

  protected generateGroupByClause(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    const aggConfig = node.metadata?.aggregationConfig as AggregationConfig | undefined;

    if (!aggConfig?.groupBy || aggConfig.groupBy.length === 0) {
      return this.createEmptyFragment('group_by_clause');
    }

    const groupByClause = this.generateGroupByList(aggConfig.groupBy);
    return {
      sql: `GROUP BY ${groupByClause}`,
      dependencies: [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'group_by',
        lineCount: 1,
      },
    };
  }

  // ==================== AGGREGATE-SPECIFIC METHODS ====================

  /**
   * Generate complete aggregation SQL with PostgreSQL optimizations
   */
  public generateAggregationSQL(
    sourceColumns: SourceColumn[],
    aggConfig: AggregationConfig,
    options: {
      useWindowFunctions?: boolean;
      distinctAggregates?: boolean;
      materializeIntermediate?: boolean;
    } = {}
  ): GeneratedSQLFragment {
    const errors: SQLGenerationError[] = [];
    const warnings: string[] = [];

    // Validate aggregation configuration
    this.validateAggregationConfig(sourceColumns, aggConfig, errors, warnings);

    if (errors.length > 0) {
      return this.errorFragment('aggregation_generation', errors, warnings);
    }

    // Generate SQL based on optimization options
    let sql: string;

    if (options.useWindowFunctions && this.featureSupport.supports.windowFunctions) {
      sql = this.generateWindowAggregation(sourceColumns, aggConfig, options);
    } else if (options.materializeIntermediate) {
      sql = this.generateMaterializedAggregation(sourceColumns, aggConfig);
    } else {
      sql = this.generateTraditionalAggregation(sourceColumns, aggConfig, options);
    }

    // Add PostgreSQL performance hints
    const performanceHints = this.generateAggregationPerformanceHints(sourceColumns, aggConfig, options);
    warnings.push(...performanceHints);

    // Optimize aggregation SQL
    sql = this.optimizeAggregationSQL(sql, aggConfig);

    return {
      sql,
      dependencies: this.extractSourceTableDependencies(sourceColumns),
      parameters: new Map(),
      errors,
      warnings,
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'complete_aggregation',
        lineCount: sql.split('\n').length,
      },
    };
  }

  /**
   * Generate window function aggregations (no GROUP BY reduction)
   */
  private generateWindowAggregation(
    sourceColumns: SourceColumn[],
    aggConfig: AggregationConfig,
    options: { distinctAggregates?: boolean }
  ): string {
    const selectColumns: string[] = [];

    // Add grouping columns
    aggConfig.groupBy.forEach((col) => {
      selectColumns.push(this.sanitizeIdentifier(col));
    });

    // Add aggregate functions as window functions
    aggConfig.aggregates.forEach((agg) => {
      const distinctClause = options.distinctAggregates ? 'DISTINCT ' : '';
      const windowFunction = `${agg.function}(${distinctClause}${this.sanitizeIdentifier(agg.column)})`;
      const windowDef = this.buildWindowDefinition(aggConfig.groupBy);
      selectColumns.push(`${windowFunction} OVER (${windowDef}) AS ${this.sanitizeIdentifier(agg.alias)}`);
    });

    // Add non-grouping source columns
    const nonGroupingColumns = sourceColumns.filter((col) => !aggConfig.groupBy.includes(col.name));
    nonGroupingColumns.forEach((col) => {
      selectColumns.push(this.sanitizeIdentifier(col.name));
    });

    const columnList = selectColumns.join(',\n    ');
    return `SELECT\n    ${columnList}\nFROM source_table`;
  }

  /**
   * Generate aggregation with materialized intermediate results
   */
  private generateMaterializedAggregation(
    _sourceColumns: SourceColumn[],
    aggConfig: AggregationConfig
  ): string {
    // Create CTE for pre-aggregation
    const cte = `WITH pre_aggregated AS (\n  SELECT\n    ${aggConfig.groupBy.map((gb) => this.sanitizeIdentifier(gb)).join(',\n    ')},\n    ${aggConfig.aggregates
      .map((agg) => {
        const func = agg.function.toLowerCase();
        return `${func}(${this.sanitizeIdentifier(agg.column)}) AS ${this.sanitizeIdentifier(agg.column + '_' + func)}`;
      })
      .join(',\n    ')}\n  FROM source_table\n  GROUP BY ${aggConfig.groupBy.map((gb) => this.sanitizeIdentifier(gb)).join(', ')}`;

    // Main query with additional aggregates if needed
    const mainQuery = `SELECT\n  ${aggConfig.groupBy.map((gb) => this.sanitizeIdentifier(gb)).join(',\n  ')},\n  ${aggConfig.aggregates
      .map((agg) => {
        const func = agg.function.toLowerCase();
        return `${this.sanitizeIdentifier(agg.column + '_' + func)} AS ${this.sanitizeIdentifier(agg.alias)}`;
      })
      .join(',\n  ')}\nFROM pre_aggregated`;

    // Add HAVING clause if present
    if (aggConfig.having) {
      return `${cte}\n)\n${mainQuery}\nHAVING ${aggConfig.having}`;
    }

    return `${cte}\n)\n${mainQuery}`;
  }

  /**
   * Generate traditional GROUP BY aggregation
   */
  private generateTraditionalAggregation(
    _sourceColumns: SourceColumn[],
    aggConfig: AggregationConfig,
    options: { distinctAggregates?: boolean }
  ): string {
    const selectColumns: string[] = [];

    // Add grouping columns
    aggConfig.groupBy.forEach((col) => {
      selectColumns.push(this.sanitizeIdentifier(col));
    });

    // Add aggregate functions
    aggConfig.aggregates.forEach((agg) => {
      const distinctClause = options.distinctAggregates ? 'DISTINCT ' : '';
      selectColumns.push(
        `${agg.function}(${distinctClause}${this.sanitizeIdentifier(agg.column)}) AS ${this.sanitizeIdentifier(agg.alias)}`
      );
    });

    const columnList = selectColumns.join(',\n    ');
    let sql = `SELECT\n    ${columnList}\nFROM source_table\nGROUP BY ${aggConfig.groupBy.map((gb) => this.sanitizeIdentifier(gb)).join(', ')}`;

    // Add HAVING clause
    if (aggConfig.having) {
      sql += `\nHAVING ${aggConfig.having}`;
    }

    return sql;
  }

  /**
   * Generate FILTER clause for conditional aggregates (PostgreSQL 9.4+)
   */
  public generateFilteredAggregate(
    column: string,
    aggregateFunction: string,
    filterCondition: string,
    alias: string
  ): string {
    if (this.featureSupport.supports.filteredAggregates) {
      return `${aggregateFunction}(${this.sanitizeIdentifier(column)}) FILTER (WHERE ${filterCondition}) AS ${this.sanitizeIdentifier(alias)}`;
    } else {
      // Use CASE statement for older PostgreSQL
      return `${aggregateFunction}(CASE WHEN ${filterCondition} THEN ${this.sanitizeIdentifier(column)} END) AS ${this.sanitizeIdentifier(alias)}`;
    }
  }

  /**
   * Generate ROLLUP/CUBE/GROUPING SETS for multi-level aggregations
   */
  public generateGroupingExtensions(
    groupByColumns: string[],
    extensionType: 'ROLLUP' | 'CUBE' | 'GROUPING_SETS' = 'ROLLUP'
  ): string {
    if (!this.featureSupport.supports.groupingExtensions) {
      throw new Error(`${extensionType} requires PostgreSQL 9.5+`);
    }

    const columnList = groupByColumns.map((col) => this.sanitizeIdentifier(col)).join(', ');

    switch (extensionType) {
      case 'ROLLUP':
        return `GROUP BY ROLLUP(${columnList})`;
      case 'CUBE':
        return `GROUP BY CUBE(${columnList})`;
      case 'GROUPING_SETS':
        return `GROUP BY GROUPING SETS((${columnList}))`;
    }
  }

  // ==================== OPTIMIZATION METHODS ====================

  /**
   * Optimize aggregation SQL for PostgreSQL
   */
  private optimizeAggregationSQL(sql: string, aggConfig: AggregationConfig): string {
    let optimized = sql;

    // Replace COUNT(*) with COUNT(1) for consistency
    optimized = optimized.replace(/COUNT\(\s*\*\s*\)/gi, 'COUNT(1)');

    // Replace AVG with safer division to avoid overflow
    optimized = optimized.replace(
      /AVG\(([^)]+)\)/gi,
      'CASE WHEN COUNT($1) > 0 THEN SUM($1)::numeric / COUNT($1) ELSE NULL END'
    );

    // Optimize HAVING clause
    if (aggConfig.having) {
      optimized = this.optimizeHavingClauseInSQL(optimized);
    }

    // Remove unnecessary GROUP BY columns
    optimized = this.pruneGroupByColumns(optimized, aggConfig);

    return optimized;
  }

  /**
   * Prune unnecessary GROUP BY columns
   */
  private pruneGroupByColumns(sql: string, _aggConfig: AggregationConfig): string {
    // In production, this would analyze functional dependencies
    // For now, we'll just clean up obvious duplicates

    const groupByMatch = sql.match(/GROUP BY\s+([^(\n]+(?:\([^)]+\)[^(\n]*)*)/i);
    if (!groupByMatch) {
      return sql;
    }

    const groupByClause = groupByMatch[1];
    const columns = groupByClause.split(',').map((col) => col.trim());
    const uniqueColumns = [...new Set(columns)];

    if (uniqueColumns.length < columns.length) {
      const optimizedClause = uniqueColumns.join(', ');
      return sql.replace(groupByClause, optimizedClause);
    }

    return sql;
  }

  /**
   * Optimize HAVING clause (string version for optimizeAggregationSQL)
   */
  private optimizeHavingClauseInSQL(sql: string): string {
    return sql
      .replace(/\s*=\s*NULL\b/gi, ' IS NULL')
      .replace(/\s*<>\s*NULL\b/gi, ' IS NOT NULL')
      .replace(/\bHAVING\s+COUNT\(\s*\*\s*\)\s*>\s*0/gi, '') // HAVING COUNT(*) > 0 is redundant
      .replace(/\s+AND\s+/gi, ' AND ')
      .replace(/\s+OR\s+/gi, ' OR ');
  }

  /**
   * Optimize HAVING clause (expression version)
   */
  private optimizeHavingClause(havingClause: string): string {
    return havingClause
      .replace(/\s*=\s*NULL\b/gi, ' IS NULL')
      .replace(/\s*<>\s*NULL\b/gi, ' IS NOT NULL')
      .replace(/\s+AND\s+/gi, ' AND ')
      .replace(/\s+OR\s+/gi, ' OR ');
  }

  /**
   * Generate performance hints for aggregation
   */
  private generateAggregationPerformanceHints(
    sourceColumns: SourceColumn[],
    aggConfig: AggregationConfig,
    options: any
  ): string[] {
    const hints: string[] = [];

    // Check for indexes on GROUP BY columns
    aggConfig.groupBy.forEach((column) => {
      const sourceCol = sourceColumns.find((c) => c.name === column);
      if (sourceCol && !sourceCol.isPrimaryKey && !sourceCol.isUnique) {
        hints.push(`Consider adding index on ${column} for GROUP BY performance`);
      }
    });

    // Check for expensive aggregates
    aggConfig.aggregates.forEach((agg) => {
      if (agg.function === 'STDDEV' || agg.function === 'VARIANCE') {
        hints.push(`${agg.function} on ${agg.column} is computationally expensive`);
      }

      if (options.distinctAggregates) {
        hints.push(`DISTINCT aggregate on ${agg.column} may be memory-intensive`);
      }
    });

    // Check for large GROUP BY sets
    if (aggConfig.groupBy.length > 5) {
      hints.push('Large GROUP BY set may benefit from materialized intermediate results');
    }

    // Suggest partitioning for large aggregations
    if (aggConfig.aggregates.some((agg) => agg.function === 'COUNT' && agg.column === '*')) {
      hints.push('For COUNT(*) on large tables, consider using estimated statistics');
    }

    return hints;
  }

  // ==================== VALIDATION METHODS ====================

  private validateAggregationConfig(
    sourceColumns: SourceColumn[],
    aggConfig: AggregationConfig,
    errors: SQLGenerationError[],
    warnings: string[]
  ): void {
    const sourceColumnNames = new Set(sourceColumns.map((c) => c.name));

    // Validate GROUP BY columns
    aggConfig.groupBy.forEach((column) => {
      if (!sourceColumnNames.has(column)) {
        errors.push({
          code: 'GROUP_BY_COLUMN_NOT_FOUND',
          message: `GROUP BY column "${column}" not found in source`,
          severity: 'ERROR',
        });
      }
    });

    // Validate aggregate columns
    aggConfig.aggregates.forEach((agg) => {
      if (agg.column !== '*' && !sourceColumnNames.has(agg.column)) {
        errors.push({
          code: 'AGGREGATE_COLUMN_NOT_FOUND',
          message: `Aggregate column "${agg.column}" not found in source`,
          severity: 'ERROR',
        });
      }

      // Validate aggregate function compatibility with data type
      if (agg.column !== '*') {
        const sourceCol = sourceColumns.find((c) => c.name === agg.column);
        if (sourceCol) {
          const isCompatible = this.validateAggregateCompatibility(agg.function, sourceCol.dataType);
          if (!isCompatible) {
            warnings.push(
              `Aggregate function ${agg.function} may not be compatible with ${sourceCol.dataType} column "${agg.column}"`
            );
          }
        }
      }
    });

    // Validate HAVING clause syntax
    if (aggConfig.having) {
      const validation = this.validateHavingClause(aggConfig.having, aggConfig);
      if (!validation.valid) {
        errors.push({
          code: 'INVALID_HAVING_CLAUSE',
          message: validation.error || 'Invalid HAVING clause',
          severity: 'ERROR',
        });
      }
    }

    // Check for mixed aggregate and non-aggregate columns
    const hasAggregates = aggConfig.aggregates.length > 0;
    const hasGroupBy = aggConfig.groupBy.length > 0;

    if (hasAggregates && !hasGroupBy) {
      warnings.push('Aggregate functions without GROUP BY will produce single row result');
    }
  }

  private validateAggregateCompatibility(functionName: string, dataType: PostgreSQLDataType): boolean {
    const numericTypes = [
      PostgreSQLDataType.SMALLINT,
      PostgreSQLDataType.INTEGER,
      PostgreSQLDataType.BIGINT,
      PostgreSQLDataType.DECIMAL,
      PostgreSQLDataType.NUMERIC,
      PostgreSQLDataType.REAL,
      PostgreSQLDataType.DOUBLE_PRECISION,
      PostgreSQLDataType.SERIAL,
      PostgreSQLDataType.BIGSERIAL,
    ];

    const numericFunctions = ['SUM', 'AVG', 'STDDEV', 'VARIANCE'];

    if (numericFunctions.includes(functionName) && !numericTypes.includes(dataType)) {
      return false;
    }

    if (functionName === 'COUNT') {
      return true; // COUNT works on any type
    }

    if (functionName === 'MIN' || functionName === 'MAX') {
      return true; // MIN/MAX work on comparable types
    }

    return true;
  }

  private validateHavingClause(
    havingClause: string,
    aggConfig: AggregationConfig
  ): { valid: boolean; error?: string } {
    // Check for aggregate functions in HAVING
    const aggregateFunctions = ['SUM', 'AVG', 'COUNT', 'MIN', 'MAX', 'STDDEV', 'VARIANCE'];
    const hasAggregateInHaving = aggregateFunctions.some((func) =>
      new RegExp(`\\b${func}\\s*\\(`, 'i').test(havingClause)
    );

    if (!hasAggregateInHaving) {
      return {
        valid: false,
        error: 'HAVING clause should contain aggregate functions or references to aggregate aliases',
      };
    }

    // Check for valid column references
    const allowedColumns = [...aggConfig.groupBy, ...aggConfig.aggregates.map((agg) => agg.alias)];

    const columnPattern = /\b([a-zA-Z_][a-zA-Z0-9_]*)\b/g;
    let match: RegExpExecArray | null;

    while ((match = columnPattern.exec(havingClause)) !== null) {
      const columnName = match[1];

      // Skip SQL keywords and functions
      if (this.reservedKeywords.has(columnName.toUpperCase())) {
        continue;
      }

      // Skip aggregate functions
      if (aggregateFunctions.includes(columnName.toUpperCase())) {
        continue;
      }

      // Check if it's a known column or alias
      if (!allowedColumns.includes(columnName)) {
        return {
          valid: false,
          error: `Invalid column reference in HAVING clause: "${columnName}"`,
        };
      }
    }

    return { valid: true };
  }

  // ==================== HELPER METHODS ====================

  private generateAggregateSelect(aggConfig: AggregationConfig): string {
    const columns: string[] = [];

    // Add GROUP BY columns
    aggConfig.groupBy.forEach((col) => {
      columns.push(this.sanitizeIdentifier(col));
    });

    // Add aggregate functions
    aggConfig.aggregates.forEach((agg) => {
      const distinctClause = agg.distinct ? 'DISTINCT ' : '';
      const funcExpr =
        agg.column === '*'
          ? `${agg.function}(*)`
          : `${agg.function}(${distinctClause}${this.sanitizeIdentifier(agg.column)})`;
      columns.push(`${funcExpr} AS ${this.sanitizeIdentifier(agg.alias)}`);
    });

    return columns.join(', ');
  }

  private extractSourceDependencies(node: CanvasNode): string[] {
    const dependencies: string[] = [];
    if (node.metadata?.tableMapping) {
      dependencies.push(`${node.metadata.tableMapping.schema}.${node.metadata.tableMapping.name}`);
    }
    return dependencies;
  }

  private extractSourceTableDependencies(sourceColumns: SourceColumn[]): string[] {
    // Extract table name from column references (assuming format "table.column")
    const tables = new Set<string>();
    sourceColumns.forEach((col) => {
      const parts = col.name.split('.');
      if (parts.length === 2) {
        tables.add(parts[0]);
      }
    });
    return Array.from(tables);
  }

  private generateGroupByList(groupByColumns: string[]): string {
    return groupByColumns.map((col) => this.sanitizeIdentifier(col)).join(', ');
  }

  private buildWindowDefinition(groupByColumns: string[]): string {
    if (groupByColumns.length === 0) {
      return 'PARTITION BY 1';
    }
    const partitionBy = groupByColumns.map((col) => this.sanitizeIdentifier(col)).join(', ');
    return `PARTITION BY ${partitionBy}`;
  }

  private generateOrderByFromSortConfig(sortConfig: any): GeneratedSQLFragment {
    const orderByClauses = sortConfig.columns.map((col: any) => {
      const parts = [this.sanitizeIdentifier(col.column), col.direction];
      if (col.nullsFirst !== undefined) {
        parts.push(col.nullsFirst ? 'NULLS FIRST' : 'NULLS LAST');
      }
      return parts.join(' ');
    });

    let sql = `ORDER BY ${orderByClauses.join(', ')}`;

    if (sortConfig.limit) {
      sql += `\nLIMIT ${sortConfig.limit}`;
    }

    if (sortConfig.offset) {
      sql += `\nOFFSET ${sortConfig.offset}`;
    }

    return {
      sql,
      dependencies: [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'order_by',
        lineCount: sql.split('\n').length,
      },
    };
  }

  private generateFallbackSelect(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node, connection } = context;
    const sourceRef = connection?.sourceNodeId
      ? this.sanitizeIdentifier(connection.sourceNodeId)
      : this.sanitizeIdentifier(node.name.toLowerCase().replace(/\s+/g, '_'));

    return {
      sql: `SELECT * FROM ${sourceRef}`,
      dependencies: [node.name],
      parameters: new Map(),
      errors: [],
      warnings: ['No aggregation configuration found, using fallback SELECT'],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'fallback_select',
        lineCount: 1,
      },
    };
  }

  /**
   * Helper to create an empty fragment with a specific fragment type.
   * This avoids conflict with the base class's protected emptyFragment() method.
   */
  private createEmptyFragment(fragmentType: string): GeneratedSQLFragment {
    const frag = this.emptyFragment(); // call base class method
    frag.metadata.fragmentType = fragmentType;
    return frag;
  }

  private errorFragment(
    fragmentType: string,
    errors: SQLGenerationError[],
    warnings: string[] = []
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