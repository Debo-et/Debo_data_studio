// src/generators/JoinSQLGenerator.ts

import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment, CTEChain, SQLGenerationError } from './BaseSQLGenerator';
import { CanvasNode, JoinConfig, PostgresColumn, CanvasConnection, SortConfig } from '../types/pipeline-types';

/**
 * PostgreSQL JOIN SQL Generator
 * Supports INNER, LEFT, RIGHT, FULL, CROSS joins with optimization
 */
export class JoinSQLGenerator extends BaseSQLGenerator {
  // ==================== TEMPLATE METHOD IMPLEMENTATIONS ====================

  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node, connection } = context;
    const joinConfig = node.metadata?.joinConfig;
    
    if (!joinConfig) {
      return this.generateFallbackSelect(context);
    }

    // Extract source tables from connections
    const sources = this.extractJoinSources(node, connection);
    if (sources.length < 2) {
      return this.generateFallbackSelect(context);
    }

    // Build SELECT clause
    const selectColumns = this.generateJoinSelectColumns(sources, joinConfig);
    
    return {
      sql: `SELECT ${selectColumns}`,
      dependencies: sources.map(s => s.tableName),
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'join_select',
        lineCount: 1
      }
    };
  }

  protected generateJoinConditions(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    const joinConfig = node.metadata?.joinConfig;
    const sources = this.extractJoinSources(node, context.connection);
    
    if (!joinConfig || sources.length < 2) {
      return this.emptyFragment('join_conditions');
    }

    // Build JOIN clause
    const joinClause = this.buildJoinClause(sources, joinConfig);
    
    return {
      sql: joinClause,
      dependencies: sources.map(s => s.tableName),
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'join_conditions',
        lineCount: joinClause.split('\n').length
      }
    };
  }

  protected generateWhereClause(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    const joinConfig = node.metadata?.joinConfig;
    
    if (!joinConfig?.whereClause) {
      return this.emptyFragment('where_clause');
    }

    const whereClause = this.optimizeJoinWhereClause(joinConfig.whereClause);
    
    return {
      sql: `WHERE ${whereClause}`,
      dependencies: [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'join_where',
        lineCount: 1
      }
    };
  }

  protected generateHavingClause(_context: SQLGenerationContext): GeneratedSQLFragment {
    // JOIN nodes typically don't use HAVING
    return this.emptyFragment('having_clause');
  }

  protected generateOrderByClause(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    
    // JOIN nodes might need ordering if specified
    if (node.metadata?.sortConfig) {
      return this.generateOrderByFromSortConfig(node.metadata.sortConfig);
    }
    
    return this.emptyFragment('order_by_clause');
  }

  protected generateGroupByClause(_context: SQLGenerationContext): GeneratedSQLFragment {
    // JOIN nodes typically don't use GROUP BY
    return this.emptyFragment('group_by_clause');
  }

  // ==================== JOIN-SPECIFIC METHODS ====================

  /**
   * Build complete JOIN SQL with optimization
   */
  public generateJoinSQL(
    sources: Array<{ table: string; alias?: string; columns: PostgresColumn[] }>,
    joinConfig: JoinConfig,
    options: {
      useCTE?: boolean;
      materializeSubqueries?: boolean;
      preferExistsOverIn?: boolean;
    } = {}
  ): GeneratedSQLFragment {
    const errors: SQLGenerationError[] = [];
    const warnings: string[] = [];

    // Validate join configuration
    this.validateJoinConfiguration(sources, joinConfig, errors, warnings);

    if (errors.length > 0) {
      return this.errorFragment('join_generation', errors, warnings);
    }

    // Optimize join order for performance
    const optimizedSources = this.optimizeJoinOrder(sources, joinConfig.condition);
    
    // Generate JOIN SQL based on optimization preferences
    let sql: string;
    
    if (options.useCTE && this.featureSupport.supports.commonTableExpressions) {
      sql = this.generateJoinWithCTE(optimizedSources, joinConfig, options);
    } else if (options.materializeSubqueries) {
      sql = this.generateJoinWithMaterializedSubqueries(optimizedSources, joinConfig);
    } else {
      sql = this.generateTraditionalJoin(optimizedSources, joinConfig);
    }

    // Add PostgreSQL performance hints
    const performanceHints = this.generateJoinPerformanceHints(optimizedSources, joinConfig);
    warnings.push(...performanceHints);

    // Optimize join condition
    sql = this.optimizeJoinCondition(sql, joinConfig.condition);

    return {
      sql,
      dependencies: optimizedSources.map(s => s.table),
      parameters: new Map(),
      errors,
      warnings,
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'complete_join',
        lineCount: sql.split('\n').length
      }
    };
  }

  /**
   * Generate traditional JOIN syntax
   */
  private generateTraditionalJoin(
    sources: Array<{ table: string; alias?: string; columns: PostgresColumn[] }>,
    joinConfig: JoinConfig
  ): string {
    if (sources.length < 2) {
      throw new Error('At least two tables required for JOIN');
    }

    const firstSource = sources[0];

    // Start with FROM clause
    let sql = `FROM ${this.sanitizeIdentifier(firstSource.table)}`;
    if (firstSource.alias) {
      sql += ` AS ${this.sanitizeIdentifier(firstSource.alias)}`;
    }

    // Add JOIN clauses
    for (let i = 1; i < sources.length; i++) {
      const source = sources[i];
      const joinType = this.getJoinTypeForTable(i, joinConfig.type);
      const alias = source.alias || `t${i + 1}`;
      
      sql += `\n${joinType} JOIN ${this.sanitizeIdentifier(source.table)} AS ${this.sanitizeIdentifier(alias)}`;
      
      if (joinConfig.condition) {
        // Replace table references with aliases in condition
        const condition = this.adaptJoinCondition(joinConfig.condition, i + 1, joinConfig.leftAlias, joinConfig.rightAlias);
        sql += ` ON ${condition}`;
      }
    }

    // Add WHERE clause if present
    if (joinConfig.whereClause) {
      sql += `\nWHERE ${joinConfig.whereClause}`;
    }

    return sql;
  }

  /**
   * Generate JOIN using CTEs for complex subqueries
   */
  private generateJoinWithCTE(
    sources: Array<{ table: string; alias?: string; columns: PostgresColumn[] }>,
    joinConfig: JoinConfig,
    options: { materializeSubqueries?: boolean }
  ): string {
    // Create CTEs for each source
    const ctes: CTEChain[] = sources.map((source, index) => ({
      name: source.alias || `source_${index + 1}`,
      query: `SELECT * FROM ${this.sanitizeIdentifier(source.table)}`,
      materialized: options.materializeSubqueries || false,
      columns: source.columns.map(c => c.name)
    }));

    // Generate CTE chain
    const cteChain = this.generateCTEChain(ctes, {
      materialized: options.materializeSubqueries,
      recursive: false
    });

    // Build main query with CTEs
    let sql = `${cteChain.sql}\n\nSELECT `;
    
    // Add column list
    const columns = this.generateColumnListFromSources(sources, joinConfig);
    sql += columns;
    
    // Add FROM clause with CTE references
    sql += `\nFROM ${ctes[0].name}`;
    
    // Add JOIN clauses
    for (let i = 1; i < ctes.length; i++) {
      const joinType = this.getJoinTypeForTable(i, joinConfig.type);
      sql += `\n${joinType} JOIN ${ctes[i].name} ON ${joinConfig.condition}`;
    }
    
    // Add WHERE clause
    if (joinConfig.whereClause) {
      sql += `\nWHERE ${joinConfig.whereClause}`;
    }

    return sql;
  }

  /**
   * Generate JOIN with materialized subqueries
   */
  private generateJoinWithMaterializedSubqueries(
    sources: Array<{ table: string; alias?: string; columns: PostgresColumn[] }>,
    joinConfig: JoinConfig
  ): string {
    // PostgreSQL LATERAL JOIN with subqueries
    const lateralJoins: string[] = [];
    
    sources.forEach((source, index) => {
      const alias = source.alias || `t${index + 1}`;
      const joinType = index === 0 ? '' : this.getJoinTypeForTable(index, joinConfig.type);
      
      if (index === 0) {
        lateralJoins.push(`FROM ${this.sanitizeIdentifier(source.table)} AS ${alias}`);
      } else {
        lateralJoins.push(`${joinType} JOIN LATERAL (`);
        lateralJoins.push(`  SELECT * FROM ${this.sanitizeIdentifier(source.table)}`);
        if (joinConfig.condition) {
          lateralJoins.push(`  WHERE ${this.adaptJoinCondition(joinConfig.condition, index + 1, joinConfig.leftAlias, joinConfig.rightAlias)}`);
        }
        lateralJoins.push(`) AS ${alias} ON TRUE`);
      }
    });

    return lateralJoins.join('\n');
  }

  // ==================== OPTIMIZATION METHODS ====================

  /**
   * Optimize join order based on table sizes and conditions
   */
  private optimizeJoinOrder(
    sources: Array<{ table: string; alias?: string; columns: PostgresColumn[] }>,
    joinCondition: string
  ): Array<{ table: string; alias?: string; columns: PostgresColumn[] }> {
    if (sources.length <= 2) {
      return sources;
    }

    // Simple heuristic: place smaller tables first
    // In production, this would use table statistics
    const estimatedSizes = sources.map(source => ({
      source,
      size: this.estimateTableSize(source)
    }));

    // Sort by estimated size (smallest first)
    estimatedSizes.sort((a, b) => a.size - b.size);

    // Reorder to minimize intermediate result sizes
    const optimized: typeof sources = [];
    const used = new Set<number>();

    // Start with smallest table
    optimized.push(estimatedSizes[0].source);
    used.add(0);

    // Greedy algorithm: add next best table based on join selectivity
    while (optimized.length < sources.length) {
      let bestIndex = -1;
      let bestSelectivity = Infinity;

      for (let i = 0; i < estimatedSizes.length; i++) {
        if (used.has(i)) continue;

        // Estimate join selectivity with current optimized tables
        const selectivity = this.estimateJoinSelectivity(
          estimatedSizes[i].source,
          optimized,
          joinCondition
        );

        if (selectivity < bestSelectivity) {
          bestSelectivity = selectivity;
          bestIndex = i;
        }
      }

      if (bestIndex !== -1) {
        optimized.push(estimatedSizes[bestIndex].source);
        used.add(bestIndex);
      }
    }

    return optimized;
  }

  /**
   * Optimize join conditions for PostgreSQL
   */
  private optimizeJoinCondition(sql: string, condition: string): string {
    // Replace INNER JOIN with JOIN
    sql = sql.replace(/INNER JOIN/g, 'JOIN');

    // Simplify join conditions
    const optimizedCondition = condition
      .replace(/\s*=\s*NULL\b/gi, ' IS NULL')
      .replace(/\b(\w+)\.(\w+)\s*=\s*\1\.(\w+)\b/gi, '$1.$2 = $1.$3')
      .replace(/\s+AND\s+/gi, ' AND ')
      .replace(/\s+OR\s+/gi, ' OR ');

    // Replace condition in SQL
    sql = sql.replace(new RegExp(this.escapeRegex(condition), 'g'), optimizedCondition);

    return sql;
  }

  /**
   * Generate performance hints for JOIN optimization
   */
  private generateJoinPerformanceHints(
    sources: Array<{ table: string; alias?: string; columns: PostgresColumn[] }>,
    joinConfig: JoinConfig
  ): string[] {
    const hints: string[] = [];

    // Check for missing indexes on join columns
    const joinColumns = this.extractJoinColumns(joinConfig.condition);
    
    joinColumns.forEach(({ table, column }) => {
      const source = sources.find(s => 
        s.table === table || s.alias === table
      );
      
      if (source) {
        const hasIndex = source.columns.some(c => 
          c.name === column && (c.isPrimaryKey || c.isUnique)
        );
        
        if (!hasIndex) {
          hints.push(`Consider adding index on ${table}.${column} for join performance`);
        }
      }
    });

    // Suggest join type optimizations
    if (joinConfig.type === 'FULL') {
      hints.push('FULL JOIN may be expensive; consider if INNER or LEFT JOIN suffices');
    }

    if (sources.length > 3) {
      hints.push('Multiple joins may benefit from materializing intermediate results with CTEs');
    }

    // Check for cartesian products
    if (!joinConfig.condition && joinConfig.type !== 'CROSS') {
      hints.push('Warning: No join condition specified, may produce cartesian product');
    }

    return hints;
  }

  // ==================== HELPER METHODS ====================

  private extractJoinSources(
    node: CanvasNode,
    connection?: CanvasConnection
  ): Array<{ tableName: string; alias?: string; columns: PostgresColumn[] }> {
    const sources: Array<{ tableName: string; alias?: string; columns: PostgresColumn[] }> = [];

    // Extract from node metadata
    if (node.metadata?.tableMapping) {
      sources.push({
        tableName: `${node.metadata.tableMapping.schema}.${node.metadata.tableMapping.name}`,
        alias: node.name,
        columns: node.metadata.tableMapping.columns
      });
    }

    // Extract from connection metadata if available
    if (connection?.dataFlow?.schemaMappings) {
      // This would typically come from upstream nodes
      // For now, we'll create a placeholder
      sources.push({
        tableName: 'source_table',
        alias: 'source',
        columns: []
      });
    }

    return sources;
  }

  private generateJoinSelectColumns(
    sources: Array<{ tableName: string; alias?: string; columns: PostgresColumn[] }>,
    _joinConfig: JoinConfig
  ): string {
    const columns: string[] = [];

    sources.forEach((source, index) => {
      const alias = source.alias || `t${index + 1}`;
      
      if (source.columns.length > 0) {
        source.columns.forEach(column => {
          const columnAlias = `${alias}_${column.name}`;
          columns.push(`${this.sanitizeIdentifier(alias)}.${this.sanitizeIdentifier(column.name)} AS ${this.sanitizeIdentifier(columnAlias)}`);
        });
      } else {
        // Use wildcard if no columns specified
        columns.push(`${this.sanitizeIdentifier(alias)}.*`);
      }
    });

    return columns.join(', ');
  }

  private buildJoinClause(
    sources: Array<{ tableName: string; alias?: string; columns: PostgresColumn[] }>,
    joinConfig: JoinConfig
  ): string {
    if (sources.length < 2) {
      return `FROM ${this.sanitizeIdentifier(sources[0].tableName)}`;
    }

    const firstSource = sources[0];
    let sql = `FROM ${this.sanitizeIdentifier(firstSource.tableName)}`;
    
    if (firstSource.alias) {
      sql += ` AS ${this.sanitizeIdentifier(firstSource.alias)}`;
    }

    // Add additional tables with joins
    for (let i = 1; i < sources.length; i++) {
      const source = sources[i];
      const joinType = this.getJoinTypeForTable(i, joinConfig.type);
      const alias = source.alias || `t${i + 1}`;
      
      sql += `\n${joinType} JOIN ${this.sanitizeIdentifier(source.tableName)} AS ${this.sanitizeIdentifier(alias)}`;
      
      if (joinConfig.condition) {
        const adaptedCondition = this.adaptJoinCondition(
          joinConfig.condition,
          i + 1,
          joinConfig.leftAlias,
          joinConfig.rightAlias
        );
        sql += ` ON ${adaptedCondition}`;
      }
    }

    return sql;
  }

  private getJoinTypeForTable(index: number, overallType: string): string {
    if (overallType === 'CROSS' || index === 0) {
      return overallType;
    }
    
    // For multi-way joins, use INNER by default
    return overallType === 'FULL' ? 'FULL OUTER' : overallType;
  }

  private adaptJoinCondition(
    condition: string,
    tableIndex: number,
    leftAlias?: string,
    rightAlias?: string
  ): string {
    let adapted = condition;
    
    if (leftAlias && rightAlias) {
      adapted = adapted
        .replace(new RegExp(`\\b${leftAlias}\\b`, 'g'), 't1')
        .replace(new RegExp(`\\b${rightAlias}\\b`, 'g'), `t${tableIndex + 1}`);
    }
    
    return adapted;
  }

  private extractJoinColumns(condition: string): Array<{ table: string; column: string }> {
    const columns: Array<{ table: string; column: string }> = [];
    const pattern = /\b([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\b/g;
    let match: RegExpExecArray | null;
    
    while ((match = pattern.exec(condition)) !== null) {
      columns.push({
        table: match[1],
        column: match[2]
      });
    }
    
    return columns;
  }

  private estimateTableSize(source: { table: string; columns: PostgresColumn[] }): number {
    // Simple heuristic: more columns = larger row size
    // In production, this would use table statistics
    return source.columns.length;
  }

  private estimateJoinSelectivity(
    _source: { table: string; columns: PostgresColumn[] },
    _existingTables: Array<{ table: string; columns: PostgresColumn[] }>,
    _condition: string
  ): number {
    // Simple heuristic: assume 10% selectivity for joins
    // In production, this would use column statistics and histograms
    return 0.1;
  }

  private validateJoinConfiguration(
    sources: Array<{ table: string; alias?: string; columns: PostgresColumn[] }>,
    joinConfig: JoinConfig,
    errors: SQLGenerationError[],
    warnings: string[]
  ): void {
    if (sources.length < 2) {
      errors.push({
        code: 'INSUFFICIENT_TABLES',
        message: 'At least two tables required for JOIN operation',
        severity: 'ERROR'
      });
    }

    if (joinConfig.type === 'CROSS' && joinConfig.condition) {
      warnings.push('CROSS JOIN with condition may be unnecessary');
    }

    if (!joinConfig.condition && joinConfig.type !== 'CROSS') {
      warnings.push('No join condition specified; may produce cartesian product');
    }

    // Check for ambiguous column names
    const allColumns = new Map<string, string[]>();
    sources.forEach(source => {
      source.columns.forEach(col => {
        if (!allColumns.has(col.name)) {
          allColumns.set(col.name, []);
        }
        allColumns.get(col.name)!.push(source.alias || source.table);
      });
    });

    allColumns.forEach((tables, column) => {
      if (tables.length > 1) {
        warnings.push(`Column "${column}" exists in multiple tables: ${tables.join(', ')}. Use table aliases.`);
      }
    });
  }

  private generateColumnListFromSources(
    sources: Array<{ table: string; alias?: string; columns: PostgresColumn[] }>,
    _joinConfig: JoinConfig
  ): string {
    const columns: string[] = [];
    
    sources.forEach((source, index) => {
      const alias = source.alias || `t${index + 1}`;
      
      if (source.columns.length > 0) {
        source.columns.forEach(col => {
          const columnName = `${alias}.${col.name}`;
          const columnAlias = `${alias}_${col.name}`;
          columns.push(`${this.sanitizeIdentifier(columnName)} AS ${this.sanitizeIdentifier(columnAlias)}`);
        });
      } else {
        columns.push(`${this.sanitizeIdentifier(alias)}.*`);
      }
    });
    
    return columns.join(', ');
  }

  private optimizeJoinWhereClause(whereClause: string): string {
    // Optimize WHERE clause for PostgreSQL
    return whereClause
      .replace(/\s*=\s*NULL\b/gi, ' IS NULL')
      .replace(/\s*<>\s*NULL\b/gi, ' IS NOT NULL')
      .replace(/\bLIKE\s+'%(.+)%'/gi, "ILIKE '%$1%'")
      .replace(/\s+AND\s+/gi, ' AND ')
      .replace(/\s+OR\s+/gi, ' OR ');
  }

  private generateOrderByFromSortConfig(sortConfig: SortConfig): GeneratedSQLFragment {
    const orderByClauses = sortConfig.columns.map(col => {
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
        lineCount: sql.split('\n').length
      }
    };
  }

  private generateFallbackSelect(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    
    return {
      sql: `SELECT * FROM ${this.sanitizeIdentifier(node.name.toLowerCase().replace(/\s+/g, '_'))}`,
      dependencies: [node.name],
      parameters: new Map(),
      errors: [],
      warnings: ['No join configuration found, using fallback SELECT'],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'fallback_select',
        lineCount: 1
      }
    };
  }

  private emptyFragment(fragmentType: string): GeneratedSQLFragment {
    return {
      sql: '',
      dependencies: [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType,
        lineCount: 0
      }
    };
  }

  private errorFragment(fragmentType: string, errors: SQLGenerationError[], warnings: string[] = []): GeneratedSQLFragment {
    return {
      sql: '',
      dependencies: [],
      parameters: new Map(),
      errors,
      warnings,
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType,
        lineCount: 0
      }
    };
  }

  private escapeRegex(string: string): string {
    return string.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }
}