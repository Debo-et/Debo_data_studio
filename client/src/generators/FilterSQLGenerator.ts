// src/generators/FilterSQLGenerator.ts

import {
  BaseSQLGenerator,
  SQLGenerationContext,
  GeneratedSQLFragment,
  SQLGenerationError,
} from './BaseSQLGenerator';
import { FilterConfig, PostgreSQLDataType, SortConfig } from '../types/pipeline-types';
import { UnifiedCanvasNode } from '../types/unified-pipeline.types';
import { globalLogger } from '../utils/Logger';

/**
 * PostgreSQL FILTER SQL Generator
 * Handles WHERE clause generation, complex predicates, and parameterized queries.
 * Now overrides generateSQL to produce a complete SELECT ... FROM ... WHERE ... statement
 * with proper schema validation and optimization.
 */
export class FilterSQLGenerator extends BaseSQLGenerator {
  // ==================== CONSTRUCTOR WITH LOGGING ====================
  constructor(options?: any) {
    super(options);
    globalLogger.debug(`[FilterSQLGenerator] Constructor called with options:`, options);
  }

  // ==================== TEMPLATE METHOD IMPLEMENTATIONS ====================

  protected generateSelectStatement(): GeneratedSQLFragment {
    return this.emptyFragment();
  }

  protected generateJoinConditions(): GeneratedSQLFragment {
    return this.emptyFragment();
  }

  protected generateWhereClause(): GeneratedSQLFragment {
    return this.emptyFragment();
  }

  protected generateHavingClause(): GeneratedSQLFragment {
    return this.emptyFragment();
  }

  protected generateOrderByClause(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    const sortConfig = node.metadata?.sortConfig as SortConfig | undefined;
    if (sortConfig) {
      globalLogger.debug(`[FilterSQLGenerator] generateOrderByClause using sortConfig`, sortConfig);
      return this.generateOrderByFromSortConfig(sortConfig);
    }
    return this.emptyFragment();
  }

  protected generateGroupByClause(): GeneratedSQLFragment {
    return this.emptyFragment();
  }

  // ==================== OVERRIDDEN GENERATION (MAIN ENTRY POINT) ====================

  // src/generators/FilterSQLGenerator.ts

public generateSQL(context: SQLGenerationContext): GeneratedSQLFragment {
  globalLogger.debug(`[FilterSQLGenerator] generateSQL called for node ${context.node.id} (${context.node.name})`, {
    hasUpstreamSchema: !!context.upstreamSchema,
    upstreamSchemaLength: context.upstreamSchema?.length,
    hasConnection: !!context.connection,
    connectionSourceId: context.connection?.sourceNodeId,
    indentLevel: context.indentLevel,
    options: context.options,
  });

  const { node, connection, upstreamSchema, options, indentLevel } = context;
  const errors: SQLGenerationError[] = [];
  const warnings: string[] = [];

  // Determine source reference (table or CTE name)
  let sourceRef = 'source_table';
  if (connection?.sourceNodeId) {
    sourceRef = this.sanitizeIdentifier(connection.sourceNodeId);
    globalLogger.debug(`[FilterSQLGenerator] Using source reference from connection: ${sourceRef}`);
  } else {
    warnings.push('No incoming connection for filter node – using placeholder source.');
    globalLogger.warn(`[FilterSQLGenerator] No incoming connection, using placeholder source: ${sourceRef}`);
  }

  // Extract filter configuration
  const filterConfig = node.metadata?.filterConfig as FilterConfig | undefined;
  let condition = filterConfig?.condition || node.metadata?.filterCondition || '1=1';

  // --- NEW: Warn if fallback condition is used ---
  if (!filterConfig?.condition && !node.metadata?.filterCondition) {
    warnings.push('No filter condition configured; using fallback condition "1=1" (all rows pass).');
  }
  // ----------------------------------------------

  globalLogger.debug(`[FilterSQLGenerator] Filter condition from config: ${condition}`);

  // Validate condition against upstream schema (if available)
  if (upstreamSchema && upstreamSchema.length > 0) {
    const validation = this.validatePredicate(condition, upstreamSchema);
    if (!validation.valid) {
      validation.errors.forEach((err) => {
        errors.push({
          code: 'INVALID_FILTER_CONDITION',
          message: err,
          severity: 'ERROR',
          suggestion: 'Check column names and syntax in filter condition',
        });
        globalLogger.warn(`[FilterSQLGenerator] Invalid filter condition: ${err}`);
      });
    } else {
      globalLogger.debug(`[FilterSQLGenerator] Filter condition validated successfully`);
    }
  } else {
    warnings.push('No upstream schema information – filter condition will not be validated.');
    globalLogger.warn(`[FilterSQLGenerator] No upstream schema for validation`);
  }

  // Apply operation (INCLUDE / EXCLUDE)
  if (filterConfig?.operation === 'EXCLUDE') {
    condition = `NOT (${condition})`;
    globalLogger.debug(`[FilterSQLGenerator] Applied EXCLUDE operation, new condition: ${condition}`);
  }

  // Transform to PostgreSQL syntax
  const originalCondition = condition;
  condition = this.transformPredicateToPostgreSQL(condition);
  if (originalCondition !== condition) {
    globalLogger.debug(`[FilterSQLGenerator] Transformed predicate: ${originalCondition} -> ${condition}`);
  }

  // Optimize for readability / performance (optional)
  if (options.optimizeForReadability) {
    const before = condition;
    condition = this.optimizeBooleanLogic(condition);
    condition = this.optimizeNullComparisons(condition);
    if (before !== condition) {
      globalLogger.debug(`[FilterSQLGenerator] Optimized condition: ${before} -> ${condition}`);
    }
  }

  // FIX: Always use SELECT * – filtering should not change column projection
  const selectClause = '*';

  const indent = '  '.repeat(indentLevel);
  let sql = `${indent}SELECT ${selectClause}\n${indent}FROM ${sourceRef}\n${indent}WHERE ${condition}`;
  globalLogger.debug(`[FilterSQLGenerator] Base SQL (without ORDER BY): ${sql.substring(0, 200)}...`);

  // Add ORDER BY if sortConfig present
  const orderByFragment = this.generateOrderByClause(context);
  if (orderByFragment.sql) {
    sql += `\n${orderByFragment.sql}`;
    globalLogger.debug(`[FilterSQLGenerator] Added ORDER BY clause: ${orderByFragment.sql}`);
  }

  // Add comments if requested
  if (options.includeComments) {
    sql = this.addFilterComments(sql, node);
  }

  globalLogger.debug(`[FilterSQLGenerator] Final SQL generated: ${sql.substring(0, 300)}...`);

  return {
    sql,
    dependencies: connection ? [connection.sourceNodeId] : [],
    parameters: new Map(),
    errors,
    warnings,
    metadata: {
      generatedAt: new Date().toISOString(),
      fragmentType: 'filter',
      lineCount: sql.split('\n').length,
      condition,
      source: sourceRef,
      columnsUsed: upstreamSchema?.map(c => c.name) || [],
    },
  };
}
  // ==================== FILTER-SPECIFIC PUBLIC METHODS ====================

  /**
   * Generate complete filter SQL with parameterization support
   */
  public generateFilterSQL(
    sourceColumns: Array<{ name: string; dataType: PostgreSQLDataType }>,
    filterConfig: FilterConfig,
    options: {
      parameterize?: boolean;
      usePreparedStatements?: boolean;
      optimizeForIndex?: boolean;
    } = {},
  ): GeneratedSQLFragment {
    globalLogger.debug(`[FilterSQLGenerator] generateFilterSQL called`, {
      sourceColumnsCount: sourceColumns.length,
      filterConfig: { condition: filterConfig.condition, operation: filterConfig.operation, hasParams: !!filterConfig.parameters },
      options,
    });

    const errors: SQLGenerationError[] = [];
    const warnings: string[] = [];

    // Validate filter configuration
    this.validateFilterConfig(sourceColumns, filterConfig, errors, warnings);

    if (errors.length > 0) {
      globalLogger.error(`[FilterSQLGenerator] Filter config validation failed`, errors);
      return this.errorFragment('filter_generation', errors, warnings);
    }

    // Build WHERE clause
    const whereResult = this.buildWhereClause(filterConfig, sourceColumns, options);
    globalLogger.debug(`[FilterSQLGenerator] WHERE clause built: ${whereResult.sql}`);

    // Generate complete SQL
    const sql = this.buildFilterSelect(sourceColumns, whereResult.sql);
    globalLogger.debug(`[FilterSQLGenerator] Complete filter SQL: ${sql.substring(0, 200)}...`);

    // Add performance hints
    const performanceHints = this.generateFilterPerformanceHints(
      sourceColumns,
      filterConfig,
      whereResult.sql,
    );
    warnings.push(...performanceHints);
    if (performanceHints.length > 0) {
      globalLogger.debug(`[FilterSQLGenerator] Performance hints: ${performanceHints.join(', ')}`);
    }

    // Optimize SQL
    const optimizedSql = this.optimizeFilterSQL(sql, filterConfig, options);
    if (optimizedSql !== sql) {
      globalLogger.debug(`[FilterSQLGenerator] SQL optimized: ${optimizedSql.substring(0, 200)}...`);
    }

    return {
      sql: optimizedSql,
      dependencies: this.extractSourceTableDependencies(sourceColumns),
      parameters: whereResult.parameters,
      errors: [...errors, ...whereResult.errors],
      warnings: [...warnings, ...whereResult.warnings],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'complete_filter',
        lineCount: optimizedSql.split('\n').length,
      },
    };
  }

  /**
   * Generate parameterized WHERE clause for prepared statements
   */
  public generateParameterizedWhereClause(
  condition: string,
  parameters: Record<string, any>,
  sourceColumns: Array<{ name: string; dataType: PostgreSQLDataType }>,
): { sql: string; paramList: Array<{ name: string; value: any; type: PostgreSQLDataType }> } {
  globalLogger.debug(`[FilterSQLGenerator] generateParameterizedWhereClause called`, {
    conditionLength: condition.length,
    paramKeys: Object.keys(parameters),
    sourceColumnsCount: sourceColumns.length,
  });

  const paramList: Array<{ name: string; value: any; type: PostgreSQLDataType }> = [];
  let sql = condition;
  let paramIndex = 1;

  // Replace named parameters (:param) with PostgreSQL positional parameters
  Object.entries(parameters).forEach(([key, value]) => {
    const paramPattern = new RegExp(`:${key}\\b`, 'g');
    const column = sourceColumns.find((c) => c.name === key);
    const paramType = column?.dataType || PostgreSQLDataType.VARCHAR;

    if (paramPattern.test(sql)) {
      sql = sql.replace(paramPattern, `$${paramIndex}`);
      paramList.push({ name: key, value, type: paramType });
      globalLogger.debug(`[FilterSQLGenerator] Replaced :${key} with $${paramIndex} (type: ${paramType})`);
      paramIndex++;
    }
  });

  // Replace ? placeholders with positional parameters
  let qmIndex = paramIndex;
  let placeholderCount = (sql.match(/\?/g) || []).length;
  while (placeholderCount > 0) {
    sql = sql.replace('?', `$${qmIndex}`);
    globalLogger.debug(`[FilterSQLGenerator] Replaced ? with $${qmIndex}`);
    qmIndex++;
    placeholderCount--;
  }

  globalLogger.debug(`[FilterSQLGenerator] Parameterized WHERE clause: ${sql}, paramList size: ${paramList.length}`);
  return { sql, paramList };
}

  /**
   * Generate complex predicate evaluation with PostgreSQL optimizations
   */
  public generateComplexPredicate(
    predicate: string,
    sourceColumns: Array<{ name: string; dataType: PostgreSQLDataType }>,
    options: {
      useBooleanLogic?: boolean;
      optimizeForNulls?: boolean;
      addTypeCasting?: boolean;
    } = {},
  ): { sql: string; errors: string[] } {
    globalLogger.debug(`[FilterSQLGenerator] generateComplexPredicate called`, {
      predicateLength: predicate.length,
      sourceColumnsCount: sourceColumns.length,
      options,
    });

    const errors: string[] = [];

    // Validate predicate syntax
    const validation = this.validatePredicate(predicate, sourceColumns);
    if (!validation.valid) {
      errors.push(...validation.errors);
      globalLogger.warn(`[FilterSQLGenerator] Predicate validation failed`, { errors });
      return { sql: '', errors };
    }

    // Transform to PostgreSQL syntax
    let sql = this.transformPredicateToPostgreSQL(predicate);
    globalLogger.debug(`[FilterSQLGenerator] Transformed predicate: ${predicate} -> ${sql}`);

    // Apply optimizations
    if (options.optimizeForNulls) {
      const before = sql;
      sql = this.optimizeNullComparisons(sql);
      if (before !== sql) globalLogger.debug(`[FilterSQLGenerator] Optimized NULL comparisons: ${before} -> ${sql}`);
    }

    if (options.useBooleanLogic) {
      const before = sql;
      sql = this.optimizeBooleanLogic(sql);
      if (before !== sql) globalLogger.debug(`[FilterSQLGenerator] Optimized boolean logic: ${before} -> ${sql}`);
    }

    if (options.addTypeCasting) {
      const before = sql;
      sql = this.addPredicateTypeCasting(sql, sourceColumns);
      if (before !== sql) globalLogger.debug(`[FilterSQLGenerator] Added type casting: ${before} -> ${sql}`);
    }

    // Optimize for index usage
    const beforeIdx = sql;
    sql = this.optimizeForIndexUsage(sql, sourceColumns);
    if (beforeIdx !== sql) globalLogger.debug(`[FilterSQLGenerator] Optimized for index usage: ${beforeIdx} -> ${sql}`);

    return { sql, errors };
  }

  // ==================== PREDICATE BUILDING METHODS ====================

  private buildWhereClause(
    filterConfig: FilterConfig,
    sourceColumns: Array<{ name: string; dataType: PostgreSQLDataType }>,
    options?: { parameterize?: boolean; usePreparedStatements?: boolean; optimizeForIndex?: boolean },
  ): {
    sql: string;
    parameters: Map<string, any>;
    errors: SQLGenerationError[];
    warnings: string[];
  } {
    globalLogger.debug(`[FilterSQLGenerator] buildWhereClause called`, {
      condition: filterConfig.condition,
      operation: filterConfig.operation,
      hasParams: !!filterConfig.parameters,
      options,
    });

    const errors: SQLGenerationError[] = [];
    const warnings: string[] = [];
    const parameters = new Map<string, any>();

    let condition = filterConfig.condition;

    // Validate condition syntax
    const validation = this.validatePredicate(condition, sourceColumns);
    if (!validation.valid) {
      errors.push(
        ...validation.errors.map((msg) => ({
          code: 'INVALID_PREDICATE',
          message: msg,
          severity: 'ERROR' as const,
        })),
      );
      globalLogger.warn(`[FilterSQLGenerator] Predicate validation failed`, validation.errors);
    }

    // Transform to PostgreSQL syntax
    const originalCondition = condition;
    condition = this.transformPredicateToPostgreSQL(condition);
    if (originalCondition !== condition) {
      globalLogger.debug(`[FilterSQLGenerator] Transformed condition: ${originalCondition} -> ${condition}`);
    }

    // Apply operation (INCLUDE/EXCLUDE)
    if (filterConfig.operation === 'EXCLUDE') {
      condition = `NOT (${condition})`;
      globalLogger.debug(`[FilterSQLGenerator] Applied EXCLUDE operation: ${condition}`);
    }

    // Parameterize if requested
    if (options?.parameterize && filterConfig.parameters) {
      const paramResult = this.parameterizeCondition(
        condition,
        filterConfig.parameters,
        sourceColumns,
      );
      condition = paramResult.sql;
      paramResult.parameters.forEach((value, key) => parameters.set(key, value));
      globalLogger.debug(`[FilterSQLGenerator] Parameterized condition: ${condition}, params: ${paramResult.parameters.size}`);
    }

    // Optimize for index usage
    if (options?.optimizeForIndex) {
      const before = condition;
      condition = this.optimizeForIndexUsage(condition, sourceColumns);
      if (before !== condition) globalLogger.debug(`[FilterSQLGenerator] Optimized for index: ${before} -> ${condition}`);
    }

    // Add type casting for safe comparisons
    const beforeCast = condition;
    condition = this.addPredicateTypeCasting(condition, sourceColumns);
    if (beforeCast !== condition) globalLogger.debug(`[FilterSQLGenerator] Added type casting: ${beforeCast} -> ${condition}`);

    // Optimize NULL comparisons
    const beforeNull = condition;
    condition = this.optimizeNullComparisons(condition);
    if (beforeNull !== condition) globalLogger.debug(`[FilterSQLGenerator] Optimized NULL comparisons: ${beforeNull} -> ${condition}`);

    // Optimize boolean logic
    const beforeBool = condition;
    condition = this.optimizeBooleanLogic(condition);
    if (beforeBool !== condition) globalLogger.debug(`[FilterSQLGenerator] Optimized boolean logic: ${beforeBool} -> ${condition}`);

    // Check for potential performance issues
    this.checkFilterPerformance(condition, sourceColumns, warnings);
    if (warnings.length > 0) {
      globalLogger.debug(`[FilterSQLGenerator] Performance warnings: ${warnings.join(', ')}`);
    }

    return {
      sql: condition,
      parameters,
      errors,
      warnings,
    };
  }

  private parameterizeCondition(
    condition: string,
    parameters: Record<string, any>,
    _sourceColumns: Array<{ name: string; dataType: PostgreSQLDataType }>,
  ): { sql: string; parameters: Map<string, any> } {
    const paramMap = new Map<string, any>();
    let sql = condition;
    let paramIndex = 1;

    Object.entries(parameters).forEach(([key, value]) => {
      // Replace named parameters (:param) with positional parameters
      const namedPattern = new RegExp(`:${key}\\b`, 'g');
      if (namedPattern.test(sql)) {
        sql = sql.replace(namedPattern, `$${paramIndex}`);
        paramMap.set(`$${paramIndex}`, value);
        globalLogger.debug(`[FilterSQLGenerator] Parameterized :${key} -> $${paramIndex} with value ${value}`);
        paramIndex++;
      }

      // Replace ? placeholders
      const questionMarkPattern = /\?/g;
      if (questionMarkPattern.test(sql) && paramMap.size < Object.keys(parameters).length) {
        sql = sql.replace('?', `$${paramIndex}`);
        paramMap.set(`$${paramIndex}`, value);
        globalLogger.debug(`[FilterSQLGenerator] Parameterized ? -> $${paramIndex} with value ${value}`);
        paramIndex++;
      }
    });

    return { sql, parameters: paramMap };
  }

  // ==================== OPTIMIZATION METHODS ====================

  private optimizeFilterSQL(
    sql: string,
    filterConfig: FilterConfig,
    options: { optimizeForIndex?: boolean },
  ): string {
    let optimized = sql;

    // Replace INNER JOIN with JOIN
    optimized = optimized.replace(/INNER JOIN/g, 'JOIN');

    // Optimize WHERE clause
    optimized = this.optimizeWhereClause(optimized, filterConfig);

    // Add query hints if optimizing for index
    if (options.optimizeForIndex) {
      optimized = this.addIndexHints(optimized);
    }

    // Clean up whitespace
    optimized = optimized.replace(/\s+/g, ' ').replace(/\s+WHERE\s+/g, '\nWHERE ');

    return optimized;
  }

  private optimizeWhereClause(sql: string, _filterConfig: FilterConfig): string {
    return sql
      .replace(/\s*=\s*NULL\b/gi, ' IS NULL')
      .replace(/\s*<>\s*NULL\b/gi, ' IS NOT NULL')
      .replace(/\bLIKE\s+'%(.+)%'/gi, "ILIKE '%$1%'")
      .replace(/\s+AND\s+/gi, ' AND ')
      .replace(/\s+OR\s+/gi, ' OR ')
      .replace(/\(\(/g, '(')
      .replace(/\)\)/g, ')');
  }

  private optimizeNullComparisons(condition: string): string {
    return condition
      .replace(/\s*=\s*NULL\b/gi, ' IS NULL')
      .replace(/\s*<>\s*NULL\b/gi, ' IS NOT NULL')
      .replace(/\bISNULL\(/gi, 'COALESCE(')
      .replace(/\bCOALESCE\(([^,]+),\s*NULL\s*\)/gi, '$1');
  }

  private optimizeBooleanLogic(condition: string): string {
    // Apply De Morgan's laws and other boolean optimizations
    return condition
      .replace(/NOT\s*\(\s*([^)]+)\s+AND\s+([^)]+)\s*\)/gi, '(NOT $1 OR NOT $2)')
      .replace(/NOT\s*\(\s*([^)]+)\s+OR\s+([^)]+)\s*\)/gi, '(NOT $1 AND NOT $2)')
      .replace(/\(([^)]+)\s+AND\s+TRUE\)/gi, '$1')
      .replace(/\(([^)]+)\s+OR\s+FALSE\)/gi, '$1')
      .replace(/TRUE\s+AND\s+([^)]+)/gi, '$1')
      .replace(/FALSE\s+OR\s+([^)]+)/gi, '$1');
  }

  private optimizeForIndexUsage(
    condition: string,
    sourceColumns: Array<{ name: string; dataType: PostgreSQLDataType }>,
  ): string {
    let optimized = condition;

    // Transform NOT LIKE to indexable conditions where possible
    optimized = optimized.replace(/NOT\s+LIKE\s+'([^']*)'/gi, (match, pattern) => {
      if (!pattern.includes('%') && !pattern.includes('_')) {
        return `<> '${pattern}'`;
      }
      return match;
    });

    // Transform date range conditions for index usage
    optimized = this.optimizeDateRangeConditions(optimized, sourceColumns);

    return optimized;
  }

  private optimizeDateRangeConditions(
    condition: string,
    sourceColumns: Array<{ name: string; dataType: PostgreSQLDataType }>,
  ): string {
    const dateColumns = sourceColumns.filter((col) =>
      [PostgreSQLDataType.DATE, PostgreSQLDataType.TIMESTAMP, PostgreSQLDataType.TIMESTAMPTZ].includes(
        col.dataType,
      ),
    );

    dateColumns.forEach((col) => {
      const rangePattern = new RegExp(
        `\\b${col.name}\\s*>=\\s*'([^']+)'\\s+AND\\s+${col.name}\\s*<=\\s*'([^']+)'`,
        'gi',
      );
      condition = condition.replace(rangePattern, `${col.name} BETWEEN '$1' AND '$2'`);
    });

    return condition;
  }

  private addPredicateTypeCasting(
    condition: string,
    sourceColumns: Array<{ name: string; dataType: PostgreSQLDataType }>,
  ): string {
    let casted = condition;

    // Add explicit type casting for safe comparisons
    sourceColumns.forEach((col) => {
      const comparisonPattern = new RegExp(
        `(${col.name})\\s*([=<>!]+)\\s*('[^']*'|\\d+)`,
        'gi',
      );

      let match: RegExpExecArray | null;
      while ((match = comparisonPattern.exec(condition)) !== null) {
        const [fullMatch, column, operator, value] = match;

        // Add type casting for string comparisons to dates/timestamps
        if (
          (col.dataType === PostgreSQLDataType.DATE || col.dataType === PostgreSQLDataType.TIMESTAMP) &&
          value.startsWith("'")
        ) {
          const castedValue = value.replace(/'/g, '') + `'::${col.dataType.toLowerCase()}`;
          const replacement = `${column} ${operator} ${castedValue}`;
          casted = casted.replace(fullMatch, replacement);
        }
      }
    });

    return casted;
  }

  private addIndexHints(sql: string): string {
    // PostgreSQL doesn't support index hints like MySQL, but we can add comments
    const indexHint = '/*+ IndexScan(source_table) */';

    if (sql.startsWith('SELECT')) {
      return sql.replace('SELECT', `SELECT ${indexHint}`);
    }

    return sql;
  }

  private generateFilterPerformanceHints(
    sourceColumns: Array<{ name: string; dataType: PostgreSQLDataType }>,
    _filterConfig: FilterConfig,
    whereClause: string,
  ): string[] {
    const hints: string[] = [];

    // Check for expensive LIKE patterns
    const likePatterns = whereClause.match(/LIKE\s+'[^']*%[^']*%[^']*'/gi);
    if (likePatterns) {
      hints.push('LIKE patterns with leading and trailing wildcards cannot use indexes efficiently');
    }

    // Check for OR conditions that might benefit from UNION
    const orCount = (whereClause.match(/\bOR\b/gi) || []).length;
    if (orCount > 2) {
      hints.push('Multiple OR conditions may benefit from UNION or index-only scans');
    }

    // Check for functions on indexed columns
    const functionPattern = /\b(UPPER|LOWER|SUBSTRING|EXTRACT|DATE_TRUNC)\([^)]+\)/gi;
    if (functionPattern.test(whereClause)) {
      hints.push('Functions on indexed columns prevent index usage; consider functional indexes');
    }

    // Check for implicit type conversions
    const implicitConversionPattern = /(\w+)\s*=\s*'(\d+)'/g;
    let match: RegExpExecArray | null;
    while ((match = implicitConversionPattern.exec(whereClause)) !== null) {
      const [, column, value] = match;
      const sourceCol = sourceColumns.find((c) => c.name === column);
      if (sourceCol && sourceCol.dataType !== PostgreSQLDataType.VARCHAR) {
        hints.push(`Implicit type conversion in ${column} = '${value}' may impact performance`);
      }
    }

    return hints;
  }

  // ==================== VALIDATION METHODS ====================

  private validateFilterConfig(
    sourceColumns: Array<{ name: string; dataType: PostgreSQLDataType }>,
    filterConfig: FilterConfig,
    errors: SQLGenerationError[],
    warnings: string[],
  ): void {
    globalLogger.debug(`[FilterSQLGenerator] validateFilterConfig called`);

    // Validate condition syntax
    const validation = this.validatePredicate(filterConfig.condition, sourceColumns);
    if (!validation.valid) {
      validation.errors.forEach((error) => {
        errors.push({
          code: 'INVALID_CONDITION',
          message: error,
          severity: 'ERROR' as const,
        });
      });
      globalLogger.warn(`[FilterSQLGenerator] Invalid condition: ${validation.errors.join(', ')}`);
    }

    // Check for SQL injection patterns
    if (this.detectSQLInjectionPatterns(filterConfig.condition)) {
      errors.push({
        code: 'POTENTIAL_SQL_INJECTION',
        message: 'Condition contains potential SQL injection patterns',
        severity: 'ERROR' as const,
        suggestion: 'Use parameterized queries',
      });
      globalLogger.warn(`[FilterSQLGenerator] Potential SQL injection in condition: ${filterConfig.condition}`);
    }

    // Validate parameters
    if (filterConfig.parameters) {
      Object.entries(filterConfig.parameters).forEach(([key, value]) => {
        if (value === undefined || value === null) {
          warnings.push(`Parameter "${key}" has null/undefined value`);
          globalLogger.warn(`[FilterSQLGenerator] Parameter ${key} is null/undefined`);
        }
      });
    }
  }

/**
 * Validates a predicate (WHERE clause condition) against the known source columns.
 * Returns an object with `valid` flag and an array of error messages.
 * This version strips table qualifiers (e.g., "R.status" -> "status") before validation.
 */
private validatePredicate(
  predicate: string,
  sourceColumns: Array<{ name: string; dataType: PostgreSQLDataType }>
): { valid: boolean; errors: string[] } {
  const errors: string[] = [];

  if (!predicate || predicate.trim() === '') {
    errors.push('Predicate cannot be empty');
    return { valid: false, errors };
  }

  // --- NEW: Strip table aliases (e.g., "R.", "cte_R.") from the predicate ---
  // This must be done before removing string literals to avoid breaking quoted strings.
  // We'll do it on the original predicate, but we need to preserve string literals temporarily.
  // Simpler: Remove any word followed by a dot, but only outside of quotes.
  // For simplicity, we assume no dots inside string literals (unlikely in a filter condition).
  let aliasStripped = predicate.replace(/\b\w+\./g, '');

  // Now remove string and numeric literals to avoid false positives
  let cleanedPredicate = aliasStripped
    .replace(/'[^']*'/g, '')           // remove single-quoted strings
    .replace(/\b\d+(\.\d+)?\b/g, '')   // remove numeric literals
    .replace(/[=<>!]+|AND|OR|NOT|IN|BETWEEN|LIKE|IS/g, ' ')
    .replace(/[(),]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  // Check for balanced parentheses in original predicate
  const openParens = (predicate.match(/\(/g) || []).length;
  const closeParens = (predicate.match(/\)/g) || []).length;
  if (openParens !== closeParens) {
    errors.push('Unbalanced parentheses in predicate');
  }

  // Check for dangerous SQL keywords
  const invalidKeywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'CREATE', 'ALTER'];
  invalidKeywords.forEach((keyword) => {
    if (new RegExp(`\\b${keyword}\\s+TABLE\\b`, 'i').test(predicate)) {
      errors.push(`Predicate contains potentially dangerous SQL keyword: ${keyword}`);
    }
  });

  // Extract identifiers from the cleaned predicate (now without table aliases)
  const columnPattern = /\b([a-zA-Z_][a-zA-Z0-9_]*)\b/g;
  let match: RegExpExecArray | null;
  while ((match = columnPattern.exec(cleanedPredicate)) !== null) {
    const columnName = match[1];

    // Skip SQL keywords and common functions
    if (this.reservedKeywords.has(columnName.toUpperCase())) {
      continue;
    }
    const commonFunctions = ['COALESCE', 'NULLIF', 'CAST', 'EXTRACT', 'DATE_TRUNC'];
    if (commonFunctions.includes(columnName.toUpperCase())) {
      continue;
    }

    // Check if it's a known column
    if (!sourceColumns.some((c) => c.name === columnName)) {
      // Report the original column reference (from the original predicate) for clarity
      // but the error is still correct.
      errors.push(`Unknown column reference: "${columnName}"`);
    }
  }

  return { valid: errors.length === 0, errors };
}

  private detectSQLInjectionPatterns(condition: string): boolean {
    const injectionPatterns = [
      /;\s*(DROP|DELETE|UPDATE|INSERT|CREATE|ALTER)/i,
      /UNION\s+ALL/i,
      /OR\s+['"]?1['"]?\s*=\s*['"]?1['"]?/i,
      /EXEC\s*\(/i,
      /xp_cmdshell/i,
    ];

    return injectionPatterns.some((pattern) => pattern.test(condition));
  }

  private checkFilterPerformance(
    condition: string,
    _sourceColumns: Array<{ name: string; dataType: PostgreSQLDataType }>,
    warnings: string[],
  ): void {
    // Check for full table scan patterns
    const fullScanPatterns = [
      /\bLIKE\s+'%[^']*%'/i, // Leading wildcard
      /IS NOT NULL/i, // May require full scan without index
      /<>/i, // Not equal may require full scan
      /OR\b.*\bOR\b/i, // Multiple ORs
    ];

    fullScanPatterns.forEach((pattern) => {
      if (pattern.test(condition)) {
        warnings.push('Condition may cause full table scan; consider adding appropriate indexes');
      }
    });

    // Check for expensive string functions
    const expensiveFunctions = ['SUBSTRING', 'UPPER', 'LOWER', 'REPLACE'];
    expensiveFunctions.forEach((func) => {
      if (new RegExp(`\\b${func}\\([^)]*\\)`, 'i').test(condition)) {
        warnings.push(`Function ${func} in WHERE clause may impact performance`);
      }
    });
  }

  // ==================== HELPER METHODS ====================

  private extractSourceTableDependencies(
    sourceColumns: Array<{ name: string; dataType: PostgreSQLDataType }>,
  ): string[] {
    const tables = new Set<string>();

    sourceColumns.forEach((col) => {
      const parts = col.name.split('.');
      if (parts.length === 2) {
        tables.add(parts[0]);
      }
    });

    return Array.from(tables);
  }

  private transformPredicateToPostgreSQL(predicate: string): string {
    return predicate
      .replace(/\bISNULL\(/gi, 'COALESCE(')
      .replace(/\bLEN\(/gi, 'LENGTH(')
      .replace(/\bGETDATE\(\)/gi, 'CURRENT_TIMESTAMP')
      .replace(/\bCONVERT\(/gi, 'CAST(')
      .replace(/\[/g, '"')
      .replace(/\]/g, '"')
      .replace(/N'([^']*)'/g, "'$1'"); // Remove N prefix from strings
  }

  private buildFilterSelect(
    sourceColumns: Array<{ name: string; dataType: PostgreSQLDataType }>,
    whereClause: string,
  ): string {
    const columnList = sourceColumns.map((col) => this.sanitizeIdentifier(col.name)).join(', ');

    if (columnList) {
      return `SELECT\n    ${columnList}\nFROM source_table\nWHERE ${whereClause}`;
    } else {
      return `SELECT *\nFROM source_table\nWHERE ${whereClause}`;
    }
  }

  private generateOrderByFromSortConfig(sortConfig: SortConfig): GeneratedSQLFragment {
    const orderByClauses = sortConfig.columns.map((col) => {
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

  private addFilterComments(sql: string, node: UnifiedCanvasNode): string {
    const condition = (node.metadata?.filterConfig as FilterConfig)?.condition || node.metadata?.filterCondition || '1=1';
    const comments: string[] = [
      `-- Filter node: ${node.name} (${node.id})`,
      `-- Condition: ${condition}`,
    ];
    if (node.metadata?.description) {
      comments.push(`-- Description: ${node.metadata.description}`);
    }
    comments.push('');
    return comments.join('\n') + sql;
  }

  private createEmptyFragment(fragmentType: string): GeneratedSQLFragment {
    return {
      sql: '',
      dependencies: [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType,
        lineCount: 0,
      },
    };
  }

  private errorFragment(
    fragmentType: string,
    errors: SQLGenerationError[],
    warnings: string[] = [],
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

  protected emptyFragment(): GeneratedSQLFragment {
    return this.createEmptyFragment('empty');
  }
}