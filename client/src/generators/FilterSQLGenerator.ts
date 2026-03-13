// src/generators/FilterSQLGenerator.ts

import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment, SQLGenerationError } from './BaseSQLGenerator';
import { CanvasNode, FilterConfig, PostgreSQLDataType, SortConfig } from '../types/pipeline-types';

/**
 * PostgreSQL FILTER SQL Generator
 * Handles WHERE clause generation, complex predicates, and parameterized queries
 */
export class FilterSQLGenerator extends BaseSQLGenerator {
  // ==================== TEMPLATE METHOD IMPLEMENTATIONS ====================

  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    
    return {
      sql: 'SELECT *',
      dependencies: this.extractSourceDependencies(node),
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'filter_select',
        lineCount: 1
      }
    };
  }

  protected generateJoinConditions(_context: SQLGenerationContext): GeneratedSQLFragment {
    // FILTER nodes typically don't have JOIN conditions
    return this.emptyFragment('join_conditions');
  }

  protected generateWhereClause(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    const filterConfig = node.metadata?.filterConfig;
    
    if (!filterConfig) {
      return this.emptyFragment('where_clause');
    }

    const whereClause = this.buildWhereClause(filterConfig, node.metadata?.tableMapping?.columns || []);
    
    return {
      sql: `WHERE ${whereClause.sql}`,
      dependencies: [],
      parameters: whereClause.parameters,
      errors: whereClause.errors,
      warnings: whereClause.warnings,
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'filter_where',
        lineCount: 1
      }
    };
  }

  protected generateHavingClause(_context: SQLGenerationContext): GeneratedSQLFragment {
    // FILTER nodes use WHERE, not HAVING
    return this.emptyFragment('having_clause');
  }

  protected generateOrderByClause(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    
    if (node.metadata?.sortConfig) {
      return this.generateOrderByFromSortConfig(node.metadata.sortConfig);
    }
    
    return this.emptyFragment('order_by_clause');
  }

  protected generateGroupByClause(_context: SQLGenerationContext): GeneratedSQLFragment {
    // FILTER nodes typically don't use GROUP BY
    return this.emptyFragment('group_by_clause');
  }

  // ==================== FILTER-SPECIFIC METHODS ====================

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
    } = {}
  ): GeneratedSQLFragment {
    const errors: SQLGenerationError[] = [];
    const warnings: string[] = [];

    // Validate filter configuration
    this.validateFilterConfig(sourceColumns, filterConfig, errors, warnings);

    if (errors.length > 0) {
      return this.errorFragment('filter_generation', errors, warnings);
    }

    // Build WHERE clause
    const whereResult = this.buildWhereClause(filterConfig, sourceColumns, options);
    
    // Generate complete SQL
    const sql = this.buildFilterSelect(sourceColumns, whereResult.sql);
    
    // Add performance hints
    const performanceHints = this.generateFilterPerformanceHints(sourceColumns, filterConfig, whereResult.sql);
    warnings.push(...performanceHints);

    // Optimize SQL
    const optimizedSql = this.optimizeFilterSQL(sql, filterConfig, options);

    return {
      sql: optimizedSql,
      dependencies: this.extractSourceTableDependencies(sourceColumns),
      parameters: whereResult.parameters,
      errors: [...errors, ...whereResult.errors],
      warnings: [...warnings, ...whereResult.warnings],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'complete_filter',
        lineCount: optimizedSql.split('\n').length
      }
    };
  }

  /**
   * Generate parameterized WHERE clause for prepared statements
   */
  public generateParameterizedWhereClause(
    condition: string,
    parameters: Record<string, any>,
    sourceColumns: Array<{ name: string; dataType: PostgreSQLDataType }>
  ): { sql: string; paramList: Array<{ name: string; value: any; type: PostgreSQLDataType }> } {
    const paramList: Array<{ name: string; value: any; type: PostgreSQLDataType }> = [];
    let sql = condition;
    let paramIndex = 1;
    
    // Replace parameter placeholders with PostgreSQL positional parameters
    Object.entries(parameters).forEach(([key, value]) => {
      const paramPattern = new RegExp(`:${key}\\b`, 'g');
      const column = sourceColumns.find(c => c.name === key);
      const paramType = column?.dataType || PostgreSQLDataType.VARCHAR;
      
      if (paramPattern.test(sql)) {
        sql = sql.replace(paramPattern, `$${paramIndex}`);
        paramList.push({ name: key, value, type: paramType });
        paramIndex++;
      }
    });
    
    // Replace ? placeholders with positional parameters
    const questionMarkPattern = /\?/g;
    let match: RegExpExecArray | null;
    let qmIndex = paramIndex;
    
    while ((match = questionMarkPattern.exec(condition)) !== null) {
      sql = sql.replace('?', `$${qmIndex}`);
      qmIndex++;
    }
    
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
    } = {}
  ): { sql: string; errors: string[] } {
    const errors: string[] = [];
    
    // Validate predicate syntax
    const validation = this.validatePredicate(predicate, sourceColumns);
    if (!validation.valid) {
      errors.push(...validation.errors);
      return { sql: '', errors };
    }
    
    // Transform to PostgreSQL syntax
    let sql = this.transformPredicateToPostgreSQL(predicate);
    
    // Apply optimizations
    if (options.optimizeForNulls) {
      sql = this.optimizeNullComparisons(sql);
    }
    
    if (options.useBooleanLogic) {
      sql = this.optimizeBooleanLogic(sql);
    }
    
    if (options.addTypeCasting) {
      sql = this.addPredicateTypeCasting(sql, sourceColumns);
    }
    
    // Optimize for index usage
    sql = this.optimizeForIndexUsage(sql, sourceColumns);
    
    return { sql, errors };
  }

  // ==================== PREDICATE BUILDING METHODS ====================

  private buildWhereClause(
    filterConfig: FilterConfig,
    sourceColumns: Array<{ name: string; dataType: PostgreSQLDataType }>,
    options?: { parameterize?: boolean; usePreparedStatements?: boolean; optimizeForIndex?: boolean }
  ): { 
    sql: string; 
    parameters: Map<string, any>;
    errors: SQLGenerationError[];
    warnings: string[]; 
  } {
    const errors: SQLGenerationError[] = [];
    const warnings: string[] = [];
    const parameters = new Map<string, any>();
    
    let condition = filterConfig.condition;
    
    // Validate condition syntax
    const validation = this.validatePredicate(condition, sourceColumns);
    if (!validation.valid) {
      errors.push(...validation.errors.map(msg => ({
        code: 'INVALID_PREDICATE',
        message: msg,
        severity: 'ERROR' as const
      })));
    }
    
    // Transform to PostgreSQL syntax
    condition = this.transformPredicateToPostgreSQL(condition);
    
    // Apply operation (INCLUDE/EXCLUDE)
    if (filterConfig.operation === 'EXCLUDE') {
      condition = `NOT (${condition})`;
    }
    
    // Parameterize if requested
    if (options?.parameterize && filterConfig.parameters) {
      const paramResult = this.parameterizeCondition(condition, filterConfig.parameters, sourceColumns);
      condition = paramResult.sql;
      paramResult.parameters.forEach((value, key) => parameters.set(key, value));
    }
    
    // Optimize for index usage
    if (options?.optimizeForIndex) {
      condition = this.optimizeForIndexUsage(condition, sourceColumns);
    }
    
    // Add type casting for safe comparisons
    condition = this.addPredicateTypeCasting(condition, sourceColumns);
    
    // Optimize NULL comparisons
    condition = this.optimizeNullComparisons(condition);
    
    // Optimize boolean logic
    condition = this.optimizeBooleanLogic(condition);
    
    // Check for potential performance issues
    this.checkFilterPerformance(condition, sourceColumns, warnings);
    
    return {
      sql: condition,
      parameters,
      errors,
      warnings
    };
  }

  private parameterizeCondition(
    condition: string,
    parameters: Record<string, any>,
    _sourceColumns: Array<{ name: string; dataType: PostgreSQLDataType }>
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
        paramIndex++;
      }
      
      // Replace ? placeholders
      const questionMarkPattern = /\?/g;
      if (questionMarkPattern.test(sql) && paramMap.size < Object.keys(parameters).length) {
        sql = sql.replace('?', `$${paramIndex}`);
        paramMap.set(`$${paramIndex}`, value);
        paramIndex++;
      }
    });
    
    return { sql, parameters: paramMap };
  }

  // ==================== OPTIMIZATION METHODS ====================

  private optimizeFilterSQL(
    sql: string,
    filterConfig: FilterConfig,
    options: { optimizeForIndex?: boolean }
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
      .replace(/NOT\s*\(\s*([^)]+)\s*AND\s*([^)]+)\s*\)/gi, '(NOT $1 OR NOT $2)')
      .replace(/NOT\s*\(\s*([^)]+)\s*OR\s*([^)]+)\s*\)/gi, '(NOT $1 AND NOT $2)')
      .replace(/\(([^)]+)\s*AND\s*TRUE\)/gi, '$1')
      .replace(/\(([^)]+)\s*OR\s*FALSE\)/gi, '$1')
      .replace(/TRUE\s*AND\s*([^)]+)/gi, '$1')
      .replace(/FALSE\s*OR\s*([^)]+)/gi, '$1');
  }

  private optimizeForIndexUsage(condition: string, sourceColumns: Array<{ name: string; dataType: PostgreSQLDataType }>): string {
    let optimized = condition;
    
    // Transform OR conditions to UNION for better index usage
    const orPattern = /\(([^)]+)\s*OR\s*([^)]+)\)/g;
    const orMatches = [...condition.matchAll(orPattern)];
    
    if (orMatches.length > 0 && this.featureSupport.supports.commonTableExpressions) {
      // Consider transforming to UNION for complex OR conditions
      // This would be implemented based on query planner analysis
    }
    
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

  private optimizeDateRangeConditions(condition: string, sourceColumns: Array<{ name: string; dataType: PostgreSQLDataType }>): string {
    const dateColumns = sourceColumns.filter(col => 
      [PostgreSQLDataType.DATE, PostgreSQLDataType.TIMESTAMP, PostgreSQLDataType.TIMESTAMPTZ].includes(col.dataType)
    );
    
    dateColumns.forEach(col => {
      const rangePattern = new RegExp(`\\b${col.name}\\s*>=\\s*'([^']+)'\\s+AND\\s+${col.name}\\s*<=\\s*'([^']+)'`, 'gi');
      condition = condition.replace(rangePattern, `${col.name} BETWEEN '$1' AND '$2'`);
    });
    
    return condition;
  }

  private addPredicateTypeCasting(condition: string, sourceColumns: Array<{ name: string; dataType: PostgreSQLDataType }>): string {
    let casted = condition;
    
    // Add explicit type casting for safe comparisons
    sourceColumns.forEach(col => {
      const comparisonPattern = new RegExp(`(${col.name})\\s*([=<>!]+)\\s*('[^']*'|\\d+)`, 'gi');
      
      let match: RegExpExecArray | null;
      while ((match = comparisonPattern.exec(condition)) !== null) {
        const [fullMatch, column, operator, value] = match;
        
        // Add type casting for string comparisons to dates/timestamps
        if ((col.dataType === PostgreSQLDataType.DATE || 
             col.dataType === PostgreSQLDataType.TIMESTAMP) &&
            value.startsWith("'")) {
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
    whereClause: string
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
      const sourceCol = sourceColumns.find(c => c.name === column);
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
    warnings: string[]
  ): void {
    // Validate condition syntax
    const validation = this.validatePredicate(filterConfig.condition, sourceColumns);
    if (!validation.valid) {
      validation.errors.forEach(error => {
        errors.push({
          code: 'INVALID_CONDITION',
          message: error,
          severity: 'ERROR' as const,
          // Note: Removed 'field' property as it's not in SQLGenerationError interface
        });
      });
    }
    
    // Check for SQL injection patterns
    if (this.detectSQLInjectionPatterns(filterConfig.condition)) {
      errors.push({
        code: 'POTENTIAL_SQL_INJECTION',
        message: 'Condition contains potential SQL injection patterns',
        severity: 'ERROR' as const,
        suggestion: 'Use parameterized queries'
      });
    }
    
    // Validate parameters
    if (filterConfig.parameters) {
      Object.entries(filterConfig.parameters).forEach(([key, value]) => {
        if (value === undefined || value === null) {
          warnings.push(`Parameter "${key}" has null/undefined value`);
        }
      });
    }
  }

  private validatePredicate(
    predicate: string,
    sourceColumns: Array<{ name: string; dataType: PostgreSQLDataType }>
  ): { valid: boolean; errors: string[] } {
    const errors: string[] = [];
    
    if (!predicate || predicate.trim() === '') {
      errors.push('Predicate cannot be empty');
      return { valid: false, errors };
    }
    
    // Check for balanced parentheses
    const openParens = (predicate.match(/\(/g) || []).length;
    const closeParens = (predicate.match(/\)/g) || []).length;
    
    if (openParens !== closeParens) {
      errors.push('Unbalanced parentheses in predicate');
    }
    
    // Check for invalid SQL keywords
    const invalidKeywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'CREATE', 'ALTER'];
    invalidKeywords.forEach(keyword => {
      if (new RegExp(`\\b${keyword}\\s+TABLE\\b`, 'i').test(predicate)) {
        errors.push(`Predicate contains potentially dangerous SQL keyword: ${keyword}`);
      }
    });
    
    // Check for valid column references
    const columnPattern = /\b([a-zA-Z_][a-zA-Z0-9_]*)\b/g;
    let match: RegExpExecArray | null;
    
    while ((match = columnPattern.exec(predicate)) !== null) {
      const columnName = match[1];
      
      // Skip SQL keywords and functions
      if (this.reservedKeywords.has(columnName.toUpperCase())) {
        continue;
      }
      
      // Skip common functions
      const commonFunctions = ['COALESCE', 'NULLIF', 'CAST', 'EXTRACT', 'DATE_TRUNC'];
      if (commonFunctions.includes(columnName.toUpperCase())) {
        continue;
      }
      
      // Check if it's a known column
      if (!sourceColumns.some(c => c.name === columnName)) {
        errors.push(`Unknown column reference: "${columnName}"`);
      }
    }
    
    return {
      valid: errors.length === 0,
      errors
    };
  }

  private detectSQLInjectionPatterns(condition: string): boolean {
    const injectionPatterns = [
      /;\s*(DROP|DELETE|UPDATE|INSERT|CREATE|ALTER)/i,
      /UNION\s+ALL/i,
      /OR\s+['"]?1['"]?\s*=\s*['"]?1['"]?/i,
      /EXEC\s*\(/i,
      /xp_cmdshell/i
    ];
    
    return injectionPatterns.some(pattern => pattern.test(condition));
  }

  private checkFilterPerformance(
    condition: string,
    _sourceColumns: Array<{ name: string; dataType: PostgreSQLDataType }>,
    warnings: string[]
  ): void {
    // Check for full table scan patterns
    const fullScanPatterns = [
      /\bLIKE\s+'%[^']*%'/i, // Leading wildcard
      /IS NOT NULL/i, // May require full scan without index
      /<>/i, // Not equal may require full scan
      /OR\b.*\bOR\b/i // Multiple ORs
    ];
    
    fullScanPatterns.forEach(pattern => {
      if (pattern.test(condition)) {
        warnings.push('Condition may cause full table scan; consider adding appropriate indexes');
      }
    });
    
    // Check for expensive string functions
    const expensiveFunctions = ['SUBSTRING', 'UPPER', 'LOWER', 'REPLACE'];
    expensiveFunctions.forEach(func => {
      if (new RegExp(`\\b${func}\\([^)]*\\)`, 'i').test(condition)) {
        warnings.push(`Function ${func} in WHERE clause may impact performance`);
      }
    });
  }

  // ==================== HELPER METHODS ====================

  private extractSourceDependencies(node: CanvasNode): string[] {
    const dependencies: string[] = [];
    
    if (node.metadata?.tableMapping) {
      dependencies.push(`${node.metadata.tableMapping.schema}.${node.metadata.tableMapping.name}`);
    }
    
    return dependencies;
  }

  private extractSourceTableDependencies(
    sourceColumns: Array<{ name: string; dataType: PostgreSQLDataType }>
  ): string[] {
    const tables = new Set<string>();
    
    sourceColumns.forEach(col => {
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
    whereClause: string
  ): string {
    const columnList = sourceColumns.map(col => this.sanitizeIdentifier(col.name)).join(', ');
    
    if (columnList) {
      return `SELECT\n    ${columnList}\nFROM source_table\nWHERE ${whereClause}`;
    } else {
      return `SELECT *\nFROM source_table\nWHERE ${whereClause}`;
    }
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
}