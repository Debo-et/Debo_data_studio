// src/generators/BaseSQLGenerator.ts
import {
  UnifiedCanvasNode,
  UnifiedCanvasConnection,
  PostgreSQLDataType,
  NodeType,
  PostgresColumn} from '../types/unified-pipeline.types';
import { ConnectionValidationResult } from '../utils/connection-validator';

// ==================== TYPES & INTERFACES ====================

export interface SQLGenerationContext {
  node: UnifiedCanvasNode;                     // now using unified node
  connection?: UnifiedCanvasConnection;         // unified connection (may still be CanvasConnection in some places)
  validationResult?: ConnectionValidationResult;
  indentLevel: number;
  parameters: Map<string, any>;
  options: SQLGenerationOptions;
}

export interface SQLGenerationOptions {
  includeComments: boolean;
  formatSQL: boolean;
  targetDialect: 'POSTGRESQL' | 'MYSQL' | 'SQLSERVER' | 'ORACLE';
  postgresVersion: string; // e.g., '14.0', '15.0'
  useCTEs: boolean;
  optimizeForReadability: boolean;
  includeExecutionPlan: boolean;
  parameterizeValues: boolean;
  maxLineLength: number;
}

export interface GeneratedSQLFragment {
  sql: string;
  dependencies: string[];
  parameters: Map<string, any>;
  errors: SQLGenerationError[];
  warnings: string[];
  metadata: {
    generatedAt: string;
    fragmentType: string;
    lineCount: number;
    [key: string]: any;
  };
}

export interface SQLGenerationError {
  code: string;
  message: string;
  severity: 'ERROR' | 'WARNING' | 'INFO';
  line?: number;
  column?: number;
  suggestion?: string;
  field?: string;
}

export interface CTEChain {
  name: string;
  query: string;
  materialized: boolean;
  columns: string[];
}

export interface PostgreSQLFeatureSupport {
  version: string;
  supports: {
    generatedColumns: boolean;
    identityColumns: boolean;
    jsonPathQueries: boolean;
    sqlJSON: boolean;
    multirangeTypes: boolean;
    storedGeneratedColumns: boolean;
    tableAccessMethod: boolean;
    recursiveCTE: boolean;
    windowFunctions: boolean;
    commonTableExpressions: boolean;
    jsonb: boolean;
    upsert: boolean;
    fullTextSearch: boolean;
    xml: boolean;
    filteredAggregates: boolean;
    groupingExtensions: boolean;
    [key: string]: boolean;
  };
}

// ==================== ABSTRACT BASE CLASS ====================

export abstract class BaseSQLGenerator {
  protected readonly postgresVersion: string;
  protected readonly featureSupport: PostgreSQLFeatureSupport;
  protected readonly reservedKeywords = new Set([
    'ALL', 'ANALYSE', 'ANALYZE', 'AND', 'ANY', 'ARRAY', 'AS', 'ASC',
    'ASYMMETRIC', 'AUTHORIZATION', 'BINARY', 'BOTH', 'CASE', 'CAST',
    'CHECK', 'COLLATE', 'COLUMN', 'CONCURRENTLY', 'CONSTRAINT', 'CREATE',
    'CROSS', 'CURRENT_CATALOG', 'CURRENT_DATE', 'CURRENT_ROLE',
    'CURRENT_SCHEMA', 'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'CURRENT_USER',
    'DEFAULT', 'DEFERRABLE', 'DESC', 'DISTINCT', 'DO', 'ELSE', 'END',
    'EXCEPT', 'FALSE', 'FETCH', 'FOR', 'FOREIGN', 'FREEZE', 'FROM',
    'FULL', 'GRANT', 'GROUP', 'HAVING', 'ILIKE', 'IN', 'INITIALLY',
    'INNER', 'INTERSECT', 'INTO', 'IS', 'ISNULL', 'JOIN', 'LATERAL',
    'LEADING', 'LEFT', 'LIKE', 'LIMIT', 'LOCALTIME', 'LOCALTIMESTAMP',
    'NATURAL', 'NOT', 'NOTNULL', 'NULL', 'OFFSET', 'ON', 'ONLY', 'OR',
    'ORDER', 'OUTER', 'OVERLAPS', 'PLACING', 'PRIMARY', 'REFERENCES',
    'RETURNING', 'RIGHT', 'SELECT', 'SESSION_USER', 'SIMILAR', 'SOME',
    'SYMMETRIC', 'TABLE', 'THEN', 'TO', 'TRAILING', 'TRUE', 'UNION',
    'UNIQUE', 'USER', 'USING', 'VARIADIC', 'VERBOSE', 'WHEN', 'WHERE',
    'WINDOW', 'WITH'
  ]);

  constructor(options: Partial<SQLGenerationOptions> = {}) {
    this.postgresVersion = options.postgresVersion || '14.0';
    this.featureSupport = this.detectPostgreSQLFeatures(this.postgresVersion);
  }

  // ==================== ABSTRACT TEMPLATE METHODS ====================
  protected abstract generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment;
  protected abstract generateJoinConditions(context: SQLGenerationContext): GeneratedSQLFragment;
  protected abstract generateWhereClause(context: SQLGenerationContext): GeneratedSQLFragment;
  protected abstract generateHavingClause(context: SQLGenerationContext): GeneratedSQLFragment;
  protected abstract generateOrderByClause(context: SQLGenerationContext): GeneratedSQLFragment;
  protected abstract generateGroupByClause(context: SQLGenerationContext): GeneratedSQLFragment;

  // ==================== CORE GENERATION METHODS ====================
  public generateSQL(context: SQLGenerationContext): GeneratedSQLFragment {
    const errors: SQLGenerationError[] = [];
    const warnings: string[] = [];
    const parameters = new Map<string, any>();
    const dependencies: string[] = [];

    try {
      this.validatePostgreSQLCompatibility(context, errors, warnings);

      const selectFragment = this.generateSelectStatement(context);
      const joinFragment = this.generateJoinConditions(context);
      const whereFragment = this.generateWhereClause(context);
      const havingFragment = this.generateHavingClause(context);
      const orderByFragment = this.generateOrderByClause(context);
      const groupByFragment = this.generateGroupByClause(context);

      const fragments = [selectFragment, joinFragment, whereFragment, havingFragment, orderByFragment, groupByFragment];
      
      fragments.forEach(fragment => {
        dependencies.push(...fragment.dependencies);
        fragment.parameters.forEach((value, key) => parameters.set(key, value));
        errors.push(...fragment.errors);
        warnings.push(...fragment.warnings);
      });

      let sql = this.assembleSQL(
        selectFragment.sql,
        joinFragment.sql,
        whereFragment.sql,
        groupByFragment.sql,
        havingFragment.sql,
        orderByFragment.sql,
        context
      );

      sql = this.optimizeForPostgreSQL(sql, context);

      if (context.options.formatSQL) {
        sql = this.formatSQL(sql, context.options.maxLineLength);
      }

      if (context.options.includeComments) {
        sql = this.addComments(sql, context);
      }

      const syntaxErrors = this.validateSQLSyntax(sql);
      errors.push(...syntaxErrors);

      return {
        sql,
        dependencies: [...new Set(dependencies)],
        parameters,
        errors,
        warnings,
        metadata: {
          generatedAt: new Date().toISOString(),
          fragmentType: 'complete_sql',
          lineCount: sql.split('\n').length
        }
      };

    } catch (error) {
      errors.push({
        code: 'GENERATION_FAILED',
        message: error instanceof Error ? error.message : 'SQL generation failed',
        severity: 'ERROR',
        suggestion: 'Check node configuration and dependencies'
      });

      return {
        sql: '',
        dependencies,
        parameters,
        errors,
        warnings,
        metadata: {
          generatedAt: new Date().toISOString(),
          fragmentType: 'error',
          lineCount: 0
        }
      };
    }
  }

  public generateCTEChain(
    ctes: CTEChain[],
    options: { materialized?: boolean; recursive?: boolean } = {}
  ): GeneratedSQLFragment {
    const errors: SQLGenerationError[] = [];
    const warnings: string[] = [];

    if (ctes.length === 0) {
      return {
        sql: '',
        dependencies: [],
        parameters: new Map(),
        errors,
        warnings,
        metadata: { generatedAt: new Date().toISOString(), fragmentType: 'cte_chain', lineCount: 0 }
      };
    }

    if (options.recursive && !this.featureSupport.supports.recursiveCTE) {
      warnings.push('Recursive CTEs require PostgreSQL 8.4+');
    }

    const cteDefinitions = ctes.map((cte, _index) => {
      const materializedClause = options.materialized ? 'MATERIALIZED ' : '';
      const columnList = cte.columns.length > 0 
        ? ` (${cte.columns.map(col => this.sanitizeIdentifier(col)).join(', ')})`
        : '';
      return `${this.sanitizeIdentifier(cte.name)}${columnList} AS ${materializedClause}(\n${this.indent(cte.query, 2)}\n)`;
    });

    const recursiveKeyword = options.recursive ? 'RECURSIVE ' : '';
    const sql = `WITH ${recursiveKeyword}${cteDefinitions.join(',\n')}`;

    return {
      sql,
      dependencies: [],
      parameters: new Map(),
      errors,
      warnings,
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'cte_chain', lineCount: sql.split('\n').length }
    };
  }

  public generateTempTable(
    tableName: string,
    columns: PostgresColumn[],
    options: {
      onCommit?: 'PRESERVE_ROWS' | 'DELETE_ROWS' | 'DROP';
      unlogged?: boolean;
      withData?: boolean;
    } = {}
  ): GeneratedSQLFragment {
    const errors: SQLGenerationError[] = [];
    const warnings: string[] = [];

    if (!this.isValidIdentifier(tableName)) {
      errors.push({
        code: 'INVALID_TABLE_NAME',
        message: `Invalid table name: ${tableName}`,
        severity: 'ERROR',
        suggestion: 'Use alphanumeric characters and underscores only'
      });
    }

    const columnDefinitions = columns.map(column => {
      const parts = [
        this.sanitizeIdentifier(column.name),
        column.dataType
      ];

      if (column.length !== undefined) {
        parts[1] = `${parts[1]}(${column.length})`;
      }

      if (!column.nullable) {
        parts.push('NOT NULL');
      }

      if (column.defaultValue) {
        parts.push(`DEFAULT ${this.sanitizeValue(column.defaultValue)}`);
      }

      return parts.join(' ');
    });

    const tempKeyword = options.unlogged ? 'UNLOGGED' : 'TEMPORARY';
    const onCommitClause = options.onCommit 
      ? ` ON COMMIT ${options.onCommit === 'DROP' ? 'DROP' : options.onCommit === 'DELETE_ROWS' ? 'DELETE ROWS' : 'PRESERVE ROWS'}`
      : '';
    
    const sql = `CREATE ${tempKeyword} TABLE ${this.sanitizeIdentifier(tableName)} (\n  ${columnDefinitions.join(',\n  ')}\n)${onCommitClause};`;

    if (options.withData) {
      warnings.push('WITH DATA option is implied for temporary tables');
    }

    return {
      sql,
      dependencies: [],
      parameters: new Map(),
      errors,
      warnings,
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'temp_table', lineCount: sql.split('\n').length }
    };
  }

  // ==================== POSTGRESQL-SPECIFIC UTILITIES ====================
  public castToType(value: string, targetType: PostgreSQLDataType): string {
    if (!value.trim()) {
      return `NULL::${targetType}`;
    }

    switch (targetType) {
      case PostgreSQLDataType.JSON:
      case PostgreSQLDataType.JSONB:
        return `(${value})::${targetType}`;
      case PostgreSQLDataType.TIMESTAMPTZ:
        return `(${value})::timestamptz`;
      case PostgreSQLDataType.UUID:
        const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
        if (!uuidRegex.test(value.replace(/'/g, ''))) {
          throw new Error(`Invalid UUID format: ${value}`);
        }
        return `'${value.replace(/'/g, '')}'::uuid`;
      default:
        return `(${value})::${targetType}`;
    }
  }

  public generateArrayAggregation(
    column: string,
    options: {
      distinct?: boolean;
      orderBy?: string;
      filter?: string;
      delimiter?: string;
    } = {}
  ): string {
    const distinctClause = options.distinct ? 'DISTINCT ' : '';
    const orderByClause = options.orderBy ? ` ORDER BY ${options.orderBy}` : '';
    const filterClause = options.filter ? ` FILTER (WHERE ${options.filter})` : '';
    
    if (options.delimiter) {
      return `string_agg(${distinctClause}${column}::text, '${this.escapeString(options.delimiter)}'${orderByClause})${filterClause}`;
    }
    
    return `array_agg(${distinctClause}${column}${orderByClause})${filterClause}`;
  }

  public generateJSONAggregation(
    columns: Array<{ name: string; alias?: string }>,
    options: {
      aggregateAsObject?: boolean;
      includeNulls?: boolean;
      jsonb?: boolean;
    } = {}
  ): string {
    const jsonType = options.jsonb ? 'jsonb' : 'json';
    const nullHandling = options.includeNulls ? '' : ' WHERE column_name IS NOT NULL';
    
    if (options.aggregateAsObject) {
      const pairs = columns.map(col => {
        const key = col.alias || col.name;
        const value = col.name;
        return `'${this.escapeString(key)}', ${value}`;
      });
      return `jsonb_build_object(${pairs.join(', ')})::${jsonType}`;
    }
    
    const columnList = columns.map(col => col.name).join(', ');
    return `${jsonType}_agg(${columnList}${nullHandling})`;
  }

  public generateWindowFunction(
    functionName: string,
    argumentsList: string[],
    options: {
      partitionBy?: string[];
      orderBy?: Array<{ column: string; direction?: 'ASC' | 'DESC'; nulls?: 'FIRST' | 'LAST' }>;
      frame?: {
        type: 'ROWS' | 'RANGE' | 'GROUPS';
        start: 'UNBOUNDED PRECEDING' | 'CURRENT ROW' | number;
        end?: 'UNBOUNDED FOLLOWING' | 'CURRENT ROW' | number;
      };
    } = {}
  ): string {
    const args = argumentsList.length > 0 ? argumentsList.join(', ') : '';
    const funcCall = `${functionName}(${args})`;
    
    const windowParts: string[] = [];
    
    if (options.partitionBy && options.partitionBy.length > 0) {
      windowParts.push(`PARTITION BY ${options.partitionBy.join(', ')}`);
    }
    
    if (options.orderBy && options.orderBy.length > 0) {
      const orderClauses = options.orderBy.map(order => {
        const parts = [order.column];
        if (order.direction) parts.push(order.direction);
        if (order.nulls) parts.push(`NULLS ${order.nulls}`);
        return parts.join(' ');
      });
      windowParts.push(`ORDER BY ${orderClauses.join(', ')}`);
    }
    
    if (options.frame) {
      const frameStart = options.frame.start === 'CURRENT ROW' 
        ? 'CURRENT ROW' 
        : typeof options.frame.start === 'number'
          ? `${options.frame.start} PRECEDING`
          : options.frame.start;
      
      const frameEnd = options.frame.end
        ? (options.frame.end === 'CURRENT ROW'
            ? 'CURRENT ROW'
            : typeof options.frame.end === 'number'
              ? `${options.frame.end} FOLLOWING`
              : options.frame.end)
        : '';
      
      const frameClause = `${options.frame.type} BETWEEN ${frameStart}${frameEnd ? ` AND ${frameEnd}` : ''}`;
      windowParts.push(frameClause);
    }
    
    if (windowParts.length === 0) {
      return `${funcCall} OVER ()`;
    }
    
    return `${funcCall} OVER (${windowParts.join(' ')})`;
  }

  public optimizeCTE(cteChain: CTEChain[]): CTEChain[] {
    return cteChain.map(cte => {
      let optimizedQuery = cte.query;
      if (this.countCTEUsage(cte.name, cteChain) > 1) {
        cte.materialized = true;
      }
      optimizedQuery = this.optimizeSubqueries(optimizedQuery);
      optimizedQuery = this.cleanParentheses(optimizedQuery);
      return { ...cte, query: optimizedQuery };
    });
  }

  // ==================== SANITIZATION AND VALIDATION ====================
  public sanitizeIdentifier(identifier: string): string {
    if (!identifier) {
      throw new Error('Identifier cannot be empty');
    }
    
    const needsQuoting = 
      this.reservedKeywords.has(identifier.toUpperCase()) ||
      /^[0-9]/.test(identifier) ||
      /[^a-zA-Z0-9_]/.test(identifier) ||
      identifier.includes('-') ||
      identifier.includes(' ') ||
      identifier.toLowerCase() !== identifier;
    
    if (needsQuoting) {
      return `"${identifier.replace(/"/g, '""')}"`;
    }
    
    return identifier;
  }

  public sanitizeValue(value: any): string {
    if (value === null || value === undefined) {
      return 'NULL';
    }
    
    if (typeof value === 'boolean') {
      return value ? 'TRUE' : 'FALSE';
    }
    
    if (typeof value === 'number') {
      return value.toString();
    }
    
    if (typeof value === 'string') {
      return `'${this.escapeString(value)}'`;
    }
    
    if (value instanceof Date) {
      return `'${value.toISOString()}'::timestamptz`;
    }
    
    if (Array.isArray(value)) {
      const elements = value.map(v => this.sanitizeValue(v).replace(/^'|'$/g, ''));
      return `ARRAY[${elements.join(', ')}]`;
    }
    
    if (typeof value === 'object') {
      return `'${this.escapeString(JSON.stringify(value))}'::jsonb`;
    }
    
    throw new Error(`Unsupported value type: ${typeof value}`);
  }

  public validateSQLSyntax(sql: string): SQLGenerationError[] {
    const errors: SQLGenerationError[] = [];
    const lines = sql.split('\n');
    
    lines.forEach((line, index) => {
      const lineNumber = index + 1;
      const openParens = (line.match(/\(/g) || []).length;
      const closeParens = (line.match(/\)/g) || []).length;
      
      if (openParens !== closeParens) {
        errors.push({
          code: 'UNBALANCED_PARENTHESES',
          message: `Unbalanced parentheses in line ${lineNumber}`,
          severity: 'ERROR',
          line: lineNumber,
          suggestion: 'Check for missing ( or )'
        });
      }
      
      const singleQuotes = (line.match(/'/g) || []).length;
      const doubleQuotes = (line.match(/"/g) || []).length;
      
      if (singleQuotes % 2 !== 0) {
        errors.push({
          code: 'UNCLOSED_SINGLE_QUOTE',
          message: `Unclosed single quote in line ${lineNumber}`,
          severity: 'ERROR',
          line: lineNumber,
          suggestion: 'Add closing single quote'
        });
      }
      
      if (doubleQuotes % 2 !== 0) {
        errors.push({
          code: 'UNCLOSED_DOUBLE_QUOTE',
          message: `Unclosed double quote in line ${lineNumber}`,
          severity: 'ERROR',
          line: lineNumber,
          suggestion: 'Add closing double quote'
        });
      }
      
      if (line.trim().endsWith(';') && index !== lines.length - 1) {
        errors.push({
          code: 'EARLY_SEMICOLON',
          message: `Semicolon before end of statement in line ${lineNumber}`,
          severity: 'WARNING',
          line: lineNumber,
          suggestion: 'Move semicolon to last line'
        });
      }
    });
    
    return errors;
  }

  public validatePostgreSQLCompatibility(
    context: SQLGenerationContext,
    _errors: SQLGenerationError[],
    warnings: string[]
  ): void {
    const { node, options } = context;
    
    if (node.type === NodeType.JSON && !this.featureSupport.supports.jsonPathQueries) {
      warnings.push('JSON path queries require PostgreSQL 12+');
    }
    
    if (options.postgresVersion < '12.0') {
      if (node.metadata?.postgresConfig?.isolationLevel === 'SERIALIZABLE') {
        warnings.push('SERIALIZABLE isolation level improvements require PostgreSQL 9.1+');
      }
    }

    // TODO: Migrate this check to use the unified configuration model.
    // The old `tableMapping` is no longer a top-level property; it is now inside
    // the configuration union (e.g., for input nodes). This section needs to be
    // updated when concrete generators are fully migrated.
    /*
    const hasGeneratedColumns = node.metadata?.tableMapping?.columns?.some(
      col => col.defaultValue?.includes('GENERATED')
    );
    
    if (hasGeneratedColumns && !this.featureSupport.supports.generatedColumns) {
      errors.push({
        code: 'UNSUPPORTED_FEATURE',
        message: 'Generated columns require PostgreSQL 12+',
        severity: 'ERROR',
        suggestion: 'Use computed columns in application layer'
      });
    }
    */
  }

  // ==================== HELPER METHODS ====================
  protected assembleSQL(
    select: string,
    join: string,
    where: string,
    groupBy: string,
    having: string,
    orderBy: string,
    _context: SQLGenerationContext
  ): string {
    const parts: string[] = [select];
    
    if (join) parts.push(join);
    if (where) parts.push(where);
    if (groupBy) parts.push(groupBy);
    if (having) parts.push(having);
    if (orderBy) parts.push(orderBy);
    
    // LIMIT and OFFSET are now expected to be included in the orderBy fragment
    // or handled by the concrete generator. The old direct access to
    // node.metadata.sortConfig is removed.
    
    return parts.join('\n') + ';';
  }

  protected optimizeForPostgreSQL(sql: string, _context: SQLGenerationContext): string {
    let optimized = sql;
    optimized = optimized.replace(/INNER JOIN/g, 'JOIN');
    optimized = optimized.replace(/LIKE '(?![A-Z])[^']*'/gi, match => {
      const pattern = match.substring(6, match.length - 1);
      return /[A-Z]/.test(pattern) ? match : match.replace('LIKE', 'ILIKE');
    });
    optimized = optimized.replace(
      /CASE WHEN (\w+) IS NULL THEN (\w+) ELSE \1 END/gi,
      'COALESCE($1, $2)'
    );
    optimized = optimized.replace(
      /array_to_string\(array_agg\((\w+)\), ','\)/gi,
      'string_agg($1, \',\')'
    );
    return optimized;
  }

  protected formatSQL(sql: string, maxLineLength: number = 80): string {
    const lines = sql.split('\n');
    const formattedLines: string[] = [];
    
    for (const line of lines) {
      if (line.length <= maxLineLength) {
        formattedLines.push(line);
        continue;
      }
      
      const splitPoints = [' JOIN ', ' ON ', ' WHERE ', ' AND ', ' OR ', ' GROUP BY ', ' ORDER BY '];
      let splitLine = line;
      
      for (const point of splitPoints) {
        if (splitLine.includes(point) && splitLine.length > maxLineLength) {
          const parts = splitLine.split(point);
          if (parts.length === 2) {
            formattedLines.push(parts[0] + point.trim());
            formattedLines.push('  ' + parts[1]);
            splitLine = parts[1];
          }
        }
      }
      
      if (splitLine === line) {
        formattedLines.push(line);
      }
    }
    
    return formattedLines.join('\n');
  }

  protected addComments(sql: string, context: SQLGenerationContext): string {
    const comments: string[] = [];
    comments.push(`-- Generated by PostgreSQL SQL Generator`);
    comments.push(`-- Node: ${context.node.name} (${context.node.type})`);
    comments.push(`-- Generated at: ${new Date().toISOString()}`);
    comments.push(`-- PostgreSQL Version: ${context.options.postgresVersion}`);
    
    if (context.node.metadata?.description) {
      comments.push(`-- Description: ${context.node.metadata.description}`);
    }
    
    comments.push('');
    return comments.join('\n') + sql;
  }

  protected escapeString(str: string): string {
    return str.replace(/'/g, "''").replace(/\\/g, '\\\\');
  }

  protected indent(text: string, spaces: number): string {
    const indent = ' '.repeat(spaces);
    return text.split('\n').map(line => indent + line).join('\n');
  }

  protected isValidIdentifier(identifier: string): boolean {
    return /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(identifier);
  }

  /**
   * Returns an empty SQL fragment. Useful for concrete generators that need
   * to satisfy the abstract methods without contributing SQL.
   */
  protected emptyFragment(): GeneratedSQLFragment {
    return {
      sql: '',
      dependencies: [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'empty', lineCount: 0 }
    };
  }

  protected detectPostgreSQLFeatures(version: string): PostgreSQLFeatureSupport {
    const versionNum = parseFloat(version);
    
    return {
      version,
      supports: {
        generatedColumns: versionNum >= 12.0,
        identityColumns: versionNum >= 10.0,
        jsonPathQueries: versionNum >= 12.0,
        sqlJSON: versionNum >= 9.4,
        multirangeTypes: versionNum >= 14.0,
        storedGeneratedColumns: versionNum >= 12.0,
        tableAccessMethod: versionNum >= 12.0,
        recursiveCTE: versionNum >= 8.4,
        windowFunctions: versionNum >= 8.4,
        commonTableExpressions: versionNum >= 8.4,
        jsonb: versionNum >= 9.4,
        upsert: versionNum >= 9.5,
        fullTextSearch: versionNum >= 8.3,
        xml: versionNum >= 8.3,
        filteredAggregates: versionNum >= 9.4,
        groupingExtensions: versionNum >= 9.5
      }
    };
  }

  private countCTEUsage(cteName: string, cteChain: CTEChain[]): number {
    let count = 0;
    cteChain.forEach(cte => {
      const regex = new RegExp(`\\b${cteName}\\b`, 'gi');
      const matches = cte.query.match(regex);
      if (matches) count += matches.length;
    });
    return count;
  }

  private optimizeSubqueries(query: string): string {
    return query.replace(
      /WHERE EXISTS\s*\(\s*SELECT 1 FROM (\w+) sub WHERE sub\.(\w+) = main\.(\w+)\s*\)/gi,
      'INNER JOIN $1 sub ON sub.$2 = main.$3'
    );
  }

  private cleanParentheses(query: string): string {
    return query.replace(/\(\(\s*([^)]+)\s*\)\)/g, '($1)');
  }
}

// ==================== CONCRETE EXAMPLE GENERATOR ====================
export class SelectSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    const columns = this.extractColumns(node);
    const table = this.extractTableName(node);
    
    if (columns.length === 0) {
      return {
        sql: `SELECT * FROM ${this.sanitizeIdentifier(table)}`,
        dependencies: [table],
        parameters: new Map(),
        errors: [],
        warnings: [],
        metadata: { generatedAt: new Date().toISOString(), fragmentType: 'select_statement', lineCount: 1 }
      };
    }
    
    const columnList = columns.map(col => {
      const alias = col.alias ? ` AS ${this.sanitizeIdentifier(col.alias)}` : '';
      return `${this.sanitizeIdentifier(col.name)}${alias}`;
    }).join(', ');
    
    return {
      sql: `SELECT ${columnList} FROM ${this.sanitizeIdentifier(table)}`,
      dependencies: [table],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'select_statement', lineCount: 1 }
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
  
  private extractColumns(_node: UnifiedCanvasNode): Array<{ name: string; alias?: string }> {
    // TODO: Replace with extraction from unified configuration.
    // For now, return empty array (or attempt to read from legacy fields).
    return [];
  }
  
  private extractTableName(node: UnifiedCanvasNode): string {
    // TODO: Replace with extraction from unified configuration (e.g., input node table name).
    return node.name.toLowerCase().replace(/\s+/g, '_');
  }
}

// ==================== FACTORY REMOVED ====================
// The SQLGeneratorFactory has been moved to its own file:
// src/generators/SQLGeneratorFactory.ts
// This resolves the circular dependency caused by importing concrete generators here.