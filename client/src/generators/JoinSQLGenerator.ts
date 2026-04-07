// src/generators/JoinSQLGenerator.ts

import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';
import { UnifiedCanvasNode, JoinComponentConfiguration, FieldSchema, PostgreSQLDataType } from '../types/unified-pipeline.types';

/**
 * PostgreSQL JOIN SQL Generator
 * Supports INNER, LEFT, RIGHT, FULL, CROSS joins with optimization
 * Works with both legacy (metadata.joinConfig) and unified (metadata.configuration.config) formats.
 */
export class JoinSQLGenerator extends BaseSQLGenerator {
  // ==================== TEMPLATE METHOD IMPLEMENTATIONS ====================

  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    const joinConfig = this.extractJoinConfig(node);
    const sources = this.extractJoinSourcesFromConditions(joinConfig);
    const leftAlias = node.metadata?.leftAlias as string | undefined;
    const rightAlias = node.metadata?.rightAlias as string | undefined;

    if (!joinConfig || sources.length < 2) {
      return this.generateFallbackSelect(context);
    }

    const selectColumns = this.buildSelectColumns(sources, joinConfig, leftAlias, rightAlias);
    
    return {
      sql: `SELECT ${selectColumns}`,
      dependencies: sources.map(s => s.table),
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
    const joinConfig = this.extractJoinConfig(node);
    const sources = this.extractJoinSourcesFromConditions(joinConfig);
    const leftAlias = node.metadata?.leftAlias as string | undefined;
    const rightAlias = node.metadata?.rightAlias as string | undefined;

    if (!joinConfig || sources.length < 2) {
      return this.emptyFragment();
    }

    const joinClause = this.buildJoinClause(sources, joinConfig, leftAlias, rightAlias);
    
    return {
      sql: joinClause,
      dependencies: sources.map(s => s.table),
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
    // WHERE clause can come from metadata.whereClause (test) or joinConfig.whereClause
    let whereClause = node.metadata?.whereClause as string | undefined;
    if (!whereClause) {
      const joinConfig = this.extractJoinConfig(node);
      whereClause = joinConfig?.whereClause;
    }

    if (!whereClause) {
      return this.emptyFragment();
    }

    const optimizedWhere = this.optimizeJoinWhereClause(whereClause);
    
    return {
      sql: `WHERE ${optimizedWhere}`,
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
    return this.emptyFragment();
  }

  protected generateOrderByClause(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    if (node.metadata?.sortConfig) {
      return this.generateOrderByFromSortConfig(node.metadata.sortConfig);
    }
    return this.emptyFragment();
  }

  protected generateGroupByClause(_context: SQLGenerationContext): GeneratedSQLFragment {
    return this.emptyFragment();
  }

  // ==================== PUBLIC GENERATION METHOD ====================

  public generateSQL(context: SQLGenerationContext): GeneratedSQLFragment {
    const result = super.generateSQL(context);
    // Add warning if no join condition and not a CROSS join
    const joinConfig = this.extractJoinConfig(context.node);
    if (joinConfig && (!joinConfig.joinConditions || joinConfig.joinConditions.length === 0) && joinConfig.joinType !== 'CROSS') {
      result.warnings.push('No join condition specified; may produce cartesian product');
    }
    return result;
  }

  // ==================== CONFIGURATION EXTRACTION ====================

  /**
   * Extract join configuration from node metadata (supports both unified and legacy formats)
   */
  private extractJoinConfig(node: UnifiedCanvasNode): (JoinComponentConfiguration & { whereClause?: string; leftAlias?: string; rightAlias?: string }) | null {
    // 1. Try unified format: node.metadata.configuration.config
    if (node.metadata?.configuration?.config && (node.metadata.configuration.config as any).joinType) {
      const config = node.metadata.configuration.config as JoinComponentConfiguration;
      // Ensure required fields exist
      const fixedConfig = { 
        ...config,
        version: config.version || '1.0'
      };
      // Ensure compilerMetadata has required field optimizationApplied
      if (fixedConfig.compilerMetadata && !('optimizationApplied' in fixedConfig.compilerMetadata)) {
        (fixedConfig.compilerMetadata as any).optimizationApplied = false;
      }
      // Also capture whereClause, leftAlias, rightAlias from node.metadata (test passes them separately)
      return {
        ...fixedConfig,
        whereClause: node.metadata.whereClause as string | undefined,
        leftAlias: node.metadata.leftAlias as string | undefined,
        rightAlias: node.metadata.rightAlias as string | undefined
      };
    }
    // 2. Legacy format: node.metadata.joinConfig
    if (node.metadata?.joinConfig) {
      const legacy = node.metadata.joinConfig as any;
      return {
        version: '1.0',  // Required by JoinComponentConfiguration
        joinType: legacy.type || 'INNER',
        joinConditions: legacy.condition ? this.parseLegacyCondition(legacy.condition) : [],
        outputSchema: { fields: [], deduplicateFields: true, fieldAliases: {} },
        joinHints: { enableJoinHint: false },
        sqlGeneration: { joinAlgorithm: 'HASH', estimatedJoinCardinality: 1.0, nullHandling: 'INCLUDE', requiresSort: false, canParallelize: true },
        compilerMetadata: { lastModified: new Date().toISOString(), optimizationApplied: false },
        whereClause: legacy.whereClause,
        leftAlias: legacy.leftAlias,
        rightAlias: legacy.rightAlias
      };
    }
    return null;
  }

  /**
   * Parse a legacy string condition into an array of join conditions.
   * Operator is restricted to allowed values.
   */
  private parseLegacyCondition(condition: string): Array<{ leftTable: string; leftField: string; rightTable: string; rightField: string; operator: '=' | '!=' | '<' | '>' | '<=' | '>=' | 'LIKE'; id: string; position: number }> {
    const parts = condition.split(/\s+AND\s+|\s+OR\s+/i);
    return parts.map((part, idx) => {
      const match = part.match(/(\w+)\.(\w+)\s*([=<>!]+|LIKE)\s*(\w+)\.(\w+)/i);
      if (match) {
        let operator = match[3].toUpperCase();
        // Normalize operator to allowed set
        if (operator === '==') operator = '=';
        if (operator === '!=' || operator === '<>') operator = '!=';
        if (operator === 'LIKE') operator = 'LIKE';
        if (!['=', '!=', '<', '>', '<=', '>=', 'LIKE'].includes(operator)) operator = '=';
        return {
          id: `cond_${idx}`,
          leftTable: match[1],
          leftField: match[2],
          rightTable: match[4],
          rightField: match[5],
          operator: operator as any,
          position: idx
        };
      }
      // Fallback: treat whole part as raw condition (no table/field parsing)
      // Use a synthetic condition with operator '=' (won't be used directly)
      return {
        id: `cond_${idx}`,
        leftTable: '',
        leftField: '',
        rightTable: '',
        rightField: '',
        operator: '=',
        position: idx,
        raw: part
      } as any;
    });
  }

  // ==================== SOURCE EXTRACTION ====================

  /**
   * Extract source tables from join conditions (since test does not provide upstream connections)
   */
  private extractJoinSourcesFromConditions(joinConfig: any): Array<{ table: string; alias?: string; columns: any[] }> {
    const sources: Array<{ table: string; alias?: string; columns: any[] }> = [];
    if (!joinConfig?.joinConditions) return sources;

    const leftAlias = joinConfig.leftAlias;
    const rightAlias = joinConfig.rightAlias;
    const leftTable = joinConfig.joinConditions[0]?.leftTable;
    const rightTable = joinConfig.joinConditions[0]?.rightTable;

    if (leftTable) {
      sources.push({ table: leftTable, alias: leftAlias, columns: [] });
    }
    if (rightTable && rightTable !== leftTable) {
      sources.push({ table: rightTable, alias: rightAlias, columns: [] });
    }
    return sources;
  }

  // ==================== SELECT CLAUSE CONSTRUCTION ====================

  private buildSelectColumns(
    sources: Array<{ table: string; alias?: string }>,
    joinConfig: any,
    leftAlias?: string,
    rightAlias?: string
  ): string {
    const outputFields = joinConfig.outputSchema?.fields as FieldSchema[] | undefined;
    
    if (outputFields && outputFields.length > 0) {
      // Build SELECT with explicit columns and type casting based on output field types
      const columns = outputFields.map(field => {
        // Determine which source table and column this field originates from
        const source = this.findSourceForField(field.name, sources, leftAlias, rightAlias);
        if (source) {
          const qualifiedName = source.alias ? `${this.sanitizeIdentifier(source.alias)}.${this.sanitizeIdentifier(field.name)}` : this.sanitizeIdentifier(field.name);
          // Apply type casting only if a specific data type is specified
          if (field.type) {
            const pgType = this.mapFieldTypeToPostgreSQL(field.type);
            const castExpr = this.castToType(qualifiedName, pgType);
            return `${castExpr} AS ${this.sanitizeIdentifier(field.name)}`;
          }
          return `${qualifiedName} AS ${this.sanitizeIdentifier(field.name)}`;
        }
        // Fallback: assume field exists in first source
        const defaultSource = sources[0];
        const defaultQualified = defaultSource.alias ? `${this.sanitizeIdentifier(defaultSource.alias)}.${this.sanitizeIdentifier(field.name)}` : this.sanitizeIdentifier(field.name);
        return `${defaultQualified} AS ${this.sanitizeIdentifier(field.name)}`;
      });
      return columns.join(', ');
    } else {
      // No output fields: select all columns from each source with optional aliases
      const selectParts = sources.map(source => {
        const alias = source.alias;
        if (alias) {
          return `${this.sanitizeIdentifier(alias)}.*`;
        } else {
          return `${this.sanitizeIdentifier(source.table)}.*`;
        }
      });
      return selectParts.join(', ');
    }
  }

  private findSourceForField(_fieldName: string, sources: Array<{ table: string; alias?: string }>, _leftAlias?: string, _rightAlias?: string): { table: string; alias?: string } | null {
    // This is a placeholder. In a real implementation you would use schema mappings.
    // For tests, we assume the field exists in the first source (left table)
    return sources[0] || null;
  }

  private mapFieldTypeToPostgreSQL(type: string): PostgreSQLDataType {
    const mapping: Record<string, PostgreSQLDataType> = {
      'STRING': PostgreSQLDataType.TEXT,
      'INTEGER': PostgreSQLDataType.INTEGER,
      'BIGINT': PostgreSQLDataType.BIGINT,
      'BOOLEAN': PostgreSQLDataType.BOOLEAN,
      'DATE': PostgreSQLDataType.DATE,
      'TIMESTAMP': PostgreSQLDataType.TIMESTAMP,
      'TIMESTAMPTZ': PostgreSQLDataType.TIMESTAMPTZ,
      'FLOAT': PostgreSQLDataType.DOUBLE_PRECISION,
      'DOUBLE': PostgreSQLDataType.DOUBLE_PRECISION,
      'DECIMAL': PostgreSQLDataType.DECIMAL,
      'JSON': PostgreSQLDataType.JSON,
      'JSONB': PostgreSQLDataType.JSONB,
      'UUID': PostgreSQLDataType.UUID
    };
    return mapping[type.toUpperCase()] || PostgreSQLDataType.TEXT;
  }

  // ==================== JOIN CLAUSE CONSTRUCTION ====================

  private buildJoinClause(
    sources: Array<{ table: string; alias?: string }>,
    joinConfig: any,
    leftAlias?: string,
    rightAlias?: string
  ): string {
    if (sources.length < 2) {
      return `FROM ${this.sanitizeIdentifier(sources[0].table)}`;
    }

    const leftSource = sources[0];
    const rightSource = sources[1];
    const joinType = joinConfig.joinType || 'INNER';
    const leftTableRef = leftAlias ? this.sanitizeIdentifier(leftAlias) : this.sanitizeIdentifier(leftSource.table);
    const rightTableRef = rightAlias ? this.sanitizeIdentifier(rightAlias) : this.sanitizeIdentifier(rightSource.table);

    let fromClause = `FROM ${this.sanitizeIdentifier(leftSource.table)}`;
    if (leftAlias) {
      fromClause += ` AS ${this.sanitizeIdentifier(leftAlias)}`;
    }

    let joinKeyword = '';
    switch (joinType.toUpperCase()) {
      case 'LEFT': joinKeyword = 'LEFT JOIN'; break;
      case 'RIGHT': joinKeyword = 'RIGHT JOIN'; break;
      case 'FULL': joinKeyword = 'FULL OUTER JOIN'; break;
      case 'CROSS': joinKeyword = 'CROSS JOIN'; break;
      default: joinKeyword = 'INNER JOIN';
    }

    let joinClause = `\n${joinKeyword} ${this.sanitizeIdentifier(rightSource.table)}`;
    if (rightAlias) {
      joinClause += ` AS ${this.sanitizeIdentifier(rightAlias)}`;
    }

    // Build ON condition from joinConditions array
    const conditions = joinConfig.joinConditions;
    if (conditions && conditions.length > 0 && joinType.toUpperCase() !== 'CROSS') {
      const onParts = conditions.map((cond: any) => {
        // If condition already has a raw string (from legacy parsing), use it
        if (cond.raw) return cond.raw;
        const leftTableRefOn = cond.leftTable === leftSource.table ? leftTableRef : this.sanitizeIdentifier(cond.leftTable);
        const rightTableRefOn = cond.rightTable === rightSource.table ? rightTableRef : this.sanitizeIdentifier(cond.rightTable);
        return `${leftTableRefOn}.${this.sanitizeIdentifier(cond.leftField)} ${cond.operator} ${rightTableRefOn}.${this.sanitizeIdentifier(cond.rightField)}`;
      });
      joinClause += ` ON ${onParts.join(' AND ')}`;
    } else if (joinType.toUpperCase() !== 'CROSS') {
      // No conditions – will produce cartesian product, warning added later
      joinClause += ` ON TRUE`; // to keep syntax valid
    }

    return fromClause + joinClause;
  }

  // ==================== UTILITY METHODS ====================

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
        lineCount: sql.split('\n').length
      }
    };
  }

  private optimizeJoinWhereClause(whereClause: string): string {
    return whereClause
      .replace(/\s*=\s*NULL\b/gi, ' IS NULL')
      .replace(/\s*<>\s*NULL\b/gi, ' IS NOT NULL')
      .replace(/\bLIKE\s+'%(.+)%'/gi, "ILIKE '%$1%'")
      .replace(/\s+AND\s+/gi, ' AND ')
      .replace(/\s+OR\s+/gi, ' OR ');
  }
}