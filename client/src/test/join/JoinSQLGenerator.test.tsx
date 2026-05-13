// src/generators/JoinSQLGenerator.ts

import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment, SQLGenerationOptions } from '../../generators/BaseSQLGenerator';
import { JoinConfig, PostgresColumn, SortConfig } from '../../types/pipeline-types';
import { JoinComponentConfiguration } from '../../types/unified-pipeline.types';

/**
 * PostgreSQL JOIN SQL Generator
 * Supports INNER, LEFT, RIGHT, FULL, CROSS joins with optimization
 */
export class JoinSQLGenerator extends BaseSQLGenerator {
  constructor(options: Partial<SQLGenerationOptions> = {}) {
    super(options);
  }

  // ==================== TEMPLATE METHOD IMPLEMENTATIONS ====================

  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    const joinConfig = this.extractJoinConfig(node);
    const sources = this.extractJoinSourcesFromConfig(joinConfig, node, context.connection);
    
    if (!joinConfig || sources.length < 2) {
      return this.generateFallbackSelect(context);
    }

    const selectColumns = this.generateJoinSelectColumns(sources, joinConfig, node);
    
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
    const joinConfig = this.extractJoinConfig(node);
    const sources = this.extractJoinSourcesFromConfig(joinConfig, node, context.connection);
    
    if (!joinConfig || sources.length < 2) {
      return this.emptyFragment();
    }

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
    let whereClause: string | undefined = node.metadata?.whereClause;
    
    if (!whereClause) {
      const joinConfig = this.extractJoinConfig(node);
      if (joinConfig && (joinConfig as any).whereClause) {
        whereClause = (joinConfig as any).whereClause;
      }
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

  // ==================== JOIN-SPECIFIC METHODS ====================

  /**
   * Extract join configuration from unified node metadata
   */
  private extractJoinConfig(node: any): JoinComponentConfiguration | null {
    const config = node.metadata?.configuration;
    if (config && config.type === 'JOIN') {
      return config.config as JoinComponentConfiguration;
    }
    // Fallback for legacy tests that may set joinConfig directly
    if (node.metadata?.joinConfig) {
      const legacy = node.metadata.joinConfig;
      return {
        joinType: legacy.type || 'INNER',
        joinConditions: legacy.condition ? this.parseLegacyCondition(legacy.condition) : [],
        joinHints: { enableJoinHint: false },
        outputSchema: { fields: [], deduplicateFields: true, fieldAliases: {} },
        sqlGeneration: { joinAlgorithm: 'HASH', estimatedJoinCardinality: 1.0, nullHandling: 'INCLUDE', requiresSort: false, canParallelize: true },
        compilerMetadata: { lastModified: new Date().toISOString() },
        ...(legacy.leftAlias && { leftAlias: legacy.leftAlias }),
        ...(legacy.rightAlias && { rightAlias: legacy.rightAlias }),
      };
    }
    return null;
  }

  /**
   * Parse legacy condition string into join conditions array
   */
  private parseLegacyCondition(condition: string): Array<{ id: string; leftTable: string; leftField: string; rightTable: string; rightField: string; operator: string; position: number }> {
    const parts = condition.split(/\s*=\s*/);
    if (parts.length === 2) {
      const leftMatch = parts[0].match(/(\w+)\.(\w+)/);
      const rightMatch = parts[1].match(/(\w+)\.(\w+)/);
      if (leftMatch && rightMatch) {
        return [{
          id: `legacy_${Date.now()}`,
          leftTable: leftMatch[1],
          leftField: leftMatch[2],
          rightTable: rightMatch[1],
          rightField: rightMatch[2],
          operator: '=',
          position: 0
        }];
      }
    }
    return [];
  }

  /**
   * Extract source table information from join configuration and upstream connections
   */
  private extractJoinSourcesFromConfig(
    joinConfig: JoinComponentConfiguration | null,
    _node: any,
    _connection?: any
  ): Array<{ tableName: string; alias?: string; columns: PostgresColumn[] }> {
    const sources: Array<{ tableName: string; alias?: string; columns: PostgresColumn[] }> = [];

    if (!joinConfig) return sources;

    const leftTable = joinConfig.joinConditions[0]?.leftTable || 'left_table';
    const rightTable = joinConfig.joinConditions[0]?.rightTable || 'right_table';
    const leftAlias = (joinConfig as any).leftAlias;
    const rightAlias = (joinConfig as any).rightAlias;

    const outputFields = joinConfig.outputSchema?.fields || [];
    const leftColumns: PostgresColumn[] = outputFields
      .filter(f => f.name.startsWith(`${leftTable}_`) || !f.name.includes('_'))
      .map(f => ({ name: f.name, dataType: f.type as any, nullable: f.nullable }));
    const rightColumns: PostgresColumn[] = outputFields
      .filter(f => f.name.startsWith(`${rightTable}_`))
      .map(f => ({ name: f.name.replace(`${rightTable}_`, ''), dataType: f.type as any, nullable: f.nullable }));

    sources.push({
      tableName: leftTable,
      alias: leftAlias,
      columns: leftColumns.length ? leftColumns : []
    });
    sources.push({
      tableName: rightTable,
      alias: rightAlias,
      columns: rightColumns.length ? rightColumns : []
    });

    return sources;
  }

  /**
   * Generate SELECT clause with proper column aliases and type casting
   */
  private generateJoinSelectColumns(
    sources: Array<{ tableName: string; alias?: string; columns: PostgresColumn[] }>,
    joinConfig: JoinComponentConfiguration,
    _node: any
  ): string {
    const outputFields = joinConfig.outputSchema?.fields || [];
    const hasOutputFields = outputFields.length > 0;
    
    if (hasOutputFields) {
      const selectItems: string[] = [];
      
      for (const field of outputFields) {
        let sourceTable = '';
        let sourceColumn = field.name;
        
        const underscoreIndex = field.name.indexOf('_');
        if (underscoreIndex > 0) {
          const possibleTable = field.name.substring(0, underscoreIndex);
          const possibleColumn = field.name.substring(underscoreIndex + 1);
          const matchingSource = sources.find(s => s.tableName === possibleTable || s.alias === possibleTable);
          if (matchingSource) {
            sourceTable = matchingSource.alias || matchingSource.tableName;
            sourceColumn = possibleColumn;
          } else {
            sourceColumn = field.name;
          }
        }
        
        if (!sourceTable && sources.length > 0) {
          sourceTable = sources[0].alias || sources[0].tableName;
        }
        
        let expression = sourceTable ? `${this.sanitizeIdentifier(sourceTable)}.${this.sanitizeIdentifier(sourceColumn)}` : this.sanitizeIdentifier(sourceColumn);
        
        // Apply type casting if a non-default type is specified
        const fieldType = field.type as string;
        if (fieldType && fieldType !== 'STRING' && fieldType !== 'VARCHAR' && fieldType !== 'TEXT') {
          const pgType = this.mapToPostgresType(fieldType);
          expression = this.castToType(expression, pgType as any);
        }
        
        selectItems.push(`${expression} AS ${this.sanitizeIdentifier(field.name)}`);
      }
      
      return selectItems.join(', ');
    }
    
    // Fallback: select all columns from each source with table prefix
    const selectItems: string[] = [];
    for (const source of sources) {
      const alias = source.alias || source.tableName;
      if (source.columns.length > 0) {
        for (const col of source.columns) {
          selectItems.push(`${this.sanitizeIdentifier(alias)}.${this.sanitizeIdentifier(col.name)} AS ${this.sanitizeIdentifier(`${alias}_${col.name}`)}`);
        }
      } else {
        selectItems.push(`${this.sanitizeIdentifier(alias)}.*`);
      }
    }
    return selectItems.join(', ');
  }

  /**
   * Build JOIN clause from configuration
   */
  private buildJoinClause(
    sources: Array<{ tableName: string; alias?: string; columns: PostgresColumn[] }>,
    joinConfig: JoinComponentConfiguration
  ): string {
    if (sources.length < 2) {
      return `FROM ${this.sanitizeIdentifier(sources[0].tableName)}`;
    }

    const leftSource = sources[0];
    const rightSource = sources[1];
    const leftAlias = leftSource.alias || leftSource.tableName;
    const rightAlias = rightSource.alias || rightSource.tableName;
    
    const joinType = this.getJoinTypeString(joinConfig.joinType);
    
    const onConditions: string[] = [];
    for (const condition of joinConfig.joinConditions) {
      const leftRef = `${this.sanitizeIdentifier(leftAlias)}.${this.sanitizeIdentifier(condition.leftField)}`;
      const rightRef = `${this.sanitizeIdentifier(rightAlias)}.${this.sanitizeIdentifier(condition.rightField)}`;
      onConditions.push(`${leftRef} = ${rightRef}`);
    }
    
    const onClause = onConditions.length > 0 ? ` ON ${onConditions.join(' AND ')}` : '';
    
    let sql = `FROM ${this.sanitizeIdentifier(leftSource.tableName)} AS ${this.sanitizeIdentifier(leftAlias)}`;
    sql += `\n${joinType} JOIN ${this.sanitizeIdentifier(rightSource.tableName)} AS ${this.sanitizeIdentifier(rightAlias)}${onClause}`;
    
    return sql;
  }

  /**
   * Get JOIN type string for SQL
   */
  private getJoinTypeString(joinType: string): string {
    switch (joinType.toUpperCase()) {
      case 'INNER': return 'INNER JOIN';
      case 'LEFT': return 'LEFT JOIN';
      case 'RIGHT': return 'RIGHT JOIN';
      case 'FULL': return 'FULL OUTER JOIN';
      case 'CROSS': return 'CROSS JOIN';
      default: return 'INNER JOIN';
    }
  }

  /**
   * Map frontend type to PostgreSQL type for casting
   */
  private mapToPostgresType(type: string): string {
    const typeMap: Record<string, string> = {
      'STRING': 'TEXT',
      'VARCHAR': 'VARCHAR',
      'TEXT': 'TEXT',
      'INTEGER': 'INTEGER',
      'BIGINT': 'BIGINT',
      'DECIMAL': 'NUMERIC',
      'FLOAT': 'FLOAT8',
      'DOUBLE': 'FLOAT8',
      'BOOLEAN': 'BOOLEAN',
      'DATE': 'DATE',
      'DATETIME': 'TIMESTAMP',
      'TIMESTAMP': 'TIMESTAMP',
      'TIME': 'TIME',
      'JSON': 'JSONB',
      'UUID': 'UUID',
      'BINARY': 'BYTEA',
    };
    return typeMap[type.toUpperCase()] || 'TEXT';
  }

  /**
   * Optimize WHERE clause for PostgreSQL
   */
  private optimizeJoinWhereClause(whereClause: string): string {
    return whereClause
      .replace(/\s*=\s*NULL\b/gi, ' IS NULL')
      .replace(/\s*<>\s*NULL\b/gi, ' IS NOT NULL')
      .replace(/\bLIKE\s+'%(.+)%'/gi, "ILIKE '%$1%'")
      .replace(/\s+AND\s+/gi, ' AND ')
      .replace(/\s+OR\s+/gi, ' OR ');
  }

  /**
   * Generate ORDER BY from sort config
   */
  private generateOrderByFromSortConfig(sortConfig: SortConfig): GeneratedSQLFragment {
    const orderByClauses = sortConfig.columns.map(col => {
      const parts = [this.sanitizeIdentifier(col.column), col.direction];
      if (col.nullsFirst !== undefined) {
        parts.push(col.nullsFirst ? 'NULLS FIRST' : 'NULLS LAST');
      }
      return parts.join(' ');
    });

    let sql = `ORDER BY ${orderByClauses.join(', ')}`;
    if (sortConfig.limit) sql += `\nLIMIT ${sortConfig.limit}`;
    if (sortConfig.offset) sql += `\nOFFSET ${sortConfig.offset}`;

    return {
      sql,
      dependencies: [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'order_by', lineCount: sql.split('\n').length }
    };
  }

  /**
   * Fallback SELECT when join configuration is missing
   */
  private generateFallbackSelect(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    return {
      sql: `SELECT * FROM ${this.sanitizeIdentifier(node.name.toLowerCase().replace(/\s+/g, '_'))}`,
      dependencies: [node.name],
      parameters: new Map(),
      errors: [],
      warnings: ['No join configuration found, using fallback SELECT'],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'fallback_select', lineCount: 1 }
    };
  }

  // The following legacy methods are kept for compatibility but not used in the new implementation
  public generateJoinSQL(
    _sources: Array<{ table: string; alias?: string; columns: PostgresColumn[] }>,
    joinConfig: JoinConfig,
    _options: any = {}
  ): GeneratedSQLFragment {
    const mockNode = { metadata: { joinConfig } } as any;
    const context = { node: mockNode, options: this as any } as any;
    return this.generateSQL(context);
  }


}