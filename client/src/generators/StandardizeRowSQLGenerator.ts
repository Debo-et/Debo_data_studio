// src/generators/StandardizeRowSQLGenerator.ts
import {
  BaseSQLGenerator,
  SQLGenerationContext,
  GeneratedSQLFragment,
} from './BaseSQLGenerator';
import { UnifiedCanvasNode, NodeType } from '../types/unified-pipeline.types';

interface StandardizationRule {
  column: string;
  operation: 'trim' | 'upper' | 'lower' | 'initcap' | 'phone' | 'email' | 'zip' | 'custom';
  params?: Record<string, any>;
  customExpression?: string;
}

export class StandardizeRowSQLGenerator extends BaseSQLGenerator {
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
    const { node } = context;
    const rules = this.extractRules(node);

    if (rules.length === 0) {
      return this.fallbackSelect(node);
    }

    const allColumns = node.metadata?.schemas?.output?.fields || [];
    if (allColumns.length === 0) {
      return this.fallbackSelect(node);
    }

    const selectExpressions = allColumns.map(col => {
      const rule = rules.find(r => r.column === col.name);
      if (!rule) {
        return this.sanitizeIdentifier(col.name);
      }

      let expr = this.sanitizeIdentifier(col.name);
      switch (rule.operation) {
        case 'trim':
          expr = `TRIM(${expr})`;
          break;
        case 'upper':
          expr = `UPPER(${expr})`;
          break;
        case 'lower':
          expr = `LOWER(${expr})`;
          break;
        case 'initcap':
          expr = `INITCAP(${expr})`;
          break;
        case 'phone':
          expr = `REGEXP_REPLACE(${expr}, '\\D', '', 'g')`;
          break;
        case 'email':
          expr = `LOWER(TRIM(${expr}))`;
          break;
        case 'zip':
          expr = `SUBSTRING(REGEXP_REPLACE(${expr}, '\\D', '', 'g') FROM 1 FOR 5)`;
          break;
        case 'custom':
          if (rule.customExpression) {
            expr = rule.customExpression.replace(
              /\{column\}/g,
              this.sanitizeIdentifier(col.name)
            );
          }
          break;
      }
      return `${expr} AS ${this.sanitizeIdentifier(col.name)}`;
    });

    const sql = `SELECT ${selectExpressions.join(', ')} FROM source_table`;
    return {
      sql,
      dependencies: ['source_table'],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'standardize_select',
        lineCount: sql.split('\n').length,
      },
    };
  }

  private extractRules(node: UnifiedCanvasNode): StandardizationRule[] {
    if (node.type !== NodeType.STANDARDIZE_ROW) {
      return [];
    }

    const config = node.metadata?.configuration?.config;
    if (!config || typeof config !== 'object') {
      return [];
    }

    const maybeRules = (config as any).rules;
    if (!Array.isArray(maybeRules)) {
      return [];
    }

    return maybeRules.filter(
      (rule): rule is StandardizationRule =>
        rule &&
        typeof rule.column === 'string' &&
        ['trim', 'upper', 'lower', 'initcap', 'phone', 'email', 'zip', 'custom'].includes(
          rule.operation
        )
    );
  }

  private fallbackSelect(node: UnifiedCanvasNode): GeneratedSQLFragment {
    const tableName = node.name.toLowerCase().replace(/\s+/g, '_') + '_source';
    const sql = `SELECT * FROM ${this.sanitizeIdentifier(tableName)}`;
    return {
      sql,
      dependencies: [tableName],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'fallback_select',
        lineCount: 1,
      },
    };
  }
}