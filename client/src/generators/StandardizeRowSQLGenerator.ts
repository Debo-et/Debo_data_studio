// src/generators/StandardizeRowSQLGenerator.ts
import {
  BaseSQLGenerator,
  SQLGenerationContext,
  GeneratedSQLFragment,
} from './BaseSQLGenerator';
import { UnifiedCanvasNode, NodeType } from '../types/unified-pipeline.types';

// Type for a single standardization rule
interface StandardizationRule {
  column: string;
  operation: 'trim' | 'upper' | 'lower' | 'initcap' | 'phone' | 'email' | 'zip' | 'custom';
  params?: Record<string, any>;
  customExpression?: string;
}

export class StandardizeRowSQLGenerator extends BaseSQLGenerator {
  // ------------------------------------------------------------------------
  // Required implementations of abstract methods
  // ------------------------------------------------------------------------
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

  // ------------------------------------------------------------------------
  // Core SELECT generation
  // ------------------------------------------------------------------------
  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    const rules = this.extractRules(node); // Always an array

    if (rules.length === 0) {
      return this.fallbackSelect(node);
    }

    const allColumns = node.metadata?.schemas?.output?.fields || [];
    if (allColumns.length === 0) {
      // No schema info – fallback to passthrough
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
          // Remove all non-digit characters
          expr = `REGEXP_REPLACE(${expr}, '\\D', '', 'g')`;
          break;
        case 'email':
          expr = `LOWER(TRIM(${expr}))`;
          break;
        case 'zip':
          // Keep only first five digits
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

  // ------------------------------------------------------------------------
  // Helper methods
  // ------------------------------------------------------------------------

  /**
   * Safely extract standardization rules from the node's configuration.
   * Always returns an array (empty if none found).
   */
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

    // Optional: validate each rule's shape
    return maybeRules.filter(
      (rule): rule is StandardizationRule =>
        rule &&
        typeof rule.column === 'string' &&
        ['trim', 'upper', 'lower', 'initcap', 'phone', 'email', 'zip', 'custom'].includes(
          rule.operation
        )
    );
  }

  /**
   * Generate a simple SELECT * FROM source_table as a fallback.
   */
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

  /**
   * Return an empty fragment for unused clauses.
   */
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
        lineCount: 0,
      },
    };
  }
}