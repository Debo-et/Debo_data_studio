// src/generators/ReplaceSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';
import { UnifiedCanvasNode } from '../types/unified-pipeline.types';

interface ReplaceRule {
  field: string;
  search: string;
  replace: string;
  caseSensitive?: boolean;
  regex?: boolean;
}

export class ReplaceSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;

    // Safely extract rules if they exist in the configuration
    let rules: ReplaceRule[] = [];
    const config = node.metadata?.configuration?.config;
    if (config && 'rules' in config && Array.isArray(config.rules)) {
      rules = config.rules as ReplaceRule[];
    }

    if (rules.length === 0) {
      return this.fallbackSelect(node);
    }

    const columns = node.metadata?.schemas?.output?.fields || [];
    const selectExpressions = columns.map(col => {
      const rule = rules.find(r => r.field === col.name);
      if (!rule) return this.sanitizeIdentifier(col.name);

      // Apply replacement
      let expr = this.sanitizeIdentifier(col.name);
      if (rule.regex) {
        expr = `REGEXP_REPLACE(${expr}, '${this.escapeString(rule.search)}', '${this.escapeString(rule.replace)}', 'g')`;
      } else {
        expr = `REPLACE(${expr}, '${this.escapeString(rule.search)}', '${this.escapeString(rule.replace)}')`;
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
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'replace_select', lineCount: 1 }
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

  private fallbackSelect(node: UnifiedCanvasNode): GeneratedSQLFragment {
    return {
      sql: `SELECT * FROM ${this.sanitizeIdentifier(node.name)}`,
      dependencies: [node.name],
      parameters: new Map(),
      errors: [],
      warnings: ['No replace rules, using fallback'],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'replace_fallback', lineCount: 1 }
    };
  }

  private emptyFragment(): GeneratedSQLFragment {
    return {
      sql: '',
      dependencies: [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'empty', lineCount: 0 }
    };
  }
}