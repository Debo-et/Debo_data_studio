// src/generators/DataMaskingSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';
import { UnifiedCanvasNode, NodeType } from '../types/unified-pipeline.types';

interface MaskingRule {
  column: string;
  method: 'redact' | 'hash' | 'partial' | 'random';
  params?: Record<string, any>;
}

// Type guard to check if the node is a data masking component
function isMaskingNode(node: UnifiedCanvasNode): boolean {
  return node.type === NodeType.DATA_MASKING;
}

export class DataMaskingSQLGenerator extends BaseSQLGenerator {
  // Implement all abstract methods from BaseSQLGenerator
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
    
    // Safely extract masking rules
    let rules: MaskingRule[] = [];
    if (isMaskingNode(node) && node.metadata?.configuration) {
      const config = node.metadata.configuration;
      // Masking nodes likely use the 'OTHER' type with a config that contains 'rules'
      if (config.type === 'OTHER') {
        const otherConfig = config.config as Record<string, any>;
        rules = otherConfig.rules || [];
      }
      // If there is a dedicated masking configuration type in the future, handle it here
    }

    if (rules.length === 0) {
      return this.fallbackSelect(node);
    }

    const allColumns = node.metadata?.schemas?.output?.fields || [];
    const selectExpressions = allColumns.map(col => {
      const rule = rules.find(r => r.column === col.name);
      if (!rule) return this.sanitizeIdentifier(col.name);

      let expr: string;
      switch (rule.method) {
        case 'redact':
          expr = `'***'`;
          break;
        case 'hash':
          expr = `md5(${this.sanitizeIdentifier(col.name)}::text)`;
          break;
        case 'partial':
          const showFirst = rule.params?.showFirst ?? 4;
          const showLast = rule.params?.showLast ?? 0;
          expr = `concat(substring(${this.sanitizeIdentifier(col.name)}::text from 1 for ${showFirst}), '****', substring(${this.sanitizeIdentifier(col.name)}::text from length(${this.sanitizeIdentifier(col.name)}::text) - ${showLast} + 1))`;
          break;
        case 'random':
          expr = `(random()*1000)::int::text`; // simplistic
          break;
        default:
          expr = this.sanitizeIdentifier(col.name);
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
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'data_masking', lineCount: 1 }
    };
  }

  // Helper method to generate a simple SELECT when no masking rules are present
  private fallbackSelect(node: UnifiedCanvasNode): GeneratedSQLFragment {
    const allColumns = node.metadata?.schemas?.output?.fields || [];
    const selectExpressions = allColumns.map(col => this.sanitizeIdentifier(col.name));
    const sql = `SELECT ${selectExpressions.join(', ')} FROM source_table`;
    return {
      sql,
      dependencies: ['source_table'],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'data_masking_fallback', lineCount: 1 }
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