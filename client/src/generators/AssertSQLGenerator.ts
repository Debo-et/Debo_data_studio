// src/generators/AssertSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';
import { UnifiedCanvasNode } from '../types/unified-pipeline.types';

interface AssertConfig {
  condition: string;
  message?: string;
  action: 'filter' | 'error';
}

export class AssertSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    const config = node.metadata?.configuration?.config as AssertConfig | undefined;

    if (!config || !config.condition) {
      return this.fallbackSelect(node);
    }

    if (config.action === 'filter') {
      return {
        sql: `SELECT * FROM source_table WHERE ${config.condition}`,
        dependencies: ['source_table'],
        parameters: new Map(),
        errors: [],
        warnings: [],
        metadata: { generatedAt: new Date().toISOString(), fragmentType: 'assert_filter', lineCount: 1 }
      };
    } else {
      return {
        sql: `-- Assertion with error: ${config.message || 'condition failed'}\nSELECT * FROM source_table WHERE ${config.condition}`,
        dependencies: ['source_table'],
        parameters: new Map(),
        errors: [],
        warnings: ['Assert with error action cannot be fully expressed in plain SQL; row will be filtered instead'],
        metadata: { generatedAt: new Date().toISOString(), fragmentType: 'assert_error', lineCount: 1 }
      };
    }
  }

  protected generateJoinConditions(_context: SQLGenerationContext): GeneratedSQLFragment {
    return this.emptyFragment();
  }

  protected generateWhereClause(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    const config = node.metadata?.configuration?.config as AssertConfig | undefined;

    if (config?.action === 'filter' && config?.condition) {
      return {
        sql: `WHERE ${config.condition}`,
        dependencies: [],
        parameters: new Map(),
        errors: [],
        warnings: [],
        metadata: { generatedAt: new Date().toISOString(), fragmentType: 'assert_where', lineCount: 1 }
      };
    }
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
      warnings: ['No assert condition, using fallback'],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'assert_fallback', lineCount: 1 }
    };
  }
  // private emptyFragment() removed – use protected base method
}