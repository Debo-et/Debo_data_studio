// src/generators/LookupSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';
import { UnifiedCanvasNode, isLookupConfig, LookupComponentConfiguration } from '../types/unified-pipeline.types';

export class LookupSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    const config = this.getLookupConfig(node);
    if (!config) return this.fallbackSelect(node);

    // Build SELECT clause with lookup fields
    const selectColumns = [
      ...config.lookupKeyFields.map(f => this.sanitizeIdentifier(f)),
      ...config.lookupReturnFields.map(f => this.sanitizeIdentifier(f))
    ].join(', ');

    const fromTable = this.sanitizeIdentifier(config.lookupTable);
    return {
      sql: `SELECT ${selectColumns} FROM ${fromTable}`,
      dependencies: [config.lookupTable],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'lookup_select', lineCount: 1 }
    };
  }

  protected generateJoinConditions(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node, connection } = context;
    const config = this.getLookupConfig(node);
    if (!config || !connection) return this.emptyFragment();

    // Build JOIN condition based on key fields
    const joinConditions = config.lookupKeyFields.map(keyField => {
      // Assume source column is the same as keyField, but could be mapped differently
      return `${connection.sourceNodeId}.${this.sanitizeIdentifier(keyField)} = ${config.lookupTable}.${this.sanitizeIdentifier(keyField)}`;
    }).join(' AND ');

    const joinType = config.sqlGeneration.joinType === 'LEFT' ? 'LEFT JOIN' : 'JOIN';
    return {
      sql: `${joinType} ${this.sanitizeIdentifier(config.lookupTable)} ON ${joinConditions}`,
      dependencies: [config.lookupTable],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'lookup_join', lineCount: 1 }
    };
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

  private getLookupConfig(node: UnifiedCanvasNode): LookupComponentConfiguration | undefined {
    if (!node.metadata?.configuration) return undefined;
    const conf = node.metadata.configuration;
    return isLookupConfig(conf) ? conf.config : undefined;
  }

  private fallbackSelect(node: UnifiedCanvasNode): GeneratedSQLFragment {
    return {
      sql: `SELECT * FROM ${this.sanitizeIdentifier(node.name)}`,
      dependencies: [node.name],
      parameters: new Map(),
      errors: [],
      warnings: ['No lookup configuration, using fallback'],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'lookup_fallback', lineCount: 1 }
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