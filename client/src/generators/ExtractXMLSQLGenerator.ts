// src/generators/ExtractXMLSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';
import { UnifiedCanvasNode } from '../types/unified-pipeline.types';

interface XMLExtractConfig {
  sourceColumn: string;
  xpath: string;
  targetColumn: string;
  dataType?: string;
}

export class ExtractXMLSQLGenerator extends BaseSQLGenerator {
  // Implement all required abstract methods with empty fragments
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

  // Main select generation
  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    const config = node.metadata?.configuration?.config;

    // Type guard: check if config is an object with required properties
    if (!config || typeof config !== 'object' || !('sourceColumn' in config) || !('xpath' in config) || !('targetColumn' in config)) {
      return this.fallbackSelect(node);
    }

    // Now TypeScript knows config has sourceColumn, xpath, targetColumn
    const typedConfig = config as XMLExtractConfig;

    // PostgreSQL: use xpath function
    let expr = `(xpath('${this.escapeString(typedConfig.xpath)}', ${this.sanitizeIdentifier(typedConfig.sourceColumn)}::xml))[1]::text`;
    if (typedConfig.dataType) {
      expr = `(${expr})::${typedConfig.dataType.toLowerCase()}`;
    }

    const otherColumns = node.metadata?.schemas?.output?.fields
      .filter(c => c.name !== typedConfig.sourceColumn)
      .map(c => this.sanitizeIdentifier(c.name)) || [];

    const selectList = [...otherColumns, `${expr} AS ${this.sanitizeIdentifier(typedConfig.targetColumn)}`].join(', ');
    const sql = `SELECT ${selectList} FROM source_table`;

    return {
      sql,
      dependencies: ['source_table'],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'extract_xml', lineCount: 1 }
    };
  }

  // Fallback when config is missing/invalid
  private fallbackSelect(_node: UnifiedCanvasNode): GeneratedSQLFragment {
    const sql = `SELECT * FROM source_table`;
    return {
      sql,
      dependencies: ['source_table'],
      parameters: new Map(),
      errors: [{
        code: 'MISSING_XML_EXTRACT_CONFIG',
        message: 'XML extract configuration is missing or incomplete',
        severity: 'ERROR',
        suggestion: 'Ensure sourceColumn, xpath, and targetColumn are defined in node.config'
      }],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'fallback_select', lineCount: 1 }
    };
  }

  // Helper to produce an empty fragment
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