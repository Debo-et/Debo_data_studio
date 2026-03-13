// src/generators/ExtractDelimitedSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';
import { UnifiedCanvasNode } from '../types/unified-pipeline.types';

interface ExtractConfig {
  sourceColumn: string;
  delimiter: string;
  targetColumns: string[];
  quoteChar?: string;
}

export class ExtractDelimitedSQLGenerator extends BaseSQLGenerator {
  // ==================== IMPLEMENT ALL ABSTRACT METHODS ====================
  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    // Safely extract and cast the configuration
    const rawConfig = node.metadata?.configuration?.config;
    const config = rawConfig as ExtractConfig | undefined;

    if (!config || !config.sourceColumn || !config.delimiter || !config.targetColumns.length) {
      return this.fallbackSelect(node);
    }

    // PostgreSQL: split_part or regexp_split_to_array
    // For simplicity, use split_part repeated for each target column
    const expressions = config.targetColumns.map((col, idx) => {
      const pos = idx + 1;
      return `split_part(${this.sanitizeIdentifier(config.sourceColumn)}, '${this.escapeString(config.delimiter)}', ${pos}) AS ${this.sanitizeIdentifier(col)}`;
    });

    // Also include original columns except the source column? Usually we keep all.
    const allColumns = node.metadata?.schemas?.output?.fields || [];
    const otherColumns = allColumns
      .filter(c => c.name !== config.sourceColumn)
      .map(c => this.sanitizeIdentifier(c.name));

    const selectList = [...otherColumns, ...expressions].join(', ');
    const sql = `SELECT ${selectList} FROM source_table`;

    return {
      sql,
      dependencies: ['source_table'],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'extract_delimited', lineCount: 1 }
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

  // ==================== PRIVATE HELPERS ====================
  private fallbackSelect(node: UnifiedCanvasNode): GeneratedSQLFragment {
    // A minimal SELECT that at least returns the source table
    const tableName = node.name.toLowerCase().replace(/\s+/g, '_');
    return {
      sql: `SELECT * FROM ${this.sanitizeIdentifier(tableName)}`,
      dependencies: [tableName],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'fallback_select', lineCount: 1 }
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