// src/generators/ExtractRegexSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';
import { UnifiedCanvasNode } from '../types/unified-pipeline.types';

interface RegexExtractConfig {
  sourceColumn: string;
  pattern: string;
  targetColumns: Array<{ name: string; group: number }>;
  flags?: string;
}

export class ExtractRegexSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    const config = this.extractRegexConfig(node);

    if (!config) {
      return this.fallbackSelect(node);
    }

    // PostgreSQL: use regexp_matches to extract capturing groups
    const expressions = config.targetColumns.map(tc => {
      // Each target column corresponds to a capturing group
      return `(regexp_matches(${this.sanitizeIdentifier(config.sourceColumn)}, '${this.escapeString(config.pattern)}', '${config.flags || ''}'))[${tc.group}] AS ${this.sanitizeIdentifier(tc.name)}`;
    });

    const otherColumns = node.metadata?.schemas?.output?.fields
      .filter(c => c.name !== config.sourceColumn)
      .map(c => this.sanitizeIdentifier(c.name)) || [];

    const selectList = [...otherColumns, ...expressions].join(', ');
    const sql = `SELECT ${selectList} FROM source_table`;

    return {
      sql,
      dependencies: ['source_table'],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'extract_regex', lineCount: 1 }
    };
  }

  // Implement remaining abstract methods (all return empty fragments)
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

  // Helper to produce a default fragment when configuration is missing
  private fallbackSelect(_node: UnifiedCanvasNode): GeneratedSQLFragment {
    return {
      sql: `SELECT * FROM source_table`,
      dependencies: ['source_table'],
      parameters: new Map(),
      errors: [{
        code: 'MISSING_REGEX_CONFIG',
        message: 'Regex extract configuration is missing or incomplete',
        severity: 'ERROR',
        suggestion: 'Ensure sourceColumn, pattern, and targetColumns are defined in node.configuration'
      }],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'extract_regex_fallback',
        lineCount: 1
      }
    };
  }

  // Extract and validate the regex configuration from the node
  private extractRegexConfig(node: UnifiedCanvasNode): RegexExtractConfig | null {
    const config = node.metadata?.configuration?.config as any; // Use 'any' temporarily for runtime checks
    if (!config || typeof config !== 'object') return null;

    // Validate required fields
    if (
      typeof config.sourceColumn !== 'string' ||
      typeof config.pattern !== 'string' ||
      !Array.isArray(config.targetColumns) ||
      config.targetColumns.length === 0
    ) {
      return null;
    }

    // Validate each target column
    const validTargetColumns = config.targetColumns.every(
      (tc: any) =>
        typeof tc === 'object' &&
        typeof tc.name === 'string' &&
        typeof tc.group === 'number' &&
        tc.group > 0
    );

    if (!validTargetColumns) return null;

    return {
      sourceColumn: config.sourceColumn,
      pattern: config.pattern,
      targetColumns: config.targetColumns.map((tc: any) => ({
        name: tc.name,
        group: tc.group
      })),
      flags: typeof config.flags === 'string' ? config.flags : ''
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