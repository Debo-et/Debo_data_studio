// src/generators/ExtractJSONSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';
import { UnifiedCanvasNode } from '../types/unified-pipeline.types';

interface JSONExtractConfig {
  sourceColumn: string;
  jsonType: 'json' | 'jsonb';
  mappings: Array<{ targetColumn: string; jsonPath: string; dataType?: string }>;
}

export class ExtractJSONSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    
    // Safely extract and validate configuration
    const config = this.extractConfig(node);
    if (!config) {
      return this.fallbackSelect(node);
    }

    const source = this.sanitizeIdentifier(config.sourceColumn);
    const expressions = config.mappings.map(m => {
      // Use appropriate JSON operator based on type (jsonb recommended)
      const op = config.jsonType === 'jsonb' ? '->>' : '->>';
      let expr = `${source}${op}'${m.jsonPath}'`; // simplified path expression
      if (m.dataType) {
        expr = `(${expr})::${m.dataType.toLowerCase()}`;
      }
      return `${expr} AS ${this.sanitizeIdentifier(m.targetColumn)}`;
    });

    // Include all output fields except the source JSON column
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
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'extract_json',
        lineCount: sql.split('\n').length,
      },
    };
  }

  // Required abstract method implementations (return empty fragments)
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

  // Helper: produce a fallback SELECT when configuration is missing/invalid
  private fallbackSelect(_node: UnifiedCanvasNode): GeneratedSQLFragment {
    const warning = 'Missing or invalid JSON extraction configuration; using SELECT * FROM source_table';
    return {
      sql: 'SELECT * FROM source_table',
      dependencies: ['source_table'],
      parameters: new Map(),
      errors: [],
      warnings: [warning],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'extract_json_fallback',
        lineCount: 1,
      },
    };
  }

  // Helper: produce an empty fragment for unused clauses
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

  // Safely extract and validate the JSON extract configuration without relying on type discriminators
  private extractConfig(node: UnifiedCanvasNode): JSONExtractConfig | null {
    const configuration = node.metadata?.configuration;
    // Check if configuration exists and has a 'config' property that is an object
    if (!configuration || typeof configuration !== 'object' || !('config' in configuration)) {
      return null;
    }

    const rawConfig = (configuration as any).config as Partial<JSONExtractConfig> | undefined;
    if (!rawConfig || typeof rawConfig !== 'object') {
      return null;
    }

    // Validate required fields
    if (typeof rawConfig.sourceColumn !== 'string' || !rawConfig.sourceColumn) {
      return null;
    }
    if (!Array.isArray(rawConfig.mappings) || rawConfig.mappings.length === 0) {
      return null;
    }
    // Optionally validate jsonType
    if (rawConfig.jsonType !== 'json' && rawConfig.jsonType !== 'jsonb') {
      // Default to 'jsonb' if missing or invalid? Or treat as invalid.
      // Here we treat missing/invalid as null, but you could default.
      return null;
    }

    // Validate each mapping (basic check)
    const mappings = rawConfig.mappings.filter(m => 
      m && typeof m.targetColumn === 'string' && typeof m.jsonPath === 'string'
    );
    if (mappings.length === 0) {
      return null;
    }

    // Construct and return a valid config object
    return {
      sourceColumn: rawConfig.sourceColumn,
      jsonType: rawConfig.jsonType,
      mappings: mappings.map(m => ({
        targetColumn: m.targetColumn,
        jsonPath: m.jsonPath,
        dataType: m.dataType, // optional
      })),
    };
  }
}