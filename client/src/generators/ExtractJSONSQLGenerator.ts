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
    const { node, connection, upstreamSchema } = context;
    
    const config = this.extractConfig(node);
    if (!config) {
      return this.fallbackSelect(context);
    }

    const sourceColumn = this.sanitizeIdentifier(config.sourceColumn);
    const sourceRef = connection
      ? this.sanitizeIdentifier(connection.sourceNodeId)
      : 'source_table';

    // Collect other columns to keep (everything except the JSON source column)
    const otherColumns: string[] = [];
    if (upstreamSchema && upstreamSchema.length > 0) {
      otherColumns.push(...upstreamSchema
        .filter(col => col.name !== config.sourceColumn)
        .map(col => this.sanitizeIdentifier(col.name))
      );
    } else {
      // Fallback to node's output schema
      const outputFields = node.metadata?.schemas?.output?.fields || [];
      otherColumns.push(...outputFields
        .filter((f: any) => f.name !== config.sourceColumn)
        .map((f: any) => this.sanitizeIdentifier(f.name))
      );
    }

    // Build JSON extraction expressions
    const expressions = config.mappings.map(m => {
      const op = config.jsonType === 'jsonb' ? '->>' : '->>';
      let expr = `${sourceColumn}${op}'${m.jsonPath}'`;
      if (m.dataType) {
        expr = `(${expr})::${m.dataType.toLowerCase()}`;
      }
      return `${expr} AS ${this.sanitizeIdentifier(m.targetColumn)}`;
    });

    const selectList = [...otherColumns, ...expressions].join(', ');
    const sql = `SELECT ${selectList} FROM ${sourceRef}`;

    return {
      sql,
      dependencies: connection ? [connection.sourceNodeId] : [],
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

  // ========== Configuration Extraction ==========
  private extractConfig(node: UnifiedCanvasNode): JSONExtractConfig | null {
    // 1. Legacy metadata.extractJSONConfig
    const legacy = (node.metadata as any)?.extractJSONConfig;
    if (legacy) {
      return this.normalizeConfig(legacy);
    }

    // 2. Unified configuration
    const configuration = node.metadata?.configuration;
    if (configuration && typeof configuration === 'object' && 'config' in configuration) {
      const raw = (configuration as any).config;
      if (raw) {
        return this.normalizeConfig(raw);
      }
    }

    return null;
  }

  private normalizeConfig(raw: any): JSONExtractConfig | null {
    if (typeof raw.sourceColumn !== 'string' || !Array.isArray(raw.mappings)) {
      return null;
    }
    const mappings = raw.mappings.filter((m: any) =>
      m && typeof m.targetColumn === 'string' && typeof m.jsonPath === 'string'
    );
    if (mappings.length === 0) return null;

    return {
      sourceColumn: raw.sourceColumn,
      jsonType: raw.jsonType === 'jsonb' ? 'jsonb' : 'json',
      mappings: mappings.map((m: any) => ({
        targetColumn: m.targetColumn,
        jsonPath: m.jsonPath,
        dataType: m.dataType,
      })),
    };
  }

  // ========== Fallback ==========
  private fallbackSelect(context: SQLGenerationContext): GeneratedSQLFragment {
    const sourceRef = context.connection
      ? this.sanitizeIdentifier(context.connection.sourceNodeId)
      : 'source_table';
    const warning = 'Missing or invalid JSON extraction configuration; using SELECT *';
    return {
      sql: `SELECT * FROM ${sourceRef}`,
      dependencies: context.connection ? [context.connection.sourceNodeId] : [],
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

  // ========== Required Abstract Methods (Unused) ==========
  protected generateJoinConditions(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateWhereClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateHavingClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateOrderByClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateGroupByClause(): GeneratedSQLFragment { return this.emptyFragment(); }
}