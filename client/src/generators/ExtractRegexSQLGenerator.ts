// src/generators/ExtractRegexSQLGenerator.ts
import {
  BaseSQLGenerator,
  SQLGenerationContext,
  GeneratedSQLFragment,
} from './BaseSQLGenerator';
import { UnifiedCanvasNode } from '../types/unified-pipeline.types';

interface RegexExtractConfig {
  sourceColumn: string;
  pattern: string;
  targetColumns: Array<{ name: string; group: number }>;
  flags?: string;
}

export class ExtractRegexSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(
    context: SQLGenerationContext
  ): GeneratedSQLFragment {
    const { node, connection, upstreamSchema } = context;
    const config = this.extractRegexConfig(node);

    if (!config) {
      return this.fallbackSelect(context);
    }

    // Determine source reference (table or CTE alias from incoming connection)
    const sourceRef = connection?.sourceNodeId
      ? this.sanitizeIdentifier(connection.sourceNodeId)
      : 'source_table';

    const selectParts: string[] = [];

    // Include all upstream columns except the one being extracted from
    const upstreamCols = upstreamSchema || [];
    for (const col of upstreamCols) {
      if (col.name !== config.sourceColumn) {
        selectParts.push(this.sanitizeIdentifier(col.name));
      }
    }

    // Build regex extraction expressions for each target column
    const escapedPattern = this.escapeString(config.pattern);
    const flagsPart = config.flags ? `, '${this.escapeString(config.flags)}'` : '';

    for (const target of config.targetColumns) {
      // PostgreSQL: (regexp_match(column, pattern[, flags]))[group] AS alias
      const expr = `(regexp_match(${this.sanitizeIdentifier(
        config.sourceColumn
      )}, '${escapedPattern}'${flagsPart}))[${target.group}]`;
      selectParts.push(`${expr} AS ${this.sanitizeIdentifier(target.name)}`);
    }

    const selectClause = selectParts.join(', ');
    const sql = `SELECT ${selectClause} FROM ${sourceRef}`;

    return {
      sql,
      dependencies: connection ? [connection.sourceNodeId] : [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'extract_regex',
        lineCount: 1,
      },
    };
  }

  // Implement remaining abstract methods (return empty fragments)
  protected generateJoinConditions(
    _context: SQLGenerationContext
  ): GeneratedSQLFragment {
    return this.emptyFragment();
  }

  protected generateWhereClause(
    _context: SQLGenerationContext
  ): GeneratedSQLFragment {
    return this.emptyFragment();
  }

  protected generateHavingClause(
    _context: SQLGenerationContext
  ): GeneratedSQLFragment {
    return this.emptyFragment();
  }

  protected generateOrderByClause(
    _context: SQLGenerationContext
  ): GeneratedSQLFragment {
    return this.emptyFragment();
  }

  protected generateGroupByClause(
    _context: SQLGenerationContext
  ): GeneratedSQLFragment {
    return this.emptyFragment();
  }

  // Fallback select when configuration is missing
  private fallbackSelect(context: SQLGenerationContext): GeneratedSQLFragment {
    const { connection } = context;
    const sourceRef = connection?.sourceNodeId
      ? this.sanitizeIdentifier(connection.sourceNodeId)
      : 'source_table';

    return {
      sql: `SELECT * FROM ${sourceRef}`,
      dependencies: connection ? [connection.sourceNodeId] : [],
      parameters: new Map(),
      errors: [
        {
          code: 'MISSING_REGEX_CONFIG',
          message:
            'Regex extract configuration is missing or incomplete',
          severity: 'ERROR',
          suggestion:
            'Ensure sourceColumn, pattern, and targetColumns are defined in node.configuration',
        },
      ],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'extract_regex_fallback',
        lineCount: 1,
      },
    };
  }

  // Extract and validate the regex configuration from the node
  private extractRegexConfig(node: UnifiedCanvasNode): RegexExtractConfig | null {
    // Try multiple possible locations for the configuration
    let config: any = node.metadata?.extractRegexConfig;
    if (!config) {
      config = node.metadata?.configuration?.config;
    }
    if (!config && node.metadata?.configuration && typeof node.metadata.configuration === 'object') {
      config = node.metadata.configuration;
    }

    if (!config || typeof config !== 'object') return null;

    // Validate required fields
    if (
      typeof config.sourceColumn !== 'string' ||
      typeof config.regexPattern !== 'string' ||
      !Array.isArray(config.outputColumns) ||
      config.outputColumns.length === 0
    ) {
      return null;
    }

    // Validate each output column (supports both 'group' and zero-based 'position')
    const validTargetColumns = config.outputColumns.every(
      (oc: any) =>
        oc &&
        typeof oc.name === 'string' &&
        (oc.group !== undefined || oc.position !== undefined)
    );
    if (!validTargetColumns) return null;

    return {
      sourceColumn: config.sourceColumn,
      pattern: config.regexPattern,
      targetColumns: config.outputColumns.map((oc: any) => ({
        name: oc.name,
        group: oc.group !== undefined ? oc.group : oc.position + 1, // position is 0-based
      })),
      flags: typeof config.flags === 'string' ? config.flags : '',
    };
  }
}