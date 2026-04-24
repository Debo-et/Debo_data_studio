// src/generators/SplitRowSQLGenerator.ts
import {
  BaseSQLGenerator,
  SQLGenerationContext,
  GeneratedSQLFragment,
  SQLGenerationError,
} from './BaseSQLGenerator';
import { UnifiedCanvasNode } from '../types/unified-pipeline.types';

interface SplitRowConfig {
  sourceColumn: string;
  delimiter: string;
  targetColumn: string;
}

export class SplitRowSQLGenerator extends BaseSQLGenerator {
  // ==================== Abstract Method Implementations ====================
  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node, connection, upstreamSchema } = context;
    const config = this.getConfig(node);

    // Validate configuration
    if (!config?.sourceColumn || !config?.delimiter || !config?.targetColumn) {
      return this.fallbackSelect(
        node,
        connection,
        upstreamSchema,
        'Missing required split row configuration (sourceColumn, delimiter, targetColumn)'
      );
    }

    const sourceCol = config.sourceColumn;
    const targetCol = config.targetColumn;
    
    // Escape regex special characters (.*+?^${}()|[]\) and then only escape single quotes.
    // Do NOT use escapeString() here as it would double‑escape backslashes.
    const rawDelim = config.delimiter;
    const escapedForRegex = rawDelim.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const delimiter = escapedForRegex.replace(/'/g, "''");

    // Determine source reference
    const sourceRef = connection
      ? this.sanitizeIdentifier(connection.sourceNodeId)
      : 'source_table';
    const errors: SQLGenerationError[] = [];
    if (!connection) {
      errors.push({
        code: 'MISSING_CONNECTION',
        message: 'SplitRow node requires an incoming connection',
        severity: 'ERROR',
      });
      return this.errorFragment('split_row_select', errors, []);
    }

    // Build SELECT list: preserve all upstream columns except the one being split
    const selectParts: string[] = [];
    if (upstreamSchema && upstreamSchema.length > 0) {
      for (const col of upstreamSchema) {
        const colName = col.name;
        if (colName === sourceCol) {
          selectParts.push(
            `regexp_split_to_table(${this.sanitizeIdentifier(colName)}, '${delimiter}') AS ${this.sanitizeIdentifier(targetCol)}`
          );
        } else {
          selectParts.push(this.sanitizeIdentifier(colName));
        }
      }
    } else {
      selectParts.push(
        `regexp_split_to_table(${this.sanitizeIdentifier(sourceCol)}, '${delimiter}') AS ${this.sanitizeIdentifier(targetCol)}`
      );
    }

    const selectClause = selectParts.join(', ');
    const sql = `SELECT ${selectClause} FROM ${sourceRef}`;

    return {
      sql,
      dependencies: [sourceRef],
      parameters: new Map(),
      errors,
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'split_row_select',
        lineCount: sql.split('\n').length,
        sourceColumn: sourceCol,
        targetColumn: targetCol,
        delimiter: config.delimiter,
      },
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

  // ==================== Helper Methods ====================

  /**
   * Retrieve split-row configuration from node metadata, supporting both
   * unified format (configuration.config) and legacy test format (splitRowConfig).
   */
  private getConfig(node: UnifiedCanvasNode): SplitRowConfig | undefined {
    // Priority 1: unified configuration
    const unifiedConfig = node.metadata?.configuration?.config as SplitRowConfig | undefined;
    if (unifiedConfig?.sourceColumn && unifiedConfig?.delimiter && unifiedConfig?.targetColumn) {
      return unifiedConfig;
    }

    // Priority 2: legacy metadata.splitRowConfig (used by test helpers)
    const legacyConfig = node.metadata?.splitRowConfig as any;
    if (legacyConfig) {
      return {
        sourceColumn: legacyConfig.splitColumn,
        delimiter: legacyConfig.delimiter,
        targetColumn: Array.isArray(legacyConfig.outputColumns)
          ? legacyConfig.outputColumns[0]
          : legacyConfig.targetColumn,
      };
    }

    return undefined;
  }

  /**
   * Fallback select statement when configuration is invalid.
   */
  private fallbackSelect(
    _node: UnifiedCanvasNode,
    connection: SQLGenerationContext['connection'],
    upstreamSchema: SQLGenerationContext['upstreamSchema'],
    reason: string
  ): GeneratedSQLFragment {
    const error: SQLGenerationError = {
      code: 'INVALID_SPLIT_ROW_CONFIG',
      message: reason,
      severity: 'ERROR',
      suggestion:
        'Ensure sourceColumn, delimiter, and targetColumn are defined in node.metadata.configuration.config',
    };

    const sourceRef = connection
      ? this.sanitizeIdentifier(connection.sourceNodeId)
      : 'source_table';
    const selectParts = upstreamSchema?.map(col => this.sanitizeIdentifier(col.name)) ?? ['*'];
    const sql = `-- ERROR: ${reason}\nSELECT ${selectParts.join(', ')} FROM ${sourceRef};`;

    return {
      sql,
      dependencies: [sourceRef],
      parameters: new Map(),
      errors: [error],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'split_row_fallback',
        lineCount: sql.split('\n').length,
      },
    };
  }

  private errorFragment(
    fragmentType: string,
    errors: SQLGenerationError[],
    warnings: string[]
  ): GeneratedSQLFragment {
    return {
      sql: '',
      dependencies: [],
      parameters: new Map(),
      errors,
      warnings,
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType,
        lineCount: 0,
      },
    };
  }
}