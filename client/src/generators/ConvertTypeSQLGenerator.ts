// src/generators/ConvertTypeSQLGenerator.ts
import {
  BaseSQLGenerator,
  SQLGenerationContext,
  GeneratedSQLFragment,
} from './BaseSQLGenerator';
import { PostgreSQLDataType } from '../types/unified-pipeline.types';

interface ConversionItem {
  column: string; // source column name
  targetType: PostgreSQLDataType;
  alias?: string;
  format?: string; // e.g., for date conversions
  fallbackExpression?: string;
}

// Type guard for the original configuration structure
function isConversionConfig(
  config: any
): config is { conversions: ConversionItem[] } {
  return config && Array.isArray(config.conversions);
}

export class ConvertTypeSQLGenerator extends BaseSQLGenerator {
  /**
   * Generates a SELECT statement that projects only the converted columns.
   * Conversion rules are taken from:
   *   - node.metadata.convertConfig.conversions   (test helper style)
   *   - node.metadata.configuration.config.conversions (original style)
   */
  public generateSelectStatement(
    context: SQLGenerationContext
  ): GeneratedSQLFragment {
    const { node, connection } = context;

    // --------------------------------------------------------------------
    // 1. Extract conversions from all possible metadata locations
    // --------------------------------------------------------------------
    let conversions: ConversionItem[] = [];

    // Test helper style: node.metadata.convertConfig.conversions
    const convertConfig = node.metadata?.convertConfig as any;
    if (convertConfig?.conversions && Array.isArray(convertConfig.conversions)) {
      conversions = convertConfig.conversions.map((c: any) => ({
        column: c.sourceColumn,
        targetType: c.targetType,
        alias: c.targetAlias || c.sourceColumn,
        format: c.format,
        fallbackExpression: c.fallbackExpression,
      }));
    }
    // Original style: node.metadata.configuration.config.conversions
    else if (isConversionConfig(node.metadata?.configuration?.config)) {
      const config = node.metadata.configuration.config;
      conversions = config.conversions.map((c: any) => ({
        column: c.column,
        targetType: c.targetType,
        alias: c.alias || c.column,
        format: c.format,
        fallbackExpression: c.fallbackExpression,
      }));
    }

    // --------------------------------------------------------------------
    // 2. Determine the source reference (from incoming connection)
    // --------------------------------------------------------------------
    const sourceRef = connection
      ? this.sanitizeIdentifier(connection.sourceNodeId)
      : 'source'; // fallback, should not happen in valid pipeline

    // --------------------------------------------------------------------
    // 3. Fallback if no conversions are defined
    // --------------------------------------------------------------------
    if (conversions.length === 0) {
      return {
        sql: `SELECT * FROM ${sourceRef}`,
        dependencies: [sourceRef],
        parameters: new Map(),
        errors: [],
        warnings: [
          'No conversion rules defined, passing through all columns unchanged',
        ],
        metadata: {
          generatedAt: new Date().toISOString(),
          fragmentType: 'convert_fallback',
          lineCount: 1,
        },
      };
    }

    // --------------------------------------------------------------------
    // 4. Build SELECT expressions **only** for the converted columns
    // --------------------------------------------------------------------
    const selectExpressions = conversions.map((conv) => {
      // Start with the sanitized source column
      let expr = this.sanitizeIdentifier(conv.column);

      // Apply format‑specific conversion (e.g., TO_DATE)
      if (conv.format) {
        switch (conv.targetType) {
          case PostgreSQLDataType.DATE:
            expr = `TO_DATE(${expr}, '${this.escapeString(conv.format)}')`;
            break;
          case PostgreSQLDataType.TIMESTAMP:
          case PostgreSQLDataType.TIMESTAMPTZ:
            expr = `TO_TIMESTAMP(${expr}, '${this.escapeString(conv.format)}')`;
            break;
          default:
            expr = `CAST(${expr} AS ${conv.targetType})`;
        }
      } else {
        // Standard CAST
        expr = `CAST(${expr} AS ${conv.targetType})`;
      }

      // Apply fallback expression (COALESCE)
      if (conv.fallbackExpression) {
        expr = `COALESCE(${expr}, ${conv.fallbackExpression})`;
      }

      const alias = conv.alias || conv.column;
      return `${expr} AS ${this.sanitizeIdentifier(alias)}`;
    });

    const sql = `SELECT ${selectExpressions.join(', ')} FROM ${sourceRef}`;

    return {
      sql,
      dependencies: [sourceRef],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'convert_type',
        lineCount: 1,
      },
    };
  }

  // ----------------------------------------------------------------------
  // Required abstract method implementations (unused for Convert)
  // ----------------------------------------------------------------------
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

  // ----------------------------------------------------------------------
  // Helper for empty fragments (delegates to base class protected method)
  // ----------------------------------------------------------------------
  protected emptyFragment(): GeneratedSQLFragment {
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
}