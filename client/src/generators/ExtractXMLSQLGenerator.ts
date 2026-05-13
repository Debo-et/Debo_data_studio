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
    const { node, connection, upstreamSchema } = context;

    // Read configuration from the correct metadata key used by the test helper
    const config = node.metadata?.extractXMLConfig as XMLExtractConfig | undefined;

    if (!config || typeof config !== 'object' || !config.sourceColumn || !config.xpath || !config.targetColumn) {
      return this.fallbackSelect(node);
    }

    // Determine the source table / subquery reference
    let sourceRef: string;
    if (connection) {
      // Use the source node ID (which will be replaced by the pipeline with the actual subquery/table)
      sourceRef = this.sanitizeIdentifier(connection.sourceNodeId);
    } else {
      // Fallback: try to get table name from node metadata
      sourceRef = node.metadata?.tableMapping?.tableName ||
                 node.metadata?.tableName ||
                 node.metadata?.postgresTableName ||
                 'source_table';
    }

    // Build the XML extraction expression
    let extractExpr = `(xpath('${this.escapeString(config.xpath)}', ${this.sanitizeIdentifier(config.sourceColumn)}::xml))[1]::text`;
    if (config.dataType) {
      extractExpr = `(${extractExpr})::${config.dataType.toLowerCase()}`;
    }

    // Build column list: preserve all upstream columns except the source XML column,
    // and add the extracted column with its alias.
    const selectParts: string[] = [];
    const upstreamColumns = upstreamSchema || [];

    for (const col of upstreamColumns) {
      if (col.name === config.sourceColumn) {
        // Skip the raw XML column; we'll add the extracted expression instead
        continue;
      }
      selectParts.push(this.sanitizeIdentifier(col.name));
      if (col.name.toLowerCase() === 'id') {
      }
    }

    // If no upstream schema was provided, fall back to a safe default that includes '*'
    if (upstreamColumns.length === 0) {
      // Without schema info, we cannot reliably list columns. Use '*' and hope the pipeline handles it.
      // This is a fallback; the test will provide schema via upstream propagation.
      selectParts.push('*');
    }

    // Add the extracted column
    selectParts.push(`${extractExpr} AS ${this.sanitizeIdentifier(config.targetColumn)}`);

    const selectClause = selectParts.join(', ');
    const sql = `SELECT ${selectClause} FROM ${sourceRef}`;

    return {
      sql,
      dependencies: connection ? [connection.sourceNodeId] : [],
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
        suggestion: 'Ensure sourceColumn, xpath, and targetColumn are defined in node.metadata.extractXMLConfig'
      }],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'fallback_select', lineCount: 1 }
    };
  }
}