// src/generators/ExtractDelimitedSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';
import { UnifiedCanvasNode, UnifiedCanvasConnection } from '../types/unified-pipeline.types';
import { globalLogger } from '../utils/Logger';

interface ExtractDelimitedConfig {
  sourceColumn: string;
  delimiter: string;
  outputColumns: Array<{ name: string; position: number; type: string }>;
  quoteChar?: string;
}

export class ExtractDelimitedSQLGenerator extends BaseSQLGenerator {
  constructor(options?: any) {
    super(options);
    globalLogger.debug(`[ExtractDelimitedSQLGenerator] Constructor called with options:`, options);
  }

  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node, connection, upstreamSchema } = context;
    globalLogger.debug(`[ExtractDelimitedSQLGenerator] generateSelectStatement called for node ${node.id} (${node.name})`, {
      hasConfig: !!node.metadata?.extractDelimitedConfig,
      hasConnection: !!connection,
      upstreamSchemaLength: upstreamSchema?.length,
    });

    const config = node.metadata?.extractDelimitedConfig as ExtractDelimitedConfig | undefined;

    if (!config || !config.sourceColumn || !config.delimiter || !config.outputColumns?.length) {
      globalLogger.warn(`[ExtractDelimitedSQLGenerator] Missing or incomplete extract configuration for node ${node.id}, using fallback`);
      return this.fallbackSelect(node, connection);
    }

    const errors: any[] = [];
    const warnings: string[] = [];

    // Extract target column names from the outputColumns array
    const targetColumnNames = config.outputColumns.map(col => col.name);

    // Build SPLIT_PART expressions for each target column
    const expressions = targetColumnNames.map((col, idx) => {
      const pos = idx + 1;
      const escapedDelim = this.escapeString(config.delimiter);
      return `split_part(${this.sanitizeIdentifier(config.sourceColumn)}, '${escapedDelim}', ${pos}) AS ${this.sanitizeIdentifier(col)}`;
    });

    // Preserve all upstream columns except the source column being split
    const otherColumns: string[] = [];
    if (upstreamSchema && upstreamSchema.length > 0) {
      otherColumns.push(
        ...upstreamSchema
          .filter(col => col.name !== config.sourceColumn)
          .map(col => this.sanitizeIdentifier(col.name))
      );
      globalLogger.debug(`[ExtractDelimitedSQLGenerator] Added ${otherColumns.length} other columns from upstream schema`);
    } else if (node.metadata?.schemas?.output?.fields) {
      otherColumns.push(
        ...node.metadata.schemas.output.fields
          .filter((f: any) => f.name !== config.sourceColumn)
          .map((f: any) => this.sanitizeIdentifier(f.name))
      );
      warnings.push('Using output schema fields because upstream schema not available');
      globalLogger.warn(`[ExtractDelimitedSQLGenerator] Using output schema fields as fallback`);
    }

    const selectList = [...otherColumns, ...expressions].join(', ');

    // Determine source table reference from the incoming connection
    const sourceTable = connection
      ? this.sanitizeIdentifier(connection.sourceNodeId)
      : 'source'; // should not happen in a valid pipeline

    const sql = `SELECT ${selectList} FROM ${sourceTable}`;
    globalLogger.debug(`[ExtractDelimitedSQLGenerator] Generated SQL: ${sql.substring(0, 200)}...`);

    return {
      sql,
      dependencies: connection ? [connection.sourceNodeId] : [],
      parameters: new Map(),
      errors,
      warnings,
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'extract_delimited',
        lineCount: sql.split('\n').length,
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

  private fallbackSelect(node: UnifiedCanvasNode, connection?: UnifiedCanvasConnection): GeneratedSQLFragment {
    const sourceTable = connection
      ? this.sanitizeIdentifier(connection.sourceNodeId)
      : this.sanitizeIdentifier(node.name.toLowerCase().replace(/\s+/g, '_'));

    const sql = `SELECT * FROM ${sourceTable}`;
    globalLogger.debug(`[ExtractDelimitedSQLGenerator] Fallback SQL: ${sql}`);

    return {
      sql,
      dependencies: connection ? [connection.sourceNodeId] : [],
      parameters: new Map(),
      errors: [],
      warnings: ['Using fallback SELECT * because extract configuration is missing or incomplete'],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'fallback_extract',
        lineCount: 1,
      },
    };
  }
}