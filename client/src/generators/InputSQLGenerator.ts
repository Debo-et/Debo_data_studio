// src/generators/InputSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment, SQLGenerationError } from './BaseSQLGenerator';
import { UnifiedCanvasNode, isInputConfig, InputComponentConfiguration } from '../types/unified-pipeline.types';
import { globalLogger } from '../utils/Logger';

export class InputSQLGenerator extends BaseSQLGenerator {
  // ==================== CONSTRUCTOR WITH LOGGING ====================
  constructor(options?: any) {
    super(options);
    globalLogger.debug(`[InputSQLGenerator] Constructor called with options:`, options);
  }

  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    globalLogger.debug(`[InputSQLGenerator] generateSelectStatement called for node ${context.node.id} (${context.node.name})`, {
      hasUpstreamSchema: !!context.upstreamSchema,
      upstreamSchemaLength: context.upstreamSchema?.length,
      hasConnection: !!context.connection,
      indentLevel: context.indentLevel,
      options: context.options,
    });

    const { node, upstreamSchema, options } = context;
    const errors: SQLGenerationError[] = [];
    const warnings: string[] = [];

    // 1. Determine target table name – prioritize tableMapping (used by test helpers)
    let tableName = this.getTableName(node, warnings);
    globalLogger.debug(`[InputSQLGenerator] Resolved table name: ${tableName}`);

    if (!tableName) {
      errors.push({
        code: 'MISSING_TABLE_NAME',
        message: 'Input node does not specify a table name',
        severity: 'ERROR',
        suggestion: 'Provide tableName in node metadata (tableMapping.tableName) or configuration',
      });
      globalLogger.error(`[InputSQLGenerator] Missing table name for node ${node.id}`);
      return this.errorFragment('input_select', errors, warnings);
    }
    const sanitizedTable = this.sanitizeIdentifier(tableName);
    globalLogger.debug(`[InputSQLGenerator] Sanitized table name: ${sanitizedTable}`);

    // 2. Determine column list
    let columnList: string[] = [];
    const config = this.getInputConfig(node);

    // Prefer explicit pushdown column selection
    if (config?.pushdown?.enabled && config.pushdown.columnSelection?.length) {
      columnList = config.pushdown.columnSelection;
      globalLogger.debug(`[InputSQLGenerator] Using pushdown column selection: ${columnList.join(', ')}`);
    }
    // Otherwise, use upstream schema if available (from pipeline propagation)
    else if (upstreamSchema && upstreamSchema.length > 0) {
      columnList = upstreamSchema.map(col => col.name);
      globalLogger.debug(`[InputSQLGenerator] Using upstream schema columns: ${columnList.join(', ')}`);
    }
    // Fallback to node's output schema metadata
    else if (node.metadata?.schemas?.output?.fields?.length) {
      columnList = node.metadata.schemas.output.fields.map(f => f.name);
      globalLogger.debug(`[InputSQLGenerator] Using output schema fields: ${columnList.join(', ')}`);
    }

    let selectClause: string;
    if (columnList.length > 0) {
      selectClause = columnList.map(col => this.sanitizeIdentifier(col)).join(', ');
      globalLogger.debug(`[InputSQLGenerator] SELECT clause with ${columnList.length} columns`);
    } else {
      selectClause = '*';
      warnings.push('No column information available, using SELECT *');
      globalLogger.warn(`[InputSQLGenerator] No column info, using SELECT *`);
    }

    // 3. Build base SELECT – no extra parentheses or aliases (pipeline will handle wrapping)
    let sql = `SELECT ${selectClause} FROM ${sanitizedTable}`;
    globalLogger.debug(`[InputSQLGenerator] Base SELECT SQL: ${sql}`);

    // 4. Apply pushdown filter if configured
    if (config?.pushdown?.enabled && config.pushdown.filterClause) {
      sql += `\nWHERE ${config.pushdown.filterClause}`;
      globalLogger.debug(`[InputSQLGenerator] Added pushdown filter: ${config.pushdown.filterClause}`);
    }

    // 5. Apply pushdown limit
    if (config?.pushdown?.enabled && config.pushdown.limit) {
      sql += `\nLIMIT ${config.pushdown.limit}`;
      globalLogger.debug(`[InputSQLGenerator] Added pushdown limit: ${config.pushdown.limit}`);
    }

    // 6. Add table alias if requested (rarely used)
    if (config?.sqlGeneration?.alias) {
      sql += ` AS ${this.sanitizeIdentifier(config.sqlGeneration.alias)}`;
      globalLogger.debug(`[InputSQLGenerator] Added table alias: ${config.sqlGeneration.alias}`);
    }

    // 7. Add comments if requested
    if (options.includeComments) {
      sql = this.addInputComments(sql, node, tableName);
    }

    globalLogger.debug(`[InputSQLGenerator] Final SQL: ${sql.substring(0, 300)}...`);

    return {
      sql,
      dependencies: [tableName],
      parameters: new Map(),
      errors,
      warnings,
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'input_select',
        lineCount: sql.split('\n').length,
        tableName,
        columnCount: columnList.length || -1,
      },
    };
  }

  // ----------------------------------------------------------------------
  // Required abstract method implementations (unused for input)
  // ----------------------------------------------------------------------
  protected generateJoinConditions(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateWhereClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateHavingClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateOrderByClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateGroupByClause(): GeneratedSQLFragment { return this.emptyFragment(); }

  // ----------------------------------------------------------------------
  // Private helpers
  // ----------------------------------------------------------------------
  private getInputConfig(node: UnifiedCanvasNode): InputComponentConfiguration | undefined {
    if (!node.metadata?.configuration) return undefined;
    const conf = node.metadata.configuration;
    return isInputConfig(conf) ? conf.config : undefined;
  }

  private getTableName(node: UnifiedCanvasNode, warnings: string[]): string {
    globalLogger.debug(`[InputSQLGenerator] getTableName called for node ${node.id}`);

    // PRIORITY 1: tableMapping.tableName (used by test helpers)
    if (node.metadata?.tableMapping?.tableName) {
      globalLogger.debug(`[InputSQLGenerator] Using tableMapping.tableName: ${node.metadata.tableMapping.tableName}`);
      return node.metadata.tableMapping.tableName;
    }

    // PRIORITY 2: configuration.sourceDetails.tableName
    const config = this.getInputConfig(node);
    if (config?.sourceDetails?.tableName) {
      globalLogger.debug(`[InputSQLGenerator] Using config.sourceDetails.tableName: ${config.sourceDetails.tableName}`);
      return config.sourceDetails.tableName;
    }

    // PRIORITY 3: node.metadata.tableName (new fallback for test compatibility)
    if (node.metadata?.tableName) {
      globalLogger.debug(`[InputSQLGenerator] Using metadata.tableName: ${node.metadata.tableName}`);
      return node.metadata.tableName;
    }

    // PRIORITY 4: node.metadata.postgresTableName (legacy)
    if (node.metadata?.postgresTableName) {
      globalLogger.debug(`[InputSQLGenerator] Using metadata.postgresTableName: ${node.metadata.postgresTableName}`);
      return node.metadata.postgresTableName;
    }

    // PRIORITY 5: node.metadata.fullRepositoryMetadata?.postgresTableName
    if (node.metadata?.fullRepositoryMetadata?.postgresTableName) {
      globalLogger.debug(`[InputSQLGenerator] Using fullRepositoryMetadata.postgresTableName: ${node.metadata.fullRepositoryMetadata.postgresTableName}`);
      return node.metadata.fullRepositoryMetadata.postgresTableName;
    }

    // PRIORITY 6: fallback to node.name (with warning)
    warnings.push(`No explicit table name for input node "${node.name}", using node name as fallback`);
    globalLogger.warn(`[InputSQLGenerator] No explicit table name, falling back to node name: ${node.name}`);
    return node.name;
  }

  private addInputComments(sql: string, node: UnifiedCanvasNode, tableName: string): string {
    const comments: string[] = [
      `-- Input node: ${node.name} (${node.id})`,
      `-- Source table: ${tableName}`,
    ];
    if (node.metadata?.description) {
      comments.push(`-- Description: ${node.metadata.description}`);
    }
    comments.push('');
    return comments.join('\n') + sql;
  }

  private errorFragment(fragmentType: string, errors: SQLGenerationError[], warnings: string[]): GeneratedSQLFragment {
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

  protected emptyFragment(): GeneratedSQLFragment {
    return {
      sql: '',
      dependencies: [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'empty', lineCount: 0 },
    };
  }
}