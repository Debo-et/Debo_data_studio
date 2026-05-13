// src/generators/OutputSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment, SQLGenerationError } from './BaseSQLGenerator';
import { UnifiedCanvasNode, isOutputConfig, OutputComponentConfiguration } from '../types/unified-pipeline.types';
import { globalLogger } from '../utils/Logger';

/**
 * OutputSQLGenerator produces the final INSERT statement for a pipeline.
 * It is called by SQLGenerationPipeline when the output node is reached.
 */
export class OutputSQLGenerator extends BaseSQLGenerator {
  // ==================== CONSTRUCTOR WITH LOGGING ====================
  constructor(options?: any) {
    super(options);
    globalLogger.debug(`[OutputSQLGenerator] Constructor called with options:`, options);
  }

  // ==================== TEMPLATE METHOD IMPLEMENTATIONS (unused for output) ====================
  protected generateSelectStatement(_context: SQLGenerationContext): GeneratedSQLFragment {
    return this.emptyFragment();
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

  // ==================== MAIN PUBLIC METHOD ====================

  /**
   * Generate the full INSERT statement. Called by the pipeline when this node is the final sink.
   * @param sourceSQL The SQL for the source data (may be a subquery or CTE reference).
   * @param node The output node (UnifiedCanvasNode).
   * @param mappings Schema mappings from the incoming connection (optional).
   */
  public generateInsertSQL(
    sourceSQL: string,
    node: UnifiedCanvasNode,
    mappings: Array<{ sourceColumn: string; targetColumn: string }> = []
  ): GeneratedSQLFragment {
    globalLogger.debug(`[OutputSQLGenerator] generateInsertSQL called`, {
      nodeId: node.id,
      nodeName: node.name,
      sourceSQLLength: sourceSQL?.length || 0,
      mappingsCount: mappings.length,
    });

    const errors: SQLGenerationError[] = [];
    const warnings: string[] = [];

    // 1. Determine target table name
    let targetTable = this.getTargetTableName(node);
    globalLogger.debug(`[OutputSQLGenerator] Resolved target table name: ${targetTable}`);

    if (!targetTable) {
      errors.push({
        code: 'MISSING_TARGET_TABLE',
        message: 'Output node does not specify a target table name',
        severity: 'ERROR',
        suggestion: 'Provide targetTableName in node metadata (targetTableName) or configuration',
      });
      globalLogger.error(`[OutputSQLGenerator] Missing target table for node ${node.id}`);
      return this.errorFragment('output_insert', errors, warnings);
    }
    const sanitizedTarget = this.sanitizeIdentifier(targetTable);
    globalLogger.debug(`[OutputSQLGenerator] Sanitized target table: ${sanitizedTarget}`);

    // 2. Determine target columns
    let targetColumns: string[] = [];
    if (mappings && mappings.length > 0) {
      // Use explicit schema mappings from the connection
      targetColumns = mappings.map(m => m.targetColumn);
      globalLogger.debug(`[OutputSQLGenerator] Using ${targetColumns.length} target columns from mappings: ${targetColumns.join(', ')}`);
    } else {
      // Fallback: try to extract column names from the source SQL
      targetColumns = this.extractColumnNamesFromSelect(sourceSQL);
      globalLogger.debug(`[OutputSQLGenerator] Extracted ${targetColumns.length} target columns from source SQL: ${targetColumns.join(', ')}`);
      if (targetColumns.length === 0) {
        warnings.push('Could not determine target columns; using SELECT * without explicit column list');
        globalLogger.warn(`[OutputSQLGenerator] No target columns determined, will use SELECT *`);
      }
    }

    // 3. Clean the source SQL: remove trailing semicolon and strip outer parentheses if any (safety)
    let cleanedSource = sourceSQL.trim();
    if (cleanedSource.endsWith(';')) {
      cleanedSource = cleanedSource.slice(0, -1).trim();
      globalLogger.debug(`[OutputSQLGenerator] Removed trailing semicolon from source SQL`);
    }
    // Remove a single pair of outer parentheses that wrap the whole query (common in nested builds)
    if (cleanedSource.startsWith('(') && cleanedSource.endsWith(')')) {
      cleanedSource = cleanedSource.slice(1, -1).trim();
      globalLogger.debug(`[OutputSQLGenerator] Removed outer parentheses from source SQL`);
    }

    // 4. Build INSERT statement
    let sql: string;
    if (targetColumns.length > 0) {
      const columnList = targetColumns.map(c => this.sanitizeIdentifier(c)).join(', ');
      sql = `INSERT INTO ${sanitizedTarget} (${columnList})\n${cleanedSource}`;
      globalLogger.debug(`[OutputSQLGenerator] Built INSERT with explicit column list (${targetColumns.length} columns)`);
    } else {
      sql = `INSERT INTO ${sanitizedTarget}\n${cleanedSource}`;
      globalLogger.debug(`[OutputSQLGenerator] Built INSERT without explicit column list`);
    }

    // 5. Add TRUNCATE if overwrite mode is enabled
    const config = this.getOutputConfig(node);
    if (config?.targetDetails?.mode === 'OVERWRITE') {
      sql = `TRUNCATE ${sanitizedTarget};\n${sql}`;
      globalLogger.debug(`[OutputSQLGenerator] Added TRUNCATE before INSERT (OVERWRITE mode)`);
    }

    // 6. Add semicolon if not present
    if (!sql.trim().endsWith(';')) {
      sql += ';';
      globalLogger.debug(`[OutputSQLGenerator] Added trailing semicolon`);
    }

    globalLogger.debug(`[OutputSQLGenerator] Final SQL length: ${sql.length} characters`);

    return {
      sql,
      dependencies: [targetTable],
      parameters: new Map(),
      errors,
      warnings,
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'output_insert',
        lineCount: sql.split('\n').length,
        targetTable,
        columnCount: targetColumns.length,
      },
    };
  }

  // ==================== HELPER METHODS ====================

  /**
   * Extract target table name from node metadata or configuration.
   * Prioritizes node.metadata.targetTableName (used by test helpers).
   */
  private getTargetTableName(node: UnifiedCanvasNode): string {
    globalLogger.debug(`[OutputSQLGenerator] getTargetTableName called for node ${node.id}`);

    // PRIORITY 1: metadata.targetTableName (used by test helpers)
    if (node.metadata?.targetTableName) {
      globalLogger.debug(`[OutputSQLGenerator] Using metadata.targetTableName: ${node.metadata.targetTableName}`);
      return node.metadata.targetTableName;
    }

    // PRIORITY 2: configuration.targetDetails.tableName
    const config = this.getOutputConfig(node);
    if (config?.targetDetails?.tableName) {
      globalLogger.debug(`[OutputSQLGenerator] Using config.targetDetails.tableName: ${config.targetDetails.tableName}`);
      return config.targetDetails.tableName;
    }

    // PRIORITY 3: node.metadata.postgresTableName (legacy)
    if (node.metadata?.postgresTableName) {
      globalLogger.debug(`[OutputSQLGenerator] Using metadata.postgresTableName: ${node.metadata.postgresTableName}`);
      return node.metadata.postgresTableName;
    }

    // PRIORITY 4: node.metadata.fullRepositoryMetadata?.postgresTableName
    if (node.metadata?.fullRepositoryMetadata?.postgresTableName) {
      globalLogger.debug(`[OutputSQLGenerator] Using fullRepositoryMetadata.postgresTableName: ${node.metadata.fullRepositoryMetadata.postgresTableName}`);
      return node.metadata.fullRepositoryMetadata.postgresTableName;
    }

    // PRIORITY 5: fallback to node.name
    globalLogger.warn(`[OutputSQLGenerator] No explicit target table name, falling back to node name: ${node.name}`);
    return node.name;
  }

  /**
   * Extract the OutputComponentConfiguration from node metadata.
   */
  private getOutputConfig(node: UnifiedCanvasNode): OutputComponentConfiguration | undefined {
    if (!node.metadata?.configuration) return undefined;
    const conf = node.metadata.configuration;
    return isOutputConfig(conf) ? conf.config : undefined;
  }

  /**
   * Locates the part of the SQL between SELECT and FROM, respecting parentheses.
   * The search for 'FROM' ignores occurrences inside parentheses.
   */
  private extractSelectClause(sql: string): string {
    const upperSql = sql.toUpperCase();
    const selectPos = upperSql.indexOf('SELECT');
    if (selectPos === -1) return '';

    let depth = 0;
    let i = selectPos + 6; // length of 'SELECT'

    while (i < sql.length) {
      const ch = sql[i];
      if (ch === '(') depth++;
      if (ch === ')') depth--;

      // Check for "FROM" keyword when depth == 0
      if (depth === 0 && upperSql.substring(i, i + 4) === 'FROM') {
        // Ensure we are at a word boundary
        const prevChar = i > 0 ? sql[i - 1] : ' ';
        const nextChar = i + 4 < sql.length ? sql[i + 4] : ' ';
        if (/\s/.test(prevChar) && /\s/.test(nextChar)) {
          break;
        }
      }
      i++;
    }

    if (i < sql.length) {
      // Found FROM keyword
      return sql.substring(selectPos + 6, i).trim();
    }
    // No FROM clause (e.g., SELECT constant)
    return sql.substring(selectPos + 6).trim();
  }

/**
 * Extract column names from a SELECT statement (robust heuristic).
 * Supports "SELECT a, b, c FROM ..." and "SELECT a AS x, b AS y FROM ..."
 */
private extractColumnNamesFromSelect(sql: string): string[] {
  globalLogger.debug(`[OutputSQLGenerator] extractColumnNamesFromSelect called, SQL length: ${sql.length}`);

  const selectClause = this.extractSelectClause(sql);
  if (!selectClause) {
    globalLogger.warn(`[OutputSQLGenerator] Could not extract SELECT clause from SQL: ${sql.substring(0, 100)}...`);
    return [];
  }

  globalLogger.debug(`[OutputSQLGenerator] SELECT clause: ${selectClause.substring(0, 200)}...`);

  // Split by commas, respecting parentheses (e.g., function calls)
  const parts = this.splitSelectClause(selectClause);
  const columns: string[] = [];

  for (const part of parts) {
    // Look for explicit alias: expression AS alias
    const aliasMatch = part.match(/\s+AS\s+([^\s,]+)$/i);
    if (aliasMatch) {
      columns.push(aliasMatch[1]);
      globalLogger.debug(`[OutputSQLGenerator] Found explicit alias: ${aliasMatch[1]}`);
      continue;
    }

    // Otherwise, take the last token (assuming it's the column name or alias)
    const tokens = part.trim().split(/\s+/);
    const lastToken = tokens[tokens.length - 1];
    // Skip '*' and expressions with parentheses (likely function calls)
    if (lastToken && lastToken !== '*' && !lastToken.includes('(')) {
      columns.push(lastToken);
      globalLogger.debug(`[OutputSQLGenerator] Inferred column name: ${lastToken}`);
    } else {
      globalLogger.debug(`[OutputSQLGenerator] Skipping token: ${lastToken || 'empty'}`);
    }
  }

  // Remove duplicates while preserving order
  const uniqueColumns = [...new Map(columns.map((c, idx) => [c, idx])).keys()];
  globalLogger.debug(`[OutputSQLGenerator] Extracted ${uniqueColumns.length} unique columns: ${uniqueColumns.join(', ')}`);
  return uniqueColumns;
}

  /**
   * Split a SELECT clause by commas, respecting parentheses.
   */
  private splitSelectClause(clause: string): string[] {
    const parts: string[] = [];
    let depth = 0;
    let current = '';
    for (let i = 0; i < clause.length; i++) {
      const ch = clause[i];
      if (ch === '(') depth++;
      if (ch === ')') depth--;
      if (ch === ',' && depth === 0) {
        parts.push(current.trim());
        current = '';
      } else {
        current += ch;
      }
    }
    if (current.trim()) parts.push(current.trim());
    return parts;
  }

  // ==================== PROTECTED OVERRIDES ====================
  
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

  protected errorFragment(
    fragmentType: string,
    errors: SQLGenerationError[],
    warnings: string[] = []
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