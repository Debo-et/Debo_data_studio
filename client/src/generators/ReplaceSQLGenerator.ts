// src/generators/ReplaceSQLGenerator.ts
import {
  BaseSQLGenerator,
  SQLGenerationContext,
  GeneratedSQLFragment,
  SQLGenerationError,
} from './BaseSQLGenerator';
import { globalLogger } from '../utils/Logger';

interface ReplaceRule {
  field: string;
  search?: string;
  searchValue?: string;   // alternative naming from test helper
  replace?: string;
  replacement?: string;   // alternative naming
  caseSensitive?: boolean;
  regex?: boolean;
}

export class ReplaceSQLGenerator extends BaseSQLGenerator {
  constructor(options?: any) {
    super(options);
    globalLogger.debug(`[ReplaceSQLGenerator] Constructor called with options:`, options);
  }

  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node, connection, upstreamSchema } = context;
    const errors: SQLGenerationError[] = [];
    const warnings: string[] = [];

    globalLogger.debug(`[ReplaceSQLGenerator] generateSelectStatement called for node ${node.id} (${node.name})`, {
      hasUpstreamSchema: !!upstreamSchema,
      upstreamSchemaLength: upstreamSchema?.length,
      hasConnection: !!connection,
    });

    // 1. Determine source table reference
    let sourceRef: string;
    if (connection) {
      sourceRef = this.sanitizeIdentifier(connection.sourceNodeId);
      globalLogger.debug(`[ReplaceSQLGenerator] Using source reference from connection: ${sourceRef}`);
    } else {
      sourceRef = this.sanitizeIdentifier(node.name);
      warnings.push('No upstream connection; using node name as table reference');
      globalLogger.warn(`[ReplaceSQLGenerator] No upstream connection, falling back to node name: ${sourceRef}`);
    }

    // 2. Extract replace rules (prioritise direct metadata.replaceConfig)
    let rules: ReplaceRule[] = [];
    const replaceConfig = node.metadata?.replaceConfig;
    if (replaceConfig && Array.isArray(replaceConfig.rules)) {
      rules = replaceConfig.rules as ReplaceRule[];
      globalLogger.debug(`[ReplaceSQLGenerator] Found ${rules.length} rules in metadata.replaceConfig`);
    } else {
      const config = node.metadata?.configuration?.config;
      if (config && 'rules' in config && Array.isArray(config.rules)) {
        rules = config.rules as ReplaceRule[];
        globalLogger.debug(`[ReplaceSQLGenerator] Found ${rules.length} rules in legacy configuration.config`);
      }
    }

    if (rules.length === 0) {
      warnings.push('No replace rules configured; using pass‑through');
      return {
        sql: `SELECT * FROM ${sourceRef}`,
        dependencies: [sourceRef],
        parameters: new Map(),
        errors,
        warnings,
        metadata: { generatedAt: new Date().toISOString(), fragmentType: 'replace_fallback', lineCount: 1 },
      };
    }

    // 3. Determine column list from upstream schema
    let columns: string[] = [];
    if (upstreamSchema && upstreamSchema.length > 0) {
      columns = upstreamSchema.map(col => col.name);
    } else if (node.metadata?.schemas?.output?.fields) {
      columns = node.metadata.schemas.output.fields.map((f: any) => f.name);
    } else {
      warnings.push('No schema information available; using SELECT *');
      return {
        sql: `SELECT * FROM ${sourceRef}`,
        dependencies: [sourceRef],
        parameters: new Map(),
        errors,
        warnings,
        metadata: { generatedAt: new Date().toISOString(), fragmentType: 'replace_fallback', lineCount: 1 },
      };
    }

    // 4. Build SELECT expressions with REPLACE applied
    const selectParts = columns.map(col => {
      const rule = rules.find(r => r.field === col);
      if (!rule) {
        return this.sanitizeIdentifier(col);
      }
      const colExpr = this.sanitizeIdentifier(col);
      
      // Normalize property names (support both 'search'/'replace' and 'searchValue'/'replacement')
      const searchStr = rule.search ?? rule.searchValue ?? '';
      const replaceStr = rule.replace ?? rule.replacement ?? '';
      
      if (rule.regex) {
        const expr = `REGEXP_REPLACE(${colExpr}, '${this.escapeString(searchStr)}', '${this.escapeString(replaceStr)}', 'g') AS ${this.sanitizeIdentifier(col)}`;
        globalLogger.debug(`[ReplaceSQLGenerator] Applied regex replace to ${col}: ${expr}`);
        return expr;
      } else {
        const expr = `REPLACE(${colExpr}, '${this.escapeString(searchStr)}', '${this.escapeString(replaceStr)}') AS ${this.sanitizeIdentifier(col)}`;
        globalLogger.debug(`[ReplaceSQLGenerator] Applied replace to ${col}: ${expr}`);
        return expr;
      }
    });

    const sql = `SELECT ${selectParts.join(', ')} FROM ${sourceRef}`;
    globalLogger.debug(`[ReplaceSQLGenerator] Generated SQL: ${sql.substring(0, 200)}...`);

    return {
      sql,
      dependencies: [sourceRef],
      parameters: new Map(),
      errors,
      warnings,
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'replace_select', lineCount: sql.split('\n').length },
    };
  }

  protected generateJoinConditions(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateWhereClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateHavingClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateOrderByClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateGroupByClause(): GeneratedSQLFragment { return this.emptyFragment(); }

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