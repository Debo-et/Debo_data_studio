// src/generators/DataMaskingSQLGenerator.ts
import {
  BaseSQLGenerator,
  SQLGenerationContext,
  GeneratedSQLFragment,
} from './BaseSQLGenerator';
import { UnifiedCanvasNode } from '../types/unified-pipeline.types';
import { globalLogger } from '../utils/Logger';

// Test metadata uses 'field' and 'maskType', but we unify to an internal format.
interface RawMaskingRule {
  field: string;               // column name
  maskType: string;            // e.g., 'PARTIAL', 'EMAIL'
  parameters?: Record<string, any>;
}

interface MaskingRule {
  column: string;
  method: string;
  params?: Record<string, any>;
}


export class DataMaskingSQLGenerator extends BaseSQLGenerator {
  constructor(options?: any) {
    super(options);
    globalLogger.debug(`[DataMaskingSQLGenerator] Constructor called with options:`, options);
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

  public generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node, connection, upstreamSchema } = context;

    // Determine source reference (node ID or table name)
    const sourceRef = connection
      ? this.sanitizeIdentifier(connection.sourceNodeId)
      : 'source_table';
    globalLogger.debug(`[DataMaskingSQLGenerator] Using source reference: ${sourceRef}`);

    // Extract masking rules from metadata
    let rules: MaskingRule[] = [];

    // Primary: dataMaskingConfig.rules as built by test helper
    const rawRules = node.metadata?.dataMaskingConfig?.rules as RawMaskingRule[] | undefined;
    if (rawRules && rawRules.length > 0) {
      rules = rawRules.map(r => ({
        column: r.field,
        method: r.maskType,
        params: r.parameters,
      }));
      globalLogger.debug(`[DataMaskingSQLGenerator] Found ${rules.length} rules in dataMaskingConfig`);
    }
    // Fallback: configuration object (if present)
    else if (node.metadata?.configuration?.type === 'OTHER') {
      const otherConfig = node.metadata.configuration.config as Record<string, any>;
      rules = (otherConfig.rules || []).map((r: any) => ({
        column: r.column || r.field,
        method: r.method || r.maskType,
        params: r.params || r.parameters,
      }));
      globalLogger.debug(`[DataMaskingSQLGenerator] Found ${rules.length} rules in configuration.config`);
    }

    if (rules.length === 0) {
      globalLogger.warn(`[DataMaskingSQLGenerator] No masking rules found, using fallback SELECT *`);
      return this.fallbackSelect(node, sourceRef);
    }

    // Determine columns to output (use upstream schema if available)
    const allColumns = upstreamSchema && upstreamSchema.length > 0
      ? upstreamSchema
      : (node.metadata?.schemas?.output?.fields || []);

    if (allColumns.length === 0) {
      const sql = `SELECT * FROM ${sourceRef}`;
      globalLogger.warn(`[DataMaskingSQLGenerator] No column schema available, using SELECT *`);
      return {
        sql,
        dependencies: connection ? [connection.sourceNodeId] : [],
        parameters: new Map(),
        errors: [],
        warnings: ['No column information available; using SELECT *'],
        metadata: {
          generatedAt: new Date().toISOString(),
          fragmentType: 'data_masking_fallback',
          lineCount: 1,
        },
      };
    }

    // Build SELECT expressions with masking applied
    const selectExpressions = allColumns.map((col) => {
      const rule = rules.find((r) => r.column === col.name);
      if (!rule) {
        return this.sanitizeIdentifier(col.name);
      }

      const colSan = this.sanitizeIdentifier(col.name);
      let expr: string;

      switch (rule.method) {
        case 'redact':
          expr = `'***'`;
          break;
        case 'hash':
          expr = `md5(${colSan}::text)`;
          break;
        case 'PARTIAL': {
          // Test expects: CONCAT('XXX-XX-', RIGHT(ssn, 4))
          const replacement = this.escapeString(rule.params?.replacement || '****');
          const keepEnd = rule.params?.end ?? 4;
          expr = `CONCAT('${replacement}', RIGHT(${colSan}, ${keepEnd}))`;
          break;
        }
        case 'EMAIL': {
          // Test expects: CONCAT(LEFT(email, 2), '****', SUBSTRING(email FROM POSITION('@' IN email)))
          expr = `CONCAT(LEFT(${colSan}, 2), '****', SUBSTRING(${colSan} FROM POSITION('@' IN ${colSan})))`;
          break;
        }
        case 'random':
          expr = `(random()*1000)::int::text`;
          break;
        default:
          expr = colSan;
      }

      return `${expr} AS ${colSan}`;
    });

    const sql = `SELECT ${selectExpressions.join(', ')} FROM ${sourceRef}`;
    globalLogger.debug(`[DataMaskingSQLGenerator] Generated SQL: ${sql.substring(0, 200)}...`);

    return {
      sql,
      dependencies: connection ? [connection.sourceNodeId] : [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'data_masking',
        lineCount: 1,
      },
    };
  }

  private fallbackSelect(node: UnifiedCanvasNode, sourceRef: string): GeneratedSQLFragment {
    const allColumns = node.metadata?.schemas?.output?.fields || [];
    let sql: string;
    if (allColumns.length > 0) {
      const selectExpressions = allColumns.map((col) => this.sanitizeIdentifier(col.name));
      sql = `SELECT ${selectExpressions.join(', ')} FROM ${sourceRef}`;
    } else {
      sql = `SELECT * FROM ${sourceRef}`;
    }

    return {
      sql,
      dependencies: [],
      parameters: new Map(),
      errors: [],
      warnings: ['No masking rules defined; using fallback SELECT'],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'data_masking_fallback',
        lineCount: 1,
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