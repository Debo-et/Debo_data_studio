import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';
import { FieldSchema } from '../types/unified-pipeline.types';

interface ComplianceConfig {
  schema: FieldSchema[];
  strict: boolean;
  rejectOutput?: string;  // name of reject output
}

export class SchemaComplianceSQLGenerator extends BaseSQLGenerator {
  // ==================== Required abstract implementations ====================
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

  // ==================== Main logic ====================
  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    // Assert the configuration type – we are in the correct generator for this node type
    const config = node.metadata?.configuration?.config as ComplianceConfig | undefined;

    if (!config || !config.schema.length) {
      // Fallback: return a simple SELECT * (previously intended as fallbackSelect)
      return {
        sql: `SELECT * FROM source_table`,
        dependencies: ['source_table'],
        parameters: new Map(),
        errors: [],
        warnings: [],
        metadata: {
          generatedAt: new Date().toISOString(),
          fragmentType: 'schema_compliance_fallback',
          lineCount: 1,
        },
      };
    }

    // Build validation conditions for each column
    const conditions = config.schema
      .map(field => {
        const col = this.sanitizeIdentifier(field.name);
        const checks: string[] = [];

        if (!field.nullable) checks.push(`${col} IS NOT NULL`);
        if (field.type === 'INTEGER') checks.push(`${col} ~ '^\\d+$'`);
        if (field.length) checks.push(`length(${col}::text) <= ${field.length}`);

        return checks.join(' AND ');
      })
      .filter(c => c)
      .map(c => `(${c})`)
      .join(' AND ');

    const isValidExpr = conditions ? `CASE WHEN ${conditions} THEN 1 ELSE 0 END` : '1';

    const sql = `SELECT *, ${isValidExpr} AS __is_valid FROM source_table`;

    return {
      sql,
      dependencies: ['source_table'],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'schema_compliance',
        lineCount: 1,
      },
    };
  }

  // ==================== Helper ====================
  private emptyFragment(): GeneratedSQLFragment {
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