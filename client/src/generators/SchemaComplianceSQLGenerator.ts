// src/generators/SchemaComplianceSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';

// Expected schema item as defined in test helpers
interface ExpectedField {
  name: string;
  type: string;
  nullable?: boolean; // optional, not used for now
}

export class SchemaComplianceSQLGenerator extends BaseSQLGenerator {
  protected generateJoinConditions(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateWhereClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateHavingClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateOrderByClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateGroupByClause(): GeneratedSQLFragment { return this.emptyFragment(); }

  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node, upstreamSchema, connection } = context;
    const warnings: string[] = [];

    // Extract expected schema from node metadata (direct path used by tests)
    const expectedSchema: ExpectedField[] =
      node.metadata?.schemaComplianceConfig?.expectedSchema ?? [];

    if (expectedSchema.length === 0) {
      warnings.push('No expected schema defined; compliance check will be skipped.');
    }

    // Determine source reference (CTE alias or table name)
    let sourceRef: string;
    if (connection?.sourceNodeId) {
      sourceRef = context.nodeAliasMap?.get(connection.sourceNodeId)
        ?? this.sanitizeIdentifier(connection.sourceNodeId);
    } else {
      sourceRef = 'source_table';
      warnings.push('No incoming connection; using default table name "source_table".');
    }

    // Compare actual upstream schema with expected schema
    if (upstreamSchema && expectedSchema.length > 0) {
      const upstreamColMap = new Map(
        upstreamSchema.map(col => [col.name, col.dataType])
      );

      for (const expected of expectedSchema) {
        const actualType = upstreamColMap.get(expected.name);
        if (!actualType) {
          warnings.push(`Expected column "${expected.name}" is missing from upstream schema.`);
          continue;
        }

        // Simple type comparison (case-insensitive)
        const expectedTypeUpper = expected.type.toUpperCase();
        const actualTypeUpper = actualType.toUpperCase();
        if (expectedTypeUpper !== actualTypeUpper) {
          warnings.push(
            `Column "${expected.name}" type mismatch: expected ${expected.type}, got ${actualType}.`
          );
        }

        // Note: nullable checks are not performed because upstreamSchema does not carry nullability.
      }
    } else if (!upstreamSchema) {
      warnings.push('No upstream schema information available; skipping compliance checks.');
    }

    // Pass-through SELECT (no actual filtering; compliance is advisory)
    const sql = `SELECT * FROM ${sourceRef}`;

    return {
      sql,
      dependencies: connection ? [connection.sourceNodeId] : [],
      parameters: new Map(),
      errors: [],
      warnings,
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'schema_compliance',
        lineCount: 1,
      },
    };
  }
}