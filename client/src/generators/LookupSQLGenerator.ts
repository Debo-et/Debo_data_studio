// src/generators/LookupSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment, SQLGenerationError } from './BaseSQLGenerator';
import { NodeType } from '../types/unified-pipeline.types';
import { globalLogger } from '../utils/Logger';

interface LookupConfig {
  lookupTable: string;
  keyMapping: Array<{ sourceColumn: string; targetColumn: string }>;
  outputColumns: string[];
}

export class LookupSQLGenerator extends BaseSQLGenerator {
  // Required abstract method stubs (unused in lookup)
  protected generateSelectStatement(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateJoinConditions(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateWhereClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateHavingClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateOrderByClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateGroupByClause(): GeneratedSQLFragment { return this.emptyFragment(); }

  generateSQL(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node, connection, upstreamSchema } = context;
    const errors: SQLGenerationError[] = [];
    const warnings: string[] = [];

    // Validate node type
    if (node.type !== NodeType.LOOKUP) {
      errors.push({
        code: 'INVALID_NODE_TYPE',
        message: `LookupSQLGenerator expects node type LOOKUP, got ${node.type}`,
        severity: 'ERROR',
      });
      return this.errorFragment('lookup_select', errors, warnings);
    }

    // Extract configuration
    const config = node.metadata?.lookupConfig as LookupConfig | undefined;
    if (!config) {
      errors.push({
        code: 'MISSING_CONFIG',
        message: 'Lookup node missing lookupConfig metadata',
        severity: 'ERROR',
        suggestion: 'Provide lookupTable, keyMapping, and outputColumns in node.metadata.lookupConfig',
      });
      return this.errorFragment('lookup_select', errors, warnings);
    }

    const { lookupTable, keyMapping, outputColumns } = config;
    if (!lookupTable || !keyMapping?.length || !outputColumns?.length) {
      errors.push({
        code: 'INVALID_CONFIG',
        message: 'lookupTable, keyMapping, and outputColumns are required',
        severity: 'ERROR',
      });
      return this.errorFragment('lookup_select', errors, warnings);
    }

    // Determine source reference – the pipeline will later replace the node ID with the appropriate CTE alias.
    const sourceRef = connection ? this.sanitizeIdentifier(connection.sourceNodeId) : 'source';
    const lookupRef = this.sanitizeIdentifier(lookupTable);

    // Build SELECT list: all source columns + requested lookup output columns
    const sourceColumns = upstreamSchema?.map(col => col.name) ?? ['*'];
    const selectParts: string[] = [];

    // Add source columns with qualification to avoid ambiguity
    sourceColumns.forEach(col => {
      selectParts.push(`${sourceRef}.${this.sanitizeIdentifier(col)}`);
    });

    // Add lookup columns, also qualified
    outputColumns.forEach(col => {
      selectParts.push(`${lookupRef}.${this.sanitizeIdentifier(col)}`);
    });

    // Build ON conditions from keyMapping
    const onConditions = keyMapping.map(mapping => {
      const leftCol = this.sanitizeIdentifier(mapping.sourceColumn);
      const rightCol = this.sanitizeIdentifier(mapping.targetColumn);
      return `${sourceRef}.${leftCol} = ${lookupRef}.${rightCol}`;
    }).join(' AND ');

    const sql = `SELECT ${selectParts.join(', ')}\nFROM ${sourceRef}\nLEFT JOIN ${lookupRef} ON ${onConditions}`;

    globalLogger.debug(`[LookupSQLGenerator] Generated SQL: ${sql}`);

    return {
      sql,
      dependencies: [sourceRef, lookupTable],
      parameters: new Map(),
      errors,
      warnings,
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'lookup_select',
        lineCount: sql.split('\n').length,
      },
    };
  }

  private errorFragment(fragmentType: string, errors: SQLGenerationError[], warnings: string[]): GeneratedSQLFragment {
    return {
      sql: '',
      dependencies: [],
      parameters: new Map(),
      errors,
      warnings,
      metadata: { generatedAt: new Date().toISOString(), fragmentType, lineCount: 0 },
    };
  }
}