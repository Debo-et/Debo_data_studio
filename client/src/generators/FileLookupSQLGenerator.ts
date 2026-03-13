// src/generators/FileLookupSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';
import { NodeType } from '../types/unified-pipeline.types'; // adjust import path if needed

export class FileLookupSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;

    // Guard: only handle file lookup nodes
    if (node.type !== NodeType.FILE_LOOKUP) {
      return {
        sql: '',
        dependencies: [],
        parameters: new Map(),
        errors: [{
          code: 'INVALID_NODE_TYPE',
          message: `FileLookupSQLGenerator expects node type FILE_LOOKUP, got ${node.type}`,
          severity: 'ERROR'
        }],
        warnings: [],
        metadata: { generatedAt: new Date().toISOString(), fragmentType: 'error', lineCount: 0 }
      };
    }

    // Type-safe access: assert the expected config shape
    const fileLookupConfig = node.metadata?.configuration?.config as { filePath?: string } | undefined;
    const filePath = fileLookupConfig?.filePath;

    if (!filePath) {
      return {
        sql: '',
        dependencies: [],
        parameters: new Map(),
        errors: [{
          code: 'MISSING_FILE_PATH',
          message: 'File path not specified in node configuration',
          severity: 'ERROR'
        }],
        warnings: [],
        metadata: { generatedAt: new Date().toISOString(), fragmentType: 'error', lineCount: 0 }
      };
    }

    const tableName = `lookup_${node.id.replace(/-/g, '_')}`;

    // For now, we assume a temporary or foreign table with that name already exists.
    const sql = `SELECT * FROM ${this.sanitizeIdentifier(tableName)}`;

    return {
      sql,
      dependencies: [tableName],
      parameters: new Map(),
      errors: [],
      warnings: ['FileLookup requires temporary table creation'],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'file_lookup', lineCount: 1 }
    };
  }

  // Implement remaining abstract methods – file lookup nodes have no joins, filters, etc.
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

  private emptyFragment(): GeneratedSQLFragment {
    return {
      sql: '',
      dependencies: [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'empty', lineCount: 0 }
    };
  }
}