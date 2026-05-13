// src/generators/FileLookupSQLGenerator.ts
import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';
import { NodeType } from '../types/unified-pipeline.types';

export class FileLookupSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;

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
}