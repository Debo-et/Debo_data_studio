// src/generators/UnpivotSQLGenerator.ts
import {
  BaseSQLGenerator,
  SQLGenerationContext,
  GeneratedSQLFragment
} from './BaseSQLGenerator';
import {
  UnifiedCanvasNode,
  getComponentConfig
} from '../types/unified-pipeline.types';

// Expected configuration shape for an unpivot operation.
// (Store this under a component configuration of type 'OTHER'.)
interface UnpivotConfig {
  columnsToUnpivot: string[];  // columns that become rows
  keyColumnName: string;       // name for the new column that stores original column names
  valueColumnName: string;     // name for the new column that stores values
  excludeColumns?: string[];   // columns to keep as-is
}

export class UnpivotSQLGenerator extends BaseSQLGenerator {
  // --- Required abstract method implementations (all return empty fragments) ---
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

  // --- Main SELECT generation ---
  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    const config = this.extractUnpivotConfig(node);

    // If no valid configuration, fall back to a simple SELECT *
    if (!config || !config.columnsToUnpivot.length) {
      return this.fallbackSelect(node);
    }

    const exclude = config.excludeColumns || [];
    const fixedColumns = exclude.map(col => this.sanitizeIdentifier(col)).join(', ');

    // Build UNION ALL queries for each column to unpivot
    const unionQueries = config.columnsToUnpivot.map(col => {
      const selectParts = [];
      if (fixedColumns) {
        selectParts.push(fixedColumns);
      }
      selectParts.push(
        `${this.sanitizeValue(col)} AS ${this.sanitizeIdentifier(config.keyColumnName)}`,
        `${this.sanitizeIdentifier(col)} AS ${this.sanitizeIdentifier(config.valueColumnName)}`
      );
      return `SELECT ${selectParts.join(', ')} FROM source_table`;
    }).join('\nUNION ALL\n');

    const sql = unionQueries;

    return {
      sql,
      dependencies: ['source_table'],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'unpivot',
        lineCount: unionQueries.split('\n').length
      }
    };
  }

  // --- Helper: safe extraction of UnpivotConfig from node metadata ---
  private extractUnpivotConfig(node: UnifiedCanvasNode): UnpivotConfig | undefined {
    const config = node.metadata?.configuration;
    if (!config) return undefined;

    // If the component type is 'OTHER', we assume its config holds our UnpivotConfig.
    // (If you later add a dedicated 'UNPIVOT' type to ComponentConfiguration,
    // you can add a type guard here.)
    if (config.type === 'OTHER') {
      return config.config as UnpivotConfig;
    }

    return undefined;
  }

  // --- Helper: fallback SELECT when no configuration is present ---
  private fallbackSelect(node: UnifiedCanvasNode): GeneratedSQLFragment {
    const tableName = this.extractTableName(node);
    const sql = `SELECT * FROM ${this.sanitizeIdentifier(tableName)}`;

    return {
      sql,
      dependencies: [tableName],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'fallback_select',
        lineCount: 1
      }
    };
  }

  // --- Helper: extract a reasonable table name from the node ---
  private extractTableName(node: UnifiedCanvasNode): string {
    // Try to get from input configuration if available
    const inputConfig = getComponentConfig(node, 'INPUT');
    if (inputConfig && inputConfig.sourceDetails.tableName) {
      return inputConfig.sourceDetails.tableName;
    }
    // Fall back to node name sanitized
    return node.name.toLowerCase().replace(/\s+/g, '_');
  }

  // --- Helper: produce an empty fragment (reduces duplication) ---
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
        lineCount: 0
      }
    };
  }
}