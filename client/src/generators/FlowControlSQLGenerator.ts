// src/generators/FlowControlSQLGenerator.ts
import {
  BaseSQLGenerator,
  SQLGenerationContext,
  GeneratedSQLFragment,
} from './BaseSQLGenerator';

export class FlowToIterateSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(_context: SQLGenerationContext): GeneratedSQLFragment {
    return this.flowStub('FLOW_TO_ITERATE');
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

  private flowStub(nodeType: string): GeneratedSQLFragment {
    return {
      sql: `-- ${nodeType} node: flow control, no SQL generated`,
      dependencies: [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'flow_stub',
        lineCount: 1,
      },
    };
  }
}

export class IterateToFlowSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(_context: SQLGenerationContext): GeneratedSQLFragment {
    return this.flowStub('ITERATE_TO_FLOW');
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

  private flowStub(nodeType: string): GeneratedSQLFragment {
    return {
      sql: `-- ${nodeType} node: flow control, no SQL generated`,
      dependencies: [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'flow_stub',
        lineCount: 1,
      },
    };
  }
}

export class ReplicateSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(_context: SQLGenerationContext): GeneratedSQLFragment {
    return this.flowStub('REPLICATE');
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

  private flowStub(nodeType: string): GeneratedSQLFragment {
    return {
      sql: `-- ${nodeType} node: flow control, no SQL generated`,
      dependencies: [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'flow_stub',
        lineCount: 1,
      },
    };
  }
}

/**
 * UniteSQLGenerator – generates UNION, UNION ALL, INTERSECT, EXCEPT queries.
 * Uses the incoming node aliases provided in the generation context.
 */
export class UniteSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node, incomingNodeIds, nodeAliasMap, indentLevel } = context;
    const config = node.metadata?.uniteConfig || {};
    const setOperation = config.setOperation || 'UNION';
    const unionAll = config.unionAll !== false; // default true

    const indent = '  '.repeat(indentLevel);
    const branchQueries: string[] = [];

    if (!incomingNodeIds || incomingNodeIds.length === 0) {
      // No branches – return empty fragment (should not happen in valid pipeline)
      return this.emptyFragment();
    }

    for (const srcId of incomingNodeIds) {
      const alias = nodeAliasMap?.get(srcId) ?? srcId;
      branchQueries.push(`${indent}SELECT * FROM ${this.sanitizeIdentifier(alias)}`);
    }

    const operator = setOperation === 'UNION' && unionAll ? 'UNION ALL' : setOperation;
    const sql = branchQueries.join(`\n${indent}${operator}\n`);

    return {
      sql,
      dependencies: incomingNodeIds,
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'unite',
        lineCount: sql.split('\n').length,
      },
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

export class FlowMergeSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(_context: SQLGenerationContext): GeneratedSQLFragment {
    return this.flowStub('FLOW_MERGE');
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

  private flowStub(nodeType: string): GeneratedSQLFragment {
    return {
      sql: `-- ${nodeType} node: flow control, no SQL generated`,
      dependencies: [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'flow_stub',
        lineCount: 1,
      },
    };
  }
}

export class FlowMeterSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(_context: SQLGenerationContext): GeneratedSQLFragment {
    return this.flowStub('FLOW_METER');
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

  private flowStub(nodeType: string): GeneratedSQLFragment {
    return {
      sql: `-- ${nodeType} node: flow control, no SQL generated`,
      dependencies: [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'flow_stub',
        lineCount: 1,
      },
    };
  }
}

export class FlowMeterCatcherSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(_context: SQLGenerationContext): GeneratedSQLFragment {
    return this.flowStub('FLOW_METER_CATCHER');
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

  private flowStub(nodeType: string): GeneratedSQLFragment {
    return {
      sql: `-- ${nodeType} node: flow control, no SQL generated`,
      dependencies: [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'flow_stub',
        lineCount: 1,
      },
    };
  }
}