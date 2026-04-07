// src/generators/SQLGeneratorFactory.ts

import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';
import { NodeType } from '../types/pipeline-types';

// ==================== Concrete Generators ====================

class MapSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateJoinConditions(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateWhereClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateHavingClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateOrderByClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateGroupByClause(): GeneratedSQLFragment { return this.emptyFragment(); }

  generateSQL(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node, connection, indentLevel } = context;
    const indent = '  '.repeat(indentLevel);
    const columns = node.metadata?.tableMapping?.columns || [{ name: 'id', dataType: 'INTEGER' }];
    const sourceTable = connection ? this.sanitizeIdentifier(connection.sourceNodeId) : 'source';
    
    const selectColumns = columns.map((col: { name: string; dataType: string }) => this.sanitizeIdentifier(col.name)).join(',\n' + indent + '  ');
    const sql = `${indent}SELECT\n${indent}  ${selectColumns}\n${indent}FROM ${sourceTable}`;
    
    return {
      sql,
      dependencies: connection ? [connection.sourceNodeId] : [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'map',
        lineCount: sql.split('\n').length,
      },
    };
  }
}

class JoinSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateJoinConditions(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateWhereClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateHavingClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateOrderByClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateGroupByClause(): GeneratedSQLFragment { return this.emptyFragment(); }

  generateSQL(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node, indentLevel } = context;
    const indent = '  '.repeat(indentLevel);
    const leftRef = 'left_source';
    const rightRef = 'right_source';
    const joinCondition = node.metadata?.joinCondition || '1 = 1';
    
    const sql = `${indent}SELECT *\n${indent}FROM ${leftRef}\n${indent}JOIN ${rightRef} ON ${joinCondition}`;
    
    return {
      sql,
      dependencies: [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'join',
        lineCount: sql.split('\n').length,
      },
    };
  }
}

class FilterRowSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateJoinConditions(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateWhereClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateHavingClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateOrderByClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateGroupByClause(): GeneratedSQLFragment { return this.emptyFragment(); }

  generateSQL(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node, connection, indentLevel } = context;
    const indent = '  '.repeat(indentLevel);
    const condition = node.metadata?.filterCondition || '1 = 1';
    const sourceTable = connection ? this.sanitizeIdentifier(connection.sourceNodeId) : 'source';
    
    const sql = `${indent}SELECT *\n${indent}FROM ${sourceTable}\n${indent}WHERE ${condition}`;
    
    return {
      sql,
      dependencies: connection ? [connection.sourceNodeId] : [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'filter',
        lineCount: sql.split('\n').length,
      },
    };
  }
}

class AggregateRowSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateJoinConditions(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateWhereClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateHavingClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateOrderByClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateGroupByClause(): GeneratedSQLFragment { return this.emptyFragment(); }

  generateSQL(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node, connection, indentLevel } = context;
    const indent = '  '.repeat(indentLevel);
    const groupBy = node.metadata?.groupByColumns?.join(', ') || '1';
    const sourceTable = connection ? this.sanitizeIdentifier(connection.sourceNodeId) : 'source';
    
    const sql = `${indent}SELECT\n${indent}  ${groupBy},\n${indent}  COUNT(*) as count\n${indent}FROM ${sourceTable}\n${indent}GROUP BY ${groupBy}`;
    
    return {
      sql,
      dependencies: connection ? [connection.sourceNodeId] : [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'aggregate',
        lineCount: sql.split('\n').length,
      },
    };
  }
}

class SortRowSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateJoinConditions(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateWhereClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateHavingClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateOrderByClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateGroupByClause(): GeneratedSQLFragment { return this.emptyFragment(); }

  generateSQL(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node, connection, indentLevel } = context;
    const indent = '  '.repeat(indentLevel);
    const orderBy = node.metadata?.orderBy || 'id ASC';
    const sourceTable = connection ? this.sanitizeIdentifier(connection.sourceNodeId) : 'source';
    
    const sql = `${indent}SELECT *\n${indent}FROM ${sourceTable}\n${indent}ORDER BY ${orderBy}`;
    
    return {
      sql,
      dependencies: connection ? [connection.sourceNodeId] : [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'sort',
        lineCount: sql.split('\n').length,
      },
    };
  }
}

class InputSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateJoinConditions(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateWhereClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateHavingClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateOrderByClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateGroupByClause(): GeneratedSQLFragment { return this.emptyFragment(); }

  generateSQL(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    const tableName = node.metadata?.tableName || this.sanitizeIdentifier(node.name);
    const columns = node.metadata?.columns?.map((c: any) => this.sanitizeIdentifier(c.name)).join(', ') || '*';
    
    const sql = `SELECT ${columns} FROM ${tableName}`;
    
    return {
      sql,
      dependencies: [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'input',
        lineCount: 1,
      },
    };
  }
}

class OutputSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateJoinConditions(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateWhereClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateHavingClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateOrderByClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateGroupByClause(): GeneratedSQLFragment { return this.emptyFragment(); }

  generateSQL(_context: SQLGenerationContext): GeneratedSQLFragment {
    // Output nodes are not used in CTE definitions; they only define the final INSERT target.
    return this.emptyFragment();
  }
}

class DefaultSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateJoinConditions(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateWhereClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateHavingClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateOrderByClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateGroupByClause(): GeneratedSQLFragment { return this.emptyFragment(); }

  generateSQL(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node, connection, indentLevel } = context;
    const indent = '  '.repeat(indentLevel);
    const sourceTable = connection ? this.sanitizeIdentifier(connection.sourceNodeId) : 'unknown_source';
    
    const sql = `${indent}SELECT * FROM ${sourceTable}`;
    
    return {
      sql,
      dependencies: connection ? [connection.sourceNodeId] : [],
      parameters: new Map(),
      errors: [],
      warnings: [`Using default SQL generator for node type ${node.type}`],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'default',
        lineCount: sql.split('\n').length,
      },
    };
  }
}

// ==================== Factory ====================

export class SQLGeneratorFactory {
  static createGenerator(type: string, options?: any): BaseSQLGenerator | undefined {
    switch (type) {
      case NodeType.MAP:
        return new MapSQLGenerator(options);
      case NodeType.JOIN:
        return new JoinSQLGenerator(options);
      case NodeType.FILTER_ROW:
        return new FilterRowSQLGenerator(options);
      case NodeType.AGGREGATE_ROW:
        return new AggregateRowSQLGenerator(options);
      case NodeType.SORT_ROW:
        return new SortRowSQLGenerator(options);
      case NodeType.INPUT:
        return new InputSQLGenerator(options);
      case NodeType.OUTPUT:
        return new OutputSQLGenerator(options);
      default:
        return new DefaultSQLGenerator(options);
    }
  }
}