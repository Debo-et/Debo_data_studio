// src/generators/__tests__/JoinSQLGenerator.test.ts
import { JoinSQLGenerator } from '../../generators/JoinSQLGenerator';
import { UnifiedCanvasNode, NodeType, JoinComponentConfiguration, FieldSchema } from '../../types/unified-pipeline.types';
import { SQLGenerationOptions } from '../../generators/BaseSQLGenerator';
import { compareSQL } from '../../test/utils/sqlComparator';

describe('JoinSQLGenerator', () => {
  let generator: JoinSQLGenerator;
  const defaultOptions: SQLGenerationOptions = {
    includeComments: false,
    formatSQL: false,
    targetDialect: 'POSTGRESQL',
    postgresVersion: '14.0',
    useCTEs: false,
    optimizeForReadability: false,
    includeExecutionPlan: false,
    parameterizeValues: false,
    maxLineLength: 80,
  };

  beforeEach(() => {
    generator = new JoinSQLGenerator(defaultOptions);
  });

  const createMockNode = (
    joinConfig: Partial<JoinComponentConfiguration>,
    extraMetadata: Record<string, any> = {}
  ): UnifiedCanvasNode => {
    return {
      id: 'join-node-1',
      name: 'JoinNode',
      type: NodeType.JOIN,
      position: { x: 0, y: 0 },
      size: { width: 200, height: 120 },
      metadata: {
        configuration: {
          type: 'JOIN',
          config: {
            version: '1.0',
            joinType: 'INNER',
            joinConditions: [],
            joinHints: { enableJoinHint: false },
            outputSchema: { fields: [], deduplicateFields: true, fieldAliases: {} },
            sqlGeneration: { joinAlgorithm: 'HASH', estimatedJoinCardinality: 1.0, nullHandling: 'INCLUDE', requiresSort: false, canParallelize: true },
            compilerMetadata: { lastModified: new Date().toISOString() },
            ...joinConfig,
          } as JoinComponentConfiguration,
        },
        ...extraMetadata, // whereClause, leftAlias, rightAlias, etc. go here
      },
    };
  };

  const mockConnection = (sourceNodeId: string, targetNodeId: string): any => ({
    id: 'conn1',
    sourceNodeId,
    targetNodeId,
    dataFlow: { schemaMappings: [] },
    status: 'VALID',
  });

  it('generates INNER JOIN SQL', () => {
    const node = createMockNode({
      joinType: 'INNER',
      joinConditions: [
        { id: 'c1', leftTable: 'left_table', leftField: 'id', rightTable: 'right_table', rightField: 'user_id', operator: '=', position: 0 }
      ],
    });
    const context = {
      node,
      connection: mockConnection('source1', 'join-node-1'),
      indentLevel: 0,
      parameters: new Map(),
      options: defaultOptions,
    };
    const result = generator.generateSQL(context);
    const expected = `
      SELECT
        left_table.*,
        right_table.*
      FROM left_table
      INNER JOIN right_table ON left_table.id = right_table.user_id
    `;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('generates LEFT JOIN SQL with multiple conditions', () => {
    const node = createMockNode({
      joinType: 'LEFT',
      joinConditions: [
        { id: 'c1', leftTable: 'employees', leftField: 'dept_id', rightTable: 'departments', rightField: 'id', operator: '=', position: 0 },
        { id: 'c2', leftTable: 'employees', leftField: 'status', rightTable: 'departments', rightField: 'active', operator: '=', position: 1 }
      ],
    });
    const context = {
      node,
      connection: mockConnection('source1', 'join-node-1'),
      indentLevel: 0,
      parameters: new Map(),
      options: defaultOptions,
    };
    const result = generator.generateSQL(context);
    const expected = `
      SELECT
        employees.*,
        departments.*
      FROM employees
      LEFT JOIN departments ON employees.dept_id = departments.id AND employees.status = departments.active
    `;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('handles CROSS JOIN', () => {
    const node = createMockNode({
      joinType: 'CROSS',
      joinConditions: [],
    });
    const context = {
      node,
      connection: mockConnection('source1', 'join-node-1'),
      indentLevel: 0,
      parameters: new Map(),
      options: defaultOptions,
    };
    const result = generator.generateSQL(context);
    const expected = `
      SELECT
        left_table.*,
        right_table.*
      FROM left_table
      CROSS JOIN right_table
    `;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('includes WHERE clause from node metadata', () => {
    const node = createMockNode({
      joinType: 'INNER',
      joinConditions: [{ id: 'c1', leftTable: 'orders', leftField: 'customer_id', rightTable: 'customers', rightField: 'id', operator: '=', position: 0 }],
    }, { whereClause: 'orders.amount > 100' }); // ✅ whereClause in metadata
    const context = {
      node,
      connection: mockConnection('source1', 'join-node-1'),
      indentLevel: 0,
      parameters: new Map(),
      options: defaultOptions,
    };
    const result = generator.generateSQL(context);
    const expected = `
      SELECT
        orders.*,
        customers.*
      FROM orders
      INNER JOIN customers ON orders.customer_id = customers.id
      WHERE orders.amount > 100
    `;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('applies type casting in SELECT clause when output fields have specified data types', () => {
    const outputFields: FieldSchema[] = [
      { id: 'f1', name: 'order_id', type: 'INTEGER', nullable: true, isKey: true },
      { id: 'f2', name: 'amount_str', type: 'STRING', nullable: true, isKey: false },
    ];
    const node = createMockNode({
      joinType: 'INNER',
      joinConditions: [{ id: 'c1', leftTable: 'orders', leftField: 'id', rightTable: 'customers', rightField: 'order_id', operator: '=', position: 0 }],
      outputSchema: {
        fields: outputFields,
        deduplicateFields: true,
        fieldAliases: {},
      },
    });
    // Override the output schema in the config
    (node.metadata!.configuration as any).config.outputSchema = {
      fields: outputFields,
      deduplicateFields: true,
      fieldAliases: {},
    };
    const context = {
      node,
      connection: mockConnection('source1', 'join-node-1'),
      indentLevel: 0,
      parameters: new Map(),
      options: defaultOptions,
    };
    const result = generator.generateSQL(context);
    const expected = `
      SELECT
        orders.id AS order_id,
        CAST(orders.amount AS TEXT) AS amount_str
      FROM orders
      INNER JOIN customers ON orders.id = customers.order_id
    `;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('handles missing join conditions gracefully', () => {
    const node = createMockNode({
      joinType: 'INNER',
      joinConditions: [],
    });
    const context = {
      node,
      connection: mockConnection('source1', 'join-node-1'),
      indentLevel: 0,
      parameters: new Map(),
      options: defaultOptions,
    };
    const result = generator.generateSQL(context);
    expect(result.warnings).toContainEqual(expect.stringContaining('No join condition specified'));
    expect(result.sql).toContain('INNER JOIN');
    expect(result.sql).not.toContain('ON');
  });

  it('generates SQL with table aliases from configuration', () => {
    // ✅ FIX: leftAlias and rightAlias moved to extraMetadata (second argument)
    const node = createMockNode({
      joinType: 'INNER',
      joinConditions: [{ id: 'c1', leftTable: 'employees', leftField: 'dept_id', rightTable: 'departments', rightField: 'id', operator: '=', position: 0 }],
      // leftAlias and rightAlias are NO LONGER passed inside joinConfig
    }, {
      leftAlias: 'e',
      rightAlias: 'd',
    });

    const context = {
      node,
      connection: mockConnection('source1', 'join-node-1'),
      indentLevel: 0,
      parameters: new Map(),
      options: defaultOptions,
    };
    const result = generator.generateSQL(context);
    const expected = `
      SELECT
        e.*,
        d.*
      FROM employees AS e
      INNER JOIN departments AS d ON e.dept_id = d.id
    `;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });
});