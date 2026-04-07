import { SQLGenerationPipeline } from '../../generators/SQLGenerationPipeline';
import {
  CanvasNode,
  CanvasConnection,
  ConnectionStatus,
  PostgreSQLDataType,
  NodeType,
} from '../../types/pipeline-types';
import { compareSQL } from '../utils/sqlComparator';

describe('UniqRow Integration', () => {
  it('generates correct pipeline SQL for a simple uniq row operation', async () => {
    // Input node
    const inputNode: CanvasNode = {
      id: 'input1',
      name: 'customers',
      type: NodeType.INPUT,
      position: { x: 0, y: 0 },
      size: { width: 180, height: 100 },
      metadata: {
        tableMapping: {
          schema: 'public',
          name: 'customers',
          columns: [
            { name: 'id', dataType: PostgreSQLDataType.INTEGER, nullable: false },
            { name: 'first_name', dataType: PostgreSQLDataType.VARCHAR, nullable: true },
            { name: 'last_name', dataType: PostgreSQLDataType.VARCHAR, nullable: true },
            { name: 'email', dataType: PostgreSQLDataType.VARCHAR, nullable: true },
          ],
        },
      } as any, // Cast to any to satisfy NodeMetadata constraints (test uses custom config)
    };

    // UniqRow node
    const uniqNode: CanvasNode = {
      id: 'uniq1',
      name: 'Deduplicate by email',
      type: NodeType.UNIQ_ROW,
      position: { x: 200, y: 0 },
      size: { width: 160, height: 90 },
      metadata: {
        configuration: {
          type: 'UNIQ_ROW',
          config: {
            columns: ['email'],
            keep: 'first',
            treatNullsAsEqual: true,
          },
        },
      } as any,
    };

    // Output node
    const outputNode: CanvasNode = {
      id: 'out1',
      name: 'unique_customers',
      type: NodeType.OUTPUT,
      position: { x: 400, y: 0 },
      size: { width: 180, height: 100 },
      metadata: {
        targetTableName: 'unique_customers_table',
      } as any,
    };

    const connections: CanvasConnection[] = [
      {
        id: 'conn1',
        sourceNodeId: 'input1',
        sourcePortId: 'output-1',
        targetNodeId: 'uniq1',
        targetPortId: 'input-1',
        status: ConnectionStatus.VALID,
        dataFlow: { schemaMappings: [] },
      },
      {
        id: 'conn2',
        sourceNodeId: 'uniq1',
        sourcePortId: 'output-1',
        targetNodeId: 'out1',
        targetPortId: 'input-1',
        status: ConnectionStatus.VALID,
        dataFlow: { schemaMappings: [] },
      },
    ];

    const pipeline = new SQLGenerationPipeline(
      [inputNode, uniqNode, outputNode],
      connections,
      {
        includeComments: false,
        formatSQL: false,
        useCTEs: true,
        postgresVersion: '14.0',
      },
    );

    const result = await pipeline.generate();
    expect(result.errors).toHaveLength(0);
    expect(result.sql).toBeDefined();

    const expectedSQL = `
      WITH
      cte_input1 AS (
        SELECT * FROM customers
      ),
      cte_uniq1 AS (
        SELECT DISTINCT ON (email) * FROM cte_input1 ORDER BY email, id ASC
      )
      INSERT INTO unique_customers_table (id, first_name, last_name, email)
      SELECT id, first_name, last_name, email FROM cte_uniq1;
    `;

    const comparison = compareSQL(result.sql, expectedSQL);
    expect(comparison.success).toBe(true);
  });

  it('handles multiple key fields and LAST strategy', async () => {
    const inputNode: CanvasNode = {
      id: 'input1',
      name: 'orders',
      type: NodeType.INPUT,
      position: { x: 0, y: 0 },
      size: { width: 180, height: 100 },
      metadata: {
        tableMapping: {
          schema: 'public',
          name: 'orders',
          columns: [
            { name: 'order_id', dataType: PostgreSQLDataType.INTEGER, nullable: false },
            { name: 'customer_id', dataType: PostgreSQLDataType.INTEGER, nullable: false },
            { name: 'order_date', dataType: PostgreSQLDataType.DATE, nullable: true },
            { name: 'amount', dataType: PostgreSQLDataType.DECIMAL, nullable: true },
          ],
        },
      } as any,
    };

    const uniqNode: CanvasNode = {
      id: 'uniq1',
      name: 'Latest order per customer',
      type: NodeType.UNIQ_ROW,
      position: { x: 200, y: 0 },
      size: { width: 160, height: 90 },
      metadata: {
        configuration: {
          type: 'UNIQ_ROW',
          config: {
            columns: ['customer_id'],
            keep: 'last',
            sortFields: [{ field: 'order_date', direction: 'DESC' }],
          },
        },
      } as any,
    };

    const outputNode: CanvasNode = {
      id: 'out1',
      name: 'latest_orders',
      type: NodeType.OUTPUT,
      position: { x: 400, y: 0 },
      size: { width: 180, height: 100 },
      metadata: { targetTableName: 'latest_orders' } as any,
    };

    const connections: CanvasConnection[] = [
      {
        id: 'c1',
        sourceNodeId: 'input1',
        sourcePortId: 'output-1',
        targetNodeId: 'uniq1',
        targetPortId: 'input-1',
        status: ConnectionStatus.VALID,
        dataFlow: { schemaMappings: [] },
      },
      {
        id: 'c2',
        sourceNodeId: 'uniq1',
        sourcePortId: 'output-1',
        targetNodeId: 'out1',
        targetPortId: 'input-1',
        status: ConnectionStatus.VALID,
        dataFlow: { schemaMappings: [] },
      },
    ];

    const pipeline = new SQLGenerationPipeline(
      [inputNode, uniqNode, outputNode],
      connections,
      {
        useCTEs: true,
        includeComments: false,
        formatSQL: false,
        postgresVersion: '14.0',
      },
    );

    const result = await pipeline.generate();

    const expectedSQL = `
      WITH
      cte_input1 AS (SELECT * FROM orders),
      cte_uniq1 AS (SELECT DISTINCT ON (customer_id) * FROM cte_input1 ORDER BY customer_id, order_date DESC)
      INSERT INTO latest_orders (order_id, customer_id, order_date, amount)
      SELECT order_id, customer_id, order_date, amount FROM cte_uniq1;
    `;

    const comparison = compareSQL(result.sql, expectedSQL);
    expect(comparison.success).toBe(true);
  });
});