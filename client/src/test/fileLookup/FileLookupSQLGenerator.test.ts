// src/test/generators/FileLookupSQLGenerator.test.ts
import { FileLookupSQLGenerator } from '../../generators/FileLookupSQLGenerator';
import { UnifiedCanvasNode, NodeType } from '../../types/unified-pipeline.types';
import { compareSQL } from '../utils/sqlComparator';
import { mockInitialConfig } from './fileLookupMocks';

describe('FileLookupSQLGenerator', () => {
  let generator: FileLookupSQLGenerator;

  beforeEach(() => {
    generator = new FileLookupSQLGenerator({
      postgresVersion: '14.0',
      includeComments: false,
      formatSQL: false,
    });
  });

  const createLookupNode = (overrides: Partial<UnifiedCanvasNode> = {}): UnifiedCanvasNode => ({
    id: 'lookup1',
    name: 'FileLookup',
    type: NodeType.FILE_LOOKUP,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 120 },
    metadata: {
      configuration: {
        type: 'FILE_LOOKUP',
        config: { ...mockInitialConfig, ...overrides },
      },
      schemas: {
        input: [{ id: 'input_schema', name: 'Input', fields: [], isTemporary: false, isMaterialized: false }],
        output: { id: 'output_schema', name: 'Output', fields: [], isTemporary: false, isMaterialized: false },
      },
    },
    ...overrides,
  });

it('generates SELECT SQL for a valid file lookup node', () => {
  const node = createLookupNode();
  const context = {
    node,
    indentLevel: 0,
    parameters: new Map(),
    options: {
      includeComments: false,
      formatSQL: false,
      targetDialect: 'POSTGRESQL',
      postgresVersion: '14.0',
      useCTEs: false,
      optimizeForReadability: true,
      includeExecutionPlan: false,
      parameterizeValues: false,
      maxLineLength: 80,
    } as const, // 👈 ensures literal types
  };
  const result = generator.generateSQL(context);
  const expected = `SELECT * FROM lookup_lookup1`;
  const comparison = compareSQL(result.sql, expected);
  expect(comparison.success).toBe(true);
});

  it('returns error when node type is not FILE_LOOKUP', () => {
    const node = createLookupNode({ type: NodeType.JOIN });
    const context = { node, indentLevel: 0, parameters: new Map(), options: {} as any };
    const result = generator.generateSQL(context);
    expect(result.errors).toHaveLength(1);
    expect(result.errors[0].code).toBe('INVALID_NODE_TYPE');
  });

  it('returns error when file path is missing', () => {
    const node = createLookupNode({
      metadata: {
        configuration: { type: 'FILE_LOOKUP', config: { ...mockInitialConfig, file: { path: '', format: 'CSV', options: {} } } },
      },
    } as any);
    const context = { node, indentLevel: 0, parameters: new Map(), options: {} as any };
    const result = generator.generateSQL(context);
    expect(result.errors).toHaveLength(1);
    expect(result.errors[0].code).toBe('MISSING_FILE_PATH');
  });

  it('sanitizes table name from node id', () => {
    const node = createLookupNode({ id: 'lookup-123-test' });
    const context = { node, indentLevel: 0, parameters: new Map(), options: {} as any };
    const result = generator.generateSQL(context);
    // node.id replaced hyphens with underscores: lookup_123_test
    expect(result.sql).toContain('FROM lookup_lookup_123_test');
  });

  it('includes warning about temporary table requirement', () => {
    const node = createLookupNode();
    const context = { node, indentLevel: 0, parameters: new Map(), options: {} as any };
    const result = generator.generateSQL(context);
    expect(result.warnings).toContain('FileLookup requires temporary table creation');
  });
});