// src/generators/AddCRCSQLGenerator.test.ts
import { AddCRCSQLGenerator } from '../../generators/AddCRCSQLGenerator';
import { UnifiedCanvasNode, NodeType, ComponentConfiguration } from '../../types/unified-pipeline.types';
import { compareSQL } from '../utils/sqlComparator';

// Local type with required configuration (matching SQLGenerationContext expectation)
type TestNode = UnifiedCanvasNode & {
  metadata: UnifiedCanvasNode['metadata'] & { configuration: ComponentConfiguration };
};

describe('AddCRCSQLGenerator', () => {
  let generator: AddCRCSQLGenerator;

  beforeEach(() => {
    generator = new AddCRCSQLGenerator({ postgresVersion: '14.0', includeComments: false, formatSQL: false });
  });

  const createMockNode = (config: any): TestNode => ({
    id: 'crc-node',
    name: 'Add CRC',
    type: NodeType.ADD_CRC_ROW,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      configuration: { type: 'ADD_CRC_ROW', config },
      schemas: {
        output: {
          id: 'output',
          name: 'Output',
          fields: [
            { id: 'f1', name: 'id', type: 'INTEGER', nullable: true, isKey: false },
            { id: 'f2', name: 'name', type: 'STRING', nullable: true, isKey: false },
            { id: 'f3', name: 'value', type: 'DECIMAL', nullable: true, isKey: false },
            { id: 'f4', name: 'crc', type: 'STRING', nullable: false, isKey: false },
          ],
          isTemporary: false,
          isMaterialized: false,
        },
      },
    },
  } as TestNode);

  const baseConfig = {
    includedColumns: ['id', 'name'],
    algorithm: 'crc32' as const,
    outputColumn: 'crc',
    nullHandling: 'TREAT_AS_EMPTY',
    characterEncoding: 'UTF-8',
    computeOnWholeRow: false,
    columnSeparator: ',',
  };

  it('generates SQL for CRC32 algorithm with selected columns', () => {
    const node = createMockNode(baseConfig);
    const context = { node, indentLevel: 0, parameters: new Map(), options: {} as any };
    const result = generator.generateSQL(context);
    const expected = `SELECT id, name, value, hashtext(COALESCE(id::text, '') || COALESCE(name::text, '')) AS crc FROM source_table;`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('generates SQL for MD5 algorithm', () => {
    const config = { ...baseConfig, algorithm: 'md5' };
    const node = createMockNode(config);
    const context = { node, indentLevel: 0, parameters: new Map(), options: {} as any };
    const result = generator.generateSQL(context);
    const expected = `SELECT id, name, value, md5(COALESCE(id::text, '') || COALESCE(name::text, '')) AS crc FROM source_table;`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('generates SQL for hashtext algorithm', () => {
    const config = { ...baseConfig, algorithm: 'hashtext' };
    const node = createMockNode(config);
    const context = { node, indentLevel: 0, parameters: new Map(), options: {} as any };
    const result = generator.generateSQL(context);
    const expected = `SELECT id, name, value, hashtext(COALESCE(id::text, '') || COALESCE(name::text, '')) AS crc FROM source_table;`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('uses all columns when includedColumns is empty', () => {
    const config = { ...baseConfig, includedColumns: [] };
    const node = createMockNode(config);
    const context = { node, indentLevel: 0, parameters: new Map(), options: {} as any };
    const result = generator.generateSQL(context);
    const expected = `SELECT id, name, value, hashtext(COALESCE(id::text, '') || COALESCE(name::text, '') || COALESCE(value::text, '')) AS crc FROM source_table;`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('handles whole-row computation with custom separator', () => {
    const config = {
      ...baseConfig,
      computeOnWholeRow: true,
      columnSeparator: '|',
      includedColumns: [],
    };
    const node = createMockNode(config);
    const context = { node, indentLevel: 0, parameters: new Map(), options: {} as any };
    const result = generator.generateSQL(context);
    const expected = `SELECT id, name, value, hashtext(COALESCE(id::text, '') || '|' || COALESCE(name::text, '') || '|' || COALESCE(value::text, '')) AS crc FROM source_table;`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('handles NULL values with COALESCE (TREAT_AS_EMPTY)', () => {
    const config = { ...baseConfig, nullHandling: 'TREAT_AS_EMPTY' };
    const node = createMockNode(config);
    const context = { node, indentLevel: 0, parameters: new Map(), options: {} as any };
    const result = generator.generateSQL(context);
    expect(result.sql).toContain("COALESCE(id::text, '')");
    expect(result.sql).toContain("COALESCE(name::text, '')");
  });

  it('falls back when configuration is missing', () => {
    const node = createMockNode(undefined as any) as any;
    delete node.metadata?.configuration;
    const context = { node, indentLevel: 0, parameters: new Map(), options: {} as any };
    const result = generator.generateSQL(context);
    const expected = `SELECT * FROM Add CRC;`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
    expect(result.warnings).toContain('No CRC config, using fallback');
  });

  it('sanitizes column names with special characters', () => {
    const node = {
      ...createMockNode(baseConfig),
      metadata: {
        ...createMockNode(baseConfig).metadata,
        schemas: {
          output: {
            id: 'out',
            name: 'Out',
            fields: [
              { id: 'f1', name: 'first-name', type: 'STRING', nullable: true, isKey: false },
              { id: 'f2', name: 'last name', type: 'STRING', nullable: true, isKey: false },
              { id: 'f3', name: 'crc', type: 'STRING', nullable: false, isKey: false },
            ],
          },
        },
      },
    } as TestNode;
    const context = { node, indentLevel: 0, parameters: new Map(), options: {} as any };
    const result = generator.generateSQL(context);
    expect(result.sql).toContain('"first-name"');
    expect(result.sql).toContain('"last name"');
  });
});