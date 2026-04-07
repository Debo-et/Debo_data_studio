// src/test/generators/ExtractJSONSQLGenerator.test.ts

import { ExtractJSONSQLGenerator } from '../../generators/ExtractJSONSQLGenerator';
import { UnifiedCanvasNode, NodeType } from '../../types/unified-pipeline.types';
import { SQLGenerationContext } from '../../generators/BaseSQLGenerator';
import { compareSQL } from '../utils/sqlComparator';

describe('ExtractJSONSQLGenerator', () => {
  let generator: ExtractJSONSQLGenerator;

  beforeEach(() => {
    generator = new ExtractJSONSQLGenerator({
      postgresVersion: '14.0',
      includeComments: false,
      formatSQL: false,
    });
  });

  const createMockNode = (config: any): UnifiedCanvasNode => ({
    id: 'json-extract-1',
    name: 'Extract JSON',
    type: NodeType.EXTRACT_JSON_FIELDS,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      configuration: {
        type: 'EXTRACT_JSON_FIELDS',
        config,
      },
      schemas: {
        output: {
          id: 'output-schema',
          name: 'Output',
          fields: [
            { id: 'f1', name: 'user_id', type: 'INTEGER', nullable: true, isKey: false },
            { id: 'f2', name: 'user_name', type: 'STRING', nullable: true, isKey: false },
          ],
          isTemporary: false,
          isMaterialized: false,
        },
      },
    },
  });

  const mockContext = (node: UnifiedCanvasNode): SQLGenerationContext => ({
    node,
    connection: undefined,
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
    },
  });

  it('generates SQL for basic JSON extraction (jsonb)', () => {
    const config = {
      sourceColumn: 'json_data',
      jsonType: 'jsonb',
      mappings: [
        { targetColumn: 'user_id', jsonPath: 'id', dataType: 'INTEGER' },
        { targetColumn: 'user_name', jsonPath: 'name', dataType: 'VARCHAR' },
      ],
    };
    const node = createMockNode(config);
    const result = generator.generateSQL(mockContext(node));

    const expected = `
      SELECT (json_data->>'id')::INTEGER AS user_id,
             (json_data->>'name')::VARCHAR AS user_name
      FROM source_table
    `;
    const comparison = compareSQL(expected, result.sql);
    expect(comparison.success).toBe(true);
  });

  it('generates SQL for json type (not jsonb)', () => {
    const config = {
      sourceColumn: 'json_data',
      jsonType: 'json',
      mappings: [{ targetColumn: 'value', jsonPath: 'val', dataType: 'TEXT' }],
    };
    const node = createMockNode(config);
    const result = generator.generateSQL(mockContext(node));

    const expected = `SELECT (json_data->>'val')::TEXT AS value FROM source_table`;
    const comparison = compareSQL(expected, result.sql);
    expect(comparison.success).toBe(true);
  });

  it('handles no type casting when dataType omitted', () => {
    const config = {
      sourceColumn: 'json_data',
      jsonType: 'jsonb',
      mappings: [{ targetColumn: 'raw_value', jsonPath: 'field' }],
    };
    const node = createMockNode(config);
    const result = generator.generateSQL(mockContext(node));

    const expected = `SELECT json_data->>'field' AS raw_value FROM source_table`;
    const comparison = compareSQL(expected, result.sql);
    expect(comparison.success).toBe(true);
  });

  it('includes other output columns from schema', () => {
    const config = {
      sourceColumn: 'json_data',
      jsonType: 'jsonb',
      mappings: [{ targetColumn: 'extracted', jsonPath: 'foo' }],
    };
    const node = createMockNode(config);
    // Add an extra column to output schema not derived from JSON
    node.metadata!.schemas!.output!.fields.push({
      id: 'f3',
      name: 'static_col',
      type: 'STRING',
      nullable: true,
      isKey: false,
    });
    const result = generator.generateSQL(mockContext(node));

    const expected = `SELECT static_col, (json_data->>'foo') AS extracted FROM source_table`;
    const comparison = compareSQL(expected, result.sql);
    expect(comparison.success).toBe(true);
  });

  it('falls back to SELECT * when configuration is missing', () => {
    const node = createMockNode(null as any);
    const result = generator.generateSQL(mockContext(node));
    const expected = `SELECT * FROM source_table`;
    const comparison = compareSQL(expected, result.sql);
    expect(comparison.success).toBe(true);
    expect(result.warnings).toContain('Missing or invalid JSON extraction configuration; using SELECT * FROM source_table');
  });

  it('falls back when config has no mappings', () => {
    const config = { sourceColumn: 'json_data', jsonType: 'jsonb', mappings: [] };
    const node = createMockNode(config);
    const result = generator.generateSQL(mockContext(node));
    const expected = `SELECT * FROM source_table`;
    const comparison = compareSQL(expected, result.sql);
    expect(comparison.success).toBe(true);
    expect(result.warnings[0]).toMatch(/invalid/i);
  });

  it('handles missing sourceColumn in config', () => {
    const config = { jsonType: 'jsonb', mappings: [{ targetColumn: 'val', jsonPath: 'x' }] };
    const node = createMockNode(config);
    const result = generator.generateSQL(mockContext(node));
    const expected = `SELECT * FROM source_table`;
    const comparison = compareSQL(expected, result.sql);
    expect(comparison.success).toBe(true);
  });

  it('handles invalid jsonType (defaults to fallback)', () => {
    const config = {
      sourceColumn: 'json_data',
      jsonType: 'invalid',
      mappings: [{ targetColumn: 'val', jsonPath: 'x' }],
    };
    const node = createMockNode(config);
    const result = generator.generateSQL(mockContext(node));
    const expected = `SELECT * FROM source_table`;
    const comparison = compareSQL(expected, result.sql);
    expect(comparison.success).toBe(true);
  });
});