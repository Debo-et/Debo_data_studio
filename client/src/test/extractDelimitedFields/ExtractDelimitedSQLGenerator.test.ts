// src/generators/__tests__/ExtractDelimitedSQLGenerator.test.ts
import { ExtractDelimitedSQLGenerator } from '../../generators/ExtractDelimitedSQLGenerator';
import { SQLGenerationContext } from '../../generators/BaseSQLGenerator';
import { UnifiedCanvasNode, NodeType } from '../../types/unified-pipeline.types';
import { compareSQL } from '../../test/utils/sqlComparator';

describe('ExtractDelimitedSQLGenerator', () => {
  let generator: ExtractDelimitedSQLGenerator;

  beforeEach(() => {
    generator = new ExtractDelimitedSQLGenerator({ postgresVersion: '14.0', includeComments: false, formatSQL: false });
  });

  const createNodeWithConfig = (config: any, nodeName = 'extractNode'): UnifiedCanvasNode => ({
    id: 'node-1',
    name: nodeName,
    type: NodeType.EXTRACT_DELIMITED_FIELDS,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      configuration: { type: 'EXTRACT_DELIMITED', config },
      schemas: {
        output: {
          id: 'output-schema',
          name: 'Output',
          fields: config.outputColumns.map((col: any) => ({
            id: col.id,
            name: col.name,
            type: col.type,
            nullable: true,
            isKey: false,
          })),
          isTemporary: false,
          isMaterialized: false,
        },
      },
    },
  });

  it('generates basic SQL with split_part', () => {
    const config = {
      sourceColumn: 'raw_address',
      delimiter: ',',
      outputColumns: [
        { id: 'c1', name: 'street', type: 'STRING', position: 1 },
        { id: 'c2', name: 'city', type: 'STRING', position: 2 },
        { id: 'c3', name: 'zip', type: 'STRING', position: 3 },
      ],
      trimWhitespace: true,
      nullIfEmpty: false,
      errorHandling: 'fail',
    };
    const node = createNodeWithConfig(config);
    const context: SQLGenerationContext = {
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
      },
    };
    const result = generator.generateSQL(context);
    const expected = `SELECT split_part(raw_address, ',', 1) AS street, split_part(raw_address, ',', 2) AS city, split_part(raw_address, ',', 3) AS zip FROM source_table;`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('handles custom delimiter and quote character', () => {
    const config = {
      sourceColumn: 'csv_data',
      delimiter: '|',
      quoteChar: '"',
      outputColumns: [
        { id: 'c1', name: 'col1', type: 'STRING', position: 1 },
        { id: 'c2', name: 'col2', type: 'INTEGER', position: 2 },
      ],
      trimWhitespace: false,
      nullIfEmpty: true,
    };
    const node = createNodeWithConfig(config);
    const context: SQLGenerationContext = { ...createBaseContext(node) };
    const result = generator.generateSQL(context);
    const expected = `SELECT split_part(csv_data, '|', 1) AS col1, split_part(csv_data, '|', 2) AS col2 FROM source_table;`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('includes other columns from output schema (non-source)', () => {
    const config = {
      sourceColumn: 'raw',
      delimiter: ',',
      outputColumns: [
        { id: 'c1', name: 'extracted', type: 'STRING', position: 1 },
      ],
    };
    // Add an extra field in output schema that is not an extracted column (e.g., constant or existing column)
    const node = createNodeWithConfig(config);
    if (node.metadata?.schemas?.output) {
      node.metadata.schemas.output.fields.push({
        id: 'extra',
        name: 'existing_id',
        type: 'INTEGER',
        nullable: false,
        isKey: true,
      });
    }
    const context: SQLGenerationContext = { ...createBaseContext(node) };
    const result = generator.generateSQL(context);
    const expected = `SELECT existing_id, split_part(raw, ',', 1) AS extracted FROM source_table;`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('falls back to SELECT * when no output columns defined', () => {
    const config = {
      sourceColumn: 'data',
      delimiter: ',',
      outputColumns: [],
    };
    const node = createNodeWithConfig(config);
    const context: SQLGenerationContext = { ...createBaseContext(node) };
    const result = generator.generateSQL(context);
    const expected = `SELECT * FROM source_table;`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
    expect(result.warnings).toContainEqual(expect.stringContaining('No output columns defined'));
  });

  it('handles missing source column gracefully', () => {
    const config = {
      sourceColumn: '',
      delimiter: ',',
      outputColumns: [{ id: 'c1', name: 'out', type: 'STRING', position: 1 }],
    };
    const node = createNodeWithConfig(config);
    const context: SQLGenerationContext = { ...createBaseContext(node) };
    const result = generator.generateSQL(context);
    const expected = `SELECT * FROM source_table;`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
    expect(result.errors.length).toBeGreaterThan(0);
    expect(result.errors[0].code).toBe('INVALID_CONFIG');
  });

  it('generates SQL with type casting (if needed) - not directly in extract, but shows full SELECT', () => {
    const config = {
      sourceColumn: 'amount_str',
      delimiter: ',',
      outputColumns: [
        { id: 'c1', name: 'amount_int', type: 'INTEGER', position: 1 },
      ],
    };
    const node = createNodeWithConfig(config);
    // Override output schema field type to INTEGER
    if (node.metadata?.schemas?.output) {
      node.metadata.schemas.output.fields[0].type = 'INTEGER';
    }
    const context: SQLGenerationContext = { ...createBaseContext(node) };
    const result = generator.generateSQL(context);
    // Expect split_part result to be cast to INTEGER
    const expected = `SELECT (split_part(amount_str, ',', 1))::integer AS amount_int FROM source_table;`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });
});

// Helper to create a minimal context
function createBaseContext(node: UnifiedCanvasNode): SQLGenerationContext {
  return {
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
    },
  };
}