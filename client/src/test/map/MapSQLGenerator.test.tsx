import { MapSQLGenerator, CanvasMappingContext } from '../../generators/MapSQLGenerator';
import { PostgreSQLDataType } from '../../types/unified-pipeline.types';

/**
 * Normalize SQL for robust comparison:
 * - Remove double quotes and backticks
 * - Collapse all whitespace to single spaces
 * - Trim trailing semicolon
 * - Convert to lowercase
 */
function normalizeSql(sql: string): string {
  return sql
    .replace(/["`]/g, '')          // remove all double quotes and backticks
    .replace(/\s+/g, ' ')          // collapse any whitespace sequence
    .replace(/;\s*$/, '')          // remove trailing semicolon
    .trim()
    .toLowerCase();
}

function compareSql(actual: string, expected: string): { success: boolean; message?: string } {
  const normActual = normalizeSql(actual);
  const normExpected = normalizeSql(expected);
  const success = normActual === normExpected;
  return {
    success,
    message: success ? undefined : `\nExpected: ${normExpected}\nActual:   ${normActual}`,
  };
}

describe('MapSQLGenerator', () => {
  let generator: MapSQLGenerator;

  beforeEach(() => {
    generator = new MapSQLGenerator({
      postgresVersion: '14.0',
      includeComments: false,
      formatSQL: false,
    });
  });

  it('generates simple direct mapping SQL', () => {
    const context: CanvasMappingContext = {
      sourceTables: [
        {
          id: 'source1',
          name: 'source_table',
          type: 'input',
          columns: [{ id: 'col1', name: 'first_name', type: 'string' }],
        },
      ],
      targetTables: [
        {
          id: 'target1',
          name: 'target_table',
          type: 'output',
          columns: [{ id: 'out1', name: 'full_name', type: 'string' }],
        },
      ],
      wires: [
        {
          id: 'wire1',
          sourceTableId: 'source1',
          sourceColumnId: 'col1',
          targetTableId: 'target1',
          targetColumnId: 'out1',
        },
      ],
      variables: [],
      nodeId: 'map1',
      nodeName: 'Map',
    };
    const result = generator.generateSQLFromCanvasMapping(context);
    const expected = `SELECT first_name AS full_name FROM source_table`;
    const comparison = compareSql(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('generates SQL with type conversion', () => {
    const context: CanvasMappingContext = {
      sourceTables: [
        {
          id: 'source1',
          name: 'source_table',
          type: 'input',
          columns: [
            { id: 'col1', name: 'first_name', type: 'string' },
            { id: 'col3', name: 'age', type: 'integer' },
          ],
        },
      ],
      targetTables: [
        {
          id: 'target1',
          name: 'target_table',
          type: 'output',
          columns: [
            { id: 'out1', name: 'full_name', type: 'string' },
            { id: 'out2', name: 'age_group_str', type: 'string' },
          ],
        },
      ],
      wires: [
        {
          id: 'wire1',
          sourceTableId: 'source1',
          sourceColumnId: 'col1',
          targetTableId: 'target1',
          targetColumnId: 'out1',
        },
        {
          id: 'wire2',
          sourceTableId: 'source1',
          sourceColumnId: 'col3',
          targetTableId: 'target1',
          targetColumnId: 'out2',
        },
      ],
      variables: [],
      nodeId: 'map1',
      nodeName: 'Map',
    };

    // Provide output schema metadata to trigger type casting
    const mapNode = {
      id: 'map1',
      name: 'Map',
      type: 'MAP',
      position: { x: 0, y: 0 },
      size: { width: 100, height: 100 },
      metadata: {
        configuration: { type: 'MAP', config: {} } as any,
        schemas: {
          output: {
            id: 'output_schema',
            name: 'Output',
            fields: [
              { id: 'f1', name: 'full_name', type: 'STRING', nullable: true, isKey: false },
              { id: 'f2', name: 'age_group_str', type: 'STRING', nullable: true, isKey: false },
            ],
            isTemporary: false,
            isMaterialized: false,
          },
        },
      },
    } as any;

    const result = generator.generateSQLFromCanvasMapping({
      ...context,
      node: mapNode,
    } as CanvasMappingContext);
    const expected = `SELECT first_name AS full_name, (age)::TEXT AS age_group_str FROM source_table`;
    const comparison = compareSql(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('generates SQL with transformation expression', () => {
    const context: CanvasMappingContext = {
      sourceTables: [
        {
          id: 'source1',
          name: 'source_table',
          type: 'input',
          columns: [
            { id: 'col1', name: 'first_name', type: 'string' },
            { id: 'col2', name: 'last_name', type: 'string' },
          ],
        },
      ],
      targetTables: [
        {
          id: 'target1',
          name: 'target_table',
          type: 'output',
          columns: [{ id: 'out1', name: 'full_name', type: 'string' }],
        },
      ],
      wires: [
        {
          id: 'wire1',
          sourceTableId: 'source1',
          sourceColumnId: 'col1',
          targetTableId: 'target1',
          targetColumnId: 'out1',
          transformation: `CONCAT({first_name}, ' ', {last_name})`,
        },
      ],
      variables: [],
      nodeId: 'map1',
      nodeName: 'Map',
    };
    const result = generator.generateSQLFromCanvasMapping(context);
    const expected = `SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM source_table`;
    const comparison = compareSql(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('generates SQL with default value (COALESCE)', () => {
    const sourceColumns = [{ name: 'first_name', dataType: PostgreSQLDataType.VARCHAR }];
    const mappings = [
      {
        sourceColumn: 'first_name',
        targetColumn: 'full_name',
        transformation: undefined,
        defaultValue: 'Unknown',
        isRequired: true,
      },
    ];
    const result = generator.generateMappingSQL(sourceColumns, mappings, []);
    const expected = `SELECT COALESCE(first_name, 'Unknown') AS full_name FROM source_table`;
    const comparison = compareSql(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('generates SQL with conditional logic (CASE)', () => {
    const sourceColumns = [
      { name: 'first_name', dataType: PostgreSQLDataType.VARCHAR },
      { name: 'age', dataType: PostgreSQLDataType.INTEGER },
    ];
    const mappings = [
      { sourceColumn: 'first_name', targetColumn: 'full_name', isRequired: true },
      {
        sourceColumn: 'age',
        targetColumn: 'age_group',
        isRequired: true,
        transformation: `CASE WHEN age < 18 THEN 'minor' ELSE age::TEXT END`,
      },
    ];
    // FIX: Pass empty rules array to avoid double CASE wrapping
    const result = generator.generateMappingSQL(sourceColumns, mappings, []);
    const expected = `SELECT first_name AS full_name, CASE WHEN age < 18 THEN 'minor' ELSE age::TEXT END AS age_group FROM source_table`;
    const comparison = compareSql(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('handles multiple wires to same target (concat)', () => {
    const context: CanvasMappingContext = {
      sourceTables: [
        {
          id: 'source1',
          name: 'source_table',
          type: 'input',
          columns: [
            { id: 'col1', name: 'first_name', type: 'string' },
            { id: 'col2', name: 'last_name', type: 'string' },
          ],
        },
      ],
      targetTables: [
        {
          id: 'target1',
          name: 'target_table',
          type: 'output',
          columns: [{ id: 'out1', name: 'full_name', type: 'string' }],
        },
      ],
      wires: [
        {
          id: 'wire1',
          sourceTableId: 'source1',
          sourceColumnId: 'col1',
          targetTableId: 'target1',
          targetColumnId: 'out1',
        },
        {
          id: 'wire2',
          sourceTableId: 'source1',
          sourceColumnId: 'col2',
          targetTableId: 'target1',
          targetColumnId: 'out1',
        },
      ],
      variables: [],
      nodeId: 'map1',
      nodeName: 'Map',
    };
    const result = generator.generateSQLFromCanvasMapping(context);
    const expected = `SELECT CONCAT(first_name, last_name) AS full_name FROM source_table`;
    const comparison = compareSql(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('handles empty mappings (fallback)', () => {
    const context: CanvasMappingContext = {
      sourceTables: [
        {
          id: 'source1',
          name: 'source_table',
          type: 'input',
          columns: [{ id: 'col1', name: 'first_name', type: 'string' }],
        },
      ],
      targetTables: [
        {
          id: 'target1',
          name: 'target_table',
          type: 'output',
          columns: [{ id: 'out1', name: 'full_name', type: 'string' }],
        },
      ],
      wires: [],
      variables: [],
      nodeId: 'map1',
      nodeName: 'Map',
    };
    const result = generator.generateSQLFromCanvasMapping(context);
    const expected = `SELECT * FROM source_table`;
    const comparison = compareSql(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('validates mapping configuration', () => {
    const context: CanvasMappingContext = {
      sourceTables: [
        {
          id: 'source1',
          name: 'source_table',
          type: 'input',
          columns: [{ id: 'col1', name: 'first_name', type: 'string' }],
        },
      ],
      targetTables: [
        {
          id: 'target1',
          name: 'target_table',
          type: 'output',
          columns: [
            { id: 'out1', name: 'full_name', type: 'string' },
            { id: 'out2', name: 'unmapped_column', type: 'string' },
          ],
        },
      ],
      wires: [
        {
          id: 'wire1',
          sourceTableId: 'source1',
          sourceColumnId: 'col1',
          targetTableId: 'target1',
          targetColumnId: 'out1',
        },
      ],
      variables: [],
      nodeId: 'map1',
      nodeName: 'Map',
    };
    const validation = generator.validateCanvasMapping(context);
    expect(validation.isValid).toBe(true);
    expect(validation.warnings).toContainEqual(expect.stringContaining('unmapped_column'));
  });
});