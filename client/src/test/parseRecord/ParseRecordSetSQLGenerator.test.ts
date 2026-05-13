import { ParseRecordSetSQLGenerator } from '../../generators/ParseRecordSetSQLGenerator';
import { SQLGenerationContext } from '../../generators/BaseSQLGenerator';
import { UnifiedCanvasNode, NodeType } from '../../types/unified-pipeline.types';
import { compareSQL } from '../utils/sqlComparator';

describe('ParseRecordSetSQLGenerator', () => {
  let generator: ParseRecordSetSQLGenerator;

  beforeEach(() => {
    generator = new ParseRecordSetSQLGenerator({ postgresVersion: '14.0', includeComments: false, formatSQL: false });
  });

  const createContext = (node: UnifiedCanvasNode): SQLGenerationContext => ({
    node,
    indentLevel: 0,
    parameters: new Map(),
    options: {
      includeComments: false,
      formatSQL: false,
      targetDialect: 'POSTGRESQL',
      postgresVersion: '14.0',
      useCTEs: true,
      optimizeForReadability: true,
      includeExecutionPlan: false,
      parameterizeValues: false,
      maxLineLength: 80,
    },
  });

  it('generates SQL for JSON record set parsing', () => {
    const node: UnifiedCanvasNode = {
      id: 'parse1',
      name: 'Parse JSON',
      type: NodeType.PARSE_RECORD_SET,
      position: { x: 0, y: 0 },
      size: { width: 200, height: 100 },
      metadata: {
        configuration: {
          type: 'PARSE_RECORD_SET',
          config: {
            sourceColumn: 'json_data',
            recordType: 'json',
            targetColumns: [
              { name: 'id', path: '$.id', type: 'integer' },
              { name: 'name', path: '$.name', type: 'text' },
            ],
          },
        },
        schemas: {
          output: {
            id: 'out',
            name: 'Output',
            fields: [
              { id: 'f1', name: 'id', type: 'INTEGER', nullable: true, isKey: false },
              { id: 'f2', name: 'name', type: 'STRING', nullable: true, isKey: false },
            ],
            isTemporary: false,
            isMaterialized: false,
          },
        },
      },
    } as any;

    const context = createContext(node);
    const fragment = generator.generateSQL(context);
    const expected = `SELECT id, name FROM source_table, LATERAL jsonb_to_recordset(source_table.json_data::jsonb) AS parsed(id integer, name text)`;
    const comparison = compareSQL(fragment.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('generates SQL for XML record set parsing with xmltable', () => {
    const node: UnifiedCanvasNode = {
      id: 'parse_xml',
      name: 'Parse XML',
      type: NodeType.PARSE_RECORD_SET,
      position: { x: 0, y: 0 },
      size: { width: 200, height: 100 },
      metadata: {
        configuration: {
          type: 'PARSE_RECORD_SET',
          config: {
            sourceColumn: 'xml_data',
            recordType: 'xml',
            xpath: '/root/record',
            targetColumns: [
              { name: 'value', path: '@value', type: 'text' },
              { name: 'date', path: 'date/text()', type: 'date' },
            ],
          },
        },
        schemas: {
          output: {
            id: 'out',
            name: 'Output',
            fields: [
              { id: 'f1', name: 'value', type: 'STRING', nullable: true, isKey: false },
              { id: 'f2', name: 'date', type: 'DATE', nullable: true, isKey: false },
            ],
            isTemporary: false,
            isMaterialized: false,
          },
        },
      },
    } as any;

    const context = createContext(node);
    const fragment = generator.generateSQL(context);
    const expected = `SELECT value, date FROM source_table, LATERAL xmltable('/root/record' PASSING source_table.xml_data::xml COLUMNS value text PATH '@value', date date PATH 'date/text()') AS parsed`;
    const comparison = compareSQL(fragment.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('generates SQL for delimited record set parsing (regexp_split_to_table)', () => {
    const node: UnifiedCanvasNode = {
      id: 'parse_delimited',
      name: 'Parse Delimited',
      type: NodeType.PARSE_RECORD_SET,
      position: { x: 0, y: 0 },
      size: { width: 200, height: 100 },
      metadata: {
        configuration: {
          type: 'PARSE_RECORD_SET',
          config: {
            sourceColumn: 'csv_line',
            recordType: 'delimited',
            delimiter: ',',
            targetColumns: [{ name: 'token', path: '', type: 'text' }],
          },
        },
        schemas: {
          output: {
            id: 'out',
            name: 'Output',
            fields: [{ id: 'f1', name: 'token', type: 'STRING', nullable: true, isKey: false }],
            isTemporary: false,
            isMaterialized: false,
          },
        },
      },
    } as any;

    const context = createContext(node);
    const fragment = generator.generateSQL(context);
    const expected = `SELECT token FROM source_table, LATERAL regexp_split_to_table(source_table.csv_line, ',') AS parsed(token)`;
    const comparison = compareSQL(fragment.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('returns error fragment when configuration is missing', () => {
    const node: UnifiedCanvasNode = {
      id: 'bad',
      name: 'Bad',
      type: NodeType.PARSE_RECORD_SET,
      position: { x: 0, y: 0 },
      size: { width: 200, height: 100 },
      metadata: {}, // no configuration
    } as any;
    const context = createContext(node);
    const fragment = generator.generateSQL(context);
    expect(fragment.errors).toHaveLength(1);
    expect(fragment.errors[0].code).toBe('MISSING_CONFIG');
    expect(fragment.sql).toContain('Fallback');
  });

  it('returns error fragment when XML config missing xpath', () => {
    const node: UnifiedCanvasNode = {
      id: 'xml_no_xpath',
      name: 'XML No XPath',
      type: NodeType.PARSE_RECORD_SET,
      position: { x: 0, y: 0 },
      size: { width: 200, height: 100 },
      metadata: {
        configuration: {
          type: 'PARSE_RECORD_SET',
          config: {
            sourceColumn: 'xml_col',
            recordType: 'xml',
            targetColumns: [],
          },
        },
      },
    } as any;
    const context = createContext(node);
    const fragment = generator.generateSQL(context);
    expect(fragment.errors).toHaveLength(1);
    expect(fragment.errors[0].message).toMatch(/xpath is required/);
  });

  it('returns error fragment when delimited config missing delimiter', () => {
    const node: UnifiedCanvasNode = {
      id: 'delim_no_delim',
      name: 'No Delimiter',
      type: NodeType.PARSE_RECORD_SET,
      position: { x: 0, y: 0 },
      size: { width: 200, height: 100 },
      metadata: {
        configuration: {
          type: 'PARSE_RECORD_SET',
          config: {
            sourceColumn: 'data',
            recordType: 'delimited',
            targetColumns: [{ name: 'part', path: '' }],
          },
        },
      },
    } as any;
    const context = createContext(node);
    const fragment = generator.generateSQL(context);
    expect(fragment.errors).toHaveLength(1);
    expect(fragment.errors[0].message).toMatch(/delimiter is required/);
  });
});