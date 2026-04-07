// __tests__/ExtractXMLSQLGenerator.test.ts
import { ExtractXMLSQLGenerator } from '../../generators/ExtractXMLSQLGenerator';
import { UnifiedCanvasNode, NodeType } from '../../types/unified-pipeline.types';
import { compareSQL } from '../utils/sqlComparator';

describe('ExtractXMLSQLGenerator', () => {
  let generator: ExtractXMLSQLGenerator;

  beforeEach(() => {
    generator = new ExtractXMLSQLGenerator({ postgresVersion: '14.0', includeComments: false, formatSQL: false });
  });

  const createMockNode = (config: any): UnifiedCanvasNode => ({
    id: 'xml-node-1',
    name: 'XML Extractor',
    type: NodeType.EXTRACT_XML_FIELD,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      configuration: {
        type: 'EXTRACT_XML_FIELD',
        config: config,
      },
      schemas: {
        output: {
          id: 'out',
          name: 'output',
          fields: [
            { id: 'f1', name: 'customer_name', type: 'STRING', nullable: true, isKey: false },
            { id: 'f2', name: 'customer_id', type: 'INTEGER', nullable: true, isKey: false },
          ],
          isTemporary: false,
          isMaterialized: false,
        },
      },
    },
  });

  it('generates correct SQL for single XPath extraction', () => {
    const config = {
      sourceColumn: 'xml_data',
      xpath: '/root/name',
      targetColumn: 'customer_name',
      dataType: 'STRING',
    };
    const node = createMockNode(config);
    const context: any = {
      node,
      options: { includeComments: false, formatSQL: false, targetDialect: 'POSTGRESQL', postgresVersion: '14.0', useCTEs: false, optimizeForReadability: true, includeExecutionPlan: false, parameterizeValues: false, maxLineLength: 80 },
      indentLevel: 0,
      parameters: new Map(),
    };
    const fragment = generator.generateSQL(context);
    const expected = `SELECT (xpath('/root/name', xml_data::xml))[1]::text AS customer_name FROM source_table`;
    const result = compareSQL(fragment.sql, expected);
    expect(result.success).toBe(true);
  });

  it('generates SQL with type casting when dataType is INTEGER', () => {
    const config = {
      sourceColumn: 'xml_data',
      xpath: '/root/id',
      targetColumn: 'customer_id',
      dataType: 'INTEGER',
    };
    const node = createMockNode(config);
    const context: any = {
      node,
      options: { includeComments: false, formatSQL: false, targetDialect: 'POSTGRESQL', postgresVersion: '14.0', useCTEs: false, optimizeForReadability: true, includeExecutionPlan: false, parameterizeValues: false, maxLineLength: 80 },
      indentLevel: 0,
      parameters: new Map(),
    };
    const fragment = generator.generateSQL(context);
    const expected = `SELECT ((xpath('/root/id', xml_data::xml))[1]::text)::integer AS customer_id FROM source_table`;
    const result = compareSQL(fragment.sql, expected);
    expect(result.success).toBe(true);
  });

  it('handles missing configuration with fallback and error', () => {
    const node = createMockNode(null as any); // invalid config
    const context: any = {
      node,
      options: { includeComments: false, formatSQL: false, targetDialect: 'POSTGRESQL', postgresVersion: '14.0', useCTEs: false, optimizeForReadability: true, includeExecutionPlan: false, parameterizeValues: false, maxLineLength: 80 },
      indentLevel: 0,
      parameters: new Map(),
    };
    const fragment = generator.generateSQL(context);
    expect(fragment.sql).toBe('SELECT * FROM source_table');
    expect(fragment.errors).toHaveLength(1);
    expect(fragment.errors[0].code).toBe('MISSING_XML_EXTRACT_CONFIG');
  });

  it('generates SQL for multiple output columns (via repeated calls – generator handles one per node)', () => {
    // In a real pipeline, multiple columns would be expressed as multiple xpath expressions
    // but our generator assumes a single target column. This test verifies the structure.
    const config = {
      sourceColumn: 'xml_data',
      xpath: '/root/item',
      targetColumn: 'item_value',
      dataType: 'VARCHAR',
    };
    const node = createMockNode(config);
    const context: any = {
      node,
      options: { includeComments: false, formatSQL: false, targetDialect: 'POSTGRESQL', postgresVersion: '14.0', useCTEs: false, optimizeForReadability: true, includeExecutionPlan: false, parameterizeValues: false, maxLineLength: 80 },
      indentLevel: 0,
      parameters: new Map(),
    };
    const fragment = generator.generateSQL(context);
    const expected = `SELECT (xpath('/root/item', xml_data::xml))[1]::text AS item_value FROM source_table`;
    const result = compareSQL(fragment.sql, expected);
    expect(result.success).toBe(true);
  });
});