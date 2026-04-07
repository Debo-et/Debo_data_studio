// src/test/generators/SampleRowSQLGenerator.test.ts
import { SampleRowSQLGenerator } from '../../generators/SampleRowSQLGenerator';
import { SQLGenerationContext } from '../../generators/BaseSQLGenerator';
import { NodeType, UnifiedCanvasNode } from '../../types/unified-pipeline.types';
import { compareSQL } from '../utils/sqlComparator';

describe('SampleRowSQLGenerator', () => {
  let generator: SampleRowSQLGenerator;

  beforeEach(() => {
    generator = new SampleRowSQLGenerator({
      postgresVersion: '14.0',
      includeComments: false,
      formatSQL: false,
    });
  });

  const createNodeWithConfig = (config: any): UnifiedCanvasNode => ({
    id: 'sample-node-1',
    name: 'Sample Row',
    type: NodeType.SAMPLE_ROW,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      configuration: {
        type: 'SAMPLE_ROW',
        config,
      },
    },
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
      useCTEs: false,
      optimizeForReadability: false,
      includeExecutionPlan: false,
      parameterizeValues: false,
      maxLineLength: 80,
    },
  });

  it('generates LIMIT query for firstRows method', () => {
    const config = {
      samplingMethod: 'firstRows',
      sampleValue: 15,
      ignoreEmptyRows: false,
      includeHeader: true,
    };
    const node = createNodeWithConfig(config);
    const context = createContext(node);
    const result = generator.generateSelectStatement(context);

    const expected = 'SELECT * FROM source_table LIMIT 15';
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
    expect(result.warnings).toHaveLength(0);
    expect(result.errors).toHaveLength(0);
  });

  it('generates TABLESAMPLE for percentage method', () => {
    const config = {
      samplingMethod: 'percentage',
      sampleValue: 12.5,
      ignoreEmptyRows: true,
      includeHeader: false,
    };
    const node = createNodeWithConfig(config);
    const context = createContext(node);
    const result = generator.generateSelectStatement(context);

    const expected = 'SELECT * FROM source_table TABLESAMPLE SYSTEM(12.5)';
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('generates ORDER BY random() LIMIT for random method without seed', () => {
    const config = {
      samplingMethod: 'random',
      sampleValue: 100,
    };
    const node = createNodeWithConfig(config);
    const context = createContext(node);
    const result = generator.generateSelectStatement(context);

    const expected = 'SELECT * FROM source_table ORDER BY random() LIMIT 100';
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('falls back to all rows when config is missing', () => {
    const node = createNodeWithConfig(null); // invalid config
    const context = createContext(node);
    const result = generator.generateSelectStatement(context);

    const expected = 'SELECT * FROM source_table';
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
    expect(result.warnings).toContain('No valid sample configuration found; returning all rows.');
  });

  it('returns warning for unsupported node type', () => {
    const node: UnifiedCanvasNode = {
      id: 'other-node',
      name: 'Other',
      type: NodeType.JOIN, // not SAMPLE_ROW
      position: { x: 0, y: 0 },
      size: { width: 200, height: 100 },
    };
    const context = createContext(node);
    const result = generator.generateSelectStatement(context);

    const expected = 'SELECT * FROM source_table';
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
    expect(result.warnings).toContain('No valid sample configuration found; returning all rows.');
  });

  it('handles missing sampleValue gracefully', () => {
    const config = {
      samplingMethod: 'firstRows',
      // sampleValue missing
    };
    const node = createNodeWithConfig(config);
    const context = createContext(node);
    const result = generator.generateSelectStatement(context);

    const expected = 'SELECT * FROM source_table';
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
    expect(result.warnings).toContain('No valid sample configuration found; returning all rows.');
  });

  it('handles negative sampleValue as fallback', () => {
    const config = {
      samplingMethod: 'firstRows',
      sampleValue: -5,
    };
    const node = createNodeWithConfig(config);
    const context = createContext(node);
    const result = generator.generateSelectStatement(context);

    const expected = 'SELECT * FROM source_table';
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
    expect(result.warnings).toContain('No valid sample configuration found; returning all rows.');
  });
});