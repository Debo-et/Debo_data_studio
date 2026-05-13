// src/test/generators/DataMaskingSQLGenerator.test.ts
import { DataMaskingSQLGenerator } from '../../generators/DataMaskingSQLGenerator';
import { SQLGenerationContext } from '../../generators/BaseSQLGenerator';
import { UnifiedCanvasNode, NodeType, DataMaskingComponentConfiguration } from '../../types/unified-pipeline.types';
import { compareSQL } from '../utils/sqlComparator';

describe('DataMaskingSQLGenerator', () => {
  let generator: DataMaskingSQLGenerator;

  beforeEach(() => {
    generator = new DataMaskingSQLGenerator({ postgresVersion: '14.0', includeComments: false, formatSQL: false });
  });

  const createMockNode = (rules: any[] = []): UnifiedCanvasNode => {
    const config: DataMaskingComponentConfiguration = {
      version: '1.0',
      rules: rules.map((r, idx) => ({ ...r, id: `rule-${idx}`, position: idx })),
      outputSchema: {
        id: 'out-schema',
        name: 'Output',
        fields: rules.map(r => ({ id: r.column, name: r.column, type: 'STRING', nullable: true, isKey: false })),
        isTemporary: false,
        isMaterialized: false,
      },
      sqlGeneration: { selectExpressions: [], estimatedRowMultiplier: 1 },
      compilerMetadata: { lastModified: '', createdBy: '', ruleCount: rules.length, validationStatus: 'VALID', warnings: [], dependencies: [], compiledSql: undefined },
    };
    return {
      id: 'mask-node',
      name: 'Mask Data',
      type: NodeType.DATA_MASKING,
      position: { x: 0, y: 0 },
      size: { width: 100, height: 100 },
      metadata: {
        configuration: { type: 'DATA_MASKING', config },
        schemas: { output: config.outputSchema },
      },
    } as UnifiedCanvasNode;
  };

  const createContext = (node: UnifiedCanvasNode): SQLGenerationContext => ({
    node,
    indentLevel: 0,
    parameters: new Map(),
    options: { includeComments: false, formatSQL: false, targetDialect: 'POSTGRESQL', postgresVersion: '14.0', useCTEs: false, optimizeForReadability: true, includeExecutionPlan: false, parameterizeValues: false, maxLineLength: 80 },
  });

  it('generates simple REPLACE masking', () => {
    const rules = [{ column: 'email', maskingType: 'REPLACE', parameters: { replaceValue: 'REDACTED' } }];
    const node = createMockNode(rules);
    const context = createContext(node);
    const result = generator.generateSelectStatement(context);
    const expected = `SELECT 'REDACTED' AS email FROM source_table`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('generates HASH masking with MD5', () => {
    const rules = [{ column: 'ssn', maskingType: 'HASH', parameters: { hashAlgorithm: 'MD5' } }];
    const node = createMockNode(rules);
    const context = createContext(node);
    const result = generator.generateSelectStatement(context);
    const expected = `SELECT md5(ssn::text) AS ssn FROM source_table`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('generates HASH masking with SHA256', () => {
    const rules = [{ column: 'ssn', maskingType: 'HASH', parameters: { hashAlgorithm: 'SHA256' } }];
    const node = createMockNode(rules);
    const context = createContext(node);
    const result = generator.generateSelectStatement(context);
    // PostgreSQL's encode(sha256(...), 'hex')
    const expected = `SELECT encode(sha256(ssn::bytea), 'hex') AS ssn FROM source_table`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('generates NULLIFY masking', () => {
    const rules = [{ column: 'salary', maskingType: 'NULLIFY' }];
    const node = createMockNode(rules);
    const context = createContext(node);
    const result = generator.generateSelectStatement(context);
    const expected = `SELECT NULL AS salary FROM source_table`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('generates RANDOM masking (string)', () => {
    const rules = [{ column: 'name', maskingType: 'RANDOM', parameters: { randomType: 'STRING', randomLength: 8 } }];
    const node = createMockNode(rules);
    const context = createContext(node);
    const result = generator.generateSelectStatement(context);
    // Expected uses md5(random()::text) to simulate random string
    const expected = `SELECT left(md5(random()::text), 8) AS name FROM source_table`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('generates RANDOM masking (number)', () => {
    const rules = [{ column: 'salary', maskingType: 'RANDOM', parameters: { randomType: 'NUMBER', randomLength: 5 } }];
    const node = createMockNode(rules);
    const context = createContext(node);
    const result = generator.generateSelectStatement(context);
    const expected = `SELECT floor(random() * 100000)::int AS salary FROM source_table`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('generates EMAIL masking', () => {
    const rules = [{ column: 'email', maskingType: 'EMAIL' }];
    const node = createMockNode(rules);
    const context = createContext(node);
    const result = generator.generateSelectStatement(context);
    // Expected: replace with 'user@example.com'
    const expected = `SELECT 'user@example.com' AS email FROM source_table`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('generates CREDIT_CARD masking', () => {
    const rules = [{ column: 'credit_card', maskingType: 'CREDIT_CARD' }];
    const node = createMockNode(rules);
    const context = createContext(node);
    const result = generator.generateSelectStatement(context);
    const expected = `SELECT '****-****-****-1234' AS credit_card FROM source_table`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('generates PHONE masking', () => {
    const rules = [{ column: 'phone', maskingType: 'PHONE' }];
    const node = createMockNode(rules);
    const context = createContext(node);
    const result = generator.generateSelectStatement(context);
    const expected = `SELECT '***-***-1234' AS phone FROM source_table`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('generates SSN masking', () => {
    const rules = [{ column: 'ssn', maskingType: 'SSN' }];
    const node = createMockNode(rules);
    const context = createContext(node);
    const result = generator.generateSelectStatement(context);
    const expected = `SELECT '***-**-1234' AS ssn FROM source_table`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('generates CUSTOM expression masking', () => {
    const rules = [{ column: 'name', maskingType: 'CUSTOM', parameters: { customExpression: "CONCAT(SUBSTRING(name,1,1), '***')" } }];
    const node = createMockNode(rules);
    const context = createContext(node);
    const result = generator.generateSelectStatement(context);
    const expected = `SELECT CONCAT(SUBSTRING(name,1,1), '***') AS name FROM source_table`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('handles multiple rules on different columns', () => {
    const rules = [
      { column: 'email', maskingType: 'EMAIL' },
      { column: 'ssn', maskingType: 'SSN' },
      { column: 'salary', maskingType: 'NULLIFY' },
    ];
    const node = createMockNode(rules);
    const context = createContext(node);
    const result = generator.generateSelectStatement(context);
    const expected = `SELECT 'user@example.com' AS email, '***-**-1234' AS ssn, NULL AS salary FROM source_table`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('falls back to passthrough SELECT when no rules', () => {
    const node = createMockNode([]);
    // Add a fallback schema with columns
    node.metadata!.schemas!.output!.fields = [
      { name: 'col1', type: 'STRING', nullable: true, isKey: false, id: 'c1' },
      { name: 'col2', type: 'INTEGER', nullable: true, isKey: false, id: 'c2' },
    ];
    const context = createContext(node);
    const result = generator.generateSelectStatement(context);
    const expected = `SELECT col1, col2 FROM source_table`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('throws error if output schema missing and no rules', () => {
    const node = createMockNode([]);
    delete node.metadata!.schemas!.output;
    const context = createContext(node);
    // Should return an error fragment
    const result = generator.generateSelectStatement(context);
    expect(result.errors.length).toBeGreaterThan(0);
    expect(result.errors[0].code).toBe('NO_OUTPUT_SCHEMA');
  });
});