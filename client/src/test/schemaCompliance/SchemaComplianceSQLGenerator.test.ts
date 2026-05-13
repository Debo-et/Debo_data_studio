// src/test/generators/SchemaComplianceSQLGenerator.test.ts
import { SchemaComplianceSQLGenerator } from '../../generators/SchemaComplianceSQLGenerator';
import { UnifiedCanvasNode, NodeType, NodeStatus } from '../../types/unified-pipeline.types';
import { SQLGenerationContext } from '../../generators/BaseSQLGenerator';
import { compareSQL } from '../utils/sqlComparator';
import { SchemaComplianceCheckConfiguration } from '../../types/unified-pipeline.types'; // added import

describe('SchemaComplianceSQLGenerator', () => {
  let generator: SchemaComplianceSQLGenerator;

  beforeEach(() => {
    generator = new SchemaComplianceSQLGenerator({ postgresVersion: '14.0', includeComments: false, formatSQL: false });
  });

  // Helper to create a valid compilerMetadata object
  const createCompilerMetadata = () => ({
    lastModified: new Date().toISOString(),
    createdBy: 'test-user',
    validationStatus: 'VALID' as const,
    dependencies: [] as string[],
    warnings: [],
    compiledSql: undefined,
  });

  const createNodeWithConfig = (expectedSchema: any[]): UnifiedCanvasNode => ({
    id: 'compliance-node',
    name: 'Schema Check',
    type: NodeType.SCHEMA_COMPLIANCE_CHECK,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      configuration: {
        type: 'SCHEMA_COMPLIANCE_CHECK',
        config: {
          version: '1.0',
          expectedSchema,
          mode: 'strict',
          errorHandling: 'skipRow',
          options: { continueOnFirstError: true },   // options can be provided here directly
          compilerMetadata: createCompilerMetadata(), // ✅ now satisfies the interface
        },
      },
      schemas: {
        input: [{ id: 'in', name: 'Input', fields: [], isTemporary: false, isMaterialized: false }],
      },
    },
    status: NodeStatus.IDLE,
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
      optimizeForReadability: true,
      includeExecutionPlan: false,
      parameterizeValues: false,
      maxLineLength: 80,
    },
  });

  it('generates SELECT with __is_valid column for simple schema', () => {
    const expectedSchema = [
      { id: 'c1', name: 'id', dataType: 'INTEGER', nullable: false, required: true },
      { id: 'c2', name: 'name', dataType: 'STRING', nullable: true },
    ];
    const node = createNodeWithConfig(expectedSchema);
    const context = createContext(node);
    const result = generator.generateSQL(context);
    const expectedSQL = `SELECT *, CASE WHEN id IS NOT NULL AND id ~ '^\\d+$' THEN 1 ELSE 0 END AS __is_valid FROM source_table`;
    const comparison = compareSQL(result.sql, expectedSQL);
    expect(comparison.success).toBe(true);
  });

  it('handles multiple columns with nullability and type checks', () => {
    const expectedSchema = [
      { id: 'c1', name: 'email', dataType: 'STRING', nullable: false, required: true },
      { id: 'c2', name: 'age', dataType: 'INTEGER', nullable: true, required: true },
      { id: 'c3', name: 'active', dataType: 'BOOLEAN', nullable: false, required: true },
    ];
    const node = createNodeWithConfig(expectedSchema);
    const context = createContext(node);
    const result = generator.generateSQL(context);
    const expectedSQL = `SELECT *, CASE WHEN email IS NOT NULL AND (age IS NULL OR age ~ '^\\d+$') AND active IS NOT NULL THEN 1 ELSE 0 END AS __is_valid FROM source_table`;
    const comparison = compareSQL(result.sql, expectedSQL);
    expect(comparison.success).toBe(true);
  });

  it('adds length check when length is specified', () => {
    const expectedSchema = [
      { id: 'c1', name: 'code', dataType: 'STRING', nullable: false, length: 10, required: true },
    ];
    const node = createNodeWithConfig(expectedSchema);
    const context = createContext(node);
    const result = generator.generateSQL(context);
    const expectedSQL = `SELECT *, CASE WHEN code IS NOT NULL AND length(code::text) <= 10 THEN 1 ELSE 0 END AS __is_valid FROM source_table`;
    const comparison = compareSQL(result.sql, expectedSQL);
    expect(comparison.success).toBe(true);
  });

  it('uses fallback SELECT * when no expected schema is defined', () => {
    const node = createNodeWithConfig([]);
    const context = createContext(node);
    const result = generator.generateSQL(context);
    const expectedSQL = `SELECT * FROM source_table`;
    const comparison = compareSQL(result.sql, expectedSQL);
    expect(comparison.success).toBe(true);
    expect(result.warnings).toContain('No expected schema defined, using fallback SELECT *');
  });

  it('respects case sensitivity option', () => {
    // ✅ Fix: Use type assertion to access the specific config
    const node = createNodeWithConfig([
      { id: 'c1', name: 'Name', dataType: 'STRING', nullable: false, required: true },
    ]);
    const config = node.metadata!.configuration.config as SchemaComplianceCheckConfiguration;
    config.options = { ...config.options, caseSensitiveColumnNames: true };

    const context = createContext(node);
    const result = generator.generateSQL(context);
    // The generated SQL should use exact column name "Name" (quoted if necessary)
    expect(result.sql).toContain('"Name" IS NOT NULL');
  });

  it('includes custom validation expression if provided', () => {
    const expectedSchema = [
      {
        id: 'c1',
        name: 'age',
        dataType: 'INTEGER',
        nullable: false,
        expression: 'age BETWEEN 0 AND 120',
        required: true,
      },
    ];
    const node = createNodeWithConfig(expectedSchema);
    const context = createContext(node);
    const result = generator.generateSQL(context);
    const expectedSQL = `SELECT *, CASE WHEN age IS NOT NULL AND age BETWEEN 0 AND 120 THEN 1 ELSE 0 END AS __is_valid FROM source_table`;
    const comparison = compareSQL(result.sql, expectedSQL);
    expect(comparison.success).toBe(true);
  });

  it('handles multiple validation rules for a column', () => {
    const expectedSchema = [
      {
        id: 'c1',
        name: 'price',
        dataType: 'DECIMAL',
        nullable: false,
        required: true,
        validationRules: [
          { id: 'r1', type: 'range', params: { min: 0, max: 1000 } },
          { id: 'r2', type: 'expression', params: { expression: 'price % 10 = 0' } },
        ],
      },
    ];
    const node = createNodeWithConfig(expectedSchema);
    const context = createContext(node);
    const result = generator.generateSQL(context);
    const expectedSQL = `SELECT *, CASE WHEN price IS NOT NULL AND price >= 0 AND price <= 1000 AND price % 10 = 0 THEN 1 ELSE 0 END AS __is_valid FROM source_table`;
    const comparison = compareSQL(result.sql, expectedSQL);
    expect(comparison.success).toBe(true);
  });
});