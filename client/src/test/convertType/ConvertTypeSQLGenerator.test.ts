// src/generators/ConvertTypeSQLGenerator.test.ts
import { ConvertTypeSQLGenerator } from '../../generators//ConvertTypeSQLGenerator';
import { SQLGenerationContext } from '../../generators/BaseSQLGenerator';
import {
  UnifiedCanvasNode,
  ConvertComponentConfiguration,
  NodeType,
  PostgreSQLDataType,
} from '../../types/unified-pipeline.types';
import { compareSQL } from '../utils/sqlComparator';
import { mockInitialConfig } from './mocks/convertTypeMocks';

describe('ConvertTypeSQLGenerator', () => {
  let generator: ConvertTypeSQLGenerator;
  let mockNode: UnifiedCanvasNode;
  let mockContext: SQLGenerationContext;

  beforeEach(() => {
    generator = new ConvertTypeSQLGenerator({ postgresVersion: '14.0', includeComments: false, formatSQL: false });
    mockNode = {
      id: 'convert-node',
      name: 'Convert Node',
      type: NodeType.CONVERT_TYPE,
      position: { x: 0, y: 0 },
      size: { width: 200, height: 120 },
      metadata: {
        configuration: { type: 'CONVERT', config: mockInitialConfig },
        schemas: {
          output: {
            id: 'output',
            name: 'Output',
            fields: [
              { id: 'f1', name: 'full_name', type: 'STRING', nullable: true, isKey: false },
              { id: 'f2', name: 'age_group', type: 'STRING', nullable: true, isKey: false },
              { id: 'f3', name: 'birth_year', type: 'INTEGER', nullable: true, isKey: false },
            ],
            isTemporary: false,
            isMaterialized: false,
          },
        },
      },
    };
    mockContext = {
      node: mockNode,
      connection: undefined,
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
        parameterizeValues: true,
        maxLineLength: 80,
      },
    };
  });

  it('generates SELECT with CASTs for simple conversions', () => {
    const config: ConvertComponentConfiguration = {
      version: '1.0',
      rules: [
        {
          id: 'r1',
          sourceColumn: 'first_name',
          targetColumn: 'full_name',
          targetType: PostgreSQLDataType.VARCHAR,
          position: 0,
        },
        {
          id: 'r2',
          sourceColumn: 'age',
          targetColumn: 'age_str',
          targetType: PostgreSQLDataType.VARCHAR,
          position: 1,
        },
      ],
      outputSchema: {
        id: 'out',
        name: 'Output',
        fields: [],
        isTemporary: false,
        isMaterialized: false,
      },
      sqlGeneration: { requiresCasting: true, usesConditionalLogic: false, estimatedRowMultiplier: 1.0 },
      compilerMetadata: {
        lastModified: '',
        createdBy: '',
        ruleCount: 2,
        validationStatus: 'VALID',
        dependencies: [],
      },
    };

    mockNode.metadata!.configuration = { type: 'CONVERT', config };
    const result = generator.generateSelectStatement(mockContext);
    const expected = `SELECT CAST(first_name AS VARCHAR) AS full_name, CAST(age AS VARCHAR) AS age_str FROM source_table`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('generates SELECT with TO_DATE for date conversions with format', () => {
    const config: ConvertComponentConfiguration = {
      version: '1.0',
      rules: [
        {
          id: 'r1',
          sourceColumn: 'birth_date',
          targetColumn: 'birth_date',
          targetType: PostgreSQLDataType.DATE,
          position: 0,
          parameters: { format: 'YYYY-MM-DD' },
        },
      ],
      outputSchema: {
        id: 'out',
        name: 'Output',
        fields: [],
        isTemporary: false,
        isMaterialized: false,
      },
      sqlGeneration: { requiresCasting: true, usesConditionalLogic: false, estimatedRowMultiplier: 1.0 },
      compilerMetadata: {
        lastModified: '',
        createdBy: '',
        ruleCount: 1,
        validationStatus: 'VALID',
        dependencies: [],
      },
    };

    mockNode.metadata!.configuration = { type: 'CONVERT', config };
    const result = generator.generateSelectStatement(mockContext);
    const expected = `SELECT TO_DATE(birth_date, 'YYYY-MM-DD') AS birth_date FROM source_table`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('generates SELECT with COALESCE fallback expression', () => {
    const config: ConvertComponentConfiguration = {
      version: '1.0',
      rules: [
        {
          id: 'r1',
          sourceColumn: 'age',
          targetColumn: 'age_group',
          targetType: PostgreSQLDataType.VARCHAR,
          position: 0,
          parameters: { defaultValue: "'Unknown'" }, // ✅ was fallbackExpression
        },
      ],
      outputSchema: {
        id: 'out',
        name: 'Output',
        fields: [],
        isTemporary: false,
        isMaterialized: false,
      },
      sqlGeneration: { requiresCasting: true, usesConditionalLogic: true, estimatedRowMultiplier: 1.0 },
      compilerMetadata: {
        lastModified: '',
        createdBy: '',
        ruleCount: 1,
        validationStatus: 'VALID',
        dependencies: [],
      },
    };

    mockNode.metadata!.configuration = { type: 'CONVERT', config };
    const result = generator.generateSelectStatement(mockContext);
    const expected = `SELECT COALESCE(CAST(age AS VARCHAR), 'Unknown') AS age_group FROM source_table`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('generates SELECT with multiple columns and correct order', () => {
    const config: ConvertComponentConfiguration = {
      version: '1.0',
      rules: [
        {
          id: 'r1',
          sourceColumn: 'first_name',
          targetColumn: 'full_name',
          targetType: PostgreSQLDataType.VARCHAR,
          position: 0,
        },
        {
          id: 'r2',
          sourceColumn: 'age',
          targetColumn: 'age_group',
          targetType: PostgreSQLDataType.VARCHAR,
          position: 1,
          parameters: { defaultValue: "'Unknown'" }, // ✅ was fallbackExpression
        },
        {
          id: 'r3',
          sourceColumn: 'birth_date',
          targetColumn: 'birth_year',
          targetType: PostgreSQLDataType.INTEGER,
          position: 2,
          parameters: { format: 'YYYY' },
        },
      ],
      outputSchema: {
        id: 'out',
        name: 'Output',
        fields: [],
        isTemporary: false,
        isMaterialized: false,
      },
      sqlGeneration: { requiresCasting: true, usesConditionalLogic: true, estimatedRowMultiplier: 1.0 },
      compilerMetadata: {
        lastModified: '',
        createdBy: '',
        ruleCount: 3,
        validationStatus: 'VALID',
        dependencies: [],
      },
    };

    mockNode.metadata!.configuration = { type: 'CONVERT', config };
    const result = generator.generateSelectStatement(mockContext);
    const expected = `SELECT CAST(first_name AS VARCHAR) AS full_name, COALESCE(CAST(age AS VARCHAR), 'Unknown') AS age_group, CAST(TO_DATE(birth_date, 'YYYY') AS INTEGER) AS birth_year FROM source_table`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('falls back to SELECT * when no conversion rules', () => {
    const config: ConvertComponentConfiguration = {
      version: '1.0',
      rules: [],
      outputSchema: {
        id: 'out',
        name: 'Output',
        fields: [],
        isTemporary: false,
        isMaterialized: false,
      },
      sqlGeneration: { requiresCasting: false, usesConditionalLogic: false, estimatedRowMultiplier: 1.0 },
      compilerMetadata: {
        lastModified: '',
        createdBy: '',
        ruleCount: 0,
        validationStatus: 'VALID',
        dependencies: [],
      },
    };

    mockNode.metadata!.configuration = { type: 'CONVERT', config };
    const result = generator.generateSelectStatement(mockContext);
    const expected = `SELECT * FROM source_table`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });
});