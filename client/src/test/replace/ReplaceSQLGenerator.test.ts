import { ReplaceSQLGenerator } from '../../generators/ReplaceSQLGenerator';
import { SQLGenerationContext } from '../../generators/BaseSQLGenerator';
import { compareSQL } from '../../test/utils/sqlComparator';
import { UnifiedCanvasNode, ReplaceComponentConfiguration, NodeType } from '../../types/unified-pipeline.types';


describe('ReplaceSQLGenerator', () => {
  let generator: ReplaceSQLGenerator;

  beforeEach(() => {
    generator = new ReplaceSQLGenerator({
      postgresVersion: '14.0',
      includeComments: false,
      formatSQL: false,
      targetDialect: 'POSTGRESQL',
    });
  });

  const createMockContext = (config: ReplaceComponentConfiguration): SQLGenerationContext => ({
    node: {
      id: 'replace1',
      name: 'Replace',
      type: NodeType.REPLACE,          // ✅ Use enum to match UnifiedCanvasNode type
      position: { x: 0, y: 0 },
      size: { width: 200, height: 100 },
      metadata: {
        configuration: { type: 'REPLACE', config },
        schemas: {
          output: {
            id: 'out_schema',
            name: 'Output',
            fields: [
              { id: 'f1', name: 'first_name', type: 'STRING', nullable: true, isKey: false },
              { id: 'f2', name: 'last_name', type: 'STRING', nullable: true, isKey: false },
              { id: 'f3', name: 'age', type: 'INTEGER', nullable: true, isKey: false },
            ],
            isTemporary: false,
            isMaterialized: false,
          },
        },
      },
    } as UnifiedCanvasNode,
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
      parameterizeValues: true,
      maxLineLength: 80,
    },
  });

  it('generates SQL with simple REPLACE rules', () => {
    const config: ReplaceComponentConfiguration = {
      version: '1.0',
      rules: [
        {
          id: 'r1',
          column: 'first_name',
          searchValue: 'John',
          replacement: 'Jonathan',
          caseSensitive: false,
          regex: false,
          scope: 'all',
          position: 0,
        },
      ],
      outputSchema: {
        id: 'out',
        name: 'Output',
        fields: [],
        isTemporary: false,
        isMaterialized: false,
      },
      sqlGeneration: { requiresRegex: false },
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        createdBy: 'test',
        ruleCount: 1,
        validationStatus: 'VALID',
        dependencies: [],
      },
    };
    const context = createMockContext(config);
    const result = generator.generateSQL(context);
    const expected = `SELECT REPLACE(first_name, 'John', 'Jonathan') AS first_name, last_name, age FROM source_table`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('generates SQL with REGEXP_REPLACE rule', () => {
    const config: ReplaceComponentConfiguration = {
      version: '1.0',
      rules: [
        {
          id: 'r1',
          column: 'last_name',
          searchValue: '\\d+',
          replacement: '',
          caseSensitive: true,
          regex: true,
          scope: 'all',
          position: 0,
        },
      ],
      outputSchema: {
        id: 'out',
        name: 'Output',
        fields: [],
        isTemporary: false,
        isMaterialized: false,
      },
      sqlGeneration: { requiresRegex: true },
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        createdBy: 'test',
        ruleCount: 1,
        validationStatus: 'VALID',
        dependencies: [],
      },
    };
    const context = createMockContext(config);
    const result = generator.generateSQL(context);
    const expected = `SELECT REGEXP_REPLACE(last_name, '\\d+', '', 'g') AS last_name, first_name, age FROM source_table`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('generates SQL with multiple rules', () => {
    const config: ReplaceComponentConfiguration = {
      version: '1.0',
      rules: [
        {
          id: 'r1',
          column: 'first_name',
          searchValue: 'John',
          replacement: 'Jonathan',
          caseSensitive: false,
          regex: false,
          scope: 'all',
          position: 0,
        },
        {
          id: 'r2',
          column: 'age',
          searchValue: '0',
          replacement: 'unknown',
          caseSensitive: false,
          regex: false,
          scope: 'all',
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
      sqlGeneration: { requiresRegex: false },
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        createdBy: 'test',
        ruleCount: 2,
        validationStatus: 'VALID',
        dependencies: [],
      },
    };
    const context = createMockContext(config);
    const result = generator.generateSQL(context);
    const expected = `SELECT REPLACE(first_name, 'John', 'Jonathan') AS first_name, last_name, REPLACE(age, '0', 'unknown') AS age FROM source_table`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('handles case when no rules are defined (fallback to SELECT *)', () => {
    const config: ReplaceComponentConfiguration = {
      version: '1.0',
      rules: [],
      outputSchema: {
        id: 'out',
        name: 'Output',
        fields: [],
        isTemporary: false,
        isMaterialized: false,
      },
      sqlGeneration: { requiresRegex: false },
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        createdBy: 'test',
        ruleCount: 0,
        validationStatus: 'VALID',
        dependencies: [],
      },
    };
    const context = createMockContext(config);
    const result = generator.generateSQL(context);
    const expected = `SELECT * FROM source_table`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('handles rule for a column that does not exist in output schema (should ignore)', () => {
    const config: ReplaceComponentConfiguration = {
      version: '1.0',
      rules: [
        {
          id: 'r1',
          column: 'non_existent_column',
          searchValue: 'test',
          replacement: 'changed',
          caseSensitive: false,
          regex: false,
          scope: 'all',
          position: 0,
        },
      ],
      outputSchema: {
        id: 'out',
        name: 'Output',
        fields: [
          { id: 'f1', name: 'first_name', type: 'STRING', nullable: true, isKey: false },
        ],
        isTemporary: false,
        isMaterialized: false,
      },
      sqlGeneration: { requiresRegex: false },
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        createdBy: 'test',
        ruleCount: 1,
        validationStatus: 'VALID',
        dependencies: [],
      },
    };
    const context = createMockContext(config);
    const result = generator.generateSQL(context);
    const expected = `SELECT first_name FROM source_table`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });
});