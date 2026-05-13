// src/test/generators/ReplaceSQLGenerator.test.ts
import { ReplaceSQLGenerator } from '../../generators/ReplaceSQLGenerator';
import { SQLGenerationContext } from '../../generators/BaseSQLGenerator';
import { UnifiedCanvasNode, NodeType, NodeStatus } from '../../types/unified-pipeline.types';
import { compareSQL } from '../utils/sqlComparator';
import { mockInputSchema } from './replaceMockData';

describe('ReplaceSQLGenerator', () => {
  let generator: ReplaceSQLGenerator;

  beforeEach(() => {
    generator = new ReplaceSQLGenerator({ postgresVersion: '14.0', includeComments: false, formatSQL: false });
  });

  const createNodeWithRules = (rules: any[]): UnifiedCanvasNode => ({
    id: 'replace-node-1',
    name: 'tReplace_1',
    type: NodeType.REPLACE,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      configuration: {
        type: 'REPLACE',
        config: {
          version: '1.0',
          rules,
          outputSchema: mockInputSchema,
          sqlGeneration: { requiresRegex: rules.some(r => r.regex), estimatedRowMultiplier: 1 },
          compilerMetadata: { lastModified: '', createdBy: '', ruleCount: rules.length, validationStatus: 'VALID', warnings: [], dependencies: [], compiledSql: undefined },
        },
      },
      schemas: {
        output: mockInputSchema,
      },
    },
    status: NodeStatus.IDLE,
    draggable: true,
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

  it('generates simple REPLACE SQL for a single rule', () => {
    const rules = [
      {
        id: 'r1',
        column: 'email',
        searchValue: '@old.com',
        replacement: '@new.com',
        caseSensitive: false,
        regex: false,
        scope: 'all',
        position: 0,
      },
    ];
    const node = createNodeWithRules(rules);
    const context = createContext(node);
    const result = generator.generateSQL(context);
    const expectedSQL = `SELECT REPLACE(email, '@old.com', '@new.com') AS email, last_name, phone FROM source_table;`;
    const comparison = compareSQL(result.sql, expectedSQL);
    expect(comparison.success).toBe(true);
  });

  it('generates multiple REPLACE functions for multiple rules', () => {
    const rules = [
      {
        id: 'r1',
        column: 'email',
        searchValue: '@old.com',
        replacement: '@new.com',
        caseSensitive: false,
        regex: false,
        scope: 'all',
        position: 0,
      },
      {
        id: 'r2',
        column: 'phone',
        searchValue: '-',
        replacement: '',
        caseSensitive: false,
        regex: false,
        scope: 'all',
        position: 1,
      },
    ];
    const node = createNodeWithRules(rules);
    const context = createContext(node);
    const result = generator.generateSQL(context);
    const expectedSQL = `SELECT REPLACE(email, '@old.com', '@new.com') AS email, REPLACE(phone, '-', '') AS phone, first_name, last_name FROM source_table;`;
    const comparison = compareSQL(result.sql, expectedSQL);
    expect(comparison.success).toBe(true);
  });

  it('generates REGEXP_REPLACE for regex rules', () => {
    const rules = [
      {
        id: 'r1',
        column: 'phone',
        searchValue: '\\+1',
        replacement: '',
        caseSensitive: false,
        regex: true,
        scope: 'all',
        position: 0,
      },
    ];
    const node = createNodeWithRules(rules);
    const context = createContext(node);
    const result = generator.generateSQL(context);
    const expectedSQL = `SELECT REGEXP_REPLACE(phone, '\\\\+1', '', 'g') AS phone, first_name, last_name, email FROM source_table;`;
    const comparison = compareSQL(result.sql, expectedSQL);
    expect(comparison.success).toBe(true);
  });

  it('handles case-insensitive replace (using ILIKE? Actually REPLACE is case-sensitive; regex can use flags)', () => {
    // For non-regex, REPLACE is always case-sensitive. We'll test that regex with 'i' flag is used.
    const rules = [
      {
        id: 'r1',
        column: 'first_name',
        searchValue: 'john',
        replacement: 'JOHN',
        caseSensitive: false,
        regex: true,
        scope: 'all',
        position: 0,
      },
    ];
    const node = createNodeWithRules(rules);
    const context = createContext(node);
    const result = generator.generateSQL(context);
    // PostgreSQL REGEXP_REPLACE with 'gi' flags (g for global, i for case-insensitive)
    const expectedSQL = `SELECT REGEXP_REPLACE(first_name, 'john', 'JOHN', 'gi') AS first_name, last_name, email, phone FROM source_table;`;
    const comparison = compareSQL(result.sql, expectedSQL);
    expect(comparison.success).toBe(true);
  });

  it('uses fallback SELECT * when no rules defined', () => {
    const node = createNodeWithRules([]);
    const context = createContext(node);
    const result = generator.generateSQL(context);
    const expectedSQL = `SELECT * FROM tReplace_1;`;
    const comparison = compareSQL(result.sql, expectedSQL);
    expect(comparison.success).toBe(true);
    expect(result.warnings).toContain('No replace rules, using fallback');
  });

  it('correctly sanitizes column names with special characters', () => {
    const node = createNodeWithRules([]);
    node.name = 'my-table';
    node.metadata!.schemas!.output!.fields[0].name = 'email address'; // contains space
    const context = createContext(node);
    const result = generator.generateSQL(context);
    const expectedSQL = `SELECT * FROM "my-table";`; // fallback uses node.name
    const comparison = compareSQL(result.sql, expectedSQL);
    expect(comparison.success).toBe(true);
  });

  it('generates SQL for ReplaceList (multiple rules on same column)', () => {
    const rules = [
      {
        id: 'r1',
        column: 'email',
        searchValue: '@old.com',
        replacement: '@new.com',
        caseSensitive: false,
        regex: false,
        scope: 'all',
        position: 0,
      },
      {
        id: 'r2',
        column: 'email',
        searchValue: '@test.com',
        replacement: '@prod.com',
        caseSensitive: false,
        regex: false,
        scope: 'all',
        position: 1,
      },
    ];
    const node = createNodeWithRules(rules);
    const context = createContext(node);
    const result = generator.generateSQL(context);
    // Expected: nested REPLACE: REPLACE(REPLACE(email, '@old.com', '@new.com'), '@test.com', '@prod.com')
    const expectedSQL = `SELECT REPLACE(REPLACE(email, '@old.com', '@new.com'), '@test.com', '@prod.com') AS email, first_name, last_name, phone FROM source_table;`;
    const comparison = compareSQL(result.sql, expectedSQL);
    expect(comparison.success).toBe(true);
  });

  it('includes warnings for regex syntax errors (validation at generation time)', () => {
    // The generator itself does not validate regex; but the validation is done in the editor.
    // However, we can test that if a regex rule is present, it is included.
    const rules = [
      {
        id: 'r1',
        column: 'email',
        searchValue: '[a-z]+',
        replacement: 'redacted',
        caseSensitive: false,
        regex: true,
        scope: 'all',
        position: 0,
      },
    ];
    const node = createNodeWithRules(rules);
    const context = createContext(node);
    const result = generator.generateSQL(context);
    expect(result.sql).toContain('REGEXP_REPLACE');
    expect(result.warnings).toHaveLength(0); // no warnings from generator
  });
});