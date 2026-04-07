// src/test/generators/ExtractRegexSQLGenerator.test.ts

import { ExtractRegexSQLGenerator } from '../../generators/ExtractRegexSQLGenerator';
import { UnifiedCanvasNode, NodeType } from '../../types/unified-pipeline.types';

// ---------- SQL Comparison Helper (self-contained) ----------
interface ComparisonResult {
  success: boolean;
  diff?: string;
}

function normalize(sql: string, caseSensitive = false): string {
  let normalized = sql.trim().replace(/\s+/g, ' ');
  if (!caseSensitive) normalized = normalized.toLowerCase();
  return normalized.replace(/;$/, '');
}

function compareSQL(actual: string, expected: string): ComparisonResult {
  const actualNorm = normalize(actual);
  const expectedNorm = normalize(expected);

  if (actualNorm === expectedNorm) {
    return { success: true };
  }

  const actualLines = actual.split('\n');
  const expectedLines = expected.split('\n');
  const diffLines: string[] = [];

  for (let i = 0; i < Math.max(actualLines.length, expectedLines.length); i++) {
    const actualLine = actualLines[i] || '';
    const expectedLine = expectedLines[i] || '';
    if (actualLine !== expectedLine) {
      diffLines.push(`Line ${i + 1}: expected "${expectedLine}"`);
      diffLines.push(`          actual   "${actualLine}"`);
    }
  }

  return {
    success: false,
    diff: diffLines.join('\n'),
  };
}

// ---------- Tests ----------
describe('ExtractRegexSQLGenerator', () => {
  let generator: ExtractRegexSQLGenerator;

  beforeEach(() => {
    generator = new ExtractRegexSQLGenerator({
      postgresVersion: '14.0',
      includeComments: false,
      formatSQL: false,
    });
  });

  // Helper to create a minimal valid node
  const createMockNode = (configOverride: any = {}): UnifiedCanvasNode => ({
    id: 'regex-node-1',
    name: 'Extract Logs',
    type: NodeType.EXTRACT_REGEX_FIELDS,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      configuration: {
        type: 'EXTRACT_REGEX_FIELDS',
        config: {
          version: '1.0',
          sourceColumn: 'raw_log',
          regexPattern: '(\\d{4})-(\\d{2})-(\\d{2}) (\\w+) (.*)',
          caseInsensitive: false,
          multiline: false,
          dotAll: false,
          rules: [
            { groupIndex: 1, columnName: 'year', dataType: 'STRING', nullable: true, position: 0, id: 'r1' },
            { groupIndex: 2, columnName: 'month', dataType: 'STRING', nullable: true, position: 1, id: 'r2' },
            { groupIndex: 3, columnName: 'day', dataType: 'STRING', nullable: true, position: 2, id: 'r3' },
            { groupIndex: 4, columnName: 'level', dataType: 'STRING', nullable: true, position: 3, id: 'r4' },
            { groupIndex: 5, columnName: 'message', dataType: 'STRING', nullable: true, position: 4, id: 'r5' },
          ],
          errorHandling: { onNoMatch: 'skipRow', onConversionError: 'setNull' },
          parallelization: true,
          batchSize: 1000,
          outputSchema: {
            id: 'out',
            name: 'Output',
            fields: [
              { id: 'f1', name: 'year', type: 'STRING', nullable: true, isKey: false },
              { id: 'f2', name: 'month', type: 'STRING', nullable: true, isKey: false },
              { id: 'f3', name: 'day', type: 'STRING', nullable: true, isKey: false },
              { id: 'f4', name: 'level', type: 'STRING', nullable: true, isKey: false },
              { id: 'f5', name: 'message', type: 'STRING', nullable: true, isKey: false },
            ],
            isTemporary: false,
            isMaterialized: false,
          },
          compilerMetadata: {
            lastModified: '',
            createdBy: 'test',
            validationStatus: 'VALID',
            warnings: [],
            dependencies: ['raw_log'],
          },
          ...configOverride,
        },
      },
      schemas: {
        output: {
          id: 'out',
          name: 'Output',
          fields: [
            { id: 'f1', name: 'year', type: 'STRING', nullable: true, isKey: false },
            { id: 'f2', name: 'month', type: 'STRING', nullable: true, isKey: false },
            { id: 'f3', name: 'day', type: 'STRING', nullable: true, isKey: false },
            { id: 'f4', name: 'level', type: 'STRING', nullable: true, isKey: false },
            { id: 'f5', name: 'message', type: 'STRING', nullable: true, isKey: false },
          ],
          isTemporary: false,
          isMaterialized: false,
        },
      },
    },
  });

  const createContext = (node: UnifiedCanvasNode) => ({
    node,
    indentLevel: 0,
    parameters: new Map(),
    options: {
      includeComments: false,
      formatSQL: false,
      targetDialect: 'POSTGRESQL' as const,
      postgresVersion: '14.0',
      useCTEs: false,
      optimizeForReadability: true,
      includeExecutionPlan: false,
      parameterizeValues: true,
      maxLineLength: 80,
    },
  });

  it('generates correct SQL for valid regex configuration', () => {
    const node = createMockNode();
    const context = createContext(node);
    const result = generator.generateSQL(context);

    const expectedSQL = `
      SELECT year, month, day, level, message,
             (regexp_matches(raw_log, '(\\d{4})-(\\d{2})-(\\d{2}) (\\w+) (.*)', ''))[1] AS year,
             (regexp_matches(raw_log, '(\\d{4})-(\\d{2})-(\\d{2}) (\\w+) (.*)', ''))[2] AS month,
             (regexp_matches(raw_log, '(\\d{4})-(\\d{2})-(\\d{2}) (\\w+) (.*)', ''))[3] AS day,
             (regexp_matches(raw_log, '(\\d{4})-(\\d{2})-(\\d{2}) (\\w+) (.*)', ''))[4] AS level,
             (regexp_matches(raw_log, '(\\d{4})-(\\d{2})-(\\d{2}) (\\w+) (.*)', ''))[5] AS message
      FROM source_table
    `;

    const comparison = compareSQL(result.sql, expectedSQL);
    expect(comparison.success).toBe(true);
  });

  it('includes flags when caseInsensitive/multiline/dotAll are true', () => {
    const node = createMockNode({
      caseInsensitive: true,
      multiline: true,
      dotAll: true,
    });
    const context = createContext(node);
    const result = generator.generateSQL(context);

    // Flags should be "ims" (i + m + s)
    expect(result.sql).toContain(`regexp_matches(raw_log, '(\\d{4})-(\\d{2})-(\\d{2}) (\\w+) (.*)', 'ims')`);
  });

  it('handles missing configuration gracefully (fallback)', () => {
    const node = createMockNode();
    // ✅ FIX: delete the configuration property instead of setting to undefined
    if (node.metadata) {
      node.metadata = undefined;
    }

    const context = createContext(node);
    const result = generator.generateSQL(context);

    expect(result.errors).toHaveLength(1);
    expect(result.errors[0].code).toBe('MISSING_REGEX_CONFIG');
    expect(result.sql).toBe('SELECT * FROM source_table');
  });

  it('handles invalid regex pattern (error in fragment)', () => {
    const node = createMockNode({ regexPattern: '(unclosed' });
    const context = createContext(node);
    const result = generator.generateSQL(context);

    // Expect errors (BaseSQLGenerator catches invalid regex)
    expect(result.errors.length).toBeGreaterThan(0);
    expect(result.errors.some(e => e.message.toLowerCase().includes('regex'))).toBe(true);
  });

  it('includes other output columns in SELECT (non-regex columns)', () => {
    const node = createMockNode();
    // Add an extra column that is not a regex group
    if (node.metadata?.schemas?.output) {
      node.metadata.schemas.output.fields.push({
        id: 'f6',
        name: 'static_id',
        type: 'STRING',
        nullable: false,
        isKey: true,
      });
    }
    const context = createContext(node);
    const result = generator.generateSQL(context);

    // The SELECT should contain 'static_id' before the regex columns
    expect(result.sql).toMatch(/SELECT static_id, year, month, day, level, message,/);
  });

  it('handles zero capturing groups (should produce error)', () => {
    const node = createMockNode({ regexPattern: 'no_groups_here' });
    const context = createContext(node);
    const result = generator.generateSQL(context);

    // No groups means no rules -> at least one rule required
    expect(result.errors.some(e => e.message.toLowerCase().includes('capturing'))).toBe(true);
  });

  it('generates SQL with proper identifier quoting for special column names', () => {
    const node = createMockNode({
      sourceColumn: 'raw log',  // contains space
      rules: [
        { groupIndex: 1, columnName: 'year value', dataType: 'STRING', nullable: true, position: 0, id: 'r1' },
      ],
    });
    // Override schema to match
    if (node.metadata?.schemas?.output) {
      node.metadata.schemas.output.fields = [
        { id: 'f1', name: 'year value', type: 'STRING', nullable: true, isKey: false },
      ];
    }
    const context = createContext(node);
    const result = generator.generateSQL(context);

    // Expect double quoting
    expect(result.sql).toContain('"raw log"');
    expect(result.sql).toContain('"year value"');
  });
});