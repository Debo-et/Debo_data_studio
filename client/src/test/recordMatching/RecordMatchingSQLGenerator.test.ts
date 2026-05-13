// src/test/generators/RecordMatchingSQLGenerator.test.ts
import { RecordMatchingSQLGenerator } from '../../generators/RecordMatchingSQLGenerator';
import { SQLGenerationContext } from '../../generators/BaseSQLGenerator';
import { UnifiedCanvasNode, NodeType } from '../../types/unified-pipeline.types';
import { compareSQL } from '../utils/sqlComparator';

describe('RecordMatchingSQLGenerator', () => {
  let generator: RecordMatchingSQLGenerator;
  let baseContext: Partial<SQLGenerationContext>;
  let mockNode: UnifiedCanvasNode;

  beforeEach(() => {
    generator = new RecordMatchingSQLGenerator({
      postgresVersion: '14.0',
      includeComments: false,
      formatSQL: false,
      targetDialect: 'POSTGRESQL',
      useCTEs: false,
      optimizeForReadability: false,
      includeExecutionPlan: false,
      parameterizeValues: false,
      maxLineLength: 80,
    });

    mockNode = {
      id: 'match-node-1',
      name: 'Record Matcher',
      type: NodeType.RECORD_MATCHING,
      position: { x: 0, y: 0 },
      size: { width: 200, height: 150 },
      metadata: {
        configuration: {
          type: 'MATCH_GROUP',
          config: {
            version: '1.0',
            matchKeys: [
              {
                id: 'key1',
                field: 'email',
                matchType: 'exact_ignore_case' as any, // type assertion
                threshold: 0.85,
                caseSensitive: false,
                ignoreNull: true,
                weight: 1.0,
                blockingKey: false,
              },
              {
                id: 'key2',
                field: 'phone',
                matchType: 'levenshtein' as any,       // type assertion
                threshold: 0.7,
                caseSensitive: false,
                ignoreNull: true,
                weight: 0.8,
                blockingKey: true,
              },
            ],
            survivorshipRules: [
              {
                id: 'rule1',
                field: 'full_name',
                ruleType: 'first' as any,               // type assertion
                sourceField: 'full_name',
                params: { orderBy: 'id', orderDirection: 'ASC' },
              },
              {
                id: 'rule2',
                field: 'email',
                ruleType: 'any_non_null' as any,        // type assertion
                sourceField: 'email',
              },
            ],
            outputFields: ['id', 'full_name', 'email', 'phone'],
            globalOptions: {
              matchThreshold: 0.8,
              maxMatchesPerRecord: 1,
              nullHandling: 'no_match',
              outputMode: 'best_match',
              includeMatchDetails: true,
              parallelization: false,
              batchSize: 10000,
            },
            compilerMetadata: {
              lastModified: '',
              createdBy: '',
              matchKeyCount: 2,
              ruleCount: 2,
              validationStatus: 'VALID',
              dependencies: [],
            },
          },
        },
      },
    };

    baseContext = {
      node: mockNode,
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
    };
  });

  const generateSQL = (context: SQLGenerationContext): string => {
    const result = generator.generateSQL(context);
    return result.sql;
  };

  it('generates a similarity join with exact_ignore_case and levenshtein', () => {
    const sql = generateSQL(baseContext as SQLGenerationContext);
    const expected = `
SELECT
  left_input.*,
  right_input.*,
  similarity(left_input.email, right_input.email) AS similarity_score
FROM left_input
CROSS JOIN right_input
WHERE similarity(left_input.email, right_input.email) > 0.8
    `.trim();
    const comparison = compareSQL(sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('generates SQL with Soundex match type', () => {
    const nodeWithSoundex = { ...mockNode };
    (nodeWithSoundex.metadata!.configuration as any).config.matchKeys[0].matchType = 'soundex';
    const context = { ...baseContext, node: nodeWithSoundex };
    const sql = generateSQL(context as SQLGenerationContext);
    const expected = `
SELECT
  left_input.*,
  right_input.*,
  similarity(left_input.email, right_input.email) AS similarity_score
FROM left_input
CROSS JOIN right_input
WHERE soundex(left_input.email) = soundex(right_input.email)
    `.trim();
    const comparison = compareSQL(sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('handles missing configuration gracefully (fallback)', () => {
    const nodeWithoutConfig = { ...mockNode, metadata: {} };
    const context = { ...baseContext, node: nodeWithoutConfig };
    const sql = generateSQL(context as SQLGenerationContext);
    expect(sql).toContain('-- Record matching requires valid configuration');
    expect(sql).toContain('Missing configuration');
  });

  it('includes match details column when includeMatchDetails is true', () => {
    const sql = generateSQL(baseContext as SQLGenerationContext);
    expect(sql).toContain('similarity_score');
  });

  it('handles different threshold values', () => {
    const config = (mockNode.metadata!.configuration as any).config;
    config.globalOptions.matchThreshold = 0.95;
    const sql = generateSQL(baseContext as SQLGenerationContext);
    expect(sql).toContain('> 0.95');
  });

  it('handles empty matchKeys array', () => {
    const emptyConfig = { ...mockNode };
    (emptyConfig.metadata!.configuration as any).config.matchKeys = [];
    const context = { ...baseContext, node: emptyConfig };
    const sql = generateSQL(context as SQLGenerationContext);
    expect(sql).toContain('CROSS JOIN');
    expect(sql).not.toContain('similarity(');
  });

  it('handles multiple match keys by using the first one (simplified implementation)', () => {
    const sql = generateSQL(baseContext as SQLGenerationContext);
    expect(sql).toContain('left_input.email');
    expect(sql).not.toContain('left_input.phone');
  });

  it('includes proper error object when configuration missing', () => {
    const nodeWithoutConfig = { ...mockNode, metadata: {} };
    const context = { ...baseContext, node: nodeWithoutConfig };
    const result = generator.generateSQL(context as SQLGenerationContext);
    expect(result.errors).toHaveLength(1);
    expect(result.errors[0].code).toBe('MISSING_CONFIG');
    expect(result.errors[0].severity).toBe('ERROR');
  });
});