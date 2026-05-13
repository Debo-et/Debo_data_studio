// src/test/replicate/replicateSQLGenerator.test.ts
import { compareSQL } from '../utils/sqlComparator';
import { generateReplicateSQL, SimpleColumn } from './replicateSQLHelper';
import { ReplicateComponentConfiguration } from '../../types/unified-pipeline.types';

describe('Replicate SQL Generation', () => {
  const mockInputColumns: SimpleColumn[] = [
    { name: 'user_id', type: 'INTEGER' },
    { name: 'username', type: 'VARCHAR' },
    { name: 'email', type: 'VARCHAR' },
  ];

  const baseConfig: ReplicateComponentConfiguration = {
    version: '1.0',
    addBranchIdentifier: false,
    outputSchema: {} as any,
    sqlGeneration: { passthrough: true, estimatedRowMultiplier: 1.0 },
    compilerMetadata: {} as any,
  };

  it('generates passthrough SQL without branch identifier', () => {
    const sql = generateReplicateSQL(mockInputColumns, baseConfig);
    const expected = `SELECT "user_id", "username", "email" FROM source_data`;
    const result = compareSQL(sql, expected);
    expect(result.success).toBe(true);
  });

  it('generates SQL with branch identifier column', () => {
    const config: ReplicateComponentConfiguration = {
      ...baseConfig,
      addBranchIdentifier: true,
      branchIdentifierColumnName: 'branch_id',
    };
    const sql = generateReplicateSQL(mockInputColumns, config);
    const expected = `SELECT "user_id", "username", "email", 'branch_value' AS "branch_id" FROM source_data`;
    const result = compareSQL(sql, expected);
    expect(result.success).toBe(true);
  });

  it('generates SQL with custom branch column name', () => {
    const config: ReplicateComponentConfiguration = {
      ...baseConfig,
      addBranchIdentifier: true,
      branchIdentifierColumnName: 'replica_source',
    };
    const sql = generateReplicateSQL(mockInputColumns, config);
    const expected = `SELECT "user_id", "username", "email", 'branch_value' AS "replica_source" FROM source_data`;
    const result = compareSQL(sql, expected);
    expect(result.success).toBe(true);
  });

  it('handles empty input columns gracefully', () => {
    const sql = generateReplicateSQL([], { ...baseConfig, addBranchIdentifier: false });
    const expected = `SELECT  FROM source_data`; // Edge case, but function should produce valid SQL
    const result = compareSQL(sql, expected);
    expect(result.success).toBe(true);
  });

  it('handles input column names that need quoting', () => {
    const columnsWithSpecialChars: SimpleColumn[] = [
      { name: 'first-name', type: 'VARCHAR' },
      { name: 'last name', type: 'VARCHAR' },
      { name: 'select', type: 'VARCHAR' }, // reserved keyword
    ];
    const sql = generateReplicateSQL(columnsWithSpecialChars, baseConfig);
    const expected = `SELECT "first-name", "last name", "select" FROM source_data`;
    const result = compareSQL(sql, expected);
    expect(result.success).toBe(true);
  });

  it('fails comparison and returns diff when SQL mismatches', () => {
    const actual = 'SELECT id FROM users';
    const expected = 'SELECT user_id FROM users';
    const result = compareSQL(actual, expected);
    expect(result.success).toBe(false);
    expect(result.diff).toContain('expected "SELECT user_id FROM users"');
    expect(result.diff).toContain('actual   "SELECT id FROM users"');
  });
});