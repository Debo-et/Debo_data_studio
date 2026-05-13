// src/test/generators/DenormalizeSQLGenerator.test.ts
import { DenormalizeComponentConfiguration } from '../../types/unified-pipeline.types';
import { compareSQL } from '../utils/sqlComparator';

/**
 * Generate PostgreSQL SQL for tDenormalize (explode delimited string into rows).
 * Returns an object containing the generated SQL and any warnings.
 */
function generateDenormalizeSQL(
  config: DenormalizeComponentConfiguration,
  sourceTable: string,
  inputSchema: Array<{ name: string; type: string }>
): { sql: string; warnings: string[] } {
  const warnings: string[] = [];
  const {
    sourceColumn,
    delimiter,
    trimValues,
    treatEmptyAsNull,
    quoteChar,
    escapeChar,
    outputColumnName,
    addRowNumber,
    rowNumberColumnName = 'row_index',
    keepColumns,
  } = config;

  // Validate source column exists
  if (!inputSchema.some(col => col.name === sourceColumn)) {
    throw new Error(`Source column "${sourceColumn}" not found in input schema`);
  }

  // Build the unnest expression
  let unnestExpr = `unnest(string_to_array(${sourceColumn}, '${delimiter.replace(/'/g, "''")}'))`;
  if (trimValues) {
    unnestExpr = `trim(${unnestExpr})`;
  }
  if (treatEmptyAsNull) {
    unnestExpr = `NULLIF(${unnestExpr}, '')`;
  }
  if (quoteChar) {
    // Simplified: remove surrounding quotes if present
    unnestExpr = `trim(BOTH '${quoteChar.replace(/'/g, "''")}' FROM ${unnestExpr})`;
  }
  // Escape char not easily supported in simple SQL, add warning
  if (escapeChar) {
    warnings.push(`Escape character '${escapeChar}' is not directly supported in PostgreSQL string_to_array; consider preprocessing.`);
  }

  // Build SELECT list: keep columns + denormalized value + optional row number
  const selectParts: string[] = [];
  for (const col of inputSchema) {
    if (keepColumns.includes(col.name)) {
      selectParts.push(col.name);
    }
  }
  selectParts.push(`${unnestExpr} AS ${outputColumnName}`);
  if (addRowNumber) {
    selectParts.push(`row_number() OVER () AS ${rowNumberColumnName}`);
  }

  const selectClause = selectParts.join(',\n  ');
  const fromClause = `${sourceTable}, LATERAL ${unnestExpr.split(' AS ')[0]}`;
  const sql = `SELECT\n  ${selectClause}\nFROM ${fromClause}`;

  return { sql, warnings };
}

describe('DenormalizeSQLGenerator', () => {
  const mockInputSchema = [
    { name: 'id', type: 'integer' },
    { name: 'name', type: 'string' },
    { name: 'tags', type: 'string' },
  ];
  const sourceTable = 'source_data';

  it('generates basic denormalize SQL', () => {
    const config: DenormalizeComponentConfiguration = {
      version: '1.0',
      sourceColumn: 'tags',
      delimiter: ',',
      trimValues: false,
      treatEmptyAsNull: false,
      outputColumnName: 'tag',
      addRowNumber: false,
      keepColumns: ['id', 'name'],
      errorHandling: 'fail',
      batchSize: 1000,
      parallelization: false,
    };

    const { sql } = generateDenormalizeSQL(config, sourceTable, mockInputSchema);
    const expected = `SELECT
  id,
  name,
  unnest(string_to_array(tags, ',')) AS tag
FROM source_data, LATERAL unnest(string_to_array(tags, ','))`;

    const comparison = compareSQL(sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('handles trimValues and treatEmptyAsNull', () => {
    const config: DenormalizeComponentConfiguration = {
      version: '1.0',
      sourceColumn: 'tags',
      delimiter: '|',
      trimValues: true,
      treatEmptyAsNull: true,
      outputColumnName: 'cleaned_tag',
      addRowNumber: false,
      keepColumns: ['id'],
      errorHandling: 'fail',
      batchSize: 1000,
      parallelization: false,
    };

    const { sql } = generateDenormalizeSQL(config, sourceTable, mockInputSchema);
    const expected = `SELECT
  id,
  NULLIF(trim(unnest(string_to_array(tags, '|'))), '') AS cleaned_tag
FROM source_data, LATERAL unnest(string_to_array(tags, '|'))`;

    const comparison = compareSQL(sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('adds row number column when requested', () => {
    const config: DenormalizeComponentConfiguration = {
      version: '1.0',
      sourceColumn: 'tags',
      delimiter: ',',
      trimValues: false,
      treatEmptyAsNull: false,
      outputColumnName: 'tag',
      addRowNumber: true,
      rowNumberColumnName: 'rn',
      keepColumns: ['id', 'name'],
      errorHandling: 'fail',
      batchSize: 1000,
      parallelization: false,
    };

    const { sql } = generateDenormalizeSQL(config, sourceTable, mockInputSchema);
    const expected = `SELECT
  id,
  name,
  unnest(string_to_array(tags, ',')) AS tag,
  row_number() OVER () AS rn
FROM source_data, LATERAL unnest(string_to_array(tags, ','))`;

    const comparison = compareSQL(sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('handles quoteChar by trimming surrounding quotes', () => {
    const config: DenormalizeComponentConfiguration = {
      version: '1.0',
      sourceColumn: 'tags',
      delimiter: ',',
      trimValues: true,
      treatEmptyAsNull: false,
      quoteChar: '"',
      outputColumnName: 'tag',
      addRowNumber: false,
      keepColumns: ['id'],
      errorHandling: 'fail',
      batchSize: 1000,
      parallelization: false,
    };

    const { sql } = generateDenormalizeSQL(config, sourceTable, mockInputSchema);
    const expected = `SELECT
  id,
  trim(BOTH '"' FROM trim(unnest(string_to_array(tags, ',')))) AS tag
FROM source_data, LATERAL unnest(string_to_array(tags, ','))`;

    const comparison = compareSQL(sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('throws error if source column does not exist', () => {
    const config: DenormalizeComponentConfiguration = {
      version: '1.0',
      sourceColumn: 'missing_col',
      delimiter: ',',
      trimValues: false,
      treatEmptyAsNull: false,
      outputColumnName: 'out',
      addRowNumber: false,
      keepColumns: [],
      errorHandling: 'fail',
      batchSize: 1000,
      parallelization: false,
    };

    expect(() => generateDenormalizeSQL(config, sourceTable, mockInputSchema))
      .toThrow('Source column "missing_col" not found in input schema');
  });

  it('generates warning when escapeChar is provided', () => {
    const config: DenormalizeComponentConfiguration = {
      version: '1.0',
      sourceColumn: 'tags',
      delimiter: ',',
      trimValues: false,
      treatEmptyAsNull: false,
      escapeChar: '\\',
      outputColumnName: 'tag',
      addRowNumber: false,
      keepColumns: ['id'],
      errorHandling: 'fail',
      batchSize: 1000,
      parallelization: false,
    };

    const { sql, warnings } = generateDenormalizeSQL(config, sourceTable, mockInputSchema);
    expect(warnings).toContain("Escape character '\\' is not directly supported in PostgreSQL string_to_array; consider preprocessing.");
    // SQL should still generate without escape char logic
    const expected = `SELECT
  id,
  unnest(string_to_array(tags, ',')) AS tag
FROM source_data, LATERAL unnest(string_to_array(tags, ','))`;
    const comparison = compareSQL(sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('handles empty keepColumns (only output denormalized column)', () => {
    const config: DenormalizeComponentConfiguration = {
      version: '1.0',
      sourceColumn: 'tags',
      delimiter: ';',
      trimValues: false,
      treatEmptyAsNull: false,
      outputColumnName: 'value',
      addRowNumber: false,
      keepColumns: [],
      errorHandling: 'fail',
      batchSize: 1000,
      parallelization: false,
    };

    const { sql } = generateDenormalizeSQL(config, sourceTable, mockInputSchema);
    const expected = `SELECT
  unnest(string_to_array(tags, ';')) AS value
FROM source_data, LATERAL unnest(string_to_array(tags, ';'))`;
    const comparison = compareSQL(sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('handles delimiter that needs escaping (e.g., tab, backslash)', () => {
    const config: DenormalizeComponentConfiguration = {
      version: '1.0',
      sourceColumn: 'tags',
      delimiter: '\t',
      trimValues: false,
      treatEmptyAsNull: false,
      outputColumnName: 'tag',
      addRowNumber: false,
      keepColumns: ['id'],
      errorHandling: 'fail',
      batchSize: 1000,
      parallelization: false,
    };

    const { sql } = generateDenormalizeSQL(config, sourceTable, mockInputSchema);
    // The delimiter in string_to_array should be properly escaped (tab becomes \t in string literal)
    const expected = `SELECT
  id,
  unnest(string_to_array(tags, E'\t')) AS tag
FROM source_data, LATERAL unnest(string_to_array(tags, E'\t'))`;
    const comparison = compareSQL(sql, expected);
    expect(comparison.success).toBe(true);
  });
});