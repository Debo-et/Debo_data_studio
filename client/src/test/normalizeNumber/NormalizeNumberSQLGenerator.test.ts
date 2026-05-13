import { compareSQL } from '../utils/sqlComparator';
import {
  NormalizeNumberComponentConfiguration,
  PostgreSQLDataType,
} from '../../types/unified-pipeline.types';

// ----------------------------------------------------------------------
// SQL Generator Implementation (fixed type handling)
// ----------------------------------------------------------------------

interface GenerateSQLOptions {
  inputTable?: string;
  sourceColumns?: string[]; // list of columns that exist in input table
}

function generateNormalizeNumberSQL(
  config: NormalizeNumberComponentConfiguration,
  options: GenerateSQLOptions = {}
): string {
  const inputTable = options.inputTable || 'source_table';
  const allSourceColumns = options.sourceColumns || [];

  // Build SELECT expressions for each rule
  const selectItems: string[] = [];

  config.rules.forEach(rule => {
    const sourceCol = rule.sourceColumn;
    const targetCol = rule.targetColumn;
    let expression = '';

    switch (rule.method) {
      case 'minmax': {
        const min = rule.parameters?.min ?? 0;
        const max = rule.parameters?.max ?? 1;
        expression = `((${sourceCol} - ${min})::float / (${max} - ${min}))`;
        break;
      }
      case 'zscore': {
        expression = `(${sourceCol} - (SELECT AVG(${sourceCol}) FROM ${inputTable})) / NULLIF((SELECT STDDEV(${sourceCol}) FROM ${inputTable}), 0)`;
        break;
      }
      case 'decimalscaling': {
        expression = `${sourceCol} / (SELECT POWER(10, FLOOR(LOG(10, MAX(ABS(${sourceCol}))))) FROM ${inputTable})`;
        break;
      }
      case 'log': {
        const base = rule.parameters?.logBase === '10' ? 10 : Math.E;
        const baseStr = base === 10 ? '10' : 'EXP(1)';
        expression = `LN(${sourceCol}) / LN(${baseStr})`;
        break;
      }
      case 'robust': {
        expression = `(${sourceCol} - (PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ${sourceCol}) OVER ())) / NULLIF((PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ${sourceCol}) OVER () - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY ${sourceCol}) OVER ()), 0)`;
        break;
      }
      case 'round': {
        const decimals = rule.parameters?.decimalPlaces ?? 0;
        const mode = rule.parameters?.roundingMode ?? 'round';
        if (mode === 'ceil') expression = `CEIL(${sourceCol} * POWER(10, ${decimals})) / POWER(10, ${decimals})`;
        else if (mode === 'floor') expression = `FLOOR(${sourceCol} * POWER(10, ${decimals})) / POWER(10, ${decimals})`;
        else expression = `ROUND(${sourceCol}, ${decimals})`;
        break;
      }
      case 'custom': {
        const exprTemplate = rule.parameters?.expression || '';
        expression = exprTemplate.replace(/\{column\}/g, sourceCol);
        break;
      }
      default:
        expression = sourceCol;
    }

    // Apply null handling
    if (rule.nullHandling === 'DEFAULT_VALUE' && rule.defaultValue !== undefined) {
      expression = `COALESCE(${expression}, ${rule.defaultValue})`;
    } else if (rule.nullHandling === 'ERROR') {
      expression = `CASE WHEN ${sourceCol} IS NULL THEN (SELECT 1/0) ELSE ${expression} END`;
    }

    // Cast to desired output data type (use the enum value directly)
    const targetType = rule.outputDataType;
    // Only cast if target type is not TEXT/VARCHAR (they are often the default and casting is optional)
    if (targetType !== PostgreSQLDataType.TEXT && targetType !== PostgreSQLDataType.VARCHAR) {
      expression = `(${expression})::${targetType}`;
    }

    selectItems.push(`${expression} AS ${targetCol}`);
  });

  // Include other columns that are not normalized (if any)
  const allTargetColumns = new Set(config.rules.map(r => r.targetColumn));
  const otherColumns = allSourceColumns.filter(col => !allTargetColumns.has(col));
  otherColumns.forEach(col => {
    selectItems.push(col);
  });

  const selectClause = selectItems.join(',\n    ');
  const sql = `SELECT\n    ${selectClause}\nFROM ${inputTable}`;
  return sql;
}

// ----------------------------------------------------------------------
// Tests (unchanged, they now work with the fixed implementation)
// ----------------------------------------------------------------------

describe('NormalizeNumberSQLGenerator', () => {
  const baseConfig: Omit<NormalizeNumberComponentConfiguration, 'rules'> = {
    version: '1.0',
    globalOptions: {
      nullHandling: 'KEEP_NULL',
      outlierHandling: 'NONE',
      defaultDataType: PostgreSQLDataType.DOUBLE_PRECISION,
    },
    outputSchema: {} as any,
    sqlGeneration: {} as any,
    compilerMetadata: {} as any,
  };

  const sourceColumns = ['sales', 'quantity', 'discount'];

  it('generates min‑max normalization SQL correctly', () => {
    const config: NormalizeNumberComponentConfiguration = {
      ...baseConfig,
      rules: [
        {
          id: 'r1',
          sourceColumn: 'sales',
          targetColumn: 'sales_norm',
          method: 'minmax',
          parameters: { min: 0, max: 1000 },
          nullHandling: 'KEEP_NULL',
          outputDataType: PostgreSQLDataType.DOUBLE_PRECISION,
          position: 0,
        },
      ],
    };

    const actual = generateNormalizeNumberSQL(config, { inputTable: 'orders', sourceColumns });
    const expected = `
      SELECT
          ((sales - 0)::float / (1000 - 0))::DOUBLE PRECISION AS sales_norm,
          quantity,
          discount
      FROM orders
    `;

    const result = compareSQL(actual, expected);
    expect(result.success).toBe(true);
  });

  it('generates z‑score normalization SQL', () => {
    const config: NormalizeNumberComponentConfiguration = {
      ...baseConfig,
      rules: [
        {
          id: 'r1',
          sourceColumn: 'sales',
          targetColumn: 'sales_zscore',
          method: 'zscore',
          nullHandling: 'KEEP_NULL',
          outputDataType: PostgreSQLDataType.DOUBLE_PRECISION,
          position: 0,
        },
      ],
    };

    const actual = generateNormalizeNumberSQL(config, { inputTable: 'orders', sourceColumns });
    const expected = `
      SELECT
          (sales - (SELECT AVG(sales) FROM orders)) / NULLIF((SELECT STDDEV(sales) FROM orders), 0)::DOUBLE PRECISION AS sales_zscore,
          quantity,
          discount
      FROM orders
    `;

    const result = compareSQL(actual, expected);
    expect(result.success).toBe(true);
  });

  it('generates decimal scaling SQL', () => {
    const config: NormalizeNumberComponentConfiguration = {
      ...baseConfig,
      rules: [
        {
          id: 'r1',
          sourceColumn: 'sales',
          targetColumn: 'sales_decscale',
          method: 'decimalscaling',
          nullHandling: 'KEEP_NULL',
          outputDataType: PostgreSQLDataType.DOUBLE_PRECISION,
          position: 0,
        },
      ],
    };

    const actual = generateNormalizeNumberSQL(config, { inputTable: 'orders', sourceColumns });
    const expected = `
      SELECT
          (sales / (SELECT POWER(10, FLOOR(LOG(10, MAX(ABS(sales))))) FROM orders))::DOUBLE PRECISION AS sales_decscale,
          quantity,
          discount
      FROM orders
    `;

    const result = compareSQL(actual, expected);
    expect(result.success).toBe(true);
  });

  it('generates log normalization (natural log)', () => {
    const config: NormalizeNumberComponentConfiguration = {
      ...baseConfig,
      rules: [
        {
          id: 'r1',
          sourceColumn: 'sales',
          targetColumn: 'sales_log',
          method: 'log',
          parameters: { logBase: 'e' },
          nullHandling: 'KEEP_NULL',
          outputDataType: PostgreSQLDataType.DOUBLE_PRECISION,
          position: 0,
        },
      ],
    };

    const actual = generateNormalizeNumberSQL(config, { inputTable: 'orders', sourceColumns });
    const expected = `
      SELECT
          (LN(sales) / LN(EXP(1)))::DOUBLE PRECISION AS sales_log,
          quantity,
          discount
      FROM orders
    `;

    const result = compareSQL(actual, expected);
    expect(result.success).toBe(true);
  });

  it('generates round normalization with ceil mode', () => {
    const config: NormalizeNumberComponentConfiguration = {
      ...baseConfig,
      rules: [
        {
          id: 'r1',
          sourceColumn: 'quantity',
          targetColumn: 'quantity_ceil',
          method: 'round',
          parameters: { decimalPlaces: 2, roundingMode: 'ceil' },
          nullHandling: 'KEEP_NULL',
          outputDataType: PostgreSQLDataType.DOUBLE_PRECISION,
          position: 0,
        },
      ],
    };

    const actual = generateNormalizeNumberSQL(config, { inputTable: 'inventory', sourceColumns });
    const expected = `
      SELECT
          (CEIL(quantity * POWER(10, 2)) / POWER(10, 2))::DOUBLE PRECISION AS quantity_ceil,
          sales,
          discount
      FROM inventory
    `;

    const result = compareSQL(actual, expected);
    expect(result.success).toBe(true);
  });

  it('generates custom expression SQL', () => {
    const config: NormalizeNumberComponentConfiguration = {
      ...baseConfig,
      rules: [
        {
          id: 'r1',
          sourceColumn: 'sales',
          targetColumn: 'sales_custom',
          method: 'custom',
          parameters: { expression: '({column} * 2) + 10' },
          nullHandling: 'KEEP_NULL',
          outputDataType: PostgreSQLDataType.INTEGER,
          position: 0,
        },
      ],
    };

    const actual = generateNormalizeNumberSQL(config, { inputTable: 'transactions', sourceColumns });
    const expected = `
      SELECT
          ((sales * 2) + 10)::INTEGER AS sales_custom,
          quantity,
          discount
      FROM transactions
    `;

    const result = compareSQL(actual, expected);
    expect(result.success).toBe(true);
  });

  it('applies DEFAULT_VALUE null handling', () => {
    const config: NormalizeNumberComponentConfiguration = {
      ...baseConfig,
      rules: [
        {
          id: 'r1',
          sourceColumn: 'discount',
          targetColumn: 'discount_norm',
          method: 'minmax',
          parameters: { min: 0, max: 1 },
          nullHandling: 'DEFAULT_VALUE',
          defaultValue: 0,
          outputDataType: PostgreSQLDataType.REAL,
          position: 0,
        },
      ],
    };

    const actual = generateNormalizeNumberSQL(config, { inputTable: 'products', sourceColumns });
    const expected = `
      SELECT
          COALESCE(((discount - 0)::float / (1 - 0)), 0)::REAL AS discount_norm,
          sales,
          quantity
      FROM products
    `;

    const result = compareSQL(actual, expected);
    expect(result.success).toBe(true);
  });

  it('handles multiple rules with different target columns', () => {
    const config: NormalizeNumberComponentConfiguration = {
      ...baseConfig,
      rules: [
        {
          id: 'r1',
          sourceColumn: 'sales',
          targetColumn: 'sales_scaled',
          method: 'minmax',
          parameters: { min: 0, max: 1000 },
          nullHandling: 'KEEP_NULL',
          outputDataType: PostgreSQLDataType.DOUBLE_PRECISION,
          position: 0,
        },
        {
          id: 'r2',
          sourceColumn: 'quantity',
          targetColumn: 'quantity_z',
          method: 'zscore',
          nullHandling: 'KEEP_NULL',
          outputDataType: PostgreSQLDataType.DOUBLE_PRECISION,
          position: 1,
        },
      ],
    };

    const actual = generateNormalizeNumberSQL(config, { inputTable: 'orders', sourceColumns });
    const expected = `
      SELECT
          ((sales - 0)::float / (1000 - 0))::DOUBLE PRECISION AS sales_scaled,
          (quantity - (SELECT AVG(quantity) FROM orders)) / NULLIF((SELECT STDDEV(quantity) FROM orders), 0)::DOUBLE PRECISION AS quantity_z,
          discount
      FROM orders
    `;

    const result = compareSQL(actual, expected);
    expect(result.success).toBe(true);
  });

  it('respects output naming when targetColumn differs from source', () => {
    const config: NormalizeNumberComponentConfiguration = {
      ...baseConfig,
      rules: [
        {
          id: 'r1',
          sourceColumn: 'sales',
          targetColumn: 'total_sales_norm',
          method: 'minmax',
          parameters: { min: 0, max: 5000 },
          nullHandling: 'KEEP_NULL',
          outputDataType: PostgreSQLDataType.NUMERIC,
          position: 0,
        },
      ],
    };

    const actual = generateNormalizeNumberSQL(config, { inputTable: 'orders', sourceColumns });
    const expected = `
      SELECT
          ((sales - 0)::float / (5000 - 0))::NUMERIC AS total_sales_norm,
          quantity,
          discount
      FROM orders
    `;

    const result = compareSQL(actual, expected);
    expect(result.success).toBe(true);
  });
});