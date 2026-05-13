// src/test/aggregate/AggregateEditor.test.tsx
import { render, screen, fireEvent } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import '@testing-library/jest-dom';
import { AggregateEditor } from '../../components/Editor/Aggregates/AggregateEditor';
import { AggregateSQLGenerator } from '../../generators/AggregateSQLGenerator';
import { PostgreSQLDataType, AggregationConfig } from '../../types/pipeline-types';
import { compareSQL } from '../utils/sqlComparator';

// Mock framer-motion to avoid animation issues in tests
jest.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: any) => <div {...props}>{children}</div>,
  },
}));

describe('AggregateEditor', () => {
  const mockInputColumns = [
    { name: 'customer_id', type: 'integer', id: 'c1' },
    { name: 'product_name', type: 'string', id: 'c2' },
    { name: 'quantity', type: 'integer', id: 'c3' },
    { name: 'price', type: 'decimal', id: 'c4' },
    { name: 'order_date', type: 'date', id: 'c5' },
  ];

  const mockOnSave = jest.fn();
  const mockOnClose = jest.fn();
  const nodeId = 'test-node';
  const nodeMetadata = { name: 'Aggregate Test' };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  const getSavedConfig = () => {
    expect(mockOnSave).toHaveBeenCalledTimes(1);
    return mockOnSave.mock.calls[0][0];
  };

  const renderEditor = (initialConfig?: any) => {
    render(
      <AggregateEditor
        nodeId={nodeId}
        nodeMetadata={nodeMetadata}
        inputColumns={mockInputColumns}
        initialConfig={initialConfig}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );
  };

  test('renders with default empty state', () => {
    renderEditor();
    expect(screen.getByText(/Aggregate Configuration/i)).toBeInTheDocument();
    expect(screen.getByText(/Group By Columns/i)).toBeInTheDocument();
    expect(screen.getByText(/Aggregations/i)).toBeInTheDocument();
    expect(screen.getByText(/HAVING Conditions/i)).toBeInTheDocument();
    expect(screen.getByText(/Output Schema Preview/i)).toBeInTheDocument();
    expect(screen.getAllByRole('combobox')).toHaveLength(2); // function + field selects
    expect(screen.getByPlaceholderText('Alias')).toBeInTheDocument();
  });

  test('allows toggling group by columns', () => {
    renderEditor();
    const customerIdDiv = screen.getByText('customer_id').closest('div');
    expect(customerIdDiv).toBeInTheDocument();

    fireEvent.click(customerIdDiv!);
    expect(screen.getByText('customer_id')).toBeInTheDocument();

    fireEvent.click(customerIdDiv!);
    expect(screen.queryByText('customer_id')).not.toBeInTheDocument();
  });

  test('adds and removes aggregation rows', () => {
    renderEditor();
    const addBtn = screen.getByRole('button', { name: /Add Aggregation/i });
    fireEvent.click(addBtn);
    const removeBtns = screen.getAllByRole('button', { name: /trash/i });
    expect(removeBtns).toHaveLength(2);
    fireEvent.click(removeBtns[1]);
    expect(screen.getAllByRole('button', { name: /trash/i })).toHaveLength(1);
  });

  test('prevents removing the last aggregation row', () => {
    renderEditor();
    const removeBtn = screen.getByRole('button', { name: /trash/i });
    expect(removeBtn).not.toBeDisabled();
    fireEvent.click(removeBtn);
    expect(screen.getAllByRole('button', { name: /trash/i })).toHaveLength(1);
  });

  test('updates aggregation function, field, alias, and distinct flag', async () => {
    renderEditor();
    const functionSelect = screen.getAllByRole('combobox')[0];
    const fieldSelect = screen.getAllByRole('combobox')[1];
    const aliasInput = screen.getByPlaceholderText('Alias');
    const distinctCheckbox = screen.getByLabelText('Distinct');

    await userEvent.selectOptions(functionSelect, 'SUM');
    expect(functionSelect).toHaveValue('SUM');

    await userEvent.selectOptions(fieldSelect, 'quantity');
    expect(fieldSelect).toHaveValue('quantity');

    await userEvent.type(aliasInput, 'total_qty');
    expect(aliasInput).toHaveValue('total_qty');

    fireEvent.click(distinctCheckbox);
    expect(distinctCheckbox).toBeChecked();
  });

  test('adds and configures HAVING conditions', () => {
    renderEditor();
    // Add group by and aggregate to create output columns
    const customerIdDiv = screen.getByText('customer_id').closest('div');
    fireEvent.click(customerIdDiv!);
    const addAggBtn = screen.getByRole('button', { name: /Add Aggregation/i });
    fireEvent.click(addAggBtn);

    const addHavingBtn = screen.getByRole('button', { name: /Add Condition/i });
    fireEvent.click(addHavingBtn);

    // Now there should be a HAVING row with selects
    const havingFieldSelects = screen.getAllByRole('combobox').filter(
      select => (select as HTMLSelectElement).value === '' && (select as HTMLSelectElement).options.length > 0
    );
    if (havingFieldSelects.length) {
      fireEvent.change(havingFieldSelects[0], { target: { value: 'customer_id' } });
    }
    const operatorSelects = screen.getAllByRole('combobox').filter(
      select => (select as HTMLSelectElement).value === '='
    );
    expect(operatorSelects.length).toBeGreaterThan(0);
  });

  test('shows error when no group by and no aggregations', () => {
    renderEditor();
    const removeBtn = screen.getByRole('button', { name: /trash/i });
    fireEvent.click(removeBtn);
    const saveBtn = screen.getByRole('button', { name: /Save Configuration/i });
    expect(saveBtn).toBeDisabled();
    expect(screen.getByText(/Specify at least one group‑by column or one aggregation/i)).toBeInTheDocument();
  });

  test('shows error when aggregation field is missing', () => {
    renderEditor();
    const saveBtn = screen.getByRole('button', { name: /Save Configuration/i });
    expect(screen.getByText(/field is required/i)).toBeInTheDocument();
    expect(saveBtn).toBeDisabled();
  });

  test('shows warning when alias is missing', () => {
    renderEditor();
    const aliasInput = screen.getByPlaceholderText('Alias');
    expect(aliasInput).toHaveValue('');
    expect(screen.getByText(/using default alias/i)).toBeInTheDocument();

    const fieldSelect = screen.getAllByRole('combobox')[1];
    fireEvent.change(fieldSelect, { target: { value: 'customer_id' } });
    const saveBtn = screen.getByRole('button', { name: /Save Configuration/i });
    expect(saveBtn).not.toBeDisabled();
  });

  test('shows error for duplicate output column names', () => {
    renderEditor();
    const customerIdDiv = screen.getByText('customer_id').closest('div');
    fireEvent.click(customerIdDiv!);
    const addAggBtn = screen.getByRole('button', { name: /Add Aggregation/i });
    fireEvent.click(addAggBtn);

    const fieldSelects = screen.getAllByRole('combobox').filter(
      select => (select as HTMLSelectElement).value === ''
    );
    fieldSelects.forEach(select => fireEvent.change(select, { target: { value: 'customer_id' } }));
    const aliasInputs = screen.getAllByPlaceholderText('Alias');
    aliasInputs.forEach(input => fireEvent.change(input, { target: { value: 'cust_id' } }));

    expect(screen.getByText(/Duplicate output column names: cust_id/i)).toBeInTheDocument();
  });

  test('saves a valid configuration with group by and aggregate', () => {
    renderEditor();
    const customerIdDiv = screen.getByText('customer_id').closest('div');
    fireEvent.click(customerIdDiv!);

    const functionSelect = screen.getAllByRole('combobox')[0];
    const fieldSelect = screen.getAllByRole('combobox')[1];
    const aliasInput = screen.getByPlaceholderText('Alias');
    fireEvent.change(functionSelect, { target: { value: 'SUM' } });
    fireEvent.change(fieldSelect, { target: { value: 'quantity' } });
    fireEvent.change(aliasInput, { target: { value: 'total_qty' } });

    const saveBtn = screen.getByRole('button', { name: /Save Configuration/i });
    fireEvent.click(saveBtn);

    const config = getSavedConfig();
    expect(config.groupByFields).toEqual(['customer_id']);
    expect(config.aggregateFunctions).toHaveLength(1);
    expect(config.aggregateFunctions[0]).toMatchObject({
      function: 'SUM',
      field: 'quantity',
      alias: 'total_qty',
      distinct: false,
    });
    expect(config.outputSchema.fields).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ name: 'customer_id', type: 'INTEGER' }),
        expect.objectContaining({ name: 'total_qty', type: 'DECIMAL' }),
      ])
    );
  });

  test('saves configuration with distinct aggregate', () => {
    renderEditor();
    const functionSelect = screen.getAllByRole('combobox')[0];
    const fieldSelect = screen.getAllByRole('combobox')[1];
    const distinctCheckbox = screen.getByLabelText('Distinct');
    fireEvent.change(functionSelect, { target: { value: 'COUNT' } });
    fireEvent.change(fieldSelect, { target: { value: 'customer_id' } });
    fireEvent.click(distinctCheckbox);

    const saveBtn = screen.getByRole('button', { name: /Save Configuration/i });
    fireEvent.click(saveBtn);

    const config = getSavedConfig();
    expect(config.aggregateFunctions[0].distinct).toBe(true);
    expect(config.aggregateFunctions[0].function).toBe('COUNT');
    expect(config.sqlGeneration.aggregateClause).toContain('DISTINCT');
  });

  test('saves configuration with HAVING conditions', () => {
    renderEditor();
    const customerIdDiv = screen.getByText('customer_id').closest('div');
    fireEvent.click(customerIdDiv!);
    const functionSelect = screen.getAllByRole('combobox')[0];
    const fieldSelect = screen.getAllByRole('combobox')[1];
    fireEvent.change(functionSelect, { target: { value: 'SUM' } });
    fireEvent.change(fieldSelect, { target: { value: 'quantity' } });

    const addHavingBtn = screen.getByRole('button', { name: /Add Condition/i });
    fireEvent.click(addHavingBtn);

    const havingFieldSelect = screen.getAllByRole('combobox').find(
      select => (select as HTMLSelectElement).options.length > 1 && (select as HTMLSelectElement).value === ''
    );
    if (havingFieldSelect) {
      fireEvent.change(havingFieldSelect, { target: { value: 'customer_id' } });
    }
    const operatorSelect = screen.getAllByRole('combobox').find(
      select => (select as HTMLSelectElement).value === '='
    );
    if (operatorSelect) {
      fireEvent.change(operatorSelect, { target: { value: '>' } });
    }
    const valueInput = screen.getByPlaceholderText('Value');
    fireEvent.change(valueInput, { target: { value: '5' } });

    const saveBtn = screen.getByRole('button', { name: /Save Configuration/i });
    fireEvent.click(saveBtn);

    const config = getSavedConfig();
    expect(config.havingConditions).toBeDefined();
    expect(config.havingConditions).toHaveLength(1);
    expect(config.havingConditions![0]).toMatchObject({
      field: 'customer_id',
      operator: '>',
      value: '5',
    });
    expect(config.sqlGeneration.havingClause).toBe('customer_id > 5');
  });

  test('loads existing configuration', () => {
    const initialConfig: any = {
      groupByFields: ['product_name'],
      aggregateFunctions: [
        { id: 'agg1', function: 'AVG', field: 'price', alias: 'avg_price', distinct: false },
        { id: 'agg2', function: 'COUNT', field: '*', alias: 'row_count', distinct: false },
      ],
      havingConditions: [{ id: 'h1', field: 'avg_price', operator: '>=', value: '100' }],
      optimization: {},
      outputSchema: {},
      sqlGeneration: {},
      compilerMetadata: {},
    };
    renderEditor(initialConfig);
    expect(screen.getByText('product_name')).toBeInTheDocument();
    expect(screen.getByDisplayValue('avg_price')).toBeInTheDocument();
    expect(screen.getByDisplayValue('>=')).toBeInTheDocument();
    expect(screen.getByDisplayValue('100')).toBeInTheDocument();
  });
});

// ==================== AGGREGATE SQL GENERATOR TESTS ====================
describe('AggregateSQLGenerator', () => {
  let generator: AggregateSQLGenerator;

  beforeEach(() => {
    generator = new AggregateSQLGenerator({ postgresVersion: '14.0', includeComments: false, formatSQL: false });
  });

  const mockSourceColumns = [
    { name: 'customer_id', dataType: PostgreSQLDataType.INTEGER, isPrimaryKey: false },
    { name: 'product_name', dataType: PostgreSQLDataType.VARCHAR },
    { name: 'quantity', dataType: PostgreSQLDataType.INTEGER },
    { name: 'price', dataType: PostgreSQLDataType.DECIMAL },
    { name: 'order_date', dataType: PostgreSQLDataType.DATE },
  ];

  const createAggConfig = (overrides: Partial<AggregationConfig> = {}): AggregationConfig => ({
    groupBy: [],
    aggregates: [],
    having: undefined,
    ...overrides,
  });

  test('generates simple GROUP BY with COUNT', () => {
    const aggConfig = createAggConfig({
      groupBy: ['customer_id'],
      aggregates: [{ column: 'customer_id', function: 'COUNT', alias: 'num_orders' }],
    });
    const result = generator.generateAggregationSQL(mockSourceColumns, aggConfig);
    const expected = `SELECT customer_id, COUNT(customer_id) AS num_orders FROM source_table GROUP BY customer_id`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  test('generates multiple aggregates and GROUP BY', () => {
    const aggConfig = createAggConfig({
      groupBy: ['product_name'],
      aggregates: [
        { column: 'quantity', function: 'SUM', alias: 'total_qty' },
        { column: 'price', function: 'AVG', alias: 'avg_price' },
        { column: '*', function: 'COUNT', alias: 'row_count' },
      ],
    });
    const result = generator.generateAggregationSQL(mockSourceColumns, aggConfig);
    const expected = `SELECT product_name, SUM(quantity) AS total_qty, AVG(price) AS avg_price, COUNT(*) AS row_count FROM source_table GROUP BY product_name`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  test('generates aggregation with HAVING clause', () => {
    const aggConfig = createAggConfig({
      groupBy: ['customer_id'],
      aggregates: [{ column: 'quantity', function: 'SUM', alias: 'total_qty' }],
      having: 'SUM(quantity) > 100',
    });
    const result = generator.generateAggregationSQL(mockSourceColumns, aggConfig);
    const expected = `SELECT customer_id, SUM(quantity) AS total_qty FROM source_table GROUP BY customer_id HAVING SUM(quantity) > 100`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  test('generates distinct aggregate using global option', () => {
    const aggConfig = createAggConfig({
      groupBy: [],
      aggregates: [{ column: 'customer_id', function: 'COUNT', alias: 'unique_customers' }],
    });
    const result = generator.generateAggregationSQL(mockSourceColumns, aggConfig, { distinctAggregates: true });
    const expected = `SELECT COUNT(DISTINCT customer_id) AS unique_customers FROM source_table`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  test('generates window function aggregation (no GROUP BY reduction)', () => {
    const aggConfig = createAggConfig({
      groupBy: ['customer_id'],
      aggregates: [{ column: 'price', function: 'SUM', alias: 'total_spent' }],
    });
    const result = generator.generateAggregationSQL(mockSourceColumns, aggConfig, { useWindowFunctions: true });
    const expected = `SELECT customer_id, SUM(price) OVER (PARTITION BY customer_id) AS total_spent FROM source_table`;
    const comparison = compareSQL(result.sql, expected);
    expect(comparison.success).toBe(true);
  });

  test('generates materialized aggregation (CTE)', () => {
    const aggConfig = createAggConfig({
      groupBy: ['product_name'],
      aggregates: [{ column: 'quantity', function: 'SUM', alias: 'total_qty' }],
      having: 'SUM(quantity) > 10',
    });
    const result = generator.generateAggregationSQL(mockSourceColumns, aggConfig, { materializeIntermediate: true });
    // Check that the SQL contains the WITH clause (simplified check)
    expect(result.sql).toContain('WITH pre_aggregated AS');
    expect(result.sql).toContain('FROM pre_aggregated');
    expect(result.sql).toContain('HAVING SUM(quantity) > 10');
  });

  test('generates filtered aggregate using FILTER clause (PostgreSQL 9.4+)', () => {
    const filtered = generator.generateFilteredAggregate('price', 'AVG', 'price > 0', 'avg_positive_price');
    const expected = `AVG(price) FILTER (WHERE price > 0) AS avg_positive_price`;
    expect(filtered).toBe(expected);
  });

  test('validates missing GROUP BY column', () => {
    const aggConfig = createAggConfig({
      groupBy: ['nonexistent'],
      aggregates: [{ column: 'quantity', function: 'SUM', alias: 'total' }],
    });
    const result = generator.generateAggregationSQL(mockSourceColumns, aggConfig);
    expect(result.errors).toHaveLength(1);
    expect(result.errors[0].code).toBe('GROUP_BY_COLUMN_NOT_FOUND');
    expect(result.errors[0].message).toContain('nonexistent');
  });

  test('validates missing aggregate column', () => {
    const aggConfig = createAggConfig({
      groupBy: ['customer_id'],
      aggregates: [{ column: 'fake_col', function: 'SUM', alias: 'total' }],
    });
    const result = generator.generateAggregationSQL(mockSourceColumns, aggConfig);
    expect(result.errors).toHaveLength(1);
    expect(result.errors[0].code).toBe('AGGREGATE_COLUMN_NOT_FOUND');
  });

  test('validates HAVING clause with invalid column reference', () => {
    const aggConfig = createAggConfig({
      groupBy: ['customer_id'],
      aggregates: [{ column: 'quantity', function: 'SUM', alias: 'total_qty' }],
      having: 'invalid_col > 10',
    });
    const result = generator.generateAggregationSQL(mockSourceColumns, aggConfig);
    expect(result.errors).toHaveLength(1);
    expect(result.errors[0].code).toBe('INVALID_HAVING_CLAUSE');
    expect(result.errors[0].message).toContain('invalid_col');
  });

  test('generates ROLLUP extension', () => {
    const rollupSql = generator.generateGroupingExtensions(['customer_id', 'product_name'], 'ROLLUP');
    expect(rollupSql).toBe('GROUP BY ROLLUP(customer_id, product_name)');
  });

  test('generates CUBE extension', () => {
    const cubeSql = generator.generateGroupingExtensions(['customer_id', 'product_name'], 'CUBE');
    expect(cubeSql).toBe('GROUP BY CUBE(customer_id, product_name)');
  });

  test('handles fallback when no aggregation config', () => {
    const context = {
      node: { name: 'test_node', metadata: {} } as any,
      indentLevel: 0,
      parameters: new Map(),
      options: { postgresVersion: '14.0', includeComments: false, formatSQL: false, targetDialect: 'POSTGRESQL', useCTEs: false, optimizeForReadability: true, includeExecutionPlan: false, parameterizeValues: true, maxLineLength: 80 },
    };
    // @ts-ignore - accessing protected method for test
    const fragment = generator.generateSelectStatement(context);
    expect(fragment.sql).toBe('SELECT * FROM test_node');
    expect(fragment.warnings).toContain('No aggregation configuration found, using fallback SELECT');
  });
});