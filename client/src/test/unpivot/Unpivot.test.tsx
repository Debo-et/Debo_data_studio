import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';

// Component under test
import UnpivotRowEditor from '../../components/Editor/Aggregates/UnpivotRowEditor';
import { UnpivotSQLGenerator } from '../../generators/UnpivotSQLGenerator';
import { SQLGenerationContext } from '../../generators/BaseSQLGenerator';
import { compareSQL } from '../utils/sqlComparator';

// ==================== MOCKS ====================
jest.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: any) => <div {...props}>{children}</div>,
  },
}));

// Mock node metadata (not directly used by the editor, but required by props)
const mockNodeMetadata = { id: 'test-node', name: 'Test Node' };

// ==================== TESTS ====================

describe('UnpivotRowEditor Component', () => {
  const mockInputColumns = [
    { name: 'id', type: 'INTEGER' },
    { name: 'name', type: 'STRING' },
    { name: 'age', type: 'INTEGER' },
    { name: 'salary', type: 'DECIMAL' },
  ];

  const mockOnSave = jest.fn();
  const mockOnClose = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
  });

  const renderEditor = (initialConfig?: any) => {
    return render(
      <UnpivotRowEditor
        nodeId="node-123"
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        initialConfig={initialConfig}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );
  };

  it('renders correctly with all input columns', () => {
    renderEditor();
    expect(screen.getByText(/Unpivot Row Configuration/i)).toBeInTheDocument();
    expect(screen.getByText('id')).toBeInTheDocument();
    expect(screen.getByText('name')).toBeInTheDocument();
    expect(screen.getByText('age')).toBeInTheDocument();
    expect(screen.getByText('salary')).toBeInTheDocument();
  });

  it('allows toggling a column as Key', async () => {
    renderEditor();
    const idRow = screen.getByText('id').closest('div');
    const keyCheckbox = idRow?.querySelector('input[type="checkbox"][value="on"]');
    expect(keyCheckbox).toBeInTheDocument();
    fireEvent.click(keyCheckbox!);
    expect(keyCheckbox).toBeChecked();
  });

  it('allows toggling a column as Unpivot', async () => {
    renderEditor();
    const ageRow = screen.getByText('age').closest('div');
    const unpivotCheckbox = ageRow?.querySelectorAll('input[type="checkbox"]')[1];
    fireEvent.click(unpivotCheckbox!);
    expect(unpivotCheckbox).toBeChecked();
  });

  it('prevents a column from being both Key and Unpivot', () => {
    renderEditor();
    const nameRow = screen.getByText('name').closest('div');
    const keyCheckbox = nameRow?.querySelectorAll('input[type="checkbox"]')[0];
    const unpivotCheckbox = nameRow?.querySelectorAll('input[type="checkbox"]')[1];

    fireEvent.click(keyCheckbox!);
    expect(keyCheckbox).toBeChecked();
    expect(unpivotCheckbox).not.toBeChecked();

    fireEvent.click(unpivotCheckbox!);
    expect(unpivotCheckbox).toBeChecked();
    expect(keyCheckbox).not.toBeChecked();
  });

  it('selects all columns as Unpivot via "All Unpivot" button', () => {
    renderEditor();
    const allUnpivotBtn = screen.getByText('All Unpivot');
    fireEvent.click(allUnpivotBtn);
    const checkboxes = screen.getAllByLabelText('Unpivot');
    checkboxes.forEach(cb => expect(cb).toBeChecked());
    const keyCheckboxes = screen.getAllByLabelText('Key');
    keyCheckboxes.forEach(cb => expect(cb).not.toBeChecked());
  });

  it('clears all selections via "Clear" button', () => {
    renderEditor();
    // First select some
    const allUnpivotBtn = screen.getByText('All Unpivot');
    fireEvent.click(allUnpivotBtn);
    const clearBtn = screen.getByText('Clear');
    fireEvent.click(clearBtn);
    const checkboxes = screen.getAllByLabelText('Unpivot');
    checkboxes.forEach(cb => expect(cb).not.toBeChecked());
    const keyCheckboxes = screen.getAllByLabelText('Key');
    keyCheckboxes.forEach(cb => expect(cb).not.toBeChecked());
  });

  it('updates output column name fields', () => {
    renderEditor();
    const columnNameInput = screen.getByPlaceholderText('e.g. attribute');
    const valueInput = screen.getByPlaceholderText('e.g. value');

    fireEvent.change(columnNameInput, { target: { value: 'my_attribute' } });
    fireEvent.change(valueInput, { target: { value: 'my_value' } });

    expect(columnNameInput).toHaveValue('my_attribute');
    expect(valueInput).toHaveValue('my_value');
  });

  it('disables save button when validation fails (no keys or no unpivot columns)', () => {
    renderEditor();
    const saveBtn = screen.getByText('Save Configuration');
    expect(saveBtn).toBeDisabled();

    // Add a key column
    const idRow = screen.getByText('id').closest('div');
    const keyCheckbox = idRow?.querySelector('input[type="checkbox"][value="on"]');
    fireEvent.click(keyCheckbox!);
    // Still missing unpivot columns
    expect(saveBtn).toBeDisabled();

    // Add an unpivot column
    const ageRow = screen.getByText('age').closest('div');
    const unpivotCheckbox = ageRow?.querySelectorAll('input[type="checkbox"]')[1];
    fireEvent.click(unpivotCheckbox!);
    expect(saveBtn).toBeEnabled();
  });

  it('calls onSave with correct configuration when saved', async () => {
    renderEditor();

    // Select key column 'id' and unpivot column 'age'
    const idRow = screen.getByText('id').closest('div');
    const idKeyCheckbox = idRow?.querySelector('input[type="checkbox"][value="on"]');
    fireEvent.click(idKeyCheckbox!);

    const ageRow = screen.getByText('age').closest('div');
    const ageUnpivotCheckbox = ageRow?.querySelectorAll('input[type="checkbox"]')[1];
    fireEvent.click(ageUnpivotCheckbox!);

    // Set output column names
    const columnNameInput = screen.getByPlaceholderText('e.g. attribute');
    const valueInput = screen.getByPlaceholderText('e.g. value');
    fireEvent.change(columnNameInput, { target: { value: 'attr' } });
    fireEvent.change(valueInput, { target: { value: 'val' } });

    // Select null handling
    const nullSelect = screen.getByLabelText('Null Handling');
    fireEvent.change(nullSelect, { target: { value: 'INCLUDE' } });

    const saveBtn = screen.getByText('Save Configuration');
    fireEvent.click(saveBtn);

    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledTimes(1);
      const savedConfig = mockOnSave.mock.calls[0][0];
      expect(savedConfig.keyColumns).toEqual(['id']);
      expect(savedConfig.unpivotColumns).toEqual(['age']);
      expect(savedConfig.columnNameColumn).toBe('attr');
      expect(savedConfig.valueColumn).toBe('val');
      expect(savedConfig.nullHandling).toBe('INCLUDE');
      expect(savedConfig.outputSchema).toBeDefined();
      expect(savedConfig.outputSchema.fields).toHaveLength(3); // id, attr, val
    });
  });

  it('restores initial configuration when provided', () => {
    const initialConfig = {
      version: '1.0',
      keyColumns: ['id'],
      unpivotColumns: ['name', 'age'],
      columnNameColumn: 'original_column',
      valueColumn: 'original_value',
      valueDataType: 'STRING',
      nullHandling: 'EXCLUDE',
      outputSchema: {} as any,
      sqlGeneration: {} as any,
      compilerMetadata: {} as any,
    };
    renderEditor(initialConfig);

    // Check checkboxes
    const idRow = screen.getByText('id').closest('div');
    const idKeyCheckbox = idRow?.querySelector('input[type="checkbox"][value="on"]');
    expect(idKeyCheckbox).toBeChecked();

    const nameRow = screen.getByText('name').closest('div');
    const nameUnpivotCheckbox = nameRow?.querySelectorAll('input[type="checkbox"]')[1];
    expect(nameUnpivotCheckbox).toBeChecked();

    const ageRow = screen.getByText('age').closest('div');
    const ageUnpivotCheckbox = ageRow?.querySelectorAll('input[type="checkbox"]')[1];
    expect(ageUnpivotCheckbox).toBeChecked();

    // Output column fields
    expect(screen.getByPlaceholderText('e.g. attribute')).toHaveValue('original_column');
    expect(screen.getByPlaceholderText('e.g. value')).toHaveValue('original_value');
    expect(screen.getByLabelText('Null Handling')).toHaveValue('EXCLUDE');
  });
});

describe('UnpivotSQLGenerator', () => {
  let generator: UnpivotSQLGenerator;

  beforeEach(() => {
    generator = new UnpivotSQLGenerator({ postgresVersion: '14.0', includeComments: false, formatSQL: false });
  });

  // Helper to build a full SQLGenerationContext with a node containing the Unpivot configuration
  const createContext = (unpivotConfig: any) => {
    const node = {
      id: 'unpivot-1',
      name: 'Unpivot',
      type: 'UNPIVOT_ROW' as any,
      position: { x: 0, y: 0 },
      size: { width: 100, height: 100 },
      metadata: {
        configuration: {
          type: 'UNPIVOT_ROW',
          config: unpivotConfig,
        },
      },
    } as any;
    return {
      node,
      options: {
        includeComments: false,
        formatSQL: false,
        targetDialect: 'POSTGRESQL',
        postgresVersion: '14.0',
        useCTEs: true,
        optimizeForReadability: true,
        includeExecutionPlan: false,
        parameterizeValues: false,
        maxLineLength: 80,
      },
      indentLevel: 0,
      parameters: new Map(),
    } as SQLGenerationContext;
  };

  const expectedUnpivotSQL = (keyCols: string[], unpivotCols: string[], keyColName: string, valueColName: string) => {
    const fixedCols = keyCols.join(', ');
    const unions = unpivotCols.map(col =>
      `SELECT ${fixedCols ? fixedCols + ', ' : ''}'${col}' AS ${keyColName}, ${col} AS ${valueColName} FROM source_table`
    ).join('\nUNION ALL\n');
    return unions;
  };

  it('generates correct SQL for single unpivot column with one key column', () => {
    const config = {
      columnsToUnpivot: ['age'],
      keyColumnName: 'attribute',
      valueColumnName: 'value',
      excludeColumns: ['id'],
    };
    const context = createContext(config);
    const fragment = generator.generateSQL(context); // ✅ public method
    const expected = expectedUnpivotSQL(['id'], ['age'], 'attribute', 'value');
    const comparison = compareSQL(fragment.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('generates correct SQL for multiple unpivot columns with multiple key columns', () => {
    const config = {
      columnsToUnpivot: ['age', 'salary'],
      keyColumnName: 'metric',
      valueColumnName: 'amount',
      excludeColumns: ['id', 'name'],
    };
    const context = createContext(config);
    const fragment = generator.generateSQL(context);
    const expected = expectedUnpivotSQL(['id', 'name'], ['age', 'salary'], 'metric', 'amount');
    const comparison = compareSQL(fragment.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('handles no key columns (empty exclude list)', () => {
    const config = {
      columnsToUnpivot: ['age', 'salary'],
      keyColumnName: 'metric',
      valueColumnName: 'amount',
      excludeColumns: [],
    };
    const context = createContext(config);
    const fragment = generator.generateSQL(context);
    const expected = expectedUnpivotSQL([], ['age', 'salary'], 'metric', 'amount');
    const comparison = compareSQL(fragment.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('returns fallback SELECT * when configuration is missing', () => {
    const context = createContext(null as any); // no config
    const fragment = generator.generateSQL(context);
    const expected = 'SELECT * FROM unpivot';
    const comparison = compareSQL(fragment.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('returns fallback SELECT * when columnsToUnpivot is empty', () => {
    const config = {
      columnsToUnpivot: [],
      keyColumnName: 'attr',
      valueColumnName: 'val',
      excludeColumns: ['id'],
    };
    const context = createContext(config);
    const fragment = generator.generateSQL(context);
    const expected = 'SELECT * FROM unpivot';
    const comparison = compareSQL(fragment.sql, expected);
    expect(comparison.success).toBe(true);
  });

  it('properly escapes column names with spaces or reserved words', () => {
    const config = {
      columnsToUnpivot: ['first name', 'last-name'],
      keyColumnName: 'column name',
      valueColumnName: 'value',
      excludeColumns: ['user id'],
    };
    const context = createContext(config);
    const fragment = generator.generateSQL(context);
    // The generator uses sanitizeIdentifier, which double‑quotes problematic identifiers.
    const expected = `SELECT "user id", 'first name' AS "column name", "first name" AS value FROM source_table\nUNION ALL\nSELECT "user id", 'last-name' AS "column name", "last-name" AS value FROM source_table`;
    const comparison = compareSQL(fragment.sql, expected);
    expect(comparison.success).toBe(true);
  });
});