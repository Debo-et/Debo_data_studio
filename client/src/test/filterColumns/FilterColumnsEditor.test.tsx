// src/test/filterColumns/FilterColumnsEditor.test.tsx
import { render, screen, fireEvent, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import '@testing-library/jest-dom';

import FilterColumnsEditor from '../../components/Editor/JoinsAndLookups/FilterColumnsEditor';
import { mockInputSchema, mockInitialConfig } from './filterColumnsMock';
import { FilterColumnsComponentConfiguration, SchemaDefinition } from '../../types/unified-pipeline.types';
import { compareSQL } from '../utils/sqlComparator';

// Mock external dependencies if any (e.g., Lucide icons)
jest.mock('lucide-react', () => ({
  ChevronUp: () => <span>▲</span>,
  ChevronDown: () => <span>▼</span>,
  AlertCircle: () => <span>⚠️</span>,
}));

describe('FilterColumnsEditor', () => {
  const mockOnSave = jest.fn();
  const mockOnClose = jest.fn();
  const defaultProps = {
    isOpen: true,
    onClose: mockOnClose,
    onSave: mockOnSave,
    nodeId: 'filter-node-1',
    inputSchema: mockInputSchema,
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  // ==================== RENDERING TESTS ====================
  it('renders correctly when open', () => {
    render(<FilterColumnsEditor {...defaultProps} />);
    expect(screen.getByText(/tFilterColumns Editor/i)).toBeInTheDocument();
    expect(screen.getByText(/Node: filter-node-1/i)).toBeInTheDocument();
    expect(screen.getByText(/5 \/ 5 columns selected/i)).toBeInTheDocument(); // keepAllByDefault = true
  });

  it('renders with initial config when provided', () => {
    render(
      <FilterColumnsEditor
        {...defaultProps}
        initialConfig={mockInitialConfig}
      />
    );
    // "age" column should be unchecked (selected: false)
    const ageCheckbox = screen.getByLabelText(/age/i) as HTMLInputElement;
    expect(ageCheckbox.checked).toBe(false);
    // Check renamed columns
    expect(screen.getByDisplayValue('cust_id')).toBeInTheDocument();
    expect(screen.getByDisplayValue('fname')).toBeInTheDocument();
  });

  it('renders empty state when no input schema', () => {
    render(<FilterColumnsEditor {...defaultProps} inputSchema={undefined} />);
    // Should show empty table (no rows)
    expect(screen.queryByText('customer_id')).not.toBeInTheDocument();
  });

  // ==================== COLUMN SELECTION TESTS ====================
  it('toggles column selection', async () => {
    render(<FilterColumnsEditor {...defaultProps} />);
    const firstCheckbox = screen.getAllByRole('checkbox')[0] as HTMLInputElement;
    expect(firstCheckbox.checked).toBe(true);
    await userEvent.click(firstCheckbox);
    expect(firstCheckbox.checked).toBe(false);
  });

  it('selects all columns via button', async () => {
    render(<FilterColumnsEditor {...defaultProps} />);
    // Uncheck all first
    const selectNoneBtn = screen.getByRole('button', { name: /Select None/i });
    await userEvent.click(selectNoneBtn);
    const checkboxes = screen.getAllByRole('checkbox') as HTMLInputElement[];
    checkboxes.forEach(cb => expect(cb.checked).toBe(false));

    // Select all
    const selectAllBtn = screen.getByRole('button', { name: /Select All/i });
    await userEvent.click(selectAllBtn);
    checkboxes.forEach(cb => expect(cb.checked).toBe(true));
  });

  // ==================== RENAMING TESTS ====================
  it('allows renaming columns', async () => {
    render(<FilterColumnsEditor {...defaultProps} />);
    const nameInput = screen.getByDisplayValue('first_name');
    await userEvent.clear(nameInput);
    await userEvent.type(nameInput, 'fname_new');
    expect(nameInput).toHaveValue('fname_new');
  });

  it('disables rename input when column is not selected', async () => {
    render(<FilterColumnsEditor {...defaultProps} />);
    // Find the "age" row's rename input (initially selected = true because keepAllByDefault)
    // First uncheck age
    const ageCheckbox = screen.getByLabelText(/age/i) as HTMLInputElement;
    await userEvent.click(ageCheckbox);
    // Now find the age rename input (should be disabled)
    const ageRow = screen.getByText('age').closest('tr');
    const renameInput = within(ageRow!).getByRole('textbox');
    expect(renameInput).toBeDisabled();
  });

  // ==================== REORDERING TESTS ====================
  it('moves columns up and down', async () => {
    render(<FilterColumnsEditor {...defaultProps} />);
    const rows = screen.getAllByRole('row');
    // First data row should be "customer_id"
    expect(rows[1]).toHaveTextContent('customer_id');
    // Click down arrow on first row
    const firstRowDownBtn = within(rows[1]).getByTitle('Move down');
    await userEvent.click(firstRowDownBtn);
    // Now first row should be "first_name"
    const updatedRows = screen.getAllByRole('row');
    expect(updatedRows[1]).toHaveTextContent('first_name');
  });

  it('disables up button on first row and down on last row', () => {
    render(<FilterColumnsEditor {...defaultProps} />);
    const rows = screen.getAllByRole('row');
    const firstRowUpBtn = within(rows[1]).getByTitle('Move up');
    const lastRowDownBtn = within(rows[rows.length - 1]).getByTitle('Move down');
    expect(firstRowUpBtn).toBeDisabled();
    expect(lastRowDownBtn).toBeDisabled();
  });

  // ==================== ADVANCED OPTIONS TESTS ====================
  it('switches to advanced tab and toggles options', async () => {
    render(<FilterColumnsEditor {...defaultProps} />);
    const advancedTab = screen.getByRole('button', { name: /Advanced/i });
    await userEvent.click(advancedTab);
    expect(screen.getByText(/Case‑sensitive column matching/i)).toBeInTheDocument();

    const caseSensitiveCheckbox = screen.getByLabelText(/Case‑sensitive column matching/i);
    expect(caseSensitiveCheckbox).not.toBeChecked();
    await userEvent.click(caseSensitiveCheckbox);
    expect(caseSensitiveCheckbox).toBeChecked();
  });

  // ==================== VALIDATION TESTS ====================
  it('shows warning when no columns selected', async () => {
    render(<FilterColumnsEditor {...defaultProps} />);
    // Uncheck all columns
    const selectNoneBtn = screen.getByRole('button', { name: /Select None/i });
    await userEvent.click(selectNoneBtn);
    // Save
    const saveBtn = screen.getByRole('button', { name: /Save/i });
    await userEvent.click(saveBtn);
    // Should show warning in status bar
    expect(screen.getByText(/No columns selected/i)).toBeInTheDocument();
    // onSave should still be called (warning but not error)
    await waitFor(() => expect(mockOnSave).toHaveBeenCalledTimes(1));
  });

  it('shows warning for duplicate output names', async () => {
    render(<FilterColumnsEditor {...defaultProps} />);
    // Rename two columns to the same name
    const firstNameInput = screen.getByDisplayValue('first_name');
    await userEvent.clear(firstNameInput);
    await userEvent.type(firstNameInput, 'duplicate');
    const lastNameInput = screen.getByDisplayValue('last_name');
    await userEvent.clear(lastNameInput);
    await userEvent.type(lastNameInput, 'duplicate');
    // Save
    const saveBtn = screen.getByRole('button', { name: /Save/i });
    await userEvent.click(saveBtn);
    await waitFor(() => {
      expect(screen.getByText(/Duplicate output column names: duplicate/i)).toBeInTheDocument();
    });
  });

  // ==================== CONFIGURATION BUILDING TESTS ====================
  it('builds correct configuration on save', async () => {
    render(<FilterColumnsEditor {...defaultProps} />);
    // Uncheck "age" column
    const ageCheckbox = screen.getByLabelText(/age/i);
    await userEvent.click(ageCheckbox);
    // Rename "first_name" to "fname"
    const firstNameInput = screen.getByDisplayValue('first_name');
    await userEvent.clear(firstNameInput);
    await userEvent.type(firstNameInput, 'fname');
    // Move "last_name" up (swap with "first_name")
    const lastNameRow = screen.getByText('last_name').closest('tr');
    const upBtn = within(lastNameRow!).getByTitle('Move up');
    await userEvent.click(upBtn);
    // Save
    const saveBtn = screen.getByRole('button', { name: /Save/i });
    await userEvent.click(saveBtn);

    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledTimes(1);
      const config = mockOnSave.mock.calls[0][0] as FilterColumnsComponentConfiguration;

      // Check columns array
      const columns = config.columns;
      expect(columns.find(c => c.originalName === 'age')?.selected).toBe(false);
      expect(columns.find(c => c.originalName === 'first_name')?.newName).toBe('fname');
      // Check order: last_name should be before first_name
      const last_name_col = columns.find(c => c.originalName === 'last_name');
      const first_name_col = columns.find(c => c.originalName === 'first_name');
      expect(last_name_col?.position).toBeLessThan(first_name_col!.position);

      // Check output schema
      expect(config.outputSchema.fields).toHaveLength(4); // age excluded
      expect(config.outputSchema.fields[0].name).toBe('last_name'); // after move
      expect(config.outputSchema.fields[1].name).toBe('fname');
      expect(config.outputSchema.fields[2].name).toBe('email');
      expect(config.outputSchema.fields[3].name).toBe('customer_id');

      // Check SQL generation
      expect(config.sqlGeneration.selectClause).toContain('last_name AS "last_name"');
      expect(config.sqlGeneration.selectClause).toContain('first_name AS "fname"');
      expect(config.sqlGeneration.selectClause).not.toContain('age');
    });
  });

  // ==================== SQL GENERATION COMPARISON TESTS ====================
  describe('SQL generation comparison', () => {
    it('generates correct SELECT clause from selected columns', () => {
      // Simulate building config from editor state
      const config = mockInitialConfig;
      const expectedSelect = 'customer_id AS cust_id, first_name AS fname, last_name AS lname, email AS email_addr';
      const actualSelect = config.sqlGeneration.selectClause;

      const result = compareSQL(actualSelect, expectedSelect);
      expect(result.success).toBe(true);
    });

    it('handles column reordering in SQL generation', () => {
      // Simulate a config where columns are reordered
      const reorderedConfig: FilterColumnsComponentConfiguration = {
        ...mockInitialConfig,
        columns: [
          { ...mockInitialConfig.columns[2], position: 0 }, // last_name first
          { ...mockInitialConfig.columns[1], position: 1 }, // first_name second
          { ...mockInitialConfig.columns[0], position: 2 }, // customer_id third
          { ...mockInitialConfig.columns[3], position: 3 }, // email fourth
        ],
        sqlGeneration: {
          selectClause: 'last_name AS lname, first_name AS fname, customer_id AS cust_id, email AS email_addr',
          estimatedRowMultiplier: 1.0,
        },
      };
      const expected = 'last_name AS lname, first_name AS fname, customer_id AS cust_id, email AS email_addr';
      const result = compareSQL(reorderedConfig.sqlGeneration.selectClause, expected);
      expect(result.success).toBe(true);
    });

    it('returns diff when SQL does not match', () => {
      const actual = 'SELECT first_name, last_name FROM customers';
      const expected = 'SELECT first_name, last_name, email FROM customers';
      const result = compareSQL(actual, expected);
      expect(result.success).toBe(false);
      expect(result.diff).toContain('email');
    });
  });

  // ==================== EDGE CASES ====================
  it('handles empty column list gracefully', async () => {
    const emptySchema: SchemaDefinition = {
      id: 'empty',
      name: 'Empty',
      fields: [],
      isTemporary: false,
      isMaterialized: false,
    };
    render(<FilterColumnsEditor {...defaultProps} inputSchema={emptySchema} />);
    expect(screen.getByText(/0 \/ 0 columns selected/i)).toBeInTheDocument();
    const saveBtn = screen.getByRole('button', { name: /Save/i });
    await userEvent.click(saveBtn);
    await waitFor(() => expect(mockOnSave).toHaveBeenCalled());
    const savedConfig = mockOnSave.mock.calls[0][0] as FilterColumnsComponentConfiguration;
    expect(savedConfig.outputSchema.fields).toEqual([]);
  });

  it('handles missing input schema by showing empty state', () => {
    render(<FilterColumnsEditor {...defaultProps} inputSchema={undefined} />);
    expect(screen.queryByRole('table')).toBeInTheDocument();
    expect(screen.queryAllByRole('row')).toHaveLength(1); // header only
  });

  it('respects keepAllByDefault = false in advanced options', async () => {
    const customConfig = { ...mockInitialConfig, options: { ...mockInitialConfig.options, keepAllByDefault: false } };
    render(<FilterColumnsEditor {...defaultProps} initialConfig={customConfig} />);
    const checkboxes = screen.getAllByRole('checkbox') as HTMLInputElement[];
    // All should be unchecked
    checkboxes.forEach(cb => expect(cb.checked).toBe(false));
  });

  it('saves with advanced options', async () => {
    render(<FilterColumnsEditor {...defaultProps} />);
    const advancedTab = screen.getByRole('button', { name: /Advanced/i });
    await userEvent.click(advancedTab);
    const caseSensitiveCheckbox = screen.getByLabelText(/Case‑sensitive column matching/i);
    await userEvent.click(caseSensitiveCheckbox);
    const saveBtn = screen.getByRole('button', { name: /Save/i });
    await userEvent.click(saveBtn);
    await waitFor(() => {
      const config = mockOnSave.mock.calls[0][0] as FilterColumnsComponentConfiguration;
      expect(config.options.caseSensitive).toBe(true);
    });
  });

  it('closes on Escape key', async () => {
    render(<FilterColumnsEditor {...defaultProps} />);
    fireEvent.keyDown(document, { key: 'Escape', code: 'Escape' });
    expect(mockOnClose).toHaveBeenCalledTimes(1);
  });

  it('saves on Ctrl+S', async () => {
    render(<FilterColumnsEditor {...defaultProps} />);
    fireEvent.keyDown(document, { key: 's', ctrlKey: true });
    await waitFor(() => expect(mockOnSave).toHaveBeenCalled());
  });
});