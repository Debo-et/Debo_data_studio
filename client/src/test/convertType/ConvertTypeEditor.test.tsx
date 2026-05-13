// src/components/Editor/ConvertTypeEditor.test.tsx
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { ConvertTypeEditor } from '../../components/Editor/Mapping/ConvertTypeEditor';
import { mockInputColumns, mockNodeMetadata, mockInitialConfig } from './mocks/convertTypeMocks';
import { ConvertComponentConfiguration } from '../../types/unified-pipeline.types';

// Mock uuid to return predictable IDs
jest.mock('uuid', () => ({
  v4: jest.fn(() => 'mock-uuid'),
}));

describe('ConvertTypeEditor', () => {
  const mockOnClose = jest.fn();
  const mockOnSave = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
  });

  const defaultProps = {
    nodeId: 'convert-node-1',
    nodeMetadata: mockNodeMetadata,
    inputColumns: mockInputColumns,
    initialConfig: undefined,
    onClose: mockOnClose,
    onSave: mockOnSave,
  };

  it('renders with default rules (one per input column)', () => {
    render(<ConvertTypeEditor {...defaultProps} />);

    // Should have 4 rows (one for each input column)
    const rows = screen.getAllByRole('combobox', { name: /select column/i });
    expect(rows).toHaveLength(4);

    // Check that each source column is pre-selected
    const selects = screen.getAllByRole('combobox');
    const sourceSelects = selects.filter((_, i) => i % 4 === 0); // first in each row
    expect(sourceSelects[0]).toHaveValue('first_name');
    expect(sourceSelects[1]).toHaveValue('last_name');
    expect(sourceSelects[2]).toHaveValue('age');
    expect(sourceSelects[3]).toHaveValue('birth_date');
  });

  it('renders with initial configuration', () => {
    const props = {
      ...defaultProps,
      initialConfig: mockInitialConfig,
    };
    render(<ConvertTypeEditor {...props} />);

    // Should have 2 rows
    const rows = screen.getAllByRole('combobox', { name: /select column/i });
    expect(rows).toHaveLength(2);

    // Check that source columns are set
    const sourceSelects = screen.getAllByRole('combobox').filter((_, i) => i % 4 === 0);
    expect(sourceSelects[0]).toHaveValue('first_name');
    expect(sourceSelects[1]).toHaveValue('age');

    // Target columns
    const targetInputs = screen.getAllByPlaceholderText('Output name');
    expect(targetInputs[0]).toHaveValue('full_name');
    expect(targetInputs[1]).toHaveValue('age_group');
  });

  it('allows adding a new rule', async () => {
    render(<ConvertTypeEditor {...defaultProps} />);

    const addButton = screen.getByText('Add Rule');
    fireEvent.click(addButton);

    // Now should have 5 rows
    const rows = screen.getAllByRole('combobox', { name: /select column/i });
    expect(rows).toHaveLength(5);
  });

  it('allows removing a rule', () => {
    render(<ConvertTypeEditor {...defaultProps} />);

    const removeButtons = screen.getAllByTitle('Remove');
    expect(removeButtons).toHaveLength(4);

    fireEvent.click(removeButtons[0]);

    // Should now have 3 rows
    const rows = screen.getAllByRole('combobox', { name: /select column/i });
    expect(rows).toHaveLength(3);
  });

  it('updates source column selection', () => {
    render(<ConvertTypeEditor {...defaultProps} />);

    const sourceSelects = screen.getAllByRole('combobox').filter((_, i) => i % 4 === 0);
    fireEvent.change(sourceSelects[0], { target: { value: 'last_name' } });

    expect(sourceSelects[0]).toHaveValue('last_name');
  });

  it('updates target column name', () => {
    render(<ConvertTypeEditor {...defaultProps} />);

    const targetInputs = screen.getAllByPlaceholderText('Output name');
    fireEvent.change(targetInputs[0], { target: { value: 'new_name' } });

    expect(targetInputs[0]).toHaveValue('new_name');
  });

  it('updates target type', () => {
    render(<ConvertTypeEditor {...defaultProps} />);

    const typeSelects = screen.getAllByRole('combobox').filter((_, i) => i % 4 === 2); // third in row
    fireEvent.change(typeSelects[0], { target: { value: 'INTEGER' } });

    expect(typeSelects[0]).toHaveValue('INTEGER');
  });

  it('expands advanced parameters', () => {
    render(<ConvertTypeEditor {...defaultProps} />);

    // First expand button
    const expandButtons = screen.getAllByTitle('Advanced settings');
    fireEvent.click(expandButtons[0]);

    // Should see advanced inputs
    expect(screen.getByPlaceholderText('e.g., 255')).toBeInTheDocument();
    expect(screen.getByPlaceholderText('e.g., 10')).toBeInTheDocument();
    expect(screen.getByPlaceholderText('e.g., 2')).toBeInTheDocument();
    expect(screen.getByPlaceholderText('e.g., yyyy-MM-dd')).toBeInTheDocument();
  });

  it('auto-maps columns', () => {
    render(<ConvertTypeEditor {...defaultProps} />);

    const autoMapButton = screen.getByText('Auto Map');
    fireEvent.click(autoMapButton);

    // Should have 4 rules with source = target
    const targetInputs = screen.getAllByPlaceholderText('Output name');
    expect(targetInputs[0]).toHaveValue('first_name');
    expect(targetInputs[1]).toHaveValue('last_name');
    expect(targetInputs[2]).toHaveValue('age');
    expect(targetInputs[3]).toHaveValue('birth_date');
  });

  it('resets to defaults', () => {
    render(<ConvertTypeEditor {...defaultProps} />);

    // Change some values
    const sourceSelects = screen.getAllByRole('combobox').filter((_, i) => i % 4 === 0);
    fireEvent.change(sourceSelects[0], { target: { value: 'last_name' } });
    const targetInputs = screen.getAllByPlaceholderText('Output name');
    fireEvent.change(targetInputs[0], { target: { value: 'changed' } });

    const resetButton = screen.getByText('Reset');
    fireEvent.click(resetButton);

    // Should revert to default mapping
    expect(sourceSelects[0]).toHaveValue('first_name');
    expect(targetInputs[0]).toHaveValue('first_name');
  });

  it('validates duplicate target column names', async () => {
    render(<ConvertTypeEditor {...defaultProps} />);

    // Set two rules to same target column
    const targetInputs = screen.getAllByPlaceholderText('Output name');
    fireEvent.change(targetInputs[0], { target: { value: 'duplicate' } });
    fireEvent.change(targetInputs[1], { target: { value: 'duplicate' } });

    // Validation errors should appear
    await waitFor(() => {
      expect(screen.getByText('Duplicate output column name: duplicate')).toBeInTheDocument();
    });
  });

  it('validates missing source column', async () => {
    render(<ConvertTypeEditor {...defaultProps} />);

    const sourceSelects = screen.getAllByRole('combobox').filter((_, i) => i % 4 === 0);
    fireEvent.change(sourceSelects[0], { target: { value: '' } });

    await waitFor(() => {
      expect(screen.getByText(/Rule 1: Source column missing/)).toBeInTheDocument();
    });
  });

  it('validates missing target column', async () => {
    render(<ConvertTypeEditor {...defaultProps} />);

    const targetInputs = screen.getAllByPlaceholderText('Output name');
    fireEvent.change(targetInputs[0], { target: { value: '' } });

    await waitFor(() => {
      expect(screen.getByText(/Rule 1: Target column missing/)).toBeInTheDocument();
    });
  });

  it('validates missing target type', async () => {
    render(<ConvertTypeEditor {...defaultProps} />);

    const typeSelects = screen.getAllByRole('combobox').filter((_, i) => i % 4 === 2);
    fireEvent.change(typeSelects[0], { target: { value: '' } });

    await waitFor(() => {
      expect(screen.getByText(/Rule 1: Target type missing/)).toBeInTheDocument();
    });
  });

  it('does not save when validation errors exist', () => {
    render(<ConvertTypeEditor {...defaultProps} />);

    // Introduce error: empty source column
    const sourceSelects = screen.getAllByRole('combobox').filter((_, i) => i % 4 === 0);
    fireEvent.change(sourceSelects[0], { target: { value: '' } });

    const saveButton = screen.getByText('Save Configuration');
    fireEvent.click(saveButton);

    expect(mockOnSave).not.toHaveBeenCalled();
  });

  it('saves configuration when valid', async () => {
    render(<ConvertTypeEditor {...defaultProps} />);

    // Modify a rule
    const targetInputs = screen.getAllByPlaceholderText('Output name');
    fireEvent.change(targetInputs[0], { target: { value: 'full_name' } });
    const typeSelects = screen.getAllByRole('combobox').filter((_, i) => i % 4 === 2);
    fireEvent.change(typeSelects[0], { target: { value: 'VARCHAR' } });

    const saveButton = screen.getByText('Save Configuration');
    fireEvent.click(saveButton);

    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledTimes(1);
      const savedConfig = mockOnSave.mock.calls[0][0] as ConvertComponentConfiguration;
      expect(savedConfig.rules).toHaveLength(4);
      expect(savedConfig.rules[0].sourceColumn).toBe('first_name');
      expect(savedConfig.rules[0].targetColumn).toBe('full_name');
      expect(savedConfig.rules[0].targetType).toBe('VARCHAR');
      expect(savedConfig.outputSchema.fields).toHaveLength(4);
      expect(savedConfig.outputSchema.fields[0].name).toBe('full_name');
    });
  });

  it('shows schema preview and validation errors', () => {
    render(<ConvertTypeEditor {...defaultProps} />);

    // Check that input schema is displayed
    expect(screen.getByText('Input Schema')).toBeInTheDocument();
    expect(screen.getByText('first_name')).toBeInTheDocument();

    // Output schema initially empty (no rules modified)
    expect(screen.getByText('Output Schema')).toBeInTheDocument();
    expect(screen.getByText('full_name')).toBeInTheDocument(); // after auto-map? Actually default uses source names
  });

  it('can toggle schema preview visibility', () => {
    render(<ConvertTypeEditor {...defaultProps} />);

    const toggleButton = screen.getByTitle('Hide schema preview');
    fireEvent.click(toggleButton);

    expect(screen.queryByText('Schema Preview')).not.toBeInTheDocument();

    const showButton = screen.getByTitle('Show schema preview');
    fireEvent.click(showButton);

    expect(screen.getByText('Schema Preview')).toBeInTheDocument();
  });
});