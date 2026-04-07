import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import '@testing-library/jest-dom';
import UniqRowEditor from '../../components/Editor/Aggregates/UniqRowEditor';
import { UniqRowComponentConfiguration } from '../../types/unified-pipeline.types';

// Mock input columns
const mockInputColumns = [
  { name: 'id', type: 'integer', id: 'col1' },
  { name: 'first_name', type: 'string', id: 'col2' },
  { name: 'last_name', type: 'string', id: 'col3' },
  { name: 'age', type: 'integer', id: 'col4' },
];

// Mock configuration
const mockInitialConfig: UniqRowComponentConfiguration = {
  version: '1.0',
  keyFields: ['first_name', 'last_name'],
  keepStrategy: 'FIRST',
  treatNullsAsEqual: true,
  sortFields: [{ field: 'id', direction: 'ASC' }],
  outputDuplicateCount: true,
  duplicateCountColumnName: 'dup_cnt',
  compilerMetadata: {
    lastModified: '2025-01-01T00:00:00Z',
    createdBy: 'test',
    validationStatus: 'VALID',
    warnings: [],
    dependencies: [],
  },
};

describe('UniqRowEditor', () => {
  const mockOnSave = jest.fn();
  const mockOnClose = jest.fn();
  const mockNodeMetadata = { id: 'node1', name: 'Test Uniq Row' };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('renders correctly with default state (no initial config)', () => {
    render(
      <UniqRowEditor
        nodeId="node1"
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    expect(screen.getByText(/tUniqRow Configuration/i)).toBeInTheDocument();
    // Key fields checkboxes should all be unchecked initially
    mockInputColumns.forEach(col => {
      const checkbox = screen.getByLabelText(col.name);
      expect(checkbox).not.toBeChecked();
    });
    // Keep strategy default should be 'First occurrence'
    expect(screen.getByLabelText('First occurrence')).toBeChecked();
    expect(screen.getByLabelText('Last occurrence')).not.toBeChecked();
    // Treat nulls as equal should be checked by default
    expect(screen.getByLabelText('Treat null values as equal')).toBeChecked();
    // Advanced options hidden initially
    expect(screen.queryByText('Sort order')).not.toBeInTheDocument();
  });

  it('loads initial configuration when provided', () => {
    render(
      <UniqRowEditor
        nodeId="node1"
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        initialConfig={mockInitialConfig}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    // Key fields should be checked
    expect(screen.getByLabelText('first_name')).toBeChecked();
    expect(screen.getByLabelText('last_name')).toBeChecked();
    expect(screen.getByLabelText('id')).not.toBeChecked();

    // Keep strategy
    expect(screen.getByLabelText('First occurrence')).toBeChecked();
    expect(screen.getByLabelText('Last occurrence')).not.toBeChecked();

    // Nulls as equal
    expect(screen.getByLabelText('Treat null values as equal')).toBeChecked();

    // Advanced options should be visible because sortFields exist
    const advancedToggle = screen.getByText('Show Advanced');
    fireEvent.click(advancedToggle);
    expect(screen.getByDisplayValue('id')).toBeInTheDocument();
    expect(screen.getByDisplayValue('ASC')).toBeInTheDocument();

    // Duplicate count column
    expect(screen.getByLabelText('Output duplicate count column')).toBeChecked();
    expect(screen.getByDisplayValue('dup_cnt')).toBeInTheDocument();
  });

  it('allows selecting/deselecting key fields', async () => {
    render(
      <UniqRowEditor
        nodeId="node1"
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    const firstNameCheckbox = screen.getByLabelText('first_name');
    const lastNameCheckbox = screen.getByLabelText('last_name');

    await userEvent.click(firstNameCheckbox);
    await userEvent.click(lastNameCheckbox);

    expect(firstNameCheckbox).toBeChecked();
    expect(lastNameCheckbox).toBeChecked();
  });

  it('switches keep strategy', async () => {
    render(
      <UniqRowEditor
        nodeId="node1"
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    const lastRadio = screen.getByLabelText('Last occurrence');
    await userEvent.click(lastRadio);
    expect(lastRadio).toBeChecked();
    expect(screen.getByLabelText('First occurrence')).not.toBeChecked();
  });

  it('toggles treat nulls as equal', async () => {
    render(
      <UniqRowEditor
        nodeId="node1"
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    const checkbox = screen.getByLabelText('Treat null values as equal');
    expect(checkbox).toBeChecked();
    await userEvent.click(checkbox);
    expect(checkbox).not.toBeChecked();
  });

  it('shows advanced options and allows adding sort fields', async () => {
    render(
      <UniqRowEditor
        nodeId="node1"
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    const advancedToggle = screen.getByText('Show Advanced');
    await userEvent.click(advancedToggle);

    expect(screen.getByText('Sort order (to determine first/last)')).toBeInTheDocument();

    const addSortBtn = screen.getByText('Add sort field');
    await userEvent.click(addSortBtn);

    // Should have one sort field row with default first column
    expect(screen.getByDisplayValue(mockInputColumns[0].name)).toBeInTheDocument();
    expect(screen.getByDisplayValue('ASC')).toBeInTheDocument();

    // Add a second sort field
    await userEvent.click(addSortBtn);
    const sortSelects = screen.getAllByRole('combobox', { name: '' });
    expect(sortSelects.length).toBe(2);
  });

  it('allows removing sort fields', async () => {
    render(
      <UniqRowEditor
        nodeId="node1"
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        initialConfig={mockInitialConfig} // includes one sort field
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    const advancedToggle = screen.getByText('Show Advanced');
    await userEvent.click(advancedToggle);

    const removeButton = screen.getByRole('button', { name: /trash/i });
    await userEvent.click(removeButton);
    expect(screen.queryByDisplayValue('id')).not.toBeInTheDocument();
  });

  it('enables duplicate count column and changes column name', async () => {
    render(
      <UniqRowEditor
        nodeId="node1"
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    const advancedToggle = screen.getByText('Show Advanced');
    await userEvent.click(advancedToggle);

    const dupCheckbox = screen.getByLabelText('Output duplicate count column');
    await userEvent.click(dupCheckbox);

    const columnNameInput = screen.getByPlaceholderText('duplicate_count');
    expect(columnNameInput).toBeInTheDocument();

    await userEvent.clear(columnNameInput);
    await userEvent.type(columnNameInput, 'my_dups');
    expect(columnNameInput).toHaveValue('my_dups');
  });

  it('displays validation error when saving with no key fields', async () => {
    render(
      <UniqRowEditor
        nodeId="node1"
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    const saveBtn = screen.getByText('Save Configuration');
    await userEvent.click(saveBtn);

    expect(await screen.findByText(/At least one key field must be selected/i)).toBeInTheDocument();
    expect(mockOnSave).not.toHaveBeenCalled();
  });

  it('saves configuration with correct payload', async () => {
    render(
      <UniqRowEditor
        nodeId="node1"
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    // Select key fields
    await userEvent.click(screen.getByLabelText('first_name'));
    await userEvent.click(screen.getByLabelText('age'));

    // Choose keep strategy = LAST
    await userEvent.click(screen.getByLabelText('Last occurrence'));

    // Uncheck treat nulls as equal
    await userEvent.click(screen.getByLabelText('Treat null values as equal'));

    // Advanced: add sort field
    const advancedToggle = screen.getByText('Show Advanced');
    await userEvent.click(advancedToggle);
    await userEvent.click(screen.getByText('Add sort field'));
    const sortSelect = screen.getByDisplayValue(mockInputColumns[0].name);
    await userEvent.selectOptions(sortSelect, 'age');
    const dirSelect = screen.getByDisplayValue('ASC');
    await userEvent.selectOptions(dirSelect, 'DESC');

    // Output duplicate count
    await userEvent.click(screen.getByLabelText('Output duplicate count column'));
    const dupNameInput = screen.getByPlaceholderText('duplicate_count');
    await userEvent.type(dupNameInput, '_cnt');

    const saveBtn = screen.getByText('Save Configuration');
    await userEvent.click(saveBtn);

    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledTimes(1);
      const savedConfig = mockOnSave.mock.calls[0][0] as UniqRowComponentConfiguration;
      expect(savedConfig.keyFields).toEqual(['first_name', 'age']);
      expect(savedConfig.keepStrategy).toBe('LAST');
      expect(savedConfig.treatNullsAsEqual).toBe(false);
      expect(savedConfig.sortFields).toEqual([{ field: 'age', direction: 'DESC' }]);
      expect(savedConfig.outputDuplicateCount).toBe(true);
      expect(savedConfig.duplicateCountColumnName).toBe('duplicate_count_cnt');
      expect(savedConfig.version).toBe('1.0');
    });
  });

  it('closes modal when Cancel is clicked', async () => {
    render(
      <UniqRowEditor
        nodeId="node1"
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    const cancelBtn = screen.getByText('Cancel');
    await userEvent.click(cancelBtn);
    expect(mockOnClose).toHaveBeenCalledTimes(1);
    expect(mockOnSave).not.toHaveBeenCalled();
  });
});