// src/components/Editor/Mapping/__tests__/NormalizeEditor.test.tsx
import { fireEvent, screen, waitFor } from '@testing-library/react';
import { customRender, mockInputColumns } from './test-utils';
import NormalizeEditor from '../../components/Editor/Mapping/NormalizeEditor';
import { NormalizeComponentConfiguration } from '../../types/unified-pipeline.types';

jest.mock('lucide-react', () => ({
  ChevronDown: () => <div data-testid="chevron-down" />,
  ChevronUp: () => <div data-testid="chevron-up" />,
  X: () => <div data-testid="x" />,
}));

describe('NormalizeEditor', () => {
  const mockOnSave = jest.fn();
  const mockOnClose = jest.fn();
  const nodeId = 'test-node-1';
  const nodeMetadata = { id: nodeId, name: 'Normalize Node', type: 'NORMALIZE' } as any;

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('renders correctly with input columns', () => {
    customRender(
      <NormalizeEditor
        nodeId={nodeId}
        nodeMetadata={nodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    expect(screen.getByText(/tNormalize Configuration/i)).toBeInTheDocument();
    expect(screen.getByText('Source Column')).toBeInTheDocument();
    expect(screen.getByText('Delimiter')).toBeInTheDocument();
    expect(screen.getByText('Output Column Name')).toBeInTheDocument();

    // Check input schema checkboxes appear
    mockInputColumns.forEach(col => {
      expect(screen.getByLabelText(col.name)).toBeInTheDocument();
    });
  });

  it('validates required fields before saving', async () => {
    customRender(
      <NormalizeEditor
        nodeId={nodeId}
        nodeMetadata={nodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    const saveBtn = screen.getByText('Save Configuration');
    fireEvent.click(saveBtn);

    await waitFor(() => {
      expect(screen.getByText('Source column is required.')).toBeInTheDocument();
      expect(screen.getByText('Delimiter cannot be empty.')).toBeInTheDocument();
      expect(screen.getByText('Output column name is required.')).toBeInTheDocument();
    });
    expect(mockOnSave).not.toHaveBeenCalled();
  });

  it('allows selecting source column and setting delimiter', async () => {
    customRender(
      <NormalizeEditor
        nodeId={nodeId}
        nodeMetadata={nodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    // Select source column
    const sourceSelect = screen.getByLabelText(/Source Column/i);
    fireEvent.change(sourceSelect, { target: { value: 'tags' } });
    expect(sourceSelect).toHaveValue('tags');

    // Delimiter input
    const delimiterInput = screen.getByPlaceholderText('e.g., , | \t');
    fireEvent.change(delimiterInput, { target: { value: '|' } });
    expect(delimiterInput).toHaveValue('|');

    // Output column name auto-populates? Default behavior sets to source column name if empty.
    const outputInput = screen.getByPlaceholderText('e.g., normalized_value');
    expect(outputInput).toHaveValue('tags'); // because source column changed
  });

  it('toggles keep columns checkboxes correctly', () => {
    customRender(
      <NormalizeEditor
        nodeId={nodeId}
        nodeMetadata={nodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    // By default all columns except source? source not selected yet so all checked initially
    const idCheckbox = screen.getByLabelText('id');
    const nameCheckbox = screen.getByLabelText('name');
    const tagsCheckbox = screen.getByLabelText('tags');
    const deptCheckbox = screen.getByLabelText('department');

    expect(idCheckbox).toBeChecked();
    expect(nameCheckbox).toBeChecked();
    expect(tagsCheckbox).toBeChecked();
    expect(deptCheckbox).toBeChecked();

    // Uncheck one
    fireEvent.click(nameCheckbox);
    expect(nameCheckbox).not.toBeChecked();

    // Selecting source column should disable its checkbox and auto-uncheck
    const sourceSelect = screen.getByLabelText(/Source Column/i);
    fireEvent.change(sourceSelect, { target: { value: 'tags' } });
    expect(tagsCheckbox).toBeDisabled();
    expect(tagsCheckbox).not.toBeChecked(); // removed from keep columns
  });

  it('saves configuration with valid inputs', async () => {
    customRender(
      <NormalizeEditor
        nodeId={nodeId}
        nodeMetadata={nodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    // Fill required fields
    fireEvent.change(screen.getByLabelText(/Source Column/i), { target: { value: 'tags' } });
    fireEvent.change(screen.getByPlaceholderText('e.g., , | \t'), { target: { value: ',' } });
    fireEvent.change(screen.getByPlaceholderText('e.g., normalized_value'), { target: { value: 'tag' } });

    // Keep only id and department
    fireEvent.click(screen.getByLabelText('name')); // uncheck
    fireEvent.click(screen.getByLabelText('tags')); // should be disabled? it's source, but it's already disabled, so it's fine

    // Advanced: add row number
    const addRowNumberCheck = screen.getByLabelText('Add row number column');
    fireEvent.click(addRowNumberCheck);
    const rowNumberInput = screen.getByPlaceholderText('e.g., row_index');
    fireEvent.change(rowNumberInput, { target: { value: 'row_num' } });

    // Save
    fireEvent.click(screen.getByText('Save Configuration'));

    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledTimes(1);
      const config: NormalizeComponentConfiguration = mockOnSave.mock.calls[0][0];
      expect(config.sourceColumn).toBe('tags');
      expect(config.delimiter).toBe(',');
      expect(config.outputColumnName).toBe('tag');
      expect(config.keepColumns).toEqual(expect.arrayContaining(['id', 'department']));
      expect(config.keepColumns).not.toContain('name');
      expect(config.keepColumns).not.toContain('tags');
      expect(config.addRowNumber).toBe(true);
      expect(config.rowNumberColumnName).toBe('row_num');
      expect(config.trimValues).toBe(true); // default
      expect(config.treatEmptyAsNull).toBe(false);
      expect(config.errorHandling).toBe('fail');
      expect(config.batchSize).toBe(1000);
      expect(config.parallelization).toBe(false);
    });
  });

  it('preview shows normalized rows based on configuration', async () => {
    // We need to mock the sample data generation inside the component.
    // Since the component uses createSampleData internally, we'll rely on actual logic.
    // To test preview, we set source column and delimiter and check the preview table.

    customRender(
      <NormalizeEditor
        nodeId={nodeId}
        nodeMetadata={nodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    fireEvent.change(screen.getByLabelText(/Source Column/i), { target: { value: 'tags' } });
    fireEvent.change(screen.getByPlaceholderText('e.g., , | \t'), { target: { value: ',' } });
    fireEvent.change(screen.getByPlaceholderText('e.g., normalized_value'), { target: { value: 'tag' } });

    // Preview should appear
    await waitFor(() => {
      // Look for table headers: keep columns (id, name, department) + output column + optional row number
      expect(screen.getByText('id')).toBeInTheDocument();
      expect(screen.getByText('name')).toBeInTheDocument();
      expect(screen.getByText('department')).toBeInTheDocument();
      expect(screen.getByText('tag')).toBeInTheDocument();
      // Expect rows: each tag value becomes a row
      // Alice has 'a','b','c' -> 3 rows, Bob 2 rows, Charlie 4 rows
      // total 9 rows, but preview shows first 10, so all 9.
      const rows = screen.getAllByRole('row');
      // Header row + 9 data rows = 10 rows
      expect(rows.length).toBe(10);
    });
  });

  it('handles edge case: empty source column selection', () => {
    customRender(
      <NormalizeEditor
        nodeId={nodeId}
        nodeMetadata={nodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );
    const saveBtn = screen.getByText('Save Configuration');
    fireEvent.click(saveBtn);
    expect(screen.getByText('Source column is required.')).toBeInTheDocument();
  });

  it('handles edge case: delimiter empty', () => {
    customRender(
      <NormalizeEditor
        nodeId={nodeId}
        nodeMetadata={nodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );
    fireEvent.change(screen.getByLabelText(/Source Column/i), { target: { value: 'tags' } });
    const delimiterInput = screen.getByPlaceholderText('e.g., , | \t');
    fireEvent.change(delimiterInput, { target: { value: '' } });
    fireEvent.click(screen.getByText('Save Configuration'));
    expect(screen.getByText('Delimiter cannot be empty.')).toBeInTheDocument();
  });

  it('handles addRowNumber but no rowNumberColumnName', async () => {
    customRender(
      <NormalizeEditor
        nodeId={nodeId}
        nodeMetadata={nodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );
    fireEvent.change(screen.getByLabelText(/Source Column/i), { target: { value: 'tags' } });
    fireEvent.change(screen.getByPlaceholderText('e.g., , | \t'), { target: { value: ',' } });
    fireEvent.change(screen.getByPlaceholderText('e.g., normalized_value'), { target: { value: 'tag' } });
    fireEvent.click(screen.getByLabelText('Add row number column'));
    // leave rowNumberColumnName empty
    fireEvent.click(screen.getByText('Save Configuration'));
    await waitFor(() => {
      expect(screen.getByText('Row number column name is required.')).toBeInTheDocument();
    });
  });

  it('handles advanced options: error handling, batch size, parallelization', () => {
    customRender(
      <NormalizeEditor
        nodeId={nodeId}
        nodeMetadata={nodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );
    // Open advanced
    fireEvent.click(screen.getByText('Advanced Options'));
    expect(screen.getByLabelText('Error Handling')).toBeInTheDocument();
    expect(screen.getByLabelText('Batch Size')).toBeInTheDocument();
    expect(screen.getByLabelText('Enable parallel processing')).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText('Error Handling'), { target: { value: 'skip' } });
    fireEvent.change(screen.getByLabelText('Batch Size'), { target: { value: '500' } });
    fireEvent.click(screen.getByLabelText('Enable parallel processing'));

    // Save and verify
    fireEvent.change(screen.getByLabelText(/Source Column/i), { target: { value: 'tags' } });
    fireEvent.change(screen.getByPlaceholderText('e.g., , | \t'), { target: { value: ',' } });
    fireEvent.change(screen.getByPlaceholderText('e.g., normalized_value'), { target: { value: 'tag' } });
    fireEvent.click(screen.getByText('Save Configuration'));

    expect(mockOnSave).toHaveBeenCalledWith(expect.objectContaining({
      errorHandling: 'skip',
      batchSize: 500,
      parallelization: true,
    }));
  });
});