import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { ParseRecordSetEditor } from '../../components/Editor/Parsing/ParseRecordSetEditor';
import { ParseRecordSetComponentConfiguration } from '../../types/unified-pipeline.types';

// Mock the global fetch for import-from-header preview (if needed)
global.fetch = jest.fn();

describe('ParseRecordSetEditor', () => {
  const mockNodeId = 'test-node-123';
  const mockNodeMetadata = { name: 'Parse Test Node' };
  const mockInputColumns = [
    { name: 'raw_data', type: 'STRING', id: 'col1' },
    { name: 'other_field', type: 'INTEGER', id: 'col2' },
  ];
  const mockOnClose = jest.fn();
  const mockOnSave = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
  });

  const renderEditor = (initialConfig?: ParseRecordSetComponentConfiguration) => {
    return render(
      <ParseRecordSetEditor
        nodeId={mockNodeId}
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        initialConfig={initialConfig}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );
  };

  it('renders basic tab with source column selector and delimiter options', () => {
    renderEditor();
    expect(screen.getByText(/Source Column/i)).toBeInTheDocument();
    expect(screen.getByText('raw_data')).toBeInTheDocument();
    expect(screen.getByLabelText(/Newline/)).toBeInTheDocument();
    expect(screen.getByLabelText(/Comma/)).toBeInTheDocument();
  });

  it('allows switching between tabs', () => {
    renderEditor();
    fireEvent.click(screen.getByText(/Columns/i));
    expect(screen.getByText(/Add Column/i)).toBeInTheDocument();
    fireEvent.click(screen.getByText(/Advanced/i));
    expect(screen.getByText(/Trim whitespace/i)).toBeInTheDocument();
    fireEvent.click(screen.getByText(/Preview/i));
    expect(screen.getByText(/Refresh Preview/i)).toBeInTheDocument();
  });

  it('adds a new column and updates column table', () => {
    renderEditor();
    fireEvent.click(screen.getByText(/Columns/i));
    fireEvent.click(screen.getByText(/Add Column/i));
    expect(screen.getByDisplayValue('column_1')).toBeInTheDocument();
    const deleteButtons = screen.getAllByText('✕');
    expect(deleteButtons.length).toBe(1);
  });

  it('edits column name and data type', async () => {
    renderEditor();
    fireEvent.click(screen.getByText(/Columns/i));
    fireEvent.click(screen.getByText(/Add Column/i));
    const nameInput = screen.getByDisplayValue('column_1');
    await userEvent.clear(nameInput);
    await userEvent.type(nameInput, 'user_id');
    expect(nameInput).toHaveValue('user_id');
    const typeSelect = screen.getByRole('combobox');
    fireEvent.change(typeSelect, { target: { value: 'INTEGER' } });
    expect(typeSelect).toHaveValue('INTEGER');
  });

  it('deletes a column', () => {
    renderEditor();
    fireEvent.click(screen.getByText(/Columns/i));
    fireEvent.click(screen.getByText(/Add Column/i));
    expect(screen.getByDisplayValue('column_1')).toBeInTheDocument();
    const deleteButton = screen.getByText('✕');
    fireEvent.click(deleteButton);
    expect(screen.queryByDisplayValue('column_1')).not.toBeInTheDocument();
    expect(screen.getByText(/No columns defined/)).toBeInTheDocument();
  });

  it('imports columns from header when "Import from Header" is clicked', async () => {
    // Mock fetch to return a sample CSV header row
    (global.fetch as jest.Mock).mockResolvedValueOnce({
      ok: true,
      json: async () => ({ filePath: '/mock/path.csv' }),
    });
    // Mock the splitFields behaviour indirectly by providing sample data
    // The actual import uses a hardcoded sample; we'll override the internal call via spy
    // For simplicity, we simulate the effect by setting columns directly after import.
    // Since the import function is async and uses real XLSX? Actually it uses a hardcoded sample.
    // We'll trust the component's logic and just test that columns are added.
    renderEditor();
    fireEvent.click(screen.getByText(/Columns/i));
    // Select source column first
    const sourceSelect = screen.getByRole('combobox', { name: /Source Column/i });
    fireEvent.change(sourceSelect, { target: { value: 'raw_data' } });
    // Click import
    const importBtn = screen.getByText(/Import from Header/i);
    fireEvent.click(importBtn);
    // Wait for async operation (the component uses a simulated sample)
    await waitFor(() => {
      // After import, columns should be populated (at least one)
      expect(screen.getByDisplayValue(/name/)).toBeInTheDocument();
    });
  });

  it('validates that source column is selected before saving', () => {
    renderEditor();
    const saveBtn = screen.getByText(/Save/i);
    fireEvent.click(saveBtn);
    expect(window.alert).toHaveBeenCalledWith(expect.stringContaining('source column'));
  });

  it('validates that at least one column is defined before saving', () => {
    renderEditor();
    const sourceSelect = screen.getByRole('combobox', { name: /Source Column/i });
    fireEvent.change(sourceSelect, { target: { value: 'raw_data' } });
    const saveBtn = screen.getByText(/Save/i);
    fireEvent.click(saveBtn);
    expect(window.alert).toHaveBeenCalledWith(expect.stringContaining('at least one output column'));
  });

  it('calls onSave with correct configuration when valid', () => {
    renderEditor();
    // Select source column
    const sourceSelect = screen.getByRole('combobox', { name: /Source Column/i });
    fireEvent.change(sourceSelect, { target: { value: 'raw_data' } });
    // Add a column
    fireEvent.click(screen.getByText(/Columns/i));
    fireEvent.click(screen.getByText(/Add Column/i));
    // Save
    const saveBtn = screen.getByText(/Save/i);
    fireEvent.click(saveBtn);
    expect(mockOnSave).toHaveBeenCalledTimes(1);
    const savedConfig = mockOnSave.mock.calls[0][0] as ParseRecordSetComponentConfiguration;
    expect(savedConfig.sourceColumn).toBe('raw_data');
    expect(savedConfig.columns).toHaveLength(1);
    expect(savedConfig.recordDelimiter).toBe('\n');
    expect(savedConfig.fieldDelimiter).toBe(',');
    expect(savedConfig.outputSchema.fields).toHaveLength(1);
  });

  it('prefills form with initialConfig when provided', () => {
    const initial: ParseRecordSetComponentConfiguration = {
      version: '1.0',
      sourceColumn: 'other_field',
      recordDelimiter: '|',
      fieldDelimiter: '\t',
      quoteChar: '"',
      escapeChar: '\\',
      hasHeader: false,
      columns: [
        {
          id: 'col1',
          name: 'predefined_col',
          type: 'STRING',
          length: 100,
          nullable: false,
          position: 0,
          fieldIndex: 1,
        },
      ],
      trimWhitespace: false,
      nullIfEmpty: false,
      errorHandling: 'skipRow',
      parallelization: false,
      batchSize: 500,
      outputSchema: {} as any,
      compilerMetadata: {} as any,
    };
    renderEditor(initial);
    expect(screen.getByRole('combobox', { name: /Source Column/i })).toHaveValue('other_field');
    expect(screen.getByDisplayValue('predefined_col')).toBeInTheDocument();
    expect(screen.getByLabelText(/Custom record delimiter/i)).toBeChecked();
    expect(screen.getByDisplayValue('|')).toBeInTheDocument();
    expect(screen.getByDisplayValue('\t')).toBeInTheDocument();
    expect(screen.getByDisplayValue('"')).toBeInTheDocument();
    expect(screen.getByDisplayValue('\\')).toBeInTheDocument();
    expect(screen.getByRole('checkbox', { name: /Trim whitespace/i })).not.toBeChecked();
  });
});