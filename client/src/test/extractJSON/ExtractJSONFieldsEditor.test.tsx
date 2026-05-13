// src/test/components/ExtractJSONFieldsEditor.test.tsx

import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import { ExtractJSONFieldsEditor } from '../../components/Editor/Parsing/ExtractJSONFieldsEditor';
import { ExtractJSONFieldsConfiguration } from '../../types/unified-pipeline.types';

// Mock SimpleColumn input
const mockInputColumns = [
  { id: 'col1', name: 'json_payload', type: 'jsonb' },
  { id: 'col2', name: 'other_column', type: 'text' },
];

describe('ExtractJSONFieldsEditor', () => {
  const mockOnSave = jest.fn();
  const mockOnClose = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('renders correctly with input columns', () => {
    render(
      <ExtractJSONFieldsEditor
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    expect(screen.getByText(/Extract JSON Fields/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Source Column/i)).toBeInTheDocument();
    expect(screen.getByText('json_payload')).toBeInTheDocument();
    expect(screen.getByText('other_column')).toBeInTheDocument();
    expect(screen.getByText(/Add Column/i)).toBeInTheDocument();
  });

  it('allows adding and removing output columns', () => {
    render(
      <ExtractJSONFieldsEditor
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    // Initially no columns
    expect(screen.queryByPlaceholderText(/column_name/)).not.toBeInTheDocument();

    // Add first column
    fireEvent.click(screen.getByText(/Add Column/i));
    expect(screen.getAllByPlaceholderText(/column_name/)).toHaveLength(1);

    // Add second column
    fireEvent.click(screen.getByText(/Add Column/i));
    expect(screen.getAllByPlaceholderText(/column_name/)).toHaveLength(2);

    // Remove first column (click trash icon)
    const trashButtons = screen.getAllByRole('button', { name: /remove column/i });
    fireEvent.click(trashButtons[0]);
    expect(screen.getAllByPlaceholderText(/column_name/)).toHaveLength(1);
  });

  it('validates required fields and shows warnings', async () => {
    render(
      <ExtractJSONFieldsEditor
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    // Try to save without any columns
    fireEvent.click(screen.getByText(/Save & Compile/i));
    await waitFor(() => {
      expect(screen.getByText(/At least one output column must be defined/i)).toBeInTheDocument();
    });
    expect(mockOnSave).not.toHaveBeenCalled();

    // Add a column but leave name empty
    fireEvent.click(screen.getByText(/Add Column/i));
    const nameInput = screen.getByPlaceholderText(/column_name/);
    fireEvent.change(nameInput, { target: { value: '' } });

    const jsonPathInput = screen.getByPlaceholderText(/\$\.field/);
    fireEvent.change(jsonPathInput, { target: { value: '$.id' } });

    fireEvent.click(screen.getByText(/Save & Compile/i));
    await waitFor(() => {
      expect(screen.getByText(/Output column .* has no name/i)).toBeInTheDocument();
    });
    expect(mockOnSave).not.toHaveBeenCalled();

    // Fill name and path
    fireEvent.change(nameInput, { target: { value: 'user_id' } });
    fireEvent.click(screen.getByText(/Save & Compile/i));
    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledTimes(1);
    });
  });

  it('saves configuration with correct data types and options', async () => {
    render(
      <ExtractJSONFieldsEditor
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    // Add a column
    fireEvent.click(screen.getByText(/Add Column/i));
    const nameInput = screen.getByPlaceholderText(/column_name/);
    fireEvent.change(nameInput, { target: { value: 'customer_id' } });

    const jsonPathInput = screen.getByPlaceholderText(/\$\.field/);
    fireEvent.change(jsonPathInput, { target: { value: '$.customer.id' } });

    // Change type to INTEGER
    const typeSelect = screen.getByRole('combobox', { name: /type/i });
    fireEvent.change(typeSelect, { target: { value: 'INTEGER' } });

    // Check nullable checkbox (default is checked)
    const nullableCheckbox = screen.getByRole('checkbox', { name: /nullable/i });
    expect(nullableCheckbox).toBeChecked();

    // Add default value
    const defaultValueInput = screen.getByPlaceholderText(/NULL/);
    fireEvent.change(defaultValueInput, { target: { value: '-1' } });

    // Open advanced options and set error handling
    fireEvent.click(screen.getByText(/Advanced Options/i));
    const errorHandlingSelect = screen.getByLabelText(/Error Handling/i);
    fireEvent.change(errorHandlingSelect, { target: { value: 'setNull' } });

    // Disable parallelization
    const parallelCheckbox = screen.getByLabelText(/Enable parallelization/i);
    fireEvent.click(parallelCheckbox);

    // Save
    fireEvent.click(screen.getByText(/Save & Compile/i));

    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledTimes(1);
      const config = mockOnSave.mock.calls[0][0] as ExtractJSONFieldsConfiguration;

      expect(config.sourceColumn).toBe('json_payload');
      expect(config.jsonType).toBeUndefined(); // default not set in UI? Actually our editor doesn't set jsonType, it's optional.
      expect(config.outputColumns).toHaveLength(1);
      const col = config.outputColumns[0];
      expect(col.name).toBe('customer_id');
      expect(col.jsonPath).toBe('$.customer.id');
      expect(col.type).toBe('INTEGER');
      expect(col.nullable).toBe(true);
      expect(col.defaultValue).toBe('-1');
      expect(config.errorHandling).toBe('setNull');
      expect(config.parallelization).toBe(false);
      expect(config.batchSize).toBeUndefined();
    });
  });

  it('pre-populates from initialConfig', () => {
    const initialConfig: ExtractJSONFieldsConfiguration = {
      version: '1.0',
      sourceColumn: 'other_column',
      outputColumns: [
        {
          id: 'pre1',
          name: 'pre_name',
          jsonPath: '$.name',
          type: 'STRING',
          nullable: false,
          defaultValue: 'anonymous',
        },
      ],
      errorHandling: 'skip',
      parallelization: true,
      batchSize: 500,
    };

    render(
      <ExtractJSONFieldsEditor
        inputColumns={mockInputColumns}
        initialConfig={initialConfig}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    expect(screen.getByDisplayValue('other_column')).toBeInTheDocument();
    expect(screen.getByDisplayValue('pre_name')).toBeInTheDocument();
    expect(screen.getByDisplayValue('$.name')).toBeInTheDocument();
    expect(screen.getByDisplayValue('anonymous')).toBeInTheDocument();

    const typeSelect = screen.getByDisplayValue('STRING');
    expect(typeSelect).toBeInTheDocument();

    const nullableCheckbox = screen.getByRole('checkbox', { name: /nullable/i });
    expect(nullableCheckbox).not.toBeChecked();

    // Advanced options
    fireEvent.click(screen.getByText(/Advanced Options/i));
    const errorSelect = screen.getByLabelText(/Error Handling/i);
    expect(errorSelect).toHaveValue('skip');

    const parallelCheckbox = screen.getByLabelText(/Enable parallelization/i);
    expect(parallelCheckbox).toBeChecked();

    const batchInput = screen.getByDisplayValue('500');
    expect(batchInput).toBeInTheDocument();
  });

  it('handles missing input columns gracefully', () => {
    render(
      <ExtractJSONFieldsEditor
        inputColumns={[]}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );
    expect(screen.getByText(/No input columns available/i)).toBeInTheDocument();
    const sourceSelect = screen.getByLabelText(/Source Column/i);
    expect(sourceSelect).toHaveValue('');
  });

  it('allows editing JSONPath and data type per column', async () => {
    render(
      <ExtractJSONFieldsEditor
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    fireEvent.click(screen.getByText(/Add Column/i));
    const nameInput = screen.getByPlaceholderText(/column_name/);
    fireEvent.change(nameInput, { target: { value: 'price' } });

    const jsonPathInput = screen.getByPlaceholderText(/\$\.field/);
    fireEvent.change(jsonPathInput, { target: { value: '$.price' } });

    const typeSelect = screen.getByRole('combobox', { name: /type/i });
    fireEvent.change(typeSelect, { target: { value: 'DECIMAL' } });

    // Verify that the change is reflected in the saved config
    fireEvent.click(screen.getByText(/Save & Compile/i));
    await waitFor(() => {
      const config = mockOnSave.mock.calls[0][0];
      expect(config.outputColumns[0].type).toBe('DECIMAL');
      expect(config.outputColumns[0].jsonPath).toBe('$.price');
    });
  });
});