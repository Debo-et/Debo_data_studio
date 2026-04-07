// src/test/pivot/PivotToColumnsDelimitedEditor.test.tsx
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import '@testing-library/jest-dom';
import { PivotToColumnsDelimitedEditor } from '../../components/Editor/Aggregates/PivotToColumnsDelimitedEditor';
import { PivotToColumnsDelimitedConfiguration } from '../../components/Editor/Aggregates/PivotToColumnsDelimitedEditor';

const mockInputColumns = [
  { name: 'id', type: 'integer', id: 'col1' },
  { name: 'data', type: 'string', id: 'col2' },
  { name: 'extra', type: 'string', id: 'col3' },
];

const mockOnSave = jest.fn();
const mockOnClose = jest.fn();

describe('PivotToColumnsDelimitedEditor', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('renders correctly with input columns', () => {
    render(
      <PivotToColumnsDelimitedEditor
        nodeId="node1"
        nodeMetadata={{} as any}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    expect(screen.getByText('Pivot to Columns (Delimited)')).toBeInTheDocument();
    expect(screen.getByLabelText('Source Column *')).toBeInTheDocument();
    expect(screen.getByDisplayValue('data')).toBeInTheDocument(); // first column is default
    expect(screen.getByLabelText('Pair Delimiter')).toHaveValue(',');
    expect(screen.getByLabelText('Key‑Value Separator')).toHaveValue(':');
  });

  it('allows changing source column', async () => {
    render(
      <PivotToColumnsDelimitedEditor
        nodeId="node1"
        nodeMetadata={{} as any}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    const sourceSelect = screen.getByLabelText('Source Column *');
    fireEvent.click(sourceSelect);
    const extraOption = await screen.findByText('extra');
    fireEvent.click(extraOption);

    expect(sourceSelect).toHaveTextContent('extra');
  });

  it('validates required fields and shows errors', async () => {
    render(
      <PivotToColumnsDelimitedEditor
        nodeId="node1"
        nodeMetadata={{} as any}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    // Clear delimiter to trigger error
    const delimiterInput = screen.getByLabelText('Pair Delimiter');
    await userEvent.clear(delimiterInput);
    fireEvent.blur(delimiterInput);

    const saveButton = screen.getByText('Save Configuration');
    fireEvent.click(saveButton);

    await waitFor(() => {
      expect(screen.getByText('Pair delimiter cannot be empty.')).toBeInTheDocument();
    });
    expect(mockOnSave).not.toHaveBeenCalled();
  });

  it('saves configuration when valid', async () => {
    render(
      <PivotToColumnsDelimitedEditor
        nodeId="node1"
        nodeMetadata={{} as any}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    // Modify some settings
    const prefixInput = screen.getByLabelText('Column Prefix (optional)');
    await userEvent.type(prefixInput, 'pvt_');

    const fixedListRadio = screen.getByLabelText('Fixed list of columns');
    fireEvent.click(fixedListRadio);

    const addColumnBtn = screen.getByText('Add Column');
    fireEvent.click(addColumnBtn);
    const fixedColumnInput = screen.getByDisplayValue('col_1');
    await userEvent.clear(fixedColumnInput);
    await userEvent.type(fixedColumnInput, 'name');

    const saveButton = screen.getByText('Save Configuration');
    fireEvent.click(saveButton);

    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledTimes(1);
      const savedConfig = mockOnSave.mock.calls[0][0] as PivotToColumnsDelimitedConfiguration;
      expect(savedConfig.columnPrefix).toBe('pvt_');
      expect(savedConfig.columnGeneration).toBe('fixedList');
      expect(savedConfig.fixedColumns).toEqual(['name']);
      expect(savedConfig.compilerMetadata?.validationStatus).toBe('VALID');
    });
  });

  it('loads initial configuration if provided', () => {
    const initialConfig: PivotToColumnsDelimitedConfiguration = {
      version: '1.0',
      sourceColumn: 'extra',
      delimiter: '|',
      keyValueSeparator: '=',
      columnGeneration: 'fixedList',
      fixedColumns: ['age', 'city'],
      missingKeyHandling: 'default',
      defaultValue: 'unknown',
      valueType: 'string',
      columnPrefix: 'col_',
      trimWhitespace: false,
      caseSensitiveKeys: true,
      errorHandling: 'skip',
      parallelization: true,
      batchSize: 500,
    };

    render(
      <PivotToColumnsDelimitedEditor
        nodeId="node1"
        nodeMetadata={{} as any}
        inputColumns={mockInputColumns}
        initialConfig={initialConfig}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    expect(screen.getByLabelText('Source Column *')).toHaveTextContent('extra');
    expect(screen.getByLabelText('Pair Delimiter')).toHaveValue('|');
    expect(screen.getByLabelText('Key‑Value Separator')).toHaveValue('=');
    expect(screen.getByLabelText('Column Prefix (optional)')).toHaveValue('col_');
    expect(screen.getByDisplayValue('age')).toBeInTheDocument();
    expect(screen.getByDisplayValue('city')).toBeInTheDocument();
    expect(screen.getByLabelText('Parallel execution')).toBeChecked();
  });

  it('shows warnings for missing default value', async () => {
    render(
      <PivotToColumnsDelimitedEditor
        nodeId="node1"
        nodeMetadata={{} as any}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    // Set missing key handling to 'default' and clear default value
    const missingHandlingSelect = screen.getByLabelText('Missing Key Handling');
    fireEvent.click(missingHandlingSelect);
    const defaultOption = await screen.findByText('Use default value');
    fireEvent.click(defaultOption);

    const defaultValueInput = screen.getByLabelText('Default Value');
    await userEvent.clear(defaultValueInput);

    const saveButton = screen.getByText('Save Configuration');
    fireEvent.click(saveButton);

    await waitFor(() => {
      expect(screen.getByText(/Default value is empty/)).toBeInTheDocument();
    });
    // Save should still proceed (warning only)
    expect(mockOnSave).toHaveBeenCalled();
  });
});