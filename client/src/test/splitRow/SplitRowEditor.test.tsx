// src/components/Editor/__tests__/SplitRowEditor.test.tsx
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import SplitRowEditor from '../../components/Editor/Aggregates/SplitRowEditor';
import { NormalizeComponentConfiguration } from '../../types/unified-pipeline.types';

// Mock input columns
const mockInputColumns = [
  { name: 'full_name', type: 'string', id: 'col1' },
  { name: 'address', type: 'string', id: 'col2' },
  { name: 'age', type: 'integer', id: 'col3' },
];

// Mock initial config (optional)
const mockInitialConfig: NormalizeComponentConfiguration = {
  version: '1.0',
  sourceColumn: 'full_name',
  delimiter: ',',
  trimValues: true,
  treatEmptyAsNull: false,
  outputColumnName: 'split_part',
  addRowNumber: true,
  rowNumberColumnName: 'row_idx',
  keepColumns: ['full_name', 'age'],
  errorHandling: 'skip',
  parallelization: false,
  batchSize: 500,
  compilerMetadata: {
    lastModified: new Date().toISOString(),
    createdBy: 'test',
    validationStatus: 'VALID',
    warnings: [],
  },
};

describe('SplitRowEditor', () => {
  const mockOnSave = jest.fn();
  const mockOnClose = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('renders correctly with default values when no initial config', () => {
    render(
      <SplitRowEditor
        nodeId="node-123"
        nodeName="Test Split"
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    // Header and basic elements
    expect(screen.getByText(/Split Row Configuration/i)).toBeInTheDocument();
    expect(screen.getByText(/Test Split/i)).toBeInTheDocument();
    expect(screen.getByText(/3 input columns/i)).toBeInTheDocument();

    // Default form values
    expect(screen.getByLabelText(/Source Column/i)).toHaveValue('');
    expect(screen.getByLabelText(/Delimiter/i)).toHaveValue(',');
    expect(screen.getByLabelText(/Output Column Name/i)).toHaveValue('split_value');
    expect(screen.getByLabelText(/Trim values/i)).toBeChecked();
    expect(screen.getByLabelText(/Treat empty as null/i)).not.toBeChecked();
    expect(screen.getByLabelText(/Add row number column/i)).not.toBeChecked();
    expect(screen.getByLabelText(/Enable parallel processing/i)).toBeChecked();
  });

  it('loads initial configuration values when provided', () => {
    render(
      <SplitRowEditor
        nodeId="node-123"
        inputColumns={mockInputColumns}
        initialConfig={mockInitialConfig}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    expect(screen.getByLabelText(/Source Column/i)).toHaveValue('full_name');
    expect(screen.getByLabelText(/Delimiter/i)).toHaveValue(',');
    expect(screen.getByLabelText(/Output Column Name/i)).toHaveValue('split_part');
    expect(screen.getByLabelText(/Add row number column/i)).toBeChecked();
    expect(screen.getByLabelText(/Row Number Column Name/i)).toHaveValue('row_idx');
    // Keep columns checkboxes – we can verify that 'full_name' and 'age' are checked
    expect(screen.getByLabelText('full_name')).toBeChecked();
    expect(screen.getByLabelText('age')).toBeChecked();
    expect(screen.getByLabelText('address')).not.toBeChecked();
    expect(screen.getByLabelText(/Enable parallel processing/i)).not.toBeChecked();
    expect(screen.getByLabelText(/Batch Size/i)).toHaveValue(500);
  });

  it('shows validation errors when required fields are missing', async () => {
    render(
      <SplitRowEditor
        nodeId="node-123"
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    // Click save without filling required fields
    const saveBtn = screen.getByRole('button', { name: /Save & Compile/i });
    fireEvent.click(saveBtn);

    await waitFor(() => {
      expect(screen.getByText(/Source column is required/i)).toBeInTheDocument();
      expect(screen.getByText(/Delimiter is required/i)).toBeInTheDocument();
      expect(screen.getByText(/Output column name is required/i)).toBeInTheDocument();
    });

    // Save should not be called
    expect(mockOnSave).not.toHaveBeenCalled();
  });

  it('validates output column name uniqueness against keep columns', async () => {
    render(
      <SplitRowEditor
        nodeId="node-123"
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    // Select source column
    fireEvent.change(screen.getByLabelText(/Source Column/i), { target: { value: 'full_name' } });
    fireEvent.change(screen.getByLabelText(/Delimiter/i), { target: { value: ',' } });

    // Set output column name equal to an existing kept column
    fireEvent.change(screen.getByLabelText(/Output Column Name/i), { target: { value: 'full_name' } });

    const saveBtn = screen.getByRole('button', { name: /Save & Compile/i });
    fireEvent.click(saveBtn);

    await waitFor(() => {
      expect(screen.getByText(/Output column name must be unique/i)).toBeInTheDocument();
    });
    expect(mockOnSave).not.toHaveBeenCalled();
  });

  it('validates row number column name uniqueness', async () => {
    render(
      <SplitRowEditor
        nodeId="node-123"
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    fireEvent.change(screen.getByLabelText(/Source Column/i), { target: { value: 'full_name' } });
    fireEvent.change(screen.getByLabelText(/Delimiter/i), { target: { value: ',' } });
    fireEvent.change(screen.getByLabelText(/Output Column Name/i), { target: { value: 'split_val' } });

    // Enable row number and set name equal to output column
    fireEvent.click(screen.getByLabelText(/Add row number column/i));
    fireEvent.change(screen.getByLabelText(/Row Number Column Name/i), { target: { value: 'split_val' } });

    fireEvent.click(screen.getByRole('button', { name: /Save & Compile/i }));

    await waitFor(() => {
      expect(screen.getByText(/Row number column name must be unique/i)).toBeInTheDocument();
    });
    expect(mockOnSave).not.toHaveBeenCalled();
  });

  it('allows toggling all keep columns', () => {
    render(
      <SplitRowEditor
        nodeId="node-123"
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    // Initially all columns are kept by default (since initialConfig not provided, keepColumns defaults to all)
    expect(screen.getByLabelText('full_name')).toBeChecked();
    expect(screen.getByLabelText('address')).toBeChecked();
    expect(screen.getByLabelText('age')).toBeChecked();

    // Clear all
    fireEvent.click(screen.getByText(/Clear All/i));
    expect(screen.getByLabelText('full_name')).not.toBeChecked();
    expect(screen.getByLabelText('address')).not.toBeChecked();
    expect(screen.getByLabelText('age')).not.toBeChecked();

    // Select all
    fireEvent.click(screen.getByText(/Select All/i));
    expect(screen.getByLabelText('full_name')).toBeChecked();
    expect(screen.getByLabelText('address')).toBeChecked();
    expect(screen.getByLabelText('age')).toBeChecked();
  });

  it('saves configuration with all values when valid', async () => {
    render(
      <SplitRowEditor
        nodeId="node-123"
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    // Fill form
    fireEvent.change(screen.getByLabelText(/Source Column/i), { target: { value: 'address' } });
    fireEvent.change(screen.getByLabelText(/Delimiter/i), { target: { value: '|' } });
    fireEvent.change(screen.getByLabelText(/Quote Character/i), { target: { value: '"' } });
    fireEvent.change(screen.getByLabelText(/Escape Character/i), { target: { value: '\\' } });
    fireEvent.click(screen.getByLabelText(/Trim values/i)); // uncheck
    fireEvent.click(screen.getByLabelText(/Treat empty as null/i));
    fireEvent.change(screen.getByLabelText(/Output Column Name/i), { target: { value: 'split_addr' } });
    fireEvent.click(screen.getByLabelText(/Add row number column/i));
    fireEvent.change(screen.getByLabelText(/Row Number Column Name/i), { target: { value: 'row_id' } });
    
    // Keep only 'age' column
    fireEvent.click(screen.getByLabelText('full_name'));
    fireEvent.click(screen.getByLabelText('address'));
    expect(screen.getByLabelText('age')).toBeChecked();

    // ✅ FIX: Set batch size BEFORE unchecking parallel processing
    // (Batch size input is only visible when parallelization is enabled)
    fireEvent.change(screen.getByLabelText(/Batch Size/i), { target: { value: '200' } });
    fireEvent.click(screen.getByLabelText(/Enable parallel processing/i)); // uncheck

    // Change error handling
    fireEvent.click(screen.getByLabelText(/Set null/i));

    // Save
    fireEvent.click(screen.getByRole('button', { name: /Save & Compile/i }));

    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledTimes(1);
      const savedConfig = mockOnSave.mock.calls[0][0] as NormalizeComponentConfiguration;
      expect(savedConfig.sourceColumn).toBe('address');
      expect(savedConfig.delimiter).toBe('|');
      expect(savedConfig.quoteChar).toBe('"');
      expect(savedConfig.escapeChar).toBe('\\');
      expect(savedConfig.trimValues).toBe(false);
      expect(savedConfig.treatEmptyAsNull).toBe(true);
      expect(savedConfig.outputColumnName).toBe('split_addr');
      expect(savedConfig.addRowNumber).toBe(true);
      expect(savedConfig.rowNumberColumnName).toBe('row_id');
      expect(savedConfig.keepColumns).toEqual(['age']);
      expect(savedConfig.errorHandling).toBe('setNull');
      expect(savedConfig.parallelization).toBe(false);
      expect(savedConfig.batchSize).toBe(200);
      // ✅ FIX: Use optional chaining for compilerMetadata
      expect(savedConfig.compilerMetadata?.validationStatus).toBe('VALID');
    });
  });

  it('calls onClose when cancel button is clicked', () => {
    render(
      <SplitRowEditor
        nodeId="node-123"
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );
    fireEvent.click(screen.getByRole('button', { name: /Cancel/i }));
    expect(mockOnClose).toHaveBeenCalledTimes(1);
    expect(mockOnSave).not.toHaveBeenCalled();
  });

  it('updates output schema preview dynamically', () => {
    render(
      <SplitRowEditor
        nodeId="node-123"
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    // Initially keep all columns, output column "split_value"
    expect(screen.getByText('split_value')).toBeInTheDocument();
    expect(screen.getByText('full_name')).toBeInTheDocument();
    expect(screen.getByText('address')).toBeInTheDocument();
    expect(screen.getByText('age')).toBeInTheDocument();

    // Remove 'address' from kept columns
    fireEvent.click(screen.getByLabelText('address'));
    expect(screen.queryByText('address')).not.toBeInTheDocument();

    // Change output column name
    fireEvent.change(screen.getByLabelText(/Output Column Name/i), { target: { value: 'new_col' } });
    expect(screen.getByText('new_col')).toBeInTheDocument();

    // Enable row number column
    fireEvent.click(screen.getByLabelText(/Add row number column/i));
    expect(screen.getByText('row_index')).toBeInTheDocument(); // default name
  });
});