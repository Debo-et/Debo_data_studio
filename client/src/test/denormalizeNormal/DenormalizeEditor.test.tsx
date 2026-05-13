// src/test/editor/DenormalizeEditor.test.tsx
import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import { DenormalizeEditor } from '../../components/Editor/Aggregates/DenormalizeEditor';
import { DenormalizeComponentConfiguration } from '../../types/unified-pipeline.types';

// Mock external dependencies
jest.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: any) => <div {...props}>{children}</div>,
  },
}));

jest.mock('lucide-react', () => ({
  X: () => <span>✕</span>,
  Save: () => <span>💾</span>,
  AlertCircle: () => <span>⚠️</span>,
}));

// Mock shadcn/ui components
jest.mock('../../../ui/card', () => ({
  Card: ({ children }: any) => <div className="card">{children}</div>,
  CardContent: ({ children }: any) => <div className="card-content">{children}</div>,
  CardDescription: ({ children }: any) => <div className="card-description">{children}</div>,
  CardHeader: ({ children }: any) => <div className="card-header">{children}</div>,
  CardTitle: ({ children }: any) => <div className="card-title">{children}</div>,
}));

jest.mock('../../../ui/input', () => ({
  Input: (props: any) => <input {...props} />,
}));

jest.mock('../../../ui/label', () => ({
  Label: ({ children, ...props }: any) => <label {...props}>{children}</label>,
}));

jest.mock('../../../ui/checkbox', () => ({
  Checkbox: ({ id, checked, onChange }: any) => (
    <input type="checkbox" id={id} checked={checked} onChange={onChange} />
  ),
}));

jest.mock('../../../ui/Button', () => ({
  Button: ({ children, onClick, className }: any) => (
    <button onClick={onClick} className={className}>
      {children}
    </button>
  ),
}));

jest.mock('../../../ui/badge', () => ({
  Badge: ({ children }: any) => <span className="badge">{children}</span>,
}));

jest.mock('../../../ui/select', () => ({
  Select: ({ children, value, onValueChange }: any) => (
    <div data-testid="select">
      <select value={value} onChange={(e) => onValueChange(e.target.value)}>
        {children}
      </select>
    </div>
  ),
  SelectContent: ({ children }: any) => <>{children}</>,
  SelectItem: ({ children, value }: any) => <option value={value}>{children}</option>,
  SelectTrigger: ({ children }: any) => <>{children}</>,
  SelectValue: ({ placeholder }: any) => <>{placeholder}</>,
}));

jest.mock('../../../ui/radio-group', () => ({
  RadioGroup: ({ children, value, onValueChange }: any) => (
    <div data-testid="radio-group" data-value={value}>
      {React.Children.map(children, child =>
        React.cloneElement(child, { onChange: () => onValueChange(child.props.value) })
      )}
    </div>
  ),
  RadioGroupItem: ({ value, id }: any) => <input type="radio" value={value} id={id} />,
}));

describe('DenormalizeEditor', () => {
  const mockInputColumns = [
    { name: 'id', type: 'integer', id: 'col1' },
    { name: 'name', type: 'string', id: 'col2' },
    { name: 'tags', type: 'string', id: 'col3' },
    { name: 'categories', type: 'string', id: 'col4' },
  ];

  const mockNodeMetadata = { id: 'node-123', name: 'DenormalizeTags' };
  const mockOnClose = jest.fn();
  const mockOnSave = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('renders correctly with default props', () => {
    render(
      <DenormalizeEditor
        nodeId="test-node"
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    expect(screen.getByText(/tDenormalize Configuration/i)).toBeInTheDocument();
    expect(screen.getByText(/Source Column \*/i)).toBeInTheDocument();
    expect(screen.getByText(/Delimiter \*/i)).toBeInTheDocument();
    expect(screen.getByText(/Output Column Name \*/i)).toBeInTheDocument();
    expect(screen.getByText(/Keep Columns/i)).toBeInTheDocument();
    expect(screen.getByText(/Parse Options/i)).toBeInTheDocument();
    expect(screen.getByText(/Additional Columns/i)).toBeInTheDocument();
    expect(screen.getByText(/Execution/i)).toBeInTheDocument();
  });

  it('loads initial configuration when provided', () => {
    const initialConfig: DenormalizeComponentConfiguration = {
      version: '1.0',
      sourceColumn: 'tags',
      delimiter: '|',
      trimValues: false,
      treatEmptyAsNull: true,
      quoteChar: '"',
      escapeChar: '\\',
      outputColumnName: 'tag',
      addRowNumber: true,
      rowNumberColumnName: 'rn',
      keepColumns: ['id', 'name'],
      errorHandling: 'skip',
      batchSize: 500,
      parallelization: true,
    };

    render(
      <DenormalizeEditor
        nodeId="test-node"
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        initialConfig={initialConfig}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    // Source column
    const select = screen.getByTestId('select').querySelector('select') as HTMLSelectElement;
    expect(select.value).toBe('tags');

    // Delimiter
    expect(screen.getByDisplayValue('|')).toBeInTheDocument();

    // Output column name
    expect(screen.getByDisplayValue('tag')).toBeInTheDocument();

    // Checkboxes
    const trimCheckbox = screen.getByLabelText('Trim whitespace from values') as HTMLInputElement;
    expect(trimCheckbox.checked).toBe(false);
    const treatEmptyCheckbox = screen.getByLabelText('Treat empty strings as NULL') as HTMLInputElement;
    expect(treatEmptyCheckbox.checked).toBe(true);
    const rowNumberCheckbox = screen.getByLabelText('Add row number column') as HTMLInputElement;
    expect(rowNumberCheckbox.checked).toBe(true);
    expect(screen.getByDisplayValue('rn')).toBeInTheDocument();

    // Keep columns
    const keepId = screen.getByLabelText('id (integer)') as HTMLInputElement;
    const keepName = screen.getByLabelText('name (string)') as HTMLInputElement;
    const keepTags = screen.getByLabelText('tags (string)') as HTMLInputElement;
    expect(keepId.checked).toBe(true);
    expect(keepName.checked).toBe(true);
    expect(keepTags.checked).toBe(false);

    // Error handling
    const radioGroup = screen.getByTestId('radio-group');
    expect(radioGroup).toHaveAttribute('data-value', 'skip');

    // Batch size
    expect(screen.getByDisplayValue('500')).toBeInTheDocument();

    // Parallelization
    const parallelCheckbox = screen.getByLabelText('Enable parallel processing') as HTMLInputElement;
    expect(parallelCheckbox.checked).toBe(true);
  });

  it('validates required fields and shows errors', async () => {
    render(
      <DenormalizeEditor
        nodeId="test-node"
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    // Clear source column (select empty option? Not possible, but we can simulate by clearing the select)
    const select = screen.getByTestId('select').querySelector('select') as HTMLSelectElement;
    fireEvent.change(select, { target: { value: '' } });

    // Clear delimiter
    const delimiterInput = screen.getByPlaceholderText(/e\.g\.,/);
    fireEvent.change(delimiterInput, { target: { value: '' } });

    // Clear output column name
    const outputInput = screen.getByPlaceholderText('denormalized_value');
    fireEvent.change(outputInput, { target: { value: '' } });

    // Set batch size to invalid
    const batchInput = screen.getByLabelText('Batch Size');
    fireEvent.change(batchInput, { target: { value: '0' } });

    const saveButton = screen.getByRole('button', { name: /Save Configuration/i });
    fireEvent.click(saveButton);

    await waitFor(() => {
      expect(screen.getByText(/Source column is required/i)).toBeInTheDocument();
      expect(screen.getByText(/Delimiter is required/i)).toBeInTheDocument();
      expect(screen.getByText(/Output column name is required/i)).toBeInTheDocument();
      expect(screen.getByText(/Batch size must be between 1 and 10000/i)).toBeInTheDocument();
    });

    expect(mockOnSave).not.toHaveBeenCalled();
  });

  it('allows toggling keep columns', () => {
    render(
      <DenormalizeEditor
        nodeId="test-node"
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    // Initially all columns are kept (default behaviour from editor)
    const checkboxes = screen.getAllByRole('checkbox');
    const keepCheckboxes = checkboxes.filter(cb => cb.id?.startsWith('keep-'));
    expect(keepCheckboxes).toHaveLength(mockInputColumns.length);
    keepCheckboxes.forEach(cb => expect(cb).toBeChecked());

    // Toggle off first column
    fireEvent.click(keepCheckboxes[0]);
    expect(keepCheckboxes[0]).not.toBeChecked();

    // Click "Select All" button
    const selectAllBtn = screen.getByRole('button', { name: /Select All/i });
    fireEvent.click(selectAllBtn);
    keepCheckboxes.forEach(cb => expect(cb).toBeChecked());

    // Click "Deselect All"
    const deselectAllBtn = screen.getByRole('button', { name: /Deselect All/i });
    fireEvent.click(deselectAllBtn);
    keepCheckboxes.forEach(cb => expect(cb).not.toBeChecked());
  });

  it('shows row number column input when add row number is checked', () => {
    render(
      <DenormalizeEditor
        nodeId="test-node"
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    expect(screen.queryByLabelText('Row Number Column Name')).not.toBeInTheDocument();

    const addRowCheckbox = screen.getByLabelText('Add row number column');
    fireEvent.click(addRowCheckbox);

    expect(screen.getByLabelText('Row Number Column Name')).toBeInTheDocument();
  });

  it('calls onSave with correct configuration on save', async () => {
    render(
      <DenormalizeEditor
        nodeId="test-node"
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    // Select source column
    const select = screen.getByTestId('select').querySelector('select') as HTMLSelectElement;
    fireEvent.change(select, { target: { value: 'tags' } });

    // Set delimiter
    fireEvent.change(screen.getByPlaceholderText(/e\.g\.,/), { target: { value: '|' } });

    // Set output column name
    fireEvent.change(screen.getByPlaceholderText('denormalized_value'), { target: { value: 'tag_value' } });

    // Uncheck "Trim whitespace"
    fireEvent.click(screen.getByLabelText('Trim whitespace from values'));

    // Check "Treat empty as NULL"
    fireEvent.click(screen.getByLabelText('Treat empty strings as NULL'));

    // Add row number
    fireEvent.click(screen.getByLabelText('Add row number column'));
    fireEvent.change(screen.getByLabelText('Row Number Column Name'), { target: { value: 'row_idx' } });

    // Keep only 'id' and 'name'
    const keepCheckboxes = screen.getAllByRole('checkbox').filter(cb => cb.id?.startsWith('keep-'));
    keepCheckboxes.forEach(cb => fireEvent.click(cb)); // uncheck all
    fireEvent.click(screen.getByLabelText('id (integer)')); // check id
    fireEvent.click(screen.getByLabelText('name (string)')); // check name

    // Set error handling to 'setNull'
    const radioSetNull = screen.getByLabelText('Set NULL');
    fireEvent.click(radioSetNull);

    // Set batch size
    fireEvent.change(screen.getByLabelText('Batch Size'), { target: { value: '200' } });

    // Enable parallelization
    fireEvent.click(screen.getByLabelText('Enable parallel processing'));

    const saveButton = screen.getByRole('button', { name: /Save Configuration/i });
    fireEvent.click(saveButton);

    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledTimes(1);
      const savedConfig = mockOnSave.mock.calls[0][0] as DenormalizeComponentConfiguration;

      expect(savedConfig.sourceColumn).toBe('tags');
      expect(savedConfig.delimiter).toBe('|');
      expect(savedConfig.outputColumnName).toBe('tag_value');
      expect(savedConfig.trimValues).toBe(false);
      expect(savedConfig.treatEmptyAsNull).toBe(true);
      expect(savedConfig.addRowNumber).toBe(true);
      expect(savedConfig.rowNumberColumnName).toBe('row_idx');
      expect(savedConfig.keepColumns).toEqual(['id', 'name']);
      expect(savedConfig.errorHandling).toBe('setNull');
      expect(savedConfig.batchSize).toBe(200);
      expect(savedConfig.parallelization).toBe(true);
      expect(savedConfig.compilerMetadata?.validationStatus).toBe('VALID');
    });
  });

  it('calls onClose when cancel button is clicked', () => {
    render(
      <DenormalizeEditor
        nodeId="test-node"
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );
    const cancelButton = screen.getByRole('button', { name: /Cancel/i });
    fireEvent.click(cancelButton);
    expect(mockOnClose).toHaveBeenCalled();
  });

  it('supports keyboard shortcuts: Escape to close, Ctrl+S to save', () => {
    render(
      <DenormalizeEditor
        nodeId="test-node"
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    fireEvent.keyDown(window, { key: 'Escape' });
    expect(mockOnClose).toHaveBeenCalled();

    fireEvent.keyDown(window, { key: 's', ctrlKey: true });
    expect(mockOnSave).toHaveBeenCalled();
  });
});