// src/components/Editor/__tests__/ExtractDelimitedFieldsConfigModal.test.tsx
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import { ExtractDelimitedFieldsConfigModal } from '../../components/Editor/Parsing/ExtractDelimitedFieldsConfigModal';
import { ExtractDelimitedFieldsConfiguration } from '../../types/unified-pipeline.types';

// Mock framer-motion to avoid animation issues
jest.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: any) => <div {...props}>{children}</div>,
  },
}));

// Mock UI components (Button, Card, etc.) to simplify testing
jest.mock('../../../ui/Button', () => ({
  Button: ({ children, onClick, variant, ...props }: any) => (
    <button onClick={onClick} data-variant={variant} {...props}>
      {children}
    </button>
  ),
}));
jest.mock('../../../ui/card', () => ({
  Card: ({ children }: any) => <div data-testid="card">{children}</div>,
  CardContent: ({ children }: any) => <div>{children}</div>,
}));
jest.mock('../../../ui/badge', () => ({
  Badge: ({ children }: any) => <span data-testid="badge">{children}</span>,
}));
jest.mock('../../../ui/input', () => ({
  Input: (props: any) => <input {...props} />,
}));
jest.mock('../../../ui/label', () => ({
  Label: ({ children, htmlFor }: any) => <label htmlFor={htmlFor}>{children}</label>,
}));

describe('ExtractDelimitedFieldsConfigModal', () => {
  const mockInputColumns = [
    { name: 'full_address', type: 'string' },
    { name: 'user_id', type: 'integer' },
    { name: 'raw_data', type: 'text' },
  ];

  const mockOnSave = jest.fn();
  const mockOnClose = jest.fn();

  const defaultProps = {
    isOpen: true,
    onClose: mockOnClose,
    nodeId: 'node-123',
    nodeName: 'ExtractAddress',
    inputColumns: mockInputColumns,
    initialConfig: undefined,
    onSave: mockOnSave,
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('renders correctly when open', () => {
    render(<ExtractDelimitedFieldsConfigModal {...defaultProps} />);
    expect(screen.getByText(/Extract Delimited Fields/i)).toBeInTheDocument();
    expect(screen.getByText(/Node:/i)).toHaveTextContent('ExtractAddress');
    expect(screen.getByLabelText(/Source Column/i)).toBeInTheDocument();
    expect(screen.getByText(/Output Columns/i)).toBeInTheDocument();
    expect(screen.getByText(/Advanced/i)).toBeInTheDocument();
  });

  it('disables source column selection when no input columns', () => {
    render(<ExtractDelimitedFieldsConfigModal {...defaultProps} inputColumns={[]} />);
    const select = screen.getByLabelText(/Source Column/i) as HTMLSelectElement;
    expect(select.options.length).toBe(1); // only the "Select column" placeholder
    expect(select.value).toBe('');
  });

  it('allows selecting a source column', async () => {
    render(<ExtractDelimitedFieldsConfigModal {...defaultProps} />);
    const select = screen.getByLabelText(/Source Column/i) as HTMLSelectElement;
    fireEvent.change(select, { target: { value: 'full_address' } });
    expect(select.value).toBe('full_address');
  });

  it('allows changing delimiter via preset buttons', () => {
    render(<ExtractDelimitedFieldsConfigModal {...defaultProps} />);
    const commaBtn = screen.getByRole('button', { name: /Comma/i });
    const tabBtn = screen.getByRole('button', { name: /Tab/i });
    fireEvent.click(tabBtn);
    // Need to check internal state; we can verify by looking at custom delimiter input value
    const delimiterInput = screen.getByPlaceholderText(/Custom delimiter/i) as HTMLInputElement;
    expect(delimiterInput.value).toBe('\t');
    fireEvent.click(commaBtn);
    expect(delimiterInput.value).toBe(',');
  });

  it('allows entering a custom delimiter', () => {
    render(<ExtractDelimitedFieldsConfigModal {...defaultProps} />);
    const delimiterInput = screen.getByPlaceholderText(/Custom delimiter/i);
    fireEvent.change(delimiterInput, { target: { value: '|' } });
    expect(delimiterInput).toHaveValue('|');
  });

  it('toggles trim whitespace and nullIfEmpty checkboxes', () => {
    render(<ExtractDelimitedFieldsConfigModal {...defaultProps} />);
    const trimCheckbox = screen.getByLabelText(/Trim whitespace/i) as HTMLInputElement;
    const nullIfEmptyCheckbox = screen.getByLabelText(/Treat empty strings as NULL/i) as HTMLInputElement;
    expect(trimCheckbox.checked).toBe(true);
    expect(nullIfEmptyCheckbox.checked).toBe(false);
    fireEvent.click(trimCheckbox);
    expect(trimCheckbox.checked).toBe(false);
    fireEvent.click(nullIfEmptyCheckbox);
    expect(nullIfEmptyCheckbox.checked).toBe(true);
  });

  it('adds and removes output columns', () => {
    render(<ExtractDelimitedFieldsConfigModal {...defaultProps} />);
    // Initially one column (Field1)
    expect(screen.getAllByPlaceholderText(/Column name/i)).toHaveLength(1);
    const addButton = screen.getByRole('button', { name: /Add Column/i });
    fireEvent.click(addButton);
    expect(screen.getAllByPlaceholderText(/Column name/i)).toHaveLength(2);
    // Remove second column
    const removeButtons = screen.getAllByRole('button', { name: '' }); // Trash2 icon buttons
    fireEvent.click(removeButtons[1]);
    expect(screen.getAllByPlaceholderText(/Column name/i)).toHaveLength(1);
  });

  it('prevents removing the last column', () => {
    render(<ExtractDelimitedFieldsConfigModal {...defaultProps} />);
    const removeButton = screen.getByRole('button', { name: '' });
    expect(removeButton).toBeDisabled();
  });

  it('updates column name and type', () => {
    render(<ExtractDelimitedFieldsConfigModal {...defaultProps} />);
    const nameInput = screen.getByPlaceholderText(/Column name/i);
    fireEvent.change(nameInput, { target: { value: 'street' } });
    expect(nameInput).toHaveValue('street');
    const typeSelect = screen.getByDisplayValue('String') as HTMLSelectElement;
    fireEvent.change(typeSelect, { target: { value: 'INTEGER' } });
    expect(typeSelect.value).toBe('INTEGER');
  });

  it('reorders columns via drag and drop', async () => {
    render(<ExtractDelimitedFieldsConfigModal {...defaultProps} />);
    // Add a second column
    fireEvent.click(screen.getByRole('button', { name: /Add Column/i }));
    const nameInputs = screen.getAllByPlaceholderText(/Column name/i);
    fireEvent.change(nameInputs[0], { target: { value: 'First' } });
    fireEvent.change(nameInputs[1], { target: { value: 'Second' } });
    // Simulate drag and drop
    const dragHandles = screen.getAllByTestId('grip-vertical'); // Lucide icon might not have testid, but we can use class
    // In real test, we would use userEvent.dragAndDrop, but for simplicity we trigger events
    const firstRow = dragHandles[0].closest('div')!;
    const secondRow = dragHandles[1].closest('div')!;
    fireEvent.dragStart(firstRow, { dataTransfer: { setData: jest.fn() } });
    fireEvent.dragOver(secondRow);
    fireEvent.drop(secondRow);
    // After reorder, the first name input should now be "Second"
    const updatedInputs = screen.getAllByPlaceholderText(/Column name/i);
    expect(updatedInputs[0]).toHaveValue('Second');
    expect(updatedInputs[1]).toHaveValue('First');
  });

  it('changes error handling option', () => {
    render(<ExtractDelimitedFieldsConfigModal {...defaultProps} />);
    fireEvent.click(screen.getByText(/Advanced/i));
    const errorSelect = screen.getByLabelText(/Error Handling/i) as HTMLSelectElement;
    fireEvent.change(errorSelect, { target: { value: 'skip' } });
    expect(errorSelect.value).toBe('skip');
  });

  it('toggles parallel processing and batch size', () => {
    render(<ExtractDelimitedFieldsConfigModal {...defaultProps} />);
    fireEvent.click(screen.getByText(/Advanced/i));
    const parallelCheckbox = screen.getByLabelText(/Enable parallel processing/i) as HTMLInputElement;
    const batchSizeInput = screen.getByLabelText(/Batch Size/i) as HTMLInputElement;
    expect(parallelCheckbox.checked).toBe(false);
    fireEvent.click(parallelCheckbox);
    expect(parallelCheckbox.checked).toBe(true);
    fireEvent.change(batchSizeInput, { target: { value: '500' } });
    expect(batchSizeInput.value).toBe('500');
  });

  it('loads initial configuration when provided', () => {
    const initialConfig: ExtractDelimitedFieldsConfiguration = {
      version: '1.0',
      sourceColumn: 'raw_data',
      delimiter: '|',
      quoteChar: '"',
      escapeChar: '\\',
      trimWhitespace: false,
      nullIfEmpty: true,
      outputColumns: [
        { id: 'col1', name: 'part1', type: 'STRING', position: 1 },
        { id: 'col2', name: 'part2', type: 'INTEGER', position: 2 },
      ],
      errorHandling: 'setNull',
      parallelization: true,
      batchSize: 2000,
    };
    render(<ExtractDelimitedFieldsConfigModal {...defaultProps} initialConfig={initialConfig} />);
    // Check source column
    const sourceSelect = screen.getByLabelText(/Source Column/i) as HTMLSelectElement;
    expect(sourceSelect.value).toBe('raw_data');
    // Check delimiter
    const delimiterInput = screen.getByPlaceholderText(/Custom delimiter/i) as HTMLInputElement;
    expect(delimiterInput.value).toBe('|');
    // Check trim checkbox
    const trimCheckbox = screen.getByLabelText(/Trim whitespace/i) as HTMLInputElement;
    expect(trimCheckbox.checked).toBe(false);
    // Check output columns count
    const nameInputs = screen.getAllByPlaceholderText(/Column name/i);
    expect(nameInputs).toHaveLength(2);
    expect(nameInputs[0]).toHaveValue('part1');
    expect(nameInputs[1]).toHaveValue('part2');
    // Advanced tab
    fireEvent.click(screen.getByText(/Advanced/i));
    const errorSelect = screen.getByLabelText(/Error Handling/i) as HTMLSelectElement;
    expect(errorSelect.value).toBe('setNull');
    const parallelCheckbox = screen.getByLabelText(/Enable parallel processing/i) as HTMLInputElement;
    expect(parallelCheckbox.checked).toBe(true);
    const batchSizeInput = screen.getByLabelText(/Batch Size/i) as HTMLInputElement;
    expect(batchSizeInput.value).toBe('2000');
  });

  it('calls onSave with correct configuration', async () => {
    render(<ExtractDelimitedFieldsConfigModal {...defaultProps} />);
    // Set source column
    const sourceSelect = screen.getByLabelText(/Source Column/i);
    fireEvent.change(sourceSelect, { target: { value: 'full_address' } });
    // Set custom delimiter
    const delimiterInput = screen.getByPlaceholderText(/Custom delimiter/i);
    fireEvent.change(delimiterInput, { target: { value: ';' } });
    // Add a second column and rename
    fireEvent.click(screen.getByRole('button', { name: /Add Column/i }));
    const nameInputs = screen.getAllByPlaceholderText(/Column name/i);
    fireEvent.change(nameInputs[0], { target: { value: 'street' } });
    fireEvent.change(nameInputs[1], { target: { value: 'city' } });
    // Change type of second column to INTEGER
    const typeSelects = screen.getAllByRole('combobox', { name: '' });
    fireEvent.change(typeSelects[1], { target: { value: 'INTEGER' } });
    // Click Save
    const saveButton = screen.getByRole('button', { name: /Save/i });
    fireEvent.click(saveButton);
    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledTimes(1);
      const savedConfig = mockOnSave.mock.calls[0][0] as ExtractDelimitedFieldsConfiguration;
      expect(savedConfig.sourceColumn).toBe('full_address');
      expect(savedConfig.delimiter).toBe(';');
      expect(savedConfig.trimWhitespace).toBe(true);
      expect(savedConfig.nullIfEmpty).toBe(false);
      expect(savedConfig.outputColumns).toHaveLength(2);
      expect(savedConfig.outputColumns[0]).toMatchObject({ name: 'street', type: 'STRING', position: 1 });
      expect(savedConfig.outputColumns[1]).toMatchObject({ name: 'city', type: 'INTEGER', position: 2 });
      expect(savedConfig.errorHandling).toBe('fail');
      expect(savedConfig.parallelization).toBe(false);
      expect(savedConfig.batchSize).toBe(1000);
    });
  });

  it('calls onClose when Cancel is clicked', () => {
    render(<ExtractDelimitedFieldsConfigModal {...defaultProps} />);
    const cancelButton = screen.getByRole('button', { name: /Cancel/i });
    fireEvent.click(cancelButton);
    expect(mockOnClose).toHaveBeenCalled();
  });
});