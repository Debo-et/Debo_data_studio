// src/components/Editor/Aggregates/__tests__/DenormalizeSortedRowEditor.test.tsx
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import { DenormalizeSortedRowEditor } from '../../components/Editor/Aggregates/DenormalizeSortedRowEditor';
import { DenormalizeSortedRowComponentConfiguration } from '../../types/unified-pipeline.types';

// Mock child components (if needed)
jest.mock('../../../ui/Button', () => ({
  Button: ({ children, onClick, disabled, className }: any) => (
    <button onClick={onClick} disabled={disabled} className={className}>
      {children}
    </button>
  ),
}));
jest.mock('../../../ui/input', () => ({
  Input: (props: any) => <input {...props} />,
}));
jest.mock('../../../ui/label', () => ({
  Label: ({ children, htmlFor }: any) => <label htmlFor={htmlFor}>{children}</label>,
}));
jest.mock('../../../ui/select', () => ({
  Select: ({ children, value, onValueChange }: any) => (
    <select
      value={value}
      onChange={(e) => onValueChange(e.target.value)}
      data-testid="mock-select"
    >
      {children}
    </select>
  ),
  SelectTrigger: ({ children }: any) => <>{children}</>,
  SelectValue: () => null,
  SelectContent: ({ children }: any) => <>{children}</>,
  SelectItem: ({ value, children }: any) => <option value={value}>{children}</option>,
}));
jest.mock('../../../ui/checkbox', () => ({
  Checkbox: ({ checked, onChange, id }: any) => (
    <input type="checkbox" checked={checked} onChange={onChange} id={id} />
  ),
}));
jest.mock('../../../ui/badge', () => ({
  Badge: ({ children, className }: any) => <span className={className}>{children}</span>,
}));
jest.mock('../../../ui/card', () => ({
  Card: ({ children }: any) => <div>{children}</div>,
  CardHeader: ({ children, onClick }: any) => <div onClick={onClick}>{children}</div>,
  CardTitle: ({ children }: any) => <div>{children}</div>,
  CardContent: ({ children }: any) => <div>{children}</div>,
}));

describe('DenormalizeSortedRowEditor', () => {
  const mockInputColumns = [
    { name: 'customer_id', type: 'INTEGER', id: 'c1' },
    { name: 'order_date', type: 'DATE', id: 'c2' },
    { name: 'product', type: 'STRING', id: 'c3' },
    { name: 'quantity', type: 'INTEGER', id: 'c4' },
    { name: 'price', type: 'DECIMAL', id: 'c5' },
  ];

  const mockNodeMetadata = { id: 'node-123', name: 'DenormalizeNode' };
  const mockOnClose = jest.fn();
  const mockOnSave = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('renders correctly with input columns', () => {
    render(
      <DenormalizeSortedRowEditor
        nodeId="test-node"
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    expect(screen.getByText(/Denormalize \(Sorted Row\)/i)).toBeInTheDocument();
    expect(screen.getByText(/Group By Keys/i)).toBeInTheDocument();
    expect(screen.getByText(/Sort Order \(within groups\)/i)).toBeInTheDocument();
    expect(screen.getByText(/Denormalized Columns/i)).toBeInTheDocument();
    expect(screen.getByText('customer_id')).toBeInTheDocument();
    expect(screen.getByText('order_date')).toBeInTheDocument();
    expect(screen.getByText('product')).toBeInTheDocument();
  });

  it('allows adding and removing sort keys', async () => {
    render(
      <DenormalizeSortedRowEditor
        nodeId="test-node"
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    // Initially there is one sort key (default)
    const addSortKeyBtn = screen.getAllByText(/Add Sort Key/i)[0];
    expect(screen.getAllByTestId('mock-select').length).toBe(1); // one sort key select

    // Add a second sort key
    fireEvent.click(addSortKeyBtn);
    await waitFor(() => {
      expect(screen.getAllByTestId('mock-select').length).toBe(2);
    });

    // Remove the first sort key
    const deleteButtons = screen.getAllByRole('button', { name: /trash/i });
    fireEvent.click(deleteButtons[0]);
    await waitFor(() => {
      expect(screen.getAllByTestId('mock-select').length).toBe(1);
    });
  });

  it('allows adding and removing denormalized columns', async () => {
    render(
      <DenormalizeSortedRowEditor
        nodeId="test-node"
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    const addColumnBtn = screen.getAllByText(/Add Column/i)[0];
    expect(screen.getAllByTestId('mock-select').length).toBe(1); // one sort key, one denorm column? Actually there are separate selects for denorm columns. We'll count rows.
    const initialRows = screen.getAllByRole('row').length;
    fireEvent.click(addColumnBtn);
    await waitFor(() => {
      expect(screen.getAllByRole('row').length).toBe(initialRows + 1);
    });
  });

  it('validates groupBy keys are required', async () => {
    render(
      <DenormalizeSortedRowEditor
        nodeId="test-node"
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    // Uncheck all groupBy checkboxes
    const checkboxes = screen.getAllByRole('checkbox');
    const groupByCheckboxes = checkboxes.filter(
      (cb) => mockInputColumns.some((col) => cb.id?.includes(col.name))
    );
    groupByCheckboxes.forEach((cb) => fireEvent.click(cb));

    await waitFor(() => {
      expect(screen.getByText(/At least one group-by key is required/i)).toBeInTheDocument();
    });

    const saveBtn = screen.getByRole('button', { name: /Save Configuration/i });
    expect(saveBtn).toBeDisabled();
  });

  it('validates duplicate output column names', async () => {
    render(
      <DenormalizeSortedRowEditor
        nodeId="test-node"
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    // Add a second denorm column and set both to same output name
    const addColumnBtn = screen.getAllByText(/Add Column/i)[0];
    fireEvent.click(addColumnBtn);
    await waitFor(() => {
      expect(screen.getAllByPlaceholderText('Output name').length).toBe(2);
    });

    const outputInputs = screen.getAllByPlaceholderText('Output name');
    fireEvent.change(outputInputs[0], { target: { value: 'duplicate' } });
    fireEvent.change(outputInputs[1], { target: { value: 'duplicate' } });

    await waitFor(() => {
      expect(screen.getByText(/Duplicate output column names: duplicate/i)).toBeInTheDocument();
    });
  });

  it('validates FIRST/LAST aggregations require sort keys', async () => {
    render(
      <DenormalizeSortedRowEditor
        nodeId="test-node"
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    // Remove all sort keys
    const deleteSortBtns = screen.getAllByRole('button', { name: /trash/i }).filter(
      (btn) => btn.closest('div')?.querySelector('select') // crude: but we can just click all delete buttons
    );
    deleteSortBtns.forEach((btn) => fireEvent.click(btn));

    // Set first denorm column aggregation to FIRST
    const aggregationSelects = screen.getAllByTestId('mock-select');
    // The denorm column aggregation selects are after the sort key selects (if any)
    fireEvent.change(aggregationSelects[0], { target: { value: 'FIRST' } });

    await waitFor(() => {
      expect(
        screen.getByText(/FIRST\/LAST aggregations require at least one sort key/i)
      ).toBeInTheDocument();
    });
  });

  it('saves configuration with correct structure', async () => {
    render(
      <DenormalizeSortedRowEditor
        nodeId="test-node"
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    // Set groupBy: customer_id
    const customerCheckbox = screen.getByLabelText('customer_id');
    fireEvent.click(customerCheckbox);

    // Add a sort key: order_date DESC
    const sortKeySelects = screen.getAllByTestId('mock-select');
    fireEvent.change(sortKeySelects[0], { target: { value: 'order_date' } });
    const directionSelect = screen.getAllByTestId('mock-select')[1];
    fireEvent.change(directionSelect, { target: { value: 'DESC' } });

    // Configure denorm column: product -> STRING_AGG with separator ';'
    const denormAggSelect = screen.getAllByTestId('mock-select')[2];
    fireEvent.change(denormAggSelect, { target: { value: 'STRING_AGG' } });
    const separatorInput = screen.getByPlaceholderText('Separator');
    fireEvent.change(separatorInput, { target: { value: ';' } });

    // Click save
    const saveBtn = screen.getByRole('button', { name: /Save Configuration/i });
    fireEvent.click(saveBtn);

    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledTimes(1);
      const savedConfig = mockOnSave.mock.calls[0][0] as DenormalizeSortedRowComponentConfiguration;
      expect(savedConfig.groupByFields).toEqual(['customer_id']);
      expect(savedConfig.sortKeys).toHaveLength(1);
      expect(savedConfig.sortKeys[0].field).toBe('order_date');
      expect(savedConfig.sortKeys[0].direction).toBe('DESC');
      expect(savedConfig.denormalizedColumns).toHaveLength(1);
      expect(savedConfig.denormalizedColumns[0].sourceField).toBe('product');
      expect(savedConfig.denormalizedColumns[0].aggregation).toBe('STRING_AGG');
      expect(savedConfig.denormalizedColumns[0].separator).toBe(';');
      expect(savedConfig.outputSchema.fields).toHaveLength(2); // groupBy + denorm
    });
  });
});