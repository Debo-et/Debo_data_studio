// src/test/addCRC/AddCRCRowEditor.test.tsx
import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import AddCRCRowEditor from '../../components/Editor/Parsing/AddCRCRowEditor';
import { AddCRCRowComponentConfiguration } from '../../types/unified-pipeline.types';

// Mock shadcn/ui components with explicit .tsx extensions and __esModule
jest.mock('../../components/ui/card.tsx', () => ({
  __esModule: true,
  Card: ({ children }: { children: React.ReactNode }) => <div data-testid="card">{children}</div>,
  CardContent: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
  CardDescription: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
  CardHeader: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
  CardTitle: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
}));

jest.mock('../../components/ui/input.tsx', () => ({
  __esModule: true,
  Input: (props: any) => <input data-testid="input" {...props} />,
}));

jest.mock('../../components/ui/label.tsx', () => ({
  __esModule: true,
  Label: ({ children, htmlFor }: any) => <label htmlFor={htmlFor}>{children}</label>,
}));

jest.mock('../../components/ui/Button.tsx', () => ({
  __esModule: true,
  Button: ({ children, onClick, disabled }: any) => (
    <button onClick={onClick} disabled={disabled} data-testid="button">
      {children}
    </button>
  ),
}));

jest.mock('../../components/ui/switch.tsx', () => ({
  __esModule: true,
  Switch: ({ checked, onCheckedChange, onChange }: any) => (
    <input
      type="checkbox"
      checked={checked}
      onChange={(e) => {
        const handler = onCheckedChange || onChange;
        if (handler) handler(e.target.checked);
      }}
      data-testid="switch"
    />
  ),
}));

jest.mock('../../components/ui/select.tsx', () => ({
  __esModule: true,
  Select: ({ children, onValueChange, value }: any) => (
    <select data-testid="select" value={value} onChange={(e) => onValueChange(e.target.value)}>
      {children}
    </select>
  ),
  SelectContent: ({ children }: any) => <>{children}</>,
  SelectItem: ({ children, value }: any) => <option value={value}>{children}</option>,
  SelectTrigger: ({ children }: any) => <>{children}</>,
  SelectValue: ({ placeholder }: any) => <>{placeholder}</>,
}));

jest.mock('../../components/ui/checkbox.tsx', () => ({
  __esModule: true,
  Checkbox: ({ checked, onCheckedChange, onChange, id }: any) => (
    <input
      type="checkbox"
      checked={checked}
      onChange={(e) => {
        // Accept both onCheckedChange (shadcn) and onChange (native fallback)
        const handler = onCheckedChange || onChange;
        if (handler) handler(e.target.checked);
      }}
      id={id}
      data-testid="checkbox"
    />
  ),
}));

describe('AddCRCRowEditor', () => {
  const mockInputColumns = [
    { name: 'id', type: 'INTEGER', id: 'col1' },
    { name: 'name', type: 'STRING', id: 'col2' },
    { name: 'value', type: 'DECIMAL', id: 'col3' },
  ];
  const mockNodeId = 'node-123';
  const mockNodeMetadata = { name: 'CRC Node', id: 'node-123' };
  const mockOnClose = jest.fn();
  const mockOnSave = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('renders with default values when no initialConfig', () => {
    render(
      <AddCRCRowEditor
        nodeId={mockNodeId}
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    expect(screen.getByText(/tAddCRCRow Configuration/i)).toBeInTheDocument();
    expect(screen.getByText(/CRC-32 \(32-bit\)/i)).toBeInTheDocument();
    expect(screen.getByDisplayValue('crc')).toBeInTheDocument();
    expect(screen.getByText(/Compute CRC on entire row/i)).toBeInTheDocument();
    expect(screen.getByText(/Columns to Include/i)).toBeInTheDocument();
    mockInputColumns.forEach(col => {
      expect(screen.getByText(col.name)).toBeInTheDocument();
    });
  });

  it('loads initial configuration when provided', () => {
    const initialConfig: AddCRCRowComponentConfiguration = {
      version: '1.0',
      includedColumns: ['id', 'name'],
      algorithm: 'CRC16',
      outputColumnName: 'row_hash',
      nullHandling: 'USE_DEFAULT',
      defaultValue: '0',
      characterEncoding: 'UTF-16',
      computeOnWholeRow: false,
      columnSeparator: '|',
      outputSchema: {} as any,
      sqlGeneration: { canPushDown: false, requiresExpression: true },
      compilerMetadata: { lastModified: '', createdBy: '', validationStatus: 'VALID', warnings: [], dependencies: [] },
    };

    render(
      <AddCRCRowEditor
        nodeId={mockNodeId}
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        initialConfig={initialConfig}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    expect(screen.getByDisplayValue('row_hash')).toBeInTheDocument();
    const selects = screen.getAllByTestId('select');
    const algorithmSelect = selects[0] as HTMLSelectElement;
    expect(algorithmSelect.value).toBe('CRC16');
    expect(screen.getByDisplayValue('0')).toBeInTheDocument();
    expect(screen.getByDisplayValue('UTF-16')).toBeInTheDocument();

    // Use regex to match label text that includes the column name followed by type
    const idCheckbox = screen.getByLabelText(/^id/) as HTMLInputElement;
    const nameCheckbox = screen.getByLabelText(/^name/) as HTMLInputElement;
    const valueCheckbox = screen.getByLabelText(/^value/) as HTMLInputElement;
    expect(idCheckbox.checked).toBe(true);
    expect(nameCheckbox.checked).toBe(true);
    expect(valueCheckbox.checked).toBe(false);
  });

  it('validates output column name conflict with existing column', () => {
    render(
      <AddCRCRowEditor
        nodeId={mockNodeId}
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    const outputInput = screen.getByDisplayValue('crc');
    fireEvent.change(outputInput, { target: { value: 'id' } });
    expect(screen.getByText(/Column name already exists in input/i)).toBeInTheDocument();
    const saveButton = screen.getByText('Save Configuration');
    expect(saveButton).toBeDisabled();
  });

  it('allows toggling all columns', () => {
    render(
      <AddCRCRowEditor
        nodeId={mockNodeId}
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    const selectAllButton = screen.getByText('Select All');
    fireEvent.click(selectAllButton);
    const checkboxes = screen.getAllByTestId('checkbox');
    checkboxes.forEach(cb => expect(cb).toBeChecked());

    const deselectAllButton = screen.getByText('Deselect All');
    fireEvent.click(deselectAllButton);
    checkboxes.forEach(cb => expect(cb).not.toBeChecked());
  });

  it('shows default value input when nullHandling is USE_DEFAULT', async () => {
    render(
      <AddCRCRowEditor
        nodeId={mockNodeId}
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    const selects = screen.getAllByTestId('select');
    const nullHandlingSelect = selects[1];
    fireEvent.change(nullHandlingSelect, { target: { value: 'USE_DEFAULT' } });
    expect(await screen.findByLabelText('Default Value')).toBeInTheDocument();
  });

  it('shows column separator input when computeOnWholeRow is true', () => {
    render(
      <AddCRCRowEditor
        nodeId={mockNodeId}
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    const switchElement = screen.getByTestId('switch');
    fireEvent.click(switchElement);
    expect(screen.getByLabelText('Column Separator')).toBeInTheDocument();
    expect(screen.queryByText(/Columns to Include/i)).not.toBeInTheDocument();
  });

  it('saves configuration with correct output schema and fields', async () => {
    render(
      <AddCRCRowEditor
        nodeId={mockNodeId}
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    const selects = screen.getAllByTestId('select');
    const algorithmSelect = selects[0];
    fireEvent.change(algorithmSelect, { target: { value: 'CRC8' } });
    const outputInput = screen.getByDisplayValue('crc');
    fireEvent.change(outputInput, { target: { value: 'checksum' } });
    const nameCheckbox = screen.getByLabelText(/^name/);
    const selectAll = screen.getByText('Select All');
    fireEvent.click(selectAll);
    // Now uncheck the 'name' checkbox
    fireEvent.click(nameCheckbox);
    const nullSelect = selects[1];
    fireEvent.change(nullSelect, { target: { value: 'USE_DEFAULT' } });
    const defaultInput = await screen.findByLabelText('Default Value');
    fireEvent.change(defaultInput, { target: { value: 'NULL' } });

    const saveButton = screen.getByText('Save Configuration');
    fireEvent.click(saveButton);

    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledTimes(1);
      const config = mockOnSave.mock.calls[0][0] as AddCRCRowComponentConfiguration;
      expect(config.algorithm).toBe('CRC8');
      expect(config.outputColumnName).toBe('checksum');
      expect(config.includedColumns).toEqual(['id', 'value']);
      expect(config.nullHandling).toBe('USE_DEFAULT');
      expect(config.defaultValue).toBe('NULL');
      expect(config.outputSchema.fields).toHaveLength(4);
      const crcField = config.outputSchema.fields.find(f => f.name === 'checksum');
      expect(crcField).toBeDefined();
      expect(crcField?.type).toBe('STRING');
    });
  });

  it('cancels and calls onClose', () => {
    render(
      <AddCRCRowEditor
        nodeId={mockNodeId}
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );
    const cancelButton = screen.getByText('Cancel');
    fireEvent.click(cancelButton);
    expect(mockOnClose).toHaveBeenCalled();
    expect(mockOnSave).not.toHaveBeenCalled();
  });
});