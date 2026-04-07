// src/test/unite/UniteEditor.test.tsx
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import '@testing-library/jest-dom';
import { UniteEditor } from '../../components/Editor/Aggregates/UniteEditor';
import { UniteComponentConfiguration, SchemaDefinition } from '../../types/unified-pipeline.types';

// Mock child components if needed
jest.mock('../../components/common/Select', () => ({ value, onChange, options, label }: any) => (
  <div>
    <label>{label}</label>
    <select value={value} onChange={(e) => onChange(e.target.value)} data-testid={label}>
      {options.map((opt: any) => <option key={opt.value} value={opt.value}>{opt.label}</option>)}
    </select>
  </div>
));

describe('UniteEditor', () => {
  const mockInputSchemas: SchemaDefinition[] = [
    {
      id: 'schema1',
      name: 'customers',
      fields: [
        { id: 'c1', name: 'id', type: 'INTEGER', nullable: false, isKey: true },
        { id: 'c2', name: 'name', type:'STRING', nullable: true, isKey: false },
        { id: 'c3', name: 'age', type: 'INTEGER', nullable: true, isKey: false },
      ],
      isTemporary: false,
      isMaterialized: false,
    },
    {
      id: 'schema2',
      name: 'leads',
      fields: [
        { id: 'l1', name: 'lead_id', type: 'INTEGER', nullable: false, isKey: true },
        { id: 'l2', name: 'full_name', type: 'STRING', nullable: true, isKey: false },
        { id: 'l3', name: 'age', type: 'INTEGER', nullable: true, isKey: false },
      ],
      isTemporary: false,
      isMaterialized: false,
    },
  ];

  const mockOnSave = jest.fn();
  const mockOnClose = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('renders with multiple input schemas', () => {
    render(
      <UniteEditor
        nodeId="unite-node-1"
        nodeMetadata={{ name: 'Unite Node' } as any}
        inputSchemas={mockInputSchemas}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );

    expect(screen.getByText(/Union Mode/i)).toBeInTheDocument();
    expect(screen.getByText(/Schema Handling/i)).toBeInTheDocument();
    expect(screen.getByText(/Add Source Column/i)).toBeInTheDocument();
    // Should display input schema names
    expect(screen.getByText(/customers/i)).toBeInTheDocument();
    expect(screen.getByText(/leads/i)).toBeInTheDocument();
  });

  it('allows changing union mode', async () => {
    render(
      <UniteEditor
        nodeId="unite-node-1"
        nodeMetadata={{ name: 'Unite Node' } as any}
        inputSchemas={mockInputSchemas}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );

    const unionModeSelect = screen.getByTestId('Union Mode');
    await userEvent.selectOptions(unionModeSelect, 'DISTINCT');
    expect(unionModeSelect).toHaveValue('DISTINCT');
  });

  it('allows toggling source column and setting name/type', async () => {
    render(
      <UniteEditor
        nodeId="unite-node-1"
        nodeMetadata={{ name: 'Unite Node' } as any}
        inputSchemas={mockInputSchemas}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );

    const addSourceCheckbox = screen.getByLabelText(/Add Source Column/i);
    await userEvent.click(addSourceCheckbox);

    // Source column name input appears
    const nameInput = screen.getByLabelText(/Source Column Name/i);
    await userEvent.type(nameInput, 'source_id');

    const typeSelect = screen.getByTestId('Source Column Type');
    await userEvent.selectOptions(typeSelect, 'INTEGER');

    expect(nameInput).toHaveValue('source_id');
    expect(typeSelect).toHaveValue('INTEGER');
  });

  it('saves configuration with user selections', async () => {
    render(
      <UniteEditor
        nodeId="unite-node-1"
        nodeMetadata={{ name: 'Unite Node' } as any}
        inputSchemas={mockInputSchemas}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );

    // Change union mode to DISTINCT
    const unionModeSelect = screen.getByTestId('Union Mode');
    await userEvent.selectOptions(unionModeSelect, 'DISTINCT');

    // Change schema handling to strict
    const schemaHandlingSelect = screen.getByTestId('Schema Handling');
    await userEvent.selectOptions(schemaHandlingSelect, 'strict');

    // Enable source column
    const addSourceCheckbox = screen.getByLabelText(/Add Source Column/i);
    await userEvent.click(addSourceCheckbox);
    const sourceNameInput = screen.getByLabelText(/Source Column Name/i);
    await userEvent.type(sourceNameInput, 'origin');
    const sourceTypeSelect = screen.getByTestId('Source Column Type');
    await userEvent.selectOptions(sourceTypeSelect, 'STRING');

    // Click save
    const saveButton = screen.getByText(/Save Configuration/i);
    fireEvent.click(saveButton);

    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledTimes(1);
      const savedConfig = mockOnSave.mock.calls[0][0] as UniteComponentConfiguration;
      expect(savedConfig.unionMode).toBe('DISTINCT');
      expect(savedConfig.schemaHandling).toBe('strict');
      expect(savedConfig.addSourceColumn).toBe(true);
      expect(savedConfig.sourceColumnName).toBe('origin');
      expect(savedConfig.sourceColumnType).toBe('STRING');
    });
  });

  it('closes editor when cancel is clicked', () => {
    render(
      <UniteEditor
        nodeId="unite-node-1"
        nodeMetadata={{ name: 'Unite Node' } as any}
        inputSchemas={mockInputSchemas}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );
    const cancelButton = screen.getByText(/Cancel/i);
    fireEvent.click(cancelButton);
    expect(mockOnClose).toHaveBeenCalledTimes(1);
    expect(mockOnSave).not.toHaveBeenCalled();
  });

  it('handles edge case: no input schemas', () => {
    render(
      <UniteEditor
        nodeId="unite-node-1"
        nodeMetadata={{ name: 'Unite Node' } as any}
        inputSchemas={[]}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );
    expect(screen.getByText(/No input schemas provided/i)).toBeInTheDocument();
    const saveButton = screen.getByText(/Save Configuration/i);
    expect(saveButton).toBeDisabled();
  });

  it('loads initial configuration correctly', () => {
    const initialConfig: UniteComponentConfiguration = {
      version: '1.0',
      unionMode: 'DISTINCT',
      addSourceColumn: true,
      sourceColumnName: 'branch',
      sourceColumnType: 'STRING',
      schemaHandling: 'strict',
    };

    render(
      <UniteEditor
        nodeId="unite-node-1"
        nodeMetadata={{ name: 'Unite Node' } as any}
        inputSchemas={mockInputSchemas}
        initialConfig={initialConfig}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );

    expect(screen.getByTestId('Union Mode')).toHaveValue('DISTINCT');
    expect(screen.getByTestId('Schema Handling')).toHaveValue('strict');
    expect(screen.getByLabelText(/Add Source Column/i)).toBeChecked();
    expect(screen.getByLabelText(/Source Column Name/i)).toHaveValue('branch');
    expect(screen.getByTestId('Source Column Type')).toHaveValue('STRING');
  });

  it('validates source column name when addSourceColumn is enabled', async () => {
    render(
      <UniteEditor
        nodeId="unite-node-1"
        nodeMetadata={{ name: 'Unite Node' } as any}
        inputSchemas={mockInputSchemas}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );

    const addSourceCheckbox = screen.getByLabelText(/Add Source Column/i);
    await userEvent.click(addSourceCheckbox);
    const sourceNameInput = screen.getByLabelText(/Source Column Name/i);
    await userEvent.clear(sourceNameInput);
    // Name empty → save button disabled
    const saveButton = screen.getByText(/Save Configuration/i);
    expect(saveButton).toBeDisabled();

    await userEvent.type(sourceNameInput, 'valid_name');
    expect(saveButton).toBeEnabled();
  });
});