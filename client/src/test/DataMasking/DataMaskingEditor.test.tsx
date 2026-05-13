// src/test/components/DataMaskingEditor.test.tsx
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import '@testing-library/jest-dom';
import DataMaskingEditor from '../../components/Editor/Parsing/DataMaskingEditor';
import { DataMaskingComponentConfiguration } from '../../types/unified-pipeline.types';

// Mock data source (input columns)
const mockInputColumns = [
  { name: 'email', type: 'STRING', id: 'col1' },
  { name: 'ssn', type: 'STRING', id: 'col2' },
  { name: 'credit_card', type: 'STRING', id: 'col3' },
  { name: 'phone', type: 'STRING', id: 'col4' },
  { name: 'name', type: 'STRING', id: 'col5' },
  { name: 'salary', type: 'INTEGER', id: 'col6' },
];

const mockNodeMetadata = {
  id: 'node-123',
  name: 'Mask Personal Data',
  type: 'DATA_MASKING',
};

const mockOnSave = jest.fn();
const mockOnClose = jest.fn();

describe('DataMaskingEditor', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('renders correctly with input columns and no initial rules', () => {
    render(
      <DataMaskingEditor
        nodeId="test-node"
        nodeMetadata={mockNodeMetadata as any}
        inputColumns={mockInputColumns}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );

    expect(screen.getByText(/Data Masking Editor/i)).toBeInTheDocument();
    expect(screen.getByText(/Mask Personal Data/i)).toBeInTheDocument();
    expect(screen.getByText(/6 input columns • 0 rules/i)).toBeInTheDocument();
    expect(screen.getByText(/No masking rules defined/i)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Add Rule/i })).toBeInTheDocument();
  });

  it('allows adding a new masking rule', async () => {
    render(
      <DataMaskingEditor
        nodeId="test-node"
        nodeMetadata={mockNodeMetadata as any}
        inputColumns={mockInputColumns}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );

    fireEvent.click(screen.getByRole('button', { name: /Add Rule/i }));

    // Now there should be a row with column selector
    expect(screen.getAllByRole('combobox')).toHaveLength(2); // column + masking type
    expect(screen.getByText(/Replace/i)).toBeInTheDocument(); // default masking type
  });

  it('allows selecting a column and masking type', async () => {
    render(
      <DataMaskingEditor
        nodeId="test-node"
        nodeMetadata={mockNodeMetadata as any}
        inputColumns={mockInputColumns}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );

    fireEvent.click(screen.getByRole('button', { name: /Add Rule/i }));

    // Select column 'email'
    const columnSelect = screen.getAllByRole('combobox')[0];
    await userEvent.click(columnSelect);
    await userEvent.click(screen.getByText('email'));

    // Change masking type to 'HASH'
    const typeSelect = screen.getAllByRole('combobox')[1];
    await userEvent.click(typeSelect);
    await userEvent.click(screen.getByText('Hash'));

    // Hash algorithm selector should appear
    expect(screen.getByText(/SHA256/i)).toBeInTheDocument();
  });

  it('renders correct parameters for each masking type', async () => {
    render(
      <DataMaskingEditor
        nodeId="test-node"
        nodeMetadata={mockNodeMetadata as any}
        inputColumns={mockInputColumns}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );

    fireEvent.click(screen.getByRole('button', { name: /Add Rule/i }));

    // REPLACE (default) → shows "Replace with:" input
    expect(screen.getByLabelText(/Replace with:/i)).toBeInTheDocument();

    // Change to RANDOM
    const typeSelect = screen.getAllByRole('combobox')[1];
    await userEvent.click(typeSelect);
    await userEvent.click(screen.getByText('Random'));
    expect(screen.getByText(/String/i)).toBeInTheDocument(); // random type selector
    expect(screen.getByDisplayValue('10')).toBeInTheDocument(); // length input

    // Change to CUSTOM
    await userEvent.click(typeSelect);
    await userEvent.click(screen.getByText('Custom Expression'));
    expect(screen.getByPlaceholderText(/SQL expression/i)).toBeInTheDocument();
  });

  it('removes a rule when clicking remove button', async () => {
    render(
      <DataMaskingEditor
        nodeId="test-node"
        nodeMetadata={mockNodeMetadata as any}
        inputColumns={mockInputColumns}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );

    fireEvent.click(screen.getByRole('button', { name: /Add Rule/i }));
    expect(screen.getAllByRole('row')).toHaveLength(2); // header + one rule

    const removeBtn = screen.getByRole('button', { name: /Remove rule/i });
    fireEvent.click(removeBtn);

    await waitFor(() => {
      expect(screen.queryByRole('row', { name: /email/i })).not.toBeInTheDocument();
      expect(screen.getByText(/No masking rules defined/i)).toBeInTheDocument();
    });
  });

  it('resets all rules when clicking Reset All', async () => {
    render(
      <DataMaskingEditor
        nodeId="test-node"
        nodeMetadata={mockNodeMetadata as any}
        inputColumns={mockInputColumns}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );

    // Add two rules
    fireEvent.click(screen.getByRole('button', { name: /Add Rule/i }));
    fireEvent.click(screen.getByRole('button', { name: /Add Rule/i }));
    expect(screen.getAllByRole('row')).toHaveLength(3); // header + 2 rules

    fireEvent.click(screen.getByRole('button', { name: /Reset All/i }));
    await waitFor(() => {
      expect(screen.getByText(/No masking rules defined/i)).toBeInTheDocument();
    });
  });

  it('saves configuration with correct structure', async () => {
    render(
      <DataMaskingEditor
        nodeId="test-node"
        nodeMetadata={mockNodeMetadata as any}
        inputColumns={mockInputColumns}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );

    // Add a rule for email → HASH
    fireEvent.click(screen.getByRole('button', { name: /Add Rule/i }));
    const columnSelect = screen.getAllByRole('combobox')[0];
    await userEvent.click(columnSelect);
    await userEvent.click(screen.getByText('email'));

    const typeSelect = screen.getAllByRole('combobox')[1];
    await userEvent.click(typeSelect);
    await userEvent.click(screen.getByText('Hash'));

    // Add a second rule for ssn → REPLACE
    fireEvent.click(screen.getByRole('button', { name: /Add Rule/i }));
    const columnSelect2 = screen.getAllByRole('combobox')[2]; // after second add
    await userEvent.click(columnSelect2);
    await userEvent.click(screen.getByText('ssn'));

    const typeSelect2 = screen.getAllByRole('combobox')[3];
    await userEvent.click(typeSelect2);
    await userEvent.click(screen.getByText('Replace'));

    // Save
    fireEvent.click(screen.getByRole('button', { name: /Save Configuration/i }));

    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledTimes(1);
      const savedConfig = mockOnSave.mock.calls[0][0] as DataMaskingComponentConfiguration;
      expect(savedConfig.rules).toHaveLength(2);
      expect(savedConfig.rules[0].column).toBe('email');
      expect(savedConfig.rules[0].maskingType).toBe('HASH');
      expect(savedConfig.rules[1].column).toBe('ssn');
      expect(savedConfig.rules[1].maskingType).toBe('REPLACE');
      expect(savedConfig.outputSchema.fields).toHaveLength(6); // all input columns present
      expect(savedConfig.compilerMetadata.validationStatus).toBe('VALID');
    });
  });

  it('shows warning when saving with no rules', async () => {
    render(
      <DataMaskingEditor
        nodeId="test-node"
        nodeMetadata={mockNodeMetadata as any}
        inputColumns={mockInputColumns}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );

    fireEvent.click(screen.getByRole('button', { name: /Save Configuration/i }));

    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalled();
      const savedConfig = mockOnSave.mock.calls[0][0] as DataMaskingComponentConfiguration;
      expect(savedConfig.compilerMetadata.validationStatus).toBe('WARNING');
      expect(savedConfig.compilerMetadata.warnings).toContain('No masking rules defined');
    });
  });

  it('closes when Cancel is clicked', () => {
    render(
      <DataMaskingEditor
        nodeId="test-node"
        nodeMetadata={mockNodeMetadata as any}
        inputColumns={mockInputColumns}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );
    fireEvent.click(screen.getByRole('button', { name: /Close/i }));
    expect(mockOnClose).toHaveBeenCalled();
  });
});