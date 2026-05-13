import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import NormalizeNumberEditor from '../../components/Editor/Mapping/NormalizeNumberEditor';
import {
  NormalizeNumberComponentConfiguration,
  PostgreSQLDataType,
} from '../../types/unified-pipeline.types';

// Mock framer-motion to avoid animation issues in tests
jest.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: any) => <div {...props}>{children}</div>,
  },
}));

describe('NormalizeNumberEditor', () => {
  const mockInputColumns = [
    { name: 'sales', type: 'double', id: 'col1' },
    { name: 'quantity', type: 'int', id: 'col2' },
    { name: 'discount', type: 'float', id: 'col3' },
  ];

  const mockOnSave = jest.fn();
  const mockOnClose = jest.fn();
  const mockNodeMetadata = { name: 'Normalize Node', id: 'node-123' };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  const renderEditor = (initialConfig?: NormalizeNumberComponentConfiguration) => {
    return render(
      <NormalizeNumberEditor
        nodeId="test-node"
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        initialConfig={initialConfig}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );
  };

  it('renders correctly with column list', () => {
    renderEditor();
    expect(screen.getByText(/Normalize Number/i)).toBeInTheDocument();
    expect(screen.getByText('sales')).toBeInTheDocument();
    expect(screen.getByText('quantity')).toBeInTheDocument();
    expect(screen.getByText('discount')).toBeInTheDocument();
    expect(screen.getByText('Global Settings')).toBeInTheDocument();
  });

  it('allows selecting a column and configures default rule', async () => {
    renderEditor();
    const salesCheckbox = screen.getByLabelText('sales');
    fireEvent.click(salesCheckbox);

    await waitFor(() => {
      expect(screen.getByText('sales')).toBeInTheDocument(); // rule header
      const methodSelect = screen.getByDisplayValue('minmax');
      expect(methodSelect).toBeInTheDocument();
    });
  });

  it('removes a column when unchecking', async () => {
    renderEditor();
    const salesCheckbox = screen.getByLabelText('sales');
    fireEvent.click(salesCheckbox); // select
    fireEvent.click(salesCheckbox); // deselect

    await waitFor(() => {
      expect(screen.queryByText('sales')).not.toBeInTheDocument(); // rule header gone
    });
  });

  it('selects all columns via "Select All" button', () => {
    renderEditor();
    const selectAllBtn = screen.getByText('Select All');
    fireEvent.click(selectAllBtn);

    mockInputColumns.forEach(col => {
      expect(screen.getByLabelText(col.name)).toBeChecked();
    });
    // Should have three rule panels
    expect(screen.getAllByText(/Min‑Max/).length).toBe(3);
  });

  it('clears all columns via "Clear All" button', () => {
    renderEditor();
    const selectAllBtn = screen.getByText('Select All');
    fireEvent.click(selectAllBtn);
    const clearAllBtn = screen.getByText('Clear All');
    fireEvent.click(clearAllBtn);

    mockInputColumns.forEach(col => {
      expect(screen.getByLabelText(col.name)).not.toBeChecked();
    });
    expect(screen.queryByText(/Min‑Max/)).not.toBeInTheDocument();
  });

  it('changes method for a column and shows appropriate parameters', async () => {
    renderEditor();
    const salesCheckbox = screen.getByLabelText('sales');
    fireEvent.click(salesCheckbox);

    const methodSelect = screen.getByDisplayValue('minmax');
    fireEvent.change(methodSelect, { target: { value: 'log' } });

    await waitFor(() => {
      expect(screen.getByText('Base')).toBeInTheDocument();
      const baseSelect = screen.getByDisplayValue('Natural (e)');
      expect(baseSelect).toBeInTheDocument();
    });

    // Change to custom
    fireEvent.change(methodSelect, { target: { value: 'custom' } });
    await waitFor(() => {
      expect(screen.getByPlaceholderText(/e\.g\., \({column} - 0\.5\) \* 2/)).toBeInTheDocument();
    });
  });

  it('updates min/max parameters for minmax method', async () => {
    renderEditor();
    const salesCheckbox = screen.getByLabelText('sales');
    fireEvent.click(salesCheckbox);

    const minInput = screen.getByLabelText('Min');
    const maxInput = screen.getByLabelText('Max');
    fireEvent.change(minInput, { target: { value: '10' } });
    fireEvent.change(maxInput, { target: { value: '200' } });

    // Save and verify config
    const saveBtn = screen.getByText('Save Configuration');
    fireEvent.click(saveBtn);

    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledTimes(1);
      const config = mockOnSave.mock.calls[0][0] as NormalizeNumberComponentConfiguration;
      const rule = config.rules.find(r => r.sourceColumn === 'sales');
      expect(rule?.parameters?.min).toBe(10);
      expect(rule?.parameters?.max).toBe(200);
    });
  });

  it('disables save button when validation fails (min >= max)', async () => {
    renderEditor();
    const salesCheckbox = screen.getByLabelText('sales');
    fireEvent.click(salesCheckbox);

    const minInput = screen.getByLabelText('Min');
    const maxInput = screen.getByLabelText('Max');
    fireEvent.change(minInput, { target: { value: '100' } });
    fireEvent.change(maxInput, { target: { value: '50' } });

    const saveBtn = screen.getByText('Save Configuration');
    expect(saveBtn).toBeDisabled();
    expect(screen.getByText(/⚠️ 1 warning/)).toBeInTheDocument();
  });

  it('disables save button when custom expression is empty', async () => {
    renderEditor();
    const salesCheckbox = screen.getByLabelText('sales');
    fireEvent.click(salesCheckbox);

    const methodSelect = screen.getByDisplayValue('minmax');
    fireEvent.change(methodSelect, { target: { value: 'custom' } });

    const textarea = screen.getByPlaceholderText(/e\.g\., \({column} - 0\.5\) \* 2/);
    expect(textarea).toHaveValue('');

    const saveBtn = screen.getByText('Save Configuration');
    expect(saveBtn).toBeDisabled();
  });

  it('applies global null handling and outlier handling to rules on save', async () => {
    renderEditor();
    // Select a column
    const salesCheckbox = screen.getByLabelText('sales');
    fireEvent.click(salesCheckbox);

    // Change global settings
    const nullHandlingSelect = screen.getByLabelText('Null Handling');
    fireEvent.change(nullHandlingSelect, { target: { value: 'DEFAULT_VALUE' } });

    const outlierHandlingSelect = screen.getByLabelText('Outlier Handling');
    fireEvent.change(outlierHandlingSelect, { target: { value: 'CLIP' } });

    const saveBtn = screen.getByText('Save Configuration');
    fireEvent.click(saveBtn);

    await waitFor(() => {
      const config = mockOnSave.mock.calls[0][0];
      expect(config.globalOptions.nullHandling).toBe('DEFAULT_VALUE');
      expect(config.globalOptions.outlierHandling).toBe('CLIP');
      const rule = config.rules[0];
      expect(rule.nullHandling).toBe('DEFAULT_VALUE');
      expect(rule.outlierHandling).toBe('CLIP');
    });
  });

  it('applies output naming strategy (suffix, prefix) to target columns', async () => {
    renderEditor();
    const salesCheckbox = screen.getByLabelText('sales');
    fireEvent.click(salesCheckbox);

    const namingSelect = screen.getByLabelText('Output Naming');
    fireEvent.change(namingSelect, { target: { value: 'suffix' } });
    const suffixInput = screen.getByLabelText('Suffix');
    fireEvent.change(suffixInput, { target: { value: '_norm' } });

    const saveBtn = screen.getByText('Save Configuration');
    fireEvent.click(saveBtn);

    await waitFor(() => {
      const config = mockOnSave.mock.calls[0][0];
      const rule = config.rules[0];
      expect(rule.targetColumn).toBe('sales_norm');
    });
  });

  it('loads initial configuration correctly', () => {
    const initialConfig: NormalizeNumberComponentConfiguration = {
      version: '1.0',
      rules: [
        {
          id: 'rule1',
          sourceColumn: 'sales',
          targetColumn: 'sales_scaled',
          method: 'minmax',
          parameters: { min: 0, max: 1000 },
          nullHandling: 'KEEP_NULL',
          outputDataType: PostgreSQLDataType.DOUBLE_PRECISION,
          position: 0,
        },
      ],
      globalOptions: {
        nullHandling: 'ERROR',
        outlierHandling: 'CLIP',
        defaultDataType: PostgreSQLDataType.REAL,
      },
      outputSchema: {} as any,
      sqlGeneration: {} as any,
      compilerMetadata: {} as any,
    };
    renderEditor(initialConfig);

    expect(screen.getByLabelText('sales')).toBeChecked();
    expect(screen.getByDisplayValue('minmax')).toBeInTheDocument();
    expect(screen.getByLabelText('Min')).toHaveValue(0);
    expect(screen.getByLabelText('Max')).toHaveValue(1000);

    const nullHandlingSelect = screen.getByLabelText('Null Handling');
    expect(nullHandlingSelect).toHaveValue('ERROR');
    const outlierHandlingSelect = screen.getByLabelText('Outlier Handling');
    expect(outlierHandlingSelect).toHaveValue('CLIP');
  });

  it('calls onClose when cancel button is clicked', () => {
    renderEditor();
    const cancelBtn = screen.getByText('Cancel');
    fireEvent.click(cancelBtn);
    expect(mockOnClose).toHaveBeenCalled();
  });
});