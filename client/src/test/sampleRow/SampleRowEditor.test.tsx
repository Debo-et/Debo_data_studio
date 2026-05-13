// src/test/editor/SampleRowEditor.test.tsx
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import SampleRowEditor from '../../components/Editor/Aggregates/SampleRowEditor';
import { SampleRowComponentConfiguration } from '../../types/unified-pipeline.types';

// Mock custom UI components (simplified)
jest.mock('../../ui/label', () => ({ Label, ...props }: any) => <label {...props} />);
jest.mock('../../ui/input', () => ({ Input, ...props }: any) => <input {...props} />);
jest.mock('../../ui/Button', () => ({ Button, children, ...props }: any) => (
  <button {...props}>{children}</button>
));
jest.mock('../../ui/switch', () => ({ Switch, ...props }: any) => (
  <input type="checkbox" role="switch" {...props} />
));
jest.mock('../../ui/select', () => ({
  Select: ({ children, ...props }: any) => <div {...props}>{children}</div>,
  SelectTrigger: ({ children, ...props }: any) => <button {...props}>{children}</button>,
  SelectValue: ({ placeholder }: any) => <span>{placeholder}</span>,
  SelectContent: ({ children }: any) => <div>{children}</div>,
  SelectItem: ({ children, value }: any) => <div data-testid={`select-item-${value}`}>{children}</div>,
}));
jest.mock('../../ui/card', () => ({
  Card: ({ children }: any) => <div className="card">{children}</div>,
  CardContent: ({ children }: any) => <div>{children}</div>,
  CardHeader: ({ children }: any) => <div>{children}</div>,
  CardTitle: ({ children }: any) => <div>{children}</div>,
}));

describe('SampleRowEditor', () => {
  const mockNodeId = 'sample-node-1';
  const mockNodeName = 'Sample Row Node';
  const mockInputColumns = [
    { name: 'id', type: 'integer' },
    { name: 'name', type: 'string' },
    { name: 'amount', type: 'decimal' },
  ];
  const mockOnSave = jest.fn();
  const mockOnClose = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('renders with default configuration', () => {
    render(
      <SampleRowEditor
        nodeId={mockNodeId}
        nodeName={mockNodeName}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    expect(screen.getByText(/Sample Row Configuration/i)).toBeInTheDocument();
    expect(screen.getByText(`Node: ${mockNodeName}`)).toBeInTheDocument();
    expect(screen.getByText('Sampling Method')).toBeInTheDocument();
    expect(screen.getByText('Number of rows')).toBeInTheDocument();
    const rowsInput = screen.getByLabelText('Number of rows');
    expect(rowsInput).toHaveValue(10);
    expect(screen.getByText('Input Schema')).toBeInTheDocument();
    expect(screen.getByText('id')).toBeInTheDocument();
    expect(screen.getByText('name')).toBeInTheDocument();
    expect(screen.getByText('amount')).toBeInTheDocument();
  });

  it('allows changing sampling method to percentage', async () => {
    render(
      <SampleRowEditor
        nodeId={mockNodeId}
        nodeName={mockNodeName}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    const methodSelect = screen.getByRole('button', { name: /select sampling method/i });
    fireEvent.click(methodSelect);
    const percentageOption = await screen.findByTestId('select-item-percentage');
    fireEvent.click(percentageOption);

    expect(screen.getByText('Percentage (%)')).toBeInTheDocument();
    const percentInput = screen.getByLabelText('Percentage (%)');
    expect(percentInput).toHaveValue(10);
    fireEvent.change(percentInput, { target: { value: '25.5' } });
    expect(percentInput).toHaveValue(25.5);
  });

  it('allows changing sampling method to random', async () => {
    render(
      <SampleRowEditor
        nodeId={mockNodeId}
        nodeName={mockNodeName}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    const methodSelect = screen.getByRole('button', { name: /select sampling method/i });
    fireEvent.click(methodSelect);
    const randomOption = await screen.findByTestId('select-item-random');
    fireEvent.click(randomOption);

    expect(screen.getByText('Sample size (rows)')).toBeInTheDocument();
    expect(screen.getByText('Random seed (optional)')).toBeInTheDocument();
    const seedInput = screen.getByLabelText('Random seed (optional)');
    fireEvent.change(seedInput, { target: { value: '123' } });
    expect(seedInput).toHaveValue(123);
  });

  it('toggles advanced options', () => {
    render(
      <SampleRowEditor
        nodeId={mockNodeId}
        nodeName={mockNodeName}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    const ignoreEmptySwitch = screen.getByLabelText('Ignore empty rows');
    const includeHeaderSwitch = screen.getByLabelText('Include header row (if present)');

    expect(ignoreEmptySwitch).not.toBeChecked();
    expect(includeHeaderSwitch).toBeChecked(); // default true

    fireEvent.click(ignoreEmptySwitch);
    expect(ignoreEmptySwitch).toBeChecked();

    fireEvent.click(includeHeaderSwitch);
    expect(includeHeaderSwitch).not.toBeChecked();
  });

  it('saves configuration with firstRows method', async () => {
    render(
      <SampleRowEditor
        nodeId={mockNodeId}
        nodeName={mockNodeName}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    const rowsInput = screen.getByLabelText('Number of rows');
    fireEvent.change(rowsInput, { target: { value: '50' } });
    const saveBtn = screen.getByText('Save Configuration');
    fireEvent.click(saveBtn);

    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledTimes(1);
      const savedConfig = mockOnSave.mock.calls[0][0] as SampleRowComponentConfiguration;
      expect(savedConfig.samplingMethod).toBe('firstRows');
      expect(savedConfig.sampleValue).toBe(50);
      expect(savedConfig.ignoreEmptyRows).toBe(false);
      expect(savedConfig.includeHeader).toBe(true);
      expect(savedConfig.outputSchema.fields).toHaveLength(mockInputColumns.length);
      expect(savedConfig.compilerMetadata.lastModified).toBeDefined();
      expect(savedConfig.compilerMetadata.validationStatus).toBe('VALID');
    });
  });

  it('saves configuration with percentage method', async () => {
    render(
      <SampleRowEditor
        nodeId={mockNodeId}
        nodeName={mockNodeName}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    const methodSelect = screen.getByRole('button', { name: /select sampling method/i });
    fireEvent.click(methodSelect);
    const percentageOption = await screen.findByTestId('select-item-percentage');
    fireEvent.click(percentageOption);

    const percentInput = screen.getByLabelText('Percentage (%)');
    fireEvent.change(percentInput, { target: { value: '12.5' } });
    fireEvent.click(screen.getByText('Save Configuration'));

    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledWith(
        expect.objectContaining({
          samplingMethod: 'percentage',
          sampleValue: 12.5,
        })
      );
    });
  });

  it('saves configuration with random method and seed', async () => {
    render(
      <SampleRowEditor
        nodeId={mockNodeId}
        nodeName={mockNodeName}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    const methodSelect = screen.getByRole('button', { name: /select sampling method/i });
    fireEvent.click(methodSelect);
    const randomOption = await screen.findByTestId('select-item-random');
    fireEvent.click(randomOption);

    const sampleSize = screen.getByLabelText('Sample size (rows)');
    fireEvent.change(sampleSize, { target: { value: '200' } });
    const seedInput = screen.getByLabelText('Random seed (optional)');
    fireEvent.change(seedInput, { target: { value: '999' } });
    fireEvent.click(screen.getByText('Save Configuration'));

    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledWith(
        expect.objectContaining({
          samplingMethod: 'random',
          sampleValue: 200,
          randomSeed: 999,
        })
      );
    });
  });

  it('calls onClose when cancel button clicked', () => {
    render(
      <SampleRowEditor
        nodeId={mockNodeId}
        nodeName={mockNodeName}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );
    fireEvent.click(screen.getByText('Cancel'));
    expect(mockOnClose).toHaveBeenCalledTimes(1);
    expect(mockOnSave).not.toHaveBeenCalled();
  });

  it('displays warnings from initial config', () => {
    const initialConfig: SampleRowComponentConfiguration = {
      version: '1.0',
      samplingMethod: 'firstRows',
      sampleValue: 10,
      ignoreEmptyRows: false,
      includeHeader: true,
      outputSchema: {
        id: 'out',
        name: 'Output',
        fields: [],
        isTemporary: false,
        isMaterialized: false,
      },
      sqlGeneration: { estimatedRowMultiplier: 0.1 },
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        createdBy: 'test',
        validationStatus: 'WARNING',
        warnings: ['Sample size too large', 'No columns selected'],
        dependencies: [],
      },
    };

    render(
      <SampleRowEditor
        nodeId={mockNodeId}
        nodeName={mockNodeName}
        inputColumns={mockInputColumns}
        initialConfig={initialConfig}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    expect(screen.getByText(/Warnings:/i)).toBeInTheDocument();
    expect(screen.getByText('Sample size too large')).toBeInTheDocument();
    expect(screen.getByText('No columns selected')).toBeInTheDocument();
  });
});