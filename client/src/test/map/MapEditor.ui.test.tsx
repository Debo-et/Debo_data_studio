// src/test/map/MapEditor.ui.test.tsx
import { render, screen, fireEvent, waitFor, within } from '@testing-library/react';
import '@testing-library/jest-dom';
import userEvent from '@testing-library/user-event';
import MapEditor from '../../components/Editor/Mapping/MapEditor';
import { MapComponentConfiguration } from '../../types/unified-pipeline.types';

// Mocks (unchanged)
jest.mock('reactflow', () => ({
  useReactFlow: jest.fn(),
  useStore: jest.fn(),
  getConnectedEdges: jest.fn(),
  MarkerType: {},
}));
jest.mock('../../hooks', () => ({
  useAppDispatch: jest.fn(),
  useAppSelector: jest.fn(),
}));
jest.mock('../../store/slices/logsSlice', () => ({
  addLog: jest.fn(),
}));
jest.mock('../../services/database-api.service', () => ({
  DatabaseApiService: jest.fn().mockImplementation(() => ({
    executeQuery: jest.fn().mockResolvedValue({ success: true, result: { rows: [] } }),
    createForeignTable: jest.fn().mockResolvedValue({ success: true, tableName: 'test_table' }),
  })),
}));

describe('MapEditor UI', () => {
  const mockInputColumns = [
    { name: 'first_name', type: 'string', id: 'col1' },
    { name: 'last_name', type: 'string', id: 'col2' },
    { name: 'age', type: 'integer', id: 'col3' },
  ];
  const mockOutputColumns = [
    { name: 'first_name', type: 'string', id: 'out1' },
    { name: 'last_name', type: 'string', id: 'out2' },
    { name: 'age', type: 'integer', id: 'out3' },
    { name: 'age_group', type: 'string', id: 'out4' },
  ];
  const mockOnSave = jest.fn();
  const mockOnClose = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
  });

  // Helper: get main input panel container
  const getInputPanel = () => {
    const headerSpan = screen.getAllByText('Input Table')[0];
    return headerSpan.closest('.border-r.bg-white') as HTMLElement;
  };

  // Helper: get main output panel container
  const getOutputPanel = () => {
    const headerSpan = screen.getAllByText('Output Table')[0];
    return headerSpan.closest('.bg-white.overflow-auto') as HTMLElement;
  };

  // Helper: get the output table inside the bottom schema comparison panel
  const getBottomOutputTable = () => {
    // The bottom panel contains exactly two tables (input schema and output schema)
    const tables = screen.getAllByRole('table');
    if (tables.length < 2) {
      throw new Error('Expected at least two tables in the Schema Comparison panel');
    }
    // The second table is the output schema table
    return tables[1];
  };

  it('renders correctly with input and output columns', () => {
    render(
      <MapEditor
        inputColumns={mockInputColumns}
        outputColumns={mockOutputColumns}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );
    expect(screen.getAllByText(/Input Table/i).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/Output Table/i).length).toBeGreaterThan(0);
    expect(screen.getAllByText('first_name').length).toBeGreaterThan(0);
    expect(screen.getAllByText('last_name').length).toBeGreaterThan(0);
    expect(screen.getAllByText('age').length).toBeGreaterThan(0);
    expect(screen.getAllByText('age_group').length).toBeGreaterThan(0);
  });

  it('allows manual mapping by clicking input column then output column', async () => {
    render(
      <MapEditor
        inputColumns={mockInputColumns}
        outputColumns={mockOutputColumns}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );
    const inputPanel = getInputPanel();
    const outputPanel = getOutputPanel();

    const inputFirstName = within(inputPanel).getByText('first_name');
    fireEvent.click(inputFirstName);
    expect(await screen.findByText(/Click an output column to map/i)).toBeInTheDocument();

    const outputFirstName = within(outputPanel).getByText('first_name');
    fireEvent.click(outputFirstName);

    await waitFor(() => {
      const outputRow = outputFirstName.closest('.border-b.border-gray-100') as HTMLElement;
      expect(outputRow.textContent).toContain('1 source');
    });
  });

  it('opens expression editor on double-click output column', async () => {
    render(
      <MapEditor
        inputColumns={mockInputColumns}
        outputColumns={mockOutputColumns}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );
    const bottomOutputTable = getBottomOutputTable();
    const ageRow = within(bottomOutputTable).getByText('age').closest('tr') as HTMLElement;
    fireEvent.doubleClick(ageRow);
    expect(await screen.findByText('Expression Editor')).toBeInTheDocument();
  });

  it('saves configuration on save (with manual mapping)', async () => {
    render(
      <MapEditor
        inputColumns={mockInputColumns}
        outputColumns={mockOutputColumns}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );
    const inputPanel = getInputPanel();
    const outputPanel = getOutputPanel();

    const inputFirstName = within(inputPanel).getByText('first_name');
    fireEvent.click(inputFirstName);
    const outputFirstName = within(outputPanel).getByText('first_name');
    fireEvent.click(outputFirstName);

    fireEvent.click(screen.getByText('Save & Compile'));

    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledTimes(1);
      const config = mockOnSave.mock.calls[0][0] as MapComponentConfiguration;
      expect(config.transformations).toHaveLength(1);
      expect(config.transformations[0].sourceField).toBe('first_name');
      expect(config.transformations[0].targetField).toBe('first_name');
      // The component auto‑generates an expression, so direct mapping becomes false
      expect(config.transformations[0].isDirectMapping).toBe(false);
      expect(config.outputSchema.fields).toHaveLength(4);
    });
  });

  it('auto-maps columns by name', async () => {
    render(
      <MapEditor
        inputColumns={mockInputColumns}
        outputColumns={mockOutputColumns}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );
    fireEvent.click(screen.getByText('Auto Map'));

    // Wait for the toolbar to show 3/4 mapped
    await waitFor(() => {
      const mappedCounter = screen.getByText(/Mapped: \d+\/4 \(\d+%\)/);
      expect(mappedCounter.textContent).toMatch(/3\/4/);
    });

    const outputPanel = getOutputPanel();
    const firstNameRow = within(outputPanel).getByText('first_name').closest('.border-b.border-gray-100');
    const lastNameRow = within(outputPanel).getByText('last_name').closest('.border-b.border-gray-100');
    const ageRow = within(outputPanel).getByText('age').closest('.border-b.border-gray-100');
    const ageGroupRow = within(outputPanel).getByText('age_group').closest('.border-b.border-gray-100');

    expect(firstNameRow?.textContent).toContain('1 source');
    expect(lastNameRow?.textContent).toContain('1 source');
    expect(ageRow?.textContent).toContain('1 source');
    expect(ageGroupRow?.textContent).toContain('Unmapped');
  });

    it('saves configuration with updated data type', async () => {
    render(
      <MapEditor
        inputColumns={mockInputColumns}
        outputColumns={mockOutputColumns}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );
    // Auto-map to create mappings
    fireEvent.click(screen.getByText('Auto Map'));

    // Wait for auto-map to complete (toolbar shows 3/4)
    await waitFor(() => {
      const mappedCounter = screen.getByText(/Mapped: \d+\/4 \(\d+%\)/);
      expect(mappedCounter.textContent).toMatch(/3\/4/);
    });

    // Change data type of 'age' in the bottom output table
    const bottomOutputTable = getBottomOutputTable();
    // There are two elements with text 'age': column name and expression.
    // The first one is the column name cell.
    const ageTextElement = within(bottomOutputTable).getAllByText('age')[0];
    const ageRow = ageTextElement.closest('tr') as HTMLElement;
    const select = within(ageRow).getByRole('combobox');
    await userEvent.selectOptions(select, 'VARCHAR');

    fireEvent.click(screen.getByText('Save & Compile'));

    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledTimes(1);
      const config = mockOnSave.mock.calls[0][0] as MapComponentConfiguration;
      const ageField = config.outputSchema.fields.find(f => f.name === 'age');
      expect(ageField).toBeDefined();
      // VARCHAR maps to DataType.STRING
      expect(ageField?.type).toBe('STRING');
    });
  });
});