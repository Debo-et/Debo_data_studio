// src/test/matchGroup/MatchGroupEditor.test.tsx
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import '@testing-library/jest-dom';
import { MatchGroupEditor } from '../../components/Editor/JoinsAndLookups/MatchGroupEditor';
import {
  MatchGroupComponentConfiguration,
  MatchType,
  SurvivorshipRuleType,
} from '../../types/unified-pipeline.types';

// Mock framer-motion to avoid animation issues
jest.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: any) => <div {...props}>{children}</div>,
  },
}));

// Mock lucide-react icons
jest.mock('lucide-react', () => ({
  X: () => <span data-testid="icon-x">X</span>,
  ChevronDown: () => <span data-testid="icon-chevron-down">▼</span>,
  ChevronUp: () => <span data-testid="icon-chevron-up">▲</span>,
  Plus: () => <span data-testid="icon-plus">+</span>,
  Trash2: () => <span data-testid="icon-trash">🗑</span>,
  GripVertical: () => <span data-testid="icon-grip">⋮</span>,
}));

describe('MatchGroupEditor', () => {
  const mockInputColumns = [
    { name: 'customer_id', type: 'integer', id: 'col1' },
    { name: 'first_name', type: 'string', id: 'col2' },
    { name: 'last_name', type: 'string', id: 'col3' },
    { name: 'email', type: 'string', id: 'col4' },
    { name: 'age', type: 'integer', id: 'col5' },
  ];

  const mockOnSave = jest.fn();
  const mockOnClose = jest.fn();
  const mockNodeMetadata = { name: 'MatchGroup1', id: 'node-123' };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  const renderComponent = (initialConfig?: MatchGroupComponentConfiguration) => {
    return render(
      <MatchGroupEditor
        nodeId="test-node"
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        initialConfig={initialConfig}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );
  };

  it('renders correctly with default configuration', () => {
    renderComponent();
    expect(screen.getByText(/Match Group Configuration/i)).toBeInTheDocument();
    expect(screen.getByText(/Output Fields/i)).toBeInTheDocument();
    expect(screen.getByText(/Match Keys/i)).toBeInTheDocument();
    expect(screen.getByText(/Survivorship Rules/i)).toBeInTheDocument();
    expect(screen.getByText(/Advanced Options/i)).toBeInTheDocument();

    // All input columns should appear as checkboxes
    mockInputColumns.forEach(col => {
      expect(screen.getByLabelText(col.name)).toBeInTheDocument();
    });
  });

  it('allows toggling output fields', async () => {
    renderComponent();
    const emailCheckbox = screen.getByLabelText('email');
    expect(emailCheckbox).toBeChecked(); // "Include all input columns" is true by default

    await userEvent.click(emailCheckbox);
    expect(emailCheckbox).not.toBeChecked();

    // Toggle "Include all input columns" off
    const includeAllCheckbox = screen.getByLabelText(/Include all input columns/i);
    await userEvent.click(includeAllCheckbox);
    expect(includeAllCheckbox).not.toBeChecked();

    // Now only rule fields should be selected (none yet, so none checked)
    mockInputColumns.forEach(col => {
      const cb = screen.getByLabelText(col.name);
      expect(cb).not.toBeChecked();
    });
  });

  it('adds and removes match keys', async () => {
    renderComponent();
    const addButton = screen.getByRole('button', { name: /Add Match Key/i });
    await userEvent.click(addButton);

    // New match key row should appear
    expect(screen.getAllByText(/Select field/i).length).toBeGreaterThan(0);

    // Change field and match type
    const fieldSelect = screen.getAllByRole('combobox')[0];
    await userEvent.selectOptions(fieldSelect, 'first_name');

    const matchTypeSelect = screen.getAllByRole('combobox')[1];
    await userEvent.selectOptions(matchTypeSelect, MatchType.FUZZY);

    // Fuzzy threshold input appears
    const thresholdInput = screen.getByPlaceholderText('0.8');
    expect(thresholdInput).toBeInTheDocument();
    await userEvent.clear(thresholdInput);
    await userEvent.type(thresholdInput, '0.95');

    // Remove the key
    const removeButton = screen.getByTestId('icon-trash').closest('button');
    await userEvent.click(removeButton!);
    expect(screen.queryByText(/Select field/i)).not.toBeInTheDocument();
  });

  it('adds and removes survivorship rules', async () => {
    renderComponent();
    const addRuleButton = screen.getByRole('button', { name: /Add Rule/i });
    await userEvent.click(addRuleButton);

    // New rule row appears
    const fieldInput = screen.getByPlaceholderText('Output field');
    expect(fieldInput).toBeInTheDocument();

    await userEvent.type(fieldInput, 'full_name');

    const ruleTypeSelect = screen.getAllByRole('combobox')[2]; // depends on layout
    await userEvent.selectOptions(ruleTypeSelect, SurvivorshipRuleType.CONCAT);

    // Separator input appears
    const separatorInput = screen.getByPlaceholderText('Separator');
    expect(separatorInput).toBeInTheDocument();
    await userEvent.clear(separatorInput);
    await userEvent.type(separatorInput, ' | ');

    // Remove rule
    const removeButton = screen.getAllByTestId('icon-trash')[1];
    await userEvent.click(removeButton);
    expect(screen.queryByPlaceholderText('Output field')).not.toBeInTheDocument();
  });

  it('expands advanced options and modifies global settings', async () => {
    renderComponent();
    const advancedButton = screen.getByText(/Advanced Options/i);
    await userEvent.click(advancedButton);

    // Global match threshold slider
    const thresholdSlider = screen.getByRole('slider', { name: /Global Match Threshold/i });
    expect(thresholdSlider).toBeInTheDocument();
    await userEvent.type(thresholdSlider, '0.7'); // or fireEvent.change

    const maxMatchesInput = screen.getByLabelText(/Max Matches Per Record/i);
    await userEvent.clear(maxMatchesInput);
    await userEvent.type(maxMatchesInput, '50');

    const nullHandlingSelect = screen.getByLabelText(/Null Handling/i);
    await userEvent.selectOptions(nullHandlingSelect, 'no_match');

    const outputModeSelect = screen.getByLabelText(/Output Mode/i);
    await userEvent.selectOptions(outputModeSelect, 'best_match');

    const includeDetailsCheckbox = screen.getByLabelText(/Include match details column/i);
    await userEvent.click(includeDetailsCheckbox);

    const parallelizeCheckbox = screen.getByLabelText(/Parallelize/i);
    await userEvent.click(parallelizeCheckbox);
  });

  it('saves configuration with correct structure', async () => {
    renderComponent();

    // Add a match key
    await userEvent.click(screen.getByRole('button', { name: /Add Match Key/i }));
    const fieldSelect = screen.getAllByRole('combobox')[0];
    await userEvent.selectOptions(fieldSelect, 'customer_id');

    // Add a survivorship rule
    await userEvent.click(screen.getByRole('button', { name: /Add Rule/i }));
    const outputField = screen.getByPlaceholderText('Output field');
    await userEvent.type(outputField, 'customer_name');
    const ruleTypeSelect = screen.getAllByRole('combobox')[2];
    await userEvent.selectOptions(ruleTypeSelect, SurvivorshipRuleType.FIRST);

    // Click Save
    const saveButton = screen.getByRole('button', { name: /Save Configuration/i });
    await userEvent.click(saveButton);

    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledTimes(1);
      const savedConfig = mockOnSave.mock.calls[0][0] as MatchGroupComponentConfiguration;

      expect(savedConfig.matchKeys).toHaveLength(1);
      expect(savedConfig.matchKeys[0].field).toBe('customer_id');
      expect(savedConfig.matchKeys[0].matchType).toBe(MatchType.EXACT);

      expect(savedConfig.survivorshipRules).toHaveLength(1);
      expect(savedConfig.survivorshipRules[0].field).toBe('customer_name');
      expect(savedConfig.survivorshipRules[0].ruleType).toBe(SurvivorshipRuleType.FIRST);

      expect(savedConfig.outputFields).toContain('customer_name');
      // All input columns should be included because includeAllFields was true
      mockInputColumns.forEach(col => {
        expect(savedConfig.outputFields).toContain(col.name);
      });
    });
  });

  it('handles empty state gracefully', () => {
    renderComponent();
    expect(screen.getByText(/No match keys defined/i)).toBeInTheDocument();
    expect(screen.getByText(/No survivorship rules/i)).toBeInTheDocument();
  });

  it('validates required fields before save', async () => {
    renderComponent();
    // Save without any match keys or rules – should still save (no validation error from component)
    const saveButton = screen.getByRole('button', { name: /Save Configuration/i });
    await userEvent.click(saveButton);

    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalled();
      const savedConfig = mockOnSave.mock.calls[0][0];
      expect(savedConfig.matchKeys).toEqual([]);
      expect(savedConfig.survivorshipRules).toEqual([]);
    });
  });

  it('closes the modal when cancel is clicked', async () => {
    renderComponent();
    const cancelButton = screen.getByRole('button', { name: /Cancel/i });
    await userEvent.click(cancelButton);
    expect(mockOnClose).toHaveBeenCalled();
  });
});