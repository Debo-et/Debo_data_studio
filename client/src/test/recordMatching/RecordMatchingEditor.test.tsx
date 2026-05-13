// src/test/components/RecordMatchingEditor.test.tsx
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import '@testing-library/jest-dom';
import { RecordMatchingEditor } from '../../components/Editor/JoinsAndLookups/RecordMatchingEditor';
import { 
  MatchType, 
  SurvivorshipRuleType, 
  MatchGroupComponentConfiguration,
  FieldSchema 
} from '../../types/unified-pipeline.types';

// Mock external dependencies that might cause side effects
jest.mock('../../ui/Button', () => ({ Button: (props: any) => <button {...props} /> }));
jest.mock('../../ui/input', () => ({ Input: (props: any) => <input {...props} /> }));
jest.mock('../../ui/label', () => ({ Label: (props: any) => <label {...props} /> }));
jest.mock('../../ui/checkbox', () => ({ Checkbox: (props: any) => <input type="checkbox" {...props} /> }));
jest.mock('../../ui/select', () => ({
  Select: ({ children, ...props }: any) => <select {...props}>{children}</select>,
  SelectTrigger: (props: any) => <button {...props} />,
  SelectValue: (props: any) => <span {...props} />,
  SelectContent: ({ children }: any) => <div>{children}</div>,
  SelectItem: ({ children, value }: any) => <option value={value}>{children}</option>,
}));
jest.mock('../../ui/slider', () => ({ Slider: (props: any) => <input type="range" {...props} /> }));
jest.mock('../../ui/badge', () => ({ Badge: (props: any) => <span {...props} /> }));

describe('RecordMatchingEditor', () => {
  // Mock data
  const mockInputFields: FieldSchema[] = [
    { id: 'f1', name: 'id', type: 'INTEGER', nullable: false, isKey: true },
    { id: 'f2', name: 'full_name', type: 'STRING', nullable: true, isKey: false },
    { id: 'f3', name: 'email', type: 'STRING', nullable: false, isKey: false },
    { id: 'f4', name: 'phone', type: 'STRING', nullable: true, isKey: false },
  ];

  const mockInitialConfig: MatchGroupComponentConfiguration = {
    version: '1.0',
    matchKeys: [
      {
        id: 'mk1',
        field: 'email',
        matchType: MatchType.EXACT_IGNORE_CASE,
        threshold: 0.9,
        caseSensitive: false,
        ignoreNull: true,
        weight: 1.5,
        blockingKey: true,
      },
    ],
    survivorshipRules: [
      {
        id: 'sr1',
        field: 'full_name',
        ruleType: SurvivorshipRuleType.FIRST,
        sourceField: 'full_name',
        params: { orderBy: 'id', orderDirection: 'ASC' },
      },
    ],
    outputFields: ['id', 'full_name', 'email'],
    globalOptions: {
      matchThreshold: 0.8,
      maxMatchesPerRecord: 1,
      nullHandling: 'no_match',
      outputMode: 'best_match',
      includeMatchDetails: false,
      parallelization: false,
      batchSize: 10000,
    },
    sqlGeneration: {},
    compilerMetadata: {
      lastModified: new Date().toISOString(),
      createdBy: 'test',
      matchKeyCount: 1,
      ruleCount: 1,
      validationStatus: 'VALID',
      warnings: [],
      dependencies: [],
    },
  };

  const mockOnSave = jest.fn();
  const mockOnClose = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
  });

  // ---------- Basic Rendering ----------
  it('renders correctly with input fields and no initial config', () => {
    render(
      <RecordMatchingEditor
        inputFields={mockInputFields}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );

    expect(screen.getByText(/Record Matching Editor/i)).toBeInTheDocument();
    expect(screen.getByText('Input Schema')).toBeInTheDocument();
    expect(screen.getByText('Output Fields')).toBeInTheDocument();
    expect(screen.getByText('Matching Keys')).toBeInTheDocument();
    expect(screen.getByText('Survivorship Rules')).toBeInTheDocument();
    expect(screen.getByText('Advanced')).toBeInTheDocument();
    
    // All input fields should be visible
    expect(screen.getByText('id')).toBeInTheDocument();
    expect(screen.getByText('full_name')).toBeInTheDocument();
    expect(screen.getByText('email')).toBeInTheDocument();
    expect(screen.getByText('phone')).toBeInTheDocument();
  });

  it('loads initial configuration when provided', () => {
    render(
      <RecordMatchingEditor
        inputFields={mockInputFields}
        initialConfig={mockInitialConfig}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );

    // Match key should be pre-filled
    expect(screen.getByDisplayValue('email')).toBeInTheDocument();
    expect(screen.getByDisplayValue(MatchType.EXACT_IGNORE_CASE)).toBeInTheDocument();
    
    // Survivorship rule should be present
    expect(screen.getByDisplayValue('full_name')).toBeInTheDocument();
    expect(screen.getByDisplayValue(SurvivorshipRuleType.FIRST)).toBeInTheDocument();
    
    // Output fields should be pre-selected (checkboxes checked)
    const idCheckbox = screen.getByLabelText('id') as HTMLInputElement;
    const emailCheckbox = screen.getByLabelText('email') as HTMLInputElement;
    expect(idCheckbox.checked).toBe(true);
    expect(emailCheckbox.checked).toBe(true);
  });

  // ---------- Match Keys ----------
  it('allows adding a new match key', async () => {
    render(
      <RecordMatchingEditor
        inputFields={mockInputFields}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );

    const addKeyBtn = screen.getByRole('button', { name: /Add Key/i });
    fireEvent.click(addKeyBtn);

    // New key row appears with default values
    const fieldSelects = screen.getAllByRole('combobox');
    // There should be at least one new select for the key field
    expect(fieldSelects.length).toBeGreaterThan(0);
  });

  it('allows updating a match key field and match type', async () => {
    render(
      <RecordMatchingEditor
        inputFields={mockInputFields}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );

    // Add a key first
    fireEvent.click(screen.getByRole('button', { name: /Add Key/i }));
    
    // Find the first select (field selection) and change it
    const fieldSelect = screen.getAllByRole('combobox')[0];
    await userEvent.selectOptions(fieldSelect, 'email');
    expect(fieldSelect).toHaveValue('email');

    // Find match type select (second combobox)
    const typeSelect = screen.getAllByRole('combobox')[1];
    await userEvent.selectOptions(typeSelect, MatchType.FUZZY);
    expect(typeSelect).toHaveValue(MatchType.FUZZY);
  });

  it('allows removing a match key', () => {
    render(
      <RecordMatchingEditor
        inputFields={mockInputFields}
        initialConfig={mockInitialConfig}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );

    // Initially one match key exists
    expect(screen.getByDisplayValue('email')).toBeInTheDocument();
    
    // Find remove button (trash icon button) – it's opacity-0 group-hover, but we can click anyway
    const removeBtns = screen.getAllByRole('button', { name: /trash/i });
    fireEvent.click(removeBtns[0]);
    
    // The key should be gone
    expect(screen.queryByDisplayValue('email')).not.toBeInTheDocument();
  });

  // ---------- Survivorship Rules ----------
  it('allows adding a survivorship rule', () => {
    render(
      <RecordMatchingEditor
        inputFields={mockInputFields}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );

    fireEvent.click(screen.getByRole('button', { name: /Add Rule/i }));
    // A new rule row appears; we can verify by checking for output field select
    const outputFieldSelects = screen.getAllByRole('combobox').filter(
      (el) => (el as HTMLSelectElement).name !== 'matchType'
    );
    expect(outputFieldSelects.length).toBeGreaterThan(0);
  });

  it('allows updating survivorship rule type and parameters', async () => {
    render(
      <RecordMatchingEditor
        inputFields={mockInputFields}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );

    // Add a rule
    fireEvent.click(screen.getByRole('button', { name: /Add Rule/i }));
    
    // Find the rule type select (third combobox typically)
    const ruleTypeSelects = screen.getAllByRole('combobox');
    // The rule type select is after field and match type in the row
    const ruleTypeSelect = ruleTypeSelects[2];
    await userEvent.selectOptions(ruleTypeSelect, SurvivorshipRuleType.CONCAT);
    expect(ruleTypeSelect).toHaveValue(SurvivorshipRuleType.CONCAT);
    
    // When rule type is CONCAT, a separator input appears
    expect(screen.getByPlaceholderText('Separator')).toBeInTheDocument();
  });

  it('allows removing a survivorship rule', () => {
    render(
      <RecordMatchingEditor
        inputFields={mockInputFields}
        initialConfig={mockInitialConfig}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );

    expect(screen.getByDisplayValue('full_name')).toBeInTheDocument();
    const removeBtns = screen.getAllByRole('button', { name: /trash/i });
    // The first trash is for match key, second for rule
    fireEvent.click(removeBtns[1]);
    expect(screen.queryByDisplayValue('full_name')).not.toBeInTheDocument();
  });

  // ---------- Output Fields Selection ----------
  it('allows toggling output fields via checkboxes', () => {
    render(
      <RecordMatchingEditor
        inputFields={mockInputFields}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );

    const idCheckbox = screen.getByLabelText('id') as HTMLInputElement;
    expect(idCheckbox.checked).toBe(false);
    fireEvent.click(idCheckbox);
    expect(idCheckbox.checked).toBe(true);
    
    // The Output Fields panel should reflect the selection (the selected field appears in the right panel)
    expect(screen.getByText('id')).toBeInTheDocument();
  });

  // ---------- Auto-Configure ----------
  it('auto-configures match keys and rules based on common field names', () => {
    render(
      <RecordMatchingEditor
        inputFields={mockInputFields}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );

    fireEvent.click(screen.getByRole('button', { name: /Auto‑configure/i }));
    
    // Should have created match keys for 'id' and 'email' (common key names)
    // And selected all output fields
    const idCheckbox = screen.getByLabelText('id') as HTMLInputElement;
    const emailCheckbox = screen.getByLabelText('email') as HTMLInputElement;
    expect(idCheckbox.checked).toBe(true);
    expect(emailCheckbox.checked).toBe(true);
    
    // Survivorship rules for each field should exist
    expect(screen.getAllByDisplayValue(SurvivorshipRuleType.FIRST).length).toBeGreaterThan(0);
  });

  // ---------- Advanced Options ----------
  it('updates global match threshold slider', () => {
    render(
      <RecordMatchingEditor
        inputFields={mockInputFields}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );

    const slider = screen.getByRole('slider', { name: /match threshold/i }) as HTMLInputElement;
    // Slider is rendered as input range; we can change its value
    fireEvent.change(slider, { target: { value: '0.65' } });
    expect(slider.value).toBe('0.65');
  });

  it('updates output mode and null handling', async () => {
    render(
      <RecordMatchingEditor
        inputFields={mockInputFields}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );

    // Switch to Advanced tab
    fireEvent.click(screen.getByText('Advanced'));
    
    const outputModeSelect = screen.getByLabelText(/Output Mode/i) as HTMLSelectElement;
    await userEvent.selectOptions(outputModeSelect, 'all_matches');
    expect(outputModeSelect.value).toBe('all_matches');
    
    const nullHandlingSelect = screen.getByLabelText(/Null Handling/i) as HTMLSelectElement;
    await userEvent.selectOptions(nullHandlingSelect, 'match');
    expect(nullHandlingSelect.value).toBe('match');
  });

  // ---------- Validation & Warnings ----------
  it('shows validation warnings when no match keys defined', async () => {
    render(
      <RecordMatchingEditor
        inputFields={mockInputFields}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );
    
    // Initially no match keys, no output fields selected -> warning
    await waitFor(() => {
      expect(screen.getByText(/No matching keys defined/i)).toBeInTheDocument();
      expect(screen.getByText(/No output fields selected/i)).toBeInTheDocument();
    });
  });

  it('shows warning when output field missing survivorship rule', async () => {
    // Provide some output fields but no rules
    const configWithOutputOnly: MatchGroupComponentConfiguration = {
      ...mockInitialConfig,
      matchKeys: [{ id: 'mk1', field: 'email', matchType: MatchType.EXACT, caseSensitive: false, ignoreNull: true, threshold: 0.8, weight: 1, blockingKey: false }],
      survivorshipRules: [], // no rules
      outputFields: ['email', 'full_name'],
    };
    
    render(
      <RecordMatchingEditor
        inputFields={mockInputFields}
        initialConfig={configWithOutputOnly}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );
    
    await waitFor(() => {
      expect(screen.getByText(/output fields missing survivorship rules/i)).toBeInTheDocument();
    });
  });

  // ---------- Save Callback ----------
  it('calls onSave with complete configuration when save button clicked', async () => {
    render(
      <RecordMatchingEditor
        inputFields={mockInputFields}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );
    
    // Auto-configure to set up valid state
    fireEvent.click(screen.getByRole('button', { name: /Auto‑configure/i }));
    
    // Change some advanced options
    fireEvent.click(screen.getByText('Advanced'));
    const thresholdSlider = screen.getByRole('slider', { name: /match threshold/i });
    fireEvent.change(thresholdSlider, { target: { value: '0.75' } });
    
    const saveBtn = screen.getByRole('button', { name: /Save & Compile/i });
    fireEvent.click(saveBtn);
    
    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledTimes(1);
      const savedConfig = mockOnSave.mock.calls[0][0] as MatchGroupComponentConfiguration;
      expect(savedConfig.matchKeys.length).toBeGreaterThan(0);
      expect(savedConfig.survivorshipRules.length).toBeGreaterThan(0);
      expect(savedConfig.outputFields.length).toBeGreaterThan(0);
      expect(savedConfig.globalOptions.matchThreshold).toBe(0.75);
      expect(savedConfig.compilerMetadata.validationStatus).toBe('VALID');
    });
  });

  it('disables save button when validation has errors', () => {
    render(
      <RecordMatchingEditor
        inputFields={mockInputFields}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );
    // No match keys, no output fields -> ERROR
    const saveBtn = screen.getByRole('button', { name: /Save & Compile/i });
    expect(saveBtn).toBeDisabled();
  });

  it('calls onClose when cancel button clicked', () => {
    render(
      <RecordMatchingEditor
        inputFields={mockInputFields}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );
    const cancelBtn = screen.getByRole('button', { name: /Cancel/i });
    fireEvent.click(cancelBtn);
    expect(mockOnClose).toHaveBeenCalledTimes(1);
  });

  // ---------- Edge Cases ----------
  it('handles empty input fields gracefully', () => {
    render(
      <RecordMatchingEditor
        inputFields={[]}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );
    expect(screen.getByText(/Input Schema/i)).toBeInTheDocument();
    expect(screen.getByText(/No matching keys defined/i)).toBeInTheDocument();
    // Auto-configure should not crash
    fireEvent.click(screen.getByRole('button', { name: /Auto‑configure/i }));
    // No changes, but no error
    expect(screen.getByText(/No matching keys defined/i)).toBeInTheDocument();
  });

  it('handles missing initial config gracefully', () => {
    render(
      <RecordMatchingEditor
        inputFields={mockInputFields}
        initialConfig={undefined}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );
    // Should render with default empty state
    expect(screen.getByText(/No matching keys defined/i)).toBeInTheDocument();
  });
});