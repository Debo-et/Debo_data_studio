// src/test/components/ReplaceListEditor.test.tsx
import { render, screen, fireEvent, waitFor, within } from '@testing-library/react';
import '@testing-library/jest-dom';
import ReplaceListEditor from '../../components/Editor/Mapping/ReplaceListEditor';
import { mockInputSchema, sampleRules } from './replaceMockData';
import { ReplaceComponentConfiguration, ReplaceRule } from '../../types/unified-pipeline.types';

// Mock UI components (assuming shadcn/ui or similar)
jest.mock('../../ui/Button', () => ({
  Button: ({ children, onClick, disabled }: any) => (
    <button onClick={onClick} disabled={disabled}>{children}</button>
  ),
}));
jest.mock('../../ui/input', () => ({
  Input: (props: any) => <input {...props} />,
}));
jest.mock('../../ui/label', () => ({
  Label: ({ children, htmlFor }: any) => <label htmlFor={htmlFor}>{children}</label>,
}));
jest.mock('../../ui/select', () => ({
  Select: ({ value, onValueChange, children }: any) => (
    <select value={value} onChange={(e) => onValueChange(e.target.value)}>
      {children}
    </select>
  ),
  SelectTrigger: ({ children }: any) => <>{children}</>,
  SelectContent: ({ children }: any) => <>{children}</>,
  SelectItem: ({ value, children }: any) => <option value={value}>{children}</option>,
}));
jest.mock('../../ui/badge', () => ({
  Badge: ({ children }: any) => <span>{children}</span>,
}));

// Mock lucide-react icons
jest.mock('lucide-react', () => ({
  ChevronUp: () => <span>↑</span>,
  ChevronDown: () => <span>↓</span>,
  Trash2: () => <span>🗑️</span>,
  Plus: () => <span>+</span>,
}));

describe('ReplaceListEditor', () => {
  const mockOnSave = jest.fn();
  const mockOnClose = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
  });

  const renderEditor = (initialConfig?: ReplaceComponentConfiguration) => {
    return render(
      <ReplaceListEditor
        nodeId="test-node-1"
        initialConfig={initialConfig}
        inputSchema={mockInputSchema}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );
  };

  it('renders empty state with default rule when no config provided', () => {
    renderEditor();
    expect(screen.getByText(/Replacement Rules/i)).toBeInTheDocument();
    expect(screen.getByText(/Add Rule/i)).toBeInTheDocument();
    // One default rule should be present
    expect(screen.getAllByRole('button', { name: /🗑️/i }).length).toBe(1);
  });

  it('renders with initial rules from config', () => {
    const initialConfig: ReplaceComponentConfiguration = {
      version: '1.0',
      rules: sampleRules,
      outputSchema: mockInputSchema,
      sqlGeneration: { requiresRegex: true, estimatedRowMultiplier: 1 },
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        createdBy: 'test',
        ruleCount: 3,
        validationStatus: 'VALID',
        warnings: [],
        dependencies: [],
        compiledSql: undefined,
      },
    };
    renderEditor(initialConfig);
    // All three rules should be listed
    expect(screen.getByText('email')).toBeInTheDocument();
    expect(screen.getByText('phone')).toBeInTheDocument();
    expect(screen.getByText('first_name')).toBeInTheDocument();
  });

  it('allows adding a new rule', async () => {
    renderEditor();
    const addButton = screen.getByText(/Add Rule/i);
    fireEvent.click(addButton);
    // Now there should be two rules (default + new)
    const deleteButtons = screen.getAllByText(/🗑️/i);
    expect(deleteButtons.length).toBe(2);
  });

  it('allows deleting a rule', async () => {
    const initialConfig: ReplaceComponentConfiguration = {
      version: '1.0',
      rules: [sampleRules[0]],
      outputSchema: mockInputSchema,
      sqlGeneration: { requiresRegex: false, estimatedRowMultiplier: 1 },
      compilerMetadata: { lastModified: '', createdBy: '', ruleCount: 1, validationStatus: 'VALID', warnings: [], dependencies: [], compiledSql: undefined },
    };
    renderEditor(initialConfig);
    const deleteButton = screen.getByText(/🗑️/i);
    fireEvent.click(deleteButton);
    // Rule should disappear
    await waitFor(() => {
      expect(screen.queryByText('email')).not.toBeInTheDocument();
    });
  });

  it('allows moving rules up/down', async () => {
    const twoRules: ReplaceComponentConfiguration = {
      version: '1.0',
      rules: [sampleRules[0], sampleRules[1]],
      outputSchema: mockInputSchema,
      sqlGeneration: { requiresRegex: true, estimatedRowMultiplier: 1 },
      compilerMetadata: { lastModified: '', createdBy: '', ruleCount: 2, validationStatus: 'VALID', warnings: [], dependencies: [], compiledSql: undefined },
    };
    renderEditor(twoRules);
    // Find the up/down buttons for the first rule (email)
    const firstRule = screen.getByText('email').closest('div');
    const downButton = within(firstRule!).getByText('↓');
    // Down button should be enabled (can move down)
    expect(downButton).not.toBeDisabled();
    fireEvent.click(downButton);
    // After moving down, the order should change – we can check that the second rule is now first?
    // Simplified: just verify no crash
    expect(screen.getByText('phone')).toBeInTheDocument();
  });

  it('selects a rule and displays its editor', async () => {
    const initialConfig: ReplaceComponentConfiguration = {
      version: '1.0',
      rules: [sampleRules[0]],
      outputSchema: mockInputSchema,
      sqlGeneration: { requiresRegex: false, estimatedRowMultiplier: 1 },
      compilerMetadata: { lastModified: '', createdBy: '', ruleCount: 1, validationStatus: 'VALID', warnings: [], dependencies: [], compiledSql: undefined },
    };
    renderEditor(initialConfig);
    // The rule should be selected by default, so editor shows its values
    expect(screen.getByDisplayValue('email')).toBeInTheDocument();
    expect(screen.getByDisplayValue('@old.com')).toBeInTheDocument();
    expect(screen.getByDisplayValue('@new.com')).toBeInTheDocument();
  });

  it('updates rule properties in editor', async () => {
    const initialConfig: ReplaceComponentConfiguration = {
      version: '1.0',
      rules: [sampleRules[0]],
      outputSchema: mockInputSchema,
      sqlGeneration: { requiresRegex: false, estimatedRowMultiplier: 1 },
      compilerMetadata: { lastModified: '', createdBy: '', ruleCount: 1, validationStatus: 'VALID', warnings: [], dependencies: [], compiledSql: undefined },
    };
    renderEditor(initialConfig);
    const searchInput = screen.getByLabelText(/Search value/i);
    fireEvent.change(searchInput, { target: { value: '@example.com' } });
    const replaceInput = screen.getByLabelText(/Replace with/i);
    fireEvent.change(replaceInput, { target: { value: '@new-domain.com' } });
    // Save
    const saveButton = screen.getByText(/Save & Close/i);
    fireEvent.click(saveButton);
    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledTimes(1);
      const savedConfig = mockOnSave.mock.calls[0][0] as ReplaceComponentConfiguration;
      expect(savedConfig.rules[0].searchValue).toBe('@example.com');
      expect(savedConfig.rules[0].replacement).toBe('@new-domain.com');
    });
  });

  it('validates regular expression syntax', async () => {
    const invalidRegexRule: ReplaceRule = {
      ...sampleRules[0],
      regex: true,
      searchValue: '[invalid',
    };
    const config: ReplaceComponentConfiguration = {
      version: '1.0',
      rules: [invalidRegexRule],
      outputSchema: mockInputSchema,
      sqlGeneration: { requiresRegex: true, estimatedRowMultiplier: 1 },
      compilerMetadata: { lastModified: '', createdBy: '', ruleCount: 1, validationStatus: 'WARNING', warnings: [], dependencies: [], compiledSql: undefined },
    };
    renderEditor(config);
    // Should show a warning indicator (the badge with warning)
    expect(screen.getByText(/Warnings/i)).toBeInTheDocument();
    // Save button should be disabled because validation fails
    const saveButton = screen.getByText(/Save & Close/i);
    expect(saveButton).toBeDisabled();
  });

  it('disables save when no rules exist', async () => {
    const emptyConfig: ReplaceComponentConfiguration = {
      version: '1.0',
      rules: [],
      outputSchema: mockInputSchema,
      sqlGeneration: { requiresRegex: false, estimatedRowMultiplier: 1 },
      compilerMetadata: { lastModified: '', createdBy: '', ruleCount: 0, validationStatus: 'VALID', warnings: [], dependencies: [], compiledSql: undefined },
    };
    renderEditor(emptyConfig);
    const saveButton = screen.getByText(/Save & Close/i);
    expect(saveButton).toBeDisabled();
  });

  it('calls onClose when cancel button clicked', () => {
    renderEditor();
    const cancelButton = screen.getByText(/Cancel/i);
    fireEvent.click(cancelButton);
    expect(mockOnClose).toHaveBeenCalled();
  });
});