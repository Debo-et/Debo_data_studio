// src/test/integration/ReplaceListFlow.test.tsx
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import ReplaceListEditor from '../../components/Editor/Mapping/ReplaceListEditor';
import { ReplaceSQLGenerator } from '../../generators/ReplaceSQLGenerator';
import { mockInputSchema } from './replaceMockData';
import { compareSQL } from '../utils/sqlComparator';

// Mock the same UI components as before (simplified)
jest.mock('../../ui/Button', () => ({ Button: ({ children, onClick }: any) => <button onClick={onClick}>{children}</button> }));
jest.mock('../../ui/input', () => ({ Input: (props: any) => <input {...props} /> }));
jest.mock('../../ui/label', () => ({ Label: ({ children }: any) => <label>{children}</label> }));
jest.mock('../../ui/select', () => ({ Select: ({ value, onValueChange, children }: any) => <select value={value} onChange={e => onValueChange(e.target.value)}>{children}</select> }));
jest.mock('../../ui/select', () => ({ SelectItem: ({ value, children }: any) => <option value={value}>{children}</option> }));
jest.mock('lucide-react', () => ({ ChevronUp: () => '↑', ChevronDown: () => '↓', Trash2: () => '🗑️', Plus: () => '+' }));

describe('ReplaceList Integration', () => {
  let savedConfig: any = null;

  const mockOnSave = (config: any) => { savedConfig = config; };
  const mockOnClose = jest.fn();

  beforeEach(() => {
    savedConfig = null;
    mockOnClose.mockClear();
  });

  it('configures rules via editor and generates correct SQL', async () => {
    render(
      <ReplaceListEditor
        nodeId="test-node"
        inputSchema={mockInputSchema}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );

    // Add a new rule (there is already a default rule)
    const addButton = screen.getByText(/Add Rule/i);
    fireEvent.click(addButton);

    // Select the second rule (or edit the first)
    // Find rule list items and click on the second one
    const ruleItems = screen.getAllByText(/email|phone|first_name/);
    // The default rule's column is the first field 'first_name'? Actually initial default uses first column.
    // Let's edit the first rule to be for email
    const firstRule = ruleItems[0].closest('div');
    fireEvent.click(firstRule!);

    // Change column to 'email'
    const columnSelect = screen.getByLabelText(/Column/i);
    fireEvent.change(columnSelect, { target: { value: 'email' } });

    // Set search value and replacement
    const searchInput = screen.getByLabelText(/Search value/i);
    fireEvent.change(searchInput, { target: { value: '@gmail.com' } });
    const replaceInput = screen.getByLabelText(/Replace with/i);
    fireEvent.change(replaceInput, { target: { value: '@company.com' } });

    // Save
    const saveButton = screen.getByText(/Save & Close/i);
    fireEvent.click(saveButton);

    await waitFor(() => {
      expect(savedConfig).not.toBeNull();
      expect(savedConfig.rules).toHaveLength(2); // default + new
      const emailRule = savedConfig.rules.find((r: any) => r.column === 'email');
      expect(emailRule).toBeDefined();
      expect(emailRule.searchValue).toBe('@gmail.com');
      expect(emailRule.replacement).toBe('@company.com');
    });

    // Now use the saved config to generate SQL
    const generator = new ReplaceSQLGenerator({ postgresVersion: '14.0', includeComments: false, formatSQL: false });
    const node = {
      id: 'replace-1',
      name: 'tReplace_1',
      type: 'REPLACE',
      position: { x: 0, y: 0 },
      size: { width: 200, height: 100 },
      metadata: {
        configuration: { type: 'REPLACE', config: savedConfig },
        schemas: { output: mockInputSchema },
      },
      status: 'idle',                     // ✅ Fixed: lowercase 'idle' to match NodeStatus enum
      draggable: true,
    } as any;
    const context = {
      node,
      indentLevel: 0,
      parameters: new Map(),
      options: {
        targetDialect: 'POSTGRESQL',
        postgresVersion: '14.0',
        useCTEs: false,
        includeComments: false,
        formatSQL: false,
        optimizeForReadability: false,
        includeExecutionPlan: false,
        parameterizeValues: false,
        maxLineLength: 80,
      } as const,                         // ✅ Fixed: 'as const' to infer literal types
    };
    const result = generator.generateSQL(context);
    const expectedSQL = `SELECT REPLACE(email, '@gmail.com', '@company.com') AS email, first_name, last_name, phone FROM source_table;`;
    const comparison = compareSQL(result.sql, expectedSQL);
    expect(comparison.success).toBe(true);
  });
});