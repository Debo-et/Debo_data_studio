// src/test/components/ExtractRegexFieldsEditor.test.tsx

import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import '@testing-library/jest-dom';

// Mock all UI components to simplify testing
jest.mock('../../ui/Button', () => ({
  Button: ({ children, onClick, ...props }: any) => (
    <button onClick={onClick} {...props}>{children}</button>
  ),
}));
jest.mock('../../ui/input', () => ({
  Input: (props: any) => <input {...props} />,
}));
jest.mock('../../ui/label', () => ({
  Label: ({ children, ...props }: any) => <label {...props}>{children}</label>,
}));
jest.mock('../../ui/textarea', () => ({
  Textarea: (props: any) => <textarea {...props} />,
}));
jest.mock('../../ui/select', () => ({
  Select: ({ children, value, onValueChange, ...props }: any) => (
    <select
      value={value}
      onChange={(e) => onValueChange(e.target.value)}
      {...props}
    >
      {children}
    </select>
  ),
}));
jest.mock('../../ui/switch', () => ({
  Switch: ({ checked, onCheckedChange, ...props }: any) => (
    <input
      type="checkbox"
      checked={checked}
      onChange={(e) => onCheckedChange(e.target.checked)}
      {...props}
    />
  ),
}));
jest.mock('../../ui/card', () => ({
  Card: ({ children }: any) => <div>{children}</div>,
  CardContent: ({ children }: any) => <div>{children}</div>,
  CardHeader: ({ children }: any) => <div>{children}</div>,
  CardTitle: ({ children }: any) => <div>{children}</div>,
}));
jest.mock('../../ui/badge', () => ({
  Badge: ({ children }: any) => <span>{children}</span>,
}));

// Mock icons
jest.mock('lucide-react', () => ({
  X: () => <span>✕</span>,
  RefreshCw: () => <span>⟳</span>,
  AlertCircle: () => <span>⚠</span>,
}));

// Mock framer-motion
jest.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: any) => <div {...props}>{children}</div>,
  },
}));

// Import the component after mocks
import { ExtractRegexFieldsEditor } from '../../components/Editor/Parsing/ExtractRegexFieldsEditor';
import type { ExtractRegexFieldsConfiguration } from '../../types/unified-pipeline.types';

// Mock input columns
const mockInputColumns = [
  { name: 'log_line', type: 'VARCHAR' },
  { name: 'timestamp', type: 'TIMESTAMP' },
  { name: 'message', type: 'TEXT' },
];

const mockOnClose = jest.fn();
const mockOnSave = jest.fn();

beforeEach(() => {
  jest.clearAllMocks();
});

describe('ExtractRegexFieldsEditor', () => {
  describe('Rendering', () => {
    it('renders with empty initial config', () => {
      render(
        <ExtractRegexFieldsEditor
          nodeId="node-1"
          nodeMetadata={{}}
          inputColumns={mockInputColumns}
          onClose={mockOnClose}
          onSave={mockOnSave}
        />
      );

      expect(screen.getByText(/ExtractRegexFields Configuration/i)).toBeInTheDocument();
      expect(screen.getByLabelText(/Source Column/i)).toBeInTheDocument();
      expect(screen.getByLabelText(/Regular Expression/i)).toBeInTheDocument();
      expect(screen.getByText(/Detected capturing groups: 0/i)).toBeInTheDocument();
    });

    it('renders with initial config', () => {
      const initialConfig: ExtractRegexFieldsConfiguration = {
        version: '1.0',
        sourceColumn: 'log_line',
        regexPattern: '(\\d{4})-(\\d{2})-(\\d{2})',
        caseInsensitive: true,
        multiline: false,
        dotAll: false,
        rules: [
          {
            id: 'rule-1',
            groupIndex: 1,
            columnName: 'year',
            dataType: 'STRING',
            nullable: true,
            position: 0,
          },
          {
            id: 'rule-2',
            groupIndex: 2,
            columnName: 'month',
            dataType: 'STRING',
            nullable: true,
            position: 1,
          },
        ],
        errorHandling: { onNoMatch: 'skipRow', onConversionError: 'setNull' },
        parallelization: true,
        batchSize: 500,
        outputSchema: {} as any,
        sqlGeneration: { canPushDown: true, estimatedRowMultiplier: 1.0 },
        compilerMetadata: {
          lastModified: '',
          createdBy: '',
          validationStatus: 'VALID',
          warnings: [],
          dependencies: [],
        },
      };

      render(
        <ExtractRegexFieldsEditor
          nodeId="node-1"
          nodeMetadata={{}}
          inputColumns={mockInputColumns}
          initialConfig={initialConfig}
          onClose={mockOnClose}
          onSave={mockOnSave}
        />
      );

      // Source column should be pre-selected
      expect(screen.getByDisplayValue('log_line')).toBeInTheDocument();
      // Regex pattern should be filled
      expect(screen.getByDisplayValue(/(\\d{4})-(\\d{2})-(\\d{2})/)).toBeInTheDocument();
      // Case insensitive switch should be checked
      const caseSwitch = screen.getByLabelText(/Case insensitive/i) as HTMLInputElement;
      expect(caseSwitch.checked).toBe(true);
    });
  });

  describe('Regex validation and group detection', () => {
    it('shows error for invalid regex', async () => {
      render(
        <ExtractRegexFieldsEditor
          nodeId="node-1"
          nodeMetadata={{}}
          inputColumns={mockInputColumns}
          onClose={mockOnClose}
          onSave={mockOnSave}
        />
      );

      const regexTextarea = screen.getByLabelText(/Regular Expression/i);
      await userEvent.type(regexTextarea, '(unclosed');

      await waitFor(() => {
        expect(screen.getByText(/Invalid regex:/i)).toBeInTheDocument();
        expect(screen.getByText(/Detected capturing groups: 0/i)).toBeInTheDocument();
      });
    });

    it('detects capturing groups and creates rules automatically', async () => {
      render(
        <ExtractRegexFieldsEditor
          nodeId="node-1"
          nodeMetadata={{}}
          inputColumns={mockInputColumns}
          onClose={mockOnClose}
          onSave={mockOnSave}
        />
      );

      const regexTextarea = screen.getByLabelText(/Regular Expression/i);
      await userEvent.type(regexTextarea, '(\\d+)-(\\w+)-(\\d{4})');

      await waitFor(() => {
        expect(screen.getByText(/Detected capturing groups: 3/i)).toBeInTheDocument();
      });

      // Switch to Columns tab to see auto-created rules
      fireEvent.click(screen.getByText('columns'));
      expect(screen.getAllByRole('row')).toHaveLength(4); // header + 3 rows
      expect(screen.getByDisplayValue('group1')).toBeInTheDocument();
      expect(screen.getByDisplayValue('group2')).toBeInTheDocument();
      expect(screen.getByDisplayValue('group3')).toBeInTheDocument();
    });
  });

  describe('Column management', () => {
    beforeEach(async () => {
      render(
        <ExtractRegexFieldsEditor
          nodeId="node-1"
          nodeMetadata={{}}
          inputColumns={mockInputColumns}
          onClose={mockOnClose}
          onSave={mockOnSave}
        />
      );
      const regexTextarea = screen.getByLabelText(/Regular Expression/i);
      await userEvent.type(regexTextarea, '(group1)-(group2)');
      fireEvent.click(screen.getByText('columns'));
    });

    it('allows editing column names and types', async () => {
      const columnNameInput = screen.getByDisplayValue('group1');
      await userEvent.clear(columnNameInput);
      await userEvent.type(columnNameInput, 'first_group');

      const typeSelect = screen.getAllByRole('combobox')[0];
      await userEvent.selectOptions(typeSelect, 'INTEGER');

      expect(screen.getByDisplayValue('first_group')).toBeInTheDocument();
      expect(typeSelect).toHaveValue('INTEGER');
    });

    it('auto-generates names', async () => {
      fireEvent.click(screen.getByText('Auto-generate names'));
      expect(screen.getByDisplayValue('group1')).toBeInTheDocument();
      expect(screen.getByDisplayValue('group2')).toBeInTheDocument();
    });

    it('removes a rule (but keeps at least one)', async () => {
      const removeButtons = screen.getAllByRole('button', { name: /✕/i });
      expect(removeButtons).toHaveLength(2);
      fireEvent.click(removeButtons[0]);
      // Should still have one rule left
      expect(screen.getAllByRole('row')).toHaveLength(2); // header + 1 row
    });
  });

  describe('Preview tab', () => {
    it('loads and displays preview data', async () => {
      render(
        <ExtractRegexFieldsEditor
          nodeId="node-1"
          nodeMetadata={{}}
          inputColumns={mockInputColumns}
          onClose={mockOnClose}
          onSave={mockOnSave}
        />
      );

      const regexTextarea = screen.getByLabelText(/Regular Expression/i);
      await userEvent.type(regexTextarea, '(\\d{4})-(\\d{2})-(\\d{2})');

      fireEvent.click(screen.getByText('preview'));
      fireEvent.click(screen.getByText('Refresh'));

      await waitFor(() => {
        expect(screen.getByText('2025-03-18 INFO User logged in')).toBeInTheDocument();
        expect(screen.getByText('2025-03-18 ERROR Failed to connect')).toBeInTheDocument();
      });
    });
  });

  describe('Validation and saving', () => {
    it('fails validation when source column missing', async () => {
      window.alert = jest.fn();
      render(
        <ExtractRegexFieldsEditor
          nodeId="node-1"
          nodeMetadata={{}}
          inputColumns={mockInputColumns}
          onClose={mockOnClose}
          onSave={mockOnSave}
        />
      );

      fireEvent.click(screen.getByText('Save Configuration'));
      expect(window.alert).toHaveBeenCalledWith(expect.stringContaining('Source column is required'));
    });

    it('saves valid configuration', async () => {
      render(
        <ExtractRegexFieldsEditor
          nodeId="node-1"
          nodeMetadata={{}}
          inputColumns={mockInputColumns}
          onClose={mockOnClose}
          onSave={mockOnSave}
        />
      );

      // Select source column
      const sourceSelect = screen.getByLabelText(/Source Column/i);
      await userEvent.selectOptions(sourceSelect, 'log_line');

      // Enter regex
      const regexTextarea = screen.getByLabelText(/Regular Expression/i);
      await userEvent.type(regexTextarea, '(\\w+): (\\d+)');

      // Wait for rules to appear
      await waitFor(() => {
        expect(screen.getByText(/Detected capturing groups: 2/i)).toBeInTheDocument();
      });

      fireEvent.click(screen.getByText('Save Configuration'));

      await waitFor(() => {
        expect(mockOnSave).toHaveBeenCalledTimes(1);
        const savedConfig = mockOnSave.mock.calls[0][0] as ExtractRegexFieldsConfiguration;
        expect(savedConfig.sourceColumn).toBe('log_line');
        expect(savedConfig.regexPattern).toBe('(\\w+): (\\d+)');
        expect(savedConfig.rules).toHaveLength(2);
        expect(savedConfig.rules[0].columnName).toBe('group1');
        expect(savedConfig.rules[1].columnName).toBe('group2');
        expect(savedConfig.outputSchema.fields).toHaveLength(2);
      });
    });
  });

  describe('Advanced settings', () => {
    it('toggles parallelization and batch size', async () => {
      render(
        <ExtractRegexFieldsEditor
          nodeId="node-1"
          nodeMetadata={{}}
          inputColumns={mockInputColumns}
          onClose={mockOnClose}
          onSave={mockOnSave}
        />
      );

      fireEvent.click(screen.getByText('advanced'));

      const parallelSwitch = screen.getByLabelText(/Parallel execution/i);
      expect(parallelSwitch).not.toBeChecked();

      fireEvent.click(parallelSwitch);
      expect(parallelSwitch).toBeChecked();

      const batchInput = await screen.findByLabelText(/Batch size/i);
      expect(batchInput).toBeInTheDocument();
      await userEvent.clear(batchInput);
      await userEvent.type(batchInput, '5000');
      expect(batchInput).toHaveValue(5000);
    });
  });
});