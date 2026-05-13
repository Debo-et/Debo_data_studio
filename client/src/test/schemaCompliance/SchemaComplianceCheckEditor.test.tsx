// src/test/components/SchemaComplianceCheckEditor.test.tsx
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import { SchemaComplianceCheckEditor } from '../../components/Editor/Parsing/SchemaComplianceCheckEditor';
import { mockInputColumns } from './schemaComplianceMock';
import { SchemaComplianceCheckConfiguration } from '../../types/unified-pipeline.types';

// Mock UI library components (simplified – adapt to your actual imports)
jest.mock('../../ui/Button', () => ({
  Button: ({ children, onClick, variant }: any) => (
    <button onClick={onClick} data-variant={variant}>{children}</button>
  ),
}));
jest.mock('../../ui/input', () => ({
  Input: (props: any) => <input {...props} />,
}));
jest.mock('../../ui/label', () => ({
  Label: ({ children }: any) => <label>{children}</label>,
}));
jest.mock('../../ui/switch', () => ({
  Switch: ({ checked, onCheckedChange }: any) => (
    <button onClick={() => onCheckedChange(!checked)} data-checked={checked}>
      Switch
    </button>
  ),
}));
jest.mock('../../ui/textarea', () => ({
  Textarea: (props: any) => <textarea {...props} />,
}));
jest.mock('../../ui/select', () => ({
  Select: ({ children, value, onValueChange }: any) => (
    <select value={value} onChange={(e) => onValueChange(e.target.value)}>
      {children}
    </select>
  ),
  SelectTrigger: ({ children }: any) => <>{children}</>,
  SelectContent: ({ children }: any) => <>{children}</>,
  SelectItem: ({ children, value }: any) => <option value={value}>{children}</option>,
  SelectValue: () => null,
}));
jest.mock('../../ui/table', () => ({
  Table: ({ children }: any) => <table>{children}</table>,
  TableHeader: ({ children }: any) => <thead>{children}</thead>,
  TableBody: ({ children }: any) => <tbody>{children}</tbody>,
  TableRow: ({ children }: any) => <tr>{children}</tr>,
  TableHead: ({ children }: any) => <th>{children}</th>,
  TableCell: ({ children }: any) => <td>{children}</td>,
}));
jest.mock('../../ui/badge', () => ({
  Badge: ({ children }: any) => <span>{children}</span>,
}));
jest.mock('../../ui/card', () => ({
  Card: ({ children }: any) => <div>{children}</div>,
  CardHeader: ({ children }: any) => <div>{children}</div>,
  CardTitle: ({ children }: any) => <h3>{children}</h3>,
  CardContent: ({ children }: any) => <div>{children}</div>,
}));
jest.mock('lucide-react', () => ({
  Plus: () => <span>+</span>,
  Trash2: () => <span>🗑️</span>,
  Edit2: () => <span>✏️</span>,
  X: () => <span>✕</span>,
}));

describe('SchemaComplianceCheckEditor', () => {
  const mockOnSave = jest.fn();
  const mockOnClose = jest.fn();
  const defaultProps = {
    nodeId: 'node-123',
    nodeMetadata: {},
    inputColumns: mockInputColumns,
    onClose: mockOnClose,
    onSave: mockOnSave,
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('renders with default configuration when no initialConfig provided', () => {
    render(<SchemaComplianceCheckEditor {...defaultProps} />);
    expect(screen.getByText(/Schema Compliance Check/i)).toBeInTheDocument();
    expect(screen.getByText(/Validation Mode/i)).toBeInTheDocument();
    expect(screen.getByText(/Error Handling/i)).toBeInTheDocument();
    expect(screen.getByText(/Basic Settings/i)).toBeInTheDocument();
    expect(screen.getByText(/Column Rules/i)).toBeInTheDocument();
  });

  it('switches tabs', () => {
    render(<SchemaComplianceCheckEditor {...defaultProps} />);
    // Initially basic settings tab
    expect(screen.getByText(/Validation Mode/i)).toBeInTheDocument();

    // Click Column Rules tab
    fireEvent.click(screen.getByText(/Column Rules/i));
    expect(screen.getByText(/Expected Columns/i)).toBeInTheDocument();

    // Click Advanced tab
    fireEvent.click(screen.getByText(/Advanced \/ Preview/i));
    expect(screen.getByText(/Output Schema \(Valid Rows\)/i)).toBeInTheDocument();
  });

  it('allows adding a new expected column from input', async () => {
    render(<SchemaComplianceCheckEditor {...defaultProps} />);
    fireEvent.click(screen.getByText(/Column Rules/i));

    // Click "Add from Input"
    fireEvent.click(screen.getByText(/Add from Input/i));

    // Prompt for column name
    window.prompt = jest.fn(() => 'id');
    fireEvent.click(screen.getByText(/Add from Input/i));

    await waitFor(() => {
      // Column should appear in the table
      expect(screen.getByText('id')).toBeInTheDocument();
    });
  });

  it('allows adding a new custom column', async () => {
    render(<SchemaComplianceCheckEditor {...defaultProps} />);
    fireEvent.click(screen.getByText(/Column Rules/i));

    fireEvent.click(screen.getByText(/New Column/i));

    // Column editor modal appears – fill and save
    await waitFor(() => {
      expect(screen.getByText(/Edit Column/i)).toBeInTheDocument();
    });

    fireEvent.change(screen.getByLabelText(/Column Name/i), { target: { value: 'custom_col' } });
    fireEvent.click(screen.getByText(/Save/));

    await waitFor(() => {
      expect(screen.getByText('custom_col')).toBeInTheDocument();
    });
  });

  it('allows editing an existing column', async () => {
    // Pre‑populate with a column
    const initialConfig: SchemaComplianceCheckConfiguration = {
      version: '1.0',
      expectedSchema: [
        {
          id: 'col1',
          name: 'test_col',
          dataType: 'STRING',
          nullable: true,
          required: false,
          validationRules: [],
        },
      ],
      mode: 'lenient',
      errorHandling: 'skipRow',
      compilerMetadata: { lastModified: '', createdBy: '', validationStatus: 'VALID', warnings: [], dependencies: [] },
    };
    render(<SchemaComplianceCheckEditor {...defaultProps} initialConfig={initialConfig} />);
    fireEvent.click(screen.getByText(/Column Rules/i));

    const editButton = screen.getByText('✏️');
    fireEvent.click(editButton);

    await waitFor(() => {
      expect(screen.getByText(/Edit Column/i)).toBeInTheDocument();
    });

    fireEvent.change(screen.getByLabelText(/Column Name/i), { target: { value: 'edited_col' } });
    fireEvent.click(screen.getByText(/Save/));

    await waitFor(() => {
      expect(screen.getByText('edited_col')).toBeInTheDocument();
      expect(screen.queryByText('test_col')).not.toBeInTheDocument();
    });
  });

  it('allows removing a column', async () => {
    const initialConfig: SchemaComplianceCheckConfiguration = {
      version: '1.0',
      expectedSchema: [
        {
          id: 'col1',
          name: 'to_remove',
          dataType: 'STRING',
          nullable: true,
          required: false,
          validationRules: [],
        },
      ],
      mode: 'lenient',
      errorHandling: 'skipRow',
      compilerMetadata: { lastModified: '', createdBy: '', validationStatus: 'VALID', warnings: [], dependencies: [] },
    };
    render(<SchemaComplianceCheckEditor {...defaultProps} initialConfig={initialConfig} />);
    fireEvent.click(screen.getByText(/Column Rules/i));

    expect(screen.getByText('to_remove')).toBeInTheDocument();

    const deleteButton = screen.getByText('🗑️');
    fireEvent.click(deleteButton);

    await waitFor(() => {
      expect(screen.queryByText('to_remove')).not.toBeInTheDocument();
    });
  });

  it('allows adding validation rules to a column', async () => {
    const initialConfig: SchemaComplianceCheckConfiguration = {
      version: '1.0',
      expectedSchema: [
        {
          id: 'col1',
          name: 'age',
          dataType: 'INTEGER',
          nullable: false,
          required: true,
          validationRules: [],
        },
      ],
      mode: 'strict',
      errorHandling: 'skipRow',
      compilerMetadata: { lastModified: '', createdBy: '', validationStatus: 'VALID', warnings: [], dependencies: [] },
    };
    render(<SchemaComplianceCheckEditor {...defaultProps} initialConfig={initialConfig} />);
    fireEvent.click(screen.getByText(/Column Rules/i));

    const addRuleButton = screen.getByText('+ rule');
    fireEvent.click(addRuleButton);

    await waitFor(() => {
      expect(screen.getByText(/Add Validation Rule/i)).toBeInTheDocument();
    });

    // Select rule type and save
    const ruleTypeSelect = screen.getByRole('combobox', { name: /Rule Type/i });
    fireEvent.change(ruleTypeSelect, { target: { value: 'range' } });

    // Enter parameters JSON
    const paramsTextarea = screen.getByLabelText(/Parameters \(JSON\)/i);
    fireEvent.change(paramsTextarea, { target: { value: '{"min":0,"max":120}' } });

    fireEvent.click(screen.getByText(/Save/));

    await waitFor(() => {
      expect(screen.getByText('1 rules')).toBeInTheDocument();
    });
  });

  it('updates error handling options', () => {
    render(<SchemaComplianceCheckEditor {...defaultProps} />);
    const errorHandlingSelect = screen.getByRole('combobox', { name: /Error Handling/i });
    fireEvent.change(errorHandlingSelect, { target: { value: 'rejectFlow' } });

    // Reject output settings should appear
    expect(screen.getByText(/Reject Output Settings/i)).toBeInTheDocument();
  });

  it('calls onSave with the correct configuration when saved', async () => {
    const initialConfig: SchemaComplianceCheckConfiguration = {
      version: '1.0',
      expectedSchema: [
        {
          id: 'col1',
          name: 'email',
          dataType: 'STRING',
          nullable: false,
          required: true,
          validationRules: [{ id: 'rule1', type: 'pattern', params: { pattern: '^\\S+@\\S+\\.\\S+$' } }],
        },
      ],
      mode: 'strict',
      errorHandling: 'skipRow',
      options: { continueOnFirstError: true, maxErrorsPerRow: 5 },
      compilerMetadata: { lastModified: '', createdBy: '', validationStatus: 'VALID', warnings: [], dependencies: [] },
    };
    render(<SchemaComplianceCheckEditor {...defaultProps} initialConfig={initialConfig} />);

    // Change something, e.g., mode to lenient
    const lenientRadio = screen.getByLabelText(/Lenient \(ignore extra columns\)/i);
    fireEvent.click(lenientRadio);

    const saveButton = screen.getByText(/Save Configuration/i);
    fireEvent.click(saveButton);

    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledTimes(1);
      const savedConfig = mockOnSave.mock.calls[0][0];
      expect(savedConfig.mode).toBe('lenient');
      expect(savedConfig.expectedSchema).toHaveLength(1);
      expect(savedConfig.expectedSchema[0].name).toBe('email');
      expect(savedConfig.compilerMetadata.validationStatus).toBe('VALID');
    });
    expect(mockOnClose).toHaveBeenCalled();
  });

  it('shows warning when no expected columns defined on save', async () => {
    render(<SchemaComplianceCheckEditor {...defaultProps} />);
    const saveButton = screen.getByText(/Save Configuration/i);
    fireEvent.click(saveButton);

    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledWith(
        expect.objectContaining({
          compilerMetadata: expect.objectContaining({
            validationStatus: 'WARNING',
            warnings: ['No expected columns defined'],
          }),
        })
      );
    });
  });

  it('handles reject flow output settings toggle', () => {
    render(<SchemaComplianceCheckEditor {...defaultProps} />);
    const errorHandlingSelect = screen.getByRole('combobox', { name: /Error Handling/i });
    fireEvent.change(errorHandlingSelect, { target: { value: 'rejectFlow' } });

    const rejectToggle = screen.getByText(/Reject Output Settings/i).closest('button');
    // The reject output switch is rendered as a button (from our mock)
    fireEvent.click(rejectToggle!);

    // The "Add error details columns" switch should appear
    expect(screen.getByText(/Add error details columns/i)).toBeInTheDocument();
  });
});