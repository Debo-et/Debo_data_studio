// __tests__/ExtractXMLFieldEditor.test.tsx
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import '@testing-library/jest-dom';
import ExtractXMLFieldEditor from '../../components/Editor/Parsing/ExtractXMLFieldEditor';
import { ExtractXMLFieldConfiguration } from '../../types/unified-pipeline.types';

// Mock framer-motion to avoid animation issues
jest.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: any) => <div {...props}>{children}</div>,
  },
}));

describe('ExtractXMLFieldEditor', () => {
  const mockInputColumns = [
    { name: 'xml_column', type: 'xml' },
    { name: 'other_column', type: 'string' },
  ];
  const mockOnSave = jest.fn();
  const mockOnClose = jest.fn();
  const nodeName = 'XML Extractor 1';
  const nodeId = 'node-123';

  beforeEach(() => {
    jest.clearAllMocks();
  });

  const renderComponent = (initialConfig?: ExtractXMLFieldConfiguration) => {
    return render(
      <ExtractXMLFieldEditor
        nodeId={nodeId}
        nodeName={nodeName}
        inputColumns={mockInputColumns}
        initialConfig={initialConfig}
        onSave={mockOnSave}
        onClose={mockOnClose}
      />
    );
  };

  it('renders correctly with default props', () => {
    renderComponent();
    expect(screen.getByText(/XML Field Extractor/i)).toBeInTheDocument();
    expect(screen.getByText(`Node: ${nodeName}`)).toBeInTheDocument();
    expect(screen.getByText('Source XML Column')).toBeInTheDocument();
    expect(screen.getByText('Output Columns')).toBeInTheDocument();
    expect(screen.getByText('+ Add Column')).toBeInTheDocument();
  });

  it('validates that source column is required', async () => {
    renderComponent();
    const saveBtn = screen.getByText('Save & Compile');
    fireEvent.click(saveBtn);
    await waitFor(() => {
      expect(screen.getByText(/Source column is required/i)).toBeInTheDocument();
    });
    expect(mockOnSave).not.toHaveBeenCalled();
  });

  it('validates that at least one output column exists', async () => {
    renderComponent();
    // Select source column
    const sourceSelect = screen.getByRole('combobox', { name: /Source XML Column/i });
    await userEvent.selectOptions(sourceSelect, 'xml_column');
    // No output columns added yet
    const saveBtn = screen.getByText('Save & Compile');
    fireEvent.click(saveBtn);
    await waitFor(() => {
      expect(screen.getByText(/At least one output column is required/i)).toBeInTheDocument();
    });
    expect(mockOnSave).not.toHaveBeenCalled();
  });

  it('validates empty XPath and duplicate output column names', async () => {
    renderComponent();
    // Select source column
    const sourceSelect = screen.getByRole('combobox', { name: /Source XML Column/i });
    await userEvent.selectOptions(sourceSelect, 'xml_column');

    // Add two columns
    const addBtn = screen.getByText('+ Add Column');
    fireEvent.click(addBtn); // first column
    fireEvent.click(addBtn); // second column

    // Set same name for both
    const nameInputs = screen.getAllByRole('textbox', { name: '' });
    // First name input
    await userEvent.clear(nameInputs[0]);
    await userEvent.type(nameInputs[0], 'duplicate');
    // Second name input
    await userEvent.clear(nameInputs[1]);
    await userEvent.type(nameInputs[1], 'duplicate');

    // Leave XPath empty for both
    const xpathInputs = screen.getAllByPlaceholderText('/root/element');
    expect(xpathInputs).toHaveLength(2);
    // Keep empty

    const saveBtn = screen.getByText('Save & Compile');
    fireEvent.click(saveBtn);

    await waitFor(() => {
      expect(screen.getByText(/Output column name cannot be empty/i)).toBeInTheDocument();
      expect(screen.getByText(/Duplicate output column name: duplicate/i)).toBeInTheDocument();
      expect(screen.getByText(/XPath cannot be empty/i)).toBeInTheDocument();
    });
    expect(mockOnSave).not.toHaveBeenCalled();
  });

  it('allows adding and removing output columns', async () => {
    renderComponent();
    const addBtn = screen.getByText('+ Add Column');
    fireEvent.click(addBtn);
    expect(screen.getAllByRole('textbox', { name: '' })).toHaveLength(1); // name input

    fireEvent.click(addBtn);
    expect(screen.getAllByRole('textbox', { name: '' })).toHaveLength(2);

    const removeButtons = screen.getAllByTitle('Remove');
    fireEvent.click(removeButtons[0]);
    await waitFor(() => {
      expect(screen.getAllByRole('textbox', { name: '' })).toHaveLength(1);
    });
  });

  it('allows adding and removing namespace mappings', async () => {
    renderComponent();
    expect(screen.queryByPlaceholderText('Prefix')).not.toBeInTheDocument();
    const addNsBtn = screen.getByText('+ Add Namespace');
    fireEvent.click(addNsBtn);
    expect(screen.getByPlaceholderText('Prefix')).toBeInTheDocument();
    expect(screen.getByPlaceholderText('URI')).toBeInTheDocument();

    const removeNsBtn = screen.getByText('✕');
    fireEvent.click(removeNsBtn);
    await waitFor(() => {
      expect(screen.queryByPlaceholderText('Prefix')).not.toBeInTheDocument();
    });
  });

  it('toggles parallel processing and shows batch size', async () => {
    renderComponent();
    const parallelCheckbox = screen.getByLabelText('Enable parallel processing');
    expect(parallelCheckbox).not.toBeChecked();
    expect(screen.queryByLabelText('Batch size')).not.toBeInTheDocument();

    fireEvent.click(parallelCheckbox);
    expect(parallelCheckbox).toBeChecked();
    expect(screen.getByLabelText('Batch size')).toBeInTheDocument();

    fireEvent.click(parallelCheckbox);
    expect(parallelCheckbox).not.toBeChecked();
    expect(screen.queryByLabelText('Batch size')).not.toBeInTheDocument();
  });

  it('saves configuration with valid data', async () => {
    renderComponent();
    // Select source column
    const sourceSelect = screen.getByRole('combobox', { name: /Source XML Column/i });
    await userEvent.selectOptions(sourceSelect, 'xml_column');

    // Add output column
    const addBtn = screen.getByText('+ Add Column');
    fireEvent.click(addBtn);

    // Fill column details
    const nameInput = screen.getByRole('textbox', { name: '' });
    await userEvent.type(nameInput, 'customer_name');

    const xpathInput = screen.getByPlaceholderText('/root/element');
    await userEvent.type(xpathInput, '/root/customer/name');

    // Set data type to STRING (default is fine)
    // Leave length empty

    // Add a namespace
    const addNsBtn = screen.getByText('+ Add Namespace');
    fireEvent.click(addNsBtn);
    const prefixInput = screen.getByPlaceholderText('Prefix');
    const uriInput = screen.getByPlaceholderText('URI');
    await userEvent.type(prefixInput, 'ns');
    await userEvent.type(uriInput, 'http://example.com/ns');

    // Set error handling to skipRow
    const errorSelect = screen.getByRole('combobox', { name: /On extraction error/i });
    await userEvent.selectOptions(errorSelect, 'skipRow');

    // Enable parallel processing
    const parallelCheckbox = screen.getByLabelText('Enable parallel processing');
    fireEvent.click(parallelCheckbox);
    const batchSizeInput = screen.getByLabelText('Batch size');
    await userEvent.clear(batchSizeInput);
    await userEvent.type(batchSizeInput, '500');

    // Save
    const saveBtn = screen.getByText('Save & Compile');
    fireEvent.click(saveBtn);

    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledTimes(1);
      const config = mockOnSave.mock.calls[0][0] as ExtractXMLFieldConfiguration;
      expect(config.sourceColumn).toBe('xml_column');
      expect(config.xpathExpressions).toHaveLength(1);
      expect(config.xpathExpressions[0].outputColumn).toBe('customer_name');
      expect(config.xpathExpressions[0].xpath).toBe('/root/customer/name');
      expect(config.namespaceMappings).toHaveLength(1);
      expect(config.namespaceMappings[0]).toEqual({ prefix: 'ns', uri: 'http://example.com/ns' });
      expect(config.errorHandling).toBe('skipRow');
      expect(config.parallelization).toBe(true);
      expect(config.batchSize).toBe(500);
      expect(config.compilerMetadata?.validationStatus).toBe('VALID');
    });
  });

  it('loads initial configuration correctly', () => {
    const initialConfig: ExtractXMLFieldConfiguration = {
      version: '1.0',
      sourceColumn: 'other_column',
      xpathExpressions: [
        {
          id: 'expr-1',
          outputColumn: 'user_id',
          xpath: '/users/user/@id',
          dataType: 'INTEGER',
          nullable: false,
          position: 0,
        },
      ],
      namespaceMappings: [{ prefix: 'x', uri: 'http://x.com' }],
      errorHandling: 'setNull',
      parallelization: true,
      batchSize: 200,
      compilerMetadata: {
        lastModified: '',
        createdBy: '',
        ruleCount: 1,
        validationStatus: 'VALID',
        warnings: [],
        dependencies: [],
      },
    };
    renderComponent(initialConfig);
    expect(screen.getByDisplayValue('other_column')).toBeInTheDocument();
    expect(screen.getByDisplayValue('user_id')).toBeInTheDocument();
    expect(screen.getByDisplayValue('/users/user/@id')).toBeInTheDocument();
    expect(screen.getByDisplayValue('x')).toBeInTheDocument();
    expect(screen.getByDisplayValue('http://x.com')).toBeInTheDocument();
    expect(screen.getByDisplayValue('200')).toBeInTheDocument();
    expect(screen.getByLabelText('Enable parallel processing')).toBeChecked();
  });
});