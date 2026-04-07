// src/test/replicate/ReplicateEditor.test.tsx
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import '@testing-library/jest-dom';
import ReplicateEditor from '../../components/Editor/JoinsAndLookups/ReplicateEditor';
import { ReplicateComponentConfiguration } from '../../types/unified-pipeline.types';

// Mock external dependencies
jest.mock('../../hooks', () => ({
  useAppDispatch: jest.fn(),
  useAppSelector: jest.fn(),
}));

jest.mock('../../services/database-api.service', () => ({
  DatabaseApiService: jest.fn().mockImplementation(() => ({
    executeQuery: jest.fn().mockResolvedValue({ success: true }),
  })),
}));

describe('ReplicateEditor', () => {
  const mockInputColumns = [
    { name: 'id', type: 'INTEGER', id: 'col1' },
    { name: 'name', type: 'VARCHAR', id: 'col2' },
    { name: 'active', type: 'BOOLEAN', id: 'col3' },
  ];

  const mockOnSave = jest.fn();
  const mockOnClose = jest.fn();
  const nodeId = 'node-123';
  const nodeMetadata = { name: 'ReplicateNode', id: nodeId };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('renders correctly with input columns', () => {
    render(
      <ReplicateEditor
        nodeId={nodeId}
        nodeMetadata={nodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    expect(screen.getByText(/tReplicate Configuration/i)).toBeInTheDocument();
    expect(screen.getByText(/3 input columns/i)).toBeInTheDocument();
    expect(screen.getByText('id')).toBeInTheDocument();
    expect(screen.getByText('name')).toBeInTheDocument();
    expect(screen.getByText('active')).toBeInTheDocument();
    expect(screen.getByLabelText(/Add branch identifier column/i)).not.toBeChecked();
  });

  it('shows branch column input when checkbox is checked', async () => {
    render(
      <ReplicateEditor
        nodeId={nodeId}
        nodeMetadata={nodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    const checkbox = screen.getByLabelText(/Add branch identifier column/i);
    fireEvent.click(checkbox);

    expect(await screen.findByLabelText(/Column Name/i)).toBeInTheDocument();
    expect(screen.getByDisplayValue('branch_id')).toBeInTheDocument();
  });

  it('validates empty branch column name', async () => {
    render(
      <ReplicateEditor
        nodeId={nodeId}
        nodeMetadata={nodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    const checkbox = screen.getByLabelText(/Add branch identifier column/i);
    fireEvent.click(checkbox);

    const branchInput = screen.getByLabelText(/Column Name/i);
    await userEvent.clear(branchInput);
    fireEvent.change(branchInput, { target: { value: '' } });

    const saveButton = screen.getByText(/Save Configuration/i);
    fireEvent.click(saveButton);

    expect(await screen.findByText(/Branch identifier column name cannot be empty/i)).toBeInTheDocument();
    expect(mockOnSave).not.toHaveBeenCalled();
  });

  it('validates branch column name conflict with existing input columns', async () => {
    render(
      <ReplicateEditor
        nodeId={nodeId}
        nodeMetadata={nodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    const checkbox = screen.getByLabelText(/Add branch identifier column/i);
    fireEvent.click(checkbox);

    const branchInput = screen.getByLabelText(/Column Name/i);
    await userEvent.clear(branchInput);
    fireEvent.change(branchInput, { target: { value: 'name' } }); // 'name' already exists

    const saveButton = screen.getByText(/Save Configuration/i);
    fireEvent.click(saveButton);

    expect(await screen.findByText(/Column "name" already exists in input/i)).toBeInTheDocument();
    expect(mockOnSave).not.toHaveBeenCalled();
  });

  it('saves configuration without branch identifier', async () => {
    render(
      <ReplicateEditor
        nodeId={nodeId}
        nodeMetadata={nodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    const saveButton = screen.getByText(/Save Configuration/i);
    fireEvent.click(saveButton);

    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledTimes(1);
      const config = mockOnSave.mock.calls[0][0] as ReplicateComponentConfiguration;
      expect(config.addBranchIdentifier).toBe(false);
      expect(config.branchIdentifierColumnName).toBeUndefined();
      expect(config.outputSchema.fields).toHaveLength(3);
      expect(config.sqlGeneration?.passthrough).toBe(true);
      expect(config.compilerMetadata.validationStatus).toBe('VALID');
    });
  });

  it('saves configuration with branch identifier', async () => {
    render(
      <ReplicateEditor
        nodeId={nodeId}
        nodeMetadata={nodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    const checkbox = screen.getByLabelText(/Add branch identifier column/i);
    fireEvent.click(checkbox);

    const branchInput = screen.getByLabelText(/Column Name/i);
    await userEvent.clear(branchInput);
    fireEvent.change(branchInput, { target: { value: 'branch_code' } });

    const saveButton = screen.getByText(/Save Configuration/i);
    fireEvent.click(saveButton);

    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledTimes(1);
      const config = mockOnSave.mock.calls[0][0] as ReplicateComponentConfiguration;
      expect(config.addBranchIdentifier).toBe(true);
      expect(config.branchIdentifierColumnName).toBe('branch_code');
      expect(config.outputSchema.fields).toHaveLength(4); // input columns + branch column
      expect(config.outputSchema.fields[3].name).toBe('branch_code');
      expect(config.sqlGeneration?.passthrough).toBe(false);
    });
  });

  it('loads initial configuration if provided', () => {
    const initialConfig: ReplicateComponentConfiguration = {
      version: '1.0',
      addBranchIdentifier: true,
      branchIdentifierColumnName: 'initial_branch',
      outputSchema: {
        id: 'test_schema',
        name: 'Test Schema',
        fields: [],
        isTemporary: false,
        isMaterialized: false,
        metadata: {},
      },
      sqlGeneration: { passthrough: false, estimatedRowMultiplier: 1.0 },
      compilerMetadata: {
        lastModified: '2024-01-01T00:00:00Z',
        createdBy: 'test',
        validationStatus: 'VALID',
        warnings: [],
      },
    };

    render(
      <ReplicateEditor
        nodeId={nodeId}
        nodeMetadata={nodeMetadata}
        inputColumns={mockInputColumns}
        initialConfig={initialConfig}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    expect(screen.getByLabelText(/Add branch identifier column/i)).toBeChecked();
    expect(screen.getByDisplayValue('initial_branch')).toBeInTheDocument();
  });

  it('calls onClose when cancel button clicked', () => {
    render(
      <ReplicateEditor
        nodeId={nodeId}
        nodeMetadata={nodeMetadata}
        inputColumns={mockInputColumns}
        onClose={mockOnClose}
        onSave={mockOnSave}
      />
    );

    const cancelButton = screen.getByText(/Cancel/i);
    fireEvent.click(cancelButton);
    expect(mockOnClose).toHaveBeenCalledTimes(1);
  });
});