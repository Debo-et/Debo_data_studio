// src/test/components/FileLookupEditor.test.tsx
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import { FileLookupEditor } from '../../components/Editor/JoinsAndLookups/FileLookupEditor';
import { mockInputColumns, mockFileColumns, mockInitialConfig } from './fileLookupMocks';

// Mock fetch for file preview
global.fetch = jest.fn();

describe('FileLookupEditor', () => {
  const mockOnSave = jest.fn();
  const mockOnClose = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
    (global.fetch as jest.Mock).mockResolvedValue({
      ok: true,
      json: async () => ({ columns: mockFileColumns }),
    });
  });

  const renderEditor = (props = {}) => {
    return render(
      <FileLookupEditor
        nodeId="test-node"
        nodeMetadata={{ name: 'TestLookup' }}
        inputColumns={mockInputColumns}
        initialConfig={undefined}
        onClose={mockOnClose}
        onSave={mockOnSave}
        {...props}
      />
    );
  };

  it('renders all tabs and basic elements', () => {
    renderEditor();
    expect(screen.getByText('tFileLookup Configuration')).toBeInTheDocument();
    expect(screen.getByText('File Source')).toBeInTheDocument();
    expect(screen.getByText('Key & Return Fields')).toBeInTheDocument();
    expect(screen.getByText('Caching')).toBeInTheDocument();
    expect(screen.getByText('Advanced')).toBeInTheDocument();
    expect(screen.getByLabelText('File Path')).toBeInTheDocument();
    expect(screen.getByText('Save Configuration')).toBeInTheDocument();
  });

  it('loads initial configuration when provided', () => {
    renderEditor({ initialConfig: mockInitialConfig });
    expect(screen.getByDisplayValue('/data/lookup/customers.csv')).toBeInTheDocument();
    // Check that key mapping row appears
    expect(screen.getByText('customer_id')).toBeInTheDocument();
  });

  it('allows adding and removing key mappings', async () => {
    renderEditor();
    fireEvent.click(screen.getByText('Add Key Mapping'));
    const rows = screen.getAllByRole('row');
    // Header + initial zero rows? Actually initial has 0 rows, so after add we have 1 data row
    expect(rows.length).toBeGreaterThan(1);
    fireEvent.click(screen.getAllByRole('button', { name: /trash/i })[0]);
    await waitFor(() => {
      expect(screen.queryByRole('row', { name: /input field/i })).not.toBeInTheDocument();
    });
  });

  it('previews file schema and displays columns', async () => {
    renderEditor();
    const previewButton = screen.getByRole('button', { name: /Preview/i });
    fireEvent.click(previewButton);
    await waitFor(() => {
      expect(screen.getByText('customer_id')).toBeInTheDocument();
      expect(screen.getByText('name')).toBeInTheDocument();
    });
  });

  it('shows error when preview fails', async () => {
    (global.fetch as jest.Mock).mockRejectedValueOnce(new Error('Network error'));
    renderEditor();
    fireEvent.click(screen.getByRole('button', { name: /Preview/i }));
    await waitFor(() => {
      expect(screen.getByText(/failed to load file schema/i)).toBeInTheDocument();
    });
  });

  it('validates that file path is required before preview', async () => {
    renderEditor();
    // Clear file path
    const filePathInput = screen.getByLabelText('File Path');
    fireEvent.change(filePathInput, { target: { value: '' } });
    fireEvent.click(screen.getByRole('button', { name: /Preview/i }));
    await waitFor(() => {
      expect(screen.getByText(/please enter a file path/i)).toBeInTheDocument();
    });
  });

  it('saves configuration with all settings', async () => {
    renderEditor();
    // Fill file path
    fireEvent.change(screen.getByLabelText('File Path'), { target: { value: '/test/file.csv' } });
    // Preview to load columns
    fireEvent.click(screen.getByRole('button', { name: /Preview/i }));
    await waitFor(() => {
      expect(screen.getByText('customer_id')).toBeInTheDocument();
    });
    // Add key mapping
    fireEvent.click(screen.getByText('Add Key Mapping'));
    // Select input field and file column
    const selects = screen.getAllByRole('combobox');
    // First select is input field dropdown
    fireEvent.click(selects[0]);
    fireEvent.click(screen.getByText('customer_id'));
    // Second select is operator (default '=')
    // Third select is file column
    fireEvent.click(selects[2]);
    fireEvent.click(screen.getByText('customer_id'));
    // Select a return field (toggle checkbox)
    const checkboxes = screen.getAllByRole('checkbox');
    fireEvent.click(checkboxes[0]); // select first file column
    // Save
    fireEvent.click(screen.getByText('Save Configuration'));
    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledTimes(1);
      const savedConfig = mockOnSave.mock.calls[0][0];
      expect(savedConfig.type).toBe('FILE_LOOKUP');
      expect(savedConfig.config.file.path).toBe('/test/file.csv');
      expect(savedConfig.config.keyMappings).toHaveLength(1);
      expect(savedConfig.config.returnFields).toHaveLength(1);
    });
  });

  it('cancels and closes without saving', () => {
    renderEditor();
    fireEvent.click(screen.getByText('Cancel'));
    expect(mockOnClose).toHaveBeenCalled();
    expect(mockOnSave).not.toHaveBeenCalled();
  });

  // Edge Cases
  it('handles CSV delimiter and header options', async () => {
    renderEditor();
    // Switch to CSV format (default)
    // Change delimiter
    const delimiterInput = screen.getByLabelText('Delimiter');
    fireEvent.change(delimiterInput, { target: { value: '|' } });
    const headerCheckbox = screen.getByLabelText('First row is header');
    fireEvent.click(headerCheckbox);
    // Preview
    fireEvent.click(screen.getByRole('button', { name: /Preview/i }));
    await waitFor(() => {
      expect(screen.getByText('customer_id')).toBeInTheDocument();
    });
    // Save and verify options
    fireEvent.click(screen.getByText('Save Configuration'));
    expect(mockOnSave).toHaveBeenCalledWith(
      expect.objectContaining({
        config: expect.objectContaining({
          file: expect.objectContaining({
            options: expect.objectContaining({ delimiter: '|', header: false }),
          }),
        }),
      })
    );
  });

  it('handles missing key mappings gracefully', async () => {
    renderEditor();
    // Preview columns
    fireEvent.change(screen.getByLabelText('File Path'), { target: { value: '/test.csv' } });
    fireEvent.click(screen.getByRole('button', { name: /Preview/i }));
    await waitFor(() => {
      expect(screen.getByText('customer_id')).toBeInTheDocument();
    });
    // Do not add any key mappings
    // Save
    fireEvent.click(screen.getByText('Save Configuration'));
    await waitFor(() => {
      const saved = mockOnSave.mock.calls[0][0];
      expect(saved.config.keyMappings).toEqual([]);
    });
  });

  it('handles cache disabled', () => {
    renderEditor();
    const cacheSwitch = screen.getByRole('switch', { name: /enable in-memory cache/i });
    fireEvent.click(cacheSwitch);
    fireEvent.click(screen.getByText('Save Configuration'));
    expect(mockOnSave).toHaveBeenCalledWith(
      expect.objectContaining({
        config: expect.objectContaining({
          cache: expect.objectContaining({ enabled: false }),
        }),
      })
    );
  });

  it('handles fallback default value when onMissing = DEFAULT', () => {
    renderEditor();
    // Go to Caching tab (fallback is inside Caching tab)
    fireEvent.click(screen.getByText('Caching'));
    const fallbackSelect = screen.getByLabelText('When key not found');
    fireEvent.click(fallbackSelect);
    fireEvent.click(screen.getByText('Use default value'));
    const defaultValueInput = screen.getByLabelText('Default Value');
    fireEvent.change(defaultValueInput, { target: { value: 'N/A' } });
    fireEvent.click(screen.getByText('Save Configuration'));
    expect(mockOnSave).toHaveBeenCalledWith(
      expect.objectContaining({
        config: expect.objectContaining({
          fallback: { onMissing: 'DEFAULT', defaultValue: 'N/A' },
        }),
      })
    );
  });

  it('handles parallelization settings', () => {
    renderEditor();
    fireEvent.click(screen.getByText('Advanced'));
    const parallelSwitch = screen.getByRole('switch', { name: /enable parallel processing/i });
    fireEvent.click(parallelSwitch);
    const maxThreads = screen.getByLabelText('Max Threads');
    fireEvent.change(maxThreads, { target: { value: '8' } });
    const batchSize = screen.getByLabelText('Batch Size');
    fireEvent.change(batchSize, { target: { value: '2000' } });
    fireEvent.click(screen.getByText('Save Configuration'));
    expect(mockOnSave).toHaveBeenCalledWith(
      expect.objectContaining({
        config: expect.objectContaining({
          parallelization: { enabled: true, maxThreads: 8, batchSize: 2000 },
        }),
      })
    );
  });

  it('displays SQL preview after file selection', async () => {
    renderEditor();
    fireEvent.change(screen.getByLabelText('File Path'), { target: { value: '/data.csv' } });
    fireEvent.click(screen.getByRole('button', { name: /Preview/i }));
    await waitFor(() => {
      expect(screen.getByText('customer_id')).toBeInTheDocument();
    });
    // Add a key mapping
    fireEvent.click(screen.getByText('Add Key Mapping'));
    const selects = screen.getAllByRole('combobox');
    fireEvent.click(selects[0]);
    fireEvent.click(screen.getByText('customer_id'));
    fireEvent.click(selects[2]);
    fireEvent.click(screen.getByText('customer_id'));
    // Go to Advanced tab to see SQL preview
    fireEvent.click(screen.getByText('Advanced'));
    expect(screen.getByText(/LEFT JOIN lookup_table ON/)).toBeInTheDocument();
  });
});