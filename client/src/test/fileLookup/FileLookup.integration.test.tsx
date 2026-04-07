// src/test/integration/FileLookup.integration.test.tsx
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { FileLookupEditor } from '../../components/Editor/JoinsAndLookups/FileLookupEditor';
import { FileLookupSQLGenerator } from '../../generators/FileLookupSQLGenerator';
import { compareSQL } from '../utils/sqlComparator';
import { mockInputColumns, mockFileColumns } from './fileLookupMocks';

// Mock the file preview API
jest.mock('../../../services/filePreview.service', () => ({
  previewFileSchema: jest.fn().mockResolvedValue({ columns: mockFileColumns }),
}));

describe('FileLookup Integration', () => {
  it('saves configuration and generates correct SQL', async () => {
    const onSave = jest.fn();
    render(
      <FileLookupEditor
        nodeId="lookup_integration"
        nodeMetadata={{ name: 'CustomerLookup' }}
        inputColumns={mockInputColumns}
        initialConfig={undefined}
        onClose={jest.fn()}
        onSave={onSave}
      />
    );

    // Fill form
    fireEvent.change(screen.getByLabelText('File Path'), { target: { value: '/data/customers.csv' } });
    fireEvent.click(screen.getByText('Preview'));
    await waitFor(() => {
      expect(screen.getByText('customer_id')).toBeInTheDocument();
    });

    // Add key mapping
    fireEvent.click(screen.getByText('Add Key Mapping'));
    const selects = screen.getAllByRole('combobox');
    fireEvent.click(selects[0]);
    fireEvent.click(screen.getByText('customer_id'));
    fireEvent.click(selects[2]);
    fireEvent.click(screen.getByText('customer_id'));

    // Select return fields
    const checkboxes = screen.getAllByRole('checkbox');
    fireEvent.click(checkboxes[0]); // customer_id
    fireEvent.click(checkboxes[1]); // name
    fireEvent.click(checkboxes[2]); // city

    // Save
    fireEvent.click(screen.getByText('Save Configuration'));
    await waitFor(() => {
      expect(onSave).toHaveBeenCalled();
    });

    const savedConfig = onSave.mock.calls[0][0].config;
    // Generate SQL from the saved config using the generator
    const generator = new FileLookupSQLGenerator({ postgresVersion: '14.0', includeComments: false, formatSQL: false });
    const node = {
      id: 'lookup_integration',
      name: 'CustomerLookup',
      type: 'FILE_LOOKUP',
      metadata: { configuration: { type: 'FILE_LOOKUP', config: savedConfig } },
    } as any;
    const context = { node, indentLevel: 0, parameters: new Map(), options: {} as any };
    const sqlResult = generator.generateSQL(context);

    const expectedSQL = `SELECT * FROM lookup_lookup_integration`;
    const comparison = compareSQL(sqlResult.sql, expectedSQL);
    expect(comparison.success).toBe(true);
  });
});