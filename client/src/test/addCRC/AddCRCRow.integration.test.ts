// src/integration/AddCRCRow.integration.test.ts
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import AddCRCRowEditor from '../../components/Editor//AddCRCRowEditor';  // ✅ default import
import { AddCRCSQLGenerator } from '../../generators/AddCRCSQLGenerator';
import { compareSQL } from '../utils/sqlComparator';
import { NodeType } from '../../types/unified-pipeline.types';

describe('AddCRCRow Integration', () => {
  const mockInputColumns = [
    { name: 'user_id', type: 'INTEGER' },
    { name: 'email', type: 'STRING' },
  ];
  const mockNodeId = 'node-crc';
  const mockNodeMetadata = { name: 'CRC Node' };
  let savedConfig: any;
  const onSave = (config: any) => { savedConfig = config; };
  const onClose = jest.fn();

  it('produces correct SQL from user selections', async () => {
    render(
      <AddCRCRowEditor
        nodeId={mockNodeId}
        nodeMetadata={mockNodeMetadata}
        inputColumns={mockInputColumns}
        initialConfig={undefined}
        onClose={onClose}
        onSave={onSave}
      />
    );

    // ... (UI interactions as before) ...

    fireEvent.click(screen.getByText('Save Configuration'));

    await waitFor(() => {
      expect(savedConfig).toBeDefined();
      const generator = new AddCRCSQLGenerator({ postgresVersion: '14.0', includeComments: false, formatSQL: false });
      const node = {
        id: mockNodeId,
        name: 'CRC Node',
        type: 'tAddCRCRow' as NodeType,
        position: { x: 0, y: 0 },
        size: { width: 200, height: 100 },
        metadata: {
          configuration: savedConfig,  // ✅ direct assignment
          schemas: { output: savedConfig.outputSchema },
        },
      };
      const context = { node, indentLevel: 0, parameters: new Map(), options: {} as any };
      const result = generator.generateSQL(context);
      const expectedSQL = `SELECT user_id, email, hashtext(COALESCE(email::text, '')) AS row_checksum FROM source_table;`;
      const comparison = compareSQL(result.sql, expectedSQL);
      expect(comparison.success).toBe(true);
    });
  });
});