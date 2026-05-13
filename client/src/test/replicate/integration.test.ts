// src/test/replicate/integration.test.ts
import { generateReplicateSQL } from './replicateSQLHelper';
import { compareSQL } from '../utils/sqlComparator';

// Mock data source returning a list of columns
const mockDataSourceColumns = [
  { name: 'order_id', type: 'INTEGER' },
  { name: 'product_name', type: 'VARCHAR' },
  { name: 'quantity', type: 'INTEGER' },
];

// Simulate a pipeline where Replicate node receives data from a source
function simulateReplicatePipeline(
  inputColumns: typeof mockDataSourceColumns,
  addBranchId: boolean,
  branchColumnName?: string
): string {
  const config: any = {
    addBranchIdentifier: addBranchId,
    branchIdentifierColumnName: branchColumnName,
  };
  return generateReplicateSQL(inputColumns, config);
}

describe('Replicate Integration with Mock Data Source', () => {
  it('produces correct SQL for passthrough replication', () => {
    const sql = simulateReplicatePipeline(mockDataSourceColumns, false);
    const expected = `SELECT "order_id", "product_name", "quantity" FROM source_data`;
    expect(compareSQL(sql, expected).success).toBe(true);
  });

  it('produces correct SQL for replication with branch identifier', () => {
    const sql = simulateReplicatePipeline(mockDataSourceColumns, true, 'branch_id');
    const expected = `SELECT "order_id", "product_name", "quantity", 'branch_value' AS "branch_id" FROM source_data`;
    expect(compareSQL(sql, expected).success).toBe(true);
  });
});