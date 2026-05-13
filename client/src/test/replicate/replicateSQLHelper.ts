// src/test/helpers/replicateSQLHelper.ts
import { ReplicateComponentConfiguration } from '../../types/unified-pipeline.types';

export interface SimpleColumn {
  name: string;
  type?: string;
}

export function generateReplicateSQL(
  inputColumns: SimpleColumn[],
  config: ReplicateComponentConfiguration
): string {
  const selectColumns = inputColumns.map(col => `"${col.name}"`).join(', ');
  let sql = `SELECT ${selectColumns} FROM source_data`;

  if (config.addBranchIdentifier && config.branchIdentifierColumnName) {
    // In a real pipeline, the branch identifier would be injected by the execution framework.
    // For testing, we simulate adding a literal string column.
    const branchCol = `'branch_value' AS "${config.branchIdentifierColumnName}"`;
    sql = `SELECT ${selectColumns}, ${branchCol} FROM source_data`;
  }

  return sql;
}