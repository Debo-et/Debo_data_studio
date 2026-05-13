// src/test/mocks/schemaComplianceMock.ts
import { SimpleColumn } from '../../pages/canvas.types';

export const mockInputColumns: SimpleColumn[] = [
  { name: 'id', type: 'INTEGER' },
  { name: 'name', type: 'VARCHAR' },
  { name: 'email', type: 'VARCHAR' },
  { name: 'age', type: 'INTEGER' },
  { name: 'active', type: 'BOOLEAN' },
  { name: 'created_at', type: 'TIMESTAMP' },
  { name: 'salary', type: 'DECIMAL' },
];