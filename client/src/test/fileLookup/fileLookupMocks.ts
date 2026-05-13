// src/test/mocks/fileLookupMocks.ts
import { FileLookupComponentConfiguration } from '../../types/unified-pipeline.types';

export const mockInputColumns = [
  { name: 'customer_id', type: 'INTEGER', id: 'col1' },
  { name: 'order_date', type: 'DATE', id: 'col2' },
  { name: 'amount', type: 'DECIMAL', id: 'col3' },
];

export const mockFileColumns = [
  { name: 'customer_id', type: 'INTEGER', sample: '101' },
  { name: 'name', type: 'STRING', sample: 'Acme Inc' },
  { name: 'city', type: 'STRING', sample: 'New York' },
  { name: 'region', type: 'STRING', sample: 'East' },
];

export const mockInitialConfig: FileLookupComponentConfiguration = {
  version: '1.0',
  file: {
    path: '/data/lookup/customers.csv',
    format: 'CSV',
    options: { delimiter: ',', header: true },
  },
  keyMappings: [{ inputField: 'customer_id', fileColumn: 'customer_id', operator: '=' }],
  returnFields: [
    { fileColumn: 'name', outputName: 'customer_name', dataType: 'STRING' },
    { fileColumn: 'city', outputName: 'city', dataType: 'STRING' },
  ],
  cache: { enabled: true, size: 1000, ttlSeconds: 300, type: 'LRU' },
  fallback: { onMissing: 'NULL' },
  errorHandling: 'FAIL',
  parallelization: { enabled: false, maxThreads: 4, batchSize: 1000 },
  outputSchema: {
    id: 'output_schema',
    name: 'Lookup Output',
    fields: [],
    isTemporary: false,
    isMaterialized: false,
  },
  compilerMetadata: { lastModified: new Date().toISOString(), validationStatus: 'VALID', warnings: [] },
};