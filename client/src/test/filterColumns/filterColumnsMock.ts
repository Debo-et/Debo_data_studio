// src/test/mocks/filterColumnsMock.ts

import { SchemaDefinition, DataType, FilterColumnsComponentConfiguration } from '../../types/unified-pipeline.types';

export const mockInputSchema: SchemaDefinition = {
  id: 'input-schema-1',
  name: 'Customer Input',
  alias: 'c',
  fields: [
    { id: 'f1', name: 'customer_id', type: 'INTEGER' as DataType, nullable: false, isKey: true },
    { id: 'f2', name: 'first_name', type: 'STRING' as DataType, nullable: true, isKey: false },
    { id: 'f3', name: 'last_name', type: 'STRING' as DataType, nullable: true, isKey: false },
    { id: 'f4', name: 'email', type: 'STRING' as DataType, nullable: false, isKey: false },
    { id: 'f5', name: 'age', type: 'INTEGER' as DataType, nullable: true, isKey: false },
  ],
  isTemporary: false,
  isMaterialized: false,
};

export const mockInitialConfig: FilterColumnsComponentConfiguration = {
  version: '1.0',
  columns: [
    { id: 'col1', originalName: 'customer_id', newName: 'cust_id', selected: true, position: 0 },
    { id: 'col2', originalName: 'first_name', newName: 'fname', selected: true, position: 1 },
    { id: 'col3', originalName: 'last_name', newName: 'lname', selected: true, position: 2 },
    { id: 'col4', originalName: 'email', newName: 'email_addr', selected: true, position: 3 },
    { id: 'col5', originalName: 'age', newName: 'age_years', selected: false, position: 4 },
  ],
  options: {
    caseSensitive: false,
    keepAllByDefault: true,
    errorOnMissingColumn: false,
  },
  outputSchema: {
    id: 'output-schema',
    name: 'Filtered Output',
    fields: [
      { id: 'f1_out', name: 'cust_id', type: 'INTEGER', nullable: false, isKey: true },
      { id: 'f2_out', name: 'fname', type: 'STRING', nullable: true, isKey: false },
      { id: 'f3_out', name: 'lname', type: 'STRING', nullable: true, isKey: false },
      { id: 'f4_out', name: 'email_addr', type: 'STRING', nullable: false, isKey: false },
    ],
    isTemporary: false,
    isMaterialized: false,
  },
  sqlGeneration: {
    selectClause: 'customer_id AS cust_id, first_name AS fname, last_name AS lname, email AS email_addr',
    estimatedRowMultiplier: 1.0,
  },
  compilerMetadata: {
    lastModified: new Date().toISOString(),
    createdBy: 'test',
    validationStatus: 'VALID',
    warnings: [],
    dependencies: ['customer_id', 'first_name', 'last_name', 'email'],
  },
};