// src/test/mocks/replaceMockData.ts
import { SchemaDefinition, DataType } from '../../types/metadata';
import { ReplaceRule } from '../../types/unified-pipeline.types';

export const mockInputSchema: SchemaDefinition = {
  id: 'input-schema-1',
  name: 'Customers',
  fields: [
    { id: 'f1', name: 'first_name', type: 'STRING' as DataType, nullable: true, isKey: false },
    { id: 'f2', name: 'last_name', type: 'STRING', nullable: true, isKey: false },
    { id: 'f3', name: 'email', type: 'STRING', nullable: true, isKey: false },
    { id: 'f4', name: 'phone', type: 'STRING', nullable: true, isKey: false },
  ],
  isTemporary: false,
  isMaterialized: false,
};

export const sampleRules: ReplaceRule[] = [
  {
    id: 'rule-1',
    column: 'email',
    searchValue: '@old.com',
    replacement: '@new.com',
    caseSensitive: false,
    regex: false,
    scope: 'all',
    position: 0,
  },
  {
    id: 'rule-2',
    column: 'phone',
    searchValue: '\\+1',
    replacement: '',
    caseSensitive: false,
    regex: true,
    scope: 'all',
    position: 1,
  },
  {
    id: 'rule-3',
    column: 'first_name',
    searchValue: '^Mr\\. ',
    replacement: '',
    caseSensitive: false,
    regex: true,
    scope: 'first',
    position: 2,
  },
];