// src/test/mocks/convertTypeMocks.ts
import {
  ConvertComponentConfiguration,
  DataType,
  NodeType,
  UnifiedCanvasNode,
} from '../../../types/unified-pipeline.types';

export const mockInputColumns = [
  { name: 'first_name', type: 'STRING' },
  { name: 'last_name', type: 'STRING' },
  { name: 'age', type: 'INTEGER' },
  { name: 'birth_date', type: 'DATE' },
];

export const mockNodeMetadata: UnifiedCanvasNode = {
  id: 'convert-node-1',
  name: 'Convert Node',
  type: NodeType.CONVERT_TYPE,   // ✅ correct enum value
  position: { x: 0, y: 0 },
  size: { width: 200, height: 120 },
  metadata: {
    configuration: {
      type: 'CONVERT',
      config: {
        version: '1.0',
        rules: [],
        outputSchema: {
          id: 'output-schema',
          name: 'Output Schema',
          fields: [],
          isTemporary: false,
          isMaterialized: false,
        },
        sqlGeneration: {
          requiresCasting: false,
          usesConditionalLogic: false,
          estimatedRowMultiplier: 1,
        },
        compilerMetadata: {
          lastModified: new Date().toISOString(),
          createdBy: 'test',
          ruleCount: 0,
          validationStatus: 'VALID',
          dependencies: [],
        },
      } as ConvertComponentConfiguration,
    },
    schemas: {
      input: [
        {
          id: 'input-schema',
          name: 'Input Schema',
          fields: mockInputColumns.map((col, idx) => ({
            id: `field-${idx}`,
            name: col.name,
            type: col.type as DataType,
            nullable: true,
            isKey: false,
          })),
          isTemporary: false,
          isMaterialized: false,
        },
      ],
      output: {
        id: 'output-schema',
        name: 'Output Schema',
        fields: [],
        isTemporary: false,
        isMaterialized: false,
      },
    },
  },
};

export const mockInitialConfig: ConvertComponentConfiguration = {
  version: '1.0',
  rules: [
    {
      id: 'rule-1',
      sourceColumn: 'first_name',
      targetColumn: 'full_name',
      targetType: 'STRING',
      position: 0,
      parameters: { trim: true, nullHandling: 'KEEP_NULL' },
    },
    {
      id: 'rule-2',
      sourceColumn: 'age',
      targetColumn: 'age_group',
      targetType: 'STRING',
      position: 1,
      parameters: {
        defaultValue: 'Unknown',
        nullHandling: 'DEFAULT',
      },
    },
  ],
  outputSchema: {
    id: 'output-schema',
    name: 'Output Schema',
    fields: [],
    isTemporary: false,
    isMaterialized: false,
  },
  sqlGeneration: {
    requiresCasting: true,
    usesConditionalLogic: true,
    estimatedRowMultiplier: 1.0,
  },
  compilerMetadata: {
    lastModified: new Date().toISOString(),
    createdBy: 'test',
    ruleCount: 2,
    validationStatus: 'VALID',
    warnings: [],
    dependencies: [],
  },
};