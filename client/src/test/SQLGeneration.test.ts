// tests/SQLGeneration.test.ts
import { SQLGenerationPipeline, PipelineGenerationOptions } from '../generators/SQLGenerationPipeline';
import { CanvasNode, CanvasConnection, NodeMetadata, PostgreSQLDataType } from '../types/pipeline-types';
import { NodeType } from '@/types/unified-pipeline.types';
import {
  buildInputNode,
  buildOutputNode,
  buildFilterNode,
  buildMapNode,
  buildJoinNode,
  buildEdge,
  MockColumn,
} from './helpers/buildNode';

// ==================== HELPER FUNCTIONS FOR ADDITIONAL NODE TYPES ====================

function buildConvertNode(
  id: string,
  name: string,
  conversions: Array<{ column: string; targetType: PostgreSQLDataType; alias?: string }>
): CanvasNode {
  return {
    id,
    name,
    type: NodeType.CONVERT_TYPE,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      convertConfig: {
        conversions: conversions.map((c, idx) => ({
          id: `conv_${idx}`,
          sourceColumn: c.column,
          targetType: c.targetType,
          targetAlias: c.alias || c.column,
        })),
      },
    } as NodeMetadata,
  };
}

function buildReplaceNode(
  id: string,
  name: string,
  replacements: Array<{ column: string; search: string; replace: string; regex?: boolean }>
): CanvasNode {
  return {
    id,
    name,
    type: NodeType.REPLACE,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      replaceConfig: {
        rules: replacements.map((r, idx) => ({
          id: `rep_${idx}`,
          field: r.column,
          searchValue: r.search,
          replacement: r.replace,
          regex: r.regex || false,
        })),
      },
    } as NodeMetadata,
  };
}

function buildReplaceListNode(
  id: string,
  name: string,
  column: string,
  searchReplacePairs: Array<{ search: string; replace: string; regex?: boolean }>
): CanvasNode {
  return {
    id,
    name,
    type: NodeType.REPLACE_LIST,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      replaceListConfig: {
        column,
        pairs: searchReplacePairs.map(p => ({ search: p.search, replace: p.replace, regex: p.regex || false })),
      },
    } as NodeMetadata,
  };
}

function buildExtractDelimitedNode(
  id: string,
  name: string,
  column: string,
  delimiter: string,
  outputColumns: string[]
): CanvasNode {
  return {
    id,
    name,
    type: NodeType.EXTRACT_DELIMITED_FIELDS,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      extractDelimitedConfig: {
        sourceColumn: column,
        delimiter,
        outputColumns: outputColumns.map((col, idx) => ({
          name: col,
          position: idx,
          type: 'VARCHAR',
        })),
      },
    } as NodeMetadata,
  };
}

function buildExtractJSONNode(
  id: string,
  name: string,
  column: string,
  jsonPaths: Array<{ path: string; alias: string; type?: string }>
): CanvasNode {
  return {
    id,
    name,
    type: NodeType.EXTRACT_JSON_FIELDS,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      extractJSONConfig: {
        sourceColumn: column,
        mappings: jsonPaths.map((j, idx) => ({
          id: `json_${idx}`,
          jsonPath: j.path,
          targetColumn: j.alias,
          dataType: j.type || 'VARCHAR',
        })),
      },
    } as NodeMetadata,
  };
}

function buildExtractXMLNode(
  id: string,
  name: string,
  column: string,
  xpath: string,
  alias: string
): CanvasNode {
  return {
    id,
    name,
    type: NodeType.EXTRACT_XML_FIELD,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      extractXMLConfig: {
        sourceColumn: column,
        xpath,
        targetColumn: alias,
      },
    } as NodeMetadata,
  };
}

function buildNormalizeNode(
  id: string,
  name: string,
  column: string,
  decimalSeparator?: string,
  groupingSeparator?: string
): CanvasNode {
  return {
    id,
    name,
    type: NodeType.NORMALIZE,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      normalizeConfig: {
        sourceColumn: column,
        decimalSeparator: decimalSeparator || '.',
        groupingSeparator: groupingSeparator || ',',
      },
    } as NodeMetadata,
  };
}

function buildNormalizeNumberNode(
  id: string,
  name: string,
  column: string,
  targetType: 'INTEGER' | 'DECIMAL'
): CanvasNode {
  return {
    id,
    name,
    type: NodeType.NORMALIZE_NUMBER,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      normalizeNumberConfig: {
        sourceColumn: column,
        targetType,
      },
    } as NodeMetadata,
  };
}

function buildAggregateNode(
  id: string,
  name: string,
  groupBy: string[],
  aggregates: Array<{ function: string; column: string; alias: string; distinct?: boolean }>
): CanvasNode {
  return {
    id,
    name,
    type: NodeType.AGGREGATE_ROW,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      aggregationConfig: {
        groupBy: groupBy,
        aggregates: aggregates.map((a, idx) => ({
          id: `agg_${idx}`,
          function: a.function,
          column: a.column,
          alias: a.alias,
          distinct: a.distinct || false,
        })),
      },
    } as NodeMetadata,
  };
}

function buildSortNode(
  id: string,
  name: string,
  sortColumns: Array<{ column: string; direction: 'ASC' | 'DESC'; nullsFirst?: boolean; expression?: string }>
): CanvasNode {
  return {
    id,
    name,
    type: NodeType.SORT_ROW,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      sortConfig: {
        columns: sortColumns.map((s, idx) => ({
          id: `sort_${idx}`,
          column: s.column,
          direction: s.direction,
          nullsFirst: s.nullsFirst ?? false,
          expression: s.expression,
        })),
      },
    } as NodeMetadata,
  };
}

function buildReplicateNode(id: string, name: string, copies: number): CanvasNode {
  return {
    id,
    name,
    type: NodeType.REPLICATE,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      replicateConfig: {
        numberOfCopies: copies,
      },
    } as NodeMetadata,
  };
}

function buildUniteNode(id: string, name: string, unionAll: boolean = true): CanvasNode {
  return {
    id,
    name,
    type: NodeType.UNITE,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      uniteConfig: {
        unionAll,
      },
    } as NodeMetadata,
  };
}

function buildUniqRowNode(id: string, name: string, keyColumns: string[]): CanvasNode {
  return {
    id,
    name,
    type: NodeType.UNIQ_ROW,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      uniqRowConfig: {
        keyColumns,
      },
    } as NodeMetadata,
  };
}

function buildSplitRowNode(
  id: string,
  name: string,
  splitColumn: string,
  delimiter: string,
  outputColumns: string[]
): CanvasNode {
  return {
    id,
    name,
    type: NodeType.SPLIT_ROW,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      splitRowConfig: {
        splitColumn,
        delimiter,
        outputColumns,
      },
    } as NodeMetadata,
  };
}

function buildPivotToColumnsDelimitedNode(
  id: string,
  name: string,
  pivotColumn: string,
  valueColumn: string,
  delimiter: string
): CanvasNode {
  return {
    id,
    name,
    type: NodeType.PIVOT_TO_COLUMNS_DELIMITED,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      pivotToColumnsDelimitedConfig: {
        pivotColumn,
        valueColumn,
        delimiter,
      },
    } as NodeMetadata,
  };
}

function buildDenormalizeNode(
  id: string,
  name: string,
  keyColumns: string[],
  denormalizeColumn: string,
  delimiter: string
): CanvasNode {
  return {
    id,
    name,
    type: NodeType.DENORMALIZE,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      denormalizeConfig: {
        keyColumns,
        denormalizeColumn,
        delimiter,
      },
    } as NodeMetadata,
  };
}

function buildExtractRegexNode(
  id: string,
  name: string,
  column: string,
  regex: string,
  outputColumns: string[]
): CanvasNode {
  return {
    id,
    name,
    type: NodeType.EXTRACT_REGEX_FIELDS,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      extractRegexConfig: {
        sourceColumn: column,
        regexPattern: regex,
        outputColumns: outputColumns.map((col, idx) => ({
          name: col,
          position: idx,
        })),
      },
    } as NodeMetadata,
  };
}

function buildParseRecordSetNode(
  id: string,
  name: string,
  column: string,
  schema: Array<{ name: string; type: string }>
): CanvasNode {
  return {
    id,
    name,
    type: NodeType.PARSE_RECORD_SET,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      parseRecordSetConfig: {
        sourceColumn: column,
        schema,
      },
    } as NodeMetadata,
  };
}

function buildSampleRowNode(
  id: string,
  name: string,
  sampleSize: number,
  isAbsolute: boolean = false
): CanvasNode {
  return {
    id,
    name,
    type: NodeType.SAMPLE_ROW,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      sampleRowConfig: {
        sampleSize,
        isAbsolute,
      },
    } as NodeMetadata,
  };
}

function buildDataMaskingNode(
  id: string,
  name: string,
  rules: Array<{ column: string; maskType: string; params?: any }>
): CanvasNode {
  return {
    id,
    name,
    type: NodeType.DATA_MASKING,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      dataMaskingConfig: {
        rules: rules.map((r, idx) => ({
          id: `mask_${idx}`,
          field: r.column,
          maskType: r.maskType,
          parameters: r.params || {},
        })),
      },
    } as NodeMetadata,
  };
}

// ==================== NEW NODE TYPE HELPERS (MISSING FROM REVIEW) ====================

function buildRowGeneratorNode(
  id: string,
  name: string,
  rowCount: number,
  columns: Array<{ name: string; type: string; function: string; params?: Record<string, any> }>
): CanvasNode {
  return {
    id,
    name,
    type: NodeType.ROW_GENERATOR,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      rowGeneratorConfig: {
        rowCount,
        seed: 12345,
        useSeed: true,
        columns: columns.map((c, idx) => ({
          id: `gen_${idx}`,
          name: c.name,
          type: c.type,
          function: c.function,
          parameters: c.params || {},
        })),
      },
    } as NodeMetadata,
  };
}

function buildLookupNode(
  id: string,
  name: string,
  lookupTable: string,
  keyMapping: Array<{ sourceColumn: string; targetColumn: string }>,
  outputColumns: string[]
): CanvasNode {
  return {
    id,
    name,
    type: NodeType.LOOKUP,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      lookupConfig: {
        lookupTable,
        keyMapping,
        outputColumns,
      },
    } as NodeMetadata,
  };
}

function buildCacheNode(id: string, name: string, cacheName: string, action: 'IN' | 'OUT'): CanvasNode {
  return {
    id,
    name,
    type: action === 'IN' ? NodeType.CACHE_IN : NodeType.CACHE_OUT,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      cacheConfig: {
        cacheName,
      },
    } as NodeMetadata,
  };
}


function buildSchemaComplianceNode(
  id: string,
  name: string,
  expectedSchema: Array<{ name: string; type: string; nullable: boolean }>
): CanvasNode {
  return {
    id,
    name,
    type: NodeType.SCHEMA_COMPLIANCE_CHECK,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      schemaComplianceConfig: {
        expectedSchema,
      },
    } as NodeMetadata,
  };
}

function buildFilterColumnsNode(id: string, name: string, includedColumns: string[]): CanvasNode {
  return {
    id,
    name,
    type: NodeType.FILTER_COLUMNS,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata: {
      filterColumnsConfig: {
        includedColumns,
      },
    } as NodeMetadata,
  };
}

// ==================== TEST SUITE ====================

describe('SQL Generation Pipeline – Combination Tests', () => {
  const baseColumns: MockColumn[] = [
    { name: 'id', dataType: PostgreSQLDataType.INTEGER },
    { name: 'name', dataType: PostgreSQLDataType.VARCHAR },
    { name: 'age', dataType: PostgreSQLDataType.INTEGER },
    { name: 'salary', dataType: PostgreSQLDataType.DECIMAL },
    { name: 'department', dataType: PostgreSQLDataType.VARCHAR },
    { name: 'hire_date', dataType: PostgreSQLDataType.DATE },
    { name: 'json_data', dataType: PostgreSQLDataType.JSONB },
    { name: 'xml_data', dataType: PostgreSQLDataType.TEXT },
  ];

  // ---------- Original tests (preserved exactly as in user's file) ----------
  test.only('Input → Filter → Map → Output', async () => {
    const input = buildInputNode('in', 'src', 'source', baseColumns);
    const filter = buildFilterNode('f1', 'filter', 'age >= 21');
    const map = buildMapNode('m1', 'rename', [
      { sourceColumn: 'name', targetColumn: 'full_name' },
      { sourceColumn: 'age', targetColumn: 'years' },
    ]);
    const output = buildOutputNode('out', 'output', 'target');
    const edges = [
      buildEdge('e1', 'in', 'f1'),
      buildEdge('e2', 'f1', 'm1'),
      buildEdge('e3', 'm1', 'out'),
    ];
    await expectSQL(
      { nodes: [input, filter, map, output], edges },
      `INSERT INTO "target" (full_name, years)
       SELECT name AS full_name, age AS years
       FROM (SELECT * FROM "source" WHERE age >= 21) AS cte_f1;`
    );
  });

  test('Input → Map → Filter → Output', async () => {
    const input = buildInputNode('in', 'src', 'source', baseColumns);
    const map = buildMapNode('m1', 'rename', [
      { sourceColumn: 'name', targetColumn: 'full_name' },
    ]);
    const filter = buildFilterNode('f1', 'filter', 'full_name IS NOT NULL');
    const output = buildOutputNode('out', 'output', 'target');
    const edges = [
      buildEdge('e1', 'in', 'm1'),
      buildEdge('e2', 'm1', 'f1'),
      buildEdge('e3', 'f1', 'out'),
    ];
    await expectSQL(
      { nodes: [input, map, filter, output], edges },
      `INSERT INTO "target" (full_name)
       SELECT full_name
       FROM (SELECT name AS full_name FROM "source") AS cte_m1
       WHERE full_name IS NOT NULL;`
    );
  });

  test('Input → Filter → Aggregate → Output', async () => {
    const input = buildInputNode('in', 'sales', 'sales', [
      { name: 'region', dataType: PostgreSQLDataType.VARCHAR },
      { name: 'amount', dataType: PostgreSQLDataType.INTEGER },
    ]);
    const filter = buildFilterNode('f1', 'high_sales', 'amount > 1000');
    const aggregate = buildAggregateNode('agg', 'sum_by_region', ['region'], [
      { function: 'SUM', column: 'amount', alias: 'total' },
    ]);
    const output = buildOutputNode('out', 'output', 'regional_sales');
    const edges = [
      buildEdge('e1', 'in', 'f1'),
      buildEdge('e2', 'f1', 'agg'),
      buildEdge('e3', 'agg', 'out'),
    ];
    await expectSQL(
      { nodes: [input, filter, aggregate, output], edges },
      `INSERT INTO "regional_sales" (region, total)
       SELECT region, SUM(amount) AS total
       FROM (SELECT * FROM "sales" WHERE amount > 1000) AS cte_f1
       GROUP BY region;`
    );
  });

  test('Input → Map → Sort → Output', async () => {
    const input = buildInputNode('in', 'employees', 'employees', [
      { name: 'first_name', dataType: PostgreSQLDataType.VARCHAR },
      { name: 'last_name', dataType: PostgreSQLDataType.VARCHAR },
    ]);
    const map = buildMapNode('m1', 'fullname', [
      { sourceColumn: "first_name || ' ' || last_name", targetColumn: 'full_name', transformation: "first_name || ' ' || last_name" },
    ]);
    const sort = buildSortNode('s1', 'order_by_name', [{ column: 'full_name', direction: 'ASC' }]);
    const output = buildOutputNode('out', 'output', 'sorted_names');
    const edges = [
      buildEdge('e1', 'in', 'm1'),
      buildEdge('e2', 'm1', 's1'),
      buildEdge('e3', 's1', 'out'),
    ];
    await expectSQL(
      { nodes: [input, map, sort, output], edges },
      `INSERT INTO "sorted_names" (full_name)
       SELECT full_name
       FROM (SELECT first_name || ' ' || last_name AS full_name FROM "employees") AS cte_m1
       ORDER BY full_name ASC;`
    );
  });

  test('Input → Convert → Output', async () => {
    const input = buildInputNode('in', 'src', 'source', baseColumns);
    const convert = buildConvertNode('conv', 'convert_age', [
      { column: 'age', targetType: PostgreSQLDataType.BIGINT, alias: 'age_bigint' },
      { column: 'salary', targetType: PostgreSQLDataType.VARCHAR, alias: 'salary_str' },
    ]);
    const output = buildOutputNode('out', 'output', 'converted_table');
    const edges = [
      buildEdge('e1', 'in', 'conv'),
      buildEdge('e2', 'conv', 'out'),
    ];
    await expectSQL(
      { nodes: [input, convert, output], edges },
      `INSERT INTO "converted_table" (age_bigint, salary_str)
       SELECT CAST(age AS BIGINT) AS age_bigint, CAST(salary AS VARCHAR) AS salary_str FROM "source";`
    );
  });

  test('Input → Replace → Output', async () => {
    const input = buildInputNode('in', 'src', 'source', baseColumns);
    const replace = buildReplaceNode('rep', 'replace_name', [
      { column: 'name', search: 'John', replace: 'Jonathan' },
    ]);
    const output = buildOutputNode('out', 'output', 'replaced_table');
    const edges = [
      buildEdge('e1', 'in', 'rep'),
      buildEdge('e2', 'rep', 'out'),
    ];
    await expectSQL(
      { nodes: [input, replace, output], edges },
      `INSERT INTO "replaced_table" (id, name, age, salary, department, hire_date, json_data, xml_data)
       SELECT id, REPLACE(name, 'John', 'Jonathan') AS name, age, salary, department, hire_date, json_data, xml_data FROM "source";`
    );
  });

  test('Input → ReplaceList → Output', async () => {
    const input = buildInputNode('in', 'src', 'source', [
      { name: 'id', dataType: PostgreSQLDataType.INTEGER },
      { name: 'status', dataType: PostgreSQLDataType.VARCHAR },
    ]);
    const replaceList = buildReplaceListNode('replist', 'replace_status', 'status', [
      { search: 'A', replace: 'Active' },
      { search: 'I', replace: 'Inactive' },
    ]);
    const output = buildOutputNode('out', 'output', 'replaced_table');
    const edges = [
      buildEdge('e1', 'in', 'replist'),
      buildEdge('e2', 'replist', 'out'),
    ];
    await expectSQL(
      { nodes: [input, replaceList, output], edges },
      `INSERT INTO "replaced_table" (id, status)
       SELECT id,
              CASE status
                WHEN 'A' THEN 'Active'
                WHEN 'I' THEN 'Inactive'
                ELSE status
              END AS status
       FROM "source";`
    );
  });

  test('Input → ExtractDelimited → Output', async () => {
    const input = buildInputNode('in', 'src', 'source', [
      { name: 'id', dataType: PostgreSQLDataType.INTEGER },
      { name: 'csv_data', dataType: PostgreSQLDataType.VARCHAR },
    ]);
    const extract = buildExtractDelimitedNode('ext', 'split_csv', 'csv_data', ',', ['col1', 'col2', 'col3']);
    const output = buildOutputNode('out', 'output', 'extracted_table');
    const edges = [
      buildEdge('e1', 'in', 'ext'),
      buildEdge('e2', 'ext', 'out'),
    ];
    await expectSQL(
      { nodes: [input, extract, output], edges },
      `INSERT INTO "extracted_table" (id, col1, col2, col3)
       SELECT id,
              SPLIT_PART(csv_data, ',', 1) AS col1,
              SPLIT_PART(csv_data, ',', 2) AS col2,
              SPLIT_PART(csv_data, ',', 3) AS col3
       FROM "source";`
    );
  });

  test('Input → ExtractJSON → Output', async () => {
    const input = buildInputNode('in', 'src', 'source', [
      { name: 'id', dataType: PostgreSQLDataType.INTEGER },
      { name: 'json_data', dataType: PostgreSQLDataType.JSONB },
    ]);
    const extract = buildExtractJSONNode('ext', 'extract_json', 'json_data', [
      { path: '$.name', alias: 'name', type: 'VARCHAR' },
      { path: '$.age', alias: 'age', type: 'INTEGER' },
    ]);
    const output = buildOutputNode('out', 'output', 'extracted_table');
    const edges = [
      buildEdge('e1', 'in', 'ext'),
      buildEdge('e2', 'ext', 'out'),
    ];
    await expectSQL(
      { nodes: [input, extract, output], edges },
      `INSERT INTO "extracted_table" (id, name, age)
       SELECT id,
              json_data->>'$.name' AS name,
              (json_data->>'$.age')::INTEGER AS age
       FROM "source";`
    );
  });

  test('Input → ExtractXML → Output', async () => {
    const input = buildInputNode('in', 'src', 'source', [
      { name: 'id', dataType: PostgreSQLDataType.INTEGER },
      { name: 'xml_data', dataType: PostgreSQLDataType.TEXT },
    ]);
    const extract = buildExtractXMLNode('ext', 'extract_xml', 'xml_data', '/root/name', 'extracted_name');
    const output = buildOutputNode('out', 'output', 'extracted_table');
    const edges = [
      buildEdge('e1', 'in', 'ext'),
      buildEdge('e2', 'ext', 'out'),
    ];
    await expectSQL(
      { nodes: [input, extract, output], edges },
      `INSERT INTO "extracted_table" (id, extracted_name)
       SELECT id,
              (xpath('/root/name', xml_data::xml))[1]::text AS extracted_name
       FROM "source";`
    );
  });

  test('Input → Normalize → Output', async () => {
    const input = buildInputNode('in', 'src', 'source', [
      { name: 'id', dataType: PostgreSQLDataType.INTEGER },
      { name: 'amount', dataType: PostgreSQLDataType.VARCHAR },
    ]);
    const normalize = buildNormalizeNode('norm', 'normalize_amount', 'amount', '.', ',');
    const output = buildOutputNode('out', 'output', 'normalized_table');
    const edges = [
      buildEdge('e1', 'in', 'norm'),
      buildEdge('e2', 'norm', 'out'),
    ];
    await expectSQL(
      { nodes: [input, normalize, output], edges },
      `INSERT INTO "normalized_table" (id, amount)
       SELECT id,
              REPLACE(REPLACE(amount, ',', ''), '.', '.')::DECIMAL AS amount
       FROM "source";`
    );
  });

  test('Input → NormalizeNumber → Output', async () => {
    const input = buildInputNode('in', 'src', 'source', [
      { name: 'id', dataType: PostgreSQLDataType.INTEGER },
      { name: 'raw_number', dataType: PostgreSQLDataType.VARCHAR },
    ]);
    const normalize = buildNormalizeNumberNode('norm', 'normalize_number', 'raw_number', 'DECIMAL');
    const output = buildOutputNode('out', 'output', 'normalized_table');
    const edges = [
      buildEdge('e1', 'in', 'norm'),
      buildEdge('e2', 'norm', 'out'),
    ];
    await expectSQL(
      { nodes: [input, normalize, output], edges },
      `INSERT INTO "normalized_table" (id, raw_number)
       SELECT id,
              CAST(raw_number AS DECIMAL) AS raw_number
       FROM "source";`
    );
  });

  test('Input → Replicate → Output', async () => {
    const input = buildInputNode('in', 'src', 'source', baseColumns);
    const replicate = buildReplicateNode('rep', 'replicate', 3);
    const output = buildOutputNode('out', 'output', 'replicated_table');
    const edges = [
      buildEdge('e1', 'in', 'rep'),
      buildEdge('e2', 'rep', 'out'),
    ];
    await expectSQL(
      { nodes: [input, replicate, output], edges },
      `INSERT INTO "replicated_table" (id, name, age, salary, department, hire_date, json_data, xml_data)
       SELECT * FROM "source"
       UNION ALL
       SELECT * FROM "source"
       UNION ALL
       SELECT * FROM "source";`
    );
  });

  test('Input → UniqRow → Output', async () => {
    const input = buildInputNode('in', 'src', 'source', baseColumns);
    const uniq = buildUniqRowNode('uniq', 'deduplicate', ['id', 'name']);
    const output = buildOutputNode('out', 'output', 'dedup_table');
    const edges = [
      buildEdge('e1', 'in', 'uniq'),
      buildEdge('e2', 'uniq', 'out'),
    ];
    await expectSQL(
      { nodes: [input, uniq, output], edges },
      `INSERT INTO "dedup_table" (id, name, age, salary, department, hire_date, json_data, xml_data)
       SELECT DISTINCT ON (id, name) * FROM "source";`
    );
  });

  test('Input → SplitRow → Output', async () => {
    const input = buildInputNode('in', 'src', 'source', [
      { name: 'id', dataType: PostgreSQLDataType.INTEGER },
      { name: 'tags', dataType: PostgreSQLDataType.VARCHAR },
    ]);
    const split = buildSplitRowNode('split', 'split_tags', 'tags', '|', ['tag1', 'tag2']);
    const output = buildOutputNode('out', 'output', 'split_table');
    const edges = [
      buildEdge('e1', 'in', 'split'),
      buildEdge('e2', 'split', 'out'),
    ];
    await expectSQL(
      { nodes: [input, split, output], edges },
      `INSERT INTO "split_table" (id, tag1, tag2)
       SELECT id,
              SPLIT_PART(tags, '|', 1) AS tag1,
              SPLIT_PART(tags, '|', 2) AS tag2
       FROM "source";`
    );
  });

  test('Input → PivotToColumnsDelimited → Output', async () => {
    const input = buildInputNode('in', 'src', 'source', [
      { name: 'id', dataType: PostgreSQLDataType.INTEGER },
      { name: 'key', dataType: PostgreSQLDataType.VARCHAR },
      { name: 'value', dataType: PostgreSQLDataType.VARCHAR },
    ]);
    const pivot = buildPivotToColumnsDelimitedNode('pivot', 'pivot_table', 'key', 'value', '|');
    const output = buildOutputNode('out', 'output', 'pivoted_table');
    const edges = [
      buildEdge('e1', 'in', 'pivot'),
      buildEdge('e2', 'pivot', 'out'),
    ];
    await expectSQL(
      { nodes: [input, pivot, output], edges },
      `INSERT INTO "pivoted_table" (id, col1, col2, col3)
       SELECT id,
              MAX(CASE WHEN key = 'k1' THEN value END) AS col1,
              MAX(CASE WHEN key = 'k2' THEN value END) AS col2,
              MAX(CASE WHEN key = 'k3' THEN value END) AS col3
       FROM "source"
       GROUP BY id;`
    );
  });

  test('Input → Denormalize → Output', async () => {
    const input = buildInputNode('in', 'src', 'source', [
      { name: 'id', dataType: PostgreSQLDataType.INTEGER },
      { name: 'category', dataType: PostgreSQLDataType.VARCHAR },
    ]);
    const denorm = buildDenormalizeNode('denorm', 'denormalize', ['id'], 'category', ',');
    const output = buildOutputNode('out', 'output', 'denormalized_table');
    const edges = [
      buildEdge('e1', 'in', 'denorm'),
      buildEdge('e2', 'denorm', 'out'),
    ];
    await expectSQL(
      { nodes: [input, denorm, output], edges },
      `INSERT INTO "denormalized_table" (id, categories)
       SELECT id, STRING_AGG(category, ',') FROM "source" GROUP BY id;`
    );
  });

  test('Input → ExtractRegex → Output', async () => {
    const input = buildInputNode('in', 'src', 'source', [
      { name: 'id', dataType: PostgreSQLDataType.INTEGER },
      { name: 'text', dataType: PostgreSQLDataType.VARCHAR },
    ]);
    const regex = buildExtractRegexNode('regex', 'extract_regex', 'text', '(\\d{4})-(\\d{2})-(\\d{2})', ['year', 'month', 'day']);
    const output = buildOutputNode('out', 'output', 'extracted_table');
    const edges = [
      buildEdge('e1', 'in', 'regex'),
      buildEdge('e2', 'regex', 'out'),
    ];
    await expectSQL(
      { nodes: [input, regex, output], edges },
      `INSERT INTO "extracted_table" (id, year, month, day)
       SELECT id,
              (regexp_match(text, '(\\d{4})-(\\d{2})-(\\d{2})'))[1] AS year,
              (regexp_match(text, '(\\d{4})-(\\d{2})-(\\d{2})'))[2] AS month,
              (regexp_match(text, '(\\d{4})-(\\d{2})-(\\d{2})'))[3] AS day
       FROM "source";`
    );
  });

  test('Input → ParseRecordSet → Output', async () => {
    const input = buildInputNode('in', 'src', 'source', [
      { name: 'id', dataType: PostgreSQLDataType.INTEGER },
      { name: 'json_array', dataType: PostgreSQLDataType.JSONB },
    ]);
    const parse = buildParseRecordSetNode('parse', 'parse_json', 'json_array', [
      { name: 'item_id', type: 'INTEGER' },
      { name: 'item_name', type: 'VARCHAR' },
    ]);
    const output = buildOutputNode('out', 'output', 'parsed_table');
    const edges = [
      buildEdge('e1', 'in', 'parse'),
      buildEdge('e2', 'parse', 'out'),
    ];
    await expectSQL(
      { nodes: [input, parse, output], edges },
      `INSERT INTO "parsed_table" (id, item_id, item_name)
       SELECT id,
              (json_array->>'item_id')::INTEGER AS item_id,
              json_array->>'item_name' AS item_name
       FROM "source", jsonb_array_elements(json_array) AS elem;`
    );
  });

  test('Input → SampleRow → Output', async () => {
    const input = buildInputNode('in', 'src', 'source', baseColumns);
    const sample = buildSampleRowNode('sample', 'sample_rows', 100, false);
    const output = buildOutputNode('out', 'output', 'sampled_table');
    const edges = [
      buildEdge('e1', 'in', 'sample'),
      buildEdge('e2', 'sample', 'out'),
    ];
    await expectSQL(
      { nodes: [input, sample, output], edges },
      `INSERT INTO "sampled_table" (id, name, age, salary, department, hire_date, json_data, xml_data)
       SELECT * FROM "source" TABLESAMPLE SYSTEM(100);`
    );
  });

  test('Input → DataMasking → Output', async () => {
    const input = buildInputNode('in', 'src', 'source', [
      { name: 'id', dataType: PostgreSQLDataType.INTEGER },
      { name: 'ssn', dataType: PostgreSQLDataType.VARCHAR },
      { name: 'email', dataType: PostgreSQLDataType.VARCHAR },
    ]);
    const masking = buildDataMaskingNode('mask', 'mask_data', [
      { column: 'ssn', maskType: 'PARTIAL', params: { start: 0, end: 4, replacement: 'XXX-XX-' } },
      { column: 'email', maskType: 'EMAIL', params: {} },
    ]);
    const output = buildOutputNode('out', 'output', 'masked_table');
    const edges = [
      buildEdge('e1', 'in', 'masking'),
      buildEdge('e2', 'masking', 'out'),
    ];
    await expectSQL(
      { nodes: [input, masking, output], edges },
      `INSERT INTO "masked_table" (id, ssn, email)
       SELECT id,
              CONCAT('XXX-XX-', RIGHT(ssn, 4)) AS ssn,
              CONCAT(LEFT(email, 2), '****', SUBSTRING(email FROM POSITION('@' IN email))) AS email
       FROM "source";`
    );
  });

  test('Two inputs → Join → Filter → Output', async () => {
    const left = buildInputNode('left', 'orders', 'orders', [
      { name: 'order_id', dataType: PostgreSQLDataType.INTEGER },
      { name: 'customer_id', dataType: PostgreSQLDataType.INTEGER },
      { name: 'amount', dataType: PostgreSQLDataType.INTEGER },
    ]);
    const right = buildInputNode('right', 'customers', 'customers', [
      { name: 'customer_id', dataType: PostgreSQLDataType.INTEGER },
      { name: 'country', dataType: PostgreSQLDataType.VARCHAR },
    ]);
    const join = buildJoinNode('join', 'joined', 'INNER', 'orders.customer_id = customers.customer_id');
    const filter = buildFilterNode('f1', 'filter', 'amount > 500 AND country = \'USA\'');
    const output = buildOutputNode('out', 'output', 'filtered_orders');
    const edges = [
      buildEdge('e1', 'left', 'join'),
      buildEdge('e2', 'right', 'join'),
      buildEdge('e3', 'join', 'f1'),
      buildEdge('e4', 'f1', 'out'),
    ];
    await expectSQL(
      { nodes: [left, right, join, filter, output], edges },
      `INSERT INTO "filtered_orders" (order_id, customer_id, amount, country)
       SELECT order_id, customer_id, amount, country
       FROM (SELECT * FROM "orders" INNER JOIN "customers" ON orders.customer_id = customers.customer_id) AS cte_join
       WHERE amount > 500 AND country = 'USA';`
    );
  });

  test('Two inputs → Map left → Join → Output', async () => {
    const left = buildInputNode('left', 'orders', 'orders', [
      { name: 'order_id', dataType: PostgreSQLDataType.INTEGER },
      { name: 'customer_id', dataType: PostgreSQLDataType.INTEGER },
      { name: 'amount', dataType: PostgreSQLDataType.INTEGER },
    ]);
    const right = buildInputNode('right', 'customers', 'customers', [
      { name: 'customer_id', dataType: PostgreSQLDataType.INTEGER },
      { name: 'country', dataType: PostgreSQLDataType.VARCHAR },
    ]);
    const mapLeft = buildMapNode('map', 'map_orders', [
      { sourceColumn: 'amount', targetColumn: 'amount_usd', transformation: 'amount * 1.1' },
    ]);
    const join = buildJoinNode('join', 'joined', 'INNER', 'orders.customer_id = customers.customer_id');
    const output = buildOutputNode('out', 'output', 'joined_orders');
    const edges = [
      buildEdge('e1', 'left', 'mapLeft'),
      buildEdge('e2', 'mapLeft', 'join'),
      buildEdge('e3', 'right', 'join'),
      buildEdge('e4', 'join', 'out'),
    ];
    await expectSQL(
      { nodes: [left, right, mapLeft, join, output], edges },
      `INSERT INTO "joined_orders" (order_id, customer_id, amount_usd, country)
       SELECT order_id, customer_id, amount_usd, country
       FROM (SELECT order_id, customer_id, amount * 1.1 AS amount_usd FROM "orders") AS cte_map
       INNER JOIN "customers" ON orders.customer_id = customers.customer_id;`
    );
  });

  test('Multiple independent branches then Unite', async () => {
    const branch1Input = buildInputNode('b1_in', 'branch1', 'table_a', baseColumns);
    const branch1Filter = buildFilterNode('b1_filter', 'filter_a', 'age > 30');
    const branch2Input = buildInputNode('b2_in', 'branch2', 'table_b', baseColumns);
    const branch2Map = buildMapNode('b2_map', 'rename_b', [
      { sourceColumn: 'name', targetColumn: 'full_name' },
    ]);
    const unite = buildUniteNode('unite', 'union_all', true);
    const output = buildOutputNode('out', 'output', 'unified_table');
    const edges = [
      buildEdge('e1', 'b1_in', 'b1_filter'),
      buildEdge('e2', 'b1_filter', 'unite'),
      buildEdge('e3', 'b2_in', 'b2_map'),
      buildEdge('e4', 'b2_map', 'unite'),
      buildEdge('e5', 'unite', 'out'),
    ];
    await expectSQL(
      { nodes: [branch1Input, branch1Filter, branch2Input, branch2Map, unite, output], edges },
      `INSERT INTO "unified_table" (id, name, age, salary, department, hire_date, json_data, xml_data)
       SELECT id, name, age, salary, department, hire_date, json_data, xml_data
       FROM (SELECT * FROM "table_a" WHERE age > 30) AS cte_b1_filter
       UNION ALL
       SELECT id, name AS full_name, age, salary, department, hire_date, json_data, xml_data
       FROM "table_b";`
    );
  });

  test('Two branches with aggregates and sort then Unite', async () => {
    const leftInput = buildInputNode('left', 'sales_eu', 'sales_eu', [
      { name: 'region', dataType: PostgreSQLDataType.VARCHAR },
      { name: 'amount', dataType: PostgreSQLDataType.INTEGER },
    ]);
    const rightInput = buildInputNode('right', 'sales_us', 'sales_us', [
      { name: 'region', dataType: PostgreSQLDataType.VARCHAR },
      { name: 'amount', dataType: PostgreSQLDataType.INTEGER },
    ]);
    const leftAgg = buildAggregateNode('left_agg', 'agg_eu', ['region'], [
      { function: 'SUM', column: 'amount', alias: 'total' },
    ]);
    const rightAgg = buildAggregateNode('right_agg', 'agg_us', ['region'], [
      { function: 'SUM', column: 'amount', alias: 'total' },
    ]);
    const unite = buildUniteNode('unite', 'union_all', true);
    const sort = buildSortNode('sort', 'order_by_total', [{ column: 'total', direction: 'DESC' }]);
    const output = buildOutputNode('out', 'output', 'combined_sales');
    const edges = [
      buildEdge('e1', 'left', 'leftAgg'),
      buildEdge('e2', 'leftAgg', 'unite'),
      buildEdge('e3', 'right', 'rightAgg'),
      buildEdge('e4', 'rightAgg', 'unite'),
      buildEdge('e5', 'unite', 'sort'),
      buildEdge('e6', 'sort', 'out'),
    ];
    await expectSQL(
      { nodes: [leftInput, rightInput, leftAgg, rightAgg, unite, sort, output], edges },
      `INSERT INTO "combined_sales" (region, total)
       SELECT region, total
       FROM (
         SELECT region, SUM(amount) AS total FROM "sales_eu" GROUP BY region
         UNION ALL
         SELECT region, SUM(amount) AS total FROM "sales_us" GROUP BY region
       ) AS cte_unite
       ORDER BY total DESC;`
    );
  });

  test('Complex pipeline: Input → Map → Join → Filter → Aggregate → Sort → Output', async () => {
    const leftInput = buildInputNode('left', 'orders', 'orders', [
      { name: 'order_id', dataType: PostgreSQLDataType.INTEGER },
      { name: 'customer_id', dataType: PostgreSQLDataType.INTEGER },
      { name: 'product_id', dataType: PostgreSQLDataType.INTEGER },
      { name: 'quantity', dataType: PostgreSQLDataType.INTEGER },
      { name: 'price', dataType: PostgreSQLDataType.DECIMAL },
    ]);
    const rightInput = buildInputNode('right', 'products', 'products', [
      { name: 'product_id', dataType: PostgreSQLDataType.INTEGER },
      { name: 'category', dataType: PostgreSQLDataType.VARCHAR },
    ]);
    const mapLeft = buildMapNode('map_left', 'map_orders', [
      { sourceColumn: 'order_id', targetColumn: 'order_id' },
      { sourceColumn: 'customer_id', targetColumn: 'cust_id' },
      { sourceColumn: 'quantity * price', targetColumn: 'line_total', transformation: 'quantity * price' },
    ]);
    const join = buildJoinNode('join', 'join_orders_products', 'INNER', 'orders.product_id = products.product_id');
    const filter = buildFilterNode('filter', 'filter_high_value', 'line_total > 1000');
    const aggregate = buildAggregateNode('agg', 'sum_by_category', ['category'], [
      { function: 'SUM', column: 'line_total', alias: 'total_sales' },
      { function: 'COUNT', column: '*', alias: 'num_orders' },
    ]);
    const sort = buildSortNode('sort', 'sort_by_total', [{ column: 'total_sales', direction: 'DESC' }]);
    const output = buildOutputNode('out', 'output', 'category_summary');
    const edges = [
      buildEdge('e1', 'left', 'mapLeft'),
      buildEdge('e2', 'mapLeft', 'join'),
      buildEdge('e3', 'right', 'join'),
      buildEdge('e4', 'join', 'filter'),
      buildEdge('e5', 'filter', 'aggregate'),
      buildEdge('e6', 'aggregate', 'sort'),
      buildEdge('e7', 'sort', 'out'),
    ];
    await expectSQL(
      { nodes: [leftInput, rightInput, mapLeft, join, filter, aggregate, sort, output], edges },
      `INSERT INTO "category_summary" (category, total_sales, num_orders)
       SELECT category, SUM(line_total) AS total_sales, COUNT(*) AS num_orders
       FROM (
         SELECT category, line_total
         FROM (
           SELECT order_id, customer_id, quantity * price AS line_total, product_id
           FROM "orders"
         ) AS cte_map_left
         INNER JOIN "products" ON orders.product_id = products.product_id
         WHERE line_total > 1000
       ) AS cte_filter
       GROUP BY category
       ORDER BY total_sales DESC;`
    );
  });

  test('Pipeline with no output node should error', async () => {
    const input = buildInputNode('in', 'src', 'table', baseColumns);
    const filter = buildFilterNode('f1', 'filter', 'id > 0');
    const pipeline = new SQLGenerationPipeline([input, filter], [], { logLevel: 'error' });
    const result = await pipeline.generate();
    expect(result.errors.length).toBeGreaterThan(0);
    expect(result.errors[0].message).toMatch(/output node/i);
  });

  test('Output node with auto-generated name should error', async () => {
    const input = buildInputNode('in', 'src', 'table', baseColumns);
    const output = buildOutputNode('out', 'output_1', 'target');
    output.name = 'output_1'; // auto-generated pattern
    const edge = buildEdge('e1', 'in', 'out');
    const pipeline = new SQLGenerationPipeline([input, output], [edge], { logLevel: 'error' });
    const result = await pipeline.generate();
    expect(result.errors.some((e) => e.message.includes('auto‑generated names'))).toBe(true);
  });

  test('Cycle detection', async () => {
    const nodeA = buildInputNode('A', 'a', 'a', baseColumns);
    const nodeB = buildFilterNode('B', 'b', '1=1');
    const nodeC = buildOutputNode('C', 'c', 'target');
    const edges = [
      buildEdge('e1', 'A', 'B'),
      buildEdge('e2', 'B', 'A'),
      buildEdge('e3', 'A', 'C'),
    ];
    const pipeline = new SQLGenerationPipeline([nodeA, nodeB, nodeC], edges, { logLevel: 'error' });
    const result = await pipeline.generate();
    expect(result.errors.some((e) => e.message.includes('circular dependencies'))).toBe(true);
  });

  test('Missing configuration uses fallback', async () => {
    const input = buildInputNode('in', 'src', 'table', baseColumns);
    const brokenFilter: CanvasNode = {
      id: 'f1',
      name: 'broken',
      type: NodeType.FILTER_ROW,
      position: { x: 0, y: 0 },
      size: { width: 200, height: 100 },
      metadata: {}, // missing filterConfig
    } as CanvasNode;
    const output = buildOutputNode('out', 'out', 'target');
    const edges = [
      buildEdge('e1', 'in', 'f1'),
      buildEdge('e2', 'f1', 'out'),
    ];
    const result = await new SQLGenerationPipeline([input, brokenFilter, output], edges, { logLevel: 'error' }).generate();
    expect(result.warnings.some((w) => w.message.includes('fallback'))).toBe(true);
    expect(result.sql).toContain('SELECT * FROM "table"');
  });

  test('Join node with missing join condition should produce warning', async () => {
    const left = buildInputNode('left', 'orders', 'orders', [
      { name: 'order_id', dataType: PostgreSQLDataType.INTEGER },
    ]);
    const right = buildInputNode('right', 'customers', 'customers', [
      { name: 'customer_id', dataType: PostgreSQLDataType.INTEGER },
    ]);
    const join = buildJoinNode('join1', 'joined', 'INNER', ''); // empty condition
    const output = buildOutputNode('out', 'output', 'joined_table');
    const edges = [
      buildEdge('e1', 'left', 'join1'),
      buildEdge('e2', 'right', 'join1'),
      buildEdge('e3', 'join1', 'out'),
    ];
    const result = await new SQLGenerationPipeline([left, right, join, output], edges, { logLevel: 'error' }).generate();
    expect(result.warnings.some((w) => w.message.includes('join condition'))).toBe(true);
  });

  test('Map node with invalid transformation expression should error', async () => {
    const input = buildInputNode('in', 'src', 'source', baseColumns);
    const mapNode = buildMapNode('map1', 'invalid_map', [
      { sourceColumn: 'age', targetColumn: 'age_squared', transformation: 'INVALID_EXPRESSION' },
    ]);
    const output = buildOutputNode('out', 'out', 'target');
    const edges = [
      buildEdge('e1', 'in', 'map1'),
      buildEdge('e2', 'map1', 'out'),
    ];
    const result = await new SQLGenerationPipeline([input, mapNode, output], edges, { logLevel: 'error' }).generate();
    expect(result.errors.some((e) => e.message.includes('transformation'))).toBe(true);
  });

  test('Aggregate node with no group by and no aggregates should error', async () => {
    const input = buildInputNode('in', 'src', 'source', baseColumns);
    const aggregate: CanvasNode = {
      id: 'agg1',
      name: 'empty_agg',
      type: NodeType.AGGREGATE_ROW,
      position: { x: 0, y: 0 },
      size: { width: 200, height: 100 },
      metadata: {
        aggregationConfig: {
          groupBy: [],
          aggregates: [],
        },
      } as NodeMetadata,
    };
    const output = buildOutputNode('out', 'output', 'target');
    const edges = [
      buildEdge('e1', 'in', 'agg1'),
      buildEdge('e2', 'agg1', 'out'),
    ];
    const result = await new SQLGenerationPipeline([input, aggregate, output], edges, { logLevel: 'error' }).generate();
    expect(result.errors.some((e) => e.message.includes('aggregate'))).toBe(true);
  });

  test('Sort node with no sort columns should be a no-op', async () => {
    const input = buildInputNode('in', 'src', 'source', baseColumns);
    const sort = buildSortNode('sort', 'empty_sort', []);
    const output = buildOutputNode('out', 'output', 'target');
    const edges = [
      buildEdge('e1', 'in', 'sort'),
      buildEdge('e2', 'sort', 'out'),
    ];
    const result = await new SQLGenerationPipeline([input, sort, output], edges, { logLevel: 'error' }).generate();
    expect(result.sql).toContain('SELECT * FROM "source"');
    expect(result.sql).not.toContain('ORDER BY');
  });

  // ==================== NEW TESTS (from code review) ====================

  // ---------- New node type tests ----------
  describe('New node types', () => {
    test('RowGenerator → Filter → Output', async () => {
      const generator = buildRowGeneratorNode('gen', 'synthetic', 100, [
        { name: 'seq', type: 'INTEGER', function: 'ROW_NUMBER' },
        { name: 'rand_name', type: 'VARCHAR', function: 'RANDOM_STRING', params: { length: 10 } },
      ]);
      const filter = buildFilterNode('f1', 'filter', 'seq <= 50');
      const output = buildOutputNode('out', 'output', 'sample_data');
      const edges = [
        buildEdge('e1', 'gen', 'f1'),
        buildEdge('e2', 'f1', 'out'),
      ];
      await expectSQL(
        { nodes: [generator, filter, output], edges },
        `INSERT INTO "sample_data" (seq, rand_name)
         SELECT seq, rand_name
         FROM (SELECT generate_series(1,100) AS seq, md5(random()::text) AS rand_name) AS cte_gen
         WHERE seq <= 50;`
      );
    });

    test('Input → Lookup → Output', async () => {
      const input = buildInputNode('in', 'orders', 'orders', [
        { name: 'order_id', dataType: PostgreSQLDataType.INTEGER },
        { name: 'cust_id', dataType: PostgreSQLDataType.INTEGER },
      ]);
      const lookup = buildLookupNode('lk', 'customer_lookup', 'customers', [
        { sourceColumn: 'cust_id', targetColumn: 'id' },
      ], ['name', 'email']);
      const output = buildOutputNode('out', 'output', 'enriched_orders');
      const edges = [
        buildEdge('e1', 'in', 'lk'),
        buildEdge('e2', 'lk', 'out'),
      ];
      await expectSQL(
        { nodes: [input, lookup, output], edges },
        `INSERT INTO "enriched_orders" (order_id, cust_id, name, email)
         SELECT orders.order_id, orders.cust_id, customers.name, customers.email
         FROM "orders"
         LEFT JOIN "customers" ON orders.cust_id = customers.id;`
      );
    });

    test('CacheIn → CacheOut → Output', async () => {
      const input = buildInputNode('in', 'src', 'source', baseColumns);
      const cacheIn = buildCacheNode('cin', 'cache_in', 'temp_cache', 'IN');
      const cacheOut = buildCacheNode('cout', 'cache_out', 'temp_cache', 'OUT');
      const output = buildOutputNode('out', 'output', 'cached_data');
      const edges = [
        buildEdge('e1', 'in', 'cin'),
        buildEdge('e2', 'cin', 'cacheOut'),
        buildEdge('e3', 'cacheOut', 'out'),
      ];
      await expectSQL(
        { nodes: [input, cacheIn, cacheOut, output], edges },
        `INSERT INTO "cached_data" (id, name, age, salary, department, hire_date, json_data, xml_data)
         SELECT * FROM "source";` // caching is a no-op in SQL generation
      );
    });

    test('Input → SchemaComplianceCheck → Output (warning on mismatch)', async () => {
      const input = buildInputNode('in', 'src', 'source', baseColumns);
      const compliance = buildSchemaComplianceNode('sc', 'check_schema', [
        { name: 'id', type: 'INTEGER', nullable: false },
        { name: 'extra_col', type: 'VARCHAR', nullable: true },
      ]);
      const output = buildOutputNode('out', 'output', 'checked');
      const edges = [
        buildEdge('e1', 'in', 'sc'),
        buildEdge('e2', 'sc', 'out'),
      ];
      const result = await new SQLGenerationPipeline([input, compliance, output], edges, { logLevel: 'error' }).generate();
      expect(result.warnings.some(w => w.message.includes('missing column'))).toBe(true);
      expect(result.sql).toContain('SELECT * FROM "source"');
    });

    test('Input → FilterColumns → Output', async () => {
      const input = buildInputNode('in', 'src', 'source', baseColumns);
      const filterCols = buildFilterColumnsNode('fc', 'keep_only', ['id', 'name']);
      const output = buildOutputNode('out', 'output', 'filtered_cols');
      const edges = [
        buildEdge('e1', 'in', 'filterCols'),
        buildEdge('e2', 'filterCols', 'out'),
      ];
      await expectSQL(
        { nodes: [input, filterCols, output], edges },
        `INSERT INTO "filtered_cols" (id, name)
         SELECT id, name FROM "source";`
      );
    });
  });

  // ---------- Advanced DAG patterns ----------
  describe('Advanced DAG patterns', () => {
    test('Fan‑out: Input → Filter → two Map branches → Unite → Output', async () => {
      const input = buildInputNode('in', 'src', 'source', baseColumns);
      const filter = buildFilterNode('f1', 'filter', 'active = true');
      const mapA = buildMapNode('mA', 'mapA', [{ sourceColumn: 'name', targetColumn: 'full_name' }]);
      const mapB = buildMapNode('mB', 'mapB', [{ sourceColumn: 'age', targetColumn: 'years' }]);
      const unite = buildUniteNode('unite', 'union', true);
      const output = buildOutputNode('out', 'output', 'combined');
      const edges = [
        buildEdge('e1', 'in', 'filter'),
        buildEdge('e2', 'filter', 'mapA'),
        buildEdge('e3', 'filter', 'mapB'),
        buildEdge('e4', 'mapA', 'unite'),
        buildEdge('e5', 'mapB', 'unite'),
        buildEdge('e6', 'unite', 'out'),
      ];
      await expectSQL(
        { nodes: [input, filter, mapA, mapB, unite, output], edges },
        `INSERT INTO "combined" (full_name, years)
         SELECT full_name, years FROM (
           SELECT name AS full_name, age AS years FROM (SELECT * FROM "source" WHERE active = true) AS cte_filter
           UNION ALL
           SELECT name AS full_name, age AS years FROM (SELECT * FROM "source" WHERE active = true) AS cte_filter
         ) AS cte_unite;`
      );
    });

    test('Diamond: Input → MapA and Input → MapB → Join → Output', async () => {
      const input = buildInputNode('in', 'src', 'source', baseColumns);
      const mapA = buildMapNode('mA', 'mapA', [{ sourceColumn: 'id', targetColumn: 'id_a' }]);
      const mapB = buildMapNode('mB', 'mapB', [{ sourceColumn: 'id', targetColumn: 'id_b' }]);
      const join = buildJoinNode('join', 'self_join', 'INNER', 'id_a = id_b');
      const output = buildOutputNode('out', 'output', 'joined');
      const edges = [
        buildEdge('e1', 'in', 'mapA'),
        buildEdge('e2', 'in', 'mapB'),
        buildEdge('e3', 'mapA', 'join'),
        buildEdge('e4', 'mapB', 'join'),
        buildEdge('e5', 'join', 'out'),
      ];
      await expectSQL(
        { nodes: [input, mapA, mapB, join, output], edges },
        `INSERT INTO "joined" (id_a, id_b)
         SELECT id_a, id_b
         FROM (SELECT id AS id_a FROM "source") AS cte_mA
         INNER JOIN (SELECT id AS id_b FROM "source") AS cte_mB ON id_a = id_b;`
      );
    });

    test('Multi‑way join (3 inputs) → Output', async () => {
      const a = buildInputNode('A', 'table_a', 'a', [{ name: 'id', dataType: PostgreSQLDataType.INTEGER }]);
      const b = buildInputNode('B', 'table_b', 'b', [{ name: 'id', dataType: PostgreSQLDataType.INTEGER }, { name: 'aid', dataType: PostgreSQLDataType.INTEGER }]);
      const c = buildInputNode('C', 'table_c', 'c', [{ name: 'id', dataType: PostgreSQLDataType.INTEGER }, { name: 'bid', dataType: PostgreSQLDataType.INTEGER }]);
      const join = buildJoinNode('join', 'three_way', 'INNER', 'A.id = b.aid AND b.id = c.bid');
      const output = buildOutputNode('out', 'output', 'result');
      const edges = [
        buildEdge('e1', 'A', 'join'),
        buildEdge('e2', 'B', 'join'),
        buildEdge('e3', 'C', 'join'),
        buildEdge('e4', 'join', 'out'),
      ];
      await expectSQL(
        { nodes: [a, b, c, join, output], edges },
        `INSERT INTO "result" (id, id, id)
         SELECT A.id, b.id, c.id
         FROM "a" AS A
         INNER JOIN "b" ON A.id = b.aid
         INNER JOIN "c" ON b.id = c.bid;`
      );
    });

    test('Replicate with multiple consumers → Unite', async () => {
      const input = buildInputNode('in', 'src', 'source', baseColumns);
      const replicate = buildReplicateNode('rep', 'replicate', 2);
      const map1 = buildMapNode('m1', 'add_suffix', [{ sourceColumn: 'name', targetColumn: 'name', transformation: "name || '_v1'" }]);
      const map2 = buildMapNode('m2', 'add_prefix', [{ sourceColumn: 'name', targetColumn: 'name', transformation: "'v2_' || name" }]);
      const unite = buildUniteNode('unite', 'union', true);
      const output = buildOutputNode('out', 'output', 'combined');
      const edges = [
        buildEdge('e1', 'in', 'replicate'),
        buildEdge('e2', 'replicate', 'map1'),
        buildEdge('e3', 'replicate', 'map2'),
        buildEdge('e4', 'map1', 'unite'),
        buildEdge('e5', 'map2', 'unite'),
        buildEdge('e6', 'unite', 'out'),
      ];
      await expectSQL(
        { nodes: [input, replicate, map1, map2, unite, output], edges },
        `INSERT INTO "combined" (id, name, age, salary, department, hire_date, json_data, xml_data)
         SELECT id, name, age, salary, department, hire_date, json_data, xml_data
         FROM (
           SELECT * FROM "source"
           UNION ALL
           SELECT * FROM "source"
         ) AS cte_replicate;`
      );
    });
  });

  // ---------- Join type variants ----------
  describe('Join type variants', () => {
    test('LEFT JOIN with filter on right table', async () => {
      const left = buildInputNode('L', 'left_tbl', 'left', [{ name: 'id', dataType: PostgreSQLDataType.INTEGER }, { name: 'val', dataType: PostgreSQLDataType.VARCHAR }]);
      const right = buildInputNode('R', 'right_tbl', 'right', [{ name: 'id', dataType: PostgreSQLDataType.INTEGER }, { name: 'status', dataType: PostgreSQLDataType.VARCHAR }]);
      const join = buildJoinNode('join', 'left_join', 'LEFT', 'L.id = R.id');
      const filter = buildFilterNode('f1', 'filter', "R.status = 'active'");
      const output = buildOutputNode('out', 'output', 'result');
      const edges = [
        buildEdge('e1', 'L', 'join'),
        buildEdge('e2', 'R', 'join'),
        buildEdge('e3', 'join', 'filter'),
        buildEdge('e4', 'filter', 'out'),
      ];
      await expectSQL(
        { nodes: [left, right, join, filter, output], edges },
        `INSERT INTO "result" (id, val, id, status)
         SELECT L.id, L.val, R.id, R.status
         FROM "left" AS L
         LEFT JOIN "right" AS R ON L.id = R.id
         WHERE R.status = 'active';`
      );
    });

    test('RIGHT JOIN', async () => {
      const left = buildInputNode('L', 'left_tbl', 'left', [{ name: 'id', dataType: PostgreSQLDataType.INTEGER }]);
      const right = buildInputNode('R', 'right_tbl', 'right', [{ name: 'id', dataType: PostgreSQLDataType.INTEGER }]);
      const join = buildJoinNode('join', 'right_join', 'RIGHT', 'L.id = R.id');
      const output = buildOutputNode('out', 'output', 'result');
      const edges = [
        buildEdge('e1', 'L', 'join'),
        buildEdge('e2', 'R', 'join'),
        buildEdge('e3', 'join', 'out'),
      ];
      await expectSQL(
        { nodes: [left, right, join, output], edges },
        `INSERT INTO "result" (id, id)
         SELECT L.id, R.id
         FROM "left" AS L
         RIGHT JOIN "right" AS R ON L.id = R.id;`
      );
    });
  });

  // ---------- Advanced edge cases for existing nodes ----------
  describe('Advanced edge cases for existing nodes', () => {
    test('Aggregate with multiple group by and distinct count', async () => {
      const input = buildInputNode('in', 'sales', 'sales', [
        { name: 'region', dataType: PostgreSQLDataType.VARCHAR },
        { name: 'product', dataType: PostgreSQLDataType.VARCHAR },
        { name: 'amount', dataType: PostgreSQLDataType.INTEGER },
        { name: 'customer_id', dataType: PostgreSQLDataType.INTEGER },
      ]);
      const agg = buildAggregateNode('agg', 'complex_agg', ['region', 'product'], [
        { function: 'SUM', column: 'amount', alias: 'total' },
        { function: 'COUNT', column: 'customer_id', alias: 'unique_customers', distinct: true },
      ]);
      const output = buildOutputNode('out', 'output', 'summary');
      const edges = [
        buildEdge('e1', 'in', 'agg'),
        buildEdge('e2', 'agg', 'out'),
      ];
      await expectSQL(
        { nodes: [input, agg, output], edges },
        `INSERT INTO "summary" (region, product, total, unique_customers)
         SELECT region, product, SUM(amount) AS total, COUNT(DISTINCT customer_id) AS unique_customers
         FROM "sales"
         GROUP BY region, product;`
      );
    });

    test('Filter with complex condition (IN, BETWEEN, IS NULL)', async () => {
      const input = buildInputNode('in', 'people', 'people', [
        { name: 'age', dataType: PostgreSQLDataType.INTEGER },
        { name: 'country', dataType: PostgreSQLDataType.VARCHAR },
        { name: 'status', dataType: PostgreSQLDataType.VARCHAR },
      ]);
      const filter = buildFilterNode('f1', 'filter', "age BETWEEN 18 AND 65 AND (country IN ('US','CA') OR status IS NULL)");
      const output = buildOutputNode('out', 'output', 'filtered');
      const edges = [
        buildEdge('e1', 'in', 'filter'),
        buildEdge('e2', 'filter', 'out'),
      ];
      await expectSQL(
        { nodes: [input, filter, output], edges },
        `INSERT INTO "filtered" (age, country, status)
         SELECT age, country, status
         FROM "people"
         WHERE age BETWEEN 18 AND 65 AND (country IN ('US','CA') OR status IS NULL);`
      );
    });

    test('Sort with NULLS FIRST and expression', async () => {
      const input = buildInputNode('in', 'src', 'source', baseColumns);
      const sort = buildSortNode('s1', 'complex_sort', [
        { column: 'priority', direction: 'DESC', nullsFirst: true },
        { column: 'LOWER(name)', direction: 'ASC', expression: 'LOWER(name)' },
      ]);
      const output = buildOutputNode('out', 'output', 'sorted');
      const edges = [
        buildEdge('e1', 'in', 'sort'),
        buildEdge('e2', 'sort', 'out'),
      ];
      await expectSQL(
        { nodes: [input, sort, output], edges },
        `INSERT INTO "sorted" (id, name, age, salary, department, hire_date, json_data, xml_data)
         SELECT * FROM "source"
         ORDER BY priority DESC NULLS FIRST, LOWER(name) ASC;`
      );
    });

    test('ReplaceList with regex replacement', async () => {
      const input = buildInputNode('in', 'src', 'source', [
        { name: 'text', dataType: PostgreSQLDataType.VARCHAR },
      ]);
      const replaceList = buildReplaceListNode('rl', 'regex_replace', 'text', [
        { search: '\\d+', replace: '[NUM]', regex: true },
        { search: '\\s+', replace: ' ', regex: true },
      ]);
      const output = buildOutputNode('out', 'output', 'cleaned');
      const edges = [
        buildEdge('e1', 'in', 'replaceList'),
        buildEdge('e2', 'replaceList', 'out'),
      ];
      await expectSQL(
        { nodes: [input, replaceList, output], edges },
        `INSERT INTO "cleaned" (text)
         SELECT REGEXP_REPLACE(REGEXP_REPLACE(text, '\\d+', '[NUM]', 'g'), '\\s+', ' ', 'g') AS text
         FROM "source";`
      );
    });
  });

  // ---------- Negative tests and validation ----------
  describe('Negative tests and validation', () => {
    test('Input node with empty table name errors', async () => {
      const input = buildInputNode('in', 'src', '', baseColumns);
      const output = buildOutputNode('out', 'output', 'target');
      const edge = buildEdge('e1', 'in', 'out');
      const pipeline = new SQLGenerationPipeline([input, output], [edge], { logLevel: 'error' });
      const result = await pipeline.generate();
      expect(result.errors.some(e => e.code === 'MISSING_TABLE_NAME')).toBe(true);
    });

    test('Map node referencing non‑existent column errors', async () => {
      const input = buildInputNode('in', 'src', 'source', baseColumns);
      const map = buildMapNode('map', 'bad_map', [{ sourceColumn: 'nonexistent', targetColumn: 'bad' }]);
      const output = buildOutputNode('out', 'output', 'target');
      const edges = [
        buildEdge('e1', 'in', 'map'),
        buildEdge('e2', 'map', 'out'),
      ];
      const pipeline = new SQLGenerationPipeline([input, map, output], edges, { logLevel: 'error' });
      const result = await pipeline.generate();
      expect(result.errors.some(e => e.message.includes('nonexistent'))).toBe(true);
    });

    test('Join with type mismatch warning', async () => {
      const left = buildInputNode('L', 'left_tbl', 'left', [{ name: 'id', dataType: PostgreSQLDataType.VARCHAR }]);
      const right = buildInputNode('R', 'right_tbl', 'right', [{ name: 'id', dataType: PostgreSQLDataType.INTEGER }]);
      const join = buildJoinNode('join', 'mismatch', 'INNER', 'L.id = R.id');
      const output = buildOutputNode('out', 'output', 'result');
      const edges = [
        buildEdge('e1', 'L', 'join'),
        buildEdge('e2', 'R', 'join'),
        buildEdge('e3', 'join', 'out'),
      ];
      const pipeline = new SQLGenerationPipeline([left, right, join, output], edges, { logLevel: 'error' });
      const result = await pipeline.generate();
      expect(result.warnings.some(w => w.message.includes('type mismatch'))).toBe(true);
    });

    test('Two nodes produce identical sanitized CTE names – should be unique', async () => {
      const nodeA = buildInputNode('node-123!', 'a', 'table_a', baseColumns);
      const nodeB = buildFilterNode('node-123!', 'b', '1=1');
      const output = buildOutputNode('out', 'output', 'target');
      const edges = [
        buildEdge('e1', 'node-123!', 'node-123!'),
        buildEdge('e2', 'node-123!', 'out'),
      ];
      const pipeline = new SQLGenerationPipeline([nodeA, nodeB, output], edges, { logLevel: 'error' });
      const result = await pipeline.generate();
      const cteNames = result.sql.match(/cte_\S+/g) || [];
      const uniqueNames = new Set(cteNames);
      expect(uniqueNames.size).toBe(cteNames.length);
    });
  });

  // ---------- Schema propagation tests ----------
  describe('Schema propagation', () => {
    test('Map renames column, downstream filter uses new name', async () => {
      const input = buildInputNode('in', 'src', 'source', [
        { name: 'old_name', dataType: PostgreSQLDataType.VARCHAR },
      ]);
      const map = buildMapNode('map', 'rename', [{ sourceColumn: 'old_name', targetColumn: 'new_name' }]);
      const filter = buildFilterNode('f1', 'filter', "new_name IS NOT NULL");
      const output = buildOutputNode('out', 'output', 'target');
      const edges = [
        buildEdge('e1', 'in', 'map'),
        buildEdge('e2', 'map', 'filter'),
        buildEdge('e3', 'filter', 'out'),
      ];
      await expectSQL(
        { nodes: [input, map, filter, output], edges },
        `INSERT INTO "target" (new_name)
         SELECT new_name
         FROM (SELECT old_name AS new_name FROM "source") AS cte_map
         WHERE new_name IS NOT NULL;`
      );
    });

    test('Map drops column, downstream filter referencing dropped column errors', async () => {
      const input = buildInputNode('in', 'src', 'source', baseColumns);
      const filterCols = buildFilterColumnsNode('fc', 'drop_col', ['id']); // keep only id
      const filter = buildFilterNode('f1', 'filter', "name = 'John'"); // name no longer exists
      const output = buildOutputNode('out', 'output', 'target');
      const edges = [
        buildEdge('e1', 'in', 'filterCols'),
        buildEdge('e2', 'filterCols', 'filter'),
        buildEdge('e3', 'filter', 'out'),
      ];
      const pipeline = new SQLGenerationPipeline([input, filterCols, filter, output], edges, { logLevel: 'error' });
      const result = await pipeline.generate();
      expect(result.errors.some(e => e.message.includes('name'))).toBe(true);
    });

    test('Convert changes type, downstream aggregation works', async () => {
      const input = buildInputNode('in', 'src', 'source', [
        { name: 'amount_str', dataType: PostgreSQLDataType.VARCHAR },
      ]);
      const convert = buildConvertNode('conv', 'to_numeric', [
        { column: 'amount_str', targetType: PostgreSQLDataType.DECIMAL, alias: 'amount_num' },
      ]);
      const agg = buildAggregateNode('agg', 'sum_agg', [], [
        { function: 'SUM', column: 'amount_num', alias: 'total' },
      ]);
      const output = buildOutputNode('out', 'output', 'summary');
      const edges = [
        buildEdge('e1', 'in', 'convert'),
        buildEdge('e2', 'convert', 'agg'),
        buildEdge('e3', 'agg', 'out'),
      ];
      await expectSQL(
        { nodes: [input, convert, agg, output], edges },
        `INSERT INTO "summary" (total)
         SELECT SUM(amount_num) AS total
         FROM (SELECT CAST(amount_str AS DECIMAL) AS amount_num FROM "source") AS cte_conv;`
      );
    });

    test('Join with column name collision – both sides have "id"', async () => {
      const left = buildInputNode('L', 'left_tbl', 'left', [{ name: 'id', dataType: PostgreSQLDataType.INTEGER }, { name: 'val', dataType: PostgreSQLDataType.VARCHAR }]);
      const right = buildInputNode('R', 'right_tbl', 'right', [{ name: 'id', dataType: PostgreSQLDataType.INTEGER }, { name: 'desc', dataType: PostgreSQLDataType.VARCHAR }]);
      const join = buildJoinNode('join', 'collision', 'INNER', 'L.id = R.id');
      const output = buildOutputNode('out', 'output', 'result');
      const edges = [
        buildEdge('e1', 'L', 'join'),
        buildEdge('e2', 'R', 'join'),
        buildEdge('e3', 'join', 'out'),
      ];
      await expectSQL(
        { nodes: [left, right, join, output], edges },
        `INSERT INTO "result" (id, val, id, desc)
         SELECT L.id, L.val, R.id, R.desc
         FROM "left" AS L
         INNER JOIN "right" AS R ON L.id = R.id;`
      );
    });
  });

  // ---------- Pipeline generation options tests ----------
  describe('Pipeline generation options', () => {
    test('useCTEs: false inlines all CTEs as subqueries', async () => {
      const input = buildInputNode('in', 'src', 'source', baseColumns);
      const filter = buildFilterNode('f1', 'filter', 'age > 21');
      const output = buildOutputNode('out', 'output', 'target');
      const edges = [
        buildEdge('e1', 'in', 'filter'),
        buildEdge('e2', 'filter', 'out'),
      ];
      const options: Partial<PipelineGenerationOptions> = { useCTEs: false, formatSQL: false };
      const pipeline = new SQLGenerationPipeline([input, filter, output], edges, options);
      const result = await pipeline.generate();
      expect(result.sql).not.toContain('WITH');
      expect(result.sql).toMatch(/\(\s*SELECT.*FROM\s+"source"\s+WHERE age > 21\s*\)/);
    });

    test('materializeIntermediate: true adds MATERIALIZED keyword', async () => {
      const input = buildInputNode('in', 'src', 'source', baseColumns);
      const filter = buildFilterNode('f1', 'filter', 'age > 21');
      const output = buildOutputNode('out', 'output', 'target');
      const edges = [
        buildEdge('e1', 'in', 'filter'),
        buildEdge('e2', 'filter', 'out'),
      ];
      const options: Partial<PipelineGenerationOptions> = { materializeIntermediate: true, formatSQL: false };
      const pipeline = new SQLGenerationPipeline([input, filter, output], edges, options);
      const result = await pipeline.generate();
      expect(result.sql).toMatch(/AS MATERIALIZED\s*\(/);
    });

    test('wrapInTransaction: true adds BEGIN and COMMIT', async () => {
      const input = buildInputNode('in', 'src', 'source', baseColumns);
      const output = buildOutputNode('out', 'output', 'target');
      const edge = buildEdge('e1', 'in', 'out');
      const options: Partial<PipelineGenerationOptions> = { wrapInTransaction: true, formatSQL: false };
      const pipeline = new SQLGenerationPipeline([input, output], [edge], options);
      const result = await pipeline.generate();
      expect(result.sql).toMatch(/^BEGIN;/);
      expect(result.sql).toMatch(/COMMIT;$/);
    });

    test('formatSQL: true produces indented SQL', async () => {
      const input = buildInputNode('in', 'src', 'source', baseColumns);
      const filter = buildFilterNode('f1', 'filter', 'age > 21');
      const output = buildOutputNode('out', 'output', 'target');
      const edges = [
        buildEdge('e1', 'in', 'filter'),
        buildEdge('e2', 'filter', 'out'),
      ];
      const options: Partial<PipelineGenerationOptions> = { formatSQL: true, includeComments: false };
      const pipeline = new SQLGenerationPipeline([input, filter, output], edges, options);
      const result = await pipeline.generate();
      expect(result.sql.split('\n').some(line => /^ {2,}/.test(line))).toBe(true);
    });
  });
});

// Helper to generate SQL and compare with expected
async function expectSQL(
  pipeline: { nodes: CanvasNode[]; edges: CanvasConnection[] },
  expectedSQL: string
) {
  const generator = new SQLGenerationPipeline(pipeline.nodes, pipeline.edges, {
    includeComments: false,
    formatSQL: false,
    postgresVersion: '14.0',
    logLevel: 'error',
  });
  const result = await generator.generate();
  expect(result.errors).toHaveLength(0);
  const actual = result.sql.trim();
  const expected = expectedSQL.trim();
  if (actual !== expected) {
    console.error('SQL mismatch:\n', getDiff(expected, actual));
    throw new Error(`SQL mismatch. See diff above.`);
  }
}

function getDiff(expected: string, actual: string): string {
  return `Expected:\n${expected}\n\nActual:\n${actual}`;
}