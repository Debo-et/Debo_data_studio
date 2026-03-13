// src/utils/metadataGenerator.ts
import { 
  ComponentMetadataUnion,
  createComponentMetadata,
  getComponentHandles,
  validateConfiguration,
  getOutputSchema,
  getComponentMetadataType,
  isSourceComponent,
  isSinkComponent,
  Schema} from '../types/component-metadata';
import { 
  CanvasNode, 
  ComponentPort, 
  extractComponentTypeFromDragData,
  PortType,
  NodeStatus 
} from './canvasUtils';

// ==================== DEFAULT VALUE GENERATORS ====================

function generateDefaultMappings(inputSchema?: Schema, outputSchema?: Schema): Array<{
  id: string;
  source: string;
  target: string;
  expression?: string;
}> {
  const mappings: { id: string; source: string; target: string; expression?: string; }[] = [];
  
  if (inputSchema && outputSchema) {
    // Create direct field mappings where names match
    inputSchema.fields.forEach(sourceField => {
      const matchingTarget = outputSchema.fields.find(targetField => 
        targetField.name.toLowerCase() === sourceField.name.toLowerCase()
      );
      
      if (matchingTarget) {
        mappings.push({
          id: `mapping-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
          source: sourceField.name,
          target: matchingTarget.name,
          expression: sourceField.name
        });
      }
    });
  }
  
  return mappings;
}

function generateDefaultSchema(fields: string[] = []): Schema {
  return {
    fields: fields.map(field => ({
      name: field,
      type: 'string',
      nullable: true
    }))
  };
}

function generateDefaultAggregations(fields: string[]): Array<{
  outputField: string;
  function: 'sum' | 'avg' | 'min' | 'max' | 'count';
  inputField: string;
}> {
  return fields.map(field => ({
    outputField: `${field}_total`,
    function: 'sum',
    inputField: field
  }));
}

function generateDefaultSortKeys(fields: string[]): Array<{
  field: string;
  order: 'asc' | 'desc';
}> {
  return fields.map(field => ({
    field,
    order: 'asc'
  }));
}

function generateDefaultColumns(count: number = 5): Array<{
  name: string;
  generator: 'random' | 'sequence' | 'constant' | 'pattern' | 'function';
  parameters: any;
  type: string;
}> {
  const columns = [];
  const columnTypes = ['string', 'integer', 'float', 'boolean', 'date'];
  const generators: Array<'random' | 'sequence' | 'constant' | 'pattern' | 'function'> = 
    ['random', 'sequence', 'constant', 'pattern', 'function'];
  
  for (let i = 0; i < count; i++) {
    const type = columnTypes[i % columnTypes.length];
    const generator = generators[i % generators.length];
    
    let parameters = {};
    switch (generator) {
      case 'random':
        if (type === 'string') {
          parameters = { length: 10, charset: 'alphanumeric' };
        } else if (type === 'integer') {
          parameters = { min: 1, max: 1000 };
        } else if (type === 'float') {
          parameters = { min: 0, max: 100, precision: 2 };
        } else if (type === 'boolean') {
          parameters = { trueProbability: 0.5 };
        } else if (type === 'date') {
          parameters = { start: '2023-01-01', end: '2023-12-31' };
        }
        break;
      case 'sequence':
        parameters = { start: 1, step: 1 };
        break;
      case 'constant':
        parameters = { value: `value_${i + 1}` };
        break;
      case 'pattern':
        parameters = { pattern: 'ABC-###' };
        break;
      case 'function':
        parameters = { expression: `return ${type === 'string' ? '"default"' : type === 'number' ? '0' : 'null'}` };
        break;
    }
    
    columns.push({
      name: `column_${i + 1}`,
      generator,
      parameters,
      type
    });
  }
  
  return columns;
}

function generateDefaultJoinKeys(): { leftKeys: string[]; rightKeys: string[] } {
  return {
    leftKeys: ['id'],
    rightKeys: ['id']
  };
}

function generateDefaultMatchGroupKeys(): string[] {
  return ['name', 'email'];
}

function generateDefaultCacheKeyFields(): string[] {
  return ['id', 'key'];
}

// ==================== COMPONENT FACTORY FUNCTIONS ====================

function createMapMetadata(node: CanvasNode, inputSchema?: Schema): ComponentMetadataUnion {
  extractComponentTypeFromDragData({ type: node.type });
  const outputSchema = inputSchema || { fields: [] };
  
  return createComponentMetadata('Map', {
    mappings: generateDefaultMappings(inputSchema, outputSchema),
    inputSchema: inputSchema || { fields: [] },
    outputSchema,
    defaultMappings: true,
    mappingMode: inputSchema ? 'auto' : 'manual'
  });
}

function createJoinMetadata(_node: CanvasNode): ComponentMetadataUnion {
  const { leftKeys, rightKeys } = generateDefaultJoinKeys();
  
  return createComponentMetadata('Join', {
    joinType: 'INNER',
    leftKeys,
    rightKeys,
    prefixAliases: true,
    prefixLeft: 'A_',
    prefixRight: 'B_',
    nullEquality: true
  });
}

function createMatchGroupMetadata(_node: CanvasNode): ComponentMetadataUnion {
  const keys = generateDefaultMatchGroupKeys();
  
  return createComponentMetadata('MatchGroup', {
    keys,
    threshold: 0.8,
    algorithm: 'levenshtein',
    outputGroupId: true,
    maxGroupSize: 100
  });
}

function createFilterRowMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('FilterRow', {
    condition: '1 = 1',
    filterType: 'include',
    validateCondition: true,
    onError: 'include',
    caseSensitive: true
  });
}

function createSortRowMetadata(_node: CanvasNode, fields: string[] = []): ComponentMetadataUnion {
  return createComponentMetadata('SortRow', {
    sortKeys: generateDefaultSortKeys(fields),
    nullHandling: 'last',
    stableSort: true,
    caseSensitive: true
  });
}

function createAggregateRowMetadata(_node: CanvasNode, fields: string[] = []): ComponentMetadataUnion {
  return createComponentMetadata('AggregateRow', {
    groupFields: fields.slice(0, 2),
    aggregations: generateDefaultAggregations(fields.slice(2, 4)),
    includeGroupCount: true
  });
}

function createDenormalizeMetadata(_node: CanvasNode, fields: string[] = []): ComponentMetadataUnion {
  return createComponentMetadata('Denormalize', {
    groupFields: fields.slice(0, 2),
    targetColumns: fields.slice(2, 4),
    delimiter: ',',
    preserveOrder: true,
    sortWithinGroup: [{ field: fields[0] || 'id', order: 'asc' }]
  });
}

function createNormalizeMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('Normalize', {
    nestedField: 'data',
    keyFields: ['id', 'key'],
    expansionType: 'array',
    preserveNulls: true,
    flattenNested: false,
    maxDepth: 3
  });
}

function createRowGeneratorMetadata(_node: CanvasNode): ComponentMetadataUnion {
  const columns = generateDefaultColumns(5);
  const outputSchema = generateDefaultSchema(columns.map(c => c.name));
  
  return createComponentMetadata('RowGenerator', {
    numRows: 1000,
    columns,
    outputSchema,
    batchSize: 100,
    seed: 42,
    rateLimit: 1000
  });
}

function createExcelSourceMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('Excel', {
    filePath: '/path/to/file.xlsx',
    sheetName: 'Sheet1',
    headerRow: 1,
    skipRows: 0,
    columns: generateDefaultColumns(5),
    readOnly: true
  });
}

function createCSVSourceMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('CSV', {
    filePath: '/path/to/file.csv',
    delimiter: ',',
    quoteChar: '"',
    headerRow: true,
    skipRows: 0,
    inferSchema: true,
    encoding: 'UTF-8',
    dateFormat: 'yyyy-MM-dd'
  });
}

function createDatabaseSourceMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('Database', {
    connectionString: 'jdbc:mysql://localhost:3306/database',
    query: 'SELECT * FROM table',
    parameters: [],
    fetchSize: 1000,
    timeout: 30,
    schema: 'public',
    readOnly: true
  });
}

function createJSONSourceMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('JSON', {
    filePath: '/path/to/file.json',
    jsonPath: '$.*',
    flatten: false,
    schema: generateDefaultSchema(['id', 'name', 'value']),
    encoding: 'UTF-8',
    prettyPrint: true
  });
}

function createExcelSinkMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('ExcelOutput', {
    filePath: '/path/to/output.xlsx',
    sheetName: 'Output',
    writeMode: 'overwrite',
    includeHeader: true,
    autoSizeColumns: true,
    format: {
      headerStyle: { bold: true, backgroundColor: '#f0f0f0' },
      dataStyle: { wrapText: true }
    }
  });
}

function createCSVSinkMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('CSVOutput', {
    filePath: '/path/to/output.csv',
    delimiter: ',',
    quoteChar: '"',
    includeHeader: true,
    writeMode: 'overwrite',
    encoding: 'UTF-8',
    dateFormat: 'yyyy-MM-dd',
    lineSeparator: '\n'
  });
}

function createDatabaseSinkMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('DatabaseOutput', {
    connectionString: 'jdbc:mysql://localhost:3306/database',
    tableName: 'output_table',
    writeMode: 'insert',
    batchSize: 1000,
    keyColumns: ['id'],
    onConflict: 'error',
    transaction: true
  });
}

function createCacheInMetadata(node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('CacheIn', {
    cacheName: `cache_${node.id.substr(0, 8)}`,
    keyFields: generateDefaultCacheKeyFields(),
    ttl: 3600,
    maxSize: 10000,
    evictionPolicy: 'lru',
    persistence: 'memory',
    compression: false
  });
}

function createCacheOutMetadata(node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('CacheOut', {
    cacheName: `cache_${node.id.substr(0, 8)}`,
    keyFields: generateDefaultCacheKeyFields(),
    lookupType: 'exact',
    defaultValues: {},
    onMiss: 'null'
  });
}

function createReplicateMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('Replicate', {
    numCopies: 2,
    copyStrategy: 'identical',
    loadBalancing: 'broadcast'
  });
}

function createUniteMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('Unite', {
    inputSchemas: [],
    mergeStrategy: 'union',
    schemaAlignment: true,
    conflictResolution: 'first',
    deduplicate: false
  });
}

function createAssertMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('Assert', {
    conditions: [
      {
        field: 'id',
        operator: 'not-null',
        message: 'ID cannot be null'
      }
    ],
    onFailure: 'route',
    stopOnFirstError: false,
    includeDetails: true
  });
}

function createSchemaComplianceCheckMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('SchemaComplianceCheck', {
    expectedSchema: generateDefaultSchema(['id', 'name', 'value']),
    validationRules: [
      {
        field: 'id',
        rule: 'required',
        message: 'ID is required'
      },
      {
        field: 'name',
        rule: 'type',
        parameters: 'string',
        message: 'Name must be a string'
      }
    ],
    strictMode: false,
    treatWarningsAsErrors: false,
    errorThreshold: 0
  });
}

function createFlowMergeMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('FlowMerge', {
    mergeOrder: 'sequential',
    priorityRules: [],
    bufferSize: 1000,
    timeout: 30,
    preserveSource: false
  });
}

function createFileLookupMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('FileLookup', {
    keyField: 'id',
    filePath: '/path/to/lookup.csv',
    fieldMapping: { id: 'lookup_id', name: 'lookup_name' },
    fileType: 'csv',
    cacheSize: 1000,
    reloadOnChange: false,
    joinType: 'inner'
  });
}

function createFlowMeterMetadata(node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('FlowMeter', {
    meterId: `meter_${node.id.substr(0, 8)}`,
    metrics: ['count', 'rate', 'throughput', 'latency'],
    samplingRate: 1,
    windowSize: 60,
    aggregationInterval: '1m',
    includeDetails: true
  });
}

function createFlowMeterCatcherMetadata(node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('FlowMeterCatcher', {
    meterId: `meter_${node.id.substr(0, 8)}`,
    aggregationWindow: '5m',
    aggregationFunctions: ['sum', 'avg', 'min', 'max', 'count'],
    outputFormat: 'json',
    alertRules: [
      {
        metric: 'error_rate',
        condition: '>',
        threshold: 0.1,
        action: 'alert'
      }
    ]
  });
}

function createFlowToIterateMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('FlowToIterate', {
    subflowReference: 'subflow_1',
    batchSize: 100,
    iterationVariable: 'iteration',
    parallelProcessing: false,
    maxConcurrent: 1
  });
}

function createIterateToFlowMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('IterateToFlow', {
    mergeStrategy: 'append',
    aggregation: {
      function: 'sum',
      field: 'value'
    },
    deduplicate: false,
    sortResults: false
  });
}

function createSurvivorshipRuleMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('SurvivorshipRule', {
    rules: [
      {
        field: 'created_date',
        priority: 1,
        function: 'most-recent',
        parameters: { dateFormat: 'yyyy-MM-dd' }
      }
    ],
    keyFields: ['id', 'name'],
    tieBreaker: 'first',
    outputAllCandidates: false
  });
}

function createRuleSurvivorshipMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('RuleSurvivorship', {
    criteria: [
      {
        field: 'completeness',
        condition: '>',
        weight: 0.3
      }
    ],
    scoringRules: [
      {
        name: 'completeness_score',
        expression: 'completeness * 100',
        weight: 0.3
      }
    ],
    threshold: 0.7,
    outputScore: true,
    normalization: 'min-max'
  });
}

function createRecordMatchingMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('RecordMatching', {
    matchFields: ['name', 'email', 'phone'],
    algorithm: 'fuzzy',
    threshold: 0.85,
    outputFields: ['match_score', 'match_group'],
    blockingStrategy: 'standard',
    scoringWeights: { name: 0.4, email: 0.4, phone: 0.2 }
  });
}

function createFilterColumnsMetadata(_node: CanvasNode, fields: string[] = []): ComponentMetadataUnion {
  return createComponentMetadata('FilterColumns', {
    columns: fields.slice(0, 5),
    mode: 'keep',
    caseSensitive: false,
    patternMatching: false,
    useRegex: false,
    preserveOrder: true
  });
}

function createReplaceMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('Replace', {
    column: 'status',
    findValue: 'active',
    replaceValue: 'ACTIVE',
    useRegex: false,
    replaceAll: true,
    caseSensitive: false,
    wholeWord: true
  });
}

function createConvertTypeMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('ConvertType', {
    conversions: [
      {
        field: 'amount',
        type: 'decimal',
        format: '#,##0.00',
        precision: 10,
        scale: 2,
        onError: 'null'
      }
    ],
    defaultFormat: 'standard',
    strictMode: false
  });
}

function createExtractDelimitedFieldsMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('ExtractDelimitedFields', {
    sourceField: 'full_name',
    delimiter: ' ',
    targetFields: [
      { name: 'first_name', type: 'string', position: 0, trim: true },
      { name: 'last_name', type: 'string', position: 1, trim: true }
    ],
    quoteChar: '"',
    escapeChar: '\\',
    headerRow: false,
    skipEmpty: true,
    trimValues: true
  });
}

function createExtractRegexFieldsMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('ExtractRegexFields', {
    sourceField: 'log_entry',
    pattern: '^(\\w+)\\s+(\\d+)\\s+(.*)$',
    groups: [
      { groupIndex: 1, targetField: 'level', type: 'string' },
      { groupIndex: 2, targetField: 'timestamp', type: 'integer' },
      { groupIndex: 3, targetField: 'message', type: 'string' }
    ],
    flags: 'i',
    extractAll: false,
    caseSensitive: true
  });
}

function createExtractJSONFieldsMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('ExtractJSONFields', {
    sourceField: 'json_data',
    jsonPaths: [
      { path: '$.name', targetField: 'name', type: 'string' },
      { path: '$.value', targetField: 'value', type: 'number' }
    ],
    flattenArrays: false,
    nullOnMissing: true,
    prettyPrint: false
  });
}

function createExtractXMLFieldMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('ExtractXMLField', {
    sourceField: 'xml_data',
    xpath: '/root/item',
    targetFields: [
      { xpath: '@id', targetField: 'id', type: 'string' },
      { xpath: 'text()', targetField: 'content', type: 'string' }
    ],
    namespaces: { ns: 'http://example.com/ns' },
    treatAsDocument: true,
    validation: true
  });
}

function createParseRecordSetMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('ParseRecordSet', {
    recordDelimiter: '\n',
    fieldDefinitions: [
      { name: 'id', type: 'integer', start: 0, length: 10 },
      { name: 'name', type: 'string', start: 11, length: 50 }
    ],
    format: 'fixed-width',
    encoding: 'UTF-8',
    skipHeader: false,
    skipFooter: false
  });
}

function createSplitRowMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('SplitRow', {
    fieldToSplit: 'tags',
    splitRule: {
      type: 'delimiter',
      value: ','
    },
    maxSplits: 10,
    preserveEmpty: false,
    trimResults: true,
    outputSchema: generateDefaultSchema(['tag'])
  });
}

function createPivotToColumnsDelimitedMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('PivotToColumnsDelimited', {
    pivotColumn: 'category',
    valueColumn: 'value',
    outputPrefix: 'cat_',
    aggregateFunction: 'sum',
    fillMissing: 0,
    sortPivotValues: true
  });
}

function createUnpivotRowMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('UnpivotRow', {
    columnsToUnpivot: ['jan', 'feb', 'mar'],
    keyField: 'month',
    valueField: 'value',
    preserveOriginal: true,
    includeNulls: false,
    valueType: 'number'
  });
}

function createDenormalizeSortedRowMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('DenormalizeSortedRow', {
    groupFields: ['group_id'],
    delimiter: ',',
    sortWithinGroup: true,
    sortFields: [
      { field: 'timestamp', order: 'desc' }
    ],
    preserveGroupOrder: true
  });
}

function createNormalizeNumberMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('NormalizeNumber', {
    sourceField: 'score',
    targetField: 'normalized_score',
    formatRules: {
      locale: 'en-US',
      thousandSeparator: ',',
      decimalSeparator: '.',
      scaleFactor: 1,
      min: 0,
      max: 100,
      precision: 2
    },
    normalizationMethod: 'min-max'
  });
}

function createUniqRowMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('UniqRow', {
    uniqueFields: ['id', 'email'],
    keep: 'first',
    caseSensitive: true,
    considerNullsEqual: true,
    sortBeforeDedupe: false
  });
}

function createSampleRowMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('SampleRow', {
    samplingType: 'percent',
    parameters: { percent: 10 },
    seed: 12345,
    method: 'random',
    preserveOrder: false
  });
}

function createAddCRCRowMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('AddCRCRow', {
    algorithm: 'CRC32',
    targetField: 'checksum',
    fieldsToInclude: ['id', 'name', 'value'],
    outputFormat: 'hex',
    includeRowNumber: false
  });
}

function createStandardizeRowMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('StandardizeRow', {
    rules: [
      {
        field: 'phone',
        pattern: '^\\d{10}$',
        targetFormat: '(###) ###-####',
        validation: true
      }
    ],
    dictionary: {},
    locale: 'en-US',
    timezone: 'UTC',
    onError: 'skip'
  });
}

function createDataMaskingMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('DataMasking', {
    fields: ['ssn', 'email', 'phone'],
    method: 'asterisk',
    maskLength: 4,
    maskChar: '*',
    preserveFormat: true
  });
}

function createReplaceListMetadata(_node: CanvasNode): ComponentMetadataUnion {
  return createComponentMetadata('ReplaceList', {
    targetColumn: 'status',
    replacements: [
      { find: 'active', replace: 'ACTIVE', exact: true },
      { find: 'inactive', replace: 'INACTIVE', exact: true }
    ],
    listSource: 'inline',
    caseSensitive: false
  });
}

// ==================== MAIN GENERATOR FUNCTION ====================

export function generateDefaultMetadata(node: CanvasNode, inputSchema?: Schema): ComponentMetadataUnion | undefined {
  // Get the standardized component type
  const standardizedType = getComponentMetadataType(node.type?.toString() || node.technology || '');
  
  // Map to specific metadata generator
  switch (standardizedType) {
    // Single Input/Output Transformers
    case 'Map':
      return createMapMetadata(node, inputSchema);
    case 'Denormalize':
      return createDenormalizeMetadata(node, inputSchema?.fields.map(f => f.name) || []);
    case 'Normalize':
      return createNormalizeMetadata(node);
    case 'AggregateRow':
      return createAggregateRowMetadata(node, inputSchema?.fields.map(f => f.name) || []);
    case 'SortRow':
      return createSortRowMetadata(node, inputSchema?.fields.map(f => f.name) || []);
    case 'FilterRow':
      return createFilterRowMetadata(node);
    case 'FilterColumns':
      return createFilterColumnsMetadata(node, inputSchema?.fields.map(f => f.name) || []);
    case 'Replace':
      return createReplaceMetadata(node);
    case 'ConvertType':
      return createConvertTypeMetadata(node);
    case 'ExtractDelimitedFields':
      return createExtractDelimitedFieldsMetadata(node);
    case 'ExtractRegexFields':
      return createExtractRegexFieldsMetadata(node);
    case 'ExtractJSONFields':
      return createExtractJSONFieldsMetadata(node);
    case 'ExtractXMLField':
      return createExtractXMLFieldMetadata(node);
    case 'ParseRecordSet':
      return createParseRecordSetMetadata(node);
    case 'SplitRow':
      return createSplitRowMetadata(node);
    case 'PivotToColumnsDelimited':
      return createPivotToColumnsDelimitedMetadata(node);
    case 'UnpivotRow':
      return createUnpivotRowMetadata(node);
    case 'DenormalizeSortedRow':
      return createDenormalizeSortedRowMetadata(node);
    case 'NormalizeNumber':
      return createNormalizeNumberMetadata(node);
    case 'UniqRow':
      return createUniqRowMetadata(node);
    case 'SampleRow':
      return createSampleRowMetadata(node);
    case 'AddCRCRow':
      return createAddCRCRowMetadata(node);
    case 'StandardizeRow':
      return createStandardizeRowMetadata(node);
    case 'DataMasking':
      return createDataMaskingMetadata(node);
    case 'ReplaceList':
      return createReplaceListMetadata(node);
    
    // Multi-Handle Components
    case 'Join':
      return createJoinMetadata(node);
    case 'MatchGroup':
      return createMatchGroupMetadata(node);
    case 'Replicate':
      return createReplicateMetadata(node);
    case 'Unite':
      return createUniteMetadata(node);
    case 'FlowMerge':
      return createFlowMergeMetadata(node);
    case 'FileLookup':
      return createFileLookupMetadata(node);
    case 'FlowMeter':
      return createFlowMeterMetadata(node);
    case 'Assert':
      return createAssertMetadata(node);
    case 'SchemaComplianceCheck':
      return createSchemaComplianceCheckMetadata(node);
    
    // Source Nodes
    case 'RowGenerator':
      return createRowGeneratorMetadata(node);
    case 'Excel':
      return createExcelSourceMetadata(node);
    case 'CSV':
      return createCSVSourceMetadata(node);
    case 'Database':
      return createDatabaseSourceMetadata(node);
    case 'JSON':
      return createJSONSourceMetadata(node);
    
    // Paired/Cache Components
    case 'CacheIn':
      return createCacheInMetadata(node);
    case 'CacheOut':
      return createCacheOutMetadata(node);
    case 'FlowMeterCatcher':
      return createFlowMeterCatcherMetadata(node);
    case 'FlowToIterate':
      return createFlowToIterateMetadata(node);
    case 'IterateToFlow':
      return createIterateToFlowMetadata(node);
    
    // Specialized Components
    case 'SurvivorshipRule':
      return createSurvivorshipRuleMetadata(node);
    case 'RuleSurvivorship':
      return createRuleSurvivorshipMetadata(node);
    case 'RecordMatching':
      return createRecordMatchingMetadata(node);
    
    // Data Sink Nodes
    case 'ExcelOutput':
      return createExcelSinkMetadata(node);
    case 'CSVOutput':
      return createCSVSinkMetadata(node);
    case 'DatabaseOutput':
      return createDatabaseSinkMetadata(node);
    
    // Default fallback
    default:
      console.warn(`No metadata generator found for component type: ${standardizedType}`);
      return undefined;
  }
}

// ==================== NODE METADATA UPDATER ====================

export function updateNodeWithMetadata(
  node: CanvasNode,
  metadata: ComponentMetadataUnion,
  inputSchemas: Schema[] = []
): CanvasNode {
  // Generate handles from metadata with safe access
  const handleDefinitions = getComponentHandles(metadata);
  
  // Convert handle definitions to ComponentPorts
  const connectionPorts: ComponentPort[] = handleDefinitions.map(handle => ({
    id: handle.id,
    type: handle.type === 'input' ? PortType.INPUT : PortType.OUTPUT,
    side: handle.position,
    position: handle.positionPercent || 50,
    label: handle.label || (handle.type === 'input' ? 'Input' : 'Output'),
    maxConnections: handle.maxConnections || (handle.type === 'input' ? 1 : 999),
    dataType: handle.dataType,
    schema: handle.schema,
    isConnected: false
  }));
  
  // Calculate output schema based on configuration and input schemas
  const outputSchema = getOutputSchema(metadata, inputSchemas);
  
  // Extract visual properties from metadata with safe access
  const visualMetadata = (metadata as any).visual || {};
  const visualProperties = {
    color: visualMetadata.color,
    icon: visualMetadata.icon,
    shape: visualMetadata.shape,
    borderColor: visualMetadata.color,
    backgroundColor: visualMetadata.color ? `${visualMetadata.color}15` : '#ffffff15',
    borderStyle: visualMetadata.borderStyle || 'solid',
    borderWidth: visualMetadata.borderWidth || 1
  };
  
  // Generate component description
  const metadataAny = metadata as any;
  const description = metadataAny.description || 
    `${metadataAny.type} ${
      metadataAny.category === 'source' ? 'Source' : 
      metadataAny.category === 'sink' ? 'Destination' : 
      metadataAny.category === 'transformer' ? 'Transformer' : 'Component'
    }`;
  
  // Determine component category
  let componentCategory: 'input' | 'output' | 'process' = 'process';
  if (metadataAny.category === 'source' || isSourceComponent(metadataAny.type)) {
    componentCategory = 'input';
  } else if (metadataAny.category === 'sink' || isSinkComponent(metadataAny.type)) {
    componentCategory = 'output';
  } else if (
    metadataAny.category === 'transformer' || 
    metadataAny.category === 'multi-handle' || 
    metadataAny.category === 'paired' || 
    metadataAny.category === 'specialized'
  ) {
    componentCategory = 'process';
  }
  
  // Determine component type
  let componentType = 'processing';
  if (componentCategory === 'input') {
    componentType = 'input';
  } else if (componentCategory === 'output') {
    componentType = 'output';
  }
  
  // Create metadata object with all properties
  const metadataObject: any = {
    ...(node.metadata || {}),
    componentMetadata: metadata,
    description,
    outputSchema,
    inputSchemas,
    validation: validateConfiguration(metadata),
    tags: metadataAny.tags || [],
    version: metadataAny.version || '1.0',
    configuration: metadataAny.configuration || {},
    lastUpdated: new Date().toISOString(),
    configurationStatus: 'default'
  };
  
  // Update node with metadata
  const updatedNode: CanvasNode = {
    ...node,
    componentType,
    componentCategory,
    connectionPorts,
    visualProperties,
    metadata: metadataObject,
    status: NodeStatus.IDLE
  };
  
  return updatedNode;
}
// ==================== CONNECTION VALIDATION ====================

export function validateNodeConnections(
  node: CanvasNode,
  connections: Array<{ 
    sourceNodeId: string; 
    sourcePortId: string;
    targetNodeId: string; 
    targetPortId: string;
  }>,
  allNodes: CanvasNode[]
): { 
  isValid: boolean; 
  errors: string[]; 
  warnings: string[]; 
  validConnections: string[];
} {
  const errors: string[] = [];
  const warnings: string[] = [];
  const validConnections: string[] = [];
  
  const metadata = node.metadata?.componentMetadata;
  if (!metadata) {
    return { 
      isValid: true, 
      errors: [], 
      warnings: ['No metadata found for validation'], 
      validConnections: [] 
    };
  }
  
  // Get connections for this node
  const nodeConnections = connections.filter(conn => 
    conn.sourceNodeId === node.id || conn.targetNodeId === node.id
  );
  
  // Validate each handle
  const handleDefinitions = getComponentHandles(metadata);
  
  handleDefinitions.forEach(handle => {
    const connectionsForHandle = nodeConnections.filter(conn => 
      (conn.sourceNodeId === node.id && conn.sourcePortId === handle.id) ||
      (conn.targetNodeId === node.id && conn.targetPortId === handle.id)
    );
    
    const connectionCount = connectionsForHandle.length;
    
    // Check required handles
    if (handle.required && connectionCount === 0) {
      errors.push(`Required handle "${handle.label || handle.id}" is not connected`);
    }
    
    // Check maximum connections
    if (handle.maxConnections && connectionCount > handle.maxConnections) {
      errors.push(`Handle "${handle.label || handle.id}" exceeds maximum connections (${handle.maxConnections})`);
    }
    
    // Check if handle is connected to correct type
    connectionsForHandle.forEach(conn => {
      const isSourceConnection = conn.sourceNodeId === node.id;
      const otherNodeId = isSourceConnection ? conn.targetNodeId : conn.sourceNodeId;
      const otherNode = allNodes.find(n => n.id === otherNodeId);
      
      if (otherNode) {
        const otherMetadata = otherNode.metadata?.componentMetadata;
        if (otherMetadata) {
          // Validate schema compatibility
          const nodeSchema = node.metadata?.outputSchema;
          const otherSchema = otherNode.metadata?.inputSchema;
          
          if (nodeSchema && otherSchema && isSourceConnection) {
            // Check if output schema is compatible with input schema
            const compatibleFields = nodeSchema.fields.filter((field: { name: string; type: string; }) => 
              otherSchema.fields.some((otherField: { name: string; type: string }) => 
                otherField.name === field.name && 
                otherField.type === field.type
              )
            );
            
            if (compatibleFields.length === 0) {
              warnings.push(`Schema mismatch between ${node.name} and ${otherNode.name}`);
            }
          }
        }
        
        validConnections.push(conn.sourceNodeId === node.id ? conn.targetNodeId : conn.sourceNodeId);
      }
    });
  });
  
  // Component-specific validation
  const configValidation = validateConfiguration(metadata);
  errors.push(...configValidation.errors);
  warnings.push(...configValidation.warnings);
  
  return {
    isValid: errors.length === 0,
    errors,
    warnings,
    validConnections: [...new Set(validConnections)]
  };
}

// ==================== SCHEMA PROPAGATION ====================

export function propagateSchemaThroughPipeline(
  nodes: CanvasNode[],
  connections: Array<{ 
    sourceNodeId: string; 
    sourcePortId: string;
    targetNodeId: string; 
    targetPortId: string;
  }>
): CanvasNode[] {
  const updatedNodes = [...nodes];
  const visited = new Set<string>();
  const queue: string[] = [];
  
  // Find source nodes (no input connections)
  const sourceNodes = nodes.filter(node => {
    const isSource = !connections.some(conn => conn.targetNodeId === node.id);
    return isSource || (node.metadata?.componentMetadata?.category === 'source');
  });
  
  // Initialize queue with source nodes
  sourceNodes.forEach(node => {
    queue.push(node.id);
  });
  
  while (queue.length > 0) {
    const nodeId = queue.shift()!;
    if (visited.has(nodeId)) continue;
    
    visited.add(nodeId);
    const nodeIndex = updatedNodes.findIndex(n => n.id === nodeId);
    if (nodeIndex === -1) continue;
    
    const node = updatedNodes[nodeIndex];
    const metadata = node.metadata?.componentMetadata;
    
    if (!metadata) continue;
    
    // Get input connections for this node
    const inputConnections = connections.filter(conn => conn.targetNodeId === nodeId);
    
    // Collect input schemas
    const inputSchemas: Schema[] = [];
    
    for (const conn of inputConnections) {
      const sourceNode = updatedNodes.find(n => n.id === conn.sourceNodeId);
      if (sourceNode && sourceNode.metadata?.outputSchema) {
        inputSchemas.push(sourceNode.metadata.outputSchema);
        
        // Add source node to queue if not visited
        if (!visited.has(sourceNode.id) && !queue.includes(sourceNode.id)) {
          queue.push(sourceNode.id);
        }
      }
    }
    
    // Calculate output schema
    const outputSchema = getOutputSchema(metadata, inputSchemas);
    
    // Update node with new schema
    const updatedNode = {
      ...node,
      metadata: {
        ...node.metadata,
        inputSchemas,
        outputSchema,
        schemaPropagated: true,
        lastPropagation: new Date().toISOString()
      }
    };
    
    updatedNodes[nodeIndex] = updatedNode;
    
    // Find downstream nodes and add to queue
    const outputConnections = connections.filter(conn => conn.sourceNodeId === nodeId);
    outputConnections.forEach(conn => {
      if (!visited.has(conn.targetNodeId) && !queue.includes(conn.targetNodeId)) {
        queue.push(conn.targetNodeId);
      }
    });
  }
  
  return updatedNodes;
}

// ==================== CONFIGURATION VALIDATION ====================

export function validateConfigurationForNode(
  node: CanvasNode,
  configuration: any
): { 
  isValid: boolean; 
  errors: string[]; 
  warnings: string[]; 
  requiredFields: string[];
} {
  const metadata = node.metadata?.componentMetadata;
  if (!metadata) {
    return {
      isValid: false,
      errors: ['No metadata found for node'],
      warnings: [],
      requiredFields: []
    };
  }
  
  const errors: string[] = [];
  const warnings: string[] = [];
  const requiredFields = metadata.validation?.requiredFields || [];
  
  // Check required fields
  requiredFields.forEach((field: string | number) => {
    if (!configuration[field] || 
        (Array.isArray(configuration[field]) && configuration[field].length === 0)) {
      errors.push(`Required field "${field}" is missing or empty`);
    }
  });
  
  // Type-specific validation
  switch (metadata.type) {
    case 'Map':
      if (configuration.mappings) {
        const hasValidMappings = configuration.mappings.some((mapping: any) => 
          mapping.source && mapping.target
        );
        if (!hasValidMappings) {
          warnings.push('No valid mappings defined');
        }
      }
      break;
    
    case 'Join':
      if (configuration.leftKeys && configuration.rightKeys) {
        if (configuration.leftKeys.length !== configuration.rightKeys.length) {
          errors.push('Number of left keys must match number of right keys');
        }
        if (configuration.leftKeys.length === 0 || configuration.rightKeys.length === 0) {
          warnings.push('Join keys are empty');
        }
      }
      break;
    
    case 'MatchGroup':
      if (configuration.threshold !== undefined && 
          (configuration.threshold < 0 || configuration.threshold > 1)) {
        errors.push('Threshold must be between 0 and 1');
      }
      if (!configuration.keys || configuration.keys.length === 0) {
        errors.push('Match keys are required');
      }
      break;
    
    case 'RowGenerator':
      if (configuration.numRows !== undefined && configuration.numRows < 1) {
        errors.push('Number of rows must be at least 1');
      }
      if (!configuration.columns || configuration.columns.length === 0) {
        warnings.push('No columns defined for row generation');
      }
      break;
    
    case 'FilterRow':
      if (!configuration.condition || configuration.condition.trim() === '') {
        errors.push('Filter condition is required');
      }
      break;
    
    case 'Excel':
    case 'CSV':
    case 'Database':
      if (!configuration.filePath && !configuration.connectionString) {
        errors.push('Source configuration is incomplete');
      }
      break;
    
    case 'ExcelOutput':
    case 'CSVOutput':
    case 'DatabaseOutput':
      if (!configuration.filePath && !configuration.tableName) {
        errors.push('Output destination is not specified');
      }
      break;
  }
  
  return {
    isValid: errors.length === 0,
    errors,
    warnings,
    requiredFields
  };
}

// ==================== METADATA TRANSFORMATION ====================

export function transformLegacyMetadata(legacyMetadata: any): ComponentMetadataUnion | undefined {
  if (!legacyMetadata || !legacyMetadata.type) {
    return undefined;
  }
  
  // Explicitly type the legacy metadata structure
  const typedLegacyMetadata = legacyMetadata as {
    type: string;
    configuration?: any;
  };
  
  const type = getComponentMetadataType(typedLegacyMetadata.type);
  const configuration = typedLegacyMetadata.configuration || {};
  
  // Transform legacy configuration to new format
  switch (type) {
    case 'Map':
      return createComponentMetadata('Map', {
        mappings: configuration.mappings || [],
        inputSchema: configuration.inputSchema || { fields: [] },
        outputSchema: configuration.outputSchema || { fields: [] },
        defaultMappings: configuration.defaultMappings || true
      });
    
    case 'Join':
      return createComponentMetadata('Join', {
        joinType: configuration.joinType || 'INNER',
        leftKeys: configuration.leftKeys || [],
        rightKeys: configuration.rightKeys || [],
        filterExpression: configuration.filterExpression
      });
    
    case 'MatchGroup':
      return createComponentMetadata('MatchGroup', {
        keys: configuration.keys || [],
        threshold: configuration.threshold || 0.8,
        algorithm: configuration.algorithm || 'levenshtein',
        outputGroupId: configuration.outputGroupId !== false
      });
    
    default:
      // Only create metadata if type is a valid ComponentMetadataType
      const validTypes = [
        'Map', 'Denormalize', 'Normalize', 'AggregateRow', 'SortRow', 'FilterRow', 'FilterColumns',
        'Replace', 'ConvertType', 'ExtractDelimitedFields', 'ExtractRegexFields', 'ExtractJSONFields',
        'ExtractXMLField', 'ParseRecordSet', 'SplitRow', 'PivotToColumnsDelimited', 'UnpivotRow',
        'DenormalizeSortedRow', 'NormalizeNumber', 'UniqRow', 'SampleRow', 'AddCRCRow',
        'StandardizeRow', 'DataMasking', 'ReplaceList', 'Join', 'MatchGroup', 'Replicate', 'Unite',
        'FlowMerge', 'FileLookup', 'FlowMeter', 'Assert', 'SchemaComplianceCheck', 'RowGenerator',
        'Excel', 'CSV', 'Database', 'JSON', 'CacheIn', 'CacheOut', 'FlowMeterCatcher', 'FlowToIterate',
        'IterateToFlow', 'SurvivorshipRule', 'RuleSurvivorship', 'RecordMatching', 'ExcelOutput',
        'CSVOutput', 'DatabaseOutput'
      ];
      
      if (validTypes.includes(type)) {
        return createComponentMetadata(type as any, configuration);
      }
      
      console.warn(`Invalid component type in legacy metadata: ${type}`);
      return undefined;
  }
}

// ==================== METADATA MERGING ====================

export function mergeMetadata(
  existingMetadata: ComponentMetadataUnion,
  updates: Partial<ComponentMetadataUnion['configuration']>
): ComponentMetadataUnion {
  return {
      ...existingMetadata,
      configuration: {
          ...existingMetadata.configuration,
          ...updates
      },
      metadata: {
          ...(existingMetadata as any).metadata,
          lastUpdated: new Date().toISOString(),
          updatedBy: 'user'
      }
  } as unknown as ComponentMetadataUnion;
}

// ==================== METADATA COMPARISON ====================

export function compareMetadata(
  metadataA: ComponentMetadataUnion,
  metadataB: ComponentMetadataUnion
): { 
  isEqual: boolean; 
  differences: Array<{ field: string; valueA: any; valueB: any }> 
} {
  const differences: Array<{ field: string; valueA: any; valueB: any }> = [];
  
  // Compare type
  if (metadataA.type !== metadataB.type) {
    differences.push({ field: 'type', valueA: metadataA.type, valueB: metadataB.type });
  }
  
  // Compare category
  if (metadataA.category !== metadataB.category) {
    differences.push({ field: 'category', valueA: metadataA.category, valueB: metadataB.category });
  }
  
  // Compare configuration
  const compareObjects = (objA: any, objB: any, path: string = '') => {
    const keys = new Set([...Object.keys(objA || {}), ...Object.keys(objB || {})]);
    
    keys.forEach(key => {
      const currentPath = path ? `${path}.${key}` : key;
      
      if (objA[key] !== objB[key]) {
        if (typeof objA[key] === 'object' && typeof objB[key] === 'object') {
          compareObjects(objA[key], objB[key], currentPath);
        } else {
          differences.push({ 
            field: currentPath, 
            valueA: objA[key], 
            valueB: objB[key] 
          });
        }
      }
    });
  };
  
  compareObjects(metadataA.configuration, metadataB.configuration, 'configuration');
  
  return {
    isEqual: differences.length === 0,
    differences
  };
}

// ==================== METADATA SERIALIZATION ====================

export function serializeMetadata(metadata: ComponentMetadataUnion): string {
  return JSON.stringify(metadata, (key, value) => {
    // Handle circular references
    if (typeof value === 'object' && value !== null) {
      if (key === 'metadata' && value.original) {
        return '[Circular]';
      }
    }
    return value;
  }, 2);
}

export function deserializeMetadata(jsonString: string): ComponentMetadataUnion | undefined {
  try {
    const parsed = JSON.parse(jsonString);
    if (parsed.type) {
      return createComponentMetadata(parsed.type as any, parsed.configuration || {});
    }
  } catch (error) {
    console.error('Failed to deserialize metadata:', error);
  }
  return undefined;
}

// ==================== METADATA EXPORT/IMPORT ====================

export function exportMetadataAsTemplate(metadata: ComponentMetadataUnion): any {
  return {
    type: metadata.type,
    category: metadata.category,
    description: metadata.description,
    configuration: metadata.configuration,
    validation: metadata.validation,
    visual: metadata.visual,
    tags: metadata.tags,
    version: metadata.version,
    exportDate: new Date().toISOString()
  };
}

export function importMetadataFromTemplate(template: any): ComponentMetadataUnion | undefined {
  if (!template.type) {
    return undefined;
  }
  
  return createComponentMetadata(template.type as any, template.configuration || {});
}

// ==================== BULK METADATA OPERATIONS ====================

export function updateAllNodesMetadata(
  nodes: CanvasNode[],
  connections: Array<{ 
    sourceNodeId: string; 
    sourcePortId: string;
    targetNodeId: string; 
    targetPortId: string;
  }>
): CanvasNode[] {
  return nodes.map(node => {
    const metadata = node.metadata?.componentMetadata || 
                     generateDefaultMetadata(node);
    
    if (metadata) {
      // Get input schemas from connections
      const inputConnections = connections.filter(conn => conn.targetNodeId === node.id);
      const inputSchemas: Schema[] = [];
      
      inputConnections.forEach(conn => {
        const sourceNode = nodes.find(n => n.id === conn.sourceNodeId);
        if (sourceNode?.metadata?.outputSchema) {
          inputSchemas.push(sourceNode.metadata.outputSchema);
        }
      });
      
      return updateNodeWithMetadata(node, metadata, inputSchemas);
    }
    
    return node;
  });
}

export function validateAllNodes(
  nodes: CanvasNode[],
  connections: Array<{ 
    sourceNodeId: string; 
    sourcePortId: string;
    targetNodeId: string; 
    targetPortId: string;
  }>
): {
  valid: string[];
  invalid: Array<{ nodeId: string; errors: string[]; warnings: string[] }>;
  summary: {
    totalNodes: number;
    validNodes: number;
    invalidNodes: number;
    totalErrors: number;
    totalWarnings: number;
  };
} {
  const valid: string[] = [];
  const invalid: Array<{ nodeId: string; errors: string[]; warnings: string[] }> = [];
  let totalErrors = 0;
  let totalWarnings = 0;
  
  nodes.forEach(node => {
    const validation = validateNodeConnections(node, connections, nodes);
    
    if (validation.isValid) {
      valid.push(node.id);
    } else {
      invalid.push({
        nodeId: node.id,
        errors: validation.errors,
        warnings: validation.warnings
      });
      totalErrors += validation.errors.length;
      totalWarnings += validation.warnings.length;
    }
  });
  
  return {
    valid,
    invalid,
    summary: {
      totalNodes: nodes.length,
      validNodes: valid.length,
      invalidNodes: invalid.length,
      totalErrors,
      totalWarnings
    }
  };
}

// ==================== UTILITY FUNCTIONS ====================

export function getComponentSummary(metadata: ComponentMetadataUnion): {
  type: string;
  category: string;
  description: string;
  inputCount: number;
  outputCount: number;
  requiredConfig: string[];
  tags: string[];
} {
  const handles = getComponentHandles(metadata);
  const inputHandles = handles.filter(h => h.type === 'input');
  const outputHandles = handles.filter(h => h.type === 'output');
  
  return {
    type: metadata.type,
    category: metadata.category,
    description: metadata.description || '',
    inputCount: inputHandles.length,
    outputCount: outputHandles.length,
    requiredConfig: metadata.validation?.requiredFields || [],
    tags: metadata.tags || []
  };
}

export function getComponentTemplate(type: string): ComponentMetadataUnion | undefined {
  const mockNode: CanvasNode = {
    id: 'template',
    name: 'template',
    type: type,
    position: { x: 0, y: 0 },
    size: { width: 100, height: 50 }
  };
  
  return generateDefaultMetadata(mockNode);
}

export function listAllComponentTypes(): Array<{
  type: string;
  category: string;
  description: string;
  icon: string;
  color: string;
}> {
  const types = [
    // Single Input/Output Transformers
    'Map', 'Denormalize', 'Normalize', 'AggregateRow', 'SortRow', 'FilterRow', 'FilterColumns',
    'Replace', 'ConvertType', 'ExtractDelimitedFields', 'ExtractRegexFields', 'ExtractJSONFields',
    'ExtractXMLField', 'ParseRecordSet', 'SplitRow', 'PivotToColumnsDelimited', 'UnpivotRow',
    'DenormalizeSortedRow', 'NormalizeNumber', 'UniqRow', 'SampleRow', 'AddCRCRow',
    'StandardizeRow', 'DataMasking', 'ReplaceList',
    // Multi-Handle Components
    'Join', 'MatchGroup', 'Replicate', 'Unite', 'FlowMerge', 'FileLookup', 'FlowMeter',
    'Assert', 'SchemaComplianceCheck',
    // Source Nodes
    'RowGenerator', 'Excel', 'CSV', 'Database', 'JSON',
    // Paired/Cache Components
    'CacheIn', 'CacheOut', 'FlowMeterCatcher', 'FlowToIterate', 'IterateToFlow',
    // Specialized Components
    'SurvivorshipRule', 'RuleSurvivorship', 'RecordMatching',
    // Data Sink Nodes
    'ExcelOutput', 'CSVOutput', 'DatabaseOutput'
  ];
  
  return types.map(type => {
    const template = getComponentTemplate(type);
    return {
      type,
      category: template?.category || 'transformer',
      description: template?.description || `${type} component`,
      icon: template?.visual?.icon || '⚙️',
      color: template?.visual?.color || '#6b7280'
    };
  });
}